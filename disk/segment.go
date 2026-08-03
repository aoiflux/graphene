package disk

// WAL segmentation: retiring a log instead of discarding it.
//
// Compaction folds everything in the log into the CSR and then truncates,
// which is correct for durability and destroys the record of *how* the store
// reached its current state. Every commit's sequence number, timestamp, actor,
// and signature goes with it, as does every key rotation. The engine ends up
// able to prove what it holds and unable to say who put it there — which for a
// forensic store is the wrong half to keep.
//
// Segmentation retires the log to a numbered file instead. Retired segments are
// **not replayed**: their contents are already in the CSR, so replay still reads
// only the active segment and open cost is unchanged. They are kept as history,
// and read only when someone asks a question about the past.
//
// # Chaining
//
// Each segment's container header carries the digest of the segment it follows.
// Retiring segment N computes its digest and writes it into segment N+1's
// header, so the sequence is a hash chain: a retired segment that is deleted or
// replaced breaks the link in its successor, which is still present. That makes
// a removed segment **provably missing** rather than invisibly absent, which is
// the difference between an audit trail and a collection of files.
//
// # Retention is the caller's
//
// How long evidence is kept is a legal and operational question, not an
// engineering one — the plan files it as Q15 and leaves it open. So the engine
// provides the mechanism and takes the policy as a parameter, the same way
// compaction is advisory rather than automatic. The default keeps nothing, which
// is exactly the behaviour before segmentation existed.

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// segmentPrefix and segmentSuffix bracket a retired segment's sequence number:
// graphene.000001.wal
const (
	segmentPrefix = "graphene."
	segmentSuffix = ".wal"
)

// RetentionPolicy decides which retired segments are kept.
//
// The zero value keeps nothing, which reproduces the behaviour before
// segmentation: compaction discards the log. A caller that wants history has to
// say how much, because the engine has no basis for choosing.
//
// Rules are applied together and a segment survives only if it satisfies all of
// them. That is deliberate: "keep 30 days" and "keep at most 2 GB" are usually
// both meant, and a policy that honoured whichever was looser would surprise
// whoever set the tighter one.
type RetentionPolicy struct {
	// MaxSegments keeps at most this many retired segments, newest first.
	MaxSegments int

	// MaxBytes keeps segments while their total size is under this.
	MaxBytes int64

	// MaxAge keeps segments modified within this duration.
	MaxAge time.Duration
}

// Keeps reports whether the policy retains anything at all.
func (p RetentionPolicy) Keeps() bool {
	return p.MaxSegments > 0 || p.MaxBytes > 0 || p.MaxAge > 0
}

// SegmentInfo describes one retired segment.
type SegmentInfo struct {
	Path     string
	Sequence uint64
	Bytes    int64
	Modified time.Time

	// Digest is the SHA-256 of the whole file, which is what the next segment's
	// header names.
	Digest [walPrevDigestSize]byte

	// PrevDigest is what this segment's own header names, linking it backwards.
	PrevDigest [walPrevDigestSize]byte
}

// segmentPath builds the filename for a retired segment.
func segmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%06d%s", segmentPrefix, seq, segmentSuffix))
}

// parseSegmentSeq extracts a sequence number from a retired segment's name, and
// reports whether the name is one.
func parseSegmentSeq(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	// The active log is "graphene.wal", whose prefix and suffix *overlap* — it
	// carries both and has nothing between them. Slicing it without this guard
	// asks for name[9:8] and panics, and treating it as a segment would be worse
	// than panicking: retention would delete the live log.
	if len(name) <= len(segmentPrefix)+len(segmentSuffix) {
		return 0, false
	}
	mid := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	if mid == "" {
		return 0, false
	}
	seq, err := strconv.ParseUint(mid, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// ListSegments returns the retired segments in dir, oldest first.
//
// Reads the files; does not open the store. A segment whose header cannot be
// read is reported with a zero PrevDigest rather than omitted, because a
// damaged segment is exactly what someone auditing would want to see.
func ListSegments(dir string) ([]SegmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}

	var out []SegmentInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := parseSegmentSeq(e.Name())
		if !ok {
			continue
		}
		p := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("list segments: %w", err)
		}
		info, err := e.Info()
		if err != nil {
			return nil, fmt.Errorf("list segments: %w", err)
		}

		s := SegmentInfo{
			Path:     p,
			Sequence: seq,
			Bytes:    info.Size(),
			Modified: info.ModTime(),
			Digest:   sha256.Sum256(data),
		}
		if len(data) >= walFileHeaderSize && string(data[0:4]) == walMagic {
			copy(s.PrevDigest[:], data[14:46])
		}
		out = append(out, s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out, nil
}

// VerifySegmentChain checks that each segment names its predecessor's digest.
//
// The active log is not included: it is still being written, so its digest is
// not yet fixed. Pass its PrevDigest separately if the chain's head matters.
//
// A break is reported with the sequence numbers either side, because "the chain
// is broken" is not actionable and "segment 7 does not follow segment 6" is.
func VerifySegmentChain(segments []SegmentInfo) error {
	for i := 1; i < len(segments); i++ {
		prev, cur := segments[i-1], segments[i]
		if cur.PrevDigest == ([walPrevDigestSize]byte{}) {
			return fmt.Errorf("segment %d names no predecessor; the chain cannot be followed past it",
				cur.Sequence)
		}
		if cur.PrevDigest != prev.Digest {
			return fmt.Errorf("segment %d does not follow segment %d — it names predecessor %x, "+
				"but segment %d hashes to %x. A segment has been removed or replaced",
				cur.Sequence, prev.Sequence, cur.PrevDigest[:8], prev.Sequence, prev.Digest[:8])
		}
	}
	return nil
}

// Rotate retires the active log to a numbered segment and starts a fresh one.
//
// Called instead of Truncate when retention is configured. The ordering matters
// and is the same shape as compaction's: the old file is closed and renamed
// before the new one is created, so a crash leaves either a complete retired
// segment or an active log that still holds everything.
func (w *WAL) Rotate(dir string, seq uint64) (SegmentInfo, error) {
	if err := w.beginMaintenance(); err != nil {
		return SegmentInfo{}, err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return SegmentInfo{}, err
	}

	name := w.file.Name()
	if err := w.file.Sync(); err != nil {
		return SegmentInfo{}, fmt.Errorf("wal rotate: sync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return SegmentInfo{}, fmt.Errorf("wal rotate: close: %w", err)
	}

	dst := segmentPath(dir, seq)
	if err := os.Rename(name, dst); err != nil {
		return SegmentInfo{}, fmt.Errorf("wal rotate: rename: %w", err)
	}

	// The retired segment's digest is what the new active log names as its
	// predecessor, which is what makes the sequence a chain.
	retired, err := os.ReadFile(dst)
	if err != nil {
		return SegmentInfo{}, fmt.Errorf("wal rotate: read retired segment: %w", err)
	}
	digest := sha256.Sum256(retired)

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return SegmentInfo{}, fmt.Errorf("wal rotate: create active log: %w", err)
	}
	header := walFileHeader{Version: walFramingV2, SegmentSeq: seq + 1, PrevDigest: digest}
	if _, err := f.Write(appendWALFileHeader(header)); err != nil {
		f.Close()
		return SegmentInfo{}, fmt.Errorf("wal rotate: write header: %w", err)
	}

	w.file = f
	w.framing = walFramingV2
	w.dataStart = walFileHeaderSize

	fi, _ := os.Stat(dst)
	out := SegmentInfo{Path: dst, Sequence: seq, Digest: digest}
	if fi != nil {
		out.Bytes = fi.Size()
		out.Modified = fi.ModTime()
	}
	if len(retired) >= walFileHeaderSize && string(retired[0:4]) == walMagic {
		copy(out.PrevDigest[:], retired[14:46])
	}
	return out, nil
}

// ErrRetentionWouldBreakChain is returned when a policy would delete a segment
// that a kept segment still names.
var ErrRetentionWouldBreakChain = errors.New("disk: retention would break the segment chain")

// applyRetention deletes retired segments the policy does not keep, oldest
// first, and returns what was removed.
//
// Only ever removes from the **oldest** end. Deleting from the middle would
// leave a kept segment naming a predecessor that no longer exists, which is
// indistinguishable from tampering — the chain would report a break that the
// operator caused on purpose, and a verifier could not tell the difference.
func applyRetention(dir string, policy RetentionPolicy) ([]SegmentInfo, error) {
	if !policy.Keeps() {
		// Nothing is kept, so everything retired is removed. This is the
		// pre-segmentation behaviour: compaction discards the log.
		segs, err := ListSegments(dir)
		if err != nil {
			return nil, err
		}
		for _, s := range segs {
			if err := os.Remove(s.Path); err != nil {
				return nil, fmt.Errorf("retention: %w", err)
			}
		}
		return segs, nil
	}

	segs, err := ListSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return nil, nil
	}

	// Decide the oldest sequence to keep. Each rule proposes a cut; the tightest
	// wins, because a caller setting both a count and an age means both.
	keepFrom := 0

	if policy.MaxSegments > 0 && len(segs) > policy.MaxSegments {
		if cut := len(segs) - policy.MaxSegments; cut > keepFrom {
			keepFrom = cut
		}
	}
	if policy.MaxBytes > 0 {
		var total int64
		cut := len(segs)
		for i := len(segs) - 1; i >= 0; i-- {
			total += segs[i].Bytes
			if total > policy.MaxBytes {
				break
			}
			cut = i
		}
		if cut > keepFrom {
			keepFrom = cut
		}
	}
	if policy.MaxAge > 0 {
		cutoff := time.Now().Add(-policy.MaxAge)
		cut := 0
		for i, s := range segs {
			if s.Modified.Before(cutoff) {
				cut = i + 1
			}
		}
		if cut > keepFrom {
			keepFrom = cut
		}
	}

	removed := segs[:keepFrom]
	for _, s := range removed {
		if err := os.Remove(s.Path); err != nil {
			return nil, fmt.Errorf("retention: %w", err)
		}
	}
	return removed, nil
}
