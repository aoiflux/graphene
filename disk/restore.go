package disk

// Restore, and recovery to a point in time.
//
// # What a restore is
//
// A backup is an image plus a log, cut so that between them they hold each
// commit exactly once. Restoring is therefore copying them back out — verified
// first, because a backup nobody checked is a belief rather than a copy, and the
// moment it is needed is the worst moment to discover it rotted in the archive.
//
// # What a point in time is
//
// The log is a sequence of commits, each carrying its own sequence number and
// wall-clock instant, and replay applies a batch only when it reaches the commit
// record that closes it. So recovering to a point is truncating the copied log
// at the end of the last commit at or below that point: the restored directory
// *is* the store as it stood then, with no flag anywhere saying "stop early" for
// a later open to ignore.
//
// Two consequences follow from that being the mechanism, and both are limits
// worth stating rather than discovering.
//
// **The cut lands on commit boundaries, and nothing else is a boundary.** The
// single-record mutators — AddNode, AddEdge, DeleteNode and the rest — append a
// bare record with no sequence number and no timestamp, because they have never
// been transactions (see Store.Sync). They are restored when the commit that
// follows them is, and dropped with it otherwise. A store written entirely
// through them has no boundaries at all, so the only point in time it can be
// recovered to is the image. RestoreInfo.DroppedRecords reports how many records
// a cut discarded, so this is visible rather than silent.
//
// **A point inside the image cannot be reached.** Compaction folds commits into
// the image and retires the log behind them, which is exactly what makes an
// image plus a log hold everything once — and it is not reversible. The earliest
// point a given backup can be restored to is therefore the commit high-water
// mark of the image it carries, and asking for less returns
// ErrRestorePointTooEarly naming that figure. Recovering further back needs an
// older backup, which is the honest answer and the reason to keep more than one.

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// ErrRestorePointTooEarly reports a requested recovery point that the backup's
// image has already folded in, so no amount of replaying less log can reach it.
var ErrRestorePointTooEarly = errors.New("graphene: the recovery point is inside the backup's image")

// ErrRestoreDestinationInUse reports a destination that already holds files.
var ErrRestoreDestinationInUse = errors.New("graphene: the restore destination is not empty")

// RestoreOptions selects what to recover. The zero value restores everything the
// backup holds.
type RestoreOptions struct {
	// AtCommitSeq recovers the store as it stood after the last commit whose
	// sequence number is at or below this. Zero means every commit in the backup.
	AtCommitSeq uint64

	// AtTime recovers the store as it stood after the last commit at or before
	// this instant. The zero value means no time bound.
	//
	// Set with AtCommitSeq, both bounds apply and the tighter one wins: the cut
	// is the last commit satisfying each. Commit timestamps come from the writer's
	// clock at the moment of commit, so a store whose clock stepped backwards has
	// timestamps that do not increase with sequence — hence AtCommitSeq being the
	// figure the manifest reports and the one to prefer when precision matters.
	AtTime time.Time

	// ActorID is recorded against the restore entry in the audit log, when the
	// backup carries one.
	ActorID uint64
}

// wanted reports whether a point in time was asked for at all.
func (o RestoreOptions) wanted() bool {
	return o.AtCommitSeq > 0 || !o.AtTime.IsZero()
}

// RestoreInfo describes what a restore produced.
type RestoreInfo struct {
	// Dir is the restored store, ready to Open.
	Dir string

	// From is the manifest of the backup it came from.
	From BackupInfo

	// CommitSeq is the newest commit the restored store holds, and CommitTime
	// when it was made. With no point in time asked for these are the backup's
	// own figures.
	CommitSeq  uint64
	CommitTime time.Time

	// WALBytes is the length of the restored log. Shorter than the backup's when
	// a point in time cut it.
	WALBytes int64

	// DroppedRecords counts the records the cut discarded — the ones a full
	// restore would have replayed and this one will not. Zero when no point in
	// time was asked for.
	//
	// Worth looking at even when the cut landed where it was asked to: a large
	// figure on a small rewind is the signature of a store written through the
	// single-record mutators, whose records the cut takes with the following
	// commit. See the file comment.
	DroppedRecords int
}

// Restore copies the backup at src into dst and returns what it produced.
//
// dst must not exist, or must be an empty directory. Restoring over a store is
// refused rather than merged: a directory holding half of one store and half of
// another is not recoverable by anything, and the operator who wants the old
// one gone is better placed to say so than this function is.
//
// The backup is verified against its manifest before a byte is copied — every
// file present, at the recorded length, hashing to the recorded digest — so a
// restore that returns successfully is one whose source was whole. The copy is
// then hashed again as it is written, so it is also one whose destination
// matches.
func Restore(src, dst string, opts RestoreOptions) (RestoreInfo, error) {
	info, err := VerifyBackup(src)
	if err != nil {
		return RestoreInfo{}, err
	}

	srcAbs, err := filepath.Abs(src)
	if err != nil {
		return RestoreInfo{}, fmt.Errorf("restore: %w", err)
	}
	dstAbs, err := filepath.Abs(dst)
	if err != nil {
		return RestoreInfo{}, fmt.Errorf("restore: %w", err)
	}
	if srcAbs == dstAbs {
		return RestoreInfo{}, errors.New("restore: destination is the backup's own directory")
	}
	if err := restoreDestinationIsEmpty(dstAbs); err != nil {
		return RestoreInfo{}, err
	}
	if err := os.MkdirAll(dstAbs, 0700); err != nil {
		return RestoreInfo{}, fmt.Errorf("restore: mkdir %s: %w", dstAbs, err)
	}

	out := RestoreInfo{
		Dir:        dstAbs,
		From:       info,
		CommitSeq:  info.CommitSeq,
		CommitTime: info.TakenAt,
		WALBytes:   info.WALBytes,
	}

	for _, f := range info.Files {
		n, digest, cerr := copyFileSync(filepath.Join(srcAbs, f.Name), filepath.Join(dstAbs, f.Name), -1)
		if cerr != nil {
			return RestoreInfo{}, fmt.Errorf("restore: %s: %w", f.Name, cerr)
		}
		if n != f.Bytes {
			return RestoreInfo{}, fmt.Errorf("restore: %s copied %d bytes, the manifest says %d", f.Name, n, f.Bytes)
		}
		if hex.EncodeToString(digest) != f.Digest {
			return RestoreInfo{}, fmt.Errorf("restore: %s does not hash to what the manifest says", f.Name)
		}
	}

	if opts.wanted() {
		cut, cerr := restoreToPoint(dstAbs, info, opts)
		if cerr != nil {
			// A failed cut leaves a directory holding a full restore, which is a
			// working store at the wrong point rather than a broken one. Removing
			// it would be worse: the copy is the expensive half and the operator
			// may well want to retry the cut against a different point.
			return RestoreInfo{}, cerr
		}
		out.CommitSeq = cut.commitSeq
		out.CommitTime = time.Unix(0, cut.unixNano)
		out.DroppedRecords = cut.dropped
		if cut.offset >= 0 {
			out.WALBytes = cut.offset
		}
	}

	if err := recordRestore(dstAbs, out, opts.ActorID); err != nil {
		return RestoreInfo{}, err
	}
	if err := syncDir(dstAbs); err != nil {
		return RestoreInfo{}, fmt.Errorf("restore: %w", err)
	}
	return out, nil
}

// restoreDestinationIsEmpty refuses anything but a missing or empty directory.
func restoreDestinationIsEmpty(dir string) error {
	entries, err := os.ReadDir(dir)
	switch {
	case os.IsNotExist(err):
		return nil
	case err != nil:
		return fmt.Errorf("restore: %w", err)
	case len(entries) > 0:
		return fmt.Errorf("%w: %s holds %d entries", ErrRestoreDestinationInUse, dir, len(entries))
	}
	return nil
}

// recordRestore appends a restore entry to the audit log the backup carried.
//
// Only when there is one. A store that was never audited does not acquire a
// chain here: starting one at a restore would produce a log whose first entry
// claims to describe a history it has none of.
func recordRestore(dir string, out RestoreInfo, actorID uint64) error {
	if _, err := os.Stat(filepath.Join(dir, auditFileName)); err != nil {
		return nil
	}
	al, err := OpenAuditLog(dir)
	if err != nil {
		return fmt.Errorf("restore: audit: %w", err)
	}
	defer al.Close()

	detail := fmt.Sprintf("restored from a backup taken at commit %d; this store holds commits up to %d",
		out.From.CommitSeq, out.CommitSeq)
	if out.DroppedRecords > 0 {
		detail += fmt.Sprintf(" (%d records dropped by the recovery point)", out.DroppedRecords)
	}
	if _, err := al.Record(AuditRestore, actorID, detail); err != nil {
		return fmt.Errorf("restore: audit: %w", err)
	}
	return nil
}

// walCut is where a point-in-time recovery truncates the log, and what the
// store holds once it does.
type walCut struct {
	offset    int64 // log length after the cut
	commitSeq uint64
	unixNano  int64
	commits   int // commits kept
	dropped   int // records the cut discards
}

// restoreToPoint truncates the restored log at the requested point.
func restoreToPoint(dir string, info BackupInfo, opts RestoreOptions) (walCut, error) {
	// Read the floor from the image itself rather than from the manifest. The
	// manifest's file digests are checked, the manifest is not — and this is the
	// figure that decides whether a recovery point is reachable, so it should
	// come from the bytes it is a property of.
	floor, err := imageCommitSeq(filepath.Join(dir, csrFileName))
	if err != nil {
		return walCut{}, fmt.Errorf("restore: %w", err)
	}
	if opts.AtCommitSeq > 0 && opts.AtCommitSeq < floor {
		return walCut{}, fmt.Errorf("%w: commit %d is folded into the image, whose earliest reachable "+
			"point is commit %d — recovering further back needs an older backup",
			ErrRestorePointTooEarly, opts.AtCommitSeq, floor)
	}

	logPath := filepath.Join(dir, walFileName)
	cut, err := scanWALCut(logPath, opts)
	if err != nil {
		return walCut{}, fmt.Errorf("restore: %w", err)
	}
	if cut.commitSeq == 0 {
		// Nothing in the log is at or below the recovery point, so the restored
		// store is the image and whatever the cut left in front of it.
		cut.commitSeq = floor
	}
	if cut.commitSeq < floor {
		return walCut{}, fmt.Errorf("%w: the requested point falls before commit %d, which the image "+
			"already holds", ErrRestorePointTooEarly, floor)
	}

	if cut.offset >= 0 && cut.offset < info.WALBytes {
		if err := os.Truncate(logPath, cut.offset); err != nil {
			return walCut{}, fmt.Errorf("restore: truncate log: %w", err)
		}
		if err := syncPath(logPath); err != nil {
			return walCut{}, fmt.Errorf("restore: %w", err)
		}
	}
	return cut, nil
}

// scanWALCut walks the log and reports where a recovery point falls.
//
// It drives the ordinary replay parser rather than a second reader of the same
// format. That matters more than the code it saves: the parser is the one the
// store itself replays through and the one FuzzWALReplay covers, so a cut is
// placed by the same code that will later decide what the truncated log means.
// A private scanner would be a second opinion about the format, and the two
// would drift.
//
// Nothing is applied — the callbacks only count — so the walk costs a read of
// the file and no graph state at all.
func scanWALCut(path string, opts RestoreOptions) (walCut, error) {
	// offset starts negative rather than at zero because a headerless v1 log's
	// records begin at byte zero, so zero is a real cut and cannot double as
	// "there was nothing to cut".
	cut := walCut{offset: -1}

	total, err := walWalk(path, -1, &cut, opts)
	if err != nil {
		return walCut{}, err
	}
	if cut.offset < 0 {
		return cut, nil // the backup carries no log
	}

	// A second walk, over the kept prefix, to say how many records the cut
	// discards.
	//
	// Counted this way rather than accumulated during the first walk because a
	// commit is dispatched *before* the batch it closes is applied: at the moment
	// the offset is taken, the parser has not yet reported the very records that
	// commit makes durable. Marking the count there would attribute every kept
	// batch to the drop. The prefix is already parsed once by the truncation that
	// follows, so this is one more pass over bytes the restore is reading anyway.
	kept, err := walWalk(path, cut.offset, nil, opts)
	if err != nil {
		return walCut{}, err
	}
	cut.dropped = total - kept
	return cut, nil
}

// walWalk parses a log up to limit bytes (negative for all of it), counting the
// records it would apply. When into is non-nil it also tracks the newest commit
// at or below the recovery point and the offset just past it.
func walWalk(path string, limit int64, into *walCut, opts RestoreOptions) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	header, dataStart, err := readWALFileHeader(f)
	if err != nil {
		return 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if _, err := f.Seek(dataStart, io.SeekStart); err != nil {
		return 0, err
	}
	if into != nil {
		into.offset = dataStart
	}

	var src io.Reader = f
	size := fi.Size()
	if limit >= 0 {
		src = io.LimitReader(f, limit-dataStart)
		size = limit
	}

	counted := 0
	counting := &countingReader{r: bufio.NewReader(src)}
	count := func([]byte) error { counted++; return nil }

	cb := ReplayCallbacks{
		NodeFunc:          count,
		EdgeFunc:          count,
		NodePropFunc:      count,
		EdgePropFunc:      count,
		NodeDeleteFunc:    count,
		EdgeDeleteFunc:    count,
		NodePropPurgeFunc: count,
		EdgePropPurgeFunc: count,
		KeyTransitionFunc: count,
	}
	if into != nil {
		cb.CommitFunc = func(m batchMeta) {
			if opts.AtCommitSeq > 0 && m.CommitSeq > opts.AtCommitSeq {
				return
			}
			if !opts.AtTime.IsZero() && time.Unix(0, m.UnixNano).After(opts.AtTime) {
				return
			}
			// The parser has read this commit record's header, payload and footer
			// before dispatching, so the count is the offset just past it — which
			// is where a truncation keeps the batch whole and takes nothing of the
			// next one.
			into.offset = dataStart + counting.n
			into.commitSeq = m.CommitSeq
			into.unixNano = m.UnixNano
			into.commits++
		}
	}

	if err := replayRecords(counting, size, header.Version, cb); err != nil {
		return 0, fmt.Errorf("scan log: %w", err)
	}
	return counted, nil
}

// countingReader reports how many bytes the reader above it has consumed.
//
// It sits between the parser and the buffer, not below it, so the count is what
// the parser has taken rather than what the buffer has read ahead.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// imageCommitSeq reads the commit high-water mark out of a CSR image's header,
// without parsing the records behind it.
//
// Zero for a missing image and for one written before v8, which is the honest
// answer in both cases: neither carries the mark, so neither constrains a
// recovery point.
func imageCommitSeq(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, csrV8HeaderSize)
	n, err := f.ReadAt(buf, 0)
	if n < csrV8HeaderSize {
		if err != nil && err != io.EOF {
			return 0, err
		}
		return 0, nil // too short to be a v8 image
	}
	if string(buf[0:4]) != "GCSR" {
		return 0, fmt.Errorf("%s is not a CSR image", path)
	}
	if binary.LittleEndian.Uint16(buf[4:6]) < csrVersionSectioned {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(buf[46:54]), nil
}

// syncPath fsyncs a file by name, for a change made through a path-based call
// like os.Truncate that leaves no handle behind.
func syncPath(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	return f.Close()
}
