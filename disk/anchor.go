package disk

// External anchoring: the one check that does not compare the store to itself.
//
// Every verification the engine performs — snapshot roots, attestations, segment
// chains, the audit log, and the custody report that walks all four — compares
// the store against the store. That is enough to catch corruption and enough to
// catch an outsider. It is not enough to catch an insider holding the signing
// key, because such an adversary can rebuild every chain consistently and the
// result verifies perfectly. §12.4's T-02, T-06, T-09 and T-14 are all this
// shape, and §12.5 says outright that anchoring is the only control for them.
//
// # What is anchored
//
// A Checkpoint binds every history's head into one digest — snapshot,
// attestation, WAL segments, audit log, redaction ledger, grant ledger. Anchoring the snapshot
// root alone — which is what CustodyForAnchored accepts — leaves the others
// free: an adversary who rewrites only the audit log changes no snapshot root
// and passes that check. Binding them together removes the choice.
//
// # No transport, by decision
//
// The plan's Q12 asks whether anchoring is in scope and answers: define the
// interface, ship no transport. This file follows that. Publishing a digest to a
// timestamp authority, a transparency log, a blockchain, another organisation's
// storage, or a printed sheet in a safe are all valid anchors with wildly
// different operational properties, and all of them would be a network
// dependency the engine has never had. The engine says what it needs from an
// anchor and takes one as a parameter.
//
// # What an anchor actually proves
//
// Only this: a digest existed at a time. It does not prove the store is honest,
// and an anchor consulted only when trouble is suspected proves nothing about
// the period nobody was watching. The guarantee is bounded by publishing
// frequency — everything since the last publication is still freely rewritable,
// which is why AnchorAudit reports the unanchored tail rather than leaving the
// reader to infer it.

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// domainCheckpoint separates checkpoint digests from every other hashed
// structure, so a value from one can never be replayed as the other.
const domainCheckpoint = 0xC5

// checkpointFileName holds the local record of what has been published.
const checkpointFileName = "graphene.checkpoints"

// Checkpoint is the state of every history at one instant.
//
// It is the unit of anchoring: its Digest is what goes outside, and everything
// else is what the digest commits to.
type Checkpoint struct {
	// Seq is the checkpoint's position in this store's own sequence, from 1.
	Seq      uint64
	UnixNano int64
	ActorID  uint64

	// The heads of every history the store keeps. A zero value means that
	// history was not established — which the digest still commits to, so "there
	// was no audit log" cannot be retrofitted later into "here is the audit log".
	SnapshotRoot  merkle.Hash
	AttestationID [attestationIDSize]byte
	SegmentHead   [walPrevDigestSize]byte
	AuditHead     [sha256.Size]byte
	RedactionHead [sha256.Size]byte
	GrantHead     [sha256.Size]byte

	// Counts, so a truncation is visible as more than a changed hash. A reader
	// comparing two checkpoints learns whether history grew or shrank.
	SegmentCount   uint64
	AuditCount     uint64
	RedactionCount uint64
	GrantCount     uint64

	// Prev is the digest of the checkpoint before this one, zero for the first.
	// Chaining them means a removed checkpoint is a broken link rather than an
	// absence — the same argument as everywhere else in this engine.
	Prev merkle.Hash

	// Digest binds every field above. This is the value to publish, and the
	// only value an Anchor ever sees.
	Digest merkle.Hash
}

// checkpointSignedData is the canonical byte string a checkpoint's digest covers.
func checkpointSignedData(c Checkpoint) []byte {
	buf := make([]byte, 0, 1+8+8+8+merkle.Size+attestationIDSize+walPrevDigestSize+3*sha256.Size+8+8+8+8+merkle.Size)
	buf = append(buf, domainCheckpoint)
	buf = binary.LittleEndian.AppendUint64(buf, c.Seq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(c.UnixNano))
	buf = binary.LittleEndian.AppendUint64(buf, c.ActorID)
	buf = append(buf, c.SnapshotRoot[:]...)
	buf = append(buf, c.AttestationID[:]...)
	buf = append(buf, c.SegmentHead[:]...)
	buf = append(buf, c.AuditHead[:]...)
	buf = append(buf, c.RedactionHead[:]...)
	buf = append(buf, c.GrantHead[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, c.SegmentCount)
	buf = binary.LittleEndian.AppendUint64(buf, c.AuditCount)
	buf = binary.LittleEndian.AppendUint64(buf, c.RedactionCount)
	buf = binary.LittleEndian.AppendUint64(buf, c.GrantCount)
	return append(buf, c.Prev[:]...)
}

// computeCheckpointDigest derives a checkpoint's digest from its contents.
func computeCheckpointDigest(c Checkpoint) merkle.Hash {
	return merkle.Hash(sha256.Sum256(checkpointSignedData(c)))
}

// String is a one-line rendering for a log or a CLI.
func (c Checkpoint) String() string {
	return fmt.Sprintf("checkpoint %d at %s: digest %x (snapshot %x, %d segments, %d audit entries, %d redactions, %d grants)",
		c.Seq, time.Unix(0, c.UnixNano).UTC().Format(time.RFC3339), c.Digest[:8],
		c.SnapshotRoot[:8], c.SegmentCount, c.AuditCount, c.RedactionCount, c.GrantCount)
}

// --- the interface, and nothing behind it ---

// AnchorRecord is one published digest as the anchor reports it back.
type AnchorRecord struct {
	Digest merkle.Hash

	// UnixNano is when the *anchor* says the digest was published, which is the
	// only interesting timestamp here. A time the store wrote down is a time the
	// store can rewrite.
	UnixNano int64

	// Ref is the anchor's own handle on the publication — a transaction hash, a
	// timestamp-token serial, a URL, a page number. Opaque to the engine, and
	// the thing a human follows to check the anchor independently.
	Ref string
}

// Anchor is somewhere outside the store where a digest can be put and later read
// back.
//
// The engine ships no implementation. What makes an anchor an anchor is that an
// adversary with full control of the store cannot alter it, and nothing inside
// this process can establish that property — it comes from where the anchor
// lives and who runs it.
//
// Implementations must be append-only from the engine's point of view: Publish
// adds, and nothing removes. An anchor that can be rewritten is not one.
type Anchor interface {
	// Publish records a digest and returns the anchor's receipt for it.
	Publish(digest merkle.Hash) (AnchorRecord, error)

	// Records returns everything published, oldest first.
	Records() ([]AnchorRecord, error)
}

// --- building a checkpoint from live state ---

// Checkpoint captures the current state of every history without publishing
// anything.
//
// The returned checkpoint's Prev and Seq continue this store's local checkpoint
// chain, so two consecutive calls with no intervening publication produce the
// same Seq — capturing is not recording.
func (s *Store) Checkpoint() (Checkpoint, error) {
	history, err := s.CheckpointHistory()
	if err != nil {
		return Checkpoint{}, err
	}

	c := Checkpoint{Seq: uint64(len(history)) + 1, ActorID: s.attestActorID}
	if n := len(history); n > 0 {
		c.Prev = history[n-1].Digest
	}

	// Snapshot. A store that has never compacted has no root, and the zero value
	// is committed to rather than treated as missing data.
	if roots, rerr := s.SnapshotRoots(); rerr == nil {
		c.SnapshotRoot = roots.Snapshot
	} else if !errorIs(rerr, ErrNoSnapshotRoots) {
		return Checkpoint{}, rerr
	}

	// Attestation over that snapshot.
	if att, aerr := s.SnapshotAttestation(); aerr == nil {
		c.AttestationID = att.ID
	} else if !errorIs(aerr, ErrNoAttestation) {
		return Checkpoint{}, aerr
	}

	// Segment chain head.
	segs, serr := ListSegments(s.dir)
	if serr != nil {
		return Checkpoint{}, serr
	}
	c.SegmentCount = uint64(len(segs))
	if n := len(segs); n > 0 {
		c.SegmentHead = segs[n-1].Digest
	}

	// Audit chain head.
	entries, eerr := ReadAuditLog(s.dir)
	if eerr != nil {
		return Checkpoint{}, eerr
	}
	c.AuditCount = uint64(len(entries))
	if n := len(entries); n > 0 {
		c.AuditHead = entries[n-1].Hash
	}

	// Redaction ledger head. Read from disk rather than from s.redactions, so a
	// store with redaction switched off still commits to whatever ledger the
	// directory holds — turning the option off must not make past redactions
	// vanish from the digest.
	reds, rderr := ReadRedactions(s.dir)
	if rderr != nil {
		return Checkpoint{}, rderr
	}
	c.RedactionCount = uint64(len(reds))
	if n := len(reds); n > 0 {
		c.RedactionHead = reds[n-1].Hash
	}

	// Grant ledger head, read from disk for the same reason as the redactions:
	// turning Options.Roles off must not make past privilege changes vanish from
	// the digest.
	grants, gerr := ReadGrants(s.dir)
	if gerr != nil {
		return Checkpoint{}, gerr
	}
	c.GrantCount = uint64(len(grants))
	if n := len(grants); n > 0 {
		c.GrantHead = grants[n-1].Hash
	}

	c.UnixNano = time.Now().UnixNano()
	c.Digest = computeCheckpointDigest(c)
	return c, nil
}

// PublishCheckpoint records the intent to publish, captures the current state,
// hands the digest to the anchor, and appends the checkpoint to this store's
// local chain.
//
// # Why the audit entry comes first
//
// Publishing is an operator action and belongs in the audit log. Writing that
// entry *after* the capture would move the audit head, so the store would no
// longer match the checkpoint it had just published — every publication would
// immediately invalidate itself and VerifyAgainstAnchor could never report a
// settled store. Recording the intent beforehand puts the entry inside the
// checkpoint that covers it, and leaves the four heads untouched from capture
// onwards.
//
// The cost is that the entry cannot name the anchor's receipt, which does not
// exist yet. That is the right trade: the receipt is held by the anchor and by
// the local chain, whereas a checkpoint that can never be shown to be current is
// useful to nobody. An entry with no matching checkpoint means a publication was
// attempted and failed, which is itself worth recording.
//
// # Why the anchor comes before the local append
//
// A checkpoint recorded locally but never published claims an external witness
// it does not have, and that lie is worse than the gap. If the local append then
// fails, the anchor holds a digest this store cannot explain — loud, and
// therefore the right failure.
func (s *Store) PublishCheckpoint(a Anchor) (Checkpoint, AnchorRecord, error) {
	if a == nil {
		return Checkpoint{}, AnchorRecord{}, errors.New("disk: publish checkpoint: no anchor")
	}

	history, err := s.CheckpointHistory()
	if err != nil {
		return Checkpoint{}, AnchorRecord{}, err
	}
	if err := s.recordAudit(AuditCheckpoint, s.attestActorID,
		fmt.Sprintf("publishing checkpoint %d", len(history)+1)); err != nil {
		return Checkpoint{}, AnchorRecord{}, err
	}

	c, err := s.Checkpoint()
	if err != nil {
		return Checkpoint{}, AnchorRecord{}, err
	}

	rec, err := a.Publish(c.Digest)
	if err != nil {
		return c, AnchorRecord{}, fmt.Errorf("disk: anchor rejected the checkpoint: %w", err)
	}
	if rec.Digest != c.Digest {
		return c, rec, fmt.Errorf("disk: the anchor acknowledged a different digest (%x, wanted %x)",
			rec.Digest[:8], c.Digest[:8])
	}

	if err := appendCheckpoint(s.dir, c); err != nil {
		return c, rec, fmt.Errorf("disk: checkpoint %x was published but not recorded locally: %w",
			c.Digest[:8], err)
	}
	return c, rec, nil
}

// --- the local chain ---

// checkpointRecordSize is the fixed on-disk size of one checkpoint.
const checkpointRecordSize = 8 + 8 + 8 + merkle.Size + attestationIDSize +
	walPrevDigestSize + 3*sha256.Size + 8 + 8 + 8 + 8 + merkle.Size + merkle.Size

func appendCheckpointRecord(buf []byte, c Checkpoint) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, c.Seq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(c.UnixNano))
	buf = binary.LittleEndian.AppendUint64(buf, c.ActorID)
	buf = append(buf, c.SnapshotRoot[:]...)
	buf = append(buf, c.AttestationID[:]...)
	buf = append(buf, c.SegmentHead[:]...)
	buf = append(buf, c.AuditHead[:]...)
	buf = append(buf, c.RedactionHead[:]...)
	buf = append(buf, c.GrantHead[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, c.SegmentCount)
	buf = binary.LittleEndian.AppendUint64(buf, c.AuditCount)
	buf = binary.LittleEndian.AppendUint64(buf, c.RedactionCount)
	buf = binary.LittleEndian.AppendUint64(buf, c.GrantCount)
	buf = append(buf, c.Prev[:]...)
	return append(buf, c.Digest[:]...)
}

func readCheckpointRecord(b []byte) Checkpoint {
	var c Checkpoint
	c.Seq = binary.LittleEndian.Uint64(b[0:8])
	c.UnixNano = int64(binary.LittleEndian.Uint64(b[8:16]))
	c.ActorID = binary.LittleEndian.Uint64(b[16:24])
	off := 24
	copy(c.SnapshotRoot[:], b[off:off+merkle.Size])
	off += merkle.Size
	copy(c.AttestationID[:], b[off:off+attestationIDSize])
	off += attestationIDSize
	copy(c.SegmentHead[:], b[off:off+walPrevDigestSize])
	off += walPrevDigestSize
	copy(c.AuditHead[:], b[off:off+sha256.Size])
	off += sha256.Size
	copy(c.RedactionHead[:], b[off:off+sha256.Size])
	off += sha256.Size
	copy(c.GrantHead[:], b[off:off+sha256.Size])
	off += sha256.Size
	c.SegmentCount = binary.LittleEndian.Uint64(b[off : off+8])
	off += 8
	c.AuditCount = binary.LittleEndian.Uint64(b[off : off+8])
	off += 8
	c.RedactionCount = binary.LittleEndian.Uint64(b[off : off+8])
	off += 8
	c.GrantCount = binary.LittleEndian.Uint64(b[off : off+8])
	off += 8
	copy(c.Prev[:], b[off:off+merkle.Size])
	off += merkle.Size
	copy(c.Digest[:], b[off:off+merkle.Size])
	return c
}

// appendCheckpoint adds one checkpoint to dir's local chain, syncing before it
// returns — a checkpoint the anchor knows about and the disk does not is the
// gap this whole file exists to close.
func appendCheckpoint(dir string, c Checkpoint) error {
	f, err := os.OpenFile(filepath.Join(dir, checkpointFileName),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(appendCheckpointRecord(make([]byte, 0, checkpointRecordSize), c)); err != nil {
		return err
	}
	return f.Sync()
}

// CheckpointHistory returns this store's local checkpoint chain, oldest first.
//
// A missing file is an empty history rather than an error: a store that has
// never anchored is the common case, not a fault.
func (s *Store) CheckpointHistory() ([]Checkpoint, error) { return ReadCheckpoints(s.dir) }

// ReadCheckpoints reads dir's checkpoint chain without opening the store.
func ReadCheckpoints(dir string) ([]Checkpoint, error) {
	data, err := os.ReadFile(filepath.Join(dir, checkpointFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data)%checkpointRecordSize != 0 {
		// A torn tail from a crash mid-append. Everything before it is whole and
		// is returned; the partial record is dropped rather than guessed at.
		data = data[:len(data)-len(data)%checkpointRecordSize]
	}
	out := make([]Checkpoint, 0, len(data)/checkpointRecordSize)
	for off := 0; off+checkpointRecordSize <= len(data); off += checkpointRecordSize {
		out = append(out, readCheckpointRecord(data[off:off+checkpointRecordSize]))
	}
	return out, nil
}

// VerifyCheckpointChain checks that each checkpoint's digest follows from its
// contents and links to the one before it.
//
// This is a local check and catches only a clumsy edit. An adversary who
// recomputes the chain forward passes it — which is precisely why the digests
// are published, and why this function is not the interesting one.
func VerifyCheckpointChain(chain []Checkpoint) error {
	var prev merkle.Hash
	for i, c := range chain {
		if want := uint64(i + 1); c.Seq != want {
			return fmt.Errorf("checkpoint chain: entry %d carries sequence %d, expected %d", i, c.Seq, want)
		}
		if c.Prev != prev {
			return fmt.Errorf("checkpoint chain: entry %d does not link to its predecessor; "+
				"a checkpoint was removed or reordered", c.Seq)
		}
		if got := computeCheckpointDigest(c); got != c.Digest {
			return fmt.Errorf("checkpoint chain: entry %d's digest does not follow from its contents; "+
				"it was edited after being written", c.Seq)
		}
		prev = c.Digest
	}
	return nil
}

// --- verification against the anchor ---

// AnchorAudit is the result of checking a store against what was published.
type AnchorAudit struct {
	// Checkpoints and Published are what each side holds.
	Checkpoints []Checkpoint
	Published   []AnchorRecord

	// Matched counts local checkpoints whose digest appears in the anchor.
	Matched int

	// LastAnchored is the most recent checkpoint the anchor confirms, and
	// LastAnchoredAt is when the anchor says it was published. Everything the
	// store did afterwards is inside the unanchored window.
	LastAnchored   *Checkpoint
	LastAnchoredAt int64

	// CurrentMatchesLast reports whether the live store still stands where the
	// last anchored checkpoint said it did. False is not by itself wrong: an
	// active store moves on. It is wrong when nothing was supposed to change.
	CurrentMatchesLast bool

	// Gaps reuses the custody vocabulary so a caller handling both does not need
	// two shapes for the same idea.
	Gaps []CustodyGap
}

// Broken reports whether any check actively failed.
func (a AnchorAudit) Broken() bool {
	for _, g := range a.Gaps {
		if g.Fatal {
			return true
		}
	}
	return false
}

// Summary is a one-line verdict.
func (a AnchorAudit) Summary() string {
	switch {
	case a.Broken():
		noun := "findings"
		if len(a.Gaps) == 1 {
			noun = "finding"
		}
		return fmt.Sprintf("anchor: BROKEN — %d of %d checkpoints confirmed, %d %s",
			a.Matched, len(a.Checkpoints), len(a.Gaps), noun)
	case len(a.Checkpoints) == 0 && len(a.Published) == 0:
		return "anchor: nothing has ever been published; the store is unanchored"
	default:
		return fmt.Sprintf("anchor: %d of %d checkpoints confirmed by the anchor",
			a.Matched, len(a.Checkpoints))
	}
}

// VerifyAgainstAnchor checks this store's history against what was published
// externally.
//
// Three questions, in order of what they can prove:
//
//  1. Does the local checkpoint chain hold together? (Weak — recomputable.)
//  2. Does every locally recorded checkpoint appear in the anchor? (Strong — a
//     rewritten history produces digests the anchor never saw.)
//  3. Does the live store still match the last anchored checkpoint, and how much
//     has happened since? (Bounds what is still freely rewritable.)
func (s *Store) VerifyAgainstAnchor(a Anchor) (AnchorAudit, error) {
	if a == nil {
		return AnchorAudit{}, errors.New("disk: verify against anchor: no anchor")
	}

	audit := AnchorAudit{}
	chain, err := s.CheckpointHistory()
	if err != nil {
		return audit, err
	}
	audit.Checkpoints = chain

	published, err := a.Records()
	if err != nil {
		return audit, fmt.Errorf("disk: the anchor could not be read: %w", err)
	}
	audit.Published = published

	byDigest := make(map[merkle.Hash]AnchorRecord, len(published))
	for _, rec := range published {
		byDigest[rec.Digest] = rec
	}

	if cerr := VerifyCheckpointChain(chain); cerr != nil {
		audit.Gaps = append(audit.Gaps, CustodyGap{
			Layer: LayerExternal, Fatal: true,
			Detail: fmt.Sprintf("the local checkpoint chain is broken: %v", cerr),
		})
	}

	// **The check worth having, and it runs both ways.**
	//
	// A locally recorded checkpoint the anchor never saw means the local chain
	// was rewritten after the fact — an adversary can forge a consistent chain,
	// but not a publication that already happened.
	//
	// A published digest with no local checkpoint is the same attack from the
	// other end: destroy the local record and the store looks as though it was
	// never anchored. That reading has to be fatal, because it is otherwise
	// indistinguishable from innocence and strictly easier to perform than
	// forging a chain.
	localDigests := make(map[merkle.Hash]struct{}, len(chain))
	for _, c := range chain {
		localDigests[c.Digest] = struct{}{}
	}

	for _, c := range chain {
		rec, ok := byDigest[c.Digest]
		if !ok {
			audit.Gaps = append(audit.Gaps, CustodyGap{
				Layer: LayerExternal, Fatal: true,
				Detail: fmt.Sprintf("checkpoint %d (digest %x) was never published; "+
					"the local history does not match what was witnessed", c.Seq, c.Digest[:8]),
			})
			continue
		}
		audit.Matched++
		last := c
		audit.LastAnchored = &last
		audit.LastAnchoredAt = rec.UnixNano
	}

	orphaned := 0
	for _, rec := range published {
		if _, ok := localDigests[rec.Digest]; !ok {
			orphaned++
		}
	}
	if orphaned > 0 {
		audit.Gaps = append(audit.Gaps, CustodyGap{
			Layer: LayerExternal, Fatal: true,
			Detail: fmt.Sprintf("the anchor witnessed %d checkpoint(s) this store has no record of. "+
				"Either the local checkpoint chain was deleted, or a publication succeeded and its "+
				"local append did not — in both cases this store cannot account for something that "+
				"provably happened", orphaned),
		})
	}

	if len(chain) == 0 {
		if len(published) == 0 {
			audit.Gaps = append(audit.Gaps, CustodyGap{
				Layer: LayerExternal,
				Detail: "the store has never published a checkpoint, so no external witness exists " +
					"for any of its history",
			})
		}
		return audit, nil
	}

	if audit.LastAnchored == nil {
		return audit, nil
	}

	// Where the store stands now, against where it was last witnessed.
	current, err := s.Checkpoint()
	if err != nil {
		return audit, err
	}
	last := *audit.LastAnchored
	audit.CurrentMatchesLast = current.SnapshotRoot == last.SnapshotRoot &&
		current.AttestationID == last.AttestationID &&
		current.SegmentHead == last.SegmentHead &&
		current.AuditHead == last.AuditHead &&
		current.RedactionHead == last.RedactionHead &&
		current.GrantHead == last.GrantHead

	// **History that shrank is the fatal case.** A store moving forward is
	// ordinary; a store with fewer segments or audit entries than the anchor
	// witnessed has had history removed, and no legitimate operation does that
	// except retention — which is why the detail names it rather than leaving
	// the reader to wonder.
	if current.SegmentCount < last.SegmentCount || current.AuditCount < last.AuditCount ||
		current.RedactionCount < last.RedactionCount || current.GrantCount < last.GrantCount {
		audit.Gaps = append(audit.Gaps, CustodyGap{
			Layer: LayerExternal, Fatal: true,
			Detail: fmt.Sprintf("history shrank since checkpoint %d: %d→%d segments, %d→%d audit entries, "+
				"%d→%d redaction records. Retention can legitimately remove segments; nothing "+
				"legitimately removes audit entries, and a removed redaction record is the destruction "+
				"of the only evidence that a destruction happened",
				last.Seq, last.SegmentCount, current.SegmentCount, last.AuditCount, current.AuditCount,
				last.RedactionCount, current.RedactionCount),
		})
	}

	if !audit.CurrentMatchesLast {
		audit.Gaps = append(audit.Gaps, CustodyGap{
			Layer: LayerExternal,
			Detail: fmt.Sprintf("the store has moved on since checkpoint %d was witnessed at %s; "+
				"everything after it rests on the store's own word. Publish again to close the window",
				last.Seq, time.Unix(0, audit.LastAnchoredAt).UTC().Format(time.RFC3339)),
		})
	}

	return audit, nil
}

// CustodyForAnchor is CustodyFor with the external layer answered by an anchor
// rather than by a hash the caller typed in.
//
// CustodyForAnchored takes a bare snapshot root, which is what someone who wrote
// one down has. This form is stronger in two ways: the anchor covers all four
// histories rather than the snapshot alone, and it covers every checkpoint in
// the store's past rather than one moment.
func (s *Store) CustodyForAnchor(id store.NodeID, verifier store.Verifier, a Anchor) (CustodyReport, error) {
	r, err := s.CustodyFor(id, verifier)
	if err != nil {
		return r, err
	}

	audit, err := s.VerifyAgainstAnchor(a)
	if err != nil {
		return r, err
	}

	// Replace the placeholder gap with what the anchor actually found.
	kept := r.Gaps[:0]
	for _, g := range r.Gaps {
		if g.Layer != LayerExternal {
			kept = append(kept, g)
		}
	}
	r.Gaps = append(kept, audit.Gaps...)

	// The entity must be inside an image the anchor witnessed. Being in *an*
	// image is not the same as being in one anybody vouched for from outside.
	if r.InSnapshot && audit.LastAnchored != nil && audit.LastAnchored.SnapshotRoot != r.SnapshotRoot {
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer: LayerExternal,
			Detail: fmt.Sprintf("the entity was proved into snapshot %x, but the last witnessed "+
				"checkpoint names %x; this image has no external witness",
				r.SnapshotRoot[:8], audit.LastAnchored.SnapshotRoot[:8]),
		})
	}
	return r, nil
}

// --- a deliberately unusable implementation, for tests ---

// ErrAnchorInsideStore rejects an anchor kept where the store is.
var ErrAnchorInsideStore = errors.New("disk: an anchor inside the store directory anchors nothing")

// InsecureLocalAnchor keeps published digests in a plain file on the same
// machine.
//
// **This is not an anchor.** It is named as it is because every property that
// makes anchoring worth doing comes from the anchor being beyond the reach of
// whoever can rewrite the store, and a local file is not. It exists so the
// verification path above can be tested and demonstrated without inventing a
// network dependency the plan's Q12 rules out.
//
// The one thing it does enforce is the one thing it can: NewInsecureLocalAnchor
// refuses a path inside the store directory. That is not security, it is a
// tripwire against the most likely misuse — putting the witness in the same
// place as the thing it witnesses.
type InsecureLocalAnchor struct {
	path string
}

// NewInsecureLocalAnchor opens or creates a local anchor file at path, which
// must not be inside storeDir.
func NewInsecureLocalAnchor(path, storeDir string) (*InsecureLocalAnchor, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	absStore, err := filepath.Abs(storeDir)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(absStore, abs)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("%w: %s is inside %s", ErrAnchorInsideStore, abs, absStore)
	}
	return &InsecureLocalAnchor{path: abs}, nil
}

const anchorRecordFixed = merkle.Size + 8 + 4

// Publish appends a digest with the time this machine believes it to be.
func (l *InsecureLocalAnchor) Publish(digest merkle.Hash) (AnchorRecord, error) {
	rec := AnchorRecord{
		Digest:   digest,
		UnixNano: time.Now().UnixNano(),
		Ref:      fmt.Sprintf("local:%x", digest[:8]),
	}

	buf := make([]byte, 0, anchorRecordFixed+len(rec.Ref))
	buf = append(buf, rec.Digest[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.UnixNano))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(rec.Ref)))
	buf = append(buf, rec.Ref...)

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return AnchorRecord{}, err
	}
	defer f.Close()
	if _, err := f.Write(buf); err != nil {
		return AnchorRecord{}, err
	}
	if err := f.Sync(); err != nil {
		return AnchorRecord{}, err
	}
	return rec, nil
}

// Records returns every published digest, oldest first.
func (l *InsecureLocalAnchor) Records() ([]AnchorRecord, error) {
	data, err := os.ReadFile(l.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []AnchorRecord
	for off := 0; off+anchorRecordFixed <= len(data); {
		var rec AnchorRecord
		copy(rec.Digest[:], data[off:off+merkle.Size])
		off += merkle.Size
		rec.UnixNano = int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		n := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		if n < 0 || off+n > len(data) {
			break // torn tail
		}
		rec.Ref = string(data[off : off+n])
		off += n
		out = append(out, rec)
	}
	return out, nil
}
