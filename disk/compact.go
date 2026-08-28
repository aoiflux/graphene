package disk

// Compaction: merging the delta layer into a freshly built CSR image and
// truncating the WAL behind it.
//
// Crash safety rests on the ordering here — write a temp file, checkpoint the
// WAL, rename atomically, then retire the log — so the steps are not
// independent and should not be reordered without rereading why each sits where
// it does.
//
// # Why this is three functions
//
// It used to be one, holding the store's write lock from the first map read to
// the last publish. Everything a writer did waited on the whole of it: an O(n)
// scan, a sort, a Merkle pass over every node, edge, index entry and tombstone,
// a whole-image serialisation, and a whole-image write and fsync. On any store
// large enough to be worth compacting, the last two of those dominate, and
// neither touches store state.
//
// So the work splits at the point where it stops reading the store:
//
//	compactPin     under s.mu — collect the records and everything else the
//	               image is built from, at one epoch
//	build          no lock    — sort, hash, serialise, write, fsync
//	compactCommit  under s.mu — checkpoint, rename, retire the log, splice the
//	               delta, publish
//
// The middle stage is safe outside the lock because compactPlan shares nothing
// mutable with the store. The record slices are freshly built; a *store.Node in
// the delta is replaced rather than edited when it changes, so the Labels slice
// a plan holds is not written again; a CSRGraph is immutable once published;
// and the property entries and redaction ledger are copies taken under the lock.
//
// # What the released lock costs
//
// Two things, and both are handled below rather than assumed away.
//
// Commits land during the build. They are in the log and in the delta, but not
// in the image, so the log cannot simply be emptied behind them — see the
// retire branches. And they are in the delta *beside* the versions the image
// just absorbed, so publishing an empty delta layer would lose them — see
// deltaLayer.since.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// ErrCompactionInProgress is returned by Compact when one is already running.
//
// Compactions were mutually exclusive by construction while the store lock was
// held throughout. It is now an explicit refusal rather than a queue: two
// compactions of the same store do not produce a better image than one, and the
// second would spend a full build to write the same bytes.
var ErrCompactionInProgress = errors.New("graphene: a compaction is already running")

// compactPlan is everything a compaction reads from the store, captured under
// the lock at a single epoch. Once built it shares nothing mutable with the
// store; see the file comment for why each field is safe to hold.
type compactPlan struct {
	// epoch is the newest applied epoch the image folds in. Everything after it
	// is a commit that landed during the build.
	epoch uint64

	nodes []nodeRecord
	edges []rawEdge

	payload csrPayload

	nodeSeqHW   uint64
	edgeSeqHW   uint64
	commitSeqHW uint64
	compactedAt time.Time

	// walOff is where the log stood when the plan was pinned, so records before
	// it are in the image and records after it are not. walFraming is the log's
	// framing at that moment, which decides whether a tail can be carried.
	walOff     int64
	walFraming uint16

	// keyTimelineLen is how many rotations the in-memory timeline held at the
	// pin. Anything appended past it happened during the build and belongs to
	// the log that survives the retire.
	keyTimelineLen int
}

// Compact merges the delta layer into the CSR and truncates the WAL.
// This should be called after a bulk ingest is complete.
// Compact is crash-safe: it writes a temp CSR file then atomically renames it.
//
// Writers are blocked only for the pin and the commit. A reader is never
// blocked by a compaction beyond the moment the new view is published.
func (s *Store) Compact() error {
	return s.CompactCtx(context.Background())
}

// CompactCtx is Compact, abandoned if ctx is cancelled.
//
// Cancellation reaches the build and stops there. Once the commit begins —
// the checkpoint, the rename, the retire — the compaction runs to completion
// whatever ctx says, because those steps are the ordering that makes a crash
// recoverable and there is no correct place to stop in the middle of them. A
// cancelled compaction therefore leaves the store exactly as it found it, minus
// a temp file the open path already ignores.
//
// This is the whole of what cancellation can usefully mean here, and it is not
// a compromise: the build is where the seconds are. The commit is a checkpoint
// fsync, two renames and a pass over the delta.
func (s *Store) CompactCtx(ctx context.Context) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	plan, err := s.compactPin()
	if err != nil {
		return err
	}
	defer s.compactRelease()

	if s.afterPinHook != nil {
		s.afterPinHook()
	}

	newCSR, tmpPath, err := plan.build(ctx, s.dir)
	if err != nil {
		return err
	}

	return s.compactCommit(plan, newCSR, tmpPath)
}

// compactPin captures the image's inputs at one epoch.
func (s *Store) compactPin() (*compactPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.compacting {
		return nil, ErrCompactionInProgress
	}
	// A backup is copying the image and the log as a matched pair. Compacting
	// underneath one would replace the image and retire the log while the copy
	// had only reached one of them, which is the single way to produce an
	// inconsistent copy of an otherwise append-only directory. See backup.go.
	if s.backups > 0 {
		return nil, ErrBackupInProgress
	}

	// Everything is read through one reader at the newest applied epoch, so the
	// image being built is the graph as it stood at a single instant rather than
	// a walk over fields that could each have moved.
	r := s.writerLocked()
	cur := r.v

	// Collect all nodes and edges from both CSR and delta.
	var nodes []nodeRecord
	var edges []rawEdge

	// From existing CSR — skip entries the delta has an opinion about, whether
	// that is an update (the delta copy is emitted below) or a tombstone (the
	// record is gone). Either way the rebuilt CSR reclaims the space and never
	// double-counts.
	if cur.csr != nil {
		for i := 1; i < len(cur.csr.nodes); i++ {
			n := cur.csr.nodes[i]
			if n.ID == store.InvalidNodeID {
				continue
			}
			if r.deltaNodeKnown(n.ID) {
				continue
			}
			nodes = append(nodes, n)
		}
		for i := 1; i < len(cur.csr.edges); i++ {
			e := cur.csr.edges[i]
			if e.ID == store.InvalidEdgeID {
				continue
			}
			if r.deltaEdgeKnown(e.ID) {
				continue
			}
			edges = append(edges, e)
		}
	}

	// From delta. A tombstoned entry resolves to nil and is simply not carried
	// forward, which is what makes compaction the point at which a delete stops
	// costing memory.
	for id, ver := range cur.delta.nodes {
		n, ok := ver.at(r.epoch)
		if !ok || n == nil {
			continue
		}
		nodes = append(nodes, nodeRecord{ID: id, Labels: n.Labels, Properties: cloneBytes(n.Properties)})
	}
	for id, ver := range cur.delta.edges {
		e, ok := ver.at(r.epoch)
		if !ok || e == nil {
			continue
		}
		edges = append(edges, rawEdge{
			ID:         id,
			Src:        e.Src,
			Dst:        e.Dst,
			Labels:     e.Labels,
			Weight:     e.Weight,
			Properties: cloneBytes(e.Properties),
		})
	}

	// Chain this image to the one it replaces, so the sequence of compactions is
	// itself verifiable. A substituted snapshot breaks the link even when the
	// substitute is internally consistent. The attestation chains the same way,
	// so a removed attestation is provably missing rather than invisibly absent.
	var prevRoot merkle.Hash
	var prevAttestation [attestationIDSize]byte
	if cur.csr != nil {
		if roots, ok := cur.csr.Roots(); ok {
			prevRoot = roots.Snapshot
		}
		prevAttestation = cur.csr.attestation.ID
	}

	// Tombstones are rebuilt from the ledger rather than carried forward from the
	// previous image, so the image and the ledger cannot drift into disagreeing
	// about what was destroyed. The ledger is the record; this is its projection.
	//
	// Read from disk rather than from s.redactions so that a store reopened with
	// Options.Redaction off still carries forward the removals already recorded —
	// switching the option off must not quietly drop them out of the image.
	//
	// Read here rather than in the build so that the image is a projection of the
	// ledger as it stood at the pinned epoch, and not of one a redaction issued
	// during the build has already moved on from.
	ledger, err := ReadRedactions(s.dir)
	if err != nil {
		return nil, fmt.Errorf("compact: reading the redaction ledger: %w", err)
	}

	// v8 carries two marks across the truncation that follows.
	//
	// Compaction empties the WAL, and until v8 that took the commit sequence and
	// the last compaction time with it: the counter restarted from zero on the
	// next open, and nothing could say when the image was built. Both now survive
	// in the header, which is what makes a commit sequence number a durable
	// identity rather than an ordering within one log generation.
	compactedAt := time.Now()

	// Draining first is not required — every byte in the file at this moment
	// belongs to an epoch at or before the pin, because epochs are allocated
	// under this lock — but it moves the boundary forward. Bytes still queued
	// are written after the offset is taken and end up in the carried tail,
	// where replay applies them a second time on top of an image that already
	// holds them. That is harmless, being upserts, and worth avoiding.
	_ = s.wal.FlushQueued()

	plan := &compactPlan{
		epoch:       r.epoch,
		nodes:       nodes,
		edges:       edges,
		nodeSeqHW:   s.nodeSeq.Load(),
		edgeSeqHW:   s.edgeSeq.Load(),
		commitSeqHW: s.commitSeq.Load(),
		compactedAt: compactedAt,

		walOff:         s.wal.Size(),
		walFraming:     s.wal.Framing(),
		keyTimelineLen: len(s.keyTimeline),

		// Carrying the property index into the CSR so it no longer has to be
		// reconstructed from the WAL on the next open. Ordered-key declarations
		// travel with the image too; without them every reopen silently turned
		// declared range queries back into scans.
		payload: csrPayload{
			NodeProps:         s.propIdx.NodeEntries(),
			EdgeProps:         s.propIdx.EdgeEntries(),
			OrderedNodeKeys:   s.propIdx.OrderedNodeKeys(),
			OrderedEdgeKeys:   s.propIdx.OrderedEdgeKeys(),
			PrevSnapshotRoot:  prevRoot,
			WithSnapshotRoots: true,
			Tombstones:        tombstonesFromLedger(ledger),
			Signer:            s.signer,
			AttestActorID:     s.attestActorID,
			AttestUnixNano:    compactedAt.UnixNano(),
			PrevAttestation:   prevAttestation,
		},
	}

	s.compacting = true
	return plan, nil
}

// compactRelease clears the in-progress flag however the compaction ended.
func (s *Store) compactRelease() {
	s.mu.Lock()
	s.compacting = false
	s.mu.Unlock()
}

// build turns the plan into a serialised image on disk, with no lock held.
//
// A failure here leaves a stray temp file and nothing else: the log is intact,
// the image on disk is the previous one, and the store's view has not moved.
// That is the same state the existing "a stray .tmp is ignored" open path
// already handles.
func (p *compactPlan) build(ctx context.Context, dir string) (*CSRGraph, string, error) {
	newCSR := Build(p.nodes, p.edges)
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}

	// Persist the current sequence high-water marks so a subsequent reopen never
	// reuses an ID whose record was dropped from this rebuilt CSR. The marks are
	// the pinned ones; a commit that landed during the build carries its own
	// higher IDs in the log tail, and replay raises the counters from those.
	newCSR.nodeSeqHW = p.nodeSeqHW
	newCSR.edgeSeqHW = p.edgeSeqHW
	newCSR.commitSeqHW = p.commitSeqHW
	newCSR.lastCompactUnixNano = p.compactedAt.UnixNano()

	data, err := newCSR.SerialiseWithPayload(p.payload)
	if err != nil {
		return nil, "", fmt.Errorf("compact: %w", err)
	}
	// The last place a cancellation is free. Past this the image is written and
	// fsynced, and the caller has paid for it whether or not it is installed.
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}

	tmpPath := filepath.Join(dir, csrFileName+".tmp")
	// Synced, not merely written. The checkpoint below is itself fsynced and
	// says "everything before this record is in the image"; if the image's
	// blocks are still in the page cache when the power goes, that marker is a
	// durable lie. The sync has to land before the checkpoint, not after.
	if err := writeFileSync(tmpPath, data, 0600); err != nil {
		return nil, "", fmt.Errorf("compact: write tmp CSR: %w", err)
	}
	return newCSR, tmpPath, nil
}

// compactCommit installs the built image and retires the log behind it.
func (s *Store) compactCommit(p *compactPlan, newCSR *CSRGraph, tmpPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Checkpoint WAL then atomic rename. The offset the marker went down at is
	// the end of every record the log held, which is what the retire branches
	// below need in order to tell the image's records from the tail's.
	markerOff, err := s.wal.checkpointAt()
	if err != nil {
		return fmt.Errorf("compact: wal checkpoint: %w", err)
	}

	csrPath := filepath.Join(s.dir, csrFileName)
	if err := os.Rename(tmpPath, csrPath); err != nil {
		return fmt.Errorf("compact: rename CSR: %w", err)
	}
	// And the rename itself. os.Rename is atomic with respect to a concurrent
	// reader — either name resolves to one whole file or the other — but that
	// is a different property from surviving a power loss, which needs the
	// directory's own entries flushed. Before the WAL is retired below, because
	// the log is what recovers the store if this fails.
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("compact: %w", err)
	}

	if err := s.retireLog(p, newCSR, markerOff); err != nil {
		return err
	}

	s.lastCompact = p.compactedAt

	// Swap in the new image under the delta that survived it. Both halves change
	// together because they are one value; the old view is left exactly as it
	// is rather than cleared, so a snapshot still reading it keeps working and
	// the garbage collector reclaims it when the last one closes.
	spliced, shadowed := s.cur().delta.since(p.epoch, newCSR)
	s.publishCompacted(newCSR, spliced, shadowed)

	// Everything folded into the image is durable in it, including any commit
	// that had been written but was still waiting on its fsync when this
	// started: the image was synced and the log checkpointed above. Publishing
	// the applied epoch is what stops those commits being stranded invisible
	// after the log that held them was retired.
	s.publishEpoch(s.mutEpoch.Load())

	return nil
}

// retireLog empties or rotates the log now that the image holds its records.
//
// Either way this happens *after* the CSR has been renamed into place, so a
// crash between the two leaves a log that still holds everything the new image
// already contains. Replaying it again is harmless; losing it would not be.
//
// The three branches are about the tail — the bytes written between the pin and
// the checkpoint, which are commits the image does not hold and which exist
// nowhere else on disk.
func (s *Store) retireLog(p *compactPlan, newCSR *CSRGraph, markerOff int64) error {
	tail := markerOff - p.walOff

	switch {
	// Nothing landed during the build. The log holds exactly what the image
	// holds, so it is retired whole, exactly as it always was.
	case tail <= 0:
		if s.retention.Keeps() {
			return s.rotateLog(newCSR, len(p.nodes), len(p.edges))
		}
		if err := s.wal.Truncate(); err != nil {
			return fmt.Errorf("compact: wal truncate: %w", err)
		}
		// Rotation and truncation both start a fresh log, so the rotations
		// recorded in the retired one are no longer in the live timeline. They
		// remain readable in a retained segment, which is the point of keeping
		// one.
		s.keyTimeline = nil
		if aerr := s.recordAudit(AuditCompact, s.attestActorID,
			fmt.Sprintf("log discarded; %d nodes, %d edges; snapshot %x",
				len(p.nodes), len(p.edges), newCSR.roots.Snapshot[:8])); aerr != nil {
			return fmt.Errorf("compact: %w", aerr)
		}
		return nil

	// A tail, and a log that can carry it: rebuild the log holding the tail
	// alone. This is the steady state for a store being compacted under load,
	// and it is what keeps the invariant the rest of the engine reads as
	// obvious — the image holds every epoch up to the pin, the log holds every
	// epoch after it, and neither holds both.
	case !s.retention.Keeps() && p.walFraming == walFramingV2:
		if err := s.wal.truncateFrom(p.walOff, markerOff); err != nil {
			return fmt.Errorf("compact: %w", err)
		}
		// The rotations the retire discarded are the ones at or before the pin;
		// whatever was recorded during the build is in the carried tail and so
		// is still "seen in the current log".
		s.keyTimeline = append([]KeyTransition(nil), s.keyTimeline[p.keyTimelineLen:]...)
		if aerr := s.recordAudit(AuditCompact, s.attestActorID,
			fmt.Sprintf("log rebuilt over %d bytes committed during the build; %d nodes, %d edges; snapshot %x",
				tail, len(p.nodes), len(p.edges), newCSR.roots.Snapshot[:8])); aerr != nil {
			return fmt.Errorf("compact: %w", aerr)
		}
		return nil

	// A tail this cannot carry, so the log is left alone.
	//
	// Retention retires the whole file to a numbered segment, and replay reads
	// only the active log — so carrying the tail forward would mean the segment
	// chain and the active log both claiming the same records, with a window
	// between the two renames in which a crash leaves the tail in neither.
	// A pre-container log cannot carry a tail at all: its records are framed v1
	// and the rebuilt log's header declares v2, and a file holding two framings
	// is a file whose second half fails its own CRCs.
	//
	// Leaving the log is always safe — its records are in the image as well, so
	// replay applies them a second time as upserts. The cost is that this
	// compaction reclaims no log space, which the next one does once the store
	// is quiet. Recorded in the audit because an operator watching the log fail
	// to shrink deserves the reason.
	default:
		if aerr := s.recordAudit(AuditCompact, s.attestActorID,
			fmt.Sprintf("log kept, %d bytes committed during the build could not be carried; %d nodes, %d edges; snapshot %x",
				tail, len(p.nodes), len(p.edges), newCSR.roots.Snapshot[:8])); aerr != nil {
			return fmt.Errorf("compact: %w", aerr)
		}
		return nil
	}
}

// rotateLog retires the active log to a numbered segment and applies retention.
//
// With retention configured the log is kept rather than truncated. The
// distinction is what a caller asked for, not what the engine thinks best — how
// long evidence is held is not the engine's decision.
func (s *Store) rotateLog(newCSR *CSRGraph, nodes, edges int) error {
	seg, err := s.wal.Rotate(s.dir, s.segmentSeq)
	if err != nil {
		return fmt.Errorf("compact: wal rotate: %w", err)
	}
	s.segmentSeq++

	removed, err := applyRetention(s.dir, s.retention)
	if err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	// Recorded because it is a deletion of evidence, whoever authorised it.
	// A retention policy quietly discarding history is the thing an audit is
	// for, even — especially — when the discarding was intended.
	for _, r := range removed {
		if aerr := s.recordAudit(AuditRetentionDelete, s.attestActorID,
			fmt.Sprintf("segment %d (%d bytes, digest %x)", r.Sequence, r.Bytes, r.Digest[:8])); aerr != nil {
			return fmt.Errorf("compact: %w", aerr)
		}
	}

	s.keyTimeline = nil

	if aerr := s.recordAudit(AuditCompact, s.attestActorID,
		fmt.Sprintf("retired segment %d; %d nodes, %d edges; snapshot %x",
			seg.Sequence, nodes, edges, newCSR.roots.Snapshot[:8])); aerr != nil {
		return fmt.Errorf("compact: %w", aerr)
	}
	return nil
}
