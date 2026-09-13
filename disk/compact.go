package disk

// Compaction: merging the delta layer into a freshly built CSR image and
// truncating the WAL behind it.
//
// Crash safety rests on the ordering here — write and fsync a temp image, flush
// the WAL, rename atomically, then retire the log behind a checkpoint marker —
// so the steps are not independent and should not be reordered without
// rereading why each sits where it does. The marker in particular is written by
// retireLog, not before it: it means "replay stops here", which is true only of
// a log that is about to be retired.
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
//	compactCommit  under s.mu — flush, rename, retire the log, splice the delta,
//	               publish
//
// The middle stage is safe outside the lock because compactPlan shares nothing
// mutable with the store, with one deliberate exception. A CSRGraph is immutable
// once published, so the plan holds the image itself rather than a copy of its
// records and reads it in the build; the delta's records are copied under the
// lock, and a *store.Node in the delta is replaced rather than edited when it
// changes, so the Labels slice a plan holds is not written again; the redaction
// ledger is a copy taken under the lock.
//
// The exception is the property index, which the plan holds by pointer and the
// build streams -- see compactPlan.propIdx for the whole argument. It is there
// because materialising the index was the largest allocation left in a
// compaction after the image itself, and the merge below is what makes streaming
// it sound.
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
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/aoiflux/graphene/index"
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

// The durability steps of a compaction, in the order they run.
//
// These name the points where a crash produces a *different* on-disk state, not
// every statement in the sequence. That is the granularity crash-safety is
// argued at — "the image is synced before the checkpoint is written", "the
// rename happens before the log is retired" — so it is the granularity a test
// has to be able to stop at. compactStepHook fires immediately before each.
//
// Steps may be added as the sequence grows; see compactStepHook on the reason a
// hook must ignore names it does not know.
const (
	compactStepSerialise   = "serialise"    // before any of the image is produced
	compactStepWriteTmp    = "write-tmp"    // before the temp image is created
	compactStepSyncTmp     = "sync-tmp"     // written, not yet fsynced
	compactStepDrainWAL    = "drain-wal"    // before the log is flushed for its end offset
	compactStepRename      = "rename"       // before the temp image becomes the image
	compactStepSyncDir     = "sync-dir"     // renamed, directory entry not yet durable
	compactStepCheckpoint  = "checkpoint"   // before a "replay stops here" marker
	compactStepRetireWhole = "retire-whole" // branch 1: nothing landed during the build
	compactStepRetireTail  = "retire-tail"  // branch 2: the log is rebuilt over the tail
	compactStepRetireKeep  = "retire-keep"  // branch 3: the log is kept as it stands
)

// fireCompactStep runs the step hook, if one is installed.
func (s *Store) fireCompactStep(step string) error {
	if s.compactStepHook == nil {
		return nil
	}
	return s.compactStepHook(step)
}

// fireStep is the plan's copy of the same seam. build runs with no lock and no
// store reference by design — see the file comment — so the hook travels with
// the plan rather than being read back off the store from another goroutine.
func (p *compactPlan) fireStep(step string) error {
	if p.stepHook == nil {
		return nil
	}
	return p.stepHook(step)
}

// compactPlan is everything a compaction reads from the store, captured under
// the lock at a single epoch. Once built it shares nothing mutable with the
// store; see the file comment for why each field is safe to hold.
type compactPlan struct {
	// epoch is the newest applied epoch the image folds in. Everything after it
	// is a commit that landed during the build.
	epoch uint64

	// The image is a merge of two things, and only one of them has to be
	// copied.
	//
	// csr is the image the delta sits on. It is immutable once published, so
	// the build reads it directly with no lock and the plan's only obligation
	// is to keep it reachable for the length of the build. Copying its records
	// into the plan — which is what this used to do — cost one entry per live
	// record, 56 B a node and 80 B an edge, built under the store lock and held
	// beside the very image it was copied out of. Nil when the store has none.
	//
	// deltaNodes and deltaEdges are the records the delta contributes, copied
	// under the lock because the delta's maps are not immutable. deltaKnown*
	// are every identifier the delta had an opinion about at the pinned epoch,
	// tombstones included: that is what tells the merge which image records to
	// drop, and it is the only reason a tombstone survives the pin at all.
	//
	// All four are in ascending identifier order by the time the build walks
	// them — sortDelta, which runs outside the lock.
	csr             *CSRGraph
	deltaNodes      []nodeRecord
	deltaEdges      []rawEdge
	deltaKnownNodes []store.NodeID
	deltaKnownEdges []store.EdgeID

	// examined is how many records the merge walks: the image's live records
	// plus every delta entry. The compaction metric reports it, and it is a
	// figure the pin can take for free — both counts are maintained.
	examined int64

	// propIdx is the store's property index, held by pointer: the one thing in
	// this plan the store goes on mutating while the build runs. The build
	// streams it -- see nodePropSeq -- rather than the pin materialising every
	// entry, which on the store this program is aimed at was 28 million entries
	// of 48 bytes plus a copied value each, built under the store lock.
	//
	// Three things make that sound, and none of them is "the index does not
	// change":
	//
	// The index is not epoch-versioned, so the build sees entries registered
	// after the pin. Every index mutation is journaled to the WAL before it is
	// applied, under s.mu, so an entry the build happens to catch is an entry the
	// log also carries; and retireLog only truncates the log whole when nothing
	// landed during the build, in which case index-at-build is index-at-pin.
	// Otherwise the tail survives and replay re-applies each mutation as an
	// idempotent upsert or purge. So the image and the log converge on the same
	// index whichever entries the stream caught.
	//
	// An entry naming an entity the image does not hold would be an orphan
	// against invariant 15.5 until that replay, which is a worse state than
	// "stale": it is a posting pointing at nothing. That one is closed by
	// construction rather than by convergence -- nodePropSeq filters every entry
	// against the built image, which is O(1) per entry on the page table.
	//
	// And a live reader replaces its index wholesale on reload (live.go), which
	// would leave this pointer addressing an abandoned one. A live reader cannot
	// compact: Compact goes through mustWrite first.
	propIdx *index.PropertyIndex

	// indexMode is which encoding of that index the image will carry: GPIX and
	// GPIR under IndexMapped, GIDX under IndexResident.
	//
	// Taken from the option rather than from where the store's index currently
	// lives, and the difference matters on a machine that could not map. An image
	// is portable and its digest is an identity for its contents, so two parties
	// compacting the same content under the same options have to produce the same
	// bytes -- and they would not if the format followed a runtime capability
	// instead of a configured intent. A store that asked for a mapped index and
	// could not have one still writes a file that will give the next machine one;
	// what it cannot do is read this one in place, which is what the fallback
	// metric reports. IndexResident is how an operator asks for v8 back.
	indexMode IndexMode

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

	// stepHook is the store's compactStepHook, copied at the pin. Nil in
	// production. A func value, so the plan still shares nothing mutable with
	// the store.
	stepHook func(step string) error
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
// the flush, the rename, the retire — the compaction runs to completion
// whatever ctx says, because those steps are the ordering that makes a crash
// recoverable and there is no correct place to stop in the middle of them. A
// cancelled compaction therefore leaves the store exactly as it found it, minus
// a temp file the open path already ignores.
//
// This is the whole of what cancellation can usefully mean here, and it is not
// a compromise: the build is where the seconds are. The commit is an fsync, two
// renames and a pass over the delta.
func (s *Store) CompactCtx(ctx context.Context) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var started time.Time
	if s.metricsOn() {
		started = time.Now()
	}

	plan, err := s.compactPin()
	if err != nil {
		// Deliberately unrecorded. ErrCompactionInProgress and
		// ErrBackupInProgress mean no compaction happened, not that one failed,
		// and a background compactor that reports a refusal as a failure gives
		// an operator an error rate made entirely of the trigger working.
		return err
	}
	defer s.compactRelease()

	if s.afterPinHook != nil {
		s.afterPinHook()
	}

	newCSR, tmpPath, err := plan.build(ctx, s.dir)
	if err == nil {
		err = s.compactCommit(plan, newCSR, tmpPath)
	}
	if err == nil {
		s.warnIDHeadroom()
	}
	if s.metricsOn() {
		s.record(store.Metric{
			Kind:     store.MetricCompaction,
			Duration: time.Since(started),
			Count:    int64(compactedRecords(newCSR)),
			Examined: plan.examined,
			Bytes:    s.imageBytes(),
			Err:      err,
		})
	}
	return err
}

// warnIDHeadroom reports a store approaching the end of its identifier space.
//
// Called after the commit rather than inside it, with no lock held: it reads
// two atomic counters and a constant, and a metric sink is caller code that
// must not run under the store lock (store/metrics.go). A compaction that
// failed does not report — the figures would be the same, but an operator
// reading an audit chain should not find a lifetime warning filed against an
// operation that did not happen.
//
// The audit entry's failure is swallowed. The compaction succeeded; turning
// that into an error because a warning could not be filed would be reporting
// the wrong thing failed.
func (s *Store) warnIDHeadroom() {
	threshold := s.idHeadroomWarn
	if threshold < 0 {
		return
	}
	if threshold == 0 {
		threshold = defaultIDHeadroomWarn
	}
	kinds := []struct {
		name   string
		issued uint64
		warned *atomic.Bool
	}{
		{"node", s.nodeSeq.Load(), &s.nodeHeadroomWarned},
		{"edge", s.edgeSeq.Load(), &s.edgeHeadroomWarned},
	}
	for _, k := range kinds {
		if idHeadroom(k.issued) >= threshold || k.warned.Swap(true) {
			continue
		}
		detail := fmt.Sprintf("%s identifiers: %d of %d issued, %.4f%% of the space remaining",
			k.name, k.issued, uint64(maxCSREntityID), idHeadroom(k.issued)*100)
		_ = s.recordAudit(AuditIDHeadroomLow, 0, detail)
		if s.metricsOn() {
			s.record(store.Metric{
				Kind:     store.MetricIDHeadroomLow,
				Count:    int64(k.issued),
				Examined: int64(uint64(maxCSREntityID)),
			})
		}
	}
}

// compactedRecords describes the image a compaction produced,
// and answer nothing when it produced none — a build cancelled part way returns
// a nil CSR, and a metric reporting zero records for it is more honest than one
// reporting the plan's intent as though it had happened.
func compactedRecords(csr *CSRGraph) int {
	if csr == nil {
		return 0
	}
	return csr.NodeCount() + csr.EdgeCount()
}

// imageBytes is the compacted image's size on disk, or zero if it cannot be
// read. One stat per compaction, which is nothing beside the compaction, and it
// reports the file that now exists rather than the size the build intended.
func (s *Store) imageBytes() int64 {
	fi, err := os.Stat(filepath.Join(s.dir, csrFileName))
	if err != nil {
		return 0
	}
	return fi.Size()
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

	// Copy the delta, and only the delta. The image beneath it is immutable, so
	// the build walks it where it lies; see compactPlan.
	//
	// Presized from counts the delta maintains rather than grown by doubling: a
	// slice that doubles its way to n entries has touched 2n, and the point of
	// this stage is that what a compaction holds is proportional to what was
	// written since the last one.
	deltaNodes := make([]nodeRecord, 0, cur.delta.liveNodes)
	deltaEdges := make([]rawEdge, 0, cur.delta.liveEdges)
	knownNodes := make([]store.NodeID, 0, len(cur.delta.nodes))
	knownEdges := make([]store.EdgeID, 0, len(cur.delta.edges))

	// An entry the delta has an opinion about at this epoch displaces the
	// image's record, whether the opinion is an update (copied below) or a
	// tombstone (the record is gone). Either way the rebuilt CSR reclaims the
	// space and never double-counts, which is what makes compaction the point at
	// which a delete stops costing memory. This is reader.deltaNodeKnown's test,
	// run once over the map here instead of once per image record later.
	for id, ver := range cur.delta.nodes {
		n, ok := ver.at(r.epoch)
		if !ok {
			continue
		}
		knownNodes = append(knownNodes, id)
		if n == nil {
			continue
		}
		deltaNodes = append(deltaNodes, nodeRecord{ID: id, Labels: n.Labels, Properties: cloneBytes(n.Properties)})
	}
	for id, ver := range cur.delta.edges {
		e, ok := ver.at(r.epoch)
		if !ok {
			continue
		}
		knownEdges = append(knownEdges, id)
		if e == nil {
			continue
		}
		deltaEdges = append(deltaEdges, rawEdge{
			ID:         id,
			Src:        e.Src,
			Dst:        e.Dst,
			Labels:     e.Labels,
			Weight:     e.Weight,
			Properties: cloneBytes(e.Properties),
		})
	}

	examined := int64(len(cur.delta.nodes) + len(cur.delta.edges))
	if cur.csr != nil {
		examined += int64(cur.csr.NodeCount() + cur.csr.EdgeCount())
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
		epoch: r.epoch,

		csr:             cur.csr,
		deltaNodes:      deltaNodes,
		deltaEdges:      deltaEdges,
		deltaKnownNodes: knownNodes,
		deltaKnownEdges: knownEdges,
		examined:        examined,

		nodeSeqHW:   s.nodeSeq.Load(),
		edgeSeqHW:   s.edgeSeq.Load(),
		commitSeqHW: s.commitSeq.Load(),
		compactedAt: compactedAt,

		walOff:         s.wal.Size(),
		walFraming:     s.wal.Framing(),
		keyTimelineLen: len(s.keyTimeline),

		propIdx:   s.propIdx,
		indexMode: s.indexMode,

		// The image carries the property index so it no longer has to be
		// reconstructed from the WAL on the next open, and the ordered-key
		// declarations with it; without those every reopen silently turned
		// declared range queries back into scans.
		//
		// NodeProps and EdgeProps are deliberately absent here: build attaches
		// them once the image exists, because the filter that keeps them honest
		// needs it. What is left is declarations and projections -- small, and
		// the pin is the right place to read them.
		payload: csrPayload{
			CompositeNodeKeys: s.propIdx.CompositeNodeKeys(),
			CompositeEdgeKeys: s.propIdx.CompositeEdgeKeys(),
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

	plan.stepHook = s.compactStepHook

	s.compacting = true
	return plan, nil
}

// compactRelease clears the in-progress flag however the compaction ended.
func (s *Store) compactRelease() {
	s.mu.Lock()
	s.compacting = false
	s.mu.Unlock()
}

// sortDelta puts the plan's delta records and its known-identifier sets in
// ascending identifier order.
//
// That is what lets nodeSeq and edgeSeq merge against the image with two cursors
// and no lookup structure. It runs here, outside the store lock, because it is
// O(delta log delta) of pure CPU over memory the plan already owns and there is
// no reason for a writer to wait on it.
func (p *compactPlan) sortDelta() {
	slices.SortFunc(p.deltaNodes, func(a, b nodeRecord) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(p.deltaEdges, func(a, b rawEdge) int { return cmp.Compare(a.ID, b.ID) })
	slices.Sort(p.deltaKnownNodes)
	slices.Sort(p.deltaKnownEdges)
}

// nodeSeq yields the node records the new image will hold: every record in the
// pinned image the delta has no opinion about, merged with the delta's own, in
// ascending identifier order.
//
// buildSeq walks this more than once — see there for why — so it keeps no state
// between walks and starts from the beginning each time. Both inputs are already
// ascending: the image's arena is in identifier order by construction, and
// sortDelta does the rest. So a walk is two cursors over the delta and one pass
// over the image, and nothing is materialised.
//
// A record the delta knows about is dropped here and re-emitted from deltaNodes
// when the merge reaches it, which for an update is the same identifier one step
// later and for a tombstone is never.
//
// Ascending order is not something buildSeq needs; it places every record by its
// own identifier. It is what makes the adjacency arrays a compaction produces
// identical to the ones the same image produces when it is read back from disk,
// where records arrive in file order. Before this the delta's edges were
// appended in Go map order, so a freshly compacted store and a reopened one
// could disagree about the order of a node's incident edges.
func (p *compactPlan) nodeSeq() iter.Seq[nodeRecord] {
	return func(yield func(nodeRecord) bool) {
		d, k := 0, 0
		if p.csr != nil {
			for n := range p.csr.Nodes() {
				for d < len(p.deltaNodes) && p.deltaNodes[d].ID < n.ID {
					if !yield(p.deltaNodes[d]) {
						return
					}
					d++
				}
				for k < len(p.deltaKnownNodes) && p.deltaKnownNodes[k] < n.ID {
					k++
				}
				if k < len(p.deltaKnownNodes) && p.deltaKnownNodes[k] == n.ID {
					continue
				}
				if !yield(n) {
					return
				}
			}
		}
		for ; d < len(p.deltaNodes); d++ {
			if !yield(p.deltaNodes[d]) {
				return
			}
		}
	}
}

// edgeSeq is nodeSeq for edges. The two are written out rather than shared
// behind a type parameter because the merge would then take an identifier
// accessor as a function value, called once per record on a pass the build makes
// five times.
func (p *compactPlan) edgeSeq() iter.Seq[rawEdge] {
	return func(yield func(rawEdge) bool) {
		d, k := 0, 0
		if p.csr != nil {
			for e := range p.csr.Edges() {
				for d < len(p.deltaEdges) && p.deltaEdges[d].ID < e.ID {
					if !yield(p.deltaEdges[d]) {
						return
					}
					d++
				}
				for k < len(p.deltaKnownEdges) && p.deltaKnownEdges[k] < e.ID {
					k++
				}
				if k < len(p.deltaKnownEdges) && p.deltaKnownEdges[k] == e.ID {
					continue
				}
				if !yield(e) {
					return
				}
			}
		}
		for ; d < len(p.deltaEdges); d++ {
			if !yield(p.deltaEdges[d]) {
				return
			}
		}
	}
}

// nodePropSeq yields the property-index entries the new image will carry: every
// entry the live index holds for a node the image holds, in the (key, value, id)
// order the format's byte-determinism contract requires.
//
// The filter is the point. The stream runs during the build, so it can see an
// entry registered for a node committed after the pin -- a node this image does
// not contain. Writing it would put a posting into the image naming nothing,
// which is exactly the orphan invariant 15.5 forbids, and it would stay that way
// until the log replayed. csr.containsNode is one page-table probe against the
// image the entries are being written for, so the section cannot contain one.
//
// The other direction needs no filter: an entry the stream misses because its
// key was already walked is in the log tail, and replay registers it.
//
// Values are not copied. ForEachNodeProperty owns the bytes it yields for the
// duration of the call, the writer copies them into its output buffer and the
// Merkle stream hashes them immediately, and nothing else here retains them --
// which is the allocation this item removes, one per entry.
//
// Re-runnable, as csrPayload requires: each walk re-enters the index.
func (p *compactPlan) nodePropSeq(csr *CSRGraph) iter.Seq[index.NodePropEntry] {
	return func(yield func(index.NodePropEntry) bool) {
		p.propIdx.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
			if !csr.containsNode(id) {
				return true
			}
			return yield(index.NodePropEntry{ID: id, Key: key, Value: value})
		})
	}
}

// edgePropSeq is nodePropSeq for edge properties.
func (p *compactPlan) edgePropSeq(csr *CSRGraph) iter.Seq[index.EdgePropEntry] {
	return func(yield func(index.EdgePropEntry) bool) {
		p.propIdx.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
			if !csr.containsEdge(id) {
				return true
			}
			return yield(index.EdgePropEntry{ID: id, Key: key, Value: value})
		})
	}
}

// mappedIndexSource is nodePropSeq and edgePropSeq in the shape GPIX is written
// from: the same entries, the same filter against the built image, grouped by
// value instead of flattened to one triple each.
//
// Grouped because that is what the format is: a value is stored once with its id
// list, so the writer needs the group rather than having to detect the boundary
// in a flat stream. The index has the grouping already — a posting list *is* the
// group — so this is the cheaper walk of the two, not a concession to the format.
//
// The ids handed to the encoder are a buffer this reuses per value. The encoder
// writes them and hands each to the reverse sorter by value, retaining nothing,
// which is why one buffer does: it grows to the widest value's id list.
//
// Two buffers rather than one because the encoder walks node keys and edge keys
// in sequence today and nothing in the format requires it to keep doing so.
func (p *compactPlan) mappedIndexSource(csr *CSRGraph, dir string) *gpixSource {
	nodeWalk := p.propIdx.NodeValueWalker()
	edgeWalk := p.propIdx.EdgeValueWalker()
	var nodeIDs, edgeIDs []uint64
	return &gpixSource{
		NodeKeys: p.propIdx.NodePropKeys(),
		EdgeKeys: p.propIdx.EdgePropKeys(),
		NodeValues: func(key string, fn func(value []byte, ids []uint64) bool) error {
			nodeWalk(key, func(value []byte, ids []store.NodeID) bool {
				nodeIDs = nodeIDs[:0]
				for _, id := range ids {
					if csr.containsNode(id) {
						nodeIDs = append(nodeIDs, uint64(id))
					}
				}
				// A value every one of whose holders the image dropped is not in
				// the image's index. The encoder skips an empty list for the same
				// reason, and says so there.
				if len(nodeIDs) == 0 {
					return true
				}
				return fn(value, nodeIDs)
			})
			return nil
		},
		EdgeValues: func(key string, fn func(value []byte, ids []uint64) bool) error {
			edgeWalk(key, func(value []byte, ids []store.EdgeID) bool {
				edgeIDs = edgeIDs[:0]
				for _, id := range ids {
					if csr.containsEdge(id) {
						edgeIDs = append(edgeIDs, uint64(id))
					}
				}
				if len(edgeIDs) == 0 {
					return true
				}
				return fn(value, edgeIDs)
			})
			return nil
		},
		// The store's own directory, so an index too large to sort in memory
		// spills onto the filesystem whose free space was sized for the image it
		// is being written beside — not onto whatever /tmp happens to be.
		ScratchDir: dir,
	}
}

// build turns the plan into a serialised image on disk, with no lock held.
//
// A failure here changes nothing: the log is intact, the image on disk is the
// previous one, and the store's view has not moved.
//
// It also leaves no temp file. The open path ignores a stray .tmp and always
// has, so correctness never depended on removing it — but a failed compaction
// of a large store leaves a temp file the size of the image, and the most
// likely reason a compaction fails is that the disk is full. Keeping the
// carcass around turns one recoverable failure into a machine with no room to
// retry on. Removal is best-effort for exactly that reason: if it fails, the
// open path is still correct, so there is nothing further to report.
func (p *compactPlan) build(ctx context.Context, dir string) (*CSRGraph, string, error) {
	// Ascending order for the merge below. Pure CPU over memory the plan owns,
	// and deliberately not done at the pin: this is the stage that holds no
	// lock.
	p.sortDelta()

	// buildSeq refuses a duplicate identifier. Nothing is on disk yet, so the
	// no-temp-file contract above holds for this path too.
	//
	// What comes out of it shares the pinned image's property bytes: buildSeq
	// copies record values, and a record value is two slice headers. So when the
	// image is mapped, the graph this compaction is about to publish addresses
	// that mapping for every record the delta did not touch -- and so does the
	// one after it, having been built from records that already do.
	//
	// That is the whole reason a compaction neither creates nor retires a
	// mapping. Unmapping the image a compaction replaced would be a
	// use-after-unmap on the graph it just published; keeping each mapping until
	// its dependents were gone would pin the first one for the life of the store
	// and add one per compaction. So the store maps once at Open and holds it
	// until Close, and a compaction writes a file. See mapping.go.
	newCSR, err := buildSeq(p.nodeSeq(), p.edgeSeq())
	if err != nil {
		return nil, "", fmt.Errorf("compact: %w", err)
	}
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

	// The property index travels as a stream over the live index, filtered
	// against the image that has just been built. It is attached here rather
	// than at the pin for two reasons: the filter needs the image, and a
	// cancelled build should not have walked the index at all.
	//
	// One encoding or the other, never both: they carry the same entries, and an
	// image holding a second copy no reader of either kind would open is the
	// largest single thing this program has taken out of a compaction. Which one
	// is the mode's decision, made at the pin -- see compactPlan.indexMode -- and
	// it is what stamps the image v8 or v9.
	if p.indexMode == IndexMapped {
		p.payload.MappedIndex = p.mappedIndexSource(newCSR, dir)
	} else {
		p.payload.NodeProps = p.nodePropSeq(newCSR)
		p.payload.EdgeProps = p.edgePropSeq(newCSR)
	}

	// The last place a cancellation is free. Past this the image is written and
	// fsynced, and the caller has paid for it whether or not it is installed.
	//
	// This used to sit *after* serialisation, because serialisation was a
	// separate step that produced an image-sized []byte for the write to
	// consume. The write is the serialisation now — SerialiseTo streams into
	// the temp file — so the last free moment is before it rather than between
	// the two.
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := p.fireStep(compactStepSerialise); err != nil {
		return nil, "", err
	}

	tmpPath := filepath.Join(dir, csrFileName+".tmp")
	// Synced, not merely written. The checkpoint below is itself fsynced and
	// says "everything before this record is in the image"; if the image's
	// blocks are still in the page cache when the power goes, that marker is a
	// durable lie. The sync has to land before the checkpoint, not after.
	if err := p.fireStep(compactStepWriteTmp); err != nil {
		// Before the file exists, so there is nothing to clean up. The step is
		// still worth stopping at: it is the boundary between "the compaction
		// touched no file in the directory" and "it did".
		return nil, "", err
	}
	beforeSync := func() error { return p.fireStep(compactStepSyncTmp) }
	write := func(f *os.File) error { return newCSR.SerialiseTo(f, p.payload) }
	if err := writeStreamSync(tmpPath, 0600, beforeSync, write); err != nil {
		// This is the only statement in build that can have created the file,
		// so cleaning up here covers every way build can fail with a temp file
		// on disk. A partial write counts: the file is truncated on open, so a
		// failure part way through leaves a short file under the real name.
		// A signer error arrives here too, now that signing happens inside the
		// write rather than before it.
		removeTmpImage(tmpPath)
		return nil, "", fmt.Errorf("compact: write tmp CSR: %w", err)
	}
	return newCSR, tmpPath, nil
}

// removeTmpImage deletes a temp image that will never be installed.
//
// Best-effort by design. The caller is already returning an error, and the open
// path treats a stray .tmp as absent, so a failure to remove costs disk space
// and nothing else — reporting it would replace the error that actually
// explains the failure with one about the cleanup after it.
func removeTmpImage(tmpPath string) {
	if tmpPath == "" {
		return
	}
	_ = os.Remove(tmpPath)
}

// compactCommit installs the built image and retires the log behind it.
func (s *Store) compactCommit(p *compactPlan, newCSR *CSRGraph, tmpPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the log and take its end offset, then rename. The offset is the end
	// of every record the log held, which is what the retire branches below need
	// in order to tell the image's records from the tail's.
	//
	// Neither the *checkpoint marker* nor the fsync happens here; both belong to
	// the branch. The marker says "replay stops at this point", and one branch
	// keeps the log and goes on appending to it — writing it up front stranded
	// every record committed after any compaction that took that branch. The
	// fsync follows the marker for the two branches that write one, and is
	// explicit in the third, which keeps a compaction at one fsync rather than
	// two.
	if err := s.fireCompactStep(compactStepDrainWAL); err != nil {
		removeTmpImage(tmpPath)
		return err
	}
	endOff, err := s.wal.drainedEnd()
	if err != nil {
		// Nothing has been installed, so the built image is now unreachable.
		// Same reasoning as in build: an image-sized file left behind by a
		// failure whose likeliest cause is a full disk.
		removeTmpImage(tmpPath)
		return fmt.Errorf("compact: wal flush: %w", err)
	}

	csrPath := filepath.Join(s.dir, csrFileName)
	if err := s.fireCompactStep(compactStepRename); err != nil {
		removeTmpImage(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, csrPath); err != nil {
		removeTmpImage(tmpPath)
		return fmt.Errorf("compact: rename CSR: %w", err)
	}
	// Past this point the temp name no longer exists: the rename consumed it.
	// Every later failure in this function leaves the *new* image installed and
	// the log un-retired, which is the case the open path recovers by replay.
	// And the rename itself. os.Rename is atomic with respect to a concurrent
	// reader — either name resolves to one whole file or the other — but that
	// is a different property from surviving a power loss, which needs the
	// directory's own entries flushed. Before the WAL is retired below, because
	// the log is what recovers the store if this fails.
	if err := s.fireCompactStep(compactStepSyncDir); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("compact: %w", err)
	}

	if err := s.retireLog(p, newCSR, endOff); err != nil {
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
// endOff, which are commits the image does not hold and which exist nowhere
// else on disk.
//
// Each branch is also responsible for its own checkpoint marker, and the third
// deliberately writes none. A marker means "replay stops here", which is only
// true of a log about to be retired; left in a log that keeps being appended
// to, it silently discards every record written after this compaction.
func (s *Store) retireLog(p *compactPlan, newCSR *CSRGraph, endOff int64) error {
	tail := endOff - p.walOff

	switch {
	// Nothing landed during the build. The log holds exactly what the image
	// holds, so it is retired whole, exactly as it always was.
	case tail <= 0:
		if err := s.fireCompactStep(compactStepRetireWhole); err != nil {
			return err
		}
		if err := s.fireCompactStep(compactStepCheckpoint); err != nil {
			return err
		}
		if _, err := s.wal.checkpointAt(); err != nil {
			return fmt.Errorf("compact: wal checkpoint: %w", err)
		}
		if s.retention.Keeps() {
			return s.rotateLog(newCSR)
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
				newCSR.NodeCount(), newCSR.EdgeCount(), newCSR.roots.Snapshot[:8])); aerr != nil {
			return fmt.Errorf("compact: %w", aerr)
		}
		return nil

	// A tail, and a log that can carry it: rebuild the log holding the tail
	// alone. This is the steady state for a store being compacted under load,
	// and it is what keeps the invariant the rest of the engine reads as
	// obvious — the image holds every epoch up to the pin, the log holds every
	// epoch after it, and neither holds both.
	case !s.retention.Keeps() && p.walFraming == walFramingV2:
		if err := s.fireCompactStep(compactStepRetireTail); err != nil {
			return err
		}
		if err := s.fireCompactStep(compactStepCheckpoint); err != nil {
			return err
		}
		markerOff, err := s.wal.checkpointAt()
		if err != nil {
			return fmt.Errorf("compact: wal checkpoint: %w", err)
		}
		if err := s.wal.truncateFrom(p.walOff, markerOff); err != nil {
			return fmt.Errorf("compact: %w", err)
		}
		// The rotations the retire discarded are the ones at or before the pin;
		// whatever was recorded during the build is in the carried tail and so
		// is still "seen in the current log".
		s.keyTimeline = append([]KeyTransition(nil), s.keyTimeline[p.keyTimelineLen:]...)
		if aerr := s.recordAudit(AuditCompact, s.attestActorID,
			fmt.Sprintf("log rebuilt over %d bytes committed during the build; %d nodes, %d edges; snapshot %x",
				tail, newCSR.NodeCount(), newCSR.EdgeCount(), newCSR.roots.Snapshot[:8])); aerr != nil {
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
	//
	// And no checkpoint marker, which is the whole reason the marker moved out
	// of compactCommit. This log outlives the compaction and goes on being
	// appended to; a marker in the middle of it means replay stops there, so
	// every record written after this compaction would be read back as though
	// it had never been committed. The fsync the marker used to bring with it is
	// still needed and is issued on its own: the tail commits have to be durable
	// before the epoch covering them is published.
	default:
		if err := s.fireCompactStep(compactStepRetireKeep); err != nil {
			return err
		}
		if err := s.wal.Sync(); err != nil {
			return fmt.Errorf("compact: wal sync: %w", err)
		}
		if aerr := s.recordAudit(AuditCompact, s.attestActorID,
			fmt.Sprintf("log kept, %d bytes committed during the build could not be carried; %d nodes, %d edges; snapshot %x",
				tail, newCSR.NodeCount(), newCSR.EdgeCount(), newCSR.roots.Snapshot[:8])); aerr != nil {
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
func (s *Store) rotateLog(newCSR *CSRGraph) error {
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
			seg.Sequence, newCSR.NodeCount(), newCSR.EdgeCount(), newCSR.roots.Snapshot[:8])); aerr != nil {
		return fmt.Errorf("compact: %w", aerr)
	}
	return nil
}
