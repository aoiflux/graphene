package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// Store is the on-disk implementation of store.GraphStore.
//
// Architecture:
//   - New writes go to WAL + delta (in-memory map) immediately.
//   - Reads merge CSR (bulk data after last compaction) + delta layer.
//   - Compact() rebuilds the CSR from the merged set and truncates the WAL.
//
// This is optimised for bulk ingest → read-heavy workloads. CSR provides
// O(degree) neighbourhood queries with sequential memory access.
type Store struct {
	mu  sync.RWMutex
	dir string
	wal *WAL

	// viewPtr is the CSR image and the delta layer above it, as one value. See
	// view.go for why the two travel together and why the epoch does not.
	//
	// Replaced only by Compact and only under the write lock, so every read
	// under s.mu sees one view for the whole of its work. Loaded without the
	// lock by the point-read fast path and by Snapshot.
	viewPtr atomic.Pointer[view]

	// mutEpoch stamps delta mutations; visibleEpoch is how far a reader may see.
	//
	// They are the same number in the steady state and differ only while a
	// commit is written but not yet durable. That gap is the whole of the
	// group-commit design: a batch appends its records and stamps them under the
	// lock, releases the lock, waits for the fsync that covers them, and only
	// then advances visibleEpoch. Because visibility is the epoch and not the
	// map write, no reader can observe a commit that a crash would take back.
	//
	// Single-record mutators, which have never fsynced (see Sync), advance both
	// under the lock: their durability boundary is the next Sync, exactly as
	// documented, and making them invisible until then would break
	// read-your-writes for no gain.
	mutEpoch     atomic.Uint64
	visibleEpoch atomic.Uint64

	// unpublished counts commits that have applied their records and released
	// the lock but have not yet advanced visibleEpoch — that is, the group-commit
	// gap described above, made observable to the write path.
	//
	// It exists because version retention has to know about it. While it is
	// non-zero a reader can still be running at an older epoch, so a superseded
	// version and the postings beside it are still needed; while it is zero the
	// gap is closed before the lock is released and nothing can observe one. See
	// retainLocked, which is the only reader of this.
	unpublished atomic.Int64

	// snaps tracks open snapshots so version chains know how far back to keep
	// history, and so an operator can see a leaked one. Guarded by mu.
	snaps snapshotRegistry

	// --- lock-free read support ---
	//
	// A CSRGraph is immutable once published, so a reader that gets the pointer
	// atomically can read a record from it without holding s.mu. The lock is
	// still needed for the delta maps, which are ordinary Go maps — but those
	// only *shadow* CSR records, they never rewrite them.
	//
	// csrShadowed counts CSR records that a delta update or a tombstone has
	// superseded within the current epoch. While it is zero, every CSR record is
	// still the truth, so a point read that finds its answer in the CSR needs no
	// lock at all. Crucially, appending new entities does not shadow anything, so
	// ongoing writes do not disable the fast path for pre-existing records.
	//
	// The counter is only ever incremented within the life of one CSR (Compact
	// resets it when it publishes the next), so reading zero after a lookup proves
	// it was zero throughout. A reader also re-checks the CSR *pointer* it read
	// from, which is what catches a Compact that swapped the CSR and cleared the
	// counter underneath it.
	//
	// The validity check compares the CSR, not the view: a view is replaced by
	// every compaction *and* is the thing Snapshot pins, but a plain commit
	// leaves v.csr identical. Comparing the image is what keeps a concurrent
	// writer from knocking every in-flight point read off the fast path.
	csrShadowed atomic.Int64

	// Property index. Resident; restored from the image's GIDX section at open
	// since v6, with only the entries logged after the last compaction replayed
	// from the WAL on top. Before v6 it lived in the log alone, and every
	// restart replayed the whole index.
	propIdx *index.PropertyIndex

	// openOpts is the Options this store was opened with, kept whole.
	//
	// The fields below decompose the ones this type reads on a hot path, and
	// that is not a duplicate of this: those are what the store *uses*, and this
	// is what it would have to be given again to be the same store. Only
	// CompactAndReopen needs it, and it needs all of it -- the metrics sink, the
	// observer, the anchor, the ledgers -- because a reopen that silently
	// reconstructed Options from the fields it happened to keep would return a
	// handle configured differently from the one it replaced, and nothing would
	// say so.
	openOpts Options

	// imageMode is Options.ImageMode as given, resolved at the point of use so
	// the zero value stays the documented default. See mapping.go.
	imageMode ImageMode

	// adjacency is Options.Adjacency as given: whether the image's adjacency
	// arrays are built at Open or on first use. See adjacency_mode.go.
	adjacency AdjacencyMode

	// indexMode is Options.IndexMode as given, on the same terms. See
	// index_mode.go.
	indexMode IndexMode

	// images holds every mapping of graphene.csr this store has created, and is
	// what keeps them alive: a CSRGraph deliberately does not reference its
	// mapping, because runtime.AddCleanup never fires on an object its argument
	// is reachable from. Guarded by mu.
	//
	// A writer has at most one entry here for the life of the store. Only a live
	// reader, which rebuilds from the files on a Refresh across a compaction,
	// ever accumulates more — see sweepImages.
	images []*mapping

	// imageVersion is the container version of the image this store loaded, and
	// imageIndexOnDisk what that image carries in place of a property index.
	// Both are zero on a store with no image. Guarded by mu; see
	// StorageStats.ImageVersion, which is the only thing that reads them.
	//
	// Held on the store rather than on the graph because they are facts about
	// the file, and the graph outlives it: a compaction publishes a graph with no
	// file behind it at all, and a caller asking what version is on disk after
	// one is asking about the image that is still there.
	imageVersion     uint16
	imageIndexOnDisk string

	// indexImages holds the mappings created for the property index's base, as
	// distinct from the ones the graph's records address.
	//
	// Two lists rather than one because the two have different lifetimes and
	// only one of them is safe to sweep at a compaction. A graph mapping is
	// addressed by every graph built from it, including the ones a compaction
	// publishes later, so a writer's single entry in images above lives until
	// Close. An index mapping is addressed by exactly one base -- nothing builds
	// a base out of another one -- so the one a compaction replaces can be
	// released as soon as the base over it is unreachable. Guarded by mu. See
	// sweepIndexImages.
	indexImages []*mapping

	// live, and the three numbers a live reader advances over. Guarded by mu;
	// zero and unused on every other kind of store. See live.go.
	live      bool
	logPath   string
	logGen    uint64
	replayOff int64

	// syncOnCommit forces an fsync at each batch commit. Guarded by mu.
	syncOnCommit bool

	// reindexPolicy governs what updates do to propIdx. Guarded by mu.
	reindexPolicy store.ReindexPolicy

	// uniqueEdgeTypes holds the edge types under a cardinality constraint.
	// Guarded by mu, and read on every edge write — see disk/edgeunique.go.
	uniqueEdgeTypes store.UniqueEdgeTypeSet

	// Sequence counters (shared across CSR and delta).
	nodeSeq atomic.Uint64
	edgeSeq atomic.Uint64

	// idHeadroomWarn is Options.IDHeadroomWarn as given: zero means the default,
	// negative disables. Resolved at the point of use so the zero Store stays
	// the documented default.
	idHeadroomWarn float64

	// nodeHeadroomWarned and edgeHeadroomWarned record that the
	// identifier-headroom warning has already fired for that kind in this
	// process, so a store compacting on a timer near the threshold reports it
	// once rather than every few minutes. Atomic because the check runs after
	// compactCommit has released the store lock.
	nodeHeadroomWarned atomic.Bool
	edgeHeadroomWarned atomic.Bool

	// deltaSoftLimit is Options.DeltaSoftLimit as given: the delta size above
	// which the store says so. Zero disables it. Set once at Open.
	deltaSoftLimit int64

	// memBudget is Options.MemoryBudget as given: the heap ceiling a compaction
	// is refused against. Zero disables it. Set once at Open, and kept because
	// the Open-side check is over by then and the Compact-side one is not — a
	// store compacting on a timer has to be able to ask months later. See
	// budget.go.
	memBudget int64

	// memBudgetSource names the instrument a discovered budget came from, and is
	// empty when the caller stated one or when nothing was discovered.
	//
	// Kept because a figure the caller did not choose has to be explicable. A
	// store refusing a compaction against a budget nobody set, with nothing
	// saying where it came from, is the kind of refusal an operator cannot act
	// on. StorageStats.MemoryBudgetSource is where it surfaces.
	memBudgetSource string

	// residentAdvice is Options.ResidentAdvice. Set once at Open and read without
	// a lock, like the other Open-time settings beside it.
	residentAdvice bool

	// reportProcMem is Options.ReportProcessMemory. Set once at Open and read
	// without a lock, like the other Open-time settings beside it.
	reportProcMem bool

	// batchCaps is Options.MaxBatchBytes and MaxBatchRecords resolved against the
	// budget, the way compactBufs is Options.Compact resolved. Set once at Open,
	// and read on every batch write with no lock: the fields never change after
	// Open, so the check can run before the lock is taken, which is where a
	// refusal has to happen if it is to burn no identifiers.
	batchCaps batchCaps

	// compactBufs is Options.Compact divided into the four sizes a mapped-index
	// compaction is built with. Resolved once at Open, for the reason memBudget
	// is kept: the store compacts on a timer and has to know months later. A
	// zero value means every consumer takes its own default.
	compactBufs compactBuffers

	// deltaOverBudget is whether the delta is currently above that limit, and
	// deltaBudgetReported whether the crossing has been announced to a metrics
	// sink. Both are maintained by noteDeltaBytesLocked under the store lock and
	// read without it, which is why they are atomics rather than guarded fields.
	//
	// The pair is the identifier-headroom warning's shape with one difference:
	// that figure only rises, so having warned once is the end of it, while a
	// delta comes back down. A compaction that brings it under the limit clears
	// both, so the next crossing is announced too.
	deltaOverBudget     atomic.Bool
	deltaBudgetReported atomic.Bool

	// commitSeq numbers batch commits. Like nodeSeq and edgeSeq it has a
	// high-water mark in the CSR header (v8, commitSeqHW), so compaction
	// truncating the log no longer resets it: on open it resumes from the larger
	// of the mark in the image and the highest value the surviving WAL replays.
	// That is what makes a commit sequence number a durable identity rather than
	// an ordering within one log generation.
	commitSeq atomic.Uint64

	// nowUnixNano supplies the commit timestamp. Indirected so tests can pin it;
	// production leaves it nil and reads the clock.
	nowUnixNano func() int64

	// maxSnapshotAge bounds how long a snapshot may pin history; zero is
	// unlimited. Set once at Open. See Options.MaxSnapshotAge.
	maxSnapshotAge time.Duration

	// metrics receives what the store did, nil when Options.Metrics was unset.
	// Set once at Open and never changed, so no lock guards it. Read through
	// Store.record, which is where the nil check lives.
	metrics store.Metrics

	// signer signs each batch commit; nil leaves commits unsigned. Set through
	// Open's options and not changed afterwards, so a log never contains a
	// signing policy that shifted underneath it mid-run.
	signer store.Signer

	// verifier and requireSigned govern the replay Open performs. Held on the
	// store so a later Replay — after a Truncate, say — applies the same policy
	// the store was opened under.
	verifier      store.Verifier
	requireSigned bool

	// audit records operator actions when configured; nil when not. Its own
	// locking is internal, so it is not guarded by mu — an audit append must not
	// be able to block on the store's write lock, or recording an action could
	// deadlock against the action itself.
	audit *AuditLog

	// retention governs which retired segments survive a compaction, and
	// segmentSeq numbers the next one. Guarded by mu.
	retention  RetentionPolicy
	segmentSeq uint64

	// redactions is the ledger of attested removals, nil when Options.Redaction
	// is off, and redaction the policy bounding a single cascade.
	redactions *RedactionLedger
	redaction  RedactionPolicy

	// grants is the ledger of privilege changes, nil when Options.Roles is off.
	grants *GrantLedger

	// keyTimeline holds the rotations seen in the current log, in the order they
	// were recorded. Guarded by mu.
	//
	// Bounded by the log's lifetime: a compaction truncates it, so rotations
	// before the last compaction are gone. Durable key history needs WAL
	// retention, which is not built — see KeyTimeline's doc.
	keyTimeline []KeyTransition

	// attestActorID is recorded as the actor in the snapshot attestation written
	// at compaction. Separate from any transaction's actor because compaction is
	// not a transaction — it is an operator action, and attributing it to
	// whoever happened to write last would be wrong.
	attestActorID uint64

	// compacting is set while a compaction is between its pin and its publish.
	// Guarded by mu.
	//
	// Needed only because that span now has the lock released in the middle of
	// it. Two compactions running at once would race for one temp image path and
	// each splice the delta against the other's epoch; while the lock was held
	// end to end, mu itself was the exclusion.
	compacting bool

	// backups counts the backups currently reading this directory. Guarded by mu.
	//
	// A backup is a pure read of the store's files and so does not exclude
	// writers, other backups, or anything else — only compaction, which is the
	// one operation that rewrites bytes already on disk. See backup.go.
	backups int

	// auto is the background compaction trigger, nil unless Options.AutoCompact
	// asked for one. See autocompact.go for the lifecycle it owes Close.
	auto *autoCompactor

	// afterPinHook runs between a compaction's pin and its build, with no lock
	// held. Nil in production, and reachable only from inside this package.
	//
	// It exists for the same reason as the WAL's writeHook: the window it opens
	// is the whole point of the three-stage compaction, and without a seam the
	// only way to land a commit inside it is to race one and hope. A test that
	// hopes is a test that passes on a fast machine.
	//
	// Set once, before the store is used.
	afterPinHook func()

	// compactStepHook runs immediately before each named durability step of a
	// compaction and, by returning an error, makes that step fail. Nil in
	// production, and reachable only from inside this package.
	//
	// A compaction's crash-safety is a property of the *order* of its steps, so
	// the only way to test it is to stop at each one in turn and check what a
	// reopen recovers. Without a seam, the two steps a test can reach are the
	// two that happen to be reproducible by hand — writing a stray temp file and
	// writing a checkpoint marker — and the rest are covered by argument.
	//
	// The step names are the compactStep* constants in compact.go. A hook that
	// does not recognise a step must return nil: steps are added as the
	// sequence grows, and a hook that failed on an unknown name would turn every
	// such addition into an unrelated test failure.
	//
	// Set once, before the store is used.
	compactStepHook func(step string) error

	// afterBackupPinHook is the same seam for a backup: it runs between the pin
	// and the first file copied, with no lock held, so a test can land a
	// compaction attempt or a write inside the window a backup opens. Nil in
	// production.
	afterBackupPinHook func()

	// lastCompact is when Compact last completed in this process. Guarded by mu.
	// Not persisted — a reopened store reports zero even if its image on disk was
	// compacted a moment earlier. Persisting it needs a CSR header field and is
	// held for the v8 format change, like the commit sequence high-water mark.
	lastCompact time.Time

	// lock is the process-level lock on the store directory: exclusive for a
	// writer, shared for a reader. Taken before any other file in the directory
	// is touched and released after every one of them is closed. See lock.go for
	// what it does and does not promise. Never nil after a successful Open, and
	// its own methods are nil-safe regardless.
	lock *storeLock

	// readOnly refuses every mutation. Set once at Open and never changed, so no
	// lock guards it — a store cannot become writable, and a caller racing a
	// mutation against the Open that configured it has a worse problem than this
	// field.
	readOnly bool

	// strictProjectionKeys is Options.RefuseUnindexedProjectionKeys. Set once at
	// Open and never changed, on the same terms as readOnly above.
	strictProjectionKeys bool

	// dropped names the constraints the catalogue recorded that the data did not
	// satisfy at open, and which were therefore not applied. Always empty under
	// ConstraintRefuse, which fails the open instead. Guarded by mu.
	dropped []DroppedDeclaration

	// unclean records that the previous exclusive holder of this directory did
	// not close cleanly. Informational: the store opens either way, because WAL
	// replay is already crash-safe and refusing would turn every crash into a
	// manual intervention. See RecoveredFromUncleanShutdown.
	unclean bool
}

// nextCommitMeta allocates the provenance for one batch commit.
//
// Callers hold s.mu, but commitSeq is atomic anyway so the number is unique
// even for a future writer that does not.
func (s *Store) nextCommitMeta(ctx store.TxContext) batchMeta {
	now := time.Now().UnixNano
	if s.nowUnixNano != nil {
		now = s.nowUnixNano
	}
	return batchMeta{
		CommitSeq: s.commitSeq.Add(1),
		UnixNano:  now(),
		ActorID:   ctx.ActorID,
		Signer:    s.signer,
	}
}

// now reads the clock through the same indirection the commit timestamp uses,
// so a test that pins time pins all of it rather than half.
func (s *Store) now() time.Time {
	if s.nowUnixNano != nil {
		return time.Unix(0, s.nowUnixNano())
	}
	return time.Now()
}

// StorageStats implements store.StorageReporter.
//
// Taken under a read lock so the delta counts are mutually consistent — a
// caller comparing DeltaNodes against CSRNodes should not see figures from
// either side of a compaction. The WAL size is read outside that consistency
// guarantee (see WAL.Size) and may lag by one record.
func (s *Store) StorageStats() store.StorageStats {
	st := s.collectStorageStats()

	// The one field here that asks the operating system a question, and so the
	// one taken after the lock has been dropped rather than under it. A kernel
	// call is not bounded by anything this package controls, and holding a read
	// lock across one would let a slow /proc read block a commit -- for a figure
	// that is about the process rather than about the store and cannot be
	// inconsistent with the rest of the struct in any way a caller could act on.
	st.Process = s.processMemory()
	return st
}

// collectStorageStats is StorageStats' locked half. It takes the read lock
// itself rather than requiring it, which is why it is not named ...Locked.
func (s *Store) collectStorageStats() store.StorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v := s.viewPtr.Load()
	st := store.StorageStats{
		DeltaNodes:   v.delta.liveNodes,
		DeltaEdges:   v.delta.liveEdges,
		DeletedNodes: v.delta.maskedNodes,
		DeletedEdges: v.delta.maskedEdges,
		WALBytes:     s.wal.Size(),
		CommitSeq:    s.commitSeq.Load(),
		LastCompact:  s.lastCompact,

		// What is holding history open. An operator looking at a store whose
		// memory will not come down needs to be able to distinguish "the delta
		// is genuinely large" from "one abandoned snapshot is pinning an image
		// and every version written since", and these are the figures that
		// separate them.
		OpenSnapshots:       s.snaps.count,
		OldestSnapshotEpoch: s.snaps.oldest,
		VisibleEpoch:        s.visibleEpoch.Load(),
	}
	if csr := v.csr; csr != nil {
		st.CSRNodes = csr.NodeCount()
		st.CSREdges = csr.EdgeCount()
	}
	st.ImageMode, _ = s.imageHolding()
	st.IndexMode = s.indexHolding()

	// The budget in force and where it came from. Both are set once at Open and
	// never change, so they cost two field reads; they are here rather than only
	// on Options because a caller that did not construct the store -- an
	// operator reading stats, a test asserting a discovered figure -- has no
	// other way to see which of the two a refusal would be measured against.
	st.MemoryBudgetBytes = s.memBudget
	st.MemoryBudgetSource = s.memBudgetSource
	st.ImageVersion = s.imageVersion
	st.IndexOnDisk = s.imageIndexOnDisk

	// The byte figures come from the store's own mapping lists, not from the pair
	// imageHolding returns. The mode above is about the *read path* -- whether
	// what a caller is served addresses a file -- and after a compaction the
	// answer is honestly "heap", because the graph on top was built in memory.
	// The pages are still there even so: the mapping taken at Open is held until
	// Close, every blob the compaction carried forward addresses it, and until
	// v0.8.0 this field reported zero for it. An operator sizing a machine wants
	// what is mapped, not what is being read out of it. See Store.mappedBytes.
	st.ImageMappedBytes = s.mappedBytes()

	// And the index's share of it, a subset rather than a second file: GPIX and
	// GPIR are sections of the image. See index.PropertyIndex.MappedBytes.
	st.IndexMappedBytes = s.index().MappedBytes()
	st.Adjacency = s.adjacencyHolding()
	st.PropertyNodeEntries, st.PropertyEdgeEntries = s.index().EntryCounts()

	// Identifiers issued, not identifiers present. The counters are what the
	// next write takes from, and they are what the ceiling binds; the image's
	// own highest identifier is lower whenever the top of the space has been
	// deleted, which would report headroom the store does not have.
	// The delta's own bytes and the whole-store estimate. Both are read under the
	// lock already held, and neither costs a pass: see estimate.go, which has to
	// be free because AutoCompact evaluates a policy against this on a ticker.
	st.DeltaBytes = v.delta.bytes
	st.DeltaOverBudget = s.deltaOverBudget.Load()
	st.EstimatedResidentBytes = s.estimateResidentLocked().Total

	st.HighestNodeID = s.nodeSeq.Load()
	st.HighestEdgeID = s.edgeSeq.Load()
	st.IDCeiling = maxCSREntityID
	st.NodeIDHeadroom = idHeadroom(st.HighestNodeID)
	st.EdgeIDHeadroom = idHeadroom(st.HighestEdgeID)
	return st
}

// processMemory reads the kernel's counters for the whole process, or reports
// that nothing was read. No lock held; see StorageStats.
//
// The zero value is not a reading and does not pretend to be: store.ProcessMemory
// .Known is false for it, which is the same "unanswerable is not zero" rule the
// sysmem readers follow and MemoryBudgetSource follows for the other question.
// A store that did not ask and a platform that cannot answer are deliberately
// the same value here -- in both cases nothing was measured, and a caller that
// needs to tell them apart is looking at its own Options.
func (s *Store) processMemory() store.ProcessMemory {
	if !s.reportProcMem {
		return store.ProcessMemory{}
	}
	m, ok := readSysMemory()
	if !ok {
		return store.ProcessMemory{}
	}
	return processMemoryFrom(m)
}

// processMemoryFrom is the translation half, split out so the rules below can be
// tested against a fabricated reading rather than against whatever the machine
// running the tests happens to hold. sysMemoryFromCounters and parseProcStatus
// are split for the same reason.
func processMemoryFrom(m sysMemory) store.ProcessMemory {
	// A reading with no instrument behind it is not a reading. Nothing in the
	// tree produces one -- every reader that returns true names its source --
	// but Known is the contract this type publishes, and a value that answered
	// false to it while carrying figures would be the one shape a caller cannot
	// defend against.
	if m.Source == "" {
		return store.ProcessMemory{}
	}
	p := store.ProcessMemory{
		AnonBytes: m.Anon,
		PeakBytes: m.Peak,
		Split:     m.Split,
		Current:   m.Current,
		Source:    m.Source,
	}
	// The file-backed share is a subtraction rather than a reading on every
	// platform that answers at all, and it is only meaningful where the two
	// classes were told apart. Where they were not, sysMemory puts the whole
	// total in both fields, so subtracting would yield zero and read as "no
	// mapped pages" -- the one wrong answer available. Split is what says not to.
	if m.Split && m.Resident > m.Anon {
		p.FileBytes = m.Resident - m.Anon
	}
	return p
}

// noteDeltaBytesLocked records whether the delta has reached
// Options.DeltaSoftLimit. Caller holds s.mu.
//
// Called from every path that changes the delta's byte count rather than from the
// commit paths, so a store written one record at a time reports what a store
// written in batches reports. It is a comparison the caller has already paid for
// and two atomic stores, which is what lets it sit on the write path.
//
// Clearing deltaBudgetReported on the way back under the limit is what re-arms
// the metric for the next crossing.
func (s *Store) noteDeltaBytesLocked(bytes int64) {
	if s.deltaSoftLimit <= 0 {
		return
	}
	over := bytes >= s.deltaSoftLimit
	s.deltaOverBudget.Store(over)
	if !over {
		s.deltaBudgetReported.Store(false)
	}
}

// reportDeltaBudget emits MetricDeltaOverBudget for a crossing not yet announced.
// Called with no lock held, after a commit has published.
//
// The flag is read twice: once without the lock, which is the point -- every
// commit that is not at the limit pays one atomic load and leaves -- and once
// under it, beside the byte figure the event carries, so the two agree.
//
// A compaction that resolves the crossing between that read and the
// compare-and-swap costs one event describing a delta which has just come back
// under the limit. That is an advisory event arriving a moment late rather than a
// wrong one, and excluding it would mean holding the store lock across a call into
// a caller's sink.
func (s *Store) reportDeltaBudget() {
	if !s.deltaOverBudget.Load() || !s.metricsOn() {
		return
	}

	s.mu.RLock()
	over, bytes := s.deltaOverBudget.Load(), s.cur().delta.bytes
	s.mu.RUnlock()

	if !over || !s.deltaBudgetReported.CompareAndSwap(false, true) {
		return
	}
	s.record(store.Metric{
		Kind:     store.MetricDeltaOverBudget,
		Bytes:    bytes,
		Examined: s.deltaSoftLimit,
	})
}

// defaultIDHeadroomWarn is the fraction of the identifier space at which a
// compaction starts reporting. One tenth of 2^32 identifiers is still some
// four hundred million, which at a million burned per rebuild is hundreds of
// rebuilds of warning — the point of the figure is that the remedy involves
// planning, so it has to arrive long before it is urgent.
const defaultIDHeadroomWarn = 0.10

// idHeadroom is the fraction of the identifier space still unissued.
//
// Clamped at zero rather than allowed to go negative: a store that somehow
// issued past the ceiling is in a state the next open refuses, and a negative
// fraction would render as a number an operator has to interpret instead of a
// zero they cannot misread.
func idHeadroom(issued uint64) float64 {
	if issued >= maxCSREntityID {
		return 0
	}
	return float64(maxCSREntityID-issued) / float64(maxCSREntityID)
}

// index returns the property index the store is currently serving from.
//
// Read paths that hold no reader use this rather than the propIdx field. The
// field is what a *writer* files into; this is what a reader answers from, and
// Refresh is the one operation that makes them briefly different — it builds a
// replacement index and publishes it with the image and delta it belongs to, so
// a reader that has not seen the new view must not see the new index either.
func (s *Store) index() *index.PropertyIndex { return s.cur().idx }

// observeCommitMeta advances the commit counter past a value read back from the
// log, so a reopened store does not reissue sequence numbers the log already
// used.
func (s *Store) observeCommitMeta(m batchMeta) {
	for {
		cur := s.commitSeq.Load()
		if m.CommitSeq <= cur || s.commitSeq.CompareAndSwap(cur, m.CommitSeq) {
			return
		}
	}
}

const (
	walFileName = "graphene.wal"
	csrFileName = "graphene.csr"

	labelCountFieldBytes      = 1
	legacyLabelBytesPerValue  = 1
	currentLabelBytesPerValue = 2

	nodePayloadIDBytes      = 8
	nodePayloadLabelStart   = nodePayloadIDBytes + labelCountFieldBytes
	nodePayloadPropLenBytes = 4

	edgePayloadIDsBytes      = 8 + 8 + 8
	edgePayloadLabelStart    = edgePayloadIDsBytes + labelCountFieldBytes
	edgePayloadWeightBytes   = 4
	edgePayloadPropLenBytes  = 4
	edgePayloadTailFixedSize = edgePayloadWeightBytes + edgePayloadPropLenBytes

	csrVersionV2            = 2
	csrVersionV3            = 3
	csrVersionWithU16Labels = 4
	csrVersionWithSeqHW     = 5
	csrVersionWithPropIndex = 6
	csrVersionNoAdjacency   = 7 // stopped writing the never-read adjacency arrays
	csrVersionMappedIndex   = 9 // carries the property index as GPIX/GPIR, not GIDX

	// csrVersionCurrent is what a store writes by default and csrVersionMax is
	// the highest it reads. They were one number until v9, because every version
	// before it was written by every build that could read it.
	//
	// v9 is chosen by the payload rather than by the build: it means "this image
	// carries the property index as GPIX and GPIR", which is a decision about how
	// the index is held, not about how new the writer is. So a build read v9 from
	// the change that taught it to, and writes v9 from this one, which made that
	// the default. Anything asking which version an operator's next compaction
	// will produce wants csrVersionCurrent; anything bounding a file it is about
	// to parse wants csrVersionMax. Options.IndexMode: IndexResident still writes
	// csrVersionSectioned, so the two constants are not the same number and the
	// distinction they were introduced for outlives the flip.
	csrVersionCurrent        = csrVersionMappedIndex
	csrVersionMax            = csrVersionMappedIndex
	csrV6HeaderSize          = 46 // magic4 + version2 + counts16 + seqHW16 + indexOffset8
	csrV5HeaderSize          = 38
	csrIndexSectionMagic     = "GIDX"
	csrIndexSectionMagicSize = 4

	// Smallest number of bytes a single record can occupy on disk. Used to bound
	// header-declared counts against the file's actual remaining length before
	// allocating from them — see deserialiseCSR.
	//
	// node:  id8 + labelCount1 + propLen4                       (zero labels, zero-length blob)
	// edge:  id8 + src8 + dst8 + labelCount1 + weight4 + propLen4
	// entry: id8 + keyLen2 + valLen4                            (index section)
	minNodeRecordBytes = 13
	minEdgeRecordBytes = 33
	minPropEntryBytes  = 14

	// Build stores records in pages of 4096 identifiers, so opening a file costs
	// memory proportional to the pages its identifiers fall in, not to its
	// highest ID. Two bounds keep even that from being a file's to choose.
	//
	// maxCSREntityID is the absolute ceiling, and what it bounds is the page
	// directory: one int32 per page of the identifier space up to the highest ID
	// named, so 4 MiB per kind at the ceiling for a file that names an identifier
	// just below it. IDs are never reused, and Compact preserves them, so a
	// long-lived store's highest ID only ever grows — 2^32 is the ceiling on the
	// engine's addressable lifetime, which at a million identifiers burned per
	// rebuild is some four thousand rebuilds. The headroom figures in
	// StorageStats are how an operator sees it coming.
	//
	// csrIDSparsityFactor bounds the pages the records touch against the record
	// count actually present, because the absolute ceiling alone is no protection:
	// a file of scattered 13-byte records, one per page, would charge 4096 record
	// slots each. Allowing 256 identifiers per surviving record is far past what
	// rollbacks and deletions produce in practice, and it makes a small file's
	// worst case small.
	//
	// csrIDSparsityFloor keeps the relative bound from over-constraining a nearly
	// empty file, whose record count is too small to derive a useful ceiling
	// from. Sixteen pages is what it allows, and with the directory it is what
	// caps a hostile minimal file: 4 MiB per kind plus the pages it touches.
	maxCSREntityID      = 1 << 32
	csrIDSparsityFactor = 256
	csrIDSparsityFloor  = 1 << 16
)

// Options configure a store at Open. The zero value is the historical
// behaviour: unsigned commits, unverified replay.
type Options struct {
	// Signer signs every batch commit. Nil leaves commits unsigned.
	Signer store.Signer

	// Verifier checks commit signatures during the replay that Open performs.
	// Nil skips the check — a signed log still replays, but nothing confirms it.
	Verifier store.Verifier

	// RequireSignedCommits rejects a log containing a committed batch with no
	// signature. This is what closes the downgrade: without it, stripping a
	// signature turns a commit into an ordinary unsigned one and verification is
	// simply skipped.
	//
	// Not implied by setting Verifier, because an existing store's older commits
	// are genuinely unsigned and a caller enabling verification mid-life has to
	// tolerate those until a compaction retires them.
	RequireSignedCommits bool

	// AttestActorID is recorded as the actor in the snapshot attestation written
	// at each compaction, when Signer is set. Compaction is an operator action
	// rather than a transaction, so it carries its own actor rather than
	// inheriting one from whichever write happened last.
	AttestActorID uint64

	// Audit enables the hash-chained audit log, recording operator actions —
	// opens, compactions, key rotations, retention deletions — in
	// graphene.audit.
	//
	// Off by default. It is another file, another sync per recorded action, and
	// a caller who does not need one should not pay for it. Reads and queries
	// are never recorded regardless: see audit.go for why that boundary is where
	// it is.
	Audit bool

	// Retention decides which retired WAL segments survive a compaction.
	//
	// The zero value keeps none, which is the behaviour before segmentation
	// existed: compaction discards the log along with every commit's actor,
	// timestamp, signature, and every key rotation it held. Set it to keep that
	// history. How long evidence should be kept is a legal and operational
	// question rather than an engineering one, so the engine takes it as a
	// parameter and has no default opinion.
	Retention RetentionPolicy

	// Redaction enables the redaction ledger, which records who destroyed what,
	// when, and why — in its own file that compaction never truncates. See
	// redact.go for why it cannot live in the WAL.
	//
	// Off by default, and the default keeps DeleteNode's existing behaviour: an
	// unrecorded, unattributed removal. A store that has not opted in has no
	// ledger and RedactNode refuses rather than silently degrading to a delete.
	Redaction bool

	// RedactionPolicy bounds a single redaction's cascade. The zero value
	// permits any size; the engine has no opinion on how much removal is too
	// much, because that is the caller's legal and operational question.
	RedactionPolicy RedactionPolicy

	// Roles enables the grant ledger, which records who was given which
	// capabilities, by whom, and why.
	//
	// Off by default, and worth being clear about what turning it on buys: it is
	// an **audit mechanism, not a security boundary**. Nothing in the engine
	// consults it. What it provides is that a privilege change cannot happen
	// without leaving an attested record, because capabilities are derived from
	// the ledger and from nothing else. See rbac.go.
	Roles bool

	// VerifyOnOpen checks the compacted image before loading it: the body
	// digest, the Merkle roots against the records they describe, and — when a
	// Verifier is set — the snapshot attestation's signature. A failure fails
	// the Open, so the store never comes up holding records it could not vouch
	// for.
	//
	// Off by default, and the reason is worth stating rather than assuming.
	// Hashing the whole image costs time proportional to the file on every
	// startup, which is the same argument that keeps Open from running
	// VerifyIndexes — a check that makes every startup slower gets switched off,
	// and a check that is switched off protects nothing. Signature verification
	// of the *log* is not covered by this flag and always runs when a Verifier
	// is set, because it rides on the replay Open performs anyway.
	//
	// Turn it on where opening is rare relative to querying, which is the
	// ingest-once/query-many shape this engine is built for. See StrictOptions.
	VerifyOnOpen bool

	// ReadOnly opens the store for reading and takes a *shared* process lock, so
	// it runs alongside other readers but never alongside a writer. Every
	// mutating method returns ErrReadOnly, and nothing under dir is modified —
	// not the WAL, not the ledgers. (The lock file itself is created if absent;
	// a lock needs something to lock.)
	//
	// # The view is fixed at Open
	//
	// This is the part to understand before relying on it. Open materialises the
	// whole store into memory once — the delta layer and property index from a
	// WAL replay, the CSR from one read or one mapping of graphene.csr — and
	// nothing re-reads afterwards. (VerifyOnOpen shares that one read rather than
	// taking its own, so enabling it does not change this either.) A read-only
	// store therefore shows the graph as it stood when it opened, for as long as
	// it lives. Reopen to advance.
	//
	// Under the default ImageMode a read-only store maps the image, which is
	// consistent with a fixed view rather than in tension with it: the engine
	// never rewrites graphene.csr in place, so the mapped bytes are the ones that
	// were there at Open. See ImageMode and §15.13.
	//
	// That is why a reader is refused alongside a writer rather than permitted:
	// the alternative is a permanently stale view with nothing to indicate it is
	// stale, which is worse than a clear refusal. See lock.go.
	//
	// Use it for the read-only work the CLI does — verify, custody, prove — and
	// for any consumer querying a store another process might want to write.
	//
	// See LiveReader for the mode that does advance, and what it gives up to.
	ReadOnly bool

	// Constraints decides what an open does when graphene.schema names a
	// constraint the data does not satisfy — a real case, because a store
	// written by a build that did not read the catalogue could have accumulated
	// duplicates under a key another build declared unique.
	//
	// The zero value refuses, which is right for a writable store: it is the
	// same check DeclareUniqueProperty already makes, at the same moment, and a
	// store that comes up believing a constraint it does not keep has handed the
	// caller the guarantee and none of the behaviour. OpenReadOnly and OpenLive
	// default it to ConstraintDrop instead — enforcement is meaningless without
	// writes, and reading a damaged store is exactly what a reader opens one to
	// do. See DroppedDeclarations.
	Constraints ConstraintPolicy

	// RefuseUnindexedProjectionKeys makes a projection fail on a key no id in
	// this store is indexed under, instead of returning nothing for it.
	//
	// Off by default, and the default behaviour is unchanged to the byte. A
	// projection is positioned by the keys it is given and the property index
	// accepts any key handed to IndexNodeProperty, so an unindexed key is not
	// wrong to it: it matches nothing, the pass succeeds, and a materialised
	// result comes back full length and entirely nil. That is correct for a pass
	// that legitimately finds nothing and indistinguishable from a typo. See
	// store.PropertyKeyLister.
	//
	// The intended shape is on in tests and off in production, which is why it is
	// set here and not per call: a caller wanting the check does not want to
	// remember it at each of twenty call sites, and a caller not wanting it
	// should pay nothing. Turning it on costs one key-list read per projection
	// call, which is bounded by the number of distinct keys in the index and not
	// by the ids being projected -- negligible for a streaming pass over
	// millions, and per-call overhead for a loop of small ones.
	//
	// It is not the default, and the reason is consistency rather than cost. This
	// engine is organised against silent wrong answers, which is the argument for
	// making it the default; against that, turning a call that succeeds today
	// into one that returns an error is a break a caller finds at runtime rather
	// than at compile time, and this release declined to weaken the mapped-slice
	// lifetime contract for exactly that reason. Graph.NodePropKeys makes the
	// same check available to a caller who would rather write it themselves.
	//
	// Not offered by the in-memory backend, which has no options to carry it.
	RefuseUnindexedProjectionKeys bool

	// LiveReader opens a read-only store that can be advanced with Refresh,
	// taking **no process lock at all**. It implies ReadOnly.
	//
	// This is the third open mode, not a variation on the second, and the
	// difference is a guarantee rather than a feature. ReadOnly promises that no
	// writer is running, and that promise is exactly what makes its view safe to
	// fix at Open. A reader that follows a writer cannot make that promise: a
	// shared lock and the writer's exclusive lock cannot coexist. So this mode
	// gives it up.
	//
	// What is *not* given up is anything on the writing side. The writer still
	// takes an exclusive lock, so two writers are still impossible, and a store
	// with live readers attached is written exactly as it was before.
	//
	// What a live reader gets in exchange is Refresh: it replays whatever the
	// writer has made durable since the last call, and rebuilds from the image
	// when a compaction has replaced the log underneath it. It advances only when
	// Refresh is called. See live.go.
	LiveReader bool

	// MaxSnapshotAge bounds how long a Snapshot may be held before its reads
	// start returning ErrSnapshotExpired. Zero, the default, is unlimited.
	//
	// A snapshot pins the image it was taken against and every delta version
	// written since, so one that is never closed holds memory the store can
	// otherwise release at the next compaction. This is the guard for a caller
	// that leaks one — it does not close the snapshot, it stops it being useful,
	// which turns a silent leak into an error at the point of use.
	MaxSnapshotAge time.Duration

	// AutoCompact runs a compaction in the background when the policy fires.
	// Nil, the default, leaves compaction entirely to the caller — which is what
	// the engine has always done, and what Graph.ShouldCompact is for.
	//
	// Set it when nothing in the application is going to call Compact on a
	// schedule. Nothing else bounds delta growth: everything written since the
	// last compaction stays in memory and is replayed at every open, so a store
	// that is never compacted degrades in memory, open time and read speed at
	// once, with no error and no warning until someone measures it.
	//
	// store.DefaultCompactionPolicy() is a starting point, not a tuned setting.
	//
	// # It cannot reopen, so it does not bound the payload term
	//
	// A background compaction writes a new image and truncates the log, and the
	// records this process holds go on carrying their own payload bytes --
	// because AttachBase, which makes a record address the mapped file instead
	// of the heap, runs on the load path, and a compaction is not one. Only a
	// reopen sheds that, and a background goroutine cannot reopen the store it
	// is running inside: the handle belongs to the caller.
	//
	// So this bounds the delta and the log and does not bound what a long-lived
	// writing process accumulates. Measured, that term climbs about 49 MiB per
	// 100,000 records written and never resets. A process that ingests for hours
	// needs Graph.CompactAndReopen on its own schedule; this is for a store whose
	// writes are occasional and whose handle outlives them.
	AutoCompact *store.CompactionPolicy

	// AutoCompactInterval is how often the policy is evaluated. Zero means 30s.
	// Ignored when AutoCompact is nil.
	AutoCompactInterval time.Duration

	// AutoCompactObserver is told the outcome of each background compaction:
	// the rule that fired and whatever the compaction returned. Nil discards
	// both, which makes a failing background compaction silent — see
	// store.CompactionObserver.
	//
	// Worth attaching one if MemoryBudget is set. A compaction refused for its
	// size arrives here and nowhere else, and it recurs on every tick for as
	// long as the policy keeps firing — which it will, because the refusal is
	// what stopped the delta from shrinking. Without an observer that is a
	// store whose delta grows without bound and says nothing.
	//
	// The same nil-by-default shape as Signer and Verifier, and where a metrics
	// sink will attach. store.CompactionObserverFunc wraps a closure.
	AutoCompactObserver store.CompactionObserver

	// Metrics receives what the store did: commits, fsyncs, compactions,
	// queries, the replay each open performs. Nil, the default, records
	// nothing and costs nothing — every call site is inside a nil check, and
	// the clock reads that a measurement needs happen inside that branch too.
	//
	// The same nil-by-default shape as Signer and AutoCompactObserver, and for
	// the same reason: an embedded engine that logs or exports on its own
	// behalf has taken a decision belonging to the program embedding it. What
	// it can offer is somewhere to attach one.
	//
	// Record is called from whichever goroutine did the work and sometimes with
	// a store lock held, so an implementation must not block and must never
	// call back into the store. See store.Metrics.
	Metrics store.Metrics

	// MaxReplayRecords and MaxReplayBytes bound the WAL replay an Open will
	// perform. Zero — and any negative value — is unlimited, which is the
	// historical behaviour. Exceeding either refuses the Open with
	// ErrReplayBudget naming both the figure and the budget.
	//
	// The log is the half of an open whose cost nothing predicts. An image is a
	// file whose size an operator can see; a log that was never compacted is a
	// whole write history that is replayed into memory on every open, and §16's
	// delta-growth limitation says so without offering a way to find out first.
	// These are that way, and PreflightOpen is how to obtain both figures
	// without opening anything.
	//
	// Three things to know before setting either.
	//
	// They bound replay and nothing else. The image is loaded by the same Open
	// and is not covered — on a compacted store it is the larger half. For a
	// figure covering both, set MemoryBudget, which is modelled rather than
	// counted and so answers a different question: these two refuse on what the
	// log *is*, that one on what the whole open would *cost*.
	//
	// MaxReplayBytes is measured against the records region, not the file:
	// OpenEstimate.WALReplayBytes, which is the log's size less its 50-byte
	// container header. Feeding it WALBytes instead makes it refuse a store that
	// has not changed.
	//
	// MaxReplayRecords costs a counting pass over the log, because the number of
	// records in a log is not a property of its size. The pass is skipped
	// entirely when the records region is too small to hold the budget's worth of
	// records — which is every compacted store — and stops as soon as the count
	// passes the budget, so its cost is set by the budget rather than by the log.
	// Between those two, a store that is large but under budget pays one extra
	// sequential read at open. MaxReplayBytes costs nothing and bounds records
	// implicitly, since no record is smaller than nine bytes; prefer it unless
	// you specifically need to admit a log that is large in bytes and small in
	// records.
	//
	// A refused Open is not a no-op on disk. By the time either budget can be
	// checked, the directory has been created if it was missing, a stranded
	// rebuilt log has been adopted, and an empty log has been given its container
	// header. Nothing is replayed and no ledger is opened, so nothing is recorded
	// about the refusal — but PreflightOpen is the only genuinely non-mutating
	// way to ask the question.
	MaxReplayRecords int64
	MaxReplayBytes   int64

	// MemoryBudget bounds the heap an Open or a Compact may need, in bytes. Zero
	// — and any negative value — is unlimited, which is the historical
	// behaviour. Exceeding it refuses the operation with an error wrapping
	// ErrMemoryBudget and carrying the arithmetic; see MemoryBudgetError.
	//
	// # It is a refusal and never a degradation
	//
	// Go cannot catch an out-of-memory condition: there is no allocation failure
	// to handle and no warning before the OOM killer, so a process that has
	// started allocating past what the machine has cannot do anything about it.
	// This therefore decides *before* allocating and declines. It does not
	// shrink a cache, spill a merge to disk, reduce a batch, or degrade a query
	// plan, and no future version will make it do any of those silently — an
	// engine that quietly got slower instead of saying no would be the outcome
	// this option exists to prevent. See budget.go.
	//
	// # What it is compared against, at each of the two moments
	//
	// An Open is compared against OpenEstimate.HeapBytesFor for these same
	// Options, which is a model built from the image header and a log walk. A
	// Compact is compared against its working set plus what the store currently
	// holds, as EstimateResident reports it. Both are models: retained heap, not
	// resident set size, and a floor on the latter rather than a prediction of
	// it — RSS ran 1.19x to 1.54x live heap on the one fixture measured. Leave
	// room, and prefer a budget derived from a figure this package reported for
	// a store of the shape you run over one derived from the machine's size.
	//
	// # Three things to know before setting it
	//
	// A refused Compact leaves a store that works and a delta that goes on
	// growing. That is a worse position than having compacted, and the only
	// position available: the compaction is what needs the memory. A caller
	// running AutoCompact gets the refusal through AutoCompactObserver rather
	// than as a failed write, and it is deliberately not recorded as a
	// compaction failure — see checkCompactBudget.
	//
	// The Open check costs one sequential pass over the log, on top of the pass
	// the replay itself will make. What is guaranteed is bounded memory, not
	// bounded I/O.
	//
	// The figure is a whole-store one, not a whole-process one. Nothing here
	// knows what else the program has allocated, so a budget set to the
	// container's limit is a budget with no room for the program using the
	// store.
	MemoryBudget int64

	// DiscoverMemoryBudget asks the engine to read the memory ceiling this
	// process is already running under and derive MemoryBudget from it, when
	// MemoryBudget is not set. Off by default.
	//
	// # Why this is a flag and not the meaning of zero
	//
	// Zero MemoryBudget is documented above as unlimited and has been since the
	// option existed. Quietly redefining it would change the behaviour of every
	// deployment that never set it -- a store that opened yesterday might refuse
	// to open today because the engine discovered a figure nobody asked it to
	// look for. That is the same silent reinterpretation of a shipped default
	// that was refused for what MaxDeltaBytes counts, and it is refused here for
	// the same reason.
	//
	// # An explicit budget always wins
	//
	// A MemoryBudget the caller set is used exactly as given, and this flag then
	// does nothing -- it can never raise a stated figure, and it can never
	// lower one either. Discovery is for the caller who does not know what
	// environment the program will run in, not a second opinion about one who
	// does.
	//
	// # What is discovered, and what fraction of it is taken
	//
	// A cgroup v2 or v1 memory limit on linux, a Job Object memory limit on
	// windows, and on either platform the machine's size when there is no limit;
	// on darwin the machine's size alone, because no per-process ceiling there
	// constrains the class this engine cares about. StorageStats.MemoryBudgetSource
	// names which of those answered, and reports "" on a platform that answered
	// nothing -- where this flag leaves MemoryBudget at zero rather than
	// inventing a figure.
	//
	// Only a fraction of the discovered ceiling is taken, and a smaller fraction
	// of a machine than of a limit. The reason is the last paragraph of
	// MemoryBudget above: the figure is a whole-store one and nothing here knows
	// what else the program has allocated, so a budget set to the container's
	// limit is a budget with no room for the program using the store. A budget
	// derived from a limit is therefore always strictly below it.
	DiscoverMemoryBudget bool

	// ReportProcessMemory asks StorageStats to fill in StorageStats.Process --
	// the resident anonymous and file-backed split as the kernel reports it, for
	// the whole process. Off by default.
	//
	// # Why a flag rather than always
	//
	// StorageStats is polled. AutoCompact evaluates a CompactionPolicy against
	// it on every tick, and bulk.Run evaluates one after every batch, which at a
	// byte-bounded import is thousands of times. Reading the kernel's counters
	// costs about 800 ns and two allocations on windows 11 and a read and parse
	// of /proc/self/status on linux -- small, and still not something to add to
	// a call that most callers make in a loop and never read this field of.
	//
	// The rest of StorageStats is assembled from figures the store already holds
	// and is O(1) for exactly this reason. This is the only field in it that
	// asks the operating system a question, so it is the only one with a switch.
	//
	// # What it does not do
	//
	// Nothing acts on it. It is not a second budget and it does not feed the
	// batch caps or CompactionPolicy, both of which are deliberately driven by
	// figures the engine can compute before it does the work rather than by one
	// it can only observe afterwards. This is an instrument, and a caller that
	// wants a bound is looking for MemoryBudget or CompactionPolicy.
	//
	// The figure is the process, not the store; store.ProcessMemory says why
	// that is the honest reading and how to get a store-shaped number out of it.
	ReportProcessMemory bool

	// ResidentAdvice lets the engine tell the kernel how its mapped image is
	// about to be used, and when it has finished with it. Off by default.
	//
	// It bounds the *resident* class rather than the anonymous one. A mapped
	// image is file-backed page cache, which the kernel reclaims under pressure
	// instead of killing the process -- so it is not the class the 2 GiB
	// directive is about, and this option is not what holds that bound.
	// MemoryBudget, the batch caps and CompactionPolicy are.
	//
	// What it is for is the other half of the same problem. Those pages are
	// charged against a linux cgroup exactly as anonymous ones are, measured at
	// 1,182 MiB during an ingest of 400,000 x 3.2 KB records -- twelve times the
	// anonymous class, and linear in the store. A container sized to the
	// anonymous figure goes into reclaim during the ingest, and reclaim inside a
	// compaction is a latency event inside the operation that is already the
	// peak.
	//
	// # The trade, stated once
	//
	// A page dropped and then read again is a major fault. Less resident memory,
	// more faults. A store that compacts and then serves reads out of the pages
	// the compaction dropped pays for all of them again, so this is worth having
	// during a bulk ingest and is not obviously worth having on a read-serving
	// store. That judgement is the caller's, which is why this is a switch and
	// not a default.
	//
	// # Where it does and does not apply
	//
	// linux and darwin, through madvise. Windows takes its access-pattern hint
	// when the file is opened rather than against a live mapping, and its answer
	// to giving pages back is a working-set cap on the whole process; that is
	// LimitWorkingSet, a function rather than an option precisely because its
	// scope is the process. Setting this on a platform that cannot act on it
	// reports store.MetricResidentAdvice rather than doing nothing quietly.
	//
	// It changes no contract. Advice drops pages, never the mapping, so a
	// Properties or Labels slice a caller is holding stays valid and stays at
	// the same address -- the distinction that makes this buildable where
	// unmapping a replaced image was not. See disk/advise.go.
	ResidentAdvice bool

	// MaxBatchBytes and MaxBatchRecords cap a single AddNodesBatch or
	// AddEdgesBatch. A batch over either is refused with an error wrapping
	// ErrBatchTooLarge and carrying both figures; see BatchTooLargeError.
	//
	// # Why two dimensions
	//
	// A byte cap does not bound ten million empty nodes -- each is 80 bytes of
	// structure carrying no payload, so the byte figure stays small while the
	// allocation does not. A record cap does not bound ten records carrying a
	// gigabyte each. Whichever is reached first refuses the batch.
	//
	// MaxBatchBytes is counted the way the delta counts the same records, so it
	// and MaxDeltaBytes are in the same units.
	//
	// # Zero derives from MemoryBudget, or caps nothing
	//
	// Left at zero, each is derived from the memory budget in force -- including
	// one DiscoverMemoryBudget found, so a store that discovered a 2 GiB cgroup
	// limit refuses batches a store on a 64 GiB machine accepts, with no figure
	// written down by either caller. That derivation is Badger's: its batch caps
	// come out of its memory configuration rather than sitting beside it, so a
	// caller who turns memory down gets smaller batches without a second
	// decision and the two settings cannot be put into disagreement.
	//
	// With no budget at all, zero caps nothing and these calls behave exactly as
	// they did before the option existed. An upgrade does not start refusing
	// batches that a program has always passed.
	//
	// # It refuses; it does not split
	//
	// AddNodes documents that either every node is added or none is, and
	// splitting an oversized batch here would break that for every caller
	// relying on it. The error names both caps and the figures measured, which
	// is what a caller needs to split by the one that actually bound.
	MaxBatchBytes   int64
	MaxBatchRecords int

	// Compact sizes the intermediates a compaction holds while it builds. The
	// zero value is every prior version's behaviour; see CompactOptions, which
	// says what the one figure covers and what it deliberately does not.
	//
	// It is the other half of MemoryBudget rather than a duplicate of it: that
	// one refuses a compaction whose working set does not fit, this one sets how
	// large a part of that working set is. A figure below the floor is refused by
	// Open with ErrCompactWorkingBytes.
	Compact CompactOptions

	// IDHeadroomWarn is the fraction of the identifier space remaining below
	// which a completed compaction emits MetricIDHeadroomLow and writes an
	// AuditIDHeadroomLow entry. Zero takes the default of 0.10; a negative value
	// disables the warning entirely.
	//
	// It is checked at compaction rather than per write because it is a property
	// of the store's lifetime, not of any one batch: the fraction moves by
	// millionths per write and an operator needs it once, early, with somewhere
	// durable to read it later. The warning is sticky per kind within a process
	// — once it has fired for nodes it does not fire again for nodes — so a
	// store compacting on a timer near the threshold does not fill its audit
	// chain with the same observation.
	//
	// There is no refusal here, deliberately. A store at 5% headroom works
	// exactly as it did at 95%, and the remedy — export and import into a fresh
	// store — is a decision with downtime in it. Refusing writes early would
	// take that decision on the operator's behalf.
	IDHeadroomWarn float64

	// DeltaSoftLimit is how many bytes of delta records the store may hold before
	// it reports that it is over budget. Zero, the default, disables the report.
	//
	// Soft, and the word is the contract: no write fails for this, none is
	// delayed, and nothing is spilled or degraded. What happens is that
	// StorageStats.DeltaOverBudget turns true and one MetricDeltaOverBudget is
	// emitted for the crossing. A limit that refused writes would be a different
	// feature and a worse one -- the delta is where a commit goes, so refusing to
	// grow it means refusing data the caller has nowhere else to put.
	//
	// It is for the deployment that compacts on its own schedule and wants to know
	// when that schedule is not keeping up. A caller that would rather the engine
	// act on the figure wants CompactionPolicy.MaxDeltaBytes, with AutoCompact or
	// Graph.CompactIfDue.
	DeltaSoftLimit int64

	// ImageMode decides whether the compacted image is mapped or copied into the
	// heap. The zero value, ImageMapped, maps it — which is the default because
	// the blob half of an image is the largest single thing a store holds and
	// copying it buys nothing but the copy.
	//
	// What it changes for a caller is one sentence, and it is on ImageMapped:
	// the Properties and Labels slices a read hands back address the file, so
	// they are valid for the life of the handle and not past Close. ImageHeap is
	// the way back to bytes that are ordinary heap. store.CloneNode and
	// store.CloneEdge are the way to keep one record either way.
	//
	// See mapping.go for the lifetime argument and §14.18 for the measurement.
	ImageMode ImageMode

	// IndexMode decides whether a property index the image carries is read in
	// place or rebuilt in the heap at Open, and which of the two encodings the
	// next compaction writes.
	//
	// The zero value, IndexMapped, reads it in place: the index stops being a
	// function of the number of entries and becomes a directory and a value table
	// per key. It is the memory this section of the program is about, and it is
	// the default because nothing else in the engine is within an order of
	// magnitude of the index's residency.
	//
	// Two consequences a caller has to know about. A point lookup becomes a binary
	// search through file-backed pages, so it is slower warm and much slower cold
	// — NodesByPropertyBatch turns a pass of many cold lookups back into a
	// sequential sweep. And the image the next compaction writes is v9, which no
	// earlier build opens: IndexResident writes v8 and is the way back.
	//
	// See index_mode.go for the whole argument, including why a store that could
	// not map its image still writes the format it was asked for.
	IndexMode IndexMode

	// Adjacency decides whether the image's adjacency arrays — the inverse of
	// the edge records, which is what a traversal, a degree query and a delete
	// cascade all read — are built while the image loads or the first time one
	// of those asks for them.
	//
	// The zero value, AdjacencyEager, builds them at Open, which is what every
	// version before this one did. AdjacencyLazy is for a process that opens a
	// store to read properties out of it and never walks it: 16 bytes per edge
	// and 16 per node slot that are then never allocated. A process that does
	// walk it pays the same total either way, at a different moment.
	//
	// A writer that deletes cannot skip the build. DeleteNode cascades to the
	// incident edges and finds them through exactly these arrays, so a lazily
	// opened writer builds them on its first delete, having also paid for the
	// branch on every read until then. The mode is worth asking for in a process
	// that reads properties or aggregates without traversing and without
	// deleting, and worth nothing at all in one that deletes.
	//
	// Nothing in the file changes and nothing about an answer changes.
	// StorageStats.Adjacency reports which side of the build a handle is on --
	// the answer rather than the request, so a caller that expected "deferred"
	// and finds "built" can see that something asked.
	Adjacency AdjacencyMode
}

// ErrReplayBudget reports an Open refused because the log exceeds
// Options.MaxReplayRecords or Options.MaxReplayBytes.
//
// The engine cannot catch an out-of-memory condition in Go, so "fail
// predictably" can only mean refusing before allocating rather than degrading
// while allocating. This is that refusal for the replay half of an open. The
// wrapped message names the figure and the budget, because a refusal that does
// not say by how much cannot be acted on.
var ErrReplayBudget = errors.New("disk: replaying this log would exceed the configured budget")

// recordAudit appends an entry when auditing is enabled, and does nothing
// otherwise.
//
// Errors are returned rather than swallowed by every caller in turn: an audit
// log that silently stops recording is worse than none, because it still looks
// like a record.
func (s *Store) recordAudit(kind AuditKind, actorID uint64, detail string) error {
	if s.audit == nil {
		return nil
	}
	_, err := s.audit.Record(kind, actorID, detail)
	return err
}

// ErrAuditKindReserved rejects a caller trying to write an engine-owned entry.
var ErrAuditKindReserved = errors.New("disk: that audit kind is the engine's to write")

// RecordAudit appends a caller's own entry to the audit log, and does nothing
// when auditing is disabled.
//
// This is what makes the engine's audit log usable for events only the caller
// knows about — an export, an operator sign-off, a case-file reference. The
// engine never interprets Detail.
//
// # Why only AuditCustom and above
//
// Kinds below AuditCustom are the engine's. Letting a caller write AuditCompact
// would let them fabricate compaction records, and CustodyFor compares recorded
// compactions against retired segments precisely to catch a compaction that was
// never recorded — a forgeable record defeats the check it exists to support.
// An audit log a caller can write arbitrary engine history into is not evidence
// of what the engine did.
func (s *Store) RecordAudit(kind AuditKind, actorID uint64, detail string) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	if kind < AuditCustom {
		return fmt.Errorf("%w: %s (use AuditCustom or above)", ErrAuditKindReserved, kind)
	}
	return s.recordAudit(kind, actorID, detail)
}

// AuditEntries returns this store's audit log, oldest first.
func (s *Store) AuditEntries() ([]AuditEntry, error) {
	return ReadAuditLog(s.dir)
}

// StrictOptions returns the most cautious configuration the engine offers:
// every commit signed and required to be signed, and the image verified before
// it is loaded.
//
// It exists because the safe posture was previously three separate settings a
// caller had to know about individually, and a security control nobody can find
// is a security control nobody uses. The permissive default is kept for
// compatibility — this is how to opt out of it in one call.
//
//	s, err := disk.OpenWithOptions(dir, disk.StrictOptions(key, ring, actorID))
func StrictOptions(signer store.Signer, verifier store.Verifier, actorID uint64) Options {
	return Options{
		Signer:               signer,
		Verifier:             verifier,
		RequireSignedCommits: true,
		AttestActorID:        actorID,
		VerifyOnOpen:         true,
		Audit:                true,
	}
}

// verifyImage runs the checks VerifyOnOpen asks for, in increasing order of
// what they prove: the bytes are unchanged, the roots describe those bytes, and
// a named key vouched for the result.
//
// The three legs stay separate and stay in this order — the digest matching is
// no evidence the roots do, which is what catches an edit that repaired the
// digest — but they read the image once between them and parse it once. Before,
// each leg opened the file for itself and the last two parsed it again: on a
// store whose image is most of the machine's memory, an opt-in integrity check
// that costs four reads and three parses is one nobody can afford to leave on,
// and a check that is turned off protects nothing.
//
// It now takes the image the open is about to load rather than a path, so the
// count is one: the same bytes are hashed, parsed for the roots, and then
// installed. Under a mapping that is one mapping, and the parse here allocates
// no property arena either — the graph it builds to check the roots is
// discarded, and while it exists its blobs address the file.
func verifyImage(src *imageSource, opts Options) error {
	data := src.data

	switch status, _ := csrDigestStatus(data); status {
	case DigestMismatch:
		return fmt.Errorf("verify image: the compacted image does not match its own digest — " +
			"it has changed since it was written")
	case DigestAbsent:
		// A pre-v8 image carries no digest. Not a failure: it is the honest
		// answer for a file that was never covered, and refusing it would make
		// enabling verification break every older store. Nothing further is
		// checked, because a file this old carries no roots either.
		return nil
	}

	csr, section, err := deserialiseCSRFrom(data, src.mapped(), rootsAdjacencyMode())
	if err != nil {
		return fmt.Errorf("verify image: verify roots: %w", err)
	}
	if err := verifyCSRRootsOf(csr, section); err != nil {
		if errors.Is(err, ErrNoSnapshotRoots) {
			return nil
		}
		return fmt.Errorf("verify image: %w", err)
	}

	// The attestation is only checkable with a verifier; without one there is
	// nothing to check it against, which is not the same as it being absent.
	if opts.Verifier == nil {
		return nil
	}
	if csr.attestation.Signature == nil {
		return nil
	}
	if err := VerifyAttestation(opts.Verifier, csr.attestation); err != nil {
		return fmt.Errorf("verify image: %w", err)
	}
	return nil
}

// Open opens (or creates) a disk-backed Store rooted at dir.
// On first use dir will be created. On restart, the existing CSR (if any) is
// read into memory and parsed, and the WAL is replayed into the delta layer on
// top of it. Neither is bounded by anything but the files themselves — see
// Options.MaxReplayRecords and PreflightOpen, which is how to find out what an
// open will cost before paying it.
// Open takes an exclusive process-level lock on dir and fails with
// ErrStoreLocked if another process — or another Store in this one — already
// holds it. Use OpenReadOnly for a store you only intend to query.
func Open(dir string) (*Store, error) {
	return OpenWithOptions(dir, Options{})
}

// OpenReadOnly opens dir for querying under a shared process lock, so any number
// of readers coexist but none runs alongside a writer. Every mutating method
// returns ErrReadOnly and nothing under dir is modified.
//
// The view is fixed at the moment of the call and never advances — see
// Options.ReadOnly, which explains why, and lock.go for what the lock does and
// does not promise.
func OpenReadOnly(dir string) (*Store, error) {
	return OpenWithOptions(dir, Options{ReadOnly: true, Constraints: ConstraintDrop})
}

// OpenWithOptions is Open with signing and verification configured.
//
// Verification happens during the replay Open performs, so a log that fails it
// fails the Open — the store does not come up holding records it could not
// authenticate. That is deliberately unlike the CSR digest, which is opt-in and
// checked separately: the log is replayed on every open regardless, so checking
// signatures there costs only the verification itself.
func OpenWithOptions(dir string, opts Options) (*Store, error) {
	// A setting that can never be honoured is refused before the directory is
	// touched. This is not the memory budget's kind of refusal -- there is no
	// store to measure and nothing to compare -- it is arithmetic on the Options
	// alone, so it costs nothing and it is the one refusal that can come first.
	//
	// It stops the Open rather than the first compaction because under
	// AutoCompact the first compaction is a background tick, and a configuration
	// error that surfaces only there surfaces to nobody.
	if err := opts.Compact.validate(); err != nil {
		return nil, fmt.Errorf("disk.Open: %w", err)
	}
	// The batch caps are the same kind of arithmetic-only refusal, and belong
	// beside it: a cap no batch could ever satisfy is a store nothing can write
	// to, and finding that out on the first write is finding it out too late.
	if err := validateBatchCaps(opts); err != nil {
		return nil, fmt.Errorf("disk.Open: %w", err)
	}

	// Discovery next, and before anything reads opts.MemoryBudget -- the Open
	// budget check below, the store field it is kept in, and openOpts, which a
	// reopen after a compaction is built from. Resolving it once here is what
	// makes a discovered budget as stable across a store's life as a stated one.
	//
	// It reads a file on linux and calls the kernel on windows, so it is not
	// free; it happens once per Open, only when asked for, and an Open is
	// already a log replay.
	opts, budgetSource := resolveMemoryBudget(opts)

	// Creating the directory is a write, so a read-only open does not do it. A
	// missing directory is then a real error rather than an empty store, which
	// is the honest answer: there is nothing there to read.
	// LiveReader implies ReadOnly. A live *writer* is not a thing: the mode
	// exists so a reader can follow a writer, and a writer already sees its own
	// commits.
	if opts.LiveReader {
		opts.ReadOnly = true
	}
	if opts.ReadOnly {
		if fi, err := os.Stat(dir); err != nil {
			return nil, fmt.Errorf("disk.Open: read-only open of %s: %w", dir, err)
		} else if !fi.IsDir() {
			return nil, fmt.Errorf("disk.Open: read-only open of %s: not a directory", dir)
		}
	} else if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("disk.Open: mkdir %s: %w", dir, err)
	}

	// The process lock comes before every other file in the directory. Opening
	// the WAL first would append a container header to a log another process is
	// mid-write on, which is the corruption this exists to prevent.
	// A live reader is a read-only store that takes no lock. The writer's
	// exclusive lock is deliberately unchanged: see live.go for why the reader is
	// the side that gives.
	mode := LockExclusive
	switch {
	case opts.LiveReader:
		mode = LockNone
	case opts.ReadOnly:
		mode = LockShared
	}
	lock, prevOwner, err := acquireStoreLock(dir, mode)
	if err != nil {
		return nil, fmt.Errorf("disk.Open: %w", err)
	}

	// A rebuilt log left stranded beside the one it was meant to replace is
	// recovered here, before anything opens either. Only a writer does it:
	// adoption is a rename, and a read-only store does not write to the
	// directory. See adoptOrphanedLog for why the state is unambiguous.
	walPath := filepath.Join(dir, walFileName)
	if opts.LiveReader {
		// Possibly the rebuilt log under its temporary name; see liveLogPath.
		walPath = liveLogPath(dir)
	}
	if !opts.ReadOnly {
		if err := adoptOrphanedLog(walPath); err != nil {
			lock.release()
			return nil, fmt.Errorf("disk.Open: %w", err)
		}
	}

	wal, err := openWALFor(walPath, opts.ReadOnly)
	if err != nil {
		lock.release()
		return nil, err
	}

	// The replay budget, checked here and not later, for two reasons that both
	// matter. The log handle is open, so its framing, container size and length
	// are all known and the figures in the refusal are the real ones. And
	// nothing has happened yet that a refusal would have to undo: no ledger is
	// open, and in particular the hash-chained unclean-restart entry has not
	// been written. A budget refusal is meant to be a routine, repeatable
	// outcome, and a routine outcome that appends a forensic record of a crash
	// that did not happen would be manufacturing history.
	if err := checkReplayBudget(wal, opts); err != nil {
		wal.Close()
		lock.release()
		return nil, fmt.Errorf("disk.Open: %w", err)
	}

	// The memory budget, second, and here for the same two reasons. It is the
	// more general of the two and the more expensive to evaluate — a model over
	// the image header as well as the log — so the specific, cheap refusal goes
	// first and a store over both budgets is refused on the one that names the
	// log, which is the actionable half.
	if err := checkOpenBudget(dir, opts); err != nil {
		wal.Close()
		lock.release()
		return nil, fmt.Errorf("disk.Open: %w", err)
	}

	s := &Store{
		dir:      dir,
		openOpts: opts,
		wal:      wal,
		lock:     lock,
		readOnly: opts.ReadOnly,
		// Only a writer inherits the previous holder's state. A read-only store
		// never claims the directory and never overwrites the marker, so the
		// crashed writer's evidence is still there for the next *writer* to find
		// — and a reader reporting a recovery it did not perform would be a claim
		// about a directory it does not own.
		unclean: !opts.ReadOnly && prevOwner.Present && !prevOwner.Clean,
		live:    opts.LiveReader,
		logPath: walPath,
		propIdx: index.NewPropertyIndex(),

		uniqueEdgeTypes: make(store.UniqueEdgeTypeSet),

		maxSnapshotAge:       opts.MaxSnapshotAge,
		idHeadroomWarn:       opts.IDHeadroomWarn,
		deltaSoftLimit:       opts.DeltaSoftLimit,
		memBudget:            opts.MemoryBudget,
		memBudgetSource:      budgetSource,
		reportProcMem:        opts.ReportProcessMemory,
		residentAdvice:       opts.ResidentAdvice,
		batchCaps:            batchCapsFor(opts),
		compactBufs:          compactBuffersFor(opts.Compact.MaxWorkingBytes),
		imageMode:            opts.ImageMode,
		indexMode:            opts.IndexMode,
		strictProjectionKeys: opts.RefuseUnindexedProjectionKeys,
		adjacency:            opts.Adjacency,
		syncOnCommit:         true,
		metrics:              opts.Metrics,
		signer:               opts.Signer,
		verifier:             opts.Verifier,
		requireSigned:        opts.RequireSignedCommits,
		attestActorID:        opts.AttestActorID,
		retention:            opts.Retention,
		redaction:            opts.RedactionPolicy,
	}

	// The three ledgers all open their files for append, so a read-only store
	// opens none of them and leaves the fields nil.
	//
	// Nothing is lost by that. Every read path here is directory-based —
	// ReadAuditLog, ReadRedactions, ReadGrants all take dir and parse the file
	// themselves — and every write path already refuses on a nil ledger, which is
	// how "the option is off" has always been expressed. Read-only is a third way
	// to reach the same nil, and the existing checks cover it unchanged.
	if opts.Audit && !opts.ReadOnly {
		al, aerr := OpenAuditLog(dir)
		if aerr != nil {
			wal.Close()
			lock.release()
			return nil, fmt.Errorf("disk.Open: %w", aerr)
		}
		s.audit = al
	}

	if opts.Redaction && !opts.ReadOnly {
		rl, rerr := OpenRedactionLedger(dir)
		if rerr != nil {
			wal.Close()
			s.audit.Close()
			lock.release()
			return nil, fmt.Errorf("disk.Open: %w", rerr)
		}
		s.redactions = rl
	}

	if opts.Roles && !opts.ReadOnly {
		gl, gerr := OpenGrantLedger(dir)
		if gerr != nil {
			wal.Close()
			s.audit.Close()
			s.redactions.Close()
			lock.release()
			return nil, fmt.Errorf("disk.Open: %w", gerr)
		}
		s.grants = gl
	}

	// A store that came up after an unclean shutdown says so in the log, once,
	// while it still knows. The store opens either way: WAL replay is crash-safe
	// by construction, and refusing would turn every killed process into a manual
	// intervention. What the record buys is that the *next* reader of the audit
	// log can tell an orderly history from one with a gap in it.
	if s.unclean && s.audit != nil {
		if aerr := s.recordAudit(AuditUncleanRestart, opts.AttestActorID,
			fmt.Sprintf("previous holder pid %d did not close the store", prevOwner.PID)); aerr != nil {
			wal.Close()
			s.audit.Close()
			s.redactions.Close()
			s.grants.Close()
			lock.release()
			return nil, fmt.Errorf("disk.Open: %w", aerr)
		}
	}

	// Resume numbering past whatever segments already exist, so a reopened store
	// never reuses a sequence number and never overwrites retired history.
	if existing, lerr := ListSegments(dir); lerr == nil {
		for _, seg := range existing {
			if seg.Sequence >= s.segmentSeq {
				s.segmentSeq = seg.Sequence + 1
			}
		}
	}

	// From here on the WAL file handle is open, so every failure path has to
	// close it — otherwise a rejected store leaks the handle and, on Windows,
	// leaves the file undeletable.
	fail := func(format string, args ...any) (*Store, error) {
		wal.Close()
		// The audit log is opened above, so every failure path from here has to
		// release it too. Missing this leaked a handle on exactly the paths that
		// matter — a store rejected by VerifyOnOpen — and on Windows left the
		// file undeletable.
		s.audit.Close()
		s.redactions.Close()
		s.grants.Close()
		// And the process lock, last, so a rejected store does not hold the
		// directory against the next opener — which on a VerifyOnOpen failure is
		// very likely to be an operator trying to work out what is wrong with it.
		s.lock.release()
		return nil, fmt.Errorf(format, args...)
	}

	// A store always has a view, even before the first compaction: an empty delta
	// layer over a nil image. Nothing may read or write the delta before this,
	// which is why it is published here rather than lazily.
	s.publishView(&view{delta: newDeltaLayer()})

	// The declaration catalogue, read before the image and before any entry
	// lands. Only the structural half is applied here: an ordered or composite
	// declaration made now is maintained by the entries as they arrive, rather
	// than backfilled over them afterwards, which is the argument csr_io.go
	// already makes for the image's GORD section. The constraints wait until the
	// data is complete — see below. Nothing is written: an open is not a writer.
	catalogue, err := readCatalogue(dir)
	if err != nil {
		return fail("disk.Open: %w", err)
	}
	s.applyCatalogueStructure(catalogue)

	// Try to load existing CSR.
	csrPath := filepath.Join(dir, csrFileName)
	if _, err := os.Stat(csrPath); err == nil {
		// Mapped or read once here, and used twice below. See verifyImage for
		// what that replaced.
		src, serr := s.openImage(csrPath)
		if serr != nil {
			return fail("disk.Open: load CSR: %w", serr)
		}
		// Integrity checks run before the image is loaded, so a store that fails
		// them never comes up holding records it could not vouch for.
		if opts.VerifyOnOpen {
			if err := verifyImage(src, opts); err != nil {
				src.discard()
				return fail("disk.Open: %w", err)
			}
		}
		// loadImage owns src from here, including releasing it if the parse
		// fails.
		if err := s.loadImage(src); err != nil {
			return fail("disk.Open: load CSR: %w", err)
		}
	}

	// Replay WAL into delta.
	// Attached after the store exists so the closure has somewhere to report to,
	// and only when a sink is listening: a nil observer is what keeps the fsync
	// path free of a clock read.
	if s.metrics != nil {
		wal.syncObserver = func(covered uint64, d time.Duration, serr error) {
			s.record(store.Metric{
				Kind:     store.MetricSync,
				Duration: d,
				Count:    int64(covered),
				Err:      serr,
			})
		}
	}

	if err := s.replayWAL(); err != nil {
		return fail("disk.Open: replay WAL: %w", err)
	}

	// The catalogue's constraints, now that every record exists. These validate
	// against the data, so they could not run before the replay: a unique key
	// checked against a half-replayed store would pass on a graph that does not
	// exist yet.
	dropped, err := s.applyCatalogueConstraints(catalogue, opts.Constraints)
	if err != nil {
		return fail("disk.Open: %w", err)
	}
	s.dropped = dropped

	// Open deliberately does NOT run VerifyIndexes.
	//
	// It measured ~200ms on a 100k-node store — an O(V+E) tax on every startup —
	// and it cannot catch much that is not already covered:
	//
	//   - Corruption in the CSR index section is rejected while parsing it
	//     (magic, counts, and bounds are all checked in readCSRIndexSection).
	//   - The label postings and the property index are both reconstructed by
	//     inserting through the normal code paths, which sort and deduplicate, so
	//     the structures are correct by construction whatever the file contained.
	//
	// What is left is engine bugs, which belong in tests, not in a startup scan.
	// Callers recovering a suspect store can run Graph.VerifyIndexes() and then
	// Graph.RebuildIndexes() explicitly.

	// Last, and only once the store is fully built: the trigger evaluates
	// StorageStats and may compact on its first tick, and neither is meaningful
	// against a store still being assembled. A read-only store never gets one —
	// it cannot compact, and mustWrite would refuse every tick.
	if opts.AutoCompact != nil && !opts.ReadOnly {
		s.startAutoCompact(*opts.AutoCompact, opts.AutoCompactInterval, opts.AutoCompactObserver)
	}

	return s, nil
}

// --- GraphStore implementation ---

func (s *Store) AddNode(n *store.Node) (store.NodeID, error) {
	if err := s.mustWrite(); err != nil {
		return store.InvalidNodeID, err
	}
	if len(n.Labels) == 0 {
		return store.InvalidNodeID, fmt.Errorf("AddNode: %w", store.ErrNoLabels)
	}
	stored := &store.Node{}
	stored.Labels = make([]store.NodeType, len(n.Labels))
	copy(stored.Labels, n.Labels)
	if len(n.Properties) > 0 {
		stored.Properties = make([]byte, len(n.Properties))
		copy(stored.Properties, n.Properties)
	}

	// ID assignment, WAL append, and delta apply are all done under s.mu so the
	// operation is atomic w.r.t. other writers (WAL order == apply order).
	s.mu.Lock()
	defer s.mu.Unlock()

	id := store.NodeID(s.nodeSeq.Add(1))
	stored.ID = id

	// Serialise to WAL payload: id(8) + labelCount(1) + labels(2*N) + propLen(4) + props
	if err := s.wal.appendNodeOwned(stored); err != nil {
		return store.InvalidNodeID, fmt.Errorf("AddNode: wal: %w", err)
	}

	// Visible immediately. A single write has never been fsynced (see Sync), so
	// its durability boundary is the next Sync and holding it invisible until
	// then would break read-your-writes without buying any guarantee.
	e := s.nextEpoch()
	s.putNode(e, stored)
	s.publishEpoch(e)

	return id, nil
}

// AddNodesBatch adds nodes in order and returns assigned IDs.
// On error, successfully written prefixes are committed and returned.
func (s *Store) AddNodesBatch(nodes []*store.Node) ([]store.NodeID, error) {
	if err := s.mustWrite(); err != nil {
		return nil, err
	}
	// The batch caps come first, for the same reason the label check below is
	// here and not inside the loop that takes identifiers: a refusal must burn
	// nothing. They are ahead of the label check because they are the cheaper
	// question on the batch that most needs refusing — an oversized batch stops
	// at a cap's worth of records rather than walking the whole slice.
	if err := s.batchCaps.checkNodes("AddNodesBatch", nodes); err != nil {
		return nil, err
	}

	// Labels are validated before the lock, and before any ID is taken. A
	// rejection partway through the loop below would burn the IDs of the prefix
	// it had already reached — permitted by the ID invariant, but pointless for
	// a check that needs nothing from the store.
	for i, n := range nodes {
		if len(n.Labels) == 0 {
			return nil, fmt.Errorf("AddNodesBatch: node %d: %w", i, store.ErrNoLabels)
		}
	}
	ids := make([]store.NodeID, len(nodes))
	stored := make([]*store.Node, len(nodes))

	// The batch is assembled, written and applied under one lock hold — WAL
	// order matches apply order, and the batch is atomic with respect to other
	// writers. The lock is released *before* waiting for durability, which is
	// what lets a second committer join this one's fsync; see syncgate.go.
	s.mu.Lock()
	unlock := sync.OnceFunc(s.mu.Unlock)
	defer unlock()

	for i, n := range nodes {
		id := store.NodeID(s.nodeSeq.Add(1))
		ids[i] = id

		node := &store.Node{ID: id}
		if len(n.Labels) > 0 {
			node.Labels = make([]store.NodeType, len(n.Labels))
			copy(node.Labels, n.Labels)
		}
		if len(n.Properties) > 0 {
			node.Properties = make([]byte, len(n.Properties))
			copy(node.Properties, n.Properties)
		}
		stored[i] = node
	}

	// One framed, transactional write for the whole batch. Previously this was
	// one WAL append — and one write syscall — per node.
	batch := newWALBatchFramed(len(stored)*64, s.wal.Framing())
	for _, n := range stored {
		node := n
		batch.addWith(walRecordNode, func(dst []byte) []byte {
			return appendMarshalledNode(dst, node)
		})
	}
	framed, err := batch.finish(s.nextCommitMeta(store.TxContext{}))
	if err != nil {
		return nil, err
	}
	var started time.Time
	if s.metricsOn() {
		started = time.Now()
	}
	ticket, err := s.wal.QueueBatch(framed)
	if err != nil {
		// Apply nothing. The commit marker never reached the file, so replay will
		// discard whatever partial bytes did — that absence *is* the rollback.
		// The IDs assigned above are simply never used, which is allowed: IDs are
		// monotonic and never reused, but were never promised to be dense.
		return nil, fmt.Errorf("AddNodesBatch: wal: %w", err)
	}

	epoch := s.nextEpoch()
	// Applied but not yet visible from here until publishEpoch below. See the
	// field's comment and retainLocked.
	s.unpublished.Add(1)
	defer s.unpublished.Add(-1)
	s.commitNodesBatch(epoch, stored)
	sync := s.syncOnCommit
	unlock()

	// One branch and one error, so the commit is measured once however it ends.
	// A failed commit is still a commit that happened, and an error rate is a
	// metric a sink hearing only about successes cannot compute.
	var cerr error
	if sync {
		// A failure here leaves the batch unpublished. The records are in the
		// delta but no reader can see them, because visibility is the epoch — so
		// a failed fsync cannot hand anyone a commit the disk never took. A later
		// commit whose sync succeeds covers these bytes too and will publish past
		// them, which is correct: at that point they are durable.
		cerr = s.wal.AwaitSync(ticket)
	} else {
		// Not waiting for durability does not mean leaving the bytes in a
		// queue. syncOnCommit off is documented as "durable at the next Sync,
		// Compact or Close", and bytes still in the ring would not survive even
		// a process kill, which the page cache does.
		cerr = s.wal.FlushQueued()
	}
	if s.metricsOn() {
		s.record(store.Metric{
			Kind:     store.MetricCommit,
			Duration: time.Since(started),
			Count:    int64(len(stored)),
			Bytes:    int64(len(framed)),
			Err:      cerr,
		})
	}
	if cerr != nil {
		return nil, fmt.Errorf("AddNodesBatch: wal: %w", cerr)
	}
	s.publishEpoch(epoch)
	s.reportDeltaBudget()
	return ids, nil
}

func (s *Store) AddEdge(e *store.Edge) (store.EdgeID, error) {
	if err := s.mustWrite(); err != nil {
		return store.InvalidEdgeID, err
	}
	if len(e.Labels) == 0 {
		return store.InvalidEdgeID, fmt.Errorf("AddEdge: %w", store.ErrNoLabels)
	}
	stored := &store.Edge{
		Src:    e.Src,
		Dst:    e.Dst,
		Weight: e.Weight,
	}
	stored.Labels = make([]store.EdgeType, len(e.Labels))
	copy(stored.Labels, e.Labels)
	if len(e.Properties) > 0 {
		stored.Properties = make([]byte, len(e.Properties))
		copy(stored.Properties, e.Properties)
	}

	// Endpoint validation, WAL append, and delta apply are all done under one
	// lock hold, so an edge can never be created onto a node that a concurrent
	// DeleteNode has already removed.
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.nodeExistsLocked(e.Src) {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Src}
	}
	if !s.nodeExistsLocked(e.Dst) {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Dst}
	}
	// Before the ID is issued and long before the WAL is touched: a refusal
	// whose record is already appended cannot be taken back by returning an
	// error.
	if err := s.checkEdgeCardinalityLocked(e.Src, e.Dst, stored.Labels, store.InvalidEdgeID); err != nil {
		return store.InvalidEdgeID, fmt.Errorf("AddEdge: %w", err)
	}

	id := store.EdgeID(s.edgeSeq.Add(1))
	stored.ID = id

	if err := s.wal.appendEdgeOwned(stored); err != nil {
		return store.InvalidEdgeID, fmt.Errorf("AddEdge: wal: %w", err)
	}

	ep := s.nextEpoch()
	s.putEdge(ep, stored, true)
	s.publishEpoch(ep)

	return id, nil
}

// AddEdgesBatch adds edges in order and returns assigned IDs.
// On error, successfully written prefixes are committed and returned.
func (s *Store) AddEdgesBatch(edges []*store.Edge) ([]store.EdgeID, error) {
	if err := s.mustWrite(); err != nil {
		return nil, err
	}
	// The caps first, and outside the lock for the same reason the label check
	// is: the size of the argument is a property of the argument. See
	// AddNodesBatch.
	if err := s.batchCaps.checkEdges("AddEdgesBatch", edges); err != nil {
		return nil, err
	}
	// Labels first, outside the lock: unlike endpoint validity, an empty label
	// set is a property of the argument and cannot change under a concurrent
	// writer, so there is nothing to gain by checking it under the lock.
	for i, e := range edges {
		if len(e.Labels) == 0 {
			return nil, fmt.Errorf("AddEdgesBatch: edge %d: %w", i, store.ErrNoLabels)
		}
	}
	ids := make([]store.EdgeID, len(edges))
	stored := make([]*store.Edge, len(edges))

	// Whole batch under one lock hold: endpoint validation cannot race a
	// concurrent DeleteNode, and WAL order matches apply order. Released before
	// the durability wait, as in AddNodesBatch.
	s.mu.Lock()
	unlock := sync.OnceFunc(s.mu.Unlock)
	defer unlock()

	claimed := s.batchPairClaims(edges)
	for i, e := range edges {
		// Validation happens before anything is written, so a failure here means
		// the transaction never started. Returning ids[:i] would name IDs for
		// edges that do not exist — the old non-atomic behaviour, and now wrong.
		if !s.nodeExistsLocked(e.Src) {
			return nil, &store.ErrInvalidEdge{MissingID: e.Src}
		}
		if !s.nodeExistsLocked(e.Dst) {
			return nil, &store.ErrInvalidEdge{MissingID: e.Dst}
		}
		// Cardinality against the store and against the batch: two duplicates
		// arriving together are the same violation as one arriving after the
		// other, and the adjacency cannot see the batch's own earlier edges.
		if err := s.checkEdgeCardinalityLocked(e.Src, e.Dst, e.Labels, store.InvalidEdgeID); err != nil {
			return nil, fmt.Errorf("AddEdgesBatch: edge %d: %w", i, err)
		}
		if err := claimBatchPair(claimed, s.uniqueEdgeTypes, e, i); err != nil {
			return nil, fmt.Errorf("AddEdgesBatch: edge %d: %w", i, err)
		}

		id := store.EdgeID(s.edgeSeq.Add(1))
		ids[i] = id

		edge := &store.Edge{
			ID:     id,
			Src:    e.Src,
			Dst:    e.Dst,
			Weight: e.Weight,
		}
		if len(e.Labels) > 0 {
			edge.Labels = make([]store.EdgeType, len(e.Labels))
			copy(edge.Labels, e.Labels)
		}
		if len(e.Properties) > 0 {
			edge.Properties = make([]byte, len(e.Properties))
			copy(edge.Properties, e.Properties)
		}
		stored[i] = edge
	}

	batch := newWALBatchFramed(len(stored)*80, s.wal.Framing())
	for _, e := range stored {
		edge := e
		batch.addWith(walRecordEdge, func(dst []byte) []byte {
			return appendMarshalledEdge(dst, edge)
		})
	}
	framed, err := batch.finish(s.nextCommitMeta(store.TxContext{}))
	if err != nil {
		return nil, err
	}
	var started time.Time
	if s.metricsOn() {
		started = time.Now()
	}
	ticket, err := s.wal.QueueBatch(framed)
	if err != nil {
		return nil, fmt.Errorf("AddEdgesBatch: wal: %w", err)
	}

	epoch := s.nextEpoch()
	s.unpublished.Add(1)
	defer s.unpublished.Add(-1)
	s.commitEdgesBatch(epoch, stored)
	sync := s.syncOnCommit
	unlock()

	var cerr error
	if sync {
		cerr = s.wal.AwaitSync(ticket)
	} else {
		// Not waiting for durability does not mean leaving the bytes in a
		// queue. syncOnCommit off is documented as "durable at the next Sync,
		// Compact or Close", and bytes still in the ring would not survive even
		// a process kill, which the page cache does.
		cerr = s.wal.FlushQueued()
	}
	if s.metricsOn() {
		s.record(store.Metric{
			Kind:     store.MetricCommit,
			Duration: time.Since(started),
			Count:    int64(len(stored)),
			Bytes:    int64(len(framed)),
			Err:      cerr,
		})
	}
	if cerr != nil {
		return nil, fmt.Errorf("AddEdgesBatch: wal: %w", cerr)
	}
	s.publishEpoch(epoch)
	s.reportDeltaBudget()
	return ids, nil
}

// The four mutators hold s.mu across BOTH the WAL append and the in-memory
// apply. This keeps the WAL record order identical to the apply order (so the
// reopened state always matches the live state) and makes DeleteNode's cascade
// atomic. The WAL append is safe under s.mu: WAL maintenance ops (Checkpoint /
// Truncate) only run inside Compact, which itself holds s.mu, so they can never
// run concurrently with a mutator.

// SetReindexPolicy implements store.Reindexer.
func (s *Store) SetReindexPolicy(p store.ReindexPolicy) {
	s.mu.Lock()
	s.reindexPolicy = p
	s.mu.Unlock()
}

// ReindexPolicy implements store.Reindexer.
func (s *Store) ReindexPolicy() store.ReindexPolicy {
	s.mu.RLock()
	p := s.reindexPolicy
	s.mu.RUnlock()
	return p
}

// DeclareOrderedNodeProperty implements store.OrderedIndexDeclarer.
func (s *Store) DeclareOrderedNodeProperty(key string) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	// The lock is here for the catalogue, not for the index: the sharded index
	// has its own locking, but the sidecar is written from what the store has
	// declared, so two declarations racing could each write a file missing the
	// other's key. It also closes a pre-existing race this method has always
	// had with itself.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.propIdx.DeclareOrderedNodeKey(key)
	return s.persistCatalogueLocked()
}

// DeclareOrderedEdgeProperty implements store.OrderedIndexDeclarer.
func (s *Store) DeclareOrderedEdgeProperty(key string) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.propIdx.DeclareOrderedEdgeKey(key)
	return s.persistCatalogueLocked()
}

// OrderedNodeProperties implements store.OrderedIndexDeclarer.
func (s *Store) OrderedNodeProperties() []string { return s.index().OrderedNodeKeys() }

// OrderedEdgeProperties implements store.OrderedIndexDeclarer.
func (s *Store) OrderedEdgeProperties() []string { return s.index().OrderedEdgeKeys() }

// DeclareUniqueNodeProperty implements store.UniqueIndexDeclarer.
//
// The store lock is held across the whole validation, so nothing can create a
// second holder of a value between the check finding one and the declaration
// taking effect.
func (s *Store) DeclareUniqueNodeProperty(key string) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	conflicts, err := s.propIdx.DeclareUniqueNodeKey(key, s.nodeExistsLocked)
	if err != nil {
		return err
	}
	if len(conflicts) > 0 {
		return &store.UniqueViolationsError{Kind: "node", Key: key, Conflicts: conflicts}
	}
	return s.persistCatalogueLocked()
}

// DeclareUniqueEdgeProperty implements store.UniqueIndexDeclarer.
func (s *Store) DeclareUniqueEdgeProperty(key string) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	conflicts, err := s.propIdx.DeclareUniqueEdgeKey(key, s.edgeExistsLocked)
	if err != nil {
		return err
	}
	if len(conflicts) > 0 {
		return &store.UniqueViolationsError{Kind: "edge", Key: key, Conflicts: conflicts}
	}
	return s.persistCatalogueLocked()
}

// UniqueNodeProperties implements store.UniqueIndexDeclarer.
func (s *Store) UniqueNodeProperties() []string { return s.index().UniqueNodeKeys() }

// Dir returns the directory this store was opened on.
//
// Present so the sidecar files that live beside the image — the label table, and
// whatever follows it — can be found by code outside this package without
// threading the path through every constructor.
func (s *Store) Dir() string { return s.dir }

// UniqueEdgeProperties implements store.UniqueIndexDeclarer.
func (s *Store) UniqueEdgeProperties() []string { return s.index().UniqueEdgeKeys() }

// UniqueNodeOwner implements store.UniqueIndexDeclarer.
func (s *Store) UniqueNodeOwner(key string, value []byte) (store.NodeID, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.propIdx.IsUniqueNodeKey(key) {
		return store.InvalidNodeID, false, fmt.Errorf("%w: %q", store.ErrKeyNotUnique, key)
	}
	owner, held := s.propIdx.NodeUniqueOwner(key, value)
	if !held || !s.nodeExistsLocked(owner) {
		return store.InvalidNodeID, false, nil
	}
	return owner, true, nil
}

// UniqueEdgeOwner implements store.UniqueIndexDeclarer.
func (s *Store) UniqueEdgeOwner(key string, value []byte) (store.EdgeID, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.propIdx.IsUniqueEdgeKey(key) {
		return store.InvalidEdgeID, false, fmt.Errorf("%w: %q", store.ErrKeyNotUnique, key)
	}
	owner, held := s.propIdx.EdgeUniqueOwner(key, value)
	if !held || !s.edgeExistsLocked(owner) {
		return store.InvalidEdgeID, false, nil
	}
	return owner, true, nil
}

// DeclareCompositeNodeProperties implements store.CompositeIndexDeclarer.
func (s *Store) DeclareCompositeNodeProperties(keys []string) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.propIdx.DeclareCompositeNodeKeys(keys); err != nil {
		return err
	}
	return s.persistCatalogueLocked()
}

// DeclareCompositeEdgeProperties implements store.CompositeIndexDeclarer.
func (s *Store) DeclareCompositeEdgeProperties(keys []string) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.propIdx.DeclareCompositeEdgeKeys(keys); err != nil {
		return err
	}
	return s.persistCatalogueLocked()
}

// CompositeNodeProperties implements store.CompositeIndexDeclarer.
func (s *Store) CompositeNodeProperties() [][]string { return s.index().CompositeNodeKeys() }

// CompositeEdgeProperties implements store.CompositeIndexDeclarer.
func (s *Store) CompositeEdgeProperties() [][]string { return s.index().CompositeEdgeKeys() }

// PurgeNodeIndex implements store.Reindexer. The purge is journalled so replay
// does not resurrect the superseded entries.
func (s *Store) PurgeNodeIndex(id store.NodeID) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.purgeNodeIndexLocked(id)
}

// PurgeEdgeIndex implements store.Reindexer.
func (s *Store) PurgeEdgeIndex(id store.EdgeID) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.purgeEdgeIndexLocked(id)
}

// purgeNodeIndexLocked journals then applies a property-index purge. Caller must
// hold s.mu. Writing the record unconditionally keeps replay deterministic even
// when the index currently holds nothing for id.
func (s *Store) purgeNodeIndexLocked(id store.NodeID) error {
	if err := s.wal.AppendNodePropPurge(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("purge node index: wal: %w", err)
	}
	s.propIdx.RemoveNode(id)
	return nil
}

// purgeEdgeIndexLocked journals then applies a property-index purge for an edge.
func (s *Store) purgeEdgeIndexLocked(id store.EdgeID) error {
	if err := s.wal.AppendEdgePropPurge(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("purge edge index: wal: %w", err)
	}
	s.propIdx.RemoveEdge(id)
	return nil
}

func (s *Store) UpdateNode(n *store.Node) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	if len(n.Labels) == 0 {
		return fmt.Errorf("UpdateNode: node %d: %w", n.ID, store.ErrNoLabels)
	}

	stored := &store.Node{ID: n.ID}
	stored.Labels = make([]store.NodeType, len(n.Labels))
	copy(stored.Labels, n.Labels)
	if len(n.Properties) > 0 {
		stored.Properties = make([]byte, len(n.Properties))
		copy(stored.Properties, n.Properties)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.nodeExistsLocked(n.ID) {
		return &store.ErrNotFound{Kind: "node", ID: uint64(n.ID)}
	}
	// The refusal has to happen before the log is touched: an update whose
	// record is already appended cannot be taken back by returning an error.
	if s.reindexPolicy == store.ReindexReject && s.propIdx.NodeHasEntries(n.ID) {
		return fmt.Errorf("UpdateNode: node %d: %w", n.ID, store.ErrIndexedPropertiesRequired)
	}
	// An edit is a fresh node record re-appended with the same ID; replay applies
	// it as an upsert (last write wins).
	if err := s.wal.appendNodeOwned(stored); err != nil {
		return fmt.Errorf("UpdateNode: wal: %w", err)
	}
	if s.reindexPolicy == store.ReindexPurge {
		if err := s.purgeNodeIndexLocked(n.ID); err != nil {
			return fmt.Errorf("UpdateNode: %w", err)
		}
	}
	e := s.nextEpoch()
	s.applyNodeUpsert(e, stored)
	s.publishEpoch(e)
	return nil
}

func (s *Store) UpdateEdge(e *store.Edge) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	if len(e.Labels) == 0 {
		return fmt.Errorf("UpdateEdge: edge %d: %w", e.ID, store.ErrNoLabels)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Endpoints are immutable: load the current edge to preserve Src/Dst.
	cur, ok := s.getEdgeLocked(e.ID)
	if !ok {
		return &store.ErrNotFound{Kind: "edge", ID: uint64(e.ID)}
	}

	stored := &store.Edge{
		ID:     e.ID,
		Src:    cur.Src,
		Dst:    cur.Dst,
		Weight: e.Weight,
	}
	stored.Labels = make([]store.EdgeType, len(e.Labels))
	copy(stored.Labels, e.Labels)
	if len(e.Properties) > 0 {
		stored.Properties = make([]byte, len(e.Properties))
		copy(stored.Properties, e.Properties)
	}

	// The refusal has to happen before the log is touched: an update whose
	// record is already appended cannot be taken back by returning an error.
	if s.reindexPolicy == store.ReindexReject && s.propIdx.EdgeHasEntries(e.ID) {
		return fmt.Errorf("UpdateEdge: edge %d: %w", e.ID, store.ErrIndexedPropertiesRequired)
	}
	// An update can add a declared label to an edge whose pair already has one.
	// Endpoints are immutable, so the pair to check is the stored one, and the
	// edge cannot collide with itself.
	if err := s.checkEdgeCardinalityLocked(cur.Src, cur.Dst, stored.Labels, e.ID); err != nil {
		return fmt.Errorf("UpdateEdge: %w", err)
	}
	if err := s.wal.appendEdgeOwned(stored); err != nil {
		return fmt.Errorf("UpdateEdge: wal: %w", err)
	}
	if s.reindexPolicy == store.ReindexPurge {
		if err := s.purgeEdgeIndexLocked(e.ID); err != nil {
			return fmt.Errorf("UpdateEdge: %w", err)
		}
	}
	ep := s.nextEpoch()
	s.applyEdgeUpsert(ep, stored)
	s.publishEpoch(ep)
	return nil
}

func (s *Store) DeleteEdge(id store.EdgeID) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.edgeExistsLocked(id) {
		return &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
	}
	if err := s.wal.AppendEdgeDelete(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("DeleteEdge: wal: %w", err)
	}
	e := s.nextEpoch()
	s.applyEdgeDelete(e, id)
	s.publishEpoch(e)
	return nil
}

func (s *Store) DeleteNode(id store.NodeID) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.nodeExistsLocked(id) {
		return &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	return s.deleteNodeLocked(id, s.incidentEdgeIDsLocked(id))
}

// deleteNodeLocked tombstones a node and the given incident edges.
//
// Split out so RedactNode can compute the cascade, write its ledger record, and
// then perform exactly the deletion it recorded — rather than recomputing the
// cascade and risking a record that describes a different set from the one
// actually removed. Caller must hold s.mu and must have checked the node exists.
func (s *Store) deleteNodeLocked(id store.NodeID, incident []store.EdgeID) error {
	// Durably record tombstones for the cascaded edges first, then the node, so
	// a crash mid-delete never leaves an edge pointing at a missing node.
	for _, eid := range incident {
		if err := s.wal.AppendEdgeDelete(marshalID(uint64(eid))); err != nil {
			return fmt.Errorf("DeleteNode: wal edge tombstone: %w", err)
		}
	}
	if err := s.wal.AppendNodeDelete(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("DeleteNode: wal node tombstone: %w", err)
	}

	// One epoch for the whole cascade, so no reader can catch the node deleted
	// and its edges not, or the reverse. The atomicity used to rest on holding
	// the lock across both; now it is a property of the records themselves.
	e := s.nextEpoch()
	for _, eid := range incident {
		s.applyEdgeDelete(e, eid)
	}
	s.applyNodeDelete(e, id)
	s.publishEpoch(e)
	return nil
}

// shadowCSRNode records that a CSR-resident node has been superseded by the
// delta layer, disabling the lock-free read path until the next Compact.
// Caller must hold s.mu.
func (s *Store) shadowCSRNode(id store.NodeID) {
	if s.readerLocked().nodeInCSR(id) {
		s.csrShadowed.Add(1)
	}
}

// shadowCSREdge is shadowCSRNode for edges. Caller must hold s.mu.
func (s *Store) shadowCSREdge(id store.EdgeID) {
	if s.readerLocked().edgeInCSR(id) {
		s.csrShadowed.Add(1)
	}
}

// --- in-memory apply helpers (shared by live mutators and WAL replay) ---
//
// All require s.mu held exclusively and all stamp the epoch they are given, so
// the record they write is visible to a reader at that epoch and to no earlier
// one. Replay uses them too: a log replayed at open produces the same version
// stack a live run would have, one epoch per record, which is what makes the
// reopened state indistinguishable from the state that was lost.

// applyNodeUpsert inserts or updates a node in the delta overlay. A CSR-resident
// node being updated simply gains a delta version that shadows the CSR copy.
func (s *Store) applyNodeUpsert(epoch uint64, n *store.Node) {
	s.shadowCSRNode(n.ID)
	s.putNode(epoch, n)
}

// applyEdgeUpsert inserts or updates an edge in the delta overlay.
func (s *Store) applyEdgeUpsert(epoch uint64, e *store.Edge) {
	s.shadowCSREdge(e.ID)
	s.putEdge(epoch, e, false)
}

// applyNodeDelete tombstones a node. The incident-edge cascade is performed by
// the caller (DeleteNode) or by separate edge tombstones on replay, so this does
// not touch edges.
func (s *Store) applyNodeDelete(epoch uint64, id store.NodeID) {
	s.shadowCSRNode(id)
	s.tombstoneNode(epoch, id)
}

// applyEdgeDelete tombstones an edge.
func (s *Store) applyEdgeDelete(epoch uint64, id store.EdgeID) {
	s.shadowCSREdge(id)
	s.tombstoneEdge(epoch, id)
}

// The *Locked helpers below are the mutation paths' view of the graph, and so
// resolve at the newest applied epoch rather than the newest visible one — see
// writerLocked. Read paths build their own reader; a Snapshot supplies one.
// All three run the identical resolution code in view.go and view_read.go.

// nodeExistsLocked reports whether the node is live. Caller must hold s.mu
// exclusively.
func (s *Store) nodeExistsLocked(id store.NodeID) bool {
	return s.writerLocked().nodeExists(id)
}

// edgeExistsLocked reports whether the edge is live. Caller must hold s.mu
// exclusively.
func (s *Store) edgeExistsLocked(id store.EdgeID) bool {
	return s.writerLocked().edgeExists(id)
}

// getEdgeLocked returns the authoritative live edge (delta override or CSR copy)
// or (nil, false) if it is missing or masked. Caller must hold s.mu exclusively.
func (s *Store) getEdgeLocked(id store.EdgeID) (*store.Edge, bool) {
	return s.writerLocked().edge(id)
}

// getNodeLocked is getEdgeLocked for nodes. Caller must hold s.mu exclusively.
func (s *Store) getNodeLocked(id store.NodeID) (*store.Node, bool) {
	return s.writerLocked().node(id)
}

// incidentEdgeIDsLocked returns the deduped, still-live edge IDs incident to id
// (as Src or Dst) gathered from both layers. Caller must hold s.mu exclusively.
func (s *Store) incidentEdgeIDsLocked(id store.NodeID) []store.EdgeID {
	return s.writerLocked().incidentEdgeIDsOf(id)
}

// csrFastRead returns the published CSR when a point read may safely bypass the
// store lock, along with the epoch the caller must re-check afterwards.
//
// It returns nil when no CSR exists or something has already shadowed part of
// it. Callers must confirm with csrFastReadValid *after* reading the record;
// only then is the answer known to have been current throughout.
func (s *Store) csrFastRead() (*CSRGraph, bool) {
	csr := s.viewPtr.Load().csr
	if csr == nil || s.csrShadowed.Load() != 0 {
		return nil, false
	}
	return csr, true
}

// csrFastReadValid reports whether nothing invalidated csr during the read.
//
// The validity check is on the **pointer itself**, not a separate generation
// counter. An earlier version used a counter and was wrong: `Compact` bumped the
// generation before storing the new pointer, so a reader could sample the new
// generation, load the *old* pointer, see a shadow count that had already been
// cleared, and accept a superseded CSR — tombstoned records and all. Every
// ordering of two independent atomics has some such window, because the reader
// needs the pointer and its validity to agree and they were separate words.
//
// Comparing the pointer removes the second word entirely. It is sound because
// the caller still holds a reference to the CSR it read, so that object cannot
// be collected and its address cannot be reused by a later one while the check
// is running.
func (s *Store) csrFastReadValid(csr *CSRGraph) bool {
	return s.csrShadowed.Load() == 0 && s.viewPtr.Load().csr == csr
}

func (s *Store) GetNode(id store.NodeID) (*store.Node, error) {
	// Unlocked path first: if the record lives in an unshadowed CSR, the store
	// lock buys nothing — the CSR cannot change under us.
	if csr, ok := s.csrFastRead(); ok {
		if rec, found := csr.GetNode(id); found {
			node := &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)}
			if s.csrFastReadValid(csr) {
				return node, nil
			}
		}
	}

	// Hold RLock across the delta + CSR lookup so the view read is not racing a
	// concurrent Compact swap.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n, ok := s.readerLocked().node(id); ok {
		return n, nil
	}
	return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
}

func (s *Store) GetEdge(id store.EdgeID) (*store.Edge, error) {
	if csr, ok := s.csrFastRead(); ok {
		if rec, found := csr.GetEdge(id); found {
			edge := rawEdgeToStore(rec)
			if s.csrFastReadValid(csr) {
				return edge, nil
			}
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.readerLocked().edge(id); ok {
		return e, nil
	}
	return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
}

// EdgesOf holds the read lock across BOTH the delta and CSR passes: the view and
// the versions it resolves must come from one consistent state, otherwise a
// concurrent Compact could swap the image between reading its adjacency and
// resolving the (now-replaced) delta — re-emitting a deleted edge.
func (s *Store) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().edgesOf(id, dir, edgeTypes), nil
}

// IncidentEdges implements store.AdjacencyReader. It walks the same delta-then-
// CSR sequence as EdgesOf, applying the same tombstone and delta-override rules,
// but appends to the caller's buffer instead of materialising edge records.
func (s *Store) IncidentEdges(dst []store.IncidentEdge, id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.IncidentEdge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().incidentEdges(dst, id, dir, edgeTypes), nil
}

// NodeExists implements store.AdjacencyReader.
func (s *Store) NodeExists(id store.NodeID) bool {
	if csr, ok := s.csrFastRead(); ok {
		if _, found := csr.GetNode(id); found && s.csrFastReadValid(csr) {
			return true
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().nodeExists(id)
}

// Neighbours resolves the incident edges into distinct neighbouring nodes.
//
// One lock hold covers the edge walk and every node resolution. It used to take
// the lock again per neighbour through GetNode, which was both slower and a
// weaker guarantee: the node records could come from a different instant than
// the edges that led to them.
func (s *Store) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().neighbours(id, dir, edgeTypes), nil
}

func (s *Store) NodesByType(t store.NodeType) ([]store.NodeID, error) {
	// Candidate collection and re-validation share one lock hold: taking the read
	// lock once per candidate (via GetNode) dominated the cost on large graphs.
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().nodesByType(t), nil
}

func (s *Store) EdgesByType(t store.EdgeType) ([]store.EdgeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().edgesByType(t), nil
}

func (s *Store) NodeCount() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().nodeCount(), nil
}

func (s *Store) EdgeCount() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readerLocked().edgeCount(), nil
}

func (s *Store) Close() error {
	// The background compactor goes first, and Close waits for it. Everything
	// closed below is something a compaction in flight is holding — the log it
	// checkpoints, the audit log it records to, the directory the process lock
	// covers — so closing them under a running one is a use-after-close, not a
	// race that merely loses work. See autocompact.go.
	s.stopAutoCompact()

	// The WAL closes even if the audit log fails to, because a WAL left open
	// leaks a file handle and, on Windows, leaves the file undeletable. The
	// audit error is still returned — it is not more important than durability,
	// but it is not nothing either.
	walErr := s.wal.Close()
	if err := s.audit.Close(); err != nil && walErr == nil {
		walErr = err
	}
	if err := s.redactions.Close(); err != nil && walErr == nil {
		walErr = err
	}
	if err := s.grants.Close(); err != nil && walErr == nil {
		walErr = err
	}

	// Every image mapping goes here, reachable or not. This is where a mapped
	// Properties slice stops being valid, which is the one sentence Close adds to
	// the aliasing contract — store.CloneNode is for a caller who needs the bytes
	// afterwards. Before the lock is released, so no other process takes the
	// directory while this one still has a section open on a file in it.
	if err := s.closeImages(); err != nil && walErr == nil {
		walErr = err
	}

	// The clean marker goes down after every store file is closed and before the
	// lock is released, which is the only window where "this process is done with
	// the directory" is true and still recordable. Marking earlier would call a
	// shutdown clean while the WAL was still open; marking later is impossible,
	// because releasing the lock closes the file the marker lives in.
	if err := s.lock.markClean(); err != nil && walErr == nil {
		walErr = err
	}
	// Released last, so no other process can take the directory while this one
	// still holds a handle on anything in it. Idempotent, so the double Close
	// that a defer plus an explicit call produces is still harmless.
	if err := s.lock.release(); err != nil && walErr == nil {
		walErr = err
	}
	return walErr
}

// LockMode reports which process-level lock this store holds: LockExclusive for
// a writable store, LockShared for one opened with Options.ReadOnly, LockNone
// for a live reader, which takes none.
//
// It reports the lock the store is holding rather than re-deriving one from the
// options it was opened with. That is the same distinction StorageStats draws
// throughout -- what is held, not what was asked for -- and here the two differ.
// Before v0.8.0 this re-derived from Options.ReadOnly, which LiveReader implies,
// and so answered LockShared for a store holding no lock at all: a shared lock
// excludes every writer, and not excluding the writer is precisely what OpenLive
// gives up. LockNone existed and was documented as the mode OpenLive takes; it
// was simply not reachable from a handle.
//
// Pair it with LockEnforced. A mode says what this process asked for and that
// says whether the platform made it true.
func (s *Store) LockMode() LockMode { return s.lock.mode }

// LockEnforced reports whether the process-level lock is real on this platform.
//
// It is true on Windows, Linux, macOS and the BSDs. It is false on platforms
// whose standard library offers no file-locking primitive — solaris, aix, plan9,
// js/wasm — where the store still opens but nothing excludes a second process.
// See lock_unsupported.go for why that is an honest false rather than a refusal
// to open.
func (s *Store) LockEnforced() bool { return lockingEnforced }

// ReadOnly reports whether this store refuses mutations.
func (s *Store) ReadOnly() bool { return s.readOnly }

// RecoveredFromUncleanShutdown reports that the process which last held this
// store exclusively did not close it — it crashed, was killed, or exited without
// calling Close.
//
// The store opened anyway, and that is not a compromise: WAL replay is crash-safe
// by construction, and refusing would make every killed process an incident
// requiring manual intervention. What this exposes is the choice the engine
// cannot make for the caller — whether *this* store, holding *this* evidence, is
// one where an unclean restart warrants running VerifyIndexes, reopening under
// Options.VerifyOnOpen, or escalating to a human.
//
// When Options.Audit is on, the same fact is recorded durably as
// AuditUncleanRestart. This accessor is the in-process view of it, and is false
// on a read-only store, which never claims the directory and so cannot have been
// the holder that left it dirty.
func (s *Store) RecoveredFromUncleanShutdown() bool { return s.unclean }

// mustWrite refuses a mutation on a read-only store.
//
// Every exported mutator calls it first. The WAL and the ledgers refuse writes
// on their own too, but those refusals arrive late and describe the wrong thing
// — a caller who gets "wal: file is read-only" from AddNode has to work out that
// the store, not the log, is what they misconfigured.
func (s *Store) mustWrite() error {
	if s.readOnly {
		return ErrReadOnly
	}
	return nil
}

// SetSyncOnCommit controls whether a batch write is flushed to the platter
// before it returns.
//
// Default is true: a transaction whose commit is not fsynced is not a commit,
// and batching gives the engine its first well-defined durability boundary. The
// cost is one fsync per batch (~0.1–1 ms depending on device), which a batch of
// any size amortises well and a batch of one does not.
//
// Turn it off only if you sync explicitly — via Compact or Close — and can
// afford to lose everything since.
func (s *Store) SetSyncOnCommit(v bool) {
	s.mu.Lock()
	s.syncOnCommit = v
	s.mu.Unlock()
}

// checkPropKey rejects a key the record encoding cannot round-trip.
//
// A property-index record stores the key length in a uint16, so a longer key is
// truncated on the way out and replays under a *different* key on the way back
// in — the index and the log disagreeing about what was registered, which is the
// one failure a journal exists to rule out. The bound is on the key only: value
// lengths are a uint32 and are caller-encoded bytes by contract.
func checkPropKey(key string) error {
	if len(key) > maxPropKeyLen {
		return fmt.Errorf("property key is %d bytes, limit is %d", len(key), maxPropKeyLen)
	}
	return nil
}

func (s *Store) IndexNodeProperty(id store.NodeID, key string, value []byte) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	if err := checkPropKey(key); err != nil {
		return fmt.Errorf("IndexNodeProperty: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexNodePropertyLocked(id, key, value)
}

func (s *Store) IndexEdgeProperty(id store.EdgeID, key string, value []byte) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	if err := checkPropKey(key); err != nil {
		return fmt.Errorf("IndexEdgeProperty: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexEdgePropertyLocked(id, key, value)
}

// indexNodePropertyLocked journals then applies one property-index entry. Caller
// must hold s.mu.
//
// Both halves of that sentence used to be false here: registration took no lock
// at all, and applied to the index *before* appending to the log. A failed
// append then returned an error having already registered the entry, leaving the
// index holding something the log has never heard of — which the next open
// silently drops. The four record mutators have always held s.mu across both
// steps precisely so that WAL order matches apply order; index registration is
// not exempt, and cannot be once a unique constraint has to decide whether a
// value is already taken.
func (s *Store) indexNodePropertyLocked(id store.NodeID, key string, value []byte) error {
	// Uniqueness is checked before the append, not after: a refused entry must
	// leave no record behind, and a record already in the log is not something a
	// returned error can take back.
	if s.propIdx.IsUniqueNodeKey(key) {
		if owner, held := s.propIdx.NodeUniqueOwner(key, value); held && owner != id {
			return &index.ErrUniqueTaken{Key: key, Value: value, Owner: uint64(owner)}
		}
	}
	if err := s.wal.appendNodePropOwned(id, key, value); err != nil {
		return fmt.Errorf("IndexNodeProperty: wal: %w", err)
	}
	// The checked form re-tests under the shard lock. The probe above is not
	// redundant with it: this one runs before the log is touched, that one
	// closes the window a probe cannot.
	if err := s.propIdx.IndexNodeUnique(id, key, value); err != nil {
		return err
	}
	return nil
}

// indexEdgePropertyLocked is indexNodePropertyLocked for edges.
func (s *Store) indexEdgePropertyLocked(id store.EdgeID, key string, value []byte) error {
	if s.propIdx.IsUniqueEdgeKey(key) {
		if owner, held := s.propIdx.EdgeUniqueOwner(key, value); held && owner != id {
			return &index.ErrUniqueTaken{Key: key, Value: value, Owner: uint64(owner)}
		}
	}
	if err := s.wal.appendEdgePropOwned(id, key, value); err != nil {
		return fmt.Errorf("IndexEdgeProperty: wal: %w", err)
	}
	if err := s.propIdx.IndexEdgeUnique(id, key, value); err != nil {
		return err
	}
	return nil
}

// NodesByProperty returns the nodes indexed under key with exactly value.
//
// The postings are resolved against the records before being returned. Without
// that step a caller can observe a deletion mid-flight: DeleteNode journals its
// tombstones and applies them under one write lock, but this path only locks the
// index, so it can read postings that the delete has not reached yet and hand
// back an entity the records no longer have. Filtering makes the records the
// authority, so every ID returned resolved to a live node at the instant it was
// checked.
//
// That is the guarantee, and it is deliberately not stronger: the node may be
// deleted the moment this returns. A caller that needs the result to stay true
// while it is used should take a Snapshot and query through that, which fixes
// the records the postings resolve against.
func (s *Store) NodesByProperty(key string, value []byte) ([]store.NodeID, error) {
	// The lock is taken before the postings are read, not after. A miss
	// therefore pays for it, which it used to avoid — the trade is that the
	// index and the records it is resolved against now come from one reader, so
	// a Refresh that replaces both cannot land between them and leave this
	// filtering the new graph's postings through the old graph's records.
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()
	ids := r.index().NodesByProperty(key, value)
	if len(ids) == 0 {
		return ids, nil
	}
	return liveNodeIDs(r, ids), nil
}

// EdgesByProperty returns the edges indexed under key with exactly value. It
// resolves postings against the records for the reason given on NodesByProperty.
func (s *Store) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()
	ids := r.index().EdgesByProperty(key, value)
	if len(ids) == 0 {
		return ids, nil
	}
	return liveEdgeIDs(r, ids), nil
}

// DegreeOf implements store.DegreeCounter. With no edge-type filter it answers
// from the CSR offset arrays and the delta adjacency lists without materialising
// a single edge record.
func (s *Store) DegreeOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) (int, error) {
	// Deliberately mirrors EdgesOf, which reports an unknown node as an empty
	// adjacency rather than an error on this backend.
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()
	if edgeTypes == nil {
		return r.degree(id, dir), nil
	}
	return r.degreeFiltered(id, dir, edgeTypes), nil
}

// commitNodesBatch applies node records to in-memory delta/index state.
// Caller must hold s.mu.
// ReserveNodeID implements store.Transactor.
func (s *Store) ReserveNodeID() store.NodeID {
	return store.NodeID(s.nodeSeq.Add(1))
}

// ReserveEdgeID implements store.Transactor.
func (s *Store) ReserveEdgeID() store.EdgeID {
	return store.EdgeID(s.edgeSeq.Add(1))
}

func (s *Store) commitNodesBatch(epoch uint64, nodes []*store.Node) {
	for _, n := range nodes {
		s.putNode(epoch, n)
	}
}

// commitEdgesBatch applies edge records to in-memory delta/index state.
// Caller must hold s.mu.
func (s *Store) commitEdgesBatch(epoch uint64, edges []*store.Edge) {
	for _, e := range edges {
		s.putEdge(epoch, e, true)
	}
}

func (s *Store) replayWAL() error {
	// Everything a replay applies is durable by definition — it is already on
	// the disk it was read from — so the epoch it lands at is published once at
	// the end rather than per record. Publishing per record would make a
	// half-applied log briefly visible, which is exactly the state replay exists
	// to avoid, and there is nothing a reader could usefully do with it.
	defer func() { s.publishEpoch(s.mutEpoch.Load()) }()

	var started time.Time
	if s.metricsOn() {
		started = time.Now()
	}
	before := s.mutEpoch.Load()

	off, err := s.wal.replayFrom(0, s.replayCallbacks())
	s.replayOff = off

	// Emitted once per open, and the single best indicator of how overdue a
	// compaction is: the log is bounded only by compaction, so its size is also
	// how long the next open will take. Count is epochs advanced rather than
	// records, because that is what the replay actually applied — a torn tail
	// contributes bytes and no epoch, which is the distinction worth seeing.
	if s.metricsOn() {
		s.record(store.Metric{
			Kind:     store.MetricReplay,
			Duration: time.Since(started),
			Count:    int64(s.mutEpoch.Load() - before),
			Bytes:    off,
			Err:      err,
		})
	}
	return err
}

// replayCallbacks is the handler set a replay applies through.
//
// Separated from replayWAL because a live reader replays the same records from
// an offset rather than from the start, and applying them through a second,
// parallel set of handlers is how the two paths would drift apart. There is one
// definition of what a log record does to the store.
func (s *Store) replayCallbacks() ReplayCallbacks {
	return ReplayCallbacks{
		Verifier:             s.verifier,
		RequireSignedCommits: s.requireSigned,
		CommitFunc:           s.observeCommitMeta,
		KeyTransitionFunc: func(payload []byte) error {
			t, err := readKeyTransitionPayload(payload)
			if err != nil {
				return fmt.Errorf("key transition: %w", err)
			}
			s.keyTimeline = append(s.keyTimeline, t)
			return nil
		},
		NodeFunc: func(payload []byte) error {
			n, err := unmarshalNode(payload)
			if err != nil {
				return err
			}
			// Upsert: a re-appended record for an existing ID is an edit.
			s.applyNodeUpsert(s.nextEpoch(), n)
			if uint64(n.ID) > s.nodeSeq.Load() {
				s.nodeSeq.Store(uint64(n.ID))
			}
			return nil
		},
		EdgeFunc: func(payload []byte) error {
			e, err := unmarshalEdge(payload)
			if err != nil {
				return err
			}
			s.applyEdgeUpsert(s.nextEpoch(), e)
			if uint64(e.ID) > s.edgeSeq.Load() {
				s.edgeSeq.Store(uint64(e.ID))
			}
			return nil
		},
		NodeDeleteFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.applyNodeDelete(s.nextEpoch(), store.NodeID(id))
			if id > s.nodeSeq.Load() {
				s.nodeSeq.Store(id)
			}
			return nil
		},
		EdgeDeleteFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.applyEdgeDelete(s.nextEpoch(), store.EdgeID(id))
			if id > s.edgeSeq.Load() {
				s.edgeSeq.Store(id)
			}
			return nil
		},
		NodePropFunc: func(payload []byte) error {
			id, key, value, err := unmarshalNodeProp(payload)
			if err != nil {
				return err
			}
			s.propIdx.IndexNode(id, key, value)
			return nil
		},
		EdgePropFunc: func(payload []byte) error {
			id, key, value, err := unmarshalEdgeProp(payload)
			if err != nil {
				return err
			}
			s.propIdx.IndexEdge(id, key, value)
			return nil
		},
		NodePropPurgeFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.propIdx.RemoveNode(store.NodeID(id))
			return nil
		},
		EdgePropPurgeFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.propIdx.RemoveEdge(store.EdgeID(id))
			return nil
		},
	}
}

// storeEdgeMatchesFilter returns true if the edge carries any label in the filter (OR semantics).
func storeEdgeMatchesFilter(filter []store.EdgeType, e *store.Edge) bool {
	for _, ft := range filter {
		if e.HasLabel(ft) {
			return true
		}
	}
	return false
}

// rawEdgeMatchesFilter returns true if the raw label slice contains any filter label (OR semantics).
func rawEdgeMatchesFilter(filter []store.EdgeType, labels []store.EdgeType) bool {
	for _, ft := range filter {
		if rawEdgeHasLabel(labels, ft) {
			return true
		}
	}
	return false
}

// csrBytes returns the property blob for a record that lives in the CSR.
//
// The CSR is immutable once published, and the API contract states that reads
// may hand back pointers into internal state (see API_REFERENCE §"Do not mutate
// returned structs"). Delta-resident reads already alias; this makes
// CSR-resident reads consistent with them.
func csrBytes(src []byte) []byte {
	return src
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// removeEdgeID returns ids with occurrences of target removed, preserving order.
//
// This is for **adjacency lists only**. Delta label postings are sorted and use
// store.DeleteSortedID instead; adjacency cannot be sorted because EdgesOf must
// return edges in insertion order for traversal results to stay stable.
func removeEdgeID(ids []store.EdgeID, target store.EdgeID) []store.EdgeID {
	out := ids[:0]
	for _, id := range ids {
		if id != target {
			out = append(out, id)
		}
	}
	return out
}

// GetNodesBatch resolves many node IDs with one lock-free attempt for the whole
// batch, falling back to a single locked pass for whatever it could not serve.
//
// The per-item path takes the store lock (or performs the lock-free
// validity dance) once per ID. Batching amortises both: one `csrFastRead`, one
// validity re-check, and at most one `RLock` for the remainder.
//
// **Order is preserved.** Records resolved without the lock and records resolved
// under it are interleaved back into request order rather than concatenated,
// because a caller that asked for [a b c] and received [a c b] would have no way
// to tell without comparing IDs.
func (s *Store) GetNodesBatch(ids []store.NodeID) ([]*store.Node, []store.NodeID) {
	if len(ids) == 0 {
		return nil, nil
	}
	// Positional: slot i holds the record for ids[i], or nil if unresolved.
	slots := make([]*store.Node, len(ids))
	pending := make([]int, 0, len(ids)) // indices still needing the locked path

	if csr, ok := s.csrFastRead(); ok {
		for i, id := range ids {
			if rec, found := csr.GetNode(id); found {
				slots[i] = &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)}
				continue
			}
			pending = append(pending, i)
		}
		if !s.csrFastReadValid(csr) {
			// The CSR moved under us: discard everything the fast path produced
			// and redo the whole batch under the lock. Partial trust is not an
			// option — some slots could be from the superseded CSR.
			for i := range slots {
				slots[i] = nil
			}
			pending = pending[:0]
			for i := range ids {
				pending = append(pending, i)
			}
		}
	} else {
		for i := range ids {
			pending = append(pending, i)
		}
	}

	if len(pending) > 0 {
		s.mu.RLock()
		r := s.readerLocked()
		for _, i := range pending {
			// One reader for the whole remainder, so the records that needed the
			// lock still describe a single instant.
			if n, ok := r.node(ids[i]); ok {
				slots[i] = n
			}
		}
		s.mu.RUnlock()
	}

	return compactNodes(slots, ids)
}

// GetEdgesBatch is GetNodesBatch for edges.
func (s *Store) GetEdgesBatch(ids []store.EdgeID) ([]*store.Edge, []store.EdgeID) {
	if len(ids) == 0 {
		return nil, nil
	}
	slots := make([]*store.Edge, len(ids))
	pending := make([]int, 0, len(ids))

	if csr, ok := s.csrFastRead(); ok {
		for i, id := range ids {
			if rec, found := csr.GetEdge(id); found {
				slots[i] = rawEdgeToStore(rec)
				continue
			}
			pending = append(pending, i)
		}
		if !s.csrFastReadValid(csr) {
			for i := range slots {
				slots[i] = nil
			}
			pending = pending[:0]
			for i := range ids {
				pending = append(pending, i)
			}
		}
	} else {
		for i := range ids {
			pending = append(pending, i)
		}
	}

	if len(pending) > 0 {
		s.mu.RLock()
		r := s.readerLocked()
		for _, i := range pending {
			// One reader resolves the whole remainder; the same code the point
			// path uses, so there is not a second delta-then-CSR rule to keep
			// correct.
			if e, ok := r.edge(ids[i]); ok {
				slots[i] = e
			}
		}
		s.mu.RUnlock()
	}

	return compactEdges(slots, ids)
}

// compactNodes removes the nil slots, preserving order, and reports which ids
// they corresponded to.
func compactNodes(slots []*store.Node, ids []store.NodeID) ([]*store.Node, []store.NodeID) {
	found := slots[:0]
	var missing []store.NodeID
	for i, n := range slots {
		if n == nil {
			missing = append(missing, ids[i])
			continue
		}
		found = append(found, n)
	}
	return found, missing
}

func compactEdges(slots []*store.Edge, ids []store.EdgeID) ([]*store.Edge, []store.EdgeID) {
	found := slots[:0]
	var missing []store.EdgeID
	for i, e := range slots {
		if e == nil {
			missing = append(missing, ids[i])
			continue
		}
		found = append(found, e)
	}
	return found, missing
}

// Sync forces everything written so far to durable storage.
//
// Single writes are not synced individually — that would cost an fsync per
// AddNode, turning a ~6 µs operation into a ~1 ms one. Batch commits do sync by
// default (see SetSyncOnCommit); for individual writes this is how a caller
// establishes a durability point without paying for a full Compact.
//
// After it returns, everything written before the call survives power loss.
func (s *Store) Sync() error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	return s.wal.Sync()
}
