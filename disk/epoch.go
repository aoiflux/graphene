package disk

// Epoch bookkeeping: allocating them, publishing them, and knowing how far back
// history has to be kept.
//
// See view.go for what an epoch means. This file is the store's side of it —
// the two counters, the snapshot registry that decides version retention, and
// the accessors every read and write path starts from.

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/aoiflux/graphene/store"
)

// ErrSnapshotExpired is returned by a read through a snapshot held longer than
// Options.MaxSnapshotAge allows.
var ErrSnapshotExpired = errors.New("graphene: snapshot expired")

// cur returns the current view.
//
// Safe without the lock — the pointer is atomic — but callers reading the maps
// inside it still need s.mu, because a view is immutable as a *pair* and its
// delta layer is not.
func (s *Store) cur() *view { return s.viewPtr.Load() }

// readerLocked builds a read context at the newest visible epoch. Caller holds
// s.mu (read or write).
//
// The epoch is loaded before the view. Epochs advance and views gain content,
// so in that order the view is never missing a commit the epoch names; the
// reverse order can name a commit the view has not got. Under the lock either
// order would do, but the rule is written the same way everywhere so there is
// one thing to check rather than two.
func (s *Store) readerLocked() reader {
	epoch := s.visibleEpoch.Load()
	return reader{v: s.viewPtr.Load(), epoch: epoch}
}

// writerLocked builds a read context at the newest *applied* epoch, published
// or not. Caller holds s.mu exclusively.
//
// Mutators need this rather than readerLocked. With group commit a batch
// applies its records under the lock and publishes its epoch after the fsync,
// so between those two points visibleEpoch is behind what the delta actually
// holds. A reader is meant to be behind — that is the durability boundary doing
// its job. A writer is not: validating an edge's endpoints against a stale view
// would reject a node the log already contains, and compaction reading one
// would fold a partial graph into the image and then truncate the log holding
// the rest.
//
// Safe because s.mu is exclusive: every applied mutation is complete, and no
// new one can be in progress.
func (s *Store) writerLocked() reader {
	return reader{v: s.viewPtr.Load(), epoch: s.mutEpoch.Load()}
}

// nextEpoch allocates the epoch for one mutation. Caller holds s.mu
// exclusively, which is what makes the allocation order the apply order.
func (s *Store) nextEpoch() uint64 { return s.mutEpoch.Add(1) }

// publishEpoch makes every mutation up to e visible to new readers.
//
// Monotonic by CAS rather than by store, because with group commit two
// committers can finish out of order. Publishing the larger is right in that
// case and not merely convenient: one fsync covers every byte written before
// it, so if the later commit is durable the earlier one is too.
func (s *Store) publishEpoch(e uint64) {
	for {
		cur := s.visibleEpoch.Load()
		if e <= cur || s.visibleEpoch.CompareAndSwap(cur, e) {
			return
		}
	}
}

// publishView installs a new (image, delta) pair. Caller holds s.mu
// exclusively.
//
// The shadow counter resets because a freshly built CSR already incorporates
// every update and tombstone that had accumulated against the previous one.
// Order matters: publish the view *before* clearing the shadow count.
//
// Clearing first would open a window in which a reader still holding the old
// image sees a clean count and concludes that image is authoritative — even
// though it was just superseded, tombstones and all. Publishing first means any
// reader that trusts a zero count is re-checking against an image pointer that
// has already moved, and bails out.
func (s *Store) publishView(v *view) {
	// A view always names an index. Callers build the pair they mean and leave
	// this field alone; defaulting it here is for the ones that are only
	// replacing the image or the delta, which is all of them but Refresh.
	if v.idx == nil {
		v.idx = s.propIdx
	}
	s.viewPtr.Store(v)
	s.csrShadowed.Store(0)
}

// publishCSR installs a new image under a fresh, empty delta layer. Caller
// holds s.mu exclusively.
func (s *Store) publishCSR(csr *CSRGraph) {
	s.publishView(&view{csr: csr, delta: newDeltaLayer()})
}

// publishCompacted installs a rebuilt image under the delta that survived it.
// Caller holds s.mu exclusively.
//
// publishCSR is the special case where nothing survived, and its zeroing of
// csrShadowed is only right *because* nothing did. A compaction that ran with
// the lock released leaves behind every commit that landed while it built, and
// some of those supersede a record the new image holds — so the count has to be
// restored to what those survivors shadow rather than cleared. Clearing it
// would put the lock-free point read back on a path that returns the image's
// superseded copy.
//
// The two stores are in the opposite order to publishView's, and the order is
// the whole of the correctness argument. csrFastRead reads the image pointer
// and then the count; publishView can store the view first because the count it
// then writes is zero, so the worst a reader sees is a stale non-zero count and
// a needless trip through the lock. Here the count is going *up*, and a reader
// that sampled the new image before the count went up would find its record,
// re-check against a count that is still zero and a pointer that still matches,
// and accept a record the surviving delta supersedes. Storing the count first
// closes that: sequential consistency means any reader that observes the new
// image also observes the count that was stored before it.
func (s *Store) publishCompacted(csr *CSRGraph, delta *deltaLayer, shadowed int64) {
	s.csrShadowed.Store(shadowed)
	// Compaction rebuilds the image and splices the delta; it does not rebuild
	// the index, which already describes both halves and goes on describing
	// them. Carrying the same pointer is what makes that explicit rather than
	// implicit in a field nobody reassigned.
	s.viewPtr.Store(&view{csr: csr, delta: delta, idx: s.propIdx})
}

// --- snapshot registry ---

// snapshotRegistry tracks open snapshots.
//
// It exists for two reasons and neither is correctness of the reads
// themselves: version chains need to know how far back to keep history, and an
// operator staring at a store whose memory will not come down needs to be able
// to see that something is holding an old epoch open. Guarded by Store.mu.
type snapshotRegistry struct {
	// open counts snapshots by the epoch they read at. A map rather than a
	// min-heap because the number of distinct open epochs is small in every
	// intended use and a map keeps close() O(1) with no invalidation dance.
	open map[uint64]int
	// oldest is the smallest key in open, cached because retention is consulted
	// on every mutation and open is consulted on Close.
	oldest      uint64
	count       int
	oldestSince time.Time
}

func (g *snapshotRegistry) add(epoch uint64, now time.Time) {
	if g.open == nil {
		g.open = make(map[uint64]int)
	}
	if g.count == 0 || epoch < g.oldest {
		g.oldest = epoch
		g.oldestSince = now
	}
	g.open[epoch]++
	g.count++
}

func (g *snapshotRegistry) remove(epoch uint64, now time.Time) {
	n, ok := g.open[epoch]
	if !ok {
		return
	}
	if n == 1 {
		delete(g.open, epoch)
	} else {
		g.open[epoch] = n - 1
	}
	g.count--
	if g.count == 0 {
		g.oldest = 0
		g.oldestSince = time.Time{}
		return
	}
	if epoch != g.oldest {
		return
	}
	// The oldest just went away; find the next one. O(distinct open epochs),
	// paid once per closed snapshot rather than once per mutation, which is the
	// asymmetry the cache exists for.
	first := true
	for e := range g.open {
		if first || e < g.oldest {
			g.oldest, first = e, false
		}
	}
	g.oldestSince = now
}

// retain reports the oldest epoch a live snapshot can still read at.
//
// ok is false when there are none — the common case, and the one where a
// version chain collapses to a single entry and the delta's memory profile is
// the plain map it replaced.
func (g *snapshotRegistry) retain() (epoch uint64, ok bool) {
	if g.count == 0 {
		return 0, false
	}
	return g.oldest, true
}

// retainLocked is the store's view of the same question. Caller holds s.mu.
func (s *Store) retainLocked() (uint64, bool) { return s.snaps.retain() }

// --- Snapshot ---

// Snapshot implements store.Snapshotter.
//
// The returned view is fixed: every read through it sees the graph as it stood
// when the snapshot was taken, however long it is held and whatever writers do
// meanwhile. Close it — a snapshot pins the image and the delta versions it
// needs, and an abandoned one keeps both alive.
func (s *Store) Snapshot() (store.Snapshot, error) {
	// Both words under the read lock. Compact is the one operation that replaces
	// a delta layer rather than extending it, and it holds the write lock, so
	// this is what stops a snapshot pairing a new layer with an old epoch.
	s.mu.Lock()
	now := s.now()
	r := reader{v: s.viewPtr.Load(), epoch: s.visibleEpoch.Load()}
	s.snaps.add(r.epoch, now)
	s.mu.Unlock()

	// Outside the lock, deliberately, and the reason is the same one that keeps
	// the query metric outside its read lock: a sink is caller code, and this
	// one runs under the *write* lock, so a slow implementation would stall
	// every writer in the process for as long as it took.
	s.record(store.Metric{Kind: store.MetricSnapshotOpen})
	return &snapshot{s: s, r: r, opened: now, maxAge: s.maxSnapshotAge}, nil
}

// snapshot is a pinned read view. It implements the read half of
// store.GraphStore and nothing else — a snapshot that could be written to would
// not be one.
type snapshot struct {
	s      *Store
	r      reader
	opened time.Time
	maxAge time.Duration
	closed atomic.Bool
}

// use returns the reader and whether the caller may hold s.mu.
//
// A delta layer that Compact has superseded is never written to again, so a
// snapshot taken before the last compaction reads with no lock at all. That is
// not an optimisation bolted on afterwards; it falls out of layers being
// replaced rather than mutated, and it is why long analytical reads stop
// contending with ingest once a compaction has passed under them.
func (sn *snapshot) use() (reader, bool, error) {
	if sn.closed.Load() {
		return reader{}, false, errors.New("graphene: snapshot is closed")
	}
	if sn.maxAge > 0 && sn.s.now().Sub(sn.opened) > sn.maxAge {
		return reader{}, false, ErrSnapshotExpired
	}
	// Frozen once the store has moved on to a different view.
	return sn.r, sn.s.viewPtr.Load() == sn.r.v, nil
}

func (sn *snapshot) Epoch() uint64 { return sn.r.epoch }

func (sn *snapshot) Close() error {
	if sn.closed.Swap(true) {
		return nil
	}
	sn.s.mu.Lock()
	closed := sn.s.now()
	sn.s.snaps.remove(sn.r.epoch, closed)
	sn.s.mu.Unlock()
	// How long it was held, so a sink can find the leaked one without keeping
	// state of its own. Taken from the store's clock rather than time.Now, which
	// is what lets a test pin it — the same indirection nowUnixNano exists for.
	sn.s.record(store.Metric{
		Kind:  store.MetricSnapshotClose,
		Count: closed.Sub(sn.opened).Nanoseconds(),
	})
	return nil
}

func (sn *snapshot) GetNode(id store.NodeID) (*store.Node, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	if n, ok := r.node(id); ok {
		return n, nil
	}
	return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
}

func (sn *snapshot) GetEdge(id store.EdgeID) (*store.Edge, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	if e, ok := r.edge(id); ok {
		return e, nil
	}
	return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
}

func (sn *snapshot) NodeExists(id store.NodeID) bool {
	r, locked, err := sn.use()
	if err != nil {
		return false
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.nodeExists(id)
}

func (sn *snapshot) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.edgesOf(id, dir, edgeTypes), nil
}

func (sn *snapshot) IncidentEdges(dst []store.IncidentEdge, id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.IncidentEdge, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.incidentEdges(dst, id, dir, edgeTypes), nil
}

func (sn *snapshot) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.neighbours(id, dir, edgeTypes), nil
}

func (sn *snapshot) DegreeOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) (int, error) {
	r, locked, err := sn.use()
	if err != nil {
		return 0, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	if edgeTypes == nil {
		return r.degree(id, dir), nil
	}
	return r.degreeFiltered(id, dir, edgeTypes), nil
}

func (sn *snapshot) NodesByType(t store.NodeType) ([]store.NodeID, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.nodesByType(t), nil
}

func (sn *snapshot) EdgesByType(t store.EdgeType) ([]store.EdgeID, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.edgesByType(t), nil
}

func (sn *snapshot) NodeCount() (uint64, error) {
	r, locked, err := sn.use()
	if err != nil {
		return 0, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.nodeCount(), nil
}

func (sn *snapshot) EdgeCount() (uint64, error) {
	r, locked, err := sn.use()
	if err != nil {
		return 0, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.edgeCount(), nil
}

// NodesByProperty resolves postings against the snapshot's own view.
//
// The property index itself is not versioned — it is a live structure with its
// own locking — so what the snapshot pins is the *record* set. A posting for a
// node that did not yet exist at this epoch, or that has since been deleted,
// resolves to nothing and is dropped. The reverse case, an entry indexed after
// the snapshot was taken, is the one this cannot exclude, and it is why
// NodesByProperty on a snapshot is documented as "the postings now, resolved
// against the graph then" rather than as a fully versioned read.
func (sn *snapshot) NodesByProperty(key string, value []byte) ([]store.NodeID, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	ids := r.index().NodesByProperty(key, value)
	if len(ids) == 0 {
		return ids, nil
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	out := ids[:0]
	for _, id := range ids {
		if r.nodeExists(id) {
			out = append(out, id)
		}
	}
	return out, nil
}

// EdgesByProperty is NodesByProperty for edges; see there for what it pins.
func (sn *snapshot) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	ids := r.index().EdgesByProperty(key, value)
	if len(ids) == 0 {
		return ids, nil
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	out := ids[:0]
	for _, id := range ids {
		if r.edgeExists(id) {
			out = append(out, id)
		}
	}
	return out, nil
}

// QueryNodeIDs runs the planner's own plan against this snapshot's state.
//
// Not a reimplementation: it is the same queryNodeIDs the live store calls,
// handed a different reader. The property index it consults is the live one —
// see NodesByProperty for what that does and does not fix — but every candidate
// it produces is resolved against the pinned records, so the answer describes
// one graph rather than whatever each stage happened to find.
func (sn *snapshot) QueryNodeIDs(q store.NodeQuery) ([]store.NodeID, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return sn.s.queryNodeIDs(r, q)
}

func (sn *snapshot) QueryEdgeIDs(q store.EdgeQuery) ([]store.EdgeID, error) {
	r, locked, err := sn.use()
	if err != nil {
		return nil, err
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return sn.s.queryEdgeIDs(r, q)
}
