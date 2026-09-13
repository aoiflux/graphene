package disk

// Handing ids to a sink instead of returning them.
//
// Every read in this package returns a slice, and a slice is a promise: these ids
// were true at one instant, they are yours, keep them. A caller that wants one id
// at a time gets that promise anyway and pays for it twice — once when the index
// merges its two sides into a new array, once when liveness is resolved into
// another — for a key whose answer is a million ids, that is sixteen megabytes to
// deliver eight bytes at a time.
//
// So these read the same answer and never hold it. The index hands over a cursor
// (index/stream.go) rather than an array, and what crosses the store boundary is
// one bounded batch at a time.
//
// # The lock, which is the whole design
//
// A property read has to resolve every id against the records, and that needs the
// store read lock. Holding it across the caller's sink is what disk/scan.go's
// header refuses: a sink is caller code running on this goroutine, and a sink
// that reads the store from inside its own loop takes a second read lock, which
// Go's RWMutex does not promise to grant while a writer waits behind the first.
// That is not a hazard a doc comment can discharge — for-each-id-then-load-it is
// the obvious use of an API like this one.
//
// So the lock is taken per batch and never across fn, exactly as scanChunked
// does, and the reader is pinned once for the whole pass so that the answer still
// comes from one instant. Pinning a reader across a lock release is safe for the
// reason a snapshot is: the reader holds the view, the view holds the image, and
// nothing frees an image a live reference names. Once a compaction publishes a
// new view the pinned one is frozen, and then no lock is needed at all — which is
// the same test snapshot.use makes.
//
// NodesByPropertyBatch takes the other choice: it holds the lock for the whole
// batch and says so. That is right for a batch, whose sink is called once per
// value and hands back a slice, and wrong here, where it is called once per id.

import (
	"context"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

var _ store.PropertyStreamer = (*Store)(nil)
var _ store.NodeQueryStreamer = (*Store)(nil)

// NodesByPropertyFunc implements store.PropertyStreamer.
func (s *Store) NodesByPropertyFunc(ctx context.Context, key string, value []byte,
	fn func(id store.NodeID) bool,
) error {
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	s.mu.RLock()
	r := s.readerLocked()
	st := r.index().NodesByPropertyStream(key, value)
	s.mu.RUnlock()
	return streamNodeIDs(s, r, &cc, st, fn)
}

// EdgesByPropertyFunc implements store.PropertyStreamer.
func (s *Store) EdgesByPropertyFunc(ctx context.Context, key string, value []byte,
	fn func(id store.EdgeID) bool,
) error {
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	s.mu.RLock()
	r := s.readerLocked()
	st := r.index().EdgesByPropertyStream(key, value)
	s.mu.RUnlock()
	return streamEdgeIDs(s, r, &cc, st, fn)
}

// streamNodeIDs drains st, resolving liveness under the store lock in batches and
// calling fn outside it.
//
// The two entity kinds get one function each rather than one generic function
// with a liveness predicate passed in, because the predicate would be a closure
// on a reader method and the whole point here is a loop that allocates nothing.
func streamNodeIDs(s *Store, r reader, cc *store.CancelCheck, st *index.IDStream[store.NodeID],
	fn func(store.NodeID) bool,
) error {
	buf := make([]store.NodeID, 0, scanChunk)
	size := scanFirstChunk
	for {
		// Once per batch rather than only every 256 ids, because a batch takes
		// the store lock and resolves up to five hundred records under it: a
		// pass abandoned mid-way should stop at the next batch boundary, not at
		// the next multiple of 256 ids, which a short answer never reaches.
		if err := cc.Check(); err != nil {
			return err
		}
		locked := s.cur() == r.v
		if locked {
			s.mu.RLock()
		}
		batch, done := st.Fill(buf[:0], size)
		// Compacted in place: the write index never passes the read index, so the
		// live ids of a batch need no second array to live in.
		live, stop := 0, error(nil)
		for _, id := range batch {
			if err := cc.Step(); err != nil {
				stop = err
				break
			}
			if r.nodeExists(id) {
				batch[live] = id
				live++
			}
		}
		if locked {
			s.mu.RUnlock()
		}
		buf = batch
		if stop != nil {
			return stop
		}
		for _, id := range batch[:live] {
			if !fn(id) {
				return nil
			}
		}
		if done {
			return nil
		}
		if size < scanChunk {
			size *= 2
		}
	}
}

// streamEdgeIDs is streamNodeIDs for edges.
func streamEdgeIDs(s *Store, r reader, cc *store.CancelCheck, st *index.IDStream[store.EdgeID],
	fn func(store.EdgeID) bool,
) error {
	buf := make([]store.EdgeID, 0, scanChunk)
	size := scanFirstChunk
	for {
		// See streamNodeIDs.
		if err := cc.Check(); err != nil {
			return err
		}
		locked := s.cur() == r.v
		if locked {
			s.mu.RLock()
		}
		batch, done := st.Fill(buf[:0], size)
		live, stop := 0, error(nil)
		for _, id := range batch {
			if err := cc.Step(); err != nil {
				stop = err
				break
			}
			if r.edgeExists(id) {
				batch[live] = id
				live++
			}
		}
		if locked {
			s.mu.RUnlock()
		}
		buf = batch
		if stop != nil {
			return stop
		}
		for _, id := range batch[:live] {
			if !fn(id) {
				return nil
			}
		}
		if done {
			return nil
		}
		if size < scanChunk {
			size *= 2
		}
	}
}

// ForEachNodeID implements store.NodeQueryStreamer.
//
// One query shape is answered without ever holding the answer, and every other
// shape is answered by running the query and handing the slice over. That is not
// a shortcut: an ordering, a window taken from the end, a conjunction narrowed
// over several filters and a label union all need the candidate set in hand
// before the first id of the result is known. Streaming those would mean
// pretending, and the pretence would be a second copy rather than none.
//
// The shape that does stream is the one this program was measured against: one
// equality filter, ascending, no labels and no id list — "every node holding
// this indexed value". For it the driver is the property index, its output is
// already ascending and already the result, and the window is a count.
//
// TestForEachNodeID_AgreesWithQueryNodeIDs is what keeps the two paths from
// drifting: whatever the query, this yields exactly what QueryNodeIDs returns, in
// that order.
func (s *Store) ForEachNodeID(ctx context.Context, query store.NodeQuery,
	fn func(id store.NodeID) bool,
) error {
	if f, ok := streamableNodeQuery(query); ok {
		return s.forEachStreamedNodeID(ctx, query, f, fn)
	}
	ids, err := s.QueryNodeIDsCtx(ctx, query)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// forEachStreamedNodeID is the streamed shape, with the window applied as a
// count rather than as a slice bound.
func (s *Store) forEachStreamedNodeID(ctx context.Context, query store.NodeQuery,
	f store.PropertyFilter, fn func(id store.NodeID) bool,
) error {
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	limit := query.Limit
	if limit == 0 {
		// Zero and negative both mean "no bound" to ApplyNodeQueryWindow, and
		// this counts down, so both become a count that never reaches zero.
		limit = -1
	}
	seen := 0
	return s.NodesByPropertyFunc(ctx, f.Key, f.Value, func(id store.NodeID) bool {
		if seen < offset {
			seen++
			return true
		}
		if limit < 0 {
			return fn(id)
		}
		if !fn(id) {
			return false
		}
		limit--
		return limit > 0
	})
}

// streamableNodeQuery reports the filter a query can be streamed from, and
// whether it can be.
//
// Conservative on purpose. MatchAny over a single filter is the same query as
// MatchAll over it, and a prefix or range filter on a declared ordered key drives
// ascending too — both could be admitted here. Neither is, because the cost of
// admitting a shape wrongly is a wrong answer on a path whose whole promise is
// that it returns what the other path returns, and the cost of refusing one is
// that a rare query allocates what it allocated before.
func streamableNodeQuery(q store.NodeQuery) (store.PropertyFilter, bool) {
	var none store.PropertyFilter
	if len(q.IDs) > 0 || len(q.Types) > 0 || len(q.Filters) != 1 {
		return none, false
	}
	if store.NormalizedFilterMode(q.FilterMode) != store.MatchAll {
		return none, false
	}
	if store.NormalizedQueryOrder(q.Order) != store.QueryOrderAsc {
		return none, false
	}
	if f := q.Filters[0]; f.Op == store.PropertyOpEqual {
		return f, true
	}
	return none, false
}
