package disk

// Counting without materialising what is counted.
//
// The same answers memory/aggregate.go gives, reached the hard way. A label
// posting here is a *candidate*, not an answer: entries are left behind while a
// snapshot pins an older view, and every read re-resolves each candidate against
// the record visible at its own epoch. So a per-type count is O(candidates)
// rather than O(types) — which is the same order EdgeCount and NodeCount already
// pay, and still far cheaper than the NodesByType-per-type loop it replaces,
// because nothing is copied into a result slice.

import (
	"context"

	"github.com/aoiflux/graphene/store"
)

// CountNodesByType implements store.Aggregator.
func (s *Store) CountNodesByType(ctx context.Context) (map[store.NodeType]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	r := s.readerLocked()
	out := make(map[store.NodeType]uint64)
	for _, t := range r.nodeTypesPresent() {
		if err := cc.Step(); err != nil {
			return nil, err
		}
		// nodesByType dedupes across the two layers and drops candidates this
		// reader cannot see, which is exactly the count wanted. It allocates a
		// slice per type; the alternative is a second implementation of the
		// resolution rules, and a second one is a second thing that can be
		// wrong.
		if n := len(r.nodesByType(t)); n > 0 {
			out[t] = uint64(n)
		}
	}
	return out, nil
}

// CountEdgesByType implements store.Aggregator.
func (s *Store) CountEdgesByType(ctx context.Context) (map[store.EdgeType]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	r := s.readerLocked()
	out := make(map[store.EdgeType]uint64)
	for _, t := range r.edgeTypesPresent() {
		if err := cc.Step(); err != nil {
			return nil, err
		}
		if n := len(r.edgesByType(t)); n > 0 {
			out[t] = uint64(n)
		}
	}
	return out, nil
}

// nodeTypesPresent returns every node type either layer has a posting for.
//
// A superset of the types actually live — a posting can name nothing this reader
// can see — which is why the caller drops the empty ones rather than trusting
// this list.
func (r reader) nodeTypesPresent() []store.NodeType {
	seen := make(map[store.NodeType]struct{}, len(r.v.delta.nodesByType))
	for t := range r.v.delta.nodesByType {
		seen[t] = struct{}{}
	}
	if r.v.csr != nil {
		for t := range r.v.csr.nodesByLabel {
			seen[t] = struct{}{}
		}
	}
	out := make([]store.NodeType, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	return out
}

// edgeTypesPresent is nodeTypesPresent for edges.
func (r reader) edgeTypesPresent() []store.EdgeType {
	seen := make(map[store.EdgeType]struct{}, len(r.v.delta.edgesByType))
	for t := range r.v.delta.edgesByType {
		seen[t] = struct{}{}
	}
	if r.v.csr != nil {
		for t := range r.v.csr.edgesByLabel {
			seen[t] = struct{}{}
		}
	}
	out := make([]store.EdgeType, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	return out
}

// CountNodesByProperty implements store.Aggregator.
func (s *Store) CountNodesByProperty(ctx context.Context, key string) (map[string]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	r := s.readerLocked()
	out := make(map[string]uint64)
	var stop error
	s.propIdx.ForEachNodeValue(key, func(value []byte, ids []store.NodeID) bool {
		var live uint64
		for _, id := range ids {
			if err := cc.Step(); err != nil {
				stop = err
				return false
			}
			if r.nodeExists(id) {
				live++
			}
		}
		if live > 0 {
			out[string(value)] = live
		}
		return true
	})
	if stop != nil {
		return nil, stop
	}
	return out, nil
}

var _ store.PropertyBatcher = (*Store)(nil)

// NodesByPropertyBatch implements store.PropertyBatcher.
//
// The read lock is held for the whole batch, which is what CountNodesByProperty
// already does for a whole key and for the same reason: the postings and the
// records they are resolved against must come from one reader, or a Refresh
// landing mid-pass would filter the new graph's postings through the old graph's
// records. Writers therefore wait on a batch, and a batch over a million values
// is not a short wait — a caller that cannot block writers for that long should
// split its values across several calls, which costs one extra cursor per call
// and nothing else.
//
// Because that lock is held across fn, **fn must not read this store**. A GetNode
// or a query from inside the callback asks for a second read lock on a goroutine
// that already holds one, and Go's RWMutex does not promise it while a writer is
// queued: the pass deadlocks. That is the cost of the scratch buffer below, and it
// is the whole reason store.PropertyStreamer exists as a separate contract — its
// per-id sink is allowed to read the store precisely because it holds no lock
// across the callback. Use NodesByPropertyFunc where the callback needs to load
// what an id names.
//
// Liveness is resolved into a reused buffer rather than in place, because the ids
// the index hands over are its own scratch. Nothing here allocates per value.
func (s *Store) NodesByPropertyBatch(ctx context.Context, key string, values [][]byte,
	fn func(i int, ids []store.NodeID) bool,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	r := s.readerLocked()
	var live []store.NodeID
	var stop error
	err := s.propIdx.NodesByPropertyBatch(key, values, func(i int, ids []store.NodeID) bool {
		live = live[:0]
		for _, id := range ids {
			if err := cc.Step(); err != nil {
				stop = err
				return false
			}
			if r.nodeExists(id) {
				live = append(live, id)
			}
		}
		// A value whose every holder is dead is a value with no holder, which is
		// the one reading consistent with the index's own: it reports no callback
		// for a value it does not hold, and a record layer that reports an empty
		// one would make the two disagree about the same absence.
		if len(live) == 0 {
			return true
		}
		return fn(i, live)
	})
	if stop != nil {
		return stop
	}
	return err
}

// EdgesByPropertyBatch implements store.PropertyBatcher.
func (s *Store) EdgesByPropertyBatch(ctx context.Context, key string, values [][]byte,
	fn func(i int, ids []store.EdgeID) bool,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	r := s.readerLocked()
	var live []store.EdgeID
	var stop error
	err := s.propIdx.EdgesByPropertyBatch(key, values, func(i int, ids []store.EdgeID) bool {
		live = live[:0]
		for _, id := range ids {
			if err := cc.Step(); err != nil {
				stop = err
				return false
			}
			if r.edgeExists(id) {
				live = append(live, id)
			}
		}
		if len(live) == 0 {
			return true
		}
		return fn(i, live)
	})
	if stop != nil {
		return stop
	}
	return err
}

// CountEdgesByProperty implements store.Aggregator.
func (s *Store) CountEdgesByProperty(ctx context.Context, key string) (map[string]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	r := s.readerLocked()
	out := make(map[string]uint64)
	var stop error
	s.propIdx.ForEachEdgeValue(key, func(value []byte, ids []store.EdgeID) bool {
		var live uint64
		for _, id := range ids {
			if err := cc.Step(); err != nil {
				stop = err
				return false
			}
			if r.edgeExists(id) {
				live++
			}
		}
		if live > 0 {
			out[string(value)] = live
		}
		return true
	})
	if stop != nil {
		return nil, stop
	}
	return out, nil
}
