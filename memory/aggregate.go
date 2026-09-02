package memory

// Counting without materialising what is counted.
//
// The label postings and the property index already hold these answers; what
// was missing was a way to ask for them. A caller wanting a per-type breakdown
// had to call NodesByType once per type and take the length of each returned
// slice — one copy of every ID in the graph, built to be discarded.

import (
	"context"

	"github.com/aoiflux/graphene/store"
)

// CountNodesByType implements store.Aggregator.
//
// O(types) and exact. The postings are maintained on every write and unmaintained
// on every delete, so their lengths are the answer rather than an upper bound —
// unlike the disk backend, where a posting is a candidate until it is resolved.
func (s *Store) CountNodesByType(ctx context.Context) (map[store.NodeType]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	out := make(map[store.NodeType]uint64, len(s.nodesByType))
	for t, ids := range s.nodesByType {
		if err := cc.Step(); err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			continue
		}
		out[t] = uint64(len(ids))
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
	out := make(map[store.EdgeType]uint64, len(s.edgesByType))
	for t, ids := range s.edgesByType {
		if err := cc.Step(); err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			continue
		}
		out[t] = uint64(len(ids))
	}
	return out, nil
}

// CountNodesByProperty implements store.Aggregator.
//
// The liveness filter is not pedantry: a posting can outlive the entity it names
// while a snapshot pins an older view, and a distribution that counted those
// would report entities no read would ever return. It is the same filter the
// unique declaration applies for the same reason.
func (s *Store) CountNodesByProperty(ctx context.Context, key string) (map[string]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	out := make(map[string]uint64)
	var stop error
	s.propIdx.ForEachNodeValue(key, func(value []byte, ids []store.NodeID) bool {
		var live uint64
		for _, id := range ids {
			if err := cc.Step(); err != nil {
				stop = err
				return false
			}
			if s.nodeExistsLocked(id) {
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

// CountEdgesByProperty implements store.Aggregator.
func (s *Store) CountEdgesByProperty(ctx context.Context, key string) (map[string]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	out := make(map[string]uint64)
	var stop error
	s.propIdx.ForEachEdgeValue(key, func(value []byte, ids []store.EdgeID) bool {
		var live uint64
		for _, id := range ids {
			if err := cc.Step(); err != nil {
				stop = err
				return false
			}
			if s.edgeExistsLocked(id) {
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
