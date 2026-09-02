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
