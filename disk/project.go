package disk

// Serving indexed property values without reading the records that carry them.
//
// # Why this is a store method and not just the index's
//
// The index already answers it (index/project.go). What the store adds is the
// pointer: which property index this read is resolved against. That is
// `reader.index()` and not `Store.propIdx`, for the reason every other read path
// goes through a reader — a compaction publishes a new view with a new index, and
// a pass that loaded the field twice could straddle the two.
//
// # The lock, or the absence of one
//
// None is held across the walk, and none is needed. The projection touches no
// record, so there is nothing to resolve against the store's view; the postings
// it does touch are protected by the index's own shard locks, which
// index/project.go takes per chunk and never across the caller's callback. The
// store lock is taken once, to read the view pointer, and released.
//
// # What that costs in consistency, stated rather than implied
//
// This is **not** a point-in-time read of the whole pass. NodesByPropertyFunc is,
// because the index copies its delta side once when the stream is made; a
// projection resolves each chunk of ids as it reaches it, so a write landing
// mid-pass can be visible for later ids and not earlier ones. Each id's values
// are read at one instant — the same guarantee a loop of GetNode calls gives, and
// the same one §16's read model already describes. A caller that needs the whole
// pass at one instant wants a Snapshot and the records.

import (
	"context"

	"github.com/aoiflux/graphene/store"
)

var _ store.Projector = (*Store)(nil)

// ForEachNodeProjection implements store.Projector.
func (s *Store) ForEachNodeProjection(ctx context.Context, ids []store.NodeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) error {
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	if len(ids) == 0 || len(keys) == 0 {
		return nil
	}

	s.mu.RLock()
	idx := s.readerLocked().index()
	s.mu.RUnlock()

	var stop error
	idx.ProjectNodes(ids, keys, func(idIdx, keyIdx int, value []byte) bool {
		if err := cc.Step(); err != nil {
			stop = err
			return false
		}
		return fn(idIdx, keyIdx, value)
	})
	return stop
}

// ForEachEdgeProjection implements store.Projector.
func (s *Store) ForEachEdgeProjection(ctx context.Context, ids []store.EdgeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) error {
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	if len(ids) == 0 || len(keys) == 0 {
		return nil
	}

	s.mu.RLock()
	idx := s.readerLocked().index()
	s.mu.RUnlock()

	var stop error
	idx.ProjectEdges(ids, keys, func(idIdx, keyIdx int, value []byte) bool {
		if err := cc.Step(); err != nil {
			stop = err
			return false
		}
		return fn(idIdx, keyIdx, value)
	})
	return stop
}
