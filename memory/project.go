package memory

// Serving indexed property values without reading the records that carry them.
//
// The memory backend holds the caller's own records, so nothing here avoids a
// fault or a copy the way the disk backend's mapped image makes it avoid one —
// see disk/project.go for what the saving actually is and when it exists. This
// is here so a consumer written against store.Projector behaves the same on both
// backends rather than discovering that one of them does not implement it.

import (
	"context"

	"github.com/aoiflux/graphene/store"
)

var (
	_ store.Projector         = (*Store)(nil)
	_ store.PropertyKeyLister = (*Store)(nil)
)

// NodePropKeys implements store.PropertyKeyLister.
//
// Exact here rather than an upper bound: this backend has no base to merge, so
// a key with no remaining entries is not in the index to be named.
func (s *Store) NodePropKeys() []string { return s.propIdx.NodePropKeys() }

// EdgePropKeys implements store.PropertyKeyLister.
func (s *Store) EdgePropKeys() []string { return s.propIdx.EdgePropKeys() }

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

	var stop error
	s.propIdx.ProjectNodes(ids, keys, func(idIdx, keyIdx int, value []byte) bool {
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

	var stop error
	s.propIdx.ProjectEdges(ids, keys, func(idIdx, keyIdx int, value []byte) bool {
		if err := cc.Step(); err != nil {
			stop = err
			return false
		}
		return fn(idIdx, keyIdx, value)
	})
	return stop
}
