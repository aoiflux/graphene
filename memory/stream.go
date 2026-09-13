package memory

// Streaming reads, for parity of answers.
//
// A memory store keeps its postings in a map of slices. There is no disk-resident
// side to walk in place, so the ids under a value are already an array and handing
// them over one at a time saves nothing this backend was paying — the same
// position memory.NodesByPropertyBatch takes, and for the same reason: a caller
// that switched backends and found the method missing would have to write the loop
// twice.
//
// What is not parity-only is the lock. fn may read the store, so the store lock
// cannot be held across it, and that is a rule about this API rather than about
// the backend: the disk implementation takes it seriously because it has a lock
// worth holding, and this one takes it seriously because the contract says so and
// a caller must be able to rely on it whichever backend is underneath.

import (
	"context"

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
	ids, err := s.NodesByProperty(key, value)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := cc.Step(); err != nil {
			return err
		}
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// EdgesByPropertyFunc implements store.PropertyStreamer.
func (s *Store) EdgesByPropertyFunc(ctx context.Context, key string, value []byte,
	fn func(id store.EdgeID) bool,
) error {
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	ids, err := s.EdgesByProperty(key, value)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := cc.Step(); err != nil {
			return err
		}
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachNodeID implements store.NodeQueryStreamer.
func (s *Store) ForEachNodeID(ctx context.Context, query store.NodeQuery,
	fn func(id store.NodeID) bool,
) error {
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
