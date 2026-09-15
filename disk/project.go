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
	"errors"
	"fmt"
	"slices"

	"github.com/aoiflux/graphene/store"
)

// ErrUnindexedProjectionKey is returned by the projection methods, under
// Options.RefuseUnindexedProjectionKeys, for a key no id in this store is
// indexed under.
//
// Wrapped with the offending key, so errors.Is identifies the class and the
// message identifies the mistake. The first key in request order wins, so the
// error is the same on every run over the same request.
var ErrUnindexedProjectionKey = errors.New("disk: no id is indexed under this projection key")

// checkProjectionKeys refuses a key absent from the index's key list.
//
// Absence is the only direction this may act on. Under a mapped base the list is
// an upper bound -- a key whose entries have all been retracted is still named --
// so a key present in it may still project nothing, and refusing on presence
// would be wrong. A key missing from it matches nothing for certain, whatever ids
// are passed, which is exactly the check the option promises.
//
// known is sorted, which NodePropKeys guarantees.
func checkProjectionKeys(known, want []string) error {
	for _, k := range want {
		if _, found := slices.BinarySearch(known, k); !found {
			return fmt.Errorf("%w: %q", ErrUnindexedProjectionKey, k)
		}
	}
	return nil
}

var (
	_ store.Projector         = (*Store)(nil)
	_ store.PropertyKeyLister = (*Store)(nil)
)

// NodePropKeys implements store.PropertyKeyLister.
//
// Through the reader for the reason the file header gives: a compaction
// publishes a new view with a new index, and reading Store.propIdx directly
// would let a pass straddle the two. Under a mapped base the list is the union
// of the image's keys and the delta's, which is what makes it usable on a store
// that has just been opened and never written to -- every key it has is in the
// base, and a validator that saw only the delta would reject all of them.
func (s *Store) NodePropKeys() []string {
	s.mu.RLock()
	idx := s.readerLocked().index()
	s.mu.RUnlock()
	return idx.NodePropKeys()
}

// EdgePropKeys implements store.PropertyKeyLister.
func (s *Store) EdgePropKeys() []string {
	s.mu.RLock()
	idx := s.readerLocked().index()
	s.mu.RUnlock()
	return idx.EdgePropKeys()
}

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

	if s.strictProjectionKeys {
		if err := checkProjectionKeys(idx.NodePropKeys(), keys); err != nil {
			return err
		}
	}

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

	if s.strictProjectionKeys {
		if err := checkProjectionKeys(idx.EdgePropKeys(), keys); err != nil {
			return err
		}
	}

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
