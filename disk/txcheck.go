package disk

// Read-set validation.
//
// A tracked transaction arrives carrying what it read. This re-reads each of
// those observations under the write lock the transaction is about to be
// applied under, and refuses the whole transaction if any of them has moved.
// See store/readcheck.go for what the guarantee is and is not.
//
// The comparison itself is deliberately not here. It lives in
// store.ReadSetValidator, which both backends drive, so the two cannot drift
// into accepting different read sets — the same reason the parity suite exists,
// applied one level down.

import "github.com/aoiflux/graphene/store"

// validateReadsLocked reports the first observation that no longer holds.
// Caller must hold s.mu exclusively.
//
// Every read goes through writerLocked rather than readerLocked, for the reason
// that function documents: with group commit, visibleEpoch lags what the delta
// already holds, and validating against a stale view would report a conflict
// with a commit that is already applied — refusing a transaction over a write
// that had, in fact, already happened.
func (s *Store) validateReadsLocked(checks []store.ReadCheck) error {
	if len(checks) == 0 {
		return nil
	}
	r := s.writerLocked()

	return store.ReadSetValidator{
		Node:       r.node,
		Edge:       r.edge,
		NodeExists: r.nodeExists,
		// A missing node is the empty incident set rather than an error, which
		// is what this walk already does. The in-memory backend's EdgesOf
		// reports one instead, so its validator is written to this rule rather
		// than to its own — a read set must not validate differently on the two.
		EdgesOf: r.edgesOf,
		UniqueNodeOwner: func(key string, value []byte) (store.NodeID, bool, error) {
			owner, held := s.propIdx.NodeUniqueOwner(key, value)
			if !held || !s.nodeExistsLocked(owner) {
				return store.InvalidNodeID, false, nil
			}
			return owner, true, nil
		},
		UniqueEdgeOwner: func(key string, value []byte) (store.EdgeID, bool, error) {
			owner, held := s.propIdx.EdgeUniqueOwner(key, value)
			if !held || !s.edgeExistsLocked(owner) {
				return store.InvalidEdgeID, false, nil
			}
			return owner, true, nil
		},
	}.Validate(checks)
}
