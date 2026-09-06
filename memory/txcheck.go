package memory

// Read-set validation. See disk/txcheck.go and store/readcheck.go — this is the
// same feature reaching the same decision through plain maps instead of a delta
// layer over a CSR image, which is exactly why the comparison itself lives in
// store.ReadSetValidator rather than in either backend.

import "github.com/aoiflux/graphene/store"

// validateReadsLocked reports the first observation that no longer holds.
// Caller must hold s.mu exclusively.
func (s *Store) validateReadsLocked(checks []store.ReadCheck) error {
	if len(checks) == 0 {
		return nil
	}

	return store.ReadSetValidator{
		Node: func(id store.NodeID) (*store.Node, bool) {
			n, ok := s.nodes[id]
			return n, ok
		},
		Edge: func(id store.EdgeID) (*store.Edge, bool) {
			e, ok := s.edges[id]
			return e, ok
		},
		NodeExists: s.nodeExistsLocked,
		EdgesOf:    s.edgesOfLocked,
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
