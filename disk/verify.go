package disk

// Structural self-check and repair across every index the store holds. Split out
// of store.go, unchanged.
//
// Both are structural only. Neither can tell whether an indexed *value* still
// matches its entity's properties, because those values are caller-encoded and
// the property blob is opaque to the engine — see Store.VerifyIndexes.

import (
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// VerifyIndexes implements store.IndexVerifier. It cross-checks the CSR label
// postings, the CSR adjacency arrays, the delta label postings, and the property
// index against the live records, returning the first inconsistency found.
func (s *Store) VerifyIndexes() error {
	if err := s.propIdx.Verify(); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.csr != nil {
		if err := s.csr.verifyLabelIndex(); err != nil {
			return err
		}
		if err := s.csr.verifyAdjacency(); err != nil {
			return err
		}
	}

	// Delta label postings must describe live delta records. Membership sets are
	// built once and reused for the reverse direction; scanning the postings per
	// record would make verification quadratic in the size of the largest label.
	deltaNodeMembers := make(map[store.NodeType]map[store.NodeID]struct{}, len(s.deltaNodesByType))
	for lbl, ids := range s.deltaNodesByType {
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("delta node label index: %v postings are not strictly ascending", lbl)
		}
		seen := make(map[store.NodeID]struct{}, len(ids))
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("delta node label index: %v lists node %d twice", lbl, id)
			}
			seen[id] = struct{}{}
			n, ok := s.deltaNodes[id]
			if !ok {
				return fmt.Errorf("delta node label index: %v lists node %d, which is not in the delta", lbl, id)
			}
			if !n.HasLabel(lbl) {
				return fmt.Errorf("delta node label index: %v lists node %d, which does not carry that label", lbl, id)
			}
		}
		deltaNodeMembers[lbl] = seen
	}
	for id, n := range s.deltaNodes {
		for _, lbl := range n.Labels {
			if _, ok := deltaNodeMembers[lbl][id]; !ok {
				return fmt.Errorf("delta node label index: node %d carries %v but is missing from the postings", id, lbl)
			}
		}
	}

	deltaEdgeMembers := make(map[store.EdgeType]map[store.EdgeID]struct{}, len(s.deltaEdgesByType))
	for lbl, ids := range s.deltaEdgesByType {
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("delta edge label index: %v postings are not strictly ascending", lbl)
		}
		seen := make(map[store.EdgeID]struct{}, len(ids))
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("delta edge label index: %v lists edge %d twice", lbl, id)
			}
			seen[id] = struct{}{}
			e, ok := s.deltaEdges[id]
			if !ok {
				return fmt.Errorf("delta edge label index: %v lists edge %d, which is not in the delta", lbl, id)
			}
			if !e.HasLabel(lbl) {
				return fmt.Errorf("delta edge label index: %v lists edge %d, which does not carry that label", lbl, id)
			}
		}
		deltaEdgeMembers[lbl] = seen
	}
	for id, e := range s.deltaEdges {
		for _, lbl := range e.Labels {
			if _, ok := deltaEdgeMembers[lbl][id]; !ok {
				return fmt.Errorf("delta edge label index: edge %d carries %v but is missing from the postings", id, lbl)
			}
		}
	}

	// A delta entry and a tombstone for the same ID are mutually exclusive.
	for id := range s.deltaNodes {
		if _, del := s.deletedNodes[id]; del {
			return fmt.Errorf("node %d is both present in the delta and masked as deleted", id)
		}
	}
	for id := range s.deltaEdges {
		if _, del := s.deletedEdges[id]; del {
			return fmt.Errorf("edge %d is both present in the delta and masked as deleted", id)
		}
	}

	// Delta adjacency must agree with the delta edges' endpoints.
	for nodeID, a := range s.deltaAdj {
		for _, eid := range a.out {
			e, ok := s.deltaEdges[eid]
			if !ok {
				return fmt.Errorf("delta adjacency: node %d lists outbound edge %d, which is not in the delta", nodeID, eid)
			}
			if e.Src != nodeID {
				return fmt.Errorf("delta adjacency: node %d lists outbound edge %d, whose Src is %d", nodeID, eid, e.Src)
			}
		}
		for _, eid := range a.in {
			e, ok := s.deltaEdges[eid]
			if !ok {
				return fmt.Errorf("delta adjacency: node %d lists inbound edge %d, which is not in the delta", nodeID, eid)
			}
			if e.Dst != nodeID {
				return fmt.Errorf("delta adjacency: node %d lists inbound edge %d, whose Dst is %d", nodeID, eid, e.Dst)
			}
		}
	}

	// The property index must not outlive the entities it describes.
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if !s.nodeExistsLocked(id) {
			return fmt.Errorf("property index: node %d has entries but is not live", id)
		}
	}
	for _, id := range s.propIdx.IndexedEdgeIDs() {
		if !s.edgeExistsLocked(id) {
			return fmt.Errorf("property index: edge %d has entries but is not live", id)
		}
	}
	return nil
}

// RebuildIndexes implements store.IndexRebuilder. It recomputes the CSR label
// postings, the delta label postings, and the delta adjacency from the records
// they describe, then drops property-index entries whose entity is not live.
//
// Everything it touches is derived state held in memory, so it writes nothing to
// disk and needs no WAL records. Use it after recovering a store whose indexes
// may not match its records; a following Compact persists the repaired state.
func (s *Store) RebuildIndexes() error {
	s.mu.Lock()

	if s.csr != nil {
		s.csr.buildLabelIndex()
	}

	s.deltaNodesByType = make(map[store.NodeType][]store.NodeID, len(s.deltaNodesByType))
	s.deltaEdgesByType = make(map[store.EdgeType][]store.EdgeID, len(s.deltaEdgesByType))
	s.deltaAdj = make(map[store.NodeID]*deltaAdj, len(s.deltaAdj))

	for id, n := range s.deltaNodes {
		s.indexDeltaNodeLabels(id, n.Labels)
		s.ensureDeltaAdj(id)
	}
	for id, e := range s.deltaEdges {
		s.indexDeltaEdgeLabels(id, e.Labels)
		// Mirrors applyEdgeUpsert: delta adjacency lists only edges the CSR
		// adjacency does not already cover, so the two never double-count.
		if !s.edgeInCSR(id) {
			s.ensureDeltaAdj(e.Src).out = append(s.ensureDeltaAdj(e.Src).out, id)
			s.ensureDeltaAdj(e.Dst).in = append(s.ensureDeltaAdj(e.Dst).in, id)
		}
	}

	var deadNodes []store.NodeID
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if !s.nodeExistsLocked(id) {
			deadNodes = append(deadNodes, id)
		}
	}
	var deadEdges []store.EdgeID
	for _, id := range s.propIdx.IndexedEdgeIDs() {
		if !s.edgeExistsLocked(id) {
			deadEdges = append(deadEdges, id)
		}
	}
	s.mu.Unlock()

	for _, id := range deadNodes {
		s.propIdx.RemoveNode(id)
	}
	for _, id := range deadEdges {
		s.propIdx.RemoveEdge(id)
	}
	return nil
}
