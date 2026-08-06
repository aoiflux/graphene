package disk

import (
	"testing"

	"github.com/aoiflux/graphene/store"
)

// InvalidNodeID / InvalidEdgeID are the zero values, and the CSR indexes its
// record arrays by ID with slot 0 unused. Anything that answers a question about
// slot 0 as though it were a record is reporting on a placeholder.

func TestInvalidID_DoesNotExist(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	// With nothing in the store at all.
	if s.NodeExists(store.InvalidNodeID) {
		t.Error("an empty store claims node 0 exists")
	}
	if s.edgeExistsLocked(store.InvalidEdgeID) {
		t.Error("an empty store claims edge 0 exists")
	}

	// With records present and compacted, which is when the CSR arrays exist and
	// slot 0 is a real allocated element.
	hub := addNodeD(t, s, store.NodeTypeMicroArtefact)
	spoke := addNodeD(t, s, store.NodeTypeTag)
	if _, err := s.AddEdge(&store.Edge{
		Src: hub, Dst: spoke, Labels: []store.EdgeType{store.EdgeTypeTaggedWith},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	if s.NodeExists(store.InvalidNodeID) {
		t.Error("a compacted store claims node 0 exists; slot 0 is the unused placeholder")
	}
	if s.edgeExistsLocked(store.InvalidEdgeID) {
		t.Error("a compacted store claims edge 0 exists; slot 0 is the unused placeholder")
	}

	// And the lookups agree with the existence check.
	if _, err := s.GetNode(store.InvalidNodeID); err == nil {
		t.Error("GetNode(0) returned a record")
	}
	if _, err := s.GetEdge(store.InvalidEdgeID); err == nil {
		t.Error("GetEdge(0) returned a record")
	}
}

// A proof must not be obtainable for the placeholder slot either — a proof about
// nothing that verifies is worse than no proof.
func TestInvalidID_IsNotProvable(t *testing.T) {
	_, s, _ := redactableStore(t)
	addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ProveNode(store.InvalidNodeID); err == nil {
		t.Fatal("built an inclusion proof for node 0")
	}
}
