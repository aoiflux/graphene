package memory

import (
	"errors"
	"testing"

	"graphene/store"
)

// helpers

func newNode(labels ...store.NodeType) *store.Node {
	return &store.Node{Labels: labels}
}

func newEdge(src, dst store.NodeID, labels ...store.EdgeType) *store.Edge {
	return &store.Edge{Src: src, Dst: dst, Labels: labels}
}

// --- AddNode / GetNode ---

func TestStore_AddGetNode(t *testing.T) {
	s := New()
	id, err := s.AddNode(newNode(store.NodeTypeCase))
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if id == store.InvalidNodeID {
		t.Fatal("AddNode returned InvalidNodeID")
	}
	n, err := s.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !n.HasLabel(store.NodeTypeCase) {
		t.Error("stored node missing Case label")
	}
}

func TestStore_GetNode_NotFound(t *testing.T) {
	s := New()
	_, err := s.GetNode(99)
	var nf *store.ErrNotFound
	if !errors.As(err, &nf) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_AddNode_MultiLabel(t *testing.T) {
	s := New()
	id, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact))
	n, _ := s.GetNode(id)
	if !n.HasLabel(store.NodeTypeEvidenceFile) || !n.HasLabel(store.NodeTypeMicroArtefact) {
		t.Error("multi-label node not stored correctly")
	}
}

func TestStore_AddNode_CopiesLabels(t *testing.T) {
	lbls := []store.NodeType{store.NodeTypeCase}
	n := &store.Node{Labels: lbls}
	s := New()
	id, _ := s.AddNode(n)
	lbls[0] = store.NodeTypeTag // mutate caller's slice
	stored, _ := s.GetNode(id)
	if stored.Labels[0] != store.NodeTypeCase {
		t.Error("AddNode should copy label slice; original mutation leaked in")
	}
}

// --- AddEdge / GetEdge ---

func TestStore_AddGetEdge(t *testing.T) {
	s := New()
	nid1, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	nid2, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	eid, err := s.AddEdge(newEdge(nid1, nid2, store.EdgeTypeSimilarTo))
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	e, err := s.GetEdge(eid)
	if err != nil {
		t.Fatalf("GetEdge: %v", err)
	}
	if e.Src != nid1 || e.Dst != nid2 {
		t.Error("edge src/dst mismatch")
	}
	if !e.HasLabel(store.EdgeTypeSimilarTo) {
		t.Error("edge missing SimilarTo label")
	}
}

func TestStore_AddEdge_InvalidSrc(t *testing.T) {
	s := New()
	nid, _ := s.AddNode(newNode(store.NodeTypeCase))
	_, err := s.AddEdge(newEdge(99, nid, store.EdgeTypeBelongsTo))
	var inv *store.ErrInvalidEdge
	if !errors.As(err, &inv) {
		t.Errorf("expected ErrInvalidEdge for invalid src, got %v", err)
	}
}

func TestStore_AddEdge_InvalidDst(t *testing.T) {
	s := New()
	nid, _ := s.AddNode(newNode(store.NodeTypeCase))
	_, err := s.AddEdge(newEdge(nid, 99, store.EdgeTypeBelongsTo))
	var inv *store.ErrInvalidEdge
	if !errors.As(err, &inv) {
		t.Errorf("expected ErrInvalidEdge for invalid dst, got %v", err)
	}
}

func TestStore_AddEdge_MultiLabel(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	eid, _ := s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo, store.EdgeTypeReuse))
	e, _ := s.GetEdge(eid)
	if !e.HasLabel(store.EdgeTypeSimilarTo) || !e.HasLabel(store.EdgeTypeReuse) {
		t.Error("multi-label edge not stored correctly")
	}
}

// --- Neighbours / EdgesOf ---

func TestStore_Neighbours_Outbound(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))
	s.AddEdge(newEdge(a, c, store.EdgeTypeContains))

	nbs, err := s.Neighbours(a, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nbs) != 2 {
		t.Errorf("expected 2 outbound neighbours, got %d", len(nbs))
	}
}

func TestStore_Neighbours_Inbound(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))

	nbs, _ := s.Neighbours(b, store.DirectionInbound, nil)
	if len(nbs) != 1 || nbs[0].Node.ID != a {
		t.Errorf("inbound neighbour should be a, got %v", nbs)
	}
}

func TestStore_EdgesOf_TypeFilter(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeCase))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))
	s.AddEdge(newEdge(a, c, store.EdgeTypeBelongsTo))

	edges, _ := s.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeContains})
	if len(edges) != 1 || !edges[0].HasLabel(store.EdgeTypeContains) {
		t.Errorf("type filter failed: got %v", edges)
	}
}

func TestStore_EdgesOf_MultiLabelFilter_OR(t *testing.T) {
	// An edge with labels [SimilarTo, Reuse] should be returned when filtering
	// by either SimilarTo or Reuse (OR semantics).
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo, store.EdgeTypeReuse))

	edges, _ := s.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeSimilarTo})
	if len(edges) != 1 {
		t.Errorf("OR filter: expected 1 edge via SimilarTo filter, got %d", len(edges))
	}
	edges, _ = s.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeReuse})
	if len(edges) != 1 {
		t.Errorf("OR filter: expected 1 edge via Reuse filter, got %d", len(edges))
	}
}

// --- NodesByType / EdgesByType ---

func TestStore_NodesByType(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeCase))
	b, _ := s.AddNode(newNode(store.NodeTypeCase, store.NodeTypeEvidenceFile))
	s.AddNode(newNode(store.NodeTypeMicroArtefact))

	ids, _ := s.NodesByType(store.NodeTypeCase)
	if len(ids) != 2 {
		t.Fatalf("NodesByType(Case): got %v, want 2 ids", ids)
	}
	set := map[store.NodeID]bool{a: true, b: true}
	for _, id := range ids {
		if !set[id] {
			t.Errorf("unexpected id %d", id)
		}
	}
}

func TestStore_EdgesByType(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeCase))
	e1, _ := s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	s.AddEdge(newEdge(a, c, store.EdgeTypeBelongsTo))

	ids, _ := s.EdgesByType(store.EdgeTypeSimilarTo)
	if len(ids) != 1 || ids[0] != e1 {
		t.Errorf("EdgesByType(SimilarTo): got %v, want [%d]", ids, e1)
	}
}

// --- Counts ---

func TestStore_Counts(t *testing.T) {
	s := New()
	s.AddNode(newNode(store.NodeTypeCase))
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))

	nc, _ := s.NodeCount()
	ec, _ := s.EdgeCount()
	if nc != 3 {
		t.Errorf("NodeCount = %d, want 3", nc)
	}
	if ec != 1 {
		t.Errorf("EdgeCount = %d, want 1", ec)
	}
}

// --- Property index ---

func TestStore_PropertyIndex(t *testing.T) {
	s := New()
	id, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.IndexNodeProperty(id, "sha256", []byte("deadbeef"))

	ids, err := s.NodesByProperty("sha256", []byte("deadbeef"))
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Errorf("NodesByProperty: got %v %v", ids, err)
	}

	ids, _ = s.NodesByProperty("sha256", []byte("notfound"))
	if len(ids) != 0 {
		t.Errorf("expected no results for missing value, got %v", ids)
	}
}

func TestStore_EdgePropertyIndex(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	eid, _ := s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	s.IndexEdgeProperty(eid, "bucket", []byte("high"))

	ids, _ := s.EdgesByProperty("bucket", []byte("high"))
	if len(ids) != 1 || ids[0] != eid {
		t.Errorf("EdgesByProperty: got %v", ids)
	}
}

// --- Close ---

func TestStore_Close(t *testing.T) {
	s := New()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
