package store

import (
	"bytes"
	"testing"
)

// TestCloneNode_SharesNothing is the whole contract: the copy must not alias the
// original in any field, because the original may be a view of a mapped file the
// caller is about to outlive.
func TestCloneNode_SharesNothing(t *testing.T) {
	orig := &Node{
		ID:         42,
		Labels:     []NodeType{NodeTypeEvidenceFile, NodeTypeMicroArtefact},
		Properties: []byte("payload"),
	}
	got := CloneNode(orig)

	if got == orig {
		t.Fatal("CloneNode returned the original")
	}
	if got.ID != orig.ID {
		t.Errorf("ID = %d, want %d", got.ID, orig.ID)
	}
	if !bytes.Equal(got.Properties, orig.Properties) {
		t.Errorf("Properties = %q, want %q", got.Properties, orig.Properties)
	}
	if len(got.Labels) != len(orig.Labels) {
		t.Fatalf("Labels length %d, want %d", len(got.Labels), len(orig.Labels))
	}

	// Mutate through the original's backing arrays. Labels is the field easy to
	// forget, and forgetting it is not a partial copy but a wrong one: the copy
	// would keep pointing at the store's label arena.
	orig.Properties[0] = 'X'
	orig.Labels[0] = NodeTypeCase
	if got.Properties[0] == 'X' {
		t.Error("the copy's Properties alias the original's")
	}
	if got.Labels[0] == NodeTypeCase {
		t.Error("the copy's Labels alias the original's")
	}
}

// TestCloneEdge_SharesNothing is CloneNode's assertion for the edge, including
// the three scalar fields a copy could silently drop.
func TestCloneEdge_SharesNothing(t *testing.T) {
	orig := &Edge{
		ID:         7,
		Src:        1,
		Dst:        2,
		Labels:     []EdgeType{EdgeTypeSimilarTo},
		Weight:     0.75,
		Properties: []byte("edge-payload"),
	}
	got := CloneEdge(orig)

	if got.ID != 7 || got.Src != 1 || got.Dst != 2 || got.Weight != 0.75 {
		t.Errorf("scalar fields did not survive the copy: %+v", *got)
	}
	orig.Properties[0] = 'X'
	orig.Labels[0] = EdgeTypeContains
	if got.Properties[0] == 'X' {
		t.Error("the copy's Properties alias the original's")
	}
	if got.Labels[0] == EdgeTypeContains {
		t.Error("the copy's Labels alias the original's")
	}
}

// TestClone_NilAndEmptyAreDistinct pins the part a copy is most likely to get
// wrong in a way nothing notices until a byte comparison fails: both stores
// normalise an empty blob to nil on the way in, so a copy that turned nil into
// an empty slice — or the reverse — would be distinguishable from its original.
func TestClone_NilAndEmptyAreDistinct(t *testing.T) {
	if CloneNode(nil) != nil {
		t.Error("CloneNode(nil) is not nil")
	}
	if CloneEdge(nil) != nil {
		t.Error("CloneEdge(nil) is not nil")
	}

	withNil := CloneNode(&Node{ID: 1, Properties: nil, Labels: nil})
	if withNil.Properties != nil {
		t.Errorf("a nil Properties became %#v", withNil.Properties)
	}
	if withNil.Labels != nil {
		t.Errorf("a nil Labels became %#v", withNil.Labels)
	}

	withEmpty := CloneNode(&Node{ID: 1, Properties: []byte{}, Labels: []NodeType{}})
	if withEmpty.Properties == nil {
		t.Error("an empty Properties became nil")
	}
	if withEmpty.Labels == nil {
		t.Error("an empty Labels became nil")
	}
}

// TestCloneNodes_KeepsTheHoles is why the batch form exists rather than a caller
// mapping over the slice: GetNodesBatch leaves nil where an ID was not found,
// and the result's correspondence with the IDs asked for is the whole point of
// that shape.
func TestCloneNodes_KeepsTheHoles(t *testing.T) {
	in := []*Node{
		{ID: 1, Properties: []byte("a")},
		nil,
		{ID: 3, Properties: []byte("c")},
	}
	got := CloneNodes(in)
	if len(got) != 3 {
		t.Fatalf("length %d, want 3", len(got))
	}
	if got[1] != nil {
		t.Error("the hole was filled in")
	}
	if got[0] == in[0] || got[2] == in[2] {
		t.Error("an entry was not copied")
	}
	in[0].Properties[0] = 'X'
	if got[0].Properties[0] == 'X' {
		t.Error("a copied entry aliases its original")
	}

	if CloneNodes(nil) != nil {
		t.Error("CloneNodes(nil) is not nil")
	}
	if CloneEdges(nil) != nil {
		t.Error("CloneEdges(nil) is not nil")
	}
}

// TestCloneEdges_KeepsTheHoles is CloneNodes's assertion for edges.
func TestCloneEdges_KeepsTheHoles(t *testing.T) {
	in := []*Edge{{ID: 1, Properties: []byte("a")}, nil}
	got := CloneEdges(in)
	if len(got) != 2 || got[1] != nil || got[0] == in[0] {
		t.Fatalf("CloneEdges did not preserve the shape: %#v", got)
	}
}
