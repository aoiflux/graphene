package store

import "testing"

func TestNodeHasLabel(t *testing.T) {
	n := &Node{Labels: []NodeType{NodeTypeEvidenceFile, NodeTypeMicroArtefact}}
	if !n.HasLabel(NodeTypeEvidenceFile) {
		t.Error("expected HasLabel(EvidenceFile) = true")
	}
	if !n.HasLabel(NodeTypeMicroArtefact) {
		t.Error("expected HasLabel(MicroArtefact) = true")
	}
	if n.HasLabel(NodeTypeCase) {
		t.Error("expected HasLabel(Case) = false")
	}
}

func TestNodeHasLabel_Empty(t *testing.T) {
	n := &Node{}
	if n.HasLabel(NodeTypeEvidenceFile) {
		t.Error("empty Labels: HasLabel should return false")
	}
}

func TestEdgeHasLabel(t *testing.T) {
	e := &Edge{Labels: []EdgeType{EdgeTypeContains, EdgeTypeSimilarTo}}
	if !e.HasLabel(EdgeTypeContains) {
		t.Error("expected HasLabel(Contains) = true")
	}
	if !e.HasLabel(EdgeTypeSimilarTo) {
		t.Error("expected HasLabel(SimilarTo) = true")
	}
	if e.HasLabel(EdgeTypeBelongsTo) {
		t.Error("expected HasLabel(BelongsTo) = false")
	}
}

func TestNodeTypeString(t *testing.T) {
	cases := []struct {
		t    NodeType
		want string
	}{
		{NodeTypeEvidenceFile, "EvidenceFile"},
		{NodeTypeMicroArtefact, "MicroArtefact"},
		{NodeTypeTag, "Tag"},
		{NodeTypeCase, "Case"},
		{NodeTypeUnknown, "Unknown"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Errorf("NodeType(%d).String() = %q, want %q", c.t, got, c.want)
		}
	}
}

func TestEdgeTypeString(t *testing.T) {
	cases := []struct {
		t    EdgeType
		want string
	}{
		{EdgeTypeContains, "Contains"},
		{EdgeTypeSimilarTo, "SimilarTo"},
		{EdgeTypeReuse, "Reuse"},
		{EdgeTypeTemporal, "Temporal"},
		{EdgeTypeTaggedWith, "TaggedWith"},
		{EdgeTypeBelongsTo, "BelongsTo"},
		{EdgeTypeUnknown, "Unknown"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Errorf("EdgeType(%d).String() = %q, want %q", c.t, got, c.want)
		}
	}
}
