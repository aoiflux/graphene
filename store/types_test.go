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

func TestCustomTypeHelpers(t *testing.T) {
	if !CustomNodeType(7).IsCustom() {
		t.Fatal("CustomNodeType(7).IsCustom() = false, want true")
	}
	if NodeTypeCase.IsCustom() {
		t.Fatal("NodeTypeCase.IsCustom() = true, want false")
	}
	if !CustomEdgeType(3).IsCustom() {
		t.Fatal("CustomEdgeType(3).IsCustom() = false, want true")
	}
	if EdgeTypeContains.IsCustom() {
		t.Fatal("EdgeTypeContains.IsCustom() = true, want false")
	}
}

func TestParseNodeType(t *testing.T) {
	tests := []struct {
		in      string
		want    NodeType
		wantErr bool
	}{
		{in: "Case", want: NodeTypeCase},
		{in: "micro_artefact", want: NodeTypeMicroArtefact},
		{in: "custom:7", want: CustomNodeType(7)},
		{in: "custom(12)", want: CustomNodeType(12)},
		{in: "custom-127", want: CustomNodeType(127)},
		{in: "130", want: NodeType(130)},
		{in: "custom:500", wantErr: true},
		{in: "", wantErr: true},
		{in: "not-a-type", wantErr: true},
	}

	for _, tc := range tests {
		got, err := ParseNodeType(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("ParseNodeType(%q): expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseNodeType(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseNodeType(%q): got %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParseEdgeType(t *testing.T) {
	tests := []struct {
		in      string
		want    EdgeType
		wantErr bool
	}{
		{in: "Contains", want: EdgeTypeContains},
		{in: "similar_to", want: EdgeTypeSimilarTo},
		{in: "custom:4", want: CustomEdgeType(4)},
		{in: "custom(10)", want: CustomEdgeType(10)},
		{in: "131", want: EdgeType(131)},
		{in: "custom:999", wantErr: true},
		{in: "", wantErr: true},
		{in: "not-an-edge-type", wantErr: true},
	}

	for _, tc := range tests {
		got, err := ParseEdgeType(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("ParseEdgeType(%q): expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseEdgeType(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ParseEdgeType(%q): got %d, want %d", tc.in, got, tc.want)
		}
	}
}
