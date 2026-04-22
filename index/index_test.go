package index

import (
	"testing"

	"graphene/store"
)

// --- TypeIndex ---

func TestTypeIndex_NodeLabels(t *testing.T) {
	idx := NewTypeIndex()

	idx.IndexNodeLabels(1, []store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact})
	idx.IndexNodeLabels(2, []store.NodeType{store.NodeTypeMicroArtefact})
	idx.IndexNode(3, store.NodeTypeCase)

	ids := idx.NodesByType(store.NodeTypeMicroArtefact)
	if len(ids) != 2 {
		t.Fatalf("NodesByType(MicroArtefact): got %d ids, want 2", len(ids))
	}

	ids = idx.NodesByType(store.NodeTypeEvidenceFile)
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("NodesByType(EvidenceFile): got %v, want [1]", ids)
	}

	ids = idx.NodesByType(store.NodeTypeCase)
	if len(ids) != 1 || ids[0] != 3 {
		t.Fatalf("NodesByType(Case): got %v, want [3]", ids)
	}

	ids = idx.NodesByType(store.NodeTypeTag)
	if len(ids) != 0 {
		t.Fatalf("NodesByType(Tag): got %v, want []", ids)
	}
}

func TestTypeIndex_EdgeLabels(t *testing.T) {
	idx := NewTypeIndex()

	idx.IndexEdgeLabels(10, []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeSimilarTo})
	idx.IndexEdgeLabels(11, []store.EdgeType{store.EdgeTypeContains})
	idx.IndexEdge(12, store.EdgeTypeBelongsTo)

	ids := idx.EdgesByType(store.EdgeTypeContains)
	if len(ids) != 2 {
		t.Fatalf("EdgesByType(Contains): got %d ids, want 2", len(ids))
	}
	ids = idx.EdgesByType(store.EdgeTypeSimilarTo)
	if len(ids) != 1 || ids[0] != 10 {
		t.Fatalf("EdgesByType(SimilarTo): got %v, want [10]", ids)
	}
	ids = idx.EdgesByType(store.EdgeTypeBelongsTo)
	if len(ids) != 1 || ids[0] != 12 {
		t.Fatalf("EdgesByType(BelongsTo): got %v, want [12]", ids)
	}
}

func TestTypeIndex_Counts(t *testing.T) {
	idx := NewTypeIndex()
	// node 1 has 2 labels → 2 label-index entries; node 2 has 1 label → 1 entry. Total = 3.
	idx.IndexNodeLabels(1, []store.NodeType{store.NodeTypeCase, store.NodeTypeEvidenceFile})
	idx.IndexNodeLabels(2, []store.NodeType{store.NodeTypeCase})
	// edge 10 has 1 label → 1 entry; edge 11 has 2 labels → 2 entries. Total = 3.
	idx.IndexEdgeLabels(10, []store.EdgeType{store.EdgeTypeContains})
	idx.IndexEdgeLabels(11, []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeReuse})

	// NodeCount / EdgeCount count label-index entries (one per label per entity).
	if got := idx.NodeCount(); got != 3 {
		t.Errorf("NodeCount = %d, want 3", got)
	}
	if got := idx.EdgeCount(); got != 3 {
		t.Errorf("EdgeCount = %d, want 3", got)
	}
}

// --- TemporalIndex ---

func TestTemporalIndex_RangeQuery(t *testing.T) {
	ti := NewTemporalIndex()
	ti.Add(100, 1)
	ti.Add(200, 2)
	ti.Add(300, 3)
	ti.Add(150, 4)

	res := ti.Range(100, 200)
	if len(res) != 3 {
		t.Fatalf("Range(100,200): got %d entries, want 3", len(res))
	}
	// Should be sorted ascending.
	if res[0].TimestampNs > res[1].TimestampNs || res[1].TimestampNs > res[2].TimestampNs {
		t.Error("Range results not sorted ascending")
	}

	res = ti.Range(201, 300)
	if len(res) != 1 || res[0].NodeID != 3 {
		t.Fatalf("Range(201,300): got %v, want [{300 3}]", res)
	}

	res = ti.Range(400, 500)
	if res != nil {
		t.Errorf("Range(400,500): expected nil, got %v", res)
	}
}

func TestTemporalIndex_Exact(t *testing.T) {
	ti := NewTemporalIndex()
	ti.Add(50, 7)

	res := ti.Range(50, 50)
	if len(res) != 1 || res[0].NodeID != 7 {
		t.Fatalf("exact match: got %v", res)
	}
}

// --- PropertyIndex ---

func TestPropertyIndex_NodeLookup(t *testing.T) {
	pi := NewPropertyIndex()
	pi.IndexNode(1, "sha256", []byte("aaaa"))
	pi.IndexNode(2, "sha256", []byte("bbbb"))
	pi.IndexNode(3, "sha256", []byte("aaaa"))
	pi.IndexNode(4, "filename", []byte("evidence.bin"))

	ids := pi.NodesByProperty("sha256", []byte("aaaa"))
	if len(ids) != 2 {
		t.Fatalf("NodesByProperty sha256=aaaa: got %v", ids)
	}

	ids = pi.NodesByProperty("sha256", []byte("bbbb"))
	if len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("NodesByProperty sha256=bbbb: got %v", ids)
	}

	ids = pi.NodesByProperty("sha256", []byte("missing"))
	if len(ids) != 0 {
		t.Errorf("expected empty result for missing value, got %v", ids)
	}

	ids = pi.NodesByProperty("filename", []byte("evidence.bin"))
	if len(ids) != 1 || ids[0] != 4 {
		t.Fatalf("NodesByProperty filename: got %v", ids)
	}
}

func TestPropertyIndex_EdgeLookup(t *testing.T) {
	pi := NewPropertyIndex()
	pi.IndexEdge(10, "weight_bucket", []byte("high"))
	pi.IndexEdge(11, "weight_bucket", []byte("low"))
	pi.IndexEdge(12, "weight_bucket", []byte("high"))

	ids := pi.EdgesByProperty("weight_bucket", []byte("high"))
	if len(ids) != 2 {
		t.Fatalf("EdgesByProperty high: got %v", ids)
	}
	ids = pi.EdgesByProperty("weight_bucket", []byte("medium"))
	if len(ids) != 0 {
		t.Errorf("expected empty result for missing value, got %v", ids)
	}
}

func TestPropertyIndex_Entries(t *testing.T) {
	pi := NewPropertyIndex()
	pi.IndexNode(1, "k", []byte("v"))
	pi.IndexNode(2, "k", []byte("v2"))
	pi.IndexEdge(10, "ek", []byte("ev"))

	ne := pi.NodeEntries()
	if len(ne) != 2 {
		t.Fatalf("NodeEntries: got %d, want 2", len(ne))
	}
	ee := pi.EdgeEntries()
	if len(ee) != 1 {
		t.Fatalf("EdgeEntries: got %d, want 1", len(ee))
	}
}
