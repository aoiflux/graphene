package index

import (
	"testing"
)

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

func TestPropertyIndex_RemoveNode(t *testing.T) {
	pi := NewPropertyIndex()
	pi.IndexNode(1, "sha256", []byte("aaaa"))
	pi.IndexNode(2, "sha256", []byte("aaaa"))
	pi.IndexNode(1, "tool", []byte("strings"))

	pi.RemoveNode(1)

	if got := pi.NodesByProperty("sha256", []byte("aaaa")); len(got) != 1 || got[0] != 2 {
		t.Fatalf("sha256=aaaa after RemoveNode(1): got %v, want [2]", got)
	}
	if got := pi.NodesByProperty("tool", []byte("strings")); len(got) != 0 {
		t.Fatalf("tool=strings after RemoveNode(1): got %v, want []", got)
	}
	// The now-empty tool bucket should be gone; node 2 still enumerable.
	if entries := pi.NodeEntries(); len(entries) != 1 || entries[0].ID != 2 {
		t.Fatalf("NodeEntries after RemoveNode(1): got %+v, want single entry for node 2", entries)
	}
}

func TestPropertyIndex_RemoveEdge(t *testing.T) {
	pi := NewPropertyIndex()
	pi.IndexEdge(10, "algo", []byte("tlsh"))
	pi.IndexEdge(11, "algo", []byte("tlsh"))

	pi.RemoveEdge(10)

	if got := pi.EdgesByProperty("algo", []byte("tlsh")); len(got) != 1 || got[0] != 11 {
		t.Fatalf("algo=tlsh after RemoveEdge(10): got %v, want [11]", got)
	}
}
