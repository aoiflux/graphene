package disk

// Label postings are read by merging two ascending sources, not by
// concatenating them and deduping through a map. Two things have to hold for
// that to be safe, and neither is checked by the query tests, which sort before
// comparing:
//
//  1. Both sources really are ascending and duplicate-free in every layer state
//     the store can be in. The delta's is maintained by indexNodeLabels; the
//     CSR's by buildLabelIndex; a compaction splices the two and re-sorts. A
//     regression in any of those turns the merge into silent data loss rather
//     than a wrong order, because a merge drops what it walks past.
//  2. The merged result is itself ascending, which is the claim driveNodeLabels
//     makes when it reports sortedAsc and the query path then skips its sort on.
//
// The layer states are enumerated deliberately: delta-only, split across a CSR
// and a delta, fully compacted, and reopened from disk. The merge breaks first
// on the split state, which is exactly the one a single-shot test misses.

import (
	"fmt"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// labelFixture fills a store with nodes and edges whose labels overlap, then
// mutates enough of them to leave the delta with opinions about records the
// image also holds — the case where the two postings name the same ID.
func labelFixture(t *testing.T, s *Store) {
	t.Helper()
	const n = 120

	nodes := make([]store.NodeID, 0, n)
	for i := range n {
		labels := []store.NodeType{store.NodeTypeMicroArtefact}
		switch {
		case i%7 == 0:
			labels = []store.NodeType{store.NodeTypeCase, store.NodeTypeTag}
		case i%3 == 0:
			labels = []store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact}
		}
		id, err := s.AddNode(&store.Node{Labels: labels, Properties: []byte(fmt.Sprintf("n%03d", i))})
		if err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
		nodes = append(nodes, id)
	}
	for i := 1; i < len(nodes); i += 2 {
		_, err := s.AddEdge(&store.Edge{
			Src:    nodes[i-1],
			Dst:    nodes[i],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
		if err != nil {
			t.Fatalf("AddEdge %d: %v", i, err)
		}
	}
}

// mutateAcrossLayers updates and deletes records the image already holds, so the
// delta ends up carrying postings for IDs the CSR also carries, plus postings
// for IDs that are no longer live and must be dropped on the way out.
func mutateAcrossLayers(t *testing.T, s *Store) {
	t.Helper()
	ids, err := s.NodesByType(store.NodeTypeMicroArtefact)
	if err != nil {
		t.Fatalf("NodesByType: %v", err)
	}
	for i, id := range ids {
		switch i % 5 {
		case 0:
			// Same label, new record: the delta now names an ID the CSR names.
			n, err := s.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			n.Properties = []byte("updated")
			if err := s.UpdateNode(n); err != nil {
				t.Fatalf("UpdateNode: %v", err)
			}
		case 1:
			// Label dropped: a posting that must not survive re-resolution.
			n, err := s.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			n.Labels = []store.NodeType{store.NodeTypeTag}
			if err := s.UpdateNode(n); err != nil {
				t.Fatalf("UpdateNode: %v", err)
			}
		case 2:
			if err := s.DeleteNode(id); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
		}
	}
}

// scanNodesByType answers the same question by walking every record, which is
// what the postings are an index over.
func scanNodesByType(s *Store, t store.NodeType) []store.NodeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()
	var out []store.NodeID
	for _, id := range r.allNodeIDs() {
		if r.nodeHasLabel(id, t) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

func scanEdgesByType(s *Store, t store.EdgeType) []store.EdgeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()
	var out []store.EdgeID
	for _, id := range r.allEdgeIDs() {
		if r.edgeHasLabel(id, t) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// assertPostingsMerge checks both claims against every label in use.
func assertPostingsMerge(t *testing.T, s *Store, state string) {
	t.Helper()
	for _, lbl := range []store.NodeType{
		store.NodeTypeMicroArtefact,
		store.NodeTypeEvidenceFile,
		store.NodeTypeCase,
		store.NodeTypeTag,
	} {
		got, err := s.NodesByType(lbl)
		if err != nil {
			t.Fatalf("%s: NodesByType(%v): %v", state, lbl, err)
		}
		if !slices.IsSorted(got) {
			t.Errorf("%s: NodesByType(%v) is not ascending: %v", state, lbl, got)
		}
		if want := scanNodesByType(s, lbl); !slices.Equal(got, want) {
			t.Errorf("%s: NodesByType(%v) = %v, the record scan says %v", state, lbl, got, want)
		}
	}

	got, err := s.EdgesByType(store.EdgeTypeContains)
	if err != nil {
		t.Fatalf("%s: EdgesByType: %v", state, err)
	}
	if !slices.IsSorted(got) {
		t.Errorf("%s: EdgesByType is not ascending: %v", state, got)
	}
	if want := scanEdgesByType(s, store.EdgeTypeContains); !slices.Equal(got, want) {
		t.Errorf("%s: EdgesByType = %v, the record scan says %v", state, got, want)
	}
}

func TestLabelPostings_MergeHoldsInEveryLayerState(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	labelFixture(t, s)
	assertPostingsMerge(t, s, "delta only")

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	assertPostingsMerge(t, s, "fully compacted")

	mutateAcrossLayers(t, s)
	assertPostingsMerge(t, s, "split layers")

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s.Close() }()
	assertPostingsMerge(t, s, "reopened")

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact after reopen: %v", err)
	}
	assertPostingsMerge(t, s, "recompacted")
}

// A node carrying the same label twice must appear once. Both posting builders
// deduplicate, and the merge assumes they did.
func TestLabelPostings_RepeatedLabelAppearsOnce(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	id, err := s.AddNode(&store.Node{Labels: []store.NodeType{
		store.NodeTypeCase, store.NodeTypeCase, store.NodeTypeCase,
	}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	for _, state := range []string{"delta", "compacted"} {
		ids, err := s.NodesByType(store.NodeTypeCase)
		if err != nil {
			t.Fatalf("%s: NodesByType: %v", state, err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("%s: NodesByType = %v, want exactly [%d]", state, ids, id)
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
	}
}
