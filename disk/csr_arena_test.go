package disk

import (
	"bytes"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The two invariants the record arena and the neighbour arrays rest on. Both
// are the kind CONTRIBUTING §2 calls out: they hold silently when the code is
// right and produce a wrong answer rather than a crash when it is not, so
// neither is covered by any test that merely reads a store back.

// TestArenaRecordsDoNotAliasAcrossRecords pins the three-index slicing in
// csrRecordSource.blobAt and labelRun, which the loader now reaches instead.
//
// Every record's Properties is a sub-slice of one shared arena. If the sub-slice
// were taken with the two-index form its capacity would run to the end of the
// arena, and a caller appending one byte to a record's blob would write over the
// next record's bytes in place — silently, with no allocation and no error, and
// visible only as another entity's properties changing.
//
// Mutation check: change the three-index slices in blobAt/labelRun to
// two-index and this test fails.
func TestArenaRecordsDoNotAliasAcrossRecords(t *testing.T) {
	nodes := []nodeRecord{
		{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("first")},
		{ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("second")},
		{ID: 3, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("third")},
	}
	edges := []rawEdge{
		{ID: 1, Src: 1, Dst: 2, Labels: []store.EdgeType{store.EdgeTypeContains}, Properties: []byte("edge-one")},
		{ID: 2, Src: 2, Dst: 3, Labels: []store.EdgeType{store.EdgeTypeContains}, Properties: []byte("edge-two")},
	}

	g, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	blob := g.Serialise()
	csr, _, err := deserialiseCSR(blob)
	if err != nil {
		t.Fatalf("deserialise: %v", err)
	}

	// Snapshot what the later records say before anyone appends.
	second, ok := csr.GetNode(2)
	if !ok {
		t.Fatal("node 2 missing")
	}
	third, ok := csr.GetNode(3)
	if !ok {
		t.Fatal("node 3 missing")
	}
	wantSecond := append([]byte(nil), second.Properties...)
	wantThird := append([]byte(nil), third.Properties...)

	// Append to the first record's blob the way any caller may.
	first, ok := csr.GetNode(1)
	if !ok {
		t.Fatal("node 1 missing")
	}
	grown := append(first.Properties, 'X', 'Y', 'Z')
	if !bytes.Equal(grown[:len(first.Properties)], first.Properties) {
		t.Fatalf("append corrupted its own prefix")
	}

	if got, _ := csr.GetNode(2); !bytes.Equal(got.Properties, wantSecond) {
		t.Fatalf("appending to node 1 changed node 2: got %q want %q", got.Properties, wantSecond)
	}
	if got, _ := csr.GetNode(3); !bytes.Equal(got.Properties, wantThird) {
		t.Fatalf("appending to node 3 changed node 3: got %q want %q", got.Properties, wantThird)
	}

	// The same contract for labels, and for the edge arenas.
	e1, ok := csr.GetEdge(1)
	if !ok {
		t.Fatal("edge 1 missing")
	}
	e2, ok := csr.GetEdge(2)
	if !ok {
		t.Fatal("edge 2 missing")
	}
	wantE2Labels := append([]store.EdgeType(nil), e2.Labels...)
	wantE2Props := append([]byte(nil), e2.Properties...)

	_ = append(e1.Labels, store.EdgeTypeSimilarTo)
	_ = append(e1.Properties, 'Q')

	if got, _ := csr.GetEdge(2); !equalEdgeTypes(got.Labels, wantE2Labels) {
		t.Fatalf("appending to edge 1 labels changed edge 2: got %v want %v", got.Labels, wantE2Labels)
	}
	if got, _ := csr.GetEdge(2); !bytes.Equal(got.Properties, wantE2Props) {
		t.Fatalf("appending to edge 1 properties changed edge 2: got %q want %q", got.Properties, wantE2Props)
	}
}

// TestArenaRoundTripPreservesRecords is the plain correctness half: an arena-
// backed load must return exactly what was serialised, including the
// empty-blob-is-nil normalisation both stores apply on the way in.
func TestArenaRoundTripPreservesRecords(t *testing.T) {
	nodes := []nodeRecord{
		{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact, store.NodeTypeEvidenceFile}, Properties: []byte("two")},
		{ID: 3, Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte{}},
	}
	edges := []rawEdge{
		{ID: 1, Src: 1, Dst: 2, Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.5},
		{ID: 2, Src: 2, Dst: 3, Labels: []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeSimilarTo}, Properties: []byte("e2")},
	}

	g, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	blob := g.Serialise()
	csr, _, err := deserialiseCSR(blob)
	if err != nil {
		t.Fatalf("deserialise: %v", err)
	}

	for _, want := range nodes {
		got, ok := csr.GetNode(want.ID)
		if !ok {
			t.Fatalf("node %d missing", want.ID)
		}
		if !equalNodeTypes(got.Labels, want.Labels) {
			t.Errorf("node %d labels: got %v want %v", want.ID, got.Labels, want.Labels)
		}
		if len(want.Properties) == 0 {
			if got.Properties != nil {
				t.Errorf("node %d: empty blob did not normalise to nil, got %q", want.ID, got.Properties)
			}
			continue
		}
		if !bytes.Equal(got.Properties, want.Properties) {
			t.Errorf("node %d properties: got %q want %q", want.ID, got.Properties, want.Properties)
		}
	}

	for _, want := range edges {
		got, ok := csr.GetEdge(want.ID)
		if !ok {
			t.Fatalf("edge %d missing", want.ID)
		}
		if got.Src != want.Src || got.Dst != want.Dst || got.Weight != want.Weight {
			t.Errorf("edge %d endpoints/weight: got %d->%d w=%v want %d->%d w=%v",
				want.ID, got.Src, got.Dst, got.Weight, want.Src, want.Dst, want.Weight)
		}
		if !equalEdgeTypes(got.Labels, want.Labels) {
			t.Errorf("edge %d labels: got %v want %v", want.ID, got.Labels, want.Labels)
		}
		if !bytes.Equal(got.Properties, want.Properties) {
			t.Errorf("edge %d properties: got %q want %q", want.ID, got.Properties, want.Properties)
		}
	}
}

func equalNodeTypes(a, b []store.NodeType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalEdgeTypes(a, b []store.EdgeType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
