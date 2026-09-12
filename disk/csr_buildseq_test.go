package disk

// buildSeq's own contract: that it builds the same graph a slice does, and that
// it refuses a sequence which does not yield the same records on every pass.
//
// The second half is the interesting one. buildSeq walks its inputs three times
// for nodes and five times for edges, which is a contract no slice caller can
// break and every generator caller can. A sequence that grows between the pass
// that sizes the adjacency arrays and the pass that fills them writes past the
// end of them; the count checks are there so that the failure is an error naming
// the cause rather than an index panic two passes downstream.

import (
	"errors"
	"iter"
	"reflect"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// buildSeqFixture is a graph with several pages, dead pages between them, a node
// carrying labels and properties, and edges in both directions — enough shape
// that two builds agreeing on it is worth something.
func buildSeqFixture() ([]nodeRecord, []rawEdge) {
	ids := []store.NodeID{1, 2, 3, 4095, 4096, 4097, 9000, 12290}
	nodes := make([]nodeRecord, 0, len(ids))
	for i, id := range ids {
		nodes = append(nodes, nodeRecord{
			ID:         id,
			Labels:     []store.NodeType{store.NodeType(i%3 + 1)},
			Properties: []byte{byte(i), byte(i + 1)},
		})
	}
	edges := []rawEdge{
		{ID: 1, Src: 1, Dst: 2, Weight: 1, Labels: []store.EdgeType{1}},
		{ID: 2, Src: 2, Dst: 1, Weight: 2},
		{ID: 3, Src: 4095, Dst: 4096, Weight: 3},
		{ID: 4096, Src: 9000, Dst: 1, Weight: 4},
		{ID: 8191, Src: 12290, Dst: 9000, Weight: 5},
		{ID: 8192, Src: 1, Dst: 12290, Weight: 6},
	}
	return nodes, edges
}

// generated yields the same records as the slice without being one, so the test
// exercises the sequence path rather than slices.Values.
func generated[T any](src []T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := range src {
			if !yield(src[i]) {
				return
			}
		}
	}
}

func TestBuildSeq_MatchesTheSliceBuild(t *testing.T) {
	nodes, edges := buildSeqFixture()

	want, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got, err := buildSeq(generated(nodes), generated(edges))
	if err != nil {
		t.Fatalf("buildSeq: %v", err)
	}

	// Every field, not a sample of them: the page directory, the arenas, both
	// offset arrays, both edge arrays and the label index. A difference in any
	// of them is a different graph.
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("buildSeq built a different graph from Build over the same records\n"+
			"nodes: %d vs %d, edges: %d vs %d, outEdges: %v vs %v",
			want.NodeCount(), got.NodeCount(), want.EdgeCount(), got.EdgeCount(),
			want.outEdges, got.outEdges)
	}
}

func TestBuildSeq_EmptyNodeSequenceNeverWalksTheEdges(t *testing.T) {
	walked := false
	edges := func(yield func(rawEdge) bool) {
		walked = true
		yield(rawEdge{ID: 1, Src: 1, Dst: 1})
	}

	g, err := buildSeq(slices.Values([]nodeRecord(nil)), edges)
	if err != nil {
		t.Fatalf("buildSeq: %v", err)
	}
	if walked {
		t.Error("the edge sequence was walked for a graph with no nodes; " +
			"the edges are dropped, so walking them is work that produces nothing")
	}
	if g.NodeCount() != 0 || g.EdgeCount() != 0 {
		t.Errorf("empty build produced %d nodes and %d edges", g.NodeCount(), g.EdgeCount())
	}
	if len(g.outOffset) != 1 || len(g.inOffset) != 1 {
		t.Errorf("empty build left offset arrays of %d and %d, want one sentinel each",
			len(g.outOffset), len(g.inOffset))
	}
}

// unstableNodes yields base on every pass except the one named, where it yields
// base plus extra (grow) or base minus its last record (shrink).
func unstableNodes(base []nodeRecord, extra *nodeRecord, atPass int) iter.Seq[nodeRecord] {
	pass := 0
	return func(yield func(nodeRecord) bool) {
		pass++
		recs := base
		switch {
		case pass != atPass:
		case extra != nil:
			recs = append(append([]nodeRecord(nil), base...), *extra)
		default:
			recs = base[:len(base)-1]
		}
		for _, r := range recs {
			if !yield(r) {
				return
			}
		}
	}
}

func unstableEdges(base []rawEdge, extra *rawEdge, atPass int) iter.Seq[rawEdge] {
	pass := 0
	return func(yield func(rawEdge) bool) {
		pass++
		recs := base
		switch {
		case pass != atPass:
		case extra != nil:
			recs = append(append([]rawEdge(nil), base...), *extra)
		default:
			recs = base[:len(base)-1]
		}
		for _, r := range recs {
			if !yield(r) {
				return
			}
		}
	}
}

func TestBuildSeq_RefusesASequenceThatChangesBetweenPasses(t *testing.T) {
	nodes, edges := buildSeqFixture()

	// Identifiers inside the extent the first pass measured, so the failure
	// under test is the count disagreeing and not an index running off the
	// directory the first pass sized.
	extraNode := nodeRecord{ID: 5}
	extraEdge := rawEdge{ID: 7, Src: 2, Dst: 3}

	cases := []struct {
		name  string
		nodes iter.Seq[nodeRecord]
		edges iter.Seq[rawEdge]
	}{
		{
			// Pass 2 over nodes: the one that touches pages.
			name:  "a node appears before the pages are touched",
			nodes: unstableNodes(nodes, &extraNode, 2),
			edges: slices.Values(edges),
		},
		{
			// Pass 3 over nodes: the one that places records.
			name:  "a node vanishes before the records are placed",
			nodes: unstableNodes(nodes, nil, 3),
			edges: slices.Values(edges),
		},
		{
			name:  "an edge appears before the pages are touched",
			nodes: slices.Values(nodes),
			edges: unstableEdges(edges, &extraEdge, 2),
		},
		{
			name:  "an edge vanishes before the records are placed",
			nodes: slices.Values(nodes),
			edges: unstableEdges(edges, nil, 3),
		},
		{
			name:  "an edge vanishes before the degrees are counted",
			nodes: slices.Values(nodes),
			edges: unstableEdges(edges, nil, 4),
		},
		{
			// The dangerous one. The adjacency arrays are sized from the count
			// the degree pass agreed with; an extra edge here writes past their
			// end.
			name:  "an edge appears while the adjacency is being filled",
			nodes: slices.Values(nodes),
			edges: unstableEdges(edges, &extraEdge, 5),
		},
		{
			// The same pass, one short. Nothing writes out of range, so this is
			// the check after the loop rather than the one inside it.
			name:  "an edge vanishes while the adjacency is being filled",
			nodes: slices.Values(nodes),
			edges: unstableEdges(edges, nil, 5),
		},
		{
			// And the case the check *inside* that loop is for. The extra edge
			// above is incident on nodes in the middle of the arena, so its
			// write lands in another node's range and the count check after the
			// loop is what notices. This one is incident on the highest node, so
			// its write is past the end of an array sized before the pass began:
			// without the check it is an index panic rather than an error.
			name:  "an edge past the end of the adjacency appears while it is being filled",
			nodes: slices.Values(nodes),
			edges: unstableEdges(edges, &rawEdge{ID: 9, Src: 12290, Dst: 12290}, 5),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := buildSeq(tc.nodes, tc.edges)
			if !errors.Is(err, errUnstableBuild) {
				t.Fatalf("buildSeq = (%v, %v), want errUnstableBuild", g, err)
			}
		})
	}
}

func TestBuildSeq_AcceptsARepeatableSequence(t *testing.T) {
	nodes, edges := buildSeqFixture()

	// The same generator shape the refusal cases use, with the pass number that
	// never fires: if the harness itself perturbed the records, every case above
	// would pass for the wrong reason.
	g, err := buildSeq(unstableNodes(nodes, &nodeRecord{ID: 5}, 0), unstableEdges(edges, nil, 0))
	if err != nil {
		t.Fatalf("buildSeq over a stable sequence: %v", err)
	}
	if g.NodeCount() != len(nodes) || g.EdgeCount() != len(edges) {
		t.Fatalf("built %d nodes and %d edges, want %d and %d",
			g.NodeCount(), g.EdgeCount(), len(nodes), len(edges))
	}
}
