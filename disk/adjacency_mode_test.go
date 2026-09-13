package disk

// AdjacencyMode's own tests.
//
// Two properties carry this landing, and they are not the same property.
//
// The first is that deferring the build changes nothing about an answer. That is
// easy to assert and easy to assert vacuously: both arms of a comparison can take
// the eager path and agree perfectly. So every test that compares the two modes
// also asserts that they were in different states when the comparison was made —
// AdjacencyBuilt is what makes the difference observable, and without it a change
// that quietly stopped honouring the option would leave this whole file passing.
//
// The second is that the arrays are derived from the record arena rather than
// from the sequence that filled it. That is what makes eager and lazy one piece
// of code instead of two, and it is what replaced four cases in
// TestBuildSeq_RefusesASequenceThatChangesBetweenPasses: a build that no longer
// re-walks the sequence has no late pass for an unstable sequence to corrupt.

import (
	"fmt"
	"sync"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// adjacencyCorpus is a chain of n nodes spanning more than one page, joined by
// n-1 edges, plus a second edge fanning every node back to the first — so both
// directions are non-trivial and node 1 has a high in-degree.
func adjacencyCorpus(n int) ([]nodeRecord, []rawEdge) {
	nodes := make([]nodeRecord, 0, n)
	for i := 1; i <= n; i++ {
		id := store.NodeID(i)
		if i > n/2 {
			// Past the halfway mark identifiers jump a page, so the slot
			// arithmetic is exercised across a directory gap rather than only
			// inside page zero.
			id = store.NodeID(csrPageSlots + i)
		}
		nodes = append(nodes, nodeRecord{
			ID:         id,
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("n%d", i)),
		})
	}
	edges := make([]rawEdge, 0, 2*n)
	eid := store.EdgeID(1)
	for i := 1; i < len(nodes); i++ {
		edges = append(edges, rawEdge{
			ID: eid, Src: nodes[i-1].ID, Dst: nodes[i].ID,
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
		eid++
	}
	for i := 1; i < len(nodes); i++ {
		edges = append(edges, rawEdge{
			ID: eid, Src: nodes[i].ID, Dst: nodes[0].ID,
			Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
		})
		eid++
	}
	return nodes, edges
}

// arenaAdjacency is what the adjacency arrays must contain, computed the slow
// and obvious way: walk the live edge records and group them by endpoint.
func arenaAdjacency(g *CSRGraph) (out, in map[store.NodeID][]store.EdgeID) {
	out = map[store.NodeID][]store.EdgeID{}
	in = map[store.NodeID][]store.EdgeID{}
	for e := range g.Edges() {
		out[e.Src] = append(out[e.Src], e.ID)
		in[e.Dst] = append(in[e.Dst], e.ID)
	}
	return out, in
}

func sameEdgeIDs(a, b []store.EdgeID) bool {
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

// assertAdjacencyMatchesArena is the whole-graph check. Every live node's two
// lists must be exactly the live edges naming it, in ascending edge-ID order,
// which is the arena's order.
func assertAdjacencyMatchesArena(t *testing.T, g *CSRGraph) {
	t.Helper()
	wantOut, wantIn := arenaAdjacency(g)
	seen := 0
	for id := range g.NodeIDs() {
		seen++
		if got := g.OutboundEdgeIDs(id); !sameEdgeIDs(got, wantOut[id]) {
			t.Fatalf("node %d outbound %v, want %v", id, got, wantOut[id])
		}
		if got := g.InboundEdgeIDs(id); !sameEdgeIDs(got, wantIn[id]) {
			t.Fatalf("node %d inbound %v, want %v", id, got, wantIn[id])
		}
		if g.OutDegree(id) != len(wantOut[id]) || g.InDegree(id) != len(wantIn[id]) {
			t.Fatalf("node %d degrees (%d, %d), want (%d, %d)",
				id, g.OutDegree(id), g.InDegree(id), len(wantOut[id]), len(wantIn[id]))
		}
	}
	if seen == 0 {
		t.Fatal("the corpus has no live nodes, so this asserted nothing")
	}
}

// The property that replaced the four deleted instability cases. Adjacency is
// the inverse of the arena, so it cannot disagree with the records however the
// build sequence behaved after the records were placed.
func TestAdjacency_IsTheInverseOfTheRecordArena(t *testing.T) {
	nodes, edges := adjacencyCorpus(9000)
	g, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	assertAdjacencyMatchesArena(t, g)
}

// And the reason that property holds: the build stopped walking the edge
// sequence for adjacency. Three passes, not five — which for compaction is three
// walks of a merge over an image and two sorted slices instead of five.
func TestBuildSeq_WalksEachSequenceThreeTimes(t *testing.T) {
	nodes, edges := adjacencyCorpus(20)

	nodePasses, edgePasses := 0, 0
	countedNodes := func(yield func(nodeRecord) bool) {
		nodePasses++
		for _, r := range nodes {
			if !yield(r) {
				return
			}
		}
	}
	countedEdges := func(yield func(rawEdge) bool) {
		edgePasses++
		for _, r := range edges {
			if !yield(r) {
				return
			}
		}
	}

	if _, err := buildSeq(countedNodes, countedEdges, AdjacencyEager); err != nil {
		t.Fatalf("build: %v", err)
	}
	if nodePasses != 3 || edgePasses != 3 {
		t.Fatalf("buildSeq walked the sequences %d and %d times, want 3 and 3 — "+
			"a fourth walk of the edges means adjacency went back to reading the "+
			"sequence instead of the arena", nodePasses, edgePasses)
	}
}

// The mode's own claim: lazy holds nothing until something asks.
func TestAdjacency_LazyDefersTheBuildAndFirstUseMakesIt(t *testing.T) {
	nodes, edges := adjacencyCorpus(500)

	lazy, err := buildSeq(seqOf(nodes), seqOf(edges), AdjacencyLazy)
	if err != nil {
		t.Fatalf("lazy build: %v", err)
	}
	if lazy.AdjacencyBuilt() {
		t.Fatal("AdjacencyLazy built the arrays at build time")
	}
	if lazy.outOffset != nil || lazy.outEdges != nil || lazy.inOffset != nil || lazy.inEdges != nil {
		t.Fatal("AdjacencyLazy allocated the arrays it was told to defer")
	}

	eager, err := buildSeq(seqOf(nodes), seqOf(edges), AdjacencyEager)
	if err != nil {
		t.Fatalf("eager build: %v", err)
	}
	if !eager.AdjacencyBuilt() {
		t.Fatal("AdjacencyEager did not build the arrays")
	}

	// The comparison, made while the two are genuinely in different states.
	// Reading the lazy one is what ends that.
	id := nodes[1].ID
	got := lazy.OutboundEdgeIDs(id)
	if !lazy.AdjacencyBuilt() {
		t.Fatal("a read of the adjacency did not build it")
	}
	if !sameEdgeIDs(got, eager.OutboundEdgeIDs(id)) {
		t.Fatalf("lazy outbound %v, eager %v", got, eager.OutboundEdgeIDs(id))
	}
	assertAdjacencyMatchesArena(t, lazy)
}

// Every accessor is an entry point, and one that forgot to build would return an
// empty list rather than fail — the quietest possible wrong answer.
func TestAdjacency_EveryAccessorBuildsIt(t *testing.T) {
	nodes, edges := adjacencyCorpus(64)
	id := nodes[1].ID

	for _, tc := range []struct {
		name string
		call func(*CSRGraph) int
	}{
		{"OutboundEdgeIDs", func(g *CSRGraph) int { return len(g.OutboundEdgeIDs(id)) }},
		{"InboundEdgeIDs", func(g *CSRGraph) int { return len(g.InboundEdgeIDs(id)) }},
		{"OutDegree", func(g *CSRGraph) int { return g.OutDegree(id) }},
		{"InDegree", func(g *CSRGraph) int { return g.InDegree(id) }},
		{"OutboundEdges", func(g *CSRGraph) int { e, _ := g.OutboundEdges(id); return len(e) }},
		{"InboundEdges", func(g *CSRGraph) int { e, _ := g.InboundEdges(id); return len(e) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lazy, err := buildSeq(seqOf(nodes), seqOf(edges), AdjacencyLazy)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			eager, err := buildSeq(seqOf(nodes), seqOf(edges), AdjacencyEager)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			want := tc.call(eager)
			if want == 0 {
				t.Fatalf("the fixture gives %s nothing to find, so this asserted nothing", tc.name)
			}
			if got := tc.call(lazy); got != want {
				t.Fatalf("%s on a deferred graph = %d, want %d", tc.name, got, want)
			}
			if !lazy.AdjacencyBuilt() {
				t.Fatalf("%s answered without building the adjacency", tc.name)
			}
		})
	}
}

// Verifying adjacency is one of the things that needs adjacency. An image that
// reported itself sound without ever building the arrays would make Verify
// weaker under one mode than the other.
func TestAdjacency_VerifyBuildsIt(t *testing.T) {
	nodes, edges := adjacencyCorpus(300)
	g, err := buildSeq(seqOf(nodes), seqOf(edges), AdjacencyLazy)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := g.verifyAdjacency(); err != nil {
		t.Fatalf("verifyAdjacency on a deferred graph: %v", err)
	}
	if !g.AdjacencyBuilt() {
		t.Fatal("verifyAdjacency passed without building the adjacency it verifies")
	}
}

// The empty image is the one shape where the offset arrays are pure sentinel, and
// it takes a different path out of buildSeq. Both modes have to leave it in the
// state verifyAdjacency expects.
func TestAdjacency_TheEmptyImageHoldsItsSentinels(t *testing.T) {
	for _, mode := range []AdjacencyMode{AdjacencyEager, AdjacencyLazy} {
		t.Run(mode.String(), func(t *testing.T) {
			g, err := buildSeq(seqOf([]nodeRecord(nil)), seqOf([]rawEdge(nil)), mode)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if err := g.verifyAdjacency(); err != nil {
				t.Fatalf("verifyAdjacency on an empty %s graph: %v", mode, err)
			}
			if len(g.outOffset) != 1 || len(g.inOffset) != 1 {
				t.Fatalf("empty %s build left offset arrays of %d and %d, want one sentinel each",
					mode, len(g.outOffset), len(g.inOffset))
			}
		})
	}
}

// The build is triggered by readers, and readers of an image are not serialised
// with each other — the fast read path takes no store lock. Run under -race.
func TestAdjacency_ConcurrentFirstUseIsSafe(t *testing.T) {
	nodes, edges := adjacencyCorpus(2000)
	g, err := buildSeq(seqOf(nodes), seqOf(edges), AdjacencyLazy)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	eager, err := buildSeq(seqOf(nodes), seqOf(edges), AdjacencyEager)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	const goroutines = 64
	var wg sync.WaitGroup
	bad := make([]string, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := nodes[i%len(nodes)].ID
			if !sameEdgeIDs(g.OutboundEdgeIDs(id), eager.OutboundEdgeIDs(id)) {
				bad[i] = fmt.Sprintf("node %d outbound disagrees", id)
				return
			}
			if !sameEdgeIDs(g.InboundEdgeIDs(id), eager.InboundEdgeIDs(id)) {
				bad[i] = fmt.Sprintf("node %d inbound disagrees", id)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	for _, b := range bad {
		if b != "" {
			t.Fatalf("concurrent first use: %s", b)
		}
	}
	assertAdjacencyMatchesArena(t, g)
}

func TestAdjacencyMode_String(t *testing.T) {
	for _, tc := range []struct {
		mode AdjacencyMode
		want string
	}{
		{AdjacencyEager, "eager"},
		{AdjacencyLazy, "lazy"},
		{AdjacencyMode(7), "AdjacencyMode(7)"},
	} {
		if got := tc.mode.String(); got != tc.want {
			t.Fatalf("AdjacencyMode(%d).String() = %q, want %q", uint8(tc.mode), got, tc.want)
		}
	}
}

// seqOf is slices.Values under a name that says what it is for here: a fresh,
// repeatable sequence per build, so two builds in one test cannot share a cursor.
func seqOf[T any](s []T) func(func(T) bool) {
	return func(yield func(T) bool) {
		for _, v := range s {
			if !yield(v) {
				return
			}
		}
	}
}

// The premise rootsAdjacencyMode states, held to account.
//
// Verifying an image's roots recomputes trees over records, property entries and
// tombstones. None of those is adjacency, so the graph verifyImage builds to
// check them can be built without it. If a root is ever added over adjacency,
// this fails — which is the point: the alternative is that the arrays start
// being built on demand inside a verification whose cost nobody re-examines.
func TestRoots_DoNotNeedTheAdjacency(t *testing.T) {
	s, dir := openFresh(t)
	determinismFixture(t, s)
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	csr, section, err := deserialiseCSRFrom(readCSR(t, dir), false, rootsAdjacencyMode())
	if err != nil {
		t.Fatalf("deserialise: %v", err)
	}
	if err := verifyCSRRootsOf(csr, section); err != nil {
		t.Fatalf("verifyCSRRootsOf: %v", err)
	}
	if csr.AdjacencyBuilt() {
		t.Fatal("checking the roots built the adjacency arrays — either a root now " +
			"covers adjacency, in which case rootsAdjacencyMode must say so, or " +
			"something on the root path reads it that should not")
	}
}
