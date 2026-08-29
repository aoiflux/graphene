package graphene_test

// Allocation guard tests.
//
// A benchmark records a number once; a guard test fails when it comes back.
// Before these existed nothing in the suite failed if GetNode started
// allocating — CONTRIBUTING §2's "a test that cannot fail is worse than no
// test", pointed at performance rather than correctness.
//
// These are deliberately untagged, so they run in `make check` alongside
// everything else: they are microseconds on a 200-node fixture, not a stress
// fixture. What they assert is a *ceiling*, not an exact count, because a
// tighter number is a win and should not be a test failure — the failure that
// matters is the count climbing back.

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// allocGuardFixture builds a small compacted disk store: 200 nodes in a chain,
// compacted so the reads under test are answered from the CSR image, which is
// the path these counts describe.
func allocGuardFixture(t *testing.T) (*graphene.Graph, []store.NodeID) {
	t.Helper()

	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })

	ids := make([]store.NodeID, 0, 200)
	for i := 0; i < 200; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	edges := make([]*store.Edge, 0, 200)
	for i := 0; i < len(ids)-1; i++ {
		edges = append(edges, &store.Edge{
			Src: ids[i], Dst: ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		t.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	return g, ids
}

// assertAllocsAtMost fails if fn allocates more than want times per call.
func assertAllocsAtMost(t *testing.T, name string, want float64, fn func()) {
	t.Helper()
	if got := testing.AllocsPerRun(200, fn); got > want {
		t.Errorf("%s: %.0f allocs/op, ceiling is %.0f", name, got, want)
	}
}

func TestAllocGuards_ReadPath(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates; these counts are not the engine's under it")
	}
	g, ids := allocGuardFixture(t)
	mid := ids[100]

	// GetNode returns a *store.Node, so one allocation *is* its contract. The
	// guard is that it is still exactly the result and nothing beside it — this
	// is the ~6 ns lock-free path and the control for every A/B in Phase 7.
	assertAllocsAtMost(t, "GetNode", 1, func() { g.GetNode(mid) })

	// Degree answers from the CSR's offset arithmetic and must materialise
	// nothing at all. This is the one that should never move off zero.
	assertAllocsAtMost(t, "Degree", 0, func() { g.Degree(mid, nil) })

	assertAllocsAtMost(t, "EdgeExists", 3, func() { g.EdgeExists(mid, ids[101], nil) })
	assertAllocsAtMost(t, "EdgesOf", 6, func() { g.EdgesOf(mid, store.DirectionBoth, nil) })
	assertAllocsAtMost(t, "Neighbours", 9, func() { g.Neighbours(mid, store.DirectionBoth, nil) })
	assertAllocsAtMost(t, "NodesByType", 5, func() { g.NodesByType(store.NodeTypeMicroArtefact) })
	assertAllocsAtMost(t, "BFSIDs depth 3", 6, func() {
		g.BFSIDs(mid, 3, store.DirectionOutbound, nil)
	})
}

// TestAllocGuards_AdjacencyReader pins the two extension-interface paths a
// traversal actually runs on: NodeExists, and IncidentEdges appending into a
// buffer the caller already grew. Both must be allocation-free, which is the
// whole reason walker.go reuses one buffer across a walk.
func TestAllocGuards_AdjacencyReader(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates; these counts are not the engine's under it")
	}
	g, ids := allocGuardFixture(t)

	s, ok := g.Forensics()
	if !ok {
		t.Fatal("expected a disk store")
	}
	adj, ok := any(s).(store.AdjacencyReader)
	if !ok {
		t.Fatal("disk store does not implement store.AdjacencyReader")
	}

	mid := ids[100]
	assertAllocsAtMost(t, "NodeExists", 0, func() { adj.NodeExists(mid) })

	// Warm the buffer first, exactly as a walk does, so what is measured is the
	// steady state rather than the first expansion.
	buf, err := adj.IncidentEdges(nil, mid, store.DirectionBoth, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertAllocsAtMost(t, "IncidentEdges into a warm buffer", 0, func() {
		buf, _ = adj.IncidentEdges(buf[:0], mid, store.DirectionBoth, nil)
	})
}
