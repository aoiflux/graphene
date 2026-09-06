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
	// One allocation: the result. The label postings are merged rather than
	// concatenated and deduped through a map, so the copy and the map are gone
	// and this is the floor a slice-returning read can reach.
	assertAllocsAtMost(t, "NodesByType", 2, func() { g.NodesByType(store.NodeTypeMicroArtefact) })

	// A window on a labelled query must cost the window. The driver stops at
	// offset+limit, so this is bounded by the ten rows asked for and not by the
	// two hundred that carry the label — the whole point of the push-down, and
	// the count that would climb back first if the driver stopped honouring it.
	limited := store.NodeQuery{Types: []store.NodeType{store.NodeTypeMicroArtefact}, Limit: 10}
	assertAllocsAtMost(t, "QueryNodeIDs with Limit 10", 4, func() { g.QueryNodeIDs(limited) })
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

// TestAllocGuards_Scan pins what iteration costs.
//
// A scan exists so a caller can visit a graph without holding it, so the number
// that matters is not the total but that it does not grow with the graph: the
// disk implementation resolves a fixed batch at a time and reuses one buffer
// across batches, so a full walk of two hundred nodes allocates what a walk of
// two hundred thousand would.
func TestAllocGuards_Scan(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates; these counts are not the engine's under it")
	}
	g, _ := allocGuardFixture(t)

	snap, err := g.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snap.Close() }()
	sc, ok := snap.(store.Scanner)
	if !ok {
		t.Fatal("a disk snapshot must implement store.Scanner")
	}

	assertAllocsAtMost(t, "ScanNodes, drained", 8, func() {
		for _, err := range sc.ScanNodes() {
			if err != nil {
				t.Fatal(err)
			}
		}
	})
	assertAllocsAtMost(t, "ScanNodes, stopped at the first id", 8, func() {
		for range sc.ScanNodes() {
			break
		}
	})
	assertAllocsAtMost(t, "ScanNodesByType, drained", 8, func() {
		for _, err := range sc.ScanNodesByType(store.NodeTypeMicroArtefact) {
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

// TestAllocGuards_Paths pins what a path search costs in allocations, and it is
// a slope rather than a ceiling.
//
// A ceiling on this fixture would mostly measure the fixture: a 200-node chain
// makes a 200-node path, and materialising it is one *store.Node plus one
// *store.Edge per element — so the count is dominated by the answer's size and
// would barely move if the search itself started allocating per node examined.
// What the searches actually promise is that the cost tracks the path and not
// the walk, so that is what is asserted: run the same search to two different
// depths and check the difference is the records, with the search's own
// allocation constant.
//
// The bar this replaces was a benchmark number nobody could fail: 37 allocs/op
// on BenchmarkShortestPath, recorded and then unguarded while Phase 4
// refactored the tail both searches now share.
func TestAllocGuards_Paths(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates; these counts are not the engine's under it")
	}
	g, ids := allocGuardFixture(t)

	const near, far = 20, 180

	// perNode is what materialisePath must cost and nothing more: GetNode and
	// GetEdge each return a pointer, so two allocations per step of the path.
	// The slack is for the frontier and heap buffers, which double as the walk
	// deepens and so contribute a handful of allocations across the whole span,
	// not a share of each node.
	const perNode = 2.2

	measure := func(name string, run func(dst store.NodeID)) (float64, float64) {
		shortRun := testing.AllocsPerRun(100, func() { run(ids[near]) })
		longRun := testing.AllocsPerRun(100, func() { run(ids[far]) })

		grew := longRun - shortRun
		if ceiling := perNode * float64(far-near); grew > ceiling {
			t.Errorf("%s: %.0f more allocs for %d more path nodes (%.2f each), ceiling is %.2f — "+
				"the search is allocating per node examined, not per node returned",
				name, grew, far-near, grew/float64(far-near), perNode)
		}

		// What is left after the records is the search itself: its maps, its
		// queue, and the result. Constant, and small.
		overhead := shortRun - perNode*float64(near)
		if overhead > 30 {
			t.Errorf("%s: %.0f allocs beyond the path records on a %d-node path, ceiling is 30",
				name, overhead, near)
		}
		return shortRun, longRun
	}

	measure("ShortestPath", func(dst store.NodeID) { g.ShortestPath(ids[0], dst, nil) })

	cost := func(store.IncidentEdge) float64 { return 1 }
	measure("ShortestWeightedPath", func(dst store.NodeID) {
		g.ShortestWeightedPath(ids[0], dst, nil, cost)
	})

	// A nil heuristic must cost nothing over Dijkstra — weightedPath tests for
	// nil rather than calling an always-zero function precisely so that it does
	// not, and this is what would notice if that changed.
	measure("AStarPath with a nil heuristic", func(dst store.NodeID) {
		g.AStarPath(ids[0], dst, nil, cost, nil)
	})
}
