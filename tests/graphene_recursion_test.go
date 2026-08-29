package graphene_test

// The one failure a caller cannot handle.
//
// HasCycle recurses once per hop and its only bound was the caller's maxDepth,
// so a deep graph plus a generous depth overflowed the goroutine stack — a
// crash, in a library embedded in someone else's process. store.MaxRecursionDepth
// turns it into an error. The traversal package's walks were guarded in Phase 1;
// this one is not in that package and was missed.

import (
	"errors"
	"math"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// deepChain builds a path of length nodes in two batch calls, which matters:
// this is the one test that needs a graph deeper than the recursion limit, and
// building it one record at a time would dominate its runtime.
func deepChain(t *testing.T, g *graphene.Graph, length int) store.NodeID {
	t.Helper()
	nodes := make([]*store.Node, length)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	edges := make([]*store.Edge, length-1)
	for i := range edges {
		edges[i] = &store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{store.EdgeTypeContains}}
	}
	if _, err := g.AddEdges(edges); err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	return ids[0]
}

func TestHasCycle_RefusesToOverflowTheStack(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()

	// One frame past the limit is enough, and the chain is acyclic — so without
	// the guard this does not return an error, it recurses until the runtime
	// kills the process. The memory backend alone: the graph's depth is what is
	// under test and it is a property of the records, not of a backend.
	origin := deepChain(t, g, store.MaxRecursionDepth+2)

	found, err := g.HasCycle(origin, math.MaxInt, nil)
	if !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("HasCycle down a %d-node chain with an unbounded depth: want ErrBudgetExceeded, got %v",
			store.MaxRecursionDepth+2, err)
	}
	if found {
		t.Error("HasCycle reported a cycle in an acyclic chain")
	}
}

func TestHasCycle_TheGuardDoesNotFireOnRealWalks(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()

	// Well inside the limit, and still deeper than anything a caller would
	// plausibly walk. The guard must be invisible here, or it has replaced a
	// crash with a refusal to do legitimate work.
	origin := deepChain(t, g, 5000)

	found, err := g.HasCycle(origin, math.MaxInt, nil)
	if err != nil {
		t.Fatalf("HasCycle down a 5 000-node chain: %v", err)
	}
	if found {
		t.Error("HasCycle reported a cycle in an acyclic chain")
	}
}
