package graphene_test

// Traversal budgets and cancellation.
//
// The failure these prevent is not a wrong answer, it is a walk that never
// comes back. Depth was the only limit the engine offered, and depth bounds
// nothing once a hub is in range: the tests below build the shapes that used to
// have no defence and assert that the walk now refuses rather than runs away.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

// hub builds one node with `degree` neighbours attached to it. This is the
// shape the spec calls supernode_bfs_exploding.
func hub(t *testing.T, g *graphene.Graph, degree int) store.NodeID {
	t.Helper()
	centre, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	spokes := make([]*store.Node, degree)
	for i := range spokes {
		spokes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(spokes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	edges := make([]*store.Edge, degree)
	for i, id := range ids {
		edges[i] = &store.Edge{Src: centre, Dst: id, Labels: []store.EdgeType{store.EdgeTypeContains}}
	}
	if _, err := g.AddEdges(edges); err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	return centre
}

func TestBudget_SupernodeBFSTerminates(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		centre := hub(t, g, 20_000)

		// Unbounded, this walk materialises 20 001 nodes and 20 000 edges from
		// one hop. The point is not that it is slow — it is that nothing tells
		// the caller how big it will be before it happens.
		_, err := g.BFSCtx(context.Background(), centre, 1, store.DirectionBoth, nil,
			store.Budget{MaxNodes: 1000})
		if !errors.Is(err, store.ErrBudgetExceeded) {
			t.Fatalf("BFS from a 20k-degree hub with MaxNodes 1000: want ErrBudgetExceeded, got %v", err)
		}

		// The same limit expressed as edges.
		_, err = g.BFSCtx(context.Background(), centre, 1, store.DirectionBoth, nil,
			store.Budget{MaxEdges: 500})
		if !errors.Is(err, store.ErrBudgetExceeded) {
			t.Fatalf("BFS with MaxEdges 500: want ErrBudgetExceeded, got %v", err)
		}

		// And the ID-only walk, which allocates least and so is the one most
		// likely to be reached for on a graph of unknown shape.
		_, err = g.BFSIDsCtx(context.Background(), centre, 1, store.DirectionBoth, nil,
			store.Budget{MaxNodes: 1000})
		if !errors.Is(err, store.ErrBudgetExceeded) {
			t.Fatalf("BFSIDs with MaxNodes 1000: want ErrBudgetExceeded, got %v", err)
		}
	})
}

// TestBudget_ZeroIsUnlimited is the compatibility assertion: a zero Budget must
// be indistinguishable from the unbounded call, or every existing caller is
// affected by a feature they did not ask for.
func TestBudget_ZeroIsUnlimited(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		ids := snapChain(t, g, 400)

		want, err := g.BFS(ids[0], 400, store.DirectionBoth, nil)
		if err != nil {
			t.Fatalf("BFS: %v", err)
		}
		got, err := g.BFSCtx(context.Background(), ids[0], 400, store.DirectionBoth, nil, store.Budget{})
		if err != nil {
			t.Fatalf("BFSCtx: %v", err)
		}
		if len(got.Nodes) != len(want.Nodes) || len(got.Edges) != len(want.Edges) {
			t.Fatalf("a zero budget changed the walk: want %d nodes/%d edges, got %d/%d",
				len(want.Nodes), len(want.Edges), len(got.Nodes), len(got.Edges))
		}

		// A budget larger than the walk must also not change it.
		big, err := g.BFSCtx(context.Background(), ids[0], 400, store.DirectionBoth, nil,
			store.Budget{MaxNodes: 1 << 20, MaxEdges: 1 << 20, MaxTime: time.Minute})
		if err != nil {
			t.Fatalf("BFSCtx with a generous budget: %v", err)
		}
		if len(big.Nodes) != len(want.Nodes) {
			t.Fatalf("a generous budget changed the walk: want %d nodes, got %d", len(want.Nodes), len(big.Nodes))
		}
	})
}

func TestBudget_AppliesToEveryTraversal(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		ids := snapChain(t, g, 2000)
		tight := store.Budget{MaxNodes: 10}

		if _, err := g.DFSCtx(context.Background(), ids[0], 2000, store.DirectionBoth, nil, tight); !errors.Is(err, store.ErrBudgetExceeded) {
			t.Errorf("DFSCtx: want ErrBudgetExceeded, got %v", err)
		}
		if _, err := g.ProvenanceChainCtx(context.Background(), ids[len(ids)-1], 2000, nil, tight); !errors.Is(err, store.ErrBudgetExceeded) {
			t.Errorf("ProvenanceChainCtx: want ErrBudgetExceeded, got %v", err)
		}
		if _, err := g.ShortestPathCtx(context.Background(), ids[0], ids[len(ids)-1], nil, tight); !errors.Is(err, store.ErrBudgetExceeded) {
			t.Errorf("ShortestPathCtx: want ErrBudgetExceeded, got %v", err)
		}

		pattern := &traversal.Pattern{
			Nodes: []traversal.PatternNode{
				{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			},
			Edges: []traversal.PatternEdge{{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeContains}}},
		}
		if _, err := g.FindPatternsCtx(context.Background(), pattern, ids, 0, tight); !errors.Is(err, store.ErrBudgetExceeded) {
			t.Errorf("FindPatternsCtx: want ErrBudgetExceeded, got %v", err)
		}
	})
}

// TestCtx_CancelStopsTraversal checks that a cancelled context aborts a walk
// rather than being noticed only at the end.
func TestCtx_CancelStopsTraversal(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		ids := snapChain(t, g, 4000)

		// Already cancelled: refused before any work at all, which a caller
		// passing a dead context is entitled to.
		dead, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := g.BFSCtx(dead, ids[0], 4000, store.DirectionBoth, nil, store.Budget{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled context did not stop the walk: %v", err)
		}

		// Cancelled while running. The check is periodic, so the walk finishes
		// the current run of steps — what must not happen is it running to
		// completion.
		ctx, cancel2 := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Millisecond)
			cancel2()
		}()
		var lastErr error
		for i := 0; i < 200; i++ {
			_, lastErr = g.BFSCtx(ctx, ids[0], 4000, store.DirectionBoth, nil, store.Budget{})
			if lastErr != nil {
				break
			}
		}
		if !errors.Is(lastErr, context.Canceled) {
			t.Fatalf("cancelling mid-walk did not stop it: %v", lastErr)
		}

		// A deadline is expressed through the budget rather than the context
		// when the caller has no context to hand.
		//
		// Negative rather than a small positive duration, which is what this
		// used to be. A positive MaxTime is only observable once the platform's
		// monotonic clock has advanced past it, and on Windows that clock is the
		// system timer interrupt: 15.6 ms by default, ~0.5 ms while some
		// unrelated program has raised the resolution, and therefore different
		// between two runs on one machine. Whether a walk of a given size
		// straddled a tick was the thing this assertion was really testing, so
		// it passed or failed according to what else the machine was doing.
		//
		// A negative MaxTime is a deadline already passed. It needs no elapsed
		// time to be observed and so means the same thing on every platform.
		// What it does not cover — that a deadline reached mid-walk stops the
		// walk, on the cadence it claims — is covered exactly in
		// traversal/guard_test.go, against a clock the test drives itself.
		if _, err := g.BFSCtx(context.Background(), ids[0], 4000, store.DirectionBoth, nil,
			store.Budget{MaxTime: -time.Second}); !errors.Is(err, store.ErrBudgetExceeded) {
			t.Fatalf("a MaxTime already elapsed did not stop the walk: %v", err)
		}
	})
}

// TestBudget_SnapshotTraversalIsBounded is the two features together: a walk
// that must be both consistent and bounded, which is the combination an
// analytical query over a live store actually needs.
func TestBudget_SnapshotTraversalIsBounded(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		centre := hub(t, g, 5000)

		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer snap.Close()

		_, err = traversal.BFSCtx(context.Background(), snap, centre, 1, store.DirectionBoth, nil,
			store.Budget{MaxNodes: 100})
		if !errors.Is(err, store.ErrBudgetExceeded) {
			t.Fatalf("bounded BFS over a snapshot: want ErrBudgetExceeded, got %v", err)
		}

		full, err := traversal.BFSCtx(context.Background(), snap, centre, 1, store.DirectionBoth, nil, store.Budget{})
		if err != nil {
			t.Fatalf("unbounded BFS over a snapshot: %v", err)
		}
		if len(full.Nodes) != 5001 {
			t.Fatalf("snapshot walk reached %d nodes, want 5001", len(full.Nodes))
		}
	})
}
