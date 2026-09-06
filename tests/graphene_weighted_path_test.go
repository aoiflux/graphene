package graphene_test

// Weighted shortest paths, checked against a naive oracle.
//
// The oracle is a Bellman-Ford written against the public API, in the same
// spirit as referenceBFS: a different algorithm with a different termination
// argument, so an error in the Dijkstra shows up as a disagreement rather than
// as two implementations of the same mistake agreeing with each other.
//
// One deliberate departure from referenceBFS: the oracle walks EdgesOf, not
// Neighbours. Neighbours reports one result per distinct neighbour, which is
// the right shape for an unweighted walk and destroys the case this feature
// exists for — two parallel edges between the same pair are two different
// costs, and an oracle that sees only one of them cannot tell whether the
// search picked the cheaper.

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

// weightCost reads an edge's weight as its traversal cost directly. Fixtures
// below use whole-number weights so that a sum is exact in float64 and the
// oracle and the search cannot disagree in the last bits.
func weightCost(e store.IncidentEdge) float64 { return float64(e.Weight) }

// referenceBellmanFord returns the cheapest cost from src to every reachable
// node, treating the graph as undirected exactly as the searches under test do.
// Unreachable nodes are +Inf.
//
// Deliberately the textbook shape — relax every edge |V|-1 times — because it
// shares nothing with Dijkstra but the answer.
func referenceBellmanFord(t *testing.T, g *graphene.Graph, src store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost) map[store.NodeID]float64 {
	t.Helper()

	ids, err := g.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}

	// Collect every edge once, from its source side.
	type arc struct {
		a, b store.NodeID
		step store.IncidentEdge
	}
	var arcs []arc
	for _, id := range ids {
		edges, err := g.EdgesOf(id, store.DirectionOutbound, edgeTypes)
		if err != nil {
			t.Fatalf("EdgesOf(%d): %v", id, err)
		}
		for _, e := range edges {
			arcs = append(arcs, arc{
				a: e.Src, b: e.Dst,
				step: store.IncidentEdge{Edge: e.ID, Weight: e.Weight},
			})
		}
	}

	dist := make(map[store.NodeID]float64, len(ids))
	for _, id := range ids {
		dist[id] = math.Inf(1)
	}
	dist[src] = 0

	for round := 0; round < len(ids); round++ {
		changed := false
		for _, a := range arcs {
			// Undirected: relax both ways. Neighbour is filled in per direction
			// because a cost function is entitled to look at it.
			for _, d := range [2][2]store.NodeID{{a.a, a.b}, {a.b, a.a}} {
				from, to := d[0], d[1]
				if math.IsInf(dist[from], 1) {
					continue
				}
				step := a.step
				step.Neighbour = to
				if nd := dist[from] + cost(step); nd < dist[to] {
					dist[to] = nd
					changed = true
				}
			}
		}
		if !changed {
			break
		}
	}
	return dist
}

// pathCost re-derives what a returned path costs, from the edge records rather
// than from anything the search reported. It also checks the path is a real
// walk: consecutive nodes joined by the edge between them, in order.
func pathCost(t *testing.T, res *traversal.PathResult, src, dst store.NodeID, cost store.EdgeCost) float64 {
	t.Helper()

	if len(res.Nodes) == 0 {
		t.Fatal("path has no nodes")
	}
	if res.Nodes[0].ID != src {
		t.Fatalf("path starts at %d, want %d", res.Nodes[0].ID, src)
	}
	if res.Nodes[len(res.Nodes)-1].ID != dst {
		t.Fatalf("path ends at %d, want %d", res.Nodes[len(res.Nodes)-1].ID, dst)
	}
	if len(res.Edges) != len(res.Nodes)-1 {
		t.Fatalf("path has %d nodes and %d edges", len(res.Nodes), len(res.Edges))
	}

	total := 0.0
	for i, e := range res.Edges {
		from, to := res.Nodes[i].ID, res.Nodes[i+1].ID
		if !(e.Src == from && e.Dst == to) && !(e.Src == to && e.Dst == from) {
			t.Fatalf("edge %d (%d->%d) does not join path nodes %d and %d",
				e.ID, e.Src, e.Dst, from, to)
		}
		total += cost(store.IncidentEdge{Edge: e.ID, Neighbour: to, Weight: e.Weight})
	}
	return total
}

// buildWeightedFixture is buildTraversalFixture with costs, plus the shapes a
// weighted search gets wrong that an unweighted one cannot: parallel edges of
// unequal cost, a zero-cost edge, and two routes whose hop counts and costs
// disagree.
//
// Weights are whole numbers so the arithmetic is exact; nothing here depends on
// them being small.
func buildWeightedFixture(t *testing.T, g *graphene.Graph, n int) []store.NodeID {
	t.Helper()

	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{benchLabelFor(i)}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
	}

	add := func(src, dst store.NodeID, lbl store.EdgeType, w float32) {
		if _, err := g.AddEdge(&store.Edge{
			Src: src, Dst: dst,
			Labels: []store.EdgeType{lbl},
			Weight: w,
		}); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}

	// A chain, cheap per hop.
	for i := 0; i < n-1; i++ {
		add(ids[i], ids[i+1], store.EdgeTypeContains, float32(1+i%3))
	}
	// Long strides, expensive per hop — so the fewest-hops route and the
	// cheapest route are different routes, which is the whole point.
	for i := 0; i < n; i++ {
		add(ids[i], ids[(i+7)%n], store.EdgeTypeSimilarTo, float32(9+i%4))
	}
	// Two parallel edges between the same pair, one much cheaper. A search that
	// dedupes by neighbour takes whichever adjacency reports first.
	add(ids[0], ids[1], store.EdgeTypeReuse, 40)
	add(ids[0], ids[1], store.EdgeTypeBelongsTo, 2)
	// A self-loop, which must not make the search loop.
	add(ids[2], ids[2], store.EdgeTypeSimilarTo, 5)
	// A zero-cost edge: legal, and the case a `> 0` guard would reject.
	add(ids[3], ids[4], store.EdgeTypeReuse, 0)
	// A back edge closing a cycle.
	add(ids[n-1], ids[0], store.EdgeTypeContains, 6)

	return ids
}

// buildWeightedChain is a bare line of nodes, each joined to the next by one
// edge of weight 1. No strides, no parallel edges: a search from one end must
// cross every node in between, which is what makes "an edge deeper in the walk"
// mean something.
func buildWeightedChain(t *testing.T, g *graphene.Graph, n int) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 0; i < n-1; i++ {
		if _, err := g.AddEdge(&store.Edge{
			Src: ids[i], Dst: ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
			Weight: 1,
		}); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	return ids
}

// TestShortestWeightedPath_MatchesBellmanFord is the parity check, over every
// backend, both layer states, several edge-type filters, and many endpoints.
func TestShortestWeightedPath_MatchesBellmanFord(t *testing.T) {
	for name, open := range traversalBackends() {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				ids := buildWeightedFixture(t, g, 40)
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				filters := [][]store.EdgeType{
					nil,
					{store.EdgeTypeContains},
					{store.EdgeTypeContains, store.EdgeTypeSimilarTo},
					{store.EdgeTypeReuse},
				}

				for _, et := range filters {
					for _, src := range []store.NodeID{ids[0], ids[5], ids[len(ids)-1]} {
						want := referenceBellmanFord(t, g, src, et, weightCost)

						for _, dst := range ids {
							res, err := g.ShortestWeightedPath(src, dst, et, weightCost)
							wantCost := want[dst]

							if math.IsInf(wantCost, 1) {
								if !errors.Is(err, traversal.ErrNoPath) {
									t.Fatalf("types=%v %d->%d: unreachable per the oracle, got err=%v",
										et, src, dst, err)
								}
								continue
							}
							if err != nil {
								t.Fatalf("types=%v %d->%d: want cost %v, got err %v",
									et, src, dst, wantCost, err)
							}
							if got := pathCost(t, res, src, dst, weightCost); got != wantCost {
								t.Fatalf("types=%v %d->%d: path costs %v, cheapest is %v",
									et, src, dst, got, wantCost)
							}
						}
					}
				}
			})
		}
	}
}

// TestShortestWeightedPath_PrefersCheapOverShort is the feature, stated as a
// test: a route with more hops and less cost must win, and the unweighted
// search must disagree — otherwise the weighted one is not doing anything.
func TestShortestWeightedPath_PrefersCheapOverShort(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)

			mk := func() store.NodeID {
				id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
				if err != nil {
					t.Fatalf("AddNode: %v", err)
				}
				return id
			}
			link := func(a, b store.NodeID, w float32) {
				if _, err := g.AddEdge(&store.Edge{
					Src: a, Dst: b,
					Labels: []store.EdgeType{store.EdgeTypeContains},
					Weight: w,
				}); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			}

			// src --100--> toll --------> dst      one hop each, 200 total
			//  \--1--> a --1--> b --1--> c --1--> dst   four hops, 4 total
			src, dst := mk(), mk()
			toll := mk()
			a, b, c := mk(), mk(), mk()

			link(src, toll, 100)
			link(toll, dst, 100)
			link(src, a, 1)
			link(a, b, 1)
			link(b, c, 1)
			link(c, dst, 1)

			short, err := g.ShortestPath(src, dst, nil)
			if err != nil {
				t.Fatalf("ShortestPath: %v", err)
			}
			if len(short.Edges) != 2 {
				t.Fatalf("unweighted search took %d hops, want the 2-hop route", len(short.Edges))
			}

			cheap, err := g.ShortestWeightedPath(src, dst, nil, weightCost)
			if err != nil {
				t.Fatalf("ShortestWeightedPath: %v", err)
			}
			if got := pathCost(t, cheap, src, dst, weightCost); got != 4 {
				t.Fatalf("weighted search cost %v, want 4 (the four-hop route)", got)
			}
			if len(cheap.Edges) != 4 {
				t.Fatalf("weighted search took %d hops, want 4", len(cheap.Edges))
			}
		})
	}
}

// TestShortestWeightedPath_TakesTheCheaperParallelEdge is the case the dropped
// neighbour dedupe exists for. Both edges reach the same node; only one of them
// is cheap, and which one adjacency reports first is not something the caller
// controls.
func TestShortestWeightedPath_TakesTheCheaperParallelEdge(t *testing.T) {
	for name, open := range traversalBackends() {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				var ids [2]store.NodeID
				for i := range ids {
					id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
					if err != nil {
						t.Fatalf("AddNode: %v", err)
					}
					ids[i] = id
				}
				// Dear one first, so taking the first reported edge is wrong.
				for _, w := range []float32{50, 3} {
					if _, err := g.AddEdge(&store.Edge{
						Src: ids[0], Dst: ids[1],
						Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
						Weight: w,
					}); err != nil {
						t.Fatalf("AddEdge: %v", err)
					}
				}
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				res, err := g.ShortestWeightedPath(ids[0], ids[1], nil, weightCost)
				if err != nil {
					t.Fatalf("ShortestWeightedPath: %v", err)
				}
				if got := pathCost(t, res, ids[0], ids[1], weightCost); got != 3 {
					t.Fatalf("took the %v edge, want the 3 one", got)
				}
			})
		}
	}
}

// TestShortestWeightedPath_RefusesBadCosts covers what the contract refuses,
// and the fact that it refuses rather than returning a plausible wrong path.
func TestShortestWeightedPath_RefusesBadCosts(t *testing.T) {
	g := graphene.NewInMemory()
	t.Cleanup(func() { g.Close() })
	ids := buildWeightedFixture(t, g, 10)

	t.Run("nil", func(t *testing.T) {
		_, err := g.ShortestWeightedPath(ids[0], ids[9], nil, nil)
		if !errors.Is(err, traversal.ErrNilCost) {
			t.Fatalf("a nil cost was accepted: %v", err)
		}
	})

	t.Run("negative", func(t *testing.T) {
		_, err := g.ShortestWeightedPath(ids[0], ids[9], nil,
			func(store.IncidentEdge) float64 { return -1 })
		if !errors.Is(err, traversal.ErrNegativeCost) {
			t.Fatalf("a negative cost was accepted: %v", err)
		}
	})

	t.Run("NaN", func(t *testing.T) {
		_, err := g.ShortestWeightedPath(ids[0], ids[9], nil,
			func(store.IncidentEdge) float64 { return math.NaN() })
		if !errors.Is(err, traversal.ErrNegativeCost) {
			t.Fatalf("a NaN cost was accepted: %v", err)
		}
	})

	// The check is per relaxation, not a sample of the first edge: a cost that
	// only goes negative deeper in the walk must still be caught. A plain chain,
	// so reaching the far end has to cross the bad edge.
	t.Run("negative only later", func(t *testing.T) {
		c := graphene.NewInMemory()
		t.Cleanup(func() { c.Close() })
		chain := buildWeightedChain(t, c, 20)

		_, err := c.ShortestWeightedPath(chain[0], chain[19], nil,
			func(e store.IncidentEdge) float64 {
				if e.Neighbour == chain[15] {
					return -5
				}
				return 1
			})
		if !errors.Is(err, traversal.ErrNegativeCost) {
			t.Fatalf("a negative cost deeper in the walk was accepted: %v", err)
		}
	})

	// And the honest limit of that guarantee, pinned so it is not mistaken for a
	// bug later: the refusal covers costs the search *uses*. Dijkstra stops the
	// moment the destination is settled, so a negative edge in a corner it never
	// examined is never reported — and the path returned is still correct,
	// because that edge played no part in it. Making this an error instead would
	// mean costing every edge in the graph before answering, which is the whole
	// expense the search exists to avoid.
	t.Run("a negative edge the search never reaches", func(t *testing.T) {
		c := graphene.NewInMemory()
		t.Cleanup(func() { c.Close() })
		chain := buildWeightedChain(t, c, 20)

		res, err := c.ShortestWeightedPath(chain[0], chain[2], nil,
			func(e store.IncidentEdge) float64 {
				if e.Neighbour == chain[18] {
					return -5
				}
				return 1
			})
		if err != nil {
			t.Fatalf("a negative cost outside the searched region was reported: %v", err)
		}
		if len(res.Edges) != 2 {
			t.Fatalf("got a %d-hop path, want 2", len(res.Edges))
		}
	})

	// Zero is not negative, and a zero-cost graph is a legitimate one.
	t.Run("all zero", func(t *testing.T) {
		res, err := g.ShortestWeightedPath(ids[0], ids[9], nil,
			func(store.IncidentEdge) float64 { return 0 })
		if err != nil {
			t.Fatalf("a zero cost was refused: %v", err)
		}
		if got := pathCost(t, res, ids[0], ids[9], func(store.IncidentEdge) float64 { return 0 }); got != 0 {
			t.Fatalf("cost %v over a zero-cost graph", got)
		}
	})
}

// TestShortestWeightedPath_Degenerate covers the endpoints that are not really
// a search: a node to itself, and a node with nothing between it and the target.
func TestShortestWeightedPath_Degenerate(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := buildWeightedFixture(t, g, 8)

			island, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}

			res, err := g.ShortestWeightedPath(ids[0], ids[0], nil, weightCost)
			if err != nil {
				t.Fatalf("src == dst: %v", err)
			}
			if len(res.Nodes) != 1 || len(res.Edges) != 0 {
				t.Fatalf("src == dst returned %d nodes / %d edges, want 1 / 0",
					len(res.Nodes), len(res.Edges))
			}

			if _, err := g.ShortestWeightedPath(ids[0], island, nil, weightCost); !errors.Is(err, traversal.ErrNoPath) {
				t.Fatalf("a disconnected node: want ErrNoPath, got %v", err)
			}

			if _, err := g.ShortestWeightedPath(ids[0], store.NodeID(999999), nil, weightCost); err == nil {
				t.Fatal("a nonexistent destination returned a path")
			}
		})
	}
}

// TestAStarPath_MatchesDijkstra pins the relationship between the two: with an
// admissible heuristic A* must return a path of the same cost, and with nil it
// must be Dijkstra exactly.
func TestAStarPath_MatchesDijkstra(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := buildWeightedFixture(t, g, 30)

			// Admissible by construction: the cheapest edge in the fixture that
			// is not free costs 1, so "hops remaining, at 1 each" can never
			// exceed the true cost. Hops remaining is estimated from the chain
			// index, which is the one thing this fixture guarantees.
			index := make(map[store.NodeID]int, len(ids))
			for i, id := range ids {
				index[id] = i
			}

			for _, dst := range []store.NodeID{ids[7], ids[20], ids[29]} {
				want, err := g.ShortestWeightedPath(ids[0], dst, nil, weightCost)
				if err != nil {
					t.Fatalf("Dijkstra %d: %v", dst, err)
				}
				wantCost := pathCost(t, want, ids[0], dst, weightCost)

				h := func(id store.NodeID) float64 {
					d := index[dst] - index[id]
					if d < 0 {
						d = -d
					}
					// Divided down so the estimate stays under the true cost
					// even where the cheapest route is not the chain.
					return float64(d) / 8
				}

				got, err := g.AStarPath(ids[0], dst, nil, weightCost, h)
				if err != nil {
					t.Fatalf("A* %d: %v", dst, err)
				}
				if c := pathCost(t, got, ids[0], dst, weightCost); c != wantCost {
					t.Fatalf("A* to %d cost %v, Dijkstra %v", dst, c, wantCost)
				}

				nilH, err := g.AStarPath(ids[0], dst, nil, weightCost, nil)
				if err != nil {
					t.Fatalf("A* with a nil heuristic %d: %v", dst, err)
				}
				if c := pathCost(t, nilH, ids[0], dst, weightCost); c != wantCost {
					t.Fatalf("A* with a nil heuristic to %d cost %v, Dijkstra %v", dst, c, wantCost)
				}
			}
		})
	}
}

// TestShortestWeightedPath_BudgetChargesSettledNodes pins where MaxNodes is
// charged. Charging at discovery instead would make the same limit stop this
// walk far earlier than a BFS over the same graph, which is the surprise the
// choice exists to avoid.
func TestShortestWeightedPath_BudgetChargesSettledNodes(t *testing.T) {
	g := graphene.NewInMemory()
	t.Cleanup(func() { g.Close() })
	ids := buildWeightedFixture(t, g, 60)

	tiny := store.Budget{MaxNodes: 3}
	_, err := g.ShortestWeightedPathCtx(t.Context(), ids[0], ids[59], nil, weightCost, tiny)
	if !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("MaxNodes: 3 did not stop a 60-node search: %v", err)
	}

	// The same walk unbounded must succeed, or the test above is only proving
	// that the fixture is broken.
	if _, err := g.ShortestWeightedPathCtx(t.Context(), ids[0], ids[59], nil, weightCost, store.Budget{}); err != nil {
		t.Fatalf("unbounded: %v", err)
	}

	// A budget larger than the graph must not fire.
	roomy := store.Budget{MaxNodes: 10_000, MaxEdges: 10_000}
	if _, err := g.ShortestWeightedPathCtx(t.Context(), ids[0], ids[59], nil, weightCost, roomy); err != nil {
		t.Fatalf("a budget larger than the graph fired: %v", err)
	}
}

// TestAStarPath_InconsistentHeuristicTerminates is the guard on the settled
// test in weightedPath.
//
// An inconsistent heuristic can reach a node again, later, at a genuinely
// smaller distance. Accepting that update would clear the node's settled flag
// and re-point its parent at a node whose own chain runs back through it — a
// cycle in the parent chain, which the reconstruction walks forever. The
// documented cost of a bad heuristic is a path that may not be the cheapest;
// it is not a hang, and this is what says so.
//
// The heuristic below is deliberately adversarial rather than merely poor: it
// swings between zero and a large value with no relation to the graph.
func TestAStarPath_InconsistentHeuristicTerminates(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := buildWeightedFixture(t, g, 40)

			rank := make(map[store.NodeID]int, len(ids))
			for i, id := range ids {
				rank[id] = i
			}
			h := func(id store.NodeID) float64 {
				if rank[id]%3 == 0 {
					return 500
				}
				return 0
			}

			done := make(chan struct{})
			var res *traversal.PathResult
			var err error
			go func() {
				defer close(done)
				res, err = g.AStarPathCtx(t.Context(), ids[0], ids[39], nil, weightCost, h, store.Budget{})
			}()

			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("A* with an inconsistent heuristic did not terminate")
			}

			if err != nil {
				t.Fatalf("A* with an inconsistent heuristic: %v", err)
			}
			// The path may be dearer than the cheapest — that is the documented
			// cost — but it must be a real, connected path from src to dst.
			pathCost(t, res, ids[0], ids[39], weightCost)
		})
	}
}

// TestAStarPath_InconsistentHeuristicCostsCorrectness demonstrates that
// store.NodeHeuristic's consistency obligation is real rather than boilerplate,
// and pins the choice weightedPath makes when it is broken.
//
// The graph has a cheap two-hop route and a dear one-hop route to the same
// node. The heuristic is inconsistent: it hides the node on the cheap route
// behind a huge estimate, so the dear route is settled first. Because a settled
// node is never reopened, the cheap route is found too late to be used and the
// search returns the dear answer — without an error, which is exactly why the
// obligation is the caller's to keep.
//
// Dijkstra over the same graph finds the cheap route, which is what makes this
// a statement about the heuristic and not about the graph.
func TestAStarPath_InconsistentHeuristicCostsCorrectness(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)

			mk := func() store.NodeID {
				id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
				if err != nil {
					t.Fatalf("AddNode: %v", err)
				}
				return id
			}
			link := func(a, b store.NodeID, w float32) {
				if _, err := g.AddEdge(&store.Edge{
					Src: a, Dst: b,
					Labels: []store.EdgeType{store.EdgeTypeContains},
					Weight: w,
				}); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			}

			src, mid, hub, dst := mk(), mk(), mk(), mk()
			link(src, hub, 10) // dear, direct
			link(src, mid, 1)  // cheap, but two hops
			link(mid, hub, 1)
			link(hub, dst, 1)

			// Cheapest is src -> mid -> hub -> dst == 3.
			cheap, err := g.ShortestWeightedPath(src, dst, nil, weightCost)
			if err != nil {
				t.Fatalf("Dijkstra: %v", err)
			}
			if got := pathCost(t, cheap, src, dst, weightCost); got != 3 {
				t.Fatalf("Dijkstra found cost %v, want 3", got)
			}

			// Inconsistent: h(mid) is enormous while h(hub) is zero, so hub is
			// settled by way of the dear edge before mid is looked at, and the
			// estimate drops by 100 across an edge costing 1.
			h := func(id store.NodeID) float64 {
				switch id {
				case mid:
					return 100
				case dst:
					return 1000
				default:
					return 0
				}
			}

			got, err := g.AStarPath(src, dst, nil, weightCost, h)
			if err != nil {
				t.Fatalf("A*: %v", err)
			}
			if c := pathCost(t, got, src, dst, weightCost); c != 11 {
				t.Fatalf("A* with an inconsistent heuristic cost %v; want 11, the dear route "+
					"it is documented to settle for. If this now returns 3, the search has "+
					"started reopening settled nodes and both the doc comment on "+
					"store.NodeHeuristic and the settled test in weightedPath need revisiting.", c)
			}
		})
	}
}
