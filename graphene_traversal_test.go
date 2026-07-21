package graphene_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

// Traversal was rewritten to walk adjacency by edge ID and materialise records
// only when they are kept. These tests pin the two invariants that makes safe:
// BFSIDs must agree with BFS on which nodes are reachable, and the rewritten
// walks must return exactly what a straightforward Neighbours-based walk does.

// referenceBFS is a deliberately naive breadth-first walk written directly
// against the public Neighbours API — the shape the optimised traversal
// replaced. It is the oracle for these tests.
func referenceBFS(t *testing.T, g *graphene.Graph, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (nodes []store.NodeID, edges []store.EdgeID) {
	t.Helper()
	if maxDepth < 0 {
		maxDepth = 0
	}
	if _, err := g.GetNode(origin); err != nil {
		return nil, nil
	}

	visited := map[store.NodeID]struct{}{origin: {}}
	seenEdges := map[store.EdgeID]struct{}{}
	nodes = []store.NodeID{origin}

	type qitem struct {
		id    store.NodeID
		depth int
	}
	queue := []qitem{{id: origin, depth: 0}}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.depth >= maxDepth {
			continue
		}
		nbs, err := g.Neighbours(cur.id, dir, edgeTypes)
		if err != nil {
			t.Fatalf("Neighbours: %v", err)
		}
		for _, nb := range nbs {
			if _, seen := seenEdges[nb.Edge.ID]; !seen {
				seenEdges[nb.Edge.ID] = struct{}{}
				edges = append(edges, nb.Edge.ID)
			}
			if _, seen := visited[nb.Node.ID]; seen {
				continue
			}
			visited[nb.Node.ID] = struct{}{}
			nodes = append(nodes, nb.Node.ID)
			queue = append(queue, qitem{id: nb.Node.ID, depth: cur.depth + 1})
		}
	}
	return nodes, edges
}

// buildTraversalFixture creates a graph with branching, cycles, multi-edges
// between the same pair, self-loops, and mixed edge types — the shapes where a
// dedupe or ordering mistake would show up.
func buildTraversalFixture(t *testing.T, g *graphene.Graph, n int) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{benchLabelFor(i)}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
	}

	add := func(src, dst store.NodeID, lbl store.EdgeType) {
		if _, err := g.AddEdge(&store.Edge{Src: src, Dst: dst, Labels: []store.EdgeType{lbl}}); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}

	for i := 0; i < n-1; i++ {
		add(ids[i], ids[i+1], store.EdgeTypeContains)
	}
	for i := 0; i < n; i++ {
		add(ids[i], ids[(i+7)%n], store.EdgeTypeSimilarTo)
	}
	// Two parallel edges between the same pair — Neighbours reports one.
	add(ids[0], ids[1], store.EdgeTypeReuse)
	add(ids[0], ids[1], store.EdgeTypeBelongsTo)
	// A self-loop.
	add(ids[2], ids[2], store.EdgeTypeSimilarTo)
	// A back edge, closing a cycle.
	add(ids[n-1], ids[0], store.EdgeTypeContains)

	return ids
}

// edgeIDsOf extracts edge IDs from a BFS result, returning nil for an empty
// result so comparisons against the reference walk are not tripped up by the
// nil-versus-empty-slice distinction.
func edgeIDsOf(r *traversal.BFSResult) []store.EdgeID {
	if r == nil || len(r.Edges) == 0 {
		return nil
	}
	out := make([]store.EdgeID, len(r.Edges))
	for i, e := range r.Edges {
		out[i] = e.ID
	}
	return out
}

func traversalBackends() map[string]func(t *testing.T) *graphene.Graph {
	return map[string]func(t *testing.T) *graphene.Graph{
		"memory": func(t *testing.T) *graphene.Graph { return graphene.NewInMemory() },
		"disk": func(t *testing.T) *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { g.Close() })
			return g
		},
	}
}

func TestBFS_MatchesNeighbourReference(t *testing.T) {
	dirs := []store.Direction{store.DirectionOutbound, store.DirectionInbound, store.DirectionBoth}
	depths := []int{0, 1, 2, 3, 10}

	for name, open := range traversalBackends() {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				ids := buildTraversalFixture(t, g, 60)
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				for _, dir := range dirs {
					for _, depth := range depths {
						for _, et := range [][]store.EdgeType{nil, {store.EdgeTypeContains}, {store.EdgeTypeContains, store.EdgeTypeSimilarTo}} {
							wantNodes, wantEdges := referenceBFS(t, g, ids[0], depth, dir, et)

							got, err := g.BFS(ids[0], depth, dir, et)
							if err != nil {
								t.Fatalf("BFS: %v", err)
							}
							gotNodes := graphene.NodeIDsFromBFS(got)
							gotEdges := edgeIDsOf(got)

							if !reflect.DeepEqual(gotNodes, wantNodes) {
								t.Fatalf("dir=%v depth=%d types=%v: BFS nodes\n got: %v\nwant: %v", dir, depth, et, gotNodes, wantNodes)
							}
							if !reflect.DeepEqual(gotEdges, wantEdges) {
								t.Fatalf("dir=%v depth=%d types=%v: BFS edges\n got: %v\nwant: %v", dir, depth, et, gotEdges, wantEdges)
							}
						}
					}
				}
			})
		}
	}
}

func TestBFSIDs_MatchesBFS(t *testing.T) {
	dirs := []store.Direction{store.DirectionOutbound, store.DirectionInbound, store.DirectionBoth}
	depths := []int{0, 1, 2, 3, 10}

	for name, open := range traversalBackends() {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				ids := buildTraversalFixture(t, g, 60)
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				for _, origin := range []store.NodeID{ids[0], ids[7], ids[len(ids)-1]} {
					for _, dir := range dirs {
						for _, depth := range depths {
							for _, et := range [][]store.EdgeType{nil, {store.EdgeTypeContains}} {
								full, err := g.BFS(origin, depth, dir, et)
								if err != nil {
									t.Fatalf("BFS: %v", err)
								}
								want := graphene.NodeIDsFromBFS(full)

								got, err := g.BFSIDs(origin, depth, dir, et)
								if err != nil {
									t.Fatalf("BFSIDs: %v", err)
								}
								if !reflect.DeepEqual(got, want) {
									t.Fatalf("origin=%d dir=%v depth=%d types=%v\n BFSIDs: %v\n BFS:    %v", origin, dir, depth, et, got, want)
								}
							}
						}
					}
				}
			})
		}
	}
}

// BFSIDs must report a missing origin the same way BFS does.
func TestBFSIDs_MissingOrigin(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			buildTraversalFixture(t, g, 10)

			_, bfsErr := g.BFS(store.NodeID(9999), 3, store.DirectionBoth, nil)
			_, idsErr := g.BFSIDs(store.NodeID(9999), 3, store.DirectionBoth, nil)
			if (bfsErr == nil) != (idsErr == nil) {
				t.Fatalf("BFS err=%v but BFSIDs err=%v", bfsErr, idsErr)
			}
			if idsErr == nil {
				t.Fatal("BFSIDs accepted a missing origin")
			}
		})
	}
}

// Deleting nodes mid-graph leaves tombstones on the disk backend; traversal must
// agree with the reference walk across that layer boundary too.
func TestTraversal_AgreesAfterDeletes(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := buildTraversalFixture(t, g, 50)

			rng := rand.New(rand.NewSource(11))
			for i := 0; i < 10; i++ {
				victim := ids[1+rng.Intn(len(ids)-2)]
				if _, err := g.GetNode(victim); err != nil {
					continue
				}
				if err := g.DeleteNode(victim); err != nil {
					t.Fatalf("DeleteNode: %v", err)
				}
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes: %v", err)
			}

			for _, dir := range []store.Direction{store.DirectionOutbound, store.DirectionBoth} {
				wantNodes, wantEdges := referenceBFS(t, g, ids[0], 5, dir, nil)

				got, err := g.BFS(ids[0], 5, dir, nil)
				if err != nil {
					t.Fatalf("BFS: %v", err)
				}
				gotNodes := graphene.NodeIDsFromBFS(got)
				gotEdges := edgeIDsOf(got)
				if !reflect.DeepEqual(gotNodes, wantNodes) {
					t.Fatalf("dir=%v BFS nodes after deletes\n got: %v\nwant: %v", dir, gotNodes, wantNodes)
				}
				if !reflect.DeepEqual(gotEdges, wantEdges) {
					t.Fatalf("dir=%v BFS edges after deletes\n got: %v\nwant: %v", dir, gotEdges, wantEdges)
				}

				ids2, err := g.BFSIDs(ids[0], 5, dir, nil)
				if err != nil {
					t.Fatalf("BFSIDs: %v", err)
				}
				if !reflect.DeepEqual(ids2, wantNodes) {
					t.Fatalf("dir=%v BFSIDs after deletes\n got: %v\nwant: %v", dir, ids2, wantNodes)
				}
			}
		})
	}
}

// ShortestPath now carries edge IDs through the frontier and materialises edges
// only for the final path. The path itself must be unchanged and well-formed.
func TestShortestPath_PathIsWellFormed(t *testing.T) {
	for name, open := range traversalBackends() {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				ids := buildTraversalFixture(t, g, 40)
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				res, err := g.ShortestPath(ids[0], ids[len(ids)-1], nil)
				if err != nil {
					t.Fatalf("ShortestPath: %v", err)
				}
				if len(res.Nodes) < 2 {
					t.Fatalf("path too short: %d nodes", len(res.Nodes))
				}
				if len(res.Edges) != len(res.Nodes)-1 {
					t.Fatalf("path has %d nodes but %d edges", len(res.Nodes), len(res.Edges))
				}
				if res.Nodes[0].ID != ids[0] {
					t.Fatalf("path starts at %d, want %d", res.Nodes[0].ID, ids[0])
				}
				if res.Nodes[len(res.Nodes)-1].ID != ids[len(ids)-1] {
					t.Fatalf("path ends at %d, want %d", res.Nodes[len(res.Nodes)-1].ID, ids[len(ids)-1])
				}
				// Every consecutive node pair must be joined by the edge at that index.
				for i, e := range res.Edges {
					a, b := res.Nodes[i].ID, res.Nodes[i+1].ID
					if !(e.Src == a && e.Dst == b) && !(e.Src == b && e.Dst == a) {
						t.Fatalf("edge %d (%d→%d) does not join nodes %d and %d", i, e.Src, e.Dst, a, b)
					}
				}
			})
		}
	}
}

// The traversals must not allocate per visited node. This asserts the property
// directly rather than relying on a benchmark being read by a human.
func TestTraversal_AllocationsDoNotScaleWithGraphSize(t *testing.T) {
	measure := func(n int) (bfsIDs float64) {
		g := graphene.NewInMemory()
		ids := make([]store.NodeID, 0, n)
		for i := 0; i < n; i++ {
			id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			ids = append(ids, id)
		}
		for i := 0; i < n-1; i++ {
			g.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{store.EdgeTypeContains}})
		}
		return testing.AllocsPerRun(20, func() {
			if _, err := g.BFSIDs(ids[0], n, store.DirectionOutbound, nil); err != nil {
				t.Fatalf("BFSIDs: %v", err)
			}
		})
	}

	small := measure(200)
	large := measure(2000)

	// A 10x larger graph must not cost 10x the allocations. Both the visited map
	// and the result slice still grow with the result size, so allow generous
	// headroom — the point is that it is not linear in edges traversed.
	if large > small*4 {
		t.Fatalf("BFSIDs allocations scale with graph size: %.0f allocs at n=200, %.0f at n=2000", small, large)
	}
	t.Logf("BFSIDs allocs/op: n=200 -> %.0f, n=2000 -> %.0f", small, large)
}
