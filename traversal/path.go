package traversal

import (
	"context"

	"errors"

	"github.com/aoiflux/graphene/store"
)

// ErrNoPath is returned when no path exists between the source and destination.
var ErrNoPath = errors.New("no path between nodes")

// PathResult holds a single shortest path between two nodes.
type PathResult struct {
	// Nodes is the ordered sequence from Src to Dst inclusive.
	Nodes []*store.Node
	// Edges is the ordered sequence of edges traversed from Src to Dst.
	Edges []*store.Edge
}

// visitEntry records how a node was discovered in the BFS frontier.
//
// It holds the connecting edge's ID rather than the edge itself: a bidirectional
// search touches far more nodes than end up on the path, and materialising an
// edge record for each one is wasted work. reconstructPath fetches only the
// edges the final path actually uses.
type visitEntry struct {
	parent store.NodeID
	edge   store.EdgeID
}

// ShortestPath finds the shortest path between src and dst using bidirectional
// BFS. Bidirectional BFS meets in the middle, making it significantly faster
// than single-direction BFS on large, dense graphs.
//
// Pass nil edgeTypes to traverse all edge types.
// The search treats the graph as undirected (DirectionBoth) for path finding.
func ShortestPath(g store.GraphReader, src, dst store.NodeID, edgeTypes []store.EdgeType) (*PathResult, error) {
	return ShortestPathCtx(context.Background(), g, src, dst, edgeTypes, store.Budget{})
}

// ShortestPathCtx is ShortestPath with a cancellable context and a budget.
// See BFSCtx.
//
// Both frontiers charge against one budget, which is the right accounting: the
// cost of a bidirectional search is what the two halves spend together, and
// splitting the allowance between them would let a search that has exhausted
// its budget on one side keep going on the other.
func ShortestPathCtx(ctx context.Context, g store.GraphReader, src, dst store.NodeID, edgeTypes []store.EdgeType, budget store.Budget) (*PathResult, error) {
	guard := newGuard(ctx, budget)
	if err := guard.enter(); err != nil {
		return nil, err
	}

	if src == dst {
		node, err := g.GetNode(src)
		if err != nil {
			return nil, err
		}
		return &PathResult{Nodes: []*store.Node{node}}, nil
	}

	w := newWalker(g)

	fwdVisited := map[store.NodeID]visitEntry{src: {parent: store.InvalidNodeID}}
	bwdVisited := map[store.NodeID]visitEntry{dst: {parent: store.InvalidNodeID}}

	fwdFrontier := []store.NodeID{src}
	bwdFrontier := []store.NodeID{dst}

	// Two buffers per direction, swapped at every level, rather than a fresh
	// slice grown from nil per level — the same shape BFSCtx uses and for the
	// same reason. expandAndAdvance appends into the spare and returns it; the
	// frontier it just consumed becomes the next spare, so after the widest
	// level neither buffer allocates again. Growing from nil instead made this
	// the single largest allocation site in the engine: 590k of the 789k
	// allocations in BenchmarkShortestPath, 75% of the whole benchmark.
	//
	// The two directions keep separate spares. Sharing one would alias the
	// frontier the other half is still reading.
	var fwdSpare, bwdSpare []store.NodeID

	meetNode := store.InvalidNodeID

	for len(fwdFrontier) > 0 && len(bwdFrontier) > 0 {
		var nextFwd []store.NodeID
		var err error
		meetNode, nextFwd, err = expandAndAdvance(w, &guard, fwdSpare[:0], fwdFrontier, fwdVisited, bwdVisited, edgeTypes)
		if err != nil {
			return nil, err
		}
		if meetNode != store.InvalidNodeID {
			break
		}
		fwdFrontier, fwdSpare = nextFwd, fwdFrontier

		var nextBwd []store.NodeID
		meetNode, nextBwd, err = expandAndAdvance(w, &guard, bwdSpare[:0], bwdFrontier, bwdVisited, fwdVisited, edgeTypes)
		if err != nil {
			return nil, err
		}
		if meetNode != store.InvalidNodeID {
			break
		}
		bwdFrontier, bwdSpare = nextBwd, bwdFrontier
	}

	if meetNode == store.InvalidNodeID {
		return nil, ErrNoPath
	}

	return reconstructPath(g, meetNode, fwdVisited, bwdVisited)
}

// expandAndAdvance advances the BFS frontier by one level, records newly-visited
// nodes in myVisited (with parent/edge provenance), and checks for intersection
// with otherVisited. Returns the meeting node (or InvalidNodeID) and the next frontier.
//
// next is the caller's buffer, already truncated to zero length, and must not
// alias frontier — this level's output is written while this level's input is
// still being read. The returned slice is the caller's to keep; see the swap in
// ShortestPathCtx.
func expandAndAdvance(
	w *walker,
	guard *guard,
	next []store.NodeID,
	frontier []store.NodeID,
	myVisited map[store.NodeID]visitEntry,
	otherVisited map[store.NodeID]visitEntry,
	edgeTypes []store.EdgeType,
) (store.NodeID, []store.NodeID, error) {
	for _, id := range frontier {
		if err := guard.step(); err != nil {
			return store.InvalidNodeID, nil, err
		}
		incident, err := w.incidentEdges(id, store.DirectionBoth, edgeTypes)
		if err != nil {
			continue
		}
		w.beginExpansion()
		for _, step := range incident {
			eid, nbID := step.Edge, step.Neighbour
			// One visit per distinct neighbour, matching Neighbours.
			if !w.markNeighbour(nbID) {
				continue
			}
			if err := guard.crossEdge(); err != nil {
				return store.InvalidNodeID, nil, err
			}
			if !w.nodeExists(nbID) {
				continue
			}
			if _, seen := myVisited[nbID]; !seen {
				if err := guard.visitNode(); err != nil {
					return store.InvalidNodeID, nil, err
				}
				myVisited[nbID] = visitEntry{parent: id, edge: eid}
				next = append(next, nbID)
			}
			if _, inOther := otherVisited[nbID]; inOther {
				return nbID, next, nil
			}
		}
	}
	return store.InvalidNodeID, next, nil
}

// reconstructPath assembles src→meet (fwdVisited) then meet→dst (bwdVisited).
func reconstructPath(
	g store.GraphReader,
	meet store.NodeID,
	fwd, bwd map[store.NodeID]visitEntry,
) (*PathResult, error) {
	// Both chains are walked twice: once to count, once to fill. A parent-chain
	// walk is map lookups over a path that is short by construction — the search
	// stopped the moment the two frontiers met — so counting costs far less than
	// the growth it removes. This assembled six slices by appending to nil and
	// then concatenated two of them; it now allocates each exactly once.
	//
	// The counting loops mirror the filling loops condition for condition, which
	// is the point: a count derived some other way could disagree with the walk,
	// and the failure would be a silently truncated path rather than an error.
	fwdN, fwdE := fwdChainLen(fwd, meet)
	bwdN := bwdChainLen(bwd, meet)

	allIDs := make([]store.NodeID, fwdN, fwdN+bwdN)
	allEdges := make([]store.EdgeID, fwdE, fwdE+bwdN)

	// Forward half: meet back to src, written back-to-front so the result is in
	// src→meet order without a separate reversing pass.
	i, j := fwdN, fwdE
	for cur := meet; cur != store.InvalidNodeID; {
		i--
		allIDs[i] = cur
		v := fwd[cur]
		if v.edge != store.InvalidEdgeID {
			j--
			allEdges[j] = v.edge
		}
		cur = v.parent
	}

	// Backward half: meet to dst, already in order, skipping meet itself.
	for cur := meet; cur != store.InvalidNodeID; {
		v := bwd[cur]
		if v.edge != store.InvalidEdgeID {
			allEdges = append(allEdges, v.edge)
			allIDs = append(allIDs, v.parent)
		}
		cur = v.parent
	}

	return materialisePath(g, allIDs, allEdges)
}

// materialisePath turns a path of IDs into a path of records.
//
// Shared by the unweighted and weighted searches, which agree on exactly this
// much: both spend their time on IDs and both fetch records only for the path
// they ended up with — not for the far larger set of nodes they had to touch
// to find it. Everything before this point differs between them.
func materialisePath(g store.GraphReader, ids []store.NodeID, edges []store.EdgeID) (*PathResult, error) {
	result := &PathResult{
		Nodes: make([]*store.Node, 0, len(ids)),
		Edges: make([]*store.Edge, 0, len(edges)),
	}
	for _, id := range ids {
		n, err := g.GetNode(id)
		if err != nil {
			return nil, err
		}
		result.Nodes = append(result.Nodes, n)
	}
	for _, eid := range edges {
		e, err := g.GetEdge(eid)
		if err != nil {
			return nil, err
		}
		result.Edges = append(result.Edges, e)
	}
	return result, nil
}

// fwdChainLen counts what reconstructPath's forward walk will emit. Its loop is
// that walk's loop with the appends removed — deliberately, so the two cannot
// drift into disagreeing about the length of the same chain.
func fwdChainLen(m map[store.NodeID]visitEntry, meet store.NodeID) (nodes, edges int) {
	for cur := meet; cur != store.InvalidNodeID; {
		nodes++
		v := m[cur]
		if v.edge != store.InvalidEdgeID {
			edges++
		}
		cur = v.parent
	}
	return nodes, edges
}

// bwdChainLen is fwdChainLen for the backward walk, which contributes a node
// only where it contributes an edge — so one count serves for both.
func bwdChainLen(m map[store.NodeID]visitEntry, meet store.NodeID) int {
	n := 0
	for cur := meet; cur != store.InvalidNodeID; {
		v := m[cur]
		if v.edge != store.InvalidEdgeID {
			n++
		}
		cur = v.parent
	}
	return n
}
