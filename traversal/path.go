package traversal

import (
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
func ShortestPath(g store.GraphStore, src, dst store.NodeID, edgeTypes []store.EdgeType) (*PathResult, error) {
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

	meetNode := store.InvalidNodeID

	for len(fwdFrontier) > 0 && len(bwdFrontier) > 0 {
		var nextFwd []store.NodeID
		meetNode, nextFwd = expandAndAdvance(w, fwdFrontier, fwdVisited, bwdVisited, edgeTypes)
		if meetNode != store.InvalidNodeID {
			break
		}
		fwdFrontier = nextFwd

		var nextBwd []store.NodeID
		meetNode, nextBwd = expandAndAdvance(w, bwdFrontier, bwdVisited, fwdVisited, edgeTypes)
		if meetNode != store.InvalidNodeID {
			break
		}
		bwdFrontier = nextBwd
	}

	if meetNode == store.InvalidNodeID {
		return nil, ErrNoPath
	}

	return reconstructPath(g, meetNode, fwdVisited, bwdVisited)
}

// expandAndAdvance advances the BFS frontier by one level, records newly-visited
// nodes in myVisited (with parent/edge provenance), and checks for intersection
// with otherVisited. Returns the meeting node (or InvalidNodeID) and the next frontier.
func expandAndAdvance(
	w *walker,
	frontier []store.NodeID,
	myVisited map[store.NodeID]visitEntry,
	otherVisited map[store.NodeID]visitEntry,
	edgeTypes []store.EdgeType,
) (store.NodeID, []store.NodeID) {
	var next []store.NodeID
	for _, id := range frontier {
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
			if !w.nodeExists(nbID) {
				continue
			}
			if _, seen := myVisited[nbID]; !seen {
				myVisited[nbID] = visitEntry{parent: id, edge: eid}
				next = append(next, nbID)
			}
			if _, inOther := otherVisited[nbID]; inOther {
				return nbID, next
			}
		}
	}
	return store.InvalidNodeID, next
}

// reconstructPath assembles src→meet (fwdVisited) then meet→dst (bwdVisited).
func reconstructPath(
	g store.GraphStore,
	meet store.NodeID,
	fwd, bwd map[store.NodeID]visitEntry,
) (*PathResult, error) {
	// Walk fwd from meet back to src, then reverse.
	var fwdIDs []store.NodeID
	var fwdEdges []store.EdgeID
	for cur := meet; cur != store.InvalidNodeID; {
		fwdIDs = append(fwdIDs, cur)
		v := fwd[cur]
		if v.edge != store.InvalidEdgeID {
			fwdEdges = append(fwdEdges, v.edge)
		}
		cur = v.parent
	}
	reverseNodeIDs(fwdIDs)
	reverseEdgeIDs(fwdEdges)

	// Walk bwd from meet to dst (skip meet itself).
	var bwdIDs []store.NodeID
	var bwdEdges []store.EdgeID
	for cur := meet; cur != store.InvalidNodeID; {
		v := bwd[cur]
		if v.edge != store.InvalidEdgeID {
			bwdEdges = append(bwdEdges, v.edge)
			bwdIDs = append(bwdIDs, v.parent)
		}
		cur = v.parent
	}

	allIDs := append(fwdIDs, bwdIDs...)
	allEdges := append(fwdEdges, bwdEdges...)

	// Records are materialised only now, for the path itself — not for the far
	// larger set of nodes the bidirectional search had to touch to find it.
	result := &PathResult{}
	for _, id := range allIDs {
		n, err := g.GetNode(id)
		if err != nil {
			return nil, err
		}
		result.Nodes = append(result.Nodes, n)
	}
	for _, eid := range allEdges {
		e, err := g.GetEdge(eid)
		if err != nil {
			return nil, err
		}
		result.Edges = append(result.Edges, e)
	}
	return result, nil
}

func reverseNodeIDs(s []store.NodeID) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func reverseEdgeIDs(s []store.EdgeID) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
