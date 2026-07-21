package traversal

import (
	"github.com/aoiflux/graphene/store"
)

// DFSResult holds the provenance chain discovered by a DFS walk.
type DFSResult struct {
	// Chain is the ordered sequence of nodes from origin back to the root
	// (e.g. the EvidenceFile). If no root is reached within maxDepth, Chain
	// contains the deepest path found.
	Chain []*store.Node
	// Edges contains the edges in the same traversal order as Chain.
	Edges []*store.Edge
}

// ProvenanceChain walks backwards from origin following inbound edges of the
// given edgeTypes (typically EdgeTypeContains and/or EdgeTypeReuse) until it
// reaches a node with no further inbound edges of those types, or until
// maxDepth is exceeded.
//
// Pass nil edgeTypes to follow all inbound edge types.
// The returned chain is ordered from origin (index 0) to root (last index).
func ProvenanceChain(g store.GraphStore, origin store.NodeID, maxDepth int, edgeTypes []store.EdgeType) (*DFSResult, error) {
	if maxDepth <= 0 {
		maxDepth = 64 // safe default for provenance depth
	}

	result := &DFSResult{}
	visited := make(map[store.NodeID]struct{})

	if err := dfsInbound(g, origin, maxDepth, edgeTypes, visited, result); err != nil {
		return nil, err
	}
	return result, nil
}

// dfsInbound is the recursive DFS kernel. It appends to result as it discovers
// the provenance path.
func dfsInbound(
	g store.GraphStore,
	id store.NodeID,
	remaining int,
	edgeTypes []store.EdgeType,
	visited map[store.NodeID]struct{},
	result *DFSResult,
) error {
	if _, seen := visited[id]; seen {
		return nil
	}
	visited[id] = struct{}{}

	node, err := g.GetNode(id)
	if err != nil {
		return err
	}
	result.Chain = append(result.Chain, node)

	if remaining == 0 {
		return nil
	}

	edges, err := g.EdgesOf(id, store.DirectionInbound, edgeTypes)
	if err != nil {
		return err
	}

	// follow the first unvisited inbound parent (provenance is typically a
	// chain, not a DAG; for DAG provenance, callers should use BFS instead)
	for _, e := range edges {
		parentID := e.Src
		if _, seen := visited[parentID]; seen {
			continue
		}
		result.Edges = append(result.Edges, e)
		return dfsInbound(g, parentID, remaining-1, edgeTypes, visited, result)
	}
	return nil
}

// DFS performs a general depth-first traversal from origin, following edges in
// the given direction, up to maxDepth hops. It returns every node and edge
// within maxDepth hops of origin, in DFS discovery order.
//
// The node set is identical to what BFS returns for the same arguments; only the
// order differs. Use BFS when you want level order, DFS when you want a
// depth-first walk, and BFSIDs when you only need the reachable IDs.
//
// Pass nil edgeTypes to follow all edge types.
func DFS(g store.GraphStore, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*BFSResult, error) {
	if maxDepth < 0 {
		maxDepth = 0
	}
	// bestRemaining records the largest remaining budget each node has been
	// expanded with — not merely whether it has been seen. See dfsGeneral.
	bestRemaining := make(map[store.NodeID]int)
	seenEdges := make(map[store.EdgeID]struct{})
	result := &BFSResult{}

	if err := dfsGeneral(newWalker(g), origin, maxDepth, dir, edgeTypes, bestRemaining, seenEdges, result); err != nil {
		return nil, err
	}
	return result, nil
}

// dfsGeneral is the recursive kernel.
//
// # Why bestRemaining is a depth, not a visited flag
//
// A boolean visited set is wrong for a *depth-limited* walk. Depth-first order
// means a node is often first reached by a long path, arriving with little
// budget left; marking it visited then blocks the shorter path that would have
// arrived with budget to spare, and everything beyond it is never explored.
//
//	A → B → C → E   plus a shortcut   A → C
//
// At maxDepth 2 the walk reaches C via A→B→C with remaining=0 and stops. The
// shortcut A→C would arrive with remaining=1 — enough to reach E — but a visited
// flag would skip it, and E would be missed even though it is two hops away.
// BFS does not have this problem because it always reaches a node by its
// shortest path first.
//
// Recording the best budget instead lets a node be re-expanded when a later path
// reaches it with more depth to spend. Each node is expanded at most once per
// distinct remaining value, so the walk is bounded by O(V × maxDepth) expansions
// rather than O(V) — the price of a correct answer. Nodes are appended to the
// result only on first arrival, so no duplicates appear.
func dfsGeneral(
	w *walker,
	id store.NodeID,
	remaining int,
	dir store.Direction,
	edgeTypes []store.EdgeType,
	bestRemaining map[store.NodeID]int,
	seenEdges map[store.EdgeID]struct{},
	result *BFSResult,
) error {
	prev, seen := bestRemaining[id]
	if seen && prev >= remaining {
		// Already expanded with at least this much budget; nothing new to find.
		return nil
	}
	bestRemaining[id] = remaining

	if !seen {
		node, err := w.g.GetNode(id)
		if err != nil {
			return err
		}
		result.Nodes = append(result.Nodes, node)
	}

	if remaining == 0 {
		return nil
	}

	// DFS recurses, so the walker's shared edge buffer cannot be held across the
	// recursive call — the callee reuses it. Copy this frontier out first. It is
	// the one unavoidable allocation per visited node in a depth-first walk.
	incident, err := w.incidentEdges(id, dir, edgeTypes)
	if err != nil {
		return err
	}
	frontier := make([]store.IncidentEdge, len(incident))
	copy(frontier, incident)

	seenThisLevel := make(map[store.NodeID]struct{}, len(frontier))
	for _, step := range frontier {
		eid, nbID := step.Edge, step.Neighbour
		// One visit per distinct neighbour at this level, matching Neighbours.
		if _, dup := seenThisLevel[nbID]; dup {
			continue
		}
		seenThisLevel[nbID] = struct{}{}

		// Neighbours drops pairs whose node cannot be resolved, edge included.
		if _, alreadySeen := bestRemaining[nbID]; !alreadySeen && !w.nodeExists(nbID) {
			continue
		}

		if _, edgeSeen := seenEdges[eid]; !edgeSeen {
			edge, err := w.g.GetEdge(eid)
			if err != nil {
				continue
			}
			seenEdges[eid] = struct{}{}
			result.Edges = append(result.Edges, edge)
		}
		if err := dfsGeneral(w, nbID, remaining-1, dir, edgeTypes, bestRemaining, seenEdges, result); err != nil {
			return err
		}
	}
	return nil
}
