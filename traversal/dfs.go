package traversal

import (
	"graphene/store"
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
// the given direction, up to maxDepth hops. It returns all reachable nodes and
// edges in DFS discovery order.
//
// Pass nil edgeTypes to follow all edge types.
func DFS(g store.GraphStore, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*BFSResult, error) {
	visited := make(map[store.NodeID]struct{})
	seenEdges := make(map[store.EdgeID]struct{})
	result := &BFSResult{}

	if err := dfsGeneral(g, origin, maxDepth, dir, edgeTypes, visited, seenEdges, result); err != nil {
		return nil, err
	}
	return result, nil
}

func dfsGeneral(
	g store.GraphStore,
	id store.NodeID,
	remaining int,
	dir store.Direction,
	edgeTypes []store.EdgeType,
	visited map[store.NodeID]struct{},
	seenEdges map[store.EdgeID]struct{},
	result *BFSResult,
) error {
	if _, seen := visited[id]; seen {
		return nil
	}
	visited[id] = struct{}{}

	node, err := g.GetNode(id)
	if err != nil {
		return err
	}
	result.Nodes = append(result.Nodes, node)

	if remaining == 0 {
		return nil
	}

	neighbours, err := g.Neighbours(id, dir, edgeTypes)
	if err != nil {
		return err
	}

	for _, nb := range neighbours {
		if _, edgeSeen := seenEdges[nb.Edge.ID]; !edgeSeen {
			seenEdges[nb.Edge.ID] = struct{}{}
			result.Edges = append(result.Edges, nb.Edge)
		}
		if err := dfsGeneral(g, nb.Node.ID, remaining-1, dir, edgeTypes, visited, seenEdges, result); err != nil {
			return err
		}
	}
	return nil
}
