package traversal

import (
	"graphene/store"
)

// BFSResult holds the subgraph discovered by a BFS walk.
type BFSResult struct {
	// Nodes contains every node (including the origin) found within k hops.
	Nodes []*store.Node
	// Edges contains every edge traversed during the walk.
	Edges []*store.Edge
}

// BFS performs a breadth-first traversal from origin up to maxDepth hops.
// Only edges matching edgeTypes are followed (pass nil to follow all types).
// dir controls whether outbound, inbound, or both edge directions are followed.
//
// The returned BFSResult includes the origin node and all nodes/edges within
// the requested depth.
func BFS(g store.GraphStore, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*BFSResult, error) {
	if maxDepth < 0 {
		maxDepth = 0
	}

	visited := make(map[store.NodeID]struct{})
	seenEdges := make(map[store.EdgeID]struct{})

	result := &BFSResult{}

	originNode, err := g.GetNode(origin)
	if err != nil {
		return nil, err
	}

	result.Nodes = append(result.Nodes, originNode)
	visited[origin] = struct{}{}

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

		neighbours, err := g.Neighbours(cur.id, dir, edgeTypes)
		if err != nil {
			return nil, err
		}

		for _, nb := range neighbours {
			if _, edgeSeen := seenEdges[nb.Edge.ID]; !edgeSeen {
				seenEdges[nb.Edge.ID] = struct{}{}
				result.Edges = append(result.Edges, nb.Edge)
			}

			if _, nodeSeen := visited[nb.Node.ID]; nodeSeen {
				continue
			}

			visited[nb.Node.ID] = struct{}{}
			result.Nodes = append(result.Nodes, nb.Node)
			queue = append(queue, qitem{id: nb.Node.ID, depth: cur.depth + 1})
		}
	}

	return result, nil
}
