package traversal

import (
	"github.com/aoiflux/graphene/store"
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
//
// If you only need the reachable node IDs, use BFSIDs: it does the same walk
// without materialising a single node or edge record.
func BFS(g store.GraphStore, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*BFSResult, error) {
	if maxDepth < 0 {
		maxDepth = 0
	}

	w := newWalker(g)

	originNode, err := g.GetNode(origin)
	if err != nil {
		return nil, err
	}

	result := &BFSResult{Nodes: []*store.Node{originNode}}
	visited := map[store.NodeID]struct{}{origin: {}}
	seenEdges := make(map[store.EdgeID]struct{})

	type qitem struct {
		id    store.NodeID
		depth int
	}
	// The queue is consumed with a head index rather than by re-slicing.
	// `queue = queue[1:]` slides the window forward through the backing array, so
	// every append past the original capacity has to reallocate — one allocation
	// per visited node on a long walk.
	queue := []qitem{{id: origin, depth: 0}}
	for head := 0; head < len(queue); head++ {
		cur := queue[head]

		if cur.depth >= maxDepth {
			continue
		}

		incident, err := w.incidentEdges(cur.id, dir, edgeTypes)
		if err != nil {
			return nil, err
		}

		w.beginExpansion()
		for _, step := range incident {
			eid, nbID := step.Edge, step.Neighbour
			// One result per distinct neighbour per expansion, matching Neighbours.
			if !w.markNeighbour(nbID) {
				continue
			}

			// A visited neighbour was already resolved, so its node exists and
			// need not be fetched again. An unvisited one is fetched now, and a
			// node that cannot be resolved drops its edge too — Neighbours omits
			// such pairs entirely, and BFS must not diverge from that.
			_, alreadyVisited := visited[nbID]
			var nbNode *store.Node
			if !alreadyVisited {
				nbNode, err = g.GetNode(nbID)
				if err != nil {
					continue
				}
			}

			if _, edgeSeen := seenEdges[eid]; !edgeSeen {
				edge, err := g.GetEdge(eid)
				if err != nil {
					continue
				}
				seenEdges[eid] = struct{}{}
				result.Edges = append(result.Edges, edge)
			}

			if alreadyVisited {
				continue
			}
			visited[nbID] = struct{}{}
			result.Nodes = append(result.Nodes, nbNode)
			queue = append(queue, qitem{id: nbID, depth: cur.depth + 1})
		}
	}

	return result, nil
}

// BFSIDs performs the same walk as BFS and returns only the reachable node IDs,
// in discovery order, starting with origin.
//
// It never materialises a node or edge record, so its cost is the graph walk
// itself: on a backend implementing store.AdjacencyReader the whole traversal
// allocates only the visited set and the queue, independent of how many edges it
// crosses. Prefer it whenever the records themselves are not needed — reachability
// checks, scoping a pattern match, or feeding IDs into a query.
func BFSIDs(g store.GraphStore, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NodeID, error) {
	if maxDepth < 0 {
		maxDepth = 0
	}

	w := newWalker(g)
	if !w.nodeExists(origin) {
		return nil, &store.ErrNotFound{Kind: "node", ID: uint64(origin)}
	}

	out := []store.NodeID{origin}
	visited := map[store.NodeID]struct{}{origin: {}}

	// Level-synchronous walk: `out` doubles as the queue, so no separate
	// allocation is needed. levelEnd marks where the current depth stops.
	for depth, start, levelEnd := 0, 0, 1; depth < maxDepth && start < levelEnd; depth++ {
		for ; start < levelEnd; start++ {
			incident, err := w.incidentEdges(out[start], dir, edgeTypes)
			if err != nil {
				return nil, err
			}
			for _, step := range incident {
				nbID := step.Neighbour
				if _, seen := visited[nbID]; seen {
					continue
				}
				if !w.nodeExists(nbID) {
					continue
				}
				visited[nbID] = struct{}{}
				out = append(out, nbID)
			}
		}
		levelEnd = len(out)
	}

	return out, nil
}
