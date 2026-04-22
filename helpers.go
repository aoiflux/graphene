package graphene

import (
	"errors"
	"graphene/store"
	"graphene/traversal"
)

// GraphStats holds high-level statistics about the graph.
type GraphStats struct {
	NodeCount uint64
	EdgeCount uint64
}

// Stats returns high-level counts for the graph.
func (g *Graph) Stats() (*GraphStats, error) {
	nc, err := g.NodeCount()
	if err != nil {
		return nil, err
	}
	ec, err := g.EdgeCount()
	if err != nil {
		return nil, err
	}
	return &GraphStats{NodeCount: nc, EdgeCount: ec}, nil
}

// --- Batch reads ---

// GetNodes fetches multiple nodes by ID in the order given.
// If any ID is not found the error is returned immediately.
func (g *Graph) GetNodes(ids []store.NodeID) ([]*store.Node, error) {
	out := make([]*store.Node, len(ids))
	for i, id := range ids {
		n, err := g.GetNode(id)
		if err != nil {
			return nil, err
		}
		out[i] = n
	}
	return out, nil
}

// GetEdges fetches multiple edges by ID in the order given.
// If any ID is not found the error is returned immediately.
func (g *Graph) GetEdges(ids []store.EdgeID) ([]*store.Edge, error) {
	out := make([]*store.Edge, len(ids))
	for i, id := range ids {
		e, err := g.GetEdge(id)
		if err != nil {
			return nil, err
		}
		out[i] = e
	}
	return out, nil
}

// --- Batch writes ---

// AddNodes adds multiple nodes in order, returning their assigned IDs.
// If any node fails to be added, the already-added nodes are not rolled back.
func (g *Graph) AddNodes(nodes []*store.Node) ([]store.NodeID, error) {
	ids := make([]store.NodeID, len(nodes))
	for i, n := range nodes {
		id, err := g.AddNode(n)
		if err != nil {
			return ids[:i], err
		}
		ids[i] = id
	}
	return ids, nil
}

// AddEdges adds multiple edges in order, returning their assigned IDs.
// If any edge fails to be added, the already-added edges are not rolled back.
func (g *Graph) AddEdges(edges []*store.Edge) ([]store.EdgeID, error) {
	ids := make([]store.EdgeID, len(edges))
	for i, e := range edges {
		id, err := g.AddEdge(e)
		if err != nil {
			return ids[:i], err
		}
		ids[i] = id
	}
	return ids, nil
}

// --- Bulk property indexing ---

// IndexNodeProperties indexes all key-value pairs in props for the given node.
// Indexing stops and the error is returned on first failure.
func (g *Graph) IndexNodeProperties(id store.NodeID, props map[string][]byte) error {
	for k, v := range props {
		if err := g.IndexNodeProperty(id, k, v); err != nil {
			return err
		}
	}
	return nil
}

// IndexEdgeProperties indexes all key-value pairs in props for the given edge.
// Indexing stops and the error is returned on first failure.
func (g *Graph) IndexEdgeProperties(id store.EdgeID, props map[string][]byte) error {
	for k, v := range props {
		if err := g.IndexEdgeProperty(id, k, v); err != nil {
			return err
		}
	}
	return nil
}

// --- Multi-key property queries ---

// NodesByProperties returns the intersection of all NodeIDs that match every
// key-value pair in props (AND semantics). Returns an empty slice when props is empty.
func (g *Graph) NodesByProperties(props map[string][]byte) ([]store.NodeID, error) {
	var result []store.NodeID
	first := true
	for k, v := range props {
		hits, err := g.NodesByProperty(k, v)
		if err != nil {
			return nil, err
		}
		if first {
			result = hits
			first = false
			continue
		}
		result = intersectNodeIDs(result, hits)
		if len(result) == 0 {
			return nil, nil
		}
	}
	return result, nil
}

// EdgesByProperties returns the intersection of all EdgeIDs that match every
// key-value pair in props (AND semantics). Returns an empty slice when props is empty.
func (g *Graph) EdgesByProperties(props map[string][]byte) ([]store.EdgeID, error) {
	var result []store.EdgeID
	first := true
	for k, v := range props {
		hits, err := g.EdgesByProperty(k, v)
		if err != nil {
			return nil, err
		}
		if first {
			result = hits
			first = false
			continue
		}
		result = intersectEdgeIDs(result, hits)
		if len(result) == 0 {
			return nil, nil
		}
	}
	return result, nil
}

// --- Multi-type queries ---

// NodesByAnyType returns all NodeIDs that carry at least one of the given labels
// (OR semantics). Duplicate IDs are deduplicated.
func (g *Graph) NodesByAnyType(types []store.NodeType) ([]store.NodeID, error) {
	seen := make(map[store.NodeID]struct{})
	var out []store.NodeID
	for _, t := range types {
		ids, err := g.NodesByType(t)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	return out, nil
}

// EdgesByAnyType returns all EdgeIDs that carry at least one of the given labels
// (OR semantics). Duplicate IDs are deduplicated.
func (g *Graph) EdgesByAnyType(types []store.EdgeType) ([]store.EdgeID, error) {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	for _, t := range types {
		ids, err := g.EdgesByType(t)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	return out, nil
}

// --- Degree helpers ---

// InDegree returns the number of inbound edges for node id.
// Pass nil edgeTypes to count all inbound edges.
func (g *Graph) InDegree(id store.NodeID, edgeTypes []store.EdgeType) (int, error) {
	edges, err := g.EdgesOf(id, store.DirectionInbound, edgeTypes)
	if err != nil {
		return 0, err
	}
	return len(edges), nil
}

// OutDegree returns the number of outbound edges for node id.
// Pass nil edgeTypes to count all outbound edges.
func (g *Graph) OutDegree(id store.NodeID, edgeTypes []store.EdgeType) (int, error) {
	edges, err := g.EdgesOf(id, store.DirectionOutbound, edgeTypes)
	if err != nil {
		return 0, err
	}
	return len(edges), nil
}

// Degree returns the total (in + out) edge count for node id.
// Pass nil edgeTypes to count all edges. Note that for undirected use-cases,
// edges that appear in both directions are counted twice.
func (g *Graph) Degree(id store.NodeID, edgeTypes []store.EdgeType) (int, error) {
	edges, err := g.EdgesOf(id, store.DirectionBoth, edgeTypes)
	if err != nil {
		return 0, err
	}
	return len(edges), nil
}

// --- Connectivity helpers ---

// EdgeExists reports whether at least one direct edge exists from src to dst.
// Pass nil edgeTypes to consider edges of any type.
func (g *Graph) EdgeExists(src, dst store.NodeID, edgeTypes []store.EdgeType) (bool, error) {
	edges, err := g.EdgesOf(src, store.DirectionOutbound, edgeTypes)
	if err != nil {
		return false, err
	}
	for _, e := range edges {
		if e.Dst == dst {
			return true, nil
		}
	}
	return false, nil
}

// IsConnected reports whether src and dst are reachable from one another via
// any sequence of edges. It uses the shortest-path algorithm internally and
// considers all edge types.
func (g *Graph) IsConnected(src, dst store.NodeID) (bool, error) {
	result, err := g.ShortestPath(src, dst, nil)
	if err != nil {
		if errors.Is(err, traversal.ErrNoPath) {
			return false, nil
		}
		var notFound *store.ErrNotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	return len(result.Nodes) > 0, nil
}

// --- Neighbour helpers ---

// NeighboursByNodeType returns all directly connected nodes of a specific
// NodeType, optionally filtered by edge types.
// Pass nil edgeTypes to follow all edge types.
func (g *Graph) NeighboursByNodeType(id store.NodeID, dir store.Direction, nodeType store.NodeType, edgeTypes []store.EdgeType) ([]*store.Node, error) {
	neighbours, err := g.Neighbours(id, dir, edgeTypes)
	if err != nil {
		return nil, err
	}
	var out []*store.Node
	for _, nb := range neighbours {
		if nb.Node.HasLabel(nodeType) {
			out = append(out, nb.Node)
		}
	}
	return out, nil
}

// --- Subgraph extraction ---

// InducedSubgraph returns the nodes and all edges between them for the given
// set of node IDs. The result edges are those whose Src AND Dst are both in
// the provided set.
func (g *Graph) InducedSubgraph(nodeIDs []store.NodeID) ([]*store.Node, []*store.Edge, error) {
	inSet := make(map[store.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		inSet[id] = struct{}{}
	}

	nodes := make([]*store.Node, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		n, err := g.GetNode(id)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, n)
	}

	seen := make(map[store.EdgeID]struct{})
	var edges []*store.Edge
	for _, id := range nodeIDs {
		outEdges, err := g.EdgesOf(id, store.DirectionOutbound, nil)
		if err != nil {
			return nil, nil, err
		}
		for _, e := range outEdges {
			if _, ok := seen[e.ID]; ok {
				continue
			}
			if _, dstIn := inSet[e.Dst]; dstIn {
				seen[e.ID] = struct{}{}
				edges = append(edges, e)
			}
		}
	}
	return nodes, edges, nil
}

// --- Cycle detection ---

// HasCycle reports whether any cycle is reachable from origin within maxDepth
// hops following outbound edges. It uses DFS and detects back-edges in the
// recursion stack. Pass nil edgeTypes to follow all edge types.
func (g *Graph) HasCycle(origin store.NodeID, maxDepth int, edgeTypes []store.EdgeType) (bool, error) {
	visited := make(map[store.NodeID]bool) // true = on current stack
	found := false

	var dfs func(id store.NodeID, depth int) error
	dfs = func(id store.NodeID, depth int) error {
		if found || depth > maxDepth {
			return nil
		}
		if onStack, seen := visited[id]; seen {
			if onStack {
				found = true
			}
			return nil
		}
		visited[id] = true
		neighbours, err := g.Neighbours(id, store.DirectionOutbound, edgeTypes)
		if err != nil {
			return err
		}
		for _, nb := range neighbours {
			if err := dfs(nb.Node.ID, depth+1); err != nil {
				return err
			}
			if found {
				return nil
			}
		}
		visited[id] = false // pop from stack
		return nil
	}

	return found, dfs(origin, 0)
}

// --- Result helpers ---

// NodesFromBFS returns the slice of nodes from a BFS result. Nil-safe.
func NodesFromBFS(r *traversal.BFSResult) []*store.Node {
	if r == nil {
		return nil
	}
	return r.Nodes
}

// EdgesFromBFS returns the slice of edges from a BFS result. Nil-safe.
func EdgesFromBFS(r *traversal.BFSResult) []*store.Edge {
	if r == nil {
		return nil
	}
	return r.Edges
}

// NodeIDsFromBFS returns the node IDs from a BFS result for use as scope in
// follow-up queries (e.g. FindPatterns).
func NodeIDsFromBFS(r *traversal.BFSResult) []store.NodeID {
	if r == nil {
		return nil
	}
	ids := make([]store.NodeID, len(r.Nodes))
	for i, n := range r.Nodes {
		ids[i] = n.ID
	}
	return ids
}

// NodeIDsFromPath returns the ordered node IDs from a PathResult.
func NodeIDsFromPath(r *traversal.PathResult) []store.NodeID {
	if r == nil {
		return nil
	}
	ids := make([]store.NodeID, len(r.Nodes))
	for i, n := range r.Nodes {
		ids[i] = n.ID
	}
	return ids
}

// FilterNodesByLabel returns only the nodes from ns that carry the given label.
func FilterNodesByLabel(ns []*store.Node, label store.NodeType) []*store.Node {
	var out []*store.Node
	for _, n := range ns {
		if n.HasLabel(label) {
			out = append(out, n)
		}
	}
	return out
}

// FilterEdgesByLabel returns only the edges from es that carry the given label.
func FilterEdgesByLabel(es []*store.Edge, label store.EdgeType) []*store.Edge {
	var out []*store.Edge
	for _, e := range es {
		if e.HasLabel(label) {
			out = append(out, e)
		}
	}
	return out
}

// --- Internal helpers ---

func intersectNodeIDs(a, b []store.NodeID) []store.NodeID {
	set := make(map[store.NodeID]struct{}, len(b))
	for _, id := range b {
		set[id] = struct{}{}
	}
	var out []store.NodeID
	for _, id := range a {
		if _, ok := set[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

func intersectEdgeIDs(a, b []store.EdgeID) []store.EdgeID {
	set := make(map[store.EdgeID]struct{}, len(b))
	for _, id := range b {
		set[id] = struct{}{}
	}
	var out []store.EdgeID
	for _, id := range a {
		if _, ok := set[id]; ok {
			out = append(out, id)
		}
	}
	return out
}
