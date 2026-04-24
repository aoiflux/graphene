package traversal

import (
	"github.com/aoiflux/graphene/store"
)

// PatternNode describes a node constraint in a query pattern.
// Labels lists the node labels that a data node must ALL carry to match
// (AND semantics). An empty Labels slice matches any node.
type PatternNode struct {
	ID     int // local ID within the pattern (0-based)
	Labels []store.NodeType
}

// PatternEdge describes an edge constraint in a query pattern.
// Labels lists the edge labels that a data edge must ALL carry to match
// (AND semantics). An empty Labels slice matches any edge.
type PatternEdge struct {
	SrcPatternID int
	DstPatternID int
	Labels       []store.EdgeType
}

// Pattern is a small query graph (2–20 nodes) used for subgraph matching.
type Pattern struct {
	Nodes []PatternNode
	Edges []PatternEdge
}

// SubgraphMatch is one successful mapping of pattern nodes to data graph nodes.
// Mapping[patternNodeID] = dataNodeID.
type SubgraphMatch struct {
	Mapping []store.NodeID
}

// FindSubgraphMatches searches the data graph for all subgraphs that match the
// given pattern. It uses a VF2-inspired backtracking algorithm pruned by
// NodeType and EdgeType label constraints.
//
// scope limits the search to a set of candidate node IDs. Pass nil to search
// the entire graph (expensive on billion-node graphs; prefer scoping to a case
// or neighbourhood result).
//
// maxMatches caps results to avoid unbounded output; pass 0 for no cap.
func FindSubgraphMatches(
	g store.GraphStore,
	pattern *Pattern,
	scope []store.NodeID,
	maxMatches int,
) ([]SubgraphMatch, error) {
	if len(pattern.Nodes) == 0 {
		return nil, nil
	}

	// Build candidate lists per pattern node from scope.
	candidates, err := buildCandidates(g, pattern, scope)
	if err != nil {
		return nil, err
	}

	var results []SubgraphMatch
	mapping := make([]store.NodeID, len(pattern.Nodes))
	for i := range mapping {
		mapping[i] = store.InvalidNodeID
	}
	used := make(map[store.NodeID]bool)

	var backtrack func(depth int) error
	backtrack = func(depth int) error {
		if depth == len(pattern.Nodes) {
			// Full mapping found — verify all pattern edges are satisfied.
			if checkEdges(g, pattern, mapping) {
				cp := make([]store.NodeID, len(mapping))
				copy(cp, mapping)
				results = append(results, SubgraphMatch{Mapping: cp})
			}
			return nil
		}

		for _, cand := range candidates[depth] {
			if used[cand] {
				continue
			}
			// Partial edge check: verify edges between already-mapped nodes and this one.
			if !partialEdgeCheck(g, pattern, mapping, depth, cand) {
				continue
			}
			mapping[depth] = cand
			used[cand] = true

			if err := backtrack(depth + 1); err != nil {
				return err
			}

			if maxMatches > 0 && len(results) >= maxMatches {
				return nil
			}

			mapping[depth] = store.InvalidNodeID
			used[cand] = false
		}
		return nil
	}

	if err := backtrack(0); err != nil {
		return nil, err
	}
	return results, nil
}

// buildCandidates returns, for each pattern node index, the list of data nodes
// that carry ALL of the pattern node's required labels.
func buildCandidates(g store.GraphStore, pattern *Pattern, scope []store.NodeID) ([][]store.NodeID, error) {
	candidates := make([][]store.NodeID, len(pattern.Nodes))

	for i, pn := range pattern.Nodes {
		if scope != nil {
			// Filter scope: node must carry all required labels.
			for _, id := range scope {
				if len(pn.Labels) == 0 {
					candidates[i] = append(candidates[i], id)
					continue
				}
				n, err := g.GetNode(id)
				if err != nil {
					continue
				}
				if nodeHasAllLabels(n, pn.Labels) {
					candidates[i] = append(candidates[i], id)
				}
			}
		} else {
			if len(pn.Labels) == 0 {
				// Unsupported without scope — caller should always provide scope
				// for full-graph searches to avoid loading all node IDs.
				continue
			}
			// Use the first label to seed candidates, then filter by remaining labels.
			ids, err := g.NodesByType(pn.Labels[0])
			if err != nil {
				return nil, err
			}
			if len(pn.Labels) == 1 {
				candidates[i] = ids
			} else {
				for _, id := range ids {
					n, err := g.GetNode(id)
					if err != nil {
						continue
					}
					if nodeHasAllLabels(n, pn.Labels) {
						candidates[i] = append(candidates[i], id)
					}
				}
			}
		}
	}
	return candidates, nil
}

// partialEdgeCheck verifies that all pattern edges between already-committed
// nodes (indices 0..depth-1) and the candidate at position depth are satisfied.
func partialEdgeCheck(g store.GraphStore, pattern *Pattern, mapping []store.NodeID, depth int, cand store.NodeID) bool {
	for _, pe := range pattern.Edges {
		// Only check edges where both endpoints are now committed.
		srcMapped := pe.SrcPatternID < depth || pe.SrcPatternID == depth
		dstMapped := pe.DstPatternID < depth || pe.DstPatternID == depth
		if !srcMapped || !dstMapped {
			continue
		}

		var srcID, dstID store.NodeID
		if pe.SrcPatternID == depth {
			srcID = cand
		} else {
			srcID = mapping[pe.SrcPatternID]
		}
		if pe.DstPatternID == depth {
			dstID = cand
		} else {
			dstID = mapping[pe.DstPatternID]
		}

		if srcID == store.InvalidNodeID || dstID == store.InvalidNodeID {
			continue
		}

		if !edgeExists(g, srcID, dstID, pe.Labels) {
			return false
		}
	}
	return true
}

// checkEdges verifies all pattern edges against a complete mapping.
func checkEdges(g store.GraphStore, pattern *Pattern, mapping []store.NodeID) bool {
	for _, pe := range pattern.Edges {
		srcID := mapping[pe.SrcPatternID]
		dstID := mapping[pe.DstPatternID]
		if !edgeExists(g, srcID, dstID, pe.Labels) {
			return false
		}
	}
	return true
}

// edgeExists returns true if there is at least one edge from srcID to dstID
// whose labels include ALL of the required labels (AND semantics).
// An empty requiredLabels slice matches any edge.
func edgeExists(g store.GraphStore, srcID, dstID store.NodeID, requiredLabels []store.EdgeType) bool {
	// Use the first required label as a filter to narrow candidates, then
	// check the rest.
	var filter []store.EdgeType
	if len(requiredLabels) > 0 {
		filter = []store.EdgeType{requiredLabels[0]}
	}
	edges, err := g.EdgesOf(srcID, store.DirectionOutbound, filter)
	if err != nil {
		return false
	}
	for _, e := range edges {
		if e.Dst != dstID {
			continue
		}
		if edgeHasAllLabels(e, requiredLabels) {
			return true
		}
	}
	return false
}

// nodeHasAllLabels returns true if n carries every label in required.
func nodeHasAllLabels(n *store.Node, required []store.NodeType) bool {
	for _, req := range required {
		if !n.HasLabel(req) {
			return false
		}
	}
	return true
}

// edgeHasAllLabels returns true if e carries every label in required.
func edgeHasAllLabels(e *store.Edge, required []store.EdgeType) bool {
	for _, req := range required {
		if !e.HasLabel(req) {
			return false
		}
	}
	return true
}
