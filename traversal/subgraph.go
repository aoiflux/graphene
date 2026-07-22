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

	probe := newEdgeProbe(g)

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
			if checkEdges(probe, pattern, mapping) {
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
			if !partialEdgeCheck(probe, pattern, mapping, depth, cand) {
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

	// Label postings, fetched once per distinct label rather than per (scope
	// node, pattern node) pair. Testing membership against an ascending posting
	// list is a binary search over IDs; the previous version materialised the
	// whole node record just to read its labels back off it.
	//
	// Scope order is preserved deliberately: candidate order determines which
	// matches a maxMatches-capped search returns, so reordering here would
	// silently change results.
	postings := map[store.NodeType][]store.NodeID{}
	labelPostings := func(t store.NodeType) ([]store.NodeID, error) {
		if ids, ok := postings[t]; ok {
			return ids, nil
		}
		ids, err := g.NodesByType(t)
		if err != nil {
			return nil, err
		}
		postings[t] = ids
		return ids, nil
	}

	for i, pn := range pattern.Nodes {
		if scope != nil {
			if len(pn.Labels) == 0 {
				candidates[i] = append(candidates[i], scope...)
				continue
			}
			lists := make([][]store.NodeID, 0, len(pn.Labels))
			for _, lbl := range pn.Labels {
				ids, err := labelPostings(lbl)
				if err != nil {
					return nil, err
				}
				if len(ids) == 0 {
					lists = nil
					break
				}
				lists = append(lists, ids)
			}
			if lists == nil {
				continue // some required label has no nodes at all
			}
			for _, id := range scope {
				all := true
				for _, ids := range lists {
					if !store.SortedContainsID(ids, id) {
						all = false
						break
					}
				}
				if all {
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
func partialEdgeCheck(probe *edgeProbe, pattern *Pattern, mapping []store.NodeID, depth int, cand store.NodeID) bool {
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

		if !probe.exists(srcID, dstID, pe.Labels) {
			return false
		}
	}
	return true
}

// checkEdges verifies all pattern edges against a complete mapping.
func checkEdges(probe *edgeProbe, pattern *Pattern, mapping []store.NodeID) bool {
	for _, pe := range pattern.Edges {
		srcID := mapping[pe.SrcPatternID]
		dstID := mapping[pe.DstPatternID]
		if !probe.exists(srcID, dstID, pe.Labels) {
			return false
		}
	}
	return true
}

// edgeProbe answers "is there an edge from src to dst with these labels?"
// without materialising the edges it rejects.
//
// This is the hot path of subgraph matching: backtracking asks it once per
// candidate pair per pattern edge, and the previous version called EdgesOf,
// which builds a *store.Edge for **every** outbound edge of src just to compare
// one field on each. On the two-hop benchmark that was ~200 allocations per
// scoped node.
//
// AdjacencyReader.IncidentEdges yields (edge ID, far node) pairs into a caller
// buffer instead, so the walk allocates nothing and a record is built only for
// an edge that already matched on endpoint — and only when more than one label
// has to be checked, which the store's own filter cannot express (it is OR, the
// pattern's is AND).
type edgeProbe struct {
	g   store.GraphStore
	adj store.AdjacencyReader // nil when the backend does not support it
	buf []store.IncidentEdge

	// One-entry memo of the last (source, filter) adjacency walk.
	//
	// Backtracking holds the source fixed while it iterates every candidate for
	// the next pattern node, so the same adjacency was being re-walked once per
	// candidate — an RLock and a filtered scan each time, O(candidates × degree)
	// where O(degree) suffices. The hit rate on that loop is effectively 100%.
	//
	// Caching within a single search does not weaken any guarantee: the
	// consistency model already states a query is not a snapshot, and the
	// previous code gave no cross-call guarantee either.
	memoValid  bool
	memoSrc    store.NodeID
	memoFilter store.EdgeType // zero when the walk was unfiltered
	memoHasFil bool
}

func newEdgeProbe(g store.GraphStore) *edgeProbe {
	p := &edgeProbe{g: g}
	if a, ok := g.(store.AdjacencyReader); ok {
		p.adj = a
	}
	return p
}

func (p *edgeProbe) exists(srcID, dstID store.NodeID, requiredLabels []store.EdgeType) bool {
	// The store filter takes OR semantics, so it can only narrow by one label;
	// the remaining labels are checked against the record.
	var filter []store.EdgeType
	if len(requiredLabels) > 0 {
		filter = requiredLabels[:1]
	}

	if p.adj == nil {
		return p.existsFallback(srcID, dstID, filter, requiredLabels)
	}

	if !p.memoHit(srcID, filter) {
		var err error
		p.buf, err = p.adj.IncidentEdges(p.buf[:0], srcID, store.DirectionOutbound, filter)
		if err != nil {
			p.memoValid = false
			return false
		}
		p.memoValid = true
		p.memoSrc = srcID
		p.memoHasFil = len(filter) > 0
		if p.memoHasFil {
			p.memoFilter = filter[0]
		}
	}

	for _, ie := range p.buf {
		if ie.Neighbour != dstID {
			continue
		}
		if len(requiredLabels) <= 1 {
			// The store's filter already proved the only label required, so the
			// endpoint match is the whole answer — nothing to materialise.
			return true
		}
		e, err := p.g.GetEdge(ie.Edge)
		if err != nil {
			continue
		}
		if edgeHasAllLabels(e, requiredLabels) {
			return true
		}
	}
	return false
}

// memoHit reports whether buf already holds the adjacency walk for this source
// and filter.
func (p *edgeProbe) memoHit(srcID store.NodeID, filter []store.EdgeType) bool {
	if !p.memoValid || p.memoSrc != srcID {
		return false
	}
	if p.memoHasFil != (len(filter) > 0) {
		return false
	}
	return !p.memoHasFil || p.memoFilter == filter[0]
}

// existsFallback preserves behaviour for backends without AdjacencyReader.
func (p *edgeProbe) existsFallback(srcID, dstID store.NodeID, filter, requiredLabels []store.EdgeType) bool {
	edges, err := p.g.EdgesOf(srcID, store.DirectionOutbound, filter)
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
