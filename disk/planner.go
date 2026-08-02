package disk

// Query execution for the disk backend: choosing what drives a query, narrowing
// the candidate set through the remaining filters, and reporting the resulting
// plan. Split out of store.go, unchanged.

import (
	"math"
	"slices"

	"github.com/aoiflux/graphene/store"
)

func (s *Store) QueryNodeIDs(query store.NodeQuery) ([]store.NodeID, error) {
	candidates, sortedAsc, plan := s.driveNodeCandidates(query)

	if len(query.Types) > 0 {
		typeSet := make(map[store.NodeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		filtered := make([]store.NodeID, 0, len(candidates))
		for _, id := range candidates {
			n, err := s.GetNode(id)
			if err != nil {
				continue
			}
			if nodeHasAnyType(n, typeSet) {
				filtered = append(filtered, id)
			}
		}
		candidates = filtered
	}

	if len(query.Filters) > 0 {
		// Both sides must be ascending for the merge. The driving set often
		// already is; when it is not, sorting it once here is repaid immediately
		// because the merge output is ascending too, which retires the sort below.
		if !sortedAsc {
			candidates = store.SortDedupeIDs(candidates)
			sortedAsc = true
		}
		if store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
			// Every filter's own set contains the answer, so the residual pass
			// can narrow the candidates directly and skip the driving filter
			// entirely rather than re-deriving a set it was already built from.
			candidates = s.propIdx.NarrowNodesByFilters(candidates, query.Filters, plan.DriverFilter)
		} else {
			matched := s.matchNodeIDsByFilters(query.Filters, store.MatchAny)
			candidates = store.IntersectSortedIDs(candidates, matched)
		}
	}

	order := store.NormalizedQueryOrder(query.Order)
	// An ascending candidate set needs no sort at all, and only a linear reverse
	// to satisfy a descending query.
	switch {
	case sortedAsc && order == store.QueryOrderAsc:
		// already in the requested order
	case sortedAsc:
		store.ReverseIDs(candidates)
	default:
		store.SortIDsForOrder(candidates, order)
	}
	return store.ApplyNodeQueryWindow(candidates, query.Offset, query.Limit), nil
}

func (s *Store) QueryEdgeIDs(query store.EdgeQuery) ([]store.EdgeID, error) {
	candidates, sortedAsc, plan := s.driveEdgeCandidates(query)

	if len(query.Types) > 0 || len(query.SrcIDs) > 0 || len(query.DstIDs) > 0 {
		typeSet := make(map[store.EdgeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		srcSet := makeNodeIDSet(query.SrcIDs)
		dstSet := makeNodeIDSet(query.DstIDs)

		filtered := make([]store.EdgeID, 0, len(candidates))
		for _, id := range candidates {
			e, err := s.GetEdge(id)
			if err != nil {
				continue
			}
			if len(typeSet) > 0 && !edgeHasAnyType(e, typeSet) {
				continue
			}
			if len(srcSet) > 0 {
				if _, ok := srcSet[e.Src]; !ok {
					continue
				}
			}
			if len(dstSet) > 0 {
				if _, ok := dstSet[e.Dst]; !ok {
					continue
				}
			}
			filtered = append(filtered, id)
		}
		candidates = filtered
	}

	if len(query.Filters) > 0 {
		if !sortedAsc {
			candidates = store.SortDedupeIDs(candidates)
			sortedAsc = true
		}
		if store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
			candidates = s.propIdx.NarrowEdgesByFilters(candidates, query.Filters, plan.DriverFilter)
		} else {
			matched := s.matchEdgeIDsByFilters(query.Filters, store.MatchAny)
			candidates = store.IntersectSortedIDs(candidates, matched)
		}
	}

	order := store.NormalizedQueryOrder(query.Order)
	switch {
	case sortedAsc && order == store.QueryOrderAsc:
		// already in the requested order
	case sortedAsc:
		store.ReverseIDs(candidates)
	default:
		store.SortIDsForOrder(candidates, order)
	}
	return store.ApplyEdgeQueryWindow(candidates, query.Offset, query.Limit), nil
}

// sortDedupeNodeIDs sorts ids ascending and removes duplicates, in place.
//
// The ordered index emits IDs in value order, and an entity registered under two
// values for the same key appears once per value, so both properties have to be
// restored before the result can be used as a driving set.
func sortDedupeNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) < 2 {
		return ids
	}
	slices.Sort(ids)
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// sortDedupeEdgeIDs is sortDedupeNodeIDs for edge IDs.
func sortDedupeEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) < 2 {
		return ids
	}
	slices.Sort(ids)
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// --- query planning ---
//
// Queries used to start from a full enumeration of the delta layer plus every
// live CSR record. The drive* helpers instead pick the cheapest index that is
// still a guaranteed superset of the result, leaving the filter stages (and
// therefore the results) unchanged.

// labelSizeUnknown marks "this query has no labels to drive from". It must lose
// every comparison against a real cardinality.
const labelSizeUnknown = -1

// labelDriverWins reports whether driving from label postings beats driving from
// an equality filter of the given cardinality.
//
// Ties go to equality deliberately: the equality path resolves through the
// property index and returns candidates already in ascending ID order, while the
// label path has to dedupe across types and reports unsorted. Equal candidate
// counts therefore are not equal cost.
func labelDriverWins(labelSize, equalitySize int) bool {
	return labelSize != labelSizeUnknown && labelSize < equalitySize
}

// nodeLabelCandidateCount is an upper bound on the number of nodes carrying any
// of types. It double-counts a node present in both the delta and the CSR, and
// it ignores tombstones.
//
// An upper bound is exactly what costing needs here: the chosen driver must be a
// superset of the answer, and overestimating can only make the planner more
// reluctant to pick labels — never wrong, just occasionally conservative.
func (s *Store) nodeLabelCandidateCount(types []store.NodeType) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := 0
	for _, t := range types {
		total += len(s.deltaNodesByType[t])
		if s.csr != nil {
			// NodesByType aliases CSR memory; len is O(1) and nothing escapes.
			total += len(s.csr.NodesByType(t))
		}
	}
	return total
}

// edgeLabelCandidateCount is the edge equivalent of nodeLabelCandidateCount.
func (s *Store) edgeLabelCandidateCount(types []store.EdgeType) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := 0
	for _, t := range types {
		total += len(s.deltaEdgesByType[t])
		if s.csr != nil {
			total += len(s.csr.EdgesByType(t))
		}
	}
	return total
}

// driveNodeCandidates returns the starting candidate set for a node query and
// whether it is already in ascending ID order.
// driveNodeCandidates picks the cheapest source that is guaranteed to contain
// the query's answer. It returns the candidates, whether they are ascending, and
// a plan describing that choice — including the filter it consumed, so the
// residual pass does not evaluate that filter a second time.
func (s *Store) driveNodeCandidates(query store.NodeQuery) ([]store.NodeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		return s.collectCandidateNodeIDs(query.IDs), false, store.QueryPlan{Driver: store.DriverIDs, DriverFilter: -1}
	}

	// Most selective equality filter, if any qualifies as a driver. Its
	// cardinality is known exactly and is almost always far below the graph size.
	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := s.propIdx.NodeCardinality(f.Key, f.Value)
		if bestFilter == nil || size < bestSize {
			filter := f
			bestFilter = &filter
			bestSize = size
		}
	}
	// Labels bound the result too, and their posting sizes are known in O(1)
	// (NodesByType aliases CSR-owned memory, so this counts rather than
	// materialises). Comparing the two means a highly selective label is no
	// longer passed over in favour of a weak equality filter: the planner used
	// to take any equality driver unconditionally, so `Types=[Case]` (100 nodes)
	// combined with a 14 000-hit filter drove from the filter.
	labelSize := labelSizeUnknown
	if len(query.Types) > 0 {
		labelSize = s.nodeLabelCandidateCount(query.Types)
	}

	if bestFilter != nil {
		if labelDriverWins(labelSize, bestSize) {
			return s.driveNodeLabels(query.Types)
		}
		return s.liveNodeIDs(s.propIdx.NodesByProperty(bestFilter.Key, bestFilter.Value)), true, store.QueryPlan{
			Driver:       store.DriverEquality,
			DriverKey:    bestFilter.Key,
			DriverFilter: store.FilterIndexOf(query.Filters, *bestFilter),
		}
	}

	// A range or prefix filter on a key declared ordered bounds the result too,
	// and resolving it here means the query never enumerates the whole graph.
	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		if ids, served := s.propIdx.NodesMatchingOrdered(nil, f); served {
			return s.liveNodeIDs(sortDedupeNodeIDs(ids)), true, store.QueryPlan{
				Driver:       store.DriverOrdered,
				DriverKey:    f.Key,
				DriverFilter: store.FilterIndexOf(query.Filters, f),
			}
		}
	}

	if len(query.Types) > 0 {
		return s.driveNodeLabels(query.Types)
	}

	return s.collectCandidateNodeIDs(nil), false, store.QueryPlan{Driver: store.DriverScan, DriverFilter: -1}
}

// driveNodeLabels resolves the union of the given labels' postings. It is served
// by the CSR label index plus the delta, so it costs time proportional to the
// number of matches rather than to the graph.
//
// The result is unsorted: postings are individually ascending, but a union over
// several labels interleaves them.
func (s *Store) driveNodeLabels(types []store.NodeType) ([]store.NodeID, bool, store.QueryPlan) {
	seen := make(map[store.NodeID]struct{})
	var out []store.NodeID
	for _, t := range types {
		ids, err := s.NodesByType(t)
		if err != nil {
			continue
		}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out, false, store.QueryPlan{Driver: store.DriverLabels, DriverFilter: -1}
}

// liveNodeIDs drops IDs that no longer resolve to a live node, preserving order.
func (s *Store) liveNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) == 0 {
		return ids // a miss should not pay for the lock
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := ids[:0]
	for _, id := range ids {
		if s.nodeExistsLocked(id) {
			out = append(out, id)
		}
	}
	return out
}

// liveEdgeIDs drops IDs that no longer resolve to a live edge, preserving order.
func (s *Store) liveEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) == 0 {
		return ids // a miss should not pay for the lock
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := ids[:0]
	for _, id := range ids {
		if s.edgeExistsLocked(id) {
			out = append(out, id)
		}
	}
	return out
}

// driveEdgeCandidates returns the starting candidate set for an edge query and
// whether it is already in ascending ID order.
// driveEdgeCandidates mirrors driveNodeCandidates: it returns the candidates,
// whether they are ascending, and a plan naming the source it drove from and the
// filter it consumed, so the residual pass does not re-evaluate that filter.
func (s *Store) driveEdgeCandidates(query store.EdgeQuery) ([]store.EdgeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		return s.collectCandidateEdgeIDs(query.IDs), false, store.QueryPlan{Driver: store.DriverIDs, DriverFilter: -1}
	}

	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := s.propIdx.EdgeCardinality(f.Key, f.Value)
		if bestFilter == nil || size < bestSize {
			filter := f
			bestFilter = &filter
			bestSize = size
		}
	}

	// Anchored queries are bounded by the anchors' degree, which the CSR offset
	// arrays give us without touching a single edge record.
	anchorDir := store.DirectionOutbound
	anchors := query.SrcIDs
	anchorSize := -1
	if len(query.SrcIDs) > 0 {
		s.mu.RLock()
		anchorSize = s.degreeSumLocked(query.SrcIDs, store.DirectionOutbound)
		s.mu.RUnlock()
	}
	if len(query.DstIDs) > 0 {
		s.mu.RLock()
		dstSize := s.degreeSumLocked(query.DstIDs, store.DirectionInbound)
		s.mu.RUnlock()
		if anchorSize < 0 || dstSize < anchorSize {
			anchorSize = dstSize
			anchorDir = store.DirectionInbound
			anchors = query.DstIDs
		}
	}

	// Three possible drivers, costed on the same scale. An unavailable driver is
	// MaxInt so it can never win a comparison, which keeps the cases below free
	// of sentinel checks.
	eqCost := math.MaxInt
	if bestFilter != nil {
		eqCost = bestSize
	}
	anchorCost := math.MaxInt
	if anchorSize >= 0 {
		anchorCost = anchorSize
	}
	labelCost := math.MaxInt
	if len(query.Types) > 0 {
		labelCost = s.edgeLabelCandidateCount(query.Types)
	}

	switch {
	// Labels must beat both alternatives strictly: they return unsorted
	// candidates, where equality and adjacency both return ascending.
	case labelCost < eqCost && labelCost < anchorCost:
		return s.driveEdgeLabels(query.Types)
	case bestFilter != nil && eqCost <= anchorCost:
		return s.liveEdgeIDs(s.propIdx.EdgesByProperty(bestFilter.Key, bestFilter.Value)), true, store.QueryPlan{
			Driver:       store.DriverEquality,
			DriverKey:    bestFilter.Key,
			DriverFilter: store.FilterIndexOf(query.Filters, *bestFilter),
		}
	case anchorSize >= 0:
		return s.incidentEdgeIDs(anchors, anchorDir), false, store.QueryPlan{
			Driver: store.DriverAdjacency, DriverFilter: -1,
		}
	}

	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		if ids, served := s.propIdx.EdgesMatchingOrdered(nil, f); served {
			return s.liveEdgeIDs(sortDedupeEdgeIDs(ids)), true, store.QueryPlan{
				Driver:       store.DriverOrdered,
				DriverKey:    f.Key,
				DriverFilter: store.FilterIndexOf(query.Filters, f),
			}
		}
	}

	if len(query.Types) > 0 {
		return s.driveEdgeLabels(query.Types)
	}

	return s.collectCandidateEdgeIDs(nil), false, store.QueryPlan{Driver: store.DriverScan, DriverFilter: -1}
}

// driveEdgeLabels is the edge counterpart of driveNodeLabels: the union of the
// given labels' postings, served by the CSR label index plus the delta, and
// unsorted because a union over several labels interleaves ascending runs.
func (s *Store) driveEdgeLabels(types []store.EdgeType) ([]store.EdgeID, bool, store.QueryPlan) {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	for _, t := range types {
		ids, err := s.EdgesByType(t)
		if err != nil {
			continue
		}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out, false, store.QueryPlan{Driver: store.DriverLabels, DriverFilter: -1}
}

func (s *Store) collectCandidateNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) > 0 {
		out := make([]store.NodeID, 0, len(ids))
		seen := make(map[store.NodeID]struct{}, len(ids))
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, err := s.GetNode(id); err == nil {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out
	}

	out := make([]store.NodeID, 0)
	seen := make(map[store.NodeID]struct{})

	s.mu.RLock()
	for id := range s.deltaNodes {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	s.mu.RUnlock()

	if s.csr != nil {
		s.mu.RLock()
		for i := 1; i < len(s.csr.nodes); i++ {
			n := s.csr.nodes[i]
			if n.ID == store.InvalidNodeID {
				continue
			}
			if _, del := s.deletedNodes[n.ID]; del {
				continue
			}
			if _, ok := seen[n.ID]; !ok {
				seen[n.ID] = struct{}{}
				out = append(out, n.ID)
			}
		}
		s.mu.RUnlock()
	}

	return out
}

func (s *Store) collectCandidateEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) > 0 {
		out := make([]store.EdgeID, 0, len(ids))
		seen := make(map[store.EdgeID]struct{}, len(ids))
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, err := s.GetEdge(id); err == nil {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out
	}

	out := make([]store.EdgeID, 0)
	seen := make(map[store.EdgeID]struct{})

	s.mu.RLock()
	for id := range s.deltaEdges {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	s.mu.RUnlock()

	if s.csr != nil {
		s.mu.RLock()
		for i := 1; i < len(s.csr.edges); i++ {
			e := s.csr.edges[i]
			if e.ID == store.InvalidEdgeID {
				continue
			}
			if _, del := s.deletedEdges[e.ID]; del {
				continue
			}
			if _, ok := seen[e.ID]; !ok {
				seen[e.ID] = struct{}{}
				out = append(out, e.ID)
			}
		}
		s.mu.RUnlock()
	}

	return out
}

// matchNodeIDsByFilters returns the ascending, deduplicated set of node IDs
// satisfying the filters under the given mode.
//
// Each filter is resolved to a sorted slice and the slices are merged, rather
// than each being built into a map and the maps intersected. Merging is one pass
// per side with no hashing, the output stays sorted so the query path can skip
// its final sort, and an empty intersection under MatchAll can stop early.
func (s *Store) matchNodeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) []store.NodeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.NodeID
	for i, f := range filters {
		set := s.matchOneNodeFilter(f)
		if i == 0 {
			acc = set
			continue
		}
		if mode == store.MatchAny {
			acc = store.UnionSortedIDs(acc, set)
			continue
		}
		acc = store.IntersectSortedIDs(acc, set)
		if len(acc) == 0 {
			// Nothing can re-enter an empty intersection.
			return acc
		}
	}
	return acc
}

// matchOneNodeFilter resolves a single filter to an ascending, deduplicated set.
func (s *Store) matchOneNodeFilter(f store.PropertyFilter) []store.NodeID {
	if f.Op == store.PropertyOpEqual {
		// Postings are already ascending and deduplicated.
		return s.propIdx.NodesByProperty(f.Key, f.Value)
	}
	// A key declared ordered answers ranges and prefixes by binary search. Its
	// comparison is byte-wise, so the whole predicate is resolved there — mixing
	// it with the scan matcher below would apply two orderings to one key.
	if ids, served := s.propIdx.NodesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	// Otherwise scan only the buckets belonging to this key, never the whole index.
	// One comparison per distinct value, not per entry.
	var out []store.NodeID
	s.propIdx.ForEachNodeValue(f.Key, func(value []byte, ids []store.NodeID) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, ids...)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

// matchEdgeIDsByFilters is matchNodeIDsByFilters for edge properties.
func (s *Store) matchEdgeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) []store.EdgeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.EdgeID
	for i, f := range filters {
		set := s.matchOneEdgeFilter(f)
		if i == 0 {
			acc = set
			continue
		}
		if mode == store.MatchAny {
			acc = store.UnionSortedIDs(acc, set)
			continue
		}
		acc = store.IntersectSortedIDs(acc, set)
		if len(acc) == 0 {
			return acc
		}
	}
	return acc
}

func (s *Store) matchOneEdgeFilter(f store.PropertyFilter) []store.EdgeID {
	if f.Op == store.PropertyOpEqual {
		return s.propIdx.EdgesByProperty(f.Key, f.Value)
	}
	if ids, served := s.propIdx.EdgesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	var out []store.EdgeID
	s.propIdx.ForEachEdgeValue(f.Key, func(value []byte, ids []store.EdgeID) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, ids...)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

func nodeHasAnyType(n *store.Node, typeSet map[store.NodeType]struct{}) bool {
	for _, lbl := range n.Labels {
		if _, ok := typeSet[lbl]; ok {
			return true
		}
	}
	return false
}

func edgeHasAnyType(e *store.Edge, typeSet map[store.EdgeType]struct{}) bool {
	for _, lbl := range e.Labels {
		if _, ok := typeSet[lbl]; ok {
			return true
		}
	}
	return false
}

func makeNodeIDSet(ids []store.NodeID) map[store.NodeID]struct{} {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[store.NodeID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

// ExplainNodeQuery reports how the planner would resolve query, without
// returning the matching entities.
//
// It runs the driving step for real — that is the only way to know how large the
// candidate set is, and the candidate size is what decides how each residual
// filter is applied — but stops before applying them. So the cost is the driver,
// not the query.
//
// The plan is diagnostic. Which index the planner picks is free to change as the
// cost model improves; the results a query returns are not.
func (s *Store) ExplainNodeQuery(query store.NodeQuery) (store.QueryPlan, error) {
	candidates, _, plan := s.driveNodeCandidates(query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = s.propIdx.PlanNodeResiduals(query.Filters, plan.DriverFilter, len(candidates))
	}
	ids, err := s.QueryNodeIDs(query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}

// ExplainEdgeQuery is ExplainNodeQuery for edge queries.
func (s *Store) ExplainEdgeQuery(query store.EdgeQuery) (store.QueryPlan, error) {
	candidates, _, plan := s.driveEdgeCandidates(query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = s.propIdx.PlanEdgeResiduals(query.Filters, plan.DriverFilter, len(candidates))
	}
	ids, err := s.QueryEdgeIDs(query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}
