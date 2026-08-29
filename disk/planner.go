package disk

// Query execution for the disk backend: choosing what drives a query, narrowing
// the candidate set through the remaining filters, and reporting the resulting
// plan. Split out of store.go, unchanged.

import (
	"math"
	"slices"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// QueryNodeIDs resolves query against the newest visible state.
//
// One read lock covers the whole query: the driving step, every candidate
// resolution and the residual filters. It used to take the lock afresh at each
// of those, which meant a query could drive from a candidate set the graph no
// longer agreed with by the time the residuals ran — a query that returned an
// ID for a node deleted halfway through it. Holding the lock once costs writers
// a longer wait and buys the query an answer that was true at one instant.
func (s *Store) QueryNodeIDs(query store.NodeQuery) ([]store.NodeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queryNodeIDs(s.readerLocked(), query)
}

// queryNodeIDs is the body, resolved against a caller-supplied reader so a
// snapshot runs the identical plan over its own pinned state. Caller holds
// whatever lock that reader requires.
func (s *Store) queryNodeIDs(r reader, query store.NodeQuery) ([]store.NodeID, error) {
	candidates, sortedAsc, plan := s.driveNodeCandidates(r, query)

	if len(query.Types) > 0 {
		typeSet := make(map[store.NodeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		filtered := make([]store.NodeID, 0, len(candidates))
		for _, id := range candidates {
			n, ok := r.node(id)
			if !ok {
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
			candidates = r.index().NarrowNodesByFilters(candidates, query.Filters, plan.DriverFilters)
		} else {
			matched := s.matchNodeIDsByFilters(r, query.Filters, store.MatchAny)
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

// QueryEdgeIDs resolves query against the newest visible state. See
// QueryNodeIDs for why one lock covers the whole of it.
func (s *Store) QueryEdgeIDs(query store.EdgeQuery) ([]store.EdgeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queryEdgeIDs(s.readerLocked(), query)
}

func (s *Store) queryEdgeIDs(r reader, query store.EdgeQuery) ([]store.EdgeID, error) {
	candidates, sortedAsc, plan := s.driveEdgeCandidates(r, query)

	if len(query.Types) > 0 || len(query.SrcIDs) > 0 || len(query.DstIDs) > 0 {
		typeSet := make(map[store.EdgeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		srcSet := makeNodeIDSet(query.SrcIDs)
		dstSet := makeNodeIDSet(query.DstIDs)

		filtered := make([]store.EdgeID, 0, len(candidates))
		for _, id := range candidates {
			e, ok := r.edge(id)
			if !ok {
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
			candidates = r.index().NarrowEdgesByFilters(candidates, query.Filters, plan.DriverFilters)
		} else {
			matched := s.matchEdgeIDsByFilters(r, query.Filters, store.MatchAny)
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

// driverUnavailable is the cost of a driver this query cannot use. MaxInt so it
// loses every comparison without the comparisons needing sentinel checks — the
// same idiom driveEdgeCandidates already uses for its three drivers.
const driverUnavailable = math.MaxInt

// bestOrderedNodeDriver returns the most selective range or prefix filter the
// ordered index can serve, and its estimated cardinality.
//
// "Most selective" is new. This used to be "the first one in the query", because
// a range had no size and so could not be compared with anything — including
// with the other ranges in the same query. See index/stats.go.
func (s *Store) bestOrderedNodeDriver(r reader, query store.NodeQuery) (*store.PropertyFilter, int) {
	var best *store.PropertyFilter
	bestSize := 0
	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		size, served := r.index().NodeRangeCardinality(f)
		if !served {
			continue
		}
		if best == nil || size < bestSize {
			filter := f
			best = &filter
			bestSize = size
		}
	}
	return best, bestSize
}

// bestOrderedEdgeDriver is bestOrderedNodeDriver for edge queries.
func (s *Store) bestOrderedEdgeDriver(r reader, query store.EdgeQuery) (*store.PropertyFilter, int) {
	var best *store.PropertyFilter
	bestSize := 0
	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		size, served := r.index().EdgeRangeCardinality(f)
		if !served {
			continue
		}
		if best == nil || size < bestSize {
			filter := f
			best = &filter
			bestSize = size
		}
	}
	return best, bestSize
}

// labelDriverWins reports whether driving from label postings beats driving from
// an equality filter of the given cardinality.
//
// Ties go to equality deliberately: the equality path resolves through the
// property index and returns candidates already in ascending ID order, while the
// label path has to dedupe across types and reports unsorted. Equal candidate
// counts therefore are not equal cost.
func labelDriverWins(labelSize, equalitySize int) bool {
	return labelSize < equalitySize
}

// driveNodeCandidates returns the starting candidate set for a node query and
// whether it is already in ascending ID order.
// driveNodeCandidates picks the cheapest source that is guaranteed to contain
// the query's answer. It returns the candidates, whether they are ascending, and
// a plan describing that choice — including the filter it consumed, so the
// residual pass does not evaluate that filter a second time.
func (s *Store) driveNodeCandidates(r reader, query store.NodeQuery) ([]store.NodeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		return collectCandidateNodeIDs(r, query.IDs), false, store.QueryPlan{Driver: store.DriverIDs}
	}

	// Most selective equality filter, if any qualifies as a driver. Its
	// cardinality is known exactly and is almost always far below the graph size.
	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := r.index().NodeCardinality(f.Key, f.Value)
		if bestFilter == nil || size < bestSize {
			filter := f
			bestFilter = &filter
			bestSize = size
		}
	}
	// A range or prefix filter on a key declared ordered bounds the result too,
	// and the ordered index can now say by how much, so it competes rather than
	// being tried in query order once everything else has been ruled out.
	bestOrdered, orderedSize := s.bestOrderedNodeDriver(r, query)

	// A declared composite whose keys the query pins to values answers the whole
	// conjunction at once, and reports the exact size of what it would return.
	// A composite covers a tuple of at least two keys, so a query carrying
	// fewer filters than that cannot match one however many are declared.
	// Testing it here rather than inside MatchNodeComposite is what keeps a
	// single-filter query from touching the property index at all: the flag it
	// would otherwise read is a cold cache line on a struct this query has no
	// other reason to load, and it measured at ~20 ns against a 230 ns query.
	var comp index.CompositeMatch
	hasComposite := false
	if len(query.Filters) > 1 {
		comp, hasComposite = r.index().MatchNodeComposite(query.Filters, query.FilterMode)
	}

	// Labels bound the result too, and their posting sizes are known in O(1)
	// (NodesByType aliases CSR-owned memory, so this counts rather than
	// materialises). Comparing them means a highly selective label is no longer
	// passed over in favour of a weak equality filter: the planner used to take
	// any equality driver unconditionally, so `Types=[Case]` (100 nodes)
	// combined with a 14 000-hit filter drove from the filter.
	//
	// All three are now on one scale. Before, they were on none: an equality
	// filter beat every range, and any range beat every label, whatever their
	// sizes — so a 3-hit range lost to a 900 000-hit equality filter, and a
	// whole-key range beat a 10-node label.
	eqCost := driverUnavailable
	if bestFilter != nil {
		eqCost = bestSize
	}
	orderedCost := driverUnavailable
	if bestOrdered != nil {
		orderedCost = orderedSize
	}
	labelCost := driverUnavailable
	if len(query.Types) > 0 {
		labelCost = r.nodeLabelCandidateCount(query.Types)
	}
	compositeCost := driverUnavailable
	if hasComposite {
		compositeCost = comp.Size
	}

	switch {
	// Labels must beat every alternative strictly: they return unsorted
	// candidates, where equality, ordered and composite all return ascending, so
	// an equal count is not an equal cost.
	case labelDriverWins(labelCost, eqCost) && labelDriverWins(labelCost, orderedCost) &&
		labelDriverWins(labelCost, compositeCost):
		return driveNodeLabels(r, query.Types)

	// The composite wins its ties against the other two ascending drivers,
	// because a tie on candidates is not a tie on the work that follows: it
	// retires every filter it covers, so the residual pass has nothing left to
	// apply where equality or a range would leave the rest of the conjunction
	// to evaluate one candidate at a time.
	case hasComposite && compositeCost <= eqCost && compositeCost <= orderedCost:
		// served can only be false if the declaration disappeared between
		// matching and lookup, which nothing in the engine does — kept because
		// the contract permits it and a planner that silently drove from an
		// empty set would return too few rows rather than too many.
		if ids, served := r.index().NodesByComposite(comp); served {
			return liveNodeIDs(r, ids), true, store.QueryPlan{
				Driver:        store.DriverComposite,
				DriverKey:     comp.Name(),
				DriverFilters: comp.Filters,
			}
		}

	// Equality wins ties against a range: both yield ascending candidates, but
	// the ordered path pays a sort-and-dedupe that the postings do not.
	case bestFilter != nil && eqCost <= orderedCost:
		return liveNodeIDs(r, r.index().NodesByProperty(bestFilter.Key, bestFilter.Value)), true, store.QueryPlan{
			Driver:        store.DriverEquality,
			DriverKey:     bestFilter.Key,
			DriverFilters: store.FilterMaskOf(query.Filters, *bestFilter),
		}

	case bestOrdered != nil:
		// served was already established when the driver was costed, so the
		// fallback here is unreachable rather than merely unlikely — it is kept
		// because NodesMatchingOrdered's contract allows it and a planner that
		// silently returned the wrong candidate set would be worse than one that
		// scans.
		if ids, served := r.index().NodesMatchingOrdered(nil, *bestOrdered); served {
			return liveNodeIDs(r, sortDedupeNodeIDs(ids)), true, store.QueryPlan{
				Driver:        store.DriverOrdered,
				DriverKey:     bestOrdered.Key,
				DriverFilters: store.FilterMaskOf(query.Filters, *bestOrdered),
			}
		}
	}

	if len(query.Types) > 0 {
		return driveNodeLabels(r, query.Types)
	}

	return r.allNodeIDs(), false, store.QueryPlan{Driver: store.DriverScan}
}

// driveNodeLabels resolves the union of the given labels' postings. It is served
// by the CSR label index plus the delta, so it costs time proportional to the
// number of matches rather than to the graph.
//
// The result is unsorted: postings are individually ascending, but a union over
// several labels interleaves them.
func driveNodeLabels(r reader, types []store.NodeType) ([]store.NodeID, bool, store.QueryPlan) {
	seen := make(map[store.NodeID]struct{})
	var out []store.NodeID
	for _, t := range types {
		for _, id := range r.nodesByType(t) {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out, false, store.QueryPlan{Driver: store.DriverLabels}
}

// liveNodeIDs drops IDs that do not resolve to a live node for r, preserving
// order.
//
// This is what keeps the property index from handing back an entity the records
// no longer have. The index is not versioned, so its postings are the current
// ones whatever epoch r reads at; resolving each against r is what makes the
// records the authority.
func liveNodeIDs(r reader, ids []store.NodeID) []store.NodeID {
	if len(ids) == 0 {
		return ids
	}
	out := ids[:0]
	for _, id := range ids {
		if r.nodeExists(id) {
			out = append(out, id)
		}
	}
	return out
}

// liveEdgeIDs is liveNodeIDs for edges.
func liveEdgeIDs(r reader, ids []store.EdgeID) []store.EdgeID {
	if len(ids) == 0 {
		return ids
	}
	out := ids[:0]
	for _, id := range ids {
		if r.edgeExists(id) {
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
func (s *Store) driveEdgeCandidates(r reader, query store.EdgeQuery) ([]store.EdgeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		return collectCandidateEdgeIDs(r, query.IDs), false, store.QueryPlan{Driver: store.DriverIDs}
	}

	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := r.index().EdgeCardinality(f.Key, f.Value)
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
		anchorSize = r.degreeSum(query.SrcIDs, store.DirectionOutbound)
	}
	if len(query.DstIDs) > 0 {
		dstSize := r.degreeSum(query.DstIDs, store.DirectionInbound)
		if anchorSize < 0 || dstSize < anchorSize {
			anchorSize = dstSize
			anchorDir = store.DirectionInbound
			anchors = query.DstIDs
		}
	}

	bestOrdered, orderedSize := s.bestOrderedEdgeDriver(r, query)
	// See driveNodeCandidates for why the filter count is tested here.
	var comp index.CompositeMatch
	hasComposite := false
	if len(query.Filters) > 1 {
		comp, hasComposite = r.index().MatchEdgeComposite(query.Filters, query.FilterMode)
	}

	// Four possible drivers, costed on the same scale. An unavailable driver is
	// MaxInt so it can never win a comparison, which keeps the cases below free
	// of sentinel checks.
	//
	// The ordered driver is the one that is new here. It used to sit below this
	// switch, reachable only when equality, adjacency and labels were all
	// absent — so `Types=[X] AND ts BETWEEN a AND b` drove from every edge
	// labelled X even when the range matched three of them.
	eqCost := driverUnavailable
	if bestFilter != nil {
		eqCost = bestSize
	}
	anchorCost := driverUnavailable
	if anchorSize >= 0 {
		anchorCost = anchorSize
	}
	orderedCost := driverUnavailable
	if bestOrdered != nil {
		orderedCost = orderedSize
	}
	labelCost := driverUnavailable
	if len(query.Types) > 0 {
		labelCost = r.edgeLabelCandidateCount(query.Types)
	}
	compositeCost := driverUnavailable
	if hasComposite {
		compositeCost = comp.Size
	}

	switch {
	// Labels must beat every alternative strictly: they return unsorted
	// candidates, where equality, adjacency, ordered and composite all return
	// ascending.
	case labelCost < eqCost && labelCost < anchorCost && labelCost < orderedCost &&
		labelCost < compositeCost:
		return driveEdgeLabels(r, query.Types)
	// Ties go to the composite, for the reason given in driveNodeCandidates.
	case hasComposite && compositeCost <= eqCost && compositeCost <= anchorCost &&
		compositeCost <= orderedCost:
		if ids, served := r.index().EdgesByComposite(comp); served {
			return liveEdgeIDs(r, ids), true, store.QueryPlan{
				Driver:        store.DriverComposite,
				DriverKey:     comp.Name(),
				DriverFilters: comp.Filters,
			}
		}
	case bestFilter != nil && eqCost <= anchorCost && eqCost <= orderedCost:
		return liveEdgeIDs(r, r.index().EdgesByProperty(bestFilter.Key, bestFilter.Value)), true, store.QueryPlan{
			Driver:        store.DriverEquality,
			DriverKey:     bestFilter.Key,
			DriverFilters: store.FilterMaskOf(query.Filters, *bestFilter),
		}
	case anchorSize >= 0 && anchorCost <= orderedCost:
		return r.incidentEdgeIDsForAll(anchors, anchorDir), false, store.QueryPlan{
			Driver: store.DriverAdjacency,
		}
	case bestOrdered != nil:
		// See driveNodeCandidates: served was established when this driver was
		// costed, so the fallback is unreachable and kept only because the
		// contract permits it.
		if ids, served := r.index().EdgesMatchingOrdered(nil, *bestOrdered); served {
			return liveEdgeIDs(r, sortDedupeEdgeIDs(ids)), true, store.QueryPlan{
				Driver:        store.DriverOrdered,
				DriverKey:     bestOrdered.Key,
				DriverFilters: store.FilterMaskOf(query.Filters, *bestOrdered),
			}
		}
	}

	if len(query.Types) > 0 {
		return driveEdgeLabels(r, query.Types)
	}

	return r.allEdgeIDs(), false, store.QueryPlan{Driver: store.DriverScan}
}

// driveEdgeLabels is the edge counterpart of driveNodeLabels: the union of the
// given labels' postings, served by the CSR label index plus the delta, and
// unsorted because a union over several labels interleaves ascending runs.
func driveEdgeLabels(r reader, types []store.EdgeType) ([]store.EdgeID, bool, store.QueryPlan) {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	for _, t := range types {
		for _, id := range r.edgesByType(t) {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out, false, store.QueryPlan{Driver: store.DriverLabels}
}

// collectCandidateNodeIDs resolves an explicit ID list against r, dropping the
// ones that are not live and deduplicating what remains.
func collectCandidateNodeIDs(r reader, ids []store.NodeID) []store.NodeID {
	out := make([]store.NodeID, 0, len(ids))
	seen := make(map[store.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		if !r.nodeExists(id) {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func collectCandidateEdgeIDs(r reader, ids []store.EdgeID) []store.EdgeID {
	out := make([]store.EdgeID, 0, len(ids))
	seen := make(map[store.EdgeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		if !r.edgeExists(id) {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
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
func (s *Store) matchNodeIDsByFilters(r reader, filters []store.PropertyFilter, mode store.MatchMode) []store.NodeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.NodeID
	for i, f := range filters {
		set := s.matchOneNodeFilter(r, f)
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
func (s *Store) matchOneNodeFilter(r reader, f store.PropertyFilter) []store.NodeID {
	if f.Op == store.PropertyOpEqual {
		// Postings are already ascending and deduplicated.
		return r.index().NodesByProperty(f.Key, f.Value)
	}
	// A key declared ordered answers ranges and prefixes by binary search. Its
	// comparison is byte-wise, so the whole predicate is resolved there — mixing
	// it with the scan matcher below would apply two orderings to one key.
	if ids, served := r.index().NodesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	// Otherwise scan only the buckets belonging to this key, never the whole index.
	// One comparison per distinct value, not per entry.
	var out []store.NodeID
	r.index().ForEachNodeValue(f.Key, func(value []byte, ids []store.NodeID) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, ids...)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

// matchEdgeIDsByFilters is matchNodeIDsByFilters for edge properties.
func (s *Store) matchEdgeIDsByFilters(r reader, filters []store.PropertyFilter, mode store.MatchMode) []store.EdgeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.EdgeID
	for i, f := range filters {
		set := s.matchOneEdgeFilter(r, f)
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

func (s *Store) matchOneEdgeFilter(r reader, f store.PropertyFilter) []store.EdgeID {
	if f.Op == store.PropertyOpEqual {
		return r.index().EdgesByProperty(f.Key, f.Value)
	}
	if ids, served := r.index().EdgesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	var out []store.EdgeID
	r.index().ForEachEdgeValue(f.Key, func(value []byte, ids []store.EdgeID) bool {
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
	// One lock hold, and the query runs through the internal entry point rather
	// than the public one: re-entering QueryNodeIDs would take the read lock a
	// second time, which a Go RWMutex does not promise to grant while a writer
	// is queued behind the first.
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()

	candidates, _, plan := s.driveNodeCandidates(r, query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = r.index().PlanNodeResiduals(query.Filters, plan.DriverFilters, len(candidates))
	}
	ids, err := s.queryNodeIDs(r, query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}

// ExplainEdgeQuery is ExplainNodeQuery for edge queries.
func (s *Store) ExplainEdgeQuery(query store.EdgeQuery) (store.QueryPlan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()

	candidates, _, plan := s.driveEdgeCandidates(r, query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = r.index().PlanEdgeResiduals(query.Filters, plan.DriverFilters, len(candidates))
	}
	ids, err := s.queryEdgeIDs(r, query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}
