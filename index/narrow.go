package index

import (
	"context"
	"math/bits"
	"sort"

	"github.com/aoiflux/graphene/store"
)

// Residual filter evaluation.
//
// A query that reaches here has already been driven: the planner picked the most
// selective filter it could serve from an index and produced a candidate set.
// What remains is applying the *other* filters.
//
// Resolving each of those to its own full set and intersecting is the wrong
// shape when the candidate set is small. A filter the index cannot serve — a
// Contains, or a range on a key that was never declared ordered — costs a scan
// of every entry registered under its key. Doing that to eliminate candidates
// from a set of three is work proportional to the graph in service of an answer
// proportional to nothing.
//
// So each residual filter is costed both ways and evaluated whichever way is
// cheaper: probe the candidates through the reverse map, or build the set and
// intersect. Filters are taken most-selective-first so that candidates die as
// early as possible, and the pass stops the moment nothing is left.

// NarrowNodesByFilters returns the ascending subset of candidates matching every
// filter. candidates must be ascending and deduplicated; the result is too.
//
// **The candidates slice is consumed.** Filtering happens in place and the
// result reuses its backing array, so the caller must not use candidates
// afterwards — the same contract store.IntersectSortedIDs carries, for the same
// reason.
//
// skip marks the filters already applied to produce the candidates. They are not
// re-evaluated. It is a set rather than one index because a composite driver
// consumes its whole key tuple.
//
// This implements MatchAll only. Under MatchAny a candidate set driven by one
// filter is not a superset of the answer, so there is nothing to narrow.
func (p *PropertyIndex) NarrowNodesByFilters(candidates []store.NodeID, filters []store.PropertyFilter, skip store.FilterMask) []store.NodeID {
	out, _ := p.NarrowNodesByFiltersCtx(context.Background(), candidates, filters, skip)
	return out
}

// NarrowNodesByFiltersCtx is NarrowNodesByFilters, abandoned if ctx is
// cancelled. It returns no candidates alongside the error: a residual pass
// stopped part way has applied some filters and not others, so what it holds is
// a superset of the answer that looks exactly like the answer.
func (p *PropertyIndex) NarrowNodesByFiltersCtx(ctx context.Context, candidates []store.NodeID, filters []store.PropertyFilter, skip store.FilterMask) ([]store.NodeID, error) {
	if noResiduals(filters, skip) {
		return candidates, nil
	}
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	costs := p.nodeCosts()
	plan := p.planResiduals(filters, skip, costs)
	for _, step := range plan {
		if len(candidates) == 0 {
			return candidates, nil
		}
		// Checked unamortised between steps as well as inside them: one step is a
		// pass over every candidate or a scan of a whole key, so it is expensive
		// enough to be worth not starting.
		if err := cc.Check(); err != nil {
			return nil, err
		}
		// Decided here rather than when the plan was built: each step shrinks the
		// candidate set, so a filter that was not worth probing against the
		// original count often is against what survived the step before it.
		if probeIsCheaper(len(candidates), step.step.Cost, costs.probe) {
			var err error
			candidates, err = p.probeNodes(candidates, step.filter, &cc)
			if err != nil {
				return nil, err
			}
			continue
		}
		matched, err := p.matchNodes(step.filter, &cc)
		if err != nil {
			return nil, err
		}
		candidates = store.IntersectSortedIDs(candidates, matched)
	}
	return candidates, nil
}

// NarrowEdgesByFilters is NarrowNodesByFilters for edge properties, and consumes
// its candidates slice in the same way.
func (p *PropertyIndex) NarrowEdgesByFilters(candidates []store.EdgeID, filters []store.PropertyFilter, skip store.FilterMask) []store.EdgeID {
	out, _ := p.NarrowEdgesByFiltersCtx(context.Background(), candidates, filters, skip)
	return out
}

// NarrowEdgesByFiltersCtx is NarrowNodesByFiltersCtx for edge properties.
func (p *PropertyIndex) NarrowEdgesByFiltersCtx(ctx context.Context, candidates []store.EdgeID, filters []store.PropertyFilter, skip store.FilterMask) ([]store.EdgeID, error) {
	if noResiduals(filters, skip) {
		return candidates, nil
	}
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return nil, err
	}
	costs := p.edgeCosts()
	plan := p.planResiduals(filters, skip, costs)
	for _, step := range plan {
		if len(candidates) == 0 {
			return candidates, nil
		}
		if err := cc.Check(); err != nil {
			return nil, err
		}
		if probeIsCheaper(len(candidates), step.step.Cost, costs.probe) {
			var err error
			candidates, err = p.probeEdges(candidates, step.filter, &cc)
			if err != nil {
				return nil, err
			}
			continue
		}
		matched, err := p.matchEdges(step.filter, &cc)
		if err != nil {
			return nil, err
		}
		candidates = store.IntersectSortedIDs(candidates, matched)
	}
	return candidates, nil
}

// residualStep is one filter plus how it was decided to be applied.
type residualStep struct {
	filter store.PropertyFilter
	step   store.ResidualStep
}

// PlanNodeResiduals reports the residual filters in the order they will be
// applied, with the cost estimate for each. It backs Graph.ExplainNodeQuery.
//
// `Probe` is reported as the decision would be made at the *start* of the
// residual pass, against candidateCount. The executor re-decides it per step,
// because each step shrinks the candidate set — so a filter reported here as
// building its own set may end up probed once an earlier filter has thinned the
// candidates. The order and the costs are exact; that one flag is a forecast.
func (p *PropertyIndex) PlanNodeResiduals(filters []store.PropertyFilter, skip store.FilterMask, candidateCount int) []store.ResidualStep {
	costs := p.nodeCosts()
	return exportSteps(p.planResiduals(filters, skip, costs), candidateCount, costs.probe)
}

// PlanEdgeResiduals is PlanNodeResiduals for edge properties.
func (p *PropertyIndex) PlanEdgeResiduals(filters []store.PropertyFilter, skip store.FilterMask, candidateCount int) []store.ResidualStep {
	costs := p.edgeCosts()
	return exportSteps(p.planResiduals(filters, skip, costs), candidateCount, costs.probe)
}

func exportSteps(plan []residualStep, candidateCount, probeCost int) []store.ResidualStep {
	if len(plan) == 0 {
		return nil
	}
	out := make([]store.ResidualStep, len(plan))
	for i, s := range plan {
		out[i] = s.step
		out[i].Probe = probeIsCheaper(candidateCount, s.step.Cost, probeCost)
	}
	return out
}

// filterCosts is the three ways a filter's set size can be sized, bundled so
// that adding one does not touch four call sites.
type filterCosts struct {
	// keyCount is the number of entries registered under a key: what a scan of
	// that key would touch, and an upper bound on any predicate over it.
	keyCount func(string) int
	// cardinality is an equality filter's exact postings length.
	cardinality func(string, []byte) int
	// rangeCardinality sizes a range or prefix from the ordered index, and
	// reports false for a key that has none — in which case keyCount is all
	// there is.
	rangeCardinality func(store.PropertyFilter) (int, bool)
	// probe is what one probe of one candidate costs, in the same unit the three
	// above are counted in: one entry touched. It is a number rather than a
	// function because it does not vary by filter — it is a property of where
	// this kind's reverse direction lives. See reverseProbeCost.
	probe int
}

func (p *PropertyIndex) nodeCosts() filterCosts {
	return filterCosts{p.nodeKeyEntryCount, p.nodeCardinality, p.NodeRangeCardinality, p.nodeProbeCost()}
}

func (p *PropertyIndex) edgeCosts() filterCosts {
	return filterCosts{p.edgeKeyEntryCount, p.edgeCardinality, p.EdgeRangeCardinality, p.edgeProbeCost()}
}

// nodeProbeCost is what probing one node candidate costs.
//
// One atomic load for a store with no base, and the constant it then returns is
// what this whole cost model assumed before there were bases.
func (p *PropertyIndex) nodeProbeCost() int {
	if st := p.baseRef.Load(); st != nil {
		return st.nodeProbe
	}
	return 1
}

// edgeProbeCost is nodeProbeCost for edges. The two kinds are sized separately
// because they are separate reverse arrays: a store with two million edge entries
// and four hundred node entries should not plan its node residuals as though a
// probe cost what an edge probe costs.
func (p *PropertyIndex) edgeProbeCost() int {
	if st := p.baseRef.Load(); st != nil {
		return st.edgeProbe
	}
	return 1
}

// planResiduals costs every filter both ways and orders them most-selective-first.
//
// The estimate for an equality filter is its exact postings cardinality. A range
// or prefix on a key declared ordered is sized from that index — see stats.go,
// which is what makes a selective range sort ahead of a weak equality filter
// instead of behind every one of them.
//
// Anything else falls back to the number of entries under the key, which is what
// a scan of that key would touch and therefore an upper bound on the matches.
// It is a loose bound, and deliberately still a bound: without an ordering there
// is nothing cheaper than a scan that could tighten it, and guessing a
// selectivity would order the steps on a number with no evidence behind it.
//
// All of these are map lookups and binary searches, not scans, so planning stays
// cheap relative to any decision it makes.
func (p *PropertyIndex) planResiduals(
	filters []store.PropertyFilter,
	skip store.FilterMask,
	costs filterCosts,
) []residualStep {
	steps := make([]residualStep, 0, len(filters)-skip.Count())
	for i, f := range filters {
		if skip.Has(i) {
			continue
		}
		cost := 0
		switch {
		case f.Op == store.PropertyOpEqual:
			cost = costs.cardinality(f.Key, f.Value)
		default:
			if est, served := costs.rangeCardinality(f); served {
				cost = est
			} else {
				cost = costs.keyCount(f.Key)
			}
		}
		steps = append(steps, residualStep{
			filter: f,
			step:   store.ResidualStep{Key: f.Key, Op: f.Op, Cost: cost},
		})
	}
	// Ascending estimated match count: the filter most likely to empty the
	// candidate set runs first, and the loop stops as soon as it does.
	if len(steps) > 1 {
		sort.SliceStable(steps, func(i, j int) bool { return steps[i].step.Cost < steps[j].step.Cost })
	}
	return steps
}

// probeNodes keeps the candidates whose own registered values satisfy f,
// preserving order.
func (p *PropertyIndex) probeNodes(candidates []store.NodeID, f store.PropertyFilter, cc *store.CancelCheck) ([]store.NodeID, error) {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	ordered := sh.orderedNodeKeys[f.Key] != nil
	// A probe reads the reverse direction, and the delta's reverse direction knows
	// only what has been written since the last compaction — so under a base the
	// delta saying "no entry under this key" is not an answer. See
	// baseSide.entryMatches.
	s, hasBase := p.nodeBase()
	out := candidates[:0]
	for _, id := range candidates {
		if err := cc.Step(); err != nil {
			return nil, err
		}
		if postingsMatch(&sh.nodes, id, f, ordered) ||
			(hasBase && s.entryMatches(uint64(id), f, ordered)) {
			out = append(out, id)
		}
	}
	return out, nil
}

// probeEdges is probeNodes for edge properties.
func (p *PropertyIndex) probeEdges(candidates []store.EdgeID, f store.PropertyFilter, cc *store.CancelCheck) ([]store.EdgeID, error) {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	ordered := sh.orderedEdgeKeys[f.Key] != nil
	s, hasBase := p.edgeBase() // see probeNodes
	out := candidates[:0]
	for _, id := range candidates {
		if err := cc.Step(); err != nil {
			return nil, err
		}
		if postingsMatch(&sh.edges, id, f, ordered) ||
			(hasBase && s.entryMatches(uint64(id), f, ordered)) {
			out = append(out, id)
		}
	}
	return out, nil
}

// refsMatch reports whether any value registered under f.Key satisfies f.
//
// An entity with no entry for the key does not match, which is what the forward
// path does too: it could only ever return entities present in that key's
// postings.
//
// ordered selects the comparison rule, and getting it wrong here would make the
// probe disagree with the index for exactly the keys where a range query is
// fastest — see store.PropertyFilterMatchesOrdered.
func postingsMatch[T entityID](p *postings[T], id T, f store.PropertyFilter, ordered bool) bool {
	matched := false
	p.forEachRef(id, func(ref propRef) bool {
		if p.keyName(ref.keyID) != f.Key {
			return true
		}
		// unsafeBytes, not a conversion: this runs once per candidate, and a copy
		// here made the probe path allocate proportionally to the candidate set
		// it exists to avoid materialising. The predicates below only read it,
		// which is the same contract the scan path already relies on.
		if filterMatchesValue(f, unsafeBytes(ref.value), ordered) {
			matched = true
			return false
		}
		return true
	})
	return matched
}

// matchNodes resolves one filter to its own ascending, deduplicated set.
func (p *PropertyIndex) matchNodes(f store.PropertyFilter, cc *store.CancelCheck) ([]store.NodeID, error) {
	if f.Op == store.PropertyOpEqual {
		return p.NodesByProperty(f.Key, f.Value), nil
	}
	if ids, served := p.NodesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids), nil
	}
	// One comparison per distinct value, not per entry: the predicate reads only
	// the value, so every id sharing it gets the same answer.
	var out []store.NodeID
	var cerr error
	p.ForEachNodeValue(f.Key, func(value []byte, ids []store.NodeID) bool {
		if cerr = cc.Step(); cerr != nil {
			return false
		}
		if store.PropertyFilterMatches(f, value) {
			out = append(out, ids...)
		}
		return true
	})
	if cerr != nil {
		return nil, cerr
	}
	return store.SortDedupeIDs(out), nil
}

// matchEdges is matchNodes for edge properties.
func (p *PropertyIndex) matchEdges(f store.PropertyFilter, cc *store.CancelCheck) ([]store.EdgeID, error) {
	if f.Op == store.PropertyOpEqual {
		return p.EdgesByProperty(f.Key, f.Value), nil
	}
	if ids, served := p.EdgesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids), nil
	}
	var out []store.EdgeID
	var cerr error
	p.ForEachEdgeValue(f.Key, func(value []byte, ids []store.EdgeID) bool {
		if cerr = cc.Step(); cerr != nil {
			return false
		}
		if store.PropertyFilterMatches(f, value) {
			out = append(out, ids...)
		}
		return true
	})
	if cerr != nil {
		return nil, cerr
	}
	return store.SortDedupeIDs(out), nil
}

func (p *PropertyIndex) nodeCardinality(key string, value []byte) int {
	return p.NodeCardinality(key, value)
}

func (p *PropertyIndex) edgeCardinality(key string, value []byte) int {
	return p.EdgeCardinality(key, value)
}

// nodeKeyEntryCount is the number of (id, value) entries registered under key,
// which is what a scan of that key would visit.
//
// Under a base it includes the base's entries for the key, which the base reports
// from its key directory without being walked. It is an upper bound, for the same
// reason NodeCardinality is: what it decides is probe versus build, and a count
// that is too high makes the wrong choice, never the wrong answer.
func (p *PropertyIndex) nodeKeyEntryCount(key string) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	n := sh.nodes.perKey[key]
	sh.mu.RUnlock()
	if s, hasBase := p.nodeBase(); hasBase {
		return s.mergeKeyEntryCount(key, n)
	}
	return n
}

func (p *PropertyIndex) edgeKeyEntryCount(key string) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	n := sh.edges.perKey[key]
	sh.mu.RUnlock()
	if s, hasBase := p.edgeBase(); hasBase {
		return s.mergeKeyEntryCount(key, n)
	}
	return n
}

// noResiduals reports that the driver already applied every filter, so there is
// no residual pass to plan.
//
// This is the single-filter query, which is also the most common one, and
// planning it allocated a slice and consulted the index to conclude there was
// nothing to do — measured at +14% and +70% allocations before this check
// existed. A composite driver reaches it too, and for the same reason: it
// consumes every filter it was matched on.
func noResiduals(filters []store.PropertyFilter, skip store.FilterMask) bool {
	return len(filters) == skip.Count()
}

// probeIsCheaper compares the two ways to apply one residual filter: probing
// costs probeCost per candidate, while building the filter's own set costs its
// size plus a merge.
//
// probeCost was implicitly one, and was right to be, for as long as the reverse
// direction was a map in the heap: a probe was one map lookup and a set element
// was one slice element, so counting them in the same unit compared like with
// like. Under a mapped base a probe is not a lookup at all — it is a binary
// search of a disk-resident array — and charging it one unit made residual
// planning prefer it in exactly the band where it had become the expensive
// option. See reverseProbeCost for what it charges instead.
//
// The cap is the other half of the same decision, and it is a footprint bound
// rather than a cost estimate. Building the set materialises setSize ids;
// probing materialises nothing at all. A probeCost of twenty-five would
// otherwise license building a set twenty-five times the candidate slice the
// caller already holds — two hundred megabytes against an eight-megabyte
// candidate set, which is the shape of thing this whole program exists to
// remove. Past the cap the probe is chosen however slow it is, and that is the
// trade this program sanctions: bounded and slower beats fast and unbounded.
func probeIsCheaper(candidates, setSize, probeCost int) bool {
	if setSize > residualBuildCap {
		return true
	}
	// candidateCount reaches here from PlanNodeResiduals, which takes it from a
	// caller; a negative one would otherwise widen to an enormous unsigned
	// product and forecast a build for every filter.
	if candidates < 0 {
		candidates = 0
	}
	// Widened because this is the one place in the file where two counts
	// multiply. candidates is bounded by the entity count and probeCost by the
	// width of an int, so the product cannot overflow sixty-four bits — but it
	// can overflow a thirty-two-bit int, and an overflowed comparison here is a
	// planner that picks at random.
	return uint64(candidates)*uint64(probeCost) < uint64(setSize)
}

// residualBuildCap bounds what one residual filter may materialise instead of
// probing, in ids: 4Mi ids is 32 MiB of transient slice.
//
// It is deliberately well above the sizes where the cost comparison above is
// interesting and well below the sizes that threaten a budget, so that it changes
// no decision that was being made on the merits. A set larger than this was
// already probed rather than built for every candidate count under the cap, so
// the cap only ever overrides the case where the candidate set is larger still —
// where the alternative is holding two enormous id slices at once in order to
// intersect them.
const residualBuildCap = 1 << 22

// reverseProbeCost is what one probe of one candidate costs against a base whose
// reverse direction holds this many entries, counted in the unit planResiduals
// sizes a filter's set in: one entry touched.
//
// A probe of the base is gpirLowerBound — a binary search by id over a
// disk-resident array — so it is bits.Len(entries) reads, each of them its own
// cache line and, on an image that has not been read yet, its own page. At the
// twenty-eight million entries this program is aimed at that is twenty-five,
// against the one the model charged.
//
// Derived rather than fixed at a constant, because the constant would be wrong at
// both ends. A store whose image holds a thousand entries charges ten, not
// twenty-five, so a small base does not push every residual onto the forward
// path; a store with no base charges one, which is what the model did before
// bases existed and what the memory backend still does.
//
// What it deliberately does not count: the walk of the entry run the search lands
// on. The run is contiguous in the reverse array, but resolving each of its
// entries reads a value out of its own key's runs, so an entity carrying thirteen
// entries pays something closer to thirteen further reads. Leaving that out
// understates the probe, which is the safe direction — it keeps this from moving
// a decision that the search term alone does not justify moving.
func reverseProbeCost(entries int) int {
	if entries <= 0 {
		return 1
	}
	return bits.Len(uint(entries))
}
