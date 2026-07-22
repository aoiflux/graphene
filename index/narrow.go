package index

import (
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
// skip names a filter already applied to produce candidates, by index into
// filters, or -1. It is not re-evaluated.
//
// This implements MatchAll only. Under MatchAny a candidate set driven by one
// filter is not a superset of the answer, so there is nothing to narrow.
func (p *PropertyIndex) NarrowNodesByFilters(candidates []store.NodeID, filters []store.PropertyFilter, skip int) []store.NodeID {
	if noResiduals(filters, skip) {
		return candidates
	}
	plan := p.planResiduals(filters, skip, p.nodeKeyEntryCount, p.nodeCardinality)
	for _, step := range plan {
		if len(candidates) == 0 {
			return candidates
		}
		// Decided here rather than when the plan was built: each step shrinks the
		// candidate set, so a filter that was not worth probing against the
		// original count often is against what survived the step before it.
		if probeIsCheaper(len(candidates), step.step.Cost) {
			candidates = p.probeNodes(candidates, step.filter)
			continue
		}
		candidates = store.IntersectSortedIDs(candidates, p.matchNodes(step.filter))
	}
	return candidates
}

// NarrowEdgesByFilters is NarrowNodesByFilters for edge properties, and consumes
// its candidates slice in the same way.
func (p *PropertyIndex) NarrowEdgesByFilters(candidates []store.EdgeID, filters []store.PropertyFilter, skip int) []store.EdgeID {
	if noResiduals(filters, skip) {
		return candidates
	}
	plan := p.planResiduals(filters, skip, p.edgeKeyEntryCount, p.edgeCardinality)
	for _, step := range plan {
		if len(candidates) == 0 {
			return candidates
		}
		if probeIsCheaper(len(candidates), step.step.Cost) {
			candidates = p.probeEdges(candidates, step.filter)
			continue
		}
		candidates = store.IntersectSortedIDs(candidates, p.matchEdges(step.filter))
	}
	return candidates
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
func (p *PropertyIndex) PlanNodeResiduals(filters []store.PropertyFilter, skip, candidateCount int) []store.ResidualStep {
	return exportSteps(p.planResiduals(filters, skip, p.nodeKeyEntryCount, p.nodeCardinality), candidateCount)
}

// PlanEdgeResiduals is PlanNodeResiduals for edge properties.
func (p *PropertyIndex) PlanEdgeResiduals(filters []store.PropertyFilter, skip, candidateCount int) []store.ResidualStep {
	return exportSteps(p.planResiduals(filters, skip, p.edgeKeyEntryCount, p.edgeCardinality), candidateCount)
}

func exportSteps(plan []residualStep, candidateCount int) []store.ResidualStep {
	if len(plan) == 0 {
		return nil
	}
	out := make([]store.ResidualStep, len(plan))
	for i, s := range plan {
		out[i] = s.step
		out[i].Probe = probeIsCheaper(candidateCount, s.step.Cost)
	}
	return out
}

// planResiduals costs every filter both ways and orders them most-selective-first.
//
// The estimate for an equality filter is its exact postings cardinality. For
// anything else it is the number of entries under the key, which is what a scan
// of that key would touch and therefore an upper bound on the matches. Both are
// map lookups, not scans, so planning is cheap relative to any decision it makes.
func (p *PropertyIndex) planResiduals(
	filters []store.PropertyFilter,
	skip int,
	keyCount func(string) int,
	cardinality func(string, []byte) int,
) []residualStep {
	n := len(filters)
	if skip >= 0 {
		n--
	}
	steps := make([]residualStep, 0, n)
	for i, f := range filters {
		if i == skip {
			continue
		}
		cost := 0
		if f.Op == store.PropertyOpEqual {
			cost = cardinality(f.Key, f.Value)
		} else {
			cost = keyCount(f.Key)
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
func (p *PropertyIndex) probeNodes(candidates []store.NodeID, f store.PropertyFilter) []store.NodeID {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	ordered := sh.orderedNodeKeys[f.Key] != nil
	out := candidates[:0]
	for _, id := range candidates {
		if postingsMatch(&sh.nodes, id, f, ordered) {
			out = append(out, id)
		}
	}
	return out
}

// probeEdges is probeNodes for edge properties.
func (p *PropertyIndex) probeEdges(candidates []store.EdgeID, f store.PropertyFilter) []store.EdgeID {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	ordered := sh.orderedEdgeKeys[f.Key] != nil
	out := candidates[:0]
	for _, id := range candidates {
		if postingsMatch(&sh.edges, id, f, ordered) {
			out = append(out, id)
		}
	}
	return out
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
		v := []byte(ref.value)
		if ordered {
			if store.PropertyFilterMatchesOrdered(f, v) {
				matched = true
				return false
			}
			return true
		}
		if store.PropertyFilterMatches(f, v) {
			matched = true
			return false
		}
		return true
	})
	return matched
}

// matchNodes resolves one filter to its own ascending, deduplicated set.
func (p *PropertyIndex) matchNodes(f store.PropertyFilter) []store.NodeID {
	if f.Op == store.PropertyOpEqual {
		return p.NodesByProperty(f.Key, f.Value)
	}
	if ids, served := p.NodesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	// One comparison per distinct value, not per entry: the predicate reads only
	// the value, so every id sharing it gets the same answer.
	var out []store.NodeID
	p.ForEachNodeValue(f.Key, func(value []byte, ids []store.NodeID) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, ids...)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

// matchEdges is matchNodes for edge properties.
func (p *PropertyIndex) matchEdges(f store.PropertyFilter) []store.EdgeID {
	if f.Op == store.PropertyOpEqual {
		return p.EdgesByProperty(f.Key, f.Value)
	}
	if ids, served := p.EdgesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	var out []store.EdgeID
	p.ForEachEdgeValue(f.Key, func(value []byte, ids []store.EdgeID) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, ids...)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

func (p *PropertyIndex) nodeCardinality(key string, value []byte) int {
	return p.NodeCardinality(key, value)
}

func (p *PropertyIndex) edgeCardinality(key string, value []byte) int {
	return p.EdgeCardinality(key, value)
}

// nodeKeyEntryCount is the number of (id, value) entries registered under key,
// which is what a scan of that key would visit.
func (p *PropertyIndex) nodeKeyEntryCount(key string) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.nodes.perKey[key]
}

func (p *PropertyIndex) edgeKeyEntryCount(key string) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.edges.perKey[key]
}

// noResiduals reports that the driver already applied every filter, so there is
// no residual pass to plan.
//
// This is the single-filter query, which is also the most common one, and
// planning it allocated a slice and consulted the index to conclude there was
// nothing to do — measured at +14% and +70% allocations before this check
// existed.
func noResiduals(filters []store.PropertyFilter, skip int) bool {
	return len(filters) == 0 || (len(filters) == 1 && skip == 0)
}

// probeIsCheaper compares the two ways to apply one residual filter: probing
// costs a reverse-map lookup per candidate, while building the filter's own set
// costs its size plus a merge.
func probeIsCheaper(candidates, setSize int) bool {
	return candidates < setSize
}
