package index

import "github.com/aoiflux/graphene/store"

// Cardinality estimates for range and prefix filters.
//
// The planner picks a driver by comparing candidate-set sizes on one scale. An
// equality filter supplies its size exactly — the postings list length is a map
// lookup. A label supplies its size exactly too. A range or prefix filter used
// to supply nothing at all, so it could not enter the comparison, and both
// planners worked around that by position rather than by cost: the node planner
// took the first ordered filter that could be served, ahead of any label; the
// edge planner tried ordered filters only after every other driver had been
// ruled out. One over-used ranges, the other under-used them, and neither was
// deciding anything — the query's filter order was.
//
// What the ordered index already knows is enough to fix that. rangeFor resolves
// a filter to a [lo, hi) window over the distinct values with two binary
// searches. The number of *entries* in that window is the estimate the planner
// wants, and this file is how it gets one without walking the window.
//
// # Two regimes, because they have different jobs
//
// A narrow window is counted exactly. It costs one len() per distinct value,
// touches no ID, and it is the case where precision decides something: a range
// matching 3 of 900 000 entries has to beat every other driver, and rounding it
// to a mean would throw away exactly the fact that makes it win.
//
// A wide window is estimated as the window's share of the distinct values times
// the mean postings length. It is O(1), and precision there buys nothing: a
// driver estimated at 40% of the key is losing to any label or equality filter
// whatever the true figure is, and the only comparison a wide window can win is
// against a full scan, which it wins on any estimate.
//
// # Where the estimate is wrong, stated plainly
//
// The wide-window estimate assumes postings lengths are roughly even across the
// key. A key whose values are near-unique — a hash, a timestamp, an offset, most
// of what a forensic graph indexes — has a mean of about 1, so the estimate is
// almost exact. A key with one value holding most of the entries breaks the
// assumption, and a wide window that happens to contain that value is
// under-estimated. That cannot make a result wrong: the driver is a superset
// whatever its size, and every filter is still applied. It can make the planner
// pick a worse driver than it would have with a full count, which is the trade
// the exact-window threshold is placed to bound.
//
// This is deliberately not a histogram. A histogram would put the same numbers
// in a persisted structure that has to be built, maintained, versioned into the
// image, and invalidated when it drifts; the ordered index is already the sorted
// structure a histogram would be built from, so reading it costs less than
// summarising it and can never be stale. Should skew show up as a real misplan
// this is the place a per-key distribution would go, behind the same two calls.

// exactWindowValues is the widest value window counted exactly rather than
// estimated.
//
// The count is one len() per distinct value over contiguous memory, so the work
// is a few cache lines at this width — cheaper than the map lookup the caller
// already did to find the index. Above it, the estimate is used, on the argument
// in the file comment: precision stops deciding anything once a window is wide.
const exactWindowValues = 64

// estimateRange returns the number of entries in values[lo:hi), exactly for a
// narrow window and estimated for a wide one.
func (o *orderedIndex[T]) estimateRange(lo, hi int) int {
	if lo < 0 {
		lo = 0
	}
	if hi > len(o.values) {
		hi = len(o.values)
	}
	if hi <= lo {
		return 0
	}
	width := hi - lo
	// A window covering the whole key is the total, with no arithmetic and no
	// rounding — the commonest wide case, and the one an unbounded prefix or a
	// range over every value produces.
	if width == len(o.values) {
		return o.totalIDs
	}
	if width <= exactWindowValues {
		n := 0
		for i := lo; i < hi; i++ {
			n += len(o.values[i].ids)
		}
		return n
	}
	// Mean postings length times the window width, rounded up so that a window
	// holding anything at all never estimates zero: zero is the answer for an
	// empty window and has to stay distinguishable from "few".
	est := (o.totalIDs*width + len(o.values) - 1) / len(o.values)
	if est > o.totalIDs {
		est = o.totalIDs
	}
	if est < 1 {
		est = 1
	}
	return est
}

// NodeRangeCardinality estimates how many node entries satisfy f.
//
// served is false when f.Key is not declared ordered or when f.Op is one no
// ordering can answer, which is the same condition NodesMatchingOrdered reports:
// a caller that gets false here would get false there, so the planner can use
// this to decide whether to drive from f without first resolving it.
func (p *PropertyIndex) NodeRangeCardinality(f store.PropertyFilter) (int, bool) {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	idx := sh.orderedNodeKeys[f.Key]
	if idx == nil {
		return 0, false
	}
	lo, hi, ok := idx.rangeFor(f)
	if !ok {
		return 0, false
	}
	return idx.estimateRange(lo, hi), true
}

// EdgeRangeCardinality is NodeRangeCardinality for edge properties.
func (p *PropertyIndex) EdgeRangeCardinality(f store.PropertyFilter) (int, bool) {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	idx := sh.orderedEdgeKeys[f.Key]
	if idx == nil {
		return 0, false
	}
	lo, hi, ok := idx.rangeFor(f)
	if !ok {
		return 0, false
	}
	return idx.estimateRange(lo, hi), true
}
