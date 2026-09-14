package index

// Reading base ∪ delta − retracted.
//
// retract.go argues why the three terms are shaped the way they are. This file
// is the arithmetic: one merge per read path, written once over T and reached
// from the public methods in property_index.go, unique_index.go, narrow.go and
// stats.go by a single branch on whether a base is attached.
//
// # Two walks in opposite styles
//
// The delta's values are a map, or a sorted slice the caller has already built.
// The base's are pushed through a callback, because a run of ids is a view into
// the image and handing one back would mean either copying it or letting it
// escape. So every merge here is written from the base's side: the callback
// drains the delta up to the value it was handed, merges that value's two
// postings lists, and whatever the delta has left over is emitted after the base
// walk ends. One pass over each side, and no value's ids held twice.
//
// # What a merged result costs
//
// The public read paths already copy: NodesByProperty hands back a fresh slice
// and always did, because a caller may keep it. So a merge costs the same
// allocation as a lookup without a base, sized from the two sides rather than
// one, and nothing that was free becomes expensive. The walks — ForEachNodeValue
// and its relatives — are the exception: they hand out the index's own memory,
// and a value held by both sides has no single array to hand out. Those merge
// into a scratch buffer that is reused across values and across keys, so a walk
// allocates once for the widest value it meets rather than once per value.
//
// # What a base that breaks its own order does here
//
// Every merge below assumes the base's runs ascend and its values ascend, which
// is the base's promise and not something re-checked per probe — the same
// position IDRun.Contains and disk's gpixBase take, and for the same reason: a
// reader that re-verified would make every query pay for the file being sound.
// A base that broke it would make a merged result mis-ordered or a delta value
// visited twice. Bounded, never out of range — every walk here is guarded by its
// own slice length, not by the base's claims — but wrong. Establishing the order
// over a whole section is an O(entries) pass, and that pass is the bounded
// verifier's (R3e), which is where a file that lies about its order is named.
//
// # A shard lock is held across a base read, and that is deliberate
//
// The walks hold a shard read lock for the whole walk — they always have, and
// their doc comments say so, because the ids they hand out are the shard's. Under
// a base the walk also touches the image, so a page fault now stalls writers to
// that one shard. The alternative is to copy the delta's side of every key out
// from under the lock before touching the base, which is the allocation this
// program exists to remove. The point lookups do not have the problem: their
// delta side is copied by postings.lookup already, so the merge runs outside the
// lock entirely.

import (
	"bytes"
	"slices"

	"github.com/aoiflux/graphene/store"
)

// mergeRun appends the union of a base run and a delta postings list to dst,
// ascending and deduplicated, skipping retracted base ids.
//
// An id may legitimately be in both: registering a triple the base already holds
// is idempotent from the caller's point of view, and the delta has no way to know
// the base already has it. So this is a set union, not a concatenation.
func (s baseSide[T]) mergeRun(dst []T, run IDRun, delta []T) []T {
	i, n, j := 0, run.Len(), 0
	for i < n && j < len(delta) {
		bid := run.At(i)
		if s.gone.has(bid) {
			i++
			continue
		}
		d := uint64(delta[j])
		switch {
		case bid < d:
			dst = append(dst, T(bid))
			i++
		case d < bid:
			dst = append(dst, delta[j])
			j++
		default:
			dst = append(dst, delta[j])
			i++
			j++
		}
	}
	for ; i < n; i++ {
		if bid := run.At(i); !s.gone.has(bid) {
			dst = append(dst, T(bid))
		}
	}
	return append(dst, delta[j:]...)
}

// runMerge is that same union, one id at a time and resumable, for the streaming
// read paths: they hand each id to a sink and never build the union at all, which
// for a key whose base run holds a million ids is eight megabytes not allocated to
// be walked once and dropped.
//
// # Why this is a second loop and not the only one
//
// It was written as the only one first, with mergeRun above reduced to a loop over
// it, because a mode with two implementations is a mode with two things to keep
// agreeing. Measured, that cost BenchmarkUnionLookup's low-cardinality shape two
// to three times its wall clock — 4278 and 3357 ns became 12059 and 7825 — on a
// path that gains nothing from the change, because the tight loop above keeps i, n
// and j in registers for the whole merge and a cursor must load and store them
// through a pointer once per id. Slower is acceptable in this program where it
// buys memory; this bought none, and charged NodesByProperty for it.
//
// So there are two loops, and what holds them together is
// TestIDStream_AnswersExactlyWhatNodesByPropertyAnswers rather than a shared
// function: it drains the stream and compares it to NodesByProperty over every
// combination of base, delta and retraction the fixture can make. A divergence
// fails there, which is what sharing the code was for.
//
// A cursor rather than a function taking a sink, because a sink is a closure and
// the caller that needs to step away between batches cannot be written as one.
//
// The pending-base-id idiom is nodeScanCursor's in disk/scan.go, and for the same
// reason: a retracted run of base ids has to be skipped before either side can be
// compared, and skipping it inside the comparison means re-testing the same bit
// once per delta id that precedes it.
type runMerge[T entityID] struct {
	gone  *retractSet
	run   IDRun
	i, n  int
	delta []T
	j     int

	// cur is a live base id already drawn from the run and not yet emitted, and
	// held says whether it means anything.
	cur  uint64
	held bool
}

func (s baseSide[T]) runMergeOf(run IDRun, delta []T) runMerge[T] {
	return runMerge[T]{gone: s.gone, run: run, n: run.Len(), delta: delta}
}

// next returns the next id of the union, ascending, or false once both sides are
// drained. An id both sides hold is returned once, from the delta.
func (m *runMerge[T]) next() (T, bool) {
	if !m.held {
		for m.i < m.n {
			id := m.run.At(m.i)
			m.i++
			if m.gone.has(id) {
				continue
			}
			m.cur, m.held = id, true
			break
		}
	}
	if m.held {
		if m.j < len(m.delta) {
			if d := uint64(m.delta[m.j]); d <= m.cur {
				m.j++
				m.held = d != m.cur
				return m.delta[m.j-1], true
			}
		}
		m.held = false
		return T(m.cur), true
	}
	if m.j < len(m.delta) {
		m.j++
		return m.delta[m.j-1], true
	}
	var zero T
	return zero, false
}

// mergeLookup returns the ids under key=value across base and delta.
//
// delta is the copy postings.lookup already made, so this runs outside the shard
// lock and may return delta itself when the base holds nothing.
func (s baseSide[T]) mergeLookup(key string, value []byte, delta []T) []T {
	run, err := s.base().Lookup(s.kind, key, value)
	if err != nil {
		s.fault(err)
		return delta
	}
	if run.Len() == 0 {
		return delta
	}
	return s.mergeRun(make([]T, 0, run.Len()+len(delta)), run, delta)
}

// lookupMerge is mergeLookup's cursor form: the same two sides, resumable, and
// with no slice sized from the base's side of them.
//
// A base that cannot be read yields an empty run rather than an error, which is
// what mergeLookup does with the same fault for the same reason: every read path
// here has to fold an unreadable run into "the base holds nothing under this
// value", because none of them has an error to return. The batch path is the one
// exception and says why.
func (s baseSide[T]) lookupMerge(key string, value []byte, delta []T) runMerge[T] {
	run, err := s.base().Lookup(s.kind, key, value)
	if err != nil {
		s.fault(err)
		run = IDRun{}
	}
	return s.runMergeOf(run, delta)
}

// mergeCardinality is the number of ids under key=value, as an upper bound.
//
// Exact whenever it can be: a value held only by the base, in a store that has
// retracted nothing, is its run's length. Once either side can overlap the other
// it is the sum, which counts an id held by both twice and counts a retracted id
// that is still in the base.
//
// An upper bound is what this is for. The planner compares candidate sets to pick
// the smallest driver, and a driver is required to be a superset of the answer,
// not the answer — so an over-count can cost a worse plan and can never cost a
// wrong result. Counting exactly would mean walking the run and testing a bit per
// id, turning a constant into a pass over a postings list on a path the planner
// takes once per candidate filter per query.
func (s baseSide[T]) mergeCardinality(key string, value []byte, deltaN int) int {
	run, err := s.base().Lookup(s.kind, key, value)
	if err != nil {
		s.fault(err)
		return deltaN
	}
	return run.Len() + deltaN
}

// mergeKeys appends the base's keys for this kind to the delta's, dropping any
// the delta already names. The result is unsorted; every caller sorts.
//
// Keys are few — a handful per store, against millions of entries — so this is a
// sort and a walk rather than a map.
//
// # A base key can outlive its last entry
//
// The resident index deletes a key's bucket when the last entry under it goes,
// which is one of the invariants Verify checks. A base cannot: retracting every
// id that held a key leaves the key in the base's directory until the next
// compaction declines to write it. So this list can name a key that no live entry
// is under, where the resident index's would not.
//
// That is invisible to every caller, and it is worth saying why rather than
// leaving it to be rediscovered. The only readers of this list are the entry
// walks — NodeEntries, EdgeEntries, ForEachNodeProperty, ForEachEdgeProperty —
// and a key with no live entry contributes nothing to any of them. Nothing
// exported returns the list itself. Making it exact would mean asking, per key,
// whether any of its entries survives retraction, which is the pass over the
// whole key that this design exists to avoid paying at a key's every mention.
func (s baseSide[T]) mergeKeys(delta []string) []string {
	base := s.base().Keys(s.kind)
	if len(base) == 0 {
		return delta
	}
	slices.Sort(delta)
	// Only the sorted prefix is searched: the base's own keys are appended past
	// it in ascending order, which does not keep the whole slice sorted, and they
	// are distinct from each other so they cannot collide among themselves.
	n := len(delta)
	for _, k := range base {
		if _, found := slices.BinarySearch(delta[:n], k); !found {
			delta = append(delta, k)
		}
	}
	return delta
}

// mergeForEachValue walks key's distinct values across base and delta together,
// ascending by value, calling fn once per value with the merged id list.
//
// The delta side arrives as an ascending value list and a function returning the
// ids under vals[i], rather than as the shard's map. That is what lets one merge
// serve both a walk that holds the shard lock for its whole length — the value
// walks, whose callers already hold the store lock and so cannot invert against
// it — and a walk that must not, which is every walk that yields to caller code.
// See deltaCopy in property_index.go for the second kind and why it exists. A nil
// idsOf means the delta has no side here at all, which is what the composite
// refill passes.
//
// buf is scratch for the values both sides hold; it is returned so a walk across
// several keys reuses one allocation. ids handed to fn is either the delta's own
// slice, a base run merged into buf, or buf alone — in every case it belongs to
// the index for the duration of the call, which is the contract
// ForEachNodeValue already states.
//
// fn returning false stops the whole walk, both sides of it. That needs its own
// flag rather than the base walk's return value: ForEachValue reports a caller's
// stop and a completed walk the same way — as no error — so without the flag the
// delta's remaining values would be drained into a callback that has already said
// it wants nothing more. Which is how the first version of this got it wrong.
func (s baseSide[T]) mergeForEachValue(key string, vals []string, idsOf func(i int) []T, buf []T,
	fn func(value []byte, ids []T) bool,
) []T {
	j, stopped := 0, false
	err := s.base().ForEachValue(s.kind, key, nil, func(value []byte, run IDRun) bool {
		for ; j < len(vals) && bytes.Compare(unsafeBytes(vals[j]), value) < 0; j++ {
			if !fn(unsafeBytes(vals[j]), idsOf(j)) {
				stopped = true
				return false
			}
		}
		var delta []T
		if j < len(vals) && vals[j] == string(value) {
			delta = idsOf(j)
			j++
		}
		buf = s.mergeRun(buf[:0], run, delta)
		if len(buf) == 0 {
			// Every id under this value has been retracted. The value is gone as
			// far as any reader is concerned, and handing out an empty list would
			// make a filter scan count a value that holds nothing.
			return true
		}
		if !fn(value, buf) {
			stopped = true
			return false
		}
		return true
	})
	// A fault is recorded and the delta is still drained. It is the undamaged half
	// and the values left in it are above the one the base stopped at, so they are
	// still emitted in order — the same choice mergeLookup makes.
	if err != nil {
		s.fault(err)
	}
	for ; !stopped && j < len(vals); j++ {
		if !fn(unsafeBytes(vals[j]), idsOf(j)) {
			break
		}
	}
	return buf
}

// mergeForEachEntry is mergeForEachValue flattened to one call per entry, in
// (value, id) order — the order NodeEntries contracts for and ForEachNodeProperty
// inherits.
func (s baseSide[T]) mergeForEachEntry(key string, vals []string, idsOf func(i int) []T, buf []T,
	fn func(id T, value []byte) bool,
) []T {
	return s.mergeForEachValue(key, vals, idsOf, buf, func(value []byte, ids []T) bool {
		for _, id := range ids {
			if !fn(id, value) {
				return false
			}
		}
		return true
	})
}

// mergeKeyEntryCount is the number of (id, value) entries under key across both
// sides, as an upper bound for the same reason mergeCardinality is one.
func (s baseSide[T]) mergeKeyEntryCount(key string, deltaN int) int {
	_, entries := s.base().KeyStats(s.kind, key)
	return entries + deltaN
}

// forEachID visits every id of this kind the base carries and has not retracted.
//
// Ids the delta also carries are visited from both sides. ForEachIndexedNodeID
// already repeats an id across shards and says so; its callers keep what they are
// looking for rather than what they are given, so a repeat costs a map write.
func (s baseSide[T]) forEachID(fn func(T) bool) {
	s.base().ForEachID(s.kind, func(id uint64) bool {
		if s.gone.has(id) {
			return true
		}
		return fn(T(id))
	})
}

// appendEntriesOf appends the base's entries for one entity to out.
//
// The caller checks the retraction bit first: a retracted id has no base entries
// at all, and testing one bit is cheaper than the reverse-section search this
// would otherwise do.
// entryMatches reports whether the base holds a value under f.Key for id that
// satisfies f.
//
// The probe path's other half, and the reason it exists is worth stating: a probe
// asks the *reverse* direction a question about one id, and the delta's reverse
// direction knows only about ids written since the last compaction. So a probe
// that consulted the delta alone answered "no entry under this key, therefore no
// match" for every entity the image holds — which is most of them — and a
// type-narrowed property query came back empty rather than wrong. That was the one
// read path base ∪ delta − retracted had not reached, and it was invisible until
// the mapped index became the default, because nothing else attaches a base.
//
// Retracted first: an id whose entries have all been dropped has no base entries
// at all, which one bit settles without searching the reverse direction for it.
// Nothing is cloned — the predicate only reads the value, the same contract
// postingsMatch relies on for the delta's side.
func (s baseSide[T]) entryMatches(id uint64, f store.PropertyFilter, ordered bool) bool {
	if s.gone.has(id) {
		return false
	}
	matched := false
	err := s.base().ForEachEntryOf(s.kind, id, func(key string, value []byte) bool {
		if key != f.Key {
			return true
		}
		if filterMatchesValue(f, value, ordered) {
			matched = true
			return false
		}
		return true
	})
	if err != nil {
		s.fault(err)
	}
	return matched
}

// filterMatchesValue applies f to one value under the comparison rule the key was
// declared with.
//
// Shared by both halves of the probe rather than written twice: ordered and
// unordered keys compare differently, and a probe that picked the rule one way for
// the delta and the other way for the base would disagree with itself about the
// same entity depending on which side happened to hold its entry.
func filterMatchesValue(f store.PropertyFilter, v []byte, ordered bool) bool {
	if ordered {
		return store.PropertyFilterMatchesOrdered(f, v)
	}
	return store.PropertyFilterMatches(f, v)
}

func (s baseSide[T]) appendEntriesOf(out []PropEntry, id uint64) []PropEntry {
	err := s.base().ForEachEntryOf(s.kind, id, func(key string, value []byte) bool {
		out = append(out, PropEntry{Key: key, Value: bytes.Clone(value)})
		return true
	})
	if err != nil {
		s.fault(err)
	}
	return out
}

// dedupPropEntries removes adjacent duplicates from a sorted entry list.
//
// Needed only under a base: the delta cannot hold one entity's (key, value) twice
// because postings.add is idempotent, but an entry the base already held and the
// delta has since re-registered appears on both sides. A caller re-registering
// what it was handed must not be handed the same pair twice, or an idempotent
// re-ingest grows its own log.
func dedupPropEntries(e []PropEntry) []PropEntry {
	return slices.CompactFunc(e, func(a, b PropEntry) bool {
		return a.Key == b.Key && bytes.Equal(a.Value, b.Value)
	})
}

// hasEntries reports whether the base carries any entry for id.
func (s baseSide[T]) hasEntries(id uint64) bool {
	return !s.gone.has(id) && s.base().HasEntries(s.kind, id)
}

// --- ranges ---

// orderedScan is one range or prefix filter reduced to what a walk of the base's
// values needs: where to start, and when to stop.
//
// It is deliberately not a membership test. Whether a value matches is decided by
// store.PropertyFilterMatchesOrdered, the same function the delta's scan path
// uses, so the two sides of a merged range cannot drift apart on the meaning of
// an operator. What is here is only the two bounds, so the walk starts at the
// first value that can match and stops at the first that cannot — a base with a
// million values under a key is not walked to answer a filter that selects three.
type orderedScan struct {
	// from is the lowest value worth visiting. A nil from starts at the first.
	from []byte

	// until, when bounded, is the highest; untilInc says whether it matches.
	until    []byte
	untilInc bool
	bounded  bool
}

// done reports that value is past the scan's upper bound, so the walk can stop.
func (sc orderedScan) done(value []byte) bool {
	if !sc.bounded {
		return false
	}
	c := bytes.Compare(value, sc.until)
	return c > 0 || (c == 0 && !sc.untilInc)
}

// orderedScanFor resolves a filter to its bounds, reporting whether an ordering
// can answer the operator at all.
//
// It mirrors orderedIndex.rangeFor case for case, in bytes rather than in
// positions, and the two are checked against each other on every value of the
// differential suite rather than by inspection.
func orderedScanFor(f store.PropertyFilter) (orderedScan, bool) {
	switch f.Op {
	case store.PropertyOpGreaterThan:
		return orderedScan{from: f.Value}, true
	case store.PropertyOpGreaterThanOrEqual:
		return orderedScan{from: f.Value}, true
	case store.PropertyOpLessThan:
		return orderedScan{until: f.Value, bounded: true}, true
	case store.PropertyOpLessThanOrEqual:
		return orderedScan{until: f.Value, untilInc: true, bounded: true}, true
	case store.PropertyOpBetweenInclusive:
		if len(f.ValueUpper) == 0 {
			// A bound-less Between matches nothing, which PropertyFilterMatches
			// also decides; stopping at the first value visited is how that is
			// said here.
			return orderedScan{from: f.Value, until: f.Value, bounded: true}, true
		}
		return orderedScan{from: f.Value, until: f.ValueUpper, untilInc: true, bounded: true}, true
	case store.PropertyOpPrefix:
		if len(f.Value) == 0 {
			return orderedScan{}, true
		}
		upper, bounded := prefixUpperBound(f.Value)
		if !bounded {
			// The prefix is all 0xFF, so every value at or after it matches.
			return orderedScan{from: f.Value}, true
		}
		return orderedScan{from: f.Value, until: upper, bounded: true}, true
	default:
		// Equal is served by the hash postings; Contains cannot be answered from
		// an ordering.
		return orderedScan{}, false
	}
}

// appendOrderedRange appends the ids matching f across base and delta, in
// ascending value order and ascending id within each value — the order
// NodesMatchingOrdered contracts for, now honoured across both sides rather than
// within one.
//
// The delta's window is [lo, hi) over its own sorted values, already resolved by
// rangeFor. The base is walked from sc.from and stopped at sc.done, with each
// value put to the filter itself so that an exclusive bound excludes the same
// value on both sides.
func (s baseSide[T]) appendOrderedRange(dst []T, key string, f store.PropertyFilter, sc orderedScan,
	o *orderedIndex[T], lo, hi int,
) []T {
	if lo < 0 {
		lo = 0
	}
	if hi > len(o.values) {
		hi = len(o.values)
	}
	j := lo
	err := s.base().ForEachValue(s.kind, key, sc.from, func(value []byte, run IDRun) bool {
		if sc.done(value) {
			return false
		}
		if !store.PropertyFilterMatchesOrdered(f, value) {
			// The boundary value of an exclusive bound. The delta's window
			// already excludes it, so the pointer is left where it is and the
			// next base value drains it if it was ever in range.
			return true
		}
		for ; j < hi && bytes.Compare(unsafeBytes(o.values[j].value), value) < 0; j++ {
			dst = append(dst, o.values[j].ids...)
		}
		var delta []T
		if j < hi && o.values[j].value == string(value) {
			delta = o.values[j].ids
			j++
		}
		dst = s.mergeRun(dst, run, delta)
		return true
	})
	if err != nil {
		s.fault(err)
	}
	// Whatever the delta has left is in range by construction: hi is its own
	// upper bound for this filter, so no predicate is owed here.
	for ; j < hi; j++ {
		dst = append(dst, o.values[j].ids...)
	}
	return dst
}

// rangeEntries estimates how many base entries satisfy f.
//
// It keeps the two regimes residual.go sets out and for the reasons given there.
// A narrow window is counted exactly, from the run lengths, which the base
// reports without reading a single id — and precision in a narrow window is what
// decides whether a range beats every other driver. A window wider than that is
// the key's whole entry count: an over-estimate, which can only cost a worse plan
// and which a wide window would have lost on anyway.
func (s baseSide[T]) rangeEntries(key string, f store.PropertyFilter, sc orderedScan) int {
	seen, n, wide := 0, 0, false
	err := s.base().ForEachValue(s.kind, key, sc.from, func(value []byte, run IDRun) bool {
		if sc.done(value) {
			return false
		}
		if !store.PropertyFilterMatchesOrdered(f, value) {
			return true
		}
		seen++
		if seen > exactWindowValues {
			wide = true
			return false
		}
		n += run.Len()
		return true
	})
	if err != nil {
		s.fault(err)
		return 0
	}
	if wide {
		_, entries := s.base().KeyStats(s.kind, key)
		return entries
	}
	return n
}

// --- uniqueness ---

// soleHolder returns the one id holding value under key across base and delta,
// and whether there is exactly one.
//
// It returns an error rather than folding a base fault into "unheld", because
// this is the predicate a unique constraint is enforced with: a caller that
// cannot read the base must be refused, not waved through. Every other read path
// in this file can afford to answer from the delta alone and record the fault,
// and this one cannot.
func (s baseSide[T]) soleHolder(key string, value []byte, delta []T) (T, bool, error) {
	var zero T
	run, err := s.base().Lookup(s.kind, key, value)
	if err != nil {
		s.fault(err)
		return zero, false, err
	}
	var first T
	n := 0
	for i, l := 0, run.Len(); i < l; i++ {
		id := run.At(i)
		if s.gone.has(id) {
			continue
		}
		if n > 0 {
			return zero, false, nil
		}
		first, n = T(id), 1
	}
	for _, id := range delta {
		// The base's single survivor and a delta entry for the same id are one
		// entity registered twice, not two holders.
		if n == 1 && id == first {
			continue
		}
		if n > 0 {
			return zero, false, nil
		}
		first, n = id, 1
	}
	return first, n == 1, nil
}

// mergeConflicts collects every value under key held by two or more live
// entities, across base and delta.
//
// # Why this is usually not a walk of the base
//
// A declaration is checked at every open, over a key that may hold tens of
// millions of base entries, and walking them would put a full sequential read of
// the index section on the open path of a store whose constraint has always held.
// It does not have to: the base reports how many distinct values and how many
// entries a key carries, and entries == distinct says every run holds exactly one
// id. Then no base value can conflict with itself, and the only conflicts
// possible are a delta value meeting the one base id that shares it — which is a
// walk of the delta, with one point lookup per delta value.
//
// When the two disagree some base value holds two ids. That is either a real
// conflict, which the caller is about to be told about in full, or ids retracted
// since the base was written, which only a walk can distinguish from a conflict.
// Either way the walk is what the answer costs, and it is bounded by the key.
func (s baseSide[T]) mergeConflicts(key string, bucket map[string][]T, live func(T) bool) ([]store.UniqueConflict, error) {
	distinct, entries := s.base().KeyStats(s.kind, key)
	if entries == distinct {
		return s.conflictsByProbe(key, bucket, live)
	}
	return s.conflictsByWalk(key, bucket, live)
}

// conflictsByProbe handles the case where every base run holds one id: only the
// values the delta names can conflict.
func (s baseSide[T]) conflictsByProbe(key string, bucket map[string][]T, live func(T) bool) ([]store.UniqueConflict, error) {
	var out []store.UniqueConflict
	vals := sortedBucketValues(bucket, nil)
	for _, v := range vals {
		holders := liveHolders(bucket[v], live)
		run, err := s.base().Lookup(s.kind, key, unsafeBytes(v))
		if err != nil {
			s.fault(err)
			return nil, err
		}
		if id, ok := run.First(); ok && !s.gone.has(id) && live(T(id)) && !slices.Contains(holders, id) {
			holders = append(holders, id)
			slices.Sort(holders)
		}
		if len(holders) > 1 {
			out = append(out, store.UniqueConflict{Value: []byte(v), IDs: holders})
		}
	}
	return out, nil
}

// conflictsByWalk merges the base's values with the delta's and checks each.
func (s baseSide[T]) conflictsByWalk(key string, bucket map[string][]T, live func(T) bool) ([]store.UniqueConflict, error) {
	var out []store.UniqueConflict
	vals := sortedBucketValues(bucket, nil)
	j := 0
	check := func(value []byte, run IDRun, delta []T) {
		holders := liveHolders(delta, live)
		for i, l := 0, run.Len(); i < l; i++ {
			id := run.At(i)
			if s.gone.has(id) || !live(T(id)) || slices.Contains(holders, id) {
				continue
			}
			holders = append(holders, id)
		}
		if len(holders) > 1 {
			slices.Sort(holders)
			out = append(out, store.UniqueConflict{Value: bytes.Clone(value), IDs: holders})
		}
	}
	err := s.base().ForEachValue(s.kind, key, nil, func(value []byte, run IDRun) bool {
		for ; j < len(vals) && bytes.Compare(unsafeBytes(vals[j]), value) < 0; j++ {
			check(unsafeBytes(vals[j]), IDRun{}, bucket[vals[j]])
		}
		var delta []T
		if j < len(vals) && vals[j] == string(value) {
			delta = bucket[vals[j]]
			j++
		}
		check(value, run, delta)
		return true
	})
	if err != nil {
		s.fault(err)
		return nil, err
	}
	for ; j < len(vals); j++ {
		check(unsafeBytes(vals[j]), IDRun{}, bucket[vals[j]])
	}
	// Values arrive in ascending order from both sides, so out is already sorted
	// by value — the order conflictsUnder sorts for, and for its reason.
	return out, nil
}

// liveHolders is the live ids of a postings list, as the uint64s a conflict
// carries.
func liveHolders[T entityID](ids []T, live func(T) bool) []uint64 {
	var out []uint64
	for _, id := range ids {
		if live(id) {
			out = append(out, uint64(id))
		}
	}
	return out
}
