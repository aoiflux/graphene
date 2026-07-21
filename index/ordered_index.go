package index

import (
	"bytes"
	"sort"

	"github.com/aoiflux/graphene/store"
)

// orderedIndex keeps the distinct values registered under one property key in
// ascending byte order, each with its own ascending postings list.
//
// It answers range and prefix predicates with two binary searches and a walk of
// the matching slice, instead of visiting every entry under the key.
//
// # Ordering contract
//
// Values are ordered by bytes.Compare, and nothing else. That is a genuine total
// order, which the scan path's "numeric if both sides parse, otherwise bytes"
// rule is not (see index/encoding). Declaring a key ordered therefore *changes
// how its range predicates compare* — from the mixed rule to plain byte order —
// which is why it is opt-in per key and why callers should encode values with
// index/encoding first.
//
// Equality and Contains do not use this structure: equality is already O(1)
// through the hash postings, and Contains cannot be answered from any ordering.
type orderedIndex[T entityID] struct {
	// values is sorted ascending by bytes.Compare and holds no duplicates.
	values []orderedValue[T]
}

type orderedValue[T entityID] struct {
	value string // raw encoded bytes, as a map-safe string
	ids   []T    // ascending, deduplicated
}

func newOrderedIndex[T entityID]() *orderedIndex[T] {
	return &orderedIndex[T]{}
}

// search returns the position of value, and whether it is present.
func (o *orderedIndex[T]) search(value string) (int, bool) {
	pos := sort.Search(len(o.values), func(i int) bool { return o.values[i].value >= value })
	return pos, pos < len(o.values) && o.values[pos].value == value
}

// add registers id under value, keeping both levels sorted.
func (o *orderedIndex[T]) add(id T, value string) {
	pos, found := o.search(value)
	if found {
		ids, inserted := insertSorted(o.values[pos].ids, id)
		if inserted {
			o.values[pos].ids = ids
		}
		return
	}
	o.values = append(o.values, orderedValue[T]{})
	copy(o.values[pos+1:], o.values[pos:])
	o.values[pos] = orderedValue[T]{value: value, ids: []T{id}}
}

// remove drops id from value's postings, deleting the value when it empties.
func (o *orderedIndex[T]) remove(id T, value string) {
	pos, found := o.search(value)
	if !found {
		return
	}
	ids, removed := deleteSorted(o.values[pos].ids, id)
	if !removed {
		return
	}
	if len(ids) == 0 {
		o.values = append(o.values[:pos], o.values[pos+1:]...)
		return
	}
	o.values[pos].ids = ids
}

// lowerBound returns the first index whose value is >= v.
func (o *orderedIndex[T]) lowerBound(v string) int {
	return sort.Search(len(o.values), func(i int) bool { return o.values[i].value >= v })
}

// upperBound returns the first index whose value is > v.
func (o *orderedIndex[T]) upperBound(v string) int {
	return sort.Search(len(o.values), func(i int) bool { return o.values[i].value > v })
}

// forEachInRange visits every ID whose value falls in values[lo:hi).
func (o *orderedIndex[T]) forEachInRange(lo, hi int, fn func(id T) bool) {
	if lo < 0 {
		lo = 0
	}
	if hi > len(o.values) {
		hi = len(o.values)
	}
	for i := lo; i < hi; i++ {
		for _, id := range o.values[i].ids {
			if !fn(id) {
				return
			}
		}
	}
}

// rangeFor resolves a filter to a [lo, hi) window over the sorted values, and
// reports whether the operator is one this index can answer at all.
func (o *orderedIndex[T]) rangeFor(f store.PropertyFilter) (lo, hi int, ok bool) {
	v := string(f.Value)
	switch f.Op {
	case store.PropertyOpGreaterThan:
		return o.upperBound(v), len(o.values), true
	case store.PropertyOpGreaterThanOrEqual:
		return o.lowerBound(v), len(o.values), true
	case store.PropertyOpLessThan:
		return 0, o.lowerBound(v), true
	case store.PropertyOpLessThanOrEqual:
		return 0, o.upperBound(v), true
	case store.PropertyOpBetweenInclusive:
		if len(f.ValueUpper) == 0 {
			// Matches PropertyFilterMatches, which rejects a bound-less Between.
			return 0, 0, true
		}
		return o.lowerBound(v), o.upperBound(string(f.ValueUpper)), true
	case store.PropertyOpPrefix:
		if len(f.Value) == 0 {
			return 0, len(o.values), true
		}
		lo = o.lowerBound(v)
		upper, bounded := prefixUpperBound(f.Value)
		if !bounded {
			// The prefix is all 0xFF: every value at or after it matches.
			return lo, len(o.values), true
		}
		return lo, o.lowerBound(string(upper)), true
	default:
		// Equal is served by the hash postings; Contains needs a scan.
		return 0, 0, false
	}
}

// prefixUpperBound returns the exclusive upper bound for a prefix range.
// Duplicated from index/encoding to keep this package dependency-free.
func prefixUpperBound(prefix []byte) ([]byte, bool) {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == 0xFF {
			continue
		}
		upper := make([]byte, i+1)
		copy(upper, prefix[:i+1])
		upper[i]++
		return upper, true
	}
	return nil, false
}

// verify checks the ordered index against the hash postings it mirrors.
func (o *orderedIndex[T]) verify(kind, key string, bucket map[string][]T) error {
	if len(o.values) != len(bucket) {
		return errIndexf("%s ordered index %q: holds %d values but the postings hold %d",
			kind, key, len(o.values), len(bucket))
	}
	for i, ov := range o.values {
		if i > 0 && o.values[i-1].value >= ov.value {
			return errIndexf("%s ordered index %q: values not strictly ascending at %d", kind, key, i)
		}
		ids, ok := bucket[ov.value]
		if !ok {
			return errIndexf("%s ordered index %q: value %q is absent from the postings", kind, key, ov.value)
		}
		if len(ids) != len(ov.ids) {
			return errIndexf("%s ordered index %q: value %q has %d ids but the postings have %d",
				kind, key, ov.value, len(ov.ids), len(ids))
		}
		for j := range ids {
			if ids[j] != ov.ids[j] {
				return errIndexf("%s ordered index %q: value %q id %d disagrees with the postings",
					kind, key, ov.value, j)
			}
		}
	}
	return nil
}

// compareValues is the ordering this index implements, exposed for tests and for
// callers that need to reason about it.
func compareValues(a, b []byte) int { return bytes.Compare(a, b) }
