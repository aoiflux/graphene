package store

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// MatchMode controls how multiple filters are combined.
// MatchAll applies AND semantics; MatchAny applies OR semantics.
type MatchMode uint8

const (
	MatchAll MatchMode = iota
	MatchAny
)

// QueryOrder controls deterministic result ordering for query ID outputs.
// QueryOrderAsc sorts IDs ascending; QueryOrderDesc sorts descending.
type QueryOrder uint8

const (
	QueryOrderAsc QueryOrder = iota
	QueryOrderDesc
)

// PropertyOp defines the comparison operation for a property filter.
type PropertyOp uint8

const (
	PropertyOpEqual PropertyOp = iota
	PropertyOpPrefix
	PropertyOpContains
	PropertyOpGreaterThan
	PropertyOpGreaterThanOrEqual
	PropertyOpLessThan
	PropertyOpLessThanOrEqual
	PropertyOpBetweenInclusive
)

// PropertyFilter describes a single property constraint.
//
// Value is required for all operators.
// ValueUpper is required for PropertyOpBetweenInclusive.
type PropertyFilter struct {
	Key        string
	Op         PropertyOp
	Value      []byte
	ValueUpper []byte
}

// NodeQuery describes node-oriented query constraints.
//
// IDs and Types are optional pre-filters. Types use OR semantics.
// Filters are combined using FilterMode (default MatchAll when zero value).
type NodeQuery struct {
	IDs        []NodeID
	Types      []NodeType
	Filters    []PropertyFilter
	FilterMode MatchMode
	Order      QueryOrder
	Offset     int
	Limit      int
}

// EdgeQuery describes edge-oriented query constraints.
//
// IDs, Types, SrcIDs, and DstIDs are optional pre-filters. Types use OR semantics.
// Filters are combined using FilterMode (default MatchAll when zero value).
type EdgeQuery struct {
	IDs        []EdgeID
	Types      []EdgeType
	SrcIDs     []NodeID
	DstIDs     []NodeID
	Filters    []PropertyFilter
	FilterMode MatchMode
	Order      QueryOrder
	Offset     int
	Limit      int
}

// RelationQuery describes relation retrieval around anchor nodes.
//
// Anchors identifies the node(s) to traverse from.
// Direction controls which side of each relation is considered anchored.
// CounterpartIDs (optional) constrain the opposite endpoint.
// EdgeTypes and Filters constrain relation edges.
type RelationQuery struct {
	Anchors      []NodeID
	Direction    Direction
	Counterparts []NodeID
	EdgeTypes    []EdgeType
	Filters      []PropertyFilter
	FilterMode   MatchMode
	Order        QueryOrder
	Offset       int
	Limit        int
}

// EntityID constrains the sorted-postings helpers to the two ID types.
type EntityID interface {
	~uint64
}

// InsertSortedID inserts id into an ascending slice, reporting whether it was
// added. A duplicate is ignored and reported as false.
//
// Keeping postings sorted is what makes removal O(log n) instead of a full
// rewrite, and lets a lookup return an already-ordered result the query path can
// use without sorting again.
func InsertSortedID[T EntityID](ids []T, id T) ([]T, bool) {
	// Fast path: IDs are issued monotonically, so a newly created entity almost
	// always belongs at the end. Checking the tail first turns the common insert
	// back into a bare append — without it, every ingest pays a binary search to
	// rediscover that the answer is "the end", which measured as a 12–30%
	// regression on AddNode and AddEdge.
	if n := len(ids); n == 0 || ids[n-1] < id {
		return append(ids, id), true
	} else if ids[n-1] == id {
		return ids, false
	}

	pos := sortSearchID(ids, id)
	if pos < len(ids) && ids[pos] == id {
		return ids, false
	}
	var zero T
	ids = append(ids, zero)
	copy(ids[pos+1:], ids[pos:])
	ids[pos] = id
	return ids, true
}

// DeleteSortedID removes id from an ascending slice, reporting whether it was
// present.
func DeleteSortedID[T EntityID](ids []T, id T) ([]T, bool) {
	pos := sortSearchID(ids, id)
	if pos >= len(ids) || ids[pos] != id {
		return ids, false
	}
	return append(ids[:pos], ids[pos+1:]...), true
}

// SortedContainsID reports membership in an ascending slice in O(log n).
func SortedContainsID[T EntityID](ids []T, id T) bool {
	pos := sortSearchID(ids, id)
	return pos < len(ids) && ids[pos] == id
}

// sortSearchID returns the first index whose value is >= id.
//
// Hand-rolled rather than sort.Search: the callback form costs an indirect call
// per probe, and this sits on the delete and update paths.
func sortSearchID[T EntityID](ids []T, id T) int {
	lo, hi := 0, len(ids)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ids[mid] < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// IntersectSortedIDs returns the ascending intersection of two ascending,
// duplicate-free slices.
//
// A merge rather than a hash probe: both sides arrive sorted (postings are kept
// that way), so building a map of one side just to probe it with the other costs
// an allocation and a hash per element to reach the same answer. The merge walks
// each side once and allocates a single result slice — and, because the output
// is ascending too, the query path can skip its final sort.
//
// The result reuses a's backing array. a must not be used afterwards.
func IntersectSortedIDs[T EntityID](a, b []T) []T {
	if len(a) == 0 || len(b) == 0 {
		return a[:0]
	}
	out := a[:0]
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// UnionSortedIDs returns the ascending union of two ascending, duplicate-free
// slices. It allocates a fresh result rather than reusing either input.
func UnionSortedIDs[T EntityID](a, b []T) []T {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]T, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// SortDedupeIDs sorts ids ascending and removes duplicates, in place.
func SortDedupeIDs[T EntityID](ids []T) []T {
	if len(ids) < 2 {
		return ids
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// ReverseIDs reverses ids in place.
//
// Turning an ascending result into a descending one is a linear reverse; sorting
// it again would be O(n log n) for the same answer.
func ReverseIDs[T EntityID](ids []T) {
	for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
		ids[i], ids[j] = ids[j], ids[i]
	}
}

// IsSortedIDs reports whether ids is strictly ascending. Used by index
// verification.
func IsSortedIDs[T EntityID](ids []T) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			return false
		}
	}
	return true
}

// SupersetDrivers returns the filters whose individual match set is guaranteed
// to contain the query's whole result, making any one of them usable on its own
// as the driving set.
//
// Under MatchAll the result is the intersection of every filter's match set, so
// it is contained in each one and all filters qualify. Under MatchAny the result
// is a union, so a single filter only bounds it when it is the only filter.
//
// Whether a given filter can actually be *served* from an index is a separate
// question the store answers: equality goes through the hash postings, ranges
// and prefixes need the key to have been declared ordered, and Contains cannot
// be served at all.
func SupersetDrivers(filters []PropertyFilter, mode MatchMode) []PropertyFilter {
	if len(filters) == 0 {
		return nil
	}
	if NormalizedFilterMode(mode) == MatchAny && len(filters) > 1 {
		return nil
	}
	return filters
}

// EqualityDrivers returns the subset of SupersetDrivers that the hash postings
// can serve directly — the equality filters.
//
// Non-equality operators are excluded here because the postings are keyed by
// exact value; serving a prefix or range from them would mean a scan. Those are
// handled by the ordered index when the key is declared for it.
func EqualityDrivers(filters []PropertyFilter, mode MatchMode) []PropertyFilter {
	var out []PropertyFilter
	for _, f := range SupersetDrivers(filters, mode) {
		if f.Op == PropertyOpEqual {
			out = append(out, f)
		}
	}
	return out
}

// OrderedDrivers returns the subset of SupersetDrivers whose operator an ordered
// index can answer: the range comparisons and Prefix.
//
// Equality is excluded (the hash postings are cheaper) and so is Contains, which
// no ordering can bound.
func OrderedDrivers(filters []PropertyFilter, mode MatchMode) []PropertyFilter {
	var out []PropertyFilter
	for _, f := range SupersetDrivers(filters, mode) {
		switch f.Op {
		case PropertyOpGreaterThan, PropertyOpGreaterThanOrEqual,
			PropertyOpLessThan, PropertyOpLessThanOrEqual,
			PropertyOpBetweenInclusive, PropertyOpPrefix:
			out = append(out, f)
		}
	}
	return out
}

// NormalizedFilterMode returns mode when set, otherwise MatchAll.
func NormalizedFilterMode(mode MatchMode) MatchMode {
	if mode == MatchAny {
		return MatchAny
	}
	return MatchAll
}

// NormalizedQueryOrder returns order when set, otherwise QueryOrderAsc.
func NormalizedQueryOrder(order QueryOrder) QueryOrder {
	if order == QueryOrderDesc {
		return QueryOrderDesc
	}
	return QueryOrderAsc
}

// PropertyFilterMatches reports whether actual satisfies the given filter.
//
// For numeric comparisons (>, >=, <, <=, between), this function first attempts
// numeric comparison using ParseFloat on both values. If parsing fails for either
// side, it falls back to byte-wise lexicographic comparison.
func PropertyFilterMatches(filter PropertyFilter, actual []byte) bool {
	return propertyFilterMatches(filter, actual, comparePropertyValues)
}

// PropertyFilterMatchesOrdered is PropertyFilterMatches under byte-wise
// comparison, which is how a key declared ordered is compared.
//
// The two rules disagree, and deliberately so: the scan rule in
// comparePropertyValues prefers numeric order when both operands parse as
// numbers, which reads better for un-encoded values but is not a valid ordering
// (under it "9" < "10" < "1x" < "9", a cycle), so no sorted structure can be
// built on it. Anything evaluating a filter against a declared key must use this
// function, or it will disagree with the index that serves the same predicate.
func PropertyFilterMatchesOrdered(filter PropertyFilter, actual []byte) bool {
	return propertyFilterMatches(filter, actual, bytes.Compare)
}

func propertyFilterMatches(filter PropertyFilter, actual []byte, cmp func(a, b []byte) int) bool {
	switch filter.Op {
	case PropertyOpPrefix:
		return strings.HasPrefix(string(actual), string(filter.Value))
	case PropertyOpContains:
		return strings.Contains(string(actual), string(filter.Value))
	case PropertyOpGreaterThan:
		return cmp(actual, filter.Value) > 0
	case PropertyOpGreaterThanOrEqual:
		return cmp(actual, filter.Value) >= 0
	case PropertyOpLessThan:
		return cmp(actual, filter.Value) < 0
	case PropertyOpLessThanOrEqual:
		return cmp(actual, filter.Value) <= 0
	case PropertyOpBetweenInclusive:
		if len(filter.ValueUpper) == 0 {
			return false
		}
		return cmp(actual, filter.Value) >= 0 && cmp(actual, filter.ValueUpper) <= 0
	case PropertyOpEqual:
		fallthrough
	default:
		return bytes.Equal(actual, filter.Value)
	}
}

func comparePropertyValues(actual []byte, expected []byte) int {
	aNum, aErr := strconv.ParseFloat(string(actual), 64)
	bNum, bErr := strconv.ParseFloat(string(expected), 64)
	if aErr == nil && bErr == nil {
		switch {
		case aNum < bNum:
			return -1
		case aNum > bNum:
			return 1
		default:
			return 0
		}
	}
	return bytes.Compare(actual, expected)
}

// ApplyNodeQueryWindow applies offset/limit pagination over sorted node IDs.
// Offset <= 0 means start from 0. Limit <= 0 means no upper bound.
func ApplyNodeQueryWindow(ids []NodeID, offset, limit int) []NodeID {
	if len(ids) == 0 {
		return nil
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ids) {
		return nil
	}
	end := len(ids)
	if limit > 0 {
		end = offset + limit
		if end > len(ids) {
			end = len(ids)
		}
	}
	out := make([]NodeID, end-offset)
	copy(out, ids[offset:end])
	return out
}

// ApplyEdgeQueryWindow applies offset/limit pagination over sorted edge IDs.
// Offset <= 0 means start from 0. Limit <= 0 means no upper bound.
func ApplyEdgeQueryWindow(ids []EdgeID, offset, limit int) []EdgeID {
	if len(ids) == 0 {
		return nil
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ids) {
		return nil
	}
	end := len(ids)
	if limit > 0 {
		end = offset + limit
		if end > len(ids) {
			end = len(ids)
		}
	}
	out := make([]EdgeID, end-offset)
	copy(out, ids[offset:end])
	return out
}

// FilterIndexOf returns the position of f within filters, or -1 if absent.
//
// The query planner uses this to tell the residual pass which filter it already
// applied to build the candidate set. Two identical filters in one query make
// the answer ambiguous, which is harmless: under MatchAll applying either copy
// is idempotent, so skipping one and evaluating the other changes nothing.
func FilterIndexOf(filters []PropertyFilter, f PropertyFilter) int {
	for i, c := range filters {
		if c.Key == f.Key && c.Op == f.Op &&
			bytes.Equal(c.Value, f.Value) && bytes.Equal(c.ValueUpper, f.ValueUpper) {
			return i
		}
	}
	return -1
}

// --- Query plans ---

// DriverKind names the source a query was driven from — the set the planner
// chose as its starting point because it is guaranteed to contain the answer.
type DriverKind uint8

const (
	DriverScan     DriverKind = iota // every entity; nothing better was available
	DriverIDs                        // the query's explicit ID list
	DriverEquality                   // an equality filter's postings
	DriverOrdered                    // a range or prefix on a key declared ordered
	DriverLabels                     // the label postings for the query's types
	DriverAdjacency                  // incident-edge lists of the anchored endpoints
)

func (d DriverKind) String() string {
	switch d {
	case DriverIDs:
		return "ids"
	case DriverEquality:
		return "equality"
	case DriverOrdered:
		return "ordered"
	case DriverLabels:
		return "labels"
	case DriverAdjacency:
		return "adjacency"
	default:
		return "scan"
	}
}

// ResidualStep describes one filter that was not used to drive the query, and
// how the planner decided to apply it.
type ResidualStep struct {
	Key   string
	Op    PropertyOp
	Probe bool // test the candidates directly, rather than build this filter's set
	Cost  int  // estimated size of this filter's own set
}

// QueryPlan reports how a query was resolved. It is diagnostic output: the
// planner's choices are not part of the API contract and may change as the cost
// model improves. Results never do.
type QueryPlan struct {
	Driver       DriverKind
	DriverKey    string // property key, when the driver was a filter
	DriverFilter int    // index into the query's Filters, or -1
	Candidates   int    // size of the driving set
	Residuals    []ResidualStep
	Results      int
}

// String renders a plan as a single line, for tests and for humans.
func (p QueryPlan) String() string {
	var b strings.Builder
	b.WriteString("driver=")
	b.WriteString(p.Driver.String())
	if p.DriverKey != "" {
		b.WriteString("(" + p.DriverKey + ")")
	}
	fmt.Fprintf(&b, " candidates=%d", p.Candidates)
	for _, r := range p.Residuals {
		method := "set"
		if r.Probe {
			method = "probe"
		}
		fmt.Fprintf(&b, " residual=%s:%s~%d", r.Key, method, r.Cost)
	}
	fmt.Fprintf(&b, " results=%d", p.Results)
	return b.String()
}
