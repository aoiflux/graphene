package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"

	"github.com/aoiflux/graphene/store"
)

// Composite indexes: one lookup for a conjunction of equality filters.
//
// The single-key postings answer "which entities have key=value". A query that
// asks for two of those has to drive from one and eliminate against the other,
// which costs the size of the driving set even when the conjunction is tiny. The
// shape that hurts is a pair of individually weak keys whose combination is
// selective: a case identifier with a hundred values and a bucket with ten, over
// a million nodes, is 10 000 candidates thinned to about a thousand. A composite
// index over (case, bucket) is one map lookup returning exactly the thousand.
//
// # Why this does not live in a propertyShard
//
// Shards are chosen by hashing the property key, and the whole point of a
// composite is that it spans several keys — which will generally hash to several
// shards. Putting one in a shard would mean an operation that needs two shard
// locks at once, and PropertyIndex's freedom from deadlock rests on that never
// happening (see its type comment).
//
// So a composite index owns its state outright and takes exactly one lock: its
// own. It observes the same (id, key, value) registration stream the shards do,
// and holds, per entity, the values that entity carries for each member key.
// That duplicates state the shards already have — but the shards hold it behind
// the locks this must not take, so "read it from there" is the one option that
// is not available.
//
// # Multi-valued members
//
// A key can hold several values for one entity: index entries are additive, and
// UpdateNode deliberately does not purge them (see store.GraphStore). An entity
// whose "bucket" was registered as both "hot" and "cold" genuinely matches
// bucket="hot" and bucket="cold" through the single-key postings, so a composite
// that filed it under only one of them would return fewer rows than the
// intersection it replaces. It is therefore filed under every tuple in the cross
// product of its members' values.
//
// In the normal case every member holds one value and the cross product is one
// tuple. A cross product larger than that is the caller's own registrations
// described exactly, not a blow-up this index invented.

// ErrCompositeKeys reports a composite declaration that cannot be indexed.
var ErrCompositeKeys = errors.New("graphene: invalid composite index keys")

// maxCompositeKeys bounds a tuple's width. The per-entity "which positions are
// filled" set is a uint64, and a composite over more than 64 keys is a schema
// question rather than an index one.
const maxCompositeKeys = 64

// compositeIndex maps an encoded tuple of values to the entities carrying it.
type compositeIndex[T entityID] struct {
	// keys is the declared tuple, in declaration order. Immutable once built, so
	// it is read without the lock.
	keys []string

	mu sync.RWMutex

	// postings maps an encoded tuple (see encodeTuple) to ascending,
	// deduplicated ids — the same shape and the same ordering guarantee as the
	// single-key postings, so a composite-driven query needs no sort.
	postings map[string][]T

	// members holds, per entity, the values it carries at each position.
	//
	// This is what makes registration a single-lock operation: a new value at
	// one position needs the other positions' values to form a tuple, and they
	// are here rather than behind a shard lock this must not take.
	members map[T]*memberState

	// entries is the number of (id, tuple) pairs, maintained rather than
	// recomputed for the same reason orderedIndex.totalIDs is: the planner reads
	// it to size this index against the alternatives.
	entries int
}

// memberState is one entity's values across a composite's key positions.
type memberState struct {
	// one holds the single value at each position — true for essentially every
	// entity, since a key normally carries one value per entity.
	one []string

	// filled marks the positions that have a value. A separate bitmask because
	// "" is a legal property value and so cannot stand in for absent.
	filled uint64

	// more carries the extra values for a position that went multi-valued. nil
	// for every entity that never did, which is almost all of them.
	more map[int][]string
}

func newCompositeIndex[T entityID](keys []string) *compositeIndex[T] {
	own := make([]string, len(keys))
	copy(own, keys)
	return &compositeIndex[T]{
		keys:     own,
		postings: make(map[string][]T),
		members:  make(map[T]*memberState),
	}
}

// validateCompositeKeys checks a declaration before anything is built from it.
func validateCompositeKeys(keys []string) error {
	if len(keys) < 2 {
		// A one-key composite is the single-key postings under another name, and
		// would be maintained twice to answer one question.
		return fmt.Errorf("%w: need at least 2 keys, got %d", ErrCompositeKeys, len(keys))
	}
	if len(keys) > maxCompositeKeys {
		return fmt.Errorf("%w: %d keys exceeds the maximum of %d",
			ErrCompositeKeys, len(keys), maxCompositeKeys)
	}
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k == "" {
			return fmt.Errorf("%w: empty key", ErrCompositeKeys)
		}
		if _, dup := seen[k]; dup {
			return fmt.Errorf("%w: key %q appears twice", ErrCompositeKeys, k)
		}
		seen[k] = struct{}{}
	}
	return nil
}

// compositeName joins a tuple into the stable identity used to look a
// declaration up.
//
// NUL rather than a printable separator: property keys are caller-supplied
// strings, and a separator one of them could contain would let two different
// tuples share a name.
func compositeName(keys []string) string { return strings.Join(keys, "\x00") }

// encodeTuple renders one value per position as a single map key.
//
// Length-prefixed rather than delimited: property values are arbitrary bytes, so
// no separator is unavailable, and ("a", "bc") must not collide with ("ab", "c").
func encodeTuple(values []string) string {
	n := 0
	for _, v := range values {
		n += 4 + len(v)
	}
	b := make([]byte, 0, n)
	var l [4]byte
	for _, v := range values {
		binary.LittleEndian.PutUint32(l[:], uint32(len(v)))
		b = append(b, l[:]...)
		b = append(b, v...)
	}
	return string(b)
}

// valuesAt returns every value the entity carries at position pos.
func (m *memberState) valuesAt(pos int) []string {
	if m.filled&(1<<uint(pos)) == 0 {
		return nil
	}
	if extra := m.more[pos]; len(extra) > 0 {
		out := make([]string, 0, 1+len(extra))
		return append(append(out, m.one[pos]), extra...)
	}
	return m.one[pos : pos+1]
}

// add records value at pos, reporting whether it was new.
func (m *memberState) add(pos int, value string) bool {
	bit := uint64(1) << uint(pos)
	if m.filled&bit == 0 {
		m.one[pos] = value
		m.filled |= bit
		return true
	}
	if m.one[pos] == value {
		return false
	}
	for _, v := range m.more[pos] {
		if v == value {
			return false
		}
	}
	if m.more == nil {
		m.more = make(map[int][]string)
	}
	m.more[pos] = append(m.more[pos], value)
	return true
}

// complete reports whether every position has a value, which is what it takes
// for the entity to appear under any tuple at all.
func (m *memberState) complete(width int) bool {
	return bits.OnesCount64(m.filled) == width
}

// forEachTuple calls fn for every tuple in the cross product of the entity's
// values, with position pos pinned to pinned when pin is true.
//
// Pinning is what makes registration cheap: when a value arrives at pos, the
// tuples that become reachable are exactly those carrying it there, and the rest
// were filed when their own values arrived.
func (m *memberState) forEachTuple(width, pos int, pinned string, pin bool, fn func(string)) {
	if m.more == nil {
		// Every position holds exactly one value, which is the case for
		// essentially every entity — more is allocated only when a second value
		// arrives at some position. The cross product is then the single tuple
		// already assembled in one, and pinning cannot change it, because a
		// value that reached this path is the one add just stored there.
		//
		// Worth its own branch rather than falling through: this runs on every
		// registration, and the walk below allocates a buffer to rediscover a
		// tuple that is sitting in front of it.
		fn(encodeTuple(m.one))
		return
	}
	buf := make([]string, width)
	var walk func(i int)
	walk = func(i int) {
		if i == width {
			fn(encodeTuple(buf))
			return
		}
		if pin && i == pos {
			buf[i] = pinned
			walk(i + 1)
			return
		}
		for _, v := range m.valuesAt(i) {
			buf[i] = v
			walk(i + 1)
		}
	}
	walk(0)
}

// register records that id carries value at position pos.
//
// When the entity's tuple is complete, every tuple carrying value at pos is
// filed. That is right in both directions: if pos had no value before, the
// entity was in no tuple at all and this is its whole cross product; if it did,
// the tuples not carrying value were filed when their own values arrived.
func (c *compositeIndex[T]) register(id T, pos int, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.members[id]
	if m == nil {
		m = &memberState{one: make([]string, len(c.keys))}
		c.members[id] = m
	}
	if !m.add(pos, value) {
		return
	}
	if !m.complete(len(c.keys)) {
		// An early exit, not the thing that prevents a partial filing: the cross
		// product of a position with no values is empty, so an incomplete state
		// produces no tuples whether this returns or not. What it buys is the
		// ingest path, where every entity is incomplete until its last member
		// key arrives — without it each of those registrations allocates a walk
		// buffer to discover there is nothing to walk.
		return
	}
	m.forEachTuple(len(c.keys), pos, value, true, func(tuple string) {
		ids, inserted := insertSorted(c.postings[tuple], id)
		if inserted {
			c.postings[tuple] = ids
			c.entries++
		}
	})
}

// remove drops every trace of id.
//
// The tuples to unfile are recomputed from the entity's own member values rather
// than found by searching the postings, which is what keeps removal proportional
// to that entity instead of to the index.
func (c *compositeIndex[T]) remove(id T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.members[id]
	if m == nil {
		return
	}
	delete(c.members, id)
	if !m.complete(len(c.keys)) {
		return
	}
	m.forEachTuple(len(c.keys), 0, "", false, func(tuple string) {
		ids, removed := deleteSorted(c.postings[tuple], id)
		if !removed {
			return
		}
		c.entries--
		if len(ids) == 0 {
			delete(c.postings, tuple)
			return
		}
		c.postings[tuple] = ids
	})
}

// lookup returns a copy of the ascending ids filed under the given values.
func (c *compositeIndex[T]) lookup(values []string) []T {
	tuple := encodeTuple(values)
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.postings[tuple]
	if len(ids) == 0 {
		return nil
	}
	out := make([]T, len(ids))
	copy(out, ids)
	return out
}

// cardinality is lookup's size without the copy — what the planner costs a
// composite driver by. Exact, not estimated: the answer is one map lookup away,
// so there is nothing here for stats.go's two regimes to decide between.
func (c *compositeIndex[T]) cardinality(values []string) int {
	tuple := encodeTuple(values)
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.postings[tuple])
}

// verify re-derives the whole index from the member states it holds and reports
// the first disagreement.
//
// A composite index is maintained incrementally from a stream of registrations,
// and drift between the members and the postings is a wrong query answer rather
// than a slow one: the planner drives straight from these postings and applies
// no residual filter for the keys they cover, so a missing entry is a row the
// query silently does not return. Both directions are therefore checked — every
// tuple a member state implies is filed, and every filed entry is implied by one.
func (c *compositeIndex[T]) verify(kind string, cc *store.CancelCheck) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	name := strings.Join(c.keys, ",")
	width := len(c.keys)

	implied := make(map[string]map[T]struct{})
	counted := 0
	for id, m := range c.members {
		if err := cc.Step(); err != nil {
			return err
		}
		if len(m.one) != width {
			return errIndexf("%s composite index (%s): entity %v holds %d positions, want %d",
				kind, name, id, len(m.one), width)
		}
		if !m.complete(width) {
			continue
		}
		m.forEachTuple(width, 0, "", false, func(tuple string) {
			set := implied[tuple]
			if set == nil {
				set = make(map[T]struct{})
				implied[tuple] = set
			}
			if _, dup := set[id]; !dup {
				set[id] = struct{}{}
				counted++
			}
		})
	}

	for tuple, ids := range c.postings {
		if err := cc.Step(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return errIndexf("%s composite index (%s): an empty postings list was retained", kind, name)
		}
		for i := 1; i < len(ids); i++ {
			if ids[i-1] >= ids[i] {
				return errIndexf("%s composite index (%s): postings are not ascending and deduplicated at %d",
					kind, name, i)
			}
		}
		set := implied[tuple]
		for _, id := range ids {
			if _, ok := set[id]; !ok {
				return errIndexf("%s composite index (%s): entity %v is filed under a tuple its own values do not produce",
					kind, name, id)
			}
		}
		if len(set) != len(ids) {
			return errIndexf("%s composite index (%s): a tuple holds %d entities but their values imply %d",
				kind, name, len(ids), len(set))
		}
	}

	if counted != c.entries {
		return errIndexf("%s composite index (%s): entry count is %d but the member values imply %d",
			kind, name, c.entries, counted)
	}
	if len(implied) != len(c.postings) {
		return errIndexf("%s composite index (%s): %d tuples are filed but the member values imply %d",
			kind, name, len(c.postings), len(implied))
	}
	return nil
}

// --- the set of declared composites ---

// compositeSet holds every declared composite index for one entity kind.
type compositeSet[T entityID] struct {
	mu sync.RWMutex

	// byName is the declarations, keyed by compositeName.
	byName map[string]*compositeIndex[T]

	// byKey maps one property key to the composites that use it, with the
	// position it occupies in each. Without it, every registration would scan
	// every declaration to find out it had no work to do.
	byKey map[string][]compositeMember[T]
}

// compositeMember is one composite's use of one key.
type compositeMember[T entityID] struct {
	idx *compositeIndex[T]
	pos int
}

func newCompositeSet[T entityID]() compositeSet[T] {
	return compositeSet[T]{
		byName: make(map[string]*compositeIndex[T]),
		byKey:  make(map[string][]compositeMember[T]),
	}
}

// declare registers keys, returning the new index and true, or nil and false if
// that tuple was already declared.
func (c *compositeSet[T]) declare(keys []string) (*compositeIndex[T], bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	name := compositeName(keys)
	if _, exists := c.byName[name]; exists {
		return nil, false
	}
	idx := newCompositeIndex[T](keys)
	c.byName[name] = idx
	for pos, k := range idx.keys {
		c.byKey[k] = append(c.byKey[k], compositeMember[T]{idx: idx, pos: pos})
	}
	return idx, true
}

// membersOf returns the composites using key.
//
// The slice is only ever appended to under the write lock and never mutated in
// place, so the header copied out here stays valid once the lock is released —
// which is what lets a registration update each composite without holding this
// lock as well.
func (c *compositeSet[T]) membersOf(key string) []compositeMember[T] {
	c.mu.RLock()
	m := c.byKey[key]
	c.mu.RUnlock()
	return m
}

// all returns every declared index.
func (c *compositeSet[T]) all() []*compositeIndex[T] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*compositeIndex[T], 0, len(c.byName))
	for _, idx := range c.byName {
		out = append(out, idx)
	}
	return out
}

// tuples returns the declared key tuples, ordered so two stores that declared
// the same set report it identically.
func (c *compositeSet[T]) tuples() [][]string {
	c.mu.RLock()
	out := make([][]string, 0, len(c.byName))
	for _, idx := range c.byName {
		t := make([]string, len(idx.keys))
		copy(t, idx.keys)
		out = append(out, t)
	}
	c.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return compositeName(out[i]) < compositeName(out[j])
	})
	return out
}

// registered files a new (id, key, value) into every composite using key.
//
// Called after the owning shard's lock has been released, and taking only the
// composite's own lock — so no operation ever holds a shard lock and a composite
// lock at once, and there is still no lock order to get wrong.
func (c *compositeSet[T]) registered(id T, key, value string) {
	for _, m := range c.membersOf(key) {
		m.idx.register(id, m.pos, value)
	}
}

// removed drops id from every declared composite.
func (c *compositeSet[T]) removed(id T) {
	for _, idx := range c.all() {
		idx.remove(id)
	}
}

// find returns the composite declared over exactly these keys, in this order.
func (c *compositeSet[T]) find(keys []string) (*compositeIndex[T], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	idx, ok := c.byName[compositeName(keys)]
	return idx, ok
}

// verifyAll checks every declared composite.
func (c *compositeSet[T]) verifyAll(kind string, cc *store.CancelCheck) error {
	for _, idx := range c.all() {
		if err := idx.verify(kind, cc); err != nil {
			return err
		}
	}
	return nil
}

// CompositeMatch describes a declared composite that a query's equality filters
// fully cover, and what driving from it would cost.
type CompositeMatch struct {
	// Keys is the declared tuple, in declaration order.
	Keys []string

	// Values is the filter value for each position, in the same order.
	Values [][]byte

	// Filters marks the query filters this match consumes, so the residual pass
	// skips all of them rather than re-deriving the conjunction the candidates
	// were built from.
	Filters store.FilterMask

	// Size is the exact number of entities filed under this tuple.
	Size int
}

// Name renders the tuple for a query plan's DriverKey.
func (m CompositeMatch) Name() string { return strings.Join(m.Keys, "+") }

// matchComposite finds the declared composite that best fits a query.
//
// A composite answers a conjunction only when every one of its keys is pinned to
// a value, so a declaration qualifies only if the query carries an equality
// filter for each of its keys. Nothing weaker works: the postings are keyed by
// the whole tuple, so a partially specified tuple has no entry to look up.
//
// Where several declarations qualify, the smallest result wins — the same rule
// every other driver is chosen by.
func matchComposite[T entityID](
	c *compositeSet[T],
	filters []store.PropertyFilter,
	mode store.MatchMode,
) (CompositeMatch, bool) {
	drivers := store.EqualityDrivers(filters, mode)
	if len(drivers) < 2 {
		// One equality filter cannot cover a tuple of two or more, and the
		// single-key postings answer it better anyway.
		return CompositeMatch{}, false
	}

	// First value wins for a repeated key. Two equality filters on one key under
	// MatchAll can both hold only if they carry the same value; if they differ
	// the answer is empty, which the residual pass then produces, because the
	// filter that was not chosen is not marked as consumed.
	byKey := make(map[string][]byte, len(drivers))
	for _, f := range drivers {
		if _, seen := byKey[f.Key]; !seen {
			byKey[f.Key] = f.Value
		}
	}

	var best CompositeMatch
	found := false
	for _, idx := range c.all() {
		values := make([]string, len(idx.keys))
		raw := make([][]byte, len(idx.keys))
		covered := true
		for i, k := range idx.keys {
			v, ok := byKey[k]
			if !ok {
				covered = false
				break
			}
			values[i] = string(v)
			raw[i] = v
		}
		if !covered {
			continue
		}
		size := idx.cardinality(values)
		if found && size >= best.Size {
			continue
		}
		mask := store.FilterMask(0)
		for i, k := range idx.keys {
			for j, f := range filters {
				if f.Key == k && f.Op == store.PropertyOpEqual && string(f.Value) == values[i] {
					mask = mask.Set(j)
					break
				}
			}
		}
		best = CompositeMatch{Keys: idx.keys, Values: raw, Filters: mask, Size: size}
		found = true
	}
	return best, found
}

// lookupComposite resolves a match to its ascending, deduplicated ids. ok is
// false only if the declaration went away between planning and execution, which
// nothing in the engine does.
func lookupComposite[T entityID](c *compositeSet[T], m CompositeMatch) ([]T, bool) {
	idx, ok := c.find(m.Keys)
	if !ok {
		return nil, false
	}
	values := make([]string, len(m.Values))
	for i, v := range m.Values {
		values[i] = string(v)
	}
	return idx.lookup(values), true
}
