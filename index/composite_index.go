package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
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

	// members maps an entity to its row in slots.
	//
	// This is what makes registration a single-lock operation: a new value at
	// one position needs the other positions' values to form a tuple, and they
	// are here rather than behind a shard lock this must not take.
	//
	// A row index rather than a pointer to a per-entity struct, because that
	// struct was the largest thing this index held. See slots.
	members map[T]int32

	// slots holds one value reference per position, width of them per row, laid
	// out row-major. noValue marks a position with no value.
	//
	// This is the columnar form of what used to be a *memberState per entity: a
	// 48-byte struct, a separately allocated backing array of width string
	// headers, and one freshly converted copy of every value's bytes, reached
	// through a pointer in the map. Measured over 200,000 entities of a width-2
	// composite, that arrangement cost 135.65 B per entry and was 85% of what a
	// composite index holds — the largest single term in a default configuration
	// at the shape this engine is sized for. A row is width int32s and the values
	// are interned once in vtab.
	//
	// A row index is int32 and cannot overflow before this array could not be
	// allocated: 2^31 rows of a width-2 composite is 17 GB of slots alone.
	slots []int32

	// freeRow lists the rows of removed entities, for reuse. The array itself is
	// never shrunk — Go's maps do not shrink either, and a store that deleted
	// everything is about to be compacted.
	freeRow []int32

	// extra carries the second and later values at a position that went
	// multi-valued, keyed by row*width+pos. nil for every index whose entities
	// each hold one value per key, which is almost all of them.
	extra map[int64][]int32

	// extraRows marks, one bit per row, the rows with any overflow list, so the
	// registration path costs no map lookup in an index where some single entity
	// once held two values at one key. See hasExtras.
	extraRows []uint64

	// vtab interns the values the rows refer to.
	vtab valueTable

	// scratch assembles one tuple's values during register and remove, both of
	// which hold mu exclusively. The read paths bring their own: see verify.
	scratch []string

	// entries is the number of (id, tuple) pairs, maintained rather than
	// recomputed for the same reason orderedIndex.totalIDs is: the planner reads
	// it to size this index against the alternatives.
	entries int
}

// noValue marks a position in slots that holds no value. Distinct from any
// reference because references index vtab and are therefore non-negative.
const noValue = int32(-1)

// valueTable interns the values a composite's rows carry.
//
// A row stores an int32 reference rather than a string, and that is the whole of
// the saving. Every registration arrives as a freshly converted string, so the
// form this replaces held one copy of a value's bytes per entity carrying it —
// for a key with a thousand distinct values over a million entities, a thousand
// copies would have done.
//
// Refcounted rather than grow-only, and that is not a refinement: a composite
// over a high-cardinality key would otherwise gain one permanent slot per value
// ever registered, so a store that rewrites its derived layer would accumulate a
// table nothing reads and nothing frees — a leak this index does not have today
// and must not acquire in exchange for the bytes above. postings.shared achieves
// the same by deleting its entry when the bucket empties; here the holders are
// rows rather than a bucket, so the count is explicit.
//
// # Why the reverse direction is not a map
//
// It was, and it cost more than the rows saved on exactly the composite
// postings.canonical warns about: a member key whose values rarely repeat. A
// map[string]int32 measures at ~96 B per distinct value, so a width-2 composite
// with one distinct value per entity per position paid it 400,000 times and came
// out 16.6% worse per entry than the per-entity form it replaced — measured, not
// reasoned about. canonical's own answer is to intern only a value's second
// entry, which needs a count this side does not have before the lookup that
// would establish it.
//
// So the reverse direction is an open-addressed set of references instead,
// probed by hashing the value and comparing against vals. It holds 4 bytes per
// bucket at a load factor of one half — 8 B per distinct value against 96 — and
// that shape then comes out slightly better than what it replaced, while the
// shape composites are actually for holds under a third.
//
// Caching each value's hash beside it was tried, to spare the rehash its string
// hashing. It bought 8% of the build time on the high-cardinality shape and cost
// 3.3% of that shape's bytes, which is the wrong direction under this
// programme's priority, so the rehash hashes strings.
//
// The seed is per-table and random. Nothing here is persisted or enumerated in
// bucket order, so there is no determinism to preserve, and property values are
// caller-supplied: a fixed seed would let a caller choose values that collide.
type valueTable struct {
	// vals is the interned strings, addressed by reference. A freed slot holds
	// "" and is listed in free.
	vals []string

	// count is the number of rows referring to each slot.
	count []int32

	// free lists slots whose count reached zero, for reuse — so a store that
	// churns values reuses slots instead of growing vals forever.
	free []int32

	// buckets holds a reference, bucketEmpty or bucketDead. A power of two, so
	// the probe masks rather than divides.
	buckets []int32

	// live is the number of buckets holding a reference, and tombs the number
	// holding a tombstone. Both drive the rehash, and live is what verify checks
	// the free list against.
	live  int
	tombs int

	seed maphash.Seed
}

const (
	// bucketEmpty ends a probe: nothing was ever stored here.
	bucketEmpty = int32(-1)

	// bucketDead continues a probe but may be claimed by an insertion. Linear
	// probing cannot simply empty a slot — doing so cuts the chain that reached
	// whatever was placed after it.
	bucketDead = int32(-2)
)

func newValueTable() valueTable { return valueTable{seed: maphash.MakeSeed()} }

// find locates value, returning its reference, the bucket it occupies, and
// whether it was there. When it was not, the bucket returned is the one an
// insertion should claim — the first tombstone passed, or the empty slot that
// ended the probe.
func (t *valueTable) find(value string) (int32, int, bool) {
	mask := len(t.buckets) - 1
	i := int(maphash.String(t.seed, value)) & mask
	dead := -1
	for {
		switch b := t.buckets[i]; {
		case b == bucketEmpty:
			if dead >= 0 {
				return 0, dead, false
			}
			return 0, i, false
		case b == bucketDead:
			if dead < 0 {
				dead = i
			}
		case t.vals[b] == value:
			return b, i, true
		}
		i = (i + 1) & mask
	}
}

// ref returns the reference for value, taking one count on it.
func (t *valueTable) ref(value string) int32 {
	if len(t.buckets) == 0 {
		t.rehash(8)
	}
	r, slot, ok := t.find(value)
	if ok {
		t.count[r]++
		return r
	}
	if n := len(t.free); n > 0 {
		r = t.free[n-1]
		t.free = t.free[:n-1]
		t.vals[r] = value
		t.count[r] = 1
	} else {
		r = int32(len(t.vals))
		t.vals = append(t.vals, value)
		t.count = append(t.count, 1)
	}
	// The bucket the probe above ended on, not a second probe: it is empty or a
	// tombstone either way, and storing value in vals does not move it.
	if t.buckets[slot] == bucketDead {
		t.tombs--
	}
	t.buckets[slot] = r
	t.live++
	if (t.live+t.tombs)*2 >= len(t.buckets) {
		t.rehash(0)
	}
	return r
}

// unref drops one count, freeing the slot at zero. noValue is ignored, so a
// caller releasing a row need not check each position first.
func (t *valueTable) unref(r int32) {
	if r == noValue {
		return
	}
	t.count[r]--
	if t.count[r] > 0 {
		return
	}
	// Before vals[r] is cleared: the probe compares against it.
	if _, slot, ok := t.find(t.vals[r]); ok {
		t.buckets[slot] = bucketDead
		t.live--
		t.tombs++
	}
	t.vals[r] = ""
	t.free = append(t.free, r)
}

// rehash rebuilds the bucket array, dropping every tombstone. size fixes the new
// length; zero picks the smallest power of two that keeps the load under a half.
func (t *valueTable) rehash(size int) {
	n := size
	if n == 0 {
		n = 8
		for n < (t.live+1)*2 {
			n *= 2
		}
	}
	t.buckets = make([]int32, n)
	for i := range t.buckets {
		t.buckets[i] = bucketEmpty
	}
	t.tombs = 0
	t.live = 0
	for r := range t.vals {
		if t.count[r] <= 0 {
			continue
		}
		// find rather than a bare scan for an empty bucket: it is the same probe
		// the lookups will make, and a disagreement between the two would
		// otherwise be silent.
		_, slot, _ := t.find(t.vals[r])
		t.buckets[slot] = int32(r)
		t.live++
	}
}

// at returns the value a reference names. noValue has none and must not reach
// here; every caller establishes the position is filled first.
func (t *valueTable) at(r int32) string { return t.vals[r] }

// verifyRef checks that a reference names a live interned value.
func (t *valueTable) verifyRef(ref int32) error {
	if ref < 0 || int(ref) >= len(t.vals) {
		return fmt.Errorf("names value %d, outside the %d interned", ref, len(t.vals))
	}
	if t.count[ref] <= 0 {
		return fmt.Errorf("names value %d, which is free", ref)
	}
	return nil
}

// verify checks the intern table against itself: that the two directions agree,
// that no count went negative, and that the free list holds exactly the slots no
// row refers to.
//
// The last of those is the one worth having. A slot freed twice appears once in
// the counts and twice in the free list, and the next two registrations then
// share a slot — two distinct values collapsing into one, which is a composite
// answering a query with entities that do not match it. Nothing else here would
// notice, because every individual reference would still resolve.
func (t *valueTable) verify() error {
	if len(t.vals) != len(t.count) {
		return fmt.Errorf("%d values against %d reference counts", len(t.vals), len(t.count))
	}
	if n := len(t.buckets); n != 0 && n&(n-1) != 0 {
		return fmt.Errorf("the bucket array is %d long, which is not a power of two", n)
	}
	live := 0
	for ref, v := range t.vals {
		if t.count[ref] < 0 {
			return fmt.Errorf("value %d has a reference count of %d", ref, t.count[ref])
		}
		if t.count[ref] == 0 {
			continue
		}
		live++
		if got, _, ok := t.find(v); !ok || got != int32(ref) {
			return fmt.Errorf("value %d is referred to but the probe does not find it", ref)
		}
	}
	if live != t.live {
		return fmt.Errorf("%d values are referred to but %d buckets are occupied", live, t.live)
	}
	if live != len(t.vals)-len(t.free) {
		return fmt.Errorf("%d values are referred to but the free list implies %d",
			live, len(t.vals)-len(t.free))
	}
	return nil
}

func newCompositeIndex[T entityID](keys []string) *compositeIndex[T] {
	own := make([]string, len(keys))
	copy(own, keys)
	return &compositeIndex[T]{
		keys:     own,
		postings: make(map[string][]T),
		members:  make(map[T]int32),
		vtab:     newValueTable(),
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

// CompositeName is the stable identity of a declared composite, for a caller
// outside this package that has to file tuples under the same name the index
// would.
//
// Exported for the one such caller there is: a bulk load writes GCPX without
// building an index, so it needs to agree with this package about which
// composite a key tuple names. See EncodeCompositeTuple for why that agreement
// is reached by calling rather than by re-deriving.
func CompositeName(keys []string) string { return compositeName(keys) }

// EncodeCompositeTuple renders one value per position as the bytes a composite
// posting is filed under.
//
// This encoding is not order-preserving with respect to the values it encodes --
// it is length-prefixed, so a longer value can sort before a shorter one that
// precedes it alphabetically -- and GCPX binary-searches the tuples it holds. So
// a second implementation of it that disagreed by a byte would make that search
// bounded, silent and wrong. There is one implementation and this is how a caller
// outside the package reaches it.
func EncodeCompositeTuple(values []string) []byte { return []byte(encodeTuple(values)) }

// slotAt returns the reference at one position of a row, or noValue.
func (c *compositeIndex[T]) slotAt(row int32, pos int) int32 {
	return c.slots[int(row)*len(c.keys)+pos]
}

func (c *compositeIndex[T]) setSlot(row int32, pos int, ref int32) {
	c.slots[int(row)*len(c.keys)+pos] = ref
}

// extraKey addresses one position's overflow list.
func (c *compositeIndex[T]) extraKey(row int32, pos int) int64 {
	return int64(row)*int64(len(c.keys)) + int64(pos)
}

func (c *compositeIndex[T]) extraAt(row int32, pos int) []int32 {
	if c.extra == nil {
		return nil
	}
	return c.extra[c.extraKey(row, pos)]
}

// hasExtras reports whether any position of the row went multi-valued.
//
// A bitset rather than a probe into extra, because this is read on every
// registration and the alternative costs one map lookup per position for every
// entity in an index where any single entity ever held two values at one key.
// One bit per row is 0.125 B per entry against that.
func (c *compositeIndex[T]) hasExtras(row int32) bool {
	i := int(row) >> 6
	return i < len(c.extraRows) && c.extraRows[i]&(uint64(1)<<(uint(row)&63)) != 0
}

func (c *compositeIndex[T]) markExtras(row int32) {
	i := int(row) >> 6
	for len(c.extraRows) <= i {
		c.extraRows = append(c.extraRows, 0)
	}
	c.extraRows[i] |= uint64(1) << (uint(row) & 63)
}

func (c *compositeIndex[T]) clearExtras(row int32) {
	if i := int(row) >> 6; i < len(c.extraRows) {
		c.extraRows[i] &^= uint64(1) << (uint(row) & 63)
	}
}

// newRow returns a row with every position empty, reusing a released one where
// there is one.
func (c *compositeIndex[T]) newRow() int32 {
	if n := len(c.freeRow); n > 0 {
		row := c.freeRow[n-1]
		c.freeRow = c.freeRow[:n-1]
		return row
	}
	row := int32(len(c.slots) / len(c.keys))
	for range c.keys {
		c.slots = append(c.slots, noValue)
	}
	return row
}

// releaseRow drops every value the row holds and lists it for reuse.
//
// Called only after the row's tuples have been unfiled, because unfiling reads
// the values this releases.
func (c *compositeIndex[T]) releaseRow(row int32) {
	for pos := range c.keys {
		c.vtab.unref(c.slotAt(row, pos))
		c.setSlot(row, pos, noValue)
		if c.extra == nil {
			continue
		}
		k := c.extraKey(row, pos)
		for _, ref := range c.extra[k] {
			c.vtab.unref(ref)
		}
		delete(c.extra, k)
	}
	c.clearExtras(row)
	c.freeRow = append(c.freeRow, row)
}

// addValue records value at pos of row, reporting whether it was new.
func (c *compositeIndex[T]) addValue(row int32, pos int, value string) bool {
	if c.slotAt(row, pos) == noValue {
		c.setSlot(row, pos, c.vtab.ref(value))
		return true
	}
	if c.vtab.at(c.slotAt(row, pos)) == value {
		return false
	}
	k := c.extraKey(row, pos)
	for _, ref := range c.extra[k] {
		if c.vtab.at(ref) == value {
			return false
		}
	}
	if c.extra == nil {
		c.extra = make(map[int64][]int32)
	}
	c.extra[k] = append(c.extra[k], c.vtab.ref(value))
	c.markExtras(row)
	return true
}

// complete reports whether every position has a value, which is what it takes
// for the entity to appear under any tuple at all.
func (c *compositeIndex[T]) complete(row int32) bool {
	for pos := range c.keys {
		if c.slotAt(row, pos) == noValue {
			return false
		}
	}
	return true
}

// forEachTuple calls fn for every tuple in the cross product of the row's
// values, with position pos pinned to pinned when pin is true.
//
// Pinning is what makes registration cheap: when a value arrives at pos, the
// tuples that become reachable are exactly those carrying it there, and the rest
// were filed when their own values arrived.
//
// buf belongs to the caller and must hold width entries. It is a parameter
// rather than a field because the read paths run under a shared lock and two of
// them may be walking at once; register and remove hold mu exclusively and pass
// the index's own scratch.
func (c *compositeIndex[T]) forEachTuple(row int32, pos int, pinned string, pin bool, buf []string, fn func(string)) {
	if !c.hasExtras(row) {
		// Every position holds exactly one value, which is the case for
		// essentially every entity — a position gains an overflow list only when
		// a second value arrives at it. The cross product is then the single
		// tuple the row already spells out, and pinning cannot change it,
		// because a value that reached this path is the one addValue just stored
		// there.
		for i := range c.keys {
			buf[i] = c.vtab.at(c.slotAt(row, i))
		}
		fn(encodeTuple(buf))
		return
	}
	width := len(c.keys)
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
		buf[i] = c.vtab.at(c.slotAt(row, i))
		walk(i + 1)
		for _, ref := range c.extraAt(row, i) {
			buf[i] = c.vtab.at(ref)
			walk(i + 1)
		}
	}
	walk(0)
}

// tupleBuf returns the index's own width-sized assembly buffer. Callers hold mu
// exclusively; the read paths bring their own.
func (c *compositeIndex[T]) tupleBuf() []string {
	if cap(c.scratch) < len(c.keys) {
		c.scratch = make([]string, len(c.keys))
	}
	return c.scratch[:len(c.keys)]
}

// register records that id carries value at position pos.
//
// When the entity's tuple is complete, every tuple carrying value at pos is
// filed. That is right in both directions: if pos had no value before, the
// entity was in no tuple at all and this is its whole cross product; if it did,
// the tuples not carrying value were filed when their own values arrived.
//
// s and hasBase are the attached base, threaded down from PropertyIndex because
// a composite holds no reference to one. They are used for exactly one thing: a
// row created here for an entity the base already describes is filled from the
// base first, without which a write to one member of such an entity would file
// nothing. See baseSide.hydrateCompositeRow, which is also where the case that
// makes it necessary is written out. A composite the base does not carry needs
// none of this -- its rows were filled at open -- so the hydration is gated on
// the same test the fill is skipped by.
func (c *compositeIndex[T]) register(id T, pos int, value string, s baseSide[T], hasBase bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	row, ok := c.members[id]
	if !ok {
		row = c.newRow()
		c.members[id] = row
		if hasBase && s.carriesComposite(c.keys) {
			s.hydrateCompositeRow(id, c.keys, func(p int, v string) {
				c.addValue(row, p, v)
			})
		}
	}
	if !c.addValue(row, pos, value) {
		return
	}
	if !c.complete(row) {
		// An early exit, not the thing that prevents a partial filing: the cross
		// product of a position with no values is empty, so an incomplete row
		// produces no tuples whether this returns or not. What it buys is the
		// ingest path, where every entity is incomplete until its last member
		// key arrives — without it each of those registrations walks and encodes
		// a tuple to discover it has nothing to file.
		return
	}
	c.forEachTuple(row, pos, value, true, c.tupleBuf(), func(tuple string) {
		ids, inserted := insertSorted(c.postings[tuple], id)
		if inserted {
			c.postings[tuple] = ids
			c.entries++
		}
	})
}

// remove drops every trace of id.
//
// The tuples to unfile are recomputed from the entity's own row rather than
// found by searching the postings, which is what keeps removal proportional to
// that entity instead of to the index.
func (c *compositeIndex[T]) remove(id T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	row, ok := c.members[id]
	if !ok {
		return
	}
	delete(c.members, id)
	if c.complete(row) {
		c.forEachTuple(row, 0, "", false, c.tupleBuf(), func(tuple string) {
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
	// After the unfiling and never before it: releasing the row drops the
	// references the walk above reads the values through.
	c.releaseRow(row)
}

// lookup returns the ascending ids filed under the given values, over
// base(tuple) - retracted union delta(tuple).
//
// The delta side is copied out under the read lock and the base is read after it
// is released, which is postings.lookup's arrangement and is available for the
// same reason: the answer is one slice, so the copy is bounded by the answer
// rather than by the composite.
//
// With no base, or with a base that does not carry this composite, the second
// term is absent and this is the map lookup it always was -- a composite the base
// does not carry was filled at open, so its postings already hold the image's
// entries.
func (c *compositeIndex[T]) lookup(s baseSide[T], hasBase bool, values []string) []T {
	tuple := encodeTuple(values)
	c.mu.RLock()
	ids := c.postings[tuple]
	var delta []T
	if len(ids) > 0 {
		delta = make([]T, len(ids))
		copy(delta, ids)
	}
	c.mu.RUnlock()

	if !hasBase || !s.carriesComposite(c.keys) {
		return delta
	}
	out := s.mergeCompositeRun(c.keys, tuple, delta)
	if len(out) == 0 {
		return nil
	}
	return out
}

// cardinality is lookup's size without the copy — what the planner costs a
// composite driver by. Exact, not estimated: the answer is one map lookup away,
// so there is nothing here for stats.go's two regimes to decide between.
func (c *compositeIndex[T]) cardinality(s baseSide[T], hasBase bool, values []string) int {
	tuple := encodeTuple(values)
	c.mu.RLock()
	n := len(c.postings[tuple])
	c.mu.RUnlock()

	if !hasBase || !s.carriesComposite(c.keys) {
		return n
	}
	// An upper bound once a base carries the tuple, because the retracted ids
	// are not counted out. See mergeCompositeCardinality for why that is the
	// right trade for a planner input and not for an answer.
	return s.mergeCompositeCardinality(c.keys, tuple, n)
}

// verify checks the postings against the member states it holds and reports the
// first disagreement.
//
// A composite index is maintained incrementally from a stream of registrations,
// and drift between the members and the postings is a wrong query answer rather
// than a slow one: the planner drives straight from these postings and applies
// no residual filter for the keys they cover, so a missing entry is a row the
// query silently does not return. Both directions are therefore established —
// every tuple a member state implies is filed, and every filed entry is implied
// by one.
//
// # Why the second direction is a count
//
// This used to re-derive the index: a map of tuple to a set of ids, built from
// every member, then compared with the postings. That is a whole second copy of
// the index held in the routine whose job is to be safe to run on a store that
// is already close to its ceiling — and it is not necessary. The first loop
// establishes implied ⊆ filed by probing rather than by remembering. Both sides
// are then sets, so a matching count is equality:
//
//	implied ⊆ filed  ∧  |implied| = |filed|  ⟹  implied = filed
//
// |filed| is Σ len(ids), exact because the postings are checked to be strictly
// ascending, and |implied| is the running count, exact because a member's
// values are checked to be distinct per position and its tuples are therefore
// distinct. The tuple-count check the old code ended with follows too: postings
// lists are never empty and every implied tuple has an id filed under it, so the
// two tuple sets are the same set.
//
// What the count cannot do is name the offender, so when it disagrees, the
// diagnosis runs unimpliedEntry — a second walk with the same bound, paid only
// by an index already known to be broken.
func (c *compositeIndex[T]) verify(kind string, cc *store.CancelCheck) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	name := strings.Join(c.keys, ",")
	width := len(c.keys)

	// Postings first: the second loop binary-searches these lists, which is
	// only sound once they are known to be ascending.
	filed := 0
	for _, ids := range c.postings {
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
		filed += len(ids)
	}

	// The intern table before any row is read through it: every check below
	// resolves references against it, so a table that disagrees with itself
	// would have them all reporting values no row holds.
	if err := c.vtab.verify(); err != nil {
		return errIndexf("%s composite index (%s): %s", kind, name, err.Error())
	}

	counted := 0
	buf := make([]string, width)
	// seenRow marks the rows an entity names, so the overflow lists can be
	// checked against them afterwards. One bit per row, the same bound the
	// extras bitset already costs.
	seenRow := make([]uint64, (len(c.slots)/width)/64+1)
	var verr error
	for id, row := range c.members {
		if err := cc.Step(); err != nil {
			return err
		}
		if row < 0 || int(row)*width+width > len(c.slots) {
			return errIndexf("%s composite index (%s): entity %v names row %d, outside the %d slots held",
				kind, name, id, row, len(c.slots))
		}
		if err := c.verifyRow(row); err != nil {
			return errIndexf("%s composite index (%s): entity %v %s", kind, name, id, err.Error())
		}
		if w := int(row) >> 6; seenRow[w]&(uint64(1)<<(uint(row)&63)) != 0 {
			return errIndexf("%s composite index (%s): entity %v shares row %d with another entity",
				kind, name, id, row)
		} else {
			seenRow[w] |= uint64(1) << (uint(row) & 63)
		}
		if !c.complete(row) {
			continue
		}
		c.forEachTuple(row, 0, "", false, buf, func(tuple string) {
			if verr != nil {
				return
			}
			if !containsSorted(c.postings[tuple], id) {
				verr = errIndexf("%s composite index (%s): entity %v holds values implying a tuple it is not filed under",
					kind, name, id)
				return
			}
			counted++
		})
		if verr != nil {
			return verr
		}
	}

	// Every overflow list belongs to a position of a row some entity names.
	//
	// The list is addressed by row*width+pos, so a value written at a position
	// outside the declared width does not land outside the array — it lands on a
	// different row, and is read back as that entity's value. The walk above
	// catches the consequence when that row is live, by reporting a tuple the
	// other entity is not filed under; this catches it when the row is not, and
	// catches an orphan releaseRow failed to delete, which nothing else would.
	for k := range c.extra {
		if err := cc.Step(); err != nil {
			return err
		}
		row := k / int64(width)
		if row < 0 || int(row)*width+width > len(c.slots) {
			return errIndexf("%s composite index (%s): an overflow list is held for row %d, outside the %d slots",
				kind, name, row, len(c.slots))
		}
		if seenRow[int(row)>>6]&(uint64(1)<<(uint(row)&63)) == 0 {
			return errIndexf("%s composite index (%s): an overflow list is held for row %d, which no entity names",
				kind, name, row)
		}
	}

	if counted != filed {
		return c.unimpliedEntry(kind, name, width, counted, filed, cc)
	}
	if counted != c.entries {
		return errIndexf("%s composite index (%s): entry count is %d but the member values imply %d",
			kind, name, c.entries, counted)
	}
	return nil
}

// unimpliedEntry names the filed entry that verify's counts proved must exist.
//
// Only reached when the postings hold a pair no member's values produce, and it
// re-walks them to say which one — the message the old set-rebuilding verify
// gave for free. Bounded like the rest: one member's cross product at a time,
// nothing accumulated. Caller holds c.mu.
func (c *compositeIndex[T]) unimpliedEntry(kind, name string, width, counted, filed int, cc *store.CancelCheck) error {
	buf := make([]string, width)
	for tuple, ids := range c.postings {
		if err := cc.Step(); err != nil {
			return err
		}
		for _, id := range ids {
			row, ok := c.members[id]
			if !ok {
				return errIndexf("%s composite index (%s): entity %v is filed but holds no member values",
					kind, name, id)
			}
			if !c.complete(row) {
				return errIndexf("%s composite index (%s): entity %v is filed but does not hold a value at every position",
					kind, name, id)
			}
			found := false
			c.forEachTuple(row, 0, "", false, buf, func(t string) {
				if t == tuple {
					found = true
				}
			})
			if !found {
				return errIndexf("%s composite index (%s): entity %v is filed under a tuple its own values do not produce",
					kind, name, id)
			}
		}
	}
	// Unreachable while the walk above and verify's first loop agree on what
	// they visit; reported rather than ignored, because a count that disagrees
	// with nothing findable is itself the finding.
	return errIndexf("%s composite index (%s): %d entries are filed but the member values imply %d, and every filed entry is implied",
		kind, name, filed, counted)
}

// verifyRow checks the parts of a row that forEachTuple trusts: every reference
// it can reach names a live value, no position holds the same value twice, an
// overflow list exists only at a position that is itself filled, and the extras
// bitset agrees with the overflow lists it is a summary of.
//
// A value stored beyond the declared width — which the per-entity form this
// replaced could hold and drift on unnoticed, because it was never enumerated —
// is not a state a row can represent: a row is exactly width slots. That check
// is therefore absent rather than missing.
func (c *compositeIndex[T]) verifyRow(row int32) error {
	marked := false
	for pos := range c.keys {
		slot := c.slotAt(row, pos)
		if slot != noValue {
			if err := c.vtab.verifyRef(slot); err != nil {
				return fmt.Errorf("at position %d %s", pos, err.Error())
			}
		}
		extra := c.extraAt(row, pos)
		if len(extra) == 0 {
			continue
		}
		marked = true
		if slot == noValue {
			return fmt.Errorf("holds extra values at position %d but no value there", pos)
		}
		for i, ref := range extra {
			if err := c.vtab.verifyRef(ref); err != nil {
				return fmt.Errorf("at position %d %s", pos, err.Error())
			}
			if ref == slot {
				return fmt.Errorf("holds the same value twice at position %d", pos)
			}
			for _, other := range extra[i+1:] {
				if ref == other {
					return fmt.Errorf("holds the same value twice at position %d", pos)
				}
			}
		}
	}
	if marked != c.hasExtras(row) {
		return fmt.Errorf("carries overflow values the extras bitset disagrees with")
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
func (c *compositeSet[T]) registered(id T, key, value string, s baseSide[T], hasBase bool) {
	for _, m := range c.membersOf(key) {
		m.idx.register(id, m.pos, value, s, hasBase)
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
	s baseSide[T], hasBase bool,
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
		size := idx.cardinality(s, hasBase, values)
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
func lookupComposite[T entityID](c *compositeSet[T], s baseSide[T], hasBase bool, m CompositeMatch) ([]T, bool) {
	idx, ok := c.find(m.Keys)
	if !ok {
		return nil, false
	}
	values := make([]string, len(m.Values))
	for i, v := range m.Values {
		values[i] = string(v)
	}
	return idx.lookup(s, hasBase, values), true
}
