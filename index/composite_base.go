package index

// Composites that the base answers in place.
//
// # What this file changes
//
// A composite index is the one part of this index that a Base could not answer.
// Everything else — the forward postings, the reverse direction, the ordered
// runs — is read out of the image on the query that needs it, and costs the
// process a page of the file rather than a byte of its heap. A composite was
// filled into the heap at open instead, by fillCompositeFromBase, and stayed
// there for the life of the handle.
//
// That was not an oversight, and the reason is in AttachBase's comment: a
// composite is an index over a tuple of keys that the forward direction holds
// separately, so answering one from a base that carries only the forward
// direction means intersecting the member keys' runs on every query — which is
// the work a composite exists to have done once. Filling it once at open is the
// cheaper of the two things the image made possible.
//
// The third thing — write the postings into the image and read them in place
// like everything else — is what GCPX does, and this file is the index's half of
// it. It is worth what it costs: at 1,400,000 nodes the composites are 128.2 MiB
// of the 237.8 MiB a default configuration holds, 53.9%, and the largest single
// term in that column. See docs/MEMORY_MODEL.md §8.6 and §9.7.
//
// # Optional, per composite, and identified by key tuple
//
// A base may carry postings for every declared composite, for some of them, or
// for none. None is what every base written before GCPX existed carries, and the
// fill is what answers those — so this is a capability, tested for, and never a
// requirement placed on Base.
//
// Per composite rather than all-or-nothing, because the two sides are declared
// separately: an image is written with the composites the store had at its last
// compaction, and the store that opens it may declare one more. That composite
// has no postings in the image and is filled, while its neighbours are read in
// place, and nothing about the two arrangements has to agree.
//
// Identified by key tuple, never by ordinal. A store can be reopened with its
// composites declared in a different order, or with one of them dropped, and an
// ordinal would then hand one composite's postings to another — a wrong answer
// produced silently by an index, which is the failure mode this package is
// organised against. compositeName is the identity on both sides.
//
// # The three terms, and where the merge happens
//
// A base-backed composite answers the same three-term read every other path
// answers:
//
//	base(tuple) − retracted ∪ delta(tuple)
//
// with the retraction set doing exactly what it does everywhere else. That it
// works unchanged here is not luck: a retraction bit is per entity, and its
// meaning is "this entity's entries are no longer these" — which is the grain a
// composite needs, because a composite entry is derived from an entity's entries
// and is void precisely when they are. A finer grain would have needed a second
// answer for composites; this one needs none.
//
// The merge is reached from PropertyIndex rather than from compositeIndex,
// because the base and the retraction sets hang off PropertyIndex and a composite
// deliberately knows nothing outside itself. So the baseSide is threaded down
// into the read paths as a value, the way the shard paths already thread it.

import (
	"sort"
)

// CompositeBase is a Base that also carries the postings of one or more declared
// composite indexes, so that they are read in place rather than filled into the
// heap at open.
//
// It is a separate interface rather than three more methods on Base, and that is
// the whole compatibility story on this side: an image written before GCPX
// existed parses into a base that does not implement this, the type assertion
// fails, and every composite is filled exactly as it was before. There is no
// version to test and no fallback to arrange.
//
// A tuple here is always the encoding encodeTuple produces, and it is produced on
// this side of the boundary in both directions — the writer is handed the map
// keys of the resident postings, and the reader hands back the bytes it was
// given. So the encoding stays this package's business, and an implementation
// neither parses a tuple nor constructs one. That matters more than it looks: the
// encoding is little-endian length-prefixed and therefore *not* order-preserving
// with respect to the values it encodes, so two implementations of it that
// disagreed would produce a file whose binary search is bounded, silent and
// wrong.
type CompositeBase interface {
	Base

	// CompositeTuples returns the key tuples this base carries postings for,
	// each in its own declaration order. A base that carries none returns nil.
	CompositeTuples(kind EntityKind) [][]string

	// LookupComposite returns the ids filed under one encoded tuple, ascending.
	// A composite this base does not carry, or a tuple it never saw, yields an
	// empty run and no error — the same shape as Lookup, and for the same
	// reason: the delta may well hold it.
	LookupComposite(kind EntityKind, keys []string, tuple []byte) (IDRun, error)

	// ForEachCompositeTuple walks one composite's distinct tuples in ascending
	// byte order, with the ids filed under each. Returning false stops the walk
	// without an error.
	//
	// This is what a compaction reads to write the next image's postings, and
	// the ascending order is why it is a walk rather than repeated lookups: the
	// delta's tuples are sorted once and merged against this in a single pass.
	ForEachCompositeTuple(kind EntityKind, keys []string, fn func(tuple []byte, ids IDRun) bool) error
}

// compositeBaseOf returns b as a CompositeBase, or nil.
func compositeBaseOf(b Base) CompositeBase {
	cb, _ := b.(CompositeBase)
	return cb
}

// carriedComposites indexes a base's carried tuples by compositeName, for the one
// question every read path asks: does the base answer this composite, or was it
// filled?
//
// Built once when the base is installed and never written to again, so it is read
// without a lock — the same treatment baseState gives the probe costs, and for
// the same reason: the base is immutable, so anything derived from it is too.
func carriedComposites(b Base, kind EntityKind) map[string]bool {
	cb := compositeBaseOf(b)
	if cb == nil {
		return nil
	}
	tuples := cb.CompositeTuples(kind)
	if len(tuples) == 0 {
		return nil
	}
	out := make(map[string]bool, len(tuples))
	for _, t := range tuples {
		out[compositeName(t)] = true
	}
	return out
}

// carriesComposite reports whether the base answers this composite in place.
func (s baseSide[T]) carriesComposite(keys []string) bool {
	return s.carried[compositeName(keys)]
}

// compositeBase returns the base as a CompositeBase. Only ever reached after
// carriesComposite said yes, which cannot be true of a base that is not one.
func (s baseSide[T]) compositeBase() CompositeBase {
	return compositeBaseOf(s.st.b)
}

// mergeCompositeRun unions the base's ids for one tuple with the delta's,
// ascending, deduplicated and skipping retracted ids.
//
// The delta side arrives already copied out from under the composite's lock, so
// the base read — which may fault a page of the image — happens with no lock
// held. That is the point lookups' arrangement rather than the walks', and it is
// available here for the same reason it is available there: the answer is one
// slice, so copying it is bounded by the answer rather than by the key.
func (s baseSide[T]) mergeCompositeRun(keys []string, tuple string, delta []T) []T {
	run, err := s.compositeBase().LookupComposite(s.kind, keys, []byte(tuple))
	if err != nil {
		s.fault(err)
		// The delta side is undamaged and is what this can still answer. See
		// PropertyIndex.BaseFault for why a fault is recorded rather than
		// flattened into "that tuple has no ids".
		return delta
	}
	if run.Len() == 0 {
		return delta
	}
	return s.mergeRun(make([]T, 0, run.Len()+len(delta)), run, delta)
}

// mergeCompositeCardinality is the planner's size for a base-backed tuple.
//
// An upper bound, exactly as mergeCardinality is, and for the argument stated
// there: a driver must be a superset of the answer rather than the answer, so an
// over-count can cost a worse plan and can never cost a wrong result. Counting
// the retractions out would mean walking the run and testing a bit per id, on a
// path the planner takes once per candidate per query.
func (s baseSide[T]) mergeCompositeCardinality(keys []string, tuple string, deltaN int) int {
	run, err := s.compositeBase().LookupComposite(s.kind, keys, []byte(tuple))
	if err != nil {
		s.fault(err)
		return deltaN
	}
	return run.Len() + deltaN
}

// hydrateCompositeRow supplies the member values the base holds for one entity,
// so that a write to an entity the image already describes files the tuples it
// should.
//
// # Why this exists
//
// It is the one thing a base-backed composite genuinely needs that a filled one
// does not.
//
// A composite files a tuple only from a complete row: every member position must
// hold a value, because the cross product of a position with no values is empty.
// With the composite filled from the base, every entity the image held already
// had a complete row, so a later write to one of its members arrived at a row
// that only needed updating.
//
// Read in place, that row does not exist. An entity carrying bucket="b1" and
// shard="s0" in the image, whose caller now registers shard="s9" without purging
// first, would create a row holding s9 and nothing else, find it incomplete, and
// file nothing — and the tuple (b1, s9) would be held by neither side. The base
// holds (b1, s0), which is still true; the delta holds nothing, which is not.
//
// So a row created for an entity the base knows is filled from the base first.
// The values are the base's own and remain true: index entries are additive and
// an update purges before it re-registers, so an entity that was not retracted
// still carries everything the image says it does. Filing the same (id, pos,
// value) twice is a documented no-op, so a value both sides hold costs one
// redundant addValue.
//
// # What it costs, and what it does not
//
// One reverse-direction walk per entity written, not per entity in the base —
// this runs on row creation and nowhere else. A rebuild that rewrites every
// entity pays it once each and ends up holding what the fill would have held; a
// process that writes a thousand entities into a store of a million pays it a
// thousand times and holds a thousand rows. That is the same shape as the rest of
// the delta, which is the property that makes a base worth having at all.
//
// # The retraction check is a correctness guard, not an optimisation
//
// A retracted entity's base entries are void, and hydrating from them would file
// tuples out of values the caller has already dropped — resurrection, by the
// exact route retract.go's header calls unrepresentable. It stays unrepresentable
// because the bit is tested here.
//
// The bit is tested before the walk, and RemoveNode sets it before it touches
// anything else, so a hydration that observed it unset was concurrent with the
// removal rather than after it — and the composite unfiling that removal goes on
// to do is what settles that interleaving. Which is to say this is no safer and
// no less safe than registering a property for an entity being deleted underneath
// you, which is a race the store's own locking is what prevents.
func (s baseSide[T]) hydrateCompositeRow(id T, keys []string, set func(pos int, value string)) {
	if s.gone.has(uint64(id)) {
		return
	}
	b := s.st.b
	if !b.HasEntries(s.kind, uint64(id)) {
		return
	}
	err := b.ForEachEntryOf(s.kind, uint64(id), func(key string, value []byte) bool {
		for pos, k := range keys {
			if k == key {
				set(pos, string(value))
			}
		}
		return true
	})
	if err != nil {
		s.fault(err)
	}
}

// forEachMergedTuple walks one composite's distinct tuples ascending by their
// encoded bytes, with the ascending ids each is filed under, over base ∪ delta −
// retracted.
//
// This is what a compaction writes the next image's GCPX from, and the two
// orderings it promises are the two that format's reader depends on and does not
// re-check. See gcpxTupleWalker.
//
// # Why the composite's lock is held across the whole walk
//
// Because the caller is a compaction, which holds the store's write lock, so
// there is nothing to contend with and a snapshot is what the writer needs. The
// alternative — copy every tuple and every id list out first — is a second copy
// of the whole composite at exactly the moment the process is nearest its
// ceiling, which is the allocation this programme exists to remove.
//
// # The merge is driven from the base
//
// The base's walk is a push and the delta's tuples are a sorted slice, so the
// base drives and the delta is drained ahead of each base tuple. A tuple both
// sides hold is emitted once with its ids unioned; a tuple only the base holds is
// emitted with its retracted ids dropped, and may come out empty, in which case
// it is not emitted at all — a tuple every one of whose entities has been deleted
// is not a tuple the next image should carry.
func (c *compositeIndex[T]) forEachMergedTuple(
	s baseSide[T], hasBase bool,
	fn func(tuple []byte, ids []uint64) bool,
) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tuples := make([]string, 0, len(c.postings))
	for t := range c.postings {
		tuples = append(tuples, t)
	}
	sort.Strings(tuples)

	emitDelta := func(t string) bool {
		return fn([]byte(t), widen(c.postings[t]))
	}

	if !hasBase || !s.carriesComposite(c.keys) {
		for _, t := range tuples {
			if !emitDelta(t) {
				return nil
			}
		}
		return nil
	}

	i := 0
	stopped := false
	err := s.compositeBase().ForEachCompositeTuple(s.kind, c.keys,
		func(tuple []byte, run IDRun) bool {
			bt := string(tuple)
			for i < len(tuples) && tuples[i] < bt {
				if !emitDelta(tuples[i]) {
					stopped = true
					return false
				}
				i++
			}
			var delta []T
			if i < len(tuples) && tuples[i] == bt {
				delta = c.postings[tuples[i]]
				i++
			}
			merged := s.mergeRun(make([]T, 0, run.Len()+len(delta)), run, delta)
			if len(merged) == 0 {
				return true
			}
			if !fn(tuple, widen(merged)) {
				stopped = true
				return false
			}
			return true
		})
	if err != nil {
		return err
	}
	if stopped {
		return nil
	}
	for ; i < len(tuples); i++ {
		if !emitDelta(tuples[i]) {
			return nil
		}
	}
	return nil
}

// widen copies a typed id list into the untyped form the format layer writes.
//
// A copy rather than an unsafe reinterpretation: store.NodeID and store.EdgeID
// are distinct defined types over uint64 and nothing guarantees they stay that
// way, and this runs once per distinct tuple at a compaction rather than per
// query.
func widen[T entityID](ids []T) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

// reset empties a composite, keeping its declaration.
//
// Called when a base is installed that carries this composite's postings: what
// the rows and the postings held is now in the image, read in place, and holding
// it twice is the resident cost the section was written to remove. See
// PropertyIndex.SwapBase, which is the only caller and whose precondition is what
// makes this safe.
//
// The declaration survives, for the reason clearDelta keeps the ordered keys: a
// composite declared over a tuple is declared for the life of the store, and
// dropping it here would turn every query that matched it back into an
// intersection until the next reopen.
func (c *compositeIndex[T]) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postings = make(map[string][]T)
	c.members = make(map[T]int32)
	c.slots = nil
	c.freeRow = nil
	c.extra = nil
	c.extraRows = nil
	c.vtab = newValueTable()
	c.entries = 0
}

// resetCarried empties every composite the base carries, and leaves the rest.
func (c *compositeSet[T]) resetCarried(s baseSide[T]) {
	for _, idx := range c.all() {
		if s.carriesComposite(idx.keys) {
			idx.reset()
		}
	}
}

func (p *PropertyIndex) forEachNodeCompositeTuple(keys []string, fn func(tuple []byte, ids []uint64) bool) (bool, error) {
	idx, ok := p.nodeComposites.find(keys)
	if !ok {
		return false, nil
	}
	s, hasBase := p.nodeBase()
	return true, idx.forEachMergedTuple(s, hasBase, fn)
}

func (p *PropertyIndex) forEachEdgeCompositeTuple(keys []string, fn func(tuple []byte, ids []uint64) bool) (bool, error) {
	idx, ok := p.edgeComposites.find(keys)
	if !ok {
		return false, nil
	}
	s, hasBase := p.edgeBase()
	return true, idx.forEachMergedTuple(s, hasBase, fn)
}

// ForEachCompositeTuple walks one declared composite's tuples over base ∪ delta −
// retracted, in the ascending byte order a GCPX writer requires, handing each
// tuple's encoded bytes and the ascending ids filed under it.
//
// The tuple bytes are this package's encoding and are opaque to the caller: they
// are what LookupComposite will be given back, and nothing outside this package
// parses or constructs one. See CompositeBase.
//
// ok is false when nothing is declared over these keys, which is what a caller
// enumerating from CompositeNodeKeys cannot see happen.
func (p *PropertyIndex) ForEachCompositeTuple(
	kind EntityKind, keys []string,
	fn func(tuple []byte, ids []uint64) bool,
) (ok bool, err error) {
	if kind == NodeKind {
		return p.forEachNodeCompositeTuple(keys, fn)
	}
	return p.forEachEdgeCompositeTuple(keys, fn)
}
