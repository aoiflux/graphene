package index

// The disk-resident base, and the ids retracted from it.
//
// # What changes when a base is attached
//
// With no base, PropertyIndex is the whole index: the shards hold every entry
// and a lookup is a map lookup. With one, the shards hold only what has been
// registered since the base was written, and every read answers
//
//	base(key, value) − retracted ∪ delta(key, value)
//
// The three terms are not symmetric. base is immutable and, under a mapped
// image, file-backed: it is read in place and costs almost nothing resident.
// delta is the shards, unchanged in shape and in cost. retracted is one bit per
// base id — the only resident structure this arrangement adds, and the only one
// that grows with the base rather than with the writes since it.
//
// # Why a bit, and not a deletion applied to the base
//
// docs/TECHNICAL_DETAILS.md §14.7 records the hazard that sank the earlier
// attempt at a lazily-loaded index: a structure that reads part of the base into
// memory in order to answer a query can re-read an entry that was deleted after
// it was loaded, and hand back an entity that is gone. That cannot be audited
// away. It is a property of keeping two mutable copies of one fact.
//
// A retraction bit removes the second copy. The base is never written to and
// never read into a resident structure, and the bit is consulted on the way out
// of every read path, so a removed id cannot leave one. Resurrection is not
// prevented here; it is unrepresentable.
//
// # Why one bit per id is exactly the right grain
//
// This index has no per-key purge. A caller changing what one entity is indexed
// under must drop every entry for it and register the whole new set —
// NodeEntriesOf exists so that it can find out what that set was, and the log
// has a purge record per entity and an add record per entry to match. So the
// only mutation that ever reaches a base entry is "this id's entries are no
// longer these", and one bit per id says precisely that: the base's answers for
// the id are void, and whatever the delta now holds is the truth. A finer grain
// — per (id, key), or per (id, key, value) — would encode a distinction the
// update path cannot make, and would cost per entry rather than per entity.
//
// That is also why a reindex needs no special case. It purges and re-registers,
// so the id is retracted and its new entries land in the delta; the base's
// entries for it stay hidden for as long as this base lives, and the next
// compaction simply does not write them.

import (
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/aoiflux/graphene/store"
)

// baseState is a base together with the retraction sets that apply to it.
//
// The two are attached and replaced as one, because a retraction is meaningless
// apart from the base it retracts from: a bitset sized for one base's highest id
// says nothing about another's, and a bit set against entries that are no longer
// there would hide an entry that is.
type baseState struct {
	b        Base
	nodeGone retractSet
	edgeGone retractSet

	// firstFault holds the first error read out of the base, if any. Most read
	// paths on PropertyIndex have no error to return — NodesByProperty returns a
	// slice and nothing else — so a run that will not decode is recorded here
	// and reported by BaseFault and by Verify, rather than being flattened into
	// "this value has no ids". See BaseFault for why that is the shape.
	firstFault atomic.Pointer[baseFault]
}

// baseFault boxes an error so it can live in an atomic.Pointer.
type baseFault struct{ err error }

// note records the first fault and ignores the rest. The first one is the
// diagnosis; the ones after it are the same damage seen again.
func (st *baseState) note(err error) {
	if err == nil {
		return
	}
	st.firstFault.CompareAndSwap(nil, &baseFault{err: err})
}

// retractSet is the set of base ids removed since the base was written.
//
// It is a flat bitset over [0, limit] with atomic words, and it takes no lock on
// either path. The plan for this item put it behind an RWMutex and fixed a lock
// order — shard, then retracted, with RemoveNode taking retracted first — which
// is a rule somebody has to keep rather than a structure that cannot break it.
// The bitset needs neither rule: its length is fixed when the base is attached,
// because an id above the base's highest was never in the base and so can never
// need retracting, and with no resize to serialise a retraction is one atomic OR
// and a test is one atomic load.
//
// The words are allocated on the first retraction rather than at attach. A
// process that only reads — one of the two this program's consumer runs is
// read-only — never allocates them at all, and at the sizes this program targets
// that is a few megabytes it never holds.
type retractSet struct {
	// limit is the highest id the set can hold: the base's highest indexed id of
	// this kind, fixed for the base's lifetime.
	limit uint64

	// words is nil until the first retraction, and is never replaced after that.
	words atomic.Pointer[retractWords]

	// alloc serialises that one allocation and guards nothing else.
	alloc sync.Mutex
}

// retractWords is the bitset's backing array. Named so it can be addressed by an
// atomic.Pointer, which needs a type to point at rather than a slice header.
type retractWords []atomic.Uint64

// has reports whether id has been retracted.
func (s *retractSet) has(id uint64) bool {
	if id > s.limit {
		return false
	}
	w := s.words.Load()
	if w == nil {
		return false
	}
	return (*w)[id>>6].Load()&(uint64(1)<<(id&63)) != 0
}

// add retracts id.
//
// An id above the base's highest is ignored rather than refused: it was never in
// the base, so the delta is already the whole truth about it and there is
// nothing to hide.
func (s *retractSet) add(id uint64) {
	if id > s.limit {
		return
	}
	w := s.words.Load()
	if w == nil {
		w = s.allocate()
	}
	(*w)[id>>6].Or(uint64(1) << (id & 63))
}

// allocate installs the word array, once.
func (s *retractSet) allocate() *retractWords {
	s.alloc.Lock()
	defer s.alloc.Unlock()
	if w := s.words.Load(); w != nil {
		return w
	}
	w := make(retractWords, s.limit/64+1)
	s.words.Store(&w)
	return &w
}

// count reports how many ids are retracted. It is a pass over the words, for
// statistics and for tests; no read path calls it.
func (s *retractSet) count() int {
	w := s.words.Load()
	if w == nil {
		return 0
	}
	n := 0
	for i := range *w {
		n += bits.OnesCount64((*w)[i].Load())
	}
	return n
}

// bytes reports how much the set holds resident, which is the whole of what a
// base costs in memory beyond its fences and key directory.
func (s *retractSet) bytes() int {
	w := s.words.Load()
	if w == nil {
		return 0
	}
	return len(*w) * 8
}

// AttachBase makes b the disk-resident half of this index, the shards becoming
// the delta over it.
//
// Almost nothing is read from b here. Its two highest ids fix the length of the
// retraction sets, and every entry it holds is read in place, on the query that
// needs it — that is the whole point of it being a base.
//
// The exception is the declared composites, which are filled from it. A composite
// cannot be answered in place: it is an index over a tuple of keys that the
// forward direction holds separately, and answering one from the base would mean
// intersecting the member keys' runs per query, which is the work a composite
// exists to have done once. So it is done once, here. See fillCompositeFromBase
// for what that costs and why it is the one resident structure a mapped index
// still pays for.
//
// A base may be attached to an index that has none, which is what a store does
// once, at open. Replacing one belongs to compaction: it is the only thing that
// can produce a new base, and the only thing that knows which retractions
// survive into it — an id retracted from the old base is simply absent from the
// new one, so the new set starts empty and receives only what is retracted after
// the pin.
//
// A run that will not decode during the composite fill is recorded rather than
// returned: the error here is for misuse, and damage to the image is what
// BaseFault reports. A caller that will not serve a half-built composite index
// checks it afterwards, which is what a store does at open.
func (p *PropertyIndex) AttachBase(b Base) error {
	if b == nil {
		return errIndexf("index: AttachBase needs a base")
	}
	st := &baseState{b: b}
	st.nodeGone.limit = b.MaxID(NodeKind)
	st.edgeGone.limit = b.MaxID(EdgeKind)
	if !p.baseRef.CompareAndSwap(nil, st) {
		return errIndexf("index: a base is already attached")
	}
	// After the swap, not before: the fill reads through nodeBase and edgeBase,
	// which is also what records a fault, and both of those are reached from
	// baseRef.
	if s, ok := p.nodeBase(); ok {
		for _, idx := range p.nodeComposites.all() {
			fillCompositeFromBase(s, idx)
		}
	}
	if s, ok := p.edgeBase(); ok {
		for _, idx := range p.edgeComposites.all() {
			fillCompositeFromBase(s, idx)
		}
	}
	return nil
}

// fillCompositeFromBase files every entry the base holds under one composite's
// member keys into that composite.
//
// This is the resident cost a mapped index does not remove, and it is
// proportional to the entries under the composite's member keys rather than to
// the index as a whole — a store that declares no composite pays nothing, and one
// that declares a tuple over two keys pays for those two.
//
// No lock is taken over the base: it is immutable. The shard locks are not taken
// either, deliberately, because filing under one would mean holding a shard lock
// and a composite lock at once — the single thing the composite design avoids.
// The delta's own entries are filed separately, by whoever has them.
//
// Filing the same (id, pos, value) twice is a no-op, so an entry the base and the
// delta both hold costs one redundant register and nothing else.
func fillCompositeFromBase[T entityID](s baseSide[T], idx *compositeIndex[T]) {
	var vals []string
	var buf []T
	for pos, key := range idx.keys {
		vals, buf = s.mergeForEachEntry(key, nil, vals, buf,
			func(id T, value []byte) bool {
				idx.register(id, pos, string(value))
				return true
			})
	}
}

// AttachedBase returns the attached base, or nil.
func (p *PropertyIndex) AttachedBase() Base {
	if st := p.baseRef.Load(); st != nil {
		return st.b
	}
	return nil
}

// BaseFault returns the first error read out of the base, or nil.
//
// It exists because most of this index's read paths have no error to return, and
// the alternative to recording one is worse than either a panic or a lie. A run
// that will not decode is a damaged image; reporting it as "that value has no
// ids" turns damage into a wrong answer, which is the failure mode the Base
// interface's own comment refuses. So the read returns what it could — the delta
// side, which is undamaged — and the fault is recorded here, where Verify,
// VerifyIndexes and a store's statistics can all see it.
//
// A fault is sticky. The image does not repair itself, and the first error is the
// diagnosis.
func (p *PropertyIndex) BaseFault() error {
	st := p.baseRef.Load()
	if st == nil {
		return nil
	}
	if f := st.firstFault.Load(); f != nil {
		return f.err
	}
	return nil
}

// RetractedCounts returns how many base ids of each kind have been retracted,
// and how many bytes the two sets hold.
//
// Both counts are a pass over the bitsets, so this is for statistics and tests,
// not for a query path.
func (p *PropertyIndex) RetractedCounts() (nodes, edges, residentBytes int) {
	st := p.baseRef.Load()
	if st == nil {
		return 0, 0, 0
	}
	return st.nodeGone.count(), st.edgeGone.count(),
		st.nodeGone.bytes() + st.edgeGone.bytes()
}

// baseSide is one entity kind's view of the base: which kind to address it with,
// and which retraction set applies.
//
// It exists so the union helpers in union.go are written once over T instead of
// twice, once for nodes and once for edges. It is passed by value and holds no
// lock; the base it names is immutable and the retraction set is atomic, so a
// copy of it is as good as the original for as long as the caller holds it.
type baseSide[T entityID] struct {
	st   *baseState
	kind EntityKind
	gone *retractSet
}

// base returns the interface to read through.
func (s baseSide[T]) base() Base { return s.st.b }

// fault records an error the base returned. See BaseFault.
func (s baseSide[T]) fault(err error) { s.st.note(err) }

// nodeBase returns the node side of the attached base, if there is one.
//
// One atomic load, and the branch on it is the whole cost a store with no base
// pays for this file existing.
func (p *PropertyIndex) nodeBase() (baseSide[store.NodeID], bool) {
	st := p.baseRef.Load()
	if st == nil {
		return baseSide[store.NodeID]{}, false
	}
	return baseSide[store.NodeID]{st: st, kind: NodeKind, gone: &st.nodeGone}, true
}

// edgeBase is nodeBase for edges.
func (p *PropertyIndex) edgeBase() (baseSide[store.EdgeID], bool) {
	st := p.baseRef.Load()
	if st == nil {
		return baseSide[store.EdgeID]{}, false
	}
	return baseSide[store.EdgeID]{st: st, kind: EdgeKind, gone: &st.edgeGone}, true
}

// retractNode hides every base entry for id.
//
// It is called before the delta's entries for id are dropped, not after. Either
// order makes the id disappear exactly once — it is visible from the delta until
// the last shard is purged, and from nowhere afterwards — but retracting first
// is what keeps a concurrent uniqueness check honest: a check that runs in the
// window sees the id from the delta and refuses a registration it could have
// allowed, which costs a caller one retry. The other order lets it see neither
// side and allow a duplicate, which costs a caller its constraint.
func (p *PropertyIndex) retractNode(id store.NodeID) {
	if st := p.baseRef.Load(); st != nil {
		st.nodeGone.add(uint64(id))
	}
}

// retractEdge is retractNode for edges.
func (p *PropertyIndex) retractEdge(id store.EdgeID) {
	if st := p.baseRef.Load(); st != nil {
		st.edgeGone.add(uint64(id))
	}
}
