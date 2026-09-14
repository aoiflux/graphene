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
	"context"
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

	// nodeProbe and edgeProbe are what probing one candidate of each kind costs
	// against this base. Computed once here rather than per query: the base is
	// immutable, so the figure is too, and the alternative is summing the key
	// directory on every residual pass. See reverseProbeCost.
	nodeProbe int
	edgeProbe int

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
	if !p.baseRef.CompareAndSwap(nil, newBaseState(b)) {
		return errIndexf("index: a base is already attached")
	}
	// After the swap, not before: the fill reads through nodeBase and edgeBase,
	// which is also what records a fault, and both of those are reached from
	// baseRef.
	p.fillCompositesFromBase()
	return nil
}

// newBaseState builds the state a base is installed with.
//
// Everything in it is derived from the base and nothing from the index, which is
// what lets it be built before either entry point commits to installing it: the
// probe costs are a function of the base's entry counts and the retraction sets
// are sized from its highest ids, so a state that is never stored costs two
// method calls and an allocation and leaves nothing behind.
func newBaseState(b Base) *baseState {
	st := &baseState{b: b}
	st.nodeGone.limit = b.MaxID(NodeKind)
	st.edgeGone.limit = b.MaxID(EdgeKind)
	st.nodeProbe = reverseProbeCost(b.TotalEntries(NodeKind))
	st.edgeProbe = reverseProbeCost(b.TotalEntries(EdgeKind))
	return st
}

// fillCompositesFromBase files the attached base's entries into every declared
// composite. See fillCompositeFromBase.
func (p *PropertyIndex) fillCompositesFromBase() {
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
}

// SwapBase replaces the base with one that already holds everything this index
// holds, and empties the delta behind it.
//
// # What it is for
//
// A compaction writes every entry the index holds into the image it builds, and
// then goes on holding all of them. AttachBase runs on the load path and nowhere
// else, so until something reopens the directory the process keeps the entries in
// the shards it wrote them from -- the compaction's own output, resident. At the
// shape this engine is sized for that is the largest single thing a compaction
// fails to give back: measured over a whole-layer rebuild, the index is 68% of
// the gap between a store that has just compacted and the same store reopened.
//
// This is the entry point that closes it. AttachBase cannot: it is a
// CompareAndSwap against nil, deliberately, because attaching a second base to an
// index whose delta still describes the first is how an entry goes missing.
//
// # The precondition, which is the whole of the correctness argument
//
// b must hold exactly what this index currently answers with: base ∪ delta −
// retracted, as of now. Not a superset and not a subset. The caller is a
// compaction that has just written b from this index and has established that
// nothing has been registered or retracted since -- see Store.compactCommit for
// how that is established and what happens when it cannot be.
//
// Everything else follows from it. The delta can be emptied because b holds every
// entry it held. The new retraction sets start empty because an id retracted from
// the old base is simply absent from b. The composites are left exactly as they
// are, and that is not an oversight: their content is already the entries b holds,
// so refiling from b would walk every entry under every member key to arrive back
// where it started, once per compaction.
//
// # Why the base is installed before the delta is cleared
//
// The two are separate stores and a reader can be between them. This index's read
// paths take a shard lock, release it, and then load the base -- NodesByProperty
// is the shape -- so a reader that finds an emptied shard goes on to load
// whichever base is installed by then.
//
// Installing first makes that safe by ordering. A reader that saw the cleared
// shard was necessarily after the clear, which was after the install, so the base
// it loads is b and b holds what the shard no longer does. The other order has a
// reader read an emptied shard and then load the *old* base, which answers with
// neither half.
//
// The window the safe order does leave is a reader holding the full delta and the
// new base at once, which double-counts nothing: every merge here is a set union
// over ascending ids, so an entry both sides hold is yielded once. See mergeRun.
//
// # What the caller must exclude
//
// Everything that writes. The delta is emptied shard by shard, and a registration
// landing in a shard this has already cleared is an entry b does not hold and the
// delta no longer does. A compaction holds the store's write lock across this,
// which is the same thing that makes the precondition above checkable at all.
func (p *PropertyIndex) SwapBase(b Base) error {
	if b == nil {
		return errIndexf("index: SwapBase needs a base")
	}
	p.baseRef.Store(newBaseState(b))
	p.clearDelta()
	return nil
}

// clearDelta empties the shards, keeping every declaration made over them.
//
// Declarations are schema and entries are data. A key declared ordered is
// declared for the life of the store -- the image records it in GORD and the open
// path re-states it -- so dropping the declaration here would turn every range
// query on that key back into a scan until the next reopen, which is the
// regression loadIndex's own comment exists to prevent. The structure under it is
// replaced rather than kept, because what it held is now in the base.
//
// Unique keys need nothing: uniqueness is a predicate over the postings rather
// than a second index, and the predicate reads the base as well as the delta.
func (p *PropertyIndex) clearDelta() {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		sh.nodes = newPostings[store.NodeID]()
		sh.edges = newPostings[store.EdgeID]()
		for k := range sh.orderedNodeKeys {
			sh.orderedNodeKeys[k] = newOrderedIndex[store.NodeID]()
		}
		for k := range sh.orderedEdgeKeys {
			sh.orderedEdgeKeys[k] = newOrderedIndex[store.EdgeID]()
		}
		sh.mu.Unlock()
	}
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

// VerifyBase checks the structure of the disk-resident half of the index, and
// returns nil when there is none.
//
// Separate from Verify, and the separation is the point. Verify is what a test
// calls after a handful of writes; this is a pass over every entry the image
// holds, in order to establish the things the read paths assume and cannot
// afford to re-check — the value ordering a binary search depends on, the id
// ordering an IDRun promises, and the agreement between the forward and reverse
// directions that a delete cascade depends on. Rolling the two together would put
// an O(index) pass behind a call this package makes on every store that opens
// with a Verifier.
//
// So the two are called together by the one caller that means "check everything":
// Store.VerifyIndexes, which is what `graphene verify` runs and what an operator
// runs on a store they have reason to doubt.
//
// Cancellation is honoured throughout and costs nothing to abandon: the whole
// thing is read-only, so a cancelled check means only that the question went
// unanswered. See Base.Verify for what the implementation must bound.
func (p *PropertyIndex) VerifyBase() error {
	return p.VerifyBaseCtx(context.Background())
}

// VerifyBaseCtx is VerifyBase, abandoned if ctx is cancelled.
func (p *PropertyIndex) VerifyBaseCtx(ctx context.Context) error {
	st := p.baseRef.Load()
	if st == nil {
		return nil
	}
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	// The recorded fault first. A run that would not decode has already been read
	// once, and reporting the damage that was actually hit beats reporting
	// whatever the structural pass reaches first.
	if f := st.firstFault.Load(); f != nil {
		return f.err
	}
	return st.b.Verify(&cc)
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
