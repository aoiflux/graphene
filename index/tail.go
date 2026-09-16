package index

// What a compaction does about the writes that land while it builds.
//
// SwapBase asks for a base holding exactly what the index answers with, and a
// compaction can only promise that if nothing was registered or retracted
// between the pin and the commit. So the adoption was gated on the log not
// having grown, and a compaction that ran while anything was writing gave
// nothing back -- which is every compaction during a bulk ingest, the one
// workload the bound exists for. The gate was a floor of "no worse than
// before"; this is the thing that makes it unnecessary.
//
// A Tail is the difference recorded rather than excluded. While it is open the
// index appends every mutation it applies to a flat log, and the commit replays
// that log over the swapped base: the base holds what the index held at the pin,
// the replay puts back what landed after it, and the union is what the index
// answered with all along. The precondition is met by reconstruction instead of
// by waiting for a quiet moment.
//
// # Why a log of operations and not a second set of shards
//
// The plan this came from proposed splitting the shards at the pin, the way the
// record delta is split -- freeze what is there, write into fresh structures,
// drop the frozen set at the commit. That closes one of the two directions and
// not the other. A registration landing during the build is in the new shards
// and survives; a *retraction* is not, because a retraction against a base is
// not a shard entry at all. It is a bit in baseState.nodeGone, and newBaseState
// starts those sets empty by design -- an id retracted from the old base is
// simply absent from a base written after it. That is exactly wrong here: the
// image was built from records pinned before the delete, so it *does* hold the
// entry, and a swap that forgets the retraction brings a deleted entity's
// properties back from the dead until the next reopen.
//
// An operation log carries both because it records what was called rather than
// what it left behind. Replaying it through RemoveNode re-retracts against the
// base now installed, and replaying a registration through IndexNode re-files it
// in the shards and in whatever composites are declared over it. There is one
// code path, the live one, and no second implementation of what a mutation means.
//
// # What it costs, and why it is smaller than it looks
//
// One entry is an id, two string headers and a discriminant. Neither string is
// copied: the key is the caller's and the value is the same interned string the
// shard was handed, so a recorded registration adds a slice element and no bytes
// of backing. Against the ~160 B an entry costs in the shards, recording it
// again costs TailOpBytes -- and it is transient, released the moment the commit
// has replayed it.
//
// It is still unbounded in principle -- a firehose against a long build -- so a
// capture has a limit and gives up cleanly when it is passed. A dropped capture
// is not an error: the commit sees an incomplete tail, declines the adoption and
// takes the path it took before this existed. The memory result is lost and
// nothing else is.
//
// # What the caller must exclude
//
// Mutations must be serialised with respect to each other for the log's order to
// be the order they were applied in. Store.compactCommit's callers have always
// been: every mutator holds the store's write lock across journalling and
// applying, precisely so that WAL order matches apply order, and index
// registration is not exempt (see Store.indexNodePropertyLocked). The capture
// inherits that invariant rather than adding one.

import (
	"sync"

	"github.com/aoiflux/graphene/store"
)

// TailOpBytes is what one recorded mutation is charged.
//
// An accounting figure rather than a measurement, and deliberately so twice
// over. The key and the value are not in it because neither is copied -- see the
// file comment. And it is tailOp's width on a 64-bit build stated as a constant
// rather than read out of unsafe.Sizeof, so a 32-bit build, where the two string
// headers are half as wide, is charged more than it holds. A bound that errs
// towards declining is the right direction for one whose failure is a memory
// result not taken; TestTail_ChargeIsNeverAnUndercharge holds the inequality.
//
// A caller sizing a capture multiplies this by the mutations it is willing to
// hold, and gets what it asked for rather than a figure to add a margin to. See
// tailBlockOps for why the limit is not approached in doublings.
const TailOpBytes = 48

// tailBlockOps is how many operations one block of the log holds.
//
// The log is a list of fixed blocks rather than one growing slice, and the
// reason is allocator churn rather than elegance. Go grows a large slice by a
// quarter and copies it, so an append-only log of n operations allocates roughly
// five times what it ends up holding and copies most of it several times. That
// was measured rather than assumed: over a million registrations one growing
// slice cost 269 B of garbage per mutation against the 48 it kept, and put the
// registration path at 1.2-1.3x while a capture was open. In fixed blocks the
// allocation is 48 B per mutation and nothing else, no operation is ever copied,
// the figure the limit is compared against is the figure actually held, and the
// wall clock is inside the noise. BenchmarkIndexNodeCaptureOn is both arms.
//
// 1,024 is one 48 KiB block, which is large enough that the per-block bookkeeping
// disappears and small enough that a capture recording a handful of writes is not
// charged a megabyte to do it.
const tailBlockOps = 1024

type tailKind uint8

const (
	tailNodeReg tailKind = iota
	tailEdgeReg
	tailNodeDel
	tailEdgeDel
)

// tailOp is one mutation, as it was called.
type tailOp struct {
	id    uint64
	key   string
	value string
	kind  tailKind
}

// Tail is the log of index mutations applied since a capture was opened.
//
// Safe for concurrent append, which costs one mutex per mutation while a capture
// is open and one atomic load when none is. Nothing reads it until the capture
// has been stopped.
type Tail struct {
	mu      sync.Mutex
	blocks  [][]tailOp
	n       int64
	limit   int64
	dropped bool
}

// Len reports how many mutations the tail holds, and 0 once it has been dropped.
func (t *Tail) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return int(t.n)
}

// Bytes reports what the recorded mutations cost, by the accounting TailOpBytes
// describes.
func (t *Tail) Bytes() int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.n * TailOpBytes
}

// Complete reports whether this tail holds every mutation applied since it was
// opened.
//
// A nil tail is complete and empty, which is the right answer for a caller that
// did not ask for a capture and the wrong one for a caller that asked and lost
// the handle. Only the first of those is a thing this package can be handed.
func (t *Tail) Complete() bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.dropped
}

// append records one mutation, or notes that it could not.
//
// Past the limit the recorded operations are released rather than kept. A
// partial log is worth nothing -- replaying a prefix of a sequence of mutations
// is not a prefix of their effect -- so holding it would be paying the memory
// for a result already lost.
func (t *Tail) append(op tailOp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dropped {
		return
	}
	if (t.n+1)*TailOpBytes > t.limit {
		t.dropped = true
		t.blocks = nil
		t.n = 0
		return
	}
	last := len(t.blocks) - 1
	if last < 0 || len(t.blocks[last]) == tailBlockOps {
		t.blocks = append(t.blocks, make([]tailOp, 0, tailBlockOps))
		last++
	}
	t.blocks[last] = append(t.blocks[last], op)
	t.n++
}

// apply replays the tail through the index's live mutation paths.
//
// In order, and through the exported methods rather than through the structures
// under them, because the order and the methods are the whole argument that the
// result is what the index held. A delete that followed a registration must
// clear what that registration filed, in the shards and in every composite
// declared over it, and the one thing that certainly does that correctly is the
// call the store made in the first place.
//
// Registrations replay through IndexNode and not IndexNodeUnique. The entry was
// accepted once already; re-checking it against a base that now holds it is a
// constraint failing on the value it is protecting.
func (t *Tail) apply(p *PropertyIndex) {
	if t == nil {
		return
	}
	t.mu.Lock()
	blocks := t.blocks
	t.mu.Unlock()
	for _, block := range blocks {
		for _, op := range block {
			switch op.kind {
			case tailNodeReg:
				p.IndexNode(store.NodeID(op.id), op.key, []byte(op.value))
			case tailEdgeReg:
				p.IndexEdge(store.EdgeID(op.id), op.key, []byte(op.value))
			case tailNodeDel:
				p.RemoveNode(store.NodeID(op.id))
			case tailEdgeDel:
				p.RemoveEdge(store.EdgeID(op.id))
			}
		}
	}
}

// CaptureTail begins recording every mutation this index applies, and returns
// the log to pass back to SwapBase.
//
// maxBytes bounds what the recording is allowed to hold, by TailOpBytes's
// accounting. A capture that would pass it is dropped -- see Tail.Complete --
// and the caller is expected to decline whatever the capture was for rather than
// to treat it as a failure.
//
// One capture at a time. A second call while one is open replaces it, which is a
// caller bug rather than a supported mode: the replaced log is the evidence some
// other operation was relying on. A store serialises this behind the same flag
// that stops two compactions overlapping.
func (p *PropertyIndex) CaptureTail(maxBytes int64) *Tail {
	t := &Tail{limit: maxBytes}
	p.tail.Store(t)
	return t
}

// StopCapture ends the recording and returns what it holds, or nil if none was
// open. Idempotent, so every path out of the operation that opened one can call
// it without first working out whether an earlier path already did.
func (p *PropertyIndex) StopCapture() *Tail {
	return p.tail.Swap(nil)
}

// capture records one mutation if a capture is open.
//
// The whole cost when none is -- the common case, and the case every write on
// every store that never compacts is in -- is one atomic load and a branch, the
// same shape compDeclared already uses on this path and for the same reason.
func (p *PropertyIndex) capture(kind tailKind, id uint64, key, value string) {
	if t := p.tail.Load(); t != nil {
		t.append(tailOp{id: id, key: key, value: value, kind: kind})
	}
}
