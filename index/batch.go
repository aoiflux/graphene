package index

// Resolving many exact values in one pass.
//
// # What this is for, and what it is not
//
// NodesByProperty answers one value. A consumer joining an external table
// against the index calls it once per row — a million digests against a million
// indexed digests — and pays a search of the whole key for each. That is the
// right cost for a lookup and the wrong cost for a join, for a reason that is
// about the disk and not about the arithmetic: a base's values are sorted, so an
// *ascending* sequence of wants walks them monotonically, and a search that
// starts where the previous one finished turns a million random probes into one
// forward sweep. ValueCursor is that position; this file is the merge around it.
//
// It is not a faster lookup. One value through here costs a sort of one element
// and a cursor allocation more than NodesByProperty does. The win is entirely in
// the ordering, so it arrives at the scale the ordering can be amortised over.
//
// # Why a callback, and not a slice of slices
//
// The obvious signature returns [][]NodeID, one entry per value. At the size
// this exists for that is a million slice headers and a million one-element
// backing arrays — tens of megabytes and a million allocations, to hand back
// results the caller is about to fold into its own array anyway. Memory is this
// program's first constraint, so the results go to a callback that also carries
// the caller's own index, and the caller keeps whatever shape it actually wants.
// Nothing here allocates per value.
//
// The id slice handed to the callback is scratch, reused across values. That is
// the same contract ForEachNodeValue states, and for the same reason: a walk
// that allocated a result per value would reintroduce the cost it exists to
// remove.
//
// # Why the callback order is not the input order
//
// The sort is the point, so the mapped path visits values ascending. A store
// with no base has no ordering to exploit and visits them as they came, because
// making it sort would charge a memory store for a disk store's problem. So the
// order is deliberately unspecified and the callback carries i: which value a
// result belongs to is answered by the index, not by the sequence.
//
// # Which values are visited
//
// Only those with at least one live id. A value the caller named and the index
// does not hold produces no callback at all, which is what makes a probe of a
// million values against an empty key cost a million searches and no calls. It
// is also what mergeForEachValue already decided for a value whose every id has
// been retracted, and disagreeing with it here would mean the same value being
// present to one walk and absent to another.

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"

	"github.com/aoiflux/graphene/store"
)

// NodesByPropertyBatch resolves an unordered set of exact values against key in
// one pass, calling fn with the live node ids for each value that has any.
//
// fn receives the index of the value in values, not the value itself, and the id
// slice belongs to the index for the duration of the call: read it, do not
// retain it. Returning false stops the batch. The callback order is the index's
// choice — see the file comment.
//
// values is not modified. Resolving them in order needs an ordering, and this
// builds its own rather than sorting the caller's slice: a batch API that
// permuted its input would be a trap for a caller that indexes something else by
// the same positions.
func (p *PropertyIndex) NodesByPropertyBatch(key string, values [][]byte,
	fn func(i int, ids []store.NodeID) bool,
) error {
	side, hasBase := p.nodeBase()
	return batchByProperty(p, key, values, side, hasBase,
		func(sh *propertyShard) *postings[store.NodeID] { return &sh.nodes }, fn)
}

// EdgesByPropertyBatch is NodesByPropertyBatch for edges.
func (p *PropertyIndex) EdgesByPropertyBatch(key string, values [][]byte,
	fn func(i int, ids []store.EdgeID) bool,
) error {
	side, hasBase := p.edgeBase()
	return batchByProperty(p, key, values, side, hasBase,
		func(sh *propertyShard) *postings[store.EdgeID] { return &sh.edges }, fn)
}

// batchByProperty is the batch read, written once over both entity kinds.
//
// A free function rather than a method because Go methods cannot take type
// parameters, and sel rather than a second copy of the body because the only
// difference between the two kinds is which postings container of the shard is
// read.
func batchByProperty[T entityID](p *PropertyIndex, key string, values [][]byte,
	side baseSide[T], hasBase bool, sel func(*propertyShard) *postings[T],
	fn func(i int, ids []T) bool,
) error {
	if len(values) == 0 {
		return nil
	}
	sh := p.shardFor(key)

	// The base is skipped when it holds nothing for this key, and skipping it
	// skips the sort with it. A key declared since the last compaction is the
	// common case in a store that has just been written to, and it should not pay
	// for an ordering that buys it nothing.
	if hasBase {
		if distinct, _ := side.base().KeyStats(side.kind, key); distinct == 0 {
			hasBase = false
		}
	}
	if !hasBase {
		return batchDeltaOnly(sh, key, values, sel, fn)
	}

	order, err := ascendingOrder(values)
	if err != nil {
		return err
	}
	cur := side.base().Cursor(side.kind, key)

	var buf []T
	prev := -1
	for at := range values {
		i := at
		if order != nil {
			i = int(order[at].at)
		}
		// Adjacent equal values are one answer reported twice. The cursor would
		// give the same run again for a repeated want — that is its contract —
		// but the delta lookup and the merge would be repeated too, so the
		// previous result is reused instead. buf still holds it: the branch
		// returns before anything overwrites it.
		if prev >= 0 && bytes.Equal(values[i], values[prev]) {
			if len(buf) > 0 && !fn(i, buf) {
				return nil
			}
			prev = i
			continue
		}
		prev = i

		// The base is read outside the shard lock, so the one thing here that can
		// block — a page fault against the image — blocks no writer. The lock
		// then covers a map lookup and a merge over two resident lists, which is
		// strictly less than the walks in union.go hold it for.
		run, _, err := cur.Seek(values[i])
		if err != nil {
			// Recorded like any other base fault, and also returned. Every other
			// read path has to fold an unreadable run into "this value has no
			// ids" because it has no error to return; this one does, and a bulk
			// join that silently dropped rows off a damaged image would be a
			// wrong answer rather than a failure.
			side.fault(err)
			return err
		}
		sh.mu.RLock()
		buf = side.mergeRun(buf[:0], run, sel(sh).lookupRef(key, values[i]))
		sh.mu.RUnlock()
		if len(buf) > 0 && !fn(i, buf) {
			return nil
		}
	}
	return nil
}

// batchDeltaOnly is the batch with no base to merge: one map lookup per value, in
// input order.
//
// The delta's ids are copied into buf rather than handed out directly, so the
// shard lock is released before the callback runs. The alternative is holding a
// read lock across arbitrary caller code for the length of the batch, which for
// a million values is a writer stall measured in seconds. buf is reused, so the
// copy allocates once for the longest postings list in the batch.
func batchDeltaOnly[T entityID](sh *propertyShard, key string, values [][]byte,
	sel func(*propertyShard) *postings[T], fn func(i int, ids []T) bool,
) error {
	var buf []T
	for i, v := range values {
		sh.mu.RLock()
		buf = append(buf[:0], sel(sh).lookupRef(key, v)...)
		sh.mu.RUnlock()
		if len(buf) > 0 && !fn(i, buf) {
			return nil
		}
	}
	return nil
}

// ascendingOrder returns a permutation of values' indices in ascending value
// order, or nil if values is already ascending.
//
// The nil case is not an optimisation for its own sake: a caller resolving a
// sorted external table — which is the shape this API is for — then pays one
// comparison per value and no memory at all. Detecting it costs the pass that
// would otherwise be the sort's first pass anyway.
//
// int32 rather than int: four bytes per value against eight, which at a million
// values is the difference between 4 MiB of scratch and 8. A batch too long for
// an int32 to index is refused rather than truncated, because the failure it
// would otherwise produce is a silently partial answer.
//
// The sort is unstable, and deterministic all the same: pdqsort's pattern
// breaking is seeded from the length, so one input always produces one order and
// a test over a batch with repeated values stays reproducible. Stability would
// buy only that reproducibility, and it measured at 2.4x the cost of not having
// it.
func ascendingOrder(values [][]byte) ([]batchKey, error) {
	if len(values) > math.MaxInt32 {
		return nil, errIndexf("property batch: %d values exceeds the %d this can index",
			len(values), math.MaxInt32)
	}
	sorted := true
	for i := 1; i < len(values); i++ {
		if bytes.Compare(values[i-1], values[i]) > 0 {
			sorted = false
			break
		}
	}
	if sorted {
		return nil, nil
	}
	order := make([]batchKey, len(values))
	for i, v := range values {
		order[i] = batchKey{prefix: batchPrefixOf(v), at: int32(i)}
	}
	slices.SortFunc(order, func(a, b batchKey) int {
		if a.prefix != b.prefix {
			if a.prefix < b.prefix {
				return -1
			}
			return 1
		}
		return bytes.Compare(values[a.at], values[b.at])
	})
	return order, nil
}

// batchKey is one value's place in the ordering: where it came from, and enough
// of it to compare against another without going back for the rest.
//
// Eight bytes per value, and each half is there for a measured reason.
//
// The position is an int32 rather than an int because four bytes against eight is
// 6 MiB against 12 at a million and a half values, and this is scratch that
// exists only for the length of one call.
//
// The prefix is what makes the ordering affordable. Sorting positions alone
// means every one of the roughly n log n comparisons dereferences two slice
// headers into a table of values the caller allocated wherever it liked — two
// cache misses to compare four bytes. Fifty thousand 32-byte digests ordered that
// way measured 13 ms; with the prefix in the key, 5.4 ms, because almost every
// comparison settles on one 32-bit field and never reads the value at all. Four
// bytes rather than eight halves the scratch and measured *faster* than eight, so
// there is no trade to weigh: the cost of a narrower key is that values sharing
// four leading bytes fall through to comparing the values, which is what sorting
// positions alone did for every comparison.
type batchKey struct {
	prefix uint32
	at     int32
}

// batchPrefixOf is the first four bytes of a value, zero-padded on the right and
// read big-endian, so that comparing two prefixes as numbers orders them exactly
// as bytes.Compare orders the bytes they were taken from — including the case of
// a value shorter than four bytes, which pads with the zeros that bytes.Compare
// treats a shorter value as ending before.
//
// This is the same trick and the same argument as gpix's own eight-byte value
// prefix, at half the width. It is written here rather than shared because index
// cannot import disk, and because the two widths answer different questions: that
// one skips a disk read, this one skips a pointer chase.
func batchPrefixOf(v []byte) uint32 {
	var b [4]byte
	copy(b[:], v)
	return binary.BigEndian.Uint32(b[:])
}
