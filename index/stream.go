package index

// Reading one key=value without building its answer.
//
// NodesByProperty returns a slice, and for almost every caller that is the right
// shape: the ids are few, the caller keeps them, and a copy is what makes keeping
// them safe. It stops being the right shape at the size this program exists for.
// A key the base holds a million ids under costs eight megabytes to answer and
// eight more to filter for liveness, and a caller that reads one id, loads the
// node behind it and forgets both never needed either.
//
// An IDStream is that answer, resumable. It holds the delta's side — copied once
// under the shard lock, because a postings list is the shard's own and the next
// write may reallocate it — and a cursor over the base's, which is a view into
// the image and is not copied at all. Both sides are walked to the end or
// abandoned in the middle; nothing between the two is ever materialised.
//
// # What it does not save
//
// The delta's side is still copied, so a store with no base, or one written to
// heavily since its last compaction, pays what NodesByProperty pays for that
// half. The copy is not avoidable here: the alternative is to hold the shard's
// read lock for as long as the caller takes to consume the stream, and a shard
// lock held across caller code is the deadlock disk/scan.go's header describes.
//
// # Why it is resumable rather than a callback
//
// A callback form would be shorter and would suit this package, whose walks are
// all callbacks. It does not suit the layer above: a store has to resolve every
// id against its records, that resolution needs the store's read lock, and the
// lock must not be held across the caller's sink. So the layer above wants to
// take ids a bounded batch at a time and step away between batches, which is
// what a cursor lets it do and what a callback would not.

import "github.com/aoiflux/graphene/store"

// IDStream yields the ids registered under one key=value, ascending and
// deduplicated, from the base and the delta together.
//
// It is a read of a fixed pair of sides: the delta's as it stood when the stream
// was made, the base's for as long as the base lives. Writes landing afterwards
// are not seen, which is the same reading NodesByProperty gives — it copies at
// one instant too — and is what lets the stream be consumed without a lock.
//
// Not safe for concurrent use. One goroutine drains one stream.
type IDStream[T entityID] struct {
	m runMerge[T]
}

// Next returns the next id, or false once the stream is drained.
func (s *IDStream[T]) Next() (T, bool) { return s.m.next() }

// Fill appends at most limit ids to dst and reports whether the stream is
// drained, for a caller that wants to hold a lock per batch rather than per id.
//
// A limit of zero or less appends nothing and reports not-drained, so that a
// caller whose batch size is computed cannot turn a bug into an infinite loop
// that also looks like a finished stream.
func (s *IDStream[T]) Fill(dst []T, limit int) ([]T, bool) {
	if limit <= 0 {
		return dst, false
	}
	for len(dst) < limit {
		id, ok := s.Next()
		if !ok {
			return dst, true
		}
		dst = append(dst, id)
	}
	return dst, false
}

// NodesByPropertyStream returns a stream over the node ids under key=value.
//
// The shard lock is taken and released here, not held for the stream's life: what
// crosses the boundary is a copy of the delta's postings and a cursor over the
// base, and neither needs the lock again.
func (p *PropertyIndex) NodesByPropertyStream(key string, value []byte) *IDStream[store.NodeID] {
	sh := p.shardFor(key)
	sh.mu.RLock()
	delta := sh.nodes.lookup(key, string(value))
	sh.mu.RUnlock()
	if s, hasBase := p.nodeBase(); hasBase {
		return &IDStream[store.NodeID]{m: s.lookupMerge(key, value, delta)}
	}
	// With no base the run is empty, and runMerge with an empty run is a walk of
	// the delta alone. Stating it as the zero value rather than as a second
	// branch inside next keeps one merge rather than two.
	return &IDStream[store.NodeID]{m: runMerge[store.NodeID]{delta: delta}}
}

// EdgesByPropertyStream is NodesByPropertyStream for edges.
func (p *PropertyIndex) EdgesByPropertyStream(key string, value []byte) *IDStream[store.EdgeID] {
	sh := p.shardFor(key)
	sh.mu.RLock()
	delta := sh.edges.lookup(key, string(value))
	sh.mu.RUnlock()
	if s, hasBase := p.edgeBase(); hasBase {
		return &IDStream[store.EdgeID]{m: s.lookupMerge(key, value, delta)}
	}
	return &IDStream[store.EdgeID]{m: runMerge[store.EdgeID]{delta: delta}}
}
