package index

// Reading a few indexed values for many entities, without their records.
//
// # What this is for
//
// A caller that wants three fields of a million records reads a million records
// to get them. Under a mapped image that means faulting in a million payloads —
// tens of kilobytes apiece for the shape this engine is sized for — to reach a
// few dozen bytes of each. The index already holds those values: they were
// handed to IndexNode, and the reverse map knows which entity carries which. So
// the read exists; what was missing was a way to ask for it that does not cost
// more than the records did.
//
// The crossover is not a detail, it is the whole result. Measured against
// GetNodesBatch on the same store, four keys a node, 25,000 nodes, medians of
// four rounds:
//
//	payload   record path   this
//	  512 B         89 ns   418 ns
//	  4 KiB        172 ns   449 ns
//	 16 KiB      6,728 ns   502 ns
//	 64 KiB     32,556 ns   559 ns
//
// This column is flat because it never touches a payload; the record column is
// not, and what moves it is not really the payload size but whether the store
// still fits in page cache — payload size is what decides that. So the honest
// statement is: below the crossover this costs about 300 ns a record more than
// reading the record, and so pays for itself the moment the caller's own decode
// of those fields exceeds 300 ns; above it there is no crossover to discuss.
//
// It allocates less than reading the records at every size: 434,704 B and **150
// allocations** for 25,000 records, against 2,009,600 B and 25,002. See
// TECHNICAL_DETAILS.md §14.29.
//
// # Why a callback with indices, and not a map per entity
//
// The same argument batch.go makes. The obvious signature returns one
// map[string][]byte per id, which at the size this exists for is a million maps
// to describe a few million small values, and the caller folds them into its own
// shape immediately. So results go to a callback carrying the caller's own
// positions — which id, which key — and the caller keeps whatever shape it
// wanted. Nothing here allocates per entity.
//
// An entity may carry several values under one key; the callback is called once
// per (entity, key, value), so that is expressible rather than silently reduced
// to the first.
//
// # Why it batches
//
// A shard lock must not be held across the caller's callback. That is not a
// preference: the caller's sink is expected to read the store, the store's read
// path takes its own locks, and a shard lock held across arbitrary caller code
// is the deadlock disk/scan.go's header describes and index/walk_lock_test.go
// pins. So each chunk of ids is collected under the locks into a scratch buffer
// and yielded once the locks are released.
//
// The values handed to the callback live in that buffer and are reused by the
// next chunk. Read them, do not retain them — the same contract
// NodesByPropertyBatch states for its id slice, for the same reason.

import (
	"github.com/aoiflux/graphene/store"
)

// projectChunk is how many ids are resolved under the locks before the batch is
// handed to the callback.
//
// It bounds the scratch buffer rather than the work: at the shape this is for —
// a handful of keys, values of a few dozen bytes — 512 ids is tens of kilobytes
// held, and the lock is taken once per shard per chunk instead of once per id.
const projectChunk = 512

// projHit is one (entity, key, value) result, with the value held as a range
// into the batch's slab so a hit costs no allocation of its own.
type projHit struct {
	idIdx  int32
	keyIdx int32
	off    int32
	length int32
}

// projBuf is the scratch a projection collects into under the locks and yields
// from once they are released. It is reused across chunks.
//
// head and next are a bucket chain over the hits, keyed by an id's position
// within the chunk. They exist so the base's half can be de-duplicated against
// the delta's without a per-chunk map or a scan of every hit: an entry the base
// holds and the delta has re-registered is one entry, and a projection that
// yielded it twice would make an idempotent re-ingest double-count. That is the
// same union NodeEntriesOf performs, and it dedups for the same reason.
type projBuf struct {
	hits []projHit
	vals []byte
	head []int32 // per id-within-chunk, index of its newest hit, or -1
	next []int32 // parallel to hits: the previous hit for the same id
	base int     // the chunk's first id index, so head can be addressed from 0
}

func (b *projBuf) reset(chunkStart, chunkLen int) {
	b.hits = b.hits[:0]
	b.vals = b.vals[:0]
	b.next = b.next[:0]
	b.base = chunkStart
	if cap(b.head) < chunkLen {
		b.head = make([]int32, chunkLen)
	}
	b.head = b.head[:chunkLen]
	for i := range b.head {
		b.head[i] = -1
	}
}

// hasHit reports whether this chunk already holds exactly this (id, key, value).
func (b *projBuf) hasHit(idIdx, keyIdx int, value []byte) bool {
	for h := b.head[idIdx-b.base]; h >= 0; h = b.next[h] {
		hit := b.hits[h]
		if int(hit.keyIdx) != keyIdx || int(hit.length) != len(value) {
			continue
		}
		if string(b.vals[hit.off:hit.off+hit.length]) == string(value) {
			return true
		}
	}
	return false
}

// add copies one value into the slab and records where it went.
//
// The copy is what lets the locks be released before the callback runs, and it
// is also what makes a base entry safe to hand out: a base's bytes are a view
// into the image, and a compaction may install a different base while a long
// walk is in flight.
func (b *projBuf) add(idIdx, keyIdx int, value []byte) {
	off := len(b.vals)
	b.vals = append(b.vals, value...)
	b.record(idIdx, keyIdx, off, len(value))
}

// addString is add for a value already held as a string, which is how the delta
// holds one. Appending a string to a byte slice copies without the allocation
// a []byte conversion would make first — and at four keys an entity that
// conversion was an allocation per hit.
func (b *projBuf) addString(idIdx, keyIdx int, value string) {
	off := len(b.vals)
	b.vals = append(b.vals, value...)
	b.record(idIdx, keyIdx, off, len(value))
}

// record files the hit that add and addString have just copied bytes for.
func (b *projBuf) record(idIdx, keyIdx, off, length int) {
	b.hits = append(b.hits, projHit{
		idIdx:  int32(idIdx),
		keyIdx: int32(keyIdx),
		off:    int32(off),
		length: int32(length),
	})
	slot := idIdx - b.base
	b.next = append(b.next, b.head[slot])
	b.head[slot] = int32(len(b.hits) - 1)
}

// yield replays the batch to fn with the locks released, reporting whether to
// continue.
func (b *projBuf) yield(fn func(idIdx, keyIdx int, value []byte) bool) bool {
	for _, h := range b.hits {
		if !fn(int(h.idIdx), int(h.keyIdx), b.vals[h.off:h.off+h.length]) {
			return false
		}
	}
	return true
}

// keyPlan is the keys a projection wants, grouped by the shard that owns them.
//
// Grouping is the point. A key lives in exactly one shard, so asking for four
// keys touches at most four shards — against the sixteen NodeEntriesOf takes,
// because it answers "everything this entity is indexed under" and cannot know
// where that is. Naming the keys is what turns sixteen locks a record into four
// locks a chunk.
type keyPlan struct {
	shards []int    // distinct shard indices, ascending
	byKey  []int    // shard index per requested key, parallel to the caller's keys
	names  []string // the caller's keys, so a shard can match its own
}

func (p *PropertyIndex) planKeys(keys []string) keyPlan {
	plan := keyPlan{byKey: make([]int, len(keys)), names: keys}
	seen := make(map[int]struct{}, len(keys))
	for i, k := range keys {
		s := p.shardIndexFor(k)
		plan.byKey[i] = s
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			plan.shards = append(plan.shards, s)
		}
	}
	// Ascending, so every projection takes the shards it needs in one order.
	// Nothing here holds two at once, so this is not required for correctness;
	// it is required for the next person who adds something that does.
	for i := 1; i < len(plan.shards); i++ {
		for j := i; j > 0 && plan.shards[j] < plan.shards[j-1]; j-- {
			plan.shards[j], plan.shards[j-1] = plan.shards[j-1], plan.shards[j]
		}
	}
	return plan
}

// ProjectNodes calls fn once per (id, key, value) the index holds for the
// requested keys, for the requested ids.
//
// fn receives positions rather than values for the first two arguments: idIdx
// indexes ids, keyIdx indexes keys. The value is scratch owned by the index for
// the duration of the call — read it, do not retain it. Returning false stops
// the projection.
//
// **This is a read of the index, not of the records.** The values are the ones
// handed to IndexNode; if a caller indexed something other than what it put in
// the payload, this returns what it indexed. Keys that were never indexed
// produce no callback at all, which is how a caller asking for a non-indexed key
// finds out: silence, not an error.
//
// The order is unspecified and deliberately so — the delta's side is read shard
// by shard and the base's per entity, and imposing an order over the two would
// mean materialising both. A caller that needs its results positioned uses the
// indices, which is why they are there.
//
// Ids that carry no requested key produce nothing. Duplicate ids are resolved
// once per occurrence, at each occurrence's own index.
func (p *PropertyIndex) ProjectNodes(ids []store.NodeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) {
	projectEntities(p, ids, keys, fn,
		func(sh *propertyShard) *postings[store.NodeID] { return &sh.nodes },
		p.nodeBase)
}

// ProjectEdges is ProjectNodes for edges.
func (p *PropertyIndex) ProjectEdges(ids []store.EdgeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) {
	projectEntities(p, ids, keys, fn,
		func(sh *propertyShard) *postings[store.EdgeID] { return &sh.edges },
		p.edgeBase)
}

// projectEntities is the shared body. The two kinds differ only in which
// postings container a shard offers and which base side answers, so they are
// parameters rather than a second copy of the batching and the locking.
func projectEntities[T entityID](
	p *PropertyIndex,
	ids []T,
	keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
	postingsOf func(*propertyShard) *postings[T],
	baseOf func() (baseSide[T], bool),
) {
	if len(ids) == 0 || len(keys) == 0 {
		return
	}
	plan := p.planKeys(keys)
	base, hasBase := baseOf()

	var buf projBuf
	for start := 0; start < len(ids); start += projectChunk {
		end := min(start+projectChunk, len(ids))
		buf.reset(start, end-start)

		for _, si := range plan.shards {
			sh := &p.shards[si]
			sh.mu.RLock()
			pst := postingsOf(sh)
			// Resolve the interned key ids once per shard per chunk rather than
			// once per entity: the table is the shard's and cannot change while
			// the lock is held.
			wanted := projWanted(pst, plan, si)
			if len(wanted) > 0 {
				// The callback is built once per shard, not once per id. Built
				// inside the loop it would capture a fresh i each time, which is
				// a closure on the heap per entity — measured at one allocation
				// a record, which for a method whose whole point is to allocate
				// nothing per record is the entire budget.
				cur := 0
				visit := func(r propRef) bool {
					if k, ok := wanted[r.keyID]; ok {
						buf.addString(cur, k, r.value)
					}
					return true
				}
				for i := start; i < end; i++ {
					cur = i
					pst.forEachRef(ids[i], visit)
				}
			}
			sh.mu.RUnlock()
		}

		if hasBase {
			// One closure for the chunk, for the same reason as above.
			cur := 0
			visit := func(key string, value []byte) bool {
				for k, name := range plan.names {
					if name != key {
						continue
					}
					// The delta may have re-registered what the base already
					// held; that is one entry, not two.
					if !buf.hasHit(cur, k, value) {
						buf.add(cur, k, value)
					}
				}
				return true
			}
			b := base.base()
			for i := start; i < end; i++ {
				if base.gone.has(uint64(ids[i])) {
					continue
				}
				cur = i
				if err := b.ForEachEntryOf(base.kind, uint64(ids[i]), visit); err != nil {
					base.fault(err)
					break
				}
			}
		}

		if !buf.yield(fn) {
			return
		}
	}
}

// projWanted maps this shard's interned key ids to the caller's key positions.
//
// A key the shard has never seen has no interned id and simply does not appear,
// so an unindexed key costs one map lookup for the whole projection rather than
// a comparison per entity.
func projWanted[T entityID](pst *postings[T], plan keyPlan, shard int) map[uint32]int {
	var wanted map[uint32]int
	for k, name := range plan.names {
		if plan.byKey[k] != shard {
			continue
		}
		id, ok := pst.keyIDs[name]
		if !ok {
			continue
		}
		if wanted == nil {
			wanted = make(map[uint32]int, len(plan.names))
		}
		wanted[id] = k
	}
	return wanted
}
