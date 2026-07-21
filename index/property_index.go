package index

import (
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"github.com/aoiflux/graphene/store"
)

// PropertyIndex is a secondary index from (property key, encoded value) to the
// set of node or edge IDs that carry that property value.
//
// Properties in Graphene are stored as opaque msgpack blobs. The property index
// operates one level above raw storage: callers decode the blob (or know the
// encoding) and explicitly register individual key-value pairs for indexing by
// calling IndexNode / IndexEdge.  This keeps the storage layer schema-agnostic
// while still enabling O(1) lookups on frequently queried fields (e.g. hash,
// filename, timestamp string).
//
// value is stored internally as a string key derived from the raw byte slice,
// so any deterministic encoding (msgpack, raw bytes, string cast) works as long
// as the same encoding is used for both IndexNode and NodesByProperty calls.
//
// # Structure
//
//   - Postings lists are kept sorted by ID. Membership and insertion are
//     O(log n) + memmove, lookups return an already-ordered slice (so the query
//     path can skip sorting), and duplicate registrations are idempotent.
//   - A reverse map from ID to its registered (key, value) pairs makes
//     RemoveNode / RemoveEdge proportional to that entity's own entries rather
//     than to the size of the whole index.
//
// # Sharding
//
// The index is split into propertyShards independent shards, chosen by hashing
// the property key. One global lock meant that registering "sha256" on one
// goroutine blocked a lookup of "bucket" on another even though the two share no
// state; with per-key shards, unrelated keys no longer contend.
//
// **The reverse map is sharded alongside the forward map, not by ID.** Each
// shard holds only the (key, value) pairs for keys it owns, so removing an
// entity is a pass over the shards with each one taking its own lock
// independently — no operation ever needs two shard locks at once, which means
// there is no lock ordering to get wrong and no deadlock to reason about. The
// cost is that RemoveNode touches every shard instead of one map, which is a
// handful of lookups against work that is already proportional to the entity's
// entries.
//
// PropertyIndex is safe for concurrent use.
type PropertyIndex struct {
	shards [propertyShards]propertyShard
}

// propertyShards must be a power of two so the hash can be masked.
const propertyShards = 16

type propertyShard struct {
	mu    sync.RWMutex
	nodes postings[store.NodeID]
	edges postings[store.EdgeID]

	// Keys declared ordered, with their sorted value structures. Declaring a key
	// is opt-in because it changes how that key's range predicates compare —
	// see orderedIndex and index/encoding.
	orderedNodeKeys map[string]*orderedIndex[store.NodeID]
	orderedEdgeKeys map[string]*orderedIndex[store.EdgeID]
}

// shardFor returns the shard owning key.
//
// FNV-1a: cheap, allocation-free over a string, and well enough distributed for
// the handful of distinct property keys a workload typically registers.
func (p *PropertyIndex) shardFor(key string) *propertyShard {
	return &p.shards[p.shardIndexFor(key)]
}

// shardIndexFor is shardFor as an index, which bulk loading needs so it can
// group entries by shard before touching any of them.
func (p *PropertyIndex) shardIndexFor(key string) int {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return int(h & (propertyShards - 1))
}

// errIndexf builds an index consistency error.
func errIndexf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}

// NodePropEntry is a single (nodeID, key, value) tuple used when enumerating
// all indexed node property entries (e.g. for WAL re-emission after compaction).
type NodePropEntry struct {
	ID    store.NodeID
	Key   string
	Value []byte
}

// EdgePropEntry is a single (edgeID, key, value) tuple.
type EdgePropEntry struct {
	ID    store.EdgeID
	Key   string
	Value []byte
}

// NewPropertyIndex returns an empty PropertyIndex.
func NewPropertyIndex() *PropertyIndex {
	p := &PropertyIndex{}
	for i := range p.shards {
		p.shards[i] = propertyShard{
			nodes:           newPostings[store.NodeID](),
			edges:           newPostings[store.EdgeID](),
			orderedNodeKeys: make(map[string]*orderedIndex[store.NodeID]),
			orderedEdgeKeys: make(map[string]*orderedIndex[store.EdgeID]),
		}
	}
	return p
}

// DeclareOrderedNodeKey builds and maintains an ordered index over key, so that
// range and prefix filters on it are answered by binary search instead of a scan
// of every entry under that key.
//
// Entries already registered under key are absorbed, so this can be called at
// any point in a store's life.
//
// Declaring a key changes how its range predicates compare: from the scan path's
// "numeric when both sides parse, byte-wise otherwise" rule to plain byte order.
// Encode values with index/encoding (or use a naturally byte-ordered form such
// as fixed-width zero-padded digits or hex) so byte order means what you intend.
// Equality lookups are unaffected.
func (p *PropertyIndex) DeclareOrderedNodeKey(key string) {
	sh := p.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, exists := sh.orderedNodeKeys[key]; exists {
		return
	}
	idx := newOrderedIndex[store.NodeID]()
	for value, ids := range sh.nodes.byKey[key] {
		for _, id := range ids {
			idx.add(id, value)
		}
	}
	sh.orderedNodeKeys[key] = idx
}

// DeclareOrderedEdgeKey is DeclareOrderedNodeKey for edge properties.
func (p *PropertyIndex) DeclareOrderedEdgeKey(key string) {
	sh := p.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, exists := sh.orderedEdgeKeys[key]; exists {
		return
	}
	idx := newOrderedIndex[store.EdgeID]()
	for value, ids := range sh.edges.byKey[key] {
		for _, id := range ids {
			idx.add(id, value)
		}
	}
	sh.orderedEdgeKeys[key] = idx
}

// OrderedNodeKeys returns the declared ordered node keys, sorted.
func (p *PropertyIndex) OrderedNodeKeys() []string {
	var out []string
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		for k := range sh.orderedNodeKeys {
			out = append(out, k)
		}
		sh.mu.RUnlock()
	}
	sort.Strings(out)
	return out
}

// OrderedEdgeKeys returns the declared ordered edge keys, sorted.
func (p *PropertyIndex) OrderedEdgeKeys() []string {
	var out []string
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		for k := range sh.orderedEdgeKeys {
			out = append(out, k)
		}
		sh.mu.RUnlock()
	}
	sort.Strings(out)
	return out
}

// IndexNode records that nodeID has property key=value. Re-registering an
// identical (id, key, value) triple is a no-op.
func (p *PropertyIndex) IndexNode(id store.NodeID, key string, value []byte) {
	vk := string(value)
	sh := p.shardFor(key)
	sh.mu.Lock()
	sh.nodes.add(id, key, vk)
	if idx := sh.orderedNodeKeys[key]; idx != nil {
		idx.add(id, vk)
	}
	sh.mu.Unlock()
}

// IndexEdge records that edgeID has property key=value. Re-registering an
// identical (id, key, value) triple is a no-op.
func (p *PropertyIndex) IndexEdge(id store.EdgeID, key string, value []byte) {
	vk := string(value)
	sh := p.shardFor(key)
	sh.mu.Lock()
	sh.edges.add(id, key, vk)
	if idx := sh.orderedEdgeKeys[key]; idx != nil {
		idx.add(id, vk)
	}
	sh.mu.Unlock()
}

// RemoveNode drops every indexed entry for the given node id across all keys
// and values. Buckets left empty are removed so they do not accumulate.
func (p *PropertyIndex) RemoveNode(id store.NodeID) {
	// Each shard owns the entries for its own keys, so they are removed
	// independently — one lock at a time, never two. That is what keeps this
	// deadlock-free without any lock ordering rule.
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		if len(sh.orderedNodeKeys) > 0 {
			sh.nodes.forEachRef(id, func(ref propRef) bool {
				if idx := sh.orderedNodeKeys[ref.key]; idx != nil {
					idx.remove(id, ref.value)
				}
				return true
			})
		}
		sh.nodes.remove(id)
		sh.mu.Unlock()
	}
}

// RemoveEdge drops every indexed entry for the given edge id across all keys
// and values. Buckets left empty are removed so they do not accumulate.
func (p *PropertyIndex) RemoveEdge(id store.EdgeID) {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		if len(sh.orderedEdgeKeys) > 0 {
			sh.edges.forEachRef(id, func(ref propRef) bool {
				if idx := sh.orderedEdgeKeys[ref.key]; idx != nil {
					idx.remove(id, ref.value)
				}
				return true
			})
		}
		sh.edges.remove(id)
		sh.mu.Unlock()
	}
}

// NodesMatchingOrdered appends the IDs matching a range or prefix filter to dst,
// using the ordered index for f.Key. ok is false when the key is not declared
// ordered or the operator cannot be served from an ordering, in which case the
// caller must fall back to scanning the key's entries.
//
// Results are appended in ascending value order, then ascending ID within each
// value — not in overall ID order, so callers that need sorted IDs must sort.
func (p *PropertyIndex) NodesMatchingOrdered(dst []store.NodeID, f store.PropertyFilter) ([]store.NodeID, bool) {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	idx := sh.orderedNodeKeys[f.Key]
	if idx == nil {
		return dst, false
	}
	lo, hi, ok := idx.rangeFor(f)
	if !ok {
		return dst, false
	}
	idx.forEachInRange(lo, hi, func(id store.NodeID) bool {
		dst = append(dst, id)
		return true
	})
	return dst, true
}

// EdgesMatchingOrdered is NodesMatchingOrdered for edge properties.
func (p *PropertyIndex) EdgesMatchingOrdered(dst []store.EdgeID, f store.PropertyFilter) ([]store.EdgeID, bool) {
	sh := p.shardFor(f.Key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	idx := sh.orderedEdgeKeys[f.Key]
	if idx == nil {
		return dst, false
	}
	lo, hi, ok := idx.rangeFor(f)
	if !ok {
		return dst, false
	}
	idx.forEachInRange(lo, hi, func(id store.EdgeID) bool {
		dst = append(dst, id)
		return true
	})
	return dst, true
}

// NodesByProperty returns all NodeIDs that have an indexed entry for key=value,
// in ascending ID order. Returns nil if no match.
func (p *PropertyIndex) NodesByProperty(key string, value []byte) []store.NodeID {
	sh := p.shardFor(key)
	sh.mu.RLock()
	out := sh.nodes.lookup(key, string(value))
	sh.mu.RUnlock()
	return out
}

// EdgesByProperty returns all EdgeIDs that have an indexed entry for key=value,
// in ascending ID order. Returns nil if no match.
func (p *PropertyIndex) EdgesByProperty(key string, value []byte) []store.EdgeID {
	sh := p.shardFor(key)
	sh.mu.RLock()
	out := sh.edges.lookup(key, string(value))
	sh.mu.RUnlock()
	return out
}

// NodeCardinality returns the number of node IDs registered under key=value
// without copying the postings list. Used by the query planner to pick the most
// selective driving index.
func (p *PropertyIndex) NodeCardinality(key string, value []byte) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	n := sh.nodes.cardinality(key, string(value))
	sh.mu.RUnlock()
	return n
}

// EdgeCardinality returns the number of edge IDs registered under key=value.
func (p *PropertyIndex) EdgeCardinality(key string, value []byte) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	n := sh.edges.cardinality(key, string(value))
	sh.mu.RUnlock()
	return n
}

// ForEachNodeEntry calls fn for every (id, value) registered under key, holding
// only a read lock and allocating nothing. Return false from fn to stop early.
//
// This is the scan path for operators the index cannot answer directly (prefix,
// contains, and the ordered comparisons). It touches only the buckets belonging
// to key, unlike NodeEntries which materialises the entire index.
func (p *PropertyIndex) ForEachNodeEntry(key string, fn func(id store.NodeID, value []byte) bool) {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	sh.nodes.forEach(key, fn)
}

// ForEachEdgeEntry calls fn for every (id, value) registered under key.
// Return false from fn to stop early.
func (p *PropertyIndex) ForEachEdgeEntry(key string, fn func(id store.EdgeID, value []byte) bool) {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	sh.edges.forEach(key, fn)
}

// NodeEntries returns all indexed node property entries.
// Used by disk.Store.Compact() to re-emit entries to the fresh WAL.
//
// This materialises the whole index; query paths should use ForEachNodeEntry.
func (p *PropertyIndex) NodeEntries() []NodePropEntry {
	var out []NodePropEntry
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		sh.nodes.forEachAll(func(id store.NodeID, key string, value []byte) bool {
			out = append(out, NodePropEntry{ID: id, Key: key, Value: value})
			return true
		})
		sh.mu.RUnlock()
	}
	return out
}

// EdgeEntries returns all indexed edge property entries.
func (p *PropertyIndex) EdgeEntries() []EdgePropEntry {
	var out []EdgePropEntry
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		sh.edges.forEachAll(func(id store.EdgeID, key string, value []byte) bool {
			out = append(out, EdgePropEntry{ID: id, Key: key, Value: value})
			return true
		})
		sh.mu.RUnlock()
	}
	return out
}

// Verify checks the index's internal invariants and returns the first violation
// found. It is intended for tests, for `Graph.VerifyIndexes`, and for validating
// an index that was loaded from disk rather than built in memory.
//
// The invariants are:
//
//   - every postings list is strictly ascending (sorted, no duplicates);
//   - every (id, key, value) in a postings list has exactly one matching entry
//     in the reverse map, and vice versa;
//   - no bucket or key map is left empty;
//   - the cached entry count matches the number of postings entries.
//
// It cannot check whether an indexed value still reflects the entity's current
// properties — values are caller-encoded opaque bytes, so only the caller knows
// that. See store.ReindexPolicy for how that staleness is managed.
func (p *PropertyIndex) Verify() error {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		err := sh.verify()
		sh.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// verify checks one shard's invariants. Caller must hold sh.mu.
func (sh *propertyShard) verify() error {
	if err := sh.nodes.verify("node"); err != nil {
		return err
	}
	if err := sh.edges.verify("edge"); err != nil {
		return err
	}
	// Each ordered index must mirror the hash postings for its key exactly.
	for key, idx := range sh.orderedNodeKeys {
		if err := idx.verify("node", key, sh.nodes.byKey[key]); err != nil {
			return err
		}
	}
	for key, idx := range sh.orderedEdgeKeys {
		if err := idx.verify("edge", key, sh.edges.byKey[key]); err != nil {
			return err
		}
	}
	return nil
}

// IndexedNodeIDs returns every node ID that has at least one indexed entry.
// Used by integrity checks to detect postings that outlived their entity.
func (p *PropertyIndex) IndexedNodeIDs() []store.NodeID {
	seen := make(map[store.NodeID]struct{})
	var out []store.NodeID
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		sh.nodes.forEachRefID(func(id store.NodeID) {
			if _, dup := seen[id]; dup {
				return
			}
			seen[id] = struct{}{}
			out = append(out, id)
		})
		sh.mu.RUnlock()
	}
	return out
}

// IndexedEdgeIDs returns every edge ID that has at least one indexed entry.
func (p *PropertyIndex) IndexedEdgeIDs() []store.EdgeID {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		sh.edges.forEachRefID(func(id store.EdgeID) {
			if _, dup := seen[id]; dup {
				return
			}
			seen[id] = struct{}{}
			out = append(out, id)
		})
		sh.mu.RUnlock()
	}
	return out
}

// --- postings ---

// entityID constrains the postings container to the two ID types.
type entityID interface {
	~uint64
}

// propRef is one (key, value) pair an entity is registered under, used by the
// reverse map to make removal proportional to the entity's own entries.
type propRef struct {
	key   string
	value string
}

// postings holds key → value → sorted []ID together with the reverse ID → refs
// mapping. It is not itself locked; PropertyIndex owns the lock.
type postings[T entityID] struct {
	byKey map[string]map[string][]T
	count int // total number of (id, key, value) triples

	// Reverse mapping, split by arity.
	//
	// Sharding the index by key made "one entry per entity per shard" the
	// overwhelmingly common case: each key lives in exactly one shard, and an
	// entity normally carries one value per key, so it lands in each relevant
	// shard exactly once. A map[T][]propRef therefore allocated a one-element
	// backing array per entity — roughly 32 B of pure overhead on top of the map
	// entry itself, which measured as a meaningful share of the index's memory.
	//
	// ref1 holds that case inline. refN carries only entities that genuinely
	// have several entries in this shard, which needs two keys hashing here.
	// An id appears in exactly one of the two, never both.
	ref1 map[T]propRef
	refN map[T][]propRef

	// Entries per key, so the query planner can cost a scan of a key without
	// walking that key's buckets to find out how big it is.
	perKey map[string]int
}

// forEachRef visits every reverse entry registered for id. Return false from fn
// to stop early. It allocates nothing in the common single-entry case.
func (p *postings[T]) forEachRef(id T, fn func(propRef) bool) {
	if r, ok := p.ref1[id]; ok {
		fn(r)
		return
	}
	for _, r := range p.refN[id] {
		if !fn(r) {
			return
		}
	}
}

// refCount reports how many reverse entries id has.
func (p *postings[T]) refCount(id T) int {
	if _, ok := p.ref1[id]; ok {
		return 1
	}
	return len(p.refN[id])
}

// hasRefs reports whether id has any reverse entry.
func (p *postings[T]) hasRefs(id T) bool {
	if _, ok := p.ref1[id]; ok {
		return true
	}
	_, ok := p.refN[id]
	return ok
}

// addRef records one reverse entry, promoting to the overflow map on the second.
func (p *postings[T]) addRef(id T, ref propRef) {
	if existing, ok := p.ref1[id]; ok {
		delete(p.ref1, id)
		p.refN[id] = []propRef{existing, ref}
		return
	}
	if cur, ok := p.refN[id]; ok {
		p.refN[id] = append(cur, ref)
		return
	}
	p.ref1[id] = ref
}

// dropRefs removes every reverse entry for id.
func (p *postings[T]) dropRefs(id T) {
	delete(p.ref1, id)
	delete(p.refN, id)
}

// forEachRefID visits every id holding a reverse entry.
func (p *postings[T]) forEachRefID(fn func(T)) {
	for id := range p.ref1 {
		fn(id)
	}
	for id := range p.refN {
		fn(id)
	}
}

func newPostings[T entityID]() postings[T] {
	return postings[T]{
		byKey:  make(map[string]map[string][]T),
		ref1:   make(map[T]propRef),
		refN:   make(map[T][]propRef),
		perKey: make(map[string]int),
	}
}

// add registers id under key=value, ignoring exact duplicates.
func (p *postings[T]) add(id T, key, value string) {
	bucket := p.byKey[key]
	if bucket == nil {
		bucket = make(map[string][]T)
		p.byKey[key] = bucket
	}
	ids, inserted := insertSorted(bucket[value], id)
	if !inserted {
		return
	}
	bucket[value] = ids
	p.addRef(id, propRef{key: key, value: value})
	p.perKey[key]++
	p.count++
}

// remove drops every entry registered for id.
func (p *postings[T]) remove(id T) {
	if !p.hasRefs(id) {
		return
	}
	p.forEachRef(id, func(ref propRef) bool {
		bucket := p.byKey[ref.key]
		if bucket == nil {
			return true
		}
		ids, removed := deleteSorted(bucket[ref.value], id)
		if removed {
			p.count--
			if p.perKey[ref.key]--; p.perKey[ref.key] <= 0 {
				delete(p.perKey, ref.key)
			}
		}
		if len(ids) == 0 {
			delete(bucket, ref.value)
		} else {
			bucket[ref.value] = ids
		}
		if len(bucket) == 0 {
			delete(p.byKey, ref.key)
		}
		return true
	})
	p.dropRefs(id)
}

// lookup returns a copy of the sorted postings list for key=value.
func (p *postings[T]) lookup(key, value string) []T {
	bucket := p.byKey[key]
	if bucket == nil {
		return nil
	}
	ids := bucket[value]
	if len(ids) == 0 {
		return nil
	}
	out := make([]T, len(ids))
	copy(out, ids)
	return out
}

// cardinality returns the postings-list length without copying.
func (p *postings[T]) cardinality(key, value string) int {
	bucket := p.byKey[key]
	if bucket == nil {
		return 0
	}
	return len(bucket[value])
}

// forEach visits every (id, value) under key. The value slice aliases the
// index's internal string and must not be retained or mutated by fn.
func (p *postings[T]) forEach(key string, fn func(id T, value []byte) bool) {
	bucket := p.byKey[key]
	if bucket == nil {
		return
	}
	for value, ids := range bucket {
		raw := unsafeBytes(value)
		for _, id := range ids {
			if !fn(id, raw) {
				return
			}
		}
	}
}

// forEachAll visits every (id, key, value) triple in the index, copying each
// value so the callback may retain it.
func (p *postings[T]) forEachAll(fn func(id T, key string, value []byte) bool) {
	for key, bucket := range p.byKey {
		for value, ids := range bucket {
			for _, id := range ids {
				if !fn(id, key, []byte(value)) {
					return
				}
			}
		}
	}
}

// unsafeBytes exposes a string's bytes without copying. The result is only
// handed to filter predicates, which read it and never retain or mutate it —
// that read-only contract is what makes this safe, and it keeps the scan path
// allocation-free.
func unsafeBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// verify walks the postings and the reverse map and cross-checks them.
func (p *postings[T]) verify(kind string) error {
	seen := make(map[T]map[propRef]int)
	total := 0

	for key, bucket := range p.byKey {
		if len(bucket) == 0 {
			return fmt.Errorf("%s index: key %q has an empty bucket map", kind, key)
		}
		for value, ids := range bucket {
			if len(ids) == 0 {
				return fmt.Errorf("%s index: key %q value %q has an empty postings list", kind, key, value)
			}
			for i, id := range ids {
				if i > 0 && ids[i-1] >= id {
					return fmt.Errorf("%s index: key %q value %q postings not strictly ascending at %d (%d >= %d)",
						kind, key, value, i, uint64(ids[i-1]), uint64(id))
				}
				refs := seen[id]
				if refs == nil {
					refs = make(map[propRef]int)
					seen[id] = refs
				}
				refs[propRef{key: key, value: value}]++
				total++
			}
		}
	}

	if total != p.count {
		return fmt.Errorf("%s index: cached count %d != %d actual postings entries", kind, p.count, total)
	}

	// The reverse map is split by arity, so its own invariants are checked first:
	// an id lives in exactly one of the two maps, and the overflow map only ever
	// holds ids that genuinely have two or more entries. A bug in the promotion
	// path would otherwise show up much later as a lost or duplicated entry.
	for id := range p.ref1 {
		if _, both := p.refN[id]; both {
			return fmt.Errorf("%s index: id %d is in both the inline and overflow reverse maps",
				kind, uint64(id))
		}
	}
	for id, refs := range p.refN {
		if len(refs) < 2 {
			return fmt.Errorf("%s index: id %d is in the overflow reverse map with only %d entries",
				kind, uint64(id), len(refs))
		}
	}

	// Reverse map must agree with the postings, in both directions.
	var verr error
	p.forEachRefID(func(id T) {
		if verr != nil {
			return
		}
		n := p.refCount(id)
		if n == 0 {
			verr = fmt.Errorf("%s index: id %d has an empty reverse entry", kind, uint64(id))
			return
		}
		fromPostings := seen[id]
		if len(fromPostings) != n {
			verr = fmt.Errorf("%s index: id %d has %d reverse refs but appears in %d postings",
				kind, uint64(id), n, len(fromPostings))
			return
		}
		p.forEachRef(id, func(ref propRef) bool {
			if fromPostings[ref] != 1 {
				verr = fmt.Errorf("%s index: id %d reverse ref (%q=%q) appears %d times in postings",
					kind, uint64(id), ref.key, ref.value, fromPostings[ref])
				return false
			}
			return true
		})
	})
	if verr != nil {
		return verr
	}
	for id := range seen {
		if !p.hasRefs(id) {
			return fmt.Errorf("%s index: id %d appears in postings but has no reverse entry", kind, uint64(id))
		}
	}
	return nil
}

// insertSorted inserts id into the ascending slice, reporting whether it was
// added (false means it was already present).
func insertSorted[T entityID](ids []T, id T) ([]T, bool) {
	pos := sort.Search(len(ids), func(i int) bool { return ids[i] >= id })
	if pos < len(ids) && ids[pos] == id {
		return ids, false
	}
	var zero T
	ids = append(ids, zero)
	copy(ids[pos+1:], ids[pos:])
	ids[pos] = id
	return ids, true
}

// deleteSorted removes id from the ascending slice, reporting whether it was
// present.
func deleteSorted[T entityID](ids []T, id T) ([]T, bool) {
	pos := sort.Search(len(ids), func(i int) bool { return ids[i] >= id })
	if pos >= len(ids) || ids[pos] != id {
		return ids, false
	}
	return append(ids[:pos], ids[pos+1:]...), true
}
