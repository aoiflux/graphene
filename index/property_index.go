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
// Structure:
//
//   - Postings lists are kept sorted by ID. Membership and insertion are
//     O(log n) + memmove, lookups return an already-ordered slice (so the query
//     path can skip sorting), and duplicate registrations are idempotent.
//   - A reverse map from ID to its registered (key, value) pairs makes
//     RemoveNode / RemoveEdge proportional to that entity's own entries rather
//     than to the size of the whole index.
//
// PropertyIndex is safe for concurrent use.
type PropertyIndex struct {
	mu    sync.RWMutex
	nodes postings[store.NodeID]
	edges postings[store.EdgeID]
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
	return &PropertyIndex{
		nodes: newPostings[store.NodeID](),
		edges: newPostings[store.EdgeID](),
	}
}

// IndexNode records that nodeID has property key=value. Re-registering an
// identical (id, key, value) triple is a no-op.
func (p *PropertyIndex) IndexNode(id store.NodeID, key string, value []byte) {
	p.mu.Lock()
	p.nodes.add(id, key, string(value))
	p.mu.Unlock()
}

// IndexEdge records that edgeID has property key=value. Re-registering an
// identical (id, key, value) triple is a no-op.
func (p *PropertyIndex) IndexEdge(id store.EdgeID, key string, value []byte) {
	p.mu.Lock()
	p.edges.add(id, key, string(value))
	p.mu.Unlock()
}

// RemoveNode drops every indexed entry for the given node id across all keys
// and values. Buckets left empty are removed so they do not accumulate.
func (p *PropertyIndex) RemoveNode(id store.NodeID) {
	p.mu.Lock()
	p.nodes.remove(id)
	p.mu.Unlock()
}

// RemoveEdge drops every indexed entry for the given edge id across all keys
// and values. Buckets left empty are removed so they do not accumulate.
func (p *PropertyIndex) RemoveEdge(id store.EdgeID) {
	p.mu.Lock()
	p.edges.remove(id)
	p.mu.Unlock()
}

// NodesByProperty returns all NodeIDs that have an indexed entry for key=value,
// in ascending ID order. Returns nil if no match.
func (p *PropertyIndex) NodesByProperty(key string, value []byte) []store.NodeID {
	p.mu.RLock()
	out := p.nodes.lookup(key, string(value))
	p.mu.RUnlock()
	return out
}

// EdgesByProperty returns all EdgeIDs that have an indexed entry for key=value,
// in ascending ID order. Returns nil if no match.
func (p *PropertyIndex) EdgesByProperty(key string, value []byte) []store.EdgeID {
	p.mu.RLock()
	out := p.edges.lookup(key, string(value))
	p.mu.RUnlock()
	return out
}

// NodeCardinality returns the number of node IDs registered under key=value
// without copying the postings list. Used by the query planner to pick the most
// selective driving index.
func (p *PropertyIndex) NodeCardinality(key string, value []byte) int {
	p.mu.RLock()
	n := p.nodes.cardinality(key, string(value))
	p.mu.RUnlock()
	return n
}

// EdgeCardinality returns the number of edge IDs registered under key=value.
func (p *PropertyIndex) EdgeCardinality(key string, value []byte) int {
	p.mu.RLock()
	n := p.edges.cardinality(key, string(value))
	p.mu.RUnlock()
	return n
}

// ForEachNodeEntry calls fn for every (id, value) registered under key, holding
// only a read lock and allocating nothing. Return false from fn to stop early.
//
// This is the scan path for operators the index cannot answer directly (prefix,
// contains, and the ordered comparisons). It touches only the buckets belonging
// to key, unlike NodeEntries which materialises the entire index.
func (p *PropertyIndex) ForEachNodeEntry(key string, fn func(id store.NodeID, value []byte) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.nodes.forEach(key, fn)
}

// ForEachEdgeEntry calls fn for every (id, value) registered under key.
// Return false from fn to stop early.
func (p *PropertyIndex) ForEachEdgeEntry(key string, fn func(id store.EdgeID, value []byte) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.edges.forEach(key, fn)
}

// NodeEntries returns all indexed node property entries.
// Used by disk.Store.Compact() to re-emit entries to the fresh WAL.
//
// This materialises the whole index; query paths should use ForEachNodeEntry.
func (p *PropertyIndex) NodeEntries() []NodePropEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]NodePropEntry, 0, p.nodes.count)
	p.nodes.forEachAll(func(id store.NodeID, key string, value []byte) bool {
		out = append(out, NodePropEntry{ID: id, Key: key, Value: value})
		return true
	})
	return out
}

// EdgeEntries returns all indexed edge property entries.
func (p *PropertyIndex) EdgeEntries() []EdgePropEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]EdgePropEntry, 0, p.edges.count)
	p.edges.forEachAll(func(id store.EdgeID, key string, value []byte) bool {
		out = append(out, EdgePropEntry{ID: id, Key: key, Value: value})
		return true
	})
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
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.nodes.verify("node"); err != nil {
		return err
	}
	return p.edges.verify("edge")
}

// IndexedNodeIDs returns every node ID that has at least one indexed entry.
// Used by integrity checks to detect postings that outlived their entity.
func (p *PropertyIndex) IndexedNodeIDs() []store.NodeID {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]store.NodeID, 0, len(p.nodes.refs))
	for id := range p.nodes.refs {
		out = append(out, id)
	}
	return out
}

// IndexedEdgeIDs returns every edge ID that has at least one indexed entry.
func (p *PropertyIndex) IndexedEdgeIDs() []store.EdgeID {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]store.EdgeID, 0, len(p.edges.refs))
	for id := range p.edges.refs {
		out = append(out, id)
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
	refs  map[T][]propRef
	count int // total number of (id, key, value) triples
}

func newPostings[T entityID]() postings[T] {
	return postings[T]{
		byKey: make(map[string]map[string][]T),
		refs:  make(map[T][]propRef),
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
	p.refs[id] = append(p.refs[id], propRef{key: key, value: value})
	p.count++
}

// remove drops every entry registered for id.
func (p *postings[T]) remove(id T) {
	refs, ok := p.refs[id]
	if !ok {
		return
	}
	for _, ref := range refs {
		bucket := p.byKey[ref.key]
		if bucket == nil {
			continue
		}
		ids, removed := deleteSorted(bucket[ref.value], id)
		if removed {
			p.count--
		}
		if len(ids) == 0 {
			delete(bucket, ref.value)
		} else {
			bucket[ref.value] = ids
		}
		if len(bucket) == 0 {
			delete(p.byKey, ref.key)
		}
	}
	delete(p.refs, id)
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

	// Reverse map must agree with the postings, in both directions.
	for id, refs := range p.refs {
		if len(refs) == 0 {
			return fmt.Errorf("%s index: id %d has an empty reverse entry", kind, uint64(id))
		}
		fromPostings := seen[id]
		if len(fromPostings) != len(refs) {
			return fmt.Errorf("%s index: id %d has %d reverse refs but appears in %d postings",
				kind, uint64(id), len(refs), len(fromPostings))
		}
		for _, ref := range refs {
			if fromPostings[ref] != 1 {
				return fmt.Errorf("%s index: id %d reverse ref (%q=%q) appears %d times in postings",
					kind, uint64(id), ref.key, ref.value, fromPostings[ref])
			}
		}
	}
	for id := range seen {
		if _, ok := p.refs[id]; !ok {
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
