package index

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

	// Composite indexes span several keys, which hash to several shards, so they
	// cannot live in one. They sit beside the shards with their own locks — see
	// composite_index.go, which is also where the argument that this introduces
	// no lock ordering lives.
	nodeComposites compositeSet[store.NodeID]
	edgeComposites compositeSet[store.EdgeID]

	// compDeclared is read on the registration path before either set is
	// touched, so a store that declared no composite — the default — pays one
	// atomic load per indexed property and no lock at all.
	compDeclared atomic.Bool

	// baseRef is the disk-resident half of the index, or nil. When it is set the
	// shards above are the delta over it and every read is base ∪ delta −
	// retracted; see retract.go for the design and union.go for the merges. A
	// store with no base pays one atomic load and a branch per read path.
	baseRef atomic.Pointer[baseState]
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

	// Keys declared unique. No structure accompanies them: uniqueness is a
	// predicate over the postings above, not a second index. See
	// unique_index.go.
	uniqueNodeKeys map[string]struct{}
	uniqueEdgeKeys map[string]struct{}
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
	p := &PropertyIndex{
		nodeComposites: newCompositeSet[store.NodeID](),
		edgeComposites: newCompositeSet[store.EdgeID](),
	}
	for i := range p.shards {
		p.shards[i] = propertyShard{
			nodes:           newPostings[store.NodeID](),
			edges:           newPostings[store.EdgeID](),
			orderedNodeKeys: make(map[string]*orderedIndex[store.NodeID]),
			orderedEdgeKeys: make(map[string]*orderedIndex[store.EdgeID]),
			uniqueNodeKeys:  make(map[string]struct{}),
			uniqueEdgeKeys:  make(map[string]struct{}),
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

// DeclareCompositeNodeKeys builds and maintains a composite index over the given
// node property keys, so that a query pinning all of them to values is answered
// by one lookup instead of by driving from the most selective of them and
// eliminating against the rest.
//
// Entries already registered are absorbed, so this can be called at any point in
// a store's life. Declaring the same tuple twice is a no-op. Order is part of a
// declaration's identity but not of its use: a query is matched against the key
// *set*, so (a, b) serves a query filtering on b and a.
//
// Returns ErrCompositeKeys for a tuple that cannot be indexed — fewer than two
// keys, a repeated key, an empty key, or more than 64.
func (p *PropertyIndex) DeclareCompositeNodeKeys(keys []string) error {
	if err := validateCompositeKeys(keys); err != nil {
		return err
	}
	idx, created := p.nodeComposites.declare(keys)
	if !created {
		return nil
	}
	// Published before the backfill, and after the declaration is visible to
	// membersOf. That order is what makes a concurrent IndexNode unable to lose
	// an entry: it writes to the shard *before* reading this flag, so a
	// registration that reads false must have reached the shard before this
	// store, and therefore before the snapshot below.
	p.compDeclared.Store(true)

	// The base first, for a composite declared over a store that already has one.
	// AttachBase does the same for the other order — a composite declared before
	// the base arrived, which is every composite a store restores from its
	// catalogue at Open. See fillCompositeFromBase.
	if s, hasBase := p.nodeBase(); hasBase {
		fillCompositeFromBase(s, idx)
	}

	// One member key at a time: snapshot that key's entries under its own read
	// lock, release, then file them. Filing under the shard lock would mean
	// holding a shard lock and a composite lock at once, which is the single
	// thing this design exists to avoid.
	for pos, key := range idx.keys {
		type entry struct {
			id    store.NodeID
			value string
		}
		var pending []entry
		sh := p.shardFor(key)
		sh.mu.RLock()
		for value, ids := range sh.nodes.byKey[key] {
			for _, id := range ids {
				pending = append(pending, entry{id: id, value: value})
			}
		}
		sh.mu.RUnlock()
		for _, e := range pending {
			idx.register(e.id, pos, e.value)
		}
	}
	return nil
}

// DeclareCompositeEdgeKeys is DeclareCompositeNodeKeys for edge properties.
func (p *PropertyIndex) DeclareCompositeEdgeKeys(keys []string) error {
	if err := validateCompositeKeys(keys); err != nil {
		return err
	}
	idx, created := p.edgeComposites.declare(keys)
	if !created {
		return nil
	}
	p.compDeclared.Store(true)
	// See DeclareCompositeNodeKeys for why the base is walked first.
	if s, hasBase := p.edgeBase(); hasBase {
		fillCompositeFromBase(s, idx)
	}
	for pos, key := range idx.keys {
		type entry struct {
			id    store.EdgeID
			value string
		}
		var pending []entry
		sh := p.shardFor(key)
		sh.mu.RLock()
		for value, ids := range sh.edges.byKey[key] {
			for _, id := range ids {
				pending = append(pending, entry{id: id, value: value})
			}
		}
		sh.mu.RUnlock()
		for _, e := range pending {
			idx.register(e.id, pos, e.value)
		}
	}
	return nil
}

// CompositeNodeKeys returns the declared node key tuples, each in its own
// declared order and the whole sorted, so two stores holding the same
// declarations report them identically.
func (p *PropertyIndex) CompositeNodeKeys() [][]string { return p.nodeComposites.tuples() }

// CompositeEdgeKeys is CompositeNodeKeys for edge properties.
func (p *PropertyIndex) CompositeEdgeKeys() [][]string { return p.edgeComposites.tuples() }

// MatchNodeComposite reports the declared composite that best fits a node
// query's equality filters, and the exact size of the set it would drive from.
// ok is false when no declaration is fully covered by the query.
func (p *PropertyIndex) MatchNodeComposite(filters []store.PropertyFilter, mode store.MatchMode) (CompositeMatch, bool) {
	if !p.compDeclared.Load() {
		return CompositeMatch{}, false
	}
	return matchComposite(&p.nodeComposites, filters, mode)
}

// MatchEdgeComposite is MatchNodeComposite for edge queries.
func (p *PropertyIndex) MatchEdgeComposite(filters []store.PropertyFilter, mode store.MatchMode) (CompositeMatch, bool) {
	if !p.compDeclared.Load() {
		return CompositeMatch{}, false
	}
	return matchComposite(&p.edgeComposites, filters, mode)
}

// NodesByComposite returns the ascending node IDs filed under a match's tuple.
// ok is false only if the declaration is gone, which nothing in the engine does.
func (p *PropertyIndex) NodesByComposite(m CompositeMatch) ([]store.NodeID, bool) {
	return lookupComposite(&p.nodeComposites, m)
}

// EdgesByComposite is NodesByComposite for edge properties.
func (p *PropertyIndex) EdgesByComposite(m CompositeMatch) ([]store.EdgeID, bool) {
	return lookupComposite(&p.edgeComposites, m)
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

	// Composites observe the same registration, from outside the shard lock.
	// The shard write above happens first, which is what makes a declaration
	// racing this one unable to lose the entry — see DeclareCompositeNodeKeys.
	if p.compDeclared.Load() {
		p.nodeComposites.registered(id, key, vk)
	}
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

	// See IndexNode for why this sits outside the shard lock.
	if p.compDeclared.Load() {
		p.edgeComposites.registered(id, key, vk)
	}
}

// RemoveNode drops every indexed entry for the given node id across all keys
// and values. Buckets left empty are removed so they do not accumulate.
func (p *PropertyIndex) RemoveNode(id store.NodeID) {
	// Before the shards, not after: retractNode says why the order matters to a
	// concurrent uniqueness check. It is a no-op with no base attached.
	p.retractNode(id)
	// Each shard owns the entries for its own keys, so they are removed
	// independently — one lock at a time, never two. That is what keeps this
	// deadlock-free without any lock ordering rule.
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		if len(sh.orderedNodeKeys) > 0 {
			sh.nodes.forEachRef(id, func(ref propRef) bool {
				if idx := sh.orderedNodeKeys[sh.nodes.keyName(ref.keyID)]; idx != nil {
					idx.remove(id, ref.value)
				}
				return true
			})
		}
		sh.nodes.remove(id)
		sh.mu.Unlock()
	}
	if p.compDeclared.Load() {
		p.nodeComposites.removed(id)
	}
}

// RemoveEdge drops every indexed entry for the given edge id across all keys
// and values. Buckets left empty are removed so they do not accumulate.
func (p *PropertyIndex) RemoveEdge(id store.EdgeID) {
	p.retractEdge(id)
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.Lock()
		if len(sh.orderedEdgeKeys) > 0 {
			sh.edges.forEachRef(id, func(ref propRef) bool {
				if idx := sh.orderedEdgeKeys[sh.edges.keyName(ref.keyID)]; idx != nil {
					idx.remove(id, ref.value)
				}
				return true
			})
		}
		sh.edges.remove(id)
		sh.mu.Unlock()
	}
	if p.compDeclared.Load() {
		p.edgeComposites.removed(id)
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
	// A base's values are not in the delta's ordered index and cannot be: absorbing
	// them is the residency this whole item removes. They are walked instead, from
	// the filter's own lower bound, and merged value by value — which is why the
	// declaration still has to be honoured here rather than by falling back to the
	// scan path. The two compare differently, and a declared key that silently
	// started comparing numerically because it acquired a base would answer a
	// different question than the one it was declared for.
	if s, hasBase := p.nodeBase(); hasBase {
		if sc, scannable := orderedScanFor(f); scannable {
			return s.appendOrderedRange(dst, f.Key, f, sc, idx, lo, hi), true
		}
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
	// See NodesMatchingOrdered.
	if s, hasBase := p.edgeBase(); hasBase {
		if sc, scannable := orderedScanFor(f); scannable {
			return s.appendOrderedRange(dst, f.Key, f, sc, idx, lo, hi), true
		}
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
//
// Under a base the result is the base's run merged with the delta's postings and
// the retracted ids dropped. The merge runs outside the shard lock, because
// postings.lookup has already copied the delta's side — so the one thing that can
// block here, a page fault against the image, blocks no writer.
func (p *PropertyIndex) NodesByProperty(key string, value []byte) []store.NodeID {
	sh := p.shardFor(key)
	sh.mu.RLock()
	out := sh.nodes.lookup(key, string(value))
	sh.mu.RUnlock()
	if s, hasBase := p.nodeBase(); hasBase {
		return s.mergeLookup(key, value, out)
	}
	return out
}

// EdgesByProperty returns all EdgeIDs that have an indexed entry for key=value,
// in ascending ID order. Returns nil if no match.
func (p *PropertyIndex) EdgesByProperty(key string, value []byte) []store.EdgeID {
	sh := p.shardFor(key)
	sh.mu.RLock()
	out := sh.edges.lookup(key, string(value))
	sh.mu.RUnlock()
	if s, hasBase := p.edgeBase(); hasBase {
		return s.mergeLookup(key, value, out)
	}
	return out
}

// NodeCardinality returns the number of node IDs registered under key=value
// without copying the postings list. Used by the query planner to pick the most
// selective driving index.
//
// Under a base it is an upper bound rather than a count: see mergeCardinality for
// which cases are exact and why a bound is what the planner needs.
func (p *PropertyIndex) NodeCardinality(key string, value []byte) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	n := sh.nodes.cardinality(key, string(value))
	sh.mu.RUnlock()
	if s, hasBase := p.nodeBase(); hasBase {
		return s.mergeCardinality(key, value, n)
	}
	return n
}

// EdgeCardinality returns the number of edge IDs registered under key=value.
func (p *PropertyIndex) EdgeCardinality(key string, value []byte) int {
	sh := p.shardFor(key)
	sh.mu.RLock()
	n := sh.edges.cardinality(key, string(value))
	sh.mu.RUnlock()
	if s, hasBase := p.edgeBase(); hasBase {
		return s.mergeCardinality(key, value, n)
	}
	return n
}

// ForEachNodeEntry calls fn for every (id, value) registered under key, in
// (value, id) order, holding only a read lock. Return false from fn to stop
// early.
//
// It touches only the buckets belonging to key, unlike NodeEntries which
// materialises the entire index — but it does allocate the key's distinct values
// to order them, for the reason postings.forEach gives.
func (p *PropertyIndex) ForEachNodeEntry(key string, fn func(id store.NodeID, value []byte) bool) {
	p.forEachNodeEntryBuf(key, nil, fn)
}

// forEachNodeEntryBuf is ForEachNodeEntry with the value buffer carried in, for
// a walk that spans keys. See postings.forEachBuf.
func (p *PropertyIndex) forEachNodeEntryBuf(key string, dst []string, fn func(id store.NodeID, value []byte) bool) []string {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if s, hasBase := p.nodeBase(); hasBase {
		dst, _ = s.mergeForEachEntry(key, sh.nodes.byKey[key], dst, nil, fn)
		return dst
	}
	return sh.nodes.forEachBuf(key, dst, fn)
}

// ForEachNodeValue calls fn once per distinct value under key, with that value's
// id list. Prefer it over ForEachNodeEntry when the callback depends only on the
// value — a filter scan does, and this makes it one comparison per value instead
// of one per entry.
//
// The id slice is owned by the index: read it, do not retain or mutate it.
func (p *PropertyIndex) ForEachNodeValue(key string, fn func(value []byte, ids []store.NodeID) bool) {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	// Under a base the values arrive in ascending order rather than in the map's,
	// because merging two sides needs both walked the same way. Nothing depended
	// on the unordered form — forEachValue's callers are filter scans — and the
	// order is not promised, so this is not a contract that has now changed.
	if s, hasBase := p.nodeBase(); hasBase {
		s.mergeForEachValue(key, sh.nodes.byKey[key], nil, nil, fn)
		return
	}
	sh.nodes.forEachValue(key, fn)
}

// ForEachEdgeValue is ForEachNodeValue for edge properties.
func (p *PropertyIndex) ForEachEdgeValue(key string, fn func(value []byte, ids []store.EdgeID) bool) {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if s, hasBase := p.edgeBase(); hasBase {
		s.mergeForEachValue(key, sh.edges.byKey[key], nil, nil, fn)
		return
	}
	sh.edges.forEachValue(key, fn)
}

// ForEachEdgeEntry calls fn for every (id, value) registered under key.
// Return false from fn to stop early.
func (p *PropertyIndex) ForEachEdgeEntry(key string, fn func(id store.EdgeID, value []byte) bool) {
	p.forEachEdgeEntryBuf(key, nil, fn)
}

// forEachEdgeEntryBuf is forEachNodeEntryBuf for edge properties.
func (p *PropertyIndex) forEachEdgeEntryBuf(key string, dst []string, fn func(id store.EdgeID, value []byte) bool) []string {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if s, hasBase := p.edgeBase(); hasBase {
		dst, _ = s.mergeForEachEntry(key, sh.edges.byKey[key], dst, nil, fn)
		return dst
	}
	return sh.edges.forEachBuf(key, dst, fn)
}

// NodeEntries returns all indexed node property entries, ordered by
// (Key, Value, ID).
//
// The order is part of the contract, not an accident of enumeration. Entries
// live in per-shard maps, and Go randomises map iteration order on every range,
// so an unordered walk enumerates the same index differently on every call.
// disk.Store.Compact() writes these entries straight into the CSR's index
// section, which made the serialised image differ byte-for-byte between two
// compactions of an identical store. That makes the file's digest useless as an
// identity for its contents: a snapshot hash is not reproducible, and two
// parties holding the same evidence cannot agree on one. Pinned by
// TestCompact_IsByteDeterministic.
//
// The order follows the index's own nesting — key, then value, then the postings
// list, which is already ascending by ID — so ordering costs one sort of the
// keys and one of each key's distinct values, and the entries are then emitted
// straight into place. Ordering by ID instead would group an entity's entries
// contiguously, which reads slightly better for a future per-entity digest, but
// it is a transpose of the layout above: it measured ~2.2x slower on the
// compaction path even after sorting a packed key and permuting, because every
// entry lands in a random slot. Determinism is what is actually required here,
// and this order delivers it for close to nothing.
//
// This materialises the whole index; query paths should use ForEachNodeEntry.
//
// Under a base that includes the base, which is every byte of the index section
// brought into the heap — the one thing the disk-resident index exists to stop
// doing. It is still correct, because a caller asking for every entry is asking
// for exactly that, and the base path streams through the merged walk so nothing
// beyond the result itself is held at once. Compaction does not come here: it
// takes ForEachNodeProperty, which is the same walk without the slice.
func (p *PropertyIndex) NodeEntries() []NodePropEntry {
	out := make([]NodePropEntry, 0, p.nodeEntryCount())
	if _, hasBase := p.nodeBase(); hasBase {
		p.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
			out = append(out, NodePropEntry{ID: id, Key: key, Value: bytes.Clone(value)})
			return true
		})
		return out
	}
	vals := make([]string, 0, 64)
	for _, key := range p.nodePropKeys() {
		sh := p.shardFor(key)
		sh.mu.RLock()
		bucket := sh.nodes.byKey[key]
		vals = sortedBucketValues(bucket, vals)
		for _, v := range vals {
			for _, id := range bucket[v] {
				// Copied per entry, not per value: the callers of this API are
				// handed ownership of Value, and sharing one backing array
				// between the entries of a multi-ID posting would alias them.
				out = append(out, NodePropEntry{ID: id, Key: key, Value: []byte(v)})
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// EdgeEntries returns all indexed edge property entries, ordered by
// (Key, Value, ID). See NodeEntries for why the order is a contract.
func (p *PropertyIndex) EdgeEntries() []EdgePropEntry {
	out := make([]EdgePropEntry, 0, p.edgeEntryCount())
	if _, hasBase := p.edgeBase(); hasBase {
		p.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
			out = append(out, EdgePropEntry{ID: id, Key: key, Value: bytes.Clone(value)})
			return true
		})
		return out
	}
	vals := make([]string, 0, 64)
	for _, key := range p.edgePropKeys() {
		sh := p.shardFor(key)
		sh.mu.RLock()
		bucket := sh.edges.byKey[key]
		vals = sortedBucketValues(bucket, vals)
		for _, v := range vals {
			for _, id := range bucket[v] {
				out = append(out, EdgePropEntry{ID: id, Key: key, Value: []byte(v)})
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// sortedBucketValues fills dst with bucket's distinct values in ascending order,
// reusing dst's capacity across keys.
//
// It sizes the buffer to the bucket up front rather than letting append double
// into it. Reuse alone is not enough when one key holds most of the index: the
// first key it meets grows the slice by doubling and pays about twice the bytes
// it ends up holding, which on the export path was 7 MB of a 100 000-entry key.
// Sized once, the walk allocates the widest key's values and nothing after.
func sortedBucketValues[T entityID](bucket map[string][]T, dst []string) []string {
	if cap(dst) < len(bucket) {
		dst = make([]string, 0, len(bucket))
	}
	dst = dst[:0]
	for v := range bucket {
		dst = append(dst, v)
	}
	slices.Sort(dst)
	return dst
}

// nodePropKeys and edgePropKeys return every indexed key, sorted.
//
// Keys are collected across all shards and sorted globally rather than walked
// shard by shard, so the enumeration order does not depend on which shard a key
// happens to hash to. There are few keys relative to entries, so this is cheap.
func (p *PropertyIndex) nodePropKeys() []string {
	var out []string
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		for k := range sh.nodes.byKey {
			out = append(out, k)
		}
		sh.mu.RUnlock()
	}
	if s, hasBase := p.nodeBase(); hasBase {
		out = s.mergeKeys(out)
	}
	slices.Sort(out)
	return out
}

func (p *PropertyIndex) edgePropKeys() []string {
	var out []string
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		for k := range sh.edges.byKey {
			out = append(out, k)
		}
		sh.mu.RUnlock()
	}
	if s, hasBase := p.edgeBase(); hasBase {
		out = s.mergeKeys(out)
	}
	slices.Sort(out)
	return out
}

// EntryCounts returns the number of indexed (id, key, value) triples, split by
// entity kind. Both are exact: each shard keeps a running count, so this does
// not walk the index.
//
// Under a base they are upper bounds. The base reports its own total without
// being walked, and the entries it holds for an id retracted since it was written
// are still in that total — finding out how many would mean the pass this figure
// exists to avoid.
func (p *PropertyIndex) EntryCounts() (nodes, edges int) {
	return p.nodeEntryCount(), p.edgeEntryCount()
}

// nodeEntryCount and edgeEntryCount total the per-shard entry counts so the
// caller can size its result exactly. Without this the collection loop grows by
// append and copies the whole slice ~log2(n) times on the way up — 5 MB of
// copying for a 100k-entry index, on a path that already runs during compaction.
func (p *PropertyIndex) nodeEntryCount() int {
	total := 0
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		total += sh.nodes.count
		sh.mu.RUnlock()
	}
	if s, hasBase := p.nodeBase(); hasBase {
		total += s.base().TotalEntries(NodeKind)
	}
	return total
}

func (p *PropertyIndex) edgeEntryCount() int {
	total := 0
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		total += sh.edges.count
		sh.mu.RUnlock()
	}
	if s, hasBase := p.edgeBase(); hasBase {
		total += s.base().TotalEntries(EdgeKind)
	}
	return total
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
//
// Under a base it checks the delta and reports any fault already read out of the
// base, but it does not verify the base's own structure. That is a pass over the
// whole index section with O(1) memory, which belongs to the bounded verifier
// that exists to pay for it rather than to a function every test calls.
func (p *PropertyIndex) Verify() error {
	return p.VerifyCtx(context.Background())
}

// VerifyCtx is Verify, abandoned if ctx is cancelled.
//
// Verify is read-only, so there is nothing a cancelled one leaves behind and no
// point at which stopping is unsafe. That is the whole reason it is cancellable
// where RebuildIndexes largely is not.
func (p *PropertyIndex) VerifyCtx(ctx context.Context) error {
	if err := p.BaseFault(); err != nil {
		return err
	}
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		err := sh.verify(&cc)
		sh.mu.RUnlock()
		if err != nil {
			return err
		}
	}
	// Composites are checked outside the shard loop because they are not owned
	// by a shard, and each one re-derives itself rather than being compared
	// against the postings it was built from — see compositeIndex.verify.
	if err := p.nodeComposites.verifyAll("node", &cc); err != nil {
		return err
	}
	return p.edgeComposites.verifyAll("edge", &cc)
}

// verify checks one shard's invariants. Caller must hold sh.mu.
func (sh *propertyShard) verify(cc *store.CancelCheck) error {
	if err := sh.nodes.verify("node", cc); err != nil {
		return err
	}
	if err := sh.edges.verify("edge", cc); err != nil {
		return err
	}
	// Each ordered index must mirror the hash postings for its key exactly.
	for key, idx := range sh.orderedNodeKeys {
		if err := idx.verify("node", key, sh.nodes.byKey[key], cc); err != nil {
			return err
		}
	}
	for key, idx := range sh.orderedEdgeKeys {
		if err := idx.verify("edge", key, sh.edges.byKey[key], cc); err != nil {
			return err
		}
	}
	return nil
}

// ForEachIndexedNodeID calls fn for every node ID that has at least one indexed
// entry, stopping early if fn returns false. Used by integrity checks to detect
// postings that outlived their entity.
//
// Callback rather than a slice, and the difference is the whole point. The
// callers keep only the ids they find *wrong* — a handful in a healthy store —
// so returning every indexed id charged the check a map to deduplicate plus a
// slice to hold the answer, both proportional to the entire index, to produce
// something thrown away one element at a time. At the ~28M entries this engine
// is being sized for that is over a gigabyte, spent by the routine whose job is
// to be safe to run on a store that is already near its ceiling.
//
// # Ids repeat
//
// The index is sharded by key, so an entity carrying entries under keys that
// hash to different shards is yielded once per such shard. Deduplicating here
// is exactly the map this exists to avoid; a caller that minds deduplicates
// what it keeps, which is bounded by what it is looking for rather than by the
// index. The two callers in this tree keep only dead ids and do precisely that.
//
// A shard's read lock is held for the duration of that shard's walk, so fn must
// not call back into the index.
func (p *PropertyIndex) ForEachIndexedNodeID(fn func(store.NodeID) bool) {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		done := sh.nodes.forEachRefID(fn)
		sh.mu.RUnlock()
		if !done {
			return
		}
	}
	// The base's ids come after the delta's, retracted ones omitted. An id both
	// sides carry is visited from each, which is the same repeat the shards
	// already produce and costs the same callers nothing.
	if s, hasBase := p.nodeBase(); hasBase {
		s.forEachID(fn)
	}
}

// ForEachIndexedEdgeID is ForEachIndexedNodeID for edges; the same shard-repeat
// caveat applies.
func (p *PropertyIndex) ForEachIndexedEdgeID(fn func(store.EdgeID) bool) {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		done := sh.edges.forEachRefID(fn)
		sh.mu.RUnlock()
		if !done {
			return
		}
	}
	if s, hasBase := p.edgeBase(); hasBase {
		s.forEachID(fn)
	}
}

// --- postings ---

// entityID constrains the postings container to the two ID types.
type entityID interface {
	~uint64
}

// propRef is one (key, value) pair an entity is registered under, used by the
// reverse map to make removal proportional to the entity's own entries.
// propRef is one reverse entry: which key/value an entity was registered under.
//
// The key is held as an interned id rather than a string. A shard owns only a
// handful of distinct keys, but a propRef exists per (entity, key) — so the
// string header was paid hundreds of thousands of times to describe a handful of
// strings. Four bytes plus padding replaces sixteen.
type propRef struct {
	keyID uint32
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

	// shared holds the canonical string for values carrying two or more entries,
	// so their reverse entries point at one copy instead of one each. Values that
	// never repeat never enter it — see canonical.
	shared map[string]string

	// Entries per key, so the query planner can cost a scan of a key without
	// walking that key's buckets to find out how big it is.
	perKey map[string]int

	// Key interning. A shard sees only the keys that hash to it, so these stay
	// tiny — which is the whole point: the table is paid once per key, the
	// saving once per entry.
	keyNames []string
	keyIDs   map[string]uint32
}

// internKey returns a stable id for key within this postings set.
func (p *postings[T]) internKey(key string) uint32 {
	if id, ok := p.keyIDs[key]; ok {
		return id
	}
	id := uint32(len(p.keyNames))
	p.keyNames = append(p.keyNames, key)
	p.keyIDs[key] = id
	return id
}

// keyName resolves an interned id back to its key.
func (p *postings[T]) keyName(id uint32) string { return p.keyNames[id] }

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

// forEachRefID visits every id holding a reverse entry, stopping early if fn
// returns false. It reports whether the walk ran to the end.
//
// The two maps partition the ids by arity — verify checks that no id is in both
// — so an id is visited exactly once per postings container.
func (p *postings[T]) forEachRefID(fn func(T) bool) bool {
	for id := range p.ref1 {
		if !fn(id) {
			return false
		}
	}
	for id := range p.refN {
		if !fn(id) {
			return false
		}
	}
	return true
}

func newPostings[T entityID]() postings[T] {
	return postings[T]{
		byKey:  make(map[string]map[string][]T),
		ref1:   make(map[T]propRef),
		refN:   make(map[T][]propRef),
		perKey: make(map[string]int),
		keyIDs: make(map[string]uint32),
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
	p.addRef(id, propRef{keyID: p.internKey(key), value: p.canonical(value, len(ids))})
	p.perKey[key]++
	p.count++
}

// canonical returns the string to store in a reverse entry for a value that now
// has n entries under its key.
//
// Every reverse entry used to keep the caller's own string. The forward index
// deduplicates by content — a map key is written once and never replaced — so
// with a thousand nodes sharing one value, the forward side held one string and
// the reverse side pinned a thousand copies of it. Measured, that is ~32 B per
// entry, the largest single component of index memory after the map machinery
// itself.
//
// Interning unconditionally would be the wrong trade: a key like a hash has one
// entry per distinct value, so a table mapping value → canonical string costs an
// entry per value and saves nothing at all.
//
// The repetition is knowable exactly when it starts to matter. n is the number
// of entries this value now has, so a value only enters the table on its
// *second* entry — a key whose values never repeat never allocates a single
// table slot, and a low-cardinality key pays one slot to save a copy per entry.
func (p *postings[T]) canonical(value string, n int) string {
	if n < 2 {
		// First entry for this value: the forward bucket's key already holds
		// this exact string, so it is shared by construction.
		return value
	}
	if canon, ok := p.shared[value]; ok {
		return canon
	}
	if p.shared == nil {
		p.shared = make(map[string]string)
	}
	p.shared[value] = value
	return value
}

// remove drops every entry registered for id.
func (p *postings[T]) remove(id T) {
	if !p.hasRefs(id) {
		return
	}
	p.forEachRef(id, func(ref propRef) bool {
		key := p.keyName(ref.keyID)
		bucket := p.byKey[key]
		if bucket == nil {
			return true
		}
		ids, removed := deleteSorted(bucket[ref.value], id)
		if removed {
			p.count--
			if p.perKey[key]--; p.perKey[key] <= 0 {
				delete(p.perKey, key)
			}
		}
		if len(ids) == 0 {
			delete(bucket, ref.value)
			delete(p.shared, ref.value)
		} else {
			bucket[ref.value] = ids
		}
		if len(bucket) == 0 {
			delete(p.byKey, key)
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
// forEachValue calls fn once per distinct value under key, with that value's
// whole id list.
//
// forEach below calls its predicate once per *entry*. Every scan caller uses it
// to evaluate a filter, and a filter reads only the value — so a value shared by
// a thousand entities was compared a thousand times to reach the same answer.
// Iterating by value makes the comparison count the number of distinct values
// rather than the number of entries.
//
// ids is the live posting slice: callers must not retain or mutate it, the same
// contract as unsafeBytes below.
func (p *postings[T]) forEachValue(key string, fn func(value []byte, ids []T) bool) {
	bucket := p.byKey[key]
	if bucket == nil {
		return
	}
	for value, ids := range bucket {
		if !fn(unsafeBytes(value), ids) {
			return
		}
	}
}

// forEach visits every (id, value) under key, values in ascending byte order and
// ids ascending within each value — the order NodeEntries produces, because this
// is NodeEntries' streaming form and owes the same contract for the reason that
// function's comment gives at length.
//
// The sort is not optional. Entries live in a map, Go randomises map iteration,
// and an export walks this to write its property section: without it two dumps
// of one unchanged graph differ in the order of their property lines, so an
// export cannot tell a caller which of the two is the graph. It reproduced about
// one run in seven on a 200-value key.
//
// forEachValue above deliberately does not sort. Its callers are filter scans
// that read only the value and do not care in which order they reject it, and
// they are the hot path this one is not: the only callers of forEach are
// ForEachNodeProperty and ForEachEdgeProperty. The sort is over distinct values
// while the walk it orders visits every entry under each of them, so it is the
// smaller term either way.
func (p *postings[T]) forEach(key string, fn func(id T, value []byte) bool) {
	p.forEachBuf(key, nil, fn)
}

// forEachBuf is forEach with the value buffer supplied, so a walk over several
// keys sorts into one slice instead of one per key. It returns the buffer,
// grown to whatever the largest key needed — the same reuse NodeEntries makes
// across its keys, and it matters for the same reason: the buffer is as long as
// a key's distinct values, which on a high-cardinality key is most of the index.
func (p *postings[T]) forEachBuf(key string, dst []string, fn func(id T, value []byte) bool) []string {
	bucket := p.byKey[key]
	if bucket == nil {
		return dst
	}
	dst = sortedBucketValues(bucket, dst)
	for _, value := range dst {
		raw := unsafeBytes(value)
		for _, id := range bucket[value] {
			if !fn(id, raw) {
				return dst
			}
		}
	}
	return dst
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
//
// # Why nothing is accumulated
//
// This used to build a map[T]map[propRef]int over every posting — the reverse
// index rebuilt a second time, with a map header of its own per entity — and
// then compare it with the reverse index already sitting beside it. At the
// entry counts this engine is being budgeted for, that was the largest
// allocation in the whole integrity check, made by the one routine an operator
// reaches for when memory is already the problem. It also called internKey
// under a read lock, which appends.
//
// It is not needed. Both directions follow from probing and counting:
//
//   - Every postings list is strictly ascending, checked below. So an entity
//     appears at most once under a (key, value), and "is it there" and "how
//     many times" are the same question — one binary search answers it.
//   - Each entity's reverse refs are checked to be distinct, so the reverse
//     side holds exactly Σ refCount entries.
//   - Probing every reverse ref into the postings establishes reverse ⊆
//     postings. Both are sets of (id, key, value), so once the totals agree
//     they are the same set — which is what "every posting has a reverse entry"
//     was asking, and it is now answered without holding either of them.
//
// A total that disagrees knows a posting is unreferenced but not which, so
// unreferencedPosting goes and finds it. That walk is bounded too, and is paid
// only by an index already known to be broken.
func (p *postings[T]) verify(kind string, cc *store.CancelCheck) error {
	total := 0

	for key, bucket := range p.byKey {
		if len(bucket) == 0 {
			return fmt.Errorf("%s index: key %q has an empty bucket map", kind, key)
		}
		for value, ids := range bucket {
			if err := cc.Step(); err != nil {
				return err
			}
			if len(ids) == 0 {
				return fmt.Errorf("%s index: key %q value %q has an empty postings list", kind, key, value)
			}
			for i, id := range ids {
				if i > 0 && ids[i-1] >= id {
					return fmt.Errorf("%s index: key %q value %q postings not strictly ascending at %d (%d >= %d)",
						kind, key, value, i, uint64(ids[i-1]), uint64(id))
				}
			}
			total += len(ids)
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
	reverse := 0
	var verr error
	p.forEachRefID(func(id T) bool {
		if err := cc.Step(); err != nil {
			verr = err
			return false
		}
		n := p.refCount(id)
		if n == 0 {
			verr = fmt.Errorf("%s index: id %d has an empty reverse entry", kind, uint64(id))
			return false
		}
		if ref, dup := p.duplicateRef(id); dup {
			verr = fmt.Errorf("%s index: id %d holds the reverse ref (%q=%q) twice",
				kind, uint64(id), p.safeKeyName(ref.keyID), ref.value)
			return false
		}
		p.forEachRef(id, func(ref propRef) bool {
			if int(ref.keyID) >= len(p.keyNames) {
				verr = fmt.Errorf("%s index: id %d has a reverse ref naming key id %d, but only %d keys are interned",
					kind, uint64(id), ref.keyID, len(p.keyNames))
				return false
			}
			key := p.keyName(ref.keyID)
			if !containsSorted(p.byKey[key][ref.value], id) {
				verr = fmt.Errorf("%s index: id %d reverse ref (%q=%q) has no matching posting",
					kind, uint64(id), key, ref.value)
				return false
			}
			return true
		})
		if verr != nil {
			return false
		}
		reverse += n
		return true
	})
	if verr != nil {
		return verr
	}
	if reverse != total {
		return p.unreferencedPosting(kind, total, reverse, cc)
	}
	return nil
}

// duplicateRef reports a reverse ref an entity holds twice.
//
// Pairwise because the list is one entry per key the entity carries *in this
// shard*, which is a handful — allocating a set to compare a handful of pairs
// would cost more than the comparisons, and would do it once per entity.
func (p *postings[T]) duplicateRef(id T) (propRef, bool) {
	refs := p.refN[id]
	for i, r := range refs {
		for _, s := range refs[i+1:] {
			if r == s {
				return r, true
			}
		}
	}
	return propRef{}, false
}

// safeKeyName is keyName for error paths, where the id being reported may be
// the very thing that is wrong.
func (p *postings[T]) safeKeyName(id uint32) string {
	if int(id) >= len(p.keyNames) {
		return fmt.Sprintf("<key id %d>", id)
	}
	return p.keyNames[id]
}

// unreferencedPosting names the posting that verify's totals proved has no
// reverse entry.
//
// Reached only when the reverse side is a proper subset of the postings, and it
// re-walks them to say which posting is orphaned — the message the old
// map-building verify gave for free. Nothing accumulates here either.
func (p *postings[T]) unreferencedPosting(kind string, total, reverse int, cc *store.CancelCheck) error {
	for key, bucket := range p.byKey {
		keyID, interned := p.keyIDs[key]
		for value, ids := range bucket {
			if err := cc.Step(); err != nil {
				return err
			}
			for _, id := range ids {
				if !p.hasRefs(id) {
					return fmt.Errorf("%s index: id %d appears in postings but has no reverse entry", kind, uint64(id))
				}
				if !interned {
					return fmt.Errorf("%s index: key %q holds postings but was never interned", kind, key)
				}
				found := false
				p.forEachRef(id, func(ref propRef) bool {
					if ref.keyID == keyID && ref.value == value {
						found = true
						return false
					}
					return true
				})
				if !found {
					return fmt.Errorf("%s index: id %d appears under (%q=%q) with no matching reverse ref",
						kind, uint64(id), key, value)
				}
			}
		}
	}
	// Unreachable while this walk and the probing loop agree on what they
	// visit; reported rather than swallowed, because totals that disagree with
	// nothing findable are themselves the finding.
	return fmt.Errorf("%s index: %d postings entries are covered by %d reverse refs, and every posting is referenced",
		kind, total, reverse)
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

// containsSorted reports whether the ascending slice holds id.
func containsSorted[T entityID](ids []T, id T) bool {
	pos := sort.Search(len(ids), func(i int) bool { return ids[i] >= id })
	return pos < len(ids) && ids[pos] == id
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

// ForEachNodeProperty calls fn for every indexed (id, key, value) triple, in
// the same (key, value, id) order NodeEntries produces. Return false from fn to
// stop early.
//
// The streaming counterpart to NodeEntries, and the one a bulk export wants:
// NodeEntries materialises every triple in the index at once, which on a large
// store is a slice proportional to everything that was ever indexed. This walks
// the same triples one key at a time, holding the key list and one buffer of
// distinct values — reused across keys, so it grows to the widest key and not to
// the index — rather than every triple.
//
// The order is the contract and not a convenience. An export writes its property
// section from this walk, and two dumps of one unchanged graph that disagree
// about the order of their property lines are two different dumps.
//
// The value slice is owned by the index: read it, do not retain or mutate it.
func (p *PropertyIndex) ForEachNodeProperty(fn func(id store.NodeID, key string, value []byte) bool) {
	var vals []string
	for _, key := range p.nodePropKeys() {
		stop := false
		vals = p.forEachNodeEntryBuf(key, vals, func(id store.NodeID, value []byte) bool {
			if !fn(id, key, value) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

// ForEachEdgeProperty is ForEachNodeProperty for edge properties.
func (p *PropertyIndex) ForEachEdgeProperty(fn func(id store.EdgeID, key string, value []byte) bool) {
	var vals []string
	for _, key := range p.edgePropKeys() {
		stop := false
		vals = p.forEachEdgeEntryBuf(key, vals, func(id store.EdgeID, value []byte) bool {
			if !fn(id, key, value) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

// PropEntry is one (key, value) pair an entity is indexed under.
type PropEntry struct {
	Key   string
	Value []byte
}

// NodeEntriesOf returns every property-index entry registered for id, sorted by
// key then value.
//
// This is the reverse map read forwards. It exists because replacing what one
// entity is indexed under is only expressible as "drop everything, register the
// new set": the log has a purge record per entity and an add record per entry,
// and no per-key purge — so a caller changing one key has to know the others in
// order to put them back. Sorted, so the re-registration that follows frames the
// same bytes every time.
func (p *PropertyIndex) NodeEntriesOf(id store.NodeID) []PropEntry {
	var out []PropEntry
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		sh.nodes.forEachRef(id, func(r propRef) bool {
			out = append(out, PropEntry{Key: sh.nodes.keyName(r.keyID), Value: []byte(r.value)})
			return true
		})
		sh.mu.RUnlock()
	}
	// A retracted id has no base entries at all, which one bit settles without
	// searching the base's reverse direction for an id that is not there.
	if s, hasBase := p.nodeBase(); hasBase && !s.gone.has(uint64(id)) {
		out = s.appendEntriesOf(out, uint64(id))
		sortPropEntries(out)
		return dedupPropEntries(out)
	}
	sortPropEntries(out)
	return out
}

// EdgeEntriesOf is NodeEntriesOf for edges.
func (p *PropertyIndex) EdgeEntriesOf(id store.EdgeID) []PropEntry {
	var out []PropEntry
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		sh.edges.forEachRef(id, func(r propRef) bool {
			out = append(out, PropEntry{Key: sh.edges.keyName(r.keyID), Value: []byte(r.value)})
			return true
		})
		sh.mu.RUnlock()
	}
	if s, hasBase := p.edgeBase(); hasBase && !s.gone.has(uint64(id)) {
		out = s.appendEntriesOf(out, uint64(id))
		sortPropEntries(out)
		return dedupPropEntries(out)
	}
	sortPropEntries(out)
	return out
}

func sortPropEntries(e []PropEntry) {
	slices.SortFunc(e, func(a, b PropEntry) int {
		if c := strings.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return bytes.Compare(a.Value, b.Value)
	})
}

// NodeHasEntries reports whether id carries any property-index entry.
//
// Sixteen map lookups, which is what the reverse map being sharded by key rather
// than by entity costs. It is paid only where an update has to decide whether it
// is about to leave something stale behind.
func (p *PropertyIndex) NodeHasEntries(id store.NodeID) bool {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		has := sh.nodes.hasRefs(id)
		sh.mu.RUnlock()
		if has {
			return true
		}
	}
	if s, hasBase := p.nodeBase(); hasBase {
		return s.hasEntries(uint64(id))
	}
	return false
}

// EdgeHasEntries is NodeHasEntries for edges.
func (p *PropertyIndex) EdgeHasEntries(id store.EdgeID) bool {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		has := sh.edges.hasRefs(id)
		sh.mu.RUnlock()
		if has {
			return true
		}
	}
	if s, hasBase := p.edgeBase(); hasBase {
		return s.hasEntries(uint64(id))
	}
	return false
}
