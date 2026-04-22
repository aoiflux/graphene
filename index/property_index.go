package index

import (
	"sync"

	"graphene/store"
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
// PropertyIndex is safe for concurrent use.
type PropertyIndex struct {
	mu          sync.RWMutex
	nodesByProp map[string]map[string][]store.NodeID // key → string(value) → []NodeID
	edgesByProp map[string]map[string][]store.EdgeID // key → string(value) → []EdgeID
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
		nodesByProp: make(map[string]map[string][]store.NodeID),
		edgesByProp: make(map[string]map[string][]store.EdgeID),
	}
}

// IndexNode records that nodeID has property key=value.
func (p *PropertyIndex) IndexNode(id store.NodeID, key string, value []byte) {
	vk := string(value)
	p.mu.Lock()
	m := p.nodesByProp[key]
	if m == nil {
		m = make(map[string][]store.NodeID)
		p.nodesByProp[key] = m
	}
	m[vk] = append(m[vk], id)
	p.mu.Unlock()
}

// IndexEdge records that edgeID has property key=value.
func (p *PropertyIndex) IndexEdge(id store.EdgeID, key string, value []byte) {
	vk := string(value)
	p.mu.Lock()
	m := p.edgesByProp[key]
	if m == nil {
		m = make(map[string][]store.EdgeID)
		p.edgesByProp[key] = m
	}
	m[vk] = append(m[vk], id)
	p.mu.Unlock()
}

// NodesByProperty returns all NodeIDs that have an indexed entry for key=value.
// Returns nil if no match.
func (p *PropertyIndex) NodesByProperty(key string, value []byte) []store.NodeID {
	vk := string(value)
	p.mu.RLock()
	m := p.nodesByProp[key]
	if m == nil {
		p.mu.RUnlock()
		return nil
	}
	ids := m[vk]
	out := make([]store.NodeID, len(ids))
	copy(out, ids)
	p.mu.RUnlock()
	return out
}

// EdgesByProperty returns all EdgeIDs that have an indexed entry for key=value.
// Returns nil if no match.
func (p *PropertyIndex) EdgesByProperty(key string, value []byte) []store.EdgeID {
	vk := string(value)
	p.mu.RLock()
	m := p.edgesByProp[key]
	if m == nil {
		p.mu.RUnlock()
		return nil
	}
	ids := m[vk]
	out := make([]store.EdgeID, len(ids))
	copy(out, ids)
	p.mu.RUnlock()
	return out
}

// NodeEntries returns all indexed node property entries.
// Used by disk.Store.Compact() to re-emit entries to the fresh WAL.
func (p *PropertyIndex) NodeEntries() []NodePropEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []NodePropEntry
	for key, m := range p.nodesByProp {
		for vk, ids := range m {
			for _, id := range ids {
				out = append(out, NodePropEntry{ID: id, Key: key, Value: []byte(vk)})
			}
		}
	}
	return out
}

// EdgeEntries returns all indexed edge property entries.
func (p *PropertyIndex) EdgeEntries() []EdgePropEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []EdgePropEntry
	for key, m := range p.edgesByProp {
		for vk, ids := range m {
			for _, id := range ids {
				out = append(out, EdgePropEntry{ID: id, Key: key, Value: []byte(vk)})
			}
		}
	}
	return out
}
