package index

import (
	"sync"

	"github.com/aoiflux/graphene/store"
)

// TypeIndex maintains in-memory secondary indexes from NodeType → []NodeID
// and EdgeType → []EdgeID. It is used by both the in-memory and on-disk
// implementations to answer NodesByType / EdgesByType queries cheaply.
//
// TypeIndex is safe for concurrent use.
type TypeIndex struct {
	mu          sync.RWMutex
	nodesByType map[store.NodeType][]store.NodeID
	edgesByType map[store.EdgeType][]store.EdgeID
}

// NewTypeIndex returns an empty TypeIndex.
func NewTypeIndex() *TypeIndex {
	return &TypeIndex{
		nodesByType: make(map[store.NodeType][]store.NodeID),
		edgesByType: make(map[store.EdgeType][]store.EdgeID),
	}
}

// IndexNode records id under a single label. Convenience wrapper around IndexNodeLabels.
func (t *TypeIndex) IndexNode(id store.NodeID, nt store.NodeType) {
	t.mu.Lock()
	t.nodesByType[nt] = append(t.nodesByType[nt], id)
	t.mu.Unlock()
}

// IndexNodeLabels records id under each of its labels.
func (t *TypeIndex) IndexNodeLabels(id store.NodeID, labels []store.NodeType) {
	t.mu.Lock()
	for _, lbl := range labels {
		t.nodesByType[lbl] = append(t.nodesByType[lbl], id)
	}
	t.mu.Unlock()
}

// IndexEdge records id under a single label. Convenience wrapper around IndexEdgeLabels.
func (t *TypeIndex) IndexEdge(id store.EdgeID, et store.EdgeType) {
	t.mu.Lock()
	t.edgesByType[et] = append(t.edgesByType[et], id)
	t.mu.Unlock()
}

// IndexEdgeLabels records id under each of its labels.
func (t *TypeIndex) IndexEdgeLabels(id store.EdgeID, labels []store.EdgeType) {
	t.mu.Lock()
	for _, lbl := range labels {
		t.edgesByType[lbl] = append(t.edgesByType[lbl], id)
	}
	t.mu.Unlock()
}

// NodesByType returns a copy of all NodeIDs of the given type.
func (t *TypeIndex) NodesByType(nt store.NodeType) []store.NodeID {
	t.mu.RLock()
	ids := t.nodesByType[nt]
	out := make([]store.NodeID, len(ids))
	copy(out, ids)
	t.mu.RUnlock()
	return out
}

// EdgesByType returns a copy of all EdgeIDs of the given type.
func (t *TypeIndex) EdgesByType(et store.EdgeType) []store.EdgeID {
	t.mu.RLock()
	ids := t.edgesByType[et]
	out := make([]store.EdgeID, len(ids))
	copy(out, ids)
	t.mu.RUnlock()
	return out
}

// NodeCount returns the number of indexed nodes across all types.
func (t *TypeIndex) NodeCount() uint64 {
	t.mu.RLock()
	var n uint64
	for _, ids := range t.nodesByType {
		n += uint64(len(ids))
	}
	t.mu.RUnlock()
	return n
}

// EdgeCount returns the number of indexed edges across all types.
func (t *TypeIndex) EdgeCount() uint64 {
	t.mu.RLock()
	var n uint64
	for _, ids := range t.edgesByType {
		n += uint64(len(ids))
	}
	t.mu.RUnlock()
	return n
}
