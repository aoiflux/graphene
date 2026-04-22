package index

import (
	"sort"
	"sync"

	"graphene/store"
)

// TemporalEntry associates a Unix nanosecond timestamp with a NodeID.
type TemporalEntry struct {
	TimestampNs int64
	NodeID      store.NodeID
}

// TemporalIndex is a sorted slice of TemporalEntry values that supports
// efficient range queries by timestamp. Insertions keep the slice sorted via
// binary search + insert; this is efficient for bulk ingest followed by reads.
//
// TemporalIndex is safe for concurrent use.
type TemporalIndex struct {
	mu      sync.RWMutex
	entries []TemporalEntry // sorted by TimestampNs ascending
}

// NewTemporalIndex returns an empty TemporalIndex.
func NewTemporalIndex() *TemporalIndex {
	return &TemporalIndex{}
}

// Add inserts an entry into the index, maintaining sort order.
func (ti *TemporalIndex) Add(tsNs int64, id store.NodeID) {
	entry := TemporalEntry{TimestampNs: tsNs, NodeID: id}

	ti.mu.Lock()
	pos := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].TimestampNs >= tsNs
	})
	// insert at pos
	ti.entries = append(ti.entries, TemporalEntry{})
	copy(ti.entries[pos+1:], ti.entries[pos:])
	ti.entries[pos] = entry
	ti.mu.Unlock()
}

// Range returns all entries with timestamps in [fromNs, toNs] inclusive.
func (ti *TemporalIndex) Range(fromNs, toNs int64) []TemporalEntry {
	ti.mu.RLock()
	defer ti.mu.RUnlock()

	lo := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].TimestampNs >= fromNs
	})
	hi := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].TimestampNs > toNs
	})

	if lo >= hi {
		return nil
	}

	out := make([]TemporalEntry, hi-lo)
	copy(out, ti.entries[lo:hi])
	return out
}

// Len returns the number of indexed entries.
func (ti *TemporalIndex) Len() int {
	ti.mu.RLock()
	n := len(ti.entries)
	ti.mu.RUnlock()
	return n
}
