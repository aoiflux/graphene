package index

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// NodeEntries/EdgeEntries materialise and order the whole index. They sit on
// the compaction path (disk.Store.Compact serialises their output straight into
// the CSR index section), so their cost is paid on every compaction and scales
// with total indexed entries rather than with what changed.
//
// The shape below mirrors buildDurabilityFixture in the root benchmark suite:
// two keys per node, one high-cardinality (a hash, distinct per node) and one
// low-cardinality (a bucket, ~1000 distinct values). Cardinality matters because
// it decides how much work an ordering has to do beyond comparing IDs.
func benchPropertyIndex(n int) *PropertyIndex {
	p := NewPropertyIndex()
	for i := 0; i < n; i++ {
		id := store.NodeID(i + 1)
		p.IndexNode(id, "sha256", []byte(fmt.Sprintf("hash-%07d", i)))
		p.IndexNode(id, "bucket", []byte(fmt.Sprintf("bucket-%04d", i%1000)))
	}
	return p
}

func BenchmarkNodeEntries(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			p := benchPropertyIndex(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := len(p.NodeEntries()); got != n*2 {
					b.Fatalf("got %d entries, want %d", got, n*2)
				}
			}
		})
	}
}

// Why NodeEntries emits in (Key, Value, ID) order rather than (ID, Key, Value).
//
// Both orders are canonical, so both satisfy the determinism requirement that
// motivated ordering at all. They differ in cost, and the difference is
// structural rather than incidental: the index nests key → value → ID-ascending
// postings, so (Key, Value, ID) is a sorted walk of the layout while
// (ID, Key, Value) is a transpose of it — a full sort plus a permutation in
// which every entry lands in a random slot.
//
// Measuring the two as separate binaries makes the comparison hostage to machine
// drift. Running both here, over one fixture in one process, does not.
//
//	go test ./index/ -bench=BenchmarkNodeEntriesOrdering -benchmem -run=^$
func BenchmarkNodeEntriesOrdering(b *testing.B) {
	const n = 50_000
	p := benchPropertyIndex(n)

	b.Run("structure-order/KeyValueID", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if got := len(p.NodeEntries()); got != n*2 {
				b.Fatalf("got %d entries, want %d", got, n*2)
			}
		}
	})

	b.Run("sort-entries/IDKeyValue", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if got := len(nodeEntriesByIDReference(p)); got != n*2 {
				b.Fatalf("got %d entries, want %d", got, n*2)
			}
		}
	})
}

// nodeEntriesByIDReference is the rejected (ID, Key, Value) implementation, kept
// only as the comparison arm of BenchmarkNodeEntriesOrdering. It is the obvious
// way to write this — collect, then sort — and exists here so the claim that it
// is the more expensive way stays checkable rather than remembered.
func nodeEntriesByIDReference(p *PropertyIndex) []NodePropEntry {
	out := make([]NodePropEntry, 0, p.nodeEntryCount())
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		sh.nodes.forEachAll(func(id store.NodeID, key string, value []byte) bool {
			out = append(out, NodePropEntry{ID: id, Key: key, Value: value})
			return true
		})
		sh.mu.RUnlock()
	}
	slices.SortFunc(out, func(a, b NodePropEntry) int {
		if a.ID != b.ID {
			return cmp.Compare(a.ID, b.ID)
		}
		if c := cmp.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return bytes.Compare(a.Value, b.Value)
	})
	return out
}
