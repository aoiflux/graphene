package index

// What the streaming entry walk costs, in the three shapes a store is in.
//
// ForEachNodeProperty is the export path and the compaction payload path, so it
// is walked once per compaction and once per dump over every entry the index
// holds. It is also the walk that stopped holding a shard lock across its
// callback, which cost it a copy of the delta's side per key — so it is the one
// that has to be measured rather than reasoned about.
//
//	go test ./index/ -bench=BenchmarkForEachNodeProperty -benchmem -run=^$
//
// The three shapes are not variations on one another. delta is a store that has
// never compacted: every entry is in the shards and the copy is over all of
// them. base is a compacted store with nothing written since: the shards are
// empty, so the copy is nothing and the walk is the base's runs alone. mixed is
// the ordinary running state, and is the only one where both halves are paid.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// benchTriples is benchPropertyIndex's corpus as base triples: the same two keys
// per node, one distinct per node and one over a thousand buckets.
func benchTriples(n int) []triple {
	out := make([]triple, 0, n*2)
	for i := range n {
		id := uint64(i + 1)
		out = append(out,
			triple{NodeKind, id, "sha256", []byte(fmt.Sprintf("hash-%07d", i))},
			triple{NodeKind, id, "bucket", []byte(fmt.Sprintf("bucket-%04d", i%1000))})
	}
	return out
}

func BenchmarkForEachNodeProperty(b *testing.B) {
	for _, n := range []int{10_000, 50_000} {
		// Never compacted: every entry in the shards.
		b.Run(fmt.Sprintf("delta/nodes=%d", n), func(b *testing.B) {
			benchWalk(b, benchPropertyIndex(n), n*2)
		})

		// Compacted and quiet: every entry in the base, shards empty.
		b.Run(fmt.Sprintf("base/nodes=%d", n), func(b *testing.B) {
			p := NewPropertyIndex()
			if err := p.AttachBase(newFakeBase(benchTriples(n))); err != nil {
				b.Fatal(err)
			}
			benchWalk(b, p, n*2)
		})

		// Compacted, then written to: both halves merged, which is what a store
		// between two compactions is doing.
		b.Run(fmt.Sprintf("mixed/nodes=%d", n), func(b *testing.B) {
			p := NewPropertyIndex()
			if err := p.AttachBase(newFakeBase(benchTriples(n))); err != nil {
				b.Fatal(err)
			}
			extra := n / 10
			for i := range extra {
				id := store.NodeID(n + i + 1)
				p.IndexNode(id, "sha256", []byte(fmt.Sprintf("late-%07d", i)))
				p.IndexNode(id, "bucket", []byte(fmt.Sprintf("bucket-%04d", i%1000)))
			}
			benchWalk(b, p, (n+extra)*2)
		})
	}
}

// benchWalk drains the walk and checks the count, so a walk that stopped early
// cannot look fast.
func benchWalk(b *testing.B, p *PropertyIndex, want int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		got := 0
		p.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
			got++
			return true
		})
		if got != want {
			b.Fatalf("the walk yielded %d entries, want %d", got, want)
		}
	}
}
