//go:build stress

package graphene_test

// StorageStats, measured because N2 made it do arithmetic.
//
// Before N2 the call read maintained counters and returned. After it, the same
// call also totals seven resident terms, one of which takes every property shard's
// read lock and one of which walks the label postings. Those are all bounded by
// the number of declared keys and distinct labels rather than by the store's size,
// which is the claim this benchmark exists to check: the figure is read by
// AutoCompact on a ticker, so a cost proportional to the data would put a pass
// over the store on a timer.
//
// It was placed identically in both arms of N2's A/B, which is why it calls nothing
// N2 added: it compiles against either tree and measures the same method in both.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func benchmarkStorageStats(b *testing.B, n int) {
	g, err := graphene.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatal(err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%07d", i)),
			"bucket": []byte(fmt.Sprintf("bucket-%04d", i%1000)),
		}); err != nil {
			b.Fatal(err)
		}
	}
	if err := g.Compact(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		st, ok := g.StorageStats()
		if !ok {
			b.Fatal("no StorageStats")
		}
		if st.CSRNodes == 0 {
			b.Fatal("empty stats")
		}
	}
}

// Two sizes, twenty-five times apart. The whole point is that they should cost
// the same: if the larger one is slower the figure is not bounded by the
// declarations after all, and a ticker is walking the store.
func BenchmarkStorageStats_2k(b *testing.B)  { benchmarkStorageStats(b, 2_000) }
func BenchmarkStorageStats_50k(b *testing.B) { benchmarkStorageStats(b, 50_000) }
