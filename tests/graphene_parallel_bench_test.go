// Concurrency benchmarks.
//
// Every other benchmark in this suite is single-threaded, which hides the axis
// that matters most for a store built on RWMutex: how throughput behaves when
// many goroutines hit it at once. Read paths should scale with cores; write
// paths serialise on the store lock and will not.
//
// These also supply the evidence the plan's "shard PropertyIndex locks" item is
// waiting on — that change cannot be justified or measured without a contended
// benchmark.
//
//	go test . -tags=stress -bench=Parallel -benchmem -run=^$
//
// Run with -cpu=1,2,4,8,16 to see the scaling curve rather than a single point.

//go:build stress

package graphene_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// --- Read paths: should scale with cores ---

func BenchmarkParallel_PointLookup_Memory(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := f.g.GetNode(f.ids[i%len(f.ids)]); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

func BenchmarkParallel_PointLookup_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := f.g.GetNode(f.ids[i%len(f.ids)]); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

func BenchmarkParallel_PropertyEqual_Memory(b *testing.B) {
	f := memGraph()
	// Precompute the lookup keys. Formatting them inside the timed region made
	// Sprintf the dominant cost and masked whatever the index was doing, which
	// defeats the point of a contention benchmark.
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("hash-%07d", i%benchNodeCount))
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := f.g.NodesByProperty("sha256", keys[i&1023]); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

func BenchmarkParallel_Neighbours_Memory(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := f.g.Neighbours(f.ids[i%len(f.ids)], store.DirectionOutbound, nil); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

func BenchmarkParallel_BFS3Hop_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := f.g.BFS(f.ids[i%len(f.ids)], 3, store.DirectionOutbound, nil); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

// --- Write paths: serialise on the store lock ---

func BenchmarkParallel_AddNode_Memory(b *testing.B) {
	g := graphene.NewInMemory()
	node := &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := g.AddNode(node); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// Contention on the property index specifically. PropertyIndex holds one global
// RWMutex, so concurrent registration serialises on it regardless of whether the
// goroutines touch the same key — which is exactly what sharding by key hash
// would fix.
func BenchmarkParallel_IndexNodeProperty_SameKey(b *testing.B) {
	g := graphene.NewInMemory()
	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		b.Fatal(err)
	}
	var counter atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			if err := g.IndexNodeProperty(id, "sha256", []byte(fmt.Sprintf("v-%d", n))); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// The same load spread over distinct keys. With one global lock this performs
// identically to the same-key case; with per-key sharding it should not.
func BenchmarkParallel_IndexNodeProperty_DistinctKeys(b *testing.B) {
	g := graphene.NewInMemory()
	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		b.Fatal(err)
	}
	var counter atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			key := fmt.Sprintf("key-%d", n%16)
			if err := g.IndexNodeProperty(id, key, []byte(fmt.Sprintf("v-%d", n))); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// --- Mixed workload: the realistic case ---

// Nine reads per write, all concurrent. A read-mostly load on an RWMutex should
// still scale, but every writer briefly excludes every reader.
func BenchmarkParallel_MixedReadWrite_Memory(b *testing.B) {
	g := graphene.NewInMemory()
	const seed = 20_000
	nodes := make([]*store.Node, seed)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabel(i)}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < seed-1; i++ {
		if _, err := g.AddEdge(&store.Edge{
			Src: ids[i], Dst: ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 9 {
				if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
					b.Error(err)
					return
				}
			} else {
				if _, err := g.Neighbours(ids[i%len(ids)], store.DirectionOutbound, nil); err != nil {
					b.Error(err)
					return
				}
			}
			i++
		}
	})
}
