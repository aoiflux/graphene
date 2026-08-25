//go:build stress

// Bulk read and write baselines.
//
// These exist before the bulk work does, deliberately: the plan's sequencing puts
// "establish baselines" ahead of every implementation step, because the bulk read
// path had no benchmark at all and there was nothing to improve *against*.
//
// Two things are measured that the rest of the suite does not:
//
//   - **Batch size is swept**, because the whole question is whether batching
//     amortises anything. A single size cannot show that.
//   - **Shuffled versus sequential IDs**, which isolates the cache-locality win a
//     sorted walk could capture. If shuffled is not materially slower, there is no
//     locality to recover and sorting would be pure overhead.

package graphene_test

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

var bulkSizes = []int{100, 1_000, 10_000, 100_000}

// --- Bulk reads ---------------------------------------------------------

// idsAscending returns n IDs from the fixture in ascending order.
func idsAscending(f *benchFixture, n int) []store.NodeID {
	if n > len(f.ids) {
		n = len(f.ids)
	}
	out := make([]store.NodeID, n)
	copy(out, f.ids[:n])
	return out
}

// idsShuffled returns the same n IDs in a fixed pseudo-random order, so the only
// difference from idsAscending is access order.
func idsShuffled(f *benchFixture, n int) []store.NodeID {
	out := idsAscending(f, n)
	r := rand.New(rand.NewSource(42)) // fixed seed: same permutation every run
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func benchmarkGetNodes(b *testing.B, f *benchFixture, ids []store.NodeID) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, _, err := f.g.GetNodes(ids)
		if err != nil {
			b.Fatal(err)
		}
		if len(got) != len(ids) {
			b.Fatalf("got %d nodes, want %d", len(got), len(ids))
		}
	}
	// Per-node cost is the number that matters; b.N counts whole batches.
	b.ReportMetric(float64(len(ids)), "nodes/op")
}

func BenchmarkBulkRead_GetNodes_Memory(b *testing.B) {
	f := memGraph()
	for _, n := range bulkSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkGetNodes(b, f, idsAscending(f, n))
		})
	}
}

func BenchmarkBulkRead_GetNodes_Disk(b *testing.B) {
	f := diskGraph()
	for _, n := range bulkSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkGetNodes(b, f, idsAscending(f, n))
		})
	}
}

// The pair that decides whether a sorted walk is worth building. If these two
// measure the same, there is no locality win available and step 8 of the plan
// should be dropped rather than implemented.
func BenchmarkBulkRead_GetNodes_Shuffled_Memory(b *testing.B) {
	f := memGraph()
	for _, n := range bulkSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkGetNodes(b, f, idsShuffled(f, n))
		})
	}
}

func BenchmarkBulkRead_GetNodes_Shuffled_Disk(b *testing.B) {
	f := diskGraph()
	for _, n := range bulkSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkGetNodes(b, f, idsShuffled(f, n))
		})
	}
}

// The per-item loop the batch API is supposed to beat. If GetNodes does not beat
// this, it is not earning its existence.
func BenchmarkBulkRead_GetNodeLoop_Memory(b *testing.B) {
	f := memGraph()
	for _, n := range bulkSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ids := idsAscending(f, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, id := range ids {
					if _, err := f.g.GetNode(id); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// --- Bulk writes --------------------------------------------------------

func makeNodes(n int) []*store.Node {
	out := make([]*store.Node, n)
	for i := range out {
		out[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	return out
}

// Each iteration writes into a fresh store, so batch N is measured against an
// empty graph every time rather than one that grew across iterations.
func benchmarkAddNodesFresh(b *testing.B, n int, disk bool) {
	nodes := makeNodes(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var g *graphene.Graph
		var dir string
		if disk {
			d, err := os.MkdirTemp("", "graphene-bulkw-*")
			if err != nil {
				b.Fatal(err)
			}
			dir = d
			gg, err := graphene.Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			g = gg
		} else {
			g = graphene.NewInMemory()
		}
		b.StartTimer()

		if _, err := g.AddNodes(nodes); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		g.Close()
		if dir != "" {
			os.RemoveAll(dir)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(n), "nodes/op")
}

func BenchmarkBulkWrite_AddNodes_Memory(b *testing.B) {
	for _, n := range bulkSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) { benchmarkAddNodesFresh(b, n, false) })
	}
}

// The WAL-bound case — the one the bulk work is really aimed at.
func BenchmarkBulkWrite_AddNodes_Disk(b *testing.B) {
	for _, n := range bulkSizes[:3] { // 100k on disk per iteration is too slow to sweep
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) { benchmarkAddNodesFresh(b, n, true) })
	}
}

// The per-item loop, for the same reason as the read side.
func BenchmarkBulkWrite_AddNodeLoop_Memory(b *testing.B) {
	for _, n := range bulkSizes[:3] {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			nodes := makeNodes(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				g := graphene.NewInMemory()
				b.StartTimer()
				for _, nd := range nodes {
					if _, err := g.AddNode(nd); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				g.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkBulkWrite_AddEdges_Memory(b *testing.B) {
	for _, n := range bulkSizes[:3] {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				g := graphene.NewInMemory()
				ids, err := g.AddNodes(makeNodes(n + 1))
				if err != nil {
					b.Fatal(err)
				}
				edges := make([]*store.Edge, n)
				for j := range edges {
					edges[j] = &store.Edge{
						Src: ids[j], Dst: ids[j+1],
						Labels: []store.EdgeType{store.EdgeTypeContains},
					}
				}
				b.StartTimer()

				if _, err := g.AddEdges(edges); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				g.Close()
				b.StartTimer()
			}
			b.ReportMetric(float64(n), "edges/op")
		})
	}
}

// Sync-on-commit is a durability change, not a performance one, so it is
// measured separately: batching removes syscalls, fsync adds one back. Reporting
// only the combined figure would hide which effect dominates.
func BenchmarkBulkWrite_AddNodes_Disk_NoSync(b *testing.B) {
	for _, n := range bulkSizes[:3] {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			nodes := makeNodes(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir, err := os.MkdirTemp("", "graphene-bulkw-ns-*")
				if err != nil {
					b.Fatal(err)
				}
				g, err := graphene.Open(dir)
				if err != nil {
					b.Fatal(err)
				}
				if ds, ok := g.GraphStore.(interface{ SetSyncOnCommit(bool) }); ok {
					ds.SetSyncOnCommit(false)
				}
				b.StartTimer()

				if _, err := g.AddNodes(nodes); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				g.Close()
				os.RemoveAll(dir)
				b.StartTimer()
			}
			b.ReportMetric(float64(n), "nodes/op")
		})
	}
}

// --- Step 8 probe: is a sorted walk worth building? ---------------------
//
// The disk CSR is a flat array indexed by ID, so ascending IDs walk memory
// sequentially and shuffled ones thrash cache — measured at up to 3.3× on a 100k
// batch. The proposed optimisation is to sort internally and permute the results
// back into request order.
//
// That is an *upper bound*, not a result: the sort and the permutation have to be
// paid out of the win. This prototypes the whole optimisation in the benchmark —
// allocate pairs, sort, call, permute — so the real cost is measured before any
// production code is written for it.

type idPos struct {
	id  store.NodeID
	pos int
}

// sortedWalk is what the optimisation would do, cost included.
func sortedWalk(g *graphene.Graph, ids []store.NodeID) ([]*store.Node, error) {
	pairs := make([]idPos, len(ids))
	for i, id := range ids {
		pairs[i] = idPos{id: id, pos: i}
	}
	sort.Slice(pairs, func(a, b int) bool { return pairs[a].id < pairs[b].id })

	sorted := make([]store.NodeID, len(pairs))
	for i, p := range pairs {
		sorted[i] = p.id
	}
	found, _, err := g.GetNodes(sorted)
	if err != nil {
		return nil, err
	}
	// Permute back into request order.
	out := make([]*store.Node, len(found))
	for i, n := range found {
		out[pairs[i].pos] = n
	}
	return out, nil
}

func BenchmarkBulkRead_Shuffled_Direct_Disk(b *testing.B) {
	f := diskGraph()
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ids := idsShuffled(f, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := f.g.GetNodes(ids); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBulkRead_Shuffled_SortedWalk_Disk(b *testing.B) {
	f := diskGraph()
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ids := idsShuffled(f, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := sortedWalk(f.g, ids); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
