// Memory-footprint benchmarks.
//
// The rest of the suite reports B/op, which is bytes *allocated during* an
// operation. That is not the same question as how much heap a loaded graph
// *occupies*, which is the second-ranked optimisation axis. These measure the
// latter: build a graph, drop everything transient, and read what is still live.
//
//	go test . -tags=stress -bench=Footprint -benchtime=1x -run=^$
//
// Reported metrics are bytes-per-node and bytes-per-edge, so results are
// comparable across fixture sizes.

//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/index/encoding"
	"github.com/aoiflux/graphene/store"
)

// liveHeap returns the currently reachable heap in bytes.
//
// Two GC cycles: the first runs finalisers and frees what died during the build,
// the second collects anything the first resurrected or newly unreachable. The
// keepAlive argument must be the graph under test, otherwise the compiler is
// free to collect it before the measurement runs and the number becomes zero.
func liveHeap(keepAlive any) uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(keepAlive)
	return ms.HeapAlloc
}

// footprintOf builds a graph with build() and reports the heap it retains,
// having subtracted the heap that was already live beforehand.
func footprintOf(b *testing.B, nodes, edges int, build func() any) {
	b.Helper()

	base := liveHeap(nil)
	g := build()
	total := liveHeap(g)

	var retained uint64
	if total > base {
		retained = total - base
	}

	b.ReportMetric(float64(retained)/float64(nodes), "B/node")
	if edges > 0 {
		b.ReportMetric(float64(retained)/float64(edges), "B/edge")
	}
	b.ReportMetric(float64(retained)/(1<<20), "MiB_total")
	// The default ns/op is meaningless for a one-shot build; zero it out so the
	// reported metrics are the only signal.
	b.ReportMetric(0, "ns/op")
}

const footprintNodes = 100_000

// buildFootprintGraph populates g and returns the number of edges added.
func buildFootprintGraph(g *graphene.Graph, n int, withProps, ordered bool) int {
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabel(i)}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		panic(err)
	}

	if withProps {
		for i, id := range ids {
			err := g.IndexNodeProperties(id, map[string][]byte{
				"sha256": []byte(fmt.Sprintf("hash-%07d", i)),
				"bucket": []byte(fmt.Sprintf("bucket-%04d", i%1000)),
				"score":  encoding.Int64(int64(i % 1000)),
			})
			if err != nil {
				panic(err)
			}
		}
	}
	if ordered {
		if err := g.DeclareOrderedProperty("score"); err != nil {
			panic(err)
		}
	}

	edges := make([]*store.Edge, 0, 2*n)
	for i := 0; i < n-1; i++ {
		edges = append(edges, &store.Edge{
			Src: ids[i], Dst: ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	for i := 0; i < n; i++ {
		edges = append(edges, &store.Edge{
			Src: ids[i], Dst: ids[(i+13)%n],
			Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		panic(err)
	}
	return len(edges)
}

// --- Baselines: topology only, no property index ---

func BenchmarkFootprint_Memory_TopologyOnly(b *testing.B) {
	var edges int
	footprintOf(b, footprintNodes, 2*footprintNodes-1, func() any {
		g := graphene.NewInMemory()
		edges = buildFootprintGraph(g, footprintNodes, false, false)
		return g
	})
	_ = edges
}

func BenchmarkFootprint_Disk_TopologyOnly(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-fp-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	footprintOf(b, footprintNodes, 2*footprintNodes-1, func() any {
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		buildFootprintGraph(g, footprintNodes, false, false)
		if err := g.Compact(); err != nil {
			b.Fatal(err)
		}
		return g
	})
}

// --- With the property index: 3 entries per node ---

func BenchmarkFootprint_Memory_WithPropertyIndex(b *testing.B) {
	footprintOf(b, footprintNodes, 2*footprintNodes-1, func() any {
		g := graphene.NewInMemory()
		buildFootprintGraph(g, footprintNodes, true, false)
		return g
	})
}

func BenchmarkFootprint_Disk_WithPropertyIndex(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-fp-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	footprintOf(b, footprintNodes, 2*footprintNodes-1, func() any {
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		buildFootprintGraph(g, footprintNodes, true, false)
		if err := g.Compact(); err != nil {
			b.Fatal(err)
		}
		return g
	})
}

// --- With an ordered key declared on top ---

func BenchmarkFootprint_Memory_WithOrderedKey(b *testing.B) {
	footprintOf(b, footprintNodes, 2*footprintNodes-1, func() any {
		g := graphene.NewInMemory()
		buildFootprintGraph(g, footprintNodes, true, true)
		return g
	})
}

// --- The cost of deletions the CSR has not reclaimed ---
//
// The CSR addresses records by ID through a dense array, so its size follows the
// highest ID ever issued, not the number of live records. Deleting half the graph
// should therefore free the records but not the array — this measures how much.

func BenchmarkFootprint_Disk_HalfDeleted_Uncompacted(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-fp-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	live := footprintNodes / 2
	footprintOf(b, live, live, func() any {
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		buildFootprintGraph(g, footprintNodes, false, false)
		if err := g.Compact(); err != nil {
			b.Fatal(err)
		}
		// Delete every other node without compacting afterwards.
		for i := 0; i < footprintNodes; i += 2 {
			_ = g.DeleteNode(store.NodeID(i + 1))
		}
		return g
	})
}

func BenchmarkFootprint_Disk_HalfDeleted_Compacted(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-fp-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	live := footprintNodes / 2
	footprintOf(b, live, live, func() any {
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		buildFootprintGraph(g, footprintNodes, false, false)
		if err := g.Compact(); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < footprintNodes; i += 2 {
			_ = g.DeleteNode(store.NodeID(i + 1))
		}
		// Compact should reclaim the deleted records — but the dense arrays are
		// still sized by the highest surviving ID.
		if err := g.Compact(); err != nil {
			b.Fatal(err)
		}
		return g
	})
}

// --- Where does the property index's ~107 B per entry go? ---
//
// Same number of entries, varying how many *distinct* values they share. If the
// per-entry cost barely moves as cardinality falls, each entry is holding its own
// copy of the value and interning would pay. If it drops sharply, the value bytes
// are already shared and the cost is postings plus map overhead instead — in
// which case interning would add a map for nothing.
//
// Measure before optimising; the answer decides whether the interning item is
// worth doing at all.

func benchmarkPropertyIndexFootprint(b *testing.B, entries, distinctValues int) {
	footprintOf(b, entries, 0, func() any {
		g := graphene.NewInMemory()
		nodes := make([]*store.Node, entries)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			panic(err)
		}
		for i, id := range ids {
			// A fixed-width value so byte length is constant across cardinalities
			// and only the number of distinct strings varies.
			v := []byte(fmt.Sprintf("value-%026d", i%distinctValues))
			if err := g.IndexNodeProperty(id, "k", v); err != nil {
				panic(err)
			}
		}
		return g
	})
}

// 100k entries, 1 distinct value — maximum sharing opportunity.
func BenchmarkFootprint_PropIndex_Cardinality1(b *testing.B) {
	benchmarkPropertyIndexFootprint(b, 100_000, 1)
}

// 100k entries, 100 distinct values.
func BenchmarkFootprint_PropIndex_Cardinality100(b *testing.B) {
	benchmarkPropertyIndexFootprint(b, 100_000, 100)
}

// 100k entries, 10k distinct values.
func BenchmarkFootprint_PropIndex_Cardinality10k(b *testing.B) {
	benchmarkPropertyIndexFootprint(b, 100_000, 10_000)
}

// 100k entries, all distinct — no sharing possible.
func BenchmarkFootprint_PropIndex_CardinalityAll(b *testing.B) {
	benchmarkPropertyIndexFootprint(b, 100_000, 100_000)
}

// Topology-only control at the same node count, so the property index's own
// share can be isolated by subtraction.
func BenchmarkFootprint_PropIndex_NoIndex(b *testing.B) {
	footprintOf(b, 100_000, 0, func() any {
		g := graphene.NewInMemory()
		nodes := make([]*store.Node, 100_000)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		if _, err := g.AddNodes(nodes); err != nil {
			panic(err)
		}
		return g
	})
}

// --- On-disk size, the other half of P1 ---

func BenchmarkFootprint_DiskFileSize(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-fp-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	edges := buildFootprintGraph(g, footprintNodes, true, false)
	if err := g.Compact(); err != nil {
		b.Fatal(err)
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}

	csr, err := os.Stat(dir + string(os.PathSeparator) + "graphene.csr")
	if err != nil {
		b.Fatal(err)
	}
	wal, err := os.Stat(dir + string(os.PathSeparator) + "graphene.wal")
	if err != nil {
		b.Fatal(err)
	}

	total := csr.Size() + wal.Size()
	b.ReportMetric(float64(total)/float64(footprintNodes), "B/node")
	b.ReportMetric(float64(total)/float64(edges), "B/edge")
	b.ReportMetric(float64(csr.Size())/(1<<20), "CSR_MiB")
	b.ReportMetric(float64(wal.Size())/(1<<20), "WAL_MiB")
	b.ReportMetric(0, "ns/op")
}
