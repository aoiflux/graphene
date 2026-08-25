// Coverage benchmarks: the write path, a scale sweep, and the public API
// surface that had no numbers at all.
//
// The original suite measured five operations. Everything else — batch ingest,
// DFS, provenance chains, pattern matching, subgraph extraction, connectivity
// helpers, edge and relation queries, cold open — was unmeasured, which means
// any regression in them would have been invisible.
//
//	go test . -tags=stress -bench='Ingest|Scale|Walk|Pattern|Subgraph|Connect|QueryEdges|ColdOpen' -benchmem -run=^$

//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

// =============================================================================
// Write path
// =============================================================================

func BenchmarkIngest_AddNode_Single(b *testing.B) {
	g := graphene.NewInMemory()
	node := &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.AddNode(node); err != nil {
			b.Fatal(err)
		}
	}
}

// Batched ingest amortises the lock hold and the slice growth across the batch.
// Reported per node, so it is directly comparable with the single-node figure.
func benchmarkAddNodesBatch(b *testing.B, batch int) {
	g := graphene.NewInMemory()
	nodes := make([]*store.Node, batch)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.AddNodes(nodes); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/node")
}

func BenchmarkIngest_AddNodes_Batch100(b *testing.B)  { benchmarkAddNodesBatch(b, 100) }
func BenchmarkIngest_AddNodes_Batch1000(b *testing.B) { benchmarkAddNodesBatch(b, 1000) }

func BenchmarkIngest_AddEdge_Single(b *testing.B) {
	g := graphene.NewInMemory()
	a, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		b.Fatal(err)
	}
	c, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		b.Fatal(err)
	}
	e := &store.Edge{Src: a, Dst: c, Labels: []store.EdgeType{store.EdgeTypeContains}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.AddEdge(e); err != nil {
			b.Fatal(err)
		}
	}
}

// End-to-end ingest of a whole graph, the shape a bulk load actually takes:
// batch nodes, register properties, batch edges, compact.
func BenchmarkIngest_EndToEnd_Disk_10k(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, err := os.MkdirTemp("", "graphene-ingest-*")
		if err != nil {
			b.Fatal(err)
		}
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		buildFixture(g, 10_000)
		if err := g.Compact(); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		g.Close()
		os.RemoveAll(dir)
		b.StartTimer()
	}
}

// The write cost of the WAL on the durable backend, versus the in-memory store.
func BenchmarkIngest_AddNode_Disk(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-ingest-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	node := &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.AddNode(node); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Scale sweep — does cost track the answer, or the graph?
// =============================================================================
//
// A query served from an index should be flat across these; one that scans
// should climb linearly. Running the same operation at three sizes turns that
// into an observation rather than an assumption.

var (
	scaleOnce  sync.Map // size -> *sync.Once
	scaleGraph sync.Map // size -> *benchFixture
)

func scaleFixture(size int) *benchFixture {
	onceAny, _ := scaleOnce.LoadOrStore(size, &sync.Once{})
	onceAny.(*sync.Once).Do(func() {
		scaleGraph.Store(size, buildFixture(graphene.NewInMemory(), size))
	})
	v, _ := scaleGraph.Load(size)
	return v.(*benchFixture)
}

func benchmarkScalePointLookup(b *testing.B, size int) {
	f := scaleFixture(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GetNode(f.ids[i%len(f.ids)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScale_PointLookup_10k(b *testing.B)  { benchmarkScalePointLookup(b, 10_000) }
func BenchmarkScale_PointLookup_100k(b *testing.B) { benchmarkScalePointLookup(b, 100_000) }

func benchmarkScaleEqualityQuery(b *testing.B, size int) {
	f := scaleFixture(size)
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte(fmt.Sprintf("hash-%07d", size/2))},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScale_EqualityQuery_10k(b *testing.B)  { benchmarkScaleEqualityQuery(b, 10_000) }
func BenchmarkScale_EqualityQuery_100k(b *testing.B) { benchmarkScaleEqualityQuery(b, 100_000) }

func benchmarkScaleTypeQuery(b *testing.B, size int) {
	f := scaleFixture(size)
	q := store.NodeQuery{Types: []store.NodeType{store.NodeTypeCase}, Limit: 10}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScale_TypeQuery_10k(b *testing.B)  { benchmarkScaleTypeQuery(b, 10_000) }
func BenchmarkScale_TypeQuery_100k(b *testing.B) { benchmarkScaleTypeQuery(b, 100_000) }

func benchmarkScaleBFS(b *testing.B, size int) {
	f := scaleFixture(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.BFS(f.midNode, 4, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScale_BFS4Hop_10k(b *testing.B)  { benchmarkScaleBFS(b, 10_000) }
func BenchmarkScale_BFS4Hop_100k(b *testing.B) { benchmarkScaleBFS(b, 100_000) }

// =============================================================================
// Traversal variants that had no numbers
// =============================================================================

func BenchmarkWalk_DFS_Deep(b *testing.B) {
	g, ids := buildChain(b, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.DFS(ids[0], 10_000, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWalk_ProvenanceChain(b *testing.B) {
	g, ids := buildChain(b, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.ProvenanceChain(ids[len(ids)-1], 64, []store.EdgeType{store.EdgeTypeContains}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWalk_ShortestPath_Disk(b *testing.B) {
	f := diskGraph()
	src, dst := f.ids[0], f.ids[200]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.ShortestPath(src, dst, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Pattern matching and subgraph extraction
// =============================================================================

func BenchmarkPattern_TwoHop_Scoped(b *testing.B) {
	f := memGraph()
	pattern := &traversal.Pattern{
		Nodes: []traversal.PatternNode{
			{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		},
		Edges: []traversal.PatternEdge{
			{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeContains}},
		},
	}
	scope := f.ids[:2_000]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.FindPatterns(pattern, scope, 100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubgraph_Induced_1k(b *testing.B) {
	f := memGraph()
	scope := f.ids[:1_000]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := f.g.InducedSubgraph(scope); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Connectivity helpers
// =============================================================================

func BenchmarkConnect_EdgeExists(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := f.ids[i%(len(f.ids)-1)]
		dst := f.ids[i%(len(f.ids)-1)+1]
		if _, err := f.g.EdgeExists(src, dst, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnect_IsConnected_Near(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.IsConnected(f.ids[0], f.ids[50]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnect_NeighboursByNodeType(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := f.g.NeighboursByNodeType(f.ids[i%len(f.ids)],
			store.DirectionOutbound, store.NodeTypeMicroArtefact, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Edge and relation queries
// =============================================================================

func BenchmarkQueryEdges_ByType_Memory(b *testing.B) {
	f := memGraph()
	q := store.EdgeQuery{Types: []store.EdgeType{store.EdgeTypeBelongsTo}, Limit: 50}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryEdgeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryEdges_ByType_Disk(b *testing.B) {
	f := diskGraph()
	q := store.EdgeQuery{Types: []store.EdgeType{store.EdgeTypeBelongsTo}, Limit: 50}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryEdgeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryRelations_Both_Disk(b *testing.B) {
	f := diskGraph()
	q := store.RelationQuery{Anchors: []store.NodeID{f.hub}, Direction: store.DirectionBoth}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryRelationIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDegree_Typed_Disk(b *testing.B) {
	f := diskGraph()
	types := []store.EdgeType{store.EdgeTypeBelongsTo}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.InDegree(f.hub, types); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Cold open — WAL replay with nothing compacted
// =============================================================================

// The uncompacted counterpart to BenchmarkReopenCompactedStore: everything is
// still in the WAL, so open pays full replay.
func BenchmarkColdOpen_UncompactedWAL_10k(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-coldopen-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	buildFixture(g, 10_000)
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reopened, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		if err := reopened.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
