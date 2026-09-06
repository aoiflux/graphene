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

// The weighted searches, on the same fixture and endpoints as
// BenchmarkWalk_ShortestPath_Disk above so the three numbers are comparable.
//
// The cost function is the similarity *gap*, not the weight. That is not a
// stylistic choice: the fixture puts 0.5 on chain edges and 0.9 on the +13
// strides because Weight is a similarity score, so reading the weight directly
// as a distance inverts the graph — the strides, which are the shortcuts,
// become the expensive edges, and the search grinds down the chain one hop at a
// time. The gap reading (1 - weight) makes a stride cost 0.1 against a chain
// hop's 0.5, which is the same topology the unweighted search sees.
func benchSimilarityGap(e store.IncidentEdge) float64 { return 1 - float64(e.Weight) }

func BenchmarkWalk_ShortestWeightedPath_Disk(b *testing.B) {
	f := diskGraph()
	src, dst := f.ids[0], f.ids[200]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.ShortestWeightedPath(src, dst, nil, benchSimilarityGap); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// A weighted grid, because A* needs a graph a heuristic can say something about
// -----------------------------------------------------------------------------
//
// The shared fixture cannot host this benchmark. Its 1000 inbound edges into
// one hub put every node within two hops of every other, which makes any
// estimate based on how far apart two nodes look — the only structure that
// fixture has — an overestimate, and an overestimating heuristic does not make
// A* slower, it makes it wrong. The first draft of this benchmark asserted
// otherwise and the check below is what caught it.
//
// A grid has the property the hub destroys: distance in the graph is bounded
// below by distance in the layout, so Manhattan distance times the cheapest
// edge is admissible by construction, and consistent too.

const (
	gridSide     = 100 // gridSide × gridSide nodes
	gridFastCost = 0.5 // the cheapest edge, and so the heuristic's scale
)

var (
	gridFixtureOnce sync.Once
	gridFixture     *benchFixture
	gridFixtureDir  string
)

// gridGraph returns a gridSide × gridSide 4-connected lattice on disk,
// compacted, with two classes of edge cost.
//
// Costs come from the same similarity-gap reading the other weighted benchmarks
// use, so a "fast" edge carries a high similarity. Alternating them by row and
// column means the cheapest route is not simply the straight one, which is what
// keeps the search from being a formality.
func gridGraph() *benchFixture {
	gridFixtureOnce.Do(func() {
		dir, err := os.MkdirTemp("", "graphene-bench-grid-*")
		if err != nil {
			panic(err)
		}
		gridFixtureDir = dir
		g, err := graphene.Open(dir)
		if err != nil {
			panic(err)
		}

		n := gridSide * gridSide
		nodes := make([]*store.Node, n)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			panic(fmt.Sprintf("grid fixture: AddNodes: %v", err))
		}

		at := func(r, c int) store.NodeID { return ids[r*gridSide+c] }
		edges := make([]*store.Edge, 0, 2*n)
		link := func(a, b store.NodeID, fast bool) {
			w := float32(0)
			if fast {
				w = 1 - gridFastCost
			}
			edges = append(edges, &store.Edge{
				Src: a, Dst: b,
				Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
				Weight: w,
			})
		}
		for r := 0; r < gridSide; r++ {
			for c := 0; c < gridSide; c++ {
				if c+1 < gridSide {
					link(at(r, c), at(r, c+1), r%3 == 0)
				}
				if r+1 < gridSide {
					link(at(r, c), at(r+1, c), c%3 == 0)
				}
			}
		}
		if _, err := g.AddEdges(edges); err != nil {
			panic(fmt.Sprintf("grid fixture: AddEdges: %v", err))
		}
		if err := g.Compact(); err != nil {
			panic(fmt.Sprintf("grid fixture: Compact: %v", err))
		}
		gridFixture = &benchFixture{g: g, ids: ids, midNode: at(gridSide/2, gridSide/2)}
	})
	return gridFixture
}

// gridManhattan is the admissible, consistent heuristic for the grid: the
// fewest edges that can separate two cells, each priced at the cheapest edge
// the graph contains. It cannot overestimate, because no route can use fewer
// edges than the layout requires or a cheaper edge than the cheapest one.
func gridManhattan(ids []store.NodeID, dst store.NodeID) store.NodeHeuristic {
	base := ids[0]
	dr, dc := int(dst-base)/gridSide, int(dst-base)%gridSide
	return func(id store.NodeID) float64 {
		r, c := int(id-base)/gridSide, int(id-base)%gridSide
		d := 0
		if r > dr {
			d += r - dr
		} else {
			d += dr - r
		}
		if c > dc {
			d += c - dc
		} else {
			d += dc - c
		}
		return float64(d) * gridFastCost
	}
}

func BenchmarkWalk_Grid_Dijkstra(b *testing.B) {
	f := gridGraph()
	src, dst := f.ids[0], f.ids[len(f.ids)-1]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.ShortestWeightedPath(src, dst, nil, benchSimilarityGap); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWalk_Grid_AStar is the same query with the estimate, and the pair is
// the whole argument for A*: Dijkstra settles a ball around the source because
// it has no reason to prefer a direction, and a guided search walks at the
// target instead.
func BenchmarkWalk_Grid_AStar(b *testing.B) {
	f := gridGraph()
	src, dst := f.ids[0], f.ids[len(f.ids)-1]
	h := gridManhattan(f.ids, dst)

	// Checked once, outside the timer, and by cost rather than by hop count: an
	// inadmissible heuristic returns a dearer path without complaining, and a
	// benchmark of a wrong answer measures nothing. Comparing hops instead is
	// the mistake this check was written to catch, and did.
	want, err := f.g.ShortestWeightedPath(src, dst, nil, benchSimilarityGap)
	if err != nil {
		b.Fatal(err)
	}
	got, err := f.g.AStarPath(src, dst, nil, benchSimilarityGap, h)
	if err != nil {
		b.Fatal(err)
	}
	if wc, gc := benchPathCost(want), benchPathCost(got); gc != wc {
		b.Fatalf("the heuristic is not admissible on this fixture: A* cost %v, Dijkstra %v", gc, wc)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.AStarPath(src, dst, nil, benchSimilarityGap, h); err != nil {
			b.Fatal(err)
		}
	}
}

func benchPathCost(p *traversal.PathResult) float64 {
	total := 0.0
	for _, e := range p.Edges {
		total += benchSimilarityGap(store.IncidentEdge{Edge: e.ID, Weight: e.Weight})
	}
	return total
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
