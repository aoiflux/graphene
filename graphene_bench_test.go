// Read-path benchmarks for the Graphene engine.
//
// Run with:
//
//	go test . -tags=stress -bench=. -benchmem -benchtime=1s -run=^$
//
// The fixtures are built once per process and shared across benchmarks; graph
// construction is never inside the timed region.

//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// benchNodeCount is the fixture size for the read benchmarks. Override with
// GRAPHENE_BENCH_NODES to sweep scale.
var benchNodeCount = envIntDefault("GRAPHENE_BENCH_NODES", 100_000)

// Fixture label distribution (for benchNodeCount = 100k):
//
//	NodeTypeCase          — every 1000th node   (100 nodes, highly selective)
//	NodeTypeEvidenceFile  — every 10th node     (9 900 nodes)
//	NodeTypeMicroArtefact — the rest            (90 000 nodes)
//
// Indexed properties per node:
//
//	"sha256" — unique per node        (high cardinality, 1 hit)
//	"bucket" — 1000 distinct values   (medium cardinality, ~100 hits)
//	"score"  — 1000 distinct numerics (range/prefix queries)
type benchFixture struct {
	g       *graphene.Graph
	ids     []store.NodeID
	hub     store.NodeID // node with a large inbound degree
	midNode store.NodeID
}

var (
	memFixtureOnce  sync.Once
	memFixture      *benchFixture
	diskFixtureOnce sync.Once
	diskFixture     *benchFixture
	diskFixtureDir  string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if diskFixtureDir != "" {
		os.RemoveAll(diskFixtureDir)
	}
	if orderedDskDir != "" {
		os.RemoveAll(orderedDskDir)
	}
	for _, d := range append(blobDskDirs, plannerDir) {
		os.RemoveAll(d)
	}
	os.Exit(code)
}

func envIntDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return def
	}
	return n
}

func benchLabel(i int) store.NodeType {
	switch {
	case i%1000 == 0:
		return store.NodeTypeCase
	case i%10 == 0:
		return store.NodeTypeEvidenceFile
	default:
		return store.NodeTypeMicroArtefact
	}
}

// buildFixture populates g with the standard benchmark graph.
func buildFixture(g *graphene.Graph, n int) *benchFixture {
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabel(i)}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		panic(fmt.Sprintf("bench fixture: AddNodes: %v", err))
	}

	for i, id := range ids {
		err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%07d", i)),
			"bucket": []byte(fmt.Sprintf("bucket-%04d", i%1000)),
			"score":  []byte(fmt.Sprintf("%06d", i%1000)),
		})
		if err != nil {
			panic(fmt.Sprintf("bench fixture: IndexNodeProperties: %v", err))
		}
	}

	// Topology: a chain plus a +13 stride (branching factor 2, so a 3-hop BFS
	// touches a handful of nodes rather than the whole graph), plus one hub node
	// that collects 1000 inbound edges to exercise anchored relation queries.
	hub := ids[n/2]
	edges := make([]*store.Edge, 0, 2*n+1000)
	for i := 0; i < n-1; i++ {
		edges = append(edges, &store.Edge{
			Src:    ids[i],
			Dst:    ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
			Weight: 0.5,
		})
	}
	for i := 0; i < n; i++ {
		edges = append(edges, &store.Edge{
			Src:    ids[i],
			Dst:    ids[(i+13)%n],
			Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
			Weight: 0.9,
		})
	}
	for i := 0; i < 1000; i++ {
		edges = append(edges, &store.Edge{
			Src:    ids[(i*37)%n],
			Dst:    hub,
			Labels: []store.EdgeType{store.EdgeTypeBelongsTo},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		panic(fmt.Sprintf("bench fixture: AddEdges: %v", err))
	}

	return &benchFixture{g: g, ids: ids, hub: hub, midNode: ids[n/3]}
}

func memGraph() *benchFixture {
	memFixtureOnce.Do(func() {
		memFixture = buildFixture(graphene.NewInMemory(), benchNodeCount)
	})
	return memFixture
}

func diskGraph() *benchFixture {
	diskFixtureOnce.Do(func() {
		dir, err := os.MkdirTemp("", "graphene-bench-*")
		if err != nil {
			panic(err)
		}
		diskFixtureDir = dir
		g, err := graphene.Open(dir)
		if err != nil {
			panic(err)
		}
		diskFixture = buildFixture(g, benchNodeCount)
		// Compact so reads exercise the CSR path, which is the shipping
		// read-optimised representation.
		if err := g.Compact(); err != nil {
			panic(err)
		}
	})
	return diskFixture
}

// =============================================================================
// Primary index — point lookups
// =============================================================================

func BenchmarkPointLookupNode_Memory(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.GetNode(f.ids[i%len(f.ids)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPointLookupNode_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.GetNode(f.ids[i%len(f.ids)]); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Label / type index
// =============================================================================

// NodesByType on the most selective label (100 of 100 000 nodes). A working
// label index should make this proportional to the result size, not the graph.
func BenchmarkNodesByType_Selective_Memory(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.NodesByType(store.NodeTypeCase); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNodesByType_Selective_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.NodesByType(store.NodeTypeCase); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Query planner — type pre-filter with a small window
// =============================================================================

// Ten rows off a highly selective label. Cost should track the window, not the
// graph size.
func BenchmarkQueryNodes_TypeLimit10_Memory(b *testing.B) {
	f := memGraph()
	q := store.NodeQuery{Types: []store.NodeType{store.NodeTypeCase}, Limit: 10}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_TypeLimit10_Disk(b *testing.B) {
	f := diskGraph()
	q := store.NodeQuery{Types: []store.NodeType{store.NodeTypeCase}, Limit: 10}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Property index — equality (index-accelerated) vs. non-equality (scan)
// =============================================================================

// Raw single-key equality straight through the property index.
func BenchmarkNodesByProperty_Equal_Memory(b *testing.B) {
	f := memGraph()
	val := []byte(fmt.Sprintf("hash-%07d", benchNodeCount/2))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.NodesByProperty("sha256", val); err != nil {
			b.Fatal(err)
		}
	}
}

// The same equality lookup routed through QueryNodeIDs. The gap between this and
// the benchmark above is the query-path overhead.
func BenchmarkQueryNodes_PropertyEqual_Memory(b *testing.B) {
	f := memGraph()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte(fmt.Sprintf("hash-%07d", benchNodeCount/2))},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyEqual_Disk(b *testing.B) {
	f := diskGraph()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte(fmt.Sprintf("hash-%07d", benchNodeCount/2))},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// Medium-cardinality equality (~100 hits) combined with a type pre-filter.
func BenchmarkQueryNodes_TypeAndPropertyEqual_Memory(b *testing.B) {
	f := memGraph()
	q := store.NodeQuery{
		Types:   []store.NodeType{store.NodeTypeMicroArtefact},
		Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-0042")}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyPrefix_Memory(b *testing.B) {
	f := memGraph()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-00")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyRange_Memory(b *testing.B) {
	f := memGraph()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("000100"), ValueUpper: []byte("000200")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyRange_Disk(b *testing.B) {
	f := diskGraph()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("000100"), ValueUpper: []byte("000200")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Ordered (range) index ---
//
// The fixture's "score" and "bucket" values are zero-padded fixed-width strings,
// so byte order already matches their intended order and declaring them ordered
// does not change any result — only how the range is found.

var (
	orderedMemOnce  sync.Once
	orderedMemGraph *benchFixture
	orderedDskOnce  sync.Once
	orderedDskGraph *benchFixture
	orderedDskDir   string
)

func orderedMemory() *benchFixture {
	orderedMemOnce.Do(func() {
		f := buildFixture(graphene.NewInMemory(), benchNodeCount)
		declareOrdered(f.g)
		orderedMemGraph = f
	})
	return orderedMemGraph
}

func orderedDisk() *benchFixture {
	orderedDskOnce.Do(func() {
		dir, err := os.MkdirTemp("", "graphene-ordered-bench-*")
		if err != nil {
			panic(err)
		}
		orderedDskDir = dir
		g, err := graphene.Open(dir)
		if err != nil {
			panic(err)
		}
		f := buildFixture(g, benchNodeCount)
		if err := g.Compact(); err != nil {
			panic(err)
		}
		declareOrdered(g)
		orderedDskGraph = f
	})
	return orderedDskGraph
}

func declareOrdered(g *graphene.Graph) {
	for _, k := range []string{"score", "bucket"} {
		if err := g.DeclareOrderedProperty(k); err != nil {
			panic(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyRange_Ordered_Memory(b *testing.B) {
	f := orderedMemory()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("000100"), ValueUpper: []byte("000200")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyRange_Ordered_Disk(b *testing.B) {
	f := orderedDisk()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("000100"), ValueUpper: []byte("000200")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyPrefix_Ordered_Memory(b *testing.B) {
	f := orderedMemory()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-00")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// A narrow range — the shape an ordered index helps most, where a scan still
// visits every entry under the key but a binary search visits almost none.
func BenchmarkQueryNodes_PropertyRange_Narrow_Ordered_Memory(b *testing.B) {
	f := orderedMemory()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("000500"), ValueUpper: []byte("000502")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNodes_PropertyRange_Narrow_Scan_Memory(b *testing.B) {
	f := memGraph()
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("000500"), ValueUpper: []byte("000502")},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// Declaring an ordered key over an already-populated index: the one-off build cost.
func BenchmarkDeclareOrderedProperty(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		g := graphene.NewInMemory()
		buildFixture(g, 20_000)
		b.StartTimer()
		if err := g.DeclareOrderedProperty("score"); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Adjacency — anchored relation queries and neighbourhood reads
// =============================================================================

// Relations anchored on a single node. This should be O(degree); today it is
// routed through QueryEdgeIDs, which enumerates every edge in the graph.
func BenchmarkQueryRelations_Anchored_Memory(b *testing.B) {
	f := memGraph()
	q := store.RelationQuery{Anchors: []store.NodeID{f.hub}, Direction: store.DirectionInbound}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryRelationIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryRelations_Anchored_Disk(b *testing.B) {
	f := diskGraph()
	q := store.RelationQuery{Anchors: []store.NodeID{f.hub}, Direction: store.DirectionInbound}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryRelationIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNeighbours1Hop_Memory(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.Neighbours(f.midNode, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNeighbours1Hop_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.GraphStore.Neighbours(f.midNode, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBFS3Hop_Memory(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.BFS(f.midNode, 3, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBFS3Hop_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.BFS(f.midNode, 3, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// Degree on the hub node. The CSR can answer this from its offset arrays without
// materialising a single edge.
func BenchmarkDegree_Hub_Memory(b *testing.B) {
	f := memGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.InDegree(f.hub, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDegree_Hub_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.InDegree(f.hub, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Write-side cost of the indexes
// =============================================================================

// DeleteNode has to purge the node's property-index entries. Without a reverse
// ID -> (key, value) map that purge walks the entire index.
func BenchmarkDeleteNode_WithPropertyIndex(b *testing.B) {
	const fixtureNodes = 20_000
	g := graphene.NewInMemory()
	ids := make([]store.NodeID, 0, fixtureNodes)
	for i := 0; i < fixtureNodes; i++ {
		id, err := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{benchLabel(i)}})
		if err != nil {
			b.Fatal(err)
		}
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%07d", i)),
			"bucket": []byte(fmt.Sprintf("bucket-%04d", i%1000)),
		}); err != nil {
			b.Fatal(err)
		}
		ids = append(ids, id)
	}

	cursor := 0
	round := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cursor >= len(ids) {
			// Refill so long runs keep measuring deletes against a populated index.
			b.StopTimer()
			round++
			ids = ids[:0]
			for j := 0; j < fixtureNodes; j++ {
				id, err := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{benchLabel(j)}})
				if err != nil {
					b.Fatal(err)
				}
				if err := g.IndexNodeProperties(id, map[string][]byte{
					"sha256": []byte(fmt.Sprintf("hash-r%d-%07d", round, j)),
					"bucket": []byte(fmt.Sprintf("bucket-%04d", j%1000)),
				}); err != nil {
					b.Fatal(err)
				}
				ids = append(ids, id)
			}
			cursor = 0
			b.StartTimer()
		}
		if err := g.GraphStore.DeleteNode(ids[cursor]); err != nil {
			b.Fatal(err)
		}
		cursor++
	}
}

// =============================================================================
// Traversal — allocation is the metric that matters here
// =============================================================================

// buildChain returns a graph that is one long path, so a depth-N walk visits
// exactly N nodes. This isolates per-visited-node cost.
func buildChain(b *testing.B, n int) (*graphene.Graph, []store.NodeID) {
	b.Helper()
	g := graphene.NewInMemory()
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatal(err)
	}
	edges := make([]*store.Edge, 0, n-1)
	for i := 0; i < n-1; i++ {
		edges = append(edges, &store.Edge{
			Src: ids[i], Dst: ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		b.Fatal(err)
	}
	return g, ids
}

// buildFanout returns a shallow, wide graph: one root with `width` children,
// each of which has `width` children. A depth-2 walk visits width² nodes.
func buildFanout(b *testing.B, width int) (*graphene.Graph, store.NodeID) {
	b.Helper()
	g := graphene.NewInMemory()
	total := 1 + width + width*width
	nodes := make([]*store.Node, total)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatal(err)
	}
	edges := make([]*store.Edge, 0, width+width*width)
	for i := 0; i < width; i++ {
		edges = append(edges, &store.Edge{
			Src: ids[0], Dst: ids[1+i],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	for i := 0; i < width; i++ {
		for j := 0; j < width; j++ {
			edges = append(edges, &store.Edge{
				Src: ids[1+i], Dst: ids[1+width+i*width+j],
				Labels: []store.EdgeType{store.EdgeTypeContains},
			})
		}
	}
	if _, err := g.AddEdges(edges); err != nil {
		b.Fatal(err)
	}
	return g, ids[0]
}

// Deep walk: 10 000 nodes in a single chain.
func BenchmarkBFS_Deep(b *testing.B) {
	g, ids := buildChain(b, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.BFS(ids[0], 10_000, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// The same walk, IDs only — no node or edge record is ever built.
func BenchmarkBFSIDs_Deep(b *testing.B) {
	g, ids := buildChain(b, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.BFSIDs(ids[0], 10_000, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// Wide walk: 100 × 100 fan-out, 10 101 nodes reached in two hops.
func BenchmarkBFS_Wide(b *testing.B) {
	g, root := buildFanout(b, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.BFS(root, 2, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBFSIDs_Wide(b *testing.B) {
	g, root := buildFanout(b, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.BFSIDs(root, 2, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// Traversal over the shared 100k-node disk fixture, where every CSR edge that is
// materialised costs a record allocation plus a property-blob clone.
func BenchmarkBFS_Disk_Deep(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.BFS(f.midNode, 12, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBFSIDs_Disk_Deep(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.BFSIDs(f.midNode, 12, store.DirectionOutbound, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Durability — restart and compaction cost
// =============================================================================

// buildDurabilityFixture writes a disk store with n nodes and 2n indexed
// property entries, compacts it, and returns the directory. The caller owns the
// directory and must remove it.
func buildDurabilityFixture(b *testing.B, n int) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "graphene-durability-*")
	if err != nil {
		b.Fatal(err)
	}
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabel(i)}}
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
	edges := make([]*store.Edge, 0, n-1)
	for i := 0; i < n-1; i++ {
		edges = append(edges, &store.Edge{
			Src:    ids[i],
			Dst:    ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		b.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		b.Fatal(err)
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}
	return dir
}

// Reopening a compacted store. Before the property index was persisted in the
// CSR, this replayed every indexed entry from the WAL, so the cost grew with the
// total number of entries no matter how little had changed since.
func BenchmarkReopenCompactedStore(b *testing.B) {
	dir := buildDurabilityFixture(b, 50_000)
	defer os.RemoveAll(dir)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// Compacting an already-compacted store with nothing pending. This isolates the
// fixed cost of a compaction: rebuild the CSR, write it, and (previously)
// re-emit the entire property index into the fresh WAL.
func BenchmarkCompactSteadyState(b *testing.B) {
	dir := buildDurabilityFixture(b, 50_000)
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.Compact(); err != nil {
			b.Fatal(err)
		}
	}
}

// Full index verification over a compacted store — the cost of the integrity
// check that now runs on every Open.
func BenchmarkVerifyIndexes_Disk(b *testing.B) {
	f := diskGraph()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.g.VerifyIndexes(); err != nil {
			b.Fatal(err)
		}
	}
}

// Deleting from a label with many members. Label postings are the last index
// still stored unsorted, so removal rewrites the whole list: one delete from a
// 50 000-member label costs 50 000 comparisons.
//
// No property index here, so label-postings maintenance is the only cost in
// frame.
func benchmarkDeleteFromHotLabel(b *testing.B, members int) {
	g := graphene.NewInMemory()
	fill := func() []store.NodeID {
		nodes := make([]*store.Node, members)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			b.Fatal(err)
		}
		return ids
	}

	ids := fill()
	cursor := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cursor >= len(ids) {
			b.StopTimer()
			ids = fill()
			cursor = 0
			b.StartTimer()
		}
		if err := g.GraphStore.DeleteNode(ids[cursor]); err != nil {
			b.Fatal(err)
		}
		cursor++
	}
}

func BenchmarkDeleteNode_HotLabel_10k(b *testing.B) { benchmarkDeleteFromHotLabel(b, 10_000) }
func BenchmarkDeleteNode_HotLabel_50k(b *testing.B) { benchmarkDeleteFromHotLabel(b, 50_000) }

// Relabelling also reconciles the postings: remove from every old label, append
// to every new one.
func BenchmarkUpdateNode_HotLabel_50k(b *testing.B) {
	const members = 50_000
	g := graphene.NewInMemory()
	nodes := make([]*store.Node, members)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Alternate the label so each update genuinely moves the node between
		// two large postings lists.
		lbl := store.NodeTypeMicroArtefact
		if i%2 == 1 {
			lbl = store.NodeTypeEvidenceFile
		}
		if err := g.GraphStore.UpdateNode(&store.Node{
			ID:     ids[i%len(ids)],
			Labels: []store.NodeType{lbl},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// Indexing throughput: the per-entry cost paid on the write path.
func BenchmarkIndexNodeProperty(b *testing.B) {
	g := graphene.NewInMemory()
	id, err := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.GraphStore.IndexNodeProperty(id, "sha256", []byte(fmt.Sprintf("hash-%07d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Residual filter evaluation
// =============================================================================
//
// A selective driver paired with a filter no index can serve. Resolving the
// second filter to its own set costs a scan of every entry under its key; testing
// the handful of candidates against it costs a lookup each. These measure the gap.

func BenchmarkQueryNodes_EqualityPlusContains_Memory(b *testing.B) {
	benchmarkResidual(b, memGraph())
}

func BenchmarkQueryNodes_EqualityPlusContains_Disk(b *testing.B) {
	benchmarkResidual(b, diskGraph())
}

func benchmarkResidual(b *testing.B, f *benchFixture) {
	q := store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "sha256", Op: store.PropertyOpEqual,
				Value: []byte(fmt.Sprintf("hash-%07d", benchNodeCount/2))},
			{Key: "bucket", Op: store.PropertyOpContains, Value: []byte("bucket")},
		},
		FilterMode: store.MatchAll,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// Two equality filters of very different selectivity: the residual is served
// from postings either way, so this is the case where probing must not lose.
func BenchmarkQueryNodes_TwoEqualities_Memory(b *testing.B) {
	f := memGraph()
	q := store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "sha256", Op: store.PropertyOpEqual,
				Value: []byte(fmt.Sprintf("hash-%07d", benchNodeCount/2))},
			{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-0500")},
		},
		FilterMode: store.MatchAll,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// The edge equivalent of the residual case: a selective driver plus a filter no
// index can serve.
//
// This builds its own fixture rather than extending the shared one. Adding edge
// properties to the shared graph would change the memory profile of every other
// benchmark that uses it, which would silently invalidate their comparison
// against the published baseline.
var (
	edgePropOnce  sync.Once
	edgePropGraph *graphene.Graph
)

const edgePropCount = 20_000

func edgePropFixture() *graphene.Graph {
	edgePropOnce.Do(func() {
		g := graphene.NewInMemory()
		nodes := make([]*store.Node, edgePropCount+1)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeTag}}
		}
		nodeIDs, err := g.AddNodes(nodes)
		if err != nil {
			panic(err)
		}
		edges := make([]*store.Edge, edgePropCount)
		for i := range edges {
			edges[i] = &store.Edge{Src: nodeIDs[i], Dst: nodeIDs[i+1],
				Labels: []store.EdgeType{store.EdgeTypeContains}}
		}
		ids, err := g.AddEdges(edges)
		if err != nil {
			panic(err)
		}
		for i, id := range ids {
			if err := g.IndexEdgeProperties(id, map[string][]byte{
				"edge_sha":  []byte(fmt.Sprintf("esha-%07d", i)),
				"edge_kind": []byte(fmt.Sprintf("kind-%d", i%8)),
			}); err != nil {
				panic(err)
			}
		}
		edgePropGraph = g
	})
	return edgePropGraph
}

func BenchmarkQueryEdges_EqualityPlusContains_Memory(b *testing.B) {
	g := edgePropFixture()
	q := store.EdgeQuery{
		Filters: []store.PropertyFilter{
			{Key: "edge_sha", Op: store.PropertyOpEqual,
				Value: []byte(fmt.Sprintf("esha-%07d", edgePropCount/2))},
			{Key: "edge_kind", Op: store.PropertyOpContains, Value: []byte("kind")},
		},
		FilterMode: store.MatchAll,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryEdgeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// Where does reopen time actually go?
// =============================================================================
//
// Open eagerly materialises the whole graph: it reads the CSR file, parses every
// node and edge record, and inserts every property entry into the index. Making
// it faster means knowing which of those dominates, and the honest way to find
// out is to vary one while holding the others fixed.
//
// These build the same node and edge counts every time and change only how many
// property entries each node carries. The slope across them is the per-entry
// index cost; the intercept is the CSR parse plus the fixed overhead.

// buildReopenFixture is buildDurabilityFixture with a controllable number of
// indexed properties per node.
func buildReopenFixture(b *testing.B, n, propsPerNode int) string {
	b.Helper()
	dir, err := os.MkdirTemp("", "graphene-reopen-*")
	if err != nil {
		b.Fatal(err)
	}
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabel(i)}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatal(err)
	}
	keys := []string{"sha256", "bucket", "tool", "stage", "owner", "phase", "zone", "tier"}
	if propsPerNode > len(keys) {
		b.Fatalf("propsPerNode %d exceeds %d available keys", propsPerNode, len(keys))
	}
	for i, id := range ids {
		props := make(map[string][]byte, propsPerNode)
		for k := 0; k < propsPerNode; k++ {
			// Key 0 is unique per node; the rest are low-cardinality, matching
			// the shape of a real key mix rather than a best or worst case.
			if k == 0 {
				props[keys[k]] = []byte(fmt.Sprintf("hash-%07d", i))
			} else {
				props[keys[k]] = []byte(fmt.Sprintf("%s-%04d", keys[k], i%1000))
			}
		}
		if len(props) > 0 {
			if err := g.IndexNodeProperties(id, props); err != nil {
				b.Fatal(err)
			}
		}
	}
	edges := make([]*store.Edge, 0, n-1)
	for i := 0; i < n-1; i++ {
		edges = append(edges, &store.Edge{
			Src: ids[i], Dst: ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		b.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		b.Fatal(err)
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}
	return dir
}

func benchmarkReopenWithProps(b *testing.B, propsPerNode int) {
	dir := buildReopenFixture(b, 50_000, propsPerNode)
	defer os.RemoveAll(dir)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// The intercept: no property entries at all, so this is CSR parse + WAL + fixed
// cost, with the index doing nothing.
func BenchmarkReopen_Props0(b *testing.B) { benchmarkReopenWithProps(b, 0) }
func BenchmarkReopen_Props1(b *testing.B) { benchmarkReopenWithProps(b, 1) }
func BenchmarkReopen_Props2(b *testing.B) { benchmarkReopenWithProps(b, 2) }
func BenchmarkReopen_Props4(b *testing.B) { benchmarkReopenWithProps(b, 4) }
func BenchmarkReopen_Props8(b *testing.B) { benchmarkReopenWithProps(b, 8) }
