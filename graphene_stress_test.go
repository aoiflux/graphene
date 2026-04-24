// Stress tests and benchmarks for the Graphene engine.
// Run stress tests with: go test ./... -tags=stress -race -count=1
// Run benchmarks with:   go test ./... -bench=. -benchtime=5s

//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

const (
	stressNodes = 100_000
	stressEdges = 500_000
)

// --- Large graph insert + BFS ---

func TestStress_LargeInsert(t *testing.T) {
	g := graphene.NewInMemory()

	ids := make([]store.NodeID, stressNodes)
	for i := range ids {
		id, err := g.GraphStore.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeMicroArtefact},
		})
		if err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
		ids[i] = id
	}

	nc, _ := g.GraphStore.NodeCount()
	if nc != stressNodes {
		t.Errorf("NodeCount: %d, want %d", nc, stressNodes)
	}

	// Chain: ids[0]→ids[1]→...→ids[stressEdges] (wraps modulo)
	for i := 0; i < stressEdges; i++ {
		src := ids[i%stressNodes]
		dst := ids[(i+1)%stressNodes]
		if _, err := g.GraphStore.AddEdge(&store.Edge{
			Src:    src,
			Dst:    dst,
			Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
		}); err != nil {
			t.Fatalf("AddEdge %d: %v", i, err)
		}
	}

	ec, _ := g.GraphStore.EdgeCount()
	if ec != stressEdges {
		t.Errorf("EdgeCount: %d, want %d", ec, stressEdges)
	}

	// BFS from first node — verify it visits nodes without crashing.
	res, err := g.BFS(ids[0], 3, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) == 0 {
		t.Error("BFS on large graph returned 0 nodes")
	}
}

// --- Concurrent writes ---

func TestStress_ConcurrentWrites(t *testing.T) {
	g := graphene.NewInMemory()
	const goroutines = 50
	const nodesPerGoroutine = 200

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < nodesPerGoroutine; j++ {
				g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			}
		}()
	}
	wg.Wait()

	nc, _ := g.GraphStore.NodeCount()
	want := goroutines * nodesPerGoroutine
	if int(nc) != want {
		t.Errorf("ConcurrentWrites: NodeCount=%d, want %d", nc, want)
	}
}

// --- Concurrent reads ---

func TestStress_ConcurrentReads(t *testing.T) {
	g := graphene.NewInMemory()
	const n = 1000
	ids := make([]store.NodeID, n)
	for i := range ids {
		ids[i], _ = g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	}
	for i := 0; i < n-1; i++ {
		g.GraphStore.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	}

	const goroutines = 50
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			targetID := ids[gid%n]
			_, err := g.GraphStore.GetNode(targetID)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d: GetNode %d: %w", gid, targetID, err)
				return
			}
			_, err = g.GraphStore.Neighbours(targetID, store.DirectionOutbound, nil)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d: Neighbours: %w", gid, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// --- Property index at scale ---

func TestStress_PropertyIndexScale(t *testing.T) {
	g := graphene.NewInMemory()
	const count = 50_000

	target := []byte("needle")
	var targetIDs []store.NodeID

	for i := 0; i < count; i++ {
		id, _ := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		var hashVal []byte
		if i%100 == 0 {
			hashVal = target
			targetIDs = append(targetIDs, id)
		} else {
			hashVal = []byte(fmt.Sprintf("hash-%d", i))
		}
		g.GraphStore.IndexNodeProperty(id, "sha256", hashVal)
	}

	results, err := g.GraphStore.NodesByProperty("sha256", target)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(targetIDs) {
		t.Errorf("NodesByProperty scale: got %d, want %d", len(results), len(targetIDs))
	}
}

// --- Compact under load (disk store) ---

func TestStress_CompactUnderLoad(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	const writerGoroutines = 10
	const nodesPerWriter = 100

	var wg sync.WaitGroup
	for i := 0; i < writerGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < nodesPerWriter; j++ {
				g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			}
		}()
	}

	// Compact in the middle of writers.
	wg.Wait()
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact under load: %v", err)
	}

	// Close and reopen — data should be consistent.
	g.Close()
	g2, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()

	nc, _ := g2.GraphStore.NodeCount()
	want := writerGoroutines * nodesPerWriter
	if int(nc) != want {
		t.Errorf("after compact+restart: NodeCount=%d, want %d", nc, want)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkAddNode(b *testing.B) {
	g := graphene.NewInMemory()
	node := &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.GraphStore.AddNode(node)
	}
}

func BenchmarkGetNode(b *testing.B) {
	g := graphene.NewInMemory()
	id, _ := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.GraphStore.GetNode(id)
	}
}

func BenchmarkBFS(b *testing.B) {
	g := graphene.NewInMemory()
	const chainLen = 1000
	ids := make([]store.NodeID, chainLen)
	for i := range ids {
		ids[i], _ = g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	}
	for i := 0; i < chainLen-1; i++ {
		g.GraphStore.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.BFS(ids[0], chainLen, store.DirectionOutbound, nil)
	}
}

func BenchmarkShortestPath(b *testing.B) {
	g := graphene.NewInMemory()
	const chainLen = 500
	ids := make([]store.NodeID, chainLen)
	for i := range ids {
		ids[i], _ = g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	}
	for i := 0; i < chainLen-1; i++ {
		g.GraphStore.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.ShortestPath(ids[0], ids[chainLen-1], nil)
	}
}

func BenchmarkPropertyIndexLookup(b *testing.B) {
	g := graphene.NewInMemory()
	const n = 10_000
	for i := 0; i < n; i++ {
		id, _ := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		g.GraphStore.IndexNodeProperty(id, "sha256", []byte(fmt.Sprintf("hash-%d", i)))
	}
	key := []byte("hash-5000")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.GraphStore.NodesByProperty("sha256", key)
	}
}

// --- Persistent, full-capability limit test (disk store) ---

func TestStress_PersistentOneMillionFullCapabilities(t *testing.T) {
	if os.Getenv("GRAPHENE_PERSISTENT_STRESS") != "1" {
		t.Skip("set GRAPHENE_PERSISTENT_STRESS=1 to run this persistent limit test")
	}

	nodeTarget := envInt("GRAPHENE_PERSISTENT_STRESS_NODES", 1_000_000)
	if nodeTarget < 10_000 {
		t.Fatalf("GRAPHENE_PERSISTENT_STRESS_NODES too small: %d (min 10000)", nodeTarget)
	}

	edgeStride := envInt("GRAPHENE_PERSISTENT_STRESS_EDGE_STRIDE", 4)
	if edgeStride < 2 {
		edgeStride = 2
	}
	checkpointLabel := "edge-1"

	dirName := os.Getenv("GRAPHENE_PERSISTENT_STRESS_DIR")
	if dirName == "" {
		dirName = "graphene_stress_data_1m"
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	dir := filepath.Join(cwd, dirName)
	if _, err := os.Stat(dir); err == nil {
		t.Fatalf("persistent dir already exists: %s (delete it manually or set GRAPHENE_PERSISTENT_STRESS_DIR)", dir)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", dir, err)
	}

	t.Logf("persistent stress dir: %s", dir)
	t.Logf("target nodes=%d, edgeStride=%d", nodeTarget, edgeStride)

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	caseID, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}

	const fileCount = 1_000
	fileIDs := make([]store.NodeID, fileCount)
	for i := 0; i < fileCount; i++ {
		fid, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		if err != nil {
			t.Fatalf("AddNode file %d: %v", i, err)
		}
		fileIDs[i] = fid
		if _, err := g.AddEdge(&store.Edge{Src: fid, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}}); err != nil {
			t.Fatalf("AddEdge BelongsTo file %d: %v", i, err)
		}
	}

	patternA, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	patternB, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	patternC, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if _, err := g.AddEdge(&store.Edge{Src: patternA, Dst: patternB, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err != nil {
		t.Fatalf("AddEdge pattern A->B: %v", err)
	}
	if _, err := g.AddEdge(&store.Edge{Src: patternB, Dst: patternC, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err != nil {
		t.Fatalf("AddEdge pattern B->C: %v", err)
	}
	if _, err := g.AddEdge(&store.Edge{Src: patternC, Dst: patternA, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}}); err != nil {
		t.Fatalf("AddEdge pattern C->A: %v", err)
	}

	ids := make([]store.NodeID, nodeTarget)
	for i := 0; i < nodeTarget; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatalf("AddNode artefact %d: %v", i, err)
		}
		ids[i] = id

		if err := g.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("bucket-%03d", i%1000))); err != nil {
			t.Fatalf("IndexNodeProperty bucket %d: %v", i, err)
		}
		if i%100_000 == 0 {
			if err := g.IndexNodeProperty(id, "needle", []byte("true")); err != nil {
				t.Fatalf("IndexNodeProperty needle %d: %v", i, err)
			}
		}

		fileID := fileIDs[i%fileCount]
		if _, err := g.AddEdge(&store.Edge{Src: fileID, Dst: id, Labels: []store.EdgeType{store.EdgeTypeContains}}); err != nil {
			t.Fatalf("AddEdge contains %d: %v", i, err)
		}
		if i > 0 {
			eid, err := g.AddEdge(&store.Edge{Src: ids[i-1], Dst: id, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9})
			if err != nil {
				t.Fatalf("AddEdge similar %d: %v", i, err)
			}
			if i == 1 || i%100_000 == 0 {
				checkpointLabel = fmt.Sprintf("edge-%d", i)
				if err := g.IndexEdgeProperty(eid, "checkpoint", []byte(checkpointLabel)); err != nil {
					t.Fatalf("IndexEdgeProperty checkpoint %d: %v", i, err)
				}
			}
		}
		if i > edgeStride && i%edgeStride == 0 {
			if _, err := g.AddEdge(&store.Edge{Src: ids[i-edgeStride], Dst: id, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err != nil {
				t.Fatalf("AddEdge reuse %d: %v", i, err)
			}
		}

		if i > 0 && i%100_000 == 0 {
			t.Logf("ingested %d/%d artefacts", i, nodeTarget)
		}
	}

	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close after compact: %v", err)
	}

	g2, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()

	nc, err := g2.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	wantNodes := uint64(1 + fileCount + 3 + nodeTarget)
	if nc != wantNodes {
		t.Fatalf("NodeCount=%d, want %d", nc, wantNodes)
	}

	ec, err := g2.EdgeCount()
	if err != nil {
		t.Fatalf("EdgeCount: %v", err)
	}
	wantEdges := uint64(fileCount + 3 + nodeTarget + (nodeTarget - 1) + (nodeTarget / edgeStride))
	if ec < wantEdges-2 || ec > wantEdges+2 {
		t.Fatalf("EdgeCount=%d, expected around %d", ec, wantEdges)
	}

	microIDs, err := g2.NodesByType(store.NodeTypeMicroArtefact)
	if err != nil {
		t.Fatalf("NodesByType(MicroArtefact): %v", err)
	}
	if len(microIDs) != nodeTarget+3 {
		t.Fatalf("NodesByType(MicroArtefact)=%d, want %d", len(microIDs), nodeTarget+3)
	}

	containsIDs, err := g2.EdgesByType(store.EdgeTypeContains)
	if err != nil {
		t.Fatalf("EdgesByType(Contains): %v", err)
	}
	if len(containsIDs) != nodeTarget {
		t.Fatalf("EdgesByType(Contains)=%d, want %d", len(containsIDs), nodeTarget)
	}

	bucketHits, err := g2.NodesByProperty("bucket", []byte("bucket-042"))
	if err != nil {
		t.Fatalf("NodesByProperty(bucket): %v", err)
	}
	if len(bucketHits) == 0 {
		t.Fatal("NodesByProperty(bucket-042) returned 0")
	}

	needleHits, err := g2.NodesByProperty("needle", []byte("true"))
	if err != nil {
		t.Fatalf("NodesByProperty(needle): %v", err)
	}
	if len(needleHits) == 0 {
		t.Fatal("NodesByProperty(needle=true) returned 0")
	}

	checkpointHits, err := g2.EdgesByProperty("checkpoint", []byte(checkpointLabel))
	if err != nil {
		t.Fatalf("EdgesByProperty(checkpoint): %v", err)
	}
	if len(checkpointHits) == 0 {
		t.Fatalf("EdgesByProperty(checkpoint=%s) returned 0", checkpointLabel)
	}

	origin := ids[nodeTarget/2]
	bfs, err := g2.BFS(origin, 3, store.DirectionBoth, nil)
	if err != nil {
		t.Fatalf("BFS: %v", err)
	}
	if len(bfs.Nodes) < 5 {
		t.Fatalf("BFS nodes=%d, want >= 5", len(bfs.Nodes))
	}

	dfs, err := g2.DFS(origin, 3, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatalf("DFS: %v", err)
	}
	if len(dfs.Nodes) < 3 {
		t.Fatalf("DFS nodes=%d, want >= 3", len(dfs.Nodes))
	}

	chain, err := g2.ProvenanceChain(origin, 8, []store.EdgeType{store.EdgeTypeContains})
	if err != nil {
		t.Fatalf("ProvenanceChain: %v", err)
	}
	if len(chain.Chain) < 2 {
		t.Fatalf("ProvenanceChain nodes=%d, want >= 2", len(chain.Chain))
	}

	sp, err := g2.ShortestPath(ids[nodeTarget/4], ids[nodeTarget/4+2048], nil)
	if err != nil {
		t.Fatalf("ShortestPath: %v", err)
	}
	if len(sp.Nodes) == 0 {
		t.Fatal("ShortestPath returned empty path")
	}

	deg, err := g2.Degree(origin, nil)
	if err != nil {
		t.Fatalf("Degree: %v", err)
	}
	if deg <= 0 {
		t.Fatalf("Degree=%d, want > 0", deg)
	}

	exists, err := g2.EdgeExists(ids[nodeTarget/3], ids[nodeTarget/3+1], []store.EdgeType{store.EdgeTypeSimilarTo})
	if err != nil {
		t.Fatalf("EdgeExists: %v", err)
	}
	if !exists {
		t.Fatal("expected SimilarTo edge to exist between adjacent artefact nodes")
	}

	subNodes := []store.NodeID{ids[nodeTarget/5], ids[nodeTarget/5+1], ids[nodeTarget/5+2], ids[nodeTarget/5+3], ids[nodeTarget/5+4]}
	_, subEdges, err := g2.InducedSubgraph(subNodes)
	if err != nil {
		t.Fatalf("InducedSubgraph: %v", err)
	}
	if len(subEdges) == 0 {
		t.Fatal("InducedSubgraph returned 0 edges")
	}

	pattern := &traversal.Pattern{
		Nodes: []traversal.PatternNode{
			{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		},
		Edges: []traversal.PatternEdge{
			{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeReuse}},
			{SrcPatternID: 1, DstPatternID: 2, Labels: []store.EdgeType{store.EdgeTypeReuse}},
			{SrcPatternID: 2, DstPatternID: 0, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
		},
	}
	matches, err := g2.FindPatterns(pattern, []store.NodeID{patternA, patternB, patternC}, 10)
	if err != nil {
		t.Fatalf("FindPatterns: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("FindPatterns returned 0 matches for known triangle")
	}

	t.Logf("persistent limit test complete: dir=%s nodes=%d edges=%d", dir, nc, ec)
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
