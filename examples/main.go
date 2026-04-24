// Package examples demonstrates common Graphene usage patterns for the
// SYNTHRA forensic platform.
//
// Run with: go run ./examples
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
	"github.com/aoiflux/graphene/viz"
)

func main() {
	fmt.Println("=== Graphene Examples ===")
	fmt.Println()

	fmt.Println("--- In-Memory Examples ---")
	fmt.Println()
	example1_SingleFileArtefacts()
	example2_ArtefactSimilarityCluster()
	example3_MultiFileCaseGraph()
	example4_PropertyIndexLookup()
	example5_ProvenanceChain()
	example6_ShortestPath()
	example7_PatternMatching()

	fmt.Println("--- On-Disk Examples ---")
	fmt.Println()
	example8_DiskPersistence()
	example9_DiskCompactAndReload()
	example10_DiskPropertyIndexSurvivesRestart()
	example11_DiskBulkIngestWithCompaction()

	fmt.Println("--- Helper & Utility Examples ---")
	fmt.Println()
	example12_BatchWritesAndStats()
	example13_MultiKeyPropertyQuery()
	example14_DegreeAndConnectivity()
	example15_InducedSubgraph()
	example16_CycleDetection()
	example17_ResultHelpers()

	fmt.Println("--- Limit Showcase (Opt-In) ---")
	fmt.Println()
	if os.Getenv("GRAPHENE_RUN_LIMIT_EXAMPLE") == "1" {
		example18_PersistentExtremeScaleShowcase()
	} else {
		fmt.Println("  Skipped (set GRAPHENE_RUN_LIMIT_EXAMPLE=1 to execute).")
		fmt.Println()
	}

	fmt.Println("--- Visualization Examples ---")
	fmt.Println()
	example19_VisualizationCaseMap()
	example20_VisualizationSimilarityMesh()
	example21_VisualizationPatternScope()
}

// ----------------------------------------------------------------------------
// Example 1 — A single evidence file containing micro-artefacts
// ----------------------------------------------------------------------------
//
// Graph shape:
//
//	Case ← BelongsTo ← EvidenceFile → Contains → MicroArtefact (x3)
func example1_SingleFileArtefacts() {
	fmt.Println("--- Example 1: Single evidence file with micro-artefacts ---")

	g := graphene.NewInMemory()

	caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	art1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art3, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeContains}})

	// Index artefact hashes for fast lookup.
	g.IndexNodeProperty(art1, "sha256", []byte("aabbcc"))
	g.IndexNodeProperty(art2, "sha256", []byte("ddeeff"))
	g.IndexNodeProperty(art3, "sha256", []byte("112233"))

	nc, _ := g.NodeCount()
	ec, _ := g.EdgeCount()
	fmt.Printf("  Nodes: %d  Edges: %d\n", nc, ec)

	// Walk outbound from the file — should reach all 4 neighbours.
	result, _ := g.BFS(fileID, 1, store.DirectionOutbound, nil)
	fmt.Printf("  1-hop outbound from EvidenceFile: %d nodes\n", len(result.Nodes))

	// Confirm artefacts are findable by hash.
	hits, _ := g.NodesByProperty("sha256", []byte("ddeeff"))
	fmt.Printf("  NodesByProperty(sha256=ddeeff): node %d\n", hits[0])

	_ = caseID
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 2 — Similarity cluster: artefacts linked by similarity / reuse
// ----------------------------------------------------------------------------
//
// Graph shape:
//
//	art1 ←SimilarTo→ art2 ←SimilarTo→ art3
//	art1 ──Reuse──→ art3
func example2_ArtefactSimilarityCluster() {
	fmt.Println("--- Example 2: Artefact similarity cluster ---")

	g := graphene.NewInMemory()

	art1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art3, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: art1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.92})
	g.AddEdge(&store.Edge{Src: art2, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.92})
	g.AddEdge(&store.Edge{Src: art2, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.87})
	g.AddEdge(&store.Edge{Src: art3, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.87})
	g.AddEdge(&store.Edge{Src: art1, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	// BFS from art1 — both direction, only SimilarTo edges.
	result, _ := g.BFS(art1, 2, store.DirectionBoth, []store.EdgeType{store.EdgeTypeSimilarTo})
	fmt.Printf("  Artefacts reachable from art1 via SimilarTo (depth 2): %d\n", len(result.Nodes))

	// All artefacts reachable from art1 following any edge.
	result, _ = g.BFS(art1, 2, store.DirectionBoth, nil)
	fmt.Printf("  Artefacts reachable from art1 via any edge (depth 2): %d\n", len(result.Nodes))

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 3 — Multi-file case: two evidence files sharing artefacts
// ----------------------------------------------------------------------------
//
// Graph shape:
//
//	Case ← BelongsTo ← file1 → Contains → art1, art2
//	Case ← BelongsTo ← file2 → Contains → art2, art3
//	art1 ←SimilarTo→ art3  (cross-file similarity)
func example3_MultiFileCaseGraph() {
	fmt.Println("--- Example 3: Multi-file case with shared artefacts ---")

	g := graphene.NewInMemory()

	caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	file1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	file2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	art1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art3, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: file1, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
	g.AddEdge(&store.Edge{Src: file2, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
	g.AddEdge(&store.Edge{Src: file1, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: file1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: file2, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: file2, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: art1, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.95})
	g.AddEdge(&store.Edge{Src: art3, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.95})

	// All nodes within 3 hops of the case node.
	result, _ := g.BFS(caseID, 3, store.DirectionBoth, nil)
	fmt.Printf("  Nodes within 3 hops of Case: %d\n", len(result.Nodes))

	// art2 appears in both files — find its provenance files.
	neighbours, _ := g.Neighbours(art2, store.DirectionInbound, []store.EdgeType{store.EdgeTypeContains})
	fmt.Printf("  Files containing art2: %d\n", len(neighbours))
	for _, nb := range neighbours {
		fmt.Printf("    file node %d\n", nb.Node.ID)
	}

	// Cross-file artefact similarity: art1 → art3 path.
	path, _ := g.ShortestPath(art1, art3, []store.EdgeType{store.EdgeTypeSimilarTo})
	fmt.Printf("  Shortest SimilarTo path art1→art3: %d nodes\n", len(path.Nodes))

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 4 — Property index: hash-based deduplication and lookup
// ----------------------------------------------------------------------------
func example4_PropertyIndexLookup() {
	fmt.Println("--- Example 4: Property index lookup ---")

	g := graphene.NewInMemory()

	hashes := []string{
		"e3b0c44298fc1c149afbf4c8996fb924", // empty file
		"d41d8cd98f00b204e9800998ecf8427e",
		"e3b0c44298fc1c149afbf4c8996fb924", // duplicate hash
		"098f6bcd4621d373cade4e832627b4f6",
	}

	for _, h := range hashes {
		id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		g.IndexNodeProperty(id, "md5", []byte(h))
	}

	// Find all artefacts sharing the same hash (duplicates).
	duplicateHash := []byte("e3b0c44298fc1c149afbf4c8996fb924")
	hits, _ := g.NodesByProperty("md5", duplicateHash)
	fmt.Printf("  Artefacts with hash %s...: %d (expected 2)\n", string(duplicateHash[:8]), len(hits))

	// Unknown hash — returns empty.
	misses, _ := g.NodesByProperty("md5", []byte("notahash"))
	fmt.Printf("  Artefacts with unknown hash: %d (expected 0)\n", len(misses))

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 5 — Provenance chain: trace an artefact back to its origin
// ----------------------------------------------------------------------------
//
// Graph shape:
//
//	EvidenceFile → Contains → containerArt → Contains → leafArt
func example5_ProvenanceChain() {
	fmt.Println("--- Example 5: Provenance chain ---")

	g := graphene.NewInMemory()

	fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	// A zip archive artefact extracted from the file.
	containerArt, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	// A PE binary artefact extracted from the zip.
	leafArt, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: fileID, Dst: containerArt, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: containerArt, Dst: leafArt, Labels: []store.EdgeType{store.EdgeTypeContains}})

	// Walk backwards from the leaf artefact to find the evidence file.
	chain, err := g.ProvenanceChain(leafArt, 10, []store.EdgeType{store.EdgeTypeContains})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  Provenance depth for leafArt: %d nodes\n", len(chain.Chain))
	for i, n := range chain.Chain {
		fmt.Printf("    [%d] node %d  labels: %v\n", i, n.ID, n.Labels)
	}

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 6 — Shortest path between two artefacts across a large graph
// ----------------------------------------------------------------------------
func example6_ShortestPath() {
	fmt.Println("--- Example 6: Shortest path between artefacts ---")

	g := graphene.NewInMemory()

	// Build a small network: a ─ b ─ c ─ d ─ e (linear)
	//                              └── f ──┘      (shortcut b→f→d)
	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	d, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	e, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	f, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	for _, edge := range [][2]store.NodeID{{a, b}, {b, c}, {c, d}, {d, e}, {b, f}, {f, d}} {
		g.AddEdge(&store.Edge{Src: edge[0], Dst: edge[1], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
		g.AddEdge(&store.Edge{Src: edge[1], Dst: edge[0], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	}

	// Long route: a→b→c→d→e  (4 hops)
	// Short route via shortcut: a→b→f→d→e  (4 hops, same — BFS finds one)
	path, err := g.ShortestPath(a, e, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Shortest path a→e: %d nodes, %d edges\n", len(path.Nodes), len(path.Edges))
	for i, n := range path.Nodes {
		fmt.Printf("    [%d] node %d\n", i, n.ID)
	}

	// Disconnected nodes.
	isolated, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	_, err = g.ShortestPath(a, isolated, nil)
	fmt.Printf("  Path to isolated node: %v\n", err)

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 7 — Subgraph pattern matching: find reuse triangles
// ----------------------------------------------------------------------------
//
// Looks for the pattern: A →Reuse→ B →Reuse→ C →SimilarTo→ A
// which would indicate two artefacts that reuse a third which then loops back.
func example7_PatternMatching() {
	fmt.Println("--- Example 7: Subgraph pattern matching ---")

	g := graphene.NewInMemory()

	// Build a matching triangle.
	x, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	y, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	z, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: x, Dst: y, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: y, Dst: z, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: z, Dst: x, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	// Add a non-matching node to ensure the filter works.
	g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	// Pattern: 3 MicroArtefact nodes with Reuse→Reuse→SimilarTo cycle.
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

	scope := []store.NodeID{x, y, z}
	matches, err := g.FindPatterns(pattern, scope, 0)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  Reuse-triangle matches found: %d\n", len(matches))
	for i, m := range matches {
		fmt.Printf("    Match %d: %v\n", i, m.Mapping)
	}

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 8 — On-disk: open, write, close, reopen — WAL replay
// ----------------------------------------------------------------------------
//
// Demonstrates that nodes and edges written before Close() are automatically
// recovered on the next Open() via WAL replay, without any explicit Compact().
func example8_DiskPersistence() {
	fmt.Println("--- Example 8: On-disk WAL persistence ---")

	dir, err := os.MkdirTemp("", "graphene-ex8-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fmt.Printf("  Store dir: %s\n", filepath.Base(dir))

	var fileID, art1, art2 store.NodeID

	// --- Session 1: ingest ---
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}

		caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		fileID, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		art1, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		art2, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

		g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
		g.AddEdge(&store.Edge{Src: fileID, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
		g.AddEdge(&store.Edge{Src: fileID, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
		g.AddEdge(&store.Edge{Src: art1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.88})

		nc, _ := g.NodeCount()
		ec, _ := g.EdgeCount()
		fmt.Printf("  Session 1 written — nodes: %d  edges: %d\n", nc, ec)
		g.Close()
	}

	// --- Session 2: reopen without Compact (pure WAL replay) ---
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}
		defer g.Close()

		nc, _ := g.NodeCount()
		ec, _ := g.EdgeCount()
		fmt.Printf("  Session 2 restored  — nodes: %d  edges: %d\n", nc, ec)

		// Verify the EvidenceFile node is readable.
		n, err := g.GetNode(fileID)
		if err != nil {
			log.Fatalf("GetNode failed after restart: %v", err)
		}
		fmt.Printf("  EvidenceFile (node %d) labels: %v\n", n.ID, n.Labels)

		// Verify neighbourhood survived.
		neighbours, _ := g.Neighbours(fileID, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeContains})
		fmt.Printf("  Artefacts contained in file: %d\n", len(neighbours))
	}

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 9 — On-disk: Compact then reload from CSR
// ----------------------------------------------------------------------------
//
// After Compact() the WAL is truncated and the graph is stored as a CSR file.
// A subsequent Open() loads from the CSR, not the WAL.
func example9_DiskCompactAndReload() {
	fmt.Println("--- Example 9: On-disk Compact + CSR reload ---")

	dir, err := os.MkdirTemp("", "graphene-ex9-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var art1, art2, art3 store.NodeID

	// --- Session 1: ingest + compact ---
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}

		fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		art1, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		art2, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		art3, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

		g.AddEdge(&store.Edge{Src: fileID, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
		g.AddEdge(&store.Edge{Src: fileID, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
		g.AddEdge(&store.Edge{Src: fileID, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeContains}})
		g.AddEdge(&store.Edge{Src: art1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeReuse}})
		g.AddEdge(&store.Edge{Src: art2, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeReuse}})

		if err := g.Compact(); err != nil {
			log.Fatalf("Compact: %v", err)
		}
		fmt.Printf("  Session 1: compacted into CSR\n")

		// Confirm WAL is now empty (only CSR on disk).
		walInfo, _ := os.Stat(filepath.Join(dir, "graphene.wal"))
		csrInfo, _ := os.Stat(filepath.Join(dir, "graphene.csr"))
		fmt.Printf("  WAL size after compact: %d bytes\n", walInfo.Size())
		fmt.Printf("  CSR size after compact: %d bytes\n", csrInfo.Size())
		g.Close()
	}

	// --- Session 2: reload from CSR ---
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatalf("Open after compact: %v", err)
		}
		defer g.Close()

		nc, _ := g.NodeCount()
		ec, _ := g.EdgeCount()
		fmt.Printf("  Session 2 (from CSR)  — nodes: %d  edges: %d\n", nc, ec)

		// Provenance from art3 back to EvidenceFile.
		chain, _ := g.ProvenanceChain(art3, 10, []store.EdgeType{store.EdgeTypeContains})
		fmt.Printf("  Provenance chain art3→origin: %d nodes\n", len(chain.Chain))

		// NodesByType still works after CSR reload.
		artefacts, _ := g.NodesByType(store.NodeTypeMicroArtefact)
		fmt.Printf("  MicroArtefact nodes in CSR: %d\n", len(artefacts))

		_ = art1
		_ = art2
	}

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 10 — On-disk: property index survives Close/reopen and Compact
// ----------------------------------------------------------------------------
func example10_DiskPropertyIndexSurvivesRestart() {
	fmt.Println("--- Example 10: On-disk property index persistence ---")

	dir, err := os.MkdirTemp("", "graphene-ex10-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	hashes := map[string]store.NodeID{}

	// --- Session 1: write + index ---
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}

		entries := []struct {
			sha256 string
		}{
			{"deadbeefdeadbeef"},
			{"cafebabecafebabe"},
			{"deadbeefdeadbeef"}, // duplicate — should yield 2 hits
		}

		for _, e := range entries {
			id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			g.IndexNodeProperty(id, "sha256", []byte(e.sha256))
			hashes[e.sha256] = id // last writer wins for the map; both are indexed
		}

		hits, _ := g.NodesByProperty("sha256", []byte("deadbeefdeadbeef"))
		fmt.Printf("  Session 1: hits for deadbeef: %d\n", len(hits))
		g.Close()
	}

	// --- Session 2: reopen (WAL replay rebuilds index) ---
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}

		hits, _ := g.NodesByProperty("sha256", []byte("deadbeefdeadbeef"))
		fmt.Printf("  Session 2 (WAL replay): hits for deadbeef: %d\n", len(hits))
		hits, _ = g.NodesByProperty("sha256", []byte("cafebabecafebabe"))
		fmt.Printf("  Session 2 (WAL replay): hits for cafebabe: %d\n", len(hits))

		// Now compact — index must survive.
		if err := g.Compact(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}

	// --- Session 3: reopen (post-compact, index re-emitted into WAL) ---
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}
		defer g.Close()

		hits, _ := g.NodesByProperty("sha256", []byte("deadbeefdeadbeef"))
		fmt.Printf("  Session 3 (post-compact): hits for deadbeef: %d\n", len(hits))
		hits, _ = g.NodesByProperty("sha256", []byte("cafebabecafebabe"))
		fmt.Printf("  Session 3 (post-compact): hits for cafebabe: %d\n", len(hits))
	}

	_ = hashes
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 11 — On-disk: bulk ingest pattern with mid-stream Compact
// ----------------------------------------------------------------------------
//
// Real-world pattern: ingest a batch of files, Compact once, then continue
// adding delta nodes. Demonstrates that NodesByType merges CSR + delta.
func example11_DiskBulkIngestWithCompaction() {
	fmt.Println("--- Example 11: On-disk bulk ingest with mid-stream Compact ---")

	dir, err := os.MkdirTemp("", "graphene-ex11-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer g.Close()

	// --- Batch 1: 5 evidence files, each with 3 artefacts ---
	for range 5 {
		fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		for range 3 {
			artID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			g.AddEdge(&store.Edge{Src: fileID, Dst: artID, Labels: []store.EdgeType{store.EdgeTypeContains}})
			g.IndexNodeProperty(artID, "batch", []byte("1"))
		}
	}

	nc, _ := g.NodeCount()
	fmt.Printf("  After batch 1: %d nodes (5 files + 15 artefacts)\n", nc)

	// Compact batch 1 into CSR.
	if err := g.Compact(); err != nil {
		log.Fatalf("Compact: %v", err)
	}
	fmt.Println("  Compacted batch 1 into CSR")

	// --- Batch 2: 3 more files added into delta layer ---
	for range 3 {
		fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		for range 3 {
			artID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			g.AddEdge(&store.Edge{Src: fileID, Dst: artID, Labels: []store.EdgeType{store.EdgeTypeContains}})
			g.IndexNodeProperty(artID, "batch", []byte("2"))
		}
	}

	nc, _ = g.NodeCount()
	fmt.Printf("  After batch 2: %d nodes (CSR + delta merged)\n", nc)

	// NodesByType merges CSR nodes and delta nodes transparently.
	files, _ := g.NodesByType(store.NodeTypeEvidenceFile)
	artefacts, _ := g.NodesByType(store.NodeTypeMicroArtefact)
	fmt.Printf("  EvidenceFile nodes: %d  MicroArtefact nodes: %d\n", len(files), len(artefacts))

	// Property index works across both batches.
	batch1Hits, _ := g.NodesByProperty("batch", []byte("1"))
	batch2Hits, _ := g.NodesByProperty("batch", []byte("2"))
	fmt.Printf("  Batch-1 artefacts indexed: %d  Batch-2: %d\n", len(batch1Hits), len(batch2Hits))

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 12 — Batch writes and Stats
// ----------------------------------------------------------------------------
//
// Demonstrates AddNodes / AddEdges for bulk ingestion and Stats for a quick
// graph overview.
func example12_BatchWritesAndStats() {
	fmt.Println("--- Example 12: Batch writes and Stats ---")

	g := graphene.NewInMemory()

	// Ingest three artefacts in one call.
	arts, err := g.AddNodes([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  AddNodes returned %d IDs: %v\n", len(arts), arts)

	// Link them in a chain with AddEdges.
	_, err = g.AddEdges([]*store.Edge{
		{Src: arts[0], Dst: arts[1], Labels: []store.EdgeType{store.EdgeTypeReuse}},
		{Src: arts[1], Dst: arts[2], Labels: []store.EdgeType{store.EdgeTypeReuse}},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Index all artefacts with a shared tool tag using IndexNodeProperties.
	for _, id := range arts {
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"tool":    []byte("strings"),
			"version": []byte("2.41"),
		}); err != nil {
			log.Fatal(err)
		}
	}

	stats, _ := g.Stats()
	fmt.Printf("  Stats — nodes: %d  edges: %d\n", stats.NodeCount, stats.EdgeCount)

	// Confirm index: all three artefacts share the same tool.
	hits, _ := g.NodesByProperty("tool", []byte("strings"))
	fmt.Printf("  Artefacts tagged tool=strings: %d\n", len(hits))

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 13 — Multi-key property query (AND semantics)
// ----------------------------------------------------------------------------
//
// Shows NodesByProperties to find artefacts that satisfy multiple property
// constraints simultaneously, e.g. a specific hash AND a specific tool.
func example13_MultiKeyPropertyQuery() {
	fmt.Println("--- Example 13: Multi-key property query ---")

	g := graphene.NewInMemory()

	type artDef struct {
		sha256 string
		tool   string
	}
	defs := []artDef{
		{"deadbeef", "strings"},
		{"cafebabe", "binwalk"},
		{"deadbeef", "binwalk"}, // same hash, different tool
	}

	ids := make([]store.NodeID, len(defs))
	for i, d := range defs {
		id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		g.IndexNodeProperty(id, "sha256", []byte(d.sha256))
		g.IndexNodeProperty(id, "tool", []byte(d.tool))
		ids[i] = id
	}

	// Query: hash=deadbeef AND tool=strings — only defs[0] qualifies.
	hits, err := g.NodesByProperties(map[string][]byte{
		"sha256": []byte("deadbeef"),
		"tool":   []byte("strings"),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  sha256=deadbeef AND tool=strings: %d node(s) (expected 1)\n", len(hits))

	// Query: hash=deadbeef only — two artefacts qualify.
	hits, _ = g.NodesByProperties(map[string][]byte{
		"sha256": []byte("deadbeef"),
	})
	fmt.Printf("  sha256=deadbeef only: %d node(s) (expected 2)\n", len(hits))

	// EdgesByProperties: edge with matching metadata.
	n1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	n2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	eid, _ := g.AddEdge(&store.Edge{Src: n1, Dst: n2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	g.IndexEdgeProperties(eid, map[string][]byte{
		"algorithm": []byte("tlsh"),
		"score":     []byte("95"),
	})

	eHits, _ := g.EdgesByProperties(map[string][]byte{
		"algorithm": []byte("tlsh"),
		"score":     []byte("95"),
	})
	fmt.Printf("  Edges with algorithm=tlsh AND score=95: %d (expected 1)\n", len(eHits))

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 14 — Degree counts and connectivity checks
// ----------------------------------------------------------------------------
//
// Shows InDegree / OutDegree / Degree and EdgeExists / IsConnected helpers.
func example14_DegreeAndConnectivity() {
	fmt.Println("--- Example 14: Degree and connectivity ---")

	g := graphene.NewInMemory()

	fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	art1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art3, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	isolated, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: fileID, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: art1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9})
	g.AddEdge(&store.Edge{Src: art2, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9})

	outDeg, _ := g.OutDegree(fileID, nil)
	fmt.Printf("  OutDegree(file): %d (expected 3)\n", outDeg)

	inDeg, _ := g.InDegree(art1, nil)
	fmt.Printf("  InDegree(art1, all): %d (expected 2 — file + art2)\n", inDeg)

	inDegContains, _ := g.InDegree(art1, []store.EdgeType{store.EdgeTypeContains})
	fmt.Printf("  InDegree(art1, Contains): %d (expected 1)\n", inDegContains)

	deg, _ := g.Degree(art1, nil)
	fmt.Printf("  Degree(art1, all): %d (expected 3)\n", deg)

	// EdgeExists
	exists, _ := g.EdgeExists(fileID, art1, []store.EdgeType{store.EdgeTypeContains})
	fmt.Printf("  EdgeExists(file→art1, Contains): %v\n", exists)
	exists, _ = g.EdgeExists(fileID, art1, []store.EdgeType{store.EdgeTypeSimilarTo})
	fmt.Printf("  EdgeExists(file→art1, SimilarTo): %v\n", exists)

	// IsConnected
	connected, _ := g.IsConnected(art1, art3)
	fmt.Printf("  IsConnected(art1, art3): %v (via file)\n", connected)
	connected, _ = g.IsConnected(art1, isolated)
	fmt.Printf("  IsConnected(art1, isolated): %v\n", connected)

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 15 — Induced subgraph extraction
// ----------------------------------------------------------------------------
//
// Given a BFS result, extracts the induced subgraph (all nodes + only the
// edges whose endpoints are both in the result set).
func example15_InducedSubgraph() {
	fmt.Println("--- Example 15: Induced subgraph extraction ---")

	g := graphene.NewInMemory()

	fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	art1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art3, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: fileID, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: art1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.85})
	g.AddEdge(&store.Edge{Src: art2, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	// BFS from file — collect reachable node IDs as the scope.
	bfsResult, _ := g.BFS(fileID, 1, store.DirectionOutbound, nil)
	scope := graphene.NodeIDsFromBFS(bfsResult)
	fmt.Printf("  BFS scope size: %d nodes\n", len(scope))

	// Extract only the artefact subgraph (exclude the file node).
	artScope := graphene.FilterNodesByLabel(bfsResult.Nodes, store.NodeTypeMicroArtefact)
	artIDs := make([]store.NodeID, len(artScope))
	for i, n := range artScope {
		artIDs[i] = n.ID
	}

	nodes, edges, err := g.InducedSubgraph(artIDs)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Induced artefact subgraph: %d nodes, %d edges\n", len(nodes), len(edges))
	fmt.Printf("  (edges with both endpoints in the artefact set only)\n")

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 16 — Cycle detection
// ----------------------------------------------------------------------------
//
// HasCycle is useful for detecting circular provenance chains or dependency
// loops during forensic analysis.
func example16_CycleDetection() {
	fmt.Println("--- Example 16: Cycle detection ---")

	g := graphene.NewInMemory()

	// DAG: a → b → c (clean provenance, no cycle)
	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: b, Dst: c, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	cycle, _ := g.HasCycle(a, 10, nil)
	fmt.Printf("  DAG (a→b→c): cycle detected = %v (expected false)\n", cycle)

	// Introduce a back-edge: c → a creates a cycle.
	g.AddEdge(&store.Edge{Src: c, Dst: a, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	cycle, _ = g.HasCycle(a, 10, nil)
	fmt.Printf("  After adding c→a: cycle detected = %v (expected true)\n", cycle)

	// NeighboursByNodeType: find only artefact neighbours of a.
	nbrs, _ := g.NeighboursByNodeType(a, store.DirectionOutbound, store.NodeTypeMicroArtefact, nil)
	fmt.Printf("  Outbound MicroArtefact neighbours of a: %d\n", len(nbrs))

	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 17 — Result helper functions
// ----------------------------------------------------------------------------
//
// Demonstrates the slice-oriented result helpers that make it easy to chain
// traversal results into follow-up queries.
func example17_ResultHelpers() {
	fmt.Println("--- Example 17: Result helper functions ---")

	g := graphene.NewInMemory()

	caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	art1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: fileID, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: art1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.75})

	// BFS from file, then extract IDs to use as FindPatterns scope.
	bfs, _ := g.BFS(fileID, 2, store.DirectionBoth, nil)

	fmt.Printf("  NodesFromBFS: %d nodes\n", len(graphene.NodesFromBFS(bfs)))
	fmt.Printf("  EdgesFromBFS: %d edges\n", len(graphene.EdgesFromBFS(bfs)))

	scope := graphene.NodeIDsFromBFS(bfs)
	fmt.Printf("  NodeIDsFromBFS (scope for FindPatterns): %v\n", scope)

	// Filter the BFS result down to just artefacts.
	artNodes := graphene.FilterNodesByLabel(graphene.NodesFromBFS(bfs), store.NodeTypeMicroArtefact)
	fmt.Printf("  MicroArtefacts in BFS result: %d\n", len(artNodes))

	// Filter edges to Contains only.
	containsOnly := graphene.FilterEdgesByLabel(graphene.EdgesFromBFS(bfs), store.EdgeTypeContains)
	fmt.Printf("  Contains edges in BFS result: %d\n", len(containsOnly))

	// ShortestPath and extract ordered node IDs.
	path, _ := g.ShortestPath(art1, caseID, nil)
	pathIDs := graphene.NodeIDsFromPath(path)
	fmt.Printf("  ShortestPath art1→case node IDs: %v\n", pathIDs)

	// Nil-safe accessors: no panic on nil result.
	fmt.Printf("  NodesFromBFS(nil): %v\n", graphene.NodesFromBFS(nil))
	fmt.Printf("  EdgesFromBFS(nil): %v\n", graphene.EdgesFromBFS(nil))

	_ = caseID
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 18 — Persistent extreme-scale limit showcase (opt-in)
// ----------------------------------------------------------------------------
//
// Builds a large persistent graph in the current directory (default 1,000,000
// artefacts), compacts/reopens, and runs a broad verification suite:
//   - type/property index lookups
//   - BFS / DFS / ProvenanceChain / ShortestPath (5+ hop traversals)
//   - degree + connectivity checks
//   - induced subgraph extraction
//   - pattern matching
//
// This example is intentionally opt-in and does NOT delete the directory.
// Set GRAPHENE_RUN_LIMIT_EXAMPLE=1 to run it.
// Optional env vars:
//
//	GRAPHENE_LIMIT_EXAMPLE_NODES=1000000
//	GRAPHENE_LIMIT_EXAMPLE_EDGE_STRIDE=4
//	GRAPHENE_LIMIT_EXAMPLE_DIR=graphene_example_limit_1m
func example18_PersistentExtremeScaleShowcase() {
	fmt.Println("--- Example 18: Persistent extreme-scale limit showcase ---")

	nodeTarget := 1_000_000
	edgeStride := 4
	dirName := "data"

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("  Getwd: %v", err)
	}
	dir := filepath.Join(cwd, dirName)
	if _, err := os.Stat(dir); err == nil {
		log.Fatalf("  directory already exists: %s (remove it manually or set GRAPHENE_LIMIT_EXAMPLE_DIR)", dir)
	} else if !os.IsNotExist(err) {
		log.Fatalf("  stat %s: %v", dir, err)
	}

	fmt.Printf("  Store dir (persistent): %s\n", dir)
	fmt.Printf("  Target nodes=%d edgeStride=%d\n", nodeTarget, edgeStride)

	g, err := graphene.Open(dir)
	if err != nil {
		log.Fatalf("  Open: %v", err)
	}

	caseID, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		log.Fatalf("  AddNode case: %v", err)
	}

	const fileCount = 1_000
	fileIDs := make([]store.NodeID, fileCount)
	for i := 0; i < fileCount; i++ {
		fid, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		if err != nil {
			log.Fatalf("  AddNode file %d: %v", i, err)
		}
		fileIDs[i] = fid
		if _, err := g.AddEdge(&store.Edge{Src: fid, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}}); err != nil {
			log.Fatalf("  AddEdge BelongsTo file %d: %v", i, err)
		}
	}

	// Seed a guaranteed pattern match for FindPatterns.
	patternA, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	patternB, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	patternC, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if _, err := g.AddEdge(&store.Edge{Src: patternA, Dst: patternB, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err != nil {
		log.Fatalf("  AddEdge pattern A->B: %v", err)
	}
	if _, err := g.AddEdge(&store.Edge{Src: patternB, Dst: patternC, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err != nil {
		log.Fatalf("  AddEdge pattern B->C: %v", err)
	}
	if _, err := g.AddEdge(&store.Edge{Src: patternC, Dst: patternA, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}}); err != nil {
		log.Fatalf("  AddEdge pattern C->A: %v", err)
	}

	ids := make([]store.NodeID, nodeTarget)
	checkpointLabel := "edge-1"
	for i := 0; i < nodeTarget; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			log.Fatalf("  AddNode artefact %d: %v", i, err)
		}
		ids[i] = id

		if err := g.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("bucket-%03d", i%1000))); err != nil {
			log.Fatalf("  IndexNodeProperty bucket %d: %v", i, err)
		}
		if i%100_000 == 0 {
			if err := g.IndexNodeProperty(id, "needle", []byte("true")); err != nil {
				log.Fatalf("  IndexNodeProperty needle %d: %v", i, err)
			}
		}

		fileID := fileIDs[i%fileCount]
		if _, err := g.AddEdge(&store.Edge{Src: fileID, Dst: id, Labels: []store.EdgeType{store.EdgeTypeContains}}); err != nil {
			log.Fatalf("  AddEdge contains %d: %v", i, err)
		}
		if i > 0 {
			eid, err := g.AddEdge(&store.Edge{Src: ids[i-1], Dst: id, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9})
			if err != nil {
				log.Fatalf("  AddEdge similar %d: %v", i, err)
			}
			if i == 1 || i%100_000 == 0 {
				checkpointLabel = fmt.Sprintf("edge-%d", i)
				if err := g.IndexEdgeProperty(eid, "checkpoint", []byte(checkpointLabel)); err != nil {
					log.Fatalf("  IndexEdgeProperty checkpoint %d: %v", i, err)
				}
			}
		}
		if i > edgeStride && i%edgeStride == 0 {
			if _, err := g.AddEdge(&store.Edge{Src: ids[i-edgeStride], Dst: id, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err != nil {
				log.Fatalf("  AddEdge reuse %d: %v", i, err)
			}
		}

		if i > 0 && i%100_000 == 0 {
			fmt.Printf("  Ingested %d/%d artefacts\n", i, nodeTarget)
		}
	}

	if err := g.Compact(); err != nil {
		log.Fatalf("  Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		log.Fatalf("  Close after compact: %v", err)
	}

	g2, err := graphene.Open(dir)
	if err != nil {
		log.Fatalf("  Reopen: %v", err)
	}
	defer g2.Close()

	nc, err := g2.NodeCount()
	if err != nil {
		log.Fatalf("  NodeCount: %v", err)
	}
	ec, err := g2.EdgeCount()
	if err != nil {
		log.Fatalf("  EdgeCount: %v", err)
	}

	wantNodes := uint64(1 + fileCount + 3 + nodeTarget)
	wantEdges := uint64(fileCount + 3 + nodeTarget + (nodeTarget - 1) + (nodeTarget / edgeStride))
	fmt.Printf("  Counts after compact+reopen: nodes=%d/%d edges=%d/~%d\n", nc, wantNodes, ec, wantEdges)

	// Index-heavy queries.
	microIDs, _ := g2.NodesByType(store.NodeTypeMicroArtefact)
	containsIDs, _ := g2.EdgesByType(store.EdgeTypeContains)
	bucketHits, _ := g2.NodesByProperty("bucket", []byte("bucket-042"))
	needleHits, _ := g2.NodesByProperty("needle", []byte("true"))
	checkpointHits, _ := g2.EdgesByProperty("checkpoint", []byte(checkpointLabel))
	fmt.Printf("  Type/property queries: micro=%d contains=%d bucket-042=%d needle=%d checkpoint=%d\n",
		len(microIDs), len(containsIDs), len(bucketHits), len(needleHits), len(checkpointHits))

	// 5+ hop traversal checks.
	origin := ids[nodeTarget/2]
	bfs6, _ := g2.BFS(origin, 6, store.DirectionBoth, nil)
	dfs6, _ := g2.DFS(origin, 6, store.DirectionOutbound, nil)
	chain, _ := g2.ProvenanceChain(origin, 8, []store.EdgeType{store.EdgeTypeContains})
	path, _ := g2.ShortestPath(ids[nodeTarget/4], ids[nodeTarget/4+2048], nil)
	fmt.Printf("  Traversals: BFS(depth=6) nodes=%d edges=%d | DFS(depth=6) nodes=%d\n", len(bfs6.Nodes), len(bfs6.Edges), len(dfs6.Nodes))
	fmt.Printf("  ProvenanceChain nodes=%d | ShortestPath nodes=%d edges=%d\n", len(chain.Chain), len(path.Nodes), len(path.Edges))

	// Helper queries and neighbourhood extraction.
	deg, _ := g2.Degree(origin, nil)
	exists, _ := g2.EdgeExists(ids[nodeTarget/3], ids[nodeTarget/3+1], []store.EdgeType{store.EdgeTypeSimilarTo})
	connected, _ := g2.IsConnected(ids[nodeTarget/6], ids[nodeTarget/6+3000])
	scope := graphene.NodeIDsFromBFS(bfs6)
	if len(scope) > 64 {
		scope = scope[:64]
	}
	_, inducedEdges, _ := g2.InducedSubgraph(scope)
	fmt.Printf("  Helpers: Degree=%d EdgeExists(adjacent SimilarTo)=%v IsConnected=%v InducedEdges(scope=%d)=%d\n",
		deg, exists, connected, len(scope), len(inducedEdges))
	inducedNodes, err := g2.GetNodes(scope)
	if err != nil {
		log.Fatalf("  GetNodes for visualization: %v", err)
	}
	vizPath := filepath.Join(dir, "graphene_visualization.html")
	if err := viz.ExportInteractiveHTML(inducedNodes, inducedEdges, vizPath); err != nil {
		log.Fatalf("  write visualization html: %v", err)
	}

	// Pattern matching on known seeded triangle.
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
	matches, _ := g2.FindPatterns(pattern, []store.NodeID{patternA, patternB, patternC}, 10)
	fmt.Printf("  FindPatterns seeded-triangle matches: %d\n", len(matches))
	fmt.Printf("  Visualization: %s\n", vizPath)

	fmt.Println("  Limit showcase complete. Directory intentionally kept for inspection/deletion.")
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 19 — Visualization: case map snapshot
// ----------------------------------------------------------------------------
func example19_VisualizationCaseMap() {
	fmt.Println("--- Example 19: Visualization case map snapshot ---")

	g := graphene.NewInMemory()
	caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})

	var scope []store.NodeID
	scope = append(scope, caseID)
	for i := 0; i < 4; i++ {
		fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		scope = append(scope, fileID)
		_, _ = g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
		for j := 0; j < 3; j++ {
			artID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			scope = append(scope, artID)
			_, _ = g.AddEdge(&store.Edge{Src: fileID, Dst: artID, Labels: []store.EdgeType{store.EdgeTypeContains}})
		}
	}

	nodes, edges, err := collectScopeForViz(g, scope)
	if err != nil {
		log.Fatalf("  collectScopeForViz: %v", err)
	}

	cwd, _ := os.Getwd()
	out := filepath.Join(cwd, "viz_case_map.html")
	err = viz.ExportInteractiveHTMLWithOptions(nodes, edges, out, viz.ExportOptions{
		Title:    "GrapheneDB Case Map",
		Subtitle: "Case, evidence files, and contained artifacts in one connected view.",
	})
	if err != nil {
		log.Fatalf("  viz export: %v", err)
	}

	fmt.Printf("  Visualization: %s\n", out)
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 20 — Visualization: similarity + reuse mesh
// ----------------------------------------------------------------------------
func example20_VisualizationSimilarityMesh() {
	fmt.Println("--- Example 20: Visualization similarity mesh ---")

	g := graphene.NewInMemory()
	scope := make([]store.NodeID, 0, 20)
	ids := make([]store.NodeID, 0, 14)
	for i := 0; i < 14; i++ {
		id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		ids = append(ids, id)
		scope = append(scope, id)
	}

	for i := 0; i < len(ids)-1; i++ {
		_, _ = g.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.55 + float32(i%5)/10})
	}
	for i := 0; i < len(ids)-3; i += 2 {
		_, _ = g.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+3], Labels: []store.EdgeType{store.EdgeTypeReuse}})
	}
	for i := 0; i < len(ids)-5; i += 4 {
		_, _ = g.AddEdge(&store.Edge{Src: ids[i+5], Dst: ids[i], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.93})
	}

	nodes, edges, err := collectScopeForViz(g, scope)
	if err != nil {
		log.Fatalf("  collectScopeForViz: %v", err)
	}

	cwd, _ := os.Getwd()
	out := filepath.Join(cwd, "viz_similarity_mesh.html")
	err = viz.ExportInteractiveHTMLWithOptions(nodes, edges, out, viz.ExportOptions{
		Title:    "GrapheneDB Similarity Mesh",
		Subtitle: "Weighted SimilarTo and Reuse relationships with filterable edge types.",
	})
	if err != nil {
		log.Fatalf("  viz export: %v", err)
	}

	fmt.Printf("  Visualization: %s\n", out)
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Example 21 — Visualization: scoped pattern neighborhood
// ----------------------------------------------------------------------------
func example21_VisualizationPatternScope() {
	fmt.Println("--- Example 21: Visualization pattern scope ---")

	g := graphene.NewInMemory()
	x, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	y, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	z, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	_, _ = g.AddEdge(&store.Edge{Src: x, Dst: y, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	_, _ = g.AddEdge(&store.Edge{Src: y, Dst: z, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	_, _ = g.AddEdge(&store.Edge{Src: z, Dst: x, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.91})

	for i := 0; i < 8; i++ {
		n, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if i%2 == 0 {
			_, _ = g.AddEdge(&store.Edge{Src: x, Dst: n, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.60})
		} else {
			_, _ = g.AddEdge(&store.Edge{Src: n, Dst: z, Labels: []store.EdgeType{store.EdgeTypeReuse}})
		}
	}

	bfs, err := g.BFS(x, 2, store.DirectionBoth, nil)
	if err != nil {
		log.Fatalf("  BFS for visualization scope: %v", err)
	}
	scope := graphene.NodeIDsFromBFS(bfs)
	nodes, edges, err := collectScopeForViz(g, scope)
	if err != nil {
		log.Fatalf("  collectScopeForViz: %v", err)
	}

	cwd, _ := os.Getwd()
	out := filepath.Join(cwd, "viz_pattern_scope.html")
	err = viz.ExportInteractiveHTMLWithOptions(nodes, edges, out, viz.ExportOptions{
		Title:    "GrapheneDB Pattern Scope View",
		Subtitle: "A BFS-scoped neighborhood around a seeded pattern for rapid structural inspection.",
	})
	if err != nil {
		log.Fatalf("  viz export: %v", err)
	}

	fmt.Printf("  Visualization: %s\n", out)
	fmt.Println()
}

func collectScopeForViz(g *graphene.Graph, scope []store.NodeID) ([]*store.Node, []*store.Edge, error) {
	nodes, err := g.GetNodes(scope)
	if err != nil {
		return nil, nil, err
	}
	_, edges, err := g.InducedSubgraph(scope)
	if err != nil {
		return nil, nil, err
	}
	return nodes, edges, nil
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
