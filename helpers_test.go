package graphene_test

import (
	"errors"
	"testing"

	"graphene"
	"graphene/store"
)

// newTestGraph builds an in-memory graph with a standard case/file/artefact
// layout used across multiple helper tests:
//
//	Case ← BelongsTo ← file1 → Contains → art1, art2
//	Case ← BelongsTo ← file2 → Contains → art2, art3
//	art1 ←SimilarTo→ art3
//	art2 → Reuse → art1
func newTestGraph(t *testing.T) (g *graphene.Graph, caseID, file1, file2, art1, art2, art3 store.NodeID) {
	t.Helper()
	g = graphene.NewInMemory()

	var err error
	caseID, err = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	file1, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	file2, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	art1, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art3, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	g.AddEdge(&store.Edge{Src: file1, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
	g.AddEdge(&store.Edge{Src: file2, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
	g.AddEdge(&store.Edge{Src: file1, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: file1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: file2, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: file2, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeContains}})
	g.AddEdge(&store.Edge{Src: art1, Dst: art3, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9})
	g.AddEdge(&store.Edge{Src: art3, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9})
	g.AddEdge(&store.Edge{Src: art2, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	return
}

// --- Stats ---

func TestStats(t *testing.T) {
	g, _, _, _, _, _, _ := newTestGraph(t)
	stats, err := g.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.NodeCount != 6 {
		t.Errorf("NodeCount: got %d, want 6", stats.NodeCount)
	}
	if stats.EdgeCount != 9 {
		t.Errorf("EdgeCount: got %d, want 9", stats.EdgeCount)
	}
}

// --- Batch reads ---

func TestGetNodes(t *testing.T) {
	g, caseID, file1, _, art1, _, _ := newTestGraph(t)
	ids := []store.NodeID{caseID, file1, art1}
	nodes, err := g.GetNodes(ids)
	if err != nil {
		t.Fatalf("GetNodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("GetNodes: got %d nodes, want 3", len(nodes))
	}
	if !nodes[0].HasLabel(store.NodeTypeCase) {
		t.Error("first node should be Case")
	}
	if !nodes[1].HasLabel(store.NodeTypeEvidenceFile) {
		t.Error("second node should be EvidenceFile")
	}
}

func TestGetNodes_NotFound(t *testing.T) {
	g := graphene.NewInMemory()
	_, err := g.GetNodes([]store.NodeID{999})
	var nf *store.ErrNotFound
	if !errors.As(err, &nf) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestGetEdges(t *testing.T) {
	g := graphene.NewInMemory()
	n1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	n2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	e1, _ := g.AddEdge(&store.Edge{Src: n1, Dst: n2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	e2, _ := g.AddEdge(&store.Edge{Src: n2, Dst: n1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	edges, err := g.GetEdges([]store.EdgeID{e1, e2})
	if err != nil {
		t.Fatalf("GetEdges: %v", err)
	}
	if len(edges) != 2 {
		t.Fatalf("GetEdges: got %d, want 2", len(edges))
	}
	if edges[0].ID != e1 || edges[1].ID != e2 {
		t.Error("GetEdges returned wrong order")
	}
}

func TestGetEdges_NotFound(t *testing.T) {
	g := graphene.NewInMemory()
	_, err := g.GetEdges([]store.EdgeID{999})
	var nf *store.ErrNotFound
	if !errors.As(err, &nf) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- Batch writes ---

func TestAddNodes(t *testing.T) {
	g := graphene.NewInMemory()
	nodes := []*store.Node{
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
		{Labels: []store.NodeType{store.NodeTypeCase}},
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("AddNodes: got %d IDs, want 3", len(ids))
	}
	for i, id := range ids {
		if id == store.InvalidNodeID {
			t.Errorf("AddNodes[%d]: got InvalidNodeID", i)
		}
	}
	nc, _ := g.NodeCount()
	if nc != 3 {
		t.Errorf("NodeCount after AddNodes: %d, want 3", nc)
	}
}

func TestAddEdges(t *testing.T) {
	g := graphene.NewInMemory()
	n1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	n2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	n3, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	edges := []*store.Edge{
		{Src: n1, Dst: n2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
		{Src: n2, Dst: n3, Labels: []store.EdgeType{store.EdgeTypeReuse}},
	}
	ids, err := g.AddEdges(edges)
	if err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("AddEdges: got %d IDs, want 2", len(ids))
	}
	ec, _ := g.EdgeCount()
	if ec != 2 {
		t.Errorf("EdgeCount after AddEdges: %d, want 2", ec)
	}
}

func TestAddEdges_InvalidSrc(t *testing.T) {
	g := graphene.NewInMemory()
	n1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	_, err := g.AddEdges([]*store.Edge{
		{Src: n1, Dst: 999, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
	})
	if err == nil {
		t.Error("expected error for invalid dst node")
	}
}

// --- Bulk property indexing ---

func TestIndexNodeProperties(t *testing.T) {
	g := graphene.NewInMemory()
	id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	props := map[string][]byte{
		"sha256": []byte("aabbcc"),
		"md5":    []byte("112233"),
		"size":   []byte("4096"),
	}
	if err := g.IndexNodeProperties(id, props); err != nil {
		t.Fatalf("IndexNodeProperties: %v", err)
	}

	for k, v := range props {
		hits, err := g.NodesByProperty(k, v)
		if err != nil {
			t.Errorf("NodesByProperty(%q): %v", k, err)
		}
		if len(hits) != 1 || hits[0] != id {
			t.Errorf("NodesByProperty(%q): got %v, want [%d]", k, hits, id)
		}
	}
}

func TestIndexEdgeProperties(t *testing.T) {
	g := graphene.NewInMemory()
	n1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	n2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	eid, _ := g.AddEdge(&store.Edge{Src: n1, Dst: n2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	props := map[string][]byte{
		"algorithm": []byte("tlsh"),
		"threshold": []byte("80"),
	}
	if err := g.IndexEdgeProperties(eid, props); err != nil {
		t.Fatalf("IndexEdgeProperties: %v", err)
	}
	hits, _ := g.EdgesByProperty("algorithm", []byte("tlsh"))
	if len(hits) != 1 || hits[0] != eid {
		t.Errorf("EdgesByProperty: got %v, want [%d]", hits, eid)
	}
}

// --- Multi-key property queries ---

func TestNodesByProperties_ANDSemantics(t *testing.T) {
	g := graphene.NewInMemory()

	// art1: both sha256 and md5 indexed
	art1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	g.IndexNodeProperty(art1, "sha256", []byte("aabbcc"))
	g.IndexNodeProperty(art1, "md5", []byte("112233"))

	// art2: only sha256 indexed
	art2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	g.IndexNodeProperty(art2, "sha256", []byte("aabbcc"))

	// Query requiring both keys — only art1 qualifies.
	hits, err := g.NodesByProperties(map[string][]byte{
		"sha256": []byte("aabbcc"),
		"md5":    []byte("112233"),
	})
	if err != nil {
		t.Fatalf("NodesByProperties: %v", err)
	}
	if len(hits) != 1 || hits[0] != art1 {
		t.Errorf("NodesByProperties AND: got %v, want [%d]", hits, art1)
	}

	// Query with only sha256 — both qualify.
	hits, err = g.NodesByProperties(map[string][]byte{
		"sha256": []byte("aabbcc"),
	})
	if err != nil {
		t.Fatalf("NodesByProperties single key: %v", err)
	}
	if len(hits) != 2 {
		t.Errorf("NodesByProperties single key: got %d hits, want 2", len(hits))
	}
}

func TestNodesByProperties_NoMatch(t *testing.T) {
	g := graphene.NewInMemory()
	id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	g.IndexNodeProperty(id, "sha256", []byte("aabbcc"))

	hits, err := g.NodesByProperties(map[string][]byte{
		"sha256": []byte("aabbcc"),
		"md5":    []byte("nomatch"),
	})
	if err != nil {
		t.Fatalf("NodesByProperties: %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("expected 0 hits, got %d", len(hits))
	}
}

// --- Multi-type queries ---

func TestNodesByAnyType(t *testing.T) {
	g, _, _, _, _, _, _ := newTestGraph(t)

	hits, err := g.NodesByAnyType([]store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeCase})
	if err != nil {
		t.Fatalf("NodesByAnyType: %v", err)
	}
	// 2 evidence files + 1 case = 3
	if len(hits) != 3 {
		t.Errorf("NodesByAnyType(File|Case): got %d, want 3", len(hits))
	}
}

func TestNodesByAnyType_Dedup(t *testing.T) {
	g := graphene.NewInMemory()
	// multi-label node counts once
	id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact}})
	g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	hits, _ := g.NodesByAnyType([]store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact})
	seen := make(map[store.NodeID]int)
	for _, h := range hits {
		seen[h]++
	}
	if seen[id] > 1 {
		t.Errorf("node %d appeared %d times, want 1 (dedup check)", id, seen[id])
	}
}

func TestEdgesByAnyType(t *testing.T) {
	g, _, _, _, _, _, _ := newTestGraph(t)

	hits, err := g.EdgesByAnyType([]store.EdgeType{store.EdgeTypeSimilarTo, store.EdgeTypeReuse})
	if err != nil {
		t.Fatalf("EdgesByAnyType: %v", err)
	}
	// 2 SimilarTo + 1 Reuse = 3
	if len(hits) != 3 {
		t.Errorf("EdgesByAnyType(SimilarTo|Reuse): got %d, want 3", len(hits))
	}
}

// --- Degree helpers ---

func TestInDegree(t *testing.T) {
	g, _, _, _, art1, _, _ := newTestGraph(t)
	// art1 receives: Contains (from file1) + SimilarTo (from art3) + Reuse (from art2)
	deg, err := g.InDegree(art1, nil)
	if err != nil {
		t.Fatalf("InDegree: %v", err)
	}
	if deg != 3 {
		t.Errorf("InDegree(art1): got %d, want 3", deg)
	}
}

func TestOutDegree(t *testing.T) {
	g, _, file1, _, _, _, _ := newTestGraph(t)
	// file1 sends: BelongsTo (to case) + Contains (to art1) + Contains (to art2)
	deg, err := g.OutDegree(file1, nil)
	if err != nil {
		t.Fatalf("OutDegree: %v", err)
	}
	if deg != 3 {
		t.Errorf("OutDegree(file1): got %d, want 3", deg)
	}
}

func TestOutDegree_FilteredByType(t *testing.T) {
	g, _, file1, _, _, _, _ := newTestGraph(t)
	deg, err := g.OutDegree(file1, []store.EdgeType{store.EdgeTypeContains})
	if err != nil {
		t.Fatalf("OutDegree filtered: %v", err)
	}
	if deg != 2 {
		t.Errorf("OutDegree(file1, Contains): got %d, want 2", deg)
	}
}

func TestDegree(t *testing.T) {
	g, _, _, _, art1, _, _ := newTestGraph(t)
	// art1 in=3, out=1 (SimilarTo to art3)
	deg, err := g.Degree(art1, nil)
	if err != nil {
		t.Fatalf("Degree: %v", err)
	}
	if deg != 4 {
		t.Errorf("Degree(art1): got %d, want 4", deg)
	}
}

// --- Connectivity helpers ---

func TestEdgeExists_True(t *testing.T) {
	g, _, file1, _, art1, _, _ := newTestGraph(t)
	exists, err := g.EdgeExists(file1, art1, []store.EdgeType{store.EdgeTypeContains})
	if err != nil {
		t.Fatalf("EdgeExists: %v", err)
	}
	if !exists {
		t.Error("expected edge file1→art1 (Contains) to exist")
	}
}

func TestEdgeExists_False_WrongType(t *testing.T) {
	g, _, file1, _, art1, _, _ := newTestGraph(t)
	exists, err := g.EdgeExists(file1, art1, []store.EdgeType{store.EdgeTypeSimilarTo})
	if err != nil {
		t.Fatalf("EdgeExists: %v", err)
	}
	if exists {
		t.Error("expected no SimilarTo edge from file1 to art1")
	}
}

func TestEdgeExists_False_NoEdge(t *testing.T) {
	g, _, _, _, art1, _, art3 := newTestGraph(t)
	// art3 → art1 exists (SimilarTo), but art1 → art3 outbound to art3 via Contains does not.
	exists, err := g.EdgeExists(art1, art3, []store.EdgeType{store.EdgeTypeContains})
	if err != nil {
		t.Fatalf("EdgeExists: %v", err)
	}
	if exists {
		t.Error("expected no Contains edge from art1 to art3")
	}
}

func TestIsConnected_True(t *testing.T) {
	g, caseID, _, _, _, _, art3 := newTestGraph(t)
	// art3 → file2 → case (via Contains + BelongsTo)
	connected, err := g.IsConnected(art3, caseID)
	if err != nil {
		t.Fatalf("IsConnected: %v", err)
	}
	if !connected {
		t.Error("expected art3 and case to be connected")
	}
}

func TestIsConnected_False(t *testing.T) {
	g := graphene.NewInMemory()
	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	connected, err := g.IsConnected(a, b)
	if err != nil {
		t.Fatalf("IsConnected: %v", err)
	}
	if connected {
		t.Error("isolated nodes should not be connected")
	}
}

// --- Neighbour helpers ---

func TestNeighboursByNodeType(t *testing.T) {
	g, _, file1, _, _, _, _ := newTestGraph(t)

	// Outbound neighbours of file1 that are MicroArtefacts (ignoring Case)
	arts, err := g.NeighboursByNodeType(file1, store.DirectionOutbound, store.NodeTypeMicroArtefact, nil)
	if err != nil {
		t.Fatalf("NeighboursByNodeType: %v", err)
	}
	if len(arts) != 2 {
		t.Errorf("NeighboursByNodeType(file1, MicroArtefact): got %d, want 2", len(arts))
	}
	for _, n := range arts {
		if !n.HasLabel(store.NodeTypeMicroArtefact) {
			t.Errorf("returned node %d is not a MicroArtefact", n.ID)
		}
	}
}

// --- Subgraph extraction ---

func TestInducedSubgraph(t *testing.T) {
	g, _, _, _, art1, art2, art3 := newTestGraph(t)

	nodes, edges, err := g.InducedSubgraph([]store.NodeID{art1, art2, art3})
	if err != nil {
		t.Fatalf("InducedSubgraph: %v", err)
	}
	if len(nodes) != 3 {
		t.Errorf("InducedSubgraph nodes: got %d, want 3", len(nodes))
	}
	// Edges within the set: art1↔art3 (x2 SimilarTo) + art2→art1 (Reuse) = 3
	if len(edges) != 3 {
		t.Errorf("InducedSubgraph edges: got %d, want 3", len(edges))
	}
	// All edge endpoints must be within the set.
	inSet := map[store.NodeID]struct{}{art1: {}, art2: {}, art3: {}}
	for _, e := range edges {
		if _, ok := inSet[e.Src]; !ok {
			t.Errorf("edge Src %d is outside the induced subgraph set", e.Src)
		}
		if _, ok := inSet[e.Dst]; !ok {
			t.Errorf("edge Dst %d is outside the induced subgraph set", e.Dst)
		}
	}
}

func TestInducedSubgraph_NoInternalEdges(t *testing.T) {
	g, caseID, _, _, _, _, _ := newTestGraph(t)
	// Case has no edges to itself; subgraph of one node should have 0 edges.
	nodes, edges, err := g.InducedSubgraph([]store.NodeID{caseID})
	if err != nil {
		t.Fatalf("InducedSubgraph: %v", err)
	}
	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	}
	if len(edges) != 0 {
		t.Errorf("expected 0 internal edges, got %d", len(edges))
	}
}

// --- Cycle detection ---

func TestHasCycle_True(t *testing.T) {
	g := graphene.NewInMemory()
	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	// a → b → c → a  (cycle)
	g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: b, Dst: c, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: c, Dst: a, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	found, err := g.HasCycle(a, 10, nil)
	if err != nil {
		t.Fatalf("HasCycle: %v", err)
	}
	if !found {
		t.Error("expected cycle to be detected")
	}
}

func TestHasCycle_False(t *testing.T) {
	g := graphene.NewInMemory()
	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	// DAG: a → b → c (no cycle)
	g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: b, Dst: c, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	found, err := g.HasCycle(a, 10, nil)
	if err != nil {
		t.Fatalf("HasCycle: %v", err)
	}
	if found {
		t.Error("no cycle expected in a DAG")
	}
}

func TestHasCycle_DepthCap(t *testing.T) {
	// Cycle exists but is deeper than maxDepth — should not be reported.
	g := graphene.NewInMemory()
	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	d, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	// a → b → c → d → a  (cycle at depth 4)
	g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: b, Dst: c, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: c, Dst: d, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	g.AddEdge(&store.Edge{Src: d, Dst: a, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	found, err := g.HasCycle(a, 2, nil) // only look 2 hops deep
	if err != nil {
		t.Fatalf("HasCycle: %v", err)
	}
	if found {
		t.Error("cycle beyond maxDepth should not be detected")
	}
}

// --- Result helpers ---

func TestNodesFromBFS_Nil(t *testing.T) {
	if got := graphene.NodesFromBFS(nil); got != nil {
		t.Errorf("NodesFromBFS(nil): got %v, want nil", got)
	}
}

func TestEdgesFromBFS_Nil(t *testing.T) {
	if got := graphene.EdgesFromBFS(nil); got != nil {
		t.Errorf("EdgesFromBFS(nil): got %v, want nil", got)
	}
}

func TestNodeIDsFromBFS(t *testing.T) {
	g, _, file1, _, _, _, _ := newTestGraph(t)
	result, err := g.BFS(file1, 1, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatalf("BFS: %v", err)
	}
	ids := graphene.NodeIDsFromBFS(result)
	if len(ids) != len(result.Nodes) {
		t.Errorf("NodeIDsFromBFS: got %d IDs, want %d", len(ids), len(result.Nodes))
	}
	for i, n := range result.Nodes {
		if ids[i] != n.ID {
			t.Errorf("NodeIDsFromBFS[%d]: got %d, want %d", i, ids[i], n.ID)
		}
	}
}

func TestNodeIDsFromPath(t *testing.T) {
	g, caseID, _, file2, _, _, art3 := newTestGraph(t)
	result, err := g.ShortestPath(art3, caseID, nil)
	if err != nil {
		t.Fatalf("ShortestPath: %v", err)
	}
	ids := graphene.NodeIDsFromPath(result)
	if len(ids) == 0 {
		t.Error("NodeIDsFromPath: expected non-empty path")
	}
	_ = file2
}

func TestFilterNodesByLabel(t *testing.T) {
	g, _, file1, _, _, _, _ := newTestGraph(t)
	result, _ := g.BFS(file1, 1, store.DirectionOutbound, nil)

	arts := graphene.FilterNodesByLabel(result.Nodes, store.NodeTypeMicroArtefact)
	for _, n := range arts {
		if !n.HasLabel(store.NodeTypeMicroArtefact) {
			t.Errorf("FilterNodesByLabel returned non-MicroArtefact node %d", n.ID)
		}
	}
	if len(arts) == 0 {
		t.Error("expected at least one MicroArtefact in BFS from file1")
	}
}

func TestFilterEdgesByLabel(t *testing.T) {
	g, _, file1, _, _, _, _ := newTestGraph(t)
	result, _ := g.BFS(file1, 1, store.DirectionOutbound, nil)

	containsEdges := graphene.FilterEdgesByLabel(result.Edges, store.EdgeTypeContains)
	for _, e := range containsEdges {
		if !e.HasLabel(store.EdgeTypeContains) {
			t.Errorf("FilterEdgesByLabel returned edge %d without Contains label", e.ID)
		}
	}
}
