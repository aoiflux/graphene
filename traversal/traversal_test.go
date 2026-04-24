package traversal

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene/memory"
	"github.com/aoiflux/graphene/store"
)

// buildGraph builds a simple linear chain: n0 → n1 → n2 → ... → nN
// using the given edge label.
func buildChain(s *memory.Store, length int, edgeLabel store.EdgeType) []store.NodeID {
	ids := make([]store.NodeID, length)
	for i := range ids {
		id, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		ids[i] = id
	}
	for i := 0; i < length-1; i++ {
		s.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{edgeLabel}})
	}
	return ids
}

// --- BFS ---

func TestBFS_Depth1(t *testing.T) {
	s := memory.New()
	ids := buildChain(s, 4, store.EdgeTypeContains)

	res, err := BFS(s, ids[0], 1, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatal(err)
	}
	// At depth 1 from ids[0]: ids[0] and ids[1]
	if len(res.Nodes) != 2 {
		t.Errorf("BFS depth=1: got %d nodes, want 2", len(res.Nodes))
	}
	if len(res.Edges) != 1 {
		t.Errorf("BFS depth=1: got %d edges, want 1", len(res.Edges))
	}
}

func TestBFS_FullChain(t *testing.T) {
	s := memory.New()
	ids := buildChain(s, 5, store.EdgeTypeContains)

	res, err := BFS(s, ids[0], 10, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) != 5 {
		t.Errorf("BFS full chain: got %d nodes, want 5", len(res.Nodes))
	}
}

func TestBFS_TypeFilter(t *testing.T) {
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	s.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	s.AddEdge(&store.Edge{Src: a, Dst: c, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})

	// Only follow SimilarTo — should NOT reach c.
	res, _ := BFS(s, a, 2, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeSimilarTo})
	for _, n := range res.Nodes {
		if n.ID == c {
			t.Error("BFS type filter leaked through BelongsTo edge")
		}
	}
	if len(res.Nodes) != 2 { // a + b
		t.Errorf("expected 2 nodes, got %d", len(res.Nodes))
	}
}

func TestBFS_Inbound(t *testing.T) {
	s := memory.New()
	ids := buildChain(s, 3, store.EdgeTypeContains)

	// Walk inbound from the tail.
	res, _ := BFS(s, ids[2], 2, store.DirectionInbound, nil)
	if len(res.Nodes) != 3 {
		t.Errorf("BFS inbound: got %d nodes, want 3", len(res.Nodes))
	}
}

func TestBFS_StarGraph(t *testing.T) {
	s := memory.New()
	center, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	for i := 0; i < 10; i++ {
		leaf, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		s.AddEdge(&store.Edge{Src: center, Dst: leaf, Labels: []store.EdgeType{store.EdgeTypeContains}})
	}
	res, _ := BFS(s, center, 1, store.DirectionOutbound, nil)
	if len(res.Nodes) != 11 { // center + 10 leaves
		t.Errorf("star BFS: got %d nodes, want 11", len(res.Nodes))
	}
}

// --- DFS / ProvenanceChain ---

func TestProvenanceChain(t *testing.T) {
	s := memory.New()
	// file → art1 → art2
	file, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	art1, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	art2, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	s.AddEdge(&store.Edge{Src: file, Dst: art1, Labels: []store.EdgeType{store.EdgeTypeContains}})
	s.AddEdge(&store.Edge{Src: art1, Dst: art2, Labels: []store.EdgeType{store.EdgeTypeContains}})

	chain, err := ProvenanceChain(s, art2, 10, []store.EdgeType{store.EdgeTypeContains})
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.Chain) != 3 {
		t.Errorf("provenance chain: got %d nodes, want 3", len(chain.Chain))
	}
	if chain.Chain[0].ID != art2 {
		t.Error("provenance chain should start at origin")
	}
}

func TestProvenanceChain_Cycle(t *testing.T) {
	// Ensure no infinite loop with a cycle.
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	s.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	s.AddEdge(&store.Edge{Src: b, Dst: a, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	_, err := ProvenanceChain(s, a, 20, nil)
	if err != nil {
		t.Errorf("ProvenanceChain on cycle should not error, got %v", err)
	}
}

// --- ShortestPath ---

func TestShortestPath_Direct(t *testing.T) {
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	s.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	res, err := ShortestPath(s, a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) != 2 {
		t.Errorf("direct path: got %d nodes, want 2", len(res.Nodes))
	}
}

func TestShortestPath_SameNode(t *testing.T) {
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	res, err := ShortestPath(s, a, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) != 1 || res.Nodes[0].ID != a {
		t.Error("same-node path should return single node")
	}
}

func TestShortestPath_Chain(t *testing.T) {
	s := memory.New()
	ids := buildChain(s, 5, store.EdgeTypeSimilarTo)

	res, err := ShortestPath(s, ids[0], ids[4], nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) != 5 {
		t.Errorf("chain shortest path: got %d nodes, want 5", len(res.Nodes))
	}
}

func TestShortestPath_NoPath(t *testing.T) {
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	_, err := ShortestPath(s, a, b, nil)
	if !errors.Is(err, ErrNoPath) {
		t.Errorf("expected ErrNoPath, got %v", err)
	}
}

func TestShortestPath_TwoRoutes(t *testing.T) {
	// a → b → d  (length 2)
	// a → c → d  (length 2, same)
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	d, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	s.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	s.AddEdge(&store.Edge{Src: a, Dst: c, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	s.AddEdge(&store.Edge{Src: b, Dst: d, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	s.AddEdge(&store.Edge{Src: c, Dst: d, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	res, err := ShortestPath(s, a, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) != 3 {
		t.Errorf("two-route graph: got %d nodes, want 3", len(res.Nodes))
	}
}

// --- Subgraph matching ---

func TestFindSubgraphMatches_Triangle(t *testing.T) {
	// Build triangle a → b → c → a
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	s.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	s.AddEdge(&store.Edge{Src: b, Dst: c, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	s.AddEdge(&store.Edge{Src: c, Dst: a, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	pattern := &Pattern{
		Nodes: []PatternNode{
			{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		},
		Edges: []PatternEdge{
			{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
			{SrcPatternID: 1, DstPatternID: 2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
			{SrcPatternID: 2, DstPatternID: 0, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
		},
	}

	scope := []store.NodeID{a, b, c}
	matches, err := FindSubgraphMatches(s, pattern, scope, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Error("expected at least one triangle match")
	}
}

func TestFindSubgraphMatches_NoMatch(t *testing.T) {
	s := memory.New()
	a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	s.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	// Pattern requires an edge from b back to a — which doesn't exist.
	pattern := &Pattern{
		Nodes: []PatternNode{
			{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		},
		Edges: []PatternEdge{
			{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
			{SrcPatternID: 1, DstPatternID: 0, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
		},
	}

	matches, _ := FindSubgraphMatches(s, pattern, []store.NodeID{a, b}, 0)
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestFindSubgraphMatches_EmptyPattern(t *testing.T) {
	s := memory.New()
	matches, err := FindSubgraphMatches(s, &Pattern{}, nil, 0)
	if err != nil || matches != nil {
		t.Errorf("empty pattern: expected nil, nil; got %v %v", matches, err)
	}
}

func TestFindSubgraphMatches_MaxMatches(t *testing.T) {
	// Build a graph with many valid matches and cap at 2.
	s := memory.New()
	var nodeIDs []store.NodeID
	for i := 0; i < 6; i++ {
		id, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		nodeIDs = append(nodeIDs, id)
	}
	// Connect every pair.
	for i := 0; i < len(nodeIDs); i++ {
		for j := i + 1; j < len(nodeIDs); j++ {
			s.AddEdge(&store.Edge{Src: nodeIDs[i], Dst: nodeIDs[j], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
		}
	}

	pattern := &Pattern{
		Nodes: []PatternNode{
			{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		},
		Edges: []PatternEdge{
			{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
		},
	}

	matches, _ := FindSubgraphMatches(s, pattern, nodeIDs, 2)
	if len(matches) != 2 {
		t.Errorf("maxMatches=2: got %d", len(matches))
	}
}

func TestFindSubgraphMatches_MultiLabel_AND(t *testing.T) {
	// Pattern node requires both MicroArtefact AND EvidenceFile labels.
	s := memory.New()
	dual, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact, store.NodeTypeEvidenceFile}})
	single, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	s.AddEdge(&store.Edge{Src: dual, Dst: single, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	s.AddEdge(&store.Edge{Src: single, Dst: dual, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})

	pattern := &Pattern{
		Nodes: []PatternNode{
			{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact, store.NodeTypeEvidenceFile}},
			{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		},
		Edges: []PatternEdge{
			{SrcPatternID: 0, DstPatternID: 1},
		},
	}

	matches, _ := FindSubgraphMatches(s, pattern, []store.NodeID{dual, single}, 0)
	// Only dual satisfies pattern node 0; single does not.
	for _, m := range matches {
		if m.Mapping[0] == single {
			t.Error("single-label node should not match two-label pattern node")
		}
	}
}
