// Package graphene is an application-specific graph storage engine designed for
// SYNTHRA's forensic micro-artefact platform. It provides:
//
//   - A pluggable GraphStore interface (store.GraphStore)
//   - An in-memory reference implementation (memory.Store)
//   - An on-disk, bulk-ingest-optimised CSR implementation (disk.Store)
//   - Core traversal algorithms: BFS, DFS, bidirectional-BFS shortest path,
//     and VF2-inspired subgraph pattern matching
//   - Secondary indexes: type index, temporal index, and property index
//
// # Quick start
//
//	// In-memory (development / small cases)
//	g := graphene.NewInMemory()
//
//	// On-disk (production)
//	g, err := graphene.Open("/data/cases/case01")
//
//	// Add artefacts
//	caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
//	fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
//	artID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
//	g.AddEdge(&store.Edge{Src: fileID, Dst: artID, Labels: []store.EdgeType{store.EdgeTypeContains}})
//	g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
//
//	// Index a decoded property value for fast lookup
//	g.IndexNodeProperty(artID, "sha256", []byte("d4e5f6..."))
//	hits, _ := g.NodesByProperty("sha256", []byte("d4e5f6..."))
//
//	// k-hop neighbourhood
//	result, _ := g.BFS(artID, 2, store.DirectionBoth, nil)
//
//	// Provenance chain back to evidence file
//	chain, _ := g.ProvenanceChain(artID, 10, []store.EdgeType{store.EdgeTypeContains})
//
//	// Shortest path
//	path, _ := g.ShortestPath(artID, caseID, nil)
package graphene

import (
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/memory"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

// Graph wraps a GraphStore and exposes the traversal API in one place.
// This is the primary entry point for SYNTHRA consumers.
type Graph struct {
	store.GraphStore
}

// NewInMemory returns a Graph backed by the in-memory store.
// Suitable for development, testing, and small investigations.
func NewInMemory() *Graph {
	return &Graph{GraphStore: memory.New()}
}

// Open returns a Graph backed by the on-disk CSR store rooted at dir.
// dir is created if it does not exist. On restart, the WAL is replayed
// automatically. Call Graph.Compact() after bulk ingest to rebuild the CSR
// and free WAL space.
func Open(dir string) (*Graph, error) {
	s, err := disk.Open(dir)
	if err != nil {
		return nil, err
	}
	return &Graph{GraphStore: s}, nil
}

// Compact is available when the Graph is backed by a disk.Store. It merges
// the delta layer into the CSR and truncates the WAL. Call it after a bulk
// ingest is complete.
func (g *Graph) Compact() error {
	s, ok := g.GraphStore.(*disk.Store)
	if !ok {
		return nil // no-op for in-memory
	}
	return s.Compact()
}

// --- Traversal convenience methods ---

// BFS performs a breadth-first traversal from origin up to maxDepth hops.
// Pass nil edgeTypes to follow all edge types.
func (g *Graph) BFS(origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*traversal.BFSResult, error) {
	return traversal.BFS(g.GraphStore, origin, maxDepth, dir, edgeTypes)
}

// DFS performs a depth-first traversal from origin up to maxDepth hops.
func (g *Graph) DFS(origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*traversal.BFSResult, error) {
	return traversal.DFS(g.GraphStore, origin, maxDepth, dir, edgeTypes)
}

// ProvenanceChain walks inbound edges from origin back to the root evidence
// source (e.g. the EvidenceFile node), following the given edge types.
// Pass nil edgeTypes to follow all inbound edges.
func (g *Graph) ProvenanceChain(origin store.NodeID, maxDepth int, edgeTypes []store.EdgeType) (*traversal.DFSResult, error) {
	return traversal.ProvenanceChain(g.GraphStore, origin, maxDepth, edgeTypes)
}

// ShortestPath finds the shortest undirected path between src and dst using
// bidirectional BFS.
func (g *Graph) ShortestPath(src, dst store.NodeID, edgeTypes []store.EdgeType) (*traversal.PathResult, error) {
	return traversal.ShortestPath(g.GraphStore, src, dst, edgeTypes)
}

// FindPatterns searches for all subgraphs matching pattern within scope.
// scope limits the candidate nodes; pass nil to search all nodes of the
// matching type (expensive on large graphs — prefer scoping to a case BFS
// result).
// maxMatches caps output; pass 0 for no cap.
func (g *Graph) FindPatterns(pattern *traversal.Pattern, scope []store.NodeID, maxMatches int) ([]traversal.SubgraphMatch, error) {
	return traversal.FindSubgraphMatches(g.GraphStore, pattern, scope, maxMatches)
}
