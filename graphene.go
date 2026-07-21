// Package graphene is an application-specific graph storage engine designed for
// Indicer's forensic micro-artefact platform. It provides:
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
//	// Modify or remove entities (durable; edge endpoints are immutable,
//	// DeleteNode cascades to incident edges)
//	g.UpdateEdge(&store.Edge{ID: eid, Labels: []store.EdgeType{store.EdgeTypeReuse}, Weight: 0.4})
//	g.DeleteNode(artID)
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
// This is the primary entry point for Indicer consumers.
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

// --- Index maintenance ---

// VerifyIndexes cross-checks every index against the records it describes and
// returns the first inconsistency found, or nil if they all agree. Both bundled
// backends support it; a backend that does not returns nil.
//
// It validates structure — postings ordering, reverse-map agreement, adjacency
// endpoints, and that no index entry outlives its entity. It cannot validate
// that an indexed *value* still matches the entity's properties: those values
// are caller-encoded and opaque to the engine. See SetReindexPolicy.
//
// Intended for tests, for CI, and after recovering a store whose indexes may
// have been rebuilt from a partial log.
func (g *Graph) VerifyIndexes() error {
	v, ok := g.GraphStore.(store.IndexVerifier)
	if !ok {
		return nil
	}
	return v.VerifyIndexes()
}

// DeclareOrderedProperty builds and maintains an ordered index over a node
// property key, so that range filters (`>`, `>=`, `<`, `<=`, `Between`) and
// `Prefix` on that key are answered by binary search instead of by scanning
// every entry registered under it. Entries already present are absorbed, so this
// can be called at any point.
//
// **Declaring a key changes how its range predicates compare.** Undeclared keys
// use the scan-path rule: try numeric comparison, fall back to byte order. That
// rule is fine value-by-value but is not a valid sort order — "9" < "10" < "1x"
// < "9" under it — so no ordered structure can be built on it. A declared key is
// compared byte-wise throughout. Encode values so byte order matches your intent:
//
//	// zero-padded fixed width, or index/encoding for real numbers
//	g.IndexNodeProperty(id, "score", encoding.Int64(score))
//	g.DeclareOrderedProperty("score")
//
//	g.QueryNodes(store.NodeQuery{Filters: []store.PropertyFilter{{
//	    Key: "score", Op: store.PropertyOpBetweenInclusive,
//	    Value: encoding.Int64(100), ValueUpper: encoding.Int64(200),
//	}}})
//
// Equality lookups are unaffected. Backends without the extension ignore this
// and keep scanning.
func (g *Graph) DeclareOrderedProperty(key string) error {
	d, ok := g.GraphStore.(store.OrderedIndexDeclarer)
	if !ok {
		return nil
	}
	return d.DeclareOrderedNodeProperty(key)
}

// DeclareOrderedEdgeProperty is DeclareOrderedProperty for edge properties.
func (g *Graph) DeclareOrderedEdgeProperty(key string) error {
	d, ok := g.GraphStore.(store.OrderedIndexDeclarer)
	if !ok {
		return nil
	}
	return d.DeclareOrderedEdgeProperty(key)
}

// OrderedProperties returns the node and edge property keys currently backed by
// an ordered index, each sorted.
func (g *Graph) OrderedProperties() (nodeKeys, edgeKeys []string) {
	d, ok := g.GraphStore.(store.OrderedIndexDeclarer)
	if !ok {
		return nil, nil
	}
	return d.OrderedNodeProperties(), d.OrderedEdgeProperties()
}

// RebuildIndexes discards and recomputes every index derivable from the stored
// records — label postings and adjacency — and drops property-index entries
// whose entity no longer exists. Backends that do not support it return nil.
//
// It repairs structure, not content: property-index *values* are supplied by the
// caller and cannot be recovered from the records, so entries for live entities
// are left as they are. The disk backend runs this automatically on Open when
// its own verification fails, so calling it by hand is normally unnecessary.
func (g *Graph) RebuildIndexes() error {
	r, ok := g.GraphStore.(store.IndexRebuilder)
	if !ok {
		return nil
	}
	return r.RebuildIndexes()
}

// SetReindexPolicy controls what UpdateNode / UpdateEdge do to the property
// index. See store.ReindexPolicy for the trade-off between the two modes; the
// default (store.ReindexKeep) preserves historical behaviour.
//
// Prefer UpdateNodeIndexed / UpdateEdgeIndexed over either policy where you can:
// they update and re-register in one step, so the index is never stale and never
// silently loses entries.
func (g *Graph) SetReindexPolicy(p store.ReindexPolicy) {
	if r, ok := g.GraphStore.(store.Reindexer); ok {
		r.SetReindexPolicy(p)
	}
}

// ReindexPolicy returns the configured policy, or store.ReindexKeep if the
// backend does not support configuring one.
func (g *Graph) ReindexPolicy() store.ReindexPolicy {
	if r, ok := g.GraphStore.(store.Reindexer); ok {
		return r.ReindexPolicy()
	}
	return store.ReindexKeep
}

// UpdateNodeIndexed updates a node and replaces its property-index entries in
// one step: every entry previously registered for the node is dropped and props
// is registered in its place.
//
// This is the correct way to edit a node whose properties are indexed. Plain
// UpdateNode cannot maintain the index — the engine does not know how to decode
// your Properties blob — so it either leaves stale entries behind or (under
// store.ReindexPurge) drops entries that were still valid. Passing the full
// desired index state here avoids both.
//
// Pass a nil or empty props map to update the node and leave it un-indexed.
func (g *Graph) UpdateNodeIndexed(n *store.Node, props map[string][]byte) error {
	if err := g.UpdateNode(n); err != nil {
		return err
	}
	if r, ok := g.GraphStore.(store.Reindexer); ok {
		if err := r.PurgeNodeIndex(n.ID); err != nil {
			return err
		}
	}
	return g.IndexNodeProperties(n.ID, props)
}

// UpdateEdgeIndexed updates an edge and replaces its property-index entries in
// one step. See UpdateNodeIndexed.
func (g *Graph) UpdateEdgeIndexed(e *store.Edge, props map[string][]byte) error {
	if err := g.UpdateEdge(e); err != nil {
		return err
	}
	if r, ok := g.GraphStore.(store.Reindexer); ok {
		if err := r.PurgeEdgeIndex(e.ID); err != nil {
			return err
		}
	}
	return g.IndexEdgeProperties(e.ID, props)
}

// --- Traversal convenience methods ---

// BFS performs a breadth-first traversal from origin up to maxDepth hops.
// Pass nil edgeTypes to follow all edge types.
func (g *Graph) BFS(origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*traversal.BFSResult, error) {
	return traversal.BFS(g.GraphStore, origin, maxDepth, dir, edgeTypes)
}

// BFSIDs performs the same walk as BFS but returns only the reachable node IDs,
// in discovery order, starting with origin.
//
// It never materialises a node or edge record, so on the bundled backends the
// whole traversal allocates only its visited set and result slice, no matter how
// many edges it crosses. Prefer it over BFS whenever the records are not needed:
// reachability checks, scoping a pattern match, or feeding IDs into a query.
func (g *Graph) BFSIDs(origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NodeID, error) {
	return traversal.BFSIDs(g.GraphStore, origin, maxDepth, dir, edgeTypes)
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
