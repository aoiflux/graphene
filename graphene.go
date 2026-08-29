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
	"context"
	"fmt"

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
//
// Open takes an exclusive lock on dir for the lifetime of the Graph, so no other
// process — and no other Graph in this one — can open it until Close. A store
// already held returns disk.ErrStoreLocked naming the holder. Use OpenReadOnly
// for a graph you only intend to query.
func Open(dir string) (*Graph, error) {
	s, err := disk.Open(dir)
	if err != nil {
		return nil, err
	}
	return &Graph{GraphStore: s}, nil
}

// OpenReadOnly returns a Graph that can query dir but never write to it, holding
// a shared lock so any number of readers coexist.
//
// No reader runs alongside a writer, and that is deliberate rather than a
// limitation of the lock. The engine loads a store into memory once at open and
// never re-reads it, so a reader admitted alongside a writer would serve a graph
// frozen at its own open, indefinitely, with nothing to signal that it had gone
// stale. Being refused is the better answer, and reopening is how a reader
// advances.
//
// Every mutating call returns disk.ErrReadOnly, including Compact. Nothing under
// dir is modified.
//
//	g, err := graphene.OpenReadOnly(dir)
//	if errors.Is(err, disk.ErrStoreLocked) {
//	    // a writer has it; retry, wait, or report — the engine does not choose
//	}
func OpenReadOnly(dir string) (*Graph, error) {
	s, err := disk.OpenReadOnly(dir)
	if err != nil {
		return nil, err
	}
	return &Graph{GraphStore: s}, nil
}

// OpenLive opens dir for reading by a Graph that can follow a writer.
//
// It takes **no process lock**, so unlike OpenReadOnly it is neither refused
// alongside a writer nor refuses one. That is the trade, and it is worth
// understanding before choosing this over OpenReadOnly: what is given up is the
// "no writer is running" guarantee, which is the whole reason OpenReadOnly may
// fix its view at open. Nothing on the writing side changes — a writer still
// takes an exclusive lock, so two writers remain impossible.
//
// The view is still fixed between calls to Refresh; there is no polling
// goroutine, because how often to look is the caller's decision. Every mutating
// call returns disk.ErrReadOnly and nothing under dir is modified.
//
//	g, err := graphene.OpenLive(dir)
//	...
//	for range time.Tick(time.Second) {
//	    if info, err := g.Refresh(); err == nil && info.Advanced() {
//	        // the graph moved; re-run whatever depends on it
//	    }
//	}
//
// See docs/TECHNICAL_DETAILS.md §9.1b for the protocol and §14.10 for the
// measurement that shaped it.
func OpenLive(dir string) (*Graph, error) {
	s, err := disk.OpenLive(dir)
	if err != nil {
		return nil, err
	}
	return &Graph{GraphStore: s}, nil
}

// Refresh advances a graph opened with OpenLive to the newest state the writer
// has made durable, and reports what it applied.
//
// Backends that cannot advance return an error rather than nil. A no-op would
// leave a caller believing a stale graph is fresh, which is precisely the
// failure OpenLive exists to remove.
func (g *Graph) Refresh() (store.RefreshInfo, error) {
	r, ok := g.GraphStore.(store.Refresher)
	if !ok {
		return store.RefreshInfo{}, disk.ErrNotLiveReader
	}
	return r.Refresh()
}

// IsLive reports whether this graph can be advanced with Refresh — that is,
// whether it was opened with OpenLive.
func (g *Graph) IsLive() bool {
	r, ok := g.GraphStore.(store.Refresher)
	return ok && r.IsLiveReader()
}

// OpenWithOptions returns a Graph backed by a disk store opened with opts.
//
// Open gives you the historical defaults: unsigned commits, no verification on
// open, no audit log. That is the right default for a graph database and the
// wrong one for a store holding evidence, and there was previously no way to ask
// for the other posture without bypassing this package entirely.
//
//	key, pub, _ := signing.GenerateKey(1)
//	ring := signing.NewKeyring()
//	ring.Add(1, pub)
//
//	opts := disk.StrictOptions(key, ring, operatorActorID)
//	opts.Retention = disk.RetentionPolicy{MaxSegments: 50}
//	opts.Redaction = true
//	g, err := graphene.OpenWithOptions(dir, opts)
//
// See docs/API_REFERENCE.md §22 for which options an evidentiary deployment
// wants and why.
func OpenWithOptions(dir string, opts disk.Options) (*Graph, error) {
	s, err := disk.OpenWithOptions(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Graph{GraphStore: s}, nil
}

// Forensics returns the disk store behind this Graph, and whether there is one.
//
// The integrity machinery — signed commits, snapshot roots, inclusion proofs,
// attributed redaction, chain of custody, checkpoints and anchoring — lives on
// disk.Store rather than here, and this is the supported way to reach it.
//
// # Why an accessor and not forty methods
//
// Forwarding each call would double an API surface of about fifty symbols, and
// every one of them would be a place for the façade's version to drift from the
// engine's. It would also flatten a distinction worth keeping: none of that
// machinery works on the in-memory backend, so a Graph that cannot do it should
// say so once rather than fail fifty times. Returning false is that answer.
//
//	if s, ok := g.Forensics(); ok {
//	    proof, err := s.ProveNode(id)
//	}
//
// The store is the same one the Graph is using, not a copy — calls through it
// are visible to the Graph immediately, and closing the Graph closes it.
//
// See SECURITY.md for what each mechanism proves and does not, and
// docs/FORENSICS.md for how to use them.
func (g *Graph) Forensics() (*disk.Store, bool) {
	s, ok := g.GraphStore.(*disk.Store)
	return s, ok
}

// Compact is available when the Graph is backed by a disk.Store. It merges
// the delta layer into the CSR and truncates the WAL. Call it after a bulk
// ingest is complete.
func (g *Graph) Compact() error {
	return g.CompactCtx(context.Background())
}

// CompactCtx is Compact, abandoned if ctx is cancelled.
//
// Cancellation reaches the image build, which is where the seconds are, and
// stops there; a compaction that has begun installing its image finishes. See
// disk.Store.CompactCtx for why that is the only correct place to stop.
func (g *Graph) CompactCtx(ctx context.Context) error {
	s, ok := g.GraphStore.(*disk.Store)
	if !ok {
		return nil // no-op for in-memory
	}
	return s.CompactCtx(ctx)
}

// --- Backup and restore ---

// Backup writes a consistent copy of the store into dst, which must not already
// hold a store or a backup. Available when the Graph is backed by a disk.Store.
//
// The graph stays open and writable throughout; only compaction is refused for
// the duration. See disk.Store.Backup for what makes the copy consistent, and
// Restore for reading one back.
//
// The in-memory backend has no directory to copy and returns an error rather
// than succeeding silently — a backup that quietly did nothing is worse than one
// that failed, because only the second is noticed before it is needed.
func (g *Graph) Backup(dst string) (disk.BackupInfo, error) {
	return g.BackupCtx(context.Background(), dst)
}

// BackupCtx is Backup, abandoned if ctx is cancelled. A cancelled backup leaves
// no manifest, so what it wrote cannot be mistaken for a complete copy.
func (g *Graph) BackupCtx(ctx context.Context, dst string) (disk.BackupInfo, error) {
	s, ok := g.GraphStore.(*disk.Store)
	if !ok {
		return disk.BackupInfo{}, fmt.Errorf("Backup: %T is not backed by a directory", g.GraphStore)
	}
	return s.BackupCtx(ctx, dst)
}

// Restore copies the backup at src into dst and returns what it produced,
// including how far the restored store was rewound if a recovery point was
// asked for.
//
// dst must not exist or must be empty. The backup is verified against its
// manifest before anything is copied. Open the result with graphene.Open.
func Restore(src, dst string, opts disk.RestoreOptions) (disk.RestoreInfo, error) {
	return disk.Restore(src, dst, opts)
}

// VerifyBackup checks a backup directory against its manifest without restoring
// it: every file present, at the recorded length, hashing to the recorded
// digest.
//
// Worth running on a schedule against an archive. The alternative is finding out
// during a recovery, which is the one moment there is no time to react.
func VerifyBackup(dir string) (disk.BackupInfo, error) {
	return disk.VerifyBackup(dir)
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

// DeclareCompositeProperties builds and maintains a composite index over the
// given node property keys, so that a query pinning all of them to values is
// answered by one lookup instead of by driving from the most selective of them
// and eliminating against the rest. Entries already registered are absorbed, so
// this can be called at any point.
//
// The win is the conjunction that is far more selective than any of its parts.
// Two keys that are individually weak — a case identifier and a bucket — pin a
// query to a small set together while either alone leaves thousands of
// candidates to eliminate:
//
//	g.DeclareCompositeProperties([]string{"case", "bucket"})
//
//	g.QueryNodes(store.NodeQuery{Filters: []store.PropertyFilter{
//	    {Key: "case", Op: store.PropertyOpEqual, Value: []byte("C-17")},
//	    {Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")},
//	}})
//
// A composite is used only when the query pins **every** one of its keys with an
// equality filter: the postings are keyed by the whole tuple, so a partially
// specified one has no entry to look up. Order is part of a declaration's
// identity but not of its use — (a, b) serves a query filtering on b and a.
//
// It is not free, which is why it is opt-in: every registration on a member key
// files the entity into the composite too, and the composite holds that entity's
// values for each of its keys. Declare the tuples your queries actually use.
//
// Declarations survive a compaction — they are written into the CSR image and
// re-applied on open — but not a reopen with no compaction since. Returns an
// error for a tuple that cannot be indexed: fewer than two keys, a repeated key,
// an empty key, or more than 64. Backends without the extension ignore this and
// keep intersecting.
func (g *Graph) DeclareCompositeProperties(keys []string) error {
	d, ok := g.GraphStore.(store.CompositeIndexDeclarer)
	if !ok {
		return nil
	}
	return d.DeclareCompositeNodeProperties(keys)
}

// DeclareCompositeEdgeProperties is DeclareCompositeProperties for edge
// properties.
func (g *Graph) DeclareCompositeEdgeProperties(keys []string) error {
	d, ok := g.GraphStore.(store.CompositeIndexDeclarer)
	if !ok {
		return nil
	}
	return d.DeclareCompositeEdgeProperties(keys)
}

// CompositeProperties returns the node and edge property key tuples currently
// backed by a composite index, each tuple in its declared order.
func (g *Graph) CompositeProperties() (nodeKeys, edgeKeys [][]string) {
	d, ok := g.GraphStore.(store.CompositeIndexDeclarer)
	if !ok {
		return nil, nil
	}
	return d.CompositeNodeProperties(), d.CompositeEdgeProperties()
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

// Snapshot returns a consistent read view of the graph.
//
// Every read through it sees the graph as it stood when the snapshot was taken,
// however long it is held and whatever writers do meanwhile. That is what makes
// a multi-step read — a traversal, a report, an export — describe one graph
// rather than a sequence of instants that happen to be adjacent.
//
//	snap, err := g.Snapshot()
//	if err != nil {
//		return err
//	}
//	defer snap.Close()
//	result, err := traversal.BFS(snap, origin, 3, store.DirectionBoth, nil)
//
// The traversal functions take a store.GraphReader, which both a Graph and a
// Snapshot satisfy, so the walk above is the same call it would be against the
// live store.
//
// Close it. On the disk backend a snapshot pins the image it was taken against
// and every delta version written since, so an abandoned one holds memory that
// the next compaction would otherwise release —
// StorageStats().OpenSnapshots is where that shows up.
//
// A backend that cannot provide one returns an error naming itself, matching
// how the other optional capabilities report their absence. Both bundled
// backends can.
func (g *Graph) Snapshot() (store.Snapshot, error) {
	sn, ok := g.GraphStore.(store.Snapshotter)
	if !ok {
		return nil, fmt.Errorf("Snapshot: %T does not support snapshots", g.GraphStore)
	}
	return sn.Snapshot()
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

// --- Bounded and cancellable traversals ---
//
// Each of these is the method above it with a context and a store.Budget. The
// unbounded forms remain exactly what they were and cost exactly what they
// cost; nothing here changes an existing call.
//
// Reach for these whenever the shape of the graph is not known in advance. A
// depth limit does not bound a walk that passes through a hub — one node of
// degree 100 000 puts 100 000 entries in the visited set at depth one — and
// without a budget the only symptom is the process growing until it stops.
// A budget turns that into store.ErrBudgetExceeded, which is something a caller
// can act on.
//
//	res, err := g.BFSCtx(ctx, origin, 3, store.DirectionBoth, nil,
//	        store.Budget{MaxNodes: 100_000, MaxTime: 5 * time.Second})
//	if errors.Is(err, store.ErrBudgetExceeded) {
//	        // too big: narrow the walk, or scope it and try again
//	}
//
// For a walk that must also see one consistent graph, take a Snapshot and pass
// it to the traversal package directly — every function there accepts a
// store.GraphReader, which a Snapshot is.

// BFSCtx is BFS bounded by budget and cancellable through ctx.
func (g *Graph) BFSCtx(ctx context.Context, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType, budget store.Budget) (*traversal.BFSResult, error) {
	return traversal.BFSCtx(ctx, g.GraphStore, origin, maxDepth, dir, edgeTypes, budget)
}

// BFSIDsCtx is BFSIDs bounded by budget and cancellable through ctx.
func (g *Graph) BFSIDsCtx(ctx context.Context, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType, budget store.Budget) ([]store.NodeID, error) {
	return traversal.BFSIDsCtx(ctx, g.GraphStore, origin, maxDepth, dir, edgeTypes, budget)
}

// DFSCtx is DFS bounded by budget and cancellable through ctx.
func (g *Graph) DFSCtx(ctx context.Context, origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType, budget store.Budget) (*traversal.BFSResult, error) {
	return traversal.DFSCtx(ctx, g.GraphStore, origin, maxDepth, dir, edgeTypes, budget)
}

// ProvenanceChainCtx is ProvenanceChain bounded by budget and cancellable
// through ctx.
func (g *Graph) ProvenanceChainCtx(ctx context.Context, origin store.NodeID, maxDepth int, edgeTypes []store.EdgeType, budget store.Budget) (*traversal.DFSResult, error) {
	return traversal.ProvenanceChainCtx(ctx, g.GraphStore, origin, maxDepth, edgeTypes, budget)
}

// ShortestPathCtx is ShortestPath bounded by budget and cancellable through ctx.
func (g *Graph) ShortestPathCtx(ctx context.Context, src, dst store.NodeID, edgeTypes []store.EdgeType, budget store.Budget) (*traversal.PathResult, error) {
	return traversal.ShortestPathCtx(ctx, g.GraphStore, src, dst, edgeTypes, budget)
}

// FindPatternsCtx is FindPatterns bounded by budget and cancellable through
// ctx.
func (g *Graph) FindPatternsCtx(ctx context.Context, pattern *traversal.Pattern, scope []store.NodeID, maxMatches int, budget store.Budget) ([]traversal.SubgraphMatch, error) {
	return traversal.FindSubgraphMatchesCtx(ctx, g.GraphStore, pattern, scope, maxMatches, budget)
}
