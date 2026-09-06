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
	"slices"

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
	if err := loadSidecars(s); err != nil {
		s.Close()
		return nil, err
	}
	return &Graph{GraphStore: s}, nil
}

// loadSidecars applies the sidecar tables that this layer owns.
//
// Today that is the label table alone: the declaration catalogue is read and
// applied inside disk.OpenWithOptions, because it has to land in two places —
// the ordered and composite declarations before the image loads, the
// constraints after the WAL replays — and neither point is reachable from here.
// It also has to reach a read-only store, whose Declare methods correctly
// refuse.
//
// It stays a named function anyway, because every Open path calls it and the
// bug it replaced was OpenWithOptions calling none of this: a store opened with
// disk.StrictOptions rendered every custom label as a number.
//
// The label table is strict. A table disagreeing with what this process already
// believes is an error rather than a resolution, because picking one silently is
// the confident wrong answer it exists to prevent.
func loadSidecars(gs store.GraphStore) error {
	return loadTypeNames(gs)
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
	if err := loadSidecars(s); err != nil {
		s.Close()
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
	if err := loadSidecars(s); err != nil {
		s.Close()
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
	if err := loadSidecars(s); err != nil {
		s.Close()
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
	return g.VerifyIndexesCtx(context.Background())
}

// VerifyIndexesCtx is VerifyIndexes, abandoned if ctx is cancelled. A backend
// that supports verification but not cancellation runs to completion, which is
// the same answer a caller would have got before asking.
//
// Worth reaching for on a large store: the check walks every posting and every
// record with the store's lock held, so a caller that has given up on the
// answer is otherwise holding writers up for a result nobody will read.
func (g *Graph) VerifyIndexesCtx(ctx context.Context) error {
	if v, ok := g.GraphStore.(store.IndexVerifierCtx); ok {
		return v.VerifyIndexesCtx(ctx)
	}
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

// DeclareUniqueProperty enforces that at most one live node holds any given
// value under key, and validates that the graph already satisfies it.
//
// This is what turns an indexed value into a name. Without it, a natural key —
// "ver:<sha256>", "perm:android.permission.INTERNET" — resolves to a set, and
// re-ingesting the same source appends to that set instead of finding what is
// already there. With it, NodeByProperty is total: a key names one node or none,
// and UpsertNode can be written at all.
//
//	if err := g.DeclareUniqueProperty("k"); err != nil {
//	    var v *store.UniqueViolationsError
//	    if errors.As(err, &v) {
//	        // v.Conflicts names every value held more than once
//	    }
//	}
//
// **Existing data is validated first, and every violation is reported**, not the
// first — a caller declaring a constraint on a graph that has been running
// without one is about to repair it, and one duplicate per pass is not a repair
// a person can finish. Nothing is declared when the graph does not satisfy it.
//
// Declaring a key already declared is a no-op, so this belongs at every Open —
// though it is no longer load-bearing there. The declaration is recorded in
// graphene.schema beside the image and re-applied by every Open, including a
// read-only one, so a process that forgets to declare no longer gets a store
// with the constraint quietly absent.
//
// Because the record outlives the process, an Open re-validates: a store whose
// data stopped satisfying a recorded constraint — an older build wrote the
// duplicates, say — is refused rather than opened without it. See
// disk.ConstraintPolicy for the escape hatch that lets such a store be opened
// and repaired.
//
// **Unlike the ordered and composite declarations, this returns an error on a
// backend that cannot enforce it.** Those two are optimisations, and a store
// that ignores one still answers every query correctly. This one is a promise
// about what the graph can contain, and a store that quietly does not keep it
// has given the caller the guarantee and none of the behaviour.
func (g *Graph) DeclareUniqueProperty(key string) error {
	d, ok := g.GraphStore.(store.UniqueIndexDeclarer)
	if !ok {
		return fmt.Errorf("DeclareUniqueProperty: %T cannot enforce unique properties", g.GraphStore)
	}
	return d.DeclareUniqueNodeProperty(key)
}

// DeclareUniqueEdgeProperty is DeclareUniqueProperty for edge properties.
func (g *Graph) DeclareUniqueEdgeProperty(key string) error {
	d, ok := g.GraphStore.(store.UniqueIndexDeclarer)
	if !ok {
		return fmt.Errorf("DeclareUniqueEdgeProperty: %T cannot enforce unique properties", g.GraphStore)
	}
	return d.DeclareUniqueEdgeProperty(key)
}

// UniqueProperties returns the node and edge property keys currently under a
// unique constraint, each sorted.
func (g *Graph) UniqueProperties() (nodeKeys, edgeKeys []string) {
	d, ok := g.GraphStore.(store.UniqueIndexDeclarer)
	if !ok {
		return nil, nil
	}
	return d.UniqueNodeProperties(), d.UniqueEdgeProperties()
}

// DeclareUniqueEdge enforces that at most one live edge of type t joins any
// ordered pair of nodes, and validates that the graph already satisfies it.
//
// This is the structural counterpart to DeclareUniqueProperty, and it exists
// because that one does not scale to bulk edges. UpsertEdge resolves an edge
// through a declared-unique *property*, which is right for an edge carrying
// identity of its own and wrong for structure: a caller writing millions of
// ownership edges would pay an index entry each to restate what the endpoints
// already say. The alternative such a caller is left with is a convention —
// own a subtree, delete it before re-ingesting, never add an edge twice by hand
// — which holds exactly as long as nobody forgets.
//
//	if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
//	    var v *store.EdgeCardinalityViolationsError
//	    if errors.As(err, &v) {
//	        // v.Conflicts names every pair joined more than once
//	    }
//	}
//
// **Per label, not per label set.** An edge carries one or more types and every
// filter in the engine treats a type list as OR over labels, so a declaration
// names one type and means "at most one live edge from src to dst carries this
// type". An edge labelled {Owns, Contains} counts against a declaration on each
// independently.
//
// **Existing data is validated first, and every violation is reported**, not
// the first: a caller declaring a constraint over an existing graph is about to
// repair it, and one pair per pass turns a script into an afternoon. Nothing is
// declared when the graph does not already satisfy the rule.
//
// Declaring a type already declared is a no-op, so this belongs at every Open.
// As with DeclareUniqueProperty the declaration is recorded in graphene.schema
// and re-applied by every Open, so it no longer has to be re-made by each
// process — and an Open re-validates it against the data.
//
// The constraint is answered from the source node's outbound adjacency rather
// than from an index, so nothing new is stored and nothing can go stale — but
// enforcement costs a scan of that adjacency on every write of a constrained
// edge. That is cheap for the ownership edges this exists for and worth knowing
// about before declaring a type whose sources are hubs.
//
// **Like DeclareUniqueProperty and unlike the ordered and composite
// declarations, this returns an error on a backend that cannot enforce it.**
func (g *Graph) DeclareUniqueEdge(t store.EdgeType) error {
	d, ok := g.GraphStore.(store.EdgeCardinalityDeclarer)
	if !ok {
		return fmt.Errorf("DeclareUniqueEdge: %T cannot enforce edge cardinality", g.GraphStore)
	}
	return d.DeclareUniqueEdgeType(t)
}

// UniqueEdges returns the edge types currently under a cardinality constraint,
// sorted.
func (g *Graph) UniqueEdges() []store.EdgeType {
	d, ok := g.GraphStore.(store.EdgeCardinalityDeclarer)
	if !ok {
		return nil
	}
	return d.UniqueEdgeTypes()
}

// UpsertNode creates or replaces the node identified by value under the
// declared-unique property key, and reports whether it was created.
//
// This is the operation that makes re-ingesting a source safe. Run twice over
// the same input it produces the same graph, where AddNode would produce two of
// everything:
//
//	g.DeclareUniqueProperty("k")
//	id, created, err := g.UpsertNode("k", []byte("ver:9f3a"),
//	    &store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: blob},
//	    map[string][]byte{"state": []byte("extracted")})
//
// On a hit the node's labels and properties are replaced by n, and each key
// named in props is *replaced* rather than added to: the superseded value stops
// matching, which is what a field used as a pipeline work queue depends on.
// Keys props does not name keep the entries they had, and the key entry is
// registered for you, so props need not repeat it.
//
// An upsert therefore states what happens to the index, which is why the
// ReindexPolicy — whose job is to decide that for an update that cannot know —
// does not apply to it.
//
// The record and its index entries become durable together: this is one
// transaction, so a crash cannot leave a node that its own key cannot find.
//
// key must be declared unique, or this returns store.ErrKeyNotUnique. For
// several upserts and the edges between them, use Begin and Tx.UpsertNode, which
// commits the lot as one unit.
func (g *Graph) UpsertNode(key string, value []byte, n *store.Node, props map[string][]byte) (store.NodeID, bool, error) {
	tx := g.Begin()
	id, created := tx.UpsertNode(key, value, n, props)
	if err := tx.Commit(); err != nil {
		return store.InvalidNodeID, false, err
	}
	return id, created, nil
}

// UpsertEdge is UpsertNode for edges, keyed on a declared-unique edge property.
//
// Endpoints are immutable, so on a hit the existing edge keeps the Src and Dst
// it was created with. An edge's unique value should therefore identify the pair
// it connects — "<srcKey>:<dstKey>:<relation>" is the usual shape — because a
// key that is stable while its endpoints are meant to move names two different
// edges rather than one.
func (g *Graph) UpsertEdge(key string, value []byte, e *store.Edge, props map[string][]byte) (store.EdgeID, bool, error) {
	tx := g.Begin()
	id, created := tx.UpsertEdge(key, value, e, props)
	if err := tx.Commit(); err != nil {
		return store.InvalidEdgeID, false, err
	}
	return id, created, nil
}

// NodeByProperty returns the single node holding value under key.
//
// It is NodesByProperty with the plural removed, which is only meaningful once
// the key is unique — so it refuses on a key that is not declared, rather than
// returning the first of several and letting the caller believe a constraint is
// in force. A value no node holds is *store.ErrNotFound.
func (g *Graph) NodeByProperty(key string, value []byte) (*store.Node, error) {
	d, ok := g.GraphStore.(store.UniqueIndexDeclarer)
	if !ok {
		return nil, fmt.Errorf("NodeByProperty: %T cannot enforce unique properties", g.GraphStore)
	}
	if !slices.Contains(d.UniqueNodeProperties(), key) {
		return nil, fmt.Errorf("NodeByProperty: node property %q is not declared unique", key)
	}
	ids, err := g.NodesByProperty(key, value)
	if err != nil {
		return nil, err
	}
	switch len(ids) {
	case 0:
		return nil, &store.ErrNotFound{Kind: "node", ID: 0}
	case 1:
		return g.GetNode(ids[0])
	default:
		// Reachable only if the constraint was declared and then violated, which
		// no write path permits — so saying so is more useful than picking one.
		return nil, &store.UniqueViolationsError{
			Kind: "node", Key: key,
			Conflicts: []store.UniqueConflict{{Value: value, IDs: nodeIDsToUint64(ids)}},
		}
	}
}

// EdgeByProperty is NodeByProperty for edges.
func (g *Graph) EdgeByProperty(key string, value []byte) (*store.Edge, error) {
	d, ok := g.GraphStore.(store.UniqueIndexDeclarer)
	if !ok {
		return nil, fmt.Errorf("EdgeByProperty: %T cannot enforce unique properties", g.GraphStore)
	}
	if !slices.Contains(d.UniqueEdgeProperties(), key) {
		return nil, fmt.Errorf("EdgeByProperty: edge property %q is not declared unique", key)
	}
	ids, err := g.EdgesByProperty(key, value)
	if err != nil {
		return nil, err
	}
	switch len(ids) {
	case 0:
		return nil, &store.ErrNotFound{Kind: "edge", ID: 0}
	case 1:
		return g.GetEdge(ids[0])
	default:
		return nil, &store.UniqueViolationsError{
			Kind: "edge", Key: key,
			Conflicts: []store.UniqueConflict{{Value: value, IDs: edgeIDsToUint64(ids)}},
		}
	}
}

func nodeIDsToUint64(ids []store.NodeID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

func edgeIDsToUint64(ids []store.EdgeID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
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
// Declarations survive both a compaction and a bare reopen: they are recorded in
// graphene.schema beside the image, and written into the CSR image as well so
// that an engine predating the catalogue still finds them. Returns an error for
// a tuple that cannot be indexed: fewer than two keys, a repeated key, an empty
// key, or more than 64. Backends without the extension ignore this and keep
// intersecting.
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
	return g.RebuildIndexesCtx(context.Background())
}

// RebuildIndexesCtx is RebuildIndexes, abandoned if ctx is cancelled at the
// points where abandoning it is safe — which is before the structural rebuild
// and during the sweep after it, and nowhere in between. See
// store.IndexRebuilderCtx for why a rebuild cannot simply stop where it is.
func (g *Graph) RebuildIndexesCtx(ctx context.Context) error {
	if r, ok := g.GraphStore.(store.IndexRebuilderCtx); ok {
		return r.RebuildIndexesCtx(ctx)
	}
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

// ReindexPolicy returns the configured policy, or the default if the backend
// does not support configuring one.
//
// It used to report ReindexKeep for such a backend — naming a policy that was in
// force nowhere, since a store with no Reindexer does not consult one.
func (g *Graph) ReindexPolicy() store.ReindexPolicy {
	if r, ok := g.GraphStore.(store.Reindexer); ok {
		return r.ReindexPolicy()
	}
	return store.ReindexReject
}

// UpdateNodeIndexed updates a node and replaces its property-index entries in
// one step: every entry previously registered for the node is dropped and props
// is registered in its place.
//
// This is the correct way to edit a node whose properties are indexed. Plain
// UpdateNode cannot maintain the index — the engine does not know how to decode
// your Properties blob — so under ReindexReject, the default, it refuses; under
// ReindexKeep it leaves stale entries behind, and under ReindexPurge it drops
// entries that were still valid. Passing the desired index state here avoids all
// three, because it is the one call that knows what the update did.
//
// props is the node's whole indexed state afterwards: **keys it does not mention
// are dropped.** Use UpdateNodePartialIndex when only one field moved. Pass nil
// to update the node and leave it un-indexed.
//
// The record and the entries are one transaction, so a crash cannot land one
// without the other. Before v0.5.0 this was three separate calls that the
// documentation described as atomic and that were not.
func (g *Graph) UpdateNodeIndexed(n *store.Node, props map[string][]byte) error {
	tx := g.Begin()
	tx.UpdateNodeIndexed(n, props)
	return tx.Commit()
}

// UpdateNodePartialIndex updates a node and replaces the property-index entries
// for the keys named in changed, leaving every other key as it was.
//
// This is what a state transition wants:
//
//	g.UpdateNodePartialIndex(n, map[string][]byte{"state": []byte("analyzed")})
//
// The node's natural key, and anything else it is indexed under, survive; the
// named key stops matching its old value and starts matching the new one. The
// alternative — enumerating every other key so UpdateNodeIndexed can put them
// back — is how one of them eventually goes missing.
func (g *Graph) UpdateNodePartialIndex(n *store.Node, changed map[string][]byte) error {
	tx := g.Begin()
	tx.UpdateNodePartialIndex(n, changed)
	return tx.Commit()
}

// UpdateEdgeIndexed updates an edge and replaces its property-index entries in
// one step. See UpdateNodeIndexed.
func (g *Graph) UpdateEdgeIndexed(e *store.Edge, props map[string][]byte) error {
	tx := g.Begin()
	tx.UpdateEdgeIndexed(e, props)
	return tx.Commit()
}

// UpdateEdgePartialIndex is UpdateNodePartialIndex for edges.
func (g *Graph) UpdateEdgePartialIndex(e *store.Edge, changed map[string][]byte) error {
	tx := g.Begin()
	tx.UpdateEdgePartialIndex(e, changed)
	return tx.Commit()
}

// reindexNode is the non-transactional fallback for a backend that cannot do
// transactions: purge, then register. It is not atomic, which is what Tx.Atomic
// reports and why both bundled backends never reach it.
func (g *Graph) reindexNode(id store.NodeID, props map[string][]byte, partial bool) error {
	if !partial {
		if r, ok := g.GraphStore.(store.Reindexer); ok {
			if err := r.PurgeNodeIndex(id); err != nil {
				return err
			}
		}
	}
	return g.IndexNodeProperties(id, props)
}

// reindexEdge is reindexNode for edges.
func (g *Graph) reindexEdge(id store.EdgeID, props map[string][]byte, partial bool) error {
	if !partial {
		if r, ok := g.GraphStore.(store.Reindexer); ok {
			if err := r.PurgeEdgeIndex(id); err != nil {
				return err
			}
		}
	}
	return g.IndexEdgeProperties(id, props)
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
//
// Shortest here means fewest edges. When the edges are not interchangeable —
// a similarity score, a transfer size, a duration — the path with the fewest
// of them is not the cheapest one, and ShortestWeightedPath is the question
// being asked instead.
func (g *Graph) ShortestPath(src, dst store.NodeID, edgeTypes []store.EdgeType) (*traversal.PathResult, error) {
	return traversal.ShortestPath(g.GraphStore, src, dst, edgeTypes)
}

// ShortestWeightedPath finds the cheapest undirected path between src and dst,
// where the cost of each step is whatever cost reports for it.
//
// The cost comes from the caller rather than from the edge, because the engine
// has no single notion of distance to offer: store.Edge.Weight is a similarity
// score for EdgeTypeSimilarTo and zero for everything else, and reinterpreting
// it as a distance would make "more similar" mean "further away". The selector
// is handed each step with the edge weight already in it, so the common
// readings cost nothing to express:
//
//	// closer means more similar
//	g.ShortestWeightedPath(a, b, nil, func(e store.IncidentEdge) float64 {
//		return 1 - float64(e.Weight)
//	})
//
// cost must not be nil, and must return a non-negative, deterministic value;
// see store.EdgeCost. Returns traversal.ErrNoPath when the two nodes are not
// connected.
func (g *Graph) ShortestWeightedPath(src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost) (*traversal.PathResult, error) {
	return traversal.ShortestWeightedPath(g.GraphStore, src, dst, edgeTypes, cost)
}

// AStarPath is ShortestWeightedPath guided by a heuristic estimating the
// remaining cost from each node to dst.
//
// A heuristic that never overestimates returns the same path Dijkstra would,
// sooner. One that overestimates returns a worse path and reports no error, so
// store.NodeHeuristic states the obligation; pass nil when there is no estimate
// to make, which is plain Dijkstra.
func (g *Graph) AStarPath(src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost, heuristic store.NodeHeuristic) (*traversal.PathResult, error) {
	return traversal.AStarPath(g.GraphStore, src, dst, edgeTypes, cost, heuristic)
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

// NeighbourFrequency counts, for each node one hop from any anchor, how many
// distinct anchors reach it.
//
// This is the aggregate most cross-graph analytics turn out to be: rank the
// things a set of sources point at, by how many of them point at it. "Which
// trackers do the most app versions embed", "which permissions co-occur",
// "which libraries are shared across the corpus" are one call each.
//
// The alternative is a loop over Neighbours in the caller, which materialises
// every neighbouring record — labels and property blobs included — to increment
// a counter and throw the record away.
//
//	freq, err := g.NeighbourFrequency(versions, store.DirectionOutbound,
//	        []store.EdgeType{edgeEmbeds}, []store.NodeType{nodeTracker})
//
// Anchors are counted, not edges: a neighbour reached twice from one anchor
// counts once, and a repeated anchor is one anchor. Counting edges would make
// the answer depend on how the graph happens to be modelled rather than on what
// it says.
//
// nil edgeTypes follows every edge and nil nodeTypes counts every neighbour;
// both filters are OR over labels, as everywhere else.
//
// Use NeighbourFrequencyCtx on an anchor set whose size is data rather than
// something you chose — it is the form that can be bounded and cancelled, and
// one hub among the anchors is enough to make this arbitrarily large.
func (g *Graph) NeighbourFrequency(anchors []store.NodeID, dir store.Direction, edgeTypes []store.EdgeType, nodeTypes []store.NodeType) (map[store.NodeID]uint64, error) {
	return traversal.NeighbourFrequency(g.GraphStore, anchors, dir, edgeTypes, nodeTypes)
}

// NeighbourFrequencyCtx is NeighbourFrequency bounded by budget and cancellable
// through ctx.
func (g *Graph) NeighbourFrequencyCtx(ctx context.Context, anchors []store.NodeID, dir store.Direction, edgeTypes []store.EdgeType, nodeTypes []store.NodeType, budget store.Budget) (map[store.NodeID]uint64, error) {
	return traversal.NeighbourFrequencyCtx(ctx, g.GraphStore, anchors, dir, edgeTypes, nodeTypes, budget)
}

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

// ShortestWeightedPathCtx is ShortestWeightedPath bounded by budget and
// cancellable through ctx.
//
// Budget.MaxNodes counts nodes settled — reached with their final cost — which
// is the weighted reading of "nodes visited"; see traversal.
func (g *Graph) ShortestWeightedPathCtx(ctx context.Context, src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost, budget store.Budget) (*traversal.PathResult, error) {
	return traversal.ShortestWeightedPathCtx(ctx, g.GraphStore, src, dst, edgeTypes, cost, budget)
}

// AStarPathCtx is AStarPath bounded by budget and cancellable through ctx.
func (g *Graph) AStarPathCtx(ctx context.Context, src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost, heuristic store.NodeHeuristic, budget store.Budget) (*traversal.PathResult, error) {
	return traversal.AStarPathCtx(ctx, g.GraphStore, src, dst, edgeTypes, cost, heuristic, budget)
}

// FindPatternsCtx is FindPatterns bounded by budget and cancellable through
// ctx.
func (g *Graph) FindPatternsCtx(ctx context.Context, pattern *traversal.Pattern, scope []store.NodeID, maxMatches int, budget store.Budget) ([]traversal.SubgraphMatch, error) {
	return traversal.FindSubgraphMatchesCtx(ctx, g.GraphStore, pattern, scope, maxMatches, budget)
}

// --- Bulk export support ---

// ForEachNodeProperty implements store.PropertyEnumerator, so a Graph can be
// handed straight to the bulk package.
//
// Explicit rather than inherited, and that is the whole point of it existing.
// Graph embeds store.GraphStore, and an embedded interface promotes only the
// methods in its own set — so without this a Graph would not satisfy
// PropertyEnumerator, and `bulk.ExportJSONL(w, g, ...)` would have compiled,
// run, and produced a dump with every indexed property entry silently missing.
// The bulk package refuses that case rather than exporting it, and this is what
// keeps the obvious call from hitting the refusal.
//
// A backend that cannot enumerate is a no-op here, which is the same answer the
// other optional capabilities give.
func (g *Graph) ForEachNodeProperty(fn func(id store.NodeID, key string, value []byte) bool) {
	if pe, ok := g.GraphStore.(store.PropertyEnumerator); ok {
		pe.ForEachNodeProperty(fn)
	}
}

// ForEachEdgeProperty implements store.PropertyEnumerator. See
// ForEachNodeProperty for why it is written out rather than inherited.
func (g *Graph) ForEachEdgeProperty(fn func(id store.EdgeID, key string, value []byte) bool) {
	if pe, ok := g.GraphStore.(store.PropertyEnumerator); ok {
		pe.ForEachEdgeProperty(fn)
	}
}
