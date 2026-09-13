package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// GraphStore is the core persistence and retrieval interface for the graph engine.
// All implementations (in-memory, on-disk CSR, etc.) satisfy this interface.
// Thread safety is implementation-defined; callers should document their assumptions.
type GraphStore interface {
	// --- Write ---

	// AddNode persists n and assigns n.ID. The caller must set n.Labels (at least
	// one label is required); all other fields are optional. Returns the assigned NodeID.
	AddNode(n *Node) (NodeID, error)

	// AddEdge persists e and assigns e.ID. Src and Dst must already exist.
	// Returns the assigned EdgeID.
	AddEdge(e *Edge) (EdgeID, error)

	// --- Mutate ---

	// UpdateNode replaces the Labels and Properties of the node identified by
	// n.ID with the values carried on n. n.ID must reference an existing node
	// (otherwise *ErrNotFound is returned) and n.Labels must contain at least
	// one label. The node's ID is never changed.
	//
	// UpdateNode does not touch the property index: values registered via
	// IndexNodeProperty are caller-encoded and decoupled from Properties, so the
	// engine cannot tell that an indexed field changed.
	//
	// The consequence is a wrong answer, not a slow one. Index entries are
	// additive, so a value registered before the update keeps matching after it:
	// NodesByProperty returns an entity that no longer holds the value it was
	// found by, and the query planner trusts that result. Registering the new
	// value does not displace the old one.
	//
	// For an entity with indexed properties, prefer graphene.UpdateNodeIndexed,
	// which updates and re-registers in one step. Reach for plain UpdateNode when
	// the entity has no index entries, or when you are purging them yourself —
	// see ReindexPolicy for the two ways to do that and what each costs.
	UpdateNode(n *Node) error

	// UpdateEdge replaces the Labels, Weight and Properties of the edge
	// identified by e.ID. The endpoints (Src/Dst) are immutable: any Src/Dst
	// set on e is ignored. Returns *ErrNotFound if the edge does not exist and
	// requires at least one label. To reconnect an edge, delete it and add a
	// new one.
	//
	// As with UpdateNode, the property index is left untouched, with the same
	// stale-entry consequence. Prefer graphene.UpdateEdgeIndexed for an edge that
	// carries indexed properties.
	UpdateEdge(e *Edge) error

	// DeleteNode removes the node with the given id and cascades to every edge
	// incident to it (inbound and outbound), so no edge is ever left pointing
	// at a missing node. Property-index entries for the node and the cascaded
	// edges are purged. Returns *ErrNotFound if the node does not exist.
	//
	// IDs are never reused: a deleted id is not handed out again.
	//
	// Concurrency: DeleteNode is atomic (validation, cascade, and apply happen
	// under a single lock hold). It is safe to run concurrently with AddEdge on
	// the same node — the edge is either created before the node is removed (and
	// then cascaded) or rejected with ErrInvalidEdge. No dangling edge results.
	DeleteNode(id NodeID) error

	// DeleteEdge removes a single edge and purges its property-index entries.
	// Returns *ErrNotFound if the edge does not exist.
	DeleteEdge(id EdgeID) error

	// --- Read: single entity ---

	GetNode(id NodeID) (*Node, error)
	GetEdge(id EdgeID) (*Edge, error)

	// --- Read: adjacency ---

	// Neighbours returns all nodes connected to id via edges matching the given
	// direction and (optionally) edge type filter. Pass nil edgeTypes to return
	// all edge types.
	Neighbours(id NodeID, dir Direction, edgeTypes []EdgeType) ([]NeighbourResult, error)

	// EdgesOf returns all edges incident to id in the given direction.
	// Pass nil edgeTypes to return all edge types.
	EdgesOf(id NodeID, dir Direction, edgeTypes []EdgeType) ([]*Edge, error)

	// --- Read: by type ---

	// NodesByType returns all NodeIDs that carry the given label.
	NodesByType(t NodeType) ([]NodeID, error)

	// EdgesByType returns all EdgeIDs that carry the given label.
	EdgesByType(t EdgeType) ([]EdgeID, error)

	// --- Property index ---

	// IndexNodeProperty registers a decoded property key-value pair for nodeID.
	// value should use a deterministic encoding (e.g. raw msgpack for that key)
	// so that the same encoding can be used in NodesByProperty queries.
	// The entry is persisted to the WAL on disk-backed stores.
	IndexNodeProperty(id NodeID, key string, value []byte) error

	// IndexEdgeProperty registers a decoded property key-value pair for edgeID.
	IndexEdgeProperty(id EdgeID, key string, value []byte) error

	// NodesByProperty returns all NodeIDs that have an indexed entry for key=value.
	NodesByProperty(key string, value []byte) ([]NodeID, error)

	// EdgesByProperty returns all EdgeIDs that have an indexed entry for key=value.
	EdgesByProperty(key string, value []byte) ([]EdgeID, error)

	// QueryNodeIDs returns NodeIDs that satisfy the provided constraints.
	// Implementations should return deterministic ordering.
	QueryNodeIDs(query NodeQuery) ([]NodeID, error)

	// QueryEdgeIDs returns EdgeIDs that satisfy the provided constraints.
	// Implementations should return deterministic ordering.
	QueryEdgeIDs(query EdgeQuery) ([]EdgeID, error)

	// --- Lifecycle ---

	// NodeCount returns the total number of nodes in the store.
	NodeCount() (uint64, error)

	// EdgeCount returns the total number of edges in the store.
	EdgeCount() (uint64, error)

	// Close releases all resources held by the store.
	Close() error
}

// ReindexPolicy controls what happens to an entity's property-index entries
// when UpdateNode / UpdateEdge replaces its properties.
//
// The store cannot re-derive index entries itself: indexed values are supplied
// by the caller in the caller's own encoding (see IndexNodeProperty), and the
// Properties blob is opaque to the storage layer. So an update that is only
// given the new record cannot know which entries it invalidated, and every
// answer to that is wrong in some case. The policy chooses which way to be
// wrong — or, by default, refuses the question.
type ReindexPolicy uint8

const (
	// ReindexReject makes UpdateNode / UpdateEdge return
	// ErrIndexedPropertiesRequired when the entity carries index entries. It is
	// the default.
	//
	// Failure mode: none. It is the only option that cannot silently produce a
	// wrong answer, because it produces no answer. An entity with no entries
	// updates normally, so this costs nothing for the graphs it cannot hurt.
	//
	// Use UpdateNodeIndexed to replace the entries wholesale, or
	// UpdateNodePartialIndex to replace only the keys that changed. Both know
	// what the update did, which is the thing UpdateNode cannot.
	//
	// **This became the default in v0.5.0, replacing ReindexKeep.** The old
	// default was a silently wrong answer that required every caller to remember
	// a second method name, and a caller who forgot got a query result the
	// planner trusted. Set ReindexKeep explicitly to restore it.
	ReindexReject ReindexPolicy = iota

	// ReindexKeep leaves property-index entries untouched on update. This was
	// the default before v0.5.0.
	//
	// Failure mode: entries become STALE. A node whose indexed field changed is
	// still returned by NodesByProperty for its old value, and the query planner
	// trusts that answer. Callers must re-register changed fields themselves.
	ReindexKeep

	// ReindexPurge drops all property-index entries for the updated entity, so
	// the index can never report a value the entity no longer has.
	//
	// Failure mode: entries are LOST. Updating a node drops index entries that
	// were still accurate — including ones for fields the update did not touch —
	// so queries silently return fewer results until the caller re-registers.
	// This is not the safe option either; it is the other way of being wrong.
	//
	// Prefer UpdateNodeIndexed / UpdateEdgeIndexed, which purge and re-register
	// in one step and therefore avoid both failure modes.
	ReindexPurge
)

// String names the policy, for diagnostics.
func (p ReindexPolicy) String() string {
	switch p {
	case ReindexReject:
		return "reject"
	case ReindexKeep:
		return "keep"
	case ReindexPurge:
		return "purge"
	default:
		return "unknown"
	}
}

// Reindexer is an optional extension implemented by stores that support
// configuring how updates interact with the property index. Both bundled
// backends implement it.
type Reindexer interface {
	// SetReindexPolicy sets the policy for subsequent updates. It does not
	// retroactively affect entries already registered.
	SetReindexPolicy(p ReindexPolicy)

	// ReindexPolicy returns the currently configured policy.
	ReindexPolicy() ReindexPolicy

	// PurgeNodeIndex drops every property-index entry for id. On durable stores
	// the purge is written to the log so it survives a restart.
	PurgeNodeIndex(id NodeID) error

	// PurgeEdgeIndex drops every property-index entry for id.
	PurgeEdgeIndex(id EdgeID) error
}

// OrderedIndexDeclarer is an optional extension implemented by stores that can
// maintain an ordered index over a property key, so range and prefix filters on
// it are answered by binary search instead of by scanning the key's entries.
//
// Declaring a key changes how its range predicates compare. Undeclared keys use
// PropertyFilterMatches, which tries numeric comparison first and falls back to
// byte order; that rule is fine for evaluating one value at a time but is not a
// valid sort order, so an ordered index cannot be built on it (see
// index/encoding for a worked example). A declared key is therefore compared
// byte-wise throughout. Encode its values so byte order matches your intent —
// index/encoding provides order-preserving encoders — or use a naturally ordered
// form such as fixed-width zero-padded digits or hex.
//
// Equality lookups are unaffected: they go through the hash postings either way.
type OrderedIndexDeclarer interface {
	// DeclareOrderedNodeProperty builds and maintains an ordered index over a
	// node property key, absorbing entries already registered under it.
	DeclareOrderedNodeProperty(key string) error

	// DeclareOrderedEdgeProperty is the edge-property equivalent.
	DeclareOrderedEdgeProperty(key string) error

	OrderedIndexReporter
}

// OrderedIndexReporter is the read half of OrderedIndexDeclarer: which keys are
// declared, without the power to declare one.
//
// It is split out because a declaration belongs to the store while a view of the
// store does not get to change it, and a consumer that only wants to know —
// an export writing the declarations into a dump header, a report naming them —
// should be able to ask a Snapshot instead of having to hold the store as well.
// Every OrderedIndexDeclarer satisfies it.
type OrderedIndexReporter interface {
	// OrderedNodeProperties returns the declared node keys, sorted.
	OrderedNodeProperties() []string

	// OrderedEdgeProperties returns the declared edge keys, sorted.
	OrderedEdgeProperties() []string
}

// RefreshInfo reports what one Refresher.Refresh call did.
type RefreshInfo struct {
	// Epoch is the newest commit visible after the refresh.
	Epoch uint64

	// LogGeneration identifies the log file the reader is now reading. A change
	// between two refreshes means the writer compacted.
	LogGeneration uint64

	// Reloaded is true when the log had been replaced and the reader was rebuilt
	// from the image and the new log, rather than advanced over appended bytes.
	Reloaded bool

	// Bytes is how many bytes of the log this call applied.
	Bytes int64
}

// Advanced reports whether the refresh changed anything.
func (r RefreshInfo) Advanced() bool { return r.Reloaded || r.Bytes > 0 }

// Refresher is a store that can be advanced to whatever another process has
// since made durable.
//
// It is not what an ordinary read-only store does, and the distinction is a
// guarantee rather than a feature. A store opened read-only promises that no
// writer is running — that is what its shared process lock buys — and that
// promise is exactly what makes fixing its view at open the right behaviour. A
// reader that follows a writer cannot make it: a shared lock and the writer's
// exclusive lock cannot coexist. So a Refresher gives it up, takes no process
// lock, and offers this instead. The writer's exclusive lock is untouched.
//
// Implemented only by a disk store opened with disk.Options.LiveReader. The
// memory backend does not implement it, and correctly so: there is no second
// process to follow, and nothing for a refresh to read.
type Refresher interface {
	// Refresh advances the store to the newest durable state on disk.
	//
	// Safe to call concurrently with reads. A failed refresh leaves the store
	// serving exactly what it was serving before the call.
	Refresh() (RefreshInfo, error)

	// IsLiveReader reports whether this store can actually be advanced. A store
	// that answers false returns an error from Refresh rather than silently
	// succeeding, because a caller refreshing a store that cannot advance is
	// reading stale data believing it is fresh.
	IsLiveReader() bool
}

// CompositeIndexDeclarer is an optional extension implemented by stores that can
// build and maintain a composite index over several property keys, so that a
// query pinning all of them to values is answered by one lookup instead of by
// driving from the most selective of them and eliminating against the rest.
//
// The win is the conjunction that is far more selective than any of its parts: a
// case identifier with a hundred values and a bucket with ten, over a million
// nodes, is ten thousand candidates thinned to about a thousand. A composite
// over the pair returns the thousand directly.
//
// It is opt-in because it is not free. Every registration on a member key files
// the entity into the composite as well, and the composite holds that entity's
// values for each of its keys — so a declaration that no query matches costs
// memory and write time for nothing.
//
// A composite is used only when the query pins *every* one of its keys with an
// equality filter. The postings are keyed by the whole tuple, so there is no
// entry for a partially specified one — a declaration over three keys does
// nothing for a query that fixes two of them.
type CompositeIndexDeclarer interface {
	// DeclareCompositeNodeProperties builds and maintains a composite index over
	// the given node property keys, absorbing entries already registered under
	// them. Declaring the same tuple twice is a no-op.
	//
	// Order is part of a declaration's identity but not of its use: a query is
	// matched against the key set, so (a, b) serves a query filtering on b and a.
	//
	// Returns an error for a tuple that cannot be indexed — fewer than two keys,
	// a repeated key, an empty key, or more than 64.
	DeclareCompositeNodeProperties(keys []string) error

	// DeclareCompositeEdgeProperties is the edge-property equivalent.
	DeclareCompositeEdgeProperties(keys []string) error

	CompositeIndexReporter
}

// CompositeIndexReporter is the read half of CompositeIndexDeclarer, split out
// for the same reason OrderedIndexReporter is: a Snapshot can say what the store
// declared without being able to declare anything itself.
type CompositeIndexReporter interface {
	// CompositeNodeProperties returns the declared node key tuples, each in its
	// own declared order and the whole in a stable order.
	CompositeNodeProperties() [][]string

	// CompositeEdgeProperties returns the declared edge key tuples.
	CompositeEdgeProperties() [][]string
}

// UniqueIndexDeclarer is an optional extension implemented by stores that can
// enforce at most one live entity per value under a property key.
//
// Unlike the ordered and composite declarers, a store that does not implement
// this must not be treated as having silently accepted the declaration. Those
// two are optimisations: a store ignoring them answers every query correctly,
// only slower. This one is a constraint, and a store that quietly does not
// enforce it hands the caller exactly the guarantee they asked for and none of
// the behaviour — so Graph.DeclareUniqueProperty returns an error rather than
// nil when the backend lacks it.
type UniqueIndexDeclarer interface {
	// DeclareUniqueNodeProperty enforces uniqueness on a node property key from
	// this point on, after checking that the graph already satisfies it.
	//
	// Existing data is validated first, and a graph that violates the constraint
	// is reported through *UniqueViolationsError naming every offending value —
	// not the first — so it can be repaired in one pass. Nothing is declared in
	// that case.
	//
	// Declaring a key already declared is a no-op, so this is safe to call on
	// every Open. Declarations live in memory: like ordered keys, they must be
	// re-declared by the process that opens the store.
	DeclareUniqueNodeProperty(key string) error

	// DeclareUniqueEdgeProperty is the edge-property equivalent.
	DeclareUniqueEdgeProperty(key string) error

	// UniqueNodeProperties returns the declared unique node keys, sorted.
	UniqueNodeProperties() []string

	// UniqueEdgeProperties returns the declared unique edge keys, sorted.
	UniqueEdgeProperties() []string

	// UniqueNodeOwner returns the live node holding value under key.
	//
	// It reports an error when key is not declared unique, because the answer
	// would otherwise be "one of the nodes holding it", which is not a thing a
	// caller can act on. found is false when no live node holds the value.
	//
	// Liveness matters here and not only pedantically: a posting can outlive the
	// entity it names while a snapshot pins an older view, and an upsert that
	// reused a dead node's ID would resurrect it.
	UniqueNodeOwner(key string, value []byte) (id NodeID, found bool, err error)

	// UniqueEdgeOwner is UniqueNodeOwner for edges.
	UniqueEdgeOwner(key string, value []byte) (id EdgeID, found bool, err error)
}

// ErrKeyNotUnique is returned when an operation that needs a key to name at most
// one entity is given a key with no unique declaration.
var ErrKeyNotUnique = errors.New("graphene: property key is not declared unique")

// EdgeCardinalityDeclarer is an optional extension implemented by stores that
// can enforce at most one live edge of a type between any two nodes.
//
// It is the structural counterpart to UniqueIndexDeclarer, and it exists because
// that one does not scale to bulk edges: a unique property key costs an index
// entry per edge, which a caller writing millions of ownership edges cannot
// afford to spend restating what the endpoints already say. See
// store/edgeunique.go for why the constraint is per label rather than per label
// set.
//
// Like UniqueIndexDeclarer and unlike the ordered and composite declarers, this
// is a constraint rather than an optimisation. A store that quietly does not
// enforce it hands the caller the guarantee they asked for and none of the
// behaviour, so Graph.DeclareUniqueEdge returns an error rather than nil when
// the backend lacks it.
type EdgeCardinalityDeclarer interface {
	// DeclareUniqueEdgeType enforces at most one live edge of type t between any
	// ordered pair of nodes from this point on, after checking that the graph
	// already satisfies it.
	//
	// Existing data is validated first, and a graph that violates the constraint
	// is reported through *EdgeCardinalityViolationsError naming every offending
	// pair — not the first — so it can be repaired in one pass. Nothing is
	// declared in that case.
	//
	// Declaring a type already declared is a no-op, so this is safe to call on
	// every Open. Declarations live in memory: like unique property keys, they
	// must be re-declared by the process that opens the store.
	DeclareUniqueEdgeType(t EdgeType) error

	// UniqueEdgeTypes returns the declared types, sorted.
	UniqueEdgeTypes() []EdgeType

	// EdgeBetween returns a live edge from src to dst carrying any of edgeTypes.
	//
	// nil edgeTypes means any type. Which edge, when several match, is
	// unspecified unless the type is declared — the point of a declaration is
	// that the question has one answer. found is false when no live edge
	// matches, which is not an error.
	EdgeBetween(src, dst NodeID, edgeTypes []EdgeType) (id EdgeID, found bool, err error)
}

// Aggregator is an optional extension implemented by stores that can count
// without materialising what they count.
//
// The counts are the ones a caller cannot get cheaply from outside: a per-type
// breakdown means one NodesByType call per type and a slice per call, and a
// per-value breakdown means reading every entity under a key. Both are answered
// here from postings the store already maintains.
//
// Unlike the constraint declarers this is an optimisation, but there is no
// generic fallback to degrade to — a GraphStore cannot enumerate its own types —
// so Graph reports an error rather than a wrong answer on a backend without it.
// Both bundled backends implement it.
type Aggregator interface {
	// CountNodesByType returns the number of live nodes carrying each type.
	//
	// A node carrying two labels is counted under both, so the values sum to
	// more than NodeCount on any graph using multi-label nodes. Types with no
	// live nodes are absent rather than zero.
	CountNodesByType(ctx context.Context) (map[NodeType]uint64, error)

	// CountEdgesByType is CountNodesByType for edges, with the same
	// multi-label caveat.
	CountEdgesByType(ctx context.Context) (map[EdgeType]uint64, error)

	// CountNodesByProperty returns the number of live nodes holding each
	// distinct value under key.
	//
	// Only entities with an index entry under key are counted, which is the
	// useful reading: the question is "how do the values of this indexed field
	// distribute", and an entity that never registered one has no value to
	// distribute. Values are the caller-encoded bytes as stored, keyed by their
	// string form.
	CountNodesByProperty(ctx context.Context, key string) (map[string]uint64, error)

	// CountEdgesByProperty is CountNodesByProperty for edges.
	CountEdgesByProperty(ctx context.Context, key string) (map[string]uint64, error)
}

// PropertyBatcher is an optional extension implemented by stores that can
// resolve many exact property values in one pass.
//
// It exists for one shape of work: joining an external set of values against an
// indexed key — a million content digests against the nodes holding them —
// where calling NodesByProperty once per value is correct but pays a search of
// the whole key for each. A backend whose index is disk-resident can do far
// better by resolving them in sorted order, because then each search begins where
// the last one ended and the pass sweeps the index forward instead of probing it
// at random.
//
// Unlike Aggregator there *is* a correct fallback — NodesByProperty in a loop
// — which is why this is not on GraphStore. A caller that wants it unconditionally
// should ask for it and loop when it is absent, and Graph.NodesByPropertyBatch
// does exactly that.
type PropertyBatcher interface {
	// NodesByPropertyBatch resolves values against key, calling fn once for each
	// value that has at least one live node.
	//
	// fn receives the index of the value within values, not the value, and the id
	// slice belongs to the store for the duration of the call: read it, do not
	// retain it. Returning false ends the batch without an error.
	//
	// Three things are deliberately not promised. The callback order is not the
	// input order — resolving in sorted order is the entire point, and which
	// value a result belongs to is answered by i. Values with no live holder
	// produce no callback, so a caller counting matches counts calls rather than
	// assuming one per value. And values is not modified: an implementation that
	// needs an ordering builds its own.
	//
	// The result is the same set of (value, ids) pairs NodesByProperty would give
	// for each value, and is subject to the same liveness caveat: postings are
	// resolved against the records, so every id handed over was live when it was
	// checked and may be deleted the moment the batch returns.
	NodesByPropertyBatch(ctx context.Context, key string, values [][]byte,
		fn func(i int, ids []NodeID) bool) error

	// EdgesByPropertyBatch is NodesByPropertyBatch for edges.
	EdgesByPropertyBatch(ctx context.Context, key string, values [][]byte,
		fn func(i int, ids []EdgeID) bool) error
}

// IndexVerifier is an optional extension implemented by stores that can
// self-check their indexes against the records those indexes describe.
type IndexVerifier interface {
	// VerifyIndexes reports the first structural inconsistency it finds between
	// the store's indexes and its live records, or nil if everything agrees.
	VerifyIndexes() error
}

// IndexRebuilder is an optional extension implemented by stores that can
// reconstruct their derived indexes from the records they hold.
type IndexRebuilder interface {
	// RebuildIndexes discards and recomputes every index that is derivable from
	// the stored records — label postings and adjacency — and drops property
	// index entries whose entity no longer exists.
	//
	// It cannot recreate property entries themselves: those values come from the
	// caller and are not recoverable from the records. RebuildIndexes therefore
	// repairs structure, not content.
	RebuildIndexes() error
}

// IndexVerifierCtx is IndexVerifier for a store whose check can be abandoned.
//
// Separate interfaces rather than one with both methods, matching how every
// other capability here is discovered: a backend that can verify but not
// cancel stays usable through IndexVerifier, and nothing that already
// implements it has to grow a method to keep working.
type IndexVerifierCtx interface {
	// VerifyIndexesCtx is VerifyIndexes, abandoned if ctx is cancelled. It
	// reports ctx.Err() in that case, which is distinguishable from an
	// inconsistency: a cancelled check found nothing, it stopped looking.
	VerifyIndexesCtx(ctx context.Context) error
}

// IndexRebuilderCtx is IndexRebuilder for a store whose repair can be
// abandoned.
type IndexRebuilderCtx interface {
	// RebuildIndexesCtx is RebuildIndexes, abandoned if ctx is cancelled where
	// abandoning it is safe.
	//
	// That qualification is the whole contract. A rebuild replaces derived state
	// wholesale, and half a replacement is an index that omits records it should
	// name — the failure that costs a query result rather than a query. An
	// implementation must therefore either finish the structural rebuild or not
	// begin it, and may only honour a cancellation at points where what it
	// leaves behind is a superset of the truth. Both bundled backends cancel
	// before the rebuild and during the sweep that follows it, and nowhere in
	// between.
	//
	// A cancelled rebuild is one to run again. It leaves the store no worse than
	// it found it — the sweep it interrupted was removing entries that were
	// already there — but it does not leave it repaired, and VerifyIndexes will
	// still report whatever the sweep did not reach.
	RebuildIndexesCtx(ctx context.Context) error
}

// QuerierCtx is an optional extension implemented by stores whose queries can
// be abandoned part way.
//
// Worth having separately from the traversal budgets: a query is not a walk,
// it has no frontier to bound, and what makes it worth stopping is usually not
// its own cost but the read lock it holds while a writer waits.
type QuerierCtx interface {
	// QueryNodeIDsCtx is QueryNodeIDs, abandoned if ctx is cancelled.
	//
	// Nothing partial is returned with the error. A query stopped part way holds
	// a candidate set that some filters have been applied to and others have
	// not — a superset of the answer that is shaped exactly like the answer, and
	// the one result worse than none.
	QueryNodeIDsCtx(ctx context.Context, query NodeQuery) ([]NodeID, error)

	// QueryEdgeIDsCtx is QueryEdgeIDs, abandoned if ctx is cancelled.
	QueryEdgeIDsCtx(ctx context.Context, query EdgeQuery) ([]EdgeID, error)
}

// PropertyEnumerator is an optional extension implemented by stores that can
// walk every indexed property entry.
//
// It exists for bulk export, and it exists as its own capability because those
// entries cannot be recovered from anything else the store exposes. A node's
// Properties blob is opaque to the engine — the values in the index were handed
// to IndexNodeProperty separately, by a caller who knew how to derive them — so
// a dump that carried only the records would restore a graph whose queries
// answered nothing. RebuildIndexes says the same thing from the other side: it
// repairs structure, not content.
type PropertyEnumerator interface {
	// ForEachNodeProperty calls fn for every indexed (id, key, value) triple.
	// Return false from fn to stop early.
	//
	// The value slice belongs to the store: read it, do not retain or mutate it.
	// A caller keeping one must copy it, which is what a bulk export does as it
	// encodes.
	ForEachNodeProperty(fn func(id NodeID, key string, value []byte) bool)

	// ForEachEdgeProperty is ForEachNodeProperty for edge properties.
	ForEachEdgeProperty(fn func(id EdgeID, key string, value []byte) bool)
}

// AdjacencyReader is an optional extension for allocation-free traversal.
//
// Neighbours and EdgesOf each allocate a result slice (and Neighbours a dedupe
// map) on every call, which is what makes a k-hop walk allocate proportionally
// to the number of nodes it visits. These methods let a traversal reuse one
// buffer across the whole walk and materialise a *Node or *Edge only when it
// actually needs one.
//
// Both bundled backends implement it; callers must type-assert and fall back to
// EdgesOf / Neighbours when a backend does not.
type AdjacencyReader interface {
	// IncidentEdges appends one entry per edge incident to id in the given
	// direction, filtered by edgeTypes (nil means all types), to dst and returns
	// the extended slice. Passing a reused dst (sliced to :0) makes repeated
	// calls allocation-free.
	//
	// Each entry carries the node at the far end as well as the edge, because
	// the store already has the edge record in hand while filtering. Resolving
	// the neighbour separately would mean re-acquiring the store lock once per
	// incident edge, which costs more than the allocation it saves.
	//
	// Entries are produced in the same order EdgesOf returns the corresponding
	// edges, so a caller can switch between the two without changing results.
	// Returns *ErrNotFound on backends whose EdgesOf does so for a missing node.
	IncidentEdges(dst []IncidentEdge, id NodeID, dir Direction, edgeTypes []EdgeType) ([]IncidentEdge, error)

	// NodeExists reports whether a node is live, without materialising it.
	NodeExists(id NodeID) bool
}

// IncidentEdge is one step out of a node: the edge, the node it leads to, and
// the edge's weight. For a self-loop, Neighbour is the node itself.
type IncidentEdge struct {
	Edge      EdgeID
	Neighbour NodeID

	// Weight is the edge record's Weight, carried here for the same reason
	// Neighbour is: the store has the edge in hand while filtering, so copying
	// a float32 out costs nothing, while a weighted traversal that had to fetch
	// it would pay a GetEdge per relaxation.
	//
	// It keeps Edge.Weight's meaning — a similarity score for EdgeTypeSimilarTo,
	// zero otherwise — and is not a traversal cost. Turning one into the other
	// is the caller's job, through an EdgeCost.
	Weight float32
}

// EdgeCost turns one incident step into a traversal cost. Lower is cheaper.
//
// It exists because a graph carries no single notion of distance: the same
// edges are a hop count to one caller, a similarity gap to another, and a
// latency to a third. Rather than reinterpret Edge.Weight — which is documented
// as a similarity score and encoded as one in the Merkle leaf — a weighted
// walk takes the reading from its caller.
//
// The contract is what Dijkstra needs to be correct:
//
//   - Non-negative. A negative cost makes a settled node re-openable and the
//     algorithm simply wrong; it is refused rather than silently mishandled.
//   - Deterministic. Two calls on the same step return the same cost, or the
//     path reported is not the path that was costed.
//   - Cheap. It runs once per incident edge examined, on the inner loop.
//
// A cost needing more than the weight can materialise the edge inside the
// selector, but that is a store read per relaxation and will dominate the walk.
// The common readings need no such thing: a hop count is
//
//	func(store.IncidentEdge) float64 { return 1 }
//
// and a similarity gap is
//
//	func(e store.IncidentEdge) float64 { return 1 - float64(e.Weight) }
type EdgeCost func(IncidentEdge) float64

// NodeHeuristic estimates the remaining cost from a node to the destination,
// for a heuristic-guided search. Lower means closer.
//
// It must be non-negative, and it must never overestimate the true remaining
// cost — an estimate that is too large makes the search skip the cheapest path
// and return a more expensive one, silently. It should also be consistent
// (h(a) <= cost(a,b) + h(b)), which is what allows a node to be settled once
// and never revisited; an admissible but inconsistent estimate can still
// return a suboptimal path under that rule.
//
// Returning 0 for every node is always valid and reduces the search to plain
// Dijkstra, which is the right fallback when no estimate is available.
type NodeHeuristic func(NodeID) float64

// DegreeCounter is an optional extension implemented by stores that can count
// incident edges without materialising them. Callers should type-assert against
// it and fall back to len(EdgesOf(...)) when it is not implemented.
//
// It is deliberately not part of GraphStore so that third-party implementations
// remain valid without change.
type DegreeCounter interface {
	// DegreeOf returns the number of edges incident to id in the given
	// direction, counting only edges carrying one of edgeTypes (nil means all
	// types). Returns *ErrNotFound if the node does not exist.
	DegreeOf(id NodeID, dir Direction, edgeTypes []EdgeType) (int, error)
}

// ErrNotFound is returned when a requested node or edge does not exist.
type ErrNotFound struct {
	Kind string // "node" or "edge"
	ID   uint64
}

func (e *ErrNotFound) Error() string {
	return e.Kind + " not found: id=" + uint64ToStr(e.ID)
}

// ErrInvalidEdge is returned when AddEdge references non-existent src or dst nodes.
type ErrInvalidEdge struct {
	MissingID NodeID
}

func (e *ErrInvalidEdge) Error() string {
	return "edge references non-existent node: id=" + uint64ToStr(uint64(e.MissingID))
}

// uint64ToStr is a zero-dependency uint64 → decimal string conversion.
func uint64ToStr(v uint64) string {
	if v == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[pos:])
}

// NodeQueryExplainer is implemented by stores that can report how they would
// resolve a node query. It is optional: Graph.ExplainNodeQuery reports an error
// for a store that does not implement it.
type NodeQueryExplainer interface {
	ExplainNodeQuery(query NodeQuery) (QueryPlan, error)
}

// EdgeQueryExplainer is NodeQueryExplainer for edge queries.
type EdgeQueryExplainer interface {
	ExplainEdgeQuery(query EdgeQuery) (QueryPlan, error)
}

// BatchReader is implemented by stores that can resolve many IDs under a single
// lock hold, rather than one acquisition per ID.
//
// Both methods preserve the order of ids in found: a resolved entity appears at
// the same relative position it was requested in, with missing ids removed
// rather than left as nil holes. Callers needing to correlate a record back to a
// requested id can read the record's own ID field.
//
// Duplicate ids resolve independently — a duplicate that exists appears once per
// occurrence, and one that does not appears once per occurrence in missing.
type BatchReader interface {
	GetNodesBatch(ids []NodeID) (found []*Node, missing []NodeID)
	GetEdgesBatch(ids []EdgeID) (found []*Edge, missing []EdgeID)
}

// Transactor is implemented by stores that can apply a mixed set of nodes and
// edges as one atomic unit.
//
// This exists because the slice APIs cannot express it. AddNodes followed by
// AddEdges is two transactions, so a crash between them leaves a graph whose
// nodes arrived and whose edges did not — the exact shape ingest produces, and
// the exact shape a transaction is for.
//
// IDs are reserved before commit so a transaction can wire an edge to a node it
// has added but not yet committed. A reserved ID that is never committed is
// simply never used: IDs are monotonic and never reused, but have never been
// promised to be dense (see AddNodesBatch, which already burns IDs on WAL
// failure).
type Transactor interface {
	// ReserveNodeID and ReserveEdgeID hand out an ID that no other writer will
	// receive. They do not create anything.
	ReserveNodeID() NodeID
	ReserveEdgeID() EdgeID

	// ApplyTransaction commits every operation together, or none of them.
	//
	// Operations apply **in the order given**. Each is evaluated against the
	// store as modified by the operations before it, so a transaction may add a
	// node and then delete it, or delete an edge and add a replacement.
	//
	// Records passed in must already carry an ID and must be owned by the store
	// (the caller must not retain or mutate them).
	//
	// On error nothing is applied and the store is unchanged.
	ApplyTransaction(ops []TxOp) error
}

// TxContext names who is making a change, so a committed transaction records
// more than what changed.
//
// The engine stores these values and binds them to the commit. It does not
// authenticate them, and it cannot: Graphene is a library linked into the
// caller's process, so whatever the caller supplies here is asserted rather
// than proven. Treat the result as attribution and audit, not as a security
// boundary — a boundary needs a process boundary, which this engine does not
// have.
//
// The zero value means "unattributed", which is what every write made through
// the plain APIs carries.
type TxContext struct {
	// ActorID identifies the principal responsible for the change. Its meaning
	// is the caller's to define; the engine only records it.
	ActorID uint64

	// RoleID is the role the actor was acting under. There is no role model in
	// the engine — nothing is checked against this — so it exists to make a
	// later RBAC layer's decisions reconstructible from the log.
	RoleID uint32

	// KeyID names the signing key that should cover this commit. Nothing is
	// signed yet; the field is carried so that when signing lands it does not
	// require another change to the commit record's layout.
	KeyID uint64
}

// Unattributed reports whether c carries no actor information.
func (c TxContext) Unattributed() bool {
	return c.ActorID == 0 && c.RoleID == 0 && c.KeyID == 0
}

// Signer signs commit metadata so a committed transaction carries evidence of
// who produced it, not merely a claim.
//
// The engine never generates, stores, or rotates a key. It signs with what the
// caller hands it and verifies with what the caller hands it. That is not
// laziness about key management — it is the only honest arrangement for a
// library linked into someone else's process. A key the engine held would live
// in the caller's address space anyway, so custody is the caller's whatever the
// API pretends, and pretending otherwise would be the more dangerous design.
//
// Consequently: a signature proves the holder of a key signed these bytes. It
// does not prove the process was not compromised, because a compromised process
// holding the key signs whatever it likes. What signing buys over the digest is
// that an attacker who edits the log must also possess the key — see the plan's
// §11.1 for where that boundary sits.
type Signer interface {
	// KeyID identifies the key, so a verifier can select the right public key
	// and so a rotation is reconstructible from the log.
	KeyID() uint64

	// Sign returns a signature over data. It must be deterministic for a given
	// key and input — Ed25519 is, which is why it is the recommended scheme;
	// a non-deterministic scheme still verifies but makes golden-vector tests
	// impossible.
	Sign(data []byte) ([]byte, error)
}

// Verifier checks signatures produced by a Signer.
//
// Separate from Signer because the two are held by different parties in every
// arrangement that matters: the writer signs, and a reader — possibly on another
// machine, possibly years later — verifies. A verifier that could sign would be
// a verifier holding the private key.
type Verifier interface {
	// Verify reports whether sig is a valid signature over data for keyID.
	//
	// Returning an error rather than a bool for the unknown-key case matters:
	// "this key is not one I know" and "this signature is forged" call for
	// different responses, and collapsing them loses that.
	Verify(keyID uint64, data, sig []byte) error
}

// ActorTransactor is implemented by stores that can record who committed a
// transaction, alongside when it was committed.
//
// Separate from Transactor rather than folded into it, following the same rule
// as every other optional interface here: a third-party store that already
// satisfies Transactor keeps working untouched, and Graph degrades to the
// unattributed path when this is absent.
type ActorTransactor interface {
	Transactor

	// ApplyTransactionAs is ApplyTransaction, recording ctx against the commit.
	// ApplyTransaction is equivalent to passing the zero TxContext.
	ApplyTransactionAs(ops []TxOp, ctx TxContext) error
}

// StorageStats reports what a backend is currently holding, so an operator can
// see when it needs attention.
//
// The counts that matter operationally are the delta ones. Everything written
// since the last compaction lives in memory and is replayed from the log at
// every open, so unbounded delta growth degrades memory, open time, and read
// speed together — with no symptom until someone looks. Nothing in the engine
// triggers a compaction, which makes this the input a caller needs in order to
// decide.
type StorageStats struct {
	// Delta layer: written since the last compaction, resident in memory.
	DeltaNodes   int
	DeltaEdges   int
	DeletedNodes int // tombstones masking CSR records
	DeletedEdges int

	// The compacted image underneath.
	CSRNodes int
	CSREdges int

	// PropertyEntries counts indexed (id, key, value) triples across both.
	PropertyNodeEntries int
	PropertyEdgeEntries int

	// WALBytes is the log's size on disk. It is bounded only by compaction, so
	// it is also the best proxy for how long the next open will take.
	WALBytes int64

	// CommitSeq is the last commit sequence number issued. It is durable across
	// compaction: v8 carries a high-water mark in the CSR header, so the counter
	// survives the log truncation that used to reset it and names a commit for
	// the life of the store rather than for the life of one WAL generation.
	CommitSeq uint64

	// VisibleEpoch is the newest version of the graph a reader can see. It
	// orders mutations within one open store and means nothing across
	// processes; a Snapshot's Epoch is a value this counter once had.
	VisibleEpoch uint64

	// OpenSnapshots is how many snapshots are currently held, and
	// OldestSnapshotEpoch the epoch the oldest of them reads at (zero when there
	// are none).
	//
	// These are the figures that separate "the delta is genuinely large" from
	// "something is holding an old view open". A snapshot pins the image it was
	// taken against and every version written since, so a store whose memory
	// will not come down after a compaction is usually explained here.
	OpenSnapshots       int
	OldestSnapshotEpoch uint64

	// LastCompact is when Compact last completed, zero if it has not run in this
	// process. It is not persisted, so a reopened store reports zero even if the
	// image on disk was compacted moments earlier.
	LastCompact time.Time

	// HighestNodeID and HighestEdgeID are the largest identifiers the store has
	// ever *issued*, not the largest it still holds. Identifiers are never
	// reused and compaction preserves them, so these only ever rise — deleting
	// the record that held the maximum does not lower them, and reopening the
	// store does not either, because the marks are persisted in the image header.
	//
	// They are the numerator of the headroom below, and the reason a store has a
	// finite lifetime measured in identifiers rather than in records.
	HighestNodeID uint64
	HighestEdgeID uint64

	// IDCeiling is the highest identifier the storage format can address, per
	// kind. Zero from a backend with no such limit.
	IDCeiling uint64

	// NodeIDHeadroom and EdgeIDHeadroom are the fraction of the identifier space
	// still unissued, from 1.0 on a new store to 0.0 at the ceiling. Below the
	// ceiling the store keeps working exactly as it did; at it, writes fail and
	// the remedy is an export and import into a fresh store, which is a new
	// store with new identifiers rather than a renumbering of this one.
	//
	// This is the figure to alarm on, because it moves with identifiers issued -
	// a workload that rebuilds a derived layer burns them far faster than its
	// record count suggests, and nothing else in these statistics shows that.
	NodeIDHeadroom float64
	EdgeIDHeadroom float64

	// ImageMode names how the backend is holding its compacted image: "mapped"
	// when the image file is mapped and the property blobs a read returns
	// address it, "heap" when the image was copied in, and "" from a backend
	// that has no image at all.
	//
	// It is here because it is the difference between a store whose resident
	// memory includes its blobs and one whose does not, and because it is how a
	// caller confirms that asking for a mapping got one — a fallback is always
	// available, always correct, and always more expensive.
	ImageMode string

	// ImageMappedBytes is how much of the image is mapped rather than resident,
	// zero when it is not mapped. These bytes are page cache the kernel may
	// evict, so they are not part of what the process must keep.
	ImageMappedBytes int64

	// IndexMode names where the property index is: "mapped" when it is read out
	// of the image, "resident" when it is held in the heap, and "" from a backend
	// that has no image to read one from.
	//
	// It is the answer rather than the request, which matters because the two can
	// differ: a store configured for a mapped index reports "resident" when the
	// image it opened carries no such index, or when the image itself is in the
	// heap. The difference is most of what a store holds — a resident index costs
	// about a hundred bytes per indexed entry and a mapped one costs a fence per
	// key — so it is here rather than only in a metric a caller has to subscribe
	// to.
	IndexMode string
}

// DeltaRecords is the total number of records held in memory since the last
// compaction.
func (s StorageStats) DeltaRecords() int {
	return s.DeltaNodes + s.DeltaEdges + s.DeletedNodes + s.DeletedEdges
}

// CSRRecords is the total number of records in the compacted image.
func (s StorageStats) CSRRecords() int { return s.CSRNodes + s.CSREdges }

// StorageReporter is implemented by stores that can describe their own storage
// state. The in-memory backend does not: it has no delta, no log, and no
// compaction, so there is nothing for it to report.
type StorageReporter interface {
	StorageStats() StorageStats
}

// CompactionPolicy describes when a store is due for compaction.
//
// It is advisory. Evaluating it changes nothing — the caller decides whether to
// act, which is deliberate: compaction rebuilds the whole image and its cost is
// the caller's to schedule. See Graph.ShouldCompact.
//
// A zero field disables that rule. A zero policy therefore never fires.
type CompactionPolicy struct {
	// MaxDeltaRecords fires when the in-memory delta exceeds this many records.
	MaxDeltaRecords int

	// MaxWALBytes fires when the log grows past this size.
	MaxWALBytes int64

	// MaxDeltaRatio fires when delta records exceed this fraction of the
	// compacted image — the rule that catches a small store churning heavily,
	// which MaxDeltaRecords alone would miss.
	MaxDeltaRatio float64
}

// DefaultCompactionPolicy returns a starting point, not a tuned
// recommendation.
//
// These numbers are not derived from measurement, and CONTRIBUTING.md is clear
// about what an unmeasured number is worth. They are set where an operator
// would probably rather be told too early than too late: a delta of 100k
// records or a 256 MB log are both well inside what the engine handles, and
// both are far past the point where a compaction would have been cheap.
// Callers with a measured workload should replace them.
func DefaultCompactionPolicy() CompactionPolicy {
	return CompactionPolicy{
		MaxDeltaRecords: 100_000,
		MaxWALBytes:     256 << 20,
		MaxDeltaRatio:   0.5,
	}
}

// Evaluate reports whether stats breach the policy, and which rule fired.
//
// The reason is part of the result rather than something the caller reconstructs:
// "compact now" is not actionable on its own, and the three rules fire for
// genuinely different reasons.
func (p CompactionPolicy) Evaluate(s StorageStats) (bool, string) {
	delta := s.DeltaRecords()

	if p.MaxDeltaRecords > 0 && delta >= p.MaxDeltaRecords {
		return true, fmt.Sprintf("delta holds %d records, at or past the %d limit",
			delta, p.MaxDeltaRecords)
	}
	if p.MaxWALBytes > 0 && s.WALBytes >= p.MaxWALBytes {
		return true, fmt.Sprintf("write-ahead log is %d bytes, at or past the %d limit",
			s.WALBytes, p.MaxWALBytes)
	}
	// Ratio is meaningless against an empty image: every first write would breach
	// it. MaxDeltaRecords is the rule that covers a store with no CSR yet.
	if p.MaxDeltaRatio > 0 && s.CSRRecords() > 0 {
		ratio := float64(delta) / float64(s.CSRRecords())
		if ratio >= p.MaxDeltaRatio {
			return true, fmt.Sprintf("delta is %.0f%% of the compacted image, at or past the %.0f%% limit",
				ratio*100, p.MaxDeltaRatio*100)
		}
	}
	return false, ""
}

// CompactionObserver is told the outcome of each background compaction.
//
// A compaction the engine started has nowhere to report to — the library has no
// logger and returns no error to anyone — so without an observer a failing one
// fails silently and repeats on the next tick. Nil is the default and costs
// nothing.
//
// Implementations are called from the background goroutine and must not block
// on the store.
type CompactionObserver interface {
	// Compacted reports the policy rule that fired and what the compaction
	// returned; err is nil when it succeeded.
	Compacted(reason string, err error)
}

// CompactionObserverFunc adapts a plain function to CompactionObserver.
//
// The interface exists rather than a bare func field because the options struct
// carrying it is a comparable value, and a struct with a func field is not.
type CompactionObserverFunc func(reason string, err error)

// Compacted calls f.
func (f CompactionObserverFunc) Compacted(reason string, err error) { f(reason, err) }

// TxOpKind identifies which operation a TxOp carries.
type TxOpKind uint8

const (
	TxOpAddNode TxOpKind = iota + 1
	TxOpAddEdge
	TxOpUpdateNode
	TxOpUpdateEdge
	TxOpDeleteNode
	TxOpDeleteEdge
	TxOpIndexNode
	TxOpIndexEdge
	TxOpUpsertNode
	TxOpUpsertEdge
	TxOpUpdateNodeIndexed
	TxOpUpdateEdgeIndexed
)

func (k TxOpKind) String() string {
	switch k {
	case TxOpAddNode:
		return "add-node"
	case TxOpAddEdge:
		return "add-edge"
	case TxOpUpdateNode:
		return "update-node"
	case TxOpUpdateEdge:
		return "update-edge"
	case TxOpDeleteNode:
		return "delete-node"
	case TxOpDeleteEdge:
		return "delete-edge"
	case TxOpIndexNode:
		return "index-node"
	case TxOpIndexEdge:
		return "index-edge"
	case TxOpUpsertNode:
		return "upsert-node"
	case TxOpUpsertEdge:
		return "upsert-edge"
	case TxOpUpdateNodeIndexed:
		return "update-node-indexed"
	case TxOpUpdateEdgeIndexed:
		return "update-edge-indexed"
	default:
		return "unknown"
	}
}

// TxOp is one buffered operation in a transaction.
//
// Exactly one of the payload fields is meaningful, selected by Kind: Node for
// the node operations, Edge for the edge operations, NodeID/EdgeID for deletes
// and for the index operations, which additionally carry Props.
type TxOp struct {
	Kind   TxOpKind
	Node   *Node
	Edge   *Edge
	NodeID NodeID
	EdgeID EdgeID

	// Props carries the property-index entries for TxOpIndexNode and
	// TxOpIndexEdge, registered against NodeID or EdgeID respectively.
	//
	// Entries are applied in sorted key order rather than map order. That is not
	// a cosmetic choice: the operations are framed into the log in the order
	// they are applied, and map iteration is randomised, so two identical
	// transactions would otherwise produce different bytes.
	Props map[string][]byte

	// Key and Value are the declared-unique property and the value identifying
	// the entity, for TxOpUpsertNode and TxOpUpsertEdge.
	Key   string
	Value []byte

	// Expect is who held Key=Value when the operation was buffered — the
	// entity's own ID for an update, InvalidNodeID / InvalidEdgeID for a create.
	//
	// It exists because an upsert has to hand back an ID immediately, so that
	// the caller can name it in the edges it buffers next, which means resolving
	// the key before the write lock is taken. Re-reading the key under the lock
	// and comparing against this is what turns that early read from an
	// assumption into a checked one: a mismatch means another writer took the
	// key in between, and the transaction is refused with ErrWriteConflict
	// rather than quietly creating a second entity.
	Expect uint64

	// Partial selects how TxOpUpdateNodeIndexed and TxOpUpdateEdgeIndexed treat
	// keys Props does not mention: false replaces the entity's whole entry set
	// with Props, true leaves unmentioned keys as they are.
	//
	// The two exist because both are honest answers to different questions.
	// "Here is everything this entity is indexed under" is what a re-ingest
	// knows; "this one field moved" is what a state transition knows, and making
	// the second caller enumerate the first's answer is how an untouched key
	// gets dropped by accident.
	Partial bool
}

// Syncer is implemented by stores that can force pending writes to durable
// storage. Backends without durability (the in-memory store) do not implement
// it, and Graph.Sync is a no-op for them.
type Syncer interface {
	Sync() error
}
