package store

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
// Properties blob is opaque to the storage layer. So the only two honest
// options are to keep the old entries or to drop them, and each has a failure
// mode the caller has to choose between.
type ReindexPolicy uint8

const (
	// ReindexKeep leaves property-index entries untouched on update. This is the
	// default and preserves historical behaviour.
	//
	// Failure mode: entries become STALE. A node whose indexed field changed is
	// still returned by NodesByProperty for its old value, and the query planner
	// trusts that answer. Callers must re-register changed fields themselves.
	ReindexKeep ReindexPolicy = iota

	// ReindexPurge drops all property-index entries for the updated entity, so
	// the index can never report a value the entity no longer has.
	//
	// Failure mode: entries are LOST. Updating a node drops index entries that
	// were still accurate — including ones for fields the update did not touch —
	// so queries silently return fewer results until the caller re-registers.
	//
	// Prefer UpdateNodeIndexed / UpdateEdgeIndexed, which purge and re-register
	// in one step and therefore avoid both failure modes.
	ReindexPurge
)

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

	// OrderedNodeProperties returns the declared node keys, sorted.
	OrderedNodeProperties() []string

	// OrderedEdgeProperties returns the declared edge keys, sorted.
	OrderedEdgeProperties() []string
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

// IncidentEdge is one step out of a node: the edge and the node it leads to.
// For a self-loop, Neighbour is the node itself.
type IncidentEdge struct {
	Edge      EdgeID
	Neighbour NodeID
}

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

// TxOpKind identifies which operation a TxOp carries.
type TxOpKind uint8

const (
	TxOpAddNode TxOpKind = iota + 1
	TxOpAddEdge
	TxOpUpdateNode
	TxOpUpdateEdge
	TxOpDeleteNode
	TxOpDeleteEdge
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
	default:
		return "unknown"
	}
}

// TxOp is one buffered operation in a transaction.
//
// Exactly one of the payload fields is meaningful, selected by Kind: Node for
// the node operations, Edge for the edge operations, NodeID/EdgeID for deletes.
type TxOp struct {
	Kind   TxOpKind
	Node   *Node
	Edge   *Edge
	NodeID NodeID
	EdgeID EdgeID
}

// Syncer is implemented by stores that can force pending writes to durable
// storage. Backends without durability (the in-memory store) do not implement
// it, and Graph.Sync is a no-op for them.
type Syncer interface {
	Sync() error
}
