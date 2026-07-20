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
	// IndexNodeProperty are caller-encoded and decoupled from Properties, so
	// callers that change indexed fields should re-index explicitly.
	UpdateNode(n *Node) error

	// UpdateEdge replaces the Labels, Weight and Properties of the edge
	// identified by e.ID. The endpoints (Src/Dst) are immutable: any Src/Dst
	// set on e is ignored. Returns *ErrNotFound if the edge does not exist and
	// requires at least one label. To reconnect an edge, delete it and add a
	// new one.
	//
	// As with UpdateNode, the property index is left untouched.
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
