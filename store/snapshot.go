package store

// Consistent reads.
//
// A plain GraphStore read is point-in-time: it answers from whatever the store
// holds at the instant it runs, and two reads in a row can disagree if a writer
// runs between them. That is fine for a lookup and wrong for anything that
// reads more than once — a traversal following an edge to a node another
// goroutine has just deleted, a report whose totals do not add up because the
// graph moved under it, a verification pass comparing an index against records
// that changed while it walked them.
//
// A Snapshot fixes the answer. Every read through one sees the graph as it
// stood when the snapshot was taken, for as long as it is held.

// GraphReader is everything that reads a graph and nothing that writes one.
//
// It exists so a traversal, a query or a report can be handed either a live
// store or a Snapshot and not care which. Both satisfy it structurally; nothing
// has to be declared or adapted. That is the whole point of shaping Snapshot as
// the read half of GraphStore rather than as a new API — isolation becomes a
// property of the value passed in, not a second set of functions to write.
type GraphReader interface {
	GetNode(id NodeID) (*Node, error)
	GetEdge(id EdgeID) (*Edge, error)

	Neighbours(id NodeID, dir Direction, edgeTypes []EdgeType) ([]NeighbourResult, error)
	EdgesOf(id NodeID, dir Direction, edgeTypes []EdgeType) ([]*Edge, error)

	NodesByType(t NodeType) ([]NodeID, error)
	EdgesByType(t EdgeType) ([]EdgeID, error)

	NodesByProperty(key string, value []byte) ([]NodeID, error)
	EdgesByProperty(key string, value []byte) ([]EdgeID, error)

	QueryNodeIDs(q NodeQuery) ([]NodeID, error)
	QueryEdgeIDs(q EdgeQuery) ([]EdgeID, error)

	NodeCount() (uint64, error)
	EdgeCount() (uint64, error)
}

// Snapshotter is an optional extension implemented by stores that can hand out
// a fixed read view. Both bundled backends implement it.
type Snapshotter interface {
	// Snapshot returns a consistent read view of the store as it stands now.
	//
	// The caller must Close it. A snapshot holds the state it was taken against
	// alive; an abandoned one keeps memory the store could otherwise release.
	Snapshot() (Snapshot, error)
}

// Snapshot is a fixed read view of a graph.
//
// Reads through one are safe from multiple goroutines. Close is idempotent.
//
// # What is fixed and what is not
//
// The records are fixed: nodes, edges, adjacency, labels, and everything
// derived from them. The property index is not — it is a live structure shared
// with the store — so NodesByProperty and EdgesByProperty resolve the current
// postings against the pinned records. An ID whose node did not exist at this
// epoch, or that has since been deleted, is dropped. An entry registered after
// the snapshot was taken, for a node that already existed, is the one case this
// cannot exclude. See the method comments.
type Snapshot interface {
	GraphReader

	// Epoch names the version of the graph this snapshot reads. Two snapshots
	// with the same Epoch over the same store see the same graph. It is
	// monotonic within one open store and means nothing across processes.
	Epoch() uint64

	// Close releases the state the snapshot pins. Idempotent.
	Close() error
}
