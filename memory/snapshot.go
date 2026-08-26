package memory

// Snapshot isolation for the reference backend.
//
// The disk backend versions its delta entries so a snapshot is two words. This
// one copies, which is O(V+E) in time and resident bytes per snapshot — and that
// is the right trade here, not a shortcut. memory.Store is the oracle the disk
// store is tested against (CONTRIBUTING §3), so what matters is that its answers
// are obviously correct by construction; a second versioning scheme would be a
// second thing that can be wrong in the same way, which is exactly what an
// oracle must not be.
//
// The frozen copy is an ordinary Store that nothing writes to, so every read
// method already on Store — including the whole query planner — works against it
// unchanged. That is the other half of the trade: no parallel read path.

import (
	"errors"
	"maps"
	"sync/atomic"

	"github.com/aoiflux/graphene/store"
)

// Snapshot implements store.Snapshotter.
func (s *Store) Snapshot() (store.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	frozen := &Store{
		// Node and edge records are replaced on update, never mutated in place,
		// so the maps can be cloned shallowly and the records shared.
		nodes: maps.Clone(s.nodes),
		edges: maps.Clone(s.edges),

		// Adjacency and the label postings are the two structures the live store
		// *does* mutate in place — append for adjacency, sorted insert for
		// postings, both of which can write into the existing backing array. They
		// have to be copied properly or a later write would reach through the
		// snapshot.
		adj:         cloneAdjacency(s.adj),
		nodesByType: cloneNodePostings(s.nodesByType),
		edgesByType: cloneEdgePostings(s.edgesByType),

		// The property index is shared, not copied. It is a live structure with
		// its own locking, and store.Snapshot documents property reads as "the
		// postings now, resolved against the graph then" for exactly this reason:
		// every posting is filtered through the frozen records below, so an entry
		// for a node this snapshot does not have is dropped.
		propIdx:       s.propIdx,
		reindexPolicy: s.reindexPolicy,
	}
	frozen.nodeSeq.Store(s.nodeSeq.Load())
	frozen.edgeSeq.Store(s.edgeSeq.Load())

	return &snapshot{s: frozen, epoch: s.version.Load()}, nil
}

func cloneAdjacency(src map[store.NodeID]*adjacency) map[store.NodeID]*adjacency {
	dst := make(map[store.NodeID]*adjacency, len(src))
	for id, a := range src {
		dst[id] = &adjacency{
			out: append([]store.EdgeID(nil), a.out...),
			in:  append([]store.EdgeID(nil), a.in...),
		}
	}
	return dst
}

func cloneNodePostings(src map[store.NodeType][]store.NodeID) map[store.NodeType][]store.NodeID {
	dst := make(map[store.NodeType][]store.NodeID, len(src))
	for t, ids := range src {
		dst[t] = append([]store.NodeID(nil), ids...)
	}
	return dst
}

func cloneEdgePostings(src map[store.EdgeType][]store.EdgeID) map[store.EdgeType][]store.EdgeID {
	dst := make(map[store.EdgeType][]store.EdgeID, len(src))
	for t, ids := range src {
		dst[t] = append([]store.EdgeID(nil), ids...)
	}
	return dst
}

// errSnapshotClosed is returned by every read after Close.
var errSnapshotClosed = errors.New("graphene: snapshot is closed")

// snapshot is a frozen Store presented through the read-only interface.
type snapshot struct {
	s      *Store
	epoch  uint64
	closed atomic.Bool
}

func (sn *snapshot) Epoch() uint64 { return sn.epoch }

// Close releases the copy. Idempotent.
//
// It drops the store reference so the copied maps become collectable
// immediately rather than whenever the caller lets go of the Snapshot value —
// the copy is the whole graph, and holding it a moment longer than needed is
// the cost this backend already pays most of.
func (sn *snapshot) Close() error {
	if sn.closed.Swap(true) {
		return nil
	}
	sn.s = nil
	return nil
}

func (sn *snapshot) live() (*Store, error) {
	if sn.closed.Load() {
		return nil, errSnapshotClosed
	}
	return sn.s, nil
}

func (sn *snapshot) GetNode(id store.NodeID) (*store.Node, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.GetNode(id)
}

func (sn *snapshot) GetEdge(id store.EdgeID) (*store.Edge, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.GetEdge(id)
}

func (sn *snapshot) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.Neighbours(id, dir, edgeTypes)
}

func (sn *snapshot) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.EdgesOf(id, dir, edgeTypes)
}

func (sn *snapshot) IncidentEdges(dst []store.IncidentEdge, id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.IncidentEdge, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.IncidentEdges(dst, id, dir, edgeTypes)
}

func (sn *snapshot) NodeExists(id store.NodeID) bool {
	s, err := sn.live()
	if err != nil {
		return false
	}
	return s.NodeExists(id)
}

func (sn *snapshot) DegreeOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) (int, error) {
	s, err := sn.live()
	if err != nil {
		return 0, err
	}
	return s.DegreeOf(id, dir, edgeTypes)
}

func (sn *snapshot) NodesByType(t store.NodeType) ([]store.NodeID, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.NodesByType(t)
}

func (sn *snapshot) EdgesByType(t store.EdgeType) ([]store.EdgeID, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.EdgesByType(t)
}

func (sn *snapshot) NodesByProperty(key string, value []byte) ([]store.NodeID, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.NodesByProperty(key, value)
}

func (sn *snapshot) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.EdgesByProperty(key, value)
}

func (sn *snapshot) QueryNodeIDs(q store.NodeQuery) ([]store.NodeID, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.QueryNodeIDs(q)
}

func (sn *snapshot) QueryEdgeIDs(q store.EdgeQuery) ([]store.EdgeID, error) {
	s, err := sn.live()
	if err != nil {
		return nil, err
	}
	return s.QueryEdgeIDs(q)
}

func (sn *snapshot) NodeCount() (uint64, error) {
	s, err := sn.live()
	if err != nil {
		return 0, err
	}
	return s.NodeCount()
}

func (sn *snapshot) EdgeCount() (uint64, error) {
	s, err := sn.live()
	if err != nil {
		return 0, err
	}
	return s.EdgeCount()
}
