package memory

import (
	"sync"
	"sync/atomic"

	"graphene/index"
	"graphene/store"
)

// adjacency holds the outbound and inbound edge ID lists for a single node.
type adjacency struct {
	out []store.EdgeID // edges where this node is Src
	in  []store.EdgeID // edges where this node is Dst
}

// Store is a thread-safe, in-memory implementation of store.GraphStore.
// It is the reference implementation used for development, testing, and small
// investigations where disk persistence is not required.
type Store struct {
	mu sync.RWMutex

	nodes map[store.NodeID]*store.Node
	edges map[store.EdgeID]*store.Edge
	adj   map[store.NodeID]*adjacency

	// type indexes
	nodesByType map[store.NodeType][]store.NodeID
	edgesByType map[store.EdgeType][]store.EdgeID

	// property index
	propIdx *index.PropertyIndex

	nodeSeq atomic.Uint64
	edgeSeq atomic.Uint64
}

// New returns an initialised in-memory Store.
func New() *Store {
	return &Store{
		nodes:       make(map[store.NodeID]*store.Node),
		edges:       make(map[store.EdgeID]*store.Edge),
		adj:         make(map[store.NodeID]*adjacency),
		nodesByType: make(map[store.NodeType][]store.NodeID),
		edgesByType: make(map[store.EdgeType][]store.EdgeID),
		propIdx:     index.NewPropertyIndex(),
	}
}

// nextNodeID returns the next available NodeID (never 0).
func (s *Store) nextNodeID() store.NodeID {
	return store.NodeID(s.nodeSeq.Add(1))
}

// nextEdgeID returns the next available EdgeID (never 0).
func (s *Store) nextEdgeID() store.EdgeID {
	return store.EdgeID(s.edgeSeq.Add(1))
}

// ensureAdj returns (creating if needed) the adjacency entry for id.
// Must be called with s.mu write-locked.
func (s *Store) ensureAdj(id store.NodeID) *adjacency {
	a, ok := s.adj[id]
	if !ok {
		a = &adjacency{}
		s.adj[id] = a
	}
	return a
}

// --- GraphStore implementation ---

func (s *Store) AddNode(n *store.Node) (store.NodeID, error) {
	id := s.nextNodeID()

	// make a copy so the caller can't mutate our stored node
	stored := &store.Node{
		ID: id,
	}
	if len(n.Labels) > 0 {
		stored.Labels = make([]store.NodeType, len(n.Labels))
		copy(stored.Labels, n.Labels)
	}
	if len(n.Properties) > 0 {
		stored.Properties = make([]byte, len(n.Properties))
		copy(stored.Properties, n.Properties)
	}

	s.mu.Lock()
	s.nodes[id] = stored
	for _, lbl := range n.Labels {
		s.nodesByType[lbl] = append(s.nodesByType[lbl], id)
	}
	s.ensureAdj(id)
	s.mu.Unlock()

	return id, nil
}

func (s *Store) AddEdge(e *store.Edge) (store.EdgeID, error) {
	// validate src and dst exist
	s.mu.RLock()
	_, srcOK := s.nodes[e.Src]
	_, dstOK := s.nodes[e.Dst]
	s.mu.RUnlock()

	if !srcOK {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Src}
	}
	if !dstOK {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Dst}
	}

	id := s.nextEdgeID()

	stored := &store.Edge{
		ID:     id,
		Src:    e.Src,
		Dst:    e.Dst,
		Weight: e.Weight,
	}
	if len(e.Labels) > 0 {
		stored.Labels = make([]store.EdgeType, len(e.Labels))
		copy(stored.Labels, e.Labels)
	}
	if len(e.Properties) > 0 {
		stored.Properties = make([]byte, len(e.Properties))
		copy(stored.Properties, e.Properties)
	}

	s.mu.Lock()
	s.edges[id] = stored
	for _, lbl := range e.Labels {
		s.edgesByType[lbl] = append(s.edgesByType[lbl], id)
	}
	s.ensureAdj(e.Src).out = append(s.ensureAdj(e.Src).out, id)
	s.ensureAdj(e.Dst).in = append(s.ensureAdj(e.Dst).in, id)
	s.mu.Unlock()

	return id, nil
}

func (s *Store) GetNode(id store.NodeID) (*store.Node, error) {
	s.mu.RLock()
	n, ok := s.nodes[id]
	s.mu.RUnlock()

	if !ok {
		return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	return n, nil
}

func (s *Store) GetEdge(id store.EdgeID) (*store.Edge, error) {
	s.mu.RLock()
	e, ok := s.edges[id]
	s.mu.RUnlock()

	if !ok {
		return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
	}
	return e, nil
}

func (s *Store) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.nodes[id]; !ok {
		return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}

	a := s.adj[id]
	if a == nil {
		return nil, nil
	}

	var edgeIDs []store.EdgeID
	switch dir {
	case store.DirectionOutbound:
		edgeIDs = a.out
	case store.DirectionInbound:
		edgeIDs = a.in
	case store.DirectionBoth:
		edgeIDs = append(a.out, a.in...)
	}

	result := make([]*store.Edge, 0, len(edgeIDs))
	for _, eid := range edgeIDs {
		e := s.edges[eid]
		if e == nil {
			continue
		}
		if edgeTypes != nil && !edgeMatchesFilter(edgeTypes, e) {
			continue
		}
		result = append(result, e)
	}
	return result, nil
}

func (s *Store) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error) {
	edges, err := s.EdgesOf(id, dir, edgeTypes)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]store.NeighbourResult, 0, len(edges))
	seen := make(map[store.NodeID]struct{})

	for _, e := range edges {
		var neighbourID store.NodeID
		if e.Src == id {
			neighbourID = e.Dst
		} else {
			neighbourID = e.Src
		}
		if _, already := seen[neighbourID]; already {
			continue
		}
		seen[neighbourID] = struct{}{}

		n, ok := s.nodes[neighbourID]
		if ok {
			results = append(results, store.NeighbourResult{Node: n, Edge: e})
		}
	}
	return results, nil
}

func (s *Store) NodesByType(t store.NodeType) ([]store.NodeID, error) {
	s.mu.RLock()
	ids := s.nodesByType[t]
	out := make([]store.NodeID, len(ids))
	copy(out, ids)
	s.mu.RUnlock()
	return out, nil
}

func (s *Store) EdgesByType(t store.EdgeType) ([]store.EdgeID, error) {
	s.mu.RLock()
	ids := s.edgesByType[t]
	out := make([]store.EdgeID, len(ids))
	copy(out, ids)
	s.mu.RUnlock()
	return out, nil
}

func (s *Store) NodeCount() (uint64, error) {
	s.mu.RLock()
	n := uint64(len(s.nodes))
	s.mu.RUnlock()
	return n, nil
}

func (s *Store) EdgeCount() (uint64, error) {
	s.mu.RLock()
	n := uint64(len(s.edges))
	s.mu.RUnlock()
	return n, nil
}

func (s *Store) Close() error {
	// nothing to flush for in-memory store
	return nil
}

func (s *Store) IndexNodeProperty(id store.NodeID, key string, value []byte) error {
	s.propIdx.IndexNode(id, key, value)
	return nil
}

func (s *Store) IndexEdgeProperty(id store.EdgeID, key string, value []byte) error {
	s.propIdx.IndexEdge(id, key, value)
	return nil
}

func (s *Store) NodesByProperty(key string, value []byte) ([]store.NodeID, error) {
	return s.propIdx.NodesByProperty(key, value), nil
}

func (s *Store) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error) {
	return s.propIdx.EdgesByProperty(key, value), nil
}

// --- helpers ---

// edgeMatchesFilter returns true if the edge carries any label present in the
// filter slice (OR semantics — consistent with the existing single-type filter).
func edgeMatchesFilter(filter []store.EdgeType, e *store.Edge) bool {
	for _, ft := range filter {
		if e.HasLabel(ft) {
			return true
		}
	}
	return false
}

// containsEdgeType is kept for internal use by other helpers.
func containsEdgeType(types []store.EdgeType, t store.EdgeType) bool {
	for _, et := range types {
		if et == t {
			return true
		}
	}
	return false
}
