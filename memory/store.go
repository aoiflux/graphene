package memory

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
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

// AddNodesBatch adds nodes in order and returns assigned IDs.
// On error, returns successfully added IDs up to the failing index.
func (s *Store) AddNodesBatch(nodes []*store.Node) ([]store.NodeID, error) {
	ids := make([]store.NodeID, len(nodes))

	s.mu.Lock()
	defer s.mu.Unlock()

	for i, n := range nodes {
		id := s.nextNodeID()

		stored := &store.Node{ID: id}
		if len(n.Labels) > 0 {
			stored.Labels = make([]store.NodeType, len(n.Labels))
			copy(stored.Labels, n.Labels)
		}
		if len(n.Properties) > 0 {
			stored.Properties = make([]byte, len(n.Properties))
			copy(stored.Properties, n.Properties)
		}

		s.nodes[id] = stored
		for _, lbl := range n.Labels {
			s.nodesByType[lbl] = append(s.nodesByType[lbl], id)
		}
		s.ensureAdj(id)
		ids[i] = id
	}

	return ids, nil
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

// AddEdgesBatch adds edges in order and returns assigned IDs.
// On error, returns successfully added IDs up to the failing index.
func (s *Store) AddEdgesBatch(edges []*store.Edge) ([]store.EdgeID, error) {
	ids := make([]store.EdgeID, len(edges))

	s.mu.Lock()
	defer s.mu.Unlock()

	for i, e := range edges {
		if _, ok := s.nodes[e.Src]; !ok {
			return ids[:i], &store.ErrInvalidEdge{MissingID: e.Src}
		}
		if _, ok := s.nodes[e.Dst]; !ok {
			return ids[:i], &store.ErrInvalidEdge{MissingID: e.Dst}
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

		s.edges[id] = stored
		for _, lbl := range e.Labels {
			s.edgesByType[lbl] = append(s.edgesByType[lbl], id)
		}
		s.ensureAdj(e.Src).out = append(s.ensureAdj(e.Src).out, id)
		s.ensureAdj(e.Dst).in = append(s.ensureAdj(e.Dst).in, id)
		ids[i] = id
	}

	return ids, nil
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

func (s *Store) QueryNodeIDs(query store.NodeQuery) ([]store.NodeID, error) {
	s.mu.RLock()
	candidates := make([]store.NodeID, 0, len(s.nodes))
	if len(query.IDs) > 0 {
		seen := make(map[store.NodeID]struct{}, len(query.IDs))
		for _, id := range query.IDs {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, ok := s.nodes[id]; ok {
				seen[id] = struct{}{}
				candidates = append(candidates, id)
			}
		}
	} else {
		for id := range s.nodes {
			candidates = append(candidates, id)
		}
	}
	s.mu.RUnlock()

	if len(query.Types) > 0 {
		typeSet := make(map[store.NodeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		filtered := make([]store.NodeID, 0, len(candidates))
		for _, id := range candidates {
			n, err := s.GetNode(id)
			if err != nil {
				continue
			}
			if nodeHasAnyType(n, typeSet) {
				filtered = append(filtered, id)
			}
		}
		candidates = filtered
	}

	if len(query.Filters) > 0 {
		matched := s.matchNodeIDsByFilters(query.Filters, store.NormalizedFilterMode(query.FilterMode))
		candidates = intersectNodeIDSet(candidates, matched)
	}

	order := store.NormalizedQueryOrder(query.Order)
	sort.Slice(candidates, func(i, j int) bool {
		if order == store.QueryOrderDesc {
			return candidates[i] > candidates[j]
		}
		return candidates[i] < candidates[j]
	})
	return store.ApplyNodeQueryWindow(candidates, query.Offset, query.Limit), nil
}

func (s *Store) QueryEdgeIDs(query store.EdgeQuery) ([]store.EdgeID, error) {
	s.mu.RLock()
	candidates := make([]store.EdgeID, 0, len(s.edges))
	if len(query.IDs) > 0 {
		seen := make(map[store.EdgeID]struct{}, len(query.IDs))
		for _, id := range query.IDs {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, ok := s.edges[id]; ok {
				seen[id] = struct{}{}
				candidates = append(candidates, id)
			}
		}
	} else {
		for id := range s.edges {
			candidates = append(candidates, id)
		}
	}
	s.mu.RUnlock()

	if len(query.Types) > 0 || len(query.SrcIDs) > 0 || len(query.DstIDs) > 0 {
		typeSet := make(map[store.EdgeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		srcSet := makeNodeIDSet(query.SrcIDs)
		dstSet := makeNodeIDSet(query.DstIDs)

		filtered := make([]store.EdgeID, 0, len(candidates))
		for _, id := range candidates {
			e, err := s.GetEdge(id)
			if err != nil {
				continue
			}
			if len(typeSet) > 0 && !edgeHasAnyType(e, typeSet) {
				continue
			}
			if len(srcSet) > 0 {
				if _, ok := srcSet[e.Src]; !ok {
					continue
				}
			}
			if len(dstSet) > 0 {
				if _, ok := dstSet[e.Dst]; !ok {
					continue
				}
			}
			filtered = append(filtered, id)
		}
		candidates = filtered
	}

	if len(query.Filters) > 0 {
		matched := s.matchEdgeIDsByFilters(query.Filters, store.NormalizedFilterMode(query.FilterMode))
		candidates = intersectEdgeIDSet(candidates, matched)
	}

	order := store.NormalizedQueryOrder(query.Order)
	sort.Slice(candidates, func(i, j int) bool {
		if order == store.QueryOrderDesc {
			return candidates[i] > candidates[j]
		}
		return candidates[i] < candidates[j]
	})
	return store.ApplyEdgeQueryWindow(candidates, query.Offset, query.Limit), nil
}

// --- helpers ---

func (s *Store) matchNodeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) map[store.NodeID]struct{} {
	if len(filters) == 0 {
		return nil
	}
	allEntries := s.propIdx.NodeEntries()
	sets := make([]map[store.NodeID]struct{}, 0, len(filters))
	for _, f := range filters {
		set := make(map[store.NodeID]struct{})
		if f.Op == store.PropertyOpEqual {
			for _, id := range s.propIdx.NodesByProperty(f.Key, f.Value) {
				set[id] = struct{}{}
			}
		} else {
			for _, entry := range allEntries {
				if entry.Key != f.Key {
					continue
				}
				if store.PropertyFilterMatches(f, entry.Value) {
					set[entry.ID] = struct{}{}
				}
			}
		}
		sets = append(sets, set)
	}
	if mode == store.MatchAny {
		out := make(map[store.NodeID]struct{})
		for _, set := range sets {
			for id := range set {
				out[id] = struct{}{}
			}
		}
		return out
	}
	out := make(map[store.NodeID]struct{})
	for id := range sets[0] {
		out[id] = struct{}{}
	}
	for i := 1; i < len(sets); i++ {
		for id := range out {
			if _, ok := sets[i][id]; !ok {
				delete(out, id)
			}
		}
	}
	return out
}

func (s *Store) matchEdgeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) map[store.EdgeID]struct{} {
	if len(filters) == 0 {
		return nil
	}
	allEntries := s.propIdx.EdgeEntries()
	sets := make([]map[store.EdgeID]struct{}, 0, len(filters))
	for _, f := range filters {
		set := make(map[store.EdgeID]struct{})
		if f.Op == store.PropertyOpEqual {
			for _, id := range s.propIdx.EdgesByProperty(f.Key, f.Value) {
				set[id] = struct{}{}
			}
		} else {
			for _, entry := range allEntries {
				if entry.Key != f.Key {
					continue
				}
				if store.PropertyFilterMatches(f, entry.Value) {
					set[entry.ID] = struct{}{}
				}
			}
		}
		sets = append(sets, set)
	}
	if mode == store.MatchAny {
		out := make(map[store.EdgeID]struct{})
		for _, set := range sets {
			for id := range set {
				out[id] = struct{}{}
			}
		}
		return out
	}
	out := make(map[store.EdgeID]struct{})
	for id := range sets[0] {
		out[id] = struct{}{}
	}
	for i := 1; i < len(sets); i++ {
		for id := range out {
			if _, ok := sets[i][id]; !ok {
				delete(out, id)
			}
		}
	}
	return out
}

func nodeHasAnyType(n *store.Node, typeSet map[store.NodeType]struct{}) bool {
	for _, lbl := range n.Labels {
		if _, ok := typeSet[lbl]; ok {
			return true
		}
	}
	return false
}

func edgeHasAnyType(e *store.Edge, typeSet map[store.EdgeType]struct{}) bool {
	for _, lbl := range e.Labels {
		if _, ok := typeSet[lbl]; ok {
			return true
		}
	}
	return false
}

func makeNodeIDSet(ids []store.NodeID) map[store.NodeID]struct{} {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[store.NodeID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func intersectNodeIDSet(candidates []store.NodeID, keep map[store.NodeID]struct{}) []store.NodeID {
	out := make([]store.NodeID, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := keep[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

func intersectEdgeIDSet(candidates []store.EdgeID, keep map[store.EdgeID]struct{}) []store.EdgeID {
	out := make([]store.EdgeID, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := keep[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

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
