package memory

import (
	"fmt"
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

	// reindexPolicy governs what updates do to propIdx. Guarded by mu.
	reindexPolicy store.ReindexPolicy

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

// indexNodeLabels adds id to the postings for each distinct label.
//
// Postings are kept in ascending ID order so removal is a binary search plus a
// memmove rather than a rewrite of the whole list. Insertion is usually O(1)
// anyway: IDs are issued monotonically, so a new node appends to the end.
//
// Repeated labels are skipped. A caller may legitimately pass the same label
// twice, and without this the postings would list id once per repetition — the
// sorted insert would reject the duplicate, but the check keeps the intent
// explicit. The node's own Labels slice is left exactly as the caller supplied.
// Must be called with s.mu write-locked.
func (s *Store) indexNodeLabels(id store.NodeID, labels []store.NodeType) {
	for i, lbl := range labels {
		if containsNodeType(labels[:i], lbl) {
			continue
		}
		// Append directly when id already sorts last, which is the ingest case.
		// Calling through the generic helper costs a non-inlined call per label;
		// ingest does this for every node, so the branch is worth spelling out.
		ids := s.nodesByType[lbl]
		if n := len(ids); n == 0 || ids[n-1] < id {
			s.nodesByType[lbl] = append(ids, id)
			continue
		}
		if updated, added := store.InsertSortedID(ids, id); added {
			s.nodesByType[lbl] = updated
		}
	}
}

// indexEdgeLabels adds id to the postings for each distinct label, keeping them
// in ascending ID order. Must be called with s.mu write-locked.
func (s *Store) indexEdgeLabels(id store.EdgeID, labels []store.EdgeType) {
	for i, lbl := range labels {
		if containsEdgeType(labels[:i], lbl) {
			continue
		}
		ids := s.edgesByType[lbl]
		if n := len(ids); n == 0 || ids[n-1] < id {
			s.edgesByType[lbl] = append(ids, id)
			continue
		}
		if updated, added := store.InsertSortedID(ids, id); added {
			s.edgesByType[lbl] = updated
		}
	}
}

// unindexNodeLabels removes id from the postings for each of its labels.
// Must be called with s.mu write-locked.
func (s *Store) unindexNodeLabels(id store.NodeID, labels []store.NodeType) {
	for _, lbl := range labels {
		ids, removed := store.DeleteSortedID(s.nodesByType[lbl], id)
		if !removed {
			continue
		}
		if len(ids) == 0 {
			delete(s.nodesByType, lbl)
			continue
		}
		s.nodesByType[lbl] = ids
	}
}

// unindexEdgeLabels removes id from the postings for each of its labels.
// Must be called with s.mu write-locked.
func (s *Store) unindexEdgeLabels(id store.EdgeID, labels []store.EdgeType) {
	for _, lbl := range labels {
		ids, removed := store.DeleteSortedID(s.edgesByType[lbl], id)
		if !removed {
			continue
		}
		if len(ids) == 0 {
			delete(s.edgesByType, lbl)
			continue
		}
		s.edgesByType[lbl] = ids
	}
}

func containsNodeType(types []store.NodeType, t store.NodeType) bool {
	for _, v := range types {
		if v == t {
			return true
		}
	}
	return false
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
	s.indexNodeLabels(id, stored.Labels)
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
		s.indexNodeLabels(id, stored.Labels)
		s.ensureAdj(id)
		ids[i] = id
	}

	return ids, nil
}

func (s *Store) AddEdge(e *store.Edge) (store.EdgeID, error) {
	stored := &store.Edge{
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

	// Validate and insert under one lock hold so an edge can never be created
	// onto a node a concurrent DeleteNode has removed.
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.nodes[e.Src]; !ok {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Src}
	}
	if _, ok := s.nodes[e.Dst]; !ok {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Dst}
	}

	id := s.nextEdgeID()
	stored.ID = id

	s.edges[id] = stored
	s.indexEdgeLabels(id, stored.Labels)
	s.ensureAdj(stored.Src).out = append(s.ensureAdj(stored.Src).out, id)
	s.ensureAdj(stored.Dst).in = append(s.ensureAdj(stored.Dst).in, id)

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
		s.indexEdgeLabels(id, stored.Labels)
		s.ensureAdj(e.Src).out = append(s.ensureAdj(e.Src).out, id)
		s.ensureAdj(e.Dst).in = append(s.ensureAdj(e.Dst).in, id)
		ids[i] = id
	}

	return ids, nil
}

// SetReindexPolicy implements store.Reindexer.
func (s *Store) SetReindexPolicy(p store.ReindexPolicy) {
	s.mu.Lock()
	s.reindexPolicy = p
	s.mu.Unlock()
}

// ReindexPolicy implements store.Reindexer.
func (s *Store) ReindexPolicy() store.ReindexPolicy {
	s.mu.RLock()
	p := s.reindexPolicy
	s.mu.RUnlock()
	return p
}

// DeclareOrderedNodeProperty implements store.OrderedIndexDeclarer.
func (s *Store) DeclareOrderedNodeProperty(key string) error {
	s.propIdx.DeclareOrderedNodeKey(key)
	return nil
}

// DeclareOrderedEdgeProperty implements store.OrderedIndexDeclarer.
func (s *Store) DeclareOrderedEdgeProperty(key string) error {
	s.propIdx.DeclareOrderedEdgeKey(key)
	return nil
}

// OrderedNodeProperties implements store.OrderedIndexDeclarer.
func (s *Store) OrderedNodeProperties() []string { return s.propIdx.OrderedNodeKeys() }

// OrderedEdgeProperties implements store.OrderedIndexDeclarer.
func (s *Store) OrderedEdgeProperties() []string { return s.propIdx.OrderedEdgeKeys() }

// PurgeNodeIndex implements store.Reindexer.
func (s *Store) PurgeNodeIndex(id store.NodeID) error {
	s.propIdx.RemoveNode(id)
	return nil
}

// PurgeEdgeIndex implements store.Reindexer.
func (s *Store) PurgeEdgeIndex(id store.EdgeID) error {
	s.propIdx.RemoveEdge(id)
	return nil
}

func (s *Store) UpdateNode(n *store.Node) error {
	if len(n.Labels) == 0 {
		return fmt.Errorf("UpdateNode: node %d must carry at least one label", n.ID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.nodes[n.ID]
	if !ok {
		return &store.ErrNotFound{Kind: "node", ID: uint64(n.ID)}
	}

	if s.reindexPolicy == store.ReindexPurge {
		s.propIdx.RemoveNode(n.ID)
	}

	// Reconcile the type index: drop old labels, add new ones.
	s.unindexNodeLabels(n.ID, existing.Labels)

	updated := &store.Node{ID: n.ID}
	updated.Labels = make([]store.NodeType, len(n.Labels))
	copy(updated.Labels, n.Labels)
	if len(n.Properties) > 0 {
		updated.Properties = make([]byte, len(n.Properties))
		copy(updated.Properties, n.Properties)
	}

	s.nodes[n.ID] = updated
	s.indexNodeLabels(n.ID, updated.Labels)
	return nil
}

func (s *Store) UpdateEdge(e *store.Edge) error {
	if len(e.Labels) == 0 {
		return fmt.Errorf("UpdateEdge: edge %d must carry at least one label", e.ID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.edges[e.ID]
	if !ok {
		return &store.ErrNotFound{Kind: "edge", ID: uint64(e.ID)}
	}

	if s.reindexPolicy == store.ReindexPurge {
		s.propIdx.RemoveEdge(e.ID)
	}

	// Reconcile the type index: drop old labels, add new ones.
	s.unindexEdgeLabels(e.ID, existing.Labels)

	// Endpoints are immutable — keep existing Src/Dst (and adjacency untouched).
	updated := &store.Edge{
		ID:     e.ID,
		Src:    existing.Src,
		Dst:    existing.Dst,
		Weight: e.Weight,
	}
	updated.Labels = make([]store.EdgeType, len(e.Labels))
	copy(updated.Labels, e.Labels)
	if len(e.Properties) > 0 {
		updated.Properties = make([]byte, len(e.Properties))
		copy(updated.Properties, e.Properties)
	}

	s.edges[e.ID] = updated
	s.indexEdgeLabels(e.ID, updated.Labels)
	return nil
}

func (s *Store) DeleteEdge(id store.EdgeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.edges[id]; !ok {
		return &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
	}
	s.deleteEdgeLocked(id)
	return nil
}

func (s *Store) DeleteNode(id store.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		return &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}

	// Cascade: delete every incident edge (outbound + inbound) first.
	if a := s.adj[id]; a != nil {
		incident := make([]store.EdgeID, 0, len(a.out)+len(a.in))
		incident = append(incident, a.out...)
		incident = append(incident, a.in...)
		for _, eid := range incident {
			if _, ok := s.edges[eid]; ok {
				s.deleteEdgeLocked(eid)
			}
		}
	}

	s.unindexNodeLabels(id, node.Labels)
	delete(s.nodes, id)
	delete(s.adj, id)
	s.propIdx.RemoveNode(id)
	return nil
}

// deleteEdgeLocked removes a single edge and all its index/adjacency entries.
// Caller must hold s.mu.
func (s *Store) deleteEdgeLocked(id store.EdgeID) {
	e := s.edges[id]
	if e == nil {
		return
	}
	delete(s.edges, id)
	if a := s.adj[e.Src]; a != nil {
		a.out = removeEdgeID(a.out, id)
	}
	if a := s.adj[e.Dst]; a != nil {
		a.in = removeEdgeID(a.in, id)
	}
	s.unindexEdgeLabels(id, e.Labels)
	s.propIdx.RemoveEdge(id)
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
		// Build into a fresh slice — appending onto a.out under RLock would race
		// concurrent readers writing a.out's spare capacity.
		edgeIDs = make([]store.EdgeID, 0, len(a.out)+len(a.in))
		edgeIDs = append(edgeIDs, a.out...)
		edgeIDs = append(edgeIDs, a.in...)
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

// IncidentEdges implements store.AdjacencyReader. It mirrors EdgesOf's ordering
// (outbound before inbound for DirectionBoth) but appends to the caller's buffer
// instead of building a fresh []*store.Edge.
func (s *Store) IncidentEdges(dst []store.IncidentEdge, id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.IncidentEdge, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.nodes[id]; !ok {
		return dst, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	a := s.adj[id]
	if a == nil {
		return dst, nil
	}

	appendMatching := func(eids []store.EdgeID) {
		for _, eid := range eids {
			e := s.edges[eid]
			if e == nil {
				continue
			}
			if edgeTypes != nil && !edgeMatchesFilter(edgeTypes, e) {
				continue
			}
			nb := e.Dst
			if e.Src != id {
				nb = e.Src
			}
			dst = append(dst, store.IncidentEdge{Edge: eid, Neighbour: nb})
		}
	}

	switch dir {
	case store.DirectionOutbound:
		appendMatching(a.out)
	case store.DirectionInbound:
		appendMatching(a.in)
	case store.DirectionBoth:
		appendMatching(a.out)
		appendMatching(a.in)
	}
	return dst, nil
}

// NodeExists implements store.AdjacencyReader.
func (s *Store) NodeExists(id store.NodeID) bool {
	s.mu.RLock()
	_, ok := s.nodes[id]
	s.mu.RUnlock()
	return ok
}

// DegreeOf implements store.DegreeCounter: it counts incident edges without
// building the []*store.Edge slice that EdgesOf would return.
func (s *Store) DegreeOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.nodes[id]; !ok {
		return 0, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	a := s.adj[id]
	if a == nil {
		return 0, nil
	}
	if edgeTypes == nil {
		switch dir {
		case store.DirectionOutbound:
			return len(a.out), nil
		case store.DirectionInbound:
			return len(a.in), nil
		default:
			return len(a.out) + len(a.in), nil
		}
	}

	count := 0
	countMatching := func(eids []store.EdgeID) {
		for _, eid := range eids {
			e := s.edges[eid]
			if e != nil && edgeMatchesFilter(edgeTypes, e) {
				count++
			}
		}
	}
	switch dir {
	case store.DirectionOutbound:
		countMatching(a.out)
	case store.DirectionInbound:
		countMatching(a.in)
	default:
		countMatching(a.out)
		countMatching(a.in)
	}
	return count, nil
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

// NodesByProperty returns the nodes indexed under key with exactly value.
//
// The postings are resolved against the records before being returned. Without
// that step a caller can observe a deletion mid-flight: DeleteNode removes the
// record and its index entries under one write lock, but this path only locks
// the index, so it can read postings that the delete has not reached yet and
// hand back an entity the records no longer have. Filtering makes the records
// the authority, so every ID returned resolved to a live node at the instant it
// was checked.
//
// That is the guarantee, and it is deliberately not stronger: the node may be
// deleted the moment this returns. Ruling that out needs snapshot isolation,
// which the store does not offer.
func (s *Store) NodesByProperty(key string, value []byte) ([]store.NodeID, error) {
	return s.liveNodeIDs(s.propIdx.NodesByProperty(key, value)), nil
}

// EdgesByProperty returns the edges indexed under key with exactly value. It
// resolves postings against the records for the reason given on NodesByProperty.
func (s *Store) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error) {
	return s.liveEdgeIDs(s.propIdx.EdgesByProperty(key, value)), nil
}

func (s *Store) QueryNodeIDs(query store.NodeQuery) ([]store.NodeID, error) {
	candidates, sortedAsc, plan := s.driveNodeCandidates(query)

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
		// Both sides must be ascending for the merge. The driving set often
		// already is; when it is not, sorting it once here is repaid immediately
		// because the merge output is ascending too, which retires the sort below.
		if !sortedAsc {
			candidates = store.SortDedupeIDs(candidates)
			sortedAsc = true
		}
		if store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
			// Every filter's own set contains the answer, so the residual pass
			// can narrow the candidates directly and skip the driving filter
			// entirely rather than re-deriving a set it was already built from.
			candidates = s.propIdx.NarrowNodesByFilters(candidates, query.Filters, plan.DriverFilter)
		} else {
			matched := s.matchNodeIDsByFilters(query.Filters, store.MatchAny)
			candidates = store.IntersectSortedIDs(candidates, matched)
		}
	}

	order := store.NormalizedQueryOrder(query.Order)
	// An ascending candidate set needs no sort at all, and only a linear reverse
	// to satisfy a descending query.
	switch {
	case sortedAsc && order == store.QueryOrderAsc:
		// already in the requested order
	case sortedAsc:
		store.ReverseIDs(candidates)
	default:
		sort.Slice(candidates, func(i, j int) bool {
			if order == store.QueryOrderDesc {
				return candidates[i] > candidates[j]
			}
			return candidates[i] < candidates[j]
		})
	}
	return store.ApplyNodeQueryWindow(candidates, query.Offset, query.Limit), nil
}

func (s *Store) QueryEdgeIDs(query store.EdgeQuery) ([]store.EdgeID, error) {
	candidates, sortedAsc, plan := s.driveEdgeCandidates(query)

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
		if !sortedAsc {
			candidates = store.SortDedupeIDs(candidates)
			sortedAsc = true
		}
		if store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
			candidates = s.propIdx.NarrowEdgesByFilters(candidates, query.Filters, plan.DriverFilter)
		} else {
			matched := s.matchEdgeIDsByFilters(query.Filters, store.MatchAny)
			candidates = store.IntersectSortedIDs(candidates, matched)
		}
	}

	order := store.NormalizedQueryOrder(query.Order)
	switch {
	case sortedAsc && order == store.QueryOrderAsc:
		// already in the requested order
	case sortedAsc:
		store.ReverseIDs(candidates)
	default:
		sort.Slice(candidates, func(i, j int) bool {
			if order == store.QueryOrderDesc {
				return candidates[i] > candidates[j]
			}
			return candidates[i] < candidates[j]
		})
	}
	return store.ApplyEdgeQueryWindow(candidates, query.Offset, query.Limit), nil
}

// VerifyIndexes implements store.IndexVerifier. It cross-checks the label
// postings, the adjacency lists, and the property index against the live node
// and edge records, returning the first inconsistency found.
func (s *Store) VerifyIndexes() error {
	if err := s.propIdx.Verify(); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Label postings must list exactly the live entities carrying that label.
	//
	// The membership sets built while walking the postings are reused for the
	// reverse direction; testing membership by scanning the postings slice would
	// make verification quadratic in the size of the largest label.
	nodeMembers := make(map[store.NodeType]map[store.NodeID]struct{}, len(s.nodesByType))
	for lbl, ids := range s.nodesByType {
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("node label index: %v postings are not strictly ascending", lbl)
		}
		seen := make(map[store.NodeID]struct{}, len(ids))
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("node label index: %v lists node %d twice", lbl, id)
			}
			seen[id] = struct{}{}
			n, ok := s.nodes[id]
			if !ok {
				return fmt.Errorf("node label index: %v lists node %d, which does not exist", lbl, id)
			}
			if !n.HasLabel(lbl) {
				return fmt.Errorf("node label index: %v lists node %d, which does not carry that label", lbl, id)
			}
		}
		nodeMembers[lbl] = seen
	}
	for id, n := range s.nodes {
		for _, lbl := range n.Labels {
			if _, ok := nodeMembers[lbl][id]; !ok {
				return fmt.Errorf("node label index: node %d carries %v but is missing from the postings", id, lbl)
			}
		}
	}

	edgeMembers := make(map[store.EdgeType]map[store.EdgeID]struct{}, len(s.edgesByType))
	for lbl, ids := range s.edgesByType {
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("edge label index: %v postings are not strictly ascending", lbl)
		}
		seen := make(map[store.EdgeID]struct{}, len(ids))
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("edge label index: %v lists edge %d twice", lbl, id)
			}
			seen[id] = struct{}{}
			e, ok := s.edges[id]
			if !ok {
				return fmt.Errorf("edge label index: %v lists edge %d, which does not exist", lbl, id)
			}
			if !e.HasLabel(lbl) {
				return fmt.Errorf("edge label index: %v lists edge %d, which does not carry that label", lbl, id)
			}
		}
		edgeMembers[lbl] = seen
	}
	for id, e := range s.edges {
		for _, lbl := range e.Labels {
			if _, ok := edgeMembers[lbl][id]; !ok {
				return fmt.Errorf("edge label index: edge %d carries %v but is missing from the postings", id, lbl)
			}
		}
	}

	// Adjacency must agree with the edges' endpoints, in both directions.
	//
	// The forward pass proves every adjacency entry is a real edge with the right
	// endpoint and appears at most once. Given that, matching per-node counts is
	// enough to prove no edge is missing — and it keeps this linear rather than
	// quadratic in the degree of the busiest node.
	outCount := make(map[store.NodeID]int, len(s.adj))
	inCount := make(map[store.NodeID]int, len(s.adj))
	for nodeID, a := range s.adj {
		if _, ok := s.nodes[nodeID]; !ok {
			return fmt.Errorf("adjacency: entry for node %d, which does not exist", nodeID)
		}
		seenOut := make(map[store.EdgeID]struct{}, len(a.out))
		for _, eid := range a.out {
			if _, dup := seenOut[eid]; dup {
				return fmt.Errorf("adjacency: node %d lists outbound edge %d twice", nodeID, eid)
			}
			seenOut[eid] = struct{}{}
			e, ok := s.edges[eid]
			if !ok {
				return fmt.Errorf("adjacency: node %d lists outbound edge %d, which does not exist", nodeID, eid)
			}
			if e.Src != nodeID {
				return fmt.Errorf("adjacency: node %d lists outbound edge %d, whose Src is %d", nodeID, eid, e.Src)
			}
		}
		seenIn := make(map[store.EdgeID]struct{}, len(a.in))
		for _, eid := range a.in {
			if _, dup := seenIn[eid]; dup {
				return fmt.Errorf("adjacency: node %d lists inbound edge %d twice", nodeID, eid)
			}
			seenIn[eid] = struct{}{}
			e, ok := s.edges[eid]
			if !ok {
				return fmt.Errorf("adjacency: node %d lists inbound edge %d, which does not exist", nodeID, eid)
			}
			if e.Dst != nodeID {
				return fmt.Errorf("adjacency: node %d lists inbound edge %d, whose Dst is %d", nodeID, eid, e.Dst)
			}
		}
		outCount[nodeID] = len(a.out)
		inCount[nodeID] = len(a.in)
	}

	wantOut := make(map[store.NodeID]int, len(s.adj))
	wantIn := make(map[store.NodeID]int, len(s.adj))
	for _, e := range s.edges {
		wantOut[e.Src]++
		wantIn[e.Dst]++
	}
	for nodeID, want := range wantOut {
		if outCount[nodeID] != want {
			return fmt.Errorf("adjacency: node %d has %d outbound entries, but %d edges start there",
				nodeID, outCount[nodeID], want)
		}
	}
	for nodeID, want := range wantIn {
		if inCount[nodeID] != want {
			return fmt.Errorf("adjacency: node %d has %d inbound entries, but %d edges end there",
				nodeID, inCount[nodeID], want)
		}
	}

	// The property index must not outlive the entities it describes.
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if _, ok := s.nodes[id]; !ok {
			return fmt.Errorf("property index: node %d has entries but does not exist", id)
		}
	}
	for _, id := range s.propIdx.IndexedEdgeIDs() {
		if _, ok := s.edges[id]; !ok {
			return fmt.Errorf("property index: edge %d has entries but does not exist", id)
		}
	}
	return nil
}

// RebuildIndexes implements store.IndexRebuilder. It recomputes the label
// postings and the adjacency lists from the node and edge records, then drops
// property-index entries belonging to entities that no longer exist.
func (s *Store) RebuildIndexes() error {
	s.mu.Lock()

	s.nodesByType = make(map[store.NodeType][]store.NodeID, len(s.nodesByType))
	s.edgesByType = make(map[store.EdgeType][]store.EdgeID, len(s.edgesByType))
	s.adj = make(map[store.NodeID]*adjacency, len(s.nodes))

	for id, n := range s.nodes {
		s.indexNodeLabels(id, n.Labels)
		s.ensureAdj(id)
	}
	for id, e := range s.edges {
		s.indexEdgeLabels(id, e.Labels)
		s.ensureAdj(e.Src).out = append(s.ensureAdj(e.Src).out, id)
		s.ensureAdj(e.Dst).in = append(s.ensureAdj(e.Dst).in, id)
	}

	// Collect dangling property-index owners while we still hold the lock, but
	// purge after releasing it: the property index has its own lock.
	var deadNodes []store.NodeID
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if _, ok := s.nodes[id]; !ok {
			deadNodes = append(deadNodes, id)
		}
	}
	var deadEdges []store.EdgeID
	for _, id := range s.propIdx.IndexedEdgeIDs() {
		if _, ok := s.edges[id]; !ok {
			deadEdges = append(deadEdges, id)
		}
	}
	s.mu.Unlock()

	for _, id := range deadNodes {
		s.propIdx.RemoveNode(id)
	}
	for _, id := range deadEdges {
		s.propIdx.RemoveEdge(id)
	}
	return nil
}

// sortDedupeNodeIDs sorts ids ascending and removes duplicates, in place.
//
// The ordered index emits IDs in value order, and an entity registered under two
// values for the same key appears once per value, so both properties have to be
// restored before the result can be used as a driving set.
func sortDedupeNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) < 2 {
		return ids
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// sortDedupeEdgeIDs is sortDedupeNodeIDs for edge IDs.
func sortDedupeEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) < 2 {
		return ids
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// --- query planning ---
//
// Every query used to start from "all nodes" / "all edges" and filter down. The
// drive* helpers instead pick the cheapest available index as the starting set.
// Each candidate source is a guaranteed superset of the result, so the filter
// stages that follow are unchanged and the output is identical either way — the
// only difference is how much the store had to read to get there.

// driveNodeCandidates returns the starting candidate set for a node query and
// whether it is already in ascending ID order.
// driveNodeCandidates picks the cheapest source that is guaranteed to contain
// the query's answer. It returns the candidates, whether they are ascending, and
// a plan describing that choice — including the filter it consumed, so the
// residual pass does not evaluate that filter a second time.
func (s *Store) driveNodeCandidates(query store.NodeQuery) ([]store.NodeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]store.NodeID, 0, len(query.IDs))
		seen := make(map[store.NodeID]struct{}, len(query.IDs))
		for _, id := range query.IDs {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, ok := s.nodes[id]; ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out, false, store.QueryPlan{Driver: store.DriverIDs, DriverFilter: -1}
	}

	s.mu.RLock()
	total := len(s.nodes)
	typeSize := -1
	if len(query.Types) > 0 {
		typeSize = 0
		for _, t := range query.Types {
			typeSize += len(s.nodesByType[t])
		}
	}
	s.mu.RUnlock()

	// Most selective equality filter, if any qualifies as a driver.
	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := s.propIdx.NodeCardinality(f.Key, f.Value)
		if bestFilter == nil || size < bestSize {
			filter := f
			bestFilter = &filter
			bestSize = size
		}
	}

	if bestFilter != nil && bestSize <= total && (typeSize < 0 || bestSize <= typeSize) {
		ids := s.propIdx.NodesByProperty(bestFilter.Key, bestFilter.Value)
		return s.liveNodeIDs(ids), true, store.QueryPlan{
			Driver:       store.DriverEquality,
			DriverKey:    bestFilter.Key,
			DriverFilter: store.FilterIndexOf(query.Filters, *bestFilter),
		}
	}

	// A range or prefix filter on a key declared ordered bounds the result too,
	// and resolving it here means the query never enumerates the whole graph.
	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		if ids, served := s.propIdx.NodesMatchingOrdered(nil, f); served {
			return s.liveNodeIDs(sortDedupeNodeIDs(ids)), true, store.QueryPlan{
				Driver:       store.DriverOrdered,
				DriverKey:    f.Key,
				DriverFilter: store.FilterIndexOf(query.Filters, f),
			}
		}
	}

	if typeSize >= 0 && typeSize <= total {
		// A single label's postings are already ascending, so the query path can
		// skip its sort entirely. A union of several is not.
		return s.nodeIDsForTypes(query.Types), len(query.Types) == 1, store.QueryPlan{Driver: store.DriverLabels, DriverFilter: -1}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]store.NodeID, 0, len(s.nodes))
	for id := range s.nodes {
		out = append(out, id)
	}
	return out, false, store.QueryPlan{Driver: store.DriverScan, DriverFilter: -1}
}

// liveNodeIDs drops IDs that no longer resolve to a node, preserving order.
func (s *Store) liveNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) == 0 {
		return ids // a miss should not pay for the lock
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := ids[:0]
	for _, id := range ids {
		if _, ok := s.nodes[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// nodeIDsForTypes returns the deduplicated union of the type postings lists.
func (s *Store) nodeIDsForTypes(types []store.NodeType) []store.NodeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[store.NodeID]struct{})
	var out []store.NodeID
	for _, t := range types {
		for _, id := range s.nodesByType[t] {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

// driveEdgeCandidates returns the starting candidate set for an edge query and
// whether it is already in ascending ID order.
// driveEdgeCandidates mirrors driveNodeCandidates: it returns the candidates,
// whether they are ascending, and a plan naming the source it drove from and the
// filter it consumed, so the residual pass does not re-evaluate that filter.
func (s *Store) driveEdgeCandidates(query store.EdgeQuery) ([]store.EdgeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]store.EdgeID, 0, len(query.IDs))
		seen := make(map[store.EdgeID]struct{}, len(query.IDs))
		for _, id := range query.IDs {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, ok := s.edges[id]; ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out, false, store.QueryPlan{Driver: store.DriverIDs, DriverFilter: -1}
	}

	s.mu.RLock()
	total := len(s.edges)
	typeSize := -1
	if len(query.Types) > 0 {
		typeSize = 0
		for _, t := range query.Types {
			typeSize += len(s.edgesByType[t])
		}
	}
	// Anchored queries: the incident-edge lists bound the result exactly, so
	// prefer whichever endpoint set has the smaller total degree.
	anchorDir := store.DirectionOutbound
	anchors := query.SrcIDs
	anchorSize := -1
	if len(query.SrcIDs) > 0 {
		anchorSize = s.degreeSumLocked(query.SrcIDs, store.DirectionOutbound)
	}
	if len(query.DstIDs) > 0 {
		if dstSize := s.degreeSumLocked(query.DstIDs, store.DirectionInbound); anchorSize < 0 || dstSize < anchorSize {
			anchorSize = dstSize
			anchorDir = store.DirectionInbound
			anchors = query.DstIDs
		}
	}
	s.mu.RUnlock()

	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := s.propIdx.EdgeCardinality(f.Key, f.Value)
		if bestFilter == nil || size < bestSize {
			filter := f
			bestFilter = &filter
			bestSize = size
		}
	}

	best := total
	strategy := "all"
	if bestFilter != nil && bestSize <= best {
		best = bestSize
		strategy = "property"
	}
	if anchorSize >= 0 && anchorSize <= best {
		best = anchorSize
		strategy = "anchor"
	}
	if typeSize >= 0 && typeSize <= best {
		strategy = "type"
	}

	switch strategy {
	case "property":
		return s.liveEdgeIDs(s.propIdx.EdgesByProperty(bestFilter.Key, bestFilter.Value)), true, store.QueryPlan{
			Driver:       store.DriverEquality,
			DriverKey:    bestFilter.Key,
			DriverFilter: store.FilterIndexOf(query.Filters, *bestFilter),
		}
	case "anchor":
		return s.incidentEdgeIDs(anchors, anchorDir), false, store.QueryPlan{
			Driver: store.DriverAdjacency, DriverFilter: -1,
		}
	case "type":
		return s.edgeIDsForTypes(query.Types), len(query.Types) == 1, store.QueryPlan{
			Driver: store.DriverLabels, DriverFilter: -1,
		}
	}

	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		if ids, served := s.propIdx.EdgesMatchingOrdered(nil, f); served {
			return s.liveEdgeIDs(sortDedupeEdgeIDs(ids)), true, store.QueryPlan{
				Driver:       store.DriverOrdered,
				DriverKey:    f.Key,
				DriverFilter: store.FilterIndexOf(query.Filters, f),
			}
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]store.EdgeID, 0, len(s.edges))
	for id := range s.edges {
		out = append(out, id)
	}
	return out, false, store.QueryPlan{Driver: store.DriverScan, DriverFilter: -1}
}

// degreeSumLocked totals the incident-edge counts for ids. Caller must hold s.mu.
func (s *Store) degreeSumLocked(ids []store.NodeID, dir store.Direction) int {
	total := 0
	for _, id := range ids {
		a := s.adj[id]
		if a == nil {
			continue
		}
		switch dir {
		case store.DirectionOutbound:
			total += len(a.out)
		case store.DirectionInbound:
			total += len(a.in)
		default:
			total += len(a.out) + len(a.in)
		}
	}
	return total
}

// incidentEdgeIDs returns the deduplicated edge IDs incident to ids in dir.
func (s *Store) incidentEdgeIDs(ids []store.NodeID, dir store.Direction) []store.EdgeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	add := func(eids []store.EdgeID) {
		for _, eid := range eids {
			if _, ok := seen[eid]; ok {
				continue
			}
			if _, ok := s.edges[eid]; !ok {
				continue
			}
			seen[eid] = struct{}{}
			out = append(out, eid)
		}
	}
	for _, id := range ids {
		a := s.adj[id]
		if a == nil {
			continue
		}
		switch dir {
		case store.DirectionOutbound:
			add(a.out)
		case store.DirectionInbound:
			add(a.in)
		default:
			add(a.out)
			add(a.in)
		}
	}
	return out
}

// liveEdgeIDs drops IDs that no longer resolve to an edge, preserving order.
func (s *Store) liveEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) == 0 {
		return ids // a miss should not pay for the lock
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := ids[:0]
	for _, id := range ids {
		if _, ok := s.edges[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// edgeIDsForTypes returns the deduplicated union of the type postings lists.
func (s *Store) edgeIDsForTypes(types []store.EdgeType) []store.EdgeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	for _, t := range types {
		for _, id := range s.edgesByType[t] {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

// --- helpers ---

// matchNodeIDsByFilters returns the ascending, deduplicated set of node IDs
// satisfying the filters under the given mode.
//
// Each filter is resolved to a sorted slice and the slices are merged, rather
// than each being built into a map and the maps intersected. Merging is one pass
// per side with no hashing, the output stays sorted so the query path can skip
// its final sort, and an empty intersection under MatchAll can stop early.
func (s *Store) matchNodeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) []store.NodeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.NodeID
	for i, f := range filters {
		set := s.matchOneNodeFilter(f)
		if i == 0 {
			acc = set
			continue
		}
		if mode == store.MatchAny {
			acc = store.UnionSortedIDs(acc, set)
			continue
		}
		acc = store.IntersectSortedIDs(acc, set)
		if len(acc) == 0 {
			// Nothing can re-enter an empty intersection.
			return acc
		}
	}
	return acc
}

// matchOneNodeFilter resolves a single filter to an ascending, deduplicated set.
func (s *Store) matchOneNodeFilter(f store.PropertyFilter) []store.NodeID {
	if f.Op == store.PropertyOpEqual {
		// Postings are already ascending and deduplicated.
		return s.propIdx.NodesByProperty(f.Key, f.Value)
	}
	// A key declared ordered answers ranges and prefixes by binary search. Its
	// comparison is byte-wise, so the whole predicate is resolved there — mixing
	// it with the scan matcher below would apply two orderings to one key.
	if ids, served := s.propIdx.NodesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	// Otherwise scan only the buckets belonging to this key, never the whole index.
	var out []store.NodeID
	s.propIdx.ForEachNodeEntry(f.Key, func(id store.NodeID, value []byte) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, id)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

// matchEdgeIDsByFilters is matchNodeIDsByFilters for edge properties.
func (s *Store) matchEdgeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) []store.EdgeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.EdgeID
	for i, f := range filters {
		set := s.matchOneEdgeFilter(f)
		if i == 0 {
			acc = set
			continue
		}
		if mode == store.MatchAny {
			acc = store.UnionSortedIDs(acc, set)
			continue
		}
		acc = store.IntersectSortedIDs(acc, set)
		if len(acc) == 0 {
			return acc
		}
	}
	return acc
}

func (s *Store) matchOneEdgeFilter(f store.PropertyFilter) []store.EdgeID {
	if f.Op == store.PropertyOpEqual {
		return s.propIdx.EdgesByProperty(f.Key, f.Value)
	}
	if ids, served := s.propIdx.EdgesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	var out []store.EdgeID
	s.propIdx.ForEachEdgeEntry(f.Key, func(id store.EdgeID, value []byte) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, id)
		}
		return true
	})
	return store.SortDedupeIDs(out)
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

// removeEdgeID returns ids with occurrences of target removed, preserving order.
//
// This is for **adjacency lists only**. Label postings are sorted and use
// store.DeleteSortedID instead; adjacency cannot be sorted because EdgesOf must
// return edges in insertion order for traversal results to stay stable.
// Reuses the backing array (safe: callers hold the write lock and reads copy).
func removeEdgeID(ids []store.EdgeID, target store.EdgeID) []store.EdgeID {
	out := ids[:0]
	for _, id := range ids {
		if id != target {
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

// ExplainNodeQuery reports how the planner would resolve query, without
// returning the matching entities.
//
// It runs the driving step for real — that is the only way to know how large the
// candidate set is, and the candidate size is what decides how each residual
// filter is applied — but stops before applying them. So the cost is the driver,
// not the query.
//
// The plan is diagnostic. Which index the planner picks is free to change as the
// cost model improves; the results a query returns are not.
func (s *Store) ExplainNodeQuery(query store.NodeQuery) (store.QueryPlan, error) {
	candidates, _, plan := s.driveNodeCandidates(query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = s.propIdx.PlanNodeResiduals(query.Filters, plan.DriverFilter, len(candidates))
	}
	ids, err := s.QueryNodeIDs(query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}

// ExplainEdgeQuery is ExplainNodeQuery for edge queries.
func (s *Store) ExplainEdgeQuery(query store.EdgeQuery) (store.QueryPlan, error) {
	candidates, _, plan := s.driveEdgeCandidates(query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = s.propIdx.PlanEdgeResiduals(query.Filters, plan.DriverFilter, len(candidates))
	}
	ids, err := s.QueryEdgeIDs(query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}
