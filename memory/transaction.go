package memory

import (
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// Transaction application.
//
// Same contract and same ordering semantics as the disk backend — see
// disk/transaction.go for the reasoning. Two differences follow from there being
// no WAL here:
//
//   - Atomicity is structural. Resolution decides everything that will happen;
//     application cannot then fail. There is nothing to log and nothing to undo.
//   - Resolution still runs as its own pass rather than applying as it goes,
//     because a transaction must reject exactly what disk rejects. This backend
//     is the oracle disk is compared against, so "which transactions fail" has
//     to match, not merely "what a successful one leaves behind".

type txActionKind uint8

const (
	txActionPutNode txActionKind = iota + 1
	txActionPutEdge
	txActionDelNode
	txActionDelEdge
	// Index purges accompany an update when ReindexPolicy is ReindexPurge, so a
	// transactional update leaves the property index in the same state a plain
	// UpdateNode would.
	txActionPurgeNodeIndex
	txActionPurgeEdgeIndex
)

type txAction struct {
	kind   txActionKind
	node   *store.Node
	edge   *store.Edge
	nodeID store.NodeID
	edgeID store.EdgeID
}

// txView layers a transaction's pending effects over the store, so each
// operation is evaluated against the ones before it.
type txView struct {
	s *Store

	nodes    map[store.NodeID]*store.Node
	edges    map[store.EdgeID]*store.Edge
	delNode  map[store.NodeID]struct{}
	delEdge  map[store.EdgeID]struct{}
	incident map[store.NodeID][]store.EdgeID
}

func newTxView(s *Store, hint int) *txView {
	return &txView{
		s:        s,
		nodes:    make(map[store.NodeID]*store.Node, hint),
		edges:    make(map[store.EdgeID]*store.Edge, hint),
		delNode:  make(map[store.NodeID]struct{}),
		delEdge:  make(map[store.EdgeID]struct{}),
		incident: make(map[store.NodeID][]store.EdgeID),
	}
}

func (v *txView) nodeLive(id store.NodeID) bool {
	if _, gone := v.delNode[id]; gone {
		return false
	}
	if _, here := v.nodes[id]; here {
		return true
	}
	_, ok := v.s.nodes[id]
	return ok
}

func (v *txView) edgeLive(id store.EdgeID) bool {
	if _, gone := v.delEdge[id]; gone {
		return false
	}
	if _, here := v.edges[id]; here {
		return true
	}
	_, ok := v.s.edges[id]
	return ok
}

func (v *txView) cascadeFor(id store.NodeID) []store.EdgeID {
	var out []store.EdgeID
	seen := make(map[store.EdgeID]struct{})

	add := func(eid store.EdgeID) {
		if _, dup := seen[eid]; dup {
			return
		}
		if !v.edgeLive(eid) {
			return
		}
		seen[eid] = struct{}{}
		out = append(out, eid)
	}

	if a := v.s.adj[id]; a != nil {
		for _, eid := range a.out {
			add(eid)
		}
		for _, eid := range a.in {
			add(eid)
		}
	}
	for _, eid := range v.incident[id] {
		add(eid)
	}
	return out
}

func (s *Store) resolveTransaction(ops []store.TxOp) ([]txAction, error) {
	v := newTxView(s, len(ops))
	actions := make([]txAction, 0, len(ops))

	for i, op := range ops {
		switch op.Kind {
		case store.TxOpAddNode, store.TxOpUpdateNode:
			n := op.Node
			if n == nil {
				return nil, fmt.Errorf("transaction op %d (%s): nil node", i, op.Kind)
			}
			if len(n.Labels) == 0 {
				return nil, fmt.Errorf("transaction op %d (%s): node %d must carry at least one label", i, op.Kind, n.ID)
			}
			if op.Kind == store.TxOpUpdateNode && !v.nodeLive(n.ID) {
				return nil, &store.ErrNotFound{Kind: "node", ID: uint64(n.ID)}
			}
			v.nodes[n.ID] = n
			delete(v.delNode, n.ID)
			actions = append(actions, txAction{kind: txActionPutNode, node: n})
			if op.Kind == store.TxOpUpdateNode && s.reindexPolicy == store.ReindexPurge {
				actions = append(actions, txAction{kind: txActionPurgeNodeIndex, nodeID: n.ID})
			}

		case store.TxOpAddEdge, store.TxOpUpdateEdge:
			e := op.Edge
			if e == nil {
				return nil, fmt.Errorf("transaction op %d (%s): nil edge", i, op.Kind)
			}
			if op.Kind == store.TxOpUpdateEdge && !v.edgeLive(e.ID) {
				return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(e.ID)}
			}
			if !v.nodeLive(e.Src) {
				return nil, &store.ErrInvalidEdge{MissingID: e.Src}
			}
			if !v.nodeLive(e.Dst) {
				return nil, &store.ErrInvalidEdge{MissingID: e.Dst}
			}
			if _, known := v.edges[e.ID]; !known {
				v.incident[e.Src] = append(v.incident[e.Src], e.ID)
				if e.Dst != e.Src {
					v.incident[e.Dst] = append(v.incident[e.Dst], e.ID)
				}
			}
			v.edges[e.ID] = e
			delete(v.delEdge, e.ID)
			actions = append(actions, txAction{kind: txActionPutEdge, edge: e})
			if op.Kind == store.TxOpUpdateEdge && s.reindexPolicy == store.ReindexPurge {
				actions = append(actions, txAction{kind: txActionPurgeEdgeIndex, edgeID: e.ID})
			}

		case store.TxOpDeleteNode:
			if !v.nodeLive(op.NodeID) {
				return nil, &store.ErrNotFound{Kind: "node", ID: uint64(op.NodeID)}
			}
			for _, eid := range v.cascadeFor(op.NodeID) {
				v.delEdge[eid] = struct{}{}
				delete(v.edges, eid)
				actions = append(actions, txAction{kind: txActionDelEdge, edgeID: eid})
			}
			v.delNode[op.NodeID] = struct{}{}
			delete(v.nodes, op.NodeID)
			actions = append(actions, txAction{kind: txActionDelNode, nodeID: op.NodeID})

		case store.TxOpDeleteEdge:
			if !v.edgeLive(op.EdgeID) {
				return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(op.EdgeID)}
			}
			v.delEdge[op.EdgeID] = struct{}{}
			delete(v.edges, op.EdgeID)
			actions = append(actions, txAction{kind: txActionDelEdge, edgeID: op.EdgeID})

		default:
			return nil, fmt.Errorf("transaction op %d: unknown kind %d", i, op.Kind)
		}
	}
	return actions, nil
}

// This backend deliberately does NOT implement store.ActorTransactor.
//
// It could: accepting a TxContext and discarding it compiles and would make the
// two backends' method sets identical. But the interface means "this store
// records who committed", and there is nowhere here to record it — everything is
// in memory and there is no log. Claiming it would make Tx.Attributed report
// true for an actor that is about to be dropped, which is worse than not
// offering the feature.
//
// Graph falls back to ApplyTransaction, so an attributed transaction against
// this backend still commits; it commits unattributed, and Tx.Attributed says
// so. That is the same honesty Tx.Atomic already applies to the non-atomic
// fallback.

// ApplyTransaction implements store.Transactor.
func (s *Store) ApplyTransaction(ops []store.TxOp) error {
	if len(ops) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	actions, err := s.resolveTransaction(ops)
	if err != nil {
		return err
	}

	for _, a := range actions {
		switch a.kind {
		case txActionPutNode:
			s.upsertNodeLocked(a.node)
		case txActionPutEdge:
			s.upsertEdgeLocked(a.edge)
		case txActionDelNode:
			s.deleteNodeRecordLocked(a.nodeID)
		case txActionDelEdge:
			if _, ok := s.edges[a.edgeID]; ok {
				s.deleteEdgeLocked(a.edgeID)
			}
		case txActionPurgeNodeIndex:
			s.propIdx.RemoveNode(a.nodeID)
		case txActionPurgeEdgeIndex:
			s.propIdx.RemoveEdge(a.edgeID)
		}
	}
	return nil
}

// upsertNodeLocked inserts or replaces a node, keeping label postings correct
// for the replace case. Caller must hold s.mu.
func (s *Store) upsertNodeLocked(n *store.Node) {
	if prev, ok := s.nodes[n.ID]; ok {
		s.unindexNodeLabels(n.ID, prev.Labels)
	}
	s.nodes[n.ID] = n
	s.indexNodeLabels(n.ID, n.Labels)
	s.ensureAdj(n.ID)
}

// upsertEdgeLocked inserts or replaces an edge. Adjacency is recorded only for a
// genuinely new edge, so a replaced edge is never listed twice.
func (s *Store) upsertEdgeLocked(e *store.Edge) {
	prev, existed := s.edges[e.ID]
	if existed {
		s.unindexEdgeLabels(e.ID, prev.Labels)
		if prev.Src != e.Src || prev.Dst != e.Dst {
			// Endpoints moved: drop the old adjacency entries before adding new.
			if a := s.adj[prev.Src]; a != nil {
				a.out = removeEdgeID(a.out, e.ID)
			}
			if a := s.adj[prev.Dst]; a != nil {
				a.in = removeEdgeID(a.in, e.ID)
			}
			existed = false
		}
	}
	s.edges[e.ID] = e
	s.indexEdgeLabels(e.ID, e.Labels)
	if !existed {
		s.ensureAdj(e.Src).out = append(s.ensureAdj(e.Src).out, e.ID)
		s.ensureAdj(e.Dst).in = append(s.ensureAdj(e.Dst).in, e.ID)
	}
}

// deleteNodeRecordLocked removes a node without cascading: resolution has
// already emitted explicit delete actions for its incident edges.
func (s *Store) deleteNodeRecordLocked(id store.NodeID) {
	if n, ok := s.nodes[id]; ok {
		s.unindexNodeLabels(id, n.Labels)
	}
	delete(s.nodes, id)
	delete(s.adj, id)
	s.propIdx.RemoveNode(id)
}
