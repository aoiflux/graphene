package disk

import (
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// Transaction application.
//
// A transaction is a sequence of operations that commit together or not at all,
// each evaluated against the store as modified by the operations before it. That
// ordering is what makes "add a node, then delete it" and "delete an edge, then
// add a replacement" mean what a caller expects.
//
// The work happens in three phases, and the split is deliberate:
//
//	resolve → one flat list of primitive actions
//	frame   → those actions, in order, into one WAL batch
//	apply   → those same actions, in order, to the delta and indexes
//
// Framing and applying read the *same resolved list*, so the log and memory
// cannot disagree about what the transaction did. An earlier shape validated and
// then re-derived the work while applying; two passes computing the same thing
// from different inputs is precisely how a WAL drifts from the state it is
// supposed to describe.
//
// Everything runs under one lock hold, so the store cannot move between
// resolution and apply.

type txActionKind uint8

const (
	txActionPutNode txActionKind = iota + 1
	txActionPutEdge
	txActionDelNode
	txActionDelEdge
	// Index purges accompany an update when the store's ReindexPolicy is
	// ReindexPurge. Graph.UpdateNode does this outside a transaction, so a
	// transaction that skipped it would leave stale property-index entries that
	// the non-transactional path removes — a silent divergence between two ways
	// of doing the same thing.
	txActionPurgeNodeIndex
	txActionPurgeEdgeIndex
)

// txAction is a resolved primitive: no conditionals left, nothing to look up.
type txAction struct {
	kind   txActionKind
	node   *store.Node
	edge   *store.Edge
	nodeID store.NodeID
	edgeID store.EdgeID
}

// txView tracks what the transaction has done so far, layered over the store.
// Resolution consults this rather than the store alone, because an operation may
// depend on an earlier one in the same transaction.
type txView struct {
	s *Store

	nodes   map[store.NodeID]*store.Node // added or updated here
	edges   map[store.EdgeID]*store.Edge // added or updated here
	delNode map[store.NodeID]struct{}
	delEdge map[store.EdgeID]struct{}

	// incident indexes edges this transaction created, by endpoint, so a
	// DeleteNode cascade can find them. Edges already in the store are found
	// through incidentEdgeIDsLocked instead.
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

// nodeLive reports whether id resolves to a live node in this transaction's view.
func (v *txView) nodeLive(id store.NodeID) bool {
	if _, gone := v.delNode[id]; gone {
		return false
	}
	if _, here := v.nodes[id]; here {
		return true
	}
	return v.s.nodeExistsLocked(id)
}

// edgeLive reports whether id resolves to a live edge in this transaction's view.
func (v *txView) edgeLive(id store.EdgeID) bool {
	if _, gone := v.delEdge[id]; gone {
		return false
	}
	if _, here := v.edges[id]; here {
		return true
	}
	return v.s.edgeExistsLocked(id)
}

// cascadeFor returns the live edges incident to id: those already in the store
// plus those this transaction created, minus anything already deleted here.
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

	for _, eid := range v.s.incidentEdgeIDsLocked(id) {
		add(eid)
	}
	for _, eid := range v.incident[id] {
		add(eid)
	}
	return out
}

// resolveTransaction turns ops into primitive actions, or fails.
//
// Nothing is written here. Every error path leaves the store untouched simply
// because nothing has been touched yet.
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
			// Cascade first, node last: a crash or a truncated replay must never
			// leave an edge pointing at a node that is already gone.
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

// ApplyTransaction implements store.Transactor.
func (s *Store) ApplyTransaction(ops []store.TxOp) error {
	return s.ApplyTransactionAs(ops, store.TxContext{})
}

// ApplyTransactionAs implements store.ActorTransactor.
func (s *Store) ApplyTransactionAs(ops []store.TxOp, ctx store.TxContext) error {
	if len(ops) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	actions, err := s.resolveTransaction(ops)
	if err != nil {
		return err
	}
	if len(actions) == 0 {
		return nil
	}

	batch := newWALBatch(len(actions) * 64)
	for _, a := range actions {
		switch a.kind {
		case txActionPutNode:
			node := a.node
			batch.addWith(walRecordNode, func(dst []byte) []byte {
				return appendMarshalledNode(dst, node)
			})
		case txActionPutEdge:
			edge := a.edge
			batch.addWith(walRecordEdge, func(dst []byte) []byte {
				return appendMarshalledEdge(dst, edge)
			})
		case txActionDelNode:
			batch.add(walRecordNodeDelete, marshalID(uint64(a.nodeID)))
		case txActionDelEdge:
			batch.add(walRecordEdgeDelete, marshalID(uint64(a.edgeID)))
		case txActionPurgeNodeIndex:
			batch.add(walRecordNodePropPurge, marshalID(uint64(a.nodeID)))
		case txActionPurgeEdgeIndex:
			batch.add(walRecordEdgePropPurge, marshalID(uint64(a.edgeID)))
		}
	}

	// One write. If it fails nothing is applied: the commit marker never reached
	// the file, so replay discards whatever partial bytes did. That absence is
	// the rollback — there is no undo path that could itself go wrong.
	framed, err := batch.finish(s.nextCommitMeta(ctx))
	if err != nil {
		return fmt.Errorf("ApplyTransaction: %w", err)
	}
	if err := s.wal.AppendBatch(framed, s.syncOnCommit); err != nil {
		return fmt.Errorf("ApplyTransaction: wal: %w", err)
	}

	for _, a := range actions {
		switch a.kind {
		case txActionPutNode:
			s.applyNodeUpsert(a.node)
		case txActionPutEdge:
			s.applyEdgeUpsert(a.edge)
		case txActionDelNode:
			s.applyNodeDelete(a.nodeID)
		case txActionDelEdge:
			s.applyEdgeDelete(a.edgeID)
		case txActionPurgeNodeIndex:
			s.propIdx.RemoveNode(a.nodeID)
		case txActionPurgeEdgeIndex:
			s.propIdx.RemoveEdge(a.edgeID)
		}
	}
	return nil
}
