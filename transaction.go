package graphene

import (
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// Transactions.
//
// A transaction buffers writes and commits them as one unit. It exists because
// the slice APIs cannot express the shape ingest actually has:
//
//	AddNodes(nodes)   // transaction 1
//	AddEdges(edges)   // transaction 2  ← a crash here leaves nodes without edges
//
// Those are two commits. A Tx makes it one:
//
//	tx := g.Begin()
//	a := tx.AddNode(&store.Node{Labels: ...})
//	b := tx.AddNode(&store.Node{Labels: ...})
//	tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: ...})   // references pending nodes
//	if err := tx.Commit(); err != nil { ... }
//
// IDs are handed out immediately rather than at commit, which is what lets an
// edge reference a node the transaction has not committed yet. A transaction
// that is rolled back therefore burns the IDs it reserved. That is allowed and
// always has been: IDs are monotonic and never reused, but have never been
// promised to be contiguous.

// ErrTxDone is returned by any method called on a transaction that has already
// been committed or rolled back.
var ErrTxDone = errors.New("graphene: transaction already finished")

// Tx is a set of writes that commit together or not at all.
//
// A Tx is **not** safe for concurrent use by multiple goroutines. It is a
// caller-side buffer; the store lock is taken once, at Commit.
//
// Writes are buffered in memory until Commit, so a transaction costs memory
// proportional to its size. That is the same trade the slice APIs make, but it
// means a single enormous transaction is not free — for bulk loads that do not
// need whole-file atomicity, commit in chunks.
type Tx struct {
	g  *Graph
	tr store.Transactor // nil if the backend cannot do this natively

	// ops is the transaction, in issue order. Order is part of the semantics:
	// each operation is evaluated at commit against the store as modified by the
	// ones before it, so adding a node and then deleting it is meaningful, and
	// so is deleting an edge and adding a replacement.
	ops []store.TxOp

	// nodesAdded/edgesAdded count creations, for Len and for sizing.
	nodesAdded int
	edgesAdded int

	// actor is recorded against the commit. Zero means unattributed, which is
	// what a transaction carries unless As is called.
	actor store.TxContext

	done bool
	// err latches the first buffering error. AddNode/AddEdge return IDs rather
	// than errors for ergonomics, so a problem detected while buffering has to
	// surface at Commit.
	err error
}

// Begin starts a transaction.
//
// If the backend does not implement store.Transactor, the transaction still
// works but commits by replaying the buffered writes through the batch APIs,
// which is *not* atomic across the node/edge boundary. Callers who need the
// guarantee can check Atomic.
func (g *Graph) Begin() *Tx {
	tx := &Tx{g: g}
	if tr, ok := g.GraphStore.(store.Transactor); ok {
		tx.tr = tr
	}
	return tx
}

// Atomic reports whether Commit is all-or-nothing on this backend. It is false
// only for third-party stores that do not implement store.Transactor; both
// bundled backends return true.
func (tx *Tx) Atomic() bool { return tx.tr != nil }

// As records who is making this transaction. It returns tx so it can be chained
// onto Begin.
//
// The actor is written into the commit record alongside the commit's sequence
// number and wall-clock time, which makes the change attributable when the log
// is read back. It is recorded, not verified — see store.TxContext. Attribution
// is per-transaction because that is the unit the log can record it against:
// writes made through the plain APIs outside a transaction produce no commit
// record and are therefore unattributed.
//
// Attributed reports whether the actor will actually be durable, which is false
// on backends that keep no log.
func (tx *Tx) As(ctx store.TxContext) *Tx {
	tx.actor = ctx
	return tx
}

// Attributed reports whether this transaction's actor will be recorded durably
// on commit. It is false when no actor has been set, and false on a backend that
// does not implement store.ActorTransactor — the in-memory store, for instance,
// accepts an actor and has nowhere to keep it.
func (tx *Tx) Attributed() bool {
	if tx.actor.Unattributed() {
		return false
	}
	_, ok := tx.g.GraphStore.(store.ActorTransactor)
	return ok
}

// AddNode buffers a node and returns the ID it will have once committed.
//
// The returned ID is usable immediately as an edge endpoint within this
// transaction. It is reserved, not created: if the transaction is rolled back or
// fails, the ID is never used by anything.
//
// The node is copied, so the caller may reuse its slices as soon as this
// returns — the same contract as AddNode.
func (tx *Tx) AddNode(n *store.Node) store.NodeID {
	if tx.done {
		tx.setErr(ErrTxDone)
		return store.InvalidNodeID
	}
	if n == nil {
		tx.setErr(errors.New("Tx.AddNode: nil node"))
		return store.InvalidNodeID
	}

	id := tx.reserveNodeID()
	stored := &store.Node{ID: id}
	if len(n.Labels) > 0 {
		stored.Labels = make([]store.NodeType, len(n.Labels))
		copy(stored.Labels, n.Labels)
	}
	if len(n.Properties) > 0 {
		stored.Properties = make([]byte, len(n.Properties))
		copy(stored.Properties, n.Properties)
	}
	tx.ops = append(tx.ops, store.TxOp{Kind: store.TxOpAddNode, Node: stored})
	tx.nodesAdded++
	return id
}

// AddEdge buffers an edge and returns the ID it will have once committed.
//
// Src and Dst may name nodes that already exist or nodes added earlier in this
// same transaction. Endpoints are validated at Commit, under the store lock —
// validating here would be racy, because a node can be deleted between buffering
// and committing.
func (tx *Tx) AddEdge(e *store.Edge) store.EdgeID {
	if tx.done {
		tx.setErr(ErrTxDone)
		return store.InvalidEdgeID
	}
	if e == nil {
		tx.setErr(errors.New("Tx.AddEdge: nil edge"))
		return store.InvalidEdgeID
	}

	id := tx.reserveEdgeID()
	stored := copyEdge(e)
	stored.ID = id
	tx.ops = append(tx.ops, store.TxOp{Kind: store.TxOpAddEdge, Edge: stored})
	tx.edgesAdded++
	return id
}

// UpdateNode buffers a replacement for an existing node.
//
// The node must exist when the transaction commits — either in the store, or
// created earlier in this same transaction. Labels must be non-empty. Update
// replaces the record wholesale, exactly as Graph.UpdateNode does.
func (tx *Tx) UpdateNode(n *store.Node) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	if n == nil {
		tx.setErr(errors.New("Tx.UpdateNode: nil node"))
		return
	}
	if n.ID == store.InvalidNodeID {
		tx.setErr(errors.New("Tx.UpdateNode: node has no ID"))
		return
	}
	tx.ops = append(tx.ops, store.TxOp{Kind: store.TxOpUpdateNode, Node: copyNode(n)})
}

// UpdateEdge buffers a replacement for an existing edge. Same rules as
// UpdateNode; both endpoints must also resolve at commit.
func (tx *Tx) UpdateEdge(e *store.Edge) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	if e == nil {
		tx.setErr(errors.New("Tx.UpdateEdge: nil edge"))
		return
	}
	if e.ID == store.InvalidEdgeID {
		tx.setErr(errors.New("Tx.UpdateEdge: edge has no ID"))
		return
	}
	tx.ops = append(tx.ops, store.TxOp{Kind: store.TxOpUpdateEdge, Edge: copyEdge(e)})
}

// DeleteNode buffers a node deletion.
//
// Deletion cascades: every edge incident to the node goes too, including edges
// created earlier in this same transaction. The cascade is computed at commit,
// under the store lock — computing it at buffer time would resolve against a
// graph that can still change before the transaction commits.
func (tx *Tx) DeleteNode(id store.NodeID) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	tx.ops = append(tx.ops, store.TxOp{Kind: store.TxOpDeleteNode, NodeID: id})
}

// DeleteEdge buffers an edge deletion.
func (tx *Tx) DeleteEdge(id store.EdgeID) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	tx.ops = append(tx.ops, store.TxOp{Kind: store.TxOpDeleteEdge, EdgeID: id})
}

// Len reports how many nodes and edges this transaction *creates*. It does not
// count updates or deletes; use Ops for the total.
func (tx *Tx) Len() (nodes, edges int) { return tx.nodesAdded, tx.edgesAdded }

// Ops reports the total number of buffered operations.
func (tx *Tx) Ops() int { return len(tx.ops) }

// Commit applies every buffered operation as one unit, in the order issued.
//
// On error nothing is applied and the store is unchanged. The transaction is
// finished either way: a failed Commit does not need, and does not accept, a
// Rollback.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	if tx.err != nil {
		return tx.err
	}
	if len(tx.ops) == 0 {
		return nil
	}

	if tx.tr != nil {
		// Route through the attributed path only when there is something to
		// attribute, so a backend that implements both interfaces sees exactly
		// the call it saw before As existed.
		if at, ok := tx.tr.(store.ActorTransactor); ok && !tx.actor.Unattributed() {
			return at.ApplyTransactionAs(tx.ops, tx.actor)
		}
		return tx.tr.ApplyTransaction(tx.ops)
	}
	return tx.commitFallback()
}

// Rollback discards the transaction. It costs nothing: nothing has been written.
//
// Rolling back a transaction that has already finished returns ErrTxDone, so a
// deferred Rollback after a successful Commit is harmless but not silent —
// ignore its error in that idiom:
//
//	tx := g.Begin()
//	defer func() { _ = tx.Rollback() }()
func (tx *Tx) Rollback() error {
	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	tx.ops = nil
	return nil
}

func copyNode(n *store.Node) *store.Node {
	out := &store.Node{ID: n.ID}
	if len(n.Labels) > 0 {
		out.Labels = make([]store.NodeType, len(n.Labels))
		copy(out.Labels, n.Labels)
	}
	if len(n.Properties) > 0 {
		out.Properties = make([]byte, len(n.Properties))
		copy(out.Properties, n.Properties)
	}
	return out
}

func copyEdge(e *store.Edge) *store.Edge {
	out := &store.Edge{ID: e.ID, Src: e.Src, Dst: e.Dst, Weight: e.Weight}
	if len(e.Labels) > 0 {
		out.Labels = make([]store.EdgeType, len(e.Labels))
		copy(out.Labels, e.Labels)
	}
	if len(e.Properties) > 0 {
		out.Properties = make([]byte, len(e.Properties))
		copy(out.Properties, e.Properties)
	}
	return out
}

func (tx *Tx) setErr(err error) {
	if tx.err == nil {
		tx.err = err
	}
}

// Placeholder IDs for backends that cannot reserve. They count *down* from the
// top of the range, where real IDs count up from 1, so the two cannot meet
// before a store has issued 2^63 IDs. commitFallback rewrites endpoints from
// placeholder to real once the store has assigned them.
//
// These never reach a store: they exist only inside an uncommitted Tx.
const (
	placeholderNodeBase = ^uint64(0) // MaxUint64
	placeholderEdgeBase = ^uint64(0)
)

func (tx *Tx) reserveNodeID() store.NodeID {
	if tx.tr != nil {
		return tx.tr.ReserveNodeID()
	}
	return store.NodeID(placeholderNodeBase - uint64(tx.nodesAdded))
}

func (tx *Tx) reserveEdgeID() store.EdgeID {
	if tx.tr != nil {
		return tx.tr.ReserveEdgeID()
	}
	return store.EdgeID(placeholderEdgeBase - uint64(tx.edgesAdded))
}

// commitFallback supports stores that do not implement store.Transactor.
//
// It replays the operations in order through the public API, translating
// placeholder IDs to the real ones as the store assigns them. Ordering is
// therefore preserved, but **atomicity is not** — a failure partway through
// leaves the earlier operations applied. Atomic() reports false for exactly this
// path, and both bundled backends avoid it.
func (tx *Tx) commitFallback() error {
	nodeIDs := make(map[store.NodeID]store.NodeID)
	edgeIDs := make(map[store.EdgeID]store.EdgeID)

	realNode := func(id store.NodeID) store.NodeID {
		if real, ok := nodeIDs[id]; ok {
			return real
		}
		return id
	}
	realEdge := func(id store.EdgeID) store.EdgeID {
		if real, ok := edgeIDs[id]; ok {
			return real
		}
		return id
	}

	for i, op := range tx.ops {
		var err error
		switch op.Kind {
		case store.TxOpAddNode:
			placeholder := op.Node.ID
			var id store.NodeID
			id, err = tx.g.AddNode(op.Node)
			if err == nil {
				nodeIDs[placeholder] = id
			}

		case store.TxOpAddEdge:
			placeholder := op.Edge.ID
			e := *op.Edge
			e.Src, e.Dst = realNode(e.Src), realNode(e.Dst)
			var id store.EdgeID
			id, err = tx.g.AddEdge(&e)
			if err == nil {
				edgeIDs[placeholder] = id
			}

		case store.TxOpUpdateNode:
			n := *op.Node
			n.ID = realNode(n.ID)
			err = tx.g.UpdateNode(&n)

		case store.TxOpUpdateEdge:
			e := *op.Edge
			e.ID = realEdge(e.ID)
			e.Src, e.Dst = realNode(e.Src), realNode(e.Dst)
			err = tx.g.UpdateEdge(&e)

		case store.TxOpDeleteNode:
			err = tx.g.DeleteNode(realNode(op.NodeID))

		case store.TxOpDeleteEdge:
			err = tx.g.DeleteEdge(realEdge(op.EdgeID))

		default:
			err = fmt.Errorf("unknown op kind %d", op.Kind)
		}
		if err != nil {
			return fmt.Errorf("Tx.Commit: op %d (%s): %w", i, op.Kind, err)
		}
	}
	return nil
}
