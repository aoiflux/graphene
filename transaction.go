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
	g     *Graph
	tr    store.Transactor // nil if the backend cannot do this natively
	nodes []*store.Node
	edges []*store.Edge
	done  bool
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
	tx.nodes = append(tx.nodes, stored)
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
	tx.edges = append(tx.edges, stored)
	return id
}

// Len reports how many nodes and edges are buffered.
func (tx *Tx) Len() (nodes, edges int) { return len(tx.nodes), len(tx.edges) }

// Commit applies every buffered write as one unit.
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
	if len(tx.nodes) == 0 && len(tx.edges) == 0 {
		return nil
	}

	if tx.tr != nil {
		return tx.tr.ApplyTransaction(tx.nodes, tx.edges)
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
	tx.nodes = nil
	tx.edges = nil
	return nil
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
	return store.NodeID(placeholderNodeBase - uint64(len(tx.nodes)))
}

func (tx *Tx) reserveEdgeID() store.EdgeID {
	if tx.tr != nil {
		return tx.tr.ReserveEdgeID()
	}
	return store.EdgeID(placeholderEdgeBase - uint64(len(tx.edges)))
}

// commitFallback supports stores that do not implement store.Transactor. It is
// not atomic across the node/edge boundary and does not pretend to be; Atomic
// reports false for exactly this path.
func (tx *Tx) commitFallback() error {
	placeholders := make(map[store.NodeID]store.NodeID, len(tx.nodes))
	if len(tx.nodes) > 0 {
		ids, err := tx.g.AddNodes(tx.nodes)
		if err != nil {
			return fmt.Errorf("Tx.Commit: nodes: %w", err)
		}
		for i, n := range tx.nodes {
			placeholders[n.ID] = ids[i]
			n.ID = ids[i]
		}
	}
	if len(tx.edges) > 0 {
		for _, e := range tx.edges {
			if real, ok := placeholders[e.Src]; ok {
				e.Src = real
			}
			if real, ok := placeholders[e.Dst]; ok {
				e.Dst = real
			}
		}
		if _, err := tx.g.AddEdges(tx.edges); err != nil {
			return fmt.Errorf("Tx.Commit: edges: %w", err)
		}
	}
	return nil
}
