package graphene

import (
	"bytes"
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

	// claimed maps a unique key's value to the ID this transaction has already
	// resolved it to, so upserting one key twice in a transaction returns one
	// entity rather than two. The store cannot answer for these: nothing has
	// been registered there yet.
	claimed map[txClaim]uint64

	// tracked turns on read-set recording. Reads work either way; this decides
	// whether Commit is allowed to refuse because of one.
	tracked bool

	// checks is the read set, in the order the reads were made, and seen keeps
	// it free of duplicates. Both stay nil on an untracked transaction.
	checks []store.ReadCheck
	seen   map[readKey]struct{}

	// ov indexes the buffered ops for reading. Built on the first read and not
	// before, so a write-only transaction pays nothing for it.
	ov *txOverlay

	done bool
	// err latches the first buffering error. AddNode/AddEdge return IDs rather
	// than errors for ergonomics, so a problem detected while buffering has to
	// surface at Commit.
	err error
}

// txClaim identifies one value under one unique key. The kind is part of the
// identity because node and edge keys are declared separately and may share a
// name without sharing a meaning.
type txClaim struct {
	kind  string // "node" or "edge"
	key   string
	value string
}

// Begin starts a transaction.
//
// If the backend does not implement store.Transactor, the transaction still
// works but commits by replaying the buffered writes through the batch APIs,
// which is *not* atomic across the node/edge boundary. Callers who need the
// guarantee can check Atomic.
func (g *Graph) Begin() *Tx {
	tx := &Tx{g: g, claimed: make(map[txClaim]uint64)}
	if tr, ok := g.GraphStore.(store.Transactor); ok {
		tx.tr = tr
	}
	return tx
}

// BeginTracked starts a transaction that records what it reads, and is refused
// if any of it changed before the commit.
//
// This is what makes read-modify-write safe:
//
//	for {
//	    tx := g.BeginTracked()
//	    n, err := tx.GetNode(id)
//	    if err != nil { return err }
//	    tx.UpdateNode(advance(n))
//	    err = tx.Commit()
//	    if errors.Is(err, store.ErrWriteConflict) { continue } // someone won; re-read
//	    return err
//	}
//
// Without the tracking, that loop's Commit cannot tell whether the node it
// decided from is still the node it is overwriting. With it, a change to any
// record the transaction read — or to any of them disappearing — refuses the
// whole transaction with store.ErrWriteConflict, and the retry sees what the
// winner wrote. Nothing is applied on a refusal.
//
// It is opt-in rather than the default because it can only ever *add* a
// failure: existing code that reads inside a transaction would start seeing
// conflicts from commits that previously succeeded, having silently overwritten.
//
// What it does not give is serialisability. Reads are checked, not
// predicates: a transaction that found no node under a label is not protected
// against one appearing. See store.ReadCheck for the full statement, and note
// that a backend that cannot validate a read set refuses such a commit outright
// rather than applying it unprotected.
func (g *Graph) BeginTracked() *Tx {
	tx := g.Begin()
	tx.tracked = true
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

// UpsertNode buffers a create-or-update keyed on a declared-unique property,
// and returns the ID the entity will have once committed, along with whether
// this transaction is creating it.
//
// This is what makes re-ingesting a source idempotent. The second run resolves
// the same key to the same node and replaces it, where AddNode would have
// written a second one:
//
//	g.DeclareUniqueProperty("k")
//
//	tx := g.Begin()
//	ver, _ := tx.UpsertNode("k", []byte("ver:9f3a"),
//	    &store.Node{Labels: ..., Properties: blob},
//	    map[string][]byte{"state": []byte("extracted")})
//	perm, _ := tx.UpsertNode("k", []byte("perm:INTERNET"), permNode, nil)
//	tx.AddEdge(&store.Edge{Src: ver, Dst: perm, Labels: ...})
//	err := tx.Commit()
//
// The returned ID is usable immediately as an edge endpoint, which is the whole
// reason the key is resolved here rather than at commit. That early read is
// re-checked under the write lock: if another writer took the key in between,
// Commit returns ErrWriteConflict and the transaction is refused whole. Retrying
// resolves to whatever the winner wrote. A single-writer caller never sees this.
//
// created is therefore trustworthy exactly when Commit succeeds, and means
// nothing if it does not.
//
// On a hit, the node's labels and properties are replaced by n, and each key
// named in props is *replaced* rather than added to — so the superseded value
// stops matching, which is what a field used as a work queue depends on. Keys
// props does not name keep what they had. The key entry is registered for you,
// so props need not repeat it.
//
// Upserting one key twice in a transaction returns the same ID both times, and
// the later call wins.
func (tx *Tx) UpsertNode(key string, value []byte, n *store.Node, props map[string][]byte) (store.NodeID, bool) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return store.InvalidNodeID, false
	}
	if n == nil {
		tx.setErr(errors.New("Tx.UpsertNode: nil node"))
		return store.InvalidNodeID, false
	}

	claim := txClaim{kind: "node", key: key, value: string(value)}
	existing, resolved := tx.claimed[claim]
	created := false
	if !resolved {
		d, ok := tx.g.GraphStore.(store.UniqueIndexDeclarer)
		if !ok {
			tx.setErr(fmt.Errorf("Tx.UpsertNode: %T cannot enforce unique properties", tx.g.GraphStore))
			return store.InvalidNodeID, false
		}
		owner, found, err := d.UniqueNodeOwner(key, value)
		if err != nil {
			tx.setErr(fmt.Errorf("Tx.UpsertNode: %w", err))
			return store.InvalidNodeID, false
		}
		if found {
			existing = uint64(owner)
		} else {
			existing = uint64(tx.reserveNodeID())
			created = true
			tx.nodesAdded++
		}
		tx.claimed[claim] = existing
	}
	id := store.NodeID(existing)

	stored := copyNode(n)
	stored.ID = id

	expect := uint64(store.InvalidNodeID)
	if !created {
		expect = existing
	}
	tx.ops = append(tx.ops, store.TxOp{
		Kind:   store.TxOpUpsertNode,
		Node:   stored,
		NodeID: id,
		Key:    key,
		Value:  bytes.Clone(value),
		Expect: expect,
		Props:  copyProps(props),
	})
	return id, created
}

// UpsertEdge is UpsertNode for edges, keyed on a declared-unique edge property.
//
// Endpoints are immutable, so on a hit the existing edge keeps the Src and Dst
// it was created with and e supplies only labels, weight and properties. That is
// the same rule UpdateEdge applies, and it is why an edge's unique value should
// name the pair it connects — an edge whose key is stable but whose endpoints
// were meant to move is a different edge.
func (tx *Tx) UpsertEdge(key string, value []byte, e *store.Edge, props map[string][]byte) (store.EdgeID, bool) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return store.InvalidEdgeID, false
	}
	if e == nil {
		tx.setErr(errors.New("Tx.UpsertEdge: nil edge"))
		return store.InvalidEdgeID, false
	}

	claim := txClaim{kind: "edge", key: key, value: string(value)}
	existing, resolved := tx.claimed[claim]
	created := false
	if !resolved {
		d, ok := tx.g.GraphStore.(store.UniqueIndexDeclarer)
		if !ok {
			tx.setErr(fmt.Errorf("Tx.UpsertEdge: %T cannot enforce unique properties", tx.g.GraphStore))
			return store.InvalidEdgeID, false
		}
		owner, found, err := d.UniqueEdgeOwner(key, value)
		if err != nil {
			tx.setErr(fmt.Errorf("Tx.UpsertEdge: %w", err))
			return store.InvalidEdgeID, false
		}
		if found {
			existing = uint64(owner)
		} else {
			existing = uint64(tx.reserveEdgeID())
			created = true
			tx.edgesAdded++
		}
		tx.claimed[claim] = existing
	}
	id := store.EdgeID(existing)

	stored := copyEdge(e)
	stored.ID = id

	expect := uint64(store.InvalidEdgeID)
	if !created {
		expect = existing
	}
	tx.ops = append(tx.ops, store.TxOp{
		Kind:   store.TxOpUpsertEdge,
		Edge:   stored,
		EdgeID: id,
		Key:    key,
		Value:  bytes.Clone(value),
		Expect: expect,
		Props:  copyProps(props),
	})
	return id, created
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

// UpdateNodeIndexed buffers a node update together with the complete
// replacement of its property-index entries.
//
// props is the entity's whole indexed state afterwards: keys it does not mention
// are dropped. That is the right shape when the caller knows everything the node
// should be indexed under, which a re-ingest does. When only one field moved, use
// UpdateNodePartialIndex instead — enumerating the other keys just to preserve
// them is how one of them eventually goes missing.
//
// The record and the entries move in one operation, so the store's ReindexPolicy
// has nothing left to decide and this is not subject to the refusal an ordinary
// UpdateNode meets under ReindexReject.
func (tx *Tx) UpdateNodeIndexed(n *store.Node, props map[string][]byte) {
	tx.bufferIndexedNodeUpdate(n, props, false)
}

// UpdateNodePartialIndex buffers a node update that replaces only the entries
// for the keys named in changed, leaving every other key as it was.
//
// This is the shape a state transition has:
//
//	tx.UpdateNodePartialIndex(n, map[string][]byte{"state": []byte("analyzed")})
//
// The node's key entry, and anything else it is indexed under, survive
// untouched; "state" stops matching its old value and starts matching the new
// one. Replacing rather than adding is the point: an added value leaves the old
// one matching too, and a field used as a work queue then hands the same entity
// back to the worker forever.
func (tx *Tx) UpdateNodePartialIndex(n *store.Node, changed map[string][]byte) {
	tx.bufferIndexedNodeUpdate(n, changed, true)
}

func (tx *Tx) bufferIndexedNodeUpdate(n *store.Node, props map[string][]byte, partial bool) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	if n == nil {
		tx.setErr(errors.New("Tx.UpdateNodeIndexed: nil node"))
		return
	}
	if n.ID == store.InvalidNodeID {
		tx.setErr(errors.New("Tx.UpdateNodeIndexed: node ID is unset"))
		return
	}
	tx.ops = append(tx.ops, store.TxOp{
		Kind:    store.TxOpUpdateNodeIndexed,
		Node:    copyNode(n),
		NodeID:  n.ID,
		Props:   copyProps(props),
		Partial: partial,
	})
}

// UpdateEdgeIndexed is UpdateNodeIndexed for edges.
func (tx *Tx) UpdateEdgeIndexed(e *store.Edge, props map[string][]byte) {
	tx.bufferIndexedEdgeUpdate(e, props, false)
}

// UpdateEdgePartialIndex is UpdateNodePartialIndex for edges.
func (tx *Tx) UpdateEdgePartialIndex(e *store.Edge, changed map[string][]byte) {
	tx.bufferIndexedEdgeUpdate(e, changed, true)
}

func (tx *Tx) bufferIndexedEdgeUpdate(e *store.Edge, props map[string][]byte, partial bool) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	if e == nil {
		tx.setErr(errors.New("Tx.UpdateEdgeIndexed: nil edge"))
		return
	}
	if e.ID == store.InvalidEdgeID {
		tx.setErr(errors.New("Tx.UpdateEdgeIndexed: edge ID is unset"))
		return
	}
	tx.ops = append(tx.ops, store.TxOp{
		Kind:    store.TxOpUpdateEdgeIndexed,
		Edge:    copyEdge(e),
		EdgeID:  e.ID,
		Props:   copyProps(props),
		Partial: partial,
	})
}

// IndexNodeProperties buffers property-index entries for id.
//
// Topology and index become durable together or not at all. Registering them
// outside the transaction — which was the only option before, and which the
// documentation used to instruct — leaves a window in which a crash produces
// nodes whose records are correct and whose keys resolve to nothing. Such a node
// cannot be found again by the key it was written under, so a later re-ingest
// creates a second one, and the duplicate is the lasting damage rather than the
// crash.
//
// The entries are copied, so the caller may reuse the map and its values as soon
// as this returns. A key registered twice in one transaction keeps the later
// value, matching the last-write-wins rule the log already applies to records.
func (tx *Tx) IndexNodeProperties(id store.NodeID, props map[string][]byte) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	if len(props) == 0 {
		return
	}
	tx.ops = append(tx.ops, store.TxOp{
		Kind:   store.TxOpIndexNode,
		NodeID: id,
		Props:  copyProps(props),
	})
}

// IndexEdgeProperties buffers property-index entries for id. See
// IndexNodeProperties.
func (tx *Tx) IndexEdgeProperties(id store.EdgeID, props map[string][]byte) {
	if tx.done {
		tx.setErr(ErrTxDone)
		return
	}
	if len(props) == 0 {
		return
	}
	tx.ops = append(tx.ops, store.TxOp{
		Kind:   store.TxOpIndexEdge,
		EdgeID: id,
		Props:  copyProps(props),
	})
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
		if len(tx.checks) > 0 {
			// A read set is a constraint, not a hint: committing without
			// validating it would hand back the guarantee the caller asked for
			// and none of the behaviour. Both bundled backends implement this.
			ct, ok := tx.tr.(store.CheckedTransactor)
			if !ok {
				return fmt.Errorf("Tx.Commit: %T cannot validate a read set; use Begin rather than BeginTracked, or accept the risk explicitly", tx.g.GraphStore)
			}
			return ct.ApplyTransactionChecked(tx.ops, tx.checks, tx.actor)
		}
		// Route through the attributed path only when there is something to
		// attribute, so a backend that implements both interfaces sees exactly
		// the call it saw before As existed.
		if at, ok := tx.tr.(store.ActorTransactor); ok && !tx.actor.Unattributed() {
			return at.ApplyTransactionAs(tx.ops, tx.actor)
		}
		return tx.tr.ApplyTransaction(tx.ops)
	}
	if len(tx.checks) > 0 {
		return fmt.Errorf("Tx.Commit: %T does not implement store.Transactor, so a read set cannot be validated under a write lock it does not have", tx.g.GraphStore)
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
	tx.checks = nil
	tx.seen = nil
	tx.ov = nil
	return nil
}

// copyProps deep-copies an index-entry map, values included. The store must not
// retain caller memory, and an index entry outlives the call that registered it
// by definition.
func copyProps(props map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(props))
	for k, v := range props {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
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

		case store.TxOpIndexNode:
			err = tx.g.IndexNodeProperties(realNode(op.NodeID), op.Props)

		case store.TxOpIndexEdge:
			err = tx.g.IndexEdgeProperties(realEdge(op.EdgeID), op.Props)

		case store.TxOpUpdateNodeIndexed:
			n := *op.Node
			n.ID = realNode(n.ID)
			if err = tx.g.UpdateNode(&n); err == nil {
				err = tx.g.reindexNode(n.ID, op.Props, op.Partial)
			}

		case store.TxOpUpdateEdgeIndexed:
			e := *op.Edge
			e.ID = realEdge(e.ID)
			if err = tx.g.UpdateEdge(&e); err == nil {
				err = tx.g.reindexEdge(e.ID, op.Props, op.Partial)
			}

		case store.TxOpUpsertNode, store.TxOpUpsertEdge:
			// An upsert resolves a key under the write lock and refuses on a
			// conflict; a backend with no transaction has no such lock to hold,
			// so replaying one here would be a create-or-update with the check
			// quietly dropped. Refusing is the honest answer, and Atomic already
			// tells a caller which backends take this path.
			err = fmt.Errorf("%s is not supported on %T, which does not implement store.Transactor",
				op.Kind, tx.g.GraphStore)

		default:
			err = fmt.Errorf("unknown op kind %d", op.Kind)
		}
		if err != nil {
			return fmt.Errorf("Tx.Commit: op %d (%s): %w", i, op.Kind, err)
		}
	}
	return nil
}
