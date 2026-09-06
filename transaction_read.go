package graphene

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aoiflux/graphene/store"
)

// Reading inside a transaction.
//
// Two separate things live in this file, and they are separate on purpose.
//
// **Read-your-own-writes** is an overlay: the buffered operations replayed in
// order over whatever the store answers, so a transaction sees the graph as it
// will be once it commits. It is always on, costs nothing until the first read,
// and works on any backend — a store that cannot do transactions natively still
// answers reads, and the overlay is caller-side arithmetic on top.
//
// **Read-set tracking** is opt-in, through BeginTracked. It records what the
// store said, so Commit can refuse if it has since changed. It is off by
// default because it introduces a failure a caller did not have before: code
// that upgraded and started reading inside its transactions would begin seeing
// ErrWriteConflict from commits that used to succeed. Reads are available on
// both kinds of transaction; only the recording differs.
//
// The two are independent by construction. The overlay answers from the
// transaction's own buffer and records nothing, because a value this
// transaction wrote cannot be invalidated by another writer — the write lock
// resolves that, and it is the same argument that lets an untracked transaction
// commit unprotected. Only a read that reached the store is recorded.

// Tx deliberately does not implement store.GraphReader. A Tx is not safe for
// concurrent use, and its reads have a side effect on a tracked transaction, so
// handing one to a traversal that expected a plain reader would be a trap. Pass
// g or a Snapshot for that.

// txOverlay is the transaction's buffered writes, indexed for reading.
//
// It is rebuilt lazily by replaying tx.ops, so a transaction that never reads
// never builds one. upTo is how far the replay has got; buffering methods do
// not touch it, which is what keeps the read machinery out of the write path.
type txOverlay struct {
	upTo int

	nodes    map[store.NodeID]*store.Node
	edges    map[store.EdgeID]*store.Edge
	delNodes map[store.NodeID]struct{}
	delEdges map[store.EdgeID]struct{}

	// incident lists the buffered edges touching each node, so an adjacency read
	// costs the node's pending degree rather than a walk of the whole operation
	// list. It is the same structure, built the same way, as txView.incident in
	// both backends. Entries are never removed: a deleted edge leaves the store
	// map, and the read filters on that.
	incident map[store.NodeID][]store.EdgeID

	// owned records which entity this transaction has registered a
	// declared-unique value against, so a unique lookup can see a claim the
	// index has not been told about yet.
	owned map[txClaim]uint64

	// uniqueNode and uniqueEdge are the backend's declared unique key sets,
	// resolved once. A nil map means the backend cannot declare unique keys at
	// all, in which case no op can be registering one.
	uniqueNode map[string]struct{}
	uniqueEdge map[string]struct{}
	keysLoaded bool
}

func (tx *Tx) overlayFor() *txOverlay {
	if tx.ov == nil {
		tx.ov = &txOverlay{
			nodes:    make(map[store.NodeID]*store.Node),
			edges:    make(map[store.EdgeID]*store.Edge),
			delNodes: make(map[store.NodeID]struct{}),
			delEdges: make(map[store.EdgeID]struct{}),
			incident: make(map[store.NodeID][]store.EdgeID),
			owned:    make(map[txClaim]uint64),
		}
	}
	tx.syncOverlay(tx.ov)
	return tx.ov
}

// loadUniqueKeys resolves the declared unique key sets, once per transaction.
//
// It is deliberately not resolved at Begin: most transactions never read, and
// the ones that do usually do not name a unique key. A declaration made *during*
// the transaction is therefore not seen, which is the right trade — declaring a
// constraint from inside a transaction that is also reading against it is not a
// thing the API supports in either direction.
func (ov *txOverlay) loadUniqueKeys(gs store.GraphStore) {
	if ov.keysLoaded {
		return
	}
	ov.keysLoaded = true
	d, ok := gs.(store.UniqueIndexDeclarer)
	if !ok {
		return
	}
	ov.uniqueNode = make(map[string]struct{})
	for _, k := range d.UniqueNodeProperties() {
		ov.uniqueNode[k] = struct{}{}
	}
	ov.uniqueEdge = make(map[string]struct{})
	for _, k := range d.UniqueEdgeProperties() {
		ov.uniqueEdge[k] = struct{}{}
	}
}

// syncOverlay replays every op buffered since the last read.
//
// Replaying in order is what makes the overlay agree with the backend: a
// transaction that adds a node and then deletes it has deleted it, and the two
// maps hold the net effect rather than the history.
func (tx *Tx) syncOverlay(ov *txOverlay) {
	if ov.upTo == len(tx.ops) {
		return
	}
	ov.loadUniqueKeys(tx.g.GraphStore)

	for _, op := range tx.ops[ov.upTo:] {
		switch op.Kind {
		case store.TxOpAddNode, store.TxOpUpdateNode:
			ov.putNode(op.Node)

		case store.TxOpUpdateNodeIndexed:
			ov.putNode(op.Node)
			ov.claimNode(op.Node.ID, op.Props)

		case store.TxOpUpsertNode:
			ov.putNode(op.Node)
			ov.claim(txClaim{kind: "node", key: op.Key, value: string(op.Value)}, uint64(op.NodeID))
			ov.claimNode(op.NodeID, op.Props)

		case store.TxOpAddEdge:
			ov.putEdge(op.Edge)

		case store.TxOpUpdateEdge:
			ov.putEdge(tx.pinEndpoints(ov, op.Edge))

		case store.TxOpUpdateEdgeIndexed:
			ov.putEdge(tx.pinEndpoints(ov, op.Edge))
			ov.claimEdge(op.Edge.ID, op.Props)

		case store.TxOpUpsertEdge:
			ov.putEdge(tx.pinEndpoints(ov, op.Edge))
			ov.claim(txClaim{kind: "edge", key: op.Key, value: string(op.Value)}, uint64(op.EdgeID))
			ov.claimEdge(op.EdgeID, op.Props)

		case store.TxOpDeleteNode:
			delete(ov.nodes, op.NodeID)
			ov.delNodes[op.NodeID] = struct{}{}

		case store.TxOpDeleteEdge:
			delete(ov.edges, op.EdgeID)
			ov.delEdges[op.EdgeID] = struct{}{}

		case store.TxOpIndexNode:
			ov.claimNode(op.NodeID, op.Props)

		case store.TxOpIndexEdge:
			ov.claimEdge(op.EdgeID, op.Props)
		}
	}
	ov.upTo = len(tx.ops)
}

// pinEndpoints returns e with the endpoints it will actually have once
// committed.
//
// An edge's endpoints are immutable, so both backends discard whatever Src and
// Dst an update carries and keep the ones the edge was created with. The
// overlay has to do the same, or a transaction that updated an edge would read
// back a record whose endpoints disagree with the adjacency it is listed in.
//
// The store lookup this needs is not recorded as a read: it resolves what the
// backend is going to do regardless, not something the caller decided from.
func (tx *Tx) pinEndpoints(ov *txOverlay, e *store.Edge) *store.Edge {
	var cur *store.Edge
	if pending, ok := ov.edges[e.ID]; ok {
		cur = pending
	} else if got, err := tx.g.GetEdge(e.ID); err == nil {
		cur = got
	}
	if cur == nil || (cur.Src == e.Src && cur.Dst == e.Dst) {
		return e
	}
	pinned := *e
	pinned.Src, pinned.Dst = cur.Src, cur.Dst
	return &pinned
}

func (ov *txOverlay) putNode(n *store.Node) {
	if n == nil {
		return
	}
	ov.nodes[n.ID] = n
	delete(ov.delNodes, n.ID)
}

func (ov *txOverlay) putEdge(e *store.Edge) {
	if e == nil {
		return
	}
	if _, known := ov.edges[e.ID]; !known {
		ov.incident[e.Src] = append(ov.incident[e.Src], e.ID)
		if e.Dst != e.Src {
			ov.incident[e.Dst] = append(ov.incident[e.Dst], e.ID)
		}
	}
	ov.edges[e.ID] = e
	delete(ov.delEdges, e.ID)
}

func (ov *txOverlay) claim(c txClaim, id uint64) { ov.owned[c] = id }

func (ov *txOverlay) claimNode(id store.NodeID, props map[string][]byte) {
	for k, v := range props {
		if _, unique := ov.uniqueNode[k]; unique {
			ov.owned[txClaim{kind: "node", key: k, value: string(v)}] = uint64(id)
		}
	}
}

func (ov *txOverlay) claimEdge(id store.EdgeID, props map[string][]byte) {
	for k, v := range props {
		if _, unique := ov.uniqueEdge[k]; unique {
			ov.owned[txClaim{kind: "edge", key: k, value: string(v)}] = uint64(id)
		}
	}
}

// nodeGone reports whether this transaction has deleted a node.
func (ov *txOverlay) nodeGone(id store.NodeID) bool {
	_, gone := ov.delNodes[id]
	return gone
}

// edgeGone reports whether this transaction has removed an edge, directly or by
// deleting one of its endpoints.
//
// The cascade is resolved here rather than precomputed because computing it
// eagerly would mean listing a deleted node's incident edges out of the store at
// buffer time — which is exactly the read both backends refuse to do early, on
// the grounds that the graph can still change before the transaction commits.
func (ov *txOverlay) edgeGone(e *store.Edge) bool {
	if e == nil {
		return true
	}
	if _, gone := ov.delEdges[e.ID]; gone {
		return true
	}
	return ov.nodeGone(e.Src) || ov.nodeGone(e.Dst)
}

// readKey identifies one observation, so the same read made twice is recorded
// once.
type readKey struct {
	kind  store.ReadRefKind
	id    uint64
	key   string
	value string
	dir   store.Direction
	types string
}

// record adds a check to the read set, if this transaction is tracking.
//
// A repeated observation keeps the first: it is what the earliest decision in
// the transaction was made from, and if the record has moved since, that first
// check is the one that must fail.
func (tx *Tx) record(c store.ReadCheck) {
	if !tx.tracked {
		return
	}
	k := readKey{kind: c.Kind, id: c.ID, key: c.Key, value: string(c.Value), dir: c.Dir, types: edgeTypeKey(c.Types)}
	if c.Kind == store.ReadUniqueNodeOwner || c.Kind == store.ReadUniqueEdgeOwner {
		// The owner is the answer, not the question, so two lookups of one key
		// that returned different owners must both be recorded — otherwise the
		// contradiction they represent is silently dropped.
		k.id = 0
	}
	if tx.seen == nil {
		tx.seen = make(map[readKey]struct{})
	}
	if _, dup := tx.seen[k]; dup {
		return
	}
	tx.seen[k] = struct{}{}
	tx.checks = append(tx.checks, c)
}

func edgeTypeKey(types []store.EdgeType) string {
	if len(types) == 0 {
		return ""
	}
	var b strings.Builder
	for i, t := range types {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(uint64(t), 10))
	}
	return b.String()
}

// Tracked reports whether this transaction is recording a read set. It is true
// only for a transaction started with BeginTracked.
func (tx *Tx) Tracked() bool { return tx.tracked }

// Reads reports how many distinct observations the read set holds. It is always
// 0 on an untracked transaction.
func (tx *Tx) Reads() int { return len(tx.checks) }

// GetNode returns a node as this transaction sees it.
//
// A node this transaction has added, updated or upserted comes back as it will
// be once committed; one it has deleted is reported missing. Everything else is
// read from the store, and on a tracked transaction that read is recorded: if
// the record changes or disappears before Commit, the transaction is refused
// with store.ErrWriteConflict.
//
// The returned node is the transaction's own copy for a buffered write, and the
// store's for anything else — treat it as read-only in both cases, and copy it
// before mutating it into an update.
func (tx *Tx) GetNode(id store.NodeID) (*store.Node, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	ov := tx.overlayFor()
	if ov.nodeGone(id) {
		return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	if n, ok := ov.nodes[id]; ok {
		return n, nil
	}

	n, err := tx.readNode(id)
	if err != nil {
		if isNotFound(err) {
			tx.record(store.ReadCheck{Kind: store.ReadNode, ID: uint64(id)})
		}
		return nil, err
	}
	tx.record(store.ReadCheck{
		Kind:   store.ReadNode,
		ID:     uint64(id),
		Exists: true,
		Digest: store.NodeDigest(n),
	})
	return n, nil
}

// NodeExists reports whether a node is live as this transaction sees it,
// without materialising the record.
//
// On a tracked transaction this records a weaker observation than GetNode: only
// that the node was, or was not, there. A node that changed but did not
// disappear does not conflict with it, which is the honest reading of what was
// asked.
func (tx *Tx) NodeExists(id store.NodeID) (bool, error) {
	if tx.done {
		return false, ErrTxDone
	}
	ov := tx.overlayFor()
	if ov.nodeGone(id) {
		return false, nil
	}
	if _, ok := ov.nodes[id]; ok {
		return true, nil
	}

	exists := tx.readNodeExists(id)
	tx.record(store.ReadCheck{Kind: store.ReadNodeExists, ID: uint64(id), Exists: exists})
	return exists, nil
}

// GetEdge returns an edge as this transaction sees it.
//
// An edge is reported missing when this transaction deleted it *or* deleted
// either of its endpoints, because deletion cascades and the transaction is
// entitled to see its own cascade.
func (tx *Tx) GetEdge(id store.EdgeID) (*store.Edge, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	ov := tx.overlayFor()
	if _, gone := ov.delEdges[id]; gone {
		return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
	}
	if e, ok := ov.edges[id]; ok {
		if ov.edgeGone(e) {
			return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
		}
		return e, nil
	}

	e, err := tx.readEdge(id)
	if err != nil {
		if isNotFound(err) {
			tx.record(store.ReadCheck{Kind: store.ReadEdge, ID: uint64(id)})
		}
		return nil, err
	}
	tx.record(store.ReadCheck{
		Kind:   store.ReadEdge,
		ID:     uint64(id),
		Exists: true,
		Digest: store.EdgeDigest(e),
	})
	if ov.edgeGone(e) {
		return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
	}
	return e, nil
}

// EdgesOf returns the edges incident to id in the given direction, filtered by
// edgeTypes (nil means all types), as this transaction sees them.
//
// Edges this transaction added are included, edges it deleted or cascaded away
// are not, and an edge it updated is returned with its new contents. Ordering
// follows the store's for the edges that came from there, with this
// transaction's own additions after them.
//
// A node that does not exist has no incident edges, and this reports that as an
// empty result on both backends rather than the error the in-memory store's own
// EdgesOf returns — matching what the read set validates, so the two cannot
// disagree about the same read.
//
// On a tracked transaction this records the whole incident set, contents
// included, so an edge appearing, disappearing or changing under the
// transaction is a conflict. That makes "does this node already have such an
// edge — if not, create one" safe, which it is not with a bare read. Note that
// a store-level constraint is the better answer where one fits: see
// DeclareUniqueEdge.
func (tx *Tx) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	ov := tx.overlayFor()

	base, err := tx.readEdgesOf(id, dir, edgeTypes)
	if err != nil && !isNotFound(err) {
		return nil, err
	}
	// A missing node is the empty incident set, not an error, and both backends'
	// validators agree on that — the two disagree on whether EdgesOf itself
	// reports it, which is a difference this must not inherit into the read set.
	tx.record(store.ReadCheck{
		Kind:   store.ReadNodeAdjacency,
		ID:     uint64(id),
		Digest: store.EdgeSetDigest(base),
		Dir:    dir,
		Types:  cloneEdgeTypes(edgeTypes),
	})

	if ov.nodeGone(id) {
		return nil, nil
	}

	pending := ov.incident[id]
	out := make([]*store.Edge, 0, len(base)+len(pending))
	seen := make(map[store.EdgeID]struct{}, len(base)+len(pending))
	for _, e := range base {
		seen[e.ID] = struct{}{}
		if buffered, ok := ov.edges[e.ID]; ok {
			// An update may have changed the labels, so the filter is applied
			// again below against what the edge will actually be.
			e = buffered
		}
		if ov.edgeGone(e) || !incidentTo(e, id, dir) || !edgeMatchesTypes(e, edgeTypes) {
			continue
		}
		out = append(out, e)
	}
	for _, eid := range pending {
		if _, already := seen[eid]; already {
			continue
		}
		seen[eid] = struct{}{}
		e, live := ov.edges[eid]
		if !live || ov.edgeGone(e) || !incidentTo(e, id, dir) || !edgeMatchesTypes(e, edgeTypes) {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// Neighbours returns the distinct nodes reachable from id in one step, as this
// transaction sees them.
//
// It is EdgesOf followed by a lookup per far endpoint, so on a tracked
// transaction it records the incident set *and* each neighbouring node it
// returned. Endpoints that do not resolve to a live node are dropped, matching
// what the store does.
func (tx *Tx) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	edges, err := tx.EdgesOf(id, dir, edgeTypes)
	if err != nil {
		return nil, err
	}

	results := make([]store.NeighbourResult, 0, len(edges))
	seen := make(map[store.NodeID]struct{}, len(edges))
	for _, e := range edges {
		far := e.Dst
		if e.Src != id {
			far = e.Src
		}
		if _, dup := seen[far]; dup {
			continue
		}
		seen[far] = struct{}{}

		n, err := tx.GetNode(far)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, err
		}
		results = append(results, store.NeighbourResult{Node: n, Edge: e})
	}
	return results, nil
}

// UniqueNodeOwner returns the node holding value under a declared-unique key,
// as this transaction sees it.
//
// A value this transaction has claimed — by upserting it, or by registering it
// as an index entry — resolves to the entity it claimed it for, which the store
// cannot answer for because nothing has been registered there yet. A node this
// transaction deleted holds nothing.
//
// On a tracked transaction this records the answer, so a key taken, released or
// moved by another writer before Commit is a conflict. That is the read half of
// the guarantee UpsertNode already gives on the write side.
func (tx *Tx) UniqueNodeOwner(key string, value []byte) (store.NodeID, bool, error) {
	if tx.done {
		return store.InvalidNodeID, false, ErrTxDone
	}
	ov := tx.overlayFor()
	if id, ok := ov.owned[txClaim{kind: "node", key: key, value: string(value)}]; ok {
		if ov.nodeGone(store.NodeID(id)) {
			return store.InvalidNodeID, false, nil
		}
		return store.NodeID(id), true, nil
	}

	d, ok := tx.g.GraphStore.(store.UniqueIndexDeclarer)
	if !ok {
		return store.InvalidNodeID, false, errNoUniqueKeys(tx.g.GraphStore)
	}
	owner, found, err := d.UniqueNodeOwner(key, value)
	if err != nil {
		return store.InvalidNodeID, false, err
	}
	tx.record(store.ReadCheck{
		Kind:   store.ReadUniqueNodeOwner,
		ID:     uint64(owner),
		Exists: found,
		Key:    key,
		Value:  bytes.Clone(value),
	})
	if found && ov.nodeGone(owner) {
		return store.InvalidNodeID, false, nil
	}
	return owner, found, nil
}

// UniqueEdgeOwner is UniqueNodeOwner for edges.
func (tx *Tx) UniqueEdgeOwner(key string, value []byte) (store.EdgeID, bool, error) {
	if tx.done {
		return store.InvalidEdgeID, false, ErrTxDone
	}
	ov := tx.overlayFor()
	if id, ok := ov.owned[txClaim{kind: "edge", key: key, value: string(value)}]; ok {
		if e, live := ov.edges[store.EdgeID(id)]; live && ov.edgeGone(e) {
			return store.InvalidEdgeID, false, nil
		}
		if _, gone := ov.delEdges[store.EdgeID(id)]; gone {
			return store.InvalidEdgeID, false, nil
		}
		return store.EdgeID(id), true, nil
	}

	d, ok := tx.g.GraphStore.(store.UniqueIndexDeclarer)
	if !ok {
		return store.InvalidEdgeID, false, errNoUniqueKeys(tx.g.GraphStore)
	}
	owner, found, err := d.UniqueEdgeOwner(key, value)
	if err != nil {
		return store.InvalidEdgeID, false, err
	}
	tx.record(store.ReadCheck{
		Kind:   store.ReadUniqueEdgeOwner,
		ID:     uint64(owner),
		Exists: found,
		Key:    key,
		Value:  bytes.Clone(value),
	})
	if found {
		if e, err := tx.readEdge(owner); err == nil && ov.edgeGone(e) {
			return store.InvalidEdgeID, false, nil
		}
	}
	return owner, found, nil
}

// --- where a transaction's reads come from ---
//
// A tracked transaction reads from the view its commit will be validated
// against, and an untracked one reads from the ordinary one. That difference is
// the whole of store.AppliedReader, and its doc comment is where the reasoning
// is: a tracked read taken from a view older than the validator's cannot ever
// satisfy the validator while a commit is in flight, and a retry loop built on
// one does not terminate.
//
// An untracked transaction is deliberately left alone. It has no read set, so
// nothing it reads can refuse it, and there is no reason to show it state the
// disk has not accepted yet.

// appliedReads returns the backend's applied-state reader, or nil when this
// transaction should not or cannot use one.
func (tx *Tx) appliedReads() store.AppliedReader {
	if !tx.tracked {
		return nil
	}
	ar, _ := tx.g.GraphStore.(store.AppliedReader)
	return ar
}

func (tx *Tx) readNode(id store.NodeID) (*store.Node, error) {
	if ar := tx.appliedReads(); ar != nil {
		return ar.AppliedNode(id)
	}
	return tx.g.GetNode(id)
}

func (tx *Tx) readEdge(id store.EdgeID) (*store.Edge, error) {
	if ar := tx.appliedReads(); ar != nil {
		return ar.AppliedEdge(id)
	}
	return tx.g.GetEdge(id)
}

func (tx *Tx) readEdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error) {
	if ar := tx.appliedReads(); ar != nil {
		return ar.AppliedEdgesOf(id, dir, edgeTypes)
	}
	return tx.g.EdgesOf(id, dir, edgeTypes)
}

// readNodeExists is Graph.nodeExists against the same view, materialising the
// record only when the backend has no applied-state reader to ask.
func (tx *Tx) readNodeExists(id store.NodeID) bool {
	if ar := tx.appliedReads(); ar != nil {
		_, err := ar.AppliedNode(id)
		return err == nil
	}
	return tx.g.nodeExists(id)
}

// nodeExists asks the backend directly when it can answer without building the
// record, and falls back to GetNode when it cannot.
func (g *Graph) nodeExists(id store.NodeID) bool {
	if a, ok := g.GraphStore.(store.AdjacencyReader); ok {
		return a.NodeExists(id)
	}
	_, err := g.GetNode(id)
	return err == nil
}

func errNoUniqueKeys(gs store.GraphStore) error {
	return fmt.Errorf("graphene: %T cannot enforce unique properties", gs)
}

func isNotFound(err error) bool {
	var nf *store.ErrNotFound
	return errors.As(err, &nf)
}

func incidentTo(e *store.Edge, id store.NodeID, dir store.Direction) bool {
	switch dir {
	case store.DirectionOutbound:
		return e.Src == id
	case store.DirectionInbound:
		return e.Dst == id
	default:
		return e.Src == id || e.Dst == id
	}
}

func edgeMatchesTypes(e *store.Edge, types []store.EdgeType) bool {
	if len(types) == 0 {
		return true
	}
	for _, t := range types {
		if e.HasLabel(t) {
			return true
		}
	}
	return false
}

func cloneEdgeTypes(types []store.EdgeType) []store.EdgeType {
	if len(types) == 0 {
		return nil
	}
	out := make([]store.EdgeType, len(types))
	copy(out, types)
	return out
}
