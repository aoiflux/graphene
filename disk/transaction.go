package disk

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/aoiflux/graphene/index"
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
	// Index registration inside the transaction. The record types these frame
	// into (0x03/0x04) predate transactions and already replay; what was missing
	// was a way to put them inside a batch, so that a key lookup and the record
	// it resolves to become durable together.
	txActionIndexNode
	txActionIndexEdge
)

// propEntry is one property-index registration, resolved.
type propEntry struct {
	key   string
	value []byte
}

// txAction is a resolved primitive: no conditionals left, nothing to look up.
type txAction struct {
	kind   txActionKind
	node   *store.Node
	edge   *store.Edge
	nodeID store.NodeID
	edgeID store.EdgeID

	// props is set for txActionIndexNode / txActionIndexEdge, in ascending key
	// order. Sorted rather than map-ordered because framing and applying walk
	// this same slice, and a map's randomised order would make two identical
	// transactions write different bytes — which compaction determinism, and any
	// byte-level comparison of two stores, both rest on.
	props []propEntry
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

	// claims records the unique values this transaction has taken but not yet
	// applied. The index cannot answer for them: nothing has been registered
	// there yet, so two upserts of one key in a single transaction would each
	// find the value free and each create an entity.
	claims map[uniqueClaim]uint64

	// pairs are the edge-cardinality slots taken but not yet applied, and
	// pairsOf is the reverse index that makes releasing them cheap. See
	// disk/edgeunique.go.
	pairs   map[pairSlot]store.EdgeID
	pairsOf map[store.EdgeID][]pairSlot
}

// uniqueClaim identifies one value under one unique key, on one side of the
// graph. Kind is part of the identity because node and edge keys are declared
// independently and may collide by name without meaning the same thing.
type uniqueClaim struct {
	kind  string // "node" or "edge"
	key   string
	value string
}

func newTxView(s *Store, hint int) *txView {
	return &txView{
		s:        s,
		nodes:    make(map[store.NodeID]*store.Node, hint),
		edges:    make(map[store.EdgeID]*store.Edge, hint),
		delNode:  make(map[store.NodeID]struct{}),
		delEdge:  make(map[store.EdgeID]struct{}),
		incident: make(map[store.NodeID][]store.EdgeID),
		claims:   make(map[uniqueClaim]uint64),
		pairs:    make(map[pairSlot]store.EdgeID),
		pairsOf:  make(map[store.EdgeID][]pairSlot),
	}
}

// claimUniqueNode reserves every entry under a declared-unique key for id, or
// reports the incumbent.
//
// Refusing here rather than at apply time is what makes the transaction atomic
// with respect to the constraint: resolution has written nothing, so a rejection
// leaves the store exactly as it found it. Discovering the conflict while
// applying would mean half a transaction already in the log.
func (v *txView) claimUniqueNode(id store.NodeID, entries []propEntry) error {
	for _, e := range entries {
		if !v.s.propIdx.IsUniqueNodeKey(e.key) {
			continue
		}
		c := uniqueClaim{kind: "node", key: e.key, value: string(e.value)}
		if owner, taken := v.claims[c]; taken {
			if owner != uint64(id) {
				return &index.ErrUniqueTaken{Key: e.key, Value: e.value, Owner: owner}
			}
			continue
		}
		if owner, held := v.s.propIdx.NodeUniqueOwner(e.key, e.value); held && owner != id {
			return &index.ErrUniqueTaken{Key: e.key, Value: e.value, Owner: uint64(owner)}
		}
		v.claims[c] = uint64(id)
	}
	return nil
}

// claimUniqueEdge is claimUniqueNode for edges.
func (v *txView) claimUniqueEdge(id store.EdgeID, entries []propEntry) error {
	for _, e := range entries {
		if !v.s.propIdx.IsUniqueEdgeKey(e.key) {
			continue
		}
		c := uniqueClaim{kind: "edge", key: e.key, value: string(e.value)}
		if owner, taken := v.claims[c]; taken {
			if owner != uint64(id) {
				return &index.ErrUniqueTaken{Key: e.key, Value: e.value, Owner: owner}
			}
			continue
		}
		if owner, held := v.s.propIdx.EdgeUniqueOwner(e.key, e.value); held && owner != id {
			return &index.ErrUniqueTaken{Key: e.key, Value: e.value, Owner: uint64(owner)}
		}
		v.claims[c] = uint64(id)
	}
	return nil
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

// currentEdge resolves id to the edge this transaction would read: the version
// it has written itself, else the store's. Used to carry immutable fields across
// an update.
func (v *txView) currentEdge(id store.EdgeID) (*store.Edge, bool) {
	if _, gone := v.delEdge[id]; gone {
		return nil, false
	}
	if e, here := v.edges[id]; here {
		return e, true
	}
	return v.s.getEdgeLocked(id)
}

// uniqueNodeOwner resolves key=value to the live node holding it, as this
// transaction sees things.
//
// Claims made earlier in the same transaction come first: the index cannot
// answer for them, because nothing has been registered there yet. Liveness is
// checked against the transaction's view too, so a node this transaction has
// already deleted does not hold anything.
func (v *txView) uniqueNodeOwner(key string, value []byte) (store.NodeID, bool) {
	if id, ok := v.claims[uniqueClaim{kind: "node", key: key, value: string(value)}]; ok {
		return store.NodeID(id), true
	}
	owner, held := v.s.propIdx.NodeUniqueOwner(key, value)
	if !held || !v.nodeLive(owner) {
		return store.InvalidNodeID, false
	}
	return owner, true
}

// uniqueEdgeOwner is uniqueNodeOwner for edges.
func (v *txView) uniqueEdgeOwner(key string, value []byte) (store.EdgeID, bool) {
	if id, ok := v.claims[uniqueClaim{kind: "edge", key: key, value: string(value)}]; ok {
		return store.EdgeID(id), true
	}
	owner, held := v.s.propIdx.EdgeUniqueOwner(key, value)
	if !held || !v.edgeLive(owner) {
		return store.InvalidEdgeID, false
	}
	return owner, true
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
				return nil, fmt.Errorf("transaction op %d (%s): node %d: %w", i, op.Kind, n.ID, store.ErrNoLabels)
			}
			if op.Kind == store.TxOpUpdateNode && !v.nodeLive(n.ID) {
				return nil, &store.ErrNotFound{Kind: "node", ID: uint64(n.ID)}
			}
			v.nodes[n.ID] = n
			delete(v.delNode, n.ID)
			actions = append(actions, txAction{kind: txActionPutNode, node: n})
			if op.Kind == store.TxOpUpdateNode && s.reindexPolicy == store.ReindexReject && s.propIdx.NodeHasEntries(n.ID) {
				return nil, fmt.Errorf("transaction op %d (%s): node %d: %w", i, op.Kind, n.ID, store.ErrIndexedPropertiesRequired)
			}
			if op.Kind == store.TxOpUpdateNode && s.reindexPolicy == store.ReindexPurge {
				actions = append(actions, txAction{kind: txActionPurgeNodeIndex, nodeID: n.ID})
			}

		case store.TxOpAddEdge, store.TxOpUpdateEdge:
			e := op.Edge
			if e == nil {
				return nil, fmt.Errorf("transaction op %d (%s): nil edge", i, op.Kind)
			}
			if len(e.Labels) == 0 {
				return nil, fmt.Errorf("transaction op %d (%s): edge %d: %w", i, op.Kind, e.ID, store.ErrNoLabels)
			}
			if op.Kind == store.TxOpUpdateEdge {
				cur, live := v.currentEdge(e.ID)
				if !live {
					return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(e.ID)}
				}
				// Endpoints are immutable, exactly as Store.UpdateEdge treats
				// them. Honouring a changed Src or Dst here would store a record
				// whose endpoints disagree with the adjacency lists that were
				// built from the original ones — and would diverge from the
				// non-transactional path, which this must match.
				if e.Src != cur.Src || e.Dst != cur.Dst {
					pinned := *e
					pinned.Src, pinned.Dst = cur.Src, cur.Dst
					e = &pinned
				}
			}
			if !v.nodeLive(e.Src) {
				return nil, &store.ErrInvalidEdge{MissingID: e.Src}
			}
			if !v.nodeLive(e.Dst) {
				return nil, &store.ErrInvalidEdge{MissingID: e.Dst}
			}
			// An update may have changed the labels, so whatever this edge was
			// holding is released before it claims again.
			v.releaseEdgePairs(e.ID)
			if err := v.claimEdgePairs(e); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
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
			if op.Kind == store.TxOpUpdateEdge && s.reindexPolicy == store.ReindexReject && s.propIdx.EdgeHasEntries(e.ID) {
				return nil, fmt.Errorf("transaction op %d (%s): edge %d: %w", i, op.Kind, e.ID, store.ErrIndexedPropertiesRequired)
			}
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
				v.releaseEdgePairs(eid)
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
			v.releaseEdgePairs(op.EdgeID)
			delete(v.edges, op.EdgeID)
			actions = append(actions, txAction{kind: txActionDelEdge, edgeID: op.EdgeID})

		case store.TxOpIndexNode:
			if !v.nodeLive(op.NodeID) {
				return nil, &store.ErrNotFound{Kind: "node", ID: uint64(op.NodeID)}
			}
			entries, err := resolveProps(op.Props)
			if err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if err := v.claimUniqueNode(op.NodeID, entries); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if len(entries) > 0 {
				actions = append(actions, txAction{kind: txActionIndexNode, nodeID: op.NodeID, props: entries})
			}

		case store.TxOpIndexEdge:
			if !v.edgeLive(op.EdgeID) {
				return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(op.EdgeID)}
			}
			entries, err := resolveProps(op.Props)
			if err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if err := v.claimUniqueEdge(op.EdgeID, entries); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if len(entries) > 0 {
				actions = append(actions, txAction{kind: txActionIndexEdge, edgeID: op.EdgeID, props: entries})
			}

		case store.TxOpUpsertNode:
			n := op.Node
			if n == nil {
				return nil, fmt.Errorf("transaction op %d (%s): nil node", i, op.Kind)
			}
			if len(n.Labels) == 0 {
				return nil, fmt.Errorf("transaction op %d (%s): node %d: %w", i, op.Kind, n.ID, store.ErrNoLabels)
			}
			if !v.s.propIdx.IsUniqueNodeKey(op.Key) {
				return nil, fmt.Errorf("transaction op %d (%s): %w: %q", i, op.Kind, store.ErrKeyNotUnique, op.Key)
			}
			// The buffer-time resolution, re-checked. A key that moved under the
			// transaction means another writer created or removed the entity it
			// named, and the ID already handed to the caller — and possibly
			// already used as an edge endpoint — is no longer the right one.
			// There is nothing to repair from here, so the transaction is refused
			// whole and the caller retries against what the winner wrote.
			owner, found := v.uniqueNodeOwner(op.Key, op.Value)
			if found != (store.NodeID(op.Expect) != store.InvalidNodeID) || (found && owner != store.NodeID(op.Expect)) {
				return nil, fmt.Errorf("transaction op %d (%s): key %q value %q: %w",
					i, op.Kind, op.Key, op.Value, store.ErrWriteConflict)
			}
			v.nodes[n.ID] = n
			delete(v.delNode, n.ID)
			actions = append(actions, txAction{kind: txActionPutNode, node: n})

			// The reindex policy is deliberately not consulted. It exists to
			// decide what an update does to entries it cannot reason about; an
			// upsert states what happens to them, so there is nothing to decide.
			// A key the caller named is *replaced*, not added to. Adding would
			// leave the superseded value still matching — which for a field
			// used as a work queue means the entity is handed back to the
			// worker forever, and is the exact failure this release exists to
			// remove.
			if err := checkPropsKeys(op.Props); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			var current []index.PropEntry
			if found {
				current = v.s.propIdx.NodeEntriesOf(n.ID)
			}
			entries, changed := mergedEntries(current, withKeyEntry(op.Key, op.Value, op.Props))
			if err := v.claimUniqueNode(n.ID, entries); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if changed {
				if len(current) > 0 {
					actions = append(actions, txAction{kind: txActionPurgeNodeIndex, nodeID: n.ID})
				}
				actions = append(actions, txAction{kind: txActionIndexNode, nodeID: n.ID, props: entries})
			}

		case store.TxOpUpsertEdge:
			e := op.Edge
			if e == nil {
				return nil, fmt.Errorf("transaction op %d (%s): nil edge", i, op.Kind)
			}
			if len(e.Labels) == 0 {
				return nil, fmt.Errorf("transaction op %d (%s): edge %d: %w", i, op.Kind, e.ID, store.ErrNoLabels)
			}
			if !v.s.propIdx.IsUniqueEdgeKey(op.Key) {
				return nil, fmt.Errorf("transaction op %d (%s): %w: %q", i, op.Kind, store.ErrKeyNotUnique, op.Key)
			}
			owner, found := v.uniqueEdgeOwner(op.Key, op.Value)
			if found != (store.EdgeID(op.Expect) != store.InvalidEdgeID) || (found && owner != store.EdgeID(op.Expect)) {
				return nil, fmt.Errorf("transaction op %d (%s): key %q value %q: %w",
					i, op.Kind, op.Key, op.Value, store.ErrWriteConflict)
			}
			if cur, live := v.currentEdge(e.ID); live {
				// Endpoints are immutable, as everywhere else.
				if e.Src != cur.Src || e.Dst != cur.Dst {
					pinned := *e
					pinned.Src, pinned.Dst = cur.Src, cur.Dst
					e = &pinned
				}
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

			if err := checkPropsKeys(op.Props); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			var current []index.PropEntry
			if found {
				current = v.s.propIdx.EdgeEntriesOf(e.ID)
			}
			entries, changed := mergedEntries(current, withKeyEntry(op.Key, op.Value, op.Props))
			if err := v.claimUniqueEdge(e.ID, entries); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if changed {
				if len(current) > 0 {
					actions = append(actions, txAction{kind: txActionPurgeEdgeIndex, edgeID: e.ID})
				}
				actions = append(actions, txAction{kind: txActionIndexEdge, edgeID: e.ID, props: entries})
			}

		case store.TxOpUpdateNodeIndexed:
			n := op.Node
			if n == nil {
				return nil, fmt.Errorf("transaction op %d (%s): nil node", i, op.Kind)
			}
			if len(n.Labels) == 0 {
				return nil, fmt.Errorf("transaction op %d (%s): node %d: %w", i, op.Kind, n.ID, store.ErrNoLabels)
			}
			if !v.nodeLive(n.ID) {
				return nil, &store.ErrNotFound{Kind: "node", ID: uint64(n.ID)}
			}
			if err := checkPropsKeys(op.Props); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			// The record and its entries move together, which is what makes
			// the ReindexPolicy irrelevant here: there is nothing left for it to
			// decide, so this is not subject to the reject an ordinary update is.
			v.nodes[n.ID] = n
			actions = append(actions, txAction{kind: txActionPutNode, node: n})

			current := v.s.propIdx.NodeEntriesOf(n.ID)
			var entries []propEntry
			var changed bool
			if op.Partial {
				entries, changed = mergedEntries(current, op.Props)
			} else {
				full, err := resolveProps(op.Props)
				if err != nil {
					return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
				}
				entries, changed = full, !sameEntries(full, current)
			}
			if err := v.claimUniqueNode(n.ID, entries); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if changed {
				if len(current) > 0 {
					actions = append(actions, txAction{kind: txActionPurgeNodeIndex, nodeID: n.ID})
				}
				if len(entries) > 0 {
					actions = append(actions, txAction{kind: txActionIndexNode, nodeID: n.ID, props: entries})
				}
			}

		case store.TxOpUpdateEdgeIndexed:
			e := op.Edge
			if e == nil {
				return nil, fmt.Errorf("transaction op %d (%s): nil edge", i, op.Kind)
			}
			if len(e.Labels) == 0 {
				return nil, fmt.Errorf("transaction op %d (%s): edge %d: %w", i, op.Kind, e.ID, store.ErrNoLabels)
			}
			cur, live := v.currentEdge(e.ID)
			if !live {
				return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(e.ID)}
			}
			if err := checkPropsKeys(op.Props); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if e.Src != cur.Src || e.Dst != cur.Dst {
				pinned := *e
				pinned.Src, pinned.Dst = cur.Src, cur.Dst
				e = &pinned
			}
			v.edges[e.ID] = e
			actions = append(actions, txAction{kind: txActionPutEdge, edge: e})

			currentE := v.s.propIdx.EdgeEntriesOf(e.ID)
			var entriesE []propEntry
			var changedE bool
			if op.Partial {
				entriesE, changedE = mergedEntries(currentE, op.Props)
			} else {
				full, err := resolveProps(op.Props)
				if err != nil {
					return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
				}
				entriesE, changedE = full, !sameEntries(full, currentE)
			}
			if err := v.claimUniqueEdge(e.ID, entriesE); err != nil {
				return nil, fmt.Errorf("transaction op %d (%s): %w", i, op.Kind, err)
			}
			if changedE {
				if len(currentE) > 0 {
					actions = append(actions, txAction{kind: txActionPurgeEdgeIndex, edgeID: e.ID})
				}
				if len(entriesE) > 0 {
					actions = append(actions, txAction{kind: txActionIndexEdge, edgeID: e.ID, props: entriesE})
				}
			}

		default:
			return nil, fmt.Errorf("transaction op %d: unknown kind %d", i, op.Kind)
		}
	}
	return actions, nil
}

// withKeyEntry returns props with the identity entry added, so a caller does not
// have to repeat the key it just passed as the key.
//
// The caller's map is never modified: an upsert that mutated its argument would
// break a loop that reuses one map across entities, which is the shape a bulk
// ingest naturally has.
func withKeyEntry(key string, value []byte, props map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(props)+1)
	for k, v := range props {
		out[k] = v
	}
	out[key] = value
	return out
}

// sameEntries reports whether two sorted entry lists describe the same index
// state, so an update that changes nothing frames no records at all.
func sameEntries(a []propEntry, b []index.PropEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].key != b[i].Key || !bytes.Equal(a[i].value, b[i].Value) {
			return false
		}
	}
	return true
}

// mergedEntries computes the entity's index entries after an upsert or a partial
// re-index: every key named in props gets exactly the supplied value, every key
// not named keeps what it had.
//
// changed reports whether that differs from current, which is what lets the
// common case — a re-ingest that changes nothing — skip the purge-and-rewrite
// entirely and frame no index records at all.
//
// The rewrite is whole because the log has no per-key purge: 0x07 drops an
// entity's entries and 0x03 adds one, so replacing a single key means stating
// the whole set. That is a deliberate trade — a per-key purge would be a new
// record type, and a new record type is a log an older build cannot read.
func mergedEntries(current []index.PropEntry, props map[string][]byte) (entries []propEntry, changed bool) {
	replaced := make(map[string]bool, len(props))
	for k := range props {
		replaced[k] = true
	}

	keep := make(map[string][]byte)
	for _, e := range current {
		if replaced[e.Key] {
			continue
		}
		keep[e.Key] = e.Value
	}
	for k, v := range props {
		keep[k] = v
	}

	entries, _ = resolveProps(keep)

	// Compare against current, which is already sorted by key then value. A
	// length difference is enough on its own; otherwise the sets must agree
	// pairwise.
	return entries, !sameEntries(entries, current)
}

// checkPropsKeys rejects any key the record encoding cannot round-trip, before
// anything reads the current entries.
func checkPropsKeys(props map[string][]byte) error {
	for k := range props {
		if err := checkPropKey(k); err != nil {
			return err
		}
	}
	return nil
}

// resolveProps flattens an index-entry map into ascending key order, rejecting
// any key the record encoding cannot round-trip.
func resolveProps(props map[string][]byte) ([]propEntry, error) {
	if len(props) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		if err := checkPropKey(k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	entries := make([]propEntry, len(keys))
	for i, k := range keys {
		entries[i] = propEntry{key: k, value: props[k]}
	}
	return entries, nil
}

// ApplyTransaction implements store.Transactor.
func (s *Store) ApplyTransaction(ops []store.TxOp) error {
	return s.applyTransaction(ops, nil, store.TxContext{})
}

// ApplyTransactionAs implements store.ActorTransactor.
func (s *Store) ApplyTransactionAs(ops []store.TxOp, ctx store.TxContext) error {
	return s.applyTransaction(ops, nil, ctx)
}

// ApplyTransactionChecked implements store.CheckedTransactor.
func (s *Store) ApplyTransactionChecked(ops []store.TxOp, checks []store.ReadCheck, ctx store.TxContext) error {
	return s.applyTransaction(ops, checks, ctx)
}

// applyTransaction validates the read set, resolves the operations and applies
// them, all under one exclusive hold.
//
// The read set is validated first and against the store as it stands *before*
// any operation resolves, because that is the state the reads were taken
// against. Failing there costs nothing: nothing has been touched, and the same
// argument that makes a resolution failure safe makes this one safe.
func (s *Store) applyTransaction(ops []store.TxOp, checks []store.ReadCheck, ctx store.TxContext) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	// No operations means nothing to protect, so the read set has nothing to
	// protect it from: a transaction that changes nothing cannot lose a write.
	if len(ops) == 0 {
		return nil
	}

	// Released before the durability wait at the end, so concurrent transactions
	// share an fsync instead of queueing behind each other's. See syncgate.go.
	s.mu.Lock()
	unlock := sync.OnceFunc(s.mu.Unlock)
	defer unlock()

	if err := s.validateReadsLocked(checks); err != nil {
		return err
	}

	actions, err := s.resolveTransaction(ops)
	if err != nil {
		return err
	}
	if len(actions) == 0 {
		return nil
	}

	batch := newWALBatchFramed(len(actions)*64, s.wal.Framing())
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
		case txActionIndexNode:
			id := uint64(a.nodeID)
			for _, e := range a.props {
				batch.addWith(walRecordNodeProp, func(dst []byte) []byte {
					return appendMarshalledProp(dst, id, e.key, e.value)
				})
			}
		case txActionIndexEdge:
			id := uint64(a.edgeID)
			for _, e := range a.props {
				batch.addWith(walRecordEdgeProp, func(dst []byte) []byte {
					return appendMarshalledProp(dst, id, e.key, e.value)
				})
			}
		}
	}

	// One write. If it fails nothing is applied: the commit marker never reached
	// the file, so replay discards whatever partial bytes did. That absence is
	// the rollback — there is no undo path that could itself go wrong.
	framed, err := batch.finish(s.nextCommitMeta(ctx))
	if err != nil {
		return fmt.Errorf("ApplyTransaction: %w", err)
	}
	var started time.Time
	if s.metricsOn() {
		started = time.Now()
	}
	ticket, err := s.wal.QueueBatch(framed)
	if err != nil {
		return fmt.Errorf("ApplyTransaction: wal: %w", err)
	}

	// One epoch for the whole transaction. That is what makes it atomic to a
	// reader as well as to the log: every record in it becomes visible together
	// when the epoch is published, and until then none of it is.
	epoch := s.nextEpoch()
	// Applied but not yet visible: the lock is released below and the epoch is
	// published only after the durability wait, so until then a concurrent
	// reader is running an epoch behind this one. See retainLocked.
	s.unpublished.Add(1)
	defer s.unpublished.Add(-1)
	for _, a := range actions {
		switch a.kind {
		case txActionPutNode:
			s.applyNodeUpsert(epoch, a.node)
		case txActionPutEdge:
			s.applyEdgeUpsert(epoch, a.edge)
		case txActionDelNode:
			s.applyNodeDelete(epoch, a.nodeID)
		case txActionDelEdge:
			s.applyEdgeDelete(epoch, a.edgeID)
		case txActionPurgeNodeIndex:
			s.propIdx.RemoveNode(a.nodeID)
		case txActionPurgeEdgeIndex:
			s.propIdx.RemoveEdge(a.edgeID)
		case txActionIndexNode:
			for _, e := range a.props {
				s.propIdx.IndexNode(a.nodeID, e.key, e.value)
			}
		case txActionIndexEdge:
			for _, e := range a.props {
				s.propIdx.IndexEdge(a.edgeID, e.key, e.value)
			}
		}
	}
	sync := s.syncOnCommit
	unlock()

	// One branch and one error, so the commit is measured once however it
	// ends. A failed commit is still a commit that happened, and an error rate
	// is a metric a sink hearing only about successes cannot compute.
	var cerr error
	if sync {
		// A failed sync leaves the records deliberately unpublished: they are
		// staged in the delta but no reader can reach them, because visibility
		// is the epoch. A transaction the disk did not take is not a
		// transaction.
		cerr = s.wal.AwaitSync(ticket)
	} else {
		// Not waiting for durability does not mean leaving the bytes in a
		// queue. syncOnCommit off is documented as "durable at the next Sync,
		// Compact or Close", and bytes still in the ring would not survive even
		// a process kill, which the page cache does.
		cerr = s.wal.FlushQueued()
	}
	if s.metricsOn() {
		s.record(store.Metric{
			Kind:     store.MetricCommit,
			Duration: time.Since(started),
			Count:    int64(len(actions)),
			Bytes:    int64(len(framed)),
			Err:      cerr,
		})
	}
	if cerr != nil {
		return fmt.Errorf("ApplyTransaction: wal: %w", cerr)
	}
	s.publishEpoch(epoch)
	s.reportDeltaBudget()
	return nil
}
