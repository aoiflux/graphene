package memory

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/aoiflux/graphene/index"
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
	txActionIndexNode
	txActionIndexEdge
)

// propEntry is one property-index registration, resolved.
type propEntry struct {
	key   string
	value []byte
}

type txAction struct {
	kind   txActionKind
	node   *store.Node
	edge   *store.Edge
	nodeID store.NodeID
	edgeID store.EdgeID

	// props is set for the index actions, in ascending key order. This backend
	// has no log to keep deterministic, but it is the oracle disk is compared
	// against, so it applies entries in the same order disk frames them.
	props []propEntry
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

	// claims are the unique values this transaction has taken but not yet
	// applied; see the disk backend for why the index cannot answer for them.
	claims map[uniqueClaim]uint64

	// pairs are the edge-cardinality slots taken but not yet applied, and
	// pairsOf is the reverse index that makes releasing them cheap. See
	// memory/edgeunique.go.
	pairs   map[pairSlot]store.EdgeID
	pairsOf map[store.EdgeID][]pairSlot
}

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
// reports the incumbent. Resolution has written nothing, so a rejection leaves
// the store as it found it.
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

// currentEdge resolves id to the edge this transaction would read: the version
// it has written itself, else the store's.
func (v *txView) currentEdge(id store.EdgeID) (*store.Edge, bool) {
	if _, gone := v.delEdge[id]; gone {
		return nil, false
	}
	if e, here := v.edges[id]; here {
		return e, true
	}
	e, ok := v.s.edges[id]
	return e, ok
}

// uniqueNodeOwner resolves key=value to the live node holding it, as this
// transaction sees things. Claims made earlier in the same transaction come
// first; see the disk backend for why.
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
				// Endpoints are immutable, as in Store.UpdateEdge. Relocating
				// adjacency for a changed Src or Dst — which this backend used
				// to do — accepted an operation the disk backend silently
				// ignored, so the two produced different graphs from one
				// transaction.
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
			entries := resolveProps(op.Props)
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
			entries := resolveProps(op.Props)
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
			owner, found := v.uniqueNodeOwner(op.Key, op.Value)
			if found != (store.NodeID(op.Expect) != store.InvalidNodeID) || (found && owner != store.NodeID(op.Expect)) {
				return nil, fmt.Errorf("transaction op %d (%s): key %q value %q: %w",
					i, op.Kind, op.Key, op.Value, store.ErrWriteConflict)
			}
			v.nodes[n.ID] = n
			delete(v.delNode, n.ID)
			actions = append(actions, txAction{kind: txActionPutNode, node: n})

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
				full := resolveProps(op.Props)
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
				full := resolveProps(op.Props)
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

// mergedEntries computes the entity's index entries after an upsert: every key
// named in props gets exactly the supplied value, every key not named keeps what
// it had. See the disk backend for why replacing is whole rather than per-key.
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

	entries = resolveProps(keep)
	return entries, !sameEntries(entries, current)
}

// withKeyEntry returns props with the identity entry added, without modifying
// the caller's map.
func withKeyEntry(key string, value []byte, props map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(props)+1)
	for k, v := range props {
		out[k] = v
	}
	out[key] = value
	return out
}

// resolveProps flattens an index-entry map into ascending key order.
//
// There is no key-length bound here, unlike disk: that bound exists because a
// property record stores the key length in a uint16, and this backend writes no
// records. Rejecting a key disk would accept is not parity either way round, but
// the only keys disk rejects are ones it would silently corrupt, and a caller
// that never touches disk has nothing to be protected from.
func resolveProps(props map[string][]byte) []propEntry {
	if len(props) == 0 {
		return nil
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	entries := make([]propEntry, len(keys))
	for i, k := range keys {
		entries[i] = propEntry{key: k, value: props[k]}
	}
	return entries
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
	return s.applyTransaction(ops, nil)
}

// ApplyTransactionChecked implements store.CheckedTransactor.
//
// The TxContext is accepted and dropped, exactly as the note above explains for
// ActorTransactor: there is nowhere here to record an actor. This backend does
// implement CheckedTransactor, because unlike attribution the guarantee is one
// it can actually keep — validation needs a write lock and a way to re-read,
// both of which it has.
func (s *Store) ApplyTransactionChecked(ops []store.TxOp, checks []store.ReadCheck, _ store.TxContext) error {
	return s.applyTransaction(ops, checks)
}

func (s *Store) applyTransaction(ops []store.TxOp, checks []store.ReadCheck) error {
	// No operations means nothing to protect, so the read set has nothing to
	// protect it from: a transaction that changes nothing cannot lose a write.
	if len(ops) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Before resolution, and so against the state the reads were taken against.
	// Nothing has been touched yet, which is what makes failing here free.
	if err := s.validateReadsLocked(checks); err != nil {
		return err
	}

	actions, err := s.resolveTransaction(ops)
	if err != nil {
		return err
	}

	// Bumped here rather than at the top of the function, because a transaction
	// that fails to resolve has changed nothing. Every other mutator bumps this
	// under the write lock and this one did not, which made the snapshot epoch —
	// which is this counter — repeat across a committed transaction, so two
	// snapshots straddling one reported the same Epoch over different graphs.
	s.version.Add(1)

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
//
// Endpoints need no handling here because resolution pins them: a
// TxOpUpdateEdge carries the current record's Src and Dst whatever the caller
// supplied, so a replacement is always incident to the same pair the adjacency
// lists already name.
func (s *Store) upsertEdgeLocked(e *store.Edge) {
	prev, existed := s.edges[e.ID]
	if existed {
		s.unindexEdgeLabels(e.ID, prev.Labels)
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
