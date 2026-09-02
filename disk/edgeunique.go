package disk

// The disk half of the edge cardinality constraint.
//
// Structurally the same as memory/edgeunique.go and deliberately so — the memory
// backend is the parity oracle, and "which writes fail" has to match. The
// difference is only where the answer comes from: two layers rather than one
// map, resolved at the writer's epoch, because an edge the CSR still lists may
// have been tombstoned and an edge the delta lists may not exist yet at the
// epoch doing the asking.
//
// Nothing new is stored, so nothing has to be maintained in applyEdgeDelete and
// nothing has to be rebuilt by Compact — the same argument index/unique_index.go
// makes for property keys, and the reason a declaration here cannot go stale.

import (
	"fmt"
	"slices"

	"github.com/aoiflux/graphene/store"
)

// DeclareUniqueEdgeType implements store.EdgeCardinalityDeclarer.
//
// The store lock is held across the whole validation, so nothing can create a
// second edge between a pair between the check finding one and the declaration
// taking effect. Refused on a read-only store for the reason
// DeclareUniqueNodeProperty is: a store that cannot be written to can hold the
// constraint and never enforce it, which is the guarantee without the behaviour.
func (s *Store) DeclareUniqueEdgeType(t store.EdgeType) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.uniqueEdgeTypes.Declared(t) {
		return nil
	}
	if conflicts := s.edgeCardinalityConflictsLocked(t); len(conflicts) > 0 {
		return &store.EdgeCardinalityViolationsError{Type: t, Conflicts: conflicts}
	}
	s.uniqueEdgeTypes.Declare(t)
	return nil
}

// UniqueEdgeTypes implements store.EdgeCardinalityDeclarer.
func (s *Store) UniqueEdgeTypes() []store.EdgeType {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.uniqueEdgeTypes.Types()
}

// EdgeBetween implements store.EdgeCardinalityDeclarer.
//
// A missing source node is an empty adjacency rather than an error, matching
// EdgesOf and DegreeOf on this backend.
func (s *Store) EdgeBetween(src, dst store.NodeID, edgeTypes []store.EdgeType) (store.EdgeID, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, _, found := s.readerLocked().outboundEdgeTo(src, dst, edgeTypes, store.InvalidEdgeID)
	return id, found, nil
}

// checkEdgeCardinalityLocked refuses a candidate edge that would be a second
// edge of a declared type between its endpoints.
//
// skip names an edge to ignore, which is what makes this usable from UpdateEdge:
// an edge adding a declared label to itself must not collide with itself.
// Caller must hold s.mu exclusively.
func (s *Store) checkEdgeCardinalityLocked(src, dst store.NodeID, labels []store.EdgeType, skip store.EdgeID) error {
	constrained := s.uniqueEdgeTypes.Constrained(labels)
	if len(constrained) == 0 {
		return nil
	}
	id, held, matched := s.writerLocked().outboundEdgeTo(src, dst, constrained, skip)
	if !matched {
		return nil
	}
	return &store.ErrEdgeTaken{
		Pair:  store.EdgePair{Src: src, Dst: dst},
		Type:  firstShared(constrained, held),
		Owner: id,
	}
}

// firstShared names the constrained type the incumbent actually carries, so the
// error says which declaration was violated rather than which one was checked.
func firstShared(constrained, held []store.EdgeType) store.EdgeType {
	for _, t := range constrained {
		if slices.Contains(held, t) {
			return t
		}
	}
	return store.EdgeTypeUnknown
}

// edgeCardinalityConflictsLocked collects every pair joined more than once by a
// live edge of type t. Caller must hold s.mu exclusively.
func (s *Store) edgeCardinalityConflictsLocked(t store.EdgeType) []store.EdgeCardinalityConflict {
	r := s.writerLocked()
	byPair := make(map[store.EdgePair][]store.EdgeID)
	// edgesByType has already resolved its candidates against the epoch, so
	// every ID here names an edge this writer can see carrying t.
	for _, eid := range r.edgesByType(t) {
		e, ok := r.edge(eid)
		if !ok || e == nil {
			continue
		}
		pair := store.EdgePair{Src: e.Src, Dst: e.Dst}
		byPair[pair] = append(byPair[pair], eid)
	}
	return store.CollectEdgeCardinalityConflicts(byPair)
}

// outboundEdgeTo returns the first live edge from src to dst matching edgeTypes,
// along with the labels it carries.
//
// It walks the same sequence incidentEdges does, restricted to the outbound
// direction and stopping at the first match. Written out rather than layered on
// incidentEdges because this runs on the write path of every edge once a
// constraint is declared: incidentEdges materialises the whole adjacency into a
// slice, and this needs one entry and an early exit.
//
// nil edgeTypes matches any type. skip is passed over, which is what lets an
// update re-check an edge against its own pair.
func (r reader) outboundEdgeTo(src, dst store.NodeID, edgeTypes []store.EdgeType, skip store.EdgeID) (store.EdgeID, []store.EdgeType, bool) {
	dOut, _ := r.deltaEdgeIDs(src, store.DirectionOutbound)
	for _, eid := range dOut {
		if eid == skip {
			continue
		}
		e, known := r.deltaEdge(eid)
		if !known || e == nil || e.Dst != dst {
			continue
		}
		if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, e) {
			continue
		}
		return eid, e.Labels, true
	}

	if r.v.csr == nil {
		return store.InvalidEdgeID, nil, false
	}
	cOut, _ := r.csrEdgeIDs(src, store.DirectionOutbound)
	for _, eid := range cOut {
		if eid == skip {
			continue
		}
		// A CSR edge the delta has an opinion about is answered from the delta:
		// deleted (skip it), or updated to labels the image does not know about.
		if de, known := r.deltaEdge(eid); known {
			if de == nil || de.Dst != dst {
				continue
			}
			if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, de) {
				continue
			}
			return eid, de.Labels, true
		}
		rec, found := r.v.csr.GetEdge(eid)
		if !found || rec.Dst != dst {
			continue
		}
		if edgeTypes != nil && !rawEdgeMatchesFilter(edgeTypes, rec.Labels) {
			continue
		}
		return eid, rec.Labels, true
	}
	return store.InvalidEdgeID, nil, false
}

// --- transaction-local claims ---
//
// See memory/edgeunique.go: a transaction has to answer the question against
// itself as well as against the store, in both directions, and the
// delete-the-subtree-then-rebuild shape depends on the freeing half.

// pairSlot is one (pair, type) reservation.
type pairSlot struct {
	pair store.EdgePair
	typ  store.EdgeType
}

// claimEdgePairs reserves the constrained slots e occupies, or reports the
// incumbent.
func (v *txView) claimEdgePairs(e *store.Edge) error {
	constrained := v.s.uniqueEdgeTypes.Constrained(e.Labels)
	if len(constrained) == 0 {
		return nil
	}
	pair := store.EdgePair{Src: e.Src, Dst: e.Dst}
	for _, t := range constrained {
		slot := pairSlot{pair: pair, typ: t}
		if owner, taken := v.pairs[slot]; taken {
			if owner != e.ID {
				return &store.ErrEdgeTaken{Pair: pair, Type: t, Owner: owner}
			}
			continue
		}
		if owner, found := v.storedEdgeBetween(pair, t); found && owner != e.ID {
			return &store.ErrEdgeTaken{Pair: pair, Type: t, Owner: owner}
		}
		v.pairs[slot] = e.ID
		v.pairsOf[e.ID] = append(v.pairsOf[e.ID], slot)
	}
	return nil
}

// storedEdgeBetween finds a stored edge of type t joining pair, ignoring any the
// transaction has deleted or holds its own copy of.
func (v *txView) storedEdgeBetween(pair store.EdgePair, t store.EdgeType) (store.EdgeID, bool) {
	want := []store.EdgeType{t}
	r := v.s.writerLocked()
	// The scan resumes past an edge the transaction speaks for rather than
	// stopping at it: those are answered by the claims map, and the pair may
	// still be held by a second stored edge further along the adjacency.
	skip := store.InvalidEdgeID
	for {
		id, _, found := r.outboundEdgeTo(pair.Src, pair.Dst, want, skip)
		if !found {
			return store.InvalidEdgeID, false
		}
		_, deleted := v.delEdge[id]
		_, pending := v.edges[id]
		if !deleted && !pending {
			return id, true
		}
		if id == skip {
			// Defensive: outboundEdgeTo must not return the ID it was told to
			// pass over, and looping forever if it did would be worse.
			return store.InvalidEdgeID, false
		}
		skip = id
	}
}

// releaseEdgePairs drops the slots eid holds, so an edge deleted or relabelled
// inside the transaction frees the pair it was occupying.
//
// The reverse index is what keeps this O(slots held) rather than a walk of every
// claim: a transaction that deletes a subtree and rebuilds it does this once per
// edge removed, and the walk would make that quadratic.
func (v *txView) releaseEdgePairs(eid store.EdgeID) {
	held, ok := v.pairsOf[eid]
	if !ok {
		return
	}
	for _, slot := range held {
		if v.pairs[slot] == eid {
			delete(v.pairs, slot)
		}
	}
	delete(v.pairsOf, eid)
}

// batchPairClaims returns a claim set for a batch, or nil when no declaration
// could apply to it. See memory/edgeunique.go for why a batch checks itself.
func (s *Store) batchPairClaims(edges []*store.Edge) map[pairSlot]int {
	if len(s.uniqueEdgeTypes) == 0 {
		return nil
	}
	return make(map[pairSlot]int, len(edges))
}

// claimBatchPair reserves the constrained slots of the edge at index i, or
// reports the earlier edge in the batch holding one.
func claimBatchPair(c map[pairSlot]int, declared store.UniqueEdgeTypeSet, e *store.Edge, i int) error {
	if c == nil {
		return nil
	}
	for _, t := range declared.Constrained(e.Labels) {
		slot := pairSlot{pair: store.EdgePair{Src: e.Src, Dst: e.Dst}, typ: t}
		if prior, taken := c[slot]; taken {
			return fmt.Errorf("%w: a second edge of type %s from %d to %d, first at batch index %d",
				store.ErrUniqueViolation, t, e.Src, e.Dst, prior)
		}
		c[slot] = i
	}
	return nil
}
