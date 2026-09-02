package memory

// The in-memory half of the edge cardinality constraint.
//
// Answered from the out-adjacency rather than from a side index, for the reason
// store/edgeunique.go gives: the adjacency already holds the answer, and a
// second structure holding it is a second thing that can disagree with the
// first. That also means there is nothing to maintain in deleteEdgeLocked and
// nothing to rebuild — a constraint that cannot go stale.

import (
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// DeclareUniqueEdgeType implements store.EdgeCardinalityDeclarer.
//
// The store lock is held across the whole validation, so nothing can create a
// second edge between a pair between the check finding one and the declaration
// taking effect.
func (s *Store) DeclareUniqueEdgeType(t store.EdgeType) error {
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
func (s *Store) EdgeBetween(src, dst store.NodeID, edgeTypes []store.EdgeType) (store.EdgeID, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.nodes[src]; !ok {
		return store.InvalidEdgeID, false, &store.ErrNotFound{Kind: "node", ID: uint64(src)}
	}
	id, found := s.edgeBetweenLocked(src, dst, edgeTypes)
	return id, found, nil
}

// edgeBetweenLocked scans src's outbound adjacency for an edge to dst matching
// edgeTypes. Caller must hold s.mu.
func (s *Store) edgeBetweenLocked(src, dst store.NodeID, edgeTypes []store.EdgeType) (store.EdgeID, bool) {
	a := s.adj[src]
	if a == nil {
		return store.InvalidEdgeID, false
	}
	for _, eid := range a.out {
		e := s.edges[eid]
		if e == nil || e.Dst != dst {
			continue
		}
		if edgeTypes != nil && !edgeMatchesFilter(edgeTypes, e) {
			continue
		}
		return eid, true
	}
	return store.InvalidEdgeID, false
}

// checkEdgeCardinalityLocked refuses a candidate edge that would be a second
// edge of a declared type between its endpoints.
//
// skip names an edge to ignore, which is what makes this usable from UpdateEdge:
// an edge adding a declared label to itself must not collide with itself.
// Caller must hold s.mu.
func (s *Store) checkEdgeCardinalityLocked(src, dst store.NodeID, labels []store.EdgeType, skip store.EdgeID) error {
	constrained := s.uniqueEdgeTypes.Constrained(labels)
	if len(constrained) == 0 {
		return nil
	}
	a := s.adj[src]
	if a == nil {
		return nil
	}
	// One pass over the adjacency testing every constrained label, rather than
	// one pass per label: out-degree is the expensive part and the label set is
	// almost always a single type.
	for _, eid := range a.out {
		if eid == skip {
			continue
		}
		e := s.edges[eid]
		if e == nil || e.Dst != dst {
			continue
		}
		for _, t := range constrained {
			if e.HasLabel(t) {
				return &store.ErrEdgeTaken{
					Pair:  store.EdgePair{Src: src, Dst: dst},
					Type:  t,
					Owner: eid,
				}
			}
		}
	}
	return nil
}

// edgeCardinalityConflictsLocked collects every pair joined more than once by a
// live edge of type t. Caller must hold s.mu.
func (s *Store) edgeCardinalityConflictsLocked(t store.EdgeType) []store.EdgeCardinalityConflict {
	byPair := make(map[store.EdgePair][]store.EdgeID)
	for _, eid := range s.edgesByType[t] {
		e := s.edges[eid]
		if e == nil || !e.HasLabel(t) {
			continue
		}
		pair := store.EdgePair{Src: e.Src, Dst: e.Dst}
		byPair[pair] = append(byPair[pair], eid)
	}
	return store.CollectEdgeCardinalityConflicts(byPair)
}

// pairClaims tracks the (pair, type) slots taken by edges earlier in a batch.
//
// A batch is applied atomically, so its own edges are as real as the ones
// already stored by the time anything can read them. Checking only the
// adjacency would let a single AddEdgesBatch write the duplicate the constraint
// exists to prevent — the one case where "nothing new is stored" is not enough,
// because the earlier edges of the batch are not stored yet either.
type pairClaims map[pairSlot]int

type pairSlot struct {
	pair store.EdgePair
	typ  store.EdgeType
}

// batchPairClaims returns a claim set sized for the batch, or nil when no
// declaration could apply to it.
func (s *Store) batchPairClaims(edges []*store.Edge) pairClaims {
	if len(s.uniqueEdgeTypes) == 0 {
		return nil
	}
	return make(pairClaims, len(edges))
}

// claim reserves the constrained slots of the edge at index i, or reports the
// earlier edge in the batch holding one.
//
// The incumbent is named by its position in the batch rather than by an ID,
// because nothing in the batch has one yet.
func (c pairClaims) claim(declared store.UniqueEdgeTypeSet, e *store.Edge, i int) error {
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

// --- transaction-local claims ---
//
// A transaction has to answer the cardinality question against itself as well as
// against the store, and in both directions. An edge added earlier in the
// transaction occupies a slot even though nothing is written yet; an edge
// deleted earlier frees one even though the adjacency still lists it. The second
// is not a corner case — delete the subtree you own and rebuild it is the whole
// point of an idempotent re-ingest, and a check that only consulted the stored
// adjacency would refuse every edge the transaction had just removed.

// claimEdgePairs reserves the constrained (pair, type) slots e occupies, or
// reports the incumbent.
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
// transaction has deleted or holds its own copy of — those are answered by the
// claims above, which are the transaction's view of the same edge.
func (v *txView) storedEdgeBetween(pair store.EdgePair, t store.EdgeType) (store.EdgeID, bool) {
	a := v.s.adj[pair.Src]
	if a == nil {
		return store.InvalidEdgeID, false
	}
	for _, eid := range a.out {
		if _, deleted := v.delEdge[eid]; deleted {
			continue
		}
		if _, pending := v.edges[eid]; pending {
			continue
		}
		e := v.s.edges[eid]
		if e == nil || e.Dst != pair.Dst || !e.HasLabel(t) {
			continue
		}
		return eid, true
	}
	return store.InvalidEdgeID, false
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
