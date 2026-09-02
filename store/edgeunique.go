package store

// At most one edge of a type between two nodes.
//
// # Why a property key was not enough
//
// UpsertEdge resolves an edge by a declared-unique *property*, which works and
// is the right answer for an edge that carries identity of its own. It is the
// wrong answer for bulk structure. A caller ingesting a corpus writes edges by
// the million whose whole meaning is "this owns that" — no identity, no
// properties worth indexing — and giving each one a unique key to prevent
// duplicates means paying an index entry per edge to store a fact the endpoints
// already contain.
//
// The alternative such a caller is left with is a convention: own a subtree,
// delete it before re-ingesting, and never add an edge twice by hand. That
// works exactly as long as nobody forgets, which is the definition of a footgun
// rather than a guarantee. EdgeCardinalityDeclarer moves it into the engine.
//
// # Per label, not per label set
//
// An edge carries one or more EdgeTypes and every filter in the engine treats a
// type list as OR over labels. A declaration therefore names one type and means
// "at most one live edge from src to dst carries this type". An edge labelled
// {Owns, Contains} counts against a declaration on Owns and against one on
// Contains independently, which composes; a rule about whole label *sets* would
// not, and a rule about a "primary" label would invent a concept the store does
// not have.
//
// # Nothing new is stored
//
// The same argument index/unique_index.go makes for property keys: the answer is
// already in the out-adjacency, and a second structure holding it is a second
// thing that can disagree with the first. The check is a scan of the source
// node's outbound edges, which is O(out-degree of src) — cheap for the ownership
// edges this exists for, and worth knowing about before declaring a type whose
// sources are hubs.

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// EdgePair is the ordered endpoint pair a cardinality constraint is declared
// over. Direction is part of the identity: src→dst and dst→src are two pairs.
type EdgePair struct {
	Src NodeID
	Dst NodeID
}

// EdgeCardinalityConflict names one endpoint pair joined by more than one live
// edge of a declared type.
type EdgeCardinalityConflict struct {
	Pair EdgePair
	IDs  []EdgeID
}

// EdgeCardinalityViolationsError reports every pair blocking a declaration.
//
// Every one, not the first, for the reason UniqueViolationsError gives: a caller
// declaring a constraint over an existing graph is about to repair it, and one
// duplicate per pass turns a script into an afternoon.
type EdgeCardinalityViolationsError struct {
	Type      EdgeType
	Conflicts []EdgeCardinalityConflict
}

func (e *EdgeCardinalityViolationsError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "graphene: cannot declare edge type %s unique: %d node pair(s) joined more than once",
		e.Type, len(e.Conflicts))
	// A bounded sample, because a broken import can produce thousands and an
	// error string is read by a person. The full set is on Conflicts.
	const sample = 5
	for i, c := range e.Conflicts {
		if i == sample {
			fmt.Fprintf(&b, "; and %d more", len(e.Conflicts)-sample)
			break
		}
		fmt.Fprintf(&b, "; %d->%d by %v", c.Pair.Src, c.Pair.Dst, c.IDs)
	}
	return b.String()
}

// Unwrap gives a declaration refused over existing data the same condition to
// test as one refused at write time, and the same one a unique property key
// uses. A caller handling "this violates a uniqueness rule" handles all three
// the same way.
func (e *EdgeCardinalityViolationsError) Unwrap() error { return ErrUniqueViolation }

// Sorted reports whether the conflicts are in ascending pair order, which they
// always are as returned. Present so a test can say so without reaching for the
// comparison itself.
func (e *EdgeCardinalityViolationsError) Sorted() bool {
	for i := 1; i < len(e.Conflicts); i++ {
		if comparePairs(e.Conflicts[i-1].Pair, e.Conflicts[i].Pair) > 0 {
			return false
		}
	}
	return true
}

// ErrEdgeTaken reports an edge refused because one of the declared type already
// joins the same pair.
//
// It carries the incumbent so a caller can say which one, and so a caller that
// wanted the edge to exist can use the one that does rather than looking it up
// again — the same reason ErrUniqueTaken carries its owner.
type ErrEdgeTaken struct {
	Pair  EdgePair
	Type  EdgeType
	Owner EdgeID
}

func (e *ErrEdgeTaken) Unwrap() error { return ErrUniqueViolation }

func (e *ErrEdgeTaken) Error() string {
	return fmt.Sprintf("graphene: an edge of type %s from %d to %d already exists as %d",
		e.Type, e.Pair.Src, e.Pair.Dst, e.Owner)
}

// CollectEdgeCardinalityConflicts turns a pair-to-edges grouping into the sorted
// report a declaration returns, dropping pairs joined only once.
//
// Shared rather than written twice because the two backends reach the grouping
// by completely different routes — a map walk on one, a CSR scan merged with a
// delta layer on the other — and the part that must not differ between them is
// what counts as a conflict and what order they arrive in. Map iteration would
// otherwise hand a repair tool, a test and an error message three different
// orders for one graph.
func CollectEdgeCardinalityConflicts(byPair map[EdgePair][]EdgeID) []EdgeCardinalityConflict {
	var conflicts []EdgeCardinalityConflict
	for pair, ids := range byPair {
		if len(ids) < 2 {
			continue
		}
		held := slices.Clone(ids)
		slices.Sort(held)
		conflicts = append(conflicts, EdgeCardinalityConflict{Pair: pair, IDs: held})
	}
	slices.SortFunc(conflicts, func(a, b EdgeCardinalityConflict) int {
		return comparePairs(a.Pair, b.Pair)
	})
	return conflicts
}

func comparePairs(a, b EdgePair) int {
	if c := cmp.Compare(a.Src, b.Src); c != 0 {
		return c
	}
	return cmp.Compare(a.Dst, b.Dst)
}

// UniqueEdgeTypeSet is the declaration set a backend holds.
//
// A plain set behind the store's own lock rather than anything cleverer: the
// declarations are read on every edge write and changed almost never, and both
// backends already hold a store-wide lock across the write they would be read
// in. Its methods assume the caller holds that lock.
type UniqueEdgeTypeSet map[EdgeType]struct{}

// Declared reports whether t is under a cardinality constraint.
func (s UniqueEdgeTypeSet) Declared(t EdgeType) bool {
	_, ok := s[t]
	return ok
}

// Declare adds t to the set.
func (s UniqueEdgeTypeSet) Declare(t EdgeType) { s[t] = struct{}{} }

// Constrained returns the labels of a candidate edge that are under a
// constraint, or nil when none are.
//
// nil is the answer for almost every write — a store with no declarations at all
// returns it on a length check — which is what keeps the enforcement scan off
// the path of callers who never asked for it.
func (s UniqueEdgeTypeSet) Constrained(labels []EdgeType) []EdgeType {
	if len(s) == 0 {
		return nil
	}
	var out []EdgeType
	for i, t := range labels {
		if !s.Declared(t) {
			continue
		}
		// A repeated label is one constraint, not two.
		if slices.Contains(labels[:i], t) {
			continue
		}
		out = append(out, t)
	}
	return out
}

// Types returns the declared types, sorted.
func (s UniqueEdgeTypeSet) Types() []EdgeType {
	out := make([]EdgeType, 0, len(s))
	for t := range s {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}
