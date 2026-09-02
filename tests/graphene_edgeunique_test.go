package graphene_test

// Edge cardinality: at most one edge of a declared type between two nodes.
//
// The constraint exists for the case a unique property key cannot serve. A
// caller ingesting a corpus writes ownership edges by the million — no identity,
// no properties worth indexing — and giving each one a unique key to prevent
// duplicates means paying an index entry per edge to restate what the endpoints
// already say. Without the declaration the only defence is a convention the
// caller has to keep, and a convention is not a guarantee.
//
// Both backends must refuse the same writes. memory.Store is the parity oracle,
// so every test here runs against both and any divergence is the bug.

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// twoNodes comes from graphene_labels_test.go — one Case node and one
// MicroArtefact, which is all these need.
//
// edgeOwns is a custom type, because that is what a caller declaring this
// actually has: the built-in types carry engine semantics, and the constraint is
// for structure the application named itself.
var edgeOwns = store.CustomEdgeType(0)

func owns(src, dst store.NodeID) *store.Edge {
	return &store.Edge{Src: src, Dst: dst, Labels: []store.EdgeType{edgeOwns}}
}

// TestUniqueEdge_SecondAddIsRefused is the whole point: the duplicate that plain
// AddEdge used to create silently.
func TestUniqueEdge_SecondAddIsRefused(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b := twoNodes(t, g)
		if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
			t.Fatalf("DeclareUniqueEdge on an empty graph: %v", err)
		}

		first, err := g.AddEdge(owns(a, b))
		if err != nil {
			t.Fatalf("first AddEdge: %v", err)
		}

		_, err = g.AddEdge(owns(a, b))
		if !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("second AddEdge: want ErrUniqueViolation, got %v", err)
		}

		// The incumbent is named, so a caller who wanted the edge to exist can
		// use the one that does rather than looking it up again.
		var taken *store.ErrEdgeTaken
		if !errors.As(err, &taken) {
			t.Fatalf("second AddEdge: want *store.ErrEdgeTaken, got %T", err)
		}
		if taken.Owner != first {
			t.Errorf("incumbent reported as %d, want %d", taken.Owner, first)
		}
		if taken.Type != edgeOwns {
			t.Errorf("violated type reported as %s, want %s", taken.Type, edgeOwns)
		}
		if taken.Pair != (store.EdgePair{Src: a, Dst: b}) {
			t.Errorf("pair reported as %+v, want {%d %d}", taken.Pair, a, b)
		}

		if n, err := g.EdgeCount(); err != nil || n != 1 {
			t.Fatalf("EdgeCount = %d (err %v), want 1", n, err)
		}
	})
}

// TestUniqueEdge_ConstrainsPerLabelAndPerDirection pins the two scoping
// decisions, both of which are easy to get wrong in a way no other test notices.
func TestUniqueEdge_ConstrainsPerLabelAndPerDirection(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b := twoNodes(t, g)
		if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
			t.Fatalf("DeclareUniqueEdge: %v", err)
		}
		if _, err := g.AddEdge(owns(a, b)); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}

		// Direction is part of the pair: the constraint is on src→dst, and the
		// reverse edge is a different fact about the graph.
		if _, err := g.AddEdge(owns(b, a)); err != nil {
			t.Fatalf("the reverse edge is a different pair and must be allowed: %v", err)
		}

		// An undeclared type between the same pair is untouched.
		if _, err := g.AddEdge(&store.Edge{
			Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains},
		}); err != nil {
			t.Fatalf("an undeclared type must not be constrained: %v", err)
		}

		// And a multi-label edge counts against the declaration it carries, not
		// against its label set as a whole.
		_, err := g.AddEdge(&store.Edge{
			Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains, edgeOwns},
		})
		if !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("a multi-label edge carrying a declared type: want ErrUniqueViolation, got %v", err)
		}
	})
}

// TestUniqueEdge_DeclarationReportsEveryConflict covers declaring over a graph
// that already violates the rule, which is the state a caller adopting this
// arrives in.
func TestUniqueEdge_DeclarationReportsEveryConflict(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		// Three pairs joined twice each, and one joined once, built in an order
		// that is not the order they must be reported in.
		var pairs []store.EdgePair
		for i := 0; i < 3; i++ {
			a, b := twoNodes(t, g)
			pairs = append(pairs, store.EdgePair{Src: a, Dst: b})
			for range 2 {
				if _, err := g.AddEdge(owns(a, b)); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			}
		}
		clean, cleanDst := twoNodes(t, g)
		if _, err := g.AddEdge(owns(clean, cleanDst)); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}

		err := g.DeclareUniqueEdge(edgeOwns)
		if !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("declaring over a violating graph: want ErrUniqueViolation, got %v", err)
		}
		var v *store.EdgeCardinalityViolationsError
		if !errors.As(err, &v) {
			t.Fatalf("want *store.EdgeCardinalityViolationsError, got %T", err)
		}
		if len(v.Conflicts) != len(pairs) {
			t.Fatalf("reported %d conflicts, want %d — every one, not the first",
				len(v.Conflicts), len(pairs))
		}
		if !v.Sorted() {
			t.Errorf("conflicts are not sorted: a repair tool, a test and an error message "+
				"must see one order, got %+v", v.Conflicts)
		}
		for _, c := range v.Conflicts {
			if len(c.IDs) != 2 {
				t.Errorf("pair %+v reported %d holders, want 2", c.Pair, len(c.IDs))
			}
		}

		// Nothing was declared, so the graph is exactly as it was.
		if got := g.UniqueEdges(); len(got) != 0 {
			t.Errorf("a refused declaration took effect anyway: %v", got)
		}
		if _, err := g.AddEdge(owns(clean, cleanDst)); err != nil {
			t.Fatalf("writes must be unaffected by a refused declaration: %v", err)
		}
	})
}

// TestUniqueEdge_DeclareIsIdempotent is what makes declaring at every Open the
// contract rather than a thing a caller has to guard.
func TestUniqueEdge_DeclareIsIdempotent(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b := twoNodes(t, g)
		for i := range 3 {
			if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
				t.Fatalf("declaration %d: %v", i, err)
			}
		}
		if _, err := g.AddEdge(owns(a, b)); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		// Re-declaring over a graph that satisfies the rule is still a no-op.
		if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
			t.Fatalf("re-declaration after a conforming write: %v", err)
		}
		if got := g.UniqueEdges(); len(got) != 1 || got[0] != edgeOwns {
			t.Errorf("UniqueEdges = %v, want [%s]", got, edgeOwns)
		}
	})
}

// TestUniqueEdge_EveryWriteFunnel is the coverage that matters most. There are
// four ways to create or relabel an edge, and a constraint enforced on three of
// them is not enforced.
func TestUniqueEdge_EveryWriteFunnel(t *testing.T) {
	funnels := map[string]func(t *testing.T, g *graphene.Graph, a, b store.NodeID) error{
		"AddEdge": func(t *testing.T, g *graphene.Graph, a, b store.NodeID) error {
			_, err := g.AddEdge(owns(a, b))
			return err
		},
		"AddEdges": func(t *testing.T, g *graphene.Graph, a, b store.NodeID) error {
			_, err := g.AddEdges([]*store.Edge{owns(a, b)})
			return err
		},
		"Tx.AddEdge": func(t *testing.T, g *graphene.Graph, a, b store.NodeID) error {
			tx := g.Begin()
			tx.AddEdge(owns(a, b))
			return tx.Commit()
		},
		"UpdateEdge adding the label": func(t *testing.T, g *graphene.Graph, a, b store.NodeID) error {
			// An edge of an unconstrained type, then relabelled into the
			// constraint. Nothing is created, so only the update path can catch
			// it.
			id, err := g.AddEdge(&store.Edge{
				Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains},
			})
			if err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
			return g.UpdateEdge(&store.Edge{
				ID: id, Src: a, Dst: b, Labels: []store.EdgeType{edgeOwns},
			})
		},
	}

	for name, write := range funnels {
		t.Run(name, func(t *testing.T) {
			backends(t, func(t *testing.T, g *graphene.Graph) {
				a, b := twoNodes(t, g)
				if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
					t.Fatalf("DeclareUniqueEdge: %v", err)
				}
				if _, err := g.AddEdge(owns(a, b)); err != nil {
					t.Fatalf("incumbent AddEdge: %v", err)
				}
				if err := write(t, g, a, b); !errors.Is(err, store.ErrUniqueViolation) {
					t.Fatalf("%s: want ErrUniqueViolation, got %v", name, err)
				}
			})
		})
	}
}

// TestUniqueEdge_BatchChecksItself covers the case the adjacency cannot see: two
// duplicates arriving in one batch, where neither is stored yet when the other
// is validated.
func TestUniqueEdge_BatchChecksItself(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b := twoNodes(t, g)
		if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
			t.Fatalf("DeclareUniqueEdge: %v", err)
		}

		_, err := g.AddEdges([]*store.Edge{owns(a, b), owns(a, b)})
		if !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("a batch containing its own duplicate: want ErrUniqueViolation, got %v", err)
		}
		// A refused batch is a refused batch: nothing from it was applied.
		if n, err := g.EdgeCount(); err != nil || n != 0 {
			t.Fatalf("EdgeCount after a refused batch = %d (err %v), want 0", n, err)
		}

		// The same for a transaction, which resolves its ops in order against
		// its own pending state.
		tx := g.Begin()
		tx.AddEdge(owns(a, b))
		tx.AddEdge(owns(a, b))
		if err := tx.Commit(); !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("a transaction containing its own duplicate: want ErrUniqueViolation, got %v", err)
		}
		if n, err := g.EdgeCount(); err != nil || n != 0 {
			t.Fatalf("EdgeCount after a refused transaction = %d (err %v), want 0", n, err)
		}
	})
}

// TestUniqueEdge_DeleteFreesThePair is the half that makes the constraint usable
// rather than merely safe.
//
// Re-ingesting a source means deleting the subtree it owns and rebuilding it,
// and the rebuild happens inside the same transaction as the delete. A check
// that only consulted the stored adjacency would see every edge the transaction
// had just removed and refuse the whole rebuild.
func TestUniqueEdge_DeleteFreesThePair(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b := twoNodes(t, g)
		if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
			t.Fatalf("DeclareUniqueEdge: %v", err)
		}
		id, err := g.AddEdge(owns(a, b))
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}

		// Delete and re-add across two calls.
		if err := g.DeleteEdge(id); err != nil {
			t.Fatalf("DeleteEdge: %v", err)
		}
		if _, err := g.AddEdge(owns(a, b)); err != nil {
			t.Fatalf("a deleted edge must free its pair: %v", err)
		}

		// And within one transaction, which is the shape a re-ingest uses.
		tx := g.Begin()
		tx.DeleteEdge(mustEdgeBetween(t, g, a, b))
		tx.AddEdge(owns(a, b))
		if err := tx.Commit(); err != nil {
			t.Fatalf("delete-and-rebuild in one transaction: %v", err)
		}
		if n, err := g.EdgeCount(); err != nil || n != 1 {
			t.Fatalf("EdgeCount = %d (err %v), want 1", n, err)
		}
	})
}

// TestUniqueEdge_TransactionFreesItsOwnClaims covers the reservations a
// transaction takes against itself and then gives back.
//
// TestUniqueEdge_DeleteFreesThePair covers a delete of something already stored,
// where the slot was never reserved in the first place. This is the other
// direction: an edge added *and* withdrawn inside one transaction has to release
// the pair it was holding, or the transaction blocks itself on a slot nothing
// will ever occupy. Each sub-case reaches a different release site.
func TestUniqueEdge_TransactionFreesItsOwnClaims(t *testing.T) {
	cases := map[string]func(tx *graphene.Tx, a, b store.NodeID){
		"DeleteEdge": func(tx *graphene.Tx, a, b store.NodeID) {
			id := tx.AddEdge(owns(a, b))
			tx.DeleteEdge(id)
			tx.AddEdge(owns(a, b))
		},
		"DeleteNode cascade": func(tx *graphene.Tx, a, _ store.NodeID) {
			child := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			tx.AddEdge(owns(a, child))
			tx.DeleteNode(child)
			// A fresh child, but the constraint is on the pair, and the pair the
			// cascade freed is the one that mattered.
			again := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			tx.AddEdge(owns(a, again))
		},
		"UpdateEdge dropping the label": func(tx *graphene.Tx, a, b store.NodeID) {
			id := tx.AddEdge(owns(a, b))
			tx.UpdateEdge(&store.Edge{
				ID: id, Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains},
			})
			// The pair is free again: the only edge holding it stopped carrying
			// the declared type.
			tx.AddEdge(owns(a, b))
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			backends(t, func(t *testing.T, g *graphene.Graph) {
				a, b := twoNodes(t, g)
				if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
					t.Fatalf("DeclareUniqueEdge: %v", err)
				}
				tx := g.Begin()
				build(tx, a, b)
				if err := tx.Commit(); err != nil {
					t.Fatalf("a transaction blocked on a slot it had itself released: %v", err)
				}
				// Exactly one edge of the declared type survives, and the
				// constraint still holds afterwards.
				if _, err := g.AddEdge(owns(a, b)); name != "DeleteNode cascade" && !errors.Is(err, store.ErrUniqueViolation) {
					t.Fatalf("after commit the constraint stopped holding: got %v", err)
				}
			})
		})
	}
}

// TestUniqueEdge_ReIngestHoldsCountsStable is the caller's actual workflow, run
// twice: delete the owned subtree and rebuild it, with the constraint on. The
// point is that the second pass produces the same graph rather than a second
// copy of it, and that the shared hub the subtree points at is untouched.
func TestUniqueEdge_ReIngestHoldsCountsStable(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
			t.Fatalf("DeclareUniqueEdge: %v", err)
		}
		hub, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		root, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}

		const owned = 20
		ingest := func(pass int) (nodes, edges uint64) {
			t.Helper()
			tx := g.Begin()
			for _, id := range ownedChildren(t, g, root) {
				tx.DeleteNode(id)
			}
			for range owned {
				child := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
				tx.AddEdge(&store.Edge{Src: root, Dst: child, Labels: []store.EdgeType{edgeOwns}})
				// Every child also points at the shared hub, which nothing owns
				// and nothing may delete.
				tx.AddEdge(&store.Edge{
					Src: child, Dst: hub, Labels: []store.EdgeType{store.EdgeTypeTaggedWith},
				})
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("ingest pass %d: %v", pass, err)
			}
			n, err := g.NodeCount()
			if err != nil {
				t.Fatalf("NodeCount: %v", err)
			}
			e, err := g.EdgeCount()
			if err != nil {
				t.Fatalf("EdgeCount: %v", err)
			}
			return n, e
		}

		n1, e1 := ingest(1)
		n2, e2 := ingest(2)
		if n1 != n2 || e1 != e2 {
			t.Fatalf("re-ingest changed the graph: %d nodes/%d edges became %d/%d", n1, e1, n2, e2)
		}
		if _, err := g.GetNode(hub); err != nil {
			t.Fatalf("the shared hub was collateral damage: %v", err)
		}
	})
}

// TestUniqueEdge_SurvivesCompaction reaches the half of the disk backend the
// other tests cannot.
//
// Every edge those write stays in the delta layer, so the enforcement scan only
// ever consults one of the two sources it has to merge. After a compaction the
// edges are in the CSR image and the delta is empty, which is the state a store
// spends most of its life in — and the state where a check that only looked at
// the delta would silently stop enforcing anything.
func TestUniqueEdge_SurvivesCompaction(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	a, b := twoNodes(t, g)
	if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
		t.Fatalf("DeclareUniqueEdge: %v", err)
	}
	first, err := g.AddEdge(owns(a, b))
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	_, err = g.AddEdge(owns(a, b))
	if !errors.Is(err, store.ErrUniqueViolation) {
		t.Fatalf("after compaction the constraint stopped enforcing: got %v", err)
	}
	var taken *store.ErrEdgeTaken
	if errors.As(err, &taken) && taken.Owner != first {
		t.Errorf("incumbent reported as %d, want the compacted edge %d", taken.Owner, first)
	}

	// A delete recorded in the delta must still free a pair the image holds.
	if err := g.DeleteEdge(first); err != nil {
		t.Fatalf("DeleteEdge: %v", err)
	}
	if _, err := g.AddEdge(owns(a, b)); err != nil {
		t.Fatalf("deleting a compacted edge must free its pair: %v", err)
	}

	// And declaring over an already-compacted graph has to see the image too.
	c, d := twoNodes(t, g)
	for range 2 {
		if _, err := g.AddEdge(&store.Edge{
			Src: c, Dst: d, Labels: []store.EdgeType{store.EdgeTypeReuse},
		}); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	err = g.DeclareUniqueEdge(store.EdgeTypeReuse)
	if !errors.Is(err, store.ErrUniqueViolation) {
		t.Fatalf("declaring over a violating compacted graph: want ErrUniqueViolation, got %v", err)
	}
}

// TestUniqueEdge_UnsupportedBackendErrors pins the rule that separates a
// constraint from an optimisation. A store that cannot enforce this must say so
// rather than accept the declaration and enforce nothing.
func TestUniqueEdge_UnsupportedBackendErrors(t *testing.T) {
	g := &graphene.Graph{GraphStore: noCardinalityStore{graphene.NewInMemory().GraphStore}}
	if err := g.DeclareUniqueEdge(edgeOwns); err == nil {
		t.Fatal("a backend that cannot enforce the constraint accepted the declaration")
	}
	if got := g.UniqueEdges(); got != nil {
		t.Errorf("UniqueEdges on an unsupporting backend = %v, want nil", got)
	}
	// EdgeIDBetween still has to work there, by the slower route.
	a, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id, err := g.AddEdge(owns(a, b))
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	got, found, err := g.EdgeIDBetween(a, b, nil)
	if err != nil || !found || got != id {
		t.Fatalf("EdgeIDBetween fallback = (%d, %v, %v), want (%d, true, nil)", got, found, err, id)
	}
}

// noCardinalityStore hides the extension without changing anything else, which
// is what a third-party GraphStore implementation looks like from here.
type noCardinalityStore struct{ store.GraphStore }

func mustEdgeBetween(t *testing.T, g *graphene.Graph, src, dst store.NodeID) store.EdgeID {
	t.Helper()
	id, found, err := g.EdgeIDBetween(src, dst, []store.EdgeType{edgeOwns})
	if err != nil || !found {
		t.Fatalf("EdgeIDBetween(%d, %d) = (%d, %v, %v), want a live edge", src, dst, id, found, err)
	}
	return id
}

// ownedChildren returns the nodes root owns, which is the subtree a re-ingest
// replaces.
func ownedChildren(t *testing.T, g *graphene.Graph, root store.NodeID) []store.NodeID {
	t.Helper()
	edges, err := g.EdgesOf(root, store.DirectionOutbound, []store.EdgeType{edgeOwns})
	if err != nil {
		t.Fatalf("EdgesOf: %v", err)
	}
	out := make([]store.NodeID, 0, len(edges))
	for _, e := range edges {
		out = append(out, e.Dst)
	}
	return out
}
