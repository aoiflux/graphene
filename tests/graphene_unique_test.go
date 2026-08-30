package graphene_test

// Unique property keys.
//
// The constraint exists so that an indexed value can be used as a name. Without
// it a natural key resolves to a set, and "find the node for this APK, or make
// one" is not expressible: every re-ingest appends to the set instead of finding
// what is already there. These tests pin the three things a caller has to be
// able to rely on — that a duplicate is refused, that declaring on a graph that
// already has duplicates reports all of them, and that re-declaring is free.

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func TestUnique_RefusesASecondHolder(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}

		first := mustNode(t, g)
		if err := g.IndexNodeProperties(first, map[string][]byte{"k": []byte("ver:9f3a")}); err != nil {
			t.Fatalf("first registration: %v", err)
		}

		second := mustNode(t, g)
		err := g.IndexNodeProperties(second, map[string][]byte{"k": []byte("ver:9f3a")})
		if !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("second holder: got %v, want ErrUniqueViolation", err)
		}

		// The refusal left nothing behind: the value still resolves to exactly
		// the first node.
		ids, err := g.NodesByProperty("k", []byte("ver:9f3a"))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(ids) != 1 || ids[0] != first {
			t.Fatalf("after a refused registration the key resolves to %v, want [%d]", ids, first)
		}
	})
}

// An entity re-registering the value it already holds is the steady state an
// idempotent re-ingest arrives at, and must not be mistaken for a conflict.
func TestUnique_ReRegisteringYourOwnValueIsFine(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		id := mustNode(t, g)
		for i := 0; i < 3; i++ {
			if err := g.IndexNodeProperties(id, map[string][]byte{"k": []byte("ver:same")}); err != nil {
				t.Fatalf("registration %d: %v", i, err)
			}
		}
		ids, err := g.NodesByProperty("k", []byte("ver:same"))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(ids) != 1 {
			t.Fatalf("three identical registrations produced %d postings", len(ids))
		}
	})
}

// Declaring on an existing graph must name every violation, because the caller's
// next move is to repair it and one duplicate per pass is not a repair.
func TestUnique_DeclarationReportsEveryViolation(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		// Three values duplicated, one held once. The singleton is the control:
		// a check that reports everything is as useless as one that reports the
		// first thing.
		dupes := []string{"ver:aaa", "ver:bbb", "ver:ccc"}
		for _, v := range dupes {
			for i := 0; i < 2; i++ {
				id := mustNode(t, g)
				if err := g.IndexNodeProperties(id, map[string][]byte{"k": []byte(v)}); err != nil {
					t.Fatalf("seed %s: %v", v, err)
				}
			}
		}
		solo := mustNode(t, g)
		if err := g.IndexNodeProperties(solo, map[string][]byte{"k": []byte("ver:solo")}); err != nil {
			t.Fatalf("seed solo: %v", err)
		}

		err := g.DeclareUniqueProperty("k")
		var v *store.UniqueViolationsError
		if !errors.As(err, &v) {
			t.Fatalf("declaration over duplicates: got %v, want *UniqueViolationsError", err)
		}
		if !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatal("a violations error does not satisfy errors.Is(ErrUniqueViolation)")
		}
		if len(v.Conflicts) != len(dupes) {
			t.Fatalf("reported %d conflicts, want %d: %v", len(v.Conflicts), len(dupes), v.Conflicts)
		}
		if !v.Sorted() {
			t.Fatalf("conflicts are not in value order: %v", v.Conflicts)
		}
		for i, c := range v.Conflicts {
			if string(c.Value) != dupes[i] {
				t.Fatalf("conflict %d is %q, want %q", i, c.Value, dupes[i])
			}
			if len(c.IDs) != 2 {
				t.Fatalf("conflict %q names %d ids, want 2", c.Value, len(c.IDs))
			}
		}

		// Nothing was declared, so the key is still free.
		if nodeKeys, _ := g.UniqueProperties(); len(nodeKeys) != 0 {
			t.Fatalf("a refused declaration still took effect: %v", nodeKeys)
		}
	})
}

// The repair loop the previous test exists to enable, run to completion.
func TestUnique_DeclarationSucceedsOnceDuplicatesAreRemoved(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		for i := 0; i < 3; i++ {
			id := mustNode(t, g)
			if err := g.IndexNodeProperties(id, map[string][]byte{"k": []byte("ver:dup")}); err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
		}

		err := g.DeclareUniqueProperty("k")
		var v *store.UniqueViolationsError
		if !errors.As(err, &v) {
			t.Fatalf("expected a violations error, got %v", err)
		}

		// Keep the lowest id, which is what a caller with no other information
		// would do, and delete the rest.
		keep := v.Conflicts[0].IDs[0]
		for _, id := range v.Conflicts[0].IDs {
			if id == keep {
				continue
			}
			if err := g.DeleteNode(store.NodeID(id)); err != nil {
				t.Fatalf("DeleteNode(%d): %v", id, err)
			}
		}

		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("declaration after repair: %v", err)
		}
		// And it is idempotent, which is what makes declaring at every Open the
		// documented shape rather than a thing callers have to guard.
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("re-declaration: %v", err)
		}
		nodeKeys, _ := g.UniqueProperties()
		if len(nodeKeys) != 1 || nodeKeys[0] != "k" {
			t.Fatalf("UniqueProperties reports %v, want [k]", nodeKeys)
		}
	})
}

// A transaction is refused whole, and refused before anything is written — the
// conflict is found during resolution, which has touched nothing.
func TestUnique_TransactionIsRefusedWhole(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		incumbent := mustNode(t, g)
		if err := g.IndexNodeProperties(incumbent, map[string][]byte{"k": []byte("taken")}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		before, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}

		tx := g.Begin()
		a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.IndexNodeProperties(a, map[string][]byte{"k": []byte("fresh")})
		b := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.IndexNodeProperties(b, map[string][]byte{"k": []byte("taken")})
		if err := tx.Commit(); !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("Commit: got %v, want ErrUniqueViolation", err)
		}

		after, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if after != before {
			t.Fatalf("a refused transaction added %d nodes", after-before)
		}
		// Including the node whose own entry was perfectly fine.
		ids, err := g.NodesByProperty("k", []byte("fresh"))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("the innocent half of a refused transaction was applied: %v", ids)
		}
	})
}

// Two entities claiming one value inside a single transaction. The index cannot
// catch this — nothing has been registered there yet — so the resolver has to
// track what the transaction itself has taken.
func TestUnique_TwoClaimsInOneTransactionCollide(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		tx := g.Begin()
		a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.IndexNodeProperties(a, map[string][]byte{"k": []byte("one")})
		b := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.IndexNodeProperties(b, map[string][]byte{"k": []byte("one")})
		if err := tx.Commit(); !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("Commit: got %v, want ErrUniqueViolation", err)
		}
	})
}

func TestUnique_NodeByPropertyIsTotal(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		// Undeclared: refused rather than answered, so a caller cannot believe a
		// constraint is in force when it is not.
		if _, err := g.NodeByProperty("k", []byte("x")); err == nil {
			t.Fatal("NodeByProperty answered on an undeclared key")
		}

		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}

		var nf *store.ErrNotFound
		if _, err := g.NodeByProperty("k", []byte("absent")); !errors.As(err, &nf) {
			t.Fatalf("NodeByProperty on an unheld value: got %v, want ErrNotFound", err)
		}

		id := mustNode(t, g)
		if err := g.IndexNodeProperties(id, map[string][]byte{"k": []byte("here")}); err != nil {
			t.Fatalf("register: %v", err)
		}
		got, err := g.NodeByProperty("k", []byte("here"))
		if err != nil {
			t.Fatalf("NodeByProperty: %v", err)
		}
		if got.ID != id {
			t.Fatalf("NodeByProperty returned %d, want %d", got.ID, id)
		}
	})
}

// Edges carry the same machinery, declared independently: a key name that is
// unique for nodes says nothing about edges.
func TestUnique_EdgeKeysAreDeclaredIndependently(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		src, dst := twoNodes(t, g)
		mk := func() store.EdgeID {
			t.Helper()
			id, err := g.AddEdge(&store.Edge{Src: src, Dst: dst, Labels: []store.EdgeType{store.EdgeTypeContains}})
			if err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
			return id
		}

		// Not declared for edges yet: duplicates are legal.
		for i := 0; i < 2; i++ {
			if err := g.IndexEdgeProperties(mk(), map[string][]byte{"k": []byte("rel")}); err != nil {
				t.Fatalf("edge registration %d: %v", i, err)
			}
		}
		if err := g.DeclareUniqueEdgeProperty("k"); !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("edge declaration over duplicates: got %v, want ErrUniqueViolation", err)
		}

		if err := g.DeclareUniqueEdgeProperty("other"); err != nil {
			t.Fatalf("DeclareUniqueEdgeProperty: %v", err)
		}
		if err := g.IndexEdgeProperties(mk(), map[string][]byte{"other": []byte("v")}); err != nil {
			t.Fatalf("first edge holder: %v", err)
		}
		if err := g.IndexEdgeProperties(mk(), map[string][]byte{"other": []byte("v")}); !errors.Is(err, store.ErrUniqueViolation) {
			t.Fatalf("second edge holder: got %v, want ErrUniqueViolation", err)
		}
	})
}

// Constraints are enforced from the declaration onward, in the process that
// declared them — so a reopened store that re-declares gets its data validated
// again, which is what makes "declare at every Open" load-bearing rather than
// merely harmless.
func TestUnique_DeclarationsDoNotSurviveAReopen(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := g.DeclareUniqueProperty("k"); err != nil {
		t.Fatalf("DeclareUniqueProperty: %v", err)
	}
	id := mustNode(t, g)
	if err := g.IndexNodeProperties(id, map[string][]byte{"k": []byte("ver:1")}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	re, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer re.Close()

	if nodeKeys, _ := re.UniqueProperties(); len(nodeKeys) != 0 {
		t.Fatalf("a reopened store reports %v as unique; declarations are documented as in-memory", nodeKeys)
	}
	// Re-declaring absorbs the entries written before the reopen.
	if err := re.DeclareUniqueProperty("k"); err != nil {
		t.Fatalf("re-declaration after reopen: %v", err)
	}
	other := mustNode(t, re)
	if err := re.IndexNodeProperties(other, map[string][]byte{"k": []byte("ver:1")}); !errors.Is(err, store.ErrUniqueViolation) {
		t.Fatalf("the re-declared constraint did not absorb pre-existing entries: %v", err)
	}
}

func mustNode(t *testing.T, g *graphene.Graph) store.NodeID {
	t.Helper()
	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return id
}
