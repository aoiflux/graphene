package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Attribution through the public API.
//
// Tx.As records who made a change. The engine stores what it is told and does
// not verify it — Graphene is linked into the caller's process, so an actor is
// asserted rather than proven. These tests pin the plumbing and, more usefully,
// the boundaries of what it claims.

func TestTxAs_AttributedOnDiskBackend(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	tx := g.Begin().As(store.TxContext{ActorID: 77, RoleID: 3, KeyID: 11})
	if !tx.Attributed() {
		t.Fatal("disk backend implements ActorTransactor; the transaction should be attributed")
	}
	tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	n, _ := g.NodeCount()
	if n != 1 {
		t.Fatalf("node count %d, want 1", n)
	}
}

// The in-memory backend accepts an actor and has nowhere durable to keep it.
// Attributed reports that rather than letting a caller assume otherwise — the
// same honesty Tx.Atomic already applies to the non-atomic fallback.
func TestTxAs_MemoryBackendReportsUnattributed(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()

	tx := g.Begin().As(store.TxContext{ActorID: 77})
	if tx.Attributed() {
		t.Fatal("the in-memory backend keeps no log; attribution cannot be durable there")
	}

	tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err := tx.Commit(); err != nil {
		t.Fatalf("an unattributable actor must not break the commit: %v", err)
	}
}

// A transaction with no actor is unattributed, and says so.
func TestTxAs_UnsetIsUnattributed(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if g.Begin().Attributed() {
		t.Fatal("a transaction with no actor reported itself attributed")
	}
	// Setting an all-zero context is still no actor.
	if g.Begin().As(store.TxContext{}).Attributed() {
		t.Fatal("the zero TxContext is not an actor")
	}
}

// Attribution must not change what a transaction does. An attributed and an
// unattributed transaction differ in the commit record and nowhere else.
func TestTxAs_DoesNotAlterTransactionSemantics(t *testing.T) {
	build := func(actor bool) (uint64, uint64) {
		dir := t.TempDir()
		g, err := graphene.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()

		tx := g.Begin()
		if actor {
			tx = tx.As(store.TxContext{ActorID: 5})
		}
		a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		b := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		n, _ := g.NodeCount()
		e, _ := g.EdgeCount()
		return n, e
	}

	nA, eA := build(false)
	nB, eB := build(true)
	if nA != nB || eA != eB {
		t.Fatalf("attribution changed the outcome: unattributed (%d,%d) vs attributed (%d,%d)",
			nA, eA, nB, eB)
	}
}

// Attribution survives a reopen, because it lives in the log the reopen replays.
func TestTxAs_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	tx := g.Begin().As(store.TxContext{ActorID: 1234})
	tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen after an attributed commit: %v", err)
	}
	defer g2.Close()

	n, _ := g2.NodeCount()
	if n != 1 {
		t.Fatalf("node count after reopen %d, want 1", n)
	}
}
