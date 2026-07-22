package graphene_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Transaction semantics.
//
// The property that matters is the one the slice APIs cannot provide: a mixed
// set of nodes and edges commits together or not at all. Everything here is
// asserted on both backends, because memory is the oracle disk is judged against
// and a divergence in *what a failed transaction leaves behind* is exactly the
// kind of difference that would otherwise go unnoticed.

func txBackends(t *testing.T) map[string]*graphene.Graph {
	t.Helper()
	disk, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	return map[string]*graphene.Graph{
		"memory": graphene.NewInMemory(),
		"disk":   disk,
	}
}

// TestTxMixedCommit is the case the slice APIs cannot express: nodes and the
// edges between them, in one commit.
func TestTxMixedCommit(t *testing.T) {
	for name, g := range txBackends(t) {
		t.Run(name, func(t *testing.T) {
			tx := g.Begin()
			if !tx.Atomic() {
				t.Fatal("bundled backends must be atomic")
			}

			a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			b := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
			// The edge references nodes that do not exist in the store yet.
			e := tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})

			if a == store.InvalidNodeID || b == store.InvalidNodeID || e == store.InvalidEdgeID {
				t.Fatalf("reserved an invalid id: a=%d b=%d e=%d", a, b, e)
			}

			// Nothing is visible before Commit.
			if _, err := g.GraphStore.GetNode(a); err == nil {
				t.Error("node visible before commit")
			}

			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			for _, id := range []store.NodeID{a, b} {
				if _, err := g.GraphStore.GetNode(id); err != nil {
					t.Errorf("node %d missing after commit: %v", id, err)
				}
			}
			got, err := g.GraphStore.GetEdge(e)
			if err != nil {
				t.Fatalf("edge missing after commit: %v", err)
			}
			if got.Src != a || got.Dst != b {
				t.Errorf("edge endpoints = (%d,%d), want (%d,%d)", got.Src, got.Dst, a, b)
			}
		})
	}
}

// TestTxRollbackWritesNothing pins that a rolled-back transaction is invisible.
func TestTxRollbackWritesNothing(t *testing.T) {
	for name, g := range txBackends(t) {
		t.Run(name, func(t *testing.T) {
			tx := g.Begin()
			a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			b := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})

			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}

			if _, err := g.GraphStore.GetNode(a); err == nil {
				t.Error("node exists after rollback")
			}
			nodes, err := g.GraphStore.NodesByType(store.NodeTypeCase)
			if err != nil {
				t.Fatalf("NodesByType: %v", err)
			}
			if len(nodes) != 0 {
				t.Errorf("rollback left %d nodes behind", len(nodes))
			}

			// A finished transaction rejects everything afterwards.
			if err := tx.Commit(); !errors.Is(err, graphene.ErrTxDone) {
				t.Errorf("Commit after Rollback = %v, want ErrTxDone", err)
			}
			if err := tx.Rollback(); !errors.Is(err, graphene.ErrTxDone) {
				t.Errorf("double Rollback = %v, want ErrTxDone", err)
			}
		})
	}
}

// TestTxFailedCommitAppliesNothing is the core guarantee. A transaction with a
// bad edge must leave *no* trace — in particular, the good nodes ahead of it
// must not land. That is precisely what AddNodes-then-AddEdges cannot promise.
func TestTxFailedCommitAppliesNothing(t *testing.T) {
	for name, g := range txBackends(t) {
		t.Run(name, func(t *testing.T) {
			// A pre-existing node, so we can tell "transaction rolled back" from
			// "store was empty anyway".
			existing, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}

			tx := g.Begin()
			a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			b := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})
			// Dangling endpoint: no such node, in the store or the transaction.
			tx.AddEdge(&store.Edge{Src: a, Dst: store.NodeID(1 << 40), Labels: []store.EdgeType{store.EdgeTypeContains}})

			err = tx.Commit()
			if err == nil {
				t.Fatal("Commit succeeded with a dangling edge endpoint")
			}
			var invalid *store.ErrInvalidEdge
			if !errors.As(err, &invalid) {
				t.Errorf("error = %v, want ErrInvalidEdge", err)
			}

			// Neither node from the transaction may exist.
			for _, id := range []store.NodeID{a, b} {
				if _, err := g.GraphStore.GetNode(id); err == nil {
					t.Errorf("node %d landed despite a failed commit", id)
				}
			}
			cases, err := g.GraphStore.NodesByType(store.NodeTypeCase)
			if err != nil {
				t.Fatalf("NodesByType: %v", err)
			}
			if len(cases) != 0 {
				t.Errorf("failed commit left %d nodes behind: %v", len(cases), cases)
			}

			// And the store is otherwise untouched.
			if _, err := g.GraphStore.GetNode(existing); err != nil {
				t.Errorf("pre-existing node disturbed: %v", err)
			}
		})
	}
}

// TestTxEdgeToExistingNode covers the mixed case: an edge from a node created in
// this transaction to one that already existed.
func TestTxEdgeToExistingNode(t *testing.T) {
	for name, g := range txBackends(t) {
		t.Run(name, func(t *testing.T) {
			existing, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}

			tx := g.Begin()
			fresh := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			e := tx.AddEdge(&store.Edge{Src: fresh, Dst: existing, Labels: []store.EdgeType{store.EdgeTypeContains}})
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			got, err := g.GraphStore.GetEdge(e)
			if err != nil {
				t.Fatalf("GetEdge: %v", err)
			}
			if got.Src != fresh || got.Dst != existing {
				t.Errorf("endpoints = (%d,%d), want (%d,%d)", got.Src, got.Dst, fresh, existing)
			}
		})
	}
}

// TestTxCopiesCallerData extends invariant 9 to the transaction path: buffering
// must copy, because a caller may reuse its slices long before Commit.
func TestTxCopiesCallerData(t *testing.T) {
	for name, g := range txBackends(t) {
		t.Run(name, func(t *testing.T) {
			want := []byte("tx-payload")
			buf := append([]byte(nil), want...)
			labels := []store.NodeType{store.NodeTypeCase}

			tx := g.Begin()
			id := tx.AddNode(&store.Node{Labels: labels, Properties: buf})

			// Caller reuses both slices while the transaction is still open.
			for i := range buf {
				buf[i] = 'X'
			}
			labels[0] = store.NodeTypeTag

			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			got, err := g.GraphStore.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if !bytes.Equal(got.Properties, want) {
				t.Errorf("properties = %q, want %q", got.Properties, want)
			}
			if len(got.Labels) != 1 || got.Labels[0] != store.NodeTypeCase {
				t.Errorf("labels = %v, want [Case]", got.Labels)
			}
		})
	}
}

// TestTxSurvivesReopen checks the committed transaction is durable and replays
// in an order where every edge's endpoints already exist.
func TestTxSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	tx := g.Begin()
	const n = 50
	ids := make([]store.NodeID, n)
	for i := range ids {
		ids[i] = tx.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("payload-%02d", i)),
		})
	}
	for i := 0; i < n-1; i++ {
		tx.AddEdge(&store.Edge{Src: ids[i], Dst: ids[i+1], Labels: []store.EdgeType{store.EdgeTypeContains}})
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	for i, id := range ids {
		got, err := reopened.GraphStore.GetNode(id)
		if err != nil {
			t.Fatalf("node %d missing after reopen: %v", id, err)
		}
		want := fmt.Sprintf("payload-%02d", i)
		if string(got.Properties) != want {
			t.Errorf("node %d properties = %q, want %q", id, got.Properties, want)
		}
	}
	edges, err := reopened.GraphStore.EdgesOf(ids[0], store.DirectionOutbound, nil)
	if err != nil {
		t.Fatalf("EdgesOf: %v", err)
	}
	if len(edges) != 1 {
		t.Errorf("node 0 has %d outbound edges after reopen, want 1", len(edges))
	}
}

// TestTxIDsAreNeverReused pins the documented consequence of reserving early: a
// rolled-back transaction burns its IDs, and a later write must not reuse them.
func TestTxIDsAreNeverReused(t *testing.T) {
	for name, g := range txBackends(t) {
		t.Run(name, func(t *testing.T) {
			tx := g.Begin()
			burned := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}

			next, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if next == burned {
				t.Errorf("id %d was reused after rollback", next)
			}
			if next <= burned {
				t.Errorf("ids went backwards: burned=%d next=%d", burned, next)
			}
		})
	}
}

// TestTxEmptyCommit is a boundary case that should be free, not an error.
func TestTxEmptyCommit(t *testing.T) {
	for name, g := range txBackends(t) {
		t.Run(name, func(t *testing.T) {
			if err := g.Begin().Commit(); err != nil {
				t.Errorf("empty Commit: %v", err)
			}
		})
	}
}

// TestTxParityOnFailure asserts both backends agree about what a failed
// transaction leaves behind — nothing.
func TestTxParityOnFailure(t *testing.T) {
	results := map[string]int{}
	for name, g := range txBackends(t) {
		tx := g.Begin()
		a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.AddEdge(&store.Edge{Src: a, Dst: store.NodeID(1 << 40)})

		if err := tx.Commit(); err == nil {
			t.Fatalf("%s: Commit succeeded with a dangling endpoint", name)
		}
		nodes, err := g.GraphStore.NodesByType(store.NodeTypeCase)
		if err != nil {
			t.Fatalf("%s: NodesByType: %v", name, err)
		}
		results[name] = len(nodes)
	}
	if results["memory"] != results["disk"] {
		t.Errorf("backends disagree on what a failed transaction leaves: memory=%d disk=%d",
			results["memory"], results["disk"])
	}
	if results["disk"] != 0 {
		t.Errorf("failed transaction left %d nodes behind", results["disk"])
	}
}
