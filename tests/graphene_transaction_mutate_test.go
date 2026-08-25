package graphene_test

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Transactional update and delete.
//
// Adding update/delete to a transaction is not just more operation kinds. It
// introduces ordering — an operation now has to be evaluated against the store
// *as modified by the operations before it* — and it introduces the cascade: a
// node deletion takes its incident edges with it, including edges the same
// transaction created moments earlier.
//
// These are the cases that only exist because of that. Everything runs on both
// backends: "which transactions are rejected" has to match, not merely what a
// successful one leaves behind.

func txMutBackends(t *testing.T) map[string]*graphene.Graph {
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

// seedPair creates two nodes and an edge between them, outside any transaction.
func seedPair(t *testing.T, g *graphene.Graph) (a, b store.NodeID, e store.EdgeID) {
	t.Helper()
	var err error
	if a, err = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if b, err = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if e, err = g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}}); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	return a, b, e
}

// TestTxUpdateAndDelete covers the basic mixed mutation.
func TestTxUpdateAndDelete(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			a, b, e := seedPair(t, g)

			tx := g.Begin()
			tx.UpdateNode(&store.Node{ID: a, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("updated")})
			tx.DeleteEdge(e)
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			got, err := g.GraphStore.GetNode(a)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if string(got.Properties) != "updated" {
				t.Errorf("properties = %q, want %q", got.Properties, "updated")
			}
			if len(got.Labels) != 1 || got.Labels[0] != store.NodeTypeTag {
				t.Errorf("labels = %v, want [Tag]", got.Labels)
			}
			// Label postings must have moved with the update.
			cases, _ := g.GraphStore.NodesByType(store.NodeTypeCase)
			for _, id := range cases {
				if id == a {
					t.Error("node still listed under its old label")
				}
			}
			if _, err := g.GraphStore.GetEdge(e); err == nil {
				t.Error("edge survived deletion")
			}
			if _, err := g.GraphStore.GetNode(b); err != nil {
				t.Errorf("unrelated node disturbed: %v", err)
			}
		})
	}
}

// TestTxDeleteNodeCascades pins that a node deletion inside a transaction takes
// its edges with it, exactly as DeleteNode does outside one.
func TestTxDeleteNodeCascades(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			a, b, e := seedPair(t, g)

			tx := g.Begin()
			tx.DeleteNode(a)
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			if _, err := g.GraphStore.GetNode(a); err == nil {
				t.Error("node survived deletion")
			}
			if _, err := g.GraphStore.GetEdge(e); err == nil {
				t.Error("incident edge was not cascaded")
			}
			if _, err := g.GraphStore.GetNode(b); err != nil {
				t.Errorf("far endpoint deleted too: %v", err)
			}
			// b must have no dangling adjacency left.
			edges, err := g.GraphStore.EdgesOf(b, store.DirectionInbound, nil)
			if err != nil {
				t.Fatalf("EdgesOf: %v", err)
			}
			if len(edges) != 0 {
				t.Errorf("far endpoint still lists %d inbound edges", len(edges))
			}
		})
	}
}

// TestTxCascadeCoversEdgesCreatedInSameTx is the case that made this hard: the
// cascade has to see edges that exist only inside the transaction.
func TestTxCascadeCoversEdgesCreatedInSameTx(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			tx := g.Begin()
			a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			b := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			e := tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})
			// Delete one endpoint in the same transaction. The edge above exists
			// nowhere but in this transaction, and must still be cascaded.
			tx.DeleteNode(a)
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			if _, err := g.GraphStore.GetNode(a); err == nil {
				t.Error("node a survived")
			}
			if _, err := g.GraphStore.GetEdge(e); err == nil {
				t.Error("edge created and cascaded in the same transaction survived")
			}
			if _, err := g.GraphStore.GetNode(b); err != nil {
				t.Errorf("node b should survive: %v", err)
			}
			edges, err := g.GraphStore.EdgesOf(b, store.DirectionInbound, nil)
			if err != nil {
				t.Fatalf("EdgesOf: %v", err)
			}
			if len(edges) != 0 {
				t.Errorf("node b still lists %d inbound edges", len(edges))
			}
		})
	}
}

// TestTxOrderingMatters asserts operations apply in issue order, not grouped by
// kind. Grouping creates-before-deletes would make this transaction a no-op
// instead of a deletion.
func TestTxOrderingMatters(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			tx := g.Begin()
			a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			tx.DeleteNode(a) // created then removed, all before commit
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			if _, err := g.GraphStore.GetNode(a); err == nil {
				t.Error("node created and deleted in one transaction still exists")
			}
			cases, _ := g.GraphStore.NodesByType(store.NodeTypeCase)
			if len(cases) != 0 {
				t.Errorf("label postings still list %d nodes", len(cases))
			}
		})
	}
}

// TestTxDeleteThenRecreateEdge covers the reverse order: an edge removed and
// replaced within one transaction.
func TestTxDeleteThenRecreateEdge(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			a, b, old := seedPair(t, g)

			tx := g.Begin()
			tx.DeleteEdge(old)
			fresh := tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			if _, err := g.GraphStore.GetEdge(old); err == nil {
				t.Error("old edge survived")
			}
			got, err := g.GraphStore.GetEdge(fresh)
			if err != nil {
				t.Fatalf("replacement edge missing: %v", err)
			}
			if got.Labels[0] != store.EdgeTypeSimilarTo {
				t.Errorf("replacement has label %v", got.Labels)
			}
			edges, err := g.GraphStore.EdgesOf(a, store.DirectionOutbound, nil)
			if err != nil {
				t.Fatalf("EdgesOf: %v", err)
			}
			if len(edges) != 1 {
				t.Errorf("node a has %d outbound edges, want exactly 1", len(edges))
			}
		})
	}
}

// TestTxEdgeOntoNodeDeletedEarlierFails: ordering cuts both ways. An edge onto a
// node this transaction already removed must be rejected.
func TestTxEdgeOntoNodeDeletedEarlierFails(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			a, b, _ := seedPair(t, g)

			tx := g.Begin()
			tx.DeleteNode(b)
			tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})

			err := tx.Commit()
			if err == nil {
				t.Fatal("edge onto a node deleted earlier in the transaction was accepted")
			}
			var invalid *store.ErrInvalidEdge
			if !errors.As(err, &invalid) {
				t.Errorf("error = %v, want ErrInvalidEdge", err)
			}
			// And the rejection rolls back the delete too.
			if _, err := g.GraphStore.GetNode(b); err != nil {
				t.Errorf("failed transaction left node b deleted: %v", err)
			}
		})
	}
}

// TestTxFailedMutationAppliesNothing is the atomicity guarantee, now with
// updates and deletes in the mix.
func TestTxFailedMutationAppliesNothing(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			a, b, e := seedPair(t, g)

			tx := g.Begin()
			tx.UpdateNode(&store.Node{ID: a, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("should-not-land")})
			tx.DeleteEdge(e)
			tx.DeleteNode(b)
			tx.DeleteEdge(store.EdgeID(1 << 40)) // no such edge

			if err := tx.Commit(); err == nil {
				t.Fatal("Commit succeeded with a missing edge")
			}

			// Every earlier operation must have been discarded.
			got, err := g.GraphStore.GetNode(a)
			if err != nil {
				t.Fatalf("node a missing: %v", err)
			}
			if len(got.Properties) != 0 {
				t.Errorf("update landed despite failed commit: %q", got.Properties)
			}
			if got.Labels[0] != store.NodeTypeCase {
				t.Errorf("label update landed: %v", got.Labels)
			}
			if _, err := g.GraphStore.GetEdge(e); err != nil {
				t.Errorf("edge deletion landed despite failed commit: %v", err)
			}
			if _, err := g.GraphStore.GetNode(b); err != nil {
				t.Errorf("node deletion landed despite failed commit: %v", err)
			}
		})
	}
}

// TestTxUpdateMissingFails covers the not-found paths.
func TestTxUpdateMissingFails(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			tx := g.Begin()
			tx.UpdateNode(&store.Node{ID: store.NodeID(1 << 40), Labels: []store.NodeType{store.NodeTypeTag}})
			err := tx.Commit()
			if err == nil {
				t.Fatal("update of a missing node succeeded")
			}
			var nf *store.ErrNotFound
			if !errors.As(err, &nf) {
				t.Errorf("error = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestTxMutationSurvivesReopen checks the transactional deletes and updates are
// durable, and that replay order never exposes an edge whose node is gone.
func TestTxMutationSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	a, b, e := seedPair(t, g)
	keep, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	tx := g.Begin()
	tx.UpdateNode(&store.Node{ID: keep, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("kept")})
	tx.DeleteNode(a) // cascades e
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

	if _, err := reopened.GraphStore.GetNode(a); err == nil {
		t.Error("deleted node came back after reopen")
	}
	if _, err := reopened.GraphStore.GetEdge(e); err == nil {
		t.Error("cascaded edge came back after reopen")
	}
	got, err := reopened.GraphStore.GetNode(keep)
	if err != nil {
		t.Fatalf("updated node missing after reopen: %v", err)
	}
	if string(got.Properties) != "kept" {
		t.Errorf("properties = %q, want %q", got.Properties, "kept")
	}
	if _, err := reopened.GraphStore.GetNode(b); err != nil {
		t.Errorf("surviving endpoint missing after reopen: %v", err)
	}
	edges, err := reopened.GraphStore.EdgesOf(b, store.DirectionInbound, nil)
	if err != nil {
		t.Fatalf("EdgesOf: %v", err)
	}
	if len(edges) != 0 {
		t.Errorf("surviving endpoint lists %d inbound edges after reopen", len(edges))
	}
}

// TestTxMutationParity compares the two backends across a set of transactions,
// including ones expected to fail. A divergence here is a correctness signal.
func TestTxMutationParity(t *testing.T) {
	cases := []struct {
		name string
		run  func(g *graphene.Graph) error
	}{
		{"delete node cascades", func(g *graphene.Graph) error {
			a, _, _ := seedPairNoT(g)
			tx := g.Begin()
			tx.DeleteNode(a)
			return tx.Commit()
		}},
		{"create then delete", func(g *graphene.Graph) error {
			tx := g.Begin()
			a := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			tx.DeleteNode(a)
			return tx.Commit()
		}},
		{"edge onto deleted node", func(g *graphene.Graph) error {
			a, b, _ := seedPairNoT(g)
			tx := g.Begin()
			tx.DeleteNode(b)
			tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})
			return tx.Commit()
		}},
		{"update missing node", func(g *graphene.Graph) error {
			tx := g.Begin()
			tx.UpdateNode(&store.Node{ID: store.NodeID(1 << 40), Labels: []store.NodeType{store.NodeTypeTag}})
			return tx.Commit()
		}},
		{"delete twice", func(g *graphene.Graph) error {
			a, _, _ := seedPairNoT(g)
			tx := g.Begin()
			tx.DeleteNode(a)
			tx.DeleteNode(a)
			return tx.Commit()
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backends := txMutBackends(t)
			memErr := tc.run(backends["memory"])
			diskErr := tc.run(backends["disk"])

			if (memErr == nil) != (diskErr == nil) {
				t.Fatalf("backends disagree on acceptance: memory=%v disk=%v", memErr, diskErr)
			}
			memCount, _ := backends["memory"].GraphStore.NodesByType(store.NodeTypeCase)
			diskCount, _ := backends["disk"].GraphStore.NodesByType(store.NodeTypeCase)
			if len(memCount) != len(diskCount) {
				t.Errorf("backends disagree on surviving nodes: memory=%d disk=%d",
					len(memCount), len(diskCount))
			}
		})
	}
}

// TestTxUpdateHonoursReindexPolicy pins that a transactional update leaves the
// property index in the same state a plain UpdateNode would.
//
// This is not a hypothetical: the first version of transactional update skipped
// the ReindexPurge branch that Graph.UpdateNode applies, so the same edit left
// stale index entries when made inside a transaction and removed them when made
// outside one — two ways of doing the same thing, disagreeing silently.
func TestTxUpdateHonoursReindexPolicy(t *testing.T) {
	for name, g := range txMutBackends(t) {
		t.Run(name, func(t *testing.T) {
			g.SetReindexPolicy(store.ReindexPurge)

			// Two identical nodes: one edited in a transaction, one outside.
			viaTx, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			viaPlain, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			for _, id := range []store.NodeID{viaTx, viaPlain} {
				if err := g.IndexNodeProperties(id, map[string][]byte{"tool": []byte("acquire")}); err != nil {
					t.Fatalf("IndexNodeProperties: %v", err)
				}
			}

			tx := g.Begin()
			tx.UpdateNode(&store.Node{ID: viaTx, Labels: []store.NodeType{store.NodeTypeTag}})
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			if err := g.UpdateNode(&store.Node{ID: viaPlain, Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
				t.Fatalf("UpdateNode: %v", err)
			}

			hits, err := g.NodesByProperty("tool", []byte("acquire"))
			if err != nil {
				t.Fatalf("NodesByProperty: %v", err)
			}
			for _, id := range hits {
				if id == viaTx {
					t.Error("transactional update left a stale index entry that the plain path purges")
				}
				if id == viaPlain {
					t.Error("plain update left a stale index entry (test premise is wrong)")
				}
			}
		})
	}
}

func seedPairNoT(g *graphene.Graph) (a, b store.NodeID, e store.EdgeID) {
	a, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	b, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	e, _ = g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})
	return a, b, e
}
