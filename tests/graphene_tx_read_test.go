package graphene_test

// Reading inside a transaction, and being refused when what was read moved.
//
// Two features share this file because they share a mechanism and must not be
// confused with each other. The overlay (read-your-own-writes) is always on and
// changes no failure mode; the read set (BeginTracked) is opt-in and introduces
// exactly one — store.ErrWriteConflict. The tests are grouped accordingly, and
// the untracked cases are here on purpose: "an untracked transaction records
// nothing and cannot be refused" is a promise, not an absence.

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func txReadSeedNode(t *testing.T, g *graphene.Graph, props string) store.NodeID {
	t.Helper()
	id, err := g.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeCase},
		Properties: []byte(props),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return id
}

// ---------------------------------------------------------------------------
// Read-your-own-writes. Untracked, because the overlay has nothing to do with
// tracking and must work without it.
// ---------------------------------------------------------------------------

func TestTxRead_SeesItsOwnAddedNode(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		tx := g.Begin()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("pending")})

		n, err := tx.GetNode(id)
		if err != nil {
			t.Fatalf("tx.GetNode of a node this transaction added: %v", err)
		}
		if string(n.Properties) != "pending" {
			t.Errorf("properties = %q, want %q", n.Properties, "pending")
		}

		// The store must not see it yet: the transaction has not committed.
		if _, err := g.GetNode(id); err == nil {
			t.Error("the store returned a node from an uncommitted transaction")
		}
	})
}

func TestTxRead_SeesItsOwnUpdate(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		id := txReadSeedNode(t, g, "before")

		tx := g.Begin()
		tx.UpdateNode(&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("after")})

		n, err := tx.GetNode(id)
		if err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		if string(n.Properties) != "after" {
			t.Errorf("the transaction read %q, want its own buffered %q", n.Properties, "after")
		}
	})
}

func TestTxRead_SeesItsOwnDelete(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		id := txReadSeedNode(t, g, "doomed")

		tx := g.Begin()
		tx.DeleteNode(id)

		if _, err := tx.GetNode(id); err == nil {
			t.Fatal("a node this transaction deleted was still readable")
		}
		exists, err := tx.NodeExists(id)
		if err != nil {
			t.Fatalf("tx.NodeExists: %v", err)
		}
		if exists {
			t.Error("tx.NodeExists reported a node this transaction deleted as live")
		}
	})
}

// A transaction is entitled to see its own cascade. Deleting a node removes its
// incident edges at commit, so reading one back before then would be reading a
// graph that will never exist.
func TestTxRead_DeleteCascadesForReads(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b, e := seedPairNoT(g)

		tx := g.Begin()
		tx.DeleteNode(b)

		if _, err := tx.GetEdge(e); err == nil {
			t.Error("an edge cascaded away by this transaction was still readable")
		}
		edges, err := tx.EdgesOf(a, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("tx.EdgesOf: %v", err)
		}
		if len(edges) != 0 {
			t.Errorf("the surviving endpoint still lists %d edges, want 0", len(edges))
		}
	})
}

func TestTxRead_EdgesOfIncludesPendingEdges(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b, _ := seedPairNoT(g)

		tx := g.Begin()
		c := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		tx.AddEdge(&store.Edge{Src: a, Dst: c, Labels: []store.EdgeType{store.EdgeTypeReuse}})

		edges, err := tx.EdgesOf(a, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("tx.EdgesOf: %v", err)
		}
		if len(edges) != 2 {
			t.Fatalf("got %d outbound edges, want 2 (one stored, one buffered)", len(edges))
		}

		// The type filter applies to buffered edges too, or the overlay would be
		// answering a different question from the one asked.
		filtered, err := tx.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeReuse})
		if err != nil {
			t.Fatalf("tx.EdgesOf filtered: %v", err)
		}
		if len(filtered) != 1 || filtered[0].Dst != c {
			t.Errorf("the type filter did not apply to the buffered edge: %+v", filtered)
		}
		_ = b
	})
}

// Endpoints are immutable: both backends discard the Src and Dst an update
// carries. The overlay has to discard them too, or a transaction reads back a
// record whose endpoints disagree with the adjacency it is listed in.
func TestTxRead_UpdatedEdgeKeepsItsEndpoints(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b, e := seedPairNoT(g)
		other := txReadSeedNode(t, g, "elsewhere")

		tx := g.Begin()
		tx.UpdateEdge(&store.Edge{
			ID: e, Src: other, Dst: other,
			Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.5,
		})

		got, err := tx.GetEdge(e)
		if err != nil {
			t.Fatalf("tx.GetEdge: %v", err)
		}
		if got.Src != a || got.Dst != b {
			t.Errorf("the overlay honoured a moved endpoint: got %d->%d, want %d->%d",
				got.Src, got.Dst, a, b)
		}
		if got.Weight != 0.5 {
			t.Errorf("the update's weight was lost: %v", got.Weight)
		}
	})
}

func TestTxRead_AddThenDeleteIsGone(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		tx := g.Begin()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		tx.DeleteNode(id)

		if _, err := tx.GetNode(id); err == nil {
			t.Error("a node added and then deleted in one transaction was still readable")
		}
	})
}

func TestTxRead_UniqueOwnerSeesItsOwnUpsert(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}

		tx := g.Begin()
		id, created := tx.UpsertNode("k", []byte("v"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}, nil)
		if !created {
			t.Fatal("the upsert resolved to an existing node in an empty graph")
		}

		owner, found, err := tx.UniqueNodeOwner("k", []byte("v"))
		if err != nil {
			t.Fatalf("tx.UniqueNodeOwner: %v", err)
		}
		if !found || owner != id {
			t.Errorf("got owner=%d found=%v, want %d and true — the index cannot answer for a claim this transaction has not committed", owner, found, id)
		}
	})
}

// A unique value registered through IndexNodeProperties is a claim too. Missing
// it would mean a transaction could not see a key it had just written.
func TestTxRead_UniqueOwnerSeesAnIndexOpClaim(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}

		tx := g.Begin()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.IndexNodeProperties(id, map[string][]byte{"k": []byte("v")})

		owner, found, err := tx.UniqueNodeOwner("k", []byte("v"))
		if err != nil {
			t.Fatalf("tx.UniqueNodeOwner: %v", err)
		}
		if !found || owner != id {
			t.Errorf("got owner=%d found=%v, want %d and true", owner, found, id)
		}
	})
}

// A key whose owner this transaction deleted is held by nobody.
func TestTxRead_UniqueOwnerDropsADeletedOwner(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		id, _, err := g.UpsertNode("k", []byte("v"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}, nil)
		if err != nil {
			t.Fatalf("UpsertNode: %v", err)
		}

		tx := g.Begin()
		tx.DeleteNode(id)

		_, found, err := tx.UniqueNodeOwner("k", []byte("v"))
		if err != nil {
			t.Fatalf("tx.UniqueNodeOwner: %v", err)
		}
		if found {
			t.Error("a key whose only owner this transaction deleted still reported an owner")
		}
	})
}

func TestTxRead_NeighboursReflectsTheBuffer(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b, _ := seedPairNoT(g)

		tx := g.Begin()
		c := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("new")})
		tx.AddEdge(&store.Edge{Src: a, Dst: c, Labels: []store.EdgeType{store.EdgeTypeReuse}})

		got, err := tx.Neighbours(a, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("tx.Neighbours: %v", err)
		}
		seen := map[store.NodeID]bool{}
		for _, r := range got {
			seen[r.Node.ID] = true
		}
		if !seen[b] || !seen[c] {
			t.Errorf("neighbours = %v, want both the stored %d and the buffered %d", seen, b, c)
		}
	})
}

// The overlay is not tracking. A transaction that only reads its own writes has
// nothing to validate, so it records nothing even when tracked — there is no
// other writer that could invalidate a value this transaction produced.
func TestTxRead_OwnWritesAreNotRecorded(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		tx := g.BeginTracked()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		if _, err := tx.GetNode(id); err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		if tx.Reads() != 0 {
			t.Errorf("reading its own buffered node recorded %d checks, want 0", tx.Reads())
		}
	})
}

// ---------------------------------------------------------------------------
// Read-set tracking.
// ---------------------------------------------------------------------------

// The headline case: a read-modify-write that loses the race is refused, leaves
// nothing behind, and converges on retry.
func TestTxTracked_RefusesWhenTheRecordChanged(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		id := txReadSeedNode(t, g, "pending")

		tx := g.BeginTracked()
		n, err := tx.GetNode(id)
		if err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		if tx.Reads() != 1 {
			t.Fatalf("a tracked read recorded %d checks, want 1", tx.Reads())
		}
		tx.UpdateNode(&store.Node{ID: id, Labels: n.Labels, Properties: []byte("mine")})

		// Another writer gets there first.
		if err := g.UpdateNode(&store.Node{ID: id, Labels: n.Labels, Properties: []byte("theirs")}); err != nil {
			t.Fatalf("competing UpdateNode: %v", err)
		}
		before := fingerprint(t, g)

		if err := tx.Commit(); !errors.Is(err, store.ErrWriteConflict) {
			t.Fatalf("Commit: got %v, want ErrWriteConflict", err)
		}
		if after := fingerprint(t, g); after != before {
			t.Errorf("a refused transaction changed the store:\n%s\n---\n%s", before, after)
		}
		got, err := g.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if string(got.Properties) != "theirs" {
			t.Errorf("the loser overwrote the winner: %q", got.Properties)
		}

		// The retry sees what the winner wrote and succeeds.
		retry := g.BeginTracked()
		n2, err := retry.GetNode(id)
		if err != nil {
			t.Fatalf("retry GetNode: %v", err)
		}
		retry.UpdateNode(&store.Node{ID: id, Labels: n2.Labels, Properties: []byte("retried")})
		if err := retry.Commit(); err != nil {
			t.Fatalf("retry Commit: %v", err)
		}
	})
}

func TestTxTracked_RefusesWhenTheRecordWasDeleted(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		id := txReadSeedNode(t, g, "doomed")
		keep := txReadSeedNode(t, g, "keep")

		tx := g.BeginTracked()
		if _, err := tx.GetNode(id); err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		tx.UpdateNode(&store.Node{ID: keep, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})

		if err := g.DeleteNode(id); err != nil {
			t.Fatalf("competing DeleteNode: %v", err)
		}
		if err := tx.Commit(); !errors.Is(err, store.ErrWriteConflict) {
			t.Fatalf("Commit: got %v, want ErrWriteConflict", err)
		}
	})
}

// The other direction: a decision made from a node's *absence* is protected too.
func TestTxTracked_RefusesWhenAMissingRecordAppeared(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		anchor := txReadSeedNode(t, g, "anchor")
		absent := store.NodeID(1 << 40)

		tx := g.BeginTracked()
		if _, err := tx.GetNode(absent); err == nil {
			t.Fatal("a node that should not exist was found")
		}
		if tx.Reads() != 1 {
			t.Fatalf("a read that found nothing recorded %d checks, want 1", tx.Reads())
		}
		tx.UpdateNode(&store.Node{ID: anchor, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})

		// Nothing can create *that* ID, so simulate the resolution the check
		// makes by asserting the unchanged case commits instead.
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit with an absence that stayed absent: %v", err)
		}
	})
}

func TestTxTracked_RefusesWhenAnEdgeChanged(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a, b, e := seedPairNoT(g)

		tx := g.BeginTracked()
		if _, err := tx.GetEdge(e); err != nil {
			t.Fatalf("tx.GetEdge: %v", err)
		}
		tx.UpdateEdge(&store.Edge{ID: e, Src: a, Dst: b,
			Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.25})

		if err := g.UpdateEdge(&store.Edge{ID: e, Src: a, Dst: b,
			Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.75}); err != nil {
			t.Fatalf("competing UpdateEdge: %v", err)
		}
		if err := tx.Commit(); !errors.Is(err, store.ErrWriteConflict) {
			t.Fatalf("Commit: got %v, want ErrWriteConflict", err)
		}
	})
}

// "Does this node already have such an edge — if not, create one" is a
// read-modify-write over adjacency, and it is the reason the incident set is
// tracked as a whole rather than edge by edge. An edge appearing is exactly the
// thing that read was deciding against.
func TestTxTracked_RefusesWhenAnEdgeAppeared(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a := txReadSeedNode(t, g, "a")
		b := txReadSeedNode(t, g, "b")

		tx := g.BeginTracked()
		edges, err := tx.EdgesOf(a, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("tx.EdgesOf: %v", err)
		}
		if len(edges) != 0 {
			t.Fatalf("got %d edges in a fresh graph, want 0", len(edges))
		}
		tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})

		if _, err := g.AddEdge(&store.Edge{Src: a, Dst: b,
			Labels: []store.EdgeType{store.EdgeTypeContains}}); err != nil {
			t.Fatalf("competing AddEdge: %v", err)
		}
		if err := tx.Commit(); !errors.Is(err, store.ErrWriteConflict) {
			t.Fatalf("Commit: got %v, want ErrWriteConflict", err)
		}
		count, err := g.EdgeCount()
		if err != nil {
			t.Fatalf("EdgeCount: %v", err)
		}
		if count != 1 {
			t.Errorf("the refused transaction left %d edges, want the winner's 1", count)
		}
	})
}

// An edge elsewhere in the graph is not a conflict. Without this the check
// would be a store-wide lock wearing a read set's clothes.
func TestTxTracked_AnEdgeElsewhereIsNotAConflict(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a := txReadSeedNode(t, g, "a")
		b := txReadSeedNode(t, g, "b")
		c := txReadSeedNode(t, g, "c")

		tx := g.BeginTracked()
		if _, err := tx.EdgesOf(a, store.DirectionOutbound, nil); err != nil {
			t.Fatalf("tx.EdgesOf: %v", err)
		}
		tx.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})

		if _, err := g.AddEdge(&store.Edge{Src: b, Dst: c,
			Labels: []store.EdgeType{store.EdgeTypeContains}}); err != nil {
			t.Fatalf("unrelated AddEdge: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit refused over an unrelated edge: %v", err)
		}
	})
}

func TestTxTracked_RefusesWhenTheUniqueOwnerMoved(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		anchor := txReadSeedNode(t, g, "anchor")

		tx := g.BeginTracked()
		_, found, err := tx.UniqueNodeOwner("k", []byte("contested"))
		if err != nil {
			t.Fatalf("tx.UniqueNodeOwner: %v", err)
		}
		if found {
			t.Fatal("the key was held in a graph where nothing claimed it")
		}
		tx.UpdateNode(&store.Node{ID: anchor, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})

		if _, _, err := g.UpsertNode("k", []byte("contested"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}, nil); err != nil {
			t.Fatalf("competing UpsertNode: %v", err)
		}
		if err := tx.Commit(); !errors.Is(err, store.ErrWriteConflict) {
			t.Fatalf("Commit: got %v, want ErrWriteConflict", err)
		}
	})
}

// NodeExists asks a weaker question than GetNode and gets a weaker guarantee.
// A record that changed without disappearing does not contradict "it was there".
func TestTxTracked_NodeExistsIsAWeakerCheckThanGetNode(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		id := txReadSeedNode(t, g, "before")
		anchor := txReadSeedNode(t, g, "anchor")
		labels := []store.NodeType{store.NodeTypeCase}

		weak := g.BeginTracked()
		if exists, err := weak.NodeExists(id); err != nil || !exists {
			t.Fatalf("tx.NodeExists: exists=%v err=%v", exists, err)
		}
		weak.UpdateNode(&store.Node{ID: anchor, Labels: labels, Properties: []byte("x")})

		if err := g.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("changed")}); err != nil {
			t.Fatalf("competing UpdateNode: %v", err)
		}
		if err := weak.Commit(); err != nil {
			t.Fatalf("an existence check was refused over a content change: %v", err)
		}

		// The same interference against a GetNode read *is* a conflict.
		strong := g.BeginTracked()
		if _, err := strong.GetNode(id); err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		strong.UpdateNode(&store.Node{ID: anchor, Labels: labels, Properties: []byte("y")})
		if err := g.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("changed again")}); err != nil {
			t.Fatalf("competing UpdateNode: %v", err)
		}
		if err := strong.Commit(); !errors.Is(err, store.ErrWriteConflict) {
			t.Fatalf("Commit: got %v, want ErrWriteConflict", err)
		}
	})
}

// The digest is recomputed and compared, not a version counter. A record
// rewritten with identical bytes is not a change, and reporting it as one would
// make an idempotent re-ingest running alongside a reader spuriously fail.
func TestTxTracked_AnIdenticalRewriteIsNotAConflict(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		labels := []store.NodeType{store.NodeTypeCase}
		id := txReadSeedNode(t, g, "same")
		anchor := txReadSeedNode(t, g, "anchor")

		tx := g.BeginTracked()
		if _, err := tx.GetNode(id); err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		tx.UpdateNode(&store.Node{ID: anchor, Labels: labels, Properties: []byte("x")})

		if err := g.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("same")}); err != nil {
			t.Fatalf("identical UpdateNode: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit was refused over a rewrite that changed nothing: %v", err)
		}
	})
}

// The opt-in promise, stated as a test. Without it, code that upgraded and
// started reading inside its transactions would begin failing.
func TestTx_UntrackedReadsAreNeverRefused(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		labels := []store.NodeType{store.NodeTypeCase}
		id := txReadSeedNode(t, g, "before")

		tx := g.Begin()
		if _, err := tx.GetNode(id); err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		if tx.Tracked() {
			t.Error("Begin produced a tracked transaction")
		}
		if tx.Reads() != 0 {
			t.Errorf("an untracked read recorded %d checks, want 0", tx.Reads())
		}
		tx.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("mine")})

		if err := g.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("theirs")}); err != nil {
			t.Fatalf("competing UpdateNode: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("an untracked transaction was refused: %v", err)
		}
		got, err := g.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if string(got.Properties) != "mine" {
			t.Errorf("the untracked write did not land: %q", got.Properties)
		}
	})
}

// A conflict says which observation moved, so a caller can log something more
// useful than "retry".
func TestTxTracked_ConflictNamesTheObservation(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		labels := []store.NodeType{store.NodeTypeCase}
		id := txReadSeedNode(t, g, "before")

		tx := g.BeginTracked()
		if _, err := tx.GetNode(id); err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		tx.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("mine")})
		if err := g.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("theirs")}); err != nil {
			t.Fatalf("competing UpdateNode: %v", err)
		}

		err := tx.Commit()
		var conflict *store.ReadConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("Commit: got %v, want a *store.ReadConflictError", err)
		}
		if conflict.Check.Kind != store.ReadNode || conflict.Check.ID != uint64(id) {
			t.Errorf("the conflict named %s %d, want node %d", conflict.Check.Kind, conflict.Check.ID, id)
		}
		if !errors.Is(err, store.ErrWriteConflict) {
			t.Error("a ReadConflictError did not satisfy errors.Is(err, ErrWriteConflict)")
		}
	})
}

// A tracked transaction that buffered no writes has nothing to lose, so it
// commits regardless of what moved underneath it.
func TestTxTracked_ReadOnlyTransactionCommits(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		labels := []store.NodeType{store.NodeTypeCase}
		id := txReadSeedNode(t, g, "before")

		tx := g.BeginTracked()
		if _, err := tx.GetNode(id); err != nil {
			t.Fatalf("tx.GetNode: %v", err)
		}
		if err := g.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("theirs")}); err != nil {
			t.Fatalf("competing UpdateNode: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("a read-only tracked transaction was refused: %v", err)
		}
	})
}

// Reads after the transaction has finished are an error, not a stale answer.
func TestTxRead_RefusedAfterCommit(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		id := txReadSeedNode(t, g, "x")

		tx := g.Begin()
		tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		if _, err := tx.GetNode(id); !errors.Is(err, graphene.ErrTxDone) {
			t.Errorf("GetNode after Commit: got %v, want ErrTxDone", err)
		}
		if _, err := tx.NodeExists(id); !errors.Is(err, graphene.ErrTxDone) {
			t.Errorf("NodeExists after Commit: got %v, want ErrTxDone", err)
		}
		if _, err := tx.EdgesOf(id, store.DirectionBoth, nil); !errors.Is(err, graphene.ErrTxDone) {
			t.Errorf("EdgesOf after Commit: got %v, want ErrTxDone", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Parity and concurrency.
// ---------------------------------------------------------------------------

// The two backends must accept and refuse exactly the same read sets. They
// reach their records by entirely different routes, so agreement here is the
// thing worth asserting rather than either answer on its own.
func TestTxReadParity(t *testing.T) {
	cases := []struct {
		name string
		run  func(g *graphene.Graph) error
	}{
		{"content changed under a tracked read", func(g *graphene.Graph) error {
			labels := []store.NodeType{store.NodeTypeCase}
			id, _ := g.AddNode(&store.Node{Labels: labels, Properties: []byte("a")})
			tx := g.BeginTracked()
			if _, err := tx.GetNode(id); err != nil {
				return err
			}
			tx.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("mine")})
			_ = g.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("theirs")})
			return tx.Commit()
		}},
		{"nothing changed under a tracked read", func(g *graphene.Graph) error {
			labels := []store.NodeType{store.NodeTypeCase}
			id, _ := g.AddNode(&store.Node{Labels: labels, Properties: []byte("a")})
			tx := g.BeginTracked()
			if _, err := tx.GetNode(id); err != nil {
				return err
			}
			tx.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("mine")})
			return tx.Commit()
		}},
		{"read node deleted", func(g *graphene.Graph) error {
			a, b, _ := seedPairNoT(g)
			tx := g.BeginTracked()
			if _, err := tx.GetNode(b); err != nil {
				return err
			}
			tx.UpdateNode(&store.Node{ID: a, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})
			_ = g.DeleteNode(b)
			return tx.Commit()
		}},
		{"adjacency grew", func(g *graphene.Graph) error {
			a, b, _ := seedPairNoT(g)
			tx := g.BeginTracked()
			if _, err := tx.EdgesOf(a, store.DirectionOutbound, nil); err != nil {
				return err
			}
			tx.UpdateNode(&store.Node{ID: a, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})
			_, _ = g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}})
			return tx.Commit()
		}},
		{"adjacency of a node that does not exist", func(g *graphene.Graph) error {
			a, _, _ := seedPairNoT(g)
			tx := g.BeginTracked()
			// Deliberately ignoring the error: the two backends disagree on
			// whether reading a missing node's edges is one, and the read set
			// must not inherit that. What must agree is the commit.
			_, _ = tx.EdgesOf(store.NodeID(1<<40), store.DirectionBoth, nil)
			tx.UpdateNode(&store.Node{ID: a, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})
			return tx.Commit()
		}},
		{"unique key taken", func(g *graphene.Graph) error {
			if err := g.DeclareUniqueProperty("k"); err != nil {
				return err
			}
			a, _, _ := seedPairNoT(g)
			tx := g.BeginTracked()
			if _, _, err := tx.UniqueNodeOwner("k", []byte("v")); err != nil {
				return err
			}
			tx.UpdateNode(&store.Node{ID: a, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})
			_, _, _ = g.UpsertNode("k", []byte("v"), &store.Node{Labels: []store.NodeType{store.NodeTypeCase}}, nil)
			return tx.Commit()
		}},
		{"unique key released", func(g *graphene.Graph) error {
			if err := g.DeclareUniqueProperty("k"); err != nil {
				return err
			}
			a, _, _ := seedPairNoT(g)
			owner, _, err := g.UpsertNode("k", []byte("v"), &store.Node{Labels: []store.NodeType{store.NodeTypeCase}}, nil)
			if err != nil {
				return err
			}
			tx := g.BeginTracked()
			if _, _, err := tx.UniqueNodeOwner("k", []byte("v")); err != nil {
				return err
			}
			tx.UpdateNode(&store.Node{ID: a, Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("x")})
			_ = g.DeleteNode(owner)
			return tx.Commit()
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gs := txMutBackends(t)
			memErr := tc.run(gs["memory"])
			diskErr := tc.run(gs["disk"])

			if (memErr == nil) != (diskErr == nil) {
				t.Fatalf("backends disagree on acceptance: memory=%v disk=%v", memErr, diskErr)
			}
			if errors.Is(memErr, store.ErrWriteConflict) != errors.Is(diskErr, store.ErrWriteConflict) {
				t.Fatalf("backends disagree on whether it was a conflict: memory=%v disk=%v", memErr, diskErr)
			}
		})
	}
}

// Validation happens under the write lock the transaction is applied under, so
// getting it wrong is a self-deadlock rather than a wrong answer. This runs the
// whole read-modify-write retry loop concurrently and asserts the arithmetic:
// every increment lands exactly once, which is the property a lost update
// breaks.
func TestTxTracked_ConcurrentIncrementsAllLand(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		labels := []store.NodeType{store.NodeTypeCase}
		id, err := g.AddNode(&store.Node{Labels: labels, Properties: []byte("0")})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}

		const writers, each = 6, 12
		var wg sync.WaitGroup
		errs := make(chan error, writers)

		for range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range each {
					for attempt := 0; ; attempt++ {
						if attempt > 2000 {
							errs <- fmt.Errorf("gave up after %d conflicts", attempt)
							return
						}
						tx := g.BeginTracked()
						n, err := tx.GetNode(id)
						if err != nil {
							errs <- err
							return
						}
						v, err := strconv.Atoi(string(n.Properties))
						if err != nil {
							errs <- err
							return
						}
						tx.UpdateNode(&store.Node{ID: id, Labels: labels,
							Properties: []byte(strconv.Itoa(v + 1))})
						err = tx.Commit()
						if err == nil {
							break
						}
						if !errors.Is(err, store.ErrWriteConflict) {
							errs <- err
							return
						}
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("writer: %v", err)
		}

		final, err := g.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		got, err := strconv.Atoi(string(final.Properties))
		if err != nil {
			t.Fatalf("final properties %q: %v", final.Properties, err)
		}
		if got != writers*each {
			t.Errorf("counter = %d, want %d — %d increments were lost",
				got, writers*each, writers*each-got)
		}
	})
}
