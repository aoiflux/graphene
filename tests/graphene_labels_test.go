package graphene_test

// A node or edge written with no labels.
//
// The record layout permits it, and that is the whole problem: a label-less node
// indexes into no label posting, so it is invisible to NodesByType and to every
// Types-filtered query, while NodeCount still counts it. The write succeeded,
// the data is gone, and there is no error and no symptom. Worse, UpdateNode has
// always rejected the same struct — so a node that inserted cleanly could not be
// updated afterwards.
//
// These tests pin the fix: every add path refuses, on both backends, with one
// error a caller can test for.

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func TestAddNode_RejectsEmptyLabelSet(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if _, err := g.AddNode(&store.Node{}); !errors.Is(err, store.ErrNoLabels) {
			t.Fatalf("AddNode with no labels: got %v, want ErrNoLabels", err)
		}
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{}}); !errors.Is(err, store.ErrNoLabels) {
			t.Fatalf("AddNode with an empty label slice: got %v, want ErrNoLabels", err)
		}

		n, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if n != 0 {
			t.Fatalf("a refused AddNode left %d nodes behind", n)
		}
	})
}

func TestAddEdge_RejectsEmptyLabelSet(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		src, dst := twoNodes(t, g)

		if _, err := g.AddEdge(&store.Edge{Src: src, Dst: dst}); !errors.Is(err, store.ErrNoLabels) {
			t.Fatalf("AddEdge with no labels: got %v, want ErrNoLabels", err)
		}

		e, err := g.EdgeCount()
		if err != nil {
			t.Fatalf("EdgeCount: %v", err)
		}
		if e != 0 {
			t.Fatalf("a refused AddEdge left %d edges behind", e)
		}
	})
}

// A batch is refused whole, and refused before any ID is taken — the check needs
// nothing from the store, so there is no reason for it to burn a prefix.
func TestAddNodesBatch_RejectsEmptyLabelSetWithoutPartialWrite(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		batch := []*store.Node{
			{Labels: []store.NodeType{store.NodeTypeCase}},
			{}, // the offender, second, so a per-item loop would have written the first
			{Labels: []store.NodeType{store.NodeTypeCase}},
		}
		ids, err := g.AddNodes(batch)
		if !errors.Is(err, store.ErrNoLabels) {
			t.Fatalf("AddNodes with a label-less member: got %v, want ErrNoLabels", err)
		}
		if len(ids) != 0 {
			t.Fatalf("a refused batch returned %d ids", len(ids))
		}
		n, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if n != 0 {
			t.Fatalf("a refused batch wrote %d nodes", n)
		}
	})
}

func TestAddEdgesBatch_RejectsEmptyLabelSetWithoutPartialWrite(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		src, dst := twoNodes(t, g)
		batch := []*store.Edge{
			{Src: src, Dst: dst, Labels: []store.EdgeType{store.EdgeTypeContains}},
			{Src: src, Dst: dst},
		}
		ids, err := g.AddEdges(batch)
		if !errors.Is(err, store.ErrNoLabels) {
			t.Fatalf("AddEdges with a label-less member: got %v, want ErrNoLabels", err)
		}
		if len(ids) != 0 {
			t.Fatalf("a refused batch returned %d ids", len(ids))
		}
		e, err := g.EdgeCount()
		if err != nil {
			t.Fatalf("EdgeCount: %v", err)
		}
		if e != 0 {
			t.Fatalf("a refused batch wrote %d edges", e)
		}
	})
}

// The transaction resolver already checked node labels and did not check edge
// labels, so the one path that was strictest about nodes was the only path with
// no opinion at all about edges.
func TestTx_RejectsEmptyLabelSet(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		src, dst := twoNodes(t, g)

		tx := g.Begin()
		tx.AddEdge(&store.Edge{Src: src, Dst: dst})
		if err := tx.Commit(); !errors.Is(err, store.ErrNoLabels) {
			t.Fatalf("Tx.AddEdge with no labels: got %v, want ErrNoLabels", err)
		}

		tx2 := g.Begin()
		tx2.AddNode(&store.Node{})
		if err := tx2.Commit(); !errors.Is(err, store.ErrNoLabels) {
			t.Fatalf("Tx.AddNode with no labels: got %v, want ErrNoLabels", err)
		}

		e, err := g.EdgeCount()
		if err != nil {
			t.Fatalf("EdgeCount: %v", err)
		}
		if e != 0 {
			t.Fatalf("a refused transaction wrote %d edges", e)
		}
	})
}

// Endpoints are immutable, and the transactional path used to disagree with the
// plain one about that — disk ignored a changed Src/Dst while memory relocated
// the adjacency entries, so one transaction produced two different graphs.
func TestTx_UpdateEdgeCannotMoveEndpoints(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		src, dst := twoNodes(t, g)
		other, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		eid, err := g.AddEdge(&store.Edge{Src: src, Dst: dst, Labels: []store.EdgeType{store.EdgeTypeContains}})
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}

		tx := g.Begin()
		tx.UpdateEdge(&store.Edge{
			ID:     eid,
			Src:    other, // ignored
			Dst:    other, // ignored
			Labels: []store.EdgeType{store.EdgeTypeReuse},
		})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		got, err := g.GetEdge(eid)
		if err != nil {
			t.Fatalf("GetEdge: %v", err)
		}
		if got.Src != src || got.Dst != dst {
			t.Fatalf("endpoints moved: got (%d,%d), want (%d,%d)", got.Src, got.Dst, src, dst)
		}
		if !got.HasLabel(store.EdgeTypeReuse) {
			t.Fatal("the update did not take: label was not replaced")
		}

		// Adjacency must still name the edge from its original source, and must
		// not have gained an entry on the node the caller tried to move it to.
		out, err := g.EdgesOf(src, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("EdgesOf(src): %v", err)
		}
		if len(out) != 1 || out[0].ID != eid {
			t.Fatalf("source adjacency lost the edge: %d entries", len(out))
		}
		moved, err := g.EdgesOf(other, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("EdgesOf(other): %v", err)
		}
		if len(moved) != 0 {
			t.Fatalf("the edge was relocated onto %d: %d entries", other, len(moved))
		}
	})
}

// The snapshot epoch is the memory backend's version counter, and
// ApplyTransaction was the one mutator that never bumped it — so two snapshots
// straddling a committed transaction reported the same Epoch over different
// graphs, which is exactly what Epoch promises cannot happen.
func TestSnapshotEpochAdvancesAcrossATransaction(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		before, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer before.Close()

		tx := g.Begin()
		tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		after, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer after.Close()

		if after.Epoch() == before.Epoch() {
			t.Fatalf("epoch %d unchanged across a committed transaction", after.Epoch())
		}

		bn, err := before.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount(before): %v", err)
		}
		an, err := after.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount(after): %v", err)
		}
		if bn == an {
			t.Fatalf("both snapshots see %d nodes; the fixture did not change the graph", bn)
		}
	})
}

func twoNodes(t *testing.T, g *graphene.Graph) (store.NodeID, store.NodeID) {
	t.Helper()
	a, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return a, b
}
