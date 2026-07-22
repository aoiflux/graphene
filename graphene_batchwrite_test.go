package graphene_test

import (
	"os"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Batch writes on the disk backend are transactional: the whole batch is framed
// with begin/commit markers and applied only if the commit reaches the file.
//
// These test the observable contract. The rollback machinery itself is tested at
// the WAL level in disk/walbatch_test.go, including by mutation.

func diskGraphAt(t *testing.T) (*graphene.Graph, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "graphene-batchw-*")
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close(); os.RemoveAll(dir) })
	return g, dir
}

// A batch that fails endpoint validation must create nothing — and must not name
// IDs for edges that do not exist.
func TestBatchWrite_ValidationFailureCreatesNothing(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids, err := g.AddNodes([]*store.Node{
				{Labels: []store.NodeType{store.NodeTypeTag}},
				{Labels: []store.NodeType{store.NodeTypeTag}},
			})
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			before, _ := g.EdgeCount()

			// Third edge references a node that does not exist.
			got, err := g.AddEdges([]*store.Edge{
				{Src: ids[0], Dst: ids[1], Labels: []store.EdgeType{store.EdgeTypeContains}},
				{Src: ids[1], Dst: ids[0], Labels: []store.EdgeType{store.EdgeTypeContains}},
				{Src: ids[0], Dst: store.NodeID(1 << 40), Labels: []store.EdgeType{store.EdgeTypeContains}},
			})
			if err == nil {
				t.Fatal("expected ErrInvalidEdge")
			}
			if len(got) != 0 {
				t.Errorf("returned %d IDs for a batch that failed validation; want none", len(got))
			}
			after, _ := g.EdgeCount()
			if after != before {
				t.Errorf("edge count moved from %d to %d; validation failure must create nothing", before, after)
			}
		})
	}
}

// A committed batch survives a reopen intact — the commit marker is what makes
// replay apply it.
func TestBatchWrite_CommittedBatchSurvivesReopen(t *testing.T) {
	g, dir := diskGraphAt(t)

	nodes := make([]*store.Node, 500)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()

	found, missing, err := g2.GetNodes(ids)
	if err != nil {
		t.Fatalf("GetNodes: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("%d nodes missing after reopen", len(missing))
	}
	if len(found) != len(ids) {
		t.Fatalf("got %d nodes after reopen, want %d", len(found), len(ids))
	}
	if err := g2.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after reopen: %v", err)
	}
}

// Batched and per-item writes must produce the same store. A bulk path that is
// faster but different is a corruption bug wearing an optimisation's clothes.
func TestBatchWrite_MatchesPerItemWrites(t *testing.T) {
	build := func(t *testing.T, batched bool) *graphene.Graph {
		g, _ := diskGraphAt(t)
		nodes := make([]*store.Node, 200)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{benchLabelFor(i)}}
		}
		if batched {
			if _, err := g.AddNodes(nodes); err != nil {
				t.Fatal(err)
			}
			return g
		}
		for _, n := range nodes {
			if _, err := g.AddNode(n); err != nil {
				t.Fatal(err)
			}
		}
		return g
	}

	a := build(t, true)
	b := build(t, false)

	ac, _ := a.NodeCount()
	bc, _ := b.NodeCount()
	if ac != bc {
		t.Fatalf("node counts differ: batched %d, per-item %d", ac, bc)
	}
	for _, lbl := range []store.NodeType{store.NodeTypeCase, store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact} {
		x, _ := a.NodesByType(lbl)
		y, _ := b.NodesByType(lbl)
		if len(x) != len(y) {
			t.Fatalf("label %v: batched %d ids, per-item %d", lbl, len(x), len(y))
		}
	}
	if err := a.VerifyIndexes(); err != nil {
		t.Fatalf("batched store fails verification: %v", err)
	}
}

// Sync-on-commit is the default and can be turned off; both must round-trip.
func TestBatchWrite_SyncTogglesWithoutChangingResults(t *testing.T) {
	for _, sync := range []bool{true, false} {
		g, dir := diskGraphAt(t)
		if ds, ok := g.GraphStore.(interface{ SetSyncOnCommit(bool) }); ok {
			ds.SetSyncOnCommit(sync)
		} else {
			t.Fatal("disk store should expose SetSyncOnCommit")
		}
		nodes := make([]*store.Node, 100)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeTag}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			t.Fatalf("sync=%v: AddNodes: %v", sync, err)
		}
		g.Close()

		g2, err := graphene.Open(dir)
		if err != nil {
			t.Fatalf("sync=%v: reopen: %v", sync, err)
		}
		found, missing, _ := g2.GetNodes(ids)
		if len(missing) != 0 || len(found) != len(ids) {
			t.Errorf("sync=%v: %d found, %d missing after reopen", sync, len(found), len(missing))
		}
		g2.Close()
	}
}
