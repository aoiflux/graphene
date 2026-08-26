package graphene_test

// Snapshot isolation, on both backends.
//
// The property under test is narrow and easy to state: a read through a
// snapshot answers about the graph as it stood when the snapshot was taken, and
// nothing a writer does afterwards changes that answer. Every test here is a
// way of catching a read that leaked through to the live state.

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

// backends runs fn against a fresh in-memory graph and a fresh on-disk one.
// Divergence between them is a correctness bug, not a backend difference.
func backends(t *testing.T, fn func(t *testing.T, g *graphene.Graph)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		g := graphene.NewInMemory()
		defer g.Close()
		fn(t, g)
	})
	t.Run("disk", func(t *testing.T) {
		g, err := graphene.Open(t.TempDir())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer g.Close()
		fn(t, g)
	})
}

// snapChain builds a path of length nodes joined by Contains edges.
func snapChain(t *testing.T, g *graphene.Graph, length int) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, length)
	for i := 0; i < length; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if i > 0 {
			if _, err := g.AddEdge(&store.Edge{
				Src:    ids[i-1],
				Dst:    id,
				Labels: []store.EdgeType{store.EdgeTypeContains},
			}); err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
		}
		ids = append(ids, id)
	}
	return ids
}

// TestSnapshot_StableUnderConcurrentWrites is the core assertion: repeated reads
// through one snapshot are byte-identical while a writer mutates underneath.
//
// Mutation check: pin the snapshot's epoch to the store's current one — that is,
// make a snapshot read at "now" rather than at "then" — and this fails, because
// the writer's updates start showing through.
func TestSnapshot_StableUnderConcurrentWrites(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		ids := snapChain(t, g, 64)

		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer snap.Close()

		// The answer the snapshot must keep giving, recorded before any writer
		// has run.
		want := make([]*store.Node, len(ids))
		for i, id := range ids {
			n, err := snap.GetNode(id)
			if err != nil {
				t.Fatalf("snapshot GetNode(%d): %v", id, err)
			}
			want[i] = n
		}
		wantCount, err := snap.NodeCount()
		if err != nil {
			t.Fatalf("snapshot NodeCount: %v", err)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// Every kind of mutation the snapshot has to be blind to:
				// relabelling an existing node, adding new ones, and deleting.
				target := ids[i%len(ids)]
				_ = g.UpdateNode(&store.Node{
					ID:         target,
					Labels:     []store.NodeType{store.NodeTypeCase},
					Properties: []byte(fmt.Sprintf("rewritten-%d", i)),
				})
				if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}); err != nil {
					return
				}
			}
		}()

		for round := 0; round < 200; round++ {
			for i, id := range ids {
				got, err := snap.GetNode(id)
				if err != nil {
					t.Errorf("round %d: snapshot GetNode(%d): %v", round, id, err)
					break
				}
				if !reflect.DeepEqual(got, want[i]) {
					t.Errorf("round %d: snapshot node %d changed under a concurrent writer:\n want=%+v\n got= %+v",
						round, id, want[i], got)
					break
				}
			}
			got, err := snap.NodeCount()
			if err != nil {
				t.Errorf("round %d: snapshot NodeCount: %v", round, err)
				break
			}
			if got != wantCount {
				t.Errorf("round %d: snapshot NodeCount moved: want %d, got %d", round, wantCount, got)
				break
			}
			if t.Failed() {
				break
			}
		}

		close(stop)
		wg.Wait()
	})
}

// TestSnapshot_DeletedNodeStillVisibleAtOlderEpoch is what the delete masks
// could not express. A mask recorded that an ID was gone; it had no way to say
// when, so every reader saw the deletion at once.
//
// Mutation check: revert the tombstone version to a delete mask — anything that
// erases rather than stacks — and this fails immediately.
func TestSnapshot_DeletedNodeStillVisibleAtOlderEpoch(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		ids := snapChain(t, g, 8)
		victim := ids[3]

		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer snap.Close()

		if err := g.DeleteNode(victim); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		// Gone from the store...
		if _, err := g.GetNode(victim); err == nil {
			t.Fatalf("node %d is still readable from the live store after DeleteNode", victim)
		}
		// ...and still there in the snapshot, with its edges.
		if _, err := snap.GetNode(victim); err != nil {
			t.Fatalf("snapshot lost node %d, which was live when it was taken: %v", victim, err)
		}
		edges, err := snap.EdgesOf(victim, store.DirectionBoth, nil)
		if err != nil {
			t.Fatalf("snapshot EdgesOf(%d): %v", victim, err)
		}
		if len(edges) != 2 {
			t.Fatalf("snapshot lost the cascaded edges of node %d: want 2, got %d", victim, len(edges))
		}

		// And the counts must agree with the records, not split the difference.
		nc, err := snap.NodeCount()
		if err != nil {
			t.Fatalf("snapshot NodeCount: %v", err)
		}
		if nc != uint64(len(ids)) {
			t.Fatalf("snapshot NodeCount followed the delete: want %d, got %d", len(ids), nc)
		}
	})
}

// TestSnapshot_BFSIsConsistent is the spec's inconsistent_bfs_paths.
//
// A traversal is many reads, so on a live store a concurrent delete can leave it
// holding an edge to a node that no longer exists. Run the same walk against a
// snapshot and every edge it crosses must still lead somewhere.
//
// Mutation check: pass g instead of snap and this starts failing under load.
func TestSnapshot_BFSIsConsistent(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		ids := snapChain(t, g, 200)

		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer snap.Close()

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < len(ids); i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = g.DeleteNode(ids[i])
			}
		}()

		for round := 0; round < 50; round++ {
			// A Snapshot is a store.GraphReader, so this is the same call the
			// live store would take.
			res, err := traversal.BFS(snap, ids[0], len(ids), store.DirectionBoth, nil)
			if err != nil {
				t.Errorf("round %d: BFS over snapshot: %v", round, err)
				break
			}
			if len(res.Nodes) != len(ids) {
				t.Errorf("round %d: BFS over snapshot reached %d nodes, want %d", round, len(res.Nodes), len(ids))
				break
			}
			// Every edge must connect two nodes the walk actually returned.
			present := make(map[store.NodeID]struct{}, len(res.Nodes))
			for _, n := range res.Nodes {
				present[n.ID] = struct{}{}
			}
			for _, e := range res.Edges {
				if _, ok := present[e.Src]; !ok {
					t.Errorf("round %d: edge %d points at missing Src %d", round, e.ID, e.Src)
					break
				}
				if _, ok := present[e.Dst]; !ok {
					t.Errorf("round %d: edge %d points at missing Dst %d", round, e.ID, e.Dst)
					break
				}
			}
			if t.Failed() {
				break
			}
		}

		close(stop)
		wg.Wait()
	})
}

// TestSnapshot_SurvivesCompaction is the disk backend's own hazard: compaction
// replaces the image *and* the delta layer beneath every open reader.
//
// Mutation check: let Compact clear the delta layer in place rather than publish
// a fresh one and this fails — the snapshot's records go missing under it.
func TestSnapshot_SurvivesCompaction(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	ids := snapChain(t, g, 128)
	for _, id := range ids {
		if err := g.IndexNodeProperty(id, "bucket", []byte("alpha")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}

	snap, err := g.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Close()

	before, err := snap.NodeCount()
	if err != nil {
		t.Fatalf("snapshot NodeCount: %v", err)
	}

	for round := 0; round < 5; round++ {
		// Write and compact, repeatedly, under the open snapshot.
		snapChain(t, g, 16)
		if err := g.Compact(); err != nil {
			t.Fatalf("round %d: Compact: %v", round, err)
		}

		if got, err := snap.NodeCount(); err != nil || got != before {
			t.Fatalf("round %d: snapshot NodeCount after compaction: want %d, got %d (err %v)",
				round, before, got, err)
		}
		for _, id := range ids {
			if _, err := snap.GetNode(id); err != nil {
				t.Fatalf("round %d: snapshot lost node %d across compaction: %v", round, id, err)
			}
		}
		res, err := traversal.BFS(snap, ids[0], len(ids), store.DirectionBoth, nil)
		if err != nil {
			t.Fatalf("round %d: BFS over snapshot: %v", round, err)
		}
		if len(res.Nodes) != len(ids) {
			t.Fatalf("round %d: BFS over snapshot reached %d nodes, want %d", round, len(res.Nodes), len(ids))
		}
	}
}

// TestSnapshot_EpochAndClose covers the contract around the handle itself.
func TestSnapshot_EpochAndClose(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		snapChain(t, g, 4)

		a, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		b, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		// Two snapshots with no write between them name the same graph.
		if a.Epoch() != b.Epoch() {
			t.Fatalf("two snapshots taken with no write between them disagree: %d vs %d", a.Epoch(), b.Epoch())
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		c, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer c.Close()
		if c.Epoch() <= a.Epoch() {
			t.Fatalf("epoch did not advance across a write: %d then %d", a.Epoch(), c.Epoch())
		}

		// Close is idempotent, and reads after it are refused rather than
		// answered from whatever state happens to still be reachable.
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := a.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if _, err := a.GetNode(1); err == nil {
			t.Fatal("a closed snapshot still answered GetNode")
		}
	})
}

// TestSnapshot_ParityMemoryVsDisk checks the two backends answer a snapshot the
// same way, which is the standing rule for anything both implement.
func TestSnapshot_ParityMemoryVsDisk(t *testing.T) {
	type answer struct {
		Nodes    uint64
		Edges    uint64
		ByType   []store.NodeID
		ByProp   []store.NodeID
		Query    []store.NodeID
		Outbound int
	}

	run := func(g *graphene.Graph) (answer, error) {
		var a answer
		ids := make([]store.NodeID, 0, 32)
		for i := 0; i < 32; i++ {
			id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err != nil {
				return a, err
			}
			if err := g.IndexNodeProperty(id, "bucket", []byte("alpha")); err != nil {
				return a, err
			}
			if i > 0 {
				if _, err := g.AddEdge(&store.Edge{
					Src:    ids[i-1],
					Dst:    id,
					Labels: []store.EdgeType{store.EdgeTypeContains},
				}); err != nil {
					return a, err
				}
			}
			ids = append(ids, id)
		}

		snap, err := g.Snapshot()
		if err != nil {
			return a, err
		}
		defer snap.Close()

		// Mutate afterwards. None of it may reach the answers below.
		for _, id := range ids[:8] {
			if err := g.DeleteNode(id); err != nil {
				return a, err
			}
		}

		if a.Nodes, err = snap.NodeCount(); err != nil {
			return a, err
		}
		if a.Edges, err = snap.EdgeCount(); err != nil {
			return a, err
		}
		if a.ByType, err = snap.NodesByType(store.NodeTypeMicroArtefact); err != nil {
			return a, err
		}
		if a.ByProp, err = snap.NodesByProperty("bucket", []byte("alpha")); err != nil {
			return a, err
		}
		if a.Query, err = snap.QueryNodeIDs(store.NodeQuery{
			Types: []store.NodeType{store.NodeTypeMicroArtefact},
			Order: store.QueryOrderAsc,
		}); err != nil {
			return a, err
		}
		out, err := snap.EdgesOf(ids[10], store.DirectionOutbound, nil)
		if err != nil {
			return a, err
		}
		a.Outbound = len(out)
		return a, nil
	}

	mem := graphene.NewInMemory()
	defer mem.Close()
	want, err := run(mem)
	if err != nil {
		t.Fatalf("memory: %v", err)
	}

	dsk, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dsk.Close()
	got, err := run(dsk)
	if err != nil {
		t.Fatalf("disk: %v", err)
	}

	if !reflect.DeepEqual(want, got) {
		t.Fatalf("snapshot answers diverged between backends:\n memory=%+v\n disk=  %+v", want, got)
	}
}

// TestSnapshot_ReleasesRetainedVersions checks the bookkeeping an operator sees,
// and with it that closing a snapshot actually lets history go.
func TestSnapshot_ReleasesRetainedVersions(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	snapChain(t, g, 16)

	stats, ok := g.StorageStats()
	if !ok {
		t.Fatal("disk backend does not report StorageStats")
	}
	if stats.OpenSnapshots != 0 {
		t.Fatalf("a fresh store reports %d open snapshots", stats.OpenSnapshots)
	}

	snap, err := g.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	stats, _ = g.StorageStats()
	if stats.OpenSnapshots != 1 {
		t.Fatalf("open snapshots: want 1, got %d", stats.OpenSnapshots)
	}
	if stats.OldestSnapshotEpoch != snap.Epoch() {
		t.Fatalf("oldest snapshot epoch: want %d, got %d", snap.Epoch(), stats.OldestSnapshotEpoch)
	}
	if stats.VisibleEpoch < snap.Epoch() {
		t.Fatalf("visible epoch %d is behind a snapshot taken from it (%d)", stats.VisibleEpoch, snap.Epoch())
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	stats, _ = g.StorageStats()
	if stats.OpenSnapshots != 0 {
		t.Fatalf("open snapshots after Close: want 0, got %d", stats.OpenSnapshots)
	}
	if stats.OldestSnapshotEpoch != 0 {
		t.Fatalf("oldest snapshot epoch after Close: want 0, got %d", stats.OldestSnapshotEpoch)
	}

	// And the store must still verify: the postings and adjacency left behind
	// while the snapshot was open are supersets, not corruption.
	if err := g.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after a snapshot came and went: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after compaction: %v", err)
	}
}
