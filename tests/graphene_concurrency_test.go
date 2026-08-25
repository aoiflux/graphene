package graphene_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// The disk backend serves point reads from the CSR without taking the store
// lock, which is safe only because a published CSRGraph is immutable and because
// a shadow counter disables the path the moment an update or tombstone
// supersedes any CSR record.
//
// The race detector cannot validate that reasoning — it finds unsynchronised
// memory access, not stale answers. These tests attack the logic instead: they
// run readers against a store being mutated underneath them and assert the
// answers stay coherent.

// A reader must never see a node come back to life. Once any goroutine observes
// a node as deleted, no goroutine may subsequently observe it as present — that
// is exactly the failure a stale lock-free read would produce.
func TestConcurrent_DeletedNodeNeverReappears(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	const n = 2_000
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	// Compact so the records live in the CSR and the fast path is eligible.
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// deadAt[i] is set once any reader has seen ids[i] deleted.
	deadAt := make([]atomic.Bool, n)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var violations atomic.Int64

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				i = (i + 7) % n
				_, err := g.GetNode(ids[i])
				if err != nil {
					deadAt[i].Store(true)
					continue
				}
				if deadAt[i].Load() {
					// Seen alive after having been seen dead.
					violations.Add(1)
				}
			}
		}(r * 131)
	}

	// Delete every third node while the readers run.
	for i := 0; i < n; i += 3 {
		if err := g.DeleteNode(ids[i]); err != nil {
			t.Errorf("DeleteNode: %v", err)
			break
		}
	}
	close(stop)
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("%d stale reads: a deleted node was observed alive afterwards", v)
	}

	// And the final state must be exactly right.
	for i := 0; i < n; i++ {
		_, err := g.GetNode(ids[i])
		wantDeleted := i%3 == 0
		if wantDeleted && err == nil {
			t.Fatalf("node %d should be deleted", ids[i])
		}
		if !wantDeleted && err != nil {
			t.Fatalf("node %d should be alive: %v", ids[i], err)
		}
	}
	if err := g.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
}

// An updated node must never be observed with its pre-update labels once its
// post-update labels have been seen.
func TestConcurrent_UpdatedNodeNeverRevertsForReaders(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	const n = 1_000
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	updated := make([]atomic.Bool, n)
	var violations atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				i = (i + 11) % n
				node, err := g.GetNode(ids[i])
				if err != nil {
					continue
				}
				if node.HasLabel(store.NodeTypeTag) {
					updated[i].Store(true)
					continue
				}
				if updated[i].Load() {
					// Reverted to the pre-update labels.
					violations.Add(1)
				}
			}
		}(r * 97)
	}

	for i := 0; i < n; i++ {
		if err := g.UpdateNode(&store.Node{ID: ids[i], Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
			t.Errorf("UpdateNode: %v", err)
			break
		}
	}
	close(stop)
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("%d stale reads: an updated node was observed with its old labels afterwards", v)
	}
}

// Compaction swaps the published CSR and resets the shadow counter. A reader
// mid-flight must not be fooled into trusting a CSR that has just been replaced,
// which is what the epoch check exists to prevent.
func TestConcurrent_ReadsDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	const n = 1_500
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Delete a known set, then compact repeatedly while readers run. A deleted
	// node must never be readable, before or after any compaction.
	deleted := make(map[store.NodeID]struct{})
	for i := 0; i < n; i += 5 {
		if err := g.DeleteNode(ids[i]); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
		deleted[ids[i]] = struct{}{}
	}

	var violations atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				i = (i + 5) % n
				id := ids[i]
				_, err := g.GetNode(id)
				if _, isDeleted := deleted[id]; isDeleted && err == nil {
					violations.Add(1)
				}
			}
		}(r * 53)
	}

	// Many compactions, because the window this guards is only a couple of
	// instructions wide: between publishing the new CSR and clearing the shadow
	// count. A reader holding the old pointer must never be fooled by the cleared
	// count, which is why validity is checked against the pointer itself.
	for c := 0; c < 200; c++ {
		if err := g.Compact(); err != nil {
			t.Errorf("Compact: %v", err)
			break
		}
	}
	close(stop)
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("%d reads returned a deleted node across compaction boundaries", v)
	}
	if err := g.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
}

// Appending new entities must not shadow existing CSR records — that is the
// whole point of counting shadows rather than "is the delta empty". Reads of
// pre-existing nodes must stay correct while unrelated nodes are added.
func TestConcurrent_AppendsDoNotDisturbExistingReads(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	const n = 1_000
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	var violations atomic.Int64
	var reads atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				i = (i + 3) % n
				node, err := g.GetNode(ids[i])
				reads.Add(1)
				if err != nil || !node.HasLabel(store.NodeTypeEvidenceFile) {
					violations.Add(1)
				}
			}
		}(r * 29)
	}

	for i := 0; i < 5_000; i++ {
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
			t.Errorf("AddNode: %v", err)
			break
		}
	}
	close(stop)
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("%d of %d reads of pre-existing nodes went wrong during appends", v, reads.Load())
	}
	if reads.Load() == 0 {
		t.Fatal("readers never ran")
	}
}

// Mixed load: adds, updates, deletes and reads all at once, with the index
// verifier as the final arbiter.
func TestConcurrent_MixedLoadKeepsIndexesConsistent(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)

			const seed = 500
			nodes := make([]*store.Node, seed)
			for i := range nodes {
				nodes[i] = &store.Node{Labels: []store.NodeType{benchLabelFor(i)}}
			}
			ids, err := g.AddNodes(nodes)
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			if err := g.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}

			var wg sync.WaitGroup
			var errs atomic.Int64
			stop := make(chan struct{})

			// Readers
			for r := 0; r < 4; r++ {
				wg.Add(1)
				go func(seedIdx int) {
					defer wg.Done()
					i := seedIdx
					for {
						select {
						case <-stop:
							return
						default:
						}
						i = (i + 13) % seed
						_, _ = g.GetNode(ids[i])
						_, _ = g.NodesByType(store.NodeTypeCase)
						_, _ = g.Neighbours(ids[i], store.DirectionBoth, nil)
					}
				}(r * 41)
			}

			// One writer, on disjoint ID ranges so no operation can collide with
			// another's effect and the expected outcome stays deterministic:
			// the lower half is only ever updated, the upper half only deleted.
			wg.Add(1)
			go func() {
				defer wg.Done()
				half := seed / 2
				for i := 0; i < half; i++ {
					if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
						errs.Add(1)
					}
					if err := g.UpdateNode(&store.Node{ID: ids[i],
						Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
						errs.Add(1)
					}
					if err := g.DeleteNode(ids[half+i]); err != nil {
						errs.Add(1)
					}
				}
				close(stop)
			}()

			wg.Wait()
			if e := errs.Load(); e != 0 {
				t.Fatalf("%d unexpected mutation errors", e)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes after mixed load: %v", err)
			}

			// Final state must be exactly what the writer produced.
			half := seed / 2
			for i := 0; i < half; i++ {
				n, err := g.GetNode(ids[i])
				if err != nil {
					t.Fatalf("updated node %d missing: %v", ids[i], err)
				}
				if !n.HasLabel(store.NodeTypeTag) {
					t.Fatalf("node %d did not keep its update: %v", ids[i], n.Labels)
				}
				if _, err := g.GetNode(ids[half+i]); err == nil {
					t.Fatalf("node %d should have been deleted", ids[half+i])
				}
			}
		})
	}
}
