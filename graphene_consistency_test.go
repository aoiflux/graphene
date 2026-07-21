package graphene_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Consistency hunting.
//
// The concurrency tests check that specific known hazards do not occur. These go
// the other way: they hammer the store while continuously asserting the
// invariants a graph database must never violate, and report anything that
// breaks. They are written to find bugs, not to confirm their absence.
//
// The invariants, in the order they matter:
//
//  1. Referential integrity — no read ever yields an edge whose endpoints are
//     not both live nodes.
//  2. Within-call coherence — a single API call returns internally consistent
//     data, even if the graph changes the instant after it returns.
//  3. No dangling identifiers — an ID handed back by a lookup resolves.
//  4. Convergence — once mutation stops, every index agrees with the records.

// consistencyFixture builds a graph and returns the node IDs.
func consistencyFixture(t *testing.T, g *graphene.Graph, n int) []store.NodeID {
	t.Helper()
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabelFor(i)}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%06d", i)),
			"bucket": []byte(fmt.Sprintf("bucket-%03d", i%50)),
			"tool":   []byte(fmt.Sprintf("tool-%d", i%7)),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}
	edges := make([]*store.Edge, 0, 2*n)
	for i := 0; i < n-1; i++ {
		edges = append(edges, &store.Edge{Src: ids[i], Dst: ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains}})
	}
	for i := 0; i < n; i++ {
		edges = append(edges, &store.Edge{Src: ids[i], Dst: ids[(i+11)%n],
			Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	}
	if _, err := g.AddEdges(edges); err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	return ids
}

// INVARIANT 1 & 2: every edge any read yields must be incident to the node it
// was requested for, and its reported neighbour must be the other endpoint. A
// torn read across the delta and CSR layers would break this.
func TestConsistency_ReadsAreInternallyCoherent(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := consistencyFixture(t, g, 800)
			if err := g.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}

			var violations atomic.Int64
			var checks atomic.Int64
			var wg sync.WaitGroup
			stop := make(chan struct{})

			for r := 0; r < 6; r++ {
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
						i = (i + 17) % len(ids)
						id := ids[i]

						// EdgesOf: every edge must be incident to id.
						edges, err := g.EdgesOf(id, store.DirectionBoth, nil)
						if err == nil {
							for _, e := range edges {
								checks.Add(1)
								if e.Src != id && e.Dst != id {
									t.Errorf("EdgesOf(%d) returned edge %d (%d->%d), incident to neither end",
										id, e.ID, e.Src, e.Dst)
									violations.Add(1)
								}
							}
						}

						// Neighbours: the reported node must be the far endpoint.
						nbs, err := g.Neighbours(id, store.DirectionBoth, nil)
						if err == nil {
							for _, nb := range nbs {
								checks.Add(1)
								far := nb.Edge.Dst
								if nb.Edge.Src != id {
									far = nb.Edge.Src
								}
								if nb.Node.ID != far {
									t.Errorf("Neighbours(%d): edge %d (%d->%d) paired with node %d",
										id, nb.Edge.ID, nb.Edge.Src, nb.Edge.Dst, nb.Node.ID)
									violations.Add(1)
								}
							}
						}
					}
				}(r * 61)
			}

			// Mutate underneath them.
			for i := 0; i < len(ids); i += 4 {
				if err := g.DeleteNode(ids[i]); err != nil {
					t.Errorf("DeleteNode: %v", err)
					break
				}
				if i%40 == 0 {
					if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
						t.Errorf("AddNode: %v", err)
						break
					}
				}
			}
			close(stop)
			wg.Wait()

			if v := violations.Load(); v != 0 {
				t.Fatalf("%d incoherent reads out of %d checks", v, checks.Load())
			}
			if checks.Load() == 0 {
				t.Fatal("readers performed no checks")
			}
		})
	}
}

// INVARIANT 1 at rest: after a cascade of deletions, no edge anywhere may
// reference a node that no longer exists. DeleteNode cascades to incident edges,
// so a partial cascade would show up here.
func TestConsistency_NoDanglingEdgesAfterCascades(t *testing.T) {
	for name, open := range traversalBackends() {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				ids := consistencyFixture(t, g, 600)

				for i := 0; i < len(ids); i += 3 {
					if err := g.DeleteNode(ids[i]); err != nil {
						t.Fatalf("DeleteNode: %v", err)
					}
				}
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				// Every surviving node's edges must connect two survivors.
				for _, id := range ids {
					if _, err := g.GetNode(id); err != nil {
						continue // deleted, as expected
					}
					edges, err := g.EdgesOf(id, store.DirectionBoth, nil)
					if err != nil {
						t.Fatalf("EdgesOf(%d): %v", id, err)
					}
					for _, e := range edges {
						if _, err := g.GetNode(e.Src); err != nil {
							t.Fatalf("edge %d survives but its Src %d does not", e.ID, e.Src)
						}
						if _, err := g.GetNode(e.Dst); err != nil {
							t.Fatalf("edge %d survives but its Dst %d does not", e.ID, e.Dst)
						}
					}
				}
				if err := g.VerifyIndexes(); err != nil {
					t.Fatalf("VerifyIndexes: %v", err)
				}
			})
		}
	}
}

// INVARIANT 3: a lookup must never return an entity whose deletion had already
// completed before the lookup began.
//
// The obvious version of this test — look up, then resolve, and fail on a miss —
// cannot distinguish two very different things:
//
//	(A) the index handed back an entity that was already fully deleted, which is
//	    a torn read of a single logical operation, and a real bug;
//	(B) the entity was live when the lookup ran and was deleted before the
//	    caller resolved it, which no store without snapshot isolation can
//	    prevent, and which is the caller's race, not the store's.
//
// So the deleter publishes its progress through an atomic, and the reader
// samples that counter *before* each lookup. An entity below the sampled mark
// has a happens-before edge from its completed deletion to the start of the
// lookup: seeing it is unambiguously (A). Everything else is counted and
// reported, never failed, so the numbers say how wide the benign window is
// rather than pretending it is closed.
func TestConsistency_LookupsNeverReturnCompletedDeletions(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := consistencyFixture(t, g, 800)
			if err := g.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			pos := make(map[store.NodeID]int, len(ids))
			for i, id := range ids {
				pos[id] = i
			}

			var deletedUpTo atomic.Int64 // ids[0:deletedUpTo) are fully deleted
			var violations atomic.Int64  // (A) — must be zero
			var benign atomic.Int64      // (B) — reported only
			var checks atomic.Int64
			var wg sync.WaitGroup
			stop := make(chan struct{})

			for r := 0; r < 6; r++ {
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
						i = (i + 13) % len(ids)

						for _, probe := range [][2]string{
							{"sha256", fmt.Sprintf("hash-%06d", i)},
							{"bucket", fmt.Sprintf("bucket-%03d", i%50)},
							{"tool", fmt.Sprintf("tool-%d", i%7)},
						} {
							// Sample before the lookup, so anything below the mark
							// was provably gone before the lookup started.
							mark := int(deletedUpTo.Load())
							hits, err := g.NodesByProperty(probe[0], []byte(probe[1]))
							if err != nil {
								continue
							}
							for _, id := range hits {
								checks.Add(1)
								if p, ok := pos[id]; ok && p < mark {
									violations.Add(1)
									continue
								}
								if _, err := g.GetNode(id); err != nil {
									benign.Add(1)
								}
							}
						}
					}
				}(r * 71)
			}

			for i := range ids {
				if err := g.DeleteNode(ids[i]); err != nil {
					t.Errorf("DeleteNode: %v", err)
					break
				}
				deletedUpTo.Store(int64(i + 1))
			}
			close(stop)
			wg.Wait()

			t.Logf("%d lookups; %d results for entities deleted after the lookup began (benign)",
				checks.Load(), benign.Load())
			if v := violations.Load(); v != 0 {
				t.Fatalf("%d results named entities whose deletion had already completed", v)
			}
			if checks.Load() == 0 {
				t.Fatal("readers performed no checks")
			}
		})
	}
}

// INVARIANT 3 for the query planner. Same discipline: the planner mixes
// postings, label lists and adjacency, so a half-updated index shows up as a
// result the deleter had already finished removing.
func TestConsistency_QueriesNeverReturnCompletedDeletions(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := consistencyFixture(t, g, 800)
			pos := make(map[store.NodeID]int, len(ids))
			for i, id := range ids {
				pos[id] = i
			}

			queries := []store.NodeQuery{
				{Types: []store.NodeType{store.NodeTypeCase}},
				{Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-003")}}},
				{Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-003")},
					{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-3")},
				}, FilterMode: store.MatchAll},
				{Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-003")},
					{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-3")},
				}, FilterMode: store.MatchAny},
				{Types: []store.NodeType{store.NodeTypeMicroArtefact},
					Filters: []store.PropertyFilter{{Key: "tool", Op: store.PropertyOpPrefix, Value: []byte("tool-")}}},
			}

			var deletedUpTo atomic.Int64
			var violations atomic.Int64
			var benign atomic.Int64
			var checks atomic.Int64
			var wg sync.WaitGroup
			stop := make(chan struct{})

			for r := 0; r < 6; r++ {
				wg.Add(1)
				go func(seed int) {
					defer wg.Done()
					q := seed
					for {
						select {
						case <-stop:
							return
						default:
						}
						q = (q + 1) % len(queries)
						mark := int(deletedUpTo.Load())
						got, err := g.QueryNodeIDs(queries[q])
						if err != nil {
							continue
						}
						for _, id := range got {
							checks.Add(1)
							if p, ok := pos[id]; ok && p < mark {
								violations.Add(1)
								continue
							}
							if _, err := g.GetNode(id); err != nil {
								benign.Add(1)
							}
						}
					}
				}(r)
			}

			for i := range ids {
				if err := g.DeleteNode(ids[i]); err != nil {
					t.Errorf("DeleteNode: %v", err)
					break
				}
				deletedUpTo.Store(int64(i + 1))
			}
			close(stop)
			wg.Wait()

			t.Logf("%d results; %d for entities deleted after the query began (benign)",
				checks.Load(), benign.Load())
			if v := violations.Load(); v != 0 {
				t.Fatalf("%d results named entities whose deletion had already completed", v)
			}
			if checks.Load() == 0 {
				t.Fatal("readers performed no checks")
			}
		})
	}
}

// INVARIANT 4: once mutation stops, every index must agree with the records —
// including across the property index's shards, which are locked independently.
func TestConsistency_ConvergesAfterConcurrentMutation(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			ids := consistencyFixture(t, g, 400)

			var wg sync.WaitGroup
			var errs atomic.Int64

			// Two writers on disjoint halves, so the outcome stays deterministic.
			half := len(ids) / 2
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := 0; i < half; i++ {
					if err := g.UpdateNodeIndexed(
						&store.Node{ID: ids[i], Labels: []store.NodeType{store.NodeTypeTag}},
						map[string][]byte{"sha256": []byte(fmt.Sprintf("rehashed-%06d", i))},
					); err != nil {
						errs.Add(1)
					}
				}
			}()
			go func() {
				defer wg.Done()
				for i := half; i < len(ids); i++ {
					if err := g.DeleteNode(ids[i]); err != nil {
						errs.Add(1)
					}
				}
			}()
			wg.Wait()

			if e := errs.Load(); e != 0 {
				t.Fatalf("%d unexpected mutation errors", e)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes: %v", err)
			}

			// The updated half must be findable by its new value and absent under
			// the old one — across every shard the keys hash to.
			for i := 0; i < half; i++ {
				hits, err := g.NodesByProperty("sha256", []byte(fmt.Sprintf("rehashed-%06d", i)))
				if err != nil {
					t.Fatalf("NodesByProperty: %v", err)
				}
				if len(hits) != 1 || hits[0] != ids[i] {
					t.Fatalf("node %d not findable by its new value: %v", ids[i], hits)
				}
				stale, _ := g.NodesByProperty("sha256", []byte(fmt.Sprintf("hash-%06d", i)))
				if len(stale) != 0 {
					t.Fatalf("node %d still findable by its old value: %v", ids[i], stale)
				}
				// The other keys were dropped by UpdateNodeIndexed's replacement.
				if old, _ := g.NodesByProperty("bucket", []byte(fmt.Sprintf("bucket-%03d", i%50))); len(old) > 0 {
					for _, id := range old {
						if id == ids[i] {
							t.Fatalf("node %d kept a key the update did not carry", ids[i])
						}
					}
				}
			}

			// The deleted half must be gone from every key.
			for i := half; i < len(ids); i++ {
				if _, err := g.GetNode(ids[i]); err == nil {
					t.Fatalf("node %d should be deleted", ids[i])
				}
				for _, probe := range [][2]string{
					{"sha256", fmt.Sprintf("hash-%06d", i)},
					{"bucket", fmt.Sprintf("bucket-%03d", i%50)},
					{"tool", fmt.Sprintf("tool-%d", i%7)},
				} {
					hits, _ := g.NodesByProperty(probe[0], []byte(probe[1]))
					for _, id := range hits {
						if id == ids[i] {
							t.Fatalf("deleted node %d still indexed under %s", ids[i], probe[0])
						}
					}
				}
			}
		})
	}
}

// A deterministic version of the hazard the concurrent tests can only sample.
//
// Index writes do not check that the entity exists — that check would cost a
// lock and a lookup on a hot write path, and it could not help with the case
// that actually matters, where the entity is deleted after the entry is made.
// So the index can hold entries the records do not back, and the read path is
// what has to be careful.
//
// A phantom entry makes that state without needing a race to produce it, so this
// pins the behaviour deterministically. On the unfiltered lookups it fails.
func TestConsistency_LookupsFilterEntriesWithoutRecords(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			real, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if err := g.IndexNodeProperty(real, "sha256", []byte("shared")); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
			// An ID that was never issued, sharing the value with a live node so
			// the result cannot simply be empty.
			if err := g.IndexNodeProperty(store.NodeID(1<<40), "sha256", []byte("shared")); err != nil {
				t.Fatalf("IndexNodeProperty(phantom): %v", err)
			}

			hits, err := g.NodesByProperty("sha256", []byte("shared"))
			if err != nil {
				t.Fatalf("NodesByProperty: %v", err)
			}
			if len(hits) != 1 || hits[0] != real {
				t.Fatalf("lookup returned %v, want only the live node %d", hits, real)
			}

			// The verifier is the other half of the story: the read path hides the
			// entry, and VerifyIndexes reports it rather than leaving it silent.
			if err := g.VerifyIndexes(); err == nil {
				t.Fatal("VerifyIndexes accepted an index entry with no record behind it")
			}
		})
	}
}
