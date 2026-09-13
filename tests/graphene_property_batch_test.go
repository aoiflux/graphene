package graphene_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// NodesByPropertyBatch end to end.
//
// The index package tests the merge against a fake base and the disk package
// tests the cursor against an encoded section. Neither reaches the thing a
// consumer actually does: open a store whose index is in its image, hand it a set
// of values, and expect the same rows a loop of NodesByProperty would have given.
// That is what these do, over a store that has been compacted — so the base is a
// real GPIX section — with entities written since, entities deleted since, and
// entities reindexed since, because those are the three states the delta and the
// retraction set exist for and a batch that only ever read the base would pass a
// test built on a freshly compacted store.
//
// Every case is run against both backends. The memory backend has no
// disk-resident index and so no ordering to exploit, which is exactly why it
// belongs here: the answers must not depend on that.

const batchNodes = 1200

// batchFixture writes a store whose indexed keys have the shapes a batch meets:
// one all-distinct key (the digest join this API is for), one low-cardinality key
// where a value maps to many entities, and an edge key. Then it compacts, so the
// entries are in the image, and mutates so that the delta and the retraction set
// are both non-empty.
//
// It returns the digest of every node it wrote, in write order, and the set of
// nodes that should still answer for each.
func batchFixture(t *testing.T, g *graphene.Graph) (digests [][]byte, mutated []store.NodeID) {
	t.Helper()

	nodes := make([]*store.Node, batchNodes)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("digest-%06d", i)),
			"bucket": []byte(fmt.Sprintf("b%02d", i%17)),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}
	// The image, if this backend has one. Compact is a no-op in memory.
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Now the states that only exist after a compaction, which is where a batch
	// that read the base alone would give a wrong answer rather than a slow one.
	//
	// A node written since: its entry is in the delta alone.
	freshID, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.IndexNodeProperties(freshID, map[string][]byte{
		"sha256": []byte("digest-999999"),
	}); err != nil {
		t.Fatalf("IndexNodeProperties: %v", err)
	}
	// A second holder of a value the image already has: the merge has to union the
	// two sides rather than pick one.
	sharedID, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.IndexNodeProperties(sharedID, map[string][]byte{
		"sha256": []byte("digest-000005"),
	}); err != nil {
		t.Fatalf("IndexNodeProperties: %v", err)
	}
	// A node deleted since: its base entry must not come back.
	if err := g.DeleteNode(ids[7]); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	// A node reindexed since: the old value must stop answering and the new must
	// start. UpdateNodeIndexed replaces the whole indexed state, which is the
	// resurrection case docs/TECHNICAL_DETAILS.md §14.7 names, reached through the
	// API rather than by poking the index.
	moved, err := g.GetNode(ids[9])
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if err := g.UpdateNodeIndexed(moved, map[string][]byte{
		"sha256": []byte("digest-moved"),
		"bucket": []byte(fmt.Sprintf("b%02d", 9%17)),
	}); err != nil {
		t.Fatalf("UpdateNodeIndexed: %v", err)
	}

	for i := 0; i < batchNodes; i++ {
		digests = append(digests, []byte(fmt.Sprintf("digest-%06d", i)))
	}
	digests = append(digests, []byte("digest-999999"), []byte("digest-moved"))
	// Values nothing holds, above, below and between what does.
	digests = append(digests, []byte("digest-999998"), []byte("aaa"), []byte("zzz"),
		[]byte("digest-000005x"), []byte(""))
	return digests, []store.NodeID{freshID, sharedID, ids[7], ids[9]}
}

// perValue is the answer a caller would get without this API: NodesByProperty in
// a loop. Every assertion below is against it, because it is what the batch is a
// bulk form of and the only reference that cannot be wrong in the same way.
func perValue(t *testing.T, g *graphene.Graph, key string, values [][]byte) map[int][]store.NodeID {
	t.Helper()
	out := map[int][]store.NodeID{}
	for i, v := range values {
		ids, err := g.NodesByProperty(key, v)
		if err != nil {
			t.Fatalf("NodesByProperty(%q): %v", v, err)
		}
		if len(ids) > 0 {
			out[i] = slices.Clone(ids)
		}
	}
	return out
}

func batched(t *testing.T, g *graphene.Graph, key string, values [][]byte) (map[int][]store.NodeID, []int) {
	t.Helper()
	out := map[int][]store.NodeID{}
	var order []int
	err := g.NodesByPropertyBatch(key, values, func(i int, ids []store.NodeID) bool {
		if _, dup := out[i]; dup {
			t.Fatalf("index %d reported twice", i)
		}
		out[i] = slices.Clone(ids)
		order = append(order, i)
		return true
	})
	if err != nil {
		t.Fatalf("NodesByPropertyBatch: %v", err)
	}
	return out, order
}

func sameAnswers(t *testing.T, values [][]byte, batch, loop map[int][]store.NodeID) {
	t.Helper()
	for i := range values {
		b, inBatch := batch[i]
		l, inLoop := loop[i]
		switch {
		case inBatch != inLoop:
			t.Fatalf("value %d (%q): batch reported %v, the loop reported %v",
				i, values[i], inBatch, inLoop)
		case inBatch && !slices.Equal(b, l):
			t.Fatalf("value %d (%q): batch %v, loop %v", i, values[i], b, l)
		}
	}
	if len(batch) != len(loop) {
		t.Fatalf("batch reported %d values, the loop %d", len(batch), len(loop))
	}
}

func TestNodesByPropertyBatch_AgreesWithNodesByPropertyOverACompactedStore(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			digests, _ := batchFixture(t, g)
			batch, _ := batched(t, g, "sha256", digests)
			sameAnswers(t, digests, batch, perValue(t, g, "sha256", digests))

			// The three post-compaction states, named so a failure says which.
			byValue := func(v string) []store.NodeID {
				for i, d := range digests {
					if string(d) == v {
						return batch[i]
					}
				}
				t.Fatalf("%q is not in the batch", v)
				return nil
			}
			if len(byValue("digest-000005")) != 2 {
				t.Fatalf("a value held by the image and by a node written since "+
					"resolved to %v, want both", byValue("digest-000005"))
			}
			if got := byValue("digest-000007"); len(got) != 0 {
				t.Fatalf("a deleted node came back out of the image: %v", got)
			}
			if got := byValue("digest-000009"); len(got) != 0 {
				t.Fatalf("a reindexed node still answers for its old value: %v", got)
			}
			if len(byValue("digest-moved")) != 1 {
				t.Fatalf("a reindexed node does not answer for its new value: %v",
					byValue("digest-moved"))
			}
			if len(byValue("digest-999999")) != 1 {
				t.Fatalf("a node written since the compaction is missing: %v",
					byValue("digest-999999"))
			}
		})
	}
}

// TestNodesByPropertyBatch_UnsortedInputIsTheSameAnswer. A caller's values arrive
// in whatever order its own rows are in, and the ordering the batch builds is an
// implementation detail that must not be visible in the answers.
func TestNodesByPropertyBatch_UnsortedInputIsTheSameAnswer(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			digests, _ := batchFixture(t, g)
			loop := perValue(t, g, "sha256", digests)

			rng := rand.New(rand.NewSource(20260913))
			shuffled := slices.Clone(digests)
			// A permutation, tracked, so the shuffled results can be mapped back
			// to the original positions and compared against the same reference.
			pos := make([]int, len(shuffled))
			for i := range pos {
				pos[i] = i
			}
			rng.Shuffle(len(shuffled), func(i, j int) {
				shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
				pos[i], pos[j] = pos[j], pos[i]
			})

			batch, _ := batched(t, g, "sha256", shuffled)
			remapped := map[int][]store.NodeID{}
			for i, ids := range batch {
				remapped[pos[i]] = ids
			}
			sameAnswers(t, digests, remapped, loop)
		})
	}
}

// TestNodesByPropertyBatch_LowCardinalityKey: a value held by seventy entities is
// a run the merge walks rather than a single id, and the batch must hand over all
// of them.
func TestNodesByPropertyBatch_LowCardinalityKey(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			batchFixture(t, g)
			var values [][]byte
			for i := 0; i < 20; i++ { // 17 real buckets plus three that are not
				values = append(values, []byte(fmt.Sprintf("b%02d", i)))
			}
			batch, _ := batched(t, g, "bucket", values)
			sameAnswers(t, values, batch, perValue(t, g, "bucket", values))
			// And the ids come back ascending, which is what NodesByProperty
			// promises and what a merged result is easiest to get wrong.
			for i, ids := range batch {
				if !slices.IsSorted(ids) {
					t.Fatalf("value %d: ids are not ascending: %v", i, ids)
				}
			}
		})
	}
}

// TestNodesByPropertyBatch_RepeatedAndEmptyInputs are the two shapes a caller
// reaches by accident: a value list with duplicates, and an empty one.
func TestNodesByPropertyBatch_RepeatedAndEmptyInputs(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			batchFixture(t, g)

			values := [][]byte{
				[]byte("digest-000001"), []byte("digest-000001"),
				[]byte("nothing"), []byte("digest-000002"), []byte("nothing"),
			}
			batch, _ := batched(t, g, "sha256", values)
			if len(batch) != 3 {
				t.Fatalf("reported %d of 3 positions with a holder: %v", len(batch), batch)
			}
			if !slices.Equal(batch[0], batch[1]) {
				t.Fatalf("the two positions of one value differ: %v and %v", batch[0], batch[1])
			}

			called := false
			if err := g.NodesByPropertyBatch("sha256", nil, func(int, []store.NodeID) bool {
				called = true
				return true
			}); err != nil {
				t.Fatalf("empty batch: %v", err)
			}
			if called {
				t.Fatal("an empty batch called the callback")
			}
		})
	}
}

func TestNodesByPropertyBatch_StopsAndCancels(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			digests, _ := batchFixture(t, g)

			calls := 0
			if err := g.NodesByPropertyBatch("sha256", digests, func(int, []store.NodeID) bool {
				calls++
				return calls < 5
			}); err != nil {
				t.Fatalf("declining the callback produced an error: %v", err)
			}
			if calls != 5 {
				t.Fatalf("the callback ran %d times after declining at 5", calls)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := g.NodesByPropertyBatchCtx(ctx, "sha256", digests,
				func(int, []store.NodeID) bool { return true })
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("a cancelled batch returned %v, want context.Canceled", err)
			}
		})
	}
}

func TestEdgesByPropertyBatch_AgreesWithEdgesByProperty(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			ids, err := g.AddNodes([]*store.Node{
				{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			})
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			edges := make([]*store.Edge, 60)
			for i := range edges {
				edges[i] = &store.Edge{
					Src:    ids[0],
					Dst:    ids[1],
					Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
					Weight: 0.5,
				}
			}
			edgeIDs, err := g.AddEdges(edges)
			if err != nil {
				t.Fatalf("AddEdges: %v", err)
			}
			for i, id := range edgeIDs {
				if err := g.IndexEdgeProperties(id, map[string][]byte{
					"rel": []byte(fmt.Sprintf("r%02d", i%13)),
				}); err != nil {
					t.Fatalf("IndexEdgeProperties: %v", err)
				}
			}
			if err := g.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if err := g.DeleteEdge(edgeIDs[0]); err != nil {
				t.Fatalf("DeleteEdge: %v", err)
			}

			var values [][]byte
			for i := 15; i >= 0; i-- { // descending, so an ordering is built
				values = append(values, []byte(fmt.Sprintf("r%02d", i)))
			}
			batch := map[int][]store.EdgeID{}
			if err := g.EdgesByPropertyBatch("rel", values, func(i int, got []store.EdgeID) bool {
				batch[i] = slices.Clone(got)
				return true
			}); err != nil {
				t.Fatalf("EdgesByPropertyBatch: %v", err)
			}
			for i, v := range values {
				want, err := g.EdgesByProperty("rel", v)
				if err != nil {
					t.Fatalf("EdgesByProperty(%q): %v", v, err)
				}
				have, reported := batch[i]
				if len(want) == 0 {
					if reported {
						t.Fatalf("rel=%q reported %v with no holder", v, have)
					}
					continue
				}
				if !reported || !slices.Equal(have, want) {
					t.Fatalf("rel=%q: batch %v (reported=%v), loop %v", v, have, reported, want)
				}
			}
		})
	}
}

// TestNodesByPropertyBatch_ReopenedStoreReadsFromTheImage is the case the whole
// item is for: a store opened fresh from disk, whose index is the GPIX section and
// whose delta is empty. Nothing else in this file guarantees the base is reached —
// the fixture's mutations mean the delta could be answering everything.
func TestNodesByPropertyBatch_ReopenedStoreReadsFromTheImage(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	digests, _ := batchFixture(t, g)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	sorted := slices.Clone(digests)
	sort.Slice(sorted, func(i, j int) bool { return string(sorted[i]) < string(sorted[j]) })

	batch, order := batched(t, reopened, "sha256", sorted)
	sameAnswers(t, sorted, batch, perValue(t, reopened, "sha256", sorted))
	// With the answers coming out of the image, the callbacks arrive in value
	// order — and the input is already sorted, so that is also index order. This
	// is the observable trace of the sweep at the public boundary.
	if !slices.IsSorted(order) {
		t.Fatalf("over a reopened store the callbacks did not follow value order: %v", order)
	}
	if len(batch) == 0 {
		t.Fatal("a reopened store answered nothing; the base is not being read")
	}
}
