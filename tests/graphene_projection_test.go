package graphene_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Serving indexed property values without reading the records.
//
// The thing these have to pin is not the arithmetic — there is none — but the
// agreement between two answers that come from different places. A projection
// reads the property index; everything else in the engine reads the records. The
// two are written together and must not drift, so the central test asks the
// index for what it holds and checks it against what was indexed, on both
// backends and on both sides of a compaction — because after a compaction the
// entries live in the image's own section and before it they live in the shards,
// and those are two implementations of one answer.

// projFixture writes n nodes, each indexed under every key in keys with a value
// derived from the node's position, and returns the ids in write order.
func projFixture(t *testing.T, g *graphene.Graph, n int, keys []string) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := g.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("payload-%d", i)),
		})
		if err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
		for _, k := range keys {
			if err := g.IndexNodeProperty(id, k, projValue(k, i)); err != nil {
				t.Fatalf("IndexNodeProperty %d %q: %v", i, k, err)
			}
		}
		ids = append(ids, id)
	}
	return ids
}

func projValue(key string, i int) []byte {
	return []byte(fmt.Sprintf("%s=%d", key, i))
}

// projEnv mirrors boundedEnv: a graph plus the residency under test, so every
// assertion runs against the shards and against the image's index section.
type projEnv struct {
	g       *graphene.Graph
	compact bool
}

func (e *projEnv) settle(t *testing.T) {
	t.Helper()
	if !e.compact {
		return
	}
	if err := e.g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
}

func projBackends(t *testing.T, fn func(t *testing.T, e *projEnv)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, &projEnv{g: graphene.NewInMemory()}) })
	t.Run("disk_delta", func(t *testing.T) { fn(t, &projEnv{g: openDisk(t)}) })
	t.Run("disk_image", func(t *testing.T) { fn(t, &projEnv{g: openDisk(t), compact: true}) })
}

// collectProjection drains a projection into a map keyed by (id index, key
// index), with the values sorted so a comparison does not depend on an order the
// API deliberately does not promise.
func collectProjection(t *testing.T, g *graphene.Graph, ids []store.NodeID, keys []string) map[[2]int][]string {
	t.Helper()
	got := map[[2]int][]string{}
	err := g.ForEachNodeProjection(ids, keys, func(idIdx, keyIdx int, value []byte) bool {
		got[[2]int{idIdx, keyIdx}] = append(got[[2]int{idIdx, keyIdx}], string(value))
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeProjection: %v", err)
	}
	for k := range got {
		sort.Strings(got[k])
	}
	return got
}

// The answer is the values that were indexed, at the positions they were asked
// for, on every backend and either side of a compaction.
func TestProjection_ReturnsWhatWasIndexedAtTheRightPositions(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		keys := []string{"digest", "bucket", "kind"}
		ids := projFixture(t, e.g, 40, keys)
		e.settle(t)

		got := collectProjection(t, e.g, ids, keys)
		if len(got) != len(ids)*len(keys) {
			t.Fatalf("got %d (id, key) results, want %d", len(got), len(ids)*len(keys))
		}
		for i := range ids {
			for k, key := range keys {
				vals := got[[2]int{i, k}]
				if len(vals) != 1 {
					t.Fatalf("id %d key %q: %d values, want 1 (%v)", i, key, len(vals), vals)
				}
				if vals[0] != string(projValue(key, i)) {
					t.Errorf("id %d key %q: got %q, want %q", i, key, vals[0], projValue(key, i))
				}
			}
		}
	})
}

// A key nothing is indexed under is silence, not an error and not an empty
// entry: that is how a caller asking for a non-indexed field finds out.
func TestProjection_AnUnindexedKeyProducesNothing(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		keys := []string{"digest"}
		ids := projFixture(t, e.g, 10, keys)
		e.settle(t)

		got := collectProjection(t, e.g, ids, []string{"digest", "never-indexed"})
		for i := range ids {
			if len(got[[2]int{i, 0}]) != 1 {
				t.Fatalf("id %d: digest missing", i)
			}
			if v, ok := got[[2]int{i, 1}]; ok {
				t.Errorf("id %d: unindexed key produced %v", i, v)
			}
		}
	})
}

// A key registered twice for one entity is two values, not one: the index is a
// multimap and a projection that silently kept the first would lose data.
func TestProjection_SeveralValuesUnderOneKeyAreAllReturned(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		id, err := e.g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range []string{"alpha", "beta", "gamma"} {
			if err := e.g.IndexNodeProperty(id, "tag", []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
		e.settle(t)

		got := collectProjection(t, e.g, []store.NodeID{id}, []string{"tag"})
		want := []string{"alpha", "beta", "gamma"}
		if v := got[[2]int{0, 0}]; len(v) != 3 || v[0] != want[0] || v[1] != want[1] || v[2] != want[2] {
			t.Errorf("got %v, want %v", v, want)
		}
	})
}

// The union of the image's entries and the shards' must not double-count. An
// entity indexed, compacted, then re-indexed with the same value holds that
// pair in both halves — and a projection that yielded it twice would make an
// idempotent re-ingest count everything twice.
func TestProjection_AnEntryInBothTheImageAndTheDeltaIsReturnedOnce(t *testing.T) {
	g := openDisk(t)
	keys := []string{"digest"}
	ids := projFixture(t, g, 5, keys)
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	// Re-register exactly what the image already holds.
	for i, id := range ids {
		if err := g.IndexNodeProperty(id, "digest", projValue("digest", i)); err != nil {
			t.Fatal(err)
		}
	}

	got := collectProjection(t, g, ids, keys)
	for i := range ids {
		vals := got[[2]int{i, 0}]
		if len(vals) != 1 {
			t.Errorf("id %d: got %d values %v, want 1 — the image and the delta were both counted", i, len(vals), vals)
		}
	}
}

// A value the delta adds on top of what the image holds is a second value, not a
// replacement — the dedup must be on the pair, not on the key.
func TestProjection_ADifferentValueInTheDeltaIsASecondValue(t *testing.T) {
	g := openDisk(t)
	ids := projFixture(t, g, 3, []string{"tag"})
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := g.IndexNodeProperty(id, "tag", []byte("added-later")); err != nil {
			t.Fatal(err)
		}
	}

	got := collectProjection(t, g, ids, []string{"tag"})
	for i := range ids {
		vals := got[[2]int{i, 0}]
		if len(vals) != 2 {
			t.Fatalf("id %d: got %v, want the image's value and the delta's", i, vals)
		}
		if vals[0] != "added-later" || vals[1] != string(projValue("tag", i)) {
			t.Errorf("id %d: got %v", i, vals)
		}
	}
}

// A deleted entity has no entries, from either half. The image still physically
// holds its entries until the next compaction, so this is the retraction bitset
// doing its job rather than the absence of data.
func TestProjection_ADeletedEntityProjectsNothing(t *testing.T) {
	g := openDisk(t)
	keys := []string{"digest", "bucket"}
	ids := projFixture(t, g, 6, keys)
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids[:3] {
		if err := g.DeleteNode(id); err != nil {
			t.Fatal(err)
		}
	}

	got := collectProjection(t, g, ids, keys)
	for i := range ids[:3] {
		for k := range keys {
			if v, ok := got[[2]int{i, k}]; ok {
				t.Errorf("deleted id %d key %d still projects %v", i, k, v)
			}
		}
	}
	for i := 3; i < len(ids); i++ {
		for k := range keys {
			if len(got[[2]int{i, k}]) != 1 {
				t.Errorf("surviving id %d key %d: got %v", i, k, got[[2]int{i, k}])
			}
		}
	}
}

// Duplicate ids are resolved once per occurrence, at each occurrence's own
// position — the same rule BatchReader states.
func TestProjection_DuplicateIDsResolveAtEachPosition(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		ids := projFixture(t, e.g, 2, []string{"digest"})
		e.settle(t)

		asked := []store.NodeID{ids[0], ids[1], ids[0]}
		got := collectProjection(t, e.g, asked, []string{"digest"})
		if len(got[[2]int{0, 0}]) != 1 || len(got[[2]int{2, 0}]) != 1 {
			t.Fatalf("a repeated id did not resolve at both positions: %v", got)
		}
		if got[[2]int{0, 0}][0] != got[[2]int{2, 0}][0] {
			t.Errorf("the same id gave different answers at two positions")
		}
	})
}

// The pass crosses more than one chunk, so the batching seam is exercised rather
// than assumed: projection resolves 512 ids under the locks at a time.
func TestProjection_CrossesTheChunkBoundary(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		const n = 1500 // more than two chunks
		keys := []string{"digest"}
		ids := projFixture(t, e.g, n, keys)
		e.settle(t)

		seen := make([]int, n)
		err := e.g.ForEachNodeProjection(ids, keys, func(idIdx, _ int, value []byte) bool {
			seen[idIdx]++
			if !bytes.Equal(value, projValue("digest", idIdx)) {
				t.Fatalf("id %d: got %q, want %q", idIdx, value, projValue("digest", idIdx))
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		for i, c := range seen {
			if c != 1 {
				t.Fatalf("id %d seen %d times, want 1", i, c)
			}
		}
	})
}

// A callback that declines stops the pass without an error.
func TestProjection_StopsWhenTheCallbackDeclines(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		ids := projFixture(t, e.g, 100, []string{"digest"})
		e.settle(t)

		calls := 0
		err := e.g.ForEachNodeProjection(ids, []string{"digest"}, func(int, int, []byte) bool {
			calls++
			return false
		})
		if err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Errorf("callback ran %d times after declining once", calls)
		}
	})
}

// An already-dead context stops the pass before the callback runs.
func TestProjection_CancelledContext(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		ids := projFixture(t, e.g, 10, []string{"digest"})
		e.settle(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ran := false
		err := e.g.ForEachNodeProjectionCtx(ctx, ids, []string{"digest"}, func(int, int, []byte) bool {
			ran = true
			return true
		})
		if err != ctx.Err() {
			t.Fatalf("err: got %v, want %v", err, ctx.Err())
		}
		if ran {
			t.Error("callback ran under an already-cancelled context")
		}
	})
}

// Empty inputs are not errors and call nothing back.
func TestProjection_EmptyInputs(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		ids := projFixture(t, e.g, 3, []string{"digest"})
		e.settle(t)

		for _, tc := range []struct {
			name string
			ids  []store.NodeID
			keys []string
		}{
			{"no ids", nil, []string{"digest"}},
			{"no keys", ids, nil},
			{"neither", nil, nil},
		} {
			ran := false
			if err := e.g.ForEachNodeProjection(tc.ids, tc.keys, func(int, int, []byte) bool {
				ran = true
				return true
			}); err != nil {
				t.Errorf("%s: %v", tc.name, err)
			}
			if ran {
				t.Errorf("%s: callback ran", tc.name)
			}
		}
	})
}

// The materialising form agrees with the streaming one it wraps.
func TestProjection_MaterialisedFormAgreesWithTheStream(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		keys := []string{"digest", "bucket"}
		ids := projFixture(t, e.g, 20, keys)
		e.settle(t)

		streamed := collectProjection(t, e.g, ids, keys)
		got, err := e.g.GetNodesProjected(ids, keys)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(ids) {
			t.Fatalf("got %d projections, want %d", len(got), len(ids))
		}
		for i := range ids {
			if got[i].ID != ids[i] {
				t.Fatalf("projection %d: id %d, want %d", i, got[i].ID, ids[i])
			}
			for k := range keys {
				var vals []string
				for _, v := range got[i].Values[k] {
					vals = append(vals, string(v))
				}
				sort.Strings(vals)
				want := streamed[[2]int{i, k}]
				if len(vals) != len(want) {
					t.Fatalf("projection %d key %d: got %v, want %v", i, k, vals, want)
				}
				for j := range want {
					if vals[j] != want[j] {
						t.Errorf("projection %d key %d: got %v, want %v", i, k, vals, want)
					}
				}
			}
		}
	})
}

// A value handed to the callback is scratch the index reuses. The materialising
// form must therefore copy, and this is the test that fails if it stops doing so.
func TestProjection_MaterialisedValuesSurviveTheNextPass(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		keys := []string{"digest"}
		ids := projFixture(t, e.g, 600, keys) // more than one chunk, so the buffer is reused
		e.settle(t)

		got, err := e.g.GetNodesProjected(ids, keys)
		if err != nil {
			t.Fatal(err)
		}
		// Run another projection, which reuses the index's scratch buffers.
		if err := e.g.ForEachNodeProjection(ids, keys, func(int, int, []byte) bool { return true }); err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			vals := got[i].Values[0]
			if len(vals) != 1 || !bytes.Equal(vals[0], projValue("digest", i)) {
				t.Fatalf("projection %d was overwritten by a later pass: got %q", i, vals)
			}
		}
	})
}

// Edges carry indexed properties too, and get the same answer.
func TestProjectionEdges_ReturnsWhatWasIndexed(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		nodes := projFixture(t, e.g, 2, []string{"digest"})
		var ids []store.EdgeID
		for i := 0; i < 5; i++ {
			id, err := e.g.AddEdge(&store.Edge{
				Src:    nodes[0],
				Dst:    nodes[1],
				Labels: []store.EdgeType{store.EdgeTypeContains},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := e.g.IndexEdgeProperty(id, "rel", projValue("rel", i)); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		e.settle(t)

		seen := make([]string, len(ids))
		err := e.g.ForEachEdgeProjection(ids, []string{"rel"}, func(idIdx, keyIdx int, value []byte) bool {
			if keyIdx != 0 {
				t.Errorf("unexpected key index %d", keyIdx)
			}
			seen[idIdx] = string(value)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range ids {
			if seen[i] != string(projValue("rel", i)) {
				t.Errorf("edge %d: got %q, want %q", i, seen[i], projValue("rel", i))
			}
		}
	})
}

// A projection holds shard read locks and hands out slices from a scratch buffer
// it owns. Both are reasons a concurrent reader could see the wrong thing, and
// neither shows up in a single-goroutine test. The scratch is per call rather
// than per index, which is what makes this safe — this is the test that fails if
// it ever becomes shared.
func TestProjection_ConcurrentReadersAndWriters(t *testing.T) {
	g := openDisk(t)
	keys := []string{"digest", "bucket", "kind"}
	ids := projFixture(t, g, 400, keys)
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 8)

	// Readers: each checks that every value it is handed is the one that id was
	// indexed under, so a torn or reused buffer is a failure and not a flake.
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				err := g.ForEachNodeProjection(ids, keys, func(idIdx, keyIdx int, value []byte) bool {
					if !bytes.Equal(value, projValue(keys[keyIdx], idIdx)) {
						errs <- fmt.Errorf("id %d key %q: got %q, want %q",
							idIdx, keys[keyIdx], value, projValue(keys[keyIdx], idIdx))
						return false
					}
					return true
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	// A writer, so the shard locks are actually contended rather than merely
	// taken. It indexes a key nobody projects, so it cannot change any answer
	// above — what it tests is the locking, not the visibility.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			if err := g.IndexNodeProperty(ids[i%len(ids)], "churn", projValue("churn", i)); err != nil {
				errs <- err
				return
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// The dedup compares whole values, not a prefix of them.
//
// This is the case a cheaper comparison would get wrong and a fixture of
// obviously-different strings would never reach: one entity, one key, two values
// that agree on their first bytes and differ later — the shape content digests
// take, where one pair in 256 shares a leading byte. A dedup keyed on anything
// less than the whole value silently drops the second.
func TestProjection_TheDedupComparesWholeValuesNotAPrefix(t *testing.T) {
	g := openDisk(t)

	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatal(err)
	}
	const inImage = "digest-aaaaaaaaaaaaaaaa"
	const inDelta = "digest-aaaaaaaaaaaaaaab" // same but for the last byte
	if err := g.IndexNodeProperty(id, "digest", []byte(inImage)); err != nil {
		t.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := g.IndexNodeProperty(id, "digest", []byte(inDelta)); err != nil {
		t.Fatal(err)
	}

	got := collectProjection(t, g, []store.NodeID{id}, []string{"digest"})
	vals := got[[2]int{0, 0}]
	if len(vals) != 2 {
		t.Fatalf("got %v, want both %q and %q — a prefix comparison merged them", vals, inDelta, inImage)
	}
	// collectProjection sorts, so the order here is the values', not the
	// projection's — which promises none.
	if vals[0] != inImage || vals[1] != inDelta {
		t.Errorf("got %v", vals)
	}

	// The other direction: a value that differs only in *length* must not be
	// merged into its own prefix either.
	id2, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.IndexNodeProperty(id2, "digest", []byte("prefix")); err != nil {
		t.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := g.IndexNodeProperty(id2, "digest", []byte("prefix-and-more")); err != nil {
		t.Fatal(err)
	}

	got2 := collectProjection(t, g, []store.NodeID{id2}, []string{"digest"})
	if v := got2[[2]int{0, 0}]; len(v) != 2 {
		t.Fatalf("got %v, want both the prefix and the longer value", v)
	}
}

// The dedup is keyed on (key, value), not on the value alone.
//
// One entity can carry the same bytes under two keys — a content digest indexed
// under both its own name and a generic one is the ordinary way that happens —
// and those are two entries, not one. A dedup that compared values without their
// key would return the first and silently drop the second, which is a missing
// field rather than a duplicate and so much harder to notice.
func TestProjection_TheDedupIsKeyedOnTheKeyAsWellAsTheValue(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		id, err := e.g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatal(err)
		}
		shared := []byte("the-same-bytes-under-two-keys")
		for _, k := range []string{"digest", "alt"} {
			if err := e.g.IndexNodeProperty(id, k, shared); err != nil {
				t.Fatal(err)
			}
		}
		e.settle(t)

		got := collectProjection(t, e.g, []store.NodeID{id}, []string{"digest", "alt"})
		for k, key := range []string{"digest", "alt"} {
			vals := got[[2]int{0, k}]
			if len(vals) != 1 || vals[0] != string(shared) {
				t.Errorf("key %q: got %v, want one copy of %q", key, vals, shared)
			}
		}
	})
}

// A key nothing is indexed under is not an error, and the result it produces
// looks exactly like an answer.
//
// This is the hazard the key list exists for, asserted rather than described.
// The projection comes back one entry per id, positioned to the keys asked for,
// full length, with a nil error -- and every value in it is absent. A caller who
// checks the error and reads the positions cannot tell this from a store where
// those ids simply carry nothing, and the two want very different responses.
//
// It is deliberate that this is not an error. The property index accepts any key
// handed to IndexNodeProperty, so it has no notion of a wrong one, and a pass
// that legitimately finds nothing must not fail either. What was missing is any
// way to ask.
func TestProjection_AMistypedKeyIsSilentAndTheKeyListIsWhatCatchesIt(t *testing.T) {
	projBackends(t, func(t *testing.T, e *projEnv) {
		keys := []string{"digest"}
		ids := projFixture(t, e.g, 8, keys)
		e.settle(t)

		typo := []string{"digset"}

		// The silence, both halves of it.
		n := 0
		if err := e.g.ForEachNodeProjection(ids, typo, func(int, int, []byte) bool {
			n++
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeProjection over an unindexed key: %v", err)
		}
		if n != 0 {
			t.Errorf("an unindexed key produced %d callbacks", n)
		}

		got, err := e.g.GetNodesProjected(ids, typo)
		if err != nil {
			t.Fatalf("GetNodesProjected over an unindexed key: %v", err)
		}
		if len(got) != len(ids) {
			t.Fatalf("got %d projections, want %d", len(got), len(ids))
		}
		for i := range got {
			if len(got[i].Values) != len(typo) {
				t.Fatalf("projection %d has %d value slots, want %d: the result is positioned "+
					"by the request and stays so", i, len(got[i].Values), len(typo))
			}
			if got[i].Values[0] != nil {
				t.Errorf("projection %d found values under a key nothing was indexed under: %q",
					i, got[i].Values[0])
			}
		}

		// And the discriminator. Absent from the list is the answer a validator
		// acts on; present is only "may match".
		known, ok := e.g.NodePropKeys()
		if !ok {
			t.Fatal("NodePropKeys reports nothing on a backend that implements store.Projector")
		}
		if !slices.IsSorted(known) {
			t.Errorf("NodePropKeys is unsorted, so BinarySearch on it is wrong: %q", known)
		}
		if _, found := slices.BinarySearch(known, typo[0]); found {
			t.Errorf("the key list names %q, which nothing was indexed under: %q", typo[0], known)
		}
		if _, found := slices.BinarySearch(known, keys[0]); !found {
			t.Errorf("the key list omits %q, which every id is indexed under: %q", keys[0], known)
		}
	})
}

// The declared set and the indexed set are different questions, and only one of
// them validates a projection.
//
// A key declared and never written is enforced and empty: it belongs in
// Declarations and not in the key list, because a projection over it matches
// nothing. A key indexed without a declaration is the reverse. Validating a
// projection against the declarations would therefore reject working keys and
// accept dead ones, which is why store.PropertyKeyLister is a separate thing
// from the declaration accessors rather than a view over them.
func TestPropKeys_IsTheIndexedSetAndNotTheDeclaredOne(t *testing.T) {
	g := openDisk(t)

	// Declared, never written.
	if err := g.DeclareOrderedProperty("declared-empty"); err != nil {
		t.Fatalf("DeclareOrderedProperty: %v", err)
	}
	// Written, never declared.
	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.IndexNodeProperty(id, "indexed-undeclared", []byte("v")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}

	known, ok := g.NodePropKeys()
	if !ok {
		t.Fatal("NodePropKeys reports nothing on a disk-backed graph")
	}
	if slices.Contains(known, "declared-empty") {
		t.Errorf("the key list names a declared key nothing is indexed under: %q", known)
	}
	if !slices.Contains(known, "indexed-undeclared") {
		t.Errorf("the key list omits an indexed key because it was never declared: %q", known)
	}

	cat, ok := g.Declarations()
	if !ok {
		t.Fatal("Declarations reports nothing on a disk-backed graph")
	}
	if !slices.Contains(cat.OrderedNodeKeys, "declared-empty") {
		t.Errorf("the catalogue omits the declared key: %q", cat.OrderedNodeKeys)
	}
	if slices.Contains(cat.OrderedNodeKeys, "indexed-undeclared") {
		t.Errorf("the catalogue names a key that was only ever indexed: %q", cat.OrderedNodeKeys)
	}
}
