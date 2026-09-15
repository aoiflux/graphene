package graphene_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Byte-aware batch reads.
//
// GetNodes resolves every id it is given, which is right for a few hundred and
// wrong for half a million: the size of the answer is set by the caller's id
// slice and the store's blobs, and nothing in between gets a say. These tests
// pin the three rules that make the bounded form usable — the budget is
// honoured, a call always makes progress, and resuming from rest sees every
// record exactly once in request order — and pin them on both backends and on
// both sides of a compaction, because the disk backend answers a batch from the
// image without the store lock and from the delta with it, and those are two
// code paths that must not disagree.

// boundedFixture writes one node per entry in sizes, each carrying a payload of
// that many bytes, and returns their ids in the order written.
func boundedFixture(t *testing.T, g *graphene.Graph, sizes []int) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, len(sizes))
	for i, sz := range sizes {
		id, err := g.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: bytes.Repeat([]byte{byte(i + 1)}, sz),
		})
		if err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// payloadBytes is what the budget counts: the sum of the payloads, and nothing
// about the structs holding them.
func payloadBytes(ns []*store.Node) int64 {
	var total int64
	for _, n := range ns {
		total += int64(len(n.Properties))
	}
	return total
}

// boundedEnv is a graph plus the residency this arm is testing it in.
type boundedEnv struct {
	g       *graphene.Graph
	compact bool
}

// settle moves everything written so far into the residency under test.
//
// Every test calls it after its writes and before its reads, because on disk
// the two residencies are two implementations: records in the image are read
// without the store lock and records in the delta are read with it, and an
// assertion that only ever runs against one of them is only half a test. The
// budget arithmetic in particular lives in both, so a mutation to either copy
// has to fail something.
func (e *boundedEnv) settle(t *testing.T) {
	t.Helper()
	if !e.compact {
		return
	}
	if err := e.g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
}

// boundedBackends runs fn against every backend and residency a bounded read
// can be served from.
func boundedBackends(t *testing.T, fn func(t *testing.T, e *boundedEnv)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		fn(t, &boundedEnv{g: graphene.NewInMemory()})
	})
	t.Run("disk_delta", func(t *testing.T) {
		fn(t, &boundedEnv{g: openDisk(t)})
	})
	t.Run("disk_image", func(t *testing.T) {
		fn(t, &boundedEnv{g: openDisk(t), compact: true})
	})
}

// A batch stops before the record that would take it past the budget, not
// after: the bound is a ceiling, not a target to overshoot by one blob.
func TestBounded_StopsBeforeTheRecordThatWouldOverrun(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{100, 100, 100, 100})
		e.settle(t)

		found, missing, rest, err := g.GetNodesBounded(ids, 250)
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatalf("missing: got %v, want none", missing)
		}
		if len(found) != 2 {
			t.Fatalf("found: got %d records, want 2", len(found))
		}
		if got := payloadBytes(found); got > 250 {
			t.Errorf("payload: got %d bytes, over the 250 budget", got)
		}
		if len(rest) != 2 || rest[0] != ids[2] {
			t.Errorf("rest: got %v, want the last two of %v", rest, ids)
		}
	})
}

// A budget the batch fills exactly is not exceeded and is not under-used: the
// comparison is on what the next record would take the total to, so a record
// landing on the budget to the byte still belongs in the batch.
func TestBounded_ABudgetFilledExactlyIsNotWasted(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{100, 100, 100})
		e.settle(t)

		found, _, rest, err := g.GetNodesBounded(ids, 200)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 2 {
			t.Fatalf("found: got %d records for a budget of exactly two payloads, want 2", len(found))
		}
		if got := payloadBytes(found); got != 200 {
			t.Errorf("payload: got %d bytes, want the budget filled exactly", got)
		}
		if len(rest) != 1 {
			t.Errorf("rest: got %v, want the last id", rest)
		}
	})
}

// The one rule that keeps a resume loop from hanging: a call consumes at least
// one id, so a record whose payload alone exceeds the budget is returned on its
// own rather than refused forever.
func TestBounded_ARecordLargerThanTheBudgetIsReturnedAlone(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{10, 4096, 10})
		e.settle(t)

		found, _, rest, err := g.GetNodesBounded(ids[1:], 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 1 {
			t.Fatalf("found: got %d records, want the oversized one alone", len(found))
		}
		if len(found[0].Properties) != 4096 {
			t.Errorf("payload: got %d bytes, want the 4096-byte record", len(found[0].Properties))
		}
		if len(rest) != 1 || rest[0] != ids[2] {
			t.Errorf("rest: got %v, want %v", rest, ids[2:])
		}
	})
}

// The same rule stated as the thing it protects: a loop over records that each
// exceed the budget terminates, and terminates in one pass.
func TestBounded_ResumeLoopTerminatesWhenNothingFits(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{1024, 1024, 1024, 1024, 1024})
		e.settle(t)

		seen := 0
		rest := ids
		for calls := 0; len(rest) > 0; calls++ {
			if calls > len(ids) {
				t.Fatalf("resume loop ran %d times for %d ids: it is not making progress", calls, len(ids))
			}
			found, _, next, err := g.GetNodesBounded(rest, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(found) != 1 {
				t.Fatalf("call %d: got %d records, want exactly one", calls, len(found))
			}
			seen += len(found)
			rest = next
		}
		if seen != len(ids) {
			t.Errorf("saw %d records, want %d", seen, len(ids))
		}
	})
}

// A budget of zero needs no special case and gets none — nothing fits beside the
// first record — so a caller whose arithmetic produced a zero gets
// safe-and-slow rather than the unbounded batch it was avoiding.
func TestBounded_ZeroAndNegativeBudgetsYieldOneRecord(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{8, 8, 8})
		e.settle(t)

		for _, budget := range []int64{0, -1, -1 << 40} {
			found, _, rest, err := g.GetNodesBounded(ids, budget)
			if err != nil {
				t.Fatal(err)
			}
			if len(found) != 1 {
				t.Errorf("budget %d: got %d records, want 1", budget, len(found))
			}
			if len(rest) != 2 {
				t.Errorf("budget %d: rest has %d ids, want 2", budget, len(rest))
			}
		}
	})
}

// A missing id consumes its place and is charged nothing, so a run of deleted
// ids neither stalls the walk nor eats the budget.
func TestBounded_MissingIDsAdvanceTheWalkWithoutSpendingBudget(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{100, 100, 100, 100})
		e.settle(t)
		for _, id := range ids[1:3] {
			if err := g.DeleteNode(id); err != nil {
				t.Fatal(err)
			}
		}

		found, missing, rest, err := g.GetNodesBounded(ids, 250)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 2 {
			t.Fatalf("found: got %d records, want both survivors", len(found))
		}
		if len(missing) != 2 || missing[0] != ids[1] || missing[1] != ids[2] {
			t.Errorf("missing: got %v, want %v", missing, ids[1:3])
		}
		if len(rest) != 0 {
			t.Errorf("rest: got %v, want none — the batch reached every id", rest)
		}
	})
}

// Resuming from rest until it empties sees every live record exactly once, in
// the order the ids were asked for. This is the property a caller replaces
// GetNodes with, so it is the one that has to hold across the seam between
// batches rather than only within one.
func TestBounded_ResumeSeesEveryRecordInRequestOrder(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		sizes := []int{10, 900, 30, 4000, 50, 60, 700, 80}
		ids := boundedFixture(t, g, sizes)

		want, _, err := g.GetNodes(ids)
		if err != nil {
			t.Fatal(err)
		}

		var got []*store.Node
		rest := ids
		for calls := 0; len(rest) > 0; calls++ {
			if calls > len(ids) {
				t.Fatalf("resume loop is not making progress")
			}
			found, missing, next, err := g.GetNodesBounded(rest, 1000)
			if err != nil {
				t.Fatal(err)
			}
			if len(missing) != 0 {
				t.Fatalf("unexpected missing %v", missing)
			}
			if len(found) == 0 {
				t.Fatalf("call %d returned nothing with %d ids left", calls, len(rest))
			}
			// The bound holds per batch, with the documented exemption for a
			// batch whose first record is itself over budget.
			if b := payloadBytes(found); b > 1000 && len(found) != 1 {
				t.Errorf("call %d: %d payload bytes over a 1000 budget across %d records", calls, b, len(found))
			}
			got = append(got, found...)
			rest = next
		}

		if len(got) != len(want) {
			t.Fatalf("resumed batches yielded %d records, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Fatalf("record %d: got id %d, want %d — request order is not preserved across batches", i, got[i].ID, want[i].ID)
			}
			if !bytes.Equal(got[i].Properties, want[i].Properties) {
				t.Errorf("record %d: payload differs from the one GetNodes returns", i)
			}
		}
	})
}

// collectBounded drains ids through the bounded read at the given budget and
// returns the ids it saw, failing if the loop stops making progress.
func collectBounded(t *testing.T, g *graphene.Graph, ids []store.NodeID, budget int64, when string) []store.NodeID {
	t.Helper()
	var out []store.NodeID
	rest := ids
	for calls := 0; len(rest) > 0; calls++ {
		if calls > len(ids) {
			t.Fatalf("%s: resume loop is not making progress", when)
		}
		found, missing, next, err := g.GetNodesBounded(rest, budget)
		if err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		if len(missing) != 0 {
			t.Fatalf("%s: unexpected missing %v", when, missing)
		}
		for _, n := range found {
			out = append(out, n.ID)
		}
		rest = next
	}
	return out
}

// On disk a batch is served from the image without the store lock and from the
// delta with it. A store holding some of each exercises the seam: the image
// pass abandons on the first id it cannot resolve and the whole window is redone
// under the lock, which must produce the same answer as either side alone.
func TestBounded_DiskAnswersAcrossTheImageDeltaSeam(t *testing.T) {
	g := openDisk(t)

	inImage := boundedFixture(t, g, []int{100, 100, 100})
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	inDelta := boundedFixture(t, g, []int{100, 100, 100})

	mixed := []store.NodeID{
		inImage[0], inDelta[0], inImage[1], inDelta[1], inImage[2], inDelta[2],
	}

	want, missing, err := g.GetNodes(mixed)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 || len(want) != len(mixed) {
		t.Fatalf("GetNodes: %d found, %d missing, want all %d", len(want), len(missing), len(mixed))
	}

	got := collectBounded(t, g, mixed, 250, "across the seam")
	if len(got) != len(want) {
		t.Fatalf("got %d records across the seam, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i].ID {
			t.Errorf("record %d: got id %d, want %d", i, got[i], want[i].ID)
		}
	}
}

// A compaction moves every record from the delta into the image, which is the
// other side of the same seam: the bounded read must answer identically before
// and after one.
func TestBounded_AnswersTheSameBeforeAndAfterCompaction(t *testing.T) {
	g := openDisk(t)
	ids := boundedFixture(t, g, []int{10, 900, 30, 4000, 50})

	before := collectBounded(t, g, ids, 1000, "delta-resident")
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := collectBounded(t, g, ids, 1000, "image-resident")

	if len(before) != len(after) {
		t.Fatalf("compaction changed the record count: %d then %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("record %d: id %d before compaction, %d after", i, before[i], after[i])
		}
	}
}

// --- ForEachNodeBatch ---

// The loop form delivers every record, never hands the callback an empty batch,
// and holds the bound on each one.
func TestForEachNodeBatch_DeliversEveryRecordWithinTheBound(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{10, 900, 30, 400, 50, 60, 700, 80})
		e.settle(t)

		var seen []store.NodeID
		batches := 0
		missing, err := g.ForEachNodeBatch(ids, 1000, func(batch []*store.Node) bool {
			batches++
			if len(batch) == 0 {
				t.Error("callback received an empty batch")
			}
			if b := payloadBytes(batch); b > 1000 && len(batch) != 1 {
				t.Errorf("batch %d: %d payload bytes over a 1000 budget across %d records", batches, b, len(batch))
			}
			for _, n := range batch {
				seen = append(seen, n.ID)
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatalf("missing: got %v, want none", missing)
		}
		if batches < 2 {
			t.Fatalf("got %d batches for 2230 payload bytes at a 1000 budget, want at least 2", batches)
		}
		if len(seen) != len(ids) {
			t.Fatalf("saw %d records, want %d", len(seen), len(ids))
		}
		for i := range ids {
			if seen[i] != ids[i] {
				t.Fatalf("record %d: got id %d, want %d", i, seen[i], ids[i])
			}
		}
	})
}

// A callback that returns false stops the walk without an error, and the ids it
// never reached stay unexamined — so a short missing is not a claim that the
// rest exist.
func TestForEachNodeBatch_StopsWhenTheCallbackDeclines(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{100, 100, 100, 100, 100, 100})
		e.settle(t)

		batches := 0
		missing, err := g.ForEachNodeBatch(ids, 250, func([]*store.Node) bool {
			batches++
			return false
		})
		if err != nil {
			t.Fatal(err)
		}
		if batches != 1 {
			t.Errorf("callback ran %d times after declining once", batches)
		}
		if len(missing) != 0 {
			t.Errorf("missing: got %v, want none", missing)
		}
	})
}

// Cancellation lands between batches: a batch already resolved is delivered
// whole rather than torn up, and the next one is not started.
func TestForEachNodeBatchCtx_CancellationIsCheckedBetweenBatches(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := boundedFixture(t, g, []int{100, 100, 100, 100, 100, 100})
		e.settle(t)

		ctx, cancel := context.WithCancel(context.Background())
		batches := 0
		_, err := g.ForEachNodeBatchCtx(ctx, ids, 250, func(batch []*store.Node) bool {
			batches++
			if len(batch) != 2 {
				t.Errorf("batch %d: got %d records, want 2 — a cancelled batch was truncated", batches, len(batch))
			}
			cancel()
			return true
		})
		if err != ctx.Err() {
			t.Fatalf("err: got %v, want %v", err, ctx.Err())
		}
		if batches != 1 {
			t.Errorf("ran %d batches after cancelling during the first", batches)
		}
	})
}

// A context already dead when the walk starts stops it before the first batch.
func TestForEachNodeBatchCtx_AlreadyCancelledRunsNothing(t *testing.T) {
	g := graphene.NewInMemory()
	ids := boundedFixture(t, g, []int{100, 100})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ran := false
	_, err := g.ForEachNodeBatchCtx(ctx, ids, 1<<20, func([]*store.Node) bool {
		ran = true
		return true
	})
	if err != ctx.Err() {
		t.Fatalf("err: got %v, want %v", err, ctx.Err())
	}
	if ran {
		t.Error("callback ran under an already-cancelled context")
	}
}

// An empty id slice is not an error and does not call back.
func TestBounded_EmptyInput(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		found, missing, rest, err := g.GetNodesBounded(nil, 1<<20)
		if err != nil || len(found) != 0 || len(missing) != 0 || len(rest) != 0 {
			t.Fatalf("GetNodesBounded(nil): %v %v %v %v", found, missing, rest, err)
		}
		ran := false
		miss, err := g.ForEachNodeBatch(nil, 1<<20, func([]*store.Node) bool { ran = true; return true })
		if err != nil || len(miss) != 0 || ran {
			t.Fatalf("ForEachNodeBatch(nil): missing=%v ran=%v err=%v", miss, ran, err)
		}
	})
}

// --- Edges ---

// edgeFixture writes one edge per entry in sizes between two fresh nodes, each
// carrying a payload of that many bytes, and returns their ids in order.
func edgeFixture(t *testing.T, g *graphene.Graph, sizes []int) []store.EdgeID {
	t.Helper()
	ends := boundedFixture(t, g, []int{0, 0})
	ids := make([]store.EdgeID, 0, len(sizes))
	for i, sz := range sizes {
		id, err := g.AddEdge(&store.Edge{
			Src:        ends[0],
			Dst:        ends[1],
			Labels:     []store.EdgeType{store.EdgeTypeContains},
			Properties: bytes.Repeat([]byte{byte(i + 1)}, sz),
		})
		if err != nil {
			t.Fatalf("AddEdge %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func edgePayloadBytes(es []*store.Edge) int64 {
	var total int64
	for _, e := range es {
		total += int64(len(e.Properties))
	}
	return total
}

// The edge budget is a second copy of the node budget, in four functions rather
// than two — the image and delta paths, times the two entity kinds. A copy is
// where an off-by-one hides, so the arithmetic is pinned on the edge side by the
// same two cases that pin it on the node side rather than by the loose "did the
// order survive" check that a paging test gives it for free.
func TestBoundedEdges_TheBudgetIsTheSameArithmeticAsForNodes(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := edgeFixture(t, g, []int{100, 100, 100, 100})
		e.settle(t)

		// Stops before the record that would overrun.
		found, missing, rest, err := g.GetEdgesBounded(ids, 250)
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatalf("missing: got %v, want none", missing)
		}
		if len(found) != 2 {
			t.Fatalf("found: got %d edges, want 2", len(found))
		}
		if b := edgePayloadBytes(found); b > 250 {
			t.Errorf("payload: got %d bytes, over the 250 budget", b)
		}
		if len(rest) != 2 || rest[0] != ids[2] {
			t.Errorf("rest: got %v, want the last two of %v", rest, ids)
		}

		// A budget filled to the byte is filled, not wasted.
		found, _, rest, err = g.GetEdgesBounded(ids, 200)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 2 || edgePayloadBytes(found) != 200 {
			t.Errorf("exact fit: got %d edges and %d bytes, want 2 and 200",
				len(found), edgePayloadBytes(found))
		}
		if len(rest) != 2 {
			t.Errorf("exact fit: rest has %d ids, want 2", len(rest))
		}
	})
}

// The progress rule, on the edge side: an edge whose payload alone exceeds the
// budget comes back on its own, and a loop over such edges terminates.
func TestBoundedEdges_AnEdgeLargerThanTheBudgetIsReturnedAlone(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := edgeFixture(t, g, []int{4096, 4096, 4096})
		e.settle(t)

		seen := 0
		rest := ids
		for calls := 0; len(rest) > 0; calls++ {
			if calls > len(ids) {
				t.Fatalf("resume loop ran %d times for %d ids: it is not making progress", calls, len(ids))
			}
			found, _, next, err := g.GetEdgesBounded(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			if len(found) != 1 {
				t.Fatalf("call %d: got %d edges, want exactly one", calls, len(found))
			}
			if len(found[0].Properties) != 4096 {
				t.Errorf("call %d: payload %d bytes, want 4096", calls, len(found[0].Properties))
			}
			seen++
			rest = next
		}
		if seen != len(ids) {
			t.Errorf("saw %d edges, want %d", seen, len(ids))
		}
	})
}

// A missing edge id advances the walk and is charged nothing, as a missing node
// id is.
func TestBoundedEdges_MissingIDsAdvanceTheWalkWithoutSpendingBudget(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := edgeFixture(t, g, []int{100, 100, 100, 100})
		for _, id := range ids[1:3] {
			if err := g.DeleteEdge(id); err != nil {
				t.Fatal(err)
			}
		}
		e.settle(t)

		found, missing, rest, err := g.GetEdgesBounded(ids, 250)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 2 {
			t.Fatalf("found: got %d edges, want both survivors", len(found))
		}
		if len(missing) != 2 || missing[0] != ids[1] || missing[1] != ids[2] {
			t.Errorf("missing: got %v, want %v", missing, ids[1:3])
		}
		if len(rest) != 0 {
			t.Errorf("rest: got %v, want none — the batch reached every id", rest)
		}
	})
}

// Edges carry payloads too, and the edge form is the node form with the types
// changed — so it gets the two rules that matter: the bound holds, and a resume
// loop sees everything in order.
func TestBoundedEdges_ResumeSeesEveryEdgeInRequestOrder(t *testing.T) {
	boundedBackends(t, func(t *testing.T, e *boundedEnv) {
		g := e.g
		ids := edgeFixture(t, g, []int{10, 900, 30, 400, 50})
		e.settle(t)

		var got []store.EdgeID
		batches := 0
		missing, err := g.ForEachEdgeBatch(ids, 1000, func(batch []*store.Edge) bool {
			batches++
			b := edgePayloadBytes(batch)
			for _, edge := range batch {
				got = append(got, edge.ID)
			}
			if b > 1000 && len(batch) != 1 {
				t.Errorf("batch %d: %d payload bytes over a 1000 budget", batches, b)
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(missing) != 0 {
			t.Fatalf("missing: got %v, want none", missing)
		}
		if len(got) != len(ids) {
			t.Fatalf("saw %d edges, want %d", len(got), len(ids))
		}
		for i := range ids {
			if got[i] != ids[i] {
				t.Fatalf("edge %d: got id %d, want %d", i, got[i], ids[i])
			}
		}
	})
}

// --- The stall guard ---

// stallingStore is a store whose bounded read consumes nothing: it returns the
// ids it was given, unchanged, as the remainder. Both bundled backends promise
// not to do this; the guard exists for the ones that are not in this repository.
type stallingStore struct {
	store.GraphStore
}

func (s *stallingStore) GetNodesBatchBounded(ids []store.NodeID, _ int64) ([]*store.Node, []store.NodeID, []store.NodeID) {
	return nil, nil, ids
}

func (s *stallingStore) GetEdgesBatchBounded(ids []store.EdgeID, _ int64) ([]*store.Edge, []store.EdgeID, []store.EdgeID) {
	return nil, nil, ids
}

// A paging loop over a store that never advances is a spin with no output and no
// error. It is named and returned instead.
func TestForEachBatch_AStoreThatConsumesNothingIsAnErrorNotASpin(t *testing.T) {
	g := graphene.NewInMemory()
	ids := boundedFixture(t, g, []int{10, 10, 10})
	g.GraphStore = &stallingStore{GraphStore: g.GraphStore}

	ran := false
	_, err := g.ForEachNodeBatch(ids, 1<<20, func([]*store.Node) bool { ran = true; return true })
	if !errors.Is(err, graphene.ErrBatchStalled) {
		t.Fatalf("nodes: got %v, want ErrBatchStalled", err)
	}
	if ran {
		t.Error("callback ran for a batch that resolved nothing")
	}

	_, err = g.ForEachEdgeBatch([]store.EdgeID{1, 2}, 1<<20, func([]*store.Edge) bool { return true })
	if !errors.Is(err, graphene.ErrBatchStalled) {
		t.Fatalf("edges: got %v, want ErrBatchStalled", err)
	}
}
