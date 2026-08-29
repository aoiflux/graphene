package index

// The residual pass's own cancellation contract, tested at this level rather
// than through a store.
//
// Both planners already drop a cancelled query's candidates before returning,
// so a test through a Graph cannot tell whether this layer returns them or not
// — a mutation that hands the partial set back survives every store-level
// assertion. The contract is still worth having: NarrowNodesByFiltersCtx is
// exported, its doc commits to returning nothing alongside the error, and the
// planner's guard is a second line of defence rather than the reason.

import (
	"context"
	"errors"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// narrowFixture registers n nodes under one shared value and one distinct value
// each, which is the shape a residual pass runs against: a wide driving set and
// a filter that has to look at every member of it.
func narrowFixture(t *testing.T, n int) (*PropertyIndex, []store.NodeID) {
	t.Helper()
	p := NewPropertyIndex()
	ids := make([]store.NodeID, n)
	for i := 0; i < n; i++ {
		id := store.NodeID(i + 1)
		ids[i] = id
		p.IndexNode(id, "bucket", []byte("b"))
		p.IndexNode(id, "seq", []byte{byte(i), byte(i >> 8)})
	}
	return p, ids
}

// trippingCtx reports "not cancelled" for a fixed number of Err calls and
// "cancelled" afterwards, so a test can choose which of the pass's several
// return paths the cancellation comes out of. A plain cancelled context only
// ever reaches the first one.
type trippingCtx struct {
	context.Context
	done      chan struct{}
	remaining int
}

func newTrippingCtx(clean int) *trippingCtx {
	return &trippingCtx{Context: context.Background(), done: make(chan struct{}), remaining: clean}
}

func (c *trippingCtx) Done() <-chan struct{} { return c.done }

func (c *trippingCtx) Err() error {
	c.remaining--
	if c.remaining < 0 {
		return context.Canceled
	}
	return nil
}

func TestNarrowNodesByFiltersCtx_ReturnsNoCandidatesWithTheError(t *testing.T) {
	p, ids := narrowFixture(t, 2000)

	filters := []store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b")},
		{Key: "seq", Op: store.PropertyOpContains, Value: []byte{0}},
	}

	// Every return path, not just the first. 0 is the check before the pass
	// starts; 1 is the one between plan steps; the rest are inside the loop
	// that applies a step. Each has its own return statement and each could
	// hand the partial set back independently.
	for _, clean := range []int{0, 1, 2, 3, 5} {
		candidates := append([]store.NodeID(nil), ids...)
		out, err := p.NarrowNodesByFiltersCtx(newTrippingCtx(clean), candidates, filters, store.FilterMask(0).Set(0))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("NarrowNodesByFiltersCtx cancelled after %d checks: want context.Canceled, got %v", clean, err)
		}
		// The partial set is a superset of the answer and is shaped exactly
		// like the answer, which is the one result worse than none.
		if out != nil {
			t.Errorf("cancelled after %d checks: returned %d candidates alongside the error", clean, len(out))
		}
	}
}

func TestNarrowEdgesByFiltersCtx_ReturnsNoCandidatesWithTheError(t *testing.T) {
	p := NewPropertyIndex()
	ids := make([]store.EdgeID, 2000)
	for i := range ids {
		id := store.EdgeID(i + 1)
		ids[i] = id
		p.IndexEdge(id, "bucket", []byte("b"))
		p.IndexEdge(id, "seq", []byte{byte(i), byte(i >> 8)})
	}

	filters := []store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b")},
		{Key: "seq", Op: store.PropertyOpContains, Value: []byte{0}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, err := p.NarrowEdgesByFiltersCtx(ctx, ids, filters, store.FilterMask(0).Set(0))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NarrowEdgesByFiltersCtx with a cancelled context: want context.Canceled, got %v", err)
	}
	if out != nil {
		t.Errorf("returned %d candidates alongside the cancellation", len(out))
	}
}

// The uncancelled call must still narrow, or the tests above would pass against
// an implementation that had stopped working.
func TestNarrowNodesByFiltersCtx_StillNarrowsWhenNotCancelled(t *testing.T) {
	p, ids := narrowFixture(t, 300)

	filters := []store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b")},
		{Key: "seq", Op: store.PropertyOpEqual, Value: []byte{7, 0}},
	}
	out, err := p.NarrowNodesByFiltersCtx(context.Background(), ids, filters, store.FilterMask(0).Set(0))
	if err != nil {
		t.Fatalf("NarrowNodesByFiltersCtx: %v", err)
	}
	if len(out) != 1 || out[0] != store.NodeID(8) {
		t.Fatalf("narrowed to %v, want exactly node 8", out)
	}
}
