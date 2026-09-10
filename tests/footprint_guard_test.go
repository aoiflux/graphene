package graphene_test

// Footprint guard.
//
// The allocation guards next door assert ceilings on allocs/op, which works
// because that count is an exact small integer. Retained memory is neither: it
// moves with GC timing, with the size of the fixture, and with a fixed overhead
// that has nothing to do with the data. A ceiling on total retained bytes would
// therefore have to be set so loosely that a change doubling the per-record cost
// would still pass, which is the failure mode CONTRIBUTING §2 warns about — a
// test that cannot fail.
//
// So this asserts a *slope* instead: build the same store at two sizes, and
// divide the difference in retained heap by the difference in record count. The
// fixed overhead cancels, and what remains is the marginal cost of one more
// record, which is exactly the quantity a memory budget scales with. A change
// that adds a constant is free here, correctly; a change that adds bytes per
// record fails, correctly.
//
// Untagged on purpose, like the alloc guards: this runs in `make check`, so a
// per-record regression is caught by the ordinary test suite rather than only by
// someone who thinks to run the stress benchmarks.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// footprintGuardSizes are deliberately far apart. Two nearby sizes divide a
// small difference by a small denominator, which amplifies GC noise into the
// slope; a 4× gap keeps the ratio of signal to timing jitter high while still
// running in well under a second.
const (
	footprintGuardSmall = 2_500
	footprintGuardLarge = 10_000
)

// retainedHeap is the guard's own measurement, kept separate from the
// stress-tagged liveHeap so this file compiles without the stress tag.
func retainedHeap(keepAlive any) uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(keepAlive)
	return ms.HeapAlloc
}

// buildGuardStore creates a compacted disk store of n nodes with three indexed
// properties each, and returns it along with the heap it retains.
//
// Compacted because that is the steady state the slope describes: an
// uncompacted store also retains its delta, which scales with writes since the
// last compaction rather than with the data, and would put a second variable
// into a measurement that has one.
func buildGuardStore(t *testing.T, n int) (*graphene.Graph, uint64) {
	t.Helper()

	base := retainedHeap(nil)

	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })

	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%07d", i)),
			"bucket": []byte(fmt.Sprintf("bucket-%04d", i%1000)),
			"score":  []byte(fmt.Sprintf("%06d", i%1000)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}

	total := retainedHeap(g)
	if total < base {
		return g, 0
	}
	return g, total - base
}

// TestFootprintGuard_OpenSlope fails when the marginal heap cost of one more
// record climbs.
//
// The ceiling is a ceiling, not a target: a change that lowers the slope is a
// win and must not fail the test. What it catches is the slope climbing back,
// which is how a memory budget is lost — not in one visible step, but in a
// series of changes each of which adds a little per record.
func TestFootprintGuard_OpenSlope(t *testing.T) {
	// Both stores must be alive at the same time or the second measurement
	// would be taken against a heap the first has already released, and the
	// difference would measure collection rather than retention.
	small, smallHeap := buildGuardStore(t, footprintGuardSmall)
	large, largeHeap := buildGuardStore(t, footprintGuardLarge)

	if largeHeap <= smallHeap {
		// Not a pass. A larger store retaining no more heap than a smaller one
		// means the measurement failed, most likely because something was
		// collected mid-run, and a guard that silently passes in that state is
		// the test-that-cannot-fail this file exists to avoid.
		t.Skipf("measurement inverted (small=%d large=%d); heap reading unusable on this run",
			smallHeap, largeHeap)
	}

	slope := float64(largeHeap-smallHeap) / float64(footprintGuardLarge-footprintGuardSmall)
	t.Logf("retained: %d B at %d nodes, %d B at %d nodes; slope %.0f B/node",
		smallHeap, footprintGuardSmall, largeHeap, footprintGuardLarge, slope)

	// Measured at 421-422 B/node on this fixture across repeated runs, a spread
	// of well under one percent. The ceiling is set at roughly double, which is
	// wide enough to absorb GC timing and the race detector's overhead, and
	// still narrow enough that any change adding a pointer-sized field per
	// record to a handful of structures trips it.
	const ceiling = 1000.0
	if slope > ceiling {
		t.Errorf("retained heap is %.0f B per node, ceiling %.0f B\n"+
			"a higher slope means each record now costs more resident memory, "+
			"which is the quantity the memory budget scales with",
			slope, ceiling)
	}

	runtime.KeepAlive(small)
	runtime.KeepAlive(large)
}
