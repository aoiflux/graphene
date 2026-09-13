package graphene_test

// Does the resident estimate track reality?
//
// N2's figure is arithmetic over counters, so it cannot be wrong about how many
// records there are; what it can be wrong about is what one costs. That is a
// question only a measurement answers, and this is the measurement.
//
// # Why the comparison is against retained heap and not against RSS
//
// The estimate says what it is: heap this process must keep. RSS is that plus the
// allocator's slack, its size-class rounding, the collector's headroom against
// GOGC, stacks, and the runtime's own metadata — on the one fixture measured for
// it, between 1.19x and 1.54x as much, varying across runs of an identical store.
// A band wide enough to contain that ratio would be wide enough to contain a
// doubling of the per-record cost, which is the test-that-cannot-fail
// CONTRIBUTING §2 warns about. So the estimate is compared against the quantity it
// claims to be, and the ratio to RSS is reported by the RSS benchmarks beside it
// rather than asserted here.
//
// # Why a slope and not a ceiling
//
// The same argument footprint_guard_test.go makes, and it applies with more force
// here. Both figures carry a fixed overhead that has nothing to do with the data —
// the store's own structures, the runtime's, the harness's — and comparing totals
// at one size compares those overheads as much as the model. Differencing two
// sizes cancels them and leaves the marginal cost of one record, measured two ways.
// If the model is right about what a record costs, the two slopes agree; if it is
// wrong, they diverge however well the totals happen to line up.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Three sizes, each four times the last. Far apart for the reason the footprint
// guard's two are: a small difference over a small denominator turns GC jitter
// into slope, and a 4x gap keeps the signal well above it while still running in
// about a second.
var estimateBandSizes = []int{2_000, 8_000, 32_000}

// estimateBandBlob is the payload per record.
//
// Non-zero deliberately, and large enough to matter. A fixture of bare records
// measures the model's per-record constants and nothing else; a fixture with
// blobs also measures whether the payload term is wired to the right place, which
// is where a resident estimate is most likely to be wrong by a large factor
// rather than a small one.
const estimateBandBlob = 256

// buildEstimateStore returns a compacted store of n records, the estimate it
// reports, and the heap it retains.
//
// Compacted, and then reopened, for two separate reasons. Compacted because the
// steady state is what the slope describes: an uncompacted store also holds its
// delta, which scales with writes since the last compaction rather than with the
// data. Reopened because a graph a compaction published reports the payload its
// records reference rather than the payload a loader allocated, and those differ
// for exactly as long as one process serves a freshly built image — see
// CSRGraph.payloadBytes.
//
// The fixture's own slices are dropped before either figure is taken. Without
// that, the ids and the node structs are still live at the measurement and land
// in the retained heap but not in the store's estimate, which would make the
// estimate look low by an amount proportional to n — a slope error introduced by
// the harness and attributed to the model.
func buildEstimateStore(t *testing.T, n int) (*graphene.Graph, int64, uint64) {
	t.Helper()

	dir := t.TempDir()
	func() {
		g, err := graphene.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()

		blob := make([]byte, estimateBandBlob)
		nodes := make([]*store.Node, n)
		for i := range nodes {
			nodes[i] = &store.Node{
				Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
				Properties: blob,
			}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			t.Fatal(err)
		}
		for i, id := range ids {
			err := g.IndexNodeProperties(id, map[string][]byte{
				"sha256": []byte(fmt.Sprintf("hash-%07d", i)),
				"bucket": []byte(fmt.Sprintf("bucket-%04d", i%1000)),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Compact(); err != nil {
			t.Fatal(err)
		}
	}()

	base := retainedHeap(nil)
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })

	st, ok := g.StorageStats()
	if !ok {
		t.Fatal("the disk backend does not report StorageStats")
	}
	heap := retainedHeap(g)
	if heap < base {
		heap = base // a collection between the two readings; treated as zero growth
	}
	return g, st.EstimatedResidentBytes, heap - base
}

// TestEstimatedResident_WithinBand is the N2 acceptance measurement: the model's
// marginal cost per record against the measured one.
//
// The band is wide and one-directional in intent. Over-reporting is the direction
// a figure a budget refuses on should be wrong in, so the upper bound is loose; the
// lower bound is the one that matters, because a model that understates the
// per-record cost is a model that will admit a workload that then gets killed.
func TestEstimatedResident_WithinBand(t *testing.T) {
	type point struct {
		n    int
		est  int64
		heap uint64
	}
	pts := make([]point, 0, len(estimateBandSizes))
	held := make([]*graphene.Graph, 0, len(estimateBandSizes))
	for _, n := range estimateBandSizes {
		// Every store stays open to the end of the test. Measuring the second
		// against a heap the first has already released would measure collection
		// rather than retention, which is the trap footprint_guard_test.go names.
		g, est, heap := buildEstimateStore(t, n)
		held = append(held, g)
		pts = append(pts, point{n: n, est: est, heap: heap})
		t.Logf("%6d records: estimate %9d B, retained heap %9d B (%.2fx)",
			n, est, heap, float64(heap)/float64(est))
	}

	first, last := pts[0], pts[len(pts)-1]
	dn := float64(last.n - first.n)
	estSlope := float64(last.est-first.est) / dn
	if last.heap <= first.heap {
		t.Skipf("measurement inverted (%d B at %d records, %d B at %d); heap reading unusable on this run",
			first.heap, first.n, last.heap, last.n)
	}
	heapSlope := float64(last.heap-first.heap) / dn
	ratio := estSlope / heapSlope
	t.Logf("slopes: estimate %.1f B/record, retained heap %.1f B/record, ratio %.2f",
		estSlope, heapSlope, ratio)

	// Measured at a ratio near 1 on this fixture. The band allows the model to be
	// half the truth or twice it before failing, which is loose enough to absorb
	// GC timing and the race detector and still tight enough that a term wired to
	// the wrong quantity — a payload counted once per key instead of once per
	// record, say — moves it out.
	const (
		low  = 0.5
		high = 2.0
	)
	if ratio < low || ratio > high {
		t.Errorf("the estimate's slope is %.1f B/record against a measured %.1f (ratio %.2f), "+
			"outside the band [%.1f, %.1f]\n"+
			"a ratio below the band means the model understates what a record costs, "+
			"which is the direction that admits a workload the machine then kills",
			estSlope, heapSlope, ratio, low, high)
	}

	// The estimate must also be a floor at every size, not only in slope. It
	// excludes allocator slack, size-class rounding and the runtime's own
	// metadata by construction, so a store whose estimate exceeds its retained
	// heap is double-counting something.
	for _, p := range pts {
		if p.est > int64(p.heap) {
			t.Errorf("%d records: the estimate is %d B against %d B of retained heap; "+
				"the estimate claims to be a floor on the heap and is above it",
				p.n, p.est, p.heap)
		}
	}

	for _, g := range held {
		runtime.KeepAlive(g)
	}
}
