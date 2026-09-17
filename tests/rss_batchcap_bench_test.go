// What one write batch actually holds, against what the cap charges it.
//
// Options.MaxBatchBytes is a figure in the delta's own accounting -- the same
// nodeVersionBytes the delta sums, which is why BatchTooLargeError.Bytes and
// MaxDeltaBytes are comparable. What the cap is *for* is the memory one call
// holds while it runs. Those are not the same number, and until this benchmark
// nothing had measured the ratio between them.
//
// It matters because the cap is derived rather than set: batchBudgetPercent of
// the memory budget becomes MaxBatchBytes, so a caller under a 2 GiB ceiling gets
// a cap without writing a figure down. Choosing that percent means knowing what a
// batch at the cap will hold, and the constant shipped with its own comment
// admitting the figure was chosen to be obviously safe rather than right. This is
// item 0c of docs/PLAN_BOUNDED_INGEST.md, and this is the measurement.
//
// # Three figures, because one of them alone is misleading
//
// churn is the total the allocator handed out across the call. It is what B/op
// already reports and it is *not* what a batch holds: a buffer that grows by
// doubling hands out about twice its final size, so churn counts every
// superseded copy as though all of them were live at once.
//
// peak is the largest live heap seen while the call ran, sampled. This is the
// figure the cap has to be chosen against, because it is the one that decides
// whether the process fits.
//
// retained is the live heap after the call and a collection: what the delta and
// the index went on holding. peak minus retained is the transient the call
// needed and gave back.
//
// The first version of this benchmark reported churn alone and called it an
// upper bound on the peak, on the reasoning that nothing AddNodesBatch allocates
// is collectable before it returns. That reasoning is wrong in one place and it
// is the place that dominates: the WAL frame is built by appending, so the
// intermediate buffers are garbage the moment they are superseded, inside the
// call. On the fat shape churn reads 6.4x and the peak is a good deal less. A
// figure that overstates by a factor nobody has measured is not a bound, it is a
// guess with a direction, so all three are reported and the percent is chosen
// from the peak.
//
// # Sampling, and what it costs
//
// The peak comes from a goroutine reading runtime.MemStats on a short interval.
// ReadMemStats stops the world, so this makes the call slower and the wall clock
// in this benchmark should not be read as a throughput figure -- it is a memory
// instrument and the timings beside it are the instrument's, not the engine's.
//
// A sampled peak can miss a spike between two samples, which is the one
// direction this figure can be wrong in. The interval is short against the
// shortest arm here (tens of thousands of samples over the call), and the
// allocation being measured is a steady accumulation rather than a spike, so the
// exposure is small -- but it is real, and it is why churn is reported beside it
// rather than thrown away. The two together bracket the answer.
//
// # The caller's own slice is not counted, deliberately
//
// The batch is built before the timer starts. A caller holding a million
// *store.Node values holds them whether or not it passes them to this engine, and
// no cap here can bound that -- the plan says so in its own words: "a
// ten-million-element slice is an argument". What the cap governs, and what this
// measures, is what the engine adds on top.
//
//	go test ./tests/ -tags=stress -run=^$ -bench=BatchCapRatio -benchtime=1x -count=1

//go:build stress

package graphene_test

import (
	"bytes"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// batchCapShapes are the two ends of the range the engine is aimed at, plus the
// degenerate one the two-dimensional cap exists for.
//
// The thin shape is what most of this suite runs at. The fat shape is the
// programme's target, 3.2 KB a record. The empty one carries no payload at all,
// which is the case a byte cap cannot bound on its own and is why there is a
// record cap beside it -- so it belongs here even though no real store looks
// like it.
var batchCapShapes = []struct {
	name  string
	blob  int
	nodes int
}{
	{"Empty", 0, 50_000},
	{"Thin256B", 256, 50_000},
	{"Fat3200B", 3200, 25_000},
}

// batchCapIndexed is how many index entries each record carries in the indexed
// arm: the audited consumer's declaration shape, which is what MEMORY_MODEL's
// multiplier formula is written against.
const batchCapIndexed = 4

func BenchmarkBatchCapRatio(b *testing.B) {
	for _, shape := range batchCapShapes {
		for _, indexed := range []bool{false, true} {
			name := shape.name
			if indexed {
				name += "/Indexed"
			} else {
				name += "/Bare"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				measureBatchCapRatio(b, shape.blob, shape.nodes, indexed)
			})
		}
	}
}

// measureBatchCapRatio adds one batch to an empty store and reports the ratio.
//
// One batch, into a store with nothing in it, because the question is what one
// call holds and not what an ingest accumulates. A second batch would be measured
// against a delta that already has a map to grow and a WAL file already extended,
// and would report the marginal cost of the second rather than the cost of the
// first -- and it is the first that the cap has to be able to admit.
// heapPeakSampler tracks the largest live heap seen between start and stop.
//
// A goroutine rather than a timer callback so that the sampling stops
// deterministically: stop waits for the sampler to return, so the figure read
// afterwards cannot be a partial write and no sample can land in the next arm.
type heapPeakSampler struct {
	peak atomic.Uint64
	done chan struct{}
	quit chan struct{}
}

func startHeapPeak(every time.Duration) *heapPeakSampler {
	h := &heapPeakSampler{done: make(chan struct{}), quit: make(chan struct{})}
	go func() {
		defer close(h.done)
		var ms runtime.MemStats
		for {
			select {
			case <-h.quit:
				return
			default:
			}
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > h.peak.Load() {
				h.peak.Store(ms.HeapAlloc)
			}
			time.Sleep(every)
		}
	}()
	return h
}

func (h *heapPeakSampler) stop() uint64 {
	close(h.quit)
	<-h.done
	return h.peak.Load()
}

// measureBatchCapRatio adds one batch to an empty store and reports the ratios.
//
// One batch, into a store with nothing in it, because the question is what one
// call holds and not what an ingest accumulates. A second batch would be measured
// against a delta that already has a map to grow and a WAL file already extended,
// and would report the marginal cost of the second rather than the cost of the
// first -- and it is the first that the cap has to be able to admit.
func measureBatchCapRatio(b *testing.B, blob, nodes int, indexed bool) {
	b.Helper()
	g, err := graphene.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	payload := bytes.Repeat([]byte{0xA5}, blob)
	batch := make([]*store.Node, nodes)
	for i := range batch {
		batch[i] = &store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: payload,
		}
	}

	before, ok := g.StorageStats()
	if !ok {
		b.Skip("this backend does not report StorageStats, so there is no charged figure to divide by")
	}

	// The batch and the fixture are settled out of the reading first. What is
	// being measured is the rise above the caller's own slice, which is the term
	// no cap here can bound.
	var m0, m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	base := m0.HeapAlloc

	sampler := startHeapPeak(50 * time.Microsecond)
	ids, err := g.AddNodes(batch)
	if err != nil {
		sampler.stop()
		b.Fatalf("AddNodes of %d: %v", nodes, err)
	}
	if indexed {
		// Indexing is a separate call and a separate cap, and it is included as
		// its own arm because the delta's accounting does not see index entries
		// at all -- which is M2 in the plan, the 5.6x between what MaxDeltaBytes
		// watches and what a write holds. A ratio taken on the bare arm alone
		// would be the same under-report one level down.
		for i, id := range ids {
			for k := range batchCapIndexed {
				key := fmt.Sprintf("k%d", k)
				if err := g.IndexNodeProperty(id, key, []byte(fmt.Sprintf("%s-%09d", key, i))); err != nil {
					sampler.stop()
					b.Fatalf("IndexNodeProperty: %v", err)
				}
			}
		}
	}
	peak := sampler.stop()

	runtime.ReadMemStats(&m1)
	runtime.GC()
	runtime.ReadMemStats(&m2)
	after, _ := g.StorageStats()

	accounted := after.DeltaBytes - before.DeltaBytes
	if accounted <= 0 {
		b.Fatalf("the delta accounted %d bytes for %d records; the ratio has no denominator", accounted, nodes)
	}
	churn := int64(m1.TotalAlloc - m0.TotalAlloc)
	rise := int64(0)
	if peak > base {
		rise = int64(peak - base)
	}
	retained := int64(0)
	if m2.HeapAlloc > base {
		retained = int64(m2.HeapAlloc - base)
	}

	b.ReportMetric(float64(rise)/float64(accounted), "peak/charged")
	b.ReportMetric(float64(retained)/float64(accounted), "retained/charged")
	b.ReportMetric(float64(churn)/float64(accounted), "churn/charged")
	b.ReportMetric(float64(accounted)/float64(nodes), "charged-B/node")
	b.Logf("%d nodes, blob %d, indexed %v: charged %d B; peak %d B (%.2fx), retained %d B (%.2fx), churn %d B (%.2fx)",
		nodes, blob, indexed, accounted,
		rise, float64(rise)/float64(accounted),
		retained, float64(retained)/float64(accounted),
		churn, float64(churn)/float64(accounted))
}
