// What CompactOptions.MaxWorkingBytes actually buys, measured rather than argued.
//
// The figure divides into four intermediates a mapped-index compaction holds:
// the value table's in-memory cap, the reverse sort's chunk, and the two bounds
// on the merge that drains it. Each of the four decides whether something is
// held or written to a file in the store's directory and read back, so the
// figure trades memory against sequential IO and against nothing else — the
// image is byte-identical at every setting, which disk/compact_buffers_test.go
// asserts directly.
//
// Two things this is here to establish, both of which the plan that scheduled
// the option assumed without measuring.
//
// How large the term is. It is a constant, not a function of the store: 4.125
// MiB by default, whatever the store holds. Against a compaction whose working
// set docs/MEMORY_MODEL.md puts in the hundreds of megabytes, lowering it is not
// where a memory problem is solved, and the benchmark should say so plainly
// rather than leave a reader to infer it from a ratio.
//
// What the other direction costs. Raising it is the interesting one: the mapped
// index made compaction 15–24% slower, and some of that is the spill and the
// merge. Whether it is enough of it to matter is a measurement, and this is it.
//
// One setting per process, set by the environment, because a benchmark that
// builds a large store more than once in a process measures the heap it left
// behind as much as the work — the harness lesson §14.13 records. Interleave two
// processes to A/B a setting, per CONTRIBUTING §1.

//go:build stress

package graphene_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
)

// rssCompactWorkingBytes is CompactOptions.MaxWorkingBytes for this process.
// Zero, the default, is every prior version's behaviour.
var rssCompactWorkingBytes = int64(envIntDefault("GRAPHENE_RSS_COMPACT_WORKING_BYTES", 0))

// BenchmarkRSS_CompactWorkingBytes compacts an image-plus-delta store under one
// setting of the figure.
//
// The delta is the same one per cent BenchmarkRSS_CompactIncremental uses, for
// the same reason: it is the shape a store spends its life in, and it isolates
// what the compaction spends on records it did not have to touch.
func BenchmarkRSS_CompactWorkingBytes(b *testing.B) {
	dir := rssMutableFixtureDir(b, rssNodes, rssBlob)

	g, err := graphene.OpenWithOptions(dir, disk.Options{
		Compact: disk.CompactOptions{MaxWorkingBytes: rssCompactWorkingBytes},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	added := rssNodes / 100
	if added < 1 {
		added = 1
	}
	rssWriteNodes(b, g, rssNodes, added, rssBlob)

	before, beforeHeap := settledRSS(g)

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	b.ResetTimer()
	var compactErr error
	started := time.Now()
	peak, samples := samplePeakDuring(func() { compactErr = g.Compact() })
	elapsed := time.Since(started)
	b.StopTimer()

	runtime.ReadMemStats(&m1)
	if compactErr != nil {
		b.Fatalf("Compact: %v", compactErr)
	}
	if samples == 0 {
		b.Fatal("sampler took no readings; the peak figure would be meaningless")
	}

	live := float64(rssNodes + added)
	b.ReportMetric(float64(rssCompactWorkingBytes), "maxWorkingBytes")
	b.ReportMetric(live, "liveRecords")
	b.ReportMetric(float64(m1.TotalAlloc-m0.TotalAlloc), "compactAllocB")
	b.ReportMetric(float64(m1.Mallocs-m0.Mallocs), "compactAllocs")
	b.ReportMetric(float64(peak.Total)/bytesPerMiB, "compactPeakMiB")
	if peak.Total > before.Total {
		b.ReportMetric(float64(peak.Total-before.Total)/bytesPerMiB, "compactDeltaMiB")
	}
	b.ReportMetric(float64(samples), "samples")
	b.ReportMetric(float64(elapsed.Milliseconds()), "compactMs")
	b.Logf("MaxWorkingBytes=%d, before compaction: rss %.2f MiB, heap objects %.2f MiB",
		rssCompactWorkingBytes, float64(before.Total)/bytesPerMiB,
		float64(beforeHeap.Objects)/bytesPerMiB)
	reportRSS(b, g)
}
