// Residency and allocation for the compaction a store spends its life doing.
//
// BenchmarkRSS_Compact next door compacts a store that has never been compacted,
// so every record it merges arrives from the delta and the image half of the
// plan is empty. That is the first compaction of a store's life and no other
// one. Every later compaction merges a large image with a comparatively small
// delta, and there the plan's cost is dominated by what it copies out of an
// image that is already immutable — so that is the shape any change to the copy
// has to be measured on.
//
// Allocation is reported beside residency because it is the less noisy of the
// two by a wide margin. A polled residency peak is a floor on the truth and
// carries the variance of the whole runtime; TotalAlloc across the call is exact
// and attributable. Where the two disagree the allocation figure is the one to
// trust about what the code asked for, and the residency figure the one to trust
// about what the machine had to find.

//go:build stress

package graphene_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
)

// BenchmarkRSS_CompactIncremental compacts a store that already holds an image.
//
// The delta is one per cent of the live set on purpose. A rebuild writes far
// more than that, but a small delta is what isolates the image term: whatever
// the compaction spends here, it spends on records it did not need to touch.
func BenchmarkRSS_CompactIncremental(b *testing.B) {
	dir := rssMutableFixtureDir(b, rssNodes, rssBlob)

	g, err := graphene.Open(dir)
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
	b.ReportMetric(live, "liveRecords")
	b.ReportMetric(float64(m1.TotalAlloc-m0.TotalAlloc), "compactAllocB")
	b.ReportMetric(float64(m1.TotalAlloc-m0.TotalAlloc)/live, "compactAllocBPerRecord")
	b.ReportMetric(float64(m1.Mallocs-m0.Mallocs), "compactAllocs")
	b.ReportMetric(float64(peak.Total)/bytesPerMiB, "compactPeakMiB")
	if peak.Total > before.Total {
		b.ReportMetric(float64(peak.Total-before.Total)/bytesPerMiB, "compactDeltaMiB")
	}
	b.ReportMetric(float64(samples), "samples")
	// Explicit, because reportRSS zeroes ns/op: a residency figure taken over one
	// iteration is not a rate, and suppressing it is right. The program still
	// reports wall clock for every change, so the compaction times itself.
	b.ReportMetric(float64(elapsed.Milliseconds()), "compactMs")
	b.Logf("before compaction: rss %.2f MiB, heap objects %.2f MiB",
		float64(before.Total)/bytesPerMiB, float64(beforeHeap.Objects)/bytesPerMiB)
	reportRSS(b, g)
}
