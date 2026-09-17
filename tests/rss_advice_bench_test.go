// What telling the kernel about the mapping is worth, in the class it moves.
//
// Options.ResidentAdvice does not touch anonymous memory, so every figure this
// programme has reported up to now is the wrong one to judge it by. What it
// moves is the file-backed column: the resident page cache of the mapped image,
// which a compaction makes resident in its entirety by reading every record the
// delta did not touch, and which nothing afterwards gives back.
//
// So the arm is a compaction and the figure is fileMiB after one. anonMiB is
// reported beside it as a control: a change that moved the anonymous class would
// be a change doing something other than what it says.
//
// # This benchmark reports nothing on windows, and that is the finding
//
// Windows takes its access-pattern hint when a file is opened rather than
// against a live mapping, and has no equivalent of dropping a range. So the
// advice is a no-op there, the engine says so through store.MetricResidentAdvice
// rather than silently, and the windows answer to this problem is a different
// mechanism with a different trade -- disk.LimitWorkingSet. The arm still runs
// there, and it should show two identical columns.

//go:build stress

package graphene_test

import (
	"runtime"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
)

// BenchmarkRSS_AdviceCompaction compacts a store that already holds an image,
// once with the advice and once without.
//
// Interleaving is the caller's job here, as it is for every other A/B in this
// repository: run the two sub-benchmarks in alternation rather than back to
// back. docs/benchmarks.md records what happens otherwise, twice.
func BenchmarkRSS_AdviceCompaction(b *testing.B) {
	for _, on := range []bool{false, true} {
		name := "Off"
		if on {
			name = "On"
		}
		b.Run(name, func(b *testing.B) {
			benchAdviceCompaction(b, on)
		})
	}
}

func benchAdviceCompaction(b *testing.B, advice bool) {
	dir := rssMutableFixtureDir(b, rssNodes, rssBlob)

	g, err := graphene.OpenWithOptions(dir, disk.Options{ResidentAdvice: advice})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	added := rssNodes / 100
	if added < 1 {
		added = 1
	}
	rssWriteNodes(b, g, rssNodes, added, rssBlob)

	before, _ := settledRSS(g)

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	b.ResetTimer()
	var compactErr error
	peak, samples := samplePeakDuring(func() { compactErr = g.Compact() })
	b.StopTimer()

	runtime.ReadMemStats(&m1)
	if compactErr != nil {
		b.Fatalf("Compact: %v", compactErr)
	}
	if samples == 0 {
		b.Fatal("sampler took no readings; the peak figure would be meaningless")
	}

	// The figure the option is about, and the one it must not have moved.
	if before.Split {
		b.ReportMetric(float64(before.File)/bytesPerMiB, "beforeFileMiB")
		b.ReportMetric(float64(before.Anon)/bytesPerMiB, "beforeAnonMiB")
	}
	b.ReportMetric(float64(peak.Total)/bytesPerMiB, "compactPeakMiB")
	b.ReportMetric(float64(m1.Mallocs-m0.Mallocs), "compactAllocs")
	b.ReportMetric(float64(samples), "samples")

	// reportRSS supplies anonMiB, fileMiB, rssMiB and peakMiB after the
	// compaction, which is where the two arms are compared.
	reportRSS(b, g)
}
