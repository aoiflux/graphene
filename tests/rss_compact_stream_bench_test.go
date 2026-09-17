// What not building the graph is worth, in the class a compaction peaks in.
//
// Phase 4 of docs/PLAN_BOUNDED_INGEST.md takes the CSRGraph out of the
// compaction that CompactAndReopen runs, on the grounds that it is built,
// written, published and then thrown away by the Close on the next line. The
// figure it is aiming at is the peak DURING the compaction, which is the one
// figure Options.ResidentAdvice measurably did not move -- 0.994, recorded in
// docs/benchmarks.md -- and which the stage metrics attribute to the build:
// +197.9 MiB at the build boundary against +0.035 at the commit.
//
// # The two arms need no switch in the engine
//
// "Compact, then close and reopen" is what CompactAndReopen was before this, line
// for line: disk/compact.go's CompactAndReopenCtx is CompactCtx, Close,
// OpenWithOptions. So the materialising arm is written out longhand here and the
// streaming arm is the method, and the only thing that differs between them is
// which build ran. No option, no test hook, and nothing in the engine that exists
// to be measured.
//
// # Both arms are sampled across the compaction AND the reopen
//
// They have to be. CompactAndReopen is one call and cannot be sampled halfway
// through, so an arm that sampled Compact() alone would be compared against an
// arm that had a close and an open -- and a reopen parses the whole image it
// just wrote, which is the largest allocation either arm makes. The first run of
// this benchmark did exactly that and read a 1.38x allocation regression that was
// entirely the reopen being inside one window and outside the other.
//
// Compact-and-reopen is the unit a caller actually asks for, so it is the unit
// measured, and both arms do the same close and the same open.

//go:build stress

package graphene_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
)

// rssStreamNodes sizes the fixture. Larger than rssNodes for the same reason
// the stage benchmark's is: the term under measurement is ~118 bytes a node, and
// at 50,000 that is inside the runtime's own noise.
var rssStreamNodes = envIntDefault("GRAPHENE_RSS_STREAM_NODES", 400_000)

// BenchmarkRSS_CompactStream compacts-and-reopens a store that already holds an
// image, once by building the graph and once by streaming past it.
//
// Interleaving is the caller's job, as it is for every other A/B here: run the
// two sub-benchmarks in alternation rather than back to back. docs/benchmarks.md
// records what happens otherwise, twice.
func BenchmarkRSS_CompactStream(b *testing.B) {
	for _, stream := range []bool{false, true} {
		name := "Materialise"
		if stream {
			name = "Stream"
		}
		b.Run(name, func(b *testing.B) {
			benchCompactStream(b, stream)
		})
	}
}

func benchCompactStream(b *testing.B, stream bool) {
	dir := rssMutableFixtureDir(b, rssStreamNodes, rssBlob)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { g.Close() }()

	added := rssStreamNodes / 100
	if added < 1 {
		added = 1
	}
	rssWriteNodes(b, g, rssStreamNodes, added, rssBlob)

	before, _ := settledRSS(g)

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)

	b.ResetTimer()
	var (
		compactErr error
		reopened   *graphene.Graph
	)
	started := time.Now()
	peak, samples := samplePeakDuring(func() {
		if stream {
			reopened, compactErr = g.CompactAndReopen()
			return
		}
		// The materialising arm, longhand: exactly what CompactAndReopenCtx was
		// before Phase 4 -- CompactCtx, Close, OpenWithOptions -- so the graph is
		// built, written and published, and then dropped by the close.
		if compactErr = g.Compact(); compactErr != nil {
			return
		}
		if compactErr = g.Close(); compactErr != nil {
			return
		}
		reopened, compactErr = graphene.Open(dir)
	})
	elapsed := time.Since(started)
	b.StopTimer()

	runtime.ReadMemStats(&m1)
	if compactErr != nil {
		b.Fatalf("compact: %v", compactErr)
	}
	if samples == 0 {
		b.Fatal("sampler took no readings; the peak figure would be meaningless")
	}

	// Both arms closed the receiver inside the sampled region and handed back a
	// store opened on the same directory, so everything reported below is the
	// same thing measured the same way. The deferred close closes whichever
	// handle g names by then.
	g = reopened

	live := float64(rssStreamNodes + added)
	b.ReportMetric(live, "liveRecords")
	b.ReportMetric(float64(peak.Total)/bytesPerMiB, "compactPeakMiB")
	if peak.Total > before.Total {
		b.ReportMetric(float64(peak.Total-before.Total)/bytesPerMiB, "compactRiseMiB")
		b.ReportMetric(float64(peak.Total-before.Total)/live, "compactRiseBPerRecord")
	}
	if peak.Split {
		b.ReportMetric(float64(peak.Anon)/bytesPerMiB, "compactPeakAnonMiB")
	}
	b.ReportMetric(float64(m1.TotalAlloc-m0.TotalAlloc), "compactAllocB")
	b.ReportMetric(float64(m1.Mallocs-m0.Mallocs), "compactAllocs")
	b.ReportMetric(float64(samples), "samples")
	b.ReportMetric(float64(elapsed.Milliseconds()), "compactMs")
	b.Logf("before compaction: rss %.2f MiB", float64(before.Total)/bytesPerMiB)
	reportRSS(b, g)
}
