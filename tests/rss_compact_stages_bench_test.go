// Which stage of a compaction raises the process peak.
//
// docs/PLAN_BOUNDED_INGEST.md gates Phase 4 -- a write-only compaction that
// never builds a CSRGraph -- on 0a confirming that the build is the stage the
// transient belongs to. The three stage metrics already carry the reading that
// answers it, and this is the benchmark that reads them.
//
// # Why the peak and not the current figure
//
// store.MetricCompactPin's Bytes is the process's peak, which is monotone: it
// never falls, so the rise between two boundaries is exactly what happened
// between them, with none of the variance a polled sample carries. The current
// anonymous figure is reported beside it and is the noisier of the two -- it is
// whatever the runtime had not yet returned at the instant the boundary was
// crossed -- so where they disagree the peak is the one to read.
//
// The pin boundary is the baseline: at that point the plan has copied the delta
// and nothing has been built. Everything above it is the build and the commit.

//go:build stress

package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// rssStageNodes is deliberately larger than rssNodes. The term Phase 4 would
// remove is ~118 B per node and ~142 B per edge, so at the 50,000 the other RSS
// benchmarks use it is a few megabytes -- inside the runtime's own noise. This
// is sized so the term is larger than what it is being distinguished from.
var rssStageNodes = envIntDefault("GRAPHENE_RSS_STAGE_NODES", 400_000)

// BenchmarkRSS_CompactStages attributes a compaction's peak to one of its three
// stages.
func BenchmarkRSS_CompactStages(b *testing.B) {
	dir := rssMutableFixtureDir(b, rssStageNodes, rssBlob)

	sk := &sink{}
	g, err := graphene.OpenWithOptions(dir, disk.Options{Metrics: sk})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	added := rssStageNodes / 100
	if added < 1 {
		added = 1
	}
	rssWriteNodes(b, g, rssStageNodes, added, rssBlob)

	before, _ := settledRSS(g)

	b.ResetTimer()
	if err := g.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	b.StopTimer()

	// One of each, in order. A compaction that was refused emits the pin alone,
	// which would make every rise below a subtraction from nothing -- so the
	// count is checked rather than the slice indexed hopefully.
	stages := []struct {
		name string
		kind store.MetricKind
	}{
		{"pin", store.MetricCompactPin},
		{"build", store.MetricCompactBuild},
		{"commit", store.MetricCompactCommit},
	}
	peaks := make(map[string]int64, len(stages))
	for _, st := range stages {
		got := sk.of(st.kind)
		if len(got) != 1 {
			b.Fatalf("%s: got %d metrics, want exactly 1", st.name, len(got))
		}
		m := got[0]
		peaks[st.name] = m.Bytes
		b.ReportMetric(float64(m.Bytes)/bytesPerMiB, st.name+"PeakMiB")
		b.ReportMetric(float64(m.Examined)/bytesPerMiB, st.name+"AnonMiB")
		b.ReportMetric(float64(m.Count)/bytesPerMiB, st.name+"EstMiB")
	}

	// The two rises, which are the whole point. A negative rise is impossible
	// from a monotone counter and would mean the platform is not reporting one,
	// so it is reported as zero rather than as a negative megabyte count.
	buildRise := peaks["build"] - peaks["pin"]
	commitRise := peaks["commit"] - peaks["build"]
	if buildRise < 0 {
		buildRise = 0
	}
	if commitRise < 0 {
		commitRise = 0
	}
	b.ReportMetric(float64(buildRise)/bytesPerMiB, "buildRiseMiB")
	b.ReportMetric(float64(commitRise)/bytesPerMiB, "commitRiseMiB")
	b.ReportMetric(float64(rssStageNodes+added), "liveRecords")
	if n := rssStageNodes + added; n > 0 {
		b.ReportMetric(float64(buildRise)/float64(n), "buildRiseBPerRecord")
	}

	b.Logf("before compaction: rss %.2f MiB", float64(before.Total)/bytesPerMiB)
	reportRSS(b, g)
}
