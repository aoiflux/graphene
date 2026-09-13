//go:build stress

package graphene_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// What a property join costs, both ways.
//
// This is the measurement R3e(c) exists to produce, and the two arms are the two
// things a consumer can write against the same store:
//
//	Loop   NodesByProperty once per value  — correct, and what everyone writes
//	Batch  NodesByPropertyBatch            — the same answers, resolved in order
//
// The fixture's "digest" key is 32 raw bytes, distinct for every node, which is
// the shape this is for and the worst case the declarations allow: nothing can be
// interned, no two values share a run, and the value table is as wide as the node
// count. A join over it is the pass the plan describes as "a cold random
// 1.5M-lookup pass (minutes)".
//
// Both arms are measured over a store reopened from disk, with the same values in
// the same order, and both report residency — because the reason the batch is
// shaped as a callback is memory, and a batch that were faster while allocating a
// result per value would have missed the point of the exercise.
//
//	GRAPHENE_RSS_NODES=50000 go test -tags=stress ./tests/ -run '^$' -bench PropertyBatch -benchtime 1x

// batchQueryValues returns nHit digests drawn from the fixture and nMiss that are
// not in it, shuffled, which is what a caller's own table looks like.
//
// The misses matter as much as the hits. A join is usually partial, and a value
// the index does not hold still costs a search — so an arm measured on hits alone
// would report the easy half.
func batchQueryValues(nodes, nHit, nMiss int, shuffle bool) [][]byte {
	rng := rand.New(rand.NewSource(20260913))
	out := make([][]byte, 0, nHit+nMiss)

	digestOf := func(i int) []byte {
		var d [32]byte
		binary.BigEndian.PutUint64(d[0:], uint64(i))
		binary.BigEndian.PutUint64(d[8:], uint64(i)*0x9e3779b97f4a7c15)
		binary.BigEndian.PutUint64(d[16:], ^uint64(i))
		binary.BigEndian.PutUint64(d[24:], uint64(i)<<21|uint64(i)>>11)
		return d[:]
	}
	// Hits spread across the whole key rather than a contiguous run, so the sweep
	// is not handed an artificially short distance to travel.
	for i := 0; i < nHit; i++ {
		out = append(out, digestOf(rng.Intn(nodes)))
	}
	// Misses built the same way from indices above the fixture's range, so they
	// interleave with the hits in value order rather than sorting to one end.
	for i := 0; i < nMiss; i++ {
		out = append(out, digestOf(nodes+1+rng.Intn(nodes)))
	}
	if shuffle {
		rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	}
	return out
}

func BenchmarkRSS_PropertyBatch(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)

	// Half hits, half misses: a join that matched everything would be a lookup
	// benchmark, and one that matched nothing would measure only the search.
	values := batchQueryValues(rssNodes, rssNodes/2, rssNodes/2, true)

	for _, arm := range []struct {
		name string
		run  func(g *graphene.Graph, values [][]byte) (int, error)
	}{
		{"Loop", func(g *graphene.Graph, values [][]byte) (int, error) {
			hits := 0
			for _, v := range values {
				ids, err := g.NodesByProperty("digest", v)
				if err != nil {
					return hits, err
				}
				hits += len(ids)
			}
			return hits, nil
		}},
		{"Batch", func(g *graphene.Graph, values [][]byte) (int, error) {
			hits := 0
			err := g.NodesByPropertyBatchCtx(context.Background(), "digest", values,
				func(_ int, ids []store.NodeID) bool {
					hits += len(ids)
					return true
				})
			return hits, err
		}},
	} {
		b.Run(arm.name, func(b *testing.B) {
			g, err := graphene.Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = g.Close() }()

			// Warm the open out of the measurement: what is being compared is the
			// join, not the decode that precedes it.
			if _, err := g.NodesByProperty("digest", values[0]); err != nil {
				b.Fatal(err)
			}
			base, _ := settledRSS(nil)

			var hits int
			var elapsed time.Duration
			var runErr error
			b.ResetTimer()
			peak, samples := samplePeakDuring(func() {
				started := time.Now()
				for i := 0; i < b.N; i++ {
					hits, runErr = arm.run(g, values)
					if runErr != nil {
						return
					}
				}
				elapsed = time.Since(started)
			})
			b.StopTimer()
			if runErr != nil {
				b.Fatalf("%s: %v", arm.name, runErr)
			}
			// The two arms must agree on the answer, or the comparison is between
			// two different questions. Reported rather than asserted against a
			// constant, because the figure depends on the fixture size.
			b.ReportMetric(float64(hits), "hits")
			b.ReportMetric(float64(len(values)), "values")
			reportJoinPeak(b, elapsed, peak, base, samples)
		})
	}
}

// BenchmarkRSS_PropertyBatchSorted is the same join with the caller's values
// already in order, which is the shape an external table usually arrives in.
//
// It exists to separate two costs the first benchmark adds together: the ordering
// the batch builds, and the sweep it buys. If sorted and unsorted are close, the
// sort is not where the time goes; if they are far apart, a caller that can supply
// sorted values should be told so in the documentation.
func BenchmarkRSS_PropertyBatchSorted(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	unsorted := batchQueryValues(rssNodes, rssNodes/2, rssNodes/2, true)
	sorted := batchQueryValues(rssNodes, rssNodes/2, rssNodes/2, false)
	sortValues(sorted)

	for _, arm := range []struct {
		name   string
		values [][]byte
	}{{"Unsorted", unsorted}, {"Sorted", sorted}} {
		b.Run(arm.name, func(b *testing.B) {
			g, err := graphene.Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			if _, err := g.NodesByProperty("digest", arm.values[0]); err != nil {
				b.Fatal(err)
			}
			base, _ := settledRSS(nil)

			hits := 0
			var elapsed time.Duration
			b.ResetTimer()
			peak, samples := samplePeakDuring(func() {
				started := time.Now()
				for i := 0; i < b.N; i++ {
					hits = 0
					if err := g.NodesByPropertyBatch("digest", arm.values,
						func(_ int, ids []store.NodeID) bool {
							hits += len(ids)
							return true
						}); err != nil {
						b.Error(err)
						return
					}
				}
				elapsed = time.Since(started)
			})
			b.StopTimer()
			b.ReportMetric(float64(hits), "hits")
			reportJoinPeak(b, elapsed, peak, base, samples)
		})
	}
}

// reportJoinPeak reports one arm's wall clock and its residency.
//
// The sampler ticks every millisecond, so a join shorter than a tick can return
// no samples at all. At the sizes this file is written for that does not happen;
// at a small size it does, and a benchmark that fails on a small fixture is a
// worse instrument than one that says what it could not see. So no samples after
// a join too short to have been sampled reports samples=0 and omits the peak,
// while no samples after a join long enough to have been sampled hundreds of
// times means the sampler is not running, and that is still fatal.
func reportJoinPeak(b *testing.B, elapsed time.Duration, peak, base rssSample, samples int) {
	b.ReportMetric(float64(elapsed.Milliseconds())/float64(b.N), "join_ms")
	b.ReportMetric(float64(samples), "samples")
	if samples == 0 {
		if elapsed > 50*time.Millisecond {
			b.Fatalf("no samples over %s; the sampler is not running", elapsed)
		}
		b.Logf("the join took %s, shorter than the sampler can see; no peak reported", elapsed)
		return
	}
	b.ReportMetric(float64(peak.Total)/bytesPerMiB, "peakMiB")
	if peak.Total > base.Total {
		b.ReportMetric(float64(peak.Total-base.Total)/bytesPerMiB, "deltaMiB")
	}
	if peak.Split {
		b.ReportMetric(float64(peak.Anon)/bytesPerMiB, "peakAnonMiB")
		b.ReportMetric(float64(peak.File)/bytesPerMiB, "peakFileMiB")
	}
}

// sortValues orders a value list ascending, which is what the batch would
// otherwise do for itself.
func sortValues(values [][]byte) {
	slices.SortFunc(values, bytes.Compare)
}
