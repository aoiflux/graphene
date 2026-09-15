// What bounding a batch read costs and what it saves.
//
// The saving is not allocation churn and it is not wall clock. Both arms below
// resolve the same records and allocate the same struct per record on the way
// past, so B/op and allocs/op are expected to agree; what differs is how many
// of those records are alive at once. GetNodes materialises the whole answer
// before the caller sees any of it, so its peak is set by the id slice; the
// bounded walk's peak is set by the budget. That is a difference only a live-heap
// or residency reading can see, which is why the arms that matter here are the
// RSS ones and the throughput arms exist to show that nothing was paid for it.
//
// Each arm is its own function. The two differ in what is live when the sample
// is taken, and a fixture that branched between them inside one function would
// hold the branch's own bindings alive across both — the same trap
// TECHNICAL_DETAILS.md §14.27 records for the owned-upsert arms, in its
// residency form rather than its escape-analysis form.
//
//	go test ./tests/ -tags=stress -run=^$ -bench=RSS_Batch -benchtime=1x -count=1
//	go test ./tests/ -tags=stress -run=^$ -bench=BatchWalk -benchtime=10x -count=6

//go:build stress

package graphene_test

import (
	"bytes"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

var (
	boundedBenchNodes  = envIntDefault("GRAPHENE_BOUNDED_NODES", 200_000)
	boundedBenchBlob   = envIntDefault("GRAPHENE_BOUNDED_BLOB", 512)
	boundedBenchBudget = int64(envIntDefault("GRAPHENE_BOUNDED_BUDGET", 8<<20))
)

// boundedBenchSink absorbs the payload lengths so neither arm's work can be
// optimised away, and so neither arm retains a record the other does not.
var boundedBenchSink int64

// boundedBenchStore builds a disk store of uniform-payload nodes, compacts it,
// and returns it **reopened**, with every id in write order.
//
// The reopen is load-bearing and was not obvious. Compacting inside a handle
// leaves the image it just wrote on the Go heap — StorageStats reports
// ImageMode=heap right after a Compact and ImageMode=mapped after the next Open,
// because the image is mapped on the load path and a commit does not remap in
// place. A fixture that built, compacted and measured would therefore report the
// heap-image path under the name of the mapped one, and the payload bytes would
// be counted as anonymous memory that a reopened store never charges. This is
// the same shape as R11's finding for the index term, on the image side.
//
// Compacted, because that is the residency the read path is being sized for: the
// records are in the image, their payloads alias the mapping, and the batch is
// served without the store lock. A delta-resident fixture would measure the
// locked path and a different allocation story.
func boundedBenchStore(tb testing.TB) (*graphene.Graph, []store.NodeID) {
	tb.Helper()

	dir := tb.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		tb.Fatal(err)
	}

	blob := bytes.Repeat([]byte{0xA5}, boundedBenchBlob)
	ids := make([]store.NodeID, 0, boundedBenchNodes)
	const perTx = 2000
	for start := 0; start < boundedBenchNodes; start += perTx {
		end := min(start+perTx, boundedBenchNodes)
		batch := make([]*store.Node, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, &store.Node{
				Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
				Properties: blob,
			})
		}
		got, err := g.AddNodes(batch)
		if err != nil {
			tb.Fatal(err)
		}
		ids = append(ids, got...)
	}
	if err := g.Compact(); err != nil {
		tb.Fatal(err)
	}
	if err := g.Close(); err != nil {
		tb.Fatal(err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { reopened.Close() })
	if st, ok := reopened.StorageStats(); ok && st.ImageMappedBytes == 0 {
		tb.Logf("image is not mapped (mode %v): these figures describe the heap-image path", st.ImageMode)
	}
	return reopened, ids
}

// BenchmarkRSS_BatchUnbounded resolves every id in one call and holds the answer,
// which is what a caller that writes GetNodes(ids) does.
func BenchmarkRSS_BatchUnbounded(b *testing.B) {
	g, ids := boundedBenchStore(b)
	b.ResetTimer()

	found, missing, err := g.GetNodes(ids)
	if err != nil {
		b.Fatal(err)
	}
	if len(missing) != 0 || len(found) != len(ids) {
		b.Fatalf("resolved %d of %d, %d missing", len(found), len(ids), len(missing))
	}
	for _, n := range found {
		boundedBenchSink += int64(len(n.Properties))
	}

	b.StopTimer()
	b.ReportMetric(float64(len(found)), "recordsLive")
	reportRSS(b, found)
}

// BenchmarkRSS_BatchBounded walks the same ids through a budget, sampling while
// one window is live.
//
// The sample is taken on a window in the middle of the pass rather than the
// first or last: the first is taken before the runtime has settled into the
// pass's allocation rate, and the last is the one the loop is about to drop.
// Only the sampled window is passed to reportRSS, so what the reading describes
// is a caller holding one batch and nothing else.
func BenchmarkRSS_BatchBounded(b *testing.B) {
	g, ids := boundedBenchStore(b)
	b.ResetTimer()

	// Windows are uniform here, so the count is known ahead of the walk and the
	// sample point with it.
	perWindow := int(boundedBenchBudget) / boundedBenchBlob
	sampleAt := len(ids) / perWindow / 2

	var (
		window  []*store.Node
		batches int
		total   int
	)
	missing, err := g.ForEachNodeBatch(ids, boundedBenchBudget, func(batch []*store.Node) bool {
		total += len(batch)
		for _, n := range batch {
			boundedBenchSink += int64(len(n.Properties))
		}
		if batches == sampleAt {
			window = batch
		}
		batches++
		return true
	})
	if err != nil {
		b.Fatal(err)
	}
	if len(missing) != 0 || total != len(ids) {
		b.Fatalf("walked %d of %d, %d missing", total, len(ids), len(missing))
	}
	if window == nil {
		b.Fatalf("no window sampled: %d batches, wanted to sample %d", batches, sampleAt)
	}

	b.StopTimer()
	b.ReportMetric(float64(batches), "batches")
	b.ReportMetric(float64(len(window)), "recordsLive")
	reportRSS(b, window)
}

// BenchmarkBatchWalkUnbounded is the throughput arm for GetNodes: one call, every
// record, the caller holding all of them until it is done.
func BenchmarkBatchWalkUnbounded(b *testing.B) {
	g, ids := boundedBenchStore(b)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		found, _, err := g.GetNodes(ids)
		if err != nil {
			b.Fatal(err)
		}
		for _, n := range found {
			boundedBenchSink += int64(len(n.Properties))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(ids)), "records")
}

// BenchmarkBatchWalkBounded is the same work through the budget, so the cost of
// paging it can be read against the arm above rather than guessed at.
func BenchmarkBatchWalkBounded(b *testing.B) {
	g, ids := boundedBenchStore(b)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_, err := g.ForEachNodeBatch(ids, boundedBenchBudget, func(batch []*store.Node) bool {
			for _, n := range batch {
				boundedBenchSink += int64(len(n.Properties))
			}
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(ids)), "records")
}
