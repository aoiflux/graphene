//go:build stress

package graphene_test

// What it costs to actually read the bytes of a mapped image.
//
//	GRAPHENE_RSS_NODES=100000 go test ./tests/ -tags=stress -run=^$ \
//	  -bench=RSS_BlobTouch -benchtime=1x -count=1
//
// # Why this instrument exists
//
// Every other read benchmark in the suite measures getting a record *back*, not
// reading its blob. That is the right shape for the engine's contract — reads
// alias out, so a caller that wants one field out of a 64 KiB blob pays nothing
// for the rest — and it is exactly the wrong shape for measuring what a mapping
// costs, because a blob nobody dereferences is a page nobody faults in.
//
// So the whole of the difference between a copied image and a mapped one would
// otherwise be invisible on the cost side: under a copy the bytes are read at
// open, in one sequential pass, whether or not anyone ever looks at them; under a
// mapping they are read lazily, per page, only for the pages someone touches.
// That is a large win for the read-only aggregate shape this program is aimed at,
// which resolves ten rows out of a million and a half — and it is close to a wash
// for a caller that walks everything, with the same bytes arriving as page faults
// instead of as one read.
//
// This benchmark is the second of those two shapes. It walks every record and
// touches one byte of every page of every blob, which is the most a mapping can
// be made to cost, and reports the wall clock for that walk beside the residency
// it leaves behind.
//
// It is not a cold-cache measurement and does not claim to be: the fixture was
// written moments earlier, so the pages are in the cache and what is measured is
// the minor fault, not a disk read. A genuinely cold cache would add the read the
// copying path performs at open, moved to where the bytes are used. Saying which
// of those is being measured matters more than the figure.

import (
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// blobTouchSink keeps the compiler from eliminating the reads, which are the
// entire subject of the benchmark.
var blobTouchSink byte

// BenchmarkRSS_BlobTouch opens a store and reads through every property blob it
// holds.
func BenchmarkRSS_BlobTouch(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	// The walk, timed on its own. Open is deliberately outside the timer: this
	// measures the cost of using the bytes, and BenchmarkRSS_Open already
	// measures the cost of acquiring them.
	b.ResetTimer()
	// The walk is timed explicitly as well as by the framework, because
	// reportRSS below sets ns/op to zero on purpose -- a residency benchmark's
	// per-operation time is meaningless when the operation is "open a store
	// once". The wall clock of the walk is the cost side of this item and has to
	// survive that.
	started := time.Now()
	var touched, bytesSeen int64
	for id := store.NodeID(1); id <= store.NodeID(rssNodes); id++ {
		n, err := g.GetNode(id)
		if err != nil {
			continue // a fixture hole is not this benchmark's subject
		}
		// One byte per 4 KiB, plus the last byte, so every page of the blob is
		// faulted in exactly once and the loop does not turn into a memcpy
		// benchmark.
		for off := 0; off < len(n.Properties); off += 4096 {
			blobTouchSink ^= n.Properties[off]
		}
		if len(n.Properties) > 0 {
			blobTouchSink ^= n.Properties[len(n.Properties)-1]
		}
		touched++
		bytesSeen += int64(len(n.Properties))
	}
	walk := time.Since(started)
	b.StopTimer()

	if touched == 0 {
		b.Fatal("the walk read no records")
	}
	b.ReportMetric(float64(walk.Nanoseconds())/1e6, "walkMs")
	b.ReportMetric(float64(walk.Nanoseconds())/float64(touched), "nsPerRecord")
	b.ReportMetric(float64(touched), "records")
	b.ReportMetric(float64(bytesSeen)/bytesPerMiB, "blobMiB")

	// Residency after the walk, which is the other half of the answer: touching
	// every page of a mapped image makes all of it resident, so this is the
	// mapped path's worst case against the copied path's only case.
	reportRSS(b, g)
}
