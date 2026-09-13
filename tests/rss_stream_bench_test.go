//go:build stress

package graphene_test

// What an answer costs to deliver, against what it costs to hold.
//
// The arms are the two ways a caller can read every node under one indexed value:
//
//	Slice   NodesByProperty, then a loop  — correct, and what everyone writes
//	Stream  NodesByPropertyFunc           — the same ids, none of them held
//
// The fixture puts every node under one value of one key, which is the shape the
// saving is about: a tenant id, a type discriminator, a status flag. The run is as
// long as the store, so the slice the index builds to answer it is eight bytes per
// node, and the liveness filter reuses that array rather than making a second — so
// the slice arm's cost is one array of the answer's size and the stream arm's is
// one batch of five hundred ids, whatever the answer's size.
//
// B/op is the decisive figure here and RSS is the corroboration, not the other way
// round: an allocation this size is exact, reported per operation, and cannot be
// confused with a working set that has not settled. The residency arms are one per
// process because peakMiB is a process high-water mark.
//
//	go test -tags=stress ./tests/ -run '^$' -bench 'PropertyStream|PropertyRead|ForEachNodeID' -benchtime 1x

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

const (
	// One value over this many nodes: 4 MiB of ids, an order above the RSS
	// instrument's noise floor and unambiguous in B/op.
	streamNodes = 500_000
	streamChunk = 50_000
	streamKey   = "tenant"
)

var streamValue = []byte("t-0")

// streamBenchFixture builds a compacted store where every node carries the same
// value under streamKey, and returns its directory.
// streamFixtureDir is the built fixture, shared by every arm in this process.
// Not b.TempDir(), which is removed when the benchmark that asked for it ends.
var streamFixtureDir string

func streamBenchFixture(b *testing.B) string {
	b.Helper()
	if streamFixtureDir != "" {
		return streamFixtureDir
	}
	dir, err := os.MkdirTemp("", "graphene-stream-*")
	if err != nil {
		b.Fatal(err)
	}
	buildStreamFixture(b, dir)
	streamFixtureDir = dir
	return dir
}

func buildStreamFixture(b *testing.B, dir string) {
	b.Helper()
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	for start := 0; start < streamNodes; start += streamChunk {
		n := streamChunk
		if start+n > streamNodes {
			n = streamNodes - start
		}
		nodes := make([]*store.Node, n)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			b.Fatalf("AddNodes: %v", err)
		}
		for i, id := range ids {
			if err := g.IndexNodeProperties(id, map[string][]byte{
				streamKey: streamValue,
				"seq":     []byte(fmt.Sprintf("%08d", start+i)),
			}); err != nil {
				b.Fatalf("IndexNodeProperties: %v", err)
			}
		}
	}
	if err := g.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
}

func openStreamFixture(b *testing.B, dir string) *graphene.Graph {
	b.Helper()
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	return g
}

// --- what the answer costs to deliver ---

// BenchmarkPropertyRead_Slice reads the whole value as a slice and counts it. The
// counting is there so the answer cannot be optimised away, and so both arms do
// the same trivial thing with each id.
func BenchmarkPropertyRead_Slice(b *testing.B) {
	dir := streamBenchFixture(b)
	g := openStreamFixture(b, dir)
	defer g.Close()

	b.ReportAllocs()
	b.ResetTimer()
	seen := 0
	for i := 0; i < b.N; i++ {
		ids, err := g.NodesByProperty(streamKey, streamValue)
		if err != nil {
			b.Fatal(err)
		}
		for range ids {
			seen++
		}
	}
	b.StopTimer()
	if seen != streamNodes*b.N {
		b.Fatalf("read %d ids, want %d", seen, streamNodes*b.N)
	}
}

// BenchmarkPropertyRead_Stream reads the same value through the callback.
func BenchmarkPropertyRead_Stream(b *testing.B) {
	dir := streamBenchFixture(b)
	g := openStreamFixture(b, dir)
	defer g.Close()

	b.ReportAllocs()
	b.ResetTimer()
	seen := 0
	for i := 0; i < b.N; i++ {
		err := g.NodesByPropertyFunc(streamKey, streamValue, func(store.NodeID) bool {
			seen++
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if seen != streamNodes*b.N {
		b.Fatalf("read %d ids, want %d", seen, streamNodes*b.N)
	}
}

// BenchmarkForEachNodeID_Query is the query form of the same read, slice side.
func BenchmarkForEachNodeID_Query(b *testing.B) {
	dir := streamBenchFixture(b)
	g := openStreamFixture(b, dir)
	defer g.Close()

	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: streamKey, Op: store.PropertyOpEqual, Value: streamValue}}}

	b.ReportAllocs()
	b.ResetTimer()
	seen := 0
	for i := 0; i < b.N; i++ {
		ids, err := g.QueryNodeIDs(q)
		if err != nil {
			b.Fatal(err)
		}
		for range ids {
			seen++
		}
	}
	b.StopTimer()
	if seen != streamNodes*b.N {
		b.Fatalf("read %d ids, want %d", seen, streamNodes*b.N)
	}
}

// BenchmarkForEachNodeID_Streamed is the same query through ForEachNodeID, which
// takes the streamed path for this shape.
func BenchmarkForEachNodeID_Streamed(b *testing.B) {
	dir := streamBenchFixture(b)
	g := openStreamFixture(b, dir)
	defer g.Close()

	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: streamKey, Op: store.PropertyOpEqual, Value: streamValue}}}

	b.ReportAllocs()
	b.ResetTimer()
	seen := 0
	for i := 0; i < b.N; i++ {
		err := g.ForEachNodeID(q, func(store.NodeID) bool {
			seen++
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if seen != streamNodes*b.N {
		b.Fatalf("read %d ids, want %d", seen, streamNodes*b.N)
	}
}

// --- what the answer costs to hold ---

// streamResidency samples the process halfway through the pass, which is the only
// moment the two arms differ: the slice arm is holding every id it has not reached
// yet and every id it has, and the stream arm is holding one batch.
//
// Halfway rather than at the end because at the end the slice arm's array is dead
// and the difference is a question about the collector rather than about the API.
func streamResidency(b *testing.B, streamed bool) {
	dir := streamBenchFixture(b)

	var mid rssSample
	seen := 0
	for i := 0; i < b.N; i++ {
		g := openStreamFixture(b, dir)
		if streamed {
			half := 0
			err := g.NodesByPropertyFuncCtx(context.Background(), streamKey, streamValue,
				func(store.NodeID) bool {
					half++
					if half == streamNodes/2 {
						mid, _ = settledRSS(g)
					}
					return true
				})
			if err != nil {
				b.Fatal(err)
			}
			seen = half
		} else {
			ids, err := g.NodesByProperty(streamKey, streamValue)
			if err != nil {
				b.Fatal(err)
			}
			// Sampled with the slice alive, because that is the state being
			// measured: an answer the caller is halfway through reading.
			mid, _ = settledRSS(ids)
			seen = len(ids)
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}

	if seen != streamNodes {
		b.Fatalf("the pass read %d ids, want %d", seen, streamNodes)
	}
	b.ReportMetric(map[bool]float64{true: 1, false: 0}[streamed], "streamed")
	b.ReportMetric(float64(seen), "ids")
	b.ReportMetric(float64(mid.Total)/bytesPerMiB, "midMiB")
	if mid.Split {
		b.ReportMetric(float64(mid.Anon)/bytesPerMiB, "midAnonMiB")
		b.ReportMetric(float64(mid.File)/bytesPerMiB, "midFileMiB")
	}
}

// BenchmarkRSS_PropertyStream_Slice holds the whole answer.
func BenchmarkRSS_PropertyStream_Slice(b *testing.B) { streamResidency(b, false) }

// BenchmarkRSS_PropertyStream_Stream holds one batch of it.
func BenchmarkRSS_PropertyStream_Stream(b *testing.B) { streamResidency(b, true) }
