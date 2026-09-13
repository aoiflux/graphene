//go:build stress

package graphene_test

// What adjacency costs a process that never traverses.
//
// Three arms, and they measure three different things.
//
// The residency pair is the point of the option: one process opens the fixture
// eagerly, one lazily, each reads every node by property and neither walks the
// graph. The difference between them is the four arrays — sixteen bytes per edge
// and sixteen per node slot — held for nothing. Each arm is its own benchmark so
// each runs in its own process, because peakMiB is a process high-water mark and
// two arms in one process measure the larger of the two twice.
//
// The open arm is the other half of the same figure: what the build costs in
// wall clock at the moment it happens, which a lazy process moves and a
// traversing one only defers.
//
// The degree arm is what the change charges everyone. Every adjacency read now
// passes a branch on whether the arrays exist; an eager store takes the branch
// and finds them built, every time, for ever. That cost is not paid by the
// option's user — it is paid by everyone who did not ask for it — so it is
// measured against a tree without it rather than against the lazy mode.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// Sized so the arrays are unambiguously above the noise floor: 900k edges is
// 14.4 MiB of edge lists, and 150k node slots another 2.4 MiB.
const (
	adjNodes       = 150_000
	adjFanout      = 6
	adjChunk       = 50_000
	adjProbeStride = 97
)

// adjacencyBenchFixture builds a compacted store of adjNodes nodes, each with
// one indexed property and adjFanout outbound edges to nodes ahead of it, and
// returns its directory.
//
// The edges fan forward by a stride rather than to neighbours so that no node's
// adjacency is one contiguous run of edge IDs — a layout where the offset arrays
// would be trivially compressible is not the layout a real graph has.
func adjacencyBenchFixture(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	nodes := make([]*store.Node, adjNodes)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperty(id, "seq", []byte(fmt.Sprintf("%08d", i))); err != nil {
			b.Fatalf("IndexNodeProperty: %v", err)
		}
	}

	batch := make([]*store.Edge, 0, adjChunk)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if _, err := g.AddEdges(batch); err != nil {
			b.Fatalf("AddEdges: %v", err)
		}
		batch = batch[:0]
	}
	for i := range ids {
		for k := 1; k <= adjFanout; k++ {
			dst := (i + k*adjProbeStride) % len(ids)
			batch = append(batch, &store.Edge{
				Src: ids[i], Dst: ids[dst],
				Labels: []store.EdgeType{store.EdgeTypeContains},
			})
			if len(batch) == adjChunk {
				flush()
			}
		}
	}
	flush()

	if err := g.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	return dir
}

// propertyOnlyPass is the workload the option exists for: read every node by an
// indexed property, touch nothing that walks the graph.
func propertyOnlyPass(b *testing.B, g *graphene.Graph) int {
	b.Helper()
	seen := 0
	for i := 0; i < adjNodes; i++ {
		ids, err := g.NodesByProperty("seq", []byte(fmt.Sprintf("%08d", i)))
		if err != nil {
			b.Fatalf("NodesByProperty: %v", err)
		}
		for _, id := range ids {
			if _, err := g.GetNode(id); err != nil {
				b.Fatalf("GetNode(%d): %v", id, err)
			}
			seen++
		}
	}
	return seen
}

// adjacencyResidency is the body both residency arms share: open under one mode,
// settle, run the property pass, settle again, and report what the process is
// holding. Run with -benchtime 1x.
func adjacencyResidency(b *testing.B, mode disk.AdjacencyMode) {
	dir := adjacencyBenchFixture(b)

	var opened, after rssSample
	var holding string
	var seen int
	for i := 0; i < b.N; i++ {
		g, err := graphene.OpenWithOptions(dir, disk.Options{Adjacency: mode, ReadOnly: true})
		if err != nil {
			b.Fatal(err)
		}
		o, _ := settledRSS(g)
		n := propertyOnlyPass(b, g)
		a, _ := settledRSS(g)
		st, ok := g.StorageStats()
		if !ok {
			b.Fatal("StorageStats is unavailable")
		}
		if i == 0 {
			opened, after, seen, holding = o, a, n, st.Adjacency
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}

	if seen != adjNodes {
		b.Fatalf("the pass read %d nodes, want %d", seen, adjNodes)
	}
	// The discriminator. Two arms that report the same holding are one arm run
	// twice, and everything below them is then a measurement of nothing.
	b.ReportMetric(map[string]float64{"built": 1, "deferred": 0}[holding], "built")
	b.ReportMetric(float64(seen), "nodes")
	b.ReportMetric(float64(opened.Total)/bytesPerMiB, "openedMiB")
	b.ReportMetric(float64(after.Total)/bytesPerMiB, "afterMiB")
	if after.Split {
		reportResidualDelta(b, "anon", opened.Anon, after.Anon)
		reportResidualDelta(b, "file", opened.File, after.File)
	}
}

// BenchmarkRSS_AdjacencyPropertyPass_Eager holds the arrays throughout and never
// reads them.
func BenchmarkRSS_AdjacencyPropertyPass_Eager(b *testing.B) {
	adjacencyResidency(b, disk.AdjacencyEager)
}

// BenchmarkRSS_AdjacencyPropertyPass_Lazy never builds them.
func BenchmarkRSS_AdjacencyPropertyPass_Lazy(b *testing.B) {
	adjacencyResidency(b, disk.AdjacencyLazy)
}

// adjacencyOpen times the open itself, which is where the deferred work is
// deferred from.
func adjacencyOpen(b *testing.B, mode disk.AdjacencyMode) {
	dir := adjacencyBenchFixture(b)

	// One open before the loop, so the image is in the page cache and this is a
	// measurement of the parse and not of the disk.
	warm, err := graphene.OpenWithOptions(dir, disk.Options{Adjacency: mode, ReadOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	st, _ := warm.StorageStats()
	holding := st.Adjacency
	if err := warm.Close(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g, err := graphene.OpenWithOptions(dir, disk.Options{Adjacency: mode, ReadOnly: true})
		if err != nil {
			b.Fatal(err)
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(map[string]float64{"built": 1, "deferred": 0}[holding], "built")
}

func BenchmarkAdjacencyOpen_Eager(b *testing.B) { adjacencyOpen(b, disk.AdjacencyEager) }
func BenchmarkAdjacencyOpen_Lazy(b *testing.B)  { adjacencyOpen(b, disk.AdjacencyLazy) }

// BenchmarkAdjacencyDegree_Hot is the cost the change charges a store that did
// not ask for it: one branch per adjacency read, on an image where the answer is
// always yes.
//
// The A/B for this one is two trees, both eager. Comparing the two modes here
// would measure nothing — after the first call they are the same image.
func BenchmarkAdjacencyDegree_Hot(b *testing.B) {
	dir := adjacencyBenchFixture(b)
	g, err := graphene.OpenWithOptions(dir, disk.Options{ReadOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	ids := make([]store.NodeID, 0, 4096)
	for i := 0; i < 4096; i++ {
		got, err := g.NodesByProperty("seq", []byte(fmt.Sprintf("%08d", i*17%adjNodes)))
		if err != nil {
			b.Fatalf("NodesByProperty: %v", err)
		}
		ids = append(ids, got...)
	}
	if len(ids) == 0 {
		b.Fatal("no anchors; the fixture has stopped producing them")
	}

	total := 0
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d, err := g.Degree(ids[i%len(ids)], nil)
		if err != nil {
			b.Fatal(err)
		}
		total += d
	}
	b.StopTimer()
	if total == 0 {
		b.Fatal("every degree was zero; this measured a miss, not an adjacency read")
	}
}

// BenchmarkAdjacencyCompact is the side effect: a compaction walks the edge
// sequence three times instead of five, and for compaction that sequence is a
// merge over an image and two sorted slices rather than a slice.
//
// Run with -benchtime 1x. Each iteration writes a fresh store, so b.N above one
// measures a different thing each time.
func BenchmarkAdjacencyCompact(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		nodes := make([]*store.Node, adjNodes)
		for j := range nodes {
			nodes[j] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			b.Fatalf("AddNodes: %v", err)
		}
		batch := make([]*store.Edge, 0, adjChunk)
		for j := range ids {
			for k := 1; k <= adjFanout; k++ {
				batch = append(batch, &store.Edge{
					Src: ids[j], Dst: ids[(j+k*adjProbeStride)%len(ids)],
					Labels: []store.EdgeType{store.EdgeTypeContains},
				})
				if len(batch) == adjChunk {
					if _, err := g.AddEdges(batch); err != nil {
						b.Fatalf("AddEdges: %v", err)
					}
					batch = batch[:0]
				}
			}
		}
		if len(batch) > 0 {
			if _, err := g.AddEdges(batch); err != nil {
				b.Fatalf("AddEdges: %v", err)
			}
		}
		b.StartTimer()

		if err := g.Compact(); err != nil {
			b.Fatalf("Compact: %v", err)
		}

		b.StopTimer()
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
