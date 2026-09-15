// Program baselines: process residency for the five operations the memory
// optimisation program is judged by.
//
// These differ from the Footprint benchmarks next door in what they measure and
// in the fixture they measure it on. Footprint reports retained Go heap for a
// graph built in this process; these report operating-system residency for a
// store reopened from disk, which is the only shape that can see an image that
// was never decoded into the heap, and the only shape a RAM ceiling applies to.
//
// The fixture deliberately matches the index declarations of the workload this
// engine is being sized for — eight unique keys, one of them an all-distinct
// 32-byte content digest, five ordered keys, two composites — because the
// property index, not the graph, is the dominant resident term at that shape and
// a fixture with three keys would rank the optimisations wrongly.
//
// Run one size per process. A single process that builds several large fixtures
// reports the high-water mark of the largest, and the peak figures then say
// nothing about the smaller ones:
//
//	go test ./tests/ -tags=stress -run=^$ -bench=RSS_Open -benchtime=1x -count=3
//	GRAPHENE_RSS_NODES=1500000 go test ./tests/ -tags=stress -run=^$ -bench=RSS_RebuildCycle -benchtime=1x

//go:build stress

package graphene_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

var (
	// rssNodes is deliberately small by default: the whole suite runs in CI, and
	// the sizes that matter to the program are opt-in through the environment.
	rssNodes  = envIntDefault("GRAPHENE_RSS_NODES", 50_000)
	rssBlob   = envIntDefault("GRAPHENE_RSS_BLOB", 512)
	rssCycles = envIntDefault("GRAPHENE_RSS_CYCLES", 10)

	// rssDir names a directory to build the fixture in once and reuse across
	// processes. Empty means each process builds its own and deletes it.
	//
	// Two reasons, and the second invalidates figures rather than merely costing
	// time. A 1.2 GiB fixture takes minutes to build, which is the annoyance. The
	// build also peaks at several times what the finished store costs to open --
	// near 9.4 GiB at 1.4M nodes against 2.7 GiB of settled residency -- and
	// peakMiB is the high-water mark of the *process*, so a process that builds
	// and then measures reports the build's peak under the name of the open's.
	// Splitting the build into its own process is the only way that figure means
	// what it says.
	rssDir = os.Getenv("GRAPHENE_RSS_DIR")

	// rssEdgeStride adds one edge per stride nodes; 0 writes none.
	//
	// Zero is the default because every figure recorded under "Program baselines"
	// in docs/benchmarks.md was measured on a fixture with no edges, and silently
	// adding them would make the next run incomparable to the last without
	// anything in the output saying so. The adjacency arrays, the edge records and
	// the delete-cascade path are all unmeasurable until it is set, which is the
	// whole reason it exists.
	rssEdgeStride = envIntDefault("GRAPHENE_RSS_EDGE_STRIDE", 0)

	// rssBlobDist replaces the single blob size with the long-tailed shape the
	// consumer reported -- "a few hundred bytes to 64 MiB" -- rather than one size
	// for every node. See rssBlobSize for the shape and for why it is a small-N
	// arm only.
	rssBlobDist = os.Getenv("GRAPHENE_RSS_BLOB_DIST") == "1"

	// rssNoIndex builds the fixture without declaring or populating any indexed
	// property.
	//
	// It is a differencing arm and nothing else. The property index is the term
	// this program exists to remove, and no instrument can see it directly: it is
	// spread across sixteen shards, an intern table, five ordered indexes and two
	// composites, and a heap profile attributes it to whichever map grew last. The
	// only honest way to size it against a real store is to build the same store
	// twice, once with the declarations and once without, and subtract.
	rssNoIndex = os.Getenv("GRAPHENE_RSS_NOINDEX") == "1"
)

// rssShapeFile records, inside a persistent fixture, what it was built to.
const rssShapeFile = "rss-fixture-shape.txt"

// The consumer's declared index shape. Every one of these keys is populated on
// every node, so entries-per-node is len(rssUniqueKeys)+len(rssOrderedKeys) and
// the index is the largest resident structure by a wide margin — which is the
// state the program exists to change.
var (
	rssUniqueKeys  = []string{"digest", "uid0", "uid1", "uid2", "uid3", "uid4", "uid5", "uid6"}
	rssOrderedKeys = []string{"ord0", "ord1", "ord2", "ord3", "ord4"}
	rssComposites  = [][]string{{"ord0", "ord1"}, {"ord2", "ord3"}}
)

// rssProps builds one node's indexed properties.
//
// The digest is 32 raw bytes and distinct for every node, which is the worst
// case the declarations allow: it cannot be interned, it cannot share a posting
// list, and it is the single largest contributor to index residency.
func rssProps(i int) map[string][]byte {
	props := make(map[string][]byte, len(rssUniqueKeys)+len(rssOrderedKeys))

	var digest [32]byte
	binary.BigEndian.PutUint64(digest[0:], uint64(i))
	binary.BigEndian.PutUint64(digest[8:], uint64(i)*0x9e3779b97f4a7c15)
	binary.BigEndian.PutUint64(digest[16:], ^uint64(i))
	binary.BigEndian.PutUint64(digest[24:], uint64(i)<<21|uint64(i)>>11)
	props["digest"] = digest[:]

	for k, key := range rssUniqueKeys {
		if key == "digest" {
			continue
		}
		props[key] = []byte(fmt.Sprintf("%s-%09d", key, i*len(rssUniqueKeys)+k))
	}
	// Ordered keys are low cardinality on purpose: they are the half of the index
	// that interning and shared postings can help, so keeping them distinct would
	// hide whichever optimisation addresses that half.
	for k, key := range rssOrderedKeys {
		props[key] = []byte(fmt.Sprintf("%06d", (i+k)%1000))
	}
	return props
}

// rssBlobSize is the blob length for node i.
//
// With GRAPHENE_RSS_BLOB_DIST set it is the long-tailed shape rather than one
// size for every node: 90% at blob, 9% at 128x, 1% at 8192x, which at the
// default 512 is 512 B, 64 KiB and 4 MiB. The size is a function of the index
// alone, so a fixture is reproducible and a reused one can be validated against
// the parameters that built it.
//
// Note what the tail does to the mean: 48.3 KiB per node against 512 B fixed, a
// factor of 94. This is not a knob to turn on at the sizes the rest of this file
// runs at -- 1.4M nodes would be 67 GB of blob -- it is a small-N arm for one
// question, whether a model term in mean blob bytes is the right term or whether
// the distribution itself costs something.
func rssBlobSize(i, blob int) int {
	if !rssBlobDist {
		return blob
	}
	switch m := i % 100; {
	case m < 90:
		return blob
	case m < 99:
		return blob * 128
	default:
		return blob * 8192
	}
}

// The fixture helpers below take testing.TB rather than *testing.B.
//
// They only ever call Helper, Fatal* and Cleanup, so the narrower type bought
// nothing and excluded the one caller that is not a benchmark: the ceiling run
// in ceiling_test.go, which is a test because its result is pass or fail rather
// than a figure. Sharing the builders matters more than the type -- a second
// copy of rssProps would let the fixture the ceiling run measures drift away
// from the fixture every other figure in this program was taken on.

// rssWriteEdges connects the fixture's nodes, and returns how many edges it
// wrote.
//
// One edge per stride nodes, each joining a node to one a prime distance ahead.
// The distance matters: a fixture whose edges all point at the next slot would
// lay the adjacency arrays out as one sequential run, which is neither what a
// real graph looks like nor what its residency costs to walk.
func rssWriteEdges(b testing.TB, g *graphene.Graph, ids []store.NodeID, stride int) int {
	b.Helper()

	if stride <= 0 || len(ids) < 2 {
		return 0
	}

	const chunk = 10_000
	written := 0
	batch := make([]*store.Edge, 0, chunk)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if _, err := g.AddEdges(batch); err != nil {
			b.Fatalf("AddEdges: %v", err)
		}
		written += len(batch)
		batch = batch[:0]
	}

	for i := 0; i < len(ids); i += stride {
		batch = append(batch, &store.Edge{
			Src:    ids[i],
			Dst:    ids[(i+7919)%len(ids)],
			Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
			Weight: 0.5,
		})
		if len(batch) == chunk {
			flush()
		}
	}
	flush()
	return written
}

// rssDeclare applies the declarations. They must precede any indexing, so every
// fixture builder calls this immediately after Open.
func rssDeclare(g *graphene.Graph) error {
	if rssNoIndex {
		return nil
	}
	for _, key := range rssUniqueKeys {
		if err := g.DeclareUniqueProperty(key); err != nil {
			return fmt.Errorf("declare unique %q: %w", key, err)
		}
	}
	for _, key := range rssOrderedKeys {
		if err := g.DeclareOrderedProperty(key); err != nil {
			return fmt.Errorf("declare ordered %q: %w", key, err)
		}
	}
	for _, keys := range rssComposites {
		if err := g.DeclareCompositeProperties(keys); err != nil {
			return fmt.Errorf("declare composite %v: %w", keys, err)
		}
	}
	return nil
}

// rssWriteNodes adds n nodes with blob-sized properties and indexes each one.
// It returns the ids in insertion order so callers can delete a contiguous band.
func rssWriteNodes(b testing.TB, g *graphene.Graph, from, n, blob int) []store.NodeID {
	b.Helper()

	const chunk = 10_000
	ids := make([]store.NodeID, 0, n)

	for base := 0; base < n; base += chunk {
		size := chunk
		if remaining := n - base; remaining < size {
			size = remaining
		}
		batch := make([]*store.Node, size)
		for i := range batch {
			payload := make([]byte, rssBlobSize(from+base+i, blob))
			binary.BigEndian.PutUint64(payload, uint64(from+base+i))
			batch[i] = &store.Node{
				Labels:     []store.NodeType{benchLabel(from + base + i)},
				Properties: payload,
			}
		}
		got, err := g.AddNodes(batch)
		if err != nil {
			b.Fatalf("AddNodes at %d: %v", base, err)
		}
		if !rssNoIndex {
			for i, id := range got {
				if err := g.IndexNodeProperties(id, rssProps(from+base+i)); err != nil {
					b.Fatalf("IndexNodeProperties at %d: %v", base+i, err)
				}
			}
		}
		ids = append(ids, got...)
	}
	return ids
}

// rssShape is the line written into a persistent fixture and checked on reuse.
//
// A reused fixture that quietly disagrees with the parameters of the run
// measuring it is worse than no fixture at all: every figure comes out
// plausible, and nothing in the output says the store held 400,000 nodes while
// the heading said 1,400,000. So the shape is written down, and a mismatch is
// fatal rather than a silent measurement of the wrong store.
func rssShape(nodes, blob int) string {
	return fmt.Sprintf(
		"nodes=%d blob=%d blobdist=%t edgestride=%d noindex=%t unique=%d ordered=%d composite=%d\n",
		nodes, blob, rssBlobDist, rssEdgeStride, rssNoIndex,
		len(rssUniqueKeys), len(rssOrderedKeys), len(rssComposites))
}

// rssBuildFixture writes the fixture into dir and compacts it.
func rssBuildFixture(b testing.TB, dir string, nodes, blob int) {
	b.Helper()

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	if err := rssDeclare(g); err != nil {
		b.Fatal(err)
	}
	ids := rssWriteNodes(b, g, 0, nodes, blob)
	rssWriteEdges(b, g, ids, rssEdgeStride)
	if err := g.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
}

// rssFixtureDir returns a fixture directory the caller reads and does not write.
// The caller reopens it, so the measurement covers a cold decode rather than the
// residue of the build.
//
// Without GRAPHENE_RSS_DIR the fixture is built in a temp directory and removed
// afterwards, which is right for the sizes that run in CI. With it, the fixture
// is built once and every later process measures a bare Open -- see the variable
// for why that distinction decides whether peakMiB means anything.
//
// A benchmark that writes to the store wants rssMutableFixtureDir instead.
func rssFixtureDir(b testing.TB, nodes, blob int) string {
	b.Helper()

	if rssDir == "" {
		dir, err := os.MkdirTemp("", "graphene-rss-*")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(dir) })
		rssBuildFixture(b, dir, nodes, blob)
		return dir
	}

	want := rssShape(nodes, blob)
	marker := filepath.Join(rssDir, rssShapeFile)
	switch got, err := os.ReadFile(marker); {
	case err == nil:
		if string(got) != want {
			b.Fatalf("fixture at %s was built to a different shape than this run wants:\n"+
				"  have: %s  want: %s"+
				"delete that directory, or point GRAPHENE_RSS_DIR elsewhere",
				rssDir, got, want)
		}
		return rssDir
	case !os.IsNotExist(err):
		b.Fatalf("read %s: %v", marker, err)
	}

	// No marker. An absent or empty directory is ours to build in; anything else
	// is either a store left by an interrupted build or something unrelated, and
	// building over it would produce a fixture that is neither.
	entries, err := os.ReadDir(rssDir)
	switch {
	case err == nil && len(entries) > 0:
		b.Fatalf("GRAPHENE_RSS_DIR %s is not empty and carries no %s: "+
			"delete it if it is an interrupted fixture build", rssDir, rssShapeFile)
	case err != nil && !os.IsNotExist(err):
		b.Fatalf("read dir %s: %v", rssDir, err)
	}
	if err := os.MkdirAll(rssDir, 0o755); err != nil {
		b.Fatal(err)
	}

	rssBuildFixture(b, rssDir, nodes, blob)

	// Written last, so an interrupted build leaves a directory the next run
	// refuses rather than one it trusts.
	if err := os.WriteFile(marker, []byte(want), 0o644); err != nil {
		b.Fatal(err)
	}
	return rssDir
}

// rssMutableFixtureDir is rssFixtureDir for a benchmark that writes to the store.
//
// With no persistent directory the two are the same thing: the fixture was built
// for this process and dies with it. With one, the master is copied, because a
// benchmark that adds a delta and compacts would otherwise hand the next run a
// store that is larger and freshly compacted -- so the second run of the same
// benchmark would measure a different store from the first, and the shape marker
// would still say they matched.
func rssMutableFixtureDir(b testing.TB, nodes, blob int) string {
	b.Helper()

	master := rssFixtureDir(b, nodes, blob)
	if rssDir == "" {
		return master
	}

	dst, err := os.MkdirTemp("", "graphene-rss-copy-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dst) })
	rssCopyDir(b, master, dst)
	return dst
}

// rssCopyDir copies a fixture's files. Flat by construction: a store directory
// holds no subdirectories, and failing on one is better than skipping it.
func rssCopyDir(b testing.TB, src, dst string) {
	b.Helper()

	entries, err := os.ReadDir(src)
	if err != nil {
		b.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			b.Fatalf("copy fixture: unexpected subdirectory %s", e.Name())
		}
		if e.Name() == rssShapeFile {
			continue
		}
		if err := rssCopyFile(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			b.Fatalf("copy %s: %v", e.Name(), err)
		}
	}
}

func rssCopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// rssStoreBytes is the on-disk size of the whole store directory, reported
// beside residency so the ratio the program cares about — resident bytes per
// byte of store — can be read off a single row.
func rssStoreBytes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total
}

// BenchmarkRSS_Open is the baseline every other figure is read against: what a
// process holds after opening a store and doing nothing else.
func BenchmarkRSS_Open(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	diskMiB := float64(rssStoreBytes(dir)) / bytesPerMiB

	b.ResetTimer()
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	defer g.Close()

	s := reportRSS(b, g)
	b.ReportMetric(diskMiB, "diskMiB")
	if diskMiB > 0 {
		b.ReportMetric(float64(s.Total)/bytesPerMiB/diskMiB, "residentPerDisk")
	}
}

// BenchmarkRSS_Scan is the read-only aggregate shape: open, resolve a handful of
// rows through the index, read them back. The consumer measured this path at
// nearly 5 GiB against a 1.2 GiB store, and it touches ten rows — so whatever
// dominates here is paid for opening, not for reading.
func BenchmarkRSS_Scan(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	const rows = 10
	stride := rssNodes / rows

	b.ResetTimer()
	found := 0
	for i := 0; i < rows; i++ {
		ids, err := g.GraphStore.NodesByProperty("digest", rssProps(i * stride)["digest"])
		if err != nil {
			b.Fatalf("NodesByProperty: %v", err)
		}
		nodes, _, err := g.GetNodes(ids)
		if err != nil {
			b.Fatalf("GetNodes: %v", err)
		}
		found += len(nodes)
	}
	b.StopTimer()
	if found != rows {
		b.Fatalf("expected %d rows, resolved %d", rows, found)
	}

	reportRSS(b, g)
}

// BenchmarkRSS_BulkWrite covers the write half: a chunked write of the whole
// type, which is what a rebuild does before it compacts.
func BenchmarkRSS_BulkWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-rss-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	if err := rssDeclare(g); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	rssWriteNodes(b, g, 0, rssNodes, rssBlob)
	b.StopTimer()

	reportRSS(b, g)
}

// samplePeakDuring runs fn while polling residency, and returns the largest
// reading seen along with how many readings were taken.
//
// The kernel's own peak counter cannot answer this: it is a high-water mark for
// the whole process lifetime, so it would report the cost of building the
// fixture rather than the cost of the operation under test, and there is no
// portable way to reset it. Polling has the opposite weakness — it can miss a
// spike shorter than the interval — so the interval is short relative to a
// compaction, the sample count is reported, and the result is a floor on the
// true peak rather than the peak itself.
func samplePeakDuring(fn func()) (peak rssSample, samples int) {
	return samplePeakEvery(time.Millisecond, fn)
}

// samplePeakEvery is samplePeakDuring with the interval named.
//
// One millisecond is right for a compaction measured in seconds and wrong for a
// rebuild measured in minutes, where it costs hundreds of thousands of readings
// of the very counters being read. The interval is the floor on the true peak
// this returns, so a caller that widens it is trading resolution for overhead
// deliberately rather than inheriting a constant chosen for another operation.
func samplePeakEvery(interval time.Duration, fn func()) (peak rssSample, samples int) {
	type reading struct {
		s rssSample
		n int
	}
	done := make(chan struct{})
	result := make(chan reading, 1)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var best rssSample
		n := 0
		for {
			select {
			case <-done:
				result <- reading{best, n}
				return
			case <-ticker.C:
				s := readRSS()
				n++
				if s.Total > best.Total {
					best = s
				}
			}
		}
	}()

	fn()
	close(done)
	r := <-result
	return r.s, r.n
}

// BenchmarkRSS_Compact measures the transient cost of compaction, which the
// program expects to bound by a constant rather than by the size of the store.
func BenchmarkRSS_Compact(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-rss-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	if err := rssDeclare(g); err != nil {
		b.Fatal(err)
	}
	rssWriteNodes(b, g, 0, rssNodes, rssBlob)

	before, _ := settledRSS(g)

	b.ResetTimer()
	var compactErr error
	peak, samples := samplePeakDuring(func() { compactErr = g.Compact() })
	b.StopTimer()
	if compactErr != nil {
		b.Fatalf("Compact: %v", compactErr)
	}
	if samples == 0 {
		b.Fatal("sampler took no readings; the peak figure would be meaningless")
	}

	b.ReportMetric(float64(peak.Total)/bytesPerMiB, "compactPeakMiB")
	if peak.Total > before.Total {
		b.ReportMetric(float64(peak.Total-before.Total)/bytesPerMiB, "compactDeltaMiB")
	}
	b.ReportMetric(float64(samples), "samples")
	reportRSS(b, g)
}

// BenchmarkRSS_RebuildCycle is the instrument for the cost that no
// file-size-based estimate predicts.
//
// Each cycle deletes every node written by the previous one and writes the same
// number of fresh nodes, then compacts — the shape of a derived-layer rebuild
// triggered by a version bump. The live set is identical at the end of every
// cycle, so any growth in residency across cycles is the cost of identifiers
// that were issued and can never be reused, and it is growth that compaction
// does not reclaim.
//
// A flat line means the cost is bounded. A rising line gives its slope per
// cycle, which is the number that says how many rebuilds fit under a ceiling.
func BenchmarkRSS_RebuildCycle(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-rss-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	if err := rssDeclare(g); err != nil {
		b.Fatal(err)
	}

	// Cycle 0 establishes the live set that every later cycle replaces.
	live := rssWriteNodes(b, g, 0, rssNodes, rssBlob)
	if err := g.Compact(); err != nil {
		b.Fatal(err)
	}
	first, _ := settledRSS(g)

	// A reading after every cycle, not just the last one. Two endpoints give a
	// slope but cannot say whether it is a slope at all: a one-off cost paid at
	// the first rebuild and a per-rebuild leak produce the same average over ten
	// cycles and have completely different consequences at forty. The series is
	// logged so one run answers that, and the reported slope is fitted over the
	// second half, where any one-off has already been paid.
	series := make([]uint64, 0, rssCycles+1)
	series = append(series, first.Total)

	b.ResetTimer()
	for cycle := 1; cycle <= rssCycles; cycle++ {
		for _, id := range live {
			if err := g.DeleteNode(id); err != nil {
				b.Fatalf("cycle %d: DeleteNode(%d): %v", cycle, id, err)
			}
		}
		live = rssWriteNodes(b, g, cycle*rssNodes, rssNodes, rssBlob)
		if err := g.Compact(); err != nil {
			b.Fatalf("cycle %d: Compact: %v", cycle, err)
		}
		s, _ := settledRSS(g)
		series = append(series, s.Total)
	}
	b.StopTimer()

	for cycle, total := range series {
		delta := ""
		if cycle > 0 {
			delta = fmt.Sprintf("  %+.2f MiB", float64(int64(total)-int64(series[cycle-1]))/bytesPerMiB)
		}
		b.Logf("cycle %2d  resident %7.2f MiB%s", cycle, float64(total)/bytesPerMiB, delta)
	}

	last := reportRSS(b, g)
	b.ReportMetric(float64(rssCycles), "cycles")
	b.ReportMetric(float64(first.Total)/bytesPerMiB, "cycle0MiB")
	if last.Total > first.Total && rssCycles > 0 {
		b.ReportMetric(float64(last.Total-first.Total)/float64(rssCycles)/bytesPerMiB, "MiBPerCycle")
	}
	// The steady-state slope excludes the first half, so a fixed start-up cost
	// does not inflate the figure a ceiling is computed from. Where the two
	// disagree, the cost is not a per-cycle leak and the plan built on it needs
	// revisiting.
	if half := rssCycles / 2; half > 0 {
		mid, end := series[half], series[len(series)-1]
		if end > mid {
			b.ReportMetric(float64(end-mid)/float64(rssCycles-half)/bytesPerMiB, "MiBPerCycleTail")
		}
		if rssNodes > 0 && end > mid {
			b.ReportMetric(float64(end-mid)/float64(rssCycles-half)/float64(rssNodes), "BPerBurnedID")
		}
	}
}

// BenchmarkRSS_Open_UncompactedWAL is the failure case: a store that was never
// compacted, so opening it replays the whole write history.
//
// The compacted arm above measures the cost of a store in the shape an operator
// intended. This one measures the shape an operator arrives in after a crash, a
// long-running writer, or a compaction that was never scheduled — and the
// engine currently offers no way to find out which of the two a directory is
// before paying for it. The gap between the two rows is the size of that
// surprise, and it is the number a pre-open estimate has to predict.
func BenchmarkRSS_Open_UncompactedWAL(b *testing.B) {
	dir, err := os.MkdirTemp("", "graphene-rss-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	if err := rssDeclare(g); err != nil {
		b.Fatal(err)
	}
	rssWriteNodes(b, g, 0, rssNodes, rssBlob)
	// Deliberately no Compact: everything stays in the log.
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}
	diskMiB := float64(rssStoreBytes(dir)) / bytesPerMiB

	b.ResetTimer()
	reopened, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	defer reopened.Close()

	s := reportRSS(b, reopened)
	b.ReportMetric(diskMiB, "diskMiB")
	if diskMiB > 0 {
		b.ReportMetric(float64(s.Total)/bytesPerMiB/diskMiB, "residentPerDisk")
	}
}

// BenchmarkRSS_Restore measures what recovering a store costs the machine doing
// the recovering.
//
// Backup and restore both copy through a fixed buffer with a running hash, so
// the memory a recovery needs is a constant rather than the size of the store
// being recovered. That property is the reason a backup taken from a machine
// that is at its RAM ceiling can be restored at all, and it is exactly the kind
// of property that is lost quietly: one io.ReadFile in a copy loop, added for a
// good reason, and a recovery that used to need megabytes needs the whole
// image. Nothing else in the suite would notice, because no other benchmark
// touches this path.
//
// Read restoreDeltaMiB against storeMiB. Streamed, the first stays flat while
// the second grows, and restoreDeltaPerStore falls towards zero. A restore that
// materialised a file would track storeMiB instead.
//
// The backup half is sampled too. It has to run anyway to produce something to
// restore, it makes the identical claim, and its cost is the one an operator
// pays on a live store rather than on a spare machine.
//
// peakMiB from reportRSS answers a different question and must not be read as
// this one: it is a process-lifetime high-water mark, so it reports the fixture
// build. The sampler is what sees a transient, and it reports a floor on the
// true peak — see samplePeakDuring.
func BenchmarkRSS_Restore(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	storeBytes := rssStoreBytes(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	root, err := os.MkdirTemp("", "graphene-rss-restore-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(root)

	// The store stays open across both halves so it is provably alive at the
	// final reading. Were it closed and collected mid-run, the baselines below
	// would be measured against a heap that shrank for reasons the operation
	// under test had nothing to do with, and the deltas would understate.
	before, _ := settledRSS(g)

	b.ResetTimer()
	var (
		info disk.BackupInfo
		berr error
	)
	backupDir := filepath.Join(root, "backup")
	backupPeak, backupSamples := samplePeakDuring(func() { info, berr = g.Backup(backupDir) })
	if berr != nil {
		b.Fatalf("Backup: %v", berr)
	}

	afterBackup, _ := settledRSS(g)

	var (
		out  disk.RestoreInfo
		rerr error
	)
	restorePeak, restoreSamples := samplePeakDuring(func() {
		out, rerr = graphene.Restore(backupDir, filepath.Join(root, "restored"), disk.RestoreOptions{})
	})
	b.StopTimer()
	if rerr != nil {
		b.Fatalf("Restore: %v", rerr)
	}
	if backupSamples == 0 || restoreSamples == 0 {
		b.Fatal("sampler took no readings; the peak figures would be meaningless")
	}
	if out.From.Bytes() != info.Bytes() {
		b.Fatalf("restored %d bytes, the backup holds %d", out.From.Bytes(), info.Bytes())
	}

	b.ReportMetric(float64(storeBytes)/bytesPerMiB, "storeMiB")
	b.ReportMetric(float64(info.Bytes())/bytesPerMiB, "backupMiB")

	b.ReportMetric(float64(backupPeak.Total)/bytesPerMiB, "backupPeakMiB")
	if backupPeak.Total > before.Total {
		b.ReportMetric(float64(backupPeak.Total-before.Total)/bytesPerMiB, "backupDeltaMiB")
	}
	b.ReportMetric(float64(restorePeak.Total)/bytesPerMiB, "restorePeakMiB")
	if restorePeak.Total > afterBackup.Total {
		delta := restorePeak.Total - afterBackup.Total
		b.ReportMetric(float64(delta)/bytesPerMiB, "restoreDeltaMiB")
		if storeBytes > 0 {
			b.ReportMetric(float64(delta)/float64(storeBytes), "restoreDeltaPerStore")
		}
	}
	b.ReportMetric(float64(backupSamples+restoreSamples), "samples")

	reportRSS(b, g)
}
