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
)

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

// rssDeclare applies the declarations. They must precede any indexing, so every
// fixture builder calls this immediately after Open.
func rssDeclare(g *graphene.Graph) error {
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
func rssWriteNodes(b *testing.B, g *graphene.Graph, from, n, blob int) []store.NodeID {
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
			payload := make([]byte, blob)
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
		for i, id := range got {
			if err := g.IndexNodeProperties(id, rssProps(from+base+i)); err != nil {
				b.Fatalf("IndexNodeProperties at %d: %v", base+i, err)
			}
		}
		ids = append(ids, got...)
	}
	return ids
}

// rssFixtureDir builds a store on disk once and returns its directory. The
// caller reopens it, so the measurement covers a cold decode rather than the
// residue of the build.
func rssFixtureDir(b *testing.B, nodes, blob int) string {
	b.Helper()

	dir, err := os.MkdirTemp("", "graphene-rss-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	if err := rssDeclare(g); err != nil {
		b.Fatal(err)
	}
	rssWriteNodes(b, g, 0, nodes, blob)
	if err := g.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	return dir
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
	type reading struct {
		s rssSample
		n int
	}
	done := make(chan struct{})
	result := make(chan reading, 1)

	go func() {
		ticker := time.NewTicker(time.Millisecond)
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
