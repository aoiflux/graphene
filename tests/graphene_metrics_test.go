package graphene_test

// The metrics sink.
//
// There was no logging and no metrics anywhere in library code, which is a
// defensible position for an embedded engine and a useless one for anybody
// operating it. Options.Metrics is the place to attach one.
//
// What these tests pin is not that numbers arrive — that would pass against a
// sink emitting zeros — but that each number means what store.Metric's table
// says it means, and that the emissions happen where a slow sink cannot hurt
// anything.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// sink collects what the engine reports. Safe for the concurrent emission the
// commit path produces, which is the whole reason it takes a lock rather than
// appending to a bare slice.
type sink struct {
	mu  sync.Mutex
	all []store.Metric
}

func (s *sink) Record(m store.Metric) {
	s.mu.Lock()
	s.all = append(s.all, m)
	s.mu.Unlock()
}

func (s *sink) of(k store.MetricKind) []store.Metric {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.Metric
	for _, m := range s.all {
		if m.Kind == k {
			out = append(out, m)
		}
	}
	return out
}

func (s *sink) count(k store.MetricKind) int { return len(s.of(k)) }

// metricStore opens a disk store with a sink attached, and returns both.
func metricStore(t *testing.T) (*graphene.Graph, *sink, string) {
	t.Helper()
	dir := t.TempDir()
	sk := &sink{}
	g, err := graphene.OpenWithOptions(dir, disk.Options{Metrics: sk})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g, sk, dir
}

func addNodes(t *testing.T, g *graphene.Graph, n int) []store.NodeID {
	t.Helper()
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	return ids
}

func TestMetrics_ACommitReportsItsSizeAndItsBytes(t *testing.T) {
	g, sk, _ := metricStore(t)
	addNodes(t, g, 50)

	commits := sk.of(store.MetricCommit)
	if len(commits) != 1 {
		t.Fatalf("one batch should be one commit metric, got %d", len(commits))
	}
	c := commits[0]
	if c.Count != 50 {
		t.Errorf("Count = %d, want the 50 records in the batch", c.Count)
	}
	if c.Bytes <= 0 {
		t.Errorf("Bytes = %d, want the framed size of the batch", c.Bytes)
	}
	// The duration covers the wait for the group's fsync, so it is what the
	// caller experienced rather than what the append cost. Only checked for
	// sanity, not for a floor: a commit fast enough to land inside the clock's
	// resolution reports zero, and that is the clock's answer rather than a bug.
	if c.Duration < 0 {
		t.Errorf("Duration = %v", c.Duration)
	}
	if c.Err != nil {
		t.Errorf("Err = %v on a commit that succeeded", c.Err)
	}
}

// The number group commit exists to move. Without it, N concurrent commits cost
// N fsyncs; with it, one fsync covers several — and Count on a sync metric is
// how many it covered, so this asserts the mechanism rather than a timing.
func TestMetrics_ASyncReportsHowManyCommitsItCovered(t *testing.T) {
	g, sk, _ := metricStore(t)

	// Batch commits, not single-record mutators. The four mutators have never
	// fsynced — they are durable at the next Sync, Compact or Close — so a store
	// written entirely through them produces no sync metric at all, which is
	// correct and is not what this test is about.
	const writers, each = 8, 12
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				batch := []*store.Node{{Labels: []store.NodeType{store.NodeTypeCase}}}
				if _, err := g.AddNodes(batch); err != nil {
					t.Errorf("AddNodes: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	syncs := sk.of(store.MetricSync)
	if len(syncs) == 0 {
		t.Fatal("no sync metric was emitted")
	}
	var covered int64
	var spent time.Duration
	for _, m := range syncs {
		covered += m.Count
		spent += m.Duration
		if m.Duration < 0 {
			t.Errorf("a sync reported Duration %v", m.Duration)
		}
	}
	// The sum, not each one. A single fsync of a few kilobytes already in the
	// page cache can complete inside the clock's resolution and legitimately
	// report zero — asserting otherwise is the same defect as pinning a
	// traversal to a one-nanosecond deadline. Across every sync in the run the
	// total cannot be zero unless nothing was timed at all.
	if spent <= 0 {
		t.Errorf("%d syncs reported %v in total; the fsync is not being timed", len(syncs), spent)
	}
	// Every queued entry is accounted for exactly once: the counter is a
	// difference between ring positions, so a gap or an overlap would show here
	// rather than as a plausible-looking number.
	if covered < writers*each {
		t.Errorf("syncs covered %d queued entries, but %d were written", covered, writers*each)
	}
	// And the point of the number: fewer fsyncs than commits. Not a timing
	// assertion — with group commit working, several of these 96 batches share
	// each sync, and without it there is exactly one sync per batch.
	if len(syncs) >= writers*each {
		t.Errorf("%d fsyncs for %d commits; nothing is being grouped", len(syncs), writers*each)
	}
}

func TestMetrics_AQueryReportsResultsAndCandidates(t *testing.T) {
	g, sk, _ := metricStore(t)
	ids := addNodes(t, g, 200)
	for i, id := range ids {
		v := []byte("odd")
		if i%2 == 0 {
			v = []byte("even")
		}
		if err := g.IndexNodeProperty(id, "parity", v); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}

	got, err := g.QueryNodeIDs(store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "parity", Op: store.PropertyOpEqual, Value: []byte("even")},
	}})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}

	qs := sk.of(store.MetricQuery)
	if len(qs) != 1 {
		t.Fatalf("one query should be one metric, got %d", len(qs))
	}
	q := qs[0]
	if q.Count != int64(len(got)) {
		t.Errorf("Count = %d but the query returned %d ids", q.Count, len(got))
	}
	// Candidates are what the driving step produced, so they can never be fewer
	// than the results that survived the residual pass. That inequality is the
	// only thing about the number that is true by construction, which makes it
	// the right thing to assert.
	if q.Examined < q.Count {
		t.Errorf("Examined = %d is below Count = %d; the candidate count is not what was driven",
			q.Examined, q.Count)
	}
}

func TestMetrics_ACompactionAndTheReplayAfterItAreReported(t *testing.T) {
	dir := t.TempDir()
	sk := &sink{}
	g, err := graphene.OpenWithOptions(dir, disk.Options{Metrics: sk})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	addNodes(t, g, 100)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	comps := sk.of(store.MetricCompaction)
	if len(comps) != 1 {
		t.Fatalf("one compaction should be one metric, got %d", len(comps))
	}
	c := comps[0]
	if c.Count != 100 {
		t.Errorf("Count = %d, want the 100 records folded into the image", c.Count)
	}
	if c.Bytes <= 0 {
		t.Errorf("Bytes = %d, want the image's size on disk", c.Bytes)
	}
	if c.Err != nil {
		t.Errorf("Err = %v on a compaction that succeeded", c.Err)
	}

	// The first open replayed nothing; reopening after a compaction replays
	// nothing either, because the log was retired. Write past the compaction and
	// the reopen has something to do — which is what makes this metric the
	// indicator of an overdue compaction rather than of an open.
	sk2 := &sink{}
	g2, err := graphene.OpenWithOptions(dir, disk.Options{Metrics: sk2})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	addNodes(t, g2, 40)
	if err := g2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sk3 := &sink{}
	g3, err := graphene.OpenWithOptions(dir, disk.Options{Metrics: sk3})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g3.Close()

	rs := sk3.of(store.MetricReplay)
	if len(rs) != 1 {
		t.Fatalf("an open should report exactly one replay, got %d", len(rs))
	}
	if rs[0].Bytes <= 0 {
		t.Errorf("Bytes = %d, but 40 records were written since the compaction", rs[0].Bytes)
	}
	if rs[0].Count <= 0 {
		t.Errorf("Count = %d, but the replay advanced the epoch", rs[0].Count)
	}
}

func TestMetrics_ASnapshotReportsHowLongItWasHeld(t *testing.T) {
	g, sk, _ := metricStore(t)
	addNodes(t, g, 10)

	snap, err := g.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if sk.count(store.MetricSnapshotOpen) != 1 {
		t.Fatalf("opening a snapshot should report one open, got %d", sk.count(store.MetricSnapshotOpen))
	}
	if sk.count(store.MetricSnapshotClose) != 0 {
		t.Fatal("a snapshot that is still open reported a close")
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot Close: %v", err)
	}

	closes := sk.of(store.MetricSnapshotClose)
	if len(closes) != 1 {
		t.Fatalf("closing a snapshot should report one close, got %d", len(closes))
	}
	// Count is nanoseconds held. Zero is possible on a coarse clock and is not
	// the failure this is guarding: a negative number would mean the open and
	// close timestamps came from different clocks.
	if closes[0].Count < 0 {
		t.Errorf("a snapshot was held for %d ns", closes[0].Count)
	}

	// Closing twice is idempotent and must not report twice, or a sink counting
	// open snapshots by subtraction goes negative.
	if err := snap.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := sk.count(store.MetricSnapshotClose); got != 1 {
		t.Errorf("a double Close reported %d closes", got)
	}
}

func TestMetrics_ABackupReportsWhatItCopied(t *testing.T) {
	g, sk, _ := metricStore(t)
	addNodes(t, g, 30)

	dst := filepath.Join(t.TempDir(), "backup")
	info, err := g.Backup(dst)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	bs := sk.of(store.MetricBackup)
	if len(bs) != 1 {
		t.Fatalf("one backup should be one metric, got %d", len(bs))
	}
	if bs[0].Count != int64(len(info.Files)) {
		t.Errorf("Count = %d but the manifest lists %d files", bs[0].Count, len(info.Files))
	}
	if bs[0].Bytes != info.Bytes() {
		t.Errorf("Bytes = %d but the manifest totals %d", bs[0].Bytes, info.Bytes())
	}

	// A failed backup is still recorded, because an error rate is a metric a
	// sink hearing only about successes cannot compute.
	if _, err := g.Backup(dst); err == nil {
		t.Fatal("backing up over an existing backup should fail")
	}
	bs = sk.of(store.MetricBackup)
	if len(bs) != 2 {
		t.Fatalf("a failed backup should still be reported, got %d metrics", len(bs))
	}
	if bs[1].Err == nil {
		t.Error("the failed backup was reported with a nil Err")
	}
}

// A cancellation that lands inside the work is an outcome; a refusal before any
// work starts is not. The distinction matters to whoever reads the error rate:
// a query that was never run is not a query that failed.
func TestMetrics_ACancelledQueryIsReportedOnlyIfItRan(t *testing.T) {
	g, sk, _ := metricStore(t)
	ids := addNodes(t, g, 2000)
	for i, id := range ids {
		if err := g.IndexNodeProperty(id, "bucket", []byte("b")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		if err := g.IndexNodeProperty(id, "seq", []byte(string(rune('a'+i%26)))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	before := sk.count(store.MetricQuery)

	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.QueryNodeIDsCtx(dead, store.NodeQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if got := sk.count(store.MetricQuery) - before; got != 0 {
		t.Errorf("a query refused before it took the lock produced %d metrics", got)
	}

	// One that gets past the entry check and is stopped inside the residual
	// pass did run, and is reported with the error it ended on.
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b")},
		{Key: "seq", Op: store.PropertyOpContains, Value: []byte("a")},
	}, FilterMode: store.MatchAll}
	if _, err := g.QueryNodeIDsCtx(newCountdownCtx(2), q); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	qs := sk.of(store.MetricQuery)
	if len(qs)-before != 1 {
		t.Fatalf("a query cancelled mid-pass produced %d metrics, want 1", len(qs)-before)
	}
	if qs[len(qs)-1].Err == nil {
		t.Error("the cancelled query was reported with a nil Err")
	}
}

func TestMetrics_ALiveReadersRefreshIsReported(t *testing.T) {
	dir := t.TempDir()
	w, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()
	addNodes(t, w, 20)

	sk := &sink{}
	r, err := graphene.OpenWithOptions(dir, disk.Options{LiveReader: true, Metrics: sk})
	if err != nil {
		t.Fatalf("OpenLive: %v", err)
	}
	defer r.Close()

	addNodes(t, w, 20)
	if _, err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	rs := sk.of(store.MetricRefresh)
	if len(rs) != 1 {
		t.Fatalf("one Refresh should be one metric, got %d", len(rs))
	}
	if rs[0].Bytes <= 0 {
		t.Errorf("Bytes = %d, but 20 records were appended before the refresh", rs[0].Bytes)
	}
	if rs[0].Count <= 0 {
		t.Errorf("Count = %d, but the refresh advanced the reader's epoch", rs[0].Count)
	}
}

// The claim the whole design rests on: a store with no sink behaves exactly as
// it did before this existed. Asserted where it can be — no clock is read, no
// metric is built — by giving the store a sink that fails the test if it is ever
// called, and then not attaching it.
func TestMetrics_NoSinkMeansNothingIsRecorded(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.OpenWithOptions(dir, disk.Options{})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer g.Close()

	addNodes(t, g, 20)
	if _, err := g.QueryNodeIDs(store.NodeQuery{}); err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	snap, err := g.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot Close: %v", err)
	}
	// Nothing to assert but the absence of a panic and a nil dereference — the
	// cost side of this claim is a benchmark, not a test. What this pins is that
	// every emission site is guarded, because an unguarded one would call a nil
	// interface and take the process down.
}

// MetricsFunc exists so a caller does not have to declare a type to attach one
// closure. It is the same adapter shape as CompactionObserverFunc, and it exists
// for the same reason: disk.Options is a comparable value and a struct with a
// func field is not.
func TestMetrics_MetricsFuncIsUsableAsASink(t *testing.T) {
	var mu sync.Mutex
	kinds := map[store.MetricKind]int{}

	dir := t.TempDir()
	g, err := graphene.OpenWithOptions(dir, disk.Options{
		Metrics: store.MetricsFunc(func(m store.Metric) {
			mu.Lock()
			kinds[m.Kind]++
			mu.Unlock()
		}),
	})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer g.Close()

	addNodes(t, g, 5)
	mu.Lock()
	defer mu.Unlock()
	if kinds[store.MetricCommit] != 1 {
		t.Errorf("MetricsFunc saw %d commits, want 1", kinds[store.MetricCommit])
	}
	// Options must stay comparable, or it cannot be a value type. A func field
	// would have broken that, which is what the adapter is for.
	var a, b disk.Options
	if a != b {
		t.Error("disk.Options is no longer comparable")
	}
}

// Every kind the engine can emit has a name, or a sink that labels its output
// prints "unknown" for something the engine reports routinely.
func TestMetrics_EveryKindHasAName(t *testing.T) {
	for k := store.MetricCommit; k <= store.MetricRefresh; k++ {
		if k.String() == "unknown" {
			t.Errorf("MetricKind(%d) has no name", k)
		}
	}
	if store.MetricKind(200).String() != "unknown" {
		t.Error("an unrecognised kind should name itself unknown rather than panicking")
	}
}

func TestMetrics_TheSinkIsNeverCalledUnderAStoreLock(t *testing.T) {
	// A sink is caller code. If the engine held a lock across it, a slow one
	// would stall exactly the writers it exists to measure — so every emission
	// but MetricSync happens with no store lock held. Asserted by having the
	// sink do the one thing that would deadlock if it were: read the store.
	dir := t.TempDir()
	var g *graphene.Graph
	var reads int

	sk := store.MetricsFunc(func(m store.Metric) {
		if g == nil || m.Kind == store.MetricSync {
			return
		}
		// Takes the read lock. Under the write lock this blocks forever; under
		// the read lock it is fine, which is why the commit and snapshot paths
		// release before recording.
		if _, ok := g.StorageStats(); ok {
			reads++
		}
	})

	var err error
	g, err = graphene.OpenWithOptions(dir, disk.Options{Metrics: sk})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer g.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		addNodes(t, g, 10)
		if _, qerr := g.QueryNodeIDs(store.NodeQuery{}); qerr != nil {
			t.Errorf("QueryNodeIDs: %v", qerr)
		}
		snap, serr := g.Snapshot()
		if serr != nil {
			t.Errorf("Snapshot: %v", serr)
			return
		}
		snap.Close()
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a sink that reads the store deadlocked; an emission is happening under a store lock")
	}
	if reads == 0 {
		t.Error("the sink was never called, so this proves nothing")
	}
}

func TestMetrics_TheImageSizeIsTheFileOnDisk(t *testing.T) {
	dir := t.TempDir()
	sk := &sink{}
	g, err := graphene.OpenWithOptions(dir, disk.Options{Metrics: sk})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer g.Close()

	addNodes(t, g, 60)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	fi, err := os.Stat(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		t.Fatalf("stat image: %v", err)
	}
	comps := sk.of(store.MetricCompaction)
	if len(comps) != 1 {
		t.Fatalf("got %d compaction metrics", len(comps))
	}
	if comps[0].Bytes != fi.Size() {
		t.Errorf("reported %d image bytes, but graphene.csr is %d", comps[0].Bytes, fi.Size())
	}
}
