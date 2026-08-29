package disk

// The two metric claims that can only be checked from inside this package.
//
// Everything else about the sink is asserted through a Graph in tests/. These
// two are not: one needs an fsync to fail on demand, and the other needs the
// clock to hold still. Both seams — wal.syncHook and Store.nowUnixNano — are
// unexported, which is right: they exist for tests in this package and would be
// a fault-injection API if they were not.

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

type metricSink struct {
	mu  sync.Mutex
	all []store.Metric
}

func (m *metricSink) Record(v store.Metric) {
	m.mu.Lock()
	m.all = append(m.all, v)
	m.mu.Unlock()
}

func (m *metricSink) of(k store.MetricKind) []store.Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Metric
	for _, v := range m.all {
		if v.Kind == k {
			out = append(out, v)
		}
	}
	return out
}

func openWithSink(t *testing.T) (*Store, *metricSink) {
	t.Helper()
	sink := &metricSink{}
	s, err := OpenWithOptions(t.TempDir(), Options{Metrics: sink})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, sink
}

// A commit that fails is still a commit that happened.
//
// A sink that only ever hears about successes cannot compute an error rate,
// which is the first thing anyone actually wants from these numbers — so the
// emission is on one path shared by both outcomes rather than on the success
// branch.
func TestMetrics_AFailedCommitIsStillRecorded(t *testing.T) {
	s, sink := openWithSink(t)

	failing := errors.New("device is on fire")
	s.wal.syncHook = func() error { return failing }
	// Cleared before Close, which syncs too: the injected failure is a
	// commit-path one, and leaving it set would also leave the log open on
	// Windows and break the temp-dir cleanup.
	defer func() { s.wal.syncHook = nil }()

	if _, err := s.AddNodesBatch([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeCase}},
	}); !errors.Is(err, failing) {
		t.Fatalf("a failed fsync was not reported: %v", err)
	}

	commits := sink.of(store.MetricCommit)
	if len(commits) != 1 {
		t.Fatalf("a failed commit produced %d metrics, want 1", len(commits))
	}
	if !errors.Is(commits[0].Err, failing) {
		t.Errorf("the commit was recorded with Err = %v, want the injected failure", commits[0].Err)
	}
	// And it still reports what it tried to write, so a sink can tell a failed
	// large commit from a failed empty one.
	if commits[0].Count != 1 || commits[0].Bytes <= 0 {
		t.Errorf("a failed commit reported Count=%d Bytes=%d", commits[0].Count, commits[0].Bytes)
	}

	// The sync that failed is recorded too, with the same error.
	syncs := sink.of(store.MetricSync)
	if len(syncs) == 0 {
		t.Fatal("the failed fsync produced no sync metric")
	}
	if !errors.Is(syncs[len(syncs)-1].Err, failing) {
		t.Errorf("the sync was recorded with Err = %v", syncs[len(syncs)-1].Err)
	}
}

// How long a snapshot was held, which is what lets a sink find the leaked one
// without keeping state of its own.
//
// Pinned rather than slept: a real elapsed time would make this a test about
// the clock's resolution, and asserting "greater than zero" against a fast
// machine is the same defect as a one-nanosecond traversal deadline. The store
// already indirects its clock so a test can hold it still, and holding it still
// is what turns "some duration" into an exact number.
func TestMetrics_ASnapshotReportsExactlyHowLongItWasHeld(t *testing.T) {
	s, sink := openWithSink(t)

	base := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	held := int64(1500 * time.Millisecond)
	now := base
	s.nowUnixNano = func() int64 { return now }

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	now = base + held
	if err := snap.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	closes := sink.of(store.MetricSnapshotClose)
	if len(closes) != 1 {
		t.Fatalf("got %d snapshot-close metrics, want 1", len(closes))
	}
	if closes[0].Count != held {
		t.Errorf("snapshot held for %d ns, reported %d", held, closes[0].Count)
	}
}

// The same claim on the transaction path, which is a separate commit site with
// its own error handling.
//
// Not a duplicate: ApplyTransaction frames its own batch and has its own
// sync/flush branch, so "the emission is on the shared path" has to hold in two
// places. A mutation that put it back on the success branch here survived a
// suite that only exercised AddNodesBatch.
func TestMetrics_AFailedTransactionIsStillRecorded(t *testing.T) {
	s, sink := openWithSink(t)

	failing := errors.New("device is on fire")
	s.wal.syncHook = func() error { return failing }
	defer func() { s.wal.syncHook = nil }()

	err := s.ApplyTransaction([]store.TxOp{
		{Kind: store.TxOpAddNode, Node: &store.Node{Labels: []store.NodeType{store.NodeTypeCase}}},
	})
	if !errors.Is(err, failing) {
		t.Fatalf("a failed fsync was not reported: %v", err)
	}

	commits := sink.of(store.MetricCommit)
	if len(commits) != 1 {
		t.Fatalf("a failed transaction produced %d commit metrics, want 1", len(commits))
	}
	if !errors.Is(commits[0].Err, failing) {
		t.Errorf("the transaction was recorded with Err = %v", commits[0].Err)
	}
	if commits[0].Count != 1 {
		t.Errorf("Count = %d, want the one operation the transaction carried", commits[0].Count)
	}
}
