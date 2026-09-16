package disk

// The background compactor's lifecycle.
//
// The trigger itself is five comparisons in CompactionPolicy.Evaluate, which is
// tested where it lives. What is new and worth testing is that a goroutine now
// exists at all: that it fires, that Close waits for it rather than closing the
// files underneath it, and that Close stays idempotent.

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// recordingObserver collects what the background compactor reports.
type recordingObserver struct {
	mu      sync.Mutex
	reasons []string
	errs    []error
	runs    atomic.Int64
}

func (o *recordingObserver) Compacted(reason string, err error) {
	o.mu.Lock()
	o.reasons = append(o.reasons, reason)
	o.errs = append(o.errs, err)
	o.mu.Unlock()
	o.runs.Add(1)
}

func (o *recordingObserver) waitFor(t *testing.T, n int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if o.runs.Load() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("background compaction did not run %d time(s) within %s (ran %d)", n, within, o.runs.Load())
}

func TestAutoCompact_FiresAndReportsTheRuleThatFired(t *testing.T) {
	obs := &recordingObserver{}
	dir := t.TempDir()

	// A policy that a handful of writes trips, and a tick short enough that the
	// test does not have to wait on the default half-minute.
	policy := store.CompactionPolicy{MaxDeltaRecords: 8}
	s, err := OpenWithOptions(dir, Options{
		AutoCompact:         &policy,
		AutoCompactInterval: 5 * time.Millisecond,
		AutoCompactObserver: obs,
	})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer s.Close()

	var ids []store.NodeID
	for i := 0; i < 16; i++ {
		ids = append(ids, addNodeD(t, s, store.NodeTypeEvidenceFile))
	}

	obs.waitFor(t, 1, 5*time.Second)

	obs.mu.Lock()
	reason, cerr := obs.reasons[0], obs.errs[0]
	obs.mu.Unlock()
	if cerr != nil {
		t.Fatalf("background compaction failed: %v", cerr)
	}
	if reason == "" {
		t.Fatal("background compaction reported no rule")
	}

	// It compacted the store rather than merely deciding to: the image now holds
	// the records, and every one of them still reads back.
	if s.cur().csr == nil {
		t.Fatal("the background compaction published no image")
	}
	for _, id := range ids {
		if _, err := s.GetNode(id); err != nil {
			t.Fatalf("node %d lost by the background compaction: %v", id, err)
		}
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after a background compaction: %v", err)
	}
}

// TestAutoCompact_CloseWaitsForACompactionInFlight is the lifecycle test.
//
// Close closes the log, the audit log and the redaction ledger and releases the
// process lock. A compaction in its commit stage is holding all four. If Close
// does not wait, the compaction writes to closed files — which is not a lost
// compaction but a corrupt store, and on Windows a directory that cannot be
// cleaned up afterwards.
func TestAutoCompact_CloseWaitsForACompactionInFlight(t *testing.T) {
	obs := &recordingObserver{}
	dir := t.TempDir()

	policy := store.CompactionPolicy{MaxDeltaRecords: 4}
	s, err := OpenWithOptions(dir, Options{
		Audit:               true,
		AutoCompact:         &policy,
		AutoCompactInterval: time.Millisecond,
		AutoCompactObserver: obs,
	})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}

	// Close in the middle of a compaction's build. The hook runs on the
	// background goroutine, so blocking in it holds the compaction at exactly
	// the point Close has to reckon with. Installed before any write, so the
	// first compaction the policy triggers is the one that is caught.
	closing := make(chan struct{})
	held := make(chan struct{})
	var once sync.Once
	s.afterPinHook = func() {
		once.Do(func() {
			close(held)
			<-closing
		})
	}

	for i := 0; i < 64; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}

	<-held
	go func() {
		time.Sleep(5 * time.Millisecond)
		close(closing)
	}()

	// Close has to block here until the held compaction has finished its commit.
	if err := s.Close(); err != nil {
		t.Fatalf("Close during a background compaction: %v", err)
	}
	if obs.runs.Load() == 0 {
		t.Fatal("Close returned before the compaction it was racing had reported")
	}

	// The store reopens, which it would not if the compaction had written into
	// a half-closed log.
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after Close raced a background compaction: %v", err)
	}
	defer reopened.Close()
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after the race: %v", err)
	}
}

func TestAutoCompact_CloseIsStillIdempotent(t *testing.T) {
	dir := t.TempDir()
	policy := store.CompactionPolicy{MaxDeltaRecords: 4}
	s, err := OpenWithOptions(dir, Options{
		AutoCompact:         &policy,
		AutoCompactInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	for i := 0; i < 8; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// The second Close must not wait on a WaitGroup that is already released,
	// which is the deadlock the once guards.
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second Close blocked — stopAutoCompact is not idempotent")
	}
}

// TestAutoCompact_WaitsForThePolicy: the trigger is the policy, not the tick.
// A store under the threshold must be left alone however often it is evaluated.
func TestAutoCompact_WaitsForThePolicy(t *testing.T) {
	obs := &recordingObserver{}
	dir := t.TempDir()

	// Far above anything this test writes, so every tick evaluates to "not due".
	policy := store.CompactionPolicy{MaxDeltaRecords: 1_000_000}
	s, err := OpenWithOptions(dir, Options{
		AutoCompact:         &policy,
		AutoCompactInterval: time.Millisecond,
		AutoCompactObserver: obs,
	})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer s.Close()

	for i := 0; i < 32; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}
	time.Sleep(50 * time.Millisecond) // ~50 ticks

	if n := obs.runs.Load(); n != 0 {
		t.Fatalf("compacted %d time(s) without the policy firing", n)
	}
	if s.cur().csr != nil {
		t.Fatal("an image was published without the policy firing")
	}
}

func TestAutoCompact_OffByDefault(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if s.auto != nil {
		t.Fatal("a store opened with default options started a background goroutine")
	}
	for i := 0; i < 64; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}
	time.Sleep(20 * time.Millisecond)
	if s.cur().csr != nil {
		t.Fatal("something compacted a store that was never asked to")
	}
}

// TestAutoCompact_ReadOnlyStoreStartsNothing: a read-only store cannot compact,
// so a trigger on one would refuse on every tick forever.
func TestAutoCompact_ReadOnlyStoreStartsNothing(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	policy := store.CompactionPolicy{MaxDeltaRecords: 1}
	ro, err := OpenWithOptions(dir, Options{
		ReadOnly:            true,
		AutoCompact:         &policy,
		AutoCompactInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenWithOptions read-only: %v", err)
	}
	defer ro.Close()
	if ro.auto != nil {
		t.Fatal("a read-only store started a background compactor")
	}
	if err := ro.Compact(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only Compact: want ErrReadOnly, got %v", err)
	}
}
