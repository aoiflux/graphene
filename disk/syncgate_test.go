package disk

// Group commit, tested where it can be measured.
//
// These are white-box tests in the engine package because the thing under test
// is a syscall count, and no black-box API reports one. That is the honest
// place for them: the guarantee is "N commits cost fewer than N fsyncs", and
// from outside the package the only observable is wall-clock, which is not
// evidence.

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// TestGroupCommit_OneFsyncManyCommits is the whole point of the design: enough
// concurrent committers must share fsyncs rather than each paying for one.
//
// Mutation check: make the gate sync per caller — drop the `if g.syncing` wait
// in syncGate.await, so every committer leads — and this fails with
// issued == commits.
func TestGroupCommit_OneFsyncManyCommits(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	// A slow fsync is what makes a cohort form at all. On a fast device the
	// committers would file through one at a time and each would legitimately
	// lead its own sync — which is correct behaviour and proves nothing. The
	// delay stands in for the platter round-trip the design exists to amortise.
	var syncs atomic.Int64
	s.wal.syncHook = func() error {
		syncs.Add(1)
		time.Sleep(2 * time.Millisecond)
		return nil
	}

	const writers = 16
	const roundsEach = 8
	const commits = writers * roundsEach

	var wg sync.WaitGroup
	errs := make(chan error, commits)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < roundsEach; r++ {
				_, err := s.AddNodesBatch([]*store.Node{
					{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
					{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("AddNodesBatch: %v", err)
	}

	issued := syncs.Load()
	if issued >= commits {
		t.Fatalf("group commit did not group: %d commits cost %d fsyncs", commits, issued)
	}
	t.Logf("%d commits, %d fsyncs (%.1f commits per fsync)", commits, issued, float64(commits)/float64(issued))

	// And every record is actually there, which is the part that would be easy
	// to lose while chasing the count.
	got, err := s.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if want := uint64(commits * 2); got != want {
		t.Fatalf("lost records while grouping commits: want %d nodes, got %d", want, got)
	}
}

// TestGroupCommit_EpochNeverPrecedesDurability is the invariant that makes it
// safe to apply to the delta before the fsync: no reader may see a commit whose
// bytes are not on the medium.
//
// Mutation check: publish the epoch before AwaitSync in AddNodesBatch and this
// fails — the observer sees the count move while the fsync is still blocked.
func TestGroupCommit_EpochNeverPrecedesDurability(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	before, err := s.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}

	// Hold the fsync open. Everything the committer does up to it has happened;
	// nothing a reader can see may have.
	release := make(chan struct{})
	entered := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	s.wal.syncHook = func() error {
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil
	}
	// Released on the way out however the test ends. Without this a failing
	// assertion leaves the committer parked in the hook and Close waits on it
	// for ever, which turns a clear failure into a timeout.
	defer releaseOnce.Do(func() { close(release) })

	done := make(chan error, 1)
	go func() {
		_, err := s.AddNodesBatch([]*store.Node{
			{Labels: []store.NodeType{store.NodeTypeCase}},
			{Labels: []store.NodeType{store.NodeTypeCase}},
		})
		done <- err
	}()

	<-entered

	// The commit is written and applied but not durable. A reader must still see
	// the old graph — and, decisively, the store lock must be free, or the fsync
	// is still serialising writers and nothing has been gained.
	if got, err := s.NodeCount(); err != nil || got != before {
		t.Fatalf("a commit became visible before its fsync returned: want %d, got %d (err %v)", before, got, err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot while a commit is mid-fsync: %v", err)
	}
	if got, _ := snap.NodeCount(); got != before {
		t.Fatalf("a snapshot saw a commit whose fsync had not returned: want %d, got %d", before, got)
	}
	snap.Close()

	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatalf("AddNodesBatch: %v", err)
	}

	// And once it has returned, it is visible.
	if got, err := s.NodeCount(); err != nil || got != before+2 {
		t.Fatalf("commit not visible after its fsync returned: want %d, got %d (err %v)", before+2, got, err)
	}
}

// TestGroupCommit_FailedSyncStaysInvisible checks the other half of that
// invariant: a commit whose fsync fails must not be handed to a reader either.
func TestGroupCommit_FailedSyncStaysInvisible(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	before, err := s.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}

	failing := errors.New("device is on fire")
	s.wal.syncHook = func() error { return failing }
	// Cleared before Close, which syncs too — the failure being injected is a
	// commit-path one, not a shutdown one, and leaving it set would also leave
	// the log file open on Windows and break the temp-dir cleanup.
	defer func() { s.wal.syncHook = nil }()

	if _, err := s.AddNodesBatch([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeCase}},
	}); !errors.Is(err, failing) {
		t.Fatalf("a failed fsync was not reported: %v", err)
	}

	if got, err := s.NodeCount(); err != nil || got != before {
		t.Fatalf("a commit whose fsync failed became visible: want %d, got %d (err %v)", before, got, err)
	}
}

// TestSyncGate_SharesOneSync exercises the gate directly, without a store
// around it, so a failure points at the protocol rather than at the commit path
// that uses it.
//
// Tickets here are bare numbers standing in for WAL ring positions; the flush
// callback stands in for "drain the queue and fsync it" and reports how far it
// got, which is the only thing the gate knows about the log.
func TestSyncGate_SharesOneSync(t *testing.T) {
	g := newSyncGate()

	// Ten commits queued, then ten waiters arrive for them.
	const queued = 10

	// Closed by whichever call leads, so the main goroutine can wait for the
	// flush to be in flight without racing a send.
	inFlush := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int64
	flush := func() (uint64, error) {
		calls.Add(1)
		once.Do(func() { close(inFlush) })
		<-release
		// One flush carries everything queued when it ran.
		return queued, nil
	}

	var wg sync.WaitGroup
	for tk := uint64(1); tk <= queued; tk++ {
		wg.Add(1)
		go func(tk uint64) {
			defer wg.Done()
			if err := g.await(tk, flush); err != nil {
				t.Errorf("await(%d): %v", tk, err)
			}
		}(tk)
	}

	<-inFlush
	close(release)
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("ten waiters over one queued run issued %d flushes, want 1", n)
	}
	if got := g.syncsIssued(); got != 1 {
		t.Fatalf("syncsIssued: want 1, got %d", got)
	}

	// A ticket already covered costs nothing at all.
	if err := g.await(1, func() (uint64, error) {
		t.Fatal("a durable ticket started another flush")
		return 0, nil
	}); err != nil {
		t.Fatalf("await on a durable ticket: %v", err)
	}
}

// TestSyncGate_LateWriteLeadsAgain covers the case a leader's flush cannot
// cover: bytes queued after it drained need a flush of their own.
func TestSyncGate_LateWriteLeadsAgain(t *testing.T) {
	g := newSyncGate()

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64

	go func() {
		// The first flush drains through ticket 1 and no further.
		_ = g.await(1, func() (uint64, error) {
			calls.Add(1)
			close(started)
			<-release
			return 1, nil
		})
	}()

	<-started

	done := make(chan error, 1)
	go func() {
		// Ticket 2 was queued after that drain, so it needs its own flush.
		done <- g.await(2, func() (uint64, error) {
			calls.Add(1)
			return 2, nil
		})
	}()

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("await(2): %v", err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("a commit queued mid-flush was reported durable by it: %d flushes, want 2", n)
	}
}
