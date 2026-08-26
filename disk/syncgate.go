package disk

// Group commit: one write and one fsync for many commits.
//
// # What it replaces
//
// Every batch commit used to hold the store's write lock across both the WAL
// write and the fsync that made it durable. Two committers could therefore
// never overlap at all: each paid its own ~0.1–1 ms platter round-trip, one
// after another, and sixteen concurrent writers cost sixteen fsyncs. The device
// was idle for almost all of it — an fsync is a wait, not work.
//
// The fix is not to fsync less. It is to notice that one fsync makes *every*
// byte written before it durable, so a committer that arrives while a sync is
// already in flight has nothing to do but wait for it. The first committer in
// becomes the leader; the rest wait on the condition variable and return when
// the leader's sync covers them. N commits, one fsync.
//
// # Two things had to move for that to be true
//
// **The store lock has to be released first.** Nothing is shared while the
// fsync happens under Store.mu, because then no second committer can reach the
// gate to join the cohort. So the commit path splits: assign IDs, queue the WAL
// bytes and apply the delta under the lock, release it, and *then* wait for
// durability.
//
// That reordering is only safe because visibility is not the map write. A
// commit's records become visible when its epoch is published, and the epoch is
// published after AwaitSync returns — so a reader cannot observe a commit that
// a crash would take back. Without epochs this would be an ordinary durability
// bug; with them it is the point. It is also why snapshot isolation is built
// before group commit rather than beside it.
//
// **The leader has to own the write as well as the sync.** The first version
// wrote each commit's bytes in its own goroutine and only shared the fsync, and
// it grouped nothing at all on Windows: WriteFile blocks while
// FlushFileBuffers is in flight on the same handle, so the committers who
// should have been forming a cohort were instead stuck in write(). The measured
// write cost per commit went from 37 µs at one writer to 604 µs at four —
// exactly the fsync duration — and 640 commits still cost 639 fsyncs.
//
// So commits queue their framed bytes in the WAL's ring and the leader drains
// the queue and fsyncs it, holding the write lock across both. Nothing else
// touches the file while either is happening, which leaves the platform nothing
// to serialise. See WAL.QueueBatch and WAL.flushAndSync.

import (
	"sync"
)

// syncGate elects one flusher at a time and shares its work with everyone it
// covers.
//
// Tickets are WAL ring sequence numbers: ticket T is durable once the log has
// been written and synced through position T. That makes "durable up to T" a
// contiguous prefix of the log, which is what lets one comparison answer for a
// whole cohort.
type syncGate struct {
	mu   sync.Mutex
	cond *sync.Cond

	// synced is the highest ticket known to be on the medium.
	synced uint64

	// syncing is true while a leader is mid-flush. Followers wait rather than
	// starting a second one, which is the whole saving.
	syncing bool

	// issued counts the flushes actually performed through this gate. It is the
	// figure the whole design is about — commits per fsync — and the only way to
	// assert the saving is real rather than plausible.
	issued uint64
}

func newSyncGate() *syncGate {
	g := &syncGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// noteSynced records that the log is durable through upTo, for the paths that
// write and fsync directly — Sync, Checkpoint, Truncate, Rotate and Close.
//
// Without it those syncs would be invisible to the gate and a committer waiting
// on a ticket they already covered would issue a redundant one straight after.
func (g *syncGate) noteSynced(upTo uint64) {
	g.mu.Lock()
	if upTo > g.synced {
		g.synced = upTo
	}
	g.cond.Broadcast()
	g.mu.Unlock()
}

// await returns once ticket is durable, doing the work itself if nobody else is
// already doing it.
//
// flush writes everything queued, fsyncs it, and reports how far the log is now
// durable. It runs with g.mu released, which is what lets later committers queue
// behind it and be covered by it.
func (g *syncGate) await(ticket uint64, flush func() (uint64, error)) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for {
		if ticket <= g.synced {
			return nil
		}
		if g.syncing {
			// A leader is mid-flush. Whether it covers this ticket depends on
			// whether these bytes were queued before it drained, so on waking
			// the loop re-checks rather than assuming.
			g.cond.Wait()
			continue
		}

		g.syncing = true
		g.issued++
		g.mu.Unlock()
		done, err := flush()
		g.mu.Lock()
		g.syncing = false
		if err == nil && done > g.synced {
			g.synced = done
		}
		g.cond.Broadcast()

		if err != nil {
			// The waiters are deliberately not given this error. They loop, find
			// nobody flushing, and lead one themselves — a retry, which is a
			// better answer for them than a failure that belonged to someone
			// else's syscall.
			return err
		}
		if ticket <= g.synced {
			return nil
		}
		// Queued after the leader drained. Go round and lead; this is bounded by
		// one extra flush.
	}
}

// syncsIssued reports how many flushes this gate has performed.
func (g *syncGate) syncsIssued() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.issued
}
