package disk

// Bounded memory: acting on the compaction policy rather than reporting it.
//
// CompactionPolicy.Evaluate has always been able to say "this store is due",
// and nothing has ever acted on it. That was defensible while compaction froze
// every writer for the length of a whole-image rebuild — when to pay that is
// genuinely the caller's decision, and an engine that decided for them would be
// an engine that stalls at a moment of its own choosing. It is a much smaller
// decision now that a compaction blocks writers only for its pin and its
// commit, so the trigger is offered here.
//
// It is still off by default. What changed is the cost of saying yes, not who
// gets to say it.
//
// # Why this is the only goroutine in engine code
//
// There were none before, and adding one means a lifecycle. The rules are:
//
//   - It is started by Open and stopped by Close, and by nothing else.
//   - Close cancels it and then *waits* for it. A compaction that has reached
//     its commit stage ignores cancellation, so the wait is what stops Close
//     pulling the log and the ledgers out from under a rename in progress.
//   - Waiting is bounded by one commit stage, not one compaction: the build
//     between them checks ctx and gives up.
//
// Everything the goroutine touches — the log, the audit log, the redaction
// ledger, the process lock — is closed by Close after that wait and never
// before it.

import (
	"context"
	"sync"
	"time"

	"github.com/aoiflux/graphene/store"
)

// defaultAutoCompactInterval is how often the policy is evaluated when a caller
// enables auto-compaction without saying.
//
// Evaluation is a read lock, five comparisons and the O(1) resident estimate
// the fifth one reads, so the interval is not chosen to make it cheap — it is chosen to bound how far past a threshold a store can
// drift before anything notices. Thirty seconds is short against the growth
// rates the default policy trips on and long enough that an idle store spends
// no measurable time awake.
const defaultAutoCompactInterval = 30 * time.Second

// autoCompactor is the background trigger's state. Nil when disabled, which is
// the default and the only state the engine had before.
type autoCompactor struct {
	cancel context.CancelFunc
	done   sync.WaitGroup
}

// startAutoCompact launches the background trigger. Called once, from Open.
func (s *Store) startAutoCompact(policy store.CompactionPolicy, every time.Duration, obs store.CompactionObserver) {
	if every <= 0 {
		every = defaultAutoCompactInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &autoCompactor{cancel: cancel}
	s.auto = a

	a.done.Add(1)
	go func() {
		defer a.done.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}

			due, why := policy.Evaluate(s.StorageStats())
			if !due {
				continue
			}
			// Passing ctx is what makes Close's wait bounded: a cancellation
			// during the build abandons it, and one during the commit is
			// ignored so the image lands whole.
			err := s.CompactCtx(ctx)
			if obs != nil {
				obs.Compacted(why, err)
			}
		}
	}()
}

// stopAutoCompact cancels the trigger and waits for it to leave.
//
// Idempotent without a guard, which Close needs it to be: the store documents
// that the defer-plus-explicit-call pattern is harmless. Both halves already
// are — cancelling a cancelled context does nothing, and waiting on a
// WaitGroup whose counter has reached zero returns immediately — so a sync.Once
// around them would only be a claim that they are not.
func (s *Store) stopAutoCompact() {
	a := s.auto
	if a == nil {
		return
	}
	a.cancel()
	a.done.Wait()
}
