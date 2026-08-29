package disk

// Where the store reports what it did.
//
// Every emission in the engine goes through the two helpers here, so there is
// exactly one nil check and exactly one place that decides what a measurement
// costs when nobody is listening. See store.Metrics for the contract and
// store.Metric for what each field means per kind.

import (
	"time"

	"github.com/aoiflux/graphene/store"
)

// record emits one metric, or does nothing when no sink is attached.
//
// Callers must build m inside `if s.metrics != nil` when building it costs
// anything — a clock read, a stat, a count that has to be walked. Passing an
// expensive struct to a call that will discard it is the mistake this comment
// exists to prevent, and it is the reason record does not take a closure: a
// closure that captures locals allocates, on a path that runs per commit.
func (s *Store) record(m store.Metric) {
	if s.metrics == nil {
		return
	}
	s.metrics.Record(m)
}

// metricsOn reports whether anything is listening.
//
// Guard the *construction* of a measurement with this, not just its emission:
//
//	var started time.Time
//	if s.metricsOn() {
//	    started = time.Now()
//	}
//	...
//	if s.metricsOn() {
//	    s.record(store.Metric{Kind: store.MetricQuery, Duration: time.Since(started), ...})
//	}
//
// Two clock reads are ~40 ns, which is nothing beside a compaction and
// everything beside a 300 ns query.
func (s *Store) metricsOn() bool { return s.metrics != nil }

// sinceIfOn returns the elapsed time when a sink is attached and zero when not,
// so a call site does not need a second branch to avoid an unused clock read.
// started is the zero Time when metricsOn was false, and time.Since on that is
// meaningless — hence the explicit test rather than a subtraction.
func (s *Store) sinceIfOn(started time.Time) time.Duration {
	if s.metrics == nil || started.IsZero() {
		return 0
	}
	return time.Since(started)
}
