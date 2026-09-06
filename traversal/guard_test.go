package traversal

// Deadline enforcement, tested against a clock the test owns.
//
// These live in the traversal package rather than beside the other budget tests
// because they swap timeNow, and because that is the only way to assert what a
// deadline actually promises. Against the real clock a MaxTime test asserts two
// things at once — that the guard checks, and that the machine's clock advanced
// far enough during the walk for the check to see anything — and only the first
// of those is about this code. The second depends on the platform's timer
// granularity, which on Windows is the system interrupt tick: 15.6 ms by
// default, ~0.5 ms while some unrelated program has raised the resolution, and
// therefore different from one run to the next on one machine. A test written
// against it passes or fails according to what else is running.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoiflux/graphene/memory"
	"github.com/aoiflux/graphene/store"
)

// fakeClock advances by a fixed step on every read.
//
// The step models clock granularity directly: a step of zero is a clock that
// never appears to move, which is exactly what a walk shorter than one timer
// tick observes on a coarse platform.
type fakeClock struct {
	now   time.Time
	step  time.Duration
	reads int
}

func (c *fakeClock) read() time.Time {
	c.reads++
	t := c.now
	c.now = c.now.Add(c.step)
	return t
}

// installClock points timeNow at c for the duration of the test.
func installClock(t *testing.T, c *fakeClock) {
	t.Helper()
	prev := timeNow
	timeNow = c.read
	t.Cleanup(func() { timeNow = prev })
}

func newClock(step time.Duration) *fakeClock {
	// A fixed instant, so a failure message reads the same on every machine.
	return &fakeClock{now: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), step: step}
}

// TestGuard_ZeroMaxTimeNeverReadsTheClock is the compatibility assertion for the
// indirection itself: a caller who set no deadline must not pay for one, and the
// cheapest proof is that the clock was never consulted.
func TestGuard_ZeroMaxTimeNeverReadsTheClock(t *testing.T) {
	c := newClock(time.Second)
	installClock(t, c)

	s := memory.New()
	ids := buildChain(s, 50, store.EdgeTypeContains)

	if _, err := BFSCtx(context.Background(), s, ids[0], 50, store.DirectionOutbound, nil, store.Budget{}); err != nil {
		t.Fatalf("unbounded walk: %v", err)
	}
	if _, err := BFSCtx(context.Background(), s, ids[0], 50, store.DirectionOutbound, nil,
		store.Budget{MaxNodes: 1 << 20}); err != nil {
		t.Fatalf("walk bounded by MaxNodes: %v", err)
	}
	if c.reads != 0 {
		t.Fatalf("a walk with no MaxTime read the clock %d times, want 0", c.reads)
	}
}

// TestGuard_DeadlineArrivedIsRefusedAtEntry covers the case enter()'s doc
// comment always claimed and the code did not have: a MaxTime the origin alone
// cannot afford must stop the walk before it visits anything, not 256 steps
// later.
func TestGuard_DeadlineArrivedIsRefusedAtEntry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxTime time.Duration
	}{
		{"negative", -time.Second},
		{"already elapsed", time.Second}, // the clock jumps an hour on first read
	} {
		t.Run(tc.name, func(t *testing.T) {
			step := time.Duration(0)
			if tc.name == "already elapsed" {
				step = time.Hour
			}
			c := newClock(step)
			installClock(t, c)

			g := newGuard(context.Background(), store.Budget{MaxTime: tc.maxTime})
			err := g.enter()
			if !errors.Is(err, store.ErrBudgetExceeded) {
				t.Fatalf("enter with MaxTime %s: want ErrBudgetExceeded, got %v", tc.maxTime, err)
			}
			if g.nodes != 0 {
				t.Fatalf("refused at entry but charged %d nodes", g.nodes)
			}
		})
	}
}

// TestGuard_DeadlineArrivedIsExceeded pins the comparison operator.
//
// The deadline lands exactly on a clock reading, which is not a corner case on a
// coarse clock — it is the common case, because every reading inside one timer
// tick is the same instant. `After` is strictly greater and would call that not
// yet expired, deferring the stop by another whole interval for no reason. The
// budget is spent when the deadline arrives.
func TestGuard_DeadlineArrivedIsExceeded(t *testing.T) {
	const budget = 5 * time.Second

	// The first read is newGuard's, and it advances the clock by exactly the
	// budget; freezing it afterwards leaves every later reading sitting on the
	// deadline rather than past it.
	c := newClock(budget)
	installClock(t, c)
	g := newGuard(context.Background(), store.Budget{MaxTime: budget})
	c.step = 0

	if err := g.enter(); !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("a deadline exactly reached: want ErrBudgetExceeded, got %v", err)
	}
}

// TestGuard_ShortButSlowWalkIsStopped is the defect this file exists for.
//
// The walk charges far fewer than ctxCheckInterval steps, so under the old
// single-cadence tick it never reached the clock at all and MaxTime went
// unenforced no matter how far past it the walk ran. Steps are not uniformly
// cheap — a handful of them over cold pages can outlast a budget meant for
// thousands — so this is the shape that costs something in production, and it is
// not reachable from a test that relies on wall-clock time passing.
func TestGuard_ShortButSlowWalkIsStopped(t *testing.T) {
	c := newClock(time.Second) // each charged step "takes" a second
	installClock(t, c)

	s := memory.New()
	const chain = 20
	ids := buildChain(s, chain, store.EdgeTypeContains)

	// A BFS over a chain charges roughly three ticks per node. The fixture has
	// to sit in the window that used to be unreachable — past the deadline
	// cadence, nowhere near the cancellation one — or it proves nothing.
	steps := len(ids) * 3
	if steps < deadlineCheckInterval || steps >= ctxCheckInterval {
		t.Fatalf("fixture charges about %d steps: need it between %d and %d to prove the point",
			steps, deadlineCheckInterval, ctxCheckInterval)
	}

	_, err := BFSCtx(context.Background(), s, ids[0], chain, store.DirectionOutbound, nil,
		store.Budget{MaxTime: 3 * time.Second})
	if !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("a %d-node walk overran its MaxTime without being stopped: %v", len(ids), err)
	}
}

// TestGuard_DeadlineCadence pins the cadence rather than the outcome.
//
// Two things, because the number and the property it buys can drift apart. The
// arithmetic holds for any interval. The bound below it does not: a deadline
// checked on the same 256-step cadence as cancellation is the defect this file
// was written for, and the only thing stopping a future benchmark from quietly
// restoring it is an assertion that says so.
func TestGuard_DeadlineCadence(t *testing.T) {
	c := newClock(0) // frozen: no walk can end early, so every step is charged
	installClock(t, c)

	g := newGuard(context.Background(), store.Budget{MaxTime: time.Hour})
	before := c.reads
	steps := 8 * deadlineCheckInterval
	for i := range steps {
		if err := g.step(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if got, want := c.reads-before, steps/deadlineCheckInterval; got != want {
		t.Fatalf("%d steps read the clock %d times, want %d (deadlineCheckInterval = %d)",
			steps, got, want, deadlineCheckInterval)
	}

	// A deadline must be checked far more often than a cancellation, or a short
	// walk never checks at all. The margin is generous on purpose — 16 is what
	// the benchmark chose and there is room to move it — but collapsing the two
	// cadences back together is the regression, and this is what catches it.
	if deadlineCheckInterval*8 > ctxCheckInterval {
		t.Fatalf("deadlineCheckInterval %d is too close to ctxCheckInterval %d: a deadline is "+
			"not a cancellation and must not be checked on the same cadence",
			deadlineCheckInterval, ctxCheckInterval)
	}
}

// TestGuard_SubGranularityDeadlineCannotStopAWalk is the contract, asserted
// rather than discovered.
//
// A frozen clock is what a walk that finishes inside one timer tick sees, and no
// comparison can find a deadline exceeded when no time has been observed to
// pass: the deadline is start+MaxTime and every reading is start. MaxTime is
// documented as best-effort with exactly this floor, so the walk completing is
// the correct behaviour and not a bug — and a caller who needs a hard bound has
// MaxNodes and MaxEdges, which are exact. Asserting it here is what keeps the
// documentation and the code from drifting apart.
func TestGuard_SubGranularityDeadlineCannotStopAWalk(t *testing.T) {
	c := newClock(0)
	installClock(t, c)

	s := memory.New()
	ids := buildChain(s, 50, store.EdgeTypeContains)

	res, err := BFSCtx(context.Background(), s, ids[0], 50, store.DirectionOutbound, nil,
		store.Budget{MaxTime: time.Nanosecond})
	if err != nil {
		t.Fatalf("a deadline below clock granularity must not stop a walk, got %v", err)
	}
	if len(res.Nodes) != len(ids) {
		t.Fatalf("walk reached %d nodes, want %d", len(res.Nodes), len(ids))
	}

	// The same budget expressed as work is exact, which is the alternative the
	// documentation points a caller at.
	if _, err := BFSCtx(context.Background(), s, ids[0], 50, store.DirectionOutbound, nil,
		store.Budget{MaxNodes: 10}); !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("MaxNodes 10 over a 50-node chain: want ErrBudgetExceeded, got %v", err)
	}
}

// TestGuard_GenerousDeadlineDoesNotChangeTheWalk keeps the eager cadence from
// changing an answer. Checking the clock every step is only affordable if it is
// also invisible.
func TestGuard_GenerousDeadlineDoesNotChangeTheWalk(t *testing.T) {
	c := newClock(time.Millisecond)
	installClock(t, c)

	s := memory.New()
	ids := buildChain(s, 200, store.EdgeTypeContains)

	want, err := BFSCtx(context.Background(), s, ids[0], 200, store.DirectionOutbound, nil, store.Budget{})
	if err != nil {
		t.Fatalf("unbounded: %v", err)
	}
	got, err := BFSCtx(context.Background(), s, ids[0], 200, store.DirectionOutbound, nil,
		store.Budget{MaxTime: time.Hour})
	if err != nil {
		t.Fatalf("with a generous deadline: %v", err)
	}
	if len(got.Nodes) != len(want.Nodes) || len(got.Edges) != len(want.Edges) {
		t.Fatalf("a deadline it never reached changed the walk: want %d nodes/%d edges, got %d/%d",
			len(want.Nodes), len(want.Edges), len(got.Nodes), len(got.Edges))
	}
}

// unitCost is the cost function for guard tests: every edge costs one, so a
// weighted walk covers the same ground an unweighted one does and the only
// thing under test is the budget.
func unitCost(store.IncidentEdge) float64 { return 1 }

// TestGuard_DeadlineAppliesToEveryWalk checks the guard is doing this for all of
// them and not just BFS. The four loop shapes charge through different methods —
// visitNode, crossEdge, step, descend — and the cadence split lives in tick, so
// a walk that reaches tick only through a path this misses would be unbounded.
func TestGuard_DeadlineAppliesToEveryWalk(t *testing.T) {
	s := memory.New()
	ids := buildChain(s, 20, store.EdgeTypeContains)
	last := ids[len(ids)-1]
	tight := store.Budget{MaxTime: 3 * time.Second}

	walks := map[string]func() error{
		"BFSCtx": func() error {
			_, err := BFSCtx(context.Background(), s, ids[0], 20, store.DirectionOutbound, nil, tight)
			return err
		},
		"BFSIDsCtx": func() error {
			_, err := BFSIDsCtx(context.Background(), s, ids[0], 20, store.DirectionOutbound, nil, tight)
			return err
		},
		"DFSCtx": func() error {
			_, err := DFSCtx(context.Background(), s, ids[0], 20, store.DirectionOutbound, nil, tight)
			return err
		},
		"ProvenanceChainCtx": func() error {
			_, err := ProvenanceChainCtx(context.Background(), s, last, 20, nil, tight)
			return err
		},
		"ShortestPathCtx": func() error {
			_, err := ShortestPathCtx(context.Background(), s, ids[0], last, nil, tight)
			return err
		},
		"ShortestWeightedPathCtx": func() error {
			_, err := ShortestWeightedPathCtx(context.Background(), s, ids[0], last, nil, unitCost, tight)
			return err
		},
		"AStarPathCtx": func() error {
			_, err := AStarPathCtx(context.Background(), s, ids[0], last, nil, unitCost, nil, tight)
			return err
		},
	}

	for name, walk := range walks {
		t.Run(name, func(t *testing.T) {
			c := newClock(time.Second)
			installClock(t, c)
			if err := walk(); !errors.Is(err, store.ErrBudgetExceeded) {
				t.Fatalf("%s overran its MaxTime without being stopped: %v", name, err)
			}
		})
	}
}
