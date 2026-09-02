package traversal

// The budget and cancellation accounting shared by every traversal here.
//
// # Why this is a type rather than a few counters
//
// The four walks in this package each have their own loop shape — level
// buffers, a recursive kernel, two frontiers meeting in the middle, a
// backtracking matcher — and the one thing they must agree on is *when* a walk
// has gone too far. Putting the rule in one place is what keeps "MaxNodes: 1000"
// from meaning four slightly different things.
//
// # What it costs when nothing is set
//
// Nothing measurable. A zero Budget with a background context sets `off`, and
// every method returns nil on a single boolean test that the compiler inlines.
// The engine's own benchmarks are the reason: a BFS over a large graph runs its
// inner loop millions of times, and CONTRIBUTING is explicit that a cost like
// that is measured before it is assumed to be free.

import (
	"context"
	"fmt"
	"time"

	"github.com/aoiflux/graphene/store"
)

// ctxCheckInterval is how many steps pass between context checks.
//
// Per step would be wrong twice over: ctx.Err() takes a mutex on a cancellable
// context, and time.Now() is a vDSO call at best. Both are cheap in isolation
// and neither is cheap a million times. 256 keeps the worst-case overshoot
// after a cancel to a few microseconds of walking, which is far below any
// deadline a caller would set, while amortising the checks to nothing.
const ctxCheckInterval = 256

// deadlineCheckInterval is the cadence used instead when a MaxTime is set.
//
// 256 was answering a question about cancellation, where overshooting by a few
// microseconds costs nothing because the caller has already stopped caring about
// the answer. It was the wrong number for a deadline. A walk that charged fewer
// than 256 steps never reached the check at all, so MaxTime went unenforced on
// exactly the walks a caller is least able to predict: few steps, each of them
// slow, which is what a traversal over cold pages looks like. MaxNodes and
// MaxEdges never had that hole, because they are charged per visit.
//
// 16 rather than 1, and that is measured, not assumed. Per step is the obvious
// answer and it costs about a fifth of the walk: on BenchmarkBFS_Deep_Budget,
// interleaved over ten runs, the /time arm was 2.712 ms against /nodes at
// 2.317 ms, while /unbounded and every control sat still. At 16 the same arm is
// 2.185 ms against /nodes at 2.205 ms — indistinguishable, and the only
// significant movement in the whole comparison (-19.4%, p=0.003). The clock read
// is cheap; doing it on the inner loop of a walk that charges three ticks per
// node is not.
//
// It is only paid by callers who set MaxTime — with no deadline the guard runs
// the code it always ran — so the number is answering "how little can a deadline
// overshoot for free", not "how little can a walk afford".
//
// This bounds the overshoot at 16 charged steps, which for a BFS is five or six
// nodes. It does not bound the step: nothing here can interrupt a single read
// that outlasts the whole budget, and store/budget.go says so where a caller
// sizing a Budget will read it.
const deadlineCheckInterval = 16

// timeNow is time.Now, indirected so a test can drive the clock.
//
// A traversal cannot test its own deadline handling against the real clock. The
// Go monotonic reading advances at the platform's timer granularity — on Windows
// that is the system interrupt tick, 15.6 ms by default and machine-global — so
// whether a walk observes any elapsed time at all depends on how busy the box is
// and on what unrelated software has done to the timer resolution. A test built
// on that asserts that the clock ticked, not that the guard works, and it fails
// on whichever machine happens to be quiet. guard_test.go swaps this instead.
//
// It costs an indirect call the compiler cannot inline, and it is only ever
// reached when MaxTime is set: newGuard tests the budget first, and tick tests
// the deadline first.
var timeNow = time.Now

// guard holds one traversal's limits and what it has spent.
type guard struct {
	ctx    context.Context
	budget store.Budget

	// off is the fast path: no limits and no cancellable context, so every
	// check is a single test.
	off bool

	nodes int
	edges int

	deadline time.Time
	ticks    int
}

// newGuard returns a value, not a pointer.
//
// Deliberate: a *guard would be one heap allocation per traversal, which the
// interleaved benchmark caught as +1 alloc/op on BFS3Hop_Disk against HEAD.
// Callers keep it in a local and pass &local to the recursive kernels; escape
// analysis keeps it on the stack because nothing stores the pointer.
func newGuard(ctx context.Context, budget store.Budget) guard {
	if ctx == nil {
		ctx = context.Background()
	}
	g := guard{ctx: ctx, budget: budget}
	// A background context can never be cancelled, so with no budget there is
	// nothing to check and the walk should not pay to find that out repeatedly.
	g.off = budget.Unlimited() && ctx.Done() == nil
	// Non-zero rather than positive. A negative MaxTime used to be ignored here
	// while Unlimited() still reported false, so the walk paid for accounting
	// and enforced nothing — the one reading of "minus one second" that is not
	// a limit and not unlimited either. It is a deadline already passed, and
	// enter() refuses it.
	if budget.MaxTime != 0 {
		g.deadline = timeNow().Add(budget.MaxTime)
	}
	return g
}

// visitNode charges one node against the budget.
func (g *guard) visitNode() error {
	if g.off {
		return nil
	}
	g.nodes++
	if g.budget.MaxNodes > 0 && g.nodes > g.budget.MaxNodes {
		return fmt.Errorf("%w: visited more than %d nodes", store.ErrBudgetExceeded, g.budget.MaxNodes)
	}
	return g.tick()
}

// crossEdge charges one edge against the budget.
func (g *guard) crossEdge() error {
	if g.off {
		return nil
	}
	g.edges++
	if g.budget.MaxEdges > 0 && g.edges > g.budget.MaxEdges {
		return fmt.Errorf("%w: crossed more than %d edges", store.ErrBudgetExceeded, g.budget.MaxEdges)
	}
	return g.tick()
}

// step charges work that is neither a node nor an edge — an expansion, a
// recursion, a candidate tried — so a walk that spends its time without
// visiting anything is still bounded.
func (g *guard) step() error {
	if g.off {
		return nil
	}
	return g.tick()
}

// tick performs the periodic context and clock checks.
//
// Two cadences, because they are answering two questions. Without a deadline
// this is the loop it has always been: one ctx.Err() every 256 steps. With one,
// both checks run on the much shorter deadline cadence, and only the caller who
// asked for a deadline pays for it.
func (g *guard) tick() error {
	g.ticks++
	if g.deadline.IsZero() {
		if g.ticks < ctxCheckInterval {
			return nil
		}
		g.ticks = 0
		return g.ctx.Err()
	}
	if g.ticks < deadlineCheckInterval {
		return nil
	}
	g.ticks = 0
	if err := g.ctx.Err(); err != nil {
		return err
	}
	return g.expired()
}

// expired reports the deadline as exceeded if it has arrived.
//
// Arrived, not passed: !Before rather than After. A monotonic clock advances in
// ticks, so two readings inside one tick are equal rather than ordered, and
// After would call a deadline that landed exactly on a reading not yet reached,
// deferring the stop by another whole interval for no reason. The difference is
// invisible on a fine clock and is the common case on a coarse one.
func (g *guard) expired() error {
	if !timeNow().Before(g.deadline) {
		return fmt.Errorf("%w: ran for more than %s", store.ErrBudgetExceeded, g.budget.MaxTime)
	}
	return nil
}

// enter is the check made once, before a walk starts.
//
// It catches an already-cancelled context — which the periodic checks would not
// notice for another 256 steps, and which a caller passing a dead context has
// every right to expect to be honoured immediately — and a deadline that has
// already arrived, which is a MaxTime the origin alone cannot afford.
func (g *guard) enter() error {
	if err := g.ctx.Err(); err != nil {
		return err
	}
	if g.deadline.IsZero() {
		return nil
	}
	return g.expired()
}

// maxRecursion is store.MaxRecursionDepth, kept as a local name so the checks
// below read as they did. The number and the argument for it are in
// store/budget.go, beside Budget and ErrBudgetExceeded, because HasCycle is
// outside this package and needs the same limit.
const maxRecursion = store.MaxRecursionDepth

// descend charges one stack frame.
func (g *guard) descend(depth int) error {
	if depth > maxRecursion {
		return fmt.Errorf("%w: recursed more than %d levels", store.ErrBudgetExceeded, maxRecursion)
	}
	return nil
}
