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

// ctxCheckInterval is how many steps pass between context and clock checks.
//
// Per step would be wrong twice over: ctx.Err() takes a mutex on a cancellable
// context, and time.Now() is a vDSO call at best. Both are cheap in isolation
// and neither is cheap a million times. 256 keeps the worst-case overshoot
// after a cancel to a few microseconds of walking, which is far below any
// deadline a caller would set, while amortising the checks to nothing.
const ctxCheckInterval = 256

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
	if budget.MaxTime > 0 {
		g.deadline = time.Now().Add(budget.MaxTime)
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
func (g *guard) tick() error {
	g.ticks++
	if g.ticks < ctxCheckInterval {
		return nil
	}
	g.ticks = 0
	if err := g.ctx.Err(); err != nil {
		return err
	}
	if !g.deadline.IsZero() && time.Now().After(g.deadline) {
		return fmt.Errorf("%w: ran for more than %s", store.ErrBudgetExceeded, g.budget.MaxTime)
	}
	return nil
}

// enter is the check made once, before a walk starts.
//
// It catches an already-cancelled context — which the periodic checks would not
// notice for another 256 steps, and which a caller passing a dead context has
// every right to expect to be honoured immediately — and a budget so small the
// origin alone exceeds it.
func (g *guard) enter() error {
	if err := g.ctx.Err(); err != nil {
		return err
	}
	return nil
}

// maxRecursion bounds how deep a recursive walk may go.
//
// DFS, provenance and subgraph matching all recurse once per hop, and a
// goroutine stack that runs out is a crash rather than an error — the one
// failure a caller cannot handle. Depth limits usually bound this, but
// ProvenanceChain's default depth is generous and FindSubgraphMatches recurses
// per pattern node with no depth argument at all.
//
// 100 000 frames is far past any real traversal and far short of the default
// 1 GB goroutine stack limit, so it converts the crash into ErrBudgetExceeded
// without getting in the way of legitimate work.
const maxRecursion = 100_000

// descend charges one stack frame.
func (g *guard) descend(depth int) error {
	if depth > maxRecursion {
		return fmt.Errorf("%w: recursed more than %d levels", store.ErrBudgetExceeded, maxRecursion)
	}
	return nil
}
