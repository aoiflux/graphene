package store

// Cancelling a call that is not a traversal.
//
// traversal/guard.go already amortises this for the six walks, but it is bound
// up with Budget accounting and lives in a package the index and the backends
// cannot import. The four calls that needed cancelling next — VerifyIndexes,
// RebuildIndexes, QueryNodes, QueryEdges — have no budget to spend and no walk
// to bound. What they have is a loop over everything the store holds.
//
// The rule about *when* to look at the context is the part worth having in one
// place, for the same reason the traversal guard gives: checked per iteration it
// is a mutex acquire on the inner loop of a pass over every entry in the index,
// and checked once at the top it is not a cancellation at all.

import (
	"context"
)

// cancelCheckInterval is how many steps pass between context checks.
//
// The same 256 traversal/guard.go uses, and for the same reason: ctx.Err()
// takes a mutex on a cancellable context, which is cheap once and not cheap a
// million times. The two constants are deliberately not shared — a guard tick
// also reads the clock, so the number is answering a slightly different
// question there — but they are the same number because the cost being
// amortised is the same cost.
const cancelCheckInterval = 256

// CancelCheck amortises context checks across a long loop.
//
// Use it as a value in a local and pass its address to inner helpers; nothing
// stores the pointer, so escape analysis keeps it on the stack.
//
//	cc := store.NewCancelCheck(ctx)
//	if err := cc.Check(); err != nil {
//	    return err
//	}
//	for _, id := range everything {
//	    if err := cc.Step(); err != nil {
//	        return err
//	    }
//	    ...
//	}
type CancelCheck struct {
	ctx context.Context

	// off is the fast path. A context that can never be cancelled has nothing
	// to report, so every Step is one predictable branch and the loop it is in
	// runs at the speed it ran before the parameter existed.
	off bool

	ticks int
}

// NewCancelCheck returns a check over ctx. A nil ctx is treated as
// context.Background(), so a caller that forgot one gets the uncancellable
// path rather than a panic deep inside a loop.
func NewCancelCheck(ctx context.Context) CancelCheck {
	if ctx == nil {
		ctx = context.Background()
	}
	return CancelCheck{ctx: ctx, off: ctx.Done() == nil}
}

// Check reports the context's error immediately, without amortising.
//
// Call it before the work starts — a caller passing an already-cancelled
// context has every right to see that honoured before a lock is taken, and Step
// would not notice for another 256 iterations — and anywhere a single iteration
// is itself expensive enough to be worth a check of its own.
func (c *CancelCheck) Check() error {
	if c.off {
		return nil
	}
	return c.ctx.Err()
}

// Step charges one iteration and reports the context's error on every 256th.
func (c *CancelCheck) Step() error {
	if c.off {
		return nil
	}
	c.ticks++
	if c.ticks < cancelCheckInterval {
		return nil
	}
	c.ticks = 0
	return c.ctx.Err()
}
