package store

// Bounding a traversal.
//
// Depth was the only limit the engine offered, and depth does not bound
// anything on a graph with a hub in it. A single node of degree 100 000 puts
// 100 000 entries in a visited set at depth one; at depth two the same walk can
// exhaust memory before it returns a result, and there is no point at which it
// reports that it is in trouble. The caller's only recourse was to guess a
// depth low enough that no supernode could be reached, which is a guess about
// data they are traversing precisely because they do not know its shape.
//
// A Budget bounds the walk by what it actually consumes.

import (
	"errors"
	"time"
)

// Budget bounds a traversal. The zero Budget is unlimited, so an existing call
// that does not set one behaves exactly as it did.
//
// Limits are checked as the walk proceeds and the walk stops at the first one
// exceeded, returning ErrBudgetExceeded. It is a refusal, not a truncation: a
// partial answer that looks like a complete one is the failure mode this exists
// to prevent, so nothing partial is returned alongside the error.
type Budget struct {
	// MaxNodes caps the nodes visited, including the origin. Zero is unlimited.
	//
	// This is the limit that bounds memory: the visited set is the largest thing
	// a traversal holds, and it grows one entry per node reached.
	MaxNodes int

	// MaxEdges caps the edges crossed. Zero is unlimited.
	//
	// Distinct from MaxNodes because a dense graph crosses far more edges than
	// it visits nodes — a walk over a clique of 1 000 nodes crosses half a
	// million — so the work can run away while the node count looks modest.
	MaxEdges int

	// MaxTime caps wall-clock time from the start of the walk. Zero is
	// unlimited.
	//
	// The limit of last resort, and the only one expressed in what a caller
	// actually has to spend. Checked periodically rather than per step: reading
	// the clock on the inner loop of a walk that crosses a million edges costs
	// more than the walk.
	MaxTime time.Duration
}

// Unlimited reports whether b imposes no limit at all, which lets a traversal
// skip its accounting entirely.
func (b Budget) Unlimited() bool {
	return b.MaxNodes == 0 && b.MaxEdges == 0 && b.MaxTime == 0
}

// ErrBudgetExceeded is returned when a traversal hits one of its limits.
//
// Deliberately not distinguished per limit in the sentinel: a caller that wants
// to know which one hit can look at the wrapped message, and a caller handling
// the error almost always handles all three the same way — the walk was too big
// for what it was given.
var ErrBudgetExceeded = errors.New("graphene: traversal budget exceeded")
