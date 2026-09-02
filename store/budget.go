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
	// actually has to spend.
	//
	// # It is best-effort, and MaxNodes and MaxEdges are not
	//
	// MaxNodes and MaxEdges are charged per visit, so they are exact: the walk
	// stops on the step that crosses them. MaxTime cannot be, because the walk
	// has to look at a clock to know, and a clock is neither free to read nor
	// infinitely fine.
	//
	// Two consequences a caller should size a budget around:
	//
	//   - The overshoot is bounded by one check interval plus one step. The
	//     interval is small (traversal/guard.go names it), but nothing preempts
	//     a step that runs long — a single cold page read can outlast any
	//     MaxTime by itself, and no periodic check can prevent that.
	//
	//   - A MaxTime below the platform's monotonic clock granularity cannot be
	//     observed at all, so it does not stop anything. On Linux that
	//     granularity is nanoseconds and the point is theoretical. On Windows
	//     the Go runtime reads the system interrupt time, which advances only at
	//     the timer tick — 15.6 ms by default, and around 0.5 ms while some
	//     process on the machine has raised the timer resolution. It is a global
	//     property of the machine that changes as unrelated programs start and
	//     stop, so "how small can MaxTime usefully be" has no fixed answer there.
	//
	// Read that as: MaxTime is the backstop that keeps a runaway walk from
	// running all afternoon. It is not a scheduler, and a deadline in the
	// microseconds is a deadline in name only. Bound the work with MaxNodes and
	// MaxEdges, which are exact, and let MaxTime catch what they miss.
	//
	// A negative MaxTime is a deadline that has already passed: the walk is
	// refused before it visits anything. Zero is unlimited, so the zero Budget
	// keeps behaving exactly as it did.
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

// MaxRecursionDepth bounds how deep a recursive walk may go.
//
// The recursive walks — DFS, provenance, subgraph matching, and HasCycle —
// recurse once per hop, and a goroutine stack that runs out is a crash rather
// than an error: the one failure a caller cannot handle, and the one an
// embedded engine has no business inflicting on its host. Depth limits usually
// bound this, but ProvenanceChain's default depth is generous,
// FindSubgraphMatches recurses per pattern node with no depth argument at all,
// and HasCycle recurses to whatever maxDepth the caller passed.
//
// 100 000 frames is far past any real traversal and far short of the default
// 1 GB goroutine stack limit, so it converts the crash into ErrBudgetExceeded
// without getting in the way of legitimate work.
//
// It lives here rather than in traversal because HasCycle is not in traversal,
// and two copies of a limit are two limits.
const MaxRecursionDepth = 100_000
