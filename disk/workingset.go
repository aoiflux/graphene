package disk

// Capping the resident set on windows, which is the one mechanism in this
// programme that trims rather than kills.
//
// # Why this is a function and not an Option
//
// Every other bound in this engine is per-store: a budget refuses an open, a
// batch cap refuses a slice, a compaction policy fires on one store's figures.
// This one is not. SetProcessWorkingSetSizeEx applies to the process, so a
// second store opened with a different figure would silently move the first
// one's ceiling, and a library that does that to its host has substituted its
// own judgement for the caller's.
//
// That is the same reasoning the programme already applied to GOMEMLIMIT, which
// is soft, process-global, and deliberately never an Options field: the engine
// documents it as a deployment knob rather than setting it. The difference here
// is only that windows has no environment variable for this, so there has to be
// a call -- and the call says in its name and its signature that it is about the
// process. A host that wants it calls it once, itself, at startup.
//
// # What it does and what it costs
//
// A hard maximum working set makes the kernel trim pages out of the process
// rather than let it exceed the figure. Anonymous pages go to the pagefile and
// file-backed pages are simply dropped, so the process keeps running and gets
// slower -- which is the entire point, and the entire cost. It is the only
// mechanism in Phase 2 that can make a program slower in exchange for a smaller
// RSS, and whether that trade is worth making is not something a storage library
// can know.
//
// # It is read back, because a limit that did not apply is worse than none
//
// The same rule tests/ceiling_test.go applies to the harness applies here: a
// ceiling nobody is enforcing, reported as enforced, turns a measurement into a
// fiction and a bound into a comfort. So the figure is set and then read back
// through GetProcessWorkingSetSizeEx, and what is returned is what the kernel
// says it holds rather than what was asked for.

import (
	"errors"
	"fmt"
)

// WorkingSetLimit is a resident-set cap as the operating system reports it after
// the fact, not as it was requested.
type WorkingSetLimit struct {
	// MinBytes and MaxBytes are what the kernel says the limits now are.
	MinBytes uint64
	MaxBytes uint64

	// Hard reports that exceeding MaxBytes causes the kernel to trim rather than
	// being a hint it may ignore. False means a soft limit was installed, which
	// bounds nothing.
	Hard bool

	// Source names where MaxBytes came from when it was discovered rather than
	// stated, in the form StorageStats.MemoryBudgetSource uses. Empty when the
	// caller supplied the figure.
	Source string
}

// String renders the limit for a log line.
func (l WorkingSetLimit) String() string {
	kind := "soft"
	if l.Hard {
		kind = "hard"
	}
	if l.Source == "" {
		return fmt.Sprintf("working set %d..%d bytes (%s)", l.MinBytes, l.MaxBytes, kind)
	}
	return fmt.Sprintf("working set %d..%d bytes (%s, from %s)", l.MinBytes, l.MaxBytes, kind, l.Source)
}

// ErrWorkingSetUnsupported is returned by LimitWorkingSet on every platform that
// has no such mechanism.
//
// linux and darwin are in that set and are not missing anything: there is no
// per-process resident cap on either that is both enforced and safe to use here.
// RLIMIT_RSS is accepted and unenforced on every modern kernel, and RLIMIT_AS
// counts the mapped image's address space -- the class this programme
// deliberately moved memory into -- so a limit set against it refuses a store
// that fits. What those platforms have instead is Options.ResidentAdvice, which
// asks rather than caps, and a cgroup, which the operator sets from outside.
var ErrWorkingSetUnsupported = errors.New("this platform has no process working-set limit")

// LimitWorkingSet caps the resident set of the whole process at maxBytes, and
// returns what the kernel says the limits are afterwards.
//
// The cap is hard: exceeding it makes the kernel trim pages out of the process
// rather than refuse an allocation, so nothing fails and the process gets
// slower. Read the file comment before calling this -- it affects the whole
// program, including whatever else is linked into it.
//
// A maxBytes of zero or less returns ErrWorkingSetUnsupported's sibling: an
// error, rather than removing an existing limit, because "no limit" is a
// deliberate act and should be spelled that way rather than reached by passing a
// zero somebody computed.
func LimitWorkingSet(maxBytes int64) (WorkingSetLimit, error) {
	if maxBytes <= 0 {
		return WorkingSetLimit{}, fmt.Errorf("working-set limit of %d bytes: a limit must be positive", maxBytes)
	}
	return setWorkingSetLimit(uint64(maxBytes))
}

// LimitWorkingSetFromCeiling caps the process at a fraction of the memory
// ceiling it is already running under, and names the instrument that answered.
//
// The fraction is the one Options.DiscoverMemoryBudget takes and for the same
// reason: the figure is a whole-process one, nothing here knows what else the
// program has allocated, and a cap set at the container's own limit is a cap
// with no room in it. A machine's size is taken more conservatively than an
// enforced limit, because most of a machine belongs to something else.
//
// Returns ErrWorkingSetUnsupported where the platform has no cap, and an error
// naming the failure where nothing could be discovered -- never a silent
// no-limit, which is the "option asked for and not held" failure this package
// reports everywhere else.
func LimitWorkingSetFromCeiling() (WorkingSetLimit, error) {
	c, ok := readSysCeiling()
	if !ok {
		return WorkingSetLimit{}, errors.New("no memory ceiling could be discovered on this platform")
	}
	want := budgetFromCeiling(c)
	if want <= 0 {
		return WorkingSetLimit{}, fmt.Errorf("ceiling %s reported %d bytes, which yields no usable limit", c.Source, c.Bytes)
	}
	l, err := setWorkingSetLimit(uint64(want))
	if err != nil {
		return l, err
	}
	l.Source = c.Source
	return l, nil
}
