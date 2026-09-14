package main

// Verdicts, faults and exit status.
//
// The distinction that organises this file: an *error* means the command could
// not answer the question, and a *verdict* is the answer. `custody` on a store
// held by a writer is an error. `custody` reporting that a node was never
// attested is a verdict, and a negative one is still a successful run of the
// command.
//
// This matters because it is the documented policy — "exit status is non-zero
// only when something is actually broken; an incomplete account is reported as
// findings and exits zero" — and until now that policy lived in the usage text
// and in seven hand-written os.Exit(1) calls, with nothing connecting them.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/aoiflux/graphene/disk"
)

// Verdict is a command's claim about what it checked.
type Verdict uint8

const (
	// VerdictNone is the absence of a claim: `info` and `wal` describe a store
	// rather than judging it.
	VerdictNone Verdict = iota
	// VerdictVerified means the command checked something and it held.
	VerdictVerified
	// VerdictFindings means the command checked, and what it found is
	// incomplete rather than wrong. Exits zero. Most stores are legitimately
	// not fully provisioned — never signed, never anchored, not retaining
	// history — and a tool that exited non-zero on that would be useless in the
	// scripts that most need it.
	VerdictFindings
	// VerdictBroken means the command checked and it did not hold.
	VerdictBroken
)

func (v Verdict) String() string {
	switch v {
	case VerdictVerified:
		return "ok"
	case VerdictFindings:
		return "findings"
	case VerdictBroken:
		return "broken"
	default:
		return "ok"
	}
}

// Severity grades a finding.
type Severity uint8

const (
	SevInfo Severity = iota
	SevWarn
	SevBroken
)

func (s Severity) String() string {
	switch s {
	case SevWarn:
		return "warn"
	case SevBroken:
		return "broken"
	default:
		return "info"
	}
}

// Finding is something the command noticed. Code is stable and greppable so a
// monitoring system can match on it; Message is the prose a person reads.
type Finding struct {
	Code     string   `json:"code"`
	Severity Severity `json:"severity"`
	Message  string   `json:"message"`
}

// MarshalJSON renders Severity as its name rather than its number.
func (f Finding) MarshalJSON() ([]byte, error) {
	type alias struct {
		Code     string `json:"code"`
		Severity string `json:"severity"`
		Message  string `json:"message"`
	}
	return json.Marshal(alias{f.Code, f.Severity.String(), f.Message})
}

// FaultKind classifies why a command could not answer.
type FaultKind uint8

const (
	FaultInternal FaultKind = iota
	FaultUsage              // missing operand, bad flag, unknown command
	FaultLocked             // another process holds the store
	FaultTimeout            // the deadline passed
	FaultRefused            // a mutation without -confirm
	FaultNotFound           // no such store, file or entity
	FaultIO                 // the filesystem said no
)

func (k FaultKind) String() string {
	switch k {
	case FaultUsage:
		return "usage"
	case FaultLocked:
		return "store_locked"
	case FaultTimeout:
		return "timeout"
	case FaultRefused:
		return "refused"
	case FaultNotFound:
		return "not_found"
	case FaultIO:
		return "io"
	default:
		return "internal"
	}
}

// Fault is an error the CLI has classified, carrying the operator-facing advice
// that goes with its kind.
type Fault struct {
	Kind FaultKind
	Err  error
	Hint string
}

func (f *Fault) Error() string { return f.Err.Error() }
func (f *Fault) Unwrap() error { return f.Err }

// Usagef reports a command line that was wrong.
func Usagef(format string, a ...any) error {
	return &Fault{Kind: FaultUsage, Err: fmt.Errorf(format, a...)}
}

// Refusedf reports a mutation declined for want of -confirm.
func Refusedf(format string, a ...any) error {
	return &Fault{Kind: FaultRefused, Err: fmt.Errorf(format, a...)}
}

// classify determines what kind of failure err is, and what to tell the
// operator about it. Library errors that were never wrapped in a Fault are
// probed here, so a new sentinel in disk/ does not silently become "internal".
func classify(err error) (FaultKind, string) {
	var f *Fault
	if errors.As(err, &f) {
		hint := f.Hint
		if hint == "" {
			hint = hintFor(f.Kind)
		}
		return f.Kind, hint
	}
	switch {
	case errors.Is(err, disk.ErrStoreLocked):
		return FaultLocked, hintFor(FaultLocked)
	case errors.Is(err, disk.ErrReplayBudget):
		// A refusal, not a defect: the store is intact and the budget is the
		// caller's own. Without this it would land in FaultInternal with no
		// hint, which is the outcome this switch exists to prevent — and the
		// operator most likely to see it is one who set a budget precisely
		// because they were worried about memory.
		return FaultRefused, "the store is intact; this open was refused by a " +
			"configured replay budget. `store info` reports what the log holds, " +
			"and compacting the store is what makes the log small again."
	case errors.Is(err, disk.ErrMemoryBudget):
		// The same posture, for the budget that covers the whole operation
		// rather than the log. The advice differs from the replay case in the
		// one way that matters: compacting is the remedy there and is itself
		// one of the things that can be refused here. So the hint points at the
		// figures rather than at an action, and it does not send the operator to
		// `store info` — that reports the image and log as they are on disk, and
		// this refusal is about a model of what holding them would cost.
		return FaultRefused, "the store is intact and nothing was allocated; " +
			"this was refused by a configured memory budget. The message names " +
			"what the operation needed against what was budgeted, so the choice " +
			"is to raise Options.MemoryBudget or to open with cheaper Options."
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return FaultTimeout, hintFor(FaultTimeout)
	case os.IsNotExist(underlying(err)):
		return FaultNotFound, ""
	}
	return FaultInternal, ""
}

// hintFor is the advice that belongs with a kind of failure.
func hintFor(k FaultKind) string {
	switch k {
	case FaultLocked:
		// Contention is the one failure here that is not a defect in the
		// store, and an operator who reads "store is held by another process"
		// as corruption will start reaching for repairs this tool deliberately
		// does not offer. Say what it actually is, and name the commands that
		// work regardless.
		return "the store is intact — another process simply has it open. " +
			"info, csr and wal read the files directly and work anyway."
	case FaultTimeout:
		return "raise -timeout, or run the command without one: " +
			"nothing was left half-written."
	default:
		return ""
	}
}

// exitFor maps a fault kind and a verdict onto a process exit status.
//
// The mapping is deliberately coarse, and stays the mapping this tool has
// always had: 0 ran, 1 failed, 2 the command line was wrong. Richer codes were
// considered and rejected for the default path — a CI script written as
// `[ $? -eq 1 ]` would keep running and quietly stop meaning what it said. The
// -json envelope carries the full classification in its "status" and "error.kind"
// fields, which is where a caller that wants the distinction should read it.
func exitFor(kind FaultKind, v Verdict, failed bool) int {
	switch {
	case failed && kind == FaultUsage:
		return 2
	case failed:
		return 1
	case v == VerdictBroken:
		return 1
	default:
		return 0
	}
}
