package main

// What a handler is given, and who owns the store.
//
// The framework opens the store and the framework closes it. No handler calls
// disk.Open, and no handler calls os.Exit. That is not tidiness — it is the fix
// for a real bug. Two commands used to call os.Exit(1) from the middle of a
// handler that held the store open under a deferred Close. os.Exit does not run
// deferred functions, so a `custody` that found a broken chain left the lock
// file behind, and the next process to try the store was told it was busy by a
// process that had already exited.
//
// Adding a Close before those two Exits would have fixed the two. Moving the
// open into the framework fixes the class: there is no longer anywhere in a
// handler that an early return can skip a Close, because the handler never held
// the thing in the first place.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
)

// OpenMode says what the framework opens before a handler runs.
type OpenMode uint8

const (
	// OpenNone touches no store: verify-proof, verify-backup, version.
	OpenNone OpenMode = iota
	// OpenFiles reads graphene.csr, graphene.wal and the ledgers directly and
	// takes no lock. Safe against a store another process is writing — which is
	// exactly the moment you most want to look at one.
	OpenFiles
	// OpenDiskRO is disk.OpenReadOnly: a shared lock, refused while a writer
	// holds the store.
	OpenDiskRO
	// OpenDiskRW is disk.Open: the exclusive lock.
	OpenDiskRW
	// OpenGraphRO is graphene.OpenReadOnly.
	OpenGraphRO
	// OpenGraphRW is graphene.Open.
	OpenGraphRW
)

// needsTarget reports whether this mode requires a store directory operand.
func (m OpenMode) needsTarget() bool { return m != OpenNone }

// writes reports whether this mode takes the exclusive lock.
func (m OpenMode) writes() bool { return m == OpenDiskRW || m == OpenGraphRW }

// readOnly downgrades a writing mode to its reading counterpart. A dry run gets
// this treatment automatically, so it cannot take the exclusive lock and cannot
// write, by construction rather than by the handler remembering to check.
func (m OpenMode) readOnly() OpenMode {
	switch m {
	case OpenDiskRW:
		return OpenDiskRO
	case OpenGraphRW:
		return OpenGraphRO
	default:
		return m
	}
}

// CtxTier records how much of a command's work a -timeout can actually reach.
//
// Most of the library has no context-taking variant — the whole bulk package,
// every Open (which replays the log and is the slow step for exactly the
// commands a deadline is aimed at), CustodyFor, and every ledger read. A
// deadline that silently does nothing is worse than no deadline, so each
// command declares which of these it is and the help text says so.
type CtxTier uint8

const (
	// CtxExact: every library call this command makes takes a context.
	CtxExact CtxTier = iota
	// CtxBetween: the CLI drives the loop and checks between items.
	CtxBetween
	// CtxAdvisory: one uninterruptible library call. The deadline warns and
	// does not interrupt.
	CtxAdvisory
)

// Context is what a handler gets. It owns every resource the handler uses.
type Context struct {
	Ctx     context.Context
	Globals *Globals
	Cmd     *Command
	Now     func() time.Time // injected so golden output is stable

	// Target is the operand — a store directory, a proof file — already
	// checked to be present for any command that needs one, so no handler
	// repeats `if dir == "" { return ... }`.
	Target string
	Args   []string

	disk  *disk.Store
	graph *graphene.Graph

	// metrics is the sink attached at open when -metrics was given, and nil
	// otherwise — a nil Options.Metrics is what makes the engine's emission
	// sites cost nothing.
	metrics *collector

	// tuneOpen is the command's openTuner, if its options type has one. Set by
	// invoke once the flags are parsed and read by open.
	tuneOpen openTune

	Out io.Writer // stdout; only a streaming command writes here
	Err io.Writer // stderr; notices
}

// Disk returns the opened store.
//
// It panics if the command's OpenMode did not ask for one. A registry that
// disagrees with its handler is a programming error, and a panic on the first
// test run is a better way to learn about it than a nil dereference in
// somebody's incident. A registry test walks every command and checks the
// pairing, so this should never fire outside development.
func (c *Context) Disk() *disk.Store {
	if c.disk == nil {
		panic("graphene: " + c.Cmd.Path() + " asked for the store but its OpenMode does not open one")
	}
	return c.disk
}

// Graph returns the opened graph. See Disk for the panic.
func (c *Context) Graph() *graphene.Graph {
	if c.graph == nil {
		panic("graphene: " + c.Cmd.Path() + " asked for the graph but its OpenMode does not open one")
	}
	return c.graph
}

// open acquires whatever the command declared, and returns the closer.
//
// Every path goes through OpenWithOptions rather than the four convenience
// constructors, because -metrics has to reach Options.Metrics and that can only
// be set at open time. The options below are exactly what each constructor sets
// — disk.OpenReadOnly is Options{ReadOnly: true} and Open is Options{} — so the
// modes mean what they meant before, with one field added.
func (c *Context) open(mode OpenMode) (func(), error) {
	if mode == OpenNone || mode == OpenFiles {
		return func() {}, nil
	}

	opts := ledgersPresent(c.Target)
	if c.Globals != nil && c.Globals.Metrics {
		c.metrics = newCollector()
		opts.Metrics = c.metrics
	}
	// Last, so a command's own flags see the finished Options and cannot be
	// overwritten by the framework's. ReadOnly is set after it for the one
	// field a tuner is not allowed to reach: -dry-run's downgrade is the
	// framework's guarantee, not a command's suggestion.
	if c.tuneOpen != nil {
		c.tuneOpen(&opts)
	}
	opts.ReadOnly = !mode.writes()

	switch mode {
	case OpenDiskRO, OpenDiskRW:
		s, err := disk.OpenWithOptions(c.Target, opts)
		if err != nil {
			return nil, err
		}
		c.disk = s
		return func() { _ = s.Close() }, nil

	case OpenGraphRO, OpenGraphRW:
		g, err := graphene.OpenWithOptions(c.Target, opts)
		if err != nil {
			return nil, err
		}
		c.graph = g
		return func() { _ = g.Close() }, nil
	}
	return func() {}, nil
}

// ledgersPresent opens a store the way it was built.
//
// The audit, redaction and grant ledgers are per-open options: a store whose
// directory holds graphene.audit but which is opened without Options.Audit has
// no audit log as far as the engine is concerned, and RecordAudit on it
// succeeds and writes nothing.
//
// That is right for a library — auditing is the caller's decision — and wrong
// for this tool, which is handed a directory and nothing else. It also had a
// consequence worth naming: `migrate` compacts, and a compaction on a store
// opened without Options.Audit is not recorded in the audit log. CustodyFor
// compares recorded compactions against retired segments precisely to catch a
// compaction that was never recorded, so the tool was manufacturing the exact
// anomaly the custody report exists to detect.
//
// So: if the file is there, the ledger is opened. The names are the on-disk
// format's, not this package's to choose; a store that had none gets none,
// because creating one would be a change nobody asked for.
//
// Under a read-only open the engine ignores all three (it will not open a
// ledger it cannot write), so setting them costs nothing there.
func ledgersPresent(dir string) disk.Options {
	has := func(name string) bool {
		_, err := os.Stat(filepath.Join(dir, name))
		return err == nil
	}
	return disk.Options{
		Audit:     has("graphene.audit"),
		Redaction: has("graphene.redactions"),
		Roles:     has("graphene.grants"),
	}
}

// watchdog warns, once, when an advisory deadline passes.
//
// It does not kill anything. A -timeout that hard-killed the process during a
// compaction or an import would manufacture precisely the torn write the engine
// exists to prevent, and would do it on the operator's behalf — the opposite of
// what somebody reaching for a timeout wants. Saying "this is still running"
// is the honest thing a tool can do here.
func (c *Context) watchdog(r *Result) func() {
	dl, ok := c.Ctx.Deadline()
	if !ok || c.Cmd.Tier != CtxAdvisory {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTimer(time.Until(dl))
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
			r.Notice("-timeout passed; %s cannot be interrupted and is still running — "+
				"nothing will be left half-written", c.Cmd.Path())
			if !c.Globals.Quiet && !c.Globals.JSON {
				_, _ = io.WriteString(c.Err,
					"graphene: -timeout passed; this command cannot be interrupted and is still running\n")
			}
		}
	}()
	return func() { close(done) }
}

// statAvailable reports whether target exists at all, so a missing directory is
// a clear message rather than whatever the store layer says about it.
func statAvailable(target string) error {
	if _, err := os.Stat(target); err != nil {
		return err
	}
	return nil
}
