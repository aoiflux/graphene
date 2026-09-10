package main

// run is the whole program, minus the process.
//
// main() is three lines and this returns an int, so every path through the tool
// — including every failure and every exit status — is reachable from a test
// with no subprocess and no captured file descriptors. The package had no tests
// at all before this; CI proved only that it compiled.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"
)

// run executes one invocation and returns the exit status.
func run(argv []string, stdout, stderr io.Writer) int {
	g := &Globals{}

	// Config, then environment, then flags — each layer overriding the one
	// before it, and the command line always winning. A config may set only the
	// reporting flags; see config.go for why -confirm and -dry-run are not
	// among them.
	cfg, err := loadConfig()
	if err != nil {
		return fail(stderr, "", err)
	}
	cfg.apply(g)
	applyEnv(g)

	rest, err := splitGlobals(argv, g)
	if err != nil {
		return fail(stderr, "", err)
	}

	res := resolve(rest)
	switch {
	case res.helpTopic != nil || (res.cmd == nil && res.groupHelp == "" && res.unknown == "" && len(rest) == 0):
		return writeHelp(stdout, res.helpTopic)
	case res.groupHelp != "":
		writeGroupHelp(stdout, res.groupHelp)
		return 0
	case res.unknown != "":
		if res.unknownFor != "" {
			return fail(stderr, "", Usagef(
				"%s is not a %s command — try one of: %s",
				res.unknown, res.unknownFor, verbList(res.unknownFor)))
		}
		return fail(stderr, "", Usagef("unknown subcommand %q%s", res.unknown, didYouMean(res.unknown)))
	case res.cmd == nil:
		return writeHelp(stdout, nil)
	}

	return invoke(res.cmd, res.args, g, cfg, stdout, stderr)
}

// invoke parses the command's flags, opens what it declared, runs it and
// renders the result.
func invoke(c *Command, args []string, g *Globals, cfg *Config, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(c.Path(), flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { writeCommandHelp(stderr, c, fs) }

	handler := c.newFlags(fs)
	bindGlobals(fs, g)

	var confirm bool
	if c.Mutates {
		fs.BoolVar(&confirm, "confirm", false,
			"required: this changes bytes already on disk")
	}

	if err := fs.Parse(args); err != nil {
		// flag has already written the offending flag and called Usage. Adding
		// our own line here is the classic double-print.
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	g.Confirm = confirm
	if g.Indent {
		g.JSON = true
	}

	ctx, cancel := rootContext(g)
	defer cancel()

	// A profile name in the operand position, resolved to its directory. A path
	// that exists always wins, so a profile can never shadow a real directory
	// and adding one cannot change what an existing command means.
	target := fs.Arg(0)
	if c.Open.needsTarget() {
		if dir, prof, ok := cfg.resolve(target); ok {
			if !g.Quiet {
				// Said out loud. A tool that silently substituted a path would
				// be one you could not safely paste a command into.
				_, _ = io.WriteString(stderr,
					"graphene: profile "+target+" -> "+dir+"\n")
			}
			target = dir
			applyProfileKeys(fs, prof, g, stderr)
		}
	}

	cx := &Context{
		Ctx:     ctx,
		Globals: g,
		Cmd:     c,
		Now:     time.Now,
		Target:  target,
		Args:    fs.Args(),
		Out:     stdout,
		Err:     stderr,
	}

	if g.CPUProfile != "" {
		stop, err := startProfile(g.CPUProfile)
		if err != nil {
			return fail(stderr, c.Path(), err)
		}
		defer stop()
	}

	if g.MemProfile != "" {
		// Deferred so it runs after the command has produced its result but
		// while the store is still open: a heap profile taken after Close would
		// miss the structures the store was holding, which are the point.
		defer func() {
			if err := writeHeapProfile(g.MemProfile); err != nil {
				fmt.Fprintln(stderr, "memprofile:", err)
			}
		}()
	}

	started := time.Now()
	r, err := execute(cx, c, handler, g)
	elapsed := time.Since(started)

	if err != nil {
		if g.JSON {
			kind, _ := classify(err)
			env := envelopeFor(c.Path(), r, elapsed, exitFor(kind, r.Verdict, true), err, g.Verbose)
			_ = (jsonRenderer{indent: g.Indent}).Render(stdout, env)
			return env.Exit
		}
		// Whatever the handler had already established before it failed is
		// still worth printing. `csr -verify` on a mangled image reports the
		// digest mismatch and *then* fails to parse the file — discarding the
		// first half would leave the operator with a bare error and no account
		// of what was actually checked.
		if len(r.Sections) > 0 || len(r.Findings) > 0 {
			_ = (humanRenderer{verbose: g.Verbose}).Render(stdout, r)
		}
		return fail(stderr, c.Path(), err)
	}

	exit := exitFor(FaultInternal, r.Verdict, false)

	if g.JSON {
		env := envelopeFor(c.Path(), r, elapsed, exit, nil, g.Verbose)
		if rerr := (jsonRenderer{indent: g.Indent}).Render(stdout, env); rerr != nil {
			return fail(stderr, c.Path(), rerr)
		}
		return exit
	}

	// In human mode the notices are asides, and asides belong on stderr so that
	// piping stdout to a file keeps the report and drops the commentary.
	if !g.Quiet {
		for _, n := range r.Notices {
			_, _ = io.WriteString(stderr, "graphene: "+n+"\n")
		}
	}
	if rerr := (humanRenderer{verbose: g.Verbose}).Render(stdout, r); rerr != nil {
		return fail(stderr, c.Path(), rerr)
	}
	return exit
}

// execute applies the gates, opens the store, and calls the handler.
func execute(cx *Context, c *Command, h Handler, g *Globals) (Result, error) {
	var r Result

	// The mutation gate, enforced here so a writing handler cannot forget it.
	if c.Mutates && !g.DryRun && !g.Confirm {
		return r, Refusedf(
			"%s changes bytes already on disk — re-run with -confirm, "+
				"or with -dry-run to see what it would do", c.Path())
	}

	if err := mustHaveTarget(c, cx.Target); err != nil {
		return r, err
	}

	mode := c.Open
	if g.DryRun {
		// A dry run cannot take the exclusive lock and cannot write, because
		// the mode it opens under makes both impossible — not because the
		// handler remembered to check a flag.
		mode = mode.readOnly()
	}

	// The notice, or the dry run's correction of it. A writing command's notice
	// says it takes the exclusive lock, and under -dry-run that is no longer
	// true — the mode was downgraded above. Printing it anyway would have the
	// tool describe a lock it is not holding, which is the sort of small
	// dishonesty that makes an operator stop believing the rest of the output.
	notice := c.Notice
	if g.DryRun && c.Open.writes() {
		notice = "dry run: opening read-only; nothing will be written"
	}
	if notice != "" && !g.Quiet {
		if g.JSON {
			r.Notice("%s", notice)
		} else {
			_, _ = io.WriteString(cx.Err, notice+"\n")
		}
	}

	// Preconditions that must hold before anything is acquired — see
	// Command.Before.
	if c.Before != nil {
		if err := c.Before(cx); err != nil {
			return r, err
		}
	}

	if mode != OpenNone && !c.Creates {
		if err := statAvailable(cx.Target); err != nil {
			return r, err
		}
	}
	closer, err := cx.open(mode)
	if err != nil {
		return r, err
	}
	defer closer()

	stop := cx.watchdog(&r)
	defer stop()

	out, err := h(cx)
	// Carry anything the framework recorded before the handler ran — notices,
	// mostly — into the document the handler returned.
	out.Notices = append(r.Notices, out.Notices...)
	if cx.metrics != nil {
		// After the handler and before the deferred Close, so a command's own
		// work is measured and the close is not. The close would otherwise
		// attribute its final sync to whatever ran last.
		cx.metrics.report(&out)
	}
	if out.Target == "" {
		out.Target = cx.Target
	}
	if g.DryRun {
		out.Notice("dry run: nothing was written")
	}
	return out, err
}

// rootContext builds the context every handler runs under. Ctrl-C reaches the
// same cancellation path a deadline does, so a command that can be interrupted
// is interrupted the same way however the interruption arrives.
func rootContext(g *Globals) (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	if g.Timeout <= 0 {
		return ctx, stop
	}
	tctx, cancel := context.WithTimeout(ctx, g.Timeout)
	return tctx, func() { cancel(); stop() }
}

// fail reports an error in human mode and returns the exit status. It takes no
// Globals because there is nothing in them it would consult: JSON mode never
// reaches here — an error under -json is rendered as an envelope on stdout by
// the caller, so that a script gets the same shape whether the command worked
// or not — and -quiet silences notices, not errors.
func fail(stderr io.Writer, path string, err error) int {
	kind, hint := classify(err)
	prefix := "graphene: "
	if path != "" {
		prefix = "graphene " + path + ": "
	}
	_, _ = io.WriteString(stderr, prefix+err.Error()+"\n")
	if hint != "" {
		_, _ = io.WriteString(stderr, "  "+hint+"\n")
	}
	return exitFor(kind, VerdictNone, true)
}

// writeHeapProfile writes an in-use heap profile.
//
// The collection first is what makes the profile answer "what is still held"
// rather than "what has not been collected yet"; without it the numbers include
// garbage the collector simply has not reached, and two runs of the same command
// disagree by whatever the GC happened to be doing.
func writeHeapProfile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// startProfile begins a CPU profile and returns its stopper.
func startProfile(path string) (func(), error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}, nil
}
