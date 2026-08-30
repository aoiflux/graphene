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
	"io"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"
)

// run executes one invocation and returns the exit status.
func run(argv []string, stdout, stderr io.Writer) int {
	g := &Globals{}
	applyEnv(g)

	rest, err := splitGlobals(argv, g)
	if err != nil {
		return fail(stderr, g, "", err)
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
			return fail(stderr, g, "", Usagef(
				"%s is not a %s command — try one of: %s",
				res.unknown, res.unknownFor, verbList(res.unknownFor)))
		}
		return fail(stderr, g, "", Usagef("unknown subcommand %q%s", res.unknown, didYouMean(res.unknown)))
	case res.cmd == nil:
		return writeHelp(stdout, nil)
	}

	return invoke(res.cmd, res.args, g, stdout, stderr)
}

// invoke parses the command's flags, opens what it declared, runs it and
// renders the result.
func invoke(c *Command, args []string, g *Globals, stdout, stderr io.Writer) int {
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

	cx := &Context{
		Ctx:     ctx,
		Globals: g,
		Cmd:     c,
		Now:     time.Now,
		Target:  fs.Arg(0),
		Args:    fs.Args(),
		Out:     stdout,
		Err:     stderr,
	}

	if g.Profile != "" {
		stop, err := startProfile(g.Profile)
		if err != nil {
			return fail(stderr, g, c.Path(), err)
		}
		defer stop()
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
		return fail(stderr, g, c.Path(), err)
	}

	exit := exitFor(FaultInternal, r.Verdict, false)

	if g.JSON {
		env := envelopeFor(c.Path(), r, elapsed, exit, nil, g.Verbose)
		if rerr := (jsonRenderer{indent: g.Indent}).Render(stdout, env); rerr != nil {
			return fail(stderr, g, c.Path(), rerr)
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
		return fail(stderr, g, c.Path(), rerr)
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

	if c.Notice != "" && !g.Quiet {
		if g.JSON {
			r.Notice("%s", c.Notice)
		} else {
			_, _ = io.WriteString(cx.Err, c.Notice+"\n")
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

// fail reports an error in human mode and returns the exit status.
func fail(stderr io.Writer, g *Globals, path string, err error) int {
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
