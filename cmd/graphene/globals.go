package main

// Process-wide flags, and the paging flags that deliberately are not.
//
// The `-limit` question is the interesting one. The brief asked for a global
// `--limit`, and `wal -limit N` has existed for as long as the tool has, with a
// specific meaning: stop after N records, 0 means all, default 50. Registering
// a global -limit on wal's flag set makes the flag package panic outright
// ("flag redefined: limit"). Exempting wal from the global is worse than the
// panic — the same flag would then mean two different things depending on which
// command you typed, and the one place it differed would be the command people
// use it on most.
//
// So -limit and -offset are not global. They are a shared binder with a
// per-command default, which gives the consistency the global was wanted for
// and keeps `wal -limit 0` meaning exactly what it always meant.

import (
	"flag"
	"os"
	"strings"
	"time"
)

// Globals are the options that apply to every command.
//
// Audited against every flag the fifteen existing verbs register — verify,
// node, anchor, limit, commits, pubkey, actor, edge, kind, out, root, to, from,
// format, batch, compact, no-properties, check, publish, insecure-local-file —
// and none of these collides. A test walks the registry and re-checks that, so
// a command that adds -verbose in future fails the build rather than panicking
// in a user's terminal.
type Globals struct {
	JSON     bool
	Indent   bool
	Verbose  bool
	Quiet    bool
	NoColor  bool
	LogLevel string

	// CPUProfile is a pprof output path. Named -cpuprofile rather than
	// -profile because `profile` is the group that names store directories,
	// and one word meaning two unrelated things on the same command line is
	// how somebody ends up writing a pprof file into a directory they meant to
	// inspect.
	CPUProfile string
	DryRun     bool
	Confirm    bool
	Timeout    time.Duration

	// Metrics attaches a store.Metrics sink at open time and appends a summary
	// of what the engine did. See metrics.go for why this is a global flag
	// rather than the `debug profile` verb the brief asked for.
	Metrics bool
}

// bindGlobals registers the process-wide flags on a command's flag set.
//
// The default handed to each Var is the value splitGlobals has already written.
// Both phases write the same address, and the second phase's default is the
// first phase's result — which is why `graphene -json node get -verbose <dir>`
// needs no precedence table and no merge step. A later occurrence overrides an
// earlier one; an earlier one that is not repeated persists.
func bindGlobals(fs *flag.FlagSet, g *Globals) {
	fs.BoolVar(&g.JSON, "json", g.JSON, "emit a JSON document instead of a report")
	fs.BoolVar(&g.Indent, "indent", g.Indent, "indent the JSON (implies -json)")
	fs.BoolVar(&g.Verbose, "verbose", g.Verbose, "more detail in the report")
	fs.BoolVar(&g.Quiet, "quiet", g.Quiet, "suppress the asides on stderr")
	fs.BoolVar(&g.NoColor, "no-color", g.NoColor, "plain output even on a terminal")
	fs.StringVar(&g.LogLevel, "log-level", g.LogLevel, "error | warn | info | debug")
	fs.StringVar(&g.CPUProfile, "cpuprofile", g.CPUProfile, "write a pprof CPU profile to this path")
	fs.BoolVar(&g.DryRun, "dry-run", g.DryRun, "report what would change, and change nothing")
	fs.DurationVar(&g.Timeout, "timeout", g.Timeout, "give up starting new work after this")
	fs.BoolVar(&g.Metrics, "metrics", g.Metrics,
		"report what the engine did: commits, syncs, queries, replay")
}

// globalNames is the set bindGlobals registers. The dispatcher uses it to know
// which leading tokens it may consume, and a test uses it to prove no command
// flag shadows one.
var globalNames = map[string]bool{
	"json": true, "indent": true, "verbose": true, "quiet": true,
	"no-color": true, "log-level": true, "cpuprofile": true,
	"dry-run": true, "timeout": true, "metrics": true,
}

// globalTakesValue distinguishes `-timeout 30s` from `-json`.
var globalTakesValue = map[string]bool{
	"log-level": true, "cpuprofile": true, "timeout": true,
}

// applyEnv seeds the globals from the environment, before any flag is parsed.
// NO_COLOR is honoured because it is a convention this tool does not get to
// opt out of.
func applyEnv(g *Globals) {
	if os.Getenv("NO_COLOR") != "" {
		g.NoColor = true
	}
	if v := os.Getenv("GRAPHENE_JSON"); v == "1" || v == "true" {
		g.JSON = true
	}
	if v := os.Getenv("GRAPHENE_LOG_LEVEL"); v != "" {
		g.LogLevel = v
	}
}

// splitGlobals consumes leading global flags so that `graphene -json info <dir>`
// works, and stops at the first token that is not one so that the command's own
// flags reach the command. It also stops at `--`, as the flag package does.
func splitGlobals(argv []string, g *Globals) ([]string, error) {
	fs := flag.NewFlagSet("graphene", flag.ContinueOnError)
	fs.SetOutput(devNull{})
	bindGlobals(fs, g)

	i := 0
	for i < len(argv) {
		tok := argv[i]
		if tok == "--" || !strings.HasPrefix(tok, "-") || tok == "-" {
			break
		}
		name := strings.TrimLeft(tok, "-")
		value := ""
		hasValue := false
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			name, value, hasValue = name[:eq], name[eq+1:], true
		}
		if !globalNames[name] {
			// Not ours. It belongs to the command, which has not been
			// identified yet — so stop and let dispatch find it.
			break
		}
		switch {
		case hasValue:
			if err := fs.Set(name, value); err != nil {
				return nil, err
			}
			i++
		case globalTakesValue[name]:
			if i+1 >= len(argv) {
				return nil, Usagef("flag needs an argument: -%s", name)
			}
			if err := fs.Set(name, argv[i+1]); err != nil {
				return nil, err
			}
			i += 2
		default:
			if err := fs.Set(name, "true"); err != nil {
				return nil, err
			}
			i++
		}
	}
	if g.Indent {
		g.JSON = true
	}
	return argv[i:], nil
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

// Paging is the shared shape of every listing command's -limit and -offset.
//
// Bound per command rather than globally, with the default the command wants:
// see the note at the top of this file for why a global -limit cannot work
// here.
type Paging struct {
	Limit  int
	Offset int
}

// Bind registers the paging flags with a command-specific default and help
// string, since `wal -limit` counts records and `node list -limit` counts rows.
func (p *Paging) Bind(fs *flag.FlagSet, defLimit int, limitHelp string) {
	fs.IntVar(&p.Limit, "limit", defLimit, limitHelp)
	fs.IntVar(&p.Offset, "offset", 0, "skip this many rows first")
}

// Window applies the offset and limit to a count, returning the half-open range
// to render and whether anything was cut off. A limit of 0 means all, which is
// the convention wal established.
func (p Paging) Window(total int) (lo, hi int, truncated bool) {
	lo = p.Offset
	if lo > total {
		lo = total
	}
	hi = total
	if p.Limit > 0 && lo+p.Limit < total {
		hi = lo + p.Limit
	}
	return lo, hi, hi < total || lo > 0
}
