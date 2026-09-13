package main

// The command registry — the only list of commands in the tree.
//
// There used to be three: the dispatch switch, the usage text, and the package
// doc comment. They had already drifted apart. The package doc listed eight of
// fifteen, docs/FORENSICS.md listed nine, and nobody had noticed because
// nothing connected them. Dispatch, help, the group listings and the generated
// documentation all read this slice now, so the lists cannot disagree again —
// there is only one.

import (
	"flag"
	"sort"

	"github.com/aoiflux/graphene/disk"
)

// Handler runs a command and returns the document it produced.
type Handler func(*Context) (Result, error)

// Command is one entry in the registry.
type Command struct {
	Group   string   // "" for a bare top-level command; "node", "wal", ...
	Name    string   // the verb within the group
	Aliases []string // legacy flat spellings, hidden from help but never removed
	Usage   string   // operand shape: "<dir>", "<dir|file>", "<file>"
	Short   string   // one line, lowercase, no trailing period
	Long    string   // paragraphs for `graphene help <group> <verb>`

	Open   OpenMode
	Notice string // printed to stderr before opening; says which lock is taken
	Tier   CtxTier

	// Mutates marks a command that changes bytes already on disk. The framework
	// binds -confirm, refuses without it, and downgrades the open mode under
	// -dry-run — so a handler cannot forget any of that, because it never sees
	// it.
	Mutates bool

	// Streams marks a command whose stdout is a payload rather than a report:
	// export, prove without -out. These may write to Context.Out directly.
	Streams bool

	Hidden bool

	// Creates marks a command whose target legitimately does not exist yet, so
	// the framework's "is this store there?" check is skipped for it.
	Creates bool

	// Before runs after the flags are parsed and before anything is opened.
	//
	// It exists for one real case: `import graph` must refuse a destination
	// that is not empty, and graphene.Open *creates* the directory — so by the
	// time a framework-opened handler could look, the check would already be
	// meaningless. A precondition that has to be tested before the resource is
	// acquired cannot live in the handler.
	Before func(*Context) error

	// newFlags registers this command's flags on fs and returns the handler
	// closed over them, plus the tuner if the options type has one. Written by
	// cmd(), never by hand.
	newFlags func(*flag.FlagSet) (Handler, openTune)
}

// openTune adjusts the Options the framework opens the store with. Nil unless
// the command's options type implements openTuner.
type openTune func(*disk.Options)

// openTuner is implemented by an options struct one of whose flags changes how
// the store has to be opened, rather than what the handler does once it is open.
//
// It exists because Before cannot serve: the framework opens before the handler
// runs, so a flag that decides an Option has to be read in between, and Before
// is a func(*Context) with no access to the flags. `store migrate -to 8` is the
// case — which version the next compaction writes is Options.IndexMode's
// decision, made at open, so by the time a handler could act on -to the store
// is already open under the wrong one.
//
// Deliberately narrow. A tuner may set fields on the Options and nothing else:
// it cannot refuse the open (that is Before), cannot change the open mode (that
// is Command.Open and -dry-run), and runs after -metrics and the ledger
// detection, so it cannot quietly undo either.
type openTuner interface{ tuneOpen(*disk.Options) }

// Path is how the command is spelled: "node custody", or "info".
func (c *Command) Path() string {
	if c.Group == "" {
		return c.Name
	}
	return c.Group + " " + c.Name
}

// cmd binds a typed options struct to a handler.
//
// The registry is a slice of one type, but each command wants its own flags
// with their own types. The two closures below share one *O by capture, so the
// handler reads its options as a struct and no code anywhere performs a type
// assertion or reaches for reflection.
func cmd[O any](
	c Command,
	bind func(*flag.FlagSet, *O),
	run func(*Context, *O) (Result, error),
) *Command {
	c.newFlags = func(fs *flag.FlagSet) (Handler, openTune) {
		var o O
		if bind != nil {
			bind(fs, &o)
		}
		var tune openTune
		// The assertion is on the pointer, so a tuner is declared on *O and
		// reads the same flags the handler does — one struct, filled by the
		// flag set, seen by both.
		if t, ok := any(&o).(openTuner); ok {
			tune = t.tuneOpen
		}
		return func(cx *Context) (Result, error) { return run(cx, &o) }, tune
	}
	return &c
}

// noOpts is the options type for a command that takes no flags of its own.
type noOpts struct{}

// plain is cmd() for a command with no flags.
func plain(c Command, run func(*Context) (Result, error)) *Command {
	return cmd(c, nil, func(cx *Context, _ *noOpts) (Result, error) { return run(cx) })
}

// The lookup tables, built once from registry by initRegistry.
var (
	byPath  = map[string]*Command{}
	byAlias = map[string]*Command{}
	groups  = map[string][]*Command{}
	// groupOrder keeps help output stable without sorting the registry itself,
	// so a group can be placed where it reads best rather than alphabetically.
	groupOrder []string
)

// initRegistry indexes the registry and checks its invariants.
//
// It panics on a duplicate. A registry with two commands claiming one name is a
// programming error that must not reach a build: the second would silently
// shadow the first, and the first would be a command that exists, appears in
// help, and cannot be run.
func initRegistry(cmds []*Command) {
	byPath = map[string]*Command{}
	byAlias = map[string]*Command{}
	groups = map[string][]*Command{}
	groupOrder = nil

	seenGroup := map[string]bool{}
	for _, c := range cmds {
		p := c.Path()
		if _, dup := byPath[p]; dup {
			panic("graphene: duplicate command " + p)
		}
		byPath[p] = c

		for _, a := range c.Aliases {
			if prev, dup := byAlias[a]; dup {
				panic("graphene: alias " + a + " claimed by both " + prev.Path() + " and " + p)
			}
			byAlias[a] = c
		}
		if c.Group == "" {
			if prev, dup := byAlias[c.Name]; dup && prev != c {
				panic("graphene: " + c.Name + " is both a command and an alias for " + prev.Path())
			}
			byAlias[c.Name] = c
			continue
		}
		if !seenGroup[c.Group] {
			seenGroup[c.Group] = true
			groupOrder = append(groupOrder, c.Group)
		}
		groups[c.Group] = append(groups[c.Group], c)
	}
}

// visibleGroups returns the groups worth printing in help, in registry order.
func visibleGroups() []string { return groupOrder }

// visible returns a group's commands that are not hidden.
func visible(group string) []*Command {
	out := make([]*Command, 0, len(groups[group]))
	for _, c := range groups[group] {
		if !c.Hidden {
			out = append(out, c)
		}
	}
	return out
}

// allNames lists every spelling that resolves, for the did-you-mean suggestion.
func allNames() []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for p := range byPath {
		add(p)
	}
	for a := range byAlias {
		add(a)
	}
	for g := range groups {
		add(g)
	}
	sort.Strings(out)
	return out
}

// suggest offers the closest known spelling to what was typed, or "".
//
// Deliberately conservative: a wrong suggestion sends someone down a path that
// does not exist, which is worse than no suggestion at all. Only an edit
// distance of one or two, and only for a token long enough for that to mean
// something.
func suggest(typed string) string {
	if len(typed) < 3 {
		return ""
	}
	best, bestD := "", 3
	for _, n := range allNames() {
		if d := editDistance(typed, n); d < bestD {
			best, bestD = n, d
		}
	}
	return best
}

// editDistance is Levenshtein, two rows.
func editDistance(a, b string) int {
	if a == b {
		return 0
	}
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min3(cur[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

func min3(a, b, c int) int {
	if b < a {
		a = b
	}
	if c < a {
		a = c
	}
	return a
}

// mustHaveTarget is the shared operand check.
func mustHaveTarget(c *Command, target string) error {
	if target != "" || !c.Open.needsTarget() {
		return nil
	}
	shape := c.Usage
	if shape == "" {
		shape = "<dir>"
	}
	return Usagef("need %s — usage: graphene %s %s", shape, c.Path(), shape)
}
