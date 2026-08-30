package main

// Help, generated from the registry.
//
// Two changes worth naming. First, this is derived rather than written, so the
// drift that had already set in — the usage text, the package doc and
// docs/FORENSICS.md each claiming a different number of subcommands — cannot
// happen again. Second, it goes to stdout. The old usage() wrote to stderr,
// which meant `graphene help | less` showed an empty screen and `graphene help
// > cheatsheet.txt` wrote an empty file. Help that was asked for is output;
// help that follows a mistake is a diagnostic, and only the second belongs on
// stderr.

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// writeHelp prints the root help, or drills into a topic.
func writeHelp(w io.Writer, topic []string) int {
	switch len(topic) {
	case 0:
		writeRootHelp(w)
		return 0
	case 1:
		if _, ok := groups[topic[0]]; ok {
			writeGroupHelp(w, topic[0])
			return 0
		}
		if c, ok := byAlias[topic[0]]; ok {
			writeCommandHelp(w, c, nil)
			return 0
		}
	default:
		if c, ok := byPath[topic[0]+" "+topic[1]]; ok {
			writeCommandHelp(w, c, nil)
			return 0
		}
	}
	fmt.Fprintf(w, "graphene: no help for %q%s\n", strings.Join(topic, " "), didYouMean(topic[0]))
	return 2
}

func writeRootHelp(w io.Writer) {
	fmt.Fprint(w, `graphene — inspection of a Graphene store

Usage:
  graphene [global flags] <group> <verb> [flags] <dir>
  graphene help <group> [<verb>]

`)
	for _, g := range visibleGroups() {
		cs := visible(g)
		if len(cs) == 0 {
			continue
		}
		fmt.Fprintf(w, "%s — %s\n", g, groupBlurb(g))
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, c := range cs {
			fmt.Fprintf(tw, "  %s %s\t%s\n", g, c.Name, c.Short)
		}
		tw.Flush()
		fmt.Fprintln(w)
	}

	// Bare top-level commands: version, and anything that never belonged to a
	// group.
	var bare []*Command
	for _, c := range byPath {
		if c.Group == "" && !c.Hidden {
			bare = append(bare, c)
		}
	}
	sortCommands(bare)
	if len(bare) > 0 {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, c := range bare {
			fmt.Fprintf(tw, "  %s\t%s\n", c.Name, c.Short)
		}
		tw.Flush()
		fmt.Fprintln(w)
	}

	fmt.Fprint(w, `Global flags:
  -json            emit a JSON document instead of a report
  -indent          indent the JSON (implies -json)
  -verbose         more detail in the report
  -quiet           suppress the asides on stderr
  -no-color        plain output even on a terminal
  -log-level L     error | warn | info | debug
  -metrics         report what the engine did: commits, syncs, queries, replay
  -dry-run         report what would change, and change nothing
  -timeout D       give up starting new work after this
  -cpuprofile P    write a pprof CPU profile to this path

A <dir> operand may be a profile name instead of a path — see `+"`graphene help profile`"+`.
A path that exists always wins, so a profile can never shadow a real directory.

Every subcommand that existed before groups still works by its old name.
`+"`graphene custody <dir>`"+` and `+"`graphene provenance custody <dir>`"+` are the same
command; the short spellings are kept for compatibility and hidden from this
list.

Exit status is non-zero only when something is actually broken. An incomplete
account — a store that was never signed, audited, or set to retain history — is
reported as findings and exits zero.
`)
}

// writeGroupHelp lists one group's verbs.
func writeGroupHelp(w io.Writer, group string) {
	cs := visible(group)
	fmt.Fprintf(w, "graphene %s — %s\n\nUsage:\n  graphene %s <verb> [flags] <dir>\n\n",
		group, groupBlurb(group), group)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, c := range cs {
		fmt.Fprintf(tw, "  %s\t%s\t%s\n", c.Name, c.Short, accessNote(c))
	}
	tw.Flush()
	fmt.Fprintf(w, "\ngraphene help %s <verb> for detail.\n", group)
}

// writeCommandHelp prints one command's full help. fs may be nil, in which case
// the flags are re-derived so `graphene help node custody` shows them too.
func writeCommandHelp(w io.Writer, c *Command, fs *flag.FlagSet) {
	fmt.Fprintf(w, "graphene %s %s\n\n  %s\n", c.Path(), c.Usage, c.Short)
	if c.Long != "" {
		fmt.Fprintf(w, "\n%s\n", strings.TrimRight(c.Long, "\n"))
	}

	fmt.Fprintf(w, "\nAccess: %s", accessNote(c))
	if c.Mutates {
		fmt.Fprint(w, "; requires -confirm, honours -dry-run")
	}
	fmt.Fprintln(w)

	if c.Tier == CtxAdvisory {
		// Saying this plainly is the whole reason the tier exists. A -timeout
		// somebody believes will interrupt a compaction, and does not, is worse
		// than no -timeout at all.
		fmt.Fprintln(w, "-timeout is advisory here: this command makes one library call that\n"+
			"  cannot be interrupted, so the deadline reports rather than cancels.")
	}

	if fs == nil {
		fs = flag.NewFlagSet(c.Path(), flag.ContinueOnError)
		c.newFlags(fs)
	}
	var buf strings.Builder
	fs.SetOutput(&buf)
	fs.PrintDefaults()
	if buf.Len() > 0 {
		fmt.Fprintf(w, "\nFlags:\n%s", buf.String())
	}

	if len(c.Aliases) > 0 {
		fmt.Fprintf(w, "\nAlso spelled: graphene %s\n", strings.Join(c.Aliases, ", graphene "))
	}
}

// accessNote is the human sentence for a command's OpenMode.
func accessNote(c *Command) string {
	switch c.Open {
	case OpenNone:
		return "touches no store"
	case OpenFiles:
		return "reads the files directly (safe while a writer holds the store)"
	case OpenDiskRO, OpenGraphRO:
		return "opens read-only (shared lock; refused while a writer holds it)"
	case OpenDiskRW, OpenGraphRW:
		return "opens exclusively"
	}
	return ""
}

// groupBlurb is the one-line description of a group.
func groupBlurb(group string) string {
	if b, ok := groupBlurbs[group]; ok {
		return b
	}
	return group + " commands"
}

// groupBlurbs is the one line each group is introduced with. A group without
// an entry falls back to "<name> commands", which is a placeholder and reads
// like one — a registry test would be the place to make that a build failure if
// the fallback ever starts appearing in shipped help.
var groupBlurbs = map[string]string{
	"store":       "the image, the log, and what they say about each other",
	"node":        "entities: read them, account for them, prove them",
	"edge":        "relationships",
	"traverse":    "walks over the graph's shape: neighbourhoods, paths, patterns",
	"query":       "adjacency-driven queries around nodes you name",
	"wal":         "the write-ahead log and its segments",
	"anchor":      "checkpoints and the external roots they are published to",
	"keys":        "the signing keys, and the rotations between them",
	"assertion":   "the audit chain, and the attestation over the image",
	"grant":       "role grants and the capabilities they imply — recorded, never enforced",
	"redaction":   "attributed removal: who removed what, when, and why",
	"provenance":  "where an entity came from, and who has vouched for it",
	"backup":      "consistent copies, and restoring from them",
	"export":      "getting a graph, a region of one, or a proof bundle out",
	"import":      "building a store from a dump",
	"debug":       "verification, one narrow check at a time or all of them at once",
	"maintenance": "the two operations that are safe, and the two that are refused",
	"config":      "this tool's own configuration file",
	"profile":     "names for store directories, and the keys that go with them",
}

func sortCommands(cs []*Command) {
	for i := 1; i < len(cs); i++ {
		for j := i; j > 0 && cs[j].Name < cs[j-1].Name; j-- {
			cs[j], cs[j-1] = cs[j-1], cs[j]
		}
	}
}
