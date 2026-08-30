package main

// Resolving argv onto a command.
//
// The awkward case, and the reason this is its own file: seven of the fifteen
// legacy verbs — wal, anchor, verify, export, import, grants, redactions — are
// also the names of the new groups. `graphene wal /store` must keep working,
// and `graphene wal show /store` must reach the group's verb. Both spellings
// have to resolve, and one of them has to win when they collide.
//
// The rule, stated once so it can be relied on: an exact verb match at position
// one always beats the legacy alias. `graphene wal show` is the show verb, never
// the wal alias applied to a directory named "show". A directory genuinely
// called "show" is reachable as `graphene wal -- ./show`.
//
// Resolution happens entirely on the raw []string, before any FlagSet exists.
// That is what keeps flags-before-operand working exactly as it does today: by
// the time a flag set sees the arguments, the group and verb have been stripped
// and what is left is what the command has always been handed.

import "strings"

// resolution is what the dispatcher decided.
type resolution struct {
	cmd  *Command
	args []string

	// The three ways resolution can end without a command to run.
	helpTopic  []string // `graphene help ...`
	groupHelp  string   // a group named with no verb
	unknown    string   // nothing matched
	unknownFor string   // the group the unknown verb was looked for in
}

// resolve maps argv onto a command and the arguments belonging to it.
//
//	["node","custody","-id","7","/s"] -> node custody, ["-id","7","/s"]
//	["custody","-node","7","/s"]      -> node custody, ["-node","7","/s"]  (alias)
//	["wal","-limit","5","/s"]         -> wal show,     ["-limit","5","/s"] (alias)
//	["wal","show","/s"]               -> wal show,     ["/s"]
//	["wal","--","./show"]             -> wal show,     ["./show"]          (escape)
//	["node"]                          -> groupHelp "node"
//	["node","frobnicate"]             -> unknown, with a suggestion
func resolve(argv []string) resolution {
	if len(argv) == 0 {
		return resolution{groupHelp: ""}
	}
	tok := argv[0]

	switch tok {
	case "help", "-h", "--help":
		return resolution{helpTopic: argv[1:]}
	case "version", "-version", "--version":
		if c, ok := byAlias["version"]; ok {
			return resolution{cmd: c, args: argv[1:]}
		}
	}

	// A group name.
	if _, isGroup := groups[tok]; isGroup {
		alias := byAlias[tok] // nil unless the group name is also a legacy verb

		if len(argv) == 1 {
			// Bare `graphene wal` used to be an error asking for a directory,
			// and still is — it reaches the alias, which asks for one. Bare
			// `graphene node` has no legacy meaning, so it lists the group.
			if alias != nil {
				return resolution{cmd: alias, args: nil}
			}
			return resolution{groupHelp: tok}
		}

		switch next := argv[1]; {
		case next == "-h", next == "--help", next == "help":
			return resolution{groupHelp: tok}

		case next == "--":
			// The escape hatch: everything after this belongs to the legacy
			// command, so a directory named like a verb is still reachable.
			if alias != nil {
				return resolution{cmd: alias, args: argv[2:]}
			}
			return resolution{groupHelp: tok}

		default:
			if c, ok := byPath[tok+" "+next]; ok {
				return resolution{cmd: c, args: argv[2:]}
			}
			// Not a verb of this group. If the group name is also a legacy
			// verb, this is the legacy spelling — a flag or a path, not a verb.
			if alias != nil {
				return resolution{cmd: alias, args: argv[1:]}
			}
			return resolution{unknown: next, unknownFor: tok}
		}
	}

	if c, ok := byAlias[tok]; ok {
		return resolution{cmd: c, args: argv[1:]}
	}
	return resolution{unknown: tok}
}

// didYouMean renders the suggestion line, or "".
func didYouMean(typed string) string {
	s := suggest(typed)
	if s == "" {
		return ""
	}
	return "\ndid you mean `graphene " + s + "`?"
}

// verbList renders a group's verbs for an error message.
func verbList(group string) string {
	cs := visible(group)
	names := make([]string, len(cs))
	for i, c := range cs {
		names[i] = c.Name
	}
	return strings.Join(names, ", ")
}
