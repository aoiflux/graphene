package main

// Dispatch tests.
//
// The one that matters most is TestLegacySpellings: fifteen verbs existed
// before groups did, and every one of them has to keep resolving to the same
// command forever. A table is the honest way to say that — if someone renames a
// group and quietly breaks `graphene custody`, this fails rather than a user
// discovering it.

import (
	"flag"
	"strings"
	"testing"
)

func init() { initRegistry(registry) }

// TestLegacySpellings pins every pre-registry verb to the command it must
// reach. These entries are a compatibility contract: change one only when
// deliberately breaking a spelling somebody's script depends on.
func TestLegacySpellings(t *testing.T) {
	want := map[string]string{
		"info":          "store info",
		"csr":           "store csr",
		"migrate":       "store migrate",
		"wal":           "wal show",
		"verify":        "debug indexes",
		"custody":       "provenance custody",
		"prove":         "provenance export",
		"verify-proof":  "provenance verify",
		"anchor":        "anchor verify",
		"redactions":    "redaction list",
		"grants":        "grant list",
		"backup":        "backup create",
		"verify-backup": "backup verify",
		"export":        "export graph",
		"import":        "import graph",
	}
	if len(want) != 15 {
		t.Fatalf("the tool had 15 subcommands before groups; this table has %d", len(want))
	}
	for spelling, path := range want {
		t.Run(spelling, func(t *testing.T) {
			res := resolve([]string{spelling, "/some/store"})
			if res.cmd == nil {
				t.Fatalf("graphene %s no longer resolves to anything", spelling)
			}
			if got := res.cmd.Path(); got != path {
				t.Fatalf("graphene %s reaches %q, want %q", spelling, got, path)
			}
			if len(res.args) != 1 || res.args[0] != "/some/store" {
				t.Fatalf("arguments mangled: %q", res.args)
			}
		})
	}
}

// TestLegacyFlagsReachTheCommand checks that the old flat spelling still hands
// its flags through untouched. `graphene wal -limit 5 <dir>` is the shape that
// breaks first if dispatch starts consuming tokens it should not.
func TestLegacyFlagsReachTheCommand(t *testing.T) {
	cases := []struct {
		argv []string
		path string
		args []string
	}{
		{[]string{"wal", "-limit", "5", "/s"}, "wal show", []string{"-limit", "5", "/s"}},
		{[]string{"custody", "-node", "7", "/s"}, "provenance custody", []string{"-node", "7", "/s"}},
		{[]string{"csr", "-verify", "/s"}, "store csr", []string{"-verify", "/s"}},
		{[]string{"anchor", "-publish", "/s"}, "anchor verify", []string{"-publish", "/s"}},
		{[]string{"export", "-format", "csv", "/s"}, "export graph", []string{"-format", "csv", "/s"}},
	}
	for _, c := range cases {
		res := resolve(c.argv)
		if res.cmd == nil || res.cmd.Path() != c.path {
			t.Fatalf("%v resolved to %v, want %s", c.argv, res.cmd, c.path)
		}
		if strings.Join(res.args, " ") != strings.Join(c.args, " ") {
			t.Fatalf("%v handed on %q, want %q", c.argv, res.args, c.args)
		}
	}
}

// TestGroupBeatsAlias pins the precedence rule.
//
// Seven legacy verbs are also group names, so `graphene wal show` is ambiguous
// on its face: the show verb, or the wal command pointed at a directory called
// "show"? The verb wins, and `--` is how you say you meant the directory. This
// has to be a test rather than a comment, because it is the one rule someone
// will otherwise "simplify" later.
func TestGroupBeatsAlias(t *testing.T) {
	if res := resolve([]string{"wal", "show", "/s"}); res.cmd.Path() != "wal show" {
		t.Fatalf("`wal show` reached %s, want the verb", res.cmd.Path())
	}
	res := resolve([]string{"wal", "--", "./show"})
	if res.cmd == nil || res.cmd.Path() != "wal show" {
		t.Fatalf("`wal -- ./show` reached %v", res.cmd)
	}
	if len(res.args) != 1 || res.args[0] != "./show" {
		t.Fatalf("`wal -- ./show` handed on %q, want [./show]", res.args)
	}
}

// TestBareGroupNames: a group that is also a legacy verb keeps its old
// behaviour (ask for a directory); one that is not lists its verbs.
func TestBareGroupNames(t *testing.T) {
	if res := resolve([]string{"wal"}); res.cmd == nil || res.cmd.Path() != "wal show" {
		t.Fatalf("bare `wal` should still reach the legacy command, got %v", res)
	}
	if res := resolve([]string{"provenance"}); res.groupHelp != "provenance" {
		t.Fatalf("bare `provenance` should list the group, got %+v", res)
	}
}

func TestUnknownCommand(t *testing.T) {
	res := resolve([]string{"nonesuch"})
	if res.unknown != "nonesuch" {
		t.Fatalf("want unknown, got %+v", res)
	}
	// A near miss should be suggested; a wild one should not be guessed at.
	if s := suggest("custodyy"); s != "custody" {
		t.Errorf("suggest(custodyy) = %q, want custody", s)
	}
	if s := suggest("zzzzzzzzzz"); s != "" {
		t.Errorf("suggest(zzzzzzzzzz) = %q, want no guess", s)
	}
}

func TestUnknownVerbNamesTheGroup(t *testing.T) {
	res := resolve([]string{"provenance", "frobnicate"})
	if res.unknown != "frobnicate" || res.unknownFor != "provenance" {
		t.Fatalf("want an unknown verb scoped to the group, got %+v", res)
	}
}

// TestRegistryInvariants checks the things a duplicate or a typo would break.
func TestRegistryInvariants(t *testing.T) {
	for _, c := range registry {
		if c.Short == "" {
			t.Errorf("%s has no summary", c.Path())
		}
		if c.Open.needsTarget() && c.Usage == "" {
			t.Errorf("%s needs an operand but documents no usage shape", c.Path())
		}
		if c.newFlags == nil {
			t.Errorf("%s has no handler", c.Path())
		}
	}
	// Every alias must resolve, and resolve back to its own command.
	for alias, c := range byAlias {
		res := resolve([]string{alias})
		if res.cmd == nil && res.groupHelp == "" {
			t.Errorf("alias %q resolves to nothing (owner %s)", alias, c.Path())
		}
	}
}

// TestGlobalsDoNotShadowCommandFlags turns a future collision from a runtime
// panic in somebody's terminal into a red test here.
//
// The flag package panics outright on a redefinition, so a command that adds
// -verbose or -timeout of its own would take the tool down on every invocation
// of that command — including `graphene help`.
func TestGlobalsDoNotShadowCommandFlags(t *testing.T) {
	for _, c := range registry {
		fs := flag.NewFlagSet(c.Path(), flag.ContinueOnError)
		c.newFlags(fs)
		fs.VisitAll(func(f *flag.Flag) {
			if globalNames[f.Name] {
				t.Errorf("%s defines -%s, which is also a global flag — "+
					"the flag package panics on the redefinition", c.Path(), f.Name)
			}
		})
	}
}
