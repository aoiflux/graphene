package main

// End-to-end tests: run() in, exit status and output out.
//
// No subprocess and no captured file descriptors — main() is three lines and
// everything else is reachable from here. Before this the package had no tests
// at all, and CI proved only that it compiled.

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// fixture builds a small store and returns its directory.
func fixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	nodes := []*store.Node{
		{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
		{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("add nodes: %v", err)
	}
	if _, err := g.AddEdges([]*store.Edge{
		{Src: ids[0], Dst: ids[1], Labels: []store.EdgeType{store.EdgeTypeContains}},
	}); err != nil {
		t.Fatalf("add edges: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// exec runs one invocation and returns stdout, stderr and the exit status.
func exec(t *testing.T, argv ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(argv, &out, &errb)
	return out.String(), errb.String(), code
}

// TestReadCommandsAgainstAFixture is the smoke test: every read-only command
// runs against a real store, exits zero, and prints something.
func TestReadCommandsAgainstAFixture(t *testing.T) {
	dir := fixture(t)
	for _, argv := range [][]string{
		{"info", dir},
		{"store", "info", dir},
		{"csr", dir},
		{"store", "csr", "-verify", dir},
		{"wal", dir},
		{"wal", "show", dir},
		{"verify", dir},
		{"debug", "indexes", dir},
		{"redactions", dir},
		{"grants", dir},
	} {
		t.Run(strings.Join(argv[:len(argv)-1], "_"), func(t *testing.T) {
			out, errb, code := exec(t, argv...)
			if code != 0 {
				t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, out, errb)
			}
			if strings.TrimSpace(out) == "" {
				t.Fatalf("printed nothing to stdout\nstderr:\n%s", errb)
			}
		})
	}
}

// TestGroupAndLegacyAgree is the machine-checkable form of the compatibility
// promise: the two spellings are one command, so they must produce one output.
func TestGroupAndLegacyAgree(t *testing.T) {
	dir := fixture(t)
	pairs := [][2][]string{
		{{"info", dir}, {"store", "info", dir}},
		{{"csr", dir}, {"store", "csr", dir}},
		{{"wal", dir}, {"wal", "show", dir}},
		{{"verify", dir}, {"debug", "indexes", dir}},
		{{"redactions", dir}, {"redaction", "list", dir}},
		{{"grants", dir}, {"grant", "list", dir}},
	}
	for _, p := range pairs {
		oldOut, _, oldCode := exec(t, p[0]...)
		newOut, _, newCode := exec(t, p[1]...)
		if oldOut != newOut || oldCode != newCode {
			t.Errorf("%v and %v disagree\n--- %v (exit %d) ---\n%s\n--- %v (exit %d) ---\n%s",
				p[0], p[1], p[0], oldCode, oldOut, p[1], newCode, newOut)
		}
	}
}

// TestUsageErrorsExitTwo pins the exit-status policy's middle case. A command
// line that was wrong is 2; a store that is broken is 1; findings are 0.
func TestUsageErrorsExitTwo(t *testing.T) {
	for _, argv := range [][]string{
		{"nonesuch"},
		{"provenance", "frobnicate"},
		{"info"}, // no directory
	} {
		if _, _, code := exec(t, argv...); code != 2 {
			t.Errorf("%v exited %d, want 2", argv, code)
		}
	}
}

// TestHelpGoesToStdout is a regression test for a small but real defect: the
// old usage() wrote to stderr, so `graphene help | less` showed nothing and
// `graphene help > cheatsheet.txt` wrote an empty file.
func TestHelpGoesToStdout(t *testing.T) {
	out, errb, code := exec(t, "help")
	if code != 0 {
		t.Fatalf("help exited %d", code)
	}
	if !strings.Contains(out, "graphene") || len(out) < 200 {
		t.Fatalf("help did not reach stdout:\n%s", out)
	}
	if strings.TrimSpace(errb) != "" {
		t.Errorf("help wrote to stderr as well:\n%s", errb)
	}
}

// TestHelpListsEveryVisibleCommand: help is generated from the registry, so a
// command cannot exist without appearing. This is what stops the drift that
// left the package doc describing eight of fifteen commands.
func TestHelpListsEveryVisibleCommand(t *testing.T) {
	out, _, _ := exec(t, "help")
	for _, c := range registry {
		if c.Hidden {
			continue
		}
		if !strings.Contains(out, c.Path()) {
			t.Errorf("%s is in the registry but not in help", c.Path())
		}
	}
}

// TestVersionJSON checks the envelope and that the tool reports its true
// dependency count, which is none.
func TestVersionJSON(t *testing.T) {
	out, _, code := exec(t, "-json", "version")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, out)
	}
	var env struct {
		Schema  string `json:"schema"`
		Command string `json:"command"`
		Status  string `json:"status"`
		Version struct {
			Dependencies []string `json:"dependencies"`
			CSRFormat    int      `json:"csr_format"`
		} `json:"version"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if env.Schema != SchemaVersion {
		t.Errorf("schema %q, want %q", env.Schema, SchemaVersion)
	}
	if env.Command != "version" {
		t.Errorf("command %q", env.Command)
	}
	if len(env.Version.Dependencies) != 0 {
		t.Errorf("this module has no external dependencies; the stamp claims %v",
			env.Version.Dependencies)
	}
	if env.Version.CSRFormat == 0 {
		t.Error("csr_format missing — an operator asking whether this binary can " +
			"migrate a store is asking about this number")
	}
}

// TestJSONStdoutIsOnlyJSON: the notices that go to stderr in human mode must
// move into the document, or `graphene ... -json | jq` breaks on the first
// command that opens a store.
func TestJSONStdoutIsOnlyJSON(t *testing.T) {
	out, _, _ := exec(t, "-json", "version")
	if err := json.Unmarshal([]byte(out), &map[string]any{}); err != nil {
		t.Fatalf("stdout was not a single JSON document: %v\n%s", err, out)
	}
}

// TestMissingStoreIsNotFound checks a missing directory produces a clear
// message rather than whatever the store layer says about it.
func TestMissingStoreIsNotFound(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	_, errb, code := exec(t, "verify", missing)
	if code == 0 {
		t.Fatal("a missing store should not exit zero")
	}
	if !strings.Contains(errb, "nope") {
		t.Errorf("the message does not name what was missing:\n%s", errb)
	}
}

// TestDryRunLeavesTheStoreAlone hashes the directory before and after.
//
// Asserting on the exit code alone would pass even if the command wrote — a
// test that cannot fail for the reason it exists is worse than no test.
func TestDryRunLeavesTheStoreAlone(t *testing.T) {
	dir := fixture(t)
	before := snapshotDir(t, dir)
	if _, _, code := exec(t, "-dry-run", "migrate", "-check", dir); code != 0 {
		t.Fatalf("dry-run migrate exited %d", code)
	}
	if after := snapshotDir(t, dir); after != before {
		t.Errorf("a dry run changed the store:\nbefore %s\nafter  %s", before, after)
	}
}

// snapshotDir summarises a directory's contents: name, size and modification
// time of every file, so any write shows up.
func snapshotDir(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	err := filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		b.WriteString(rel)
		b.WriteString(":")
		b.WriteString(fi.ModTime().UTC().Format("20060102150405.000000000"))
		b.WriteString(":")
		b.WriteString(string(rune(fi.Size())))
		b.WriteString("\n")
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return b.String()
}
