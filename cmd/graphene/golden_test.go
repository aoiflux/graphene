package main

// Golden output for the commands whose exact rendering people read.
//
// The other tests here assert on facts: that a field is present, that an exit
// code is right, that the legacy spelling and the group spelling agree. None of
// them notices a column that lost its alignment, a heading that changed word, a
// note that quietly stopped being printed, or a finding whose wording drifted
// away from the documentation that quotes it. Those are exactly the changes that
// break somebody's `grep`, and exactly the changes that are invisible in review.
//
// So this file keeps the rendering itself under version control. A diff here is
// not a failure: it is the change being shown to whoever is making it. Accept it
// with
//
//	go test ./cmd/graphene/ -run TestGoldenOutput -update
//
// and the diff lands in the commit, where a reviewer sees what the output used
// to look like next to what it looks like now.
//
// The corpus is deliberately read-only. A golden file for a command that writes
// would either need a store rebuilt to a byte-identical state (it is not) or
// would be asserting on something the mutation tests already own.
//
// # Why the fixture is at a relative path
//
// The store operand is printed back in most reports, and in JSON it is a field.
// A t.TempDir() path is absolute, machine-specific, and — because Go names it
// with a random number of unpredictable length — a different width on every run,
// which would move every column in any table that showed it. The fixture is
// therefore built at "fx" inside a working directory this test owns, so the
// operand is three characters on every machine.

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

var update = flag.Bool("update", false, "rewrite the golden files from the current output")

// timestamp matches the RFC 3339 stamps the reports print — `last compacted`,
// checkpoint times, audit entry times. They record when the fixture was built,
// which is a different second on every run.
var timestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)

// goldenCases is the corpus. Names become file names, so they are the stable
// identifier: renaming one orphans its golden file, which `-update` will not
// clean up, and that is intentional — an orphan is visible in `git status`.
var goldenCases = []struct {
	name string
	argv []string
}{
	// The legacy spellings first. These are the surface that must not move:
	// every one of them worked before the CLI was reorganised, and each has a
	// group spelling that must render identically.
	{"legacy_info", []string{"info", "fx"}},
	{"legacy_csr", []string{"csr", "fx"}},
	{"legacy_wal", []string{"wal", "fx"}},
	{"legacy_verify", []string{"verify", "fx"}},
	{"legacy_custody", []string{"custody", "-node", "1", "fx"}},
	{"legacy_redactions", []string{"redactions", "fx"}},
	{"legacy_grants", []string{"grants", "fx"}},

	// store
	{"store_info", []string{"store", "info", "fx"}},
	{"store_csr_verify", []string{"store", "csr", "-verify", "fx"}},
	{"store_stats", []string{"store", "stats", "fx"}},
	{"store_snapshot", []string{"store", "snapshot", "fx"}},
	{"store_health", []string{"store", "health", "fx"}},

	// wal
	{"wal_show", []string{"wal", "show", "fx"}},
	{"wal_segments", []string{"wal", "segments", "fx"}},
	{"wal_verify", []string{"wal", "verify", "fx"}},

	// node and edge
	{"node_count", []string{"node", "count", "fx"}},
	{"node_get", []string{"node", "get", "-id", "1,3", "fx"}},
	{"node_list_type", []string{"node", "list", "-type", "MicroArtefact", "fx"}},
	{"node_list_prop", []string{"node", "list", "-prop", "sha256=aa11", "fx"}},
	{"node_degree", []string{"node", "degree", "-id", "1", "fx"}},
	{"node_neighbours", []string{"node", "neighbours", "-id", "1", "fx"}},
	{"node_explain", []string{"node", "explain", "-type", "EvidenceFile", "fx"}},
	{"edge_count", []string{"edge", "count", "fx"}},
	{"edge_get", []string{"edge", "get", "-id", "1", "fx"}},
	{"edge_list", []string{"edge", "list", "-type", "Contains", "fx"}},
	{"edge_of", []string{"edge", "of", "-node", "1", "fx"}},
	{"edge_provenance", []string{"edge", "provenance", "-id", "1", "fx"}},

	// traversal
	{"traverse_bfs", []string{"traverse", "bfs", "-from", "1", "-depth", "3", "fx"}},
	{"traverse_dfs", []string{"traverse", "dfs", "-from", "1", "-depth", "3", "fx"}},
	{"traverse_path", []string{"traverse", "path", "-from", "1", "-to", "5", "fx"}},
	{"traverse_cycle", []string{"traverse", "cycle", "-from", "1", "fx"}},
	{"traverse_subgraph", []string{"traverse", "subgraph", "-id", "1,3,4", "fx"}},
	{"query_relations", []string{"query", "relations", "-anchor", "1", "fx"}},
	{"provenance_chain", []string{"provenance", "chain", "-node", "4", "fx"}},

	// governance and verification
	{"anchor_list", []string{"anchor", "list", "fx"}},
	// `anchor show` is deliberately absent. A checkpoint digest commits to the
	// capture time, so it is a different value on every run — which is the
	// point of a checkpoint, and the one place where masking the digest would
	// leave the file asserting nothing.
	{"assertion_list", []string{"assertion", "list", "fx"}},
	{"grant_list", []string{"grant", "list", "fx"}},
	{"redaction_list", []string{"redaction", "list", "fx"}},
	{"redaction_tombstones", []string{"redaction", "tombstones", "fx"}},
	{"debug_hash_check", []string{"debug", "hash-check", "fx"}},
	{"debug_indexes", []string{"debug", "indexes", "fx"}},
	{"debug_stats", []string{"debug", "stats", "fx"}},
	{"debug_integrity", []string{"debug", "integrity", "fx"}},

	// The refusals. Their wording is the whole product — an operator reads it
	// once and decides what to do next — so it is worth pinning.
	{"refuse_maintenance_repair", []string{"maintenance", "repair", "fx"}},
	{"refuse_maintenance_vacuum", []string{"maintenance", "vacuum", "fx"}},
	{"refuse_wal_compact", []string{"wal", "compact", "fx"}},

	// JSON. One per shape rather than one per command: the renderer is shared,
	// so a second table adds a file and no coverage.
	{"json_store_info", []string{"-json", "store", "info", "fx"}},
	{"json_node_list", []string{"-json", "node", "list", "-type", "MicroArtefact", "fx"}},
	{"json_debug_integrity", []string{"-json", "debug", "integrity", "fx"}},
	{"json_wal_compact", []string{"-json", "wal", "compact", "fx"}},
	{"json_indent", []string{"-json", "-indent", "store", "csr", "fx"}},
}

func TestGoldenOutput(t *testing.T) {
	// Resolved before the fixture moves the working directory: testdata is
	// relative to the package, and the commands under test have to run from
	// somewhere else entirely so that the store operand can be a short, fixed
	// relative path.
	goldenDir, err := filepath.Abs(filepath.Join("testdata", "golden"))
	if err != nil {
		t.Fatalf("locate testdata: %v", err)
	}
	goldenFixture(t)

	for _, tc := range goldenCases {
		t.Run(tc.name, func(t *testing.T) {
			out, errb, code := exec(t, tc.argv...)
			got := render(tc.argv, out, errb, code)

			path := filepath.Join(goldenDir, tc.name+".txt")
			if *update {
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}
			wantB, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v\nrun `go test ./cmd/graphene/ -run TestGoldenOutput -update` to create it", err)
			}
			if want := string(wantB); got != want {
				t.Errorf("output changed.\n"+
					"If the change is intended, accept it with:\n"+
					"  go test ./cmd/graphene/ -run TestGoldenOutput -update\n\n"+
					"--- want (%s) ---\n%s\n--- got ---\n%s", path, want, got)
			}
		})
	}
}

// render is the recorded form: the invocation, the exit status, and both
// streams. stderr is included because the notices that say a store was opened,
// or that a profile supplied the keys, are load-bearing — a report that stops
// disclosing how it was produced has regressed even if stdout is unchanged.
func render(argv []string, out, errb string, code int) string {
	var b strings.Builder
	b.WriteString("$ graphene ")
	b.WriteString(strings.Join(argv, " "))
	b.WriteString("\nexit ")
	b.WriteString(itoa(code))
	b.WriteString("\n--- stdout ---\n")
	b.WriteString(scrub(out))
	b.WriteString("--- stderr ---\n")
	b.WriteString(scrub(errb))
	return b.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var d []byte
	for n > 0 {
		d = append([]byte{byte('0' + n%10)}, d...)
		n /= 10
	}
	if neg {
		return "-" + string(d)
	}
	return string(d)
}

// scrub removes what legitimately differs between machines. Everything else is
// compared byte for byte, including whitespace: column alignment is the thing
// these files exist to protect.
//
// The version trailer that every JSON report carries names the toolchain and
// the platform it was built on. Those are facts about the machine, not about
// the store. Wall-clock stamps are facts about when the fixture was built.
//
// Nothing else is masked — in particular, digests and sizes are left alone.
// That the snapshot root of this fixture is the same on every machine and in
// every run is not an accident to be papered over: it is the property that
// makes the store verifiable, and a corpus that masked it would be unable to
// report the one change that matters most.
func scrub(s string) string {
	s = timestamp.ReplaceAllString(s, "<TIME>")
	for _, r := range []struct{ from, to string }{
		{`"go_version":"` + runtime.Version() + `"`, `"go_version":"<GO>"`},
		{`"go_version": "` + runtime.Version() + `"`, `"go_version": "<GO>"`},
		{`"os":"` + runtime.GOOS + `"`, `"os":"<OS>"`},
		{`"os": "` + runtime.GOOS + `"`, `"os": "<OS>"`},
		{`"arch":"` + runtime.GOARCH + `"`, `"arch":"<ARCH>"`},
		{`"arch": "` + runtime.GOARCH + `"`, `"arch": "<ARCH>"`},
	} {
		s = strings.ReplaceAll(s, r.from, r.to)
	}
	// Windows renders the operand with a backslash where POSIX renders a
	// forward slash. The corpus is recorded with forward slashes so one set of
	// files serves every platform in the CI matrix.
	s = strings.ReplaceAll(s, `fx\`, "fx/")
	s = strings.ReplaceAll(s, `fx\\`, "fx/")
	return s
}

// goldenFixture builds the same store as fixture() at a relative path, inside a
// working directory belonging to this test. t.Chdir restores the previous
// directory when the test ends.
func goldenFixture(t *testing.T) {
	t.Helper()
	work := t.TempDir()
	dir := filepath.Join(work, "fx")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ids, err := g.AddNodes([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
		{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{Labels: []store.NodeType{store.NodeTypeTag}},
	})
	if err != nil {
		t.Fatalf("add nodes: %v", err)
	}
	e1, e2, a1, a2, t1 := ids[0], ids[1], ids[2], ids[3], ids[4]

	if _, err := g.AddEdges([]*store.Edge{
		{Src: e1, Dst: a1, Labels: []store.EdgeType{store.EdgeTypeContains}},
		{Src: e1, Dst: a2, Labels: []store.EdgeType{store.EdgeTypeContains}},
		{Src: e2, Dst: a2, Labels: []store.EdgeType{store.EdgeTypeContains}},
		{Src: a1, Dst: a2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.75},
		{Src: a2, Dst: t1, Labels: []store.EdgeType{store.EdgeTypeTaggedWith}},
	}); err != nil {
		t.Fatalf("add edges: %v", err)
	}
	// Written one at a time and in a fixed order: ranging a map here would let
	// iteration order reach the index, and the corpus would flap.
	for _, kv := range []struct {
		id  store.NodeID
		sha string
	}{{a1, "aa11"}, {a2, "bb22"}} {
		if err := g.IndexNodeProperties(kv.id, map[string][]byte{"sha256": []byte(kv.sha)}); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	t.Chdir(work)
}
