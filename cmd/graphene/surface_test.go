package main

// Tests for the surface added on top of the original fifteen verbs: the query
// and traversal groups, the governance ledgers, point-in-time restore, and the
// mutation register CONTRIBUTING.md §2 asks for.
//
// The register is the part worth reading. Every command that declares Mutates
// gets two assertions that can actually fail: omitting -confirm must refuse,
// and -dry-run must leave the directory byte-for-byte as it was. Asserting on
// the exit code alone would pass even if the command wrote, and a test that
// cannot fail for the reason it exists is worse than no test at all.

import (
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// governedFixture builds a store with all three ledgers, so the governance
// commands have something to read and the write commands somewhere to write.
//
// The plain fixture deliberately has none of them: most stores do not, and the
// commands have to report that honestly rather than failing. Both shapes are
// exercised.
func governedFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	g, err := graphene.OpenWithOptions(dir, disk.Options{
		Audit: true, Redaction: true, Roles: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ids, err := g.AddNodes([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{Labels: []store.NodeType{store.NodeTypeTag}},
	})
	if err != nil {
		t.Fatalf("add nodes: %v", err)
	}
	if _, err := g.AddEdges([]*store.Edge{
		{Src: ids[0], Dst: ids[1], Labels: []store.EdgeType{store.EdgeTypeContains}},
		{Src: ids[1], Dst: ids[2], Labels: []store.EdgeType{store.EdgeTypeTaggedWith}},
	}); err != nil {
		t.Fatalf("add edges: %v", err)
	}

	ds, ok := g.Forensics()
	if !ok {
		t.Fatal("a disk-backed graph should expose its store")
	}
	if err := ds.RecordAudit(disk.AuditCustom, 7, "fixture built"); err != nil {
		t.Fatalf("audit: %v", err)
	}
	if _, err := ds.GrantRole(7, 1, disk.CapWrite|disk.CapRedact,
		disk.GrantRequest{GrantedBy: 1, Reason: "fixture"}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := ds.RedactNode(ids[2],
		disk.RedactionRequest{ActorID: 7, RoleID: 1, Reason: "fixture"}); err != nil {
		t.Fatalf("redact: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// TestGovernanceReads runs every read-only governance command against a store
// that actually has the ledgers.
func TestGovernanceReads(t *testing.T) {
	dir := governedFixture(t)
	for _, argv := range [][]string{
		{"assertion", "list", dir},
		{"assertion", "verify", dir},
		{"grant", "list", dir},
		{"grant", "verify", dir},
		{"grant", "check", "-actor", "7", "-cap", "redact", dir},
		{"redaction", "list", dir},
		{"redaction", "verify", dir},
		{"redaction", "impact", "-node", "2", dir},
		{"redaction", "tombstones", dir},
		{"keys", "timeline", dir},
		{"anchor", "list", dir},
		{"anchor", "show", dir},
		{"wal", "segments", dir},
		{"wal", "verify", dir},
		{"store", "stats", dir},
		{"store", "snapshot", dir},
		{"store", "health", dir},
		{"debug", "hash-check", dir},
		{"debug", "signature-check", dir},
		{"debug", "stats", dir},
		{"debug", "integrity", dir},
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

// TestLedgersAreOpenedWhenTheStoreHasThem is the regression test for a real
// defect, and the reason ledgersPresent exists.
//
// The audit ledger is a per-open option. A store whose directory holds
// graphene.audit but which is opened without Options.Audit has no audit log as
// far as the engine is concerned, and RecordAudit on it *succeeds and writes
// nothing*. Before the fix `assertion add` reported a successful write against
// a store it had not written to.
func TestLedgersAreOpenedWhenTheStoreHasThem(t *testing.T) {
	dir := governedFixture(t)

	before, err := disk.ReadAuditLog(dir)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if _, _, code := exec(t, "assertion", "add",
		"-actor", "9", "-detail", "recorded by the test", dir); code != 0 {
		t.Fatalf("assertion add exited %d", code)
	}

	after, err := disk.ReadAuditLog(dir)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if len(after) != len(before)+1 {
		t.Fatalf("the chain went from %d entries to %d; the entry was not written",
			len(before), len(after))
	}
	if got := after[len(after)-1].Detail; got != "recorded by the test" {
		t.Errorf("last entry says %q", got)
	}
	if err := disk.VerifyAuditChain(after); err != nil {
		t.Errorf("the chain no longer verifies after an append: %v", err)
	}
}

// TestAuditKindsBelowCustomAreRefused: an audit log a caller can write engine
// history into is not evidence of what the engine did. CustodyFor compares
// recorded compactions against retired segments to catch a compaction that was
// never recorded, and a forgeable record defeats that check.
func TestAuditKindsBelowCustomAreRefused(t *testing.T) {
	dir := governedFixture(t)
	before, _ := disk.ReadAuditLog(dir)

	_, errb, code := exec(t, "assertion", "add",
		"-kind", "compact", "-actor", "9", "-detail", "fabricated", dir)
	if code != 2 {
		t.Fatalf("exit %d, want 2 (a usage error)", code)
	}
	if !strings.Contains(errb, "engine") {
		t.Errorf("the refusal does not say why:\n%s", errb)
	}

	after, _ := disk.ReadAuditLog(dir)
	if len(after) != len(before) {
		t.Errorf("the refused entry was written anyway: %d -> %d", len(before), len(after))
	}
}

// TestMutationRegister is the register CONTRIBUTING.md §2 asks for: every
// command that declares Mutates, checked twice.
func TestMutationRegister(t *testing.T) {
	for _, c := range registry {
		if !c.Mutates {
			continue
		}
		t.Run(strings.ReplaceAll(c.Path(), " ", "_"), func(t *testing.T) {
			dir := governedFixture(t)
			argv := append(strings.Fields(c.Path()), mutationArgs(c.Path())...)

			// Without -confirm it must refuse, and must not have written.
			before := snapshotDir(t, dir)
			_, errb, code := exec(t, append(argv, dir)...)
			if code == 0 {
				t.Errorf("%s ran without -confirm", c.Path())
			}
			if !strings.Contains(errb, "-confirm") {
				t.Errorf("the refusal does not name -confirm:\n%s", errb)
			}
			if after := snapshotDir(t, dir); after != before {
				t.Errorf("a refused %s changed the store", c.Path())
			}

			// -dry-run must leave the directory byte-for-byte as it was.
			before = snapshotDir(t, dir)
			out, errb, code := exec(t, append([]string{"-dry-run"}, append(argv, dir)...)...)
			if code != 0 {
				t.Errorf("dry-run %s exited %d\n%s\n%s", c.Path(), code, out, errb)
			}
			if after := snapshotDir(t, dir); after != before {
				t.Errorf("a dry run of %s changed the store:\nbefore %s\nafter  %s",
					c.Path(), before, after)
			}
		})
	}
}

// mutationArgs supplies the flags each mutating command needs to get past its
// own validation and reach the gate. Kept here rather than in the registry: a
// command should not carry its test's arguments.
func mutationArgs(path string) []string {
	switch path {
	case "redaction apply":
		return []string{"-node", "1", "-actor", "7", "-reason", "a test"}
	case "node create":
		return []string{"-label", "MicroArtefact", "-index", "sha256=cc33"}
	case "edge create":
		return []string{"-src", "1", "-dst", "2", "-label", "Reuse"}
	case "node delete":
		return []string{"-id", "1"}
	case "edge delete":
		return []string{"-id", "1"}
	default:
		return nil
	}
}

// TestRestoreRoundTrip: backup, verify, restore, and the restored store holds
// what the original did. Point-in-time restore had no shell path at all before
// this, so this is the first time the CLI has exercised it.
func TestRestoreRoundTrip(t *testing.T) {
	src := governedFixture(t)
	backup := t.TempDir() + "-backup"
	dst := t.TempDir() + "-restored"

	if out, errb, code := exec(t, "backup", "create", "-to", backup, src); code != 0 {
		t.Fatalf("backup create exited %d\n%s\n%s", code, out, errb)
	}
	if out, errb, code := exec(t, "backup", "verify", backup); code != 0 {
		t.Fatalf("backup verify exited %d\n%s\n%s", code, out, errb)
	}
	if out, errb, code := exec(t, "backup", "restore", "-to", dst, backup); code != 0 {
		t.Fatalf("backup restore exited %d\n%s\n%s", code, out, errb)
	}

	before := counts(t, src)
	after := counts(t, dst)
	if before != after {
		t.Errorf("the restored store holds %v, the original %v", after, before)
	}
}

// TestRestoreRefusesANonEmptyDestination. Restoring over a store is refused
// rather than merged: a directory holding half of one store and half of another
// is not recoverable by anything.
func TestRestoreRefusesANonEmptyDestination(t *testing.T) {
	src := governedFixture(t)
	backup := t.TempDir() + "-backup2"
	if _, _, code := exec(t, "backup", "create", "-to", backup, src); code != 0 {
		t.Fatalf("backup create exited %d", code)
	}

	occupied := governedFixture(t)
	before := snapshotDir(t, occupied)
	_, errb, code := exec(t, "backup", "restore", "-to", occupied, backup)
	if code == 0 {
		t.Fatal("restoring over an existing store should be refused")
	}
	if !strings.Contains(errb, "not empty") {
		t.Errorf("the refusal does not say why:\n%s", errb)
	}
	if after := snapshotDir(t, occupied); after != before {
		t.Error("the refused restore wrote into the destination anyway")
	}
}

// TestBudgetRefusalIsAFindingNotAFailure. A walk that hits its limit is a
// question too big for the limits it was asked under, not a broken store — and
// a scripted traversal that failed closed on every hub node would be useless.
func TestBudgetRefusalIsAFindingNotAFailure(t *testing.T) {
	dir := fixture(t)
	out, _, code := exec(t, "traverse", "bfs", "-from", "1", "-max-nodes", "1", dir)
	if code != 0 {
		t.Fatalf("a budget refusal exited %d, want 0", code)
	}
	if !strings.Contains(out, "budget") {
		t.Errorf("the report does not say a budget stopped it:\n%s", out)
	}
	if strings.Contains(out, "VERIFIED") {
		t.Errorf("a refused walk reported VERIFIED:\n%s", out)
	}
}

// TestFilterGrammar checks the -prop parser against the operators the help
// text promises, since a filter that parses to the wrong operator returns a
// confidently wrong answer rather than an error.
func TestFilterGrammar(t *testing.T) {
	for _, tc := range []struct {
		spec string
		key  string
		op   store.PropertyOp
		val  string
		up   string
	}{
		{"k=v", "k", store.PropertyOpEqual, "v", ""},
		{"k~v", "k", store.PropertyOpContains, "v", ""},
		{"k^v", "k", store.PropertyOpPrefix, "v", ""},
		{"k>v", "k", store.PropertyOpGreaterThan, "v", ""},
		{"k>=v", "k", store.PropertyOpGreaterThanOrEqual, "v", ""},
		{"k<v", "k", store.PropertyOpLessThan, "v", ""},
		{"k<=v", "k", store.PropertyOpLessThanOrEqual, "v", ""},
		{"k[a:b]", "k", store.PropertyOpBetweenInclusive, "a", "b"},
		// A value may contain operator characters; a key may not. This is the
		// case that matters on Windows, where paths carry both.
		{`path^C:\Users`, "path", store.PropertyOpPrefix, `C:\Users`, ""},
	} {
		f, err := parseFilter(tc.spec)
		if err != nil {
			t.Errorf("%q: %v", tc.spec, err)
			continue
		}
		if f.Key != tc.key || f.Op != tc.op ||
			string(f.Value) != tc.val || string(f.ValueUpper) != tc.up {
			t.Errorf("%q parsed as key=%q op=%v value=%q upper=%q",
				tc.spec, f.Key, f.Op, f.Value, f.ValueUpper)
		}
	}

	for _, bad := range []string{"novalue", "=novalue", "k[nocolon]"} {
		if _, err := parseFilter(bad); err == nil {
			t.Errorf("%q parsed without error", bad)
		}
	}
}

// counts reads the node and edge totals straight from the library, so the
// round-trip check does not depend on the CLI's own output.
func counts(t *testing.T, dir string) [2]uint64 {
	t.Helper()
	g, err := graphene.OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer g.Close()
	n, err := g.NodeCount()
	if err != nil {
		t.Fatalf("node count: %v", err)
	}
	e, err := g.EdgeCount()
	if err != nil {
		t.Fatalf("edge count: %v", err)
	}
	return [2]uint64{n, e}
}
