package main

// Tests for the unique-key surface: debug unique, node upsert, edge upsert.
//
// The one that matters is TestNodeUpsertIsIdempotent. Every other assertion
// here is about wording or exit codes; that one is the reason the commands
// exist, and it fails if upsert ever stops being upsert — two invocations of
// `node create` and two of `node upsert` differ in exactly the count they
// leave behind.

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// countOf reads one labelled count out of a `node count` / `edge count` report.
// Both commands print both lines, so matching a bare digit in the output would
// be answered by whichever line happened to contain it.
func countOf(t *testing.T, out, label string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^` + label + `\s+(\d+)`).FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no %s count in:\n%s", label, out)
	}
	return m[1]
}

// emptyStore is an initialised store with nothing in it, closed — an Open that
// is never closed keeps the exclusive lock, and every command run afterwards is
// then refused by a lock the test itself holds.
func emptyStore(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// dupFixture is a store whose "k" property is held twice by one value, so a
// declaration over it must fail and name both holders.
func dupFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, v := range []string{"ver:9f3a", "ver:9f3a", "ver:be01"} {
		id, err := g.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeEvidenceFile},
		})
		if err != nil {
			t.Fatalf("add: %v", err)
		}
		if err := g.IndexNodeProperty(id, "k", []byte(v)); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

func TestDebugUniqueAcceptsAKeyThatIsUnique(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := g.IndexNodeProperty(id, "k", []byte("only")); err != nil {
		t.Fatalf("index: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	out, errb, code := exec(t, "debug", "unique", "-key", "k", dir)
	if code != 0 {
		t.Fatalf("exit %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(out, "yes") {
		t.Errorf("the report does not say the key is unique:\n%s", out)
	}
}

// TestDebugUniqueNamesEveryConflict. Reporting the first duplicate and stopping
// would make a repair one pass per duplicate, which is the whole reason the
// library returns all of them.
func TestDebugUniqueNamesEveryConflict(t *testing.T) {
	dir := dupFixture(t)

	out, _, code := exec(t, "debug", "unique", "-key", "k", dir)
	if code != 1 {
		t.Fatalf("exit %d, want 1 — a violated constraint is a broken verdict\n%s", code, out)
	}
	if !strings.Contains(out, "ver:9f3a") {
		t.Errorf("the conflicting value is not named:\n%s", out)
	}
	for _, id := range []string{"1", "2"} {
		if !strings.Contains(out, id) {
			t.Errorf("holder %s is missing from the report:\n%s", id, out)
		}
	}
	// The value held once must not be reported as a conflict.
	if strings.Contains(out, "ver:be01") {
		t.Errorf("a value held once was reported as a conflict:\n%s", out)
	}
}

func TestDebugUniqueNeedsAKey(t *testing.T) {
	if _, _, code := exec(t, "debug", "unique", fixture(t)); code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
}

// TestNodeUpsertIsIdempotent is the requirement, expressed as one test: the
// same command run twice leaves one node, where `node create` run twice leaves
// two.
func TestNodeUpsertIsIdempotent(t *testing.T) {
	dir := emptyStore(t)

	argv := []string{"node", "upsert", "-confirm", "-key", "k", "-value", "ver:9f3a",
		"-label", "EvidenceFile", "-index", "state=extracted", dir}

	out, errb, code := exec(t, argv...)
	if code != 0 {
		t.Fatalf("first upsert exited %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(out, "created") {
		t.Errorf("the first upsert did not report a create:\n%s", out)
	}

	out, errb, code = exec(t, argv...)
	if code != 0 {
		t.Fatalf("second upsert exited %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(out, "replaced") {
		t.Errorf("the second upsert did not report a replace:\n%s", out)
	}

	count, _, code := exec(t, "node", "count", dir)
	if code != 0 {
		t.Fatalf("node count exited %d", code)
	}
	if got := countOf(t, count, "nodes"); got != "1" {
		t.Errorf("two runs of node upsert left %s nodes, want 1:\n%s", got, count)
	}
}

// TestNodeUpsertRefusesADuplicatedKey: the declaration happens before anything
// is written, so a store that already violates the constraint is refused rather
// than added to.
func TestNodeUpsertRefusesADuplicatedKey(t *testing.T) {
	dir := dupFixture(t)

	out, errb, code := exec(t, "node", "upsert", "-confirm", "-key", "k",
		"-value", "ver:new", "-label", "Tag", dir)
	if code == 0 {
		t.Fatalf("the upsert ran against a store that violates its own key\n%s", out)
	}
	if !strings.Contains(errb, "debug unique") {
		t.Errorf("the refusal does not say what to run next:\n%s", errb)
	}
	// The count, not the directory bytes. This refusal happens after the store
	// is opened for writing, and opening moves file mtimes on its own — unlike
	// the framework's -confirm gate, which is checked before anything is opened
	// and which TestMutationRegister asserts on the bytes for. What has to be
	// true here is that no node was added.
	count, _, _ := exec(t, "node", "count", dir)
	if got := countOf(t, count, "nodes"); got != "3" {
		t.Errorf("a refused upsert left %s nodes, want the 3 it started with", got)
	}
}

// TestNodeUpsertRefusesIndexingItsOwnKey. Registering the key from -index as
// well would write it twice, and with a different value it would move the
// entity's name in the same breath as using it.
func TestNodeUpsertRefusesIndexingItsOwnKey(t *testing.T) {
	_, errb, code := exec(t, "node", "upsert", "-confirm", "-key", "k", "-value", "v",
		"-label", "Tag", "-index", "k=other", fixture(t))
	if code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
	if !strings.Contains(errb, "-index must not name") {
		t.Errorf("the refusal does not say why:\n%s", errb)
	}
}

// TestUpsertDryRunSaysWhichEffect. A dry run that will not say whether the
// entity exists has answered the least interesting half of the question.
func TestUpsertDryRunSaysWhichEffect(t *testing.T) {
	dir := emptyStore(t)
	create := []string{"-dry-run", "node", "upsert", "-key", "k", "-value", "ver:9f3a",
		"-label", "EvidenceFile", dir}

	out, errb, code := exec(t, create...)
	if code != 0 {
		t.Fatalf("exit %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(out, "create") {
		t.Errorf("a dry run over an absent key does not say it would create:\n%s", out)
	}

	if _, errb, code := exec(t, "node", "upsert", "-confirm", "-key", "k",
		"-value", "ver:9f3a", "-label", "EvidenceFile", dir); code != 0 {
		t.Fatalf("upsert exited %d\n%s", code, errb)
	}
	out, _, code = exec(t, create...)
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if !strings.Contains(out, "replace") {
		t.Errorf("a dry run over a present key does not say it would replace:\n%s", out)
	}
}

func TestEdgeUpsertIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := g.AddNodes([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
	}); err != nil {
		t.Fatalf("add nodes: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	argv := []string{"edge", "upsert", "-confirm", "-key", "ek", "-value", "1:2:contains",
		"-src", "1", "-dst", "2", "-label", "Contains", dir}

	if out, errb, code := exec(t, argv...); code != 0 {
		t.Fatalf("first upsert exited %d\n%s\n%s", code, out, errb)
	}
	out, errb, code := exec(t, argv...)
	if code != 0 {
		t.Fatalf("second upsert exited %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(out, "replaced") {
		t.Errorf("the second edge upsert did not report a replace:\n%s", out)
	}

	count, _, _ := exec(t, "edge", "count", dir)
	if got := countOf(t, count, "edges"); got != "1" {
		t.Errorf("two runs of edge upsert left %s edges, want 1:\n%s", got, count)
	}
}

func TestEdgeUpsertNeedsEndpoints(t *testing.T) {
	_, errb, code := exec(t, "edge", "upsert", "-confirm", "-key", "ek", "-value", "v",
		"-label", "Contains", fixture(t))
	if code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
	if !strings.Contains(errb, "-src") {
		t.Errorf("the refusal does not name the missing flag:\n%s", errb)
	}
}
