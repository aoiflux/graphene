package main

// `store migrate -to`: which format version the new image is written in.
//
// The assertion that matters here is not the version number. A downgrade
// re-encodes the property index — GPIX and GPIR become GIDX, and the store that
// reopens holds in memory what it used to read off the disk — so the question
// worth a test is whether the store still answers everything it answered before.
// The round trip therefore compares *answers* across both directions, and the
// version number is checked as well, because a migration that quietly did
// nothing would pass an answers-only test.
//
// The fixture carries one shape per thing the two encodings have to agree about:
// an all-distinct key, a low-cardinality key where many entities share one
// value, a declared ordered key so range queries go through a structure rather
// than a scan, a composite tuple, and an edge key — the reverse direction is
// per-kind in both encodings and an edge entry is the one that catches a node-only
// walk.

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// migrateFixture builds a compacted store with an index of every shape.
func migrateFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ds, ok := g.Forensics()
	if !ok {
		t.Fatal("a disk-backed graph should expose its store")
	}
	// Declared before anything is indexed, so the ordered structure is built by
	// the incremental path rather than a backfill — and so the declaration is in
	// the image's own catalogue for the migration to carry across.
	if err := ds.DeclareOrderedNodeProperty("seq"); err != nil {
		t.Fatalf("declare ordered: %v", err)
	}
	if err := ds.DeclareCompositeNodeProperties([]string{"bucket", "shard"}); err != nil {
		t.Fatalf("declare composite: %v", err)
	}

	const n = 40
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("add nodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("%064x", i)),
			"seq":    []byte(fmt.Sprintf("%08d", i)),
			"bucket": []byte(fmt.Sprintf("b%d", i%4)),
			"shard":  []byte(fmt.Sprintf("s%d", i%2)),
		}); err != nil {
			t.Fatalf("index node %d: %v", id, err)
		}
	}

	edges := make([]*store.Edge, 0, n-1)
	for i := 1; i < n; i++ {
		edges = append(edges, &store.Edge{
			Src:    ids[i-1],
			Dst:    ids[i],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	eids, err := g.AddEdges(edges)
	if err != nil {
		t.Fatalf("add edges: %v", err)
	}
	for i, id := range eids {
		if err := g.IndexEdgeProperties(id, map[string][]byte{
			"rel": []byte(fmt.Sprintf("r%d", i%3)),
		}); err != nil {
			t.Fatalf("index edge %d: %v", id, err)
		}
	}

	if err := g.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// answers asks the store everything the index can be asked and renders the
// replies as one comparable string.
//
// Read-only, so it can run between migrations without taking a write lock, and
// it reopens every time: the whole point is what a *fresh* open of the migrated
// file answers, which is where a re-encoded index would show a difference.
func answers(t *testing.T, dir string) string {
	t.Helper()
	g, err := graphene.OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("reopen %s: %v", dir, err)
	}
	defer g.Close()

	var b strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	byType, err := g.CountNodesByType()
	if err != nil {
		t.Fatalf("CountNodesByType: %v", err)
	}
	p("nodes by type: %v", byType)
	edgeTypes, err := g.CountEdgesByType()
	if err != nil {
		t.Fatalf("CountEdgesByType: %v", err)
	}
	p("edges by type: %v", edgeTypes)

	// The whole forward index, key by key: every distinct value and how many
	// entities hold it. A re-encoding that dropped, duplicated or mis-sorted an
	// entry changes this line.
	for _, key := range []string{"sha256", "seq", "bucket", "shard"} {
		counts, cerr := g.CountNodesByProperty(key)
		if cerr != nil {
			t.Fatalf("CountNodesByProperty(%q): %v", key, cerr)
		}
		p("node %s: %d distinct, %v", key, len(counts), countPairs(counts))
	}
	edgeCounts, err := g.CountEdgesByProperty("rel")
	if err != nil {
		t.Fatalf("CountEdgesByProperty: %v", err)
	}
	p("edge rel: %v", countPairs(edgeCounts))

	// Exact match, prefix, and a range over the declared ordered key.
	for _, f := range []store.PropertyFilter{
		{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte(fmt.Sprintf("%064x", 7))},
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")},
		{Key: "seq", Op: store.PropertyOpPrefix, Value: []byte("000000")},
		{Key: "seq", Op: store.PropertyOpBetweenInclusive,
			Value: []byte("00000010"), ValueUpper: []byte("00000019")},
		{Key: "seq", Op: store.PropertyOpGreaterThan, Value: []byte("00000035")},
	} {
		got, qerr := g.QueryNodeIDs(store.NodeQuery{Filters: []store.PropertyFilter{f}})
		if qerr != nil {
			t.Fatalf("query %s op %d: %v", f.Key, f.Op, qerr)
		}
		p("query %s op %d -> %v", f.Key, f.Op, got)
	}

	// The composite, which is resident under either encoding but is *filled*
	// from different sources: the image's entries under v8, the base's runs
	// under v9.
	comp, err := g.QueryNodeIDs(store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b1")},
		{Key: "shard", Op: store.PropertyOpEqual, Value: []byte("s1")},
	}})
	if err != nil {
		t.Fatalf("composite query: %v", err)
	}
	p("composite b1+s1 -> %v", comp)

	// And then every entry in the index, both kinds, through the enumerator the
	// compaction itself reads. This is the total comparison: the two encodings
	// hold the same triples or they do not. Sorted rather than compared in
	// iteration order, because the order is the byte-determinism test's business
	// and not this one's.
	ds, ok := g.Forensics()
	if !ok {
		t.Fatal("a disk-backed graph should expose its store")
	}
	var entries []string
	ds.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
		entries = append(entries, fmt.Sprintf("n%d %s=%s", id, key, value))
		return true
	})
	ds.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
		entries = append(entries, fmt.Sprintf("e%d %s=%s", id, key, value))
		return true
	})
	slices.Sort(entries)
	p("index holds %d entries", len(entries))
	for _, e := range entries {
		p("  %s", e)
	}
	return b.String()
}

// countPairs renders a value->count map in one comparable line.
func countPairs(m map[string]uint64) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, fmt.Sprintf("%s=%d", k, v))
	}
	slices.Sort(out)
	return out
}

// fieldOf reads one labelled value out of a human-mode report.
//
// The renderer pads labels to the widest one in the section, so the gap between
// a label and its value depends on what else is in the report. Asserting on
// "target format      v8" is therefore asserting on a column width, which is
// how a test comes to fail for a reason nobody meant it to check.
func fieldOf(out, label string) (string, bool) {
	for _, line := range strings.Split(out, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), label)
		if !ok || (rest != "" && !strings.HasPrefix(rest, " ")) {
			continue
		}
		return strings.TrimSpace(rest), true
	}
	return "", false
}

// wantField fails unless the report carries label with exactly this value.
func wantField(t *testing.T, out, label, want string) {
	t.Helper()
	got, ok := fieldOf(out, label)
	if !ok {
		t.Errorf("the report has no %q field:\n%s", label, out)
		return
	}
	if got != want {
		t.Errorf("%s is %q, want %q:\n%s", label, got, want, out)
	}
}

// imageDigestOf hashes the image, which is what "did not write" has to mean
// here. snapshotDir cannot serve: graphene.lock's mtime changes on every open,
// including the read-only one a plan performs, so a directory-wide comparison
// fails for a file the command is supposed to touch.
func imageDigestOf(t *testing.T, dir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		if os.IsNotExist(err) {
			return "no image"
		}
		t.Fatalf("read image: %v", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// imageVersionOf reads the container version off the image.
func imageVersionOf(t *testing.T, dir string) uint16 {
	t.Helper()
	info, err := disk.InspectCSR(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	return info.Version
}

// indexSectionsOf names the index sections the image actually carries, so a
// version number is not taken as evidence about the bytes under it.
func indexSectionsOf(t *testing.T, dir string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	var found []string
	for _, magic := range []string{"GIDX", "GPIX", "GPIR"} {
		if strings.Contains(string(data), magic) {
			found = append(found, magic)
		}
	}
	return found
}

// TestStoreMigrateTo_RoundTripsTheFormatWithoutChangingAnyAnswer is the test the
// flag exists for.
func TestStoreMigrateTo_RoundTripsTheFormatWithoutChangingAnyAnswer(t *testing.T) {
	dir := migrateFixture(t)

	if got := imageVersionOf(t, dir); got != disk.CSRVersionCurrent {
		t.Fatalf("fixture is v%d, want v%d — the default is what makes this a downgrade",
			got, disk.CSRVersionCurrent)
	}
	before := answers(t, dir)

	if _, errb, code := exec(t, "migrate", "-to", "8", dir); code != 0 {
		t.Fatalf("downgrade exited %d: %s", code, errb)
	}
	if got := imageVersionOf(t, dir); got != 8 {
		t.Fatalf("image is v%d after -to 8", got)
	}
	// The version says what a reader should expect; the sections are what it
	// gets. A v8 image carrying GPIX would be a lie told in two bytes.
	if got := indexSectionsOf(t, dir); len(got) != 1 || got[0] != "GIDX" {
		t.Errorf("v8 image carries index sections %v, want GIDX alone", got)
	}
	if got := answers(t, dir); got != before {
		t.Errorf("the downgrade changed an answer:\n--- before\n%s\n--- after\n%s", before, got)
	}

	if _, errb, code := exec(t, "migrate", "-to", "9", dir); code != 0 {
		t.Fatalf("upgrade exited %d: %s", code, errb)
	}
	if got := imageVersionOf(t, dir); got != 9 {
		t.Fatalf("image is v%d after -to 9", got)
	}
	if got := indexSectionsOf(t, dir); len(got) != 2 || got[0] != "GPIX" || got[1] != "GPIR" {
		t.Errorf("v9 image carries index sections %v, want GPIX and GPIR and no GIDX", got)
	}
	if got := answers(t, dir); got != before {
		t.Errorf("the upgrade changed an answer:\n--- before\n%s\n--- after\n%s", before, got)
	}
}

// TestStoreMigrateTo_DowngradeIsIdempotent: a second -to 8 has nothing to do and
// must not say it did something. The image is rewritten either way — a
// compaction always writes one — so this asserts the *report*, which is what an
// operator running it twice reads.
func TestStoreMigrateTo_DowngradeIsIdempotent(t *testing.T) {
	dir := migrateFixture(t)
	if _, errb, code := exec(t, "migrate", "-to", "8", dir); code != 0 {
		t.Fatalf("first downgrade exited %d: %s", code, errb)
	}
	first := answers(t, dir)

	out, errb, code := exec(t, "migrate", "-to", "8", dir)
	if code != 0 {
		t.Fatalf("second downgrade exited %d: %s", code, errb)
	}
	wantField(t, out, "migrated to", "v8")
	if got := imageVersionOf(t, dir); got != 8 {
		t.Errorf("image is v%d after two downgrades", got)
	}
	if got := answers(t, dir); got != first {
		t.Errorf("the second downgrade changed an answer:\n--- first\n%s\n--- second\n%s", first, got)
	}
}

// TestStoreMigrateTo_DefaultIsUnchanged: an invocation without -to writes what
// the build writes, which is what every existing caller depends on.
func TestStoreMigrateTo_DefaultIsUnchanged(t *testing.T) {
	dir := migrateFixture(t)
	if _, errb, code := exec(t, "migrate", "-to", "8", dir); code != 0 {
		t.Fatalf("downgrade exited %d: %s", code, errb)
	}
	out, errb, code := exec(t, "migrate", dir)
	if code != 0 {
		t.Fatalf("bare migrate exited %d: %s", code, errb)
	}
	if got := imageVersionOf(t, dir); got != disk.CSRVersionCurrent {
		t.Errorf("bare migrate produced v%d, want v%d", got, disk.CSRVersionCurrent)
	}
	if !strings.Contains(out, "target format") {
		t.Errorf("the report does not name a target:\n%s", out)
	}
}

// TestStoreMigrateTo_RefusesAVersionThisBuildCannotWrite checks the refusal
// happens at the flag, which is what keeps it from happening after the exclusive
// lock has been taken.
func TestStoreMigrateTo_RefusesAVersionThisBuildCannotWrite(t *testing.T) {
	dir := migrateFixture(t)
	before := snapshotDir(t, dir)

	for _, bad := range []string{"7", "10", "0", "banana", "-1", "v"} {
		_, errb, code := exec(t, "migrate", "-to", bad, dir)
		if code != 2 {
			t.Errorf("-to %s exited %d, want 2 (a usage error)", bad, code)
		}
		if !strings.Contains(errb, "-to") {
			t.Errorf("-to %s: the message does not name the flag:\n%s", bad, errb)
		}
	}
	// v7 is the interesting one: a real version, readable by this build, and not
	// writable by it. The message has to say which numbers are on offer rather
	// than that 7 is unknown, because 7 is not unknown.
	//
	// "v8 or v9" rather than the two substrings separately: a message reading
	// "this build writes v8v9" contains both of those and is not a sentence,
	// which is the mutant that survived the first version of this assertion.
	_, errb, _ := exec(t, "migrate", "-to", "7", dir)
	for _, want := range []string{"v8 or v9", "not v7"} {
		if !strings.Contains(errb, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, errb)
		}
	}

	// A value that is not a number at all is a different complaint. Reporting it
	// as "this build writes v8 or v9, not v0" would be a refusal and no help,
	// because nobody typed 0 — and it is what the parse check being dropped
	// actually produces, the two guards overlapping on the exit code and
	// differing only in what the operator is told. So the message is what is
	// asserted.
	_, errb, _ = exec(t, "migrate", "-to", "banana", dir)
	if !strings.Contains(errb, "not a format version") {
		t.Errorf("a non-numeric -to should be reported as unparseable:\n%s", errb)
	}
	if strings.Contains(errb, "v0") {
		t.Errorf("a non-numeric -to was reported as though it named version 0:\n%s", errb)
	}
	if after := snapshotDir(t, dir); after != before {
		t.Errorf("a refused -to changed the store:\nbefore %s\nafter  %s", before, after)
	}
}

// TestStoreMigrateTo_CheckAndDryRunReportWithoutWriting covers both ways of
// asking what would happen.
//
// -dry-run is the regression: the framework downgrades the open to read-only
// under it, so the handler's compaction used to fail with the engine's
// "store is open read-only" — a global flag the tool advertises, breaking on a
// command that had a perfectly good answer available.
func TestStoreMigrateTo_CheckAndDryRunReportWithoutWriting(t *testing.T) {
	dir := migrateFixture(t)
	before := imageDigestOf(t, dir)

	for _, tc := range []struct {
		argv []string
		want string
	}{
		{[]string{"migrate", "-check", "-to", "8", dir}, "v8"},
		{[]string{"-dry-run", "migrate", "-to", "8", dir}, "v8"},
		{[]string{"migrate", "-check", dir}, "v9"},
		{[]string{"-dry-run", "migrate", dir}, "v9"},
	} {
		out, errb, code := exec(t, tc.argv...)
		if code != 0 {
			t.Errorf("%v exited %d: %s", tc.argv, code, errb)
			continue
		}
		wantField(t, out, "target format", tc.want)
	}
	if after := imageDigestOf(t, dir); after != before {
		t.Errorf("a plan rewrote the image:\nbefore %s\nafter  %s", before, after)
	}

	// And the plan says which way it would go, in both directions.
	out, _, _ := exec(t, "migrate", "-check", "-to", "8", dir)
	if !strings.Contains(out, "compact it as v8") {
		t.Errorf("the plan does not name v8:\n%s", out)
	}
	if !strings.Contains(out, "GIDX") {
		t.Errorf("the plan does not say what happens to the index:\n%s", out)
	}
	out, _, _ = exec(t, "migrate", "-check", dir)
	if !strings.Contains(out, "nothing to do") {
		t.Errorf("a store already at the target should have nothing to do:\n%s", out)
	}
}

// TestStoreMigrateTo_AcceptsTheSpellingItPrints. The report says "v8"; pasting
// that back in has to work, or the output is not a thing you can act on.
func TestStoreMigrateTo_AcceptsTheSpellingItPrints(t *testing.T) {
	dir := migrateFixture(t)
	out, errb, code := exec(t, "migrate", "-to", "v8", dir)
	if code != 0 {
		t.Fatalf("-to v8 exited %d: %s", code, errb)
	}
	if got := imageVersionOf(t, dir); got != 8 {
		t.Errorf("image is v%d after -to v8", got)
	}
	wantField(t, out, "migrated to", "v8")
}

// TestStoreMigrateTo_NeverCompactedStore: no image at all is an ordinary state,
// and -to has to mean the same thing for it.
func TestStoreMigrateTo_NeverCompactedStore(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := g.AddNodes([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeTag}},
	}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	out, errb, code := exec(t, "migrate", "-check", "-to", "8", dir)
	if code != 0 {
		t.Fatalf("-check on a never-compacted store exited %d: %s", code, errb)
	}
	if !strings.Contains(out, "never been compacted") {
		t.Errorf("the plan does not say there is no image:\n%s", out)
	}
	wantField(t, out, "target format", "v8")

	if _, errb, code := exec(t, "migrate", "-to", "8", dir); code != 0 {
		t.Fatalf("migrate exited %d: %s", code, errb)
	}
	if got := imageVersionOf(t, dir); got != 8 {
		t.Errorf("first image is v%d, want v8", got)
	}
}

// --- the framework hook ---

// TestOpenTuner_ReachesTheOptionsAndCannotUndoReadOnly.
//
// Two claims in one test, because the interesting thing about openTuner is the
// boundary: it may set the fields a command's flags decide, and it may not touch
// the one the framework guarantees. -dry-run downgrades the open mode, and a
// tuner that could set ReadOnly back to false would turn that guarantee into a
// suggestion — silently, on a flag an operator reaches for precisely when they
// are not sure.
func TestOpenTuner_ReachesTheOptionsAndCannotUndoReadOnly(t *testing.T) {
	dir := migrateFixture(t)

	// The control arm first: without a tuner, a read-only open of this v9 image
	// holds its index mapped. If the platform cannot map there is nothing here to
	// tell apart, and a test that cannot fail should say so rather than pass.
	plain := &Context{Ctx: context.Background(), Globals: &Globals{}, Cmd: storeMigrate,
		Target: dir, Out: io.Discard, Err: io.Discard}
	closer, err := plain.open(OpenGraphRO)
	if err != nil {
		t.Fatalf("open without a tuner: %v", err)
	}
	ds, ok := plain.Graph().Forensics()
	if !ok {
		t.Fatal("a disk-backed graph should expose its store")
	}
	held := ds.StorageStats().IndexMode
	closer()
	if held != disk.IndexMapped.String() {
		t.Skipf("this platform holds a v9 index as %q, so a tuned open cannot be "+
			"told from an untuned one", held)
	}

	tuned := &Context{Ctx: context.Background(), Globals: &Globals{}, Cmd: storeMigrate,
		Target: dir, Out: io.Discard, Err: io.Discard,
		tuneOpen: func(o *disk.Options) {
			o.IndexMode = disk.IndexResident
			// Not allowed, and the point of the second assertion below.
			o.ReadOnly = false
		}}
	closer, err = tuned.open(OpenGraphRO)
	if err != nil {
		t.Fatalf("open with a tuner: %v", err)
	}
	defer closer()

	ds, ok = tuned.Graph().Forensics()
	if !ok {
		t.Fatal("a disk-backed graph should expose its store")
	}
	if got := ds.StorageStats().IndexMode; got != disk.IndexResident.String() {
		t.Errorf("the tuner asked for a resident index and the store holds %q", got)
	}
	if _, err := tuned.Graph().AddNodes([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeTag}},
	}); err == nil {
		t.Error("a tuner set ReadOnly to false and the store accepted a write — " +
			"the open mode is the framework's guarantee, not a command's suggestion")
	}
}

// TestOpenTuner_IsAbsentUnlessTheOptionsAskForOne: adding the hook must be no
// change at all to the commands that do not use it.
func TestOpenTuner_IsAbsentUnlessTheOptionsAskForOne(t *testing.T) {
	var withTuner []string
	for _, c := range registry {
		fs := flag.NewFlagSet(c.Path(), flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		if _, tune := c.newFlags(fs); tune != nil {
			withTuner = append(withTuner, c.Path())
		}
	}
	// Named rather than counted. A second command growing a tuner is a decision
	// worth making on purpose, and this is where it gets noticed.
	if want := []string{"store migrate"}; !slices.Equal(withTuner, want) {
		t.Errorf("commands with an open tuner: %v, want %v", withTuner, want)
	}
}

// TestOpenTuner_RunsAfterTheFrameworksOwnOptions. The ledgers are detected from
// the directory and -metrics is bound at open; a tuner that ran first would be
// overwritten, and one that ran last must not overwrite them.
func TestOpenTuner_RunsAfterTheFrameworksOwnOptions(t *testing.T) {
	dir := governedFixture(t)
	var saw disk.Options
	cx := &Context{Ctx: context.Background(), Globals: &Globals{Metrics: true},
		Cmd: storeMigrate, Target: dir, Out: io.Discard, Err: io.Discard,
		tuneOpen: func(o *disk.Options) { saw = *o }}
	closer, err := cx.open(OpenGraphRO)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closer()

	if !saw.Audit || !saw.Redaction || !saw.Roles {
		t.Errorf("the tuner saw Audit=%v Redaction=%v Roles=%v; the directory has all "+
			"three, so it ran before the detection", saw.Audit, saw.Redaction, saw.Roles)
	}
	if saw.Metrics == nil {
		t.Error("the tuner saw no Metrics sink under -metrics, so it ran before the bind")
	}
}
