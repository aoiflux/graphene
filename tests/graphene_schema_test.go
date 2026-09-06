package graphene_test

// The declaration catalogue.
//
// A declaration tells the engine something it cannot work out for itself. Before
// graphene.schema, telling it did not survive: ordered keys and composite tuples
// came back only after a compaction, and unique constraints came back never. The
// failure that mattered was not the lost optimisation but the lost promise — two
// processes could open one directory enforcing different rules, and the one that
// had not declared would write the duplicates the other existed to refuse.
//
// These tests pin what a caller now relies on: a declaration survives a bare
// reopen and is still enforced, it survives a backup, a catalogue naming a rule
// the data breaks refuses the open rather than dropping the rule quietly, a
// reader applies the catalogue and writes nothing, and a file this build does
// not understand is an error exactly when ignoring it would lose a constraint.

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

const (
	schemaFile       = "graphene.schema"
	schemaGoodHeader = "graphene-schema v1\n"
)

// declareAll makes one declaration of every kind the catalogue records.
func declareAll(t *testing.T, g *graphene.Graph) {
	t.Helper()
	if err := g.DeclareOrderedProperty("score"); err != nil {
		t.Fatalf("DeclareOrderedProperty: %v", err)
	}
	if err := g.DeclareOrderedEdgeProperty("confidence"); err != nil {
		t.Fatalf("DeclareOrderedEdgeProperty: %v", err)
	}
	if err := g.DeclareUniqueProperty("sha256"); err != nil {
		t.Fatalf("DeclareUniqueProperty: %v", err)
	}
	if err := g.DeclareUniqueEdgeProperty("edgekey"); err != nil {
		t.Fatalf("DeclareUniqueEdgeProperty: %v", err)
	}
	if err := g.DeclareCompositeProperties([]string{"case", "bucket"}); err != nil {
		t.Fatalf("DeclareCompositeProperties: %v", err)
	}
	if err := g.DeclareCompositeEdgeProperties([]string{"kind", "run"}); err != nil {
		t.Fatalf("DeclareCompositeEdgeProperties: %v", err)
	}
	if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
		t.Fatalf("DeclareUniqueEdge: %v", err)
	}
}

// assertAllDeclared reads every declaration back through the public accessors.
func assertAllDeclared(t *testing.T, g *graphene.Graph, when string) {
	t.Helper()

	onKeys, oeKeys := g.OrderedProperties()
	if !slices.Contains(onKeys, "score") {
		t.Errorf("%s: ordered node keys = %v, want score", when, onKeys)
	}
	if !slices.Contains(oeKeys, "confidence") {
		t.Errorf("%s: ordered edge keys = %v, want confidence", when, oeKeys)
	}

	unKeys, ueKeys := g.UniqueProperties()
	if !slices.Contains(unKeys, "sha256") {
		t.Errorf("%s: unique node keys = %v, want sha256", when, unKeys)
	}
	if !slices.Contains(ueKeys, "edgekey") {
		t.Errorf("%s: unique edge keys = %v, want edgekey", when, ueKeys)
	}

	cnKeys, ceKeys := g.CompositeProperties()
	if !containsTuple(cnKeys, []string{"case", "bucket"}) {
		t.Errorf("%s: composite node tuples = %v, want [case bucket]", when, cnKeys)
	}
	if !containsTuple(ceKeys, []string{"kind", "run"}) {
		t.Errorf("%s: composite edge tuples = %v, want [kind run]", when, ceKeys)
	}

	if !slices.Contains(g.UniqueEdges(), edgeOwns) {
		t.Errorf("%s: unique edge types = %v, want edgeOwns", when, g.UniqueEdges())
	}
}

func containsTuple(haystack [][]string, want []string) bool {
	for _, t := range haystack {
		if slices.Equal(t, want) {
			return true
		}
	}
	return false
}

func writeSchemaFile(t *testing.T, dir string, lines ...string) {
	t.Helper()
	body := schemaGoodHeader
	for _, l := range lines {
		body += l + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, schemaFile), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", schemaFile, err)
	}
}

// The headline: every declaration survives a close and a bare reopen, with no
// compaction in between. Before the catalogue, four of these seven came back
// only after a Compact and three came back never.
func TestSchema_SurvivesABareReopen(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	declareAll(t, g)
	assertAllDeclared(t, g, "before close")
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	assertAllDeclared(t, reopened, "after reopen")
}

// A declaration is a promise about content, so the reopened store must *enforce*
// it, not merely report it. Reading the declaration back proves the catalogue
// was parsed; this proves it was applied.
func TestSchema_ReopenedConstraintsAreEnforced(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := g.DeclareUniqueProperty("sha256"); err != nil {
		t.Fatalf("DeclareUniqueProperty: %v", err)
	}
	if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
		t.Fatalf("DeclareUniqueEdge: %v", err)
	}
	first := mustNode(t, g)
	if err := g.IndexNodeProperties(first, map[string][]byte{"sha256": []byte("9f3a")}); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	// The unique property constraint still refuses a second holder.
	second := mustNode(t, reopened)
	err = reopened.IndexNodeProperties(second, map[string][]byte{"sha256": []byte("9f3a")})
	if !errors.Is(err, store.ErrUniqueViolation) {
		t.Fatalf("after reopen a second holder got %v, want ErrUniqueViolation", err)
	}

	// The edge cardinality constraint still refuses a second edge on the pair.
	a, b := mustNode(t, reopened), mustNode(t, reopened)
	if _, err := reopened.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{edgeOwns}}); err != nil {
		t.Fatalf("first Owns edge: %v", err)
	}
	_, err = reopened.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{edgeOwns}})
	if !errors.Is(err, store.ErrUniqueViolation) {
		t.Fatalf("after reopen a duplicate Owns edge got %v, want ErrUniqueViolation", err)
	}
}

// A catalogue is a function of the store's declarations, not of the order they
// were made in, so two stores holding the same schema must produce identical
// bytes. Without that a backup diff, or a store fingerprint, reports a change
// that is not one.
func TestSchema_FileIsDeterministic(t *testing.T) {
	read := func(t *testing.T, declare func(*graphene.Graph)) []byte {
		t.Helper()
		dir := t.TempDir()
		g, err := graphene.Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		declare(g)
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(dir, schemaFile))
		if err != nil {
			t.Fatalf("read %s: %v", schemaFile, err)
		}
		return data
	}

	forward := read(t, func(g *graphene.Graph) {
		_ = g.DeclareOrderedProperty("a")
		_ = g.DeclareOrderedProperty("b")
		_ = g.DeclareUniqueProperty("k")
		_ = g.DeclareUniqueEdge(edgeOwns)
	})
	reversed := read(t, func(g *graphene.Graph) {
		_ = g.DeclareUniqueEdge(edgeOwns)
		_ = g.DeclareUniqueProperty("k")
		_ = g.DeclareOrderedProperty("b")
		_ = g.DeclareOrderedProperty("a")
	})

	if string(forward) != string(reversed) {
		t.Fatalf("declaration order changed the catalogue:\n--- forward ---\n%s\n--- reversed ---\n%s",
			forward, reversed)
	}
}

// A store that has never declared anything carries no catalogue, so an existing
// directory stays byte-identical until it acquires a reason not to be.
func TestSchema_NoDeclarationsWritesNoFile(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustNode(t, g)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, schemaFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a store with no declarations wrote %s (stat err = %v)", schemaFile, err)
	}
}

// The case the catalogue exists to catch. A store whose data violates a recorded
// constraint — because an older build wrote it, or because a process opened it
// without declaring — must refuse to open and name what broke, rather than
// opening with the constraint quietly not held.
func TestSchema_ViolatedConstraintRefusesTheOpen(t *testing.T) {
	dir := seedDuplicateUnderK(t)
	writeSchemaFile(t, dir, "!unique-node\tk")

	reopened, err := graphene.Open(dir)
	if err == nil {
		reopened.Close()
		t.Fatal("open succeeded on a store violating its own catalogue, want a refusal")
	}
	var v *store.UniqueViolationsError
	if !errors.As(err, &v) {
		t.Fatalf("open error = %v, want it to unwrap to a *store.UniqueViolationsError", err)
	}
	if len(v.Conflicts) == 0 {
		t.Fatal("the refusal named no conflicting values")
	}
}

// A store that cannot be opened cannot be repaired, so the refusal has an
// escape hatch — and a reader, which enforces nothing, takes it by default.
func TestSchema_ViolationsCanBeOpenedAndInspected(t *testing.T) {
	dir := seedDuplicateUnderK(t)
	writeSchemaFile(t, dir, "!unique-node\tk")

	g, err := graphene.OpenWithOptions(dir, disk.Options{Constraints: disk.ConstraintDrop})
	if err != nil {
		t.Fatalf("ConstraintDrop open: %v", err)
	}
	defer g.Close()

	s, ok := g.Forensics()
	if !ok {
		t.Fatal("no disk store behind the graph")
	}
	reports := s.DroppedDeclarations()
	if len(reports) != 1 || reports[0].Key != "k" {
		t.Fatalf("DroppedDeclarations() = %v, want one report naming k", reports)
	}
	// Dropped means dropped: the constraint is not in force, and the store says
	// so rather than reporting a rule it is not keeping.
	if nodeKeys, _ := g.UniqueProperties(); slices.Contains(nodeKeys, "k") {
		t.Error("a dropped constraint is still reported as declared")
	}
}

// seedDuplicateUnderK builds a store holding two nodes with the same value under
// "k" and no constraint declared — what a store written by a build that did not
// read the catalogue looks like to one that does.
func seedDuplicateUnderK(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for range 2 {
		id := mustNode(t, g)
		if err := g.IndexNodeProperties(id, map[string][]byte{"k": []byte("dup")}); err != nil {
			t.Fatalf("registration: %v", err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

// A generation this build does not know is an error rather than a file to guess
// at, matching the label table — but only where ignoring the line would lose
// something. That is the same optimisation-versus-constraint split the section
// table draws with its critical flag, and the reason the constraint kinds carry
// a "!".
func TestSchema_MalformedFileIsRefused(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"bad header", "graphene-schema v99\n", "unrecognised header"},
		{"empty file", "", "file is empty"},
		{"missing field", schemaGoodHeader + "!unique-node\n", "want a kind and at least one field"},
		{"empty key", schemaGoodHeader + "!unique-node\t\n", "empty key"},
		{"short composite", schemaGoodHeader + "composite-node\tonly\n", "at least two keys"},
		{"bad edge type", schemaGoodHeader + "!unique-edge-type\tnotanumber\n", "notanumber"},
		{"dangling escape", schemaGoodHeader + "ordered-node\tk\\\n", "dangling escape"},
		{"unknown escape", schemaGoodHeader + "ordered-node\tk\\q\n", "unknown escape"},
		// Written by a newer build that knows a constraint this one does not.
		// Opening without it would drop that constraint silently.
		{"unknown critical kind", schemaGoodHeader + "!constrain-everything\tk\n", "does not understand"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := emptyStoreDir(t)
			if err := os.WriteFile(filepath.Join(dir, schemaFile), []byte(tc.body), 0o644); err != nil {
				t.Fatalf("write %s: %v", schemaFile, err)
			}

			g, err := graphene.Open(dir)
			if err == nil {
				g.Close()
				t.Fatalf("open succeeded on a malformed catalogue (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// The other half of the criticality rule: an *optional* kind a later version
// added is skipped, because a reader ignoring it entirely still answers every
// query correctly. Without this the flag would mean nothing and every addition
// would be a format break.
func TestSchema_UnknownOptionalKindIsSkipped(t *testing.T) {
	dir := emptyStoreDir(t)
	writeSchemaFile(t, dir, "ordered-node\tscore", "some-future-index\tscore\textra")

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("an unknown optional declaration refused the open: %v", err)
	}
	defer g.Close()

	// The line it did understand still took effect.
	if nodeKeys, _ := g.OrderedProperties(); !slices.Contains(nodeKeys, "score") {
		t.Errorf("ordered node keys = %v, want score", nodeKeys)
	}
}

func emptyStoreDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

// A property key is arbitrary caller bytes, so the catalogue escapes rather than
// refuses: a key that could be declared before this file existed must still be
// declarable after it. The round trip is what proves the escaping, because an
// unescaped tab would come back as two fields and silently become a declaration
// on a key nobody named.
func TestSchema_EscapesAwkwardKeys(t *testing.T) {
	awkward := []string{"a\tb", "a\nb", "back\\slash", "trailing\\"}

	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, k := range awkward {
		if err := g.DeclareOrderedProperty(k); err != nil {
			t.Fatalf("DeclareOrderedProperty(%q): %v", k, err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, _ := reopened.OrderedProperties()
	for _, k := range awkward {
		if !slices.Contains(got, k) {
			t.Errorf("key %q did not survive the round trip; got %q", k, got)
		}
	}
}

// A read-only store applies the catalogue — an improvement in its own right,
// because a reader previously got none of the writer's declarations, so a
// declared range query silently became a scan for one process and not the other
// — and writes nothing at all.
func TestSchema_ReadOnlyAppliesButNeverWrites(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	declareAll(t, g)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, schemaFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", schemaFile, err)
	}

	ro, err := graphene.OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	assertAllDeclared(t, ro, "read-only")
	if err := ro.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-read %s: %v", schemaFile, err)
	}
	if string(before) != string(after) {
		t.Error("a read-only open rewrote the catalogue")
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Error("a read-only open left a .tmp behind")
	}
}

// Backup copies the directory rather than a list of names it knows, so a new
// sidecar travels with no change to backup. This is that claim, tested.
func TestSchema_TravelsWithABackup(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "backup")
	restored := filepath.Join(t.TempDir(), "restored")

	g, err := graphene.Open(src)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	declareAll(t, g)
	if _, err := g.Backup(dst); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := graphene.Restore(dst, restored, disk.RestoreOptions{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	reopened, err := graphene.Open(restored)
	if err != nil {
		t.Fatalf("open the restored store: %v", err)
	}
	defer reopened.Close()
	assertAllDeclared(t, reopened, "after backup and restore")
}

// OpenWithOptions never loaded the label table, so a store opened with
// disk.StrictOptions rendered every custom label as a number. Every open path
// now loads both sidecars; this pins the one that was missing.
func TestSchema_OpenWithOptionsLoadsBothSidecars(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := g.DeclareUniqueProperty("sha256"); err != nil {
		t.Fatalf("DeclareUniqueProperty: %v", err)
	}
	if err := g.DeclareTypeNames(nil, map[store.EdgeType]string{edgeOwns: "Owns"}); err != nil {
		t.Fatalf("DeclareTypeNames: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.OpenWithOptions(dir, disk.Options{})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer reopened.Close()

	nodeKeys, _ := reopened.UniqueProperties()
	if !slices.Contains(nodeKeys, "sha256") {
		t.Errorf("OpenWithOptions did not load the catalogue: unique node keys = %v", nodeKeys)
	}
	if got := edgeOwns.String(); got != "Owns" {
		t.Errorf("OpenWithOptions did not load the label table: edgeOwns renders as %q, want \"Owns\"", got)
	}
}
