package graphene_test

// Naming the custom label range, and persisting the naming.
//
// The registry is process-wide, so these tests share one and must not fight over
// it. Each uses its own slice of the custom range — the offsets below are
// reserved for one test each — and none registers the same offset twice with a
// different name. That is a real constraint on the feature, not an artefact of
// the tests: one process has one naming, deliberately.

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Offsets reserved per test, so the shared registry never sees a conflict it was
// not asked to produce.
const (
	offRender     = 100 // TestTypeNames_RenderAndParse
	offPersist    = 110 // TestTypeNames_PersistAndReload
	offConflict   = 120 // TestTypeNames_ConflictIsRefused
	offValidation = 130 // TestTypeNames_RejectsAmbiguousNames
	offMemory     = 140 // TestTypeNames_InMemoryRegistersWithoutPersisting
)

// TestTypeNames_RenderAndParse is the ergonomic half: a named type prints as its
// name, and every form that parsed before still parses.
func TestTypeNames_RenderAndParse(t *testing.T) {
	nt := store.CustomNodeType(offRender)
	et := store.CustomEdgeType(offRender)

	// The registry is process-wide and deliberately has no reset, so under
	// -count=N this test finds its own naming from the previous iteration still
	// in place. Both states are correct: unnamed on the first pass, already
	// AppVersion on every one after. Anything else means another test reached
	// into this test's reserved offset, which is what this guard is for.
	switch got := nt.String(); got {
	case "Custom(" + strconv.Itoa(offRender) + ")", "AppVersion":
	default:
		t.Fatalf("before naming, %d renders as %q — something else has named it",
			offRender, got)
	}
	if err := store.RegisterNodeTypeName(nt, "AppVersion"); err != nil {
		t.Fatalf("RegisterNodeTypeName: %v", err)
	}
	if err := store.RegisterEdgeTypeName(et, "Embeds"); err != nil {
		t.Fatalf("RegisterEdgeTypeName: %v", err)
	}

	if got := nt.String(); got != "AppVersion" {
		t.Errorf("NodeType.String() = %q, want AppVersion", got)
	}
	if got := et.String(); got != "Embeds" {
		t.Errorf("EdgeType.String() = %q, want Embeds", got)
	}

	// The name parses, and so does everything that parsed before it existed.
	// "in addition to, never instead of" is the whole compatibility claim.
	for _, sel := range []string{"AppVersion", "appversion", "app_version",
		"custom:" + strconv.Itoa(offRender), "Custom(" + strconv.Itoa(offRender) + ")",
		strconv.Itoa(int(store.NodeTypeCustomBase) + offRender)} {
		got, err := store.ParseNodeType(sel)
		if err != nil {
			t.Errorf("ParseNodeType(%q): %v", sel, err)
			continue
		}
		if got != nt {
			t.Errorf("ParseNodeType(%q) = %d, want %d", sel, got, nt)
		}
	}
	for _, sel := range []string{"Embeds", "embeds", "custom:" + strconv.Itoa(offRender)} {
		got, err := store.ParseEdgeType(sel)
		if err != nil || got != et {
			t.Errorf("ParseEdgeType(%q) = (%d, %v), want (%d, nil)", sel, got, err, et)
		}
	}

	// Registering the identical name again is a no-op, which is what makes this
	// safe to call at every Open.
	if err := store.RegisterNodeTypeName(nt, "AppVersion"); err != nil {
		t.Errorf("re-registering the same name: %v", err)
	}

	// Built-ins are untouched, and cannot be renamed.
	if got := store.NodeTypeCase.String(); got != "Case" {
		t.Errorf("a built-in was affected: NodeTypeCase renders as %q", got)
	}
	if err := store.RegisterNodeTypeName(store.NodeTypeCase, "Investigation"); err == nil {
		t.Error("renaming a built-in was allowed")
	}
}

// TestTypeNames_PersistAndReload is the half that matters more: the store stays
// interpretable without the program that wrote it.
func TestTypeNames_PersistAndReload(t *testing.T) {
	dir := t.TempDir()
	nt := store.CustomNodeType(offPersist)
	et := store.CustomEdgeType(offPersist)

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := g.DeclareTypeNames(
		map[store.NodeType]string{nt: "Permission"},
		map[store.EdgeType]string{et: "Requests"},
	); err != nil {
		t.Fatalf("DeclareTypeNames: %v", err)
	}
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{nt}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The file is beside the image, is human-readable, and does not claim to be
	// part of the format.
	raw, err := os.ReadFile(filepath.Join(dir, "graphene.labels"))
	if err != nil {
		t.Fatalf("the label table was not written: %v", err)
	}
	text := string(raw)
	if !strings.HasPrefix(text, "graphene-labels v1\n") {
		t.Errorf("label table has no recognisable header:\n%s", text)
	}
	for _, want := range []string{"node\t" + strconv.Itoa(int(nt)) + "\tPermission",
		"edge\t" + strconv.Itoa(int(et)) + "\tRequests"} {
		if !strings.Contains(text, want) {
			t.Errorf("label table missing %q:\n%s", want, text)
		}
	}

	// Reopening registers it. In this process it is already registered, so what
	// this actually proves is that reading it back agrees rather than
	// conflicting — which is the same check a fresh process would make, and the
	// one that fails if the file round-trips wrong.
	g2, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopening a store with a label table: %v", err)
	}
	defer g2.Close()

	nodes, edges := g2.TypeNames()
	if nodes[nt] != "Permission" || edges[et] != "Requests" {
		t.Errorf("after reopen: nodes[%d]=%q edges[%d]=%q, want Permission/Requests",
			nt, nodes[nt], et, edges[et])
	}
	if got := nt.String(); got != "Permission" {
		t.Errorf("after reopen the type renders as %q", got)
	}

	// Declaring the same table again is a no-op and leaves the file byte-identical.
	if err := g2.DeclareTypeNames(map[store.NodeType]string{nt: "Permission"}, nil); err != nil {
		t.Fatalf("re-declaring: %v", err)
	}
	again, err := os.ReadFile(filepath.Join(dir, "graphene.labels"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(again) != text {
		t.Errorf("re-declaring the same table rewrote it differently:\n%s\nvs\n%s", text, again)
	}
}

// TestTypeNames_ConflictIsRefused pins the decision that separates this from a
// convenience. Two namings of one number mean something upstream is wrong, and
// choosing between them silently is how a forensic tool reports the wrong thing
// with total confidence.
func TestTypeNames_ConflictIsRefused(t *testing.T) {
	nt := store.CustomNodeType(offConflict)
	other := store.CustomNodeType(offConflict + 1)

	if err := store.RegisterNodeTypeName(nt, "Tracker"); err != nil {
		t.Fatalf("RegisterNodeTypeName: %v", err)
	}

	// A different name for a type that has one.
	err := store.RegisterNodeTypeName(nt, "Beacon")
	if !errors.Is(err, store.ErrTypeNameConflict) {
		t.Errorf("renaming a named type: want ErrTypeNameConflict, got %v", err)
	}
	// The same name for a different type.
	err = store.RegisterNodeTypeName(other, "Tracker")
	if !errors.Is(err, store.ErrTypeNameConflict) {
		t.Errorf("reusing a taken name: want ErrTypeNameConflict, got %v", err)
	}
	// And nothing changed.
	if got := nt.String(); got != "Tracker" {
		t.Errorf("a refused registration took effect: %q", got)
	}
	if got := other.String(); got != "Custom("+strconv.Itoa(offConflict+1)+")" {
		t.Errorf("a refused registration named the wrong type: %q", got)
	}

	// A store whose table disagrees must refuse to open rather than render the
	// other one's names. Hand-written, because producing it through the API is
	// exactly what the checks above prevent.
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	table := "graphene-labels v1\nnode\t" + strconv.Itoa(int(nt)) + "\tBeacon\n"
	if err := os.WriteFile(filepath.Join(dir, "graphene.labels"), []byte(table), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := graphene.Open(dir); !errors.Is(err, store.ErrTypeNameConflict) {
		t.Fatalf("opening a store whose table disagrees: want ErrTypeNameConflict, got %v", err)
	}

	// An unreadable table is refused too, rather than silently ignored: a
	// mislabelling is worse than a failure to open.
	if err := os.WriteFile(filepath.Join(dir, "graphene.labels"),
		[]byte("graphene-labels v99\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := graphene.Open(dir); err == nil {
		t.Fatal("a label table from an unknown generation was accepted")
	}
}

// TestTypeNames_RejectsAmbiguousNames covers the property the whole mechanism
// rests on: a type must parse back to what it prints as.
func TestTypeNames_RejectsAmbiguousNames(t *testing.T) {
	nt := store.CustomNodeType(offValidation)
	for _, name := range []string{
		"",              // nothing to render
		"  ",            // nothing to render, less obviously
		" Padded",       // would not survive a round trip through a selector
		"case",          // a built-in name, which already means something else
		"MicroArtefact", // the same, in the form String() would print
		"42",            // parses as NodeType(42)
		"custom:7",      // parses as a different custom type
		"Custom(7)",     // the form String() prints for another type
		"has\ttab",      // would break the label table's own encoding
	} {
		if err := store.RegisterNodeTypeName(nt, name); err == nil {
			t.Errorf("name %q was accepted; it does not round-trip", name)
		}
	}
	// And having rejected all of them, the type is still unnamed.
	if got := nt.String(); got != "Custom("+strconv.Itoa(offValidation)+")" {
		t.Errorf("a rejected name took effect: %q", got)
	}
}

// TestTypeNames_InMemoryRegistersWithoutPersisting: there is nowhere to write,
// and that is not an error.
func TestTypeNames_InMemoryRegistersWithoutPersisting(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()

	nt := store.CustomNodeType(offMemory)
	if err := g.DeclareTypeNames(map[store.NodeType]string{nt: "Library"}, nil); err != nil {
		t.Fatalf("DeclareTypeNames on the in-memory backend: %v", err)
	}
	if got := nt.String(); got != "Library" {
		t.Errorf("NodeType.String() = %q, want Library", got)
	}
}
