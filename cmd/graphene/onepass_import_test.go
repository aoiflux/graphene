package main

// `import graph` builds the image in one pass, and says which path it took.
//
// bounded_import_test.go covers the incremental path and the bound that keeps it
// inside a ceiling. This covers the path that does not need a bound at all: a
// dump whose records carry their own index entries goes straight into the image,
// so what an import writes stops growing with the square of what it is
// importing.
//
// Two things are worth asserting and only one of them is the result. The store
// has to be right, which the byte-identity test in the root package establishes
// against the incremental importer; what is left here is that the command
// actually took the fast path when it could and actually fell back when it could
// not -- a routing bug in either direction is silent, and a fallback that always
// fired would leave every test above still passing.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// indexedFixture is a store whose records carry index entries, which is what a
// one-pass import needs in the dump and what fatFixture deliberately omits.
func indexedFixture(t *testing.T, nodes int) string {
	t.Helper()
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := g.DeclareOrderedProperty("seq"); err != nil {
		t.Fatalf("declare: %v", err)
	}
	ids := make([]store.NodeID, 0, nodes)
	for i := 1; i <= nodes; i++ {
		id, err := g.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(strings.Repeat("x", 64)),
		})
		if err != nil {
			t.Fatalf("add node: %v", err)
		}
		ids = append(ids, id)
		if err := g.IndexNodeProperty(id, "seq", []byte(seqValue(i))); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
	for i := 1; i < len(ids); i++ {
		if _, err := g.AddEdge(&store.Edge{
			Src: ids[i-1], Dst: ids[i],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		}); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// seqValue is the value a record is indexed under, zero-padded so that byte
// order matches numeric order -- which is what an ordered index over the key
// requires, and what index/encoding exists to make easy.
func seqValue(i int) string { return fmt.Sprintf("%08d", i) }

// A dump with inline entries imports in one pass, and the output says so.
func TestImportGraph_TakesTheOnePassPath(t *testing.T) {
	dump := dumpOf(t, indexedFixture(t, 200))
	into := t.TempDir() + "/into"

	out, errb, code := exec(t, "import", "graph", "-format", "dump", "-from", dump, into)
	if code != 0 {
		t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(out, "one-pass") {
		t.Fatalf("the import did not report the one-pass mode:\n%s", out)
	}
	if !strings.Contains(out, "200") {
		t.Fatalf("the import reports no node count:\n%s", out)
	}

	// The result is a store, not just a successful exit.
	g, err := graphene.Open(into)
	if err != nil {
		t.Fatalf("open the imported store: %v", err)
	}
	defer g.Close()
	ids, err := g.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(ids) != 200 {
		t.Fatalf("the imported store holds %d nodes, want 200", len(ids))
	}
	found, err := g.NodesByProperty("seq", []byte(seqValue(7)))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("the property index answers %v for a key one record carries", found)
	}
}

// Turning it off takes the incremental path, on the same dump.
//
// The flag is what makes the fallback testable at all: without it there is no
// way to ask for the old path over a dump that qualifies for the new one, and a
// routing change nobody can turn off is a change nobody can bisect.
func TestImportGraph_TheOnePassPathCanBeTurnedOff(t *testing.T) {
	dump := dumpOf(t, indexedFixture(t, 50))
	into := t.TempDir() + "/into"

	out, errb, code := exec(t, "import", "graph", "-format", "dump",
		"-one-pass=false", "-from", dump, into)
	if code != 0 {
		t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
	}
	if strings.Contains(out, "one-pass") {
		t.Fatalf("-one-pass=false still took the one-pass path:\n%s", out)
	}

	g, err := graphene.Open(into)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer g.Close()
	ids, err := g.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(ids) != 50 {
		t.Fatalf("the incremental import holds %d nodes, want 50", len(ids))
	}
}

// A JSONL dump is imported as it always was.
//
// The one-pass path is dump-only, because the entries have nowhere to live
// inline in a line-oriented format. This is the assertion that the routing does
// not reach it.
func TestImportGraph_JSONLIsUnaffected(t *testing.T) {
	src := indexedFixture(t, 30)
	path := t.TempDir() + "/graph.jsonl"
	if out, errb, code := exec(t, "export", "graph", "-format", "jsonl", "-to", path, src); code != 0 {
		t.Fatalf("export exited %d\n%s\n%s", code, out, errb)
	}
	into := t.TempDir() + "/into"
	out, errb, code := exec(t, "import", "graph", "-format", "jsonl", "-from", path, into)
	if code != 0 {
		t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
	}
	if strings.Contains(out, "one-pass") {
		t.Fatalf("a jsonl import reported the one-pass mode:\n%s", out)
	}
}

// Naming an incremental flag takes the incremental path, without being asked.
//
// -bound is how an operator holds a ceiling. A run that quietly took a different
// path would look like the bound had been honoured, so the flag decides the
// route rather than being ignored by it.
func TestImportGraph_AnIncrementalFlagTakesTheIncrementalPath(t *testing.T) {
	dump := dumpOf(t, indexedFixture(t, 60))
	into := t.TempDir() + "/into"

	out, errb, code := exec(t, "import", "graph", "-format", "dump",
		"-bound", "1", "-from", dump, into)
	if code != 0 {
		t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
	}
	if strings.Contains(out, "one-pass") {
		t.Fatalf("-bound was given and the one-pass path ran anyway:\n%s", out)
	}
}

// Asking for both is a contradiction and is refused, not resolved.
//
// One of the two has to lose, and picking a winner silently is how a caller
// ends up believing a figure they typed is in force. The message names the flag
// that clashed and both ways out of it.
func TestImportGraph_OnePassAndABoundTogetherAreRefused(t *testing.T) {
	dump := dumpOf(t, indexedFixture(t, 10))
	into := t.TempDir() + "/into"

	out, errb, code := exec(t, "import", "graph", "-format", "dump",
		"-one-pass=true", "-bound", "1", "-from", dump, into)
	if code == 0 {
		t.Fatalf("-one-pass with -bound was accepted:\n%s", out)
	}
	if !strings.Contains(errb, "bound") {
		t.Fatalf("the refusal does not name the flag that clashed:\n%s", errb)
	}
}
