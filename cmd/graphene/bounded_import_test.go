package main

// `import graph` is bounded by default, and what that costs and buys.
//
// The bound exists because an unbounded import holds the whole dump -- records
// in the delta, entries in the property index -- until something folds it into
// an image. docs/MEMORY_MODEL.md section 9.8 measured that shape dying under a
// 2 GiB ceiling at 1,200,000 records, and it died at the library's own default
// delta bound, so "the default is fine" was the defect.
//
// The ceiling harness in tests/ is what measures the memory. What is pinned here
// is the wiring the harness cannot see from outside: that the schedule fires
// during the import rather than after it, that the reopen it triggers leaves the
// command holding the handle it finished with rather than one it closed, and
// that none of it changes the store that comes out.

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// fatFixture is a store whose records are large enough that a small -bound is
// reached several times over. The blob is what makes it fat: five nodes of no
// properties, which is what fixture builds, would never reach any bound worth
// setting.
func fatFixture(t *testing.T, nodes, size int) string {
	t.Helper()
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	batch := make([]*store.Node, nodes)
	for i := range batch {
		batch[i] = &store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("%-*d", size, i)),
		}
	}
	if _, err := g.AddNodes(batch); err != nil {
		t.Fatalf("add nodes: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// dumpOf exports a store and returns the path of the dump.
func dumpOf(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "graph.dump")
	out, errb, code := exec(t, "export", "graph", "-format", "dump", "-to", path, src)
	if code != 0 {
		t.Fatalf("export exited %d\n%s\n%s", code, out, errb)
	}
	return path
}

// The schedule fires during the import, and the import survives it.
//
// Surviving it is the whole of the risk: the reopen closes the handle the
// framework opened, so a command that kept its own pointer would write the rest
// of the dump to a closed store, and the framework's closer would close a handle
// already closed and leak the one in use.
func TestImportGraph_BoundCompactsDuringTheImport(t *testing.T) {
	dump := dumpOf(t, fatFixture(t, 400, 4<<10))
	dst := filepath.Join(t.TempDir(), "bounded")

	out, errb, code := exec(t, "import", "graph", "-format", "dump",
		"-from", dump, "-bound", "1", "-batch", "16", dst)
	if code != 0 {
		t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
	}
	if !strings.Contains(out, "interim compactions") {
		t.Fatalf("a 1 MiB bound over a ~1.6 MiB dump compacted nothing during the import:\n%s", out)
	}
	if got, want := counts(t, dst), [2]uint64{400, 0}; got != want {
		t.Fatalf("the bounded import holds %v, want %v", got, want)
	}
}

// -bound 0 is the behaviour the command had before the bound existed, and stays
// available for whoever measured that it is faster on a dump that fits.
func TestImportGraph_BoundZeroCompactsNothingDuringTheImport(t *testing.T) {
	dump := dumpOf(t, fatFixture(t, 400, 4<<10))
	dst := filepath.Join(t.TempDir(), "unbounded")

	out, errb, code := exec(t, "import", "graph", "-format", "dump",
		"-from", dump, "-bound", "0", dst)
	if code != 0 {
		t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
	}
	if strings.Contains(out, "interim compactions") {
		t.Fatalf("-bound 0 compacted during the import:\n%s", out)
	}
	if got, want := counts(t, dst), [2]uint64{400, 0}; got != want {
		t.Fatalf("the unbounded import holds %v, want %v", got, want)
	}
}

// Neither bound changes what is imported. A memory bound whose setting is
// visible in the result would not be a bound, it would be a mode.
func TestImportGraph_BoundsDoNotChangeTheStore(t *testing.T) {
	src := fixture(t)
	want := counts(t, src)
	dump := dumpOf(t, src)

	for _, argv := range [][]string{
		{"-bound", "0"},
		{"-bound", "1"},
		{"-bound", "1", "-batch", "1"},
		{"-batch-bytes", "0"},
		{"-batch-bytes", "64", "-bound", "1"},
	} {
		t.Run(strings.Join(argv, " "), func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "restored")
			args := append([]string{"import", "graph", "-format", "dump", "-from", dump}, argv...)
			out, errb, code := exec(t, append(args, dst)...)
			if code != 0 {
				t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
			}
			if got := counts(t, dst); got != want {
				t.Fatalf("import under %v holds %v, the original %v", argv, got, want)
			}
		})
	}
}
