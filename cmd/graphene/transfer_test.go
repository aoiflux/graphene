package main

// The compression half of the transfer surface.
//
// `export -gzip` is only worth having if something reads it back, so the tests
// here are round trips rather than assertions about a flag. The interesting one
// is the third: import sniffs the gzip magic number rather than taking a flag,
// so a compressed dump under a name that does not admit it still imports — which
// is what lets the export write the path it was given instead of renaming it.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// gzipped reports whether a file starts with the gzip magic number, which is
// the same two bytes openDump looks for.
func gzipped(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var magic [2]byte
	if _, err := f.Read(magic[:]); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return magic[0] == 0x1f && magic[1] == 0x8b
}

// TestGzipRoundTrip covers both stream formats. csv is not here because it is
// not a stream — see TestGzipIsRefusedForCSV.
func TestGzipRoundTrip(t *testing.T) {
	src := fixture(t)
	want := counts(t, src)

	for _, format := range []string{"jsonl", "dump"} {
		t.Run(format, func(t *testing.T) {
			dump := filepath.Join(t.TempDir(), "graph."+format+".gz")
			dst := filepath.Join(t.TempDir(), "restored")

			out, errb, code := exec(t, "export", "graph", "-format", format, "-gzip", "-to", dump, src)
			if code != 0 {
				t.Fatalf("export exited %d\n%s\n%s", code, out, errb)
			}
			if !strings.Contains(out, "gzip") {
				t.Errorf("the report does not say the output is compressed:\n%s", out)
			}
			if !gzipped(t, dump) {
				t.Fatal("the export is not gzipped")
			}

			out, errb, code = exec(t, "import", "graph", "-format", format, "-from", dump, dst)
			if code != 0 {
				t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
			}
			if !strings.Contains(out, "gzip") {
				t.Errorf("the import does not report that it decompressed:\n%s", out)
			}
			if got := counts(t, dst); got != want {
				t.Errorf("round trip through gzip holds %v, the original %v", got, want)
			}
		})
	}
}

// TestGzipIsSmallerThanPlain. Without this the round trip would still pass if
// -gzip wrote an uncompressed stream through a writer that happened to have a
// gzip header — the magic check alone does not prove the body was compressed.
func TestGzipIsSmallerThanPlain(t *testing.T) {
	src := fixture(t)
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain.jsonl")
	zipped := filepath.Join(dir, "zipped.jsonl.gz")

	if _, _, code := exec(t, "export", "graph", "-to", plain, src); code != 0 {
		t.Fatalf("plain export exited %d", code)
	}
	if _, _, code := exec(t, "export", "graph", "-gzip", "-to", zipped, src); code != 0 {
		t.Fatalf("gzip export exited %d", code)
	}

	p, err := os.Stat(plain)
	if err != nil {
		t.Fatal(err)
	}
	z, err := os.Stat(zipped)
	if err != nil {
		t.Fatal(err)
	}
	if z.Size() >= p.Size() {
		t.Errorf("the gzipped export is %d bytes and the plain one %d", z.Size(), p.Size())
	}
}

// TestImportSniffsGzipUnderAnyName. The export writes the path it was told to,
// so a compressed dump can be called anything. Import must not care.
func TestImportSniffsGzipUnderAnyName(t *testing.T) {
	src := fixture(t)
	want := counts(t, src)
	dump := filepath.Join(t.TempDir(), "not-obviously-compressed.jsonl")
	dst := filepath.Join(t.TempDir(), "restored")

	// The warning is a notice, so it is on stderr with the other asides — stdout
	// stays the report, and stays valid JSON under -json.
	_, errb, code := exec(t, "export", "graph", "-gzip", "-to", dump, src)
	if code != 0 {
		t.Fatalf("export exited %d", code)
	}
	if !strings.Contains(errb, "does not say so") {
		t.Errorf("the export did not warn that the name hides the compression:\n%s", errb)
	}

	if _, errb, code := exec(t, "import", "graph", "-from", dump, dst); code != 0 {
		t.Fatalf("import of a gzipped dump under a plain name exited %d\n%s", code, errb)
	}
	if got := counts(t, dst); got != want {
		t.Errorf("the sniffed import holds %v, the original %v", got, want)
	}
}

// TestPlainImportStillWorks. The sniff must not have made the uncompressed path
// conditional on anything: every dump written before -gzip existed is plain.
func TestPlainImportStillWorks(t *testing.T) {
	src := fixture(t)
	want := counts(t, src)
	dump := filepath.Join(t.TempDir(), "plain.jsonl")
	dst := filepath.Join(t.TempDir(), "restored")

	if _, _, code := exec(t, "export", "graph", "-to", dump, src); code != 0 {
		t.Fatalf("export exited %d", code)
	}
	if gzipped(t, dump) {
		t.Fatal("an export without -gzip is compressed")
	}
	if _, errb, code := exec(t, "import", "graph", "-from", dump, dst); code != 0 {
		t.Fatalf("import exited %d\n%s", code, errb)
	}
	if got := counts(t, dst); got != want {
		t.Errorf("the plain round trip holds %v, the original %v", got, want)
	}
}

// TestGzipIsRefusedForCSV, and refused before anything is written: csv writes a
// directory, and a half-created one is the state the refusal exists to avoid.
func TestGzipIsRefusedForCSV(t *testing.T) {
	src := fixture(t)
	to := filepath.Join(t.TempDir(), "tables")

	_, errb, code := exec(t, "export", "graph", "-format", "csv", "-gzip", "-to", to, src)
	if code != 2 {
		t.Fatalf("csv with -gzip exited %d, want 2", code)
	}
	if !strings.Contains(errb, "csv") {
		t.Errorf("the refusal does not name the format:\n%s", errb)
	}
	if _, err := os.Stat(to); err == nil {
		t.Error("the refused export created its output directory anyway")
	}
}

// TestGzipSubgraphExport. `export subgraph` writes through the same function,
// and this is what keeps that true.
func TestGzipSubgraphExport(t *testing.T) {
	src := fixture(t)
	dump := filepath.Join(t.TempDir(), "region.jsonl.gz")
	dst := filepath.Join(t.TempDir(), "restored")

	if _, errb, code := exec(t, "export", "subgraph", "-id", "1,3,4", "-gzip", "-to", dump, src); code != 0 {
		t.Fatalf("export subgraph exited %d\n%s", code, errb)
	}
	if !gzipped(t, dump) {
		t.Fatal("the subgraph export is not gzipped")
	}
	if _, errb, code := exec(t, "import", "graph", "-from", dump, dst); code != 0 {
		t.Fatalf("import exited %d\n%s", code, errb)
	}
	if n := counts(t, dst)[0]; n != 3 {
		t.Errorf("the scoped export round-tripped %d nodes, want 3", n)
	}
}

// `export graph` reads through a snapshot rather than the live store, so the
// dump is of one graph and bulk streams the enumeration instead of materialising
// every ID first. A view cannot declare an index, only report what the store
// declared — so the thing to check is that the declarations still reach the
// dump, because losing them is silent: the import succeeds and the restored
// store simply answers range and composite queries the slow way.
func TestExportGraphCarriesDeclarations(t *testing.T) {
	src := t.TempDir()
	g, err := graphene.Open(src)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	if err := g.IndexNodeProperties(id, map[string][]byte{
		"sha256": []byte("aa11"),
		"case":   []byte("c1"),
		"bucket": []byte("b1"),
	}); err != nil {
		t.Fatalf("index: %v", err)
	}
	if err := g.DeclareOrderedProperty("sha256"); err != nil {
		t.Fatalf("declare ordered: %v", err)
	}
	if err := g.DeclareCompositeProperties([]string{"case", "bucket"}); err != nil {
		t.Fatalf("declare composite: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dump := filepath.Join(t.TempDir(), "graph.jsonl")
	dst := filepath.Join(t.TempDir(), "restored")
	if out, errb, code := exec(t, "export", "graph", "-to", dump, src); code != 0 {
		t.Fatalf("export exited %d\n%s\n%s", code, out, errb)
	}
	if out, errb, code := exec(t, "import", "graph", "-from", dump, dst); code != 0 {
		t.Fatalf("import exited %d\n%s\n%s", code, out, errb)
	}

	back, err := graphene.Open(dst)
	if err != nil {
		t.Fatalf("open the restored store: %v", err)
	}
	defer back.Close()

	ord, _ := back.OrderedProperties()
	if !slices.Contains(ord, "sha256") {
		t.Errorf("the ordered declaration did not survive the export: %v", ord)
	}
	comp, _ := back.CompositeProperties()
	found := false
	for _, keys := range comp {
		if slices.Equal(keys, []string{"case", "bucket"}) {
			found = true
		}
	}
	if !found {
		t.Errorf("the composite declaration did not survive the export: %v", comp)
	}
}
