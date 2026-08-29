package bulk_test

// Round-trip, and the ways a dump can be wrong.
//
// The interesting assertion is not "it did not error". It is that the graph
// that comes back is the graph that went in — and since IDs are reassigned on
// import, "the same graph" has to be checked up to isomorphism rather than by
// comparing IDs. Every fixture node therefore carries a unique blob, which is
// what lets the comparison below key on identity without depending on the one
// thing an import is documented not to preserve.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/memory"
	"github.com/aoiflux/graphene/store"
)

// storeUnderTest is everything the tests need of a backend: both halves of the
// bulk contract plus the declaration and query surfaces the comparison uses.
type storeUnderTest interface {
	bulk.Source
	bulk.Dest
	store.OrderedIndexDeclarer
	store.CompositeIndexDeclarer
}

func backends(t *testing.T, fn func(t *testing.T, mk func(t *testing.T) storeUnderTest)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		fn(t, func(t *testing.T) storeUnderTest { return memory.New() })
	})
	t.Run("disk", func(t *testing.T) {
		fn(t, func(t *testing.T) storeUnderTest {
			s, err := disk.OpenWithOptions(t.TempDir(), disk.Options{})
			if err != nil {
				t.Fatalf("disk.Open: %v", err)
			}
			t.Cleanup(func() { s.Close() })
			return s
		})
	})
}

// fixture builds a graph with everything a dump has to carry: several labels on
// one record, an empty property blob beside a non-empty one, a non-zero weight,
// indexed entries on both nodes and edges, and both kinds of declaration.
func fixture(t *testing.T, s storeUnderTest) {
	t.Helper()
	if err := s.DeclareOrderedNodeProperty("seq"); err != nil {
		t.Fatalf("DeclareOrderedNodeProperty: %v", err)
	}
	if err := s.DeclareCompositeNodeProperties([]string{"bucket", "seq"}); err != nil {
		t.Fatalf("DeclareCompositeNodeProperties: %v", err)
	}

	ids := make([]store.NodeID, 6)
	for i := range ids {
		n := &store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("node-%d", i)),
		}
		if i%2 == 0 {
			n.Labels = append(n.Labels, store.NodeTypeCase)
		}
		if i == 3 {
			// One record with no blob at all: nil and empty must not be
			// confused, which is what emptyToNil is for.
			n.Properties = nil
		}
		id, err := s.AddNode(n)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids[i] = id
		if err := s.IndexNodeProperty(id, "seq", []byte(fmt.Sprintf("%03d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		if err := s.IndexNodeProperty(id, "bucket", []byte{byte(i % 2)}); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}

	for i := 0; i < len(ids)-1; i++ {
		e := &store.Edge{
			Src:        ids[i],
			Dst:        ids[i+1],
			Labels:     []store.EdgeType{store.EdgeTypeContains},
			Properties: []byte(fmt.Sprintf("edge-%d", i)),
		}
		if i == 1 {
			e.Labels = append(e.Labels, store.EdgeTypeSimilarTo)
			e.Weight = 0.37519
		}
		id, err := s.AddEdge(e)
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		if err := s.IndexEdgeProperty(id, "kind", []byte("link")); err != nil {
			t.Fatalf("IndexEdgeProperty: %v", err)
		}
	}
}

// shape is a graph described without reference to any ID.
type shape struct {
	nodes  []string
	edges  []string
	nprops []string
	eprops []string
}

func describe(t *testing.T, s storeUnderTest) shape {
	t.Helper()
	var sh shape

	nodeIDs, err := s.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	// key is a node's identity for comparison purposes: its blob, which the
	// fixture makes unique, plus a marker for the one node that has none.
	key := make(map[store.NodeID]string, len(nodeIDs))
	for _, id := range nodeIDs {
		n, err := s.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		k := blobKey(n.Properties)
		key[id] = k
		sh.nodes = append(sh.nodes, fmt.Sprintf("%s|%v", k, n.Labels))
	}

	edgeIDs, err := s.QueryEdgeIDs(store.EdgeQuery{})
	if err != nil {
		t.Fatalf("QueryEdgeIDs: %v", err)
	}
	ekey := make(map[store.EdgeID]string, len(edgeIDs))
	for _, id := range edgeIDs {
		e, err := s.GetEdge(id)
		if err != nil {
			t.Fatalf("GetEdge: %v", err)
		}
		k := blobKey(e.Properties)
		ekey[id] = k
		sh.edges = append(sh.edges, fmt.Sprintf("%s|%s->%s|%v|%g",
			k, key[e.Src], key[e.Dst], e.Labels, e.Weight))
	}

	pe, ok := s.(store.PropertyEnumerator)
	if !ok {
		t.Fatal("backend does not enumerate property entries")
	}
	pe.ForEachNodeProperty(func(id store.NodeID, k string, v []byte) bool {
		sh.nprops = append(sh.nprops, fmt.Sprintf("%s|%s|%x", key[id], k, v))
		return true
	})
	pe.ForEachEdgeProperty(func(id store.EdgeID, k string, v []byte) bool {
		sh.eprops = append(sh.eprops, fmt.Sprintf("%s|%s|%x", ekey[id], k, v))
		return true
	})

	for _, xs := range [][]string{sh.nodes, sh.edges, sh.nprops, sh.eprops} {
		sort.Strings(xs)
	}
	return sh
}

// blobKey distinguishes a nil blob from an empty one, which base64 does not do
// on its own and which a round trip must preserve.
func blobKey(b []byte) string {
	if b == nil {
		return "<nil>"
	}
	return string(b)
}

func compare(t *testing.T, want, got shape) {
	t.Helper()
	for _, c := range []struct {
		what       string
		want, have []string
	}{
		{"nodes", want.nodes, got.nodes},
		{"edges", want.edges, got.edges},
		{"node properties", want.nprops, got.nprops},
		{"edge properties", want.eprops, got.eprops},
	} {
		if len(c.want) != len(c.have) {
			t.Errorf("%s: %d before, %d after\n before: %v\n  after: %v",
				c.what, len(c.want), len(c.have), c.want, c.have)
			continue
		}
		for i := range c.want {
			if c.want[i] != c.have[i] {
				t.Errorf("%s differ at %d:\n before: %s\n  after: %s", c.what, i, c.want[i], c.have[i])
			}
		}
	}
}

// The three formats, both backends, one assertion: what comes back is what went
// in, up to the IDs an import is documented not to preserve.
func TestRoundTrip(t *testing.T) {
	formats := []struct {
		name   string
		export func(t *testing.T, dir string, src storeUnderTest) (bulk.Summary, error)
		imprt  func(t *testing.T, dir string, dst storeUnderTest) (bulk.Summary, error)
	}{
		{
			name: "jsonl",
			export: func(t *testing.T, dir string, src storeUnderTest) (bulk.Summary, error) {
				f, err := os.Create(filepath.Join(dir, "dump.jsonl"))
				if err != nil {
					t.Fatalf("create: %v", err)
				}
				defer f.Close()
				return bulk.ExportJSONL(f, src, bulk.Options{})
			},
			imprt: func(t *testing.T, dir string, dst storeUnderTest) (bulk.Summary, error) {
				f, err := os.Open(filepath.Join(dir, "dump.jsonl"))
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				defer f.Close()
				return bulk.ImportJSONL(f, dst, bulk.Options{})
			},
		},
		{
			name: "dump",
			export: func(t *testing.T, dir string, src storeUnderTest) (bulk.Summary, error) {
				f, err := os.Create(filepath.Join(dir, "graph.gdmp"))
				if err != nil {
					t.Fatalf("create: %v", err)
				}
				defer f.Close()
				return bulk.ExportDump(f, src, bulk.Options{})
			},
			imprt: func(t *testing.T, dir string, dst storeUnderTest) (bulk.Summary, error) {
				f, err := os.Open(filepath.Join(dir, "graph.gdmp"))
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				defer f.Close()
				return bulk.ImportDump(f, dst, bulk.Options{})
			},
		},
		{
			name: "csv",
			export: func(t *testing.T, dir string, src storeUnderTest) (bulk.Summary, error) {
				return bulk.ExportCSV(filepath.Join(dir, "csv"), src, bulk.Options{})
			},
			imprt: func(t *testing.T, dir string, dst storeUnderTest) (bulk.Summary, error) {
				return bulk.ImportCSV(filepath.Join(dir, "csv"), dst, bulk.Options{})
			},
		},
	}

	for _, f := range formats {
		t.Run(f.name, func(t *testing.T) {
			backends(t, func(t *testing.T, mk func(t *testing.T) storeUnderTest) {
				src := mk(t)
				fixture(t, src)
				want := describe(t, src)

				dir := t.TempDir()
				out, err := f.export(t, dir, src)
				if err != nil {
					t.Fatalf("export: %v", err)
				}

				dst := mk(t)
				in, err := f.imprt(t, dir, dst)
				if err != nil {
					t.Fatalf("import: %v", err)
				}

				if out.Nodes != in.Nodes || out.Edges != in.Edges ||
					out.NodeProperties != in.NodeProperties || out.EdgeProperties != in.EdgeProperties {
					t.Errorf("exported %+v but imported %+v", out, in)
				}
				compare(t, want, describe(t, dst))

				// Declarations travel. They change plans, not answers, so
				// nothing above would notice their absence.
				if got := dst.OrderedNodeProperties(); len(got) != 1 || got[0] != "seq" {
					t.Errorf("ordered node keys after import: %v, want [seq]", got)
				}
				if got := dst.CompositeNodeProperties(); len(got) != 1 ||
					strings.Join(got[0], ",") != "bucket,seq" {
					t.Errorf("composite node keys after import: %v, want [[bucket seq]]", got)
				}
			})
		})
	}
}

// An empty graph is a real case: a dump of one has to be readable, and its
// trailer of four zeros has to be distinguishable from a missing trailer.
func TestRoundTripOfAnEmptyGraph(t *testing.T) {
	backends(t, func(t *testing.T, mk func(t *testing.T) storeUnderTest) {
		var buf bytes.Buffer
		out, err := bulk.ExportJSONL(&buf, mk(t), bulk.Options{})
		if err != nil {
			t.Fatalf("export: %v", err)
		}
		if out.Nodes != 0 || out.Edges != 0 {
			t.Fatalf("empty store exported %+v", out)
		}
		in, err := bulk.ImportJSONL(bytes.NewReader(buf.Bytes()), mk(t), bulk.Options{})
		if err != nil {
			t.Fatalf("import: %v", err)
		}
		if in != (bulk.Summary{}) {
			t.Errorf("importing an empty dump produced %+v", in)
		}
	})
}

// The trailer's whole purpose. A dump cut between records holds nothing but
// well-formed records, so only the absence of the trailer says the rest is
// missing — and only its counts say a data file lost rows from the middle.
func TestATruncatedDumpIsRefused(t *testing.T) {
	t.Run("jsonl-no-trailer", func(t *testing.T) {
		src := memory.New()
		fixture(t, src)
		var buf bytes.Buffer
		if _, err := bulk.ExportJSONL(&buf, src, bulk.Options{}); err != nil {
			t.Fatalf("export: %v", err)
		}
		lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
		cut := strings.Join(lines[:len(lines)-1], "\n") + "\n"

		_, err := bulk.ImportJSONL(strings.NewReader(cut), memory.New(), bulk.Options{})
		if err == nil {
			t.Fatal("a dump with its trailer removed was accepted")
		}
		if !strings.Contains(err.Error(), "trailer") {
			t.Errorf("error does not name the trailer: %v", err)
		}
	})

	t.Run("jsonl-missing-rows", func(t *testing.T) {
		src := memory.New()
		fixture(t, src)
		var buf bytes.Buffer
		if _, err := bulk.ExportJSONL(&buf, src, bulk.Options{}); err != nil {
			t.Fatalf("export: %v", err)
		}
		// Drop a property entry, not a node. Dropping a node is caught earlier
		// and more precisely — the edge that named it fails the ordering check —
		// which is a better error but a different one, and would leave the count
		// check untested. Nothing references a property entry, so its absence is
		// invisible to everything except the trailer.
		lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
		var cut []string
		dropped := false
		for _, l := range lines {
			if !dropped && strings.Contains(l, "node-property") {
				dropped = true
				continue
			}
			cut = append(cut, l)
		}
		if !dropped {
			t.Fatal("the fixture produced no property entries to drop")
		}

		_, err := bulk.ImportJSONL(strings.NewReader(strings.Join(cut, "\n")+"\n"), memory.New(), bulk.Options{})
		if err == nil {
			t.Fatal("a dump missing a record was accepted")
		}
		if !strings.Contains(err.Error(), "truncated") {
			t.Errorf("error does not say the dump is truncated: %v", err)
		}
	})

	t.Run("dump-cut-mid-record", func(t *testing.T) {
		src := memory.New()
		fixture(t, src)
		var buf bytes.Buffer
		if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
			t.Fatalf("export: %v", err)
		}
		b := buf.Bytes()
		_, err := bulk.ImportDump(bytes.NewReader(b[:len(b)-7]), memory.New(), bulk.Options{})
		if err == nil {
			t.Fatal("a dump cut inside a record was accepted")
		}
	})
}

// The CRC covers the type, the length and the payload — so a flipped bit
// anywhere in a record is caught, not just one in the data.
func TestACorruptedDumpRecordIsCaught(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	var buf bytes.Buffer
	if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	clean := buf.Bytes()

	// Every byte past the magic and the format word, one at a time, is too slow;
	// a spread through the file covers the framing, the payloads and the
	// checksums without pretending to be exhaustive.
	for _, off := range []int{7, 12, 40, len(clean) / 2, len(clean) - 20, len(clean) - 3} {
		if off < 6 || off >= len(clean) {
			continue
		}
		corrupt := bytes.Clone(clean)
		corrupt[off] ^= 0x40
		if _, err := bulk.ImportDump(bytes.NewReader(corrupt), memory.New(), bulk.Options{}); err == nil {
			t.Errorf("a dump with byte %d corrupted was accepted", off)
		}
	}
}

func TestAStreamThatIsNotADumpIsRefused(t *testing.T) {
	_, err := bulk.ImportDump(strings.NewReader("not a dump at all"), memory.New(), bulk.Options{})
	if err == nil {
		t.Fatal("arbitrary bytes were accepted as a dump")
	}
	if !strings.Contains(err.Error(), "graphene_dump") {
		t.Errorf("error does not say what it wanted: %v", err)
	}
}

// The ordering rule, enforced rather than assumed. An edge whose endpoints have
// not been seen cannot be mapped onto anything, and half-importing it would be
// worse than refusing.
func TestAnEdgeBeforeItsEndpointsIsRefused(t *testing.T) {
	lines := []string{
		`{"kind":"header","format":1}`,
		`{"kind":"edge","id":1,"src":10,"dst":11,"labels":[1]}`,
		`{"kind":"trailer","counts":{"nodes":0,"edges":1,"nodeProperties":0,"edgeProperties":0}}`,
	}
	_, err := bulk.ImportJSONL(strings.NewReader(strings.Join(lines, "\n")+"\n"), memory.New(), bulk.Options{})
	if err == nil {
		t.Fatal("an edge naming unknown endpoints was accepted")
	}
	if !strings.Contains(err.Error(), "before it is defined") {
		t.Errorf("error does not explain the ordering: %v", err)
	}
}

func TestAFutureFormatIsRefused(t *testing.T) {
	lines := []string{
		`{"kind":"header","format":99}`,
		`{"kind":"trailer","counts":{"nodes":0,"edges":0,"nodeProperties":0,"edgeProperties":0}}`,
	}
	_, err := bulk.ImportJSONL(strings.NewReader(strings.Join(lines, "\n")+"\n"), memory.New(), bulk.Options{})
	if err == nil {
		t.Fatal("a dump in an unknown format was accepted")
	}
	if !strings.Contains(err.Error(), "format 99") {
		t.Errorf("error does not name the format it found: %v", err)
	}
}

// SkipProperties has to be asked for, and what it produces is a store whose
// records are complete and whose property queries answer nothing. Worth pinning
// because it is exactly what an accidental omission would look like.
func TestSkipPropertiesLeavesTheRecordsAndDropsTheEntries(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	var buf bytes.Buffer
	if _, err := bulk.ExportJSONL(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}

	dst := memory.New()
	in, err := bulk.ImportJSONL(bytes.NewReader(buf.Bytes()), dst, bulk.Options{SkipProperties: true})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if in.Nodes == 0 {
		t.Fatal("no nodes were imported")
	}
	ids, err := dst.NodesByProperty("seq", []byte("003"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("SkipProperties still filed %d entries", len(ids))
	}
	// The counts still report what the dump held, so a caller can see what was
	// dropped rather than inferring it from a zero.
	if in.NodeProperties == 0 {
		t.Error("SkipProperties reported no node properties in a dump that carries twelve")
	}
}

// A CSV export refuses to write into a directory that already holds one, rather
// than overwriting half of it: a manifest describing one graph beside tables
// holding two is worse than a refusal.
func TestCSVWillNotOverwriteAnExistingExport(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	dir := filepath.Join(t.TempDir(), "csv")
	if _, err := bulk.ExportCSV(dir, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, err := bulk.ExportCSV(dir, src, bulk.Options{}); err == nil {
		t.Fatal("a second export into the same directory was accepted")
	}
}

// A CSV directory with no manifest is an export that did not finish, and is
// refused for the same reason a backup with no manifest is.
func TestCSVWithoutAManifestIsRefused(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	dir := filepath.Join(t.TempDir(), "csv")
	if _, err := bulk.ExportCSV(dir, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, bulk.CSVManifest)); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	if _, err := bulk.ImportCSV(dir, memory.New(), bulk.Options{}); err == nil {
		t.Fatal("a CSV directory with no manifest was accepted")
	}
}

// The batch size is a performance knob and must not be a correctness one. A
// batch of one and a batch larger than the graph have to produce the same
// store, or the flush boundaries are doing something they should not.
func TestTheBatchSizeDoesNotChangeTheResult(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	want := describe(t, src)

	var buf bytes.Buffer
	if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	for _, size := range []int{1, 2, 3, 1000} {
		t.Run(fmt.Sprintf("batch-%d", size), func(t *testing.T) {
			dst := memory.New()
			if _, err := bulk.ImportDump(bytes.NewReader(buf.Bytes()), dst, bulk.Options{BatchSize: size}); err != nil {
				t.Fatalf("import: %v", err)
			}
			compare(t, want, describe(t, dst))
		})
	}
}

// The trap this refusal exists for.
//
// *graphene.Graph embeds store.GraphStore, and an embedded interface promotes
// only the methods in its own set — so a Graph does not satisfy
// PropertyEnumerator by inheritance. Handing one to an exporter that treated a
// missing enumerator as "nothing to export" would compile, run, and produce a
// dump with every property entry silently absent. It refuses instead.
type recordsOnly struct{ inner storeUnderTest }

func (r recordsOnly) QueryNodeIDs(q store.NodeQuery) ([]store.NodeID, error) {
	return r.inner.QueryNodeIDs(q)
}
func (r recordsOnly) QueryEdgeIDs(q store.EdgeQuery) ([]store.EdgeID, error) {
	return r.inner.QueryEdgeIDs(q)
}
func (r recordsOnly) GetNode(id store.NodeID) (*store.Node, error) { return r.inner.GetNode(id) }
func (r recordsOnly) GetEdge(id store.EdgeID) (*store.Edge, error) { return r.inner.GetEdge(id) }

func TestASourceThatCannotEnumeratePropertiesIsRefused(t *testing.T) {
	src := memory.New()
	fixture(t, src)

	var buf bytes.Buffer
	_, err := bulk.ExportJSONL(&buf, recordsOnly{src}, bulk.Options{})
	if err == nil {
		t.Fatal("a source with no property enumerator produced a dump")
	}
	if !errors.Is(err, bulk.ErrNoPropertyEntries) {
		t.Errorf("want ErrNoPropertyEntries, got %v", err)
	}

	// And with SkipProperties the same source is fine, because the caller has
	// said the omission is deliberate.
	buf.Reset()
	sum, err := bulk.ExportJSONL(&buf, recordsOnly{src}, bulk.Options{SkipProperties: true})
	if err != nil {
		t.Fatalf("SkipProperties should accept a records-only source: %v", err)
	}
	if sum.Nodes == 0 {
		t.Error("nothing was exported")
	}
}

// The explicit trailer check earns its keep on exactly one input.
//
// For a graph with anything in it, a missing trailer is already caught by the
// count comparison: a zero-valued Trailer against a non-zero Summary
// disagrees. For an *empty* graph both sides are zero and agree, so without a
// separate "was there a trailer at all" test a truncated dump of an empty store
// would import cleanly — and an empty result is exactly what a truncated dump
// looks like from the outside.
func TestATruncatedDumpOfAnEmptyGraphIsRefused(t *testing.T) {
	var buf bytes.Buffer
	if _, err := bulk.ExportJSONL(&buf, memory.New(), bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("an empty dump should be a header and a trailer, got %d lines", len(lines))
	}

	_, err := bulk.ImportJSONL(strings.NewReader(lines[0]+"\n"), memory.New(), bulk.Options{})
	if err == nil {
		t.Fatal("an empty dump with no trailer was accepted")
	}
	if !strings.Contains(err.Error(), "no trailer") {
		t.Errorf("error does not say the trailer is missing: %v", err)
	}
}

// The length bound, tested for what it is actually for.
//
// A corrupted length is caught by the checksum either way — but only after the
// reader has already allocated whatever the length said. The bound exists so a
// flipped bit fails as a refusal instead of as an allocation the process cannot
// make, and the only way to see that is a length no machine could satisfy.
func TestADumpRecordLengthPastTheLimitIsRefused(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	var buf bytes.Buffer
	if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	b := buf.Bytes()

	// Layout: magic(4), format(2), then [type:1][length:4]. The first record's
	// length starts at offset 7.
	b[7], b[8], b[9], b[10] = 0xFF, 0xFF, 0xFF, 0xF0

	_, err := bulk.ImportDump(bytes.NewReader(b), memory.New(), bulk.Options{})
	if err == nil {
		t.Fatal("a record declaring 4 GiB was accepted")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("the error does not name the limit that refused it: %v", err)
	}
}
