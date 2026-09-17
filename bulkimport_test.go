package graphene

// The two import paths, held to the same answer.
//
// ImportDumpBulk and bulk.ImportDump read the same dump and must produce the
// same store. They share almost nothing on the way there -- one batches into a
// delta and compacts, the other streams into an image and commits once -- so
// "the same store" is checked as the image bytes rather than as a sample of
// query answers: two loaders that agree on every question a test thought to ask
// and disagree on the section table are two different stores, and the difference
// surfaces on the next reopen rather than here.
//
// That is the standard disk/bulk_load_test.go already sets for a load against an
// ordinary ingest. This is the same comparison one level up, over a dump.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/store"
)

// importFixture fills g with a shape that exercises every part of a dump.
//
// Two ordered keys and a composite, because the declarations travel in the
// header and are applied before a load builds; a key carrying two values on some
// records and none on others, because a record's entry list is what Format 2
// moved and a fixture where every record carried the same entries would pass
// with any consistent mistake; and edges with their own indexed properties,
// which is the half that a single cursor over the sort dropped the last time
// this area was touched.
func importFixture(t *testing.T, g *Graph, nodes int) {
	t.Helper()
	if err := g.DeclareOrderedNodeProperty("seq"); err != nil {
		t.Fatalf("declare ordered: %v", err)
	}
	if err := g.DeclareCompositeNodeProperties([]string{"tool", "host"}); err != nil {
		t.Fatalf("declare composite: %v", err)
	}
	ids := make([]store.NodeID, 0, nodes)
	for i := 1; i <= nodes; i++ {
		id, err := g.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("blob-%05d", i)),
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
		if err := g.IndexNodeProperty(id, "seq", []byte(fmt.Sprintf("%08d", i))); err != nil {
			t.Fatalf("index seq: %v", err)
		}
		if err := g.IndexNodeProperty(id, "tool", []byte(fmt.Sprintf("tool-%02d", i%7))); err != nil {
			t.Fatalf("index tool: %v", err)
		}
		// Omitted on every fifth record, so some composites are incomplete and
		// some records carry an entry count the one before them did not.
		if i%5 != 0 {
			if err := g.IndexNodeProperty(id, "host", []byte(fmt.Sprintf("host-%03d", i%11))); err != nil {
				t.Fatalf("index host: %v", err)
			}
		}
		// A second value under one key on every third record.
		if i%3 == 0 {
			if err := g.IndexNodeProperty(id, "tool", []byte(fmt.Sprintf("alt-%02d", i%4))); err != nil {
				t.Fatalf("index tool twice: %v", err)
			}
		}
	}
	for i := 1; i < len(ids); i++ {
		eid, err := g.AddEdge(&store.Edge{
			Src: ids[i-1], Dst: ids[i],
			Labels: []store.EdgeType{store.EdgeTypeContains},
			Weight: float32(i) / 8,
		})
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		if i%2 == 0 {
			if err := g.IndexEdgeProperty(eid, "rel", []byte(fmt.Sprintf("rel-%03d", i%5))); err != nil {
				t.Fatalf("index rel: %v", err)
			}
		}
	}
}

// dumpOf exports g to a file and returns a function that opens it.
func dumpOf(t *testing.T, g *Graph, opts bulk.Options) (string, func() (io.ReadCloser, error)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "graph.dump")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create dump: %v", err)
	}
	if _, err := bulk.ExportDump(f, g, opts); err != nil {
		_ = f.Close()
		t.Fatalf("ExportDump: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close dump: %v", err)
	}
	return path, func() (io.ReadCloser, error) { return os.Open(path) }
}

// imageOf reads a store's record image with the one volatile field blanked,
// which is what the two paths must agree on byte for byte.
//
// The field is lastCompactUnixNano at offset 54: a wall-clock reading, so two
// images of identical content differ there by construction. The image's own
// digest excludes it for the same reason, and disk/csr_determinism_test.go's
// stripVolatile is this function one package down -- written out again rather
// than exported, because a test reaching for an unexported constant would be
// asserting the layout it is trying to compare across.
func imageOf(t *testing.T, dir string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	const (
		lastCompactOffset = 54
		headerSize        = 102
	)
	if len(b) < headerSize || string(b[:4]) != "GCSR" {
		t.Fatalf("%s is %d bytes and does not start with GCSR; the offsets below are wrong", dir, len(b))
	}
	for i := lastCompactOffset; i < lastCompactOffset+8; i++ {
		b[i] = 0
	}
	return b
}

// openEmpty opens a fresh store and returns it with its directory.
func openEmpty(t *testing.T) (*Graph, string) {
	t.Helper()
	dir := t.TempDir()
	g, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return g, dir
}

// A dump imported both ways produces the same image, byte for byte.
//
// The incremental arm is compacted and reopened before the comparison, because
// it is the only way the two are comparable at all: an incremental import leaves
// its records in the delta and the image is whatever was there before, which for
// an empty store is an empty image. Compacting folds them in. That is not a
// thumb on the scale -- it is the arm the user guide recommends, and it is
// exactly the work the bulk arm does not have to do.
func TestImportDumpBulk_MatchesAnIncrementalImport(t *testing.T) {
	src, _ := openEmpty(t)
	importFixture(t, src, 200)
	_, open := dumpOf(t, src, bulk.Options{})
	if err := src.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	bulkG, bulkDir := openEmpty(t)
	loaded, sum, err := bulkG.ImportDumpBulk(open, BulkImportOptions{})
	if err != nil {
		t.Fatalf("ImportDumpBulk: %v", err)
	}
	if err := loaded.Close(); err != nil {
		t.Fatalf("close loaded: %v", err)
	}

	incG, incDir := openEmpty(t)
	r, err := open()
	if err != nil {
		t.Fatalf("open dump: %v", err)
	}
	incSum, err := bulk.ImportDump(r, incG, bulk.Options{})
	_ = r.Close()
	if err != nil {
		t.Fatalf("ImportDump: %v", err)
	}
	if err := incG.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := incG.Close(); err != nil {
		t.Fatalf("close incremental: %v", err)
	}

	if sum.Nodes != incSum.Nodes || sum.Edges != incSum.Edges ||
		sum.NodeProperties != incSum.NodeProperties || sum.EdgeProperties != incSum.EdgeProperties {
		t.Fatalf("the two paths report different work:\n bulk %+v\n incr %+v", sum, incSum)
	}
	if sum.Nodes != 200 {
		t.Fatalf("the load reports %d nodes, the fixture wrote 200", sum.Nodes)
	}

	want, got := imageOf(t, incDir), imageOf(t, bulkDir)
	if !bytes.Equal(want, got) {
		at := -1
		for i := range min(len(want), len(got)) {
			if want[i] != got[i] {
				at = i
				break
			}
		}
		t.Fatalf("the images differ: incremental %d bytes, bulk %d bytes, first difference at %d",
			len(want), len(got), at)
	}
}

// The store a load hands back answers what the source answered.
//
// Byte identity above says the two importers agree with each other. This says
// they agree with the graph that was exported, which is the claim a caller
// actually makes: identity between two wrong things is still wrong.
func TestImportDumpBulk_AnswersWhatTheSourceAnswered(t *testing.T) {
	src, _ := openEmpty(t)
	importFixture(t, src, 120)

	type answer struct {
		nodes, edges int
		byTool       int
		byRel        int
		composite    int
	}
	ask := func(g *Graph) answer {
		t.Helper()
		var a answer
		ids, err := g.NodesByProperty("tool", []byte("tool-03"))
		if err != nil {
			t.Fatalf("NodesByProperty tool: %v", err)
		}
		a.byTool = len(ids)
		eids, err := g.EdgesByProperty("rel", []byte("rel-002"))
		if err != nil {
			t.Fatalf("EdgesByProperty rel: %v", err)
		}
		a.byRel = len(eids)
		cids, err := g.NodesByProperties(map[string][]byte{
			"tool": []byte("tool-03"), "host": []byte("host-003"),
		})
		if err != nil {
			t.Fatalf("NodesByProperties: %v", err)
		}
		a.composite = len(cids)
		all, err := g.QueryNodeIDs(store.NodeQuery{})
		if err != nil {
			t.Fatalf("QueryNodeIDs: %v", err)
		}
		a.nodes = len(all)
		alle, err := g.QueryEdgeIDs(store.EdgeQuery{})
		if err != nil {
			t.Fatalf("QueryEdgeIDs: %v", err)
		}
		a.edges = len(alle)
		return a
	}
	want := ask(src)
	if want.byTool == 0 || want.byRel == 0 || want.composite == 0 {
		t.Fatalf("the fixture answers nothing to ask about: %+v", want)
	}

	_, open := dumpOf(t, src, bulk.Options{})
	if err := src.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	g, _ := openEmpty(t)
	loaded, _, err := g.ImportDumpBulk(open, BulkImportOptions{})
	if err != nil {
		t.Fatalf("ImportDumpBulk: %v", err)
	}
	defer loaded.Close()

	if got := ask(loaded); got != want {
		t.Fatalf("the loaded store answers %+v, the source answered %+v", got, want)
	}
}

// A dump whose records do not carry their entries is refused at the header.
//
// Refused rather than quietly imported the slow way, and refused *before* the
// receiver is closed: a caller who gets this error still holds a usable Graph
// and can fall back to bulk.ImportDump on the same file, which is the whole
// point of naming the condition.
func TestImportDumpBulk_RefusesADumpWithoutInlineEntries(t *testing.T) {
	src, _ := openEmpty(t)
	importFixture(t, src, 20)
	// SkipProperties produces a Format 2 dump with InlineEntries false, which is
	// the same header shape a Format 1 dump presents.
	_, open := dumpOf(t, src, bulk.Options{SkipProperties: true})
	if err := src.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	g, _ := openEmpty(t)
	loaded, _, err := g.ImportDumpBulk(open, BulkImportOptions{})
	if !errors.Is(err, ErrDumpNotBulkLoadable) {
		if loaded != nil {
			loaded.Close()
		}
		t.Fatalf("a dump without inline entries loaded with %v, want ErrDumpNotBulkLoadable", err)
	}
	// The receiver survived the refusal, which is what makes the fallback
	// reachable.
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatalf("the graph was closed by a refusal that happens before the load: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// An export that carries entries says so, and one that cannot does not.
//
// The flag is what the refusal above reads, so a flag that were always true
// would make that refusal unreachable and this pair of assertions is what keeps
// it honest.
func TestExportDump_TheHeaderDeclaresWhetherEntriesAreInline(t *testing.T) {
	src, _ := openEmpty(t)
	importFixture(t, src, 10)
	defer src.Close()

	for _, tc := range []struct {
		name string
		opts bulk.Options
		want bool
	}{
		{"WithProperties", bulk.Options{}, true},
		{"SkipProperties", bulk.Options{SkipProperties: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, open := dumpOf(t, src, tc.opts)
			r, err := open()
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = r.Close() }()
			scan, err := bulk.OpenDumpScan(r)
			if err != nil {
				t.Fatalf("OpenDumpScan: %v", err)
			}
			h := scan.Header()
			if h.Format != bulk.Format {
				t.Fatalf("header says format %d, this build writes %d", h.Format, bulk.Format)
			}
			if h.InlineEntries != tc.want {
				t.Fatalf("InlineEntries is %v, want %v", h.InlineEntries, tc.want)
			}
		})
	}
}

// A scan yields the nodes and the edges in separate passes over one stream.
//
// The two passes are what a load is built on, and the second one has to skip the
// records the first consumed. A scan that returned the node records to the edge
// pass would hand a load somebody else's endpoints.
func TestDumpScan_EachPassSeesOnlyItsOwnRecords(t *testing.T) {
	src, _ := openEmpty(t)
	importFixture(t, src, 40)
	_, open := dumpOf(t, src, bulk.Options{})
	if err := src.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	r1, _ := open()
	defer func() { _ = r1.Close() }()
	s1, err := bulk.OpenDumpScan(r1)
	if err != nil {
		t.Fatalf("OpenDumpScan: %v", err)
	}
	nodes, withEntries := 0, 0
	if err := s1.Nodes(func(n *store.Node, e []store.PropertyEntry) error {
		nodes++
		if len(e) > 0 {
			withEntries++
		}
		return nil
	}); err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if nodes != 40 {
		t.Fatalf("the node pass saw %d nodes, want 40", nodes)
	}
	if withEntries != 40 {
		t.Fatalf("%d of 40 node records carried entries; every one in this fixture is indexed", withEntries)
	}

	r2, _ := open()
	defer func() { _ = r2.Close() }()
	s2, err := bulk.OpenDumpScan(r2)
	if err != nil {
		t.Fatalf("OpenDumpScan: %v", err)
	}
	edges := 0
	if err := s2.Edges(func(e *store.Edge, _ []store.PropertyEntry) error {
		edges++
		return nil
	}); err != nil {
		t.Fatalf("Edges: %v", err)
	}
	if edges != 39 {
		t.Fatalf("the edge pass saw %d edges, want 39", edges)
	}
	tr, err := s2.Trailer()
	if err != nil {
		t.Fatalf("Trailer: %v", err)
	}
	if tr.Nodes != 40 || tr.Edges != 39 {
		t.Fatalf("the trailer says %d nodes and %d edges", tr.Nodes, tr.Edges)
	}
}

// The identifier map answers what it was given, consecutive or not.
//
// The consecutive case is every load nothing wrote into and is the one that
// costs 8 bytes a node. The gapped case is a load that was raced by a writer
// taking an identifier from the same counter, which BulkLoad permits by design,
// and it must not silently shift every endpoint after the gap.
func TestExportedNodeIDs_MapsBothColumns(t *testing.T) {
	t.Run("Consecutive", func(t *testing.T) {
		var m exportedNodeIDs
		exported := []store.NodeID{3, 9, 14, 90, 91}
		for i, e := range exported {
			if err := m.add(e, store.NodeID(100+i)); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		if m.assigned != nil {
			t.Fatalf("a consecutive run materialised the assigned column: %v", m.assigned)
		}
		for i, e := range exported {
			got, ok := m.lookup(e)
			if !ok || got != store.NodeID(100+i) {
				t.Fatalf("lookup(%d) = %d, %v; want %d", e, got, ok, 100+i)
			}
		}
		if _, ok := m.lookup(4); ok {
			t.Fatal("an identifier that was never added resolved")
		}
	})

	t.Run("AGapIsCarried", func(t *testing.T) {
		var m exportedNodeIDs
		// 100, 101, then a writer takes 102, so this load gets 103.
		pairs := [][2]store.NodeID{{3, 100}, {9, 101}, {14, 103}, {20, 104}}
		for _, p := range pairs {
			if err := m.add(p[0], p[1]); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		if m.assigned == nil {
			t.Fatal("a gap left the assigned column unmaterialised, so every endpoint after it is wrong")
		}
		for _, p := range pairs {
			got, ok := m.lookup(p[0])
			if !ok || got != p[1] {
				t.Fatalf("lookup(%d) = %d, %v; want %d", p[0], got, ok, p[1])
			}
		}
	})

	t.Run("DescendingIsRefused", func(t *testing.T) {
		var m exportedNodeIDs
		if err := m.add(9, 1); err != nil {
			t.Fatalf("add: %v", err)
		}
		err := m.add(3, 2)
		if !errors.Is(err, ErrDumpNotBulkLoadable) {
			t.Fatalf("a descending identifier was accepted: %v", err)
		}
	})
}
