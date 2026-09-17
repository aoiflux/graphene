package disk

// The bulk load is held to the image an ordinary ingest produces.
//
// This is the whole proof of Phase 5, and it is the same proof Phase 4 used
// because the risk is the same: a second way to reach the encoder is a second
// chance to disagree with the format. The two paths share writeImage, so what is
// being tested is not two serialisers agreeing -- it is that a sort of the
// triples as they stream past reaches the same GPIX, GPIR, GCPX and GIDX that
// the live property index reaches, that identifiers land where they land, and
// that the header fields a streaming build cannot know up front are patched with
// what it actually wrote.
//
// A difference of one byte anywhere is a difference in what a reader would find,
// and the snapshot root and the whole-file digest are both inside the
// comparison.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// bulkFixture is one graph, held in the shape both paths can write it from.
type bulkFixture struct {
	nodes []BulkNode
	edges []BulkEdge
}

// bulkDeclare makes the declarations both arms load under.
//
// An ordered key, a node composite and an edge composite, because each reaches
// the image by a different route: GORD is a list of names, GCPX is postings that
// a bulk load has to build from the records rather than read off an index, and
// the edge half of GCPX is written after the node half and is where an order
// that was right for one kind and wrong for the other would show.
func bulkDeclare(t *testing.T, s *Store) {
	t.Helper()
	if err := s.DeclareOrderedNodeProperty("path"); err != nil {
		t.Fatalf("DeclareOrderedNodeProperty: %v", err)
	}
	// Two node composites rather than one, so GCPX writes more than a single
	// directory entry per kind and the boundary between two composites' runs is
	// inside the comparison.
	//
	// They are declared in the reverse of their name order, which does NOT
	// currently distinguish the two orders a load could sort its tuples by:
	// index.PropertyIndex returns its composite list sorted by name, so both arms
	// walk them sorted whatever order they were declared in. Written this way
	// because the day that changes this is the test that notices.
	if err := s.DeclareCompositeNodeProperties([]string{"tool", "case_id"}); err != nil {
		t.Fatalf("DeclareCompositeNodeProperties: %v", err)
	}
	if err := s.DeclareCompositeNodeProperties([]string{"sha256", "path"}); err != nil {
		t.Fatalf("DeclareCompositeNodeProperties: %v", err)
	}
	if err := s.DeclareCompositeEdgeProperties([]string{"rel", "run"}); err != nil {
		t.Fatalf("DeclareCompositeEdgeProperties: %v", err)
	}
}

// newBulkFixture builds n nodes and their edges.
//
// Three shapes are deliberate and each one is a case the two paths could
// disagree about:
//
//   - every tenth node omits case_id, so it completes no composite tuple and
//     must not appear in GCPX -- the rule a bulk load restates rather than
//     reaches through the index for;
//   - every seventh node carries two values for "tool", so its row has an
//     overflow position and files the cross product rather than one tuple;
//   - every fifth node repeats one of its entries exactly, which the index
//     collapses, so the image must collapse it too.
func newBulkFixture(n int) bulkFixture {
	var fx bulkFixture
	for i := 1; i <= n; i++ {
		props := []BulkProperty{
			{Key: "sha256", Value: []byte(fmt.Sprintf("sha256-%06d", i*7))},
			{Key: "path", Value: []byte(fmt.Sprintf("/case/%04d/file-%04d", i%13, i))},
			{Key: "tool", Value: []byte(fmt.Sprintf("tool-%02d", i%5))},
		}
		if i%10 != 0 {
			props = append(props, BulkProperty{Key: "case_id", Value: []byte(fmt.Sprintf("case-%03d", i%29))})
		}
		if i%7 == 0 {
			props = append(props, BulkProperty{Key: "tool", Value: []byte(fmt.Sprintf("tool-alt-%02d", i%3))})
		}
		if i%5 == 0 {
			props = append(props, BulkProperty{Key: "sha256", Value: []byte(fmt.Sprintf("sha256-%06d", i*7))})
		}
		fx.nodes = append(fx.nodes, BulkNode{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("payload-%04d", i)),
			Index:      props,
		})
	}
	for i := 2; i <= n; i++ {
		fx.edges = append(fx.edges, BulkEdge{
			Src:        store.NodeID(i - 1),
			Dst:        store.NodeID(i),
			Labels:     []store.EdgeType{store.EdgeTypeContains},
			Weight:     float32(i%17) / 17,
			Properties: []byte(fmt.Sprintf("edge-%04d", i)),
			Index: []BulkProperty{
				{Key: "rel", Value: []byte(fmt.Sprintf("rel-%03d", i%11))},
				{Key: "run", Value: []byte(fmt.Sprintf("run-%02d", i%4))},
			},
		})
	}
	return fx
}

// ingestIncrementally writes the fixture the way every caller writes one today,
// then compacts. This is the image the bulk load must reproduce.
func ingestIncrementally(t *testing.T, s *Store, fx bulkFixture) {
	t.Helper()
	for i := range fx.nodes {
		n := fx.nodes[i]
		id, err := s.AddNode(&store.Node{Labels: n.Labels, Properties: n.Properties})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		for _, p := range n.Index {
			if err := s.IndexNodeProperty(id, p.Key, p.Value); err != nil {
				t.Fatalf("IndexNodeProperty(%d, %q): %v", id, p.Key, err)
			}
		}
	}
	for i := range fx.edges {
		e := fx.edges[i]
		id, err := s.AddEdge(&store.Edge{Src: e.Src, Dst: e.Dst, Labels: e.Labels,
			Weight: e.Weight, Properties: e.Properties})
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		for _, p := range e.Index {
			if err := s.IndexEdgeProperty(id, p.Key, p.Value); err != nil {
				t.Fatalf("IndexEdgeProperty(%d, %q): %v", id, p.Key, err)
			}
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
}

// loadInBulk writes the same fixture through BulkLoad, returning the reopened
// store.
func loadInBulk(t *testing.T, s *Store, fx bulkFixture) *Store {
	t.Helper()
	reopened, err := s.BulkLoad(
		func(w *BulkNodeWriter) error {
			for i := range fx.nodes {
				if _, err := w.AddNode(fx.nodes[i]); err != nil {
					return err
				}
			}
			return nil
		},
		func(w *BulkEdgeWriter) error {
			for i := range fx.edges {
				if _, err := w.AddEdge(fx.edges[i]); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("BulkLoad: %v", err)
	}
	return reopened
}

// csrCommitSeqOffset is where the header carries the commit sequence high-water
// mark. See csr_write.go's header for the layout.
const csrCommitSeqOffset = 46

// stripBulkVolatile is stripVolatile plus the commit sequence.
//
// The commit sequence counts durable WAL commits, and the two arms here make
// wildly different numbers of them -- an incremental ingest commits once per
// record and a bulk load commits none, which is the entire point of it. So it is
// a property of how the records arrived and not of what the image holds, which
// is the same argument stripVolatile makes for the compaction timestamp.
//
// It is stripped here rather than in stripVolatile because every other caller of
// that helper compares two images built the same way, where the mark is a real
// part of the identity and must not be blanked.
func stripBulkVolatile(data []byte) []byte {
	out := stripVolatile(data)
	if len(out) >= csrV8HeaderSize {
		for i := csrCommitSeqOffset; i < csrCommitSeqOffset+8; i++ {
			out[i] = 0
		}
	}
	return out
}

// TestBulkLoad_WritesTheSameImageAsAnIncrementalIngest is the gate on Phase 5.
func TestBulkLoad_WritesTheSameImageAsAnIncrementalIngest(t *testing.T) {
	// Both index modes, because the index reaches the encoder by two different
	// routes and a bulk load feeds both from one sort. IndexMapped writes GPIX
	// and GPIR through a grouped walk and its composites through GCPX;
	// IndexResident writes GIDX from the same sort flattened back to one triple
	// an entry. A load that agreed on one and not the other would be half-right
	// in a way only a caller who had chosen v8 would ever discover.
	for _, mode := range []IndexMode{IndexMapped, IndexResident} {
		t.Run(mode.String(), func(t *testing.T) {
			fx := newBulkFixture(150)

			incremental := func() []byte {
				s, dir := openBulk(t, mode)
				defer s.Close()
				bulkDeclare(t, s)
				ingestIncrementally(t, s, fx)
				return readCSR(t, dir)
			}
			loaded := func() []byte {
				s, dir := openBulk(t, mode)
				bulkDeclare(t, s)
				reopened := loadInBulk(t, s, fx)
				defer reopened.Close()
				return readCSR(t, dir)
			}

			a, b := stripBulkVolatile(incremental()), stripBulkVolatile(loaded())
			if !bytes.Equal(a, b) {
				t.Fatalf("the bulk load wrote a different image than an incremental ingest of the same records.\n%s\n\n"+
					"They share an encoder, so this is the sort, the patched header fields or the "+
					"composite rule disagreeing -- see bulk_index.go and bulkBuilder.composites.",
					describeDiff(t, a, b))
			}
		})
	}
}

// openBulk opens a fresh store in one index mode.
func openBulk(t *testing.T, mode IndexMode) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{IndexMode: mode})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	return s, dir
}

// The degenerate shapes, which are where a patched header field is most likely
// to be wrong.
//
// An empty load writes a header whose counts and both sequence marks are patched
// with zero. A load with no edges patches one of each pair and not the other,
// which is the case a single shared patch would get wrong.
func TestBulkLoad_DegenerateShapesWriteTheSameImage(t *testing.T) {
	shapes := []struct {
		name string
		fx   bulkFixture
	}{
		{"Empty", bulkFixture{}},
		{"NodesButNoEdges", bulkFixture{nodes: newBulkFixture(20).nodes}},
		{"OneNode", bulkFixture{nodes: newBulkFixture(1).nodes}},
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			incremental := func() []byte {
				s, dir := openFresh(t)
				defer s.Close()
				bulkDeclare(t, s)
				ingestIncrementally(t, s, sh.fx)
				return readCSR(t, dir)
			}
			loaded := func() []byte {
				s, dir := openFresh(t)
				bulkDeclare(t, s)
				reopened := loadInBulk(t, s, sh.fx)
				defer reopened.Close()
				return readCSR(t, dir)
			}
			a, b := stripBulkVolatile(incremental()), stripBulkVolatile(loaded())
			if !bytes.Equal(a, b) {
				t.Fatalf("%s: the two paths wrote different images.\n%s", sh.name, describeDiff(t, a, b))
			}
		})
	}
}

// The sort must reach the same image whether it fit in memory or spilled.
//
// MaxWorkingBytes at the floor makes the chunk small enough that a fixture this
// size produces many runs and the external merge does the work; the default
// sorts it in one chunk and opens no file. This is the same assertion
// TestCompact_ImageIsIdenticalAtEveryWorkingSize makes about a compaction's
// intermediates, and it is the only thing standing between a merge bug and an
// index that is quietly missing entries.
func TestBulkLoad_TheImageIsIdenticalWhetherTheSortSpilled(t *testing.T) {
	fx := newBulkFixture(400)
	at := func(maxWorking int64) []byte {
		dir := t.TempDir()
		s, err := OpenWithOptions(dir, Options{Compact: CompactOptions{MaxWorkingBytes: maxWorking}})
		if err != nil {
			t.Fatalf("OpenWithOptions(%d): %v", maxWorking, err)
		}
		bulkDeclare(t, s)
		reopened := loadInBulk(t, s, fx)
		defer reopened.Close()
		return readCSR(t, dir)
	}
	a, b := stripBulkVolatile(at(0)), stripBulkVolatile(at(minCompactWorkingBytes))
	if !bytes.Equal(a, b) {
		t.Fatalf("a load whose sort spilled wrote a different image than one whose sort did not.\n%s",
			describeDiff(t, a, b))
	}
}

// The loaded store must answer the queries the records were indexed for.
//
// Byte equality above already implies this, but only as long as the incremental
// path is itself correct. This reads the store back through the ordinary open
// path, which is what a caller of BulkLoad does next.
func TestBulkLoad_TheReopenedStoreAnswersQueries(t *testing.T) {
	fx := newBulkFixture(120)
	s, _ := openFresh(t)
	bulkDeclare(t, s)
	g := loadInBulk(t, s, fx)
	defer g.Close()

	st := g.StorageStats()
	if st.CSRNodes != 120 {
		t.Fatalf("the image holds %d nodes, want 120", st.CSRNodes)
	}
	if st.DeltaNodes != 0 {
		t.Fatalf("the reopened store has a delta of %d nodes; a load leaves nothing in one", st.DeltaNodes)
	}
	for _, id := range []store.NodeID{1, 2, 60, 119, 120} {
		n, err := g.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		if want := fmt.Sprintf("payload-%04d", id); string(n.Properties) != want {
			t.Fatalf("node %d carries %q, want %q", id, n.Properties, want)
		}
	}
	if _, err := g.GetNode(store.NodeID(121)); err == nil {
		t.Fatal("the image answered for an identifier no record was loaded under")
	}

	// A property lookup, a range over the ordered key, and a composite -- the
	// three index sections a load writes, each asked a question only its own
	// section can answer.
	ids, err := g.NodesByProperty("sha256", []byte(fmt.Sprintf("sha256-%06d", 60*7)))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(ids) != 1 || ids[0] != 60 {
		t.Fatalf("NodesByProperty found %v, want [60]", ids)
	}
	var tuples, posted int
	ok, err := g.index().ForEachCompositeTuple(index.NodeKind, []string{"tool", "case_id"},
		func(tuple []byte, ids []uint64) bool {
			tuples++
			posted += len(ids)
			return true
		})
	if err != nil {
		t.Fatalf("ForEachCompositeTuple: %v", err)
	}
	if !ok {
		t.Fatal("the reopened store does not carry the composite the load declared")
	}
	if tuples == 0 || posted == 0 {
		t.Fatalf("the composite holds %d tuples and %d postings; GCPX carried nothing", tuples, posted)
	}
	// Every node but the tenth completes the tuple, and every seventh files two
	// of them. Asserted as a figure rather than as "not zero", because a load
	// that filed a tuple for a record missing a member would also be non-zero.
	want := 0
	for i := 1; i <= 120; i++ {
		if i%10 == 0 {
			continue
		}
		if i%7 == 0 {
			want += 2
			continue
		}
		want++
	}
	if posted != want {
		t.Fatalf("the composite holds %d postings, want %d: a record either files a tuple it should not or misses one it should", posted, want)
	}
}

// A store that holds anything is refused, by name and before anything is
// written.
func TestBulkLoad_RefusesAStoreThatHoldsAnything(t *testing.T) {
	cases := []struct {
		name string
		fill func(t *testing.T, s *Store)
	}{
		{"AnImage", func(t *testing.T, s *Store) {
			addNodeD(t, s, store.NodeTypeMicroArtefact)
			if err := s.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}
		}},
		{"ADelta", func(t *testing.T, s *Store) {
			addNodeD(t, s, store.NodeTypeMicroArtefact)
		}},
		{"AnIndexEntry", func(t *testing.T, s *Store) {
			// An entry naming a record that was never added, so the delta is
			// empty and there is no image and the index is the only thing
			// holding anything. This is the case the first two would not catch,
			// and it is the one that matters: a load that ran here would write
			// an image whose GPIX silently lost the entry.
			if err := s.IndexNodeProperty(store.NodeID(999), "sha256", []byte("x")); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := openFresh(t)
			c.fill(t, s)
			_, err := s.BulkLoad(
				func(w *BulkNodeWriter) error { return nil },
				func(w *BulkEdgeWriter) error { return nil })
			if !errors.Is(err, ErrBulkLoadNotEmpty) {
				t.Fatalf("BulkLoad into a store holding %s returned %v, want ErrBulkLoadNotEmpty", c.name, err)
			}
		})
	}
}

// A pass that fails leaves the directory as it was found.
//
// The image is still a temp file when the caller's own function gives up, so
// there must be no image, no temp file, and the error the caller returned rather
// than whatever the encoder made of a stream that stopped.
func TestBulkLoad_AFailedPassInstallsNothing(t *testing.T) {
	boom := errors.New("the source gave up")
	for _, which := range []string{"nodes", "edges"} {
		t.Run(which, func(t *testing.T) {
			s, dir := openFresh(t)
			bulkDeclare(t, s)
			fx := newBulkFixture(40)

			_, err := s.BulkLoad(
				func(w *BulkNodeWriter) error {
					for i := range fx.nodes {
						if which == "nodes" && i == 20 {
							return boom
						}
						if _, err := w.AddNode(fx.nodes[i]); err != nil {
							return err
						}
					}
					return nil
				},
				func(w *BulkEdgeWriter) error {
					if which == "edges" {
						return boom
					}
					return nil
				})
			if !errors.Is(err, boom) {
				t.Fatalf("BulkLoad returned %v, want the caller's own error", err)
			}
			if _, err := os.Stat(filepath.Join(dir, csrFileName)); !os.IsNotExist(err) {
				t.Fatalf("a failed load left an image behind: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, csrFileName+".tmp")); !os.IsNotExist(err) {
				t.Fatalf("a failed load left its temp image behind: %v", err)
			}
		})
	}
}

// A record with no labels is refused, and nothing is installed.
func TestBulkLoad_RefusesARecordWithNoLabels(t *testing.T) {
	s, dir := openFresh(t)
	_, err := s.BulkLoad(
		func(w *BulkNodeWriter) error {
			_, err := w.AddNode(BulkNode{Properties: []byte("x")})
			return err
		},
		func(w *BulkEdgeWriter) error { return nil })
	if !errors.Is(err, store.ErrNoLabels) {
		t.Fatalf("BulkLoad returned %v, want store.ErrNoLabels", err)
	}
	if _, err := os.Stat(filepath.Join(dir, csrFileName)); !os.IsNotExist(err) {
		t.Fatalf("a refused load left an image behind: %v", err)
	}
}

// The sort must be sealed before it is read.
//
// This is the one invariant the design rests on that no ordinary run can
// violate: the encoder writes every record before it writes any section, so the
// key list is always complete by the time GPIX asks for it. The check exists for
// the case that should be impossible, and this drives it directly -- an encoder
// that ever reordered the two would fail the write rather than write an index
// missing every key discovered after it had started.
func TestBulkSorter_RefusesToBeReadBeforeItIsSealed(t *testing.T) {
	s := newBulkSorter(t.TempDir(), compactBuffers{}, nil)
	s.keyLess = bulkKeyOrder(s)
	defer s.close()
	slot, err := s.slot("sha256")
	if err != nil {
		t.Fatalf("slot: %v", err)
	}
	s.add(bulkKindNode, slot, []byte("v"), 1)
	if _, err := s.cursor(); err == nil {
		t.Fatal("an unsealed sort produced a cursor; an index written from it would be missing every key it had not yet seen")
	}
	s.seal()
	if _, err := s.cursor(); err != nil {
		t.Fatalf("a sealed sort refused a cursor: %v", err)
	}
}

// The sort's order is the order both encoders walk in, whether it spilled or
// not.
//
// Asserted directly rather than only through the image, so a failure says "the
// merge is wrong" instead of leaving it to be inferred from a byte offset. The
// floor forces enough runs that the heap merge is what produces the order.
func TestBulkSorter_YieldsKindThenKeyThenValueThenID(t *testing.T) {
	for _, name := range []string{"InMemory", "Spilled"} {
		t.Run(name, func(t *testing.T) {
			bufs := compactBuffers{}
			if name == "Spilled" {
				bufs = compactBuffersFor(minCompactWorkingBytes)
			}
			s := newBulkSorter(t.TempDir(), bufs, nil)
			s.keyLess = bulkKeyOrder(s)
			defer s.close()

			// Added in an order that is wrong on every term at once.
			keys := []string{"zeta", "alpha", "mu"}
			for i := 3000; i >= 1; i-- {
				for _, k := range keys {
					slot, err := s.slot(k)
					if err != nil {
						t.Fatalf("slot: %v", err)
					}
					kind := bulkKindNode
					if i%2 == 0 {
						kind = bulkKindEdge
					}
					s.add(kind, slot, []byte(fmt.Sprintf("v-%05d", i)), uint64(i))
				}
			}
			s.seal()
			c, err := s.cursor()
			if err != nil {
				t.Fatalf("cursor: %v", err)
			}

			var n int
			var prev bulkCursorEntry
			var prevKey string
			for {
				e, ok := c.peek()
				if !ok {
					break
				}
				key := s.keys[e.keyID]
				if n > 0 {
					if cmpBulk(prev.kind, prevKey, prev.value, prev.id, e.kind, key, e.value, e.id) >= 0 {
						t.Fatalf("entry %d is not after its predecessor: (%d,%q,%q,%d) then (%d,%q,%q,%d)",
							n, prev.kind, prevKey, prev.value, prev.id, e.kind, key, e.value, e.id)
					}
				}
				prev, prevKey = e, key
				prev.value = append([]byte(nil), e.value...)
				n++
				c.advance()
			}
			if c.err != nil {
				t.Fatalf("cursor: %v", c.err)
			}
			if want := 3 * 3000; n != want {
				t.Fatalf("the sort yielded %d entries, want %d", n, want)
			}
		})
	}
}

// cmpBulk is the order the sort promises, written out here so the test states it
// rather than reusing the code under test.
func cmpBulk(ka uint8, keya string, va []byte, ida uint64,
	kb uint8, keyb string, vb []byte, idb uint64) int {
	if ka != kb {
		if ka < kb {
			return -1
		}
		return 1
	}
	if c := bytes.Compare([]byte(keya), []byte(keyb)); c != 0 {
		return c
	}
	if c := bytes.Compare(va, vb); c != 0 {
		return c
	}
	switch {
	case ida < idb:
		return -1
	case ida > idb:
		return 1
	}
	return 0
}

// The exported tuple encoder must be the one the index files under.
//
// A bulk load builds composite tuples without an index, so it calls this rather
// than re-deriving the encoding -- and index.CompositeBase says plainly why a
// second implementation that disagreed by a byte would make GCPX's binary search
// bounded, silent and wrong. This is the assertion that the export is the same
// function.
func TestEncodeCompositeTuple_MatchesWhatTheIndexFilesUnder(t *testing.T) {
	// ("a", "bc") and ("ab", "c") are the collision the length prefix exists to
	// prevent, so they are the pair worth asserting on.
	one := index.EncodeCompositeTuple([]string{"a", "bc"})
	two := index.EncodeCompositeTuple([]string{"ab", "c"})
	if bytes.Equal(one, two) {
		t.Fatal("two different tuples encoded to the same bytes")
	}
	if index.CompositeName([]string{"tool", "case_id"}) == index.CompositeName([]string{"case_id", "tool"}) {
		t.Fatal("a composite and its reverse share a name; the two are different declarations")
	}
}

// A unique key that two records share is refused, and nothing is installed.
//
// This is the one constraint a load has to enforce for itself: the incremental
// path asks the index whether anyone else holds the value, and a load has no
// index to ask. It is checked from the sort instead -- a duplicate is a value
// with two identifiers filed against it -- so the check is exact rather than
// best-effort, and it runs in both index modes because the postings are read by
// two different walkers.
func TestBulkLoad_RefusesADuplicateUniqueValue(t *testing.T) {
	for _, mode := range []IndexMode{IndexMapped, IndexResident} {
		t.Run(mode.String(), func(t *testing.T) {
			s, dir := openBulk(t, mode)
			if err := s.DeclareUniqueNodeProperty("sha256"); err != nil {
				t.Fatalf("DeclareUniqueNodeProperty: %v", err)
			}
			_, err := s.BulkLoad(
				func(w *BulkNodeWriter) error {
					for i := 0; i < 3; i++ {
						if _, err := w.AddNode(BulkNode{
							Labels: []store.NodeType{store.NodeTypeMicroArtefact},
							Index:  []BulkProperty{{Key: "sha256", Value: []byte("the same")}},
						}); err != nil {
							return err
						}
					}
					return nil
				},
				func(w *BulkEdgeWriter) error { return nil })
			if err == nil {
				t.Fatal("three records were loaded under one unique value and the load succeeded")
			}
			if !strings.Contains(err.Error(), "unique") {
				t.Fatalf("BulkLoad returned %v, which does not name the constraint it refused", err)
			}
			if _, err := os.Stat(filepath.Join(dir, csrFileName)); !os.IsNotExist(err) {
				t.Fatalf("a refused load left an image behind: %v", err)
			}
		})
	}
}

// A unique key that no two records share is loaded, which is what makes the
// refusal above a check rather than a prohibition.
func TestBulkLoad_AcceptsDistinctUniqueValues(t *testing.T) {
	s, _ := openFresh(t)
	if err := s.DeclareUniqueNodeProperty("sha256"); err != nil {
		t.Fatalf("DeclareUniqueNodeProperty: %v", err)
	}
	g, err := s.BulkLoad(
		func(w *BulkNodeWriter) error {
			for i := 0; i < 50; i++ {
				if _, err := w.AddNode(BulkNode{
					Labels: []store.NodeType{store.NodeTypeMicroArtefact},
					Index:  []BulkProperty{{Key: "sha256", Value: []byte(fmt.Sprintf("v-%03d", i))}},
				}); err != nil {
					return err
				}
			}
			return nil
		},
		func(w *BulkEdgeWriter) error { return nil })
	if err != nil {
		t.Fatalf("BulkLoad: %v", err)
	}
	defer g.Close()
	ids, err := g.NodesByProperty("sha256", []byte("v-025"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("the unique value resolves to %d ids, want 1", len(ids))
	}
}

// A failure at each durability step leaves the directory recoverable.
//
// The plan asks for crash injection between the build and the rename, and the
// step hooks are what the compaction path already uses to express it: each named
// point is where a process could stop, and a load goes through the same commit
// as a compaction so it goes through the same points. What must hold at every
// one of them is the same thing: no image, no temp file, and a directory that
// opens as the empty store it was.
//
// The rename is the boundary. Before it the load has written nothing anyone can
// see; after it the image is installed and the log retire is what recovery
// finishes. A load has an empty log, so there is nothing for recovery to replay
// and the post-rename failures are not modelled here -- what would be left is
// the loaded store, which is a success with a failed cleanup and not a crash to
// recover from.
func TestBulkLoad_AFailureAtEachStepLeavesAnEmptyStore(t *testing.T) {
	boom := errors.New("the process stopped here")
	steps := []string{compactStepSerialise, compactStepWriteTmp, compactStepSyncTmp,
		compactStepDrainWAL, compactStepRename}

	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			s, dir := openFresh(t)
			bulkDeclare(t, s)
			s.compactStepHook = func(got string) error {
				if got == step {
					return boom
				}
				return nil
			}
			fx := newBulkFixture(60)
			_, err := s.BulkLoad(
				func(w *BulkNodeWriter) error {
					for i := range fx.nodes {
						if _, err := w.AddNode(fx.nodes[i]); err != nil {
							return err
						}
					}
					return nil
				},
				func(w *BulkEdgeWriter) error {
					for i := range fx.edges {
						if _, err := w.AddEdge(fx.edges[i]); err != nil {
							return err
						}
					}
					return nil
				})
			if !errors.Is(err, boom) {
				t.Fatalf("a failure at %s returned %v, want the injected error", step, err)
			}
			for _, name := range []string{csrFileName, csrFileName + ".tmp"} {
				if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
					t.Fatalf("a failure at %s left %s behind: %v", step, name, err)
				}
			}

			// The directory must still open, and must still be loadable: a store
			// that survived the failure but could no longer be bulk-loaded would
			// have kept something the refusal will find.
			again, err := Open(dir)
			if err != nil {
				t.Fatalf("the store does not reopen after a failure at %s: %v", step, err)
			}
			loaded := loadInBulk(t, again, fx)
			defer loaded.Close()
			if st := loaded.StorageStats(); st.CSRNodes != len(fx.nodes) {
				t.Fatalf("the retried load holds %d nodes, want %d", st.CSRNodes, len(fx.nodes))
			}
		})
	}
}

// A caller that ignores an AddNode failure still fails the load.
//
// AddNode takes an identifier before it can fail, so a record it rejects has
// already consumed one, and its index entries may have been checked halfway. A
// caller treating the error as "skip this record" would be continuing from a state
// nothing can repair -- so the failure is latched and the load ends, whatever the
// caller does next.
func TestBulkLoad_AnIgnoredFailureStillFailsTheLoad(t *testing.T) {
	s, dir := openFresh(t)
	_, err := s.BulkLoad(
		func(w *BulkNodeWriter) error {
			// The first is refused for having no labels, and the error is thrown
			// away exactly as a careless caller would.
			_, _ = w.AddNode(BulkNode{Properties: []byte("no labels")})
			for i := 0; i < 5; i++ {
				if _, err := w.AddNode(BulkNode{
					Labels: []store.NodeType{store.NodeTypeMicroArtefact},
				}); err == nil {
					t.Error("a record was accepted after a refused one; the failure did not latch")
				}
			}
			return nil
		},
		func(w *BulkEdgeWriter) error { return nil })
	if !errors.Is(err, store.ErrNoLabels) {
		t.Fatalf("BulkLoad returned %v, want the refusal the caller ignored", err)
	}
	if _, err := os.Stat(filepath.Join(dir, csrFileName)); !os.IsNotExist(err) {
		t.Fatalf("an ignored refusal still installed an image: %v", err)
	}
}
