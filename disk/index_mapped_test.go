package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// Opening a store whose property index is in the image.
//
// csr_v9_test.go judges the image; this judges the store that opens one. The
// question every test here asks is the same one: does a store reading its index
// in place answer what a store that rebuilt the same index in the heap answers.
// Not "does the mapped path work" — both paths are reachable from one file by one
// option, so the resident arm is the oracle and it is built from the same bytes.

// v9Store builds a store, compacts it, rewrites its image with the index as GPIX
// and GPIR, and returns the directory.
//
// Rewriting rather than compacting into v9 is not a shortcut: nothing in the
// engine asks for a mapped index yet, deliberately, because the default is a
// separate change with its own measurement. What the rewrite produces is the same
// image a compaction would — the same graph, the same entries in the same order,
// one encoding of the index instead of the other — so a store opening it takes
// every path a store opening a compaction's output will take.
//
// The declarations are part of the fixture because they are what makes the two
// arms differ: an ordered key is answered from the base by a walk, and a
// composite is the one resident structure a mapped index still has to build.
func v9Store(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.DeclareOrderedNodeProperty("seq"); err != nil {
		t.Fatalf("DeclareOrderedNodeProperty: %v", err)
	}
	if err := s.DeclareCompositeNodeProperties([]string{"bucket", "shard"}); err != nil {
		t.Fatalf("DeclareCompositeNodeProperties: %v", err)
	}
	for i := 0; i < 48; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		// Three keys per node: one all-distinct, one low-cardinality, and two that
		// are a composite together. That is the brief's index shape in miniature.
		props := [][2]string{
			{"seq", fmt.Sprintf("%04d", i)},
			{"bucket", fmt.Sprintf("b%d", i%4)},
			{"shard", fmt.Sprintf("s%d", i%3)},
		}
		for _, kv := range props {
			if err := s.IndexNodeProperty(id, kv[0], []byte(kv[1])); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
		}
		if i > 0 {
			e := addEdgeD(t, s, store.NodeID(i), id, store.EdgeTypeTemporal)
			if err := s.IndexEdgeProperty(e, "rel", []byte(fmt.Sprintf("r%d", i%5))); err != nil {
				t.Fatalf("IndexEdgeProperty: %v", err)
			}
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rewriteImageAsV9(t, dir)
	return dir
}

// rewriteImageAsV9 re-encodes a store's image with the index in it.
//
// The entries come out of the image's own GIDX and go back in through the same
// fixture the encoder's tests use, so what changes is the encoding and nothing
// else. The digest and the roots are recomputed by the writer, as they are for
// any image.
func rewriteImageAsV9(t testing.TB, dir string) {
	t.Helper()
	path := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	csr, section, err := deserialiseCSR(data)
	if err != nil {
		t.Fatalf("parse image: %v", err)
	}
	if section == nil {
		t.Fatal("the compacted image carries no property index")
	}
	f := newGPIXFixture()
	for _, e := range section.NodeProps {
		f.add(gpixKindNode, e.Key, string(e.Value), uint64(e.ID))
	}
	for _, e := range section.EdgeProps {
		f.add(gpixKindEdge, e.Key, string(e.Value), uint64(e.ID))
	}
	src := f.source(t.TempDir(), 0, 0)
	out, err := csr.SerialiseWithPayload(csrPayload{
		MappedIndex:       &src,
		OrderedNodeKeys:   section.OrderedNodeKeys,
		OrderedEdgeKeys:   section.OrderedEdgeKeys,
		CompositeNodeKeys: section.CompositeNodeKeys,
		CompositeEdgeKeys: section.CompositeEdgeKeys,
		WithSnapshotRoots: true,
	})
	if err != nil {
		t.Fatalf("re-serialise as v9: %v", err)
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
}

// indexAnswers is everything a store can be asked about its property index,
// collected so two stores can be compared field by field.
type indexAnswers struct {
	Mode         string
	NodeEntries  int
	EdgeEntries  int
	ByProperty   map[string][]store.NodeID
	EdgeProperty map[string][]store.EdgeID
	Range        []store.NodeID
	Composite    []store.NodeID
	EntriesOf    map[store.NodeID][]string
	Keys         []string
}

func askIndex(t *testing.T, s *Store) indexAnswers {
	t.Helper()
	a := indexAnswers{
		ByProperty:   map[string][]store.NodeID{},
		EdgeProperty: map[string][]store.EdgeID{},
		EntriesOf:    map[store.NodeID][]string{},
	}
	a.Mode = s.StorageStats().IndexMode
	st := s.StorageStats()
	a.NodeEntries, a.EdgeEntries = st.PropertyNodeEntries, st.PropertyEdgeEntries

	for i := 0; i < 48; i++ {
		for _, kv := range [][2]string{
			{"seq", fmt.Sprintf("%04d", i)},
			{"bucket", fmt.Sprintf("b%d", i%4)},
			{"shard", fmt.Sprintf("s%d", i%3)},
		} {
			ids, err := s.NodesByProperty(kv[0], []byte(kv[1]))
			if err != nil {
				t.Fatalf("NodesByProperty(%s=%s): %v", kv[0], kv[1], err)
			}
			slices.Sort(ids)
			a.ByProperty[kv[0]+"="+kv[1]] = ids
		}
	}
	// A value nothing holds, which must be an empty answer and not an error.
	if ids, err := s.NodesByProperty("bucket", []byte("b99")); err != nil {
		t.Fatalf("NodesByProperty(miss): %v", err)
	} else {
		a.ByProperty["bucket=b99"] = ids
	}
	for i := 0; i < 5; i++ {
		ids, err := s.EdgesByProperty("rel", []byte(fmt.Sprintf("r%d", i)))
		if err != nil {
			t.Fatalf("EdgesByProperty: %v", err)
		}
		slices.Sort(ids)
		a.EdgeProperty[fmt.Sprintf("r%d", i)] = ids
	}

	// The ordered key, through the planner, so the declaration is what answers it
	// rather than a scan that would agree by accident.
	rng, err := s.QueryNodeIDs(store.NodeQuery{
		Filters: []store.PropertyFilter{{
			Key: "seq", Op: store.PropertyOpBetweenInclusive,
			Value: []byte("0010"), ValueUpper: []byte("0019"),
		}},
	})
	if err != nil {
		t.Fatalf("range query: %v", err)
	}
	slices.Sort(rng)
	a.Range = rng

	// The composite, which under a mapped index exists only because declaring it
	// backfilled from the base.
	comp, err := s.QueryNodeIDs(store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")},
			{Key: "shard", Op: store.PropertyOpEqual, Value: []byte("s1")},
		},
	})
	if err != nil {
		t.Fatalf("composite query: %v", err)
	}
	slices.Sort(comp)
	a.Composite = comp

	for _, id := range []store.NodeID{1, 7, 25, 48} {
		var out []string
		for _, e := range s.index().NodeEntriesOf(id) {
			out = append(out, e.Key+"="+string(e.Value))
		}
		slices.Sort(out)
		a.EntriesOf[id] = out
	}
	a.Keys = s.index().OrderedNodeKeys()
	return a
}

func requireSameAnswers(t *testing.T, what string, want, got indexAnswers) {
	t.Helper()
	for k, w := range want.ByProperty {
		if g := got.ByProperty[k]; !slices.Equal(w, g) {
			t.Errorf("%s: NodesByProperty(%s) = %v, want %v", what, k, g, w)
		}
	}
	for k, w := range want.EdgeProperty {
		if g := got.EdgeProperty[k]; !slices.Equal(w, g) {
			t.Errorf("%s: EdgesByProperty(%s) = %v, want %v", what, k, g, w)
		}
	}
	if !slices.Equal(want.Range, got.Range) {
		t.Errorf("%s: range query = %v, want %v", what, got.Range, want.Range)
	}
	if !slices.Equal(want.Composite, got.Composite) {
		t.Errorf("%s: composite query = %v, want %v", what, got.Composite, want.Composite)
	}
	for id, w := range want.EntriesOf {
		if g := got.EntriesOf[id]; !slices.Equal(w, g) {
			t.Errorf("%s: NodeEntriesOf(%d) = %v, want %v", what, id, g, w)
		}
	}
	if !slices.Equal(want.Keys, got.Keys) {
		t.Errorf("%s: ordered keys = %v, want %v", what, got.Keys, want.Keys)
	}
}

// TestIndexMapped_AnswersWhatTheResidentIndexAnswers is the landing's whole
// claim, and the resident arm over the same file is the oracle.
//
// Both stores open the one image. One reads its index where it lies and the other
// walks it into the heap, and every question below is asked of both: point
// lookups on three keys of different cardinality, a miss, edge properties, a
// declared range through the planner, a composite, and the reverse direction.
// Nothing here is asserted against a value written into the test — the two arms
// are compared against each other, because a hand-written expectation would only
// prove the test's arithmetic.
func TestIndexMapped_AnswersWhatTheResidentIndexAnswers(t *testing.T) {
	dir := v9Store(t)

	resident, err := OpenWithOptions(dir, Options{IndexMode: IndexResident})
	if err != nil {
		t.Fatalf("open resident: %v", err)
	}
	want := askIndex(t, resident)
	if err := resident.Close(); err != nil {
		t.Fatalf("close resident: %v", err)
	}

	mapped, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("open mapped: %v", err)
	}
	defer mapped.Close()
	got := askIndex(t, mapped)

	if want.Mode != IndexResident.String() {
		t.Errorf("the resident arm reports IndexMode %q", want.Mode)
	}
	if got.Mode != IndexMapped.String() {
		t.Fatalf("the mapped arm reports IndexMode %q, so it did not attach a base", got.Mode)
	}
	requireSameAnswers(t, "mapped vs resident", want, got)

	// Entry counts are an upper bound under a base, so they may exceed the
	// resident figure — but not by being zero, which is what a store that
	// attached a base and then forgot it would report.
	if got.NodeEntries < want.NodeEntries || got.EdgeEntries < want.EdgeEntries {
		t.Errorf("entry counts under a base are %d/%d, below the resident %d/%d",
			got.NodeEntries, got.EdgeEntries, want.NodeEntries, want.EdgeEntries)
	}
	if err := mapped.VerifyIndexes(); err != nil {
		t.Errorf("VerifyIndexes on a mapped index: %v", err)
	}
}

// TestIndexMapped_TheDeltaStillWorksOverABase carries the store past the open
// that attached the base.
//
// A base is the index as of the last compaction and nothing else; everything
// written since is the delta and everything removed since is a retraction. Those
// three terms are index/union.go's arithmetic, tested there over a fake base.
// What is tested here is that a real store reaches them: a write after a mapped
// open has to appear, a delete has to disappear, and a value the base and the
// delta both hold has to appear once.
func TestIndexMapped_TheDeltaStillWorksOverABase(t *testing.T) {
	dir := v9Store(t)
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if s.StorageStats().IndexMode != IndexMapped.String() {
		t.Fatal("no base was attached")
	}

	// A new node under a value the base already carries.
	fresh := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.IndexNodeProperty(fresh, "bucket", []byte("b1")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	// And the same triple twice, which must not double the posting.
	if err := s.IndexNodeProperty(fresh, "bucket", []byte("b1")); err != nil {
		t.Fatalf("IndexNodeProperty again: %v", err)
	}
	ids, err := s.NodesByProperty("bucket", []byte("b1"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if !slices.Contains(ids, fresh) {
		t.Errorf("the node written after the open is absent from %v", ids)
	}
	if n := len(slices.Compact(slices.Clone(ids))); n != len(ids) {
		t.Errorf("postings %v carry a duplicate", ids)
	}

	// A base id removed after the open: retracted, and gone from both directions.
	victim := ids[0]
	if victim == fresh {
		victim = ids[1]
	}
	if err := s.DeleteNode(victim); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	after, err := s.NodesByProperty("bucket", []byte("b1"))
	if err != nil {
		t.Fatalf("NodesByProperty after delete: %v", err)
	}
	if slices.Contains(after, victim) {
		t.Errorf("the deleted node %d is still indexed: %v", victim, after)
	}
	if e := s.index().NodeEntriesOf(victim); len(e) != 0 {
		t.Errorf("the deleted node still has entries %v", e)
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Errorf("VerifyIndexes after a write and a delete over a base: %v", err)
	}
}

// TestIndexMapped_FallsBackOnAHeapImage checks the one condition that is not the
// caller's own choice.
//
// An index read in place addresses the image. Under ImageHeap the image is a
// buffer the parse copies what it needs out of, so a base over it would keep the
// whole file in anonymous memory to save part of it — the opposite of the trade.
// So the store rebuilds the index, says so in its statistics, and emits the
// metric, because a fallback that cost this much and said nothing would be a
// memory regression with no symptom.
func TestIndexMapped_FallsBackOnAHeapImage(t *testing.T) {
	dir := v9Store(t)
	var kinds []store.MetricKind
	s, err := OpenWithOptions(dir, Options{
		IndexMode: IndexMapped,
		ImageMode: ImageHeap,
		Metrics: store.MetricsFunc(func(m store.Metric) {
			kinds = append(kinds, m.Kind)
		}),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if got := s.StorageStats().IndexMode; got != IndexResident.String() {
		t.Errorf("IndexMode = %q over a heap image, want %q", got, IndexResident.String())
	}
	if !slices.Contains(kinds, store.MetricIndexFallback) {
		t.Errorf("no MetricIndexFallback among %v", kinds)
	}
	// And it is a fallback, not a failure: the index is all there.
	ids, err := s.NodesByProperty("bucket", []byte("b0"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(ids) != 12 {
		t.Errorf("bucket=b0 has %d nodes, want 12", len(ids))
	}
}

// TestIndexResident_EmitsNoFallbackForAModeItWasGiven separates the two silences.
//
// A store told to keep its index resident is not falling back to anything, and a
// store opening a file that carries no mapped index is not either. Reporting
// either as a fallback would make the metric useless for the case it exists for,
// which is a store that asked for the mapped index and did not get it.
func TestIndexResident_EmitsNoFallbackForAModeItWasGiven(t *testing.T) {
	for _, tc := range []struct {
		name string
		v9   bool
		mode IndexMode
	}{
		{"asked for resident over a v9 image", true, IndexResident},
		{"asked for mapped over a v8 image", false, IndexMapped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dir string
			if tc.v9 {
				dir = v9Store(t)
			} else {
				dir = v9Store(t)
				// Undo the rewrite: a plain compaction of the same store is the v8
				// arrangement, which is what every existing file is.
				plain, err := Open(dir)
				if err != nil {
					t.Fatalf("open to recompact: %v", err)
				}
				if err := plain.Compact(); err != nil {
					t.Fatalf("recompact: %v", err)
				}
				if err := plain.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
			var fallbacks int
			s, err := OpenWithOptions(dir, Options{
				IndexMode: tc.mode,
				Metrics: store.MetricsFunc(func(m store.Metric) {
					if m.Kind == store.MetricIndexFallback {
						fallbacks++
					}
				}),
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer s.Close()
			if fallbacks != 0 {
				t.Errorf("%d index fallbacks reported", fallbacks)
			}
			if got := s.StorageStats().IndexMode; got != IndexResident.String() {
				t.Errorf("IndexMode = %q, want %q", got, IndexResident.String())
			}
		})
	}
}

// TestIndexMapped_RefusesADamagedRun is the guard on the fault check at open.
//
// Under a mapped index the composites are the one thing the open reads out of the
// base, and a run that will not decode is damage to the file. A store that came
// up on it would serve a composite index missing whatever that run held — a wrong
// answer, silently, which is the outcome the Base interface refuses to produce.
//
// The corruption is a value's length prefix, overwritten to claim more bytes than
// its run holds. That passes parseGPIX, which deliberately bounds regions and not
// their contents, and fails at the read — which is exactly the class of damage
// this check exists for.
func TestIndexMapped_RefusesADamagedRun(t *testing.T) {
	dir := v9Store(t)
	path := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}

	// "b3" is a bucket value, so it is in the runs of a composite member key. The
	// run holds its length and then its bytes, and that pair occurs once.
	needle := []byte{2, 0, 0, 0, 'b', '3'}
	at := bytes.Index(data, needle)
	if at < 0 || bytes.Index(data[at+1:], needle) >= 0 {
		t.Fatalf("the value's run prefix occurs %d times, so the corruption is not aimed",
			bytes.Count(data, needle))
	}
	binary.LittleEndian.PutUint32(data[at:], 1<<30)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}

	// Both arms refuse, for different reasons in the same class. The mapped arm
	// reads the run while filling a composite; the resident arm reads every run
	// there is, because rebuilding the index is reading all of them.
	for _, mode := range []IndexMode{IndexMapped, IndexResident} {
		t.Run(mode.String(), func(t *testing.T) {
			s, err := OpenWithOptions(dir, Options{IndexMode: mode})
			if err == nil {
				s.Close()
				t.Fatal("a store opened on an image whose index cannot be read")
			}
			if !bytes.Contains([]byte(err.Error()), []byte("gpix")) {
				t.Errorf("err = %v, want it to name the section it could not read", err)
			}
		})
	}
}

// TestReadMappedIndexSections_RefusesHalfAnIndex covers the loader's own
// arithmetic without going through a store.
//
// The two directions answer different questions — forward is "which entities hold
// this value", reverse is "what does this entity hold" — and a reader with one of
// them would answer some queries and silently miss others. The removal path needs
// the reverse direction, so half an index is refused at load rather than at the
// first delete.
func TestReadMappedIndexSections_RefusesHalfAnIndex(t *testing.T) {
	g, _, v9 := v9Fixture(t, 24)
	data, err := g.SerialiseWithPayload(v9)
	if err != nil {
		t.Fatalf("serialise: %v", err)
	}
	full := sectionsOf(t, data)

	both, ok, err := readMappedIndexSections(data, full)
	if err != nil || !ok || both == nil {
		t.Fatalf("the whole index: base=%v ok=%v err=%v", both, ok, err)
	}

	for _, drop := range []string{csrSectionMappedIndex, csrSectionMappedReverse} {
		t.Run("without "+drop, func(t *testing.T) {
			var half []csrSection
			for _, s := range full {
				if s.Magic != drop {
					half = append(half, s)
				}
			}
			b, ok, err := readMappedIndexSections(data, half)
			if err == nil {
				t.Fatalf("half an index was accepted as base %v", b)
			}
			if !ok {
				t.Error("ok = false, so the file would be read as one carrying no mapped index at all")
			}
			if !bytes.Contains([]byte(err.Error()), []byte(drop)) {
				t.Errorf("err = %v, want it to name the missing %s", err, drop)
			}
		})
	}

	// And a file carrying neither is not a v9 image, which is not an error.
	var none []csrSection
	for _, s := range full {
		if s.Magic != csrSectionMappedIndex && s.Magic != csrSectionMappedReverse {
			none = append(none, s)
		}
	}
	if b, ok, err := readMappedIndexSections(data, none); ok || b != nil || err != nil {
		t.Errorf("no sections: base=%v ok=%v err=%v, want nil/false/nil", b, ok, err)
	}
}

// TestDeserialise_ReadsV9WithoutGIDX pins the dispatch.
//
// A v9 image carries no GIDX, so the loader must not go looking for one — the v6
// index-offset field is zero in that file and the GIDX reader rejects a zero
// offset. It must also not hand back entries: the whole point is that the entries
// stay in the file.
func TestDeserialise_ReadsV9WithoutGIDX(t *testing.T) {
	g, _, v9 := v9Fixture(t, 24)
	data, err := g.SerialiseWithPayload(v9)
	if err != nil {
		t.Fatalf("serialise: %v", err)
	}
	_, section, err := deserialiseCSR(data)
	if err != nil {
		t.Fatalf("deserialiseCSR on a v9 image: %v", err)
	}
	if section == nil || section.Base == nil {
		t.Fatal("a v9 image parsed to no base")
	}
	if len(section.NodeProps) != 0 || len(section.EdgeProps) != 0 {
		t.Errorf("the loader materialised %d+%d entries out of a mapped index",
			len(section.NodeProps), len(section.EdgeProps))
	}
	// The base answers, which is what says the sections were resolved against the
	// right bytes rather than merely bounded.
	if keys := section.Base.Keys(index.NodeKind); len(keys) == 0 {
		t.Error("the base carries no node keys")
	}
}

// TestInspectCSR_KnowsTheMappedIndexSections is the operator-facing half of
// understanding a section.
//
// InspectCSR reports each section as known or not, and an operator diagnosing a
// file reads that before anything else. A build that reads the two sections and
// still reports them as unknown would say the file was written by a newer version
// than itself.
func TestInspectCSR_KnowsTheMappedIndexSections(t *testing.T) {
	dir := v9Store(t)
	info, err := InspectCSR(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatalf("InspectCSR: %v", err)
	}
	if info.Version != csrVersionMappedIndex {
		t.Errorf("version = %d, want %d", info.Version, csrVersionMappedIndex)
	}
	seen := 0
	for _, s := range info.Sections {
		if s.Magic != csrSectionMappedIndex && s.Magic != csrSectionMappedReverse {
			continue
		}
		seen++
		if !s.Known {
			t.Errorf("%s is reported as unknown by a build that reads it", s.Magic)
		}
		if !s.Critical {
			t.Errorf("%s is reported as optional", s.Magic)
		}
	}
	if seen != 2 {
		t.Errorf("InspectCSR listed %d of the two mapped-index sections", seen)
	}
}
