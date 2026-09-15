package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

// v9Store builds a store, compacts it, and returns the directory. Its image
// carries the index as GPIX and GPIR, which is what the default writes.
//
// It used to build a v8 image and re-encode its GIDX through the encoder's own
// fixture, because nothing in the engine asked for a mapped index. The flip
// deleted the need and improved the fixture at the same time: the file under test
// is now the file a compaction actually produces, entries filtered against the
// image and all, rather than one assembled to look like it.
//
// The declarations are part of the fixture because they are what makes the two
// arms differ: an ordered key is answered from the base by a walk, and a
// composite is the one resident structure a mapped index still has to build.
func v9Store(t *testing.T) string {
	return indexModeStore(t, IndexMapped)
}

// v8Store is v9Store's image in the older encoding: GIDX, no mapped sections,
// which is what every file written before the flip is and what IndexResident goes
// on writing.
func v8Store(t *testing.T) string {
	return indexModeStore(t, IndexResident)
}

func indexModeStore(t *testing.T, mode IndexMode) string {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{IndexMode: mode})
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
	// A handful of entities of a second type, sharing the same keys and values.
	//
	// Their purpose is the planner's, not the index's: a residual filter is applied
	// by probing when there are fewer candidates than the filter's own set has
	// members, and every entity above is of one type — so narrowing by that type
	// yields 48 candidates against sets of at most 13 and the planner never probes.
	// Three of a rare type yields three, which does. Without these the probe path is
	// unreachable from this fixture and the bug the flip found would still be
	// invisible here.
	for i := 0; i < 3; i++ {
		id := addNodeD(t, s, store.NodeTypeTag)
		for _, kv := range [][2]string{
			{"seq", fmt.Sprintf("%04d", 100+i)},
			{"bucket", fmt.Sprintf("b%d", i%4)},
			{"shard", fmt.Sprintf("s%d", i%3)},
		} {
			if err := s.IndexNodeProperty(id, kv[0], []byte(kv[1])); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
		}
		e := addEdgeD(t, s, store.NodeID(1+i), id, store.EdgeTypeReuse)
		if err := s.IndexEdgeProperty(e, "rel", []byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatalf("IndexEdgeProperty: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
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
	Narrowed     map[string][]store.NodeID
	NarrowedEdge []store.EdgeID
	EntriesOf    map[store.NodeID][]string
	Keys         []string
}

func askIndex(t *testing.T, s *Store) indexAnswers {
	t.Helper()
	a := indexAnswers{
		ByProperty:   map[string][]store.NodeID{},
		EdgeProperty: map[string][]store.EdgeID{},
		EntriesOf:    map[store.NodeID][]string{},
		Narrowed:     map[string][]store.NodeID{},
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
	// Every filter shape, each narrowed by a type so the planner takes the probe
	// path: a candidate set arrives from the label index and the filter is asked
	// about one entity at a time, through the *reverse* direction.
	//
	// That is a different question from any of the above, and it is the one the
	// flip found unanswered. The delta's reverse direction knows only what has
	// been written since the last compaction, so under a base a probe that
	// consulted it alone said "no entry under this key" about every entity in the
	// image, and every type-narrowed property query came back empty. Not wrong —
	// empty, which is the failure this whole index is meant to make impossible.
	//
	// The lesson is the shape of the oracle rather than the bug: comparing the two
	// arms through the index's own read methods compared them on the paths that
	// had already been thought about. Reaching them through the planner is what
	// asks the question nobody had asked.
	for name, f := range map[string]store.PropertyFilter{
		"equal":  {Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")},
		"prefix": {Key: "seq", Op: store.PropertyOpPrefix, Value: []byte("010")},
		"gte":    {Key: "seq", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("0040")},
		"lt":     {Key: "shard", Op: store.PropertyOpLessThan, Value: []byte("s2")},
		"between": {Key: "seq", Op: store.PropertyOpBetweenInclusive,
			Value: []byte("0100"), ValueUpper: []byte("0101")},
		"contains": {Key: "bucket", Op: store.PropertyOpContains, Value: []byte("1")},
		"miss":     {Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b99")},
	} {
		ids, err := s.QueryNodeIDs(store.NodeQuery{
			Types:   []store.NodeType{store.NodeTypeTag},
			Filters: []store.PropertyFilter{f},
		})
		if err != nil {
			t.Fatalf("narrowed query %s: %v", name, err)
		}
		slices.Sort(ids)
		a.Narrowed[name] = ids
	}
	// And the edge half of the same path, which has its own probe.
	eids, err := s.QueryEdgeIDs(store.EdgeQuery{
		Types:   []store.EdgeType{store.EdgeTypeReuse},
		Filters: []store.PropertyFilter{{Key: "rel", Op: store.PropertyOpEqual, Value: []byte("r1")}},
	})
	if err != nil {
		t.Fatalf("narrowed edge query: %v", err)
	}
	slices.Sort(eids)
	a.NarrowedEdge = eids

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
	for k, w := range want.Narrowed {
		if g := got.Narrowed[k]; !slices.Equal(w, g) {
			t.Errorf("%s: type-narrowed %s query = %v, want %v", what, k, g, w)
		}
	}
	if !slices.Equal(want.NarrowedEdge, got.NarrowedEdge) {
		t.Errorf("%s: type-narrowed edge query = %v, want %v", what, got.NarrowedEdge, want.NarrowedEdge)
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
	// Twelve of the forty-eight, plus one of the three rare-type nodes.
	if len(ids) != 13 {
		t.Errorf("bucket=b0 has %d nodes, want 13", len(ids))
	}
}

// TestIndexResident_EmitsNoFallbackForAModeItWasGiven is the remaining silence.
//
// A store told to keep its index resident is not falling back to anything, and
// reporting it would make the metric useless for the case it exists for.
//
// This used to cover a second case — "asked for mapped over a v8 image" — and
// assert the same silence, on the reasoning that a file with no mapped index is
// not something to fall back from. v0.8.0 reversed that; the reasoning was about
// where the cause lay rather than about what the caller was paying, and that case
// is now TestIndexMapped_ReportsAnImageThatCannotSupplyOne. The two are worth
// reading together, because this file is where the line between them is drawn.
func TestIndexResident_EmitsNoFallbackForAModeItWasGiven(t *testing.T) {
	dir := v9Store(t)
	var fallbacks int
	s, err := OpenWithOptions(dir, Options{
		IndexMode: IndexResident,
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
		t.Errorf("%d index fallbacks reported to a caller who asked for a resident index",
			fallbacks)
	}
	if got := s.StorageStats().IndexMode; got != IndexResident.String() {
		t.Errorf("IndexMode = %q, want %q", got, IndexResident.String())
	}
}

// TestIndexMapped_ReportsAnImageThatCannotSupplyOne is G9: the most expensive
// thing that can happen at an open, which until v0.8.0 had no symptom.
//
// A v8 image loads its property index entry by entry whatever IndexMode asks for,
// because GIDX is the only form it has — about sevenfold on the open, and then
// the entries sit in the heap at roughly a hundred bytes each, which is the whole
// cost IndexMapped exists to remove. Every field a caller could look at said
// something reassuring or nothing: IndexMode said "resident" with no reason
// attached, ImageMode said "mapped" because the image maps perfectly well, and no
// metric fired.
//
// Three assertions, and the third is the one that makes the finding actionable
// rather than merely visible: the reason has to name the remedy, because a caller
// who learns their index is resident and cannot learn that one compaction fixes
// it has been told the half of it that does not help.
func TestIndexMapped_ReportsAnImageThatCannotSupplyOne(t *testing.T) {
	dir := v8Store(t)
	// Nothing more to do: an IndexResident compaction wrote GIDX, which is the
	// arrangement every file written before the flip is in.
	plain, err := OpenWithOptions(dir, Options{IndexMode: IndexResident})
	if err != nil {
		t.Fatalf("open to recompact: %v", err)
	}
	if err := plain.Compact(); err != nil {
		t.Fatalf("recompact: %v", err)
	}
	if err := plain.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var reasons []string
	s, err := OpenWithOptions(dir, Options{
		IndexMode: IndexMapped,
		Metrics: store.MetricsFunc(func(m store.Metric) {
			if m.Kind == store.MetricIndexFallback && m.Err != nil {
				reasons = append(reasons, m.Err.Error())
			}
		}),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	st := s.StorageStats()
	if st.ImageVersion != csrVersionSectioned {
		t.Errorf("ImageVersion = %d, want %d", st.ImageVersion, csrVersionSectioned)
	}
	if st.IndexOnDisk != indexOnDiskEntries {
		t.Errorf("IndexOnDisk = %q, want %q", st.IndexOnDisk, indexOnDiskEntries)
	}
	if st.IndexMode != IndexResident.String() {
		t.Errorf("IndexMode = %q, want %q", st.IndexMode, IndexResident.String())
	}
	if len(reasons) != 1 {
		t.Fatalf("%d index fallbacks reported, want 1: %v", len(reasons), reasons)
	}
	if !strings.Contains(reasons[0], "migrate -to 9") {
		t.Errorf("the reason does not name the remedy: %q", reasons[0])
	}
}

// TestStorageStats_ImageVersionDistinguishesTheTwoWritableFormats is the other
// half of G9: the number itself, which a running store could not report at all.
//
// Both images here are built by the same fixture and differ only in the mode
// their compaction ran under, so the field is being read against a difference the
// test created deliberately rather than against a constant.
func TestStorageStats_ImageVersionDistinguishesTheTwoWritableFormats(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dir         func(*testing.T) string
		version     uint16
		indexOnDisk string
	}{
		{"v9", v9Store, csrVersionMappedIndex, indexOnDiskMapped},
		{"v8", v8Store, csrVersionSectioned, indexOnDiskEntries},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenWithOptions(tc.dir(t), Options{IndexMode: IndexResident})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer s.Close()
			st := s.StorageStats()
			if st.ImageVersion != tc.version {
				t.Errorf("ImageVersion = %d, want %d", st.ImageVersion, tc.version)
			}
			if st.IndexOnDisk != tc.indexOnDisk {
				t.Errorf("IndexOnDisk = %q, want %q", st.IndexOnDisk, tc.indexOnDisk)
			}
		})
	}
}

// TestStorageStats_NoImageReportsNoVersion keeps the zero honest. A store that
// has never compacted has no file to have a version, and reporting the version
// this build writes would be a claim about a file that does not exist.
func TestStorageStats_NoImageReportsNoVersion(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	addNodeD(t, s, store.NodeTypeEvidenceFile)
	st := s.StorageStats()
	if st.ImageVersion != 0 {
		t.Errorf("a store with no image reports version %d", st.ImageVersion)
	}
	if st.IndexOnDisk != "" {
		t.Errorf("a store with no image reports IndexOnDisk %q", st.IndexOnDisk)
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
	if at < 0 || bytes.Contains(data[at+1:], needle) {
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
