package disk

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// The composite index on disk, end to end.
//
// csr_gcpx_test.go covers the format in isolation — what the encoder writes and
// what the parser refuses. These are the store-level facts: that a compaction
// writes the section, that a reopened store answers composite queries out of it
// rather than out of the heap, that it holds less for doing so, and that a reader
// which ignores the section entirely still answers every query correctly.
//
// The last is the compatibility claim the whole design rests on and it is the one
// worth testing hardest, because nothing else in the repo can fail if it is
// wrong: a v0.7.x binary is not here to run.

// gcpxSectionOf returns the image's GCPX body, or nil.
func gcpxSectionOf(t *testing.T, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	sec, ok := findSection(sectionsOf(t, data), csrSectionCompositeIndex)
	if !ok {
		return nil
	}
	return data[sec.Offset : sec.Offset+sec.Length]
}

// TestGCPX_ACompactionWritesTheSection is the presence check every other test
// here depends on.
//
// Without it a green suite would prove only that the fallback still works: every
// composite assertion in this package passes whether the postings are read in
// place or filled from the entries, because both answer the same question. So the
// section's existence is asserted once, explicitly, and the tests below are then
// about what it changes.
func TestGCPX_ACompactionWritesTheSection(t *testing.T) {
	body := gcpxSectionOf(t, v9Store(t))
	if body == nil {
		t.Fatal("a v9 image compacted with a composite declared carries no GCPX")
	}
	sec, err := parseGCPX(body)
	if err != nil {
		t.Fatalf("parseGCPX over the image this build wrote: %v", err)
	}
	c := sec.find(gcpxKindNode, []string{"bucket", "shard"})
	if c == nil {
		t.Fatal("the section carries no postings for the declared composite")
	}
	if c.distinct == 0 || c.entries == 0 {
		t.Errorf("the composite is declared but empty: %d tuples, %d entries", c.distinct, c.entries)
	}
}

// TestGCPX_NoCompositeDeclaredWritesNoSection keeps the section proportional to
// what asked for it.
//
// A store that declares no composite must not pay a section, a directory or a
// footer for the feature. This is the shape most stores are.
func TestGCPX_NoCompositeDeclaredWritesNoSection(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 8; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%d", i%3))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if body := gcpxSectionOf(t, dir); body != nil {
		t.Errorf("a store with no composite declared wrote %d bytes of GCPX", len(body))
	}
}

// TestGCPX_AReaderThatSkipsItAnswersEverything is the compatibility claim, and
// it is the one worth testing hardest.
//
// The whole design rests on a v0.7.x reader meeting an image with GCPX in it,
// skipping a magic it does not know, and answering every composite query
// correctly by filling the composites from the entries. Nothing else in this repo
// can fail if that is wrong — a v0.7.x binary is not here to run — so the claim is
// checked by building the two bases this build can build out of one image: the
// same GPIX and GPIR, once with the composite section and once without.
//
// Without it is exactly what an older reader has, because the decision is made by
// a type assertion in index.carriedComposites and by nothing else: no version
// number is consulted, and a base whose composite section is absent is
// indistinguishable from one written before the section existed.
//
// The two must agree on every answer and disagree on what they hold. Agreement is
// the compatibility claim; the disagreement is the item.
func TestGCPX_AReaderThatSkipsItAnswersEverything(t *testing.T) {
	dir := v9Store(t)
	data, err := os.ReadFile(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	sections := sectionsOf(t, data)

	sec := func(magic string) []byte {
		d, ok := findSection(sections, magic)
		if !ok {
			t.Fatalf("the image carries no %s", magic)
		}
		return data[d.Offset : d.Offset+d.Length]
	}
	fwd, err := parseGPIX(sec(csrSectionMappedIndex))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	rev, err := parseGPIR(sec(csrSectionMappedReverse))
	if err != nil {
		t.Fatalf("parseGPIR: %v", err)
	}
	cmp, err := parseGCPX(sec(csrSectionCompositeIndex))
	if err != nil {
		t.Fatalf("parseGCPX: %v", err)
	}

	inPlace := indexOverBase(t, fwd, rev, cmp)
	filled := indexOverBase(t, fwd, rev, nil)

	// The item: one holds the postings and the other does not.
	//
	// The floor is not zero. A declared composite costs its key names whether or
	// not it holds an entry, and that cost is the declaration rather than the
	// index -- it is there on an empty store too. So the assertion is against
	// that floor, measured rather than written down, and what it says is that the
	// base-backed arm holds its declaration and not one entry.
	floor := declarationOnlyCompositeBytes(t)
	if got := inPlace.CompositeResidentBytes(); got != floor {
		t.Errorf("the composites hold %d resident bytes over a base that carries them, "+
			"want the %d a bare declaration costs", got, floor)
	}
	if got := filled.CompositeResidentBytes(); got <= floor {
		t.Fatalf("the filled arm holds %d bytes against a floor of %d, so the "+
			"comparison is vacuous", got, floor)
	}

	// The claim: they answer identically, over every tuple the fixture can form
	// and several it cannot.
	for bucket := 0; bucket < 5; bucket++ {
		for shard := 0; shard < 4; shard++ {
			bv := fmt.Sprintf("b%d", bucket)
			sv := fmt.Sprintf("s%d", shard)
			got := compositeIDsOfIndex(t, inPlace, bv, sv)
			want := compositeIDsOfIndex(t, filled, bv, sv)
			if !sameNodeIDs(got, want) {
				t.Errorf("(bucket=%s, shard=%s): read in place %v, filled %v", bv, sv, got, want)
			}
		}
	}
}

// indexOverBase builds a property index with the fixture's composite declared
// over one of the two bases.
//
// The declaration comes before the attach, which is the order a store does not
// use — a store declares from GCMP after AttachBase has run. Both orders are
// supported and each has its own skip: AttachBase's is in
// fillCompositesFromBase, the declaration's is in DeclareCompositeNodeKeys. This
// exercises the first; TestGCPX_TheStoreOpenPathSkipsTheFillToo exercises the
// second, through a real open.
func indexOverBase(t *testing.T, fwd *gpixSection, rev *gpirSection, cmp *gcpxSection) *index.PropertyIndex {
	t.Helper()
	b, err := newGPIXBase(fwd, rev, cmp)
	if err != nil {
		t.Fatalf("newGPIXBase: %v", err)
	}
	p := index.NewPropertyIndex()
	if err := p.DeclareCompositeNodeKeys([]string{"bucket", "shard"}); err != nil {
		t.Fatalf("DeclareCompositeNodeKeys: %v", err)
	}
	if err := p.AttachBase(b); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	if f := p.BaseFault(); f != nil {
		t.Fatalf("attaching the base faulted: %v", f)
	}
	return p
}

// declarationOnlyCompositeBytes is what the fixture's composite costs declared
// and empty: the key names, and no entry.
//
// Measured rather than written down, because the per-key figure is index's and a
// constant copied across the package boundary is a constant that drifts.
func declarationOnlyCompositeBytes(t *testing.T) int64 {
	t.Helper()
	p := index.NewPropertyIndex()
	if err := p.DeclareCompositeNodeKeys([]string{"bucket", "shard"}); err != nil {
		t.Fatalf("DeclareCompositeNodeKeys: %v", err)
	}
	return p.CompositeResidentBytes()
}

func compositeIDsOfIndex(t *testing.T, p *index.PropertyIndex, bucket, shard string) []store.NodeID {
	t.Helper()
	m, ok := p.MatchNodeComposite([]store.PropertyFilter{
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte(bucket)},
		{Key: "shard", Op: store.PropertyOpEqual, Value: []byte(shard)},
	}, store.MatchAll)
	if !ok {
		return nil
	}
	ids, _ := p.NodesByComposite(m)
	return ids
}

// TestGCPX_TheStoreOpenPathSkipsTheFillToo is the same skip reached the way a
// store reaches it.
//
// A store attaches its base first and then declares its composites from GCMP, so
// the branch that matters is DeclareCompositeNodeKeys' rather than
// fillCompositesFromBase's. Both exist because both orders are supported, and a
// skip in one of them is not a skip in the other.
func TestGCPX_TheStoreOpenPathSkipsTheFillToo(t *testing.T) {
	s, err := OpenWithOptions(v9Store(t), Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	floor := declarationOnlyCompositeBytes(t)
	if got := s.propIdx.CompositeResidentBytes(); got != floor {
		t.Errorf("an opened store holds %d resident composite bytes over an image that "+
			"carries the postings, want the %d a bare declaration costs", got, floor)
	}
	if ids := compositeIDsOf(t, s, []byte("b1"), []byte("s1")); len(ids) == 0 {
		t.Error("the composite answers nothing, so holding nothing proves nothing")
	}
	if got := s.EstimateResident().Composite; got != floor {
		t.Errorf("ResidentEstimate.Composite reports %d, want the declaration's %d", got, floor)
	}
}

// TestGCPX_WritingToAnEntityTheImageHoldsFilesItsTuples is the hydration case.
//
// It is the one correctness question a base-backed composite raises that a filled
// one does not, and it is easy to get wrong in a way no other test would catch: a
// composite files a tuple only from a complete row, the rows live in the heap, and
// over a base-backed composite an entity the image holds has no row at all. So a
// caller adding one member value to such an entity would create a row holding that
// value and nothing else, find it incomplete, and file nothing — and the new tuple
// would be held by neither side.
//
// The fixture's node 1 carries bucket="b1" and shard="s1". Registering shard="s9"
// without purging must make (b1, s9) findable, because index entries are additive
// and the entity genuinely holds both shards now.
//
// See index.baseSide.hydrateCompositeRow.
func TestGCPX_WritingToAnEntityTheImageHoldsFilesItsTuples(t *testing.T) {
	dir := v9Store(t)
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Find an entity the image holds under a known tuple, so the assertion names
	// a real id rather than assuming one.
	before := compositeIDsOf(t, s, []byte("b1"), []byte("s1"))
	if len(before) == 0 {
		t.Fatal("the fixture holds no entity under (b1, s1)")
	}
	id := before[0]

	if err := s.IndexNodeProperty(id, "shard", []byte("s9")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}

	got := compositeIDsOf(t, s, []byte("b1"), []byte("s9"))
	if !containsNodeID(got, id) {
		t.Errorf("(b1, s9) answers %v, want it to hold %d: a value written to an entity "+
			"the image describes filed no tuple, so the row was never hydrated", got, id)
	}
	// The old tuple is still true — the entity holds both shards — and it comes
	// from the base while the new one comes from the delta.
	if still := compositeIDsOf(t, s, []byte("b1"), []byte("s1")); !containsNodeID(still, id) {
		t.Errorf("(b1, s1) answers %v, want it to still hold %d: adding a value "+
			"removed one the image holds", still, id)
	}
}

func containsNodeID(ids []store.NodeID, want store.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestGCPX_ARetractedEntityLeavesTheComposite is the third term.
//
// The base's postings are immutable, so a deleted entity is hidden by the
// retraction bit rather than removed. A composite read that did not consult the
// bit would resurrect it, which is the failure retract.go's header calls
// unrepresentable — this is where that claim is checked for composites.
func TestGCPX_ARetractedEntityLeavesTheComposite(t *testing.T) {
	dir := v9Store(t)
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	before := compositeIDsOf(t, s, []byte("b1"), []byte("s1"))
	if len(before) == 0 {
		t.Fatal("the fixture holds no entity under (b1, s1)")
	}
	id := before[0]

	if err := s.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	after := compositeIDsOf(t, s, []byte("b1"), []byte("s1"))
	if containsNodeID(after, id) {
		t.Errorf("(b1, s1) still answers %d after it was deleted: the composite read "+
			"does not consult the retraction set", id)
	}
	if len(after) != len(before)-1 {
		t.Errorf("(b1, s1) went from %d ids to %d, want exactly one fewer", len(before), len(after))
	}
}

// TestGCPX_CompactingOverASectionKeepsEveryTuple is the writer's own merge.
//
// A store compacting over an image that already carries GCPX holds most of its
// composites in that image and the rest in the shards, so a writer reading only
// the shards would produce an image missing everything the last one held. This is
// the second compaction, which is the first one that can fail that way.
func TestGCPX_CompactingOverASectionKeepsEveryTuple(t *testing.T) {
	dir := v9Store(t)
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	type tuple struct{ bucket, shard string }
	want := map[tuple][]store.NodeID{}
	for bucket := 0; bucket < 5; bucket++ {
		for shard := 0; shard < 4; shard++ {
			k := tuple{fmt.Sprintf("b%d", bucket), fmt.Sprintf("s%d", shard)}
			want[k] = compositeIDsOf(t, s, []byte(k.bucket), []byte(k.shard))
		}
	}

	// A write since the last compaction, so the merge has both sides to join.
	extra := addNodeD(t, s, store.NodeTypeEvidenceFile)
	for _, kv := range [][2]string{{"bucket", "b1"}, {"shard", "s1"}} {
		if err := s.IndexNodeProperty(extra, kv[0], []byte(kv[1])); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	want[tuple{"b1", "s1"}] = append(want[tuple{"b1", "s1"}], extra)

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// The base the commit installed must still answer the composites in place.
	//
	// This assertion exists because its absence hid a real bug for the length of
	// one test run. A compaction does not install the base it parsed; it installs
	// a mappedIndexBase wrapping it, and that wrapper embeds index.Base as an
	// *interface* -- so it promoted index.Base's methods and silently dropped
	// index.CompositeBase's. Every answer stayed correct, because the fallback is
	// to fill from the entries; the only symptom was that the whole saving
	// evaporated one compaction after it was won, with nothing logged and nothing
	// failing. A capability that degrades into correctness needs a test that
	// asserts the capability, not the correctness.
	if got := s.propIdx.CompositeResidentBytes(); got != declarationOnlyCompositeBytes(t) {
		t.Errorf("after a compaction the composites hold %d resident bytes, want the "+
			"declaration's %d: the base the commit installed does not answer them in place",
			got, declarationOnlyCompositeBytes(t))
	}
	for k, expect := range want {
		got := compositeIDsOf(t, s, []byte(k.bucket), []byte(k.shard))
		if !sameNodeIDs(got, expect) {
			t.Errorf("(%s, %s) answers %v after a compaction over a GCPX image, want %v",
				k.bucket, k.shard, got, expect)
		}
	}
	if body := gcpxSectionOf(t, dir); body == nil {
		t.Error("the second compaction wrote no GCPX")
	}
}

// TestGCPX_TwoCompactionsOfTheSameContentAgree is the determinism contract.
//
// Every section in this image is written so that the same content produces the
// same bytes — it is what makes a digest comparable across machines and what the
// golden corpus rests on. GCPX has two orderings that could break it: the
// directory's, which is sorted by (kind, encoded keys) rather than left in
// declaration order, and the tuple table's.
func TestGCPX_TwoCompactionsOfTheSameContentAgree(t *testing.T) {
	first := gcpxSectionOf(t, v9Store(t))
	second := gcpxSectionOf(t, v9Store(t))
	if first == nil || second == nil {
		t.Fatal("one of the two images carries no GCPX")
	}
	if !bytes.Equal(first, second) {
		t.Errorf("two compactions of the same content wrote %d and %d bytes of GCPX, "+
			"and they differ", len(first), len(second))
	}
}

func compositeIDsOf(t *testing.T, s *Store, bucket, shard []byte) []store.NodeID {
	t.Helper()
	return compositeIDsOfIndex(t, s.propIdx, string(bucket), string(shard))
}

func sameNodeIDs(a, b []store.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
