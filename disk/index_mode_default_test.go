package disk

// The default: a store writes its property index as GPIX and GPIR and reads it
// where it lies.
//
// index_mapped_test.go asks whether the two arms of Options.IndexMode agree.
// These ask the questions that only exist once one of them is the default: which
// encoding a compaction writes, what the other one is for, and what still holds
// about a file whose index moved out of the heap — its Merkle identity, its entry
// counts, and the fact that its roots can still be checked at all.

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// magicsOf names the sections an image carries, in directory order.
func magicsOf(t *testing.T, data []byte) []string {
	t.Helper()
	var out []string
	for _, s := range sectionsOf(t, data) {
		out = append(out, s.Magic)
	}
	return out
}

func versionOf(data []byte) uint16 { return binary.LittleEndian.Uint16(data[4:6]) }

// TestIndexMode_DefaultWritesTheMappedIndex is the flip itself: a store nobody
// configured writes v9 and reads its index in place.
//
// Both halves in one test because either alone is satisfiable by an engine that is
// wrong: a writer whose output nothing reads in place, or a reader with nothing to
// read. The claim is that the default path produces and consumes the same thing.
func TestIndexMode_DefaultWritesTheMappedIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	id := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.IndexNodeProperty(id, "k", []byte("v")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := readCSR(t, dir)
	if v := versionOf(data); v != csrVersionMappedIndex {
		t.Errorf("a default compaction wrote v%d, want v%d", v, csrVersionMappedIndex)
	}
	magics := magicsOf(t, data)
	for _, want := range []string{csrSectionMappedIndex, csrSectionMappedReverse} {
		if !slices.Contains(magics, want) {
			t.Errorf("the image carries %v, without %s", magics, want)
		}
	}
	// And *not* GIDX. The two encodings hold the same entries, so an image with
	// both would carry a second copy of the index — on the store this program is
	// aimed at, a second ~528 MiB — that no reader of either kind would open.
	if slices.Contains(magics, csrIndexSectionMagic) {
		t.Errorf("the image carries both encodings of the index: %v", magics)
	}

	r, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()
	if got := r.StorageStats().IndexMode; got != IndexMapped.String() {
		t.Fatalf("a default reopen holds a %q index", got)
	}
	ids, err := r.NodesByProperty("k", []byte("v"))
	if err != nil || !slices.Equal(ids, []store.NodeID{id}) {
		t.Fatalf("NodesByProperty = %v, %v", ids, err)
	}
}

// TestIndexResident_IsTheWayBackToV8 is the escape the compatibility statement
// promises: no build before this one opens a v9 image, so an operator who has to
// be read by one needs a way to get the older encoding back, and it has to be a
// way that does not lose the index.
//
// `store migrate --to 8` is the convenient form and is not built yet. This is the
// guarantee underneath it — one option and one compaction — and it is the thing
// that must hold whether or not the command exists.
func TestIndexResident_IsTheWayBackToV8(t *testing.T) {
	dir := v9Store(t)
	if v := versionOf(readCSR(t, dir)); v != csrVersionMappedIndex {
		t.Fatalf("the fixture is v%d", v)
	}

	before, err := Open(dir)
	if err != nil {
		t.Fatalf("open the v9 store: %v", err)
	}
	want := askIndex(t, before)
	if err := before.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	down, err := OpenWithOptions(dir, Options{IndexMode: IndexResident})
	if err != nil {
		t.Fatalf("open resident: %v", err)
	}
	if err := down.Compact(); err != nil {
		t.Fatalf("recompact resident: %v", err)
	}
	if err := down.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data := readCSR(t, dir)
	if v := versionOf(data); v != csrVersionSectioned {
		t.Errorf("a resident compaction wrote v%d, want v%d", v, csrVersionSectioned)
	}
	magics := magicsOf(t, data)
	if !slices.Contains(magics, csrIndexSectionMagic) {
		t.Errorf("the downgraded image carries %v, without GIDX", magics)
	}
	for _, gone := range []string{csrSectionMappedIndex, csrSectionMappedReverse} {
		if slices.Contains(magics, gone) {
			t.Errorf("the downgraded image still carries %s: %v", gone, magics)
		}
	}

	// The index survived the round trip. A downgrade that produced an older file
	// holding fewer entries would satisfy every assertion above.
	after, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen the downgraded store: %v", err)
	}
	defer after.Close()
	if got := after.StorageStats().IndexMode; got != IndexResident.String() {
		t.Errorf("a v8 image is held %q", got)
	}
	requireSameAnswers(t, "v9 then downgraded to v8", want, askIndex(t, after))
}

// TestIndexMode_WritesTheEncodingItWasAskedForOverAHeapImage pins the one case
// where the format and the reading part company.
//
// Under ImageHeap the index cannot be read in place — a base pointing into a heap
// buffer would pin the whole image to save a fraction of it — so the store falls
// back to the resident index and says so. What it does *not* do is fall back to the
// older format, because an image is portable and its digest is an identity for its
// contents: two parties compacting the same content under the same options have to
// produce the same bytes, and they would not if the encoding followed a runtime
// capability rather than a configured intent.
func TestIndexMode_WritesTheEncodingItWasAskedForOverAHeapImage(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{ImageMode: ImageHeap})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	id := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.IndexNodeProperty(id, "k", []byte("v")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if v := versionOf(readCSR(t, dir)); v != csrVersionMappedIndex {
		t.Errorf("a heap-image store wrote v%d, want the v%d it asked for", v, csrVersionMappedIndex)
	}

	r, err := OpenWithOptions(dir, Options{ImageMode: ImageHeap})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()
	if got := r.StorageStats().IndexMode; got != IndexResident.String() {
		t.Errorf("IndexMode = %q over a heap image, want %q", got, IndexResident.String())
	}
	ids, err := r.NodesByProperty("k", []byte("v"))
	if err != nil || !slices.Equal(ids, []store.NodeID{id}) {
		t.Fatalf("the fallback lost the index: %v, %v", ids, err)
	}
}

// TestSnapshotRoots_DoNotChangeWithTheIndexEncoding is the compatibility claim
// that matters most and is the easiest to break silently.
//
// A snapshot root is an identity for a store's contents that parties retain
// independently and compare later. The index root is a Merkle tree over the
// property entries, so it depends on the entries and their order and must not
// depend on how they were spelled in the file — otherwise this change would have
// invalidated every root anyone had written down, and the only symptom would be a
// verification failure long after the fact.
func TestSnapshotRoots_DoNotChangeWithTheIndexEncoding(t *testing.T) {
	roots := func(mode IndexMode) SnapshotRoots {
		t.Helper()
		dir := indexModeStore(t, mode)
		csr, _, err := deserialiseCSR(readCSR(t, dir))
		if err != nil {
			t.Fatalf("%v: parse: %v", mode, err)
		}
		r, ok := csr.Roots()
		if !ok {
			t.Fatalf("%v: the image carries no roots", mode)
		}
		return r
	}
	v8, v9 := roots(IndexResident), roots(IndexMapped)
	if v9.IndexRoot != v8.IndexRoot {
		t.Errorf("index root %x under GPIX against %x under GIDX",
			v9.IndexRoot[:8], v8.IndexRoot[:8])
	}
	if v9.Snapshot != v8.Snapshot {
		t.Errorf("snapshot root %x under GPIX against %x under GIDX",
			v9.Snapshot[:8], v8.Snapshot[:8])
	}
}

// TestVerifyCSRRoots_ChecksAMappedIndex: the roots of a v9 image are checkable.
//
// Before the flip the verifier recomputed the index root from GIDX, which a v9
// image does not carry — so it recomputed it from nothing and reported every v9
// image as one whose roots did not describe it. An offline verifier that cannot
// verify the format the engine writes is worse than none: it is a tool that
// reports damage where there is none, which is how operators learn to ignore it.
func TestVerifyCSRRoots_ChecksAMappedIndex(t *testing.T) {
	dir := v9Store(t)
	if err := VerifyCSRRoots(dir); err != nil {
		t.Fatalf("a sound v9 image was reported as broken: %v", err)
	}

	// And it is not vacuous: an entry removed from the image's index has to make
	// the index root disagree. Recompacting after a delete is the honest way to
	// produce a v9 image holding different entries; the roots are recomputed with
	// it, so what this asserts is that the check *notices* a different index rather
	// than agreeing with whatever it is shown.
	first, err := deserialiseCSRRoots(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.DeleteNode(3); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	second, err := deserialiseCSRRoots(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	if second.IndexRoot == first.IndexRoot {
		t.Error("removing an indexed node left the index root unchanged")
	}
	if err := VerifyCSRRoots(dir); err != nil {
		t.Fatalf("the recompacted v9 image was reported as broken: %v", err)
	}
}

func deserialiseCSRRoots(t *testing.T, dir string) (SnapshotRoots, error) {
	t.Helper()
	csr, _, err := deserialiseCSR(readCSR(t, dir))
	if err != nil {
		return SnapshotRoots{}, err
	}
	r, _ := csr.Roots()
	return r, nil
}

// TestInspectCSR_CountsAMappedIndex: an operator inspecting a v9 image is told how
// many entries it holds.
//
// Read out of the key directory rather than by walking the runs, which is what the
// directory is for — an inspection of a multi-gigabyte index should not cost a pass
// over it. Reported as zero, which is what it was before, is indistinguishable from
// an image whose index is empty.
func TestInspectCSR_CountsAMappedIndex(t *testing.T) {
	dir := v9Store(t)
	info, err := InspectCSR(dir)
	if err != nil {
		t.Fatalf("InspectCSR: %v", err)
	}
	// 51 nodes under three keys each, 50 edges under one.
	if info.PropertyNodeEntries != 153 || info.PropertyEdgeEntries != 50 {
		t.Errorf("property entries %d node, %d edge; want 153 and 50",
			info.PropertyNodeEntries, info.PropertyEdgeEntries)
	}
}

// TestIndexMapped_RecompactingOverABaseKeepsTheWholeIndex is the second
// compaction, which is the one every store that stays open past its first does.
//
// The writer's walk reads base ∪ delta − retracted, and until the flip nothing
// exercised the base half of it: every fixture was built from nothing and compacted
// once, so the delta was the whole index and a walk that ignored the base wrote a
// correct file. A mapped store's *next* compaction is where that stops being true —
// it would write a file holding only what had been written since the last one, and
// the loss would appear one reopen later as an index that had quietly shrunk to the
// delta. Mutation testing found this; no test did.
//
// The oracle is the same sequence carried out over the older encoding, where the
// index is entirely resident and the walk has no base to ignore. Both arms build the
// same graph, so the identifiers line up and the answers must too.
func TestIndexMapped_RecompactingOverABaseKeepsTheWholeIndex(t *testing.T) {
	// churn reopens a compacted store, writes, deletes, and compacts again.
	churn := func(dir string, mode IndexMode) {
		t.Helper()
		s, err := OpenWithOptions(dir, Options{IndexMode: mode})
		if err != nil {
			t.Fatalf("%v: reopen: %v", mode, err)
		}
		// A value the base already carries, so the merge has to deduplicate, and one
		// it does not, so a key can grow.
		fresh := addNodeD(t, s, store.NodeTypeEvidenceFile)
		for _, kv := range [][2]string{{"bucket", "b1"}, {"shard", "s0"}, {"seq", "9999"}} {
			if err := s.IndexNodeProperty(fresh, kv[0], []byte(kv[1])); err != nil {
				t.Fatalf("%v: IndexNodeProperty: %v", mode, err)
			}
		}
		// And a base entity removed, whose entries the base still holds: the walk
		// has to drop them rather than carry them into the next image.
		if err := s.DeleteNode(5); err != nil {
			t.Fatalf("%v: DeleteNode: %v", mode, err)
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("%v: recompact: %v", mode, err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("%v: close: %v", mode, err)
		}
	}

	mappedDir, residentDir := v9Store(t), v8Store(t)
	churn(mappedDir, IndexMapped)
	churn(residentDir, IndexResident)

	if v := versionOf(readCSR(t, mappedDir)); v != csrVersionMappedIndex {
		t.Fatalf("the second mapped compaction wrote v%d", v)
	}

	// The counts first, because they are what a lost base shows up as and they say
	// so directly: "the index shrank" rather than "these two queries disagree".
	wantInfo, err := InspectCSR(residentDir)
	if err != nil {
		t.Fatalf("InspectCSR resident: %v", err)
	}
	gotInfo, err := InspectCSR(mappedDir)
	if err != nil {
		t.Fatalf("InspectCSR mapped: %v", err)
	}
	if gotInfo.PropertyNodeEntries != wantInfo.PropertyNodeEntries ||
		gotInfo.PropertyEdgeEntries != wantInfo.PropertyEdgeEntries {
		t.Fatalf("the recompacted v9 image carries %d node and %d edge entries; "+
			"the same churn over GIDX carries %d and %d",
			gotInfo.PropertyNodeEntries, gotInfo.PropertyEdgeEntries,
			wantInfo.PropertyNodeEntries, wantInfo.PropertyEdgeEntries)
	}

	resident, err := OpenWithOptions(residentDir, Options{IndexMode: IndexResident})
	if err != nil {
		t.Fatalf("reopen resident: %v", err)
	}
	defer resident.Close()
	mapped, err := Open(mappedDir)
	if err != nil {
		t.Fatalf("reopen mapped: %v", err)
	}
	defer mapped.Close()
	if got := mapped.StorageStats().IndexMode; got != IndexMapped.String() {
		t.Fatalf("the recompacted store is held %q", got)
	}
	requireSameAnswers(t, "recompacted over a base", askIndex(t, resident), askIndex(t, mapped))
}
