package disk

// Tests for the mapped image: that reads really do come out of the file, that
// nothing changes about the graph either way, that the fallbacks are taken where
// they should be and reported, and that a compaction can rename over a live
// mapping — which is the property the whole Windows half rests on.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/aoiflux/graphene/store"
)

// mapFixture builds a store with n propertied nodes and edges between them,
// compacts it so there is an image, closes it, and returns the directory.
func mapFixture(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: fixtureProps(i),
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		if _, err := s.AddEdge(&store.Edge{
			Src:        ids[i-1],
			Dst:        ids[i],
			Labels:     []store.EdgeType{store.EdgeTypeSimilarTo},
			Properties: []byte(fmt.Sprintf("edge-%06d-payload", i)),
		}); err != nil {
			t.Fatalf("AddEdge: %v", err)
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

// fixtureProps is the blob mapFixture gives the i-th node, and the only place
// the answer lives so a test can assert it without restating the rule.
//
// Every fifth record carries no properties at all, and that is deliberate: both
// stores normalise an empty blob to nil on the way in, so a reader must not be
// able to tell nil from empty, and the mapped parse has its own chance to get
// that wrong in aliasCSRProperties. A fixture in which every record had a blob
// would never ask. The first record keeps one, because most of the tests here
// read node 1.
func fixtureProps(i int) []byte {
	if i%5 == 4 {
		return nil
	}
	return []byte(fmt.Sprintf("node-%06d-payload", i))
}

// inMapping reports whether b's bytes lie inside m's.
//
// Address arithmetic rather than a content comparison, because content proves
// nothing: the copy and the alias hold the same bytes, which is the point. The
// pointers are only compared, never dereferenced through the uintptr, so this is
// the one thing a test can ask that answers "did the copy actually go away".
func inMapping(b []byte, m *mapping) bool {
	if len(b) == 0 || m == nil || len(m.data) == 0 {
		return false
	}
	base := uintptr(unsafe.Pointer(&m.data[0]))
	end := base + uintptr(len(m.data))
	at := uintptr(unsafe.Pointer(&b[0]))
	return at >= base && at < end
}

// theMapping returns the store's single image mapping, failing if it has none.
func theMapping(t *testing.T, s *Store) *mapping {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.images) != 1 {
		t.Fatalf("expected exactly one image mapping, got %d", len(s.images))
	}
	return s.images[0]
}

// TestImageMapped_PropertiesAliasTheFile is the item's whole claim in one
// assertion: a property blob read out of the image is the file's bytes, not a
// copy of them.
func TestImageMapped_PropertiesAliasTheFile(t *testing.T) {
	dir := mapFixture(t, 200)

	s, err := Open(dir) // the default mode
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	m := theMapping(t, s)

	n, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !inMapping(n.Properties, m) {
		t.Error("a node's Properties do not lie inside the image mapping: the blob was copied")
	}
	if want := []byte("node-000000-payload"); !bytes.Equal(n.Properties, want) {
		t.Errorf("Properties = %q, want %q", n.Properties, want)
	}

	e, err := s.GetEdge(1)
	if err != nil {
		t.Fatalf("GetEdge: %v", err)
	}
	if !inMapping(e.Properties, m) {
		t.Error("an edge's Properties do not lie inside the image mapping")
	}

	// Labels deliberately do not. They are uint16 at an unaligned offset in the
	// record stream, so they are decoded into a heap arena in both modes, and a
	// test that did not say so would let a later change to that pass unnoticed.
	if inMapping(unsafe.Slice((*byte)(unsafe.Pointer(&n.Labels[0])), 2), m) {
		t.Error("a node's Labels lie inside the mapping; they are supposed to be decoded")
	}
}

// TestImageMode_HeapCopiesTheBlobs is the other half: ImageHeap is still the old
// behaviour exactly, and a caller who asks for independence gets it.
func TestImageMode_HeapCopiesTheBlobs(t *testing.T) {
	dir := mapFixture(t, 50)

	s, err := OpenWithOptions(dir, Options{ImageMode: ImageHeap})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	s.mu.RLock()
	images := len(s.images)
	s.mu.RUnlock()
	if images != 0 {
		t.Errorf("ImageHeap mapped %d files; it is supposed to map none", images)
	}
	if mode, n := s.imageHolding(); mode != "heap" || n != 0 {
		t.Errorf("StorageStats reports %q/%d, want heap/0", mode, n)
	}

	n, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if want := []byte("node-000000-payload"); !bytes.Equal(n.Properties, want) {
		t.Errorf("Properties = %q, want %q", n.Properties, want)
	}
}

// TestImageMode_SameGraphEitherWay is the compatibility assertion the two parse
// paths need: mapped and copied must produce the same graph, not merely a
// working one.
//
// Every record, both label sets, both adjacency directions, the counts, the
// high-water marks and the roots. If the two ever disagree, everything measured
// about the mapped path says nothing about the copied one.
func TestImageMode_SameGraphEitherWay(t *testing.T) {
	dir := mapFixture(t, 300)
	path := filepath.Join(dir, csrFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	heapCSR, heapSec, err := deserialiseCSRFrom(data, false, AdjacencyEager)
	if err != nil {
		t.Fatalf("parse copied: %v", err)
	}

	m, err := mapImage(path)
	if err != nil {
		t.Skipf("this platform cannot map: %v", err)
	}
	defer m.close()
	mapCSR, mapSec, err := deserialiseCSRFrom(m.data, true, AdjacencyEager)
	if err != nil {
		t.Fatalf("parse mapped: %v", err)
	}

	if mapCSR.NodeCount() != heapCSR.NodeCount() || mapCSR.EdgeCount() != heapCSR.EdgeCount() {
		t.Fatalf("counts differ: mapped %d/%d, copied %d/%d",
			mapCSR.NodeCount(), mapCSR.EdgeCount(), heapCSR.NodeCount(), heapCSR.EdgeCount())
	}
	if mapCSR.imgBytes != int64(len(data)) {
		t.Errorf("mapped graph reports %d image bytes, want %d", mapCSR.imgBytes, len(data))
	}
	if heapCSR.imgBytes != 0 {
		t.Errorf("copied graph reports %d image bytes, want 0", heapCSR.imgBytes)
	}

	for id := store.NodeID(1); id <= heapCSR.HighestNodeID(); id++ {
		a, aok := heapCSR.GetNode(id)
		b, bok := mapCSR.GetNode(id)
		if aok != bok {
			t.Fatalf("node %d: present copied=%v mapped=%v", id, aok, bok)
		}
		if !aok {
			continue
		}
		if !bytes.Equal(a.Properties, b.Properties) {
			t.Fatalf("node %d: properties differ", id)
		}
		// bytes.Equal cannot see this and a caller can: a record written with no
		// properties must read back as nil, not as an empty slice, from either
		// path.
		if (a.Properties == nil) != (b.Properties == nil) {
			t.Fatalf("node %d: copied nil-ness %v, mapped %v -- a caller comparing "+
				"Properties against nil would get different answers from the two modes",
				id, a.Properties == nil, b.Properties == nil)
		}
		if len(a.Properties) == 0 && a.Properties != nil {
			t.Fatalf("node %d: an empty blob came back as an empty slice rather than nil", id)
		}
		if len(a.Labels) != len(b.Labels) {
			t.Fatalf("node %d: label counts differ", id)
		}
		for i := range a.Labels {
			if a.Labels[i] != b.Labels[i] {
				t.Fatalf("node %d: label %d differs", id, i)
			}
		}
		// cap == len on both, which is what stops an append reaching the next
		// record — and under a mapping, what stops it faulting.
		if cap(b.Properties) != len(b.Properties) {
			t.Fatalf("node %d: mapped properties have cap %d and len %d; an append would "+
				"write into the file", id, cap(b.Properties), len(b.Properties))
		}
		if !sameEdgeIDList(heapCSR.OutboundEdgeIDs(id), mapCSR.OutboundEdgeIDs(id)) ||
			!sameEdgeIDList(heapCSR.InboundEdgeIDs(id), mapCSR.InboundEdgeIDs(id)) {
			t.Fatalf("node %d: adjacency differs", id)
		}
	}
	for id := store.EdgeID(1); id <= heapCSR.HighestEdgeID(); id++ {
		a, aok := heapCSR.GetEdge(id)
		b, bok := mapCSR.GetEdge(id)
		if aok != bok {
			t.Fatalf("edge %d: present copied=%v mapped=%v", id, aok, bok)
		}
		if aok && (!bytes.Equal(a.Properties, b.Properties) || a.Src != b.Src || a.Dst != b.Dst) {
			t.Fatalf("edge %d differs", id)
		}
	}
	if mapCSR.roots != heapCSR.roots {
		t.Error("the two parses produce different snapshot roots")
	}
	if mapCSR.nodeSeqHW != heapCSR.nodeSeqHW || mapCSR.edgeSeqHW != heapCSR.edgeSeqHW ||
		mapCSR.commitSeqHW != heapCSR.commitSeqHW {
		t.Error("the two parses produce different high-water marks")
	}
	if len(mapSec.NodeProps) != len(heapSec.NodeProps) || len(mapSec.EdgeProps) != len(heapSec.EdgeProps) {
		t.Error("the two parses produce different index sections")
	}
}

func sameEdgeIDList(a, b []store.EdgeID) bool {
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

// TestImageMapped_AppendToPropertiesReallocates is the three-index form's whole
// job, and under a mapping the consequence of losing it is a fault in a
// read-only region rather than a corrupted neighbouring record.
func TestImageMapped_AppendToPropertiesReallocates(t *testing.T) {
	dir := mapFixture(t, 20)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	m := theMapping(t, s)

	n, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	grown := append(n.Properties, 'X') //nolint:gocritic // the append is the subject
	if inMapping(grown, m) {
		t.Fatal("appending to a mapped Properties slice wrote into the mapping")
	}

	// And the record is unchanged, which is the observable half.
	again, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !bytes.Equal(again.Properties, []byte("node-000000-payload")) {
		t.Errorf("the record changed under the append: %q", again.Properties)
	}
}

// TestImageMapped_CompactRenamesOverALiveMapping is the property the Windows
// half of this item rests on, asserted on every platform.
//
// A compaction writes graphene.csr.tmp and renames it over graphene.csr while
// the old image is mapped. On Unix that is inode semantics. On Windows it works
// only because the file handle is closed after MapViewOfFile and the section
// inherited FILE_SHARE_DELETE — with the handle held open the rename is refused
// with a sharing violation, so this test is what would catch that regressing.
//
// Two compactions, so the second one renames over a name that has already been
// renamed over once, and the mapping is read after both.
func TestImageMapped_CompactRenamesOverALiveMapping(t *testing.T) {
	dir := mapFixture(t, 100)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	m := theMapping(t, s)

	first, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !inMapping(first.Properties, m) {
		t.Skip("this platform did not map the image; the rename question does not arise")
	}
	held := first.Properties

	for round := 1; round <= 2; round++ {
		if _, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: []byte(fmt.Sprintf("added-%d", round)),
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact %d: %v", round, err)
		}
		// The slice taken before any of this still reads its own bytes. On
		// Windows this is the whole measured claim; if the rename had gone
		// through ReplaceFileW or an aside, or if the section had been
		// invalidated, this is where it would show.
		if !bytes.Equal(held, []byte("node-000000-payload")) {
			t.Fatalf("after compaction %d a slice into the old mapping reads %q", round, held)
		}
		if got, err := s.GetNode(1); err != nil {
			t.Fatalf("GetNode after compaction %d: %v", round, err)
		} else if !bytes.Equal(got.Properties, []byte("node-000000-payload")) {
			t.Fatalf("after compaction %d the record reads %q", round, got.Properties)
		}
	}

	// One mapping, still, after two compactions. A compaction neither creates
	// nor retires one — see mapping.go — and this is the assertion that keeps
	// that from drifting into one mapping per compaction.
	s.mu.RLock()
	n := len(s.images)
	s.mu.RUnlock()
	if n != 1 {
		t.Errorf("the store holds %d mappings after two compactions, want 1", n)
	}
}

// TestImageMapped_SnapshotSurvivesCompaction pins the other reader of an old
// image: a snapshot holds the graph a compaction replaced, and that graph's
// blobs are in the mapping the store still owns.
func TestImageMapped_SnapshotSurvivesCompaction(t *testing.T) {
	dir := mapFixture(t, 80)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Close()

	for round := 0; round < 2; round++ {
		if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		runtime.GC()
		runtime.GC()
		n, err := snap.GetNode(1)
		if err != nil {
			t.Fatalf("snapshot GetNode after %d compactions: %v", round+1, err)
		}
		if !bytes.Equal(n.Properties, []byte("node-000000-payload")) {
			t.Fatalf("snapshot read %q after %d compactions", n.Properties, round+1)
		}
	}
}

// TestImageMapped_FallsBackForALiveReader checks the policy half of ImageMapped:
// a live reader holds no process lock, so by default it is not mapped, and the
// fallback is reported rather than silent.
func TestImageMapped_FallsBackForALiveReader(t *testing.T) {
	dir := mapFixture(t, 40)

	var fallbacks atomic.Int64
	var reason atomic.Value
	sink := store.MetricsFunc(func(m store.Metric) {
		if m.Kind == store.MetricImageFallback {
			fallbacks.Add(1)
			if m.Err != nil {
				reason.Store(m.Err.Error())
			}
		}
	})

	s, err := OpenWithOptions(dir, Options{LiveReader: true, Metrics: sink})
	if err != nil {
		t.Fatalf("OpenLive: %v", err)
	}
	defer s.Close()

	if !lockingEnforced {
		// The reason would be the platform rather than the mode; still a
		// fallback, still reported, so the count below is the assertion either
		// way.
		t.Log("locking is not enforced on this platform")
	}
	if fallbacks.Load() != 1 {
		t.Errorf("a live reader reported %d image fallbacks, want 1", fallbacks.Load())
	}
	if r, _ := reason.Load().(string); r == "" {
		t.Error("the fallback was reported with no reason")
	} else {
		t.Logf("fallback reason: %s", r)
	}
	if mode, _ := s.imageHolding(); mode != "heap" {
		t.Errorf("a live reader is holding its image as %q, want heap", mode)
	}

	// And it still works, which is the point of a fallback.
	if n, err := s.GetNode(1); err != nil {
		t.Fatalf("GetNode: %v", err)
	} else if !bytes.Equal(n.Properties, []byte("node-000000-payload")) {
		t.Errorf("Properties = %q", n.Properties)
	}
}

// TestImageMappedUnlocked_MapsALiveReader is the opt-in, and the sweep with it:
// a live reader that Refreshes across compactions maps each image it rebuilds
// from, and the ones nothing can reach any more are released.
//
// The assertion is bounded growth rather than an exact count, because releasing
// depends on the collector having noticed — which the GC calls below make likely
// and nothing makes certain. An unbounded list is the failure this is for.
func TestImageMappedUnlocked_MapsALiveReader(t *testing.T) {
	dir := mapFixture(t, 60)

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer w.Close()

	r, err := OpenWithOptions(dir, Options{LiveReader: true, ImageMode: ImageMappedUnlocked})
	if err != nil {
		t.Fatalf("OpenLive: %v", err)
	}
	defer r.Close()

	if mode, n := r.imageHolding(); mode != "mapped" || n == 0 {
		if mappingSupported {
			t.Fatalf("ImageMappedUnlocked live reader holds its image as %q/%d, want mapped", mode, n)
		}
		t.Skip("this platform cannot map")
	}

	// Identity, not just the count: a mapping that leaves the list has been
	// unmapped, and that is the assertion. A bounded count alone would also be
	// satisfied by a store that mapped once and never again.
	seen := map[*mapping]bool{}
	released := 0
	for round := 0; round < 5; round++ {
		if _, err := w.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: []byte(fmt.Sprintf("round-%d", round)),
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := w.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		if _, err := r.Refresh(); err != nil {
			t.Fatalf("Refresh %d: %v", round, err)
		}
		if n, err := r.GetNode(1); err != nil {
			t.Fatalf("GetNode after refresh %d: %v", round, err)
		} else if !bytes.Equal(n.Properties, []byte("node-000000-payload")) {
			t.Fatalf("after refresh %d the record reads %q", round, n.Properties)
		}
		runtime.GC()
		runtime.GC()

		r.mu.RLock()
		held := map[*mapping]bool{}
		for _, m := range r.images {
			held[m] = true
			seen[m] = true
		}
		live := len(r.images)
		r.mu.RUnlock()

		for m := range seen {
			if !held[m] {
				if !m.unmapped.Load() {
					t.Errorf("a mapping left the list without being unmapped")
				}
				released++
				delete(seen, m)
			}
		}
		t.Logf("round %d: %d mappings held, %d released so far", round, live, released)
		if live > 6 {
			t.Fatalf("round %d holds %d mappings: nothing is being released", round, live)
		}
	}
	if released == 0 {
		t.Error("five reloads mapped five images and released none: the sweep is not running, " +
			"or nothing ever becomes retirable")
	}

	// Every mapping still listed is still valid, which is what a reader depends
	// on and what would have faulted if the sweep had released one too early.
	r.mu.RLock()
	for _, m := range r.images {
		if m.unmapped.Load() {
			t.Error("a mapping is listed after being unmapped")
		}
	}
	r.mu.RUnlock()
}

// TestImageMapped_TruncationIsRefusedOnWindows is the evidence for the platform
// claim in mmap_windows.go, and half of the fault contract §15 states: Windows
// will not shorten a file with a live mapping, so the SIGBUS that a truncation
// produces on Unix has no Windows counterpart.
//
// Asserted rather than asserted-in-a-comment, because the contract documented on
// ImageMappedUnlocked is only as good as the behaviour it describes, and that is
// an operating-system property this repository does not control.
func TestImageMapped_TruncationIsRefusedOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the refusal is a Windows property; on Unix the truncation succeeds and the " +
			"fault is the documented outcome")
	}
	dir := mapFixture(t, 20)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if _, n := s.imageHolding(); n == 0 {
		t.Skip("the image is not mapped")
	}

	if err := os.Truncate(filepath.Join(dir, csrFileName), 0); err == nil {
		t.Error("Windows truncated a file with a live mapping; the fault contract on " +
			"ImageMappedUnlocked says it refuses, and a mapped reader is now exposed to " +
			"EXCEPTION_IN_PAGE_ERROR")
	} else {
		t.Logf("refused, as documented: %v", err)
	}
}

// TestImageMapped_RefreshFailureRestoresTheOldMapping covers the path that
// throws a mapping away: a reload that fails part way through puts the previous
// view back, and the reader goes on serving from the image it already had.
func TestImageMapped_RefreshFailureRestoresTheOldMapping(t *testing.T) {
	dir := mapFixture(t, 30)

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open writer: %v", err)
	}
	defer w.Close()

	r, err := OpenWithOptions(dir, Options{LiveReader: true, ImageMode: ImageMappedUnlocked})
	if err != nil {
		t.Fatalf("OpenLive: %v", err)
	}
	defer r.Close()
	if mode, _ := r.imageHolding(); mode != "mapped" {
		t.Skip("this platform cannot map")
	}

	// The compaction is what makes the reader take the reload branch rather than
	// advancing in place: it rotates the log to a new generation.
	if _, err := w.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := w.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// The failure is made in the *image*, not the log, and that is deliberate.
	// Damaging the log does not fail a reload: replay treats a record it cannot
	// read as a torn tail and stops there, which is the crash-safety property and
	// is why the first version of this test only ever skipped. An image whose
	// magic is wrong is a failure the reload cannot absorb, and it fails inside
	// loadImage -- so this covers the discard of the mapping the reload had just
	// taken as well as the restore of the one it was using.
	//
	// The reader has the *previous* image mapped, under a name the compaction
	// renamed away, so the file being damaged here is not the one it is reading.
	newImage := filepath.Join(dir, csrFileName)
	raw, err := os.ReadFile(newImage)
	if err != nil {
		t.Fatalf("read the new image: %v", err)
	}
	damaged := append([]byte(nil), raw...)
	copy(damaged[0:4], "XXXX")
	// Written aside and renamed over, rather than written in place, because a
	// compaction now leaves this process holding a mapping of the image it just
	// installed -- the index is read out of it. Windows refuses to open a file
	// with a mapped section for writing, which is the same protection
	// ImageMappedUnlocked's comment describes, and it refuses it here whether or
	// not the damage is deliberate. A rename is permitted on both platforms and
	// is how anything outside the engine would realistically replace the file.
	aside := newImage + ".damaged"
	if err := os.WriteFile(aside, damaged, 0600); err != nil {
		t.Fatalf("write the damaged image: %v", err)
	}
	if err := os.Rename(aside, newImage); err != nil {
		t.Fatalf("install the damaged image: %v", err)
	}

	if _, err := r.Refresh(); err == nil {
		t.Fatal("a Refresh onto an image with a bad magic succeeded")
	} else {
		t.Logf("Refresh refused, as it must: %v", err)
	}

	// The reader still answers, from the image it was already reading.
	n, err := r.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode after a failed Refresh: %v", err)
	}
	if !bytes.Equal(n.Properties, []byte("node-000000-payload")) {
		t.Errorf("after a failed Refresh the record reads %q", n.Properties)
	}
	if mode, _ := r.imageHolding(); mode != "mapped" {
		t.Errorf("after a failed Refresh the image is held as %q, want mapped", mode)
	}

	// And the mapping the failed reload took is not leaked. It is unreachable
	// now, so the sweep in the failure path listed it for release; a further
	// Refresh -- which fails again, on the same damaged image -- is where it
	// actually goes, once the collector has agreed.
	runtime.GC()
	runtime.GC()
	if _, err := r.Refresh(); err == nil {
		t.Fatal("the second Refresh onto the same damaged image succeeded")
	}
	r.mu.RLock()
	held := len(r.images)
	r.mu.RUnlock()
	if held > 3 {
		t.Errorf("two failed reloads left %d mappings held; each one takes one and must "+
			"release it", held)
	}
	t.Logf("%d mappings held after two failed reloads", held)
}

// TestImageMapped_VerifyOnOpenSharesTheMapping keeps the two readers of the
// image at one: verification and the load see the same bytes, so a store opened
// under VerifyOnOpen maps once rather than mapping and then reading.
func TestImageMapped_VerifyOnOpenSharesTheMapping(t *testing.T) {
	dir := mapFixture(t, 120)

	s, err := OpenWithOptions(dir, Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	m := theMapping(t, s)
	n, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !inMapping(n.Properties, m) {
		t.Error("the image the verification read is not the one the store is serving from")
	}
}

// TestImageMapped_VerifyOnOpenStillRefusesADamagedImage is the other side of
// sharing one read: the check must still fail, and the mapping must not be
// leaked when it does.
func TestImageMapped_VerifyOnOpenStillRefusesADamagedImage(t *testing.T) {
	dir := mapFixture(t, 40)
	path := filepath.Join(dir, csrFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	at := csrV8HeaderSize + 24
	if at >= len(data) {
		t.Fatal("image too small to damage")
	}
	data[at] ^= 0xFF
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write image: %v", err)
	}

	s, err := OpenWithOptions(dir, Options{VerifyOnOpen: true})
	if err == nil {
		s.Close()
		t.Fatal("a damaged image opened under VerifyOnOpen")
	}
}

// TestStorageStats_ImageMode is the operator-visible half.
func TestStorageStats_ImageMode(t *testing.T) {
	dir := mapFixture(t, 25)

	for _, tc := range []struct {
		name string
		mode ImageMode
		want string
	}{
		{"default", ImageMapped, "mapped"},
		{"heap", ImageHeap, "heap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenWithOptions(dir, Options{ImageMode: tc.mode})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer s.Close()
			st := s.StorageStats()
			if tc.want == "mapped" && !mappingSupported {
				t.Skip("this platform cannot map")
			}
			if st.ImageMode != tc.want {
				t.Errorf("ImageMode = %q, want %q", st.ImageMode, tc.want)
			}
			if tc.want == "mapped" && st.ImageMappedBytes <= 0 {
				t.Errorf("ImageMappedBytes = %d on a mapped image", st.ImageMappedBytes)
			}
			if tc.want == "heap" && st.ImageMappedBytes != 0 {
				t.Errorf("ImageMappedBytes = %d on a copied image", st.ImageMappedBytes)
			}
		})
	}

	// A store with no image reports neither.
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if st := s.StorageStats(); st.ImageMode != "" || st.ImageMappedBytes != 0 {
		t.Errorf("a store with no image reports %q/%d", st.ImageMode, st.ImageMappedBytes)
	}
}

// TestStorageStats_ImageModeAfterCompaction states the figure honestly: after a
// compaction the published graph is one the compaction built, so what a caller
// is served from is record arrays even though the store still holds the mapping
// those records' blobs address.
func TestStorageStats_ImageModeAfterCompaction(t *testing.T) {
	dir := mapFixture(t, 25)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if !mappingSupported || s.StorageStats().ImageMode != "mapped" {
		t.Skip("this platform cannot map")
	}
	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if st := s.StorageStats(); st.ImageMode != "heap" {
		t.Errorf("after a compaction the image is reported as %q; the graph being served is "+
			"one the compaction built, so heap is the honest answer", st.ImageMode)
	}
	s.mu.RLock()
	held := len(s.images)
	s.mu.RUnlock()
	if held != 1 {
		t.Errorf("the store holds %d mappings; the records it published still address one", held)
	}
}

// TestMapping_ConcurrentCompactAndReads is the -race arm: point reads run
// lock-free out of the image while a compaction swaps it, and the mapping those
// reads address must stay valid throughout.
func TestMapping_ConcurrentCompactAndReads(t *testing.T) {
	dir := mapFixture(t, 400)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var reads atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for id := store.NodeID(1); id <= 400; id++ {
					n, err := s.GetNode(id)
					if err != nil {
						t.Errorf("GetNode(%d): %v", id, err)
						return
					}
					want := fixtureProps(int(id) - 1)
					if !bytes.Equal(n.Properties, want) {
						t.Errorf("GetNode(%d) = %q, want %q", id, n.Properties, want)
						return
					}
					if (n.Properties == nil) != (want == nil) {
						t.Errorf("GetNode(%d) nil-ness %v, want %v",
							id, n.Properties == nil, want == nil)
						return
					}
					reads.Add(1)
				}
			}
		}()
	}

	for round := 0; round < 3; round++ {
		if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
			t.Errorf("AddNode: %v", err)
			break
		}
		if err := s.Compact(); err != nil {
			t.Errorf("Compact: %v", err)
			break
		}
		runtime.GC()
	}
	close(stop)
	wg.Wait()
	t.Logf("%d reads through three compactions", reads.Load())
}

// TestMapping_CloseIsIdempotent covers the one thing the sweep and Close can
// both do to the same mapping.
func TestMapping_CloseIsIdempotent(t *testing.T) {
	dir := mapFixture(t, 10)
	m, err := mapImage(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Skipf("this platform cannot map: %v", err)
	}
	if err := m.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := m.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestMapImage_RefusesAnEmptyFile keeps the fallback reachable for the one case
// that is not a platform limitation.
func TestMapImage_RefusesAnEmptyFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, csrFileName)
	if err := os.WriteFile(p, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if m, err := mapImage(p); err == nil {
		m.close()
		t.Error("an empty file was mapped")
	}
}

// TestImageMapped_AMappingFailureFallsBackAndSaysSo reaches the fallback with a
// real failure rather than a hook, which is the only failure available that is
// not a platform limitation: an empty graphene.csr cannot be mapped.
//
// The open then fails anyway, because a zero-byte image is not an image — and
// that is the point of the assertion. The fallback has to be taken and reported
// *before* the parse decides anything, so the metric fires and names the reason
// even on an open that goes on to fail for an unrelated one. A fallback that were
// silent here would be silent in the case that matters, where the parse succeeds
// and the store quietly pays for a copy it was configured not to pay for.
func TestImageMapped_AMappingFailureFallsBackAndSaysSo(t *testing.T) {
	if !mappingSupported {
		t.Skip("this platform never maps, so there is no failure to fall back from")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, csrFileName), nil, 0600); err != nil {
		t.Fatal(err)
	}

	var fallbacks int
	var reason string
	sink := store.MetricsFunc(func(m store.Metric) {
		if m.Kind == store.MetricImageFallback {
			fallbacks++
			if m.Err != nil {
				reason = m.Err.Error()
			}
		}
	})

	s, err := OpenWithOptions(dir, Options{Metrics: sink})
	if err == nil {
		s.Close()
		t.Fatal("a zero-byte image opened")
	}
	if fallbacks != 1 {
		t.Errorf("%d fallbacks reported, want 1", fallbacks)
	}
	if !strings.Contains(reason, "empty") {
		t.Errorf("the fallback reason was %q; it should name the empty file", reason)
	}
}

// TestImageHeap_ReportsNoFallback is the other half: choosing the copy is not a
// fallback from anything, and reporting it as one would make the metric useless
// for spotting the case it exists for.
func TestImageHeap_ReportsNoFallback(t *testing.T) {
	dir := mapFixture(t, 10)
	var fallbacks int
	sink := store.MetricsFunc(func(m store.Metric) {
		if m.Kind == store.MetricImageFallback {
			fallbacks++
		}
	})
	s, err := OpenWithOptions(dir, Options{ImageMode: ImageHeap, Metrics: sink})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if fallbacks != 0 {
		t.Errorf("ImageHeap reported %d fallbacks; asking for the copy is not falling back", fallbacks)
	}
}

// TestLoadImage_ReleasesTheMappingWhenTheParseFails is the ownership contract
// stated as an assertion: loadImage takes the source, so a source whose parse
// fails is released there and no caller has to reason about it.
//
// It exists because the mutation pass found nothing else covering it. Dropping
// the discard leaks a mapping that was never added to the store's list, so the
// leak is invisible to every test that counts entries in that list — the only
// place it can be seen is from a caller holding the source, which is what this
// does.
func TestLoadImage_ReleasesTheMappingWhenTheParseFails(t *testing.T) {
	src := mapFixture(t, 20)
	path := filepath.Join(src, csrFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// A separate directory, so the store being loaded into is empty and the only
	// image in play is the damaged one handed to loadImage directly.
	damagedDir := t.TempDir()
	damagedPath := filepath.Join(damagedDir, csrFileName)
	damaged := append([]byte(nil), raw...)
	copy(damaged[0:4], "XXXX")
	if err := os.WriteFile(damagedPath, damaged, 0600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	m, err := mapImage(damagedPath)
	if err != nil {
		t.Skipf("this platform cannot map: %v", err)
	}
	held := &imageSource{data: m.data, m: m}

	s.mu.Lock()
	loadErr := s.loadImage(held)
	listed := len(s.images)
	s.mu.Unlock()

	if loadErr == nil {
		t.Fatal("loadImage accepted an image with a bad magic")
	}
	if !m.unmapped.Load() {
		t.Error("loadImage failed and left the mapping it was given open")
	}
	if listed != 0 {
		t.Errorf("a failed load listed %d mappings on the store; it must list none", listed)
	}
	// And the release was the real one, not just the flag.
	if err := m.close(); err != nil {
		t.Errorf("closing an already-released mapping: %v", err)
	}
}
