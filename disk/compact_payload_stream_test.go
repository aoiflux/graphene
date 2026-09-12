package disk

// The compaction payload is streamed from the live property index rather than
// materialised at the pin, and these are the tests for the consequences.
//
// The saving is the whole index: one entry per indexed triple, forty-eight bytes
// of header and a copied value each, built under the store lock and held until
// the image had been written. What it costs is that the entries a compaction
// writes are read *during* the build, from an index the store is still mutating,
// so they are no longer a snapshot taken at the pinned epoch.
//
// Three properties make that sound, and there is an arm here for each:
//
//   - an entry naming an entity the image does not hold is never written, because
//     nodePropSeq filters against the built image (the orphan arms);
//   - an entry the stream misses, or one whose value has moved on since the pin,
//     is in the log tail and replay converges on it (the reopen arms);
//   - and whichever retire branch the commit takes, that replay happens (the
//     branch arm).
//
// Byte-determinism is covered where it already was, in csr_determinism_test.go:
// TestCompact_IdenticalStoresProduceIdenticalBytes compacts two independently
// built stores and compares the files, which is the arm that exercises this
// path end to end.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// --- helpers ---

// payloadFixture writes n nodes, each carrying two indexed properties, and one
// edge per pair of nodes carrying one.
//
// Two keys rather than one because the index walks key by key, holding a shard
// read lock per key: an entry registered during the build lands in a key the
// stream has already passed or one it has not reached yet, and a fixture with a
// single key cannot tell those apart.
func payloadFixture(t *testing.T, s *Store, n int) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, n)
	for i := range n {
		id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
		if err := s.IndexNodeProperty(id, "path", []byte(fmt.Sprintf("/n/%04d", i))); err != nil {
			t.Fatalf("index path on %d: %v", id, err)
		}
		if err := s.IndexNodeProperty(id, "kind", []byte(fmt.Sprintf("kind-%d", i%7))); err != nil {
			t.Fatalf("index kind on %d: %v", id, err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i += 2 {
		eid, err := s.AddEdge(&store.Edge{Src: ids[i-1], Dst: ids[i], Labels: []store.EdgeType{store.EdgeTypeContains}})
		if err != nil {
			t.Fatalf("AddEdge %d: %v", i, err)
		}
		if err := s.IndexEdgeProperty(eid, "rel", []byte(fmt.Sprintf("rel-%d", i%3))); err != nil {
			t.Fatalf("index rel on %d: %v", eid, err)
		}
	}
	return ids
}

// imageEntries reads back the property-index section of the image on disk.
func imageEntries(t *testing.T, dir string) *csrIndexSection {
	t.Helper()
	_, section, err := deserialiseCSR(readCSR(t, dir))
	if err != nil {
		t.Fatalf("deserialise the image: %v", err)
	}
	if section == nil {
		t.Fatal("the image carries no property-index section")
	}
	return section
}

// imageNodeIDs is the set of node identifiers the image's records cover.
func imageNodeIDs(t *testing.T, dir string) map[store.NodeID]bool {
	t.Helper()
	csr, _, err := deserialiseCSR(readCSR(t, dir))
	if err != nil {
		t.Fatalf("deserialise the image: %v", err)
	}
	out := make(map[store.NodeID]bool, csr.NodeCount())
	for n := range csr.Nodes() {
		out[n.ID] = true
	}
	return out
}

// --- the quiescent case ---

// A compaction of a store nobody is writing to must carry exactly the index.
//
// The baseline for everything below: if the stream and the materialised
// enumeration disagree here, the divergence is in the walk rather than in any of
// the concurrency the other arms set up.
func TestCompact_StreamedPayloadMatchesTheLiveIndex(t *testing.T) {
	s, dir := openFresh(t)
	defer s.Close()

	payloadFixture(t, s, 40)

	wantNodes := s.propIdx.NodeEntries()
	wantEdges := s.propIdx.EdgeEntries()

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	section := imageEntries(t, dir)
	assertNodeEntriesEqual(t, section.NodeProps, wantNodes)
	assertEdgeEntriesEqual(t, section.EdgeProps, wantEdges)
}

func assertNodeEntriesEqual(t *testing.T, got, want []index.NodePropEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("the image carries %d node entries, the index holds %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("node entry %d is (%d,%q,%q), the index holds (%d,%q,%q)",
				i, got[i].ID, got[i].Key, got[i].Value, want[i].ID, want[i].Key, want[i].Value)
		}
	}
}

func assertEdgeEntriesEqual(t *testing.T, got, want []index.EdgePropEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("the image carries %d edge entries, the index holds %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("edge entry %d is (%d,%q,%q), the index holds (%d,%q,%q)",
				i, got[i].ID, got[i].Key, got[i].Value, want[i].ID, want[i].Key, want[i].Value)
		}
	}
}

// --- the orphan arms ---

// A node committed during the build must not have its entries written.
//
// This is the one failure mode the stream introduces that convergence does not
// repair on its own. The node is not in the image: it was committed after the
// pin, so the merge never saw it. Its index entries, though, are in the live
// index by the time the stream runs, and writing them would put postings into
// the image naming a node the image does not contain — an orphan against
// invariant 15.5, and one that survives until the log is replayed. Every read
// between the commit and the next open would see it.
//
// So the filter is not an optimisation: without it this test is the bug.
func TestCompact_PayloadOmitsANodeCommittedDuringTheBuild(t *testing.T) {
	s, dir := openFresh(t)
	defer s.Close()

	payloadFixture(t, s, 20)

	// The count the image must carry: everything registered before the pin, and
	// nothing else. Pinning the count as well as the absence is what separates
	// "the orphan was skipped" from "the stream stopped at the orphan" -- the
	// second drops every entry after it, which no reopen recovers once the log
	// holding them has been retired.
	wantEntries := len(s.propIdx.NodeEntries())

	var during store.NodeID
	s.afterPinHook = func() {
		var err error
		if during, err = s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
			t.Errorf("AddNode during the build: %v", err)
			return
		}
		// Both keys: one the stream has already walked, one it has not. Which is
		// which depends on shard order, and the point is that neither reaches the
		// image.
		if err := s.IndexNodeProperty(during, "path", []byte("/during")); err != nil {
			t.Errorf("index path during the build: %v", err)
		}
		if err := s.IndexNodeProperty(during, "kind", []byte("kind-during")); err != nil {
			t.Errorf("index kind during the build: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.afterPinHook = nil

	if got := len(imageEntries(t, dir).NodeProps); got != wantEntries {
		t.Fatalf("the image carries %d node entries, want the %d registered before the pin: "+
			"the filter is not skipping one entry, it is changing how many are written", got, wantEntries)
	}

	held := imageNodeIDs(t, dir)
	if held[during] {
		t.Fatalf("node %d was committed after the pin but is in the image; "+
			"the fixture no longer sets up the case this test is about", during)
	}
	for _, e := range imageEntries(t, dir).NodeProps {
		if e.ID == during {
			t.Fatalf("the image carries entry (%d,%q,%q) for a node it does not hold: "+
				"a posting naming nothing, which is what nodePropSeq's filter exists to prevent",
				e.ID, e.Key, e.Value)
		}
		if !held[e.ID] {
			t.Fatalf("the image carries entry (%d,%q,%q) for a node it does not hold",
				e.ID, e.Key, e.Value)
		}
	}

	// And the entry is not lost: it is in the log, so a reopen has it.
	assertReopenRestoresEntry(t, s, dir, during, "path", "/during")
}

// The edge half of the same property.
func TestCompact_PayloadOmitsAnEdgeCommittedDuringTheBuild(t *testing.T) {
	s, dir := openFresh(t)
	defer s.Close()

	ids := payloadFixture(t, s, 20)

	var during store.EdgeID
	s.afterPinHook = func() {
		var err error
		during, err = s.AddEdge(&store.Edge{Src: ids[0], Dst: ids[1], Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
		if err != nil {
			t.Errorf("AddEdge during the build: %v", err)
			return
		}
		if err := s.IndexEdgeProperty(during, "rel", []byte("rel-during")); err != nil {
			t.Errorf("index rel during the build: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.afterPinHook = nil

	for _, e := range imageEntries(t, dir).EdgeProps {
		if e.ID == during {
			t.Fatalf("the image carries edge entry (%d,%q,%q) for an edge it does not hold",
				e.ID, e.Key, e.Value)
		}
	}
}

// assertReopenRestoresEntry closes the store and reopens it, asserting the entry
// is back and the indexes verify.
func assertReopenRestoresEntry(t *testing.T, s *Store, dir string, id store.NodeID, key, value string) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	ids, err := s2.NodesByProperty(key, []byte(value))
	if err != nil {
		t.Fatalf("NodesByProperty after reopen: %v", err)
	}
	found := false
	for _, got := range ids {
		if got == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("entry (%d,%q,%q) was filtered out of the image and not restored by replay: "+
			"the filter is dropping entries instead of deferring them", id, key, value)
	}
	if err := s2.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after reopen: %v", err)
	}
}

// --- the convergence arms ---

// A node deleted during the build leaves the image holding its record with no
// entries, and replay must resolve that rather than leave it standing.
//
// The mirror of the orphan case and the reason the filter is one-directional.
// The record is in the image: it was live at the pin. The entries are gone from
// the live index by the time the stream runs, so the image carries none for it.
// That is a record without postings — which is not an orphan and not a
// contradiction, only a store whose index disagrees with its records until the
// log is replayed. The delete is in the tail; after the reopen the node is gone
// and VerifyIndexes is clean.
func TestCompact_ANodeDeletedDuringTheBuildConvergesOnReopen(t *testing.T) {
	s, dir := openFresh(t)
	defer s.Close()

	ids := payloadFixture(t, s, 20)
	doomed := ids[3]

	s.afterPinHook = func() {
		if err := s.DeleteNode(doomed); err != nil {
			t.Errorf("DeleteNode during the build: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.afterPinHook = nil

	// The record is in the image, because the pin held it.
	if !imageNodeIDs(t, dir)[doomed] {
		t.Fatalf("node %d was deleted after the pin but is absent from the image; "+
			"the fixture no longer sets up the case this test is about", doomed)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if _, err := s2.GetNode(doomed); err == nil {
		t.Fatalf("node %d is present after a reopen; the delete in the log tail was not replayed "+
			"over the image that still holds its record", doomed)
	}
	if err := s2.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after reopen: %v", err)
	}
}

// A value changed during the build makes the image's records and its index
// section disagree, and the log must settle it.
//
// The records are the pinned ones, so they carry the old blob; the stream reads
// the index after the change, so the section carries the new value. Both are
// true of different epochs, which is exactly what the tail exists to reconcile.
// What must not happen is that the disagreement survives the reopen.
func TestCompact_AValueChangedDuringTheBuildConvergesOnReopen(t *testing.T) {
	s, dir := openFresh(t)
	defer s.Close()

	ids := payloadFixture(t, s, 20)
	moved := ids[5]

	s.afterPinHook = func() {
		if err := s.IndexNodeProperty(moved, "path", []byte("/moved")); err != nil {
			t.Errorf("reindex during the build: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.afterPinHook = nil

	assertReopenRestoresEntry(t, s, dir, moved, "path", "/moved")
}

// Every retire branch has to reach that replay.
//
// The convergence above rests on the log tail surviving the compaction, and
// which of retireLog's three branches runs decides whether it does. Branch one
// truncates the log whole, and is only taken when nothing landed during the
// build — so index-at-build is index-at-pin and there is nothing to converge.
// The other two carry or keep the tail. This walks all three.
func TestCompact_StreamedPayloadConvergesOnEveryRetireBranch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  Options
		churn bool
	}{
		{name: "truncate whole", opts: Options{}},
		{name: "rebuild over tail", opts: Options{}, churn: true},
		{name: "keep the log", opts: keepingRetireOpts(), churn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := OpenWithOptions(dir, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			payloadFixture(t, s, 20)

			var during store.NodeID
			if tc.churn {
				s.afterPinHook = func() {
					var aerr error
					if during, aerr = s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); aerr != nil {
						t.Errorf("AddNode during the build: %v", aerr)
						return
					}
					if aerr := s.IndexNodeProperty(during, "path", []byte("/churn")); aerr != nil {
						t.Errorf("index during the build: %v", aerr)
					}
				}
			}
			if err := s.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			s.afterPinHook = nil

			wantNodes, err := s.NodeCount()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2, err := OpenWithOptions(dir, tc.opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer s2.Close()

			if got, err := s2.NodeCount(); err != nil {
				t.Fatal(err)
			} else if got != wantNodes {
				t.Errorf("node count after reopen = %d, want %d", got, wantNodes)
			}
			if err := s2.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes after reopen: %v", err)
			}
			if tc.churn {
				ids, err := s2.NodesByProperty("path", []byte("/churn"))
				if err != nil {
					t.Fatalf("NodesByProperty: %v", err)
				}
				if len(ids) != 1 || ids[0] != during {
					t.Errorf("the entry committed during the build resolves to %v, want [%d]", ids, during)
				}
			}
		})
	}
}

// --- the allocation guard ---

// What a compaction allocates must not scale with the number of index entries.
//
// The item is a footprint item, so the guard is on bytes rather than on which
// function is called: materialising the payload cost 48 bytes of entry header
// plus a copied value per triple, and streaming it costs the entry being
// written. Asserted as a slope across two fixture sizes rather than a ceiling,
// for the reason TestAllocGuards_Paths states — a ceiling on one size is a
// ceiling on the fixture, and passes for the wrong reason as soon as the
// fixture moves.
//
// The threshold has room in it. Two keys per node put the materialised cost near
// seventy bytes per entry; anything under twenty-four means the entries are not
// being held. The records, the arenas and the image buffer are all in this
// measurement too, which is why the slope is taken between two sizes: those
// terms are proportional to the records, and doubling the entries per node while
// holding the nodes fixed is what isolates the payload.
func TestCompact_PayloadAllocationDoesNotScaleWithEntries(t *testing.T) {
	// Same node and edge count in both arms, so every term except the payload is
	// held fixed and the difference is attributable to the entries alone.
	const nodes = 600

	allocFor := func(keysPerNode int) (uint64, int) {
		s, _ := openFresh(t)
		defer s.Close()

		entries := 0
		for i := range nodes {
			id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			for k := range keysPerNode {
				key := fmt.Sprintf("k%02d", k)
				if err := s.IndexNodeProperty(id, key, []byte(fmt.Sprintf("%s-%06d", key, i))); err != nil {
					t.Fatalf("index %s: %v", key, err)
				}
				entries++
			}
		}

		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		runtime.ReadMemStats(&m1)
		return m1.TotalAlloc - m0.TotalAlloc, entries
	}

	lowAlloc, lowEntries := allocFor(2)
	highAlloc, highEntries := allocFor(10)

	if highEntries <= lowEntries {
		t.Fatalf("the fixture did not grow: %d then %d entries", lowEntries, highEntries)
	}
	if highAlloc < lowAlloc {
		// Allocation went down as the work went up. Nothing to assert against,
		// and nothing wrong either; the slope is what the guard is about.
		t.Logf("compaction allocated %d bytes for %d entries and %d for %d",
			lowAlloc, lowEntries, highAlloc, highEntries)
		return
	}
	perEntry := float64(highAlloc-lowAlloc) / float64(highEntries-lowEntries)
	const maxPerEntry = 24.0
	if perEntry > maxPerEntry {
		t.Fatalf("compaction allocates %.1f bytes per index entry (%d bytes for %d entries, "+
			"%d for %d); the payload is being materialised again rather than streamed",
			perEntry, lowAlloc, lowEntries, highAlloc, highEntries)
	}
	t.Logf("compaction allocates %.1f bytes per index entry (limit %.0f)", perEntry, maxPerEntry)
}
