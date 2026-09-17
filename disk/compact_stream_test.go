package disk

// The streaming compaction is held to the image the materialising one writes.
//
// This is the whole proof of Phase 4. The two paths share an encoder, so what is
// being tested is not two serialisers agreeing -- it is that the merge, the
// header counts and the property-index filter reach the same image when the
// figures come from a CSRGraph and when they come from the plan. A difference of
// one byte anywhere is a difference in what a reader would find, and the
// snapshot root and the whole-file digest are both inside the comparison.

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// streamMergeFixture builds a store the two paths can disagree about.
//
// determinismFixture next door is a single generation of records with no image
// underneath, which is the one shape where the merge is trivial: everything
// comes from the delta and nothing is filtered. The interesting cases are all
// here instead --
//
//   - a compaction first, so the second merge has an image AND a delta and the
//     plan's oracle has to consult both;
//   - updates, which put an identifier in deltaKnown and in deltaNodes;
//   - deletes, which put one in deltaKnown and not in deltaNodes -- the case
//     that decides whether a posting for a dropped record reaches the file;
//   - a declared composite, which is filtered through the same oracle by a
//     different route.
func streamMergeFixture(t *testing.T, s *Store) {
	t.Helper()

	const first = 90
	keys := []string{"sha256", "path", "tool", "case_id"}

	if err := s.DeclareCompositeNodeProperties([]string{"tool", "case_id"}); err != nil {
		t.Fatalf("DeclareCompositeNodeProperties: %v", err)
	}
	if err := s.DeclareOrderedNodeProperty("path"); err != nil {
		t.Fatalf("DeclareOrderedNodeProperty: %v", err)
	}

	add := func(i int) store.NodeID {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		for k, key := range keys {
			val := fmt.Sprintf("%s-%04d", key, i*7+k)
			if err := s.IndexNodeProperty(id, key, []byte(val)); err != nil {
				t.Fatalf("IndexNodeProperty(%d, %q): %v", id, key, err)
			}
		}
		return id
	}

	for i := 1; i <= first; i++ {
		id := add(i)
		if i > 1 {
			eid := addEdgeD(t, s, store.NodeID(i-1), id, store.EdgeTypeContains)
			if err := s.IndexEdgeProperty(eid, "rel", []byte(fmt.Sprintf("rel-%04d", i))); err != nil {
				t.Fatalf("IndexEdgeProperty(%d): %v", eid, err)
			}
		}
	}

	// The image the second compaction merges against.
	if err := s.Compact(); err != nil {
		t.Fatalf("fixture Compact: %v", err)
	}

	// A delta over it: new records, updated ones, and deleted ones.
	for i := first + 1; i <= first+40; i++ {
		add(i)
	}
	for i := 3; i <= first; i += 7 {
		if err := s.IndexNodeProperty(store.NodeID(i), "tool", []byte(fmt.Sprintf("tool-updated-%04d", i))); err != nil {
			t.Fatalf("update IndexNodeProperty(%d): %v", i, err)
		}
	}
	for i := 5; i <= first; i += 11 {
		if err := s.DeleteNode(store.NodeID(i)); err != nil {
			t.Fatalf("DeleteNode(%d): %v", i, err)
		}
	}
}

// TestCompactStream_WritesTheSameImageAsTheMaterialisingPath is the gate on
// Phase 4.
//
// Two stores are built by the same operations in the same order, then compacted
// by the two different paths. The images must be equal byte for byte with only
// the compaction timestamp neutralised, exactly as
// TestCompact_IdenticalStoresProduceIdenticalBytes neutralises it for two runs
// of the same path.
func TestCompactStream_WritesTheSameImageAsTheMaterialisingPath(t *testing.T) {
	// Both index modes, because the property index reaches the encoder by two
	// different routes and the filter is applied in both. IndexMapped writes GPIX
	// through mappedIndexSource and its composites through GCPX; IndexResident
	// writes GIDX through nodePropSeq and edgePropSeq. A streaming build that
	// agreed on one and not the other would be half-right in a way only a caller
	// who had chosen v8 would ever discover.
	for _, mode := range []IndexMode{IndexMapped, IndexResident} {
		t.Run(mode.String(), func(t *testing.T) {
			open := func() (*Store, string) {
				dir := t.TempDir()
				s, err := OpenWithOptions(dir, Options{IndexMode: mode})
				if err != nil {
					t.Fatalf("OpenWithOptions: %v", err)
				}
				return s, dir
			}

			materialised := func() []byte {
				s, dir := open()
				defer s.Close()
				streamMergeFixture(t, s)
				if err := s.CompactCtx(t.Context()); err != nil {
					t.Fatalf("CompactCtx: %v", err)
				}
				return readCSR(t, dir)
			}

			streamed := func() []byte {
				s, dir := open()
				streamMergeFixture(t, s)
				reopened, err := s.CompactAndReopen()
				if err != nil {
					s.Close()
					t.Fatalf("CompactAndReopen: %v", err)
				}
				defer reopened.Close()
				return readCSR(t, dir)
			}

			a, b := stripVolatile(materialised()), stripVolatile(streamed())
			if !bytes.Equal(a, b) {
				t.Fatalf("the streaming compaction wrote a different image than the materialising one.\n%s\n\n"+
					"They share an encoder, so this is the merge, the patched header counts or the "+
					"property-index filter disagreeing -- see imageMembers.", describeDiff(t, a, b))
			}
		})
	}
}

// The degenerate shapes, which are where a patched count is most likely to be
// wrong.
//
// An empty store writes a header whose two counts are patched with zero, and a
// second compaction of an unchanged store writes an image whose every record
// came from the old image and none from the delta. Neither is exotic --
// AutoCompact reaches the second one routinely -- and both are shapes where "the
// encoder counted as it went" and "the graph knew" could disagree without the
// main fixture noticing.
func TestCompactStream_DegenerateShapesWriteTheSameImage(t *testing.T) {
	shapes := []struct {
		name string
		fill func(t *testing.T, s *Store)
	}{
		{"Empty", func(t *testing.T, s *Store) {}},
		{"NothingInTheDelta", func(t *testing.T, s *Store) {
			streamMergeFixture(t, s)
			// A compaction of its own, so the one under test merges an image and
			// an empty delta.
			if err := s.Compact(); err != nil {
				t.Fatalf("settling Compact: %v", err)
			}
		}},
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			materialised := func() []byte {
				s, dir := openFresh(t)
				defer s.Close()
				sh.fill(t, s)
				if err := s.CompactCtx(t.Context()); err != nil {
					t.Fatalf("CompactCtx: %v", err)
				}
				return readCSR(t, dir)
			}
			streamed := func() []byte {
				s, dir := openFresh(t)
				sh.fill(t, s)
				reopened, err := s.CompactAndReopen()
				if err != nil {
					s.Close()
					t.Fatalf("CompactAndReopen: %v", err)
				}
				defer reopened.Close()
				return readCSR(t, dir)
			}

			a, b := stripVolatile(materialised()), stripVolatile(streamed())
			if !bytes.Equal(a, b) {
				t.Fatalf("%s: the two compaction paths wrote different images.\n%s",
					sh.name, describeDiff(t, a, b))
			}
		})
	}
}

// The streamed image must be openable, and must hold what the merge said.
//
// Byte equality above already implies this, but only as long as the
// materialising path is itself correct. This reads the store back through the
// ordinary open path and checks the records, which is what a caller of
// CompactAndReopen actually does next.
func TestCompactStream_TheReopenedStoreHoldsTheMergedRecords(t *testing.T) {
	s, _ := openFresh(t)
	streamMergeFixture(t, s)

	reopened, err := s.CompactAndReopen()
	if err != nil {
		s.Close()
		t.Fatalf("CompactAndReopen: %v", err)
	}
	defer reopened.Close()

	st := reopened.StorageStats()
	if st.CSRNodes == 0 {
		t.Fatal("the reopened store's image holds no nodes; the streamed image did not carry the merge")
	}
	if st.DeltaNodes != 0 {
		t.Fatalf("the reopened store has a delta of %d nodes; the compaction's records should all be in the image", st.DeltaNodes)
	}

	// Every deleted identifier must be gone and every surviving one present.
	// streamMergeFixture deletes 5, 16, 27 ... below 90.
	for i := 5; i <= 90; i += 11 {
		if _, err := reopened.GetNode(store.NodeID(i)); err == nil {
			t.Fatalf("node %d was deleted before the compaction but the streamed image still holds it", i)
		}
	}
	for _, id := range []store.NodeID{1, 2, 4, 91, 130} {
		if _, err := reopened.GetNode(id); err != nil {
			t.Fatalf("node %d survived the compaction but is not in the streamed image: %v", id, err)
		}
	}
}

// A merge that yields out of order must not produce a file.
//
// The materialising path caught this by finding a slot taken; nothing places
// anything in the streaming path, so the ordering check in orderedRecords is the
// only thing standing between a broken merge and an image no reader can parse.
// This drives that check directly, because a merge cannot be made to misbehave
// through the public API -- which is the point: the check exists for the case
// that should be impossible.
func TestCompactStream_OutOfOrderRecordsAreRefused(t *testing.T) {
	p := &compactPlan{
		deltaNodes: []nodeRecord{
			{ID: store.NodeID(7)},
			{ID: store.NodeID(3)},
		},
	}
	recs := &orderedRecords{p: p}

	var seen int
	for range recs.nodes() {
		seen++
	}
	if recs.err == nil {
		t.Fatal("a descending node sequence was accepted; a file written from it would name records no reader could find")
	}
	if seen != 1 {
		t.Fatalf("the walk yielded %d records before stopping, want 1: the violating record must not reach the encoder", seen)
	}

	pe := &compactPlan{
		deltaEdges: []rawEdge{
			{ID: store.EdgeID(4)},
			{ID: store.EdgeID(4)},
		},
	}
	edges := &orderedRecords{p: pe}
	for range edges.edges() {
	}
	if edges.err == nil {
		t.Fatal("a duplicate edge identifier was accepted; buildSeq refused exactly this")
	}
}

// The plan answers the same membership question the new graph would.
//
// This is the substitution Phase 4 rests on, tested on its own so a failure says
// "the oracle disagrees" rather than leaving it to be inferred from a byte
// offset in a two-megabyte image.
func TestCompactPlan_MembershipMatchesTheBuiltGraph(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	streamMergeFixture(t, s)

	p, err := s.compactPin()
	if err != nil {
		t.Fatalf("compactPin: %v", err)
	}
	defer s.compactRelease()
	p.sortDelta()

	built, err := buildSeq(p.nodeSeq(), p.edgeSeq(), p.adjacency)
	if err != nil {
		t.Fatalf("buildSeq: %v", err)
	}

	// Every identifier either path could be asked about: everything the pinned
	// image held, everything the delta mentions, and one above the high-water
	// mark that neither holds.
	var checked int
	ask := func(id store.NodeID) {
		checked++
		if want, got := built.containsNode(id), p.containsNode(id); want != got {
			t.Fatalf("node %d: the built graph says contains=%v, the plan says %v", id, want, got)
		}
	}
	for n := range p.csr.Nodes() {
		ask(n.ID)
	}
	for _, id := range p.deltaKnownNodes {
		ask(id)
	}
	ask(store.NodeID(1 << 20))
	if checked == 0 {
		t.Fatal("no identifiers were compared; the fixture is not exercising the oracle")
	}

	for e := range p.csr.Edges() {
		if want, got := built.containsEdge(e.ID), p.containsEdge(e.ID); want != got {
			t.Fatalf("edge %d: the built graph says contains=%v, the plan says %v", e.ID, want, got)
		}
	}
	for _, id := range p.deltaKnownEdges {
		if want, got := built.containsEdge(id), p.containsEdge(id); want != got {
			t.Fatalf("edge %d: the built graph says contains=%v, the plan says %v", id, want, got)
		}
	}
}
