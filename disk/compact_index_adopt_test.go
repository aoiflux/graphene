package disk

// A compaction writes every entry the index holds into the image, and used to go
// on holding all of them until something reopened the directory. These tests are
// about the step that stops it: the base is swapped for the one just written and
// the shards are emptied behind it.
//
// What they check is not "the memory went down" -- that is measured in
// tests/compact_residency_test.go against the operating system's own figure --
// but the things that have to be true for the memory to be safe to give back.
// Every query answers the same afterwards; the declarations survive; and a
// compaction that could not have written everything does not try.

import (
	"fmt"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// adoptAnswers is every question the fixture makes answerable, and what the
// store says to each.
//
// Compared before and after rather than asserted against literals, because the
// claim is that nothing observable changed. A literal would also pass if the
// fixture and the adoption were wrong in the same direction.
type adoptAnswers struct {
	nodes map[string][]store.NodeID
	edges map[string][]store.EdgeID
}

func collectAdoptAnswers(t *testing.T, s *Store, n int) adoptAnswers {
	t.Helper()
	a := adoptAnswers{nodes: map[string][]store.NodeID{}, edges: map[string][]store.EdgeID{}}
	for i := range n {
		q := fmt.Sprintf("path=/n/%04d", i)
		ids, err := s.NodesByProperty("path", []byte(fmt.Sprintf("/n/%04d", i)))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		a.nodes[q] = ids
	}
	for i := range 7 {
		q := fmt.Sprintf("kind=kind-%d", i)
		ids, err := s.NodesByProperty("kind", []byte(fmt.Sprintf("kind-%d", i)))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		a.nodes[q] = ids
	}
	// A value nothing carries. The base answers a miss differently from the
	// shards -- a binary search that lands between two values rather than a map
	// lookup that finds nothing -- so a miss is worth asking for explicitly.
	ids, err := s.NodesByProperty("kind", []byte("kind-absent"))
	if err != nil {
		t.Fatalf("kind=kind-absent: %v", err)
	}
	a.nodes["kind=kind-absent"] = ids
	for i := range 3 {
		q := fmt.Sprintf("rel=rel-%d", i)
		eids, err := s.EdgesByProperty("rel", []byte(fmt.Sprintf("rel-%d", i)))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		a.edges[q] = eids
	}
	return a
}

func (a adoptAnswers) diff(t *testing.T, want adoptAnswers, when string) {
	t.Helper()
	for q, ids := range want.nodes {
		if !slices.Equal(a.nodes[q], ids) {
			t.Errorf("%s: %s answers %v, wanted %v", when, q, a.nodes[q], ids)
		}
	}
	for q, ids := range want.edges {
		if !slices.Equal(a.edges[q], ids) {
			t.Errorf("%s: %s answers %v, wanted %v", when, q, a.edges[q], ids)
		}
	}
}

// requireAdoptable skips where this build or platform cannot adopt at all.
// Without a mapping the index stays resident and every assertion about the
// adoption is vacuous, so the tests say so rather than passing.
func requireAdoptable(t *testing.T, s *Store) {
	t.Helper()
	if err := s.imageMappingAllowed(); err != nil {
		t.Skipf("this store cannot map its image: %v", err)
	}
	if s.indexMode != IndexMapped {
		t.Skipf("this store's IndexMode is %v", s.indexMode)
	}
}

// The whole of it in one assertion: after a quiet compaction the shards are
// empty and the answers are unchanged.
func TestCompact_AdoptsTheIndexItJustWrote(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	payloadFixture(t, s, n)

	before := collectAdoptAnswers(t, s, n)
	beforeNodes, beforeEdges := s.propIdx.DeltaEntryCounts()
	if beforeNodes == 0 || beforeEdges == 0 {
		t.Fatalf("the fixture left %d node and %d edge entries in the shards; "+
			"there is nothing for this test to observe being given back", beforeNodes, beforeEdges)
	}
	if s.indexHolding() != IndexResident.String() {
		t.Fatalf("a store built from nothing reports its index as %q before any "+
			"compaction; the fixture is not the state this test is about", s.indexHolding())
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Fatalf("after a quiet compaction the index is held as %q, want %q: "+
			"the image carries every entry and the process is still holding them",
			got, IndexMapped.String())
	}
	afterNodes, afterEdges := s.propIdx.DeltaEntryCounts()
	if afterNodes != 0 || afterEdges != 0 {
		t.Errorf("after adopting its own output the shards still hold %d node and %d edge entries, want none",
			afterNodes, afterEdges)
	}
	collectAdoptAnswers(t, s, n).diff(t, before, "after the compaction")
}

// The case the whole bound exists for: a compaction that ran while something was
// writing adopts its output anyway, and the writes are still there.
//
// This used to be the gate's test -- a commit between the pin and the drain meant
// the image could not hold everything the index did, so the adoption was skipped
// and the store went on answering from the shards. That was correct and it was
// also the whole of the memory result during an ingest, because an ingest is a
// compaction with writes landing during it. The capture is what replaced it.
//
// The answer is asserted before the mode, deliberately. The memory result is what
// this is for; a wrong answer is what it costs to get it wrong, and a test that
// reports the mode first hides the cost behind the symptom.
func TestCompact_AdoptsWhenTheLogGrewDuringTheBuild(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	payloadFixture(t, s, n)
	before := collectAdoptAnswers(t, s, n)

	var during store.NodeID
	compactWithWrites(t, s, func() {
		var err error
		if during, err = s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
			t.Errorf("AddNode during the build: %v", err)
			return
		}
		if err := s.IndexNodeProperty(during, "path", []byte("/during")); err != nil {
			t.Errorf("index path during the build: %v", err)
		}
	})

	ids, err := s.NodesByProperty("path", []byte("/during"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if !slices.Equal(ids, []store.NodeID{during}) {
		t.Fatalf("the entry registered during the build answers %v, want [%d]: "+
			"the shards were emptied over an image that does not hold it", ids, during)
	}
	// And the rest of the index came through the swap unchanged, which is the
	// half a test looking only at the new entry would not notice going missing.
	collectAdoptAnswers(t, s, n).diff(t, before, "after a compaction under writes")

	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Fatalf("a compaction under writes is held as %q, want %q: the capture "+
			"exists so that this one does not have to give up its memory", got, IndexMapped.String())
	}
}

// The direction a second set of shards does not reach, and the reason the tail is
// a log of calls.
//
// An entity deleted during the build is in the image with all of its properties,
// because it was alive when the records were pinned. What hides it is a retraction
// against the base -- and SwapBase installs a base whose retraction sets start
// empty, correctly, because an id retracted from the *old* base is simply absent
// from one written after it. That reasoning does not hold for a delete that
// happened after the pin, and forgetting it brings a deleted entity's properties
// back from the dead until the next reopen.
func TestCompact_AdoptionDoesNotResurrectADeleteFromTheBuild(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	ids := payloadFixture(t, s, n)
	gone := ids[3]

	compactWithWrites(t, s, func() {
		if err := s.DeleteNode(gone); err != nil {
			t.Errorf("DeleteNode during the build: %v", err)
		}
	})

	// Both keys, because they hash to different shards and a retraction that
	// only reached one of them is a bug this would otherwise not see.
	for _, q := range []struct{ key, value string }{
		{"path", "/n/0003"},
		{"kind", "kind-3"},
	} {
		got, err := s.NodesByProperty(q.key, []byte(q.value))
		if err != nil {
			t.Fatalf("NodesByProperty(%s=%s): %v", q.key, q.value, err)
		}
		if slices.Contains(got, gone) {
			t.Errorf("%s=%s answers %v, which still holds node %d: it was deleted during "+
				"the build and the image carries the entry the delete was meant to hide",
				q.key, q.value, got, gone)
		}
	}
	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Skipf("this compaction did not adopt its output (%q); the retraction is untested here", got)
	}
}

// A capture that runs out of room declines the adoption rather than replaying
// half a log. The floor is the behaviour the tail gate had: correct, and larger.
func TestCompact_AnOverflowingCaptureDeclinesTheAdoption(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	payloadFixture(t, s, n)
	before := collectAdoptAnswers(t, s, n)

	var during store.NodeID
	compactWithWrites(t, s, func() {
		// Re-opened at a size that cannot hold one operation, which is the
		// smallest thing that makes the drop certain rather than a function of
		// how many writes this hook happens to get through.
		s.propIdx.CaptureTail(0)
		var err error
		if during, err = s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
			t.Errorf("AddNode during the build: %v", err)
			return
		}
		if err := s.IndexNodeProperty(during, "path", []byte("/during")); err != nil {
			t.Errorf("index path during the build: %v", err)
		}
	})

	ids, err := s.NodesByProperty("path", []byte("/during"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if !slices.Equal(ids, []store.NodeID{during}) {
		t.Fatalf("the entry registered during the build answers %v, want [%d]: "+
			"a dropped capture must leave the shards holding it", ids, during)
	}
	collectAdoptAnswers(t, s, n).diff(t, before, "after a dropped capture")
	if got := s.indexHolding(); got != IndexResident.String() {
		t.Fatalf("a compaction whose capture was dropped adopted its output anyway: "+
			"the index is held as %q", got)
	}
}

// A declaration is schema, not a mutation, so no capture records it -- and the
// key lists went into the image at the pin. A key declared during the build is
// therefore absent from the image's GORD, and swapping onto that base would leave
// a declared ordered key the base cannot answer for.
//
// This was already true of the tail gate it replaces: a declaration is not
// journaled to the log, so a quiet build that only declared something passed
// tail == 0 and adopted anyway. The check is new; the exposure is not.
func TestCompact_ADeclarationDuringTheBuildDeclinesTheAdoption(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	payloadFixture(t, s, n)

	compactWithWrites(t, s, func() {
		if err := s.DeclareOrderedNodeProperty("kind"); err != nil {
			t.Errorf("DeclareOrderedNodeProperty during the build: %v", err)
		}
	})

	if got := s.indexHolding(); got != IndexResident.String() {
		t.Errorf("a key declared during the build did not stop the adoption: "+
			"the index is held as %q over an image whose GORD does not name it", got)
	}
	// The declaration is what has to go on working, and over every entry rather
	// than the ones a particular shard happens to hold.
	got, ok := s.propIdx.NodesMatchingOrdered(nil, store.PropertyFilter{
		Key: "kind", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("kind-0"),
	})
	if !ok {
		t.Fatalf("kind stopped being an ordered key")
	}
	if len(got) != n {
		t.Errorf("the ordered index over kind answers %d ids, want %d: "+
			"the declaration survived and its entries did not", len(got), n)
	}
}

// Declarations are schema and entries are data. Emptying the shards must not
// take the first with the second, or every declared range query becomes a scan
// and every unique key stops being one until the next reopen.
func TestCompact_AdoptedIndexKeepsItsDeclarations(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	if err := s.DeclareOrderedNodeProperty("path"); err != nil {
		t.Fatalf("DeclareOrderedNodeProperty: %v", err)
	}
	if err := s.DeclareUniqueNodeProperty("path"); err != nil {
		t.Fatalf("DeclareUniqueNodeProperty: %v", err)
	}
	const n = 20
	payloadFixture(t, s, n)

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Skipf("this store did not adopt its output (%q); the declarations are untested here", got)
	}

	if !slices.Contains(s.propIdx.OrderedNodeKeys(), "path") {
		t.Errorf("path stopped being an ordered key when the shards were emptied")
	}
	if !s.propIdx.IsUniqueNodeKey("path") {
		t.Errorf("path stopped being a unique key when the shards were emptied")
	}

	// And the uniqueness is enforced against the base, not merely remembered:
	// the value is held by a node the image carries and the shards no longer do.
	other := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.IndexNodeProperty(other, "path", []byte("/n/0000")); err == nil {
		t.Errorf("a unique key allowed a value the base already holds")
	}
}

// Twice in a row, because the second compaction is the first one whose base is
// itself a swapped one: it reads the index it is writing out of a mapping it
// installed, and then replaces that mapping.
func TestCompact_AdoptsAcrossTwoCompactions(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	ids := payloadFixture(t, s, n)
	before := collectAdoptAnswers(t, s, n)

	if err := s.Compact(); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Skipf("this store did not adopt its output (%q)", got)
	}
	collectAdoptAnswers(t, s, n).diff(t, before, "after the first compaction")

	// Something new between them, so the second compaction has a delta over the
	// base to fold in rather than an empty one to rewrite.
	extra := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.IndexNodeProperty(extra, "kind", []byte("kind-0")); err != nil {
		t.Fatalf("index kind on the extra node: %v", err)
	}
	if err := s.DeleteNode(ids[0]); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Fatalf("the second compaction left the index held as %q", got)
	}
	if dn, de := s.propIdx.DeltaEntryCounts(); dn != 0 || de != 0 {
		t.Errorf("after the second compaction the shards hold %d node and %d edge entries, want none", dn, de)
	}

	// The deleted node is gone and the added one is there, read out of a base
	// that was written from a base.
	if got, err := s.NodesByProperty("path", []byte("/n/0000")); err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	} else if len(got) != 0 {
		t.Errorf("a deleted node still answers under its path: %v", got)
	}
	got, err := s.NodesByProperty("kind", []byte("kind-0"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if !slices.Contains(got, extra) {
		t.Errorf("the node added between the two compactions is missing from %v", got)
	}
}

// The resident figure is what an operator watches, and it is the reason this
// change exists. It has to fall at the compaction rather than at the next open.
func TestCompact_AdoptionDropsTheResidentIndex(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	payloadFixture(t, s, 200)
	before := s.propIdx.ResidentBytes()

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Skipf("this store did not adopt its output (%q)", got)
	}
	after := s.propIdx.ResidentBytes()
	if after >= before {
		t.Errorf("the index held %d bytes before the compaction and %d after; "+
			"a compaction that adopts its own output has to give the entries back",
			before, after)
	}
}

// The same figure under writes, which is the case the memory result was missing.
//
// A compaction during an ingest used to give nothing back at all, so this number
// did not move and the bound on a bulk import was a bound on the delta with the
// index climbing underneath it. What is asserted is the direction and the floor
// together: the index falls, and it does not fall to nothing, because the writes
// that landed during the build are entries the shards still legitimately hold.
func TestCompact_AdoptionUnderWritesDropsTheResidentIndex(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	payloadFixture(t, s, 200)
	before := s.propIdx.ResidentBytes()

	compactWithWrites(t, s, func() {
		for i := range 5 {
			id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
			if err != nil {
				t.Errorf("AddNode during the build: %v", err)
				return
			}
			if err := s.IndexNodeProperty(id, "path", []byte(fmt.Sprintf("/during/%d", i))); err != nil {
				t.Errorf("index path during the build: %v", err)
				return
			}
		}
	})
	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Fatalf("a compaction under writes is held as %q; the capture is what makes "+
			"this the case the figure below is about", got)
	}

	after := s.propIdx.ResidentBytes()
	if after >= before {
		t.Errorf("the index held %d bytes before a compaction under writes and %d after; "+
			"the entries the image now carries were not given back", before, after)
	}
	if after == 0 {
		t.Errorf("the index reports nothing resident, but five entries landed during " +
			"the build and the image cannot be carrying them")
	}
}
