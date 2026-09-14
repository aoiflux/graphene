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

// The gate. A commit that lands between the pin and the drain means the image
// cannot hold everything the index does -- entries registered after the stream
// are not in it, and entries naming an entity committed after the pin were
// filtered out of it on purpose -- so the adoption does not happen and the store
// keeps answering from the shards.
//
// Without the gate this test does not merely regress memory: the entry
// registered during the build is dropped from a live store.
func TestCompact_DoesNotAdoptWhenTheLogGrewDuringTheBuild(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	payloadFixture(t, s, n)

	var during store.NodeID
	s.afterPinHook = func() {
		var err error
		if during, err = s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}); err != nil {
			t.Errorf("AddNode during the build: %v", err)
			return
		}
		if err := s.IndexNodeProperty(during, "path", []byte("/during")); err != nil {
			t.Errorf("index path during the build: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.afterPinHook = nil

	// The answer first and the mode second, in that order deliberately. The
	// memory result is what this gate is for; the wrong answer is what it costs
	// to get it wrong, and a test that reports the mode first hides the cost
	// behind the symptom.
	ids, err := s.NodesByProperty("path", []byte("/during"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if !slices.Equal(ids, []store.NodeID{during}) {
		t.Fatalf("the entry registered during the build answers %v, want [%d]: "+
			"the shards were emptied over an image that does not hold it", ids, during)
	}
	if got := s.indexHolding(); got != IndexResident.String() {
		t.Fatalf("a compaction that could not write everything the index holds adopted "+
			"its output anyway: the index is held as %q", got)
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
