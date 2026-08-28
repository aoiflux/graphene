package index

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// verify has to be able to fail.
//
// A composite index is maintained incrementally and drives queries directly: the
// planner applies no residual filter for the keys a composite covers, so an
// entry that drifted out of step with the member values it was derived from is a
// row silently added to or missing from a result. VerifyIndexes is the only
// thing that can say so, and a check that cannot fail is not one.
//
// These reach into the index rather than going through a Graph, because the
// corruption they need is exactly the state no public API can produce.

func compositeUnderTest(t *testing.T) (*PropertyIndex, *compositeIndex[store.NodeID]) {
	t.Helper()
	p := NewPropertyIndex()
	if err := p.DeclareCompositeNodeKeys([]string{"a", "b"}); err != nil {
		t.Fatalf("DeclareCompositeNodeKeys: %v", err)
	}
	for id := store.NodeID(1); id <= 4; id++ {
		p.IndexNode(id, "a", []byte("x"))
		p.IndexNode(id, "b", []byte("y"))
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("a freshly built index must verify: %v", err)
	}
	idx, ok := p.nodeComposites.find([]string{"a", "b"})
	if !ok {
		t.Fatal("the declared composite is not registered")
	}
	return p, idx
}

func TestCompositeVerifyCatchesAMissingEntry(t *testing.T) {
	p, idx := compositeUnderTest(t)
	for tuple, ids := range idx.postings {
		idx.postings[tuple] = ids[1:]
	}
	if err := p.Verify(); err == nil {
		t.Error("verify accepted a composite that has dropped an entity its member values still imply")
	}
}

func TestCompositeVerifyCatchesAnExtraEntry(t *testing.T) {
	p, idx := compositeUnderTest(t)
	for tuple, ids := range idx.postings {
		idx.postings[tuple] = append(ids, store.NodeID(99))
		idx.entries++
	}
	if err := p.Verify(); err == nil {
		t.Error("verify accepted a composite filing an entity no member state produces")
	}
}

func TestCompositeVerifyCatchesACountThatDrifted(t *testing.T) {
	p, idx := compositeUnderTest(t)
	idx.entries++
	if err := p.Verify(); err == nil {
		t.Error("verify accepted a maintained entry count that no longer matches the entries")
	}
}

func TestCompositeVerifyCatchesUnsortedPostings(t *testing.T) {
	p, idx := compositeUnderTest(t)
	for _, ids := range idx.postings {
		ids[0], ids[1] = ids[1], ids[0]
	}
	if err := p.Verify(); err == nil {
		t.Error("verify accepted postings that are not ascending — the planner returns them " +
			"as a driving set and skips the sort on that promise")
	}
}

func TestCompositeVerifyCatchesARetainedEmptyTuple(t *testing.T) {
	p, idx := compositeUnderTest(t)
	idx.postings["orphan"] = nil
	if err := p.Verify(); err == nil {
		t.Error("verify accepted an empty postings list, which a removal should have deleted")
	}
}

// TestCompositeDeclarationIsValidated pins the rules at the index layer, where
// they are enforced, rather than only through the Graph façade.
func TestCompositeDeclarationIsValidated(t *testing.T) {
	p := NewPropertyIndex()
	bad := map[string][]string{
		"one key":  {"a"},
		"repeated": {"a", "a"},
		"empty":    {"a", ""},
	}
	for what, keys := range bad {
		if err := p.DeclareCompositeNodeKeys(keys); !errors.Is(err, ErrCompositeKeys) {
			t.Errorf("%s: got %v, want ErrCompositeKeys", what, err)
		}
	}
	if err := p.DeclareCompositeEdgeKeys([]string{"a", "b"}); err != nil {
		t.Errorf("a valid edge tuple was rejected: %v", err)
	}
	// Re-declaring is a no-op rather than an error: a caller re-running its
	// schema setup should not have to remember what it declared last time.
	if err := p.DeclareCompositeEdgeKeys([]string{"a", "b"}); err != nil {
		t.Errorf("re-declaring the same tuple: %v", err)
	}
	if got := p.CompositeEdgeKeys(); len(got) != 1 {
		t.Errorf("declared the same tuple twice and got %v", got)
	}
}
