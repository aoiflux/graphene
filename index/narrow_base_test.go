package index

// Narrowing a candidate set against a base.
//
// A residual filter is applied one of two ways. Either the filter's own set is
// built and intersected — the forward direction, which has read through the base
// since it acquired one — or each candidate is asked what it is indexed under and
// the answer is tested. The second is the *reverse* direction, and the delta's
// reverse direction knows only about entities written since the last compaction.
//
// So a probe that consulted the delta alone concluded "no entry under this key,
// therefore no match" about every entity the image holds, and a type-narrowed
// property query returned an empty result rather than a wrong one. That was the
// last read path base ∪ delta − retracted had not reached, and nothing found it
// until the mapped index became the default, because until then nothing attached a
// base outside this package's own tests.

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// probeCorpus is a base whose entities the delta knows nothing about, which is the
// state every entity in a compacted image is in.
func probeCorpus() []triple {
	return []triple{
		{NodeKind, 1, "size", []byte("10")},
		{NodeKind, 1, "name", []byte("alpha")},
		{NodeKind, 2, "size", []byte("25")},
		{NodeKind, 2, "name", []byte("beta")},
		{NodeKind, 3, "size", []byte("40")},
		{EdgeKind, 7, "kind", []byte("near")},
		{EdgeKind, 8, "kind", []byte("far")},
	}
}

func TestNarrow_ProbesTheBaseAndNotOnlyTheDelta(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(probeCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}

	// One candidate against a filter whose set is larger, so planResiduals chooses
	// the probe. That choice is what this test is about: the same query answered by
	// building the set was already right.
	for _, tc := range []struct {
		name   string
		f      store.PropertyFilter
		cands  []store.NodeID
		expect []store.NodeID
	}{
		{"equal", store.PropertyFilter{Key: "size", Op: store.PropertyOpEqual, Value: []byte("25")},
			[]store.NodeID{1, 2, 3}, []store.NodeID{2}},
		{"gte", store.PropertyFilter{Key: "size", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("25")},
			[]store.NodeID{1, 2, 3}, []store.NodeID{2, 3}},
		{"contains", store.PropertyFilter{Key: "name", Op: store.PropertyOpContains, Value: []byte("et")},
			[]store.NodeID{1, 2, 3}, []store.NodeID{2}},
		{"absent key", store.PropertyFilter{Key: "name", Op: store.PropertyOpEqual, Value: []byte("alpha")},
			[]store.NodeID{1, 2, 3}, []store.NodeID{1}},
		{"no match", store.PropertyFilter{Key: "size", Op: store.PropertyOpEqual, Value: []byte("99")},
			[]store.NodeID{1, 2, 3}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.probeNodes(append([]store.NodeID(nil), tc.cands...), tc.f, &store.CancelCheck{})
			if err != nil {
				t.Fatalf("probeNodes: %v", err)
			}
			if len(got) != len(tc.expect) {
				t.Fatalf("probeNodes = %v, want %v", got, tc.expect)
			}
			for i := range got {
				if got[i] != tc.expect[i] {
					t.Fatalf("probeNodes = %v, want %v", got, tc.expect)
				}
			}
		})
	}

	got, err := p.probeEdges([]store.EdgeID{7, 8},
		store.PropertyFilter{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("near")},
		&store.CancelCheck{})
	if err != nil {
		t.Fatalf("probeEdges: %v", err)
	}
	if len(got) != 1 || got[0] != 7 {
		t.Fatalf("probeEdges = %v, want [7]", got)
	}
}

// TestNarrow_ProbeHonoursRetractionAndTheDelta: the probe reads the same three
// terms every other path reads, so a retracted entity must stop matching and one
// the delta has re-registered must match on the delta's value.
func TestNarrow_ProbeHonoursRetractionAndTheDelta(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(probeCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	f := store.PropertyFilter{Key: "size", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("10")}

	// A node written after the base: only the delta holds it.
	p.IndexNode(9, "size", []byte("70"))
	got, err := p.probeNodes([]store.NodeID{1, 2, 3, 9}, f, &store.CancelCheck{})
	if err != nil {
		t.Fatalf("probeNodes: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("probeNodes = %v, want all four", got)
	}

	// And one removed after it: the base still carries its entries and the probe
	// must not find them.
	p.RemoveNode(2)
	got, err = p.probeNodes([]store.NodeID{1, 2, 3, 9}, f, &store.CancelCheck{})
	if err != nil {
		t.Fatalf("probeNodes: %v", err)
	}
	for _, id := range got {
		if id == 2 {
			t.Fatalf("a retracted node still matches the probe: %v", got)
		}
	}
	if len(got) != 3 {
		t.Fatalf("probeNodes = %v, want the other three", got)
	}
}

// TestNarrow_ProbeUsesTheDeclaredComparison: a key compares by the rule it was
// declared with, and the probe over the base has to pick that rule the same way the
// probe over the delta does — or one entity gets two answers depending on which side
// happens to hold its entry.
//
// Both arms, because either alone passes with the rule hard-coded. The values are
// numeric-looking and of unequal length, which is exactly where the two rules part
// company: an undeclared key compares "9" and "10" as numbers and a declared one
// compares them as bytes, where "9" is the larger.
func TestNarrow_ProbeUsesTheDeclaredComparison(t *testing.T) {
	corpus := []triple{
		{NodeKind, 1, "n", []byte("9")},
		{NodeKind, 2, "n", []byte("10")},
	}
	f := store.PropertyFilter{Key: "n", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("10")}

	for _, tc := range []struct {
		name    string
		ordered bool
		want    []store.NodeID
	}{
		{"declared ordered, so bytewise", true, []store.NodeID{1, 2}},
		{"undeclared, so numeric", false, []store.NodeID{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPropertyIndex()
			if err := p.AttachBase(newFakeBase(corpus)); err != nil {
				t.Fatalf("AttachBase: %v", err)
			}
			if tc.ordered {
				p.DeclareOrderedNodeKey("n")
			}
			got, err := p.probeNodes([]store.NodeID{1, 2}, f, &store.CancelCheck{})
			if err != nil {
				t.Fatalf("probeNodes: %v", err)
			}
			if len(got) != len(tc.want) || (len(got) > 0 && got[0] != tc.want[0]) {
				t.Fatalf("probeNodes = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNarrow_ProbeRecordsAFaultRatherThanSilence: a run that will not decode is
// damage, and the probe has no error to return — so it must record the fault where
// BaseFault finds it rather than report an unreadable entry as an absent one.
func TestNarrow_ProbeRecordsAFaultRatherThanSilence(t *testing.T) {
	p := NewPropertyIndex()
	b := newFakeBase(probeCorpus())
	b.failKey, b.failErr = "size", errors.New("the run will not decode")
	if err := p.AttachBase(b); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	if _, err := p.probeNodes([]store.NodeID{1}, store.PropertyFilter{
		Key: "size", Op: store.PropertyOpEqual, Value: []byte("10"),
	}, &store.CancelCheck{}); err != nil {
		t.Fatalf("probeNodes: %v", err)
	}
	if p.BaseFault() == nil {
		t.Fatal("a probe that could not read the base reported nothing")
	}
}
