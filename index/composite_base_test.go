package index

import (
	"fmt"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Composites over a base.
//
// A composite is the one part of the index a base cannot answer in place: it is
// an index over a tuple of keys the forward direction holds separately, so
// answering one from the base would mean intersecting the member keys' runs on
// every query — exactly the work a composite exists to do once. So it is filled
// from the base, and these tests say when.

// compositeCorpus is four entities over two member keys, plus a third key that
// is nobody's member.
func compositeCorpus() []triple {
	var out []triple
	for id := uint64(1); id <= 4; id++ {
		out = append(out,
			triple{NodeKind, id, "bucket", []byte(fmt.Sprintf("b%d", id%2))},
			triple{NodeKind, id, "shard", []byte(fmt.Sprintf("s%d", id%3))},
			triple{NodeKind, id, "loose", []byte("x")},
			triple{EdgeKind, id, "rel", []byte(fmt.Sprintf("r%d", id%2))},
			triple{EdgeKind, id, "run", []byte("one")},
		)
	}
	return out
}

func nodeTuple(t *testing.T, p *PropertyIndex, keys, values []string) []store.NodeID {
	t.Helper()
	filters := make([]store.PropertyFilter, len(keys))
	for i := range keys {
		filters[i] = store.PropertyFilter{
			Key: keys[i], Op: store.PropertyOpEqual, Value: []byte(values[i]),
		}
	}
	m, ok := p.MatchNodeComposite(filters, store.MatchAll)
	if !ok {
		t.Fatalf("no composite matched %v", keys)
	}
	ids, ok := p.NodesByComposite(m)
	if !ok {
		t.Fatalf("the composite over %v answered nothing", keys)
	}
	slices.Sort(ids)
	return ids
}

// TestComposites_AreFilledFromTheBaseWhicheverOrderTheyArrive is the guard on the
// ordering that actually bites.
//
// A store restores its declarations from its catalogue *before* it loads the
// image, so at Open the composite exists and the base does not — and a fill that
// only ran when the composite was declared would leave every reopened store
// answering every composite query with no matches. Silently: an empty result, not
// a slow one, which is worse than the scan GORD exists to prevent.
//
// The other order is a composite declared on a store that has been open for a
// while. Both are asserted against the same expectation, because which one
// happened is not something a query can be allowed to reveal.
func TestComposites_AreFilledFromTheBaseWhicheverOrderTheyArrive(t *testing.T) {
	base := newFakeBase(compositeCorpus())
	for _, tc := range []struct {
		name         string
		declareFirst bool
	}{
		{"declared before the base is attached", true},
		{"declared after the base is attached", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPropertyIndex()
			declare := func() {
				if err := p.DeclareCompositeNodeKeys([]string{"bucket", "shard"}); err != nil {
					t.Fatalf("declare node composite: %v", err)
				}
				if err := p.DeclareCompositeEdgeKeys([]string{"rel", "run"}); err != nil {
					t.Fatalf("declare edge composite: %v", err)
				}
			}
			if tc.declareFirst {
				declare()
			}
			if err := p.AttachBase(base); err != nil {
				t.Fatalf("AttachBase: %v", err)
			}
			if !tc.declareFirst {
				declare()
			}

			// id 2 is bucket=b0, shard=s2; id 4 is bucket=b0, shard=s1.
			if got := nodeTuple(t, p, []string{"bucket", "shard"}, []string{"b0", "s2"}); !slices.Equal(got, []store.NodeID{2}) {
				t.Errorf("(b0,s2) = %v, want [2]", got)
			}
			if got := nodeTuple(t, p, []string{"bucket", "shard"}, []string{"b1", "s1"}); !slices.Equal(got, []store.NodeID{1}) {
				t.Errorf("(b1,s1) = %v, want [1]", got)
			}
			// A tuple nothing holds, which must be empty rather than absent.
			if got := nodeTuple(t, p, []string{"bucket", "shard"}, []string{"b1", "s2"}); len(got) != 0 {
				t.Errorf("(b1,s2) = %v, want nothing", got)
			}
			if err := p.Verify(); err != nil {
				t.Errorf("Verify: %v", err)
			}
		})
	}
}

// TestComposites_OverABaseTrackTheDeltaAndRetractions checks that filling from
// the base does not turn the composite into a second, stale copy of it.
//
// The composite is a resident structure, so it is the one place where "the base is
// immutable" stops being enough on its own: what was filed from the base has to be
// unfiled when the entity is removed, and what arrives afterwards has to be filed
// on the same terms. The registration and removal paths already do both — this
// asserts they still reach a composite whose contents came from the base rather
// than from IndexNode.
func TestComposites_OverABaseTrackTheDeltaAndRetractions(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.DeclareCompositeNodeKeys([]string{"bucket", "shard"}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := p.AttachBase(newFakeBase(compositeCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}

	// An entity the base never had.
	p.IndexNode(9, "bucket", []byte("b0"))
	p.IndexNode(9, "shard", []byte("s2"))
	if got := nodeTuple(t, p, []string{"bucket", "shard"}, []string{"b0", "s2"}); !slices.Equal(got, []store.NodeID{2, 9}) {
		t.Errorf("after a write, (b0,s2) = %v, want [2 9]", got)
	}

	// A base entity removed: the composite must lose it, which only happens
	// because it was filed there in the first place.
	p.RemoveNode(2)
	if got := nodeTuple(t, p, []string{"bucket", "shard"}, []string{"b0", "s2"}); !slices.Equal(got, []store.NodeID{9}) {
		t.Errorf("after removing 2, (b0,s2) = %v, want [9]", got)
	}

	// Re-registering a base triple the composite already holds must not double it.
	p.IndexNode(4, "bucket", []byte("b0"))
	p.IndexNode(4, "shard", []byte("s1"))
	if got := nodeTuple(t, p, []string{"bucket", "shard"}, []string{"b0", "s1"}); !slices.Equal(got, []store.NodeID{4}) {
		t.Errorf("(b0,s1) = %v, want [4] exactly once", got)
	}
	if err := p.Verify(); err != nil {
		t.Errorf("Verify: %v", err)
	}
}

// TestComposites_FillOnlyTouchesMemberKeys is the cost claim.
//
// The fill is proportional to the entries under the composite's member keys, not
// to the index. A store that declares no composite must read nothing from the
// base at all — attaching one is otherwise a pass over the whole index, which is
// the residency this item removes reappearing at open.
func TestComposites_FillOnlyTouchesMemberKeys(t *testing.T) {
	base := newFakeBase(compositeCorpus())

	none := NewPropertyIndex()
	if err := none.AttachBase(base); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	if n := base.valueWalks(); n != 0 {
		t.Errorf("attaching a base to an index with no composite walked %d keys", n)
	}

	base.resetCounts()
	one := NewPropertyIndex()
	if err := one.DeclareCompositeNodeKeys([]string{"bucket", "shard"}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := one.AttachBase(base); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	// The two member keys and nothing else: not "loose", which is indexed, and
	// not either edge key, since the composite is over node keys.
	if n := base.valueWalks(); n != 2 {
		t.Errorf("filling a two-key composite walked %d keys, want 2", n)
	}
}
