package index

// The columnar member store and its intern table.
//
// What these cover is the machinery that replaced a per-entity *memberState: the
// rows, the free lists on both sides, and the open-addressed probe. None of it
// is reachable through a wrong answer that the existing composite tests would
// see — a leaked value slot is bytes, a reused row is a wrong answer only once
// two entities share one — so each of these asserts the structure directly and
// then asks Verify to agree.

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func emptyCompositeUnderTest(t *testing.T, keys ...string) (*PropertyIndex, *compositeIndex[store.NodeID]) {
	t.Helper()
	p := NewPropertyIndex()
	if err := p.DeclareCompositeNodeKeys(keys); err != nil {
		t.Fatalf("DeclareCompositeNodeKeys: %v", err)
	}
	idx, ok := p.nodeComposites.find(keys)
	if !ok {
		t.Fatal("the declared composite is not registered")
	}
	return p, idx
}

// TestValueTable_FreesTheSlotOfAValueNoRowHolds is the leak this table would
// otherwise have.
//
// A composite over a key whose values never repeat registers one distinct value
// per entity. Without the reference counts, every one of those would hold a
// permanent slot, so a store that rewrites its derived layer would grow a table
// nothing reads — which is a worse outcome than the per-entity form this
// replaced, not a better one.
func TestValueTable_FreesTheSlotOfAValueNoRowHolds(t *testing.T) {
	p, idx := emptyCompositeUnderTest(t, "case", "bucket")

	const rounds = 20
	const perRound = 50
	for r := range rounds {
		for i := range perRound {
			id := store.NodeID(r*perRound + i + 1)
			p.IndexNode(id, "case", []byte(fmt.Sprintf("case-%d-%d", r, i)))
			p.IndexNode(id, "bucket", []byte(fmt.Sprintf("bucket-%d-%d", r, i)))
		}
		for i := range perRound {
			p.RemoveNode(store.NodeID(r*perRound + i + 1))
		}
		if err := p.Verify(); err != nil {
			t.Fatalf("round %d: %v", r, err)
		}
	}

	if live := idx.vtab.live; live != 0 {
		t.Errorf("every entity was removed but %d values are still referred to", live)
	}
	// Slots are reused rather than appended to, so the table never grew past
	// what one round held: two values per entity, at once.
	if n := len(idx.vtab.vals); n > 2*perRound {
		t.Errorf("the intern table holds %d slots after %d rounds of %d entities; "+
			"it should have reused them and stayed at %d",
			n, rounds, perRound, 2*perRound)
	}
}

// TestComposite_ReusesTheRowOfARemovedEntity covers the other free list.
func TestComposite_ReusesTheRowOfARemovedEntity(t *testing.T) {
	p, idx := emptyCompositeUnderTest(t, "case", "bucket")

	for id := store.NodeID(1); id <= 100; id++ {
		p.IndexNode(id, "case", []byte("c"))
		p.IndexNode(id, "bucket", []byte("b"))
	}
	grown := len(idx.slots)

	for id := store.NodeID(1); id <= 100; id++ {
		p.RemoveNode(id)
	}
	for id := store.NodeID(1001); id <= 1100; id++ {
		p.IndexNode(id, "case", []byte("c"))
		p.IndexNode(id, "bucket", []byte("b"))
	}

	if len(idx.slots) != grown {
		t.Errorf("the slots array grew from %d to %d over a delete-and-rewrite that "+
			"left the live count unchanged", grown, len(idx.slots))
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got := len(idx.lookup(baseSide[store.NodeID]{}, false, []string{"c", "b"})); got != 100 {
		t.Errorf("the rewritten entities answer %d rows, want 100", got)
	}
}

// TestValueTable_ATombstoneDoesNotHideALaterValue is the hazard specific to
// open addressing, and the reason bucketDead exists at all.
//
// Linear probing places a colliding value after the one it collided with. Empty
// the first bucket on removal and the probe for the second ends early, at a slot
// that now looks like it was never used — so a value that is present reports
// absent, the row interns a second copy of it, and two references then name the
// same bytes. Nothing downstream would notice: both references resolve.
//
// Forced by hammering a table small enough that collisions are certain, rather
// than by choosing values against a seed that is deliberately random.
func TestValueTable_ATombstoneDoesNotHideALaterValue(t *testing.T) {
	vt := newValueTable()
	held := map[string]int32{}

	rng := rand.New(rand.NewSource(1))
	for step := range 20000 {
		v := fmt.Sprintf("v%d", rng.Intn(64))
		if ref, ok := held[v]; ok && rng.Intn(2) == 0 {
			vt.unref(ref)
			delete(held, v)
			continue
		}
		ref := vt.ref(v)
		if prev, ok := held[v]; ok {
			// Already interned: the same value must come back as the same
			// reference, or two rows now disagree about what they hold.
			if prev != ref {
				t.Fatalf("step %d: %q interned as %d and then as %d", step, v, prev, ref)
			}
			vt.unref(ref) // keep exactly one count per held value
			continue
		}
		held[v] = ref
		if got := vt.at(ref); got != v {
			t.Fatalf("step %d: reference %d for %q reads back %q", step, ref, v, got)
		}
	}

	if err := vt.verify(); err != nil {
		t.Fatalf("after 20000 interleaved interns and releases: %v", err)
	}
	if vt.live != len(held) {
		t.Errorf("%d values are held but %d buckets are occupied", len(held), vt.live)
	}
	for v, ref := range held {
		got, _, ok := vt.find(v)
		if !ok {
			t.Fatalf("%q is held but the probe does not find it", v)
		}
		if got != ref {
			t.Fatalf("%q is held as %d but the probe finds %d", v, ref, got)
		}
	}
}

// TestComposite_MultiValuedPositionIsReleasedWithItsRow covers the overflow
// lists, which are the one part of a row that is not in the slots array and so
// the one part releaseRow could leave behind.
func TestComposite_MultiValuedPositionIsReleasedWithItsRow(t *testing.T) {
	p, idx := emptyCompositeUnderTest(t, "a", "b")

	p.IndexNode(1, "a", []byte("x"))
	p.IndexNode(1, "b", []byte("y1"))
	p.IndexNode(1, "b", []byte("y2"))
	if err := p.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if idx.entries != 2 {
		t.Fatalf("the cross product files %d entries, want 2", idx.entries)
	}
	if len(idx.extra) != 1 {
		t.Fatalf("%d overflow lists are held, want 1", len(idx.extra))
	}

	p.RemoveNode(1)

	if len(idx.extra) != 0 {
		t.Errorf("%d overflow lists survived the entity that owned them", len(idx.extra))
	}
	if live := idx.vtab.live; live != 0 {
		t.Errorf("%d values are still referred to after the only entity was removed", live)
	}
	if idx.entries != 0 {
		t.Errorf("%d entries survived, want 0", idx.entries)
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("verify after removal: %v", err)
	}
}

// TestComposite_RowReuseDoesNotCarryTheOldEntitysValues is what a released row
// must guarantee, and the failure it prevents is a wrong answer rather than a
// leak: an entity inheriting a position it never registered is filed under a
// tuple it does not hold.
func TestComposite_RowReuseDoesNotCarryTheOldEntitysValues(t *testing.T) {
	p, idx := emptyCompositeUnderTest(t, "a", "b")

	p.IndexNode(1, "a", []byte("x"))
	p.IndexNode(1, "b", []byte("y"))
	p.RemoveNode(1)

	// Only one position registered, so the new entity is incomplete and belongs
	// under no tuple at all — unless it inherited the released row's "b".
	p.IndexNode(2, "a", []byte("x"))

	if idx.entries != 0 {
		t.Errorf("%d entries are filed for an entity that holds one of two positions", idx.entries)
	}
	if got := idx.lookup(baseSide[store.NodeID]{}, false, []string{"x", "y"}); len(got) != 0 {
		t.Errorf("the reused row answers %v for a tuple no live entity holds", got)
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
}
