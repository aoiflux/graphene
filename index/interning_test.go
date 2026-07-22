package index

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/aoiflux/graphene/store"
)

// Value interning in the reverse index.
//
// A reverse entry used to keep the caller's own string. The forward index
// deduplicates by content, so a thousand nodes sharing one value left the
// forward side holding one string and the reverse side pinning a thousand
// copies — measured at ~32 B per entry.
//
// Interning unconditionally would be the wrong trade: a unique-per-node key like
// a hash would pay a table slot per value and save nothing. So values are
// interned only once they actually repeat.
//
// These assert on the *mechanism*, because the payoff is invisible to any
// functional test and a footprint number can regress silently. Sharing is
// checked by backing-pointer identity, which is the property that saves memory —
// content equality would pass even with every copy retained.

func backingPtr(s string) uintptr {
	if len(s) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(unsafe.StringData(s)))
}

// distinctString builds a string with the given content in a fresh allocation,
// so equal content never implies a shared backing array.
func distinctString(content string) string {
	b := make([]byte, len(content))
	copy(b, content)
	return string(b)
}

func collectRefValues(p *postings[store.NodeID], ids []store.NodeID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		p.forEachRef(id, func(r propRef) bool {
			out = append(out, r.value)
			return true
		})
	}
	return out
}

// TestRepeatedValuesShareOneBacking is the win: many entries, one copy.
func TestRepeatedValuesShareOneBacking(t *testing.T) {
	p := newPostings[store.NodeID]()

	const n = 200
	ids := make([]store.NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = store.NodeID(i + 1)
		// A fresh allocation every time, exactly as a caller parsing input would
		// produce. Without interning each of these stays alive.
		p.add(ids[i], "bucket", distinctString("hot"))
	}

	vals := collectRefValues(&p, ids)
	if len(vals) != n {
		t.Fatalf("got %d reverse values, want %d", len(vals), n)
	}
	for _, v := range vals {
		if v != "hot" {
			t.Fatalf("value = %q, want %q", v, "hot")
		}
	}

	backings := map[uintptr]int{}
	for _, v := range vals {
		backings[backingPtr(v)]++
	}
	// The first entry keeps its own string (it is also the forward bucket's key,
	// so it is shared by construction); everything after shares one canonical
	// copy. Two distinct backings total, not n.
	if len(backings) > 2 {
		t.Errorf("%d entries are held in %d separate allocations, want at most 2",
			n, len(backings))
	}
}

// TestUniqueValuesCostNothing is the other half of the trade: a key whose values
// never repeat must not populate the table at all.
func TestUniqueValuesCostNothing(t *testing.T) {
	p := newPostings[store.NodeID]()

	const n = 200
	for i := 0; i < n; i++ {
		p.add(store.NodeID(i+1), "sha256", distinctString(fmt.Sprintf("hash-%06d", i)))
	}

	if len(p.shared) != 0 {
		t.Errorf("all-distinct key populated the intern table with %d entries, want 0",
			len(p.shared))
	}
}

// TestInternTableReleasedWithBucket guards against churn leaking: once a value
// has no entries left, its canonical string must not be retained.
func TestInternTableReleasedWithBucket(t *testing.T) {
	p := newPostings[store.NodeID]()

	ids := []store.NodeID{1, 2, 3}
	for _, id := range ids {
		p.add(id, "bucket", distinctString("warm"))
	}
	if len(p.shared) != 1 {
		t.Fatalf("intern table has %d entries after repeats, want 1", len(p.shared))
	}

	for _, id := range ids {
		p.remove(id)
	}
	if len(p.shared) != 0 {
		t.Errorf("intern table still holds %d entries after every id was removed",
			len(p.shared))
	}
	if len(p.byKey) != 0 {
		t.Errorf("forward index still holds %d keys", len(p.byKey))
	}
}

// TestInterningPreservesLookups is the plain correctness check: substituting a
// content-equal string must not change what the index answers.
func TestInterningPreservesLookups(t *testing.T) {
	p := newPostings[store.NodeID]()

	for i := 1; i <= 50; i++ {
		v := "cold"
		if i%2 == 0 {
			v = "hot"
		}
		p.add(store.NodeID(i), "bucket", distinctString(v))
		p.add(store.NodeID(i), "sha256", distinctString(fmt.Sprintf("h%03d", i)))
	}

	hot := p.byKey["bucket"]["hot"]
	if len(hot) != 25 {
		t.Errorf("bucket=hot has %d ids, want 25", len(hot))
	}
	if got := len(p.byKey["sha256"]); got != 50 {
		t.Errorf("sha256 has %d distinct values, want 50", got)
	}

	// Removing by a *different* string object with equal content must still work,
	// since removal looks the value up by content.
	p.remove(2)
	if len(p.byKey["bucket"]["hot"]) != 24 {
		t.Errorf("bucket=hot has %d ids after removing one, want 24",
			len(p.byKey["bucket"]["hot"]))
	}
	if p.hasRefs(2) {
		t.Error("id 2 still has reverse entries after removal")
	}
}
