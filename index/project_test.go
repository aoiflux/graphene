package index

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Projection invariants that need the shard map to state.
//
// The suite in tests/ covers what a projection answers. These cover the part
// that is invisible from outside the package: a shard holds whatever keys hash
// to it, and a projection asking for one of them must not be handed the others.
// Constructing that case needs shardIndexFor, so it lives here.

// collidingKeys returns two distinct key names owned by the same shard.
//
// It searches rather than hard-coding a pair, because the hash is an
// implementation detail and a hard-coded pair would stop testing anything the
// day it changed — silently, since both keys would still exist and the
// projection would still answer.
func collidingKeys(t *testing.T, p *PropertyIndex) (want, other string) {
	t.Helper()
	seen := map[int]string{}
	for i := 0; i < 4096; i++ {
		k := fmt.Sprintf("key-%d", i)
		s := p.shardIndexFor(k)
		if prev, ok := seen[s]; ok {
			return prev, k
		}
		seen[s] = k
	}
	t.Fatalf("no two of 4096 keys shared a shard, which cannot happen with %d shards", propertyShards)
	return "", ""
}

// A shard owns several keys. A projection naming one of them must return that
// one, and must not return the neighbours that happen to live beside it — the
// filter is on the key, not on the shard.
func TestProject_AKeySharingAShardIsNotReturned(t *testing.T) {
	p := NewPropertyIndex()
	want, other := collidingKeys(t, p)
	if p.shardIndexFor(want) != p.shardIndexFor(other) {
		t.Fatalf("fixture is wrong: %q and %q are not in one shard", want, other)
	}

	const n = 40
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id := store.NodeID(i + 1)
		p.IndexNode(id, want, []byte(fmt.Sprintf("want-%d", i)))
		p.IndexNode(id, other, []byte(fmt.Sprintf("other-%d", i)))
		ids = append(ids, id)
	}

	got := make([]string, n)
	p.ProjectNodes(ids, []string{want}, func(idIdx, keyIdx int, value []byte) bool {
		if keyIdx != 0 {
			t.Errorf("id %d: key index %d, want 0", idIdx, keyIdx)
		}
		if got[idIdx] != "" {
			t.Errorf("id %d: a second value %q arrived after %q — the shard's other key leaked",
				idIdx, value, got[idIdx])
		}
		got[idIdx] = string(value)
		return true
	})

	for i := range ids {
		if got[i] != fmt.Sprintf("want-%d", i) {
			t.Fatalf("id %d: got %q, want %q", i, got[i], fmt.Sprintf("want-%d", i))
		}
	}
}

// Asking for both keys of a shard returns both, at their own positions. The
// previous test proves the filter excludes; this one proves it does not
// over-exclude, which a filter that matched nothing would also pass.
func TestProject_BothKeysOfOneShardComeBackSeparately(t *testing.T) {
	p := NewPropertyIndex()
	first, second := collidingKeys(t, p)

	const n = 10
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id := store.NodeID(i + 1)
		p.IndexNode(id, first, []byte(fmt.Sprintf("a-%d", i)))
		p.IndexNode(id, second, []byte(fmt.Sprintf("b-%d", i)))
		ids = append(ids, id)
	}

	got := make([][2]string, n)
	p.ProjectNodes(ids, []string{first, second}, func(idIdx, keyIdx int, value []byte) bool {
		got[idIdx][keyIdx] = string(value)
		return true
	})
	for i := range ids {
		if got[i][0] != fmt.Sprintf("a-%d", i) || got[i][1] != fmt.Sprintf("b-%d", i) {
			t.Errorf("id %d: got %v", i, got[i])
		}
	}
}

// The callback's idIdx is a position in the caller's ids, not a position within
// whatever chunk the projection happened to resolve it in. With more ids than
// one chunk holds, those two numbers differ for every id past the first chunk,
// which is what makes this worth asserting separately.
func TestProject_PositionsAreIntoTheCallersIDsNotTheChunk(t *testing.T) {
	p := NewPropertyIndex()
	const n = projectChunk*2 + 37
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id := store.NodeID(i + 1)
		p.IndexNode(id, "k", []byte(fmt.Sprintf("v-%d", i)))
		ids = append(ids, id)
	}

	seen := make([]int, n)
	p.ProjectNodes(ids, []string{"k"}, func(idIdx, _ int, value []byte) bool {
		if idIdx < 0 || idIdx >= n {
			t.Fatalf("id index %d is outside the caller's %d ids", idIdx, n)
		}
		seen[idIdx]++
		if string(value) != fmt.Sprintf("v-%d", idIdx) {
			t.Fatalf("id index %d: got %q, want %q", idIdx, value, fmt.Sprintf("v-%d", idIdx))
		}
		return true
	})
	for i, c := range seen {
		if c != 1 {
			t.Fatalf("id index %d seen %d times, want 1", i, c)
		}
	}
}

// A value handed to the callback is a view into the batch's slab. It must be
// exactly the value and not the rest of the slab behind it, which a bound taken
// from the wrong end would silently be.
func TestProject_ValuesAreTheirOwnLengthNotTheSlabsRemainder(t *testing.T) {
	p := NewPropertyIndex()
	ids := []store.NodeID{1, 2, 3}
	for i, id := range ids {
		p.IndexNode(id, "k", []byte(fmt.Sprintf("%d", i)))
	}
	p.ProjectNodes(ids, []string{"k"}, func(idIdx, _ int, value []byte) bool {
		if len(value) != 1 {
			t.Errorf("id index %d: value %q is %d bytes, want 1", idIdx, value, len(value))
		}
		return true
	})
}
