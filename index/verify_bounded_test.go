package index

import (
	"runtime"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Verification that does not grow with the index.
//
// VerifyIndexes is the routine an operator reaches for when something is
// already wrong, which on this engine is most often that the store has grown
// past what the machine has. A check that allocates a second copy of the index
// to check the first is a check that cannot be run at the moment it is needed.
//
// Three structures used to do exactly that — the id list IndexedNodeIDs
// returned, the map[tuple]set a composite rebuilt, and the map[id]map[ref]int
// the postings cross-check accumulated. This file holds the guard that says
// they are gone, and the tests for the paths that replaced them, which the
// existing suite did not reach: a composite wider than two keys, and one whose
// members are multi-valued and therefore imply more than one tuple each.

// --- the guard ---

// verifyAllocBytes reports how many bytes Verify allocates over an index
// holding entities × keys entries.
func verifyAllocBytes(t *testing.T, entities int, keys []string) uint64 {
	t.Helper()

	p := NewPropertyIndex()
	for id := store.NodeID(1); id <= store.NodeID(entities); id++ {
		for _, k := range keys {
			p.IndexNode(id, k, []byte("v"))
		}
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("a freshly built index must verify: %v", err)
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if err := p.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(p)
	return after.TotalAlloc - before.TotalAlloc
}

// TestVerify_AllocationDoesNotTrackTheIndexSize asserts a slope, not a ceiling.
//
// A ceiling is the wrong assertion here for the reason TestAllocGuards_Paths
// records: it passes for the wrong reason on a small fixture and has to be
// raised every time anything legitimately grows. What has to be true is that
// verifying twice as much index costs no more memory, and that is a slope of
// about zero.
//
// The threshold is deliberately loose — sixteen bytes per entry — because it is
// not trying to measure the new cost. It is trying to be a wall that the three
// structures this change removed cannot get back over: the smallest of them was
// a map entry plus a slice slot per entity, and the largest was a map header
// per entity. Anything that reintroduces one of those lands in the hundreds.
func TestVerify_AllocationDoesNotTrackTheIndexSize(t *testing.T) {
	keys := []string{"alpha", "beta", "gamma", "delta"}

	const small, large = 2000, 8000
	smallBytes := verifyAllocBytes(t, small, keys)
	largeBytes := verifyAllocBytes(t, large, keys)

	entryDelta := (large - small) * len(keys)
	if largeBytes <= smallBytes {
		t.Logf("verify allocated %d bytes at %d entries and %d at %d — flat or better",
			smallBytes, small*len(keys), largeBytes, large*len(keys))
		return
	}
	perEntry := float64(largeBytes-smallBytes) / float64(entryDelta)
	t.Logf("verify allocated %d bytes at %d entries and %d at %d: %.2f B/entry marginal",
		smallBytes, small*len(keys), largeBytes, large*len(keys), perEntry)
	if perEntry > 16 {
		t.Errorf("verification allocates %.2f bytes per indexed entry — it is holding a structure "+
			"proportional to the index, which is what makes it unrunnable on the store that needs it",
			perEntry)
	}
}

// TestForEachIndexedNodeID_YieldsEveryEntityAndStopsEarly pins the contract the
// two integrity checks now rely on, including the part that is a caveat rather
// than a guarantee.
func TestForEachIndexedNodeID_YieldsEveryEntityAndStopsEarly(t *testing.T) {
	p := NewPropertyIndex()
	const entities = 40
	// Several keys, so entities land in more than one shard's reverse map and
	// the documented repetition actually happens.
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for id := store.NodeID(1); id <= entities; id++ {
		for _, k := range keys {
			p.IndexNode(id, k, []byte("v"))
		}
	}

	seen := map[store.NodeID]int{}
	p.ForEachIndexedNodeID(func(id store.NodeID) bool {
		seen[id]++
		return true
	})
	if len(seen) != entities {
		t.Errorf("walked %d distinct entities, want %d", len(seen), entities)
	}
	for id := store.NodeID(1); id <= entities; id++ {
		if seen[id] == 0 {
			t.Errorf("entity %d has entries but was never yielded", id)
		}
	}

	// Early return must stop the walk, which is what lets a check abandon on a
	// cancelled context or on the first inconsistency.
	count := 0
	p.ForEachIndexedNodeID(func(store.NodeID) bool {
		count++
		return false
	})
	if count != 1 {
		t.Errorf("returning false from the callback yielded %d ids, want 1", count)
	}
}

// --- the composite paths the old fixture never reached ---

// wideCompositeUnderTest builds a three-key composite whose middle position is
// multi-valued, so every entity implies two tuples rather than one.
//
// Both properties matter and neither was covered: the existing fixture is two
// keys with one value each, which takes forEachTuple's single-tuple fast path
// (composite_index.go) and never builds the cross product at all.
func wideCompositeUnderTest(t *testing.T) (*PropertyIndex, *compositeIndex[store.NodeID]) {
	t.Helper()

	p := NewPropertyIndex()
	if err := p.DeclareCompositeNodeKeys([]string{"a", "b", "c"}); err != nil {
		t.Fatalf("DeclareCompositeNodeKeys: %v", err)
	}
	for id := store.NodeID(1); id <= 4; id++ {
		p.IndexNode(id, "a", []byte("x"))
		p.IndexNode(id, "b", []byte("y1"))
		p.IndexNode(id, "b", []byte("y2"))
		p.IndexNode(id, "c", []byte("z"))
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("a freshly built multi-valued composite must verify: %v", err)
	}

	idx, ok := p.nodeComposites.find([]string{"a", "b", "c"})
	if !ok {
		t.Fatal("the declared composite is not registered")
	}
	if len(idx.postings) != 2 {
		t.Fatalf("the fixture must exercise the cross product: got %d tuples, want 2", len(idx.postings))
	}
	if idx.entries != 8 {
		t.Fatalf("the fixture must file two tuples per entity: got %d entries, want 8", idx.entries)
	}
	return p, idx
}

func TestCompositeVerify_WideAndMultiValuedCatchesAnExtraEntry(t *testing.T) {
	p, idx := wideCompositeUnderTest(t)
	for tuple, ids := range idx.postings {
		idx.postings[tuple] = append(ids, store.NodeID(99))
		idx.entries++
		break
	}
	err := p.Verify()
	if err == nil {
		t.Fatal("verify accepted a composite filing an entity no member state produces")
	}
	// The diagnosis, not just the count: which entity is filed wrongly is the
	// whole value of the message to whoever has to act on it.
	if !strings.Contains(err.Error(), "99") {
		t.Errorf("the error does not name the offending entity: %v", err)
	}
}

func TestCompositeVerify_WideAndMultiValuedCatchesAMissingEntry(t *testing.T) {
	p, idx := wideCompositeUnderTest(t)
	for tuple, ids := range idx.postings {
		idx.postings[tuple] = ids[1:]
		idx.entries--
		break
	}
	if err := p.Verify(); err == nil {
		t.Error("verify accepted a composite that dropped an entity its member values still imply")
	}
}

// TestCompositeVerify_CatchesADroppedTupleFromTheCrossProduct removes one whole
// tuple of the cross product, which is the corruption a two-key single-valued
// fixture cannot express: the entity is still correctly filed under its other
// tuple, so only the cross-product walk notices.
func TestCompositeVerify_CatchesADroppedTupleFromTheCrossProduct(t *testing.T) {
	p, idx := wideCompositeUnderTest(t)
	for tuple := range idx.postings {
		n := len(idx.postings[tuple])
		delete(idx.postings, tuple)
		idx.entries -= n
		break
	}
	if err := p.Verify(); err == nil {
		t.Error("verify accepted a composite missing an entire tuple of the cross product")
	}
}

// TestCompositeVerify_CatchesARepeatedValueAtOnePosition covers a corruption
// the old verify absorbed rather than reported.
//
// forEachTuple enumerates the cross product with no memory of what it has
// produced, so a position holding the same value twice yields the same tuple
// twice. The old check collected tuples into a set, which quietly deduplicated
// them; add() will not produce this state, so it can only arrive by damage, and
// damage is what verify is for.
func TestCompositeVerify_CatchesARepeatedValueAtOnePosition(t *testing.T) {
	p, idx := wideCompositeUnderTest(t)
	m := idx.members[store.NodeID(1)]
	if m == nil {
		t.Fatal("the fixture entity has no member state")
	}
	m.more[1] = append(m.more[1], m.one[1])

	err := p.Verify()
	if err == nil {
		t.Fatal("verify accepted a member state holding the same value twice at one position")
	}
	if !strings.Contains(err.Error(), "same value twice") {
		t.Errorf("the error does not say what is wrong with the member state: %v", err)
	}
}

// TestCompositeVerify_CatchesAValueBeyondTheDeclaredWidth covers the other
// masked corruption: forEachTuple only ever walks positions below the width, so
// a value stored past it is never enumerated and never contradicts anything.
func TestCompositeVerify_CatchesAValueBeyondTheDeclaredWidth(t *testing.T) {
	p, idx := wideCompositeUnderTest(t)
	m := idx.members[store.NodeID(1)]
	if m == nil {
		t.Fatal("the fixture entity has no member state")
	}
	m.more[7] = []string{"unreachable"}

	err := p.Verify()
	if err == nil {
		t.Fatal("verify accepted a member value stored outside the declared width")
	}
	if !strings.Contains(err.Error(), "outside the declared width") {
		t.Errorf("the error does not identify the out-of-range position: %v", err)
	}
}

// TestCompositeVerify_CatchesTwoErrorsThatCancelOut is the test the counting
// argument stands or falls on.
//
// The second direction is established by comparing totals rather than by
// remembering every pair, and totals are blind to two faults that offset: an
// entity that should be filed and is not, alongside one that is filed and
// should not be. What makes the argument sound anyway is the probe in the first
// loop — implied ⊆ filed is checked pair by pair, so only |implied| = |filed|
// is left to the count.
//
// Substituting one id for another inside a tuple's list produces exactly that
// pair of faults, with every count in the index still correct. Remove the probe
// and this index verifies clean.
func TestCompositeVerify_CatchesTwoErrorsThatCancelOut(t *testing.T) {
	p, idx := wideCompositeUnderTest(t)

	var swapped bool
	for _, ids := range idx.postings {
		// The last position keeps the list ascending, so the substitution is
		// not caught by the ordering check on its way past.
		if len(ids) > 0 {
			ids[len(ids)-1] = store.NodeID(99)
			swapped = true
			break
		}
	}
	if !swapped {
		t.Fatal("the fixture has no postings to corrupt")
	}

	if err := p.Verify(); err == nil {
		t.Error("verify accepted an index where one entity's filing was given to another: " +
			"every count still agrees, and only a per-pair check can see it")
	}
}

func TestCompositeVerify_CatchesACountThatDrifted(t *testing.T) {
	p, idx := wideCompositeUnderTest(t)
	idx.entries++
	if err := p.Verify(); err == nil {
		t.Error("verify accepted a maintained entry count that no longer matches the entries")
	}
}

// --- the postings cross-check, which no longer holds a copy to compare against ---

// postingsUnderTest returns an index whose node postings are reachable for
// corruption, along with the shard holding key "a".
func postingsUnderTest(t *testing.T) (*PropertyIndex, *propertyShard) {
	t.Helper()

	p := NewPropertyIndex()
	for id := store.NodeID(1); id <= 6; id++ {
		p.IndexNode(id, "a", []byte("x"))
	}
	if err := p.Verify(); err != nil {
		t.Fatalf("a freshly built index must verify: %v", err)
	}
	return p, p.shardFor("a")
}

// TestPostingsVerify_CatchesAPostingWithNoReverseEntry is the check that used
// to fall out of the accumulated map. It now costs a second walk, and only when
// the totals already disagree — so this test is also what says that walk runs.
func TestPostingsVerify_CatchesAPostingWithNoReverseEntry(t *testing.T) {
	p, sh := postingsUnderTest(t)

	sh.mu.Lock()
	sh.nodes.byKey["a"]["x"] = append(sh.nodes.byKey["a"]["x"], store.NodeID(77))
	sh.nodes.count++
	sh.mu.Unlock()

	err := p.Verify()
	if err == nil {
		t.Fatal("verify accepted a posting for an entity with no reverse entry")
	}
	if !strings.Contains(err.Error(), "77") {
		t.Errorf("the error does not name the orphaned entity: %v", err)
	}
}

// TestPostingsVerify_CatchesAReverseRefWithNoPosting is the other direction,
// which is now a probe rather than a lookup into a rebuilt map.
func TestPostingsVerify_CatchesAReverseRefWithNoPosting(t *testing.T) {
	p, sh := postingsUnderTest(t)

	sh.mu.Lock()
	ids := sh.nodes.byKey["a"]["x"]
	sh.nodes.byKey["a"]["x"] = ids[1:]
	sh.nodes.count--
	sh.mu.Unlock()

	err := p.Verify()
	if err == nil {
		t.Fatal("verify accepted a reverse ref naming a posting that is not there")
	}
	if !strings.Contains(err.Error(), "no matching posting") {
		t.Errorf("the error does not say the posting is missing: %v", err)
	}
}

// TestPostingsVerify_CatchesTwoErrorsThatCancelOut is the postings-side twin of
// TestCompositeVerify_CatchesTwoErrorsThatCancelOut, and for the same reason:
// the reverse and forward totals are compared rather than the sets, so a fault
// in each direction leaves both totals right. Only the per-ref probe sees it.
func TestPostingsVerify_CatchesTwoErrorsThatCancelOut(t *testing.T) {
	p, sh := postingsUnderTest(t)

	sh.mu.Lock()
	ids := sh.nodes.byKey["a"]["x"]
	// Substituted at the end so the list stays ascending: the entity that held
	// this slot now has a reverse ref with no posting, and the one that took it
	// has a posting with no reverse ref. Nothing counts differently.
	ids[len(ids)-1] = store.NodeID(77)
	sh.mu.Unlock()

	if err := p.Verify(); err == nil {
		t.Error("verify accepted postings and reverse refs that disagree about two entities " +
			"in opposite directions — the totals cannot see it, and the probe is what must")
	}
}

// TestPostingsVerify_CatchesADuplicatedReverseRef covers the invariant that
// makes the counting argument sound: the reverse side has exactly one entry per
// (entity, key, value), so its total is comparable with the postings total.
// Without this check a duplicated ref would inflate the reverse total and mask
// a genuinely missing posting.
func TestPostingsVerify_CatchesADuplicatedReverseRef(t *testing.T) {
	p, sh := postingsUnderTest(t)

	sh.mu.Lock()
	id := store.NodeID(1)
	ref, inline := sh.nodes.ref1[id]
	if !inline {
		sh.mu.Unlock()
		t.Fatal("the fixture entity should hold exactly one reverse ref")
	}
	delete(sh.nodes.ref1, id)
	sh.nodes.refN[id] = []propRef{ref, ref}
	sh.mu.Unlock()

	err := p.Verify()
	if err == nil {
		t.Fatal("verify accepted an entity holding the same reverse ref twice")
	}
	if !strings.Contains(err.Error(), "twice") {
		t.Errorf("the error does not say the ref is duplicated: %v", err)
	}
}

// TestPostingsVerify_CatchesAReverseRefNamingAnUninternedKey guards the probe
// itself: it resolves a ref's key through keyNames, and an out-of-range id
// there would panic inside the routine whose job is to report damage.
func TestPostingsVerify_CatchesAReverseRefNamingAnUninternedKey(t *testing.T) {
	p, sh := postingsUnderTest(t)

	sh.mu.Lock()
	id := store.NodeID(1)
	ref := sh.nodes.ref1[id]
	ref.keyID = uint32(len(sh.nodes.keyNames) + 5)
	sh.nodes.ref1[id] = ref
	sh.mu.Unlock()

	err := p.Verify()
	if err == nil {
		t.Fatal("verify accepted a reverse ref naming a key that was never interned")
	}
	if !strings.Contains(err.Error(), "interned") {
		t.Errorf("the error does not identify the unknown key id: %v", err)
	}
}
