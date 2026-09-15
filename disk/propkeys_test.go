package disk

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The key list a validator reads has to include the keys that are only in the
// image.
//
// This is the one real correctness risk in exposing it. A store opened on a v9
// image and not yet written to has every key it owns in the mapped base and none
// in the delta, so a list that read only the shards would be empty -- and a
// projection validated against it would reject every key the store actually
// carries, on the store shape this engine is built for. The union happens in
// index.PropertyIndex.nodePropKeys through baseSide.mergeKeys; this is the
// assertion that it is reached from a Store, through the reader, on the mapped
// path.
func TestPropKeys_UnionTheMappedBaseWithTheDelta(t *testing.T) {
	dir := v9Store(t)

	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if got := s.StorageStats().IndexMode; got != "mapped" {
		t.Fatalf("IndexMode = %q, want mapped: this test proves nothing over a heap index", got)
	}

	// Nothing has been written since the compaction, so every one of these is in
	// the base and none is in a shard.
	base := s.NodePropKeys()
	for _, want := range []string{"bucket", "seq", "shard"} {
		if _, found := slices.BinarySearch(base, want); !found {
			t.Errorf("NodePropKeys on a freshly opened mapped store omits %q, which is in "+
				"its image: %q", want, base)
		}
	}
	if !slices.IsSorted(base) {
		t.Errorf("NodePropKeys is not sorted, and the contract is that it is: %q", base)
	}
	if got := s.EdgePropKeys(); !slices.Contains(got, "rel") {
		t.Errorf("EdgePropKeys omits the image's edge key: %q", got)
	}

	// Now a key that exists only in the delta. Both sides have to be present at
	// once, which is the half a list taken from the base alone would fail.
	id := addNodeD(t, s, 0)
	if err := s.IndexNodeProperty(id, "written-after", []byte("v")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	both := s.NodePropKeys()
	if _, found := slices.BinarySearch(both, "written-after"); !found {
		t.Errorf("NodePropKeys omits a key written since the compaction: %q", both)
	}
	if _, found := slices.BinarySearch(both, "seq"); !found {
		t.Errorf("NodePropKeys dropped the image's keys once the delta had one: %q", both)
	}
	if !slices.IsSorted(both) {
		t.Errorf("NodePropKeys is not sorted with both sides present: %q", both)
	}

	// A key in both sides is named once. mergeKeys binary-searches the delta's
	// sorted prefix to drop the duplicate, and a duplicate here would be a
	// directory with two entries for one key.
	if err := s.IndexNodeProperty(id, "seq", []byte("9999")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	dup := s.NodePropKeys()
	n := 0
	for _, k := range dup {
		if k == "seq" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("a key held by both the base and the delta is named %d times: %q", n, dup)
	}
}

// A key nothing was ever indexed under is absent, which is the whole of what
// makes the list a validator.
//
// The direction matters and only one of the two is a guarantee. Absent means
// "matches nothing, for certain"; present means "may match", because under a
// mapped base a key whose entries have all been retracted is still named by the
// image. A validator rejects on absence and that is sound.
func TestPropKeys_AKeyNothingIndexedIsAbsent(t *testing.T) {
	dir := v9Store(t)
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	keys := s.NodePropKeys()
	if slices.Contains(keys, "sqe") {
		t.Errorf("a key never indexed is named: %q", keys)
	}
	if !slices.Contains(keys, "seq") {
		t.Fatalf("the fixture's own key is missing, so the negative above proves nothing: %q", keys)
	}
}

// RefuseUnindexedProjectionKeys turns the silence into an error, and changes
// nothing else.
//
// Both halves are the test. The option is opt-in and every existing caller is
// one that did not set it, so the lenient arm here is what says the default
// behaviour is unchanged -- and it is checked on the same store, the same ids and
// the same keys as the strict arm, so the only difference between the two is the
// option.
func TestStrictProjectionKeys_RefusesAKeyNothingIsIndexedUnder(t *testing.T) {
	dir := v9Store(t)

	lenient, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open lenient: %v", err)
	}
	defer lenient.Close()

	ids, err := lenient.NodesByProperty("seq", []byte("0000"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("the fixture resolved no ids, so nothing below is a projection")
	}

	// Lenient: no callback, no error. This is what a caller gets today.
	n := 0
	if err := lenient.ForEachNodeProjection(context.Background(), ids, []string{"sqe"},
		func(int, int, []byte) bool { n++; return true }); err != nil {
		t.Fatalf("the default refused an unindexed key: %v", err)
	}
	if n != 0 {
		t.Errorf("an unindexed key produced %d callbacks", n)
	}

	strict, err := OpenWithOptions(dir, Options{
		IndexMode:                     IndexMapped,
		ReadOnly:                      true,
		RefuseUnindexedProjectionKeys: true,
	})
	if err != nil {
		t.Fatalf("Open strict: %v", err)
	}
	defer strict.Close()

	err = strict.ForEachNodeProjection(context.Background(), ids, []string{"sqe"},
		func(int, int, []byte) bool { return true })
	if !errors.Is(err, ErrUnindexedProjectionKey) {
		t.Fatalf("strict mode returned %v, want ErrUnindexedProjectionKey", err)
	}
	if !strings.Contains(err.Error(), "sqe") {
		t.Errorf("the error does not name the key that was wrong: %v", err)
	}

	// A key that is indexed still works under the option, which is what stops
	// this from being a switch that simply breaks projections.
	n = 0
	if err := strict.ForEachNodeProjection(context.Background(), ids, []string{"seq"},
		func(int, int, []byte) bool { n++; return true }); err != nil {
		t.Fatalf("strict mode refused an indexed key: %v", err)
	}
	if n == 0 {
		t.Error("strict mode produced no callbacks for a key every id carries")
	}

	// And the first offender in request order, not an arbitrary one, so the
	// error is the same on every run.
	err = strict.ForEachNodeProjection(context.Background(), ids,
		[]string{"seq", "sqe", "bucekt"}, func(int, int, []byte) bool { return true })
	if !strings.Contains(err.Error(), "sqe") {
		t.Errorf("the error names %v, want the first unindexed key in request order", err)
	}
}

// Edges get the option too, read off their own key list.
//
// Worth its own case because the two lists are separate: a key indexed on nodes
// and never on edges is unindexed for an edge projection, and a check that read
// the node list would accept it.
func TestStrictProjectionKeys_ReadsTheEdgeKeyListForEdges(t *testing.T) {
	dir := v9Store(t)
	s, err := OpenWithOptions(dir, Options{
		IndexMode:                     IndexMapped,
		ReadOnly:                      true,
		RefuseUnindexedProjectionKeys: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	eids, err := s.EdgesByProperty("rel", []byte("r1"))
	if err != nil {
		t.Fatalf("EdgesByProperty: %v", err)
	}
	if len(eids) == 0 {
		t.Fatal("the fixture resolved no edge ids")
	}

	if err := s.ForEachEdgeProjection(context.Background(), eids, []string{"rel"},
		func(int, int, []byte) bool { return true }); err != nil {
		t.Fatalf("strict mode refused an indexed edge key: %v", err)
	}

	// "seq" is a node key in this fixture and is indexed on no edge at all.
	err = s.ForEachEdgeProjection(context.Background(), eids, []string{"seq"},
		func(int, int, []byte) bool { return true })
	if !errors.Is(err, ErrUnindexedProjectionKey) {
		t.Fatalf("an edge projection over a node-only key returned %v, want "+
			"ErrUnindexedProjectionKey: the two key lists are separate", err)
	}
}

// A key written since the last compaction is accepted, because the list the
// check reads is the union and not the image.
//
// This is the failure the option would have if it consulted the base alone: a
// caller who indexes a new key and immediately projects it would be told the key
// does not exist, which is both wrong and the exact shape of bug the option is
// supposed to catch.
func TestStrictProjectionKeys_AcceptsAKeyWrittenSinceTheCompaction(t *testing.T) {
	dir := v9Store(t)
	s, err := OpenWithOptions(dir, Options{
		IndexMode:                     IndexMapped,
		RefuseUnindexedProjectionKeys: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	id := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.IndexNodeProperty(id, "fresh", []byte("v")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}

	n := 0
	if err := s.ForEachNodeProjection(context.Background(), []store.NodeID{id},
		[]string{"fresh"}, func(int, int, []byte) bool { n++; return true }); err != nil {
		t.Fatalf("strict mode refused a key indexed a moment ago: %v", err)
	}
	if n != 1 {
		t.Errorf("the projection produced %d callbacks, want 1", n)
	}
}
