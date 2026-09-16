package index

// What the capture has to be true for.
//
// A compaction pins the index, writes a base from what it held at that instant,
// and commits some time later. Everything in these tests is that shape: register
// a few entries, open a capture, mutate, then swap in a base built from the
// pre-capture state. The base is the image; the tail is the difference; the
// assertion is always that the index answers afterwards exactly what it answered
// before the swap.
//
// The interesting case is not the entry added during the build -- a second set of
// shards would carry that. It is the entry *removed* during the build, which lives
// in the image because it was alive when the records were pinned, and which
// nothing but a replayed call puts back out of sight.

import (
	"slices"
	"testing"
	"unsafe"

	"github.com/aoiflux/graphene/store"
)

// TailOpBytes is a constant so that index/tail.go can stay out of the reviewed
// unsafe list, which leaves this to hold the one property the constant has to
// have: it may over-charge a build, never under-charge one.
func TestTail_ChargeIsNeverAnUndercharge(t *testing.T) {
	if got := int64(unsafe.Sizeof(tailOp{})); TailOpBytes < got {
		t.Errorf("a recorded mutation is charged %d bytes and occupies %d; "+
			"a capture's limit would let it hold more than it claims", TailOpBytes, got)
	}
}

// pinnedBase builds the base a compaction would have written from these triples.
func pinnedBase(t *testing.T, triples []triple) Base {
	t.Helper()
	return newFakeBase(triples)
}

func nodeIDs(p *PropertyIndex, key, value string) []store.NodeID {
	return p.NodesByProperty(key, []byte(value))
}

// The three directions in one fixture: an entry added during the build, an entry
// removed during the build, and an entry that was there all along.
func TestTail_SwapCarriesTheBuildWindowAcross(t *testing.T) {
	p := NewPropertyIndex()
	for _, n := range []struct {
		id    uint64
		value string
	}{{1, "a"}, {2, "b"}, {3, "c"}} {
		p.IndexNode(store.NodeID(n.id), "k", []byte(n.value))
	}
	base := pinnedBase(t, []triple{
		{NodeKind, 1, "k", []byte("a")},
		{NodeKind, 2, "k", []byte("b")},
		{NodeKind, 3, "k", []byte("c")},
	})

	tail := p.CaptureTail(1 << 20)
	p.IndexNode(4, "k", []byte("d"))
	p.RemoveNode(2)
	if p.StopCapture() != tail {
		t.Fatalf("StopCapture returned a different log than CaptureTail opened")
	}

	want := map[string][]store.NodeID{
		"a": {1},
		"b": nil,
		"c": {3},
		"d": {4},
	}
	before := map[string][]store.NodeID{}
	for v := range want {
		before[v] = nodeIDs(p, "k", v)
	}
	if err := p.SwapBase(base, tail); err != nil {
		t.Fatalf("SwapBase: %v", err)
	}
	for v, w := range want {
		if got := nodeIDs(p, "k", v); !slices.Equal(got, w) {
			t.Errorf("k=%s answers %v after the swap, want %v", v, got, w)
		}
		if got := nodeIDs(p, "k", v); !slices.Equal(got, before[v]) {
			t.Errorf("k=%s changed across the swap: %v became %v", v, before[v], got)
		}
	}
}

// The same swap without the tail, which is what the old gate existed to prevent
// and what a second set of shards would not have prevented.
//
// Asserting the wrong answers rather than skipping them, because the tail's whole
// justification is that these two are what happens without it. A test that only
// checked the good path would pass just as well against a SwapBase that ignored
// its tail argument.
func TestTail_WithoutItTheWindowIsLostInBothDirections(t *testing.T) {
	p := NewPropertyIndex()
	p.IndexNode(1, "k", []byte("a"))
	p.IndexNode(2, "k", []byte("b"))
	base := pinnedBase(t, []triple{
		{NodeKind, 1, "k", []byte("a")},
		{NodeKind, 2, "k", []byte("b")},
	})

	p.IndexNode(3, "k", []byte("c")) // after the pin: not in the base
	p.RemoveNode(2)                  // after the pin: the base still holds it

	if err := p.SwapBase(base, nil); err != nil {
		t.Fatalf("SwapBase: %v", err)
	}
	if got := nodeIDs(p, "k", "c"); len(got) != 0 {
		t.Errorf("k=c answers %v; this fixture is not the one the test describes", got)
	}
	if got := nodeIDs(p, "k", "b"); !slices.Equal(got, []store.NodeID{2}) {
		t.Errorf("k=b answers %v, want [2]: the deleted node was expected back "+
			"from the image, which is the direction a tail has to close", got)
	}
}

// Order, because a log replayed out of order is a different index. The last call
// for an id is what its state is, and both orderings have to come out that way.
func TestTail_ReplaysInOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(p *PropertyIndex)
		want []store.NodeID
	}{
		{
			name: "registered then removed",
			mut: func(p *PropertyIndex) {
				p.IndexNode(9, "k", []byte("z"))
				p.RemoveNode(9)
			},
			want: nil,
		},
		{
			name: "removed then registered",
			mut: func(p *PropertyIndex) {
				p.RemoveNode(9)
				p.IndexNode(9, "k", []byte("z"))
			},
			want: []store.NodeID{9},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPropertyIndex()
			p.IndexNode(1, "k", []byte("a"))
			base := pinnedBase(t, []triple{{NodeKind, 1, "k", []byte("a")}})

			tail := p.CaptureTail(1 << 20)
			tc.mut(p)
			p.StopCapture()

			before := nodeIDs(p, "k", "z")
			if err := p.SwapBase(base, tail); err != nil {
				t.Fatalf("SwapBase: %v", err)
			}
			got := nodeIDs(p, "k", "z")
			if !slices.Equal(got, tc.want) {
				t.Errorf("k=z answers %v after the swap, want %v", got, tc.want)
			}
			if !slices.Equal(got, before) {
				t.Errorf("k=z changed across the swap: %v became %v", before, got)
			}
		})
	}
}

// Edges go through their own methods and their own half of every structure, so
// they get their own pass rather than being assumed to follow the nodes.
func TestTail_CarriesEdgesToo(t *testing.T) {
	p := NewPropertyIndex()
	p.IndexEdge(1, "rel", []byte("x"))
	p.IndexEdge(2, "rel", []byte("y"))
	base := pinnedBase(t, []triple{
		{EdgeKind, 1, "rel", []byte("x")},
		{EdgeKind, 2, "rel", []byte("y")},
	})

	tail := p.CaptureTail(1 << 20)
	p.IndexEdge(3, "rel", []byte("z"))
	p.RemoveEdge(2)
	p.StopCapture()

	if err := p.SwapBase(base, tail); err != nil {
		t.Fatalf("SwapBase: %v", err)
	}
	if got := p.EdgesByProperty("rel", []byte("z")); !slices.Equal(got, []store.EdgeID{3}) {
		t.Errorf("rel=z answers %v, want [3]", got)
	}
	if got := p.EdgesByProperty("rel", []byte("y")); len(got) != 0 {
		t.Errorf("rel=y answers %v, want none: edge 2 was removed during the build", got)
	}
	if got := p.EdgesByProperty("rel", []byte("x")); !slices.Equal(got, []store.EdgeID{1}) {
		t.Errorf("rel=x answers %v, want [1]", got)
	}
}

// A registration that passed a unique constraint is an accepted mutation like any
// other, and has to replay -- through the unchecked path, because re-checking it
// against a base that now holds it is the constraint refusing its own value.
func TestTail_CarriesAUniqueRegistration(t *testing.T) {
	p := NewPropertyIndex()
	if _, err := p.DeclareUniqueNodeKey("k", func(store.NodeID) bool { return true }); err != nil {
		t.Fatalf("DeclareUniqueNodeKey: %v", err)
	}
	if err := p.IndexNodeUnique(1, "k", []byte("a")); err != nil {
		t.Fatalf("IndexNodeUnique: %v", err)
	}
	base := pinnedBase(t, []triple{{NodeKind, 1, "k", []byte("a")}})

	tail := p.CaptureTail(1 << 20)
	if err := p.IndexNodeUnique(2, "k", []byte("b")); err != nil {
		t.Fatalf("IndexNodeUnique during the build: %v", err)
	}
	p.StopCapture()

	if err := p.SwapBase(base, tail); err != nil {
		t.Fatalf("SwapBase: %v", err)
	}
	if got := nodeIDs(p, "k", "b"); !slices.Equal(got, []store.NodeID{2}) {
		t.Errorf("k=b answers %v, want [2]", got)
	}
	// And the key is still unique against the base it now reads from.
	if err := p.IndexNodeUnique(3, "k", []byte("a")); err == nil {
		t.Errorf("a unique key allowed a value the swapped-in base already holds")
	}
}

// A refused registration left nothing behind, so it must leave nothing in the log
// either -- a replay of it would install the entry the constraint refused.
func TestTail_DoesNotRecordARefusedRegistration(t *testing.T) {
	p := NewPropertyIndex()
	if _, err := p.DeclareUniqueNodeKey("k", func(store.NodeID) bool { return true }); err != nil {
		t.Fatalf("DeclareUniqueNodeKey: %v", err)
	}
	if err := p.IndexNodeUnique(1, "k", []byte("a")); err != nil {
		t.Fatalf("IndexNodeUnique: %v", err)
	}

	tail := p.CaptureTail(1 << 20)
	if err := p.IndexNodeUnique(2, "k", []byte("a")); err == nil {
		t.Fatalf("the fixture's unique key allowed a duplicate; there is nothing to test")
	}
	p.StopCapture()

	if n := tail.Len(); n != 0 {
		t.Errorf("a refused registration left %d operations in the log, want none", n)
	}
}

// Past its limit a capture gives up whole rather than keeping a prefix, because
// a prefix of a sequence of mutations is not a prefix of their effect.
func TestTail_DroppedPastItsLimit(t *testing.T) {
	p := NewPropertyIndex()

	room := p.CaptureTail(4 * TailOpBytes)
	for i := range 4 {
		p.IndexNode(store.NodeID(i+1), "k", []byte("v"))
	}
	p.StopCapture()
	if !room.Complete() {
		t.Fatalf("a capture with room for %d operations dropped %d of them",
			4, room.Len())
	}
	if got, want := room.Bytes(), int64(4*TailOpBytes); got != want {
		t.Errorf("four operations account for %d bytes, want %d", got, want)
	}

	tight := p.CaptureTail(2 * TailOpBytes)
	for i := range 4 {
		p.IndexNode(store.NodeID(i+10), "k", []byte("v"))
	}
	p.StopCapture()
	if tight.Complete() {
		t.Errorf("a capture with room for two operations took four")
	}
	if tight.Len() != 0 || tight.Bytes() != 0 {
		t.Errorf("a dropped capture still holds %d operations and %d bytes; "+
			"the memory it gave up on is the reason it gave up", tight.Len(), tight.Bytes())
	}
}

// Every path out of a compaction calls StopCapture, and more than one of them can
// be on the same compaction.
func TestTail_StopCaptureIsIdempotent(t *testing.T) {
	p := NewPropertyIndex()
	if got := p.StopCapture(); got != nil {
		t.Errorf("StopCapture with no capture open returned %v, want nil", got)
	}
	tail := p.CaptureTail(1 << 20)
	if got := p.StopCapture(); got != tail {
		t.Errorf("StopCapture returned %v, want the open capture", got)
	}
	if got := p.StopCapture(); got != nil {
		t.Errorf("a second StopCapture returned %v, want nil", got)
	}
	// And nothing is recorded once it is closed.
	p.IndexNode(1, "k", []byte("a"))
	if n := tail.Len(); n != 0 {
		t.Errorf("a stopped capture recorded %d operations", n)
	}
}

// A nil tail is what a caller that never opened one has, and the whole of what it
// must do is nothing.
func TestTail_NilIsCompleteAndEmpty(t *testing.T) {
	var tail *Tail
	if !tail.Complete() {
		t.Errorf("a nil tail reports itself incomplete")
	}
	if tail.Len() != 0 || tail.Bytes() != 0 {
		t.Errorf("a nil tail reports %d operations and %d bytes", tail.Len(), tail.Bytes())
	}
	tail.apply(NewPropertyIndex())
}
