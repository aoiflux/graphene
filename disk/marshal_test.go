package disk

import (
	"bytes"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The append-form marshallers must produce byte-identical output to the
// allocating ones. A bulk path that encodes differently is a format bug that
// would only surface on replay.
func TestAppendMarshallersMatchAllocating(t *testing.T) {
	nodes := []*store.Node{
		{ID: 1, Labels: []store.NodeType{store.NodeTypeTag}},
		{ID: 2, Labels: []store.NodeType{store.NodeTypeCase, store.NodeTypeTag}, Properties: []byte("abc")},
		{ID: 3},
		{ID: 4, Labels: []store.NodeType{store.NodeTypeTag}, Properties: bytes.Repeat([]byte{7}, 300)},
	}
	for i, n := range nodes {
		want := marshalNode(n)
		got := appendMarshalledNode(nil, n)
		if !bytes.Equal(want, got) {
			t.Fatalf("node %d: append form differs\n want %v\n got  %v", i, want, got)
		}
		// Also correct when appending onto existing bytes.
		pre := []byte{0xAA, 0xBB}
		got2 := appendMarshalledNode(append([]byte{}, pre...), n)
		if !bytes.Equal(got2[2:], want) {
			t.Fatalf("node %d: append-onto-prefix differs", i)
		}
	}

	edges := []*store.Edge{
		{ID: 1, Src: 1, Dst: 2, Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.5},
		{ID: 2, Src: 2, Dst: 3, Labels: []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeSimilarTo}, Properties: []byte("xy")},
		{ID: 3, Src: 1, Dst: 1},
	}
	for i, e := range edges {
		want := marshalEdge(e)
		got := appendMarshalledEdge(nil, e)
		if !bytes.Equal(want, got) {
			t.Fatalf("edge %d: append form differs\n want %v\n got  %v", i, want, got)
		}
	}
}

// The size functions must equal what the marshallers actually write.
//
// They exist to size a WAL frame in one allocation, so a size that is merely
// close still works — appendMarshalled measures the payload after marshalling
// and never trusts the hint. What this pins is that they do not silently drift
// into being wrong in the direction that matters: a size larger than the
// encoding wastes bytes on every record written, a smaller one reintroduces
// exactly the reallocation the pair was added to remove. Neither is visible in
// any other test, because both still produce a correct log.
func TestMarshalledSizesAreExact(t *testing.T) {
	nodes := []*store.Node{
		{ID: 1, Labels: []store.NodeType{store.NodeTypeTag}},
		{ID: 2, Labels: []store.NodeType{store.NodeTypeCase, store.NodeTypeTag}, Properties: []byte("abc")},
		{ID: 3},
		{ID: 4, Labels: []store.NodeType{store.NodeTypeTag}, Properties: bytes.Repeat([]byte{7}, 300)},
	}
	for i, n := range nodes {
		if got, want := marshalledNodeSize(n), len(appendMarshalledNode(nil, n)); got != want {
			t.Errorf("node %d: marshalledNodeSize = %d, encoding is %d bytes", i, got, want)
		}
	}

	edges := []*store.Edge{
		{ID: 1, Src: 1, Dst: 2, Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.5},
		{ID: 2, Src: 2, Dst: 3, Labels: []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeSimilarTo}, Properties: []byte("xy")},
		{ID: 3, Src: 1, Dst: 1},
	}
	for i, e := range edges {
		if got, want := marshalledEdgeSize(e), len(appendMarshalledEdge(nil, e)); got != want {
			t.Errorf("edge %d: marshalledEdgeSize = %d, encoding is %d bytes", i, got, want)
		}
	}

	props := []struct {
		key string
		val []byte
	}{
		{"sha256", []byte("abc")},
		{"", nil},
		{"bucket", bytes.Repeat([]byte{9}, 500)},
	}
	for i, pr := range props {
		if got, want := marshalledPropSize(pr.key, pr.val), len(appendMarshalledProp(nil, 7, pr.key, pr.val)); got != want {
			t.Errorf("prop %d: marshalledPropSize = %d, encoding is %d bytes", i, got, want)
		}
	}
}

// appendMarshalledProp must agree with both allocating property marshallers,
// which encode the same shape for node and edge entries.
func TestAppendMarshalledPropMatchesAllocating(t *testing.T) {
	key, val := "sha256", []byte("deadbeef")
	if got, want := appendMarshalledProp(nil, 42, key, val), marshalNodeProp(42, key, val); !bytes.Equal(got, want) {
		t.Errorf("node prop: append form differs: want %v, got %v", want, got)
	}
	if got, want := appendMarshalledProp(nil, 42, key, val), marshalEdgeProp(42, key, val); !bytes.Equal(got, want) {
		t.Errorf("edge prop: append form differs: want %v, got %v", want, got)
	}
}
