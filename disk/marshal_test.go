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
