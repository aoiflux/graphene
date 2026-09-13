package graphene_test

// The streaming reads, from outside.
//
// The unit tests in disk/ hold each path against the slice form it replaces. What
// they cannot reach is the facade: the fallbacks in helpers.go that make
// ForEachNodeID and NodesByPropertyFunc callable against a backend that implements
// neither, and the promise that both backends answer the same. Those are what a
// caller actually depends on, and they are what breaks if an optional interface is
// declared and not satisfied.

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// streamGraphFixture writes n nodes over five buckets, deletes some, and
// optionally compacts so the disk backend answers from a base.
func streamGraphFixture(t *testing.T, g *graphene.Graph, n int, compact bool) {
	t.Helper()
	for i := 0; i < n; i++ {
		id, err := g.GraphStore.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeMicroArtefact},
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"bucket": []byte(fmt.Sprintf("b%d", i%5)),
			"seq":    []byte(fmt.Sprintf("%05d", i)),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
		if i%13 == 4 {
			if err := g.GraphStore.DeleteNode(id); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
		}
	}
	if compact {
		if err := g.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
	}
}

func streamedIDs(t *testing.T, g *graphene.Graph, key string, value []byte) []store.NodeID {
	t.Helper()
	var got []store.NodeID
	err := g.NodesByPropertyFunc(key, value, func(id store.NodeID) bool {
		got = append(got, id)
		return true
	})
	if err != nil {
		t.Fatalf("NodesByPropertyFunc(%s=%q): %v", key, value, err)
	}
	return got
}

func streamedQueryIDs(t *testing.T, g *graphene.Graph, q store.NodeQuery) []store.NodeID {
	t.Helper()
	var got []store.NodeID
	if err := g.ForEachNodeID(q, func(id store.NodeID) bool {
		got = append(got, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	return got
}

// TestStream_BothBackendsAnswerTheSliceForm is the facade differential, run over
// both backends and — on disk — both sides of a compaction.
func TestStream_BothBackendsAnswerTheSliceForm(t *testing.T) {
	cases := []struct {
		name    string
		open    func(t *testing.T) *graphene.Graph
		compact bool
	}{
		{"memory", func(t *testing.T) *graphene.Graph { return graphene.NewInMemory() }, false},
		{"disk", func(t *testing.T) *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			return g
		}, false},
		{"disk, compacted", func(t *testing.T) *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			return g
		}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.open(t)
			defer g.Close()
			streamGraphFixture(t, g, 260, tc.compact)

			for _, v := range [][]byte{[]byte("b0"), []byte("b4"), []byte("b9"), []byte("")} {
				want, err := g.NodesByProperty("bucket", v)
				if err != nil {
					t.Fatalf("NodesByProperty(%q): %v", v, err)
				}
				got := streamedIDs(t, g, "bucket", v)
				if len(want) == 0 && len(got) == 0 {
					continue
				}
				if !slices.Equal(got, want) {
					t.Fatalf("bucket=%q: stream %v, slice %v", v, got, want)
				}
			}

			queries := []store.NodeQuery{
				{},
				{Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")}}},
				{Offset: 3, Limit: 8, Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")}}},
				{Order: store.QueryOrderDesc, Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")}}},
				{Types: []store.NodeType{store.NodeTypeMicroArtefact}, Limit: 11},
			}
			for i, q := range queries {
				want, err := g.QueryNodeIDs(q)
				if err != nil {
					t.Fatalf("query %d: %v", i, err)
				}
				got := streamedQueryIDs(t, g, q)
				if len(want) == 0 && len(got) == 0 {
					continue
				}
				if !slices.Equal(got, want) {
					t.Fatalf("query %d: ForEachNodeID %v, QueryNodeIDs %v", i, got, want)
				}
			}
		})
	}
}

// TestStream_EdgeFormAnswersTheSliceForm covers the edge twin through the facade.
func TestStream_EdgeFormAnswersTheSliceForm(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	a, err := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	for i := 0; i < 80; i++ {
		eid, err := g.GraphStore.AddEdge(&store.Edge{
			Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains},
		})
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		if err := g.IndexEdgeProperties(eid, map[string][]byte{
			"algo": []byte(fmt.Sprintf("a%d", i%3)),
		}); err != nil {
			t.Fatalf("IndexEdgeProperties: %v", err)
		}
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for _, v := range [][]byte{[]byte("a0"), []byte("a2"), []byte("a7")} {
		want, err := g.EdgesByProperty("algo", v)
		if err != nil {
			t.Fatalf("EdgesByProperty(%q): %v", v, err)
		}
		var got []store.EdgeID
		if err := g.EdgesByPropertyFunc("algo", v, func(id store.EdgeID) bool {
			got = append(got, id)
			return true
		}); err != nil {
			t.Fatalf("EdgesByPropertyFunc(%q): %v", v, err)
		}
		if len(want) == 0 && len(got) == 0 {
			continue
		}
		if !slices.Equal(got, want) {
			t.Fatalf("algo=%q: stream %v, slice %v", v, got, want)
		}
	}
}

// TestStream_SinkReadsTheGraph is the promise a caller will actually lean on: for
// each id, load what it names. It is the reason the disk implementation batches
// rather than holding its read lock, and the reason that choice is in the
// interface's contract rather than in one backend's comments.
func TestStream_SinkReadsTheGraph(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()
	streamGraphFixture(t, g, 400, true)

	seen := 0
	err = g.NodesByPropertyFuncCtx(context.Background(), "bucket", []byte("b1"),
		func(id store.NodeID) bool {
			n, err := g.GetNode(id)
			if err != nil {
				t.Errorf("GetNode(%d) from inside the sink: %v", id, err)
				return false
			}
			if n.ID != id {
				t.Errorf("GetNode(%d) returned node %d", id, n.ID)
				return false
			}
			seen++
			return true
		})
	if err != nil {
		t.Fatalf("NodesByPropertyFuncCtx: %v", err)
	}
	if seen == 0 {
		t.Fatal("the sink saw nothing")
	}
}
