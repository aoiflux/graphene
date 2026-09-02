package graphene_test

// Aggregation: counts the engine can answer from what it already holds.
//
// The alternative to every one of these is pulling IDs into Go and counting them
// there, which is what a caller does today. That works at a thousand entities
// and stops working well before a hundred thousand — and the failure is not an
// error, it is a report that takes an hour.
//
// The properties worth pinning are the ones a caller would otherwise have to
// discover: that a multi-label entity is counted under every label it carries,
// that deleted entities are not counted, and that a neighbour reached twice from
// one anchor is one vote rather than two.

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func TestCountByType_CountsEveryLabelAndOnlyLiveOnes(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		// One single-label node, one dual-label node, and one that gets deleted.
		single, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if _, err := g.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeCase, store.NodeTypeTag},
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		doomed, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := g.DeleteNode(doomed); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		counts, err := g.CountNodesByType()
		if err != nil {
			t.Fatalf("CountNodesByType: %v", err)
		}
		want := map[store.NodeType]uint64{
			store.NodeTypeCase: 2, // the single-label node and the dual-label one
			store.NodeTypeTag:  1, // the dual-label one; the deleted one is gone
		}
		if !maps.Equal(counts, want) {
			t.Fatalf("CountNodesByType = %v, want %v", counts, want)
		}

		// The multi-label caveat, stated as an assertion so nobody has to
		// rediscover it: these do not sum to NodeCount.
		total, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		var summed uint64
		for _, n := range counts {
			summed += n
		}
		if summed <= total {
			t.Fatalf("a dual-label node should make the per-type counts sum (%d) exceed "+
				"NodeCount (%d)", summed, total)
		}

		// Edges, same rules.
		if _, err := g.AddEdge(&store.Edge{
			Src: single, Dst: single,
			Labels: []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeReuse},
		}); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		edgeCounts, err := g.CountEdgesByType()
		if err != nil {
			t.Fatalf("CountEdgesByType: %v", err)
		}
		wantEdges := map[store.EdgeType]uint64{
			store.EdgeTypeContains: 1,
			store.EdgeTypeReuse:    1,
		}
		if !maps.Equal(edgeCounts, wantEdges) {
			t.Fatalf("CountEdgesByType = %v, want %v", edgeCounts, wantEdges)
		}
	})
}

// TestCountByType_EmptyGraphAndAbsentTypes pins that a type with nothing under
// it is absent rather than zero. A caller ranging over the map is enumerating
// what the graph contains, and a zero entry would be a type it does not.
func TestCountByType_EmptyGraphAndAbsentTypes(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		counts, err := g.CountNodesByType()
		if err != nil {
			t.Fatalf("CountNodesByType on an empty graph: %v", err)
		}
		if len(counts) != 0 {
			t.Fatalf("empty graph reported %v", counts)
		}

		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := g.DeleteNode(id); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
		counts, err = g.CountNodesByType()
		if err != nil {
			t.Fatalf("CountNodesByType: %v", err)
		}
		if n, present := counts[store.NodeTypeCase]; present {
			t.Fatalf("a type whose only node was deleted is reported as %d, want absent", n)
		}
	})
}

// TestCountByType_SurvivesCompaction reaches the disk backend's other layer.
// Before a compaction every node is in the delta; after it they are in the CSR
// image, and the count has to merge both.
func TestCountByType_SurvivesCompaction(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	for range 5 {
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Two more after compaction, so the answer needs both layers, and one of
	// the compacted nodes deleted, so the image alone is wrong.
	for range 2 {
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	if err := g.DeleteNode(1); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	counts, err := g.CountNodesByType()
	if err != nil {
		t.Fatalf("CountNodesByType: %v", err)
	}
	want := map[store.NodeType]uint64{store.NodeTypeCase: 4, store.NodeTypeTag: 2}
	if !maps.Equal(counts, want) {
		t.Fatalf("across CSR and delta: CountNodesByType = %v, want %v", counts, want)
	}
}

func TestCountByProperty_DistributesLiveValues(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		// Three nodes on "ready", one on "failed", one on "failed" then deleted,
		// and one carrying no entry under the key at all.
		add := func(state string) store.NodeID {
			t.Helper()
			id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if state != "" {
				if err := g.IndexNodeProperty(id, "state", []byte(state)); err != nil {
					t.Fatalf("IndexNodeProperty: %v", err)
				}
			}
			return id
		}
		for range 3 {
			add("ready")
		}
		add("failed")
		doomed := add("failed")
		add("") // no entry under the key
		if err := g.DeleteNode(doomed); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		counts, err := g.CountNodesByProperty("state")
		if err != nil {
			t.Fatalf("CountNodesByProperty: %v", err)
		}
		want := map[string]uint64{"ready": 3, "failed": 1}
		if !maps.Equal(counts, want) {
			t.Fatalf("CountNodesByProperty = %v, want %v — the deleted node must not "+
				"be counted and the unindexed one has no value to count", counts, want)
		}

		// A key nothing was ever registered under is an empty distribution, not
		// an error: "no node has one of those" is a legitimate answer.
		counts, err = g.CountNodesByProperty("never-used")
		if err != nil {
			t.Fatalf("CountNodesByProperty on an unused key: %v", err)
		}
		if len(counts) != 0 {
			t.Fatalf("unused key reported %v", counts)
		}
	})
}

// TestNeighbourFrequency_CountsAnchorsNotEdges is the decision that makes the
// answer mean something. Two edges from one anchor to one neighbour are one
// vote, or the ranking measures how the graph is modelled rather than what it
// says.
func TestNeighbourFrequency_CountsAnchorsNotEdges(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		tracker := labelled(t, g, store.NodeTypeTag)
		rare := labelled(t, g, store.NodeTypeTag)
		noise := labelled(t, g, store.NodeTypeEvidenceFile)

		var anchors []store.NodeID
		for i := range 3 {
			v := labelled(t, g, store.NodeTypeCase)
			anchors = append(anchors, v)
			// Two edges of different types to the same tracker: still one vote.
			labelledEdge(t, g, v, tracker, store.EdgeTypeTaggedWith)
			labelledEdge(t, g, v, tracker, store.EdgeTypeReuse)
			// A neighbour of the wrong node type, to prove the filter runs.
			labelledEdge(t, g, v, noise, store.EdgeTypeTaggedWith)
			if i == 0 {
				labelledEdge(t, g, v, rare, store.EdgeTypeTaggedWith)
			}
		}

		freq, err := g.NeighbourFrequency(anchors, store.DirectionOutbound, nil,
			[]store.NodeType{store.NodeTypeTag})
		if err != nil {
			t.Fatalf("NeighbourFrequency: %v", err)
		}
		want := map[store.NodeID]uint64{tracker: 3, rare: 1}
		if !maps.Equal(freq, want) {
			t.Fatalf("NeighbourFrequency = %v, want %v", freq, want)
		}

		// A repeated anchor is one anchor.
		repeated := append(append([]store.NodeID{}, anchors...), anchors...)
		again, err := g.NeighbourFrequency(repeated, store.DirectionOutbound, nil,
			[]store.NodeType{store.NodeTypeTag})
		if err != nil {
			t.Fatalf("NeighbourFrequency: %v", err)
		}
		if !maps.Equal(again, want) {
			t.Fatalf("duplicated anchors changed the answer: %v, want %v", again, want)
		}

		// Without the node-type filter the wrong-kind neighbour appears, which
		// is what says the filter was doing something.
		unfiltered, err := g.NeighbourFrequency(anchors, store.DirectionOutbound, nil, nil)
		if err != nil {
			t.Fatalf("NeighbourFrequency: %v", err)
		}
		if unfiltered[noise] != 3 {
			t.Fatalf("unfiltered frequency for the excluded node = %d, want 3",
				unfiltered[noise])
		}

		// And the edge-type filter narrows it independently.
		byType, err := g.NeighbourFrequency(anchors, store.DirectionOutbound,
			[]store.EdgeType{store.EdgeTypeReuse}, []store.NodeType{store.NodeTypeTag})
		if err != nil {
			t.Fatalf("NeighbourFrequency: %v", err)
		}
		if !maps.Equal(byType, map[store.NodeID]uint64{tracker: 3}) {
			t.Fatalf("filtered by edge type = %v, want just the tracker", byType)
		}
	})
}

// TestNeighbourFrequency_IsBounded covers the reason this lives in traversal at
// all. The anchor set is usually the result of an earlier query, so its size is
// data rather than something the caller chose.
func TestNeighbourFrequency_IsBounded(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		centre := hub(t, g, 5000)

		_, err := g.NeighbourFrequencyCtx(context.Background(), []store.NodeID{centre},
			store.DirectionBoth, nil, nil, store.Budget{MaxEdges: 100})
		if !errors.Is(err, store.ErrBudgetExceeded) {
			t.Fatalf("a hub anchor with MaxEdges 100: want ErrBudgetExceeded, got %v", err)
		}

		// A deadline already passed stops it before it counts anything, the
		// same way it stops a walk.
		_, err = g.NeighbourFrequencyCtx(context.Background(), []store.NodeID{centre},
			store.DirectionBoth, nil, nil, store.Budget{MaxTime: -time.Second})
		if !errors.Is(err, store.ErrBudgetExceeded) {
			t.Fatalf("an elapsed MaxTime: want ErrBudgetExceeded, got %v", err)
		}

		dead, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = g.NeighbourFrequencyCtx(dead, []store.NodeID{centre},
			store.DirectionBoth, nil, nil, store.Budget{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a cancelled context: want context.Canceled, got %v", err)
		}
	})
}

// TestNeighbourFrequency_SkipsDeadAnchors covers the race a caller cannot
// prevent: the anchor set came from an earlier query, and something was deleted
// in between.
func TestNeighbourFrequency_SkipsDeadAnchors(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		tracker := labelled(t, g, store.NodeTypeTag)
		live := labelled(t, g, store.NodeTypeCase)
		labelledEdge(t, g, live, tracker, store.EdgeTypeTaggedWith)
		gone := labelled(t, g, store.NodeTypeCase)
		labelledEdge(t, g, gone, tracker, store.EdgeTypeTaggedWith)
		if err := g.DeleteNode(gone); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		freq, err := g.NeighbourFrequency([]store.NodeID{live, gone},
			store.DirectionOutbound, nil, nil)
		if err != nil {
			t.Fatalf("a deleted anchor must be skipped, not reported: %v", err)
		}
		if !maps.Equal(freq, map[store.NodeID]uint64{tracker: 1}) {
			t.Fatalf("NeighbourFrequency = %v, want the tracker counted once", freq)
		}
	})
}

// TestAggregation_UnsupportedBackendErrors: an empty map would say the graph is
// empty, which is a wrong answer rather than a missing one.
func TestAggregation_UnsupportedBackendErrors(t *testing.T) {
	g := &graphene.Graph{GraphStore: noAggregatorStore{graphene.NewInMemory().GraphStore}}
	if _, err := g.CountNodesByType(); err == nil {
		t.Error("CountNodesByType on an unsupporting backend returned no error")
	}
	if _, err := g.CountEdgesByType(); err == nil {
		t.Error("CountEdgesByType on an unsupporting backend returned no error")
	}
	if _, err := g.CountNodesByProperty("k"); err == nil {
		t.Error("CountNodesByProperty on an unsupporting backend returned no error")
	}
	if _, err := g.CountEdgesByProperty("k"); err == nil {
		t.Error("CountEdgesByProperty on an unsupporting backend returned no error")
	}
	// Stats still works, and says it has no breakdown rather than an empty one.
	st, err := g.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.HasTypeCounts {
		t.Error("Stats claimed type counts from a backend that cannot produce them")
	}
}

// TestStats_CarriesTypeCounts wires the breakdown into the one call an operator
// actually makes.
func TestStats_CarriesTypeCounts(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		a := labelled(t, g, store.NodeTypeCase)
		b := labelled(t, g, store.NodeTypeTag)
		labelledEdge(t, g, a, b, store.EdgeTypeTaggedWith)

		st, err := g.Stats()
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if !st.HasTypeCounts {
			t.Fatal("both bundled backends can count by type; Stats says otherwise")
		}
		want := map[store.NodeType]uint64{store.NodeTypeCase: 1, store.NodeTypeTag: 1}
		if !maps.Equal(st.NodesByType, want) {
			t.Errorf("Stats.NodesByType = %v, want %v", st.NodesByType, want)
		}
		if !maps.Equal(st.EdgesByType, map[store.EdgeType]uint64{store.EdgeTypeTaggedWith: 1}) {
			t.Errorf("Stats.EdgesByType = %v, want one TaggedWith", st.EdgesByType)
		}
	})
}

type noAggregatorStore struct{ store.GraphStore }

// labelled and labelledEdge take explicit labels, unlike mustNode in
// graphene_unique_test.go — every test here is about which labels an entity
// carries, so they cannot come from a fixture default.
func labelled(t *testing.T, g *graphene.Graph, labels ...store.NodeType) store.NodeID {
	t.Helper()
	id, err := g.AddNode(&store.Node{Labels: labels})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return id
}

func labelledEdge(t *testing.T, g *graphene.Graph, src, dst store.NodeID, labels ...store.EdgeType) store.EdgeID {
	t.Helper()
	id, err := g.AddEdge(&store.Edge{Src: src, Dst: dst, Labels: labels})
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	return id
}
