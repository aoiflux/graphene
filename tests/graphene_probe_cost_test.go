package graphene_test

// Residual planning across a compaction.
//
// A compaction moves the property index out of the heap and into the image, and
// with it the reverse direction a residual probe reads. The probe stops being a
// map lookup and becomes a binary search of a disk-resident array — twenty-five
// reads at the sizes this program is aimed at, ten or so at the size this file
// builds — while the forward direction it is weighed against becomes a
// sequential walk of the same file.
//
// So the same query against the same data should take a different route on the
// two sides of a Compact, and must return the same ids either way. That pair is
// what these test: that the decision moves, and that nothing observable moves
// with it.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// probeCostStore is propertyStore's shape at a size a test can compact quickly:
// one key every node carries, one distinct per node, and one that three quarters
// of them carry.
func probeCostStore(t *testing.T, g *graphene.Graph, n int) {
	t.Helper()
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperty(id, "bucket", []byte("b")); err != nil {
			t.Fatalf("IndexNodeProperty bucket: %v", err)
		}
		if err := g.IndexNodeProperty(id, "seq", []byte(fmt.Sprintf("%07d", i))); err != nil {
			t.Fatalf("IndexNodeProperty seq: %v", err)
		}
		if i < n*3/4 {
			if err := g.IndexNodeProperty(id, "grp", []byte("g")); err != nil {
				t.Fatalf("IndexNodeProperty grp: %v", err)
			}
		}
	}
}

// wideCandidateQuery drives from the key three quarters of the nodes carry, so
// the candidate set is large and the residual set is only a little larger. That
// is the band a heap-resident reverse map is right to probe and a disk-resident
// one is not.
func wideCandidateQuery() store.NodeQuery {
	return store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "grp", Op: store.PropertyOpEqual, Value: []byte("g")},
			{Key: "seq", Op: store.PropertyOpContains, Value: []byte("0")},
		},
		FilterMode: store.MatchAll,
	}
}

// oneCandidateQuery drives from a single distinct value, so there is one
// candidate against a residual set of every node. A probe is right there however
// the reverse direction is stored, and it must stay right — the cost model is
// meant to reweigh the decision, not abolish one side of it.
func oneCandidateQuery() store.NodeQuery {
	return store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "seq", Op: store.PropertyOpEqual, Value: []byte("0000005")},
			{Key: "bucket", Op: store.PropertyOpContains, Value: []byte("b")},
		},
		FilterMode: store.MatchAll,
	}
}

// compactedProbeCostStore builds the fixture, compacts it and hands back a
// freshly opened handle on the result.
//
// Reopened rather than reused, because a store acquires its disk-resident index
// at open: compactCommit writes the section and the handle that wrote it goes on
// answering from the delta it already holds. So the route this file is about only
// changes for the next process to open the directory, and a test that compacted
// in place would measure the heap-resident planner twice and pass.
func compactedProbeCostStore(t *testing.T, n int) *graphene.Graph {
	t.Helper()
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	probeCostStore(t, g, n)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

func residualRoute(t *testing.T, g *graphene.Graph, q store.NodeQuery, when string) store.ResidualStep {
	t.Helper()
	plan, err := g.ExplainNodeQuery(q)
	if err != nil {
		t.Fatalf("ExplainNodeQuery %s compaction: %v", when, err)
	}
	if len(plan.Residuals) != 1 {
		t.Fatalf("%s compaction: %d residual steps, want exactly 1", when, len(plan.Residuals))
	}
	return plan.Residuals[0]
}

func residualQueryIDs(t *testing.T, g *graphene.Graph, q store.NodeQuery, when string) []store.NodeID {
	t.Helper()
	ids, err := g.QueryNodeIDs(q)
	if err != nil {
		t.Fatalf("QueryNodeIDs %s compaction: %v", when, err)
	}
	return ids
}

// TestResidualPlan_TheRouteFollowsWhereTheReverseIndexLives: the behavioural
// change, end to end. The same query over the same nodes probes while the index
// is in the heap and builds once it is in the image.
func TestResidualPlan_TheRouteFollowsWhereTheReverseIndexLives(t *testing.T) {
	q := wideCandidateQuery()

	heap := openDisk(t)
	probeCostStore(t, heap, 400)
	before := residualRoute(t, heap, q, "before")
	wantIDs := residualQueryIDs(t, heap, q, "before")
	if !before.Probe {
		t.Fatalf("the residual was already built before compaction (cost %d); "+
			"this fixture no longer sets up the case it is about", before.Cost)
	}

	g := compactedProbeCostStore(t, 400)
	after := residualRoute(t, g, q, "after")
	if after.Probe {
		t.Errorf("the residual is still probed against a mapped base (cost %d); "+
			"the probe is a binary search of the image there, not a map lookup", after.Cost)
	}

	// The estimate itself must not have moved. What changed is how the two sides
	// are weighed, not what either of them is: an estimate that shifted across a
	// compaction would mean the base and the delta disagree about the same key.
	if before.Cost != after.Cost {
		t.Errorf("the residual set is estimated at %d before the compaction and %d after",
			before.Cost, after.Cost)
	}

	gotIDs := residualQueryIDs(t, g, q, "after")
	assertSameIDs(t, wantIDs, gotIDs)
	if len(wantIDs) == 0 {
		t.Fatal("the query matches nothing, so agreeing about it proves nothing")
	}
}

// TestResidualPlan_ASmallCandidateSetIsStillProbed: the other half. Weighting the
// probe must leave it chosen where it is still the cheaper of the two, or the
// change is not a cost model but a switch.
func TestResidualPlan_ASmallCandidateSetIsStillProbed(t *testing.T) {
	g := compactedProbeCostStore(t, 400)

	q := oneCandidateQuery()
	step := residualRoute(t, g, q, "after")
	if !step.Probe {
		t.Errorf("one candidate against a %d-entry residual builds the whole set; "+
			"the weighting has swallowed the probe path entirely", step.Cost)
	}
	if got := residualQueryIDs(t, g, q, "after"); len(got) != 1 {
		t.Errorf("QueryNodeIDs = %v, want the single node carrying seq=0000005", got)
	}
}

// TestResidualPlan_BothRoutesAgreeOverAMixedDelta: a compacted store that has
// been written to since holds its entries on both sides, and the two routes read
// them differently — the probe asks the reverse direction of each, the build
// merges the forward ones. They must still agree.
func TestResidualPlan_BothRoutesAgreeOverAMixedDelta(t *testing.T) {
	g := compactedProbeCostStore(t, 400)

	// Twenty more nodes after the compaction, carrying the same keys, so every
	// filter below spans the base and the delta.
	extra := make([]*store.Node, 20)
	for i := range extra {
		extra[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(extra)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		for key, value := range map[string][]byte{
			"bucket": []byte("b"),
			"grp":    []byte("g"),
			"seq":    []byte(fmt.Sprintf("%07d", 1000+i)),
		} {
			if err := g.IndexNodeProperty(id, key, value); err != nil {
				t.Fatalf("IndexNodeProperty %s: %v", key, err)
			}
		}
	}

	// And one of the base's own nodes removed, so the retraction set is not empty
	// either: the probe consults it per candidate and the build merges it per
	// value, which is two more places the routes could part company.
	removed, err := g.QueryNodeIDs(store.NodeQuery{
		Filters:    []store.PropertyFilter{{Key: "seq", Op: store.PropertyOpEqual, Value: []byte("0000007")}},
		FilterMode: store.MatchAll,
	})
	if err != nil || len(removed) != 1 {
		t.Fatalf("locating the node to remove: %v (%d ids)", err, len(removed))
	}
	if err := g.DeleteNode(removed[0]); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	wide := residualQueryIDs(t, g, wideCandidateQuery(), "after")
	one := residualQueryIDs(t, g, oneCandidateQuery(), "after")

	// The same two questions asked the other way round: every id the wide query
	// returned must carry seq containing "0" and grp=g, and the removed node must
	// be in neither answer.
	for _, id := range wide {
		if id == removed[0] {
			t.Fatalf("the removed node %d survived the residual pass", id)
		}
	}
	for _, id := range one {
		if id == removed[0] {
			t.Fatalf("the removed node %d survived the probe", id)
		}
	}
	if len(wide) == 0 || len(one) != 1 {
		t.Fatalf("wide matched %d and the point query %d; the fixture stopped exercising both",
			len(wide), len(one))
	}
}

func assertSameIDs(t *testing.T, want, got []store.NodeID) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("the two routes returned %d and %d ids", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("the two routes disagree at position %d: %d against %d", i, want[i], got[i])
		}
	}
}
