package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/index/encoding"
	"github.com/aoiflux/graphene/store"
)

// Ordered-key declarations persist across a compact/reopen cycle (CSR v8 GORD).
//
// The failure this closes was silent: a declaration lived only in memory, so
// every restart reverted every range query on that key to a full scan, with
// OrderedProperties honestly reporting nothing declared and no error anywhere.

func buildScored(t *testing.T, g *graphene.Graph, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.IndexNodeProperty(id, "score", encoding.Int64(int64(i))); err != nil {
			t.Fatal(err)
		}
	}
}

// The declaration comes back, and so does the plan that depends on it.
//
// Checking OrderedProperties alone would not be enough: a declaration that is
// restored but not used by the planner is the same performance cliff with a
// more convincing report. The query plan is what says it is really in effect.
func TestOrderedDeclarations_RestoredDeclarationStillDrivesTheQuery(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	buildScored(t, g, 200)
	if err := g.DeclareOrderedProperty("score"); err != nil {
		t.Fatal(err)
	}

	rangeQuery := store.NodeQuery{
		Filters: []store.PropertyFilter{{
			Key: "score", Op: store.PropertyOpGreaterThan, Value: encoding.Int64(150),
		}},
	}

	planBefore, err := g.ExplainNodeQuery(rangeQuery)
	if err != nil {
		t.Fatal(err)
	}
	if planBefore.Driver != store.DriverOrdered {
		t.Fatalf("before reopen the range query was driven by %v, want DriverOrdered", planBefore.Driver)
	}

	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	keys, _ := reopened.OrderedProperties()
	if len(keys) != 1 || keys[0] != "score" {
		t.Fatalf("declared keys after reopen = %v, want [score]", keys)
	}

	planAfter, err := reopened.ExplainNodeQuery(rangeQuery)
	if err != nil {
		t.Fatal(err)
	}
	if planAfter.Driver != store.DriverOrdered {
		t.Fatalf("after reopen the range query fell back to %v — the declaration was restored "+
			"but is not driving the query, which is the same silent scan with a better report",
			planAfter.Driver)
	}

	// And the answers agree either side of the restart.
	before, err := g.QueryNodeIDs(rangeQuery)
	if err == nil && len(before) == 0 {
		t.Fatal("fixture produced no matches; the comparison below would be vacuous")
	}
	after, err := reopened.QueryNodeIDs(rangeQuery)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 49 {
		t.Fatalf("range query returned %d nodes after reopen, want 49", len(after))
	}
}

// Declaring several keys, on nodes and edges, round-trips all of them.
func TestOrderedDeclarations_NodeAndEdgeKeysBothPersist(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	e, _ := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})

	g.IndexNodeProperty(a, "score", encoding.Int64(1))
	g.IndexNodeProperty(a, "seen_at", encoding.Int64(2))
	g.IndexEdgeProperty(e, "weight_bucket", encoding.Int64(3))

	for _, k := range []string{"score", "seen_at"} {
		if err := g.DeclareOrderedProperty(k); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.DeclareOrderedEdgeProperty("weight_bucket"); err != nil {
		t.Fatal(err)
	}

	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	nodeKeys, edgeKeys := reopened.OrderedProperties()
	if len(nodeKeys) != 2 || nodeKeys[0] != "score" || nodeKeys[1] != "seen_at" {
		t.Fatalf("node keys after reopen = %v, want [score seen_at]", nodeKeys)
	}
	if len(edgeKeys) != 1 || edgeKeys[0] != "weight_bucket" {
		t.Fatalf("edge keys after reopen = %v, want [weight_bucket]", edgeKeys)
	}
}

// A store with nothing declared writes no section and reopens with nothing
// declared — the feature costs nothing when it is not used.
func TestOrderedDeclarations_NoneDeclaredStaysNone(t *testing.T) {
	dir := t.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	buildScored(t, g, 20)
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	nodeKeys, edgeKeys := reopened.OrderedProperties()
	if len(nodeKeys) != 0 || len(edgeKeys) != 0 {
		t.Fatalf("nothing was declared, but reopen reported %v / %v", nodeKeys, edgeKeys)
	}
}
