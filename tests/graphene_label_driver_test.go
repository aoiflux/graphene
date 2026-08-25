package graphene_test

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Label postings as a query driver.
//
// The planner picks the cheapest source that is still a guaranteed superset of
// the answer. Labels bound a result just as an equality filter does, and their
// posting sizes are known in O(1), so the two have to be costed on the same
// scale.
//
// The disk backend used not to do that: it took any equality driver
// unconditionally, so a query naming a 10-node label alongside a 1000-hit
// property filter drove from the filter and then discarded 99% of it. The
// in-memory backend already compared the two, which made this a parity
// divergence as well as a performance one — the same query could be planned
// differently depending on the backend.
//
// These assert on the *plan*, not the results. Results alone cannot tell the
// difference between a good plan and a bad one that happens to return the same
// rows, which is the whole point of ExplainNodeQuery.

// labelDriverFixture builds a graph where labels and property cardinalities are
// deliberately mismatched:
//
//	NodeTypeCase          — 10 nodes    (very selective label)
//	NodeTypeMicroArtefact — 990 nodes   (weak label)
//	"bucket"="hot"        — 500 nodes   (weak filter)
//	"sha256"=<unique>     — 1 node      (very selective filter)
func labelDriverFixture(t *testing.T, g *graphene.Graph) []store.NodeID {
	t.Helper()
	const n = 1000
	nodes := make([]*store.Node, n)
	for i := range nodes {
		label := store.NodeTypeMicroArtefact
		if i%100 == 0 {
			label = store.NodeTypeCase
		}
		nodes[i] = &store.Node{Labels: []store.NodeType{label}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		// Keyed on i/100 rather than i%2 so that "bucket" varies *within* the
		// Case label. The labels sit at i%100==0, which are all even, so an
		// i%2 split would have made every Case node "hot" and left the filter
		// unable to discriminate inside the driven set.
		bucket := "cold"
		if (i/100)%2 == 0 {
			bucket = "hot"
		}
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%06d", i)),
			"bucket": []byte(bucket),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}
	return ids
}

func labelDriverBackends(t *testing.T) map[string]*graphene.Graph {
	t.Helper()
	disk, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	return map[string]*graphene.Graph{
		"memory": graphene.NewInMemory(),
		"disk":   disk,
	}
}

// TestSelectiveLabelBeatsWeakFilter is the case the disk planner used to get
// wrong: 10 nodes by label against 500 by filter.
func TestSelectiveLabelBeatsWeakFilter(t *testing.T) {
	for name, g := range labelDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			labelDriverFixture(t, g)
			if d, ok := g.GraphStore.(interface{ Compact() error }); ok {
				_ = d.Compact() // exercise the CSR label postings, not just the delta
			}

			q := store.NodeQuery{
				Types:   []store.NodeType{store.NodeTypeCase},
				Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")}},
			}

			plan, err := g.ExplainNodeQuery(q)
			if err != nil {
				t.Fatalf("ExplainNodeQuery: %v", err)
			}
			if plan.Driver != store.DriverLabels {
				t.Errorf("drove from %v, want labels: 10 nodes carry the label against 500 matching the filter\nplan: %s",
					plan.Driver, plan)
			}
			if plan.Candidates > 50 {
				t.Errorf("candidates=%d, want ~10; the planner is not using the label postings\nplan: %s",
					plan.Candidates, plan)
			}

			// The plan must not have changed the answer.
			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 5 { // 10 Case nodes, 5 of them in a "hot" block
				t.Errorf("got %d results, want 5", len(got))
			}
		})
	}
}

// TestSelectiveFilterBeatsWeakLabel is the converse, and guards against
// "optimising" by always preferring labels.
func TestSelectiveFilterBeatsWeakLabel(t *testing.T) {
	for name, g := range labelDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			labelDriverFixture(t, g)
			if d, ok := g.GraphStore.(interface{ Compact() error }); ok {
				_ = d.Compact()
			}

			q := store.NodeQuery{
				Types:   []store.NodeType{store.NodeTypeMicroArtefact}, // 990 nodes
				Filters: []store.PropertyFilter{{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000003")}},
			}

			plan, err := g.ExplainNodeQuery(q)
			if err != nil {
				t.Fatalf("ExplainNodeQuery: %v", err)
			}
			if plan.Driver != store.DriverEquality {
				t.Errorf("drove from %v, want equality: 1 node matches the filter against 990 carrying the label\nplan: %s",
					plan.Driver, plan)
			}
			if plan.Candidates > 5 {
				t.Errorf("candidates=%d, want 1\nplan: %s", plan.Candidates, plan)
			}
		})
	}
}

// TestLabelDriverParity asserts both backends reach the same decision. memory is
// the oracle disk is judged against, so a divergence here is a correctness
// signal, not a tuning difference.
func TestLabelDriverParity(t *testing.T) {
	cases := []struct {
		name  string
		query store.NodeQuery
	}{
		{"selective label, weak filter", store.NodeQuery{
			Types:   []store.NodeType{store.NodeTypeCase},
			Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")}},
		}},
		{"weak label, selective filter", store.NodeQuery{
			Types:   []store.NodeType{store.NodeTypeMicroArtefact},
			Filters: []store.PropertyFilter{{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000007")}},
		}},
		{"label only", store.NodeQuery{
			Types: []store.NodeType{store.NodeTypeCase},
		}},
		{"filter only", store.NodeQuery{
			Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")}},
		}},
	}

	backends := labelDriverBackends(t)
	for _, g := range backends {
		labelDriverFixture(t, g)
	}
	if d, ok := backends["disk"].GraphStore.(interface{ Compact() error }); ok {
		_ = d.Compact()
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			memPlan, err := backends["memory"].ExplainNodeQuery(tc.query)
			if err != nil {
				t.Fatalf("memory explain: %v", err)
			}
			diskPlan, err := backends["disk"].ExplainNodeQuery(tc.query)
			if err != nil {
				t.Fatalf("disk explain: %v", err)
			}
			if memPlan.Driver != diskPlan.Driver {
				t.Errorf("backends disagree on driver: memory=%v disk=%v\n  memory: %s\n  disk:   %s",
					memPlan.Driver, diskPlan.Driver, memPlan, diskPlan)
			}
			if memPlan.Results != diskPlan.Results {
				t.Errorf("backends disagree on results: memory=%d disk=%d",
					memPlan.Results, diskPlan.Results)
			}
		})
	}
}
