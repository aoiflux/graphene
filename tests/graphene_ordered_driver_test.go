package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/index/encoding"
	"github.com/aoiflux/graphene/store"
)

// Range and prefix filters as query drivers.
//
// The planner picks the cheapest source that is still a guaranteed superset of
// the answer. Equality filters and labels both report their size, so they can be
// compared. A range or prefix reported nothing at all, and both planners worked
// around that by *position* rather than by cost:
//
//   - The node planner tried ordered filters after equality and before labels,
//     so any range beat any label — a range over every value in the key won
//     against a 10-node label.
//   - The edge planner tried them below its three-way switch, reachable only
//     when equality, adjacency and labels were all absent — so a range matching
//     three edges lost to a label carrying two thousand.
//   - Within one query, the *first* ordered filter won, whatever the others
//     matched.
//
// None of those is a decision; they are the query's filter order deciding. The
// ordered index can size a range in two binary searches (index/stats.go), so all
// of it is now costed on one scale.
//
// These assert on the *plan*, not the results. Results cannot distinguish a good
// plan from a bad one that returns the same rows, which is what ExplainNodeQuery
// exists for. Every case runs against both backends: the two planners have to
// reach the same decision from the same numbers, so a divergence is a parity bug
// rather than a backend difference.

const orderedDriverNodes = 2000

// orderedDriverFixture builds a graph where ranges, labels and equality filters
// are deliberately mismatched:
//
//	NodeTypeCase          — 10 nodes            (very selective label)
//	NodeTypeMicroArtefact — 1990 nodes          (weak label)
//	"ts"                  — unique per node,    ordered; any window is exact
//	"bucket"="hot"        — 1000 nodes          (weak equality filter)
func orderedDriverFixture(t *testing.T, g *graphene.Graph) []store.NodeID {
	t.Helper()
	if err := g.DeclareOrderedProperty("ts"); err != nil {
		t.Fatalf("DeclareOrderedProperty: %v", err)
	}
	nodes := make([]*store.Node, orderedDriverNodes)
	for i := range nodes {
		label := store.NodeTypeMicroArtefact
		if i%200 == 0 {
			label = store.NodeTypeCase
		}
		nodes[i] = &store.Node{Labels: []store.NodeType{label}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		bucket := "cold"
		if i%2 == 0 {
			bucket = "hot"
		}
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"ts":     encoding.Uint64(uint64(i)),
			"bucket": []byte(bucket),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}
	return ids
}

func orderedDriverBackends(t *testing.T) map[string]*graphene.Graph {
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

// tsBelow is a range filter matching the first n nodes by "ts".
func tsBelow(n uint64) store.PropertyFilter {
	return store.PropertyFilter{Key: "ts", Op: store.PropertyOpLessThan, Value: encoding.Uint64(n)}
}

// checkPlan asserts the driver and, when want >= 0, an upper bound on the
// candidate count. The bound is what catches a planner that names the right
// driver but resolves a different set.
func checkPlan(t *testing.T, g *graphene.Graph, q store.NodeQuery, want store.DriverKind, maxCandidates int, why string) store.QueryPlan {
	t.Helper()
	plan, err := g.ExplainNodeQuery(q)
	if err != nil {
		t.Fatalf("ExplainNodeQuery: %v", err)
	}
	if plan.Driver != want {
		t.Errorf("drove from %v, want %v: %s\nplan: %s", plan.Driver, want, why, plan)
	}
	if maxCandidates >= 0 && plan.Candidates > maxCandidates {
		t.Errorf("candidates=%d, want at most %d: %s\nplan: %s", plan.Candidates, maxCandidates, why, plan)
	}
	return plan
}

// TestSelectiveRangeBeatsWeakEquality is the case the node planner could not
// reach: an equality filter used to be taken unconditionally, so a range
// matching 5 nodes lost to a filter matching a thousand.
func TestSelectiveRangeBeatsWeakEquality(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			q := store.NodeQuery{Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")}, // 1000
				tsBelow(5), // 5
			}}
			checkPlan(t, g, q, store.DriverOrdered, 50,
				"5 nodes fall in the range against 1000 matching the equality filter")

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 3 { // ts in {0..4}, of which 0, 2, 4 are "hot"
				t.Errorf("got %d results, want 3", len(got))
			}
		})
	}
}

// TestSelectiveEqualityBeatsWideRange is the converse, and guards against
// "optimising" by always preferring the range.
func TestSelectiveEqualityBeatsWideRange(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			ids := orderedDriverFixture(t, g)

			q := store.NodeQuery{Filters: []store.PropertyFilter{
				tsBelow(orderedDriverNodes), // every node
				{Key: "ts", Op: store.PropertyOpEqual, Value: encoding.Uint64(7)},
			}}
			// The equality filter is on the same key, so this also pins that an
			// ordered key still serves equality from the hash postings.
			checkPlan(t, g, q, store.DriverEquality, 10,
				"one node matches the equality filter against every node in the range")

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 1 || got[0] != ids[7] {
				t.Errorf("got %v, want [%d]", got, ids[7])
			}
		})
	}
}

// TestEqualityWinsATieAgainstARange pins the tie-break, which is not arbitrary:
// both drivers yield ascending candidates, but the ordered path sorts and
// dedupes what it collects while the hash postings are already in ID order. At
// equal cardinality the cheaper one is therefore equality, and a planner that
// preferred the range would be doing a sort for nothing on every such query.
//
// "ts" is unique per node, so a range over the first half matches exactly as
// many nodes as the "hot" bucket does: 1000 against 1000.
func TestEqualityWinsATieAgainstARange(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			q := store.NodeQuery{Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")}, // 1000
				tsBelow(orderedDriverNodes / 2),                                  // 1000
			}}
			plan := checkPlan(t, g, q, store.DriverEquality, 1000,
				"both match 1000, and the ordered path would sort what the postings already order")
			if plan.DriverKey != "bucket" {
				t.Errorf("drove from key %q, want \"bucket\"\nplan: %s", plan.DriverKey, plan)
			}

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 500 { // ts < 1000 and even
				t.Errorf("got %d results, want 500", len(got))
			}
		})
	}
}

// TestSelectiveLabelBeatsWideRange is the node planner's old inversion: with no
// equality filter present, the first ordered filter was taken before labels were
// even costed, so a range over the whole key beat a 10-node label.
func TestSelectiveLabelBeatsWideRange(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			q := store.NodeQuery{
				Types:   []store.NodeType{store.NodeTypeCase}, // 10
				Filters: []store.PropertyFilter{tsBelow(orderedDriverNodes)},
			}
			checkPlan(t, g, q, store.DriverLabels, 50,
				"10 nodes carry the label against every node in the range")

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 10 {
				t.Errorf("got %d results, want 10", len(got))
			}
		})
	}
}

// TestSelectiveRangeBeatsWeakLabel is that inversion's converse: the range
// competing with labels has to work in both directions, or the fix is just the
// old bug pointing the other way.
func TestSelectiveRangeBeatsWeakLabel(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			q := store.NodeQuery{
				Types:   []store.NodeType{store.NodeTypeMicroArtefact}, // 1990
				Filters: []store.PropertyFilter{tsBelow(5)},
			}
			checkPlan(t, g, q, store.DriverOrdered, 50,
				"5 nodes fall in the range against 1990 carrying the label")

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 4 { // ts in {0..4}; ts=0 is the Case node
				t.Errorf("got %d results, want 4", len(got))
			}
		})
	}
}

// TestMostSelectiveRangeDrives pins that ranges are compared with each other and
// not just with the other drivers. The wide one is written first deliberately:
// the planner used to take whichever came first in the query.
func TestMostSelectiveRangeDrives(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			q := store.NodeQuery{Filters: []store.PropertyFilter{
				tsBelow(1500), // wide, and first
				tsBelow(4),    // narrow, and second
			}}
			plan := checkPlan(t, g, q, store.DriverOrdered, 50,
				"the narrower of the two ranges has to drive, whichever is written first")
			if !plan.DriverFilters.Has(1) || plan.DriverFilters.Count() != 1 {
				t.Errorf("drove from filters %#b, want just filter 1 (the narrow range)\nplan: %s", uint64(plan.DriverFilters), plan)
			}

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 4 {
				t.Errorf("got %d results, want 4", len(got))
			}
		})
	}
}

// TestEmptyRangeDrives is the limiting case, and the one where the estimate has
// to be exact rather than close: a range matching nothing must win against every
// alternative, because it ends the query.
func TestEmptyRangeDrives(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			q := store.NodeQuery{
				Types: []store.NodeType{store.NodeTypeCase},
				Filters: []store.PropertyFilter{
					{Key: "ts", Op: store.PropertyOpGreaterThan, Value: encoding.Uint64(orderedDriverNodes + 1)},
				},
			}
			plan := checkPlan(t, g, q, store.DriverOrdered, 0,
				"the range matches nothing, so it is the cheapest possible driver")
			if plan.Results != 0 {
				t.Errorf("results=%d, want 0\nplan: %s", plan.Results, plan)
			}
		})
	}
}

// TestUndeclaredKeyStillFallsBack pins the boundary: a range on a key that was
// never declared ordered has no index to size it, so it cannot drive and the
// query falls back to the driver it had before. Without this, "the range always
// wins" would pass every test above.
func TestUndeclaredKeyStillFallsBack(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			q := store.NodeQuery{
				Types: []store.NodeType{store.NodeTypeCase}, // 10
				Filters: []store.PropertyFilter{
					// "bucket" is indexed but not declared ordered.
					{Key: "bucket", Op: store.PropertyOpGreaterThan, Value: []byte("a")},
				},
			}
			checkPlan(t, g, q, store.DriverLabels, 50,
				"an undeclared key cannot be sized or served, so labels drive")
		})
	}
}

// --- edges ---

// orderedEdgeFixture builds a star of 1000 edges, all one type, with an ordered
// "seq" property.
func orderedEdgeFixture(t *testing.T, g *graphene.Graph) {
	t.Helper()
	if err := g.DeclareOrderedEdgeProperty("seq"); err != nil {
		t.Fatalf("DeclareOrderedEdgeProperty: %v", err)
	}
	const n = 1000
	nodes := make([]*store.Node, n+1)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	edges := make([]*store.Edge, n)
	for i := range edges {
		edges[i] = &store.Edge{
			Src:    ids[0],
			Dst:    ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		}
	}
	eids, err := g.AddEdges(edges)
	if err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	for i, eid := range eids {
		if err := g.IndexEdgeProperties(eid, map[string][]byte{"seq": encoding.Uint64(uint64(i))}); err != nil {
			t.Fatalf("IndexEdgeProperties: %v", err)
		}
	}
}

// TestEdgeSelectiveRangeBeatsLabel is the edge planner's version, and the one
// that was unreachable rather than merely mis-ordered: the ordered driver sat
// below a switch that any label already satisfied.
func TestEdgeSelectiveRangeBeatsLabel(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedEdgeFixture(t, g)

			q := store.EdgeQuery{
				Types: []store.EdgeType{store.EdgeTypeContains}, // 1000
				Filters: []store.PropertyFilter{
					{Key: "seq", Op: store.PropertyOpLessThan, Value: encoding.Uint64(3)},
				},
			}
			plan, err := g.ExplainEdgeQuery(q)
			if err != nil {
				t.Fatalf("ExplainEdgeQuery: %v", err)
			}
			if plan.Driver != store.DriverOrdered {
				t.Errorf("drove from %v, want ordered: 3 edges fall in the range against 1000 carrying the label\nplan: %s",
					plan.Driver, plan)
			}
			if plan.Candidates > 20 {
				t.Errorf("candidates=%d, want ~3\nplan: %s", plan.Candidates, plan)
			}

			got, err := g.QueryEdgeIDs(q)
			if err != nil {
				t.Fatalf("QueryEdgeIDs: %v", err)
			}
			if len(got) != 3 {
				t.Errorf("got %d results, want 3", len(got))
			}
		})
	}
}

// TestEdgeAnchorBeatsWideRange guards the other direction on the edge side: the
// anchor is exact, and a range covering the whole key must not displace it.
func TestEdgeAnchorBeatsWideRange(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedEdgeFixture(t, g)

			// A leaf node with exactly one inbound edge.
			all, err := g.QueryNodeIDs(store.NodeQuery{Types: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			var leaf store.NodeID
			for _, id := range all {
				in, err := g.EdgesOf(id, store.DirectionInbound, nil)
				if err != nil {
					t.Fatalf("EdgesOf: %v", err)
				}
				if len(in) == 1 {
					leaf = id
					break
				}
			}
			if leaf == 0 {
				t.Fatal("fixture has no leaf node")
			}

			q := store.EdgeQuery{
				DstIDs: []store.NodeID{leaf},
				Filters: []store.PropertyFilter{
					{Key: "seq", Op: store.PropertyOpGreaterThanOrEqual, Value: encoding.Uint64(0)},
				},
			}
			plan, err := g.ExplainEdgeQuery(q)
			if err != nil {
				t.Fatalf("ExplainEdgeQuery: %v", err)
			}
			if plan.Driver != store.DriverAdjacency {
				t.Errorf("drove from %v, want adjacency: 1 incident edge against every edge in the range\nplan: %s",
					plan.Driver, plan)
			}
		})
	}
}

// --- residual ordering ---

// TestRangeResidualSortsBySize pins the second half of the change: a filter that
// did not drive still has to be *costed*, and a range used to be costed at every
// entry under its key. A range matching 5 has to run before an equality filter
// matching 1000, or the query does a thousand probes it could have done five of.
func TestRangeResidualSortsBySize(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			orderedDriverFixture(t, g)

			// Types drives (10 nodes), leaving both filters as residuals. The
			// range is deliberately wider than the label — 50 against 10 — because
			// a range narrower than the label would drive, which is what the
			// cases above already pin.
			q := store.NodeQuery{
				Types: []store.NodeType{store.NodeTypeCase},
				Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")}, // 1000
					tsBelow(50), // 50
				},
			}
			plan := checkPlan(t, g, q, store.DriverLabels, 50, "labels are the cheapest driver here")
			if len(plan.Residuals) != 2 {
				t.Fatalf("got %d residuals, want 2\nplan: %s", len(plan.Residuals), plan)
			}
			if plan.Residuals[0].Key != "ts" {
				t.Errorf("residuals run %q first, want %q: 5 matches against 1000\nplan: %s",
					plan.Residuals[0].Key, "ts", plan)
			}
			// The cost has to be the range's own size, not the key's. Every node
			// carries a "ts", so the old estimate was 2000.
			if plan.Residuals[0].Cost > 60 {
				t.Errorf("range residual costed at %d, want ~5: it is being sized as the whole key\nplan: %s",
					plan.Residuals[0].Cost, plan)
			}
		})
	}
}
