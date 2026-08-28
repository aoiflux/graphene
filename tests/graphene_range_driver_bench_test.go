//go:build stress

// What costing a range filter is worth.
//
// The planner used to have no size for a range or prefix, so it could not
// compare one with anything and ordered its drivers by position instead: on the
// node side any range beat any label, on the edge side no range beat anything.
// index/stats.go supplies the size; these measure what the resulting plans are
// worth on a store big enough for a bad plan to hurt.
//
// Three shapes, chosen because each one exercised a different half of the fault:
//
//	SelectiveRange   a range matching 50 of 50 000, beside a 25 000-hit
//	                 equality filter. Used to drive from the filter.
//	WideRange        a range matching every node, beside a 100-node label.
//	                 Used to drive from the range.
//	EdgeRange        a range matching 50 edges, beside a label carrying every
//	                 edge. The range could not drive at all.
//
// Written against the public API only, so this file compiles unchanged in a HEAD
// worktree — which is the point: the comparison is against the old planner, and
// a benchmark that only builds on one side cannot make it.

package graphene_test

import (
	"os"
	"sync"
	"testing"

	graphene "github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/index/encoding"
	"github.com/aoiflux/graphene/store"
)

const rangeBenchNodes = 50_000

var (
	rangeBenchOnce  sync.Once
	rangeBenchGraph *graphene.Graph
)

// rangeBenchFixture builds one store shared by every benchmark here:
//
//	NodeTypeCase   — 100 of 50 000        (0.2%, very selective label)
//	"tier"="warm"  — 25 000 of 50 000     (50%, nearly useless as a driver)
//	"ts"           — unique per node, declared ordered
//	EdgeTypeContains over every edge, with an ordered "seq"
func rangeBenchFixture(b *testing.B) *graphene.Graph {
	b.Helper()
	rangeBenchOnce.Do(func() {
		dir, err := os.MkdirTemp("", "graphene-range-bench")
		if err != nil {
			panic(err)
		}
		g, err := graphene.Open(dir)
		if err != nil {
			panic(err)
		}
		if err := g.DeclareOrderedProperty("ts"); err != nil {
			panic(err)
		}
		if err := g.DeclareOrderedEdgeProperty("seq"); err != nil {
			panic(err)
		}

		nodes := make([]*store.Node, rangeBenchNodes)
		for i := range nodes {
			label := store.NodeTypeMicroArtefact
			if i%500 == 0 {
				label = store.NodeTypeCase
			}
			nodes[i] = &store.Node{Labels: []store.NodeType{label}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			panic(err)
		}
		for i, id := range ids {
			tier := "cold"
			if i%2 == 0 {
				tier = "warm"
			}
			if err := g.IndexNodeProperties(id, map[string][]byte{
				"ts":   encoding.Uint64(uint64(i)),
				"tier": []byte(tier),
			}); err != nil {
				panic(err)
			}
		}

		// A star, so one leaf has exactly one inbound edge.
		const edgeCount = 10_000
		edges := make([]*store.Edge, edgeCount)
		for i := range edges {
			edges[i] = &store.Edge{
				Src:    ids[0],
				Dst:    ids[i+1],
				Labels: []store.EdgeType{store.EdgeTypeContains},
			}
		}
		eids, err := g.AddEdges(edges)
		if err != nil {
			panic(err)
		}
		for i, eid := range eids {
			if err := g.IndexEdgeProperties(eid, map[string][]byte{"seq": encoding.Uint64(uint64(i))}); err != nil {
				panic(err)
			}
		}
		// Compact, so the queries run against the CSR postings rather than the
		// delta: that is the state a store spends its life in.
		if err := g.Compact(); err != nil {
			panic(err)
		}
		rangeBenchGraph = g
	})
	return rangeBenchGraph
}

// reportPlan attaches the chosen driver and candidate count to the benchmark, so
// a run records *why* it was fast rather than only that it was. Candidates is
// the number that changed; ns/op is the consequence.
func reportPlan(b *testing.B, g *graphene.Graph, q store.NodeQuery) {
	b.Helper()
	plan, err := g.ExplainNodeQuery(q)
	if err != nil {
		b.Fatalf("ExplainNodeQuery: %v", err)
	}
	b.ReportMetric(float64(plan.Candidates), "candidates")
	b.Logf("plan: %s", plan)
}

// BenchmarkRangeDriver_Selective is the node planner's old blind spot: an
// equality filter was taken unconditionally, so a range matching 50 rows lost to
// one matching 25 000.
func BenchmarkRangeDriver_Selective(b *testing.B) {
	g := rangeBenchFixture(b)
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "tier", Op: store.PropertyOpEqual, Value: []byte("warm")}, // 25 000
		{Key: "ts", Op: store.PropertyOpLessThan, Value: encoding.Uint64(50)},
	}}
	reportPlan(b, g, q)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryNodeIDs(q); err != nil {
			b.Fatalf("QueryNodeIDs: %v", err)
		}
	}
}

// BenchmarkRangeDriver_Wide is the same fault pointing the other way: with no
// equality filter present, the first range drove whatever it matched, so a
// window over every value beat a 100-node label.
func BenchmarkRangeDriver_Wide(b *testing.B) {
	g := rangeBenchFixture(b)
	q := store.NodeQuery{
		Types: []store.NodeType{store.NodeTypeCase}, // 100
		Filters: []store.PropertyFilter{
			{Key: "ts", Op: store.PropertyOpLessThan, Value: encoding.Uint64(rangeBenchNodes)},
		},
	}
	reportPlan(b, g, q)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryNodeIDs(q); err != nil {
			b.Fatalf("QueryNodeIDs: %v", err)
		}
	}
}

// BenchmarkRangeDriver_Edge is the edge planner's version, where the ordered
// driver was not mis-ordered but unreachable: it sat below a switch that any
// label already satisfied.
func BenchmarkRangeDriver_Edge(b *testing.B) {
	g := rangeBenchFixture(b)
	q := store.EdgeQuery{
		Types: []store.EdgeType{store.EdgeTypeContains}, // 10 000
		Filters: []store.PropertyFilter{
			{Key: "seq", Op: store.PropertyOpLessThan, Value: encoding.Uint64(50)},
		},
	}
	plan, err := g.ExplainEdgeQuery(q)
	if err != nil {
		b.Fatalf("ExplainEdgeQuery: %v", err)
	}
	b.ReportMetric(float64(plan.Candidates), "candidates")
	b.Logf("plan: %s", plan)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryEdgeIDs(q); err != nil {
			b.Fatalf("QueryEdgeIDs: %v", err)
		}
	}
}

// BenchmarkRangeDriver_Control is the control: a query whose plan this change
// cannot alter, because it has no range in it at all. It must not move. If it
// does, the run is measuring the machine rather than the planner.
func BenchmarkRangeDriver_Control(b *testing.B) {
	g := rangeBenchFixture(b)
	q := store.NodeQuery{
		Types:   []store.NodeType{store.NodeTypeCase},
		Filters: []store.PropertyFilter{{Key: "tier", Op: store.PropertyOpEqual, Value: []byte("warm")}},
	}
	reportPlan(b, g, q)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryNodeIDs(q); err != nil {
			b.Fatalf("QueryNodeIDs: %v", err)
		}
	}
}

// BenchmarkRangeEstimate_Cost is what the estimate itself costs, isolated from
// the query it informs: ExplainNodeQuery plans and then runs, so the planning
// half is measured here by planning a query whose driver resolves to nothing.
//
// This is the figure that would condemn the whole approach if the estimate were
// expensive — a planner that costs more than the plan it saves is a regression
// dressed as an optimisation.
func BenchmarkRangeEstimate_Cost(b *testing.B) {
	g := rangeBenchFixture(b)
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "ts", Op: store.PropertyOpGreaterThan, Value: encoding.Uint64(rangeBenchNodes + 1)},
	}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.ExplainNodeQuery(q); err != nil {
			b.Fatalf("ExplainNodeQuery: %v", err)
		}
	}
}
