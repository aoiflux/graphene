//go:build stress

// Planner driver-selection benchmarks.
//
// The standard fixture cannot express the case these exist for. Its labels and
// property cardinalities happen to line up — NodeTypeCase covers 100 nodes and
// a "bucket" value covers ~100 — so label and equality drivers tie, and ties go
// to equality. Nothing in the suite had a *selective label alongside a weak
// filter*, which is precisely the shape where driver choice matters most.
//
// This fixture deliberately separates the two axes:
//
//	NodeTypeCase  — 100 of 50 000 nodes   (0.2%, very selective)
//	"tier"="warm" — 25 000 of 50 000      (50%, nearly useless as a driver)
//
// Driving from "tier" means materialising 25 000 candidates and discarding
// 24 900 of them. Driving from the label means 100.

package graphene_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	graphene "github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

const plannerNodeCount = 50_000

var (
	plannerOnce sync.Once
	plannerFix  *benchFixture
	plannerDir  string
)

func plannerDiskGraph() *benchFixture {
	plannerOnce.Do(func() {
		dir, err := os.MkdirTemp("", "graphene-planner-bench-*")
		if err != nil {
			panic(err)
		}
		plannerDir = dir
		g, err := graphene.Open(dir)
		if err != nil {
			panic(err)
		}

		nodes := make([]*store.Node, plannerNodeCount)
		for i := range nodes {
			label := store.NodeTypeMicroArtefact
			if i%500 == 0 {
				label = store.NodeTypeCase
			}
			nodes[i] = &store.Node{Labels: []store.NodeType{label}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			panic(fmt.Sprintf("planner fixture: AddNodes: %v", err))
		}
		for i, id := range ids {
			tier := "cold"
			// Keyed on i/500 so "tier" varies within the Case label rather than
			// correlating with it.
			if (i/500)%2 == 0 {
				tier = "warm"
			}
			if err := g.IndexNodeProperties(id, map[string][]byte{
				"tier":   []byte(tier),
				"sha256": []byte(fmt.Sprintf("hash-%07d", i)),
			}); err != nil {
				panic(fmt.Sprintf("planner fixture: IndexNodeProperties: %v", err))
			}
		}
		if err := g.Compact(); err != nil {
			panic(err)
		}
		plannerFix = &benchFixture{g: g, ids: ids}
	})
	return plannerFix
}

// BenchmarkPlanner_SelectiveLabelWeakFilter is the case the change targets: the
// planner should drive from the 100-node label, not the 25 000-hit filter.
func BenchmarkPlanner_SelectiveLabelWeakFilter_Disk(b *testing.B) {
	f := plannerDiskGraph()
	q := store.NodeQuery{
		Types:   []store.NodeType{store.NodeTypeCase},
		Filters: []store.PropertyFilter{{Key: "tier", Op: store.PropertyOpEqual, Value: []byte("warm")}},
	}

	// Fail loudly rather than quietly measuring the wrong shape: if the fixture
	// ever stops producing a selective label, this benchmark stops meaning what
	// its name says.
	if plan, err := f.g.ExplainNodeQuery(q); err == nil && plan.Results == 0 {
		b.Fatalf("planner fixture produced no results: %s", plan)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPlanner_SelectiveFilterWeakLabel is the converse shape, where
// equality was already the right choice. It is the regression control for the
// added cardinality lookup: this query gains nothing from the change and must
// not lose anything either.
func BenchmarkPlanner_SelectiveFilterWeakLabel_Disk(b *testing.B) {
	f := plannerDiskGraph()
	q := store.NodeQuery{
		Types:   []store.NodeType{store.NodeTypeMicroArtefact},
		Filters: []store.PropertyFilter{{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-0000123")}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPlanner_LabelOnly has no filter at all, so driver selection is
// unchanged by definition — it isolates the cost of the added label count.
func BenchmarkPlanner_LabelOnly_Disk(b *testing.B) {
	f := plannerDiskGraph()
	q := store.NodeQuery{Types: []store.NodeType{store.NodeTypeCase}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}
