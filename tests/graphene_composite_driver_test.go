package graphene_test

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// Composite indexes as query drivers.
//
// The single-key postings answer "which entities have key=value". A conjunction
// of two of those has to drive from one and eliminate against the other, which
// costs the driving set's size however small the answer is. The shape that hurts
// is a pair of individually weak keys whose combination is selective, and it is
// common: a case identifier and a bucket are each worth little alone and pin a
// query together.
//
// A composite index over the pair is one lookup returning exactly the
// conjunction. These tests assert on the *plan*, because results cannot tell a
// good plan from a bad one that returns the same rows — and every case runs
// against both backends, since the two planners have to reach the same decision
// from the same numbers.
//
// They reuse orderedDriverBackends and checkPlan from
// graphene_ordered_driver_test.go: same harness, same reason.

const compositeNodes = 2000

// compositeFixture builds a graph whose two indexed keys are individually weak
// and jointly selective:
//
//	"case"                — 20 values, 100 nodes each
//	"bucket"              — 3 values, ~667 nodes each
//	case=CF AND bucket=hot  — 33 nodes  (20 and 3 are coprime, so the
//	                          conjunction is a real intersection rather than
//	                          one key implying the other)
//	"pin"="p"             — those same 33 nodes, for the tie case
//	NodeTypeCase          — 10 nodes    (a label more selective than the conjunction)
//	NodeTypeTag           — 50 nodes    (more selective than either key, less than
//	                          the conjunction is not — it sits between the two,
//	                          which is where the label-vs-composite comparison
//	                          actually has to be made)
//	NodeTypeMicroArtefact — 1990 nodes  (a label less selective than both)
//
// Properties are registered before the composite is declared, so the default
// path through these tests exercises the backfill. TestCompositeMaintained-
// Incrementally covers the other order.
func compositeFixture(t *testing.T, g *graphene.Graph) []store.NodeID {
	t.Helper()
	nodes := make([]*store.Node, compositeNodes)
	for i := range nodes {
		labels := []store.NodeType{store.NodeTypeMicroArtefact}
		if i%200 == 0 {
			labels = []store.NodeType{store.NodeTypeCase}
		}
		if i%40 == 0 {
			labels = append(labels, store.NodeTypeTag)
		}
		nodes[i] = &store.Node{Labels: labels}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		props := map[string][]byte{
			"case":   caseValue(i),
			"bucket": bucketValue(i),
		}
		if inConjunction(i) {
			props["pin"] = []byte("p")
		}
		if err := g.IndexNodeProperties(id, props); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}
	if err := g.DeclareCompositeProperties([]string{"case", "bucket"}); err != nil {
		t.Fatalf("DeclareCompositeProperties: %v", err)
	}
	return ids
}

var bucketNames = [3]string{"hot", "warm", "cold"}

func caseValue(i int) []byte   { return []byte("C" + string(rune('A'+i%20))) }
func bucketValue(i int) []byte { return []byte(bucketNames[i%3]) }

// inConjunction is the fixture's answer, computed independently of the index so
// that a wrong plan cannot also supply the expectation it is checked against.
func inConjunction(i int) bool { return i%20 == 5 && i%3 == 0 }

func conjunctionSize() int {
	n := 0
	for i := 0; i < compositeNodes; i++ {
		if inConjunction(i) {
			n++
		}
	}
	return n
}

// conjunctionQuery pins both member keys, in the order they were declared.
func conjunctionQuery() store.NodeQuery {
	return store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "case", Op: store.PropertyOpEqual, Value: caseValue(5)},
		{Key: "bucket", Op: store.PropertyOpEqual, Value: bucketValue(0)},
	}}
}

// TestCompositeBeatsIntersectingTwoWeakFilters is the case the composite exists
// for: neither filter is selective, and together they are.
func TestCompositeBeatsIntersectingTwoWeakFilters(t *testing.T) {
	want := conjunctionSize()
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			q := conjunctionQuery()
			plan := checkPlan(t, g, q, store.DriverComposite, want,
				"33 nodes satisfy both filters, against 100 and 667 satisfying one")

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want {
				t.Errorf("got %d nodes, want %d\nplan: %s", len(got), want, plan)
			}
		})
	}
}

// TestWithoutADeclarationTheSameQueryIntersects pins what the composite
// replaces. Without it the planner drives from the better of the two filters and
// eliminates against the other — the right choice among what it has, and a
// hundred candidates for thirty-three answers.
func TestWithoutADeclarationTheSameQueryIntersects(t *testing.T) {
	want := conjunctionSize()
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			// The fixture without the declaration.
			nodes := make([]*store.Node, compositeNodes)
			for i := range nodes {
				nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
			}
			ids, err := g.AddNodes(nodes)
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			for i, id := range ids {
				if err := g.IndexNodeProperties(id, map[string][]byte{
					"case": caseValue(i), "bucket": bucketValue(i),
				}); err != nil {
					t.Fatalf("IndexNodeProperties: %v", err)
				}
			}

			q := conjunctionQuery()
			plan := checkPlan(t, g, q, store.DriverEquality, 100,
				"with nothing declared, the more selective single key drives")
			if plan.Candidates <= want {
				t.Errorf("candidates=%d, expected more than the %d answers — "+
					"if this ever equals the answer size the composite has nothing to buy\nplan: %s",
					plan.Candidates, want, plan)
			}

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want {
				t.Errorf("got %d nodes, want %d: the answer must not depend on the plan\nplan: %s",
					len(got), want, plan)
			}
		})
	}
}

// TestCompositeConsumesEveryFilterItCovers pins the reason the driver reports a
// set of filters rather than one index: the residual pass must not re-apply the
// conjunction the candidates were built from.
func TestCompositeConsumesEveryFilterItCovers(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			plan := checkPlan(t, g, conjunctionQuery(), store.DriverComposite, -1, "")
			if plan.DriverFilters.Count() != 2 {
				t.Errorf("driver consumed %d filters, want 2\nplan: %s",
					plan.DriverFilters.Count(), plan)
			}
			if !plan.DriverFilters.Has(0) || !plan.DriverFilters.Has(1) {
				t.Errorf("driver consumed %#b, want both filters\nplan: %s",
					uint64(plan.DriverFilters), plan)
			}
			if len(plan.Residuals) != 0 {
				t.Errorf("plan has %d residual steps, want none — the composite already "+
					"applied every filter\nplan: %s", len(plan.Residuals), plan)
			}
		})
	}
}

// TestCompositeIsMatchedByKeySetNotFilterOrder pins that a declaration's order
// is its identity and not a constraint on its use.
func TestCompositeIsMatchedByKeySetNotFilterOrder(t *testing.T) {
	want := conjunctionSize()
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			// Declared (case, bucket); asked in the opposite order.
			q := store.NodeQuery{Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: bucketValue(0)},
				{Key: "case", Op: store.PropertyOpEqual, Value: caseValue(5)},
			}}
			plan := checkPlan(t, g, q, store.DriverComposite, want,
				"a composite is matched against the key set, not the filter order")

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want {
				t.Errorf("got %d nodes, want %d\nplan: %s", len(got), want, plan)
			}
		})
	}
}

// TestCompositeIsNotUsedForAPartialTuple pins the one thing a hash-keyed
// composite cannot do. The postings are keyed by the whole tuple, so a query
// that pins only some of its keys has no entry to look up — and must fall back
// rather than answer from a tuple it did not specify.
func TestCompositeIsNotUsedForAPartialTuple(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			q := store.NodeQuery{Filters: []store.PropertyFilter{
				{Key: "case", Op: store.PropertyOpEqual, Value: caseValue(5)},
			}}
			plan := checkPlan(t, g, q, store.DriverEquality, 100,
				"one of the two declared keys cannot address the tuple")

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 100 {
				t.Errorf("got %d nodes, want 100\nplan: %s", len(got), plan)
			}
		})
	}
}

// TestSelectiveLabelBeatsComposite pins that the composite is compared rather
// than preferred. A 10-node label is a smaller starting set than a 33-node
// conjunction, and the planner has to say so.
func TestSelectiveLabelBeatsComposite(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			q := conjunctionQuery()
			q.Types = []store.NodeType{store.NodeTypeCase} // 10 nodes
			checkPlan(t, g, q, store.DriverLabels, 10,
				"10 nodes carry the label against 33 in the conjunction")
		})
	}
}

// TestCompositeBeatsAWeakLabel is the mirror: the label is the wrong driver when
// it is bigger, and the composite has to win against it.
func TestCompositeBeatsAWeakLabel(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			q := conjunctionQuery()
			q.Types = []store.NodeType{store.NodeTypeMicroArtefact} // 1990 nodes
			checkPlan(t, g, q, store.DriverComposite, conjunctionSize(),
				"1990 nodes carry the label against 33 in the conjunction")
		})
	}
}

// TestCompositeBeatsALabelBetweenTheTwoSingleKeys is the comparison that only a
// middling label can make. NodeTypeTag carries 50 nodes: fewer than either
// filter alone, more than the conjunction. A planner that compared the label
// only against the single-key filters would take it — and a planner that never
// compared it at all would take it too, since it is checked first.
func TestCompositeBeatsALabelBetweenTheTwoSingleKeys(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			q := conjunctionQuery()
			q.Types = []store.NodeType{store.NodeTypeTag} // 50 nodes
			checkPlan(t, g, q, store.DriverComposite, conjunctionSize(),
				"50 nodes carry the label, 100 and 667 the single filters, 33 the conjunction")
		})
	}
}

// TestCompositeWinsATieAgainstEquality pins the tie-break. "pin" is registered on
// exactly the conjunction, so the best single-key filter and the composite report
// the same size — and a tie on candidates is not a tie on the work that follows,
// because the composite retires two filters where equality retires one.
func TestCompositeWinsATieAgainstEquality(t *testing.T) {
	want := conjunctionSize()
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeFixture(t, g)

			q := conjunctionQuery()
			q.Filters = append(q.Filters, store.PropertyFilter{
				Key: "pin", Op: store.PropertyOpEqual, Value: []byte("p"),
			})
			plan := checkPlan(t, g, q, store.DriverComposite, want,
				"the composite and the pin filter both match 33; the composite retires two filters")
			if plan.DriverFilters.Count() != 2 {
				t.Errorf("driver consumed %d filters, want the composite's 2\nplan: %s",
					plan.DriverFilters.Count(), plan)
			}
			if len(plan.Residuals) != 1 {
				t.Errorf("plan has %d residual steps, want 1 (the pin filter)\nplan: %s",
					len(plan.Residuals), plan)
			}

			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want {
				t.Errorf("got %d nodes, want %d\nplan: %s", len(got), want, plan)
			}
		})
	}
}

// TestCompositeMaintainedIncrementally declares before any property is
// registered, so nothing is absorbed and every entry arrives through the
// registration path instead. It has to reach the same index either way.
func TestCompositeMaintainedIncrementally(t *testing.T) {
	want := conjunctionSize()
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			if err := g.DeclareCompositeProperties([]string{"case", "bucket"}); err != nil {
				t.Fatalf("DeclareCompositeProperties: %v", err)
			}
			nodes := make([]*store.Node, compositeNodes)
			for i := range nodes {
				nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
			}
			ids, err := g.AddNodes(nodes)
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			// One key at a time across the whole graph, so every entity is
			// half-registered for a while. An entity is filed only once its tuple
			// is complete, and this is what proves the incomplete state is not
			// filed early or forgotten late.
			for i, id := range ids {
				if err := g.IndexNodeProperty(id, "case", caseValue(i)); err != nil {
					t.Fatalf("IndexNodeProperty: %v", err)
				}
			}
			for i, id := range ids {
				if err := g.IndexNodeProperty(id, "bucket", bucketValue(i)); err != nil {
					t.Fatalf("IndexNodeProperty: %v", err)
				}
			}

			q := conjunctionQuery()
			plan := checkPlan(t, g, q, store.DriverComposite, want,
				"an index built by registration must match one built by backfill")
			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want {
				t.Errorf("got %d nodes, want %d\nplan: %s", len(got), want, plan)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Errorf("VerifyIndexes: %v", err)
			}
		})
	}
}

// TestCompositeFollowsADeletedNode pins the removal path. A composite that keeps
// a deleted entity returns a row the graph no longer has, and the driver applies
// no residual filter that would catch it.
func TestCompositeFollowsADeletedNode(t *testing.T) {
	want := conjunctionSize()
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			ids := compositeFixture(t, g)

			var victim store.NodeID
			for i := range ids {
				if inConjunction(i) {
					victim = ids[i]
					break
				}
			}
			if err := g.DeleteNode(victim); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}

			plan := checkPlan(t, g, conjunctionQuery(), store.DriverComposite, want-1,
				"a deleted node must leave the composite, not merely fail to resolve")
			got, err := g.QueryNodeIDs(conjunctionQuery())
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want-1 {
				t.Errorf("got %d nodes, want %d\nplan: %s", len(got), want-1, plan)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Errorf("VerifyIndexes: %v", err)
			}
		})
	}
}

// TestCompositeFilesAMultiValuedMemberUnderBothTuples is the case that decides
// whether a composite is a valid substitute for the intersection it replaces.
//
// Index entries are additive: UpdateNode does not purge them, so re-registering
// a key leaves the entity matching both values through the single-key postings.
// A composite that filed it under only the newest value would return fewer rows
// than the intersection — silently, because the driver applies no residual for
// the keys it covers.
func TestCompositeFilesAMultiValuedMemberUnderBothTuples(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			ids := compositeFixture(t, g)

			// Pick a node in case=C05 whose bucket is *not* hot, then also
			// register it as hot. It now matches both, so it joins the
			// conjunction.
			var promoted store.NodeID
			promotedIdx := -1
			for i := range ids {
				if i%20 == 5 && i%3 != 0 {
					promoted, promotedIdx = ids[i], i
					break
				}
			}
			if err := g.IndexNodeProperty(promoted, "bucket", bucketValue(0)); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}

			want := conjunctionSize() + 1
			q := conjunctionQuery()
			plan := checkPlan(t, g, q, store.DriverComposite, want, "")
			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want {
				t.Fatalf("got %d nodes, want %d — an entity carrying two values for a "+
					"member key belongs under both tuples\nplan: %s", len(got), want, plan)
			}
			found := false
			for _, id := range got {
				if id == promoted {
					found = true
				}
			}
			if !found {
				t.Errorf("node %d matches both filters through the single-key postings "+
					"but the composite did not return it", promoted)
			}

			// And it is still under its original tuple — the one it had before the
			// second value arrived, which additive entries mean it still matches.
			old := store.NodeQuery{Filters: []store.PropertyFilter{
				{Key: "case", Op: store.PropertyOpEqual, Value: caseValue(promotedIdx)},
				{Key: "bucket", Op: store.PropertyOpEqual, Value: bucketValue(promotedIdx)},
			}}
			stillThere, err := g.QueryNodeIDs(old)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			found = false
			for _, id := range stillThere {
				if id == promoted {
					found = true
				}
			}
			if !found {
				t.Errorf("node %d lost its original tuple when a second value arrived", promoted)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Errorf("VerifyIndexes: %v", err)
			}
		})
	}
}

// TestCompositeRejectsAnUnindexableTuple pins the declaration's own validation.
// Each of these would otherwise produce an index that is either a duplicate of
// the single-key postings or impossible to address.
func TestCompositeRejectsAnUnindexableTuple(t *testing.T) {
	cases := map[string][]string{
		"one key":     {"case"},
		"no keys":     {},
		"repeated":    {"case", "case"},
		"empty key":   {"case", ""},
		"too many":    make([]string, 65),
		"two is fine": {"case", "bucket"},
	}
	for i := range cases["too many"] {
		cases["too many"][i] = string(rune('a' + i%26))
	}
	// The 65-key tuple would also be rejected for repeats, which would prove the
	// wrong thing; make every key distinct.
	for i := range cases["too many"] {
		cases["too many"][i] = "k" + string(rune('A'+i/26)) + string(rune('a'+i%26))
	}

	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			for what, keys := range cases {
				err := g.DeclareCompositeProperties(keys)
				if what == "two is fine" {
					if err != nil {
						t.Errorf("%s: unexpected error %v", what, err)
					}
					continue
				}
				if !errors.Is(err, index.ErrCompositeKeys) {
					t.Errorf("%s: got %v, want ErrCompositeKeys", what, err)
				}
			}
		})
	}
}

// TestCompositeDeclarationsSurviveCompaction pins the GCMP section. Declarations
// are written into the image and re-applied on open, so a store that compacted
// after declaring reopens with its composites intact — and without them, every
// such query silently goes back to intersecting.
func TestCompositeDeclarationsSurviveCompaction(t *testing.T) {
	dir := t.TempDir()
	want := conjunctionSize()

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	compositeFixture(t, g)
	if err := g.DeclareCompositeEdgeProperties([]string{"kind", "stage"}); err != nil {
		t.Fatalf("DeclareCompositeEdgeProperties: %v", err)
	}
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
	defer func() { _ = reopened.Close() }()

	nodeTuples, edgeTuples := reopened.CompositeProperties()
	if len(nodeTuples) != 1 || len(nodeTuples[0]) != 2 ||
		nodeTuples[0][0] != "case" || nodeTuples[0][1] != "bucket" {
		t.Fatalf("node composites after reopen: %v, want [[case bucket]]", nodeTuples)
	}
	if len(edgeTuples) != 1 || len(edgeTuples[0]) != 2 ||
		edgeTuples[0][0] != "kind" || edgeTuples[0][1] != "stage" {
		t.Fatalf("edge composites after reopen: %v, want [[kind stage]]", edgeTuples)
	}

	plan := checkPlan(t, reopened, conjunctionQuery(), store.DriverComposite, want,
		"a declaration written to the image must drive after a reopen")
	got, err := reopened.QueryNodeIDs(conjunctionQuery())
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if len(got) != want {
		t.Errorf("got %d nodes, want %d\nplan: %s", len(got), want, plan)
	}
	if err := reopened.VerifyIndexes(); err != nil {
		t.Errorf("VerifyIndexes: %v", err)
	}
}

// --- edges ---

// compositeEdgeFixture builds a star of 1200 edges with two weak keys whose
// conjunction is narrow, all under one label and all anchored on one node — so
// the composite has to beat the adjacency driver and the label driver as well as
// the single-key postings.
func compositeEdgeFixture(t *testing.T, g *graphene.Graph) store.NodeID {
	t.Helper()
	const n = 1200
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
		if err := g.IndexEdgeProperties(eid, map[string][]byte{
			"kind":  []byte("K" + string(rune('a'+i%20))), // 60 edges each
			"stage": []byte(bucketNames[i%3]),             // 400 edges each
		}); err != nil {
			t.Fatalf("IndexEdgeProperties: %v", err)
		}
	}
	if err := g.DeclareCompositeEdgeProperties([]string{"kind", "stage"}); err != nil {
		t.Fatalf("DeclareCompositeEdgeProperties: %v", err)
	}
	return ids[0]
}

// TestEdgeCompositeBeatsAnchorAndLabel is the edge planner's version. Its switch
// has four other drivers in it, and the composite has to be compared with all of
// them rather than tried after them.
func TestEdgeCompositeBeatsAnchorAndLabel(t *testing.T) {
	// kind=Kf AND stage=hot: i%20==5 and i%3==0 over 1200 edges.
	want := 0
	for i := 0; i < 1200; i++ {
		if i%20 == 5 && i%3 == 0 {
			want++
		}
	}
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			hub := compositeEdgeFixture(t, g)

			q := store.EdgeQuery{
				Types:  []store.EdgeType{store.EdgeTypeContains}, // 1200
				SrcIDs: []store.NodeID{hub},                      // degree 1200
				Filters: []store.PropertyFilter{
					{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("Kf")},   // 60
					{Key: "stage", Op: store.PropertyOpEqual, Value: []byte("hot")}, // 400
				},
			}
			plan, err := g.ExplainEdgeQuery(q)
			if err != nil {
				t.Fatalf("ExplainEdgeQuery: %v", err)
			}
			if plan.Driver != store.DriverComposite {
				t.Errorf("drove from %v, want composite: %d edges satisfy both filters "+
					"against 60, 400, 1200 and a hub degree of 1200\nplan: %s",
					plan.Driver, want, plan)
			}
			if plan.Candidates > want {
				t.Errorf("candidates=%d, want at most %d\nplan: %s", plan.Candidates, want, plan)
			}

			got, err := g.QueryEdgeIDs(q)
			if err != nil {
				t.Fatalf("QueryEdgeIDs: %v", err)
			}
			if len(got) != want {
				t.Errorf("got %d edges, want %d\nplan: %s", len(got), want, plan)
			}
		})
	}
}

// TestEdgeCompositeLosesToASelectiveAnchor is the same comparison the other way:
// a hub with degree 3 bounds the query better than a 20-edge conjunction, and the
// planner must say so rather than take the composite because it is there.
func TestEdgeCompositeLosesToASelectiveAnchor(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			compositeEdgeFixture(t, g)

			// A second, tiny hub with three outbound edges.
			extra := make([]*store.Node, 4)
			for i := range extra {
				extra[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
			}
			ids, err := g.AddNodes(extra)
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			small := make([]*store.Edge, 3)
			for i := range small {
				small[i] = &store.Edge{
					Src:    ids[0],
					Dst:    ids[i+1],
					Labels: []store.EdgeType{store.EdgeTypeContains},
				}
			}
			if _, err := g.AddEdges(small); err != nil {
				t.Fatalf("AddEdges: %v", err)
			}

			q := store.EdgeQuery{
				SrcIDs: []store.NodeID{ids[0]}, // degree 3
				Filters: []store.PropertyFilter{
					{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("Kf")},
					{Key: "stage", Op: store.PropertyOpEqual, Value: []byte("hot")},
				},
			}
			plan, err := g.ExplainEdgeQuery(q)
			if err != nil {
				t.Fatalf("ExplainEdgeQuery: %v", err)
			}
			if plan.Driver != store.DriverAdjacency {
				t.Errorf("drove from %v, want adjacency: a degree of 3 beats the "+
					"conjunction's 20\nplan: %s", plan.Driver, plan)
			}
		})
	}
}

// TestCompositeDoesNotCollideOnValueBoundaries pins the tuple encoding. Values
// are arbitrary bytes, so a composite that joined them with a separator — or
// concatenated them plainly — would file ("a", "bc") and ("ab", "c") under the
// same key and return each for the other.
//
// The failure this prevents is a query returning an entity that matches neither
// of its filters, with no residual pass left to catch it.
func TestCompositeDoesNotCollideOnValueBoundaries(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			if err := g.DeclareCompositeProperties([]string{"left", "right"}); err != nil {
				t.Fatalf("DeclareCompositeProperties: %v", err)
			}
			ids, err := g.AddNodes([]*store.Node{
				{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			})
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			if err := g.IndexNodeProperties(ids[0], map[string][]byte{
				"left": []byte("a"), "right": []byte("bc"),
			}); err != nil {
				t.Fatalf("IndexNodeProperties: %v", err)
			}
			if err := g.IndexNodeProperties(ids[1], map[string][]byte{
				"left": []byte("ab"), "right": []byte("c"),
			}); err != nil {
				t.Fatalf("IndexNodeProperties: %v", err)
			}

			q := store.NodeQuery{Filters: []store.PropertyFilter{
				{Key: "left", Op: store.PropertyOpEqual, Value: []byte("a")},
				{Key: "right", Op: store.PropertyOpEqual, Value: []byte("bc")},
			}}
			plan := checkPlan(t, g, q, store.DriverComposite, 1, "")
			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != 1 || got[0] != ids[0] {
				t.Errorf("got %v, want just %v — the two entities differ only in where "+
					"one value ends and the next begins\nplan: %s", got, ids[0], plan)
			}
		})
	}
}

// TestCompositeOverThreeKeys pins that width is not fixed at two: the tuple, the
// per-entity member state and the filter mask all have to carry three.
func TestCompositeOverThreeKeys(t *testing.T) {
	for name, g := range orderedDriverBackends(t) {
		t.Run(name, func(t *testing.T) {
			if err := g.DeclareCompositeProperties([]string{"case", "bucket", "host"}); err != nil {
				t.Fatalf("DeclareCompositeProperties: %v", err)
			}
			nodes := make([]*store.Node, compositeNodes)
			for i := range nodes {
				nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
			}
			ids, err := g.AddNodes(nodes)
			if err != nil {
				t.Fatalf("AddNodes: %v", err)
			}
			// host splits the graph in two, and has to split the *conjunction* in
			// two as well. Alternating on i would not: every node in the
			// conjunction has i ≡ 5 (mod 20) and so is odd, which would leave the
			// three-key result empty and every assertion below true for the wrong
			// reason. Alternating on i/20 cuts across it instead.
			want := 0
			for i, id := range ids {
				host := "h1"
				if (i/20)%2 == 1 {
					host = "h0"
				}
				if err := g.IndexNodeProperties(id, map[string][]byte{
					"case": caseValue(i), "bucket": bucketValue(i), "host": []byte(host),
				}); err != nil {
					t.Fatalf("IndexNodeProperties: %v", err)
				}
				if inConjunction(i) && (i/20)%2 == 0 {
					want++
				}
			}
			if want == 0 || want >= conjunctionSize() {
				t.Fatalf("the three-key conjunction is %d against the two-key %d — it has to be "+
					"a proper subset or this proves nothing", want, conjunctionSize())
			}

			q := store.NodeQuery{Filters: []store.PropertyFilter{
				{Key: "case", Op: store.PropertyOpEqual, Value: caseValue(5)},
				{Key: "bucket", Op: store.PropertyOpEqual, Value: bucketValue(0)},
				{Key: "host", Op: store.PropertyOpEqual, Value: []byte("h1")},
			}}
			plan := checkPlan(t, g, q, store.DriverComposite, want, "")
			if plan.DriverFilters.Count() != 3 {
				t.Errorf("driver consumed %d filters, want 3\nplan: %s",
					plan.DriverFilters.Count(), plan)
			}
			got, err := g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != want {
				t.Errorf("got %d nodes, want %d\nplan: %s", len(got), want, plan)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Errorf("VerifyIndexes: %v", err)
			}

			// Two of the three keys. This is the query that reaches the coverage
			// test with something to reject: there are enough equality filters
			// for a composite to be considered at all, and they do not address
			// the tuple. Accepting a partial cover would look up ("CF", "hot",
			// "") — no entity has an empty host, so a query with answers would
			// come back with none.
			partial := store.NodeQuery{Filters: q.Filters[:2]}
			plan = checkPlan(t, g, partial, store.DriverEquality, 100,
				"two of three keys cannot address the tuple")
			got, err = g.QueryNodeIDs(partial)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != conjunctionSize() {
				t.Errorf("got %d nodes, want %d for the two-key conjunction\nplan: %s",
					len(got), conjunctionSize(), plan)
			}
		})
	}
}
