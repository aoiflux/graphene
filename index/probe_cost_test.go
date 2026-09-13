package index

// What a residual probe costs, and what the answer depends on.
//
// Residual planning has one decision to make per filter and two ways to make it
// wrong. It can probe each candidate through the reverse direction, or it can
// build the filter's own set and intersect. Before there was a base both sides of
// that comparison were counted in the same unit — a map lookup against a slice
// element — and `candidates < setSize` was an honest comparison.
//
// A mapped base breaks the unit. A probe of it is a binary search of a
// disk-resident array, so it is log2(entries) reads rather than one, and the
// comparison silently began preferring the probe across a band twenty-five times
// wide at the sizes this program is aimed at. These tests fix the cost model in
// place, fix the footprint bound that goes with it, and assert the thing neither
// of those may change: the answer.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// TestProbeCost_AStoreWithNoBaseChargesOneUnit: the model must be a no-op where
// it was already right. A resident index, and the memory backend, probe a map —
// one lookup, one unit — so every decision they made before this existed must
// come out the same way afterwards.
func TestProbeCost_AStoreWithNoBaseChargesOneUnit(t *testing.T) {
	p := NewPropertyIndex()
	if got := p.nodeProbeCost(); got != 1 {
		t.Fatalf("nodeProbeCost with no base = %d, want 1", got)
	}
	if got := p.edgeProbeCost(); got != 1 {
		t.Fatalf("edgeProbeCost with no base = %d, want 1", got)
	}
	if got := p.nodeCosts().probe; got != 1 {
		t.Fatalf("nodeCosts().probe with no base = %d, want 1", got)
	}
	if got := p.edgeCosts().probe; got != 1 {
		t.Fatalf("edgeCosts().probe with no base = %d, want 1", got)
	}

	// And at one unit the decision is the one the old two-argument form made,
	// which is what "no-op" has to mean here.
	for _, tc := range [][2]int{{0, 0}, {1, 1}, {1, 2}, {2, 1}, {99, 100}, {100, 99}, {4000, 3000}} {
		candidates, setSize := tc[0], tc[1]
		if got, want := probeIsCheaper(candidates, setSize, 1), candidates < setSize; got != want {
			t.Errorf("probeIsCheaper(%d, %d, 1) = %v, want %v — the unweighted comparison",
				candidates, setSize, got, want)
		}
	}
}

// TestReverseProbeCost_CountsTheSearchAndNotAGuess: the figure is meant to be the
// number of reads gpirLowerBound makes, so it is asserted against a search that
// counts them rather than against a table somebody wrote down.
func TestReverseProbeCost_CountsTheSearchAndNotAGuess(t *testing.T) {
	for _, entries := range []int{1, 2, 3, 4, 5, 7, 8, 9, 100, 1000, 1 << 16, 1 << 20, 28_000_000} {
		// The same loop gpirLowerBound runs, counting its iterations. Every probe
		// is answered "not below", which is the worst case: it is the branch that
		// halves the window rounding up, so it is the one a cost model wants.
		probes := 0
		for lo, hi := 0, entries; lo < hi; probes++ {
			mid := int(uint(lo+hi) >> 1)
			hi = mid
		}
		if got := reverseProbeCost(entries); got != probes {
			t.Errorf("reverseProbeCost(%d) = %d, but the search makes %d probes", entries, got, probes)
		}
	}

	// An empty or absent reverse array charges one rather than zero: a free probe
	// would make the planner probe unconditionally, and there is no such thing.
	for _, entries := range []int{-1, 0} {
		if got := reverseProbeCost(entries); got != 1 {
			t.Errorf("reverseProbeCost(%d) = %d, want 1", entries, got)
		}
	}

	// Monotone, because a larger image must never be cheaper to probe than a
	// smaller one. A model that dipped anywhere would make a store get faster to
	// probe by growing.
	last := 0
	for entries := 0; entries < 5000; entries++ {
		got := reverseProbeCost(entries)
		if got < last {
			t.Fatalf("reverseProbeCost(%d) = %d, below reverseProbeCost(%d) = %d",
				entries, got, entries-1, last)
		}
		last = got
	}
}

// TestProbeCost_EachKindIsSizedFromItsOwnReverseArray: nodes and edges are two
// arrays, and a store with two million edge entries and a handful of node entries
// must not plan its node residuals as though a node probe cost what an edge probe
// costs.
func TestProbeCost_EachKindIsSizedFromItsOwnReverseArray(t *testing.T) {
	p := NewPropertyIndex()
	b := newFakeBase([]triple{
		{NodeKind, 1, "size", []byte("10")},
		{EdgeKind, 7, "kind", []byte("near")},
	})
	// Poked rather than built: the cost is read from TotalEntries, and building
	// sixteen million triples to move a base-two logarithm by one would be a
	// fixture nobody can run. Every other read path still sees the two real
	// entries, which is why nothing here asks the fake to Verify itself.
	b.sides[NodeKind].entries = 28_000_000
	b.sides[EdgeKind].entries = 1000
	if err := p.AttachBase(b); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}

	if got, want := p.nodeProbeCost(), reverseProbeCost(28_000_000); got != want {
		t.Errorf("nodeProbeCost = %d, want %d", got, want)
	}
	if got, want := p.edgeProbeCost(), reverseProbeCost(1000); got != want {
		t.Errorf("edgeProbeCost = %d, want %d", got, want)
	}
	if p.nodeProbeCost() <= p.edgeProbeCost() {
		t.Errorf("the larger reverse array is not the dearer probe: node %d, edge %d",
			p.nodeProbeCost(), p.edgeProbeCost())
	}

	// The bundles the planner actually reads, against an asymmetric base so that
	// a bundle wired to the other kind is a different number rather than the same
	// one. Every test above this line passes with the two swapped.
	if got, want := p.nodeCosts().probe, p.nodeProbeCost(); got != want {
		t.Errorf("nodeCosts().probe = %d, want %d — the node bundle is not sized from the node array", got, want)
	}
	if got, want := p.edgeCosts().probe, p.edgeProbeCost(); got != want {
		t.Errorf("edgeCosts().probe = %d, want %d — the edge bundle is not sized from the edge array", got, want)
	}
}

// TestProbeIsCheaper_TheBandThatMovedIsTheOneTheBaseMadeExpensive: the whole
// behavioural change, stated as a table. Between setSize and probeCost*setSize the
// old model probed; the new one builds.
func TestProbeIsCheaper_TheBandThatMovedIsTheOneTheBaseMadeExpensive(t *testing.T) {
	const probeCost = 25
	// unweighted is what the old model decided, stated per row so that a row which
	// stops testing the change fails rather than passes quietly. Three of these
	// rows are cases the old model got wrong and four are cases it got right,
	// which is the shape a cost-model change should have.
	for _, tc := range []struct {
		candidates, setSize int
		probe, unweighted   bool
		why                 string
	}{
		{1, 1000, true, true, "one candidate against a thousand entries is still a probe"},
		{39, 1000, true, true, "39 probes is 975 reads, under the thousand a build walks"},
		{40, 1000, false, true, "40 probes is 1000 reads, which is no longer cheaper"},
		{100, 1000, false, true, "the band the old model got wrong: it probed here"},
		{999, 1000, false, true, "and here, at very nearly the same size"},
		{1000, 1000, false, false, "equal counts build under either model"},
		{2000, 1000, false, false, "more candidates than entries builds under either model"},
	} {
		t.Run(fmt.Sprintf("%d-of-%d", tc.candidates, tc.setSize), func(t *testing.T) {
			if got := probeIsCheaper(tc.candidates, tc.setSize, probeCost); got != tc.probe {
				t.Fatalf("probeIsCheaper(%d, %d, %d) = %v, want %v — %s",
					tc.candidates, tc.setSize, probeCost, got, tc.probe, tc.why)
			}
			if got := probeIsCheaper(tc.candidates, tc.setSize, 1); got != tc.unweighted {
				t.Fatalf("at one unit per probe, probeIsCheaper(%d, %d, 1) = %v, want %v — "+
					"this row no longer describes the model it is compared against",
					tc.candidates, tc.setSize, got, tc.unweighted)
			}
		})
	}

	moved := 0
	for candidates := 1; candidates < 1000; candidates++ {
		if probeIsCheaper(candidates, 1000, 1) != probeIsCheaper(candidates, 1000, probeCost) {
			moved++
		}
	}
	if moved != 960 {
		t.Errorf("the weighting moves %d of the 999 candidate counts under a 1000-entry set, want 960",
			moved)
	}
}

// TestProbeIsCheaper_WillNotMaterialiseMoreThanTheCap: the footprint half. A
// probe holds nothing; a build holds setSize ids. Past the cap the probe wins on
// that ground alone, whatever the cost comparison says.
func TestProbeIsCheaper_WillNotMaterialiseMoreThanTheCap(t *testing.T) {
	// A candidate set far larger than the residual set, which is the one shape
	// where the cost comparison says build regardless of probeCost.
	const huge = residualBuildCap * 8
	if !probeIsCheaper(huge, residualBuildCap+1, 1) {
		t.Errorf("a %d-id set is built rather than probed; the cap does not hold",
			residualBuildCap+1)
	}
	if !probeIsCheaper(huge, residualBuildCap+1, 25) {
		t.Error("the cap must not depend on probeCost")
	}
	// Exactly at the cap is still the cost comparison's decision, so the bound is
	// on what is materialised and not one id below it.
	if probeIsCheaper(huge, residualBuildCap, 1) {
		t.Errorf("a set of exactly %d ids was refused; the cap is off by one", residualBuildCap)
	}
	// And the cap must be far enough above the interesting sizes that it decides
	// nothing the comparison was deciding on the merits.
	if residualBuildCap < 1<<20 {
		t.Errorf("residualBuildCap = %d, low enough to override ordinary planning", residualBuildCap)
	}
}

// TestProbeIsCheaper_ANegativeCandidateCountDoesNotWiden: candidateCount arrives
// from a caller through PlanNodeResiduals. Widened to unsigned, a negative one
// would become an enormous product and forecast a build for every filter — the
// opposite of what it used to do.
func TestProbeIsCheaper_ANegativeCandidateCountDoesNotWiden(t *testing.T) {
	for _, probeCost := range []int{1, 25} {
		if !probeIsCheaper(-1, 1, probeCost) {
			t.Errorf("probeIsCheaper(-1, 1, %d) = false; a nonsense count flipped the forecast",
				probeCost)
		}
		if probeIsCheaper(-1, 0, probeCost) {
			t.Errorf("probeIsCheaper(-1, 0, %d) = true; nothing is cheaper than nothing",
				probeCost)
		}
	}
}

// narrowCorpus is one key every node carries and one that half of them do, sized
// so that a three-candidate residual over the wide key is a probe at one unit per
// candidate and a build at fourteen.
func narrowCorpus(nodes int) []triple {
	out := make([]triple, 0, nodes*2)
	for i := 1; i <= nodes; i++ {
		out = append(out, triple{NodeKind, uint64(i), "name", []byte(fmt.Sprintf("n%04d", i))})
		if i%2 == 0 {
			out = append(out, triple{NodeKind, uint64(i), "half", []byte("h")})
		}
	}
	return out
}

// TestNarrow_TheRouteChangesAndTheAnswerDoesNot: the point of the whole item is
// that a decision moves. The point of the engine is that moving it is invisible.
//
// Two indexes over the same corpus, differing only in what their bases claim to
// hold, so the same query takes the probe through one and the build through the
// other. The answers must be identical, element for element — and the routes must
// actually have differed, or this passes by measuring one loop twice.
func TestNarrow_TheRouteChangesAndTheAnswerDoesNot(t *testing.T) {
	const nodes = 40
	filters := []store.PropertyFilter{
		{Key: "half", Op: store.PropertyOpEqual, Value: []byte("h")},
		{Key: "name", Op: store.PropertyOpContains, Value: []byte("2")},
	}
	skip := store.FilterMask(0).Set(0)
	candidates := []store.NodeID{2, 12, 22}

	answers := map[string][]store.NodeID{}
	routes := map[string]bool{}
	walked := map[string]int{}
	for _, arm := range []struct {
		name    string
		entries int
	}{
		{"small base, so a probe", 0},
		{"large base, so a build", 28_000_000},
	} {
		p := NewPropertyIndex()
		b := newFakeBase(narrowCorpus(nodes))
		if arm.entries > 0 {
			b.sides[NodeKind].entries = arm.entries
		}
		if err := p.AttachBase(b); err != nil {
			t.Fatalf("AttachBase: %v", err)
		}

		plan := p.PlanNodeResiduals(filters, skip, len(candidates))
		if len(plan) != 1 {
			t.Fatalf("%s: %d residual steps, want 1", arm.name, len(plan))
		}
		routes[arm.name] = plan[0].Probe

		// The forecast above is what ExplainNodeQuery reports; this is what the
		// executor did. They are two call sites of the same comparison and a
		// change that reached one and not the other would leave every assertion
		// in this file passing, because the answer is the same either way.
		// ForEachValue is the forward direction, so a walk of it means the set
		// was built and no walk means every candidate was probed.
		before := b.valueWalks()
		got, err := p.NarrowNodesByFiltersCtx(t.Context(),
			append([]store.NodeID(nil), candidates...), filters, skip)
		if err != nil {
			t.Fatalf("%s: NarrowNodesByFiltersCtx: %v", arm.name, err)
		}
		answers[arm.name] = got
		walked[arm.name] = b.valueWalks() - before
	}

	if routes["small base, so a probe"] != true || routes["large base, so a build"] != false {
		t.Fatalf("the two arms did not forecast different routes: %v", routes)
	}
	if walked["small base, so a probe"] != 0 {
		t.Errorf("the probing arm walked the forward direction %d times; it built the set",
			walked["small base, so a probe"])
	}
	if walked["large base, so a build"] == 0 {
		t.Error("the building arm never walked the forward direction; it probed, " +
			"so the executor and the forecast disagree about the route")
	}

	probed := answers["small base, so a probe"]
	built := answers["large base, so a build"]
	// n0002, n0012, n0022 all contain "2", so all three survive; the assertion
	// that matters is that the two routes agree, and the literal is there so a
	// change making both of them wrong in the same way still fails.
	want := []store.NodeID{2, 12, 22}
	for _, arm := range []struct {
		name string
		got  []store.NodeID
	}{{"probe", probed}, {"build", built}} {
		if len(arm.got) != len(want) {
			t.Fatalf("%s route returned %v, want %v", arm.name, arm.got, want)
		}
		for i := range want {
			if arm.got[i] != want[i] {
				t.Fatalf("%s route returned %v, want %v", arm.name, arm.got, want)
			}
		}
	}
}

// TestNarrowEdges_TheRouteChangesAndTheAnswerDoesNot is the node test for the
// other kind.
//
// Written out rather than folded into a loop over kinds, because the thing being
// tested is that there are two executors and both of them consult the cost. The
// edge loop is a copy of the node loop with the types changed, which is exactly
// the shape where a change reaches one copy and not the other — and the answer is
// identical either way, so nothing else in this package would notice.
func TestNarrowEdges_TheRouteChangesAndTheAnswerDoesNot(t *testing.T) {
	const edges = 40
	filters := []store.PropertyFilter{
		{Key: "half", Op: store.PropertyOpEqual, Value: []byte("h")},
		{Key: "name", Op: store.PropertyOpContains, Value: []byte("2")},
	}
	skip := store.FilterMask(0).Set(0)
	candidates := []store.EdgeID{2, 12, 22}
	want := []store.EdgeID{2, 12, 22}

	probed, built := false, false
	for _, arm := range []struct {
		name    string
		entries int
	}{
		{"small base, so a probe", 0},
		{"large base, so a build", 28_000_000},
	} {
		p := NewPropertyIndex()
		b := newFakeBase(narrowEdgeCorpus(edges))
		if arm.entries > 0 {
			b.sides[EdgeKind].entries = arm.entries
		}
		if err := p.AttachBase(b); err != nil {
			t.Fatalf("AttachBase: %v", err)
		}

		plan := p.PlanEdgeResiduals(filters, skip, len(candidates))
		if len(plan) != 1 {
			t.Fatalf("%s: %d residual steps, want 1", arm.name, len(plan))
		}

		before := b.valueWalks()
		got, err := p.NarrowEdgesByFiltersCtx(t.Context(),
			append([]store.EdgeID(nil), candidates...), filters, skip)
		if err != nil {
			t.Fatalf("%s: NarrowEdgesByFiltersCtx: %v", arm.name, err)
		}
		walks := b.valueWalks() - before

		if plan[0].Probe != (walks == 0) {
			t.Errorf("%s: the forecast says probe=%v and the executor walked the forward "+
				"direction %d times; the two call sites disagree",
				arm.name, plan[0].Probe, walks)
		}
		if plan[0].Probe {
			probed = true
		} else {
			built = true
		}

		if len(got) != len(want) {
			t.Fatalf("%s: NarrowEdgesByFiltersCtx = %v, want %v", arm.name, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: NarrowEdgesByFiltersCtx = %v, want %v", arm.name, got, want)
			}
		}
	}

	if !probed || !built {
		t.Errorf("the two arms took the same route (probed %v, built %v); "+
			"this tests one loop twice", probed, built)
	}
}

// narrowEdgeCorpus is narrowCorpus for edges.
func narrowEdgeCorpus(edges int) []triple {
	out := make([]triple, 0, edges*2)
	for i := 1; i <= edges; i++ {
		out = append(out, triple{EdgeKind, uint64(i), "name", []byte(fmt.Sprintf("n%04d", i))})
		if i%2 == 0 {
			out = append(out, triple{EdgeKind, uint64(i), "half", []byte("h")})
		}
	}
	return out
}

// TestPlanResiduals_TheOrderDoesNotFollowTheProbeCost: the cost model decides how
// a filter is applied, not when. Ordering is by estimated match count so that the
// candidate set empties as early as possible, and weighting the probe must not
// have reached it.
func TestPlanResiduals_TheOrderDoesNotFollowTheProbeCost(t *testing.T) {
	corpus := []triple{
		{NodeKind, 1, "wide", []byte("a")},
		{NodeKind, 2, "wide", []byte("b")},
		{NodeKind, 3, "wide", []byte("c")},
		{NodeKind, 1, "narrow", []byte("x")},
	}
	filters := []store.PropertyFilter{
		{Key: "drive", Op: store.PropertyOpEqual, Value: []byte("d")},
		{Key: "wide", Op: store.PropertyOpContains, Value: []byte("a")},
		{Key: "narrow", Op: store.PropertyOpContains, Value: []byte("x")},
	}
	skip := store.FilterMask(0).Set(0)

	for _, entries := range []int{0, 28_000_000} {
		p := NewPropertyIndex()
		b := newFakeBase(corpus)
		if entries > 0 {
			b.sides[NodeKind].entries = entries
		}
		if err := p.AttachBase(b); err != nil {
			t.Fatalf("AttachBase: %v", err)
		}
		plan := p.PlanNodeResiduals(filters, skip, 1)
		if len(plan) != 2 {
			t.Fatalf("%d residual steps, want 2", len(plan))
		}
		if plan[0].Key != "narrow" || plan[1].Key != "wide" {
			t.Fatalf("residuals ordered %q then %q; the selective one must run first",
				plan[0].Key, plan[1].Key)
		}
	}
}
