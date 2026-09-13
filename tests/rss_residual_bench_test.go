//go:build stress

package graphene_test

// What a residual route costs against a mapped index.
//
// The decision these measure is made by probeIsCheaper, and the two arms of the
// A/B are two trees rather than two sub-benchmarks: the planner picks one route
// or the other, so the only way to compare them is to compare the cost model that
// picks. The fixture is sized to sit inside the band the weighting moved — a
// candidate set large enough that probing it is many searches, against a residual
// set small enough that walking it is a short sweep.
//
// The pair matters because they measure opposite things. The timing arm is warm,
// and warm is where the probe looks good: its reads are random but they are in
// cache, and the build it is weighed against allocates a slice and sorts it. The
// page arm is what the cost model is actually about — how much of the image each
// route drags into the working set, which is the same figure as how many reads it
// would be on an image that has not been read yet.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Sized so that probing the candidates is worse than sweeping the residual key
// under a mapped base and better under a resident one, which is the band the
// weighting moved. See residualRouteFixture for the arithmetic.
const (
	residualNodes      = 200_000
	residualGroups     = 100
	residualTagged     = 20_000
	residualDriveGroup = "050"
)

// residualRouteFixture builds a compacted store and returns its directory.
//
// The shape: every node carries grp, one of a hundred values, so a driver on one
// of them yields two thousand candidates. The first twenty thousand carry tag,
// all distinct, so a residual Contains over it is estimated at twenty thousand
// entries. Two hundred and twenty thousand entries in the reverse section makes a
// probe eighteen reads, so the two routes are thirty-six thousand reads against
// twenty thousand — the probe is the dearer one, and before this cost model
// existed it was the one chosen.
func residualRouteFixture(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	nodes := make([]*store.Node, residualNodes)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperty(id, "grp", []byte(fmt.Sprintf("%03d", i%residualGroups))); err != nil {
			b.Fatalf("IndexNodeProperty grp: %v", err)
		}
		if i < residualTagged {
			if err := g.IndexNodeProperty(id, "tag", []byte(fmt.Sprintf("tag-%09d", i))); err != nil {
				b.Fatalf("IndexNodeProperty tag: %v", err)
			}
		}
	}
	if err := g.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	return dir
}

// residualQuery drives from one group and leaves a Contains over the tag key as
// the residual. Contains is deliberately unserveable by any index, so the build
// route is the sequential walk the cost model is weighing against.
func residualQuery() store.NodeQuery {
	return store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "grp", Op: store.PropertyOpEqual, Value: []byte(residualDriveGroup)},
			{Key: "tag", Op: store.PropertyOpContains, Value: []byte("7")},
		},
		FilterMode: store.MatchAll,
	}
}

// plannedRoute is what the planner decided, carried out of the loop so it can
// be reported after ResetTimer rather than dropped by it.
type plannedRoute struct {
	probe      bool
	candidates int
	entries    int
}

// plannedResidualRoute asks the planner which route it takes, so an A/B across
// two trees cannot silently compare an arm against itself.
func plannedResidualRoute(b *testing.B, g *graphene.Graph) plannedRoute {
	b.Helper()
	plan, err := g.ExplainNodeQuery(residualQuery())
	if err != nil {
		b.Fatalf("ExplainNodeQuery: %v", err)
	}
	if len(plan.Residuals) != 1 {
		b.Fatalf("%d residual steps, want 1", len(plan.Residuals))
	}
	return plannedRoute{
		probe:      plan.Residuals[0].Probe,
		candidates: plan.Candidates,
		entries:    plan.Residuals[0].Cost,
	}
}

// reportRoute attaches the decision to the results. ReportMetric only after
// ResetTimer: it clears every metric reported before it, and a route silently
// dropped from an A/B is the one thing these benchmarks exist to show.
func reportRoute(b *testing.B, r plannedRoute) {
	b.Helper()
	probe := 0.0
	if r.probe {
		probe = 1
	}
	b.ReportMetric(probe, "probe")
	b.ReportMetric(float64(r.candidates), "candidates")
	b.ReportMetric(float64(r.entries), "residual_entries")
}

// BenchmarkResidual_WideCandidates_Disk is the warm arm: the same query answered
// over and over against an image the process has already read.
func BenchmarkResidual_WideCandidates_Disk(b *testing.B) {
	dir := residualRouteFixture(b)
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()

	q := residualQuery()
	got, err := g.QueryNodeIDs(q)
	if err != nil {
		b.Fatal(err)
	}
	if len(got) == 0 {
		b.Fatal("the query matches nothing; the fixture has stopped exercising the residual pass")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ids, err := g.QueryNodeIDs(q)
		if err != nil {
			b.Fatal(err)
		}
		if len(ids) != len(got) {
			b.Fatalf("the query returned %d ids and then %d", len(got), len(ids))
		}
	}
	// After the loop, because ResetTimer drops every metric reported before it.
	b.StopTimer()
	reportRoute(b, plannedResidualRoute(b, g))
}

// BenchmarkRSS_ResidualRoutePages is the arm the cost model is about. Run it with
// -benchtime 1x: it measures one pass over an image this process has not read.
//
// The figure is how much of the mapped image the residual pass pulls into the
// working set. On an image already in the page cache that is a cheap fault; on
// one that is not it is a read of a four-kilobyte page from the disk, and the
// count is the same either way. It is the only proxy available here for the cold
// cost, because there is no way to drop the page cache from a Go test on Windows.
func BenchmarkRSS_ResidualRoutePages(b *testing.B) {
	dir := residualRouteFixture(b)

	var opened, after rssSample
	var ids []store.NodeID
	var route plannedRoute
	for i := 0; i < b.N; i++ {
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		// Settled with the store open and before the query, so the delta below is
		// what the residual pass touched and not what Open did.
		o, _ := settledRSS(g)
		out, err := g.QueryNodeIDs(residualQuery())
		if err != nil {
			b.Fatal(err)
		}
		a, _ := settledRSS(g)
		if i == 0 {
			opened, after, ids = o, a, out
			route = plannedResidualRoute(b, g)
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}
	reportRoute(b, route)

	if len(ids) == 0 {
		b.Fatal("the query matches nothing; the fixture has stopped exercising the residual pass")
	}
	b.ReportMetric(float64(len(ids)), "matched")
	b.ReportMetric(float64(after.Total)/bytesPerMiB, "afterMiB")
	if after.Total > opened.Total {
		b.ReportMetric(float64(after.Total-opened.Total)/bytesPerMiB, "deltaMiB")
	}
	if after.Split {
		reportResidualDelta(b, "anon", opened.Anon, after.Anon)
		reportResidualDelta(b, "file", opened.File, after.File)
	}
}

// reportResidualDelta reports one residency class either way round, because the
// interesting number here can be negative: a route that allocates and frees can
// leave the process holding less than it held before.
func reportResidualDelta(b *testing.B, class string, before, after uint64) {
	b.Helper()
	d := (float64(after) - float64(before)) / bytesPerMiB
	b.ReportMetric(d, class+"DeltaMiB")
	b.ReportMetric(float64(after)/bytesPerMiB, class+"AfterMiB")
}
