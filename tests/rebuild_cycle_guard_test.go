package graphene_test

// The rebuild-cycle guard: what a burned identifier costs, measured rather than
// argued.
//
// A store that rebuilds a derived layer deletes every record of a type and
// writes the same number again. Identifiers are never reused, so each cycle
// leaves its predecessors' identifiers behind at the *low* end of the space,
// where compaction recovers nothing — and while the CSR held one record slot per
// identifier ever issued, that cost 152 B of heap per burned identifier for as
// long as the store existed (docs/benchmarks.md, "The identifier high-water
// mark": 72 B of node slot plus 80 B of edge slot). Ten cycles of a 50,000-node
// layer added roughly 119 MiB permanently, and nothing an operator could do
// short of exporting into a fresh store gave it back.
//
// Since records live in pages, a cycle that burns a whole page's worth of
// identifiers leaves an int32 behind instead of 4096 slots. This guard fails if
// that stops being true.
//
// It is a slope, not a ceiling, for the same reason TestFootprintGuard_OpenSlope
// is: the fixed cost of the fixture cancels, and what remains is the marginal
// cost of one more burned identifier. Untagged, so `make check` runs it.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

const (
	// A live set small enough to keep the test under a couple of seconds, and
	// enough cycles that the burned identifiers outnumber the live ones by an
	// order of magnitude — which is the condition that separates a cost per
	// identifier from a cost per record.
	rebuildGuardLive   = 2_000
	rebuildGuardCycles = 12
)

// rebuildCycleWrite writes n nodes with one indexed property each and returns
// their identifiers.
func rebuildCycleWrite(t *testing.T, g *graphene.Graph, gen, n int) []store.NodeID {
	t.Helper()
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("generation %d: AddNodes: %v", gen, err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(fmt.Sprintf("hash-%03d-%07d", gen, i)),
		}); err != nil {
			t.Fatalf("generation %d: IndexNodeProperties: %v", gen, err)
		}
	}
	return ids
}

// TestRebuildCycle_ResidentIsProportionalToLive fails when repeated rebuilds
// start costing memory again.
func TestRebuildCycle_ResidentIsProportionalToLive(t *testing.T) {
	base := retainedHeap(nil)

	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })

	live := rebuildCycleWrite(t, g, 0, rebuildGuardLive)
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}

	// The reading after the first cycle, not the zeroth: the first rebuild pays
	// whatever one-off cost the shape has, and including it would price that
	// one-off as if it recurred.
	var first, last uint64
	for cycle := 1; cycle <= rebuildGuardCycles; cycle++ {
		for _, id := range live {
			if err := g.DeleteNode(id); err != nil {
				t.Fatalf("cycle %d: DeleteNode(%d): %v", cycle, id, err)
			}
		}
		live = rebuildCycleWrite(t, g, cycle, rebuildGuardLive)
		if err := g.Compact(); err != nil {
			t.Fatalf("cycle %d: Compact: %v", cycle, err)
		}
		if cycle == 1 {
			first = retainedHeap(g)
		}
	}
	last = retainedHeap(g)

	// The live set is identical at both readings, so everything the difference
	// contains is a consequence of identifiers that no longer name anything.
	burned := uint64(rebuildGuardLive * (rebuildGuardCycles - 1))
	if last <= first {
		t.Logf("retained heap did not grow across %d cycles (%d B then %d B); nothing to price",
			rebuildGuardCycles-1, first, last)
		return
	}
	perID := float64(last-first) / float64(burned)
	t.Logf("retained %d B after cycle 1, %d B after cycle %d (base %d): %.1f B per burned identifier",
		first, last, rebuildGuardCycles, base, perID)

	// Before the page table this measured 152 B per burned identifier on the RSS
	// instrument and decomposed exactly as 72 B of node slot plus 80 B of edge
	// slot. The ceiling here is deliberately far below that and far above what a
	// directory entry costs (4 B per 4096 identifiers, so ~0.001 B each): a
	// regression to per-identifier storage overshoots it by two orders of
	// magnitude, while GC timing on a heap this size does not come close.
	const ceiling = 24.0
	if perID > ceiling {
		t.Errorf("%.1f B retained per burned identifier, ceiling %.0f B\n"+
			"a rebuild workload burns identifiers at the low end of the space, where "+
			"compaction recovers nothing — this is the cost that made repeated rebuilds "+
			"unbounded, and it is supposed to be paid per page rather than per identifier",
			perID, ceiling)
	}
}
