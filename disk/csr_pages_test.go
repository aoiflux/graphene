package disk

// The page table's own tests: what a dead page does, what a duplicate does, and
// that a walk over pages is the same walk the file and the Merkle leaves assume.
//
// The fixtures here deliberately span more than one page (csrPageSlots = 4096
// identifiers), because every other fixture in this package is small enough to
// fit in page zero, where a paged layout and a flat one are indistinguishable.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// pagedFixture returns nodes at the given IDs, each with one label, and a chain
// of edges numbered from 1 joining consecutive nodes.
func pagedFixture(t *testing.T, ids ...store.NodeID) *CSRGraph {
	t.Helper()
	nodes := make([]nodeRecord, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, nodeRecord{
			ID:         id,
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("n%d", id)),
		})
	}
	edges := make([]rawEdge, 0, len(ids))
	for i := 1; i < len(ids); i++ {
		edges = append(edges, rawEdge{
			ID: store.EdgeID(i), Src: ids[i-1], Dst: ids[i],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	g, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return g
}

// An identifier whose page holds no record is a miss, not a phantom record and
// not an out-of-range panic. This is the case the flat layout never had: there,
// every identifier below the maximum had a slot of its own.
func TestCSRPages_GetNodeOnDeadPageIsMiss(t *testing.T) {
	g := pagedFixture(t, 1, 3, 1<<20)

	for _, id := range []store.NodeID{1, 3, 1 << 20} {
		if n, ok := g.GetNode(id); !ok || n.ID != id {
			t.Fatalf("GetNode(%d) missed a record that is present", id)
		}
	}

	// Inside the directory, on a page nothing occupies.
	for _, id := range []store.NodeID{csrPageSlots, 5000, 1<<20 - 1} {
		if _, ok := g.GetNode(id); ok {
			t.Fatalf("GetNode(%d) hit on a dead page", id)
		}
		if ids := g.OutboundEdgeIDs(id); ids != nil {
			t.Fatalf("OutboundEdgeIDs(%d) returned %v on a dead page", id, ids)
		}
		if d := g.OutDegree(id); d != 0 {
			t.Fatalf("OutDegree(%d) = %d on a dead page", id, d)
		}
	}

	// Live page, dead slot: the record there is the zero value and must not read
	// as an entity.
	if _, ok := g.GetNode(2); ok {
		t.Fatal("GetNode(2) hit a dead slot inside a live page")
	}

	// Beyond the directory entirely.
	if _, ok := g.GetNode(1 << 40); ok {
		t.Fatal("GetNode(1<<40) hit beyond the directory")
	}

	// And the invalid zero, whose slot exists and whose record is zero-valued.
	if _, ok := g.GetNode(store.InvalidNodeID); ok {
		t.Fatal("the invalid zero identifier reads as a node that exists")
	}
}

// Adjacency is one CSR over the whole slot space, so the range of the last slot
// of a page is closed by the first slot of the next one. A dead page in between
// must not break that chain, and a dead slot must be an empty range rather than
// a borrowed one.
func TestCSRPages_AdjacencySentinelsSpanDeadPages(t *testing.T) {
	a, b, c := store.NodeID(1), store.NodeID(csrPageSlots*3+7), store.NodeID(csrPageSlots*9+1)
	g := pagedFixture(t, a, b, c)

	if err := g.verifyAdjacency(); err != nil {
		t.Fatalf("verifyAdjacency: %v", err)
	}

	if got := g.OutDegree(a); got != 1 {
		t.Fatalf("OutDegree(%d) = %d, want 1", a, got)
	}
	if got := g.InDegree(b); got != 1 {
		t.Fatalf("InDegree(%d) = %d, want 1", b, got)
	}
	if got, want := g.OutDegree(b), 1; got != want {
		t.Fatalf("OutDegree(%d) = %d, want %d", b, got, want)
	}
	if got := g.OutDegree(c); got != 0 {
		t.Fatalf("OutDegree(%d) = %d, want 0 for the last node in the chain", c, got)
	}

	// The last slot of the last materialised page: its end offset is the global
	// sentinel, which is the entry the flat layout had to size maxID+2 to get.
	last := len(g.nodeRecs) - 1
	if int(g.outOffset[last+1]) != len(g.outEdges) {
		t.Fatalf("the sentinel after the last slot is %d, want %d",
			g.outOffset[last+1], len(g.outEdges))
	}

	edges, err := g.OutboundEdges(a)
	if err != nil {
		t.Fatalf("OutboundEdges(%d): %v", a, err)
	}
	if len(edges) != 1 || edges[0].Dst != b {
		t.Fatalf("OutboundEdges(%d) = %+v", a, edges)
	}
}

// Two records claiming one identifier is a file lying about itself, or a
// compaction plan that broke its disjointness rule. The flat layout kept the
// last one and said nothing.
func TestBuild_RejectsDuplicateID(t *testing.T) {
	nodes := []nodeRecord{{ID: 1}, {ID: 7}, {ID: 1}}
	if _, err := Build(nodes, nil); err == nil {
		t.Fatal("Build accepted two node records with identifier 1")
	}

	edges := []rawEdge{{ID: 2, Src: 1, Dst: 1}, {ID: 2, Src: 1, Dst: 1}}
	if _, err := Build([]nodeRecord{{ID: 1}}, edges); err == nil {
		t.Fatal("Build accepted two edge records with identifier 2")
	}

	// A record carrying the invalid zero is skipped, not refused: the flat
	// layout dropped it into the unused slot zero where no walk ever saw it, and
	// files that carry one open today.
	g, err := Build([]nodeRecord{{ID: 0}, {ID: 1}, {ID: 0}}, nil)
	if err != nil {
		t.Fatalf("Build refused records carrying the invalid zero: %v", err)
	}
	if g.NodeCount() != 1 {
		t.Fatalf("NodeCount = %d, want 1: the zero records should be skipped", g.NodeCount())
	}
}

// The relative bound is on the pages a file touches, not on its highest
// identifier. A long-lived store's identifiers are sparse and its pages are not;
// a hostile file's pages are exactly as many as its records.
func TestCheckIDCeiling_CountsTouchedPagesNotMaxID(t *testing.T) {
	// One record naming an identifier just under the ceiling: one page, legal.
	// Under the flat layout this was the canonical rejection.
	if err := checkIDCeiling("node", maxCSREntityID-1, 1, 1); err != nil {
		t.Fatalf("one record on one page was rejected: %v", err)
	}

	// Seventeen records on seventeen pages: 69,632 slots against an allowance of
	// max(65,536, 17*256) = 65,536.
	if err := checkIDCeiling("node", 17<<csrPageBits, 17, 17); err == nil {
		t.Fatal("seventeen records spread one per page were accepted")
	}

	// The same seventeen records inside one page are fine.
	if err := checkIDCeiling("node", 17, 17, 1); err != nil {
		t.Fatalf("seventeen dense records were rejected: %v", err)
	}

	// Above the absolute ceiling nothing else is consulted.
	if err := checkIDCeiling("node", maxCSREntityID+1, 1_000_000, 1); err == nil {
		t.Fatal("an identifier above the absolute ceiling was accepted")
	}
}

// Endpoint pages count against the sparsity rule, or a file could spend two
// records and a handful of edges to make Build materialise a page per endpoint.
// Every endpoint here is inside the identifier space the records already
// describe, so nothing but the page count stands between the file and the
// memory it is asking for.
func TestCheckCSREntityIDs_CountsEndpointPages(t *testing.T) {
	top := store.NodeID(20 << csrPageBits)
	nodes := []nodeRecord{{ID: 1}, {ID: top}}
	edges := make([]rawEdge, 0, 20)
	for i := 1; i <= 20; i++ {
		edges = append(edges, rawEdge{ID: store.EdgeID(i), Src: 1, Dst: store.NodeID(i << csrPageBits)})
	}

	if err := checkCSREntityIDs(nodes, edges, csrVersionCurrent, 0, 0); err == nil {
		t.Fatal("two records with endpoints on twenty pages were accepted")
	} else if !strings.Contains(err.Error(), "too sparse") {
		t.Fatalf("rejected for the wrong reason: %v", err)
	}

	// The same edges pointing inside one page are fine: it is the pages that
	// cost, not the edges.
	near := make([]rawEdge, 0, 20)
	for i := 1; i <= 20; i++ {
		near = append(near, rawEdge{ID: store.EdgeID(i), Src: 1, Dst: store.NodeID(i)})
	}
	if err := checkCSREntityIDs([]nodeRecord{{ID: 1}, {ID: 20}}, near, csrVersionCurrent, 0, 0); err != nil {
		t.Fatalf("dense endpoints were rejected: %v", err)
	}
}

// An edge may name an endpoint no node record occupies — a plan that broke its
// own cascade, or a file written by something that did. The page is materialised
// so the adjacency has somewhere to live; the node still does not exist.
func TestCSRPages_EndpointPageIsMaterialised(t *testing.T) {
	orphan := store.NodeID(csrPageSlots * 5)
	g, err := Build(
		[]nodeRecord{{ID: 1}},
		[]rawEdge{{ID: 1, Src: 1, Dst: orphan}},
	)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if _, ok := g.GetNode(orphan); ok {
		t.Fatal("an endpoint with no record reads as a node that exists")
	}
	if got := g.InDegree(orphan); got != 1 {
		t.Fatalf("InDegree(%d) = %d, want 1: the endpoint's adjacency is what the page is for", orphan, got)
	}
	if err := g.verifyAdjacency(); err != nil {
		t.Fatalf("verifyAdjacency: %v", err)
	}
	if g.nodePageCount() != 2 {
		t.Fatalf("nodePageCount = %d, want 2 (the record's page and the endpoint's)", g.nodePageCount())
	}
}

// Every walk that decides the file's bytes or the Merkle leaves has to yield
// live records in ascending identifier order, across page boundaries and past
// dead pages.
func TestCSRPages_WalkOrderIsAscendingAcrossPages(t *testing.T) {
	ids := []store.NodeID{1, 2, csrPageSlots, csrPageSlots + 1, csrPageSlots * 7, csrPageSlots*7 + 4095}
	g := pagedFixture(t, ids...)

	var seen []store.NodeID
	for n := range g.Nodes() {
		seen = append(seen, n.ID)
	}
	if len(seen) != len(ids) {
		t.Fatalf("Nodes() yielded %d records, want %d", len(seen), len(ids))
	}
	for i, id := range ids {
		if seen[i] != id {
			t.Fatalf("Nodes() yielded %v, want %v", seen, ids)
		}
	}

	var byID []store.NodeID
	for id := range g.NodeIDs() {
		byID = append(byID, id)
	}
	for i, id := range ids {
		if byID[i] != id {
			t.Fatalf("NodeIDs() yielded %v, want %v", byID, ids)
		}
	}

	// The cursor form, which a scan resumes through across a lock release.
	c := g.nodeIDCursor()
	for i := 0; ; i++ {
		id, ok := c.next()
		if !ok {
			if i != len(ids) {
				t.Fatalf("cursor stopped after %d of %d records", i, len(ids))
			}
			break
		}
		if id != ids[i] {
			t.Fatalf("cursor yielded %d at %d, want %d", id, i, ids[i])
		}
	}

	// An exhausted cursor stays exhausted, and the zero cursor is exhausted.
	if _, ok := c.next(); ok {
		t.Fatal("an exhausted cursor produced another identifier")
	}
	var nilGraph *CSRGraph
	zero := nilGraph.nodeIDCursor()
	if _, ok := zero.next(); ok {
		t.Fatal("a cursor over no image produced an identifier")
	}
}

// A walk must not allocate per page. The other alloc guards run on a fixture
// that fits in one page, where a per-page allocation is invisible.
func TestCSRPages_WalkDoesNotAllocatePerPage(t *testing.T) {
	ids := make([]store.NodeID, 0, 40)
	for i := 0; i < 40; i++ {
		ids = append(ids, store.NodeID(i*csrPageSlots+1))
	}
	g := pagedFixture(t, ids...)

	if got := testing.AllocsPerRun(50, func() {
		n := 0
		for range g.NodeIDs() {
			n++
		}
		if n != len(ids) {
			t.Fatalf("walked %d records, want %d", n, len(ids))
		}
	}); got != 0 {
		t.Fatalf("a walk over %d pages allocated %.0f times", g.nodePageCount(), got)
	}
}

// A proof is built at the leaf's index, so the index has to be the record's rank
// among the live ones — the same rank NodeLeaves produces, counted across dead
// pages and dead slots.
func TestSnapshot_LeafIndexAcrossDeadPages(t *testing.T) {
	ids := []store.NodeID{3, 9, csrPageSlots + 1, csrPageSlots * 4, csrPageSlots*4 + 2, csrPageSlots * 11}
	g := pagedFixture(t, ids...)

	leaves := g.NodeLeaves(snapshotBodyV1)
	if len(leaves) != len(ids) {
		t.Fatalf("NodeLeaves returned %d leaves for %d records", len(leaves), len(ids))
	}
	for want, id := range ids {
		got, ok := g.nodeLeafIndex(id)
		if !ok {
			t.Fatalf("nodeLeafIndex(%d) reported the record absent", id)
		}
		if got != want {
			t.Fatalf("nodeLeafIndex(%d) = %d, want %d", id, got, want)
		}
	}

	// Absent identifiers: a dead slot, a dead page, beyond the directory, zero.
	for _, id := range []store.NodeID{4, csrPageSlots * 2, 1 << 30, store.InvalidNodeID} {
		if _, ok := g.nodeLeafIndex(id); ok {
			t.Fatalf("nodeLeafIndex(%d) reported a record that is not there", id)
		}
	}

	// Edges take the same route.
	for want := 0; want < len(ids)-1; want++ {
		got, ok := g.edgeLeafIndex(store.EdgeID(want + 1))
		if !ok {
			t.Fatalf("edgeLeafIndex(%d) reported the record absent", want+1)
		}
		if got != want {
			t.Fatalf("edgeLeafIndex(%d) = %d, want %d", want+1, got, want)
		}
	}
}

// Counts are maintained rather than walked, so they have to agree with the walk
// on every input the walk tolerates.
func TestCSRPages_CountsAgreeWithTheWalk(t *testing.T) {
	g := pagedFixture(t, 1, csrPageSlots+3, csrPageSlots*6+9)

	walked := 0
	for range g.Nodes() {
		walked++
	}
	if g.NodeCount() != walked {
		t.Fatalf("NodeCount = %d, walk found %d", g.NodeCount(), walked)
	}

	walked = 0
	for range g.Edges() {
		walked++
	}
	if g.EdgeCount() != walked {
		t.Fatalf("EdgeCount = %d, walk found %d", g.EdgeCount(), walked)
	}

	if got, want := g.HighestNodeID(), store.NodeID(csrPageSlots*6+9); got != want {
		t.Fatalf("HighestNodeID = %d, want %d", got, want)
	}
	if got, want := g.HighestEdgeID(), store.EdgeID(2); got != want {
		t.Fatalf("HighestEdgeID = %d, want %d", got, want)
	}

	empty, err := Build(nil, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if empty.NodeCount() != 0 || empty.EdgeCount() != 0 || empty.HighestNodeID() != 0 {
		t.Fatal("an empty graph reports records it does not have")
	}
	// A store compacted with nothing in it is verifiable like any other: the
	// offset arrays carry their sentinel over zero slots.
	if err := empty.verifyAdjacency(); err != nil {
		t.Fatalf("verifyAdjacency on an empty graph: %v", err)
	}
	if err := empty.verifyLabelIndex(); err != nil {
		t.Fatalf("verifyLabelIndex on an empty graph: %v", err)
	}
	if _, ok := empty.GetNode(1); ok {
		t.Fatal("an empty graph answered a lookup")
	}
}
