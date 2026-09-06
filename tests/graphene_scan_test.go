package graphene_test

// Iteration.
//
// store.Scanner is offered over a Snapshot and promises three things: the same
// set the equivalent slice-returning read gives, in ascending order, on both
// backends. The set is what a consumer's correctness rests on; the order is
// what lets a scan replace a query without changing what the consumer writes;
// and the parity is what stops a caller's assumption depending on which backend
// answered.
//
// The disk implementation is the one with something to get wrong. It collects
// batches under the store lock and yields them outside it, merging a map-ordered
// delta half against an indexed image half, so it has a resumption point the
// slice-returning read does not — and a scan that lost its place would drop
// records rather than fail.

import (
	"bytes"
	"slices"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/store"
)

// scanFixture builds a graph large enough to cross the disk scan's batch
// boundary (512) several times, with deletes and mixed labels so the postings
// and the records disagree.
func scanFixture(t *testing.T, g *graphene.Graph) {
	t.Helper()
	const n = 1300

	ids := make([]store.NodeID, 0, n)
	for i := range n {
		labels := []store.NodeType{store.NodeTypeMicroArtefact}
		switch {
		case i%11 == 0:
			labels = []store.NodeType{store.NodeTypeCase, store.NodeTypeTag}
		case i%4 == 0:
			labels = []store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact}
		}
		id, err := g.AddNode(&store.Node{Labels: labels})
		if err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i += 3 {
		if _, err := g.AddEdge(&store.Edge{
			Src:    ids[i-1],
			Dst:    ids[i],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		}); err != nil {
			t.Fatalf("AddEdge %d: %v", i, err)
		}
	}
	for i := 0; i < len(ids); i += 17 {
		if err := g.DeleteNode(ids[i]); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
	}
	for i := 1; i < len(ids); i += 23 {
		if err := g.IndexNodeProperties(ids[i], map[string][]byte{
			"sha256": []byte("hash"),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}
}

// drain collects a sequence, failing on the first error it reports.
func drainNodes(t *testing.T, seq func(func(store.NodeID, error) bool)) []store.NodeID {
	t.Helper()
	var out []store.NodeID
	for id, err := range seq {
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, id)
	}
	return out
}

func drainEdges(t *testing.T, seq func(func(store.EdgeID, error) bool)) []store.EdgeID {
	t.Helper()
	var out []store.EdgeID
	for id, err := range seq {
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, id)
	}
	return out
}

func scannerOf(t *testing.T, snap store.Snapshot) store.Scanner {
	t.Helper()
	sc, ok := snap.(store.Scanner)
	if !ok {
		t.Fatalf("%T does not implement store.Scanner; both bundled backends must", snap)
	}
	return sc
}

// assertScansMatch checks a snapshot's scans against its own slice reads.
func assertScansMatch(t *testing.T, snap store.Snapshot, state string) {
	t.Helper()
	sc := scannerOf(t, snap)

	nodes := drainNodes(t, sc.ScanNodes())
	if !slices.IsSorted(nodes) {
		t.Errorf("%s: ScanNodes is not ascending", state)
	}
	want, err := snap.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("%s: QueryNodeIDs: %v", state, err)
	}
	slices.Sort(want)
	if !slices.Equal(nodes, want) {
		t.Errorf("%s: ScanNodes yielded %d ids, the query returned %d", state, len(nodes), len(want))
	}

	edges := drainEdges(t, sc.ScanEdges())
	if !slices.IsSorted(edges) {
		t.Errorf("%s: ScanEdges is not ascending", state)
	}
	wantE, err := snap.QueryEdgeIDs(store.EdgeQuery{})
	if err != nil {
		t.Fatalf("%s: QueryEdgeIDs: %v", state, err)
	}
	slices.Sort(wantE)
	if !slices.Equal(edges, wantE) {
		t.Errorf("%s: ScanEdges yielded %d ids, the query returned %d", state, len(edges), len(wantE))
	}

	for _, lbl := range []store.NodeType{
		store.NodeTypeMicroArtefact,
		store.NodeTypeEvidenceFile,
		store.NodeTypeCase,
		store.NodeTypeTag,
	} {
		got := drainNodes(t, sc.ScanNodesByType(lbl))
		if !slices.IsSorted(got) {
			t.Errorf("%s: ScanNodesByType(%v) is not ascending", state, lbl)
		}
		byType, err := snap.NodesByType(lbl)
		if err != nil {
			t.Fatalf("%s: NodesByType: %v", state, err)
		}
		slices.Sort(byType)
		if !slices.Equal(got, byType) {
			t.Errorf("%s: ScanNodesByType(%v) yielded %d ids, NodesByType returned %d",
				state, lbl, len(got), len(byType))
		}
	}
}

func TestScan_AgreesWithTheSliceReads(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		scanFixture(t, g)

		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		assertScansMatch(t, snap, "uncompacted")
		if err := snap.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if err := g.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		snap, err = g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		assertScansMatch(t, snap, "compacted")
		if err := snap.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// Records on both sides of the image, which is where the merge has a
		// resumption point and the slice read does not.
		for i := range 40 {
			if _, err := g.AddNode(&store.Node{
				Labels: []store.NodeType{store.NodeTypeCase},
			}); err != nil {
				t.Fatalf("AddNode %d: %v", i, err)
			}
		}
		snap, err = g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer func() { _ = snap.Close() }()
		assertScansMatch(t, snap, "split layers")
	})
}

// The two backends must produce the same sequence for the same graph. They
// arrive at it differently — one merges two ascending sources under a lock, the
// other sorts a map — so agreement is a result, not a shared implementation.
func TestScan_BackendsAgree(t *testing.T) {
	mem := graphene.NewInMemory()
	defer mem.Close()
	dsk, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dsk.Close()

	scanFixture(t, mem)
	scanFixture(t, dsk)
	if err := dsk.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	msnap, err := mem.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = msnap.Close() }()
	dsnap, err := dsk.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer func() { _ = dsnap.Close() }()

	if m, d := drainNodes(t, scannerOf(t, msnap).ScanNodes()), drainNodes(t, scannerOf(t, dsnap).ScanNodes()); !slices.Equal(m, d) {
		t.Errorf("ScanNodes disagrees: memory %d ids, disk %d", len(m), len(d))
	}
	if m, d := drainEdges(t, scannerOf(t, msnap).ScanEdges()), drainEdges(t, scannerOf(t, dsnap).ScanEdges()); !slices.Equal(m, d) {
		t.Errorf("ScanEdges disagrees: memory %d ids, disk %d", len(m), len(d))
	}
	m := drainNodes(t, scannerOf(t, msnap).ScanNodesByType(store.NodeTypeCase))
	d := drainNodes(t, scannerOf(t, dsnap).ScanNodesByType(store.NodeTypeCase))
	if !slices.Equal(m, d) {
		t.Errorf("ScanNodesByType disagrees: memory %d ids, disk %d", len(m), len(d))
	}
}

// Breaking out is the whole point of a scan, and the sequence is a function
// rather than a cursor, so stopping one does not consume it.
func TestScan_BreakStopsEarlyAndIsRestartable(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		scanFixture(t, g)
		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer func() { _ = snap.Close() }()

		seen := 0
		for _, err := range scannerOf(t, snap).ScanNodes() {
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			seen++
			break
		}
		if seen != 1 {
			t.Errorf("break yielded %d ids, want 1", seen)
		}

		again := drainNodes(t, scannerOf(t, snap).ScanNodes())
		if len(again) < 100 {
			t.Errorf("a restarted scan yielded %d ids; the first one consumed it", len(again))
		}
	})
}

// A snapshot closed while a caller is still pulling has to say so. Swallowing
// it would make a scan that stopped early indistinguishable from one that
// finished, which is the difference between a partial export and a complete one.
func TestScan_ClosedSnapshotReportsAnError(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		scanFixture(t, g)
		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		sc := scannerOf(t, snap)
		if err := snap.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		var gotErr error
		count := 0
		for _, err := range sc.ScanNodes() {
			if err != nil {
				gotErr = err
				break
			}
			count++
		}
		if gotErr == nil {
			t.Errorf("a scan of a closed snapshot yielded %d ids and no error", count)
		}
	})
}

// The scan is only worth having if something uses it, and bulk is the first
// thing that does: it streams a source that can, and enumerates one that
// cannot. A dump taken through a snapshot — the streaming path — must be
// byte-identical to one taken through the store, or the streaming is not a
// substitution but a second behaviour.
func TestScan_BulkExportThroughASnapshotIsIdentical(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		scanFixture(t, g)

		var direct bytes.Buffer
		if _, err := bulk.ExportJSONL(&direct, g.GraphStore, bulk.Options{}); err != nil {
			t.Fatalf("export from the store: %v", err)
		}

		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer func() { _ = snap.Close() }()
		if _, ok := snap.(store.Scanner); !ok {
			t.Fatalf("%T is not a store.Scanner, so this exercises nothing", snap)
		}
		src, ok := snap.(bulk.Source)
		if !ok {
			t.Fatalf("%T cannot be used as a bulk.Source", snap)
		}

		var streamed bytes.Buffer
		if _, err := bulk.ExportJSONL(&streamed, src, bulk.Options{}); err != nil {
			t.Fatalf("export from the snapshot: %v", err)
		}
		if !bytes.Equal(direct.Bytes(), streamed.Bytes()) {
			t.Errorf("the streamed dump differs from the enumerated one: %d bytes vs %d",
				streamed.Len(), direct.Len())
		}
	})
}
