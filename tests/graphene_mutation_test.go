package graphene_test

import (
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// mutationIDs holds the IDs assigned while building the mutation fixture. Because
// IDs are monotonic and never reused, an identical op sequence yields identical
// IDs on every store, so the memory and disk runs are directly comparable.
type mutationIDs struct {
	caseID             store.NodeID
	file1, file2       store.NodeID
	art1, art2, art3   store.NodeID
	belongs1, belongs2 store.EdgeID
	c1, c2, c3         store.EdgeID
	simTo, simBack     store.EdgeID
}

// buildMutationFixture creates a small case graph and returns the assigned IDs.
func buildMutationFixture(t *testing.T, g *graphene.Graph) mutationIDs {
	t.Helper()
	var ids mutationIDs
	must := func(id store.NodeID, err error) store.NodeID {
		t.Helper()
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		return id
	}
	mustE := func(id store.EdgeID, err error) store.EdgeID {
		t.Helper()
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		return id
	}

	ids.caseID = must(g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}))
	ids.file1 = must(g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte("file-1")}))
	ids.file2 = must(g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte("file-2")}))
	ids.art1 = must(g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("art-1")}))
	ids.art2 = must(g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("art-2")}))
	ids.art3 = must(g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("art-3")}))

	ids.belongs1 = mustE(g.AddEdge(&store.Edge{Src: ids.file1, Dst: ids.caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}}))
	ids.belongs2 = mustE(g.AddEdge(&store.Edge{Src: ids.file2, Dst: ids.caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}}))
	ids.c1 = mustE(g.AddEdge(&store.Edge{Src: ids.file1, Dst: ids.art1, Labels: []store.EdgeType{store.EdgeTypeContains}}))
	ids.c2 = mustE(g.AddEdge(&store.Edge{Src: ids.file1, Dst: ids.art2, Labels: []store.EdgeType{store.EdgeTypeContains}}))
	ids.c3 = mustE(g.AddEdge(&store.Edge{Src: ids.file2, Dst: ids.art3, Labels: []store.EdgeType{store.EdgeTypeContains}}))
	ids.simTo = mustE(g.AddEdge(&store.Edge{Src: ids.art1, Dst: ids.art3, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9}))
	ids.simBack = mustE(g.AddEdge(&store.Edge{Src: ids.art3, Dst: ids.art1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.9}))

	// Index some properties so we can assert they are purged on delete.
	if err := g.IndexNodeProperty(ids.art1, "sha256", []byte("aaa")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := g.IndexNodeProperty(ids.art2, "sha256", []byte("bbb")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := g.IndexEdgeProperty(ids.simTo, "algo", []byte("tlsh")); err != nil {
		t.Fatalf("IndexEdgeProperty: %v", err)
	}
	return ids
}

// applyMutations runs an update/delete sequence exercised across all stores.
func applyMutations(t *testing.T, g *graphene.Graph, ids mutationIDs) {
	t.Helper()

	// Update a node's labels + properties.
	if err := g.UpdateNode(&store.Node{ID: ids.art2, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("art-2-updated")}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	// Update an edge's labels + weight + properties (endpoints must be ignored).
	if err := g.UpdateEdge(&store.Edge{ID: ids.simTo, Src: 999, Dst: 999, Labels: []store.EdgeType{store.EdgeTypeReuse}, Weight: 0.5, Properties: []byte("reuse")}); err != nil {
		t.Fatalf("UpdateEdge: %v", err)
	}
	// Delete a single edge.
	if err := g.DeleteEdge(ids.c1); err != nil {
		t.Fatalf("DeleteEdge: %v", err)
	}
	// Delete a node — cascades to its incident edges (belongs2, c3).
	if err := g.DeleteNode(ids.file2); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
}

type nodeSnap struct {
	ID    store.NodeID
	Kinds []uint16
	Props string
	Out   []store.EdgeID
	In    []store.EdgeID
}

type edgeSnap struct {
	ID     store.EdgeID
	Src    store.NodeID
	Dst    store.NodeID
	Kinds  []uint16
	Weight float32
	Props  string
}

type graphSnap struct {
	NodeCount uint64
	EdgeCount uint64
	Nodes     []nodeSnap
	Edges     []edgeSnap
	ByNode    map[uint16][]store.NodeID
	ByEdge    map[uint16][]store.EdgeID
}

func snapshotGraph(t *testing.T, g *graphene.Graph) graphSnap {
	t.Helper()
	nc, err := g.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	ec, err := g.EdgeCount()
	if err != nil {
		t.Fatalf("EdgeCount: %v", err)
	}

	nodeIDs, err := g.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	edgeIDs, err := g.QueryEdgeIDs(store.EdgeQuery{})
	if err != nil {
		t.Fatalf("QueryEdgeIDs: %v", err)
	}
	sort.Slice(edgeIDs, func(i, j int) bool { return edgeIDs[i] < edgeIDs[j] })

	snap := graphSnap{
		NodeCount: nc,
		EdgeCount: ec,
		ByNode:    map[uint16][]store.NodeID{},
		ByEdge:    map[uint16][]store.EdgeID{},
	}

	for _, id := range nodeIDs {
		n, err := g.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		out, err := g.EdgesOf(id, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("EdgesOf out(%d): %v", id, err)
		}
		in, err := g.EdgesOf(id, store.DirectionInbound, nil)
		if err != nil {
			t.Fatalf("EdgesOf in(%d): %v", id, err)
		}
		snap.Nodes = append(snap.Nodes, nodeSnap{
			ID:    id,
			Kinds: nodeKinds(n.Labels),
			Props: string(n.Properties),
			Out:   sortedEdgeIDs(out),
			In:    sortedEdgeIDs(in),
		})
	}

	for _, id := range edgeIDs {
		e, err := g.GetEdge(id)
		if err != nil {
			t.Fatalf("GetEdge(%d): %v", id, err)
		}
		snap.Edges = append(snap.Edges, edgeSnap{
			ID:     id,
			Src:    e.Src,
			Dst:    e.Dst,
			Kinds:  edgeKinds(e.Labels),
			Weight: e.Weight,
			Props:  string(e.Properties),
		})
	}

	for _, t16 := range []store.NodeType{store.NodeTypeCase, store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact, store.NodeTypeTag} {
		ids, err := g.NodesByType(t16)
		if err != nil {
			t.Fatalf("NodesByType(%v): %v", t16, err)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) > 0 {
			snap.ByNode[uint16(t16)] = ids
		}
	}
	for _, t16 := range []store.EdgeType{store.EdgeTypeBelongsTo, store.EdgeTypeContains, store.EdgeTypeSimilarTo, store.EdgeTypeReuse} {
		ids, err := g.EdgesByType(t16)
		if err != nil {
			t.Fatalf("EdgesByType(%v): %v", t16, err)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) > 0 {
			snap.ByEdge[uint16(t16)] = ids
		}
	}
	return snap
}

func nodeKinds(ls []store.NodeType) []uint16 {
	out := make([]uint16, len(ls))
	for i, l := range ls {
		out[i] = uint16(l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func edgeKinds(ls []store.EdgeType) []uint16 {
	out := make([]uint16, len(ls))
	for i, l := range ls {
		out[i] = uint16(l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedEdgeIDs(edges []*store.Edge) []store.EdgeID {
	out := make([]store.EdgeID, len(edges))
	for i, e := range edges {
		out[i] = e.ID
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestMutationParity_MemoryVsDisk applies the same update/delete sequence to the
// in-memory store and to the disk store across all persistence paths (live delta,
// WAL-replay reopen, and compacted CSR reopen) and asserts identical state.
func TestMutationParity_MemoryVsDisk(t *testing.T) {
	mem := graphene.NewInMemory()
	defer mem.Close()
	memIDs := buildMutationFixture(t, mem)
	applyMutations(t, mem, memIDs)
	want := snapshotGraph(t, mem)

	// Sanity: the fixture had 6 nodes / 7 edges; after mutations file2 + its two
	// incident edges (belongs2, c3) and c1 are gone -> 5 nodes, 4 edges.
	if want.NodeCount != 5 || want.EdgeCount != 4 {
		t.Fatalf("unexpected post-mutation counts: nodes=%d edges=%d", want.NodeCount, want.EdgeCount)
	}

	// --- disk: live delta ---
	dir := t.TempDir()
	disk, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open disk: %v", err)
	}
	diskIDs := buildMutationFixture(t, disk)
	applyMutations(t, disk, diskIDs)
	if got := snapshotGraph(t, disk); !reflect.DeepEqual(want, got) {
		t.Fatalf("memory vs disk(delta) mismatch:\nwant=%+v\ngot=%+v", want, got)
	}
	if err := disk.Close(); err != nil {
		t.Fatalf("close disk: %v", err)
	}

	// --- disk: reopen via WAL replay (no compaction) ---
	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen disk: %v", err)
	}
	if got := snapshotGraph(t, reopened); !reflect.DeepEqual(want, got) {
		t.Fatalf("memory vs disk(WAL replay) mismatch:\nwant=%+v\ngot=%+v", want, got)
	}

	// --- disk: compact then reopen from CSR ---
	if err := reopened.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if got := snapshotGraph(t, reopened); !reflect.DeepEqual(want, got) {
		t.Fatalf("memory vs disk(post-compact) mismatch:\nwant=%+v\ngot=%+v", want, got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close after compact: %v", err)
	}

	fromCSR, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen from CSR: %v", err)
	}
	defer fromCSR.Close()
	if got := snapshotGraph(t, fromCSR); !reflect.DeepEqual(want, got) {
		t.Fatalf("memory vs disk(CSR reload) mismatch:\nwant=%+v\ngot=%+v", want, got)
	}
}

// TestMutateCSRResidentEntities compacts first, then mutates entities that live
// only in the CSR, exercising the delete-mask / delta-override overlay paths.
func TestMutateCSRResidentEntities(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ids := buildMutationFixture(t, g)
	if err := g.Compact(); err != nil { // everything now lives in the CSR
		t.Fatalf("compact: %v", err)
	}

	// Update a CSR-resident edge that is NOT incident to the node we delete, so we
	// can assert the update survives. Then delete a CSR-resident node (cascades to
	// its CSR-resident edges).
	if err := g.UpdateEdge(&store.Edge{ID: ids.simTo, Labels: []store.EdgeType{store.EdgeTypeReuse}, Weight: 0.3}); err != nil {
		t.Fatalf("UpdateEdge (CSR): %v", err)
	}
	if err := g.DeleteNode(ids.file1); err != nil {
		t.Fatalf("DeleteNode (CSR): %v", err)
	}

	// file1 gone; its incident edges belongs1, c1, c2 gone. Started 6 nodes/7 edges.
	assertCount(t, g, 5, 4)

	if _, err := g.GetNode(ids.file1); err == nil {
		t.Fatalf("expected file1 to be deleted")
	}
	for _, eid := range []store.EdgeID{ids.belongs1, ids.c1, ids.c2} {
		if _, err := g.GetEdge(eid); err == nil {
			t.Fatalf("expected edge %d to be cascade-deleted", eid)
		}
	}

	// The mask must survive a second compaction + reload.
	if err := g.Compact(); err != nil {
		t.Fatalf("second compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	g2, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	assertCount(t, g2, 5, 4)
	if _, err := g2.GetNode(ids.file1); err == nil {
		t.Fatalf("deleted node reappeared after compact+reload")
	}
	// The updated edge kept its new type/weight.
	e, err := g2.GetEdge(ids.simTo)
	if err != nil {
		t.Fatalf("GetEdge(simTo): %v", err)
	}
	if !e.HasLabel(store.EdgeTypeReuse) || e.Weight != 0.3 {
		t.Fatalf("updated CSR edge not persisted: %+v", e)
	}
}

// TestDeleteThenReaddNeverReusesID confirms monotonic IDs are not recycled.
func TestDeleteThenReaddNeverReusesID(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func() *graphene.Graph
	}{
		{"memory", func() *graphene.Graph { return graphene.NewInMemory() }},
		{"disk", func() *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			return g
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.open()
			defer g.Close()
			a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err := g.DeleteNode(a); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
			b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if b <= a {
				t.Fatalf("expected new id > %d, got %d (id reused)", a, b)
			}
		})
	}
}

// TestNoIDReuseAfterDeleteHighestThenCompact guards the sequence high-water mark
// persistence: deleting the top ID, compacting (which drops the record), and
// reopening must not hand the ID back out.
func TestNoIDReuseAfterDeleteHighestThenCompact(t *testing.T) {
	dir := t.TempDir()

	var topNode store.NodeID
	var topEdge store.EdgeID
	{
		g, err := graphene.Open(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		topNode, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		topEdge, _ = g.AddEdge(&store.Edge{Src: a, Dst: topNode, Labels: []store.EdgeType{store.EdgeTypeReuse}})
		if err := g.Compact(); err != nil { // ids now live only in the CSR
			t.Fatalf("compact 1: %v", err)
		}
		// Delete the highest node (cascades the highest edge), then compact so the
		// rebuilt CSR no longer physically contains those max IDs.
		if err := g.DeleteNode(topNode); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
		if err := g.Compact(); err != nil {
			t.Fatalf("compact 2: %v", err)
		}
		if err := g.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g.Close()

	newNode, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if newNode <= topNode {
		t.Fatalf("node ID reused: deleted %d, got %d after reopen", topNode, newNode)
	}
	src, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	newEdge, _ := g.AddEdge(&store.Edge{Src: src, Dst: newNode, Labels: []store.EdgeType{store.EdgeTypeReuse}})
	if newEdge <= topEdge {
		t.Fatalf("edge ID reused: deleted %d, got %d after reopen", topEdge, newEdge)
	}
}

// TestConcurrentAddEdgeVsDeleteNode races AddEdge against DeleteNode on the same
// endpoint and asserts the referential-integrity invariant always holds: no live
// edge may reference a missing node. Run with -race.
func TestConcurrentAddEdgeVsDeleteNode(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func() *graphene.Graph
	}{
		{"memory", func() *graphene.Graph { return graphene.NewInMemory() }},
		{"disk", func() *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			return g
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.open()
			defer g.Close()

			const rounds = 300
			for i := 0; i < rounds; i++ {
				anchor, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
				target, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					// May succeed or fail with ErrInvalidEdge — both are fine.
					_, _ = g.AddEdge(&store.Edge{Src: anchor, Dst: target, Labels: []store.EdgeType{store.EdgeTypeReuse}})
				}()
				go func() {
					defer wg.Done()
					_ = g.DeleteNode(target)
				}()
				wg.Wait()

				// Invariant: every surviving edge points at two live nodes.
				edgeIDs, err := g.QueryEdgeIDs(store.EdgeQuery{})
				if err != nil {
					t.Fatalf("QueryEdgeIDs: %v", err)
				}
				for _, eid := range edgeIDs {
					e, err := g.GetEdge(eid)
					if err != nil {
						continue // deleted concurrently in a prior round — fine
					}
					if _, err := g.GetNode(e.Src); err != nil {
						t.Fatalf("dangling edge %d: Src %d missing", eid, e.Src)
					}
					if _, err := g.GetNode(e.Dst); err != nil {
						t.Fatalf("dangling edge %d: Dst %d missing", eid, e.Dst)
					}
				}
			}
		})
	}
}

// TestConcurrentUpdateDeleteConsistent races Update against Delete on the same
// node/edge and asserts there is never a torn internal state: the type index and
// the query view must always agree with GetNode/GetEdge (a non-atomic
// update/delete interleave would, e.g., leave an id in nodesByType but absent
// from GetNode). Run with -race.
func TestConcurrentUpdateDeleteConsistent(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func() *graphene.Graph
	}{
		{"memory", func() *graphene.Graph { return graphene.NewInMemory() }},
		{"disk", func() *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			return g
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.open()
			defer g.Close()

			const rounds = 300
			for i := 0; i < rounds; i++ {
				// --- node: UpdateNode races DeleteNode ---
				nid, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_ = g.UpdateNode(&store.Node{ID: nid, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("x")})
				}()
				go func() {
					defer wg.Done()
					_ = g.DeleteNode(nid)
				}()
				wg.Wait()
				assertNodeConsistent(t, g, nid)

				// --- edge: UpdateEdge races DeleteEdge ---
				a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
				b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
				eid, _ := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}})
				var wg2 sync.WaitGroup
				wg2.Add(2)
				go func() {
					defer wg2.Done()
					_ = g.UpdateEdge(&store.Edge{ID: eid, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.5})
				}()
				go func() {
					defer wg2.Done()
					_ = g.DeleteEdge(eid)
				}()
				wg2.Wait()
				assertEdgeConsistent(t, g, eid)
			}
		})
	}
}

// assertNodeConsistent verifies the type index and query view agree with GetNode.
func assertNodeConsistent(t *testing.T, g *graphene.Graph, id store.NodeID) {
	t.Helper()
	n, err := g.GetNode(id)
	present := err == nil

	byTag := containsNodeID(t, mustNodesByType(t, g, store.NodeTypeTag), id)
	byArt := containsNodeID(t, mustNodesByType(t, g, store.NodeTypeMicroArtefact), id)
	ids, _ := g.QueryNodeIDs(store.NodeQuery{})
	inQuery := containsNodeID(t, ids, id)

	if present {
		if len(n.Labels) == 0 {
			t.Fatalf("node %d present but has no labels", id)
		}
		if n.HasLabel(store.NodeTypeTag) != byTag {
			t.Fatalf("node %d: Tag index (%v) disagrees with GetNode (%v)", id, byTag, n.HasLabel(store.NodeTypeTag))
		}
		if n.HasLabel(store.NodeTypeMicroArtefact) != byArt {
			t.Fatalf("node %d: MicroArtefact index (%v) disagrees with GetNode (%v)", id, byArt, n.HasLabel(store.NodeTypeMicroArtefact))
		}
		if !inQuery {
			t.Fatalf("node %d present but missing from QueryNodeIDs", id)
		}
	} else {
		if byTag || byArt || inQuery {
			t.Fatalf("node %d deleted but still visible (tag=%v art=%v query=%v)", id, byTag, byArt, inQuery)
		}
	}
}

// assertEdgeConsistent verifies the type index and query view agree with GetEdge.
func assertEdgeConsistent(t *testing.T, g *graphene.Graph, id store.EdgeID) {
	t.Helper()
	e, err := g.GetEdge(id)
	present := err == nil

	byReuse := containsEdgeID(t, mustEdgesByType(t, g, store.EdgeTypeReuse), id)
	bySimilar := containsEdgeID(t, mustEdgesByType(t, g, store.EdgeTypeSimilarTo), id)
	ids, _ := g.QueryEdgeIDs(store.EdgeQuery{})
	inQuery := containsEdgeID(t, ids, id)

	if present {
		if len(e.Labels) == 0 {
			t.Fatalf("edge %d present but has no labels", id)
		}
		if e.HasLabel(store.EdgeTypeReuse) != byReuse {
			t.Fatalf("edge %d: Reuse index disagrees with GetEdge", id)
		}
		if e.HasLabel(store.EdgeTypeSimilarTo) != bySimilar {
			t.Fatalf("edge %d: SimilarTo index disagrees with GetEdge", id)
		}
		if !inQuery {
			t.Fatalf("edge %d present but missing from QueryEdgeIDs", id)
		}
	} else {
		if byReuse || bySimilar || inQuery {
			t.Fatalf("edge %d deleted but still visible (reuse=%v similar=%v query=%v)", id, byReuse, bySimilar, inQuery)
		}
	}
}

// TestConcurrentMutationDurable proves that on the disk store the WAL append and
// the in-memory apply are atomic together: after racing updates and deletes, the
// reopened (WAL-replayed) state must exactly match the live state. A non-atomic
// mutator could let the WAL order diverge from the apply order, yielding a
// different reopened state.
func TestConcurrentMutationDurable(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Seed a pool of nodes/edges.
	pool := make([]store.NodeID, 0, 40)
	for i := 0; i < 40; i++ {
		id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		pool = append(pool, id)
	}
	edges := make([]store.EdgeID, 0, 30)
	for i := 0; i+1 < len(pool); i += 2 {
		eid, _ := g.AddEdge(&store.Edge{Src: pool[i], Dst: pool[i+1], Labels: []store.EdgeType{store.EdgeTypeReuse}})
		edges = append(edges, eid)
	}

	// Concurrently update and delete overlapping entities.
	var wg sync.WaitGroup
	for i, nid := range pool {
		wg.Add(1)
		go func(id store.NodeID, k int) {
			defer wg.Done()
			if k%3 == 0 {
				_ = g.DeleteNode(id)
			} else {
				_ = g.UpdateNode(&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("u")})
			}
		}(nid, i)
	}
	for i, eid := range edges {
		wg.Add(1)
		go func(id store.EdgeID, k int) {
			defer wg.Done()
			if k%2 == 0 {
				_ = g.DeleteEdge(id)
			} else {
				_ = g.UpdateEdge(&store.Edge{ID: id, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.7})
			}
		}(eid, i)
	}
	wg.Wait()

	live := snapshotGraph(t, g)
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got := snapshotGraph(t, reopened)

	if !reflect.DeepEqual(live, got) {
		t.Fatalf("reopened state diverged from live state after concurrent mutations:\nlive=%+v\ngot=%+v", live, got)
	}
}

func mustNodesByType(t *testing.T, g *graphene.Graph, ty store.NodeType) []store.NodeID {
	t.Helper()
	ids, err := g.NodesByType(ty)
	if err != nil {
		t.Fatalf("NodesByType: %v", err)
	}
	return ids
}

func mustEdgesByType(t *testing.T, g *graphene.Graph, ty store.EdgeType) []store.EdgeID {
	t.Helper()
	ids, err := g.EdgesByType(ty)
	if err != nil {
		t.Fatalf("EdgesByType: %v", err)
	}
	return ids
}

func containsNodeID(t *testing.T, ids []store.NodeID, want store.NodeID) bool {
	t.Helper()
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsEdgeID(t *testing.T, ids []store.EdgeID, want store.EdgeID) bool {
	t.Helper()
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestMutationErrors covers the error surfaces.
func TestMutationErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func() *graphene.Graph
	}{
		{"memory", func() *graphene.Graph { return graphene.NewInMemory() }},
		{"disk", func() *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			return g
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.open()
			defer g.Close()

			if err := g.DeleteNode(12345); err == nil {
				t.Fatalf("DeleteNode(missing) should error")
			}
			if err := g.DeleteEdge(12345); err == nil {
				t.Fatalf("DeleteEdge(missing) should error")
			}
			if err := g.UpdateNode(&store.Node{ID: 12345, Labels: []store.NodeType{store.NodeTypeTag}}); err == nil {
				t.Fatalf("UpdateNode(missing) should error")
			}
			if err := g.UpdateEdge(&store.Edge{ID: 12345, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err == nil {
				t.Fatalf("UpdateEdge(missing) should error")
			}
			// Missing labels rejected.
			n, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
			if err := g.UpdateNode(&store.Node{ID: n}); err == nil {
				t.Fatalf("UpdateNode with no labels should error")
			}
			// Adding an edge onto a deleted node fails referential integrity.
			a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err := g.DeleteNode(b); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
			if _, err := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeReuse}}); err == nil {
				t.Fatalf("AddEdge onto deleted node should error")
			}
		})
	}
}

func assertCount(t *testing.T, g *graphene.Graph, wantN, wantE uint64) {
	t.Helper()
	nc, err := g.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	ec, err := g.EdgeCount()
	if err != nil {
		t.Fatalf("EdgeCount: %v", err)
	}
	if nc != wantN || ec != wantE {
		t.Fatalf("count mismatch: nodes=%d (want %d) edges=%d (want %d)", nc, wantN, wantE, ec)
	}
}

// TestPropertyIndexPurgedOnDelete confirms deleted entities drop out of the
// property index (both stores).
func TestPropertyIndexPurgedOnDelete(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func() *graphene.Graph
	}{
		{"memory", func() *graphene.Graph { return graphene.NewInMemory() }},
		{"disk", func() *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			return g
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.open()
			defer g.Close()
			ids := buildMutationFixture(t, g)

			hits, _ := g.NodesByProperty("sha256", []byte("aaa"))
			if len(hits) != 1 {
				t.Fatalf("expected art1 indexed, got %v", hits)
			}
			if err := g.DeleteNode(ids.art1); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
			if hits, _ := g.NodesByProperty("sha256", []byte("aaa")); len(hits) != 0 {
				t.Fatalf("art1 property index not purged: %v", hits)
			}
			// The edge property for simTo (incident to art1) should be gone too.
			if hits, _ := g.EdgesByProperty("algo", []byte("tlsh")); len(hits) != 0 {
				t.Fatalf("cascaded edge property index not purged: %v", hits)
			}
		})
	}
}
