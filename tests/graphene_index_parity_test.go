package graphene_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// The query planner picks a driving index (property postings, label postings, or
// incident-edge lists) instead of enumerating the whole graph. These tests pin
// the invariant that makes that safe: a planned query must return exactly what
// the same query returns when it is forced down the explicit-ID path, which
// bypasses driving-set selection entirely.

// forceScanNodes re-runs q with every live node ID supplied explicitly, which
// makes the store fall back to candidate enumeration + filtering.
func forceScanNodes(t *testing.T, g *graphene.Graph, all []store.NodeID, q store.NodeQuery) []store.NodeID {
	t.Helper()
	forced := q
	forced.IDs = all
	ids, err := g.QueryNodeIDs(forced)
	if err != nil {
		t.Fatalf("forced scan query: %v", err)
	}
	return ids
}

func forceScanEdges(t *testing.T, g *graphene.Graph, all []store.EdgeID, q store.EdgeQuery) []store.EdgeID {
	t.Helper()
	forced := q
	forced.IDs = all
	ids, err := g.QueryEdgeIDs(forced)
	if err != nil {
		t.Fatalf("forced scan query: %v", err)
	}
	return ids
}

// plannerFixture builds a graph exercising every state the planner must respect:
// multi-label nodes, indexed and un-indexed properties, updated labels, deleted
// nodes and edges, and (on disk) records split across the CSR and the delta.
type plannerFixture struct {
	g        *graphene.Graph
	nodeIDs  []store.NodeID // live nodes
	edgeIDs  []store.EdgeID // live edges
	anchors  []store.NodeID
	deleted  []store.NodeID
	compacts bool
}

func buildPlannerFixture(t *testing.T, g *graphene.Graph, compactMidway bool) *plannerFixture {
	t.Helper()
	// The fixture updates indexed nodes with plain UpdateNode, and the stale
	// entries that leaves are part of what the planner is being asked to survive.
	// The v0.5.0 default refuses that update, so the fixture states the policy it
	// was written against rather than silently building a different graph.
	g.SetReindexPolicy(store.ReindexKeep)
	const n = 300

	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		labels := []store.NodeType{store.NodeTypeMicroArtefact}
		switch {
		case i%25 == 0:
			labels = []store.NodeType{store.NodeTypeCase, store.NodeTypeTag}
		case i%5 == 0:
			labels = []store.NodeType{store.NodeTypeEvidenceFile}
		}
		id, err := g.GraphStore.AddNode(&store.Node{Labels: labels})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		// Every third node is left out of the property index entirely, so the
		// tests cover entities the index cannot see.
		if i%3 != 0 {
			err = g.IndexNodeProperties(id, map[string][]byte{
				"sha256": []byte(fmt.Sprintf("hash-%04d", i)),
				"bucket": []byte(fmt.Sprintf("bucket-%02d", i%7)),
				"score":  []byte(fmt.Sprintf("%04d", i%50)),
			})
			if err != nil {
				t.Fatalf("IndexNodeProperties: %v", err)
			}
		}
		ids = append(ids, id)
	}

	if compactMidway {
		if err := g.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
	}

	edgeIDs := make([]store.EdgeID, 0, 2*n)
	for i := 0; i < n-1; i++ {
		eid, err := g.GraphStore.AddEdge(&store.Edge{
			Src:    ids[i],
			Dst:    ids[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
			Weight: 0.25,
		})
		if err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		if i%2 == 0 {
			if err := g.IndexEdgeProperties(eid, map[string][]byte{
				"algo": []byte(fmt.Sprintf("algo-%d", i%4)),
			}); err != nil {
				t.Fatalf("IndexEdgeProperties: %v", err)
			}
		}
		edgeIDs = append(edgeIDs, eid)
	}
	// A hub so anchored queries have a node with meaningful degree.
	hub := ids[n/2]
	for i := 0; i < 40; i++ {
		eid, err := g.GraphStore.AddEdge(&store.Edge{
			Src:    ids[(i*7)%n],
			Dst:    hub,
			Labels: []store.EdgeType{store.EdgeTypeBelongsTo},
		})
		if err != nil {
			t.Fatalf("AddEdge hub: %v", err)
		}
		edgeIDs = append(edgeIDs, eid)
	}

	// Mutate: relabel some nodes, delete some nodes (cascading edges) and one edge.
	for i := 1; i < n; i += 40 {
		if err := g.GraphStore.UpdateNode(&store.Node{
			ID:     ids[i],
			Labels: []store.NodeType{store.NodeTypeTag},
		}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
	}
	var deleted []store.NodeID
	for i := 3; i < n; i += 37 {
		if err := g.GraphStore.DeleteNode(ids[i]); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
		deleted = append(deleted, ids[i])
	}

	live := make([]store.NodeID, 0, len(ids))
	for _, id := range ids {
		if _, err := g.GraphStore.GetNode(id); err == nil {
			live = append(live, id)
		}
	}
	liveEdges := make([]store.EdgeID, 0, len(edgeIDs))
	for _, eid := range edgeIDs {
		if _, err := g.GraphStore.GetEdge(eid); err == nil {
			liveEdges = append(liveEdges, eid)
		}
	}

	return &plannerFixture{
		g:        g,
		nodeIDs:  live,
		edgeIDs:  liveEdges,
		anchors:  []store.NodeID{hub, live[0], live[len(live)-1]},
		deleted:  deleted,
		compacts: compactMidway,
	}
}

func plannerNodeQueries() map[string]store.NodeQuery {
	return map[string]store.NodeQuery{
		"types only": {
			Types: []store.NodeType{store.NodeTypeCase},
		},
		"multi type": {
			Types: []store.NodeType{store.NodeTypeCase, store.NodeTypeEvidenceFile},
		},
		"equality unique": {
			Filters: []store.PropertyFilter{{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-0100")}},
		},
		"equality medium": {
			Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-03")}},
		},
		"equality miss": {
			Filters: []store.PropertyFilter{{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("nope")}},
		},
		"two equality all": {
			Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-03")},
				{Key: "score", Op: store.PropertyOpEqual, Value: []byte("0010")},
			},
			FilterMode: store.MatchAll,
		},
		"two equality any": {
			Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-03")},
				{Key: "score", Op: store.PropertyOpEqual, Value: []byte("0010")},
			},
			FilterMode: store.MatchAny,
		},
		"prefix": {
			Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-0")}},
		},
		"contains": {
			Filters: []store.PropertyFilter{{Key: "sha256", Op: store.PropertyOpContains, Value: []byte("01")}},
		},
		"range": {
			Filters: []store.PropertyFilter{
				{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("0010"), ValueUpper: []byte("0020")},
			},
		},
		"greater than": {
			Filters: []store.PropertyFilter{{Key: "score", Op: store.PropertyOpGreaterThan, Value: []byte("0040")}},
		},
		"type and equality": {
			Types:   []store.NodeType{store.NodeTypeMicroArtefact},
			Filters: []store.PropertyFilter{{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-03")}},
		},
		"type and range": {
			Types: []store.NodeType{store.NodeTypeEvidenceFile},
			Filters: []store.PropertyFilter{
				{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("0000"), ValueUpper: []byte("0025")},
			},
		},
		"mixed ops any": {
			Filters: []store.PropertyFilter{
				{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-0100")},
				{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-1")},
			},
			FilterMode: store.MatchAny,
		},
		"descending window": {
			Types:  []store.NodeType{store.NodeTypeMicroArtefact},
			Order:  store.QueryOrderDesc,
			Offset: 5,
			Limit:  10,
		},
		"ascending window": {
			Types:  []store.NodeType{store.NodeTypeMicroArtefact},
			Order:  store.QueryOrderAsc,
			Offset: 3,
			Limit:  7,
		},
		"limit only": {
			Types: []store.NodeType{store.NodeTypeCase},
			Limit: 2,
		},
	}
}

func runNodeParity(t *testing.T, f *plannerFixture) {
	t.Helper()
	for name, q := range plannerNodeQueries() {
		t.Run(name, func(t *testing.T) {
			got, err := f.g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			want := forceScanNodes(t, f.g, f.nodeIDs, q)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("planned query != full scan\n planned (%d): %v\n scanned (%d): %v", len(got), got, len(want), want)
			}
			for _, id := range got {
				if _, err := f.g.GraphStore.GetNode(id); err != nil {
					t.Fatalf("result contains dead node %d: %v", id, err)
				}
			}
		})
	}
}

func plannerEdgeQueries(anchors []store.NodeID) map[string]store.EdgeQuery {
	return map[string]store.EdgeQuery{
		"types only": {
			Types: []store.EdgeType{store.EdgeTypeBelongsTo},
		},
		"src anchored": {
			SrcIDs: anchors,
		},
		"dst anchored": {
			DstIDs: anchors,
		},
		"src and dst anchored": {
			SrcIDs: anchors,
			DstIDs: anchors,
		},
		"anchored with type": {
			DstIDs: anchors,
			Types:  []store.EdgeType{store.EdgeTypeBelongsTo},
		},
		"anchored with filter": {
			SrcIDs:  anchors,
			Filters: []store.PropertyFilter{{Key: "algo", Op: store.PropertyOpEqual, Value: []byte("algo-0")}},
		},
		"equality only": {
			Filters: []store.PropertyFilter{{Key: "algo", Op: store.PropertyOpEqual, Value: []byte("algo-2")}},
		},
		"prefix only": {
			Filters: []store.PropertyFilter{{Key: "algo", Op: store.PropertyOpPrefix, Value: []byte("algo-")}},
		},
		"anchored window desc": {
			DstIDs: anchors,
			Order:  store.QueryOrderDesc,
			Limit:  5,
		},
	}
}

func runEdgeParity(t *testing.T, f *plannerFixture) {
	t.Helper()
	for name, q := range plannerEdgeQueries(f.anchors) {
		t.Run(name, func(t *testing.T) {
			got, err := f.g.QueryEdgeIDs(q)
			if err != nil {
				t.Fatalf("QueryEdgeIDs: %v", err)
			}
			want := forceScanEdges(t, f.g, f.edgeIDs, q)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("planned query != full scan\n planned (%d): %v\n scanned (%d): %v", len(got), got, len(want), want)
			}
			for _, id := range got {
				if _, err := f.g.GraphStore.GetEdge(id); err != nil {
					t.Fatalf("result contains dead edge %d: %v", id, err)
				}
			}
		})
	}
}

func TestQueryPlanner_ParityMemory(t *testing.T) {
	f := buildPlannerFixture(t, graphene.NewInMemory(), false)
	runNodeParity(t, f)
	runEdgeParity(t, f)
}

func TestQueryPlanner_ParityDiskDeltaOnly(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()
	f := buildPlannerFixture(t, g, false)
	runNodeParity(t, f)
	runEdgeParity(t, f)
}

// Nodes land in the CSR while edges and all mutations stay in the delta, so
// every read has to merge the two layers.
func TestQueryPlanner_ParityDiskSplitLayers(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()
	f := buildPlannerFixture(t, g, true)
	runNodeParity(t, f)
	runEdgeParity(t, f)
}

// Everything is compacted into the CSR, including the tombstones.
func TestQueryPlanner_ParityDiskFullyCompacted(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	f := buildPlannerFixture(t, g, false)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	runNodeParity(t, f)
	runEdgeParity(t, f)

	// And again after a reopen, which rebuilds the property index from the WAL.
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	f.g = reopened
	runNodeParity(t, f)
	runEdgeParity(t, f)
}

// The two backends must agree on results for identical inputs.
func TestQueryPlanner_CrossBackendAgreement(t *testing.T) {
	mem := buildPlannerFixture(t, graphene.NewInMemory(), false)
	dg, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dg.Close()
	dsk := buildPlannerFixture(t, dg, true)

	for name, q := range plannerNodeQueries() {
		t.Run("node/"+name, func(t *testing.T) {
			memIDs, err := mem.g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("memory: %v", err)
			}
			diskIDs, err := dsk.g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("disk: %v", err)
			}
			if !reflect.DeepEqual(memIDs, diskIDs) {
				t.Fatalf("backend mismatch\n memory (%d): %v\n disk   (%d): %v", len(memIDs), memIDs, len(diskIDs), diskIDs)
			}
		})
	}
	for name, q := range plannerEdgeQueries(mem.anchors) {
		t.Run("edge/"+name, func(t *testing.T) {
			memIDs, err := mem.g.QueryEdgeIDs(q)
			if err != nil {
				t.Fatalf("memory: %v", err)
			}
			diskIDs, err := dsk.g.QueryEdgeIDs(q)
			if err != nil {
				t.Fatalf("disk: %v", err)
			}
			if !reflect.DeepEqual(memIDs, diskIDs) {
				t.Fatalf("backend mismatch\n memory (%d): %v\n disk   (%d): %v", len(memIDs), memIDs, len(diskIDs), diskIDs)
			}
		})
	}
}

// DegreeOf is a fast path around EdgesOf; the two must never disagree.
func TestDegreeCounter_MatchesEdgesOf(t *testing.T) {
	backends := map[string]func(t *testing.T) *graphene.Graph{
		"memory": func(t *testing.T) *graphene.Graph { return graphene.NewInMemory() },
		"disk": func(t *testing.T) *graphene.Graph {
			g, err := graphene.Open(t.TempDir())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { g.Close() })
			return g
		},
	}

	for name, open := range backends {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				f := buildPlannerFixture(t, g, compact)
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				dirs := []store.Direction{store.DirectionOutbound, store.DirectionInbound, store.DirectionBoth}
				for _, id := range f.nodeIDs {
					for _, dir := range dirs {
						edges, err := g.EdgesOf(id, dir, nil)
						if err != nil {
							t.Fatalf("EdgesOf(%d, %v): %v", id, dir, err)
						}
						counter, ok := g.GraphStore.(store.DegreeCounter)
						if !ok {
							t.Fatal("backend does not implement store.DegreeCounter")
						}
						got, err := counter.DegreeOf(id, dir, nil)
						if err != nil {
							t.Fatalf("DegreeOf(%d, %v): %v", id, dir, err)
						}
						if got != len(edges) {
							t.Fatalf("node %d dir %v: DegreeOf=%d, len(EdgesOf)=%d", id, dir, got, len(edges))
						}
					}
				}

				// Typed degree goes through the filtered path.
				for _, id := range f.anchors {
					edges, err := g.EdgesOf(id, store.DirectionInbound, []store.EdgeType{store.EdgeTypeBelongsTo})
					if err != nil {
						t.Fatalf("EdgesOf typed: %v", err)
					}
					got, err := g.InDegree(id, []store.EdgeType{store.EdgeTypeBelongsTo})
					if err != nil {
						t.Fatalf("InDegree typed: %v", err)
					}
					if got != len(edges) {
						t.Fatalf("node %d typed InDegree=%d, want %d", id, got, len(edges))
					}
				}
			})
		}
	}
}

// Re-registering the same property triple must not duplicate postings entries.
func TestPropertyIndex_IdempotentRegistration(t *testing.T) {
	g := graphene.NewInMemory()
	id, err := g.GraphStore.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := g.GraphStore.IndexNodeProperty(id, "sha256", []byte("abc")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	ids, err := g.GraphStore.NodesByProperty("sha256", []byte("abc"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("NodesByProperty = %v, want exactly [%d]", ids, id)
	}
}

// A window changes how many rows a query returns and nothing else.
//
// That is not free any more. The window is pushed into the label driver, which
// stops the driving set short, and into the residual pass, which stops
// evaluating filters once enough candidates have survived — so a query with a
// limit now runs different code from the same query without one. The forced
// scan cannot police that: it takes the same window through the same push-down,
// so a bug in either would show up identically on both sides.
//
// The oracle here is the query itself, unwindowed. Whatever it returns, the
// windowed form has to return the corresponding slice of it, for every query in
// the planner's table and at several offsets — including offsets past the end,
// where a bounded pass has the most room to lose its place.
func runWindowParity(t *testing.T, f *plannerFixture) {
	t.Helper()
	windows := []struct{ offset, limit int }{
		{0, 1}, {0, 5}, {0, 1000}, {3, 7}, {5, 2}, {50, 5}, {10000, 5},
	}

	for name, q := range plannerNodeQueries() {
		if q.Limit != 0 || q.Offset != 0 {
			continue // already windowed; its own parity case covers it
		}
		t.Run("node/"+name, func(t *testing.T) {
			full, err := f.g.QueryNodeIDs(q)
			if err != nil {
				t.Fatalf("unwindowed query: %v", err)
			}
			for _, w := range windows {
				windowed := q
				windowed.Offset, windowed.Limit = w.offset, w.limit
				got, err := f.g.QueryNodeIDs(windowed)
				if err != nil {
					t.Fatalf("offset %d limit %d: %v", w.offset, w.limit, err)
				}
				want := store.ApplyNodeQueryWindow(append([]store.NodeID(nil), full...), w.offset, w.limit)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("offset %d limit %d: got %v, the unwindowed answer sliced is %v",
						w.offset, w.limit, got, want)
				}
			}
		})
	}

	for name, q := range plannerEdgeQueries(f.anchors) {
		if q.Limit != 0 || q.Offset != 0 {
			continue
		}
		t.Run("edge/"+name, func(t *testing.T) {
			full, err := f.g.QueryEdgeIDs(q)
			if err != nil {
				t.Fatalf("unwindowed query: %v", err)
			}
			for _, w := range windows {
				windowed := q
				windowed.Offset, windowed.Limit = w.offset, w.limit
				got, err := f.g.QueryEdgeIDs(windowed)
				if err != nil {
					t.Fatalf("offset %d limit %d: %v", w.offset, w.limit, err)
				}
				want := store.ApplyEdgeQueryWindow(append([]store.EdgeID(nil), full...), w.offset, w.limit)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("offset %d limit %d: got %v, the unwindowed answer sliced is %v",
						w.offset, w.limit, got, want)
				}
			}
		})
	}
}

func TestQueryPlanner_WindowParityMemory(t *testing.T) {
	runWindowParity(t, buildPlannerFixture(t, graphene.NewInMemory(), false))
}

func TestQueryPlanner_WindowParityDiskDeltaOnly(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()
	runWindowParity(t, buildPlannerFixture(t, g, false))
}

// The layer state the merge breaks on first, and therefore the one a bounded
// merge breaks on first too.
func TestQueryPlanner_WindowParityDiskSplitLayers(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()
	runWindowParity(t, buildPlannerFixture(t, g, true))
}

func TestQueryPlanner_WindowParityDiskFullyCompacted(t *testing.T) {
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()
	f := buildPlannerFixture(t, g, false)
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	runWindowParity(t, f)
}

// The residual pass evaluates a prefix of the candidates at a time and stops
// when enough have survived, doubling the prefix as it goes. Everything above
// runs on a few hundred candidates, which the first prefix covers whole — so
// the part that has to hold its place across prefixes is exactly the part those
// tests cannot reach.
//
// This builds a set several prefixes deep whose matches are all at the far end,
// so a window can only be filled by crossing every boundary. The filter is a
// contains, deliberately: an equality would drive the query from the property
// index and hand the residual pass the answer rather than the search.
func TestResidual_BoundedPassCrossesChunkBoundaries(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		const n, matches = 3000, 40
		for i := range n {
			id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
			if err != nil {
				t.Fatalf("AddNode %d: %v", i, err)
			}
			tier := "cold"
			if i >= n-matches {
				tier = "hot"
			}
			if err := g.IndexNodeProperties(id, map[string][]byte{
				"tier": []byte(fmt.Sprintf("%s-%04d", tier, i)),
			}); err != nil {
				t.Fatalf("IndexNodeProperties: %v", err)
			}
		}

		q := store.NodeQuery{
			Types:   []store.NodeType{store.NodeTypeCase},
			Filters: []store.PropertyFilter{{Key: "tier", Op: store.PropertyOpContains, Value: []byte("hot")}},
		}
		full, err := g.QueryNodeIDs(q)
		if err != nil {
			t.Fatalf("unwindowed: %v", err)
		}
		if len(full) != matches {
			t.Fatalf("the fixture matched %d nodes, want %d", len(full), matches)
		}

		for _, w := range []struct{ offset, limit int }{
			{0, 1}, {0, 5}, {0, matches}, {0, matches + 10}, {5, 5}, {matches - 1, 5}, {matches, 5},
		} {
			windowed := q
			windowed.Offset, windowed.Limit = w.offset, w.limit
			got, err := g.QueryNodeIDs(windowed)
			if err != nil {
				t.Fatalf("offset %d limit %d: %v", w.offset, w.limit, err)
			}
			want := store.ApplyNodeQueryWindow(append([]store.NodeID(nil), full...), w.offset, w.limit)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("offset %d limit %d: got %d ids %v, want %d %v",
					w.offset, w.limit, len(got), got, len(want), want)
			}
		}
	})
}
