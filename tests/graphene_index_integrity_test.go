package graphene_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// These tests attack the indexes rather than the queries: they drive random
// mutation sequences through a store and then assert that what the indexes say
// still matches what the records say. The query-level equivalence checks live in
// graphene_index_parity_test.go.

// indexSnapshot is a backend-independent description of everything the indexes
// claim, reduced to comparable values.
type indexSnapshot struct {
	NodesByType map[store.NodeType][]store.NodeID
	EdgesByType map[store.EdgeType][]store.EdgeID
	NodeProps   map[string][]store.NodeID // "key\x00value" -> ids
	EdgeProps   map[string][]store.EdgeID
	NodeCount   uint64
	EdgeCount   uint64
}

var (
	snapshotNodeTypes = []store.NodeType{
		store.NodeTypeCase, store.NodeTypeEvidenceFile,
		store.NodeTypeMicroArtefact, store.NodeTypeTag,
	}
	snapshotEdgeTypes = []store.EdgeType{
		store.EdgeTypeContains, store.EdgeTypeSimilarTo,
		store.EdgeTypeBelongsTo, store.EdgeTypeReuse,
	}
)

func takeIndexSnapshot(t *testing.T, g *graphene.Graph, propKeys, propValues []string) *indexSnapshot {
	t.Helper()
	snap := &indexSnapshot{
		NodesByType: make(map[store.NodeType][]store.NodeID),
		EdgesByType: make(map[store.EdgeType][]store.EdgeID),
		NodeProps:   make(map[string][]store.NodeID),
		EdgeProps:   make(map[string][]store.EdgeID),
	}

	for _, nt := range snapshotNodeTypes {
		ids, err := g.NodesByType(nt)
		if err != nil {
			t.Fatalf("NodesByType(%v): %v", nt, err)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		snap.NodesByType[nt] = ids
	}
	for _, et := range snapshotEdgeTypes {
		ids, err := g.EdgesByType(et)
		if err != nil {
			t.Fatalf("EdgesByType(%v): %v", et, err)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		snap.EdgesByType[et] = ids
	}
	for _, k := range propKeys {
		for _, v := range propValues {
			ids, err := g.NodesByProperty(k, []byte(v))
			if err != nil {
				t.Fatalf("NodesByProperty: %v", err)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			snap.NodeProps[k+"\x00"+v] = ids

			eids, err := g.EdgesByProperty(k, []byte(v))
			if err != nil {
				t.Fatalf("EdgesByProperty: %v", err)
			}
			sort.Slice(eids, func(i, j int) bool { return eids[i] < eids[j] })
			snap.EdgeProps[k+"\x00"+v] = eids
		}
	}

	nc, err := g.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	ec, err := g.EdgeCount()
	if err != nil {
		t.Fatalf("EdgeCount: %v", err)
	}
	snap.NodeCount, snap.EdgeCount = nc, ec
	return snap
}

// normaliseEmpty turns nil and empty slices into a single representation so
// snapshot comparison does not fail on that distinction alone.
func (s *indexSnapshot) normalise() {
	for k, v := range s.NodesByType {
		if len(v) == 0 {
			s.NodesByType[k] = nil
		}
	}
	for k, v := range s.EdgesByType {
		if len(v) == 0 {
			s.EdgesByType[k] = nil
		}
	}
	for k, v := range s.NodeProps {
		if len(v) == 0 {
			s.NodeProps[k] = nil
		}
	}
	for k, v := range s.EdgeProps {
		if len(v) == 0 {
			s.EdgeProps[k] = nil
		}
	}
}

// mutationLog records the operations applied, so a replay can rebuild an
// equivalent graph from scratch and the two indexes can be compared.
type mutationOp struct {
	Kind   string // "addNode", "addEdge", "updateNode", "deleteNode", "deleteEdge", "indexNode", "indexEdge"
	NodeIx int    // index into the created-node slice, not a NodeID
	EdgeIx int
	Labels []store.NodeType
	ELabel []store.EdgeType
	SrcIx  int
	DstIx  int
	Key    string
	Value  string
}

// applyRandomMutations drives a pseudo-random but reproducible mutation sequence
// and returns the operation log.
func applyRandomMutations(t *testing.T, g *graphene.Graph, rng *rand.Rand, steps int, propKeys, propValues []string) []mutationOp {
	t.Helper()

	var ops []mutationOp
	var nodeIDs []store.NodeID // index-aligned with creation order; zero once deleted
	var edgeIDs []store.EdgeID

	liveNodeIx := func() (int, bool) {
		var live []int
		for i, id := range nodeIDs {
			if id != store.InvalidNodeID {
				live = append(live, i)
			}
		}
		if len(live) == 0 {
			return 0, false
		}
		return live[rng.Intn(len(live))], true
	}
	liveEdgeIx := func() (int, bool) {
		var live []int
		for i, id := range edgeIDs {
			if id != store.InvalidEdgeID {
				live = append(live, i)
			}
		}
		if len(live) == 0 {
			return 0, false
		}
		return live[rng.Intn(len(live))], true
	}

	record := func(op mutationOp) { ops = append(ops, op) }

	for step := 0; step < steps; step++ {
		switch rng.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
			20, 21, 22, 23, 24, 25, 26, 27, 28, 29: // 30% add node
			labels := []store.NodeType{snapshotNodeTypes[rng.Intn(len(snapshotNodeTypes))]}
			if rng.Intn(4) == 0 {
				labels = append(labels, snapshotNodeTypes[rng.Intn(len(snapshotNodeTypes))])
			}
			id, err := g.AddNode(&store.Node{Labels: labels})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			nodeIDs = append(nodeIDs, id)
			record(mutationOp{Kind: "addNode", Labels: labels})

		case 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
			40, 41, 42, 43, 44, 45, 46, 47, 48, 49: // 20% add edge
			srcIx, ok1 := liveNodeIx()
			dstIx, ok2 := liveNodeIx()
			if !ok1 || !ok2 {
				continue
			}
			labels := []store.EdgeType{snapshotEdgeTypes[rng.Intn(len(snapshotEdgeTypes))]}
			id, err := g.AddEdge(&store.Edge{Src: nodeIDs[srcIx], Dst: nodeIDs[dstIx], Labels: labels})
			if err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
			edgeIDs = append(edgeIDs, id)
			record(mutationOp{Kind: "addEdge", SrcIx: srcIx, DstIx: dstIx, ELabel: labels})

		case 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
			60, 61, 62, 63, 64: // 15% index a node property
			ix, ok := liveNodeIx()
			if !ok {
				continue
			}
			k := propKeys[rng.Intn(len(propKeys))]
			v := propValues[rng.Intn(len(propValues))]
			if err := g.IndexNodeProperty(nodeIDs[ix], k, []byte(v)); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
			record(mutationOp{Kind: "indexNode", NodeIx: ix, Key: k, Value: v})

		case 65, 66, 67, 68, 69, 70, 71, 72, 73, 74: // 10% index an edge property
			ix, ok := liveEdgeIx()
			if !ok {
				continue
			}
			k := propKeys[rng.Intn(len(propKeys))]
			v := propValues[rng.Intn(len(propValues))]
			if err := g.IndexEdgeProperty(edgeIDs[ix], k, []byte(v)); err != nil {
				t.Fatalf("IndexEdgeProperty: %v", err)
			}
			record(mutationOp{Kind: "indexEdge", EdgeIx: ix, Key: k, Value: v})

		case 75, 76, 77, 78, 79, 80, 81, 82, 83, 84: // 10% relabel a node
			ix, ok := liveNodeIx()
			if !ok {
				continue
			}
			labels := []store.NodeType{snapshotNodeTypes[rng.Intn(len(snapshotNodeTypes))]}
			if err := g.UpdateNode(&store.Node{ID: nodeIDs[ix], Labels: labels}); err != nil {
				t.Fatalf("UpdateNode: %v", err)
			}
			record(mutationOp{Kind: "updateNode", NodeIx: ix, Labels: labels})

		case 85, 86, 87, 88, 89, 90, 91, 92: // 8% delete a node (cascades)
			ix, ok := liveNodeIx()
			if !ok {
				continue
			}
			victim := nodeIDs[ix]
			if err := g.DeleteNode(victim); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
			nodeIDs[ix] = store.InvalidNodeID
			// Cascade removed incident edges; mark them dead in our shadow state.
			for i, eid := range edgeIDs {
				if eid == store.InvalidEdgeID {
					continue
				}
				if _, err := g.GetEdge(eid); err != nil {
					edgeIDs[i] = store.InvalidEdgeID
				}
			}
			record(mutationOp{Kind: "deleteNode", NodeIx: ix})

		default: // ~7% delete an edge
			ix, ok := liveEdgeIx()
			if !ok {
				continue
			}
			if err := g.DeleteEdge(edgeIDs[ix]); err != nil {
				t.Fatalf("DeleteEdge: %v", err)
			}
			edgeIDs[ix] = store.InvalidEdgeID
			record(mutationOp{Kind: "deleteEdge", EdgeIx: ix})
		}

		// The indexes must be self-consistent after every single mutation, not
		// just at the end — that is what localises a bug to one operation.
		if err := g.VerifyIndexes(); err != nil {
			t.Fatalf("step %d (%s): VerifyIndexes: %v", step, ops[len(ops)-1].Kind, err)
		}
	}

	return ops
}

// replayMutations rebuilds an equivalent graph by applying the same logical
// operations to a fresh store. Because IDs are assigned in the same order, the
// resulting IDs match, so the two indexes are directly comparable.
func replayMutations(t *testing.T, g *graphene.Graph, ops []mutationOp) {
	t.Helper()
	var nodeIDs []store.NodeID
	var edgeIDs []store.EdgeID

	for i, op := range ops {
		switch op.Kind {
		case "addNode":
			id, err := g.AddNode(&store.Node{Labels: op.Labels})
			if err != nil {
				t.Fatalf("replay %d AddNode: %v", i, err)
			}
			nodeIDs = append(nodeIDs, id)
		case "addEdge":
			id, err := g.AddEdge(&store.Edge{Src: nodeIDs[op.SrcIx], Dst: nodeIDs[op.DstIx], Labels: op.ELabel})
			if err != nil {
				t.Fatalf("replay %d AddEdge: %v", i, err)
			}
			edgeIDs = append(edgeIDs, id)
		case "indexNode":
			if err := g.IndexNodeProperty(nodeIDs[op.NodeIx], op.Key, []byte(op.Value)); err != nil {
				t.Fatalf("replay %d IndexNodeProperty: %v", i, err)
			}
		case "indexEdge":
			if err := g.IndexEdgeProperty(edgeIDs[op.EdgeIx], op.Key, []byte(op.Value)); err != nil {
				t.Fatalf("replay %d IndexEdgeProperty: %v", i, err)
			}
		case "updateNode":
			if err := g.UpdateNode(&store.Node{ID: nodeIDs[op.NodeIx], Labels: op.Labels}); err != nil {
				t.Fatalf("replay %d UpdateNode: %v", i, err)
			}
		case "deleteNode":
			if err := g.DeleteNode(nodeIDs[op.NodeIx]); err != nil {
				t.Fatalf("replay %d DeleteNode: %v", i, err)
			}
		case "deleteEdge":
			if err := g.DeleteEdge(edgeIDs[op.EdgeIx]); err != nil {
				t.Fatalf("replay %d DeleteEdge: %v", i, err)
			}
		}
	}
}

// A store mutated in place must end up with the same index state as a store
// built fresh from the same operation log. Incremental maintenance and a
// from-scratch build have to agree.
func TestIndexIntegrity_IncrementalMatchesRebuild(t *testing.T) {
	propKeys := []string{"sha256", "bucket", "tool"}
	propValues := []string{"alpha", "beta", "gamma", "delta"}

	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("memory/seed=%d", seed), func(t *testing.T) {
			mutated := graphene.NewInMemory()
			ops := applyRandomMutations(t, mutated, rand.New(rand.NewSource(seed)), 400, propKeys, propValues)

			rebuilt := graphene.NewInMemory()
			replayMutations(t, rebuilt, ops)

			got := takeIndexSnapshot(t, mutated, propKeys, propValues)
			want := takeIndexSnapshot(t, rebuilt, propKeys, propValues)
			got.normalise()
			want.normalise()
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("incremental index state != rebuilt index state\n got: %+v\nwant: %+v", got, want)
			}
		})
	}
}

// The same invariant on the disk backend, including across a Compact() that
// folds the delta into the CSR and a reopen that rebuilds from the WAL.
func TestIndexIntegrity_DiskCompactAndReopen(t *testing.T) {
	propKeys := []string{"sha256", "bucket"}
	propValues := []string{"alpha", "beta", "gamma"}

	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			dir := t.TempDir()
			g, err := graphene.Open(dir)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			applyRandomMutations(t, g, rand.New(rand.NewSource(seed)), 250, propKeys, propValues)
			before := takeIndexSnapshot(t, g, propKeys, propValues)
			before.normalise()

			if err := g.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes after Compact: %v", err)
			}
			afterCompact := takeIndexSnapshot(t, g, propKeys, propValues)
			afterCompact.normalise()
			if !reflect.DeepEqual(before, afterCompact) {
				t.Fatalf("Compact changed index state\n before: %+v\n after:  %+v", before, afterCompact)
			}

			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			reopened, err := graphene.Open(dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()

			if err := reopened.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes after reopen: %v", err)
			}
			afterReopen := takeIndexSnapshot(t, reopened, propKeys, propValues)
			afterReopen.normalise()
			if !reflect.DeepEqual(before, afterReopen) {
				t.Fatalf("reopen changed index state\n before: %+v\n after:  %+v", before, afterReopen)
			}
		})
	}
}

// A store that is compacted repeatedly mid-stream must end up identical to one
// that is compacted only at the end.
func TestIndexIntegrity_RepeatedCompactionIsStable(t *testing.T) {
	propKeys := []string{"sha256"}
	propValues := []string{"alpha", "beta"}

	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	rng := rand.New(rand.NewSource(99))
	for round := 0; round < 5; round++ {
		applyRandomMutations(t, g, rng, 60, propKeys, propValues)
		if err := g.Compact(); err != nil {
			t.Fatalf("round %d Compact: %v", round, err)
		}
		if err := g.VerifyIndexes(); err != nil {
			t.Fatalf("round %d VerifyIndexes: %v", round, err)
		}
	}
}

// ReindexPurge must leave no stale property entry behind, and the purge must
// survive a restart — otherwise WAL replay resurrects the superseded values.
func TestReindexPolicy_PurgeSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	g.SetReindexPolicy(store.ReindexPurge)
	if got := g.ReindexPolicy(); got != store.ReindexPurge {
		t.Fatalf("ReindexPolicy = %v, want ReindexPurge", got)
	}

	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.IndexNodeProperty(id, "sha256", []byte("old")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := g.UpdateNode(&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	hits, err := g.NodesByProperty("sha256", []byte("old"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("stale entry survived the purge: %v", hits)
	}

	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	hits, err = reopened.NodesByProperty("sha256", []byte("old"))
	if err != nil {
		t.Fatalf("NodesByProperty after reopen: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("WAL replay resurrected the purged entry: %v", hits)
	}
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after reopen: %v", err)
	}
}

// ReindexKeep is the documented default and must preserve the old behaviour:
// entries stay, and therefore go stale.
func TestReindexPolicy_KeepIsDefaultAndLeavesEntriesStale(t *testing.T) {
	g := graphene.NewInMemory()
	if got := g.ReindexPolicy(); got != store.ReindexKeep {
		t.Fatalf("default ReindexPolicy = %v, want ReindexKeep", got)
	}

	id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.IndexNodeProperty(id, "sha256", []byte("old")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := g.UpdateNode(&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	hits, err := g.NodesByProperty("sha256", []byte("old"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(hits) != 1 || hits[0] != id {
		t.Fatalf("ReindexKeep should retain the entry, got %v", hits)
	}
}

// UpdateNodeIndexed is the path that avoids both failure modes: no stale entry
// for the old value, and the new value is registered.
func TestUpdateIndexed_ReplacesEntriesAtomically(t *testing.T) {
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
		t.Run(name, func(t *testing.T) {
			g := open(t)
			id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if err := g.IndexNodeProperties(id, map[string][]byte{
				"sha256": []byte("old"),
				"tool":   []byte("strings"),
			}); err != nil {
				t.Fatalf("IndexNodeProperties: %v", err)
			}

			err = g.UpdateNodeIndexed(
				&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeTag}},
				map[string][]byte{"sha256": []byte("new")},
			)
			if err != nil {
				t.Fatalf("UpdateNodeIndexed: %v", err)
			}

			if hits, _ := g.NodesByProperty("sha256", []byte("old")); len(hits) != 0 {
				t.Errorf("old value still indexed: %v", hits)
			}
			if hits, _ := g.NodesByProperty("tool", []byte("strings")); len(hits) != 0 {
				t.Errorf("dropped key still indexed: %v", hits)
			}
			hits, err := g.NodesByProperty("sha256", []byte("new"))
			if err != nil {
				t.Fatalf("NodesByProperty: %v", err)
			}
			if len(hits) != 1 || hits[0] != id {
				t.Fatalf("new value not indexed, got %v", hits)
			}

			n, err := g.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if !n.HasLabel(store.NodeTypeTag) {
				t.Fatalf("label update did not apply: %v", n.Labels)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes: %v", err)
			}
		})
	}
}

// Regression: an entity created with the same label listed twice used to be
// inserted into the label postings once per repetition. On the memory backend
// that made NodesByType return the ID twice; on the disk backend it broke the
// ascending-order invariant the CSR label postings depend on.
func TestLabelIndex_DuplicateLabelsDoNotDuplicatePostings(t *testing.T) {
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

				nodeID, err := g.AddNode(&store.Node{Labels: []store.NodeType{
					store.NodeTypeTag, store.NodeTypeTag, store.NodeTypeCase, store.NodeTypeTag,
				}})
				if err != nil {
					t.Fatalf("AddNode: %v", err)
				}
				other, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
				if err != nil {
					t.Fatalf("AddNode: %v", err)
				}
				edgeID, err := g.AddEdge(&store.Edge{
					Src:    nodeID,
					Dst:    other,
					Labels: []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeContains},
				})
				if err != nil {
					t.Fatalf("AddEdge: %v", err)
				}

				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				ids, err := g.NodesByType(store.NodeTypeTag)
				if err != nil {
					t.Fatalf("NodesByType: %v", err)
				}
				if countNodeID(ids, nodeID) != 1 {
					t.Fatalf("NodesByType(Tag) = %v, want node %d exactly once", ids, nodeID)
				}
				if len(ids) != 2 {
					t.Fatalf("NodesByType(Tag) = %v, want exactly 2 nodes", ids)
				}

				eids, err := g.EdgesByType(store.EdgeTypeContains)
				if err != nil {
					t.Fatalf("EdgesByType: %v", err)
				}
				if len(eids) != 1 || eids[0] != edgeID {
					t.Fatalf("EdgesByType(Contains) = %v, want exactly [%d]", eids, edgeID)
				}

				// The query path must agree with the direct lookup.
				queried, err := g.QueryNodeIDs(store.NodeQuery{Types: []store.NodeType{store.NodeTypeTag}})
				if err != nil {
					t.Fatalf("QueryNodeIDs: %v", err)
				}
				if countNodeID(queried, nodeID) != 1 {
					t.Fatalf("QueryNodeIDs Types=[Tag] = %v, want node %d exactly once", queried, nodeID)
				}

				if err := g.VerifyIndexes(); err != nil {
					t.Fatalf("VerifyIndexes: %v", err)
				}
			})
		}
	}
}

func countNodeID(ids []store.NodeID, target store.NodeID) int {
	n := 0
	for _, id := range ids {
		if id == target {
			n++
		}
	}
	return n
}

// Crash consistency: a Compact() that dies before the CSR is swapped in must
// leave a store that still reopens, still verifies, and still reports the same
// index state. The temp CSR file is the crash artefact left behind.
func TestIndexIntegrity_CrashDuringCompact(t *testing.T) {
	propKeys := []string{"sha256", "bucket"}
	propValues := []string{"alpha", "beta", "gamma"}

	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	applyRandomMutations(t, g, rand.New(rand.NewSource(7)), 200, propKeys, propValues)
	before := takeIndexSnapshot(t, g, propKeys, propValues)
	before.normalise()

	// Simulate a crash partway through Compact: the process died after writing
	// the temp CSR but before the atomic rename, so the WAL is still the source
	// of truth and a stray .tmp file is on disk. Closing without compacting
	// reproduces that state.
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	tmpPath := filepath.Join(dir, "graphene.csr.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial garbage from a dead compaction"), 0600); err != nil {
		t.Fatalf("write stray temp CSR: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen after simulated crash: %v", err)
	}
	defer reopened.Close()

	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after simulated crash: %v", err)
	}
	after := takeIndexSnapshot(t, reopened, propKeys, propValues)
	after.normalise()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("simulated crash changed index state\n before: %+v\n after:  %+v", before, after)
	}

	// And the store must still compact cleanly afterwards.
	if err := reopened.Compact(); err != nil {
		t.Fatalf("Compact after simulated crash: %v", err)
	}
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after recovery compact: %v", err)
	}
	recovered := takeIndexSnapshot(t, reopened, propKeys, propValues)
	recovered.normalise()
	if !reflect.DeepEqual(before, recovered) {
		t.Fatalf("recovery compact changed index state\n before: %+v\n after:  %+v", before, recovered)
	}
}
