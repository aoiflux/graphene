package graphene_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

type querySnapshot struct {
	NodeRangePage []store.NodeID
	NodeRangeDesc []store.NodeID
	NodeAnyMatch  []store.NodeID
	EdgeBySrcType []store.EdgeID
	EdgeScorePage []store.EdgeID
	EdgeScoreDesc []store.EdgeID
	RelationBoth  []store.EdgeID
}

func TestQueryParity_MemoryVsDisk_DeltaAndCompacted(t *testing.T) {
	mem := graphene.NewInMemory()
	defer mem.Close()

	buildParityFixture(t, mem)
	want, err := runQuerySnapshot(mem)
	if err != nil {
		t.Fatalf("memory snapshot: %v", err)
	}

	dir := t.TempDir()
	diskDelta, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open disk delta: %v", err)
	}
	buildParityFixture(t, diskDelta)
	gotDelta, err := runQuerySnapshot(diskDelta)
	if err != nil {
		t.Fatalf("disk delta snapshot: %v", err)
	}
	if err := diskDelta.Close(); err != nil {
		t.Fatalf("close disk delta: %v", err)
	}

	if !reflect.DeepEqual(want, gotDelta) {
		t.Fatalf("memory vs disk (delta) mismatch:\nwant=%+v\ngot=%+v", want, gotDelta)
	}

	diskCompacted, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen disk for compact: %v", err)
	}
	if err := diskCompacted.Compact(); err != nil {
		t.Fatalf("compact disk: %v", err)
	}
	if err := diskCompacted.Close(); err != nil {
		t.Fatalf("close after compact: %v", err)
	}

	diskReopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen compacted disk: %v", err)
	}
	defer diskReopened.Close()

	gotCompacted, err := runQuerySnapshot(diskReopened)
	if err != nil {
		t.Fatalf("disk compacted snapshot: %v", err)
	}
	if !reflect.DeepEqual(want, gotCompacted) {
		t.Fatalf("memory vs disk (compacted) mismatch:\nwant=%+v\ngot=%+v", want, gotCompacted)
	}
}

func buildParityFixture(t *testing.T, g *graphene.Graph) {
	t.Helper()

	caseID, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
	if err != nil {
		t.Fatalf("add case: %v", err)
	}
	_ = caseID

	n1, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("add n1: %v", err)
	}
	n2, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("add n2: %v", err)
	}
	n3, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("add n3: %v", err)
	}
	n4, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
	if err != nil {
		t.Fatalf("add n4: %v", err)
	}

	mustIndexNode := func(id store.NodeID, key, value string) {
		t.Helper()
		if err := g.IndexNodeProperty(id, key, []byte(value)); err != nil {
			t.Fatalf("index node property %s=%s on %d: %v", key, value, id, err)
		}
	}

	mustIndexNode(n1, "size", "10")
	mustIndexNode(n2, "size", "25")
	mustIndexNode(n3, "size", "40")
	mustIndexNode(n4, "size", "99")
	mustIndexNode(n1, "name", "artefact-alpha")
	mustIndexNode(n2, "name", "artefact-beta")
	mustIndexNode(n3, "name", "artifact-gamma")
	mustIndexNode(n4, "name", "evidence-main")
	mustIndexNode(n1, "bucket", "bucket-001")
	mustIndexNode(n2, "bucket", "bucket-002")
	mustIndexNode(n3, "bucket", "bucket-003")
	mustIndexNode(n4, "bucket", "bucket-010")

	e1, err := g.AddEdge(&store.Edge{Src: n1, Dst: n2, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.81})
	if err != nil {
		t.Fatalf("add e1: %v", err)
	}
	e2, err := g.AddEdge(&store.Edge{Src: n2, Dst: n3, Labels: []store.EdgeType{store.EdgeTypeReuse}, Weight: 0})
	if err != nil {
		t.Fatalf("add e2: %v", err)
	}
	e3, err := g.AddEdge(&store.Edge{Src: n3, Dst: n1, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 0.67})
	if err != nil {
		t.Fatalf("add e3: %v", err)
	}
	e4, err := g.AddEdge(&store.Edge{Src: n1, Dst: n4, Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0})
	if err != nil {
		t.Fatalf("add e4: %v", err)
	}

	mustIndexEdge := func(id store.EdgeID, key, value string) {
		t.Helper()
		if err := g.IndexEdgeProperty(id, key, []byte(value)); err != nil {
			t.Fatalf("index edge property %s=%s on %d: %v", key, value, id, err)
		}
	}

	mustIndexEdge(e1, "kind", "near")
	mustIndexEdge(e2, "kind", "reuse")
	mustIndexEdge(e3, "kind", "near")
	mustIndexEdge(e4, "kind", "contains")
	mustIndexEdge(e1, "score", "81")
	mustIndexEdge(e2, "score", "50")
	mustIndexEdge(e3, "score", "67")
	mustIndexEdge(e4, "score", "20")
	mustIndexEdge(e1, "bucket", "sim-a")
	mustIndexEdge(e2, "bucket", "reuse-a")
	mustIndexEdge(e3, "bucket", "sim-b")
	mustIndexEdge(e4, "bucket", "cont-a")
}

func runQuerySnapshot(g *graphene.Graph) (*querySnapshot, error) {
	anchorIDs, err := g.QueryNodeIDs(store.NodeQuery{
		Filters: []store.PropertyFilter{{Key: "name", Op: store.PropertyOpEqual, Value: []byte("artefact-alpha")}},
		Limit:   1,
	})
	if err != nil {
		return nil, err
	}
	if len(anchorIDs) != 1 {
		return nil, fmt.Errorf("expected one anchor node for artefact-alpha, got %d", len(anchorIDs))
	}
	anchorID := anchorIDs[0]

	reuseSrcIDs, err := g.QueryNodeIDs(store.NodeQuery{
		Filters: []store.PropertyFilter{{Key: "name", Op: store.PropertyOpEqual, Value: []byte("artefact-beta")}},
		Limit:   1,
	})
	if err != nil {
		return nil, err
	}
	if len(reuseSrcIDs) != 1 {
		return nil, fmt.Errorf("expected one source node for artefact-beta, got %d", len(reuseSrcIDs))
	}
	reuseSrcID := reuseSrcIDs[0]

	nodeRangePage, err := g.QueryNodeIDs(store.NodeQuery{
		Types: []store.NodeType{store.NodeTypeMicroArtefact},
		Filters: []store.PropertyFilter{
			{Key: "size", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("10")},
		},
		Offset: 1,
		Limit:  2,
	})
	if err != nil {
		return nil, err
	}

	nodeAnyMatch, err := g.QueryNodeIDs(store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "name", Op: store.PropertyOpContains, Value: []byte("artefact")},
			{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-01")},
		},
		FilterMode: store.MatchAny,
	})
	if err != nil {
		return nil, err
	}

	nodeRangeDesc, err := g.QueryNodeIDs(store.NodeQuery{
		Types: []store.NodeType{store.NodeTypeMicroArtefact},
		Filters: []store.PropertyFilter{
			{Key: "size", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("10")},
		},
		Order:  store.QueryOrderDesc,
		Offset: 0,
		Limit:  2,
	})
	if err != nil {
		return nil, err
	}

	edgeBySrcType, err := g.QueryEdgeIDs(store.EdgeQuery{
		SrcIDs: []store.NodeID{reuseSrcID},
		Types:  []store.EdgeType{store.EdgeTypeReuse},
	})
	if err != nil {
		return nil, err
	}

	edgeScorePage, err := g.QueryEdgeIDs(store.EdgeQuery{
		Filters: []store.PropertyFilter{
			{Key: "score", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("50")},
		},
		Offset: 0,
		Limit:  2,
	})
	if err != nil {
		return nil, err
	}

	edgeScoreDesc, err := g.QueryEdgeIDs(store.EdgeQuery{
		Filters: []store.PropertyFilter{
			{Key: "score", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("50")},
		},
		Order:  store.QueryOrderDesc,
		Offset: 0,
		Limit:  2,
	})
	if err != nil {
		return nil, err
	}

	rels, err := g.QueryRelations(store.RelationQuery{
		Anchors:   []store.NodeID{anchorID},
		Direction: store.DirectionBoth,
		EdgeTypes: []store.EdgeType{store.EdgeTypeSimilarTo},
		Filters: []store.PropertyFilter{
			{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("near")},
		},
	})
	if err != nil {
		return nil, err
	}
	relationBoth := make([]store.EdgeID, 0, len(rels))
	for _, e := range rels {
		relationBoth = append(relationBoth, e.ID)
	}
	sort.Slice(relationBoth, func(i, j int) bool { return relationBoth[i] < relationBoth[j] })

	return &querySnapshot{
		NodeRangePage: nodeRangePage,
		NodeRangeDesc: nodeRangeDesc,
		NodeAnyMatch:  nodeAnyMatch,
		EdgeBySrcType: edgeBySrcType,
		EdgeScorePage: edgeScorePage,
		EdgeScoreDesc: edgeScoreDesc,
		RelationBoth:  relationBoth,
	}, nil
}
