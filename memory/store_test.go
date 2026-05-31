package memory

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// helpers

func newNode(labels ...store.NodeType) *store.Node {
	return &store.Node{Labels: labels}
}

func newEdge(src, dst store.NodeID, labels ...store.EdgeType) *store.Edge {
	return &store.Edge{Src: src, Dst: dst, Labels: labels}
}

// --- AddNode / GetNode ---

func TestStore_AddGetNode(t *testing.T) {
	s := New()
	id, err := s.AddNode(newNode(store.NodeTypeCase))
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if id == store.InvalidNodeID {
		t.Fatal("AddNode returned InvalidNodeID")
	}
	n, err := s.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !n.HasLabel(store.NodeTypeCase) {
		t.Error("stored node missing Case label")
	}
}

func TestStore_GetNode_NotFound(t *testing.T) {
	s := New()
	_, err := s.GetNode(99)
	var nf *store.ErrNotFound
	if !errors.As(err, &nf) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_AddNode_MultiLabel(t *testing.T) {
	s := New()
	id, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact))
	n, _ := s.GetNode(id)
	if !n.HasLabel(store.NodeTypeEvidenceFile) || !n.HasLabel(store.NodeTypeMicroArtefact) {
		t.Error("multi-label node not stored correctly")
	}
}

func TestStore_AddNode_CopiesLabels(t *testing.T) {
	lbls := []store.NodeType{store.NodeTypeCase}
	n := &store.Node{Labels: lbls}
	s := New()
	id, _ := s.AddNode(n)
	lbls[0] = store.NodeTypeTag // mutate caller's slice
	stored, _ := s.GetNode(id)
	if stored.Labels[0] != store.NodeTypeCase {
		t.Error("AddNode should copy label slice; original mutation leaked in")
	}
}

// --- AddEdge / GetEdge ---

func TestStore_AddGetEdge(t *testing.T) {
	s := New()
	nid1, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	nid2, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	eid, err := s.AddEdge(newEdge(nid1, nid2, store.EdgeTypeSimilarTo))
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	e, err := s.GetEdge(eid)
	if err != nil {
		t.Fatalf("GetEdge: %v", err)
	}
	if e.Src != nid1 || e.Dst != nid2 {
		t.Error("edge src/dst mismatch")
	}
	if !e.HasLabel(store.EdgeTypeSimilarTo) {
		t.Error("edge missing SimilarTo label")
	}
}

func TestStore_AddEdge_InvalidSrc(t *testing.T) {
	s := New()
	nid, _ := s.AddNode(newNode(store.NodeTypeCase))
	_, err := s.AddEdge(newEdge(99, nid, store.EdgeTypeBelongsTo))
	var inv *store.ErrInvalidEdge
	if !errors.As(err, &inv) {
		t.Errorf("expected ErrInvalidEdge for invalid src, got %v", err)
	}
}

func TestStore_AddEdge_InvalidDst(t *testing.T) {
	s := New()
	nid, _ := s.AddNode(newNode(store.NodeTypeCase))
	_, err := s.AddEdge(newEdge(nid, 99, store.EdgeTypeBelongsTo))
	var inv *store.ErrInvalidEdge
	if !errors.As(err, &inv) {
		t.Errorf("expected ErrInvalidEdge for invalid dst, got %v", err)
	}
}

func TestStore_AddEdge_MultiLabel(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	eid, _ := s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo, store.EdgeTypeReuse))
	e, _ := s.GetEdge(eid)
	if !e.HasLabel(store.EdgeTypeSimilarTo) || !e.HasLabel(store.EdgeTypeReuse) {
		t.Error("multi-label edge not stored correctly")
	}
}

// --- Neighbours / EdgesOf ---

func TestStore_Neighbours_Outbound(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))
	s.AddEdge(newEdge(a, c, store.EdgeTypeContains))

	nbs, err := s.Neighbours(a, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nbs) != 2 {
		t.Errorf("expected 2 outbound neighbours, got %d", len(nbs))
	}
}

func TestStore_Neighbours_Inbound(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))

	nbs, _ := s.Neighbours(b, store.DirectionInbound, nil)
	if len(nbs) != 1 || nbs[0].Node.ID != a {
		t.Errorf("inbound neighbour should be a, got %v", nbs)
	}
}

func TestStore_EdgesOf_TypeFilter(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeCase))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))
	s.AddEdge(newEdge(a, c, store.EdgeTypeBelongsTo))

	edges, _ := s.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeContains})
	if len(edges) != 1 || !edges[0].HasLabel(store.EdgeTypeContains) {
		t.Errorf("type filter failed: got %v", edges)
	}
}

func TestStore_EdgesOf_MultiLabelFilter_OR(t *testing.T) {
	// An edge with labels [SimilarTo, Reuse] should be returned when filtering
	// by either SimilarTo or Reuse (OR semantics).
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo, store.EdgeTypeReuse))

	edges, _ := s.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeSimilarTo})
	if len(edges) != 1 {
		t.Errorf("OR filter: expected 1 edge via SimilarTo filter, got %d", len(edges))
	}
	edges, _ = s.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeReuse})
	if len(edges) != 1 {
		t.Errorf("OR filter: expected 1 edge via Reuse filter, got %d", len(edges))
	}
}

// --- NodesByType / EdgesByType ---

func TestStore_NodesByType(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeCase))
	b, _ := s.AddNode(newNode(store.NodeTypeCase, store.NodeTypeEvidenceFile))
	s.AddNode(newNode(store.NodeTypeMicroArtefact))

	ids, _ := s.NodesByType(store.NodeTypeCase)
	if len(ids) != 2 {
		t.Fatalf("NodesByType(Case): got %v, want 2 ids", ids)
	}
	set := map[store.NodeID]bool{a: true, b: true}
	for _, id := range ids {
		if !set[id] {
			t.Errorf("unexpected id %d", id)
		}
	}
}

func TestStore_EdgesByType(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeCase))
	e1, _ := s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	s.AddEdge(newEdge(a, c, store.EdgeTypeBelongsTo))

	ids, _ := s.EdgesByType(store.EdgeTypeSimilarTo)
	if len(ids) != 1 || ids[0] != e1 {
		t.Errorf("EdgesByType(SimilarTo): got %v, want [%d]", ids, e1)
	}
}

// --- Counts ---

func TestStore_Counts(t *testing.T) {
	s := New()
	s.AddNode(newNode(store.NodeTypeCase))
	a, _ := s.AddNode(newNode(store.NodeTypeEvidenceFile))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.AddEdge(newEdge(a, b, store.EdgeTypeContains))

	nc, _ := s.NodeCount()
	ec, _ := s.EdgeCount()
	if nc != 3 {
		t.Errorf("NodeCount = %d, want 3", nc)
	}
	if ec != 1 {
		t.Errorf("EdgeCount = %d, want 1", ec)
	}
}

// --- Property index ---

func TestStore_PropertyIndex(t *testing.T) {
	s := New()
	id, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	s.IndexNodeProperty(id, "sha256", []byte("deadbeef"))

	ids, err := s.NodesByProperty("sha256", []byte("deadbeef"))
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Errorf("NodesByProperty: got %v %v", ids, err)
	}

	ids, _ = s.NodesByProperty("sha256", []byte("notfound"))
	if len(ids) != 0 {
		t.Errorf("expected no results for missing value, got %v", ids)
	}
}

func TestStore_EdgePropertyIndex(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	eid, _ := s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	s.IndexEdgeProperty(eid, "bucket", []byte("high"))

	ids, _ := s.EdgesByProperty("bucket", []byte("high"))
	if len(ids) != 1 || ids[0] != eid {
		t.Errorf("EdgesByProperty: got %v", ids)
	}
}

func TestStore_QueryNodeIDs_PropertyFilters(t *testing.T) {
	s := New()
	n1, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	n2, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	n3, _ := s.AddNode(newNode(store.NodeTypeCase))

	s.IndexNodeProperty(n1, "size", []byte("12"))
	s.IndexNodeProperty(n2, "size", []byte("30"))
	s.IndexNodeProperty(n3, "size", []byte("90"))
	s.IndexNodeProperty(n1, "name", []byte("artefact-alpha"))
	s.IndexNodeProperty(n2, "name", []byte("artefact-beta"))
	s.IndexNodeProperty(n3, "name", []byte("case-main"))

	hits, err := s.QueryNodeIDs(store.NodeQuery{
		Types: []store.NodeType{store.NodeTypeMicroArtefact},
		Filters: []store.PropertyFilter{
			{Key: "size", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("20")},
			{Key: "name", Op: store.PropertyOpContains, Value: []byte("artefact")},
		},
		FilterMode: store.MatchAll,
	})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if len(hits) != 1 || hits[0] != n2 {
		t.Fatalf("QueryNodeIDs: got %v, want [%d]", hits, n2)
	}
}

func TestStore_QueryEdgeIDs_EndpointAndPrefix(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	e1, _ := s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	e2, _ := s.AddEdge(newEdge(c, b, store.EdgeTypeSimilarTo))
	s.IndexEdgeProperty(e1, "bucket", []byte("sim-high"))
	s.IndexEdgeProperty(e2, "bucket", []byte("sim-low"))

	hits, err := s.QueryEdgeIDs(store.EdgeQuery{
		Types:  []store.EdgeType{store.EdgeTypeSimilarTo},
		SrcIDs: []store.NodeID{a},
		Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("sim-h")},
		},
	})
	if err != nil {
		t.Fatalf("QueryEdgeIDs: %v", err)
	}
	if len(hits) != 1 || hits[0] != e1 {
		t.Fatalf("QueryEdgeIDs: got %v, want [%d]", hits, e1)
	}
}

func TestStore_QueryNodeIDs_Pagination(t *testing.T) {
	s := New()
	for i := 0; i < 5; i++ {
		_, _ = s.AddNode(newNode(store.NodeTypeMicroArtefact))
	}

	hits, err := s.QueryNodeIDs(store.NodeQuery{Offset: 1, Limit: 2})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("QueryNodeIDs: got %d hits, want 2", len(hits))
	}
	if hits[0] != 2 || hits[1] != 3 {
		t.Fatalf("QueryNodeIDs pagination: got %v, want [2 3]", hits)
	}
}

func TestStore_QueryEdgeIDs_Pagination(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	_, _ = s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	_, _ = s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	_, _ = s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))

	hits, err := s.QueryEdgeIDs(store.EdgeQuery{Offset: 1, Limit: 1})
	if err != nil {
		t.Fatalf("QueryEdgeIDs: %v", err)
	}
	if len(hits) != 1 || hits[0] != 2 {
		t.Fatalf("QueryEdgeIDs pagination: got %v, want [2]", hits)
	}
}

func TestStore_QueryNodeIDs_OrderDesc(t *testing.T) {
	s := New()
	for i := 0; i < 5; i++ {
		_, _ = s.AddNode(newNode(store.NodeTypeMicroArtefact))
	}

	hits, err := s.QueryNodeIDs(store.NodeQuery{Order: store.QueryOrderDesc, Offset: 1, Limit: 2})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if len(hits) != 2 || hits[0] != 4 || hits[1] != 3 {
		t.Fatalf("QueryNodeIDs descending pagination: got %v, want [4 3]", hits)
	}
}

func TestStore_QueryEdgeIDs_OrderDesc(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	_, _ = s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	_, _ = s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))
	_, _ = s.AddEdge(newEdge(a, b, store.EdgeTypeSimilarTo))

	hits, err := s.QueryEdgeIDs(store.EdgeQuery{Order: store.QueryOrderDesc, Offset: 0, Limit: 2})
	if err != nil {
		t.Fatalf("QueryEdgeIDs: %v", err)
	}
	if len(hits) != 2 || hits[0] != 3 || hits[1] != 2 {
		t.Fatalf("QueryEdgeIDs descending pagination: got %v, want [3 2]", hits)
	}
}

func TestStore_QueryNodeIDs_Combinators_TableDriven(t *testing.T) {
	s := New()
	a, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	b, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	c, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))

	s.IndexNodeProperty(a, "family", []byte("artefact"))
	s.IndexNodeProperty(a, "bucket", []byte("bucket-001"))
	s.IndexNodeProperty(b, "family", []byte("artifact"))
	s.IndexNodeProperty(b, "bucket", []byte("bucket-001"))
	s.IndexNodeProperty(c, "family", []byte("case"))
	s.IndexNodeProperty(c, "bucket", []byte("bucket-999"))

	tests := []struct {
		name string
		q    store.NodeQuery
		want []store.NodeID
	}{
		{
			name: "match all",
			q: store.NodeQuery{
				Filters: []store.PropertyFilter{
					{Key: "family", Op: store.PropertyOpContains, Value: []byte("arte")},
					{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-00")},
				},
				FilterMode: store.MatchAll,
			},
			want: []store.NodeID{a},
		},
		{
			name: "match any",
			q: store.NodeQuery{
				Filters: []store.PropertyFilter{
					{Key: "family", Op: store.PropertyOpContains, Value: []byte("arte")},
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-999")},
				},
				FilterMode: store.MatchAny,
			},
			want: []store.NodeID{a, c},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.QueryNodeIDs(tc.q)
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("QueryNodeIDs(%s): got %v, want %v", tc.name, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("QueryNodeIDs(%s): got %v, want %v", tc.name, got, tc.want)
				}
			}
		})
	}
}

func TestStore_QueryRegression_UnknownKeysAndEmptyFilters(t *testing.T) {
	s := New()
	n1, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	n2, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
	e1, _ := s.AddEdge(newEdge(n1, n2, store.EdgeTypeSimilarTo))

	s.IndexNodeProperty(n1, "name", []byte("a"))
	s.IndexEdgeProperty(e1, "kind", []byte("near"))

	nodesAll, err := s.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("QueryNodeIDs empty: %v", err)
	}
	if len(nodesAll) != 2 {
		t.Fatalf("QueryNodeIDs empty: got %d, want 2", len(nodesAll))
	}

	nodesMissing, err := s.QueryNodeIDs(store.NodeQuery{Filters: []store.PropertyFilter{{Key: "missing", Op: store.PropertyOpEqual, Value: []byte("x")}}})
	if err != nil {
		t.Fatalf("QueryNodeIDs missing key: %v", err)
	}
	if len(nodesMissing) != 0 {
		t.Fatalf("QueryNodeIDs missing key: got %v, want []", nodesMissing)
	}

	edgesMissing, err := s.QueryEdgeIDs(store.EdgeQuery{Filters: []store.PropertyFilter{{Key: "missing", Op: store.PropertyOpEqual, Value: []byte("x")}}})
	if err != nil {
		t.Fatalf("QueryEdgeIDs missing key: %v", err)
	}
	if len(edgesMissing) != 0 {
		t.Fatalf("QueryEdgeIDs missing key: got %v, want []", edgesMissing)
	}
}

func TestStore_QueryRegression_MixedNumericEncodingsAndWindowBounds(t *testing.T) {
	s := New()
	ids := make([]store.NodeID, 0, 10)
	for i := 0; i < 10; i++ {
		id, _ := s.AddNode(newNode(store.NodeTypeMicroArtefact))
		ids = append(ids, id)
	}

	s.IndexNodeProperty(ids[0], "score", []byte("2"))
	s.IndexNodeProperty(ids[1], "score", []byte("002"))
	s.IndexNodeProperty(ids[2], "score", []byte("10"))

	hits, err := s.QueryNodeIDs(store.NodeQuery{
		Filters: []store.PropertyFilter{{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("2"), ValueUpper: []byte("2")}},
	})
	if err != nil {
		t.Fatalf("QueryNodeIDs mixed numeric encodings: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("QueryNodeIDs mixed numeric encodings: got %v, want 2 hits", hits)
	}

	pageTail, err := s.QueryNodeIDs(store.NodeQuery{Offset: 9, Limit: 5})
	if err != nil {
		t.Fatalf("QueryNodeIDs tail window: %v", err)
	}
	if len(pageTail) != 1 || pageTail[0] != ids[9] {
		t.Fatalf("QueryNodeIDs tail window: got %v, want [%d]", pageTail, ids[9])
	}

	pagePastEnd, err := s.QueryNodeIDs(store.NodeQuery{Offset: 20, Limit: 5})
	if err != nil {
		t.Fatalf("QueryNodeIDs past-end window: %v", err)
	}
	if len(pagePastEnd) != 0 {
		t.Fatalf("QueryNodeIDs past-end window: got %v, want []", pagePastEnd)
	}
}

// --- Close ---

func TestStore_Close(t *testing.T) {
	s := New()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
