package disk

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// openFresh opens a new Store in a temp dir, fails the test on error.
func openFresh(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, dir
}

// addNode is a helper that adds a node and fatals on error.
func addNodeD(t *testing.T, s *Store, labels ...store.NodeType) store.NodeID {
	t.Helper()
	id, err := s.AddNode(&store.Node{Labels: labels})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	return id
}

// addEdge is a helper that adds an edge and fatals on error.
func addEdgeD(t *testing.T, s *Store, src, dst store.NodeID, labels ...store.EdgeType) store.EdgeID {
	t.Helper()
	eid, err := s.AddEdge(&store.Edge{Src: src, Dst: dst, Labels: labels})
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	return eid
}

// --- Basic CRUD ---

func TestDiskStore_AddGetNode(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	id := addNodeD(t, s, store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact)
	n, err := s.GetNode(id)
	if err != nil {
		t.Fatal(err)
	}
	if !n.HasLabel(store.NodeTypeEvidenceFile) || !n.HasLabel(store.NodeTypeMicroArtefact) {
		t.Errorf("expected both labels, got %v", n.Labels)
	}
}

func TestDiskStore_AddGetEdge(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	a := addNodeD(t, s, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeMicroArtefact)
	eid := addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	e, err := s.GetEdge(eid)
	if err != nil {
		t.Fatal(err)
	}
	if e.Src != a || e.Dst != b {
		t.Errorf("edge src/dst mismatch: %+v", e)
	}
	if !e.HasLabel(store.EdgeTypeSimilarTo) {
		t.Error("missing SimilarTo label")
	}
}

func TestDiskStore_GetNode_NotFound(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	_, err := s.GetNode(9999)
	var nf *store.ErrNotFound
	if !errors.As(err, &nf) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- WAL replay on restart ---

func TestDiskStore_Persist_Restart(t *testing.T) {
	dir := t.TempDir()

	// Write data.
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		b, _ := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		s.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Reopen and verify WAL replay.
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		nc, _ := s.NodeCount()
		ec, _ := s.EdgeCount()
		if nc != 2 {
			t.Errorf("after restart: NodeCount=%d, want 2", nc)
		}
		if ec != 1 {
			t.Errorf("after restart: EdgeCount=%d, want 1", ec)
		}
	}
}

// --- Compaction + restart ---

func TestDiskStore_Compact_Restart(t *testing.T) {
	dir := t.TempDir()

	var nodeID store.NodeID
	{
		s, _ := Open(dir)
		nodeID = addNodeD(t, s, store.NodeTypeCase)
		b := addNodeD(t, s, store.NodeTypeEvidenceFile)
		addEdgeD(t, s, nodeID, b, store.EdgeTypeContains)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	// After compact the CSR exists; WAL is truncated.
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatalf("reopen after compact: %v", err)
		}
		defer s.Close()

		n, err := s.GetNode(nodeID)
		if err != nil {
			t.Fatalf("GetNode after compact: %v", err)
		}
		if !n.HasLabel(store.NodeTypeCase) {
			t.Errorf("label missing after compact restart: %v", n.Labels)
		}
		nc, _ := s.NodeCount()
		ec, _ := s.EdgeCount()
		if nc != 2 || ec != 1 {
			t.Errorf("counts after compact restart: nodes=%d edges=%d", nc, ec)
		}
	}
}

// --- NodesByType merges CSR + delta ---

func TestDiskStore_NodesByType_MergesCsrDelta(t *testing.T) {
	dir := t.TempDir()

	s, _ := Open(dir)
	id1 := addNodeD(t, s, store.NodeTypeTag)
	id2 := addNodeD(t, s, store.NodeTypeTag)
	// Compact: id1 + id2 land in CSR.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	// Add a third node into the delta layer.
	id3 := addNodeD(t, s, store.NodeTypeTag)
	defer s.Close()

	ids, err := s.NodesByType(store.NodeTypeTag)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("NodesByType: got %d, want 3", len(ids))
	}
	have := map[store.NodeID]bool{ids[0]: true, ids[1]: true, ids[2]: true}
	for _, id := range []store.NodeID{id1, id2, id3} {
		if !have[id] {
			t.Errorf("id %d missing from NodesByType result", id)
		}
	}
}

// --- EdgesOf filter after compact ---

func TestDiskStore_EdgesOf_Filter(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	a := addNodeD(t, s, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeMicroArtefact)
	c := addNodeD(t, s, store.NodeTypeCase)
	addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	addEdgeD(t, s, a, c, store.EdgeTypeBelongsTo)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	edges, err := s.EdgesOf(a, store.DirectionOutbound, []store.EdgeType{store.EdgeTypeSimilarTo})
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 1 || !edges[0].HasLabel(store.EdgeTypeSimilarTo) {
		t.Errorf("EdgesOf filter after compact: %v", edges)
	}
}

// --- Property index persists through restart ---

func TestDiskStore_PropertyIndex_Persist(t *testing.T) {
	dir := t.TempDir()
	var id store.NodeID
	{
		s, _ := Open(dir)
		id = addNodeD(t, s, store.NodeTypeMicroArtefact)
		s.IndexNodeProperty(id, "sha256", []byte("deadbeef"))
		s.Close()
	}
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ids, err := s.NodesByProperty("sha256", []byte("deadbeef"))
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Errorf("NodesByProperty after restart: %v", ids)
		}
	}
}

func TestDiskStore_PropertyIndex_AfterCompact(t *testing.T) {
	dir := t.TempDir()
	var id store.NodeID
	{
		s, _ := Open(dir)
		id = addNodeD(t, s, store.NodeTypeMicroArtefact)
		s.IndexNodeProperty(id, "hash", []byte("cafebabe"))
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ids, err := s.NodesByProperty("hash", []byte("cafebabe"))
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != 1 || ids[0] != id {
			t.Errorf("NodesByProperty after compact+restart: %v", ids)
		}
	}
}

func TestDiskStore_NodeProperties_AfterCompact(t *testing.T) {
	dir := t.TempDir()
	var id store.NodeID
	wantProps := []byte{1, 2, 3, 4}
	{
		s, _ := Open(dir)
		id, _ = s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: wantProps,
		})
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		n, err := s.GetNode(id)
		if err != nil {
			t.Fatal(err)
		}
		if string(n.Properties) != string(wantProps) {
			t.Fatalf("GetNode after compact+restart properties = %v, want %v", n.Properties, wantProps)
		}
	}
}

func TestDiskStore_EdgeProperties_AfterCompact(t *testing.T) {
	dir := t.TempDir()
	var eid store.EdgeID
	wantProps := []byte("edge-meta")
	{
		s, _ := Open(dir)
		src := addNodeD(t, s, store.NodeTypeMicroArtefact)
		dst := addNodeD(t, s, store.NodeTypeCase)
		eid, _ = s.AddEdge(&store.Edge{
			Src:        src,
			Dst:        dst,
			Labels:     []store.EdgeType{store.EdgeTypeBelongsTo},
			Properties: wantProps,
		})
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	{
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		e, err := s.GetEdge(eid)
		if err != nil {
			t.Fatal(err)
		}
		if string(e.Properties) != string(wantProps) {
			t.Fatalf("GetEdge after compact+restart properties = %v, want %v", e.Properties, wantProps)
		}
	}
}

// --- CSR serialise/deserialise round-trip ---

func TestDiskStore_CSR_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	a := addNodeD(t, s, store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeCase)
	addEdgeD(t, s, a, b, store.EdgeTypeBelongsTo, store.EdgeTypeContains)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	n, err := s2.GetNode(a)
	if err != nil {
		t.Fatal(err)
	}
	if !n.HasLabel(store.NodeTypeEvidenceFile) || !n.HasLabel(store.NodeTypeMicroArtefact) {
		t.Errorf("multi-label node labels after CSR round-trip: %v", n.Labels)
	}

	e, err := s2.GetEdge(1)
	if err != nil {
		t.Fatal(err)
	}
	if !e.HasLabel(store.EdgeTypeBelongsTo) || !e.HasLabel(store.EdgeTypeContains) {
		t.Errorf("multi-label edge labels after CSR round-trip: %v", e.Labels)
	}
}

// --- Counts ---

func TestDiskStore_Counts(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	nc, _ := s.NodeCount()
	ec, _ := s.EdgeCount()
	if nc != 5 {
		t.Errorf("NodeCount: %d, want 5", nc)
	}
	if ec != 0 {
		t.Errorf("EdgeCount: %d, want 0", ec)
	}
}

func TestDiskStore_QueryNodeIDs_PropertyFilters(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	n1 := addNodeD(t, s, store.NodeTypeMicroArtefact)
	n2 := addNodeD(t, s, store.NodeTypeMicroArtefact)
	n3 := addNodeD(t, s, store.NodeTypeCase)

	s.IndexNodeProperty(n1, "score", []byte("45"))
	s.IndexNodeProperty(n2, "score", []byte("80"))
	s.IndexNodeProperty(n3, "score", []byte("90"))
	s.IndexNodeProperty(n1, "name", []byte("artefact-one"))
	s.IndexNodeProperty(n2, "name", []byte("artefact-two"))
	s.IndexNodeProperty(n3, "name", []byte("case-root"))

	hits, err := s.QueryNodeIDs(store.NodeQuery{
		Types: []store.NodeType{store.NodeTypeMicroArtefact},
		Filters: []store.PropertyFilter{
			{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: []byte("50"), ValueUpper: []byte("90")},
			{Key: "name", Op: store.PropertyOpContains, Value: []byte("artefact")},
		},
	})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if len(hits) != 1 || hits[0] != n2 {
		t.Fatalf("QueryNodeIDs: got %v, want [%d]", hits, n2)
	}
}

func TestDiskStore_QueryEdgeIDs_AfterCompact(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	a := addNodeD(t, s, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeMicroArtefact)
	c := addNodeD(t, s, store.NodeTypeMicroArtefact)
	e1 := addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	e2 := addEdgeD(t, s, c, b, store.EdgeTypeSimilarTo)
	s.IndexEdgeProperty(e1, "bucket", []byte("sim-high"))
	s.IndexEdgeProperty(e2, "bucket", []byte("sim-low"))

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

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

func TestDiskStore_QueryNodeIDs_Pagination(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}

	hits, err := s.QueryNodeIDs(store.NodeQuery{Offset: 2, Limit: 2})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if len(hits) != 2 || hits[0] != 3 || hits[1] != 4 {
		t.Fatalf("QueryNodeIDs pagination: got %v, want [3 4]", hits)
	}
}

func TestDiskStore_QueryEdgeIDs_Pagination_AfterCompact(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	a := addNodeD(t, s, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeMicroArtefact)
	e1 := addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	e2 := addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	_ = addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	hits, err := s.QueryEdgeIDs(store.EdgeQuery{Offset: 0, Limit: 2})
	if err != nil {
		t.Fatalf("QueryEdgeIDs: %v", err)
	}
	if len(hits) != 2 || hits[0] != e1 || hits[1] != e2 {
		t.Fatalf("QueryEdgeIDs pagination: got %v, want [%d %d]", hits, e1, e2)
	}
}

func TestDiskStore_QueryNodeIDs_OrderDesc(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}

	hits, err := s.QueryNodeIDs(store.NodeQuery{Order: store.QueryOrderDesc, Offset: 1, Limit: 2})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	if len(hits) != 2 || hits[0] != 4 || hits[1] != 3 {
		t.Fatalf("QueryNodeIDs descending pagination: got %v, want [4 3]", hits)
	}
}

func TestDiskStore_QueryEdgeIDs_OrderDesc_AfterCompact(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	a := addNodeD(t, s, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeMicroArtefact)
	_ = addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	_ = addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	e3 := addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	hits, err := s.QueryEdgeIDs(store.EdgeQuery{Order: store.QueryOrderDesc, Offset: 0, Limit: 1})
	if err != nil {
		t.Fatalf("QueryEdgeIDs: %v", err)
	}
	if len(hits) != 1 || hits[0] != e3 {
		t.Fatalf("QueryEdgeIDs descending pagination: got %v, want [%d]", hits, e3)
	}
}

func TestDiskStore_QueryNodeIDs_Combinators_TableDriven(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	a := addNodeD(t, s, store.NodeTypeMicroArtefact)
	b := addNodeD(t, s, store.NodeTypeMicroArtefact)
	c := addNodeD(t, s, store.NodeTypeMicroArtefact)

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

func TestDiskStore_QueryRegression_UnknownKeysAndEmptyFilters(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	n1 := addNodeD(t, s, store.NodeTypeMicroArtefact)
	n2 := addNodeD(t, s, store.NodeTypeMicroArtefact)
	e1 := addEdgeD(t, s, n1, n2, store.EdgeTypeSimilarTo)

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

func TestDiskStore_QueryRegression_MixedNumericEncodingsAndWindowBounds(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	ids := make([]store.NodeID, 0, 10)
	for i := 0; i < 10; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
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
