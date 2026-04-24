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
