package disk

// A tracked transaction's reads and the validation of those reads have to come
// from the same view. store.AppliedReader is that view; this pins the property
// that makes it necessary.
//
// The window it covers is real but narrow in wall-clock terms — it is open only
// while a commit waits for its fsync — so a test that raced for it would pass
// on a fast disk and fail on a loaded one. Instead this constructs the state
// directly: a record applied at a newer epoch than the one plain readers are
// allowed to see, which is exactly what group commit produces between apply and
// publish.

import (
	"testing"

	"github.com/aoiflux/graphene/store"
)

func TestAppliedReader_SeesWhatAPlainReadCannot(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	labels := []store.NodeType{store.NodeTypeCase}
	id, err := s.AddNode(&store.Node{Labels: labels, Properties: []byte("before")})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	held := s.visibleEpoch.Load()

	// One unpublished commit, which is what makes retainLocked keep the version
	// a reader at the held epoch is still entitled to. Without it the update
	// below would truncate the chain and the plain read would find nothing,
	// which is a different bug with its own test.
	s.unpublished.Add(1)
	defer s.unpublished.Add(-1)

	if err := s.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte("after")}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	// UpdateNode published its own epoch on the way out. Put visibility back
	// where it was, which is where a commit still waiting on its fsync leaves it.
	s.visibleEpoch.Store(held)

	if got, err := s.GetNode(id); err != nil {
		t.Fatalf("GetNode: %v", err)
	} else if string(got.Properties) != "before" {
		t.Fatalf("a plain read sees %q; the window this test needs is not open", got.Properties)
	}

	n, err := s.AppliedNode(id)
	if err != nil {
		t.Fatalf("AppliedNode: %v", err)
	}
	if string(n.Properties) != "after" {
		t.Errorf("AppliedNode = %q, want %q — a tracked read is taking the view its own "+
			"validation will refuse, which is the livelock", n.Properties, "after")
	}

	// The validator resolves through the same reader, so the digest a tracked
	// read records must be the one validation computes. Comparing them here is
	// what ties the two halves together: either could be moved to the other
	// view on its own and this is what would notice.
	s.mu.Lock()
	err = s.validateReadsLocked([]store.ReadCheck{{
		Kind:   store.ReadNode,
		ID:     uint64(id),
		Exists: true,
		Digest: store.NodeDigest(n),
	}})
	s.mu.Unlock()
	if err != nil {
		t.Errorf("validating what AppliedNode returned: %v", err)
	}
}

// The edge and adjacency readers are on the same view for the same reason, and
// a read set mixing views would be refused just as surely as one taken entirely
// from the old one.
func TestAppliedReader_EdgeAndAdjacencyAgreeWithValidation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	labels := []store.NodeType{store.NodeTypeCase}
	src, err := s.AddNode(&store.Node{Labels: labels})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	dst, err := s.AddNode(&store.Node{Labels: labels})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	eid, err := s.AddEdge(&store.Edge{Src: src, Dst: dst, Labels: []store.EdgeType{store.EdgeTypeContains}})
	if err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	e, err := s.AppliedEdge(eid)
	if err != nil {
		t.Fatalf("AppliedEdge: %v", err)
	}
	inc, err := s.AppliedEdgesOf(src, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatalf("AppliedEdgesOf: %v", err)
	}
	if len(inc) != 1 || inc[0].ID != eid {
		t.Fatalf("AppliedEdgesOf = %v, want just edge %d", inc, eid)
	}

	s.mu.Lock()
	err = s.validateReadsLocked([]store.ReadCheck{
		{Kind: store.ReadEdge, ID: uint64(eid), Exists: true, Digest: store.EdgeDigest(e)},
		{Kind: store.ReadNodeAdjacency, ID: uint64(src), Digest: store.EdgeSetDigest(inc), Dir: store.DirectionOutbound},
	})
	s.mu.Unlock()
	if err != nil {
		t.Errorf("validating what the applied readers returned: %v", err)
	}
}
