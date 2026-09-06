package disk

// Version retention: how deep a chain is allowed to get, and why it is not one.
//
// retainLocked keeps the version a reader at visibleEpoch can still see, because
// group commit leaves that epoch trailing for the whole of a transaction's
// durability wait and the lock is released across it. tests/graphene_visibility_test.go
// asserts the behaviour that depends on. This asserts the cost: retaining is
// bounded, and reverts to a single version as soon as nothing is in flight.
//
// A chain that grew per commit would be a leak — the delta layer holds every
// live record between compactions, so an unbounded chain is unbounded memory.

import (
	"testing"

	"github.com/aoiflux/graphene/store"
)

func nodeChainLen(s *Store, id store.NodeID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for v := s.cur().delta.nodes[id]; v != nil; v = v.prev {
		n++
	}
	return n
}

func retentionStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// A record updated through many transactions settles at two versions: the one
// being written and the one a reader at visibleEpoch is still entitled to.
func TestRetention_TransactionalChainStaysBounded(t *testing.T) {
	s := retentionStore(t)
	labels := []store.NodeType{store.NodeTypeCase}

	id, err := s.AddNode(&store.Node{Labels: labels, Properties: []byte("seed")})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	for i := range 200 {
		err := s.ApplyTransaction([]store.TxOp{{
			Kind: store.TxOpUpdateNode,
			Node: &store.Node{ID: id, Labels: labels, Properties: []byte{byte('a' + i%26)}},
		}})
		if err != nil {
			t.Fatalf("ApplyTransaction %d: %v", i, err)
		}
		if n := nodeChainLen(s, id); n > 2 {
			t.Fatalf("after %d transactions the version chain is %d deep, want at most 2", i+1, n)
		}
	}
}

// A single-record mutator advances both epochs under the lock, so the gap it
// opens is closed before anything can observe it and its chains still collapse
// to one version. That is the case the retention rule is written to keep cheap.
func TestRetention_SingleMutatorChainCollapses(t *testing.T) {
	s := retentionStore(t)
	labels := []store.NodeType{store.NodeTypeCase}

	id, err := s.AddNode(&store.Node{Labels: labels, Properties: []byte("seed")})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	for i := range 20 {
		if err := s.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte{byte('a' + i%26)}}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
	}
	if n := nodeChainLen(s, id); n != 1 {
		t.Errorf("version chain is %d deep after non-transactional updates, want 1", n)
	}
}

// An open snapshot still pins what it needs, which is the retention rule this
// one was added alongside rather than in place of.
func TestRetention_SnapshotStillPins(t *testing.T) {
	s := retentionStore(t)
	labels := []store.NodeType{store.NodeTypeCase}

	id, err := s.AddNode(&store.Node{Labels: labels, Properties: []byte("pinned")})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Close()

	for i := range 5 {
		if err := s.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
	}
	n, err := snap.GetNode(id)
	if err != nil {
		t.Fatalf("snapshot GetNode: %v", err)
	}
	if string(n.Properties) != "pinned" {
		t.Errorf("the snapshot moved: %q", n.Properties)
	}
}
