package graphene_test

// Concurrent reads against a committing writer.
//
// The disk backend publishes a transaction's epoch only after the fsync that
// covers it, and deliberately releases the store lock before that wait so
// concurrent commits share one sync. For the length of that window every plain
// reader is running an epoch behind what the delta layer already holds — and a
// reader that is behind still has to be *right*.
//
// It was not. A writer truncated each version chain to its newest entry
// whenever no snapshot was open, and dropped the superseded label postings on
// the same condition, so a record updated by a transaction disappeared entirely
// for the duration of that transaction's durability wait: not stale, missing.
// Both halves are asserted here because they were one bug with one cause and
// would come back as one.
//
// These run on both backends. The in-memory store has a single lock and no
// visibility gap, so it passes trivially — which is the point: what a caller may
// assume must not depend on which backend answers.

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// hammerCommits runs read against four goroutines while one commits updates to
// a single node through a transaction, and reports how many reads went wrong.
func hammerCommits(t *testing.T, g *graphene.Graph, read func() bool) int64 {
	t.Helper()

	labels := []store.NodeType{store.NodeTypeCase}
	id, err := g.AddNode(&store.Node{Labels: labels, Properties: []byte("seed")})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	var wrong atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := range 400 {
			tx := g.Begin()
			tx.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte{byte('a' + i%26)}})
			if err := tx.Commit(); err != nil {
				t.Errorf("Commit: %v", err)
				return
			}
		}
	}()

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if !read() {
					wrong.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return wrong.Load()
}

// A node that exists throughout must never read as missing.
func TestVisibility_CommittingWriterNeverHidesALiveNode(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		wrong := hammerCommits(t, g, func() bool {
			ids, err := g.NodesByType(store.NodeTypeCase)
			if err != nil || len(ids) != 1 {
				return false
			}
			_, err = g.GetNode(ids[0])
			return err == nil
		})
		if wrong > 0 {
			t.Errorf("a live node read as missing %d times while a transaction was committing", wrong)
		}
	})
}

// The same node must stay in its label's posting. The record and the posting
// are separate structures with separate retention rules, so one being right
// says nothing about the other.
func TestVisibility_CommittingWriterNeverHidesALabelPosting(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		wrong := hammerCommits(t, g, func() bool {
			ids, err := g.NodesByType(store.NodeTypeCase)
			return err == nil && len(ids) == 1
		})
		if wrong > 0 {
			t.Errorf("NodesByType lost a live node %d times while a transaction was committing", wrong)
		}
	})
}

// Whatever a read sees, it must be a whole version. A reader an epoch behind
// should see the previous properties or the new ones, never neither.
func TestVisibility_ReadsSeeSomeVersionNotNone(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		wrong := hammerCommits(t, g, func() bool {
			ids, err := g.NodesByType(store.NodeTypeCase)
			if err != nil || len(ids) != 1 {
				return false
			}
			n, err := g.GetNode(ids[0])
			if err != nil {
				return false
			}
			return len(n.Properties) > 0 && len(n.Labels) == 1
		})
		if wrong > 0 {
			t.Errorf("a read returned an incomplete record %d times", wrong)
		}
	})
}

// A snapshot taken while transactions commit still sees one fixed graph. The
// retention floor moved to cover plain readers; it must not have stopped
// covering the readers it already covered.
func TestVisibility_SnapshotStillPinsItsGraph(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		labels := []store.NodeType{store.NodeTypeCase}
		id, err := g.AddNode(&store.Node{Labels: labels, Properties: []byte("pinned")})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		snap, err := g.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		defer snap.Close()

		for i := range 50 {
			tx := g.Begin()
			tx.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte{byte('a' + i%26)}})
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
		}

		n, err := snap.GetNode(id)
		if err != nil {
			t.Fatalf("snapshot GetNode: %v", err)
		}
		if string(n.Properties) != "pinned" {
			t.Errorf("the snapshot moved: properties = %q, want %q", n.Properties, "pinned")
		}
		ids, err := snap.NodesByType(store.NodeTypeCase)
		if err != nil || len(ids) != 1 || ids[0] != id {
			t.Errorf("snapshot NodesByType = %v (err %v), want just %d", ids, err, id)
		}
	})
}
