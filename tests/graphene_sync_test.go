package graphene_test

import (
	"os"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Sync is the cheap durability point for individual writes, which are otherwise
// not forced to the platter until Compact or Close.

func TestSync_MakesSingleWritesDurable(t *testing.T) {
	dir, err := os.MkdirTemp("", "graphene-sync-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []store.NodeID
	for i := 0; i < 50; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := g.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Reopen *without* closing the first handle, simulating a process that died
	// after Sync returned. The records must be replayable from the WAL.
	g2, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()

	found, missing, err := g2.GetNodes(ids)
	if err != nil {
		t.Fatalf("GetNodes: %v", err)
	}
	if len(missing) != 0 || len(found) != len(ids) {
		t.Fatalf("after Sync: %d found, %d missing; want all %d", len(found), len(missing), len(ids))
	}
	g.Close()
}

// Sync must be usable on a backend that has no durability, so callers do not
// need to know which backend they hold.
func TestSync_IsANoOpInMemory(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Sync(); err != nil {
		t.Errorf("Sync on in-memory store should be a no-op, got %v", err)
	}
}

// Repeated syncs with no intervening writes must be harmless.
func TestSync_IsIdempotent(t *testing.T) {
	dir, _ := os.MkdirTemp("", "graphene-sync2-*")
	defer os.RemoveAll(dir)
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := g.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
}
