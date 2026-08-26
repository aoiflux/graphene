package graphene_test

import (
	"os"
	"path/filepath"
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

	// What this has to prove is that the bytes on the platter *at the moment Sync
	// returned* replay to every record — not merely that Close is durable, which
	// would be true even if Sync did nothing at all.
	//
	// It used to do that by reopening the same directory without closing the
	// first handle, simulating a process that died after Sync. A store now takes
	// an exclusive process lock, so that simulation is no longer available — and
	// it was never quite the thing it claimed to be, since a live handle is not a
	// dead process. Copying the directory mid-flight is: the copy is exactly what
	// a crash would have left behind, and opening it exercises the same replay
	// against a store the first handle has never touched.
	replica := t.TempDir()
	copyStoreDir(t, dir, replica)

	g2, err := graphene.Open(replica)
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

// copyStoreDir snapshots a live store directory, the way a crash would leave it.
//
// The lock file is skipped: it is coordination state belonging to the process
// that is still running, and copying it would hand the replica a stale owner
// record naming a live PID.
func copyStoreDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("snapshot %s: %v", src, err)
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == "graphene.lock" {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(src, e.Name()))
		if rerr != nil {
			t.Fatalf("snapshot %s: %v", e.Name(), rerr)
		}
		if werr := os.WriteFile(filepath.Join(dst, e.Name()), data, 0600); werr != nil {
			t.Fatalf("snapshot %s: %v", e.Name(), werr)
		}
	}
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
