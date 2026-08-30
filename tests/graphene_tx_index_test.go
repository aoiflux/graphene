package graphene_test

// Property-index registration inside a transaction.
//
// Before this, indexing was a separate call that a transaction did not buffer,
// and the documentation instructed callers to index *after* the commit. The
// window that opens between those two steps is narrow and its consequence is
// not: a crash inside it leaves nodes whose records are correct and whose keys
// resolve to nothing. Such a node cannot be found again by the key it was
// written under, so the next ingest of the same source writes a second one — and
// the duplicate, not the crash, is what is left behind.
//
// The property these tests pin is one sentence: a node and the index entries
// registered with it in the same transaction become visible together, or neither
// does — at every byte at which the log could have been cut.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func TestTx_IndexPropertiesAreVisibleAfterCommit(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		key := []byte("ver:9f3a")

		tx := g.Begin()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		tx.IndexNodeProperties(id, map[string][]byte{"k": key, "state": []byte("extracted")})
		eid := tx.AddEdge(&store.Edge{Src: id, Dst: id, Labels: []store.EdgeType{store.EdgeTypeContains}})
		tx.IndexEdgeProperties(eid, map[string][]byte{"rel": []byte("self")})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		got, err := g.NodesByProperty("k", key)
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(got) != 1 || got[0] != id {
			t.Fatalf("NodesByProperty returned %v, want [%d]", got, id)
		}
		gotE, err := g.EdgesByProperty("rel", []byte("self"))
		if err != nil {
			t.Fatalf("EdgesByProperty: %v", err)
		}
		if len(gotE) != 1 || gotE[0] != eid {
			t.Fatalf("EdgesByProperty returned %v, want [%d]", gotE, eid)
		}
	})
}

// Entries are registered against the node the transaction is creating, so the
// resolver has to see the transaction's own pending nodes and not just the
// store's. It must equally refuse an id that names nothing.
func TestTx_IndexPropertiesRejectsAnUnknownEntity(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		tx := g.Begin()
		tx.IndexNodeProperties(store.NodeID(4242), map[string][]byte{"k": []byte("v")})
		var nf *store.ErrNotFound
		if err := tx.Commit(); !errors.As(err, &nf) {
			t.Fatalf("indexing a non-existent node: got %v, want ErrNotFound", err)
		}
	})
}

// A node deleted later in the same transaction cannot carry entries out of it.
func TestTx_IndexThenDeleteLeavesNothingIndexed(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		tx := g.Begin()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		tx.IndexNodeProperties(id, map[string][]byte{"k": []byte("doomed")})
		tx.DeleteNode(id)
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		got, err := g.NodesByProperty("k", []byte("doomed"))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("a deleted node is still indexed: %v", got)
		}
	})
}

// Two transactions writing the same records and the same entries must produce
// the same log bytes. Index entries arrive as a map, and map iteration is
// randomised, so framing them in encounter order would make a transaction's
// bytes depend on nothing the caller can see.
func TestTx_IndexFramingIsDeterministic(t *testing.T) {
	logOf := func(t *testing.T) []byte {
		t.Helper()
		dir := t.TempDir()
		g, err := graphene.Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		tx := g.Begin()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		props := map[string][]byte{}
		for i := 0; i < 24; i++ {
			props[fmt.Sprintf("k%02d", i)] = []byte(fmt.Sprintf("v%02d", i))
		}
		tx.IndexNodeProperties(id, props)
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "graphene.wal"))
		if err != nil {
			t.Fatalf("read wal: %v", err)
		}
		return data
	}

	// The commit record carries a wall-clock timestamp, so only the batch body
	// up to the first commit marker is comparable. Ten runs is enough to catch a
	// map order that varies: Go randomises it per range, not per process.
	first := logOf(t)
	for i := 0; i < 10; i++ {
		if got := logOf(t); !bytes.Equal(trimAtCommit(got), trimAtCommit(first)) {
			t.Fatalf("run %d framed the same transaction differently", i)
		}
	}
}

// trimAtCommit cuts a log at the first batch-commit record type (0x0A), whose
// payload carries a timestamp and is therefore not comparable across runs.
func trimAtCommit(log []byte) []byte {
	for i := 0; i < len(log); i++ {
		if log[i] == 0x0A {
			return log[:i]
		}
	}
	return log
}

// The crash test, and the reason all of this exists.
//
// The log is cut at every byte offset. At each one the store is reopened and
// asked a single question: does the set of nodes carrying a key agree with the
// set of nodes that exist? A node present without its entry is the corruption
// Finch would otherwise have had to reconcile for at every startup; an entry
// present without its node is the mirror image and just as wrong.
func TestTx_IndexAndTopologySurviveATruncatedLogTogether(t *testing.T) {
	const nodes = 6

	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// One transaction per node, so the log holds several complete batches and a
	// truncation lands inside one of them rather than after all of them.
	want := map[string]struct{}{}
	for i := 0; i < nodes; i++ {
		key := fmt.Sprintf("ver:%02d", i)
		tx := g.Begin()
		id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		tx.IndexNodeProperties(id, map[string][]byte{"k": []byte(key)})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit %d: %v", i, err)
		}
		want[key] = struct{}{}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	full, err := os.ReadFile(filepath.Join(dir, "graphene.wal"))
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(full) == 0 {
		t.Fatal("the log is empty; the fixture wrote nothing")
	}

	for cut := 0; cut <= len(full); cut++ {
		func() {
			trunc := t.TempDir()
			if err := os.WriteFile(filepath.Join(trunc, "graphene.wal"), full[:cut], 0o644); err != nil {
				t.Fatalf("cut %d: write: %v", cut, err)
			}
			rg, err := graphene.Open(trunc)
			if err != nil {
				// A log cut mid-header can legitimately be unopenable. What is
				// not allowed is opening onto an inconsistent graph.
				return
			}
			defer rg.Close()

			count, err := rg.NodeCount()
			if err != nil {
				t.Fatalf("cut %d: NodeCount: %v", cut, err)
			}

			indexed := 0
			for key := range want {
				ids, err := rg.NodesByProperty("k", []byte(key))
				if err != nil {
					t.Fatalf("cut %d: NodesByProperty: %v", cut, err)
				}
				if len(ids) > 1 {
					t.Fatalf("cut %d: key %q resolves to %d nodes", cut, key, len(ids))
				}
				indexed += len(ids)
			}

			if uint64(indexed) != count {
				t.Fatalf("cut %d: %d nodes exist but %d are reachable by key — "+
					"topology and index did not become durable together",
					cut, count, indexed)
			}
		}()
	}
}
