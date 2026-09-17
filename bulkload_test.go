package graphene

// The façade over a bulk load: the part a caller actually touches.
//
// disk/bulk_load_test.go holds the proof that the image is right. What is left
// here is what the wrapper adds and could get wrong on its own: the sidecars
// loaded on the handle it hands back, the receiver closed whichever way the load
// went, and the refusal on a backend that has no image to write once.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// bulkLoadFixture fills a graph with n nodes and n-1 edges in one pass.
func bulkLoadFixture(t *testing.T, g *Graph, n int) *Graph {
	t.Helper()
	ids := make([]store.NodeID, 0, n)
	loaded, err := g.BulkLoad(
		func(w *BulkNodeWriter) error {
			for i := 1; i <= n; i++ {
				id, err := w.AddNode(BulkNode{
					Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
					Properties: []byte(fmt.Sprintf("payload-%04d", i)),
					Index: []BulkProperty{
						{Key: "sha256", Value: []byte(fmt.Sprintf("sha-%06d", i))},
						{Key: "tool", Value: []byte(fmt.Sprintf("tool-%02d", i%7))},
					},
				})
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return nil
		},
		func(w *BulkEdgeWriter) error {
			for i := 1; i < len(ids); i++ {
				if _, err := w.AddEdge(BulkEdge{
					Src:    ids[i-1],
					Dst:    ids[i],
					Labels: []store.EdgeType{store.EdgeTypeContains},
					Index:  []BulkProperty{{Key: "rel", Value: []byte(fmt.Sprintf("rel-%03d", i%5))}},
				}); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("BulkLoad: %v", err)
	}
	return loaded
}

// The handle a load hands back is a working graph: records, edges, the property
// index, and the declarations it was loaded under.
func TestBulkLoad_TheReturnedGraphIsUsable(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := g.DeclareUniqueProperty("sha256"); err != nil {
		t.Fatalf("DeclareUniqueProperty: %v", err)
	}
	if err := g.DeclareCompositeProperties([]string{"tool", "sha256"}); err != nil {
		t.Fatalf("DeclareCompositeProperties: %v", err)
	}

	g = bulkLoadFixture(t, g, 200)
	defer g.Close()

	n, err := g.GetNode(store.NodeID(123))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got, want := string(n.Properties), "payload-0123"; got != want {
		t.Fatalf("node 123 carries %q, want %q", got, want)
	}
	ids, err := g.NodesByProperty("sha256", []byte("sha-000123"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(ids) != 1 || ids[0] != 123 {
		t.Fatalf("NodesByProperty found %v, want [123]", ids)
	}

	// The declarations survive, which is what says the sidecars and the
	// catalogue were read on the handle the load returned rather than lost with
	// the one it closed.
	if nodeKeys, _ := g.OrderedProperties(); len(nodeKeys) != 0 {
		t.Fatalf("OrderedProperties is %v on a load that declared none", nodeKeys)
	}
	if nodeKeys, _ := g.CompositeProperties(); len(nodeKeys) != 1 {
		t.Fatalf("CompositeProperties is %v, want the one that was declared", nodeKeys)
	}

	// And the store refuses a duplicate of a unique value the load wrote, which
	// it can only do if the image's own index is what is answering: the load
	// registered nothing with a resident index, so a handle that had not picked
	// up the image would let this through.
	fresh, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatalf("AddNode after a load: %v", err)
	}
	if err := g.IndexNodeProperty(fresh, "sha256", []byte("sha-000123")); err == nil {
		t.Fatal("a value node 123 was loaded with was accepted for a second node under a unique key")
	}
}

// A load into a graph that already holds something is refused, and the caller
// gets a typed error rather than a message to match on.
func TestBulkLoad_RefusesANonEmptyGraph(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = g.BulkLoad(
		func(w *BulkNodeWriter) error { return nil },
		func(w *BulkEdgeWriter) error { return nil })
	if !errors.Is(err, disk.ErrBulkLoadNotEmpty) {
		t.Fatalf("BulkLoad into a non-empty graph returned %v, want disk.ErrBulkLoadNotEmpty", err)
	}
	// The receiver is closed whichever way the load went, which is the contract
	// and is why a caller must not go on using it.
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err == nil {
		t.Fatal("the receiver still accepts writes after a refused load; it is documented as closed")
	}
	// And the directory is still a store: the refusal happened before anything
	// was written, so reopening it finds what was there.
	again, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after a refused load: %v", err)
	}
	defer again.Close()
	if _, err := again.GetNode(store.NodeID(1)); err != nil {
		t.Fatalf("the record written before the refused load is gone: %v", err)
	}
}

// The in-memory backend has no image to write once, and says so.
func TestBulkLoad_RefusesTheInMemoryBackend(t *testing.T) {
	g := NewInMemory()
	defer g.Close()
	_, err := g.BulkLoad(
		func(w *BulkNodeWriter) error { return nil },
		func(w *BulkEdgeWriter) error { return nil })
	if err == nil {
		t.Fatal("BulkLoad on an in-memory graph succeeded; it has no image to write")
	}
}
