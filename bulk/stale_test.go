package bulk_test

// A dump has to be importable, and that is not the same claim as "the export
// returned no error".
//
// The property index is not versioned and its entries can outlive the records
// they name: IndexNodeProperty on a node that has been deleted files one, and
// so does the ReindexKeep policy on an update. An export that copied those out
// verbatim produced a stream naming an entity it never defined, which the
// importer refuses — correctly, and only after the operator has a file they
// believe is a backup.

import (
	"bytes"
	"errors"
	"testing"

	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/store"
)

func TestExport_DropsPropertyEntriesWhoseEntityIsGone(t *testing.T) {
	backends(t, func(t *testing.T, mk func(t *testing.T) storeUnderTest) {
		s := mk(t)

		keep, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeCase},
			Properties: []byte("keep"),
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		gone, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeCase},
			Properties: []byte("gone"),
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.IndexNodeProperty(keep, "sha256", []byte("a")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}

		deleter, ok := s.(interface {
			DeleteNode(id store.NodeID) error
		})
		if !ok {
			t.Skipf("%T cannot delete", s)
		}
		if err := deleter.DeleteNode(gone); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
		// After the delete, which is what leaves an entry with no record. The
		// store accepts it; whether it should is a separate question, and an
		// export that cannot survive one is broken either way.
		if err := s.IndexNodeProperty(gone, "sha256", []byte("b")); err != nil {
			t.Fatalf("IndexNodeProperty on a deleted node: %v", err)
		}

		var buf bytes.Buffer
		sum, err := bulk.ExportJSONL(&buf, s, bulk.Options{})
		if err != nil {
			t.Fatalf("export: %v", err)
		}
		if sum.NodeProperties != 1 {
			t.Errorf("exported %d node properties, want 1 — the entry for the deleted node travelled",
				sum.NodeProperties)
		}

		dst := mk(t)
		if _, err := bulk.ImportJSONL(bytes.NewReader(buf.Bytes()), dst, bulk.Options{}); err != nil {
			if errors.Is(err, bulk.ErrOutOfOrder) {
				t.Fatalf("the dump names an entity it never defines: %v", err)
			}
			t.Fatalf("import of what the export wrote: %v", err)
		}
	})
}
