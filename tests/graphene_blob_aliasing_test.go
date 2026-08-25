package graphene_test

import (
	"bytes"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Property-blob ownership.
//
// The store has two different, deliberate policies for property blobs, and both
// are easy to "optimise" into a bug because each looks like a redundant copy:
//
//   - Writes COPY. A caller may reuse or mutate the slice it passed to
//     AddNode/AddEdge/UpdateNode the moment the call returns. If the store kept
//     the caller's slice, later caller writes would silently rewrite stored data.
//
//   - Reads ALIAS. A read hands back the record's own blob without copying it.
//     This is safe because a record's blob is allocated per record, exactly
//     sized, and never written to after construction — and the API contract
//     states that reads may return pointers into internal state.
//
// These tests pin both halves. The write-side test is the important one: it is
// the reason the remaining cloneBytes calls on the ingest path must stay.

func blobBackends(t *testing.T) map[string]*graphene.Graph {
	t.Helper()
	disk, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	return map[string]*graphene.Graph{
		"memory": graphene.NewInMemory(),
		"disk":   disk,
	}
}

// TestWritesCopyCallerBlobs asserts the store does not retain caller memory.
// Mutating the slice after the write must not change what the store holds.
func TestWritesCopyCallerBlobs(t *testing.T) {
	for name, g := range blobBackends(t) {
		t.Run(name, func(t *testing.T) {
			original := []byte("original-node-payload")
			buf := append([]byte(nil), original...)

			id, err := g.AddNode(&store.Node{
				Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
				Properties: buf,
			})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}

			edgeOriginal := []byte("original-edge-payload")
			edgeBuf := append([]byte(nil), edgeOriginal...)
			eid, err := g.AddEdge(&store.Edge{
				Src:        id,
				Dst:        id,
				Labels:     []store.EdgeType{store.EdgeTypeContains},
				Properties: edgeBuf,
			})
			if err != nil {
				t.Fatalf("AddEdge: %v", err)
			}

			// The caller scribbles over its own buffers.
			for i := range buf {
				buf[i] = 'X'
			}
			for i := range edgeBuf {
				edgeBuf[i] = 'Y'
			}

			got, err := g.GraphStore.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if !bytes.Equal(got.Properties, original) {
				t.Errorf("node blob followed caller mutation: got %q, want %q",
					got.Properties, original)
			}

			gotEdge, err := g.GraphStore.GetEdge(eid)
			if err != nil {
				t.Fatalf("GetEdge: %v", err)
			}
			if !bytes.Equal(gotEdge.Properties, edgeOriginal) {
				t.Errorf("edge blob followed caller mutation: got %q, want %q",
					gotEdge.Properties, edgeOriginal)
			}
		})
	}
}

// TestUpdateCopiesCallerBlobs covers the same hazard on the update path, which
// has its own set of copies.
func TestUpdateCopiesCallerBlobs(t *testing.T) {
	for name, g := range blobBackends(t) {
		t.Run(name, func(t *testing.T) {
			id, err := g.AddNode(&store.Node{
				Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
				Properties: []byte("before"),
			})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}

			updated := []byte("after-update")
			buf := append([]byte(nil), updated...)
			if err := g.UpdateNode(&store.Node{
				ID:         id,
				Labels:     []store.NodeType{store.NodeTypeTag},
				Properties: buf,
			}); err != nil {
				t.Fatalf("UpdateNode: %v", err)
			}
			for i := range buf {
				buf[i] = 'Z'
			}

			got, err := g.GraphStore.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if !bytes.Equal(got.Properties, updated) {
				t.Errorf("updated blob followed caller mutation: got %q, want %q",
					got.Properties, updated)
			}
		})
	}
}

// TestBlobsSurviveCompaction pins the read side. Compaction rebuilds the CSR, so
// a blob read afterwards must still carry the right bytes — this is what would
// break if the CSR ever reused or mutated blob storage, which is the premise the
// aliasing read path rests on.
func TestBlobsSurviveCompaction(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	const n = 64
	want := make(map[store.NodeID][]byte, n)
	for i := 0; i < n; i++ {
		payload := bytes.Repeat([]byte{byte('a' + i%26)}, 1+i*3)
		id, err := g.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: append([]byte(nil), payload...),
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		want[id] = payload
	}

	// Read before compaction: these come from the delta overlay.
	before := make(map[store.NodeID][]byte, n)
	for id := range want {
		got, err := g.GraphStore.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode before compact: %v", err)
		}
		before[id] = got.Properties
	}

	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Reads after compaction come from the CSR, and must agree.
	for id, expect := range want {
		got, err := g.GraphStore.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode after compact: %v", err)
		}
		if !bytes.Equal(got.Properties, expect) {
			t.Fatalf("node %d: blob changed across compaction: got %q, want %q",
				id, got.Properties, expect)
		}
	}

	// Blobs handed out before compaction must not have been rewritten by it.
	for id, stale := range before {
		if !bytes.Equal(stale, want[id]) {
			t.Fatalf("node %d: compaction mutated a previously returned blob: got %q, want %q",
				id, stale, want[id])
		}
	}
}
