package graphene_test

import (
	"bytes"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// CompactAndReopen, and the one thing it is for.
//
// A compaction writes the records to the image and then goes on serving the
// graph it built in the heap, so the payload term does not fall at a compaction.
// It falls at an open, because that is where the image is attached. These tests
// assert that difference directly rather than trusting the argument for it --
// docs/MEMORY_MODEL.md section 9.8 is the argument, and it was written from a
// run where not doing this cost 661.8 MiB.

// compactReopenBlob is large enough that the payload term is unmistakable
// against the structural terms, and small enough that the whole test is a
// fraction of a second.
const compactReopenBlob = 4096

func writeBlobNodes(t *testing.T, g *graphene.Graph, n int) {
	t.Helper()
	blob := bytes.Repeat([]byte{0xA5}, compactReopenBlob)
	for i := 0; i < n; i++ {
		_, err := g.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: append([]byte(nil), blob...),
		})
		if err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
	}
}

// The payload a compaction has just written to the image is still in the heap
// afterwards, and reopening is what releases it. This is the measurement the API
// exists for, so it is asserted rather than described.
func TestCompactAndReopen_ReleasesThePayloadTheCompactionWrote(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { g.Close() }()

	const n = 2000
	writeBlobNodes(t, g, n)

	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	afterCompact, ok := g.EstimateResident()
	if !ok {
		t.Fatal("the disk backend should estimate its residency")
	}

	// The compaction above put every one of these bytes in the image. They are
	// still here, which is the whole finding.
	if afterCompact.Payload < int64(n)*compactReopenBlob/2 {
		t.Fatalf("payload after a compaction is %d bytes over %d nodes of %d; "+
			"expected the records to still be carrying their own blobs",
			afterCompact.Payload, n, compactReopenBlob)
	}

	reopened, err := g.CompactAndReopen()
	if err != nil {
		t.Fatalf("CompactAndReopen: %v", err)
	}
	g = reopened

	afterReopen, ok := g.EstimateResident()
	if !ok {
		t.Fatal("the reopened store should estimate its residency")
	}
	if afterReopen.Payload >= afterCompact.Payload {
		t.Fatalf("payload did not fall: %d bytes after a compaction, %d after the reopen",
			afterCompact.Payload, afterReopen.Payload)
	}
	// Not merely lower -- lower by the blobs. A reopen that released a megabyte
	// of something else would pass the comparison above and fail the point.
	if freed := afterCompact.Payload - afterReopen.Payload; freed < int64(n)*compactReopenBlob/2 {
		t.Fatalf("the reopen released %d bytes over %d nodes of %d; expected close to the blobs",
			freed, n, compactReopenBlob)
	}

	// And it is the same store: the reopen is a release, not a loss.
	count, err := g.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount after reopen: %v", err)
	}
	if count != uint64(n) {
		t.Fatalf("reopened store holds %d nodes, want %d", count, n)
	}
}

// The returned handle is the store; the receiver is closed. A caller who keeps
// using the old one should find out immediately rather than at the next write.
func TestCompactAndReopen_ClosesTheReceiver(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeBlobNodes(t, g, 16)

	old := g
	reopened, err := g.CompactAndReopen()
	if err != nil {
		t.Fatalf("CompactAndReopen: %v", err)
	}
	defer reopened.Close()

	if _, err := old.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err == nil {
		t.Fatal("the receiver should be closed after CompactAndReopen")
	}
	// The new handle holds the lock the old one released, so this also proves
	// the reopen actually took it rather than the old handle keeping it.
	if _, err := reopened.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatalf("the returned handle should be writable: %v", err)
	}
}

// On the in-memory backend there is no image to reattach, so the call compacts
// nothing and hands back the same Graph. It is on Graph rather than on
// disk.Store so that a rebuild loop reads the same on both backends.
func TestCompactAndReopen_IsTheSameGraphInMemory(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatal(err)
	}
	same, err := g.CompactAndReopen()
	if err != nil {
		t.Fatalf("CompactAndReopen on the memory backend: %v", err)
	}
	if same != g {
		t.Fatal("the memory backend should hand back the same Graph")
	}
	count, err := same.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("memory graph holds %d nodes, want 1", count)
	}
}
