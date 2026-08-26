package disk

// The crash window inside Compact, which the black-box
// TestIndexIntegrity_CrashDuringCompact does not reach.
//
// That test covers the first half of the window — a store that died after
// writing the temp CSR and before anything else. This covers the second half,
// after the checkpoint marker has been written and fsynced but before the
// rename put the new image in place. It is the more dangerous half: the marker
// is a durable statement that everything before it is in the image, and at that
// instant the image is still the *old* one. If replay treated the marker as
// permission to skip the records ahead of it, every write since the last
// compaction would be gone.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func TestCompact_CrashAfterCheckpointBeforeRename(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Two generations of data: one that a completed compaction puts in the
	// image, and one that is still only in the log when the crash happens. Only
	// the second is at risk, but losing either has to fail the test.
	var inImage []store.NodeID
	for i := 0; i < 32; i++ {
		inImage = append(inImage, addNodeD(t, s, store.NodeTypeEvidenceFile))
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	var inLog []store.NodeID
	for i := 0; i < 32; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.IndexNodeProperty(id, "bucket", []byte("beta")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		inLog = append(inLog, id)
	}

	// Reproduce the instant. Compact writes and fsyncs the temp image, then
	// writes and fsyncs the checkpoint, then renames. Doing the checkpoint by
	// hand and dropping a stray temp file puts the directory in exactly the
	// state a kill between those last two steps would leave.
	if err := s.wal.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	tmpPath := filepath.Join(dir, csrFileName+".tmp")
	if err := os.WriteFile(tmpPath, []byte("a compaction that never finished"), 0600); err != nil {
		t.Fatalf("write stray temp CSR: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer reopened.Close()

	for _, id := range inImage {
		if _, err := reopened.GetNode(id); err != nil {
			t.Fatalf("node %d was in the image before the crash and is gone: %v", id, err)
		}
	}
	// The records the checkpoint marker sits in front of. A replay that stopped
	// at the marker, or that started after it, loses precisely these.
	for _, id := range inLog {
		if _, err := reopened.GetNode(id); err != nil {
			t.Fatalf("node %d was written before the checkpoint and is gone: %v", id, err)
		}
	}
	got, err := reopened.NodesByProperty("bucket", []byte("beta"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(got) != len(inLog) {
		t.Fatalf("property index lost entries across the crash: want %d, got %d", len(inLog), len(got))
	}

	// The stray temp file must not have been mistaken for the image, and the
	// store must still compact.
	if err := reopened.Compact(); err != nil {
		t.Fatalf("Compact after crash recovery: %v", err)
	}
	for _, id := range append(append([]store.NodeID{}, inImage...), inLog...) {
		if _, err := reopened.GetNode(id); err != nil {
			t.Fatalf("node %d lost by the recovery compaction: %v", id, err)
		}
	}
}
