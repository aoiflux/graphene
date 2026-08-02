package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Read-only inspection of a store's files.

// buildInspectable makes a store with a compacted image and unreplayed work on
// top, then closes it.
func buildInspectable(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []store.NodeID
	for i := 0; i < 20; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.IndexNodeProperty(id, "sha256", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < 20; i++ {
		addEdgeD(t, s, ids[i-1], ids[i], store.EdgeTypeContains)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	// Work after the compaction, so the log is not empty.
	if err := s.ApplyTransactionAs(
		[]store.TxOp{{Kind: store.TxOpAddNode, Node: &store.Node{
			ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeTag}}}},
		store.TxContext{ActorID: 99},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestInspectCSR_ReportsTheImage(t *testing.T) {
	dir := buildInspectable(t)

	c, err := InspectCSR(dir)
	if err != nil {
		t.Fatalf("InspectCSR: %v", err)
	}
	if c.Version != csrVersionCurrent {
		t.Errorf("version %d, want %d", c.Version, csrVersionCurrent)
	}
	if c.NodeCount != 20 || c.EdgeCount != 19 {
		t.Errorf("counts %d/%d, want 20/19", c.NodeCount, c.EdgeCount)
	}
	if c.PropertyNodeEntries != 20 {
		t.Errorf("property entries %d, want 20", c.PropertyNodeEntries)
	}
	if c.NodeSeqHW == 0 {
		t.Error("sequence high-water not reported")
	}
	if c.FileBytes <= 0 {
		t.Error("file size not reported")
	}

	// A file path works as well as a directory.
	if _, err := InspectCSR(filepath.Join(dir, csrFileName)); err != nil {
		t.Fatalf("InspectCSR on an explicit file path: %v", err)
	}
}

// The log's commit records carry provenance, and inspection surfaces it. This is
// the externally visible payoff of the commit-record change.
func TestInspectWAL_ReportsCommitProvenance(t *testing.T) {
	dir := buildInspectable(t)

	w, err := InspectWAL(dir)
	if err != nil {
		t.Fatalf("InspectWAL: %v", err)
	}
	if len(w.Commits) != 1 {
		t.Fatalf("found %d commits, want 1", len(w.Commits))
	}
	c := w.Commits[0]
	if !c.HasDetail {
		t.Fatal("commit carries no provenance")
	}
	if c.ActorID != 99 {
		t.Errorf("actor %d, want 99", c.ActorID)
	}
	if c.UnixNano == 0 {
		t.Error("commit has no timestamp")
	}
	if !c.Validated {
		t.Error("commit did not validate against the body it describes")
	}
	if len(w.Records) == 0 {
		t.Error("no records reported")
	}
}

// **The claim that matters**: inspection reads the files and never opens the
// store, so it works while another handle holds it. An inspector that cannot be
// used against a live store is not much use, because the moment you want one is
// the moment a process is still attached.
func TestInspect_WorksWhileTheStoreIsOpen(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	addNodeD(t, s, store.NodeTypeTag)
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}

	// The store is still open, still holding its WAL handle.
	c, err := InspectCSR(dir)
	if err != nil {
		t.Fatalf("InspectCSR against a live store: %v", err)
	}
	if c.NodeCount != 5 {
		t.Errorf("image reports %d nodes, want 5", c.NodeCount)
	}

	w, err := InspectWAL(dir)
	if err != nil {
		t.Fatalf("InspectWAL against a live store: %v", err)
	}
	if len(w.Records) == 0 {
		t.Error("no records seen in a log with unreplayed work")
	}

	// And the store is unharmed by having been read.
	if _, err := s.GetNode(1); err != nil {
		t.Fatalf("store broken after inspection: %v", err)
	}
	if n, _ := s.NodeCount(); n != 6 {
		t.Errorf("node count %d after inspection, want 6", n)
	}
}

// A torn tail is the normal shape of a log after a crash. Inspection reports
// where reading stopped rather than failing, because that offset is the answer
// the operator wants.
func TestInspectWAL_ReportsATornTail(t *testing.T) {
	dir := buildInspectable(t)
	p := filepath.Join(dir, walFileName)

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Cut mid-record.
	if err := os.WriteFile(p, data[:len(data)-6], 0600); err != nil {
		t.Fatal(err)
	}

	w, err := InspectWAL(dir)
	if err != nil {
		t.Fatalf("InspectWAL on a truncated log should report, not fail: %v", err)
	}
	if !w.Truncated {
		t.Fatal("truncation not reported")
	}
	if w.TruncatedAt <= 0 {
		t.Errorf("truncation offset %d", w.TruncatedAt)
	}
}

// A batch that began and never committed is what a crash mid-transaction leaves.
// Replay discards it; inspection says so rather than leaving it to be inferred.
func TestInspectWAL_ReportsAnOpenBatch(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, walFileName)

	b := newWALBatch(64)
	b.add(walRecordNode, []byte("never-committed"))
	framed := b.finish(batchMeta{CommitSeq: 1})
	// Drop the commit marker.
	framed = framed[:len(framed)-(walRecordOverhead+walBatchCommitPayload)]
	if err := os.WriteFile(p, framed, 0600); err != nil {
		t.Fatal(err)
	}

	w, err := InspectWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !w.OpenBatch {
		t.Fatal("an uncommitted batch was not reported")
	}
	if len(w.Commits) != 0 {
		t.Fatalf("reported %d commits for a batch that never committed", len(w.Commits))
	}
}

// Inspection of a corrupt image reports what is wrong instead of panicking or
// returning something plausible.
func TestInspectCSR_ReportsCorruption(t *testing.T) {
	dir := buildInspectable(t)
	p := filepath.Join(dir, csrFileName)

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[6] = 0xFF // node count, far past what the file can hold
	data[7] = 0xFF
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	info, err := InspectCSR(dir)
	if err == nil {
		t.Fatal("a corrupt image was accepted")
	}
	// The header fields read before the failure are still reported, which is
	// what makes the error diagnosable.
	if info.Version != csrVersionCurrent {
		t.Errorf("version not reported alongside the error: %d", info.Version)
	}
	if info.FileBytes == 0 {
		t.Error("file size not reported alongside the error")
	}
}

// A missing file is a normal state, not a crash: a store that has never been
// compacted has no image at all.
func TestInspect_MissingFiles(t *testing.T) {
	dir := t.TempDir()

	if _, err := InspectCSR(dir); err == nil {
		t.Error("expected an error for a missing CSR")
	} else if !os.IsNotExist(underlyingErr(err)) {
		t.Errorf("error should be recognisable as not-exist, got %v", err)
	}
	if _, err := InspectWAL(dir); err == nil {
		t.Error("expected an error for a missing WAL")
	}
}

func underlyingErr(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		next := u.Unwrap()
		if next == nil {
			return err
		}
		err = next
	}
}
