package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// What an Open would cost, asked before it is paid.
//
// The property under test throughout is not "the number looks right" but "the
// number is replay's own". A preflight that counts records under slightly looser
// rules than the parser it is predicting reports a cost for work that will not
// happen, and the shape of the error is invisible until the one log where the
// two disagree — which is the crashed, half-written log this whole surface
// exists to be asked about. So the central test drives the real replay and
// compares, rather than asserting a constant.

// replayedRecordCount is what the parser actually applies, counted by driving
// it. This is the oracle the preflight is measured against, and it is
// deliberately the production path rather than a second implementation.
func replayedRecordCount(t *testing.T, path string) int64 {
	t.Helper()
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	var n int64
	count := func([]byte) error { n++; return nil }
	if err := w.Replay(ReplayCallbacks{
		NodeFunc:          count,
		EdgeFunc:          count,
		NodePropFunc:      count,
		EdgePropFunc:      count,
		NodeDeleteFunc:    count,
		EdgeDeleteFunc:    count,
		NodePropPurgeFunc: count,
		EdgePropPurgeFunc: count,
		KeyTransitionFunc: count,
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return n
}

// preflightFixture builds a store whose log holds every shape the walk has to
// account for: single records outside any batch, committed batches, and index
// entries, on top of a compacted image so both halves of the estimate are
// populated.
func preflightFixture(t *testing.T, nodes int) string {
	t.Helper()
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var ids []store.NodeID
	for i := 0; i < nodes; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%d", i%7))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		addEdgeD(t, s, ids[i-1], ids[i], store.EdgeTypeContains)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Work after the compaction, so the log is not empty: a transaction (which
	// frames a batch) and then single records outside one.
	if err := s.ApplyTransactionAs(
		[]store.TxOp{
			{Kind: store.TxOpAddNode, Node: &store.Node{
				ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeTag}}},
			{Kind: store.TxOpAddNode, Node: &store.Node{
				ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeTag}}},
		},
		store.TxContext{ActorID: 99},
	); err != nil {
		t.Fatalf("ApplyTransactionAs: %v", err)
	}
	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

// TestPreflightOpen_CountsWhatReplayWouldApply is the load-bearing test. Every
// other number here is an estimate; this one is an equality, and it is the
// reason the rest can be trusted.
func TestPreflightOpen_CountsWhatReplayWouldApply(t *testing.T) {
	dir := preflightFixture(t, 40)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	want := replayedRecordCount(t, filepath.Join(dir, walFileName))

	if est.WALRecords != want {
		t.Errorf("PreflightOpen counted %d records, replay applies %d: the walk is not "+
			"stopping where the parser stops", est.WALRecords, want)
	}
	if est.WALRecords == 0 {
		t.Fatal("the fixture's log is empty, so this test proves nothing")
	}
	if est.WALMaxBatchRecords == 0 {
		t.Error("the fixture commits a transaction, so some batch was buffered; " +
			"WALMaxBatchRecords should say how large it got")
	}
	if est.WALError != "" {
		t.Errorf("a healthy log reported a replay error: %s", est.WALError)
	}
	if est.WALTruncated {
		t.Error("a cleanly closed log was reported as truncated")
	}
}

// The two byte figures are different quantities and the difference is exactly
// the container header. Pinned because a budget compared against the wrong one
// refuses a store that has not changed, and 50 bytes is small enough that the
// mistake survives casual reading.
func TestPreflightOpen_SeparatesFileBytesFromReplayBytes(t *testing.T) {
	dir := preflightFixture(t, 10)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.WALFraming != walFramingV2 {
		t.Fatalf("a log written by this build should be framing v%d, got v%d",
			walFramingV2, est.WALFraming)
	}
	if got, want := est.WALBytes-est.WALReplayBytes, int64(walFileHeaderSize); got != want {
		t.Errorf("WALBytes - WALReplayBytes = %d, want %d (the container header)", got, want)
	}
}

// A batch that began and never committed is what a crash mid-transaction leaves.
// Replay reads those records, verifies them, holds them, and throws them away —
// so they cost memory and change nothing, and the estimate has to say so in the
// field that means "discarded" rather than the one that means "applied".
func TestPreflightOpen_BuffersAnOpenBatchRatherThanCountingIt(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, walFileName)

	b := newWALBatchFramed(0, walFramingV2)
	b.add(walRecordNode, []byte("never-committed-1"))
	b.add(walRecordNode, []byte("never-committed-2"))
	framed := mustFinish(b, batchMeta{CommitSeq: 1})
	// Drop the commit marker, leaving the batch open at EOF.
	framed = framed[:len(framed)-(walRecordOverhead+walBatchCommitPayloadV2)]

	withWALHeader(t, p, framed)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.WALRecords != 0 {
		t.Errorf("WALRecords = %d for a batch that never committed; replay applies none of it",
			est.WALRecords)
	}
	if est.WALRecordsBuffered != 2 {
		t.Errorf("WALRecordsBuffered = %d, want 2: the records replay reads and discards "+
			"still cost memory while it holds them", est.WALRecordsBuffered)
	}
	if !est.WALOpenBatch {
		t.Error("an uncommitted batch was not reported, so WALRecordsBuffered has no explanation")
	}
	if got := replayedRecordCount(t, p); got != 0 {
		t.Fatalf("the oracle disagrees: replay applied %d records from an uncommitted batch", got)
	}
}

// A commit that does not describe the body it follows is an incomplete
// transaction, and replay discards it rather than applying a batch that
// contradicts its own commit record. The count has to make the same choice, and
// the only way to make it is to have checked the body checksum — which is the
// part of the walk that could plausibly have been skipped as too expensive.
func TestPreflightOpen_DiscardsABatchWhoseCommitDisagrees(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, walFileName)

	b := newWALBatchFramed(0, walFramingV2)
	b.add(walRecordNode, []byte("one"))
	b.add(walRecordNode, []byte("two"))
	framed := mustFinish(b, batchMeta{CommitSeq: 1})

	// Corrupt the body checksum inside the commit payload, leaving the commit
	// record's own framing intact so it still verifies as a record.
	commitPayloadAt := len(framed) - walFooterSize - walBatchCommitPayloadV2
	binary.LittleEndian.PutUint32(framed[commitPayloadAt+4:commitPayloadAt+8], 0xDEADBEEF)
	crc := recordCRC(walFramingV2, walRecordBatchCommit, framed[commitPayloadAt:len(framed)-walFooterSize])
	binary.LittleEndian.PutUint32(framed[len(framed)-walFooterSize:], crc)

	withWALHeader(t, p, framed)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.WALRecords != 0 {
		t.Errorf("WALRecords = %d for a batch whose commit disagrees with its body; "+
			"the walk is not checking the body checksum", est.WALRecords)
	}
	if got := replayedRecordCount(t, p); got != 0 {
		t.Fatalf("the oracle disagrees: replay applied %d records", got)
	}
}

// Replay stops at a checkpoint, so a log can be large in bytes and tiny in
// records with nothing at all wrong. Without the flag, that reads as damage.
func TestPreflightOpen_StopsAtACheckpoint(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, walFileName)

	w, err := OpenWAL(p)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.AppendNode([]byte("before")); err != nil {
		t.Fatalf("AppendNode: %v", err)
	}
	if err := w.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := w.AppendNode([]byte("after-the-checkpoint")); err != nil {
			t.Fatalf("AppendNode: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if !est.WALCheckpointed {
		t.Error("the checkpoint was not reported, so a large log with one record looks broken")
	}
	if est.WALRecords != 1 {
		t.Errorf("WALRecords = %d, want 1: replay stops at the checkpoint", est.WALRecords)
	}
	if got := replayedRecordCount(t, p); got != est.WALRecords {
		t.Errorf("the oracle disagrees: replay applied %d, preflight predicted %d",
			got, est.WALRecords)
	}
}

// A torn tail is the ordinary shape of a log after a crash. Preflight reports
// where it starts and keeps the records before it, exactly as replay does.
func TestPreflightOpen_ReportsATornTail(t *testing.T) {
	dir := preflightFixture(t, 8)
	p := filepath.Join(dir, walFileName)

	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data[:len(data)-6], 0o600); err != nil {
		t.Fatal(err)
	}

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen on a truncated log should report, not fail: %v", err)
	}
	if !est.WALTruncated {
		t.Fatal("truncation not reported")
	}
	if est.WALTruncatedAt <= 0 {
		t.Errorf("truncation offset %d", est.WALTruncatedAt)
	}
	if got := replayedRecordCount(t, p); got != est.WALRecords {
		t.Errorf("the oracle disagrees on a torn log: replay applied %d, preflight predicted %d",
			got, est.WALRecords)
	}
}

// An image below v5 carries no identifier high-water marks at all, and bytes
// 22..38 of such a file are node record data. Reporting those bytes as a
// high-water mark would be an arbitrary number presented as a fact — and it
// would flow straight into the dominant term of the resident model.
func TestPreflightOpen_PreV5ImageHasNoHighWaterMark(t *testing.T) {
	dir := t.TempDir()

	// A v4 header: magic, version, and the two counts, and nothing after them but
	// what would be the record stream. The bytes standing where seqHW would live
	// are deliberately large and recognisable.
	img := make([]byte, 64)
	copy(img[0:4], "GCSR")
	binary.LittleEndian.PutUint16(img[4:6], 4)
	binary.LittleEndian.PutUint64(img[6:14], 3)  // nodeCount
	binary.LittleEndian.PutUint64(img[14:22], 1) // edgeCount
	for i := 22; i < 38; i++ {
		img[i] = 0xEE
	}
	if err := os.WriteFile(filepath.Join(dir, csrFileName), img, 0o600); err != nil {
		t.Fatal(err)
	}

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.ImageVersion != 4 {
		t.Fatalf("ImageVersion = %d, want 4", est.ImageVersion)
	}
	if est.ImageSeqHWKnown {
		t.Error("a v4 image was reported as carrying identifier high-water marks; " +
			"the format has none, and bytes 22..38 are record data")
	}
	if est.ImageNodeSeqHW != 0 || est.ImageEdgeSeqHW != 0 {
		t.Errorf("read %d/%d as high-water marks from a format that has none — "+
			"those are record bytes", est.ImageNodeSeqHW, est.ImageEdgeSeqHW)
	}
	// And the model must fall back to the counts rather than to the record bytes.
	if est.ImageHeapBytes <= 0 {
		t.Error("no resident model at all for a pre-v5 image")
	}
	if est.ImageHeapBytes > 1<<20 {
		t.Errorf("ImageHeapBytes = %d for a 4-record image: the fallback read the "+
			"record bytes as an identifier space", est.ImageHeapBytes)
	}
}

// The identifier high-water mark is the term no file-size guess predicts, so the
// model has to be driven by it rather than by the record count. Two images with
// the same records and different high-water marks must not cost the same.
func TestPreflightOpen_ModelFollowsTheIdentifierSpaceNotTheRecordCount(t *testing.T) {
	dir := preflightFixture(t, 30)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if !est.ImageSeqHWKnown {
		t.Fatal("a current image should carry identifier high-water marks")
	}
	base := est.ImageHeapBytes

	// Raise only the node high-water mark in the header, leaving every record
	// untouched, and restamp nothing: the model reads the header, so this is the
	// whole of the change it can see.
	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	burned := uint64(1_000_000)
	binary.LittleEndian.PutUint64(data[22:30], burned)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}

	after, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if after.ImageHeapBytes <= base {
		t.Fatalf("burning a million identifiers changed the model from %d to %d: "+
			"it is sized by the record count, which is the cost model this program "+
			"exists to replace", base, after.ImageHeapBytes)
	}
	// And by the page model's own arithmetic, not by an arbitrary amount. The
	// charge is per page rather than per identifier: thirty records occupy at
	// most thirty pages however far apart their identifiers are, so a million
	// burned identifiers cost thirty pages of slots — about 8.8 B each — where
	// one slot per identifier would have cost 72 B each. That difference is
	// R10(b) stated as a budget.
	//
	// denseSlot is what the array-per-identifier layout charged for one node
	// identifier: the record, plus the two adjacency offsets indexed by slot.
	// Spelled out here rather than read from a constant, because the production
	// model deliberately no longer has one — the two halves are gated separately,
	// since AdjacencyLazy removes the second and not the first.
	const denseSlot = sizeofNodeRecord + 2*sizeofOffset
	perID := float64(after.ImageHeapBytes-base) / float64(burned)
	wantPerID := float64(est.ImageNodeCount*csrPageSlots*denseSlot) / float64(burned)
	if perID < wantPerID*0.9 || perID > wantPerID*1.1 {
		t.Errorf("model charges %.1f B per burned identifier, expected about %.1f",
			perID, wantPerID)
	}
	if dense := float64(burned * denseSlot); float64(after.ImageHeapBytes) > dense/4 {
		t.Errorf("model charges %d B, which is not far enough below the %.0f B a slot "+
			"per identifier would have cost", after.ImageHeapBytes, dense)
	}
}

// Neither file has to exist. A store that has never been compacted has no image
// and one compacted to completion has no log, and both are ordinary states that
// must not read as failures.
func TestPreflightOpen_MissingFilesAreNotFailures(t *testing.T) {
	dir := t.TempDir()

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("an empty store directory should preflight as zero, not fail: %v", err)
	}
	if est.ImageBytes != 0 || est.WALBytes != 0 || est.WALRecords != 0 {
		t.Errorf("non-zero estimate for an empty directory: %+v", est)
	}
	if est.ImageHeapBytes != 0 {
		t.Errorf("ImageHeapBytes = %d with no image", est.ImageHeapBytes)
	}

	if _, err := PreflightOpen(filepath.Join(dir, "does-not-exist")); err == nil {
		t.Error("a missing directory should be an error: there is no honest answer " +
			"about a store that is not there")
	}
}

// A store caught inside truncateFrom's replace window has no graphene.wal at
// all, and its only durable log — every commit since the last compaction — is
// sitting beside it under a temporary name. Reading the name rather than the
// state reports an empty log at the exact moment the log is largest.
func TestPreflightOpen_FindsTheLogInsideTheReplaceWindow(t *testing.T) {
	dir := preflightFixture(t, 12)
	p := filepath.Join(dir, walFileName)

	full, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if full.WALRecords == 0 {
		t.Fatal("the fixture's log is empty, so this test proves nothing")
	}

	if err := os.Rename(p, p+walTmpSuffix); err != nil {
		t.Fatal(err)
	}

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.LogPath != p+walTmpSuffix {
		t.Errorf("LogPath = %s, want the temporary log", est.LogPath)
	}
	if est.WALRecords != full.WALRecords {
		t.Errorf("inside the replace window the preflight found %d records, "+
			"but the same log holds %d — the carried tail was reported as absent",
			est.WALRecords, full.WALRecords)
	}
}

// --- the budget ---

// The refusal has to name both figures. A budget that reports only that it was
// exceeded cannot be acted on: the operator cannot tell whether they are over by
// a record or by a million.
func TestReplayBudget_RefusesAndNamesTheFigures(t *testing.T) {
	dir := preflightFixture(t, 20)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}

	t.Run("bytes", func(t *testing.T) {
		_, err := OpenWithOptions(dir, Options{MaxReplayBytes: est.WALReplayBytes - 1})
		if !errors.Is(err, ErrReplayBudget) {
			t.Fatalf("Open should have been refused with ErrReplayBudget, got %v", err)
		}
		if !strings.Contains(err.Error(), fmt.Sprint(est.WALReplayBytes)) {
			t.Errorf("the refusal does not name the log's own figure: %v", err)
		}
		if !strings.Contains(err.Error(), "MaxReplayBytes") {
			t.Errorf("the refusal does not name the option that caused it: %v", err)
		}
	})

	t.Run("records", func(t *testing.T) {
		_, err := OpenWithOptions(dir, Options{MaxReplayRecords: 1})
		if !errors.Is(err, ErrReplayBudget) {
			t.Fatalf("Open should have been refused with ErrReplayBudget, got %v", err)
		}
		if !strings.Contains(err.Error(), "MaxReplayRecords") {
			t.Errorf("the refusal does not name the option that caused it: %v", err)
		}
	})
}

// The budget is opt-in, and every value that is not a positive number means
// unlimited. Pinned because `> 0` and `!= 0` differ on a negative, and an int64
// field invites one.
func TestReplayBudget_ZeroAndNegativeAreUnlimited(t *testing.T) {
	dir := preflightFixture(t, 12)

	for _, o := range []Options{
		{},
		{MaxReplayRecords: -1, MaxReplayBytes: -1},
	} {
		s, err := OpenWithOptions(dir, o)
		if err != nil {
			t.Fatalf("Open with %+v should not be refused: %v", o, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A budget generous enough to admit the log must not refuse it, and must not
// make the open depend on a counting pass that was skipped. This is the arm that
// catches an off-by-one in the "the log provably cannot exceed this" shortcut.
func TestReplayBudget_AdmitsALogInsideIt(t *testing.T) {
	dir := preflightFixture(t, 12)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}

	s, err := OpenWithOptions(dir, Options{
		MaxReplayRecords: est.WALRecords,
		MaxReplayBytes:   est.WALReplayBytes,
	})
	if err != nil {
		t.Fatalf("a budget set to exactly what PreflightOpen reported refused the open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// A refusal is meant to be a routine, repeatable outcome. One that appended a
// hash-chained record of a crash that did not happen would be manufacturing
// forensic history, and the ledger it would be written to is one an operator is
// told to rely on.
func TestReplayBudget_RefusalLeavesNoLedgerBehind(t *testing.T) {
	dir := preflightFixture(t, 12)

	before := ledgerFileSet(t, dir)
	if _, err := OpenWithOptions(dir, Options{
		Audit:          true,
		MaxReplayBytes: 1,
	}); !errors.Is(err, ErrReplayBudget) {
		t.Fatalf("expected ErrReplayBudget, got %v", err)
	}
	after := ledgerFileSet(t, dir)

	for name := range after {
		if _, had := before[name]; !had {
			t.Errorf("a refused Open created %s: the refusal is being recorded as an "+
				"event in a ledger, and a budget refusal is not one", name)
		}
	}

	// And the store still opens once the budget allows it.
	s, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatalf("the store did not open after a refusal: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// StrictOptions is the posture operators are pointed at, and it must not quietly
// acquire a replay budget: that would make the safe configuration start refusing
// large uncompacted stores, which is a behaviour change wearing a safety label.
func TestReplayBudget_StrictOptionsSetsNoBudget(t *testing.T) {
	key, ring := newAttestKey(t, 1)
	o := StrictOptions(key, ring, 5)
	if o.MaxReplayRecords != 0 || o.MaxReplayBytes != 0 {
		t.Errorf("StrictOptions set a replay budget (%d records, %d bytes); the strict "+
			"posture is about proving what a store contains, not about refusing to read it",
			o.MaxReplayRecords, o.MaxReplayBytes)
	}
}

// --- the bound that is the point ---

// TestPreflightOpen_MemoryDoesNotFollowTheLog is the guard that makes this
// surface worth having. Everything else here would still pass if PreflightOpen
// buffered the whole log, and a preflight that cannot be asked about a store too
// large to open answers the wrong question.
//
// A slope rather than a ceiling, for the reason the alloc guards already record:
// a ceiling loose enough to be stable is loose enough to miss a doubling. Two
// log sizes an order of magnitude apart, and what is asserted is that the
// per-record cost is near zero — reintroducing InspectWAL's per-record append
// puts it in the tens of bytes and fails this immediately.
//
// Both fixtures are deliberately large enough that the read buffer has already
// reached preflightReadBuffer in each. Below that cap the buffer is sized to the
// log, so a smaller pair of fixtures measures the buffer growing rather than the
// walk retaining, and reports the log's own bytes-per-record as though it were
// an allocation slope — which is what an earlier version of this test did.
func TestPreflightOpen_MemoryDoesNotFollowTheLog(t *testing.T) {
	// About 24 bytes of log per record, so both of these are comfortably past
	// the 64 KiB buffer cap.
	small, large := 6_000, 60_000

	smallDir := uncompactedLog(t, small)
	largeDir := uncompactedLog(t, large)

	// Warm: the first walk faults in whatever the runtime lazily builds, and that
	// is not per-record cost.
	if _, err := PreflightOpen(smallDir); err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}

	smallAlloc := measureAlloc(func() {
		if _, err := PreflightOpen(smallDir); err != nil {
			t.Errorf("PreflightOpen: %v", err)
		}
	})
	largeAlloc := measureAlloc(func() {
		if _, err := PreflightOpen(largeDir); err != nil {
			t.Errorf("PreflightOpen: %v", err)
		}
	})

	est, err := PreflightOpen(largeDir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.WALRecords < int64(large) {
		t.Fatalf("the large fixture holds %d records, expected at least %d — the "+
			"slope below would be measured against the wrong denominator",
			est.WALRecords, large)
	}

	slope := (float64(largeAlloc) - float64(smallAlloc)) / float64(large-small)
	t.Logf("preflight allocates %d B over %d records and %d B over %d records: %.2f B/record",
		smallAlloc, small, largeAlloc, large, slope)

	// Generous, deliberately: this is watching for a shape change, not tuning a
	// number. Retaining anything per record — a record descriptor, a payload, a
	// batch body — costs tens of bytes each and cannot hide under this.
	if slope > 4 {
		t.Errorf("PreflightOpen allocates %.2f B per log record: it is retaining "+
			"something per record, which is what makes it unusable on the stores it "+
			"exists to be asked about", slope)
	}
}

// A batch begin marker names a record count, and replay reserves from it before
// reading a single record of the batch. The bound above that reservation has to
// admit a legitimate batch, so it is derived from the log's size — and a log's
// size is a terrible allocation limit: at nine bytes a record, a 2 GiB log
// permits 238 million of them, and reserving that many pending records asks for
// something over seven gigabytes before the first one is read. The marker's own
// checksum is no defence, because it is computed over the bytes making the
// claim.
//
// What is asserted is that the reservation no longer follows the declared count.
// The batch itself still replays whatever its size, because a capped reservation
// is still grown by append — so this must not be read as a stricter validity
// rule. See TestWALReplay_RejectsBatchCountLargerThanTheLog for the rule.
//
// Driven from memory rather than from a file, which is what the parser was split
// out for: the hostile case needs a small byte stream against a large declared
// log size, and writing two gigabytes to disk to express that would be absurd.
func TestWALReplay_BatchPreallocDoesNotFollowTheDeclaredCount(t *testing.T) {
	const (
		declared = 10_000_000 // what the marker claims
		logSize  = 1 << 30    // what the parser is told the log holds
	)
	if declared > logSize/walRecordOverhead {
		t.Fatalf("the fixture is malformed: %d records would be refused outright by "+
			"the size bound, so this test would prove nothing", declared)
	}

	var count [walBatchBeginPayload]byte
	binary.LittleEndian.PutUint32(count[:], declared)
	stream := appendRecordFramed(nil, walFramingV2, walRecordBatchBegin, count[:])

	// EOF straight after the marker: the batch never commits and is rolled back,
	// which is correct and is not what is under test. What matters is what
	// getting there reserved.
	alloc := measureAlloc(func() {
		if err := replayRecords(bytes.NewReader(stream), logSize, walFramingV2,
			ReplayCallbacks{NodeFunc: func([]byte) error { return nil }}); err != nil {
			t.Errorf("replayRecords: %v", err)
		}
	})

	// One pendingRecord is 32 bytes on a 64-bit build, so honouring the declared
	// count would ask for roughly 320 MB, against the cap's two. Eight megabytes
	// sits clear of the cap and two orders of magnitude below the alternative,
	// which is the separation this is watching for rather than a tuned figure.
	if alloc > 8<<20 {
		t.Errorf("a begin marker declaring %d records made replay allocate %d B; the "+
			"reservation is following the file's own number again", declared, alloc)
	}
	t.Logf("begin marker declared %d records; replay allocated %d B (uncapped would be ~%d MB)",
		declared, alloc, declared*32/(1<<20))
}

// --- helpers ---

// withWALHeader writes hand-framed records into a log with a proper container
// header in front of them, which is what makes them replay under v2 framing.
func withWALHeader(t *testing.T, path string, framed []byte) {
	t.Helper()
	out := append(appendWALFileHeader(walFileHeader{Version: walFramingV2}), framed...)
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// uncompactedLog builds a store whose whole content is still in the log, which
// is the shape the replay budget exists for.
func uncompactedLog(t *testing.T, records int) string {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < records; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

// ledgerFileSet names every file in the store directory, so a test can assert
// that a refused Open created none of them.
func ledgerFileSet(t *testing.T, dir string) map[string]struct{} {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]struct{}, len(ents))
	for _, e := range ents {
		out[e.Name()] = struct{}{}
	}
	return out
}

// FuzzPreflightWalk is a differential target: for any byte stream at all, the
// preflight's record count must be the number the parser it predicts actually
// applies.
//
// This is the new untrusted-input surface. csr_io.go's header comment names the
// two parsers that read bytes nobody in this process wrote and says
// FuzzDeserialiseCSR and FuzzWALReplay cover them; walkRecords is a third, and a
// hand-written second implementation of a parser's stopping rules is exactly the
// kind of code that agrees with its original on every log anyone thought to
// write and disagrees on the one a crash produced.
//
// Asserting equality rather than absence-of-panic is the point. A walk that
// stopped early, or counted a rolled-back batch, or bounded a record's length
// against the wrong quantity would survive a crash-only target and still report
// a cost for work that will not happen.
func FuzzPreflightWalk(f *testing.F) {
	valid := func(payloads ...string) []byte {
		b := newWALBatchFramed(0, walFramingV2)
		for _, p := range payloads {
			b.add(walRecordNode, []byte(p))
		}
		return mustFinish(b, batchMeta{CommitSeq: 1})
	}
	committed := valid("a", "b")

	f.Add(committed)
	f.Add(committed[:len(committed)-(walRecordOverhead+walBatchCommitPayloadV2)]) // open batch
	f.Add(committed[:len(committed)-3])                                           // torn commit
	f.Add(appendRecordFramed(nil, walFramingV2, walRecordNode, []byte("single")))
	f.Add(appendRecordFramed(nil, walFramingV2, walRecordCheckpoint, nil))
	f.Add(append(valid("x"), appendRecordFramed(nil, walFramingV2, walRecordNode, []byte("after"))...))
	f.Add([]byte{})
	f.Add([]byte("GWAL not really a log at all"))

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, framing := range []uint16{walFramingV1, walFramingV2} {
			// What the parser applies, counted by driving it.
			var applied int64
			count := func([]byte) error { applied++; return nil }
			_ = replayRecords(bytes.NewReader(data), int64(len(data)), framing, ReplayCallbacks{
				NodeFunc:          count,
				EdgeFunc:          count,
				NodePropFunc:      count,
				EdgePropFunc:      count,
				NodeDeleteFunc:    count,
				EdgeDeleteFunc:    count,
				NodePropPurgeFunc: count,
				EdgePropPurgeFunc: count,
				KeyTransitionFunc: count,
			})

			// What the preflight predicts it will apply.
			est := OpenEstimate{WALReplayBytes: int64(len(data)), WALFraming: framing}
			est.walkRecords(bytes.NewReader(data), 0, 0)

			if est.WALRecords != applied {
				t.Fatalf("framing v%d: preflight predicts %d records, replay applies %d\n%x",
					framing, est.WALRecords, applied, data)
			}
			if est.WALRecordsBuffered < 0 || est.WALMaxBatchRecords < 0 {
				t.Fatalf("framing v%d: negative counts %+v", framing, est)
			}
			if est.WALRecordsBuffered > 0 && !est.WALOpenBatch {
				t.Fatalf("framing v%d: %d records reported as buffered with no open batch to "+
					"explain them\n%x", framing, est.WALRecordsBuffered, data)
			}
		}
	})
}

// --- the Options-aware model (R5) ---

// modelFixture leaves a compacted image. burn>0 first writes and deletes that
// many nodes, so the surviving records sit in a sparse identifier space -- the
// shape the paged-slot model exists to account for and the shape a rebuild
// workload produces.
func modelFixture(t *testing.T, w Options, nodes, burn int) string {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, w)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	for i := 0; i < burn; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.DeleteNode(id); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
	}
	var ids []store.NodeID
	for i := 0; i < nodes; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%06d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		if err := s.IndexNodeProperty(id, "shard", []byte(fmt.Sprintf("s%d", i%13))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		addEdgeD(t, s, ids[i-1], ids[i], store.EdgeTypeContains)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

// The load-bearing test of the model: what a preflight says an Open will hold,
// against what the opened store reports holding, under the Options that decide it.
//
// It is a relation between two models and not a constant, for the same reason
// TestPreflightOpen_CountsWhatReplayWouldApply drives the real replay: a pre-open
// estimate that agrees with nothing is a number, and the shape of its error is
// invisible until the one configuration where the two disagree.
//
// Both bounds are asserted. At or above the actual, because estimate.go's header
// makes that the whole posture -- a figure an operation is refused on must decline
// work that would have fit rather than admit work that will not -- and because
// this model was under by up to a factor of two before R5, on the default Options.
// And within a band above it, because a model that returned the largest int64
// would satisfy the first assertion and be useless.
func TestPreflight_ModelTracksTheOpenStore(t *testing.T) {
	shapes := []struct {
		name        string
		nodes, burn int
		writer      Options
		// band is how far above the actual the model may sit. It is per shape
		// rather than global because what makes it loose is the format and the
		// mode, and both are named here rather than averaged away.
		band float64
	}{
		{"v9 dense", 600, 0, Options{}, 1.25},
		{"v9 sparse", 600, 9000, Options{}, 1.25},
		// v8 is looser and cannot be otherwise: GIDX states an exact node count
		// and bounds the pair by the section's length, so the entry term carries
		// that bound rather than a count.
		{"v8 dense", 600, 0, Options{IndexMode: IndexResident}, 1.75},
		{"v8 sparse", 600, 9000, Options{IndexMode: IndexResident}, 1.75},
	}
	readers := []struct {
		name string
		opts Options
	}{
		{"defaults", Options{}},
		{"resident index", Options{IndexMode: IndexResident}},
		{"heap image", Options{ImageMode: ImageHeap}},
		{"lazy adjacency", Options{Adjacency: AdjacencyLazy}},
		{"heap image, resident index", Options{ImageMode: ImageHeap, IndexMode: IndexResident}},
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			dir := modelFixture(t, sh.writer, sh.nodes, sh.burn)
			est, err := PreflightOpen(dir)
			if err != nil {
				t.Fatalf("PreflightOpen: %v", err)
			}

			for _, r := range readers {
				t.Run(r.name, func(t *testing.T) {
					s, err := OpenWithOptions(dir, r.opts)
					if err != nil {
						t.Fatalf("OpenWithOptions: %v", err)
					}
					defer s.Close()

					actual := s.EstimateResident().Total
					model := est.HeapBytesFor(r.opts)
					st := s.StorageStats()
					if actual <= 0 {
						t.Fatalf("the opened store reports %d bytes held, so this "+
							"test compares nothing", actual)
					}
					if model < actual {
						t.Errorf("model says %d, the store holds %d (%.3fx): a budget "+
							"fed this admits a store that will not fit. Holding "+
							"%s/%s/%s.", model, actual, float64(model)/float64(actual),
							st.ImageMode, st.IndexMode, st.Adjacency)
					}
					if ratio := float64(model) / float64(actual); ratio > sh.band {
						t.Errorf("model says %d against %d held (%.2fx, band %.2fx): "+
							"too loose to refuse anything with. Holding %s/%s/%s.",
							model, actual, ratio, sh.band,
							st.ImageMode, st.IndexMode, st.Adjacency)
					}
				})
			}
		})
	}
}

// The model has to distinguish the configurations, not merely bound them. A
// HeapBytesFor that ignored Options would pass the bands above on any shape whose
// worst case it returned, so this asserts the orderings the modes imply.
func TestPreflight_ModelSeparatesTheModes(t *testing.T) {
	dir := modelFixture(t, Options{}, 600, 0)
	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}

	defaults := est.HeapBytesFor(Options{})
	lazy := est.HeapBytesFor(Options{Adjacency: AdjacencyLazy})
	resident := est.HeapBytesFor(Options{IndexMode: IndexResident})
	heap := est.HeapBytesFor(Options{ImageMode: ImageHeap})

	if lazy >= defaults {
		t.Errorf("AdjacencyLazy modelled at %d against AdjacencyEager's %d: the "+
			"adjacency term is not gated, which is what folding it into a per-slot "+
			"constant prevented", lazy, defaults)
	}
	if resident <= defaults {
		t.Errorf("IndexResident modelled at %d against IndexMapped's %d: the index "+
			"term is not gated, and it is the largest term on an indexed store",
			resident, defaults)
	}
	if heap <= defaults {
		t.Errorf("ImageHeap modelled at %d against ImageMapped's %d: the payload "+
			"term is not gated", heap, defaults)
	}
	// A live reader holds no lock, so under the default ImageMode it reads the
	// image into the heap -- and an index cannot be read in place out of a heap
	// image. The model must follow the rule rather than the option's name.
	if live := est.HeapBytesFor(Options{LiveReader: true}); live <= defaults {
		t.Errorf("a live reader modelled at %d, the same store at %d: the default "+
			"ImageMode maps only where something excludes a concurrent writer, and "+
			"a live reader excludes nothing", live, defaults)
	}
}

// A v9 image carries GPIX and GPIR and no GIDX. The preflight matched "GIDX" and
// nothing else, so on every store the current defaults produce it reported no
// property entries at all and the resident model omitted the term that dominates
// an indexed store.
func TestPreflight_ReadsTheIndexOfAV9Image(t *testing.T) {
	const nodes = 200
	dir := modelFixture(t, Options{}, nodes, 0)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.ImageVersion != csrVersionMappedIndex {
		t.Fatalf("fixture wrote v%d, so this test is not about a v9 image",
			est.ImageVersion)
	}
	if !est.ImageIndexMapped {
		t.Error("a v9 image was not reported as carrying a mapped index; " +
			"HeapBytesFor then charges it as resident, which is the wrong term " +
			"by three orders of magnitude")
	}
	// Two keys per node, and GPIR states both counts exactly rather than bounding
	// the pair the way GIDX's length does.
	if want := int64(nodes * 2); est.ImagePropertyNodeEntries != want {
		t.Errorf("ImagePropertyNodeEntries = %d, want %d", est.ImagePropertyNodeEntries, want)
	}
	if want := int64(nodes * 2); est.ImagePropertyEntriesMax != want {
		t.Errorf("ImagePropertyEntriesMax = %d, want %d exactly: GPIR carries both "+
			"counts, so this is a sum and not a bound", est.ImagePropertyEntriesMax, want)
	}
	if est.ImagePropertyKeys != 2 {
		t.Errorf("ImagePropertyKeys = %d, want 2: it is the key count and not the "+
			"entry count that a mapped index's resident cost follows",
			est.ImagePropertyKeys)
	}
}

// A page is materialised whole, so the identifier space does not bound the slots.
// pagedSlots capped its answer at seqHW+1 until R5, which on a store whose
// identifiers fit inside one page removed nine tenths of the dominant term.
func TestPreflight_SlotsAreWholePages(t *testing.T) {
	// Far fewer records than a page holds, and a high-water mark well inside it.
	if got := pagedSlots(600, 700); got != csrPageSlots {
		t.Errorf("pagedSlots(600 records, hw 700) = %d, want %d: one page is "+
			"materialised whole however few identifiers have been issued",
			got, csrPageSlots)
	}
	// Two pages spanned, and enough records to occupy both.
	if got := pagedSlots(9000, 5000); got != 2*csrPageSlots {
		t.Errorf("pagedSlots(9000 records, hw 5000) = %d, want %d", got, 2*csrPageSlots)
	}
	// Fewer records than pages spanned: the record count is the bound, because a
	// page needs a record in it to exist.
	if got := pagedSlots(3, 100_000); got != 3*csrPageSlots {
		t.Errorf("pagedSlots(3 records, hw 100000) = %d, want %d", got, 3*csrPageSlots)
	}
}

// No page below the lowest identifier present holds a record, and the record
// stream is ascending, so one addressed read bounds the band from below. This is
// the whole of the rebuild workload's sparsity: delete the low identifiers, write
// new ones at the top, and a bound counting from zero charges for every page in
// between.
func TestPreflight_LivePagesStartAtTheLowestIdentifier(t *testing.T) {
	const nodes = 200
	burned := modelFixture(t, Options{}, nodes, 9000)

	est, err := PreflightOpen(burned)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if !est.ImageFirstNodeIDKnown {
		t.Fatal("the lowest identifier was not readable from a v9 image, so the " +
			"band is bounded from zero and this test proves nothing")
	}
	if est.ImageFirstNodeID <= csrPageSlots {
		t.Fatalf("lowest identifier is %d, inside the first page: the fixture did "+
			"not burn enough to put the records above one", est.ImageFirstNodeID)
	}

	// The records occupy at most ceil(nodes/4096)+1 pages wherever they sit, so
	// the slot term must reflect that and not the pages below them.
	pages := est.nodePages()
	if want := int64(2); pages > want {
		t.Errorf("model charges %d node pages for %d records above identifier %d; "+
			"at most %d hold anything", pages, nodes, est.ImageFirstNodeID, want)
	}

	// And the model as a whole must be close to the store, which is the check that
	// would fail if the lower bound were applied to a term it does not govern.
	s, err := Open(burned)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	actual := s.EstimateResident().Total
	model := est.HeapBytesFor(Options{})
	if model < actual {
		t.Errorf("model %d under the %d held: the lower bound was applied to a term "+
			"the lowest identifier does not govern -- the page directories are "+
			"indexed by identifier space, and a dead page still costs its entry",
			model, actual)
	}
	if ratio := float64(model) / float64(actual); ratio > 1.25 {
		t.Errorf("model %d against %d held (%.2fx): the lower bound is not being "+
			"applied, so every page up to the high-water mark is charged for",
			model, actual, ratio)
	}
}

// A store that has never been compacted has no image, so every image field is
// zero and the log is the entire cost. An estimate that stopped at the image
// reported nothing for it -- and it is exactly the shape B6's notes name as the
// way to replay into an OOM.
func TestPreflight_NeverCompactedStoreIsNotFree(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 200; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.ImageBytes != 0 {
		t.Fatalf("the fixture compacted, so this is not the never-compacted case")
	}
	if est.ImageHeapBytes != 0 {
		t.Errorf("ImageHeapBytes = %d with no image", est.ImageHeapBytes)
	}
	if est.WALHeapBytes <= 0 {
		t.Fatalf("WALHeapBytes = %d for a log holding %d records: the only heap "+
			"figure covers the image, so the whole cost of a never-compacted store "+
			"reads as nothing", est.WALHeapBytes, est.WALRecords)
	}

	// And it must bound what the store actually holds once the log is replayed.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	e := s2.EstimateResident()
	if model := est.HeapBytesFor(Options{}); model < e.Total {
		t.Errorf("model %d under the %d held after replaying the log (delta %d, "+
			"index %d)", model, e.Total, e.Delta, e.Index)
	}
}

// The log half survives a compaction: work written after one is in the log and
// lands in the delta, and the model has to cover both halves at once rather than
// whichever is larger.
func TestPreflight_CoversBothHalvesAtOnce(t *testing.T) {
	dir := modelFixture(t, Options{}, 400, 0)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 300; i++ {
		id := addNodeD(t, s, store.NodeTypeTag)
		if err := s.IndexNodeProperty(id, "late", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.ImageBytes == 0 || est.WALReplayBytes == 0 {
		t.Fatalf("fixture has image %d B and log %d B: both halves must be "+
			"populated for this to test anything", est.ImageBytes, est.WALReplayBytes)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	e := s2.EstimateResident()
	model := est.HeapBytesFor(Options{})
	if model < e.Total {
		t.Errorf("model %d under the %d held: image %d + delta %d + index %d",
			model, e.Total, e.RecordArrays, e.Delta, e.Index)
	}
}

// The lowest-identifier read only ever shrinks the estimate, so a file whose
// first record claims an identifier above its own high-water mark must not be
// believed. Two fields of the same header disagreeing is the shape that turns a
// reduction into an under-report.
func TestPreflight_FirstIdentifierAboveTheHighWaterMarkIsRefused(t *testing.T) {
	dir := modelFixture(t, Options{}, 200, 0)
	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if !est.ImageFirstNodeIDKnown {
		t.Fatal("the lowest identifier was not readable, so there is nothing to refuse")
	}
	base := est.HeapBytesFor(Options{})

	// Stamp a first identifier far above the high-water mark, leaving the header's
	// own counts alone. The model reads the record stream's first eight bytes and
	// never parses a record, so this is the whole of the change it can see.
	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(data[csrV8HeaderSize:], est.ImageNodeSeqHW+1_000_000)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}

	after, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if after.ImageFirstNodeIDKnown {
		t.Errorf("a first identifier of %d was accepted against a high-water mark "+
			"of %d", after.ImageFirstNodeID, after.ImageNodeSeqHW)
	}
	if got := after.HeapBytesFor(Options{}); got < base {
		t.Errorf("model fell from %d to %d on a file that disagrees with itself: "+
			"a bound read out of one field was applied against another that "+
			"contradicts it", base, got)
	}
}

// Under a mapped image the property blobs are addressed in the file, so they cost
// no heap: that is what R2 bought and what the arena term must be gated on. Two
// stores holding the same records with very different blobs must model alike when
// mapped and differ by the blobs when read into the heap.
//
// Asserted this way round because the obvious comparison does not test it. A heap
// image also forces a resident index, so "heap models above mapped" holds whether
// or not the arena term is gated, and a mutation charging every image for its
// arenas survived exactly that check.
func TestPreflight_MappedModelDoesNotFollowTheBlobBytes(t *testing.T) {
	const nodes = 300

	build := func(blob int) (string, OpenEstimate) {
		t.Helper()
		dir := t.TempDir()
		s, err := Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		payload := bytes.Repeat([]byte("x"), blob)
		for i := 0; i < nodes; i++ {
			if _, err := s.AddNode(&store.Node{
				Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
				Properties: payload,
			}); err != nil {
				t.Fatalf("AddNode: %v", err)
			}
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		est, err := PreflightOpen(dir)
		if err != nil {
			t.Fatalf("PreflightOpen: %v", err)
		}
		return dir, est
	}

	const small, large = 16, 4096
	_, thin := build(small)
	_, fat := build(large)

	grew := fat.ImageBytes - thin.ImageBytes
	if want := int64(nodes * (large - small) / 2); grew < want {
		t.Fatalf("the two fixtures' images differ by %d B, which is not enough "+
			"blob to tell the two modes apart", grew)
	}

	// Mapped: the difference must be a rounding, not the blobs.
	mappedThin := thin.HeapBytesFor(Options{})
	mappedFat := fat.HeapBytesFor(Options{})
	if d := mappedFat - mappedThin; d > grew/4 {
		t.Errorf("mapped model rose by %d B when the image grew by %d: the arena "+
			"term is charged to a mapped image, which is the whole of what "+
			"ImageMapped removes", d, grew)
	}

	// Heap: the difference must be the blobs, or the term is not being charged at
	// all and the mode has stopped meaning anything.
	heapThin := thin.HeapBytesFor(Options{ImageMode: ImageHeap})
	heapFat := fat.HeapBytesFor(Options{ImageMode: ImageHeap})
	if d := heapFat - heapThin; d < grew/2 {
		t.Errorf("heap model rose by only %d B when the image grew by %d: the "+
			"arena term is not charged to a heap image either", d, grew)
	}
}
