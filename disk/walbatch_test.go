package disk

import (
	"os"
	"path/filepath"
	"testing"
)

// Transactional framing: a batch is applied only on reaching a valid commit
// marker. These tests exercise the rollback paths directly, because that is the
// half that never runs in normal operation and would otherwise only be exercised
// by a real crash.

func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

// collect replays and records which node payloads were applied.
func collect(t *testing.T, path string) []string {
	t.Helper()
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w.Close()
	var got []string
	err = w.Replay(ReplayCallbacks{
		NodeFunc: func(p []byte) error { got = append(got, string(p)); return nil },
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return got
}

func batchOf(payloads ...string) []byte {
	b := newWALBatch(0)
	for _, p := range payloads {
		b.add(walRecordNode, []byte(p))
	}
	return mustFinish(b, batchMeta{})
}

func TestWALBatch_CommittedBatchIsApplied(t *testing.T) {
	w, path := newTestWAL(t)
	if err := w.AppendBatch(batchOf("a", "b", "c"), false); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	w.Close()

	got := collect(t, path)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("got %v, want [a b c]", got)
	}
}

// The rollback that matters: a crash after some records but before the commit.
func TestWALBatch_TruncatedBeforeCommit_AppliesNothing(t *testing.T) {
	w, path := newTestWAL(t)
	// A committed batch first, so we can prove the rollback is scoped to the
	// incomplete one rather than discarding everything.
	if err := w.AppendBatch(batchOf("keep1", "keep2"), false); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	framed := batchOf("lost1", "lost2", "lost3")
	if err := w.AppendBatch(framed, false); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	w.Close()

	// Simulate the crash: drop the commit marker (and part of the last record).
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	commitLen := walRecordOverhead + walBatchCommitPayloadV2
	if err := os.Truncate(path, fi.Size()-int64(commitLen)-3); err != nil {
		t.Fatal(err)
	}

	got := collect(t, path)
	if len(got) != 2 || got[0] != "keep1" || got[1] != "keep2" {
		t.Fatalf("got %v, want only the committed batch [keep1 keep2]", got)
	}
}

// A commit marker that is itself torn must not commit the batch.
func TestWALBatch_TruncatedInsideCommit_AppliesNothing(t *testing.T) {
	w, path := newTestWAL(t)
	if err := w.AppendBatch(batchOf("x", "y"), false); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	w.Close()

	fi, _ := os.Stat(path)
	// Remove the last 2 bytes: the commit record's own CRC is now short.
	if err := os.Truncate(path, fi.Size()-2); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, path); len(got) != 0 {
		t.Fatalf("got %v, want nothing applied", got)
	}
}

// A commit whose count or CRC disagrees with the body must be rejected rather
// than trusted — that is what distinguishes "the commit is present" from "the
// batch is intact".
func TestWALBatch_CommitDisagreeingWithBody_IsRejected(t *testing.T) {
	w, path := newTestWAL(t)
	framed := batchOf("p", "q")
	// Corrupt one payload byte in the body, leaving that record's own CRC stale
	// too — replay stops on a bad record CRC, so instead corrupt the *commit* to
	// claim a different count.
	framed[len(framed)-walFooterSize-walBatchCommitPayloadV2] = 99 // count := 99
	// Recompute the commit record's own CRC so it parses cleanly and only the
	// batch-level check can catch it.
	commitPayloadAt := len(framed) - walFooterSize - walBatchCommitPayloadV2
	crc := computeCRC32(framed[commitPayloadAt : commitPayloadAt+walBatchCommitPayloadV2])
	framed[len(framed)-4] = byte(crc)
	framed[len(framed)-3] = byte(crc >> 8)
	framed[len(framed)-2] = byte(crc >> 16)
	framed[len(framed)-1] = byte(crc >> 24)

	if err := w.AppendBatch(framed, false); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	w.Close()

	if got := collect(t, path); len(got) != 0 {
		t.Fatalf("got %v, want nothing — the commit disagrees with the body", got)
	}
}

// Single records written around a batch keep their ordering.
func TestWALBatch_InterleavesWithSingleRecords(t *testing.T) {
	w, path := newTestWAL(t)
	if err := w.AppendNode([]byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendBatch(batchOf("in1", "in2"), false); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendNode([]byte("after")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	got := collect(t, path)
	want := []string{"before", "in1", "in2", "after"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order wrong: got %v, want %v", got, want)
		}
	}
}

// Unknown record types must fail replay loudly rather than being skipped. A
// silent skip would let a build that does not understand batch markers apply a
// rolled-back batch.
func TestWALReplay_UnknownRecordTypeIsRejected(t *testing.T) {
	w, path := newTestWAL(t)
	if err := w.AppendNode([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(appendRecord(nil, 0x7E, []byte("mystery")))
	f.Close()

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	err = w2.Replay(ReplayCallbacks{NodeFunc: func([]byte) error { return nil }})
	if err == nil {
		t.Fatal("expected replay to reject an unknown record type")
	}
}

func TestWALBatch_EmptyBatchIsANoOp(t *testing.T) {
	w, path := newTestWAL(t)
	b := newWALBatch(0)
	if !b.empty() {
		t.Fatal("new batch should be empty")
	}
	if err := w.AppendBatch(mustFinish(b, batchMeta{}), false); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	w.Close()
	if got := collect(t, path); len(got) != 0 {
		t.Fatalf("got %v, want nothing", got)
	}
}

// mustFinish is finish for tests that configure no Signer, where the error
// return is unreachable.
func mustFinish(b *walBatch, meta batchMeta) []byte {
	framed, err := b.finish(meta)
	if err != nil {
		panic("finish without a signer must not fail: " + err.Error())
	}
	return framed
}
