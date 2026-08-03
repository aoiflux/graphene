package disk

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func newByteReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// Commit provenance: the sequence number, wall-clock time, and actor now carried
// by a batch commit record.

// replayMeta collects every commit record a log reports.
func replayMeta(t *testing.T, data []byte) ([]batchMeta, int) {
	t.Helper()
	var metas []batchMeta
	applied := 0
	count := func([]byte) error { applied++; return nil }
	err := replayRecords(newByteReader(data), int64(len(data)), walFramingV1, ReplayCallbacks{
		CommitFunc:     func(m batchMeta) { metas = append(metas, m) },
		NodeFunc:       count,
		EdgeFunc:       count,
		NodePropFunc:   count,
		EdgePropFunc:   count,
		NodeDeleteFunc: count,
		EdgeDeleteFunc: count,
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return metas, applied
}

// A committed batch carries its sequence number, timestamp, and actor, and
// replay reports them.
func TestCommitMeta_RoundTripsThroughTheLog(t *testing.T) {
	b := newWALBatch(64)
	b.add(walRecordNode, []byte("one"))
	b.add(walRecordEdge, []byte("two"))
	want := batchMeta{CommitSeq: 42, UnixNano: 1_750_000_000_000_000_000, ActorID: 7}

	metas, applied := replayMeta(t, mustFinish(b, want))

	if applied != 2 {
		t.Fatalf("applied %d records, want 2", applied)
	}
	if len(metas) != 1 {
		t.Fatalf("got %d commit records, want 1", len(metas))
	}
	got := metas[0]
	if got.CommitSeq != want.CommitSeq || got.UnixNano != want.UnixNano || got.ActorID != want.ActorID {
		t.Fatalf("commit metadata round-tripped as seq=%d ts=%d actor=%d, want seq=%d ts=%d actor=%d",
			got.CommitSeq, got.UnixNano, got.ActorID, want.CommitSeq, want.UnixNano, want.ActorID)
	}
	if len(got.Signature) != 0 {
		t.Fatalf("an unsigned commit reported a %d-byte signature", len(got.Signature))
	}
}

// A log written before the commit record carried provenance must still replay.
// Rejecting it would mean an existing store stops opening after an upgrade,
// which is a far worse failure than the metadata being absent.
func TestCommitMeta_LegacyV1LogStillReplays(t *testing.T) {
	// Hand-build a v1 batch: begin, two records, then an 8-byte commit.
	var buf []byte
	beginPayload := make([]byte, walBatchBeginPayload)
	binary.LittleEndian.PutUint32(beginPayload, 2)
	buf = appendRecord(buf, walRecordBatchBegin, beginPayload)

	bodyStart := len(buf)
	buf = appendRecord(buf, walRecordNode, []byte("one"))
	buf = appendRecord(buf, walRecordEdge, []byte("two"))
	body := buf[bodyStart:]

	commit := make([]byte, walBatchCommitPayloadV1)
	binary.LittleEndian.PutUint32(commit[0:4], 2)
	binary.LittleEndian.PutUint32(commit[4:8], computeCRC32(body))
	buf = appendRecord(buf, walRecordBatchCommit, commit)

	metas, applied := replayMeta(t, buf)

	if applied != 2 {
		t.Fatalf("a v1 log applied %d records, want 2 — an existing store would not reopen", applied)
	}
	if len(metas) != 0 {
		t.Fatalf("a v1 commit reported %d metadata records; it carries none", len(metas))
	}
}

// A commit whose CRC does not describe its body is discarded, and discarding it
// must also discard its metadata — otherwise the store's commit sequence would
// advance for a transaction that never happened.
func TestCommitMeta_DiscardedBatchReportsNothing(t *testing.T) {
	b := newWALBatch(64)
	b.add(walRecordNode, []byte("one"))
	framed := mustFinish(b, batchMeta{CommitSeq: 99, ActorID: 5})

	// Corrupt the commit's body CRC, leaving the record itself intact so it
	// passes its own checksum and reaches the count/CRC comparison.
	commitPayloadAt := len(framed) - walFooterSize - walBatchCommitPayloadV2
	binary.LittleEndian.PutUint32(framed[commitPayloadAt+4:commitPayloadAt+8], 0xDEADBEEF)
	crcAt := len(framed) - walFooterSize
	binary.LittleEndian.PutUint32(framed[crcAt:],
		computeCRC32(framed[commitPayloadAt:commitPayloadAt+walBatchCommitPayloadV2]))

	metas, applied := replayMeta(t, framed)

	if applied != 0 {
		t.Fatalf("applied %d records from a batch whose commit does not match its body", applied)
	}
	if len(metas) != 0 {
		t.Fatalf("a discarded batch reported %d commit records; it committed nothing", len(metas))
	}
}

// Commit sequence numbers are unique and increasing within a store.
func TestCommitMeta_SequenceIsMonotonic(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	var seen []uint64
	for i := 0; i < 5; i++ {
		m := s.nextCommitMeta(store.TxContext{})
		seen = append(seen, m.CommitSeq)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("commit sequence went %d -> %d", seen[i-1], seen[i])
		}
	}
}

// Reopening must not reissue sequence numbers the log already used.
func TestCommitMeta_ResumesPastTheLog(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	s.observeCommitMeta(batchMeta{CommitSeq: 100})
	if got := s.nextCommitMeta(store.TxContext{}).CommitSeq; got != 101 {
		t.Fatalf("after observing sequence 100 the next issued was %d, want 101", got)
	}

	// An older value must not move the counter backwards.
	s.observeCommitMeta(batchMeta{CommitSeq: 3})
	if got := s.nextCommitMeta(store.TxContext{}).CommitSeq; got != 102 {
		t.Fatalf("an older sequence moved the counter: next issued %d, want 102", got)
	}
}

// An attributed transaction records its actor; the timestamp is real.
func TestCommitMeta_TransactionRecordsItsActor(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	const pinned = int64(1_700_000_000_123_456_789)
	s.nowUnixNano = func() int64 { return pinned }

	meta := s.nextCommitMeta(store.TxContext{ActorID: 4242, RoleID: 9, KeyID: 1})
	if meta.ActorID != 4242 {
		t.Fatalf("actor recorded as %d, want 4242", meta.ActorID)
	}
	if meta.UnixNano != pinned {
		t.Fatalf("timestamp recorded as %d, want %d", meta.UnixNano, pinned)
	}
}
