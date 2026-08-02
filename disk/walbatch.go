package disk

import "encoding/binary"

// Transactional batch framing.
//
// A batch is written to the WAL as one contiguous run:
//
//	[0x09 begin | count]  [rec 1] [rec 2] … [rec N]  [0x0A commit | count | crc]
//
// Replay buffers everything between the markers and applies it only on reaching
// a valid commit. Reaching EOF first discards the buffer — that discard *is* the
// rollback, and it costs nothing because none of it was applied.
//
// The commit carries both the record count and a CRC over the batch body, so a
// batch truncated mid-record and a batch whose commit marker is itself torn are
// both detected rather than silently half-applied.
//
// # Commit payload layouts
//
// The commit marker's payload has grown once. Its length is what distinguishes
// the versions, which is why replay accepts a set of lengths rather than one:
//
//	v1 (8 bytes)   count:4 crc:4
//	v2 (32 bytes)  count:4 crc:4 commitSeq:8 tsUnixNano:8 actorID:8
//
// v2 exists so a committed transaction records when it happened and who made
// it, not only what changed. All three fields landed together deliberately:
// each one alone would have been the same format change, and paying for it
// three times is the mistake worth avoiding.
//
// Older readers reject a v2 log, which is intended and is the same rule
// knownWALRecord applies to unknown record types — a build that cannot see the
// commit metadata must not silently present the log as if it had. Newer readers
// accept a v1 log, so an existing store still opens after an upgrade.

// walBatchBeginPayload is the begin marker's payload: the record count.
const walBatchBeginPayload = 4

const (
	// walBatchCommitPayloadV1 is the original layout: count then CRC.
	walBatchCommitPayloadV1 = 8

	// walBatchCommitPayload is the layout written today: V1 plus the commit
	// sequence number, the wall-clock time of the commit, and the actor.
	walBatchCommitPayload = walBatchCommitPayloadV1 + 8 + 8 + 8
)

// batchMeta is the provenance recorded against one commit.
//
// CommitSeq is monotonic per store. It is currently only monotonic within a WAL
// generation: Compact truncates the log, and the CSR header has nowhere to keep
// a high-water mark, so the counter restarts from whatever the surviving log
// replays. Persisting it needs a CSR header field, which is deliberately being
// held for the single v8 format change that also carries the digest and
// attestation sections rather than being spent on its own bump. Until then,
// treat CommitSeq as ordering within a generation, not as a durable identity.
type batchMeta struct {
	CommitSeq uint64
	UnixNano  int64
	ActorID   uint64
}

// walBatch accumulates framed records for a single atomic append.
//
// Records are framed directly into one growing buffer, so a batch costs one
// amortised allocation rather than the three per record the per-call path pays
// (marshal, defensive copy, frame).
type walBatch struct {
	buf   []byte
	count uint32
	// beginEnd marks where the batch body starts, so the commit CRC can be taken
	// over exactly the bytes replay will read back.
	beginEnd int
}

// newWALBatch returns a batch builder with room reserved for hint bytes.
func newWALBatch(hint int) *walBatch {
	b := &walBatch{buf: make([]byte, 0, hint+walRecordOverhead*2+walBatchBeginPayload+walBatchCommitPayload)}
	// Reserve the begin marker; count is patched in by finish().
	b.buf = appendRecord(b.buf, walRecordBatchBegin, make([]byte, walBatchBeginPayload))
	b.beginEnd = len(b.buf)
	return b
}

// add frames one record into the batch.
func (b *walBatch) add(recType byte, payload []byte) {
	b.buf = appendRecord(b.buf, recType, payload)
	b.count++
}

// empty reports whether any records were added.
func (b *walBatch) empty() bool { return b.count == 0 }

// finish patches the begin marker's count, appends the commit marker carrying
// meta, and returns the complete framed buffer ready for a single write.
func (b *walBatch) finish(meta batchMeta) []byte {
	// Patch the count into the reserved begin payload.
	binary.LittleEndian.PutUint32(b.buf[walHeaderSize:walHeaderSize+4], b.count)
	// The begin record's own CRC covers its payload, so it has to be recomputed
	// now that the count is no longer zero.
	beginCRCAt := walHeaderSize + walBatchBeginPayload
	binary.LittleEndian.PutUint32(b.buf[beginCRCAt:beginCRCAt+4],
		computeCRC32(b.buf[walHeaderSize:walHeaderSize+walBatchBeginPayload]))

	// The CRC covers the body only, exactly as it did in v1. Extending it over
	// the metadata would be no stronger — the checksum sits in the same record
	// as the bytes it describes, so it detects damage and not authorship. Making
	// the metadata trustworthy is a signature's job, and the layout leaves that
	// to the record type reserved for it.
	body := b.buf[b.beginEnd:]
	commit := make([]byte, walBatchCommitPayload)
	binary.LittleEndian.PutUint32(commit[0:4], b.count)
	binary.LittleEndian.PutUint32(commit[4:8], computeCRC32(body))
	binary.LittleEndian.PutUint64(commit[8:16], meta.CommitSeq)
	binary.LittleEndian.PutUint64(commit[16:24], uint64(meta.UnixNano))
	binary.LittleEndian.PutUint64(commit[24:32], meta.ActorID)
	b.buf = appendRecord(b.buf, walRecordBatchCommit, commit)
	return b.buf
}

// appendRecord frames one record onto dst and returns the extended slice.
func appendRecord(dst []byte, recType byte, payload []byte) []byte {
	var hdr [walHeaderSize]byte
	hdr[0] = recType
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, payload...)
	var crc [walFooterSize]byte
	binary.LittleEndian.PutUint32(crc[:], computeCRC32(payload))
	return append(dst, crc[:]...)
}

// addWith frames a record whose payload is written directly into the batch
// buffer, so the record costs no allocation of its own.
//
// The length is not known until the payload has been written, so the header is
// reserved and backfilled. Everything else — including the CRC — is computed
// over the bytes already in place, which is what removes both the per-record
// buffer and the copy into the frame.
func (b *walBatch) addWith(recType byte, marshal func(dst []byte) []byte) {
	start := len(b.buf)
	b.buf = append(b.buf, recType, 0, 0, 0, 0) // type + length placeholder
	payloadStart := len(b.buf)

	b.buf = marshal(b.buf)

	payloadLen := len(b.buf) - payloadStart
	binary.LittleEndian.PutUint32(b.buf[start+1:start+walHeaderSize], uint32(payloadLen))

	var crc [walFooterSize]byte
	binary.LittleEndian.PutUint32(crc[:], computeCRC32(b.buf[payloadStart:]))
	b.buf = append(b.buf, crc[:]...)
	b.count++
}
