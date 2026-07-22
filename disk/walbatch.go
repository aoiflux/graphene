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

// walBatchBeginPayload is the begin marker's payload: the record count.
const walBatchBeginPayload = 4

// walBatchCommitPayload is the commit marker's payload: count then CRC.
const walBatchCommitPayload = 8

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

// finish patches the begin marker's count, appends the commit marker, and
// returns the complete framed buffer ready for a single write.
func (b *walBatch) finish() []byte {
	// Patch the count into the reserved begin payload.
	binary.LittleEndian.PutUint32(b.buf[walHeaderSize:walHeaderSize+4], b.count)
	// The begin record's own CRC covers its payload, so it has to be recomputed
	// now that the count is no longer zero.
	beginCRCAt := walHeaderSize + walBatchBeginPayload
	binary.LittleEndian.PutUint32(b.buf[beginCRCAt:beginCRCAt+4],
		computeCRC32(b.buf[walHeaderSize:walHeaderSize+walBatchBeginPayload]))

	body := b.buf[b.beginEnd:]
	commit := make([]byte, walBatchCommitPayload)
	binary.LittleEndian.PutUint32(commit[0:4], b.count)
	binary.LittleEndian.PutUint32(commit[4:8], computeCRC32(body))
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
