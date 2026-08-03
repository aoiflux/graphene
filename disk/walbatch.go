package disk

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

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
//	v1 (8 bytes)    count:4 crc:4
//	v2 (32 bytes)   count:4 crc:4 commitSeq:8 tsUnixNano:8 actorID:8
//	v3 (106 bytes)  v2 fields, then keyID:8 sigLen:2 sig:64
//
// v2 exists so a committed transaction records when it happened and who made
// it, not only what changed. All three fields landed together deliberately:
// each one alone would have been the same format change, and paying for it
// three times is the mistake worth avoiding.
//
// v3 carries an Ed25519 signature over the commit's metadata and a hash of the
// batch body.
//
// # Why the signature lives here rather than in a record of its own
//
// The plan sketched both: §11.4 put it in the commit payload, §11.6 I-3 gave it
// a reserved record type. The deciding argument is ordering. Replay applies a
// batch the moment its commit validates, so a signature carried in a *following*
// record would arrive after the records it vouches for had already been applied
// — replay would have to defer application on a one-record lookahead, or accept
// applying a batch it later finds forged. In the payload, the signature is
// checked at exactly the point the batch is admitted.
//
// Older readers reject a v2 or v3 log, which is intended and is the same rule
// knownWALRecord applies to unknown record types — a build that cannot see the
// commit metadata, or cannot check a signature, must not silently present the
// log as if it had. Newer readers accept older logs, so an existing store still
// opens after an upgrade.

// walBatchBeginPayload is the begin marker's payload: the record count.
const walBatchBeginPayload = 4

const (
	// walBatchCommitPayloadV1 is the original layout: count then CRC.
	walBatchCommitPayloadV1 = 8

	// walBatchCommitPayloadV2 adds the commit sequence number, the wall-clock
	// time of the commit, and the actor.
	walBatchCommitPayloadV2 = walBatchCommitPayloadV1 + 8 + 8 + 8

	// walSignatureSize is an Ed25519 signature.
	walSignatureSize = 64

	// walBatchCommitPayloadV3 adds the signing key and the signature. Written
	// only when a Signer is configured; an unsigned store keeps writing v2.
	walBatchCommitPayloadV3 = walBatchCommitPayloadV2 + 8 + 2 + walSignatureSize
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

	// Signer signs the commit. Nil means unsigned, which writes a v2 payload —
	// signing is opt-in because it costs a key the caller may not have and a
	// signature per transaction the caller may not want.
	Signer store.Signer

	// KeyID and Signature are populated on read, not on write. On write the
	// Signer supplies them.
	KeyID     uint64
	Signature []byte
}

// signedCommitData is the byte string a commit signature covers.
//
// It binds the commit's identity (sequence, time, actor, key) to a hash of the
// batch body, so a signature cannot be lifted onto a different batch, replayed
// at a different sequence number, or reattributed to another actor. The leading
// domain byte keeps it from colliding with any other signed structure the engine
// might later define.
func signedCommitData(commitSeq uint64, unixNano int64, actorID, keyID uint64, bodyHash [sha256.Size]byte) []byte {
	buf := make([]byte, 0, 1+8+8+8+8+sha256.Size)
	buf = append(buf, walRecordBatchCommit) // domain tag
	buf = binary.LittleEndian.AppendUint64(buf, commitSeq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(unixNano))
	buf = binary.LittleEndian.AppendUint64(buf, actorID)
	buf = binary.LittleEndian.AppendUint64(buf, keyID)
	return append(buf, bodyHash[:]...)
}

// walBatch accumulates framed records for a single atomic append.
//
// Records are framed directly into one growing buffer, so a batch costs one
// amortised allocation rather than the three per record the per-call path pays
// (marshal, defensive copy, frame).
type walBatch struct {
	buf   []byte
	count uint32
	// framing must match the file this batch will be appended to; a log holding
	// two framings could not be replayed under either.
	framing uint16
	// beginEnd marks where the batch body starts, so the commit CRC can be taken
	// over exactly the bytes replay will read back.
	beginEnd int
}

// newWALBatch returns a batch builder under the original framing, for tests and
// hand-assembled fixtures.
func newWALBatch(hint int) *walBatch {
	return newWALBatchFramed(hint, walFramingV1)
}

// newWALBatchFramed returns a batch builder with room reserved for hint bytes.
func newWALBatchFramed(hint int, framing uint16) *walBatch {
	b := &walBatch{
		buf:     make([]byte, 0, hint+walRecordOverhead*2+walBatchBeginPayload+walBatchCommitPayloadV3),
		framing: framing,
	}
	// Reserve the begin marker; count is patched in by finish().
	b.buf = appendRecordFramed(b.buf, framing, walRecordBatchBegin, make([]byte, walBatchBeginPayload))
	b.beginEnd = len(b.buf)
	return b
}

// add frames one record into the batch.
func (b *walBatch) add(recType byte, payload []byte) {
	b.buf = appendRecordFramed(b.buf, b.framing, recType, payload)
	b.count++
}

// empty reports whether any records were added.
func (b *walBatch) empty() bool { return b.count == 0 }

// finish patches the begin marker's count, appends the commit marker carrying
// meta, and returns the complete framed buffer ready for a single write.
//
// Returns an error only when a configured Signer fails. That error must abort
// the commit rather than fall back to writing unsigned: a store configured to
// sign that silently stops signing is the worst outcome available, because
// nothing downstream can tell the difference between "not signed" and "signing
// broke".
func (b *walBatch) finish(meta batchMeta) ([]byte, error) {
	// Patch the count into the reserved begin payload.
	binary.LittleEndian.PutUint32(b.buf[walHeaderSize:walHeaderSize+4], b.count)
	// The begin record's own CRC covers its payload, so it has to be recomputed
	// now that the count is no longer zero.
	beginCRCAt := walHeaderSize + walBatchBeginPayload
	binary.LittleEndian.PutUint32(b.buf[beginCRCAt:beginCRCAt+4],
		recordCRC(b.framing, walRecordBatchBegin, b.buf[walHeaderSize:walHeaderSize+walBatchBeginPayload]))

	// The CRC covers the body only, exactly as it did in v1. Extending it over
	// the metadata would be no stronger — the checksum sits in the same record
	// as the bytes it describes, so it detects damage and not authorship. That
	// is a signature's job, which is what the v3 payload below carries.
	body := b.buf[b.beginEnd:]

	size := walBatchCommitPayloadV2
	if meta.Signer != nil {
		size = walBatchCommitPayloadV3
	}
	commit := make([]byte, size)
	binary.LittleEndian.PutUint32(commit[0:4], b.count)
	binary.LittleEndian.PutUint32(commit[4:8], computeCRC32(body))
	binary.LittleEndian.PutUint64(commit[8:16], meta.CommitSeq)
	binary.LittleEndian.PutUint64(commit[16:24], uint64(meta.UnixNano))
	binary.LittleEndian.PutUint64(commit[24:32], meta.ActorID)

	if meta.Signer != nil {
		keyID := meta.Signer.KeyID()
		sig, err := meta.Signer.Sign(
			signedCommitData(meta.CommitSeq, meta.UnixNano, meta.ActorID, keyID, sha256.Sum256(body)))
		if err != nil {
			return nil, fmt.Errorf("sign commit %d: %w", meta.CommitSeq, err)
		}
		if len(sig) != walSignatureSize {
			return nil, fmt.Errorf("sign commit %d: signature is %d bytes, expected %d",
				meta.CommitSeq, len(sig), walSignatureSize)
		}
		binary.LittleEndian.PutUint64(commit[32:40], keyID)
		binary.LittleEndian.PutUint16(commit[40:42], uint16(len(sig)))
		copy(commit[42:], sig)
	}

	b.buf = appendRecordFramed(b.buf, b.framing, walRecordBatchCommit, commit)
	return b.buf, nil
}

// appendRecord frames one record onto dst under the given framing and returns
// the extended slice.
//
// Framing is a parameter rather than a constant because a batch's records must
// be checksummed by the same rule as the file they are appended to — a log
// containing two framings could not be replayed by either.
func appendRecordFramed(dst []byte, framing uint16, recType byte, payload []byte) []byte {
	var hdr [walHeaderSize]byte
	hdr[0] = recType
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, payload...)
	var crc [walFooterSize]byte
	binary.LittleEndian.PutUint32(crc[:], recordCRC(framing, recType, payload))
	return append(dst, crc[:]...)
}

// appendRecord frames a record under the original framing.
//
// Retained for tests and for hand-assembled fixtures, which predate the
// container and are simpler to reason about under v1.
func appendRecord(dst []byte, recType byte, payload []byte) []byte {
	return appendRecordFramed(dst, walFramingV1, recType, payload)
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
	binary.LittleEndian.PutUint32(crc[:], recordCRC(b.framing, recType, b.buf[payloadStart:]))
	b.buf = append(b.buf, crc[:]...)
	b.count++
}
