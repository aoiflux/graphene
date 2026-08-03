package disk

// The WAL file container: a header, and framing that checksums what it frames.
//
// # What this fixes
//
// The original framing is [type:1][length:4][payload][crc32:4], and the CRC
// covers **the payload only**. Neither the type nor the length is checksummed,
// which has a consequence the crash-injection sweep found and characterised:
// flipping the type byte of a batch-begin marker turns it into an ordinary
// record, so replay never enters batch mode and the records that were meant to
// be buffered are applied one by one. For a transaction that was in flight when
// the process died — the one the begin/commit machinery exists to discard —
// that is a half-applied batch, and the record's CRC verifies the whole way
// because the payload it covers did not change.
//
// The fix is to checksum the header along with the payload. It is two lines. Its
// cost is that it changes what every record's CRC covers, so every existing log
// fails to validate under the new rule — and until now there was nowhere to say
// which rule a given file uses.
//
// # The container
//
// A file header, so framing can be versioned:
//
//	offset  size  field
//	0       4     magic "GWAL"
//	4       2     version
//	6       8     segment sequence number
//	8+      32    previous segment digest (zero for the first)
//	46      4     header CRC32 over bytes 0..41
//	50            end of header; records follow
//
// The segment fields are unused by anything today. They are here because R-F6
// and WAL segmentation both need a container, the plan is explicit that a format
// change should be spent once rather than three times, and a header added later
// would be a second migration for stores that had already taken the first.
//
// # Detecting a headerless file
//
// An existing log begins with a record type byte. Valid types are 0x01–0x0A,
// 0x0D and 0xFF; "GWAL" begins with 0x47, which is none of them. So the magic is
// an unambiguous discriminator and no version field is needed to find the
// version — a file either starts with the magic or it is the original format.
//
// Framing is therefore a property of the file, fixed when it is opened. An
// existing log keeps v1 framing for records appended to it, because rewriting
// its earlier records is not on the table. A store upgrades when compaction
// replaces the log, which is the migration path: no explicit step, and no file
// ever contains both framings.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	walMagic = "GWAL"

	// walFramingV1 is the original: CRC over the payload only. Written to files
	// that already exist without a header.
	walFramingV1 = 1

	// walFramingV2 checksums the record header along with the payload.
	walFramingV2 = 2

	walHeaderMagicSize = 4
	walFileHeaderSize  = 50
	walPrevDigestSize  = 32
)

// walFileHeader is the container header.
type walFileHeader struct {
	Version    uint16
	SegmentSeq uint64
	PrevDigest [walPrevDigestSize]byte
}

func appendWALFileHeader(h walFileHeader) []byte {
	buf := make([]byte, walFileHeaderSize)
	copy(buf[0:4], walMagic)
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	binary.LittleEndian.PutUint64(buf[6:14], h.SegmentSeq)
	copy(buf[14:46], h.PrevDigest[:])
	binary.LittleEndian.PutUint32(buf[46:50], computeCRC32(buf[0:46]))
	return buf
}

// readWALFileHeader reports the framing a file uses.
//
// A file shorter than the header, or one not starting with the magic, is the
// original format — not an error. That is the common case for any store written
// before this existed.
func readWALFileHeader(f *os.File) (walFileHeader, int64, error) {
	var h walFileHeader

	buf := make([]byte, walFileHeaderSize)
	n, err := f.ReadAt(buf, 0)
	if err != nil && n < walHeaderMagicSize {
		// Too short to carry a magic: headerless, which includes an empty file.
		return walFileHeader{Version: walFramingV1}, 0, nil
	}
	if string(buf[0:4]) != walMagic {
		return walFileHeader{Version: walFramingV1}, 0, nil
	}
	if n < walFileHeaderSize {
		return h, 0, fmt.Errorf("wal: header truncated at %d bytes, need %d", n, walFileHeaderSize)
	}
	if got, want := binary.LittleEndian.Uint32(buf[46:50]), computeCRC32(buf[0:46]); got != want {
		return h, 0, fmt.Errorf("wal: header checksum mismatch")
	}

	h.Version = binary.LittleEndian.Uint16(buf[4:6])
	h.SegmentSeq = binary.LittleEndian.Uint64(buf[6:14])
	copy(h.PrevDigest[:], buf[14:46])

	if h.Version != walFramingV2 {
		return h, 0, fmt.Errorf("wal: framing version %d, this build understands %d and headerless v%d",
			h.Version, walFramingV2, walFramingV1)
	}
	return h, walFileHeaderSize, nil
}

// recordCRC computes a record's checksum under the given framing.
//
// v1 covers the payload alone. v2 covers the type and length too, which is what
// stops a flipped type byte from silently changing a record's meaning while its
// checksum still verifies.
func recordCRC(framing uint16, recType byte, payload []byte) uint32 {
	if framing < walFramingV2 {
		return computeCRC32(payload)
	}
	// Incremental rather than over a concatenated buffer: this runs per record on
	// the write path, and an allocation per record is exactly what the batch
	// framing was built to avoid.
	var hdr [walHeaderSize]byte
	hdr[0] = recType
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(payload)))
	return crc32.Update(crc32.ChecksumIEEE(hdr[:]), crc32.IEEETable, payload)
}
