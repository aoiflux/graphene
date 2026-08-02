package disk

// Read-only inspection of a store's files.
//
// Everything here parses graphene.csr and graphene.wal directly and never calls
// Open. That is the point: Open replays the log, rebuilds indexes, and takes a
// handle on the WAL, so inspecting a store through it means contending with
// whatever process owns it — and the moment you most want to look at a store is
// the moment something is wrong with it and a live process is still attached.
//
// These functions read. They do not repair, truncate, or write, and nothing here
// should ever learn how.

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CSRInfo describes a graphene.csr file without loading the graph it holds.
type CSRInfo struct {
	Path      string
	FileBytes int64

	Version     uint16
	NodeCount   int
	EdgeCount   int
	NodeSeqHW   uint64
	EdgeSeqHW   uint64
	IndexOffset uint64

	// Property-index entries carried inside the file (v6+).
	PropertyNodeEntries int
	PropertyEdgeEntries int

	// IDSpan is the highest node and edge ID present. Compared against the
	// counts it shows how sparse the ID space has become, which is what governs
	// the memory an open costs — the CSR indexes its arrays by ID, not by count.
	MaxNodeID uint64
	MaxEdgeID uint64
}

// InspectCSR parses the CSR image at path.
//
// path may be the file itself or the directory containing it.
func InspectCSR(path string) (CSRInfo, error) {
	p, err := resolveFile(path, csrFileName)
	if err != nil {
		return CSRInfo{}, err
	}

	fi, err := os.Stat(p)
	if err != nil {
		return CSRInfo{}, fmt.Errorf("inspect csr: %w", err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return CSRInfo{}, fmt.Errorf("inspect csr: %w", err)
	}

	info := CSRInfo{Path: p, FileBytes: fi.Size()}
	if len(data) >= csrV6HeaderSize {
		info.Version = binary.LittleEndian.Uint16(data[4:6])
		info.IndexOffset = binary.LittleEndian.Uint64(data[38:46])
	}

	// The full parse is what validates the file, so its bounds checks are the
	// report. A file that fails here is exactly what an operator wants to know.
	csr, section, err := deserialiseCSR(data)
	if err != nil {
		return info, fmt.Errorf("inspect csr: %w", err)
	}

	info.NodeCount = csr.NodeCount()
	info.EdgeCount = csr.EdgeCount()
	info.NodeSeqHW = csr.nodeSeqHW
	info.EdgeSeqHW = csr.edgeSeqHW
	if len(csr.nodes) > 0 {
		info.MaxNodeID = uint64(len(csr.nodes) - 1)
	}
	if len(csr.edges) > 0 {
		info.MaxEdgeID = uint64(len(csr.edges) - 1)
	}
	if section != nil {
		info.PropertyNodeEntries = len(section.NodeProps)
		info.PropertyEdgeEntries = len(section.EdgeProps)
	}
	return info, nil
}

// WALRecordInfo describes one framed record, as read.
type WALRecordInfo struct {
	Offset   int64
	Type     byte
	TypeName string
	Length   uint32
	CRCValid bool

	// InBatch reports whether this record sits between a begin marker and its
	// commit, which is what decides whether replay would buffer it or apply it.
	InBatch bool
}

// WALCommitInfo is the provenance a batch commit carries.
type WALCommitInfo struct {
	Offset    int64
	RecordsIn int
	Validated bool // count and body CRC matched what was read back
	HasDetail bool // false for a v1 commit record, which carries none
	CommitSeq uint64
	UnixNano  int64
	ActorID   uint64
}

// WALInfo describes a graphene.wal file.
type WALInfo struct {
	Path      string
	FileBytes int64

	Records []WALRecordInfo
	Commits []WALCommitInfo

	// Truncated reports that reading stopped before the end of the file, which
	// is the normal shape of a log after a crash: the tail record was partially
	// written or fails its checksum.
	Truncated    bool
	TruncatedAt  int64
	OpenBatch    bool // a begin marker with no commit — replay would discard it
	Checkpointed bool
}

// InspectWAL walks the log at path, reporting every record it finds.
//
// It applies nothing and stops where replay would stop, so what it reports is
// what a reopen would actually see.
func InspectWAL(path string) (WALInfo, error) {
	p, err := resolveFile(path, walFileName)
	if err != nil {
		return WALInfo{}, err
	}

	fi, err := os.Stat(p)
	if err != nil {
		return WALInfo{}, fmt.Errorf("inspect wal: %w", err)
	}
	f, err := os.Open(p)
	if err != nil {
		return WALInfo{}, fmt.Errorf("inspect wal: %w", err)
	}
	defer f.Close()

	info := WALInfo{Path: p, FileBytes: fi.Size()}
	header := make([]byte, walHeaderSize)
	footer := make([]byte, walFooterSize)

	var offset int64
	inBatch := false
	batchRecords := 0
	var batchBody []byte

	for {
		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				break
			}
			info.Truncated, info.TruncatedAt = true, offset
			break
		}
		recType := header[0]
		length := binary.LittleEndian.Uint32(header[1:5])

		if int64(length) > info.FileBytes {
			info.Truncated, info.TruncatedAt = true, offset
			break
		}
		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(f, payload); err != nil {
				info.Truncated, info.TruncatedAt = true, offset
				break
			}
		}
		if _, err := io.ReadFull(f, footer); err != nil {
			info.Truncated, info.TruncatedAt = true, offset
			break
		}

		crcValid := computeCRC32(payload) == binary.LittleEndian.Uint32(footer)
		info.Records = append(info.Records, WALRecordInfo{
			Offset:   offset,
			Type:     recType,
			TypeName: walRecordName(recType),
			Length:   length,
			CRCValid: crcValid,
			InBatch:  inBatch,
		})
		if !crcValid {
			info.Truncated, info.TruncatedAt = true, offset
			break
		}

		switch recType {
		case walRecordBatchBegin:
			inBatch, batchRecords, batchBody = true, 0, nil
		case walRecordBatchCommit:
			c := WALCommitInfo{Offset: offset, RecordsIn: batchRecords}
			if len(payload) >= walBatchCommitPayloadV1 {
				c.Validated = binary.LittleEndian.Uint32(payload[0:4]) == uint32(batchRecords) &&
					binary.LittleEndian.Uint32(payload[4:8]) == computeCRC32(batchBody)
			}
			if len(payload) == walBatchCommitPayload {
				c.HasDetail = true
				c.CommitSeq = binary.LittleEndian.Uint64(payload[8:16])
				c.UnixNano = int64(binary.LittleEndian.Uint64(payload[16:24]))
				c.ActorID = binary.LittleEndian.Uint64(payload[24:32])
			}
			info.Commits = append(info.Commits, c)
			inBatch, batchRecords, batchBody = false, 0, nil
		case walRecordCheckpoint:
			info.Checkpointed = true
		default:
			if inBatch {
				batchRecords++
				batchBody = appendRecord(batchBody, recType, payload)
			}
		}

		offset += int64(walRecordOverhead) + int64(length)
	}

	info.OpenBatch = inBatch
	return info, nil
}

// walRecordName gives a record type a readable name for a dump.
func walRecordName(t byte) string {
	switch t {
	case walRecordNode:
		return "node"
	case walRecordEdge:
		return "edge"
	case walRecordNodeProp:
		return "node-prop"
	case walRecordEdgeProp:
		return "edge-prop"
	case walRecordNodeDelete:
		return "node-delete"
	case walRecordEdgeDelete:
		return "edge-delete"
	case walRecordNodePropPurge:
		return "node-prop-purge"
	case walRecordEdgePropPurge:
		return "edge-prop-purge"
	case walRecordBatchBegin:
		return "batch-begin"
	case walRecordBatchCommit:
		return "batch-commit"
	case walRecordCheckpoint:
		return "checkpoint"
	default:
		return fmt.Sprintf("unknown(0x%02X)", t)
	}
}

// resolveFile accepts either a store directory or a file path and returns the
// file to read.
func resolveFile(path, name string) (string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("%s: %w", name, err)
	}
	if fi.IsDir() {
		p := filepath.Join(path, name)
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		return p, nil
	}
	return path, nil
}
