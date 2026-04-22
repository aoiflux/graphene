package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// WAL is a simple append-only write-ahead log for crash-safe node and edge writes.
// Each record is: [type:1][length:4][payload:length][crc32:4]
//
// Record types:
//   0x01 = Node record
//   0x02 = Edge record
//   0x03 = Node property index entry
//   0x04 = Edge property index entry
//   0xFF = Checkpoint (safe truncation marker after compaction)

const (
	walRecordNode       byte = 0x01
	walRecordEdge       byte = 0x02
	walRecordNodeProp   byte = 0x03
	walRecordEdgeProp   byte = 0x04
	walRecordCheckpoint byte = 0xFF

	walHeaderSize     = 1 + 4 // type(1) + length(4)
	walFooterSize     = 4     // crc32(4)
	walRecordOverhead = walHeaderSize + walFooterSize
)

// WAL manages the write-ahead log file.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// OpenWAL opens (or creates) the WAL at path.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	return &WAL{file: f}, nil
}

// AppendNode writes a node payload to the WAL.
func (w *WAL) AppendNode(payload []byte) error {
	return w.append(walRecordNode, payload)
}

// AppendEdge writes an edge payload to the WAL.
func (w *WAL) AppendEdge(payload []byte) error {
	return w.append(walRecordEdge, payload)
}

// AppendNodeProp writes a node property index entry to the WAL.
func (w *WAL) AppendNodeProp(payload []byte) error {
	return w.append(walRecordNodeProp, payload)
}

// AppendEdgeProp writes an edge property index entry to the WAL.
func (w *WAL) AppendEdgeProp(payload []byte) error {
	return w.append(walRecordEdgeProp, payload)
}

// Checkpoint writes a checkpoint marker and syncs. After compaction, a
// checkpoint signals that all records before it are durable in the CSR and
// the WAL can be safely truncated.
func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writeRecord(walRecordCheckpoint, nil); err != nil {
		return err
	}
	return w.file.Sync()
}

// Truncate removes all records from the WAL (called after successful compaction).
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Close the file before truncating: on Windows a file opened with O_APPEND
	// cannot be truncated via the file-handle Truncate call (Access is denied).
	// Closing first and using os.Truncate on the path works on all platforms.
	name := w.file.Name()
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal truncate: close: %w", err)
	}
	if err := os.Truncate(name, 0); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("wal truncate: reopen: %w", err)
	}
	w.file = f
	return nil
}

// ReplayCallbacks groups the per-record-type handlers passed to Replay.
// A nil handler causes records of that type to be silently skipped.
type ReplayCallbacks struct {
	NodeFunc     func([]byte) error // called for each 0x01 node record
	EdgeFunc     func([]byte) error // called for each 0x02 edge record
	NodePropFunc func([]byte) error // called for each 0x03 node property entry
	EdgePropFunc func([]byte) error // called for each 0x04 edge property entry
}

// Replay reads all records from the WAL from the beginning and dispatches each
// to the matching callback in cb. It stops at EOF or a checkpoint record.
// Partial/corrupted records at the tail are silently ignored (crash-safe).
func (w *WAL) Replay(cb ReplayCallbacks) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	header := make([]byte, walHeaderSize)
	footer := make([]byte, walFooterSize)

	for {
		if _, err := io.ReadFull(w.file, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		recType := header[0]
		length := binary.LittleEndian.Uint32(header[1:5])

		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(w.file, payload); err != nil {
				break // partial record at tail — stop
			}
		}

		if _, err := io.ReadFull(w.file, footer); err != nil {
			break // partial record at tail
		}

		// Verify CRC32.
		storedCRC := binary.LittleEndian.Uint32(footer)
		if computeCRC32(payload) != storedCRC {
			break // corrupted tail record
		}

		switch recType {
		case walRecordNode:
			if cb.NodeFunc != nil {
				if err := cb.NodeFunc(payload); err != nil {
					return err
				}
			}
		case walRecordEdge:
			if cb.EdgeFunc != nil {
				if err := cb.EdgeFunc(payload); err != nil {
					return err
				}
			}
		case walRecordNodeProp:
			if cb.NodePropFunc != nil {
				if err := cb.NodePropFunc(payload); err != nil {
					return err
				}
			}
		case walRecordEdgeProp:
			if cb.EdgePropFunc != nil {
				if err := cb.EdgePropFunc(payload); err != nil {
					return err
				}
			}
		case walRecordCheckpoint:
			return nil // replay complete up to last checkpoint
		}
	}
	return nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// append is the internal write path.
func (w *WAL) append(recType byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeRecord(recType, payload)
}

// writeRecord serialises and writes one WAL record. Must hold w.mu.
func (w *WAL) writeRecord(recType byte, payload []byte) error {
	length := uint32(len(payload))
	crc := computeCRC32(payload)

	buf := make([]byte, walHeaderSize+int(length)+walFooterSize)
	buf[0] = recType
	binary.LittleEndian.PutUint32(buf[1:5], length)
	if length > 0 {
		copy(buf[walHeaderSize:], payload)
	}
	binary.LittleEndian.PutUint32(buf[walHeaderSize+int(length):], crc)

	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	return nil
}

// computeCRC32 is a simple CRC32 (IEEE) implementation with no external deps.
func computeCRC32(data []byte) uint32 {
	var crc uint32 = 0xFFFFFFFF
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xEDB88320
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}
