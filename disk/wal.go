package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
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
	mu      sync.Mutex
	writeMu sync.Mutex
	file    *os.File

	ringMask uint64
	ring     []walSlot
	head     atomic.Uint64 // next sequence to reserve
	tail     atomic.Uint64 // next sequence to consume/write

	barrier  atomic.Uint32 // 1 while maintenance op is active
	inFlight atomic.Int64  // append calls currently in progress
	closed   atomic.Uint32 // 1 once Close() starts
}

type walSlot struct {
	seq     atomic.Uint64
	ready   atomic.Uint32
	recType byte
	payload []byte
}

const defaultWALRingCapacity = 1024

// OpenWAL opens (or creates) the WAL at path.
func OpenWAL(path string) (*WAL, error) {
	return openWALWithCapacity(path, defaultWALRingCapacity)
}

func openWALWithCapacity(path string, capacity int) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}

	capPow2 := nextPowerOfTwo(capacity)
	if capPow2 < 2 {
		capPow2 = 2
	}

	w := &WAL{
		file:     f,
		ring:     make([]walSlot, capPow2),
		ringMask: uint64(capPow2 - 1),
	}
	for i := range w.ring {
		w.ring[i].seq.Store(uint64(i))
	}
	return w, nil
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
	if err := w.beginMaintenance(); err != nil {
		return err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return err
	}
	if err := w.writeRecord(walRecordCheckpoint, nil); err != nil {
		return err
	}
	return w.file.Sync()
}

// Truncate removes all records from the WAL (called after successful compaction).
func (w *WAL) Truncate() error {
	if err := w.beginMaintenance(); err != nil {
		return err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return err
	}

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
	if err := w.beginMaintenance(); err != nil {
		return err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return err
	}

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
	if err := w.beginMaintenance(); err != nil {
		return err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return err
	}

	syncErr := w.file.Sync()
	w.closed.Store(1)
	closeErr := w.file.Close()

	if syncErr != nil && closeErr != nil {
		return fmt.Errorf("wal close: sync: %v; close: %w", syncErr, closeErr)
	}
	if syncErr != nil {
		return fmt.Errorf("wal close: sync: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("wal close: %w", closeErr)
	}
	return nil
}

// append is the internal write path.
func (w *WAL) append(recType byte, payload []byte) error {
	if w.closed.Load() != 0 {
		return fmt.Errorf("wal append: closed")
	}

	if !w.enterAppend() {
		return fmt.Errorf("wal append: closed")
	}
	defer w.inFlight.Add(-1)

	var copied []byte
	if len(payload) > 0 {
		copied = make([]byte, len(payload))
		copy(copied, payload)
	}

	if err := w.enqueue(recType, copied); err != nil {
		return err
	}

	if w.writeMu.TryLock() {
		err := w.drainQueuedLocked()
		w.writeMu.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
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

func (w *WAL) beginMaintenance() error {
	w.mu.Lock()
	if w.closed.Load() != 0 {
		w.mu.Unlock()
		return fmt.Errorf("wal closed")
	}
	w.barrier.Store(1)
	for w.inFlight.Load() != 0 {
		runtime.Gosched()
	}
	w.writeMu.Lock()
	return nil
}

func (w *WAL) endMaintenance() {
	w.writeMu.Unlock()
	w.barrier.Store(0)
	w.mu.Unlock()
}

func (w *WAL) enterAppend() bool {
	for {
		if w.closed.Load() != 0 {
			return false
		}
		if w.barrier.Load() != 0 {
			runtime.Gosched()
			continue
		}
		w.inFlight.Add(1)
		if w.barrier.Load() == 0 {
			return true
		}
		w.inFlight.Add(-1)
	}
}

func (w *WAL) enqueue(recType byte, payload []byte) error {
	for {
		if seq, ok := w.tryReserve(); ok {
			slot := &w.ring[seq&w.ringMask]
			slot.recType = recType
			slot.payload = payload
			slot.seq.Store(seq)
			slot.ready.Store(1)
			return nil
		}

		// Overflow path: lock and drain until there is room.
		w.writeMu.Lock()
		if err := w.drainQueuedLocked(); err != nil {
			w.writeMu.Unlock()
			return err
		}
		for {
			if seq, ok := w.tryReserve(); ok {
				slot := &w.ring[seq&w.ringMask]
				slot.recType = recType
				slot.payload = payload
				slot.seq.Store(seq)
				slot.ready.Store(1)
				if err := w.drainQueuedLocked(); err != nil {
					w.writeMu.Unlock()
					return err
				}
				w.writeMu.Unlock()
				return nil
			}
			if err := w.drainQueuedLocked(); err != nil {
				w.writeMu.Unlock()
				return err
			}
			runtime.Gosched()
		}
	}
}

func (w *WAL) tryReserve() (uint64, bool) {
	capacity := uint64(len(w.ring))
	for {
		head := w.head.Load()
		tail := w.tail.Load()
		if head-tail >= capacity {
			return 0, false
		}
		if w.head.CompareAndSwap(head, head+1) {
			return head, true
		}
	}
}

// drainQueuedLocked writes all ready records in sequence order. Caller must hold writeMu.
func (w *WAL) drainQueuedLocked() error {
	for {
		tail := w.tail.Load()
		head := w.head.Load()
		if tail >= head {
			return nil
		}

		slot := &w.ring[tail&w.ringMask]
		if slot.seq.Load() != tail || slot.ready.Load() == 0 {
			return nil
		}

		if err := w.writeRecord(slot.recType, slot.payload); err != nil {
			return err
		}

		slot.payload = nil
		slot.ready.Store(0)
		w.tail.Store(tail + 1)
	}
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
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
