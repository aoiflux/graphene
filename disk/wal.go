package disk

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
//   0x05 = Node delete (tombstone; payload = nodeID:8)
//   0x06 = Edge delete (tombstone; payload = edgeID:8)
//   0x07 = Node property purge (payload = nodeID:8)
//   0x08 = Edge property purge (payload = edgeID:8)
//   0xFF = Checkpoint (safe truncation marker after compaction)
//
// Edits reuse the 0x01/0x02 records: a node/edge record re-appended with an
// existing ID is applied as an update (last-write-wins) on replay.
//
// The purge records (0x07/0x08) drop every property-index entry for an ID
// without deleting the entity, which is what store.ReindexPurge needs: without
// them, replay would re-apply the superseded 0x03/0x04 entries and resurrect
// values the entity no longer has.
//
// Replay **rejects** unknown record types rather than skipping them. Skipping
// would let an older binary apply a rolled-back batch by ignoring the very
// begin/commit markers that were meant to suppress it, so a WAL written by a
// newer build is not readable by an older one — the log format is
// forward-compatible only in the direction that is safe.

const (
	walRecordNode          byte = 0x01
	walRecordEdge          byte = 0x02
	walRecordNodeProp      byte = 0x03
	walRecordEdgeProp      byte = 0x04
	walRecordNodeDelete    byte = 0x05
	walRecordEdgeDelete    byte = 0x06
	walRecordNodePropPurge byte = 0x07
	walRecordEdgePropPurge byte = 0x08

	// Transaction markers. A batch is applied by replay only when its commit
	// marker is present and valid.
	//
	// These exist because per-record CRCs catch a torn *record* but not a torn
	// *batch*: if a 1 000-record write is interrupted after 500, all 500 are
	// individually valid and replay would otherwise apply half a transaction.
	walRecordBatchBegin  byte = 0x09
	walRecordBatchCommit byte = 0x0A
	walRecordCheckpoint  byte = 0xFF

	// walReplayBufferSize is the read buffer replay pulls the log through. One
	// MiB turns a multi-megabyte log's per-record reads into a couple of dozen
	// syscalls; larger buys nothing measurable and is memory held during open.
	// The buffer is capped to the log's own size, so a compacted store with a
	// near-empty WAL does not pay for a megabyte it cannot use.
	walReplayBufferSize = 1 << 20

	// walMinReplayBuffer keeps a tiny or empty log from requesting a degenerate
	// buffer; it must exceed the largest single fixed-size read below.
	walMinReplayBuffer = 4096

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

// AppendNodeDelete writes a node tombstone (payload = nodeID:8) to the WAL.
func (w *WAL) AppendNodeDelete(payload []byte) error {
	return w.append(walRecordNodeDelete, payload)
}

// AppendEdgeDelete writes an edge tombstone (payload = edgeID:8) to the WAL.
func (w *WAL) AppendEdgeDelete(payload []byte) error {
	return w.append(walRecordEdgeDelete, payload)
}

// AppendNodePropPurge records that every property-index entry for a node is to
// be dropped (payload = nodeID:8).
func (w *WAL) AppendNodePropPurge(payload []byte) error {
	return w.append(walRecordNodePropPurge, payload)
}

// AppendEdgePropPurge records that every property-index entry for an edge is to
// be dropped (payload = edgeID:8).
func (w *WAL) AppendEdgePropPurge(payload []byte) error {
	return w.append(walRecordEdgePropPurge, payload)
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
	NodeFunc       func([]byte) error // called for each 0x01 node record
	EdgeFunc       func([]byte) error // called for each 0x02 edge record
	NodePropFunc   func([]byte) error // called for each 0x03 node property entry
	EdgePropFunc   func([]byte) error // called for each 0x04 edge property entry
	NodeDeleteFunc func([]byte) error // called for each 0x05 node tombstone
	EdgeDeleteFunc func([]byte) error // called for each 0x06 edge tombstone

	NodePropPurgeFunc func([]byte) error // called for each 0x07 node property purge
	EdgePropPurgeFunc func([]byte) error // called for each 0x08 edge property purge
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

	// Replay used to read straight from the file handle: three ReadFull calls per
	// record (header, payload, footer), so a 60 000-record log cost ~180 000
	// syscalls. Profiling a cold open put 69% of the time in syscall.readFile —
	// replay is I/O-bound, not index-bound, which is the opposite of what the
	// plan assumed.
	//
	// Buffering is safe here specifically because the file is opened O_APPEND:
	// writes go to the end regardless of where reading left the offset, so
	// over-reading into the buffer cannot corrupt a subsequent append.
	// Size the buffer to the log, capped. A compacted store reopens with an empty
	// or tiny WAL, and a fixed 1 MiB buffer there is a megabyte allocated to read
	// a few hundred bytes — measurable as +1 MiB on every reopen benchmark.
	bufSize := walReplayBufferSize
	// logSize also bounds each record's declared payload length below. A record
	// cannot be longer than the file that contains it, and without that check a
	// corrupt or hostile 5-byte header claiming 0xFFFFFFFF makes replay allocate
	// 4 GiB before reading a single byte of it.
	var logSize int64
	if fi, err := w.file.Stat(); err == nil {
		logSize = fi.Size()
		if logSize < int64(bufSize) {
			bufSize = int(logSize)
		}
	}
	if bufSize < walMinReplayBuffer {
		bufSize = walMinReplayBuffer
	}
	return replayRecords(bufio.NewReaderSize(w.file, bufSize), logSize, cb)
}

// replayRecords is Replay's parser, separated from the file handling around it.
//
// The split exists so the parser can be driven from memory. Replay reads an
// *os.File, so fuzzing it end-to-end costs a file write and an open per
// candidate input — which held FuzzWALReplay to a few thousand executions a
// minute against three million for the in-memory CSR parser, on the log format
// that carries the higher risk of the two. It is also what a model-checking
// harness needs, since a generated trace is a byte slice, not a file.
//
// logSize bounds each record's declared payload length; pass 0 when the total is
// not known and the check is skipped.
func replayRecords(r io.Reader, logSize int64, cb ReplayCallbacks) error {
	header := make([]byte, walHeaderSize)
	footer := make([]byte, walFooterSize)

	// Batch state. pending is non-nil only between a begin marker and its commit;
	// reaching EOF with it still set discards the batch, which is the rollback.
	var pending []pendingRecord
	var body []byte

	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}

		recType := header[0]
		length := binary.LittleEndian.Uint32(header[1:5])

		// A record longer than the whole log is not a record. Treat it as a torn
		// tail rather than an error: the same byte pattern arises from a crash
		// mid-header, and replay's contract is to stop at the first thing it
		// cannot trust. Checking before the allocation is the point — otherwise
		// the length field alone decides how much memory replay demands.
		if logSize > 0 && int64(length) > logSize {
			break
		}

		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(r, payload); err != nil {
				break // partial record at tail — stop
			}
		}

		if _, err := io.ReadFull(r, footer); err != nil {
			break // partial record at tail
		}

		// Verify CRC32.
		storedCRC := binary.LittleEndian.Uint32(footer)
		if computeCRC32(payload) != storedCRC {
			break // corrupted tail record
		}

		switch recType {
		case walRecordBatchBegin:
			if pending != nil {
				return fmt.Errorf("wal replay: nested batch begin")
			}
			if len(payload) != walBatchBeginPayload {
				return fmt.Errorf("wal replay: malformed batch begin")
			}
			// The declared count sizes an allocation, so it is bounded by what the
			// log could actually contain: every batched record costs at least a
			// header and a footer. Without this a 13-byte log whose begin marker
			// claims 2^32-1 records asks for ~137 GB before reading the first one,
			// and the marker's own CRC is no defence — it is computed over the very
			// bytes that make the claim.
			//
			// This is an error rather than a torn tail: the record is intact and
			// its CRC verified, so what it describes is impossible rather than
			// truncated, which is the same class as the length check above it.
			batchCount := binary.LittleEndian.Uint32(payload)
			if logSize > 0 && int64(batchCount) > logSize/walRecordOverhead {
				return fmt.Errorf("wal replay: batch begin declares %d records, more than %d bytes can hold",
					batchCount, logSize)
			}
			pending = make([]pendingRecord, 0, batchCount)
			body = body[:0]
			continue

		case walRecordBatchCommit:
			if pending == nil {
				return fmt.Errorf("wal replay: batch commit without begin")
			}
			if len(payload) != walBatchCommitPayload {
				return fmt.Errorf("wal replay: malformed batch commit")
			}
			wantCount := binary.LittleEndian.Uint32(payload[0:4])
			wantCRC := binary.LittleEndian.Uint32(payload[4:8])
			if uint32(len(pending)) != wantCount || computeCRC32(body) != wantCRC {
				// The commit is present but does not describe what was read back.
				// Treat it as an incomplete transaction and discard, rather than
				// applying a batch that does not match its own commit record.
				pending = nil
				body = body[:0]
				continue
			}
			for _, rec := range pending {
				if err := applyWALRecord(cb, rec.recType, rec.payload); err != nil {
					return err
				}
			}
			pending = nil
			body = body[:0]
			continue

		case walRecordCheckpoint:
			// A checkpoint inside an open batch means the batch never committed.
			return nil // replay complete up to last checkpoint
		}

		if !knownWALRecord(recType) {
			// Deliberately an error, not a skip. Silently ignoring an unknown type
			// would let an older binary apply a *rolled back* batch by ignoring the
			// markers that were supposed to suppress it.
			return fmt.Errorf("wal replay: unknown record type 0x%02X", recType)
		}

		if pending != nil {
			// Inside a batch: buffer rather than apply, and accumulate the body
			// exactly as it was framed so the commit CRC can be checked against it.
			pending = append(pending, pendingRecord{recType: recType, payload: payload})
			body = appendRecord(body, recType, payload)
			continue
		}

		if err := applyWALRecord(cb, recType, payload); err != nil {
			return err
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
// computeCRC32 is the checksum stored in every WAL record footer.
//
// This was a bit-by-bit loop — eight iterations per byte, no table — which cost
// 46% of a cold open once replay stopped being syscall-bound. Its polynomial
// (0xEDB88320, the reversed IEEE polynomial) is precisely what
// crc32.ChecksumIEEE computes, and the standard library's amd64 implementation
// is hardware-accelerated.
//
// **The value is identical**, which is what makes this safe: every WAL already
// on disk was written with the old loop, and a different checksum would fail
// every record on replay. TestComputeCRC32Vectors pins the values so this stays
// true — it is a on-disk format guarantee, not an implementation detail.
func computeCRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// AppendBatch writes a pre-framed batch in a single write, optionally syncing.
//
// The buffer must come from walBatch.finish(), so it is already bracketed by
// begin/commit markers. Replay applies it only on reaching a valid commit, which
// is what makes the batch atomic: a torn write leaves the commit absent and the
// whole batch is discarded.
//
// Queued records are drained first. A batch must not jump ahead of writes
// already sitting in the ring, because replay order is apply order.
func (w *WAL) AppendBatch(framed []byte, sync bool) error {
	if len(framed) == 0 {
		return nil
	}
	if w.closed.Load() != 0 {
		return fmt.Errorf("wal append batch: closed")
	}
	if !w.enterAppend() {
		return fmt.Errorf("wal append batch: closed")
	}
	defer w.inFlight.Add(-1)

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	// Preserve ordering against concurrent single-record writers.
	if err := w.drainQueuedLocked(); err != nil {
		return err
	}
	if _, err := w.file.Write(framed); err != nil {
		return fmt.Errorf("wal append batch: %w", err)
	}
	if sync {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal append batch: sync: %w", err)
		}
	}
	return nil
}

// pendingRecord is one record buffered inside an uncommitted batch.
type pendingRecord struct {
	recType byte
	payload []byte
}

// knownWALRecord reports whether recType is one this build understands.
func knownWALRecord(recType byte) bool {
	switch recType {
	case walRecordNode, walRecordEdge,
		walRecordNodeProp, walRecordEdgeProp,
		walRecordNodeDelete, walRecordEdgeDelete,
		walRecordNodePropPurge, walRecordEdgePropPurge:
		return true
	}
	return false
}

// applyWALRecord dispatches one record to its callback.
func applyWALRecord(cb ReplayCallbacks, recType byte, payload []byte) error {
	switch recType {
	case walRecordNode:
		if cb.NodeFunc != nil {
			return cb.NodeFunc(payload)
		}
	case walRecordEdge:
		if cb.EdgeFunc != nil {
			return cb.EdgeFunc(payload)
		}
	case walRecordNodeProp:
		if cb.NodePropFunc != nil {
			return cb.NodePropFunc(payload)
		}
	case walRecordEdgeProp:
		if cb.EdgePropFunc != nil {
			return cb.EdgePropFunc(payload)
		}
	case walRecordNodeDelete:
		if cb.NodeDeleteFunc != nil {
			return cb.NodeDeleteFunc(payload)
		}
	case walRecordEdgeDelete:
		if cb.EdgeDeleteFunc != nil {
			return cb.EdgeDeleteFunc(payload)
		}
	case walRecordNodePropPurge:
		if cb.NodePropPurgeFunc != nil {
			return cb.NodePropPurgeFunc(payload)
		}
	case walRecordEdgePropPurge:
		if cb.EdgePropPurgeFunc != nil {
			return cb.EdgePropPurgeFunc(payload)
		}
	}
	return nil
}

// Sync flushes queued records and forces them to the platter.
//
// This is the cheap durability point. Before it existed the only ways to make a
// write durable were Checkpoint (via Compact, which rebuilds the whole CSR) or
// Close — so a caller wanting "everything so far is safe" had to pay a full
// compaction for it.
func (w *WAL) Sync() error {
	if w.closed.Load() != 0 {
		return fmt.Errorf("wal sync: closed")
	}
	if !w.enterAppend() {
		return fmt.Errorf("wal sync: closed")
	}
	defer w.inFlight.Add(-1)

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if err := w.drainQueuedLocked(); err != nil {
		return err
	}
	return w.file.Sync()
}
