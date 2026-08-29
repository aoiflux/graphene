package disk

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aoiflux/graphene/store"
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

	// walRecordKeyTransition records one signing key replacing another, signed
	// by the outgoing key. See keyrotation.go for why the outgoing key signs.
	//
	// Standalone rather than part of a batch: a rotation is not a graph mutation
	// and has no records to be atomic with. It also has to survive being the only
	// thing in the log, which a batch marker would not allow.
	walRecordKeyTransition byte = 0x0D

	walRecordCheckpoint byte = 0xFF

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

	// framing selects what a record's CRC covers, and dataStart is where records
	// begin — after a container header, or at 0 for a headerless file. Both are
	// fixed when the file is opened and never change for its lifetime, so a
	// single log never contains two framings.
	framing   uint16
	dataStart int64

	// logGen is the container header's SegmentSeq: how many times the file at
	// this path has been replaced. Every operation that ends the current file —
	// Truncate, truncateFrom, Rotate — writes the next value into the header of
	// the file it starts, so a reader that saw generation g and now sees
	// anything else knows its byte offsets name nothing.
	//
	// It is the marker TECHNICAL_DETAILS §9.1b asks for, and it costs a field
	// rather than a format change because the container already carried the
	// number and nothing read it. Zero for a headerless v1 log, which never
	// changes generation: the branches that would bump it either give the file a
	// v2 header or leave it alone.
	logGen uint64

	ringMask uint64
	ring     []walSlot
	head     atomic.Uint64 // next sequence to reserve
	tail     atomic.Uint64 // next sequence to consume/write

	barrier  atomic.Uint32 // 1 while maintenance op is active
	inFlight atomic.Int64  // append calls currently in progress
	closed   atomic.Uint32 // 1 once Close() starts

	// gate shares one fsync across every commit it covers. See syncgate.go.
	gate *syncGate

	// writeHook and syncHook stand in for the file's own calls when set.
	//
	// Nil in production, and the only way to reach them is from inside this
	// package. They exist because the engine's crash behaviour was previously
	// only testable by killing a process: there was no way to make a write short,
	// a disk full, or an fsync lie, so the ordering arguments in compact.go and
	// syncgate.go were argued and never exercised. Every write path below routes
	// through writeFile/syncFile so that a test can.
	//
	// Set once, before the WAL is used.
	writeHook func([]byte) (int, error)
	syncHook  func() error

	// syncObserver, when set, is told about each fsync the *commit path*
	// performs: how many queued entries it covered, how long the syscall took,
	// and whether it failed.
	//
	// Only flushAndSync reports. The other sync sites — Sync, Checkpoint,
	// Truncate, Close — are maintenance rather than commits, and counting them
	// here would blur the one number this exists to expose: fsyncs per commit,
	// which is whether group commit is working at all.
	//
	// Set once at Open, before the log is used, and nil unless a metrics sink is
	// attached — so an uninstrumented store does not even read the clock.
	syncObserver func(covered uint64, d time.Duration, err error)

	// lastSyncTail is the ring sequence the previous commit-path fsync reached.
	// Guarded by writeMu, which flushAndSync holds across both the drain and the
	// sync, so the subtraction below cannot race another leader.
	lastSyncTail uint64

	// readOnly refuses every path that writes. Fixed at open and never changed.
	//
	// This is a backstop rather than the guard callers meet: Store.mustWrite
	// refuses first and says something useful about the store. What this catches
	// is an engine path that reaches the log without going through a public
	// mutator — the failure mode being guarded is a read-only store silently
	// appending, which is exactly what the process lock exists to make impossible.
	//
	// When readOnly is set, file may be nil: a store whose log does not exist yet
	// has nothing to open and nothing to create.
	readOnly bool
}

// writeFile writes to the log, through the injected hook when one is set.
func (w *WAL) writeFile(b []byte) (int, error) {
	if w.writeHook != nil {
		return w.writeHook(b)
	}
	return w.file.Write(b)
}

// syncFile flushes the log, through the injected hook when one is set.
func (w *WAL) syncFile() error {
	if w.syncHook != nil {
		return w.syncHook()
	}
	return w.file.Sync()
}

// errWALReadOnly is the backstop refusal. Callers should be meeting
// disk.ErrReadOnly from the store instead; see the readOnly field.
var errWALReadOnly = errors.New("wal: log is open read-only")

type walSlot struct {
	seq     atomic.Uint64
	ready   atomic.Uint32
	recType byte
	// raw marks a slot whose payload is already framed — a batch straight from
	// walBatch.finish(), complete with its begin and commit markers — and is
	// written to the file verbatim rather than wrapped in a record header.
	//
	// Batches go through the same ring as single records because the ring's
	// sequence *is* the log order, and mixing a queue with a direct write would
	// give two ways for a record to reach the file and no way to say which one
	// got there first.
	raw     bool
	payload []byte
}

const defaultWALRingCapacity = 1024

// OpenWAL opens (or creates) the WAL at path.
func OpenWAL(path string) (*WAL, error) {
	return openWALWithCapacity(path, defaultWALRingCapacity)
}

// openWALFor opens the log for a store, writable or not.
func openWALFor(path string, readOnly bool) (*WAL, error) {
	if readOnly {
		return openWALReadOnly(path)
	}
	return OpenWAL(path)
}

// openWALReadOnly opens the log for replay and nothing else.
//
// Two things the writable path does are writes, and a read-only store must do
// neither. It opens O_CREATE, so pointing a reader at a store with no log would
// create one; and it adopts v2 framing by writing a container header into an
// empty file, so opening an empty log read-only would modify it. Here a missing
// file is simply an empty log — the honest reading of "this store has never been
// written to" — and a headerless file keeps its framing untouched.
func openWALReadOnly(path string) (*WAL, error) {
	w := &WAL{readOnly: true, gate: newSyncGate()}

	f, err := openSharedRead(path)
	switch {
	case os.IsNotExist(err):
		// No log. Nothing to replay, and nothing to create. Every method that
		// would touch the handle either refuses first (the write paths) or checks
		// for nil (Size, Replay, Close).
		w.framing = walFramingV2
		w.dataStart = walFileHeaderSize
		return w, nil
	case err != nil:
		return nil, fmt.Errorf("wal open: %w", err)
	}

	header, dataStart, err := readWALFileHeader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal open: %w", err)
	}
	// No header adoption here, unlike the writable path: writing one would be a
	// write. A headerless log replays under v1 framing, which is exactly what a
	// writer opening the same file would do with it.
	w.file = f
	w.framing = header.Version
	w.dataStart = dataStart
	w.logGen = header.SegmentSeq
	return w, nil
}

func openWALWithCapacity(path string, capacity int) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}

	// Framing is a property of the file, decided once here. A log that already
	// exists without a container header keeps the original framing for records
	// appended to it — rewriting its earlier records is not on the table — and a
	// new or freshly truncated file gets a header and the stronger framing. See
	// walcontainer.go.
	header, dataStart, err := readWALFileHeader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal open: %w", err)
	}
	if dataStart == 0 {
		// Headerless. If the file is empty this is a new log, so write a header
		// and adopt v2; otherwise leave the existing file as it is.
		fi, statErr := f.Stat()
		if statErr == nil && fi.Size() == 0 {
			if _, werr := f.Write(appendWALFileHeader(walFileHeader{Version: walFramingV2})); werr != nil {
				f.Close()
				return nil, fmt.Errorf("wal open: write header: %w", werr)
			}
			header = walFileHeader{Version: walFramingV2}
			dataStart = walFileHeaderSize
		}
	}

	capPow2 := nextPowerOfTwo(capacity)
	if capPow2 < 2 {
		capPow2 = 2
	}

	w := &WAL{
		file:      f,
		framing:   header.Version,
		dataStart: dataStart,
		logGen:    header.SegmentSeq,
		ring:      make([]walSlot, capPow2),
		ringMask:  uint64(capPow2 - 1),
		gate:      newSyncGate(),
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

// AppendKeyTransition records a signing key rotation.
func (w *WAL) AppendKeyTransition(payload []byte) error {
	return w.append(walRecordKeyTransition, payload)
}

// AppendEdgePropPurge records that every property-index entry for an edge is to
// be dropped (payload = edgeID:8).
func (w *WAL) AppendEdgePropPurge(payload []byte) error {
	return w.append(walRecordEdgePropPurge, payload)
}

// verifyCommitSignature applies the callbacks' signature policy to one commit.
//
// Four cases, and the third is the one that matters:
//
//   - no verifier: nothing is checked. A signed log replays with its signatures
//     unexamined, which is what a caller who has not opted in should get.
//   - signed and verified: the normal path.
//   - required but absent: rejected. Otherwise stripping a signature downgrades
//     a commit to an ordinary unsigned one and verification is skipped entirely.
//   - present but invalid: rejected, loudly and as an error rather than as a
//     torn tail, because a bad signature is not what a crash produces.
func verifyCommitSignature(cb ReplayCallbacks, meta batchMeta, body []byte) error {
	signed := len(meta.Signature) > 0

	if !signed {
		if cb.RequireSignedCommits {
			return fmt.Errorf("wal replay: commit %d carries no signature and signed commits are required",
				meta.CommitSeq)
		}
		return nil
	}
	if cb.Verifier == nil {
		return nil
	}

	data := signedCommitData(meta.CommitSeq, meta.UnixNano, meta.ActorID, meta.KeyID, sha256.Sum256(body))
	if err := cb.Verifier.Verify(meta.KeyID, data, meta.Signature); err != nil {
		return fmt.Errorf("wal replay: commit %d failed signature verification under key %d: %w",
			meta.CommitSeq, meta.KeyID, err)
	}
	return nil
}

// Size returns the log's current size on disk in bytes, or 0 if it cannot be
// determined.
//
// Reported without taking the maintenance barrier: this is an operational
// gauge, and blocking writers to read it would make observing the store a
// reason to slow it down. The figure may therefore lag an in-flight append by
// one record, which does not matter for anything it is used for.
func (w *WAL) Size() int64 {
	if w.file == nil {
		return 0
	}
	fi, err := w.file.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// stableSize reports the log's length with nothing in flight, so the figure
// names a record boundary rather than wherever a concurrent append had reached.
//
// Size is the cheap gauge and is deliberately unsynchronised; this is the one
// callers need when the number is going to be used as a *cut*. A backup records
// it in a manifest and a restore truncates to it, and a figure landing inside a
// record would make the copy's own length a lie about what it holds — replay
// would drop the torn record silently and the backup would claim a commit it
// did not carry.
//
// Read-only logs have nothing queued, so the barrier is all the quiescing they
// need.
func (w *WAL) stableSize() (int64, error) {
	if w.file == nil {
		return 0, nil
	}
	if err := w.beginMaintenance(); err != nil {
		return 0, err
	}
	defer w.endMaintenance()

	if !w.readOnly {
		if err := w.drainQueuedLocked(); err != nil {
			return 0, err
		}
	}
	fi, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal size: %w", err)
	}
	return fi.Size(), nil
}

// Checkpoint writes a checkpoint marker and syncs. After compaction, a
// checkpoint signals that all records before it are durable in the CSR and
// the WAL can be safely truncated.
func (w *WAL) Checkpoint() error {
	_, err := w.checkpointAt()
	return err
}

// drainedEnd flushes everything queued and reports the offset one byte past the
// last record. It writes no marker and issues no fsync.
//
// This is checkpointAt's first half. Compaction needs the number before it can
// decide whether the log is going to be retired at all, and the marker means
// "replay stops here": writing one into a log that then goes on being appended
// to strands every record written after it. See retireLog, whose third branch
// keeps the log and must therefore leave no marker in it.
//
// **Durability is the caller's, not this call's**, so that a compaction still
// costs exactly one fsync. Two of the three retire branches go on to call
// checkpointAt, which syncs; the third syncs explicitly. Syncing here as well
// would add a second fsync per compaction to pay for a number.
//
// The offset is only meaningful between the ring drain and the next append, and
// this and anything that follows it run with s.mu held, so nothing can be
// appended in between and every branch agrees on the number.
func (w *WAL) drainedEnd() (int64, error) {
	if w.readOnly {
		return 0, errWALReadOnly
	}
	if err := w.beginMaintenance(); err != nil {
		return 0, err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return 0, err
	}
	fi, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal drained end: stat: %w", err)
	}
	return fi.Size(), nil
}

// checkpointAt is Checkpoint, reporting the offset the marker was written at —
// which is to say the end of every record the log held when it went down.
//
// Compaction needs that number and cannot take it around the call: the offset
// is only meaningful between the ring drain and the marker append, and both of
// those happen inside the maintenance barrier held here. Measured before the
// call it misses whatever the drain then writes; measured after it includes the
// marker, and a marker carried forward into a rebuilt log would stop replay at
// the first record of the tail it was carried with.
func (w *WAL) checkpointAt() (int64, error) {
	if w.readOnly {
		return 0, errWALReadOnly
	}
	if err := w.beginMaintenance(); err != nil {
		return 0, err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return 0, err
	}
	fi, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal checkpoint: stat: %w", err)
	}
	off := fi.Size()

	if err := w.writeRecord(walRecordCheckpoint, nil); err != nil {
		return 0, err
	}
	if err := w.syncFile(); err != nil {
		return 0, err
	}
	w.gate.noteSynced(w.tail.Load())
	return off, nil
}

// Truncate removes all records from the WAL (called after successful compaction).
func (w *WAL) Truncate() error {
	if w.readOnly {
		return errWALReadOnly
	}
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
	// Every outstanding ticket names bytes this is about to discard. They are
	// discarded because compaction has already folded them into an image it
	// fsynced, so they are durable in the sense a committer waiting on one
	// cares about — and nothing will ever sync the log position they name.
	w.gate.noteSynced(w.tail.Load())
	if err := os.Truncate(name, 0); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("wal truncate: reopen: %w", err)
	}
	w.file = f

	// The truncation emptied the file, so its container header went with it and
	// has to be rewritten — otherwise dataStart points past the end and replay
	// skips the records written after this.
	//
	// This is also the migration path: a store whose log predates the container
	// adopts the stronger framing here, at its first compaction, without an
	// explicit step and without any file ever holding two framings.
	if _, err := f.Write(appendWALFileHeader(walFileHeader{
		Version: walFramingV2, SegmentSeq: w.logGen + 1,
	})); err != nil {
		return fmt.Errorf("wal truncate: write header: %w", err)
	}
	w.framing = walFramingV2
	w.dataStart = walFileHeaderSize
	w.logGen++
	return nil
}

// truncateFrom rebuilds the log holding only the records in [keepFrom, keepTo).
//
// This is Truncate for a compaction that ran with the store lock released: the
// image folded in everything the log held at keepFrom, and everything after it
// is a commit that landed while the image was being built and exists nowhere
// else on disk. Truncating to zero would lose those commits outright, so the
// bytes are carried into the rebuilt log rather than discarded.
//
// Carrying raw bytes is sound because a record's CRC covers its type, length
// and payload and nothing about where it sits — see recordCRC — so a record is
// the same record at a different offset. It is *only* sound within one framing,
// which is why the caller checks Framing first: a v1 log's records would fail
// their CRC under the v2 header this writes.
//
// Unlike Truncate, this cannot rewrite the file in place. Truncate discards
// data the image already holds, so a crash midway through costs nothing; here
// the tail is the only durable copy of those commits, and a crash between the
// truncation and the rewrite would take them. The new log is therefore built
// beside the old one and renamed over it, which is the same argument compaction
// makes for the image itself.
func (w *WAL) truncateFrom(keepFrom, keepTo int64) error {
	if w.readOnly {
		return errWALReadOnly
	}
	if err := w.beginMaintenance(); err != nil {
		return err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return err
	}
	if keepFrom < w.dataStart || keepTo < keepFrom {
		return fmt.Errorf("wal truncate: nonsense range [%d,%d) in a log starting at %d",
			keepFrom, keepTo, w.dataStart)
	}

	tail := make([]byte, keepTo-keepFrom)
	if len(tail) > 0 {
		if _, err := w.file.ReadAt(tail, keepFrom); err != nil {
			return fmt.Errorf("wal truncate: read tail: %w", err)
		}
	}

	name := w.file.Name()
	tmp := name + walTmpSuffix
	rebuilt := append(appendWALFileHeader(walFileHeader{
		Version: walFramingV2, SegmentSeq: w.logGen + 1,
	}), tail...)
	if err := writeFileSync(tmp, rebuilt, 0600); err != nil {
		return fmt.Errorf("wal truncate: write rebuilt log: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal truncate: close: %w", err)
	}
	// replaceFile, not os.Rename: a live reader holding the log open makes a
	// replacing rename fail on Windows, and a compaction that fails because
	// someone is *reading* the store would be a reader breaking a writer. See
	// fileshare.go for the contract and for why the tmp above is written and
	// fsynced before this point.
	if err := replaceFile(tmp, name); err != nil {
		return fmt.Errorf("wal truncate: replace: %w", err)
	}
	if err := syncDir(filepath.Dir(name)); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	}

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("wal truncate: reopen: %w", err)
	}
	w.file = f
	w.framing = walFramingV2
	w.dataStart = walFileHeaderSize
	w.logGen++

	// Every outstanding ticket names a position in the file that has just been
	// replaced. The bytes those tickets covered are durable — either folded into
	// the image compaction fsynced, or carried into the log this fsynced — so
	// releasing the waiters is telling them the truth, and nothing will ever
	// sync the offsets they actually name.
	w.gate.noteSynced(w.tail.Load())
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

	// KeyTransitionFunc is called for each 0x0D key rotation record.
	//
	// Dispatched like any other record, which means a transition inside a batch
	// is buffered with it and discarded if the batch never commits. Rotations
	// are written standalone, so that does not arise in practice — but a log
	// assembled by hand could contain one, and treating it uniformly is safer
	// than special-casing it.
	KeyTransitionFunc func([]byte) error

	// Verifier checks commit signatures. Nil means signatures are not checked —
	// a signed log still replays, but nothing confirms the signatures, which is
	// the correct behaviour for a caller who has not opted into verification and
	// may not hold the public key.
	Verifier store.Verifier

	// RequireSignedCommits rejects any committed batch that carries no
	// signature.
	//
	// This is the flag that closes the downgrade: without it, stripping the
	// signature from a commit turns it into a perfectly ordinary v2 record and
	// verification is simply skipped, so signing would buy nothing against an
	// attacker with file access. It is separate from Verifier because an
	// existing store's older commits are genuinely unsigned, and a caller
	// enabling verification mid-life needs to accept those until a compaction
	// retires them.
	RequireSignedCommits bool

	// CommitFunc is called once per successfully committed batch that carries
	// provenance, after the commit has been validated against the body it
	// describes and before the batch's records are applied.
	//
	// It is not called for a batch whose commit failed validation — that batch
	// was never committed — nor for a v1 commit record, which has no metadata to
	// report. A nil CommitFunc is skipped, so a caller that does not care about
	// provenance is unaffected.
	CommitFunc func(batchMeta)
}

// Replay reads all records from the WAL from the beginning and dispatches each
// to the matching callback in cb. It stops at EOF or a checkpoint record.
// Partial/corrupted records at the tail are silently ignored (crash-safe).
func (w *WAL) Replay(cb ReplayCallbacks) error {
	_, err := w.replayFrom(0, cb)
	return err
}

// replayFrom is Replay resuming at off bytes into the records region, reporting
// how far into that region the replay reached.
//
// The offset is relative to dataStart rather than to the file, so it survives a
// log that gains a container header it did not have — and, more to the point, it
// is the same number whichever of the two framings the file uses, which is what
// lets a live reader compare it against a generation rather than against a file
// size.
//
// Both numbers are only meaningful against a log that has not been replaced
// since; see WAL.logGen and Store.Refresh, which is the only caller that passes
// a non-zero offset.
func (w *WAL) replayFrom(off int64, cb ReplayCallbacks) (int64, error) {
	// A read-only store whose log does not exist: an empty log, replayed as no
	// records rather than as an error.
	if w.file == nil {
		return off, nil
	}

	if err := w.beginMaintenance(); err != nil {
		return off, err
	}
	defer w.endMaintenance()

	// Nothing is ever queued on a read-only log, and draining writes.
	if !w.readOnly {
		if err := w.drainQueuedLocked(); err != nil {
			return off, err
		}
	}

	// Records begin past the container header, if there is one.
	if _, err := w.file.Seek(w.dataStart+off, io.SeekStart); err != nil {
		return off, err
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
	//
	// It is the size of what remains to be read, not of the file: a resumed
	// replay that passed the whole file would let a record claim a length
	// covering bytes it cannot reach, which is the allocation this bound exists
	// to refuse.
	var logSize int64
	if fi, err := w.file.Stat(); err == nil {
		logSize = fi.Size() - (w.dataStart + off)
		if logSize < 0 {
			logSize = 0
		}
		if logSize < int64(bufSize) {
			bufSize = int(logSize)
		}
	}
	if bufSize < walMinReplayBuffer {
		bufSize = walMinReplayBuffer
	}
	read, err := replayRecordsFrom(bufio.NewReaderSize(w.file, bufSize), logSize, w.framing, cb)
	return off + read, err
}

// Framing reports which record framing this log uses. Callers building a batch
// for it must match, or the records they frame will not verify on replay.
func (w *WAL) Framing() uint16 { return w.framing }

// generation reports which file this log is, in the sequence of files that have
// held the active log at this path. See the logGen field.
func (w *WAL) generation() uint64 { return w.logGen }

// peekWALHeader reads a log's container header without opening it as a log.
//
// This is what a live reader polls: the generation and the framing are the two
// things it must agree with the writer about before a byte offset into the file
// means anything, and both are in the first 50 bytes. A missing file is not an
// error — a store compacted to completion and then reopened has no log yet, and
// that is an empty log rather than a broken one.
func peekWALHeader(path string) (walFileHeader, int64, error) {
	f, err := openSharedRead(path)
	if os.IsNotExist(err) {
		return walFileHeader{Version: walFramingV2}, walFileHeaderSize, nil
	}
	if err != nil {
		return walFileHeader{}, 0, fmt.Errorf("wal peek: %w", err)
	}
	defer f.Close()
	return readWALFileHeader(f)
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
func replayRecords(r io.Reader, logSize int64, framing uint16, cb ReplayCallbacks) error {
	_, err := replayRecordsFrom(r, logSize, framing, cb)
	return err
}

// replayRecordsFrom is replayRecords reporting how many bytes it consumed up to
// the last point it could safely be resumed from.
//
// "Safely" is the whole content of the number. It is not how far the parser
// read: it is the offset of the last boundary at which no batch was open, so a
// caller that starts again there sees a begin marker before any of the records
// that marker governs. Resuming from the parser's actual position instead would
// take a batch that was still being written when the reader caught up and apply
// its second half alone — the torn read the begin/commit markers exist to stop.
//
// A torn tail therefore rewinds: bytes read past the last clean boundary are
// deliberately not counted, because the writer will complete those records and
// the reader must read them whole.
func replayRecordsFrom(r io.Reader, logSize int64, framing uint16, cb ReplayCallbacks) (int64, error) {
	header := make([]byte, walHeaderSize)
	footer := make([]byte, walFooterSize)

	// Batch state. pending is non-nil only between a begin marker and its commit;
	// reaching EOF with it still set discards the batch, which is the rollback.
	var pending []pendingRecord
	var body []byte

	// consumed counts whole, verified records. safe is consumed as of the last
	// moment no batch was open.
	var consumed, safe int64

	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return safe, err
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
		// Under v2 this covers the type and length too, so a flipped type byte no
		// longer verifies — which is what stops a corrupted batch marker from
		// silently changing a record's meaning.
		if recordCRC(framing, recType, payload) != storedCRC {
			break // corrupted tail record
		}

		// Past every check: this record is whole and verified, so its bytes are
		// consumed whatever the switch below decides to do with it.
		consumed += int64(walHeaderSize) + int64(length) + int64(walFooterSize)

		switch recType {
		case walRecordBatchBegin:
			if pending != nil {
				return safe, fmt.Errorf("wal replay: nested batch begin")
			}
			if len(payload) != walBatchBeginPayload {
				return safe, fmt.Errorf("wal replay: malformed batch begin")
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
				return safe, fmt.Errorf("wal replay: batch begin declares %d records, more than %d bytes can hold",
					batchCount, logSize)
			}
			pending = make([]pendingRecord, 0, batchCount)
			body = body[:0]
			continue

		case walRecordBatchCommit:
			if pending == nil {
				return safe, fmt.Errorf("wal replay: batch commit without begin")
			}
			// Three accepted lengths: older logs must still replay after an
			// upgrade, so the length is the discriminator — see walbatch.go.
			switch len(payload) {
			case walBatchCommitPayloadV1, walBatchCommitPayloadV2, walBatchCommitPayloadV3:
			default:
				return safe, fmt.Errorf("wal replay: malformed batch commit: payload %d bytes, expected %d, %d or %d",
					len(payload), walBatchCommitPayloadV1, walBatchCommitPayloadV2, walBatchCommitPayloadV3)
			}
			wantCount := binary.LittleEndian.Uint32(payload[0:4])
			wantCRC := binary.LittleEndian.Uint32(payload[4:8])
			if uint32(len(pending)) != wantCount || computeCRC32(body) != wantCRC {
				// The commit is present but does not describe what was read back.
				// Treat it as an incomplete transaction and discard, rather than
				// applying a batch that does not match its own commit record.
				pending = nil
				body = body[:0]
				safe = consumed
				continue
			}
			// Only now, past validation. A batch that failed the check above was
			// never committed, so reporting its metadata would advance the store's
			// commit sequence for a transaction that did not happen.
			var meta batchMeta
			if len(payload) >= walBatchCommitPayloadV2 {
				meta = batchMeta{
					CommitSeq: binary.LittleEndian.Uint64(payload[8:16]),
					UnixNano:  int64(binary.LittleEndian.Uint64(payload[16:24])),
					ActorID:   binary.LittleEndian.Uint64(payload[24:32]),
				}
			}
			if len(payload) == walBatchCommitPayloadV3 {
				meta.KeyID = binary.LittleEndian.Uint64(payload[32:40])
				sigLen := int(binary.LittleEndian.Uint16(payload[40:42]))
				if sigLen > walSignatureSize {
					return safe, fmt.Errorf("wal replay: commit %d declares a %d-byte signature, maximum %d",
						meta.CommitSeq, sigLen, walSignatureSize)
				}
				meta.Signature = payload[42 : 42+sigLen]
			}

			// Signature check, before a single record is applied. This is the
			// reason the signature sits in the commit payload rather than in a
			// record after it: a batch must not be applied and then found forged.
			if err := verifyCommitSignature(cb, meta, body); err != nil {
				return safe, err
			}

			if cb.CommitFunc != nil && len(payload) >= walBatchCommitPayloadV2 {
				cb.CommitFunc(meta)
			}
			for _, rec := range pending {
				if err := applyWALRecord(cb, rec.recType, rec.payload); err != nil {
					return safe, err
				}
			}
			pending = nil
			body = body[:0]
			safe = consumed
			continue

		case walRecordCheckpoint:
			// A checkpoint inside an open batch means the batch never committed.
			//
			// safe, not consumed: the marker itself is not applied, and a live
			// reader that resumes here re-reads it and stops again for nothing,
			// which is what should happen — a marker is only ever written
			// immediately before the log is retired, and the retire is what tells
			// the reader to start over.
			return safe, nil // replay complete up to last checkpoint
		}

		if !knownWALRecord(recType) {
			// Deliberately an error, not a skip. Silently ignoring an unknown type
			// would let an older binary apply a *rolled back* batch by ignoring the
			// markers that were supposed to suppress it.
			return safe, fmt.Errorf("wal replay: unknown record type 0x%02X", recType)
		}

		if pending != nil {
			// Inside a batch: buffer rather than apply, and accumulate the body
			// exactly as it was framed so the commit CRC can be checked against it.
			pending = append(pending, pendingRecord{recType: recType, payload: payload})
			// Reframed under the file's own framing, because the commit's body
			// checksum was taken over the bytes the writer produced. Rebuilding
			// them under a different framing yields different footers and a
			// mismatch, which replay would read as a torn batch and discard.
			body = appendRecordFramed(body, framing, recType, payload)
			continue
		}

		if err := applyWALRecord(cb, recType, payload); err != nil {
			return safe, err
		}
		safe = consumed
	}
	return safe, nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	// A read-only log has nothing queued and nothing to sync, and may have no
	// file at all. Handled before the barrier because draining and syncing are
	// both writes.
	if w.readOnly {
		w.closed.Store(1)
		if w.file == nil {
			return nil
		}
		err := w.file.Close()
		w.file = nil
		if err != nil {
			return fmt.Errorf("wal close: %w", err)
		}
		return nil
	}

	if err := w.beginMaintenance(); err != nil {
		return err
	}
	defer w.endMaintenance()

	if err := w.drainQueuedLocked(); err != nil {
		return err
	}

	syncErr := w.syncFile()
	if syncErr == nil {
		w.gate.noteSynced(w.tail.Load())
	}
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
	if w.readOnly {
		return errWALReadOnly
	}
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

	if _, err := w.enqueue(recType, false, copied); err != nil {
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
	crc := recordCRC(w.framing, recType, payload)

	buf := make([]byte, walHeaderSize+int(length)+walFooterSize)
	buf[0] = recType
	binary.LittleEndian.PutUint32(buf[1:5], length)
	if length > 0 {
		copy(buf[walHeaderSize:], payload)
	}
	binary.LittleEndian.PutUint32(buf[walHeaderSize+int(length):], crc)

	if _, err := w.writeFile(buf); err != nil {
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

// enqueue reserves a ring slot and returns the ticket naming it.
//
// The ticket is the slot's sequence plus one, so that "everything up to ticket
// T" is exactly "tail has reached T" and zero can mean "nothing to wait for".
func (w *WAL) enqueue(recType byte, raw bool, payload []byte) (uint64, error) {
	fill := func(seq uint64) {
		slot := &w.ring[seq&w.ringMask]
		slot.recType = recType
		slot.raw = raw
		slot.payload = payload
		slot.seq.Store(seq)
		slot.ready.Store(1)
	}

	for {
		if seq, ok := w.tryReserve(); ok {
			fill(seq)
			return seq + 1, nil
		}

		// Overflow path: lock and drain until there is room.
		w.writeMu.Lock()
		if err := w.drainQueuedLocked(); err != nil {
			w.writeMu.Unlock()
			return 0, err
		}
		for {
			if seq, ok := w.tryReserve(); ok {
				fill(seq)
				if err := w.drainQueuedLocked(); err != nil {
					w.writeMu.Unlock()
					return 0, err
				}
				w.writeMu.Unlock()
				return seq + 1, nil
			}
			if err := w.drainQueuedLocked(); err != nil {
				w.writeMu.Unlock()
				return 0, err
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

		if slot.raw {
			if _, err := w.writeFile(slot.payload); err != nil {
				return fmt.Errorf("wal append batch: %w", err)
			}
		} else if err := w.writeRecord(slot.recType, slot.payload); err != nil {
			return err
		}

		slot.payload = nil
		slot.raw = false
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

// QueueBatch places a pre-framed batch in the log's queue and returns the
// ticket naming it. It is durable once AwaitSync(ticket) returns.
//
// Queued rather than written, deliberately. The obvious shape — write the bytes
// here, fsync in AwaitSync — was built first and measured, and on Windows it
// gave no group commit at all: WriteFile blocks while FlushFileBuffers is in
// flight on the same handle, so committers pile up behind the leader's own
// fsync instead of arriving in time to share it. Measured on a 640-commit run,
// the write cost per commit rose from 37 microseconds with one writer to 604
// with four — exactly the fsync duration — and 640 commits cost 639 fsyncs.
//
// Queueing moves both the write and the fsync inside the leader. No other
// goroutine touches the file while either is happening, so there is nothing for
// the platform to serialise against.
//
// framed must not be modified afterwards; it is written from this buffer.
func (w *WAL) QueueBatch(framed []byte) (uint64, error) {
	if len(framed) == 0 {
		return 0, nil
	}
	if w.readOnly {
		return 0, errWALReadOnly
	}
	if w.closed.Load() != 0 {
		return 0, fmt.Errorf("wal append batch: closed")
	}
	if !w.enterAppend() {
		return 0, fmt.Errorf("wal append batch: closed")
	}
	defer w.inFlight.Add(-1)

	return w.enqueue(0, true, framed)
}

// FlushQueued writes everything queued without syncing it, for a caller that is
// not going to wait for durability.
//
// Opportunistic: if another goroutine is already writing, the bytes stay queued
// and that goroutine will carry them. The ring is drained by whoever gets there
// first, and every path that must not leave anything behind — Sync, Checkpoint,
// Truncate, Close — drains under the maintenance barrier.
func (w *WAL) FlushQueued() error {
	if w.readOnly || w.closed.Load() != 0 {
		return nil
	}
	if !w.writeMu.TryLock() {
		return nil
	}
	err := w.drainQueuedLocked()
	w.writeMu.Unlock()
	return err
}

// AwaitSync returns once the write named by ticket is on the medium.
//
// A ticket of zero is already durable by definition: it names nothing.
func (w *WAL) AwaitSync(ticket uint64) error {
	if ticket == 0 {
		return nil
	}
	if w.readOnly {
		return errWALReadOnly
	}
	return w.gate.await(ticket, w.flushAndSync)
}

// flushAndSync is the leader's work: write everything queued, then fsync it,
// then report how far the log is now durable.
//
// writeMu is held across both. That is the point — see QueueBatch — and it
// costs nothing, because a committer that is not leading never wants the lock:
// it queued its bytes without one and is asleep on the gate.
func (w *WAL) flushAndSync() (uint64, error) {
	if w.closed.Load() != 0 {
		return 0, fmt.Errorf("wal sync: closed")
	}
	// Registers against the maintenance barrier, so a Checkpoint or Truncate
	// cannot start draining the same ring underneath this. enterAppend waits
	// for a barrier already up rather than failing, which is what a committer
	// with bytes in the queue needs it to do.
	if !w.enterAppend() {
		return 0, fmt.Errorf("wal sync: closed")
	}
	defer w.inFlight.Add(-1)

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if err := w.drainQueuedLocked(); err != nil {
		return 0, err
	}
	// Read after the drain and before the sync: everything the drain wrote is
	// covered by the sync about to happen, and anything queued after this point
	// is not.
	done := w.tail.Load()
	if w.syncObserver == nil {
		if err := w.syncFile(); err != nil {
			return 0, fmt.Errorf("wal append batch: sync: %w", err)
		}
		return done, nil
	}
	// Instrumented form, kept separate rather than folded together with a
	// conditional clock read: the uninstrumented path above is the one that runs
	// in production for a store with no sink, and it should read the file and
	// nothing else.
	started := time.Now()
	err := w.syncFile()
	covered := done - w.lastSyncTail
	w.lastSyncTail = done
	w.syncObserver(covered, time.Since(started), err)
	if err != nil {
		return 0, fmt.Errorf("wal append batch: sync: %w", err)
	}
	return done, nil
}

// AppendBatch queues a pre-framed batch and optionally waits for it to be
// durable.
//
// The buffer must come from walBatch.finish(), so it is already bracketed by
// begin/commit markers. Replay applies it only on reaching a valid commit,
// which is what makes the batch atomic: a torn write leaves the commit absent
// and the whole batch is discarded.
//
// This is the do-both form. The commit path uses QueueBatch and AwaitSync
// separately so it can release the store lock in between, which is what lets
// two committers share one fsync.
func (w *WAL) AppendBatch(framed []byte, sync bool) error {
	ticket, err := w.QueueBatch(framed)
	if err != nil {
		return err
	}
	if !sync {
		return w.FlushQueued()
	}
	return w.AwaitSync(ticket)
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
		walRecordNodePropPurge, walRecordEdgePropPurge,
		walRecordKeyTransition:
		return true
	}
	return false
}

// applyWALRecord dispatches one record to its callback.
func applyWALRecord(cb ReplayCallbacks, recType byte, payload []byte) error {
	switch recType {
	case walRecordKeyTransition:
		if cb.KeyTransitionFunc != nil {
			return cb.KeyTransitionFunc(payload)
		}
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
	if w.readOnly {
		return errWALReadOnly
	}
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
	if err := w.syncFile(); err != nil {
		return err
	}
	w.gate.noteSynced(w.tail.Load())
	return nil
}
