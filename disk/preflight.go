package disk

// What an Open will cost, answered before it is paid.
//
// # Why this is a separate file and not a field on CSRInfo
//
// inspect.go already reads both of a store's files without opening it, and its
// header comment makes the argument this file inherits: the moment you most
// want to look at a store is the moment something is wrong with it. But neither
// of its functions can answer "will this open fit in my memory", because both
// spend memory proportional to the store to find out. InspectCSR does
// os.ReadFile and a full deserialiseCSR, which materialises the whole graph.
// InspectWAL appends a WALRecordInfo per record and accumulates a batch body.
// Either one, asked about a store that will not fit, does not fit either.
//
// So this is the third inspection surface, and its distinguishing property is
// the one the other two lack: every function here runs in memory bounded by a
// constant, whatever the size of the store. The image is read as a fixed header
// plus two small addressed reads; the log is walked with a streaming checksum
// and never a retained payload. That is the whole reason to have it.
//
// # What it deliberately does not do
//
// It takes no lock, opens no store, and replays nothing. It also verifies no
// signatures and decodes no payloads, which is why every count it reports is an
// upper bound rather than an equality — see WALRecords. Over-reporting is the
// safe direction for a figure a caller is about to gate an Open on;
// under-reporting would be the failure this whole program exists to prevent.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Bytes per entity, for the resident model in imageHeapBytes.
//
// These are measured, not derived, and the measurements are in
// docs/benchmarks.md under "Program baselines". Each is named here with what it
// covers so a later change to the layout has somewhere obvious to disagree.
const (
	// estNodeSlotBytes is what one node slot costs whether or not a record
	// occupies it: 56 B of nodeRecord plus one uint64 in each of outOffset and
	// inOffset, both of which are indexed by slot. Measured at 72 B per burned
	// identifier, against 79 B of resident memory once the runtime's own
	// overhead is counted. Since the records were paged the count of slots is
	// no longer the identifier space but the pages it touches, which is what
	// imageHeapBytes models; the per-slot figure is unchanged.
	estNodeSlotBytes = 72

	// estEdgeSlotBytes is one rawEdge, which is 80 B by unsafe.Sizeof and was
	// measured at 80 B per burned edge identifier. Note that no fixture in this
	// repository exercises edges at scale: the number follows from the struct
	// layout and from the delete-cascade arithmetic, not from an edge-heavy
	// benchmark.
	estEdgeSlotBytes = 80

	// estLiveEdgeBytes is the adjacency cost of an edge that is actually
	// present: one EdgeID in outEdges and one in inEdges. Sized by the live
	// count, not by the identifier space, which is why it is a separate term.
	//
	// It assumes AdjacencyEager, and PreflightOpen has no way not to: it reads a
	// directory and takes no Options, so it cannot know that the store about to
	// be opened will defer the build. Under AdjacencyLazy the estimate is high by
	// this term and by the two per-slot uint64s inside estNodeSlotBytes. High is
	// the direction a budget refusal should be wrong in — it declines a store
	// that would have fit rather than admitting one that will not — so this is
	// stated rather than corrected. An Options-aware estimate is N2's.
	estLiveEdgeBytes = 16

	// estPropertyEntryBytes is one property-index entry. The tree reports this
	// figure three times from three dates — 107 B in docs/benchmarks.md, 103.9 B
	// in TECHNICAL_DETAILS §14.5, and 93.4 B once the arity split and key
	// interning are counted — and value interning then moves it by between −34%
	// and 0% depending on a value cardinality no header can see. The largest of
	// them is used deliberately: this term dominates an indexed store, and the
	// direction to be wrong in is up.
	estPropertyEntryBytes = 107
)

// preflightChunk is the streaming buffer walkRecords pushes record payloads
// through on their way to the checksum. It is this file's whole memory footprint
// above the read buffer, and it is a constant because that is the property the
// file exists to have.
const preflightChunk = 32 << 10

// preflightReadBuffer caps the read buffer, and is deliberately a sixteenth of
// the one replay uses.
//
// Replay's megabyte is sized against the work it does per record — decoding a
// payload, applying it, maintaining an index — so amortising the syscall over a
// large buffer is worth a megabyte of the open's memory. This walk does none of
// that: it checksums bytes and advances, so the syscall is already a small share
// of its cost and the buffer saturates far earlier. On a 2 GiB log the smaller
// buffer costs on the order of thirty thousand extra reads, tens of
// milliseconds against a multi-second pass, and buys back very nearly a megabyte
// of the constant this whole surface is promising. Under this program's priority
// order that is the right way round.
//
// It is still capped down to the log's own size, so a compacted store with a
// near-empty log does not allocate a buffer it cannot fill.
const preflightReadBuffer = 64 << 10

// csrMinHeaderSize is the smallest header any accepted version carries: magic,
// version, and the two record counts. Everything past it is version-gated.
const csrMinHeaderSize = 22

// OpenEstimate is what a store would cost to open, read from its files without
// opening it.
//
// Zero fields mean "absent or not stated", never "measured as zero" — with one
// exception that is called out on the field itself, ImageSeqHWKnown, because a
// zero identifier high-water mark is the one number that reads as a fact and is
// not one.
//
// The struct is split by half deliberately. Every Image field describes the
// compacted image alone and every WAL field the log alone, because on a store
// that has never been compacted the first half is empty and the second half is
// the entire cost. A single whole-store figure would report its smallest value
// for the largest store, which is why there is not one.
type OpenEstimate struct {
	// Dir is the store directory that was read.
	Dir string

	// LogPath is the file the WAL fields describe. Normally graphene.wal, but a
	// store caught inside truncateFrom's replace window has its only durable log
	// at graphene.wal.tmp, and that is the one an Open would adopt and replay.
	// See liveLogPath.
	LogPath string

	// --- the compacted image ---

	// ImageBytes is graphene.csr's size on disk, zero when there is none.
	ImageBytes int64

	// ImageVersion is the container version in the file's header.
	ImageVersion uint16

	// ImageNodeCount and ImageEdgeCount are the record counts the header
	// declares. They are what is present, not what the arrays are sized by —
	// see ImageNodeSeqHW.
	ImageNodeCount int64
	ImageEdgeCount int64

	// ImageNodeSeqHW and ImageEdgeSeqHW are the highest identifiers ever issued
	// at the time the image was built.
	//
	// This is deliberately not CSRInfo.MaxNodeID, the highest identifier
	// present. What sizes the arrays is neither: it is the pages the identifiers
	// fall in (see Build), which the header cannot reveal at all. The highest
	// present cannot be had from the header either — it is only knowable by
	// scanning every record, which is the parse this whole surface exists to
	// avoid — and the high-water mark bounds both the pages and the highest
	// present from above, which is the direction a budget wants to be wrong in.
	//
	// The two coincide until a compaction retires the top of the identifier
	// space, and the gap is measurable: 378.8 B/node with the high half deleted
	// against 530.8 B/node with the low half deleted, on the same live count. A
	// rebuild workload deletes the low half, where the bound is tight.
	ImageNodeSeqHW uint64
	ImageEdgeSeqHW uint64

	// ImageSeqHWKnown reports whether the two fields above were read from the
	// file or are simply absent from its format. Images below v5 carry no
	// high-water marks at all, and for those the resident model falls back to
	// the record counts — a floor rather than an upper bound, and the one place
	// the estimate can under-report.
	ImageSeqHWKnown bool

	// ImagePropertyNodeEntries is the node half of the property index, read
	// exactly from the GIDX section's own count.
	//
	// There is no matching edge figure. The edge count sits past every
	// variable-length node entry, so reaching it costs a walk over most of the
	// section — O(entries), which this surface does not spend. See
	// ImagePropertyEntriesMax.
	ImagePropertyNodeEntries int64

	// ImagePropertyEntriesMax bounds node and edge entries together, from the
	// GIDX section's length against the smallest an entry can be. It is what the
	// resident model uses, because the property index is the dominant term on an
	// indexed store and the bound is the only figure available in constant time.
	ImagePropertyEntriesMax int64

	// ImageHeapBytes models the Go heap the image alone retains once it is
	// loaded and the parse has settled.
	//
	// It is a model, not a measurement, and three things about it must be read
	// with it. It covers the image only: replaying the log is not in it, and on a
	// store that has never been compacted the log is the entire cost. It names
	// retained heap, not resident memory: on the one fixture that has been
	// measured, resident ran between 1.19x and 1.54x the live heap across three
	// runs of an identical store, so this is a floor on RSS rather than a
	// prediction of it. And it is built from bytes-per-entity constants measured
	// on one fixture, one OS and one Go version, with no edges in it — the edge
	// terms follow from the struct layout and have never been measured at scale.
	//
	// It is still worth reporting, because the term that dominates it is the one
	// no file-size-based guess predicts: identifier slots that were issued,
	// cannot be reused, and are paid for on every open forever.
	ImageHeapBytes int64

	// --- the log ---

	// WALBytes is the log file's whole size, container header included. This is
	// the figure WAL.Size and `store info` report.
	WALBytes int64

	// WALReplayBytes is the records region alone: WALBytes less the container
	// header. This is the figure replay bounds itself against, and the one
	// Options.MaxReplayBytes is compared to. The two differ by exactly 50 bytes
	// on a modern log and not at all on a headerless one, which is small enough
	// to go unnoticed and large enough to make a budget fed from the wrong field
	// refuse a store that has not changed.
	WALReplayBytes int64

	// WALFraming is 1 for a headerless log written before the container existed
	// and 2 once its checksums cover the record header too.
	WALFraming uint16

	// WALRecords is how many records a replay would apply.
	//
	// An upper bound, in three ways this surface cannot close. It assumes every
	// payload decodes, because decoding is the store's job and there is no store
	// here. It assumes no signature policy, because Options are not in scope of a
	// preflight — a caller opening with a Verifier or RequireSignedCommits may
	// see replay stop earlier. And a concurrent writer invalidates it in both
	// directions: a growing log makes it a lower bound, a rotated one makes it a
	// count of a file that no longer exists.
	//
	// Records buffered inside a batch that never commits are not here; they are
	// in WALRecordsBuffered.
	WALRecords int64

	// WALRecordsBuffered is whole, checksum-verified records that replay reads,
	// holds, and then throws away: the batch left open at the point replay
	// stops. They cost memory during the replay and change nothing after it,
	// which is why they are counted apart from the records that land.
	WALRecordsBuffered int64

	// WALMaxBatchRecords is the largest number of records replay buffers at once
	// before a commit lets go of them. It is the log half's transient peak — the
	// steady cost is what lands, this is what is held on the way.
	WALMaxBatchRecords int64

	// WALTruncated reports that the walk stopped before the end of the file, and
	// WALTruncatedAt is the file offset where it stopped. This is the ordinary
	// shape of a log after a crash rather than a fault: the tail record was
	// half-written, and replay stops there by design.
	WALTruncated   bool
	WALTruncatedAt int64

	// WALOpenBatch reports a begin marker with no commit at the stop point.
	// Replay discards that batch, so this is what explains a WALRecordsBuffered
	// above zero.
	WALOpenBatch bool

	// WALCheckpointed reports a checkpoint marker, at which replay stops
	// immediately. It is what explains a large WALBytes against a small
	// WALRecords when nothing is wrong.
	WALCheckpointed bool

	// WALError is non-empty when replay would refuse the log outright rather
	// than stop at a torn tail — a nested batch begin, a malformed marker, a
	// batch claiming more records than the file can hold, an unrecognised record
	// type. An Open will fail with something like it, so this is a preflight
	// finding rather than an estimate.
	WALError string
}

// PreflightOpen reports what opening the store at dir would cost, without
// opening it.
//
// It takes no lock and holds no handle open past its own reads, so it works
// against a store another process is writing to — the same posture as
// InspectCSR and InspectWAL, and for the same reason. What it adds over those
// two is a bound: the memory it spends is a constant, so it can be asked about a
// store that is too large to open, which is the only question worth asking
// before an open.
//
// The cost is one addressed read of the image's header and section directory —
// O(1), whatever the image's size — plus one sequential pass over the log. The
// log pass is not free: on a store that has never been compacted the log is the
// whole store, so a preflight followed by an Open reads it twice. The guarantee
// is bounded memory, not bounded I/O, and a caller that only wants the O(1) half
// can read the fields that do not depend on the walk.
//
// A missing directory is an error, because a question about a store that is not
// there has no honest answer; a directory holding neither file is a zeroed
// estimate and no error, which is what a store that has never been written
// actually costs. Note that disk.Open would create a missing directory, so a
// caller building a new store has nothing to preflight.
//
// Neither file is required to exist. A store that has never been compacted has
// no image and a store compacted to completion has no log, and both are
// ordinary.
//
// PreflightOpen stays a package-level function in disk and is deliberately not
// promoted to a Graph method or into store.GraphStore: the in-memory backend has
// no image and no log, so there is nothing there for it to answer about.
func PreflightOpen(dir string) (OpenEstimate, error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return OpenEstimate{}, fmt.Errorf("preflight: %w", err)
	}
	if !fi.IsDir() {
		return OpenEstimate{}, fmt.Errorf("preflight: %s is not a store directory", dir)
	}

	// The log is resolved the way an Open resolves it, not by name. A store
	// caught inside truncateFrom's replace window has no graphene.wal at all and
	// its only durable log — carrying every commit since the last compaction —
	// sitting beside it under a temporary name. Reading the name rather than the
	// state would report an empty log at the exact moment the log is largest.
	est := OpenEstimate{Dir: dir, LogPath: liveLogPath(dir)}

	if err := est.readImage(filepath.Join(dir, csrFileName)); err != nil {
		return est, err
	}
	if err := est.walkLog(est.LogPath, 0); err != nil {
		return est, err
	}
	est.ImageHeapBytes = est.imageHeapBytes()
	return est, nil
}

// readImage fills the image fields from the header and section directory.
//
// Every field is gated on the version that introduced it, which is the whole
// difficulty here: deserialiseCSR accepts v2 upward, and v2 through v4 carry no
// identifier high-water marks at all. Reading bytes 22..38 of such a file would
// return the first bytes of the node record stream as a high-water mark — an
// arbitrary number presented as a fact, which is worse than the absence it would
// be standing in for. imageCommitSeq already sets the pattern: tolerate a short
// read, check the magic, and gate on the version actually found.
func (e *OpenEstimate) readImage(path string) error {
	f, err := openSharedRead(path)
	if os.IsNotExist(err) {
		return nil // never compacted; the log is the whole store
	}
	if err != nil {
		return fmt.Errorf("preflight image: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("preflight image: %w", err)
	}
	e.ImageBytes = fi.Size()

	buf := make([]byte, csrV8HeaderSize)
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return fmt.Errorf("preflight image: %w", err)
	}
	if n < csrMinHeaderSize {
		return nil
	}
	if string(buf[0:4]) != "GCSR" {
		return fmt.Errorf("preflight image: %s is not a CSR image", path)
	}
	e.ImageVersion = binary.LittleEndian.Uint16(buf[4:6])
	e.ImageNodeCount = int64(binary.LittleEndian.Uint64(buf[6:14]))
	e.ImageEdgeCount = int64(binary.LittleEndian.Uint64(buf[14:22]))

	if e.ImageVersion >= csrVersionWithSeqHW && n >= csrV5HeaderSize {
		e.ImageNodeSeqHW = binary.LittleEndian.Uint64(buf[22:30])
		e.ImageEdgeSeqHW = binary.LittleEndian.Uint64(buf[30:38])
		e.ImageSeqHWKnown = true
	}
	if e.ImageVersion >= csrVersionSectioned && n >= csrV8HeaderSize {
		e.readIndexSection(f, binary.LittleEndian.Uint64(buf[62:70]))
	}
	return nil
}

// readIndexSection sizes the property index from the section directory.
//
// Failures here are silent, and that is deliberate: the counts are an estimate's
// input, not a verdict on the file. A directory that does not parse means the
// index term is unknown, and reporting zero for it is honest — InspectCSR is
// where a caller goes to be told a file is broken, and it will say so with the
// full parse's own bounds checks.
func (e *OpenEstimate) readIndexSection(f *os.File, tableOffset uint64) {
	size := uint64(e.ImageBytes)
	if tableOffset == 0 || tableOffset+2 > size {
		return
	}
	var countBuf [2]byte
	if _, err := f.ReadAt(countBuf[:], int64(tableOffset)); err != nil {
		return
	}
	count := int(binary.LittleEndian.Uint16(countBuf[:]))
	// The same bound readCSRSectionDirectory applies, against the file's length
	// rather than a buffer's: a directory cannot describe more sections than the
	// remaining bytes can hold.
	if remaining := size - tableOffset - 2; uint64(count) > remaining/csrSectionEntry {
		return
	}
	dir := make([]byte, count*csrSectionEntry)
	if _, err := f.ReadAt(dir, int64(tableOffset+2)); err != nil {
		return
	}

	// GIDX is magic(4), nodeCount(8), node entries, edgeCount(8), edge entries.
	// The node count is one addressed read; the edge count sits past every
	// variable-length node entry and is not reachable in constant time, so what
	// is reported for the pair is a bound rather than a count.
	const gidxCountsSize = csrIndexSectionMagicSize + 8 + 8

	for i := 0; i < count; i++ {
		ent := dir[i*csrSectionEntry:]
		if string(ent[0:4]) != csrSectionPropIndex {
			continue
		}
		off := binary.LittleEndian.Uint64(ent[8:16])
		length := binary.LittleEndian.Uint64(ent[16:24])
		if off > size || length > size-off {
			return
		}
		if length >= gidxCountsSize {
			e.ImagePropertyEntriesMax = int64(length-gidxCountsSize) / minPropEntryBytes
		}
		var head [csrIndexSectionMagicSize + 8]byte
		if _, err := f.ReadAt(head[:], int64(off)); err != nil {
			return
		}
		if string(head[0:csrIndexSectionMagicSize]) != csrIndexSectionMagic {
			return
		}
		e.ImagePropertyNodeEntries = int64(binary.LittleEndian.Uint64(head[csrIndexSectionMagicSize:]))
		return
	}
}

// pagedSlots is how many record slots an image holds: one page of slots per
// page its identifiers fall in, bounded above by the record count (no page
// exists without a record in it) and by the identifier space itself.
func pagedSlots(count int64, seqHW uint64) int64 {
	space := int64(seqHW) + 1
	pages := int64(seqHW>>csrPageBits) + 1
	if count < pages {
		pages = count
	}
	slots := pages * csrPageSlots
	if slots > space {
		slots = space
	}
	return slots
}

// directoryBytes is the page directory of one kind: one int32 per page of the
// identifier space up to the high-water mark. It is the term that does follow
// the identifier space rather than the records — 4 MiB per kind at the ceiling.
func directoryBytes(seqHW uint64) int64 {
	return (int64(seqHW>>csrPageBits) + 1) * 4
}

// imageHeapBytes is the model behind ImageHeapBytes, kept apart from the fields
// so the arithmetic is one readable expression and every term is attributable.
func (e *OpenEstimate) imageHeapBytes() int64 {
	if e.ImageBytes == 0 {
		return 0
	}

	// Slots, not records — but paged slots, not one per identifier ever issued.
	// A record's page is materialised whole, so the arrays cost a page of slots
	// for every page the identifiers fall in; the record count bounds those
	// pages above (a page needs a record to exist), and so does the identifier
	// space itself. That gap — between the identifiers issued and the pages they
	// occupy — is the cost no estimate derived from file size predicts. Below v5
	// there is no high-water mark to read and the record count is the only figure
	// available; it is a floor, and ImageSeqHWKnown is how a caller is told.
	nodeSlots, edgeSlots := e.ImageNodeCount, e.ImageEdgeCount
	var dirBytes int64
	if e.ImageSeqHWKnown {
		nodeSlots = pagedSlots(e.ImageNodeCount, e.ImageNodeSeqHW)
		edgeSlots = pagedSlots(e.ImageEdgeCount, e.ImageEdgeSeqHW)
		dirBytes = directoryBytes(e.ImageNodeSeqHW) + directoryBytes(e.ImageEdgeSeqHW)
	}

	total := nodeSlots*estNodeSlotBytes + edgeSlots*estEdgeSlotBytes + dirBytes
	total += e.ImageEdgeCount * estLiveEdgeBytes
	total += e.ImagePropertyEntriesMax * estPropertyEntryBytes

	// The arenas. Every blob and every label sequence is copied out of the file
	// buffer into one of four arenas, so what they hold is the record payload — a
	// quantity the header does not carry. The file's own size bounds it, and
	// bounding it that way also covers the two property arenas being presized to
	// a quarter of the image between them before a single byte is appended.
	total += e.ImageBytes
	return total
}

// --- the log walk ---

// walkLog fills the log fields, reading the log once with constant memory.
//
// maxRecords, when positive, stops the walk as soon as the count passes it. That
// is what keeps a budget check from costing a full pass over a log already known
// to be over budget: the walk's cost becomes a function of the budget rather
// than of the log. A zero maxRecords walks the whole log, which is what
// PreflightOpen wants.
//
// The stopping conditions mirror replayRecordsFrom exactly, and they have to. A
// count taken under looser rules than replay's is not replay's count, and the
// looser rules are easy to write by accident — InspectWAL bounds a record's
// declared length against the whole file where replay bounds it against the
// records region, which accepts a record replay refuses.
func (e *OpenEstimate) walkLog(path string, maxRecords int64) error {
	// openSharedRead rather than os.Open, and not because os.Open would fail to
	// read. On Windows a plain handle on the log blocks the writer from renaming
	// it away, so a preflight taken against a live store would make that store's
	// next compaction fail with "Access denied". The reading is incidental; not
	// standing on the writer's foot is the point. See fileshare.go's table.
	f, err := openSharedRead(path)
	if os.IsNotExist(err) {
		return nil // compacted to completion, or never written
	}
	if err != nil {
		return fmt.Errorf("preflight log: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("preflight log: %w", err)
	}
	e.WALBytes = fi.Size()

	header, dataStart, err := readWALFileHeader(f)
	if err != nil {
		return fmt.Errorf("preflight log: %w", err)
	}
	e.WALFraming = header.Version
	e.WALReplayBytes = max(e.WALBytes-dataStart, 0)
	if _, err := f.Seek(dataStart, io.SeekStart); err != nil {
		return fmt.Errorf("preflight log: %w", err)
	}

	bufSize := preflightReadBuffer
	if e.WALReplayBytes < int64(bufSize) {
		bufSize = int(e.WALReplayBytes)
	}
	if bufSize < walMinReplayBuffer {
		bufSize = walMinReplayBuffer
	}
	e.walkRecords(bufio.NewReaderSize(f, bufSize), dataStart, maxRecords)
	return nil
}

// walkRecords is the parser, separated from the file handling the way
// replayRecords is separated from Replay — and for the second reason too, which
// is that a parser driven from an io.Reader can be fed a hand-built log by a
// test without a file existing anywhere.
func (e *OpenEstimate) walkRecords(r io.Reader, dataStart, maxRecords int64) {
	logSize := e.WALReplayBytes
	framing := e.WALFraming

	var hdr [walHeaderSize]byte
	var foot [walFooterSize]byte
	chunk := make([]byte, preflightChunk)

	// marker holds the payload of a begin or commit record and nothing else.
	// Those are the only two records whose contents the walk has to read, and
	// both are small by construction — a begin is four bytes and the largest
	// legal commit is 106 — so retaining them costs a fixed buffer rather than a
	// payload-sized one.
	var marker [walBatchCommitPayloadV3]byte

	offset := dataStart
	inBatch := false
	var batchN int64
	var bodyCRC uint32

	torn := func() {
		e.WALTruncated, e.WALTruncatedAt = true, offset
	}
	stop := func() {
		e.WALRecordsBuffered, e.WALOpenBatch = batchN, inBatch
	}

	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				e.WALError = err.Error()
			}
			break
		}
		recType := hdr[0]
		length := binary.LittleEndian.Uint32(hdr[1:5])

		// A record longer than the records region is not a record. Replay treats
		// this as a torn tail rather than an error because the same bytes arise
		// from a crash mid-header, and the count has to stop where replay stops.
		if logSize > 0 && int64(length) > logSize {
			torn()
			break
		}

		// Whether this record's bytes go into the batch body checksum. The
		// markers themselves do not, and neither does anything replay would
		// refuse — which mirrors the switch in replayRecordsFrom, where only
		// records reaching the bottom of the loop are appended to the body.
		foldBody := inBatch && recType != walRecordBatchBegin &&
			recType != walRecordBatchCommit && recType != walRecordCheckpoint &&
			knownWALRecord(recType)
		if foldBody {
			bodyCRC = crc32.Update(bodyCRC, crc32.IEEETable, hdr[:])
		}

		// The record's own checksum, computed as recordCRC computes it: seeded
		// with the framed header under v2, from zero under v1. Streaming it is
		// the difference between this walk and InspectWAL — the payload passes
		// through a fixed buffer and is never held.
		var crc uint32
		if framing >= walFramingV2 {
			crc = crc32.ChecksumIEEE(hdr[:])
		}
		keep := (recType == walRecordBatchBegin || recType == walRecordBatchCommit) &&
			int64(length) <= int64(len(marker))
		short := false
		remaining := int64(length)
		for remaining > 0 {
			n := min(remaining, int64(len(chunk)))
			if _, err := io.ReadFull(r, chunk[:n]); err != nil {
				short = true
				break
			}
			crc = crc32.Update(crc, crc32.IEEETable, chunk[:n])
			if foldBody {
				bodyCRC = crc32.Update(bodyCRC, crc32.IEEETable, chunk[:n])
			}
			if keep {
				copy(marker[int64(length)-remaining:], chunk[:n])
			}
			remaining -= n
		}
		if short {
			torn()
			break
		}
		if _, err := io.ReadFull(r, foot[:]); err != nil {
			torn()
			break
		}
		if binary.LittleEndian.Uint32(foot[:]) != crc {
			torn()
			break
		}
		if foldBody {
			var fb [walFooterSize]byte
			binary.LittleEndian.PutUint32(fb[:], crc)
			bodyCRC = crc32.Update(bodyCRC, crc32.IEEETable, fb[:])
		}
		offset += int64(walRecordOverhead) + int64(length)

		switch recType {
		case walRecordBatchBegin:
			if inBatch {
				e.WALError = "wal replay: nested batch begin"
				stop()
				return
			}
			if int(length) != walBatchBeginPayload {
				e.WALError = "wal replay: malformed batch begin"
				stop()
				return
			}
			// The bound replay applies before it sizes an allocation from this
			// number. Reproduced rather than skipped, because a preflight that
			// accepted a marker replay refuses would report a cost for an open
			// that is going to fail.
			batchCount := binary.LittleEndian.Uint32(marker[:4])
			if logSize > 0 && int64(batchCount) > logSize/walRecordOverhead {
				e.WALError = fmt.Sprintf("wal replay: batch begin declares %d records, more than %d bytes can hold",
					batchCount, logSize)
				stop()
				return
			}
			inBatch, batchN, bodyCRC = true, 0, 0

		case walRecordBatchCommit:
			if !inBatch {
				e.WALError = "wal replay: batch commit without begin"
				stop()
				return
			}
			switch int(length) {
			case walBatchCommitPayloadV1, walBatchCommitPayloadV2, walBatchCommitPayloadV3:
			default:
				e.WALError = fmt.Sprintf("wal replay: malformed batch commit: payload %d bytes, expected %d, %d or %d",
					length, walBatchCommitPayloadV1, walBatchCommitPayloadV2, walBatchCommitPayloadV3)
				stop()
				return
			}
			wantCount := binary.LittleEndian.Uint32(marker[0:4])
			wantCRC := binary.LittleEndian.Uint32(marker[4:8])
			if uint32(batchN) == wantCount && bodyCRC == wantCRC {
				e.WALRecords += batchN
			}
			// A commit that does not describe what was read back discards its
			// batch, exactly as replay does. Either way the batch is closed and
			// its provisional count is spent.
			inBatch, batchN, bodyCRC = false, 0, 0

		case walRecordCheckpoint:
			// Replay returns here, so the walk stops here. Anything past a
			// checkpoint is not part of what an open would apply, however many
			// bytes of it there are.
			e.WALCheckpointed = true
			stop()
			return

		default:
			if !knownWALRecord(recType) {
				e.WALError = fmt.Sprintf("wal replay: unknown record type 0x%02X", recType)
				stop()
				return
			}
			if inBatch {
				batchN++
				if batchN > e.WALMaxBatchRecords {
					e.WALMaxBatchRecords = batchN
				}
			} else {
				e.WALRecords++
			}
		}

		// The budget gate, evaluated on every record rather than only on the ones
		// that land. Records provisionally buffered inside an open batch are
		// exactly what replay holds in memory whether or not the batch ever
		// commits, so a gate that skipped them would miss the one shape it exists
		// to catch: a log that is a single begin marker followed by everything.
		if maxRecords > 0 && e.WALRecords+batchN > maxRecords {
			stop()
			return
		}
	}

	// Reaching the end with a batch still open is the rollback: those records
	// were read and verified and are then discarded.
	stop()
}

// --- the Open-time budget ---

// checkReplayBudget refuses an Open whose log exceeds Options.MaxReplayRecords
// or Options.MaxReplayBytes, before anything is replayed.
//
// Bytes first, because bytes are free: the length and the container size are
// both already in hand, so the byte budget costs one subtraction and refuses
// without reading a thing. Records cost a pass, and the pass is skipped whenever
// arithmetic already settles the question.
func checkReplayBudget(w *WAL, opts Options) error {
	if opts.MaxReplayRecords <= 0 && opts.MaxReplayBytes <= 0 {
		return nil
	}
	replayBytes := w.replayBytes()

	if opts.MaxReplayBytes > 0 && replayBytes > opts.MaxReplayBytes {
		return fmt.Errorf("%w: the log holds %d bytes to replay, budget is %d (Options.MaxReplayBytes)",
			ErrReplayBudget, replayBytes, opts.MaxReplayBytes)
	}
	if opts.MaxReplayRecords <= 0 {
		return nil
	}

	// Every record costs at least a header and a footer, so a records region of
	// n bytes cannot hold more than n/9 records. When that ceiling is already
	// inside the budget the log provably cannot exceed it, and the counting pass
	// is skipped — which is every compacted store, and therefore the common case.
	if replayBytes/walRecordOverhead <= opts.MaxReplayRecords {
		return nil
	}

	// Only now, and only as far as the budget: the walk stops one record past it,
	// so what an over-budget log costs to refuse is set by the budget rather than
	// by the log.
	est := OpenEstimate{}
	if err := est.walkLog(w.path(), opts.MaxReplayRecords); err != nil {
		return err
	}
	if total := est.WALRecords + est.WALRecordsBuffered; total > opts.MaxReplayRecords {
		return fmt.Errorf("%w: the log holds more than %d records to replay, budget is %d (Options.MaxReplayRecords)",
			ErrReplayBudget, opts.MaxReplayRecords, opts.MaxReplayRecords)
	}
	return nil
}
