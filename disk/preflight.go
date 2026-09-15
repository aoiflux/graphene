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

// Bytes per entity, for the models below.
//
// The structural sizes are not here. They are estimate.go's — sizeofNodeRecord,
// sizeofRawEdge, sizeofOffset and the rest — and quoting them rather than
// restating them is the point: that file's constants are audited against
// unsafe.Sizeof by estimate_test.go, so a struct that grows a field fails a test
// instead of quietly under-reporting in one of two models that were supposed to
// agree. This file's own copies said 72 bytes for a node slot, which was 56 B of
// record with the two adjacency offsets folded in, and a folded term cannot be
// removed when Options say adjacency is deferred.
//
// What is here is what no struct layout gives: bytes per entry, per label and per
// key, measured. The measurements are in docs/benchmarks.md under "Program
// baselines" and in docs/MEMORY_MODEL.md.
const (
	// estPropertyEntryBytes is one property-index entry held resident. The tree
	// reports this figure three times from three dates — 107 B in
	// docs/benchmarks.md, 103.9 B in TECHNICAL_DETAILS §14.5, and 93.4 B once the
	// arity split and key interning are counted — and value interning then moves
	// it by between −34% and 0% depending on a value cardinality no header can
	// see. The largest of them is used deliberately: this term dominates an
	// indexed store, and the direction to be wrong in is up.
	//
	// Raised from 107 to 108 by measurement against the authority. The resident
	// index's own ResidentBytes reports 428,512 B for 4,000 entries on an
	// all-distinct key -- 107.128 B each -- so 107 was below the figure it was
	// meant to bound, by 512 B on that fixture, and "the largest of them" was not
	// the largest. All-distinct is the right end to bound from: value interning
	// only ever reduces the per-entry cost.
	estPropertyEntryBytes = 108

	// estMappedKeyBytes is one declared key of an index read in place, and is
	// gpixBase.ResidentBytes' own per-key figure: a name header, Kind and KeyID
	// with their padding, Distinct, Entries, and the two slice headers. There is
	// deliberately no term here that mentions entries, because that base holds
	// none — a 28M-entry key and an empty one cost the same.
	//
	// The key names themselves are not counted. Reading them means reading the
	// key directory, which is O(keys) of addressed reads this surface could
	// afford but which buys tens of bytes on a term measured in hundreds. It is
	// named as an under-report rather than left implicit.
	estMappedKeyBytes = 16 + 8 + 8 + 8 + 24 + 24

	// estMappedIndexFixedBytes is what a mapped base costs before any key: the
	// two slice headers over the reverse section.
	estMappedIndexFixedBytes = 48

	// estMappedKeyNameBytes is the allowance for one key's name, which the base
	// holds beside its directory entry. An allowance rather than a measurement:
	// reading the names means reading the key directory, and 64 bytes covers any
	// key name anyone writes. A longer one under-reports by the excess, on a term
	// whose whole magnitude was 235 B for two keys.
	estMappedKeyNameBytes = 64

	// estLabelPostingBytes is one entry of one by-label id list: an identifier in
	// nodesByLabel or edgesByLabel.
	//
	// Charged once per record, which assumes one label per entity. A record
	// carrying three labels is in three postings, and no header states how many
	// labels a record carries — so this is the second place the estimate can
	// under-report, after an image below v5 having no identifier high-water mark
	// to read. It is a small term (16 B per entity against 136 B of record slot)
	// and it is named rather than buried.
	estLabelPostingBytes = sizeofNodeID

	// estReplayRecordBytes is what one replayed record costs beyond its own
	// payload bytes.
	//
	// A record becomes one of two things and this is the larger: a property index
	// entry, at estPropertyEntryBytes, or a delta version, whose structure is a
	// map slot, a type posting and — for an edge — two adjacency list entries,
	// about 45 B between them. Telling which would mean decoding payloads, which
	// is the work this surface exists not to do, so every record is charged the
	// larger and a log of pure node writes is over-charged by the difference.
	//
	// Over-charged is the direction to be wrong in, and the term it sits beside
	// dominates it in any case: a record with a blob costs hundreds of payload
	// bytes and tens of structural ones.
	estReplayRecordBytes = estPropertyEntryBytes

	// estLabelSequenceBytes is a record's label sequence in the heap arena the
	// parse copies it into: one uint16 per label, and so again once per record
	// under the one-label assumption.
	//
	// This is the whole payload term of a mapped image. Property blobs stay in
	// the file under ImageMapped; label sequences do not, because a label is a
	// uint16 at an odd offset inside the record and cannot be aliased out of a
	// mapping without a cast. Confirmed exactly: 7,998 B for 2,000 nodes and
	// 1,999 edges.
	estLabelSequenceBytes = 2
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
	//
	// Worth reading before an open rather than after one, because it is the one
	// property of a store that makes the open itself expensive. Below
	// csrVersionMappedIndex the property index is loaded entry by entry whatever
	// IndexMode asks for -- measured at about sevenfold, 301 ms against 2,102 ms
	// on a 462 MiB store and 1.40 s against 9.94 s at 2.26 GiB -- and is then
	// held in the heap. ImageIndexMapped below is the same fact stated as the
	// thing that follows from it, and ImageHeapBytes has already taken it into
	// account.
	//
	// The remedy is a compaction under IndexMode: IndexMapped, or
	// `graphene store migrate -to 9`: one pass over the store, not an export and
	// import. An Open does not do it silently -- see Options.IndexMode.
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

	// ImageFirstNodeID is the lowest node identifier the image holds a record
	// for, and ImageFirstNodeIDKnown says whether it was readable.
	//
	// The record stream is ascending by identifier, so this is one addressed read
	// at the end of the header. It matters because it is the only figure available
	// in constant time that bounds the materialised pages from *below*: no page
	// under it holds a record. On a rebuild workload -- delete the low
	// identifiers, write new ones at the top -- that is the difference between
	// charging for every page up to the high-water mark and charging for the band
	// the records occupy. Measured on a fixture that burns 12,000 identifiers
	// before writing 500: 4 pages charged against 2 materialised, against 2 and 2
	// once this is read.
	//
	// Known only for v8 and above. Below that the record stream starts at a
	// different offset per version, and reading it at the wrong one would present
	// a record's bytes as an identifier.
	ImageFirstNodeID      uint64
	ImageFirstNodeIDKnown bool

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

	// ImagePropertyEdgeEntries is the edge half, and is only ever populated for a
	// v9 image. GPIR's header carries both counts; GIDX puts the edge count past
	// every variable-length node entry, which is the walk this surface does not
	// spend. Zero against a non-zero ImagePropertyNodeEntries therefore means
	// "not reachable in constant time from this format", not "no edge entries" --
	// ImageIndexMapped is how a caller tells which.
	ImagePropertyEdgeEntries int64

	// ImagePropertyEntriesMax bounds node and edge entries together. It is what
	// the resident model uses, because the property index is the dominant term on
	// an indexed store.
	//
	// For a v9 image it is the sum of two exact counts rather than a bound,
	// because GPIR states both. For v8 it is the GIDX section's length against
	// the smallest an entry can be, which is the only figure available in
	// constant time there.
	ImagePropertyEntriesMax int64

	// ImagePropertyKeys is how many distinct keys the image's index declares,
	// node and edge keys together, from GPIX's header. Zero for a v8 image, whose
	// GIDX section carries no key count.
	//
	// It is reported because it, and not the entry count, is what a mapped
	// index's resident cost follows: the entries stay in the file and what a
	// handle holds is per-key directory. Measured at 235 B for 4,000 entries
	// under two keys, against 428,512 B for the same entries held resident.
	ImagePropertyKeys int64

	// ImageIndexMapped reports that the image carries its property index in the
	// mapped form -- GPIX and GPIR -- rather than the resident one, GIDX.
	//
	// This is a fact about the file and not about the Options an Open will use,
	// and both decide what the index costs: a v8 image is read into the resident
	// index whatever IndexMode says, because GIDX is the only form it has, while
	// a v9 image is read into the resident index only if IndexMode asks for it.
	// See HeapBytesFor, which is where the two are combined.
	ImageIndexMapped bool

	// ImageHeapBytes models the Go heap the image alone retains once it is
	// loaded and the parse has settled, under the Options that hold the most of
	// it: a heap image, a resident index, eager adjacency.
	//
	// That it is the worst case and not the default one is the first thing to know
	// about it. Under the defaults — ImageMapped and IndexMapped — the two largest
	// terms here are very nearly absent: the property blobs are addressed in the
	// file rather than copied into arenas, and the index is a per-key directory
	// rather than 108 bytes an entry. On a 2,000-node fixture with 4,000 entries
	// the two configurations are 736 KB and 1.45 MB of the same store. The field
	// stays the worst case because it is the figure a caller has before they have
	// chosen anything; HeapBytesFor is how to ask about a configuration, and it is
	// what Options.MemoryBudget compares against.
	//
	// Four more things must be read with it. It covers the image only: replaying
	// the log is not in it, and on a store that has never been compacted the log
	// is the entire cost — that is WALHeapBytes. It names retained heap, not
	// resident memory: on the one fixture that has been measured, resident ran
	// between 1.19x and 1.54x the live heap across three runs of an identical
	// store, so this is a floor on RSS rather than a prediction of it. It is built
	// from bytes-per-entity constants measured on one fixture, one OS and one Go
	// version, with no edges in it — the edge terms follow from the struct layout
	// and have never been measured at scale. And two of its terms assume one label
	// per record, which a multi-label store exceeds; see estLabelPostingBytes.
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

	// WALHeapBytes models the Go heap replaying this log would retain, and is the
	// log half of what ImageHeapBytes is for the image.
	//
	// It is the term that makes the estimate usable on the store shape it most
	// needs to be usable on. A store that has never been compacted has no image
	// at all, so every image field is zero and the log is the entire cost; an
	// estimate that stopped at the image would report nothing for it.
	//
	// Two upper bounds, both loose in the safe direction. Every byte in the
	// records region is charged as though it lands, when in fact a log that
	// rewrites the same entity repeatedly keeps one version per entity and drops
	// the rest — the retention floor is one idle version, so a log of ten updates
	// to one node is charged ten times what it will hold. And every record is
	// charged estReplayRecordBytes whether it turns out to be an index entry or a
	// delta version.
	//
	// Not in it: what replay costs in transient memory beyond what it retains.
	// Records buffered inside a batch that never commits are held and discarded
	// (see WALRecordsBuffered), and their bytes are inside the records region, so
	// the figure covers them by construction rather than by a separate term.
	WALHeapBytes int64

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
	est.WALHeapBytes = est.walHeapBytes()
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
		e.readFirstNodeID(f)
	}
	return nil
}

// readFirstNodeID reads the identifier of the first node record, which is the
// lowest one the image holds.
//
// Silent on failure for the reason readIndexSection is: an unreadable figure
// means the bound stays where it was, which is correct and merely loose. The
// caller of a preflight is told a file is broken by InspectCSR, whose full parse
// has the bounds checks to say so.
func (e *OpenEstimate) readFirstNodeID(f *os.File) {
	if e.ImageNodeCount == 0 || e.ImageBytes < csrV8HeaderSize+8 {
		return
	}
	var buf [8]byte
	if _, err := f.ReadAt(buf[:], csrV8HeaderSize); err != nil {
		return
	}
	id := binary.LittleEndian.Uint64(buf[:])
	// A first identifier above the high-water mark is not a lower bound on
	// anything; it is a file disagreeing with itself. Left unset rather than
	// believed, because this figure only ever makes an estimate smaller.
	if e.ImageSeqHWKnown && id > e.ImageNodeSeqHW {
		return
	}
	e.ImageFirstNodeID = id
	e.ImageFirstNodeIDKnown = true
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

	// Three section magics carry a property index and the loop takes all of them,
	// because a v9 image splits the index in two: GPIX holds the values and their
	// directories, GPIR the reverse entries, and the counts this wants are in
	// GPIR's header while the key count is in GPIX's. Returning on the first
	// match, as this did while GIDX was the only form, reads whichever of the two
	// the directory happens to list first and stops.
	for i := 0; i < count; i++ {
		ent := dir[i*csrSectionEntry:]
		magic := string(ent[0:4])
		if magic != csrSectionPropIndex &&
			magic != csrSectionMappedIndex && magic != csrSectionMappedReverse {
			continue
		}
		off := binary.LittleEndian.Uint64(ent[8:16])
		length := binary.LittleEndian.Uint64(ent[16:24])
		if off > size || length > size-off {
			return
		}
		switch magic {
		case csrSectionPropIndex:
			e.readGIDXCounts(f, off, length)
		case csrSectionMappedIndex:
			e.readGPIXKeys(f, off, length)
		case csrSectionMappedReverse:
			e.readGPIRCounts(f, off, length)
		}
	}
}

// readGIDXCounts fills the index fields from a v8 GIDX section.
//
// GIDX is magic(4), nodeCount(8), node entries, edgeCount(8), edge entries. The
// node count is one addressed read; the edge count sits past every
// variable-length node entry and is not reachable in constant time, so what is
// reported for the pair is a bound rather than a count.
func (e *OpenEstimate) readGIDXCounts(f *os.File, off, length uint64) {
	const gidxCountsSize = csrIndexSectionMagicSize + 8 + 8
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
}

// readGPIXKeys reads the key counts out of a v9 GPIX header, and is what sets
// ImageIndexMapped: GPIX is the section whose presence means the entries are in
// the file in searchable form rather than waiting to be loaded.
func (e *OpenEstimate) readGPIXKeys(f *os.File, off, length uint64) {
	if length < gpixHeaderSize {
		return
	}
	var head [gpixHeaderSize]byte
	if _, err := f.ReadAt(head[:], int64(off)); err != nil {
		return
	}
	if string(head[0:4]) != csrSectionMappedIndex {
		return
	}
	if v := binary.LittleEndian.Uint16(head[4:6]); v != gpixBodyVersion {
		return
	}
	e.ImageIndexMapped = true
	e.ImagePropertyKeys = int64(binary.LittleEndian.Uint32(head[8:12])) +
		int64(binary.LittleEndian.Uint32(head[12:16]))
}

// readGPIRCounts reads both entry counts out of a v9 GPIR header.
//
// These are exact, and they are also checked against the section's own length
// before they are believed. A count read from a file that cannot hold the entries
// it claims is the one shape this surface must not pass on to an estimate that
// sizes an allocation, however honest the arithmetic downstream.
func (e *OpenEstimate) readGPIRCounts(f *os.File, off, length uint64) {
	if length < gpirHeaderSize {
		return
	}
	var head [gpirHeaderSize]byte
	if _, err := f.ReadAt(head[:], int64(off)); err != nil {
		return
	}
	if string(head[0:4]) != csrSectionMappedReverse {
		return
	}
	if v := binary.LittleEndian.Uint16(head[4:6]); v != gpirBodyVersion {
		return
	}
	nodes := binary.LittleEndian.Uint64(head[8:16])
	edges := binary.LittleEndian.Uint64(head[16:24])
	if room := (length - gpirHeaderSize) / gpirEntrySize; nodes > room || edges > room-nodes {
		return
	}
	e.ImagePropertyNodeEntries = int64(nodes)
	e.ImagePropertyEdgeEntries = int64(edges)
	e.ImagePropertyEntriesMax = int64(nodes + edges)
}

// spacePages is how many pages the identifier space spans up to the high-water
// mark. It sizes the page directory, which is the one term that follows the
// identifiers issued rather than the records held.
func spacePages(seqHW uint64) int64 {
	return int64(seqHW>>csrPageBits) + 1
}

// livePages is how many pages of one kind the image materialises, bounded above
// by the pages the identifier space spans and by the record count — a page needs
// a record in it to exist. It sizes the record slots, the inverse page table and
// the live-before prefix.
func livePages(count int64, seqHW uint64) int64 {
	return livePagesFrom(count, seqHW, 0)
}

// livePagesFrom is livePages with the band bounded from below as well: no page
// under the lowest identifier present holds a record, so the span to charge for
// starts there rather than at zero.
//
// firstPage is only ever a reduction, and a caller that does not know the lowest
// identifier passes zero and gets the old bound. See ImageFirstNodeID for what
// this is worth and on which workload.
func livePagesFrom(count int64, seqHW uint64, firstPage int64) int64 {
	pages := spacePages(seqHW) - firstPage
	if pages < 1 {
		pages = 1
	}
	if count < pages {
		pages = count
	}
	return pages
}

// nodePages is how many node pages the image materialises, using the lowest
// identifier present when the file states it.
func (e *OpenEstimate) nodePages() int64 {
	var firstPage int64
	if e.ImageFirstNodeIDKnown {
		firstPage = int64(e.ImageFirstNodeID >> csrPageBits)
	}
	return livePagesFrom(e.ImageNodeCount, e.ImageNodeSeqHW, firstPage)
}

// edgePages is the edge half. There is no matching lower bound for it: the edge
// record stream begins past every variable-length node record, so its first
// identifier is not addressable in constant time.
func (e *OpenEstimate) edgePages() int64 {
	return livePages(e.ImageEdgeCount, e.ImageEdgeSeqHW)
}

// pagedSlots is how many record slots an image holds: one whole page of slots per
// live page.
//
// Note what does not bound it. This capped its answer at the identifier space
// until R5, on the reasoning that an image cannot hold more slots than
// identifiers have been issued — which is true of an array indexed by identifier
// and false of this one. A page is materialised whole, so an image whose
// identifiers stop at 2,000 holds 4,096 node slots, and the cap reported 2,001.
// It fired on exactly the stores where it was most wrong: the smaller the
// high-water mark relative to a page, the larger the share of the dominant term
// it removed. Measured before the fix, ImageHeapBytes was 0.51x-0.85x of what the
// opened store held.
func pagedSlots(count int64, seqHW uint64) int64 {
	return livePages(count, seqHW) * csrPageSlots
}

// directoryBytes is the page directory of one kind: one int32 per page of the
// identifier space up to the high-water mark — 4 MiB per kind at the ceiling.
func directoryBytes(seqHW uint64) int64 {
	return spacePages(seqHW) * sizeofDirEntry
}

// imageSlots is how many record slots of each kind the image holds.
//
// Below v5 there is no identifier high-water mark to read and the record count is
// the only figure available. That is a floor rather than a bound — a sparse v4
// image costs more than this says — and ImageSeqHWKnown is how a caller is told
// which of the two they have.
func (e *OpenEstimate) imageSlots() (nodeSlots, edgeSlots int64) {
	if !e.ImageSeqHWKnown {
		return e.ImageNodeCount, e.ImageEdgeCount
	}
	return e.nodePages() * csrPageSlots, e.edgePages() * csrPageSlots
}

// recordArrayBytes mirrors CSRGraph.recordArrayBytes, term for term, from what a
// header states instead of from slice lengths.
//
// This is the term the identifier space drives rather than the record count, and
// therefore the cost no estimate derived from file size predicts: a page is
// materialised whole, so an image holds a page of slots for every page its
// identifiers fall in whether or not the records fill them.
func (e *OpenEstimate) recordArrayBytes() int64 {
	nodeSlots, edgeSlots := e.imageSlots()
	total := nodeSlots*sizeofNodeRecord + edgeSlots*sizeofRawEdge
	if !e.ImageSeqHWKnown {
		return total
	}
	pages := e.nodePages() + e.edgePages()
	// The directories are indexed by page of the identifier space and are the one
	// term the lower bound does not reduce: a dead page below the records still
	// costs its four bytes of "no page here".
	total += directoryBytes(e.ImageNodeSeqHW) + directoryBytes(e.ImageEdgeSeqHW)
	total += pages * sizeofPageEntry
	total += pages * sizeofLiveBefore
	return total
}

// adjacencyBytes mirrors CSRGraph.adjacencyBytes: two offset arrays indexed by
// slot with a sentinel, and two identifier arrays indexed by live edge.
//
// Zero under AdjacencyLazy, which is the whole reason the offsets are no longer
// folded into a per-slot constant.
func (e *OpenEstimate) adjacencyBytes() int64 {
	nodeSlots, _ := e.imageSlots()
	return (nodeSlots+1)*2*sizeofOffset + e.ImageEdgeCount*2*sizeofEdgeID
}

// labelPostingBytes mirrors CSRGraph.labelPostingBytes under the one-label
// assumption estLabelPostingBytes states.
//
// The two maps are charged for one distinct label each, which is the floor for a
// store that holds records at all. A store with many distinct labels pays more
// per map and this says less, which is the same under-report the posting count
// itself carries and is named in the same place.
func (e *OpenEstimate) labelPostingBytes() int64 {
	if e.ImageNodeCount == 0 && e.ImageEdgeCount == 0 {
		return 0
	}
	const labelMapKeyVal = 2 + 24 // a NodeType or EdgeType, and a slice header
	return (e.ImageNodeCount+e.ImageEdgeCount)*estLabelPostingBytes +
		2*residentMapBytes(1, labelMapKeyVal)
}

// payloadBytes is what the record payloads cost in the heap, which is the one
// term ImageMode changes by more than a rounding.
//
// Mapped, only the label sequences are copied out; the property blobs are
// addressed in the file. In the heap, every blob and every label sequence is
// copied into one of four arenas, and what those hold is a quantity no header
// carries — so the file's own size bounds it. Bounding it that way also covers
// the two property arenas being presized to a quarter of the image between them
// before a single byte is appended.
func (e *OpenEstimate) payloadBytes(mapped bool) int64 {
	if mapped {
		return (e.ImageNodeCount + e.ImageEdgeCount) * estLabelSequenceBytes
	}
	return e.ImageBytes
}

// indexBytes is the property index, resident or read in place.
//
// The two differ by three orders of magnitude on the same entries, which is why
// the whole of R3 happened and why a pre-open estimate that cannot tell them
// apart is not usable as a budget: 107 B per entry against 88 B per declared key.
func (e *OpenEstimate) indexBytes(resident bool) int64 {
	if resident {
		return e.ImagePropertyEntriesMax * estPropertyEntryBytes
	}
	if !e.ImageIndexMapped {
		return 0
	}
	return e.ImagePropertyKeys*(estMappedKeyBytes+estMappedKeyNameBytes) +
		estMappedIndexFixedBytes
}

// imageHeapBytes is the model behind ImageHeapBytes: the image's heap under the
// configuration that holds the most of it.
//
// Deliberately the worst case over Options rather than the default one. This is
// the figure a caller has before they have chosen anything, and the field is
// documented as such; HeapBytesFor is how to ask about a configuration.
func (e *OpenEstimate) imageHeapBytes() int64 {
	if e.ImageBytes == 0 {
		return 0
	}
	return e.recordArrayBytes() + e.adjacencyBytes() + e.labelPostingBytes() +
		e.payloadBytes(false) + e.indexBytes(true)
}

// walHeapBytes is the model behind WALHeapBytes.
//
// Buffered records are counted with the landed ones rather than instead of them:
// the two together are what the process holds at the moment replay stops, which is
// its peak, and a budget is asked about a peak.
func (e *OpenEstimate) walHeapBytes() int64 {
	if e.WALReplayBytes == 0 {
		return 0
	}
	return e.WALReplayBytes +
		(e.WALRecords+e.WALRecordsBuffered)*estReplayRecordBytes
}

// HeapBytesFor is what opening this store under opts would retain in the Go heap,
// image and log together.
//
// This is the figure Options.MemoryBudget is compared against, and the reason it
// takes Options rather than being a field is that the same store differs by more
// than a factor of two across them: on a 2,000-node fixture with 4,000 index
// entries, 735,997 B under the defaults against 1,236,992 B under ImageHeap with
// a resident index. A budget fed the wrong one of those refuses stores that fit
// or admits stores that do not, and which of the two it does depends on which
// way the caller happened to be wrong.
//
// The three modes are resolved the way an Open resolves them, by asking the
// functions that own each rule rather than by restating it here:
// imageMappingAllowedFor for the image, indexBaseAllowedFor for the index. That
// matters most where the answer is not the mode the caller named — a live reader
// holds no lock and therefore reads a heap image under the default ImageMode, a
// platform with no mapping primitive does the same, and a v8 image is read into a
// resident index whatever IndexMode asks for, because GIDX is the only form it
// has.
//
// Retained heap, not resident set size, and an estimate throughout. Read
// ImageHeapBytes' three caveats before acting on it: they all apply here.
func (e OpenEstimate) HeapBytesFor(opts Options) int64 {
	mapped := e.ImageBytes > 0 &&
		imageMappingAllowedFor(opts.ImageMode, opts.LiveReader) == nil
	resident := indexBaseAllowedFor(opts.IndexMode, mapped) != nil || !e.ImageIndexMapped

	var total int64
	if e.ImageBytes > 0 {
		total = e.recordArrayBytes() + e.labelPostingBytes() +
			e.payloadBytes(mapped) + e.indexBytes(resident)
		if opts.Adjacency != AdjacencyLazy {
			total += e.adjacencyBytes()
		}
	}
	return total + e.WALHeapBytes + defaultWALRingCapacity*sizeofWALSlot
}

// FallbacksFor names every mode an Open under opts would ask for and not get,
// and why. Empty when opts would be honoured exactly.
//
// It is the cascade as sentences, before the open rather than after it. Both
// fallbacks are already reported at the open itself, by store.MetricImageFallback
// and store.MetricIndexFallback — but a metric arrives once the memory has
// already been spent, and the case this exists for is a caller deciding whether
// to spend it. An OpenLive on the defaults is the example worth knowing: it asks
// for a mapped image, holds no lock, reads the image into the heap, and is then
// declined the mapped index because the image is in the heap. Measured, that is
// 419 MiB on a 248 MiB store, and nothing in the call said so.
//
// The reasons are the same strings the metrics carry, and they come from the same
// two functions an Open asks — imageMappingAllowedFor and indexBaseAllowedFor —
// for the reason HeapBytesFor does it that way: a second copy of the rule is a
// second thing to keep true.
//
// A fresh slice per call, so a caller cannot edit the answer.
func (e OpenEstimate) FallbacksFor(opts Options) []string {
	if e.ImageBytes == 0 {
		// No image, no modes to fall back from. A store that has never been
		// compacted builds everything from the log whatever it was asked for.
		return nil
	}

	var out []string
	mapped := imageMappingAllowedFor(opts.ImageMode, opts.LiveReader) == nil
	if !mapped && opts.ImageMode != ImageHeap {
		out = append(out, "the image will be read into the heap: "+
			imageMappingAllowedFor(opts.ImageMode, opts.LiveReader).Error())
	}

	switch {
	case !e.ImageIndexMapped && opts.IndexMode == IndexMapped:
		out = append(out, fmt.Sprintf(
			"the property index will be rebuilt in the heap: the image is version %d, "+
				"which carries it entry by entry; a mappable index arrives with version %d, "+
				"written by the next Compact under IndexMode: IndexMapped, or by "+
				"`graphene store migrate -to %d`",
			e.ImageVersion, csrVersionMappedIndex, csrVersionMappedIndex))
	case e.ImageIndexMapped:
		if why := indexBaseAllowedFor(opts.IndexMode, mapped); why != nil && opts.IndexMode != IndexResident {
			out = append(out, "the property index will be rebuilt in the heap: "+why.Error())
		}
	}
	return out
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
