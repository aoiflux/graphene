package disk

// Reading the CSR image back off disk: header and version handling, record
// parsing, the GIDX property-index section, and the bounds every one of those
// applies before allocating from a number the file supplied. Split out of
// store.go, unchanged.
//
// Everything here parses bytes the engine did not necessarily write, so it is
// the package's untrusted-input surface. FuzzDeserialiseCSR covers it.

import (
	"encoding/binary"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// loadCSR maps or reads the CSR image at path and installs it. Caller holds
// s.mu exclusively.
func (s *Store) loadCSR(path string) error {
	src, err := s.openImage(path)
	if err != nil {
		return err
	}
	return s.loadImage(src)
}

// loadImage parses an image the caller has already obtained and installs it.
// Caller holds s.mu exclusively.
//
// It takes ownership of src: a mapping is released here if the parse fails, so
// no caller has to reason about whether a failed load left one behind. Open
// calls it with the same source VerifyOnOpen hashed, which is what keeps an
// integrity check at one read of the image instead of three.
func (s *Store) loadImage(src *imageSource) error {
	csr, section, err := deserialiseCSRFrom(src.data, src.mapped(), s.adjacency)
	if err != nil {
		src.discard()
		return err
	}

	// The index before the graph, and that order is load-bearing rather than
	// tidy. Loading the index can fail — a v9 image whose runs will not decode is
	// a damaged file, and a store that came up on one would answer from a
	// half-built index — and src is this function's to release only until
	// noteImage hands the mapping's lifetime to the collector. A failure after
	// that point would leave the image mapped for the life of the process.
	//
	// Nothing in here needs the published view: the index is a structure beside
	// the graph, not a reader of it.
	if err := s.loadIndex(section, src.mapped()); err != nil {
		src.discard()
		return err
	}

	// What the file itself said, taken before noteImage hands src away. The
	// parse above has already validated these bytes, so re-reading the two of
	// them is cheaper than threading a version out of a function whose job is to
	// produce a graph.
	s.noteImageFormat(binary.LittleEndian.Uint16(src.data[4:6]), section)

	s.publishCSR(csr)

	// After the graph is published, because noteImage attaches the cleanup that
	// decides when the mapping may be released and the graph has to exist first.
	s.noteImage(src, csr)

	// Advance sequence counters past existing CSR IDs. This is the pre-v5
	// fallback: files that carry high-water marks raise the counters again
	// below, and those marks win because a deleted record's ID must not be
	// handed out twice.
	if hw := uint64(csr.HighestNodeID()); hw > s.nodeSeq.Load() {
		s.nodeSeq.Store(hw)
	}
	if hw := uint64(csr.HighestEdgeID()); hw > s.edgeSeq.Load() {
		s.edgeSeq.Store(hw)
	}
	// Restore the marks that used to be lost at every compaction (v8+). Zero
	// means the file predates them, in which case the commit counter resumes
	// from whatever the surviving WAL replays — the pre-v8 behaviour.
	if csr.commitSeqHW > s.commitSeq.Load() {
		s.commitSeq.Store(csr.commitSeqHW)
	}
	if csr.lastCompactUnixNano != 0 {
		s.lastCompact = time.Unix(0, csr.lastCompactUnixNano)
	}

	// Honour the persisted high-water marks so IDs are never reused even when the
	// record that held the max ID was deleted before this CSR was written.
	if csr.nodeSeqHW > s.nodeSeq.Load() {
		s.nodeSeq.Store(csr.nodeSeqHW)
	}
	if csr.edgeSeqHW > s.edgeSeq.Load() {
		s.edgeSeq.Store(csr.edgeSeqHW)
	}
	return nil
}

// noteImageFormat records what the image on disk is, and reports a mapped index
// that the file cannot supply. Caller holds s.mu exclusively.
//
// The report is the point. A v8 image opened under IndexMapped gets a resident
// index, and that is not a bug -- there is no mappable index in the file to read
// -- but it is the most expensive thing that can happen at an open with no
// symptom attached to it. Measured, the difference is about sevenfold: 301 ms
// against 2,102 ms on a 462 MiB store, 1.40 s against 9.94 s at 2.26 GiB,
// because the entries are loaded one at a time whatever the mode asked for. And
// then the index sits in the heap at about a hundred bytes an entry, which is
// the cost the mode exists to avoid.
//
// MetricIndexFallback's documentation used to say this case emitted nothing,
// on the grounds that a file with no mappable index is not something to fall
// back *from*. That reasoning was about where the cause lies and the metric is
// about what the caller is paying, and the second is what a caller subscribes to
// it for. The three causes are distinguished by Err, and this one names its
// remedy, which the other two cannot.
func (s *Store) noteImageFormat(version uint16, section *csrIndexSection) {
	s.imageVersion = version
	switch {
	case version >= csrVersionMappedIndex:
		s.imageIndexOnDisk = indexOnDiskMapped
	case section != nil:
		s.imageIndexOnDisk = indexOnDiskEntries
	default:
		s.imageIndexOnDisk = indexOnDiskNone
	}

	if s.indexMode == IndexMapped && s.imageIndexOnDisk == indexOnDiskEntries {
		s.recordIndexFallback(fmt.Errorf(
			"the image is version %d, which carries its property index entry by entry; "+
				"a mappable index arrives with version %d, written by the next Compact "+
				"under IndexMode: IndexMapped, or by `graphene store migrate -to %d`",
			version, csrVersionMappedIndex, csrVersionMappedIndex))
	}
}

// loadIndex installs the property index the image carries (v6+).
//
// It runs before WAL replay, so records written after the last compaction —
// including purges — apply on top of it. A pre-v6 file carries no section at
// all: its entries come from the WAL as before, and the next Compact writes them
// into the image.
//
// mapped says whether the image itself is mapped, which is what decides whether
// an index read out of it is sound. See IndexMode.
func (s *Store) loadIndex(section *csrIndexSection, mapped bool) error {
	if section == nil {
		return nil
	}

	// The base is attached first. Either order would do — AttachBase fills the
	// composites already declared and a later declaration fills itself, because a
	// store restores its declarations from its catalogue before it ever reaches
	// this function and the fill has to work both ways round — but attaching first
	// is the order that reads as what is happening: the index acquires its
	// disk-resident half, and then the declarations are re-stated over it.
	attached := false
	if section.Base != nil {
		if why := s.indexBaseAllowed(mapped); why == nil {
			if err := s.propIdx.AttachBase(section.Base); err != nil {
				return err
			}
			attached = true
		} else if s.indexMode == IndexMapped {
			// Reported, not refused. The resident index answers every query the
			// mapped one answers, at the cost this program exists to remove, so a
			// mode that cannot be honoured is a memory decision that did not go the
			// caller's way and not a reason the store cannot open. The metric is
			// skipped when the mode is what ruled it out, because a caller who asked
			// for the resident index is not being told it got one.
			s.recordIndexFallback(why)
		}
	}

	// Re-declared before anything is loaded, so the ordered structures are built
	// by the same incremental path a live declaration uses rather than by a
	// backfill afterwards. Either order is correct — DeclareOrderedNodeKey
	// backfills from what is already indexed — but doing it first means there
	// is one path to reason about. Under a base this is also the only order that
	// works, because what is loaded below is nothing: the declarations are what
	// the entries are read for.
	//
	// This is what stops a reopen silently turning every declared range query
	// back into a scan.
	for _, k := range section.OrderedNodeKeys {
		s.propIdx.DeclareOrderedNodeKey(k)
	}
	for _, k := range section.OrderedEdgeKeys {
		s.propIdx.DeclareOrderedEdgeKey(k)
	}
	// Composites are re-declared here for the same reason and with the same
	// effect: whatever is loaded below then maintains them as it lands, rather
	// than a backfill re-deriving what the incremental path would have built.
	// Over a base nothing lands, so the declaration is what fills them — from
	// the base's own runs, which is the one resident structure a mapped index
	// still pays for.
	//
	// A tuple this build will not accept is skipped, not fatal. GCMP is an
	// optional section, which is a commitment that a reader ignoring it
	// entirely still answers every query correctly — so refusing the whole
	// store over one declaration would break exactly the compatibility the
	// flag exists to provide, and would do it to a later version that
	// declares tuples this one does not allow. What is skipped is visible:
	// CompositeNodeProperties reports what is actually declared, so a tuple
	// that did not survive the open is absent there rather than assumed.
	//
	// A malformed section *body* is still fatal — see readCompositeSection.
	// That is damage to the file, not a disagreement about its contents.
	for _, keys := range section.CompositeNodeKeys {
		_ = s.propIdx.DeclareCompositeNodeKeys(keys)
	}
	for _, keys := range section.CompositeEdgeKeys {
		_ = s.propIdx.DeclareCompositeEdgeKeys(keys)
	}

	if attached {
		// Every entry is already where it is read from, so there is nothing to
		// load. The one thing this open may still read out of the base is the
		// composites' backfill, and a fault recorded there is a run that would
		// not decode -- damage to the file. The alternative to refusing is a
		// store that comes up serving a composite index missing whatever the
		// damaged run held.
		//
		// A composite the image carries postings for is not backfilled, so it
		// reads nothing here and this check has nothing to find for it. That is a
		// real change and not a gap left open: those runs stop being read at open
		// because the composite stops being derived from them, and they then
		// behave like every other key in the image -- damage is met by the read
		// that touches it and recorded for BaseFault, Verify and VerifyIndexes to
		// report. An open that wants the O(entries) pass asks for it with a
		// Verifier. See TestIndexMapped_DamagedRunSurfacesAsAFault, which pins
		// both halves.
		return s.propIdx.BaseFault()
	}
	if section.Base != nil {
		return s.rebuildIndexFrom(section.Base)
	}

	// Deliberately per-entry. Bulk loading was built and measured here — one
	// lock per shard, parallel fill, presized reverse map, batch-local value
	// interning — and it cut allocations 9-19% but cost 35-75% more resident
	// memory, because partitioning copies every entry into per-shard slices
	// and the presize is keyed on entry count rather than entity count
	for _, e := range section.NodeProps {
		s.propIdx.IndexNode(e.ID, e.Key, e.Value)
	}
	for _, e := range section.EdgeProps {
		s.propIdx.IndexEdge(e.ID, e.Key, e.Value)
	}
	return nil
}

// rebuildIndexFrom walks a mapped index into the resident one.
//
// This is what a v9 image does under IndexResident, and what one does under
// IndexMapped when the store is holding the image in the heap. It is deliberately
// the same per-entry path GIDX's entries take: two arms of one option must
// produce the same index out of the same file, and the bulk loader §14.4 measured
// and reverted stays reverted.
//
// The values handed to IndexNode are the image's own bytes, and IndexNode interns
// a string out of every one of them, so nothing here retains mapped memory. That
// is also what makes this the expensive arm — it is the ~107 bytes an entry the
// mapped index does not pay.
func (s *Store) rebuildIndexFrom(b index.Base) error {
	if err := forEachBaseEntry(b, index.NodeKind, func(id uint64, key string, value []byte) bool {
		s.propIdx.IndexNode(store.NodeID(id), key, value)
		return true
	}); err != nil {
		return fmt.Errorf("deserialiseCSR: rebuilding the node index from the image: %w", err)
	}
	if err := forEachBaseEntry(b, index.EdgeKind, func(id uint64, key string, value []byte) bool {
		s.propIdx.IndexEdge(store.EdgeID(id), key, value)
		return true
	}); err != nil {
		return fmt.Errorf("deserialiseCSR: rebuilding the edge index from the image: %w", err)
	}
	return nil
}

// basePropSeqs presents a base's entries as the payload sequences, so a caller
// that wants the entries out of a v9 image reaches them the same way it reaches a
// v8 one's.
//
// Re-runnable, which csrPayload requires of both fields: each call walks the base
// again. A fault lands in *err rather than stopping the walk silently, because an
// iter.Seq has nowhere to put one and a truncated stream of entries is exactly
// what would otherwise be mistaken for a file whose index is smaller than it is.
func basePropSeqs(b index.Base, err *error) (iter.Seq[index.NodePropEntry], iter.Seq[index.EdgePropEntry]) {
	return func(yield func(index.NodePropEntry) bool) {
			e := forEachBaseEntry(b, index.NodeKind, func(id uint64, key string, value []byte) bool {
				return yield(index.NodePropEntry{ID: store.NodeID(id), Key: key, Value: value})
			})
			if e != nil && *err == nil {
				*err = fmt.Errorf("reading the image's node index: %w", e)
			}
		}, func(yield func(index.EdgePropEntry) bool) {
			e := forEachBaseEntry(b, index.EdgeKind, func(id uint64, key string, value []byte) bool {
				return yield(index.EdgePropEntry{ID: store.EdgeID(id), Key: key, Value: value})
			})
			if e != nil && *err == nil {
				*err = fmt.Errorf("reading the image's edge index: %w", e)
			}
		}
}

// forEachBaseEntry walks one kind of a base as flat (id, key, value) triples, in
// the (key, value, id) order the GIDX stream is in.
//
// That order is the whole reason this is one function rather than two similar
// loops. It is a determinism contract -- PropertyIndex.NodeEntries states it at
// length -- and the index root is a Merkle tree over the entries in it, so a
// walk that reproduced the entries in some other order would rebuild a correct
// index and compute a root that describes nothing. The two callers are the
// resident rebuild and the root verifier, and they have to agree with each other
// and with the GIDX path they replace.
func forEachBaseEntry(b index.Base, kind index.EntityKind,
	fn func(id uint64, key string, value []byte) bool,
) error {
	for _, key := range b.Keys(kind) {
		stopped := false
		err := b.ForEachValue(kind, key, nil, func(value []byte, ids index.IDRun) bool {
			for i, n := 0, ids.Len(); i < n; i++ {
				if !fn(ids.At(i), key, value) {
					stopped = true
					return false
				}
			}
			return true
		})
		if err != nil {
			return fmt.Errorf("key %q: %w", key, err)
		}
		if stopped {
			return nil
		}
	}
	return nil
}

// deserialiseCSR reconstructs a CSRGraph from Serialise() byte slices.
// Format v2 stores labels plus a reserved property offset; format v3 stores
// 1-byte labels plus inline property blobs; format v4 stores uint16 labels
// plus inline property blobs.
func deserialiseCSR(data []byte) (*CSRGraph, *csrIndexSection, error) {
	// Eager, because this is the offline entry point — inspect, prove, a test —
	// and there is no Options in reach to say otherwise. Nothing here is holding
	// the result for the life of a process.
	return deserialiseCSRFrom(data, false, AdjacencyEager)
}

// deserialiseCSRFrom is deserialiseCSR over bytes that may be a mapping.
//
// mapped says that data will not move and will outlive the graph, which changes
// one thing: a record's property blob is a sub-slice of data rather than a copy
// into an arena the graph owns. That removes the arena -- the blob half of an
// image, which on the store this program is aimed at is most of it -- and it
// removes the span array the copy needed, because spans exist only to survive
// the arena's own growth reallocating under a slice taken mid-parse. A mapping
// never grows.
//
// Labels are copied in both modes. A label is a uint16 written at offset +9 of a
// record, so the on-disk stream is unaligned and reading it in place would need
// an unsafe cast; three megabytes at a million and a half nodes is not worth one.
//
// Everything else is identical, deliberately: the bounds, the order, the
// sections, the errors. A file parses to the same graph either way, which is what
// TestImageMode_SameGraphEitherWay asserts.
func deserialiseCSRFrom(data []byte, mapped bool, adj AdjacencyMode) (*CSRGraph, *csrIndexSection, error) {
	// Where the records begin, how many there are, and the bounds on both counts:
	// see csrRecordRegion, which is also what the walks start from, so the loader
	// and the walks cannot disagree about it.
	_, version, nodeCount, edgeCount, err := csrRecordRegion(data)
	if err != nil {
		return nil, nil, err
	}

	// Sequence high-water marks (version 5+).
	var nodeSeqHW, edgeSeqHW uint64
	if version >= csrVersionWithSeqHW {
		if len(data) < csrV5HeaderSize {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated sequence high-water header")
		}
		nodeSeqHW = binary.LittleEndian.Uint64(data[22:30])
		edgeSeqHW = binary.LittleEndian.Uint64(data[30:38])
	}

	// Property-index section offset (version 6+).
	var indexOffset uint64
	if version >= csrVersionWithPropIndex {
		if len(data) < csrV6HeaderSize {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated index-offset header")
		}
		indexOffset = binary.LittleEndian.Uint64(data[38:46])
	}

	// v8 header additions and the section directory.
	var trailer csrV8Trailer
	if version >= csrVersionSectioned {
		if len(data) < csrV8HeaderSize {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated v8 header: %d bytes, need %d",
				len(data), csrV8HeaderSize)
		}
		trailer.CommitSeqHW = binary.LittleEndian.Uint64(data[46:54])
		trailer.LastCompactUnixNano = int64(binary.LittleEndian.Uint64(data[54:62]))
		sectionTableOffset := binary.LittleEndian.Uint64(data[62:70])
		copy(trailer.Digest[:], data[csrDigestOffset:csrDigestOffset+csrDigestSize])

		sections, err := readCSRSectionDirectory(data, sectionTableOffset)
		if err != nil {
			return nil, nil, fmt.Errorf("deserialiseCSR: %w", err)
		}
		trailer.Sections = sections
		// Refuse a file whose critical sections this build cannot interpret,
		// rather than reading it as though they were absent.
		if err := checkCriticalSections(trailer.Sections); err != nil {
			return nil, nil, err
		}
		// The directory addresses the property index in v8; the v6 offset field
		// is written as zero.
		if s, ok := findSection(trailer.Sections, csrSectionPropIndex); ok {
			indexOffset = s.Offset
		}
	}

	// The records are walked rather than materialised. See csr_load_stream.go:
	// the parse used to build a []nodeRecord and a []rawEdge and hand them to
	// buildSeq, which copied every record into the arena it keeps -- two copies
	// of the whole record set alive at once, 112 MB of them at two million
	// records, visible only in the peak because the first is garbage by the time
	// anything looks.
	// One pass fills the arenas the walks read from and locates the edge region.
	// It is also where a truncated record is found, which is why every later walk
	// can treat a failure as unreachable rather than as the ordinary case.
	src, err := csrRecordSourceOf(data, mapped)
	if err != nil {
		return nil, nil, err
	}

	// Build allocates one int32 per page of the identifier space up to the
	// highest ID named, and a full page of records for every page a record or
	// an endpoint falls in. Bounding the record counts above is therefore not
	// enough: two records carrying IDs of 0x3030303030303030 would make a
	// 105-byte file demand an exabyte-scale directory, which panics in makeslice
	// rather than returning an error. Validate the IDs before Build sees them.
	shape, err := csrShapeOf(src.nodeSeq(), src.edgeSeq(), src.ext,
		nodeCount, edgeCount, version, nodeSeqHW, edgeSeqHW)
	if err != nil {
		return nil, nil, err
	}

	csr, err := buildSeqShaped(src.nodeSeq(), src.edgeSeq(), adj, shape)
	if err != nil {
		return nil, nil, err
	}
	// A walk cannot return an error, so it latches one. Checked here rather than
	// inside buildSeq because a short sequence trips errUnstableBuild first and
	// that message would name the symptom rather than the truncated record.
	if src.err != nil {
		return nil, nil, src.err
	}
	if mapped {
		csr.imgBytes = int64(len(data))
	}
	// The arenas' capacity rather than buildSeq's sum of what it placed. Both
	// describe the same payload; this one includes the slack an append-grown
	// arena is holding, and under ImageMapped it correctly omits the property
	// half, which is in the mapping and counted as ImageMappedBytes. See
	// CSRGraph.payloadBytes.
	csr.payloadBytes = src.payloadBytes()
	csr.nodeSeqHW = nodeSeqHW
	csr.edgeSeqHW = edgeSeqHW
	csr.commitSeqHW = trailer.CommitSeqHW
	csr.lastCompactUnixNano = trailer.LastCompactUnixNano

	// Pre-v6 files carry no index section; the caller falls back to the WAL.
	if version < csrVersionWithPropIndex {
		return csr, nil, nil
	}
	// The index travels one of two ways, never both. A v9 image carries it as its
	// own two sections, read where they lie; anything earlier carries the GIDX
	// stream, which the loader replays entry by entry. Which one the file holds
	// decides this, not the version it declares — the sections are what make it a
	// v9 image, and the version number is there so an older build says "written by
	// a newer version" rather than "unknown section".
	base, mappedIndex, err := readMappedIndexSections(data, trailer.Sections)
	if err != nil {
		return nil, nil, err
	}
	var section *csrIndexSection
	if mappedIndex {
		section = &csrIndexSection{Base: base}
	} else if section, err = readCSRIndexSection(data, int(indexOffset)); err != nil {
		return nil, nil, err
	}

	// Tombstones (v8+, GRDT). Read before the roots, because the roots commit to
	// them and the check below needs both.
	if s, ok := findSection(trailer.Sections, csrSectionTombstones); ok {
		ts, err := readTombstoneSection(data[s.Offset : s.Offset+s.Length])
		if err != nil {
			return nil, nil, fmt.Errorf("deserialiseCSR: tombstone section: %w", err)
		}
		csr.tombstones = ts
	}

	// Snapshot roots (v8+, GHSH). Critical when present, so an unreadable one is
	// a hard failure rather than a missing convenience: this section is the
	// file's integrity evidence.
	if s, ok := findSection(trailer.Sections, csrSectionEntityHash); ok {
		roots, err := readSnapshotSection(data[s.Offset : s.Offset+s.Length])
		if err != nil {
			return nil, nil, fmt.Errorf("deserialiseCSR: snapshot section: %w", err)
		}

		// The tombstones in the file must be the ones the root commits to. A
		// mismatch means the section was added, removed, or edited after the
		// roots were computed — which is precisely how someone would try to make
		// a removal disappear, or invent one that never happened.
		if roots.BodyVersion >= snapshotBodyV2 {
			if got := merkle.Root(tombstoneLeaves(csr.tombstones)); got != roots.TombstoneRoot {
				return nil, nil, fmt.Errorf(
					"deserialiseCSR: the image's tombstones produce root %x but the snapshot names %x",
					got[:8], roots.TombstoneRoot[:8])
			}
		} else if len(csr.tombstones) > 0 {
			return nil, nil, fmt.Errorf(
				"deserialiseCSR: the image carries %d tombstones but its snapshot root predates them "+
					"and does not cover them", len(csr.tombstones))
		}
		csr.roots = roots
	}

	// Attestation (v8+, GATT). Also critical: an unreadable one must fail the
	// open rather than leave the image looking merely unattested.
	if s, ok := findSection(trailer.Sections, csrSectionAttestation); ok {
		att, err := readAttestationSection(data[s.Offset : s.Offset+s.Length])
		if err != nil {
			return nil, nil, fmt.Errorf("deserialiseCSR: attestation section: %w", err)
		}
		// The attestation must be about the snapshot in the same file. One
		// naming a different subject is either misassembled or lifted from
		// elsewhere, and either way it does not vouch for this image.
		if att.Subject != csr.roots.Snapshot {
			return nil, nil, fmt.Errorf(
				"deserialiseCSR: attestation names snapshot %x but this image is %x",
				att.Subject[:8], csr.roots.Snapshot[:8])
		}
		csr.attestation = att
	}

	// Ordered-key declarations (v8+). Optional, so a file without the section
	// simply carries no declarations — which is what every pre-v8 file reports.
	if s, ok := findSection(trailer.Sections, csrSectionOrderedKeys); ok {
		nodeKeys, edgeKeys, err := readOrderedKeySection(data[s.Offset : s.Offset+s.Length])
		if err != nil {
			return nil, nil, fmt.Errorf("deserialiseCSR: ordered-key section: %w", err)
		}
		section.OrderedNodeKeys = nodeKeys
		section.OrderedEdgeKeys = edgeKeys
	}

	// Composite declarations (v8+). Optional on the same terms as GORD.
	if s, ok := findSection(trailer.Sections, csrSectionComposite); ok {
		nodeTuples, edgeTuples, err := readCompositeSection(data[s.Offset : s.Offset+s.Length])
		if err != nil {
			return nil, nil, fmt.Errorf("deserialiseCSR: composite section: %w", err)
		}
		section.CompositeNodeKeys = nodeTuples
		section.CompositeEdgeKeys = edgeTuples
	}
	return csr, section, nil
}

// csrIndexSection carries everything parsed out of a file's optional sections.
//
// It began as the GIDX property index alone, which is where the name comes
// from; v8 turned sections into a directory and the ordered-key declarations
// joined it. Any future optional section a loader needs to act on belongs here
// too.
type csrIndexSection struct {
	NodeProps []index.NodePropEntry
	EdgeProps []index.EdgePropEntry

	// Base is the index read in place out of GPIX and GPIR (v9+), present
	// *instead of* NodeProps and EdgeProps rather than beside them. A caller
	// either attaches it or walks it into the resident index; either way it
	// addresses the bytes this section was parsed from, so it is only as valid as
	// they are.
	Base index.Base

	// Keys declared ordered when the image was written (GORD, v8+).
	OrderedNodeKeys []string
	OrderedEdgeKeys []string

	// Key tuples declared composite when the image was written (GCMP, v8+).
	CompositeNodeKeys [][]string
	CompositeEdgeKeys [][]string
}

// checkIDSpace is the absolute half of checkIDCeiling, applied on its own
// before anything is sized from a file's identifiers.
//
// The order matters: the touched-page bitmap the relative rule needs is itself
// sized from the highest ID, so a file naming ID 2^62 would allocate a 512 GiB
// bitmap to find out that it is not allowed to name ID 2^62.
func checkIDSpace(kind string, maxID uint64) error {
	if maxID > maxCSREntityID {
		return fmt.Errorf("deserialiseCSR: %s ID %d exceeds the maximum addressable ID %d",
			kind, maxID, uint64(maxCSREntityID))
	}
	return nil
}

// checkIDCeiling bounds one entity kind both absolutely and against how many
// records of that kind the file actually carries.
//
// The relative bound is the one that matters for a small file. Records live in
// pages of csrPageSlots identifiers and a page costs a full page of slots
// whether one record falls in it or four thousand do, so the quantity to bound
// is the number of pages the file touches — not its highest ID, which after
// paging says nothing about what the file costs. Without the bound, a handful
// of records scattered one per page would each charge a whole page.
func checkIDCeiling(kind string, maxID uint64, records, touchedPages int) error {
	if err := checkIDSpace(kind, maxID); err != nil {
		return err
	}
	slots := uint64(touchedPages) * csrPageSlots
	allowed := uint64(records) * csrIDSparsityFactor
	if allowed < csrIDSparsityFloor {
		allowed = csrIDSparsityFloor
	}
	if slots > allowed {
		return fmt.Errorf("deserialiseCSR: %d %s records touch %d pages (%d slots), too sparse for the ceiling of %d",
			records, kind, touchedPages, slots, allowed)
	}
	return nil
}

// touchedPageSet counts the distinct identifier pages a file names, which is
// what Build's arenas are sized from.
//
// It is a bitmap rather than a map because it is built from untrusted input on
// the open path: at the maximum addressable ID it is 128 KiB and allocated
// once, where a map would be a per-page header and a hash insert for a file
// that may have nothing but scattered pages.
type touchedPageSet struct {
	bits  []uint64
	pages int
}

// newTouchedPageSet covers identifiers 0..maxID. maxID must already have passed
// checkIDSpace.
func newTouchedPageSet(maxID uint64) touchedPageSet {
	return touchedPageSet{bits: make([]uint64, (maxID>>csrPageBits)/64+1)}
}

// mark records the page holding id, counting it once.
func (t *touchedPageSet) mark(id uint64) {
	p := id >> csrPageBits
	w, bit := p/64, uint64(1)<<(p%64)
	if t.bits[w]&bit != 0 {
		return
	}
	t.bits[w] |= bit
	t.pages++
}

// checkCSREntityIDs rejects records whose IDs would make Build allocate memory
// the file gives no reason to believe in.
//
// Three rules, in order of strength:
//
//   - For v5+ files the header carries the sequence high-water marks. IDs are
//     handed out from those monotonic counters and Compact stamps the current
//     values, so every record's ID is <= its mark by construction. A record
//     above the mark means the file is inconsistent with itself. This is exact
//     and costs nothing.
//
//   - maxCSREntityID is a backstop, applied to every version. It exists because
//     the high-water marks live in the same header an attacker controls, so the
//     first rule alone still permits "seqHW = 2^62, one record with that ID".
//     It bounds the page directory: 4 MiB per kind at the ceiling, for a file
//     that names one identifier just below it.
//
//   - csrIDSparsityFactor bounds the *pages* the records touch against the
//     record count that is actually present, with csrIDSparsityFloor as the
//     minimum allowance — sixteen pages, which is more than any small file
//     needs. This is what stops a file of seventeen records placed one per page
//     from demanding seventeen pages of arena; the absolute ceiling alone is no
//     protection against that.
//
// The third rule is a *loose* bound on purpose, not an assertion that IDs track
// the record count. They do not: IDs are monotonic and never reused, so a
// long-lived store that has deleted heavily carries a maxID far above its live
// count and that file is perfectly valid. Under paging such a store is cheap
// anyway — a burned page costs one int32 — so the rule now rejects only the
// shape that is genuinely expensive, records spread thinly across many pages.
//
// The endpoint rule runs before the pages are counted, so that an edge naming a
// node the file does not contain cannot extend the identifier space the bitmap
// covers.
func checkCSREntityIDs(nodes []nodeRecord, edges []rawEdge, version uint16, nodeSeqHW, edgeSeqHW uint64) error {
	ns, es := slices.Values(nodes), slices.Values(edges)
	ext, err := csrExtentOf(ns, es)
	if err != nil {
		return err
	}
	_, err = csrShapeOf(ns, es, ext, len(nodes), len(edges), version, nodeSeqHW, edgeSeqHW)
	return err
}

// csrIDExtent is how far into the identifier space a record set reaches, and how
// many of its records will be placed.
//
// nodeExtent is separate from highestNode because an edge endpoint materialises
// a node page whether or not a node record falls in it, so the node directory
// has to cover the endpoints too.
type csrIDExtent struct {
	yielded              int // every node offered, invalid identifiers included
	wantNodes, wantEdges int
	highestNode          uint64
	highestEdge          uint64
	nodeExtent           uint64
}

// csrExtentOf walks both kinds once and reports how far they reach, refusing an
// identifier outside the addressable space as it goes.
//
// Refused per record rather than at the end, because the extent is what the page
// directories are then sized from: a single record carrying 0x3030303030303030
// would otherwise be found only after something had tried to allocate for it.
func csrExtentOf(nodes iter.Seq[nodeRecord], edges iter.Seq[rawEdge]) (csrIDExtent, error) {
	var ext csrIDExtent
	for n := range nodes {
		ext.yielded++
		if n.ID == store.InvalidNodeID {
			continue
		}
		if err := checkIDSpace("node", uint64(n.ID)); err != nil {
			return ext, err
		}
		ext.wantNodes++
		if id := uint64(n.ID); id > ext.highestNode {
			ext.highestNode = id
		}
	}
	ext.nodeExtent = ext.highestNode
	for e := range edges {
		if e.ID == store.InvalidEdgeID {
			continue
		}
		if err := checkIDSpace("edge", uint64(e.ID)); err != nil {
			return ext, err
		}
		if err := checkIDSpace("node", max(uint64(e.Src), uint64(e.Dst))); err != nil {
			return ext, err
		}
		ext.wantEdges++
		if id := uint64(e.ID); id > ext.highestEdge {
			ext.highestEdge = id
		}
		ext.nodeExtent = max(ext.nodeExtent, uint64(e.Src), uint64(e.Dst))
	}
	return ext, nil
}

// csrShapeOf marks the pages Build will materialise and refuses a record set
// whose identifiers would make it allocate memory the file gives no reason to
// believe in.
//
// It is both the bound and the shape, and that is deliberate: the pages have to
// be counted to apply the ceiling and they have to be marked to build the
// directory, and those were two walks over the same records doing the same
// arithmetic. The loader hands the result to buildSeqShaped, which is what takes
// an open from six passes over the mapping to three; checkCSREntityIDs throws it
// away, which is what it has always done with this work.
//
// The extent must come from csrExtentOf over the same sequences. It is a
// parameter rather than computed here because the image loader already knows it
// -- it tracks the maxima in the pass that fills its arenas -- and recomputing it
// would be a fourth walk over the mapping for numbers already in hand.
func csrShapeOf(nodes iter.Seq[nodeRecord], edges iter.Seq[rawEdge], ext csrIDExtent,
	nodeCount, edgeCount int, version uint16, nodeSeqHW, edgeSeqHW uint64) (*csrBuildShape, error) {
	shape := &csrBuildShape{
		nodeDir: newPageDir(ext.nodeExtent), edgeDir: newPageDir(ext.highestEdge),
		yielded: ext.yielded, wantNodes: ext.wantNodes, wantEdges: ext.wantEdges,
		highestNode: ext.highestNode, highestEdge: ext.highestEdge,
	}
	nodePages, edgePages := 0, 0
	mark := func(dir []int32, id uint64, count *int) {
		if dir[id>>csrPageBits] == csrDeadPage {
			*count++
		}
		touchPage(dir, id)
	}
	for n := range nodes {
		if n.ID != store.InvalidNodeID {
			mark(shape.nodeDir, uint64(n.ID), &nodePages)
		}
	}
	// Endpoints are a separate bound from identifiers, and the one that used to
	// crash. Build materialises the page of every endpoint whether or not a node
	// record falls in it, so an edge naming a node the file does not contain
	// costs a page rather than reading past the end of an array. That is bounded
	// -- the page rule below counts endpoint pages -- but keeping the check
	// confines materialisation to pages inside the identifier space the records
	// already describe. Every live edge has both endpoints present, because
	// deletion cascades to incident edges, so this rejects nothing valid.
	for e := range edges {
		if uint64(e.Src) > ext.highestNode {
			return nil, fmt.Errorf("deserialiseCSR: edge %d has source %d, beyond the highest node ID %d",
				e.ID, e.Src, ext.highestNode)
		}
		if uint64(e.Dst) > ext.highestNode {
			return nil, fmt.Errorf("deserialiseCSR: edge %d has target %d, beyond the highest node ID %d",
				e.ID, e.Dst, ext.highestNode)
		}
		if e.ID == store.InvalidEdgeID {
			continue
		}
		mark(shape.edgeDir, uint64(e.ID), &edgePages)
		mark(shape.nodeDir, uint64(e.Src), &nodePages)
		mark(shape.nodeDir, uint64(e.Dst), &nodePages)
	}

	if err := checkIDCeiling("node", ext.highestNode, nodeCount, nodePages); err != nil {
		return nil, err
	}
	if err := checkIDCeiling("edge", ext.highestEdge, edgeCount, edgePages); err != nil {
		return nil, err
	}

	// A zero mark means "not stamped" rather than "the highest ID is zero".
	// Compact always stamps the live counters, but a CSRGraph serialised straight
	// out of Build carries zeros, and those files are legitimate. Skipping the
	// comparison there costs nothing: checkIDSpace above still bounds the
	// allocation, so an unstamped file cannot name an unbounded one.
	if version >= csrVersionWithSeqHW {
		if nodeSeqHW > 0 && ext.highestNode > nodeSeqHW {
			return nil, fmt.Errorf("deserialiseCSR: node ID %d exceeds the file's own sequence high-water mark %d",
				ext.highestNode, nodeSeqHW)
		}
		if edgeSeqHW > 0 && ext.highestEdge > edgeSeqHW {
			return nil, fmt.Errorf("deserialiseCSR: edge ID %d exceeds the file's own sequence high-water mark %d",
				ext.highestEdge, edgeSeqHW)
		}
	}
	return shape, nil
}

// readMappedIndexSections parses a v9 image's index into a base, reporting
// whether the file carries one at all.
//
// Half of one is a hard failure rather than a degraded read, for the reason
// newGPIXBase gives: the two directions answer different questions and a reader
// holding one would answer some queries and silently miss others. The ok result
// is therefore "this file is one of those images", not "this file gave us
// something usable" — a file carrying GPIR alone is still a v9 image, and it is
// refused as a damaged one instead of being read as a v8 image whose index went
// missing.
func readMappedIndexSections(data []byte, sections []csrSection) (index.Base, bool, error) {
	fwd, hasFwd := findSection(sections, csrSectionMappedIndex)
	rev, hasRev := findSection(sections, csrSectionMappedReverse)
	if !hasFwd && !hasRev {
		return nil, false, nil
	}
	var (
		fwdSec *gpixSection
		revSec *gpirSection
		err    error
	)
	if hasFwd {
		if fwdSec, err = parseGPIX(data[fwd.Offset : fwd.Offset+fwd.Length]); err != nil {
			return nil, true, fmt.Errorf("deserialiseCSR: %w", err)
		}
	}
	if hasRev {
		if revSec, err = parseGPIR(data[rev.Offset : rev.Offset+rev.Length]); err != nil {
			return nil, true, fmt.Errorf("deserialiseCSR: %w", err)
		}
	}
	// The composite postings, if the image carries them. Absent is the normal
	// case and not a degradation: every v9 image written before GCPX existed is
	// absent here, and index.fillCompositesFromBase fills the composites from
	// the entries exactly as it always did.
	//
	// Malformed, though, is a refusal and not a fallback. The section being
	// optional means an older reader may skip a magic it does not know, which is
	// what checkCriticalSections does for it; it does not mean this reader may
	// read a directory that will not bound and carry on. A file whose GCPX does
	// not parse is a damaged file, the fill would answer correctly out of the
	// same bytes that produced the damage, and quietly taking the slow path
	// would turn a diagnosis into a performance mystery. This is newGPIXBase's
	// position for the same reason.
	var cmpSec *gcpxSection
	if cmp, hasCmp := findSection(sections, csrSectionCompositeIndex); hasCmp {
		if cmpSec, err = parseGCPX(data[cmp.Offset : cmp.Offset+cmp.Length]); err != nil {
			return nil, true, fmt.Errorf("deserialiseCSR: %w", err)
		}
	}
	b, err := newGPIXBase(fwdSec, revSec, cmpSec)
	if err != nil {
		return nil, true, fmt.Errorf("deserialiseCSR: %w", err)
	}
	return b, true, nil
}

// readImageIndexBase parses an image's mapped index sections and nothing else.
//
// deserialiseCSRFrom answers the same question on the way to parsing a whole
// graph. This is the question on its own, for the one caller that has an image it
// wants the index out of and a graph it already has: a compaction, installing the
// index it has just written over the one it wrote it from. Parsing the records
// again there would cost the compaction an Open to throw the result away.
//
// The digest is not checked and neither are the record sections, deliberately.
// This reads a file this process wrote and fsynced moments ago, against bytes it
// still holds; a mismatch would mean the storage changed them between the write
// and the read, which is a fault no reader here could act on and which the next
// open's VerifyOnOpen is the place to catch. What is checked is everything the
// parse itself depends on -- the magic, a version that has a section directory,
// and every bound readCSRSectionDirectory and parseGPIX apply -- because those
// are what stop a damaged directory being read as a structure.
//
// The bool is "this image carries a mapped index", which is not the same as
// "this succeeded": a v8 image and a v9 image whose index went missing are
// different answers, and only the second is an error.
func readImageIndexBase(data []byte) (index.Base, bool, error) {
	if len(data) < csrV8HeaderSize {
		return nil, false, fmt.Errorf("readImageIndexBase: %d bytes cannot hold a v8 header", len(data))
	}
	if string(data[0:4]) != "GCSR" {
		return nil, false, fmt.Errorf("readImageIndexBase: invalid magic")
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version < csrVersionSectioned {
		// No section directory, so no GPIX by construction. Not an error: it is
		// what every image before v8 looks like.
		return nil, false, nil
	}
	if version > csrVersionMax {
		return nil, false, fmt.Errorf("readImageIndexBase: unsupported version %d (supported: %d-%d)",
			version, csrVersionV2, csrVersionMax)
	}
	sections, err := readCSRSectionDirectory(data, binary.LittleEndian.Uint64(data[62:70]))
	if err != nil {
		return nil, false, fmt.Errorf("readImageIndexBase: %w", err)
	}
	return readMappedIndexSections(data, sections)
}

// readCSRIndexSection parses the property-index section at the given offset.
func readCSRIndexSection(data []byte, offset int) (*csrIndexSection, error) {
	if offset <= 0 || offset > len(data) {
		return nil, fmt.Errorf("readCSRIndexSection: index offset %d out of range (file is %d bytes)", offset, len(data))
	}
	pos := offset
	if pos+csrIndexSectionMagicSize > len(data) {
		return nil, fmt.Errorf("readCSRIndexSection: truncated index magic")
	}
	if string(data[pos:pos+csrIndexSectionMagicSize]) != csrIndexSectionMagic {
		return nil, fmt.Errorf("readCSRIndexSection: bad index magic %q", data[pos:pos+csrIndexSectionMagicSize])
	}
	pos += csrIndexSectionMagicSize

	readCount := func(what string) (int, error) {
		if pos+8 > len(data) {
			return 0, fmt.Errorf("readCSRIndexSection: truncated %s count", what)
		}
		n := int(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		if n < 0 {
			return 0, fmt.Errorf("readCSRIndexSection: negative %s count", what)
		}
		return n, nil
	}
	readEntry := func(what string, i int) (uint64, string, []byte, error) {
		if pos+14 > len(data) {
			return 0, "", nil, fmt.Errorf("readCSRIndexSection: truncated %s entry %d", what, i)
		}
		id := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		keyLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+keyLen+4 > len(data) {
			return 0, "", nil, fmt.Errorf("readCSRIndexSection: truncated %s key %d", what, i)
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen
		valLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+valLen > len(data) {
			return 0, "", nil, fmt.Errorf("readCSRIndexSection: truncated %s value %d", what, i)
		}
		val := make([]byte, valLen)
		copy(val, data[pos:pos+valLen])
		pos += valLen
		return id, key, val, nil
	}

	section := &csrIndexSection{}

	nodeCount, err := readCount("node property")
	if err != nil {
		return nil, err
	}
	// Same reasoning as the record counts in deserialiseCSR: this is a
	// length-prefix from the file, so it is bounded against what the remaining
	// bytes could actually encode before it is used to size an allocation.
	if nodeCount < 0 || nodeCount > (len(data)-pos)/minPropEntryBytes {
		return nil, fmt.Errorf("readCSRIndexSection: node property count %d exceeds what %d remaining bytes can hold",
			nodeCount, len(data)-pos)
	}
	section.NodeProps = make([]index.NodePropEntry, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id, key, val, err := readEntry("node property", i)
		if err != nil {
			return nil, err
		}
		section.NodeProps = append(section.NodeProps, index.NodePropEntry{ID: store.NodeID(id), Key: key, Value: val})
	}

	edgeCount, err := readCount("edge property")
	if err != nil {
		return nil, err
	}
	if edgeCount < 0 || edgeCount > (len(data)-pos)/minPropEntryBytes {
		return nil, fmt.Errorf("readCSRIndexSection: edge property count %d exceeds what %d remaining bytes can hold",
			edgeCount, len(data)-pos)
	}
	section.EdgeProps = make([]index.EdgePropEntry, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		id, key, val, err := readEntry("edge property", i)
		if err != nil {
			return nil, err
		}
		section.EdgeProps = append(section.EdgeProps, index.EdgePropEntry{ID: store.EdgeID(id), Key: key, Value: val})
	}

	return section, nil
}
