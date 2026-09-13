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
	"math"
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
	csr, section, err := deserialiseCSRFrom(src.data, src.mapped())
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
		// load. What the declarations above did do is the only reading of the base
		// this open performs — the composites' backfill — and a fault recorded
		// there is a run that would not decode, which is damage to the file. The
		// alternative to refusing is a store that comes up serving a composite
		// index missing whatever the damaged run held.
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
	return deserialiseCSRFrom(data, false)
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
func deserialiseCSRFrom(data []byte, mapped bool) (*CSRGraph, *csrIndexSection, error) {
	if len(data) < 22 {
		return nil, nil, fmt.Errorf("deserialiseCSR: data too short")
	}
	if string(data[0:4]) != "GCSR" {
		return nil, nil, fmt.Errorf("deserialiseCSR: invalid magic")
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version < csrVersionV2 || version > csrVersionMax {
		return nil, nil, fmt.Errorf("deserialiseCSR: unsupported version %d (supported: %d-%d)",
			version, csrVersionV2, csrVersionMax)
	}
	nodeCount := int(binary.LittleEndian.Uint64(data[6:14]))
	edgeCount := int(binary.LittleEndian.Uint64(data[14:22]))
	pos := 22

	// Sequence high-water marks (version 5+).
	var nodeSeqHW, edgeSeqHW uint64
	if version >= csrVersionWithSeqHW {
		if len(data) < csrV5HeaderSize {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated sequence high-water header")
		}
		nodeSeqHW = binary.LittleEndian.Uint64(data[22:30])
		edgeSeqHW = binary.LittleEndian.Uint64(data[30:38])
		pos = csrV5HeaderSize
	}

	// Property-index section offset (version 6+).
	var indexOffset uint64
	if version >= csrVersionWithPropIndex {
		if len(data) < csrV6HeaderSize {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated index-offset header")
		}
		indexOffset = binary.LittleEndian.Uint64(data[38:46])
		pos = csrV6HeaderSize
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
		pos = csrV8HeaderSize

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

	// Both counts come straight off the header, so a corrupt or hostile file
	// controls them completely. Allocating from them unchecked lets a 46-byte
	// file demand terabytes: nodeCount is a uint64 narrowed to int, so a large
	// value allocates until the process dies and a value above MaxInt64 goes
	// negative and panics in makeslice before any record is read.
	//
	// The bound is the cheapest sound one: every node record occupies at least
	// minNodeRecordBytes on disk, so a file cannot hold more than its own
	// remaining length divided by that. This rejects the hostile case without
	// constraining any legitimate one.
	if nodeCount < 0 || nodeCount > (len(data)-pos)/minNodeRecordBytes {
		return nil, nil, fmt.Errorf("deserialiseCSR: node count %d exceeds what %d remaining bytes can hold",
			nodeCount, len(data)-pos)
	}
	nodes := make([]nodeRecord, nodeCount)

	// Labels and property blobs are allocated once each for the whole image
	// rather than once per record, and every record's slice is a sub-slice of
	// the arena. The file already holds them as one packed byte stream; the old
	// loop unpacked that stream into two heap objects per record and handed the
	// collector 600 000 of them on a 300 000-record image, live for the life of
	// the store.
	//
	// Every sub-slice is taken with the three-index form, so cap == len and an
	// append by a caller reallocates instead of writing over the next record's
	// bytes. That is what keeps the existing csrBytes aliasing contract true
	// under a shared backing array; without it this would be a silent
	// cross-record corruption rather than an optimisation.
	//
	// Spans are recorded during the parse and resolved to slices afterwards,
	// because appending to the arena may move it and would leave any slice taken
	// mid-parse pointing at a stale array.
	nodeLabelArena := make([]store.NodeType, 0, nodeCount)
	nodeLabelSpan := make([][2]uint32, nodeCount)

	// Neither the property arena nor its span array is allocated under a
	// mapping: the blob is addressed where it lies and the record's slice is
	// taken in the loop below. See deserialiseCSRFrom.
	var nodePropArena []byte
	var nodePropSpan [][2]uint32
	if !mapped {
		nodePropArena = make([]byte, 0, len(data)/8)
		nodePropSpan = make([][2]uint32, nodeCount)
	}

	for i := range nodes {
		if pos+9 > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated node record %d", i)
		}
		nid := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		labelCount := int(data[pos])
		pos++
		labelBytes := labelCount
		if version >= csrVersionWithU16Labels {
			labelBytes = labelCount * currentLabelBytesPerValue
		}
		// Bytes still required after the labels: the property field. v2 reserved
		// 8 bytes for it; v3+ writes a 4-byte length followed by the blob.
		//
		// This used to demand 8 unconditionally, which over-reads by 4 on v3+.
		// It never fired because the file always carried trailing adjacency
		// arrays that supplied slack — arrays the reader never actually read. The
		// moment those stopped being written, a perfectly valid file ending at
		// its last record started being rejected.
		nodeTail := nodePayloadPropLenBytes
		if version == csrVersionV2 {
			nodeTail = 8
		}
		if pos+labelBytes+nodeTail > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated node labels %d", i)
		}
		labelStart := uint32(len(nodeLabelArena))
		for j := 0; j < labelCount; j++ {
			if version >= csrVersionWithU16Labels {
				nodeLabelArena = append(nodeLabelArena, store.NodeType(binary.LittleEndian.Uint16(data[pos:])))
				pos += currentLabelBytesPerValue
			} else {
				nodeLabelArena = append(nodeLabelArena, store.NodeType(data[pos]))
				pos++
			}
		}
		nodeLabelSpan[i] = [2]uint32{labelStart, uint32(labelCount)}

		nodes[i] = nodeRecord{ID: nid}
		if mapped {
			props, nextPos, err := aliasCSRProperties(data, pos, version, "node", i)
			if err != nil {
				return nil, nil, err
			}
			nodes[i].Properties = props
			pos = nextPos
		} else {
			arena, propStart, propLen, nextPos, err := readCSRPropertiesInto(nodePropArena, data, pos, version, "node", i)
			if err != nil {
				return nil, nil, err
			}
			nodePropArena = arena
			nodePropSpan[i] = [2]uint32{propStart, propLen}
			pos = nextPos
		}
	}

	for i := range nodes {
		nodes[i].Labels = arenaLabels(nodeLabelArena, nodeLabelSpan[i])
		if !mapped {
			nodes[i].Properties = arenaBytes(nodePropArena, nodePropSpan[i])
		}
	}

	if edgeCount < 0 || edgeCount > (len(data)-pos)/minEdgeRecordBytes {
		return nil, nil, fmt.Errorf("deserialiseCSR: edge count %d exceeds what %d remaining bytes can hold",
			edgeCount, len(data)-pos)
	}
	edges := make([]rawEdge, edgeCount)

	// Same arena treatment as the node records above, and for the same reason --
	// including the mapped case, where there is no property arena at all.
	edgeLabelArena := make([]store.EdgeType, 0, edgeCount)
	edgeLabelSpan := make([][2]uint32, edgeCount)

	var edgePropArena []byte
	var edgePropSpan [][2]uint32
	if !mapped {
		edgePropArena = make([]byte, 0, len(data)/8)
		edgePropSpan = make([][2]uint32, edgeCount)
	}

	for i := range edges {
		if pos+25 > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated edge record %d", i)
		}
		eid := store.EdgeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		src := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		dst := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		labelCount := int(data[pos])
		pos++
		labelBytes := labelCount
		if version >= csrVersionWithU16Labels {
			labelBytes = labelCount * currentLabelBytesPerValue
		}
		// weight(4) + the property field, which is 8 on v2 and 4 on v3+.
		// Same over-strict constant as the node case above.
		edgeTail := 4 + nodePayloadPropLenBytes
		if version == csrVersionV2 {
			edgeTail = 4 + 8
		}
		if pos+labelBytes+edgeTail > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated edge labels %d", i)
		}
		labelStart := uint32(len(edgeLabelArena))
		for j := 0; j < labelCount; j++ {
			if version >= csrVersionWithU16Labels {
				edgeLabelArena = append(edgeLabelArena, store.EdgeType(binary.LittleEndian.Uint16(data[pos:])))
				pos += currentLabelBytesPerValue
			} else {
				edgeLabelArena = append(edgeLabelArena, store.EdgeType(data[pos]))
				pos++
			}
		}
		edgeLabelSpan[i] = [2]uint32{labelStart, uint32(labelCount)}
		weight := math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		edges[i] = rawEdge{ID: eid, Src: src, Dst: dst, Weight: weight}
		if mapped {
			props, nextPos, err := aliasCSRProperties(data, pos, version, "edge", i)
			if err != nil {
				return nil, nil, err
			}
			edges[i].Properties = props
			pos = nextPos
		} else {
			arena, propStart, propLen, nextPos, err := readCSRPropertiesInto(edgePropArena, data, pos, version, "edge", i)
			if err != nil {
				return nil, nil, err
			}
			edgePropArena = arena
			edgePropSpan[i] = [2]uint32{propStart, propLen}
			pos = nextPos
		}
	}

	for i := range edges {
		edges[i].Labels = arenaLabels(edgeLabelArena, edgeLabelSpan[i])
		if !mapped {
			edges[i].Properties = arenaBytes(edgePropArena, edgePropSpan[i])
		}
	}

	// Build allocates one int32 per page of the identifier space up to the
	// highest ID named, and a full page of records for every page a record or
	// an endpoint falls in. Bounding the record counts above is therefore not
	// enough: two records carrying IDs of 0x3030303030303030 would make a
	// 105-byte file demand an exabyte-scale directory, which panics in makeslice
	// rather than returning an error. Validate the IDs before Build sees them.
	if err := checkCSREntityIDs(nodes, edges, version, nodeSeqHW, edgeSeqHW); err != nil {
		return nil, nil, err
	}

	csr, err := Build(nodes, edges)
	if err != nil {
		return nil, nil, err
	}
	if mapped {
		csr.imgBytes = int64(len(data))
	}
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
	var maxNID, maxEID uint64
	for i := range nodes {
		if uint64(nodes[i].ID) > maxNID {
			maxNID = uint64(nodes[i].ID)
		}
	}
	for i := range edges {
		if uint64(edges[i].ID) > maxEID {
			maxEID = uint64(edges[i].ID)
		}
	}

	if err := checkIDSpace("node", maxNID); err != nil {
		return err
	}
	if err := checkIDSpace("edge", maxEID); err != nil {
		return err
	}

	// Endpoints are a separate bound from IDs, and the one that used to crash.
	// Build materialises the page of every endpoint whether or not a node record
	// falls in it, so an edge naming a node the file does not contain costs a
	// page rather than reading past the end of an array. That is bounded — the
	// page rule below counts endpoint pages — but keeping the check confines
	// materialisation to pages inside the identifier space the records already
	// describe. Every live edge has both endpoints present — deletion cascades to
	// incident edges — so this rejects nothing valid.
	for i := range edges {
		if uint64(edges[i].Src) > maxNID {
			return fmt.Errorf("deserialiseCSR: edge %d has source %d, beyond the highest node ID %d",
				edges[i].ID, edges[i].Src, maxNID)
		}
		if uint64(edges[i].Dst) > maxNID {
			return fmt.Errorf("deserialiseCSR: edge %d has target %d, beyond the highest node ID %d",
				edges[i].ID, edges[i].Dst, maxNID)
		}
	}

	// Count exactly what Build will materialise: the page of every record with a
	// usable identifier, plus the pages of both endpoints of every such edge.
	// Records carrying the invalid zero identifier are skipped there, so they are
	// skipped here.
	nodePages := newTouchedPageSet(maxNID)
	edgePages := newTouchedPageSet(maxEID)
	for i := range nodes {
		if nodes[i].ID != store.InvalidNodeID {
			nodePages.mark(uint64(nodes[i].ID))
		}
	}
	for i := range edges {
		if edges[i].ID == store.InvalidEdgeID {
			continue
		}
		edgePages.mark(uint64(edges[i].ID))
		nodePages.mark(uint64(edges[i].Src))
		nodePages.mark(uint64(edges[i].Dst))
	}

	if err := checkIDCeiling("node", maxNID, len(nodes), nodePages.pages); err != nil {
		return err
	}
	if err := checkIDCeiling("edge", maxEID, len(edges), edgePages.pages); err != nil {
		return err
	}

	// A zero mark means "not stamped" rather than "the highest ID is zero".
	// Compact always stamps the live counters, but a CSRGraph serialised straight
	// out of Build carries zeros, and those files are legitimate. Skipping the
	// comparison there costs nothing: maxCSREntityID above still bounds the
	// allocation, so an unstamped file cannot name an unbounded one.
	if version >= csrVersionWithSeqHW {
		if nodeSeqHW > 0 && maxNID > nodeSeqHW {
			return fmt.Errorf("deserialiseCSR: node ID %d exceeds the file's own sequence high-water mark %d", maxNID, nodeSeqHW)
		}
		if edgeSeqHW > 0 && maxEID > edgeSeqHW {
			return fmt.Errorf("deserialiseCSR: edge ID %d exceeds the file's own sequence high-water mark %d", maxEID, edgeSeqHW)
		}
	}
	return nil
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
	b, err := newGPIXBase(fwdSec, revSec)
	if err != nil {
		return nil, true, fmt.Errorf("deserialiseCSR: %w", err)
	}
	return b, true, nil
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

// arenaLabels resolves a recorded span to a sub-slice of the label arena. The
// three-index form is load-bearing: it makes cap == len so that a caller
// appending to a record's Labels reallocates rather than overwriting the next
// record's labels in the shared array.
func arenaLabels[T store.NodeType | store.EdgeType](arena []T, span [2]uint32) []T {
	if span[1] == 0 {
		return nil
	}
	lo, hi := span[0], span[0]+span[1]
	return arena[lo:hi:hi]
}

// arenaBytes is arenaLabels for the property arena, under the same contract and
// for the same reason. A zero-length blob resolves to nil rather than to an
// empty sub-slice, because both stores normalise an empty blob to nil on the way
// in and a reader must not be able to tell the two apart.
func arenaBytes(arena []byte, span [2]uint32) []byte {
	if span[1] == 0 {
		return nil
	}
	lo, hi := span[0], span[0]+span[1]
	return arena[lo:hi:hi]
}

// readCSRPropertiesInto is readCSRProperties writing through an arena: it
// appends the blob and reports where it landed, instead of allocating one slice
// per record. It returns the (possibly reallocated) arena, which the caller must
// store back.
func readCSRPropertiesInto(arena []byte, data []byte, pos int, version uint16, kind string, index int) ([]byte, uint32, uint32, int, error) {
	if version == 2 {
		if pos+8 > len(data) {
			return arena, 0, 0, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
		}
		return arena, 0, 0, pos + 8, nil
	}
	if pos+4 > len(data) {
		return arena, 0, 0, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
	}
	propLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if pos+propLen > len(data) {
		return arena, 0, 0, pos, fmt.Errorf("deserialiseCSR: truncated %s property blob %d", kind, index)
	}
	if propLen == 0 {
		return arena, 0, 0, pos, nil
	}
	off := uint32(len(arena))
	arena = append(arena, data[pos:pos+propLen]...)
	return arena, off, uint32(propLen), pos + propLen, nil
}

// aliasCSRProperties is readCSRProperties without the copy: the returned slice
// addresses data.
//
// The three-index form is as load-bearing here as it is in arenaBytes, and for a
// larger reason. cap == len makes a caller appending to a record's Properties
// reallocate rather than write over the next record's bytes -- and under a
// mapping those bytes are a read-only file, so the write would not corrupt the
// next record, it would fault. Either way the append must not be allowed to
// reach them, and this is what stops it.
//
// A zero-length blob resolves to nil rather than to an empty sub-slice, so that
// a mapped read and a copied read are indistinguishable: both stores normalise
// an empty blob to nil on the way in.
func aliasCSRProperties(data []byte, pos int, version uint16, kind string, index int) ([]byte, int, error) {
	if version == csrVersionV2 {
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
		}
		return nil, pos + 8, nil
	}
	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
	}
	propLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if pos+propLen > len(data) {
		return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s property blob %d", kind, index)
	}
	if propLen == 0 {
		return nil, pos, nil
	}
	end := pos + propLen
	return data[pos:end:end], end, nil
}

func readCSRProperties(data []byte, pos int, version uint16, kind string, index int) ([]byte, int, error) {
	if version == 2 {
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
		}
		return nil, pos + 8, nil
	}
	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
	}
	propLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if pos+propLen > len(data) {
		return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s property blob %d", kind, index)
	}
	if propLen == 0 {
		return nil, pos, nil
	}
	props := make([]byte, propLen)
	copy(props, data[pos:pos+propLen])
	return props, pos + propLen, nil
}
