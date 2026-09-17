package disk

// Loading an image without holding every record twice.
//
// # What was held
//
// deserialiseCSRFrom parsed the record stream into a []nodeRecord and a
// []rawEdge and handed those to buildSeq, which placed each record into the
// arena the graph keeps. So for the length of the load there were two copies of
// every record alive: the parse's slice and the arena it was copied into. At two
// million 3.2 KB records that is 112 MB of parse slice against 107 MB of arena,
// and an inuse profile taken at the end of the open shows only the second,
// because by then the first is garbage. It is visible in the peak and nowhere
// else, which is why it survived this long.
//
// buildSeq already takes sequences rather than slices, and its own comment says
// why: "The one caller that does not already hold a slice is compaction."
// deserialiseCSR reached it through Build, with a slice, because it had one.
// This is the observation that the loader should not have had one either.
//
// # Why the parse can simply be run again
//
// buildSeq walks each sequence three times -- the page directory is sized from
// the highest identifier, the arena from the touched pages, and the placement is
// a third pass -- and requires that the sequence yield the same records every
// time. A walk over the image's record stream is a pure function of the bytes
// and the format version, so it satisfies that by construction rather than by
// care.
//
// What it must not do is allocate per pass, or the cure is the disease. Two
// things are parsed once, before any walk, into arenas the walks share: the
// labels always, and the property blobs when the image is not mapped. Both are
// filled in record order and every walk visits records in that same order, so a
// walk needs no span table to find a record's slice -- it carries a running
// cursor into each arena and takes the next run. That is the whole trick, and it
// is what makes the streaming form cost one extra table of nothing rather than
// the 8 bytes a record a span array would.
//
// # What it costs
//
// Three passes over the record headers instead of one. A pass skips each blob by
// its length rather than reading it, so the bytes touched are the record headers
// and the labels -- and those are pages the open faults in anyway. The wall clock
// is measured in docs/MEMORY_MODEL.md; this is memory bought with sequential
// reads over a mapping that is already resident, which is the trade this whole
// programme makes everywhere else.

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math"

	"github.com/aoiflux/graphene/store"
)

// csrRecordRegion says where an image's record stream starts and how much of it
// there is.
//
// It is the one place that answers "where do the records begin", and
// deserialiseCSRFrom takes its answer rather than computing the same thing
// beside it: a walk that started one byte from where the loader thought it did
// would produce a graph of garbage, and the failure would look like a corrupt
// file rather than like two pieces of code disagreeing.
//
// The version-dependent fields between the counts and the records -- the
// sequence high-water marks, the index offset, the v8 trailer -- are read at
// fixed offsets by the loader and are not needed to find the records, so they
// stay where they are read.
func csrRecordRegion(data []byte) (pos int, version uint16, nodeCount, edgeCount int, err error) {
	if len(data) < 22 {
		return 0, 0, 0, 0, fmt.Errorf("deserialiseCSR: data too short")
	}
	if string(data[0:4]) != "GCSR" {
		return 0, 0, 0, 0, fmt.Errorf("deserialiseCSR: invalid magic")
	}
	version = binary.LittleEndian.Uint16(data[4:6])
	if version < csrVersionV2 || version > csrVersionMax {
		return 0, 0, 0, 0, fmt.Errorf("deserialiseCSR: unsupported version %d (supported: %d-%d)",
			version, csrVersionV2, csrVersionMax)
	}
	nodeCount = int(binary.LittleEndian.Uint64(data[6:14]))
	edgeCount = int(binary.LittleEndian.Uint64(data[14:22]))

	pos = 22
	switch {
	case version >= csrVersionSectioned:
		pos = csrV8HeaderSize
	case version >= csrVersionWithPropIndex:
		pos = csrV6HeaderSize
	case version >= csrVersionWithSeqHW:
		pos = csrV5HeaderSize
	}
	if len(data) < pos {
		return 0, 0, 0, 0, fmt.Errorf("deserialiseCSR: truncated v%d header: %d bytes, need %d",
			version, len(data), pos)
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
	// constraining any legitimate one. The edge count is bounded the same way in
	// fill, against the bytes left after the nodes.
	if nodeCount < 0 || nodeCount > (len(data)-pos)/minNodeRecordBytes {
		return 0, 0, 0, 0, fmt.Errorf("deserialiseCSR: node count %d exceeds what %d remaining bytes can hold",
			nodeCount, len(data)-pos)
	}
	return pos, version, nodeCount, edgeCount, nil
}

// csrRecordSourceOf builds the source for an image and fills its arenas.
func csrRecordSourceOf(data []byte, mapped bool) (*csrRecordSource, error) {
	pos, version, nodeCount, edgeCount, err := csrRecordRegion(data)
	if err != nil {
		return nil, err
	}
	src := &csrRecordSource{
		data: data, version: version, mapped: mapped,
		nodeStart: pos, nodeCount: nodeCount, edgeCount: edgeCount,
	}
	if _, err := src.fill(); err != nil {
		return nil, err
	}
	return src, nil
}

// csrRecordSpan is one record located within the image, by offset.
//
// Offsets rather than slices, so that locating a record allocates nothing and
// the same decode serves both the arena fill and the walks that follow it. The
// edge fields are unset for a node.
type csrRecordSpan struct {
	id, src, dst uint64
	weight       float32
	labelAt      int // byte offset of the first label
	labelCount   int
	propAt       int // byte offset of the blob; propLen == 0 means there is none
	propLen      int
	next         int // byte offset of the record after this one
}

// parseCSRNodeRecord locates node record index within data.
//
// The bounds are the ones the single-pass loader made, in the same order and
// with the same messages: a file that was rejected before is rejected here, and
// with the same text.
func parseCSRNodeRecord(data []byte, pos int, version uint16, index int) (csrRecordSpan, error) {
	var s csrRecordSpan
	if pos+9 > len(data) {
		return s, fmt.Errorf("deserialiseCSR: truncated node record %d", index)
	}
	s.id = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	s.labelCount = int(data[pos])
	pos++

	labelBytes := s.labelCount
	if version >= csrVersionWithU16Labels {
		labelBytes = s.labelCount * currentLabelBytesPerValue
	}
	// Bytes still required after the labels: the property field. v2 reserved 8
	// for it; v3+ writes a 4-byte length followed by the blob.
	nodeTail := nodePayloadPropLenBytes
	if version == csrVersionV2 {
		nodeTail = 8
	}
	if pos+labelBytes+nodeTail > len(data) {
		return s, fmt.Errorf("deserialiseCSR: truncated node labels %d", index)
	}
	s.labelAt = pos
	pos += labelBytes

	var err error
	if s.propAt, s.propLen, s.next, err = csrPropertySpan(data, pos, version, "node", index); err != nil {
		return s, err
	}
	return s, nil
}

// parseCSREdgeRecord is parseCSRNodeRecord for an edge, whose header carries two
// endpoints and whose weight sits between the labels and the blob.
func parseCSREdgeRecord(data []byte, pos int, version uint16, index int) (csrRecordSpan, error) {
	var s csrRecordSpan
	if pos+25 > len(data) {
		return s, fmt.Errorf("deserialiseCSR: truncated edge record %d", index)
	}
	s.id = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	s.src = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	s.dst = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	s.labelCount = int(data[pos])
	pos++

	labelBytes := s.labelCount
	if version >= csrVersionWithU16Labels {
		labelBytes = s.labelCount * currentLabelBytesPerValue
	}
	edgeTail := 4 + nodePayloadPropLenBytes
	if version == csrVersionV2 {
		edgeTail = 4 + 8
	}
	if pos+labelBytes+edgeTail > len(data) {
		return s, fmt.Errorf("deserialiseCSR: truncated edge labels %d", index)
	}
	s.labelAt = pos
	pos += labelBytes

	s.weight = math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	var err error
	if s.propAt, s.propLen, s.next, err = csrPropertySpan(data, pos, version, "edge", index); err != nil {
		return s, err
	}
	return s, nil
}

// csrPropertySpan locates a record's property blob without reading it.
//
// It is aliasCSRProperties expressed as offsets, and the two must agree: the
// blob a walk hands out is data[propAt:propAt+propLen], which is exactly what
// aliasCSRProperties returns for the same position.
func csrPropertySpan(data []byte, pos int, version uint16, kind string, index int) (at, n, next int, err error) {
	if version == csrVersionV2 {
		if pos+8 > len(data) {
			return 0, 0, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
		}
		return 0, 0, pos + 8, nil
	}
	if pos+4 > len(data) {
		return 0, 0, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
	}
	propLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if propLen < 0 || pos+propLen > len(data) {
		return 0, 0, pos, fmt.Errorf("deserialiseCSR: truncated %s property blob %d", kind, index)
	}
	return pos, propLen, pos + propLen, nil
}

// csrRecordSource is the repeatable walk over an image's two record regions.
//
// One value covers both kinds rather than one each, because the walks have to
// agree about where the edge region starts and that offset is only known once the
// node region has been walked. Splitting it would mean handing that offset from
// one object to another and trusting both to have used the same rules to find it.
//
// It also owns the error a walk could not report: an iter.Seq has nowhere to put
// one, so a failure mid-walk latches here and the loader checks it after buildSeq
// returns. The only failures reachable there are ones the fill pass already
// walked past without complaint, so in practice this is a belt on a brace -- but
// a sequence that silently yielded fewer records than the first pass did would
// trip buildSeq's own errUnstableBuild rather than produce a short graph, and
// either way the load fails loudly rather than quietly.
type csrRecordSource struct {
	data    []byte
	version uint16
	mapped  bool

	nodeStart, nodeCount int
	edgeStart, edgeCount int

	// The arenas every walk reads from, filled once each by the pass that also
	// finds edgeStart. Under a mapping the two property arenas stay nil: a blob
	// is addressed where it lies, which is the saving deserialiseCSRFrom exists
	// to describe.
	nodeLabels []store.NodeType
	edgeLabels []store.EdgeType
	nodeProps  []byte
	edgeProps  []byte

	// ext is how far the records reach into the identifier space, tracked by the
	// fill rather than by a walk of its own. It is free there -- the fill visits
	// every record already -- and it is what lets csrShapeOf skip the pass that
	// would otherwise recompute it over the mapping.
	ext csrIDExtent

	err error
}

// fill walks both record regions once, filling the arenas and locating the edge
// region and whatever follows it.
//
// The edge count is bounded against the bytes actually remaining after the nodes
// rather than against the whole file, which is the check the single-pass loader
// made at the same point and for the same reason: both counts come off the header
// and a hostile file controls them completely.
func (s *csrRecordSource) fill() (end int, err error) {
	s.nodeLabels = make([]store.NodeType, 0, s.nodeCount)
	if !s.mapped {
		s.nodeProps = make([]byte, 0, len(s.data)/8)
	}
	pos := s.nodeStart
	for i := range s.nodeCount {
		span, err := parseCSRNodeRecord(s.data, pos, s.version, i)
		if err != nil {
			return pos, err
		}
		s.nodeLabels = appendLabelRun(s.nodeLabels, s.data, s.version, span, store.NodeType(0))
		if !s.mapped && span.propLen > 0 {
			s.nodeProps = append(s.nodeProps, s.data[span.propAt:span.propAt+span.propLen]...)
		}
		s.ext.yielded++
		if span.id != uint64(store.InvalidNodeID) {
			if err := checkIDSpace("node", span.id); err != nil {
				return pos, err
			}
			s.ext.wantNodes++
			s.ext.highestNode = max(s.ext.highestNode, span.id)
		}
		pos = span.next
	}
	s.ext.nodeExtent = s.ext.highestNode

	s.edgeStart = pos
	if s.edgeCount < 0 || s.edgeCount > (len(s.data)-pos)/minEdgeRecordBytes {
		return pos, fmt.Errorf("deserialiseCSR: edge count %d exceeds what %d remaining bytes can hold",
			s.edgeCount, len(s.data)-pos)
	}
	s.edgeLabels = make([]store.EdgeType, 0, s.edgeCount)
	if !s.mapped {
		s.edgeProps = make([]byte, 0, len(s.data)/8)
	}
	for i := range s.edgeCount {
		span, err := parseCSREdgeRecord(s.data, pos, s.version, i)
		if err != nil {
			return pos, err
		}
		s.edgeLabels = appendLabelRun(s.edgeLabels, s.data, s.version, span, store.EdgeType(0))
		if !s.mapped && span.propLen > 0 {
			s.edgeProps = append(s.edgeProps, s.data[span.propAt:span.propAt+span.propLen]...)
		}
		if span.id != uint64(store.InvalidEdgeID) {
			if err := checkIDSpace("edge", span.id); err != nil {
				return pos, err
			}
			if err := checkIDSpace("node", max(span.src, span.dst)); err != nil {
				return pos, err
			}
			s.ext.wantEdges++
			s.ext.highestEdge = max(s.ext.highestEdge, span.id)
			s.ext.nodeExtent = max(s.ext.nodeExtent, span.src, span.dst)
		}
		pos = span.next
	}
	return pos, nil
}

// appendLabelRun decodes one record's labels into the arena for its kind.
//
// The zero value of T is there only to pick the instantiation; nothing reads it.
// A label is a uint16 written unaligned in the record stream, which is why this
// copies rather than aliasing: reading it in place would need an unsafe cast, and
// four megabytes at two million records is not worth one.
func appendLabelRun[T store.NodeType | store.EdgeType](arena []T, data []byte,
	version uint16, span csrRecordSpan, _ T) []T {
	pos := span.labelAt
	for range span.labelCount {
		var v uint16
		if version >= csrVersionWithU16Labels {
			v = binary.LittleEndian.Uint16(data[pos:])
			pos += currentLabelBytesPerValue
		} else {
			v = uint16(data[pos])
			pos++
		}
		arena = append(arena, T(v))
	}
	return arena
}

// blobAt is the property slice for one record: from the arena when the image is
// copied, and from the mapping when it is not.
//
// Both forms are taken with the three-index slice, for the reason arenaBytes and
// aliasCSRProperties both give: cap == len makes a caller appending to a record's
// Properties reallocate rather than write over the next record's bytes, which
// under a mapping would fault against a read-only file.
//
// A zero-length blob is nil in both, so a mapped read and a copied read stay
// indistinguishable -- both stores normalise an empty blob to nil on the way in.
func (s *csrRecordSource) blobAt(arena []byte, span csrRecordSpan, cursor int) []byte {
	if span.propLen == 0 {
		return nil
	}
	if s.mapped {
		end := span.propAt + span.propLen
		return s.data[span.propAt:end:end]
	}
	end := cursor + span.propLen
	return arena[cursor:end:end]
}

// nodeSeq is the repeatable walk buildSeq consumes.
//
// The two cursors are what replace a span table. Every walk starts them at zero
// and advances them by each record's own counts, and because the arenas were
// filled by a walk in this same order over these same bytes, record i's run is at
// the same place on every pass. That is the property the whole design rests on,
// and TestCSRLoad_EveryPassYieldsTheSameRecords is what holds it.
func (s *csrRecordSource) nodeSeq() iter.Seq[nodeRecord] {
	return func(yield func(nodeRecord) bool) {
		pos, labCur, propCur := s.nodeStart, 0, 0
		for i := range s.nodeCount {
			span, err := parseCSRNodeRecord(s.data, pos, s.version, i)
			if err != nil {
				s.latch(err)
				return
			}
			rec := nodeRecord{
				ID:         store.NodeID(span.id),
				Labels:     labelRun(s.nodeLabels, labCur, span.labelCount),
				Properties: s.blobAt(s.nodeProps, span, propCur),
			}
			labCur += span.labelCount
			if !s.mapped {
				propCur += span.propLen
			}
			pos = span.next
			if !yield(rec) {
				return
			}
		}
	}
}

// edgeSeq is nodeSeq for edges.
func (s *csrRecordSource) edgeSeq() iter.Seq[rawEdge] {
	return func(yield func(rawEdge) bool) {
		pos, labCur, propCur := s.edgeStart, 0, 0
		for i := range s.edgeCount {
			span, err := parseCSREdgeRecord(s.data, pos, s.version, i)
			if err != nil {
				s.latch(err)
				return
			}
			rec := rawEdge{
				ID:         store.EdgeID(span.id),
				Src:        store.NodeID(span.src),
				Dst:        store.NodeID(span.dst),
				Weight:     span.weight,
				Labels:     labelRun(s.edgeLabels, labCur, span.labelCount),
				Properties: s.blobAt(s.edgeProps, span, propCur),
			}
			labCur += span.labelCount
			if !s.mapped {
				propCur += span.propLen
			}
			pos = span.next
			if !yield(rec) {
				return
			}
		}
	}
}

// latch keeps the first failure a walk hit, since a sequence cannot return one.
func (s *csrRecordSource) latch(err error) {
	if s.err == nil {
		s.err = err
	}
}

// labelRun is arenaLabels addressed by a cursor and a count rather than by a
// stored span, under the same three-index contract and with the same nil for an
// empty run.
func labelRun[T store.NodeType | store.EdgeType](arena []T, at, n int) []T {
	if n == 0 {
		return nil
	}
	end := at + n
	return arena[at:end:end]
}

// payloadBytes is what these arenas cost the graph, by the accounting
// CSRGraph.payloadBytes documents: the label arenas always, and the property
// arenas only when the image is not mapped, where they are nil and contribute
// nothing.
//
// Capacity rather than length, so the slack an append-grown arena is holding is
// charged to the store that is holding it.
func (s *csrRecordSource) payloadBytes() int64 {
	return int64(cap(s.nodeProps)+cap(s.edgeProps)) +
		int64(cap(s.nodeLabels)+cap(s.edgeLabels))*sizeofLabel
}
