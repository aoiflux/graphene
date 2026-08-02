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
	"math"
	"os"
	"time"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// loadCSR reads and parses the CSR image at path into the store.
func (s *Store) loadCSR(path string) error {
	// For this first implementation, we load the CSR into memory from the
	// serialised file. A future enhancement can mmap this file directly.
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	csr, section, err := deserialiseCSR(data)
	if err != nil {
		return err
	}
	s.publishCSR(csr)

	// Load the persisted property index (v6+). This happens before WAL replay so
	// that post-compaction WAL records — including purges — apply on top of it.
	// A pre-v6 file carries no section; its entries come from the WAL as before,
	// and the next Compact writes them into the CSR.
	if section != nil {
		// Re-declare before the entries land, so the ordered structures are built
		// by the same incremental path a live declaration uses rather than by a
		// backfill afterwards. Either order is correct — DeclareOrderedNodeKey
		// backfills from what is already indexed — but doing it first means there
		// is one path to reason about.
		//
		// This is what stops a reopen silently turning every declared range query
		// back into a scan.
		for _, k := range section.OrderedNodeKeys {
			s.propIdx.DeclareOrderedNodeKey(k)
		}
		for _, k := range section.OrderedEdgeKeys {
			s.propIdx.DeclareOrderedEdgeKey(k)
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
	}
	// Advance sequence counters past existing CSR IDs.
	for i := len(csr.nodes) - 1; i >= 1; i-- {
		if csr.nodes[i].ID != store.InvalidNodeID {
			if uint64(csr.nodes[i].ID) > s.nodeSeq.Load() {
				s.nodeSeq.Store(uint64(csr.nodes[i].ID))
			}
			break
		}
	}
	for i := len(csr.edges) - 1; i >= 1; i-- {
		if csr.edges[i].ID != store.InvalidEdgeID {
			if uint64(csr.edges[i].ID) > s.edgeSeq.Load() {
				s.edgeSeq.Store(uint64(csr.edges[i].ID))
			}
			break
		}
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

// deserialiseCSR reconstructs a CSRGraph from Serialise() byte slices.
// Format v2 stores labels plus a reserved property offset; format v3 stores
// 1-byte labels plus inline property blobs; format v4 stores uint16 labels
// plus inline property blobs.
func deserialiseCSR(data []byte) (*CSRGraph, *csrIndexSection, error) {
	if len(data) < 22 {
		return nil, nil, fmt.Errorf("deserialiseCSR: data too short")
	}
	if string(data[0:4]) != "GCSR" {
		return nil, nil, fmt.Errorf("deserialiseCSR: invalid magic")
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version < csrVersionV2 || version > csrVersionCurrent {
		return nil, nil, fmt.Errorf("deserialiseCSR: unsupported version %d (supported: %d-%d)",
			version, csrVersionV2, csrVersionCurrent)
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
		labels := make([]store.NodeType, labelCount)
		for j := 0; j < labelCount; j++ {
			if version >= csrVersionWithU16Labels {
				labels[j] = store.NodeType(binary.LittleEndian.Uint16(data[pos:]))
				pos += currentLabelBytesPerValue
			} else {
				labels[j] = store.NodeType(data[pos])
				pos++
			}
		}
		props, nextPos, err := readCSRProperties(data, pos, version, "node", i)
		if err != nil {
			return nil, nil, err
		}
		pos = nextPos
		nodes[i] = nodeRecord{ID: nid, Labels: labels, Properties: props}
	}

	if edgeCount < 0 || edgeCount > (len(data)-pos)/minEdgeRecordBytes {
		return nil, nil, fmt.Errorf("deserialiseCSR: edge count %d exceeds what %d remaining bytes can hold",
			edgeCount, len(data)-pos)
	}
	edges := make([]rawEdge, edgeCount)
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
		labels := make([]store.EdgeType, labelCount)
		for j := 0; j < labelCount; j++ {
			if version >= csrVersionWithU16Labels {
				labels[j] = store.EdgeType(binary.LittleEndian.Uint16(data[pos:]))
				pos += currentLabelBytesPerValue
			} else {
				labels[j] = store.EdgeType(data[pos])
				pos++
			}
		}
		weight := math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		props, nextPos, err := readCSRProperties(data, pos, version, "edge", i)
		if err != nil {
			return nil, nil, err
		}
		pos = nextPos
		edges[i] = rawEdge{ID: eid, Src: src, Dst: dst, Labels: labels, Weight: weight, Properties: props}
	}

	// Build indexes its arrays by entity ID, not by record count, so it allocates
	// maxID+1 slots regardless of how few records there are. Bounding the counts
	// above is therefore not enough: two records carrying IDs of 0x3030303030303030
	// make a 105-byte file demand an exabyte-scale slice, which panics in
	// makeslice rather than returning an error. Validate the IDs before Build
	// sees them.
	if err := checkCSREntityIDs(nodes, edges, version, nodeSeqHW, edgeSeqHW); err != nil {
		return nil, nil, err
	}

	csr := Build(nodes, edges)
	csr.nodeSeqHW = nodeSeqHW
	csr.edgeSeqHW = edgeSeqHW
	csr.commitSeqHW = trailer.CommitSeqHW
	csr.lastCompactUnixNano = trailer.LastCompactUnixNano

	// Pre-v6 files carry no index section; the caller falls back to the WAL.
	if version < csrVersionWithPropIndex {
		return csr, nil, nil
	}
	section, err := readCSRIndexSection(data, int(indexOffset))
	if err != nil {
		return nil, nil, err
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

	// Keys declared ordered when the image was written (GORD, v8+).
	OrderedNodeKeys []string
	OrderedEdgeKeys []string
}

// checkIDCeiling bounds the highest ID of one entity kind both absolutely and
// against how many records of that kind the file actually carries.
//
// The relative bound is the one that matters for a small file: without it a
// single 13-byte record naming the maximum permitted ID is enough to demand the
// entire absolute ceiling's worth of memory. With it, a file's worst case scales
// with what the file contains.
func checkIDCeiling(kind string, maxID uint64, records int) error {
	if maxID > maxCSREntityID {
		return fmt.Errorf("deserialiseCSR: %s ID %d exceeds the maximum addressable ID %d",
			kind, maxID, uint64(maxCSREntityID))
	}
	allowed := uint64(records) * csrIDSparsityFactor
	if allowed < csrIDSparsityFloor {
		allowed = csrIDSparsityFloor
	}
	if maxID > allowed {
		return fmt.Errorf("deserialiseCSR: %s ID %d is too sparse for %d records (ceiling %d)",
			kind, maxID, records, allowed)
	}
	return nil
}

// checkCSREntityIDs rejects records whose IDs would make Build allocate an
// array the file gives no reason to believe in.
//
// Two rules, in order of strength:
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
//
// IDs are deliberately NOT bounded by the record count. They are monotonic and
// never reused, so a long-lived store that has deleted heavily has a maxID far
// above its live count, and that file is perfectly valid.
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

	if err := checkIDCeiling("node", maxNID, len(nodes)); err != nil {
		return err
	}
	if err := checkIDCeiling("edge", maxEID, len(edges)); err != nil {
		return err
	}

	// Endpoints are a separate bound from IDs, and the one that actually crashes.
	// Build sizes the adjacency offset arrays from the highest *node* ID, then
	// indexes them by each edge's Src and Dst without checking, so an edge naming
	// a node the file does not contain reads past the end of the array rather
	// than producing a parse error. Every live edge has both endpoints present —
	// deletion cascades to incident edges — so this rejects nothing valid.
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
