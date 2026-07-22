package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// CSR (Compressed Sparse Row) is an adjacency representation optimised for
// read-heavy, bulk-ingest workloads. For each node, all outbound (and
// separately all inbound) edge indices are stored contiguously in a flat array.
//
// Layout (in-memory; serialised to disk separately):
//
//   outOffset[nodeID]  → start index in outEdges
//   outOffset[nodeID+1] → exclusive end index in outEdges
//   outEdges[i]         → EdgeID at position i
//
// Same structure exists for inbound adjacency (inOffset / inEdges).

// rawEdge is the compact on-disk/in-memory edge representation used during
// CSR construction.
type rawEdge struct {
	ID         store.EdgeID
	Src        store.NodeID
	Dst        store.NodeID
	Labels     []store.EdgeType // one or more labels; nil/empty = unknown
	Weight     float32
	Properties []byte
}

// CSRGraph holds the built adjacency arrays plus the node/edge metadata slices.
type CSRGraph struct {
	// Node metadata indexed by NodeID (1-based; index 0 is unused).
	nodes []nodeRecord // len = maxNodeID + 1

	// Edge metadata indexed by EdgeID (1-based; index 0 is unused).
	edges []rawEdge // len = maxEdgeID + 1

	// Outbound adjacency.
	outOffset []uint64 // len = maxNodeID + 2
	outEdges  []store.EdgeID

	// Inbound adjacency.
	inOffset []uint64
	inEdges  []store.EdgeID

	// Label postings, built once at construction time. Answering NodesByType /
	// EdgesByType used to mean scanning every record in the CSR; these give the
	// same answer in time proportional to the number of matches.
	//
	// Both are keyed by label and hold ascending ID lists (construction walks the
	// record arrays in ID order). They are derived state: Build recomputes them,
	// so they are not part of the on-disk format and cost one pass at load time.
	nodesByLabel map[store.NodeType][]store.NodeID
	edgesByLabel map[store.EdgeType][]store.EdgeID

	// Sequence high-water marks: the largest node/edge IDs ever issued at the
	// time this CSR was built. Persisted so that IDs are never reused after a
	// delete-then-compact-then-reopen cycle drops the record that held the max
	// ID. Zero means "unknown" (older CSR formats) — callers fall back to the
	// max ID physically present.
	nodeSeqHW uint64
	edgeSeqHW uint64
}

// nodeRecord is the compact per-node record.
type nodeRecord struct {
	ID         store.NodeID
	Labels     []store.NodeType // one or more labels; nil/empty = unknown
	Properties []byte
}

// Build constructs a CSRGraph from a slice of nodes and edges.
// nodes and edges must be complete at build time (this is the bulk-ingest path).
func Build(nodes []nodeRecord, edges []rawEdge) *CSRGraph {
	if len(nodes) == 0 {
		return &CSRGraph{
			nodesByLabel: make(map[store.NodeType][]store.NodeID),
			edgesByLabel: make(map[store.EdgeType][]store.EdgeID),
		}
	}

	// Determine max node ID.
	var maxNID uint64
	for _, n := range nodes {
		if uint64(n.ID) > maxNID {
			maxNID = uint64(n.ID)
		}
	}

	// Determine max edge ID.
	var maxEID uint64
	for _, e := range edges {
		if uint64(e.ID) > maxEID {
			maxEID = uint64(e.ID)
		}
	}

	g := &CSRGraph{
		nodes:     make([]nodeRecord, maxNID+1),
		edges:     make([]rawEdge, maxEID+1),
		outOffset: make([]uint64, maxNID+2),
		inOffset:  make([]uint64, maxNID+2),
	}

	// Fill node records.
	for _, n := range nodes {
		g.nodes[n.ID] = n
	}

	// Fill edge records.
	for _, e := range edges {
		g.edges[e.ID] = e
	}

	// Count outbound and inbound degrees.
	for _, e := range edges {
		g.outOffset[e.Src+1]++
		g.inOffset[e.Dst+1]++
	}

	// Prefix-sum to compute start offsets.
	for i := 1; i < len(g.outOffset); i++ {
		g.outOffset[i] += g.outOffset[i-1]
	}
	for i := 1; i < len(g.inOffset); i++ {
		g.inOffset[i] += g.inOffset[i-1]
	}

	// Allocate adjacency arrays.
	total := uint64(len(edges))
	g.outEdges = make([]store.EdgeID, total)
	g.inEdges = make([]store.EdgeID, total)

	// Fill adjacency arrays using a temp counter.
	outCur := make([]uint64, len(g.outOffset))
	inCur := make([]uint64, len(g.inOffset))
	copy(outCur, g.outOffset)
	copy(inCur, g.inOffset)

	for _, e := range edges {
		g.outEdges[outCur[e.Src]] = e.ID
		outCur[e.Src]++
		g.inEdges[inCur[e.Dst]] = e.ID
		inCur[e.Dst]++
	}

	g.buildLabelIndex()

	return g
}

// buildLabelIndex populates the label postings by walking the record arrays in
// ID order, which yields ascending postings lists for free.
//
// A record's labels are deduplicated as we go: a repeated label would otherwise
// append the same ID twice in a row, breaking the strict-ascending invariant the
// postings rely on. Records carrying duplicates can come from a caller or from a
// CSR file written before that was normalised, so this cannot assume clean input.
func (g *CSRGraph) buildLabelIndex() {
	g.nodesByLabel = make(map[store.NodeType][]store.NodeID)
	g.edgesByLabel = make(map[store.EdgeType][]store.EdgeID)

	for i := 1; i < len(g.nodes); i++ {
		n := g.nodes[i]
		if n.ID == store.InvalidNodeID {
			continue
		}
		for j, lbl := range n.Labels {
			if nodeRecordHasLabel(n.Labels[:j], lbl) {
				continue
			}
			g.nodesByLabel[lbl] = append(g.nodesByLabel[lbl], n.ID)
		}
	}
	for i := 1; i < len(g.edges); i++ {
		e := g.edges[i]
		if e.ID == store.InvalidEdgeID {
			continue
		}
		for j, lbl := range e.Labels {
			if rawEdgeHasLabel(e.Labels[:j], lbl) {
				continue
			}
			g.edgesByLabel[lbl] = append(g.edgesByLabel[lbl], e.ID)
		}
	}
}

// OutboundEdges returns the raw edges for nodeID in outbound direction.
func (g *CSRGraph) OutboundEdges(id store.NodeID) ([]rawEdge, error) {
	return g.adjacentEdges(id, g.outOffset, g.outEdges)
}

// InboundEdges returns the raw edges for nodeID in inbound direction.
func (g *CSRGraph) InboundEdges(id store.NodeID) ([]rawEdge, error) {
	return g.adjacentEdges(id, g.inOffset, g.inEdges)
}

func (g *CSRGraph) adjacentEdges(id store.NodeID, offsets []uint64, edgeList []store.EdgeID) ([]rawEdge, error) {
	if int(id) >= len(offsets)-1 {
		return nil, fmt.Errorf("node %d out of range", id)
	}
	start := offsets[id]
	end := offsets[id+1]
	result := make([]rawEdge, 0, end-start)
	for i := start; i < end; i++ {
		eid := edgeList[i]
		if int(eid) < len(g.edges) {
			result = append(result, g.edges[eid])
		}
	}
	return result, nil
}

// verifyLabelIndex checks that the label postings describe exactly the records
// present in the CSR, in ascending order.
func (g *CSRGraph) verifyLabelIndex() error {
	for lbl, ids := range g.nodesByLabel {
		for i, id := range ids {
			if i > 0 && ids[i-1] >= id {
				return fmt.Errorf("csr node label index: %v postings not strictly ascending at %d", lbl, i)
			}
			n, ok := g.GetNode(id)
			if !ok {
				return fmt.Errorf("csr node label index: %v lists node %d, which is not in the CSR", lbl, id)
			}
			if !nodeRecordHasLabel(n.Labels, lbl) {
				return fmt.Errorf("csr node label index: %v lists node %d, which does not carry that label", lbl, id)
			}
		}
	}
	for i := 1; i < len(g.nodes); i++ {
		n := g.nodes[i]
		if n.ID == store.InvalidNodeID {
			continue
		}
		for _, lbl := range n.Labels {
			// Postings are ascending, so membership is a binary search. A linear
			// scan here would make verification quadratic in the size of the
			// largest label.
			if !sortedContainsNodeID(g.nodesByLabel[lbl], n.ID) {
				return fmt.Errorf("csr node label index: node %d carries %v but is missing from the postings", n.ID, lbl)
			}
		}
	}

	for lbl, ids := range g.edgesByLabel {
		for i, id := range ids {
			if i > 0 && ids[i-1] >= id {
				return fmt.Errorf("csr edge label index: %v postings not strictly ascending at %d", lbl, i)
			}
			e, ok := g.GetEdge(id)
			if !ok {
				return fmt.Errorf("csr edge label index: %v lists edge %d, which is not in the CSR", lbl, id)
			}
			if !rawEdgeHasLabel(e.Labels, lbl) {
				return fmt.Errorf("csr edge label index: %v lists edge %d, which does not carry that label", lbl, id)
			}
		}
	}
	for i := 1; i < len(g.edges); i++ {
		e := g.edges[i]
		if e.ID == store.InvalidEdgeID {
			continue
		}
		for _, lbl := range e.Labels {
			if !sortedContainsEdgeID(g.edgesByLabel[lbl], e.ID) {
				return fmt.Errorf("csr edge label index: edge %d carries %v but is missing from the postings", e.ID, lbl)
			}
		}
	}
	return nil
}

// sortedContainsNodeID reports membership in an ascending slice in O(log n).
func sortedContainsNodeID(ids []store.NodeID, target store.NodeID) bool {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] >= target })
	return i < len(ids) && ids[i] == target
}

// sortedContainsEdgeID reports membership in an ascending slice in O(log n).
func sortedContainsEdgeID(ids []store.EdgeID, target store.EdgeID) bool {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] >= target })
	return i < len(ids) && ids[i] == target
}

// verifyAdjacency checks that the offset arrays are monotonic and that every
// adjacency entry points at an edge whose endpoint matches.
func (g *CSRGraph) verifyAdjacency() error {
	if len(g.outOffset) != len(g.inOffset) {
		return fmt.Errorf("csr adjacency: offset arrays differ in length (%d vs %d)", len(g.outOffset), len(g.inOffset))
	}
	for i := 1; i < len(g.outOffset); i++ {
		if g.outOffset[i] < g.outOffset[i-1] {
			return fmt.Errorf("csr adjacency: outOffset not monotonic at %d", i)
		}
		if g.inOffset[i] < g.inOffset[i-1] {
			return fmt.Errorf("csr adjacency: inOffset not monotonic at %d", i)
		}
	}
	if n := len(g.outOffset); n > 0 && int(g.outOffset[n-1]) != len(g.outEdges) {
		return fmt.Errorf("csr adjacency: outOffset tail %d != len(outEdges) %d", g.outOffset[n-1], len(g.outEdges))
	}
	if n := len(g.inOffset); n > 0 && int(g.inOffset[n-1]) != len(g.inEdges) {
		return fmt.Errorf("csr adjacency: inOffset tail %d != len(inEdges) %d", g.inOffset[n-1], len(g.inEdges))
	}

	for id := 1; id < len(g.outOffset)-1; id++ {
		nodeID := store.NodeID(id)
		for _, eid := range g.OutboundEdgeIDs(nodeID) {
			e, ok := g.GetEdge(eid)
			if !ok {
				return fmt.Errorf("csr adjacency: node %d lists outbound edge %d, which is not in the CSR", nodeID, eid)
			}
			if e.Src != nodeID {
				return fmt.Errorf("csr adjacency: node %d lists outbound edge %d, whose Src is %d", nodeID, eid, e.Src)
			}
		}
		for _, eid := range g.InboundEdgeIDs(nodeID) {
			e, ok := g.GetEdge(eid)
			if !ok {
				return fmt.Errorf("csr adjacency: node %d lists inbound edge %d, which is not in the CSR", nodeID, eid)
			}
			if e.Dst != nodeID {
				return fmt.Errorf("csr adjacency: node %d lists inbound edge %d, whose Dst is %d", nodeID, eid, e.Dst)
			}
		}
	}
	return nil
}

// OutboundEdgeIDs returns the outbound edge IDs for nodeID as a sub-slice of the
// CSR's internal adjacency array — no copy, no rawEdge materialisation. The
// result aliases CSR-owned memory: callers must hold the store lock and must not
// retain or mutate it.
func (g *CSRGraph) OutboundEdgeIDs(id store.NodeID) []store.EdgeID {
	return adjacencySlice(id, g.outOffset, g.outEdges)
}

// InboundEdgeIDs returns the inbound edge IDs for nodeID. Same aliasing contract
// as OutboundEdgeIDs.
func (g *CSRGraph) InboundEdgeIDs(id store.NodeID) []store.EdgeID {
	return adjacencySlice(id, g.inOffset, g.inEdges)
}

// OutDegree returns the outbound degree of nodeID in constant time, straight
// from the offset array. It counts edges present in the CSR, so callers must
// still account for any delete masks held by the store.
func (g *CSRGraph) OutDegree(id store.NodeID) int {
	return len(adjacencySlice(id, g.outOffset, g.outEdges))
}

// InDegree returns the inbound degree of nodeID in constant time.
func (g *CSRGraph) InDegree(id store.NodeID) int {
	return len(adjacencySlice(id, g.inOffset, g.inEdges))
}

func adjacencySlice(id store.NodeID, offsets []uint64, edgeList []store.EdgeID) []store.EdgeID {
	if int(id) >= len(offsets)-1 {
		return nil
	}
	return edgeList[offsets[id]:offsets[id+1]]
}

// GetNode returns the nodeRecord for the given ID.
func (g *CSRGraph) GetNode(id store.NodeID) (nodeRecord, bool) {
	if int(id) >= len(g.nodes) {
		return nodeRecord{}, false
	}
	n := g.nodes[id]
	return n, n.ID == id
}

// GetEdge returns the rawEdge for the given ID.
func (g *CSRGraph) GetEdge(id store.EdgeID) (rawEdge, bool) {
	if int(id) >= len(g.edges) {
		return rawEdge{}, false
	}
	e := g.edges[id]
	return e, e.ID == id
}

// NodeCount returns the number of stored nodes.
func (g *CSRGraph) NodeCount() int {
	count := 0
	for i := 1; i < len(g.nodes); i++ {
		if g.nodes[i].ID != store.InvalidNodeID {
			count++
		}
	}
	return count
}

// EdgeCount returns the number of stored edges.
func (g *CSRGraph) EdgeCount() int {
	return len(g.outEdges)
}

// Serialise writes the CSR to binary format v7 (csrVersionCurrent).
//
// v7 is v6 minus the flat adjacency arrays: the reader never parsed them — it
// rebuilds adjacency from the edge records — so they were ~21% of the file
// written on every Compact and read by nobody.
//
// Format:
//
//	[magic:4]["GCSR"][version:2=0x0007][nodeCount:8][edgeCount:8]
//	[nodeSeqHW:8][edgeSeqHW:8][indexOffset:8]
//	[nodeRecord * nodeCount] (each: id:8 + labelCount:1 + labels:(currentLabelBytesPerValue*N) + propLen:4 + props:N)
//	[rawEdge * edgeCount]    (each: id:8 + src:8 + dst:8 + labelCount:1 + labels:(currentLabelBytesPerValue*N) + weight:4 + propLen:4 + props:N)
//	--- index section, at byte indexOffset (v6+) ---
//	[magic:4]["GIDX"]
//	[nodePropCount:8][entry * nodePropCount] (each: id:8 + keyLen:2 + key + valLen:4 + val)
//	[edgePropCount:8][entry * edgePropCount]
//
// # What is persisted, and what is not
//
// The property index IS persisted: its entries are supplied by the caller in the
// caller's own encoding, so nothing in the file can reconstruct them. Before v6
// they lived only in the WAL, which meant every Compact re-emitted the whole
// index and every restart replayed it — a cost that grew forever.
//
// The label postings are NOT persisted: they are derivable from the node and
// edge records in a single pass at load time (see buildLabelIndex), so storing
// them would add file size and a consistency risk to save very little.
//
// The adjacency arrays are NOT persisted either, for the same reason — Build()
// reconstructs them from the records. They *were* written through v6, and never
// read: the reader has always rebuilt them and then used indexOffset to skip
// whatever lay between. Roughly a fifth of the file was that skipped region.
//
// indexOffset makes the index section directly addressable, so the reader never
// has to compute its position from the adjacency array sizes.
func (g *CSRGraph) Serialise() []byte {
	return g.SerialiseWithIndex(nil, nil)
}

// SerialiseWithIndex writes the CSR plus the given property-index entries.
// Passing nil for both writes a v6 file with an empty index section.
func (g *CSRGraph) SerialiseWithIndex(nodeProps []index.NodePropEntry, edgeProps []index.EdgePropEntry) []byte {
	var buf bytes.Buffer

	// Count valid nodes and edges.
	nodeCount := 0
	for i := 1; i < len(g.nodes); i++ {
		if g.nodes[i].ID != store.InvalidNodeID {
			nodeCount++
		}
	}
	edgeCount := 0
	for i := 1; i < len(g.edges); i++ {
		if g.edges[i].ID != store.InvalidEdgeID {
			edgeCount++
		}
	}

	// Header
	buf.Write([]byte("GCSR"))
	writeUint16(&buf, csrVersionCurrent)
	writeUint64(&buf, uint64(nodeCount))
	writeUint64(&buf, uint64(edgeCount))
	// Sequence high-water marks (version 5+).
	writeUint64(&buf, g.nodeSeqHW)
	writeUint64(&buf, g.edgeSeqHW)
	// Placeholder for indexOffset (version 6+); patched once the body is written.
	indexOffsetPos := buf.Len()
	writeUint64(&buf, 0)

	// Nodes (variable-length labels)
	for i := 1; i < len(g.nodes); i++ {
		n := g.nodes[i]
		if n.ID == store.InvalidNodeID {
			continue
		}
		writeUint64(&buf, uint64(n.ID))
		buf.WriteByte(byte(len(n.Labels)))
		for _, lbl := range n.Labels {
			writeUint16(&buf, uint16(lbl))
		}
		writeUint32(&buf, uint32(len(n.Properties)))
		buf.Write(n.Properties)
	}

	// Edges (variable-length labels)
	for i := 1; i < len(g.edges); i++ {
		e := g.edges[i]
		if e.ID == store.InvalidEdgeID {
			continue
		}
		writeUint64(&buf, uint64(e.ID))
		writeUint64(&buf, uint64(e.Src))
		writeUint64(&buf, uint64(e.Dst))
		buf.WriteByte(byte(len(e.Labels)))
		for _, lbl := range e.Labels {
			writeUint16(&buf, uint16(lbl))
		}
		var wbuf [4]byte
		binary.LittleEndian.PutUint32(wbuf[:], math.Float32bits(e.Weight))
		buf.Write(wbuf[:])
		writeUint32(&buf, uint32(len(e.Properties)))
		buf.Write(e.Properties)
	}

	// No adjacency arrays. They were written here through v6 and never read
	// back: deserialiseCSR rebuilds them with Build() from the records it has
	// just parsed, then jumps straight to indexOffset. Proven by
	// TestAdjacencyArraysAreDeadBytes, which corrupts the region and observes no
	// difference.
	//
	// On a 100k-node fixture that was ~4.8 MB of a ~22 MB file, written on every
	// Compact and read by nobody.

	// Index section
	indexOffset := buf.Len()
	buf.WriteString(csrIndexSectionMagic)
	writeUint64(&buf, uint64(len(nodeProps)))
	for _, e := range nodeProps {
		writePropEntry(&buf, uint64(e.ID), e.Key, e.Value)
	}
	writeUint64(&buf, uint64(len(edgeProps)))
	for _, e := range edgeProps {
		writePropEntry(&buf, uint64(e.ID), e.Key, e.Value)
	}

	out := buf.Bytes()
	binary.LittleEndian.PutUint64(out[indexOffsetPos:indexOffsetPos+8], uint64(indexOffset))
	return out
}

// writePropEntry appends one property-index entry:
// id:8 + keyLen:2 + key + valLen:4 + value.
func writePropEntry(buf *bytes.Buffer, id uint64, key string, value []byte) {
	writeUint64(buf, id)
	writeUint16(buf, uint16(len(key)))
	buf.WriteString(key)
	writeUint32(buf, uint32(len(value)))
	buf.Write(value)
}

// writeUint16 appends a little-endian uint16 to buf.
func writeUint16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

// writeUint64 appends a little-endian uint64 to buf.
func writeUint64(buf *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	buf.Write(b[:])
}

// writeUint32 appends a little-endian uint32 to buf.
func writeUint32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

// NodesByType returns the ascending node IDs carrying the given label, served
// from the label index. The result aliases CSR-owned memory: callers must hold
// the store lock and must not retain or mutate it.
func (g *CSRGraph) NodesByType(t store.NodeType) []store.NodeID {
	return g.nodesByLabel[t]
}

// EdgesByType returns the ascending edge IDs carrying the given label. Same
// aliasing contract as NodesByType.
func (g *CSRGraph) EdgesByType(t store.EdgeType) []store.EdgeID {
	return g.edgesByLabel[t]
}

// SortedEdgesByType returns edge IDs sorted for deterministic output, as a copy
// the caller owns. The label index is already built in ascending ID order, so
// this only has to detach the result from CSR-owned memory.
func (g *CSRGraph) SortedEdgesByType(t store.EdgeType) []store.EdgeID {
	ids := g.EdgesByType(t)
	out := make([]store.EdgeID, len(ids))
	copy(out, ids)
	slices.Sort(out)
	return out
}

// nodeRecordHasLabel returns true if the label slice contains t.
func nodeRecordHasLabel(labels []store.NodeType, t store.NodeType) bool {
	for _, l := range labels {
		if l == t {
			return true
		}
	}
	return false
}

// rawEdgeHasLabel returns true if the label slice contains t.
func rawEdgeHasLabel(labels []store.EdgeType, t store.EdgeType) bool {
	for _, l := range labels {
		if l == t {
			return true
		}
	}
	return false
}
