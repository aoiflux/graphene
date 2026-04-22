package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"graphene/store"
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
	ID     store.EdgeID
	Src    store.NodeID
	Dst    store.NodeID
	Labels []store.EdgeType // one or more labels; nil/empty = unknown
	Weight float32
	// Properties are stored separately in the property store; only the
	// blob offset is kept here (0 = no properties).
	PropOffset uint64
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
}

// nodeRecord is the compact per-node record.
type nodeRecord struct {
	ID         store.NodeID
	Labels     []store.NodeType // one or more labels; nil/empty = unknown
	PropOffset uint64           // offset into property blob store (0 = no props)
}

// Build constructs a CSRGraph from a slice of nodes and edges.
// nodes and edges must be complete at build time (this is the bulk-ingest path).
func Build(nodes []nodeRecord, edges []rawEdge) *CSRGraph {
	if len(nodes) == 0 {
		return &CSRGraph{}
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

	return g
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

// Serialise writes the CSR arrays to binary format v2 (variable-length labels).
//
// Format:
//
//	[magic:4][version:2=0x0002][nodeCount:8][edgeCount:8]
//	[nodeRecord * nodeCount] (each: id:8 + labelCount:1 + labels:N + propOffset:8)
//	[rawEdge * edgeCount]    (each: id:8 + src:8 + dst:8 + labelCount:1 + labels:N + weight:4 + propOffset:8)
//	[outOffset * (maxNodeID+2):8 each]
//	[outEdges * edgeCount:8 each]
//	[inOffset * (maxNodeID+2):8 each]
//	[inEdges * edgeCount:8 each]
func (g *CSRGraph) Serialise() []byte {
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
	writeUint16(&buf, 2) // version 2
	writeUint64(&buf, uint64(nodeCount))
	writeUint64(&buf, uint64(edgeCount))

	// Nodes (variable-length labels)
	for i := 1; i < len(g.nodes); i++ {
		n := g.nodes[i]
		if n.ID == store.InvalidNodeID {
			continue
		}
		writeUint64(&buf, uint64(n.ID))
		buf.WriteByte(byte(len(n.Labels)))
		for _, lbl := range n.Labels {
			buf.WriteByte(byte(lbl))
		}
		writeUint64(&buf, n.PropOffset)
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
			buf.WriteByte(byte(lbl))
		}
		var wbuf [4]byte
		binary.LittleEndian.PutUint32(wbuf[:], math.Float32bits(e.Weight))
		buf.Write(wbuf[:])
		writeUint64(&buf, e.PropOffset)
	}

	// Adjacency arrays
	for _, v := range g.outOffset {
		writeUint64(&buf, v)
	}
	for _, v := range g.outEdges {
		writeUint64(&buf, uint64(v))
	}
	for _, v := range g.inOffset {
		writeUint64(&buf, v)
	}
	for _, v := range g.inEdges {
		writeUint64(&buf, uint64(v))
	}

	return buf.Bytes()
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

// NodesByType returns all node IDs that carry the given label from the CSR.
func (g *CSRGraph) NodesByType(t store.NodeType) []store.NodeID {
	var out []store.NodeID
	for i := 1; i < len(g.nodes); i++ {
		n := g.nodes[i]
		if n.ID != store.InvalidNodeID && nodeRecordHasLabel(n.Labels, t) {
			out = append(out, n.ID)
		}
	}
	return out
}

// EdgesByType returns all edge IDs that carry the given label from the CSR.
func (g *CSRGraph) EdgesByType(t store.EdgeType) []store.EdgeID {
	var out []store.EdgeID
	for i := 1; i < len(g.edges); i++ {
		e := g.edges[i]
		if e.ID != store.InvalidEdgeID && rawEdgeHasLabel(e.Labels, t) {
			out = append(out, e.ID)
		}
	}
	return out
}

// SortedEdgesByType returns edge IDs sorted for deterministic output.
func (g *CSRGraph) SortedEdgesByType(t store.EdgeType) []store.EdgeID {
	ids := g.EdgesByType(t)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
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
