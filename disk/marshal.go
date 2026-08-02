package disk

// Wire encoding for the four WAL record kinds — nodes, edges, and the node and
// edge property-index entries — plus the bare-ID payloads tombstones and purges
// carry. Split out of store.go, unchanged.
//
// These layouts are an on-disk format promise, not an implementation detail: a
// change here is a WAL format change and needs the version discipline described
// in wal.go, not just a passing test.

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/aoiflux/graphene/store"
)

// appendMarshalledNode writes n's wire encoding onto dst:
// id(8) labelCount(1) labels(2*N) propLen(4) props(n).
//
// The allocating marshalNode below is this plus a fresh buffer; batch writes use
// this form so the payload lands straight in the frame, costing neither a
// per-record allocation nor a copy.
func appendMarshalledNode(dst []byte, n *store.Node) []byte {
	labelCount := len(n.Labels)
	propLen := len(n.Properties)

	var hdr [nodePayloadLabelStart]byte
	binary.LittleEndian.PutUint64(hdr[0:nodePayloadIDBytes], uint64(n.ID))
	hdr[nodePayloadIDBytes] = byte(labelCount)
	dst = append(dst, hdr[:]...)

	var lbl [currentLabelBytesPerValue]byte
	for _, l := range n.Labels {
		binary.LittleEndian.PutUint16(lbl[:], uint16(l))
		dst = append(dst, lbl[:]...)
	}
	var pl [nodePayloadPropLenBytes]byte
	binary.LittleEndian.PutUint32(pl[:], uint32(propLen))
	dst = append(dst, pl[:]...)
	if propLen > 0 {
		dst = append(dst, n.Properties...)
	}
	return dst
}

// appendMarshalledEdge is appendMarshalledNode for edges.
func appendMarshalledEdge(dst []byte, e *store.Edge) []byte {
	labelCount := len(e.Labels)
	propLen := len(e.Properties)

	var hdr [edgePayloadLabelStart]byte
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(e.ID))
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(e.Src))
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(e.Dst))
	hdr[edgePayloadIDsBytes] = byte(labelCount)
	dst = append(dst, hdr[:]...)

	var lbl [currentLabelBytesPerValue]byte
	for _, l := range e.Labels {
		binary.LittleEndian.PutUint16(lbl[:], uint16(l))
		dst = append(dst, lbl[:]...)
	}
	var tail [edgePayloadTailFixedSize]byte
	binary.LittleEndian.PutUint32(tail[0:4], math.Float32bits(e.Weight))
	binary.LittleEndian.PutUint32(tail[4:8], uint32(propLen))
	dst = append(dst, tail[:]...)
	if propLen > 0 {
		dst = append(dst, e.Properties...)
	}
	return dst
}

func marshalNode(n *store.Node) []byte {
	labelCount := len(n.Labels)
	propLen := len(n.Properties)
	labelsBytes := currentLabelBytesPerValue * labelCount
	// 8 (id) + 1 (labelCount) + 2*N (labels) + 4 (propLen) + propLen
	buf := make([]byte, nodePayloadLabelStart+labelsBytes+nodePayloadPropLenBytes+propLen)
	binary.LittleEndian.PutUint64(buf[0:nodePayloadIDBytes], uint64(n.ID))
	buf[nodePayloadIDBytes] = byte(labelCount)
	for i, lbl := range n.Labels {
		base := nodePayloadLabelStart + (currentLabelBytesPerValue * i)
		binary.LittleEndian.PutUint16(buf[base:base+2], uint16(lbl))
	}
	base := nodePayloadLabelStart + labelsBytes
	binary.LittleEndian.PutUint32(buf[base:base+4], uint32(propLen))
	if propLen > 0 {
		copy(buf[base+4:], n.Properties)
	}
	return buf
}

func unmarshalNode(b []byte) (*store.Node, error) {
	if len(b) < nodePayloadLabelStart {
		return nil, fmt.Errorf("unmarshalNode: payload too short (%d bytes)", len(b))
	}
	id := store.NodeID(binary.LittleEndian.Uint64(b[0:nodePayloadIDBytes]))
	labelCount := int(b[nodePayloadIDBytes])

	newBase := nodePayloadLabelStart + (currentLabelBytesPerValue * labelCount)
	oldBase := nodePayloadLabelStart + (legacyLabelBytesPerValue * labelCount)

	decodeV2 := false
	decodeV1 := false

	if len(b) >= newBase+4 {
		propLenNew := int(binary.LittleEndian.Uint32(b[newBase : newBase+4]))
		if newBase+4+propLenNew == len(b) {
			decodeV2 = true
		}
	}
	if !decodeV2 && len(b) >= oldBase+4 {
		propLenOld := int(binary.LittleEndian.Uint32(b[oldBase : oldBase+4]))
		if oldBase+4+propLenOld == len(b) {
			decodeV1 = true
		}
	}
	if !decodeV2 && !decodeV1 {
		return nil, fmt.Errorf("unmarshalNode: payload truncated (labels)")
	}

	labels := make([]store.NodeType, labelCount)
	base := oldBase
	if decodeV2 {
		for i := 0; i < labelCount; i++ {
			lb := nodePayloadLabelStart + (currentLabelBytesPerValue * i)
			labels[i] = store.NodeType(binary.LittleEndian.Uint16(b[lb : lb+2]))
		}
		base = newBase
	} else {
		for i := 0; i < labelCount; i++ {
			labels[i] = store.NodeType(b[nodePayloadLabelStart+i])
		}
	}

	propLen := int(binary.LittleEndian.Uint32(b[base : base+4]))
	if len(b) < base+4+propLen {
		return nil, fmt.Errorf("unmarshalNode: payload truncated (props)")
	}
	var props []byte
	if propLen > 0 {
		props = make([]byte, propLen)
		copy(props, b[base+4:base+4+propLen])
	}
	return &store.Node{ID: id, Labels: labels, Properties: props}, nil
}

// marshalEdge encodes an Edge: id(8) src(8) dst(8) labelCount(1) labels(2*N) weight(4) propLen(4) props(n)
func marshalEdge(e *store.Edge) []byte {
	labelCount := len(e.Labels)
	propLen := len(e.Properties)
	labelsBytes := currentLabelBytesPerValue * labelCount
	// 8+8+8 (ids) + 1 (labelCount) + 2*N (labels) + 4 (weight) + 4 (propLen) + propLen
	buf := make([]byte, edgePayloadLabelStart+labelsBytes+edgePayloadTailFixedSize+propLen)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.ID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(e.Src))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(e.Dst))
	buf[edgePayloadIDsBytes] = byte(labelCount)
	for i, lbl := range e.Labels {
		base := edgePayloadLabelStart + (currentLabelBytesPerValue * i)
		binary.LittleEndian.PutUint16(buf[base:base+2], uint16(lbl))
	}
	base := edgePayloadLabelStart + labelsBytes
	binary.LittleEndian.PutUint32(buf[base:base+4], math.Float32bits(e.Weight))
	binary.LittleEndian.PutUint32(buf[base+4:base+8], uint32(propLen))
	if propLen > 0 {
		copy(buf[base+8:], e.Properties)
	}
	return buf
}

func unmarshalEdge(b []byte) (*store.Edge, error) {
	if len(b) < edgePayloadLabelStart {
		return nil, fmt.Errorf("unmarshalEdge: payload too short (%d bytes)", len(b))
	}
	id := store.EdgeID(binary.LittleEndian.Uint64(b[0:8]))
	src := store.NodeID(binary.LittleEndian.Uint64(b[8:16]))
	dst := store.NodeID(binary.LittleEndian.Uint64(b[16:24]))
	labelCount := int(b[edgePayloadIDsBytes])

	newBase := edgePayloadLabelStart + (currentLabelBytesPerValue * labelCount)
	oldBase := edgePayloadLabelStart + (legacyLabelBytesPerValue * labelCount)

	decodeV2 := false
	decodeV1 := false

	if len(b) >= newBase+8 {
		propLenNew := int(binary.LittleEndian.Uint32(b[newBase+4 : newBase+8]))
		if newBase+8+propLenNew == len(b) {
			decodeV2 = true
		}
	}
	if !decodeV2 && len(b) >= oldBase+8 {
		propLenOld := int(binary.LittleEndian.Uint32(b[oldBase+4 : oldBase+8]))
		if oldBase+8+propLenOld == len(b) {
			decodeV1 = true
		}
	}
	if !decodeV2 && !decodeV1 {
		return nil, fmt.Errorf("unmarshalEdge: payload truncated (labels)")
	}

	labels := make([]store.EdgeType, labelCount)
	base := oldBase
	if decodeV2 {
		for i := 0; i < labelCount; i++ {
			lb := edgePayloadLabelStart + (currentLabelBytesPerValue * i)
			labels[i] = store.EdgeType(binary.LittleEndian.Uint16(b[lb : lb+2]))
		}
		base = newBase
	} else {
		for i := 0; i < labelCount; i++ {
			labels[i] = store.EdgeType(b[edgePayloadLabelStart+i])
		}
	}

	weight := math.Float32frombits(binary.LittleEndian.Uint32(b[base : base+4]))
	propLen := int(binary.LittleEndian.Uint32(b[base+4 : base+8]))
	if len(b) < base+8+propLen {
		return nil, fmt.Errorf("unmarshalEdge: payload truncated (props)")
	}
	var props []byte
	if propLen > 0 {
		props = make([]byte, propLen)
		copy(props, b[base+8:base+8+propLen])
	}
	return &store.Edge{ID: id, Src: src, Dst: dst, Labels: labels, Weight: weight, Properties: props}, nil
}

func rawEdgeToStore(re rawEdge) *store.Edge {
	return &store.Edge{
		ID:         re.ID,
		Src:        re.Src,
		Dst:        re.Dst,
		Labels:     re.Labels,
		Weight:     re.Weight,
		Properties: csrBytes(re.Properties),
	}
}

// marshalID encodes an 8-byte little-endian ID payload for tombstone records.
func marshalID(id uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, id)
	return buf
}

// unmarshalID decodes an 8-byte tombstone payload.
func unmarshalID(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, fmt.Errorf("unmarshalID: payload too short (%d bytes)", len(b))
	}
	return binary.LittleEndian.Uint64(b[:8]), nil
}

// marshalNodeProp encodes a node property index entry:
// nodeID(8) + keyLen(2) + key(keyLen) + valLen(4) + val(valLen)
func marshalNodeProp(id store.NodeID, key string, value []byte) []byte {
	kl := len(key)
	vl := len(value)
	buf := make([]byte, 8+2+kl+4+vl)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(id))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(kl))
	copy(buf[10:10+kl], key)
	binary.LittleEndian.PutUint32(buf[10+kl:14+kl], uint32(vl))
	copy(buf[14+kl:], value)
	return buf
}

func unmarshalNodeProp(b []byte) (id store.NodeID, key string, value []byte, err error) {
	if len(b) < 14 {
		return 0, "", nil, fmt.Errorf("unmarshalNodeProp: too short")
	}
	id = store.NodeID(binary.LittleEndian.Uint64(b[0:8]))
	kl := int(binary.LittleEndian.Uint16(b[8:10]))
	if len(b) < 10+kl+4 {
		return 0, "", nil, fmt.Errorf("unmarshalNodeProp: truncated key")
	}
	key = string(b[10 : 10+kl])
	vl := int(binary.LittleEndian.Uint32(b[10+kl : 14+kl]))
	if len(b) < 14+kl+vl {
		return 0, "", nil, fmt.Errorf("unmarshalNodeProp: truncated value")
	}
	value = make([]byte, vl)
	copy(value, b[14+kl:])
	return id, key, value, nil
}

// marshalEdgeProp encodes an edge property index entry:
// edgeID(8) + keyLen(2) + key(keyLen) + valLen(4) + val(valLen)
func marshalEdgeProp(id store.EdgeID, key string, value []byte) []byte {
	kl := len(key)
	vl := len(value)
	buf := make([]byte, 8+2+kl+4+vl)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(id))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(kl))
	copy(buf[10:10+kl], key)
	binary.LittleEndian.PutUint32(buf[10+kl:14+kl], uint32(vl))
	copy(buf[14+kl:], value)
	return buf
}

func unmarshalEdgeProp(b []byte) (id store.EdgeID, key string, value []byte, err error) {
	if len(b) < 14 {
		return 0, "", nil, fmt.Errorf("unmarshalEdgeProp: too short")
	}
	id = store.EdgeID(binary.LittleEndian.Uint64(b[0:8]))
	kl := int(binary.LittleEndian.Uint16(b[8:10]))
	if len(b) < 10+kl+4 {
		return 0, "", nil, fmt.Errorf("unmarshalEdgeProp: truncated key")
	}
	key = string(b[10 : 10+kl])
	vl := int(binary.LittleEndian.Uint32(b[10+kl : 14+kl]))
	if len(b) < 14+kl+vl {
		return 0, "", nil, fmt.Errorf("unmarshalEdgeProp: truncated value")
	}
	value = make([]byte, vl)
	copy(value, b[14+kl:])
	return id, key, value, nil
}
