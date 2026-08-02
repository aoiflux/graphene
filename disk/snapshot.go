package disk

// Snapshot roots: a Merkle identity for a compacted image, and inclusion proofs
// against it.
//
// The digest added in v8 says whether a file changed. It cannot say anything
// about one entity inside it — checking that an artefact is present means
// handing over the whole file. A Merkle root says the same thing about the file
// AND supports a proof, a few hundred bytes long, that a specific entity was in
// it. That proof is checkable by someone holding the root and nothing else,
// which is what makes evidence transferable rather than merely verifiable in
// place.
//
// # What is hashed, and what is not
//
// An entity's leaf covers its ID, labels, and properties. An edge's also covers
// its endpoints — as IDs, not as their version hashes. That is Q11, decided on
// the measurement in the build log: version-binding cost ~1150x more on a hub
// node because changing a node invalidates every incident edge's hash, and the
// cost scales with the graph. ID-binding detects a changed node through the
// node's own leaf and through the snapshot root; what it gives up is detecting
// it from an edge in isolation. Adding version-binding later is a new section,
// which v8 supports without a format change.
//
// Labels are sorted into the hash input. They are a set semantically, so two
// records differing only in label order describe the same entity and must not
// produce different roots. The stored order is left alone — this is a hashing
// rule, not a storage one.

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// Domain tags separating what a leaf describes. Without these an edge leaf and a
// node leaf could in principle be made to collide, which would let one be
// presented as the other.
const (
	leafTagNode      = 0x01
	leafTagEdge      = 0x02
	leafTagPropEntry = 0x03
)

// snapshotBodyVersion versions the GHSH section body, so roots can gain fields
// later without disturbing the section directory around them.
const snapshotBodyVersion = 1

// SnapshotRoots is the Merkle identity of one compacted image.
type SnapshotRoots struct {
	// Roots over the node records, the edge records, and the property-index
	// entries, each in the canonical order the file stores them in.
	NodeRoot  merkle.Hash
	EdgeRoot  merkle.Hash
	IndexRoot merkle.Hash

	// PrevRoot is the Snapshot root of the image this one replaced, zero for the
	// first. Chaining them means a substituted snapshot breaks the link even when
	// the substitute is internally consistent — an isolated root proves only that
	// a file is coherent, not that it belongs in this store's history.
	PrevRoot merkle.Hash

	// Snapshot binds the three roots and the predecessor into one value. This is
	// the number worth publishing or retaining externally.
	Snapshot merkle.Hash
}

// Zero reports whether no roots have been computed.
func (s SnapshotRoots) Zero() bool { return s.Snapshot == merkle.Hash{} }

// nodeLeafData returns the canonical bytes hashed for a node record.
func nodeLeafData(n nodeRecord) []byte {
	labels := slices.Clone(n.Labels)
	slices.Sort(labels)

	buf := make([]byte, 0, 1+8+2+len(labels)*2+4+len(n.Properties))
	buf = append(buf, leafTagNode)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(n.ID))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(labels)))
	for _, l := range labels {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(l))
	}
	// Length-prefixed so a property blob cannot be shifted into an adjacent
	// field: without it, moving a byte between labels and properties would leave
	// the concatenation unchanged.
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(n.Properties)))
	return append(buf, n.Properties...)
}

// edgeLeafData returns the canonical bytes hashed for an edge record.
func edgeLeafData(e rawEdge) []byte {
	labels := slices.Clone(e.Labels)
	slices.Sort(labels)

	buf := make([]byte, 0, 1+24+2+len(labels)*2+4+4+len(e.Properties))
	buf = append(buf, leafTagEdge)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.ID))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Src))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Dst))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(labels)))
	for _, l := range labels {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(l))
	}
	buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(e.Weight))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Properties)))
	return append(buf, e.Properties...)
}

// NodeLeaves returns the leaf hash of every live node, in ascending ID order.
//
// Order is the file's own record order, which canonical serialisation already
// fixes. A root computed from a different order would be a different root, so
// this and the writer must agree — they do by both walking the ID-indexed array.
func (g *CSRGraph) NodeLeaves() []merkle.Hash {
	out := make([]merkle.Hash, 0, len(g.nodes))
	for i := 1; i < len(g.nodes); i++ {
		if g.nodes[i].ID == store.InvalidNodeID {
			continue
		}
		out = append(out, merkle.HashLeaf(nodeLeafData(g.nodes[i])))
	}
	return out
}

// EdgeLeaves returns the leaf hash of every live edge, in ascending ID order.
func (g *CSRGraph) EdgeLeaves() []merkle.Hash {
	out := make([]merkle.Hash, 0, len(g.edges))
	for i := 1; i < len(g.edges); i++ {
		if g.edges[i].ID == store.InvalidEdgeID {
			continue
		}
		out = append(out, merkle.HashLeaf(edgeLeafData(g.edges[i])))
	}
	return out
}

// nodeLeafIndex returns the position of id among the live nodes, which is the
// index an inclusion proof is built at.
func (g *CSRGraph) nodeLeafIndex(id store.NodeID) (int, bool) {
	if int(id) >= len(g.nodes) || g.nodes[id].ID == store.InvalidNodeID {
		return 0, false
	}
	pos := 0
	for i := 1; i < int(id); i++ {
		if g.nodes[i].ID != store.InvalidNodeID {
			pos++
		}
	}
	return pos, true
}

// edgeLeafIndex is nodeLeafIndex for edges.
func (g *CSRGraph) edgeLeafIndex(id store.EdgeID) (int, bool) {
	if int(id) >= len(g.edges) || g.edges[id].ID == store.InvalidEdgeID {
		return 0, false
	}
	pos := 0
	for i := 1; i < int(id); i++ {
		if g.edges[i].ID != store.InvalidEdgeID {
			pos++
		}
	}
	return pos, true
}

// propEntryLeaves returns leaf hashes over the property-index entries, in the
// canonical order NodeEntries/EdgeEntries already guarantee.
func propEntryLeaves(nodeCount, edgeCount int, appendEntry func(i int, node bool) []byte) []merkle.Hash {
	out := make([]merkle.Hash, 0, nodeCount+edgeCount)
	for i := 0; i < nodeCount; i++ {
		out = append(out, merkle.HashLeaf(appendEntry(i, true)))
	}
	for i := 0; i < edgeCount; i++ {
		out = append(out, merkle.HashLeaf(appendEntry(i, false)))
	}
	return out
}

// computeSnapshotRoots builds the roots for an image about to be written.
func computeSnapshotRoots(g *CSRGraph, payload csrPayload, prev merkle.Hash) SnapshotRoots {
	r := SnapshotRoots{
		NodeRoot: merkle.Root(g.NodeLeaves()),
		EdgeRoot: merkle.Root(g.EdgeLeaves()),
		PrevRoot: prev,
	}

	idxLeaves := propEntryLeaves(len(payload.NodeProps), len(payload.EdgeProps), func(i int, node bool) []byte {
		var id uint64
		var key string
		var val []byte
		if node {
			e := payload.NodeProps[i]
			id, key, val = uint64(e.ID), e.Key, e.Value
		} else {
			e := payload.EdgeProps[i]
			id, key, val = uint64(e.ID), e.Key, e.Value
		}
		buf := make([]byte, 0, 1+8+2+len(key)+4+len(val))
		buf = append(buf, leafTagPropEntry)
		buf = binary.LittleEndian.AppendUint64(buf, id)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(val)))
		return append(buf, val...)
	})
	r.IndexRoot = merkle.Root(idxLeaves)
	r.Snapshot = bindSnapshotRoot(r)
	return r
}

// bindSnapshotRoot combines the component roots into the snapshot's identity.
//
// Built as a Merkle root over the four rather than a plain concatenation, so the
// same domain separation applies and no component can be shifted into another's
// position.
func bindSnapshotRoot(r SnapshotRoots) merkle.Hash {
	return merkle.Root([]merkle.Hash{
		merkle.HashLeaf(r.NodeRoot[:]),
		merkle.HashLeaf(r.EdgeRoot[:]),
		merkle.HashLeaf(r.IndexRoot[:]),
		merkle.HashLeaf(r.PrevRoot[:]),
	})
}

// --- GHSH section encoding ---

const snapshotSectionSize = 1 + 5*merkle.Size

func appendSnapshotSection(buf []byte, r SnapshotRoots) []byte {
	buf = append(buf, snapshotBodyVersion)
	buf = append(buf, r.NodeRoot[:]...)
	buf = append(buf, r.EdgeRoot[:]...)
	buf = append(buf, r.IndexRoot[:]...)
	buf = append(buf, r.PrevRoot[:]...)
	return append(buf, r.Snapshot[:]...)
}

func readSnapshotSection(data []byte) (SnapshotRoots, error) {
	var r SnapshotRoots
	if len(data) < 1 {
		return r, fmt.Errorf("empty snapshot section")
	}
	if v := data[0]; v != snapshotBodyVersion {
		return r, fmt.Errorf("snapshot section version %d, this build understands %d", v, snapshotBodyVersion)
	}
	if len(data) < snapshotSectionSize {
		return r, fmt.Errorf("truncated snapshot section: %d bytes, need %d", len(data), snapshotSectionSize)
	}
	pos := 1
	for _, dst := range []*merkle.Hash{&r.NodeRoot, &r.EdgeRoot, &r.IndexRoot, &r.PrevRoot, &r.Snapshot} {
		copy(dst[:], data[pos:pos+merkle.Size])
		pos += merkle.Size
	}

	// The bound root must agree with its components. A file whose Snapshot does
	// not follow from the four values beside it is inconsistent with itself, and
	// accepting it would mean publishing a root that proves nothing about the
	// entities the same file claims.
	if want := bindSnapshotRoot(r); want != r.Snapshot {
		return SnapshotRoots{}, fmt.Errorf("snapshot root does not follow from its components")
	}
	return r, nil
}
