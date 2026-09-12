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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
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

	// The v2 leaf encodings, which commit to a *hash* of the property blob
	// rather than to the blob itself. Distinct tags rather than a version field
	// inside the leaf, so the two encodings can never be confused for one
	// another by a verifier that only has the bytes.
	leafTagNodeV2 = 0x05
	leafTagEdgeV2 = 0x06

	// leafTagProperties separates the property-blob hash, which is §11.2's
	// `SHA256(0x07 ‖ Properties)`.
	leafTagProperties = 0x07
)

// propertiesHash is the separated hash of an entity's property blob.
//
// Separating it is what makes a property-only redaction provable without
// revealing content: two leaves for the same entity, before and after, differ in
// exactly these 32 bytes and agree everywhere else — so a verifier can see that
// the ID and labels did not change without ever being shown what was removed.
// With the blob hashed inline there is no such comparison to make; the leaves
// simply differ, and nothing says where.
// It hashes the tag and the blob as two writes rather than concatenating them
// into a buffer first: a property blob can be tens of megabytes, and copying it
// to prefix one byte was the largest allocation on the leaf path.
func propertiesHash(props []byte) merkle.Hash {
	var le leafEncoder
	return le.propHash(props)
}

// GHSH section body versions, so roots can gain fields without disturbing the
// section directory around them.
//
// v2 adds TombstoneRoot, and adding it **changes what a snapshot root is**. That
// is why the version is carried in SnapshotRoots and consulted by
// bindSnapshotRoot rather than the new component simply being appended: a root
// retained outside the system is the one value SECURITY.md tells a holder to
// keep, and silently changing how it is computed would invalidate exactly that.
// A v1 image binds four components forever.
const (
	snapshotBodyV1 = 1
	snapshotBodyV2 = 2 // adds TombstoneRoot
	snapshotBodyV3 = 3 // separated property hashes in node and edge leaves (§11.2)

	// snapshotBodyVersion is what newly written images carry.
	snapshotBodyVersion = snapshotBodyV3
)

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

	// TombstoneRoot is the root over this image's record of deliberate removals
	// (tombstone.go). Zero when nothing has been redacted — and committed to as
	// zero, so "this image recorded no removals" is a claim the root makes
	// rather than an absence of one.
	//
	// Present only from BodyVersion v2.
	TombstoneRoot merkle.Hash

	// BodyVersion is the GHSH body version these roots came from, and it decides
	// how many components bind into Snapshot. Zero is read as v1, which is what
	// every SnapshotRoots value constructed before tombstones existed meant.
	BodyVersion uint8

	// Snapshot binds the component roots and the predecessor into one value.
	// This is the number worth publishing or retaining externally.
	Snapshot merkle.Hash
}

// Zero reports whether no roots have been computed.
func (s SnapshotRoots) Zero() bool { return s.Snapshot == merkle.Hash{} }

// leafEncoder builds leaf bytes into scratch it reuses, so hashing an image's
// worth of records allocates once rather than once per record.
//
// The old free functions allocated a fresh buffer and a fresh sorted label
// slice for every leaf. That was invisible while the leaves were being
// collected into a []merkle.Hash anyway — the slice dominated — and it is the
// whole cost once they are not.
//
// A returned slice is valid only until the next call on the same encoder. Every
// caller hands it straight to merkle.HashLeaf, which reads it and retains
// nothing, and an encoder belongs to exactly one pass, so the lifetime is the
// call.
type leafEncoder struct {
	buf     []byte
	nlabels []store.NodeType
	elabels []store.EdgeType
	h       hash.Hash
	sum     [merkle.Size]byte
}

// propTagBytes is the property-hash domain tag, as a slice a hasher can take
// without allocating one per call.
var propTagBytes = [...]byte{leafTagProperties}

// propHash is propertiesHash against the encoder's own hasher.
func (le *leafEncoder) propHash(props []byte) merkle.Hash {
	if le.h == nil {
		le.h = sha256.New()
	}
	le.h.Reset()
	le.h.Write(propTagBytes[:])
	le.h.Write(props)

	// Summed into a field rather than into a local array: Sum is an interface
	// call, so a local would escape and cost an allocation per record — the
	// thing this encoder exists to avoid.
	var out merkle.Hash
	copy(out[:], le.h.Sum(le.sum[:0]))
	return out
}

// node encodes a node leaf under the given body version: v3 and later commit to
// a hash of the property blob, earlier versions to the blob itself.
func (le *leafEncoder) node(version uint8, n nodeRecord) []byte {
	le.nlabels = append(le.nlabels[:0], n.Labels...)
	slices.Sort(le.nlabels)

	buf := le.buf[:0]
	if version >= snapshotBodyV3 {
		buf = append(buf, leafTagNodeV2)
	} else {
		buf = append(buf, leafTagNode)
	}
	buf = binary.LittleEndian.AppendUint64(buf, uint64(n.ID))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(le.nlabels)))
	for _, l := range le.nlabels {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(l))
	}
	if version >= snapshotBodyV3 {
		h := le.propHash(n.Properties)
		buf = append(buf, h[:]...)
	} else {
		// Length-prefixed so a property blob cannot be shifted into an adjacent
		// field: without it, moving a byte between labels and properties would
		// leave the concatenation unchanged.
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(n.Properties)))
		buf = append(buf, n.Properties...)
	}
	le.buf = buf
	return buf
}

// edge encodes an edge leaf under the given body version.
func (le *leafEncoder) edge(version uint8, e rawEdge) []byte {
	le.elabels = append(le.elabels[:0], e.Labels...)
	slices.Sort(le.elabels)

	buf := le.buf[:0]
	if version >= snapshotBodyV3 {
		buf = append(buf, leafTagEdgeV2)
	} else {
		buf = append(buf, leafTagEdge)
	}
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.ID))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Src))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Dst))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(le.elabels)))
	for _, l := range le.elabels {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(l))
	}
	buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(e.Weight))
	if version >= snapshotBodyV3 {
		h := le.propHash(e.Properties)
		buf = append(buf, h[:]...)
	} else {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Properties)))
		buf = append(buf, e.Properties...)
	}
	le.buf = buf
	return buf
}

// propEntry encodes a property-index entry leaf.
func (le *leafEncoder) propEntry(id uint64, key string, val []byte) []byte {
	buf := le.buf[:0]
	buf = append(buf, leafTagPropEntry)
	buf = binary.LittleEndian.AppendUint64(buf, id)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
	buf = append(buf, key...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(val)))
	buf = append(buf, val...)
	le.buf = buf
	return buf
}

// nodeLeafData returns the canonical bytes hashed for a node record.
func nodeLeafData(n nodeRecord) []byte {
	var le leafEncoder
	return le.node(snapshotBodyV1, n)
}

// edgeLeafData returns the canonical bytes hashed for an edge record.
func edgeLeafData(e rawEdge) []byte {
	var le leafEncoder
	return le.edge(snapshotBodyV1, e)
}

// nodeLeafDataV2 is nodeLeafData with the property blob replaced by its hash.
//
// Everything before the final 32 bytes is the entity's identity — ID and sorted
// labels — so two v2 leaves for the same node share a byte-identical prefix and
// differ only in the property hash. That equality is the thing a property
// redaction needs to be able to demonstrate.
func nodeLeafDataV2(n nodeRecord) []byte {
	var le leafEncoder
	return le.node(snapshotBodyV3, n)
}

// edgeLeafDataV2 is edgeLeafData with the property blob replaced by its hash.
func edgeLeafDataV2(e rawEdge) []byte {
	var le leafEncoder
	return le.edge(snapshotBodyV3, e)
}

// nodeLeafFor and edgeLeafFor pick the encoding a given snapshot body version
// uses.
//
// Routed through one place because the writer, the prover and the verifier must
// agree: an image whose root was computed from v1 leaves and whose proofs are
// built from v2 leaves produces proofs that resolve to nothing.
func nodeLeafFor(version uint8, n nodeRecord) []byte {
	var le leafEncoder
	return le.node(version, n)
}

func edgeLeafFor(version uint8, e rawEdge) []byte {
	var le leafEncoder
	return le.edge(version, e)
}

// NodeLeaves returns the leaf hash of every live node, in ascending ID order,
// under the given snapshot body version's leaf encoding.
//
// Order is the file's own record order, which canonical serialisation already
// fixes. A root computed from a different order would be a different root, so
// this and the writer must agree — they do by both iterating Nodes.
//
// This materialises one hash per node, which is what an inclusion proof needs
// and what a root does not. The root is computed by snapshotRootStream instead;
// this is for prove.go, which has to hand merkle.BuildProof the whole level.
func (g *CSRGraph) NodeLeaves(version uint8) []merkle.Hash {
	out := make([]merkle.Hash, 0, g.NodeCount())
	var le leafEncoder
	for n := range g.Nodes() {
		out = append(out, merkle.HashLeaf(le.node(version, n)))
	}
	return out
}

// EdgeLeaves returns the leaf hash of every live edge, in ascending ID order.
//
// It hashes the v1 encoding whatever version says, which is a bug of record:
// node leaves moved to the separated property hash at snapshotBodyV3 and edge
// leaves did not follow. It is left alone deliberately. The EdgeRoot of every
// v3 image ever written was computed this way, so "fixing" it would change the
// identity of every existing snapshot — a retained root is the one value
// SECURITY.md tells a holder to keep, and this would silently invalidate all of
// them. Changing it is a snapshotBodyV4, not a repair.
func (g *CSRGraph) EdgeLeaves(version uint8) []merkle.Hash {
	out := make([]merkle.Hash, 0, g.EdgeCount())
	var le leafEncoder
	for e := range g.Edges() {
		out = append(out, merkle.HashLeaf(le.edge(edgeLeafVersion(version), e)))
	}
	return out
}

// edgeLeafVersion is the encoding edge leaves are actually hashed under, which
// is v1 at every body version. One function so that the writer, the streaming
// root and EdgeLeaves cannot drift apart on it, and so that the day it becomes
// a v4 there is one place to change.
func edgeLeafVersion(uint8) uint8 { return snapshotBodyV1 }

// snapshotRootStream computes the component roots as the records stream past on
// their way into the image, rather than from arrays of leaves.
//
// The arrays were the largest allocation in the whole compaction after the
// image buffer itself: 32 bytes per node, per edge and per property-index
// entry, all live at once, all folded into ninety-six bytes and dropped. A
// merkle.RootBuilder retains one hash per set bit of the count instead, so the
// three of these together are under two hundred hashes for any store that
// exists.
//
// The order leaves arrive in is the order the writer writes the records in,
// which is the order NodeLeaves/EdgeLeaves/propEntryLeaves produced them in, so
// the roots are unchanged. computeSnapshotRootsAs is the same walk driven from
// a finished graph, which is how verification re-derives an image it did not
// write.
type snapshotRootStream struct {
	version uint8
	nodes   merkle.RootBuilder
	edges   merkle.RootBuilder
	index   merkle.RootBuilder
	enc     leafEncoder
}

func newSnapshotRootStream(version uint8) *snapshotRootStream {
	return &snapshotRootStream{version: version}
}

func (s *snapshotRootStream) addNode(n nodeRecord) {
	s.nodes.AddLeafData(s.enc.node(s.version, n))
}

func (s *snapshotRootStream) addEdge(e rawEdge) {
	s.edges.AddLeafData(s.enc.edge(edgeLeafVersion(s.version), e))
}

func (s *snapshotRootStream) addPropEntry(id uint64, key string, val []byte) {
	s.index.AddLeafData(s.enc.propEntry(id, key, val))
}

// finish binds the three streamed roots with the tombstone root and the
// predecessor. Tombstones stay materialised: there is one per deliberate
// removal, which is a human-scale number, and they are hashed from a slice the
// caller already holds.
func (s *snapshotRootStream) finish(tombstones []Tombstone, prev merkle.Hash) SnapshotRoots {
	r := SnapshotRoots{
		NodeRoot:      s.nodes.Root(),
		EdgeRoot:      s.edges.Root(),
		IndexRoot:     s.index.Root(),
		PrevRoot:      prev,
		TombstoneRoot: merkle.Root(tombstoneLeaves(tombstones)),
		BodyVersion:   s.version,
	}
	r.Snapshot = bindSnapshotRoot(r)
	return r
}

// computeSnapshotRootsAs builds the roots under a specific body version.
//
// Verification needs this: an existing image must be re-derived the way it was
// written, not the way this build would write it now. It drives the same stream
// the writer drives, from a graph and a payload already in hand, so a
// verification costs the same constant memory a compaction does.
func computeSnapshotRootsAs(version uint8, g *CSRGraph, payload csrPayload, prev merkle.Hash) SnapshotRoots {
	payload = payload.withPropStreams()
	s := newSnapshotRootStream(version)
	for n := range g.Nodes() {
		s.addNode(n)
	}
	for e := range g.Edges() {
		s.addEdge(e)
	}
	// Nodes then edges, which is the order the GIDX section is written in and
	// therefore the order the index leaves were always hashed in.
	for e := range payload.NodeProps {
		s.addPropEntry(uint64(e.ID), e.Key, e.Value)
	}
	for e := range payload.EdgeProps {
		s.addPropEntry(uint64(e.ID), e.Key, e.Value)
	}
	return s.finish(payload.Tombstones, prev)
}

// bindSnapshotRoot combines the component roots into the snapshot's identity.
//
// Built as a Merkle root over the components rather than a plain concatenation,
// so the same domain separation applies and no component can be shifted into
// another's position.
//
// **The component count depends on BodyVersion**, which is what keeps a v1
// image's retained root verifiable by a build that knows about tombstones. The
// version needs no separate protection: choosing the wrong one produces a
// different bound value, so a proof claiming v1 for a v2 image simply fails to
// reproduce the root it is checked against.
func bindSnapshotRoot(r SnapshotRoots) merkle.Hash {
	leaves := []merkle.Hash{
		merkle.HashLeaf(r.NodeRoot[:]),
		merkle.HashLeaf(r.EdgeRoot[:]),
		merkle.HashLeaf(r.IndexRoot[:]),
		merkle.HashLeaf(r.PrevRoot[:]),
	}
	if r.bodyVersion() >= snapshotBodyV2 {
		leaves = append(leaves, merkle.HashLeaf(r.TombstoneRoot[:]))
	}
	return merkle.Root(leaves)
}

// bodyVersion reads a zero BodyVersion as v1.
//
// Every SnapshotRoots value constructed before tombstones existed leaves the
// field at zero and means four components, so that is what zero has to mean.
// Routed through one helper because binding and encoding must never disagree
// about it: a value bound as v1 and written as v2 produces a section that fails
// its own consistency check, which is how this was found.
func (r SnapshotRoots) bodyVersion() uint8 {
	if r.BodyVersion == 0 {
		return snapshotBodyV1
	}
	return r.BodyVersion
}

// --- GHSH section encoding ---

const (
	snapshotSectionSizeV1 = 1 + 5*merkle.Size
	snapshotSectionSizeV2 = snapshotSectionSizeV1 + merkle.Size

	// v3 changes the leaf encoding, not the section's fields, so it is the same
	// size as v2. The version still has to be carried: it decides how a
	// recomputation hashes records, which is not something the bytes reveal.
	snapshotSectionSizeV3 = snapshotSectionSizeV2
)

// appendSnapshotSection writes the layout matching r's own body version, not
// whatever version this build prefers — so what is written is always what was
// bound.
func appendSnapshotSection(buf []byte, r SnapshotRoots) []byte {
	v := r.bodyVersion()
	buf = append(buf, v)
	buf = append(buf, r.NodeRoot[:]...)
	buf = append(buf, r.EdgeRoot[:]...)
	buf = append(buf, r.IndexRoot[:]...)
	buf = append(buf, r.PrevRoot[:]...)
	if v >= snapshotBodyV2 {
		buf = append(buf, r.TombstoneRoot[:]...)
	}
	return append(buf, r.Snapshot[:]...)
}

// readSnapshotSection parses a GHSH body of either version.
//
// v1 is still accepted rather than rejected as stale: an older image's roots are
// not wrong, they simply commit to less, and a build that refused them would
// make every pre-tombstone store unverifiable by the tool that is supposed to
// verify it.
func readSnapshotSection(data []byte) (SnapshotRoots, error) {
	var r SnapshotRoots
	if len(data) < 1 {
		return r, fmt.Errorf("empty snapshot section")
	}

	var want int
	switch v := data[0]; v {
	case snapshotBodyV1:
		r.BodyVersion, want = snapshotBodyV1, snapshotSectionSizeV1
	case snapshotBodyV2:
		r.BodyVersion, want = snapshotBodyV2, snapshotSectionSizeV2
	case snapshotBodyV3:
		r.BodyVersion, want = snapshotBodyV3, snapshotSectionSizeV3
	default:
		return r, fmt.Errorf("snapshot section version %d, this build understands %d to %d",
			v, snapshotBodyV1, snapshotBodyV3)
	}
	if len(data) < want {
		return r, fmt.Errorf("truncated snapshot section: %d bytes, need %d", len(data), want)
	}

	fields := []*merkle.Hash{&r.NodeRoot, &r.EdgeRoot, &r.IndexRoot, &r.PrevRoot}
	if r.BodyVersion >= snapshotBodyV2 {
		fields = append(fields, &r.TombstoneRoot)
	}
	fields = append(fields, &r.Snapshot)

	pos := 1
	for _, dst := range fields {
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
