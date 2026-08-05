package disk

// Exporting a proof, so evidence can leave the process that holds it.
//
// Every proof this package builds — inclusion, removal, property redaction — is
// designed to be checked by someone holding a root and nothing else. Until now
// none of them could actually reach that person: they were Go structs, so
// "transferable" meant "transferable to another Go program that imports this
// package". A proof that cannot be handed over is not evidence, it is a claim
// about evidence.
//
// This file gives them a byte encoding and a verifier that needs no store, no
// image and no directory — only the bytes and a root the verifier obtained
// independently.
//
// # What travels, and what does not
//
// A proof carries the leaf data it commits to, the sibling hashes, and the
// component roots. It does **not** carry the store, the other entities, or
// anything about them: that is the whole point of a Merkle proof, and it is what
// makes handing one over safe when handing over the image would not be.
//
// A redaction proof carries less still — a tombstone names an entity and a
// digest, never a reason or an actor. See tombstone.go.
//
// # The root does not travel
//
// Deliberately. A proof file carries the roots it *claims*, and the verifier
// checks the proof against a root supplied separately. Bundling the two would
// make verification circular: whoever wrote the file would be asserting both the
// evidence and the standard it is judged against. The root has to come from
// somewhere the file's author does not control — a published checkpoint, a
// co-signed attestation, a value written down when the evidence was collected.
//
// VerifyExportedProof therefore takes the root as an argument and there is no
// form that does not.

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// proofMagic identifies an exported proof file.
const proofMagic = "GPRF"

// proofFormatVersion versions the container.
const proofFormatVersion = 1

// ProofKind says what an exported proof asserts.
type ProofKind uint8

const (
	// ProofKindNodeInclusion asserts an entity was in a snapshot.
	ProofKindNodeInclusion ProofKind = 1

	// ProofKindRedaction asserts a snapshot records an entity as deliberately
	// removed.
	ProofKindRedaction ProofKind = 2

	// ProofKindPropertyRedaction asserts an entity's properties were removed and
	// nothing else about it changed.
	ProofKindPropertyRedaction ProofKind = 3
)

func (k ProofKind) String() string {
	switch k {
	case ProofKindNodeInclusion:
		return "node-inclusion"
	case ProofKindRedaction:
		return "redaction"
	case ProofKindPropertyRedaction:
		return "property-redaction"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(k))
	}
}

// ErrProofMalformed reports a file that is not a readable proof, as distinct
// from one that reads cleanly and fails to verify. The two call for different
// responses and collapsing them loses that.
var ErrProofMalformed = errors.New("disk: malformed proof file")

// ExportedProof is a proof in transit.
type ExportedProof struct {
	Kind ProofKind

	// Exactly one of these is populated, per Kind.
	Node     *NodeInclusionProof
	Removal  *RedactionInclusionProof
	Property *PropertyRedactionProof
}

// Subject names what the proof is about, for a human reading a listing.
func (e ExportedProof) Subject() string {
	switch {
	case e.Node != nil:
		return fmt.Sprintf("node %d", e.Node.NodeID)
	case e.Property != nil:
		return fmt.Sprintf("node %d", e.Property.NodeID)
	case e.Removal != nil && e.Removal.Tombstone.EdgeID != 0:
		return fmt.Sprintf("edge %d", e.Removal.Tombstone.EdgeID)
	case e.Removal != nil:
		return fmt.Sprintf("node %d", e.Removal.Tombstone.NodeID)
	default:
		return "nothing"
	}
}

// --- encoding primitives ---
//
// Every variable-length field is length-prefixed and every count is bounded
// against the buffer before anything is allocated. This file parses bytes
// supplied by whoever handed over the proof, which is by definition not someone
// the verifier trusts — that is the situation a proof exists for.

func putBytes(buf, b []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}

type proofReader struct {
	b   []byte
	off int
}

func (r *proofReader) u8() (uint8, error) {
	if r.off+1 > len(r.b) {
		return 0, ErrProofMalformed
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}

func (r *proofReader) u32() (uint32, error) {
	if r.off+4 > len(r.b) {
		return 0, ErrProofMalformed
	}
	v := binary.LittleEndian.Uint32(r.b[r.off : r.off+4])
	r.off += 4
	return v, nil
}

func (r *proofReader) u64() (uint64, error) {
	if r.off+8 > len(r.b) {
		return 0, ErrProofMalformed
	}
	v := binary.LittleEndian.Uint64(r.b[r.off : r.off+8])
	r.off += 8
	return v, nil
}

func (r *proofReader) hash() (merkle.Hash, error) {
	var h merkle.Hash
	if r.off+merkle.Size > len(r.b) {
		return h, ErrProofMalformed
	}
	copy(h[:], r.b[r.off:r.off+merkle.Size])
	r.off += merkle.Size
	return h, nil
}

func (r *proofReader) bytes() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	// Bounded against what remains, so a twelve-byte file cannot ask for a
	// gigabyte. Same discipline as every other length prefix read from a file
	// in this package.
	if int(n) > len(r.b)-r.off {
		return nil, fmt.Errorf("%w: a %d-byte field does not fit in %d remaining bytes",
			ErrProofMalformed, n, len(r.b)-r.off)
	}
	out := make([]byte, n)
	copy(out, r.b[r.off:r.off+int(n)])
	r.off += int(n)
	return out, nil
}

// --- roots ---

func appendRoots(buf []byte, r SnapshotRoots) []byte {
	buf = append(buf, r.bodyVersion())
	buf = append(buf, r.NodeRoot[:]...)
	buf = append(buf, r.EdgeRoot[:]...)
	buf = append(buf, r.IndexRoot[:]...)
	buf = append(buf, r.PrevRoot[:]...)
	buf = append(buf, r.TombstoneRoot[:]...)
	return append(buf, r.Snapshot[:]...)
}

func (r *proofReader) roots() (SnapshotRoots, error) {
	var out SnapshotRoots
	v, err := r.u8()
	if err != nil {
		return out, err
	}
	out.BodyVersion = v
	for _, dst := range []*merkle.Hash{
		&out.NodeRoot, &out.EdgeRoot, &out.IndexRoot,
		&out.PrevRoot, &out.TombstoneRoot, &out.Snapshot,
	} {
		h, herr := r.hash()
		if herr != nil {
			return SnapshotRoots{}, herr
		}
		*dst = h
	}
	return out, nil
}

// --- merkle proofs ---

func appendMerkleProof(buf []byte, p merkle.Proof) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(p.Index))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(p.Size))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(p.Siblings)))
	for _, h := range p.Siblings {
		buf = append(buf, h[:]...)
	}
	return buf
}

func (r *proofReader) merkleProof() (merkle.Proof, error) {
	var p merkle.Proof
	idx, err := r.u32()
	if err != nil {
		return p, err
	}
	size, err := r.u32()
	if err != nil {
		return p, err
	}
	n, err := r.u32()
	if err != nil {
		return p, err
	}
	if int(n) > (len(r.b)-r.off)/merkle.Size {
		return p, fmt.Errorf("%w: %d siblings do not fit in %d remaining bytes",
			ErrProofMalformed, n, len(r.b)-r.off)
	}
	p.Index, p.Size = int(idx), int(size)
	p.Siblings = make([]merkle.Hash, n)
	for i := range p.Siblings {
		h, herr := r.hash()
		if herr != nil {
			return merkle.Proof{}, herr
		}
		p.Siblings[i] = h
	}
	return p, nil
}

// --- tombstones ---

func appendTombstoneRecord(buf []byte, t Tombstone) []byte {
	buf = append(buf, byte(t.Scope))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.NodeID))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.EdgeID))
	buf = append(buf, t.VersionHash[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, t.RedactionSeq)
	return append(buf, t.RedactionHash[:]...)
}

func (r *proofReader) tombstone() (Tombstone, error) {
	var t Tombstone
	scope, err := r.u8()
	if err != nil {
		return t, err
	}
	t.Scope = RedactionScope(scope)
	nid, err := r.u64()
	if err != nil {
		return t, err
	}
	t.NodeID = store.NodeID(nid)
	eid, err := r.u64()
	if err != nil {
		return t, err
	}
	t.EdgeID = store.EdgeID(eid)
	if t.VersionHash, err = r.hash(); err != nil {
		return Tombstone{}, err
	}
	if t.RedactionSeq, err = r.u64(); err != nil {
		return Tombstone{}, err
	}
	h, err := r.hash()
	if err != nil {
		return Tombstone{}, err
	}
	copy(t.RedactionHash[:], h[:])
	return t, nil
}

// --- the whole proofs ---

func appendNodeProof(buf []byte, p NodeInclusionProof) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, uint64(p.NodeID))
	buf = putBytes(buf, p.LeafData)
	buf = appendMerkleProof(buf, p.Proof)
	buf = append(buf, p.NodeRoot[:]...)
	return appendRoots(buf, p.Roots)
}

func (r *proofReader) nodeProof() (NodeInclusionProof, error) {
	var p NodeInclusionProof
	id, err := r.u64()
	if err != nil {
		return p, err
	}
	p.NodeID = store.NodeID(id)
	if p.LeafData, err = r.bytes(); err != nil {
		return p, err
	}
	if p.Proof, err = r.merkleProof(); err != nil {
		return p, err
	}
	if p.NodeRoot, err = r.hash(); err != nil {
		return p, err
	}
	if p.Roots, err = r.roots(); err != nil {
		return p, err
	}
	return p, nil
}

func appendRemovalProof(buf []byte, p RedactionInclusionProof) []byte {
	buf = appendTombstoneRecord(buf, p.Tombstone)
	buf = putBytes(buf, p.LeafData)
	buf = appendMerkleProof(buf, p.Proof)
	buf = append(buf, p.TombstoneRoot[:]...)
	return appendRoots(buf, p.Roots)
}

func (r *proofReader) removalProof() (RedactionInclusionProof, error) {
	var p RedactionInclusionProof
	var err error
	if p.Tombstone, err = r.tombstone(); err != nil {
		return p, err
	}
	if p.LeafData, err = r.bytes(); err != nil {
		return p, err
	}
	if p.Proof, err = r.merkleProof(); err != nil {
		return p, err
	}
	if p.TombstoneRoot, err = r.hash(); err != nil {
		return p, err
	}
	if p.Roots, err = r.roots(); err != nil {
		return p, err
	}
	return p, nil
}

// MarshalProof encodes a proof for transit.
func MarshalProof(e ExportedProof) ([]byte, error) {
	buf := make([]byte, 0, 512)
	buf = append(buf, proofMagic...)
	buf = append(buf, proofFormatVersion)
	buf = append(buf, byte(e.Kind))

	switch e.Kind {
	case ProofKindNodeInclusion:
		if e.Node == nil {
			return nil, errors.New("disk: node-inclusion proof is empty")
		}
		buf = appendNodeProof(buf, *e.Node)
	case ProofKindRedaction:
		if e.Removal == nil {
			return nil, errors.New("disk: redaction proof is empty")
		}
		buf = appendRemovalProof(buf, *e.Removal)
	case ProofKindPropertyRedaction:
		if e.Property == nil {
			return nil, errors.New("disk: property-redaction proof is empty")
		}
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Property.NodeID))
		buf = appendNodeProof(buf, e.Property.Surviving)
		buf = appendRemovalProof(buf, e.Property.Removal)
		buf = putBytes(buf, e.Property.PriorLeafData)
	default:
		return nil, fmt.Errorf("disk: cannot marshal proof kind %s", e.Kind)
	}
	return buf, nil
}

// UnmarshalProof decodes a proof received from someone else.
//
// Decoding says nothing about whether the proof is true — that is
// VerifyExportedProof's job, and it needs a root this function never sees.
func UnmarshalProof(data []byte) (ExportedProof, error) {
	var e ExportedProof
	if len(data) < 6 || string(data[:4]) != proofMagic {
		return e, fmt.Errorf("%w: not a %s file", ErrProofMalformed, proofMagic)
	}
	if v := data[4]; v != proofFormatVersion {
		return e, fmt.Errorf("%w: format version %d, this build understands %d",
			ErrProofMalformed, v, proofFormatVersion)
	}
	e.Kind = ProofKind(data[5])
	r := &proofReader{b: data, off: 6}

	switch e.Kind {
	case ProofKindNodeInclusion:
		p, err := r.nodeProof()
		if err != nil {
			return ExportedProof{}, err
		}
		e.Node = &p
	case ProofKindRedaction:
		p, err := r.removalProof()
		if err != nil {
			return ExportedProof{}, err
		}
		e.Removal = &p
	case ProofKindPropertyRedaction:
		var p PropertyRedactionProof
		id, err := r.u64()
		if err != nil {
			return ExportedProof{}, err
		}
		p.NodeID = store.NodeID(id)
		if p.Surviving, err = r.nodeProof(); err != nil {
			return ExportedProof{}, err
		}
		if p.Removal, err = r.removalProof(); err != nil {
			return ExportedProof{}, err
		}
		if p.PriorLeafData, err = r.bytes(); err != nil {
			return ExportedProof{}, err
		}
		e.Property = &p
	default:
		return ExportedProof{}, fmt.Errorf("%w: unknown proof kind %d", ErrProofMalformed, uint8(e.Kind))
	}

	// Trailing bytes mean the file is not what it claims. Ignoring them would let
	// a proof carry an unverified payload alongside a verified one.
	if r.off != len(data) {
		return ExportedProof{}, fmt.Errorf("%w: %d unread bytes after the proof",
			ErrProofMalformed, len(data)-r.off)
	}
	return e, nil
}

// VerifyExportedProof checks a decoded proof against a snapshot root the
// verifier obtained independently.
//
// Needs no store, no image and no directory — which is the property the whole
// exercise exists for. A recipient has the bytes and a root, and that is enough.
func VerifyExportedProof(snapshotRoot merkle.Hash, e ExportedProof) error {
	switch e.Kind {
	case ProofKindNodeInclusion:
		if e.Node == nil {
			return errors.New("verify proof: no node proof present")
		}
		return VerifyNodeInclusion(snapshotRoot, *e.Node)
	case ProofKindRedaction:
		if e.Removal == nil {
			return errors.New("verify proof: no removal proof present")
		}
		return VerifyRedactionInclusion(snapshotRoot, *e.Removal)
	case ProofKindPropertyRedaction:
		if e.Property == nil {
			return errors.New("verify proof: no property-redaction proof present")
		}
		return VerifyPropertyRedaction(snapshotRoot, *e.Property)
	default:
		return fmt.Errorf("verify proof: unknown kind %s", e.Kind)
	}
}

// --- building one from a store ---

// ExportNodeProof builds and encodes an inclusion proof for a node.
func (s *Store) ExportNodeProof(id store.NodeID) ([]byte, error) {
	p, err := s.ProveNode(id)
	if err != nil {
		return nil, err
	}
	return MarshalProof(ExportedProof{Kind: ProofKindNodeInclusion, Node: &p})
}

// ExportRedactionProof builds and encodes a removal proof for a node.
func (s *Store) ExportRedactionProof(id store.NodeID) ([]byte, error) {
	p, err := s.ProveRedaction(id)
	if err != nil {
		return nil, err
	}
	return MarshalProof(ExportedProof{Kind: ProofKindRedaction, Removal: &p})
}

// ExportEdgeRedactionProof builds and encodes a removal proof for an edge.
func (s *Store) ExportEdgeRedactionProof(id store.EdgeID) ([]byte, error) {
	p, err := s.ProveEdgeRedaction(id)
	if err != nil {
		return nil, err
	}
	return MarshalProof(ExportedProof{Kind: ProofKindRedaction, Removal: &p})
}

// ExportPropertyRedactionProof builds and encodes a content-free proof that a
// node's properties were removed and nothing else about it changed.
func (s *Store) ExportPropertyRedactionProof(id store.NodeID) ([]byte, error) {
	p, err := s.ProvePropertyRedaction(id)
	if err != nil {
		return nil, err
	}
	return MarshalProof(ExportedProof{Kind: ProofKindPropertyRedaction, Property: &p})
}
