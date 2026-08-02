package disk

// Attestations: a signed statement that a named actor asserts a particular
// snapshot, at a particular time.
//
// The pieces existed separately and did nothing on their own. Snapshot roots say
// what an image contains but not who produced it — anyone can compute a root.
// Commit signatures say who wrote each transaction but say nothing about the
// compacted image, which is a different artefact built later. An attestation
// binds the two: this actor, holding this key, at this time, asserts this
// snapshot root.
//
// Combined with an inclusion proof it becomes the transferable form the plan's
// §11.5 describes — a recipient holding the entity, the proof, the attestation,
// and a public key can check that a specific artefact was in a specific snapshot
// asserted by a specific party, without the store and without the file.
//
// # What it still does not prove
//
// Authorship, not truth. A signature says the key holder asserted this; it says
// nothing about whether what was ingested was accurate (§12.4 T-15), and nothing
// about a compromised process, which signs whatever it is told to. The timestamp
// is a local clock reading and is asserted rather than proven (T-09) — upgrading
// it needs an external time authority.

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// attestationBodyVersion versions the GATT section body.
const attestationBodyVersion = 1

// attestationIDSize is the truncated digest that identifies an attestation.
const attestationIDSize = 16

// domainAttestation separates attestation signatures from commit signatures, so
// neither can be presented as the other.
const domainAttestation = 0xA7

// Attestation is a signed assertion about a snapshot.
type Attestation struct {
	// ID is derived from the content, so it is stable and self-checking rather
	// than allocated. Recomputed on verification.
	ID [attestationIDSize]byte

	ActorID  uint64
	KeyID    uint64
	UnixNano int64

	// Subject is what is being attested — the snapshot root.
	Subject merkle.Hash

	// Prev links to the attestation this one follows, zero for the first. This
	// is §11.5's chain-of-custody field: it makes a removed attestation provably
	// missing rather than invisibly absent, in the same way snapshot roots chain.
	Prev [attestationIDSize]byte

	Signature []byte
}

// attestationID derives an attestation's identifier from its content.
func attestationID(a Attestation) [attestationIDSize]byte {
	sum := sha256.Sum256(attestationSignedData(a))
	var out [attestationIDSize]byte
	copy(out[:], sum[:attestationIDSize])
	return out
}

// attestationSignedData is the byte string an attestation signature covers.
//
// Everything except the ID and the signature, since the ID is derived from this
// and the signature is over it.
func attestationSignedData(a Attestation) []byte {
	buf := make([]byte, 0, 1+8+8+8+merkle.Size+attestationIDSize)
	buf = append(buf, domainAttestation)
	buf = binary.LittleEndian.AppendUint64(buf, a.ActorID)
	buf = binary.LittleEndian.AppendUint64(buf, a.KeyID)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(a.UnixNano))
	buf = append(buf, a.Subject[:]...)
	return append(buf, a.Prev[:]...)
}

// signAttestation builds and signs an attestation over subject.
func signAttestation(signer store.Signer, actorID uint64, unixNano int64, subject merkle.Hash, prev [attestationIDSize]byte) (Attestation, error) {
	a := Attestation{
		ActorID:  actorID,
		KeyID:    signer.KeyID(),
		UnixNano: unixNano,
		Subject:  subject,
		Prev:     prev,
	}
	sig, err := signer.Sign(attestationSignedData(a))
	if err != nil {
		return Attestation{}, fmt.Errorf("attest snapshot: %w", err)
	}
	if len(sig) != walSignatureSize {
		return Attestation{}, fmt.Errorf("attest snapshot: signature is %d bytes, expected %d",
			len(sig), walSignatureSize)
	}
	a.Signature = sig
	a.ID = attestationID(a)
	return a, nil
}

// ErrNoAttestation is returned when an image carries no attestation.
var ErrNoAttestation = errors.New("disk: image carries no attestation")

// VerifyAttestation checks an attestation's signature and its derived ID.
//
// Takes a Verifier rather than a store, because the party checking an
// attestation is generally not the party that produced it and may hold nothing
// but a public key.
func VerifyAttestation(v store.Verifier, a Attestation) error {
	if v == nil {
		return errors.New("verify attestation: no verifier")
	}
	if len(a.Signature) == 0 {
		return errors.New("verify attestation: unsigned")
	}
	// The ID is derived, so a mismatch means the attestation was assembled
	// rather than produced — worth catching separately from a bad signature,
	// because it points at a different kind of mistake.
	if attestationID(a) != a.ID {
		return errors.New("verify attestation: identifier does not follow from its contents")
	}
	if err := v.Verify(a.KeyID, attestationSignedData(a), a.Signature); err != nil {
		return fmt.Errorf("verify attestation: %w", err)
	}
	return nil
}

// --- GATT section encoding ---

const attestationSectionSize = 1 + attestationIDSize + 8 + 8 + 8 + merkle.Size + attestationIDSize + 2 + walSignatureSize

func appendAttestationSection(buf []byte, a Attestation) []byte {
	buf = append(buf, attestationBodyVersion)
	buf = append(buf, a.ID[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, a.ActorID)
	buf = binary.LittleEndian.AppendUint64(buf, a.KeyID)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(a.UnixNano))
	buf = append(buf, a.Subject[:]...)
	buf = append(buf, a.Prev[:]...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(a.Signature)))
	return append(buf, a.Signature...)
}

func readAttestationSection(data []byte) (Attestation, error) {
	var a Attestation
	if len(data) < 1 {
		return a, errors.New("empty attestation section")
	}
	if v := data[0]; v != attestationBodyVersion {
		return a, fmt.Errorf("attestation section version %d, this build understands %d",
			v, attestationBodyVersion)
	}
	if len(data) < attestationSectionSize {
		return a, fmt.Errorf("truncated attestation section: %d bytes, need %d",
			len(data), attestationSectionSize)
	}

	pos := 1
	copy(a.ID[:], data[pos:pos+attestationIDSize])
	pos += attestationIDSize
	a.ActorID = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	a.KeyID = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	a.UnixNano = int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8
	copy(a.Subject[:], data[pos:pos+merkle.Size])
	pos += merkle.Size
	copy(a.Prev[:], data[pos:pos+attestationIDSize])
	pos += attestationIDSize

	sigLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	if sigLen > walSignatureSize || pos+sigLen > len(data) {
		return Attestation{}, fmt.Errorf("attestation section declares a %d-byte signature", sigLen)
	}
	a.Signature = append([]byte(nil), data[pos:pos+sigLen]...)
	return a, nil
}

// --- transferable attestations ---

// NodeAttestation is the complete, self-contained claim: a signed assertion
// about a snapshot, plus a proof that a specific node was in it.
//
// This is what §11.5's template describes. A recipient needs this value and a
// public key — not the store, not the file, and not any other entity.
type NodeAttestation struct {
	Attestation Attestation
	Inclusion   NodeInclusionProof
}

// AttestNode produces a transferable claim about one node in the compacted
// image.
func (s *Store) AttestNode(id store.NodeID) (NodeAttestation, error) {
	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()

	if csr == nil {
		return NodeAttestation{}, ErrNoSnapshotRoots
	}
	if csr.attestation.Signature == nil {
		return NodeAttestation{}, ErrNoAttestation
	}
	proof, err := s.ProveNode(id)
	if err != nil {
		return NodeAttestation{}, err
	}
	return NodeAttestation{Attestation: csr.attestation, Inclusion: proof}, nil
}

// SnapshotAttestation returns the signed assertion carried by the compacted
// image.
func (s *Store) SnapshotAttestation() (Attestation, error) {
	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()

	if csr == nil || csr.attestation.Signature == nil {
		return Attestation{}, ErrNoAttestation
	}
	return csr.attestation, nil
}

// VerifyNodeAttestation checks a transferable claim end to end.
//
// Three things have to hold, and each closes a different gap:
//
//   - the attestation's signature is valid, so a named key really asserted this
//     snapshot;
//   - the attestation's subject is the snapshot root the inclusion proof
//     resolves against, so the signature covers *this* tree and not another;
//   - the inclusion proof resolves, so the node really was in that tree.
//
// Checking any two without the third leaves a hole: a valid signature over an
// unrelated snapshot, or a valid proof into a snapshot nobody vouched for.
func VerifyNodeAttestation(v store.Verifier, na NodeAttestation) error {
	if err := VerifyAttestation(v, na.Attestation); err != nil {
		return err
	}
	if err := VerifyNodeInclusion(na.Attestation.Subject, na.Inclusion); err != nil {
		return fmt.Errorf("verify node attestation: %w", err)
	}
	return nil
}
