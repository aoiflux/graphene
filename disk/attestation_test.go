package disk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// Attestations: a signed assertion about a snapshot, and the transferable claim
// it makes possible.

// newAttestKey returns a signing key and a keyring holding its public half.
//
// Kept separate from the store fixture because two stores signed by *different*
// keys sharing one key ID is exactly the confusion key IDs exist to prevent, and
// building it by accident produces a confusing failure rather than a clear one.
// A test wanting two stores under one signer passes the same key to both.
func newAttestKey(t *testing.T, id uint64) (*signing.Key, *signing.Keyring) {
	t.Helper()
	key, pub, err := signing.GenerateKey(id)
	if err != nil {
		t.Fatal(err)
	}
	ring := signing.NewKeyring()
	if err := ring.Add(id, pub); err != nil {
		t.Fatal(err)
	}
	return key, ring
}

func attestedStore(t *testing.T, n int) (dir string, ring *signing.Keyring, ids []store.NodeID) {
	t.Helper()
	key, ring := newAttestKey(t, 11)
	dir, ids = attestedStoreWithKey(t, n, key)
	return dir, ring, ids
}

func attestedStoreWithKey(t *testing.T, n int, key *signing.Key) (dir string, ids []store.NodeID) {
	t.Helper()
	dir = t.TempDir()

	s, err := OpenWithOptions(dir, Options{Signer: key, AttestActorID: 500})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	for i := 0; i < n; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.IndexNodeProperty(id, "sha256", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	return dir, ids
}

// openAttested reopens a store built by attestedStore.
func openAttested(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// **The whole point.** A recipient holding the claim and a public key can check
// that a named actor asserted a snapshot containing a specific node — without
// the store, without the file, and without any other entity.
func TestAttestation_TransferableClaimVerifies(t *testing.T) {
	dir, ring, ids := attestedStore(t, 40)
	s := openAttested(t, dir)

	claim, err := s.AttestNode(ids[9])
	if err != nil {
		t.Fatalf("AttestNode: %v", err)
	}

	if err := VerifyNodeAttestation(ring, claim); err != nil {
		t.Fatalf("a genuine claim did not verify: %v", err)
	}
	if claim.Attestation.ActorID != 500 {
		t.Errorf("attestation actor %d, want 500", claim.Attestation.ActorID)
	}
	if claim.Attestation.UnixNano == 0 {
		t.Error("attestation carries no timestamp")
	}
}

// The signature must cover the snapshot root, so a claim cannot be moved onto a
// different snapshot.
func TestAttestation_RejectsASubstitutedSubject(t *testing.T) {
	dir, ring, ids := attestedStore(t, 20)
	s := openAttested(t, dir)

	claim, err := s.AttestNode(ids[3])
	if err != nil {
		t.Fatal(err)
	}

	tampered := claim
	tampered.Attestation.Subject[0] ^= 0x01
	if err := VerifyNodeAttestation(ring, tampered); err == nil {
		t.Fatal("a claim verified after its attested snapshot root was changed")
	}
}

// Changing who is said to have attested must break the signature.
func TestAttestation_RejectsAReattributedActor(t *testing.T) {
	dir, ring, ids := attestedStore(t, 20)
	s := openAttested(t, dir)

	claim, _ := s.AttestNode(ids[1])
	tampered := claim
	tampered.Attestation.ActorID = 999
	tampered.Attestation.ID = attestationID(tampered.Attestation)

	if err := VerifyNodeAttestation(ring, tampered); err == nil {
		t.Fatal("a claim verified after being reattributed to another actor")
	}
}

// A pasted-together attestation whose ID does not follow from its contents is
// rejected separately from a bad signature — the two point at different
// mistakes.
func TestAttestation_RejectsAnInconsistentID(t *testing.T) {
	dir, ring, ids := attestedStore(t, 12)
	s := openAttested(t, dir)

	claim, _ := s.AttestNode(ids[0])
	tampered := claim
	tampered.Attestation.ID[0] ^= 0x01

	err := VerifyAttestation(ring, tampered.Attestation)
	if err == nil {
		t.Fatal("an attestation with a fabricated ID verified")
	}
}

// A valid attestation over one snapshot must not vouch for a proof into
// another. This is the gap that checking either half alone would leave.
func TestAttestation_ProofAndAttestationMustAgree(t *testing.T) {
	// One key for both stores: the point is two different *snapshots* attested by
	// the same signer, not two signers.
	key, ring := newAttestKey(t, 11)

	dirA, idsA := attestedStoreWithKey(t, 20, key)
	sA := openAttested(t, dirA)

	// Different content, so genuinely a different snapshot.
	dirB, _ := attestedStoreWithKey(t, 25, key)
	sB := openAttested(t, dirB)

	claimA, err := sA.AttestNode(idsA[2])
	if err != nil {
		t.Fatal(err)
	}
	attB, err := sB.SnapshotAttestation()
	if err != nil {
		t.Fatal(err)
	}
	if attB.Subject == claimA.Attestation.Subject {
		t.Fatal("fixture error: the two stores have the same snapshot")
	}

	// B's attestation is perfectly valid; it just does not vouch for A's proof.
	mixed := NodeAttestation{Attestation: attB, Inclusion: claimA.Inclusion}
	if err := VerifyAttestation(ring, attB); err != nil {
		t.Fatalf("B's attestation should be valid on its own: %v", err)
	}
	if err := VerifyNodeAttestation(ring, mixed); err == nil {
		t.Fatal("a proof verified against an attestation for a different snapshot")
	}
}

// Attestations survive a reopen and chain to the one they replace.
func TestAttestation_SurvivesReopenAndChains(t *testing.T) {
	dir, ring, _ := attestedStore(t, 15)

	s := openAttested(t, dir)
	first, err := s.SnapshotAttestation()
	if err != nil {
		t.Fatalf("attestation did not survive the reopen: %v", err)
	}
	if err := VerifyAttestation(ring, first); err != nil {
		t.Fatalf("the reloaded attestation did not verify: %v", err)
	}
	s.Close()

	// Compact again; the new attestation must name the old as its predecessor.
	// A fresh key is fine here — the assertion is about chaining, not about the
	// second signature verifying under the original ring.
	s2, err := OpenWithOptions(dir, Options{Signer: mustKey(t, 12), AttestActorID: 500})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	addNodeD(t, s2, store.NodeTypeTag)
	if err := s2.Compact(); err != nil {
		t.Fatal(err)
	}
	second, err := s2.SnapshotAttestation()
	if err != nil {
		t.Fatal(err)
	}
	if second.Prev != first.ID {
		t.Fatalf("the new attestation names predecessor %x, but the previous was %x",
			second.Prev[:8], first.ID[:8])
	}
}

// mustKey returns a fresh key under the given ID. The public half is discarded
// because the test above only checks chaining, not the second signature.
func mustKey(t *testing.T, id uint64) *signing.Key {
	t.Helper()
	k, _, err := signing.GenerateKey(id)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// An attestation naming a snapshot other than the one in the same file is
// refused at load: it does not vouch for this image.
func TestAttestation_RejectedWhenItNamesAnotherSnapshot(t *testing.T) {
	dir, _, _ := attestedStore(t, 10)

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}

	// Locate GATT and disturb its subject.
	sections, err := readCSRSectionDirectory(data, uint64(readU64(data, 62)))
	if err != nil {
		t.Fatal(err)
	}
	sec, ok := findSection(sections, csrSectionAttestation)
	if !ok {
		t.Fatal("no attestation section written")
	}
	subjectAt := int(sec.Offset) + 1 + attestationIDSize + 8 + 8 + 8
	data[subjectAt] ^= 0x01

	digest := computeCSRDigest(data)
	copy(data[csrDigestOffset:csrDigestOffset+csrDigestSize], digest[:])
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := deserialiseCSR(data); err == nil {
		t.Fatal("an attestation naming a different snapshot was accepted")
	}
}

func readU64(b []byte, off int) uint64 {
	var v uint64
	for i := 7; i >= 0; i-- {
		v = v<<8 | uint64(b[off+i])
	}
	return v
}

// A store with no signer produces no attestation, and says so rather than
// returning an empty one.
func TestAttestation_AbsentWithoutASigner(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SnapshotAttestation(); !errors.Is(err, ErrNoAttestation) {
		t.Fatalf("SnapshotAttestation on an unsigned store gave %v, want ErrNoAttestation", err)
	}
	if _, err := s.AttestNode(1); !errors.Is(err, ErrNoAttestation) {
		t.Fatalf("AttestNode on an unsigned store gave %v, want ErrNoAttestation", err)
	}

	// The snapshot roots are still there — integrity without authorship.
	if _, err := s.SnapshotRoots(); err != nil {
		t.Fatalf("an unsigned store should still have roots: %v", err)
	}
}
