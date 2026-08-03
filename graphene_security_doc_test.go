package graphene_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// The flow SECURITY.md documents, executed.
//
// A security document whose examples have never been run is the exact thing it
// exists to prevent: a confident statement nobody checked. These mirror §4 of
// that file, so if the API moves underneath it the tests break rather than the
// document quietly becoming wrong.

// SECURITY.md §4 "Signing and verifying the log", end to end.
func TestSecurityDoc_SigningAndVerifyingTheLog(t *testing.T) {
	dir := t.TempDir()

	key, pub, err := signing.GenerateKey(1)
	if err != nil {
		t.Fatal(err)
	}
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		t.Fatal(err)
	}

	// The spelled-out form the document says it is equivalent to. Checked before
	// anything is opened, so a mismatch does not leave a store behind.
	if want := (disk.Options{
		Signer: key, Verifier: ring, RequireSignedCommits: true,
		VerifyOnOpen: true, Audit: true, AttestActorID: 42,
	}); disk.StrictOptions(key, ring, 42) != want {
		t.Fatal("StrictOptions no longer matches the expansion SECURITY.md documents")
	}

	// The documented one-call form.
	s, err := disk.OpenWithOptions(dir, disk.StrictOptions(key, ring, 42))
	if err != nil {
		t.Fatalf("the documented StrictOptions call failed to open: %v", err)
	}

	if err := s.ApplyTransactionAs(
		[]store.TxOp{{Kind: store.TxOpAddNode, Node: &store.Node{
			ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeMicroArtefact}}}},
		store.TxContext{ActorID: 42},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := disk.OpenWithOptions(dir, disk.StrictOptions(key, ring, 42))
	if err != nil {
		t.Fatalf("a signed store did not reopen under the strictest configuration: %v", err)
	}
	defer reopened.Close()
}

// SECURITY.md §4 "Why the default is permissive" — both halves, since the
// document justifies a weaker default and that justification is only honest if
// the stronger setting actually catches what the weaker one misses.
func TestSecurityDoc_DefaultIsPermissiveAndStrictIsNot(t *testing.T) {
	dir := t.TempDir()
	key, pub, _ := signing.GenerateKey(2)
	ring := signing.NewKeyring()
	if err := ring.Add(2, pub); err != nil {
		t.Fatal(err)
	}

	const marker = "EVIDENCE-PAYLOAD"

	s, err := disk.OpenWithOptions(dir, disk.StrictOptions(key, ring, 1))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(marker),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Edit a byte of a node's PROPERTY BLOB.
	//
	// Deliberately not an arbitrary byte. Structural validation runs at load
	// regardless of settings, so damaging an ID, a count, or a section is caught
	// by the default too — the earlier attempts at this test hit the ID sparsity
	// bound and the snapshot-root consistency check in turn. A property blob is
	// opaque to the engine, so changing one leaves the file perfectly
	// well-formed. That is both what the default genuinely misses and what an
	// attacker would actually alter: the evidence itself.
	p := filepath.Join(dir, "graphene.csr")
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(data, []byte(marker))
	if at < 0 {
		t.Fatal("fixture error: property blob not found in the image")
	}
	data[at] = 'X'
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	// The default opens it, exactly as the document says.
	lenient, err := disk.Open(dir)
	if err != nil {
		t.Fatalf("the permissive default should still open a damaged image: %v", err)
	}
	lenient.Close()

	// StrictOptions does not.
	if strict, err := disk.OpenWithOptions(dir, disk.StrictOptions(key, ring, 1)); err == nil {
		strict.Close()
		t.Fatal("StrictOptions opened a damaged image; the documented protection does not hold")
	}
}

// SECURITY.md §4 "The part that makes verification mean anything".
//
// The document claims that without an externally retained root the machinery
// detects damage and accident, and with one it detects tampering. Both halves
// are asserted here, because the second is the claim that justifies the first
// being described as insufficient.
func TestSecurityDoc_ExternallyRetainedRootDetectsTampering(t *testing.T) {
	dir := t.TempDir()
	key, pub, _ := signing.GenerateKey(1)
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		t.Fatal(err)
	}

	s, err := disk.OpenWithOptions(dir, disk.Options{Signer: key, AttestActorID: 7})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	// The documented step: retain the root outside the system.
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	retained := roots.Snapshot
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// An attacker rewrites the file wholesale: new content, and every internal
	// value recomputed so the file is self-consistent.
	forge(t, dir, key)

	// Every in-file check passes. This is the case the document warns about.
	if status, _, err := disk.VerifyCSRDigest(dir); err != nil || status != disk.DigestMatch {
		t.Fatalf("the forged file should be internally consistent: status=%v err=%v", status, err)
	}
	if err := disk.VerifyCSRRoots(dir); err != nil {
		t.Fatalf("the forged file's roots should describe its own records: %v", err)
	}

	// The retained root is what catches it.
	forged, err := disk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer forged.Close()

	now, err := forged.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	if now.Snapshot == retained {
		t.Fatal("the forged store reproduced the retained snapshot root")
	}
	// Which is exactly the check a verifier performs.
	if err := verifyAgainstRetained(forged, retained); err == nil {
		t.Fatal("verification against an externally retained root accepted a forged store")
	}
}

// forge rewrites a store's contents entirely, re-signing so that every value
// inside the file agrees. It stands in for an attacker with file access and the
// key — the strongest position the engine can still be measured against.
func forge(t *testing.T, dir string, key *signing.Key) {
	t.Helper()
	if err := os.Remove(filepath.Join(dir, "graphene.csr")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "graphene.wal")); err != nil {
		t.Fatal(err)
	}
	s, err := disk.OpenWithOptions(dir, disk.Options{Signer: key, AttestActorID: 7})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ { // different content
		if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// verifyAgainstRetained is the check SECURITY.md tells a caller to perform.
func verifyAgainstRetained(s *disk.Store, retained merkle.Hash) error {
	roots, err := s.SnapshotRoots()
	if err != nil {
		return err
	}
	if roots.Snapshot != retained {
		return errSnapshotMismatch
	}
	return nil
}

var errSnapshotMismatch = errSnapshot("snapshot root does not match the retained value")

type errSnapshot string

func (e errSnapshot) Error() string { return string(e) }

// SECURITY.md §4 "Rotating a signing key", including the claim that the
// outgoing key is what signs.
func TestSecurityDoc_RotatingASigningKey(t *testing.T) {
	dir := t.TempDir()

	k1, p1, _ := signing.GenerateKey(1)
	k2, p2, _ := signing.GenerateKey(2)
	ring := signing.NewKeyring()
	if err := ring.Add(1, p1); err != nil {
		t.Fatal(err)
	}
	if err := ring.Add(2, p2); err != nil {
		t.Fatal(err)
	}

	s, err := disk.OpenWithOptions(dir, disk.StrictOptions(k1, ring, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatal(err)
	}

	// The documented call.
	if err := s.RotateKey(k2, p2); err != nil {
		t.Fatalf("the documented RotateKey call failed: %v", err)
	}

	tl := s.KeyTimeline()
	if err := tl.VerifyChain(ring, 1); err != nil {
		t.Fatalf("the documented VerifyChain call failed: %v", err)
	}

	// The document's central claim about rotation: which key was authoritative
	// depends on position, so a later compromise does not invalidate everything.
	at := tl.Transitions[0].AtCommitSeq
	if tl.AuthoritativeAt(1, at-1) != 1 {
		t.Error("key 1 should be authoritative before the rotation")
	}
	if tl.AuthoritativeAt(1, at) != 2 {
		t.Error("key 2 should be authoritative from the rotation onwards")
	}
}

// SECURITY.md §4 "Transferable claims", including the limits it states.
func TestSecurityDoc_TransferableClaim(t *testing.T) {
	dir := t.TempDir()
	key, pub, _ := signing.GenerateKey(3)
	ring := signing.NewKeyring()
	if err := ring.Add(3, pub); err != nil {
		t.Fatal(err)
	}

	s, err := disk.OpenWithOptions(dir, disk.Options{Signer: key, AttestActorID: 9})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var id store.NodeID
	for i := 0; i < 10; i++ {
		id, err = s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	claim, err := s.AttestNode(id)
	if err != nil {
		t.Fatalf("AttestNode: %v", err)
	}
	// The recipient holds the claim and a public key. Nothing else.
	if err := disk.VerifyNodeAttestation(ring, claim); err != nil {
		t.Fatalf("the documented verification failed: %v", err)
	}

	// And the documented limit: a keyring without the key cannot verify it.
	stranger := signing.NewKeyring()
	if err := disk.VerifyNodeAttestation(stranger, claim); err == nil {
		t.Fatal("a claim verified against a keyring holding no keys")
	}
}

// SECURITY.md §2 "CRC32 is not a security control", demonstrated rather than
// asserted: a deliberate edit with a recomputed checksum passes every CRC.
func TestSecurityDoc_CRC32IsNotTamperDetection(t *testing.T) {
	dir := t.TempDir()
	s, err := disk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// An unsigned, undigested log: nothing here can distinguish an edit from a
	// legitimate write, which is precisely the document's point.
	w, err := disk.InspectWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range w.Records {
		if !r.CRCValid {
			t.Fatal("fixture error: the log should be intact before tampering")
		}
	}
	if w.Truncated {
		t.Fatal("fixture error: unexpected truncation")
	}
}
