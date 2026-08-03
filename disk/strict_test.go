package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// VerifyOnOpen and StrictOptions: the safe posture in one setting.

func strictFixture(t *testing.T) (dir string, key *signing.Key, ring *signing.Keyring) {
	t.Helper()
	dir = t.TempDir()
	key, ring = newAttestKey(t, 21)

	s, err := OpenWithOptions(dir, StrictOptions(key, ring, 900))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 15; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, key, ring
}

// A clean store opens under the strictest configuration.
func TestStrict_CleanStoreOpens(t *testing.T) {
	dir, key, ring := strictFixture(t)

	s, err := OpenWithOptions(dir, StrictOptions(key, ring, 900))
	if err != nil {
		t.Fatalf("a clean store failed to open strictly: %v", err)
	}
	defer s.Close()

	n, _ := s.NodeCount()
	if n != 15 {
		t.Fatalf("node count %d, want 15", n)
	}
}

// StrictOptions turns on all three settings, so a caller does not have to know
// them individually. Asserted directly, because the value of the helper is that
// it cannot silently drift out of step with the fields it sets.
func TestStrict_OptionsEnablesEverything(t *testing.T) {
	key, ring := newAttestKey(t, 1)
	o := StrictOptions(key, ring, 5)

	if o.Signer == nil || o.Verifier == nil {
		t.Fatal("StrictOptions did not carry the signer and verifier")
	}
	if !o.RequireSignedCommits {
		t.Error("StrictOptions left RequireSignedCommits off")
	}
	if !o.VerifyOnOpen {
		t.Error("StrictOptions left VerifyOnOpen off")
	}
	if o.AttestActorID != 5 {
		t.Errorf("AttestActorID = %d, want 5", o.AttestActorID)
	}
}

// A tampered image fails the Open rather than loading and reporting later.
func TestStrict_TamperedImageFailsTheOpen(t *testing.T) {
	dir, key, ring := strictFixture(t)

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[csrV8HeaderSize+30] ^= 0x01
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	s, err := OpenWithOptions(dir, StrictOptions(key, ring, 900))
	if err == nil {
		s.Close()
		t.Fatal("a tampered image opened under VerifyOnOpen")
	}

	// And without the flag it opens, which is what makes the flag worth having.
	lenient, err := Open(dir)
	if err != nil {
		t.Fatalf("the default configuration should still open: %v", err)
	}
	lenient.Close()
}

// An edit that repaired the digest still fails, because the roots are checked
// too. This is the layered case from BL-15, now enforced at Open.
func TestStrict_CatchesAnEditThatRepairedTheDigest(t *testing.T) {
	dir, key, ring := strictFixture(t)

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[csrV8HeaderSize+18] ^= 0x01
	repaired := computeCSRDigest(data)
	copy(data[csrDigestOffset:csrDigestOffset+csrDigestSize], repaired[:])
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	// The digest now agrees with the bytes...
	if status, _, err := VerifyCSRDigest(dir); err != nil || status != DigestMatch {
		t.Fatalf("the digest should have been repaired: status=%v err=%v", status, err)
	}
	// ...and the open still fails, on the roots.
	s, err := OpenWithOptions(dir, StrictOptions(key, ring, 900))
	if err == nil {
		s.Close()
		t.Fatal("an edit with a repaired digest opened under VerifyOnOpen")
	}
}

// Enabling verification must not reject a store written before any of it
// existed. A pre-v8 image carries no digest and no roots, and that is the
// honest answer for a file that was never covered — not a failure.
//
// Checked against verifyImageOnOpen directly rather than through Open, because
// a genuine v7 file cannot be produced any more (the serialiser writes v8) and
// a v8 file relabelled as v7 is not a v7 file — its header is a different size,
// so the records misparse. That would test the wrong failure.
func TestStrict_PreV8ImageIsNotRejected(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[4], data[5] = 7, 0 // claim v7: no digest, no roots
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	if err := verifyImageOnOpen(p, Options{VerifyOnOpen: true}); err != nil {
		t.Fatalf("verification rejected an image carrying no digest; absent is not a failure: %v", err)
	}
}

// An image with roots but no attestation, opened with a verifier, passes: there
// is nothing to check rather than something that failed.
func TestStrict_UnattestedImagePassesVerification(t *testing.T) {
	dir := t.TempDir()
	_, ring := newAttestKey(t, 31)

	s, err := Open(dir) // no signer, so roots but no attestation
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithOptions(dir, Options{VerifyOnOpen: true, Verifier: ring})
	if err != nil {
		t.Fatalf("an unattested but intact image was rejected: %v", err)
	}
	defer reopened.Close()
}

// VerifyOnOpen is off unless asked for — the default stays permissive, and a
// caller who never opts in sees exactly the behaviour they saw before.
func TestStrict_DefaultIsUnchanged(t *testing.T) {
	var o Options
	if o.VerifyOnOpen || o.RequireSignedCommits || o.Signer != nil || o.Verifier != nil {
		t.Fatal("the zero Options is not the permissive default")
	}

	dir, _, _ := strictFixture(t)
	p := filepath.Join(dir, csrFileName)
	data, _ := os.ReadFile(p)
	data[csrV8HeaderSize+30] ^= 0x01
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("the default Open should not verify, and so should succeed: %v", err)
	}
	s.Close()
}
