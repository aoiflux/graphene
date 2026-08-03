package disk

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// Key rotation: recording when a key changed, so a later compromise does not
// retroactively invalidate everything the key ever signed.

// rotatingStore opens a store signed by key 1 and returns a ring holding both
// key 1 and key 2's public halves.
func rotatingStore(t *testing.T) (s *Store, k1, k2 *signing.Key, pub2 []byte, ring *signing.Keyring) {
	t.Helper()
	dir := t.TempDir()

	k1, p1, err := signing.GenerateKey(1)
	if err != nil {
		t.Fatal(err)
	}
	k2, p2, err := signing.GenerateKey(2)
	if err != nil {
		t.Fatal(err)
	}
	ring = signing.NewKeyring()
	if err := ring.Add(1, p1); err != nil {
		t.Fatal(err)
	}
	if err := ring.Add(2, p2); err != nil {
		t.Fatal(err)
	}

	s, err = OpenWithOptions(dir, Options{Signer: k1, Verifier: ring, RequireSignedCommits: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, k1, k2, p2, ring
}

func writeOne(t *testing.T, s *Store) {
	t.Helper()
	if err := s.ApplyTransactionAs(
		[]store.TxOp{{Kind: store.TxOpAddNode, Node: &store.Node{
			ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeMicroArtefact}}}},
		store.TxContext{ActorID: 1},
	); err != nil {
		t.Fatal(err)
	}
}

// A rotation is recorded, signed by the outgoing key, and the store switches.
func TestKeyRotation_RecordsAndSwitches(t *testing.T) {
	s, _, k2, pub2, ring := rotatingStore(t)

	writeOne(t, s)
	writeOne(t, s)
	before := s.commitSeq.Load()

	if err := s.RotateKey(k2, pub2); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	writeOne(t, s)

	tl := s.KeyTimeline()
	if len(tl.Transitions) != 1 {
		t.Fatalf("recorded %d transitions, want 1", len(tl.Transitions))
	}
	tr := tl.Transitions[0]
	if tr.PrevKeyID != 1 || tr.NewKeyID != 2 {
		t.Fatalf("transition says %d -> %d, want 1 -> 2", tr.PrevKeyID, tr.NewKeyID)
	}
	if tr.AtCommitSeq != before+1 {
		t.Fatalf("transition takes effect at commit %d, want %d", tr.AtCommitSeq, before+1)
	}
	if err := tl.VerifyChain(ring, 1); err != nil {
		t.Fatalf("a genuine rotation chain did not verify: %v", err)
	}
}

// **The property rotation exists for.** After a rotation, a verifier can say
// which key was authoritative at each point rather than treating the old key as
// wholly trusted or wholly suspect.
func TestKeyRotation_AuthoritativeKeyDependsOnPosition(t *testing.T) {
	s, _, k2, pub2, _ := rotatingStore(t)

	writeOne(t, s)
	writeOne(t, s)
	if err := s.RotateKey(k2, pub2); err != nil {
		t.Fatal(err)
	}
	writeOne(t, s)

	tl := s.KeyTimeline()
	at := tl.Transitions[0].AtCommitSeq

	if got := tl.AuthoritativeAt(1, at-1); got != 1 {
		t.Errorf("before the rotation the authoritative key was %d, want 1", got)
	}
	if got := tl.AuthoritativeAt(1, at); got != 2 {
		t.Errorf("at the rotation the authoritative key was %d, want 2", got)
	}
	if got := tl.AuthoritativeAt(1, at+100); got != 2 {
		t.Errorf("after the rotation the authoritative key was %d, want 2", got)
	}
}

// The transition is signed by the OUTGOING key. A record signed by the incoming
// key would prove nothing — that is exactly what an attacker substituting their
// own key would hold.
func TestKeyRotation_SignedByTheOutgoingKey(t *testing.T) {
	s, _, k2, pub2, ring := rotatingStore(t)
	writeOne(t, s)
	if err := s.RotateKey(k2, pub2); err != nil {
		t.Fatal(err)
	}

	tr := s.KeyTimeline().Transitions[0]

	// It verifies under the outgoing key...
	if err := ring.Verify(tr.PrevKeyID, keyTransitionSignedData(tr), tr.Signature); err != nil {
		t.Fatalf("the transition is not signed by the outgoing key: %v", err)
	}
	// ...and not under the incoming one.
	if err := ring.Verify(tr.NewKeyID, keyTransitionSignedData(tr), tr.Signature); err == nil {
		t.Fatal("the transition also verified under the incoming key; it should not")
	}
}

// A forged transition is rejected — an attacker cannot introduce their own key
// without the outgoing key's signature.
func TestKeyRotation_RejectsAForgedTransition(t *testing.T) {
	_, _, _, _, ring := rotatingStore(t)

	// An attacker fabricates a rotation from key 1 to a key they control.
	forged := KeyTransition{
		PrevKeyID:   1,
		NewKeyID:    99,
		AtCommitSeq: 1,
		UnixNano:    1,
		Signature:   make([]byte, walSignatureSize),
	}
	tl := KeyTimeline{Transitions: []KeyTransition{forged}}

	if err := tl.VerifyChain(ring, 1); err == nil {
		t.Fatal("a fabricated key transition verified")
	}
}

// A gap in the chain is reported distinctly from a forgery, because after a
// compaction a missing transition is expected rather than sinister.
func TestKeyRotation_ReportsABrokenChain(t *testing.T) {
	s, _, k2, pub2, ring := rotatingStore(t)
	writeOne(t, s)
	if err := s.RotateKey(k2, pub2); err != nil {
		t.Fatal(err)
	}

	tl := s.KeyTimeline()
	// Verified against the wrong starting key: the first transition replaces a
	// key that was never authoritative, which is what a missing record looks
	// like.
	err := tl.VerifyChain(ring, 77)
	if err == nil {
		t.Fatal("a chain starting from the wrong key verified")
	}
	if !contains(err.Error(), "missing from the log") {
		t.Fatalf("a broken chain should be reported as a gap, got: %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Rotations survive a reopen: they are records in the log, not memory.
func TestKeyRotation_SurvivesReopen(t *testing.T) {
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

	s, err := OpenWithOptions(dir, Options{Signer: k1, Verifier: ring, RequireSignedCommits: true})
	if err != nil {
		t.Fatal(err)
	}
	writeOne(t, s)
	if err := s.RotateKey(k2, p2); err != nil {
		t.Fatal(err)
	}
	writeOne(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen under the new key. The log contains commits signed by both.
	reopened, err := OpenWithOptions(dir, Options{Signer: k2, Verifier: ring, RequireSignedCommits: true})
	if err != nil {
		t.Fatalf("a store with a rotation failed to reopen: %v", err)
	}
	defer reopened.Close()

	tl := reopened.KeyTimeline()
	if len(tl.Transitions) != 1 {
		t.Fatalf("the rotation did not survive the reopen: %d transitions", len(tl.Transitions))
	}
	if err := tl.VerifyChain(ring, 1); err != nil {
		t.Fatalf("the reloaded chain did not verify: %v", err)
	}
}

// Rotating to the same key ID is refused: it would produce a transition that
// says nothing and a timeline that cannot be ordered.
func TestKeyRotation_RejectsTheSameKeyID(t *testing.T) {
	s, k1, _, _, _ := rotatingStore(t)

	_, samePub, _ := signing.GenerateKey(1)
	if err := s.RotateKey(k1, samePub); err == nil {
		t.Fatal("rotating to the same key ID was accepted")
	}
}

// A store with no signer cannot rotate, and says why.
func TestKeyRotation_RequiresASigner(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	k, pub, _ := signing.GenerateKey(2)
	if err := s.RotateKey(k, pub); !errors.Is(err, ErrNoSigner) {
		t.Fatalf("RotateKey without a signer gave %v, want ErrNoSigner", err)
	}
}

// Multiple rotations chain in order.
func TestKeyRotation_ChainsThroughSeveralKeys(t *testing.T) {
	dir := t.TempDir()

	ring := signing.NewKeyring()
	keys := make([]*signing.Key, 0, 4)
	pubs := make([][]byte, 0, 4)
	for id := uint64(1); id <= 4; id++ {
		k, p, err := signing.GenerateKey(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := ring.Add(id, p); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
		pubs = append(pubs, p)
	}

	s, err := OpenWithOptions(dir, Options{Signer: keys[0], Verifier: ring, RequireSignedCommits: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 1; i < len(keys); i++ {
		writeOne(t, s)
		if err := s.RotateKey(keys[i], pubs[i]); err != nil {
			t.Fatalf("rotation %d: %v", i, err)
		}
	}
	writeOne(t, s)

	tl := s.KeyTimeline()
	if len(tl.Transitions) != 3 {
		t.Fatalf("recorded %d transitions, want 3", len(tl.Transitions))
	}
	if err := tl.VerifyChain(ring, 1); err != nil {
		t.Fatalf("a three-step chain did not verify: %v", err)
	}
	// And the authoritative key advances with the sequence.
	last := tl.Transitions[len(tl.Transitions)-1]
	if got := tl.AuthoritativeAt(1, last.AtCommitSeq); got != 4 {
		t.Fatalf("after three rotations the authoritative key was %d, want 4", got)
	}
}
