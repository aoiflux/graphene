package graphene_test

import (
	"bytes"
	"errors"
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

// --- §5, anchoring ---
//
// §5 makes four claims a reader will act on: that a checkpoint binds all four
// histories, that the check runs in both directions, that a destroyed local
// record reads as broken rather than unanchored, and that the local anchor
// refuses to live inside the store. Each is executed below.

// anchoredDocStore is §5's setup: a signed, audited store with retention, plus
// an anchor kept outside it.
func anchoredDocStore(t *testing.T) (*disk.Store, disk.Anchor, string) {
	t.Helper()
	dir := t.TempDir()

	key, pub, err := signing.GenerateKey(1)
	if err != nil {
		t.Fatal(err)
	}
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		t.Fatal(err)
	}

	opts := disk.StrictOptions(key, ring, 42)
	opts.Retention = disk.RetentionPolicy{MaxSegments: 10}
	s, err := disk.OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	anchor, err := disk.NewInsecureLocalAnchor(filepath.Join(t.TempDir(), "anchor.bin"), dir)
	if err != nil {
		t.Fatal(err)
	}
	return s, anchor, dir
}

// SECURITY.md §5 "What is published" and "What is checked", the documented flow.
func TestSecurityDoc_PublishingAndCheckingACheckpoint(t *testing.T) {
	s, anchor, _ := anchoredDocStore(t)

	// The two calls the document shows.
	c, rec, err := s.PublishCheckpoint(anchor)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Digest != c.Digest {
		t.Fatal("the anchor acknowledged a digest other than the checkpoint's")
	}

	report, err := s.VerifyAgainstAnchor(anchor)
	if err != nil {
		t.Fatal(err)
	}
	if report.Broken() {
		t.Fatalf("a freshly published checkpoint reported broken: %v", report.Gaps)
	}
	if report.Matched != 1 {
		t.Fatalf("the anchor confirmed %d checkpoints, want 1", report.Matched)
	}

	// "A store that moved on is not a store that was tampered with."
	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	moved, err := s.VerifyAgainstAnchor(anchor)
	if err != nil {
		t.Fatal(err)
	}
	if moved.Broken() {
		t.Fatalf("§5 says ordinary progress is a finding, not a break: %v", moved.Gaps)
	}
	if moved.CurrentMatchesLast {
		t.Fatal("the store compacted; it cannot still match the witnessed checkpoint")
	}
	if len(moved.Gaps) == 0 {
		t.Fatal("§5 says the unanchored window is reported rather than left to inference")
	}
}

// SECURITY.md §5 "Binding all four matters" — the claim that publishing only the
// snapshot root would leave the other histories free.
func TestSecurityDoc_CheckpointBindsAllFourHistories(t *testing.T) {
	s, _, _ := anchoredDocStore(t)

	before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	// An audit entry alone: no write, no compaction, so the snapshot cannot move.
	if err := s.RecordAudit(disk.AuditCustom, 7, "a change the snapshot cannot see"); err != nil {
		t.Fatal(err)
	}

	after, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if after.SnapshotRoot != before.SnapshotRoot {
		t.Fatal("the snapshot root moved; this no longer isolates the audit history")
	}
	if after.Digest == before.Digest {
		t.Fatal("§5 claims a checkpoint binds all four histories, but a changed audit log " +
			"left the digest untouched")
	}
}

// SECURITY.md §5: "a scheme where destroying the evidence produces the innocent
// verdict is not a scheme."
func TestSecurityDoc_DestroyedLocalRecordReadsAsBroken(t *testing.T) {
	s, anchor, dir := anchoredDocStore(t)
	if _, _, err := s.PublishCheckpoint(anchor); err != nil {
		t.Fatal(err)
	}

	// Whatever the local record is called, removing it must not soften the
	// verdict. Found by name so the test breaks loudly if the file is renamed.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	removed := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".checkpoints" || e.Name() == "graphene.checkpoints" {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				t.Fatal(err)
			}
			removed++
		}
	}
	if removed != 1 {
		t.Fatalf("expected exactly one local checkpoint record to remove, found %d", removed)
	}

	report, err := s.VerifyAgainstAnchor(anchor)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Broken() {
		t.Fatal("§5 says a published digest with no local checkpoint is broken; it was not reported")
	}
	if bytes.Contains([]byte(report.Summary()), []byte("unanchored")) {
		t.Errorf("§5 says this must never read as unanchored, but the summary is %q", report.Summary())
	}
}

// SECURITY.md §5 "The one implementation, and why you must not use it" — the
// constructor refuses a path inside the store.
func TestSecurityDoc_LocalAnchorRefusesToLiveInsideTheStore(t *testing.T) {
	dir := t.TempDir()
	if _, err := disk.NewInsecureLocalAnchor(filepath.Join(dir, "anchor.bin"), dir); err == nil {
		t.Fatal("§5 says the constructor refuses a path inside the store directory; it did not")
	}
	if _, err := disk.NewInsecureLocalAnchor(filepath.Join(t.TempDir(), "anchor.bin"), dir); err != nil {
		t.Fatalf("a path outside the store was refused: %v", err)
	}
}

// --- §6, redaction ---
//
// §6 promises that content goes and the record stays, that a reason is
// mandatory, that the cascade is capped when the caller caps it, that the
// version hash is the same value as the snapshot leaf, and that the ledger
// survives compaction. Each is executed below.

func redactableDocStore(t *testing.T) (*disk.Store, string) {
	t.Helper()
	dir := t.TempDir()

	key, pub, err := signing.GenerateKey(1)
	if err != nil {
		t.Fatal(err)
	}
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		t.Fatal(err)
	}

	// The configuration §6 spells out.
	opts := disk.StrictOptions(key, ring, 42)
	opts.Redaction = true
	opts.RedactionPolicy = disk.RedactionPolicy{MaxCascade: 50}

	s, err := disk.OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

// SECURITY.md §6 "What survives", the documented flow end to end.
func TestSecurityDoc_RedactionKeepsTheRecord(t *testing.T) {
	s, _ := redactableDocStore(t)

	id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatal(err)
	}

	// The two calls §6 shows.
	impact, err := s.RedactionImpactFor(id)
	if err != nil {
		t.Fatal(err)
	}
	if impact.ExceedsPolicy {
		t.Fatal("a single node with no edges exceeded a cascade limit of 50")
	}

	rec, err := s.RedactNode(id, disk.RedactionRequest{
		ActorID: 7, RoleID: 3, Reason: "subject access request 41",
	})
	if err != nil {
		t.Fatal(err)
	}

	// The content is gone.
	if s.NodeExists(id) {
		t.Fatal("§6 says the content goes; the node is still there")
	}
	// Everything in the table is not.
	if rec.ActorID != 7 || rec.RoleID != 3 || rec.Reason != "subject access request 41" {
		t.Fatalf("§6's table promises actor, role and reason survive: %+v", rec)
	}
	if rec.UnixNano == 0 || rec.VersionHash == (merkle.Hash{}) {
		t.Fatalf("§6's table promises time and version hash survive: %+v", rec)
	}
}

// SECURITY.md §6 "A reason is mandatory."
func TestSecurityDoc_RedactionRequiresAReason(t *testing.T) {
	s, _ := redactableDocStore(t)

	id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.RedactNode(id, disk.RedactionRequest{ActorID: 1})
	if !errors.Is(err, disk.ErrRedactionUnexplained) {
		t.Fatalf("§6 says a reason is mandatory; an unexplained redaction returned %v", err)
	}
	if !s.NodeExists(id) {
		t.Fatal("a refused redaction destroyed the node anyway")
	}
}

// SECURITY.md §6: the version hash "is deliberately the same value as that
// entity's Merkle leaf in a snapshot".
func TestSecurityDoc_RedactionVersionHashIsTheSnapshotLeaf(t *testing.T) {
	s, _ := redactableDocStore(t)

	id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	proof, err := s.ProveNode(id)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := s.RedactNode(id, disk.RedactionRequest{ActorID: 1, Reason: "hash identity"})
	if err != nil {
		t.Fatal(err)
	}

	if rec.VersionHash != merkle.HashLeaf(proof.LeafData) {
		t.Fatal("§6 claims the version hash is the snapshot leaf; a holder of the old " +
			"image cannot match the record against it")
	}
}

// SECURITY.md §6: "The ledger is not in the WAL ... compaction never touches
// it", and its head is bound into §5's checkpoint.
func TestSecurityDoc_RedactionLedgerSurvivesCompactionAndIsAnchored(t *testing.T) {
	s, dir := redactableDocStore(t)

	id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatal(err)
	}

	before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, disk.RedactionRequest{ActorID: 1, Reason: "outlives the log"}); err != nil {
		t.Fatal(err)
	}
	after, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if after.Digest == before.Digest {
		t.Fatal("§6 says the ledger head is bound into the checkpoint; the digest did not move")
	}

	// Compaction must not touch it.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	ledger, err := disk.ReadRedactions(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 1 || ledger[0].Reason != "outlives the log" {
		t.Fatalf("§6 says compaction never touches the ledger; it holds %d records", len(ledger))
	}
	if err := disk.VerifyRedactionChain(ledger, nil); err != nil {
		t.Errorf("the ledger did not survive compaction intact: %v", err)
	}
}

// SECURITY.md §6 "It requires Options.Redaction, and refuses rather than quietly
// falling back to a plain delete."
func TestSecurityDoc_RedactionRefusesWithoutTheOption(t *testing.T) {
	dir := t.TempDir()
	s, err := disk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, disk.RedactionRequest{ActorID: 1, Reason: "no ledger"}); err == nil {
		t.Fatal("§6 says it refuses without Options.Redaction; it did not")
	}
	if !s.NodeExists(id) {
		t.Fatal("§6 says it refuses rather than falling back to a plain delete; the node is gone")
	}
}
