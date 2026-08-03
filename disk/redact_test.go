package disk

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// Redaction: destroying content without destroying the record that it existed.

func redactableStore(t *testing.T) (dir string, s *Store, ring *signing.Keyring) {
	t.Helper()
	key, ring := newAttestKey(t, 91)
	dir = t.TempDir()

	opts := StrictOptions(key, ring, 91)
	opts.Redaction = true
	opts.Retention = RetentionPolicy{MaxSegments: 20}

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return dir, s, ring
}

// hubWithSpokes builds a node with n incident edges, which is the shape T-11
// worries about.
func hubWithSpokes(t *testing.T, s *Store, n int) (hub store.NodeID) {
	t.Helper()
	hub = addNodeD(t, s, store.NodeTypeMicroArtefact)
	for i := 0; i < n; i++ {
		spoke := addNodeD(t, s, store.NodeTypeTag)
		if _, err := s.AddEdge(&store.Edge{
			Src: hub, Dst: spoke,
			Labels: []store.EdgeType{store.EdgeTypeTaggedWith},
		}); err != nil {
			t.Fatal(err)
		}
	}
	return hub
}

// **The whole point.** The content is gone; the fact, actor, time, reason and
// version hash are not.
func TestRedact_ContentGoesTheRecordStays(t *testing.T) {
	_, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)

	// The version hash before, so the record can be checked against it.
	impact, err := s.RedactionImpactFor(id)
	if err != nil {
		t.Fatal(err)
	}
	if impact.VersionHash == (merkle.Hash{}) {
		t.Fatal("a live node produced a zero version hash")
	}

	rec, err := s.RedactNode(id, RedactionRequest{ActorID: 7, RoleID: 3, Reason: "subject access request 41"})
	if err != nil {
		t.Fatal(err)
	}

	if s.NodeExists(id) {
		t.Fatal("the node survived its own redaction")
	}
	if rec.VersionHash != impact.VersionHash {
		t.Fatal("the record's version hash is not the one the node had")
	}
	if rec.ActorID != 7 || rec.RoleID != 3 || rec.Reason != "subject access request 41" {
		t.Fatalf("the record does not carry what was supplied: %+v", rec)
	}

	// And it is on disk, chained and signed.
	ledger, err := s.Redactions()
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 1 {
		t.Fatalf("the ledger holds %d records, want 1", len(ledger))
	}
	if err := VerifyRedactionChain(ledger, ring); err != nil {
		t.Fatalf("the ledger did not verify: %v", err)
	}
	if len(ledger[0].Signature) == 0 {
		t.Error("a signed store produced an unsigned redaction record")
	}
}

// **A redaction must be explained.** An unexplained one is indistinguishable
// from evidence destruction, which is the distinction this whole file draws.
func TestRedact_RefusesWithoutAReason(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)

	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1}); !errors.Is(err, ErrRedactionUnexplained) {
		t.Fatalf("an unexplained redaction was accepted (err=%v)", err)
	}
	if !s.NodeExists(id) {
		t.Fatal("a refused redaction destroyed the node anyway")
	}
}

// The impact report exists to be read while refusing is still possible.
func TestRedact_ImpactIsReportedBeforeAnythingIsDestroyed(t *testing.T) {
	_, s, _ := redactableStore(t)
	hub := hubWithSpokes(t, s, 5)

	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	if len(impact.CascadedEdges) != 5 {
		t.Fatalf("the impact report names %d edges, want 5", len(impact.CascadedEdges))
	}
	if !s.NodeExists(hub) {
		t.Fatal("asking what would be destroyed destroyed it")
	}
	if impact.ExceedsPolicy {
		t.Fatal("no policy is set, so nothing can exceed it")
	}
}

// **T-11: an over-broad cascade.** The engine has no opinion on how much is too
// much, but it enforces the caller's.
func TestRedact_PolicyCapsTheCascade(t *testing.T) {
	key, ring := newAttestKey(t, 92)
	dir := t.TempDir()
	opts := StrictOptions(key, ring, 92)
	opts.Redaction = true
	opts.RedactionPolicy = RedactionPolicy{MaxCascade: 3}

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	hub := hubWithSpokes(t, s, 10)

	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	if !impact.ExceedsPolicy {
		t.Fatal("a 10-edge cascade did not exceed a limit of 3")
	}

	_, err = s.RedactNode(hub, RedactionRequest{ActorID: 1, Reason: "too broad"})
	if !errors.Is(err, ErrCascadeTooLarge) {
		t.Fatalf("an over-broad redaction was accepted (err=%v)", err)
	}
	if !s.NodeExists(hub) {
		t.Fatal("a refused redaction destroyed the node anyway")
	}

	// A small one still goes through.
	small := addNodeD(t, s, store.NodeTypeTag)
	if _, err := s.RedactNode(small, RedactionRequest{ActorID: 1, Reason: "within policy"}); err != nil {
		t.Fatalf("a redaction inside the policy was refused: %v", err)
	}
}

// The record describes exactly the set that was removed, not a set recomputed
// afterwards.
func TestRedact_RecordNamesTheEdgesActuallyRemoved(t *testing.T) {
	_, s, _ := redactableStore(t)
	hub := hubWithSpokes(t, s, 4)

	rec, err := s.RedactNode(hub, RedactionRequest{ActorID: 2, Reason: "cascade check"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.CascadedEdges) != 4 {
		t.Fatalf("the record names %d cascaded edges, want 4", len(rec.CascadedEdges))
	}
	for _, eid := range rec.CascadedEdges {
		if _, err := s.GetEdge(eid); err == nil {
			t.Errorf("edge %d is named in the record but still present", eid)
		}
	}
}

// A store that did not opt in refuses rather than silently degrading to an
// unrecorded delete.
func TestRedact_RefusesWhenTheLedgerIsOff(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)

	_, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "no ledger here"})
	if err == nil {
		t.Fatal("a store with no ledger performed a redaction")
	}
	if !strings.Contains(err.Error(), "Options.Redaction") {
		t.Errorf("the error does not say how to enable it: %v", err)
	}
	if !s.NodeExists(id) {
		t.Fatal("a refused redaction destroyed the node anyway")
	}
}

// The ledger survives compaction, which is the reason it is not in the WAL.
func TestRedact_LedgerSurvivesCompaction(t *testing.T) {
	_, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 5, Reason: "outlives the log"}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		addNodeD(t, s, store.NodeTypeTag)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}

	ledger, err := s.Redactions()
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 1 {
		t.Fatalf("compaction left %d redaction records, want 1", len(ledger))
	}
	if ledger[0].Reason != "outlives the log" {
		t.Error("the surviving record is not the one written")
	}
	if err := VerifyRedactionChain(ledger, ring); err != nil {
		t.Errorf("the ledger did not survive compaction intact: %v", err)
	}
}

// An edited or excised record breaks the chain, and the two are distinguished.
func TestRedact_ChainCatchesEditAndExcision(t *testing.T) {
	_, s, ring := redactableStore(t)
	for i := 0; i < 3; i++ {
		id := addNodeD(t, s, store.NodeTypeTag)
		if _, err := s.RedactNode(id, RedactionRequest{ActorID: uint64(i), Reason: "batch"}); err != nil {
			t.Fatal(err)
		}
	}

	ledger, err := s.Redactions()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyRedactionChain(ledger, ring); err != nil {
		t.Fatalf("an untouched ledger did not verify: %v", err)
	}

	edited := append([]RedactionRecord(nil), ledger...)
	edited[1].Reason = "a different reason"
	if err := VerifyRedactionChain(edited, nil); err == nil {
		t.Fatal("an edited record verified")
	} else if !strings.Contains(err.Error(), "edited") {
		t.Errorf("an edit was not named as one: %v", err)
	}

	excised := []RedactionRecord{ledger[0], ledger[2]}
	if err := VerifyRedactionChain(excised, nil); err == nil {
		t.Fatal("a ledger with a record removed verified")
	}
}

// A forged signature is caught when a verifier is supplied, and the hash chain
// still holds without one.
func TestRedact_ForgedSignatureIsCaught(t *testing.T) {
	_, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "signature check"}); err != nil {
		t.Fatal(err)
	}

	ledger, err := s.Redactions()
	if err != nil {
		t.Fatal(err)
	}
	ledger[0].Signature[0] ^= 0x01

	if err := VerifyRedactionChain(ledger, ring); err == nil {
		t.Fatal("a damaged signature verified")
	}
	if err := VerifyRedactionChain(ledger, nil); err != nil {
		t.Errorf("without a verifier the hash chain should still hold: %v", err)
	}
}

// A torn tail leaves everything before it readable.
func TestRedact_TornTailKeepsWhatCameBefore(t *testing.T) {
	dir, s, _ := redactableStore(t)
	for i := 0; i < 2; i++ {
		id := addNodeD(t, s, store.NodeTypeTag)
		if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "torn tail"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, redactionFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-9], 0600); err != nil {
		t.Fatal(err)
	}

	ledger, err := ReadRedactions(dir)
	if err != nil {
		t.Fatalf("a torn tail made the whole ledger unreadable: %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("expected the one whole record, got %d", len(ledger))
	}
	if err := VerifyRedactionChain(ledger, nil); err != nil {
		t.Errorf("the surviving prefix did not verify: %v", err)
	}
}

// A record claiming more cascaded edges than the buffer holds must be refused,
// not allocated for. Same discipline as every other length prefix read from a
// file here.
func TestRedact_RefusesAnImpossibleCascadeCount(t *testing.T) {
	dir := t.TempDir()

	// A minimal record whose cascade count claims a great many edges.
	body := appendRedactionRecord(nil, RedactionRecord{Seq: 1, NodeID: 1, Reason: "x"})
	// Overwrite the cascade count (first field after the fixed head) with a
	// number no 60-byte record could satisfy.
	const head = 4 + 8 + 8 + 8 + 4 + 8 + 32 + 32 + 32 + 8
	body[head] = 0xFF
	body[head+1] = 0xFF
	body[head+2] = 0xFF
	body[head+3] = 0x7F

	if err := os.WriteFile(filepath.Join(dir, redactionFileName), body, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRedactions(dir); err == nil {
		t.Fatal("a record claiming 2 billion cascaded edges was accepted")
	}
}

// Custody: a redacted entity is a documented removal, not an unexplained hole.
func TestRedact_CustodyReportsARedactionRatherThanAGap(t *testing.T) {
	_, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 8, RoleID: 2, Reason: "erasure order 9"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyFor(id, ring)
	if err != nil {
		t.Fatal(err)
	}
	if r.Redacted == nil {
		t.Fatal("custody did not find the redaction record")
	}
	if r.Redacted.Reason != "erasure order 9" {
		t.Errorf("the wrong record was attached: %+v", r.Redacted)
	}
	if got := r.Summary(); !strings.Contains(got, "redacted") {
		t.Errorf("the summary does not say the entity was redacted: %q", got)
	}
	// It must not read as an entity the store never held.
	if strings.Contains(r.Summary(), "unknown to this store") {
		t.Error("a documented removal summarised as an entity that never existed")
	}
}

// A broken ledger is fatal to custody, and named.
func TestRedact_BrokenLedgerIsFatalToCustody(t *testing.T) {
	dir, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	other := addNodeD(t, s, store.NodeTypeTag)
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "will be tampered"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, redactionFileName))
	if err != nil {
		t.Fatal(err)
	}
	data[12] ^= 0x01 // inside the body, past the length prefix
	if err := os.WriteFile(filepath.Join(dir, redactionFileName), data, 0600); err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyFor(other, ring)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Broken() {
		t.Fatalf("a tampered redaction ledger did not break custody: %v", r.Gaps)
	}
	found := false
	for _, g := range r.Gaps {
		if g.Layer == LayerRedaction && g.Fatal {
			found = true
		}
	}
	if !found {
		t.Fatalf("the break was not attributed to the redaction layer: %v", r.Gaps)
	}
}

// **Deleting the ledger must be externally detectable.** That is why the
// redaction head is bound into the checkpoint.
func TestRedact_LedgerHeadIsBoundIntoTheCheckpoint(t *testing.T) {
	_, s, _ := redactableStore(t)

	before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	id := addNodeD(t, s, store.NodeTypeTag)
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "moves the head"}); err != nil {
		t.Fatal(err)
	}

	after, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if after.RedactionHead == before.RedactionHead {
		t.Fatal("a redaction did not move the checkpoint's redaction head")
	}
	if after.RedactionCount != before.RedactionCount+1 {
		t.Fatalf("the checkpoint counts %d redactions, want %d",
			after.RedactionCount, before.RedactionCount+1)
	}
	if after.Digest == before.Digest {
		t.Fatal("the checkpoint digest ignored the redaction ledger, so deleting it " +
			"would be externally invisible")
	}
}

// The audit log records the redaction too, so the two chains corroborate.
func TestRedact_IsAudited(t *testing.T) {
	dir, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 4, Reason: "audited"}); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	last := entries[len(entries)-1]
	if last.Kind != AuditRedaction {
		t.Fatalf("the last audit entry is a %s, not a redaction", last.Kind)
	}
	if last.ActorID != 4 || !strings.Contains(last.Detail, "audited") {
		t.Errorf("the audit entry does not carry the redaction's attribution: %+v", last)
	}
}

// Redacting something that is not there is an error, not a silent record.
func TestRedact_AbsentEntityIsRefused(t *testing.T) {
	_, s, _ := redactableStore(t)

	if _, err := s.RedactNode(store.NodeID(9999), RedactionRequest{ActorID: 1, Reason: "nothing here"}); err == nil {
		t.Fatal("redacting an entity that does not exist succeeded")
	}
	ledger, err := s.Redactions()
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 0 {
		t.Fatalf("a refused redaction wrote %d records", len(ledger))
	}
}

// The version hash is the snapshot's leaf hash for the same node, so a party
// holding an old image can match a record against it directly.
func TestRedact_VersionHashMatchesTheSnapshotLeaf(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	proof, err := s.ProveNode(id)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "hash identity"})
	if err != nil {
		t.Fatal(err)
	}
	if rec.VersionHash != merkle.HashLeaf(proof.LeafData) {
		t.Fatalf("the redaction record's version hash %x is not the snapshot leaf %x; "+
			"a holder of the old image cannot match the record against it",
			rec.VersionHash[:8], merkle.HashLeaf(proof.LeafData))
	}
}

// The report must not contradict itself: a redacted entity's snapshot gap
// cannot say the store never held it while the redaction gap explains exactly
// when it did.
func TestRedact_CustodyGapsDoNotContradictEachOther(t *testing.T) {
	_, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 3, Reason: "consistency"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyFor(id, ring)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range r.Gaps {
		if g.Layer == LayerSnapshot && strings.Contains(g.Detail, "no such entity") {
			t.Errorf("the snapshot gap denies an entity the redaction gap accounts for: %s", g)
		}
	}
}
