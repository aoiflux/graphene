package disk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// The audit log: what operator actions it records, and what a chain break looks
// like.

func auditedStore(t *testing.T, opts Options) (dir string, s *Store) {
	t.Helper()
	dir = t.TempDir()
	opts.Audit = true

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return dir, s
}

// A compaction is recorded, with what it did.
func TestAudit_RecordsCompaction(t *testing.T) {
	dir, s := auditedStore(t, Options{AttestActorID: 55})

	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("recorded %d entries after one compaction, want 1", len(entries))
	}
	e := entries[0]
	if e.Kind != AuditCompact {
		t.Errorf("entry kind %v, want compact", e.Kind)
	}
	if e.ActorID != 55 {
		t.Errorf("entry actor %d, want 55", e.ActorID)
	}
	if e.Detail == "" {
		t.Error("entry carries no detail; 'a compaction happened' is not an audit record")
	}
	if e.UnixNano == 0 {
		t.Error("entry carries no timestamp")
	}
}

// A key rotation is recorded — the action a compromise investigation asks about
// first.
func TestAudit_RecordsKeyRotation(t *testing.T) {
	k1, _, err := signing.GenerateKey(1)
	if err != nil {
		t.Fatal(err)
	}
	k2, p2, err := signing.GenerateKey(2)
	if err != nil {
		t.Fatal(err)
	}

	dir, s := auditedStore(t, Options{Signer: k1, AttestActorID: 9})
	if err := s.RotateKey(k2, p2); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if e.Kind == AuditKeyRotation {
			found = true
			if e.Detail == "" {
				t.Error("the rotation entry does not say which keys were involved")
			}
		}
	}
	if !found {
		t.Fatal("a key rotation was not recorded")
	}
}

// **Deleting evidence is itself recorded.** A retention policy discarding
// history is exactly what an audit is for, even when the discarding was
// intended.
func TestAudit_RecordsRetentionDeletion(t *testing.T) {
	dir, s := auditedStore(t, Options{
		AttestActorID: 3,
		Retention:     RetentionPolicy{MaxSegments: 1},
	})

	for i := 0; i < 3; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	deletions := 0
	for _, e := range entries {
		if e.Kind == AuditRetentionDelete {
			deletions++
		}
	}
	if deletions == 0 {
		t.Fatal("retention deleted segments and recorded nothing; " +
			"discarding evidence is precisely what should be audited")
	}
}

// The chain holds, and every entry hashes to what it claims.
func TestAudit_ChainVerifies(t *testing.T) {
	dir, s := auditedStore(t, Options{AttestActorID: 1})
	for i := 0; i < 4; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 4 {
		t.Fatalf("only %d entries recorded", len(entries))
	}
	if err := VerifyAuditChain(entries); err != nil {
		t.Fatalf("a genuine chain did not verify: %v", err)
	}
}

// Editing an entry's contents breaks its own hash, and the error says so.
func TestAudit_DetectsAnEditedEntry(t *testing.T) {
	dir, s := auditedStore(t, Options{AttestActorID: 1})
	for i := 0; i < 3; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	entries[1].Detail = "something else entirely"

	err = VerifyAuditChain(entries)
	if err == nil {
		t.Fatal("an edited entry verified")
	}
	if !errors.Is(err, ErrAuditChainBroken) {
		t.Fatalf("expected a chain error, got: %v", err)
	}
}

// Removing an entry from the middle breaks the link in its successor, which is
// the realistic threat: excising one embarrassing record rather than deleting
// the whole file.
func TestAudit_DetectsAnExcisedEntry(t *testing.T) {
	dir, s := auditedStore(t, Options{AttestActorID: 1})
	for i := 0; i < 4; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 4 {
		t.Fatalf("need at least 4 entries, got %d", len(entries))
	}

	excised := append(append([]AuditEntry{}, entries[:1]...), entries[2:]...)
	if err := VerifyAuditChain(excised); err == nil {
		t.Fatal("removing an entry from the middle left the chain verifying")
	}
}

// The chain resumes across a reopen rather than restarting, so a restart does
// not look like a break.
func TestAudit_ChainResumesAcrossReopen(t *testing.T) {
	dir, s := auditedStore(t, Options{AttestActorID: 1})
	addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithOptions(dir, Options{Audit: true, AttestActorID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	addNodeD(t, reopened, store.NodeTypeTag)
	if err := reopened.Compact(); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("recorded %d entries across a reopen, want 2", len(entries))
	}
	if err := VerifyAuditChain(entries); err != nil {
		t.Fatalf("the chain did not survive a reopen: %v", err)
	}
}

// A caller can record its own events, for the auditing the engine deliberately
// does not do.
func TestAudit_CallerRecordedEvents(t *testing.T) {
	dir, s := auditedStore(t, Options{AttestActorID: 1})

	if err := s.recordAudit(AuditCustom, 77, "exported case bundle CASE-2026-014"); err != nil {
		t.Fatal(err)
	}
	if err := s.recordAudit(AuditAttestationExport, 77, "node 4021 to external counsel"); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("recorded %d entries, want 2", len(entries))
	}
	if entries[0].ActorID != 77 || entries[1].Kind != AuditAttestationExport {
		t.Fatalf("caller events not recorded as given: %+v", entries)
	}
	if err := VerifyAuditChain(entries); err != nil {
		t.Fatal(err)
	}
}

// Auditing is off by default, and a store without it writes no file.
func TestAudit_OffByDefault(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, auditFileName)); !os.IsNotExist(err) {
		t.Fatal("an unaudited store wrote an audit file")
	}
	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatalf("reading an absent audit log should not fail: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("an absent audit log reported %d entries", len(entries))
	}
}

// A torn tail is what a crash mid-append leaves. Reading stops there rather
// than failing, and what came before is still usable.
func TestAudit_TornTailIsTruncatedNotFatal(t *testing.T) {
	dir, s := auditedStore(t, Options{AttestActorID: 1})
	for i := 0; i < 3; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	p := filepath.Join(dir, auditFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data[:len(data)-7], 0600); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatalf("a torn tail should truncate, not fail: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("read %d entries from a log whose last was torn, want 2", len(entries))
	}
	if err := VerifyAuditChain(entries); err != nil {
		t.Fatalf("what survived the tear does not verify: %v", err)
	}
}

// StrictOptions turns auditing on, so the safe posture includes a record of
// what was done.
func TestAudit_StrictOptionsEnablesIt(t *testing.T) {
	key, ring := newAttestKey(t, 4)
	if !StrictOptions(key, ring, 1).Audit {
		t.Fatal("StrictOptions leaves auditing off")
	}
}

// A caller can record their own events, which is what SECURITY.md §3 promises.
func TestAudit_CallerCanRecordTheirOwnEvents(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.RecordAudit(AuditCustom, 9, "exported to case file 2026-114"); err != nil {
		t.Fatal(err)
	}

	entries, err := s.AuditEntries()
	if err != nil {
		t.Fatal(err)
	}
	last := entries[len(entries)-1]
	if last.Kind != AuditCustom || last.ActorID != 9 {
		t.Fatalf("the caller's entry was not recorded as given: %+v", last)
	}
	if last.Detail != "exported to case file 2026-114" {
		t.Errorf("detail was altered: %q", last.Detail)
	}
	if err := VerifyAuditChain(entries); err != nil {
		t.Errorf("a caller entry broke the chain: %v", err)
	}
}

// **A caller must not be able to forge engine history.** CustodyFor compares
// recorded compactions against retired segments; if a caller could write
// AuditCompact, that check could be satisfied by lying.
func TestAudit_CallerCannotForgeEngineKinds(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{Audit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, k := range []AuditKind{AuditOpen, AuditCompact, AuditKeyRotation,
		AuditRetentionDelete, AuditVerify, AuditAttestationExport, AuditCheckpoint} {
		if err := s.RecordAudit(k, 1, "forged"); !errors.Is(err, ErrAuditKindReserved) {
			t.Errorf("a caller was allowed to write %s (err=%v)", k, err)
		}
	}

	entries, err := s.AuditEntries()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Detail == "forged" {
			t.Fatal("a rejected entry was written to the log anyway")
		}
	}
}

// Recording with auditing off is a no-op rather than an error: a caller should
// not have to branch on configuration they may not own.
func TestAudit_RecordingWithAuditingOffIsSilent(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	if err := s.RecordAudit(AuditCustom, 1, "nowhere to go"); err != nil {
		t.Fatalf("recording into a store with no audit log errored: %v", err)
	}
}
