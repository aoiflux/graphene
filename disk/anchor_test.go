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

// External anchoring: the only check that does not compare the store to itself.

// anchoredStore is a fully provisioned store plus an anchor kept outside it.
func anchoredStore(t *testing.T, compactions int) (dir string, s *Store, a *InsecureLocalAnchor, ring *signing.Keyring, ids []store.NodeID) {
	t.Helper()
	dir, s, ring, ids = fullyProvisionedStore(t, compactions)

	// Outside the store directory, which the constructor insists on.
	a, err := NewInsecureLocalAnchor(filepath.Join(t.TempDir(), "anchor.bin"), dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir, s, a, ring, ids
}

// **The whole point.** A published checkpoint is confirmed by the anchor, and
// the store still standing where it was published is not a finding.
func TestAnchor_PublishedCheckpointIsConfirmed(t *testing.T) {
	_, s, a, _, _ := anchoredStore(t, 2)

	c, rec, err := s.PublishCheckpoint(a)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Digest != c.Digest {
		t.Fatalf("the anchor acknowledged %x, the checkpoint is %x", rec.Digest[:8], c.Digest[:8])
	}

	audit, err := s.VerifyAgainstAnchor(a)
	if err != nil {
		t.Fatal(err)
	}
	if audit.Broken() {
		t.Fatalf("a freshly published checkpoint is not a break: %v", audit.Gaps)
	}
	if audit.Matched != 1 {
		t.Fatalf("the anchor confirmed %d of 1 checkpoints", audit.Matched)
	}
	if !audit.CurrentMatchesLast {
		t.Fatal("nothing happened after publishing, so the store should still match")
	}
	if len(audit.Gaps) != 0 {
		t.Fatalf("unexpected findings: %v", audit.Gaps)
	}
}

// **The argument for binding all four heads.** Anchoring the snapshot root alone
// leaves the other three chains free — rewriting only the audit log changes no
// snapshot root. A checkpoint must notice.
func TestAnchor_CheckpointBindsEveryHeadNotJustTheSnapshot(t *testing.T) {
	_, s, _, _, _ := anchoredStore(t, 2)

	before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	// An audit entry and nothing else: no write, no compaction, so the snapshot
	// root cannot move.
	if err := s.recordAudit(AuditCustom, 1, "a change the snapshot cannot see"); err != nil {
		t.Fatal(err)
	}

	after, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if after.SnapshotRoot != before.SnapshotRoot {
		t.Fatal("the snapshot root moved; this test no longer isolates the audit head")
	}
	if after.AuditHead == before.AuditHead {
		t.Fatal("the audit head did not move after an audit entry")
	}
	if after.Digest == before.Digest {
		t.Fatal("the checkpoint digest ignored a change to the audit log — " +
			"anchoring it would be no better than anchoring the snapshot root alone")
	}
}

// A checkpoint commits to a history being *absent*, so "there was no audit log"
// cannot later become "here is the audit log".
func TestAnchor_ZeroHeadsAreCommittedToNotIgnored(t *testing.T) {
	bare, _ := openFresh(t)
	defer bare.Close()

	c, err := bare.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if c.SnapshotRoot != (merkle.Hash{}) || c.AuditCount != 0 || c.SegmentCount != 0 {
		t.Fatalf("an uncompacted, unaudited store should have zero heads: %+v", c)
	}
	if c.Digest == (merkle.Hash{}) {
		t.Fatal("a checkpoint over zero heads produced a zero digest, so it commits to nothing")
	}
}

// **The check worth having.** A local history rewritten after the fact produces
// digests the anchor never saw.
func TestAnchor_RewrittenLocalHistoryIsCaught(t *testing.T) {
	dir, s, a, _, _ := anchoredStore(t, 2)

	if _, _, err := s.PublishCheckpoint(a); err != nil {
		t.Fatal(err)
	}

	// Someone rewrites the local chain: a plausible, internally consistent
	// checkpoint replacing the real one.
	chain, err := ReadCheckpoints(dir)
	if err != nil {
		t.Fatal(err)
	}
	forged := chain[0]
	forged.SnapshotRoot = merkle.Hash{0xFF}
	forged.Digest = computeCheckpointDigest(forged)

	buf := appendCheckpointRecord(nil, forged)
	if err := os.WriteFile(filepath.Join(dir, checkpointFileName), buf, 0600); err != nil {
		t.Fatal(err)
	}

	audit, err := s.VerifyAgainstAnchor(a)
	if err != nil {
		t.Fatal(err)
	}
	// The forged chain passes the local check — that is the point of publishing.
	if cerr := VerifyCheckpointChain(audit.Checkpoints); cerr != nil {
		t.Fatalf("the forgery was clumsy enough to fail the local check, which is not the interesting case: %v", cerr)
	}
	if !audit.Broken() {
		t.Fatal("a checkpoint the anchor never witnessed was accepted")
	}
	if audit.Matched != 0 {
		t.Fatalf("the anchor confirmed %d forged checkpoints", audit.Matched)
	}
}

// The local chain still catches a clumsy edit on its own, which is worth having
// for the case where the anchor is unreachable.
func TestAnchor_LocalChainCatchesEditAndExcision(t *testing.T) {
	_, s, a, _, _ := anchoredStore(t, 1)
	for i := 0; i < 3; i++ {
		addNodeD(t, s, store.NodeTypeTag)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.PublishCheckpoint(a); err != nil {
			t.Fatal(err)
		}
	}

	chain, err := s.CheckpointHistory()
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 {
		t.Fatalf("expected 3 checkpoints, got %d", len(chain))
	}
	if err := VerifyCheckpointChain(chain); err != nil {
		t.Fatalf("an untouched chain did not verify: %v", err)
	}

	edited := append([]Checkpoint(nil), chain...)
	edited[1].AuditCount = 999 // digest no longer follows
	if err := VerifyCheckpointChain(edited); err == nil {
		t.Fatal("an edited checkpoint verified")
	} else if !strings.Contains(err.Error(), "edited") {
		t.Errorf("an edit was not named as one: %v", err)
	}

	excised := []Checkpoint{chain[0], chain[2]}
	if err := VerifyCheckpointChain(excised); err == nil {
		t.Fatal("a chain with an entry removed verified")
	}
}

// A store whose history shrank since it was witnessed is fatal. Retention can
// legitimately remove segments; nothing legitimately removes audit entries.
func TestAnchor_ShrunkHistoryIsFatal(t *testing.T) {
	dir, s, a, ring, _ := anchoredStore(t, 3)
	if _, _, err := s.PublishCheckpoint(a); err != nil {
		t.Fatal(err)
	}

	// Close before deleting: the store holds the audit log open.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the last entry by truncating the file to the earlier entries.
	trimmed := make([]byte, 0)
	for _, e := range entries[:len(entries)-1] {
		trimmed = appendAuditRecord(trimmed, e)
	}
	if err := os.WriteFile(filepath.Join(dir, auditFileName), trimmed, 0600); err != nil {
		t.Fatal(err)
	}

	// A different signing key, but the original ring: the store must still verify
	// the attestation that is already on disk.
	key, _ := newAttestKey(t, 79)
	opts := StrictOptions(key, ring, 500)
	opts.Audit = false
	reopened, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	audit, err := reopened.VerifyAgainstAnchor(a)
	if err != nil {
		t.Fatal(err)
	}
	if !audit.Broken() {
		t.Fatalf("audit entries disappeared and nothing objected: %v", audit.Gaps)
	}
	found := false
	for _, g := range audit.Gaps {
		if g.Fatal && strings.Contains(g.Detail, "shrank") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the shrinkage was not named: %v", audit.Gaps)
	}
}

// A store that moved on since it was witnessed is not broken — it is bounded.
// The report says how much is still resting on the store's own word.
func TestAnchor_MovingOnIsAWindowNotABreak(t *testing.T) {
	_, s, a, _, _ := anchoredStore(t, 1)
	if _, _, err := s.PublishCheckpoint(a); err != nil {
		t.Fatal(err)
	}

	addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	audit, err := s.VerifyAgainstAnchor(a)
	if err != nil {
		t.Fatal(err)
	}
	if audit.Broken() {
		t.Fatalf("ordinary progress was reported as tampering: %v", audit.Gaps)
	}
	if audit.CurrentMatchesLast {
		t.Fatal("the store compacted; it cannot still match the witnessed checkpoint")
	}
	found := false
	for _, g := range audit.Gaps {
		if !g.Fatal && strings.Contains(g.Detail, "moved on") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the unanchored window was not reported: %v", audit.Gaps)
	}
}

// **Destroying the local record must not look like innocence.**
//
// Deleting graphene.checkpoints is strictly easier than forging a chain, and if
// it downgraded the verdict to "never anchored" it would be the obvious move for
// anyone covering their tracks. The anchor still holds the digests, and that
// asymmetry is the entire point.
func TestAnchor_DeletingTheLocalRecordIsFatalNotInnocent(t *testing.T) {
	dir, s, a, _, _ := anchoredStore(t, 2)
	if _, _, err := s.PublishCheckpoint(a); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(dir, checkpointFileName)); err != nil {
		t.Fatal(err)
	}

	audit, err := s.VerifyAgainstAnchor(a)
	if err != nil {
		t.Fatal(err)
	}
	if !audit.Broken() {
		t.Fatalf("deleting the local checkpoint chain was not reported as a break: %v", audit.Gaps)
	}
	if strings.Contains(audit.Summary(), "unanchored") {
		t.Errorf("a store whose record was destroyed summarised as merely unanchored: %q", audit.Summary())
	}
	found := false
	for _, g := range audit.Gaps {
		if g.Fatal && strings.Contains(g.Detail, "no record of") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the orphaned publication was not named: %v", audit.Gaps)
	}
}

// The same attack partway: keep the first checkpoint, drop the rest.
func TestAnchor_PartiallyDeletedLocalRecordIsFatal(t *testing.T) {
	dir, s, a, _, _ := anchoredStore(t, 1)
	for i := 0; i < 3; i++ {
		addNodeD(t, s, store.NodeTypeTag)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.PublishCheckpoint(a); err != nil {
			t.Fatal(err)
		}
	}

	chain, err := ReadCheckpoints(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checkpointFileName),
		appendCheckpointRecord(nil, chain[0]), 0600); err != nil {
		t.Fatal(err)
	}

	audit, err := s.VerifyAgainstAnchor(a)
	if err != nil {
		t.Fatal(err)
	}
	if !audit.Broken() {
		t.Fatalf("two deleted checkpoints went unreported: %v", audit.Gaps)
	}
	// The surviving checkpoint is genuine and still counts as confirmed; the
	// finding is about the two that vanished, not the one that remains.
	if audit.Matched != 1 {
		t.Errorf("the surviving checkpoint was confirmed %d times, want 1", audit.Matched)
	}
}

// A store that never published says so, rather than reporting no findings.
func TestAnchor_NeverPublishedIsReportedNotSilent(t *testing.T) {
	_, s, a, _, _ := anchoredStore(t, 1)

	audit, err := s.VerifyAgainstAnchor(a)
	if err != nil {
		t.Fatal(err)
	}
	if audit.Broken() {
		t.Fatal("never having published is not a break")
	}
	if len(audit.Gaps) != 1 {
		t.Fatalf("expected exactly one finding, got %v", audit.Gaps)
	}
	if !strings.Contains(audit.Summary(), "unanchored") {
		t.Errorf("the summary does not say the store is unanchored: %q", audit.Summary())
	}
}

// **The tripwire.** An anchor inside the store directory is refused, because it
// is the most likely way to build something that looks anchored and is not.
func TestAnchor_RefusesToLiveInsideTheStore(t *testing.T) {
	dir := t.TempDir()

	for _, p := range []string{
		filepath.Join(dir, "anchor.bin"),
		filepath.Join(dir, "nested", "anchor.bin"),
	} {
		if _, err := NewInsecureLocalAnchor(p, dir); !errors.Is(err, ErrAnchorInsideStore) {
			t.Errorf("an anchor at %s inside %s was accepted (err=%v)", p, dir, err)
		}
	}

	outside := filepath.Join(t.TempDir(), "anchor.bin")
	if _, err := NewInsecureLocalAnchor(outside, dir); err != nil {
		t.Errorf("an anchor outside the store was refused: %v", err)
	}
}

// Custody with an anchor: the external gap is answered rather than declared.
func TestAnchor_CustodyForAnchorClosesTheExternalGap(t *testing.T) {
	_, s, a, ring, ids := anchoredStore(t, 2)

	// One publication is enough: the audit entry recording it is inside the
	// checkpoint, so the store settles immediately rather than needing a second
	// round to catch up with itself.
	if _, _, err := s.PublishCheckpoint(a); err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyForAnchor(ids[0], ring, a)
	if err != nil {
		t.Fatal(err)
	}
	if r.Broken() {
		t.Fatalf("an anchored, fully provisioned store reported a break: %v", r.Gaps)
	}
	// Whatever remains must not be the placeholder "no anchor was supplied".
	for _, g := range r.Gaps {
		if strings.Contains(g.Detail, "no externally retained root was supplied") {
			t.Error("the placeholder external gap survived an actual anchor")
		}
	}
}

// The anchor catches the rewrite that every internal check passes.
func TestAnchor_CustodyForAnchorCatchesAWholesaleRewrite(t *testing.T) {
	dir, s, a, ring, ids := anchoredStore(t, 2)

	if _, _, err := s.PublishCheckpoint(a); err != nil {
		t.Fatal(err)
	}

	// The adversary rebuilds the local checkpoint chain to match their rewritten
	// store. Internally flawless; the anchor never saw these digests.
	chain, err := ReadCheckpoints(dir)
	if err != nil {
		t.Fatal(err)
	}
	forged := chain[0]
	forged.AuditCount++
	forged.Digest = computeCheckpointDigest(forged)
	if err := os.WriteFile(filepath.Join(dir, checkpointFileName),
		appendCheckpointRecord(nil, forged), 0600); err != nil {
		t.Fatal(err)
	}

	unanchored, err := s.CustodyFor(ids[0], ring)
	if err != nil {
		t.Fatal(err)
	}
	if unanchored.Broken() {
		t.Fatalf("the store is internally consistent; nothing internal should object: %v", unanchored.Gaps)
	}

	anchored, err := s.CustodyForAnchor(ids[0], ring, a)
	if err != nil {
		t.Fatal(err)
	}
	if !anchored.Broken() {
		t.Fatal("the anchor did not catch a rewrite that every internal check passed")
	}
}

// A torn tail from a crash mid-append leaves everything before it readable.
func TestAnchor_TornTailKeepsWhatCameBefore(t *testing.T) {
	dir, s, a, _, _ := anchoredStore(t, 1)
	for i := 0; i < 2; i++ {
		addNodeD(t, s, store.NodeTypeTag)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.PublishCheckpoint(a); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(dir, checkpointFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-7], 0600); err != nil {
		t.Fatal(err)
	}

	chain, err := ReadCheckpoints(dir)
	if err != nil {
		t.Fatalf("a torn tail made the whole chain unreadable: %v", err)
	}
	if len(chain) != 1 {
		t.Fatalf("expected the one whole checkpoint, got %d", len(chain))
	}
	if err := VerifyCheckpointChain(chain); err != nil {
		t.Fatalf("the surviving prefix did not verify: %v", err)
	}
}

// Publishing is recorded in the audit log, so the act of anchoring is itself
// covered by the next checkpoint.
func TestAnchor_PublishingIsAudited(t *testing.T) {
	dir, s, a, _, _ := anchoredStore(t, 1)

	before, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := s.PublishCheckpoint(a)
	if err != nil {
		t.Fatal(err)
	}
	after, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before)+1 {
		t.Fatalf("publishing added %d audit entries, want 1", len(after)-len(before))
	}
	last := after[len(after)-1]
	if last.Kind != AuditCheckpoint {
		t.Errorf("the entry is a %s, not a checkpoint", last.Kind)
	}

	// **The entry is inside the checkpoint that names it, not after it.** Written
	// afterwards, it would move the audit head and the store would never match
	// the checkpoint it had just published.
	if c.AuditCount != uint64(len(after)) {
		t.Fatalf("the checkpoint covers %d audit entries but the log holds %d; "+
			"the publication record fell outside its own checkpoint", c.AuditCount, len(after))
	}
	if c.AuditHead != last.Hash {
		t.Fatal("the checkpoint's audit head is not the publication entry")
	}

	// And so a capture taken immediately afterwards is identical in every head.
	next, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if next.AuditHead != c.AuditHead || next.SnapshotRoot != c.SnapshotRoot {
		t.Fatal("publishing moved a head after capturing it")
	}
	if next.Prev != c.Digest {
		t.Fatal("the next checkpoint does not link to the published one")
	}
}
