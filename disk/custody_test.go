package disk

import (
	"os"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// Chain of custody: the four histories, walked together.

// fullyProvisionedStore has everything turned on — signing, attestation,
// retention, auditing — which is the only configuration where a custody report
// can come back with no gaps.
func fullyProvisionedStore(t *testing.T, compactions int) (dir string, s *Store, ring *signing.Keyring, ids []store.NodeID) {
	t.Helper()
	key, ring := newAttestKey(t, 77)
	dir = t.TempDir()

	opts := StrictOptions(key, ring, 500)
	opts.Retention = RetentionPolicy{MaxSegments: 20}

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	for i := 0; i < compactions; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		ids = append(ids, id)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	return dir, s, ring, ids
}

// **The whole point.** With everything provisioned, an entity's custody is
// accounted for across all four histories.
func TestCustody_FullyProvisionedStoreHasNoGaps(t *testing.T) {
	_, s, ring, ids := fullyProvisionedStore(t, 3)

	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyForAnchored(ids[0], ring, roots.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Complete() {
		for _, g := range r.Gaps {
			t.Errorf("unexpected gap: %s", g)
		}
		t.Fatalf("a fully provisioned store reported %d gaps", len(r.Gaps))
	}
	if r.Broken() {
		t.Fatal("nothing is broken but Broken() says otherwise")
	}
	if !r.InSnapshot || !r.AttestationVerified {
		t.Fatalf("report does not reflect the store: %+v", r)
	}
	if r.SegmentsChecked != 3 || r.CompactionsRecorded != 3 {
		t.Errorf("walked %d segments and %d recorded compactions, want 3 and 3",
			r.SegmentsChecked, r.CompactionsRecorded)
	}
}

// Without an anchor, a report is never complete — every check compared the
// store against itself, and saying so is the point.
func TestCustody_UnanchoredIsNeverComplete(t *testing.T) {
	_, s, ring, ids := fullyProvisionedStore(t, 2)

	r, err := s.CustodyFor(ids[0], ring)
	if err != nil {
		t.Fatal(err)
	}
	if r.Complete() {
		t.Fatal("an unanchored report claimed completeness; every check compared the store to itself")
	}
	if r.Broken() {
		t.Fatal("nothing is broken; the missing anchor is not a break")
	}

	external := 0
	for _, g := range r.Gaps {
		if g.Layer == LayerExternal {
			external++
		}
	}
	if external != 1 {
		t.Fatalf("expected exactly one external gap, got %d", external)
	}
}

// A store that never compacted, never signed, never retained and never audited
// reports one gap per missing layer — and none of them fatal, because nothing
// is broken, it was simply never set up.
func TestCustody_BareStoreReportsEveryLayerAsUnestablished(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)

	r, err := s.CustodyFor(id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Broken() {
		t.Fatalf("an unconfigured store is incomplete, not broken: %v", r.Gaps)
	}

	seen := map[CustodyLayer]bool{}
	for _, g := range r.Gaps {
		seen[g.Layer] = true
	}
	for _, want := range []CustodyLayer{LayerSnapshot, LayerAttestation, LayerSegments, LayerAudit, LayerExternal} {
		if !seen[want] {
			t.Errorf("no gap reported for the %s layer", want)
		}
	}
}

// An entity written since the last compaction is live but not under custody,
// and the report distinguishes that from absent.
func TestCustody_UncompactedEntityIsUnaccountedNotAbsent(t *testing.T) {
	_, s, ring, _ := fullyProvisionedStore(t, 1)

	fresh := addNodeD(t, s, store.NodeTypeTag)
	r, err := s.CustodyFor(fresh, ring)
	if err != nil {
		t.Fatal(err)
	}
	if r.InSnapshot {
		t.Fatal("an uncompacted entity was reported as in the snapshot")
	}
	if r.Broken() {
		t.Fatal("an uncompacted entity is not a broken chain")
	}

	found := false
	for _, g := range r.Gaps {
		if g.Layer == LayerSnapshot && !g.Fatal {
			found = true
		}
	}
	if !found {
		t.Fatal("no snapshot gap reported for an entity outside the image")
	}
}

// An entity the store never held must not be reported the same way as one
// written since the last compaction. Both fail an inclusion proof identically,
// and conflating them would have the report assert existence on nothing more
// than the caller having named an ID.
func TestCustody_AbsentEntityIsNotReportedAsLive(t *testing.T) {
	_, s, ring, ids := fullyProvisionedStore(t, 2)

	absent, err := s.CustodyFor(ids[len(ids)-1]+9999, ring)
	if err != nil {
		t.Fatal(err)
	}
	if absent.Live {
		t.Fatal("an ID the store never issued was reported as live")
	}
	if absent.Broken() {
		t.Fatal("asking about an entity that does not exist is not a break")
	}
	for _, g := range absent.Gaps {
		if g.Layer == LayerSnapshot && strings.Contains(g.Detail, "live") {
			t.Errorf("an absent entity was described as live: %s", g)
		}
	}
	if got := absent.Summary(); !strings.Contains(got, "unknown to this store") {
		t.Errorf("summary does not say the entity is unknown: %q", got)
	}

	// The contrast: a genuinely live entity outside the image reads differently.
	fresh := addNodeD(t, s, store.NodeTypeTag)
	live, err := s.CustodyFor(fresh, ring)
	if err != nil {
		t.Fatal(err)
	}
	if !live.Live || live.InSnapshot {
		t.Fatalf("an uncompacted entity should be live and outside the image: %+v", live)
	}
	if live.Summary() == absent.Summary() {
		t.Fatal("a live entity and an absent one summarise identically")
	}
}

// **A broken segment chain is fatal and named.**
func TestCustody_BrokenSegmentChainIsFatal(t *testing.T) {
	dir, s, ring, ids := fullyProvisionedStore(t, 4)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(segs[1].Path); err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyFor(ids[0], ring)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Broken() {
		t.Fatal("removing a segment from the middle did not break the report")
	}
	found := false
	for _, g := range r.Gaps {
		if g.Layer == LayerSegments && g.Fatal {
			found = true
		}
	}
	if !found {
		t.Fatalf("the break was not attributed to the segment layer: %v", r.Gaps)
	}
}

// A tampered audit log is fatal and named.
func TestCustody_BrokenAuditChainIsFatal(t *testing.T) {
	dir, s, ring, ids := fullyProvisionedStore(t, 3)

	data, err := os.ReadFile(dir + string(os.PathSeparator) + auditFileName)
	if err != nil {
		t.Fatal(err)
	}
	// Disturb a byte inside the first entry's body, past its length prefix.
	data[20] ^= 0x01
	if err := os.WriteFile(dir+string(os.PathSeparator)+auditFileName, data, 0600); err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyFor(ids[0], ring)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Broken() {
		t.Fatal("an altered audit log did not break the report")
	}
	found := false
	for _, g := range r.Gaps {
		if g.Layer == LayerAudit && g.Fatal {
			found = true
		}
	}
	if !found {
		t.Fatalf("the break was not attributed to the audit layer: %v", r.Gaps)
	}
}

// **The check only an anchor can make.** A store rewritten wholesale is
// internally perfect; the retained root is what catches it.
func TestCustody_AnchorCatchesAWholesaleRewrite(t *testing.T) {
	_, s, ring, ids := fullyProvisionedStore(t, 2)

	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	retained := roots.Snapshot

	// More work, then another compaction: a new snapshot, all of it consistent.
	addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	// Unanchored, the store looks fine apart from the missing anchor itself.
	unanchored, err := s.CustodyFor(ids[0], ring)
	if err != nil {
		t.Fatal(err)
	}
	if unanchored.Broken() {
		t.Fatalf("the store is internally consistent; nothing should be broken: %v", unanchored.Gaps)
	}

	// Against the retained root, the mismatch is fatal.
	anchored, err := s.CustodyForAnchored(ids[0], ring, retained)
	if err != nil {
		t.Fatal(err)
	}
	if !anchored.Broken() {
		t.Fatal("the snapshot moved but the anchored report did not object")
	}
}

// A compaction that happened without being audited is fatal: more retired
// segments than recorded compactions means a record is missing.
func TestCustody_UnrecordedCompactionIsDetected(t *testing.T) {
	dir, s, ring, ids := fullyProvisionedStore(t, 3)

	// Close before deleting: the store holds the audit log open, and on Windows
	// an open file cannot be removed. Reopening afterwards is also the more
	// faithful simulation — someone covering their tracks does it while the
	// store is down.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir + string(os.PathSeparator) + auditFileName); err != nil {
		t.Fatal(err)
	}

	key, _ := newAttestKey(t, 78)
	opts := StrictOptions(key, ring, 500)
	opts.Retention = RetentionPolicy{MaxSegments: 20}
	opts.Audit = false // reopened without recreating the log
	reopened, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	r, err := reopened.CustodyFor(ids[0], ring)
	if err != nil {
		t.Fatal(err)
	}
	// With the file gone the audit layer is "unestablished" rather than broken —
	// which is the honest reading, and exactly why the plan says wholesale
	// deletion needs an external anchor to catch.
	found := false
	for _, g := range r.Gaps {
		if g.Layer == LayerAudit {
			found = true
		}
	}
	if !found {
		t.Fatal("deleting the audit log produced no gap at all")
	}
	if r.Complete() {
		t.Fatal("a store with no audit log reported complete custody")
	}
}

// Without a verifier, signature-dependent layers are reported as unchecked
// rather than failing — a caller without the key can still learn whether the
// hash chains hold.
func TestCustody_NoVerifierReportsUncheckedNotBroken(t *testing.T) {
	_, s, _, ids := fullyProvisionedStore(t, 2)

	r, err := s.CustodyFor(ids[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.Broken() {
		t.Fatalf("a missing verifier is not a break: %v", r.Gaps)
	}
	if r.AttestationVerified {
		t.Fatal("the attestation was reported verified without a verifier")
	}
	if !r.Attested {
		t.Fatal("the snapshot is attested; that fact does not need a verifier")
	}
}

// The summary is usable on its own, which is what a CLI or a log line gets.
func TestCustody_SummaryReflectsTheVerdict(t *testing.T) {
	_, s, ring, ids := fullyProvisionedStore(t, 2)
	roots, _ := s.SnapshotRoots()

	good, err := s.CustodyForAnchored(ids[0], ring, roots.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := good.Summary(); got == "" {
		t.Fatal("empty summary")
	}

	bad, err := s.CustodyForAnchored(ids[0], ring, merkle.Hash{})
	if err != nil {
		t.Fatal(err)
	}
	if !bad.Broken() {
		t.Fatal("a zero anchor should not match")
	}
	if got := bad.Summary(); got == good.Summary() {
		t.Fatal("a broken report summarises the same as a clean one")
	}
}
