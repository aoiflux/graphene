package disk

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// Roles and capabilities: an audit mechanism, and honest about it.

func roleStore(t *testing.T) (dir string, s *Store, ring *signing.Keyring) {
	t.Helper()
	key, ring := newAttestKey(t, 61)
	dir = t.TempDir()

	opts := StrictOptions(key, ring, 61)
	opts.Roles = true

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return dir, s, ring
}

// **INV-3, by construction.** Capabilities are a pure function of the ledger, so
// a capability cannot appear without a record — the invariant is not checked, it
// is unexpressible to violate.
func TestRBAC_CapabilitiesComeOnlyFromTheLedger(t *testing.T) {
	_, s, ring := roleStore(t)

	// Before anything, nobody holds anything.
	caps, err := s.Capabilities(7)
	if err != nil {
		t.Fatal(err)
	}
	if caps != 0 {
		t.Fatalf("an unmentioned actor holds %s", caps)
	}

	if _, err := s.GrantRole(7, 3, CapRead|CapWrite, GrantRequest{
		GrantedBy: 1, Reason: "analyst onboarding",
	}); err != nil {
		t.Fatal(err)
	}

	caps, err = s.Capabilities(7)
	if err != nil {
		t.Fatal(err)
	}
	if !caps.Has(CapRead | CapWrite) {
		t.Fatalf("actor 7 holds %s after being granted read|write", caps)
	}
	if caps.Has(CapRedact) {
		t.Fatal("actor 7 holds a capability nobody granted")
	}

	// The derivation is reproducible by a third party holding only the ledger.
	records, err := s.Grants()
	if err != nil {
		t.Fatal(err)
	}
	if got := CapabilitiesFrom(records)[7]; got != caps {
		t.Fatalf("the store says %s, the ledger says %s", caps, got)
	}
	if err := VerifyGrantChain(records, ring); err != nil {
		t.Fatalf("the grant ledger did not verify: %v", err)
	}
	if len(records[0].Signature) == 0 {
		t.Error("a signed store produced an unsigned grant record")
	}
}

// Revocation removes exactly what it names, and leaves the rest.
func TestRBAC_RevocationIsScoped(t *testing.T) {
	_, s, _ := roleStore(t)

	if _, err := s.GrantRole(7, 3, CapRead|CapWrite|CapRedact, GrantRequest{
		GrantedBy: 1, Reason: "initial",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RevokeRole(7, 3, CapRedact, GrantRequest{
		GrantedBy: 1, Reason: "redaction moved to the compliance role",
	}); err != nil {
		t.Fatal(err)
	}

	caps, err := s.Capabilities(7)
	if err != nil {
		t.Fatal(err)
	}
	if caps.Has(CapRedact) {
		t.Fatal("a revoked capability survived")
	}
	if !caps.Has(CapRead | CapWrite) {
		t.Fatalf("revocation took more than it named: %s", caps)
	}

	// An actor stripped of everything reads the same as one never mentioned,
	// because those are the same thing.
	if _, err := s.RevokeRole(7, 3, CapRead|CapWrite, GrantRequest{
		GrantedBy: 1, Reason: "offboarding",
	}); err != nil {
		t.Fatal(err)
	}
	records, err := s.Grants()
	if err != nil {
		t.Fatal(err)
	}
	if _, present := CapabilitiesFrom(records)[7]; present {
		t.Fatal("an actor with no capabilities is still listed")
	}
}

// **A privilege change must be explained**, for the same reason a redaction must:
// an unexplained one is indistinguishable from an escalation.
func TestRBAC_RefusesWithoutAReason(t *testing.T) {
	_, s, _ := roleStore(t)

	if _, err := s.GrantRole(7, 3, CapWrite, GrantRequest{GrantedBy: 1}); !errors.Is(err, ErrGrantUnexplained) {
		t.Fatalf("an unexplained grant was accepted (err=%v)", err)
	}
	if _, err := s.GrantRole(7, 3, 0, GrantRequest{GrantedBy: 1, Reason: "nothing"}); err == nil {
		t.Fatal("a grant naming no capability was accepted")
	}

	records, err := s.Grants()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("a refused grant wrote %d records", len(records))
	}
}

// **Self-granting is legal and recorded as such.** An audit wants to see it, not
// be protected from it by a check that anyone in-process can step around.
func TestRBAC_SelfGrantIsRecordedNotRefused(t *testing.T) {
	_, s, _ := roleStore(t)

	rec, err := s.GrantRole(7, 3, CapGrant|CapRedact, GrantRequest{
		GrantedBy: 7, Reason: "granting myself",
	})
	if err != nil {
		t.Fatalf("a self-grant was refused; the engine is not a boundary: %v", err)
	}
	if rec.Subject != rec.GrantedBy {
		t.Fatal("the record does not show the actor granting themselves")
	}
	if !strings.Contains(rec.String(), "by actor 7") {
		t.Errorf("the rendering hides who did it: %s", rec)
	}
}

// CheckCapability is advisory, and the engine ignores it. Both halves matter.
func TestRBAC_CheckIsAdvisoryAndTheEngineIgnoresIt(t *testing.T) {
	_, s, _ := roleStore(t)

	if err := s.CheckCapability(7, CapWrite); !errors.Is(err, ErrNotPermitted) {
		t.Fatalf("an ungranted capability passed the check (err=%v)", err)
	}

	// **The engine does not consult it.** A caller with no capabilities can still
	// write, which is the honest state of affairs and the thing rbac.go says
	// plainly rather than obscuring.
	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatalf("the engine refused a write on the basis of capabilities: %v", err)
	}

	if _, err := s.GrantRole(7, 3, CapWrite, GrantRequest{GrantedBy: 1, Reason: "now allowed"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckCapability(7, CapWrite); err != nil {
		t.Fatalf("a granted capability failed the check: %v", err)
	}
	// Asking for more than was granted still fails.
	if err := s.CheckCapability(7, CapWrite|CapRedact); !errors.Is(err, ErrNotPermitted) {
		t.Fatalf("a superset passed the check (err=%v)", err)
	}
}

// A store without Options.Roles refuses rather than silently recording nothing.
func TestRBAC_RefusesWhenTheLedgerIsOff(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	if _, err := s.GrantRole(7, 3, CapWrite, GrantRequest{
		GrantedBy: 1, Reason: "no ledger",
	}); !errors.Is(err, ErrNoGrantLedger) {
		t.Fatalf("a store with no grant ledger recorded a grant (err=%v)", err)
	}
}

// An edited or excised record breaks the chain, and the two are distinguished.
func TestRBAC_ChainCatchesEditAndExcision(t *testing.T) {
	_, s, ring := roleStore(t)
	for i := 0; i < 3; i++ {
		if _, err := s.GrantRole(uint64(i+1), 1, CapRead, GrantRequest{
			GrantedBy: 1, Reason: "batch",
		}); err != nil {
			t.Fatal(err)
		}
	}

	records, err := s.Grants()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyGrantChain(records, ring); err != nil {
		t.Fatalf("an untouched ledger did not verify: %v", err)
	}

	edited := append([]RoleGrant(nil), records...)
	edited[1].Capabilities |= CapGrant // the escalation an attacker would want
	if err := VerifyGrantChain(edited, nil); err == nil {
		t.Fatal("an edited grant verified")
	} else if !strings.Contains(err.Error(), "edited") {
		t.Errorf("an edit was not named as one: %v", err)
	}

	excised := []RoleGrant{records[0], records[2]}
	if err := VerifyGrantChain(excised, nil); err == nil {
		t.Fatal("a ledger with a record removed verified")
	}
}

// **Deleting the ledger must be externally detectable**, which is why its head is
// bound into the checkpoint.
func TestRBAC_LedgerHeadIsBoundIntoTheCheckpoint(t *testing.T) {
	_, s, _ := roleStore(t)

	before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GrantRole(7, 3, CapWrite, GrantRequest{
		GrantedBy: 1, Reason: "moves the head",
	}); err != nil {
		t.Fatal(err)
	}
	after, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	if after.GrantHead == before.GrantHead {
		t.Fatal("a grant did not move the checkpoint's grant head")
	}
	if after.GrantCount != before.GrantCount+1 {
		t.Fatalf("the checkpoint counts %d grants, want %d", after.GrantCount, before.GrantCount+1)
	}
	if after.Digest == before.Digest {
		t.Fatal("the checkpoint digest ignored the grant ledger, so deleting it " +
			"would be externally invisible")
	}
}

// The audit log corroborates the grant ledger.
func TestRBAC_IsAudited(t *testing.T) {
	dir, s, _ := roleStore(t)

	if _, err := s.GrantRole(7, 3, CapRedact, GrantRequest{
		GrantedBy: 1, Reason: "compliance sign-off",
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := ReadAuditLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	last := entries[len(entries)-1]
	if last.Kind != AuditRoleGrant {
		t.Fatalf("the last audit entry is a %s, not a role grant", last.Kind)
	}
	if !strings.Contains(last.Detail, "compliance sign-off") {
		t.Errorf("the audit entry does not carry the reason: %q", last.Detail)
	}
}

// A torn tail leaves everything before it readable.
func TestRBAC_TornTailKeepsWhatCameBefore(t *testing.T) {
	dir, s, _ := roleStore(t)
	for i := 0; i < 2; i++ {
		if _, err := s.GrantRole(uint64(i+1), 1, CapRead, GrantRequest{
			GrantedBy: 1, Reason: "torn tail",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, grantFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-9], 0600); err != nil {
		t.Fatal(err)
	}

	records, err := ReadGrants(dir)
	if err != nil {
		t.Fatalf("a torn tail made the whole ledger unreadable: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected the one whole record, got %d", len(records))
	}
	if err := VerifyGrantChain(records, nil); err != nil {
		t.Errorf("the surviving prefix did not verify: %v", err)
	}
}

// Capability rendering names what is held, and flags bits it does not know.
func TestRBAC_CapabilityRendering(t *testing.T) {
	if got := Capability(0).String(); got != "none" {
		t.Errorf("zero renders as %q", got)
	}
	if got := (CapRead | CapWrite).String(); got != "read|write" {
		t.Errorf("read|write renders as %q", got)
	}
	// An unknown bit must be visible rather than silently dropped: a reader
	// comparing two stores needs to see that one holds something this build
	// cannot name.
	if got := (CapRead | Capability(1<<20)).String(); !strings.Contains(got, "unknown") {
		t.Errorf("an unknown capability bit was hidden: %q", got)
	}
}

// Custody walks the grant ledger too: an edited grant chain is the single most
// valuable thing for an adversary to change, because it is what says whether
// their actions were ever authorised.
func TestRBAC_BrokenGrantChainIsFatalToCustody(t *testing.T) {
	dir, s, ring, ids := fullyProvisionedStore(t, 2)

	// The fixture records a grant; corrupt it.
	data, err := os.ReadFile(filepath.Join(dir, grantFileName))
	if err != nil {
		t.Fatal(err)
	}
	data[12] ^= 0x01 // inside the body, past the length prefix
	if err := os.WriteFile(filepath.Join(dir, grantFileName), data, 0600); err != nil {
		t.Fatal(err)
	}

	r, err := s.CustodyFor(ids[0], ring)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Broken() {
		t.Fatalf("a tampered grant ledger did not break custody: %v", r.Gaps)
	}
	found := false
	for _, g := range r.Gaps {
		if g.Layer == LayerRoles && g.Fatal {
			found = true
		}
	}
	if !found {
		t.Fatalf("the break was not attributed to the roles layer: %v", r.Gaps)
	}
}

// A store that never recorded a grant reports the layer as unestablished, not
// broken — nothing failed, it was simply never set up.
func TestRBAC_NoGrantsIsIncompleteNotBroken(t *testing.T) {
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
	found := false
	for _, g := range r.Gaps {
		if g.Layer == LayerRoles && !g.Fatal {
			found = true
		}
	}
	if !found {
		t.Fatalf("no roles gap reported for a store with no grant ledger: %v", r.Gaps)
	}
}
