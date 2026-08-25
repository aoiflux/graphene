package graphene_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// SECURITY.md §7, executed.
//
// §7's central claim is a *negative* one — that the role model enforces nothing —
// and negative claims are the ones a reader is most likely to disbelieve and most
// harmed by if they are wrong in either direction. Both halves are pinned here:
// that the check refuses correctly, and that the engine ignores it anyway.

func rolesDocStore(t *testing.T) *disk.Store {
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
	opts.Roles = true

	s, err := disk.OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// SECURITY.md §7 "What it does not do": the engine ignores capabilities.
func TestSecurityDoc_RolesEnforceNothing(t *testing.T) {
	s := rolesDocStore(t)
	const analyst = 7

	// The documented advisory check refuses.
	if err := s.CheckCapability(analyst, disk.CapWrite); !errors.Is(err, disk.ErrNotPermitted) {
		t.Fatalf("§7 says CheckCapability refuses an ungranted capability; got %v", err)
	}

	// **And the engine does it anyway.** §7 says so in as many words, and a
	// reader who assumed otherwise would be building on a boundary that is not
	// there.
	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatalf("§7 says the engine never consults capabilities; a write was refused: %v", err)
	}
}

// SECURITY.md §7 "What it does do" — INV-3, by construction.
func TestSecurityDoc_CapabilitiesComeOnlyFromTheLedger(t *testing.T) {
	s := rolesDocStore(t)
	const analyst = 7

	// The documented calls.
	if _, err := s.GrantRole(analyst, 3, disk.CapRead|disk.CapWrite, disk.GrantRequest{
		GrantedBy: 1, Reason: "analyst onboarding",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RevokeRole(analyst, 3, disk.CapWrite, disk.GrantRequest{
		GrantedBy: 1, Reason: "write moved to the ingest role",
	}); err != nil {
		t.Fatal(err)
	}

	caps, err := s.Capabilities(analyst)
	if err != nil {
		t.Fatal(err)
	}
	if !caps.Has(disk.CapRead) || caps.Has(disk.CapWrite) {
		t.Fatalf("the ledger replay gives %s; expected read without write", caps)
	}

	// "CapabilitiesFrom is package-level and takes a slice, so a third party
	// holding an exported ledger computes the same answer the store would."
	records, err := s.Grants()
	if err != nil {
		t.Fatal(err)
	}
	if got := disk.CapabilitiesFrom(records)[analyst]; got != caps {
		t.Fatalf("§7 promises a third party gets the same answer: store %s, ledger %s", caps, got)
	}
	if err := disk.VerifyGrantChain(records, nil); err != nil {
		t.Fatalf("the documented ledger did not verify: %v", err)
	}
}

// SECURITY.md §7: "reason required", and the chain properties it shares with the
// other ledgers.
func TestSecurityDoc_GrantsAreExplainedAndChained(t *testing.T) {
	s := rolesDocStore(t)

	if _, err := s.GrantRole(7, 3, disk.CapRead, disk.GrantRequest{GrantedBy: 1}); !errors.Is(err, disk.ErrGrantUnexplained) {
		t.Fatalf("§7 says a reason is required; got %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := s.GrantRole(uint64(i+1), 1, disk.CapRead, disk.GrantRequest{
			GrantedBy: 1, Reason: "batch",
		}); err != nil {
			t.Fatal(err)
		}
	}
	records, err := s.Grants()
	if err != nil {
		t.Fatal(err)
	}

	// "removing a record from the middle breaks every link after"
	excised := []disk.RoleGrant{records[0], records[2]}
	if err := disk.VerifyGrantChain(excised, nil); err == nil {
		t.Fatal("§7 says an excised record breaks the chain; it verified")
	}
}

// SECURITY.md §7 "Self-granting is legal, and recorded."
func TestSecurityDoc_SelfGrantIsRecorded(t *testing.T) {
	s := rolesDocStore(t)

	rec, err := s.GrantRole(7, 3, disk.CapGrant, disk.GrantRequest{
		GrantedBy: 7, Reason: "granting myself",
	})
	if err != nil {
		t.Fatalf("§7 says a self-grant is not refused: %v", err)
	}
	if rec.Subject != rec.GrantedBy {
		t.Fatal("§7 says the record shows their own ID in both fields")
	}
	if !strings.Contains(rec.String(), "by actor 7") {
		t.Errorf("the rendering hides who did it: %s", rec)
	}
}

// SECURITY.md §7: "bound into the checkpoint (§5), so deleting it wholesale is
// externally detectable rather than silent."
func TestSecurityDoc_GrantLedgerIsAnchored(t *testing.T) {
	s := rolesDocStore(t)

	before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.GrantRole(7, 3, disk.CapRead, disk.GrantRequest{
		GrantedBy: 1, Reason: "moves the head",
	}); err != nil {
		t.Fatal(err)
	}
	after, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if after.Digest == before.Digest {
		t.Fatal("§7 says the grant ledger is bound into the checkpoint; the digest did not move")
	}
}
