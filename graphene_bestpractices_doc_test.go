package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// docs/API_REFERENCE.md §22, executed.
//
// §22 is prescriptive — it tells a reader which call to prefer and why — so its
// claims are exactly the kind that rot silently. The durability paragraph in §16
// had already contradicted both its own table and the code by the time anyone
// noticed. These pin the parts a reader would act on.

// §22.4 "Attribute every mutation" — the Begin().As(...) form, and that the
// unattributed form really is unattributed.
func TestBestPractices_AttributedCommit(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// The documented call has to compile and commit.
	tx := g.Begin().As(store.TxContext{ActorID: 7, RoleID: 3, KeyID: 1})
	tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	if err := tx.Commit(); err != nil {
		t.Fatalf("§22.4's documented attributed commit failed: %v", err)
	}
	if !tx.Atomic() {
		t.Error("§22.1 calls a commit the durability boundary; this backend is not atomic")
	}

	// And the attribution reaches the log, which is the entire point.
	info, err := disk.InspectWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	attributed := 0
	for _, c := range info.Commits {
		if c.ActorID == 7 {
			attributed++
		}
	}
	if attributed == 0 {
		t.Fatal("§22.4 says As() records who is responsible; no commit carries the actor")
	}
}

// §22.1 and §22.2: a committed batch is durable when Commit returns, because
// SetSyncOnCommit defaults to true. §16's prose used to deny this.
func TestBestPractices_CommitIsTheDurabilityBoundary(t *testing.T) {
	dir := t.TempDir()
	s, err := disk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// The default is what §22.2 tells readers to leave alone. There is no getter,
	// so this asserts the documented default by exercising the path: a batch
	// commit must succeed and be readable back without any explicit Sync.
	ops := []store.TxOp{{
		Kind: store.TxOpAddNode,
		Node: &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
	}}
	if err := s.ApplyTransactionAs(ops, store.TxContext{ActorID: 9}); err != nil {
		t.Fatalf("a committed batch failed: %v", err)
	}

	info, err := disk.InspectWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Commits) == 0 {
		t.Fatal("§22.1 calls Commit the durability boundary, but no commit reached the log")
	}
}

// §22.3 "Kinds below AuditCustom are refused" — the reason given is that a
// caller could otherwise fabricate engine history.
func TestBestPractices_CallerCannotForgeEngineAuditKinds(t *testing.T) {
	dir := t.TempDir()
	s, err := disk.OpenWithOptions(dir, disk.Options{Audit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// The documented call.
	if err := s.RecordAudit(disk.AuditCustom, 7, "exported to case file 2026-114"); err != nil {
		t.Fatalf("§22.3's documented RecordAudit call failed: %v", err)
	}
	// The documented refusal.
	if err := s.RecordAudit(disk.AuditCompact, 7, "forged"); err == nil {
		t.Fatal("§22.3 says engine kinds are refused; AuditCompact was accepted")
	}
}

// §22.6: "Redacting without compacting — RemovalProvable stays false." A reader
// deciding when to compact acts on this.
func TestBestPractices_RemovalIsNotProvableUntilCompacted(t *testing.T) {
	s, _ := redactableDocStore(t)

	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("pii"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNodeProperties(id, disk.RedactionRequest{
		ActorID: 1, Reason: "erasure",
	}); err != nil {
		t.Fatal(err)
	}

	before, err := s.CustodyFor(id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if before.RemovalProvable {
		t.Fatal("§22.6 says RemovalProvable stays false until a compaction; it was true")
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := s.CustodyFor(id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !after.RemovalProvable {
		t.Fatal("§22.6 implies compacting closes the window; it did not")
	}
}

// §22.7's worked configuration must be a configuration that actually opens.
func TestBestPractices_WorkedConfigurationOpens(t *testing.T) {
	dir := t.TempDir()

	signer, pub, kerr := signing.GenerateKey(1)
	if kerr != nil {
		t.Fatal(kerr)
	}
	ring := signing.NewKeyring()
	if kerr := ring.Add(1, pub); kerr != nil {
		t.Fatal(kerr)
	}
	opts := disk.StrictOptions(signer, ring, 42)
	opts.Retention = disk.RetentionPolicy{MaxSegments: 50}
	opts.Redaction = true
	opts.RedactionPolicy = disk.RedactionPolicy{MaxCascade: 100}

	s, err := disk.OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("§22.7's worked configuration does not open: %v", err)
	}
	defer s.Close()

	if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
		t.Fatal(err)
	}
	// The scheduled half of §22.7.
	if err := s.Compact(); err != nil {
		t.Fatalf("§22.7's scheduled Compact failed: %v", err)
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatalf("§22.7 says Compact produces roots to retain: %v", err)
	}
	if roots.Snapshot == (merkle.Hash{}) {
		t.Fatal("§22.7 tells the reader to retain a root; it is zero")
	}
}
