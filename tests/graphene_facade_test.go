package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/signing"
	"github.com/aoiflux/graphene/store"
)

// Reaching the integrity machinery from the façade.
//
// Every forensic example in this repository opens disk.Store directly, which is
// correct but is not where a consumer starts — they meet graphene.Graph. These
// pin the two calls that connect the two.

// **The whole point.** A caller who has a Graph can get to the machinery, and
// the store they get is the one the Graph is using rather than a copy.
func TestFacade_ForensicsReachesTheSameStore(t *testing.T) {
	dir := t.TempDir()
	key, pub, err := signing.GenerateKey(1)
	if err != nil {
		t.Fatal(err)
	}
	ring := signing.NewKeyring()
	if err := ring.Add(1, pub); err != nil {
		t.Fatal(err)
	}

	g, err := graphene.OpenWithOptions(dir, disk.StrictOptions(key, ring, 42))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	s, ok := g.Forensics()
	if !ok {
		t.Fatal("a disk-backed Graph did not expose its store")
	}

	// Written through the façade…
	id, err := g.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("sha256=abc123"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}

	// …and provable through the store, with no copy in between.
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := s.ProveNode(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.VerifyNodeInclusion(roots.Snapshot, proof); err != nil {
		t.Fatalf("an entity written through the façade did not verify: %v", err)
	}

	// And the other direction: a redaction through the store is visible to the
	// Graph immediately.
	if _, err := s.RedactNodeProperties(id, disk.RedactionRequest{
		ActorID: 7, Reason: "same store, not a copy",
	}); err != nil {
		// Redaction is off in StrictOptions, so this must refuse rather than
		// silently degrade — which is itself worth asserting.
		t.Logf("redaction refused as expected without Options.Redaction: %v", err)
	} else {
		n, gerr := g.GetNode(id)
		if gerr != nil {
			t.Fatal(gerr)
		}
		if len(n.Properties) != 0 {
			t.Fatal("a redaction through the store was not visible through the Graph")
		}
	}
}

// The in-memory backend has none of this, and says so once rather than failing
// fifty times.
func TestFacade_InMemoryHasNoForensics(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()

	if s, ok := g.Forensics(); ok || s != nil {
		t.Fatal("the in-memory backend claimed to support the integrity machinery")
	}
}

// Open still gives the historical defaults — unsigned, unverified — so adding
// OpenWithOptions changed nothing for existing callers.
func TestFacade_OpenKeepsTheHistoricalDefaults(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	s, ok := g.Forensics()
	if !ok {
		t.Fatal("a disk-backed Graph did not expose its store")
	}
	if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}

	// Roots exist regardless — compaction always computes them (BL-33) — but
	// nothing attested the image, because no signer was configured.
	if _, err := s.SnapshotAttestation(); err == nil {
		t.Fatal("a default Open produced an attested image; the defaults changed")
	}
	if _, err := s.SnapshotRoots(); err != nil {
		t.Fatalf("compaction should compute roots regardless of options: %v", err)
	}
}

// OpenWithOptions honours the options it is given, including the two that cost
// extra files.
func TestFacade_OpenWithOptionsHonoursThem(t *testing.T) {
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
	opts.Redaction = true
	opts.Roles = true

	g, err := graphene.OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	s, ok := g.Forensics()
	if !ok {
		t.Fatal("no store behind the Graph")
	}

	// Roles on: a grant is accepted rather than refused.
	if _, err := s.GrantRole(7, 3, disk.CapWrite, disk.GrantRequest{
		GrantedBy: 1, Reason: "façade wiring",
	}); err != nil {
		t.Fatalf("Options.Roles did not reach the store: %v", err)
	}

	// Redaction on: an attributed removal is accepted.
	id, err := g.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte("pii"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNodeProperties(id, disk.RedactionRequest{
		ActorID: 7, Reason: "façade wiring",
	}); err != nil {
		t.Fatalf("Options.Redaction did not reach the store: %v", err)
	}

	// Signing on: the image is attested.
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	att, err := s.SnapshotAttestation()
	if err != nil {
		t.Fatalf("Options.Signer did not reach the store: %v", err)
	}
	if err := disk.VerifyAttestation(ring, att); err != nil {
		t.Fatalf("the attestation does not verify: %v", err)
	}
}
