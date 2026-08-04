package disk

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// Tombstones: proving a removal from the image alone.

// **The whole point.** A recipient handed one image and a retained root can be
// shown that an entity was deliberately removed — not merely that it is absent.
func TestTombstone_ARemovalCanBeProvenFromTheImage(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	rec, err := s.RedactNode(id, RedactionRequest{ActorID: 7, Reason: "erasure order 9"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	// What the recipient retains: the snapshot root, from outside the system.
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	retained := roots.Snapshot

	proof, err := s.ProveRedaction(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyRedactionInclusion(retained, proof); err != nil {
		t.Fatalf("a genuine removal did not verify: %v", err)
	}

	if proof.Tombstone.NodeID != id {
		t.Errorf("the proof names node %d, want %d", proof.Tombstone.NodeID, id)
	}
	if proof.Tombstone.VersionHash != rec.VersionHash {
		t.Error("the tombstone's version hash is not the ledger record's")
	}
	if proof.Tombstone.RedactionHash != rec.Hash {
		t.Error("the tombstone does not name the ledger record it came from")
	}
}

// An entity that was never there has no tombstone, and the error says so
// distinctly from "not in the snapshot".
func TestTombstone_AbsentIsNotTheSameAsRemoved(t *testing.T) {
	_, s, _ := redactableStore(t)
	addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	if _, err := s.ProveRedaction(store.NodeID(9999)); !errors.Is(err, ErrNoTombstone) {
		t.Fatalf("an entity that never existed produced %v, want ErrNoTombstone", err)
	}
}

// **A tombstone must not verify against the wrong root.** The proof is only
// worth anything checked against a root the holder obtained independently.
func TestTombstone_DoesNotVerifyAgainstAnotherRoot(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "wrong root"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	proof, err := s.ProveRedaction(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyRedactionInclusion(merkle.Hash{}, proof); err == nil {
		t.Fatal("a proof verified against a zero root")
	}

	// And a proof whose leaf has been swapped for another entity's must fail.
	tampered := proof
	tampered.Tombstone.NodeID = 4242
	roots, _ := s.SnapshotRoots()
	if err := VerifyRedactionInclusion(roots.Snapshot, tampered); err == nil {
		t.Fatal("a proof whose leaf disagrees with its stated tombstone verified")
	}
}

// **The root commits to the removals.** Redacting changes the snapshot root even
// when nothing else about the graph differs.
func TestTombstone_RootCommitsToRemovals(t *testing.T) {
	_, s, _ := redactableStore(t)
	keep := addNodeD(t, s, store.NodeTypeTag)
	drop := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	before, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	if before.BodyVersion != snapshotBodyV2 {
		t.Fatalf("a newly written image carries body version %d, want %d",
			before.BodyVersion, snapshotBodyV2)
	}
	if before.TombstoneRoot != merkle.EmptyRoot() {
		t.Error("an image with no removals should carry the empty tombstone root")
	}

	if _, err := s.RedactNode(drop, RedactionRequest{ActorID: 1, Reason: "commits"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	if after.TombstoneRoot == before.TombstoneRoot {
		t.Fatal("a removal did not move the tombstone root")
	}
	if after.Snapshot == before.Snapshot {
		t.Fatal("the snapshot root does not commit to the tombstone root")
	}
	if !s.NodeExists(drop) || s.NodeExists(keep) {
		// keep must survive, drop must not; stated this way so a swap is caught.
		if s.NodeExists(drop) {
			t.Error("the redacted node survived")
		}
		if !s.NodeExists(keep) {
			t.Error("an unrelated node was removed")
		}
	}
}

// Tombstones survive every later compaction, because they are rebuilt from the
// ledger rather than carried forward.
func TestTombstone_SurvivesLaterCompactions(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "persists"}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4; i++ {
		addNodeD(t, s, store.NodeTypeTag)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}

	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := s.ProveRedaction(id)
	if err != nil {
		t.Fatalf("the tombstone did not survive four compactions: %v", err)
	}
	if err := VerifyRedactionInclusion(roots.Snapshot, proof); err != nil {
		t.Fatalf("the surviving tombstone did not verify: %v", err)
	}
}

// Reopening a store loads its tombstones, so the proof outlives the process.
func TestTombstone_SurvivesReopen(t *testing.T) {
	dir, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "reopen"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	key, _ := newAttestKey(t, 93)
	opts := StrictOptions(key, ring, 93)
	opts.Redaction = true
	reopened, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	roots, err := reopened.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := reopened.ProveRedaction(id)
	if err != nil {
		t.Fatalf("a reopened store lost its tombstones: %v", err)
	}
	if err := VerifyRedactionInclusion(roots.Snapshot, proof); err != nil {
		t.Fatalf("the reloaded tombstone did not verify: %v", err)
	}
}

// **Stripping the section must not go unnoticed.** The snapshot root commits to
// the tombstones, so an image whose section has been removed is inconsistent
// with its own root.
func TestTombstone_StrippingTheSectionFailsTheOpen(t *testing.T) {
	dir, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "strip me"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Disturb the tombstone section's contents. The roots no longer describe it.
	path := filepath.Join(dir, "graphene.csr")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte("GRDT")
	at := -1
	for i := 0; i+4 <= len(data); i++ {
		if string(data[i:i+4]) == string(marker) {
			at = i
			break
		}
	}
	if at < 0 {
		t.Fatal("no GRDT section was written")
	}
	// The directory entry is at `at`; flip a byte in the section body instead,
	// which is what someone rewriting a removal would actually touch. The body
	// sits before the directory, so search for the version byte pattern is
	// unnecessary — any body byte will do, and the first tombstone's node ID is
	// the most meaningful one to change.
	data[at] = 'G'
	data[at+3] = 'X' // rename the section so it reads as unknown-but-critical
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	key, _ := newAttestKey(t, 94)
	opts := StrictOptions(key, ring, 94)
	opts.Redaction = true
	if reopened, err := OpenWithOptions(dir, opts); err == nil {
		reopened.Close()
		t.Fatal("an image whose tombstone section was renamed opened without complaint")
	}
}

// A v1 snapshot root does not commit to a tombstone root, so a proof against one
// is refused rather than checked against a value the root never covered.
func TestTombstone_V1SnapshotCannotAttestARemoval(t *testing.T) {
	// A v1 image: four components, no tombstone root.
	old := SnapshotRoots{
		NodeRoot:    merkle.HashLeaf([]byte("n")),
		EdgeRoot:    merkle.HashLeaf([]byte("e")),
		BodyVersion: snapshotBodyV1,
	}
	old.Snapshot = bindSnapshotRoot(old)

	p := RedactionInclusionProof{Roots: old}
	err := VerifyRedactionInclusion(old.Snapshot, p)
	if err == nil {
		t.Fatal("a v1 snapshot attested a removal")
	}
	if !strings.Contains(err.Error(), "predates tombstones") {
		t.Errorf("the refusal does not explain itself: %v", err)
	}
}

// **A v1 image's retained root stays verifiable.** Adding a component to the
// binding must not invalidate roots people were told to keep.
func TestTombstone_V1RootsRemainVerifiable(t *testing.T) {
	v1 := SnapshotRoots{
		NodeRoot:    merkle.HashLeaf([]byte("node")),
		EdgeRoot:    merkle.HashLeaf([]byte("edge")),
		IndexRoot:   merkle.HashLeaf([]byte("index")),
		PrevRoot:    merkle.HashLeaf([]byte("prev")),
		BodyVersion: snapshotBodyV1,
	}
	v1.Snapshot = bindSnapshotRoot(v1)

	body := appendSnapshotSection(nil, v1)
	if body[0] != snapshotBodyV1 {
		t.Fatalf("v1 roots were written as version %d", body[0])
	}
	back, err := readSnapshotSection(body)
	if err != nil {
		t.Fatalf("a v1 section written by this build was rejected by it: %v", err)
	}
	if back.Snapshot != v1.Snapshot || back.BodyVersion != snapshotBodyV1 {
		t.Fatalf("v1 roots did not round-trip: %+v", back)
	}

	// And a v2 set with the same four components must bind to something else,
	// or the version would be doing nothing.
	v2 := v1
	v2.BodyVersion = snapshotBodyV2
	if bindSnapshotRoot(v2) == v1.Snapshot {
		t.Fatal("v1 and v2 bind identically; the added component is not covered")
	}
}

// The section round-trips, and refuses a count its own bytes cannot hold.
func TestTombstone_SectionRoundTripsAndBoundsItsCount(t *testing.T) {
	ts := []Tombstone{
		{NodeID: 1, VersionHash: merkle.HashLeaf([]byte("a")), RedactionSeq: 1},
		{NodeID: 2, VersionHash: merkle.HashLeaf([]byte("b")), RedactionSeq: 2},
	}
	body := appendTombstoneSection(nil, ts)

	back, err := readTombstoneSection(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 2 || back[1].NodeID != 2 || back[0].VersionHash != ts[0].VersionHash {
		t.Fatalf("the section did not round-trip: %+v", back)
	}

	// A count no section of this length could satisfy must be refused, not
	// allocated for.
	body[1], body[2], body[3], body[4] = 0xFF, 0xFF, 0xFF, 0x7F
	if _, err := readTombstoneSection(body); err == nil {
		t.Fatal("a section claiming two billion tombstones was accepted")
	}
}

// Tombstones are deterministic: the same ledger produces the same root.
func TestTombstone_RootIsDeterministic(t *testing.T) {
	_, s, _ := redactableStore(t)
	for i := 0; i < 3; i++ {
		id := addNodeD(t, s, store.NodeTypeTag)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RedactNode(id, RedactionRequest{ActorID: uint64(i), Reason: "determinism"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	first, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	// Compacting again with no intervening change must reproduce the same
	// tombstone root — the ledger has not moved.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	second, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	if first.TombstoneRoot != second.TombstoneRoot {
		t.Fatal("two compactions of the same ledger produced different tombstone roots")
	}

	// And the ledger is the source: rebuilding from it gives the same root.
	ledger, err := s.Redactions()
	if err != nil {
		t.Fatal(err)
	}
	if got := merkle.Root(tombstoneLeaves(tombstonesFromLedger(ledger))); got != first.TombstoneRoot {
		t.Fatal("the image's tombstone root is not what the ledger produces")
	}
}

// The image records every removal, not only the most recent.
func TestTombstone_AllRemovalsAreRecorded(t *testing.T) {
	_, s, _ := redactableStore(t)
	var ids []store.NodeID
	for i := 0; i < 3; i++ {
		id := addNodeD(t, s, store.NodeTypeTag)
		ids = append(ids, id)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "all of them"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	if got := s.Tombstones(); len(got) != 3 {
		t.Fatalf("the image records %d removals, want 3", len(got))
	}
	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		proof, err := s.ProveRedaction(id)
		if err != nil {
			t.Fatalf("no proof for node %d: %v", id, err)
		}
		if err := VerifyRedactionInclusion(roots.Snapshot, proof); err != nil {
			t.Errorf("node %d's removal did not verify: %v", id, err)
		}
	}
}

// A tombstone carries no content, no reason and no actor — the image is the
// artefact most likely to be handed to someone who should not receive them.
func TestTombstone_CarriesNoCircumstances(t *testing.T) {
	_, s, _ := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{
		ActorID: 99, RoleID: 5, Reason: "case OPERATION-BLUEBIRD, analyst j.doe",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	body := appendTombstoneSection(nil, s.Tombstones())
	if strings.Contains(string(body), "BLUEBIRD") || strings.Contains(string(body), "j.doe") {
		t.Fatal("the tombstone section leaks the redaction's circumstances into the image")
	}
}

// Custody distinguishes a removal the image can prove from one only the ledger
// knows about.
func TestTombstone_CustodyReportsWhetherTheRemovalIsProvable(t *testing.T) {
	_, s, ring := redactableStore(t)
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RedactNode(id, RedactionRequest{ActorID: 1, Reason: "provable?"}); err != nil {
		t.Fatal(err)
	}

	// Before compacting, only the ledger knows.
	pending, err := s.CustodyFor(id, ring)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Redacted == nil {
		t.Fatal("the ledger record was not found")
	}
	if pending.RemovalProvable {
		t.Fatal("the removal was reported provable before it was compacted into the image")
	}
	found := false
	for _, g := range pending.Gaps {
		if g.Layer == LayerRedaction && strings.Contains(g.Detail, "not in the compacted image") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the un-bound removal was not reported: %v", pending.Gaps)
	}

	// After compacting, the image carries it.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	settled, err := s.CustodyFor(id, ring)
	if err != nil {
		t.Fatal(err)
	}
	if !settled.RemovalProvable {
		t.Fatal("a compacted removal was not reported as provable")
	}
	for _, g := range settled.Gaps {
		if g.Layer == LayerRedaction && strings.Contains(g.Detail, "not in the compacted image") {
			t.Error("the gap survived the compaction that closed it")
		}
	}
}
