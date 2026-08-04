package disk

import (
	"testing"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// Cascaded edges: an edge removed as collateral gets the same standing as one
// removed deliberately.

// **An edge removed as collateral is as absent as one removed deliberately.**
// Without its own tombstone nobody auditing "why is there no edge between A and
// B" can be shown an answer.
func TestCascade_EdgesGetTheirOwnTombstones(t *testing.T) {
	_, s, _ := redactableStore(t)
	hub := hubWithSpokes(t, s, 3)

	impact, err := s.RedactionImpactFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	if len(impact.CascadedHashes) != len(impact.CascadedEdges) {
		t.Fatalf("the impact report identifies %d of %d cascaded edges",
			len(impact.CascadedHashes), len(impact.CascadedEdges))
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	rec, err := s.RedactNode(hub, RedactionRequest{ActorID: 1, Reason: "collateral"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	// One tombstone for the node, one for each edge it took with it.
	if got := len(s.Tombstones()); got != 1+len(rec.CascadedEdges) {
		t.Fatalf("the image records %d tombstones, want %d", got, 1+len(rec.CascadedEdges))
	}

	for i, eid := range rec.CascadedEdges {
		proof, perr := s.ProveEdgeRedaction(eid)
		if perr != nil {
			t.Fatalf("no tombstone for cascaded edge %d: %v", eid, perr)
		}
		if err := VerifyRedactionInclusion(roots.Snapshot, proof); err != nil {
			t.Errorf("cascaded edge %d's removal did not verify: %v", eid, err)
		}
		if proof.Tombstone.VersionHash != rec.CascadedHashes[i] {
			t.Errorf("edge %d's tombstone does not identify what was removed", eid)
		}
		// It points back at the same ledger record as the node's removal.
		if proof.Tombstone.RedactionSeq != rec.Seq {
			t.Errorf("edge %d's tombstone names redaction %d, not %d",
				eid, proof.Tombstone.RedactionSeq, rec.Seq)
		}
	}
}

// A record from before cascade hashes existed names its edges without
// identifying them, and must produce no per-edge tombstones — an unidentifiable
// marker is worse than none, because it looks like evidence.
func TestCascade_UnidentifiedCascadeProducesNoEdgeTombstones(t *testing.T) {
	old := RedactionRecord{
		Seq: 1, NodeID: 5,
		CascadedEdges: []store.EdgeID{1, 2, 3}, // named, but no hashes
		VersionHash:   merkle.HashLeaf([]byte("v")),
		Reason:        "written before cascade hashes",
	}
	old.Hash = computeRedactionHash(old)

	ts := tombstonesFromLedger([]RedactionRecord{old})
	if len(ts) != 1 {
		t.Fatalf("an unidentified cascade produced %d tombstones, want 1 (the node's)", len(ts))
	}
	if ts[0].Scope != ScopeNode {
		t.Fatalf("the surviving tombstone is a %s", ts[0].Scope)
	}
}

// **Every extension must be covered by the record hash.** A cascade hash the
// hash skipped would be a field an adversary could rewrite freely.
func TestCascade_HashesAreCoveredByTheRecordHash(t *testing.T) {
	base := RedactionRecord{
		Seq: 1, NodeID: 5,
		CascadedEdges:  []store.EdgeID{1, 2},
		CascadedHashes: []merkle.Hash{merkle.HashLeaf([]byte("a")), merkle.HashLeaf([]byte("b"))},
		VersionHash:    merkle.HashLeaf([]byte("v")),
		Reason:         "covered",
	}
	base.Hash = computeRedactionHash(base)

	swapped := base
	swapped.CascadedHashes = []merkle.Hash{merkle.HashLeaf([]byte("b")), merkle.HashLeaf([]byte("a"))}
	if computeRedactionHash(swapped) == base.Hash {
		t.Fatal("reordering the cascade hashes did not change the record hash")
	}

	forged := base
	forged.CascadedHashes = []merkle.Hash{merkle.HashLeaf([]byte("x")), merkle.HashLeaf([]byte("y"))}
	if computeRedactionHash(forged) == base.Hash {
		t.Fatal("replacing the cascade hashes did not change the record hash")
	}

	// And it round-trips.
	back, err := parseRedactionRecord(appendRedactionRecord(nil, base)[4:])
	if err != nil {
		t.Fatal(err)
	}
	if len(back.CascadedHashes) != 2 || back.CascadedHashes[0] != base.CascadedHashes[0] {
		t.Fatalf("cascade hashes did not round-trip: %+v", back.CascadedHashes)
	}
	if computeRedactionHash(back) != base.Hash {
		t.Fatal("a round-tripped record hashes differently")
	}
}

// A record claiming more cascade hashes than its bytes hold is refused, not
// allocated for.
func TestCascade_RefusesAnImpossibleHashCount(t *testing.T) {
	r := RedactionRecord{
		Seq: 1, NodeID: 1, Reason: "x",
		CascadedHashes: []merkle.Hash{merkle.HashLeaf([]byte("a"))},
	}
	body := appendRedactionRecord(nil, r)[4:]

	// The count is the last field before the hashes.
	at := len(body) - merkle.Size - 4
	body[at], body[at+1], body[at+2], body[at+3] = 0xFF, 0xFF, 0xFF, 0x7F
	if _, err := parseRedactionRecord(body); err == nil {
		t.Fatal("a record claiming two billion cascade hashes was accepted")
	}
}

// A pre-cascade-hash record still hashes exactly as it did, which is the point
// of appending rather than inserting.
func TestCascade_PreCascadeRecordsStillVerify(t *testing.T) {
	old := RedactionRecord{
		Seq: 1, UnixNano: 1234567890, ActorID: 3,
		NodeID: 42, CascadedEdges: []store.EdgeID{9},
		VersionHash: merkle.HashLeaf([]byte("v")),
		Reason:      "no cascade hashes",
	}
	old.Hash = computeRedactionHash(old)

	if err := VerifyRedactionChain([]RedactionRecord{old}, nil); err != nil {
		t.Fatalf("a pre-cascade-hash record no longer verifies: %v", err)
	}
	back, err := parseRedactionRecord(appendRedactionRecord(nil, old)[4:])
	if err != nil {
		t.Fatal(err)
	}
	if len(back.CascadedHashes) != 0 || back.Hash != old.Hash {
		t.Fatalf("a pre-cascade-hash record did not round-trip: %+v", back)
	}
}
