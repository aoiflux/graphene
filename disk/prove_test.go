package disk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// Snapshot roots and inclusion proofs.

func provableStore(t *testing.T, n int) (*Store, []store.NodeID) {
	t.Helper()
	s, _ := openFresh(t)
	t.Cleanup(func() { s.Close() })

	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.IndexNodeProperty(id, "sha256", []byte{byte(i), byte(i >> 8)}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < n; i++ {
		addEdgeD(t, s, ids[i-1], ids[i], store.EdgeTypeContains)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	return s, ids
}

// A proof verifies against a root the verifier obtained separately, using
// nothing else from the store. This is the whole point: evidence that travels.
func TestProveNode_VerifiesAgainstAnIndependentRoot(t *testing.T) {
	s, ids := provableStore(t, 64)

	roots, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	// Stand in for a root retained when the evidence was collected.
	published := roots.Snapshot

	for _, id := range []store.NodeID{ids[0], ids[7], ids[len(ids)-1]} {
		p, err := s.ProveNode(id)
		if err != nil {
			t.Fatalf("ProveNode(%d): %v", id, err)
		}
		if err := VerifyNodeInclusion(published, p); err != nil {
			t.Fatalf("node %d: a genuine proof did not verify: %v", id, err)
		}
	}
}

// Two stores built by identical operations have identical roots.
//
// This is the content-addressing property working, not a flaw: it is what lets
// two parties holding the same evidence agree on its identity without
// exchanging files. It is recorded as a test because it is also the thing that
// makes "a root from a different store" the wrong way to construct a negative
// case — different has to mean different *content*.
func TestSnapshotRoots_IdenticalContentGivesIdenticalRoots(t *testing.T) {
	a, _ := provableStore(t, 24)
	b, _ := provableStore(t, 24)

	ra, err := a.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	rb, err := b.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	if ra.Snapshot != rb.Snapshot {
		t.Fatalf("two stores built identically disagree on their snapshot root:\n  %x\n  %x",
			ra.Snapshot, rb.Snapshot)
	}
}

// A proof must not verify against the root of a snapshot with different content.
func TestProveNode_RejectsARootFromDifferentContent(t *testing.T) {
	s, ids := provableStore(t, 32)
	p, err := s.ProveNode(ids[3])
	if err != nil {
		t.Fatal(err)
	}

	// Different size, so genuinely different content rather than a second store
	// that happens to be a byte-for-byte twin.
	other, _ := provableStore(t, 33)
	otherRoots, err := other.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	if otherRoots.Snapshot == p.Roots.Snapshot {
		t.Fatal("fixture error: the two stores have the same content")
	}

	if err := VerifyNodeInclusion(otherRoots.Snapshot, p); err == nil {
		t.Fatal("a proof verified against the root of a different snapshot")
	}
}

// Tampering with the claimed entity must break the proof. Otherwise a proof
// would attest to whatever record it was presented alongside.
func TestProveNode_RejectsAlteredLeafData(t *testing.T) {
	s, ids := provableStore(t, 32)
	roots, _ := s.SnapshotRoots()

	p, err := s.ProveNode(ids[5])
	if err != nil {
		t.Fatal(err)
	}
	for i := range p.LeafData {
		altered := p
		altered.LeafData = append([]byte(nil), p.LeafData...)
		altered.LeafData[i] ^= 0x01
		if err := VerifyNodeInclusion(roots.Snapshot, altered); err == nil {
			t.Fatalf("a proof verified after byte %d of the entity record was changed", i)
		}
	}
}

// The component roots must produce the snapshot root being checked against.
// Without that check, a valid proof into an unrelated tree would pass.
func TestVerifyNodeInclusion_RejectsMismatchedComponentRoots(t *testing.T) {
	s, ids := provableStore(t, 16)
	roots, _ := s.SnapshotRoots()

	p, err := s.ProveNode(ids[2])
	if err != nil {
		t.Fatal(err)
	}

	tampered := p
	tampered.Roots.EdgeRoot[0] ^= 0x01
	if err := VerifyNodeInclusion(roots.Snapshot, tampered); err == nil {
		t.Fatal("verification passed with a component root that does not produce the snapshot")
	}

	swapped := p
	swapped.NodeRoot[0] ^= 0x01
	if err := VerifyNodeInclusion(roots.Snapshot, swapped); err == nil {
		t.Fatal("verification passed with a node root disagreeing with the snapshot")
	}
}

// An entity written since the last compaction is live but not in the snapshot,
// and that is reported as such rather than as absent or as a silent success.
func TestProveNode_UncompactedEntityIsNotInTheSnapshot(t *testing.T) {
	s, _ := provableStore(t, 8)

	fresh := addNodeD(t, s, store.NodeTypeTag)
	if _, err := s.GetNode(fresh); err != nil {
		t.Fatalf("the node should be live: %v", err)
	}

	_, err := s.ProveNode(fresh)
	if !errors.Is(err, ErrNotInSnapshot) {
		t.Fatalf("ProveNode on an uncompacted node gave %v, want ErrNotInSnapshot", err)
	}

	// After compaction it is provable.
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	roots, _ := s.SnapshotRoots()
	p, err := s.ProveNode(fresh)
	if err != nil {
		t.Fatalf("ProveNode after compaction: %v", err)
	}
	if err := VerifyNodeInclusion(roots.Snapshot, p); err != nil {
		t.Fatalf("proof for a newly compacted node did not verify: %v", err)
	}
}

// A store that has never been compacted has no roots, and says so.
func TestSnapshotRoots_AbsentBeforeCompaction(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	addNodeD(t, s, store.NodeTypeMicroArtefact)

	if _, err := s.SnapshotRoots(); !errors.Is(err, ErrNoSnapshotRoots) {
		t.Fatalf("SnapshotRoots on an uncompacted store gave %v, want ErrNoSnapshotRoots", err)
	}
}

// Roots survive a reopen: they are in the file, not in memory.
func TestSnapshotRoots_SurviveReopen(t *testing.T) {
	s, dir := openFresh(t)
	for i := 0; i < 20; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	before, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	after, err := reopened.SnapshotRoots()
	if err != nil {
		t.Fatalf("roots did not survive the reopen: %v", err)
	}
	if after != before {
		t.Fatalf("roots changed across a reopen:\n  %x\n  %x", before.Snapshot, after.Snapshot)
	}
}

// Changing the graph changes the content roots. A root that did not move when
// the data did would be worthless.
func TestSnapshotRoots_ChangeWithTheData(t *testing.T) {
	s, _ := provableStore(t, 16)
	before, _ := s.SnapshotRoots()

	addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	after, _ := s.SnapshotRoots()

	if after.NodeRoot == before.NodeRoot {
		t.Fatal("adding a node left the node root unchanged")
	}
	if err := VerifyChain(before, after); err != nil {
		t.Fatalf("the new snapshot does not chain to the old: %v", err)
	}
}

// A chain must reject a snapshot inserted in the wrong place.
func TestVerifyChain_RejectsASubstitutedSnapshot(t *testing.T) {
	s, _ := provableStore(t, 12)
	first, _ := s.SnapshotRoots()

	addNodeD(t, s, store.NodeTypeTag)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	second, _ := s.SnapshotRoots()

	// A snapshot with different content, internally perfectly consistent. Size
	// differs so it is genuinely a different snapshot rather than a twin of the
	// first — identical content would legitimately produce an identical root.
	other, _ := provableStore(t, 20)
	foreign, _ := other.SnapshotRoots()
	if foreign.Snapshot == first.Snapshot {
		t.Fatal("fixture error: the substitute has the same content as the original")
	}

	if err := VerifyChain(first, second); err != nil {
		t.Fatalf("the genuine chain did not verify: %v", err)
	}
	if err := VerifyChain(foreign, second); err == nil {
		t.Fatal("a snapshot chained to a predecessor it never had")
	}
	if err := VerifyChain(second, first); err == nil {
		t.Fatal("the chain verified in reverse order")
	}
}

// The roots catch an edit that repaired the digest behind it.
//
// This is the case the digest alone cannot cover, and the reason both checks
// exist. Anyone who can rewrite a record can rewrite the digest — it is keyless.
// They would also have to rewrite the roots, and rewriting the roots changes the
// snapshot root, which is the value a verifier retained externally.
func TestVerifyCSRRoots_CatchesAnEditThatRepairedTheDigest(t *testing.T) {
	s, _ := provableStore(t, 24)
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	p := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}

	// Change a record, then repair the digest so it agrees with the new bytes —
	// exactly what an adversary with file access would do.
	data[csrV8HeaderSize+20] ^= 0x01
	repaired := computeCSRDigest(data)
	copy(data[csrDigestOffset:csrDigestOffset+csrDigestSize], repaired[:])
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}

	status, _, err := VerifyCSRDigest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if status != DigestMatch {
		t.Fatalf("the digest should have been repaired successfully, got %v", status)
	}

	if err := VerifyCSRRoots(dir); err == nil {
		t.Fatal("the roots accepted a record the file's own Merkle tree does not describe")
	}
}

// Labels are a set, so their stored order must not change an entity's hash.
func TestSnapshotRoots_LabelOrderDoesNotAffectTheHash(t *testing.T) {
	a := nodeRecord{ID: 1, Labels: []store.NodeType{store.NodeTypeTag, store.NodeTypeMicroArtefact}}
	b := nodeRecord{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact, store.NodeTypeTag}}

	if merkle.HashLeaf(nodeLeafData(a)) != merkle.HashLeaf(nodeLeafData(b)) {
		t.Fatal("two records differing only in label order hashed differently; " +
			"labels are a set and must be canonicalised into the hash")
	}
}

// A snapshot section whose bound root does not follow from its components is
// refused: publishing such a root would prove nothing about the file's contents.
func TestSnapshotSection_RejectsAnInconsistentBoundRoot(t *testing.T) {
	r := SnapshotRoots{NodeRoot: merkle.HashLeaf([]byte("n"))}
	r.Snapshot = bindSnapshotRoot(r)

	body := appendSnapshotSection(nil, r)
	if _, err := readSnapshotSection(body); err != nil {
		t.Fatalf("a consistent section was rejected: %v", err)
	}

	body[1] ^= 0x01 // disturb the node root
	if _, err := readSnapshotSection(body); err == nil {
		t.Fatal("a section whose bound root does not follow from its components was accepted")
	}
}
