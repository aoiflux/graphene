package disk

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// WAL segmentation and retention.

// segmentedStore compacts n times under the given policy, so n segments are
// retired.
func segmentedStore(t *testing.T, policy RetentionPolicy, compactions int) (dir string, s *Store) {
	t.Helper()
	dir = t.TempDir()

	s, err := OpenWithOptions(dir, Options{Retention: policy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	for i := 0; i < compactions; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	return dir, s
}

// The active log is never mistaken for a retired segment.
//
// "graphene.wal" carries the segment prefix and suffix with nothing between
// them. Parsing it without a length guard panics; treating it as a segment
// would be worse, because retention would delete the live log.
func TestSegment_ActiveLogIsNotASegment(t *testing.T) {
	if _, ok := parseSegmentSeq(walFileName); ok {
		t.Fatalf("%q was parsed as a retired segment; retention would delete the live log", walFileName)
	}
	for _, name := range []string{"graphene.wal", "graphene..wal", "graphene.csr", "other.000001.wal", ""} {
		if _, ok := parseSegmentSeq(name); ok && name != "" {
			t.Errorf("%q was parsed as a segment", name)
		}
	}
	if seq, ok := parseSegmentSeq("graphene.000007.wal"); !ok || seq != 7 {
		t.Fatalf("a real segment name parsed as (%d, %v)", seq, ok)
	}
}

// With retention configured, compaction retires the log instead of discarding
// it — and the commit history it holds survives.
func TestSegment_CompactionRetiresTheLog(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 3)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 3 {
		t.Fatalf("kept %d segments after 3 compactions, want 3", len(segs))
	}
	for i, s := range segs {
		if s.Sequence != uint64(i) {
			t.Errorf("segment %d has sequence %d", i, s.Sequence)
		}
		if s.Bytes == 0 {
			t.Errorf("segment %d is empty", i)
		}
	}
}

// Without retention, compaction discards the log exactly as before.
func TestSegment_NoRetentionKeepsNothing(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{}, 3)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 0 {
		t.Fatalf("the zero policy kept %d segments; it should keep none", len(segs))
	}
}

// **The chain.** Each segment names its predecessor's digest, so a removed one
// is provably missing rather than invisibly absent.
func TestSegment_ChainLinksEachToItsPredecessor(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 4)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySegmentChain(segs); err != nil {
		t.Fatalf("a genuine chain did not verify: %v", err)
	}

	// Remove one from the middle: the successor still names it, so the gap shows.
	if err := os.Remove(segs[1].Path); err != nil {
		t.Fatal(err)
	}
	after, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySegmentChain(after); err == nil {
		t.Fatal("removing a segment from the middle left the chain verifying")
	}
}

// Replacing a segment's contents breaks the link in its successor.
func TestSegment_ChainDetectsAReplacedSegment(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 3)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(segs[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0x01
	if err := os.WriteFile(segs[0].Path, data, 0600); err != nil {
		t.Fatal(err)
	}

	after, _ := ListSegments(dir)
	if err := VerifySegmentChain(after); err == nil {
		t.Fatal("altering a retired segment left the chain verifying")
	}
}

// Retired segments are not replayed: their contents are already in the CSR, so
// keeping history must not make opening slower or double-apply anything.
func TestSegment_RetiredSegmentsAreNotReplayed(t *testing.T) {
	dir, s := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 3)

	before, _ := s.NodeCount()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithOptions(dir, Options{Retention: RetentionPolicy{MaxSegments: 10}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	after, _ := reopened.NodeCount()
	if after != before {
		t.Fatalf("node count changed across a reopen with retired segments: %d -> %d", before, after)
	}
}

// Sequence numbers continue past what already exists, so a reopened store never
// overwrites retired history.
func TestSegment_SequenceResumesAfterReopen(t *testing.T) {
	dir, s := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithOptions(dir, Options{Retention: RetentionPolicy{MaxSegments: 10}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	addNodeD(t, reopened, store.NodeTypeTag)
	if err := reopened.Compact(); err != nil {
		t.Fatal(err)
	}

	segs, _ := ListSegments(dir)
	if len(segs) != 3 {
		t.Fatalf("after reopening and compacting there are %d segments, want 3", len(segs))
	}
	seen := map[uint64]bool{}
	for _, sg := range segs {
		if seen[sg.Sequence] {
			t.Fatalf("sequence %d was reused; retired history would be overwritten", sg.Sequence)
		}
		seen[sg.Sequence] = true
	}
}

// MaxSegments keeps the newest N.
func TestSegment_RetentionByCount(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 2}, 5)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 2 {
		t.Fatalf("MaxSegments=2 kept %d segments", len(segs))
	}
	// And the ones kept are the newest.
	if segs[0].Sequence != 3 || segs[1].Sequence != 4 {
		t.Fatalf("kept sequences %d and %d, want 3 and 4", segs[0].Sequence, segs[1].Sequence)
	}
	// What survives must still chain.
	if err := VerifySegmentChain(segs); err != nil {
		t.Fatalf("retention left an unverifiable chain: %v", err)
	}
}

// MaxAge keeps recent segments and drops old ones.
func TestSegment_RetentionByAge(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 3)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Backdate the oldest well past any plausible cutoff.
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(segs[0].Path, old, old); err != nil {
		t.Fatal(err)
	}

	removed, err := applyRetention(dir, RetentionPolicy{MaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].Sequence != segs[0].Sequence {
		t.Fatalf("age retention removed %d segments, want just the backdated one", len(removed))
	}

	left, _ := ListSegments(dir)
	if len(left) != 2 {
		t.Fatalf("%d segments left, want 2", len(left))
	}
}

// Rules combine: a segment survives only if it satisfies all of them.
func TestSegment_RulesCombineTightly(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 5)

	// MaxSegments would keep 4; MaxBytes is set so tightly that only one fits.
	segs, _ := ListSegments(dir)
	oneSegment := segs[len(segs)-1].Bytes

	removed, err := applyRetention(dir, RetentionPolicy{MaxSegments: 4, MaxBytes: oneSegment})
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) < 4 {
		t.Fatalf("the tighter of the two rules should win; only %d removed", len(removed))
	}
}

// Retention only ever removes from the oldest end. Deleting from the middle
// would leave a kept segment naming a predecessor that no longer exists, which
// a verifier could not distinguish from tampering.
func TestSegment_RetentionRemovesOnlyFromTheOldestEnd(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 10}, 5)

	if _, err := applyRetention(dir, RetentionPolicy{MaxSegments: 3}); err != nil {
		t.Fatal(err)
	}
	left, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 3 {
		t.Fatalf("%d segments left, want 3", len(left))
	}
	// Contiguous, newest-first, and still a chain.
	for i := 1; i < len(left); i++ {
		if left[i].Sequence != left[i-1].Sequence+1 {
			t.Fatalf("retention left a hole: %d then %d", left[i-1].Sequence, left[i].Sequence)
		}
	}
	if err := VerifySegmentChain(left); err != nil {
		t.Fatalf("what survived does not chain: %v", err)
	}
}

// A retired segment still holds the commit provenance the active log had, which
// is the whole reason to keep it.
func TestSegment_RetiredSegmentHoldsCommitHistory(t *testing.T) {
	dir := t.TempDir()

	s, err := OpenWithOptions(dir, Options{Retention: RetentionPolicy{MaxSegments: 5}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.ApplyTransactionAs(
		[]store.TxOp{{Kind: store.TxOpAddNode, Node: &store.Node{
			ID: s.ReserveNodeID(), Labels: []store.NodeType{store.NodeTypeMicroArtefact}}}},
		store.TxContext{ActorID: 4242},
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("expected one retired segment, got %d", len(segs))
	}

	// The segment is a WAL file: inspection reads it like any other.
	info, err := InspectWAL(segs[0].Path)
	if err != nil {
		t.Fatalf("inspecting a retired segment: %v", err)
	}
	found := false
	for _, c := range info.Commits {
		if c.HasDetail && c.ActorID == 4242 {
			found = true
		}
	}
	if !found {
		t.Fatal("the retired segment does not carry the commit's actor; " +
			"keeping it would serve no purpose")
	}
}

// The active log's header names the segment it follows, so the chain's head is
// linked to the retired history rather than floating free.
func TestSegment_ActiveLogNamesTheLastSegment(t *testing.T) {
	dir, _ := segmentedStore(t, RetentionPolicy{MaxSegments: 5}, 2)

	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	var prev [walPrevDigestSize]byte
	copy(prev[:], data[14:46])

	if prev != segs[len(segs)-1].Digest {
		t.Fatalf("the active log names predecessor %x, but the newest segment hashes to %x",
			prev[:8], segs[len(segs)-1].Digest[:8])
	}
}
