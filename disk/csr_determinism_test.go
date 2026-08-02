package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Byte-determinism of the serialised CSR.
//
// Two compactions of the same logical store must produce byte-identical files.
// Without that, the file's digest is not a stable identity for its contents, so
// nothing can be built on it: a snapshot hash is not reproducible, two parties
// holding identical evidence cannot agree on a digest, and "did this change?"
// cannot be answered by comparing hashes.
//
// Record order is not the risk — Build() scatters into an ID-indexed array and
// Serialise walks it, so records are emitted in ascending ID order regardless of
// the order they were collected in. The index section is the risk: it is written
// from PropertyIndex.NodeEntries()/EdgeEntries(), which range over Go maps.

// determinismFixture writes a store whose property index spans many shards and
// many keys, so a difference in map iteration order actually shows up in the
// bytes rather than being masked by a fixture too small to reorder.
func determinismFixture(t *testing.T, s *Store) {
	t.Helper()

	const nodeCount = 120
	keys := []string{"sha256", "path", "tool", "case_id", "mime", "author"}

	for i := 1; i <= nodeCount; i++ {
		id := addNodeD(t, s, store.NodeTypeMicroArtefact)
		for k, key := range keys {
			val := fmt.Sprintf("%s-%04d", key, i*7+k)
			if err := s.IndexNodeProperty(id, key, []byte(val)); err != nil {
				t.Fatalf("IndexNodeProperty(%d, %q): %v", id, key, err)
			}
		}
		if i > 1 {
			eid := addEdgeD(t, s, store.NodeID(i-1), id, store.EdgeTypeContains)
			if err := s.IndexEdgeProperty(eid, "rel", []byte(fmt.Sprintf("rel-%04d", i))); err != nil {
				t.Fatalf("IndexEdgeProperty(%d): %v", eid, err)
			}
		}
	}
}

// readCSR returns the on-disk CSR image.
func readCSR(t *testing.T, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatalf("read CSR: %v", err)
	}
	return data
}

// describeDiff reports where two CSR images diverge and, when the divergence is
// inside the index section, says so — that is the difference between "the graph
// changed" and "the same graph serialised differently".
func describeDiff(t *testing.T, a, b []byte) string {
	t.Helper()
	if len(a) != len(b) {
		return fmt.Sprintf("lengths differ: %d vs %d bytes", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			offA := binary.LittleEndian.Uint64(a[38:46])
			where := "record section"
			if uint64(i) >= offA {
				where = "index section (GIDX)"
			}
			return fmt.Sprintf("first difference at byte %d of %d, in the %s (indexOffset=%d): %#02x vs %#02x",
				i, len(a), where, offA, a[i], b[i])
		}
	}
	return "no byte difference"
}

// stripVolatile blanks the one header field that is not a function of the
// store's contents, so two images can be compared for content equality.
//
// Only the compaction timestamp qualifies. It is a wall-clock reading, so it
// differs between two compactions of an identical store by design — which is
// why the digest excludes it too. Everything else in a v8 header is derived from
// what the file holds.
func stripVolatile(data []byte) []byte {
	out := append([]byte(nil), data...)
	if len(out) >= csrV8HeaderSize {
		for i := csrLastCompactOffset; i < csrLastCompactOffset+8; i++ {
			out[i] = 0
		}
	}
	return out
}

// The digest is the same across two compactions of an unchanged store.
//
// This is the property that actually matters, and it is stronger than comparing
// files: the digest is what a second party checks, so if it is stable then two
// holders of the same evidence agree even though their files differ in when they
// were written.
func TestCompact_DigestIsStableAcrossCompactions(t *testing.T) {
	s, dir := openFresh(t)
	defer s.Close()

	determinismFixture(t, s)

	if err := s.Compact(); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	firstStatus, firstDigest, err := VerifyCSRDigest(dir)
	if err != nil {
		t.Fatalf("verify after first compact: %v", err)
	}
	if firstStatus != DigestMatch {
		t.Fatalf("digest status after writing the file: %v", firstStatus)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	secondStatus, secondDigest, err := VerifyCSRDigest(dir)
	if err != nil {
		t.Fatalf("verify after second compact: %v", err)
	}
	if secondStatus != DigestMatch {
		t.Fatalf("digest status after recompaction: %v", secondStatus)
	}

	if firstDigest != secondDigest {
		t.Fatalf("recompacting an unchanged store changed its digest:\n  %x\n  %x\n\n"+
			"The digest is meant to identify what the file holds, so recompaction "+
			"must not change it — otherwise a verifier cannot tell 'rebuilt' from 'altered'.",
			firstDigest, secondDigest)
	}
}

// Compacting twice without mutating the store must rewrite the same bytes,
// ignoring the compaction timestamp.
//
// This is the minimal statement of the property and the one that fails first:
// both compactions run in one process against one property index, so the only
// thing that can differ is the order in which that index was enumerated.
func TestCompact_IsByteDeterministic(t *testing.T) {
	s, dir := openFresh(t)
	defer s.Close()

	determinismFixture(t, s)

	if err := s.Compact(); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	first := readCSR(t, dir)

	if err := s.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	second := readCSR(t, dir)

	first, second = stripVolatile(first), stripVolatile(second)
	if !bytes.Equal(first, second) {
		t.Fatalf("two compactions of an unchanged store produced different bytes.\n%s\n\n"+
			"The CSR image is not a stable identity for its contents, so no digest, "+
			"Merkle root, or attestation can be computed over it.", describeDiff(t, first, second))
	}
}

// Two stores built by the same operations in the same order must be byte-equal.
//
// Stronger than the single-store case and the one that matters for evidence
// handling: it is what lets two parties independently reconstruct a store and
// agree on its digest. It also covers per-process state the single-store test
// shares — separate maps, separate insertion histories, separate shards.
func TestCompact_IdenticalStoresProduceIdenticalBytes(t *testing.T) {
	build := func() []byte {
		s, dir := openFresh(t)
		defer s.Close()
		determinismFixture(t, s)
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		return readCSR(t, dir)
	}

	a, b := build(), build()

	a, b = stripVolatile(a), stripVolatile(b)
	if !bytes.Equal(a, b) {
		t.Fatalf("two stores built by identical operations produced different bytes.\n%s\n\n"+
			"Independent reconstruction of the same evidence does not yield the same "+
			"digest, so a snapshot hash cannot be verified by a second party.", describeDiff(t, a, b))
	}
}

// Enumerating the property index twice must yield the same order.
//
// Separated from the two tests above so a failure names its cause rather than
// leaving it to be inferred from a byte offset: NodeEntries and EdgeEntries walk
// 16 shards and range over the maps inside each, and Go randomises map iteration
// order per range.
func TestPropertyIndexEntries_EnumerationIsStable(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	determinismFixture(t, s)

	nodesA, nodesB := s.propIdx.NodeEntries(), s.propIdx.NodeEntries()
	if len(nodesA) != len(nodesB) {
		t.Fatalf("NodeEntries returned %d then %d entries", len(nodesA), len(nodesB))
	}
	for i := range nodesA {
		if nodesA[i].ID != nodesB[i].ID || nodesA[i].Key != nodesB[i].Key ||
			!bytes.Equal(nodesA[i].Value, nodesB[i].Value) {
			t.Fatalf("NodeEntries order differs at index %d of %d: (%d,%q,%q) vs (%d,%q,%q)",
				i, len(nodesA),
				nodesA[i].ID, nodesA[i].Key, nodesA[i].Value,
				nodesB[i].ID, nodesB[i].Key, nodesB[i].Value)
		}
	}

	edgesA, edgesB := s.propIdx.EdgeEntries(), s.propIdx.EdgeEntries()
	if len(edgesA) != len(edgesB) {
		t.Fatalf("EdgeEntries returned %d then %d entries", len(edgesA), len(edgesB))
	}
	for i := range edgesA {
		if edgesA[i].ID != edgesB[i].ID || edgesA[i].Key != edgesB[i].Key ||
			!bytes.Equal(edgesA[i].Value, edgesB[i].Value) {
			t.Fatalf("EdgeEntries order differs at index %d of %d: (%d,%q,%q) vs (%d,%q,%q)",
				i, len(edgesA),
				edgesA[i].ID, edgesA[i].Key, edgesA[i].Value,
				edgesB[i].ID, edgesB[i].Key, edgesB[i].Value)
		}
	}
}
