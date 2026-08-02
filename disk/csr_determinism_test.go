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

// Recompacting an unchanged store leaves its CONTENT identity untouched while
// advancing its HISTORY.
//
// These pull apart deliberately. The component roots — nodes, edges, index — are
// pure functions of what the store holds, so recompaction must not move them: if
// it did, a verifier could not tell "rebuilt" from "altered", which is what
// canonical serialisation was introduced to guarantee (T-17).
//
// The snapshot root is different by design. It binds the component roots to the
// predecessor it replaces, so each compaction produces a new one. That is the
// chain: a substituted image can be internally perfect and still cannot claim a
// predecessor it never had.
//
// The whole-file digest follows the snapshot root, because the roots live in the
// file. So the digest identifies "this exact image", and the component roots
// identify "this content" — two questions that were conflated while there was
// only one answer available.
func TestCompact_RecompactionKeepsContentAndAdvancesHistory(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	determinismFixture(t, s)

	if err := s.Compact(); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	first, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	second, err := s.SnapshotRoots()
	if err != nil {
		t.Fatal(err)
	}

	if second.NodeRoot != first.NodeRoot {
		t.Errorf("recompaction changed the node root; content did not change:\n  %x\n  %x",
			first.NodeRoot, second.NodeRoot)
	}
	if second.EdgeRoot != first.EdgeRoot {
		t.Errorf("recompaction changed the edge root:\n  %x\n  %x", first.EdgeRoot, second.EdgeRoot)
	}
	if second.IndexRoot != first.IndexRoot {
		t.Errorf("recompaction changed the index root:\n  %x\n  %x", first.IndexRoot, second.IndexRoot)
	}

	if second.Snapshot == first.Snapshot {
		t.Fatal("recompaction left the snapshot root unchanged; the chain records no history")
	}
	if second.PrevRoot != first.Snapshot {
		t.Fatalf("the second snapshot names predecessor %x, but the first was %x",
			second.PrevRoot[:8], first.Snapshot[:8])
	}
	if err := VerifyChain(first, second); err != nil {
		t.Fatalf("consecutive compactions do not form a chain: %v", err)
	}
}

// Serialising the same graph twice with the same payload produces the same
// bytes.
//
// This is BL-1's property, stated against serialisation rather than against two
// sequential compactions — the chain makes consecutive compactions differ on
// purpose, which would otherwise mask exactly the non-determinism this is here
// to catch. Map iteration order leaking into the output still fails it.
func TestCompact_IsByteDeterministic(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	determinismFixture(t, s)
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.mu.RLock()
	csr := s.csr
	s.mu.RUnlock()

	payload := func() csrPayload {
		return csrPayload{
			NodeProps:         s.propIdx.NodeEntries(),
			EdgeProps:         s.propIdx.EdgeEntries(),
			OrderedNodeKeys:   s.propIdx.OrderedNodeKeys(),
			OrderedEdgeKeys:   s.propIdx.OrderedEdgeKeys(),
			WithSnapshotRoots: true,
		}
	}

	serialise := func() []byte {
		out, err := csr.SerialiseWithPayload(payload())
		if err != nil {
			t.Fatalf("SerialiseWithPayload: %v", err)
		}
		return stripVolatile(out)
	}
	first, second := serialise(), serialise()

	if !bytes.Equal(first, second) {
		t.Fatalf("serialising the same graph twice produced different bytes.\n%s\n\n"+
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
