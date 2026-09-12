package disk

// A byte-for-byte comparison against an image written by the build that came
// before the page table.
//
// Every other byte test in this package compares a build against itself:
// TestCompact_IsByteDeterministic serialises the same store twice and
// TestCompact_IdenticalStoresProduceIdenticalBytes serialises two stores built
// the same way. A change that reordered the record stream *consistently* — which
// is exactly what getting a paged walk wrong looks like — satisfies both while
// making every image already on disk unreadable, because the Merkle roots inside
// those images were computed from the old order.
//
// So this fixture is generated once, from the previous tree, and checked
// afterwards. Regenerating it is not a way to make this test pass: the file is
// the contract.

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

const goldenCSRPath = "testdata/csr_v8_before_pages.bin"

// goldenCSRFixture builds the graph the golden image is written from.
//
// The identifiers deliberately straddle page boundaries and leave whole pages
// empty, because that is the arrangement a paged layout can get wrong and a flat
// one cannot.
func goldenCSRFixture() ([]nodeRecord, []rawEdge, csrPayload) {
	ids := []store.NodeID{1, 2, 3, 4095, 4096, 4097, 8191, 12288}
	nodes := make([]nodeRecord, 0, len(ids))
	for i, id := range ids {
		labels := []store.NodeType{store.NodeTypeMicroArtefact}
		if i%3 == 0 {
			labels = append(labels, store.NodeTypeEvidenceFile)
		}
		nodes = append(nodes, nodeRecord{
			ID:         id,
			Labels:     labels,
			Properties: []byte(fmt.Sprintf("payload-for-%d", id)),
		})
	}

	edges := []rawEdge{
		{ID: 1, Src: 1, Dst: 4096, Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.5,
			Properties: []byte("e1")},
		{ID: 2, Src: 4095, Dst: 4097, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}, Weight: 1},
		{ID: 3, Src: 12288, Dst: 12288, Labels: []store.EdgeType{store.EdgeTypeContains}},
		{ID: 9, Src: 8191, Dst: 1, Labels: []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeSimilarTo},
			Weight: 2.25, Properties: []byte("e9")},
	}

	payload := csrPayload{
		NodeProps: slices.Values([]index.NodePropEntry{
			{ID: 1, Key: "path", Value: []byte("/a")},
			{ID: 4096, Key: "path", Value: []byte("/b")},
			{ID: 8191, Key: "sha256", Value: []byte("00ff")},
		}),
		EdgeProps: slices.Values([]index.EdgePropEntry{
			{ID: 1, Key: "rel", Value: []byte("contains")},
			{ID: 9, Key: "rel", Value: []byte("similar")},
		}),
		OrderedNodeKeys:   []string{"path"},
		CompositeNodeKeys: [][]string{{"path", "sha256"}},
		WithSnapshotRoots: true,
		Tombstones: []Tombstone{
			{Scope: ScopeNode, NodeID: 77, RedactionSeq: 4},
		},
	}
	return nodes, edges, payload
}

// goldenCSRBytes serialises that fixture with the marks the generator stamped,
// so the header is identical too.
func goldenCSRBytes(t *testing.T) []byte {
	t.Helper()
	nodes, edges, payload := goldenCSRFixture()
	g, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	g.nodeSeqHW, g.edgeSeqHW = 20000, 40
	g.commitSeqHW = 42
	g.lastCompactUnixNano = 1_757_000_000_000_000_000

	data, err := g.SerialiseWithPayload(payload)
	if err != nil {
		t.Fatalf("serialise: %v", err)
	}
	return data
}

// The bytes this build writes must be the bytes the previous build wrote.
func TestCSRPages_WalkOrderMatchesSerialisedBytes(t *testing.T) {
	want, err := os.ReadFile(filepath.FromSlash(goldenCSRPath))
	if err != nil {
		t.Fatalf("reading the golden image: %v", err)
	}
	got := goldenCSRBytes(t)

	if !bytes.Equal(got, want) {
		t.Fatalf("serialised %d bytes, the pre-page-table build wrote %d; first difference at %d.\n"+
			"The record stream or the roots moved. Regenerating this fixture would hide it: "+
			"images already on disk carry roots computed from the old order.",
			len(got), len(want), firstDiff(got, want))
	}
}

// And the image the previous build wrote must still parse, still verify against
// the roots it carries, and still produce a proof that resolves.
func TestCSRPages_GoldenImageStillVerifies(t *testing.T) {
	data, err := os.ReadFile(filepath.FromSlash(goldenCSRPath))
	if err != nil {
		t.Fatalf("reading the golden image: %v", err)
	}

	csr, section, err := deserialiseCSR(data)
	if err != nil {
		t.Fatalf("the pre-page-table image no longer parses: %v", err)
	}
	if got, want := csr.NodeCount(), 8; got != want {
		t.Fatalf("parsed %d nodes, want %d", got, want)
	}
	if got, want := csr.EdgeCount(), 4; got != want {
		t.Fatalf("parsed %d edges, want %d", got, want)
	}
	if got, want := csr.HighestNodeID(), store.NodeID(12288); got != want {
		t.Fatalf("HighestNodeID = %d, want %d", got, want)
	}

	// The roots in the file were computed by the old walk. Recomputing them from
	// the paged graph is the assertion.
	if err := verifyCSRRootsOf(csr, section); err != nil {
		t.Fatalf("roots recomputed from the paged layout disagree with the image: %v", err)
	}
	if err := csr.verifyAdjacency(); err != nil {
		t.Fatalf("verifyAdjacency: %v", err)
	}
	if err := csr.verifyLabelIndex(); err != nil {
		t.Fatalf("verifyLabelIndex: %v", err)
	}

	// An inclusion proof for a node several pages in, verified against the root
	// the file carries — the leaf index has to be the rank the old walk gave it.
	roots, ok := csr.Roots()
	if !ok {
		t.Fatal("the golden image carries no snapshot roots")
	}
	id := store.NodeID(8191)
	idx, present := csr.nodeLeafIndex(id)
	if !present {
		t.Fatalf("node %d is missing from the parsed image", id)
	}
	leaves := csr.NodeLeaves(roots.bodyVersion())
	proof, err := merkle.BuildProof(leaves, idx)
	if err != nil {
		t.Fatalf("BuildProof: %v", err)
	}
	rec, _ := csr.GetNode(id)
	leaf := merkle.HashLeaf(nodeLeafFor(roots.bodyVersion(), rec))
	if !merkle.VerifyProof(roots.NodeRoot, leaf, proof) {
		t.Fatalf("the inclusion proof for node %d does not resolve to the image's node root", id)
	}
}

func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
