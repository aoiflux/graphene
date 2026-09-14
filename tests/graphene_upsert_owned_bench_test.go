//go:build stress

package graphene_test

// What UpsertNodeOwned costs, against UpsertNode.
//
// Each arm is its own function, and each mentions exactly one of the two methods.
// That is load-bearing, not tidiness: a single body that branches between them
// passes n to UpsertNodeOwned on one path, so n escapes at the call site on both,
// and the copying arm is charged two heap allocations a real caller would not pay.
// The first version of this file branched, and read the saving as three
// allocations per node instead of one. See TECHNICAL_DETAILS.md §14.27.
//
// Both arms build a fresh *store.Node per entity with the blob already in hand,
// which is the shape the copy is paid on — a fixture that reused one node would
// measure nothing.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

const upsertPerTx = 1000

// upsertArmGraph opens a disk-backed graph with the unique key declared.
func upsertArmGraph(tb testing.TB) *graphene.Graph {
	tb.Helper()
	g, err := graphene.Open(tb.TempDir())
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() { _ = g.Close() })
	if err := g.DeclareUniqueProperty("k"); err != nil {
		tb.Fatalf("DeclareUniqueProperty: %v", err)
	}
	return g
}

func benchUpsertTxCopied(b *testing.B, blob []byte) {
	g := upsertArmGraph(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := g.Begin()
		for j := 0; j < upsertPerTx; j++ {
			n := &store.Node{
				Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
				Properties: append([]byte(nil), blob...),
			}
			tx.UpsertNode("k", []byte(fmt.Sprintf("ver:%d:%d", i, j)), n, nil)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
}

func benchUpsertTxOwned(b *testing.B, blob []byte) {
	g := upsertArmGraph(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := g.Begin()
		for j := 0; j < upsertPerTx; j++ {
			n := &store.Node{
				Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
				Properties: append([]byte(nil), blob...),
			}
			tx.UpsertNodeOwned("k", []byte(fmt.Sprintf("ver:%d:%d", i, j)), n, nil)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
}

func BenchmarkABUpsertTx(b *testing.B) {
	for _, blobBytes := range []int{512, 65536} {
		blob := make([]byte, blobBytes)
		b.Run(fmt.Sprintf("blob=%d/copied", blobBytes), func(b *testing.B) {
			benchUpsertTxCopied(b, blob)
		})
		b.Run(fmt.Sprintf("blob=%d/owned", blobBytes), func(b *testing.B) {
			benchUpsertTxOwned(b, blob)
		})
	}
}

// The RSS arms hold one large transaction open and measure the process while it
// is still buffered, which is where the copy lives: every op's node is retained
// from the call until Commit, so a bulk ingest pays for two copies of its own
// blobs at once.
//
// One arm per process — the peak is a process-lifetime high-water mark and a
// second arm in the same process would inherit the first one's.

const upsertRSSNodes = 200_000
const upsertRSSBlob = 512

func upsertRSSBlobs() [][]byte {
	blobs := make([][]byte, upsertRSSNodes)
	for j := range blobs {
		blobs[j] = make([]byte, upsertRSSBlob)
	}
	return blobs
}

func BenchmarkRSS_UpsertCopied(b *testing.B) {
	for i := 0; i < b.N; i++ {
		g := upsertArmGraph(b)
		blobs := upsertRSSBlobs()
		tx := g.Begin()
		for j := 0; j < upsertRSSNodes; j++ {
			n := &store.Node{
				Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
				Properties: blobs[j],
			}
			tx.UpsertNode("k", []byte(fmt.Sprintf("ver:%d", j)), n, nil)
		}
		// Measured with the transaction still buffered: here the caller's blobs
		// and the buffered ops are two sets of bytes.
		reportRSS(b, tx)
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}
		runtime.KeepAlive(blobs)
	}
}

func BenchmarkRSS_UpsertOwned(b *testing.B) {
	for i := 0; i < b.N; i++ {
		g := upsertArmGraph(b)
		blobs := upsertRSSBlobs()
		tx := g.Begin()
		for j := 0; j < upsertRSSNodes; j++ {
			n := &store.Node{
				Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
				Properties: blobs[j],
			}
			tx.UpsertNodeOwned("k", []byte(fmt.Sprintf("ver:%d", j)), n, nil)
		}
		// Here they are the same bytes: blobs is retained only so the fixture
		// holds what a caller would still be holding, which is the honest
		// comparison against the arm above.
		reportRSS(b, tx)
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}
		runtime.KeepAlive(blobs)
	}
}
