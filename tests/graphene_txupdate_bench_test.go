//go:build stress

package graphene_test

// Transactional update cost, with the fsync taken out.
//
// BenchmarkConcurrentCommits measures the durability wait, which on a loaded
// machine swamps everything else by an order of magnitude. This one turns
// syncOnCommit off so what is left is the part version retention actually
// touches: stacking a version, truncating the chain behind it, and maintaining
// the label posting beside it.
//
// It exists because retention now keeps the version a concurrent reader is
// still entitled to see for the length of a commit's durability wait, which
// means a chain settles at two entries where it used to collapse to one. That
// is a real cost and this is where it would show.
//
// Written against the public API only, so it compiles against a HEAD worktree
// for the interleaved A/B that CONTRIBUTING describes.

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func benchmarkTxUpdate(b *testing.B, width int) {
	g, err := graphene.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	// Same escape hatch BenchmarkBulkWrite_AddNodes_Disk_NoSync uses: the fsync
	// is a wait, not work, and it is not what this measures.
	if ds, ok := g.GraphStore.(interface{ SetSyncOnCommit(bool) }); ok {
		ds.SetSyncOnCommit(false)
	}

	labels := []store.NodeType{store.NodeTypeCase}
	ids := make([]store.NodeID, width)
	for i := range ids {
		id, err := g.AddNode(&store.Node{Labels: labels, Properties: []byte("seed")})
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		tx := g.Begin()
		for _, id := range ids {
			tx.UpdateNode(&store.Node{ID: id, Labels: labels, Properties: []byte{byte('a' + i%26)}})
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// One record, updated over and over: the shape that makes a version chain grow.
func BenchmarkTxUpdate_Disk_NoSync_1(b *testing.B) { benchmarkTxUpdate(b, 1) }

// A wider transaction, so the per-commit overhead is amortised and what remains
// is the per-record delta write.
func BenchmarkTxUpdate_Disk_NoSync_64(b *testing.B) { benchmarkTxUpdate(b, 64) }
