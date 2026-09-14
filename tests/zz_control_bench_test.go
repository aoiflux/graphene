//go:build stress

package graphene_test

// Control for the R8 refactor: UpsertNode now routes through a shared helper
// that the owned variant also calls. This arm exists in both trees and calls
// only UpsertNode, so it measures whether that indirection cost anything.

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func BenchmarkControlUpsertTx(b *testing.B) {
	const perTx = 1000
	for _, blobBytes := range []int{512, 65536} {
		blob := make([]byte, blobBytes)
		b.Run(fmt.Sprintf("blob=%d", blobBytes), func(b *testing.B) {
			g, err := graphene.Open(b.TempDir())
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer g.Close()
			if err := g.DeclareUniqueProperty("k"); err != nil {
				b.Fatalf("DeclareUniqueProperty: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx := g.Begin()
				for j := 0; j < perTx; j++ {
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
		})
	}
}
