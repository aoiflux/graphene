//go:build stress

package disk

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// What the bounded check over a mapped index costs, and that what it costs in
// memory does not depend on the size of the index.
//
// Two sizes rather than one, because the only interesting question here is the
// shape: the time should be linear in the entries and the allocation should be
// flat. A single size cannot tell you either. `B/op` and `allocs/op` reported
// identical across the two sizes is the boundedness requirement
// index.Base.Verify states, observed at the store level.
//
//	go test -tags=stress ./disk/ -run '^$' -bench VerifyIndexes -benchmem -benchtime 3x
func BenchmarkVerifyIndexes_Mapped(b *testing.B) {
	for _, nodes := range []int{5_000, 20_000} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			s := verifyBenchStore(b, nodes)
			defer s.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.VerifyIndexes(); err != nil {
					b.Fatalf("verify: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(nodes*len(verifyBenchKeys)), "entries")
		})
	}
}

// verifyBenchKeys is the consumer's index shape in miniature: one all-distinct
// 32-byte digest, one ordered sequence, and three of decreasing cardinality.
var verifyBenchKeys = []string{"seq", "digest", "bucket", "shard", "kind"}

// verifyBenchStore writes a v9 image and reopens it, so the base being checked is
// a mapping rather than something a compaction left in memory.
func verifyBenchStore(tb testing.TB, nodes int) *Store {
	tb.Helper()
	dir := tb.TempDir()
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	if err := s.DeclareOrderedNodeProperty("seq"); err != nil {
		tb.Fatalf("DeclareOrderedNodeProperty: %v", err)
	}
	for i := 0; i < nodes; i++ {
		id, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: []byte(`{}`),
		})
		if err != nil {
			tb.Fatalf("AddNode: %v", err)
		}
		values := []string{
			fmt.Sprintf("%08d", i),
			fmt.Sprintf("%064x", i),
			fmt.Sprintf("b%d", i%64),
			fmt.Sprintf("s%d", i%16),
			fmt.Sprintf("k%d", i%4),
		}
		for j, key := range verifyBenchKeys {
			if err := s.IndexNodeProperty(id, key, []byte(values[j])); err != nil {
				tb.Fatalf("IndexNodeProperty: %v", err)
			}
		}
	}
	if err := s.Compact(); err != nil {
		tb.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		tb.Fatalf("Close: %v", err)
	}
	s, err = OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		tb.Fatalf("reopen: %v", err)
	}
	if got := s.StorageStats().IndexMode; got != IndexMapped.String() {
		s.Close()
		tb.Skipf("this platform opened the index as %q, so there is no base to check", got)
	}
	return s
}
