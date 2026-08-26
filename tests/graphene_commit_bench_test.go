//go:build stress

package graphene_test

// Concurrent commit throughput: the figure group commit exists to move.
//
// Every batch commit fsyncs by default (SetSyncOnCommit is true), and an fsync
// is a wait rather than work. What matters is therefore not how fast one
// committer is but how many can be waiting on the same wait — so the benchmark
// is parameterised by writer count and the interesting column is ns/op as that
// count rises. Perfect sharing keeps it flat; no sharing makes it grow
// linearly.
//
// Run it against a HEAD worktree the way CONTRIBUTING describes. It is
// deliberately written against the public API only, so the same file compiles
// on both sides of the comparison.

import (
	"fmt"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func benchmarkConcurrentCommits(b *testing.B, writers int) {
	dir := b.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer g.Close()

	// Small batches. A large batch amortises its own fsync and would hide the
	// effect being measured; a batch of a few records is the case where the
	// fsync dominates, which is the case that was serialised.
	const perBatch = 4

	b.ResetTimer()
	b.SetParallelism(1)

	var wg sync.WaitGroup
	perWriter := b.N / writers
	if perWriter < 1 {
		perWriter = 1
	}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodes := make([]*store.Node, perBatch)
			for i := range nodes {
				nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
			}
			for i := 0; i < perWriter; i++ {
				if _, err := g.AddNodes(nodes); err != nil {
					b.Errorf("AddNodes: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
}

func BenchmarkConcurrentCommits(b *testing.B) {
	for _, writers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("writers%d", writers), func(b *testing.B) {
			benchmarkConcurrentCommits(b, writers)
		})
	}
}
