//go:build stress

package graphene_test

// Writer stall during a compaction: the figure the three-stage Compact exists
// to move.
//
// Compaction used to hold the store's write lock from its first map read to its
// last publish, which put a sort, a Merkle pass over every record, a whole-image
// serialisation and a whole-image fsync inside a writer's critical section. The
// cost is not throughput — a compaction is rare — it is a single unbounded stall
// on every writer, growing with the size of the store. That is what makes an
// engine unusable under load however good its steady-state numbers are.
//
// So the benchmark measures the stall, not the compaction. One writer commits
// in a loop while a compaction runs beside it; what is reported is the worst
// single commit and how many commits got through. Compaction wall time is
// reported too, because a change that made the stall smaller by making the
// compaction slower would not be an improvement.
//
// Run it against a HEAD worktree the way CONTRIBUTING describes. Written
// against the public API only, so the same file compiles on both sides.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// benchmarkCompactStall builds a store of size records, then times a compaction
// with one writer committing beside it.
func benchmarkCompactStall(b *testing.B, size int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := b.TempDir()
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatalf("Open: %v", err)
		}

		// A store large enough that the build dominates. Fed in batches because
		// the point is to have an image to rebuild, not to measure ingest.
		batch := make([]*store.Node, 256)
		for j := range batch {
			batch[j] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		for written := 0; written < size; written += len(batch) {
			if _, err := g.AddNodes(batch); err != nil {
				b.Fatalf("AddNodes: %v", err)
			}
		}
		// Compact once first, so the timed compaction rebuilds an existing image
		// rather than building the first one. That is the steady-state shape.
		if err := g.Compact(); err != nil {
			b.Fatalf("warm Compact: %v", err)
		}
		for written := 0; written < size/4; written += len(batch) {
			if _, err := g.AddNodes(batch); err != nil {
				b.Fatalf("AddNodes: %v", err)
			}
		}

		var (
			wg       sync.WaitGroup
			commits  int
			worst    time.Duration
			compacts time.Duration
		)
		done := make(chan struct{})

		b.StartTimer()
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			if err := g.Compact(); err != nil {
				b.Errorf("Compact: %v", err)
			}
			compacts = time.Since(start)
			close(done)
		}()

		one := []*store.Node{{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}}
		for {
			select {
			case <-done:
			default:
				t0 := time.Now()
				if _, err := g.AddNodes(one); err != nil {
					b.Fatalf("AddNodes during compact: %v", err)
				}
				if d := time.Since(t0); d > worst {
					worst = d
				}
				commits++
				continue
			}
			break
		}
		wg.Wait()
		b.StopTimer()

		b.ReportMetric(float64(worst.Microseconds()), "worst-stall-us")
		b.ReportMetric(float64(commits), "commits-during-compact")
		b.ReportMetric(float64(compacts.Milliseconds()), "compact-ms")

		if err := g.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		b.StartTimer()
	}
}

func BenchmarkCompactStall(b *testing.B) {
	for _, size := range []int{20_000, 100_000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			benchmarkCompactStall(b, size)
		})
	}
}
