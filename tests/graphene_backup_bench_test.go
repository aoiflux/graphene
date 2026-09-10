//go:build stress

package graphene_test

// What a backup costs the store it is taken from.
//
// A backup is a copy of the directory with the store still open and still being
// written, so the question is not how fast it copies — that is the disk — but
// what a writer pays while it does. Two things could make it pay:
//
//   - The pin. Backup takes the store lock to note where the log stands and to
//     list what to copy. Every writer waits on that, so it belongs in the same
//     family as the compaction stall and is measured the same way: the worst
//     single commit while a backup runs.
//   - The copy. It runs with no lock at all, but it is I/O against the same
//     files the writer is appending to, so writers can still slow down without
//     ever blocking.
//
// So the benchmark reports the worst stall, how many commits got through, and
// the throughput ratio against an unloaded control run of the same length. A
// backup that halved write throughput while blocking nobody would look perfect
// on the stall figure alone.
//
// Written against the public API only, so the same file compiles against a HEAD
// worktree — though there is nothing to compare against on that side: the whole
// feature is new. The control here is the *same process* with no backup running,
// which is the comparison that actually answers the question.

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// commitLoop commits single-node batches until done closes, reporting how many
// got through and the worst single commit.
func commitLoop(b *testing.B, g *graphene.Graph, done <-chan struct{}) (int, time.Duration) {
	b.Helper()
	one := []*store.Node{{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}}
	var (
		commits int
		worst   time.Duration
	)
	for {
		select {
		case <-done:
			return commits, worst
		default:
		}
		t0 := time.Now()
		if _, err := g.AddNodes(one); err != nil {
			b.Fatalf("AddNodes: %v", err)
		}
		if d := time.Since(t0); d > worst {
			worst = d
		}
		commits++
	}
}

func benchmarkBackupCost(b *testing.B, size int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := b.TempDir()
		g, err := graphene.Open(dir)
		if err != nil {
			b.Fatalf("Open: %v", err)
		}

		batch := make([]*store.Node, 256)
		for j := range batch {
			batch[j] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		for written := 0; written < size; written += len(batch) {
			if _, err := g.AddNodes(batch); err != nil {
				b.Fatalf("AddNodes: %v", err)
			}
		}
		// An image to copy, and a log above it, which is the shape a backup
		// actually meets.
		if err := g.Compact(); err != nil {
			b.Fatalf("Compact: %v", err)
		}
		for written := 0; written < size/4; written += len(batch) {
			if _, err := g.AddNodes(batch); err != nil {
				b.Fatalf("AddNodes: %v", err)
			}
		}

		b.StartTimer()

		// The backup, with a writer beside it.
		var (
			wg      sync.WaitGroup
			took    time.Duration
			copied  int64
			backups = make(chan struct{})
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			info, berr := g.Backup(filepath.Join(b.TempDir(), "backup"))
			took = time.Since(start)
			copied = info.Bytes()
			if berr != nil {
				b.Errorf("Backup: %v", berr)
			}
			close(backups)
		}()
		loaded, worst := commitLoop(b, g, backups)
		wg.Wait()

		// The control: the same writer, for the same length of time, with
		// nothing else running. Read after the headline rather than before,
		// deliberately — it has to be the same store in the same state, and the
		// backup is what puts it there.
		idle := make(chan struct{})
		go func() {
			time.Sleep(took)
			close(idle)
		}()
		free, freeWorst := commitLoop(b, g, idle)

		b.StopTimer()

		b.ReportMetric(float64(worst.Microseconds()), "worst-stall-us")
		b.ReportMetric(float64(freeWorst.Microseconds()), "control-worst-us")
		b.ReportMetric(float64(took.Milliseconds()), "backup-ms")
		b.ReportMetric(float64(copied)/(1<<20), "copied-MiB")
		if free > 0 {
			b.ReportMetric(float64(loaded)/float64(free), "throughput-vs-idle")
		}

		if err := g.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		b.StartTimer()
	}
}

func BenchmarkBackupCost(b *testing.B) {
	for _, size := range []int{20_000, 100_000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			benchmarkBackupCost(b, size)
		})
	}
}

// =============================================================================
// What a backup allocates, with nothing else running
// =============================================================================

// BenchmarkBackup_Quiescent is the other half of the question above: not what
// a writer pays while a backup runs, but what the backup itself allocates. It
// exists because the answer was once the whole image. A backup streams every
// file through a copy buffer and then re-verifies the copied image's digest,
// and that last leg read the image whole — so on the target workload a backup
// allocated the store's footprint a second time, on the operation an operator
// runs because the machine is already short of it. B/op is the figure to read.
// The fixture is the residency suite's own, so the row can be read beside
// BenchmarkRSS_Restore.
func BenchmarkBackup_Quiescent(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer g.Close()

	fi, err := os.Stat(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		b.Fatal(err)
	}
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := filepath.Join(root, strconv.Itoa(i))
		if _, err := g.Backup(dst); err != nil {
			b.Fatalf("Backup: %v", err)
		}
		b.StopTimer()
		os.RemoveAll(dst)
		b.StartTimer()
	}
	b.StopTimer()
	// Reported after the loop: ResetTimer deletes user-reported metrics.
	b.ReportMetric(float64(fi.Size())/(1<<20), "imageMiB")
}

// BenchmarkVerifyCSRDigest is that leg on its own: the digest a compacted
// image carries, checked against its contents. It is the check every backup
// ends with and the one `store csr -verify` and `debug hash-check` begin with.
func BenchmarkVerifyCSRDigest(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	fi, err := os.Stat(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, _, err := disk.VerifyCSRDigest(dir)
		if err != nil {
			b.Fatalf("VerifyCSRDigest: %v", err)
		}
		if status != disk.DigestMatch {
			b.Fatalf("digest status %v, want match", status)
		}
	}
	b.StopTimer()
	// Reported after the loop: ResetTimer deletes user-reported metrics.
	b.ReportMetric(float64(fi.Size())/(1<<20), "imageMiB")
}
