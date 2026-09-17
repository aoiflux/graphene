//go:build stress

package graphene_test

// What an open of a large store actually holds, and where it comes from.
//
// Phase 6 of docs/PLAN_BOUNDED_INGEST.md is gated on Phase 0 saying that the
// at-rest term rather than the transient is what is left over the line. MEMORY_MODEL
// section 9.12 says it is: the charged peak is linear at 263 MiB per million
// records and crosses 2,048 MiB at about 7.6 million, and the load itself is flat
// at 12 MiB. So the crossing is the *reopen*, which is unavoidable -- a store
// nobody can open is not a store -- and the question this answers is what the
// reopen is made of.
//
// It is a read-only measurement over a fixture that already exists, which is why
// it is a separate test rather than another arm of the ceiling harness: the
// harness builds what it measures, and at two million 3.2 KB records that is
// 7.4 GiB of writes to ask the same question a second time.
//
// Two figures come out. The peak and settled anonymous memory across the open,
// from the same instrument every other arm here uses; and an inuse_space heap
// profile written at the peak, which is the part that says *which term*. The
// estimate breaks the settled figure into records, payload, labels and adjacency
// already -- what it cannot say is what the open allocated on the way there and
// gave back, and that gap is 2.5x the settled figure at two million records.
//
//	GRAPHENE_ARENA_DIR=/path/to/fixture GRAPHENE_ARENA_PROFILE=/path/to/out.pprof \
//	  go test ./tests/ -tags=stress -count=1 -run TestArenaOpenProfile -v

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
)

var (
	arenaDir     = os.Getenv("GRAPHENE_ARENA_DIR")
	arenaProfile = os.Getenv("GRAPHENE_ARENA_PROFILE")
)

// arenaOpenArms are the configurations worth separating.
//
// Adjacency is the one term a caller can already switch off, so it is here as the
// control: whatever the lazy arm still holds is what no existing option reaches,
// and that is the figure Phase 6 would have to move.
var arenaOpenArms = []struct {
	name string
	opts disk.Options
}{
	{"Defaults", disk.Options{}},
	{"AdjacencyLazy", disk.Options{Adjacency: disk.AdjacencyLazy}},
}

func TestArenaOpenProfile(t *testing.T) {
	if arenaDir == "" {
		t.Skip("set GRAPHENE_ARENA_DIR to a built store to profile opening it")
	}
	if !rssSupported {
		t.Fatal("no RSS instrument on this platform; this measurement has nothing to report")
	}

	for _, arm := range arenaOpenArms {
		t.Run(arm.name, func(t *testing.T) {
			// Settled before the arm, so what is reported is the rise this open
			// caused rather than whatever the previous arm has not yet given back.
			runtime.GC()
			runtime.GC()
			base, _ := settledRSS(nil)

			sampler := startOpenPeak(200 * time.Microsecond)
			started := time.Now()
			g, err := graphene.OpenWithOptions(arenaDir, arm.opts)
			if err != nil {
				sampler.stop()
				t.Fatalf("Open: %v", err)
			}
			wall := time.Since(started)
			peak := sampler.stop()

			settled, _ := settledRSS(g)
			est, ok := g.EstimateResident()
			if !ok {
				t.Fatal("no estimate from a disk store")
			}
			st, _ := g.StorageStats()

			t.Logf("%s: open in %v", arm.name, wall)
			t.Logf("  anon: settled %.1f MiB, peak %.1f MiB (rise over base %.1f MiB), transient above settled %.1f MiB",
				mib(settled.Anon), mib(peak.Anon),
				mib(int64(settled.Anon)-int64(base.Anon)), mib(int64(peak.Anon)-int64(settled.Anon)))
			t.Logf("  file: settled %.1f MiB, peak %.1f MiB", mib(settled.File), mib(peak.File))
			t.Logf("  estimate: %s", ceilingTerms(g))
			t.Logf("  records are %.1f%% of the modelled heap", 100*float64(est.RecordArrays)/float64(est.Total))
			t.Logf("  %d nodes, %d edges, image %s, index %s, adjacency %s",
				st.CSRNodes, st.CSREdges, st.ImageMode, st.IndexMode, st.Adjacency)

			if arenaProfile != "" {
				path := fmt.Sprintf("%s.%s", arenaProfile, arm.name)
				writeHeapProfile(t, path, g)
				t.Logf("  inuse_space profile: %s", path)
			}
			if err := g.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// writeHeapProfile dumps inuse_space with the store still reachable, which is the
// whole point: a profile taken after Close would show what the open freed rather
// than what it holds.
func writeHeapProfile(t *testing.T, path string, keep any) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create profile: %v", err)
	}
	defer func() { _ = f.Close() }()
	runtime.GC()
	if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	runtime.KeepAlive(keep)
}

func mib[T int64 | uint64](b T) float64 { return float64(b) / (1 << 20) }

// openPeakSampler tracks the largest RSS sample seen while an open runs.
type openPeakSampler struct {
	peak rssSample
	done chan struct{}
	quit chan struct{}
}

func startOpenPeak(every time.Duration) *openPeakSampler {
	s := &openPeakSampler{done: make(chan struct{}), quit: make(chan struct{})}
	go func() {
		defer close(s.done)
		for {
			select {
			case <-s.quit:
				return
			default:
			}
			r := readRSS()
			if r.Anon > s.peak.Anon {
				s.peak.Anon = r.Anon
			}
			if r.File > s.peak.File {
				s.peak.File = r.File
			}
			time.Sleep(every)
		}
	}()
	return s
}

func (s *openPeakSampler) stop() rssSample {
	close(s.quit)
	<-s.done
	return s.peak
}
