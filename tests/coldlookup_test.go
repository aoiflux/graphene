// Cold lookup latency: what a read costs when the pages it needs are not in
// memory.
//
// Every figure this repository records for a read is warm. The store is opened,
// the benchmark loop touches the same structures a million times, and the
// kernel's page cache holds the whole image by the second iteration. That is the
// right shape for comparing two implementations of the same operation, and it is
// the wrong shape for four decisions this engine has already taken:
//
//   - the residual probe is 2.8x slower warm than the sequential walk it
//     replaced, and was taken anyway because it touches 6.6x less of the image;
//   - the batch property join is 1.17x slower on unsorted values, and was taken
//     because it resolves many values in one pass over the base;
//   - the whole bounded-batch API exists to hold less at once rather than to be
//     faster;
//   - a mapped point lookup is roughly twice the cost of a resident one.
//
// All four trade warm CPU for memory or for bytes touched, and all four were
// argued on the belief that the cold case leans the other way -- that a read
// touching less of the image wins when the image is not resident, by enough to
// pay for the warm loss. The belief is reasonable and it has never been
// measured. The deployment this release targets is precisely the one where the
// page cache is under pressure, which is where it stops being academic.
//
// # Why it was declined before, and what changed
//
// Six statements across the tree decline this measurement with the same
// sentence: there is no way to drop the page cache from a Go test on Windows.
// The Go-test framing is refuted by the ceiling harness next door, which reaches
// kernel32 through syscall.NewLazyDLL and drives subprocess arms with
// machine-readable stdout -- and it does so without leaving the zero-dependency
// invariant, since go.mod is 44 bytes and a test asserts it.
//
// What has not changed is the instrument, and this is the honest part. Linux can
// evict a file's clean pages outright. Windows cannot, from user mode, without
// privilege: trimming a working set moves pages to the standby list, where the
// next touch is a soft fault and not a disk read. So this harness does not
// report one number called "cold". It reports what each platform actually
// achieved, in the column heading, and the two are not comparable:
//
//	linux    evicted   posix_fadvise(POSIX_FADV_DONTNEED) -- genuinely cold
//	windows  trimmed   standby-warm; a lower bound on the cold cost
//
// A number labelled "cold" that is really a soft fault is worse than no number,
// because it would be used and then retracted. The asymmetry is the deliverable.
//
// # Shape of a run
//
// One eviction and one fresh handle per operation, so no operation warms
// another, and then two passes over the same sample of ids: the first touches
// each id's pages for the first time, the second finds them resident. The ratio
// between the two is the figure, and the absolute numbers are diagnostics --
// they belong to this machine's disk.
//
//	# reuses the ceiling fixture; GRAPHENE_RSS_DIR must match GRAPHENE_RSS_NODES
//	GRAPHENE_RSS_DIR=/var/tmp/graphene-fixture GRAPHENE_RSS_NODES=1400000 \
//	    go test ./tests/ -tags=stress -count=1 -run TestColdLookup -v
//
// -count=1, for the reason ceiling_test.go's header gives at length: the arms
// differ only in the environment, and a cached replay of the first one looks
// exactly like a measurement of the rest.
//
// It asserts almost nothing, following disk/mmap_spike_test.go: a latency
// threshold here would encode this machine's disk into the test suite. What it
// does assert is that each operation returned something -- a pass that resolved
// nothing would report a very good cold figure for doing no work -- and that the
// eviction step reported which of the two things it did.

//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// coldSamples is how many distinct ids each pass touches.
//
// Each one is a separate first touch, so this is the sample size of the cold
// figure and not a repeat count. It is sparse relative to the store on purpose:
// consecutive ids share pages in both the record arena and the index, so a dense
// sample would touch each page once and find it warm for every id after the
// first -- which is the warm figure wearing the cold figure's label.
var coldSamples = envIntDefault("GRAPHENE_COLD_SAMPLES", 2_000)

const (
	// coldWarmFloor is how long the warm side must run before it is divided, and
	// coldWarmMaxReps stops a very cheap operation from repeating forever.
	coldWarmFloor   = 200 * time.Millisecond
	coldWarmMaxReps = 10_000
)

// coldOp is one measured operation. Each gets its own eviction and its own
// handle.
type coldOp struct {
	name string
	// run performs the operation over the whole sample once and returns how many
	// results it produced, so a pass that silently resolved nothing is visible.
	run func(t *testing.T, g *graphene.Graph, s coldSample) int
}

// coldSample is the sample of ids, with the indexed value each carries, taken
// before the store is evicted so that taking it does not warm the measured pass.
type coldSample struct {
	ids     []store.NodeID
	digests [][]byte
	// shuffled is ids in a fixed pseudo-random order, so the batch join can be
	// measured both ways. It holds the same ids and the same count: the two
	// projection arms differ in the order values reach the join and in nothing
	// else, which is what makes their ratio attributable to the ordering.
	shuffled []store.NodeID
}

// coldShuffle returns a copy of ids in a deterministic pseudo-random order.
//
// Deterministic because the ordering is the independent variable: a run that
// shuffled differently each time would report a different arm each time, and
// the difference being measured here is small enough that that would swamp it.
//
// It carries its own generator rather than using math/rand for one reason --
// math/rand's sequence for a given seed is explicitly not guaranteed across Go
// releases, and a figure in docs/benchmarks.md should be reproducible by
// someone on a later toolchain. splitmix64 is six lines and fixed forever.
func coldShuffle(ids []store.NodeID) []store.NodeID {
	out := make([]store.NodeID, len(ids))
	copy(out, ids)
	state := uint64(0x9E3779B97F4A7C15)
	next := func() uint64 {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
	for i := len(out) - 1; i > 0; i-- {
		j := int(next() % uint64(i+1))
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func TestColdLookup_WhatAReadCostsWhenThePagesAreNotResident(t *testing.T) {
	if coldEvictMethod == "" {
		t.Skipf("no cache-eviction instrument on this platform: %s", coldEvictHowTo)
	}

	dir := rssFixtureDir(t, rssNodes, rssBlob)
	csr := filepath.Join(dir, "graphene.csr")
	st, err := os.Stat(csr)
	if err != nil {
		t.Fatalf("stat image: %v", err)
	}
	t.Logf("image %.1f MiB, %d samples, eviction: %s",
		float64(st.Size())/(1<<20), coldSamples, coldEvictMethod)

	sample := coldTakeSample(t, dir)

	ops := []coldOp{
		{
			// The mapped point lookup, whose warm cost roughly doubled when the
			// image stopped being decoded into the heap. One record, one page, no
			// index.
			name: "record point read",
			run: func(t *testing.T, g *graphene.Graph, s coldSample) int {
				n := 0
				for _, id := range s.ids {
					node, err := g.GetNode(id)
					if err != nil {
						t.Fatalf("GetNode(%d): %v", id, err)
					}
					if node != nil {
						n++
					}
				}
				return n
			},
		},
		{
			// The mapped property index: a binary search into GPIX and a read of
			// the posting it names. This is the path the residual probe's cost
			// model is written about.
			name: "index point lookup",
			run: func(t *testing.T, g *graphene.Graph, s coldSample) int {
				n := 0
				for _, d := range s.digests {
					ids, err := g.NodesByProperty("digest", d)
					if err != nil {
						t.Fatalf("NodesByProperty: %v", err)
					}
					n += len(ids)
				}
				return n
			},
		},
		{
			// The batch join: many ids, several keys, one pass over the base.
			// Warm it costs 1.17x the per-id form on unsorted values; if the
			// single pass is worth anything it is worth it here.
			//
			// ForEachNodeID yields ascending, so this arm supplies exactly the
			// ordering the documentation advises. The arm below supplies the same
			// ids shuffled, and the pair is the whole point: the advice to sort
			// has only ever rested on warm figures.
			name: "projection batch (ascending)",
			run: func(t *testing.T, g *graphene.Graph, s coldSample) int {
				return coldProject(t, g, s.ids)
			},
		},
		{
			// The same join over the same ids in a fixed shuffled order.
			//
			// # What this arm can and cannot settle, on this platform
			//
			// Ascending order buys two different things, and they are not equally
			// visible here. It makes the join's pass over the base monotonic, so
			// each posting is found by advancing a cursor rather than by seeking
			// back -- that is CPU and page locality, and it shows up under any
			// instrument. And it lets the kernel's readahead predict the next
			// page, which is worth far more when the page must come from a disk
			// than when it is on the standby list a soft fault away.
			//
			// Windows measures the first effect honestly and largely removes the
			// second, because "trimmed" is exactly the state in which a mispredicted
			// readahead costs almost nothing. So a small difference here is a lower
			// bound on the difference, and the linux arm -- which evicts for real --
			// is the one that can size the readahead half. Reading a null result on
			// this platform as "ordering does not matter cold" would be reading the
			// instrument.
			name: "projection batch (shuffled)",
			run: func(t *testing.T, g *graphene.Graph, s coldSample) int {
				return coldProject(t, g, s.shuffled)
			},
		},
		{
			// The bounded batch read, which exists to hold less at once rather
			// than to be quicker. Cold is where "fewer bytes touched" and "less
			// held" stop being the same claim.
			//
			// The bound is small on purpose. At the fixture's 512-byte blobs a
			// megabyte would swallow the whole sample in one batch, and the arm
			// would measure a batch read that never had to stop -- which is the
			// unbounded read under the bounded read's name. 64 KiB gives the
			// sample a dozen or more batches, so the per-batch cost is in the
			// figure rather than amortised out of it.
			name: "bounded batch read",
			run: func(t *testing.T, g *graphene.Graph, s coldSample) int {
				n, rest := 0, s.ids
				for len(rest) > 0 {
					found, _, next, err := g.GetNodesBounded(rest, 64<<10)
					if err != nil {
						t.Fatalf("GetNodesBounded: %v", err)
					}
					if len(found) == 0 && len(next) == len(rest) {
						t.Fatalf("GetNodesBounded made no progress over %d ids", len(rest))
					}
					n += len(found)
					rest = next
				}
				return n
			},
		},
	}

	t.Logf("%-29s %14s %14s %8s %10s", "operation", coldEvictMethod, "resident", "ratio", "results")
	noisy := 0
	for _, op := range ops {
		method, err := coldEvictBeforeOpen(csr)
		if err != nil {
			t.Fatalf("%s: evict before open: %v", op.name, err)
		}

		g, err := graphene.OpenReadOnly(dir)
		if err != nil {
			t.Fatalf("%s: OpenReadOnly: %v", op.name, err)
		}

		if m, err := coldEvictWhileOpen(g); err != nil {
			g.Close()
			t.Fatalf("%s: evict while open: %v", op.name, err)
		} else if m != "" {
			method = m
		}
		if method == "" {
			g.Close()
			t.Fatalf("%s: neither eviction hook reported what it did, so nothing "+
				"below could be labelled", op.name)
		}

		// The cold pass runs exactly once, because there is only one first
		// touch. Everything after it is warm by definition.
		start := time.Now()
		coldN := op.run(t, g, sample)
		coldD := time.Since(start)

		// The warm pass repeats until it has run long enough to be worth
		// dividing. A single warm pass is not measurable here: windows'
		// monotonic clock ticks at around a millisecond, and the first version of
		// this harness reported "0.00 us" and a ratio of +Inf for three of the
		// four operations -- a property of the clock presented as a property of
		// the store. The cold pass cannot be repeated, so the asymmetry is in the
		// method rather than a mistake in it, and it is safe: repeating the warm
		// side can only make the warm figure smaller and the ratio larger, so an
		// operation that looks unaffected by the cache under this method really
		// is.
		warmStart := time.Now()
		warmN, reps := 0, 0
		for {
			warmN = op.run(t, g, sample)
			reps++
			if time.Since(warmStart) >= coldWarmFloor || reps >= coldWarmMaxReps {
				break
			}
		}
		warmD := time.Since(warmStart) / time.Duration(reps)

		if err := g.Close(); err != nil {
			t.Fatalf("%s: Close: %v", op.name, err)
		}

		if coldN == 0 {
			t.Errorf("%s resolved nothing, so its figures are the cost of doing "+
				"no work", op.name)
		}
		if coldN != warmN {
			t.Errorf("%s resolved %d on the first pass and %d on a later one: the "+
				"two did not do the same work, so the ratio is not a ratio",
				op.name, coldN, warmN)
		}
		// The cold side has no repeat count to hide behind, and a pass that ran
		// for fewer than a few clock ticks is reporting the clock rather than the
		// store. That is not a failure -- this harness reports, it does not
		// assert an acceptance, and a red tick over a fixture someone chose to be
		// small helps nobody. It is a mark on the row, because the one outcome
		// that must not be possible is a reader taking a noisy figure for a solid
		// one.
		mark := "  "
		if coldD < coldWarmFloor/10 {
			mark = " !"
			noisy++
		}

		per := func(d time.Duration) string {
			return fmt.Sprintf("%.2f us", float64(d.Nanoseconds())/float64(len(sample.ids))/1000)
		}
		t.Logf("%-29s %14s %14s %7.2fx %10d %s (warm x%d, cold %s)",
			op.name, per(coldD), per(warmD), float64(coldD)/float64(warmD),
			coldN, mark, reps, coldD.Round(time.Microsecond))
	}
	if noisy > 0 {
		t.Logf("! %d row(s) ran their cold pass in under %s, which is close enough "+
			"to this platform's timer resolution that the figure is the clock's. "+
			"Raise GRAPHENE_COLD_SAMPLES (now %d) or point GRAPHENE_RSS_DIR at a "+
			"larger fixture; the marked rows are not evidence of anything.",
			noisy, coldWarmFloor/10, coldSamples)
	}

	t.Logf("what %q means on this platform: %s", coldEvictMethod, coldEvictNote)
}

// coldProject runs the batch join over ids in the order given and returns how
// many values came back, so the two projection arms are the same code reached
// with two orderings rather than two bodies that might drift apart.
func coldProject(t *testing.T, g *graphene.Graph, ids []store.NodeID) int {
	got, err := g.GetNodesProjected(ids, []string{"ord0", "ord1"})
	if err != nil {
		t.Fatalf("GetNodesProjected: %v", err)
	}
	n := 0
	for _, p := range got {
		for _, v := range p.Values {
			n += len(v)
		}
	}
	return n
}

// coldTakeSample picks the ids and reads the value each is indexed under,
// through a handle that is closed before anything is measured.
func coldTakeSample(t *testing.T, dir string) coldSample {
	t.Helper()

	g, err := graphene.OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer g.Close()

	count, err := g.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if count == 0 {
		t.Fatal("the fixture holds no nodes")
	}

	stride := int(count) / coldSamples
	if stride < 1 {
		stride = 1
	}

	var s coldSample
	n := 0
	err = g.ForEachNodeID(store.NodeQuery{}, func(id store.NodeID) bool {
		if n%stride == 0 {
			s.ids = append(s.ids, id)
		}
		n++
		return len(s.ids) < coldSamples
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if len(s.ids) == 0 {
		t.Fatal("sampled no ids")
	}

	// The digest each sampled id carries, so the index arm looks up a value that
	// is known to resolve. Read here rather than in the measured pass, for the
	// same reason the sample is taken here.
	got, err := g.GetNodesProjected(s.ids, []string{"digest"})
	if err != nil {
		t.Fatalf("GetNodesProjected: %v", err)
	}
	for _, p := range got {
		if len(p.Values) == 0 || len(p.Values[0]) == 0 {
			continue
		}
		d := make([]byte, len(p.Values[0][0]))
		copy(d, p.Values[0][0])
		s.digests = append(s.digests, d)
	}
	if len(s.digests) == 0 {
		t.Fatal("no sampled id carries an indexed digest: the fixture was built " +
			"with GRAPHENE_RSS_NOINDEX, and four of the five arms measure the index")
	}
	s.shuffled = coldShuffle(s.ids)
	return s
}
