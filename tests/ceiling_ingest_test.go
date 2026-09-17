// The bulk-ingest arm: what it costs to fill an empty store, and what it writes
// to do it.
//
// The acceptance next door rebuilds a layer inside a store that already exists.
// That is the consumer's sequence and it is not the question this programme
// opened with, which was "what if I bulk-ingest ten million nodes?". A rebuild
// starts from an image, so the delta it accumulates is measured against
// something; an ingest starts from nothing, and every structure it holds is one
// it created. The two arms hold different things in different proportions, and a
// figure taken on one does not transfer to the other.
//
// # Why this arm counts bytes written as well as bytes held
//
// A bounded ingest compacts every MaxDeltaBytes worth of records, and every
// compaction rewrites the whole image. So the bytes an ingest writes go as
// N^2*b / (2*chunk) -- quadratic in the data, linear in the reciprocal of the
// bound. Tightening the bound to hold memory therefore loosens nothing: it makes
// the rewrite worse, exactly in proportion. At the shape this programme targets
// the arithmetic says ~16 TB at a 32 MiB bound against ~3.9 TB at 128 MiB, and
// that is arithmetic in a plan rather than a measurement.
//
// This arm makes it a measurement. MetricCompaction already carries the image
// size at each compaction and MetricCommit the framed log bytes at each write,
// so a sink that sums them reports what the ingest actually wrote, and dividing
// by what it ended up storing gives the amplification directly. A bound chosen
// for memory can then be costed in write volume rather than defended in prose --
// which is the whole of the case for Phase 5, and the reason that phase should
// not be built on this document's extrapolations.
//
// # Running it
//
//	# small, self-contained, no fixture
//	GRAPHENE_CEILING_MIB=2048 GRAPHENE_RSS_NODES=200000 \
//	    go test ./tests/ -tags=stress -count=1 -run TestCeiling_BulkIngest -v
//
//	# the target shape, bounded, with a reopen after every interim compaction
//	GRAPHENE_CEILING_MIB=2048 GRAPHENE_CEILING_INGEST_DIR=/var/tmp/graphene-ingest \
//	    GRAPHENE_RSS_NODES=1000000 GRAPHENE_RSS_BLOB=3200 \
//	    GRAPHENE_CEILING_DELTA_MIB=32 GRAPHENE_CEILING_REOPEN=1 \
//	    go test ./tests/ -tags=stress -count=1 -run TestCeiling_BulkIngest -v
//
// -count=1 is not decoration here either, and the header of ceiling_test.go says
// what leaving it off silently does to a sweep.
//
// There is no fixture and no separate build invocation, because the ingest *is*
// the build. That is the one way this arm is cheaper than the acceptance: it
// measures the thing the other test had to arrange to happen outside its own
// ceiling.

//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// ceilingIngestDir names where the store is built.
//
// Required above ceilingSelfBuildMax, and deliberately not GRAPHENE_RSS_DIR: that
// variable names a *fixture*, guarded by a shape marker and reused across
// processes, and an ingest arm that wrote into it would leave the next run a
// store the marker still claimed was pristine. This directory is consumed by the
// run that creates it.
//
// It is required rather than defaulted because the system temp directory is the
// wrong volume for tens of gigabytes on most machines, and discovering that at
// 90% of a two-hour ingest is not a failure mode worth having.
var ceilingIngestDir = os.Getenv("GRAPHENE_CEILING_INGEST_DIR")

var (
	// ceilingIngestBudgetMiB sets Options.MemoryBudget for the run, in MiB.
	//
	// The budget is a refusal, not a bound: it gates Open and Compact and
	// nothing in between, and at a compaction it refuses the operation that
	// would have relieved the pressure. So an ingest arm with a budget set and a
	// delta bound set is the configuration where the two can contradict each
	// other -- the policy says compact, the budget says no -- and the run either
	// completes having quietly stopped compacting or fails in CompactIfDue with
	// ErrMemoryBudget. Both are results worth being able to produce on purpose.
	ceilingIngestBudgetMiB = envIntDefault("GRAPHENE_CEILING_BUDGET_MIB", 0)

	// ceilingIngestDiscover asks the engine to find its own budget instead.
	//
	// This is the arm that exercises the discovery from item 1d against a
	// ceiling the harness applied itself, so the figure the engine reads and the
	// figure the test claims come from the same kernel object. Ignored when a
	// budget is given, which is Options.DiscoverMemoryBudget's own rule.
	ceilingIngestDiscover = os.Getenv("GRAPHENE_CEILING_DISCOVER") == "1"
)

// ceilingIngestChunkMax is the coarsest the compaction check ever gets, and the
// figure the rebuild arm next door uses flat.
const ceilingIngestChunkMax = 50_000

// ceilingIngestChunkMin is the finest. Below this the per-chunk progress line
// and the CompactIfDue call start to be a measurable share of the work, and a
// bound held to within a hair is not worth an instrument that changes the
// figures it reports.
const ceilingIngestChunkMin = 1_000

// ceilingIngestChunk is how many nodes are written between compaction checks.
//
// Not a constant, and this is the one place the ingest arm had to stop copying
// the rebuild arm. A flat 50,000 is right at the 256 B records the rest of this
// suite runs at -- 12.8 MiB of delta between checks, comfortably inside a 32 MiB
// bound -- and wrong by a factor of five at the 3.2 KB records this programme
// actually targets, where the same chunk accumulates 160 MiB before anyone asks
// whether the bound was passed. An arm that overshoots its bound fivefold
// reports the memory of a 160 MiB delta under the name of a 32 MiB one, and the
// sweep it belongs to would conclude that bounding the delta does not work.
//
// So the chunk is sized in bytes and converted back to nodes: a quarter of the
// bound, so the delta is at most about 25% past the figure when the question is
// asked. Records carry more than their blob -- labels, the record header, the
// property entries -- so the real overshoot is a little larger than a quarter,
// in the direction of asking more often rather than less.
//
// With GRAPHENE_RSS_BLOB_DIST set the mean blob is about 94 times rssBlob and
// this underestimates badly. That arm is a small-N probe of one question and
// does not run under a delta bound, so it is left alone rather than given a
// second approximation.
func ceilingIngestChunk() int {
	if ceilingDeltaMiB <= 0 || rssBlob <= 0 {
		return ceilingIngestChunkMax
	}
	perCheck := (int64(ceilingDeltaMiB) << 20) / 4 / int64(rssBlob)
	switch {
	case perCheck < ceilingIngestChunkMin:
		return ceilingIngestChunkMin
	case perCheck > ceilingIngestChunkMax:
		return ceilingIngestChunkMax
	default:
		return int(perCheck)
	}
}

// ingestCounter sums what the engine wrote, by the kinds that write.
//
// Image and Log are kept apart because they scale differently and only one of
// them is the problem: the log is linear in the data and is truncated at every
// compaction, while the image is rewritten whole every time, so at any size that
// matters Image is the term and Log is the rounding error. Reporting the sum
// alone would hide which of those two a change had moved.
//
// Compactions is beside them because Image divided by it is the mean image size
// over the ingest, which is the figure the quadratic is actually made of: a
// bound that halves the chunk doubles the count and leaves the mean roughly
// alone.
//
// The figures are a separate value type from the sink that maintains them, so
// that a reading can be passed around and printed without carrying the lock with
// it. A snapshot of a counter is data; only the accumulator needs the mutex.
type ingestWrites struct {
	Image       int64 // bytes of graphene.csr written, summed over compactions
	Log         int64 // framed WAL bytes, summed over commits
	Commits     int64
	Compactions int64
}

// Total is what the device saw, which is the figure amplification is taken
// against.
func (w ingestWrites) Total() int64 { return w.Image + w.Log }

// ingestCounter is the sink. A mutex rather than atomics: store.Metrics promises
// a sink is called from whichever goroutine did the work and several at once,
// and four counters moved together under one lock is both simpler and what makes
// a snapshot consistent.
type ingestCounter struct {
	mu sync.Mutex
	w  ingestWrites
}

func (c *ingestCounter) Record(m store.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch m.Kind {
	case store.MetricCommit:
		// Counted even when Err is set. A commit that failed still framed its
		// bytes and still put them in front of the disk, and a write-volume
		// figure that only counts successes understates what the device saw.
		c.w.Log += m.Bytes
		c.w.Commits++
	case store.MetricCompaction:
		c.w.Image += m.Bytes
		c.w.Compactions++
	}
}

func (c *ingestCounter) snapshot() ingestWrites {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.w
}

// TestCeiling_BulkIngestFitsUnderTheLimit fills an empty store under a ceiling.
//
// The assertions are the same two the acceptance makes, for the same reason: the
// sequence completed, and the store afterwards holds what it should. Everything
// else is reported. See ceiling_test.go's header for why asserting peak < ceiling
// would prove nothing.
func TestCeiling_BulkIngestFitsUnderTheLimit(t *testing.T) {
	if ceilingMiB <= 0 {
		t.Skip("set GRAPHENE_CEILING_MIB to the ceiling this run is claimed to fit under")
	}
	if !rssSupported {
		// Not a skip, for the reason the acceptance gives: a pass from a platform
		// that could not report what it passed under reads as the programme's
		// acceptance having been met.
		t.Fatal("no RSS instrument on this platform; a ceiling run here could not report what it held")
	}
	if ceilingIngestDir == "" && rssNodes > ceilingSelfBuildMax {
		t.Fatalf("GRAPHENE_CEILING_INGEST_DIR is unset and GRAPHENE_RSS_NODES is %d: an ingest of "+
			"that size into the system temp directory is the wrong volume on most machines, and "+
			"finding out at the end of a long run is not a useful failure. Name a directory",
			rssNodes)
	}

	want := uint64(ceilingMiB) << 20
	if ceilingSelfApplies {
		if err := ceilingApply(want); err != nil {
			t.Fatalf("could not apply a %d MiB ceiling: %v\n\n%s", ceilingMiB, err, ceilingHowTo)
		}
	}
	got, err := ceilingRead()
	if err != nil {
		t.Fatalf("no memory ceiling is in force, so this run would prove nothing: %v\n\n%s", err, ceilingHowTo)
	}
	if got.Bytes > want {
		t.Fatalf("the ceiling in force is %.0f MiB (%s), looser than the %d MiB this run claims to prove",
			float64(got.Bytes)/bytesPerMiB, got.Source, ceilingMiB)
	}
	t.Logf("ceiling %.0f MiB in force, from %s (swap allowance: %s)",
		float64(got.Bytes)/bytesPerMiB, got.Source, orUnknown(got.SwapMax))

	dir := ceilingIngestTarget(t)

	// One sink for the whole run, outside the handle, because the handle is
	// replaced on every reopen and a counter that lived on it would reset each
	// time -- silently reporting the last window's writes as the whole ingest's.
	counter := &ingestCounter{}
	opts := disk.Options{
		Metrics:              store.MetricsFunc(counter.Record),
		MemoryBudget:         int64(ceilingIngestBudgetMiB) << 20,
		DiscoverMemoryBudget: ceilingIngestDiscover,
	}

	var (
		g      *graphene.Graph
		phases []ceilingPhase
	)
	const sampleEvery = 25 * time.Millisecond

	run := func(name string, fn func() string) {
		var note string
		start := time.Now()
		peaks := samplePeaksEvery(sampleEvery, func() { note = fn() })
		wall := time.Since(start)
		settled, _ := settledRSS(g)
		phases = append(phases, ceilingPhase{
			Name: name, Note: note, Wall: wall,
			Settled: settled, Peak: peaks.Total, PeakAnon: peaks.Anon, Samples: peaks.Samples,
		})
		ceilingProgress("phase %s done in %s: settled %.1f MiB anon / %.1f MiB total, "+
			"peak %.1f MiB total / %.1f MiB anon (%s)", name, wall.Round(time.Millisecond),
			float64(settled.Anon)/bytesPerMiB, float64(settled.Total)/bytesPerMiB,
			float64(peaks.Total.Total)/bytesPerMiB, float64(peaks.Anon.Anon)/bytesPerMiB, note)
		ceilingProgress("phase %s terms: %s", name, ceilingTerms(g))
	}

	ceilingProgress("ingest into %s: %d nodes, %d B blob, delta bound %d MiB, reopen %v",
		dir, rssNodes, rssBlob, ceilingDeltaMiB, ceilingReopen)

	run("create", func() string {
		var err error
		if g, err = graphene.OpenWithOptions(dir, opts); err != nil {
			t.Fatalf("OpenWithOptions: %v", err)
		}
		// Before any indexing, which is rssDeclare's own precondition and the
		// reason every fixture builder calls it immediately after the open.
		if err := rssDeclare(g); err != nil {
			t.Fatalf("declare: %v", err)
		}
		return holdingNote(g)
	})
	t.Cleanup(func() {
		if g != nil {
			g.Close()
		}
	})
	if st, ok := g.StorageStats(); ok && (ceilingIngestBudgetMiB > 0 || ceilingIngestDiscover) {
		// What is held, not what was asked for. A discovery that found nothing
		// leaves the budget at zero, which is documented as unlimited -- so a run
		// that believed it was budgeted and was not would otherwise report a pass
		// under a configuration nobody chose.
		// An empty source is not an unknown one: StorageStats documents that a
		// budget with no source is one the caller stated, and only a discovered
		// figure names an instrument. Printing "unknown" for the configured case
		// would invent a doubt the contract does not have.
		from := st.MemoryBudgetSource
		if from == "" {
			from = "the caller"
		}
		t.Logf("memory budget %.0f MiB in force, from %s",
			float64(st.MemoryBudgetBytes)/bytesPerMiB, from)
		if st.MemoryBudgetBytes == 0 {
			t.Fatalf("a memory budget was asked for (%d MiB, discover=%v) and none is in force",
				ceilingIngestBudgetMiB, ceilingIngestDiscover)
		}
	}

	var policy store.CompactionPolicy
	if ceilingDeltaMiB > 0 {
		// The byte rule alone. DefaultCompactionPolicy's record and ratio rules
		// would both fire before it at every size this runs at, and an arm whose
		// compactions were triggered by a rule it is not measuring reports the
		// wrong bound's figures under the right bound's name.
		policy.MaxDeltaBytes = int64(ceilingDeltaMiB) << 20
	}
	interim, reopened := 0, 0
	dueFor := ""
	maybeCompact := func() {
		if ceilingDeltaMiB <= 0 {
			return
		}
		did, why, err := g.CompactIfDue(policy)
		if err != nil {
			t.Fatalf("CompactIfDue: %v", err)
		}
		if !did {
			return
		}
		interim++
		dueFor = why
		if !ceilingReopen {
			return
		}
		// Close then open, not a second handle: the store takes an exclusive
		// lock, and the point of the exercise is to stop holding what the first
		// handle holds. See ceilingReopen for why that is the half of the bound
		// a compaction on its own does not give back.
		if err := g.Close(); err != nil {
			t.Fatalf("close for reopen after interim compaction %d: %v", interim, err)
		}
		next, err := graphene.OpenWithOptions(dir, opts)
		if err != nil {
			t.Fatalf("reopen after interim compaction %d: %v", interim, err)
		}
		g = next
		reopened++
	}

	chunk := ceilingIngestChunk()
	ceilingProgress("compaction checked every %d nodes (%.1f MiB of blob against a %d MiB bound)",
		chunk, float64(int64(chunk)*int64(rssBlob))/bytesPerMiB, ceilingDeltaMiB)

	run("ingest", func() string {
		written := 0
		for base := 0; base < rssNodes; base += chunk {
			n := chunk
			if remaining := rssNodes - base; remaining < n {
				n = remaining
			}
			written += len(rssWriteNodes(t, g, base, n, rssBlob))
			maybeCompact()

			now := readRSS()
			c := counter.snapshot()
			ceilingProgress("ingest: %d/%d written, %d interim compactions, "+
				"%.1f MiB anon / %.1f MiB total, %.1f GiB written (%.1f image + %.1f log)",
				written, rssNodes, interim,
				float64(now.Anon)/bytesPerMiB, float64(now.Total)/bytesPerMiB,
				float64(c.Total())/bytesPerGiB,
				float64(c.Image)/bytesPerGiB, float64(c.Log)/bytesPerGiB)
			ceilingProgress("ingest: %d written, terms: %s", written, ceilingTerms(g))
		}
		if written != rssNodes {
			t.Fatalf("ingested %d nodes, want %d", written, rssNodes)
		}
		if ceilingDeltaMiB <= 0 {
			return fmt.Sprintf("%d nodes, delta unbounded", written)
		}
		// Both counts even though they are equal by construction: printing the
		// pair is how a reopen that silently did not happen would show.
		return fmt.Sprintf("%d nodes, %d interim compactions and %d reopens under a %d MiB "+
			"delta bound (%s)", written, interim, reopened, ceilingDeltaMiB, orUnknown(dueFor))
	})

	beforeCompact, _ := g.StorageStats()
	run("compact", func() string {
		// Every arm pays this one, including the unbounded arm that has taken no
		// other: a store left holding its whole delta is not a store anyone
		// ships, and an ingest measured without it is measured half-finished.
		if err := g.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		return holdingNote(g)
	})

	run("reopen", func() string {
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		var err error
		if g, err = graphene.OpenWithOptions(dir, opts); err != nil {
			t.Fatalf("reopen: %v", err)
		}
		counts, err := g.CountNodesByType()
		if err != nil {
			t.Fatalf("CountNodesByType: %v", err)
		}
		var total uint64
		for _, n := range counts {
			total += n
		}
		if total != uint64(rssNodes) {
			t.Fatalf("reopened store holds %d nodes, want %d", total, rssNodes)
		}
		return fmt.Sprintf("%d nodes, %s", total, holdingNote(g))
	})

	// A row resolved after the reopen, because a count is satisfied by a store
	// that kept its records and lost its index, and the index is the term this
	// whole programme is about. One row is enough: the question is whether the
	// structure survived the ingest, not how fast it answers.
	probe := rssNodes / 2
	if ids, err := g.GraphStore.NodesByProperty("digest", rssProps(probe)["digest"]); err != nil {
		t.Fatalf("NodesByProperty after ingest: %v", err)
	} else if len(ids) != 1 {
		t.Fatalf("the digest of node %d resolves to %d ids after the ingest, want 1", probe, len(ids))
	}

	afterCompact, _ := g.StorageStats()
	onDisk := rssStoreBytes(dir)
	ceilingIngestWriteReport(t, counter.snapshot(), onDisk)
	ceilingReport(t, dir, float64(onDisk)/bytesPerMiB, phases, beforeCompact, afterCompact, ceilingTerms(g))
}

const bytesPerGiB = 1 << 30

// ceilingIngestWriteReport prints what the ingest wrote against what it stored.
//
// The amplification is the deliverable. A store that ends at G gigabytes having
// written A*G of them paid A for whatever bound produced that many compactions,
// and A is the figure Phase 5 of docs/PLAN_BOUNDED_INGEST.md has to beat -- a
// one-pass bulk load writes the data once, so its A is 1 plus the index spill
// and nothing else.
//
// The per-node figure is beside it because A on its own does not extrapolate: it
// grows with N at a fixed bound, which is precisely the quadratic. Bytes written
// per node at two sizes is what says whether the growth is the predicted one,
// and is what 0c collects at 1M and 2M.
func ceilingIngestWriteReport(t *testing.T, c ingestWrites, onDisk int64) {
	total := c.Total()
	t.Logf("wrote %.2f GiB to store %.2f GiB: %.2f GiB image over %d compactions, "+
		"%.2f GiB log over %d commits",
		float64(total)/bytesPerGiB, float64(onDisk)/bytesPerGiB,
		float64(c.Image)/bytesPerGiB, c.Compactions,
		float64(c.Log)/bytesPerGiB, c.Commits)
	if onDisk > 0 {
		t.Logf("write amplification %.2fx (image %.2fx, log %.2fx)",
			float64(total)/float64(onDisk),
			float64(c.Image)/float64(onDisk), float64(c.Log)/float64(onDisk))
	}
	if c.Compactions > 0 {
		t.Logf("mean image written per compaction: %.1f MiB",
			float64(c.Image)/float64(c.Compactions)/bytesPerMiB)
	}
	if rssNodes > 0 {
		// The figure to compare across sizes. At a fixed delta bound this grows
		// linearly in N -- which is the quadratic, seen per node.
		t.Logf("bytes written per node: %.0f B over %d nodes at a %d MiB delta bound",
			float64(total)/float64(rssNodes), rssNodes, ceilingDeltaMiB)
	}
}

// ceilingIngestTarget resolves and validates the directory to ingest into.
//
// An unnamed one is a temp directory that dies with the run, which is right for
// the sizes CI runs at. A named one must be absent or empty: building into a
// directory that already holds a store would produce a store that is neither
// this run's nor the last one's, and the residency figures would look exactly
// right either way. That is the same argument rssFixtureDir makes about its
// marker, and the same reason it refuses rather than overwrites.
func ceilingIngestTarget(t *testing.T) string {
	if ceilingIngestDir == "" {
		dir := t.TempDir()
		return dir
	}
	switch entries, err := os.ReadDir(ceilingIngestDir); {
	case err == nil && len(entries) > 0:
		t.Fatalf("GRAPHENE_CEILING_INGEST_DIR %s is not empty: an ingest arm builds its own store "+
			"and will not write into one that already exists. Delete it or name another directory",
			ceilingIngestDir)
	case err != nil && !os.IsNotExist(err):
		t.Fatalf("read dir %s: %v", ceilingIngestDir, err)
	}
	if err := os.MkdirAll(ceilingIngestDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Deliberately not removed at the end. A run this long leaves a store worth
	// looking at afterwards -- and the emptiness check above is what stops the
	// next run trusting it.
	return ceilingIngestDir
}
