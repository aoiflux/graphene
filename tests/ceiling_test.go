// The memory programme's end-to-end acceptance: the consumer's own sequence,
// run on the machine it was sized for.
//
// Everything else in this suite reports a figure. This reports pass or fail, and
// the question it answers is the one the whole programme exists for -- a 1.2 GiB
// store on a 2 GiB machine, where slower is acceptable and OOM-killed is not.
// The sequence is the consumer's: open, resolve a handful of rows through the
// index, rebuild the derived layer, compact, reopen and check the result is
// still there.
//
// Two design decisions are worth stating, because both are the difference
// between this test meaning something and merely looking as though it does.
//
// The first is that the ceiling is read back, and the run refuses to produce a
// result without one. A CI step whose limit silently failed to apply would run
// this workload unconstrained and go green, and nothing in the output would say
// so -- the same failure mode as an option asked for and not held, which is why
// StorageStats reports what is held rather than what was requested, and why the
// mode matrix in docs/MEMORY_MODEL.md section 8 prints both. A ceiling is an
// asked-for thing too.
//
// The second is that the peak is reported and not asserted. Asserting peak <
// ceiling proves nothing: a process that exceeded its ceiling is dead, so the
// assertion can only ever run when it has already passed. What is asserted is
// that the sequence completed, that the store afterwards holds what it should,
// and that a ceiling at least as tight as the one claimed was in force while it
// happened. The peak, the headroom and -- on linux -- how many times the kernel
// had to reclaim at the limit are diagnostics: they say whether the run passed
// comfortably or by a hair, which the pass alone cannot.
//
// The fixture is built outside the ceiling, by a separate invocation, because a
// build peaks at several times what the finished store costs to open: near
// 7.5 GiB at 1.4M nodes. A run that built its own fixture under a 2 GiB limit
// would be testing the builder, and would fail before it reached the store.
//
//	# once, unconstrained
//	GRAPHENE_CEILING_BUILD=1 GRAPHENE_RSS_DIR=/var/tmp/graphene-fixture \
//	    GRAPHENE_RSS_NODES=1400000 go test ./tests/ -tags=stress -run TestCeilingFixture -v
//
//	# then, under the ceiling -- see ceilingHowTo for the platform's wrapper
//	GRAPHENE_CEILING_MIB=2048 GRAPHENE_RSS_DIR=/var/tmp/graphene-fixture \
//	    GRAPHENE_RSS_NODES=1400000 go test ./tests/ -tags=stress -run TestCeiling_ -v

//go:build stress

package graphene_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// ceilingInfo is a memory ceiling as the operating system reports it, read back
// rather than assumed. Fields a platform cannot supply stay zero or empty;
// Counters says whether the event counters were readable, so an absent counter
// is distinguishable from a counter reading zero.
type ceilingInfo struct {
	Bytes    uint64 // the limit in force, in bytes
	Source   string // where it was read from, named in the report
	SwapMax  string // the swap allowance beside it, verbatim
	Peak     uint64 // high-water mark of the charge the limit is enforced against
	Reclaims uint64 // times allocation was throttled at the limit
	OOMs     uint64 // times the limit could not be satisfied by reclaim
	OOMKills uint64 // processes killed for it
	Counters bool   // whether the three counters above were readable
}

var (
	// ceilingMiB is both the gate and the claim: unset, the run skips; set, the
	// run asserts that a ceiling at least this tight was in force.
	ceilingMiB = envIntDefault("GRAPHENE_CEILING_MIB", 0)

	// ceilingBuild gates the fixture builder, which must run unconstrained.
	ceilingBuild = os.Getenv("GRAPHENE_CEILING_BUILD") == "1"

	// ceilingDeltaMiB bounds the delta during the rebuild, in MiB. Zero rebuilds
	// the whole layer into one delta and compacts once at the end.
	//
	// The bound is not a convenience. A rebuild writes thirteen index entries per
	// node and a resident entry costs about a hundred bytes, so at the shape this
	// programme targets -- 1.4M nodes -- a single delta carries eighteen million
	// entries and is larger than the ceiling on its own, before the image, the
	// records or the blobs are counted. A consumer on a bounded machine therefore
	// has to bound the delta, and CompactionPolicy is how the engine offers that.
	// Leaving this unset runs the monolithic arm, which is the one that shows why.
	ceilingDeltaMiB = envIntDefault("GRAPHENE_CEILING_DELTA_MIB", 0)

	// ceilingReadOnly stops the sequence after the scan.
	//
	// The read-only arm is the consumer's aggregate process, which opens a store
	// and reads ten rows out of it, and it is the half of the acceptance that
	// holds at the full shape: 1,400,000 nodes open and scan in 561 MiB of
	// anonymous memory under a 2 GiB ceiling. The write half does not, by a
	// factor of three, and section 9 of docs/MEMORY_MODEL.md says why. Keeping
	// the two arms separate is what lets the nightly guard the one that holds
	// without either hiding the one that does not or going permanently red over
	// it.
	//
	// It also opens the master rather than a copy, because a reader does not
	// write and there is no reason to duplicate 1.7 GiB to prove it.
	ceilingReadOnly = os.Getenv("GRAPHENE_CEILING_READONLY") == "1"
)

// ceilingRebuildChunk is how often the rebuild stops to ask whether the delta is
// due. Small enough that the answer is acted on before the delta has grown much
// past the bound, large enough that the question costs nothing measurable.
const ceilingRebuildChunk = 50_000

// ceilingSelfBuildMax is the largest fixture this test will build for itself
// rather than demanding a prebuilt one.
//
// Below it the build fits under any ceiling worth testing, and a local smoke run
// should just work. Above it the build's own peak, not the store, is what the
// ceiling would bind, and a failure would be attributed to the wrong thing.
const ceilingSelfBuildMax = 200_000

// ceilingPhase is one step of the sequence and what it cost.
type ceilingPhase struct {
	Name    string
	Note    string
	Wall    time.Duration
	Settled rssSample
	Peak    rssSample
	Samples int
}

// TestCeilingFixture_Build builds the shared fixture and nothing else.
//
// It exists so that the build gets its own invocation in its own process,
// outside whatever limit the run proper is placed under. It measures nothing:
// the figures for this shape are BenchmarkRSS_Open's job, and a build that also
// reported would report its own high-water mark under the name of the store's.
func TestCeilingFixture_Build(t *testing.T) {
	if !ceilingBuild {
		t.Skip("set GRAPHENE_CEILING_BUILD=1 to build the ceiling fixture")
	}
	if rssDir == "" {
		t.Fatal("set GRAPHENE_RSS_DIR: a fixture built into a temp directory dies with this process, " +
			"and the point of building it separately is that the next process reads it")
	}

	start := time.Now()
	dir := rssFixtureDir(t, rssNodes, rssBlob)
	t.Logf("fixture %s: %d nodes, %.1f MiB on disk, built in %s",
		dir, rssNodes, float64(rssStoreBytes(dir))/bytesPerMiB, time.Since(start).Round(time.Second))
}

// ceilingChildEnv carries the child ceiling, in MiB, to the subprocess below.
const ceilingChildEnv = "GRAPHENE_CEILING_CHILD_MIB"

// TestCeiling_TheAppliedCeilingBinds checks that a self-applied ceiling is a
// limit and not a decoration.
//
// It earns its place only on the platform that applies its own. There, the
// ceiling is written and read back by the same process, so a limit the kernel
// accepted and never enforced would pass every check in the acceptance test
// above. Where the ceiling comes from outside, the read-back is already
// independent of this process and there is nothing self-applied to doubt.
//
// The child asks the kernel rather than dying of it. The obvious spelling --
// allocate past the limit and watch the process go -- was tried and is a bad
// instrument on this platform: the limit does bind, but the death takes minutes
// rather than moments, because the runtime's fatal traceback needs memory the
// process no longer has and overflows its own stack trying to print. What
// ceilingProbe does instead is commit memory directly and report the error,
// which is the same syscall the heap grows through and answers the same
// question in a millisecond. It commits once before applying the ceiling too,
// so a machine that is simply out of commit is reported as inconclusive rather
// than as a bound limit.
func TestCeiling_TheAppliedCeilingBinds(t *testing.T) {
	if !ceilingSelfApplies {
		t.Skip("this platform reads its ceiling rather than applying one, so the read-back in " +
			"TestCeiling_ConsumerSequenceFitsUnderTheLimit is already independent of this process")
	}

	// A separate process because the ceiling has to be small enough that four
	// times it is an ordinary allocation, and a process gets one ceiling: applying
	// a small one here would leave the acceptance test running under it.
	const childMiB = 128

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run", "^TestCeilingChild_ProbesItsCeiling$", "-test.v")
	env := append(os.Environ(), ceilingChildEnv+"="+strconv.Itoa(childMiB))
	// The child inherits this process's job, so it can only check that a process
	// in no job reports no ceiling when this process is in none either. That is
	// the shipped order -- this test runs before the acceptance test applies one
	// -- and asking rather than assuming keeps the check honest if that changes.
	if _, err := ceilingRead(); err != nil {
		env = append(env, ceilingChildNoneEnv+"=1")
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	text := string(out)

	switch {
	case strings.Contains(text, ceilingProbeInconclusive):
		t.Skipf("inconclusive: this machine could not commit %d MiB even before the ceiling was "+
			"applied, so a refusal afterwards would prove nothing\n%s", childMiB*4, text)
	case strings.Contains(text, ceilingProbeNotBound):
		t.Fatalf("the child committed %d MiB under its own %d MiB ceiling: the ceiling this harness "+
			"applies does not bind\n%s", childMiB*4, childMiB, text)
	case err != nil:
		t.Fatalf("the child neither confirmed nor denied its ceiling: %v\n%s", err, text)
	case !strings.Contains(text, ceilingProbeBound):
		t.Fatalf("the child exited cleanly without reporting a verdict\n%s", text)
	}
	t.Logf("child confirmed its own %d MiB ceiling refuses a %d MiB commit", childMiB, childMiB*4)
}

// The three verdicts the child prints. Strings rather than exit codes because
// the child is a test binary and the framework owns its exit status.
const (
	ceilingProbeBound        = "CEILING-BOUND"
	ceilingProbeNotBound     = "CEILING-NOT-BOUND"
	ceilingProbeInconclusive = "CEILING-INCONCLUSIVE"
)

// ceilingChildNoneEnv tells the child that it starts in no job, so that it can
// check the other half of the contract: a process under no ceiling must not be
// reported as being under one.
const ceilingChildNoneEnv = "GRAPHENE_CEILING_CHILD_EXPECT_NONE"

// TestCeilingChild_ProbesItsCeiling is the subprocess of the test above. It
// skips unless that test started it, so a plain run of the suite never pays for
// it.
func TestCeilingChild_ProbesItsCeiling(t *testing.T) {
	mib := envIntDefault(ceilingChildEnv, 0)
	if mib <= 0 {
		t.Skip("started only by TestCeiling_TheAppliedCeilingBinds")
	}
	probe := uint64(mib) << 22 // four times the ceiling

	// No ceiling must read as no ceiling. Reporting one here would let the
	// acceptance test run unconstrained and call the result a pass, which is the
	// single failure this whole file is built to prevent.
	if os.Getenv(ceilingChildNoneEnv) == "1" {
		if got, err := ceilingRead(); err == nil {
			fmt.Println(ceilingProbeNotBound, "no ceiling was applied, yet one was read:", got.Bytes, "bytes")
			return
		}
	}

	if err := ceilingProbe(probe); err != nil {
		fmt.Println(ceilingProbeInconclusive, err)
		return
	}
	if err := ceilingApply(uint64(mib) << 20); err != nil {
		fmt.Println(ceilingProbeInconclusive, "apply:", err)
		return
	}
	if err := ceilingProbe(probe); err == nil {
		fmt.Println(ceilingProbeNotBound)
		return
	} else {
		fmt.Println(ceilingProbeBound, err)
	}
}

// TestCeiling_ConsumerSequenceFitsUnderTheLimit is the acceptance.
func TestCeiling_ConsumerSequenceFitsUnderTheLimit(t *testing.T) {
	if ceilingMiB <= 0 {
		t.Skip("set GRAPHENE_CEILING_MIB to the ceiling this run is claimed to fit under")
	}
	if !rssSupported {
		// Not a skip. This test's only output is pass or fail, and a pass from a
		// platform that could not measure what it passed under would be read as
		// the programme's acceptance having been met.
		t.Fatal("no RSS instrument on this platform; a ceiling run here could not report what it held")
	}
	if rssDir == "" && rssNodes > ceilingSelfBuildMax {
		t.Fatalf("GRAPHENE_RSS_DIR is unset and GRAPHENE_RSS_NODES is %d: building a fixture that size "+
			"inside the ceiling peaks at several times what the finished store costs to open, so the run "+
			"would fail in the builder. Build it first with TestCeilingFixture_Build", rssNodes)
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
	if got.Bytes < want {
		t.Logf("note: the ceiling in force is tighter than the %d MiB claimed; the result is stronger, "+
			"not weaker, but the figure to quote is %.0f MiB",
			ceilingMiB, float64(got.Bytes)/bytesPerMiB)
	}

	// A copy, not the master: the sequence writes, deletes and compacts, and a
	// master left rebuilt would hand the next run a different store under the
	// same shape marker.
	dir := rssMutableFixtureDir(t, rssNodes, rssBlob)
	if ceilingReadOnly {
		dir = rssFixtureDir(t, rssNodes, rssBlob)
	}
	diskMiB := float64(rssStoreBytes(dir)) / bytesPerMiB

	var (
		g      *graphene.Graph
		phases []ceilingPhase
	)
	// 25 ms rather than the sampler's default 1 ms: the rebuild phase runs for
	// minutes at the sizes this test is for, and a millisecond ticker there costs
	// hundreds of thousands of readings of the very counters being read.
	const sampleEvery = 25 * time.Millisecond

	run := func(name string, fn func() string) {
		var note string
		start := time.Now()
		peak, samples := samplePeakEvery(sampleEvery, func() { note = fn() })
		wall := time.Since(start)
		settled, _ := settledRSS(g)
		phases = append(phases, ceilingPhase{
			Name: name, Note: note, Wall: wall,
			Settled: settled, Peak: peak, Samples: samples,
		})
	}

	run("open", func() string {
		var err error
		if g, err = graphene.Open(dir); err != nil {
			t.Fatalf("Open: %v", err)
		}
		return holdingNote(g)
	})
	t.Cleanup(func() {
		if g != nil {
			g.Close()
		}
	})

	// The read-only aggregate: ten rows resolved through the all-distinct digest
	// key and read back. Ten rows is the consumer's own shape, and the point of
	// it is that whatever dominates here was paid for opening, not for reading.
	const rows = 10
	run("scan", func() string {
		stride := rssNodes / rows
		found := 0
		for i := 0; i < rows; i++ {
			ids, err := g.GraphStore.NodesByProperty("digest", rssProps(i * stride)["digest"])
			if err != nil {
				t.Fatalf("NodesByProperty: %v", err)
			}
			nodes, _, err := g.GetNodes(ids)
			if err != nil {
				t.Fatalf("GetNodes: %v", err)
			}
			found += len(nodes)
		}
		if found != rows {
			t.Fatalf("scan resolved %d rows, want %d", found, rows)
		}
		return fmt.Sprintf("%d rows", found)
	})

	// Enumerating the live set is part of a rebuild, and is measured apart from
	// it because it is the one step with an obvious unbounded spelling: a query
	// that materialises every identifier before the first delete. ForEachNodeID
	// is the streaming form, and this row is what says whether it is being used.
	var live []store.NodeID
	run("enumerate", func() string {
		query := store.NodeQuery{Types: []store.NodeType{
			store.NodeTypeMicroArtefact, store.NodeTypeEvidenceFile, store.NodeTypeCase,
		}}
		live = make([]store.NodeID, 0, rssNodes)
		if err := g.ForEachNodeID(query, func(id store.NodeID) bool {
			live = append(live, id)
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeID: %v", err)
		}
		if len(live) != rssNodes {
			t.Fatalf("enumerated %d live nodes, want %d", len(live), rssNodes)
		}
		return fmt.Sprintf("%d ids", len(live))
	})

	// One rebuild cycle: every node of the derived layer deleted and the same
	// number written back. The fresh nodes start at rssNodes so the two
	// generations carry different property values and a row resolved afterwards
	// says which one it came from. Not because reuse would collide: a delete
	// purges the entity from the unique index before the compaction, so writing
	// the same values back succeeds. That was checked rather than assumed --
	// this comment first claimed the opposite, and the mutation that reused the
	// values passed.
	if ceilingReadOnly {
		t.Log("read-only arm: the sequence stops after the scan")
		st, _ := g.StorageStats()
		ceilingReport(t, dir, diskMiB, phases, st, st)
		return
	}

	var policy store.CompactionPolicy
	if ceilingDeltaMiB > 0 {
		policy.MaxDeltaBytes = int64(ceilingDeltaMiB) << 20
	}
	interim := 0
	dueFor := ""
	maybeCompact := func() {
		if ceilingDeltaMiB <= 0 {
			return
		}
		did, why, err := g.CompactIfDue(policy)
		if err != nil {
			t.Fatalf("CompactIfDue: %v", err)
		}
		if did {
			interim++
			dueFor = why
		}
	}

	run("rebuild", func() string {
		for i, id := range live {
			if err := g.DeleteNode(id); err != nil {
				t.Fatalf("DeleteNode(%d): %v", id, err)
			}
			if (i+1)%ceilingRebuildChunk == 0 {
				maybeCompact()
			}
		}
		written := 0
		for base := 0; base < rssNodes; base += ceilingRebuildChunk {
			n := ceilingRebuildChunk
			if remaining := rssNodes - base; remaining < n {
				n = remaining
			}
			written += len(rssWriteNodes(t, g, rssNodes+base, n, rssBlob))
			maybeCompact()
		}
		if written != rssNodes {
			t.Fatalf("wrote %d nodes, want %d", written, rssNodes)
		}
		if ceilingDeltaMiB <= 0 {
			return fmt.Sprintf("%d deleted, %d written, delta unbounded", len(live), written)
		}
		return fmt.Sprintf("%d deleted, %d written, %d interim compactions under a %d MiB delta bound (%s)",
			len(live), written, interim, ceilingDeltaMiB, orUnknown(dueFor))
	})
	live = nil

	beforeCompact, _ := g.StorageStats()
	run("compact", func() string {
		if err := g.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		return holdingNote(g)
	})

	// Reopen and count. Without it a run could pass having compacted a store it
	// had quietly emptied, and every residency figure above would still look
	// exactly right.
	run("reopen", func() string {
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		var err error
		if g, err = graphene.Open(dir); err != nil {
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

	afterCompact, _ := g.StorageStats()
	ceilingReport(t, dir, diskMiB, phases, beforeCompact, afterCompact)
}

// ceilingReport prints what the run cost and what it ran under.
//
// Everything here is a diagnostic rather than an assertion, with one exception:
// a cgroup that killed something during the run did not produce a result, so an
// OOM kill fails even though this process obviously survived its own.
func ceilingReport(t *testing.T, dir string, diskMiB float64, phases []ceilingPhase,
	beforeCompact, afterCompact store.StorageStats,
) {
	// Deliberately not a t.Helper: this prints a table over many lines, and a
	// helper would stamp every one of them with the single line that called it.
	t.Logf("fixture %s: %d nodes, %.1f MiB on disk", dir, rssNodes, diskMiB)
	t.Logf("%-10s %9s %9s %9s %9s %7s %9s  %s",
		"phase", "anonMiB", "fileMiB", "rssMiB", "peakMiB", "samples", "wall", "note")
	for _, p := range phases {
		anon, file := "-", "-"
		if p.Settled.Split {
			anon = fmt.Sprintf("%.1f", float64(p.Settled.Anon)/bytesPerMiB)
			file = fmt.Sprintf("%.1f", float64(p.Settled.File)/bytesPerMiB)
		}
		// A phase shorter than the sample interval gets no reading at all, and
		// printing that as 0.0 would say the peak was nothing rather than that
		// nobody looked. The sample count is beside it for the same reason: the
		// sampler returns a floor on the peak, and how many readings went into
		// that floor is what says how good a floor it is.
		peak := "-"
		if p.Samples > 0 {
			peak = fmt.Sprintf("%.1f", float64(p.Peak.Total)/bytesPerMiB)
		}
		t.Logf("%-10s %9s %9s %9.1f %9s %7d %9s  %s",
			p.Name, anon, file,
			float64(p.Settled.Total)/bytesPerMiB, peak, p.Samples,
			p.Wall.Round(time.Millisecond), p.Note)
	}

	t.Logf("delta at compaction: %.1f MiB over %d node entries; after: %.1f MiB, %d entries, "+
		"highest node id %d (%.4f of the space left)",
		float64(beforeCompact.DeltaBytes)/bytesPerMiB, beforeCompact.PropertyNodeEntries,
		float64(afterCompact.DeltaBytes)/bytesPerMiB, afterCompact.PropertyNodeEntries,
		afterCompact.HighestNodeID, afterCompact.NodeIDHeadroom)
	t.Logf("the engine's own estimate after the sequence: %.1f MiB heap, %.1f MiB mapped",
		float64(afterCompact.EstimatedResidentBytes)/bytesPerMiB,
		float64(afterCompact.ImageMappedBytes)/bytesPerMiB)

	// The headroom line. ceilingInfo.Peak is the high-water mark of the charge
	// the limit is enforced against, which is the only peak the ceiling can be
	// differenced from; the process's own VmHWM or PeakWorkingSetSize is reported
	// beside it because the two count different things and a gap between them is
	// information rather than noise.
	final, err := ceilingRead()
	if err != nil {
		t.Fatalf("the ceiling could not be read back after the run: %v", err)
	}
	processPeak := readRSS().Peak
	if final.Peak == 0 {
		t.Logf("peak against the ceiling: not reported by this platform; process peak %.1f MiB "+
			"of a %.0f MiB ceiling", float64(processPeak)/bytesPerMiB, float64(final.Bytes)/bytesPerMiB)
	} else {
		t.Logf("peak against the ceiling: %.1f MiB of %.0f MiB, %.1f MiB of headroom (%.1f%%); "+
			"process peak %.1f MiB",
			float64(final.Peak)/bytesPerMiB, float64(final.Bytes)/bytesPerMiB,
			float64(int64(final.Bytes)-int64(final.Peak))/bytesPerMiB,
			100*float64(int64(final.Bytes)-int64(final.Peak))/float64(final.Bytes),
			float64(processPeak)/bytesPerMiB)
	}

	if final.Counters {
		t.Logf("at the limit: %d reclaim events, %d allocation failures, %d kills",
			final.Reclaims, final.OOMs, final.OOMKills)
		// This process could not have survived its own kill, but a kill counted
		// here is a kill of something in the same cgroup, and a result taken from
		// a cgroup where something died is not a result.
		if final.OOMKills > 0 {
			t.Errorf("the cgroup recorded %d OOM kills during this run", final.OOMKills)
		}
		if final.Reclaims > 0 {
			t.Logf("note: the run touched the limit %d times and the kernel reclaimed rather than "+
				"failing. It fits, but not with room to spare -- read the headroom above as an "+
				"upper bound on comfort, not on residency", final.Reclaims)
		}
	}
}

// holdingNote reports what the store is holding, not what it was asked to hold.
// Each of the three residency options has a documented fallback, and a ceiling
// run whose store quietly fell back would be measuring a configuration nobody
// chose.
func holdingNote(g *graphene.Graph) string {
	st, ok := g.StorageStats()
	if !ok {
		return "no storage stats"
	}
	return strings.Join([]string{
		"image " + orUnknown(st.ImageMode),
		"index " + orUnknown(st.IndexMode),
		"adjacency " + orUnknown(st.Adjacency),
	}, ", ")
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
