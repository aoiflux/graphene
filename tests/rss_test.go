// Process-residency instrument.
//
// The footprint suite next door measures runtime.MemStats.HeapAlloc, which
// answers "how much Go heap does this graph retain". That is the wrong question
// for a memory budget expressed in machine RAM: it cannot see a memory-mapped
// image at all (those bytes are resident, but they are not Go heap), it cannot
// see the runtime's own arenas and GC headroom, and it cannot see the page cache
// a large sequential read leaves behind.
//
// These helpers read the operating system's own accounting instead, and keep the
// two kinds of residency apart:
//
//   - anonymous: private, swap-backed pages. The Go heap, stacks and runtime
//     structures live here. This is the number a hard RAM ceiling constrains.
//   - file-backed: pages that are a view of a file on disk. A mapped image lives
//     here. Under pressure the kernel can evict these without swap, so they are
//     real residency but cheap residency.
//
// Any change that moves bytes from the first class to the second looks like a
// win on the total and like nothing at all on HeapAlloc; only the split shows
// what actually happened. Reporting a single combined figure would make a
// mapping change indistinguishable from no change.
//
// Availability is not uniform, and the helpers say so rather than inventing
// numbers: see rssSample.Split and rssSupported.
package graphene_test

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"testing"
)

// rssSample is one reading of the process's resident memory, in bytes.
//
// Fields that the platform cannot supply are left zero, and Split reports
// whether Anon and File are meaningful. Callers must check Split before
// attributing a change to one class or the other.
type rssSample struct {
	Anon  uint64 // resident private/anonymous bytes
	File  uint64 // resident file-backed bytes
	Total uint64 // resident bytes, both classes
	Peak  uint64 // high-water mark of resident bytes for the process lifetime
	Split bool   // whether Anon and File are separately measured

	// AnonClamped says that Anon is Total because the platform's private figure
	// came out larger than the whole working set, so File is zero by
	// construction rather than by measurement.
	//
	// It exists because those two zeros mean opposite things and print the same.
	// "No file-backed residency" is a finding; "the estimate collapsed" is the
	// absence of one, and a reader deciding whether to bound the file class off
	// a table of 0.0 needs to know which they are looking at. Windows is where
	// this happens -- see rss_windows_test.go for why its split is an estimate at
	// all -- and the Go runtime routinely over-commits enough to trigger it, so
	// this is the ordinary case there rather than an edge.
	//
	// Split stays true when it fires: the reading is still the platform's best
	// answer and Anon is still usable, in the conservative direction. Only File
	// is uninformative.
	AnonClamped bool
}

// goHeap is the Go runtime's own view, read beside every RSS sample so the drift
// between what the program allocated and what the OS is holding stays visible.
// A change that lowers HeapAlloc while total residency is unmoved has relocated
// bytes rather than freed them, and that is worth seeing in the same row.
type goHeap struct {
	Objects uint64 // /memory/classes/heap/objects:bytes — live heap objects
	Total   uint64 // /memory/classes/total:bytes — everything the runtime mapped
}

func readGoHeap() goHeap {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/total:bytes"},
	}
	metrics.Read(samples)
	var h goHeap
	if samples[0].Value.Kind() == metrics.KindUint64 {
		h.Objects = samples[0].Value.Uint64()
	}
	if samples[1].Value.Kind() == metrics.KindUint64 {
		h.Total = samples[1].Value.Uint64()
	}
	return h
}

// settledRSS runs the collector twice and returns the resulting sample.
//
// Two cycles for the same reason liveHeap uses two: the first runs cleanups and
// frees what died during the build, the second collects anything that only
// became unreachable as a result. The subsequent FreeOSMemory is what makes the
// reading meaningful — without it the runtime is free to keep freed spans mapped
// and the anonymous figure reports the high-water mark of the build rather than
// the steady state of the result.
//
// keepAlive must be whatever is under measurement, or the compiler may collect
// it before the sample is taken.
func settledRSS(keepAlive any) (rssSample, goHeap) {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	s := readRSS()
	h := readGoHeap()
	runtime.KeepAlive(keepAlive)
	return s, h
}

const bytesPerMiB = 1 << 20

// reportRSS attaches a settled residency reading to a benchmark's results.
//
// Metrics are in MiB to match the budget they are judged against, and follow the
// naming idiom of footprintOf next door. anonMiB and fileMiB appear only when
// the platform measures them separately; reporting a zero for an unmeasured
// class would read as "nothing there" rather than "not known".
func reportRSS(b *testing.B, keepAlive any) rssSample {
	b.Helper()

	s, h := settledRSS(keepAlive)
	if !rssSupported {
		b.Logf("RSS instrument unavailable on %s/%s; reporting Go heap only", runtime.GOOS, runtime.GOARCH)
	}
	if s.Split {
		b.ReportMetric(float64(s.Anon)/bytesPerMiB, "anonMiB")
		b.ReportMetric(float64(s.File)/bytesPerMiB, "fileMiB")
	}
	if s.Total > 0 {
		b.ReportMetric(float64(s.Total)/bytesPerMiB, "rssMiB")
	}
	if s.Peak > 0 {
		b.ReportMetric(float64(s.Peak)/bytesPerMiB, "peakMiB")
	}
	b.ReportMetric(float64(h.Objects)/bytesPerMiB, "heapMiB")
	// A one-shot fixture build makes ns/op meaningless; zero it so the residency
	// figures are the only signal, exactly as footprintOf does.
	b.ReportMetric(0, "ns/op")
	return s
}

// TestRSSInstrument_ReadsSomething checks the platform reader is wired up at all.
// It asserts only what every supported platform can supply, so that a port that
// forgets a field fails here rather than silently reporting zero for the rest of
// the program.
func TestRSSInstrument_ReadsSomething(t *testing.T) {
	if !rssSupported {
		t.Skipf("no RSS instrument for %s", runtime.GOOS)
	}
	s := readRSS()
	if s.Total == 0 && s.Peak == 0 {
		t.Fatalf("reader returned nothing usable: %+v", s)
	}
	if s.Split && s.Anon == 0 {
		t.Fatalf("reader claims an anon/file split but anon is zero: %+v", s)
	}
	t.Logf("anon=%d file=%d total=%d peak=%d split=%v", s.Anon, s.File, s.Total, s.Peak, s.Split)
}

// TestRSSInstrument_TheSplitAddsUpOrSaysItCannot holds the two invariants that
// make the anon/file columns readable.
//
// A reading that claims a split must account for all of it -- Anon plus File is
// the whole of Total -- or one class is silently absorbing bytes that belong to
// the other, which is the one error that would make a mapped-image result look
// like an anonymous one. And a reading whose split collapsed must say so, so
// that the zero it leaves in File is not read as a measurement of zero.
//
// Both are properties of the ruler rather than of graphene, which is why this
// sits beside the other calibrations and asserts nothing about a store. The
// second invariant is the one added for Phase 2 of docs/PLAN_BOUNDED_INGEST.md:
// that phase is gated on how large the file-backed class turns out to be during
// an ingest, and on windows the ordinary state of a small process is a collapsed
// split reporting 0.0 -- see rssSample.AnonClamped.
func TestRSSInstrument_TheSplitAddsUpOrSaysItCannot(t *testing.T) {
	if !rssSupported {
		t.Skipf("no RSS instrument for %s", runtime.GOOS)
	}
	s := readRSS()
	if !s.Split {
		t.Skipf("platform reports no anon/file split: %+v", s)
	}

	if s.AnonClamped {
		// The collapse is defined as anon having swallowed the whole reading. A
		// clamped sample that still carries a file figure would mean the flag and
		// the arithmetic disagree, and the flag is what the report trusts.
		if s.Anon != s.Total || s.File != 0 {
			t.Fatalf("a clamped reading must be all anon and no file: %+v", s)
		}
		t.Logf("split collapsed on this reading (anon=%d total=%d); File reads as unmeasured",
			s.Anon, s.Total)
		return
	}
	if s.Anon+s.File != s.Total {
		t.Fatalf("the split does not account for the whole reading: anon=%d + file=%d != total=%d",
			s.Anon, s.File, s.Total)
	}
	t.Logf("anon=%d + file=%d = total=%d", s.Anon, s.File, s.Total)
}

// TestRSSInstrument_TracksAnonymousGrowth is the calibration the rest of the
// program depends on: it verifies the reader actually moves with a known
// allocation, and moves in the anonymous class rather than the file class.
//
// Without this, a reader that returned a plausible constant would silently make
// every memory result in the program look like a success.
func TestRSSInstrument_TracksAnonymousGrowth(t *testing.T) {
	if !rssSupported {
		t.Skipf("no RSS instrument for %s", runtime.GOOS)
	}

	const grow = 256 << 20

	before, _ := settledRSS(nil)

	// Touch every page: an untouched allocation is reserved, not resident, and
	// would not move the reading on any of the three platforms.
	ballast := make([]byte, grow)
	for i := 0; i < len(ballast); i += 4096 {
		ballast[i] = 1
	}
	after, _ := settledRSS(&ballast)

	// Half the nominal size is a deliberately loose floor. The exact figure moves
	// with GC timing and with how much the runtime hands back to the OS, and a
	// tight bound here would be a flaky test rather than a stronger one. The
	// question being asked is "does this reader respond to real residency", and
	// half of a quarter-gigabyte answers it.
	const floor = grow / 2

	switch {
	case after.Split:
		if after.Anon < before.Anon+floor {
			t.Errorf("anon did not track a %d MiB touched allocation: before=%d after=%d",
				grow/bytesPerMiB, before.Anon, after.Anon)
		}
	case rssCurrent:
		if after.Total < before.Total+floor {
			t.Errorf("total did not track a %d MiB touched allocation: before=%d after=%d",
				grow/bytesPerMiB, before.Total, after.Total)
		}
	default:
		// A peak-only reader — darwin, which would need Mach calls and therefore
		// cgo to report the size now. Total is zero for it at every moment, so
		// asserting that a *difference* of zero covers a quarter-gigabyte is a
		// test of nothing that fails. The peak below is the whole of what this
		// platform can be calibrated on, and asserting it there rather than
		// silently skipping is what keeps a reader that returned a plausible
		// constant from passing.
		t.Logf("%s reports a peak and no current size; calibrating on the peak alone",
			runtime.GOOS)
	}
	if after.Peak < floor {
		t.Errorf("peak %d is below the size of an allocation already made", after.Peak)
	}
}
