//go:build windows

package graphene_test

import (
	"sync"
	"syscall"
	"unsafe"
)

// Windows has no direct equivalent of linux's RssAnon/RssFile. What it does
// publish is WorkingSetSize (all resident bytes, private and file-backed alike)
// and PrivateUsage, and the difference between them stands in for the
// file-backed class:
//
//	anon ≈ min(PrivateUsage, WorkingSetSize)
//	file ≈ WorkingSetSize − anon
//
// The error in that is worth stating precisely, because the split is what every
// mapped-image conclusion in this program rests on. PrivateUsage is the
// process's *commit charge*, not its resident private bytes, and the Go runtime
// routinely holds far more committed than resident — measured here, a freshly
// started test process reports 9 MiB resident against 46 MiB committed, and
// FreeOSMemory does not close the gap. So anon is overstated and file is
// understated by the current over-commit.
//
// Two properties make that tolerable. First, the error is a roughly fixed
// process overhead rather than something that scales with the mapping: measured
// across mapping and touching 128 MiB, PrivateUsage moved by 1 MiB (46 → 47)
// while WorkingSetSize moved by 131 MiB, because a read-only file mapping
// carries no commit charge at all. Second, it is one-sided in the safe
// direction — it can only make a mapping look less effective than it is, never
// more — so a mapping change that shows a win here has really won.
//
// Total and Peak come straight from the kernel and carry none of this error;
// prefer them when the question is "does the process fit in the budget", and use
// the split when the question is "which class do these bytes live in".
//
// An exact answer exists via QueryWorkingSet, which reports a sharing bit per
// resident page, but it costs a multi-megabyte buffer and a per-page walk over a
// gigabyte-scale working set to remove an error already bounded by a few tens of
// megabytes in the conservative direction.
const rssSupported = true

// WorkingSetSize is a current reading, so the calibration can watch it move.
const rssCurrent = true

// processMemoryCountersEx mirrors PROCESS_MEMORY_COUNTERS_EX. The layout must
// match exactly: cb is validated by the kernel against the struct it was given.
type processMemoryCountersEx struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
	PrivateUsage               uintptr
}

// K32GetProcessMemoryInfo is the kernel32 export of the psapi entry point,
// present since Windows 7. Using it rather than psapi.dll keeps the dependency
// on a library already loaded into every process.
var (
	rssProcOnce sync.Once
	rssProc     *syscall.LazyProc
)

func processMemoryInfo() (processMemoryCountersEx, bool) {
	rssProcOnce.Do(func() {
		rssProc = syscall.NewLazyDLL("kernel32.dll").NewProc("K32GetProcessMemoryInfo")
	})
	if err := rssProc.Find(); err != nil {
		return processMemoryCountersEx{}, false
	}

	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return processMemoryCountersEx{}, false
	}

	var counters processMemoryCountersEx
	counters.CB = uint32(unsafe.Sizeof(counters))
	ret, _, _ := rssProc.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&counters)),
		uintptr(counters.CB),
	)
	if ret == 0 {
		return processMemoryCountersEx{}, false
	}
	return counters, true
}

func readRSS() rssSample {
	c, ok := processMemoryInfo()
	if !ok {
		return rssSample{}
	}

	total := uint64(c.WorkingSetSize)
	anon := uint64(c.PrivateUsage)

	// Committed-but-paged-out private memory can push PrivateUsage past the
	// whole working set. Clamp rather than underflow, and let file go to zero:
	// "no file-backed residency measured" is the honest reading of that state.
	var file uint64
	if total > anon {
		file = total - anon
	} else {
		anon = total
	}

	return rssSample{
		Anon:  anon,
		File:  file,
		Total: total,
		Peak:  uint64(c.PeakWorkingSetSize),
		Split: true,
	}
}
