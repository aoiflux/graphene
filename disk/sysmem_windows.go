//go:build windows

package disk

// The windows half: K32GetProcessMemoryInfo for what we hold, a Job Object for
// what we are allowed.
//
// All three entry points are resolved by hand out of kernel32 the way
// lock_windows.go and fileshare_windows.go do, because the standard syscall
// package exposes none of them. K32GetProcessMemoryInfo rather than the psapi
// export of the same function keeps the dependency on a library already loaded
// into every process.
//
// # Commit charge is the right analogue, and the error is one-sided
//
// Windows publishes WorkingSetSize -- every resident byte, private and
// file-backed alike -- and PrivateUsage, which is the process's *commit charge*
// rather than its resident private bytes. So Anon here is overstated by whatever
// the Go runtime holds committed and untouched, measured at tens of megabytes
// and roughly fixed rather than scaling with the store.
//
// That error is in the safe direction twice over. A budget derived from an
// overstated Anon is smaller than it needed to be, and a mapping change that
// shows a win against it has really won -- a read-only file mapping carries no
// commit charge at all, so the mapped image is not counted here, exactly as page
// cache is reclaimed rather than counted under a cgroup.

import (
	"sync"
	"syscall"
	"unsafe"
)

var (
	sysmemOnce       sync.Once
	procMemInfo      *syscall.LazyProc
	procQueryJob     *syscall.LazyProc
	procGlobalMemory *syscall.LazyProc
)

func sysmemProcs() {
	sysmemOnce.Do(func() {
		k32 := syscall.NewLazyDLL("kernel32.dll")
		procMemInfo = k32.NewProc("K32GetProcessMemoryInfo")
		procQueryJob = k32.NewProc("QueryInformationJobObject")
		procGlobalMemory = k32.NewProc("GlobalMemoryStatusEx")
	})
}

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

func readSysMemory() (sysMemory, bool) {
	sysmemProcs()
	if err := procMemInfo.Find(); err != nil {
		return sysMemory{}, false
	}
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return sysMemory{}, false
	}
	var c processMemoryCountersEx
	c.CB = uint32(unsafe.Sizeof(c))
	ret, _, _ := procMemInfo.Call(uintptr(h), uintptr(unsafe.Pointer(&c)), uintptr(c.CB))
	if ret == 0 {
		return sysMemory{}, false
	}
	return sysMemoryFromCounters(c), true
}

// sysMemoryFromCounters is the interpretation half, split out so the clamp below
// can be tested without being inside a process that reproduces the condition.
func sysMemoryFromCounters(c processMemoryCountersEx) sysMemory {
	resident := uint64(c.WorkingSetSize)
	anon := uint64(c.PrivateUsage)
	// Committed-but-paged-out private memory can push PrivateUsage past the whole
	// working set. Clamp rather than let Resident-Anon underflow: "all of what is
	// resident is private" is the honest reading of that state, and a caller
	// subtracting to get the file-backed share must get zero rather than a number
	// near 2^64.
	anon = min(anon, resident)
	return sysMemory{
		Anon:     anon,
		Resident: resident,
		Peak:     uint64(c.PeakWorkingSetSize),
		Split:    true,
		Current:  true,
	}
}

type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type jobObjectBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type jobObjectExtendedLimitInformation struct {
	BasicLimitInformation jobObjectBasicLimitInformation
	IoInfo                ioCounters
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

const (
	jobObjectExtendedLimitInformationClass = 9
	jobObjectLimitProcessMemory            = 0x00000100
	jobObjectLimitJobMemory                = 0x00000200
)

// readSysCeiling asks which job governs this process, and falls back to the
// machine's physical memory.
//
// The handle passed to QueryInformationJobObject is NULL, which the API
// documents as "the job associated with the calling process". That is
// deliberate: it asks the kernel which job actually governs us, rather than
// reading back a value someone already knew.
func readSysCeiling() (sysCeiling, bool) {
	sysmemProcs()
	if c, ok := readJobCeiling(); ok {
		return c, true
	}
	if n, ok := readPhysicalMemory(); ok {
		return sysCeiling{Bytes: n, Source: "GlobalMemoryStatusEx ullTotalPhys", Enforced: false}, true
	}
	return sysCeiling{}, false
}

func readJobCeiling() (sysCeiling, bool) {
	if err := procQueryJob.Find(); err != nil {
		return sysCeiling{}, false
	}
	var info jobObjectExtendedLimitInformation
	var returned uint32
	ret, _, _ := procQueryJob.Call(
		0,
		uintptr(jobObjectExtendedLimitInformationClass),
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
		uintptr(unsafe.Pointer(&returned)),
	)
	if ret == 0 {
		// Not in a job, or not permitted to ask. Neither is an error worth
		// reporting: it means there is no job ceiling, which is itself a finding.
		return sysCeiling{}, false
	}
	return ceilingFromJobLimits(info)
}

// ceilingFromJobLimits reads the tighter of the two memory limits a job can
// carry, and refuses a job that carries neither.
//
// The flags check is the whole of it. A process can be inside a job that
// constrains something else entirely -- a CI runner puts its steps in one, and
// so does a container runtime -- and JobMemoryLimit is then a field nobody set.
// Believing it would hand the engine a ceiling of zero bytes, and a budget
// derived from zero is a store that refuses to open.
func ceilingFromJobLimits(info jobObjectExtendedLimitInformation) (sysCeiling, bool) {
	flags := info.BasicLimitInformation.LimitFlags
	var limit uint64
	var source string

	// Per-process first: when a job carries both, the per-process limit is what
	// binds this process and the job-wide one is shared with siblings we cannot
	// see. Taking the smaller of the two is the only reading that is right in
	// both directions.
	if flags&jobObjectLimitProcessMemory != 0 && info.ProcessMemoryLimit > 0 {
		limit = uint64(info.ProcessMemoryLimit)
		source = "Job Object JOB_OBJECT_LIMIT_PROCESS_MEMORY"
	}
	if flags&jobObjectLimitJobMemory != 0 && info.JobMemoryLimit > 0 {
		if limit == 0 || uint64(info.JobMemoryLimit) < limit {
			limit = uint64(info.JobMemoryLimit)
			source = "Job Object JOB_OBJECT_LIMIT_JOB_MEMORY"
		}
	}
	if limit == 0 {
		return sysCeiling{}, false
	}
	return sysCeiling{Bytes: limit, Source: source, Enforced: true}, true
}

// memoryStatusEx mirrors MEMORYSTATUSEX.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

func readPhysicalMemory() (uint64, bool) {
	if err := procGlobalMemory.Find(); err != nil {
		return 0, false
	}
	var st memoryStatusEx
	st.Length = uint32(unsafe.Sizeof(st))
	ret, _, _ := procGlobalMemory.Call(uintptr(unsafe.Pointer(&st)))
	if ret == 0 || st.TotalPhys == 0 {
		return 0, false
	}
	return st.TotalPhys, true
}
