//go:build stress && windows

package graphene_test

import (
	"fmt"
	"sync"
	"syscall"
	"testing"
	"unsafe"
)

// Windows is the mirror image of linux here: a process may place itself under a
// Job Object memory limit without any privilege at all, and there is no
// convenient way to apply one from outside. So this platform self-applies and
// then reads the limit back, rather than refusing and telling the caller to wrap
// the command.
//
// The instrument is JOB_OBJECT_LIMIT_JOB_MEMORY, which bounds the commit charge
// of every process in the job. Commit charge is the closest analogue Windows has
// to the anonymous class linux's memory.max constrains, and the difference is in
// the safe direction for this test: a read-only file mapping carries no commit
// charge, so the mapped image is not counted -- exactly as a page cache is
// reclaimed rather than counted on linux. What remains under the limit is the Go
// heap, the runtime's own arenas, and the stacks, which is the memory that
// actually has to fit.
//
// Exceeding it is not a kill. The allocation fails, and a Go runtime whose
// arena reservation fails throws "out of memory" and dies -- a different death
// from the OOM killer's, and an equally conclusive one.
const ceilingSelfApplies = true

const ceilingHowTo = `The harness applies the ceiling itself on windows; set GRAPHENE_CEILING_MIB
and run the test. A failure to apply one means the Job Object API refused,
which is reported above with the underlying error.`

// ioCounters is IO_COUNTERS. Present only so the extended limit structure below
// has the right size and field offsets; nothing here reads it.
type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

// jobObjectBasicLimitInformation mirrors JOBOBJECT_BASIC_LIMIT_INFORMATION.
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

// jobObjectExtendedLimitInformation mirrors JOBOBJECT_EXTENDED_LIMIT_INFORMATION.
// PeakJobMemoryUsed is the high-water mark of the charge the limit is enforced
// against, and is the windows counterpart of cgroup v2's memory.peak.
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
	jobObjectLimitJobMemory                = 0x00000200
)

var (
	jobOnce      sync.Once
	jobKernel32  *syscall.LazyDLL
	procCreate   *syscall.LazyProc
	procAssign   *syscall.LazyProc
	procSetInfo  *syscall.LazyProc
	procQuery    *syscall.LazyProc
	procAlloc    *syscall.LazyProc
	procFree     *syscall.LazyProc
	jobHandleRef syscall.Handle
)

func jobProcs() {
	jobOnce.Do(func() {
		jobKernel32 = syscall.NewLazyDLL("kernel32.dll")
		procCreate = jobKernel32.NewProc("CreateJobObjectW")
		procAssign = jobKernel32.NewProc("AssignProcessToJobObject")
		procSetInfo = jobKernel32.NewProc("SetInformationJobObject")
		procQuery = jobKernel32.NewProc("QueryInformationJobObject")
		procAlloc = jobKernel32.NewProc("VirtualAlloc")
		procFree = jobKernel32.NewProc("VirtualFree")
	})
}

// ceilingApply creates an anonymous Job Object carrying limit as its job memory
// bound and moves this process into it.
//
// The limit is set before the process is assigned, so there is no window in
// which this process is inside a job that does not yet constrain it.
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE is deliberately not set: the job's only
// member is this process, and arming it would turn any accidental close of the
// handle into a silent kill of the run. The handle is parked in a package
// variable so nothing closes it for the lifetime of the process.
func ceilingApply(limit uint64) error {
	jobProcs()

	h, _, err := procCreate.Call(0, 0)
	if h == 0 {
		return fmt.Errorf("CreateJobObject: %w", err)
	}
	job := syscall.Handle(h)

	var info jobObjectExtendedLimitInformation
	info.BasicLimitInformation.LimitFlags = jobObjectLimitJobMemory
	info.JobMemoryLimit = uintptr(limit)

	ok, _, err := procSetInfo.Call(
		uintptr(job),
		uintptr(jobObjectExtendedLimitInformationClass),
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
	)
	if ok == 0 {
		_ = syscall.CloseHandle(job)
		return fmt.Errorf("SetInformationJobObject(JobMemoryLimit=%d): %w", limit, err)
	}

	process, perr := syscall.GetCurrentProcess()
	if perr != nil {
		_ = syscall.CloseHandle(job)
		return fmt.Errorf("GetCurrentProcess: %w", perr)
	}
	ok, _, err = procAssign.Call(uintptr(job), uintptr(process))
	if ok == 0 {
		_ = syscall.CloseHandle(job)
		return fmt.Errorf("AssignProcessToJobObject: %w", err)
	}

	jobHandleRef = job
	return nil
}

// ceilingRead queries the job this process belongs to.
//
// The handle passed is NULL, which the API documents as "the job associated with
// the calling process". That is deliberate: querying the handle ceilingApply
// created would only prove that the value written is the value read back, while
// NULL asks the kernel which job actually governs this process -- the question
// worth answering, and the one that catches an assignment that silently did not
// take.
func ceilingRead() (ceilingInfo, error) {
	jobProcs()

	var info jobObjectExtendedLimitInformation
	var returned uint32
	ok, _, err := procQuery.Call(
		0,
		uintptr(jobObjectExtendedLimitInformationClass),
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
		uintptr(unsafe.Pointer(&returned)),
	)
	if ok == 0 {
		return ceilingInfo{}, fmt.Errorf("QueryInformationJobObject: %w", err)
	}
	return ceilingFromJob(info)
}

// ceilingFromJob is the interpretation half of ceilingRead, split out so it can
// be tested without being in a job.
//
// The flags check is the whole of it. A process can be inside a job that
// constrains something else entirely -- a CI runner puts its steps in one --
// and JobMemoryLimit is then a field nobody set. Believing it would hand the
// acceptance test a ceiling of zero bytes, which every peak is comfortably
// under, and the run would pass having been constrained by nothing at all.
func ceilingFromJob(info jobObjectExtendedLimitInformation) (ceilingInfo, error) {
	if info.BasicLimitInformation.LimitFlags&jobObjectLimitJobMemory == 0 {
		return ceilingInfo{}, fmt.Errorf(
			"this process is in a job object, but it carries no JOB_OBJECT_LIMIT_JOB_MEMORY (flags 0x%08x)",
			info.BasicLimitInformation.LimitFlags)
	}
	return ceilingInfo{
		Bytes:  uint64(info.JobMemoryLimit),
		Source: "Job Object JOB_OBJECT_LIMIT_JOB_MEMORY",
		// Windows has no swap switch per job: the pagefile is machine-wide, and
		// the limit is on commit rather than on residency, so paging out does not
		// buy a process headroom the way swap does under a cgroup.
		SwapMax: "n/a (commit charge, not residency)",
		Peak:    uint64(info.PeakJobMemoryUsed),
	}, nil
}

// TestCeilingFromJob_RefusesAJobThatLimitsSomethingElse is the guard for the
// case above: a job that is real, a JobMemoryLimit field that is not.
func TestCeilingFromJob_RefusesAJobThatLimitsSomethingElse(t *testing.T) {
	var info jobObjectExtendedLimitInformation
	// A limit flag that is not the memory one, and a memory field nobody set on
	// purpose. Both are what an inherited job looks like.
	info.BasicLimitInformation.LimitFlags = 0x00002000 // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	info.JobMemoryLimit = 1 << 30

	if got, err := ceilingFromJob(info); err == nil {
		t.Fatalf("a job carrying no JOB_OBJECT_LIMIT_JOB_MEMORY was read as a %d byte ceiling", got.Bytes)
	}
}

// TestCeilingFromJob_ReadsTheLimitItIsGiven is its other half: the check above
// must refuse the wrong job without also refusing the right one.
func TestCeilingFromJob_ReadsTheLimitItIsGiven(t *testing.T) {
	var info jobObjectExtendedLimitInformation
	info.BasicLimitInformation.LimitFlags = jobObjectLimitJobMemory
	info.JobMemoryLimit = 2048 << 20
	info.PeakJobMemoryUsed = 1500 << 20

	got, err := ceilingFromJob(info)
	if err != nil {
		t.Fatalf("a job carrying a memory limit was refused: %v", err)
	}
	if got.Bytes != 2048<<20 {
		t.Errorf("ceiling = %d bytes, want %d", got.Bytes, uint64(2048)<<20)
	}
	if got.Peak != 1500<<20 {
		t.Errorf("peak = %d bytes, want %d", got.Peak, uint64(1500)<<20)
	}
}

const (
	memCommit     = 0x00001000
	memReserve    = 0x00002000
	memRelease    = 0x00008000
	pageReadWrite = 0x00000004
)

// ceilingProbe commits n bytes and releases them again, reporting whether the
// kernel allowed it.
//
// This is the one question a ceiling has to answer, asked directly. Growing the
// Go heap into the limit asks the same kernel the same thing -- the runtime
// commits through this call -- but a refusal there is a fatal throw rather than
// an error, and the process then spends minutes failing to print a traceback it
// has no memory for. Here a refusal is a value.
func ceilingProbe(n uint64) error {
	jobProcs()

	addr, _, err := procAlloc.Call(0, uintptr(n), memCommit|memReserve, pageReadWrite)
	if addr == 0 {
		return fmt.Errorf("VirtualAlloc(%d bytes, MEM_COMMIT): %w", n, err)
	}
	if ok, _, err := procFree.Call(addr, 0, memRelease); ok == 0 {
		return fmt.Errorf("VirtualFree: %w", err)
	}
	return nil
}
