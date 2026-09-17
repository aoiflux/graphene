//go:build windows

package disk

// The windows half: SetProcessWorkingSetSizeEx to install the cap and
// GetProcessWorkingSetSizeEx to read back what actually took.
//
// Both are resolved by hand out of kernel32 the way sysmem_windows.go,
// lock_windows.go and fileshare_windows.go do, because the standard syscall
// package exposes neither.
//
// # The minimum, which the API insists on and this does not want
//
// SetProcessWorkingSetSizeEx takes a minimum as well as a maximum and refuses a
// minimum above the maximum. Nothing here wants to raise a floor -- raising one
// above the default needs SE_INC_BASE_PRIORITY_NAME and would be the engine
// reserving memory on its host's behalf, which is the opposite of the point. So
// the minimum is set well below the cap and QUOTA_LIMITS_HARDWS_MIN_DISABLE is
// passed with it, which tells the kernel to treat the floor as the hint it
// always was while the ceiling is enforced.

import (
	"fmt"
	"sync"
	"syscall"
	"unsafe"
)

var (
	wsOnce      sync.Once
	procSetWSEx *syscall.LazyProc
	procGetWSEx *syscall.LazyProc
)

func workingSetProcs() {
	wsOnce.Do(func() {
		k32 := syscall.NewLazyDLL("kernel32.dll")
		procSetWSEx = k32.NewProc("SetProcessWorkingSetSizeEx")
		procGetWSEx = k32.NewProc("GetProcessWorkingSetSizeEx")
	})
}

const (
	quotaLimitsHardWSMinDisable = 0x00000002
	quotaLimitsHardWSMaxEnable  = 0x00000004
)

// workingSetFloor is how far below the cap the minimum is placed.
//
// A quarter, and floored at one page so a tiny cap is still a legal pair. The
// exact figure does not matter because MIN_DISABLE makes it advisory; what
// matters is that it is strictly below the maximum, which is the one thing the
// call will refuse.
func workingSetFloor(maxBytes uint64) uint64 {
	const page = 4096
	minBytes := maxBytes / 4
	if minBytes < page {
		minBytes = page
	}
	if minBytes >= maxBytes {
		minBytes = maxBytes / 2
	}
	if minBytes == 0 {
		minBytes = 1
	}
	return minBytes
}

func setWorkingSetLimit(maxBytes uint64) (WorkingSetLimit, error) {
	workingSetProcs()
	if err := procSetWSEx.Find(); err != nil {
		return WorkingSetLimit{}, fmt.Errorf("%w: %v", ErrWorkingSetUnsupported, err)
	}
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return WorkingSetLimit{}, err
	}

	minBytes := workingSetFloor(maxBytes)
	ret, _, callErr := procSetWSEx.Call(uintptr(h), uintptr(minBytes), uintptr(maxBytes),
		uintptr(quotaLimitsHardWSMaxEnable|quotaLimitsHardWSMinDisable))
	if ret == 0 {
		return WorkingSetLimit{}, fmt.Errorf("SetProcessWorkingSetSizeEx(%d..%d): %w", minBytes, maxBytes, callErr)
	}

	// And read it back. A call that returned success and installed something
	// other than what was asked for is the failure mode this whole programme
	// refuses to ship -- tests/ceiling_test.go will not report a pass under a
	// limit it did not confirm, and a caller of this has exactly the same right
	// to know.
	return readWorkingSetLimit(h)
}

// restoreWorkingSetDefault removes any cap this process installed and hands the
// working set back to the system's own sizing.
//
// Passing (SIZE_T)-1 for both figures with no hard flag is the documented way to
// say "decide for yourself again". It exists for the tests: a test that installs
// a hard cap and leaves it installed has constrained every test that runs after
// it in the same binary, which would make an unrelated failure look like a
// memory bug. It is unexported because a caller that wants no limit should never
// have called LimitWorkingSet.
func restoreWorkingSetDefault() error {
	workingSetProcs()
	if err := procSetWSEx.Find(); err != nil {
		return err
	}
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return err
	}
	const systemDecides = ^uintptr(0)
	ret, _, callErr := procSetWSEx.Call(uintptr(h), systemDecides, systemDecides, 0)
	if ret == 0 {
		return callErr
	}
	return nil
}

// readWorkingSetLimit asks the kernel what the limits now are.
func readWorkingSetLimit(h syscall.Handle) (WorkingSetLimit, error) {
	workingSetProcs()
	if err := procGetWSEx.Find(); err != nil {
		// The cap is installed and cannot be confirmed. Reporting it as applied
		// would be the fiction above, so this is an error with the limit left
		// zero, and the caller can decide whether an unconfirmable cap is one it
		// wants to run under.
		return WorkingSetLimit{}, fmt.Errorf("the working-set limit was set and cannot be read back: %w", err)
	}
	var gotMin, gotMax uintptr
	var flags uint32
	ret, _, callErr := procGetWSEx.Call(uintptr(h),
		uintptr(unsafe.Pointer(&gotMin)), uintptr(unsafe.Pointer(&gotMax)), uintptr(unsafe.Pointer(&flags)))
	if ret == 0 {
		return WorkingSetLimit{}, fmt.Errorf("GetProcessWorkingSetSizeEx: %w", callErr)
	}
	return WorkingSetLimit{
		MinBytes: uint64(gotMin),
		MaxBytes: uint64(gotMax),
		// The flag, not the request. A kernel that installed a soft limit where a
		// hard one was asked for has bounded nothing, and the caller reading Hard
		// is the only one who can notice.
		Hard: flags&quotaLimitsHardWSMaxEnable != 0,
	}, nil
}
