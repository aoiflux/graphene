//go:build windows

package disk

// The Windows half of the process lock: LockFileEx.
//
// Go's standard syscall package exposes CreateFile and Overlapped but not
// LockFileEx, so it is resolved from kernel32 by hand. That keeps the engine at
// zero external dependencies, which is a property this repository documents and
// intends to keep — pulling in golang.org/x/sys for two calls would trade it
// away for about forty lines.
//
// # Three Windows details that shape the code
//
// Share mode: LockFileEx needs a handle other processes can also open. Go's
// os.OpenFile already passes FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
// so an os.File is exactly the right handle and a hand-rolled CreateFile is not
// needed. Opening with a restrictive share mode instead would exclude other
// processes at the *handle* level, which sounds like the same thing but is not:
// it cannot express shared-reader access, and it fails with a different error
// that callers would have to disentangle from an ordinary permission problem.
//
// Mandatory ranges: unlike flock, a LockFileEx range blocks I/O from processes
// that do not hold it. That is why lockByteOffset puts the locked byte far past
// the owner record — see its comment in lock.go.
//
// Handle ownership: a LockFileEx lock belongs to the handle, so two Opens in one
// process take two conflicting locks. That matches flock's open-file-description
// ownership on Unix, which is what lets one set of tests cover both platforms.

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// lockingEnforced reports whether this platform actually takes the lock.
const lockingEnforced = true

const (
	lockfileFailImmediately = 0x00000001
	lockfileExclusiveLock   = 0x00000002

	errorLockViolation = syscall.Errno(33)
	errorIOPending     = syscall.Errno(997)
)

// kernel32 is resolved lazily. NewLazyDLL is safe for this specific library
// despite the usual DLL-search-path concern: kernel32 is mapped into every Win32
// process by the loader before any user code runs, so the name always resolves
// to the already-loaded module and there is nothing to hijack.
var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")

	// Resolved once, explicitly. LazyProc.Call and LazyProc.Addr both panic when
	// the symbol is missing, and a panic out of a lock acquisition would be a
	// poor way to report a broken system DLL.
	resolveLockProcs sync.Once
	lockProcsErr     error
)

func lockProcs() error {
	resolveLockProcs.Do(func() {
		if err := procLockFileEx.Find(); err != nil {
			lockProcsErr = fmt.Errorf("disk: kernel32!LockFileEx: %w", err)
			return
		}
		if err := procUnlockFileEx.Find(); err != nil {
			lockProcsErr = fmt.Errorf("disk: kernel32!UnlockFileEx: %w", err)
		}
	})
	return lockProcsErr
}

func lockFile(f *os.File, mode LockMode) error {
	if err := lockProcs(); err != nil {
		return err
	}

	// LOCKFILE_FAIL_IMMEDIATELY always: acquisition never blocks. A shared lock
	// is the absence of LOCKFILE_EXCLUSIVE_LOCK rather than a flag of its own.
	flags := uintptr(lockfileFailImmediately)
	if mode == LockExclusive {
		flags |= lockfileExclusiveLock
	}

	// Locking the single byte at lockByteOffset. Past EOF is legal and does not
	// extend the file.
	ol := syscall.Overlapped{
		Offset:     uint32(lockByteOffset & 0xFFFFFFFF),
		OffsetHigh: uint32(lockByteOffset >> 32),
	}

	r1, _, errno := syscall.SyscallN(procLockFileEx.Addr(),
		f.Fd(),
		flags,
		0, // reserved, must be zero
		1, // bytes to lock, low word
		0, // bytes to lock, high word
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 != 0 {
		return nil
	}
	// A failed LOCKFILE_FAIL_IMMEDIATELY reports contention as
	// ERROR_LOCK_VIOLATION; ERROR_IO_PENDING appears on some paths for the same
	// condition. Anything else is a real failure and is passed through, because
	// reporting a permissions problem as contention would send the operator
	// looking for a process that does not exist.
	if errno == errorLockViolation || errno == errorIOPending {
		return ErrStoreLocked
	}
	if errno == 0 {
		return fmt.Errorf("disk: LockFileEx failed without an error code")
	}
	return errno
}

func unlockFile(f *os.File) error {
	if err := lockProcs(); err != nil {
		return err
	}

	ol := syscall.Overlapped{
		Offset:     uint32(lockByteOffset & 0xFFFFFFFF),
		OffsetHigh: uint32(lockByteOffset >> 32),
	}

	r1, _, errno := syscall.SyscallN(procUnlockFileEx.Addr(),
		f.Fd(),
		0, // reserved, must be zero
		1, // bytes to unlock, low word
		0, // bytes to unlock, high word
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 != 0 {
		return nil
	}
	if errno == 0 {
		return fmt.Errorf("disk: UnlockFileEx failed without an error code")
	}
	return errno
}
