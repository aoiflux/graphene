//go:build darwin

package disk

// The darwin half, and it is a raw syscall because the standard library does not
// wrap this one.
//
// syscall.Madvise exists on linux and does not exist on darwin: the wrapper
// lives in golang.org/x/sys/unix, which this module has no dependency on and is
// not acquiring one for a hint. The constants and SYS_MADVISE are both in the
// standard syscall package, so the call is spelled out here instead.
//
// The uintptr conversion is inline in the syscall.Syscall argument list on
// purpose. That is the one form go vet's unsafeptr check recognises as safe --
// the value cannot be moved by the collector between the conversion and the call
// when it is written this way -- and it is why this is not factored into a
// helper taking a uintptr.
//
// # This platform is best effort here as it is everywhere else
//
// Advice is a hint. A kernel that ignores it, or a libc that has deprecated the
// path this reaches it by, costs an optimisation and never costs correctness --
// which is the property that makes a raw call acceptable here and would not make
// it acceptable for a write. Failures are reported through
// store.MetricResidentAdvice rather than swallowed, so an option asked for and
// not held is visible.

import (
	"syscall"
	"unsafe"
)

// adviceSupported is what adviseRange's callers test before reporting a failure
// as one.
const adviceSupported = true

// adviseRange applies a to data, which must be a whole mapping. See the linux
// half for why whole rather than any range.
func adviseRange(data []byte, a mappingAdvice) error {
	if len(data) == 0 {
		return nil
	}
	flag, ok := adviceFlag(a)
	if !ok {
		return errAdviceUnsupported
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(flag))
	if errno != 0 {
		return errno
	}
	return nil
}

// adviceFlag maps the portable advice onto this platform's constant.
//
// MADV_DONTNEED rather than MADV_FREE for adviceDontNeed, and the plan this came
// from named MADV_FREE. MADV_FREE is for anonymous memory: it tells the kernel
// the contents may be discarded, which is meaningless for a read-only file
// mapping whose contents are the file. MADV_DONTNEED is the one that drops the
// resident pages and lets the next access re-read them, which is what is wanted
// and what the linux half does.
func adviceFlag(a mappingAdvice) (int, bool) {
	switch a {
	case adviceNormal:
		return syscall.MADV_NORMAL, true
	case adviceRandom:
		return syscall.MADV_RANDOM, true
	case adviceSequential:
		return syscall.MADV_SEQUENTIAL, true
	case adviceDontNeed:
		return syscall.MADV_DONTNEED, true
	}
	return 0, false
}
