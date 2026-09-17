//go:build linux

package disk

// The linux half of the advice: madvise(2), wrapped in the standard syscall
// package, so this costs no dependency.

import "syscall"

// adviceSupported is what adviseRange's callers test before reporting a failure
// as one. A platform that cannot advise has not failed to.
const adviceSupported = true

// adviseRange applies a to data, which must be a whole mapping.
//
// Whole mapping rather than any range, and the restriction is real: madvise
// requires a page-aligned start. A mapping's start is page-aligned because mmap
// returned it, and the kernel rounds the length up on its own. A sub-range
// carved out of a Go slice has no such guarantee, so the type system is not what
// stops that call being written -- this comment is.
func adviseRange(data []byte, a mappingAdvice) error {
	if len(data) == 0 {
		return nil
	}
	flag, ok := adviceFlag(a)
	if !ok {
		return errAdviceUnsupported
	}
	return syscall.Madvise(data, flag)
}

// adviceFlag maps the portable advice onto this platform's constant.
//
// MADV_DONTNEED means something different for anonymous memory, where it
// destroys the contents, and for a file-backed MAP_SHARED mapping, where it
// drops the resident pages and the next access re-reads them from the file. This
// engine's image mappings are read-only, file-backed and MAP_SHARED without
// exception -- see mmap_unix.go, where MAP_SHARED is chosen for the accounting
// -- so the destructive reading cannot arise here and no data can be lost by it.
// That is the whole argument for why dropping pages is safe where unmapping was
// not.
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
