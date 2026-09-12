//go:build unix

package disk

// The Unix half of the image mapping: mmap(2) and munmap(2), both in the
// standard syscall package.
//
// MAP_SHARED rather than MAP_PRIVATE, and the reason is measurement rather than
// semantics. Read-only, the two behave identically — there is no write to make
// private — but the kernel accounts them differently: a MAP_SHARED read-only
// file mapping's resident pages are file-backed and counted in RssFile, which is
// exactly the separation Phase 0's instrument reports and the separation the
// whole item is judged by. Under MAP_PRIVATE the same pages can be charged to
// the process as anonymous once touched, which would make a change that moved a
// gigabyte out of the heap look as though it had moved nothing.

import (
	"os"
	"syscall"
)

// mappingSupported is what openImage tests before trying.
const mappingSupported = true

// mapFile maps the whole of f read-only and returns the bytes and the release.
func mapFile(f *os.File, size int) ([]byte, func() error, error) {
	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return data, func() error { return syscall.Munmap(data) }, nil
}
