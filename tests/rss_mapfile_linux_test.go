//go:build linux

package graphene_test

import (
	"os"
	"syscall"
)

const runtimeGOOS = "linux"

// mapFileForTest maps the file read-only and shared, the same way the image
// mapping will: SHARED so the resident pages *are* the page cache and the
// file-backed accounting is unambiguous.
func mapFileForTest(f *os.File, size int) ([]byte, func(), error) {
	data, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return data, func() { _ = syscall.Munmap(data) }, nil
}
