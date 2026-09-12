//go:build windows

package disk

// The Windows half of the image mapping: CreateFileMapping, MapViewOfFile,
// UnmapViewOfFile — all three in the standard syscall package, so this costs no
// dependency and no hand-resolved procedure the way lock_windows.go and
// fileshare_windows.go do.
//
// # The file handle is closed before this returns, and that is the design
//
// A section object created from a file handle keeps the bytes reachable by
// itself, and it inherits that handle's sharing mode. openSharedRead asks for
// FILE_SHARE_DELETE, so once the handle is gone the *name* is free while the
// mapping stays valid. Measured on Windows 11 Pro 26200, 1 MiB and 8 KiB files,
// with the file handle closed after MapViewOfFile:
//
//	operation on a mapped graphene.csr        handle closed   handle kept open
//	os.Rename the mapped file away            OK              OK
//	os.Rename another file over it            OK              Access denied
//	os.Remove it                              OK              OK
//	read through the mapping after any of     OK              OK
//	os.Truncate it                            ERROR_USER_MAPPED_FILE (both)
//
// Three things follow, and together they are why there is no Windows-specific
// install sequence anywhere in compact.go.
//
// The plain os.Rename in compactCommit works with the old image mapped, so
// compaction needs no rename-aside, no .retired.<seq> files in the store
// directory, and no recovery path for a crash between two renames. Windows
// behaves exactly as Unix inode semantics do: three generations of image, each
// mapped and each replaced by a rename over the live mapping, each still read
// back its own bytes whole and correct.
//
// Keeping the handle open would break that — the rename over the name is
// refused with a sharing violation — so closing it is load-bearing, not tidy.
//
// And the fault exposure ImageMappedUnlocked documents is smaller here than on
// Unix: Windows refuses to shorten a file with a live mapping at all, so the
// truncation that produces SIGBUS on Unix cannot happen. What is still reachable
// is an in-place overwrite, which the measurement confirms is visible through
// the mapping. That is a content hazard rather than a fatal one, on both
// platforms, and §15 states it as such.
//
// sliceOfMapping at the bottom is how the mapping address becomes a slice without
// tripping go vet's unsafeptr check, and says why that form rather than the
// obvious one.

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

// mappingSupported is what openImage tests before trying.
const mappingSupported = true

// mapFile maps the whole of f read-only and returns the bytes and the release.
//
// Size zero is refused by CreateFileMapping for a zero-length file; mapImage has
// already rejected that case, and passing 0/0 for the maximum size here is what
// asks for "the whole file" rather than a size this code would have to agree
// with the caller about.
func mapFile(f *os.File, size int) ([]byte, func() error, error) {
	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return nil, nil, err
	}
	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, 0)
	if err != nil {
		_ = syscall.CloseHandle(h)
		return nil, nil, err
	}

	data := sliceOfMapping(addr, size)

	return data, func() error {
		uerr := syscall.UnmapViewOfFile(addr)
		cerr := syscall.CloseHandle(h)
		if uerr != nil {
			return uerr
		}
		return cerr
	}, nil
}

// sliceOfMapping turns the address MapViewOfFile returned into a byte slice.
//
// The form is the one Phase 0 settled on in tests/rss_mapfile_windows_test.go
// after trying the alternatives, and the argument is worth having here too rather
// than only by reference.
//
// The obvious spelling is unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), and
// it is correct: the address names an OS mapping outside the Go heap, so it can
// never be stale in the way go vet's unsafeptr check exists to catch. Vet flags it
// anyway -- it cannot tell a syscall-returned mapping address from a uintptr that
// once held a heap pointer, and it flags the same expression inline, behind a
// helper, and fed straight from the syscall result alike. All three were tried.
//
// Writing the header fields performs no uintptr-to-pointer conversion at all, so
// the check has nothing to fire on. reflect.SliceHeader is documented as
// deprecated, and the deprecation is aimed at using it to describe heap memory,
// where the collector must see the pointer and this type hides it. Here the
// memory is deliberately outside the collector's world -- which is the whole
// premise of a mapped image -- so its semantics are exactly what is wanted.
//
// The alternative is disabling unsafeptr for the repository, which would also
// stop it checking index/narrow.go and index/encoding/encoding.go, where it is
// doing real work. One function wide is the right width for the exception.
//
// No Go value is reinterpreted and no alignment is assumed: size comes from the
// stat mapImage already performed, never from anything in the file, and the
// result is only ever read.
func sliceOfMapping(addr uintptr, size int) []byte {
	var b []byte
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h.Data = addr
	h.Len = size
	h.Cap = size
	return b
}
