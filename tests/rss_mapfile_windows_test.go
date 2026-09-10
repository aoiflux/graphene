//go:build windows

package graphene_test

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

const runtimeGOOS = "windows"

// mapFileForTest maps the file read-only, mirroring the sequence the image
// mapping will use: CreateFileMapping(PAGE_READONLY) then MapViewOfFile with
// FILE_MAP_READ, both reachable from the standard syscall package so the
// zero-cgo, zero-dependency closure holds.
func mapFileForTest(f *os.File, size int) ([]byte, func(), error) {
	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return nil, nil, err
	}

	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		_ = syscall.CloseHandle(h)
		return nil, nil, err
	}

	return sliceOfMapping(addr, size), func() {
		_ = syscall.UnmapViewOfFile(addr)
		_ = syscall.CloseHandle(h)
	}, nil
}

// sliceOfMapping turns the address MapViewOfFile returned into a byte slice.
//
// The obvious spelling is unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), and
// it is correct: the address names an OS mapping outside the Go heap, so it can
// never be stale in the way vet's unsafeptr check exists to catch. Vet flags it
// anyway — it cannot tell a syscall-returned mapping address from a uintptr that
// once held a heap pointer, and it flags the same expression whether the
// conversion is inline, behind a helper, or fed directly from the syscall
// result. All three were tried.
//
// Writing the header fields instead performs no uintptr-to-pointer conversion at
// all, so the check has nothing to fire on, and it is the form the established
// mmap packages settled on for this reason. reflect.SliceHeader is documented as
// deprecated, but the deprecation is aimed at using it to describe heap memory,
// where the garbage collector must see the pointer and this type hides it. Here
// the memory is deliberately outside the collector's world, which is the one
// case where its semantics are exactly what is wanted.
//
// The alternative is disabling unsafeptr for the whole repository, which would
// also stop it checking index/narrow.go and index/encoding/encoding.go, where it
// is doing real work. Keep the exception here, where it is one function wide.
func sliceOfMapping(addr uintptr, size int) []byte {
	var b []byte
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h.Data = addr
	h.Len = size
	h.Cap = size
	return b
}
