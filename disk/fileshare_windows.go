//go:build windows

package disk

// The Windows half: CreateFileW with FILE_SHARE_DELETE, and ReplaceFileW.
//
// Both are resolved from kernel32 by hand, for the reason lock_windows.go gives
// -- the standard syscall package exposes neither, and pulling in
// golang.org/x/sys for two calls would trade away the zero-dependency property
// this repository documents and intends to keep.

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

var (
	modkernel32Share = syscall.NewLazyDLL("kernel32.dll")
	procReplaceFileW = modkernel32Share.NewProc("ReplaceFileW")
)

// openSharedRead opens path for reading with every sharing mode granted,
// including FILE_SHARE_DELETE.
//
// Go's os.Open asks for FILE_SHARE_READ|FILE_SHARE_WRITE and stops there, so a
// file it has open cannot be renamed or deleted by anyone. Granting delete as
// well is what lets the writer rotate the log out from under a reader -- the
// reader keeps reading the handle it has, which is the Unix behaviour this is
// reproducing rather than a Windows-specific relaxation.
func openSharedRead(path string) (*os.File, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := syscall.CreateFile(
		p,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		// os.IsNotExist must keep working on the result: the live reader treats a
		// missing log as an empty one, and a raw Errno would defeat that.
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

// replaceFile atomically replaces newpath with oldpath.
//
// os.Rename first, which is MoveFileEx with MOVEFILE_REPLACE_EXISTING and
// MOVEFILE_WRITE_THROUGH: one metadata operation, made durable before it
// returns. That is the stronger call and it is the one a store without a live
// reader always takes.
//
// It fails when another process holds the destination open, which is the one
// case ReplaceFileW covers and the only case it is used for. See fileshare.go
// for what that costs and how the window is closed.
func replaceFile(oldpath, newpath string) error {
	err := os.Rename(oldpath, newpath)
	if err == nil || !sharingViolation(err) {
		return err
	}

	from, cerr := syscall.UTF16PtrFromString(oldpath)
	if cerr != nil {
		return &os.LinkError{Op: "replace", Old: oldpath, New: newpath, Err: cerr}
	}
	to, cerr := syscall.UTF16PtrFromString(newpath)
	if cerr != nil {
		return &os.LinkError{Op: "replace", Old: oldpath, New: newpath, Err: cerr}
	}

	// No backup file: the caller's contract is that oldpath is the file that
	// should survive, and a backup would leave a third name in the store
	// directory for an operator to wonder about.
	r, _, e := procReplaceFileW.Call(
		uintptr(unsafe.Pointer(to)),
		uintptr(unsafe.Pointer(from)),
		0, 0, 0, 0,
	)
	if r == 0 {
		return &os.LinkError{Op: "replace", Old: oldpath, New: newpath, Err: e}
	}
	return nil
}

// sharingViolation reports whether err is Windows refusing an operation on a
// name because someone else has the file open.
func sharingViolation(err error) bool {
	const errorSharingViolation = syscall.Errno(32)
	var errno syscall.Errno
	switch e := err.(type) {
	case *os.LinkError:
		errno, _ = e.Err.(syscall.Errno)
	case *os.PathError:
		errno, _ = e.Err.(syscall.Errno)
	case syscall.Errno:
		errno = e
	}
	return errno == errorSharingViolation || errno == syscall.ERROR_ACCESS_DENIED
}

func dirOf(path string) string { return filepath.Dir(path) }
