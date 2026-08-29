//go:build !windows

package disk

// The Unix half: both contracts in fileshare.go are what the platform already
// does, so both are the ordinary call.
//
// This is not a stub standing in for a missing feature. An open file descriptor
// on Unix refers to an inode, not to a name, so renaming or unlinking the name
// is invisible to a reader that already opened it -- which is exactly the
// property openSharedRead asks for -- and rename(2) is required to be atomic and
// is unaffected by open descriptors on the destination, which is exactly what
// replaceFile asks for. Windows needs two extra calls to reach the same place.

import (
	"os"
	"path/filepath"
)

// openSharedRead opens path for reading without blocking a concurrent rename or
// unlink of it. See fileshare.go.
func openSharedRead(path string) (*os.File, error) { return os.Open(path) }

// replaceFile atomically replaces newpath with oldpath. See fileshare.go.
func replaceFile(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func dirOf(path string) string { return filepath.Dir(path) }
