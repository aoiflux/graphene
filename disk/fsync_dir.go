//go:build !windows && !plan9 && !js && !wasip1

package disk

// Directory fsync on the platforms that have it.
//
// On Linux and the BSDs, opening a directory read-only and calling fsync(2) on
// the descriptor is the documented way to make its entries durable; POSIX
// leaves the behaviour of fsync on a directory unspecified, but every kernel
// this file builds for implements it, and there is no other portable way to
// make a rename survive power loss.

import (
	"fmt"
	"os"
)

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	return f.Close()
}
