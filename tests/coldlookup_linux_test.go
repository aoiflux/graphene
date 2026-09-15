//go:build stress && linux

package graphene_test

import (
	"fmt"
	"os"
	"syscall"

	"github.com/aoiflux/graphene"
)

// Linux is the platform where "cold" means what it says.
//
// posix_fadvise(POSIX_FADV_DONTNEED) drops a file's clean page-cache pages
// outright: the next read is a disk read, not a soft fault. Nothing in this
// module uses fadvise, so this is a clean addition rather than a second copy of
// an existing wrapper, and it goes through syscall.Syscall6 because the
// zero-dependency invariant rules out golang.org/x/sys -- go.mod is 44 bytes and
// a test asserts it.
//
// It must run with the store closed, and that is not a convenience. DONTNEED
// does not discard pages that are mapped into a process's address space, so
// calling it on an image the store has mapped would return success having
// dropped nothing, and the pass after it would report a warm figure under the
// cold heading. That is the same failure this harness exists to avoid on the
// other platform, and it would be harder to spot here because the label would be
// telling the truth about the instrument and not about the result.
const coldEvictMethod = "evicted"

const coldEvictNote = "posix_fadvise(POSIX_FADV_DONTNEED) dropped the image's clean " +
	"page-cache pages, so the first pass read from the device"

const coldEvictHowTo = ""

// posixFadvDontNeed is POSIX_FADV_DONTNEED. The value is 4 on every linux
// architecture this builds for.
const posixFadvDontNeed = 4

func coldEvictBeforeOpen(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// A file the kernel has dirty pages for keeps them: DONTNEED writes nothing
	// back. The image is read-only here and was written by an earlier process, so
	// a sync is belt and braces -- but an un-synced page that survives the advice
	// is a warm page the label would call cold.
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("sync before fadvise: %w", err)
	}

	// offset 0, len 0 == the whole file.
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(),
		0, 0, posixFadvDontNeed, 0, 0)
	if errno != 0 {
		return "", fmt.Errorf("posix_fadvise(DONTNEED): %w", errno)
	}
	return coldEvictMethod, nil
}

// coldEvictWhileOpen is the windows hook, and there is nothing for it to do
// here: the eviction already happened, with the store closed, which is the only
// order in which it works.
func coldEvictWhileOpen(*graphene.Graph) (string, error) { return "", nil }
