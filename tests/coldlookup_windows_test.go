//go:build stress && windows

package graphene_test

import (
	"fmt"
	"sync"
	"syscall"

	"github.com/aoiflux/graphene"
)

// Windows cannot make a file cold from user mode, and this file says so rather
// than producing a number that would have to be retracted.
//
// The instrument available without privilege is EmptyWorkingSet, which removes
// the process's pages from its working set. They do not leave memory: a clean
// file-backed page goes to the standby list, where the next touch is a soft
// fault -- a page-table repair, in the hundreds of nanoseconds -- and not a disk
// read in the hundreds of microseconds. So what this measures is the first touch
// after a trim, which is a *lower bound* on the cold cost and not the cold cost,
// and the column is headed "trimmed" for that reason. A reader comparing this
// against the linux column is comparing two different questions.
//
// Two other routes were considered and rejected. Opening the image a second time
// with FILE_FLAG_NO_BUFFERING does not purge the cache manager's view of it --
// the documentation warns that buffered and unbuffered access to one file are
// incoherent, which is a caution against relying on exactly the behaviour that
// would be needed here. SetSystemFileCacheSize does evict, and requires
// SE_INCREASE_QUOTA_NAME, which a test process does not have and should not
// acquire. Flooding physical memory to force eviction would work and would take
// the whole machine down with it for minutes.
//
// # What the trim costs that is not the store's
//
// EmptyWorkingSet is process-wide: it trims the Go heap and the runtime's own
// pages along with the mapped image, so the first pass after it pays soft faults
// for memory that has nothing to do with the read being measured. That inflates
// the trimmed column by a fixed amount rather than a proportional one, and it
// inflates a cheap operation's ratio more than an expensive one's. It is stated
// here because the alternative is a per-region trim that does not exist.
//
// The call is read back, on the principle the ceiling harness is built on: a
// trim that silently failed would leave every page resident and report a very
// encouraging ratio, and nothing in the output would say the instrument had not
// fired.
const coldEvictMethod = "trimmed"

const coldEvictNote = "EmptyWorkingSet moved the pages to the standby list, where the " +
	"next touch is a soft fault; this is a lower bound on the cold cost, not the cold cost"

const coldEvictHowTo = ""

var (
	emptyWSOnce sync.Once
	emptyWSProc *syscall.LazyProc
)

// coldEvictBeforeOpen is the linux hook. Trimming a working set the store is not
// in achieves nothing here, so this does nothing and says nothing: the label
// comes from coldEvictWhileOpen.
func coldEvictBeforeOpen(string) (string, error) { return "", nil }

func coldEvictWhileOpen(g *graphene.Graph) (string, error) {
	// Touch the image first, so there is a mapped view in the working set for the
	// trim to remove. Without this the store may not have faulted a single page
	// of the file in yet and the trim would be trimming nothing -- which the
	// before/after check below would then report as a failure, correctly but
	// unhelpfully.
	if _, err := g.NodeCount(); err != nil {
		return "", fmt.Errorf("touch before trim: %w", err)
	}

	before, ok := processMemoryInfo()
	if !ok {
		return "", fmt.Errorf("K32GetProcessMemoryInfo unavailable, so a trim " +
			"cannot be read back")
	}

	emptyWSOnce.Do(func() {
		emptyWSProc = syscall.NewLazyDLL("kernel32.dll").NewProc("K32EmptyWorkingSet")
	})
	if err := emptyWSProc.Find(); err != nil {
		return "", fmt.Errorf("K32EmptyWorkingSet: %w", err)
	}

	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return "", err
	}
	if ret, _, callErr := emptyWSProc.Call(uintptr(handle)); ret == 0 {
		return "", fmt.Errorf("EmptyWorkingSet: %w", callErr)
	}

	after, ok := processMemoryInfo()
	if !ok {
		return "", fmt.Errorf("K32GetProcessMemoryInfo failed after the trim")
	}
	if after.WorkingSetSize >= before.WorkingSetSize {
		return "", fmt.Errorf("the working set did not fall across EmptyWorkingSet "+
			"(%d -> %d bytes): the call reported success and did nothing, and the "+
			"pass after it would be warm under the trimmed heading",
			before.WorkingSetSize, after.WorkingSetSize)
	}
	return fmt.Sprintf("%s (%.1f -> %.1f MiB working set)", coldEvictMethod,
		float64(before.WorkingSetSize)/(1<<20), float64(after.WorkingSetSize)/(1<<20)), nil
}
