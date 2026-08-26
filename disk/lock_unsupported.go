//go:build !windows && !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package disk

// Platforms with no file-locking primitive in the standard library: solaris,
// aix, plan9, js/wasm. Go's syscall package exposes Flock on the BSDs, Linux and
// macOS, and nothing equivalent on these.
//
// The store still opens here. Refusing would make the engine unusable on a
// platform for the sake of a guarantee that platform cannot offer anyway, and
// the WAL and CSR formats are portable — someone reading a store on one of these
// has a real use case that a hard failure would break for no gain.
//
// What it must not do is pretend. Store.LockEnforced reports false, and callers
// who care can check it. That is the whole of the difference: the API is
// identical, and the single word it returns is where the honesty lives.

import "os"

// lockingEnforced reports whether this platform actually takes the lock.
const lockingEnforced = false

func lockFile(*os.File, LockMode) error { return nil }

func unlockFile(*os.File) error { return nil }
