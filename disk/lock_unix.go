//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package disk

// The Unix half of the process lock: flock(2).
//
// # Why flock and not fcntl
//
// POSIX fcntl locks are owned by the *process*, and that ownership model breaks
// both halves of what this lock is for:
//
//   - A second Open in the same process would succeed silently, because the
//     kernel merges the two requests into one lock the process already holds. A
//     store can be destroyed by one process opening it twice just as thoroughly
//     as by two processes opening it once, so that has to be caught too.
//   - Closing *any* descriptor to the file drops every lock the process holds on
//     it. Some unrelated code path that stats the lock file through its own
//     handle and closes it would silently unlock the store.
//
// flock locks are owned by the open file description instead, so two Opens hold
// two independent locks that conflict with each other, and closing one handle
// releases only that handle's lock. That is the ownership model this needs.
//
// The cost is NFS: on Linux, flock over NFS has been mapped onto whole-file
// fcntl locks since 2.6.37, and on older kernels or other clients it may be
// local-only. A store on a network filesystem is outside what this engine
// promises for durability anyway — the WAL's fsync contract is no better there.
//
// flock is advisory, so nothing here blocks reads or writes of the owner record.

import (
	"errors"
	"os"
	"syscall"
)

// lockingEnforced reports whether this platform actually takes the lock.
const lockingEnforced = true

func lockFile(f *os.File, mode LockMode) error {
	how := syscall.LOCK_EX
	if mode == LockShared {
		how = syscall.LOCK_SH
	}
	// LOCK_NB: never block. See ErrStoreLocked for why the engine does not own a
	// retry policy.
	err := syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EAGAIN):
		// EWOULDBLOCK and EAGAIN are the same number on Linux and on the BSDs,
		// but that is a platform detail rather than a guarantee, and matching
		// only one of them would turn contention into an opaque open failure.
		return ErrStoreLocked
	default:
		return err
	}
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
