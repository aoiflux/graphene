package disk

// Process-level locking: what stops two processes from opening the same store
// and quietly destroying it.
//
// The in-process RWMutex on Store coordinates goroutines. Nothing coordinated
// processes, and the failure was not subtle: two processes both replay the log,
// both append to it, and both build a temp CSR and rename it over the live
// image. The second rename wins and the first process keeps serving a graph that
// no longer exists on disk. inspect.go and cmd/graphene had both grown prose
// warnings about this — enforcement is what those warnings wanted.
//
// # What is enforced
//
// One writer, many readers, across processes, on Windows, Linux and macOS:
//
//   - A writer takes an exclusive lock. A second writer is refused.
//   - A reader takes a shared lock. Any number coexist. A writer alongside them
//     is refused, and so is a reader alongside a writer.
//
// # What is not
//
// An OpenReadOnly reader's view is fixed at Open. The store materialises itself
// into memory once — the delta maps and property index come from a WAL replay,
// the CSR from one read or one mapping of graphene.csr — and nothing re-reads
// afterwards. A mapping does not weaken that: the engine never rewrites the image
// in place (§15.13), so the bytes behind it are the bytes that were there at
// Open. So a reader
// running alongside a writer would see a permanently stale graph with no
// indication that it was stale, which is worse than being refused. The shared
// lock exists for readers running alongside *other readers*; the exclusive lock
// is what keeps a writer from being one of them. Reopen to advance.
//
// # The third mode
//
// Making readers live turned out to be a locking change as well as a protocol —
// this comment used to say it was not, and it was wrong. A shared lock and an
// exclusive lock cannot coexist, so a reader that runs beside the writer cannot
// hold a shared lock, whatever protocol it speaks. OpenLive therefore takes
// LockNone: it neither excludes the writer nor is excluded by it, and gives up
// the "no writer is running" guarantee to do so. The writer's exclusive lock is
// untouched, so "one writer" still holds. See live.go for the trade and the
// protocol.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// lockFileName is the coordination file. It holds no store data: deleting it
// while nothing is running loses nothing but the clean-shutdown marker.
const lockFileName = "graphene.lock"

// lockByteOffset is where the locked range lives, far past any record this file
// will ever hold.
//
// It is out here because a Windows LockFileEx range is *mandatory* for I/O, not
// advisory: a process that does not hold the lock gets ERROR_LOCK_VIOLATION
// trying to read bytes inside it. Locking byte zero would therefore make the
// owner record unreadable by exactly the process that most wants to read it —
// the one being refused, which wants to name the holder in its error. Locking a
// byte nothing ever writes keeps the record at offset 0 readable by anyone.
//
// Locking a range past EOF is legal on both platforms and does not extend the
// file, so graphene.lock stays 32 bytes long.
const lockByteOffset = 1 << 40

// LockMode is the kind of process-level lock a store holds.
type LockMode uint8

const (
	// LockExclusive admits one holder and excludes every other process. This is
	// the zero value because it is what Open takes: a store opened for writing
	// that failed to exclude other writers would be the dangerous default.
	LockExclusive LockMode = iota

	// LockShared admits any number of holders and excludes every exclusive one.
	// OpenReadOnly takes it.
	LockShared

	// LockNone takes no operating-system lock at all. OpenLive takes it.
	//
	// This is not "locking is unavailable here" — that is what
	// lock_unsupported.go is for, and it is a property of the platform. This is a
	// caller asking, on a platform that locks perfectly well, not to participate:
	// a live reader has to run alongside the writer's exclusive lock, and a
	// shared lock cannot. The writer's exclusive lock is deliberately left as it
	// is, so what is given up is the reader's "no writer is running" guarantee
	// and nothing on the writing side. See live.go.
	//
	// The lock file is not opened either. This comment used to say it was, so
	// that a live reader could report who holds the store — but the owner record
	// is read only for a writer's unclean-shutdown check, and a live reader is
	// read-only by construction, so nothing ever consumed it. What opening it did
	// do was create a file in a directory this mode promises not to write to, and
	// hold a handle that on Windows stops anyone deleting it.
	LockNone
)

func (m LockMode) String() string {
	switch m {
	case LockExclusive:
		return "exclusive"
	case LockShared:
		return "shared"
	case LockNone:
		return "none"
	default:
		return fmt.Sprintf("LockMode(%d)", uint8(m))
	}
}

// ErrStoreLocked reports that another process holds the store in a conflicting
// mode. The wrapped message names the holder's process ID when the lock file
// records one.
//
// Acquisition never blocks and never retries. How long to wait for a store, and
// whether to wait at all, depends on what the caller is — a CLI that should
// print and exit, an ingest worker that should back off, a service that should
// fail its health check — and the engine has no basis to choose for them. The
// same reasoning keeps Retention and RedactionPolicy out of the engine's hands.
var ErrStoreLocked = errors.New("disk: store is held by another process")

// ErrReadOnly reports a mutation attempted on a store opened with
// Options.ReadOnly.
var ErrReadOnly = errors.New("disk: store is open read-only")

// --- the owner record ---

const (
	lockOwnerSize    = 32
	lockOwnerVersion = 1
)

// lockOwnerMagic tags the record so a foreign or truncated file is ignored
// rather than parsed into a plausible-looking owner.
var lockOwnerMagic = [4]byte{'G', 'L', 'K', '1'}

// lockOwner is what the last exclusive holder recorded about itself.
//
// Only an exclusive holder writes it, which is what makes it meaningful: a
// shared holder cannot claim the store, so recording one would produce an owner
// that never owned anything. Readers of the record — including a process that
// was just refused — read it without any lock, because it sits outside the
// locked range.
type lockOwner struct {
	// Present is false when the file held no readable record: a store that has
	// never been opened for writing, or one whose lock file was replaced.
	Present bool

	// PID is the recorded process ID. It is a diagnostic, not an authority — the
	// process may be long gone, and on a reused PID it may name something else
	// entirely. The lock itself is held by the OS, which is what actually
	// decides.
	PID uint64

	// Clean is true when that holder ran markClean, which Close does before it
	// releases. False means the process died, was killed, or exited without
	// closing the store.
	Clean bool
}

func appendLockOwner(dst []byte, o lockOwner) []byte {
	var rec [lockOwnerSize]byte
	copy(rec[0:4], lockOwnerMagic[:])
	rec[4] = lockOwnerVersion
	binary.LittleEndian.PutUint64(rec[8:16], o.PID)
	if o.Clean {
		rec[16] = 1
	}
	return append(dst, rec[:]...)
}

func parseLockOwner(b []byte) (lockOwner, bool) {
	if len(b) < lockOwnerSize {
		return lockOwner{}, false
	}
	if string(b[0:4]) != string(lockOwnerMagic[:]) || b[4] != lockOwnerVersion {
		return lockOwner{}, false
	}
	return lockOwner{
		Present: true,
		PID:     binary.LittleEndian.Uint64(b[8:16]),
		Clean:   b[16] == 1,
	}, true
}

// readLockOwner reads the record without holding the lock. Safe on both
// platforms: the record is outside the locked byte range, and it is written in
// one 32-byte WriteAt, so a concurrent reader sees the old record or the new one
// and never a mixture.
func readLockOwner(f *os.File) lockOwner {
	var buf [lockOwnerSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return lockOwner{}
	}
	o, ok := parseLockOwner(buf[:])
	if !ok {
		return lockOwner{}
	}
	return o
}

// peekLockOwner reads the record from a store directory without opening or
// locking anything, so a process that was just refused can say who refused it.
// An unreadable file yields a zero owner, which prints as "another process".
func peekLockOwner(dir string) lockOwner {
	f, err := os.Open(filepath.Join(dir, lockFileName))
	if err != nil {
		return lockOwner{}
	}
	defer f.Close()
	return readLockOwner(f)
}

// sharedLockAvailable reports whether a shared lock on dir can be taken right
// now, releasing it immediately. Used only to work out what kind of holder
// refused us.
func sharedLockAvailable(dir string) bool {
	f, err := os.OpenFile(filepath.Join(dir, lockFileName), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return false
	}
	defer f.Close()
	if err := lockFile(f, LockShared); err != nil {
		return false
	}
	unlockFile(f)
	return true
}

// lockedBy renders ErrStoreLocked, naming the holder only when it can do so
// truthfully.
//
// The owner record is written by exclusive holders and nobody else, so it cannot
// simply be read out and printed: a store held by three readers would be
// reported as belonging to whichever writer last touched it, which is very
// likely a process that exited hours ago. An operator sent after a dead PID is
// worse off than one told only that the store is busy.
//
// Two facts make the answer exact. A shared lock coexists with other shared
// locks and with nothing else, so if one can be taken, every holder is a reader.
// And a writer that is still holding the store has not run markClean, so its
// record is dirty — which distinguishes a live writer from the residue of a
// finished one.
func lockedBy(dir string, want LockMode) error {
	// Being refused a *shared* lock already proves a writer holds it; only an
	// exclusive request needs the probe to tell readers from a writer.
	if want == LockExclusive && sharedLockAvailable(dir) {
		return fmt.Errorf("%w: one or more readers hold it (wanted exclusive access to %s)",
			ErrStoreLocked, dir)
	}

	// A writer holds it. Name it if its record says so and is dirty; a clean
	// record belongs to a writer that has already gone, so it names nobody
	// relevant.
	//
	// The probe above races a writer releasing between the two calls, which can
	// land here with nothing holding the store at all. That costs an error string
	// that is a moment out of date, on a path that has already failed — not worth
	// a lock to close.
	if o := peekLockOwner(dir); o.Present && !o.Clean && o.PID != 0 {
		return fmt.Errorf("%w: pid %d holds it for writing (wanted %s access to %s)",
			ErrStoreLocked, o.PID, want, dir)
	}
	return fmt.Errorf("%w (wanted %s access to %s)", ErrStoreLocked, want, dir)
}

// --- the lock itself ---

type storeLock struct {
	file     *os.File
	mode     LockMode
	released atomic.Bool
}

// acquireStoreLock takes the process-level lock on dir and returns it along with
// what the previous exclusive holder recorded about itself.
//
// The returned lockOwner describes the *previous* holder, read before this one
// overwrites it. A record that is present but not clean is how an unclean
// shutdown is detected; see Store.RecoveredFromUncleanShutdown.
func acquireStoreLock(dir string, mode LockMode) (*storeLock, lockOwner, error) {
	path := filepath.Join(dir, lockFileName)

	// LockNone does not open the file at all. It is the one mode with nothing to
	// lock, so the file would exist only to be held: the previous-holder record
	// is read for a writer's unclean-shutdown check and nowhere else, and a live
	// reader is read-only by construction. Opening it would mean creating a file
	// in a directory this mode promises not to write to, and — on Windows —
	// holding a handle that stops anyone deleting it, which is a foothold in the
	// store for a mode that is supposed to leave no trace.
	if mode == LockNone {
		return &storeLock{mode: mode}, lockOwner{}, nil
	}

	// O_RDWR|O_CREATE in both remaining modes. A shared holder writes nothing,
	// but it does need the file to exist in order to lock it, and a store that
	// has only ever been read has no lock file yet. Creating one is not a write
	// to any *store* file — graphene.lock holds no graph data — but it does mean
	// read-only mode needs a writable directory. A genuinely read-only medium is
	// out of reach for this design, and failing there is better than pretending
	// the lock was taken.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, lockOwner{}, fmt.Errorf("disk: lock file: %w", err)
	}

	if err := lockFile(f, mode); err != nil {
		f.Close()
		if errors.Is(err, ErrStoreLocked) {
			return nil, lockOwner{}, lockedBy(dir, mode)
		}
		return nil, lockOwner{}, fmt.Errorf("disk: locking %s: %w", path, err)
	}

	// Read the previous record before overwriting it — this read is the only
	// chance to learn whether the last writer closed cleanly.
	prev := readLockOwner(f)

	l := &storeLock{file: f, mode: mode}
	if mode == LockExclusive {
		if err := l.write(false); err != nil {
			l.release()
			return nil, lockOwner{}, err
		}
	}
	return l, prev, nil
}

// write records this holder, clean or not, in a single 32-byte WriteAt so a
// concurrent unlocked reader never sees a torn record.
func (l *storeLock) write(clean bool) error {
	rec := appendLockOwner(nil, lockOwner{PID: uint64(os.Getpid()), Clean: clean})
	if _, err := l.file.WriteAt(rec, 0); err != nil {
		return fmt.Errorf("disk: lock file: %w", err)
	}
	// Synced, because the whole value of the marker is that it survives the crash
	// it exists to describe. Left in the page cache, the dirty flag written at
	// open could be lost in the very crash it was meant to record, and the next
	// open would read a stale clean flag and report a crash as an orderly exit.
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("disk: lock file sync: %w", err)
	}
	return nil
}

// markClean records that this holder is shutting down deliberately. Close calls
// it immediately before releasing; a process that dies never does, which is what
// leaves the record dirty for the next opener to find.
//
// A shared holder never wrote a record and must not write one now.
func (l *storeLock) markClean() error {
	if l == nil || l.mode != LockExclusive || l.released.Load() {
		return nil
	}
	return l.write(true)
}

// release drops the lock and closes the file. Idempotent and nil-safe: Close is
// reached both by defer and explicitly in places, and releasing a lock twice
// would otherwise turn a harmless double Close into an error.
func (l *storeLock) release() error {
	if l == nil || l.released.Swap(true) {
		return nil
	}
	if l.mode == LockNone {
		// Nothing was locked and nothing was opened, so there is nothing to undo.
		return nil
	}
	err := unlockFile(l.file)
	if cerr := l.file.Close(); err == nil {
		err = cerr
	}
	return err
}
