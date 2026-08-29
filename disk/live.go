package disk

// Live readers: a read-only store that can be told to advance.
//
// # What was missing, and what it costs
//
// OpenReadOnly fixes its view at the moment of the call. The store materialises
// itself once -- the delta from a WAL replay, the image from one os.ReadFile --
// and nothing re-reads afterwards, so a reader running beside a writer would
// serve a permanently stale graph with no sign that it was stale. The shared
// process lock is what stopped that: a reader alongside a writer is refused, so
// the staleness cannot arise.
//
// Making a reader live therefore costs the lock, and the trade is stated rather
// than smuggled in. A shared lock and an exclusive lock cannot coexist -- that
// is what the primitive means -- so one of the two sides has to give:
//
//   - the writer stops taking exclusive, which gives up "one writer", the
//     guarantee that stops two processes replaying and rewriting the same store;
//   - or the reader stops taking shared, which gives up "no writer is running",
//     the guarantee OpenReadOnly documents.
//
// The second is the one taken here. The writer's exclusive lock is untouched, so
// two writers are still impossible and every existing guarantee about writing
// survives; a live reader simply does not claim the directory. It takes no OS
// lock at all (LockNone), which is also why any number of live readers coexist
// with each other and with an ordinary OpenReadOnly.
//
// OpenReadOnly is unchanged. This is a third mode, not a relaxation of the
// second, and a caller who wants "no writer is running" still gets exactly that
// by not asking for this one.
//
// # How advancing works
//
// Two numbers make it a resumable read rather than a reopen.
//
// The log generation is the container header's SegmentSeq, which every
// operation that ends the current log file now increments (WAL.logGen). It
// answers "is the file I have byte offsets into still the file at this path?"
//
// The replay offset is how far into the records region this reader has applied,
// as reported by replayRecordsFrom -- which rewinds to the last point at which
// no batch was open, so a resume never begins in the middle of a transaction the
// writer had not finished.
//
// Same generation: replay the bytes that were appended and publish the epoch
// they reached. Different generation: the log was truncated, rebuilt or rotated,
// so every offset into it is meaningless and the image behind it has very likely
// been replaced too -- rebuild from the files as they now are.
//
// # Why a changed image alone does not need a reload
//
// The third retire branch leaves the log alone (see retireLog), so a compaction
// can replace the image without changing the generation. A reader that keeps its
// old image is still correct there, and this is not a lucky accident: the log was
// not truncated, so it still holds every record the new image folded in, and the
// reader has already applied them. Old image plus every record ever seen is the
// same graph as new image plus the same records. The cost is that such a reader
// keeps a larger delta until a generation does change, which is a memory
// question, not a correctness one.
//
// # What a live reader still does not promise
//
// It advances when Refresh is called and at no other time; there is no polling
// goroutine, because how often to look is the caller's decision for the same
// reason waiting for a lock is (see ErrStoreLocked). Between refreshes it is
// exactly as fixed as OpenReadOnly. And it reads only what the writer has made
// durable: a commit still inside the writer's group-commit gate is not in the
// log yet, so a live reader is behind the writer by at most one fsync -- which
// is the durability boundary doing its job rather than a limitation of this.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// ErrNotLiveReader is returned by Refresh on a store that was not opened with
// Options.LiveReader.
//
// Deliberately an error rather than a no-op. A caller refreshing a store that
// cannot advance is reading stale data believing it is fresh, which is the exact
// failure this mode exists to remove, and silently succeeding would hide it.
var ErrNotLiveReader = errors.New("disk: store was not opened as a live reader")

// OpenLive opens dir for querying by a reader that can advance.
//
// Unlike OpenReadOnly it takes no process lock, so it neither excludes a writer
// nor is excluded by one -- see the file comment for what that trades and why
// this is a third mode rather than a change to the second. Every mutating method
// returns ErrReadOnly and nothing under dir is modified.
//
// The view is fixed until Refresh is called.
func OpenLive(dir string) (*Store, error) {
	return OpenWithOptions(dir, Options{LiveReader: true})
}

// IsLiveReader reports whether this store can be advanced with Refresh.
func (s *Store) IsLiveReader() bool { return s.live }

// Refresh advances a live reader to the newest durable state of the store.
//
// It is safe to call concurrently with reads: the whole of the work happens
// under the store's write lock, and the image, the delta and the property index
// are published as one value, so a query either sees all of the new state or all
// of the old.
//
// A failed refresh leaves the reader exactly as it was, still serving the graph
// it was serving before the call. That matters more here than it usually does --
// a reload throws away every in-memory structure before rebuilding it, so
// without the restore a transient read error would leave a store that answers
// every query with an empty graph rather than an error.
func (s *Store) Refresh() (store.RefreshInfo, error) {
	if !s.live {
		return store.RefreshInfo{}, ErrNotLiveReader
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := liveLogPath(s.dir)
	header, dataStart, err := peekWALHeader(path)
	if err != nil {
		return store.RefreshInfo{}, fmt.Errorf("disk.Refresh: %w", err)
	}

	// Four things can invalidate the offset this reader holds, and all four
	// mean the same thing: start over. The generation changes when the log file
	// is replaced; the framing changes when a pre-container log is upgraded by
	// the compaction that replaced it; the path changes when the reader is
	// reading the rebuilt log through its temporary name, inside the window
	// described in fileshare.go.
	//
	// And the log can be *shorter* than the offset the reader is resuming from,
	// which the generation does not catch, because the file that replaced it may
	// carry a generation this reader has already seen. That is what an operator
	// putting a restored directory in place under a follower produces. Resuming
	// past the end of a file reads nothing, forever, so the reader would go on
	// serving a store that no longer exists and report every refresh as a
	// successful no-op. Shrinking is the one thing a log never does on its own:
	// every writer path only appends, and the paths that do make it shorter --
	// Truncate, truncateFrom, Rotate -- all change the generation. So a log
	// shorter than the reader's offset is by construction a different log.
	if header.SegmentSeq != s.logGen || header.Version != s.wal.Framing() || path != s.logPath ||
		s.logShrank(path, dataStart) {
		return s.reloadLive(path, header)
	}
	return s.advanceLive()
}

// logShrank reports a log with fewer bytes in it than the reader has already
// read, which means the file is not the one those bytes came from.
//
// A stat that cannot be taken is not treated as a shrink: the header was read
// from this path a moment ago, so a failure here is a transient filesystem
// error rather than evidence about the log, and answering "reload" to it would
// throw away the reader's whole state over a hiccup.
func (s *Store) logShrank(path string, dataStart int64) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Size()-dataStart < s.replayOff
}

// advanceLive applies the bytes appended since the last refresh.
func (s *Store) advanceLive() (store.RefreshInfo, error) {
	before := s.replayOff
	off, err := s.wal.replayFrom(s.replayOff, s.replayCallbacks())
	// The offset advances even on an error: replayRecordsFrom reports the last
	// clean boundary it reached, and the records before it were applied. Not
	// keeping it would replay them again on the next call, which for a log that
	// is genuinely damaged means applying its good prefix once per refresh
	// forever.
	s.replayOff = off
	s.publishEpoch(s.mutEpoch.Load())
	if err != nil {
		return store.RefreshInfo{}, fmt.Errorf("disk.Refresh: %w", err)
	}
	return store.RefreshInfo{
		Epoch:         s.visibleEpoch.Load(),
		LogGeneration: s.logGen,
		Bytes:         off - before,
	}, nil
}

// reloadLive rebuilds the whole reader from the files as they are now.
//
// Everything the store serves from is replaced: the image, the delta layer and
// the property index. They are published as one view, which is what makes the
// swap invisible to a concurrent query -- see view.idx.
func (s *Store) reloadLive(path string, header walFileHeader) (store.RefreshInfo, error) {
	prevView := s.viewPtr.Load()
	prevIdx := s.propIdx
	prevShadow := s.csrShadowed.Load()
	prevWAL := s.wal
	prevPath := s.logPath
	prevGen := s.logGen
	prevOff := s.replayOff

	restore := func() {
		s.propIdx = prevIdx
		s.viewPtr.Store(prevView)
		s.csrShadowed.Store(prevShadow)
		s.wal = prevWAL
		s.logPath = prevPath
		s.logGen = prevGen
		s.replayOff = prevOff
	}

	// The epochs are deliberately not rewound. A version's epoch is only ever
	// compared with other epochs in the same view, so the rebuilt delta numbering
	// its versions from wherever the counter stands is correct -- and rewinding
	// would make publishEpoch, which only advances, refuse the result.
	wal, err := openWALFor(path, true)
	if err != nil {
		return store.RefreshInfo{}, fmt.Errorf("disk.Refresh: %w", err)
	}

	s.propIdx = index.NewPropertyIndex()
	s.publishView(&view{delta: newDeltaLayer()})
	s.wal = wal
	s.logPath = path
	s.logGen = header.SegmentSeq
	s.replayOff = 0

	fail := func(err error) (store.RefreshInfo, error) {
		wal.Close()
		restore()
		return store.RefreshInfo{}, fmt.Errorf("disk.Refresh: %w", err)
	}

	csrPath := filepath.Join(s.dir, csrFileName)
	if _, serr := os.Stat(csrPath); serr == nil {
		if lerr := s.loadCSR(csrPath); lerr != nil {
			return fail(fmt.Errorf("load CSR: %w", lerr))
		}
	}

	off, rerr := s.wal.replayFrom(0, s.replayCallbacks())
	if rerr != nil {
		return fail(fmt.Errorf("replay WAL: %w", rerr))
	}
	s.replayOff = off
	s.publishEpoch(s.mutEpoch.Load())

	prevWAL.Close()
	return store.RefreshInfo{
		Epoch:         s.visibleEpoch.Load(),
		LogGeneration: s.logGen,
		Reloaded:      true,
		Bytes:         off,
	}, nil
}

// liveLogPath names the file a live reader should read the log from.
//
// Almost always the log itself. The exception is the window inside replaceFile
// on Windows, where the rebuilt log exists only under its temporary name: the
// file there is complete and fsynced, so reading it is reading the log the
// writer is in the act of installing. Without this the reader would see a
// missing log, take it for an empty one, and briefly serve a graph missing the
// commits the rebuild was carrying -- a view that loses content, which nothing
// else in the engine can do.
func liveLogPath(dir string) string {
	p := filepath.Join(dir, walFileName)
	if _, err := os.Stat(p); err == nil {
		return p
	}
	if _, err := os.Stat(p + walTmpSuffix); err == nil {
		return p + walTmpSuffix
	}
	return p
}
