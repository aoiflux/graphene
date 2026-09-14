package disk

// The image as a mapping instead of a copy.
//
// # What this buys and what it costs
//
// Opening a store used to read graphene.csr into the heap with one os.ReadFile
// and then copy every property blob out of that buffer into an arena the
// CSRGraph keeps for its life. So a 1.2 GiB image cost a 1.2 GiB transient, a
// second 1.2 GiB of arenas that never went away, and a peak of both at once.
// The bytes were already contiguous, already pointer-free, and already in
// exactly the form a reader wants: a copy into anonymous memory bought nothing
// but the copy.
//
// A mapping gives the same []byte without either. The records' Properties
// slices address the mapped file directly, so the blob half of an image stops
// being heap the collector has to keep and becomes page cache the kernel is
// free to evict. What remains on the heap is the record arenas, the label
// arena, the adjacency and the label postings — everything that is a decoded
// structure rather than a byte image. See TECHNICAL_DETAILS §14.18.
//
// Labels deliberately stay on the heap. A label is a uint16 and the record
// stores it at offset +9, so the on-disk stream is unaligned and reading it as
// []store.NodeType would need an unsafe cast this package will not make for
// three megabytes at a million and a half nodes.
//
// # The lifetime problem, and why it is smaller than it looks
//
// A mapped slice handed to a caller is only valid while the mapping is. The
// obvious design — unmap the image a compaction replaced — is wrong here, and
// finding out why decided the shape of this file.
//
// A compaction builds the new image out of the old one's records, and those
// records carry slice headers. So the graph a compaction publishes addresses
// the *previous* image's bytes for every record the delta did not touch. The
// one after that addresses them too, because it was built from records that
// already did. Unmapping at a compaction would therefore be a use-after-unmap
// on the live graph, and pinning each mapping until its dependents were gone
// would pin the first one forever while accumulating one per compaction.
//
// So: a mapping created at Open lives until Close. Compaction creates none and
// retires none — it writes a file and goes on reading blobs from the mapping it
// already had. That makes the contract on a returned Properties or Labels slice
// the one it has always been (valid for the life of the handle), with one
// sentence added: not after Close. It also means a store holds exactly one
// image mapping no matter how often it compacts.
//
// The exception is a live reader, which rebuilds from the files on a Refresh
// that crosses a compaction and therefore does create a second mapping. That is
// what retirable, the cleanup, and sweepImages below are for, and it is the
// only path that uses them. A live reader takes no process lock, so it is also
// the one mode that is not mapped by default — see ImageMode.
//
// # Why the retirement test is reachability and not a reference count
//
// A point read from the image costs about six nanoseconds and holds no lock. An
// atomic increment and decrement around it to count references would be a
// larger cost than the read. So the test is the garbage collector's: a cleanup
// on the CSRGraph marks its mapping retirable once that graph is unreachable —
// which is once viewPtr, every snapshot holding it, every in-flight lock-free
// reader's local, and any compaction plan pinning it have all let go. The
// mapping is then unmapped at the next sweep.
//
// This is a floor on the truth, not the truth: the collector does not trace the
// record slices that address the mapping, because mapped memory is not in the
// heap. So the grace period is what the sweep points give, and the contract
// documented on ImageMappedUnlocked is deliberately the conservative reading of
// it.

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync/atomic"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// ImageMode decides whether Open maps graphene.csr or reads it into the heap.
type ImageMode uint8

const (
	// ImageMapped maps the image when the platform supports it and the store
	// holds a process lock. This is the default and the zero value.
	//
	// The lock is part of the condition rather than an unrelated setting,
	// because a mapping is only as stable as the file under it. An exclusive or
	// shared lock is what makes "nothing else is rewriting graphene.csr" true,
	// and every open mode that takes one is mapped: a writer, and OpenReadOnly.
	// A live reader takes no lock, so under this mode it reads into the heap;
	// ImageMappedUnlocked is how to ask for it anyway.
	//
	// Falls back to the heap, with store.MetricImageFallback and
	// StorageStats.ImageMode reporting it, when the platform has no mapping
	// primitive, when the file is empty or larger than an int, or when the
	// mapping syscall fails. The fallback is always correct and never silent.
	ImageMapped ImageMode = iota

	// ImageHeap reads the image with one os.ReadFile and copies every property
	// blob into an arena, which is what every version before this one did.
	//
	// Choose it to keep a returned Properties slice independent of any file:
	// under it the bytes a read hands back are ordinary heap, so they survive
	// Close and survive anything done to the directory afterwards. That costs
	// the whole blob half of the image in anonymous memory, twice at the peak.
	ImageHeap

	// ImageMappedUnlocked maps the image even where nothing excludes a
	// concurrent writer from the directory: a live reader, or a platform whose
	// standard library offers no file locking.
	//
	// Two things change and both are documented rather than mitigated.
	//
	// A mapped image is exposed to the directory being mutated underneath it.
	// The engine never rewrites graphene.csr in place — it writes a temporary
	// file and renames — so nothing the engine does can reach a live mapping.
	// Something outside it can: truncating the file kills the process with
	// SIGBUS on Unix (Windows refuses the truncation outright, so there is no
	// exposure there), and overwriting bytes in place changes what the mapping
	// reads, on every platform. §15 states this as an invariant.
	//
	// And a live reader that Refreshes across a compaction maps the new image,
	// so its mappings do get retired. A Properties or Labels slice from image N
	// is valid until the second such reload after it; store.CloneNode and
	// store.CloneEdge are for a caller that keeps records across a Refresh.
	ImageMappedUnlocked
)

// String names the mode for StorageStats and for an error message.
func (m ImageMode) String() string {
	switch m {
	case ImageMapped:
		return "mapped"
	case ImageHeap:
		return "heap"
	case ImageMappedUnlocked:
		return "mapped-unlocked"
	}
	return fmt.Sprintf("ImageMode(%d)", uint8(m))
}

// errImageMappingUnsupported is what a platform with no mapping primitive in the
// standard library reports. Not an open failure: the caller falls back.
var errImageMappingUnsupported = errors.New("this platform has no file mapping in the standard library")

// errImageMappingNotAllowed is the policy half of the same thing — the platform
// could map, and the mode says not to here.
var errImageMappingNotAllowed = errors.New("mapping needs a process lock; see ImageMappedUnlocked")

// mapping is one mapped image file.
//
// Created by mapImage and released by close, exactly once. The data slice
// addresses memory the kernel owns, so nothing in it is visible to the garbage
// collector and nothing about it may be inferred from reachability — see the
// file comment.
type mapping struct {
	// data is the whole file. Immutable for the life of the mapping as far as
	// the engine is concerned; see ImageMappedUnlocked for who else can reach
	// it.
	data []byte

	// path is the file this was mapped from, for diagnostics only. It may no
	// longer name this file: a compaction renames over it.
	path string

	// retirable is set by the cleanup on the graph this mapping was attached to,
	// once that graph is unreachable. Read by sweepImages.
	//
	// One graph, one mapping: a mapping is attached exactly once, in noteImage,
	// and a compaction attaches nothing because the graph it publishes is not
	// parsed from a file. An earlier version counted attachments so that several
	// graphs could share a mapping, and the mutation pass showed the counter was
	// never anything but one -- removing it changed nothing any test could
	// observe, which for a counter is the definition of not counting. If a second
	// attachment ever becomes possible, the count comes back with a test that
	// fails without it.
	retirable atomic.Bool

	// unmapped makes close idempotent, so a sweep racing Close cannot release
	// the same region twice.
	unmapped atomic.Bool

	// release is the platform half: munmap, or UnmapViewOfFile plus the section
	// handle.
	release func() error
}

// maxMappableBytes is the largest file this will map.
//
// A mapping is addressed through a Go slice, whose length is an int, so on a
// 32-bit build the limit is the address space rather than a policy. Stated as a
// constant so the fallback has something to name.
const maxMappableBytes = int64(math.MaxInt)

// mapImage maps path read-only and whole.
//
// The file handle is closed before this returns, and on Windows that is
// load-bearing rather than tidy. A section object created from a handle opened
// with FILE_SHARE_DELETE keeps the bytes reachable on its own, and with the
// handle gone the name is free: measured on Windows 11, renaming the mapped file
// away, renaming another file over it, and deleting it all succeed, and the
// mapping goes on reading the bytes it mapped. Keeping the handle open instead
// makes the rename in compactCommit fail with a sharing violation. So both
// platforms behave the way Unix always did, and there is no Windows-specific
// install sequence in compact.go. See fileshare.go for the same measurement
// applied to the log.
func mapImage(path string) (*mapping, error) {
	f, err := openSharedRead(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return mapOpenImage(f, path)
}

// mapOpenImage is mapImage from a handle the caller already holds.
//
// It exists because opening a file is not cheap and opening one that was just
// written is worse: measured on Windows 11, the single extra CreateFile a
// compaction needed to map the image it had just written was 28% of the whole
// compaction's CPU at a thousand nodes -- the file is new, and the first open
// after a write is where a real-time scanner reads it. The handle that wrote it
// is open, has read access, and is about to be closed for nothing.
//
// The caller keeps the handle and closes it; the mapping does not need it. See
// mapImage for why that is safe on Windows as well as Unix.
func mapOpenImage(f *os.File, path string) (*mapping, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	if size <= 0 {
		return nil, fmt.Errorf("mapping %s: the file is empty", path)
	}
	if size > maxMappableBytes {
		return nil, fmt.Errorf("mapping %s: %d bytes is more than this build can address", path, size)
	}

	data, release, err := mapFile(f, int(size))
	if err != nil {
		return nil, fmt.Errorf("mapping %s: %w", path, err)
	}
	return &mapping{data: data, path: path, release: release}, nil
}

// close releases the mapping. Idempotent.
func (m *mapping) close() error {
	if m == nil || !m.unmapped.CompareAndSwap(false, true) {
		return nil
	}
	return m.release()
}

// attach records that csr's records address m's bytes, and arranges for that to
// be noticed when csr becomes unreachable.
//
// The cleanup deliberately takes m as its argument and csr does not hold m:
// runtime.AddCleanup never runs a cleanup whose argument is reachable from the
// object it watches, which is the whole reason CSRGraph carries the mapped size
// rather than the mapping. The store's own images slice is what keeps m alive.
func (m *mapping) attach(csr *CSRGraph) {
	if m == nil || csr == nil {
		return
	}
	runtime.AddCleanup(csr, func(m *mapping) { m.retirable.Store(true) }, m)
}

// imageSource is graphene.csr's bytes and, when they are mapped, the mapping
// that owns them.
//
// It exists so that VerifyOnOpen and the load can share one read of the image.
// Each used to open the file for itself: verification read it whole to hash it
// and again to parse it, and then the load read it a third time. On a store
// whose image is most of the machine's memory that is what made an opt-in
// integrity check unaffordable.
type imageSource struct {
	data []byte
	m    *mapping // nil when data is heap
}

// mapped reports whether reads from this image will alias the file.
func (src *imageSource) mapped() bool { return src != nil && src.m != nil }

// discard releases an image whose load failed. Heap bytes need nothing.
func (src *imageSource) discard() {
	if src != nil && src.m != nil {
		_ = src.m.close()
	}
}

// openImage reads or maps the image at path according to the store's mode,
// falling back to the heap whenever mapping is unavailable rather than refusing
// the open.
//
// Every fallback that is not the caller's own choice is reported: the returned
// source is always usable, and store.MetricImageFallback names why it is not
// mapped, because a store silently paying for a copy it asked not to pay for is
// a performance bug nobody can see.
func (s *Store) openImage(path string) (*imageSource, error) {
	if err := s.imageMappingAllowed(); err != nil {
		if s.imageMode != ImageHeap {
			s.recordImageFallback(err)
		}
		return s.readImage(path)
	}
	m, err := mapImage(path)
	if err != nil {
		s.recordImageFallback(err)
		return s.readImage(path)
	}
	return &imageSource{data: m.data, m: m}, nil
}

// imageMappingAllowed reports why this store will not map, or nil.
func (s *Store) imageMappingAllowed() error {
	return imageMappingAllowedFor(s.imageMode, s.live)
}

// imageMappingAllowedFor is the rule itself, as a function of the mode and
// whether the opener is a live reader.
//
// Separated from the method so that PreflightOpen's OpenEstimate.HeapBytesFor can
// answer "what would this cost under those Options" by asking the rule rather
// than by restating it. A second copy of this switch is a second thing to keep in
// step, and the one that matters most is the case where the answer is not the
// mode the caller named.
func imageMappingAllowedFor(mode ImageMode, live bool) error {
	switch mode {
	case ImageHeap:
		return errors.New("Options.ImageMode is ImageHeap")
	case ImageMappedUnlocked:
		if !mappingSupported {
			return errImageMappingUnsupported
		}
		return nil
	}
	if !mappingSupported {
		return errImageMappingUnsupported
	}
	// The default mode maps only where something excludes a concurrent writer
	// from the directory. A live reader holds no lock by construction, and on a
	// platform with no locking primitive even an exclusive open holds nothing.
	if live || !lockingEnforced {
		return errImageMappingNotAllowed
	}
	return nil
}

// readImage is the pre-mapping path, kept whole: one read, and the blobs copied
// into an arena by the parse.
func (s *Store) readImage(path string) (*imageSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return &imageSource{data: data}, nil
}

// recordImageFallback reports an image that is not mapped when the mode asked
// for one.
func (s *Store) recordImageFallback(reason error) {
	s.record(store.Metric{Kind: store.MetricImageFallback, Err: reason})
}

// noteImage records a mapping the store now owns, and pins it to the graph whose
// records address it. Caller holds s.mu exclusively.
func (s *Store) noteImage(src *imageSource, csr *CSRGraph) {
	if !src.mapped() {
		return
	}
	src.m.attach(csr)
	s.images = append(s.images, src.m)
}

// mappedIndexBase is an index base together with the fact that it was read out
// of a mapping this store owns.
//
// It exists to give runtime.AddCleanup something to watch. A cleanup needs a
// pointer to a concrete type and index.Base is an interface, so the base is
// wrapped rather than watched directly; the wrapper is what the index holds, so
// the wrapper is unreachable exactly when the base is.
//
// The mapping is not a field here, and that is the same rule mapping.attach
// states for a graph: a cleanup never runs while its argument is reachable from
// the object it watches. What keeps the mapping alive is the store's
// indexImages slice.
type mappedIndexBase struct {
	index.Base
}

// noteIndexImage records a mapping whose bytes the index base b reads, and
// arranges for it to be released once nothing can read it any more. Caller holds
// s.mu exclusively.
//
// The reachability test is the one mapping.attach uses and it is a floor on the
// truth for the same reason: a value yielded out of the base addresses the
// mapping, and the collector does not trace mapped memory. What makes the floor
// sound here is that every path yielding base bytes to a caller does so inside a
// call that holds the base -- ForEachNodeValue hands a value to a callback and
// the baseSide naming it is live across the whole walk -- so a base cannot be
// collected while one of its values is being read. A caller that keeps such a
// value past the call has the same contract as one that keeps a mapped
// Properties slice, and store.CloneNode is the same answer.
func (s *Store) noteIndexImage(m *mapping, b *mappedIndexBase) {
	if m == nil || b == nil {
		return
	}
	runtime.AddCleanup(b, func(m *mapping) { m.retirable.Store(true) }, m)
	s.indexImages = append(s.indexImages, m)
}

// sweepIndexImages unmaps every index mapping no reachable base can read any
// more. Caller holds s.mu exclusively.
//
// Separate from sweepImages, and calling that one here would be a bug rather
// than a saving: a writer's graph mapping is marked retirable as soon as the
// graph parsed from it is replaced, which a compaction does on its first commit,
// while the graph the compaction published goes on addressing that mapping for
// every record the delta did not touch. Sweeping it would be a use-after-unmap
// on the live graph. Nothing builds a base out of another base, so this list has
// no such dependency and can be swept the moment the collector says so.
func (s *Store) sweepIndexImages() {
	if len(s.indexImages) == 0 {
		return
	}
	keep := s.indexImages[:0]
	for _, m := range s.indexImages {
		if !m.retirable.Load() {
			keep = append(keep, m)
			continue
		}
		_ = m.close()
	}
	for i := len(keep); i < len(s.indexImages); i++ {
		s.indexImages[i] = nil
	}
	s.indexImages = keep
}

// sweepImages unmaps every image mapping no reachable graph can address any
// more. Caller holds s.mu exclusively.
//
// Only a live reader ever gives this anything to do. A writer maps once at Open
// and keeps that mapping until Close, because the graph each compaction
// publishes goes on addressing it — see the file comment.
func (s *Store) sweepImages() {
	if len(s.images) == 0 {
		return
	}
	keep := s.images[:0]
	for _, m := range s.images {
		if !m.retirable.Load() {
			keep = append(keep, m)
			continue
		}
		_ = m.close()
	}
	// Clear the tail so a swept mapping is not kept alive by the slice's
	// backing array, which is the one reference that would outlive the sweep.
	for i := len(keep); i < len(s.images); i++ {
		s.images[i] = nil
	}
	s.images = keep
}

// closeImages unmaps every image mapping unconditionally, reachable or not.
//
// Called by Close, which is where the handle's lifetime ends and so where the
// contract on a returned mapped slice ends with it. A slice read out of a mapped
// image and kept past Close addresses memory the process no longer has; that is
// what store.CloneNode is for, and it is the one sentence Close adds to the
// aliasing contract ImageHeap does not have.
func (s *Store) closeImages() error {
	var first error
	for i, m := range s.images {
		if err := m.close(); err != nil && first == nil {
			first = err
		}
		s.images[i] = nil
	}
	s.images = nil
	// The index's mappings go the same way and for the same reason. A base read
	// in place is only as valid as the file under it, and Close is where that
	// ends.
	for i, m := range s.indexImages {
		if err := m.close(); err != nil && first == nil {
			first = err
		}
		s.indexImages[i] = nil
	}
	s.indexImages = nil
	return first
}

// imageHolding reports how the store is holding its image, for StorageStats.
// Caller holds s.mu.
//
// The live graph's own figure rather than the mappings the store owns, and the
// difference matters after a compaction: the store still has the mapping it
// opened with — the published graph's blobs address it — but the graph is one
// the compaction built, so what a caller is served from is record arrays. This
// reports "heap" then, which is the honest answer to "is this image being served
// out of a file".
func (s *Store) imageHolding() (string, int64) {
	v := s.viewPtr.Load()
	if v == nil || v.csr == nil {
		return "", 0
	}
	if n := v.csr.MappedBytes(); n > 0 {
		return ImageMapped.String(), n
	}
	return ImageHeap.String(), 0
}
