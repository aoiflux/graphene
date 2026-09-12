package disk

// A byte sink that is memory while it is small and a file when it is not.
//
// # What needs one
//
// Both halves of the disk-resident property index are generated in one order and
// placed in another. A GPIX value table is built while the runs it addresses are
// being written, so it cannot go into the image until they are down; GPIR is
// sorted by entity while the forward pass generates it by key. The obvious answer
// to both is to hold the intermediate in memory — 16 bytes per distinct value for
// one, 24 bytes per entry for the other, which at the shape
// docs/MEMORY_MODEL.md measures is 448 MiB and 672 MiB. That is the allocation
// this whole section of the program exists to remove, so it cannot be how the
// section is built.
//
// The answer is the one compaction already uses for the image itself: put it on
// disk and stream it. Disk is the resource this program is allowed to spend —
// the brief's own words are that the target is a 2 GB machine and disk is
// unconstrained — and both intermediates are written once, sequentially, and
// read back once, sequentially, while they are still in the page cache.
//
// # Why memory first
//
// A store whose whole index fits in a megabyte should not open a file to write
// it, and neither should the several hundred serialisations the test suite does
// per run. So a spill holds its bytes in memory up to a cap and opens a file
// only when it exceeds it, which means the file path exists for the shape that
// needs it and the fast path is what every small store takes. Both paths are
// exercised: the tests set the cap to a handful of bytes.
//
// The cap is on the intermediate, not on the store. Exceeding it costs
// sequential IO and never costs correctness, which is why it can be a constant
// rather than an option.

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// spillFlushBuffer is the write buffer a spill keeps once it has opened a file.
// Small on purpose: the point of opening the file was to stop holding the
// contents, so the buffer that replaces them may not be a function of them.
const spillFlushBuffer = 64 << 10

// spillBuffer is an append-only byte sink that can be read back by offset.
//
// It is not an io.Writer. Writes cannot fail usefully here — a caller in the
// middle of encoding a section has nothing to do with an ENOSPC but abandon the
// image, which it will do anyway when the image write fails — so the first error
// is latched and reported once, at the point the bytes are needed. That is the
// same bargain imageWriter makes, for the same reason.
type spillBuffer struct {
	dir    string
	memCap int

	// mem holds the whole contents while f is nil, and is the flush buffer
	// afterwards. The two never overlap: opening the file writes mem out and
	// replaces it with a small one, so "f == nil" is exactly "everything is in
	// mem".
	mem []byte
	f   *os.File
	n   uint64
	err error
}

// newSpill returns a spill that holds up to memCap bytes before opening a file
// in dir. An empty dir means the operating system's temporary directory.
func newSpill(dir string, memCap int) *spillBuffer {
	return &spillBuffer{dir: dir, memCap: memCap}
}

// len is how many bytes have been written, which is also the offset the next
// write lands at. Callers use it as the address of what they are about to write,
// exactly as imageWriter.at is used.
func (s *spillBuffer) len() uint64 { return s.n }

// write appends p.
func (s *spillBuffer) write(p []byte) {
	if s.err != nil {
		return
	}
	s.n += uint64(len(p))
	if s.f == nil {
		if len(s.mem)+len(p) <= s.memCap {
			if s.mem == nil {
				// Allocated to the cap on the first write rather than grown into
				// it. Callers write sixteen and twenty-four bytes at a time, and
				// Go's growth factor above a few hundred bytes is about 1.25, so
				// growing into a one-megabyte cap costs five megabytes of
				// allocation on the way — which would make the encoder's
				// allocation a function of the index below the cap, and it is the
				// whole point of the cap that it is not. The cap is a constant, so
				// this is a constant.
				s.mem = make([]byte, 0, s.memCap)
			}
			s.mem = append(s.mem, p...)
			return
		}
		if !s.open() {
			return
		}
	}
	switch {
	case len(p) >= cap(s.mem):
		// Larger than the buffer: staging it would copy for nothing.
		if !s.flush() {
			return
		}
		if _, err := s.f.Write(p); err != nil {
			s.err = err
		}
	case len(p) > cap(s.mem)-len(s.mem):
		if !s.flush() {
			return
		}
		s.mem = append(s.mem, p...)
	default:
		s.mem = append(s.mem, p...)
	}
}

// open moves what is in memory to a file and installs the flush buffer.
func (s *spillBuffer) open() bool {
	f, err := os.CreateTemp(s.dir, "graphene-spill-*")
	if err != nil {
		s.err = fmt.Errorf("spill: create: %w", err)
		return false
	}
	if _, err := f.Write(s.mem); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		s.err = fmt.Errorf("spill: write %d buffered bytes: %w", len(s.mem), err)
		return false
	}
	s.f = f
	// A fresh buffer rather than a reslice of the old one: the old one is as
	// large as the cap, and keeping it would mean the spill still holds what
	// opening the file was supposed to stop holding.
	s.mem = make([]byte, 0, spillFlushBuffer)
	return true
}

// flush empties the buffer into the file, reporting whether the spill is still
// usable.
func (s *spillBuffer) flush() bool {
	if s.err != nil || s.f == nil || len(s.mem) == 0 {
		return s.err == nil
	}
	if _, err := s.f.Write(s.mem); err != nil {
		s.err = fmt.Errorf("spill: flush: %w", err)
	}
	s.mem = s.mem[:0]
	return s.err == nil
}

// section returns a reader over n bytes at off.
//
// Readers are independent and may be held at once, which is what lets the
// external merge below read every sorted run in parallel. Nothing is written
// after the first section is taken; a spill is filled and then read.
func (s *spillBuffer) section(off, n uint64) (io.Reader, error) {
	if !s.flush() {
		return nil, s.err
	}
	if s.err != nil {
		return nil, s.err
	}
	if off+n > s.n || off+n < off {
		return nil, fmt.Errorf("spill: section %d+%d is outside %d bytes", off, n, s.n)
	}
	if s.f == nil {
		return bytes.NewReader(s.mem[off : off+n]), nil
	}
	return io.NewSectionReader(s.f, int64(off), int64(n)), nil
}

// reader returns a reader over everything written.
func (s *spillBuffer) reader() (io.Reader, error) { return s.section(0, s.n) }

// close releases the spill and deletes its file if it opened one.
//
// Best-effort on the remove, and deliberately so: the caller is finishing or
// abandoning an image, and a temporary file that outlives a crash is what the
// open path already ignores. Reporting a failed unlink would replace whichever
// error actually explains the outcome.
func (s *spillBuffer) close() {
	if s.f != nil {
		name := s.f.Name()
		_ = s.f.Close()
		_ = os.Remove(name)
	}
	// The count goes with the bytes. A closed spill holds nothing, and a reader
	// taken from one would otherwise be handed an extent that is gone.
	s.f, s.mem, s.n = nil, nil, 0
}
