package disk

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestSpill_HoldsTheSameBytesWhicheverSideOfTheCapItIsOn writes one stream twice,
// once below the cap and once far above it, and asserts both hand back exactly
// what went in.
//
// The two paths are different code — an append to a slice, and a buffered write
// to a file — and what a caller reads back has to be identical, because what the
// caller does with it is copy it into an image whose digest covers it.
func TestSpill_HoldsTheSameBytesWhicheverSideOfTheCapItIsOn(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	want := make([]byte, 40_000)
	rng.Read(want)

	// Chunk sizes that straddle the flush buffer in both directions: some smaller
	// than what is left in it, some larger than the buffer itself.
	chunks := []int{1, 7, 64, 4096, spillFlushBuffer + 13}

	for _, tc := range []struct {
		name   string
		memCap int
		wantFD bool
	}{
		{"held in memory", len(want) * 2, false},
		{"exactly at the cap", len(want), false},
		{"spilled to a file", 128, true},
		{"spilled immediately", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := newSpill(dir, tc.memCap)
			defer s.close()
			for off, i := 0, 0; off < len(want); i++ {
				n := chunks[i%len(chunks)]
				if off+n > len(want) {
					n = len(want) - off
				}
				s.write(want[off : off+n])
				off += n
			}
			if s.len() != uint64(len(want)) {
				t.Fatalf("len = %d, want %d", s.len(), len(want))
			}
			if opened := s.f != nil; opened != tc.wantFD {
				t.Fatalf("opened a file = %v, want %v at cap %d", opened, tc.wantFD, tc.memCap)
			}
			r, err := s.reader()
			if err != nil {
				t.Fatalf("reader: %v", err)
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("read back: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("read back %d bytes, want %d, equal=%v", len(got), len(want), bytes.Equal(got, want))
			}
			// Sections are what the merge takes, and they must be independent:
			// two readers held at once, each seeing its own extent.
			a, err := s.section(100, 500)
			if err != nil {
				t.Fatalf("section a: %v", err)
			}
			b, err := s.section(20_000, 900)
			if err != nil {
				t.Fatalf("section b: %v", err)
			}
			gotB, _ := io.ReadAll(b)
			gotA, _ := io.ReadAll(a)
			if !bytes.Equal(gotA, want[100:600]) {
				t.Error("section at 100 does not match")
			}
			if !bytes.Equal(gotB, want[20_000:20_900]) {
				t.Error("section at 20000 does not match")
			}
		})
	}
}

// TestSpill_RefusesASectionOutsideWhatWasWritten checks the bound, including the
// wrapping case: off+n is computed on two uint64s, and a guard that wraps is a
// guard that passes before the read it protects fails.
func TestSpill_RefusesASectionOutsideWhatWasWritten(t *testing.T) {
	s := newSpill(t.TempDir(), 1<<20)
	defer s.close()
	s.write(make([]byte, 64))
	for _, tc := range []struct{ off, n uint64 }{
		{0, 65},
		{64, 1},
		{65, 0},
		{1, ^uint64(0)},
		{^uint64(0), 1},
	} {
		if _, err := s.section(tc.off, tc.n); err == nil {
			t.Errorf("section(%d, %d) was accepted over 64 bytes", tc.off, tc.n)
		}
	}
}

// TestSpill_CloseRemovesItsFile is the anti-litter check. A compaction that fails
// on a full disk is exactly when a leftover spill matters, so close has to be
// what removes it rather than process exit.
func TestSpill_CloseRemovesItsFile(t *testing.T) {
	dir := t.TempDir()
	s := newSpill(dir, 8)
	s.write(make([]byte, 4096))
	if s.f == nil {
		t.Fatal("no file was opened at a cap of 8 bytes")
	}
	if names := ls(t, dir); len(names) != 1 {
		t.Fatalf("directory holds %v, want one spill", names)
	}
	s.close()
	if names := ls(t, dir); len(names) != 0 {
		t.Fatalf("close left %v behind", names)
	}
	// Idempotent: the caller defers it and may also call it on an error path.
	s.close()
}

// TestSpill_LatchesAFailureToOpen checks that a spill which cannot open its file
// reports it when the bytes are asked for, rather than silently holding nothing.
//
// Reporting at read time rather than at write time is the bargain this type makes
// with imageWriter, and the thing that must not happen is a spill that swallows
// the failure and hands back a short region the section directory claims is
// longer.
func TestSpill_LatchesAFailureToOpen(t *testing.T) {
	s := newSpill(filepath.Join(t.TempDir(), "no-such-directory"), 4)
	defer s.close()
	s.write(make([]byte, 64))
	if _, err := s.reader(); err == nil {
		t.Fatal("reader returned no error after the file could not be created")
	}
	if _, err := s.section(0, 1); err == nil {
		t.Fatal("section returned no error after the file could not be created")
	}
}

// TestSpill_MemoryIsBoundedByTheCap is the guard on the reason this type exists.
//
// A slope rather than a ceiling, the lesson TestAllocGuards_Paths records: ten
// times the bytes through a spill with the same cap must not cost ten times the
// memory. If it does, the cap is not being honoured and the 448 MiB value table
// the mapped index removed is back.
func TestSpill_MemoryIsBoundedByTheCap(t *testing.T) {
	dir := t.TempDir()
	measure := func(total int) uint64 {
		payload := make([]byte, 512)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		s := newSpill(dir, 1024)
		for written := 0; written < total; written += len(payload) {
			s.write(payload)
		}
		r, err := s.reader()
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
		if _, err := io.Copy(io.Discard, r); err != nil {
			t.Fatalf("drain: %v", err)
		}
		runtime.ReadMemStats(&after)
		s.close()
		return after.TotalAlloc - before.TotalAlloc
	}
	small := measure(256 << 10)
	large := measure(2560 << 10)
	if large > 2*small+64<<10 {
		t.Fatalf("ten times the bytes allocated %d against %d: the cap is not bounding the spill",
			large, small)
	}
	t.Logf("spill allocation: 256 KiB in %d B, 2560 KiB in %d B", small, large)
}

// ls lists a directory's entries by name.
func ls(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

// TestSpill_LatchesAFailedWrite covers the other half of the failure story: not a
// file that could not be created, but one that stops accepting bytes part way
// through — which is what a disk filling up during a compaction looks like.
//
// The file is closed behind the spill's back because that is the only portable way
// to make a write fail on demand. What is being asserted is not how it failed but
// that the failure reaches the caller instead of leaving a region shorter than the
// section directory says it is.
func TestSpill_LatchesAFailedWrite(t *testing.T) {
	s := newSpill(t.TempDir(), 8)
	defer s.close()
	s.write(make([]byte, 64))
	if s.f == nil {
		t.Fatal("no file was opened")
	}
	if err := s.f.Close(); err != nil {
		t.Fatalf("close the file under it: %v", err)
	}
	s.write(make([]byte, 64))
	if _, err := s.reader(); err == nil {
		t.Fatal("reader returned no error after the underlying write failed")
	}
}
