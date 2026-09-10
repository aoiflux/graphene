//go:build linux || windows

package graphene_test

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRSSInstrument_SeparatesFileBackedResidency is the calibration that makes
// the mapped-image work measurable at all.
//
// The whole argument for mapping the image rather than reading it is that the
// bytes move out of the anonymous class, which a RAM ceiling constrains, and
// into the file-backed class, which the kernel can evict without swap. If the
// instrument cannot tell those apart, that change would report as "no
// improvement" and the measurement would veto a design on its own blind spot.
//
// This maps a file, touches every page, and asserts the reading lands in the
// file class and not the anonymous one. It is deliberately not a graphene test:
// it tests the ruler, not the thing being measured.
var fileBackedSink byte

func TestRSSInstrument_SeparatesFileBackedResidency(t *testing.T) {
	if !rssSupported {
		t.Skipf("no RSS instrument for %s", runtimeGOOS)
	}
	if raceEnabled {
		// Not a limitation of the reader, and worth stating rather than working
		// around: the race detector allocates shadow memory for every byte the
		// program touches, and that shadow is ordinary private anonymous memory.
		// Touching a 128 MiB mapping therefore produces roughly 128 MiB of real
		// anonymous growth alongside the file-backed growth, and no accounting
		// can separate the mapping from its shadow.
		//
		// The consequence reaches past this test: residency measured under -race
		// is not the residency the engine has, so the program's memory figures
		// are taken without it. The race detector's job here is the concurrency
		// tests, not the memory ones.
		t.Skip("race detector shadow memory is anonymous; the split cannot be measured under it")
	}

	const size = 128 << 20

	path := filepath.Join(t.TempDir(), "mapped.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	// Write real bytes: a sparse file may not produce resident pages on read.
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	for written := 0; written < size; written += len(chunk) {
		if _, err := f.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	before, _ := settledRSS(nil)

	data, unmap, err := mapFileForTest(f, size)
	if err != nil {
		t.Skipf("mapping unavailable: %v", err)
	}
	defer unmap()

	// Touch one byte per 4 KiB page so the mapping is actually faulted in. The
	// accumulator must escape: a local the function never reads back lets the
	// compiler delete the whole loop, and the test then measures nothing while
	// still looking like it ran.
	for i := 0; i < len(data); i += 4096 {
		fileBackedSink ^= data[i]
	}

	after, _ := settledRSS(&data)

	if !after.Split {
		t.Skip("platform reports no anon/file split")
	}

	// Half the mapping is a loose floor for two reasons: the kernel may evict
	// pages between the touch and the reading, and on windows the split
	// understates the file class by the process's current over-commit. A tight
	// bound here would be flaky rather than stronger.
	const floor = size / 2

	fileGrowth := int64(after.File) - int64(before.File)
	anonGrowth := int64(after.Anon) - int64(before.Anon)

	t.Logf("file %+d MiB, anon %+d MiB after mapping %d MiB",
		fileGrowth/bytesPerMiB, anonGrowth/bytesPerMiB, size/bytesPerMiB)

	if fileGrowth < floor {
		t.Errorf("file-backed residency grew by only %d bytes after mapping and touching %d bytes; "+
			"the instrument cannot see mapped pages, so it cannot judge a mapped image",
			fileGrowth, size)
	}
	// The mapping must not be charged as anonymous, or moving bytes into it would
	// look like no change at all.
	if anonGrowth >= floor {
		t.Errorf("anonymous residency grew by %d bytes for a read-only file mapping; "+
			"the split is not separating the two classes", anonGrowth)
	}
}
