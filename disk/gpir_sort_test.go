package disk

import (
	"math/rand"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

// randomGPIREntries produces entries whose (id, keyID, valueOff) triples are
// unique, so the order the sort has to produce is the only correct one and a
// comparison against a reference sort is well defined.
//
// Ids repeat deliberately: an entity carries several entries, so the secondary
// keys are what the merge is actually exercised on.
func randomGPIREntries(seed int64, n int) []gpirEntry {
	rng := rand.New(rand.NewSource(seed))
	out := make([]gpirEntry, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, gpirEntry{
			ID:       uint64(rng.Intn(n/4 + 1)),
			KeyID:    uint16(i % 5),
			ValLen:   uint32(rng.Intn(64)),
			ValueOff: uint64(i) * 8, // distinct, so the triple is
		})
	}
	return out
}

func sortedGPIRCopy(in []gpirEntry) []gpirEntry {
	out := slices.Clone(in)
	slices.SortFunc(out, gpirEntryLess)
	return out
}

// TestGPIRSorter_OrdersTheSameWhateverTheChunk is the differential check on the
// external sort: every chunk size must produce the order one in-memory sort
// produces.
//
// The chunk size decides which of two algorithms runs — a single sort, or a sort
// per run followed by a k-way merge — and the second is the one a real store
// takes. A chunk of one makes every entry its own run, which is the merge at its
// widest and the case where a heap bug shows.
func TestGPIRSorter_OrdersTheSameWhateverTheChunk(t *testing.T) {
	in := randomGPIREntries(7, 2_000)
	want := sortedGPIRCopy(in)

	for _, chunk := range []int{1, 2, 7, 64, 499, 1_999, 2_000, 4_000} {
		s := newGPIRSorter(t.TempDir(), compactBuffers{revChunk: chunk})
		for _, e := range in {
			s.add(e)
		}
		if s.count() != uint64(len(in)) {
			t.Fatalf("chunk %d: count = %d, want %d", chunk, s.count(), len(in))
		}
		got := make([]gpirEntry, 0, len(in))
		if err := s.emit(func(e gpirEntry) { got = append(got, e) }); err != nil {
			t.Fatalf("chunk %d: emit: %v", chunk, err)
		}
		s.close()
		if len(got) != len(want) {
			t.Fatalf("chunk %d: emitted %d entries, want %d", chunk, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("chunk %d: entry %d is %+v, want %+v", chunk, i, got[i], want[i])
			}
		}
	}
}

// TestGPIRSorter_EmptyAndSingle covers the two degenerate shapes, which are the
// ones a real store hits at the two ends: a graph with no indexed property, and
// one with a single entry.
func TestGPIRSorter_EmptyAndSingle(t *testing.T) {
	for _, n := range []int{0, 1} {
		for _, chunk := range []int{1, 1024} {
			s := newGPIRSorter(t.TempDir(), compactBuffers{revChunk: chunk})
			for i := 0; i < n; i++ {
				s.add(gpirEntry{ID: 5, KeyID: 1, ValLen: 2, ValueOff: 8})
			}
			count := 0
			if err := s.emit(func(gpirEntry) { count++ }); err != nil {
				t.Fatalf("n=%d chunk=%d: emit: %v", n, chunk, err)
			}
			s.close()
			if count != n {
				t.Fatalf("n=%d chunk=%d: emitted %d", n, chunk, count)
			}
		}
	}
}

// TestGPIRSorter_RefusesASecondDrain guards the one-shot contract. The merge
// consumes its readers, so a second emit would silently produce a short section
// rather than the same one.
func TestGPIRSorter_RefusesASecondDrain(t *testing.T) {
	s := newGPIRSorter(t.TempDir(), compactBuffers{revChunk: 4})
	defer s.close()
	for i := 0; i < 10; i++ {
		s.add(gpirEntry{ID: uint64(i)})
	}
	if err := s.emit(func(gpirEntry) {}); err != nil {
		t.Fatalf("first emit: %v", err)
	}
	if err := s.emit(func(gpirEntry) {}); err == nil {
		t.Fatal("a second emit was accepted")
	}
}

// TestGPIRSorter_SurvivesAFailureToSpill checks that a sorter which cannot open
// its file reports it from emit rather than emitting the subset it happened to be
// holding.
//
// Emitting a subset is the failure that matters: the section's header carries the
// count, so a short body is an image whose directory and contents disagree.
func TestGPIRSorter_SurvivesAFailureToSpill(t *testing.T) {
	s := newGPIRSorter(filepath.Join(t.TempDir(), "no-such-directory"), compactBuffers{revChunk: 2})
	defer s.close()
	for i := 0; i < 20; i++ {
		s.add(gpirEntry{ID: uint64(i), ValueOff: uint64(i) * 8})
	}
	emitted := 0
	if err := s.emit(func(gpirEntry) { emitted++ }); err == nil {
		t.Fatalf("emit reported success after emitting %d of 20 entries with no spill available",
			emitted)
	}
}

// TestGPIRSorter_MemoryIsBoundedByTheChunk is the guard on the property the
// external sort exists for.
//
// A slope, not a ceiling. Ten times the entries must not cost ten times the
// memory: if it does, the sorter is holding the whole set and the 672 MiB the
// reverse index would cost in the heap is back, in the code written to avoid it.
//
// The chunk is set to sixteen entries so that both sizes are past the read
// budget, which is where the bound actually is. The sorter's memory is one chunk
// plus the merge's read buffers, and the second is bounded by
// gpirMergeReadBudget however many runs it divides between -- so the flat region
// this asserts over is the region every real store is in, and a chunk large
// enough to keep the runs few would be measuring the chunk instead.
func TestGPIRSorter_MemoryIsBoundedByTheChunk(t *testing.T) {
	dir := t.TempDir()
	measure := func(n int) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		s := newGPIRSorter(dir, compactBuffers{revChunk: 16})
		for i := 0; i < n; i++ {
			s.add(gpirEntry{ID: uint64(i * 7919 % n), KeyID: uint16(i % 3), ValueOff: uint64(i) * 8})
		}
		if err := s.emit(func(gpirEntry) {}); err != nil {
			t.Fatalf("emit: %v", err)
		}
		runtime.ReadMemStats(&after)
		s.close()
		return after.TotalAlloc - before.TotalAlloc
	}
	small := measure(4_000)
	large := measure(40_000)
	if large > 2*small+1<<20 {
		t.Fatalf("ten times the entries allocated %d against %d: the sort is holding the set",
			large, small)
	}
	t.Logf("gpir sort allocation: 4k entries %d B, 40k entries %d B", small, large)
}

// TestGPIRSorter_CleansUpItsFile checks that close removes the spill, for the
// reason TestSpill_CloseRemovesItsFile gives: a failed compaction on a tight disk
// must not leave a second file behind.
func TestGPIRSorter_CleansUpItsFile(t *testing.T) {
	dir := t.TempDir()
	s := newGPIRSorter(dir, compactBuffers{revChunk: 4})
	for i := 0; i < 100; i++ {
		s.add(gpirEntry{ID: uint64(i), ValueOff: uint64(i) * 8})
	}
	if len(ls(t, dir)) == 0 {
		t.Fatal("a chunk of four entries over a hundred adds opened no file")
	}
	if err := s.emit(func(gpirEntry) {}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	s.close()
	if names := ls(t, dir); len(names) != 0 {
		t.Fatalf("close left %v behind", names)
	}
}
