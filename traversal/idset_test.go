package traversal

import (
	"math/rand"
	"testing"
)

// TestIDSetMatchesAMap is the oracle test: whatever the representation does
// internally, membership must be indistinguishable from a map's.
//
// The three ID patterns are chosen to drive the three states — dense stays a
// bitset from the first insert, sparse never promotes, and the mixed case
// forces a promotion and then a demotion in one run.
func TestIDSetMatchesAMap(t *testing.T) {
	patterns := map[string]func(i int) uint64{
		"dense from one":  func(i int) uint64 { return uint64(i) + 1 },
		"dense from high": func(i int) uint64 { return 90_000 + uint64(i) },
		"sparse":          func(i int) uint64 { return uint64(i) * 1_000_003 },
		"clustered":       func(i int) uint64 { return uint64(i/64)*10_000 + uint64(i%64) },
		"descending":      func(i int) uint64 { return 50_000 - uint64(i) },
		"with duplicates": func(i int) uint64 { return uint64(i/2) + 1 },
	}

	for name, gen := range patterns {
		t.Run(name, func(t *testing.T) {
			var s idSet
			oracle := map[uint64]struct{}{}

			for i := 0; i < 4000; i++ {
				id := gen(i)
				_, known := oracle[id]
				wantNew := !known
				oracle[id] = struct{}{}

				if got := s.add(id); got != wantNew {
					t.Fatalf("add(%d) at i=%d returned %v, want %v", id, i, got, wantNew)
				}
				if s.len() != len(oracle) {
					t.Fatalf("len is %d after %d inserts, want %d", s.len(), i+1, len(oracle))
				}
			}

			for id := range oracle {
				if !s.has(id) {
					t.Fatalf("has(%d) is false for an ID that was added", id)
				}
			}
			// Probe a spread of absent IDs, including ones inside the range.
			for i := 0; i < 4000; i++ {
				id := uint64(rand.Int63n(200_000))
				if _, want := oracle[id]; s.has(id) != want {
					t.Fatalf("has(%d) is %v, want %v", id, s.has(id), want)
				}
			}
		})
	}
}

// TestIDSetTransitions pins the three states and the moves between them, so a
// change that silently stopped promoting -- leaving the bitset dead code -- is
// caught. Two contracts are asserted: a small set touches neither of the
// growable representations, and past that the density rule alone decides, a
// bitset being entered only when it is no larger than the map it replaces and
// left when that stops being true.
func TestIDSetTransitions(t *testing.T) {
	var s idSet

	// Up to idSetInline entries the set is the inline array and nothing else.
	for i := uint64(1); i <= idSetInline; i++ {
		s.add(i)
	}
	if s.mode != modeInline {
		t.Fatalf("mode is %d after %d dense inserts, want inline", s.mode, idSetInline)
	}
	if s.m != nil || s.bits != nil {
		t.Fatal("an inline set allocated a map or a bitset")
	}

	// Overflowing it spills straight to the representation the density rule
	// picks, which for IDs from 1 upward is the bitset -- never a map first.
	s.add(idSetInline + 1)
	if s.mode != modeDense {
		t.Fatalf("mode is %d after overflowing a dense range, want dense", s.mode)
	}
	if s.m != nil {
		t.Fatal("a dense spill built a map on the way")
	}

	// An ID far outside the range makes the bitset the more expensive of the
	// two, so the set must fall back to a map rather than allocate for it.
	s.add(10_000_000)
	if s.mode != modeSparse {
		t.Fatalf("mode is %d for %d entries spanning 10M IDs, want sparse", s.mode, s.len())
	}
	if !s.has(1) || !s.has(10_000_000) {
		t.Fatal("demote lost an entry")
	}

	// Filling in the range makes it affordable again.
	for i := uint64(2); i < 200_000; i++ {
		s.add(i)
	}
	if s.mode != modeDense {
		t.Fatal("a set holding 200k of 10M IDs should be a bitset again")
	}
	if !s.has(1) || !s.has(10_000_000) || !s.has(199_999) {
		t.Fatal("promote lost an entry")
	}
	if s.has(200_001) {
		t.Fatal("promote invented an entry")
	}
}

// TestIDSetSmallWalkAllocatesNothing is the guard for the measurement that put
// the inline array there. The first version of this type held its map behind a
// pointer field, which the compiler cannot prove non-escaping, so every walk
// paid a heap map even when it held a dozen entries -- a flat +192 B and +2
// allocations per set against the stack-allocated maps it replaced. A short
// walk must now allocate nothing at all.
func TestIDSetSmallWalkAllocatesNothing(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates on paths this counts")
	}
	// IDs far from zero, which is the shape the disk fixtures walk: a high
	// origin with a handful of entries, where a bitset is correctly refused.
	got := testing.AllocsPerRun(200, func() {
		var s idSet
		for i := uint64(0); i < idSetInline; i++ {
			s.add(33_334 + i*13)
		}
		if s.len() != idSetInline {
			t.Fatalf("len is %d", s.len())
		}
	})
	if got != 0 {
		t.Fatalf("a %d-entry walk allocated %.0f times, want 0", idSetInline, got)
	}
}

// TestIDSetNeverCostsMoreThanAMap pins the property that makes the promotion
// safe to do without a tuning constant: the bitset is never the larger
// representation.
func TestIDSetNeverCostsMoreThanAMap(t *testing.T) {
	for _, step := range []uint64{1, 7, 64, 1000, 100_000} {
		var s idSet
		for i := uint64(0); i < 3000; i++ {
			s.add(i*step + 1)
		}
		if s.mode != modeDense {
			continue // stayed a map, which is the cheaper option by the same rule
		}
		if bitsetBytes, budget := uint64(len(s.bits))*8, uint64(s.len())*8; bitsetBytes > budget*2 {
			t.Errorf("step %d: bitset holds %d bytes for %d entries, over the %d-byte budget",
				step, bitsetBytes, s.len(), budget)
		}
	}
}

func TestIDSetReset(t *testing.T) {
	for _, start := range []uint64{1, 10_000_000} {
		var s idSet
		for i := uint64(0); i < 1000; i++ {
			s.add(start + i)
		}
		s.reset()
		if s.len() != 0 {
			t.Fatalf("len is %d after reset", s.len())
		}
		if s.has(start) {
			t.Fatal("reset left an entry behind")
		}
		if !s.add(start) {
			t.Fatal("add after reset reported the ID as already present")
		}
	}
}
