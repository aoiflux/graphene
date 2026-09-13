package disk

// The encoder's order guards.
//
// GPIX is read by binary search over a key's value table, and a run is handed to a
// caller as an ascending IDRun. Neither order is checked when the section is
// parsed: verifying either is a pass over the whole index, and that pass belongs to
// the bounded verifier rather than to every open. So the writer is where a
// disordered stream has to be caught — one comparison per value and one per entry,
// against the alternative of a file that opens cleanly and answers wrongly.
//
// Failing the compaction is the point of failing at all. The previous image is
// still installed and the store is still serving; a file written and then found
// unreadable at the next open is an unavailable store.

import (
	"strings"
	"testing"
)

// oneKeySource is a source whose single node key yields exactly what it is given,
// in the order it is given — including orders the format forbids.
func oneKeySource(key string, values []string, ids [][]uint64) gpixSource {
	return gpixSource{
		NodeKeys: []string{key},
		NodeValues: func(_ string, fn func(value []byte, ids []uint64) bool) error {
			for i, v := range values {
				if !fn([]byte(v), ids[i]) {
					return nil
				}
			}
			return nil
		},
	}
}

func TestWriteGPIX_RefusesADisorderedStream(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  gpixSource
		want string
	}{{
		name: "values descending",
		src:  oneKeySource("k", []string{"b", "a"}, [][]uint64{{1}, {2}}),
		want: "values are not ascending",
	}, {
		// A repeat is as damaging as an inversion: two runs under one value make a
		// search stop at the first and miss the second, and the value table's
		// binary search cannot tell which it landed on.
		name: "value repeated",
		src:  oneKeySource("k", []string{"a", "a"}, [][]uint64{{1}, {2}}),
		want: "values are not ascending",
	}, {
		name: "ids descending",
		src:  oneKeySource("k", []string{"a"}, [][]uint64{{3, 1}}),
		want: "ids out of order",
	}, {
		// The delta and the base can both hold one triple; a merge that failed to
		// deduplicate would produce this, and a reader handed it would report the
		// entity twice from one value.
		name: "id repeated",
		src:  oneKeySource("k", []string{"a"}, [][]uint64{{2, 2}}),
		want: "ids out of order",
	}, {
		name: "keys descending",
		src: gpixSource{
			NodeKeys: []string{"b", "a"},
			NodeValues: func(key string, fn func([]byte, []uint64) bool) error {
				fn([]byte("v"), []uint64{1})
				return nil
			},
		},
		want: "keys are not ascending",
	}, {
		name: "key repeated",
		src: gpixSource{
			NodeKeys: []string{"a", "a"},
			NodeValues: func(key string, fn func([]byte, []uint64) bool) error {
				fn([]byte("v"), []uint64{1})
				return nil
			},
		},
		want: "keys are not ascending",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := encodeMappedIndexFrom(tc.src)
			if err == nil {
				t.Fatal("the encoder accepted a stream the format forbids")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}
}

// TestWriteGPIX_AcceptsAnEmptyValueFirst is the boundary the order check itself has
// to get right: the zero-length value is a legitimate first value, and a check that
// used "have I written anything yet" as "is the previous value non-empty" would
// reject the key that starts with it.
func TestWriteGPIX_AcceptsAnEmptyValueFirst(t *testing.T) {
	src := oneKeySource("k", []string{"", "a"}, [][]uint64{{1}, {2}})
	if _, _, _, err := encodeMappedIndexFrom(src); err != nil {
		t.Fatalf("an empty first value was refused: %v", err)
	}
	// And the check is still armed after it.
	back := oneKeySource("k", []string{"a", ""}, [][]uint64{{1}, {2}})
	if _, _, _, err := encodeMappedIndexFrom(back); err == nil {
		t.Fatal("an empty value after a non-empty one was accepted")
	}
}
