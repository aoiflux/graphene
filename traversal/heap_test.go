package traversal

// The heap is the one piece of this package with no oracle above it: a Dijkstra
// over a wrong heap still returns *a* path, and on most graphs it returns the
// right one, so a broken sift shows up as an occasional too-expensive answer on
// a fixture nobody wrote. These tests check the invariant directly.

import (
	"math/rand"
	"sort"
	"testing"
)

// TestHeap_PopsInPriorityOrder is the property that matters: whatever order
// items go in, they come out ascending by priority.
func TestHeap_PopsInPriorityOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(7))

	for _, n := range []int{0, 1, 2, 3, 7, 8, 9, 64, 1000} {
		want := make([]float64, n)
		var h minHeap[int]
		for i := range want {
			p := rng.Float64() * 100
			want[i] = p
			h.push(p, i)
		}
		sort.Float64s(want)

		got := make([]float64, 0, n)
		for {
			p, _, ok := h.pop()
			if !ok {
				break
			}
			got = append(got, p)
		}

		if len(got) != n {
			t.Fatalf("n=%d: popped %d items", n, len(got))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("n=%d: item %d out of order: got %v, want %v", n, i, got[i], want[i])
			}
		}
	}
}

// TestHeap_KeepsPayloadWithPriority guards the sift loops, which move entries
// by assignment rather than by swapping. A loop that carried the priority but
// dropped the payload would still pop in order and still be wrong.
func TestHeap_KeepsPayloadWithPriority(t *testing.T) {
	var h minHeap[string]
	items := map[float64]string{
		3.5: "three-five", 1.0: "one", 9.25: "nine", 0.0: "zero", -2.5: "minus",
	}
	for p, v := range items {
		h.push(p, v)
	}
	for {
		p, v, ok := h.pop()
		if !ok {
			break
		}
		if items[p] != v {
			t.Fatalf("priority %v came back with payload %q, want %q", p, v, items[p])
		}
	}
}

// TestHeap_InterleavedPushPop covers the shape a search actually produces:
// pushes and pops mixed, so the heap is never simply filled and then drained.
func TestHeap_InterleavedPushPop(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	var h minHeap[int]
	var mirror []float64

	for step := 0; step < 5000; step++ {
		if len(mirror) == 0 || rng.Intn(3) != 0 {
			p := rng.Float64()
			h.push(p, step)
			mirror = append(mirror, p)
			sort.Float64s(mirror)
			continue
		}
		p, _, ok := h.pop()
		if !ok {
			t.Fatal("pop reported empty with items outstanding")
		}
		if p != mirror[0] {
			t.Fatalf("step %d: popped %v, want the minimum %v", step, p, mirror[0])
		}
		mirror = mirror[1:]
	}
}

// TestHeap_EmptyPop is the terminating condition every caller loops on.
func TestHeap_EmptyPop(t *testing.T) {
	var h minHeap[int]
	if _, _, ok := h.pop(); ok {
		t.Fatal("an empty heap reported an item")
	}
	h.push(1, 1)
	if _, _, ok := h.pop(); !ok {
		t.Fatal("a one-item heap reported empty")
	}
	if _, _, ok := h.pop(); ok {
		t.Fatal("a drained heap reported an item")
	}
}

// TestHeap_ClearsVacatedSlot pins the write pop makes before shrinking. Without
// it a payload holding a pointer stays reachable through the slice's spare
// capacity for the rest of the walk, which is a leak the type system will not
// mention.
func TestHeap_ClearsVacatedSlot(t *testing.T) {
	var h minHeap[*int]
	a, b := new(int), new(int)
	h.push(1, a)
	h.push(2, b)
	h.pop()
	h.pop()

	spare := h[:cap(h)]
	for i := range spare {
		if spare[i].val != nil {
			t.Fatalf("slot %d still holds a payload after the heap drained", i)
		}
	}
}
