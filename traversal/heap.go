package traversal

// A binary min-heap, hand-rolled.
//
// container/heap would do this in fewer lines and it is the wrong trade here.
// Its interface takes the collection as an interface value and calls back
// through Len/Less/Swap/Push/Pop, so every comparison is an indirect call the
// compiler cannot inline, and Push takes `any` — which boxes every element,
// putting one allocation on the inner loop of a walk that pushes once per
// relaxed edge. This package reverts changes over a single allocation per
// traversal (see the note on newGuard); an allocation per *edge* is not a
// trade worth the lines saved.
//
// Generic over the payload rather than over the ordering: the priority is
// always a float64 and always ascending, so the comparison is a direct
// float compare on a struct field with nothing to dispatch through. A caller
// wanting a different ordering negates its priorities.

// heapEntry is one queued item: what it is, and what it costs.
//
// The priority is stored beside the payload rather than recomputed from it,
// because a queued priority must not change while the item sits in the heap —
// a Dijkstra lowers a node's tentative distance while an entry for the old one
// is still queued, and re-deriving the priority at sift time would corrupt the
// heap order rather than simply leaving a stale entry to be discarded on pop.
type heapEntry[T any] struct {
	prio float64
	val  T
}

// minHeap is a binary min-heap over heapEntry, ordered by prio ascending.
//
// The zero value is an empty heap. It is a slice rather than a struct wrapping
// one so that a caller can preallocate with make(minHeap[T], 0, n) and never
// grow — the heap for a Dijkstra reaches at most one entry per relaxed edge.
type minHeap[T any] []heapEntry[T]

// push adds one item, sifting it up to its place.
func (h *minHeap[T]) push(prio float64, val T) {
	*h = append(*h, heapEntry[T]{prio: prio, val: val})
	s := *h
	i := len(s) - 1
	e := s[i]
	// The moving entry is carried in a local and written once at the end
	// rather than swapped at every level, which halves the writes.
	for i > 0 {
		parent := (i - 1) / 2
		if s[parent].prio <= e.prio {
			break
		}
		s[i] = s[parent]
		i = parent
	}
	s[i] = e
}

// pop removes and returns the lowest-priority item. The bool is false when the
// heap is empty, which is the only way this can fail — a caller looping until
// it goes false needs no separate length check.
func (h *minHeap[T]) pop() (float64, T, bool) {
	s := *h
	if len(s) == 0 {
		var zero T
		return 0, zero, false
	}

	top := s[0]
	last := len(s) - 1
	e := s[last]

	// Clear the vacated slot before shrinking, so a payload holding a pointer
	// does not keep it alive in the slice's spare capacity for the rest of the
	// walk. For the ID payloads in this package that is free; for anything else
	// it is the difference between a heap and a leak.
	var zero heapEntry[T]
	s[last] = zero
	s = s[:last]
	*h = s

	if last == 0 {
		return top.prio, top.val, true
	}

	// Sift the former last entry down from the root.
	i := 0
	for {
		left := 2*i + 1
		if left >= len(s) {
			break
		}
		child := left
		if right := left + 1; right < len(s) && s[right].prio < s[left].prio {
			child = right
		}
		if e.prio <= s[child].prio {
			break
		}
		s[i] = s[child]
		i = child
	}
	s[i] = e

	return top.prio, top.val, true
}
