package traversal

import "math/bits"

// idSet is a membership set over the IDs a traversal visits.
//
// It exists because the profile said so. At -test.memprofilerate=1 on a wide
// BFS, the two map[ID]struct{} sets this replaces were 1.13 MB of the walk's
// 2.17 MB/op — more than half of everything it allocated, and the single
// largest remaining site on the read path. That is a bytes problem rather than
// a count problem: the sets appear nowhere near the top by object count, and
// what costs is a map's growth as the walk widens.
//
// # Three representations, and why none of them is a tuning constant
//
// A bitset is the obvious answer for the IDs a compacted CSR holds — dense,
// contiguous, allocated from 1 — and the wrong answer for a short walk on a
// store that has churned, where IDs are monotonic and never reused so the
// maximum can sit far above the live count. Sizing a bitset to that maximum to
// hold a dozen entries would trade P3 for P2, which 7.5's rule reverts.
//
// So the choice between a bitset and a map is not a threshold on the entry
// count — that is a bet on the walk continuing, and a walk that stops just
// after paying for the switch has paid for nothing. It is measured density,
// tested on every insert:
//
//	a bitset covering maxSeen costs (maxSeen/64 + 1) words
//	the same entries in a map cost at least one word each
//	so the bitset is never larger once maxSeen/64 <= n
//
// which is exactly the promotion condition. For IDs starting near 1 it holds
// on the first insert, so a dense walk never builds a map at all. For a walk
// that starts at ID 90 000 it holds once ~1 400 entries are in, which is the
// point where the map genuinely is the more expensive of the two. The bitset
// is therefore never the larger representation, which is what makes this safe
// to do unconditionally rather than behind a constant fitted to a benchmark.
//
// demote is the same rule read backwards: a set already dense that meets an ID
// far past its range would have to grow beyond a map's cost to record it, so
// it becomes a map again. maxSeen only ever rises, so the two cannot alternate.
//
// # The inline array, and the measurement that added it
//
// The first version had only those two representations, and it lost the A/B on
// every *disk* walk — a flat +192 B and +2 allocations per set, whatever the
// walk's size, while winning 46-58% on the wide and deep in-memory ones. The
// cause is not the algorithm, which on those fixtures never leaves the map at
// all: midNode sits at ID ~33 000 against ~15 entries visited, so the density
// rule correctly declines to build a bitset. It is that a map reached through a
// pointer field cannot be proved non-escaping. The maps this replaced were
// short-lived locals the compiler put on the *stack*; moving one behind s.m
// moves it to the heap, and that cost is paid by every walk whether it grows
// or not.
//
// So the small case gets neither: up to idSetInline entries live in an array
// inside the set, scanned linearly, allocating nothing on any path. The bound
// is not a performance guess — it is where a linear scan stops being obviously
// cheaper than hashing, and it is the same bound and the same reasoning as
// neighbourDedupeLinear in disk/view_read.go. Overflowing it spills to whichever
// of the other two the density rule picks, so no walk large enough for the
// representation to matter is decided by this number.
type idSet struct {
	// inline holds the first idSetInline distinct IDs, unsorted. It is the whole
	// set while n <= idSetInline, and is dead once the set has spilled.
	inline  [idSetInline]uint64
	m       map[uint64]struct{}
	bits    []uint64
	n       int
	maxSeen uint64
	mode    uint8
}

// idSetInline bounds the linear-scan representation. See the type's comment:
// the same value and the same argument as neighbourDedupeLinear.
const idSetInline = 32

const (
	modeInline uint8 = iota // n <= idSetInline entries in the inline array
	modeSparse              // a map, for IDs too spread out for a bitset
	modeDense               // a bitset covering maxSeen
)

// wordsFor is the bitset length needed to cover id.
func wordsFor(id uint64) uint64 { return (id >> 6) + 1 }

// affordable reports whether a bitset covering maxSeen is no larger than the
// n entries would cost as map keys — one 8-byte word each.
func affordable(maxSeen uint64, n int) bool { return wordsFor(maxSeen) <= uint64(n) }

// has reports whether id is in the set.
func (s *idSet) has(id uint64) bool {
	switch s.mode {
	case modeInline:
		for _, v := range s.inline[:s.n] {
			if v == id {
				return true
			}
		}
		return false
	case modeDense:
		w := id >> 6
		return w < uint64(len(s.bits)) && s.bits[w]&(1<<(id&63)) != 0
	default:
		_, ok := s.m[id]
		return ok
	}
}

// add inserts id and reports whether it was newly added.
func (s *idSet) add(id uint64) bool {
	if id > s.maxSeen {
		s.maxSeen = id
	}

	if s.mode == modeInline {
		for _, v := range s.inline[:s.n] {
			if v == id {
				return false
			}
		}
		if s.n < idSetInline {
			s.inline[s.n] = id
			s.n++
			return true
		}
		s.spill()
	}

	// Past the inline array the two remaining representations are chosen by
	// density alone, re-tested on every insert because maxSeen moves.
	if s.mode == modeSparse && affordable(s.maxSeen, s.n+1) {
		s.promote()
	} else if s.mode == modeDense && !affordable(s.maxSeen, s.n+1) {
		s.demote()
	}

	if s.mode == modeDense {
		w := id >> 6
		if w >= uint64(len(s.bits)) {
			s.grow(w + 1)
		}
		mask := uint64(1) << (id & 63)
		if s.bits[w]&mask != 0 {
			return false
		}
		s.bits[w] |= mask
		s.n++
		return true
	}

	if _, ok := s.m[id]; ok {
		return false
	}
	s.m[id] = struct{}{}
	s.n++
	return true
}

// len reports how many IDs the set holds.
func (s *idSet) len() int { return s.n }

// spill moves a full inline array into whichever of the two growable
// representations the density rule picks, so the first heap allocation the set
// makes is already the right one rather than a map it would promote out of.
func (s *idSet) spill() {
	if affordable(s.maxSeen, s.n) {
		s.bits = make([]uint64, wordsFor(s.maxSeen))
		for _, id := range s.inline[:s.n] {
			s.bits[id>>6] |= 1 << (id & 63)
		}
		s.mode = modeDense
		return
	}
	s.m = make(map[uint64]struct{}, s.n*2)
	for _, id := range s.inline[:s.n] {
		s.m[id] = struct{}{}
	}
	s.mode = modeSparse
}

// promote rebuilds the set as a bitset covering every ID seen so far.
func (s *idSet) promote() {
	s.bits = make([]uint64, wordsFor(s.maxSeen))
	for id := range s.m {
		s.bits[id>>6] |= 1 << (id & 63)
	}
	s.m = nil
	s.mode = modeDense
}

// demote rebuilds the set as a map, for a walk whose IDs turned out to be too
// sparse for a bitset to be the cheaper of the two.
func (s *idSet) demote() {
	s.m = make(map[uint64]struct{}, s.n)
	for w, word := range s.bits {
		for word != 0 {
			b := word & -word
			s.m[uint64(w)<<6+uint64(bits.TrailingZeros64(b))] = struct{}{}
			word ^= b
		}
	}
	s.bits = nil
	s.mode = modeSparse
}

// grow widens the bitset to at least words, doubling so a walk climbing through
// a range does not reallocate per ID.
func (s *idSet) grow(words uint64) {
	n := uint64(len(s.bits))
	if n == 0 {
		n = 1
	}
	for n < words {
		n *= 2
	}
	next := make([]uint64, n)
	copy(next, s.bits)
	s.bits = next
}

// reset empties the set while keeping whatever storage it has reached, for a
// caller reusing one across passes. The mode is kept with it: a set that spilled
// once is likely to spill again, and rebuilding the inline array only to throw
// it away is the churn this exists to avoid.
func (s *idSet) reset() {
	s.n = 0
	switch s.mode {
	case modeDense:
		clear(s.bits)
	case modeSparse:
		clear(s.m)
	}
}
