package merkle

import (
	"testing"
)

// The soundness property, fuzzed: a proof verifies against a root if and only if
// it is a genuine proof for that leaf at that position in that tree.
//
// Plan hook V-07 earmarks this for a machine-checked proof. Until there is one,
// an exhaustive-ish search over shapes and mutations is the available evidence,
// and it is worth more than the example-based tests alone — the two bugs found
// while writing this package both survived a happy-path test and died to a sweep
// over every size and index.
//
//	go test ./merkle/ -run=XXX -fuzz=FuzzProofSoundness -fuzztime=60s
func FuzzProofSoundness(f *testing.F) {
	f.Add(uint16(1), uint16(0), uint8(0), uint8(0))
	f.Add(uint16(3), uint16(1), uint8(0), uint8(0))
	f.Add(uint16(8), uint16(5), uint8(1), uint8(2))
	f.Add(uint16(100), uint16(63), uint8(2), uint8(7))

	f.Fuzz(func(t *testing.T, rawSize, rawIndex uint16, mutation, target uint8) {
		// Bound the tree so the fuzzer explores shapes rather than sizes.
		size := int(rawSize%512) + 1
		index := int(rawIndex) % size

		l := leaves(size)
		root := Root(l)

		p, err := BuildProof(l, index)
		if err != nil {
			t.Fatalf("BuildProof failed for a valid index: size=%d index=%d: %v", size, index, err)
		}

		// A genuine proof must always verify.
		if !VerifyProof(root, l[index], p) {
			t.Fatalf("a genuine proof did not verify: size=%d index=%d", size, index)
		}

		// Any single mutation must break it. Each case below is a way an
		// attacker might try to reuse or reshape a valid proof.
		switch mutation % 4 {
		case 0: // move the proof to another position
			if size < 2 {
				return
			}
			other := (index + 1 + int(target)) % size
			if other == index {
				return
			}
			moved := Proof{Index: other, Size: p.Size, Siblings: p.Siblings}
			if VerifyProof(root, l[index], moved) {
				t.Fatalf("proof for index %d verified at index %d (size=%d)", index, other, size)
			}

		case 1: // claim a different tree size
			other := int(rawSize%1024) + 1
			if other == size {
				return
			}
			resized := Proof{Index: p.Index, Size: other, Siblings: p.Siblings}
			if resized.Index < resized.Size && VerifyProof(root, l[index], resized) {
				t.Fatalf("proof verified under a claimed size of %d, actual %d", other, size)
			}

		case 2: // corrupt one sibling
			if len(p.Siblings) == 0 {
				return
			}
			i := int(target) % len(p.Siblings)
			damaged := Proof{Index: p.Index, Size: p.Size, Siblings: append([]Hash(nil), p.Siblings...)}
			damaged.Siblings[i][int(target)%Size] ^= 0x01
			if VerifyProof(root, l[index], damaged) {
				t.Fatalf("proof verified with sibling %d corrupted (size=%d index=%d)", i, size, index)
			}

		case 3: // substitute a different leaf
			if size < 2 {
				return
			}
			other := (index + 1 + int(target)) % size
			if other == index {
				return
			}
			if VerifyProof(root, l[other], p) {
				t.Fatalf("leaf %d verified against a proof for leaf %d (size=%d)", other, index, size)
			}
		}
	})
}

// Roots must be injective over leaf content: two different leaf lists must not
// share a root. This is the property the duplicate-tail forgery breaks.
func FuzzRootDistinguishesContent(f *testing.F) {
	f.Add(uint16(3), uint16(4))
	f.Add(uint16(8), uint16(9))
	f.Add(uint16(1), uint16(2))

	f.Fuzz(func(t *testing.T, rawA, rawB uint16) {
		a := int(rawA%256) + 1
		b := int(rawB%256) + 1
		if a == b {
			return
		}
		if Root(leaves(a)) == Root(leaves(b)) {
			t.Fatalf("trees of %d and %d distinct leaves share a root", a, b)
		}
	})
}
