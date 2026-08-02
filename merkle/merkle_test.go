package merkle

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
)

// leaves builds n distinct leaf hashes.
func leaves(n int) []Hash {
	out := make([]Hash, n)
	for i := range out {
		out[i] = HashLeaf([]byte(fmt.Sprintf("leaf-%d", i)))
	}
	return out
}

// RFC 6962 §2.1 pins the empty tree's hash to SHA-256 of the empty string.
func TestEmptyRoot_MatchesTheSpecification(t *testing.T) {
	want, _ := hex.DecodeString("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	got := EmptyRoot()
	if !bytes.Equal(got[:], want) {
		t.Fatalf("empty root = %x, want %x", got, want)
	}
}

// A single-leaf tree's root is that leaf, so an inclusion proof for it is empty.
func TestRoot_SingleLeaf(t *testing.T) {
	l := leaves(1)
	if Root(l) != l[0] {
		t.Fatal("a one-leaf tree's root must be the leaf")
	}
	p, err := BuildProof(l, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Siblings) != 0 {
		t.Fatalf("proof for a one-leaf tree carries %d siblings, want 0", len(p.Siblings))
	}
	if !VerifyProof(Root(l), l[0], p) {
		t.Fatal("single-leaf proof did not verify")
	}
}

// Every leaf of every tree size in range must produce a verifying proof.
func TestProof_VerifiesForEveryLeafAndSize(t *testing.T) {
	for n := 1; n <= 64; n++ {
		l := leaves(n)
		root := Root(l)
		for i := 0; i < n; i++ {
			p, err := BuildProof(l, i)
			if err != nil {
				t.Fatalf("n=%d i=%d: %v", n, i, err)
			}
			if !VerifyProof(root, l[i], p) {
				t.Fatalf("n=%d i=%d: valid proof did not verify", n, i)
			}
		}
	}
}

// A proof must not verify a leaf that is not in the tree.
func TestProof_RejectsAForeignLeaf(t *testing.T) {
	l := leaves(16)
	root := Root(l)
	outsider := HashLeaf([]byte("not-in-the-tree"))

	for i := 0; i < len(l); i++ {
		p, _ := BuildProof(l, i)
		if VerifyProof(root, outsider, p) {
			t.Fatalf("i=%d: a leaf outside the tree verified", i)
		}
	}
}

// A proof is bound to its position: replaying it at another index must fail.
// Without that, one valid proof would attest to every leaf.
func TestProof_IsBoundToItsPosition(t *testing.T) {
	l := leaves(16)
	root := Root(l)

	p, _ := BuildProof(l, 3)
	for i := 0; i < len(l); i++ {
		if i == 3 {
			continue
		}
		moved := p
		moved.Index = i
		if VerifyProof(root, l[3], moved) {
			t.Fatalf("proof for leaf 3 verified when relabelled as leaf %d", i)
		}
	}
}

// Truncating a proof must fail rather than verify against a subtree root.
func TestProof_RejectsTruncation(t *testing.T) {
	l := leaves(32)
	root := Root(l)

	p, _ := BuildProof(l, 9)
	for cut := 0; cut < len(p.Siblings); cut++ {
		short := p
		short.Siblings = p.Siblings[:cut]
		if VerifyProof(root, l[9], short) {
			t.Fatalf("a proof truncated to %d of %d siblings verified", cut, len(p.Siblings))
		}
	}
}

// Corrupting any sibling must break the proof.
func TestProof_RejectsACorruptSibling(t *testing.T) {
	l := leaves(32)
	root := Root(l)
	p, _ := BuildProof(l, 20)

	for i := range p.Siblings {
		damaged := Proof{Index: p.Index, Size: p.Size, Siblings: append([]Hash(nil), p.Siblings...)}
		damaged.Siblings[i][0] ^= 0x01
		if VerifyProof(root, l[20], damaged) {
			t.Fatalf("flipping a bit in sibling %d still verified", i)
		}
	}
}

// **The forgery RFC 6962 exists to prevent.**
//
// Under a naive tree that duplicates the last leaf on an odd level, the leaf
// list [a b c] and [a b c c] produce the same root — so a root does not identify
// its contents and a proof can be constructed for a leaf that was never added.
// Different leaf counts must give different roots.
func TestRoot_DuplicatedTailDoesNotCollide(t *testing.T) {
	three := leaves(3)
	four := append(append([]Hash(nil), three...), three[2])

	if Root(three) == Root(four) {
		t.Fatal("[a b c] and [a b c c] share a root — the tree is vulnerable to the " +
			"duplicate-tail forgery RFC 6962's split rule prevents")
	}
}

// Domain separation: an internal node's preimage must not be presentable as a
// leaf. Without the prefixes, H(left‖right) could be claimed as leaf data.
func TestHash_LeafAndInternalAreSeparated(t *testing.T) {
	a, b := HashLeaf([]byte("a")), HashLeaf([]byte("b"))
	internal := hashInternal(a, b)

	// The same bytes hashed as a leaf must give a different result.
	asLeaf := HashLeaf(append(append([]byte(nil), a[:]...), b[:]...))
	if internal == asLeaf {
		t.Fatal("an internal node and a leaf over the same bytes collide — " +
			"the domain-separation prefixes are not doing their job")
	}
}

// The root must depend on leaf order: a tree is a sequence, not a set.
func TestRoot_DependsOnOrder(t *testing.T) {
	l := leaves(8)
	swapped := append([]Hash(nil), l...)
	swapped[0], swapped[1] = swapped[1], swapped[0]

	if Root(l) == Root(swapped) {
		t.Fatal("swapping two leaves left the root unchanged")
	}
}

// Changing any single leaf must change the root.
func TestRoot_ChangesWithEveryLeaf(t *testing.T) {
	l := leaves(24)
	base := Root(l)

	for i := range l {
		altered := append([]Hash(nil), l...)
		altered[i][0] ^= 0x01
		if Root(altered) == base {
			t.Fatalf("altering leaf %d did not change the root", i)
		}
	}
}

// Roots are stable across runs — the tree is a pure function of its leaves, with
// no map iteration or other ordering hidden inside it.
func TestRoot_IsDeterministic(t *testing.T) {
	l := leaves(50)
	first := Root(l)
	for i := 0; i < 10; i++ {
		if Root(l) != first {
			t.Fatal("Root is not deterministic")
		}
	}
}

// Out-of-range proof requests are refused rather than returning something that
// looks usable.
func TestBuildProof_RejectsBadIndex(t *testing.T) {
	l := leaves(8)
	for _, i := range []int{-1, 8, 100} {
		if _, err := BuildProof(l, i); err == nil {
			t.Fatalf("BuildProof accepted index %d for an 8-leaf tree", i)
		}
	}
}

// A proof claiming an impossible shape must be rejected without hashing.
func TestVerifyProof_RejectsImpossibleShapes(t *testing.T) {
	l := leaves(8)
	root := Root(l)
	good, _ := BuildProof(l, 2)

	for _, bad := range []Proof{
		{Index: 2, Size: 0, Siblings: good.Siblings},
		{Index: -1, Size: 8, Siblings: good.Siblings},
		{Index: 8, Size: 8, Siblings: good.Siblings},
		{Index: 2, Size: 8, Siblings: append(good.Siblings, Hash{})},
	} {
		if VerifyProof(root, l[2], bad) {
			t.Fatalf("verified an impossible proof: index=%d size=%d siblings=%d",
				bad.Index, bad.Size, len(bad.Siblings))
		}
	}
}
