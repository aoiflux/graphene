// Package merkle implements the RFC 6962 Merkle tree used for snapshot roots
// and inclusion proofs.
//
// # Why RFC 6962 rather than a naive tree
//
// The obvious construction — hash pairs, duplicate the last leaf when a level
// has an odd count — has a known forgery: duplicating a leaf produces the same
// root as a tree that genuinely contained it twice, so two different leaf lists
// can share a root. That is the Bitcoin CVE-2012-2459 shape, and a root that two
// different contents can produce is not an identity.
//
// RFC 6962 avoids it two ways, and both matter:
//
//   - **Domain separation.** A leaf hashes H(0x00 ‖ data); an internal node
//     hashes H(0x01 ‖ left ‖ right). Without the prefixes an attacker can present
//     an internal node's preimage as a leaf, which is a second-preimage attack on
//     the tree rather than on the hash.
//   - **Split at the largest power of two below n**, rather than pairing left to
//     right and promoting an odd remainder. This makes the tree shape a function
//     of the leaf count alone, so no two leaf lists of different lengths share a
//     structure.
//
// The construction is deliberately conservative: this is the component whose
// soundness everything else rests on, and plan hook V-07 earmarks it for a
// machine-checked proof. Following a specification that has been analysed for a
// decade is worth more here than anything clever.
//
// Stdlib only, per the engine's zero-dependency constraint.
package merkle

import (
	"crypto/sha256"
	"errors"
	"math/bits"
)

// Size is the length of a hash in bytes.
const Size = sha256.Size

// Hash is a SHA-256 digest.
type Hash [Size]byte

// Domain separation prefixes, per RFC 6962 §2.1.
const (
	leafPrefix     = 0x00
	internalPrefix = 0x01
)

// HashLeaf returns the leaf hash for data: H(0x00 ‖ data).
//
// Callers hash *records*, not already-hashed values — passing a digest in here
// is not wrong, but it means the leaf prefix protects the outer hash and not the
// inner one, so the caller owns that separation.
func HashLeaf(data []byte) Hash {
	h := sha256.New()
	h.Write([]byte{leafPrefix})
	h.Write(data)
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// hashInternal returns H(0x01 ‖ left ‖ right).
func hashInternal(left, right Hash) Hash {
	h := sha256.New()
	h.Write([]byte{internalPrefix})
	h.Write(left[:])
	h.Write(right[:])
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// EmptyRoot is the root of a tree with no leaves: SHA-256 of the empty string,
// per RFC 6962 §2.1. Defined so that "no entities" has a stable identity rather
// than being a special case every caller handles.
func EmptyRoot() Hash {
	return sha256.Sum256(nil)
}

// Root returns the Merkle tree hash over leaves, which must already be leaf
// hashes from HashLeaf.
//
// Taking leaf hashes rather than raw data lets a caller compute them once and
// reuse them for both the root and any inclusion proof, which matters when the
// leaves are entity records and there are millions of them.
func Root(leaves []Hash) Hash {
	if len(leaves) == 0 {
		return EmptyRoot()
	}
	if len(leaves) == 1 {
		return leaves[0]
	}
	k := splitPoint(len(leaves))
	return hashInternal(Root(leaves[:k]), Root(leaves[k:]))
}

// splitPoint returns the largest power of two strictly less than n, which is
// where RFC 6962 divides a level.
func splitPoint(n int) int {
	if n < 2 {
		return 0
	}
	return 1 << (bits.Len(uint(n-1)) - 1)
}

// Proof is an inclusion proof: the sibling hashes needed to recompute the root
// from one leaf, ordered from the leaf upwards.
type Proof struct {
	// Index is the leaf's position, and Size the total leaf count. Both are
	// needed to verify: they determine which side each sibling sits on, so a
	// proof cannot be replayed at a different position.
	Index int
	Size  int

	// Siblings, leaf-most first.
	Siblings []Hash
}

// ErrProofInvalid is returned when a proof cannot be checked at all — as opposed
// to checking cleanly and disagreeing with the root.
var ErrProofInvalid = errors.New("merkle: malformed inclusion proof")

// BuildProof returns an inclusion proof for the leaf at index.
func BuildProof(leaves []Hash, index int) (Proof, error) {
	if index < 0 || index >= len(leaves) {
		return Proof{}, ErrProofInvalid
	}
	p := Proof{Index: index, Size: len(leaves)}
	p.Siblings = appendPath(nil, leaves, index)
	return p, nil
}

// appendPath collects sibling hashes from the leaf up to the root.
func appendPath(dst []Hash, leaves []Hash, index int) []Hash {
	if len(leaves) < 2 {
		return dst
	}
	k := splitPoint(len(leaves))
	if index < k {
		dst = appendPath(dst, leaves[:k], index)
		return append(dst, Root(leaves[k:]))
	}
	dst = appendPath(dst, leaves[k:], index-k)
	return append(dst, Root(leaves[:k]))
}

// VerifyProof reports whether leaf sits at p.Index in a tree of p.Size leaves
// whose root is root.
//
// This is the function a third party runs holding only the leaf, the proof, and
// an independently obtained root — no database, no other leaves. That is what
// makes an attestation transferable, so it deliberately depends on nothing else
// in the engine.
func VerifyProof(root, leaf Hash, p Proof) bool {
	if p.Size <= 0 || p.Index < 0 || p.Index >= p.Size {
		return false
	}

	// Work out the descent from the root down: at each level, is the target leaf
	// in the left half? This is a function of Index and Size alone, which is what
	// binds a proof to its position — a proof relabelled to another index
	// produces a different descent and therefore combines its siblings in a
	// different order.
	descent := descentPath(p.Size, p.Index)

	// The path length follows from the tree's shape, so a proof with the wrong
	// number of siblings is rejected before any hashing. Without this a truncated
	// proof could verify against a subtree root.
	if len(p.Siblings) != len(descent) {
		return false
	}

	// Siblings are ordered leaf-most first while the descent was computed
	// root-most first, so the descent is consumed in reverse. Getting this
	// backwards still verifies for some shapes, which is why the tests sweep
	// every size and index rather than checking one tree.
	computed := leaf
	for i, sib := range p.Siblings {
		if descent[len(descent)-1-i] {
			computed = hashInternal(computed, sib) // target was on the left
		} else {
			computed = hashInternal(sib, computed)
		}
	}
	return computed == root
}

// descentPath returns, for each level from the root down, whether index falls in
// the left half of the range still under consideration.
func descentPath(n, index int) []bool {
	var out []bool
	for n > 1 {
		k := splitPoint(n)
		if index < k {
			out = append(out, true)
			n = k
		} else {
			out = append(out, false)
			index -= k
			n -= k
		}
	}
	return out
}
