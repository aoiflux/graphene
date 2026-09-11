package merkle

// Computing the tree head one leaf at a time.
//
// Root takes every leaf at once, which is the right shape when the leaves are
// already in hand and are wanted again for an inclusion proof. It is the wrong
// shape when the leaves are a graph: a snapshot root over 1.5M nodes, 2M edges
// and 28M index entries costs 32 B per leaf held simultaneously — roughly a
// gigabyte of hashes whose only use is to be folded into thirty-two bytes — and
// the records they are computed from are streaming past on their way into the
// image anyway.
//
// RootBuilder is the standard Certificate Transparency construction for exactly
// this: the tree head of n leaves is the concatenation of perfect subtrees whose
// sizes are the set bits of n, so only one hash per set bit ever has to be
// retained. That is at most 64 hashes for any tree this engine could build, and
// the value it produces is the same value Root produces — not an approximation
// of it, and not a different tree that happens to be cheaper.
//
// The equality is load-bearing rather than incidental: a root is the one number
// SECURITY.md tells a holder to retain, so a builder that disagreed with Root
// for even one leaf count would silently invalidate every retained root written
// through it. TestRootBuilder_EqualsRoot sweeps every size from 0 to 1025 and
// FuzzRootBuilderMatchesRoot sweeps arbitrary ones.

import (
	"crypto/sha256"
	"hash"
	"math/bits"
)

// RootBuilder computes the RFC 6962 Merkle tree hash incrementally, in memory
// proportional to the logarithm of the leaf count.
//
// The zero value is an empty tree and is ready to use. Add leaf hashes in the
// same order Root would take them; Root may be called at any point and does not
// end the tree, so a builder can be read and then added to.
type RootBuilder struct {
	// stack holds the root of one perfect subtree per set bit of n, largest
	// first. A subtree is merged the moment its sibling completes, which is
	// what keeps the depth logarithmic rather than linear.
	stack []Hash

	// n is how many leaves have been added. Its binary representation *is* the
	// stack's shape, which is what lets Add decide how many merges a new leaf
	// triggers without recording each subtree's size beside it.
	n uint64

	// h is the SHA-256 state every hash this builder computes reuses — the
	// leaves AddLeafData takes and the internal nodes Add and Root produce.
	// Built on first use, so the zero value is still a usable empty tree. sum
	// is its output buffer: Sum appends, and giving it somewhere with capacity
	// is the difference between no allocation and one per hash.
	//
	// The package-level HashLeaf and hashInternal build a hasher per call,
	// which is right for a caller hashing one thing and wrong for a caller
	// hashing tens of millions. Folding 28M leaves would otherwise allocate a
	// fresh SHA-256 state per leaf *and* per internal node.
	h   hash.Hash
	sum [Size]byte

	// node is the internal-node preimage, 0x01 followed by the two child
	// hashes, staged in a field rather than written as three slices of local
	// arrays. A [Size]byte local sliced into an interface method escapes, which
	// would be two allocations for every internal node in the tree.
	node [1 + 2*Size]byte
}

// hasher returns the builder's reusable SHA-256 state, reset and ready.
func (b *RootBuilder) hasher() hash.Hash {
	if b.h == nil {
		b.h = sha256.New()
	}
	b.h.Reset()
	return b.h
}

// internal is hashInternal against that state. It must agree with hashInternal
// exactly — the two compute the same tree, reached by different callers.
func (b *RootBuilder) internal(left, right Hash) Hash {
	b.node[0] = internalPrefix
	copy(b.node[1:], left[:])
	copy(b.node[1+Size:], right[:])

	h := b.hasher()
	h.Write(b.node[:])
	var out Hash
	copy(out[:], h.Sum(b.sum[:0]))
	return out
}

// Add appends one leaf hash, which must already be a HashLeaf result for the
// same reason Root's argument must be: the leaf prefix is the domain separation
// between a leaf and an internal node, and hashing it here as well would put
// this tree in a different domain from every other one in the engine.
func (b *RootBuilder) Add(leaf Hash) {
	b.n++

	// Every trailing zero of the new count is a level at which this leaf has
	// just completed a perfect subtree, so its sibling is on top of the stack
	// and the two merge. Counting them from n rather than from the stack is
	// what makes this exact: n=4 merges twice and n=5 merges not at all, which
	// is the difference between RFC 6962's shape and the naive pairing tree.
	h := leaf
	for merges := bits.TrailingZeros64(b.n); merges > 0; merges-- {
		last := len(b.stack) - 1
		h = b.internal(b.stack[last], h)
		b.stack = b.stack[:last]
	}
	b.stack = append(b.stack, h)
}

// leafPrefixBytes is the leaf domain separation tag as a slice a hasher can
// take without one being built per call. The internal-node tag lives in the
// builder's own preimage buffer instead; see RootBuilder.node.
var leafPrefixBytes = [...]byte{leafPrefix}

// AddLeafData appends the leaf whose contents are data. It is
// Add(HashLeaf(data)) with the SHA-256 state reused across calls.
//
// HashLeaf builds a fresh hasher and allocates a fresh sum for every leaf. That
// is the right shape for a caller hashing one leaf, and it is invisible beside
// a []Hash holding every leaf in the tree — which is exactly what stops being
// there once a root is streamed. On a compaction it would otherwise be three
// allocations per node, per edge and per property-index entry, which is the
// cost this type exists to remove.
//
// data is read and not retained, so the caller may reuse the buffer it was
// built in. The image writer does.
func (b *RootBuilder) AddLeafData(data []byte) {
	h := b.hasher()
	h.Write(leafPrefixBytes[:])
	h.Write(data)

	var leaf Hash
	copy(leaf[:], h.Sum(b.sum[:0]))
	b.Add(leaf)
}

// Len returns the number of leaves added.
func (b *RootBuilder) Len() uint64 { return b.n }

// Reset empties the builder, keeping the stack's capacity so a caller computing
// several roots in sequence allocates once.
func (b *RootBuilder) Reset() {
	b.stack = b.stack[:0]
	b.n = 0
}

// Root returns the tree hash over every leaf added so far. It equals
// Root(leaves) over the same sequence, including the two degenerate cases:
// EmptyRoot for no leaves, and the leaf itself for exactly one.
//
// It does not consume the builder. The fold is right-associative over the
// retained subtrees — RFC 6962 splits at the largest power of two below n, so
// the largest subtree is always the left child of the root and every smaller
// one hangs off the right spine — and it runs on a copy of nothing, so calling
// it twice gives the same answer twice.
func (b *RootBuilder) Root() Hash {
	if b.n == 0 {
		return EmptyRoot()
	}
	h := b.stack[len(b.stack)-1]
	for i := len(b.stack) - 2; i >= 0; i-- {
		h = b.internal(b.stack[i], h)
	}
	return h
}
