package disk

import (
	"github.com/aoiflux/graphene/index"
)

// The composite half of the base: gpixBase reading GCPX as an index.CompositeBase.
//
// # Why this is a separate interface and not three more methods on index.Base
//
// Because a base that carries no composite postings is the normal case and must
// stay the cheap one. Every v9 image written before GCPX existed carries GPIX and
// GPIR and nothing else, and a reader meeting one has to fill the composites from
// the entries exactly as it always did. index.CompositeBase is therefore a
// capability the index asks a base for by type assertion: a gpixBase with no
// GCPX section fails the assertion, the fill runs, and no version number is
// consulted anywhere.
//
// That is also why the assertion is on this file's methods and not on a field.
// The implementation below is nil-safe per composite rather than per base: a
// store may declare a composite the image does not carry — declared after the
// last compaction, say — and that one composite is filled while its neighbours
// are read in place. find returns nil for it and every method here treats nil as
// "not carried", which is the same answer index.Base gives for an absent key and
// is not an error.
//
// # What is resident
//
// One gcpxComposite per composite the image carries, holding two subslices of the
// mapping and four scalars. A composite over 1,400,000 entities costs the same as
// an empty one, which is the whole item: at that shape the resident composites
// were 128.2 MiB of the 237.8 MiB a default configuration held — 53.9%, and the
// largest single term in the column. See docs/MEMORY_MODEL.md §8.6.

// var _ index.CompositeBase = (*gpixBase)(nil) is asserted at the bottom of this
// file, after the methods it needs are in scope.

// CompositeTuples returns the key tuples this image carries postings for.
//
// The order is the directory's, which writeGCPX sorted by (kind, encoded key
// tuple) so that two compactions of the same content produce the same bytes. No
// caller depends on it — index.carriedComposites builds a set out of it — but a
// list whose order is a function of the content rather than of a map iteration is
// the property the whole image is written under.
func (b *gpixBase) CompositeTuples(kind index.EntityKind) [][]string {
	if b.cmp == nil {
		return nil
	}
	dk := kindOf(kind)
	var out [][]string
	for i := range b.cmp.composites {
		c := &b.cmp.composites[i]
		if c.kind != dk {
			continue
		}
		// Copied, because c.keys is decoded once at parse and shared by every
		// caller: handing out the slice would let one of them sort it.
		t := make([]string, len(c.keys))
		copy(t, c.keys)
		out = append(out, t)
	}
	return out
}

// LookupComposite returns the ids filed under one encoded tuple, ascending.
//
// The tuple is index's encoding and is opaque here — it arrives as bytes, it is
// compared as bytes, and this side neither parses it nor builds one. See
// index.CompositeBase for why that separation is load-bearing rather than tidy:
// the encoding is little-endian length-prefixed and therefore not
// order-preserving with respect to the values, so a second implementation of it
// that disagreed by a byte would make the binary search below bounded, silent and
// wrong.
//
// A composite this image does not carry, or a tuple it never saw, yields an empty
// run and no error. That is index.Base.Lookup's contract for an absent key and it
// holds for the same reason: the delta may well hold the tuple, and the merge on
// the other side is written once for both cases.
func (b *gpixBase) LookupComposite(kind index.EntityKind, keys []string, tuple []byte) (index.IDRun, error) {
	if b.cmp == nil {
		return index.IDRun{}, nil
	}
	c := b.cmp.find(kindOf(kind), keys)
	if c == nil {
		return index.IDRun{}, nil
	}
	ids, err := c.lookup(tuple)
	if err != nil {
		return index.IDRun{}, err
	}
	return index.NewIDRun(ids), nil
}

// ForEachCompositeTuple walks one composite's distinct tuples in ascending byte
// order, with the ids filed under each.
//
// The ascending order is the writer's promise and is not re-checked here, for the
// reason gpixBase's own comment gives: establishing it is an O(entries) pass and
// that pass is verifyGCPX's. The one caller that depends on it is a compaction
// merging this against the delta's sorted tuples in a single pass, and a base
// that broke the order there would produce a next image whose tuples do not
// ascend — which is damage propagating rather than damage detected, and is
// exactly what running the verifier before trusting an image is for.
func (b *gpixBase) ForEachCompositeTuple(kind index.EntityKind, keys []string,
	fn func(tuple []byte, ids index.IDRun) bool) error {
	if b.cmp == nil {
		return nil
	}
	c := b.cmp.find(kindOf(kind), keys)
	if c == nil {
		return nil
	}
	return c.forEachRun(func(tuple []byte, ids []byte) bool {
		return fn(tuple, index.NewIDRun(ids))
	})
}

// compositeMappedBytes is GCPX's share of the mapping, for MappedBytes.
func (b *gpixBase) compositeMappedBytes() int64 {
	if b.cmp == nil {
		return 0
	}
	return int64(len(b.cmp.body))
}

var _ index.CompositeBase = (*gpixBase)(nil)
