package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// The GCPX pass: everything the read paths assume about the composite section
// and deliberately do not re-check.
//
// parseGCPX bounds every extent the directory names, so a damaged section makes
// a lookup fail rather than address bytes outside the body. What it does not
// establish is that the section is *consistent*: that the tuples ascend the way
// the binary search assumes, that each tuple table entry's prefix is that
// tuple's, that a run's ids ascend the way a merge assumes, and that the counts
// in the directory are the counts the bytes hold. Each of those is a pass over
// every entry, and charging every open for one is the cost this whole design
// exists to remove. So they are here, with the rest of R3e's verification, and
// index.Base.Verify's comment is the argument for the split.
//
// # What is not checked here, and why
//
// That the composite agrees with the property entries it was derived from. A
// tuple is a conjunction over member keys, so checking one entry would mean a
// lookup per member per entity — an O(entries × width) pass over GPIX, which is
// a different and much larger check than this one. It is also the check that
// matters least: the two are written in the same pass from the same index and
// installed together, which is the staleness argument csr_gcpx.go makes at
// length, and nothing can install one without the other.
//
// What that leaves genuinely unverified is a writer that computed the wrong
// tuples. A verifier cannot separate that from a caller who registered those
// tuples, because the composite *is* the caller's registrations crossed with
// each other — so the check would have to rebuild the composite to compare
// against, which is the work the section removes. Stated rather than left to be
// discovered.

// verifyGCPX checks the composite section's own structure.
//
// Memory is bounded by the widest tuple and does not grow with the section, for
// the reason index.Base.Verify states as a requirement rather than an
// observation: this is what an operator runs on a store that is already near the
// ceiling this programme is aimed at.
func verifyGCPX(s *gcpxSection, cc *store.CancelCheck) error {
	if s == nil {
		return nil
	}
	for i := range s.composites {
		if err := verifyGCPXComposite(&s.composites[i], cc); err != nil {
			return err
		}
	}
	// The directory's order. Nothing reads it — find scans linearly, which is
	// cheaper than a binary search over the handful of composites a store
	// declares — so this is checked for byte determinism rather than for a
	// search: two compactions of the same content must produce the same bytes,
	// and an order no read depends on is exactly where that stops being true
	// without anything noticing.
	for i := 1; i < len(s.composites); i++ {
		prev, cur := &s.composites[i-1], &s.composites[i]
		if prev.kind > cur.kind {
			return fmt.Errorf("gcpx: directory is out of order: kind %d after kind %d",
				cur.kind, prev.kind)
		}
		if prev.kind < cur.kind {
			continue
		}
		if bytes.Compare(gcpxEncodeKeys(prev.keys), gcpxEncodeKeys(cur.keys)) >= 0 {
			return fmt.Errorf("gcpx: directory is out of order within kind %d: %v after %v",
				cur.kind, cur.keys, prev.keys)
		}
	}
	return nil
}

// verifyGCPXComposite checks one composite's tuple table against its runs.
func verifyGCPXComposite(c *gcpxComposite, cc *store.CancelCheck) error {
	if err := cc.Check(); err != nil {
		return err
	}
	kind := index.EntityKind(c.kind)

	// The runs region must be tiled exactly: the first run starts at its
	// beginning and the sentinel ends at its end. Slack either side would be
	// bytes the digest covers that no read can reach — a writer whose two
	// arithmetics have drifted, or a section assembled by something that is not
	// this writer. verifyGPIXKey states the same requirement for the same reason.
	if c.distinct > 0 {
		if off := gcpxRunOffAt(c, 0); off != 0 {
			return fmt.Errorf("gcpx: %s composite %v begins its runs at %d rather than 0",
				kind, c.keys, off)
		}
	}
	if off := gcpxRunOffAt(c, c.distinct); off != uint64(len(c.runs)) {
		return fmt.Errorf("gcpx: %s composite %v has %d bytes of runs, its tuple table ends at %d",
			kind, c.keys, len(c.runs), off)
	}
	if p := gcpxPrefixAt(c, c.distinct); p != ^uint64(0) {
		return fmt.Errorf("gcpx: %s composite %v has sentinel prefix %#x", kind, c.keys, p)
	}

	// Grows to the widest tuple and is reused. The one allocation here that is
	// not flat in the entries, and it is bounded by the same thing that bounds
	// the writer's.
	var prev []byte
	havePrev := false
	var entries uint64

	for i := uint64(0); i < c.distinct; i++ {
		if err := cc.Step(); err != nil {
			return err
		}
		tuple, ids, err := c.runAt(gcpxRunOffAt(c, i))
		if err != nil {
			return err
		}
		// The prefix the search compares before it reads anything must be this
		// tuple's. A table whose prefixes disagreed with its runs would send a
		// search down the wrong half and answer "absent" for a present tuple —
		// bounded, silent and wrong, which is the class this pass exists for.
		if got, want := gcpxPrefixAt(c, i), gcpxPrefixOf(tuple); got != want {
			return fmt.Errorf("gcpx: %s composite %v entry %d has prefix %#x for a tuple whose prefix is %#x",
				kind, c.keys, i, got, want)
		}
		// Strictly ascending, which is what the binary search assumes and what
		// makes a repeated tuple unreachable rather than merely redundant.
		if havePrev && bytes.Compare(prev, tuple) >= 0 {
			return fmt.Errorf("gcpx: %s composite %v tuples are not ascending at %d: %x after %x",
				kind, c.keys, i, tuple, prev)
		}
		prev, havePrev = append(prev[:0], tuple...), true

		// A tuple's width must be the composite's. The encoding is
		// length-prefixed, so this is arithmetic over the tuple rather than a
		// decode, and it catches the one mismatch a bounds check cannot: a run
		// belonging to a different composite.
		n, err := gcpxTupleWidth(tuple)
		if err != nil {
			return fmt.Errorf("gcpx: %s composite %v entry %d: %w", kind, c.keys, i, err)
		}
		if n != len(c.keys) {
			return fmt.Errorf("gcpx: %s composite %v entry %d holds %d members, want %d",
				kind, c.keys, i, n, len(c.keys))
		}

		run := index.NewIDRun(ids)
		if len(ids)%8 != 0 {
			return fmt.Errorf("gcpx: %s composite %v entry %d has %d id bytes, not a multiple of 8",
				kind, c.keys, i, len(ids))
		}
		if run.Len() == 0 {
			// A tuple no entity holds is not in the index, so the writer does not
			// write one. Its presence means a writer that did not skip an empty
			// merge, and the next compaction of the same content would not
			// produce it — a difference in bytes the digest covers.
			return fmt.Errorf("gcpx: %s composite %v entry %d holds no ids", kind, c.keys, i)
		}
		for j := 1; j < run.Len(); j++ {
			if run.At(j) <= run.At(j-1) {
				return fmt.Errorf("gcpx: %s composite %v entry %d has ids out of order at %d: %d after %d",
					kind, c.keys, i, j, run.At(j), run.At(j-1))
			}
		}
		entries += uint64(run.Len())
	}

	if entries != c.entries {
		return fmt.Errorf("gcpx: %s composite %v holds %d entries, its directory claims %d",
			kind, c.keys, entries, c.entries)
	}
	return nil
}

// gcpxRunOffAt is the i'th tuple table entry's run offset. Bounded by parseGCPX,
// which sized the table at (distinct+1) entries.
func gcpxRunOffAt(c *gcpxComposite, i uint64) uint64 {
	return binary.LittleEndian.Uint64(c.ttab[i*gcpxTtabEntry+8:])
}

// gcpxPrefixAt is the i'th entry's search prefix.
func gcpxPrefixAt(c *gcpxComposite, i uint64) uint64 {
	return binary.BigEndian.Uint64(c.ttab[i*gcpxTtabEntry:])
}

// gcpxTupleWidth counts the members of an encoded tuple without decoding them.
//
// The encoding is index's and is opaque to this package everywhere else — see
// index.CompositeBase for why that separation is load-bearing. Counting the
// length prefixes is the one thing that can be done to a tuple here without
// knowing what a value means, and it is enough for the check this is for.
func gcpxTupleWidth(tuple []byte) (int, error) {
	n := 0
	for off := 0; off < len(tuple); n++ {
		if off+4 > len(tuple) {
			return 0, fmt.Errorf("tuple of %d bytes has a truncated length prefix at %d",
				len(tuple), off)
		}
		l := int(binary.LittleEndian.Uint32(tuple[off:]))
		off += 4
		if l < 0 || off+l > len(tuple) {
			return 0, fmt.Errorf("tuple member %d claims %d bytes, %d remain", n, l, len(tuple)-off)
		}
		off += l
	}
	return n, nil
}
