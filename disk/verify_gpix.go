package disk

// The bounded structural check over a mapped property index.
//
// # What this is for
//
// Everything else that touches GPIX and GPIR establishes only that a read cannot
// address memory outside the section. parseGPIX bounds the directories, runAt
// bounds a run's own length prefixes against its extent, valueOf bounds a
// reverse entry against its key's runs — and that is where it stops, deliberately,
// because the remaining invariants are the ones whose check is a pass over every
// entry in the index. A reader that paid for that pass would charge every query
// for the file being trustworthy.
//
// So they are checked here, once, by whoever asks. The invariants are exactly the
// ones the read paths assume and cannot verify cheaply:
//
//   - a key's value table is tiled by its runs with no gap and no slack, so every
//     byte of the runs region is reachable from the table;
//   - each vtab entry's 8-byte prefix is the prefix of the value its run holds,
//     which is what the binary search compares before it reads anything;
//   - values ascend strictly by bytes.Compare, which is what makes that search
//     correct and is the ordering §15.8 fixes for the index as a whole;
//   - a run's ids ascend strictly, which is what IDRun promises its callers;
//   - the directory's Distinct and Entries counts are what the runs hold, which
//     is what the planner weighs a key's selectivity by;
//   - every reverse entry addresses the start of a real run of a key of its own
//     kind, agrees with that run's value length, and names an id the run holds;
//   - and the two directions agree in number, per key.
//
// # Why the last two together are a complete cross-check
//
// The reverse entries of one kind are strictly ascending in (id, keyID,
// valueOff), so they are distinct, so the map from a reverse entry to the
// (run, id) pair it resolves to is injective. Each such pair is one forward
// entry. The per-key counts then say the two sets are the same size. An
// injection between finite sets of equal size is a bijection — so checking those
// two things establishes that the forward and reverse directions describe exactly
// the same set of entries, without either side being materialised. That is the
// property a delete cascade depends on: an entity's entries are removed by
// reading the reverse direction, and one the reverse direction does not know
// about is an entry that survives its entity.
//
// # What it costs
//
// The forward pass is O(entries). The reverse pass is O(entries × log distinct),
// the log being the search that resolves a reverse entry back to its run. Memory
// is one uint64 per declared key, plus one buffer that grows to the widest value
// of the key being checked — the same buffer the writer keeps, for the same
// reason, and the only thing here that is not flat in the number of entries.
//
// # What it still cannot check
//
// Whether an indexed value matches its entity's current properties. Values are
// caller-encoded and the property blob is opaque to the engine, so only the
// caller knows that — see Store.VerifyIndexes and store.ReindexPolicy. What this
// answers is whether the index is the index it claims to be.

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// Verify implements index.Base. It returns the first inconsistency it finds in
// the image's two index sections, or the context's error if cc is cancelled.
//
// The order is forward first, then reverse. Not arbitrary: the reverse pass
// resolves every entry through the forward direction, so a forward section that
// does not hold together would make the reverse pass report its symptoms rather
// than the cause.
func (b *gpixBase) Verify(cc *store.CancelCheck) error {
	// One counter per declared key. This is the whole of what the check
	// accumulates, and a key costs the same whether it holds one entry or
	// twenty-eight million.
	revCounts := make([]uint64, len(b.fwd.keys))

	for i := range b.fwd.keys {
		if err := verifyGPIXKey(&b.fwd.keys[i], cc); err != nil {
			return err
		}
	}
	for _, kind := range [...]uint8{gpixKindNode, gpixKindEdge} {
		if err := verifyGPIRKind(b.fwd, b.rev, kind, revCounts, cc); err != nil {
			return err
		}
	}
	for i := range b.fwd.keys {
		k := &b.fwd.keys[i]
		if revCounts[i] != k.Entries {
			return fmt.Errorf("gpir: %s key %q has %d reverse entries against %d forward",
				index.EntityKind(k.Kind), k.Name, revCounts[i], k.Entries)
		}
	}
	// The composite postings, when the image carries them. Last because it is the
	// optional section: a base with no GCPX verifies exactly as it did before, and
	// verifyGCPX returns nil for a nil section rather than being branched on here.
	return verifyGCPX(b.cmp, cc)
}

// verifyGPIXKey checks one key's value table and runs against each other.
func verifyGPIXKey(k *gpixKey, cc *store.CancelCheck) error {
	if err := cc.Check(); err != nil {
		return err
	}
	kind := index.EntityKind(k.Kind)

	// The runs region must be tiled exactly: the first run starts at its
	// beginning and the sentinel ends at its end. Slack either side would be
	// bytes the digest covers that no read can reach, which is a writer whose
	// two arithmetics have drifted or a section assembled by something that is
	// not this writer — and in both cases the interesting thing about the file is
	// that nobody can say what the unreachable bytes were for.
	if off := k.runOffsetAt(0); off != 0 {
		return fmt.Errorf("gpix: %s key %q begins its runs at %d rather than 0", kind, k.Name, off)
	}
	if off := k.runOffsetAt(k.Distinct); off != uint64(len(k.runs)) {
		return fmt.Errorf("gpix: %s key %q has %d bytes of runs, its value table ends at %d",
			kind, k.Name, len(k.runs), off)
	}
	// The sentinel's prefix is never compared — a search stops at Distinct — so
	// nothing reads it and nothing would notice it holding something else. It is
	// checked because two compactions of the same content must produce the same
	// bytes, and a field no read looks at is exactly where that quietly stops
	// being true.
	if p := k.prefixAt(k.Distinct); p != ^uint64(0) {
		return fmt.Errorf("gpix: %s key %q has sentinel prefix %#x", kind, k.Name, p)
	}

	// Grows to the key's widest value and is reused across values. The one
	// allocation here that is not flat in the entries, and it is bounded by the
	// same thing that bounds the writer's.
	var prev []byte
	var entries uint64
	for i := uint64(0); i < k.Distinct; i++ {
		if err := cc.Step(); err != nil {
			return err
		}
		value, ids, err := k.runAt(i)
		if err != nil {
			return fmt.Errorf("gpix: %s key %q: %w", kind, k.Name, err)
		}
		// A run holds its value, its ids and nothing else. runAt bounds both
		// against the extent; this is what says they fill it, which is what makes
		// the tiling check above mean every byte is read by something.
		extent := k.runOffsetAt(i+1) - k.runOffsetAt(i)
		if used := 8 + uint64(len(value)) + uint64(len(ids)); used != extent {
			return fmt.Errorf("gpix: %s key %q value %d uses %d of a %d-byte run",
				kind, k.Name, i, used, extent)
		}
		// The prefix is the search's first comparison and it is only ever
		// compared against a prefix computed the same way. One that disagrees
		// with its own value sends the search down the wrong half and reports a
		// present value as absent — a wrong answer out of a file every bound
		// accepts.
		if got, want := k.prefixAt(i), gpixPrefixOf(value); got != want {
			return fmt.Errorf("gpix: %s key %q value %d has prefix %#x for value %q",
				kind, k.Name, i, got, value)
		}
		if i > 0 && bytes.Compare(prev, value) >= 0 {
			return fmt.Errorf("gpix: %s key %q values are not ascending at %d: %q after %q",
				kind, k.Name, i, value, prev)
		}
		prev = append(prev[:0], value...)

		n := gpixIDCount(ids)
		if n == 0 {
			// The writer skips a value no entity holds, and a merged walk that
			// retracted every id of one skips it too, so a present value with an
			// empty run is a value the index cannot produce. It would also make
			// the next compaction of the same content write different bytes.
			return fmt.Errorf("gpix: %s key %q value %q has no ids", kind, k.Name, value)
		}
		// Charged per id as well as per value. Step reports the context's error
		// on every 256th call, so a key with four distinct values and hundreds of
		// thousands of ids under each would otherwise tick four times and never
		// reach the interval — and a low-cardinality key is precisely the shape
		// where most of this pass's work is inside this loop rather than around
		// it. The reverse pass already charges per entry; this makes both halves
		// interruptible at the same granularity.
		for j := 0; j < n; j++ {
			if err := cc.Step(); err != nil {
				return err
			}
			if j > 0 && gpixIDAt(ids, j) <= gpixIDAt(ids, j-1) {
				return fmt.Errorf("gpix: %s key %q value %q has ids out of order at %d: %d after %d",
					kind, k.Name, value, j, gpixIDAt(ids, j), gpixIDAt(ids, j-1))
			}
		}
		entries += uint64(n)
	}
	if entries != k.Entries {
		return fmt.Errorf("gpix: %s key %q claims %d entries and holds %d",
			kind, k.Name, k.Entries, entries)
	}
	return nil
}

// verifyGPIRKind checks one kind's reverse array against the forward section and
// counts its entries per key into revCounts.
func verifyGPIRKind(fwd *gpixSection, rev *gpirSection, kind uint8,
	revCounts []uint64, cc *store.CancelCheck) error {

	if err := cc.Check(); err != nil {
		return err
	}
	arr := rev.entriesAt(kind)
	n := gpirCount(arr)
	var prev gpirEntry
	for i := 0; i < n; i++ {
		if err := cc.Step(); err != nil {
			return err
		}
		e := gpirEntryAt(arr, i)
		// The two padding bytes are written as zero and read by nothing, which
		// makes them the other place where "same content, same bytes" can stop
		// holding without any read noticing.
		if pad := binary.LittleEndian.Uint16(arr[i*gpirEntrySize+10:]); pad != 0 {
			return fmt.Errorf("gpir: %s entry %d has padding %#x", index.EntityKind(kind), i, pad)
		}
		// Strictly ascending, because forEachEntryOf walks the run of equal ids
		// that a binary search lands in and hands each entry to a caller that
		// treats it as one entry. A duplicate would remove one entity's entry
		// twice and a descending pair would hide everything after it.
		if i > 0 && gpirEntryLess(prev, e) >= 0 {
			return fmt.Errorf("gpir: %s entries are not ascending at %d: "+
				"(%d, %d, %d) after (%d, %d, %d)", index.EntityKind(kind), i,
				e.ID, e.KeyID, e.ValueOff, prev.ID, prev.KeyID, prev.ValueOff)
		}
		prev = e

		if uint64(e.KeyID) >= uint64(len(fwd.keys)) {
			return fmt.Errorf("gpir: %s entry %d for id %d names key %d of %d",
				index.EntityKind(kind), i, e.ID, e.KeyID, len(fwd.keys))
		}
		k := &fwd.keys[e.KeyID]
		// A node entry naming an edge key is not a bound violation — the key
		// exists and its runs are where they should be — but it makes the
		// per-key counts below compare two different things, and it would answer
		// "what is this node indexed under" with an edge key.
		if k.Kind != kind {
			return fmt.Errorf("gpir: %s entry %d for id %d names %s key %q",
				index.EntityKind(kind), i, e.ID, index.EntityKind(k.Kind), k.Name)
		}
		// Resolved to a run rather than merely bounded inside the runs region.
		// valueOf checks the weaker thing, because that is all a read needs to be
		// safe; this is what says the entry points at a value rather than into
		// the middle of one.
		j, ok := k.runIndexOfValueAt(e.ValueOff)
		if !ok {
			return fmt.Errorf("gpir: %s entry %d for id %d addresses %d, "+
				"which is not the start of a value of key %q",
				index.EntityKind(kind), i, e.ID, e.ValueOff, k.Name)
		}
		value, ids, err := k.runAt(j)
		if err != nil {
			return fmt.Errorf("gpir: %s entry %d for id %d: %w", index.EntityKind(kind), i, e.ID, err)
		}
		if uint64(e.ValLen) != uint64(len(value)) {
			return fmt.Errorf("gpir: %s entry %d for id %d claims %d value bytes of key %q, "+
				"which holds %d there", index.EntityKind(kind), i, e.ID, e.ValLen, k.Name, len(value))
		}
		// Through IDRun, which is the same search the read path uses: an id the
		// forward run does not hold is the asymmetry this whole pass exists to
		// find.
		if !index.NewIDRun(ids).Contains(e.ID) {
			return fmt.Errorf("gpir: %s entry says id %d is indexed under %q=%q, "+
				"which lists %d ids and not that one",
				index.EntityKind(kind), e.ID, k.Name, value, gpixIDCount(ids))
		}
		revCounts[e.KeyID]++
	}
	return nil
}

// runIndexOfValueAt returns the index of the value whose bytes begin at
// valueOff, and whether there is one.
//
// A reverse entry addresses the value bytes inside a run, which begin four bytes
// past the run's own start — so this searches the value table for that start
// rather than for the offset it was given. The search is over the same monotonic
// offsets a range walk uses, which is why an entry pointing into the middle of a
// run is found rather than accepted.
func (k *gpixKey) runIndexOfValueAt(valueOff uint64) (uint64, bool) {
	if valueOff < 4 {
		return 0, false
	}
	want := valueOff - 4
	lo, hi := uint64(0), k.Distinct
	for lo < hi {
		mid := (lo + hi) / 2
		if k.runOffsetAt(mid) < want {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, lo < k.Distinct && k.runOffsetAt(lo) == want
}
