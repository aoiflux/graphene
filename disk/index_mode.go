package disk

// Choosing between the two places a property index can live.
//
// # What the choice is
//
// A v9 image carries the index as GPIX and GPIR: a forward direction addressed
// by key and value, and a reverse one addressed by entity. Both are written to be
// read where they lie, so a store can hold a fence per key and a directory and
// nothing else — the residency this section of the program exists to remove. The
// alternative, and what every version before v9 had no choice about, is to walk
// the file's entries at Open and build the same index in the heap.
//
// So IndexMode is not a hint and not a tuning knob. It decides whether the index
// is ~107 bytes per entry of anonymous memory or a handful of kilobytes per key,
// and it decides whether a property lookup is a map probe or a binary search
// through pages that may not be resident. The second half is why the mode exists
// rather than being a fact about the format: a caller who would rather pay the
// memory than the page faults says so here.
//
// # Why it is not just Options.ImageMode
//
// Reading an index in place means holding slices of the image for the life of the
// store, which is exactly what a mapping is for and exactly what the heap path is
// not: under ImageHeap the parse copies what it needs and the file buffer becomes
// garbage, so a base pointing into it would pin the whole image in anonymous
// memory to save a fraction of it. A store asking for a mapped index over a heap
// image therefore gets the resident index and a metric saying so, on the same
// terms — and for the same reason — that a store asking for a mapped image on a
// platform without mmap gets the heap and a metric.
//
// # What a v8 image does under either mode
//
// It carries GIDX, so it is read into the heap and the mode changes nothing. That
// is not a fallback and is not reported as one: there is no index in that file to
// read in place. The next compaction is what produces one.

import (
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// IndexMode decides whether an image's property index is read in place or
// rebuilt in the heap at Open.
type IndexMode uint8

const (
	// IndexMapped reads a v9 image's GPIX and GPIR sections in place, the
	// in-memory index becoming the delta over them, and writes v9 at the next
	// compaction.
	//
	// This is the zero value and therefore the default, which is the decision
	// this whole section of the program was built to be able to make: a 1.2 GiB
	// store's index was 94.5% of its anonymous memory and 42% over a 2 GiB budget
	// on its own. Measured at the flip: see CHANGELOG and docs/MEMORY_MODEL.md.
	//
	// What it costs is stated rather than hidden. A warm point lookup is a binary
	// search through pages instead of a map probe, and a cold one is a handful of
	// random reads; NodesByPropertyBatch is the answer for a pass that does many.
	// And the image it writes is v9, which no build before this one opens —
	// IndexResident is how an operator asks for v8 back.
	//
	// Requires a mapped image, and falls back to IndexResident with
	// store.MetricIndexFallback and StorageStats.IndexMode reporting it when the
	// store has one in the heap instead — see this file's header for why. A v8
	// image is read into the heap under this mode too, with no metric, because it
	// carries nothing to read in place; the next compaction gives it one.
	IndexMapped IndexMode = iota

	// IndexResident rebuilds the index in the heap, one entry at a time, and keeps
	// writing v8 with GIDX. It is what every version before v9 did and the only
	// thing a v8 image allows.
	//
	// Ask for it to trade the memory back for the latency, or to keep writing a
	// format older builds can read.
	IndexResident
)

// String names the mode for StorageStats and for an error message.
func (m IndexMode) String() string {
	switch m {
	case IndexResident:
		return "resident"
	case IndexMapped:
		return "mapped"
	}
	return fmt.Sprintf("IndexMode(%d)", uint8(m))
}

// CSRVersionsWritable lists the container versions this build can produce, in
// ascending order.
//
// Two, for the first time in the format's history: v8 and v9 differ in how the
// property index is encoded and in nothing else, and which one a compaction
// writes is IndexMode's decision. Everything older is readable and not writable
// — those versions differ in the record layout, and reproducing one would mean
// carrying a second writer for a format nothing gains from.
//
// Exported for `graphene store migrate -to`, which has to tell an operator which
// numbers it accepts. A fresh slice per call, so a caller cannot edit the answer.
func CSRVersionsWritable() []uint16 {
	return []uint16{csrVersionSectioned, csrVersionMappedIndex}
}

// IndexModeForCSRVersion is the mode a store must be opened under for its next
// compaction to write version v, and whether this build writes v at all.
//
// The inverse of csrPayload.imageVersion, and the reason to have it is that the
// rule has one owner. A downgrade is not a separate code path: it is an ordinary
// compaction by a store that was opened under IndexResident, which is what makes
// it as crash-safe as any other. A caller wanting the number back out of a mode
// asks imageVersion, or CSRVersionCurrent for the default.
//
// v is a uint16 because that is what CSRInfo.Version is, so an operator's number
// and a file's are compared without a conversion in between that could narrow.
func IndexModeForCSRVersion(v uint16) (IndexMode, bool) {
	switch v {
	case csrVersionMappedIndex:
		return IndexMapped, true
	case csrVersionSectioned:
		return IndexResident, true
	}
	return 0, false
}

// indexBaseAllowed reports why this store will not read the image's index in
// place, or nil.
//
// mapped is whether the image itself is mapped, which the caller knows and this
// cannot: an image source is consumed by the parse.
func (s *Store) indexBaseAllowed(mapped bool) error {
	return indexBaseAllowedFor(s.indexMode, mapped)
}

// indexBaseAllowedFor is the rule itself, as a function of the mode and whether
// the image is mapped — separated from the method for the reason
// imageMappingAllowedFor is, and consumed by the same caller.
func indexBaseAllowedFor(mode IndexMode, mapped bool) error {
	if mode != IndexMapped {
		return errors.New("Options.IndexMode is IndexResident")
	}
	if !mapped {
		return errors.New("the image is in the heap, and an index read out of it " +
			"would pin the whole file to save part of it")
	}
	return nil
}

// recordIndexFallback reports an index that was rebuilt in the heap when the mode
// asked for one read in place.
//
// Reported for the same reason MetricImageFallback is: the fallback is correct,
// silent, and the difference between the two is most of what this program is
// about. A store paying for a resident index it configured away has no symptom
// other than the memory.
func (s *Store) recordIndexFallback(reason error) {
	s.record(store.Metric{Kind: store.MetricIndexFallback, Err: reason})
}

// indexHolding names where the property index actually is, for StorageStats.
//
// Read off the index rather than off the option, because the option is a request
// and this is the answer. A store that asked for a mapped index and opened a v8
// file reports "resident", which is the truth about what it is holding.
func (s *Store) indexHolding() string {
	if s.index().AttachedBase() != nil {
		return IndexMapped.String()
	}
	return IndexResident.String()
}
