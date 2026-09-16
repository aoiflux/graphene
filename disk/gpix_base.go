package disk

import (
	"bytes"
	"fmt"

	"github.com/aoiflux/graphene/index"
)

// gpixBase reads an image's GPIX and GPIR sections as an index.Base — the
// disk-resident half of the property index, read in place rather than rebuilt
// into the heap.
//
// It is the object index.Base's doc comment describes from the other side: two
// parsed sections, no state of its own, immutable for as long as the mapping it
// addresses is alive. Its lifetime is therefore the mapping's lifetime, and the
// protocol for that is the one R2 already built for the record stream — a base
// is retired with the image it was parsed out of, never separately. Nothing here
// retains, copies or frees anything.
//
// # What is resident
//
// One gpixKey per declared key plus the key strings, and two slice headers. A
// 28M-entry key costs the same as an empty one. That is the item: the resident
// cost stops being a function of the number of entries.
//
// # What this deliberately does not do
//
// It does not check the section's contents. Every read is bounded — a malformed
// run makes a lookup fail rather than address memory outside the body, and the
// method set says which methods can fail for that reason — but establishing that
// the whole section is *consistent* is an O(entries) pass, and that pass belongs
// to VerifyIndexes, which exists to pay it (R3e). A reader that re-verified per
// probe would make every query pay for the file being trustworthy.
type gpixBase struct {
	fwd *gpixSection
	rev *gpirSection

	// cmp is the composite postings, when the image carries them. Optional in a
	// way the other two are not: nil means the composites are filled from the
	// entries at open, which is what every image written before GCPX existed
	// gets and what index.fillCompositesFromBase still does. See gcpx_base.go.
	cmp *gcpxSection
}

var _ index.Base = (*gpixBase)(nil)

// The two kind namespaces must agree, since kindOf converts between them by cast
// rather than by lookup. These index a one-element array by the difference, so a
// disagreement is a compile error at the line that states the requirement rather
// than a lookup that quietly reads the wrong half of a section. Two constants in
// two packages that merely happen to be equal today are exactly the kind of
// agreement that otherwise breaks silently.
var (
	_ = [1]struct{}{}[gpixKindNode-uint8(index.NodeKind)]
	_ = [1]struct{}{}[gpixKindEdge-uint8(index.EdgeKind)]
)

// newGPIXBase pairs the two sections of one image.
//
// Both are required. A v9 image carries both or neither: the forward direction
// alone cannot answer what an entity is indexed under, which is what removing
// one needs, and the reverse alone names values it cannot resolve. Accepting
// half would mean answering some queries and silently missing others, so a
// half-written image is refused at load rather than at the first delete.
func newGPIXBase(fwd *gpixSection, rev *gpirSection, cmp *gcpxSection) (*gpixBase, error) {
	switch {
	case fwd == nil && rev == nil:
		return nil, fmt.Errorf("gpix: the image carries neither %s nor %s",
			csrSectionMappedIndex, csrSectionMappedReverse)
	case fwd == nil:
		return nil, fmt.Errorf("gpix: the image carries %s without %s",
			csrSectionMappedReverse, csrSectionMappedIndex)
	case rev == nil:
		return nil, fmt.Errorf("gpix: the image carries %s without %s",
			csrSectionMappedIndex, csrSectionMappedReverse)
	}
	return &gpixBase{fwd: fwd, rev: rev, cmp: cmp}, nil
}

// kindOf converts the interface's kind to the on-disk one.
func kindOf(k index.EntityKind) uint8 { return uint8(k) }

// Keys returns the keys the image carries for kind, ascending.
func (b *gpixBase) Keys(kind index.EntityKind) []string {
	return b.fwd.keyNames(kindOf(kind))
}

// KeyStats returns a key's distinct-value and entry counts. Both counts were
// bounded against the key's own runs region at parse time, so both fit an int.
func (b *gpixBase) KeyStats(kind index.EntityKind, key string) (distinct, entries int) {
	k := b.fwd.key(kindOf(kind), key)
	if k == nil {
		return 0, 0
	}
	return int(k.Distinct), int(k.Entries)
}

// Lookup returns the ids indexed under key=value.
func (b *gpixBase) Lookup(kind index.EntityKind, key string, value []byte) (index.IDRun, error) {
	k := b.fwd.key(kindOf(kind), key)
	if k == nil {
		return index.IDRun{}, nil
	}
	ids, err := k.idsOf(value)
	if err != nil {
		return index.IDRun{}, err
	}
	return index.NewIDRun(ids), nil
}

// ForEachValue walks key's distinct values ascending from the first not less
// than from.
//
// The lower bound is one binary search, and it is the same search an exact
// lookup does — which is why a range over a mapped key costs a point lookup plus
// a sequential walk, rather than the whole-key scan an undeclared key costs in
// the resident index. Values ascend by bytes.Compare, the ordering §15.8 fixes
// for the index as a whole.
func (b *gpixBase) ForEachValue(kind index.EntityKind, key string, from []byte,
	fn func(value []byte, ids index.IDRun) bool) error {

	k := b.fwd.key(kindOf(kind), key)
	if k == nil {
		return nil
	}
	start := uint64(0)
	if len(from) > 0 {
		i, _, err := k.search(from)
		if err != nil {
			return err
		}
		start = i
	}
	return k.forEachValue(start, func(value, ids []byte) bool {
		return fn(value, index.NewIDRun(ids))
	})
}

// Cursor returns a forward-only cursor over key's values.
//
// The cursor is where a batch's saving lives, and it is worth being precise
// about where it does not: nothing here is faster than Lookup for one value.
// What it does is carry the position of the last answer from one value to the
// next, which turns N searches over the whole vtab into one sweep across it. A
// caller with a single value should call Lookup, which is that sweep with no
// position to carry.
func (b *gpixBase) Cursor(kind index.EntityKind, key string) index.ValueCursor {
	// A nil key is not a nil cursor. The image simply does not carry this key —
	// it was declared since the last compaction, or every entry under it has been
	// retracted — and the batch still has a delta to merge against.
	return &gpixCursor{k: b.fwd.key(kindOf(kind), key)}
}

// gpixCursor is a position in one key's vtab, and the previous want.
//
// last is kept as a copy rather than as the caller's slice: the values a batch
// walks belong to the caller and may be reused between calls, and a guard that
// compared against memory the caller had since overwritten would refuse a valid
// sequence or accept an invalid one. It is appended into in place, so it grows
// once to the widest value in the batch and allocates nothing after that.
type gpixCursor struct {
	k       *gpixKey
	at      uint64
	last    []byte
	hasLast bool
}

// Seek implements index.ValueCursor.
func (c *gpixCursor) Seek(want []byte) (index.IDRun, bool, error) {
	if c.hasLast && bytes.Compare(want, c.last) < 0 {
		return index.IDRun{}, false, fmt.Errorf(
			"gpix: cursor has passed %s and cannot seek back to %s",
			gpixShortValue(c.last), gpixShortValue(want))
	}
	c.last, c.hasLast = append(c.last[:0], want...), true
	if c.k == nil {
		return index.IDRun{}, false, nil
	}
	i, found, err := c.k.searchFrom(c.at, want)
	if err != nil {
		return index.IDRun{}, false, err
	}
	// The position moves to the answer and not past it, so a repeated want is
	// found again rather than skipped. searchFrom treats a position at Distinct
	// as past the end, which is what a batch whose values run off the top of the
	// key wants.
	c.at = i
	if !found {
		return index.IDRun{}, false, nil
	}
	_, ids, err := c.k.runAt(i)
	if err != nil {
		return index.IDRun{}, false, err
	}
	return index.NewIDRun(ids), true, nil
}

// gpixShortValue renders a value for an error message, truncated.
//
// Index values are caller-supplied and may be large; an error that pasted a
// 64 KiB value into a log would be its own problem.
func gpixShortValue(v []byte) string {
	const max = 16
	if len(v) > max {
		return fmt.Sprintf("%x\u2026(%d bytes)", v[:max], len(v))
	}
	return fmt.Sprintf("%x", v)
}

// ForEachEntryOf walks the entries registered for one entity.
//
// GPIR is sorted by (id, keyID, valueOff) and keyIDs are directory order, which
// is (kind, key) ascending, while valueOffs within a key ascend with the values
// themselves because the runs are written in value order. So the array's own
// order is already key-then-value ascending and no sort happens here. A file
// that broke that ordering would yield these entries out of order — bounded and
// safe, just unordered — which is a thing verification names and a caller that
// needs the order guaranteed re-sorts, as NodeEntriesOf does for the merged set.
func (b *gpixBase) ForEachEntryOf(kind index.EntityKind, id uint64,
	fn func(key string, value []byte) bool) error {

	dk := kindOf(kind)
	var err error
	b.rev.forEachEntryOf(dk, id, func(e gpirEntry) bool {
		if uint64(e.KeyID) >= uint64(len(b.fwd.keys)) {
			err = fmt.Errorf("gpir: entry for %s %d names key %d of %d",
				kind, id, e.KeyID, len(b.fwd.keys))
			return false
		}
		k := &b.fwd.keys[e.KeyID]
		// The kind is checked because the two kinds share one keyID space: a
		// reverse entry in the node array naming an edge key would otherwise
		// report a node as indexed under a key it cannot be indexed under, and
		// the value would resolve cleanly out of the wrong key's runs.
		if k.Kind != dk {
			err = fmt.Errorf("gpir: %s %d names key %q, which is a %s key",
				kind, id, k.Name, index.EntityKind(k.Kind))
			return false
		}
		var value []byte
		if value, err = b.fwd.valueOf(e); err != nil {
			return false
		}
		return fn(k.Name, value)
	})
	return err
}

// HasEntries reports whether id carries any entry.
func (b *gpixBase) HasEntries(kind index.EntityKind, id uint64) bool {
	return b.rev.hasEntriesFor(kindOf(kind), id)
}

// ForEachID walks every distinct indexed id of kind, ascending.
func (b *gpixBase) ForEachID(kind index.EntityKind, fn func(id uint64) bool) {
	b.rev.forEachID(kindOf(kind), fn)
}

// MaxID returns the highest indexed id of kind.
func (b *gpixBase) MaxID(kind index.EntityKind) uint64 {
	return b.rev.maxIDOf(kindOf(kind))
}

// ResidentBytes is the heap this base holds, which is the directory and nothing
// else.
//
// It implements index.ResidentReporter, and the arithmetic is the item's whole
// claim written as code: one gpixKey per declared key plus that key's name, two
// slice headers for the reverse section, and no term anywhere that mentions
// Entries. A 28M-entry key and an empty one differ by nothing here. The bytes the
// entries themselves occupy are the mapping, which is page cache the kernel may
// evict rather than memory this process must keep, and MappedBytes below is what
// reports them.
//
// Until v0.8.0 this comment sent a reader to StorageStats.ImageMappedBytes for
// that figure, and it was wrong in a way worth recording. At open it is nearly
// right: the index sections are inside graphene.csr, so a mapped index is part of
// the one mapping the image made and its bytes really are in that total -- but
// only as part of it, with nothing saying how large the part is, which is the
// question anyone reading the number has. After a compaction it was not right at
// all: SwapBase installs a base parsed from a fresh mapping of the image the
// compaction wrote, held in Store.indexImages rather than Store.images, while
// ImageMappedBytes followed the live graph and went to zero. The largest file the
// store had open was then reported by nothing.
//
// The per-key figure is spelled out rather than taken from unsafe.Sizeof for the
// reason disk/view.go's cell sizes are, and estimate_test.go checks it the same
// way.
func (b *gpixBase) ResidentBytes() int64 {
	// Name header, Kind and KeyID with their padding, Distinct, Entries, and the
	// vtab and runs slice headers.
	const perKey = 16 + 8 + 8 + 8 + 24 + 24
	total := int64(len(b.fwd.keys)) * perKey
	for i := range b.fwd.keys {
		total += int64(len(b.fwd.keys[i].Name))
	}
	// The reverse section is two slice headers over the same mapping.
	total += 48
	if b.cmp != nil {
		// One gcpxComposite per carried composite: the keys slice header, kind
		// with its padding, distinct, entries, and the ttab and runs headers,
		// plus the key strings themselves. Spelled out rather than taken from
		// unsafe.Sizeof, as the per-key figure above is.
		const perComposite = 24 + 8 + 8 + 8 + 24 + 24
		total += int64(len(b.cmp.composites)) * perComposite
		for i := range b.cmp.composites {
			for _, k := range b.cmp.composites[i].keys {
				total += int64(len(k)) + 16
			}
		}
	}
	return total
}

// MappedBytes is how much mapped file this base reads its entries out of.
//
// It implements index.MappedReporter. The sections rather than the file: these
// are the slices parsed out of GPIX and GPIR, so the figure is the index's share
// of whatever mapping it was read from and not the size of that mapping. That is
// the distinction the caller needs -- at open the mapping is the image, shared
// with every record payload in it, and "the image is 1.6 GiB" does not answer
// "what is the index costing me".
func (b *gpixBase) MappedBytes() int64 {
	var n int64
	if b.fwd != nil {
		n += int64(len(b.fwd.body))
	}
	if b.rev != nil {
		n += int64(len(b.rev.nodes)) + int64(len(b.rev.edges))
	}
	return n + b.compositeMappedBytes()
}

// TotalEntries returns the entry count across every key of kind.
//
// Summed rather than stored: the count is per key in the directory, and the
// directory is a handful of entries however many entries they describe.
func (b *gpixBase) TotalEntries(kind index.EntityKind) int {
	dk := kindOf(kind)
	total := 0
	for i := range b.fwd.keys {
		if b.fwd.keys[i].Kind == dk {
			total += int(b.fwd.keys[i].Entries)
		}
	}
	return total
}
