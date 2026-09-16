package index

// What the index holds, in bytes, without walking it.
//
// # Why this is a model and not a measurement
//
// The figure exists to be read on a ticker. AutoCompact evaluates a policy
// against StorageStats on every tick and a memory budget is checked before every
// compaction, so the one thing this must not be is a pass over the index -- the
// structure whose size is the question is also the structure a 28M-entry store
// cannot afford to walk. Everything here is therefore arithmetic over counters
// the write paths already maintain, and the cost of the whole call is O(declared
// keys), not O(entries).
//
// That buys bounded time and pays for it in accuracy, and the trade is worth
// stating precisely rather than hedging about. Three classes of figure appear
// below:
//
//   - Exact. Counts the index maintains: postings.count, orderedIndex.totalIDs,
//     compositeIndex.entries, the retraction bitsets' own size, the number of
//     declared keys. Nothing is guessed about how many of anything there is.
//   - Measured. Bytes per entry, from this repository's own footprint work. The
//     tree reports the postings figure three times from three dates -- 107 B in
//     docs/benchmarks.md, 103.9 B in TECHNICAL_DETAILS §14.5, and 93.4 B once the
//     arity split and key interning are counted -- and value interning then moves
//     it by between -34% and 0% depending on a value cardinality no counter
//     carries. The largest is used, for the reason preflight.go uses the largest:
//     this term dominates an indexed store and the direction to be wrong in is
//     up. estPropertyEntryBytes there and residentBytesPerEntry here are the same
//     number arrived at the same way, deliberately not shared across the package
//     boundary, because one of them describes a file that has not been opened and
//     the other a structure that is in the heap now, and a later measurement may
//     well move one without the other.
//   - Assumed. One figure, the average length of an indexed value, which no
//     counter in this package carries and which cannot be totalled in bounded
//     time. See residentValueBytes.
//
// So: an estimate whose counts are exact and whose per-item costs are measured
// or named. TestEstimatedResident_WithinBand is what holds it honest, and the
// band is wide on purpose.

// Per-item costs. See the header for which of these are measured and which are
// assumed.
const (
	// residentBytesPerEntry is one (id, key, value) triple in the resident
	// postings: its place in a sorted id slice, its reverse-map entry, its share
	// of the value-interning table and of the bucket structure around all three.
	residentBytesPerEntry = 107

	// residentValueBytes is the assumed length of one indexed value, used only
	// where a structure's cost scales with distinct values rather than with
	// entries. Thirty-two bytes is a digest, which is the largest of the eight
	// keys the program is sized against and the one whose values are all
	// distinct -- so this is the worst case of the shape that makes the term
	// matter, rather than an average over shapes that do not.
	//
	// It is the one figure here that no counter could replace: totalling the
	// values would mean touching every distinct value, which is the walk this
	// file exists to avoid.
	residentValueBytes = 32

	// residentBytesPerOrderedValue is one orderedValue cell: a string header and
	// a slice header. Its ids are counted separately, by residentBytesPerID,
	// because totalIDs counts them exactly.
	residentBytesPerOrderedValue = 40

	// residentBytesPerID is one entity id inside a sorted posting slice.
	residentBytesPerID = 8

	// residentBytesPerCompositeEntry is one (id, tuple) pair in a composite: the
	// id in its posting slice, the entity's row in the member store, and that
	// entity's share of the encoded tuple its posting is keyed by.
	//
	// It was 160 while an entity's member values were a *memberState — a struct,
	// a backing array of width string headers, and one copy of every value's
	// bytes, behind a pointer in a map. Measured over 200,000 entities of a
	// width-2 composite, that form held 146.60 B per entry at the low tuple
	// cardinality a composite is declared for and 262.6 B at one distinct tuple
	// per entity; the columnar rows that replaced it hold 43.87 and 257.4. This
	// figure is the old one scaled by the first of those ratios, 160 ×
	// 43.87/146.60, because 160 was itself chosen against that shape rather than
	// against the worst case — a composite whose tuples are nearly all distinct
	// returns one entity per lookup and is not a composite anyone would declare.
	//
	// So it is the one per-item cost here that is not the up-direction choice the
	// header describes, and it was not before either. What keeps it honest is the
	// same thing as the rest: TestEstimatedResident_WithinBand.
	residentBytesPerCompositeEntry = 48

	// residentBytesPerDeclaredKey covers what a key costs merely by carrying an
	// entry, across the three shard maps that name it: the postings key map, its
	// value bucket map's header, and perKey. Multiplied by keys with entries,
	// which is a handful.
	//
	// It deliberately does *not* include an ordered or unique declaration's own
	// map entry. Those are charged by residentBytesPerNamedKey below, because a
	// key can be declared without being either and charging this figure twice for
	// one that is both says the shard maps exist twice.
	residentBytesPerDeclaredKey = 256

	// residentBytesPerNamedKey is one entry in a map that only names a key: a
	// string header and its control byte. The unique sets are exactly that, and an
	// ordered key's map entry is that plus the pointer counted by
	// orderedIndex.residentBytes.
	residentBytesPerNamedKey = 24
)

// ResidentBytes models the heap this index holds, in bytes.
//
// Bounded time: O(declared keys), never O(entries). Read the file header before
// acting on the number -- its counts are exact, its per-item costs are measured
// or assumed, and it deliberately does not include the base's mapped bytes, which
// are page cache rather than heap. Those are reported by the storage layer, which
// is the only place that knows how large the mapping under a base is: see
// StorageStats.IndexMappedBytes and disk.ResidentEstimate.MappedIndex.
//
// It includes the composite declarations. CompositeResidentBytes is their share
// and ResidentBytesSplit is both at once, which disk.ResidentEstimate uses so it
// can report them as two disjoint terms: the composites are the one part of this
// figure a caller changes by declaring less rather than by writing less.
//
// Under a base this is the *delta* plus the base's resident overhead, which is
// the whole point of a base: entries that moved into the image stop costing a
// hundred bytes each and start costing nothing each. A store whose index is
// mapped therefore reports a figure that falls sharply at every compaction and
// climbs again with what is written after it, which is exactly the shape an
// operator is being asked to watch.
func (p *PropertyIndex) ResidentBytes() int64 {
	total, _ := p.ResidentBytesSplit()
	return total
}

// ResidentBytesSplit is ResidentBytes with the composites' share named, and it
// is the call to make when both figures are wanted.
//
// One call rather than two because a caller reporting "index" as total minus
// composite must not subtract a figure taken at a different instant. Nothing
// here holds a lock across the whole walk -- each shard and each declaration is
// read under its own -- so two separate calls straddling a write can be
// inconsistent with each other by a shard's worth of entries, and a difference of
// two figures can be negative in a way neither figure is. Taking the composite
// total once and returning it with the sum it is part of makes that impossible
// by construction rather than by clamping afterwards.
func (p *PropertyIndex) ResidentBytesSplit() (total, composite int64) {
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		total += sh.residentBytes()
		sh.mu.RUnlock()
	}
	composite = p.CompositeResidentBytes()
	total += composite
	if st := p.baseRef.Load(); st != nil {
		// The retraction sets are exact: one bit per base id, and they report
		// their own size.
		total += int64(st.nodeGone.bytes() + st.edgeGone.bytes())
		total += baseResidentBytes(st.b)
	}
	return total, composite
}

// CompositeResidentBytes is the share of ResidentBytes that the declared
// composites account for, across both kinds.
//
// Separate because it is separately decidable. Every other term here moves with
// what the store was asked to hold; this one moves with how many composites were
// declared, and a caller looking at a figure they want smaller can act on that
// today without rewriting anything. At the shape this engine is sized for it was
// the largest single term in a default configuration, so which half of the index
// a store is paying for is not a detail.
//
// # What a base that carries the composites does to this figure
//
// It empties it. A composite the attached base answers in place holds no postings
// and no rows -- AttachBase skips its fill and SwapBase resets it -- so what it
// contributes here is its declaration's key names and nothing else. The floor is
// therefore not zero, and a caller comparing two stores should compare against a
// bare declaration rather than against zero. See composite_base.go.
//
// What is still counted, and is the point of continuing to report it: a composite
// the base does *not* carry is filled into the heap exactly as before, and a
// composite the base does carry still accumulates whatever has been registered
// since the base was installed. Both are real memory and neither is visible from
// the declaration alone.
//
// O(declared composites), like everything else in this file, and exact in its
// counts: compositeIndex.entries is maintained by the registration path.
func (p *PropertyIndex) CompositeResidentBytes() int64 {
	return p.nodeComposites.residentBytes() + p.edgeComposites.residentBytes()
}

// residentBytes is one shard's share. Caller holds sh.mu.
func (sh *propertyShard) residentBytes() int64 {
	total := int64(sh.nodes.count+sh.edges.count) * residentBytesPerEntry
	total += int64(len(sh.nodes.byKey)+len(sh.edges.byKey)) * residentBytesPerDeclaredKey
	for _, idx := range sh.orderedNodeKeys {
		total += idx.residentBytes()
	}
	for _, idx := range sh.orderedEdgeKeys {
		total += idx.residentBytes()
	}
	total += int64(len(sh.uniqueNodeKeys)+len(sh.uniqueEdgeKeys)) * residentBytesPerNamedKey
	total += int64(len(sh.orderedNodeKeys)+len(sh.orderedEdgeKeys)) * residentBytesPerNamedKey
	return total
}

// residentBytes is one ordered index's share.
//
// An ordered index is a second copy of its key's entries in a different shape,
// so it is counted separately rather than folded into the per-entry figure
// above: a store that declares no ordered key must not be charged for one.
func (o *orderedIndex[T]) residentBytes() int64 {
	return int64(len(o.values))*(residentBytesPerOrderedValue+residentValueBytes) +
		int64(o.totalIDs)*residentBytesPerID
}

// residentBytes totals one kind's composite declarations.
func (c *compositeSet[T]) residentBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total int64
	for _, idx := range c.byName {
		idx.mu.RLock()
		total += int64(idx.entries) * residentBytesPerCompositeEntry
		total += int64(len(idx.keys)) * residentBytesPerDeclaredKey
		idx.mu.RUnlock()
	}
	return total
}

// ResidentReporter is a Base that can say what it costs in heap.
//
// Optional rather than part of Base, and not for the usual reason. Base is
// implemented once in this repository and the addition would be easy; what it
// would cost is the claim Base makes about itself. Every method there is a way to
// read the index, and a size accessor is a way to read the *implementation* --
// so putting it on the interface would say that anything calling itself a base
// must account for its own memory, which is a requirement about how a base is
// built rather than about what it answers. An implementation that maps a file and
// one that holds a decoded copy both satisfy Base honestly and have entirely
// different things to say here.
//
// A base that does not implement this is charged residentBytesPerDeclaredKey per
// key it carries, which is the floor any implementation pays for naming them.
type ResidentReporter interface {
	// ResidentBytes is the heap the base holds, excluding any file it maps --
	// mapped bytes are page cache and are reported separately.
	ResidentBytes() int64
}

// MappedReporter is a Base that reads its entries out of a mapped file and can
// say how many bytes of one.
//
// Optional, and for the same reason ResidentReporter is: a base that holds a
// decoded copy has nothing to answer here, and requiring an answer would make
// Base a claim about how a base is built rather than about what it answers.
//
// The figure is the base a caller asked for, not the file it happens to live in.
// A store maps one file and reads both its graph and its index out of it, so the
// index is a *part* of what is mapped rather than a mapping of its own -- which
// is the whole reason the number is worth having separately. Without it, an
// operator can see that a store has 1.6 GiB of file mapped and cannot see that
// most of it is the property index they are choosing to keep on disk.
type MappedReporter interface {
	// MappedBytes is how much mapped file this base reads its entries out of.
	MappedBytes() int64
}

// MappedBytes is how much mapped file the property index is read out of, and
// zero when there is no base or the base holds no mapping.
//
// Not part of ResidentBytes and never added to it. These are page cache the
// kernel may evict, and the point of a mapped index is that they are *not*
// memory this process must keep; see disk.ResidentEstimate.MappedIndex, which
// reports it beside the total rather than inside it.
func (p *PropertyIndex) MappedBytes() int64 {
	st := p.baseRef.Load()
	if st == nil || st.b == nil {
		return 0
	}
	m, ok := st.b.(MappedReporter)
	if !ok {
		return 0
	}
	return m.MappedBytes()
}

// baseResidentBytes asks the base what it costs, or models a floor.
func baseResidentBytes(b Base) int64 {
	if r, ok := b.(ResidentReporter); ok {
		return r.ResidentBytes()
	}
	return int64(len(b.Keys(NodeKind))+len(b.Keys(EdgeKind))) * residentBytesPerDeclaredKey
}
