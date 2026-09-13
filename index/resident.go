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
	// id in its posting slice, the id's memberState entry, and that entity's
	// share of the encoded tuple its posting is keyed by. Composites are declared
	// rather than incidental and their tuples are the concatenation of several
	// values, which is why this is larger than a single-key entry rather than
	// smaller.
	residentBytesPerCompositeEntry = 160

	// residentBytesPerDeclaredKey covers what a key costs merely by being
	// declared, across the maps that name it: the postings key map, perKey, the
	// interning table, and an ordered or unique key's own map entry. Multiplied
	// by declared keys, which is a handful.
	residentBytesPerDeclaredKey = 256
)

// ResidentBytes models the heap this index holds, in bytes.
//
// Bounded time: O(declared keys), never O(entries). Read the file header before
// acting on the number -- its counts are exact, its per-item costs are measured
// or assumed, and it deliberately does not include the base's mapped bytes, which
// are page cache rather than heap and are reported as StorageStats.ImageMappedBytes.
//
// Under a base this is the *delta* plus the base's resident overhead, which is
// the whole point of a base: entries that moved into the image stop costing a
// hundred bytes each and start costing nothing each. A store whose index is
// mapped therefore reports a figure that falls sharply at every compaction and
// climbs again with what is written after it, which is exactly the shape an
// operator is being asked to watch.
func (p *PropertyIndex) ResidentBytes() int64 {
	var total int64
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		total += sh.residentBytes()
		sh.mu.RUnlock()
	}
	total += p.nodeComposites.residentBytes() + p.edgeComposites.residentBytes()
	if st := p.baseRef.Load(); st != nil {
		// The retraction sets are exact: one bit per base id, and they report
		// their own size.
		total += int64(st.nodeGone.bytes() + st.edgeGone.bytes())
		total += baseResidentBytes(st.b)
	}
	return total
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
	total += int64(len(sh.uniqueNodeKeys)+len(sh.uniqueEdgeKeys)) * residentBytesPerDeclaredKey
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

// baseResidentBytes asks the base what it costs, or models a floor.
func baseResidentBytes(b Base) int64 {
	if r, ok := b.(ResidentReporter); ok {
		return r.ResidentBytes()
	}
	return int64(len(b.Keys(NodeKind))+len(b.Keys(EdgeKind))) * residentBytesPerDeclaredKey
}
