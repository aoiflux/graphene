package disk

// GPIX and GPIR: the property index as bytes in the image rather than maps in
// the heap.
//
// # Why this section exists
//
// The property index is 94.5% of the anonymous memory a 1.2 GiB store holds and
// 94.1% of the transient it pays to open one — 528 MiB of GIDX entries on disk
// become 2,748 MiB of Go maps, 5.20× their own encoded size, against the 0.229×
// the record image costs once it is mapped. docs/MEMORY_MODEL.md §3 measures it
// and §6 ranks it: nothing else in the engine is within an order of magnitude,
// and no amount of tuning the maps closes a 42% overshoot of a 2 GiB budget.
//
// So the index stops being a structure that is rebuilt into the heap at open and
// becomes a structure that is read in place out of the image mapping. GIDX is
// already a flat, sorted, self-describing byte stream — it is simply in the
// wrong order to be searched, being a single run of (id, key, value) triples in
// (key, value, id) order with no way in but the front. GPIX is the same entries
// with the two directories that make them searchable: per key a table of its
// distinct values, and per value a run of the ids carrying it.
//
// # The layout, and why it is forward-only
//
// imageWriter appends. It has no Seek, because SerialiseTo streams an image into
// a temp file and a writer that could seek would invite a design that holds the
// whole section to patch it — which is the allocation this section exists to
// avoid. Everything below is therefore written in one forward pass per key, and
// every offset a reader needs is either known before the bytes it describes or
// recorded after them.
//
//	GPIX body
//	  header      magic "GPIX" | bodyVersion u16 | flags u16 | nodeKeys u32 | edgeKeys u32
//	  per key     node keys ascending, then edge keys ascending:
//	    vtab      (distinct+1) × { prefix [8]byte | runOff u64 }
//	              runOff is relative to this key's runsOff; entry [distinct] is a
//	              sentinel whose runOff is runsLen, so every run's end is the
//	              next entry's start with no special case for the last one.
//	    runs      per distinct value ascending:
//	              valLen u32 | value | idCount u32 | ids (idCount × u64)
//	  kdir        keyCount × gpixKeyDirSize, sorted by (kind, key)
//	  key bytes   addressed by the kdir
//	  footer      kdirOff u64
//
// The kdir cannot go first: it carries each key's vtabOff, runsOff, distinct and
// entries, and none of those are known until that key's entries have been
// walked. So it goes last and the footer says where — the same trick the v8
// header plays with sectionTableOffset, minus the patch, because the length of
// the section is already in the section directory and the last eight bytes of a
// known-length section need no forward reference at all.
//
// # Why a key is walked twice rather than buffered once
//
// A key's vtab precedes its runs and is sized by the key's distinct count, and
// each vtab entry holds the offset of a run that has not been written yet. Three
// ways to resolve that: buffer the vtab (16 B × distinct — 448 MiB for one
// 28M-entry all-distinct key, which is the problem restated), seek back and
// patch it (the writer cannot), or compute the offsets without the bytes.
//
// The third is free. A run's length is 4 + len(value) + 4 + 8×len(ids), which is
// known from the value and its posting list without writing anything. So pass
// one walks the key summing run lengths and writes the vtab as it goes; pass two
// walks it again and writes the runs. Two walks of one key, no buffer larger
// than one value.
//
// This is why the source below is a per-key walker rather than the whole-index
// iterator the payload uses. ForEachNodeValue already yields a key's distinct
// values ascending with each value's ascending ids, holding the index's own
// memory — so both passes are free of copies as well as of buffers, and the
// repeatability the second pass needs is the repeatability a frozen index has by
// construction.
//
// # The 8-byte prefix, and what it is for
//
// A vtab entry carries the first eight bytes of its value, zero-padded, so that
// a binary search compares two integers instead of dereferencing two runs. It is
// a fast path and not the comparison key: values sharing an eight-byte prefix
// are ordered by reading them, so the search is exact however long the common
// prefix is. Ordering on the prefix alone would have been wrong in one direction
// and slow in the other — "ab" and "ab\x00" pad to the same eight bytes while
// comparing unequal, and a key whose values are URLs shares eight bytes across
// the whole table.
//
// Prefix inequality does imply the values' order, which is what makes the fast
// path sound: if the prefixes first differ at byte j < 8 then either both values
// are longer than j, in which case bytes.Compare is decided at or before j by
// bytes the prefixes agree on, or the shorter one ends at or before j and is
// therefore a proper prefix of the other. Both cases agree with the prefix.
// TestGPIX_PrefixOrderMatchesBytesCompare asserts it over the cases that matter.
//
// # Criticality
//
// CRITICAL, unlike GIDX. GIDX is optional because a reader that skips it
// re-registers every entry from the WAL and arrives at the same index; there is
// no such fallback here, because a v9 image carries no GIDX — a second encoded
// copy of the same entries, ~528 MiB at the measured shape, that no reader of
// either kind would read. A build that skipped GPIX would therefore answer every
// property query with no matches, which is a wrong answer rather than a slow
// one, so it must refuse the file instead.

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	// gpixBodyVersion is the GPIX body version. It is independent of the image
	// version: a later body version can add fields to the kdir without moving
	// the image to v10, which is the reason GHSH carries one too.
	gpixBodyVersion = 1

	gpixHeaderSize = 16 // magic4 + bodyVersion2 + flags2 + nodeKeys4 + edgeKeys4
	gpixFooterSize = 8  // kdirOff

	// gpixVtabEntry is one value-table entry: an 8-byte value prefix and the
	// offset of that value's run, relative to the key's runsOff.
	gpixVtabEntry = 16

	// gpixKeyDirSize is one key-directory entry.
	//
	//	keyOff u64 | keyLen u32 | kind u8 | pad u8 | keyID u16 |
	//	distinct u64 | entries u64 | vtabOff u64 | runsOff u64 | runsLen u64
	gpixKeyDirSize = 56

	// gpixMinRun is the smallest a run can be: two length prefixes, an empty
	// value and no ids. Used to bound a distinct count against the bytes a key's
	// runs region actually holds, the same way every other count read from a
	// file is bounded before it sizes anything.
	gpixMinRun = 8

	gpixKindNode = 0
	gpixKindEdge = 1
)

// gpixPrefixOf returns the first eight bytes of value, zero-padded, as the
// big-endian integer the vtab compares. See the prefix discussion above for why
// this is an ordering fast path rather than the ordering itself.
func gpixPrefixOf(value []byte) uint64 {
	var buf [8]byte
	copy(buf[:], value)
	return binary.BigEndian.Uint64(buf[:])
}

// gpixWithin reports whether an extent of length bytes at off fits inside limit.
//
// Written as a subtraction rather than off+length <= limit because both operands
// come out of a file: 2^64-1 plus a length wraps to a small number, and a guard
// that wraps is a guard that passes before the slice it protects panics. Every
// bound below goes through here for that reason.
func gpixWithin(off, length, limit uint64) bool {
	return off <= limit && length <= limit-off
}

// --- writing ---

// gpixKeyDir is one kdir entry as the encoder accumulates it. The encoder holds
// one of these per declared key and nothing else, which is the whole of its
// resident cost: a few hundred bytes against an index of any size.
type gpixKeyDir struct {
	Key      string
	Kind     uint8
	KeyID    uint16
	Distinct uint64
	Entries  uint64
	VtabOff  uint64
	RunsOff  uint64
	RunsLen  uint64
}

// gpixValueWalker yields every distinct value of one key in ascending order
// together with that value's ascending ids. It must be repeatable: the encoder
// drives it twice per key and requires the same values in the same order both
// times. A frozen index satisfies that by construction, which is why freeze
// happens at the pin and not here.
//
// The ids are the walker's own memory and are neither retained nor mutated.
type gpixValueWalker func(key string, fn func(value []byte, ids []uint64) bool) error

// writeGPIX writes the GPIX body and returns the kdir it wrote, which the caller
// needs in order to build GPIR against the same key ids.
//
// nodeKeys and edgeKeys must each be ascending and free of duplicates: the kdir
// is binary-searched by (kind, key) at read time and the encoder does not sort
// what it is given. The caller has them in that order already — the index yields
// its key list sorted, for the determinism reason ForEachNodeProperty documents.
func writeGPIX(iw *imageWriter, base uint64, nodeKeys, edgeKeys []string,
	nodeWalk, edgeWalk gpixValueWalker) ([]gpixKeyDir, error) {

	iw.str(csrSectionMappedIndex)
	iw.u16(gpixBodyVersion)
	iw.u16(0)
	iw.u32(uint32(len(nodeKeys)))
	iw.u32(uint32(len(edgeKeys)))

	dirs := make([]gpixKeyDir, 0, len(nodeKeys)+len(edgeKeys))
	var err error
	if dirs, err = writeGPIXKind(iw, base, dirs, gpixKindNode, nodeKeys, nodeWalk); err != nil {
		return nil, err
	}
	if dirs, err = writeGPIXKind(iw, base, dirs, gpixKindEdge, edgeKeys, edgeWalk); err != nil {
		return nil, err
	}

	// The kdir, then the key bytes it addresses. Key bytes trail the directory so
	// that the directory is a fixed-stride array a reader can binary-search
	// without a second indirection per probe.
	kdirOff := iw.at() - base
	keyBytesOff := kdirOff + uint64(len(dirs))*gpixKeyDirSize
	var keyAt uint64
	for i := range dirs {
		d := &dirs[i]
		iw.u64(keyBytesOff + keyAt)
		iw.u32(uint32(len(d.Key)))
		iw.u8(d.Kind)
		iw.u8(0)
		iw.u16(d.KeyID)
		iw.u64(d.Distinct)
		iw.u64(d.Entries)
		iw.u64(d.VtabOff)
		iw.u64(d.RunsOff)
		iw.u64(d.RunsLen)
		keyAt += uint64(len(d.Key))
	}
	for i := range dirs {
		iw.str(dirs[i].Key)
	}
	iw.u64(kdirOff)

	if iw.err != nil {
		return nil, iw.err
	}
	return dirs, nil
}

// writeGPIXKind writes one kind's keys, appending a kdir entry for each.
//
// Both kinds go through one function because an id is eight bytes on the wire
// whichever it is; the kind is in the directory entry rather than in the code
// path.
func writeGPIXKind(iw *imageWriter, base uint64, dirs []gpixKeyDir, kind uint8,
	keys []string, walk gpixValueWalker) ([]gpixKeyDir, error) {

	for _, key := range keys {
		d := gpixKeyDir{Key: key, Kind: kind, KeyID: uint16(len(dirs))}

		// Pass one: the value table. Each entry's offset is the running sum of
		// the run lengths before it, so the runs need not exist yet — see the
		// two-walk discussion above.
		d.VtabOff = iw.at() - base
		var runAt, distinct, entries uint64
		// One scratch entry for the whole key rather than one per value. It
		// escapes — imageWriter.bytes can hand a large slice straight to the
		// io.Writer, so anything passed to it does — and an escaping array
		// declared inside the walk is a heap allocation per distinct value, which
		// is precisely the cost-proportional-to-the-index this section exists to
		// remove. TestGPIX_WriteAllocationIsFlatInEntries caught it; iw.num is
		// the same trick for the same reason.
		var ent [gpixVtabEntry]byte
		if err := walk(key, func(value []byte, ids []uint64) bool {
			binary.BigEndian.PutUint64(ent[0:8], gpixPrefixOf(value))
			binary.LittleEndian.PutUint64(ent[8:16], runAt)
			iw.bytes(ent[:])
			runAt += 8 + uint64(len(value)) + 8*uint64(len(ids))
			distinct++
			entries += uint64(len(ids))
			return true
		}); err != nil {
			return nil, fmt.Errorf("gpix: walk %q values: %w", key, err)
		}
		// The sentinel. Its prefix is never compared — a search stops at
		// distinct — and its offset is the end of the last run, which is what
		// makes every run's extent the difference of two adjacent entries.
		binary.BigEndian.PutUint64(ent[0:8], ^uint64(0))
		binary.LittleEndian.PutUint64(ent[8:16], runAt)
		iw.bytes(ent[:])

		// Pass two: the runs themselves.
		d.RunsOff = iw.at() - base
		var wrote uint64
		if err := walk(key, func(value []byte, ids []uint64) bool {
			iw.u32(uint32(len(value)))
			iw.bytes(value)
			iw.u32(uint32(len(ids)))
			for _, id := range ids {
				iw.u64(uint64(id))
			}
			wrote++
			return true
		}); err != nil {
			return nil, fmt.Errorf("gpix: write %q runs: %w", key, err)
		}
		d.RunsLen = iw.at() - base - d.RunsOff
		d.Distinct = distinct
		d.Entries = entries

		// The two passes disagreeing is a bug in the walker, not a corrupt file,
		// and it would otherwise surface as a reader following an offset into the
		// middle of a different run. Catching it here costs one comparison per
		// key and turns a silent wrong answer into a refused compaction.
		if wrote != distinct {
			return nil, fmt.Errorf("gpix: key %q yielded %d values then %d: the source is not repeatable",
				key, distinct, wrote)
		}
		if d.RunsLen != runAt {
			return nil, fmt.Errorf("gpix: key %q runs are %d bytes, value table sized them at %d",
				key, d.RunsLen, runAt)
		}
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// --- reading ---

// gpixSection is a parsed GPIX body. It holds the body and the key directory,
// and nothing per value or per id: the vtabs and runs stay in the mapping and
// are read on demand, which is the entire point of the section.
//
// Resident cost is one gpixKey per declared key plus the key strings — a few
// hundred bytes at the shape docs/MEMORY_MODEL.md measures, against the 2,748
// MiB the same entries cost as maps.
type gpixSection struct {
	body []byte
	keys []gpixKey // sorted by (kind, key), which is kdir order
}

// gpixKey is one key's directory entry, resolved against the body.
type gpixKey struct {
	Name     string
	Kind     uint8
	KeyID    uint16
	Distinct uint64
	Entries  uint64

	vtab []byte // (Distinct+1) × gpixVtabEntry
	runs []byte
}

// parseGPIX validates a GPIX body's directories and returns a reader over it.
//
// What it checks is every bound a later read depends on: the header, the footer,
// the kdir's extent, each entry's key bytes, and each entry's vtab and runs
// regions against the body and against the distinct count. What it deliberately
// does not check is the contents of the runs — that is O(entries) and belongs to
// VerifyIndexes, which exists to pay it. The invariant this function
// establishes is that no read below can address memory outside the body; the
// invariant that a run's own length prefixes are consistent is checked where
// they are read.
func parseGPIX(body []byte) (*gpixSection, error) {
	if len(body) < gpixHeaderSize+gpixFooterSize {
		return nil, fmt.Errorf("gpix: section is %d bytes, shorter than an empty one", len(body))
	}
	if string(body[0:4]) != csrSectionMappedIndex {
		return nil, fmt.Errorf("gpix: bad magic %q", body[0:4])
	}
	if v := binary.LittleEndian.Uint16(body[4:6]); v != gpixBodyVersion {
		return nil, fmt.Errorf("gpix: body version %d, this build reads %d", v, gpixBodyVersion)
	}
	nodeKeys := binary.LittleEndian.Uint32(body[8:12])
	edgeKeys := binary.LittleEndian.Uint32(body[12:16])

	kdirOff := binary.LittleEndian.Uint64(body[len(body)-gpixFooterSize:])
	total := uint64(nodeKeys) + uint64(edgeKeys)
	// A count is bounded by the bytes that remain before it sizes anything: the
	// directory has a fixed stride, so the file itself says how many entries can
	// exist.
	if total > uint64(len(body)/gpixKeyDirSize) {
		return nil, fmt.Errorf("gpix: %d keys exceed what %d bytes can hold", total, len(body))
	}
	if kdirOff < gpixHeaderSize || !gpixWithin(kdirOff, total*gpixKeyDirSize, uint64(len(body)-gpixFooterSize)) {
		return nil, fmt.Errorf("gpix: key directory at %d for %d keys is outside a %d-byte section",
			kdirOff, total, len(body))
	}

	s := &gpixSection{body: body, keys: make([]gpixKey, 0, total)}
	for i := uint64(0); i < total; i++ {
		ent := body[kdirOff+i*gpixKeyDirSize:]
		var (
			keyOff   = binary.LittleEndian.Uint64(ent[0:8])
			keyLen   = binary.LittleEndian.Uint32(ent[8:12])
			kind     = ent[12]
			keyID    = binary.LittleEndian.Uint16(ent[14:16])
			distinct = binary.LittleEndian.Uint64(ent[16:24])
			entries  = binary.LittleEndian.Uint64(ent[24:32])
			vtabOff  = binary.LittleEndian.Uint64(ent[32:40])
			runsOff  = binary.LittleEndian.Uint64(ent[40:48])
			runsLen  = binary.LittleEndian.Uint64(ent[48:56])
		)
		if kind != gpixKindNode && kind != gpixKindEdge {
			return nil, fmt.Errorf("gpix: key %d has kind %d", i, kind)
		}
		if keyID != uint16(i) {
			return nil, fmt.Errorf("gpix: key %d declares id %d", i, keyID)
		}
		limit := uint64(len(body) - gpixFooterSize)
		if !gpixWithin(keyOff, uint64(keyLen), limit) {
			return nil, fmt.Errorf("gpix: key %d name at %d+%d is outside the section", i, keyOff, keyLen)
		}
		// The runs are bounded first, because they are what bounds the distinct
		// count: a run is at least gpixMinRun bytes, so a key claiming more
		// distinct values than its own runs region could hold is refused before
		// that count sizes the vtab slice.
		if !gpixWithin(runsOff, runsLen, limit) {
			return nil, fmt.Errorf("gpix: key %d runs at %d+%d are outside the section", i, runsOff, runsLen)
		}
		if distinct > runsLen/gpixMinRun {
			return nil, fmt.Errorf("gpix: key %d claims %d values in %d bytes of runs", i, distinct, runsLen)
		}
		// The entry count is bounded the same way, by the same region: every id
		// is eight bytes inside the runs. Nothing reads it to size an
		// allocation, but the planner reads it as an int to weigh a key's
		// selectivity, and a file-supplied 2^64-1 arriving there as a negative
		// count would make the planner choose by a number that means nothing.
		if entries > runsLen/8 {
			return nil, fmt.Errorf("gpix: key %d claims %d entries in %d bytes of runs", i, entries, runsLen)
		}
		vtabLen := (distinct + 1) * gpixVtabEntry
		if !gpixWithin(vtabOff, vtabLen, limit) {
			return nil, fmt.Errorf("gpix: key %d value table at %d+%d is outside the section", i, vtabOff, vtabLen)
		}
		s.keys = append(s.keys, gpixKey{
			Name:     string(body[keyOff : keyOff+uint64(keyLen)]),
			Kind:     kind,
			KeyID:    keyID,
			Distinct: distinct,
			Entries:  entries,
			// Three-index slices: an append to either must reallocate rather
			// than write into the mapping, which is the same reason the record
			// loader takes them.
			vtab: body[vtabOff : vtabOff+vtabLen : vtabOff+vtabLen],
			runs: body[runsOff : runsOff+runsLen : runsOff+runsLen],
		})
	}

	// The directory is binary-searched, so its order is load-bearing. Checking
	// it here is one pass over a handful of entries and removes the possibility
	// that a search silently misses a key that is present.
	for i := 1; i < len(s.keys); i++ {
		if gpixKeyLess(s.keys[i], s.keys[i-1]) || (s.keys[i].Kind == s.keys[i-1].Kind &&
			s.keys[i].Name == s.keys[i-1].Name) {
			return nil, fmt.Errorf("gpix: key directory is not sorted at %d (%q after %q)",
				i, s.keys[i].Name, s.keys[i-1].Name)
		}
	}
	return s, nil
}

// gpixKeyLess orders the directory: node keys before edge keys, each ascending.
func gpixKeyLess(a, b gpixKey) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.Name < b.Name
}

// key returns the directory entry for one key, or nil if the image carries none.
//
// A missing key is not an error: it means the compaction that wrote this image
// saw no entry under that key, which is exactly what an empty postings map means
// in the resident index.
func (s *gpixSection) key(kind uint8, name string) *gpixKey {
	lo, hi := 0, len(s.keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		k := &s.keys[mid]
		if k.Kind < kind || (k.Kind == kind && k.Name < name) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(s.keys) && s.keys[lo].Kind == kind && s.keys[lo].Name == name {
		return &s.keys[lo]
	}
	return nil
}

// keyNames returns the keys the image carries for one kind, ascending.
func (s *gpixSection) keyNames(kind uint8) []string {
	var out []string
	for i := range s.keys {
		if s.keys[i].Kind == kind {
			out = append(out, s.keys[i].Name)
		}
	}
	return out
}

// runAt returns the value and id bytes of the i'th distinct value of this key.
//
// The extent comes from two adjacent vtab entries, which is what the sentinel is
// for. The length prefixes inside the run are then checked against that extent:
// parseGPIX established that the run region is inside the body, and this
// establishes that the value and the ids are inside the run.
func (k *gpixKey) runAt(i uint64) (value []byte, ids []byte, err error) {
	if i >= k.Distinct {
		return nil, nil, fmt.Errorf("gpix: value %d of %d", i, k.Distinct)
	}
	start := binary.LittleEndian.Uint64(k.vtab[i*gpixVtabEntry+8:])
	end := binary.LittleEndian.Uint64(k.vtab[(i+1)*gpixVtabEntry+8:])
	if start > end || end > uint64(len(k.runs)) {
		return nil, nil, fmt.Errorf("gpix: value %d spans %d..%d of %d bytes of runs",
			i, start, end, len(k.runs))
	}
	run := k.runs[start:end:end]
	if len(run) < gpixMinRun {
		return nil, nil, fmt.Errorf("gpix: value %d has a %d-byte run", i, len(run))
	}
	valLen := uint64(binary.LittleEndian.Uint32(run[0:4]))
	if 4+valLen+4 > uint64(len(run)) {
		return nil, nil, fmt.Errorf("gpix: value %d declares %d value bytes in a %d-byte run", i, valLen, len(run))
	}
	value = run[4 : 4+valLen : 4+valLen]
	idCount := uint64(binary.LittleEndian.Uint32(run[4+valLen:]))
	idsAt := 4 + valLen + 4
	if idCount > (uint64(len(run))-idsAt)/8 {
		return nil, nil, fmt.Errorf("gpix: value %d declares %d ids in %d remaining bytes",
			i, idCount, uint64(len(run))-idsAt)
	}
	ids = run[idsAt : idsAt+idCount*8 : idsAt+idCount*8]
	return value, ids, nil
}

// prefixAt returns the i'th vtab entry's value prefix.
func (k *gpixKey) prefixAt(i uint64) uint64 {
	return binary.BigEndian.Uint64(k.vtab[i*gpixVtabEntry:])
}

// search returns the index of the first value not less than want, and whether
// that value equals it.
//
// The comparison is the prefix where the prefixes differ and the values
// themselves where they do not — exact either way, for the reason the prefix
// discussion above gives. A malformed run makes the search fail rather than
// answer: a search that treated an unreadable run as "not equal" would report a
// present value as absent, which is the one outcome a corrupt index must not
// produce silently.
func (k *gpixKey) search(want []byte) (uint64, bool, error) {
	wantPrefix := gpixPrefixOf(want)
	var err error
	lo, hi := uint64(0), k.Distinct
	for lo < hi {
		mid := (lo + hi) / 2
		cmp := 0
		if p := k.prefixAt(mid); p != wantPrefix {
			if p < wantPrefix {
				cmp = -1
			} else {
				cmp = 1
			}
		} else {
			var have []byte
			if have, _, err = k.runAt(mid); err != nil {
				return 0, false, err
			}
			cmp = bytes.Compare(have, want)
		}
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= k.Distinct {
		return lo, false, nil
	}
	// One confirmation read: lo is the first value not less than want, which is
	// equal to it or greater, and only reading it says which.
	have, _, err := k.runAt(lo)
	if err != nil {
		return 0, false, err
	}
	return lo, bytes.Equal(have, want), nil
}

// idsOf returns the raw id bytes for an exact value, or nil if it is absent.
func (k *gpixKey) idsOf(value []byte) ([]byte, error) {
	i, ok, err := k.search(value)
	if err != nil || !ok {
		return nil, err
	}
	_, ids, err := k.runAt(i)
	return ids, err
}

// gpixIDAt reads the i'th id out of a run's id bytes.
//
// One 8-byte load per id rather than a cast to []uint64: a run begins at
// whatever offset its value length leaves, so the ids are not guaranteed to be
// eight-byte aligned, and this package will not reach for unsafe to save a load
// that the memory being page cache has already made the cheap half of the
// operation.
func gpixIDAt(ids []byte, i int) uint64 {
	return binary.LittleEndian.Uint64(ids[i*8:])
}

// gpixIDCount reports how many ids a run's id bytes hold.
func gpixIDCount(ids []byte) int { return len(ids) / 8 }

// forEachValue walks this key's distinct values ascending, from the i'th.
//
// The value and id bytes belong to the mapping: read them, do not retain them.
// That is the same contract ForEachNodeEntry states for the resident index's
// strings, which is why the callers of one can be the callers of the other.
func (k *gpixKey) forEachValue(from uint64, fn func(value []byte, ids []byte) bool) error {
	for i := from; i < k.Distinct; i++ {
		value, ids, err := k.runAt(i)
		if err != nil {
			return err
		}
		if !fn(value, ids) {
			return nil
		}
	}
	return nil
}

// --- GPIR: the reverse direction ---
//
// GPIR answers "what is this entity indexed under", which is what removing an
// entity needs and what the resident index keeps ref1/refN for — 84 of the ~107
// bytes an entry used to cost, and the largest single term in
// docs/MEMORY_MODEL.md's ranking.
//
//	GPIR body
//	  header   magic "GPIR" | bodyVersion u16 | flags u16 | nodeEntries u64 | edgeEntries u64
//	  entries  nodeEntries then edgeEntries, each gpirEntrySize bytes:
//	           id u64 | keyID u16 | pad u16 | valLen u32 | valueOff u64
//	           sorted by (id, keyID, valueOff) within each kind
//
// valueOff addresses the value bytes inside the GPIX runs region of the key
// keyID names, so a reverse entry carries no value bytes of its own. The value
// is in the file exactly once however many entities share it, which is the
// dedup the resident index buys with its intern table.

const (
	gpirBodyVersion = 1
	gpirHeaderSize  = 24 // magic4 + bodyVersion2 + flags2 + nodeCount8 + edgeCount8
	gpirEntrySize   = 24 // id8 + keyID2 + pad2 + valLen4 + valueOff8
)

// gpirEntry is one reverse entry: an entity and one key it is indexed under.
type gpirEntry struct {
	ID       uint64
	KeyID    uint16
	ValLen   uint32
	ValueOff uint64
}

// writeGPIRHeader writes the GPIR header. The counts are known before the
// entries because the sort that orders them has already counted them — this
// section is built from a file the compaction wrote, not from a stream.
func writeGPIRHeader(iw *imageWriter, nodeEntries, edgeEntries uint64) {
	iw.str(csrSectionMappedReverse)
	iw.u16(gpirBodyVersion)
	iw.u16(0)
	iw.u64(nodeEntries)
	iw.u64(edgeEntries)
}

// writeGPIREntry writes one reverse entry.
func writeGPIREntry(iw *imageWriter, e gpirEntry) {
	iw.u64(e.ID)
	iw.u16(e.KeyID)
	iw.u16(0)
	iw.u32(e.ValLen)
	iw.u64(e.ValueOff)
}

// gpirSection is a parsed GPIR body: two fixed-stride arrays and nothing else.
type gpirSection struct {
	nodes []byte
	edges []byte
}

// parseGPIR validates a GPIR body's extents.
func parseGPIR(body []byte) (*gpirSection, error) {
	if len(body) < gpirHeaderSize {
		return nil, fmt.Errorf("gpir: section is %d bytes, shorter than a header", len(body))
	}
	if string(body[0:4]) != csrSectionMappedReverse {
		return nil, fmt.Errorf("gpir: bad magic %q", body[0:4])
	}
	if v := binary.LittleEndian.Uint16(body[4:6]); v != gpirBodyVersion {
		return nil, fmt.Errorf("gpir: body version %d, this build reads %d", v, gpirBodyVersion)
	}
	nodeCount := binary.LittleEndian.Uint64(body[8:16])
	edgeCount := binary.LittleEndian.Uint64(body[16:24])
	rest := uint64(len(body) - gpirHeaderSize)
	if nodeCount > rest/gpirEntrySize || edgeCount > rest/gpirEntrySize ||
		(nodeCount+edgeCount)*gpirEntrySize > rest {
		return nil, fmt.Errorf("gpir: %d node and %d edge entries exceed what %d bytes hold",
			nodeCount, edgeCount, rest)
	}
	nodeEnd := gpirHeaderSize + nodeCount*gpirEntrySize
	edgeEnd := nodeEnd + edgeCount*gpirEntrySize
	return &gpirSection{
		nodes: body[gpirHeaderSize:nodeEnd:nodeEnd],
		edges: body[nodeEnd:edgeEnd:edgeEnd],
	}, nil
}

// entriesAt returns the array for one kind.
func (s *gpirSection) entriesAt(kind uint8) []byte {
	if kind == gpixKindEdge {
		return s.edges
	}
	return s.nodes
}

// gpirEntryAt decodes the i'th entry of an array.
func gpirEntryAt(arr []byte, i int) gpirEntry {
	ent := arr[i*gpirEntrySize:]
	return gpirEntry{
		ID:       binary.LittleEndian.Uint64(ent[0:8]),
		KeyID:    binary.LittleEndian.Uint16(ent[8:10]),
		ValLen:   binary.LittleEndian.Uint32(ent[12:16]),
		ValueOff: binary.LittleEndian.Uint64(ent[16:24]),
	}
}

// gpirCount reports how many entries an array holds.
func gpirCount(arr []byte) int { return len(arr) / gpirEntrySize }

// forEachEntryOf visits every reverse entry for one id, in (keyID, valueOff)
// order.
//
// The array is sorted by id, so this is a binary search to the first entry and a
// walk while the id matches — the contiguous-run half of the plan's "binary
// search plus run", and the reason the top levels of the search stay in cache
// across a delete cascade that walks ids ascending.
func (s *gpirSection) forEachEntryOf(kind uint8, id uint64, fn func(gpirEntry) bool) {
	arr := s.entriesAt(kind)
	n := gpirCount(arr)
	for i := gpirLowerBound(arr, id); i < n; i++ {
		e := gpirEntryAt(arr, i)
		if e.ID != id {
			return
		}
		if !fn(e) {
			return
		}
	}
}

// gpirLowerBound returns the index of the first entry whose id is not less than
// id, or the entry count if there is none.
//
// It is separate from forEachEntryOf because the two callers want different
// things from the same search: one walks the run that follows, and one only asks
// whether the run is empty — and the second is on the delete path, where a
// closure it would otherwise have to allocate is a cost paid per entity removed.
func gpirLowerBound(arr []byte, id uint64) int {
	lo, hi := 0, gpirCount(arr)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if gpirEntryAt(arr, mid).ID < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// hasEntriesFor reports whether id appears in one kind's array at all.
func (s *gpirSection) hasEntriesFor(kind uint8, id uint64) bool {
	arr := s.entriesAt(kind)
	i := gpirLowerBound(arr, id)
	return i < gpirCount(arr) && gpirEntryAt(arr, i).ID == id
}

// maxIDOf returns the highest id in one kind's array, or zero if it is empty.
// The array is sorted by id, so this is the last entry.
func (s *gpirSection) maxIDOf(kind uint8) uint64 {
	arr := s.entriesAt(kind)
	n := gpirCount(arr)
	if n == 0 {
		return 0
	}
	return gpirEntryAt(arr, n-1).ID
}

// forEachID visits every distinct id in one kind's array, ascending.
//
// This is what a bounded VerifyIndexes walks instead of materialising every
// indexed id into a map and a slice, which is what B5 removed from the resident
// index and what this section must not reintroduce.
func (s *gpirSection) forEachID(kind uint8, fn func(uint64) bool) {
	arr := s.entriesAt(kind)
	n := gpirCount(arr)
	var last uint64
	var have bool
	for i := 0; i < n; i++ {
		id := gpirEntryAt(arr, i).ID
		if have && id == last {
			continue
		}
		if !fn(id) {
			return
		}
		last, have = id, true
	}
}

// valueOf resolves a reverse entry to its value bytes inside the GPIX runs.
//
// The bound is checked here rather than at parse time because there are as many
// reverse entries as index entries and checking them all is the O(n) pass that
// belongs to verification. Every read of one is therefore checked, and an entry
// pointing outside its key's runs is an error rather than a slice into whatever
// follows.
func (s *gpixSection) valueOf(e gpirEntry) ([]byte, error) {
	if uint64(e.KeyID) >= uint64(len(s.keys)) {
		return nil, fmt.Errorf("gpir: entry for id %d names key %d of %d", e.ID, e.KeyID, len(s.keys))
	}
	k := &s.keys[e.KeyID]
	if !gpixWithin(e.ValueOff, uint64(e.ValLen), uint64(len(k.runs))) {
		return nil, fmt.Errorf("gpir: entry for id %d addresses %d+%d in %d bytes of %q runs",
			e.ID, e.ValueOff, e.ValLen, len(k.runs), k.Name)
	}
	end := e.ValueOff + uint64(e.ValLen)
	return k.runs[e.ValueOff:end:end], nil
}
