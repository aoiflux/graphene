package disk

// Fuzz targets and hostile seeds for the v8 container's section layer.
//
// FuzzDeserialiseCSR explores the records; these explore everything around
// them. The container reads a section-table offset from the header, a count and
// then a directory of (magic, flags, offset, length) from wherever that points,
// and hands each section's byte range to a parser of its own. Every one of
// those numbers comes out of the file, and each is an opportunity to slice
// outside it.
//
// Two of those parsers had no target at all — GORD and GCMP, the ordered-key
// and composite declarations. They are small and they only carry key names,
// which is exactly why they were skipped and exactly why they are worth
// covering: a section nobody thinks is dangerous is a section whose bounds
// nobody re-reads.
//
// Run:
//
//	go test ./disk/ -run=XXX -fuzz=FuzzReadOrderedKeySection -fuzztime=60s

import (
	"encoding/binary"
	"math"
	"testing"
)

// --- Hostile seeds for FuzzDeserialiseCSR ---------------------------------

// hostileCSRSeeds returns images that are structurally plausible and wrong in
// one specific way each.
//
// Random mutation almost never produces these. A fuzzer starting from a valid
// image will flip bytes inside the records for a very long time before it
// happens to write a section-table offset equal to the file length, because
// that field is eight bytes and only a handful of its 2^64 values are
// interesting. Handing it the interesting ones is the whole job of a seed
// corpus.
//
// The digest is deliberately left stale in every one of these. deserialiseCSR
// reads the digest into the trailer and does not check it — verification is a
// separate, opt-in pass over the file — so a reader that fell back on "the
// digest would have caught it" would be relying on a check that this path never
// runs.
func hostileCSRSeeds(t testingTB) [][]byte {
	t.Helper()
	base := csrSeed(t)
	if len(base) < csrV8HeaderSize {
		t.Fatalf("seed image is %d bytes, shorter than a v8 header", len(base))
	}

	var seeds [][]byte
	add := func(b []byte) { seeds = append(seeds, b) }

	clone := func() []byte { return append([]byte(nil), base...) }

	// Where the section table starts, according to the file itself.
	tableOff := binary.LittleEndian.Uint64(base[62:70])

	// --- Truncation at each structural boundary.
	//
	// A file cut short is the single most likely corruption in the field: it is
	// what a crash mid-write, a full disk, and a half-finished copy all produce.
	// Each of these cuts at a different boundary, because the parser reaches a
	// different bounds check in each case.
	add(base[:csrV8HeaderSize])                    // header, and nothing after it
	add(base[:min(csrV8HeaderSize+13, len(base))]) // part way into the first record
	if tableOff > 0 && tableOff <= uint64(len(base)) {
		add(base[:tableOff])                         // records, no directory
		add(base[:min(int(tableOff)+2, len(base))])  // directory count, no entries
		add(base[:min(int(tableOff)+14, len(base))]) // part way into an entry
		add(base[:min(int(tableOff)+2+12, len(base))])
	}
	add(base[:len(base)-1]) // one byte short of whole

	// --- Offsets pointing at, past, and around the end of the file.
	//
	// len is the classic off-by-one: it is a valid index for an empty slice and
	// an invalid one for anything that reads a byte. len-1 leaves room for one
	// byte and not for the eight the parser wants. The MaxInt64 values are the
	// conversion hazard — a uint64 that becomes a negative int indexes backwards.
	for _, off := range []uint64{
		uint64(len(base)),
		uint64(len(base)) - 1,
		math.MaxInt64,
		math.MaxInt64 + 1,
		math.MaxUint64,
		0,
	} {
		// indexOffset, the v6 field. Written as zero in v8, so a non-zero value
		// here is either a downgraded file or a forged one.
		b := clone()
		binary.LittleEndian.PutUint64(b[38:46], off)
		add(b)

		// sectionTableOffset, the v8 field that decides where the directory is.
		b = clone()
		binary.LittleEndian.PutUint64(b[62:70], off)
		add(b)
	}

	// --- A directory entry whose Offset+Length wraps.
	//
	// Each bound is individually satisfiable: an offset inside the file and a
	// length inside the file. Their sum is what names the end of the section,
	// and a sum computed in uint64 without an overflow check wraps to a small
	// number that passes a "within the file" test on the way to slicing from an
	// offset near 2^64.
	if tableOff+2+csrSectionEntry <= uint64(len(base)) {
		entry := int(tableOff) + 2
		for _, pair := range []struct{ off, length uint64 }{
			{math.MaxUint64 - 8, 64},
			{uint64(len(base)) - 4, math.MaxUint64},
			{8, math.MaxUint64 - 4},
		} {
			b := clone()
			binary.LittleEndian.PutUint64(b[entry+8:entry+16], pair.off)
			binary.LittleEndian.PutUint64(b[entry+16:entry+24], pair.length)
			add(b)
		}
	}

	// --- A GIDX entry claiming more value bytes than the section holds.
	//
	// The property index is one flat stream of length-prefixed keys and values,
	// and valLen is a uint32 straight out of the file. remaining+1 is the
	// smallest wrong answer, which is the one a bound written as >= rather than
	// > lets through.
	if gidx, ok := findSectionOffset(base, csrSectionPropIndex); ok {
		// Section body: magic4, nodePropCount8, then id8 keyLen2 key valLen4 val.
		p := gidx + 4 + 8
		if p+10 <= len(base) {
			keyLen := int(binary.LittleEndian.Uint16(base[p+8 : p+10]))
			valPos := p + 10 + keyLen
			if valPos+4 <= len(base) {
				for _, valLen := range []uint32{
					uint32(len(base)-valPos-4) + 1,
					math.MaxUint32,
					1 << 31,
				} {
					b := clone()
					binary.LittleEndian.PutUint32(b[valPos:valPos+4], valLen)
					add(b)
				}
			}
		}
	}

	return seeds
}

// findSectionOffset locates a section body by walking the directory the file
// declares, without going through the reader under test.
//
// Deliberately a second, dumber implementation. A seed builder that called
// readCSRSectionDirectory would produce no seed at all for exactly the files
// where that function is wrong, which is the population the seeds exist to
// reach.
func findSectionOffset(data []byte, magic string) (int, bool) {
	if len(data) < csrV8HeaderSize {
		return 0, false
	}
	tableOff := binary.LittleEndian.Uint64(data[62:70])
	if tableOff == 0 || tableOff+2 > uint64(len(data)) {
		return 0, false
	}
	count := int(binary.LittleEndian.Uint16(data[tableOff : tableOff+2]))
	pos := int(tableOff) + 2
	for i := 0; i < count; i++ {
		if pos+csrSectionEntry > len(data) {
			return 0, false
		}
		if string(data[pos:pos+4]) == magic {
			off := binary.LittleEndian.Uint64(data[pos+8 : pos+16])
			if off > uint64(len(data)) {
				return 0, false
			}
			return int(off), true
		}
		pos += csrSectionEntry
	}
	return 0, false
}

// TestHostileCSRSeeds_AreAllRejected is the assertion the seeds exist to carry
// into the corpus, stated once as an ordinary test so it runs on every push
// rather than only when someone fuzzes.
//
// Every seed is a file the parser must refuse or parse safely — never crash on,
// and never allocate from. A seed that is accepted is not automatically a bug
// (truncating a file at the header end leaves a legal empty image), so the
// assertion is the weaker, true one: it returns.
func TestHostileCSRSeeds_AreAllRejected(t *testing.T) {
	seeds := hostileCSRSeeds(t)
	if len(seeds) < 20 {
		t.Fatalf("only %d hostile seeds were built; the builder is not finding "+
			"the structures it thinks it is", len(seeds))
	}
	for i, seed := range seeds {
		csr, _, err := deserialiseCSR(seed)
		if err != nil {
			continue
		}
		if csr == nil {
			t.Errorf("seed %d: nil graph with nil error", i)
		}
	}
}

// --- GORD -----------------------------------------------------------------

// FuzzReadOrderedKeySection explores the ordered-key declaration parser.
//
// The section carries nothing but key names, which is why it went uncovered:
// there is no allocation here proportional to anything but the names
// themselves. What there is instead is four length prefixes read from the file
// in sequence, each of which decides how far the next read goes.
//
// The property asserted is idempotence rather than round-trip equality of
// bytes. The reader stops when it has read the counts it was promised and does
// not require the section to end there, so a section with trailing bytes parses
// to a value whose re-encoding is shorter — correctly. What must hold is that
// re-encoding and re-parsing yields the same declarations, because a reader
// that disagreed with the writer about its own output would rewrite a store's
// index declarations on every compaction.
func FuzzReadOrderedKeySection(f *testing.F) {
	f.Add(appendOrderedKeySection(nil, []string{"sha256", "path"}, []string{"rel"}))
	f.Add(appendOrderedKeySection(nil, nil, nil))
	f.Add(appendOrderedKeySection(nil, []string{""}, []string{""}))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})

	// A count larger than the remaining bytes can encode: the allocation bound.
	huge := make([]byte, 8)
	binary.LittleEndian.PutUint32(huge[0:4], math.MaxUint32)
	f.Add(huge)
	// A key whose declared length runs off the end.
	overrun := make([]byte, 6)
	binary.LittleEndian.PutUint32(overrun[0:4], 1)
	binary.LittleEndian.PutUint16(overrun[4:6], math.MaxUint16)
	f.Add(overrun)

	f.Fuzz(func(t *testing.T, data []byte) {
		nodeKeys, edgeKeys, err := readOrderedKeySection(data)
		if err != nil {
			return
		}

		// Every returned key was cut out of the input, so the section cannot
		// declare more keys than its own length permits. A bound removed from
		// the reader shows up here as a count the input could not have carried.
		if total := len(nodeKeys) + len(edgeKeys); total > len(data)/minOrderedKeyEntry {
			t.Fatalf("%d bytes yielded %d key declarations, more than %d can encode",
				len(data), total, len(data)/minOrderedKeyEntry)
		}

		reNode, reEdge, err := readOrderedKeySection(appendOrderedKeySection(nil, nodeKeys, edgeKeys))
		if err != nil {
			t.Fatalf("the writer produced bytes the reader rejects: %v", err)
		}
		if !sameStrings(nodeKeys, reNode) || !sameStrings(edgeKeys, reEdge) {
			t.Fatalf("re-encoding changed the declarations: node %q -> %q, edge %q -> %q",
				nodeKeys, reNode, edgeKeys, reEdge)
		}
	})
}

// --- GCMP -----------------------------------------------------------------

// FuzzReadCompositeSection explores the composite-declaration parser.
//
// One nesting level deeper than GORD: a count of tuples, each with a count of
// keys, each with a length. Three length prefixes deep is where a bound written
// against the wrong remaining length stops being obvious — the inner check has
// to be against the bytes left *now*, not the bytes the section started with,
// and a fuzzer finds that difference far faster than a reader does.
func FuzzReadCompositeSection(f *testing.F) {
	f.Add(appendCompositeSection(nil, [][]string{{"case", "bucket"}}, [][]string{{"rel"}}))
	f.Add(appendCompositeSection(nil, nil, nil))
	f.Add(appendCompositeSection(nil, [][]string{{}}, [][]string{{}}))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})

	huge := make([]byte, 8)
	binary.LittleEndian.PutUint32(huge[0:4], math.MaxUint32)
	f.Add(huge)
	// One tuple claiming the maximum number of keys, in a section with none.
	wideTuple := make([]byte, 6)
	binary.LittleEndian.PutUint32(wideTuple[0:4], 1)
	binary.LittleEndian.PutUint16(wideTuple[4:6], math.MaxUint16)
	f.Add(wideTuple)

	f.Fuzz(func(t *testing.T, data []byte) {
		nodeTuples, edgeTuples, err := readCompositeSection(data)
		if err != nil {
			return
		}

		// Same bound, one level down: every key in every tuple came out of the
		// input, so the total across all tuples is capped by the input length.
		total := 0
		for _, tuple := range nodeTuples {
			total += len(tuple)
		}
		for _, tuple := range edgeTuples {
			total += len(tuple)
		}
		if total > len(data)/minOrderedKeyEntry {
			t.Fatalf("%d bytes yielded %d keys across %d tuples, more than %d can encode",
				len(data), total, len(nodeTuples)+len(edgeTuples), len(data)/minOrderedKeyEntry)
		}

		reNode, reEdge, err := readCompositeSection(appendCompositeSection(nil, nodeTuples, edgeTuples))
		if err != nil {
			t.Fatalf("the writer produced bytes the reader rejects: %v", err)
		}
		if !sameTuples(nodeTuples, reNode) || !sameTuples(edgeTuples, reEdge) {
			t.Fatalf("re-encoding changed the declarations: node %q -> %q, edge %q -> %q",
				nodeTuples, reNode, edgeTuples, reEdge)
		}
	})
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameTuples(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameStrings(a[i], b[i]) {
			return false
		}
	}
	return true
}
