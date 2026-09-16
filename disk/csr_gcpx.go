package disk

// GCPX: the composite postings as bytes in the image rather than maps in the
// heap.
//
// # Why this section exists
//
// A composite index answers a conjunction of equality filters in one lookup
// instead of driving from one key and eliminating against the others. To do that
// it holds, resident, a posting list per distinct tuple and a per-entity row of
// the member values each entity carries -- and that is now the largest single
// term in a default configuration. Measured at 1,400,000 nodes it is 128.2 MiB
// of the 237.8 MiB such a store holds, 53.9%, ahead of the record arrays at
// 74.8; at the 1,800,000-node shape it is 164.8 of 305.8, the same fraction to a
// tenth of a point across a twentyfold difference in blob size. docs/MEMORY_MODEL.md
// §8.6 measures it and §9.8 confirms it independently, twice, from the ceiling
// harness's own term breakdown.
//
// GPIX did this for the single-key postings and took the property index from
// 94.5% of anonymous memory to a mapping. This is the same move for the one part
// of the index GPIX deliberately left behind.
//
// # Why the composites were left behind, and what changed
//
// index/composite_base_test.go states the reason: a composite is the one part of
// the index a base cannot answer in place, because it is an index over a tuple of
// keys that the forward direction holds separately. Answering a composite from
// GPIX would mean intersecting the member keys' runs on every query -- exactly
// the work a composite is declared to do once. So composites are *filled* from
// the base at open, by index.fillCompositeFromBase, and live in the heap.
//
// Nothing about that argument is wrong. What it establishes is that a composite
// cannot be *derived* from GPIX cheaply, which is a reason to store it, not a
// reason to store it in memory. This section stores it.
//
// appendCompositeSection's comment gives the other half, and it is the one this
// section has to answer directly:
//
//	Only the declarations travel. The postings and the per-entity member values
//	are rebuilt from the property-index entries in the same file, which is the
//	same trade GORD makes and for the same reason: the derived structure cannot
//	go stale relative to the entries if it is always derived from them, and the
//	bytes it would have taken buy nothing that the entries do not already hold.
//
// Both clauses were true when written and the second one no longer is. "The bytes
// buy nothing" was an argument about the *image*, made when the index was rebuilt
// into the heap at open and the only question was how large the file was. Once the
// index is read in place, the bytes buy the difference between a structure that is
// mapped and one that is resident, which at the shape this engine is sized for is
// 128.2 MiB of a 2 GiB budget. The first clause -- staleness -- is answered below,
// and it is answered the same way GPIX answers it rather than by being dismissed.
//
// # Staleness, which is the whole of the correctness argument
//
// A derived structure written into a file can disagree with the entries it was
// derived from. GPIX faces this and settles it with two rules, and this section
// takes both unchanged:
//
//   - A base is only ever installed by the code that just wrote it from the index
//     it replaces, having established that nothing was registered or retracted in
//     between. That is index.SwapBase's precondition and Store.compactCommit is
//     what establishes it. There is no path that reads a composite section out of
//     an image and files it against an index whose delta describes a different
//     image.
//
//   - Everything registered since is in the delta, and everything removed since is
//     retracted by the one bit per entity that index/retract.go already maintains.
//     A composite read is base(tuple) − retracted ∪ delta(tuple), which is the
//     same three terms and the same asymmetry as a single-key read, because the
//     retraction grain -- "this entity's entries are no longer these" -- is exactly
//     the grain a composite needs. An entity whose member values changed is
//     retracted from the base and re-registered into the delta, so its old tuples
//     stop answering and its new ones start, with no per-tuple bookkeeping at all.
//
// So this section cannot go stale relative to the entries in the same file, for
// the same reason GPIX cannot: it is written from them, in the same pass, and
// installed only together with them.
//
// # Criticality: OPTIONAL, and this is the difference from GPIX
//
// GPIX is CRITICAL because a v9 image carries no GIDX -- there is no second copy
// of the entries to fall back to, so a build that skipped GPIX would answer every
// property query with no matches rather than slowly.
//
// GCPX has a fallback and it is the one every reader used before this section
// existed: index.fillCompositesFromBase walks the member keys' entries in GPIX
// and files them, which is what a store does today at every open. A build that
// does not understand this section skips it, fills the composites from the
// entries, and answers every composite query correctly while paying an open-time
// pass it cannot see it is paying. That is the definition of optional in
// csr_v8.go's registry, and GORD is the precedent stated there.
//
// The consequence is the one that matters for shipping it: a v0.7.x reader meets
// an image carrying GCPX, does not recognise the magic, finds the section not
// marked critical, and skips it. No version bump past v9, no stranded readers, no
// bidirectional migration.
//
// # Why not simply extend GCMP
//
// GCMP carries the composite *declarations* and a v0.7.x reader parses its body.
// Appending postings to that body would hand an older build bytes it would read
// as a malformed declaration list, and it would fail the image rather than skip
// the part it does not understand -- turning the one guarantee the optional flag
// exists to provide into a corrupt-file error. A new magic costs four bytes in a
// registry and is skipped cleanly by every build that predates it.
//
// # The layout, and why it is forward-only
//
// imageWriter appends and has no Seek, for the reason csr_gpix.go gives at
// length. So every offset a reader needs is either known before the bytes it
// describes or recorded after them.
//
//	GCPX body
//	  header      magic "GCPX" | bodyVersion u16 | flags u16 | nodeIdx u32 | edgeIdx u32
//	  per index   node composites in declaration order, then edge:
//	    runs      per distinct tuple, ascending by the encoded bytes:
//	              tupLen u32 | tuple | idCount u32 | ids (idCount × u64)
//	  ttabs       per index, in that same order:
//	              (distinct+1) × { prefix [8]byte | runOff u64 }
//	              runOff is relative to that index's runsOff; entry [distinct] is
//	              a sentinel whose runOff is runsLen, so every run's end is the
//	              next entry's start with no special case for the last one.
//	  cdir        count × gcpxDirSize, ordered by (kind, encoded key tuple)
//	  key bytes   each index's key tuple, addressed by the cdir
//	  footer      cdirOff u64
//
// This is GPIX's arrangement with a composite where GPIX has a key and an encoded
// tuple where GPIX has a value, and the parts that look copied are copied on
// purpose: the value table after the runs because a vtab entry holds the offset
// of its run, the directory last because it carries offsets that are not known
// until the runs are written, and the footer because the last eight bytes of a
// known-length section need no forward reference.
//
// # The encoded tuple, and why byte order is enough
//
// A tuple is index.encodeTuple's output: each member value prefixed by its length
// as a little-endian u32. That encoding is not order-preserving with respect to
// the values -- a little-endian length prefix sees to that -- and it does not need
// to be. The only question ever asked of this section is whether a *specific*
// tuple is present, and the querying side encodes its tuple with the same function
// before asking. So the runs ascend by bytes.Compare over the encoded form, the
// ttab is binary-searched in that order, and the order is a total order the two
// sides agree on rather than a claim about the values inside.
//
// It is worth being explicit that this rules something out: GCPX cannot answer a
// range or a prefix query over a composite, because its order is not the values'
// order. Neither can the resident composite index it replaces -- compositeIndex
// holds postings in a map keyed by the same encoding and has no ordered read at
// all -- so nothing is lost. A composite that wanted a range would need the tuple
// encoded order-preservingly, and that is a different index.
//
// # The 8-byte prefix
//
// A ttab entry carries the first eight bytes of the encoded tuple, zero-padded, so
// a binary search compares two integers before it dereferences two runs. As in
// GPIX this is a fast path and not the comparison key: entries sharing a prefix
// are ordered by reading them. The soundness argument is gpixPrefixOf's and is not
// restated here, because it is an argument about bytes and these are bytes.
//
// In practice the prefix is weaker here than in GPIX, and it is worth saying why
// rather than letting a reader assume otherwise. The first four bytes of every
// encoded tuple are the first member's length, so tuples whose first values are
// the same length agree on half the prefix before any value byte is compared. It
// remains a strict improvement over dereferencing every probe and it is never
// wrong; it is simply less selective than it looks. Making it more selective would
// mean changing the tuple encoding, which is index.encodeTuple's business and
// would be a change to the resident index in order to speed up the mapped one.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	// gcpxBodyVersion is the GCPX body version, independent of the image version
	// for the reason gpixBodyVersion gives.
	gcpxBodyVersion = 1

	gcpxHeaderSize = 16 // magic4 + bodyVersion2 + flags2 + nodeIdx4 + edgeIdx4
	gcpxFooterSize = 8  // cdirOff

	// gcpxTtabEntry is one tuple-table entry: an 8-byte prefix of the encoded
	// tuple and the offset of its run, relative to the index's runsOff.
	gcpxTtabEntry = 16

	// gcpxDirSize is one composite-directory entry.
	//
	//	keysOff u64 | keysLen u32 | kind u8 | pad u8 | width u16 |
	//	distinct u64 | entries u64 | ttabOff u64 | runsOff u64 | runsLen u64
	gcpxDirSize = 56

	// gcpxMinRun is the smallest a run can be: two length prefixes, an empty
	// tuple and no ids. Bounds a distinct count against the bytes an index's runs
	// region actually holds, before the count sizes anything.
	gcpxMinRun = 8

	gcpxKindNode = 0
	gcpxKindEdge = 1
)

// gcpxPrefixOf returns the first eight bytes of an encoded tuple, zero-padded, as
// the big-endian integer the ttab compares.
func gcpxPrefixOf(tuple []byte) uint64 {
	var buf [8]byte
	copy(buf[:], tuple)
	return binary.BigEndian.Uint64(buf[:])
}

// gcpxEncodeKeys packs one composite's declared key tuple into the bytes the cdir
// addresses: a u16 count, then each key as a u16 length and its bytes.
//
// The declared tuple is what identifies a composite in this section, rather than
// its position among the declarations. A store can be reopened with its composites
// declared in a different order, or with one of them dropped, and neither is a
// schema change the image records -- so an ordinal would silently hand one
// composite's postings to another. Matching on the tuple makes that unrepresentable:
// a declaration the image does not carry finds nothing and is filled from the
// entries, which is exactly the fallback path for a reader that skipped the
// section entirely.
func gcpxEncodeKeys(keys []string) []byte {
	n := 2
	for _, k := range keys {
		n += 2 + len(k)
	}
	out := make([]byte, 0, n)
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(len(keys)))
	out = append(out, buf[:]...)
	for _, k := range keys {
		binary.LittleEndian.PutUint16(buf[:], uint16(len(k)))
		out = append(out, buf[:]...)
		out = append(out, k...)
	}
	return out
}

// gcpxDecodeKeys is gcpxEncodeKeys' inverse, bounded against the bytes it was
// given.
func gcpxDecodeKeys(data []byte) ([]string, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("gcpx: truncated key tuple header")
	}
	count := int(binary.LittleEndian.Uint16(data))
	pos := 2
	// A key costs at least its own two-byte length prefix, so a count larger than
	// the remaining bytes can encode is invalid before it sizes a slice.
	if count > (len(data)-pos)/2 {
		return nil, fmt.Errorf("gcpx: key tuple claims %d keys, more than %d remaining bytes can hold",
			count, len(data)-pos)
	}
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("gcpx: truncated key %d length", i)
		}
		n := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+n > len(data) {
			return nil, fmt.Errorf("gcpx: truncated key %d body", i)
		}
		keys = append(keys, string(data[pos:pos+n]))
		pos += n
	}
	if pos != len(data) {
		return nil, fmt.Errorf("gcpx: key tuple has %d trailing bytes", len(data)-pos)
	}
	return keys, nil
}

// --- writing ---

// gcpxDir is one cdir entry as the encoder accumulates it. The encoder holds one
// per declared composite and nothing else, which is its whole resident cost.
type gcpxDir struct {
	Keys     []string
	Kind     uint8
	Distinct uint64
	Entries  uint64
	TtabOff  uint64
	RunsOff  uint64
	RunsLen  uint64
}

// gcpxTupleWalker yields every distinct encoded tuple of one composite together
// with the ids filed under it. It is called once per composite.
//
// Both orders are load-bearing and neither is checked by the reader: tuples must
// ascend by bytes.Compare, which is the order the ttab is binary-searched in, and
// a tuple's ids must ascend, which is what lets a merged read walk a run and the
// delta's postings together. A source that breaks either produces a file whose
// reads are bounded and wrong, and naming that is the O(entries) pass the verifier
// pays. The writer checks both as it writes, where it costs one comparison per
// tuple and one per id.
//
// The ids are the walker's own memory and are neither retained nor mutated.
type gcpxTupleWalker func(kind uint8, keys []string, fn func(tuple []byte, ids []uint64) bool) error

// gcpxTtabMemCap is how much of the tuple-table intermediate is held in memory
// before it spills, for the reason and at the size gpixVtabMemCap documents.
const gcpxTtabMemCap = 1 << 20

// gcpxSource is everything the composite section is written from.
type gcpxSource struct {
	// NodeComposites and EdgeComposites are the declared key tuples, each in
	// declaration order within itself. The slice order does not matter: the
	// encoder sorts the directory by (kind, encoded keys) so that one store's
	// image does not depend on the order its composites happened to be declared
	// in, which is the same determinism requirement the GPIX key list meets by
	// arriving sorted.
	NodeComposites, EdgeComposites [][]string

	// Tuples walks one composite's distinct tuples. Nil means no composite yields
	// anything, which is not the same as no section: an image whose composites are
	// all empty still carries GCPX holding their directory entries, because "this
	// composite matched nothing at the last compaction" is a fact a reader must be
	// able to read rather than infer from an absence -- and inferring it wrongly
	// would mean filling from the entries, which is the slow correct answer rather
	// than the fast one.
	Tuples gcpxTupleWalker

	// ScratchDir is where the tuple table spills. Empty means the operating
	// system's temporary directory; compaction names the store's own directory.
	ScratchDir string

	// buffers sizes the intermediate, taking that consumer's default when zero.
	buffers compactBuffers
}

// writeGCPX writes the GCPX body and returns the directory it wrote.
func writeGCPX(iw *imageWriter, base uint64, src gcpxSource) ([]gcpxDir, error) {
	iw.str(csrSectionCompositeIndex)
	iw.u16(gcpxBodyVersion)
	iw.u16(0)
	iw.u32(uint32(len(src.NodeComposites)))
	iw.u32(uint32(len(src.EdgeComposites)))

	memCap := src.buffers.vtabMemCap
	if memCap <= 0 {
		memCap = gcpxTtabMemCap
	}
	ttab := newSpill(src.ScratchDir, memCap)
	defer ttab.close()

	dirs := make([]gcpxDir, 0, len(src.NodeComposites)+len(src.EdgeComposites))
	var err error
	if dirs, err = writeGCPXKind(iw, base, dirs, ttab, gcpxKindNode, src.NodeComposites, src.Tuples); err != nil {
		return nil, err
	}
	if dirs, err = writeGCPXKind(iw, base, dirs, ttab, gcpxKindEdge, src.EdgeComposites, src.Tuples); err != nil {
		return nil, err
	}

	// The tuple tables, copied out of the spill in one pass. Each entry's TtabOff
	// has held its offset within the spill up to here; it becomes an offset within
	// the section now that the region's start is known.
	ttabRegion := iw.at() - base
	r, err := ttab.reader()
	if err != nil {
		return nil, err
	}
	iw.from(r, ttab.len())
	for i := range dirs {
		dirs[i].TtabOff += ttabRegion
	}

	// The directory, ordered by (kind, encoded keys). The order is for
	// determinism rather than for search -- a store declares a handful of
	// composites and the reader scans them linearly, which is cheaper than a
	// binary search over a handful and needs no ordering invariant at read time.
	// Two compactions of the same content must produce the same bytes, though,
	// and that is what this sort is for. See TestGCPX_DirectoryOrderIsDeterministic.
	sort.SliceStable(dirs, func(i, j int) bool {
		if dirs[i].Kind != dirs[j].Kind {
			return dirs[i].Kind < dirs[j].Kind
		}
		return bytes.Compare(gcpxEncodeKeys(dirs[i].Keys), gcpxEncodeKeys(dirs[j].Keys)) < 0
	})

	encoded := make([][]byte, len(dirs))
	for i := range dirs {
		encoded[i] = gcpxEncodeKeys(dirs[i].Keys)
	}
	cdirOff := iw.at() - base
	keyBytesOff := cdirOff + uint64(len(dirs))*gcpxDirSize
	var keyAt uint64
	for i := range dirs {
		d := &dirs[i]
		iw.u64(keyBytesOff + keyAt)
		iw.u32(uint32(len(encoded[i])))
		iw.u8(d.Kind)
		iw.u8(0)
		iw.u16(uint16(len(d.Keys)))
		iw.u64(d.Distinct)
		iw.u64(d.Entries)
		iw.u64(d.TtabOff)
		iw.u64(d.RunsOff)
		iw.u64(d.RunsLen)
		keyAt += uint64(len(encoded[i]))
	}
	for i := range encoded {
		iw.bytes(encoded[i])
	}
	iw.u64(cdirOff)

	if iw.err != nil {
		return nil, iw.err
	}
	return dirs, nil
}

// writeGCPXKind writes one kind's composites, appending a directory entry each.
func writeGCPXKind(iw *imageWriter, base uint64, dirs []gcpxDir, ttab *spillBuffer,
	kind uint8, composites [][]string, walk gcpxTupleWalker) ([]gcpxDir, error) {

	// One scratch entry per composite rather than one per tuple. It escapes --
	// spillBuffer.write can hand a slice straight to an os.File -- and an escaping
	// array declared inside the walk is a heap allocation per distinct tuple,
	// which is the cost-proportional-to-the-index this section exists to remove.
	var ent [gcpxTtabEntry]byte
	// prev is the previous encoded tuple, for the order check. Copied rather than
	// retained: the walk owns the bytes it yields only for the duration of the
	// call.
	var prev []byte
	for _, keys := range composites {
		d := gcpxDir{Keys: keys, Kind: kind}
		d.TtabOff = ttab.len() // spill-relative until writeGCPX places the region
		d.RunsOff = iw.at() - base
		var runAt, distinct, entries uint64
		var orderErr error
		havePrev := false
		if walk != nil {
			if err := walk(kind, keys, func(tuple []byte, ids []uint64) bool {
				// A tuple no entity holds is not in the index and is not written.
				// The merged walk a compaction reads from skips a tuple whose every
				// id has been retracted, so writing one would put in the image
				// something the index cannot produce -- and the next compaction of
				// the same content would not write it, which is a difference in
				// bytes the digest covers.
				if len(ids) == 0 {
					return true
				}
				if havePrev && bytes.Compare(prev, tuple) >= 0 {
					orderErr = fmt.Errorf("gcpx: composite %v tuples are not ascending: %x after %x",
						keys, tuple, prev)
					return false
				}
				prev, havePrev = append(prev[:0], tuple...), true
				for i := 1; i < len(ids); i++ {
					if ids[i] <= ids[i-1] {
						orderErr = fmt.Errorf("gcpx: composite %v tuple %x has ids out of order at %d: %d after %d",
							keys, tuple, i, ids[i], ids[i-1])
						return false
					}
				}
				binary.BigEndian.PutUint64(ent[0:8], gcpxPrefixOf(tuple))
				binary.LittleEndian.PutUint64(ent[8:16], runAt)
				ttab.write(ent[:])

				iw.u32(uint32(len(tuple)))
				iw.bytes(tuple)
				iw.u32(uint32(len(ids)))
				for _, id := range ids {
					iw.u64(id)
				}
				runAt += 4 + uint64(len(tuple)) + 4 + 8*uint64(len(ids))
				distinct++
				entries += uint64(len(ids))
				return iw.err == nil
			}); err != nil {
				return nil, err
			}
		}
		if orderErr != nil {
			return nil, orderErr
		}
		if iw.err != nil {
			return nil, iw.err
		}
		// The sentinel, whose runOff is the region's length, so every run's end is
		// the next entry's start with no special case for the last one.
		//
		// Its prefix is all-ones, which is GPIX's convention and is chosen for
		// the same reason: no search reads it -- the binary search stops at
		// Distinct -- and a value that sorts above every real prefix is the one
		// that cannot turn a future reader's off-by-one into a match. verifyGCPX
		// checks it, because a field nothing reads is exactly where byte
		// determinism quietly stops being true.
		binary.BigEndian.PutUint64(ent[0:8], ^uint64(0))
		binary.LittleEndian.PutUint64(ent[8:16], runAt)
		ttab.write(ent[:])

		d.Distinct = distinct
		d.Entries = entries
		d.RunsLen = runAt
		dirs = append(dirs, d)
	}
	return dirs, nil
}

// --- reading ---

// gcpxComposite is one composite's directory entry, resolved against the body.
type gcpxComposite struct {
	keys     []string
	kind     uint8
	distinct uint64
	entries  uint64
	ttab     []byte
	runs     []byte
}

// gcpxSection is a parsed GCPX body. It holds no entry: every field below is a
// subslice of the image, so a parsed section costs one small struct per declared
// composite however many tuples the image carries.
type gcpxSection struct {
	body       []byte
	composites []gcpxComposite
}

// parseGCPX reads a GCPX body's directory and bounds every extent it names
// against the body.
//
// Everything this function checks is a bound: that an offset and a length name
// bytes inside the section, that a count cannot exceed what the bytes it
// describes could hold. It deliberately does not check that tuples ascend or that
// ids do -- those are passes over every entry, and charging every open for one is
// the cost this design exists to remove. verifyGCPX is where they are checked,
// and Base.Verify's comment is the argument for that split.
func parseGCPX(body []byte) (*gcpxSection, error) {
	if len(body) < gcpxHeaderSize+gcpxFooterSize {
		return nil, fmt.Errorf("gcpx: section is %d bytes, shorter than an empty one", len(body))
	}
	if string(body[0:4]) != csrSectionCompositeIndex {
		return nil, fmt.Errorf("gcpx: section magic is %q", body[0:4])
	}
	if v := binary.LittleEndian.Uint16(body[4:6]); v != gcpxBodyVersion {
		return nil, fmt.Errorf("gcpx: body version %d, this build reads %d", v, gcpxBodyVersion)
	}
	nodeIdx := uint64(binary.LittleEndian.Uint32(body[8:12]))
	edgeIdx := uint64(binary.LittleEndian.Uint32(body[12:16]))
	total := nodeIdx + edgeIdx

	limit := uint64(len(body))
	cdirOff := binary.LittleEndian.Uint64(body[len(body)-gcpxFooterSize:])
	if !gcpxWithin(cdirOff, total*gcpxDirSize, limit) {
		return nil, fmt.Errorf("gcpx: directory of %d entries at %d does not fit a %d-byte section",
			total, cdirOff, limit)
	}
	// A count larger than the bytes between the directory and the footer could
	// hold is invalid before it sizes a slice.
	if total > (limit-cdirOff)/gcpxDirSize {
		return nil, fmt.Errorf("gcpx: %d directory entries exceed the %d bytes that follow them",
			total, limit-cdirOff)
	}

	s := &gcpxSection{body: body, composites: make([]gcpxComposite, 0, total)}
	for i := uint64(0); i < total; i++ {
		at := cdirOff + i*gcpxDirSize
		e := body[at : at+gcpxDirSize]
		keysOff := binary.LittleEndian.Uint64(e[0:8])
		keysLen := uint64(binary.LittleEndian.Uint32(e[8:12]))
		kind := e[12]
		width := binary.LittleEndian.Uint16(e[14:16])
		distinct := binary.LittleEndian.Uint64(e[16:24])
		entries := binary.LittleEndian.Uint64(e[24:32])
		ttabOff := binary.LittleEndian.Uint64(e[32:40])
		runsOff := binary.LittleEndian.Uint64(e[40:48])
		runsLen := binary.LittleEndian.Uint64(e[48:56])

		if kind != gcpxKindNode && kind != gcpxKindEdge {
			return nil, fmt.Errorf("gcpx: directory entry %d has kind %d", i, kind)
		}
		if !gcpxWithin(keysOff, keysLen, limit) {
			return nil, fmt.Errorf("gcpx: entry %d key bytes at %d..%d fall outside a %d-byte section",
				i, keysOff, keysOff+keysLen, limit)
		}
		keys, err := gcpxDecodeKeys(body[keysOff : keysOff+keysLen])
		if err != nil {
			return nil, fmt.Errorf("gcpx: entry %d: %w", i, err)
		}
		if int(width) != len(keys) {
			return nil, fmt.Errorf("gcpx: entry %d claims width %d and carries %d keys", i, width, len(keys))
		}
		// The ttab carries one entry per distinct tuple plus the sentinel.
		ttabLen := (distinct + 1) * gcpxTtabEntry
		if distinct+1 < distinct || !gcpxWithin(ttabOff, ttabLen, limit) {
			return nil, fmt.Errorf("gcpx: entry %d tuple table of %d entries at %d does not fit a %d-byte section",
				i, distinct+1, ttabOff, limit)
		}
		if !gcpxWithin(runsOff, runsLen, limit) {
			return nil, fmt.Errorf("gcpx: entry %d runs at %d..%d fall outside a %d-byte section",
				i, runsOff, runsOff+runsLen, limit)
		}
		// A run costs at least two length prefixes, so a distinct count larger
		// than the runs region could hold is invalid however well it is bounded.
		if distinct > runsLen/gcpxMinRun {
			return nil, fmt.Errorf("gcpx: entry %d claims %d distinct tuples in %d bytes of runs",
				i, distinct, runsLen)
		}
		s.composites = append(s.composites, gcpxComposite{
			keys:     keys,
			kind:     kind,
			distinct: distinct,
			entries:  entries,
			ttab:     body[ttabOff : ttabOff+ttabLen],
			runs:     body[runsOff : runsOff+runsLen],
		})
	}
	return s, nil
}

// gcpxWithin reports whether an extent of length bytes at off fits inside limit.
//
// Written as a subtraction rather than off+length <= limit for the reason
// gpixWithin gives: both operands come out of a file, and a guard that wraps is a
// guard that passes before the slice it protects panics.
func gcpxWithin(off, length, limit uint64) bool {
	return off <= limit && length <= limit-off
}

// find returns the composite declared over keys for kind, or nil.
//
// Linear over the declared composites, which is a handful: a store declaring
// enough composites for this to matter has a schema problem a binary search would
// hide. The comparison is on the key tuple rather than on a position, for the
// reason gcpxEncodeKeys gives.
func (s *gcpxSection) find(kind uint8, keys []string) *gcpxComposite {
	for i := range s.composites {
		c := &s.composites[i]
		if c.kind != kind || len(c.keys) != len(keys) {
			continue
		}
		same := true
		for j := range keys {
			if c.keys[j] != keys[j] {
				same = false
				break
			}
		}
		if same {
			return c
		}
	}
	return nil
}

// runAt returns the run beginning at off within c's runs region, as the encoded
// tuple and the raw id bytes.
//
// Every length it reads is bounded against the region before it is used, because
// the bytes come from a file: the caller is a binary search that trusts the
// directory's offsets and nothing inside the runs.
func (c *gcpxComposite) runAt(off uint64) (tuple []byte, ids []byte, err error) {
	region := uint64(len(c.runs))
	if !gcpxWithin(off, 4, region) {
		return nil, nil, fmt.Errorf("gcpx: run at %d has no tuple length in %d bytes", off, region)
	}
	tupLen := uint64(binary.LittleEndian.Uint32(c.runs[off:]))
	off += 4
	if !gcpxWithin(off, tupLen, region) {
		return nil, nil, fmt.Errorf("gcpx: run tuple of %d bytes at %d overruns %d", tupLen, off, region)
	}
	tuple = c.runs[off : off+tupLen]
	off += tupLen
	if !gcpxWithin(off, 4, region) {
		return nil, nil, fmt.Errorf("gcpx: run at %d has no id count", off)
	}
	count := uint64(binary.LittleEndian.Uint32(c.runs[off:]))
	off += 4
	// 8 bytes an id, checked as a division so the multiplication cannot wrap.
	if count > (region-off)/8 {
		return nil, nil, fmt.Errorf("gcpx: run claims %d ids, more than %d remaining bytes hold",
			count, region-off)
	}
	ids = c.runs[off : off+count*8]
	return tuple, ids, nil
}

// lookup returns the raw id bytes filed under an encoded tuple, or nil.
//
// A binary search over the tuple table, comparing the 8-byte prefix first and
// reading the run only when the prefixes agree.
func (c *gcpxComposite) lookup(tuple []byte) ([]byte, error) {
	want := gcpxPrefixOf(tuple)
	lo, hi := uint64(0), c.distinct
	for lo < hi {
		mid := lo + (hi-lo)/2
		ent := c.ttab[mid*gcpxTtabEntry:]
		got := binary.BigEndian.Uint64(ent[0:8])
		if got < want {
			lo = mid + 1
			continue
		}
		if got > want {
			hi = mid
			continue
		}
		// The prefixes agree, so the order is decided by the tuples themselves.
		off := binary.LittleEndian.Uint64(ent[8:16])
		have, ids, err := c.runAt(off)
		if err != nil {
			return nil, err
		}
		switch bytes.Compare(have, tuple) {
		case 0:
			return ids, nil
		case -1:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return nil, nil
}

// forEachRun walks every run in order, handing each its tuple and id bytes. The
// verifier is the caller; no read path walks a composite.
func (c *gcpxComposite) forEachRun(fn func(tuple []byte, ids []byte) bool) error {
	for i := uint64(0); i < c.distinct; i++ {
		off := binary.LittleEndian.Uint64(c.ttab[i*gcpxTtabEntry+8:])
		tuple, ids, err := c.runAt(off)
		if err != nil {
			return err
		}
		if !fn(tuple, ids) {
			return nil
		}
	}
	return nil
}
