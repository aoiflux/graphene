package disk

// Writing an image, one section at a time, without holding it.
//
// Serialisation used to build the whole file in a bytes.Buffer and hand back a
// []byte for the caller to write. On the store this program is aimed at that is
// a 1.2 GiB allocation — with the buffer's own doubling it is transiently
// closer to 1.8 GiB — held at the exact moment compaction is already holding a
// freshly built CSR and a plan. It was the single largest allocation the engine
// made, and nothing read it twice: compaction's only use for the bytes was to
// pass them to writeFileSync.
//
// SerialiseTo streams the same bytes into an io.ReadWriteSeeker instead. The
// additional memory is a constant: one 64 KiB output buffer, one 64 KiB buffer
// for the digest pass, the Merkle builders' O(log n) stacks, and one scratch
// buffer sized by the largest single record. SerialiseWithPayload is kept as a
// wrapper that streams into memory, so the two cannot produce different bytes —
// there is only one writer now, and the golden fixture in
// csr_golden_bytes_test.go holds it to the bytes the buffer version wrote.
//
// # Why the file is read back for the digest
//
// The digest covers the header, and the header carries sectionTableOffset,
// which is not known until every section has been written. SHA-256 is
// sequential and the header is hashed first, so the digest cannot be teed off
// the write. The alternatives were to compute the total size up front — which
// means a second, parallel implementation of every encoder whose disagreement
// with the real one would corrupt an image silently — or to read the finished
// file back and hash it. The read is a sequential pass over bytes that were
// written moments ago and are still in the page cache, it costs no memory
// beyond the copy buffer, and it shares csrDigestHeader with every other digest
// path in the engine. Wall clock is the cheaper thing to spend here.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"math"
	"slices"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// imageWriteBufferSize is the one buffer a serialisation holds: it is filled on
// the way out and reused as the read buffer for the digest pass. 64 KiB is
// large enough that the syscall count is irrelevant beside the image and small
// enough to be a rounding error in the footprint this whole item exists to
// bound.
const imageWriteBufferSize = 64 << 10

// imageWriter is a counting, buffering, little-endian writer over the image
// being built.
//
// It tracks the offset because every section directory entry is an (offset,
// length) pair into the finished file — the position bytes.Buffer.Len() used to
// supply. It latches the first write error and drops everything after it, so
// one check at flush is equivalent to a check after every call; the offsets
// recorded past a failure are discarded along with the file.
//
// It buffers rather than wrapping bufio so that the caller owns the buffer and
// can hand the same 64 KiB to the digest pass. A blob at least as large as the
// buffer goes straight through, which spares the copy on exactly the records
// that would make it expensive.
type imageWriter struct {
	w   io.Writer
	buf []byte
	n   uint64
	err error
	num [8]byte
}

func newImageWriter(w io.Writer, buf []byte) *imageWriter {
	return &imageWriter{w: w, buf: buf[:0]}
}

// at is the offset the next byte will be written at.
func (iw *imageWriter) at() uint64 { return iw.n }

func (iw *imageWriter) bytes(p []byte) {
	if iw.err != nil {
		return
	}
	iw.n += uint64(len(p))
	if len(p) >= cap(iw.buf) {
		// Bigger than the buffer: nothing is gained by staging it.
		if !iw.drain() {
			return
		}
		if _, err := iw.w.Write(p); err != nil {
			iw.err = err
		}
		return
	}
	if len(p) > cap(iw.buf)-len(iw.buf) && !iw.drain() {
		return
	}
	iw.buf = append(iw.buf, p...)
}

func (iw *imageWriter) str(s string) {
	if iw.err != nil {
		return
	}
	iw.n += uint64(len(s))
	if len(s) > cap(iw.buf)-len(iw.buf) && !iw.drain() {
		return
	}
	iw.buf = append(iw.buf, s...)
}

func (iw *imageWriter) u8(v byte) {
	if iw.err != nil {
		return
	}
	iw.n++
	if len(iw.buf) == cap(iw.buf) && !iw.drain() {
		return
	}
	iw.buf = append(iw.buf, v)
}

func (iw *imageWriter) u16(v uint16) {
	binary.LittleEndian.PutUint16(iw.num[:2], v)
	iw.bytes(iw.num[:2])
}

func (iw *imageWriter) u32(v uint32) {
	binary.LittleEndian.PutUint32(iw.num[:4], v)
	iw.bytes(iw.num[:4])
}

func (iw *imageWriter) u64(v uint64) {
	binary.LittleEndian.PutUint64(iw.num[:8], v)
	iw.bytes(iw.num[:8])
}

// from copies exactly n bytes out of r.
//
// It exists for the one region of the image that is not encoded in place: the
// value tables of the mapped property index, which are generated while the runs
// they address are being written and therefore arrive from a spill rather than
// from a caller's slice. See csr_gpix.go.
//
// The copy reuses the writer's own buffer, so a region of any size costs no
// allocation. A short read is an error rather than a shorter section: the spill
// is a file this process wrote moments ago, and a section whose directory entry
// promises more bytes than it holds is an image no reader can bound.
func (iw *imageWriter) from(r io.Reader, n uint64) {
	if iw.err != nil || n == 0 {
		return
	}
	if !iw.drain() {
		return
	}
	copied, err := io.CopyBuffer(iw.w, io.LimitReader(r, int64(n)), iw.buf[:cap(iw.buf)])
	iw.buf = iw.buf[:0]
	iw.n += uint64(copied)
	switch {
	case err != nil:
		iw.err = err
	case uint64(copied) != n:
		iw.err = fmt.Errorf("serialise: copied %d of %d bytes", copied, n)
	}
}

// drain empties the buffer, reporting whether the writer is still usable.
func (iw *imageWriter) drain() bool {
	if len(iw.buf) == 0 {
		return iw.err == nil
	}
	if _, err := iw.w.Write(iw.buf); err != nil {
		iw.err = err
	}
	iw.buf = iw.buf[:0]
	return iw.err == nil
}

// flush empties the buffer and reports the first error the writer met.
func (iw *imageWriter) flush() error {
	iw.drain()
	return iw.err
}

// Serialise writes the CSR to the current binary format, csrVersionCurrent —
// v8, the sectioned container.
//
// The header and section directory are documented once, in csr_v8.go; this
// comment does not restate them, because the copy that used to live here
// described v7 long after v8 shipped. What is stable enough to state here is
// the record stream between the header and the sections, unchanged since v7:
//
//	[nodeRecord * nodeCount] (each: id:8 + labelCount:1 + labels:(currentLabelBytesPerValue*N) + propLen:4 + props:N)
//	[rawEdge * edgeCount]    (each: id:8 + src:8 + dst:8 + labelCount:1 + labels:(currentLabelBytesPerValue*N) + weight:4 + propLen:4 + props:N)
//
// v7 was v6 minus the flat adjacency arrays: the reader never parsed them — it
// rebuilds adjacency from the edge records — so they were ~21% of the file
// written on every Compact and read by nobody. v8 kept that and moved the
// property index out of a fixed offset and into a section.
//
// # What is persisted, and what is not
//
// The property index IS persisted: its entries are supplied by the caller in the
// caller's own encoding, so nothing in the file can reconstruct them. Before v6
// they lived only in the WAL, which meant every Compact re-emitted the whole
// index and every restart replayed it — a cost that grew forever.
//
// The label postings are NOT persisted: they are derivable from the node and
// edge records in a single pass at load time (see buildLabelIndex), so storing
// them would add file size and a consistency risk to save very little.
//
// The adjacency arrays are NOT persisted either, for the same reason — Build()
// reconstructs them from the records. They *were* written through v6, and never
// read: the reader has always rebuilt them and then used indexOffset to skip
// whatever lay between. Roughly a fifth of the file was that skipped region.
//
// The property index is reached through the section directory. v6 and v7 gave
// it a dedicated indexOffset field in the header so the reader never had to
// compute its position from the adjacency array sizes; v8 writes that field as
// zero and addresses every section, index included, through the directory.
func (g *CSRGraph) Serialise() []byte {
	return g.SerialiseWithIndex(nil, nil)
}

// csrPayload is everything the caller wants carried in the image's sections.
//
// A struct rather than a growing parameter list: v8 exists so that sections can
// be added without a format change, and a signature that has to change for each
// one would put the friction straight back.
type csrPayload struct {
	// NodeProps and EdgeProps are the property-index entries the image carries,
	// in (key, value, id) order -- a byte-determinism contract, stated on
	// PropertyIndex.NodeEntries.
	//
	// Sequences rather than slices because after the image itself this was the
	// largest thing a compaction materialised: one entry per indexed triple, 48
	// bytes of header and a copied value each, built under the store lock and
	// held until the file had been written. On the store this program is aimed at
	// that is 28 million entries. Streaming them costs the entry being written.
	//
	// A sequence here must be re-runnable, because the caller above may retry a
	// write. slices.Values is, and so is a walk of the property index.
	//
	// Nil means the image carries no property index. It is written as a section
	// holding two zero counts, which is exactly what an empty slice produced.
	//
	// Ignored when MappedIndex is set: the two are two encodings of the same
	// entries, and writing both would put a second ~528 MiB copy in the image
	// that no reader of either kind would read.
	NodeProps iter.Seq[index.NodePropEntry]
	EdgeProps iter.Seq[index.EdgePropEntry]

	// MappedIndex asks for the property index as GPIX and GPIR -- searchable in
	// place out of the image -- instead of GIDX, and stamps the image v9.
	//
	// Nil is the v8 arrangement and the default. Which one a store asks for is
	// the store's decision and is resolved before it gets here: this struct is the
	// payload, and choosing an index encoding is not a payload's business.
	MappedIndex *gpixSource

	// Keys declared ordered. Only the declarations travel — the entries are
	// already in the property index section, and the ordered structure is
	// rebuilt from them at load.
	OrderedNodeKeys []string
	OrderedEdgeKeys []string

	// Key tuples declared composite. Declarations only, for the same reason.
	CompositeNodeKeys [][]string
	CompositeEdgeKeys [][]string

	// Composites asks for the declared composites' *postings* as GCPX, read in
	// place out of the image, instead of being rebuilt into the heap from the
	// entries at every open.
	//
	// Separate from CompositeNodeKeys and not a replacement for it. Those are the
	// declarations and they go in GCMP, which a v0.7.x reader parses; the
	// postings are a section of their own precisely so that appending them to
	// GCMP's body cannot hand an older reader bytes it would read as a malformed
	// declaration list. See csr_gcpx.go, which argues the whole arrangement.
	//
	// Nil writes no section, which is what every image before this carried and
	// what a reader still handles: index.fillCompositesFromBase rebuilds the
	// composites from the entries in the same image, correctly, for the price of
	// a pass at open.
	Composites *gcpxSource

	// PrevSnapshotRoot chains this image to the one it replaces. Zero for a
	// first compaction.
	PrevSnapshotRoot merkle.Hash

	// WithSnapshotRoots asks for the GHSH section. Off by default so that
	// serialising for a test or a fixture does not pay for a Merkle pass.
	WithSnapshotRoots bool

	// Tombstones are this image's record of deliberate removals, projected from
	// the redaction ledger by the caller. They produce the GRDT section and the
	// TombstoneRoot component of the snapshot root.
	//
	// Empty is meaningful: it commits to "this image records no removals", which
	// is a claim rather than the absence of one.
	Tombstones []Tombstone

	// Signer, AttestActorID, AttestUnixNano and PrevAttestation produce a GATT
	// section attesting the snapshot root. Nil Signer writes none.
	Signer          store.Signer
	AttestActorID   uint64
	AttestUnixNano  int64
	PrevAttestation [attestationIDSize]byte
}

// withPropStreams returns the payload with both property-entry sequences
// guaranteed non-nil.
//
// An image carrying no property index is a legitimate payload -- a fixture, a
// test graph, Serialise itself -- and a nil iter.Seq cannot be ranged over,
// because the range is a call. Absorbed once per serialisation rather than
// guarded at each walk, and shared with the verifier that re-derives an image's
// roots, so the two cannot come to disagree about what nil means. The receiver
// is a value, so a caller's payload is untouched.
func (p csrPayload) withPropStreams() csrPayload {
	if p.NodeProps == nil {
		p.NodeProps = slices.Values([]index.NodePropEntry(nil))
	}
	if p.EdgeProps == nil {
		p.EdgeProps = slices.Values([]index.EdgePropEntry(nil))
	}
	return p
}

// imageVersion is the container version this payload's sections require.
//
// What actually stops a v8 build from opening a v9 image is that GPIX and GPIR
// are critical sections it does not understand -- and that is the correct outcome,
// since it could not answer a property query from an index it cannot read. The
// version is what makes the refusal legible: "written by a newer version" rather
// than "unknown section". Everything else about the container is unchanged, so a
// payload with no mapped index is written as v8 and opens in every build since
// v8.
func (p csrPayload) imageVersion() uint16 {
	if p.MappedIndex != nil {
		return csrVersionMappedIndex
	}
	return csrVersionSectioned
}

// SerialiseWithIndex writes the CSR plus the given property-index entries.
//
// Retained for callers that carry nothing but the property index; everything
// else goes through SerialiseWithPayload. Cannot fail, because the only failure
// SerialiseWithPayload has is a signer erroring and this path configures none.
func (g *CSRGraph) SerialiseWithIndex(nodeProps []index.NodePropEntry, edgeProps []index.EdgePropEntry) []byte {
	out, err := g.SerialiseWithPayload(csrPayload{
		NodeProps: slices.Values(nodeProps),
		EdgeProps: slices.Values(edgeProps),
	})
	if err != nil {
		panic("SerialiseWithIndex: unreachable, no signer configured: " + err.Error())
	}
	return out
}

// SerialiseWithPayload writes the CSR and every section the payload asks for,
// into memory.
//
// This is the whole-image form, for callers that want the bytes rather than a
// file: tests, fuzz targets, and SerialiseWithIndex. Compaction uses SerialiseTo
// directly, because holding the image is the thing this item removed.
//
// Errors only when a configured Signer fails. As with a commit, that must abort
// rather than fall back to writing an unattested image: a store configured to
// attest that silently stops is indistinguishable downstream from one that never
// attested at all.
func (g *CSRGraph) SerialiseWithPayload(payload csrPayload) ([]byte, error) {
	m := &memImage{}
	if err := g.SerialiseTo(m, payload); err != nil {
		return nil, err
	}
	return m.buf, nil
}

// SerialiseTo writes the image into dst, which must be empty and is written
// from offset zero.
//
// dst is read as well as written because the digest covers the header and the
// header is not final until the section directory has been placed; see the file
// comment. An *os.File opened O_RDWR|O_TRUNC satisfies this, and so does
// memImage.
//
// The graph's own roots, tombstones and attestation fields are updated as they
// are written, exactly as the buffered writer updated them: an image's identity
// is a property of the image, and the CSRGraph that produced it is the one
// object that can report it afterwards.
func (g *CSRGraph) SerialiseTo(dst io.ReadWriteSeeker, payload csrPayload) error {
	payload = payload.withPropStreams()
	if _, err := dst.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("serialise: seek to start: %w", err)
	}
	// One buffer for the whole serialisation: the writer fills it on the way
	// out and the digest pass reads through it afterwards.
	buf := make([]byte, imageWriteBufferSize)
	iw := newImageWriter(dst, buf)

	// The Merkle pass runs alongside the write rather than after it, which is
	// what spares a []merkle.Hash of one 32-byte entry per node, edge and index
	// entry — on the store this was built for, roughly a gigabyte of hashes
	// whose only use was to be folded into thirty-two bytes. The leaves are
	// hashed in the order they are written, which is the order they were
	// hashed in before, so the roots are the same roots.
	var roots *snapshotRootStream
	if payload.WithSnapshotRoots {
		roots = newSnapshotRootStream(snapshotBodyVersion)
	}

	// Count valid nodes and edges. Build maintains both counters, so the header
	// counts cost nothing to produce and cannot disagree with the record stream
	// written below: the same liveness test decides both.
	nodeCount := g.NodeCount()
	edgeCount := g.EdgeCount()

	// Header. The first 46 bytes keep their v6 meaning and position; v8 appends
	// to them rather than rearranging, so a reader can identify a file and read
	// its counts before it understands anything else. See csr_v8.go.
	iw.str("GCSR")
	iw.u16(payload.imageVersion())
	iw.u64(uint64(nodeCount))
	iw.u64(uint64(edgeCount))
	// Sequence high-water marks (version 5+).
	iw.u64(g.nodeSeqHW)
	iw.u64(g.edgeSeqHW)
	// indexOffset (v6/v7). Written as 0 in v8: the property index is a section
	// now. The field stays so the header prefix does not shift.
	iw.u64(0)
	// v8 additions.
	iw.u64(g.commitSeqHW)
	iw.u64(uint64(g.lastCompactUnixNano))
	sectionTableOffsetPos := int64(iw.at())
	iw.u64(0) // patched once the directory is placed
	digestPos := int64(iw.at())
	var zeroDigest [csrDigestSize]byte
	iw.bytes(zeroDigest[:]) // patched last, over a zeroed field

	// Nodes (variable-length labels), ascending by ID - the order Nodes yields,
	// the order the Merkle leaves are hashed in, and the order every reader
	// since v2 has assumed.
	for n := range g.Nodes() {
		iw.u64(uint64(n.ID))
		iw.u8(byte(len(n.Labels)))
		for _, lbl := range n.Labels {
			iw.u16(uint16(lbl))
		}
		iw.u32(uint32(len(n.Properties)))
		iw.bytes(n.Properties)
		if roots != nil {
			roots.addNode(n)
		}
	}

	// Edges (variable-length labels), ascending by ID for the same reasons.
	for e := range g.Edges() {
		iw.u64(uint64(e.ID))
		iw.u64(uint64(e.Src))
		iw.u64(uint64(e.Dst))
		iw.u8(byte(len(e.Labels)))
		for _, lbl := range e.Labels {
			iw.u16(uint16(lbl))
		}
		iw.u32(math.Float32bits(e.Weight))
		iw.u32(uint32(len(e.Properties)))
		iw.bytes(e.Properties)
		if roots != nil {
			roots.addEdge(e)
		}
	}

	// No adjacency arrays. They were written here through v6 and never read
	// back: deserialiseCSR rebuilds them with Build() from the records it has
	// just parsed, then jumps straight to indexOffset. The test that proved it by
	// corrupting the region is gone; what holds the line now is that no v7+ reader
	// ever seeks into those bytes.
	//
	// On a 100k-node fixture that was ~4.8 MB of a ~22 MB file, written on every
	// Compact and read by nobody.

	// Sections, then the directory that addresses them. The property index is a
	// section now rather than a fixed trailer — see csr_v8.go for why the format
	// grew a directory instead of another offset field.
	var sections []csrSection

	// The property index, in whichever of its two encodings the payload asks
	// for. Both go here, where GIDX has always gone: the sections that follow --
	// the snapshot roots above all -- commit to the entries, so they must be
	// written after them.
	//
	// Positions of the two GIDX counts, patched after the flush. Negative means
	// no GIDX was written, which is what a mapped index means.
	nodePropsCountPos, edgePropsCountPos := int64(-1), int64(-1)
	var nodePropCount, edgePropCount uint64

	if src := payload.MappedIndex; src != nil {
		var err error
		if sections, err = writeMappedIndexSections(iw, sections, *src, roots); err != nil {
			return err
		}
	} else {
		indexOffset := iw.at()
		iw.str(csrIndexSectionMagic)
		// Each half of the section is counted in a u64 that precedes it, and a
		// streamed payload has no length to write there. So the counts go down as
		// zeros and are patched after the flush -- which is what the header's
		// sectionTableOffset has always done, for the same reason, and the digest
		// pass reads the file back after both. The bytes that end up in the file
		// are the bytes the counted form wrote.
		//
		// The values are also owned by whoever yielded them: writePropEntry
		// copies into the output buffer and addPropEntry hashes immediately, so
		// nothing here outlives the iteration step. That is what lets the
		// property index hand out its own bytes instead of a copy per entry.
		nodePropsCountPos = int64(iw.at())
		iw.u64(0)
		for e := range payload.NodeProps {
			writePropEntry(iw, uint64(e.ID), e.Key, e.Value)
			if roots != nil {
				roots.addPropEntry(uint64(e.ID), e.Key, e.Value)
			}
			nodePropCount++
		}
		edgePropsCountPos = int64(iw.at())
		iw.u64(0)
		for e := range payload.EdgeProps {
			writePropEntry(iw, uint64(e.ID), e.Key, e.Value)
			if roots != nil {
				roots.addPropEntry(uint64(e.ID), e.Key, e.Value)
			}
			edgePropCount++
		}
		sections = append(sections, csrSection{
			// Optional: a reader that skips it answers every query correctly and
			// only pays for re-registering entries from the WAL.
			Magic:  csrSectionPropIndex,
			Offset: indexOffset,
			Length: iw.at() - indexOffset,
		})
	}

	if len(payload.OrderedNodeKeys) > 0 || len(payload.OrderedEdgeKeys) > 0 {
		ordOffset := iw.at()
		iw.bytes(appendOrderedKeySection(nil, payload.OrderedNodeKeys, payload.OrderedEdgeKeys))
		sections = append(sections, csrSection{
			// Optional for the same reason: losing it costs a scan, not an answer.
			Magic:  csrSectionOrderedKeys,
			Offset: ordOffset,
			Length: iw.at() - ordOffset,
		})
	}

	if len(payload.CompositeNodeKeys) > 0 || len(payload.CompositeEdgeKeys) > 0 {
		cmpOffset := iw.at()
		iw.bytes(appendCompositeSection(nil, payload.CompositeNodeKeys, payload.CompositeEdgeKeys))
		sections = append(sections, csrSection{
			// Optional, like GORD: a reader that skips it answers every query
			// correctly and only pays the intersection the composite avoids.
			Magic:  csrSectionComposite,
			Offset: cmpOffset,
			Length: iw.at() - cmpOffset,
		})
	}

	// The composite postings, after the declarations they are postings for. The
	// order is not required by either reader -- both are found through the
	// section directory -- and it is what a person reading a hexdump would
	// expect, which is the only argument for it.
	if src := payload.Composites; src != nil {
		cpxOffset := iw.at()
		if _, err := writeGCPX(iw, cpxOffset, *src); err != nil {
			return err
		}
		sections = append(sections, csrSection{
			// OPTIONAL, unlike GPIX, and the difference is a fallback that really
			// exists: a reader that skips this fills every declared composite
			// from the property entries in the same image and answers every query
			// correctly, paying an open-time pass and no answer. GPIX has nothing
			// to fall back to.
			Magic:  csrSectionCompositeIndex,
			Offset: cpxOffset,
			Length: iw.at() - cpxOffset,
		})
	}

	// Tombstones go down before the roots, because the roots commit to them.
	if payload.WithSnapshotRoots && len(payload.Tombstones) > 0 {
		g.tombstones = payload.Tombstones
		tsOffset := iw.at()
		iw.bytes(appendTombstoneSection(nil, payload.Tombstones))
		sections = append(sections, csrSection{
			// CRITICAL. A build that cannot read tombstones would present a
			// redacted entity as one that never existed, which is the single
			// confusion this section exists to prevent — so it must refuse the
			// file rather than answer wrongly.
			Magic:  csrSectionTombstones,
			Flags:  csrSectionCritial,
			Offset: tsOffset,
			Length: iw.at() - tsOffset,
		})
	}

	if payload.WithSnapshotRoots {
		g.roots = roots.finish(payload.Tombstones, payload.PrevSnapshotRoot)
		rootOffset := iw.at()
		iw.bytes(appendSnapshotSection(nil, g.roots))
		sections = append(sections, csrSection{
			// CRITICAL. This section is the file's integrity evidence, so a build
			// that cannot interpret it must refuse rather than open the file and
			// leave an operator believing verification is available.
			Magic:  csrSectionEntityHash,
			Flags:  csrSectionCritial,
			Offset: rootOffset,
			Length: iw.at() - rootOffset,
		})
	}

	// The attestation signs the snapshot root, so it can only be built after the
	// roots exist.
	if payload.WithSnapshotRoots && payload.Signer != nil {
		att, err := signAttestation(payload.Signer, payload.AttestActorID,
			payload.AttestUnixNano, g.roots.Snapshot, payload.PrevAttestation)
		if err != nil {
			return err
		}
		g.attestation = att
		attOffset := iw.at()
		iw.bytes(appendAttestationSection(nil, att))
		sections = append(sections, csrSection{
			// CRITICAL, like GHSH: a build that cannot check an attestation must
			// not open the file and leave an operator believing it was checked.
			Magic:  csrSectionAttestation,
			Flags:  csrSectionCritial,
			Offset: attOffset,
			Length: iw.at() - attOffset,
		})
	}

	sectionTableOffset := iw.at()
	iw.bytes(appendSectionDirectory(nil, sections))

	if err := iw.flush(); err != nil {
		return fmt.Errorf("serialise: %w", err)
	}

	// Every field written above as zeros, patched now that its value is known.
	patchU64 := func(off int64, v uint64) error {
		return patchAt(dst, off, binary.LittleEndian.AppendUint64(nil, v))
	}
	if nodePropsCountPos >= 0 {
		if err := patchU64(nodePropsCountPos, nodePropCount); err != nil {
			return err
		}
		if err := patchU64(edgePropsCountPos, edgePropCount); err != nil {
			return err
		}
	}
	if err := patchU64(sectionTableOffsetPos, sectionTableOffset); err != nil {
		return err
	}

	// Digest last: it covers the finished image with its own field zeroed, so
	// everything above — header, records, sections, directory — is inside it.
	digest, err := digestOf(dst, buf)
	if err != nil {
		return err
	}
	return patchAt(dst, digestPos, digest[:])
}

// patchAt overwrites len(p) bytes at off.
func patchAt(dst io.WriteSeeker, off int64, p []byte) error {
	if _, err := dst.Seek(off, io.SeekStart); err != nil {
		return fmt.Errorf("serialise: seek to %d: %w", off, err)
	}
	if _, err := dst.Write(p); err != nil {
		return fmt.Errorf("serialise: patch at %d: %w", off, err)
	}
	return nil
}

// digestOf reads the finished image back and computes the digest csr_v8.go
// defines, in memory bounded by one copy buffer.
//
// It hashes csrDigestHeader's view of the header rather than its own, so this
// path and every verification path are the same check reached from different
// sides — a file written here and verified by csrDigestStatusOf agree by
// construction rather than by two encoders happening to match.
func digestOf(src io.ReadSeeker, buf []byte) ([csrDigestSize]byte, error) {
	var out [csrDigestSize]byte
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return out, fmt.Errorf("serialise: seek for digest: %w", err)
	}

	var header [csrV8HeaderSize]byte
	if _, err := io.ReadFull(src, header[:]); err != nil {
		return out, fmt.Errorf("serialise: read header for digest: %w", err)
	}
	hdr := csrDigestHeader(header[:])

	h := sha256.New()
	h.Write(hdr[:])

	// A hand-rolled loop rather than io.CopyBuffer, which hands off to an
	// *os.File's own WriteTo and allocates a buffer of its own on the way past
	// the one it was given. This one uses the buffer it was given.
	rest := buf[:cap(buf)]
	for {
		n, err := src.Read(rest)
		if n > 0 {
			h.Write(rest[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, fmt.Errorf("serialise: digest pass: %w", err)
		}
	}
	copy(out[:], h.Sum(nil))
	return out, nil
}

// memImage is an in-memory io.ReadWriteSeeker, so SerialiseWithPayload can use
// the streaming writer rather than being a second implementation of it.
//
// It is deliberately minimal: the only access pattern is append to the end, then
// seek back twice to patch, then read the whole thing once. Anything else is a
// caller bug rather than a case to handle.
type memImage struct {
	buf []byte
	pos int64
}

func (m *memImage) Write(p []byte) (int, error) {
	end := m.pos + int64(len(p))
	switch {
	case end <= int64(len(m.buf)):
		// A patch, landing inside what is already there.
	case end <= int64(cap(m.buf)):
		m.buf = m.buf[:end]
	default:
		// Grown explicitly rather than by appending a zero slice, which would
		// allocate the filler as well as the buffer — on an image-sized write
		// that is a second copy of the image in garbage.
		grown := make([]byte, end, max(2*int64(cap(m.buf)), end))
		copy(grown, m.buf)
		m.buf = grown
	}
	copy(m.buf[m.pos:end], p)
	m.pos = end
	return len(p), nil
}

func (m *memImage) Read(p []byte) (int, error) {
	if m.pos >= int64(len(m.buf)) {
		return 0, io.EOF
	}
	n := copy(p, m.buf[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *memImage) Seek(off int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = off
	case io.SeekCurrent:
		abs = m.pos + off
	case io.SeekEnd:
		abs = int64(len(m.buf)) + off
	default:
		return 0, fmt.Errorf("memImage: bad whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("memImage: negative position %d", abs)
	}
	m.pos = abs
	return abs, nil
}

// writePropEntry writes one property-index entry:
// id:8 + keyLen:2 + key + valLen:4 + value.
func writePropEntry(iw *imageWriter, id uint64, key string, value []byte) {
	iw.u64(id)
	iw.u16(uint16(len(key)))
	iw.str(key)
	iw.u32(uint32(len(value)))
	iw.bytes(value)
}
