package disk

// CSR format v8: a sectioned container with a body digest.
//
// # Why the format changed
//
// v7 has a fixed 46-byte header and exactly one optional trailer, the GIDX
// property index, addressed by a single offset. Every capability that needs to
// persist something new therefore needs a version bump, and by the time this was
// written seven separate items were queued behind one: a body digest, entity
// version hashes, an attestation section, ordered-key declarations, composite
// index declarations, the commit-sequence high-water mark, and the last
// compaction time. Shipping them as v8, v9, v10 … would mean a migration each
// time.
//
// v8 is therefore a *container*, not a bigger header. It carries a section
// directory, so a future capability adds a section and does not touch the
// version at all.
//
// # Layout
//
//	offset  size  field
//	0       4     magic "GCSR"
//	4       2     version (8)
//	6       8     nodeCount
//	14      8     edgeCount
//	22      8     nodeSeqHW
//	30      8     edgeSeqHW
//	38      8     indexOffset      (kept at its v6 position; 0 in v8, see below)
//	46      8     commitSeqHW
//	54      8     lastCompactUnixNano
//	62      8     sectionTableOffset
//	70      32    digest           SHA-256, computed with these 32 bytes zeroed
//	102           end of header
//	...           node records, then edge records (unchanged from v7)
//	...           sections, in directory order
//	...           section directory at sectionTableOffset
//
// The first 46 bytes are byte-identical in meaning to v6/v7. That is deliberate:
// a reader can identify the file and its counts before it knows anything about
// sections, which is what makes cmd/graphene able to report a partially
// corrupt file rather than refusing to say anything about it.
//
// indexOffset is written as 0 in v8. The property index has become a section
// like any other, and leaving the field in place rather than reusing those eight
// bytes keeps the header's prefix stable.
//
// # Unknown sections, and why they are not simply skipped
//
// Each directory entry carries flags, and bit 0 marks the section CRITICAL. A
// reader that meets an unknown critical section must refuse the file; an unknown
// optional section is skipped.
//
// This is the same rule knownWALRecord applies to unknown record types, for the
// same reason. An attestation or signature section will be critical: a build
// that cannot check it must not present the file as though it had. A property
// index or a set of ordered-key declarations is optional — losing it costs
// performance, not correctness, and a reader that ignores one still answers
// every query correctly.
//
// Getting this bit wrong in either direction is expensive, so the default for a
// new section should be CRITICAL unless it is clearly derivable from the records.

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	// csrVersionSectioned is v8: section directory plus body digest.
	csrVersionSectioned = 8

	csrV8HeaderSize = 102
	// csrLastCompactOffset is excluded from the digest — see computeCSRDigest.
	csrLastCompactOffset = 54
	csrDigestOffset      = 70
	csrDigestSize        = 32
	csrSectionEntry      = 24 // magic4 + flags4 + offset8 + length8
	csrSectionCritial    = uint32(1 << 0)
)

// Section magics. Only GIDX and GORD are written today; the rest are registered
// here so that two capabilities cannot pick the same four bytes, and so the
// intended criticality of each is decided once rather than at implementation
// time.
const (
	csrSectionPropIndex   = "GIDX" // property index — optional, derivable-ish
	csrSectionOrderedKeys = "GORD" // ordered-key declarations — optional
	csrSectionEntityHash  = "GHSH" // per-entity version hashes — CRITICAL when written
	csrSectionAttestation = "GATT" // signatures and attestations — CRITICAL when written
	csrSectionComposite   = "GCMP" // composite index declarations — optional
	csrSectionTombstones  = "GRDT" // records of deliberate removal — CRITICAL when written

	// The disk-resident property index and its reverse direction, written only
	// by a v9 writer. CRITICAL, unlike GIDX: a v9 image carries no GIDX to fall
	// back to, so a build that skipped these would answer every property query
	// with no matches rather than slowly. See csr_gpix.go.
	//
	// Registered here ahead of the loader that reads them, which is what this
	// block is for — two capabilities must not pick the same four bytes. They are
	// deliberately not yet in checkCriticalSections; see the note there.
	csrSectionMappedIndex   = "GPIX"
	csrSectionMappedReverse = "GPIR"
)

// csrSection is one entry in the directory.
type csrSection struct {
	Magic  string
	Flags  uint32
	Offset uint64
	Length uint64
}

// Critical reports whether a reader must understand this section to use the file.
func (s csrSection) Critical() bool { return s.Flags&csrSectionCritial != 0 }

// csrV8Trailer is everything v8 carries beyond the v6 header and the records.
type csrV8Trailer struct {
	CommitSeqHW         uint64
	LastCompactUnixNano int64
	Sections            []csrSection
	Digest              [csrDigestSize]byte
}

// computeCSRDigest hashes the image over everything that is a function of its
// contents, which is the header, the records, and every section — but NOT the
// digest field itself, and NOT the compaction timestamp.
//
// # Why the timestamp is excluded
//
// The digest exists to be a stable identity for what the file holds. Two parties
// holding the same evidence have to be able to compute the same value, and a
// store that is recompacted without changing must keep the value it had.
// Hashing a wall-clock reading defeats both: two compactions of identical
// content would disagree, which is exactly the failure canonical serialisation
// was introduced to remove.
//
// So the rule is that the digest covers content, and when the file was written
// is not content. Excluding it costs nothing that was previously guaranteed:
// the timestamp is a local clock reading, asserted rather than proven, and
// anything depending on it needs an external time authority regardless.
//
// Every other v8 header field is content-determined — the counts, the sequence
// high-water marks, the commit high-water mark — so all of them are covered.
//
// # What a matching digest proves
//
// That the file has not changed since it was written. Not who wrote it: this is
// a cryptographic hash rather than a CRC, which raises the bar from "recompute
// it in one line" to "hold a preimage", but anyone who can rewrite the body can
// still rewrite the digest. It detects damage on its own, and detects tampering
// only for a verifier holding an independently retained copy of the expected
// value. Signing is what closes that gap.
func computeCSRDigest(data []byte) [csrDigestSize]byte {
	if len(data) < csrV8HeaderSize {
		return sha256.Sum256(data)
	}
	hdr := csrDigestHeader(data)
	h := sha256.New()
	h.Write(hdr[:])
	h.Write(data[csrV8HeaderSize:])
	var out [csrDigestSize]byte
	copy(out[:], h.Sum(nil))
	return out
}

// csrDigestHeader returns the header as the digest covers it: the two fields
// that are not content zeroed, everything else as written.
//
// Both the whole-buffer path and the streaming one hash this, rather than each
// zeroing the header its own way. The digest is a stable identity for the file's
// contents, so the two must agree byte for byte forever; a second copy of "which
// bytes are excluded" is a place for them to stop agreeing years from now, in a
// change that looks unrelated to either.
func csrDigestHeader(data []byte) [csrV8HeaderSize]byte {
	var out [csrV8HeaderSize]byte
	copy(out[:], data[:csrV8HeaderSize])
	clear(out[csrLastCompactOffset : csrLastCompactOffset+8]) // when it was written is not content
	clear(out[csrDigestOffset : csrDigestOffset+csrDigestSize])
	return out
}

// DigestStatus is the result of checking a CSR image's stored digest.
type DigestStatus int

const (
	// DigestAbsent means the file predates v8 and carries no digest. It is not a
	// failure — it is the honest answer for a file that was never covered.
	DigestAbsent DigestStatus = iota
	// DigestMatch means the stored digest describes the bytes on disk.
	DigestMatch
	// DigestMismatch means it does not. The file has changed since it was
	// written, by damage or by edit — the digest cannot tell which.
	DigestMismatch
)

func (d DigestStatus) String() string {
	switch d {
	case DigestMatch:
		return "match"
	case DigestMismatch:
		return "MISMATCH"
	default:
		return "absent (pre-v8 file)"
	}
}

// VerifyCSRDigest checks the digest stored in a CSR image against its contents,
// and returns both the status and the digest computed from the bytes.
//
// Not run by a default Open. Hashing the whole image costs time proportional to
// the file on every startup, and Open already declines to run VerifyIndexes for
// the same reason — a check that makes every open slower gets disabled, and a
// disabled check protects nothing. It is offered here, through
// `graphene csr -verify`, and through Options.VerifyOnOpen, so that verifying is
// a decision rather than a tax.
//
// What a match does and does not mean is worth stating plainly: it proves the
// file has not changed since it was written. It does not prove who wrote it.
// Anyone who can rewrite the body can rewrite the digest, so this detects
// damage and accident, and detects tampering only for someone holding an
// independently retained copy of the expected digest.
//
// path may be the store directory or the file.
func VerifyCSRDigest(path string) (DigestStatus, [csrDigestSize]byte, error) {
	p, err := resolveFile(path, csrFileName)
	if err != nil {
		return DigestAbsent, [csrDigestSize]byte{}, err
	}
	f, err := os.Open(p)
	if err != nil {
		return DigestAbsent, [csrDigestSize]byte{}, fmt.Errorf("verify csr digest: %w", err)
	}
	defer f.Close()
	return csrDigestStatusOf(f)
}

// csrDigestStatusOf is csrDigestStatus over a stream: the same digest and the
// same three answers, in memory bounded by the header and one copy buffer.
//
// The file is hashed as it is read rather than read whole first. An image is the
// size of the graph, and the caller that matters is the final check of every
// backup: it was the only whole-image read a backup made after the copy, on an
// operation an operator reaches for because a machine is already in trouble,
// which is the worst moment to ask for the store's footprint a second time. The
// CLI verification commands share this leg, but their roots check still parses
// the image whole, so they are bounded here and not yet overall.
//
// Reading in pieces is safe because the engine never rewrites an image in place:
// a compaction writes a temporary file and renames it over the old one, so the
// bytes behind an open handle do not change under it. See invariant §15.13.
//
// It has to agree with csrDigestStatus exactly. The two are one check reached by
// different callers, and a file whose digest verified through one and failed
// through the other would be unexplainable to whoever had to act on it —
// TestCSRDigest_StreamedMatchesWholeBuffer asserts the agreement over every
// shape the pair can tell apart.
func csrDigestStatusOf(r io.Reader) (DigestStatus, [csrDigestSize]byte, error) {
	var (
		none   [csrDigestSize]byte
		header [csrV8HeaderSize]byte
	)
	switch _, err := io.ReadFull(r, header[:]); {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// Shorter than a v8 header. Not an error: it is the same DigestAbsent the
		// whole-buffer check gives a file it does not recognise, and a pre-v8
		// image is a file that was never covered rather than a broken one.
		return DigestAbsent, none, nil
	case err != nil:
		return DigestAbsent, none, fmt.Errorf("verify csr digest: %w", err)
	}
	if binary.LittleEndian.Uint16(header[4:6]) < csrVersionSectioned {
		return DigestAbsent, none, nil
	}

	stored, _ := readCSRDigest(header[:])
	hdr := csrDigestHeader(header[:])
	h := sha256.New()
	h.Write(hdr[:])
	if _, err := io.Copy(h, r); err != nil {
		return DigestAbsent, none, fmt.Errorf("verify csr digest: %w", err)
	}

	var computed [csrDigestSize]byte
	copy(computed[:], h.Sum(nil))
	if stored == computed {
		return DigestMatch, computed, nil
	}
	return DigestMismatch, computed, nil
}

// csrDigestStatus is VerifyCSRDigest against bytes already in hand.
//
// Split out so a caller holding the image — Open under VerifyOnOpen — can ask
// the question without reading the file a second time. It cannot fail: every
// shape it does not understand is DigestAbsent, which is the honest answer for
// a file that was never covered rather than an error.
func csrDigestStatus(data []byte) (DigestStatus, [csrDigestSize]byte) {
	if len(data) < csrV8HeaderSize {
		return DigestAbsent, [csrDigestSize]byte{}
	}
	if binary.LittleEndian.Uint16(data[4:6]) < csrVersionSectioned {
		return DigestAbsent, [csrDigestSize]byte{}
	}

	stored, ok := readCSRDigest(data)
	if !ok {
		return DigestAbsent, [csrDigestSize]byte{}
	}
	computed := computeCSRDigest(data)
	if stored == computed {
		return DigestMatch, computed
	}
	return DigestMismatch, computed
}

// readCSRDigest returns the digest stored in a v8 image.
func readCSRDigest(data []byte) ([csrDigestSize]byte, bool) {
	var out [csrDigestSize]byte
	if len(data) < csrV8HeaderSize {
		return out, false
	}
	copy(out[:], data[csrDigestOffset:csrDigestOffset+csrDigestSize])
	return out, true
}

// readCSRSectionDirectory parses the directory at the given offset.
func readCSRSectionDirectory(data []byte, offset uint64) ([]csrSection, error) {
	if offset == 0 {
		return nil, nil
	}
	if offset > uint64(len(data)) || uint64(len(data))-offset < 2 {
		return nil, fmt.Errorf("section directory offset %d is outside a %d-byte file", offset, len(data))
	}
	pos := int(offset)
	count := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// A directory cannot describe more sections than the remaining bytes can
	// hold — the same bound the record counts get, for the same reason.
	if count > (len(data)-pos)/csrSectionEntry {
		return nil, fmt.Errorf("section directory declares %d sections, more than %d remaining bytes can hold",
			count, len(data)-pos)
	}

	out := make([]csrSection, 0, count)
	for i := 0; i < count; i++ {
		if pos+csrSectionEntry > len(data) {
			return nil, fmt.Errorf("truncated section directory entry %d", i)
		}
		s := csrSection{
			Magic:  string(data[pos : pos+4]),
			Flags:  binary.LittleEndian.Uint32(data[pos+4 : pos+8]),
			Offset: binary.LittleEndian.Uint64(data[pos+8 : pos+16]),
			Length: binary.LittleEndian.Uint64(data[pos+16 : pos+24]),
		}
		pos += csrSectionEntry

		if s.Offset > uint64(len(data)) || s.Length > uint64(len(data))-s.Offset {
			return nil, fmt.Errorf("section %q spans bytes %d..%d, outside a %d-byte file",
				s.Magic, s.Offset, s.Offset+s.Length, len(data))
		}
		out = append(out, s)
	}
	return out, nil
}

// checkCriticalSections refuses a file carrying a critical section this build
// does not understand.
func checkCriticalSections(sections []csrSection) error {
	for _, s := range sections {
		if !s.Critical() {
			continue
		}
		switch s.Magic {
		case csrSectionPropIndex, csrSectionOrderedKeys, csrSectionEntityHash, csrSectionAttestation,
			csrSectionTombstones, csrSectionComposite:
			// Understood.
			//
			// GPIX and GPIR are deliberately absent until the loader reads them.
			// The magics are registered in this file, but registering a magic is
			// not understanding a section: a build that listed them here would
			// accept an image whose index it then ignored, which is the wrong
			// answer their critical flag exists to refuse. They join this list in
			// the change that teaches the loader to read them.
		default:
			return fmt.Errorf("deserialiseCSR: file carries critical section %q, which this build does not understand — "+
				"it was written by a newer version and must not be read as though the section were absent", s.Magic)
		}
	}
	return nil
}

// --- GORD: ordered-key declarations ---
//
// Declaring a key ordered is what turns a range query from a scan into a binary
// search, and until v8 the declaration lived only in memory. Every restart
// silently reverted every range query to a scan: no error, no warning, and
// OrderedNodeProperties honestly reporting an empty list. A performance cliff
// with no symptom is worse than a loud failure, and this is the section that
// removes it.
//
// The entries themselves are not stored — they are already in GIDX, and a
// declaration is rebuilt from them at load. Only the fact of the declaration
// needs persisting.
//
//	[nodeKeyCount:4][keyLen:2, key]…[edgeKeyCount:4][keyLen:2, key]…
//
// Optional, not critical: a reader that skips it answers every query correctly
// and only pays the scan.

// minOrderedKeyEntry is the smallest a declaration can be: a length prefix and
// an empty key. Used to bound a count against the bytes that remain.
const minOrderedKeyEntry = 2

func appendOrderedKeySection(buf []byte, nodeKeys, edgeKeys []string) []byte {
	write := func(dst []byte, keys []string) []byte {
		var n [4]byte
		binary.LittleEndian.PutUint32(n[:], uint32(len(keys)))
		dst = append(dst, n[:]...)
		for _, k := range keys {
			var l [2]byte
			binary.LittleEndian.PutUint16(l[:], uint16(len(k)))
			dst = append(dst, l[:]...)
			dst = append(dst, k...)
		}
		return dst
	}
	buf = write(buf, nodeKeys)
	return write(buf, edgeKeys)
}

// readOrderedKeySection parses a GORD section body.
func readOrderedKeySection(data []byte) (nodeKeys, edgeKeys []string, err error) {
	pos := 0
	read := func(what string) ([]string, error) {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("truncated %s key count", what)
		}
		count := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		// Same bound as every other length prefix read from a file: a count
		// larger than the remaining bytes can encode is invalid by definition.
		if count < 0 || count > (len(data)-pos)/minOrderedKeyEntry {
			return nil, fmt.Errorf("%s key count %d exceeds what %d remaining bytes can hold",
				what, count, len(data)-pos)
		}
		out := make([]string, 0, count)
		for i := 0; i < count; i++ {
			if pos+2 > len(data) {
				return nil, fmt.Errorf("truncated %s key %d", what, i)
			}
			n := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			if pos+n > len(data) {
				return nil, fmt.Errorf("truncated %s key %d body", what, i)
			}
			out = append(out, string(data[pos:pos+n]))
			pos += n
		}
		return out, nil
	}

	if nodeKeys, err = read("ordered node"); err != nil {
		return nil, nil, err
	}
	if edgeKeys, err = read("ordered edge"); err != nil {
		return nil, nil, err
	}
	return nodeKeys, edgeKeys, nil
}

// minCompositeTuple is the smallest a composite declaration can be: a key count
// and nothing else. Used to bound a count against the bytes that remain.
const minCompositeTuple = 2

// appendCompositeSection writes a GCMP body: the declared node key tuples, then
// the edge ones.
//
// Only the declarations travel. The postings and the per-entity member values
// are rebuilt from the property-index entries in the same file, which is the
// same trade GORD makes and for the same reason: the derived structure cannot go
// stale relative to the entries if it is always derived from them, and the bytes
// it would have taken buy nothing that the entries do not already hold.
func appendCompositeSection(buf []byte, nodeTuples, edgeTuples [][]string) []byte {
	write := func(dst []byte, tuples [][]string) []byte {
		var n [4]byte
		binary.LittleEndian.PutUint32(n[:], uint32(len(tuples)))
		dst = append(dst, n[:]...)
		for _, tuple := range tuples {
			var c [2]byte
			binary.LittleEndian.PutUint16(c[:], uint16(len(tuple)))
			dst = append(dst, c[:]...)
			for _, k := range tuple {
				var l [2]byte
				binary.LittleEndian.PutUint16(l[:], uint16(len(k)))
				dst = append(dst, l[:]...)
				dst = append(dst, k...)
			}
		}
		return dst
	}
	buf = write(buf, nodeTuples)
	return write(buf, edgeTuples)
}

// readCompositeSection parses a GCMP section body.
func readCompositeSection(data []byte) (nodeTuples, edgeTuples [][]string, err error) {
	pos := 0
	read := func(what string) ([][]string, error) {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("truncated %s tuple count", what)
		}
		count := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		// Same bound as every other length prefix read from a file: a count
		// larger than the remaining bytes can encode is invalid by definition.
		if count < 0 || count > (len(data)-pos)/minCompositeTuple {
			return nil, fmt.Errorf("%s tuple count %d exceeds what %d remaining bytes can hold",
				what, count, len(data)-pos)
		}
		out := make([][]string, 0, count)
		for i := 0; i < count; i++ {
			if pos+2 > len(data) {
				return nil, fmt.Errorf("truncated %s tuple %d", what, i)
			}
			keys := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			if keys > (len(data)-pos)/minOrderedKeyEntry {
				return nil, fmt.Errorf("%s tuple %d claims %d keys, more than %d remaining bytes can hold",
					what, i, keys, len(data)-pos)
			}
			tuple := make([]string, 0, keys)
			for j := 0; j < keys; j++ {
				if pos+2 > len(data) {
					return nil, fmt.Errorf("truncated %s tuple %d key %d", what, i, j)
				}
				n := int(binary.LittleEndian.Uint16(data[pos:]))
				pos += 2
				if pos+n > len(data) {
					return nil, fmt.Errorf("truncated %s tuple %d key %d body", what, i, j)
				}
				tuple = append(tuple, string(data[pos:pos+n]))
				pos += n
			}
			out = append(out, tuple)
		}
		return out, nil
	}

	if nodeTuples, err = read("composite node"); err != nil {
		return nil, nil, err
	}
	if edgeTuples, err = read("composite edge"); err != nil {
		return nil, nil, err
	}
	return nodeTuples, edgeTuples, nil
}

// findSection returns the named section, if present.
func findSection(sections []csrSection, magic string) (csrSection, bool) {
	for _, s := range sections {
		if s.Magic == magic {
			return s, true
		}
	}
	return csrSection{}, false
}

// appendSectionDirectory writes the directory and returns it.
func appendSectionDirectory(buf []byte, sections []csrSection) []byte {
	var hdr [2]byte
	binary.LittleEndian.PutUint16(hdr[:], uint16(len(sections)))
	buf = append(buf, hdr[:]...)
	for _, s := range sections {
		var e [csrSectionEntry]byte
		copy(e[0:4], s.Magic)
		binary.LittleEndian.PutUint32(e[4:8], s.Flags)
		binary.LittleEndian.PutUint64(e[8:16], s.Offset)
		binary.LittleEndian.PutUint64(e[16:24], s.Length)
		buf = append(buf, e[:]...)
	}
	return buf
}
