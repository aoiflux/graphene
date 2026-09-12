package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// The image with the property index in it rather than beside it.
//
// Everything here works one level above csr_gpix_test.go: those tests judge the
// two sections on their own, and these judge the image that carries them — the
// version it declares, the sections it lists, and the roots it commits to. What a
// *store* does with such an image is index_mapped_test.go.

// v9Fixture returns a graph, a v8 payload carrying its entries as GIDX, and a v9
// payload carrying the same entries as GPIX and GPIR.
//
// Two payloads over one graph and one entry set is the whole point: every
// assertion below that compares the encodings is only meaningful if the content
// is identical, and the canonical order is what makes "identical" well defined.
// The entries are put through the fixture and read back out of it, so both arms
// see them in the order the index yields — (key, value, id) — rather than in the
// order they were written here.
func v9Fixture(t *testing.T, n int) (*CSRGraph, csrPayload, csrPayload) {
	t.Helper()
	g, base := streamFixture(t, n)

	f := newGPIXFixture()
	for _, e := range collectNodeProps(base) {
		f.add(gpixKindNode, e.Key, string(e.Value), uint64(e.ID))
	}
	for _, e := range collectEdgeProps(base) {
		f.add(gpixKindEdge, e.Key, string(e.Value), uint64(e.ID))
	}
	nodes, edges := canonicalEntries(f)

	v8 := base
	v8.NodeProps = slices.Values(nodes)
	v8.EdgeProps = slices.Values(edges)

	src := f.source(t.TempDir(), 0, 0)
	v9 := base
	v9.NodeProps, v9.EdgeProps = nil, nil
	v9.MappedIndex = &src
	return g, v8, v9
}

func collectNodeProps(p csrPayload) []index.NodePropEntry {
	var out []index.NodePropEntry
	for e := range p.NodeProps {
		out = append(out, e)
	}
	return out
}

func collectEdgeProps(p csrPayload) []index.EdgePropEntry {
	var out []index.EdgePropEntry
	for e := range p.EdgeProps {
		out = append(out, e)
	}
	return out
}

// canonicalEntries walks a fixture in the order both encoders write it.
func canonicalEntries(f *gpixFixture) ([]index.NodePropEntry, []index.EdgePropEntry) {
	var nodes []index.NodePropEntry
	nodeWalk := f.walker(gpixKindNode)
	for _, key := range f.keys(gpixKindNode) {
		k := key
		_ = nodeWalk(k, func(value []byte, ids []uint64) bool {
			for _, id := range ids {
				nodes = append(nodes, index.NodePropEntry{
					ID: store.NodeID(id), Key: k, Value: bytes.Clone(value)})
			}
			return true
		})
	}
	var edges []index.EdgePropEntry
	edgeWalk := f.walker(gpixKindEdge)
	for _, key := range f.keys(gpixKindEdge) {
		k := key
		_ = edgeWalk(k, func(value []byte, ids []uint64) bool {
			for _, id := range ids {
				edges = append(edges, index.EdgePropEntry{
					ID: store.EdgeID(id), Key: k, Value: bytes.Clone(value)})
			}
			return true
		})
	}
	return nodes, edges
}

// sectionsOf reads an image's directory without parsing the graph, so a test can
// judge what the writer put in the file without depending on the loader agreeing.
func sectionsOf(t *testing.T, data []byte) []csrSection {
	t.Helper()
	sections, err := readCSRSectionDirectory(data, csrSectionTableOffsetOf(data))
	if err != nil {
		t.Fatalf("section directory: %v", err)
	}
	return sections
}

func csrSectionTableOffsetOf(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[62:70])
}

// TestSerialiseTo_MappedIndexIsV9WithNoGIDX checks what the payload's choice
// actually changes in the container.
//
// Not writing GIDX is the point and not an omission: it is the same entries in a
// second encoding, ~528 MiB at the shape docs/MEMORY_MODEL.md measures, that no
// reader of either kind would read. Both new sections are critical, because a
// reader that skipped them would answer every property query with no matches
// rather than slowly.
func TestSerialiseTo_MappedIndexIsV9WithNoGIDX(t *testing.T) {
	g, _, v9 := v9Fixture(t, 40)
	data, err := g.SerialiseWithPayload(v9)
	if err != nil {
		t.Fatalf("SerialiseWithPayload: %v", err)
	}
	if v := binary.LittleEndian.Uint16(data[4:6]); v != csrVersionMappedIndex {
		t.Fatalf("version = %d, want %d", v, csrVersionMappedIndex)
	}
	sections := sectionsOf(t, data)
	if _, ok := findSection(sections, csrSectionPropIndex); ok {
		t.Error("a v9 image carries GIDX as well, which is the second copy this format removed")
	}
	for _, magic := range []string{csrSectionMappedIndex, csrSectionMappedReverse} {
		s, ok := findSection(sections, magic)
		if !ok {
			t.Fatalf("%s is missing", magic)
		}
		if !s.Critical() {
			t.Errorf("%s is not marked critical", magic)
		}
		if s.Length == 0 || s.Offset+s.Length > uint64(len(data)) {
			t.Errorf("%s at %d+%d is outside a %d-byte image", magic, s.Offset, s.Length, len(data))
		}
	}
	// The digest covers the new sections like every other byte of the image.
	if status, _ := csrDigestStatus(data); status != DigestMatch {
		t.Fatalf("digest status = %v, want DigestMatch", status)
	}
}

// TestSerialiseTo_MappedIndexKeepsTheSnapshotRoots is the compatibility claim
// that matters most, and the one nothing else would catch.
//
// An image's snapshot root is its identity: a custody chain, an attestation and
// every operator-held expected value are stated in terms of it. The property
// index changing encoding must not change it, because the entries are the same
// entries — and the only reason it does not is that GPIX's walk yields them in
// the order GIDX wrote them. That coincidence is load-bearing, so it is asserted
// rather than assumed.
func TestSerialiseTo_MappedIndexKeepsTheSnapshotRoots(t *testing.T) {
	g, v8, v9 := v9Fixture(t, 40)

	if _, err := g.SerialiseWithPayload(v8); err != nil {
		t.Fatalf("v8: %v", err)
	}
	was, ok := g.Roots()
	if !ok {
		t.Fatal("the v8 image produced no roots")
	}
	if _, err := g.SerialiseWithPayload(v9); err != nil {
		t.Fatalf("v9: %v", err)
	}
	now, ok := g.Roots()
	if !ok {
		t.Fatal("the v9 image produced no roots")
	}
	if was.IndexRoot != now.IndexRoot {
		t.Error("IndexRoot changed with the encoding: the two sections do not carry the same " +
			"entries in the same order")
	}
	if was.NodeRoot != now.NodeRoot || was.EdgeRoot != now.EdgeRoot {
		t.Error("a record root changed, which the property index cannot affect")
	}
	if was.Snapshot != now.Snapshot {
		t.Error("the snapshot root changed, so every retained expected value would have to be reissued")
	}
}

// TestSerialiseTo_MappedIndexIsByteDeterministic asserts the format's own
// contract over the new sections, on both sides of the spill thresholds.
//
// TestCompact_IsByteDeterministic states the rule for the image as a whole; this
// is the same rule where it is newly at risk, because the entries now travel
// through an external sort and two intermediate files and a sort that was not
// total or a merge that was not deterministic would produce a different image
// from the same store.
func TestSerialiseTo_MappedIndexIsByteDeterministic(t *testing.T) {
	for _, tc := range []struct {
		name            string
		vtabCap, revLen int
	}{
		{"intermediates in memory", 1 << 20, 1 << 20},
		{"intermediates spilled", 16, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			build := func() []byte {
				g, _, v9 := v9Fixture(t, 40)
				v9.MappedIndex.vtabMemCap = tc.vtabCap
				v9.MappedIndex.revChunk = tc.revLen
				data, err := g.SerialiseWithPayload(v9)
				if err != nil {
					t.Fatalf("serialise: %v", err)
				}
				return data
			}
			a, b := build(), build()
			if !bytes.Equal(a, b) {
				t.Fatalf("two images of one store differ: %d and %d bytes", len(a), len(b))
			}
		})
	}
}

// TestSerialiseTo_MappedIndexSectionsAnswerQueries parses the two sections back
// out of a finished image and asks the reader the store will use.
//
// The sections are checked on their own elsewhere; what this adds is that they
// survive being placed in an image — the offsets in a directory entry are
// relative to the file and the offsets inside a section are relative to the
// section, and getting that wrong produces a section that parses and answers
// nothing.
func TestSerialiseTo_MappedIndexSectionsAnswerQueries(t *testing.T) {
	g, v8, v9 := v9Fixture(t, 40)
	data, err := g.SerialiseWithPayload(v9)
	if err != nil {
		t.Fatalf("serialise: %v", err)
	}
	sections := sectionsOf(t, data)
	fwdSec, _ := findSection(sections, csrSectionMappedIndex)
	revSec, _ := findSection(sections, csrSectionMappedReverse)
	fwd, err := parseGPIX(data[fwdSec.Offset : fwdSec.Offset+fwdSec.Length])
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	rev, err := parseGPIR(data[revSec.Offset : revSec.Offset+revSec.Length])
	if err != nil {
		t.Fatalf("parseGPIR: %v", err)
	}
	base, err := newGPIXBase(fwd, rev)
	if err != nil {
		t.Fatalf("newGPIXBase: %v", err)
	}

	// Every entry the v8 arm carries must be answerable from both directions.
	nodes := collectNodeProps(v8)
	if got := base.TotalEntries(index.NodeKind); got != len(nodes) {
		t.Fatalf("TotalEntries = %d, want %d", got, len(nodes))
	}
	for _, e := range nodes {
		run, err := base.Lookup(index.NodeKind, e.Key, e.Value)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", e.Key, err)
		}
		if !slices.Contains(runIDs(run), uint64(e.ID)) {
			t.Fatalf("Lookup(%q, %q) does not hold node %d", e.Key, e.Value, e.ID)
		}
		found := false
		if err := base.ForEachEntryOf(index.NodeKind, uint64(e.ID),
			func(key string, value []byte) bool {
				if key == e.Key && bytes.Equal(value, e.Value) {
					found = true
					return false
				}
				return true
			}); err != nil {
			t.Fatalf("ForEachEntryOf(%d): %v", e.ID, err)
		}
		if !found {
			t.Fatalf("node %d is not reported as indexed under %q=%q", e.ID, e.Key, e.Value)
		}
	}
}

// TestSerialiseTo_MappedIndexLeavesNoSpillBehind checks that the two
// intermediates are gone when the image is.
//
// A compaction fails most often because the disk is full, and a spill of the
// index is the same order of size as the image. Leaving one behind turns a
// recoverable failure into a machine with no room to retry on, which is the
// reason build removes its own temp file.
func TestSerialiseTo_MappedIndexLeavesNoSpillBehind(t *testing.T) {
	g, _, v9 := v9Fixture(t, 40)
	scratch := t.TempDir()
	v9.MappedIndex.ScratchDir = scratch
	v9.MappedIndex.vtabMemCap = 16
	v9.MappedIndex.revChunk = 4
	if _, err := g.SerialiseWithPayload(v9); err != nil {
		t.Fatalf("serialise: %v", err)
	}
	if names := ls(t, scratch); len(names) != 0 {
		t.Fatalf("serialisation left %v in the scratch directory", names)
	}

	// And on the failing path, which is the one that matters.
	iw := &shortWriter{limit: 4096}
	g2, _, v9b := v9Fixture(t, 40)
	v9b.MappedIndex.ScratchDir = scratch
	v9b.MappedIndex.vtabMemCap = 16
	v9b.MappedIndex.revChunk = 4
	if err := g2.SerialiseTo(&writeOnlySeeker{w: iw}, v9b); err == nil {
		t.Fatal("serialising into a writer that runs out reported success")
	}
	if names := ls(t, scratch); len(names) != 0 {
		t.Fatalf("a failed serialisation left %v in the scratch directory", names)
	}
}

// writeOnlySeeker adapts a plain writer to the ReadWriteSeeker SerialiseTo takes.
// Seeking and reading fail, which is fine: the write fails first.
type writeOnlySeeker struct {
	w interface{ Write([]byte) (int, error) }
}

func (s *writeOnlySeeker) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *writeOnlySeeker) Read([]byte) (int, error) {
	return 0, fmt.Errorf("not readable")
}
func (s *writeOnlySeeker) Seek(int64, int) (int64, error) { return 0, nil }

// TestImageWriter_FromRefusesAShortSource guards the one check that stands between
// a spill that lost bytes and an image whose section directory lies.
//
// Nothing in the encoder can produce a short source today: every region is copied
// out of a spill under its own recorded length. The guard is there because the
// failure it catches is silent — a section shorter than its directory entry parses
// as whatever follows it — so it is asserted directly rather than left to the
// arrangement that currently makes it unreachable.
func TestImageWriter_FromRefusesAShortSource(t *testing.T) {
	iw := newImageWriter(io.Discard, make([]byte, 0, 64))
	iw.from(bytes.NewReader([]byte("abc")), 10)
	if err := iw.flush(); err == nil {
		t.Fatal("copying three bytes where ten were promised was accepted")
	}
}
