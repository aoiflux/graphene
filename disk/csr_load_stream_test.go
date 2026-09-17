package disk

// The premise the streaming loader rests on, held to account.
//
// csr_load_stream.go stops the loader materialising every record before building
// the graph, and pays for it by walking the image's record stream three times
// instead of once. That is only sound if a walk is repeatable -- the same records,
// in the same order, with the same bytes, every time -- because buildSeq places
// on its last pass what it sized the arena for on its first.
//
// It is repeatable by construction: a walk is a pure function of the image's
// bytes and its format version. "By construction" is an argument, and these are
// the tests that make it a fact. The cursor arithmetic is the part that could be
// wrong without being obviously wrong: the arenas are filled in record order and
// every walk re-derives each record's run from a running counter rather than from
// a stored span, so an off-by-one in one record's label count silently shifts
// every record after it -- which reads as a graph whose records all carry
// somebody else's labels, and which no count-based check would catch.

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// loadStreamFixture is a record set whose label and property runs vary per record,
// which is what makes a cursor slip visible.
//
// A fixture where every record carried one label and a blob of one size would
// pass with any consistent off-by-one, because every run would be the same width
// and a shifted cursor would land on an identical run. So the counts cycle: no
// labels, one, two, and blobs of four different lengths including empty.
func loadStreamFixture(t testing.TB, n int) ([]nodeRecord, []rawEdge) {
	t.Helper()
	labels := [][]store.NodeType{
		nil,
		{store.NodeTypeMicroArtefact},
		{store.NodeTypeEvidenceFile, store.NodeTypeMicroArtefact},
		{store.NodeTypeEvidenceFile},
	}
	edgeLabels := [][]store.EdgeType{
		{store.EdgeTypeContains},
		nil,
		{store.EdgeTypeContains, store.EdgeTypeSimilarTo},
	}
	nodes := make([]nodeRecord, 0, n)
	for i := 1; i <= n; i++ {
		var props []byte
		if blob := i % 4; blob > 0 {
			props = bytes.Repeat([]byte{byte('a' + i%23)}, blob*7)
		}
		nodes = append(nodes, nodeRecord{
			ID: store.NodeID(i), Labels: labels[i%len(labels)], Properties: props,
		})
	}
	edges := make([]rawEdge, 0, n-1)
	for i := 1; i < n; i++ {
		var props []byte
		if blob := i % 3; blob > 0 {
			props = bytes.Repeat([]byte{byte('A' + i%23)}, blob*11)
		}
		edges = append(edges, rawEdge{
			ID: store.EdgeID(i), Src: store.NodeID(i), Dst: store.NodeID(i + 1),
			Labels: edgeLabels[i%len(edgeLabels)], Weight: float32(i) / 4,
			Properties: props,
		})
	}
	return nodes, edges
}

// streamSource is the source the loader builds for this image, positioned and
// filled.
//
// csrRecordSourceOf rather than a hand-placed struct, because the offset a walk
// starts from is exactly the thing that could be wrong: a test that computed the
// record region itself would be testing its own arithmetic against the loader's
// rather than the walk against the file.
func streamSource(t *testing.T, blob []byte, mapped bool) *csrRecordSource {
	t.Helper()
	src, err := csrRecordSourceOf(blob, mapped)
	if err != nil {
		t.Fatalf("csrRecordSourceOf: %v", err)
	}
	return src
}

// Three walks over one image yield the same records, byte for byte.
//
// Three because that is how many buildSeq takes. Compared against the first walk
// rather than against the fixture, because the claim is that the walks agree with
// each other: a walk that was wrong in the same way every time would still build
// a self-consistent graph, and the byte-identity and round-trip tests next door
// are what catch that.
func TestCSRLoad_EveryPassYieldsTheSameRecords(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		name := "Copied"
		if mapped {
			name = "Mapped"
		}
		t.Run(name, func(t *testing.T) {
			nodes, edges := loadStreamFixture(t, 400)
			g, err := Build(nodes, edges)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			src := streamSource(t, g.Serialise(), mapped)

			var first []string
			for pass := range 3 {
				var got []string
				for n := range src.nodeSeq() {
					got = append(got, fmt.Sprintf("n%d|%v|%q", n.ID, n.Labels, n.Properties))
				}
				for e := range src.edgeSeq() {
					got = append(got, fmt.Sprintf("e%d|%d->%d|%g|%v|%q",
						e.ID, e.Src, e.Dst, e.Weight, e.Labels, e.Properties))
				}
				if pass == 0 {
					first = got
					if len(first) != len(nodes)+len(edges) {
						t.Fatalf("pass 0 yielded %d records, want %d", len(first), len(nodes)+len(edges))
					}
					continue
				}
				if !slices.Equal(got, first) {
					for i := range min(len(got), len(first)) {
						if got[i] != first[i] {
							t.Fatalf("pass %d differs from pass 0 at record %d:\n got %s\nwant %s",
								pass, i, got[i], first[i])
						}
					}
					t.Fatalf("pass %d yielded %d records, pass 0 yielded %d", pass, len(got), len(first))
				}
			}
		})
	}
}

// A walk hands out exactly what the fixture put in.
//
// This is the cursor check with teeth: the fixture's label and blob widths cycle,
// so a run taken one label or one byte out of place carries a different width and
// a different value, and the comparison is against the records as written rather
// than against another walk.
func TestCSRLoad_TheWalkReproducesTheRecordsAsWritten(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		name := "Copied"
		if mapped {
			name = "Mapped"
		}
		t.Run(name, func(t *testing.T) {
			nodes, edges := loadStreamFixture(t, 400)
			g, err := Build(nodes, edges)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			src := streamSource(t, g.Serialise(), mapped)

			i := 0
			for n := range src.nodeSeq() {
				want := nodes[i]
				if n.ID != want.ID || !slices.Equal(n.Labels, want.Labels) || !bytes.Equal(n.Properties, want.Properties) {
					t.Fatalf("node %d walked as {%d %v %q}, written as {%d %v %q}",
						i, n.ID, n.Labels, n.Properties, want.ID, want.Labels, want.Properties)
				}
				i++
			}
			if i != len(nodes) {
				t.Fatalf("the walk yielded %d nodes, %d were written", i, len(nodes))
			}
			i = 0
			for e := range src.edgeSeq() {
				want := edges[i]
				if e.ID != want.ID || e.Src != want.Src || e.Dst != want.Dst || e.Weight != want.Weight ||
					!slices.Equal(e.Labels, want.Labels) || !bytes.Equal(e.Properties, want.Properties) {
					t.Fatalf("edge %d walked as {%d %d->%d %g %v %q}, written as {%d %d->%d %g %v %q}",
						i, e.ID, e.Src, e.Dst, e.Weight, e.Labels, e.Properties,
						want.ID, want.Src, want.Dst, want.Weight, want.Labels, want.Properties)
				}
				i++
			}
			if i != len(edges) {
				t.Fatalf("the walk yielded %d edges, %d were written", i, len(edges))
			}
		})
	}
}

// A record's slice cannot be appended into its neighbour's bytes.
//
// csr_arena_test.go asserts this through a loaded store, which is the end-to-end
// form. This asserts it on the two functions that now produce every such slice,
// so that a three-index form lost in one of them fails here with a message naming
// it rather than three files away.
func TestCSRLoad_EveryRunIsCappedAtItsOwnLength(t *testing.T) {
	nodes, edges := loadStreamFixture(t, 64)
	g, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, mapped := range []bool{false, true} {
		src := streamSource(t, g.Serialise(), mapped)
		for n := range src.nodeSeq() {
			if got := cap(n.Labels); got != len(n.Labels) {
				t.Fatalf("mapped=%v: node %d's labels have cap %d for len %d; an append would reach the next record",
					mapped, n.ID, got, len(n.Labels))
			}
			if got := cap(n.Properties); got != len(n.Properties) {
				t.Fatalf("mapped=%v: node %d's blob has cap %d for len %d; an append would reach the next record",
					mapped, n.ID, got, len(n.Properties))
			}
		}
		for e := range src.edgeSeq() {
			if got := cap(e.Labels); got != len(e.Labels) {
				t.Fatalf("mapped=%v: edge %d's labels have cap %d for len %d", mapped, e.ID, got, len(e.Labels))
			}
			if got := cap(e.Properties); got != len(e.Properties) {
				t.Fatalf("mapped=%v: edge %d's blob has cap %d for len %d", mapped, e.ID, got, len(e.Properties))
			}
		}
	}
}

// An empty blob reads as nil whether the image was mapped or copied.
//
// Both stores normalise an empty blob to nil on the way in, so a reader that
// could tell an empty sub-slice from nil would be able to tell the two load paths
// apart -- which is what TestImageMode_SameGraphEitherWay forbids.
func TestCSRLoad_AnEmptyBlobIsNilOnBothPaths(t *testing.T) {
	g, err := Build([]nodeRecord{
		{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		{ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("x")},
	}, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	blob := g.Serialise()
	for _, mapped := range []bool{false, true} {
		src := streamSource(t, blob, mapped)
		for n := range src.nodeSeq() {
			switch n.ID {
			case 1:
				if n.Properties != nil {
					t.Fatalf("mapped=%v: an empty blob read as %v, want nil", mapped, n.Properties)
				}
				if n.Labels == nil {
					t.Fatalf("mapped=%v: a record with one label read it as nil", mapped)
				}
			case 2:
				if string(n.Properties) != "x" {
					t.Fatalf("mapped=%v: blob read as %q", mapped, n.Properties)
				}
			}
		}
	}
}
