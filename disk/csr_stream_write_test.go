package disk

// The streaming image writer.
//
// Three things have to hold, and they are separate claims:
//
//   - the bytes are the bytes the buffered writer wrote — held by the golden
//     fixture in csr_golden_bytes_test.go, and here by the file path and the
//     memory path agreeing with each other;
//   - the Merkle roots are the roots the materialised leaves produced, which is
//     checked directly against merkle.Root over NodeLeaves/EdgeLeaves rather
//     than only through the golden image;
//   - writing costs a constant, which is the entire point of the change and the
//     only one of the three that a correct-looking implementation can fail
//     silently.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// streamFixture builds a graph big enough for the image to dwarf any constant,
// with property blobs large enough that a per-record copy would show up.
func streamFixture(t *testing.T, n int) (*CSRGraph, csrPayload) {
	t.Helper()

	blob := bytes.Repeat([]byte("x"), 512)
	nodes := make([]nodeRecord, 0, n)
	var nodeProps []index.NodePropEntry
	for i := 1; i <= n; i++ {
		nodes = append(nodes, nodeRecord{
			ID:         store.NodeID(i),
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: append([]byte(fmt.Sprintf("%d:", i)), blob...),
		})
		nodeProps = append(nodeProps,
			index.NodePropEntry{ID: store.NodeID(i), Key: "path", Value: fmt.Appendf(nil, "/p/%d", i)},
			index.NodePropEntry{ID: store.NodeID(i), Key: "kind", Value: []byte("artefact")},
		)
	}

	edges := make([]rawEdge, 0, n-1)
	var edgeProps []index.EdgePropEntry
	for i := 1; i < n; i++ {
		edges = append(edges, rawEdge{
			ID:         store.EdgeID(i),
			Src:        store.NodeID(i),
			Dst:        store.NodeID(i + 1),
			Labels:     []store.EdgeType{store.EdgeTypeContains},
			Weight:     float32(i),
			Properties: blob,
		})
		edgeProps = append(edgeProps,
			index.EdgePropEntry{ID: store.EdgeID(i), Key: "rel", Value: []byte("contains")})
	}

	g, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	g.nodeSeqHW, g.edgeSeqHW = uint64(n), uint64(n)
	g.commitSeqHW = 7
	g.lastCompactUnixNano = 1_757_000_000_000_000_000

	return g, csrPayload{
		NodeProps:         slices.Values(nodeProps),
		EdgeProps:         slices.Values(edgeProps),
		OrderedNodeKeys:   []string{"path"},
		CompositeNodeKeys: [][]string{{"path", "kind"}},
		WithSnapshotRoots: true,
		Tombstones:        []Tombstone{{Scope: ScopeNode, NodeID: 77, RedactionSeq: 4}},
	}
}

// serialiseToFile writes the image through the file path and returns it.
func serialiseToFile(t *testing.T, g *CSRGraph, payload csrPayload) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "image.csr")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := g.SerialiseTo(f, payload); err != nil {
		f.Close()
		t.Fatalf("SerialiseTo: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	return data
}

// The file and the memory path are the same writer over two different seekers,
// and the seek-and-patch at the end is the part that can differ between them.
// A file that disagreed with the in-memory image would mean every test in this
// package was checking a form the store never writes.
func TestSerialiseTo_FileMatchesMemory(t *testing.T) {
	g, payload := streamFixture(t, 50)

	inMemory, err := g.SerialiseWithPayload(payload)
	if err != nil {
		t.Fatalf("SerialiseWithPayload: %v", err)
	}
	onDisk := serialiseToFile(t, g, payload)

	if !bytes.Equal(inMemory, onDisk) {
		t.Fatalf("the file is %d bytes and the buffer %d; they must be identical",
			len(onDisk), len(inMemory))
	}
	if _, _, err := deserialiseCSR(onDisk); err != nil {
		t.Fatalf("the image this build wrote does not parse: %v", err)
	}
}

// Every byte test in this package used to fit inside one output buffer, so the
// writer's refill path — drain, reset, keep going — was exercised by nothing
// that looked at the result. A drain that forgot to empty the buffer duplicated
// every 64 KiB of the image and no test noticed.
//
// So: an image several buffers long, and one whose single record is larger than
// the buffer and takes the bypass instead. Both are checked by parsing the
// result and re-deriving its roots, because that is what a duplicated or lost
// run of bytes destroys.
func TestSerialiseTo_CrossesTheBufferCleanly(t *testing.T) {
	t.Run("many buffers", func(t *testing.T) {
		g, payload := streamFixture(t, 500)
		data := serialiseToFile(t, g, payload)
		if len(data) < 4*imageWriteBufferSize {
			t.Fatalf("fixture is %d bytes, too small to cross the %d-byte buffer several times",
				len(data), imageWriteBufferSize)
		}
		assertImageReparses(t, data, g, payload)
	})

	t.Run("one record larger than the buffer", func(t *testing.T) {
		blob := bytes.Repeat([]byte("Q"), 3*imageWriteBufferSize)
		nodes := []nodeRecord{
			{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("small")},
			{ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: blob},
			{ID: 3, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("small again")},
		}
		edges := []rawEdge{{ID: 1, Src: 1, Dst: 2, Properties: blob}}
		g, err := Build(nodes, edges)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		payload := csrPayload{
			NodeProps:         slices.Values([]index.NodePropEntry{{ID: 2, Key: "blob", Value: blob}}),
			WithSnapshotRoots: true,
		}
		assertImageReparses(t, serialiseToFile(t, g, payload), g, payload)
	})
}

// assertImageReparses checks that an image parses back to the graph it was
// written from, records and roots alike.
func assertImageReparses(t *testing.T, data []byte, g *CSRGraph, payload csrPayload) {
	t.Helper()

	if status, _ := csrDigestStatus(data); status != DigestMatch {
		t.Fatalf("digest %v on an image this build just wrote", status)
	}
	back, _, err := deserialiseCSR(data)
	if err != nil {
		t.Fatalf("the image does not parse: %v", err)
	}
	if back.NodeCount() != g.NodeCount() || back.EdgeCount() != g.EdgeCount() {
		t.Fatalf("parsed %d nodes and %d edges, wrote %d and %d",
			back.NodeCount(), back.EdgeCount(), g.NodeCount(), g.EdgeCount())
	}
	for n := range g.Nodes() {
		got, ok := back.GetNode(n.ID)
		if !ok {
			t.Fatalf("node %d is missing from the image", n.ID)
		}
		if !bytes.Equal(got.Properties, n.Properties) {
			t.Fatalf("node %d came back with %d property bytes, wrote %d",
				n.ID, len(got.Properties), len(n.Properties))
		}
	}
	for e := range g.Edges() {
		got, ok := back.GetEdge(e.ID)
		if !ok {
			t.Fatalf("edge %d is missing from the image", e.ID)
		}
		if !bytes.Equal(got.Properties, e.Properties) {
			t.Fatalf("edge %d came back with %d property bytes, wrote %d",
				e.ID, len(got.Properties), len(e.Properties))
		}
	}

	wrote, ok := g.Roots()
	if !ok {
		t.Fatal("no roots on the graph that was written")
	}
	read, ok := back.Roots()
	if !ok {
		t.Fatal("the image carries no roots")
	}
	if read != wrote {
		t.Fatal("the roots in the image are not the roots that were written")
	}
	if again := computeSnapshotRootsAs(read.bodyVersion(), back, payload, read.PrevRoot); again != read {
		t.Fatal("recomputing the roots from the parsed image disagrees with the image's own")
	}
}

// The digest is computed by reading the finished file back, which is a
// different code path from the whole-buffer digest every verifier uses. They
// have to agree, or an image would be written with a digest that fails its own
// verification the moment it is opened.
func TestSerialiseTo_DigestAgreesWithEveryVerifier(t *testing.T) {
	g, payload := streamFixture(t, 40)
	data := serialiseToFile(t, g, payload)

	if status, _ := csrDigestStatus(data); status != DigestMatch {
		t.Fatalf("whole-buffer digest check: %v, want DigestMatch", status)
	}
	status, _, err := csrDigestStatusOf(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("streamed digest check: %v", err)
	}
	if status != DigestMatch {
		t.Fatalf("streamed digest check: %v, want DigestMatch", status)
	}

	// And the value itself, not merely the verdict.
	stored, ok := readCSRDigest(data)
	if !ok {
		t.Fatal("no digest in the image")
	}
	if stored != computeCSRDigest(data) {
		t.Fatal("the digest written is not the digest computeCSRDigest derives from the same bytes")
	}
}

// The roots must be the roots the materialised leaves produce. This is the
// guard that RootBuilder-in-the-write is the same tree as merkle.Root over a
// []Hash, tested against this package's own leaf functions rather than only
// through the golden image — the golden file would catch a change to the leaf
// *encoding* too, and this separates the two.
func TestSnapshotRootStream_MatchesTheMaterialisedLeaves(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 17, 64, 65, 200} {
		var g *CSRGraph
		var payload csrPayload
		if n == 0 {
			var err error
			if g, err = Build(nil, nil); err != nil {
				t.Fatalf("build: %v", err)
			}
			payload = csrPayload{WithSnapshotRoots: true}
		} else {
			g, payload = streamFixture(t, n)
		}

		if _, err := g.SerialiseWithPayload(payload); err != nil {
			t.Fatalf("n=%d: serialise: %v", n, err)
		}
		got, ok := g.Roots()
		if !ok {
			t.Fatalf("n=%d: no roots after serialising with WithSnapshotRoots", n)
		}

		v := got.bodyVersion()
		if want := merkle.Root(g.NodeLeaves(v)); got.NodeRoot != want {
			t.Fatalf("n=%d: NodeRoot %x, materialised leaves give %x", n, got.NodeRoot, want)
		}
		if want := merkle.Root(g.EdgeLeaves(v)); got.EdgeRoot != want {
			t.Fatalf("n=%d: EdgeRoot %x, materialised leaves give %x", n, got.EdgeRoot, want)
		}
		if want := computeSnapshotRootsAs(v, g, payload, payload.PrevSnapshotRoot); want != got {
			t.Fatalf("n=%d: the written roots differ from a recomputation of the same image", n)
		}
	}
}

// The acceptance criterion: writing an image costs a constant, not an image.
//
// Two assertions, because either alone can be satisfied by the wrong code. The
// ceiling catches a writer that holds the image — the buffered writer allocated
// at least the whole file, plus a bytes.Buffer doubling on top of it, plus 32
// bytes per node, per edge and per index entry for the Merkle leaves. The slope
// catches a per-record cost small enough to slip under the ceiling at these
// sizes and ruinous at the size this program is aimed at.
func TestSerialiseTo_AllocatesIndependentlyOfImageSize(t *testing.T) {
	type arm struct {
		n          int
		allocated  uint64
		imageBytes int
	}
	arms := []arm{{n: 500}, {n: 4000}}

	for i := range arms {
		g, payload := streamFixture(t, arms[i].n)
		path := filepath.Join(t.TempDir(), "image.csr")

		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		if err := g.SerialiseTo(f, payload); err != nil {
			t.Fatalf("SerialiseTo: %v", err)
		}
		runtime.ReadMemStats(&after)
		f.Close()

		fi, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		arms[i].allocated = after.TotalAlloc - before.TotalAlloc
		arms[i].imageBytes = int(fi.Size())

		t.Logf("n=%d: image %d B, allocated %d B during the write",
			arms[i].n, arms[i].imageBytes, arms[i].allocated)

		// The stated constant is one 64 KiB buffer plus the section directory,
		// the tombstone section and the iterator closures. Four buffers is
		// generous room around that and still far under the smaller image.
		if ceiling := uint64(4 * imageWriteBufferSize); arms[i].allocated > ceiling {
			t.Fatalf("n=%d: writing a %d-byte image allocated %d bytes, over the %d-byte ceiling; "+
				"the write is supposed to hold a constant, not the image",
				arms[i].n, arms[i].imageBytes, arms[i].allocated, ceiling)
		}
	}

	// And the constant must actually be constant: an eight-fold image may not
	// cost materially more to write. A ceiling alone would pass for anything
	// with a small enough coefficient; the slope is the property.
	small, large := arms[0], arms[1]
	if large.allocated > small.allocated+small.allocated/4 {
		t.Fatalf("writing a %d-byte image allocated %d B and a %d-byte one allocated %d B; "+
			"the cost is tracking the image",
			small.imageBytes, small.allocated, large.imageBytes, large.allocated)
	}
}

// failBeyond refuses any write that would reach past a byte offset — a disk
// that fills up part way through the image.
//
// The two patches at the end of SerialiseTo land inside the header, so a limit
// past the header lets them through. That is deliberate: it makes the test
// depend on the streamed body's error being *latched and reported*, rather than
// on the patch happening to fail as well. A writer that dropped the body error
// would otherwise look fine here and produce a short image with a header and a
// digest that both parse.
type failBeyond struct {
	memImage
	limit int64
}

var errWriteBudget = errors.New("failBeyond: past the limit")

func (f *failBeyond) Write(p []byte) (int, error) {
	if f.pos+int64(len(p)) > f.limit {
		return 0, errWriteBudget
	}
	return f.memImage.Write(p)
}

// The writer latches the first error and reports it at flush, which is the only
// place SerialiseTo checks. Without that check a full disk would leave a temp
// image that renamed cleanly over a good one.
func TestSerialiseTo_ReportsAWriteFailure(t *testing.T) {
	g, payload := streamFixture(t, 200)
	for _, limit := range []int64{csrV8HeaderSize + 1, 70_000, 150_000} {
		dst := &failBeyond{limit: limit}
		if err := g.SerialiseTo(dst, payload); !errors.Is(err, errWriteBudget) {
			t.Fatalf("limit %d: SerialiseTo returned %v, want the write error", limit, err)
		}
	}
}

// The leaf encoder reuses its scratch across records, so the length of the
// previous record must not reach the current one. Encoding a long record and
// then a short one is the shape that catches a missing re-slice.
func TestLeafEncoder_ReusedScratchDoesNotLeakBetweenRecords(t *testing.T) {
	long := nodeRecord{
		ID:         1,
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact, store.NodeTypeEvidenceFile},
		Properties: bytes.Repeat([]byte("y"), 4096),
	}
	short := nodeRecord{ID: 2, Properties: []byte("z")}

	var le leafEncoder
	if got, want := le.node(snapshotBodyV1, long), nodeLeafData(long); !bytes.Equal(got, want) {
		t.Fatal("the reused encoder disagrees with nodeLeafData on the long record")
	}
	if got, want := le.node(snapshotBodyV1, short), nodeLeafData(short); !bytes.Equal(got, want) {
		t.Fatalf("after a long record the encoder produced %d bytes for a short one, want %d",
			len(got), len(want))
	}
	if got, want := le.node(snapshotBodyV3, long), nodeLeafDataV2(long); !bytes.Equal(got, want) {
		t.Fatal("the reused encoder disagrees with nodeLeafDataV2")
	}

	longEdge := rawEdge{ID: 1, Src: 1, Dst: 2, Weight: 1.5, Properties: bytes.Repeat([]byte("y"), 4096)}
	shortEdge := rawEdge{ID: 2, Src: 2, Dst: 1}
	if got, want := le.edge(snapshotBodyV1, longEdge), edgeLeafData(longEdge); !bytes.Equal(got, want) {
		t.Fatal("the reused encoder disagrees with edgeLeafData on the long edge")
	}
	if got, want := le.edge(snapshotBodyV1, shortEdge), edgeLeafData(shortEdge); !bytes.Equal(got, want) {
		t.Fatalf("after a long edge the encoder produced %d bytes for a short one, want %d",
			len(got), len(want))
	}
	if got, want := le.edge(snapshotBodyV3, longEdge), edgeLeafDataV2(longEdge); !bytes.Equal(got, want) {
		t.Fatal("the reused encoder disagrees with edgeLeafDataV2")
	}
}

// The writer owns one fixed buffer and must keep owning it: the caller hands
// the same 64 KiB to the digest pass afterwards, and a writer that let append
// grow it would quietly be holding a second, larger one — the exact cost this
// item removes, reintroduced somewhere nothing looks.
//
// Straddling the boundary with each of the three primitives, because they make
// room separately and a missing check in any one of them looks correct in the
// bytes and wrong in the footprint.
func TestImageWriter_StraddlesTheBufferBoundaryWithoutGrowing(t *testing.T) {
	cases := []struct {
		name string
		tail func(iw *imageWriter)
		want string
	}{
		{"bytes", func(iw *imageWriter) { iw.bytes([]byte("abcd")) }, "abcd"},
		{"str", func(iw *imageWriter) { iw.str("abcd") }, "abcd"},
		{"u8", func(iw *imageWriter) { iw.u8('a'); iw.u8('b'); iw.u8('c'); iw.u8('d') }, "abcd"},
	}
	// Positions either side of the boundary, so the straddle is real in some
	// runs and the write fits exactly in others.
	for _, fill := range []int{imageWriteBufferSize - 6, imageWriteBufferSize - 4, imageWriteBufferSize - 2} {
		for _, c := range cases {
			var sink memImage
			buf := make([]byte, imageWriteBufferSize)
			iw := newImageWriter(&sink, buf)

			head := bytes.Repeat([]byte("."), fill)
			iw.bytes(head)
			c.tail(iw)
			if err := iw.flush(); err != nil {
				t.Fatalf("%s at %d: flush: %v", c.name, fill, err)
			}

			if got, want := string(sink.buf), string(head)+c.want; got != want {
				t.Fatalf("%s at %d: wrote %d bytes ending %q, want %d ending %q",
					c.name, fill, len(got), got[max(0, len(got)-8):],
					len(want), want[len(want)-8:])
			}
			if iw.at() != uint64(fill+len(c.want)) {
				t.Fatalf("%s at %d: offset is %d, want %d", c.name, fill, iw.at(), fill+len(c.want))
			}
			if cap(iw.buf) != imageWriteBufferSize {
				t.Fatalf("%s at %d: the writer grew its buffer to %d bytes; it must reuse the one "+
					"it was given, which the digest pass reads through afterwards",
					c.name, fill, cap(iw.buf))
			}
		}
	}
}

// memImage is the in-memory half of the writer, and the patches at the end of
// SerialiseTo are the only place anything seeks backwards. Its Read is used by
// the digest pass, so a Read that ignored the position would produce a digest
// over the wrong bytes rather than an error.
func TestMemImage_SeekReadWrite(t *testing.T) {
	var m memImage
	if _, err := m.Write([]byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Seek(2, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Write([]byte("XY")); err != nil {
		t.Fatal(err)
	}
	if got := string(m.buf); got != "abXYefgh" {
		t.Fatalf("after a patch the buffer is %q", got)
	}

	if _, err := m.Seek(4, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(&m)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != "efgh" {
		t.Fatalf("read from offset 4 gave %q, want %q", rest, "efgh")
	}

	if _, err := m.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("a negative position was accepted")
	}
	if _, err := m.Seek(0, 99); err == nil {
		t.Fatal("an unknown whence was accepted")
	}
}
