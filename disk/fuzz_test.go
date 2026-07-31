package disk

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// Fuzz targets for the two parsers that read bytes the engine did not write.
//
// deserialiseCSR and WAL.Replay both consume a file that may have been
// truncated by a crash, corrupted by the storage medium, or edited by someone
// who should not have. Their contract is to reject what they cannot trust and
// never to crash on it — a parser that panics or exhausts memory turns a
// corrupt evidence file into a denial of service against the process holding
// the evidence.
//
// Run:
//
//	go test ./disk/ -run=XXX -fuzz=FuzzDeserialiseCSR -fuzztime=60s
//
// The seed corpus below is the important part: random bytes almost never form a
// valid GCSR header, so without real images to mutate the fuzzer spends its
// whole budget being rejected at the magic check.

// A header-declared count must be checked against the file's actual length
// before it is used to size an allocation.
//
// These inputs are a few dozen bytes each and previously caused deserialiseCSR
// to allocate from them unchecked: 1<<40 asks for terabytes, and 1<<63+1
// narrows to a negative int and panics inside makeslice. Both happened before
// any record was read, so the file never had to contain anything at all.
func TestDeserialiseCSR_RejectsCountsLargerThanTheFile(t *testing.T) {
	header := func(nodeCount, edgeCount uint64) []byte {
		b := make([]byte, csrV6HeaderSize)
		copy(b, "GCSR")
		binary.LittleEndian.PutUint16(b[4:6], csrVersionCurrent)
		binary.LittleEndian.PutUint64(b[6:14], nodeCount)
		binary.LittleEndian.PutUint64(b[14:22], edgeCount)
		binary.LittleEndian.PutUint64(b[38:46], uint64(csrV6HeaderSize))
		return b
	}

	for _, tc := range []struct {
		name                 string
		nodeCount, edgeCount uint64
	}{
		{"absurd node count", 1 << 40, 0},
		{"absurd edge count", 0, 1 << 40},
		{"node count overflows int", 1<<63 + 1, 0},
		{"edge count overflows int", 0, 1<<63 + 1},
		{"one node more than the bytes allow", 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := deserialiseCSR(header(tc.nodeCount, tc.edgeCount))
			if err == nil {
				t.Fatal("accepted a count the file cannot possibly hold")
			}
		})
	}
}

// The same property for the index section's own length prefixes.
func TestReadCSRIndexSection_RejectsCountsLargerThanTheSection(t *testing.T) {
	section := make([]byte, csrIndexSectionMagicSize+8)
	copy(section, csrIndexSectionMagic)
	binary.LittleEndian.PutUint64(section[csrIndexSectionMagicSize:], 1<<40)

	if _, err := readCSRIndexSection(section, 0); err == nil {
		t.Fatal("accepted a property-entry count the section cannot possibly hold")
	}
}

// A WAL record cannot be longer than the log containing it. Replay must decide
// that from the header, before allocating the payload it describes.
func TestWALReplay_RejectsPayloadLongerThanTheLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "huge.wal")

	rec := make([]byte, walHeaderSize)
	rec[0] = walRecordNode
	binary.LittleEndian.PutUint32(rec[1:5], ^uint32(0)) // 4 GiB
	if err := os.WriteFile(path, rec, 0600); err != nil {
		t.Fatal(err)
	}

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	applied := 0
	if err := w.Replay(ReplayCallbacks{
		NodeFunc: func([]byte) error { applied++; return nil },
	}); err != nil {
		t.Fatalf("Replay should treat an over-long record as a torn tail, got: %v", err)
	}
	if applied != 0 {
		t.Fatalf("applied %d records from a log containing only a bogus header", applied)
	}
}

// csrSeed returns a serialised CSR carrying records and index entries, so the
// fuzzer has a structurally valid image to mutate rather than only noise.
func csrSeed(t testingTB) []byte {
	t.Helper()
	nodes := []nodeRecord{
		{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}, Properties: []byte("alpha")},
		{ID: 2, Labels: []store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeTag}},
	}
	edges := []rawEdge{
		{ID: 1, Src: 1, Dst: 2, Labels: []store.EdgeType{store.EdgeTypeContains}, Weight: 0.75},
	}
	g := Build(nodes, edges)
	g.nodeSeqHW, g.edgeSeqHW = 2, 1
	return g.SerialiseWithIndex(
		[]index.NodePropEntry{
			{ID: 1, Key: "sha256", Value: []byte("aabb")},
			{ID: 2, Key: "path", Value: []byte("/tmp/x")},
		},
		[]index.EdgePropEntry{
			{ID: 1, Key: "rel", Value: []byte("contains")},
		},
	)
}

// testingTB is the small slice of testing.TB these helpers need, so one seed
// builder serves both a Fuzz and a plain Test.
type testingTB interface {
	Helper()
	Fatalf(string, ...any)
}

// fuzzWALPath returns one reusable log path per worker process. os.MkdirTemp
// gives each process its own directory, so concurrent workers cannot collide.
var fuzzWALPath = sync.OnceValue(func() string {
	dir, err := os.MkdirTemp("", "graphene-fuzzwal-*")
	if err != nil {
		panic("fuzz: cannot create temp dir: " + err.Error())
	}
	return filepath.Join(dir, "fuzz.wal")
})

// walSeedBytes returns a log containing one committed batch and a couple of
// loose records, so the fuzzer starts from something structurally valid.
func walSeedBytes(t testingTB) []byte {
	t.Helper()
	dir, err := os.MkdirTemp("", "graphene-fuzzseed-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "seed.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := w.AppendNode([]byte("node-payload")); err != nil {
		t.Fatalf("AppendNode: %v", err)
	}
	if err := w.AppendNodeProp([]byte("prop-payload")); err != nil {
		t.Fatalf("AppendNodeProp: %v", err)
	}
	b := newWALBatch(64)
	b.add(walRecordNode, []byte("batched-node"))
	b.add(walRecordEdge, []byte("batched-edge"))
	if err := w.AppendBatch(b.finish(), true); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	return data
}

// walUncommittedBatchBytes returns a log whose batch begins and never commits.
// Replay must discard it entirely — that discard is the rollback.
func walUncommittedBatchBytes(t testingTB) []byte {
	t.Helper()
	b := newWALBatch(64)
	b.add(walRecordNode, []byte("never-committed"))
	framed := b.finish()
	// Drop the commit marker: begin + body, nothing else.
	return framed[:len(framed)-(walRecordOverhead+walBatchCommitPayload)]
}

// deserialiseCSR must reject anything it cannot parse, and must do so without
// panicking and without sizing an allocation from an unvalidated header field.
//
// Regression coverage: the node, edge, and property-index counts are read from
// the header and were previously passed straight to make(). A 46-byte file
// claiming 2^40 nodes allocated until the process died, and one claiming more
// than MaxInt64 narrowed to a negative int and panicked in makeslice — both
// before a single record had been read. See minNodeRecordBytes.
func FuzzDeserialiseCSR(f *testing.F) {
	f.Add(csrSeed(f))
	f.Add([]byte("GCSR"))
	f.Add([]byte{})
	// A header whose counts are absurd. This input is the bug that motivated the
	// bound; it must now be rejected rather than allocated for.
	hostile := make([]byte, csrV6HeaderSize)
	copy(hostile, "GCSR")
	binary.LittleEndian.PutUint16(hostile[4:6], csrVersionCurrent)
	binary.LittleEndian.PutUint64(hostile[6:14], 1<<40)  // nodeCount
	binary.LittleEndian.PutUint64(hostile[14:22], 1<<40) // edgeCount
	f.Add(hostile)
	// The same, but large enough that int(uint64) goes negative.
	negative := make([]byte, csrV6HeaderSize)
	copy(negative, hostile)
	binary.LittleEndian.PutUint64(negative[6:14], 1<<63+1)
	f.Add(negative)

	f.Fuzz(func(t *testing.T, data []byte) {
		csr, section, err := deserialiseCSR(data)
		if err != nil {
			return // rejecting malformed input is the expected outcome
		}
		if csr == nil {
			t.Fatal("deserialiseCSR returned nil graph with nil error")
		}

		// A parse that succeeded must have produced something self-consistent:
		// every edge endpoint must be addressable in the node array, or the
		// adjacency arrays Build() derives from them would index out of range on
		// first use rather than at parse time.
		for i := 1; i < len(csr.edges); i++ {
			e := csr.edges[i]
			if e.ID == store.InvalidEdgeID {
				continue
			}
			if uint64(e.Src) >= uint64(len(csr.nodes)) || uint64(e.Dst) >= uint64(len(csr.nodes)) {
				t.Fatalf("edge %d accepted with endpoints outside the node array: src=%d dst=%d nodes=%d",
					e.ID, e.Src, e.Dst, len(csr.nodes))
			}
		}
		if section != nil {
			for _, entry := range section.NodeProps {
				if entry.Key == "" && entry.Value == nil {
					t.Fatalf("accepted an empty node property entry for id %d", entry.ID)
				}
			}
		}
	})
}

// WAL replay must survive an arbitrary log. Corrupt records are expected and are
// treated as a torn tail; what must never happen is a panic, an unbounded
// allocation, or a partially applied batch.
//
// Regression coverage: the per-record payload length is a uint32 read straight
// from the log and was passed to make() unchecked, so a five-byte header
// claiming 0xFFFFFFFF demanded 4 GiB before reading any of it.
func FuzzWALReplay(f *testing.F) {
	f.Add(walSeedBytes(f))
	f.Add([]byte{})
	// A single header declaring a 4 GiB payload — the allocation bug.
	huge := make([]byte, walHeaderSize)
	huge[0] = walRecordNode
	binary.LittleEndian.PutUint32(huge[1:5], ^uint32(0))
	f.Add(huge)
	// A batch that begins and never commits: replay must apply none of it.
	f.Add(walUncommittedBatchBytes(f))

	f.Fuzz(func(t *testing.T, data []byte) {
		// One directory and one path for the whole worker process, overwritten per
		// input. Calling t.TempDir() per iteration means creating and removing a
		// directory for every candidate, which held this target to ~30 exec/s —
		// too slow to explore anything. Fuzzing workers run inputs sequentially
		// within a process, so a shared path is safe.
		path := fuzzWALPath()
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Skip("could not stage log")
		}
		w, err := OpenWAL(path)
		if err != nil {
			return
		}
		defer w.Close()

		applied := 0
		count := func([]byte) error { applied++; return nil }
		// Replay either succeeds or reports a malformed log. Both are fine; the
		// test is that it returns at all, without panicking or exhausting memory.
		_ = w.Replay(ReplayCallbacks{
			NodeFunc: count, EdgeFunc: count,
			NodePropFunc: count, EdgePropFunc: count,
			NodeDeleteFunc: count, EdgeDeleteFunc: count,
			NodePropPurgeFunc: count, EdgePropPurgeFunc: count,
		})
		if applied < 0 {
			t.Fatal("unreachable; keeps applied live")
		}
	})
}
