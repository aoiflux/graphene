package disk

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
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

// An absolute ID ceiling alone is not a bound. Build sizes its arrays from the
// highest ID, so one small record naming a large ID demands the whole ceiling's
// worth of memory — which is how a ~60-byte file asked for roughly 15 GB while
// satisfying every count check. The ID has to be bounded against how many
// records the file actually carries.
func TestDeserialiseCSR_RejectsSparseIDsInATinyFile(t *testing.T) {
	// One node record, ID just under the absolute ceiling.
	buf := make([]byte, csrV6HeaderSize)
	copy(buf, "GCSR")
	binary.LittleEndian.PutUint16(buf[4:6], csrVersionCurrent)
	binary.LittleEndian.PutUint64(buf[6:14], 1)               // nodeCount
	binary.LittleEndian.PutUint64(buf[14:22], 0)              // edgeCount
	binary.LittleEndian.PutUint64(buf[22:30], maxCSREntityID) // nodeSeqHW, so the mark check passes

	rec := make([]byte, 13)
	binary.LittleEndian.PutUint64(rec[0:8], maxCSREntityID-1) // the ID
	rec[8] = 0                                                // no labels
	binary.LittleEndian.PutUint32(rec[9:13], 0)               // no properties
	buf = append(buf, rec...)
	binary.LittleEndian.PutUint64(buf[38:46], uint64(len(buf)))

	if _, _, err := deserialiseCSR(buf); err == nil {
		t.Fatalf("a %d-byte file named ID %d and was accepted; Build would allocate from it",
			len(buf), maxCSREntityID-1)
	}
}

// The sparsity bound must not reject a store that has legitimately burned IDs.
// IDs are never reused and Compact preserves them, so a long-lived store's
// highest ID sits well above its live record count — that file has to open.
func TestDeserialiseCSR_AcceptsLegitimatelySparseIDs(t *testing.T) {
	const n = 400
	nodes := make([]nodeRecord, 0, n)
	for i := 1; i <= n; i++ {
		// 50x sparsity: the store deleted ~98% of what it ever created.
		nodes = append(nodes, nodeRecord{
			ID:     store.NodeID(i * 50),
			Labels: []store.NodeType{store.NodeTypeMicroArtefact},
		})
	}
	g := Build(nodes, nil)
	g.nodeSeqHW = uint64(n * 50)

	if _, _, err := deserialiseCSR(g.SerialiseWithIndex(nil, nil)); err != nil {
		t.Fatalf("rejected a legitimately sparse store (%d records, highest ID %d): %v",
			n, n*50, err)
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

// A batch-begin marker's record count sizes an allocation, so it is bounded by
// what the log could hold — not by its own CRC, which is computed over the very
// bytes making the claim.
//
// The 13-byte input below claims 2^32-1 records and asked for roughly 137 GB of
// pendingRecord before reading any of them. Found by FuzzWALReplay once the
// parser could be driven in memory; the file-backed target never reached it.
func TestWALReplay_RejectsBatchCountLargerThanTheLog(t *testing.T) {
	rec := make([]byte, 0, walRecordOverhead+walBatchBeginPayload)
	rec = appendRecord(rec, walRecordBatchBegin, []byte{0xFF, 0xFF, 0xFF, 0xFF})

	applied := 0
	err := replayRecords(bytes.NewReader(rec), int64(len(rec)), walFramingV1, ReplayCallbacks{
		NodeFunc: func([]byte) error { applied++; return nil },
	})
	if err == nil {
		t.Fatal("accepted a batch declaring more records than the log can hold")
	}
	if applied != 0 {
		t.Fatalf("applied %d records from a log containing only a begin marker", applied)
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
	if err := w.AppendBatch(mustFinish(b, batchMeta{}), true); err != nil {
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
	framed := mustFinish(b, batchMeta{})
	// Drop the commit marker: begin + body, nothing else.
	return framed[:len(framed)-(walRecordOverhead+walBatchCommitPayloadV2)]
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
		// Driven through replayRecords rather than WAL.Replay so each candidate
		// costs a bytes.Reader instead of a file write and an open. Going through
		// the file handle held this target to a few thousand executions a minute,
		// which is not enough to explore a framed binary format.
		applied := 0
		count := func([]byte) error { applied++; return nil }
		cb := ReplayCallbacks{
			NodeFunc: count, EdgeFunc: count,
			NodePropFunc: count, EdgePropFunc: count,
			NodeDeleteFunc: count, EdgeDeleteFunc: count,
			NodePropPurgeFunc: count, EdgePropPurgeFunc: count,
		}

		// Either outcome is fine — a malformed log should be reported, a torn one
		// truncated. What must not happen is a panic or an unbounded allocation.
		if err := replayRecords(bytes.NewReader(data), int64(len(data)), walFramingV1, cb); err != nil {
			return
		}

		// A clean parse must be deterministic: the same bytes must apply the same
		// records. Batch buffering makes this worth stating — a batch that commits
		// applies all of its records, one that does not applies none, and nothing
		// in between is a legal outcome.
		second := 0
		cb2 := ReplayCallbacks{
			NodeFunc:       func([]byte) error { second++; return nil },
			EdgeFunc:       func([]byte) error { second++; return nil },
			NodePropFunc:   func([]byte) error { second++; return nil },
			EdgePropFunc:   func([]byte) error { second++; return nil },
			NodeDeleteFunc: func([]byte) error { second++; return nil },
			EdgeDeleteFunc: func([]byte) error { second++; return nil },

			NodePropPurgeFunc: func([]byte) error { second++; return nil },
			EdgePropPurgeFunc: func([]byte) error { second++; return nil },
		}
		if err := replayRecords(bytes.NewReader(data), int64(len(data)), walFramingV1, cb2); err != nil {
			t.Fatalf("replay succeeded then failed on the same %d bytes: %v", len(data), err)
		}
		if second != applied {
			t.Fatalf("replay is not deterministic: applied %d records then %d from the same bytes",
				applied, second)
		}
	})
}
