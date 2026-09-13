package disk

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/aoiflux/graphene/store"
)

// TestResidentConstants_MatchTheStructsTheyDescribe is why estimate.go and
// view.go are allowed to spell their struct sizes out as literals.
//
// The literals exist so that an arithmetic model does not put unsafe in a
// production file, where tests/build_constraints audits every appearance of it by
// exact set and each one has to be argued for. A size model is not the kind of
// thing that argument should have to be made for, so the import moves here — and
// once it is here, the check it enables is strictly better than the import it
// replaced. A constant checked against unsafe.Sizeof cannot drift; a
// unsafe.Sizeof in production cannot be wrong but also cannot be *noticed*
// changing, and a struct that grows a field would silently move every figure a
// budget is refused on.
//
// So: a field added to any of these fails here, at the line naming the number
// that needs revisiting, rather than in a resident estimate nobody re-derives.
func TestResidentConstants_MatchTheStructsTheyDescribe(t *testing.T) {
	cases := []struct {
		name string
		want int64
		got  uintptr
	}{
		{"sizeofNodeRecord", sizeofNodeRecord, unsafe.Sizeof(nodeRecord{})},
		{"sizeofRawEdge", sizeofRawEdge, unsafe.Sizeof(rawEdge{})},
		{"sizeofNodeVersion", sizeofNodeVersion, unsafe.Sizeof(nodeVersion{})},
		{"sizeofEdgeVersion", sizeofEdgeVersion, unsafe.Sizeof(edgeVersion{})},
		{"sizeofNode", sizeofNode, unsafe.Sizeof(store.Node{})},
		{"sizeofEdge", sizeofEdge, unsafe.Sizeof(store.Edge{})},
		{"sizeofLabel", sizeofLabel, unsafe.Sizeof(store.NodeType(0))},
		{"sizeofLabel (edge)", sizeofLabel, unsafe.Sizeof(store.EdgeType(0))},
		{"sizeofNodeID", sizeofNodeID, unsafe.Sizeof(store.NodeID(0))},
		{"sizeofEdgeID", sizeofEdgeID, unsafe.Sizeof(store.EdgeID(0))},
		{"sizeofDeltaAdj", sizeofDeltaAdj, unsafe.Sizeof(deltaAdj{})},
		{"sizeofWALSlot", sizeofWALSlot, unsafe.Sizeof(walSlot{})},
		{"sizeofPointer", sizeofPointer, unsafe.Sizeof(uintptr(0))},
		{"sizeofDirEntry", sizeofDirEntry, unsafe.Sizeof(int32(0))},
		{"sizeofPageEntry", sizeofPageEntry, unsafe.Sizeof(uint32(0))},
		{"sizeofLiveBefore", sizeofLiveBefore, unsafe.Sizeof(int(0))},
		{"sizeofOffset", sizeofOffset, unsafe.Sizeof(uint64(0))},
	}
	for _, c := range cases {
		if c.want != int64(c.got) {
			t.Errorf("%s is %d, the struct it describes is %d bytes\n"+
				"a field was added or removed: fix the constant and re-read the resident model it feeds",
				c.name, c.want, c.got)
		}
	}
}

// TestDeltaBytes_CountsWhatWasWritten checks the maintained figure against
// records whose cost is known by construction.
//
// The blobs are sized so that the payload dominates the cells: at 4 KiB a record
// the arithmetic is dominated by a term the test controls exactly, so a
// discrepancy names itself rather than hiding inside per-cell constants.
func TestDeltaBytes_CountsWhatWasWritten(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	const (
		count = 64
		blob  = 4 << 10
	)
	payload := bytes.Repeat([]byte{0xAB}, blob)
	for i := 0; i < count; i++ {
		if _, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: payload,
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	want := int64(count) * (sizeofNodeVersion + sizeofNode + blob + sizeofLabel)
	if got := s.StorageStats().DeltaBytes; got != want {
		t.Errorf("DeltaBytes is %d, want %d", got, want)
	}
}

// TestDeltaBytes_ATombstoneCostsTheCellAlone is the property that makes deleting
// cheap enough to stack rather than apply, asserted rather than assumed.
//
// It also pins the direction of the whole accounting: a delete must *lower* the
// figure, because the record it masks stops being reachable, and only the cell
// recording that it is gone survives. An implementation that forgot to give the
// record's bytes back would report a delta that only ever grows, which is the
// most plausible way for this counter to be wrong and the least visible.
func TestDeltaBytes_ATombstoneCostsTheCellAlone(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	const blob = 8 << 10
	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
		Properties: bytes.Repeat([]byte{7}, blob),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	live := s.StorageStats().DeltaBytes
	if live < blob {
		t.Fatalf("a node with an %d-byte blob reports %d bytes of delta", blob, live)
	}

	if err := s.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if got := s.StorageStats().DeltaBytes; got != sizeofNodeVersion {
		t.Errorf("after the delete the delta holds %d bytes, want the %d-byte tombstone cell alone",
			got, int64(sizeofNodeVersion))
	}
}

// TestDeltaBytes_AnUpdateReplacesRatherThanAccumulates is the other half of the
// same claim, and it is the one the truncation return value exists for.
//
// Overwriting a record stacks a version and drops the one beneath it, so the
// figure must track the newest version rather than the sum of every version ever
// written. Without the bytes coming back from truncateNodeChain this grows
// without bound under a workload that only ever updates — which is the consumer's
// workload.
func TestDeltaBytes_AnUpdateReplacesRatherThanAccumulates(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	const blob = 2 << 10
	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
		Properties: bytes.Repeat([]byte{1}, blob),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	first := s.StorageStats().DeltaBytes

	for i := 0; i < 32; i++ {
		if err := s.UpdateNode(&store.Node{
			ID:         id,
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: bytes.Repeat([]byte{2}, blob),
		}); err != nil {
			t.Fatalf("UpdateNode %d: %v", i, err)
		}
	}
	if got := s.StorageStats().DeltaBytes; got != first {
		t.Errorf("after 32 same-sized updates the delta holds %d bytes, want the %d one record costs",
			got, first)
	}
}

// TestDeltaBytes_VerifyIndexesCatchesDrift is the mutation guard: the counter is
// adjusted on four write paths and given back by one truncation, and the drift
// check in verifyDelta is the only thing that would notice any of them being
// wrong.
//
// Asserted by corrupting the counter directly rather than by mutating a write
// path, because what is being tested is the check and not the paths.
func TestDeltaBytes_VerifyIndexesCatchesDrift(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	for i := 0; i < 8; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes on a healthy store: %v", err)
	}

	s.mu.Lock()
	s.cur().delta.bytes += 1024
	s.mu.Unlock()

	err := s.VerifyIndexes()
	if err == nil {
		t.Fatal("VerifyIndexes accepted a delta whose byte counter is 1024 too high")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("byte counter drifted")) {
		t.Errorf("the error does not name the drift: %v", err)
	}
}

// TestEstimateResident_MappingMovesTheBlobsOutOfTheHeap is the item ImageMapped
// exists for, asserted from outside as a difference between two opens of one
// image.
//
// It is deliberately not written as "Mapped is not inside Total". That claim is
// already settled by Total equalling the sum of its named terms, and an attempt to
// state it again as an inequality produced branches that could not fail. What is
// worth proving is the observable consequence: the same image, opened mapped and
// opened on the heap, must differ by very nearly the blob bytes, and it must
// differ in the direction that makes the mapped open cheaper.
//
// The blobs are large and the records few so that one term dominates the
// comparison. With a hundred records carrying 32 KiB each, the heap open is
// holding some 3 MiB the mapped open is not, against per-record costs in the tens
// of bytes — so the assertion is about the blobs and not about the constants
// around them.
func TestEstimateResident_MappingMovesTheBlobsOutOfTheHeap(t *testing.T) {
	const (
		records = 100
		blob    = 32 << 10
	)
	s, dir := openFresh(t)
	payload := bytes.Repeat([]byte{0xC3}, blob)
	for i := 0; i < records; i++ {
		id, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: payload,
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.IndexNodeProperty(id, "k", []byte("v")); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Reopened rather than measured in place: a graph a compaction published
	// reports what its records reference, and what this test is about is what a
	// loader allocates. See CSRGraph.payloadBytes.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	measure := func(t *testing.T, mode ImageMode) ResidentEstimate {
		t.Helper()
		r, err := OpenWithOptions(dir, Options{ImageMode: mode})
		if err != nil {
			t.Fatalf("open %s: %v", mode, err)
		}
		defer r.Close()
		est := r.EstimateResident()
		sum := est.RecordArrays + est.Payload + est.LabelPostings +
			est.Adjacency + est.Index + est.Delta + est.WAL
		if est.Total != sum {
			t.Errorf("%s: Total is %d, its terms sum to %d — a term is missing from the sum",
				mode, est.Total, sum)
		}
		if est.RecordArrays == 0 {
			t.Errorf("%s: an image of %d records reports no record arrays", mode, records)
		}
		if est.Index == 0 {
			t.Errorf("%s: %d indexed entries report no index", mode, records)
		}
		return est
	}

	heap := measure(t, ImageHeap)
	mapped := measure(t, ImageMapped)
	if mapped.Mapped == 0 {
		t.Skipf("this platform fell back to the heap, so there is no mapping to compare")
	}
	if heap.Mapped != 0 {
		t.Errorf("a heap image reports %d mapped bytes", heap.Mapped)
	}

	const blobBytes = int64(records) * blob
	if saved := heap.Total - mapped.Total; saved < blobBytes*9/10 {
		t.Errorf("mapping the image saved %d bytes of heap, want most of the %d of blob\n"+
			"heap: %+v\nmapped: %+v", saved, blobBytes, heap, mapped)
	}
	if mapped.Payload >= heap.Payload {
		t.Errorf("the mapped open holds %d bytes of payload against the heap open's %d",
			mapped.Payload, heap.Payload)
	}
}

// TestEstimateResident_LabelPostingsCountPostingsNotRecords is why that term loops
// over distinct labels instead of multiplying a record count.
//
// Nothing else in the suite would notice it disappearing. The band and slope checks
// compare two sizes of one store, so an error proportional to the record count
// moves both ends and cancels in the difference — and at 8 bytes per (record,
// label) this term is under a tenth of the total, far inside a band set to catch a
// doubling.
//
// What separates the right model from the plausible wrong one is multiplicity. A
// record carrying three labels sits in three postings; a model derived from the
// record count says one. Two stores with identical record counts and different
// label counts is the one comparison that tells them apart, so that is the test.
func TestEstimateResident_LabelPostingsCountPostingsNotRecords(t *testing.T) {
	const records = 400

	build := func(t *testing.T, labels []store.NodeType) int64 {
		t.Helper()
		s, dir := openFresh(t)
		for i := 0; i < records; i++ {
			if _, err := s.AddNode(&store.Node{Labels: labels}); err != nil {
				t.Fatalf("AddNode: %v", err)
			}
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		r, err := Open(dir)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer r.Close()
		return r.EstimateResident().LabelPostings
	}

	one := build(t, []store.NodeType{store.NodeTypeEvidenceFile})
	three := build(t, []store.NodeType{
		store.NodeTypeEvidenceFile,
		store.NodeTypeMicroArtefact,
		store.NodeTypeTag,
	})

	// Three labels per record is three postings per record, so the ids alone
	// account for a difference of 2 x 8 x records. Asserted as "most of" rather
	// than exactly, because the three-label store also carries two more map
	// entries and the model charges for those too.
	want := int64(2 * sizeofNodeID * records)
	if got := three - one; got < want*9/10 {
		t.Errorf("tripling the labels on %d records moved the postings term by %d B, "+
			"want most of %d — the term is being derived from the record count "+
			"rather than from the postings",
			records, got, want)
	}
}

// TestEstimateResident_CompactionMovesTheDeltaIntoTheImage is the shape an
// operator is being asked to watch, asserted as a shape rather than as numbers.
//
// Before a compaction the delta carries the records; after it the image does and
// the delta is empty. Any absolute figure here would be a restatement of the
// constants; what is worth pinning is that the two terms move in opposite
// directions and that the delta term actually reaches bottom, because a delta
// that never quite empties is the signature of a counter that leaks.
func TestEstimateResident_CompactionMovesTheDeltaIntoTheImage(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	payload := bytes.Repeat([]byte{0x5A}, 4<<10)
	for i := 0; i < 128; i++ {
		if _, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: payload,
		}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	before := s.EstimateResident()
	if before.Delta < 128*4<<10 {
		t.Fatalf("the delta holds %d bytes, less than the %d of blob written",
			before.Delta, 128*4<<10)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := s.EstimateResident()
	if after.Delta != 0 {
		t.Errorf("after a compaction the delta holds %d bytes, want none", after.Delta)
	}
	if after.Payload <= before.Payload {
		t.Errorf("the image's payload did not grow across the compaction: %d then %d",
			before.Payload, after.Payload)
	}
}

// TestEstimateResident_AdjacencyIsZeroWhileDeferred pins the one term whose whole
// purpose is to be absent, and pins it against the mode that makes it absent
// rather than against a number.
//
// R4's item is that a read-only pass need not pay for adjacency at all. If the
// estimate charged for it anyway the store would look identical in both modes,
// and the option would be unmeasurable from outside — which is the same failure
// as not having implemented it.
func TestEstimateResident_AdjacencyIsZeroWhileDeferred(t *testing.T) {
	s, dir := openFresh(t)
	a := addNodeD(t, s, store.NodeTypeEvidenceFile)
	b := addNodeD(t, s, store.NodeTypeEvidenceFile)
	addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lazy, err := OpenWithOptions(dir, Options{Adjacency: AdjacencyLazy})
	if err != nil {
		t.Fatalf("open lazy: %v", err)
	}
	defer lazy.Close()
	if got := lazy.EstimateResident().Adjacency; got != 0 {
		t.Errorf("a deferred adjacency is charged %d bytes", got)
	}

	// Touching a degree builds it, and the estimate has to notice.
	if _, err := lazy.DegreeOf(a, store.DirectionOutbound, nil); err != nil {
		t.Fatalf("DegreeOf: %v", err)
	}
	if got := lazy.EstimateResident().Adjacency; got == 0 {
		t.Error("adjacency was built and is still charged nothing")
	}
}
