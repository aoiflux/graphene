package disk

import (
	"bytes"
	"fmt"
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

// TestDeltaBytes_AccountsForVersionsASnapshotPins is the state the suite otherwise
// never reaches, and the reason the drift check walks chains rather than heads.
//
// A version chain is one version long almost always: truncateNodeChain drops
// everything below the head unless a reader could still be behind it. So every
// other test in this package exercises a delta where "the heads" and "every
// version" are the same set, and a drift check restricted to heads passes all of
// them — which a mutation restricting it proved by surviving.
//
// A snapshot is what makes them differ. It pins its epoch, so an update written
// after it has to keep the version the snapshot reads as well as the new one, and
// the counter has to account for both. That is not an accounting curiosity: it is
// the memory an abandoned snapshot pins, the thing StorageStats.OpenSnapshots exists
// to explain, and a byte figure that ignored it would under-report exactly the case
// an operator is looking for.
//
// The assertion is that a healthy store with a retained chain verifies clean. Under
// the mutation the recomputation comes up short by the pinned version's bytes and
// VerifyIndexes reports drift on a store that has none, which is the failure worth
// having: a check that cries wolf about correct state is as useless as one that is
// silent about wrong state.
func TestDeltaBytes_AccountsForVersionsASnapshotPins(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	const blob = 4 << 10
	id, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
		Properties: bytes.Repeat([]byte{1}, blob),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	oneVersion := s.StorageStats().DeltaBytes

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Close()

	// Written after the snapshot, so the version it reads cannot be dropped.
	if err := s.UpdateNode(&store.Node{
		ID:         id,
		Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
		Properties: bytes.Repeat([]byte{2}, blob),
	}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	twoVersions := s.StorageStats().DeltaBytes
	if twoVersions <= oneVersion {
		t.Fatalf("a pinned chain of two versions reports %d bytes against one version's %d; "+
			"the fixture did not reach the state this test is for", twoVersions, oneVersion)
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Errorf("VerifyIndexes on a healthy store holding a pinned version: %v", err)
	}
}

// TestEstimateResident_AdjacencyReadIsSynchronisedWithItsBuild is the other half of
// the adjacency term, and it is a race test rather than a value test.
//
// TestEstimateResident_AdjacencyIsZeroWhileDeferred proves the figure is right. It
// cannot prove the read is *safe*, and the two are separate properties here for a
// specific reason: under AdjacencyLazy the four slices are nil until the build, so
// deleting the adjBuilt.Load leaves every number this package reports unchanged and
// every test passing. A mutation deleting it survived on exactly that.
//
// What the Load is doing is not deciding a value, it is acquiring. buildAdjacency
// assembles the arrays in locals and assigns the four slice headers at the end,
// precisely so that a reader who has not seen adjBuilt cannot see a half-built one;
// the Load is the read that pairs with that Store. Reading the headers without it is
// unsynchronised with the assignment, whatever the resulting number happens to be.
//
// So the test needs a builder running concurrently with a reader, and it needs
// -race: the detector reports an unsynchronised pair whether or not the two accesses
// overlap in time, where without it the outcome is a torn slice header nobody
// observes. This is the same shape as the store lock's two tests in stream_test.go,
// and it is here for the same reason — proving a figure is right is not proving it
// was safe to read.
func TestEstimateResident_AdjacencyReadIsSynchronisedWithItsBuild(t *testing.T) {
	s, dir := openFresh(t)
	ids := make([]store.NodeID, 0, 64)
	for i := 0; i < 64; i++ {
		ids = append(ids, addNodeD(t, s, store.NodeTypeEvidenceFile))
	}
	for i := 1; i < len(ids); i++ {
		addEdgeD(t, s, ids[i-1], ids[i], store.EdgeTypeSimilarTo)
	}
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

	// The estimate is read in a loop for the whole life of the build, because the
	// window this is about is the instant the four slice headers are assigned and
	// one sample would have to land inside it.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if est := lazy.EstimateResident(); est.Total < 0 {
				panic("negative total")
			}
		}
	}()

	if _, err := lazy.DegreeOf(ids[0], store.DirectionOutbound, nil); err != nil {
		t.Fatalf("DegreeOf: %v", err)
	}
	close(stop)
	<-done

	if got := lazy.EstimateResident().Adjacency; got == 0 {
		t.Error("adjacency was built and is still charged nothing")
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
			est.Adjacency + est.Index + est.Composite + est.Delta + est.WAL
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

// TestEstimateResident_AMappedIndexDoesNotScaleWithItsEntries is R3's claim,
// asserted where it is made.
//
// "The property index costs 236 bytes for 64,000 entries, against the 6.85 MB it
// would cost held resident" is the figure this change puts in the changelog, in
// MEMORY_MODEL §6.9 and in TECHNICAL_DETAILS §14.25. It was measured by hand and
// asserted nowhere, which a mutation adding a per-entry term to gpixBase.ResidentBytes
// demonstrated by surviving the entire package.
//
// index/resident_test.go does check that a base is not charged by its entries, but
// against a fake — so it covers baseResidentBytes' dispatch and not the real
// implementation's arithmetic. The two are different claims and this is the one the
// documents rest on.
//
// Shaped as a differential at a fixed key count, because that is the shape of the
// claim: the resident cost is a function of how many keys are declared and of nothing
// else, so multiplying the entries by ten must not move it. An absolute figure would
// restate the constants and would pass unchanged if the term started scaling.
func TestEstimateResident_AMappedIndexDoesNotScaleWithItsEntries(t *testing.T) {
	indexTerm := func(t *testing.T, records int) (int64, int) {
		t.Helper()
		s, dir := openFresh(t)
		for i := 0; i < records; i++ {
			id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			// Two keys, both with every value distinct, so the entry count rises
			// with the record count and the key count does not.
			if err := s.IndexNodeProperty(id, "sha256", []byte(fmt.Sprintf("h-%08d", i))); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
			if err := s.IndexNodeProperty(id, "path", []byte(fmt.Sprintf("/p/%08d", i))); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
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
		if r.indexHolding() != IndexMapped.String() {
			t.Skipf("the reopened index is %s, so there is no mapped base to measure",
				r.indexHolding())
		}
		st := r.StorageStats()
		return r.EstimateResident().Index, st.PropertyNodeEntries
	}

	small, smallEntries := indexTerm(t, 200)
	large, largeEntries := indexTerm(t, 2_000)
	t.Logf("%d entries: %d B; %d entries: %d B", smallEntries, small, largeEntries, large)

	if largeEntries < smallEntries*5 {
		t.Fatalf("the fixture did not multiply the entries: %d then %d",
			smallEntries, largeEntries)
	}
	if large != small {
		t.Errorf("ten times the entries moved the index term from %d B to %d B\n"+
			"a mapped base costs its key directory and nothing per entry: that is the "+
			"whole of R3, and the figure quoted in MEMORY_MODEL §6.9 depends on it",
			small, large)
	}
	// And the saving has to be worth the design. At two keys over 4,000 entries the
	// resident equivalent is hundreds of kilobytes against a directory in the
	// hundreds of bytes; asserting two orders of magnitude leaves room for the
	// constants to move without making the test a restatement of them.
	resident := int64(largeEntries) * 107
	if large*100 > resident {
		t.Errorf("the mapped index costs %d B where a resident one would cost about %d B; "+
			"that is under two orders of magnitude and not the item that was shipped",
			large, resident)
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

// TestEstimateResident_MappedCountsWhatTheStoreOwns is the assertion whose
// absence was the bug.
//
// Until v0.8.0 the mapped figures were read off the live graph: est.Mapped was
// csr.imgBytes, and StorageStats took its byte column from imageHolding. Both
// answer "is what a caller reads coming out of a file", and after a compaction
// the answer is no, because the graph on top was built in memory. The mapping is
// still held — it is not released until Close, because every blob the compaction
// carried forward still addresses it — so the store went on paying for a file it
// reported nothing for.
//
// Nothing in the suite noticed, because every mapping assertion in it was taken
// on a freshly opened store, where the two questions have the same answer. The
// compaction is the whole test.
func TestEstimateResident_MappedCountsWhatTheStoreOwns(t *testing.T) {
	dir := mapFixture(t, 200)
	s, err := OpenWithOptions(dir, Options{ImageMode: ImageMapped})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	atOpen := s.EstimateResident()
	if atOpen.Mapped == 0 {
		t.Skip("this platform fell back to the heap, so there is no mapping to account for")
	}
	if st := s.StorageStats(); st.ImageMappedBytes != atOpen.Mapped {
		t.Errorf("at open, StorageStats says %d mapped image bytes and EstimateResident says %d",
			st.ImageMappedBytes, atOpen.Mapped)
	}

	if _, err := s.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
		Properties: []byte("after"),
	}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// The read path is honestly on the heap now. That is imageHolding answering
	// its own question, and it is not what is being tested.
	after := s.EstimateResident()
	if after.Mapped == 0 {
		t.Errorf("after compacting, the store reports 0 mapped bytes while still holding "+
			"the mapping it opened with (%d bytes at open); this is the under-report "+
			"v0.8.0 fixed, and it is the largest single thing the process is paying for",
			atOpen.Mapped)
	}
	if st := s.StorageStats(); st.ImageMappedBytes != after.Mapped {
		t.Errorf("after compacting, StorageStats says %d mapped image bytes and "+
			"EstimateResident says %d — the two must not diverge",
			st.ImageMappedBytes, after.Mapped)
	}

	// And the mode is still the read-path answer, deliberately. Asserted so that
	// a later change which "fixes" the apparent contradiction between
	// ImageMode:"heap" and a non-zero byte count has to come here and read why.
	if st := s.StorageStats(); st.ImageMode != ImageHeap.String() {
		t.Errorf("after compacting, ImageMode is %q; it reports what the read path is "+
			"served from, which is a graph built in memory", st.ImageMode)
	}
}

// TestEstimateResident_MappedIndexCountsTheBase covers the mapping that was
// counted nowhere at all.
//
// gpix_base.go said those bytes "are reported as ImageMappedBytes instead", and
// they were not: that figure came from the mapping the graph reads, while a base
// is read through its own, held in a separate list with a separate lifetime.
// Under IndexMapped at the scale the mode exists for, this is the largest file
// the store has open and every byte figure it published read zero.
func TestEstimateResident_MappedIndexCountsTheBase(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 500; i++ {
		id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.IndexNodeProperty(id, "k", []byte(fmt.Sprintf("v%04d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenWithOptions(dir, Options{ImageMode: ImageMapped, IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r.Close()

	st := r.StorageStats()
	if st.IndexMode != "mapped" {
		t.Skipf("the index did not map (%q); there is no base mapping to account for", st.IndexMode)
	}
	est := r.EstimateResident()
	if est.MappedIndex == 0 {
		t.Error("the index is mapped and the store reports 0 mapped index bytes")
	}
	if st.IndexMappedBytes != est.MappedIndex {
		t.Errorf("StorageStats says %d mapped index bytes and EstimateResident says %d",
			st.IndexMappedBytes, est.MappedIndex)
	}
	// A subset, not a second file. The index is two sections of the image, so it
	// has to be smaller than what is mapped and larger than nothing — a figure
	// equal to Mapped would mean the section lengths had been replaced by the
	// mapping length somewhere.
	if est.MappedIndex >= est.Mapped {
		t.Errorf("the index reports %d mapped bytes out of %d mapped in total; it is a "+
			"pair of sections inside the image and cannot be the whole of it",
			est.MappedIndex, est.Mapped)
	}
}

// TestStorageStats_AMappedModeNeverReportsZeroBytes is the consistency check
// across the four fields, run over every configuration that can produce them.
//
// They disagreed before v0.8.0 in a way no single-field assertion could catch:
// IndexMode said "mapped" while every byte figure in the struct said zero, which
// is not two right answers to two questions but one of them missing. This pins
// the pairing rather than the values.
func TestStorageStats_AMappedModeNeverReportsZeroBytes(t *testing.T) {
	dir := mapFixture(t, 120)
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"defaults", Options{}},
		{"mapped image, mapped index", Options{ImageMode: ImageMapped, IndexMode: IndexMapped}},
		{"mapped image, resident index", Options{ImageMode: ImageMapped, IndexMode: IndexResident}},
		{"heap image", Options{ImageMode: ImageHeap}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenWithOptions(dir, tc.opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer s.Close()
			st := s.StorageStats()
			if st.IndexMode == "mapped" && st.IndexMappedBytes == 0 {
				t.Error("IndexMode is mapped and IndexMappedBytes is 0")
			}
			if st.IndexMode != "mapped" && st.IndexMappedBytes != 0 {
				t.Errorf("IndexMode is %q and IndexMappedBytes is %d",
					st.IndexMode, st.IndexMappedBytes)
			}
			if st.IndexMappedBytes > st.ImageMappedBytes {
				t.Errorf("the index reports %d mapped bytes out of %d mapped in total; "+
					"it is a subset", st.IndexMappedBytes, st.ImageMappedBytes)
			}
			// One direction only for the image. A mapped mode implies bytes, but
			// bytes do not imply the mode: see the compaction case above.
			if st.ImageMode == "mapped" && st.ImageMappedBytes == 0 {
				t.Error("ImageMode is mapped and ImageMappedBytes is 0")
			}
		})
	}
}

// TestEstimateResident_CompositeIsTakenOutOfIndex is G7a: the term a caller
// changes by declaring less rather than by writing less.
//
// Two assertions, and the second is the one that matters. That Composite is
// non-zero when a composite is declared is arithmetic; that it was taken *out* of
// Index rather than left inside it is the claim the field makes about itself, and
// it is what stops a reader adding the two and double-counting.
func TestEstimateResident_CompositeIsTakenOutOfIndex(t *testing.T) {
	build := func(t *testing.T, composite bool) ResidentEstimate {
		t.Helper()
		s, _ := openFresh(t)
		defer s.Close()
		if composite {
			if err := s.DeclareCompositeNodeProperties([]string{"a", "b"}); err != nil {
				t.Fatalf("DeclareCompositeNodeProperties: %v", err)
			}
		}
		for i := 0; i < 300; i++ {
			id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if err := s.IndexNodeProperty(id, "a", []byte(fmt.Sprintf("a%02d", i%16))); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
			if err := s.IndexNodeProperty(id, "b", []byte(fmt.Sprintf("b%02d", i%8))); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
		}
		return s.EstimateResident()
	}

	plain := build(t, false)
	if plain.Composite != 0 {
		t.Errorf("a store with no composite declared reports %d composite bytes", plain.Composite)
	}

	withComposite := build(t, true)
	if withComposite.Composite == 0 {
		t.Fatal("a declared composite over 300 entities reports 0 bytes")
	}
	// The shards hold the same entries in both, so declaring a composite must
	// leave Index where it was and put the whole difference in Composite.
	if withComposite.Index != plain.Index {
		t.Errorf("Index went from %d to %d when a composite was declared; the bytes are "+
			"landing in Index as well as in Composite (%d)",
			plain.Index, withComposite.Index, withComposite.Composite)
	}
	if got := withComposite.Total - plain.Total; got != withComposite.Composite {
		t.Errorf("declaring the composite moved Total by %d and Composite reports %d; "+
			"Composite is supposed to be in Total and nothing else moved", got,
			withComposite.Composite)
	}
}
