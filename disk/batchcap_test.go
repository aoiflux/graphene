package disk

import (
	"errors"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func batchNodes(n, payload int) []*store.Node {
	out := make([]*store.Node, n)
	for i := range out {
		out[i] = &store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: make([]byte, payload),
		}
	}
	return out
}

func batchCapStore(t *testing.T, opts Options) *Store {
	t.Helper()
	s, err := OpenWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// --- the derivation ---

// A caller who turns memory down gets smaller batches without making a second
// decision. This is the property the derivation exists for, and it is why the
// caps come out of the budget rather than sitting beside it.
func TestBatchCapsFor_FollowTheBudgetDown(t *testing.T) {
	big := batchCapsFor(Options{MemoryBudget: 2 << 30})
	small := batchCapsFor(Options{MemoryBudget: 256 << 20})

	if big.bytes <= small.bytes {
		t.Errorf("a larger budget did not yield a larger byte cap: %d vs %d", big.bytes, small.bytes)
	}
	if big.records <= small.records {
		t.Errorf("a larger budget did not yield a larger record cap: %d vs %d", big.records, small.records)
	}
	// And the record cap follows the byte cap, so the two cannot disagree.
	if want := int(big.bytes / minNodeBatchCost); big.records != want {
		t.Errorf("record cap %d is not the byte cap divided by the empty-record cost %d", big.records, want)
	}
}

// No budget, no derived cap. An upgrade must not start refusing batches a
// program has always passed.
func TestBatchCapsFor_NoBudgetCapsNothing(t *testing.T) {
	c := batchCapsFor(Options{})
	if !c.none() {
		t.Errorf("a store with no budget derived caps of %d records / %d bytes", c.records, c.bytes)
	}
	if err := c.checkNodes("AddNodesBatch", batchNodes(100_000, 1024)); err != nil {
		t.Errorf("an uncapped store refused a batch: %v", err)
	}
}

// An explicit cap is used as given, on each dimension independently.
func TestBatchCapsFor_ExplicitCapsWin(t *testing.T) {
	c := batchCapsFor(Options{MemoryBudget: 2 << 30, MaxBatchBytes: 4096, MaxBatchRecords: 7})
	if c.bytes != 4096 || c.records != 7 {
		t.Errorf("explicit caps became %d records / %d bytes", c.records, c.bytes)
	}

	// Only the byte cap set, with a budget to derive the other from.
	half := batchCapsFor(Options{MemoryBudget: 2 << 30, MaxBatchBytes: 8000})
	if half.bytes != 8000 {
		t.Errorf("byte cap = %d, want 8000", half.bytes)
	}
	if want := int(8000 / minNodeBatchCost); half.records != want {
		t.Errorf("record cap = %d, want %d derived from the stated byte cap", half.records, want)
	}
}

// A discovered budget bounds batches too. That is the whole point of deriving:
// a store that found a tight cgroup limit refuses batches a store on a large
// machine accepts, with neither caller writing a figure down.
func TestBatchCapsFor_ADiscoveredBudgetBoundsBatches(t *testing.T) {
	opts, source := resolveMemoryBudget(Options{DiscoverMemoryBudget: true})
	if source == "" {
		t.Skip("this platform discovers no ceiling")
	}
	if c := batchCapsFor(opts); c.none() {
		t.Error("a discovered budget produced no batch cap")
	}
}

// --- the refusal ---

// An oversized batch is refused, and refused having burned nothing: no
// identifier is taken, no record is written, and the store is exactly as a
// caller who never made the call would have found it.
func TestAddNodesBatch_RefusesAnOversizedBatchAndBurnsNothing(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchRecords: 10})

	before := s.StorageStats()
	ids, err := s.AddNodesBatch(batchNodes(50, 16))
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("AddNodesBatch over the record cap: got %v, want ErrBatchTooLarge", err)
	}
	if ids != nil {
		t.Errorf("a refused batch returned %d ids; a refusal is not a partial write", len(ids))
	}

	after := s.StorageStats()
	if after.HighestNodeID != before.HighestNodeID {
		t.Errorf("a refused batch burned identifiers: %d -> %d", before.HighestNodeID, after.HighestNodeID)
	}
	if after.DeltaNodes != before.DeltaNodes {
		t.Errorf("a refused batch wrote %d records", after.DeltaNodes-before.DeltaNodes)
	}
	if after.WALBytes != before.WALBytes {
		t.Errorf("a refused batch appended %d bytes to the log", after.WALBytes-before.WALBytes)
	}

	// And the store still works: a refusal is not a poisoned handle.
	if _, err := s.AddNodesBatch(batchNodes(5, 16)); err != nil {
		t.Errorf("a batch under the cap after a refusal: %v", err)
	}
}

// The byte cap binds independently of the record cap: ten records carrying a
// megabyte each are refused where ten empty ones are not.
func TestAddNodesBatch_ByteCapBindsWhereTheRecordCapDoesNot(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchBytes: 64 << 10, MaxBatchRecords: 1000})

	if _, err := s.AddNodesBatch(batchNodes(10, 32<<10)); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("10 records of 32 KiB against a 64 KiB cap: got %v, want ErrBatchTooLarge", err)
	}
	if _, err := s.AddNodesBatch(batchNodes(10, 16)); err != nil {
		t.Errorf("10 tiny records against the same caps: %v", err)
	}
}

// The record cap binds where the byte cap does not, which is the case a byte cap
// alone cannot catch: empty records carry almost no payload and all of the
// structure.
func TestAddNodesBatch_RecordCapBindsWhereTheByteCapDoesNot(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchBytes: 1 << 30, MaxBatchRecords: 100})

	if _, err := s.AddNodesBatch(batchNodes(1000, 0)); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("1000 empty records against a 100 record cap: got %v, want ErrBatchTooLarge", err)
	}
}

// The refusal carries the arithmetic. A refusal that does not say by how much
// cannot be acted on, and the caller has to be able to split by the dimension
// that actually bound.
func TestBatchTooLargeError_CarriesBothCapsAndBothFigures(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchRecords: 10, MaxBatchBytes: 1 << 20})

	_, err := s.AddNodesBatch(batchNodes(50, 16))
	var e *BatchTooLargeError
	if !errors.As(err, &e) {
		t.Fatalf("got %T, want *BatchTooLargeError", err)
	}
	if e.Op != "AddNodesBatch" {
		t.Errorf("Op = %q", e.Op)
	}
	if e.MaxRecords != 10 || e.MaxBytes != 1<<20 {
		t.Errorf("caps reported as %d records / %d bytes, want 10 / %d", e.MaxRecords, e.MaxBytes, 1<<20)
	}
	// Measurement stops at the crossing rather than walking the whole slice, so
	// the figure is the cap plus one rather than the batch's total.
	if e.Records != 11 {
		t.Errorf("Records = %d, want the first count over the cap (11)", e.Records)
	}
}

func TestAddEdgesBatch_RefusesAnOversizedBatch(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchRecords: 10})

	ids, err := s.AddNodesBatch(batchNodes(2, 8))
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	edges := make([]*store.Edge, 50)
	for i := range edges {
		edges[i] = &store.Edge{
			Src:    ids[0],
			Dst:    ids[1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		}
	}

	before := s.StorageStats()
	if _, err := s.AddEdgesBatch(edges); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("AddEdgesBatch over the record cap: got %v, want ErrBatchTooLarge", err)
	}
	if after := s.StorageStats(); after.HighestEdgeID != before.HighestEdgeID {
		t.Errorf("a refused edge batch burned identifiers: %d -> %d",
			before.HighestEdgeID, after.HighestEdgeID)
	}
}

// --- the Open-time refusal ---

// A cap no batch could ever satisfy is a store nothing can write to, and that is
// worth saying at Open rather than on the first write.
func TestOpen_RefusesABatchCapNoBatchCouldSatisfy(t *testing.T) {
	_, err := OpenWithOptions(t.TempDir(), Options{MaxBatchBytes: 4})
	if !errors.Is(err, ErrBatchCapUnusable) {
		t.Fatalf("open with a 4 byte batch cap: got %v, want ErrBatchCapUnusable", err)
	}
}

// A cap that admits exactly one empty record is unusual and legitimate, and must
// not be swept up by the check above.
func TestOpen_AdmitsACapThatFitsOneRecord(t *testing.T) {
	s, err := OpenWithOptions(t.TempDir(), Options{MaxBatchBytes: minNodeBatchCost})
	if err != nil {
		t.Fatalf("open with a one-record batch cap: %v", err)
	}
	defer s.Close()
}

// --- the splitting adders ---

// The ergonomic path never sees the refusal: a batch far over the caps is
// written as several, and every identifier comes back in order.
func TestAddNodesInBatches_WritesABatchThatWouldBeRefused(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchRecords: 7})

	const total = 100
	if _, err := s.AddNodesBatch(batchNodes(total, 16)); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("the atomic call must still refuse: got %v", err)
	}

	ids, err := s.AddNodesInBatches(batchNodes(total, 16))
	if err != nil {
		t.Fatalf("AddNodesInBatches: %v", err)
	}
	if len(ids) != total {
		t.Fatalf("got %d ids, want %d", len(ids), total)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ids are not in order at %d: %d then %d", i, ids[i-1], ids[i])
		}
	}
	// Every one of them is readable, so the split committed what it claimed.
	for _, id := range ids {
		if _, err := s.GetNode(id); err != nil {
			t.Fatalf("node %d written by a split batch is unreadable: %v", id, err)
		}
	}
}

// A record larger than the byte cap on its own is still attempted rather than
// looping forever on a chunk of zero. It is the largest thing that can be tried,
// so it is what is tried.
func TestAddNodesInBatches_ASingleOversizedRecordStillMakesProgress(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchBytes: 1024})

	nodes := batchNodes(3, 4096)
	ids, err := s.AddNodesInBatches(nodes)
	if err != nil {
		t.Fatalf("AddNodesInBatches with records over the byte cap: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("got %d ids, want 3", len(ids))
	}
}

// The trade, asserted rather than only documented: the split is not atomic and
// AddNodesBatch is. A test that did not pin this would let a later change make
// the two calls identical and take the guarantee away silently.
func TestAddNodesInBatches_IsNotAtomicAndTheAtomicCallStillIs(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchRecords: 5})

	// A batch whose last record is invalid. The atomic call must leave nothing;
	// the splitting call commits the chunks that were already good.
	mixed := batchNodes(12, 8)
	mixed[len(mixed)-1] = &store.Node{Properties: []byte("no labels")}

	// The last five, so the bad record is inside the slice the atomic call sees.
	before := s.StorageStats()
	if _, err := s.AddNodesBatch(mixed[len(mixed)-5:]); err == nil {
		t.Fatal("a batch with an unlabelled node was accepted")
	}
	if after := s.StorageStats(); after.DeltaNodes != before.DeltaNodes {
		t.Errorf("the atomic call wrote %d records before failing; it must write none",
			after.DeltaNodes-before.DeltaNodes)
	}

	ids, err := s.AddNodesInBatches(mixed)
	if err == nil {
		t.Fatal("the splitting call accepted an unlabelled node")
	}
	if len(ids) == 0 {
		t.Fatal("the splitting call returned no identifiers; the chunks before the " +
			"failure were committed and the caller must be told which")
	}
	// What came back is real, which is the whole reason it is returned.
	for _, id := range ids {
		if _, err := s.GetNode(id); err != nil {
			t.Errorf("identifier %d returned by a failed split is unreadable: %v", id, err)
		}
	}
}

func TestAddEdgesInBatches_WritesABatchThatWouldBeRefused(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchRecords: 6})

	nodeIDs, err := s.AddNodesInBatches(batchNodes(2, 8))
	if err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	edges := make([]*store.Edge, 40)
	for i := range edges {
		edges[i] = &store.Edge{
			Src:    nodeIDs[0],
			Dst:    nodeIDs[1],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		}
	}

	ids, err := s.AddEdgesInBatches(edges)
	if err != nil {
		t.Fatalf("AddEdgesInBatches: %v", err)
	}
	if len(ids) != len(edges) {
		t.Fatalf("got %d ids, want %d", len(ids), len(edges))
	}
}

// With no caps there is nothing to split, and the call is one commit.
func TestAddNodesInBatches_UncappedIsASingleCommit(t *testing.T) {
	s := batchCapStore(t, Options{})

	before := s.StorageStats().CommitSeq
	if _, err := s.AddNodesInBatches(batchNodes(500, 32)); err != nil {
		t.Fatalf("AddNodesInBatches: %v", err)
	}
	if got := s.StorageStats().CommitSeq - before; got != 1 {
		t.Errorf("an uncapped store used %d commits for one call, want 1", got)
	}
}

// The caps bound how many records are grouped, not how large a record may be.
// AddNode takes an oversized record without a word, so a batch of exactly one
// must too -- otherwise the same record is writable through one call and not
// through another, which is an inconsistency rather than a bound.
func TestAddNodesBatch_ABatchOfOneIsAlwaysAdmitted(t *testing.T) {
	s := batchCapStore(t, Options{MaxBatchBytes: minNodeBatchCost, MaxBatchRecords: 1})

	if _, err := s.AddNodesBatch(batchNodes(1, 1<<20)); err != nil {
		t.Errorf("a single 1 MiB record against a %d byte cap: %v", minNodeBatchCost, err)
	}
	// Two of them is a batch, and a batch is what the caps bound.
	if _, err := s.AddNodesBatch(batchNodes(2, 1<<20)); !errors.Is(err, ErrBatchTooLarge) {
		t.Errorf("two records against a one-record cap: got %v, want ErrBatchTooLarge", err)
	}
}
