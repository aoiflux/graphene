package disk

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// Refusing an operation for its size, which is the only form "fail predictably
// under memory pressure" can take in a language with no catchable allocation
// failure.
//
// Two properties are asserted throughout and they pull in opposite directions.
// The model must never admit work that will not fit -- that is the posture
// estimate.go states and the reason the surface exists. And it must not refuse
// work that would have fit by a wide margin, because a budget that declines
// everything is indistinguishable from a store that cannot be opened, and a
// caller would remove it. Every test below is one or the other.

// --- the error's own contract ---

func TestBudget_ErrorCarriesTheArithmetic(t *testing.T) {
	err := budgetRefusal("compact", 300, 700, 800)
	if err == nil {
		t.Fatal("300 + 700 over a budget of 800 was admitted")
	}
	if !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("errors.Is(err, ErrMemoryBudget) is false for %v", err)
	}
	var mbe *MemoryBudgetError
	if !errors.As(err, &mbe) {
		t.Fatalf("errors.As to *MemoryBudgetError failed for %v", err)
	}
	if mbe.Need != 300 || mbe.Have != 700 || mbe.Budget != 800 || mbe.Op != "compact" {
		t.Errorf("figures did not survive the refusal: %+v", mbe)
	}
	if got, want := mbe.Over(), int64(200); got != want {
		t.Errorf("Over() = %d, want %d: the figure to raise the budget by has to "+
			"agree with the comparison that produced the refusal", got, want)
	}
	// Strict, so a caller can set the budget to a figure this package reported
	// and have it admitted. The equal case is the only one that use has.
	if err := budgetRefusal("open", 800, 0, 800); err != nil {
		t.Errorf("a need of exactly the budget was refused: %v", err)
	}
	// And off by default, which is the whole compatibility story.
	if err := budgetRefusal("open", 1<<40, 0, 0); err != nil {
		t.Errorf("a zero budget refused something: %v", err)
	}
	if err := budgetRefusal("open", 1<<40, 0, -1); err != nil {
		t.Errorf("a negative budget refused something: %v", err)
	}
}

// --- the Open side ---

// A refused Open must leave a store that opens. The refusal is meant to be a
// routine, repeatable outcome, so the one thing it may not do is consume the
// store it declined to read.
func TestBudget_RefusedOpenLeavesTheStoreOpenable(t *testing.T) {
	dir := modelFixture(t, Options{}, 400, 0)

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	need := est.HeapBytesFor(Options{})
	if need <= 0 {
		t.Fatalf("the fixture models at %d bytes, so there is nothing to refuse", need)
	}

	_, err = OpenWithOptions(dir, Options{MemoryBudget: need - 1})
	if !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("Open under a budget of %d against a need of %d: got %v", need-1, need, err)
	}
	var mbe *MemoryBudgetError
	if !errors.As(err, &mbe) {
		t.Fatalf("the refusal did not carry its figures: %v", err)
	}
	if mbe.Op != "open" {
		t.Errorf("Op = %q, want \"open\"", mbe.Op)
	}
	if mbe.Have != 0 {
		t.Errorf("Have = %d, want 0: nothing is held before an open", mbe.Have)
	}

	// Twice, because a refusal that half-consumed the log would pass once.
	if _, err := OpenWithOptions(dir, Options{MemoryBudget: need - 1}); !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("the second refusal differed from the first: %v", err)
	}

	s, err := OpenWithOptions(dir, Options{MemoryBudget: need})
	if err != nil {
		t.Fatalf("Open at exactly the modelled need was refused: %v", err)
	}
	defer s.Close()
	if got := s.StorageStats().CSRNodes; got != 400 {
		t.Errorf("the store admitted after two refusals holds %d nodes, want 400", got)
	}
}

// The figure gated on has to be the figure for the Options being opened with.
//
// This is the reason HeapBytesFor exists rather than ImageHeapBytes being used
// directly, and the margin is not a rounding: the same store is more than twice
// as expensive under a heap image with a resident index. A gate on the worst case
// would refuse the default open of a store that fits it easily.
func TestBudget_OpenGatesOnTheOptionsAsked(t *testing.T) {
	dir := modelFixture(t, Options{}, 400, 0)
	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}

	lean := Options{}
	rich := Options{ImageMode: ImageHeap, IndexMode: IndexResident}
	needLean, needRich := est.HeapBytesFor(lean), est.HeapBytesFor(rich)
	if needRich <= needLean {
		t.Fatalf("the two configurations model at %d and %d, so this fixture "+
			"cannot tell a per-Options gate from a worst-case one", needLean, needRich)
	}

	// A budget between the two: the lean open is admitted and the rich one is not.
	budget := (needLean + needRich) / 2
	lean.MemoryBudget, rich.MemoryBudget = budget, budget

	s, err := OpenWithOptions(dir, lean)
	if err != nil {
		t.Fatalf("the cheaper configuration was refused by a budget of %d against "+
			"a need of %d: %v", budget, needLean, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithOptions(dir, rich); !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("the more expensive configuration was admitted by a budget of %d "+
			"against a need of %d: %v", budget, needRich, err)
	}
}

// A store that has never been compacted is the shape the whole surface exists
// for: no image, so every image term is zero, and a log that is the entire write
// history replayed into memory on every open.
func TestBudget_NeverCompactedStoreIsGated(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 400; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%06d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	est, err := PreflightOpen(dir)
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if est.ImageBytes != 0 {
		t.Fatalf("the fixture was compacted, so it is not the case under test")
	}
	need := est.HeapBytesFor(Options{})
	if need <= 0 {
		t.Fatal("an uncompacted store of 400 indexed nodes modelled at nothing")
	}
	if _, err := OpenWithOptions(dir, Options{MemoryBudget: need - 1}); !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("a budget of %d against a need of %d admitted the open: %v", need-1, need, err)
	}
}

// --- the Compact side ---

// The load-bearing test: the model against what the build actually produced.
//
// Everything else here asserts that the comparison is wired up. This asserts the
// figure being compared is true, by pinning, taking the model, running the real
// build, and measuring the image that came out of it. A model that only had to
// agree with itself would pass every other test in this file.
func TestBudget_CompactModelBoundsTheBuild(t *testing.T) {
	for _, tc := range []struct {
		name     string
		burn     int
		blob     int
		postBurn int
		write    Options
	}{
		{"dense", 0, 64, 0, Options{}},
		{"sparse", 12000, 64, 0, Options{}},
		{"blobs", 0, 2048, 0, Options{}},
		{"heap image", 0, 64, 0, Options{ImageMode: ImageHeap, IndexMode: IndexResident}},
		// The rebuild shape: the delta is written to a band of identifiers the
		// image does not touch, which is the only arm in which the image's own
		// page count does not already cover the delta's pages.
		{"delta beyond the image", 0, 64, 9000, Options{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := budgetFixtureAt(t, tc.write, 300, tc.burn, tc.blob, tc.postBurn)
			defer s.Close()

			plan, err := s.compactPin()
			if err != nil {
				t.Fatalf("compactPin: %v", err)
			}
			model := plan.compactWorkingSet()

			newCSR, tmpPath, err := plan.build(t.Context(), s.dir)
			if err != nil {
				s.compactRelease()
				t.Fatalf("build: %v", err)
			}
			removeTmpImage(tmpPath)
			s.compactRelease()

			// What the build actually holds that the store did not already: the
			// new image's structure, plus the plan's own copies. The payload is
			// deliberately absent from both sides -- buildSeq copies slice
			// headers, so the bytes are the pinned image's and the plan's, and
			// counting them here would be measuring the claim rather than
			// testing it. That claim is its own test below.
			actual := newCSR.recordArrayBytes() +
				newCSR.labelPostingBytes() +
				newCSR.adjacencyBytes() +
				int64(len(plan.deltaNodes))*sizeofNodeRecord +
				int64(len(plan.deltaEdges))*sizeofRawEdge +
				int64(len(plan.deltaKnownNodes))*sizeofNodeID +
				int64(len(plan.deltaKnownEdges))*sizeofEdgeID +
				plan.deltaBytes

			if model < actual {
				t.Errorf("model %d B is below the %d B the build produced: a budget "+
					"gate on this figure admits a compaction that will not fit, "+
					"which is the one direction estimate.go forbids", model, actual)
			}
			// And not uselessly high. A model returning MaxInt64 satisfies the
			// line above and would refuse every compaction ever attempted.
			//
			// The band is on the part of the model that follows the store. The
			// mapped index's intermediates are a constant the build genuinely
			// holds -- 4.125 MiB at the default -- and on a fixture this size
			// they are most of the total, so a ratio taken over them would say
			// six times and mean "the fixture is small". Charged as a constant on
			// both sides, and the constant itself is tested where it is divided.
			var buffers int64
			if plan.indexMode == IndexMapped {
				buffers = plan.buffers.workingBytes()
			}
			const band = 3
			if model > actual*band+buffers {
				t.Errorf("model %d B is %.2fx the %d B produced with %d B of "+
					"intermediates allowed, past the %dx band: a gate this loose "+
					"refuses compactions that would have fit",
					model, float64(model-buffers)/float64(actual), actual, buffers, band)
			}
			t.Logf("model %d B (%d B of it intermediates), produced %d B, ratio %.2fx",
				model, buffers, actual, float64(model-buffers)/float64(actual))
		})
	}
}

// A compaction does not duplicate the property blobs, at any size, because
// buildSeq copies record values and a record value is two slice headers.
//
// This is the whole reason the model is not "twice the image", and it is worth a
// guard of its own: charging the blobs would be wrong in the permitted direction
// and so would survive the bound above, while making the budget refuse a
// blob-carrying store by a factor of two.
func TestBudget_CompactModelDoesNotFollowTheBlobBytes(t *testing.T) {
	const small, large = 32, 4096

	measure := func(blob int) (int64, int64) {
		t.Helper()
		s := budgetFixture(t, Options{}, 300, 0, blob)
		defer s.Close()
		plan, err := s.compactPin()
		if err != nil {
			t.Fatalf("compactPin: %v", err)
		}
		defer s.compactRelease()
		var payload int64
		if plan.csr != nil {
			payload = plan.csr.payloadBytes
		}
		return plan.compactWorkingSet(), payload
	}

	thinWS, thinPayload := measure(small)
	fatWS, fatPayload := measure(large)

	grew := fatPayload - thinPayload
	if want := int64(300 * (large - small) / 2); grew < want {
		t.Fatalf("the two fixtures' payloads differ by %d B, which is not enough "+
			"blob to tell a per-byte model from a structural one", grew)
	}
	if d := fatWS - thinWS; d > grew/4 {
		t.Errorf("the working set rose by %d B when the image's payload grew by "+
			"%d: the blobs are being charged to a build that aliases them",
			d, grew)
	}
}

// A refused compaction must leave the store exactly as it was: still readable,
// still writable, and with its delta still there to be compacted later.
func TestBudget_RefusedCompactLeavesTheStoreWorking(t *testing.T) {
	s := budgetFixture(t, Options{}, 200, 0, 64)
	defer s.Close()

	before := s.StorageStats()
	s.memBudget = 1 // refuses everything, and is the caller's own setting

	err := s.Compact()
	if !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("Compact under a budget of 1 byte: got %v", err)
	}
	var mbe *MemoryBudgetError
	if !errors.As(err, &mbe) {
		t.Fatalf("the refusal did not carry its figures: %v", err)
	}
	if mbe.Need <= 0 {
		t.Errorf("Need = %d: a compaction of a 200-node store needs something", mbe.Need)
	}

	// Refused twice, identically: a refusal that consumed the pin would leave
	// the second attempt reporting ErrCompactionInProgress instead.
	if err := s.Compact(); !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("the second refusal differed from the first: %v", err)
	}

	after := s.StorageStats()
	if after.DeltaRecords() != before.DeltaRecords() || after.CSRNodes != before.CSRNodes {
		t.Errorf("a refused compaction moved the store: delta %d->%d, image %d->%d",
			before.DeltaRecords(), after.DeltaRecords(), before.CSRNodes, after.CSRNodes)
	}

	// Still a working store on both sides.
	id := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if _, err := s.GetNode(id); err != nil {
		t.Errorf("a write after a refused compaction is unreadable: %v", err)
	}

	// And the refusal is not permanent: it is a budget, so raising it compacts.
	s.memBudget = 0
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact with the budget removed: %v", err)
	}
	// 200 compacted by the fixture, 50 more in the delta it leaves, and the one
	// written above to prove the store still took writes while refusing.
	if got, want := s.StorageStats().CSRNodes, 251; got != want {
		t.Errorf("the compaction that followed the refusals folded %d nodes, want %d",
			got, want)
	}
}

// A refusal is not a compaction that failed, and the metrics stream is where
// that distinction is visible to an operator. ErrCompactionInProgress already
// establishes the rule; this holds the budget to it.
func TestBudget_RefusedCompactIsNotRecordedAsACompaction(t *testing.T) {
	var mu sync.Mutex
	var compactions []store.Metric
	sink := store.MetricsFunc(func(m store.Metric) {
		if m.Kind != store.MetricCompaction {
			return
		}
		mu.Lock()
		compactions = append(compactions, m)
		mu.Unlock()
	})

	s := budgetFixture(t, Options{Metrics: sink}, 200, 0, 64)
	defer s.Close()

	mu.Lock()
	fromTheFixture := len(compactions)
	mu.Unlock()

	s.memBudget = 1
	if err := s.Compact(); !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("Compact: got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(compactions) != fromTheFixture {
		t.Errorf("a budget refusal was recorded as a compaction: %d metrics before, "+
			"%d after. An operator's compaction error rate would then be made "+
			"entirely of the budget working.", fromTheFixture, len(compactions))
	}
}

// AutoCompact is the caller who most needs a refusal to be legible, because it
// is the one who never sees a return value. The observer is where it arrives.
func TestBudget_AutoCompactRefusalReachesTheObserver(t *testing.T) {
	dir := t.TempDir()
	refused := make(chan error, 8)
	policy := store.CompactionPolicy{MaxDeltaRecords: 1}

	// Above what an open of an empty store costs and far below what compacting
	// one does: the point is to admit the handle and refuse the compaction, and
	// a budget small enough to refuse the open would never reach the observer.
	s, err := OpenWithOptions(dir, Options{
		MemoryBudget:        4 * defaultWALRingCapacity * sizeofWALSlot,
		AutoCompact:         &policy,
		AutoCompactInterval: 10 * time.Millisecond,
		AutoCompactObserver: store.CompactionObserverFunc(func(_ string, err error) {
			select {
			case refused <- err:
			default:
			}
		}),
	})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer s.Close()

	for i := 0; i < 5; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}

	select {
	case err := <-refused:
		if !errors.Is(err, ErrMemoryBudget) {
			t.Fatalf("the observer was told %v, want a budget refusal", err)
		}
		var mbe *MemoryBudgetError
		if !errors.As(err, &mbe) {
			t.Fatalf("the observer's error did not carry its figures: %v", err)
		}
		if mbe.Op != "compact" {
			t.Errorf("Op = %q, want \"compact\"", mbe.Op)
		}
		if mbe.Have <= 0 {
			t.Errorf("Have = %d: a compaction is refused against a store that is "+
				"already holding something", mbe.Have)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no refusal reached the observer in 5s")
	}
}

// The default is unlimited, which is the whole compatibility statement for this
// change: a caller who sets nothing sees the behaviour of every version before.
func TestBudget_ZeroIsUnlimited(t *testing.T) {
	s := budgetFixture(t, Options{}, 200, 0, 64)
	defer s.Close()
	if s.memBudget != 0 {
		t.Fatalf("memBudget = %d on an Options that set none", s.memBudget)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact with no budget: %v", err)
	}
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open with no budget: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

// An empty store is not free, and the floor is the log's ring of pending frames.
//
// Worth asserting because it is the one figure a caller cannot derive from
// anything about their data, and because it is what makes a budget below it
// refuse every open including the open of a store with nothing in it. That is
// the model being honest rather than a defect: the ring is allocated.
func TestBudget_EmptyStoreCostsItsLogRing(t *testing.T) {
	const ring = defaultWALRingCapacity * sizeofWALSlot

	est, err := PreflightOpen(t.TempDir())
	if err != nil {
		t.Fatalf("PreflightOpen: %v", err)
	}
	if got := est.HeapBytesFor(Options{}); got != ring {
		t.Errorf("an empty directory models at %d B, want exactly the %d B ring: "+
			"either a term that should be zero is not, or the ring has moved",
			got, ring)
	}

	if _, err := OpenWithOptions(t.TempDir(), Options{MemoryBudget: ring - 1}); !errors.Is(err, ErrMemoryBudget) {
		t.Errorf("a budget one byte under the ring admitted an open: %v", err)
	}
	s, err := OpenWithOptions(t.TempDir(), Options{MemoryBudget: ring})
	if err != nil {
		t.Fatalf("a budget of exactly the ring refused an empty store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// A directory the estimate cannot be taken for must not start failing because a
// budget was set. Every way PreflightOpen can fail is a way the open itself is
// about to fail, and more informatively, so the gate stands aside rather than
// converting a real diagnosis into a budget refusal.
func TestBudget_UnreadableEstimateDoesNotRefuse(t *testing.T) {
	// A file where a store directory should be. PreflightOpen refuses it; the
	// gate has no figure and must admit.
	f := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := PreflightOpen(f); err == nil {
		t.Fatal("PreflightOpen accepted a plain file, so this case is not reachable")
	}
	if err := checkOpenBudget(f, Options{MemoryBudget: 1}); err != nil {
		t.Errorf("the gate refused on a directory it could not estimate: %v", err)
	}
}

// --- the fixture ---

// budgetFixture is a store left open with a compacted image beneath a live
// delta, which is the state every compaction figure here is taken from. blob
// sizes the property payloads, and burn sparsifies the identifier space.
func budgetFixture(t *testing.T, w Options, nodes, burn, blob int) *Store {
	return budgetFixtureAt(t, w, nodes, burn, blob, 0)
}

// budgetFixtureAt is budgetFixture with postBurn identifiers thrown away between
// the compaction and the delta writes, so the delta lands in pages the image does
// not touch.
//
// That separation is the rebuild workload and it is the case the delta-page term
// exists for: without it, a mutation deleting that term survives, because a delta
// written immediately after a compaction shares the image's last page and the
// image's own page count already covers it.
func budgetFixtureAt(t *testing.T, w Options, nodes, burn, blob, postBurn int) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, w)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	payload := bytes.Repeat([]byte("p"), blob)
	for i := 0; i < burn; i++ {
		id, err := s.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeMicroArtefact},
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.DeleteNode(id); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
	}

	var ids []store.NodeID
	for i := 0; i < nodes; i++ {
		id, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
			Properties: payload,
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%06d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		addEdgeD(t, s, ids[i-1], ids[i], store.EdgeTypeContains)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Throw identifiers away after the compaction, so the delta below starts in
	// a page beyond anything the image materialised.
	for i := 0; i < postBurn; i++ {
		id, err := s.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeMicroArtefact},
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.DeleteNode(id); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
	}

	// A delta on top, so the plan has something to copy and the working set has
	// a delta term to be wrong about. Every third record carries three labels:
	// a record with three labels is in three postings, and a model charging one
	// per record under-reports exactly the store that declares the most.
	for i := 0; i < nodes/4; i++ {
		labels := []store.NodeType{store.NodeTypeEvidenceFile}
		if i%3 == 0 {
			labels = append(labels, store.NodeTypeMicroArtefact, store.NodeTypeCase)
		}
		id, err := s.AddNode(&store.Node{Labels: labels, Properties: payload})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("d%06d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	return s
}
