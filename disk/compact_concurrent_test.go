package disk

// What a compaction that runs with the store lock released has to survive.
//
// The three-stage Compact opens a window — between the pin and the commit —
// in which writers run against a store whose image is being rebuilt from an
// older epoch. Everything here is about that window, and every test in it
// fails against a compaction that holds the lock end to end for the trivial
// reason that the window does not exist. The seam is afterPinHook; see its
// doc for why the window is not raced for.
//
// Three distinct ways to lose a commit that lands in the window:
//
//  1. the log is emptied behind it, so it is gone on the next open
//  2. the delta is emptied behind it, so it is gone immediately
//  3. it supersedes a record the new image holds, and the lock-free point read
//     hands back the image's copy
//
// One test each, and each is mutation-checked below.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// TestDenseSlots_FastPathRace runs the lock-free read path against a store
// that is compacting, on a graph whose identifiers span several record pages.
//
// The page table turned one array into three that have to agree — a directory,
// an arena and the offset arrays — and a lookup reads all three without the
// store lock. That is only sound because a CSRGraph is built complete and then
// published as one pointer, never amended in place: no page is materialised
// lazily on a graph a reader can already see. Under -race, a build that
// published early or filled a page after publication is a reported race here
// rather than a wrong answer somewhere else much later.
func TestDenseSlots_FastPathRace(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Enough nodes to cross a page boundary, plus edges so the adjacency
	// arrays are read on the same path.
	const n = csrPageSlots + 500
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i += 64 {
		if _, err := s.AddEdge(&store.Edge{Src: ids[i-1], Dst: ids[i],
			Labels: []store.EdgeType{store.EdgeTypeContains}}); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Delete from the low end so the next image has dead pages behind it and
	// the surviving identifiers are the high ones — the rebuild shape.
	for i := 0; i < 400; i++ {
		if err := s.DeleteNode(ids[i]); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := ids[i%len(ids)]
				i += 7
				// Every answer is either the record or a clean miss; a torn read
				// would be neither.
				if n, err := s.GetNode(id); err == nil && n.ID != id {
					t.Errorf("GetNode(%d) returned node %d", id, n.ID)
					return
				}
				if _, err := s.DegreeOf(id, store.DirectionOutbound, nil); err != nil {
					if _, notFound := err.(*store.ErrNotFound); !notFound {
						t.Errorf("Degree(%d): %v", id, err)
						return
					}
				}
			}
		}(r)
	}

	for c := 0; c < 3; c++ {
		if err := s.Compact(); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("Compact: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

// compactWithWrites compacts s while fn runs inside the build window.
func compactWithWrites(t *testing.T, s *Store, fn func()) {
	t.Helper()
	s.afterPinHook = fn
	defer func() { s.afterPinHook = nil }()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
}

// TestCompact_CommitsDuringBuildSurviveReopen is the data-loss test.
//
// A commit that lands in the window is in the log and in the delta, and in
// neither the image being built nor anything else on disk. Retiring the log
// wholesale — which is what compaction did when nothing could land in the
// window — takes it with no error anywhere and no way to notice until the
// store is reopened.
func TestCompact_CommitsDuringBuildSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var inImage []store.NodeID
	for i := 0; i < 64; i++ {
		inImage = append(inImage, addNodeD(t, s, store.NodeTypeEvidenceFile))
	}

	var inWindow []store.NodeID
	compactWithWrites(t, s, func() {
		for i := 0; i < 16; i++ {
			id := addNodeD(t, s, store.NodeTypeMicroArtefact)
			if err := s.IndexNodeProperty(id, "bucket", []byte("window")); err != nil {
				t.Errorf("IndexNodeProperty: %v", err)
				return
			}
			inWindow = append(inWindow, id)
		}
	})

	// Visible immediately: the delta the compaction published has to carry them.
	for _, id := range inWindow {
		if _, err := s.GetNode(id); err != nil {
			t.Fatalf("node %d committed during the build is not visible after it: %v", id, err)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	for _, id := range inImage {
		if _, err := reopened.GetNode(id); err != nil {
			t.Fatalf("node %d was folded into the image and is gone after reopen: %v", id, err)
		}
	}
	// The log is the only place these existed. If it was retired whole, they are
	// gone here and nowhere else.
	for _, id := range inWindow {
		if _, err := reopened.GetNode(id); err != nil {
			t.Fatalf("node %d committed during the build did not survive reopen: %v", id, err)
		}
	}
	got, err := reopened.NodesByProperty("bucket", []byte("window"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	if len(got) != len(inWindow) {
		t.Fatalf("property entries written during the build: want %d after reopen, got %d", len(inWindow), len(got))
	}

	// And the store still compacts cleanly on top of a log that was rebuilt
	// rather than emptied.
	if err := reopened.Compact(); err != nil {
		t.Fatalf("Compact after a carried tail: %v", err)
	}
	for _, id := range append(append([]store.NodeID{}, inImage...), inWindow...) {
		if _, err := reopened.GetNode(id); err != nil {
			t.Fatalf("node %d lost by the following compaction: %v", id, err)
		}
	}
}

// TestCompact_UpdateDuringBuildBeatsTheImage covers the third loss: a record
// the image holds that a window commit supersedes.
//
// This is the csrShadowed case. The node is in the rebuilt image with its old
// labels; the update lives only in the surviving delta. A point read that takes
// the lock-free path — which it may, the image is freshly published and nothing
// in the counter's own history says otherwise — reads the image's copy unless
// the shadow count was restored when the view was published.
func TestCompact_UpdateDuringBuildBeatsTheImage(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Two shapes of shadowing, because they reach the image by different routes:
	// fromImage is in the CSR before the pin, fromDelta is only in the delta and
	// gets folded in by this compaction. Only the second is invisible to the
	// shadow counter the store maintains as it writes.
	fromImage := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.Compact(); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	fromDelta := addNodeD(t, s, store.NodeTypeEvidenceFile)

	deleted := addNodeD(t, s, store.NodeTypeEvidenceFile)

	compactWithWrites(t, s, func() {
		for _, id := range []store.NodeID{fromImage, fromDelta} {
			if err := s.UpdateNode(&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
				t.Errorf("UpdateNode(%d): %v", id, err)
				return
			}
		}
		if err := s.DeleteNode(deleted); err != nil {
			t.Errorf("DeleteNode: %v", err)
		}
	})

	for _, id := range []store.NodeID{fromImage, fromDelta} {
		n, err := s.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		if !n.HasLabel(store.NodeTypeMicroArtefact) {
			t.Fatalf("node %d: read the image's superseded copy %v, not the update committed during the build",
				id, n.Labels)
		}
	}
	if _, err := s.GetNode(deleted); err == nil {
		t.Fatalf("node %d was deleted during the build and still reads back", deleted)
	}

	// The type postings the splice rebuilt have to agree with the records.
	micro, err := s.NodesByType(store.NodeTypeMicroArtefact)
	if err != nil {
		t.Fatalf("NodesByType: %v", err)
	}
	if len(micro) != 2 {
		t.Fatalf("relabelled during the build: want 2 nodes of the new type, got %d (%v)", len(micro), micro)
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes after a spliced delta: %v", err)
	}
}

// TestCompact_EdgesCommittedDuringBuild checks the half of the splice that has
// a rule of its own: delta adjacency lists only edges the image does not hold,
// so the two counts add. An edge created in the window is not in the image and
// must be listed; an edge the image holds and the window merely updates must
// not be, or every degree over that node is one too many.
func TestCompact_EdgesCommittedDuringBuild(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	hub := addNodeD(t, s, store.NodeTypeEvidenceFile)
	var spokes []store.NodeID
	for i := 0; i < 8; i++ {
		spokes = append(spokes, addNodeD(t, s, store.NodeTypeEvidenceFile))
	}
	// One edge that will be in the image and updated in the window.
	updated := addEdgeD(t, s, hub, spokes[0], store.EdgeTypeContains)

	var created []store.EdgeID
	compactWithWrites(t, s, func() {
		if err := s.UpdateEdge(&store.Edge{
			ID: updated, Src: hub, Dst: spokes[0],
			Labels: []store.EdgeType{store.EdgeTypeReuse},
		}); err != nil {
			t.Errorf("UpdateEdge: %v", err)
			return
		}
		for _, dst := range spokes[1:] {
			created = append(created, addEdgeD(t, s, hub, dst, store.EdgeTypeContains))
		}
	})

	want := 1 + len(created)
	got, err := s.DegreeOf(hub, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatalf("DegreeOf: %v", err)
	}
	if got != want {
		t.Fatalf("hub outbound degree after a spliced adjacency: want %d, got %d", want, got)
	}
	edges, err := s.EdgesOf(hub, store.DirectionOutbound, nil)
	if err != nil {
		t.Fatalf("EdgesOf: %v", err)
	}
	if len(edges) != want {
		t.Fatalf("hub outbound edges: want %d, got %d", want, len(edges))
	}
	seen := make(map[store.EdgeID]int)
	for _, e := range edges {
		seen[e.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("edge %d listed %d times — image adjacency and delta adjacency both claim it", id, n)
		}
	}
	e, err := s.GetEdge(updated)
	if err != nil {
		t.Fatalf("GetEdge: %v", err)
	}
	if !e.HasLabel(store.EdgeTypeReuse) {
		t.Fatalf("edge %d: read the image's superseded copy %v", updated, e.Labels)
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
}

// TestCompact_SnapshotOpenedBeforeIsUnmoved checks that the splice does not
// reach back into the layer an older reader is holding. The surviving heads are
// copied rather than relinked for exactly this reason.
func TestCompact_SnapshotOpenedBeforeIsUnmoved(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	id := addNodeD(t, s, store.NodeTypeEvidenceFile)
	gone := addNodeD(t, s, store.NodeTypeEvidenceFile)

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Close()

	compactWithWrites(t, s, func() {
		if err := s.UpdateNode(&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
			t.Errorf("UpdateNode: %v", err)
		}
		if err := s.DeleteNode(gone); err != nil {
			t.Errorf("DeleteNode: %v", err)
		}
	})

	n, err := snap.GetNode(id)
	if err != nil {
		t.Fatalf("snapshot GetNode: %v", err)
	}
	if !n.HasLabel(store.NodeTypeEvidenceFile) {
		t.Fatalf("snapshot saw an update committed after it was opened: %v", n.Labels)
	}
	if _, err := snap.GetNode(gone); err != nil {
		t.Fatalf("snapshot lost a node deleted after it was opened: %v", err)
	}
}

// TestCompact_RefusesSecondCompaction pins the exclusion that s.mu used to
// provide for free. Two builds would race for one temp image path, and each
// would splice the delta against the other's epoch.
func TestCompact_RefusesSecondCompaction(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	for i := 0; i < 8; i++ {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}

	var inner error
	compactWithWrites(t, s, func() { inner = s.Compact() })

	if !errors.Is(inner, ErrCompactionInProgress) {
		t.Fatalf("second Compact inside the build window: want ErrCompactionInProgress, got %v", inner)
	}
	// And the flag is cleared, so the store is not wedged.
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact after a refused one: %v", err)
	}
}

// TestDeltaSince_DropsAbsorbedAndKeepsTheRest is the splice on its own.
//
// The reclaim half matters as much as the survival half: compaction is where a
// delete stops costing memory, and a splice that kept every chain would quietly
// turn the delta into an append-only log of the store's whole history.
func TestDeltaSince_DropsAbsorbedAndKeepsTheRest(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	absorbed := addNodeD(t, s, store.NodeTypeEvidenceFile)
	tombstoned := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.DeleteNode(tombstoned); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	s.mu.Lock()
	pin := s.mutEpoch.Load()
	live := s.cur().delta
	s.mu.Unlock()

	later := addNodeD(t, s, store.NodeTypeMicroArtefact)

	s.mu.Lock()
	spliced, shadowed := live.since(pin, nil)
	s.mu.Unlock()

	if _, ok := spliced.nodes[absorbed]; ok {
		t.Fatalf("node %d is in the image and was kept in the delta anyway", absorbed)
	}
	if _, ok := spliced.nodes[tombstoned]; ok {
		t.Fatalf("tombstone for %d outlived the compaction that reclaimed it", tombstoned)
	}
	if _, ok := spliced.nodes[later]; !ok {
		t.Fatalf("node %d was committed after the pin and was dropped", later)
	}
	if spliced.liveNodes != 1 {
		t.Fatalf("liveNodes: want 1, got %d", spliced.liveNodes)
	}
	if spliced.maskedNodes != 0 {
		t.Fatalf("maskedNodes: want 0 against an empty image, got %d", spliced.maskedNodes)
	}
	// Nothing can shadow a nil image.
	if shadowed != 0 {
		t.Fatalf("shadowed: want 0 against a nil image, got %d", shadowed)
	}
	if got := spliced.nodesByType[store.NodeTypeMicroArtefact]; len(got) != 1 || got[0] != later {
		t.Fatalf("type posting for the surviving node: want [%d], got %v", later, got)
	}
	// The chain the store is still holding must be untouched by all of that.
	if _, ok := live.nodes[absorbed]; !ok {
		t.Fatalf("since mutated the layer it was reading")
	}
}

// TestCompactCtx_CancelDuringBuildLeavesTheStoreAlone pins the cancellation
// contract: it reaches the build and nothing it touched is installed.
func TestCompactCtx_CancelDuringBuildLeavesTheStoreAlone(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	var ids []store.NodeID
	for i := 0; i < 32; i++ {
		ids = append(ids, addNodeD(t, s, store.NodeTypeEvidenceFile))
	}
	before := s.cur().csr

	ctx, cancel := context.WithCancel(context.Background())
	s.afterPinHook = cancel
	err = s.CompactCtx(ctx)
	s.afterPinHook = nil

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CompactCtx after cancellation: want context.Canceled, got %v", err)
	}
	if s.cur().csr != before {
		t.Fatalf("a cancelled compaction published an image anyway")
	}
	for _, id := range ids {
		if _, err := s.GetNode(id); err != nil {
			t.Fatalf("node %d lost by a cancelled compaction: %v", id, err)
		}
	}
	// And the store is not wedged: the in-progress flag was released.
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact after a cancelled one: %v", err)
	}
	for _, id := range ids {
		if _, err := s.GetNode(id); err != nil {
			t.Fatalf("node %d lost by the following compaction: %v", id, err)
		}
	}
}

// nodePropSet is every indexed node property a snapshot of s can see, keyed by
// id and key.
//
// The value is copied into a string deliberately. A value handed to this
// callback belongs to the index for the duration of the call and no longer —
// under IndexMapped it addresses the image — so copying is the contract this
// test keeps, as distinct from the one it is testing.
func nodePropSet(s *Store) (map[string]string, error) {
	sn, err := s.Snapshot()
	if err != nil {
		return nil, err
	}
	defer sn.Close()
	pe, ok := sn.(store.PropertyEnumerator)
	if !ok {
		return nil, errors.New("a disk snapshot no longer enumerates properties")
	}
	out := make(map[string]string)
	pe.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
		out[fmt.Sprintf("%d/%s", id, key)] = string(value)
		return true
	})
	return out, nil
}

// missingFrom names one entry of want that got does not have, or "" when got
// holds all of them. Entries got has and want does not are not a difference:
// want is a lower bound taken at an instant, and a walk that ran afterwards is
// entitled to more.
//
// One entry rather than a count, because a count does not say what a walk lost,
// and the sorted order is so that the one it names is the same on every run.
func missingFrom(got, want map[string]string) string {
	for _, k := range slices.Sorted(maps.Keys(want)) {
		g, ok := got[k]
		if !ok {
			return fmt.Sprintf("%s=%q is missing", k, want[k])
		}
		if g != want[k] {
			return fmt.Sprintf("%s reads %q, want %q", k, g, want[k])
		}
	}
	return ""
}

// TestMapping_ConcurrentCompactUnmap reads the property index out of a mapping
// while the compactions that produced it release the one before.
//
// Until a compaction adopted its own output this test had nothing to race. A
// store mapped its image once at Open and unmapped it at Close; the only code
// that ever released a mapping was a live reader's reload, and a compaction
// neither created one nor retired one. Now every compaction maps the image it
// has just written, installs the index base read out of it, and sweeps the
// mapping the previous base was read from as soon as the collector agrees
// nothing can reach it. That is a compaction unmapping a file while readers are
// reading one, which is what this is named for.
//
// The reader is snapshot.ForEachNodeProperty deliberately. It is the one index
// path that does not hold the store lock across its walk, so it is the only one
// a commit can overlap rather than queue behind, and under IndexMapped the
// values it hands the callback address the mapping. A sweep that released a
// base a walk was still inside would fault here, attributably, rather than
// somewhere else much later.
//
// Two things are asserted.
//
// No compaction makes a walk lose an entry. Each round registers entries that
// live only in the shards, publishes the whole set as a lower bound, and only
// then compacts; every walk overlapping that compaction has to hold at least
// that bound. The entries added per round are what give the bound any force —
// with a fixed set the old base already holds everything, so any answer at all
// satisfies it — and the first version of this test did without them.
//
// What this does *not* establish is the order in which the walk makes its two
// reads, and that is worth saying plainly because it is what the comment here
// first claimed. The window between loading the base and copying the delta is
// two instructions wide: with the reads deliberately reversed in the source,
// this test passed ten runs out of ten. The ordering is pinned instead by
// index's TestForEachNodeEntry_LoadsTheBaseAfterCopyingTheDelta, which holds the
// walk still on a shard lock and performs the swap's two halves by hand. What
// remains here is the integration property — a store, real compactions, real
// mappings — which is worth having and is not the same claim.
//
// And a mapping that has left the store's list has been unmapped, while one
// still in it has not. Identity is tracked rather than the count, because a
// bounded count is also what a store that quietly stopped mapping would report.
func TestMapping_ConcurrentCompactUnmap(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	requireAdoptable(t, s)

	payloadFixture(t, s, 300)
	if err := s.Compact(); err != nil {
		t.Fatalf("the compaction that gives this test something to race: %v", err)
	}
	if got := s.indexHolding(); got != IndexMapped.String() {
		t.Skipf("this store did not adopt its own output (%q), so no compaction here unmaps anything", got)
	}

	first, err := nodePropSet(s)
	if err != nil {
		t.Fatalf("the quiescent walk: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("the fixture indexed nothing, so a walk that returned nothing would pass")
	}

	// The lower bound every walk must hold, republished by the main goroutine
	// after each round's writes and before that round's compaction. A reader
	// loads it and then walks, so the walk is always at least as late as the
	// bound it is judged against.
	var bound atomic.Pointer[map[string]string]
	bound.Store(&first)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var walks atomic.Int64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				want := *bound.Load()
				got, werr := nodePropSet(s)
				if werr != nil {
					t.Errorf("a walk overlapping a compaction: %v", werr)
					return
				}
				if miss := missingFrom(got, want); miss != "" {
					t.Errorf("a walk overlapping a compaction saw %d entries and lost one of the %d "+
						"it had to hold: %s", len(got), len(want), miss)
					return
				}
				walks.Add(1)
			}
		}()
	}

	// The mappings this store has held at any point, and the ones it has let go
	// of. A sweep only releases what the collector has already agreed is
	// unreachable, so the GC is part of the sample rather than of the setup.
	seen := map[*mapping]bool{}
	released := 0
	sample := func(when string) {
		runtime.GC()
		runtime.GC()
		s.mu.RLock()
		held := make(map[*mapping]bool, len(s.indexImages))
		for _, m := range s.indexImages {
			held[m] = true
			seen[m] = true
			if m.unmapped.Load() {
				t.Errorf("%s: a mapping is listed as the index's after being unmapped", when)
			}
		}
		live := len(s.indexImages)
		s.mu.RUnlock()
		for m := range seen {
			if held[m] {
				continue
			}
			if !m.unmapped.Load() {
				t.Errorf("%s: a mapping left the list without being unmapped", when)
			}
			released++
			delete(seen, m)
		}
		t.Logf("%s: %d mappings held, %d released so far", when, live, released)
	}

	fail := func(format string, args ...any) {
		close(stop)
		wg.Wait()
		t.Fatalf(format, args...)
	}

	const rounds = 6
	for c := range rounds {
		// Entries that exist only in the shards until this round's compaction
		// folds them into a new base. Without them the old base is already a
		// complete answer, and the bound below asks nothing of anybody.
		for i := range 20 {
			id, aerr := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if aerr != nil {
				fail("AddNode in round %d: %v", c, aerr)
			}
			if ierr := s.IndexNodeProperty(id, "path", []byte(fmt.Sprintf("/r%02d/%02d", c, i))); ierr != nil {
				fail("IndexNodeProperty in round %d: %v", c, ierr)
			}
		}
		next, nerr := nodePropSet(s)
		if nerr != nil {
			fail("the walk that takes round %d's bound: %v", c, nerr)
		}
		bound.Store(&next)

		if err := s.Compact(); err != nil {
			fail("Compact %d: %v", c, err)
		}
		sample(fmt.Sprintf("round %d", c))
	}
	close(stop)
	wg.Wait()

	if walks.Load() == 0 {
		t.Fatal("no walk finished while the compactions ran, so this test raced nothing")
	}

	// Quiescent now, so the bases the walks were holding on their stacks are
	// unreachable and the next commit's sweep is free to release them. Without
	// this the release count is whatever the readers happened to be holding when
	// they stopped, which is not something to assert on.
	runtime.GC()
	runtime.GC()
	if err := s.Compact(); err != nil {
		t.Fatalf("the quiescent compaction that sweeps: %v", err)
	}
	sample("quiescent")

	if released == 0 {
		t.Errorf("%d compactions each mapped an image and none was released: either the sweep "+
			"is not running or nothing here ever becomes retirable", rounds+1)
	}
	got, err := nodePropSet(s)
	if err != nil {
		t.Fatalf("the final walk: %v", err)
	}
	want := *bound.Load()
	if miss := missingFrom(got, want); miss != "" {
		t.Errorf("after %d compactions the index answers with %d entries and has lost one of the "+
			"%d it held: %s", rounds+1, len(got), len(want), miss)
	}
	if len(got) <= len(first) {
		t.Errorf("the rounds added %d entries to the %d the fixture made and the index reports %d: "+
			"nothing was ever delta-only, so the ordering was not exercised",
			rounds*20, len(first), len(got))
	}
	t.Logf("%d walks finished across %d compactions, %d mappings released", walks.Load(), rounds+1, released)
}

// TestSnapshot_PropertyWalkAgainstIndexWrite is the whole lock cycle, built out
// of the two ordinary operations that close it.
//
// A store's write path takes the store lock and then a shard lock:
// IndexNodeProperty is s.mu.Lock followed by PropertyIndex.IndexNode. A snapshot
// property walk runs the other way round — it enters the index with no store
// lock at all, and its callback takes s.mu to ask whether the id it was handed
// still exists. If the walk is holding a shard lock while it does that, the two
// are a cycle, and this test hung against it: the writer parked on the shard and
// the walker parked on the store, with no compaction anywhere near either.
//
// The index-side half of the guard is index/walk_lock_test.go, which is sharper
// and deterministic. This one is the integration: it is the arrangement a bulk
// export against a live writer actually makes, and it is what would have caught
// the bug where it was found rather than where it lives.
func TestSnapshot_PropertyWalkAgainstIndexWrite(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	payloadFixture(t, s, 200)

	// A compaction first, so the walk merges a base with the delta rather than
	// reading the shards alone. Both shapes have to survive this; the merged one
	// is the shape a store that has been running for any length of time is in.
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Both sides are bounded rather than run against a stop channel, and that is
	// about what this measures rather than about tidiness: an unbounded writer
	// makes every walk longer than the one before it, so a slow run and a hung
	// one stop being distinguishable by a deadline.
	const rounds = 1000
	writes := make(chan struct{})
	go func() {
		defer close(writes)
		for w := range rounds {
			id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err != nil {
				t.Errorf("AddNode: %v", err)
				return
			}
			if err := s.IndexNodeProperty(id, "path", []byte(fmt.Sprintf("/w/%06d", w))); err != nil {
				t.Errorf("IndexNodeProperty: %v", err)
				return
			}
		}
	}()

	walked := make(chan int)
	go func() {
		n := 0
		for round := range rounds {
			got, err := nodePropSet(s)
			if err != nil {
				t.Errorf("walk %d: %v", round, err)
				break
			}
			n += len(got)
		}
		walked <- n
	}()

	// Twenty seconds is not a latency assertion. Against the cycle this finishes
	// never; against a walk that holds no shard lock it finishes in a second or
	// two, and it is the difference between those two that is being tested.
	select {
	case n := <-walked:
		<-writes
		if n == 0 {
			t.Fatal("a thousand walks saw no entries at all, so nothing here was tested")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("DEADLOCK: a snapshot property walk and an indexing write, no compaction involved")
	}
}
