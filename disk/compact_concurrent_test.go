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
	"testing"

	"github.com/aoiflux/graphene/store"
)

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
