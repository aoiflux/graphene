package disk

// The compaction plan's merge: what it yields, in what order, and what it costs.
//
// compactPin used to copy every live record in the store into the plan — the
// image's as well as the delta's — so a compaction's working set grew with the
// store rather than with the writes since the last one. The plan now holds the
// image itself, which is immutable, and copies only the delta; build merges the
// two as it walks them. These tests hold that merge to the collection it
// replaced, record for record, on a store shaped to contain every case the old
// loop distinguished.

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// mergeFixture builds a store whose delta exercises every branch the merge has:
// records only in the image, images records updated in the delta, image records
// tombstoned in the delta, records only in the delta, and — after the pin — delta
// entries the pinned epoch cannot see.
//
// It returns the store and the plan, pinned. The caller releases.
func mergeFixture(t *testing.T) (*Store, *compactPlan) {
	t.Helper()
	s, _ := openFresh(t)
	t.Cleanup(func() { s.Close() })

	// Generation one, which a compaction puts in the image.
	var inImage []store.NodeID
	for i := range 24 {
		inImage = append(inImage, addNodeD(t, s, store.NodeType(i%3+1)))
	}
	for i := range 20 {
		addEdgeD(t, s, inImage[i], inImage[(i+7)%24], store.EdgeTypeSimilarTo)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Generation two, which sits in the delta at the pin.
	//
	// An update, so the image's record is displaced by a delta record of the
	// same identifier.
	n, err := s.GetNode(inImage[3])
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	n.Properties = []byte("updated in the delta")
	if err := s.UpdateNode(n); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	// A delete, so the image's record is displaced by nothing at all. The
	// cascade takes its incident edges with it, which puts tombstoned edges in
	// the delta too.
	if err := s.DeleteNode(inImage[11]); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	// Records the image has never held, several of them incident on one node so
	// that the order they reach the adjacency fill in is observable.
	var inDelta []store.NodeID
	for i := range 9 {
		inDelta = append(inDelta, addNodeD(t, s, store.NodeType(i%3+1)))
	}
	for i := range 8 {
		addEdgeD(t, s, inDelta[0], inDelta[(i%8)+1], store.EdgeTypeContains)
	}

	plan, err := s.compactPin()
	if err != nil {
		t.Fatalf("compactPin: %v", err)
	}
	t.Cleanup(s.compactRelease)

	// Generation three: committed after the pin. The image must not hold any of
	// it, and the plan must not have to look at the delta again to know that.
	for i := range 5 {
		id := addNodeD(t, s, store.NodeType(i%3+1))
		addEdgeD(t, s, id, inDelta[0], store.EdgeTypeSimilarTo)
	}

	plan.sortDelta()
	return s, plan
}

// collectTheOldWay is compactPin's collection loop as it stood before the merge
// replaced it, run against a reader pinned at the plan's own epoch. It is the
// reference the merge is held to: not a restatement of the new code, but the
// code the new code replaced.
func collectTheOldWay(s *Store, epoch uint64) ([]nodeRecord, []rawEdge) {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := reader{v: s.cur(), epoch: epoch}
	cur := r.v

	var nodes []nodeRecord
	var edges []rawEdge
	if cur.csr != nil {
		for n := range cur.csr.Nodes() {
			if r.deltaNodeKnown(n.ID) {
				continue
			}
			nodes = append(nodes, n)
		}
		for e := range cur.csr.Edges() {
			if r.deltaEdgeKnown(e.ID) {
				continue
			}
			edges = append(edges, e)
		}
	}
	for id, ver := range cur.delta.nodes {
		n, ok := ver.at(r.epoch)
		if !ok || n == nil {
			continue
		}
		nodes = append(nodes, nodeRecord{ID: id, Labels: n.Labels, Properties: cloneBytes(n.Properties)})
	}
	for id, ver := range cur.delta.edges {
		e, ok := ver.at(r.epoch)
		if !ok || e == nil {
			continue
		}
		edges = append(edges, rawEdge{
			ID:         id,
			Src:        e.Src,
			Dst:        e.Dst,
			Labels:     e.Labels,
			Weight:     e.Weight,
			Properties: cloneBytes(e.Properties),
		})
	}
	return nodes, edges
}

func TestCompactPlan_MergeMatchesTheCollectionItReplaced(t *testing.T) {
	s, plan := mergeFixture(t)

	wantNodes, wantEdges := collectTheOldWay(s, plan.epoch)

	gotNodes := map[store.NodeID]nodeRecord{}
	for n := range plan.nodeSeq() {
		if _, dup := gotNodes[n.ID]; dup {
			t.Fatalf("node %d yielded twice", n.ID)
		}
		gotNodes[n.ID] = n
	}
	gotEdges := map[store.EdgeID]rawEdge{}
	for e := range plan.edgeSeq() {
		if _, dup := gotEdges[e.ID]; dup {
			t.Fatalf("edge %d yielded twice", e.ID)
		}
		gotEdges[e.ID] = e
	}

	if len(gotNodes) != len(wantNodes) {
		t.Errorf("merge yielded %d nodes, the collection it replaced yielded %d",
			len(gotNodes), len(wantNodes))
	}
	for _, want := range wantNodes {
		got, ok := gotNodes[want.ID]
		if !ok {
			t.Errorf("node %d is in the collection and not in the merge", want.ID)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("node %d: merge has %+v, collection has %+v", want.ID, got, want)
		}
		delete(gotNodes, want.ID)
	}
	for id := range gotNodes {
		t.Errorf("node %d is in the merge and not in the collection", id)
	}

	if len(gotEdges) != len(wantEdges) {
		t.Errorf("merge yielded %d edges, the collection it replaced yielded %d",
			len(gotEdges), len(wantEdges))
	}
	for _, want := range wantEdges {
		got, ok := gotEdges[want.ID]
		if !ok {
			t.Errorf("edge %d is in the collection and not in the merge", want.ID)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("edge %d: merge has %+v, collection has %+v", want.ID, got, want)
		}
		delete(gotEdges, want.ID)
	}
	for id := range gotEdges {
		t.Errorf("edge %d is in the merge and not in the collection", id)
	}

	// The shape the fixture promises. Without this the comparison above is
	// satisfied by a store where nothing interesting happened.
	if len(wantNodes) == 0 || len(wantEdges) == 0 {
		t.Fatal("the fixture produced an empty plan")
	}
}

func TestCompactPlan_SequencesAreAscendingAndRepeatable(t *testing.T) {
	_, plan := mergeFixture(t)

	// buildSeq walks the node sequence three times and the edge sequence five,
	// and sizes arrays from the first walk. A sequence that is not repeatable
	// here is an out-of-range write there.
	var firstNodes []store.NodeID
	for pass := range 3 {
		var ids []store.NodeID
		var prev store.NodeID
		for n := range plan.nodeSeq() {
			if n.ID <= prev {
				t.Fatalf("pass %d: node %d followed %d; the merge is not ascending", pass, n.ID, prev)
			}
			prev = n.ID
			ids = append(ids, n.ID)
		}
		if pass == 0 {
			firstNodes = ids
			continue
		}
		if !reflect.DeepEqual(ids, firstNodes) {
			t.Fatalf("pass %d yielded %d nodes, pass 0 yielded %d", pass, len(ids), len(firstNodes))
		}
	}

	var firstEdges []store.EdgeID
	for pass := range 5 {
		var ids []store.EdgeID
		var prev store.EdgeID
		for e := range plan.edgeSeq() {
			if e.ID <= prev {
				t.Fatalf("pass %d: edge %d followed %d; the merge is not ascending", pass, e.ID, prev)
			}
			prev = e.ID
			ids = append(ids, e.ID)
		}
		if pass == 0 {
			firstEdges = ids
			continue
		}
		if !reflect.DeepEqual(ids, firstEdges) {
			t.Fatalf("pass %d yielded %d edges, pass 0 yielded %d", pass, len(ids), len(firstEdges))
		}
	}
}

func TestCompactPlan_StopsEarlyWhenTheBuildDoes(t *testing.T) {
	_, plan := mergeFixture(t)

	// buildSeq abandons a walk on the first duplicate or refusal, so the
	// sequences have to honour a yield that returns false — in the image half
	// and in the delta half, which are two different loops.
	for _, stopAfter := range []int{1, 3, len(plan.deltaKnownNodes) + 2} {
		seen := 0
		for range plan.nodeSeq() {
			seen++
			if seen == stopAfter {
				break
			}
		}
		if seen != stopAfter {
			t.Errorf("node sequence yielded %d records for a break at %d", seen, stopAfter)
		}
	}
	for _, stopAfter := range []int{1, 3} {
		seen := 0
		for range plan.edgeSeq() {
			seen++
			if seen == stopAfter {
				break
			}
		}
		if seen != stopAfter {
			t.Errorf("edge sequence yielded %d records for a break at %d", seen, stopAfter)
		}
	}
}

func TestCompactPlan_PresizesTheDeltaCopy(t *testing.T) {
	s, plan := mergeFixture(t)

	// The delta maintains both counts, so the copy never has to grow. A slice
	// that doubles its way to n entries has touched 2n, which on a store with a
	// large delta is the allocation this stage exists to avoid.
	s.mu.Lock()
	live := s.cur().delta
	wantNodes, wantEdges := live.liveNodes, live.liveEdges
	s.mu.Unlock()

	// The fixture writes after the pin, so the live counts have moved on; the
	// plan's capacity is the count as it stood at the pin, which is no larger.
	if cap(plan.deltaNodes) > wantNodes || cap(plan.deltaNodes) < len(plan.deltaNodes) {
		t.Errorf("deltaNodes cap = %d for %d records, with %d live now; want the pinned live count",
			cap(plan.deltaNodes), len(plan.deltaNodes), wantNodes)
	}
	if cap(plan.deltaEdges) > wantEdges || cap(plan.deltaEdges) < len(plan.deltaEdges) {
		t.Errorf("deltaEdges cap = %d for %d records, with %d live now; want the pinned live count",
			cap(plan.deltaEdges), len(plan.deltaEdges), wantEdges)
	}
	if len(plan.deltaNodes) == 0 || len(plan.deltaEdges) == 0 {
		t.Fatal("the fixture produced an empty delta copy")
	}
}

// pinCost is the bytes allocated by one compactPin over a store holding live
// nodes in its image and delta nodes above them.
func pinCost(t *testing.T, live, delta int) uint64 {
	t.Helper()
	s, _ := openFresh(t)
	defer s.Close()

	for range live {
		addNodeD(t, s, store.NodeTypeEvidenceFile)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for range delta {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}

	best := ^uint64(0)
	for range 3 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		plan, err := s.compactPin()
		if err != nil {
			t.Fatalf("compactPin: %v", err)
		}
		runtime.ReadMemStats(&after)
		s.compactRelease()
		if plan.csr == nil || plan.csr.NodeCount() != live {
			t.Fatalf("pinned image holds %v records, want %d", plan.csr, live)
		}
		if cost := after.TotalAlloc - before.TotalAlloc; cost < best {
			best = cost
		}
	}
	return best
}

func TestCompactPlan_CostsNothingProportionalToTheImage(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two stores")
	}

	// No indexed properties anywhere in the fixture: csrPayload still
	// materialises the property index's entries at the pin, which is a separate
	// item of the same program, and it would drown the figure this measures.
	const delta = 16
	small := pinCost(t, 1000, delta)
	large := pinCost(t, 8000, delta)

	t.Logf("pin over 1,000 image records: %d B; over 8,000: %d B", small, large)

	// Copying the image's records cost 56 B a node, so eight times the image
	// would have cost about 392 KB more. What is left is the delta copy, the
	// redaction ledger and the payload, none of which the image's size reaches.
	const ceiling = 16 << 10
	if small > ceiling || large > ceiling {
		t.Errorf("a pin allocated %d B and %d B; both should sit under %d B, which is "+
			"the delta copy and the payload and nothing that scales with the image",
			small, large, ceiling)
	}
	// The slope, which is the claim a ceiling alone does not make: a ceiling
	// generous enough to pass at these sizes still passes for a cost of a few
	// bytes per record, and a few bytes per record is what this item removed.
	if large > small+small/4 {
		t.Errorf("a pin over eight times the image allocated %d B against %d B; "+
			"the plan is still holding something proportional to the image", large, small)
	}
}

func TestCompact_AdjacencyMatchesAReopenOfTheSameImage(t *testing.T) {
	s, dir := openFresh(t)

	var ids []store.NodeID
	for i := range 16 {
		ids = append(ids, addNodeD(t, s, store.NodeType(i%3+1)))
	}
	for i := range 12 {
		addEdgeD(t, s, ids[i], ids[(i+5)%16], store.EdgeTypeSimilarTo)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// A hub with many edges written after that compaction, so the delta half of
	// the merge has several edges landing in one node's adjacency range. Their
	// order in the image's arrays is the order the merge yields them in; before
	// the merge they arrived in Go map order, which is a different order on
	// almost every run and a different order again from the one a reopen sees.
	hub := ids[0]
	for i := range 10 {
		addEdgeD(t, s, hub, ids[(i%15)+1], store.EdgeTypeContains)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}

	built := s.cur().csr
	inMemory := fmt.Sprintf("out %v\nin %v\noutOff %v\ninOff %v",
		built.outEdges, built.inEdges, built.outOffset, built.inOffset)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	loaded := reopened.cur().csr
	fromDisk := fmt.Sprintf("out %v\nin %v\noutOff %v\ninOff %v",
		loaded.outEdges, loaded.inEdges, loaded.outOffset, loaded.inOffset)

	if inMemory != fromDisk {
		t.Errorf("the adjacency a compaction built is not the adjacency the same image "+
			"loads as:\nbuilt:\n%s\nloaded:\n%s", inMemory, fromDisk)
	}
}

// The compaction metric's two counts mean different things — "records in the
// image" and "records scanned", store/metrics.go — and until the merge they
// were the same number twice, because the plan scanned by collecting and the
// collection was the image. The pin can state the scan for nothing now: both
// counts it needs are maintained.
func TestCompact_MetricSeparatesRecordsScannedFromRecordsKept(t *testing.T) {
	sink := &countingSink{}
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{Metrics: sink})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const inImage = 24
	var ids []store.NodeID
	for range inImage {
		ids = append(ids, addNodeD(t, s, store.NodeTypeEvidenceFile))
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Four of the image's records tombstoned and six the image has never held.
	// The scan sees all ten delta entries and all twenty-four image records; the
	// image keeps twenty-six.
	const deleted, added = 4, 6
	for i := range deleted {
		if err := s.DeleteNode(ids[i]); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
	}
	for range added {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("second Compact: %v", err)
	}

	var last store.Metric
	found := 0
	for _, m := range sink.metrics {
		if m.Kind == store.MetricCompaction {
			last = m
			found++
		}
	}
	if found != 2 {
		t.Fatalf("%d compaction metrics, want 2", found)
	}
	if want := int64(inImage - deleted + added); last.Count != want {
		t.Errorf("Count = %d, want %d records in the image", last.Count, want)
	}
	if want := int64(inImage + deleted + added); last.Examined != want {
		t.Errorf("Examined = %d, want %d records scanned — the image's live records "+
			"plus every delta entry, tombstones included", last.Examined, want)
	}
	if last.Examined <= last.Count {
		t.Errorf("Examined = %d is not above Count = %d; a compaction that dropped "+
			"records scanned more than it kept", last.Examined, last.Count)
	}
}
