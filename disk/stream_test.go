package disk

// What a streaming read has to prove.
//
// Two things, and they are different in kind. That it answers what the slice form
// answers — which is a differential, run over both sides of the index and over
// every query shape, because a second path that agrees on the easy cases and
// diverges on one is worse than no second path. And that a sink may read the
// store, which is not a property of the answer at all but of the lock, and which
// no assertion about ids can reach: it is tested by making the deadlock happen if
// the rule is broken.

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// idStreamFixture writes n nodes over a handful of values under "bucket", deletes
// some, and then puts a posting back on each deleted id so that the liveness
// filter has something to do.
//
// The second half is not decoration. Deleting a node *purges* its index entries,
// so deleting is how a posting stops outliving its record, not how it starts — a
// fixture that only deletes never produces a dead id at all, and the filter that
// drops them is then untested. Registering an entry against an id whose record is
// gone is exactly the state liveNodeIDs exists for, and the store permits it: the
// index is explicit and additive, and registration does not check that the entity
// is there. The dead ids are spread through the range rather than appended, so a
// filter that only handled a dead tail would still fail.
func idStreamFixture(t *testing.T, n int) (*Store, string) {
	t.Helper()
	s, dir := openFresh(t)
	var dead []store.NodeID
	for i := 0; i < n; i++ {
		id := addNodeD(t, s, store.NodeTypeEvidenceFile)
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%d", i%5))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		if err := s.IndexNodeProperty(id, "seq", []byte(fmt.Sprintf("%05d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		if i%11 == 7 {
			if err := s.DeleteNode(store.NodeID(id)); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
			dead = append(dead, store.NodeID(id))
		}
	}
	for i, id := range dead {
		if err := s.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%d", i%5))); err != nil {
			t.Fatalf("IndexNodeProperty on a dead id: %v", err)
		}
	}
	if len(dead) == 0 {
		t.Fatalf("the fixture produced no dead postings at n=%d", n)
	}
	return s, dir
}

// idStreamHasDeadPostings reports whether the index still holds a posting for an
// id the records do not have — the condition the fixture above builds, asserted
// rather than assumed, because a compaction drops those entries and a test that
// wants them has to know which side of one it is on.
func idStreamHasDeadPostings(s *Store) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.readerLocked()
	for _, v := range idStreamBuckets() {
		for _, id := range r.index().NodesByProperty("bucket", v) {
			if !r.nodeExists(id) {
				return true
			}
		}
	}
	return false
}

func idStreamBuckets() [][]byte {
	return [][]byte{
		[]byte("b0"), []byte("b1"), []byte("b2"), []byte("b3"), []byte("b4"),
		[]byte("b9"), []byte(""), []byte("zzz"),
	}
}

// TestNodesByPropertyFunc_AnswersExactlyWhatNodesByPropertyAnswers runs on both
// sides of a compaction: before it the index is all delta, after it the answer is
// a base run merged with whatever the delta holds, and those are different code.
func TestNodesByPropertyFunc_AnswersExactlyWhatNodesByPropertyAnswers(t *testing.T) {
	s, _ := idStreamFixture(t, 300)
	defer s.Close()

	check := func(when string) {
		t.Helper()
		for _, v := range idStreamBuckets() {
			want, err := s.NodesByProperty("bucket", v)
			if err != nil {
				t.Fatalf("%s: NodesByProperty(%q): %v", when, v, err)
			}
			var got []store.NodeID
			err = s.NodesByPropertyFunc(context.Background(), "bucket", v,
				func(id store.NodeID) bool {
					got = append(got, id)
					return true
				})
			if err != nil {
				t.Fatalf("%s: NodesByPropertyFunc(%q): %v", when, v, err)
			}
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%s: bucket=%q: stream says %v, slice says %v", when, v, got, want)
			}
		}
	}

	// The liveness filter is only under test while the index holds a posting the
	// records do not, and that is a property of the fixture rather than of this
	// test. Asserted, because a fixture whose premise has quietly stopped holding
	// reads as coverage it is not.
	if !idStreamHasDeadPostings(s) {
		t.Fatal("the fixture holds no dead postings: the liveness filter is untested")
	}

	check("uncompacted")
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	check("compacted")

	// And once more with writes landing after the compaction, which is the only
	// state where both sides of the merge are non-empty at once.
	id := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.IndexNodeProperty(id, "bucket", []byte("b2")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	check("compacted, then written to")
}

// streamQueries is the matrix the two ForEachNodeID paths are held against. It
// deliberately includes shapes that cannot stream — a descending order, a window
// over a conjunction, a label union, an id list — because the fallback is where
// they have to land and landing there is what is being checked.
func idStreamQueries() []struct {
	name  string
	query store.NodeQuery
} {
	return []struct {
		name  string
		query store.NodeQuery
	}{
		{"everything", store.NodeQuery{}},
		{"one equality", store.NodeQuery{Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")}}}},
		{"one equality, nothing holds it", store.NodeQuery{Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b9")}}}},
		{"one equality, unknown key", store.NodeQuery{Filters: []store.PropertyFilter{
			{Key: "nokey", Op: store.PropertyOpEqual, Value: []byte("x")}}}},
		{"one equality, limit", store.NodeQuery{Limit: 7, Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b1")}}}},
		{"one equality, offset and limit", store.NodeQuery{Offset: 5, Limit: 9,
			Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b1")}}}},
		{"one equality, offset past the end", store.NodeQuery{Offset: 10000, Limit: 3,
			Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b1")}}}},
		{"one equality, offset only", store.NodeQuery{Offset: 4,
			Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b3")}}}},
		{"one equality, descending", store.NodeQuery{Order: store.QueryOrderDesc,
			Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")}}}},
		{"one equality and a label", store.NodeQuery{Types: []store.NodeType{store.NodeTypeEvidenceFile},
			Filters: []store.PropertyFilter{
				{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")}}}},
		{"two equalities", store.NodeQuery{Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")},
			{Key: "seq", Op: store.PropertyOpEqual, Value: []byte("00012")}}}},
		{"a prefix filter", store.NodeQuery{Filters: []store.PropertyFilter{
			{Key: "seq", Op: store.PropertyOpPrefix, Value: []byte("000")}}}},
		{"labels only", store.NodeQuery{Types: []store.NodeType{store.NodeTypeEvidenceFile}}},
		{"an empty id list is not an id list", store.NodeQuery{IDs: []store.NodeID{}}},
		{"an id list", store.NodeQuery{IDs: []store.NodeID{1, 2, 3, 8, 99999}}},
	}
}

func TestForEachNodeID_AgreesWithQueryNodeIDs(t *testing.T) {
	s, _ := idStreamFixture(t, 300)
	defer s.Close()

	run := func(when string) {
		t.Helper()
		for _, tc := range idStreamQueries() {
			want, err := s.QueryNodeIDs(tc.query)
			if err != nil {
				t.Fatalf("%s/%s: QueryNodeIDs: %v", when, tc.name, err)
			}
			var got []store.NodeID
			err = s.ForEachNodeID(context.Background(), tc.query, func(id store.NodeID) bool {
				got = append(got, id)
				return true
			})
			if err != nil {
				t.Fatalf("%s/%s: ForEachNodeID: %v", when, tc.name, err)
			}
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%s/%s: ForEachNodeID says %v, QueryNodeIDs says %v",
					when, tc.name, got, want)
			}
		}
	}

	run("uncompacted")
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	run("compacted")
}

// TestStreamableNodeQuery_DecidesTheShape names which queries take the streamed
// path. Without it the differential above is satisfied by a ForEachNodeID that
// never streams at all, which would pass every assertion and deliver nothing.
func TestStreamableNodeQuery_DecidesTheShape(t *testing.T) {
	eq := store.PropertyFilter{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b2")}
	pre := store.PropertyFilter{Key: "seq", Op: store.PropertyOpPrefix, Value: []byte("0")}

	cases := []struct {
		name  string
		query store.NodeQuery
		want  bool
	}{
		{"one equality", store.NodeQuery{Filters: []store.PropertyFilter{eq}}, true},
		{"one equality with a window", store.NodeQuery{Offset: 2, Limit: 3,
			Filters: []store.PropertyFilter{eq}}, true},
		{"explicit ascending", store.NodeQuery{Order: store.QueryOrderAsc,
			Filters: []store.PropertyFilter{eq}}, true},
		{"no filter", store.NodeQuery{}, false},
		{"two filters", store.NodeQuery{Filters: []store.PropertyFilter{eq, pre}}, false},
		{"a prefix filter", store.NodeQuery{Filters: []store.PropertyFilter{pre}}, false},
		{"descending", store.NodeQuery{Order: store.QueryOrderDesc,
			Filters: []store.PropertyFilter{eq}}, false},
		{"with a label", store.NodeQuery{Types: []store.NodeType{store.NodeTypeEvidenceFile},
			Filters: []store.PropertyFilter{eq}}, false},
		{"with an id list", store.NodeQuery{IDs: []store.NodeID{1},
			Filters: []store.PropertyFilter{eq}}, false},
		{"match any", store.NodeQuery{FilterMode: store.MatchAny,
			Filters: []store.PropertyFilter{eq}}, false},
	}
	for _, tc := range cases {
		f, got := streamableNodeQuery(tc.query)
		if got != tc.want {
			t.Errorf("%s: streamable=%v, want %v", tc.name, got, tc.want)
		}
		if got && f.Key != eq.Key {
			t.Errorf("%s: streamed from %q, want %q", tc.name, f.Key, eq.Key)
		}
	}
}

// TestNodesByPropertyFunc_SinkMayReadTheStore is the lock contract, and it is
// written as a deadlock rather than as an assertion because that is what breaking
// it produces.
//
// A writer is parked on the write lock while the pass is mid-flight, and the sink
// then reads the store. Go's RWMutex does not promise a second read lock to a
// goroutine that already holds one while a writer waits, so if this pass held the
// store lock across the sink, the read below would never return and the test would
// fail on its deadline instead of on a comparison.
//
// The sink reads twice, and the second read is the one that matters. GetNode is
// the natural thing a sink does and it is *not* sufficient here: on the disk store
// it takes the lock-free fast path over the immutable CSR and no store lock at
// all, so a pass holding s.mu across the sink does not deadlock it — which a
// mutation moving the unlock past the sink proved by surviving. CountNodesByType
// takes s.mu.RLock() unconditionally, and it stands here for every read that
// does.
func TestNodesByPropertyFunc_SinkMayReadTheStore(t *testing.T) {
	s, _ := idStreamFixture(t, 400)
	defer s.Close()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	writerQueued := make(chan struct{})
	writerDone := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		seen := 0
		done <- s.NodesByPropertyFunc(context.Background(), "bucket", []byte("b0"),
			func(id store.NodeID) bool {
				if seen == 0 {
					// Park a writer behind whatever lock this pass holds, then
					// read the store from inside the sink.
					go func() {
						close(writerQueued)
						addNodeD(t, s, store.NodeTypeEvidenceFile)
						close(writerDone)
					}()
					<-writerQueued
					// The writer needs a moment to reach the lock. If it has not
					// got there the test is merely weaker, never wrong.
					time.Sleep(20 * time.Millisecond)
				}
				seen++
				if _, err := s.GetNode(id); err != nil {
					t.Errorf("the sink could not read node %d: %v", id, err)
					return false
				}
				if _, err := s.CountNodesByType(context.Background()); err != nil {
					t.Errorf("the sink could not count nodes: %v", err)
					return false
				}
				return true
			})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("NodesByPropertyFunc: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the pass never finished: a lock is held across the sink")
	}
	select {
	case <-writerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the parked writer never got the lock")
	}
}

// TestNodesByPropertyFunc_HoldsTheLockWhileItReads is the other half of the lock
// contract, and it pulls against the test above on purpose. That one proves the
// lock is *released* before the sink. This one proves it is *taken* at all.
//
// It has to be a separate test because the release test cannot see the
// difference. Its writer is a single AddNode whose whole job is to park, and a
// pass that takes no lock leaves nothing to park behind: the write lands inside
// the sink's own sleep and is over before the next batch reads anything. So the
// release test passes unchanged with the RLock deleted outright — which a mutation
// deleting it proved by surviving.
//
// What the lock guards is not the merge. IDStream copied the delta's list at
// construction and the base is an immutable mapping, so Fill touches nothing a
// writer writes. It is the liveness filter: reader.nodeExists reads
// r.v.delta.nodes before it consults the image, and every AddNode writes that
// map. An unlocked pass is therefore a concurrent map access, and seeing it needs
// a writer that keeps writing for as long as the pass runs — one write cannot do
// it. Under -race any unsynchronised pair is reported whether or not the two
// accesses overlap in time; without -race it takes a true collision and the
// runtime's own map check, which is the weaker of the two and the reason the
// mutation arm for this one runs -race.
//
// The assertion on top keeps the test from being pure race bait, and it is a
// sandwich rather than an equality. The nodes the writer adds carry the value
// being streamed, so the pass's answer has to fall between two answers that are
// known: everything that existed before the writer was launched, and nothing that
// did not exist once it stopped. Equality with the earlier of those two is what
// this test first asserted and it is wrong - the pass pins its reader some time
// after that sample is taken, and every id added in the gap belongs to the pass's
// snapshot by right. Ascending and without duplicates is the part of the claim
// that a torn read would break on its own.
//
// streamEdgeIDs takes the lock by the same construction and has no test of its
// own for it; this one stands for both, which is worth knowing before that
// construction is changed on one side only.
func TestNodesByPropertyFunc_HoldsTheLockWhileItReads(t *testing.T) {
	s, _ := idStreamFixture(t, 6000)
	defer s.Close()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	want, err := s.NodesByProperty("bucket", []byte("b0"))
	if err != nil {
		t.Fatalf("NodesByProperty: %v", err)
	}
	want = slices.Clone(want)
	if len(want) < 2*scanChunk {
		// Fewer batches than this and the writer may never overlap a fill at all.
		t.Fatalf("the fixture answers %d ids, too few for a multi-batch pass", len(want))
	}

	stop := make(chan struct{})
	writerErr := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		defer close(writerErr)
		first := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
			if err != nil {
				writerErr <- err
				return
			}
			// Under the same key and value the pass is streaming, so that a pass
			// which re-read the delta mid-flight would show up as an extra id
			// rather than only as a race.
			if err := s.IndexNodeProperty(id, "bucket", []byte("b0")); err != nil {
				writerErr <- err
				return
			}
			if first {
				close(started)
				first = false
			}
		}
	}()

	<-started
	var got []store.NodeID
	err = s.NodesByPropertyFunc(context.Background(), "bucket", []byte("b0"),
		func(id store.NodeID) bool {
			got = append(got, id)
			return true
		})
	close(stop)
	if err != nil {
		t.Fatalf("NodesByPropertyFunc: %v", err)
	}
	if werr, ok := <-writerErr; ok && werr != nil {
		t.Fatalf("the concurrent writer failed: %v", werr)
	}

	after, err := s.NodesByProperty("bucket", []byte("b0"))
	if err != nil {
		t.Fatalf("NodesByProperty after the pass: %v", err)
	}
	if !slices.IsSorted(got) {
		t.Errorf("the pass did not answer in ascending order")
	}
	for i := 1; i < len(got); i++ {
		if got[i] == got[i-1] {
			t.Fatalf("the pass answered id %d twice", got[i])
		}
	}
	inGot := make(map[store.NodeID]bool, len(got))
	for _, id := range got {
		inGot[id] = true
	}
	for _, id := range want {
		if !inGot[id] {
			t.Fatalf("the pass lost id %d, which existed before the writer started", id)
		}
	}
	inAfter := make(map[store.NodeID]bool, len(after))
	for _, id := range after {
		inAfter[id] = true
	}
	for _, id := range got {
		if !inAfter[id] {
			t.Fatalf("the pass answered id %d, which does not hold the value", id)
		}
	}
	if len(after) <= len(want) {
		t.Fatalf("the writer added nothing during the pass: %d ids before, %d after",
			len(want), len(after))
	}
}

// TestNodesByPropertyFunc_StopsWhenTheSinkDoes: a false from the sink ends the
// pass without an error, and ends it there rather than after the current batch.
func TestNodesByPropertyFunc_StopsWhenTheSinkDoes(t *testing.T) {
	s, _ := idStreamFixture(t, 300)
	defer s.Close()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for _, stopAt := range []int{1, 2, 31, 33} {
		seen := 0
		err := s.NodesByPropertyFunc(context.Background(), "bucket", []byte("b0"),
			func(store.NodeID) bool {
				seen++
				return seen < stopAt
			})
		if err != nil {
			t.Fatalf("stopAt %d: %v", stopAt, err)
		}
		if seen != stopAt {
			t.Fatalf("stopAt %d: the sink saw %d ids", stopAt, seen)
		}
	}
}

// TestNodesByPropertyFunc_Cancels: a cancelled context abandons the pass and
// returns the context's error, with nothing partial passed off as an answer — the
// sink has already seen ids, which is why the error is the only report.
func TestNodesByPropertyFunc_Cancels(t *testing.T) {
	s, _ := idStreamFixture(t, 300)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	err := s.NodesByPropertyFunc(ctx, "bucket", []byte("b0"), func(store.NodeID) bool {
		seen++
		if seen == 3 {
			cancel()
		}
		return true
	})
	if err == nil {
		t.Fatal("a cancelled pass reported success")
	}
	if !errorsIsCanceled(err) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	// And a context dead before the call is refused before anything is read.
	dead, stop := context.WithCancel(context.Background())
	stop()
	calls := 0
	if err := s.NodesByPropertyFunc(dead, "bucket", []byte("b0"),
		func(store.NodeID) bool { calls++; return true }); err == nil {
		t.Fatal("a dead context reported success")
	}
	if calls != 0 {
		t.Fatalf("a dead context still yielded %d ids", calls)
	}
}

func errorsIsCanceled(err error) bool {
	return err == context.Canceled || (err != nil && err.Error() == context.Canceled.Error())
}

// TestEdgesByPropertyFunc_AnswersExactlyWhatEdgesByPropertyAnswers is the edge
// twin, over one shape rather than the matrix: the two functions differ only in
// which container they read.
func TestEdgesByPropertyFunc_AnswersExactlyWhatEdgesByPropertyAnswers(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()

	a := addNodeD(t, s, store.NodeTypeEvidenceFile)
	b := addNodeD(t, s, store.NodeTypeEvidenceFile)
	for i := 0; i < 60; i++ {
		eid := addEdgeD(t, s, a, b, store.EdgeTypeSimilarTo)
		if err := s.IndexEdgeProperty(eid, "rel", []byte(fmt.Sprintf("r%d", i%4))); err != nil {
			t.Fatalf("IndexEdgeProperty: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for _, v := range [][]byte{[]byte("r0"), []byte("r3"), []byte("r9")} {
		want, err := s.EdgesByProperty("rel", v)
		if err != nil {
			t.Fatalf("EdgesByProperty(%q): %v", v, err)
		}
		var got []store.EdgeID
		if err := s.EdgesByPropertyFunc(context.Background(), "rel", v,
			func(id store.EdgeID) bool { got = append(got, id); return true }); err != nil {
			t.Fatalf("EdgesByPropertyFunc(%q): %v", v, err)
		}
		if len(want) == 0 && len(got) == 0 {
			continue
		}
		if !slices.Equal(got, want) {
			t.Fatalf("rel=%q: stream says %v, slice says %v", v, got, want)
		}
	}
}
