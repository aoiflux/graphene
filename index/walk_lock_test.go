package index

// What a walk is allowed to be holding when it calls the caller back.
//
// Nothing. The entry walks yield with no shard lock held, and this file is the
// guard on that, because the cost of losing it is not a slow path but a hung
// process.
//
// The cycle, concretely. Every write path in a store takes the store lock and
// then a shard lock: disk.Store.IndexNodeProperty is s.mu.Lock followed by
// PropertyIndex.IndexNode, and a compaction's SwapBase is the same order over
// all sixteen shards at once. The property walk is the one path that runs the
// other way round — a store's callback resolves the id it was handed against the
// records, which takes the store lock — so a walk holding a shard lock while it
// calls that callback closes the cycle. It was closed: a snapshot property walk
// against a plain IndexNodeProperty hung both goroutines, with no compaction
// involved. stream.go's header had already named the hazard for the id path.
//
// The end-to-end form of that is disk's TestSnapshot_PropertyWalkAgainstIndexWrite,
// which needs a store to build the cycle out of. What is here is the half of it
// this package owns, and it is the sharper test of the two: a callback that takes
// the same shard's *write* lock deadlocks against a read lock held by its own
// goroutine, deterministically, with no second goroutine and no timing.

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// withinDeadline runs fn and fails if it has not finished in time.
//
// A deadlock is the failure this file exists to catch, and a test that catches
// it by hanging reports nothing and takes the whole package's timeout with it.
// The goroutine is leaked on failure deliberately: it is parked on a lock that
// will never be released, so there is nothing to cancel, and the process is
// about to end anyway.
func withinDeadline(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not finish: the walk is holding a lock its callback needs", what)
	}
}

// TestForEachNodeProperty_CallbackMayWriteTheIndex is the whole rule in one
// assertion: the callback takes the shard's write lock and the walk finishes.
//
// Both arms matter and they exercise different code. With no base the walk reads
// the delta alone; with one it merges the base's runs with the delta's copy, and
// that branch is the one a compacted store takes.
func TestForEachNodeProperty_CallbackMayWriteTheIndex(t *testing.T) {
	for _, withBase := range []bool{false, true} {
		name := "no base"
		if withBase {
			name = "over a base"
		}
		t.Run(name, func(t *testing.T) {
			p := NewPropertyIndex()
			if withBase {
				// Every entry in the base, so the merge walks a run per value
				// and the delta's side is empty — the compacted shape.
				var triples []triple
				for i := range 40 {
					triples = append(triples,
						triple{NodeKind, uint64(i + 1), "path", []byte(fmt.Sprintf("/n/%02d", i))},
						triple{NodeKind, uint64(i + 1), "kind", []byte(fmt.Sprintf("k-%d", i%3))})
				}
				if err := p.AttachBase(newFakeBase(triples)); err != nil {
					t.Fatal(err)
				}
			} else {
				for i := range 40 {
					p.IndexNode(store.NodeID(i+1), "path", []byte(fmt.Sprintf("/n/%02d", i)))
					p.IndexNode(store.NodeID(i+1), "kind", []byte(fmt.Sprintf("k-%d", i%3)))
				}
			}

			seen := 0
			withinDeadline(t, "the walk", func() {
				p.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
					seen++
					// The write lock on the shard this very entry came out of.
					// Under a walk that held the read lock across this, the
					// goroutine blocks on itself and never returns.
					p.IndexNode(store.NodeID(10_000+seen), key, value)
					return true
				})
			})
			if seen != 80 {
				t.Errorf("the walk yielded %d entries, want 80", seen)
			}
		})
	}
}

// TestForEachEdgeProperty_CallbackMayWriteTheIndex is the same for edges, whose
// walk is a separate function and so a separate way to lose this.
func TestForEachEdgeProperty_CallbackMayWriteTheIndex(t *testing.T) {
	p := NewPropertyIndex()
	for i := range 30 {
		p.IndexEdge(store.EdgeID(i+1), "rel", []byte(fmt.Sprintf("r-%d", i%4)))
	}
	seen := 0
	withinDeadline(t, "the edge walk", func() {
		p.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
			seen++
			p.IndexEdge(store.EdgeID(10_000+seen), key, value)
			return true
		})
	})
	if seen != 30 {
		t.Errorf("the walk yielded %d entries, want 30", seen)
	}
}

// TestForEachNodeEntry_CallbackMayWriteTheIndex covers the single-key walk, which
// is the same function one caller down and is exported on its own.
func TestForEachNodeEntry_CallbackMayWriteTheIndex(t *testing.T) {
	p := NewPropertyIndex()
	for i := range 20 {
		p.IndexNode(store.NodeID(i+1), "path", []byte(fmt.Sprintf("/n/%02d", i)))
	}
	seen := 0
	withinDeadline(t, "the single-key walk", func() {
		p.ForEachNodeEntry("path", func(id store.NodeID, value []byte) bool {
			seen++
			p.IndexNode(store.NodeID(10_000+seen), "path", value)
			return true
		})
	})
	if seen != 20 {
		t.Errorf("the walk yielded %d entries, want 20", seen)
	}
}

// TestForEachNodeProperty_YieldsWhatItHeldWhenItStarted states the other half of
// the copy's contract, so that the deadlock fix is not read as a licence to yield
// whatever the shard happens to hold at the moment of the call.
//
// The walk above registers an entry per entry it is given. If the walk read the
// live bucket as it went, it would meet its own writes and either run away or
// repeat; it reads a copy taken under the lock, so it yields exactly what was
// there when it reached that key — which is also what makes the entry count above
// an assertion rather than a coincidence.
func TestForEachNodeProperty_YieldsWhatItHeldWhenItStarted(t *testing.T) {
	p := NewPropertyIndex()
	for i := range 50 {
		p.IndexNode(store.NodeID(i+1), "path", []byte(fmt.Sprintf("/n/%02d", i)))
	}
	var got []store.NodeID
	// The deadline for the same reason the tests above have one: this callback
	// writes too, so a walk that took the lock back hangs here rather than
	// failing, and takes the package's whole timeout with it.
	withinDeadline(t, "the walk", func() {
		p.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
			got = append(got, id)
			// Two writes per entry, under the key being walked and another.
			p.IndexNode(store.NodeID(20_000+len(got)), "path", []byte("added"))
			p.IndexNode(store.NodeID(30_000+len(got)), "late", []byte("added"))
			return true
		})
	})
	if len(got) != 50 {
		t.Fatalf("the walk yielded %d entries, want the 50 it started with", len(got))
	}
	for i, id := range got {
		if id != store.NodeID(i+1) {
			t.Fatalf("entry %d is node %d, want %d: the walk read its own writes", i, id, i+1)
		}
	}
}

// TestForEachNodeEntry_LoadsTheBaseAfterCopyingTheDelta pins the order of the
// two reads a walk makes, by holding the walk still between them.
//
// SwapBase installs the new base and only then empties the shards. A walk that
// copies the delta first is therefore safe either way round — copy before the
// clear and it pairs a full delta with either base, copy after it and it has
// synchronised with the clear and so must observe the new base. A walk that
// loads the base first has neither guarantee: it can hold the base from before
// the swap and then read a shard the swap has already emptied, and the entries
// that were only ever in that delta are gone from its answer with nothing said.
//
// That window is two instructions wide, so a race will not find it: the
// concurrent form of this in disk, TestMapping_ConcurrentCompactUnmap, ran ten
// times against exactly this mistake and passed every time. So the window is
// made arbitrarily wide instead, by taking the shard's write lock before the
// walk starts and performing the swap's two halves by hand while the walk is
// parked on it. Under the right order the walk has read nothing yet; under the
// wrong one it is already holding the old base.
//
// White-box, and unavoidably so — the ordering is between two internal reads and
// there is no arrangement of the public API that separates them.
func TestForEachNodeEntry_LoadsTheBaseAfterCopyingTheDelta(t *testing.T) {
	const key = "path"

	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase([]triple{
		{NodeKind, 1, key, []byte("in-the-base")},
	})); err != nil {
		t.Fatal(err)
	}
	// Only in the shards. This is the entry the wrong order loses: the base the
	// walk would be holding predates it, and the shard it would read has been
	// emptied by the time it gets there.
	p.IndexNode(2, key, []byte("in-the-delta"))

	sh := p.shardFor(key)
	sh.mu.Lock()

	walking := make(chan struct{})
	done := make(chan []store.NodeID, 1)
	go func() {
		close(walking)
		var got []store.NodeID
		p.ForEachNodeEntry(key, func(id store.NodeID, value []byte) bool {
			got = append(got, id)
			return true
		})
		done <- got
	}()

	// The walk is now on its way to a lock this goroutine holds, and will park
	// there. A wait rather than a hook: there is nothing in the walk to hook,
	// which is the point of the arrangement.
	<-walking
	time.Sleep(100 * time.Millisecond)

	// SwapBase's two halves, in its order, with the walk held between them.
	// Calling SwapBase itself would deadlock against the lock held here, which is
	// what makes this the only way to write the test.
	p.baseRef.Store(newBaseState(newFakeBase([]triple{
		{NodeKind, 1, key, []byte("in-the-base")},
		{NodeKind, 2, key, []byte("in-the-delta")},
	})))
	sh.nodes = newPostings[store.NodeID]()
	sh.mu.Unlock()

	var got []store.NodeID
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the walk never finished")
	}

	slices.Sort(got)
	if !slices.Equal(got, []store.NodeID{1, 2}) {
		t.Fatalf("the walk answered %v, want [1 2]: it read the base before it copied the delta, "+
			"so it paired a base from before the swap with a shard the swap had already emptied", got)
	}
}
