package disk

// Live readers: following a writer, and the two numbers that make it resumable.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func addNodes(t *testing.T, s *Store, n int, label store.NodeType) []store.NodeID {
	t.Helper()
	ids := make([]store.NodeID, 0, n)
	for i := 0; i < n; i++ {
		id, err := s.AddNode(&store.Node{Labels: []store.NodeType{label}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	return ids
}

func mustRefresh(t *testing.T, r *Store) store.RefreshInfo {
	t.Helper()
	info, err := r.Refresh()
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return info
}

func mustHave(t *testing.T, r *Store, ids []store.NodeID, when string) {
	t.Helper()
	for _, id := range ids {
		if _, err := r.GetNode(id); err != nil {
			t.Errorf("%s: node %d missing: %v", when, id, err)
		}
	}
}

// The headline: a reader opened alongside a writer advances when told to.
func TestLiveReaderFollowsAWriter(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	first := addNodes(t, w, 3, 1)

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatalf("OpenLive alongside a writer: %v", err)
	}
	defer r.Close()
	if !r.IsLiveReader() {
		t.Error("IsLiveReader false on a store opened with OpenLive")
	}
	mustHave(t, r, first, "at open")

	second := addNodes(t, w, 4, 2)

	// Not before Refresh: the view is fixed between calls, and a reader that
	// drifted forward on its own would have no point at which its answers were
	// stable.
	for _, id := range second {
		if _, err := r.GetNode(id); err == nil {
			t.Fatalf("node %d visible before Refresh", id)
		}
	}

	info := mustRefresh(t, r)
	if !info.Advanced() || info.Reloaded {
		t.Errorf("refresh over appended bytes reported %+v, want advanced without a reload", info)
	}
	mustHave(t, r, first, "after refresh")
	mustHave(t, r, second, "after refresh")

	if n, err := r.NodeCount(); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Errorf("node count = %d, want 7", n)
	}
}

// A refresh with nothing to read is not an error and reports no advance.
func TestRefreshWithNothingNewIsANoOp(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	addNodes(t, w, 2, 1)

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	info := mustRefresh(t, r)
	if info.Advanced() {
		t.Errorf("refresh with nothing committed reported %+v", info)
	}
	if info = mustRefresh(t, r); info.Advanced() {
		t.Errorf("second consecutive refresh reported %+v", info)
	}
}

// Every retire branch replaces or keeps the log differently, and a reader has to
// come out of all three holding the same graph the writer holds.
func TestLiveReaderSurvivesEveryRetireBranch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  Options
		churn bool
		want  bool // a reload is expected
	}{
		// Truncate: log emptied, generation bumped.
		{name: "truncate whole", opts: Options{}, want: true},
		// truncateFrom: log rebuilt over the carried tail, generation bumped.
		{name: "rebuild over tail", opts: Options{}, churn: true, want: true},
		// Rotate: log retired to a segment, generation bumped.
		{name: "rotate", opts: Options{Retention: RetentionPolicy{MaxSegments: 4}}, want: true},
		// The keep branch: the log is untouched, so the reader carries on
		// incrementally against an image it never reloads — and is still right,
		// because the log it is reading still holds everything the new image
		// folded in.
		{name: "log kept", opts: Options{Retention: RetentionPolicy{MaxSegments: 4}}, churn: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := OpenWithOptions(dir, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()

			before := addNodes(t, w, 5, 1)

			r, err := OpenLive(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			mustHave(t, r, before, "at open")

			var during []store.NodeID
			if tc.churn {
				w.afterPinHook = func() {
					id, aerr := w.AddNode(&store.Node{Labels: []store.NodeType{2}})
					if aerr != nil {
						t.Errorf("commit during build: %v", aerr)
						return
					}
					during = append(during, id)
				}
			}
			if err := w.Compact(); err != nil {
				t.Fatal(err)
			}
			w.afterPinHook = nil

			after := addNodes(t, w, 3, 3)

			info := mustRefresh(t, r)
			if info.Reloaded != tc.want {
				t.Errorf("Reloaded = %v, want %v (%+v)", info.Reloaded, tc.want, info)
			}
			mustHave(t, r, before, "after compaction")
			mustHave(t, r, during, "after compaction")
			mustHave(t, r, after, "after compaction")

			wantCount, err := w.NodeCount()
			if err != nil {
				t.Fatal(err)
			}
			if got, err := r.NodeCount(); err != nil {
				t.Fatal(err)
			} else if got != wantCount {
				t.Errorf("reader node count = %d, writer = %d", got, wantCount)
			}
		})
	}
}

// Deletes reach a live reader too, which is the case a pure "replay what was
// appended" design gets wrong if it forgets the property index.
func TestLiveReaderFollowsDeletesAndProperties(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	ids := addNodes(t, w, 6, 1)
	for i, id := range ids {
		if err := w.IndexNodeProperty(id, "bucket", []byte(fmt.Sprintf("b%d", i%2))); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.NodesByProperty("bucket", []byte("b0"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("bucket b0 = %d nodes at open, want 3", len(got))
	}

	if err := w.DeleteNode(ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	mustRefresh(t, r)

	if _, err := r.GetNode(ids[0]); err == nil {
		t.Error("deleted node still readable after refresh")
	}
	got, err = r.NodesByProperty("bucket", []byte("b0"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("bucket b0 = %d nodes after the delete, want 2", len(got))
	}
}

// A live reader is still read-only, and a store that is not one refuses to be
// refreshed rather than pretending it advanced.
func TestLiveReaderIsStillReadOnly(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	addNodes(t, w, 1, 1)

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, err := r.AddNode(&store.Node{Labels: []store.NodeType{9}}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("AddNode on a live reader returned %v, want ErrReadOnly", err)
	}
	if err := r.Compact(); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Compact on a live reader returned %v, want ErrReadOnly", err)
	}

	if _, err := w.Refresh(); !errors.Is(err, ErrNotLiveReader) {
		t.Errorf("Refresh on a writer returned %v, want ErrNotLiveReader", err)
	}
	if w.IsLiveReader() {
		t.Error("IsLiveReader true on a writer")
	}
	w.Close()

	ro, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := ro.Refresh(); !errors.Is(err, ErrNotLiveReader) {
		t.Errorf("Refresh on an OpenReadOnly store returned %v, want ErrNotLiveReader", err)
	}
}

// The locking trade, asserted rather than described: OpenReadOnly is still
// refused beside a writer, OpenLive is not, and a writer can still start beside
// live readers. What must not change is the writer being refused beside another
// writer.
func TestLiveReaderTakesNoProcessLock(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	addNodes(t, w, 1, 1)

	if _, err := OpenReadOnly(dir); !errors.Is(err, ErrStoreLocked) {
		t.Errorf("OpenReadOnly beside a writer returned %v, want ErrStoreLocked", err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrStoreLocked) {
		t.Errorf("a second writer returned %v, want ErrStoreLocked", err)
	}

	r1, err := OpenLive(dir)
	if err != nil {
		t.Fatalf("OpenLive beside a writer: %v", err)
	}
	r2, err := OpenLive(dir)
	if err != nil {
		t.Fatalf("a second OpenLive: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// And live readers do not hold the store against a writer either.
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("a writer beside two live readers: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r1.Close(); err != nil {
		t.Fatalf("closing a live reader: %v", err)
	}
	if err := r2.Close(); err != nil {
		t.Fatalf("closing a live reader: %v", err)
	}
}

// A refresh replaces the image, the delta and the property index at once. Under
// -race this is the test that says the swap is not visible half-done.
func TestRefreshIsSafeBesideConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	ids := addNodes(t, w, 40, 1)
	for _, id := range ids {
		if err := w.IndexNodeProperty(id, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Both index-backed reads and a plain point read, because they
				// reach the pinned triple by different routes.
				if _, err := r.NodesByProperty("k", []byte("v")); err != nil {
					t.Errorf("NodesByProperty: %v", err)
					return
				}
				if _, err := r.QueryNodeIDs(store.NodeQuery{Filters: []store.PropertyFilter{
					{Key: "k", Op: store.PropertyOpEqual, Value: []byte("v")},
				}}); err != nil {
					t.Errorf("QueryNodeIDs: %v", err)
					return
				}
				if _, err := r.GetNode(ids[0]); err != nil {
					t.Errorf("GetNode: %v", err)
					return
				}
			}
		}()
	}

	for i := 0; i < 12; i++ {
		more := addNodes(t, w, 5, 2)
		for _, id := range more {
			if err := w.IndexNodeProperty(id, "k", []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		if i%3 == 2 {
			if err := w.Compact(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := r.Refresh(); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}

// Content only ever arrives. A reader that lost a record between two refreshes
// would break the one thing every view in this engine promises.
func TestLiveReaderNeverLosesARecord(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWithOptions(dir, Options{Retention: RetentionPolicy{MaxSegments: 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var seen []store.NodeID
	for round := 0; round < 10; round++ {
		seen = append(seen, addNodes(t, w, 4, store.NodeType(round))...)
		if round%2 == 1 {
			// Half the compactions take the keep branch, half do not.
			if round%4 == 1 {
				w.afterPinHook = func() {
					id, aerr := w.AddNode(&store.Node{Labels: []store.NodeType{99}})
					if aerr == nil {
						seen = append(seen, id)
					}
				}
			}
			if err := w.Compact(); err != nil {
				t.Fatal(err)
			}
			w.afterPinHook = nil
		}
		mustRefresh(t, r)
		mustHave(t, r, seen, fmt.Sprintf("round %d", round))
	}
}

// --- the resume contract ---

// A resume must never begin inside a batch. replayRecordsFrom therefore reports
// the last offset at which no batch was open, not how far it read.
func TestResumeOffsetRewindsToBeforeAnOpenBatch(t *testing.T) {
	applied := func(recs *[]string) ReplayCallbacks {
		mark := func(tag string) func([]byte) error {
			return func(p []byte) error { *recs = append(*recs, tag+":"+string(p)); return nil }
		}
		return ReplayCallbacks{NodeFunc: mark("node"), EdgeFunc: mark("edge")}
	}

	// One standalone record, then a batch that was never committed.
	var log []byte
	log = appendRecordFramed(log, walFramingV1, walRecordNode, []byte("alone"))
	standalone := int64(len(log))

	open := newWALBatch(64)
	open.add(walRecordNode, []byte("inside"))
	full := mustFinish(open, batchMeta{CommitSeq: 1})
	// Everything the batch wrote except its commit record: what a reader sees
	// when it catches up with a writer mid-transaction.
	partial := full[:len(full)-(walRecordOverhead+walBatchCommitPayloadV2)]
	log = append(log, partial...)

	var got []string
	off, err := replayRecordsFrom(newByteReader(log), int64(len(log)), walFramingV1, applied(&got))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 1 || got[0] != "node:alone" {
		t.Fatalf("applied %v, want only the standalone record", got)
	}
	if off != standalone {
		t.Fatalf("resume offset %d, want %d — a resume there would start inside the open batch",
			off, standalone)
	}

	// The writer finishes the batch. Resuming applies it exactly once.
	whole := append(append([]byte(nil), log[:standalone]...), full...)
	got = nil
	off2, err := replayRecordsFrom(newByteReader(whole[off:]), int64(len(whole))-off, walFramingV1, applied(&got))
	if err != nil {
		t.Fatalf("resumed replay: %v", err)
	}
	if len(got) != 1 || got[0] != "node:inside" {
		t.Fatalf("resumed replay applied %v, want the batch record once", got)
	}
	if off+off2 != int64(len(whole)) {
		t.Errorf("offsets summed to %d, want %d", off+off2, len(whole))
	}
}

// A torn record at the tail rewinds too: the writer will finish it, and the
// reader must read it whole rather than resume in its middle.
func TestResumeOffsetRewindsPastATornRecord(t *testing.T) {
	var log []byte
	log = appendRecordFramed(log, walFramingV1, walRecordNode, []byte("whole"))
	clean := int64(len(log))
	log = append(log, appendRecordFramed(nil, walFramingV1, walRecordNode, []byte("torn"))[:6]...)

	count := 0
	off, err := replayRecordsFrom(newByteReader(log), int64(len(log)), walFramingV1,
		ReplayCallbacks{NodeFunc: func([]byte) error { count++; return nil }})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 1 {
		t.Errorf("applied %d records, want 1", count)
	}
	if off != clean {
		t.Errorf("resume offset %d, want %d", off, clean)
	}
}

// --- the generation marker ---

// Every operation that ends the current log file has to change the generation,
// because that number is the only thing telling a reader its byte offsets are
// worthless.
func TestLogGenerationChangesWheneverTheLogIsReplaced(t *testing.T) {
	gen := func(t *testing.T, dir string) uint64 {
		t.Helper()
		h, _, err := peekWALHeader(filepath.Join(dir, walFileName))
		if err != nil {
			t.Fatalf("peek: %v", err)
		}
		return h.SegmentSeq
	}

	for _, tc := range []struct {
		name  string
		opts  Options
		churn bool
	}{
		{name: "truncate whole", opts: Options{}},
		{name: "rebuild over tail", opts: Options{}, churn: true},
		{name: "rotate", opts: Options{Retention: RetentionPolicy{MaxSegments: 4}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := OpenWithOptions(dir, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			addNodes(t, s, 3, 1)

			before := gen(t, dir)
			if tc.churn {
				s.afterPinHook = func() {
					if _, aerr := s.AddNode(&store.Node{Labels: []store.NodeType{2}}); aerr != nil {
						t.Errorf("commit during build: %v", aerr)
					}
				}
			}
			if err := s.Compact(); err != nil {
				t.Fatal(err)
			}
			s.afterPinHook = nil

			if after := gen(t, dir); after == before {
				t.Errorf("generation stayed at %d across a compaction that replaced the log", after)
			}
		})
	}
}

// The keep branch is the one case where the log is *not* replaced, so the
// generation must not move: a reader that reloaded there would throw away a
// delta it could have kept, for nothing.
func TestLogGenerationHoldsWhenTheLogIsKept(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{Retention: RetentionPolicy{MaxSegments: 4}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	addNodes(t, s, 3, 1)

	h, _, err := peekWALHeader(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	before := h.SegmentSeq

	s.afterPinHook = func() {
		if _, aerr := s.AddNode(&store.Node{Labels: []store.NodeType{2}}); aerr != nil {
			t.Errorf("commit during build: %v", aerr)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.afterPinHook = nil

	h, _, err = peekWALHeader(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if h.SegmentSeq != before {
		t.Errorf("generation moved from %d to %d without the log being replaced", before, h.SegmentSeq)
	}
}

// A store opened live must not lose the writer's uncompacted work if it is
// reopened as a writer afterwards — that is, OpenLive really writes nothing.
func TestOpenLiveWritesNothing(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := addNodes(t, w, 4, 1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// The lock file goes, so this is a directory a live reader could add one to.
	// Without that the test passes on a directory that already has one, which is
	// every directory a writer has ever opened -- and the file a read-only mode
	// is most likely to create is exactly that one. LockNone opens no lock file:
	// nothing reads the previous-holder record except a writer's unclean-shutdown
	// check, so the only thing opening it would buy is a handle that, on Windows,
	// stops anyone deleting it.
	if err := os.Remove(filepath.Join(dir, lockFileName)); err != nil {
		t.Fatal(err)
	}

	before := dirFingerprint(t, dir)

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustHave(t, r, ids, "live open of a closed store")
	mustRefresh(t, r)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	if after := dirFingerprint(t, dir); after != before {
		t.Errorf("a live reader modified the directory:\n before %s\n after  %s", before, after)
	}
}

// dirFingerprint is every file in dir with its size, which is enough to catch a
// read-only mode that writes.
func dirFingerprint(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := ""
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		out += fmt.Sprintf("%s=%d;", e.Name(), fi.Size())
	}
	return out
}

// An incremental refresh must not re-read the log it has already applied.
//
// This is the one property of Refresh that no assertion on its results can
// reach. Replay is idempotent — every record is an upsert or a tombstone — so a
// refresh that restarted from offset 0 would return the same epoch, the same
// generation and even the same Bytes, because replayFrom reports an absolute
// end offset rather than a distance. It would just do all the work again, every
// time, which is the whole of what "incremental" means and would turn following
// a writer from O(appended) into O(log) per call.
//
// What separates them is the work itself, and allocation is the deterministic
// way to see it: applying no records allocates a handful of times, applying
// twenty thousand allocates in proportion to them. The bound below is loose by
// two orders of magnitude so that it pins the shape and not a constant.
func TestARefreshWithNothingNewReReadsNothing(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ids := addNodes(t, w, 20000, 1)

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	mustRefresh(t, r)
	mustHave(t, r, ids[:1], "after the first refresh")

	// Nothing is appended between these calls, so a reader that resumes where it
	// left off has nothing to read and nothing to allocate.
	got := testing.AllocsPerRun(3, func() {
		if _, err := r.Refresh(); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
	})
	if got > 200 {
		t.Errorf("a no-op refresh over a 20000-record log allocated %.0f times; "+
			"it is re-reading the log instead of resuming from replayOff", got)
	}
}

// A reload must rebuild the delta layer, not replay into the one it has.
//
// Replay is idempotent, so for every ordinary reload — a rotation, a rebuilt
// log, a truncation — replaying the new log into the old delta reaches the same
// state and nothing distinguishes the two. The case that does is the one where
// the store on disk is *replaced by a smaller one*: a point-in-time restore put
// in place under a follower. The reader has applied records that the restored
// store never had and that no replay will contradict, because they are simply
// absent. Keeping the old delta would leave them visible for the life of the
// reader — a follower showing rows its own store no longer contains.
func TestAReloadDropsRecordsARestoreRemoved(t *testing.T) {
	dir, backupDir, restored := t.TempDir(), t.TempDir(), t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	kept := addNodes(t, w, 3, 1)
	if _, err := w.Backup(filepath.Join(backupDir, "b")); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	// Written after the backup, so the restore is a store that never had it.
	rolledBack := addNodes(t, w, 1, 2)

	r, err := OpenLive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	mustRefresh(t, r)
	mustHave(t, r, rolledBack, "before the restore")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Restore(filepath.Join(backupDir, "b"), filepath.Join(restored, "r"), RestoreOptions{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// The restored store is put in place under the reader, which is what an
	// operator recovering a directory does.
	swapStoreDir(t, filepath.Join(restored, "r"), dir)

	info := mustRefresh(t, r)
	if !info.Reloaded {
		t.Error("a replaced store did not trigger a reload")
	}
	mustHave(t, r, kept, "after the restore")
	if _, err := r.GetNode(rolledBack[0]); err == nil {
		t.Errorf("node %d survived a restore that removed it: the reload replayed "+
			"into the old delta instead of rebuilding it", rolledBack[0])
	}
}

// swapStoreDir replaces dst's contents with src's, the way an operator putting a
// restored directory in place does.
func swapStoreDir(t *testing.T, src, dst string) {
	t.Helper()
	old, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range old {
		if err := os.RemoveAll(filepath.Join(dst, e.Name())); err != nil {
			t.Fatalf("clearing %s: %v", dst, err)
		}
	}
	fresh, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range fresh {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
