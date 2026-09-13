package graphene_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// Storage statistics and the compaction advisory.
//
// Nothing in the engine triggers a compaction, so a store that is never
// compacted grows its in-memory delta and its log without bound — degrading
// memory, open time, and read speed with no error and no warning. These are the
// figures that make that visible, and the rule that decides when to say so.

func openDisk(t *testing.T) *graphene.Graph {
	t.Helper()
	g, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

// Writes land in the delta and are visible as such until a compaction moves
// them into the image.
func TestStorageStats_DeltaMovesIntoTheImageOnCompact(t *testing.T) {
	g := openDisk(t)

	const n = 50
	for i := 0; i < n; i++ {
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
			t.Fatal(err)
		}
	}

	before, ok := g.StorageStats()
	if !ok {
		t.Fatal("disk backend should report storage stats")
	}
	if before.DeltaNodes != n {
		t.Fatalf("delta holds %d nodes, want %d", before.DeltaNodes, n)
	}
	if before.CSRNodes != 0 {
		t.Fatalf("image holds %d nodes before any compaction, want 0", before.CSRNodes)
	}
	if before.WALBytes <= 0 {
		t.Fatalf("log is %d bytes after %d writes", before.WALBytes, n)
	}
	if !before.LastCompact.IsZero() {
		t.Fatal("LastCompact should be zero before the first compaction")
	}

	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	after, _ := g.StorageStats()
	if after.DeltaNodes != 0 {
		t.Fatalf("delta still holds %d nodes after compaction", after.DeltaNodes)
	}
	if after.CSRNodes != n {
		t.Fatalf("image holds %d nodes after compaction, want %d", after.CSRNodes, n)
	}
	if after.WALBytes >= before.WALBytes {
		t.Fatalf("log did not shrink: %d bytes before, %d after", before.WALBytes, after.WALBytes)
	}
	if after.LastCompact.IsZero() {
		t.Fatal("LastCompact still zero after a compaction")
	}
}

// Indexed properties are counted, since they are a large part of what the delta
// and the image actually hold.
func TestStorageStats_CountsPropertyEntries(t *testing.T) {
	g := openDisk(t)

	id, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	for _, k := range []string{"sha256", "path", "tool"} {
		if err := g.IndexNodeProperty(id, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	s, _ := g.StorageStats()
	if s.PropertyNodeEntries != 3 {
		t.Fatalf("counted %d node property entries, want 3", s.PropertyNodeEntries)
	}
}

// The memory backend has no delta, no log, and no compaction, so it reports
// nothing rather than reporting zeros that would read as "nothing to do".
func TestStorageStats_MemoryBackendReportsNothing(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()

	if _, ok := g.StorageStats(); ok {
		t.Fatal("the in-memory backend has no storage state to report")
	}

	st, err := g.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.HasStorage {
		t.Fatal("Stats reported storage detail for a backend that has none")
	}

	// And the advisory must be quiet rather than wrong.
	if due, _ := g.ShouldCompact(store.DefaultCompactionPolicy()); due {
		t.Fatal("a backend that cannot compact should never be advised to")
	}
}

// The advisory fires on the record-count rule, and says which rule fired.
func TestShouldCompact_FiresOnDeltaRecords(t *testing.T) {
	g := openDisk(t)

	policy := store.CompactionPolicy{MaxDeltaRecords: 10}

	if due, _ := g.ShouldCompact(policy); due {
		t.Fatal("advised compaction on an empty store")
	}

	for i := 0; i < 10; i++ {
		g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	}

	due, why := g.ShouldCompact(policy)
	if !due {
		t.Fatal("delta reached the limit and no compaction was advised")
	}
	if why == "" {
		t.Fatal("advice given with no reason; 'compact now' is not actionable alone")
	}

	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if due, _ := g.ShouldCompact(policy); due {
		t.Fatal("still advising compaction immediately after compacting")
	}
}

// Policy evaluation is a pure function over stats, so the rules can be checked
// without building a store for each one.
func TestCompactionPolicy_Rules(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy store.CompactionPolicy
		stats  store.StorageStats
		want   bool
	}{
		{
			name:   "zero policy never fires",
			policy: store.CompactionPolicy{},
			stats:  store.StorageStats{DeltaNodes: 1 << 20, WALBytes: 1 << 40},
			want:   false,
		},
		{
			name:   "delta records at the limit fires",
			policy: store.CompactionPolicy{MaxDeltaRecords: 100},
			stats:  store.StorageStats{DeltaNodes: 60, DeltaEdges: 40},
			want:   true,
		},
		{
			name:   "tombstones count toward the delta",
			policy: store.CompactionPolicy{MaxDeltaRecords: 100},
			stats:  store.StorageStats{DeltaNodes: 50, DeletedNodes: 50},
			want:   true,
		},
		{
			name:   "wal size fires",
			policy: store.CompactionPolicy{MaxWALBytes: 1000},
			stats:  store.StorageStats{WALBytes: 1000},
			want:   true,
		},
		{
			name:   "ratio fires on a small store churning heavily",
			policy: store.CompactionPolicy{MaxDeltaRatio: 0.5},
			stats:  store.StorageStats{DeltaNodes: 60, CSRNodes: 100},
			want:   true,
		},
		{
			name:   "ratio is not evaluated against an empty image",
			policy: store.CompactionPolicy{MaxDeltaRatio: 0.5},
			stats:  store.StorageStats{DeltaNodes: 5},
			want:   false,
		},
		{
			name:   "delta bytes fire where a record count cannot",
			policy: store.CompactionPolicy{MaxDeltaBytes: 1 << 20},
			stats:  store.StorageStats{DeltaNodes: 3, DeltaBytes: 1 << 20},
			want:   true,
		},
		{
			// The rule the byte figure exists to separate from: a store that has
			// written a great deal and is holding almost none of it.
			name:   "a large log and a small delta do not fire the byte rule",
			policy: store.CompactionPolicy{MaxDeltaBytes: 1 << 20},
			stats:  store.StorageStats{DeltaNodes: 100_000, WALBytes: 1 << 40, DeltaBytes: 4096},
			want:   false,
		},
		{
			// A backend that does not report DeltaBytes reports zero, and zero
			// must read as "cannot say" rather than as "under the limit" of a
			// rule that would then never fire, or as a breach of one that would
			// always.
			name:   "a backend that does not report delta bytes never fires it",
			policy: store.CompactionPolicy{MaxDeltaBytes: 1},
			stats:  store.StorageStats{DeltaNodes: 1 << 20},
			want:   false,
		},
		{
			name:   "below every limit stays quiet",
			policy: store.DefaultCompactionPolicy(),
			stats:  store.StorageStats{DeltaNodes: 10, CSRNodes: 1000, WALBytes: 4096},
			want:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, why := tc.policy.Evaluate(tc.stats)
			if got != tc.want {
				t.Fatalf("Evaluate = %v (%q), want %v", got, why, tc.want)
			}
			if got && why == "" {
				t.Fatal("fired without a reason")
			}
			if !got && why != "" {
				t.Fatalf("did not fire but gave a reason: %q", why)
			}
		})
	}
}

// Reading the figures must not require stopping the writers, and must not
// deadlock against them. StorageStats takes the store's read lock and reaches
// into the WAL and the property index while holding it, which is exactly the
// shape that acquires two locks in the wrong order if done carelessly.
func TestStorageStats_SafeUnderConcurrentWrites(t *testing.T) {
	g := openDisk(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 400; i++ {
			g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		}
	}()

	policy := store.DefaultCompactionPolicy()
	for i := 0; i < 400; i++ {
		s, ok := g.StorageStats()
		if !ok {
			t.Error("disk backend stopped reporting storage stats")
			break
		}
		if s.DeltaNodes < 0 || s.WALBytes < 0 {
			t.Errorf("nonsense reading under concurrency: %+v", s)
			break
		}
		g.ShouldCompact(policy)
	}
	<-done

	s, _ := g.StorageStats()
	if s.DeltaNodes != 400 {
		t.Fatalf("delta holds %d nodes after 400 writes, want 400", s.DeltaNodes)
	}
}

// Stats carries the storage detail through, so a caller needing both counts and
// operational figures makes one call.
func TestStats_CarriesStorageDetail(t *testing.T) {
	g := openDisk(t)
	g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	st, err := g.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if !st.HasStorage {
		t.Fatal("disk backend should report storage detail")
	}
	if st.NodeCount != 1 || st.Storage.DeltaNodes != 1 {
		t.Fatalf("NodeCount=%d Storage.DeltaNodes=%d, want 1 and 1",
			st.NodeCount, st.Storage.DeltaNodes)
	}
}

// --- The byte rule, and the controls over it ---

// blobNodes returns n nodes each carrying size bytes of properties, which is how
// a delta gets large without getting numerous.
func blobNodes(n, size int) []*store.Node {
	out := make([]*store.Node, n)
	for i := range out {
		out[i] = &store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: make([]byte, size),
		}
	}
	return out
}

// The byte rule fires on a store the other rules are quiet about.
//
// This is the case MaxDeltaRecords and MaxWALBytes were always going to miss and
// the reason DeltaBytes is maintained at all: a few records holding a great deal.
// Pinned against a real store rather than a StorageStats literal, because the
// literal would pass with nothing wired between the delta's accounting and the
// policy.
func TestCompactionPolicy_ByteRuleCatchesFewRecordsHoldingMuch(t *testing.T) {
	g := openDisk(t)

	// Generous against the count, tight against the bytes. Any store that
	// breaches this breaches it on the bytes.
	policy := store.CompactionPolicy{MaxDeltaRecords: 10_000, MaxDeltaBytes: 64 << 10}

	if due, _ := g.ShouldCompact(policy); due {
		t.Fatal("advised compaction on an empty store")
	}

	if _, err := g.AddNodes(blobNodes(16, 8<<10)); err != nil {
		t.Fatal(err)
	}

	s, _ := g.StorageStats()
	if s.DeltaNodes > 100 {
		t.Fatalf("fixture is not the shape this tests: %d delta records", s.DeltaNodes)
	}
	if s.DeltaBytes < 64<<10 {
		t.Fatalf("16 nodes of 8 KiB hold %d bytes, expected at least %d", s.DeltaBytes, 64<<10)
	}

	due, why := g.ShouldCompact(policy)
	if !due {
		t.Fatalf("delta holds %d bytes against a %d limit and no compaction was advised",
			s.DeltaBytes, 64<<10)
	}
	if why == "" {
		t.Fatal("advice given with no reason")
	}

	// And the same store is quiet under a policy that only counts records, which
	// is what makes the rule above the one that fired.
	if due, _ := g.ShouldCompact(store.CompactionPolicy{MaxDeltaRecords: 10_000}); due {
		t.Fatal("the record rule fired on 16 records against a 10,000 limit")
	}

	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if due, why := g.ShouldCompact(policy); due {
		t.Fatalf("still advising compaction after compacting: %s", why)
	}
}

// CompactIfDue compacts when the policy fires and does nothing when it does not.
//
// The whole value of the helper is that the two halves cannot drift, so what is
// pinned is the pairing: no compaction happens on a store that is not due, one
// does on a store that is, and the reason comes back either way.
func TestCompactIfDue_ActsOnlyWhenTheStoreIsDue(t *testing.T) {
	g := openDisk(t)

	policy := store.CompactionPolicy{MaxDeltaRecords: 10}

	did, why, err := g.CompactIfDue(policy)
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("compacted a store that was not due")
	}
	if why != "" {
		t.Fatalf("gave a reason for a store that was not due: %q", why)
	}
	if s, _ := g.StorageStats(); !s.LastCompact.IsZero() {
		t.Fatal("a compaction ran on a store that was not due")
	}

	for i := 0; i < 10; i++ {
		if _, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}); err != nil {
			t.Fatal(err)
		}
	}

	did, why, err = g.CompactIfDue(policy)
	if err != nil {
		t.Fatal(err)
	}
	if !did {
		t.Fatal("delta reached the limit and nothing was compacted")
	}
	if why == "" {
		t.Fatal("compacted without saying which rule fired")
	}

	s, _ := g.StorageStats()
	if s.DeltaNodes != 0 || s.CSRNodes != 10 {
		t.Fatalf("delta holds %d and the image %d after CompactIfDue, want 0 and 10",
			s.DeltaNodes, s.CSRNodes)
	}

	did, why, err = g.CompactIfDue(policy)
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatalf("compacted again immediately: %s", why)
	}
}

// A backend with nothing to compact is never due, so the helper is a no-op there
// rather than an unconditional compaction.
func TestCompactIfDue_MemoryBackendIsANoOp(t *testing.T) {
	g := graphene.NewInMemory()
	defer g.Close()

	for i := 0; i < 100; i++ {
		g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	}

	did, why, err := g.CompactIfDue(store.DefaultCompactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if did || why != "" {
		t.Fatalf("in-memory store reported a compaction: %v %q", did, why)
	}
}

// A failed compaction still reports which rule asked for it.
//
// The reason is what goes in an operator's log line, and a call that returns an
// error without it leaves them with a failure and no idea what triggered the
// attempt. Cancellation is the cheapest reachable failure.
func TestCompactIfDueCtx_ReasonSurvivesAFailedCompaction(t *testing.T) {
	g := openDisk(t)

	for i := 0; i < 10; i++ {
		g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	did, why, err := g.CompactIfDueCtx(ctx, store.CompactionPolicy{MaxDeltaRecords: 10})
	if err == nil {
		t.Fatal("a cancelled context compacted anyway")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("compaction failed with %v, want context.Canceled", err)
	}
	if did {
		t.Fatal("reported a compaction that did not happen")
	}
	if why == "" {
		t.Fatal("a failed compaction lost the reason it was attempted")
	}
}

// --- The soft limit ---

// A soft limit reports and refuses nothing.
//
// Every assertion here is about that word. The writes that cross it succeed, the
// ones after it succeed, the figure is readable without a sink and reported once
// with one, and a compaction that resolves the crossing arms it for the next.
func TestDeltaSoftLimit_ReportsWithoutFailingAnyWrite(t *testing.T) {
	const limit = 64 << 10

	sk := &sink{}
	g, err := graphene.OpenWithOptions(t.TempDir(), disk.Options{
		DeltaSoftLimit: limit,
		Metrics:        sk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if s, _ := g.StorageStats(); s.DeltaOverBudget {
		t.Fatal("an empty store is over budget")
	}

	// One batch well past the limit, so the crossing is not a matter of timing.
	if _, err := g.AddNodes(blobNodes(16, 8<<10)); err != nil {
		t.Fatalf("a write was refused under a soft limit: %v", err)
	}

	s, _ := g.StorageStats()
	if !s.DeltaOverBudget {
		t.Fatalf("delta holds %d bytes against a %d limit and does not report it",
			s.DeltaBytes, limit)
	}

	over := sk.of(store.MetricDeltaOverBudget)
	if len(over) != 1 {
		t.Fatalf("crossing emitted %d events, want 1", len(over))
	}
	if over[0].Bytes < limit {
		t.Fatalf("event reports %d bytes, below the %d limit that produced it", over[0].Bytes, limit)
	}
	if over[0].Examined != limit {
		t.Fatalf("event names a limit of %d, want %d", over[0].Examined, limit)
	}

	// Still over, so still silent: the event is per crossing, not per commit.
	for i := 0; i < 3; i++ {
		if _, err := g.AddNodes(blobNodes(4, 8<<10)); err != nil {
			t.Fatalf("a write was refused while over budget: %v", err)
		}
	}
	if n := sk.count(store.MetricDeltaOverBudget); n != 1 {
		t.Fatalf("%d events while sitting above the limit, want the 1 for the crossing", n)
	}

	// A compaction moves the delta into the image, which clears the figure and
	// arms the next report.
	if err := g.Compact(); err != nil {
		t.Fatal(err)
	}
	if s, _ := g.StorageStats(); s.DeltaOverBudget {
		t.Fatalf("still over budget after compacting: delta holds %d bytes", s.DeltaBytes)
	}

	if _, err := g.AddNodes(blobNodes(16, 8<<10)); err != nil {
		t.Fatal(err)
	}
	if n := sk.count(store.MetricDeltaOverBudget); n != 2 {
		t.Fatalf("second crossing brought the total to %d, want 2", n)
	}
}

// No limit, nothing said. The default must not turn a figure nobody asked for
// into an event and a flag.
func TestDeltaSoftLimit_ZeroIsSilent(t *testing.T) {
	g, sk, _ := metricStore(t)

	if _, err := g.AddNodes(blobNodes(32, 8<<10)); err != nil {
		t.Fatal(err)
	}

	s, _ := g.StorageStats()
	if s.DeltaBytes == 0 {
		t.Fatal("fixture wrote nothing")
	}
	if s.DeltaOverBudget {
		t.Fatal("reported over budget with no budget set")
	}
	if n := sk.count(store.MetricDeltaOverBudget); n != 0 {
		t.Fatalf("emitted %d budget events with no budget set", n)
	}
}
