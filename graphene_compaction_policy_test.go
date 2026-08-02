package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
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
