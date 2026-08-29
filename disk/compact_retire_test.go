package disk

// The retire branch that keeps the log, and the marker it must not leave in it.
//
// A checkpoint record means "replay stops here". Compaction used to write one
// before it knew which retire branch it would take, which is right for the two
// branches that then empty or rebuild the log and wrong for the third, which
// keeps it and goes on appending. Every record written after such a compaction
// replayed as though it had never been committed — silently, on the next open.

import (
	"testing"

	"github.com/aoiflux/graphene/store"
)

// keepingRetireOpts forces retireLog's third branch: retention keeps segments,
// which rules out both carrying the tail and truncating to zero.
func keepingRetireOpts() Options {
	return Options{Retention: RetentionPolicy{MaxSegments: 4}}
}

func TestRetiredLogKeepsWritesMadeAfterACompaction(t *testing.T) {
	dir := t.TempDir()
	opts := keepingRetireOpts()

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{1}}); err != nil {
			t.Fatal(err)
		}
	}

	// A commit lands while the image is being built, so the tail is non-empty
	// and retention forbids carrying it. That is the keep branch.
	var during store.NodeID
	s.afterPinHook = func() {
		var aerr error
		if during, aerr = s.AddNode(&store.Node{Labels: []store.NodeType{2}}); aerr != nil {
			t.Errorf("commit during build: %v", aerr)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.afterPinHook = nil

	after, err := s.AddNode(&store.Node{Labels: []store.NodeType{3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if _, err := s2.GetNode(during); err != nil {
		t.Errorf("node committed during the build is gone on reopen: %v", err)
	}
	if _, err := s2.GetNode(after); err != nil {
		t.Errorf("node written after the compaction is gone on reopen: %v", err)
	}
	if n, err := s2.NodeCount(); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Errorf("node count = %d, want 7", n)
	}
}

// The two branches that do retire the log must replay whole afterwards.
//
// This test was written to assert that they still leave a checkpoint marker,
// on the reasoning that "never write one" would otherwise pass the test above
// and quietly drop the crash bound. It does not assert that, and it cannot:
// once the marker moved out of compactCommit the record became **inert**. A
// marker says "replay stops here", which differs from EOF only if records
// follow it — and every branch that writes one goes on to empty the log,
// rebuild it excluding the marker, or rotate the whole file away. No correct
// store can have a record after a marker, so stopping at one and stopping at
// EOF are the same stop.
//
// The mutation "a retiring branch stops writing its checkpoint marker" is
// therefore an equivalent mutant rather than a test gap. Removing the record
// type would drop it from the durable format and change what `graphene inspect`
// renders, which wants its own change; until then the honest claim is the one
// below, which is that these branches replay to the end.
func TestRetiringBranchesReplayWhole(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  Options
		churn bool
	}{
		// Nothing lands during the build: the log is retired whole.
		{name: "truncate whole", opts: Options{}},
		// A commit lands during the build and the log can carry it.
		{name: "rebuild over tail", opts: Options{}, churn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := OpenWithOptions(dir, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for i := 0; i < 5; i++ {
				if _, err := s.AddNode(&store.Node{Labels: []store.NodeType{1}}); err != nil {
					t.Fatal(err)
				}
			}
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

			// The retired log is empty or holds only the carried tail; either way
			// replay of it must reach the end, and a store that reopens with the
			// right count is the evidence that it did.
			want, err := s.NodeCount()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2, err := OpenWithOptions(dir, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			if got, err := s2.NodeCount(); err != nil {
				t.Fatal(err)
			} else if got != want {
				t.Errorf("node count after reopen = %d, want %d", got, want)
			}
		})
	}
}
