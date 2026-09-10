package disk

// Kill-at-every-step for compaction.
//
// Compaction's crash-safety is not a property of any one statement; it is a
// property of the *order* of about ten of them. The image is synced before the
// checkpoint marker is written, because the marker vouches for the image. The
// rename happens before the log is retired, because the log is what recovers
// the store if the rename does not survive. Each of those arguments is a claim
// about what a reopen finds if the process dies at one specific point.
//
// Before this file, two of those points were tested — the two a test could
// reproduce by hand, by writing a stray temp file and by calling Checkpoint
// directly (TestCompact_CrashAfterCheckpointBeforeRename). The rest were
// covered by the comments asserting them. compactStepHook makes every point
// reachable, so the claims are checked rather than argued.
//
// The assertion at each point is the same, and it is the only one that matters:
// a reopen finds every record that was committed, in the image or the log, with
// its index entries intact. How far the compaction got is the engine's own
// business; not losing a committed write is the contract.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// errStepInjected is what the hook returns. Matched through errors.Is so a step
// that wraps it still reads as the injected failure, and a genuine engine error
// does not.
var errStepInjected = errors.New("injected step failure")

// compactSteps is every step the hook can name.
//
// Written out rather than derived, because the point of the list is to be a
// second statement of the sequence: a step deleted from compact.go without
// being deleted here fails TestCompact_EveryStepIsReachable below, which is how
// a quietly dropped fsync gets noticed.
var compactSteps = []string{
	compactStepSerialise,
	compactStepWriteTmp,
	compactStepSyncTmp,
	compactStepDrainWAL,
	compactStepRename,
	compactStepSyncDir,
	compactStepCheckpoint,
	compactStepRetireWhole,
	compactStepRetireTail,
	compactStepRetireKeep,
}

// stepCase is a store shape that drives a particular retire branch. All three
// branches have to be walked: they differ in whether a checkpoint marker is
// written at all, which is the most dangerous difference in the file.
type stepCase struct {
	name string
	opts Options

	// writeDuringBuild leaves a tail — records committed between the pin and the
	// commit, which the image does not hold. Retention decides whether that tail
	// can be carried, and the two answers are different branches.
	writeDuringBuild bool
}

var stepCases = []stepCase{
	{name: "quiet", opts: Options{}},
	{name: "quiet_retained", opts: Options{Retention: RetentionPolicy{MaxSegments: 4}}},
	{name: "tail_carried", opts: Options{}, writeDuringBuild: true},
	{name: "tail_kept", opts: Options{Retention: RetentionPolicy{MaxSegments: 4}}, writeDuringBuild: true},
}

// committedSet is what a reopen has to find, whatever the compaction did.
type committedSet struct {
	nodes []store.NodeID

	// bucket maps a property value to the nodes carrying it, so the assertion
	// covers the index and not only the records.
	bucket map[string][]store.NodeID
}

// seedForStep opens a store, gives it an image plus a generation of records
// that live only in the log, and returns what a reopen must find.
//
// Both generations on purpose. A compaction that fails after the rename has
// installed an image holding the first; the second is in the log alone, and a
// retire that ran when it should not have is precisely what loses it.
func seedForStep(t *testing.T, dir string, opts Options) (*Store, committedSet) {
	t.Helper()

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := committedSet{bucket: map[string][]store.NodeID{}}
	add := func(bucket string, n int) {
		for i := 0; i < n; i++ {
			id := addNodeD(t, s, store.NodeTypeEvidenceFile)
			if err := s.IndexNodeProperty(id, "bucket", []byte(bucket)); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
			want.nodes = append(want.nodes, id)
			want.bucket[bucket] = append(want.bucket[bucket], id)
		}
	}

	add("in-image", 24)
	if err := s.Compact(); err != nil {
		t.Fatalf("seed Compact: %v", err)
	}
	add("in-log", 24)

	return s, want
}

// assertRecovered reopens the directory and checks the committed set survived.
func assertRecovered(t *testing.T, dir string, opts Options, want committedSet) {
	t.Helper()

	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()

	for _, id := range want.nodes {
		if _, err := s.GetNode(id); err != nil {
			t.Errorf("node %d was committed and is gone after reopen: %v", id, err)
		}
	}
	for bucket, ids := range want.bucket {
		got, err := s.NodesByProperty("bucket", []byte(bucket))
		if err != nil {
			t.Fatalf("NodesByProperty(%q): %v", bucket, err)
		}
		if !sameIDs(got, ids) {
			t.Errorf("property %q: want %d nodes %v, got %d nodes %v",
				bucket, len(ids), ids, len(got), got)
		}
	}
	// Every record can be present while the index disagrees with them, which is
	// the failure a reopen is least likely to notice on its own.
	if err := s.VerifyIndexes(); err != nil {
		t.Errorf("VerifyIndexes after reopen: %v", err)
	}
}

func sameIDs(got, want []store.NodeID) bool {
	if len(got) != len(want) {
		return false
	}
	g := append([]store.NodeID(nil), got...)
	w := append([]store.NodeID(nil), want...)
	sort.Slice(g, func(i, j int) bool { return g[i] < g[j] })
	sort.Slice(w, func(i, j int) bool { return w[i] < w[j] })
	for i := range g {
		if g[i] != w[i] {
			return false
		}
	}
	return true
}

// TestCompact_FailsAtEveryStep stops a compaction at each step of each retire
// branch and asserts the reopen finds everything that was committed.
func TestCompact_FailsAtEveryStep(t *testing.T) {
	for _, tc := range stepCases {
		for _, step := range compactSteps {
			t.Run(fmt.Sprintf("%s/%s", tc.name, step), func(t *testing.T) {
				dir := t.TempDir()
				s, want := seedForStep(t, dir, tc.opts)

				if tc.writeDuringBuild {
					// A commit landing between the pin and the commit is the
					// only way to produce a tail, and the tail is what the
					// three retire branches disagree about.
					s.afterPinHook = func() {
						id := addNodeD(t, s, store.NodeTypeMicroArtefact)
						if err := s.IndexNodeProperty(id, "bucket", []byte("during-build")); err != nil {
							t.Errorf("IndexNodeProperty during build: %v", err)
							return
						}
						want.nodes = append(want.nodes, id)
						want.bucket["during-build"] = append(want.bucket["during-build"], id)
					}
				}

				var fired bool
				s.compactStepHook = func(got string) error {
					if got != step {
						return nil
					}
					fired = true
					return fmt.Errorf("at %s: %w", got, errStepInjected)
				}

				err := s.Compact()
				switch {
				case fired && err == nil:
					t.Fatalf("step %q was injected and Compact still reported success", step)
				case fired && !errors.Is(err, errStepInjected):
					t.Fatalf("step %q: Compact returned %v, want the injected error", step, err)
				case !fired && err != nil:
					// The step does not run in this branch, so the compaction
					// should have completed normally.
					t.Fatalf("step %q does not fire here, yet Compact failed: %v", step, err)
				}

				// A failed compaction must not leave an image-sized carcass in
				// a directory whose most likely problem is that it is full.
				tmpPath := filepath.Join(dir, csrFileName+".tmp")
				if _, statErr := os.Stat(tmpPath); statErr == nil {
					t.Errorf("step %q left a temp image at %s", step, tmpPath)
				} else if !os.IsNotExist(statErr) {
					t.Errorf("stat temp image: %v", statErr)
				}

				// Detach the hooks before Close. A store whose compaction has
				// just failed is still a store, and Close must not be measured
				// against an injected failure.
				s.compactStepHook = nil
				s.afterPinHook = nil
				if err := s.Close(); err != nil {
					t.Fatalf("Close after a failure at %q: %v", step, err)
				}

				assertRecovered(t, dir, tc.opts, want)
			})
		}
	}
}

// TestCompact_EveryStepIsReachable asserts that each named step actually fires
// in at least one of the shapes above, and that no step fires which the list
// does not name.
//
// Without it, compactSteps decays into a list of names: a step removed from
// compact.go, or one that only ever runs in a configuration no test builds,
// would go on being "covered" by a subtest that injects into nothing and passes
// because nothing failed. That is the test-that-cannot-fail CONTRIBUTING warns
// about, arrived at by neglect rather than by design.
func TestCompact_EveryStepIsReachable(t *testing.T) {
	seen := map[string]bool{}

	for _, tc := range stepCases {
		dir := t.TempDir()
		s, _ := seedForStep(t, dir, tc.opts)
		if tc.writeDuringBuild {
			s.afterPinHook = func() { addNodeD(t, s, store.NodeTypeMicroArtefact) }
		}
		s.compactStepHook = func(step string) error {
			seen[step] = true
			return nil
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("%s: Compact: %v", tc.name, err)
		}
		s.compactStepHook = nil
		s.afterPinHook = nil
		if err := s.Close(); err != nil {
			t.Fatalf("%s: Close: %v", tc.name, err)
		}
	}

	for _, step := range compactSteps {
		if !seen[step] {
			t.Errorf("step %q never fires in any tested shape: it was removed from "+
				"compact.go, or it needs a stepCase that reaches it", step)
		}
	}
	for step := range seen {
		if !containsStr(compactSteps, step) {
			t.Errorf("compact.go fires step %q, which compactSteps does not list: "+
				"add it so TestCompact_FailsAtEveryStep covers it", step)
		}
	}
}

func containsStr(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TestCompact_PanicInBuildClearsTheFlag asserts that an abnormal exit from a
// compaction still releases the in-progress flag.
//
// compactRelease is deferred, so this holds by construction — but "by
// construction" is exactly what stops being true when someone moves the defer
// below an early return. A store that survived a panic and can never compact
// again fails silently: it goes on accepting writes, and the delta simply grows
// until the machine notices.
func TestCompact_PanicInBuildClearsTheFlag(t *testing.T) {
	dir := t.TempDir()
	s, want := seedForStep(t, dir, Options{})
	defer s.Close()

	s.afterPinHook = func() { panic("injected panic between the pin and the build") }

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the injected panic did not reach the caller")
			}
		}()
		_ = s.Compact()
	}()

	s.afterPinHook = nil
	if err := s.Compact(); err != nil {
		if errors.Is(err, ErrCompactionInProgress) {
			t.Fatal("the compaction flag survived a panic: this store can never compact again")
		}
		t.Fatalf("Compact after a panic: %v", err)
	}

	for _, id := range want.nodes {
		if _, err := s.GetNode(id); err != nil {
			t.Errorf("node %d lost across the panic and the compaction after it: %v", id, err)
		}
	}
}
