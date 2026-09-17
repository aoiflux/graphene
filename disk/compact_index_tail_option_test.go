package disk

// The index tail limit as a caller's figure rather than a constant.
//
// compact_index_adopt_test.go establishes what a capture does. This is about the
// one thing that changed when the constant became CompactOptions.MaxIndexTailBytes:
// that the figure a caller writes down is the figure in force, in both
// directions, and that a figure no capture could be useful at is refused before
// the store comes up rather than on a background compaction nobody is watching.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// A figure below the floor stops the store coming up, and says both numbers.
//
// At the floor exactly, and above it, and at zero, it opens: the floor is a
// floor and not a range.
func TestCompactOptions_RefusesAnIndexTailBelowTheFloor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bytes   int64
		refused bool
	}{
		{"Zero", 0, false},
		{"OneBelowTheFloor", minIndexTailBytes - 1, true},
		{"AtTheFloor", minIndexTailBytes, false},
		{"AboveTheFloor", minIndexTailBytes * 4, false},
		{"Negative", -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenWithOptions(t.TempDir(), Options{
				Compact: CompactOptions{MaxIndexTailBytes: tc.bytes},
			})
			if err == nil {
				defer s.Close()
			}
			switch {
			case tc.refused && !errors.Is(err, ErrCompactIndexTailBytes):
				t.Fatalf("MaxIndexTailBytes %d opened with %v, want ErrCompactIndexTailBytes", tc.bytes, err)
			case !tc.refused && err != nil:
				t.Fatalf("MaxIndexTailBytes %d was refused: %v", tc.bytes, err)
			}
		})
	}
}

// A refusal on one field does not depend on the other being set.
//
// validate returned early when MaxWorkingBytes was zero, which is the default,
// so a check written after it would have been unreachable for every caller who
// set only this one.
func TestCompactOptions_TheTwoFiguresAreCheckedIndependently(t *testing.T) {
	_, err := OpenWithOptions(t.TempDir(), Options{
		Compact: CompactOptions{MaxIndexTailBytes: 1},
	})
	if !errors.Is(err, ErrCompactIndexTailBytes) {
		t.Fatalf("an index tail of 1 with no working-set figure opened with %v", err)
	}
	_, err = OpenWithOptions(t.TempDir(), Options{
		Compact: CompactOptions{MaxWorkingBytes: 1, MaxIndexTailBytes: minIndexTailBytes},
	})
	if !errors.Is(err, ErrCompactWorkingBytes) {
		t.Fatalf("a working set of 1 with a valid index tail opened with %v", err)
	}
}

// Zero resolves to what every version before the option held, so an upgrade
// changes nothing for a caller who sets nothing.
func TestCompactOptions_ZeroIndexTailIsTheShippedFigure(t *testing.T) {
	s, _ := openFresh(t)
	defer s.Close()
	if got, want := s.idxTailBytes, int64(defaultIndexTailBytes); got != want {
		t.Fatalf("a default store records %d bytes of index tail, want %d", got, want)
	}
	if want := int64(16 << 20); int64(defaultIndexTailBytes) != want {
		t.Fatalf("the default is %d, and the figure this option replaced was %d",
			defaultIndexTailBytes, want)
	}
}

// tailOptionFixture writes n indexed nodes, then compacts while more land, and
// reports whether the compaction adopted its own output.
//
// The writes during the build are the whole instrument: each is one recorded
// mutation, charged index.TailOpBytes, so how many fit is exactly the configured
// figure divided by that. A store whose limit cannot hold them falls back to the
// pre-capture behaviour and holds its index resident.
func tailOptionFixture(t *testing.T, tailBytes int64, during int) string {
	t.Helper()
	s, err := OpenWithOptions(t.TempDir(), Options{
		Compact: CompactOptions{MaxIndexTailBytes: tailBytes},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	requireAdoptable(t, s)

	const n = 20
	payloadFixture(t, s, n)
	before := collectAdoptAnswers(t, s, n)

	compactWithWrites(t, s, func() {
		for i := range during {
			id, err := s.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
			if err != nil {
				t.Errorf("AddNode during the build: %v", err)
				return
			}
			if err := s.IndexNodeProperty(id, "during", []byte(fmt.Sprintf("/d/%05d", i))); err != nil {
				t.Errorf("IndexNodeProperty during the build: %v", err)
				return
			}
		}
	})

	// Whichever way the adoption went, the store answers the same. That is the
	// property the option must not be able to break: it chooses how much memory
	// a compaction gives back, never what the store says.
	collectAdoptAnswers(t, s, n).diff(t, before, "after a build under a configured tail limit")
	for i := range during {
		want := fmt.Sprintf("/d/%05d", i)
		ids, err := s.NodesByProperty("during", []byte(want))
		if err != nil {
			t.Fatalf("NodesByProperty %s: %v", want, err)
		}
		if len(ids) != 1 {
			t.Fatalf("the entry %s registered during the build answers %v, want one identifier", want, ids)
		}
	}
	return s.indexHolding()
}

// The configured figure is the one that binds, in both directions.
//
// Same fixture, same writes during the build, one option apart: at the floor the
// capture cannot hold them and the compaction declines to adopt, and at a figure
// large enough it does. Nothing else in the two runs differs, so the adoption is
// this option's doing and not the fixture's.
func TestCompact_TheConfiguredIndexTailIsTheOneThatBinds(t *testing.T) {
	// One more mutation than the floor can hold: minIndexTailBytes divided by
	// index.TailOpBytes, rounded down, is what a capture at the floor admits.
	const during = minIndexTailBytes/index.TailOpBytes + 8

	t.Run("AtTheFloorTheCaptureOverflows", func(t *testing.T) {
		if got := tailOptionFixture(t, minIndexTailBytes, during); got != IndexResident.String() {
			t.Fatalf("a compaction recording %d mutations under a %d-byte limit adopted its output; "+
				"the index is held as %q", during, minIndexTailBytes, got)
		}
	})
	t.Run("RaisedItFits", func(t *testing.T) {
		if got := tailOptionFixture(t, 4*minIndexTailBytes, during); got != IndexMapped.String() {
			t.Fatalf("a compaction recording %d mutations under a %d-byte limit declined to adopt; "+
				"the index is held as %q", during, 4*minIndexTailBytes, got)
		}
	})
}

// The refusal names the figure in force rather than a constant.
//
// A message quoting the old constant while a caller's figure did the binding is
// the failure this carries idxTailBytes on the plan to avoid: it sends whoever
// reads it to tune a number that is not the one that refused.
func TestCompact_TheOverflowRefusalNamesTheConfiguredFigure(t *testing.T) {
	p := &compactPlan{idxTailBytes: 4096}
	s, _ := openFresh(t)
	defer s.Close()
	p.propIdx = s.propIdx

	// Opened at a size that cannot hold one operation, and then given one, which
	// is what sets the overflow the refusal reports.
	tail := s.propIdx.CaptureTail(0)
	defer s.propIdx.StopCapture()
	id := addNodeD(t, s, store.NodeTypeEvidenceFile)
	if err := s.IndexNodeProperty(id, "path", []byte("/overflow")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := p.tailUsable(s, tail, 1); err == nil {
		t.Fatal("an incomplete capture was reported as usable")
	} else if got := err.Error(); !strings.Contains(got, "4096") {
		t.Fatalf("the refusal is %q, and does not name the configured 4096", got)
	}
}
