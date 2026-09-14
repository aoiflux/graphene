package disk

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The division must reproduce today's constants exactly, or the option is a
// behaviour change dressed as a knob.
//
// Two separate claims and both matter. A caller who sets nothing gets the zero
// value, which means every consumer resolves its own default and the division
// is not consulted at all. A caller who sets the figure the defaults add up to
// gets those same four numbers back, which is what makes the figure a
// description of the current behaviour rather than a number beside it.
func TestCompactBuffers_DefaultIsExactlyTodaysConstants(t *testing.T) {
	if got := compactBuffersFor(0); got != (compactBuffers{}) {
		t.Errorf("an unset figure produced %+v, not the zero value that leaves "+
			"every threshold to resolve its own default", got)
	}

	want := compactBuffers{
		vtabMemCap:   gpixVtabMemCap,
		revChunk:     gpirSortChunkEntries,
		mergeBudget:  gpirMergeReadBudget,
		maxRunBuffer: gpirMaxRunBuffer,
	}
	if got := compactBuffersFor(defaultCompactWorkingBytes); got != want {
		t.Errorf("dividing the default figure gave %+v, want %+v -- the figure "+
			"and the constants have drifted apart, so the documented default is "+
			"not what a compaction takes", got, want)
	}
	if got := (compactBuffers{}).workingBytes(); got != defaultCompactWorkingBytes {
		t.Errorf("the defaults model %d B, and the figure that names them is %d B",
			got, defaultCompactWorkingBytes)
	}

	// The two halves agree with the arithmetic spelled out in the option's doc.
	if defaultCompactWorkingBytes != 4325376 {
		t.Errorf("the default figure is %d B; CompactOptions.MaxWorkingBytes "+
			"documents 4,325,376", defaultCompactWorkingBytes)
	}
	if minCompactWorkingBytes != 524288 {
		t.Errorf("the floor is %d B; CompactOptions.MaxWorkingBytes documents "+
			"524,288", minCompactWorkingBytes)
	}
}

// A bound that is exceeded is not a bound.
//
// The interesting direction is the only one: truncation in the division may
// leave the four adding up to less than was asked for and may never leave them
// adding up to more. The lower band is here so that "never more" cannot be
// satisfied by returning the floor for every figure.
func TestCompactBuffers_NeverExceedTheFigure(t *testing.T) {
	for _, figure := range []int64{
		minCompactWorkingBytes,
		minCompactWorkingBytes + 1,
		minCompactWorkingBytes + 7919, // a prime, so every division truncates
		defaultCompactWorkingBytes,
		defaultCompactWorkingBytes + 1,
		8 << 20,
		64 << 20,
		1 << 30,
		maxCompactWorkingBytes,
	} {
		t.Run(fmt.Sprint(figure), func(t *testing.T) {
			b := compactBuffersFor(figure)
			if b.vtabMemCap <= 0 || b.revChunk <= 0 || b.mergeBudget <= 0 ||
				b.maxRunBuffer <= 0 {
				t.Fatalf("%+v: a zero field reads as \"take the default\" at every "+
					"consumer, so the figure would be silently ignored", b)
			}
			if b.maxRunBuffer < gpirMinRunBuffer {
				t.Errorf("one run's buffer is %d B, below the %d B floor emit "+
					"raises it to -- the figure would be exceeded by the merge",
					b.maxRunBuffer, gpirMinRunBuffer)
			}
			held := b.workingBytes()
			if held > figure {
				t.Errorf("a figure of %d B produced %+v, which holds %d B: the "+
					"bound is exceeded by %d B", figure, b, held, held-figure)
			}
			// Not uselessly low: returning the floor for every figure would
			// satisfy the line above and honour nothing.
			if held < figure-4096 {
				t.Errorf("a figure of %d B produced only %d B of buffers, %d B "+
					"short: the figure is being rounded away rather than divided",
					figure, held, figure-held)
			}
		})
	}

	// Past the ceiling the figure is clamped rather than overflowed, which is
	// the one case where holding less than was asked for is the whole point.
	b := compactBuffersFor(maxCompactWorkingBytes * 64)
	if got := b.workingBytes(); got > maxCompactWorkingBytes {
		t.Errorf("a figure past the ceiling produced %d B of buffers, above the "+
			"%d B ceiling itself", got, maxCompactWorkingBytes)
	}
}

// More room must not make any one of the four smaller.
func TestCompactBuffers_RiseWithTheFigure(t *testing.T) {
	prev := compactBuffersFor(minCompactWorkingBytes)
	for figure := int64(minCompactWorkingBytes) * 2; figure <= 1<<30; figure *= 2 {
		b := compactBuffersFor(figure)
		if b.vtabMemCap < prev.vtabMemCap || b.revChunk < prev.revChunk ||
			b.mergeBudget < prev.mergeBudget || b.maxRunBuffer < prev.maxRunBuffer {
			t.Fatalf("raising the figure to %d B shrank a buffer: %+v then %+v",
				figure, prev, b)
		}
		prev = b
	}
}

// A figure the division cannot honour is refused, and refused before anything
// on disk has been touched.
//
// The alternative was to round it up to the floor, which is the failure this
// option exists to prevent in miniature: a caller who asked for a bound and got
// a larger one silently is worse off than one who was told no.
func TestCompactOptions_RefuseAFigureBelowTheFloor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		figure int64
		ok     bool
	}{
		{"unset", 0, true},
		{"exactly the floor", minCompactWorkingBytes, true},
		{"one below the floor", minCompactWorkingBytes - 1, false},
		{"a byte", 1, false},
		{"negative", -1, false},
		{"far above", 1 << 30, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CompactOptions{MaxWorkingBytes: tc.figure}.validate()
			if tc.ok {
				if err != nil {
					t.Fatalf("a figure of %d was refused: %v", tc.figure, err)
				}
				return
			}
			if !errors.Is(err, ErrCompactWorkingBytes) {
				t.Fatalf("a figure of %d gave %v, want ErrCompactWorkingBytes",
					tc.figure, err)
			}
			// The message has to carry both numbers or an operator cannot act
			// on it without reading the source.
			msg := err.Error()
			if !bytes.Contains([]byte(msg), []byte(fmt.Sprint(tc.figure))) ||
				!bytes.Contains([]byte(msg), []byte(fmt.Sprint(minCompactWorkingBytes))) {
				t.Errorf("the refusal reads %q and names neither the figure given "+
					"nor the floor it is under", msg)
			}
		})
	}
}

// The refusal precedes every side effect, including creating the directory.
func TestCompactOptions_ARefusedOpenTouchesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := OpenWithOptions(dir, Options{
		Compact: CompactOptions{MaxWorkingBytes: 4096},
	})
	if err == nil {
		_ = s.Close()
		t.Fatal("a store opened with a figure below the floor")
	}
	if !errors.Is(err, ErrCompactWorkingBytes) {
		t.Fatalf("Open failed with %v, want ErrCompactWorkingBytes", err)
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Errorf("the refused open created %s: a configuration that can never be "+
			"honoured should stop before the first write, not after it", dir)
	}

	// And the same directory opens once the figure is one the division can meet.
	s, err = OpenWithOptions(dir, Options{
		Compact: CompactOptions{MaxWorkingBytes: minCompactWorkingBytes},
	})
	if err != nil {
		t.Fatalf("OpenWithOptions at the floor: %v", err)
	}
	_ = s.Close()
}

// The setting must reach the build, and must reach it through the plan rather
// than by being read out of the store at some later moment.
func TestCompactOptions_TheSettingReachesThePlan(t *testing.T) {
	const figure = 64 << 20
	s := bufferFixture(t, Options{Compact: CompactOptions{MaxWorkingBytes: figure}}, 64)

	plan, err := s.compactPin()
	if err != nil {
		t.Fatalf("compactPin: %v", err)
	}
	defer s.compactRelease()

	want := compactBuffersFor(figure)
	if plan.buffers != want {
		t.Fatalf("the plan carries %+v, the option divides to %+v", plan.buffers, want)
	}

	// And the hop after it. The encoder is handed a source at build time and
	// there is nothing downstream that would notice a zero struct: taking the
	// defaults produces a correct image, just not the one the caller asked to
	// pay for. The csr argument only filters ids inside the walkers, which are
	// not called here.
	src := plan.mappedIndexSource(plan.csr, t.TempDir())
	if src.buffers != want {
		t.Fatalf("the encoder is handed %+v, the plan carries %+v: the figure "+
			"stops one hop short of what it sizes", src.buffers, want)
	}

	// And the working set the budget refuses on follows it. Measured against the
	// same fixture compacting under the default, so the structure terms are
	// identical and the whole difference is the intermediates.
	raised := plan.compactWorkingSet()

	d := bufferFixture(t, Options{}, 64)
	dplan, err := d.compactPin()
	if err != nil {
		t.Fatalf("compactPin: %v", err)
	}
	defer d.compactRelease()
	standard := dplan.compactWorkingSet()

	if want := want.workingBytes() - defaultCompactWorkingBytes; raised-standard != want {
		t.Fatalf("raising MaxWorkingBytes to %d B moved the modelled working set "+
			"by %d B, want %d B: the figure the option divides and the figure the "+
			"budget refuses on are not the same figure",
			figure, raised-standard, want)
	}
}

// The bytes a compaction writes do not depend on how much memory it was given.
//
// This is the claim that makes the option safe to expose at all. Every one of
// the four bounds an intermediate that spills to a file when it is exceeded, and
// a spill is supposed to change where the bytes are held and nothing else -- not
// their order, not their content, not the digest over them. If it did, an
// operator tuning for memory would silently be producing images that cannot be
// compared against another party's, which is what canonical serialisation exists
// to prevent.
func TestCompactOptions_TheImageIsIdenticalAtEverySetting(t *testing.T) {
	const nodes = 4000

	// The fixture has to cross the thresholds or this test asserts nothing. At
	// the floor the sort chunk is this many entries; the fixture writes two
	// entries per node, so it is past it several times over.
	floor := compactBuffersFor(minCompactWorkingBytes)
	if 2*nodes <= floor.revChunk {
		t.Fatalf("the fixture writes %d index entries and the floor's chunk holds "+
			"%d, so the sort spills nothing and this test compares two runs of "+
			"the same path", 2*nodes, floor.revChunk)
	}
	// And the value table, which is the other half: one row per distinct value,
	// eight bytes of prefix and eight of offset.
	if got := 2 * nodes * 16; got <= floor.vtabMemCap {
		t.Fatalf("the fixture's value tables come to %d B and the floor holds "+
			"%d B, so the value table spills nothing either", got, floor.vtabMemCap)
	}

	build := func(figure int64) []byte {
		s := bufferFixture(t, Options{
			Compact: CompactOptions{MaxWorkingBytes: figure},
		}, nodes)
		if s.indexMode != IndexMapped {
			t.Skipf("this platform fell back to %v, so no intermediate is sized "+
				"by the option", s.indexMode)
		}
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact at %d B: %v", figure, err)
		}
		return stripVolatile(readCSR(t, s.dir))
	}

	base := build(0)
	for _, figure := range []int64{minCompactWorkingBytes, 64 << 20} {
		got := build(figure)
		if !bytes.Equal(base, got) {
			t.Fatalf("compacting with MaxWorkingBytes=%d produced different bytes "+
				"from the default.\n%s\n\nThe size of an intermediate decides "+
				"where bytes are held while they are sorted, and must not decide "+
				"what is written.", figure, describeDiff(t, base, got))
		}
	}
}

// Lowering the figure has to lower what a real compaction allocates, end to end.
//
// Every other test here stops somewhere along the chain -- the division, the
// plan, the source handed to the encoder -- and each of those hops can be
// correct while the next one drops the sizes on the floor and takes its
// constants. Taking the constants produces a correct image, so nothing about
// the result gives it away. The allocation does.
//
// The figure the option moves between these two settings is 3,801,088 bytes and
// the assertion is a third of it, because the model is an upper bound and a
// fixture this size does not reach every one of the four: at the default its
// sort never spills, so the spill buffer the model charges for is never
// allocated. Undershooting is the direction the model is allowed to be wrong in.
func TestCompactOptions_LoweringTheFigureLowersWhatItAllocates(t *testing.T) {
	const nodes = 4000

	compactAlloc := func(figure int64) uint64 {
		t.Helper()
		s := bufferFixture(t, Options{
			Compact: CompactOptions{MaxWorkingBytes: figure},
		}, nodes)
		if s.indexMode != IndexMapped {
			t.Skipf("this platform fell back to %v, so no intermediate is sized "+
				"by the option", s.indexMode)
		}
		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		if err := s.Compact(); err != nil {
			t.Fatalf("Compact at %d B: %v", figure, err)
		}
		runtime.ReadMemStats(&m1)
		return m1.TotalAlloc - m0.TotalAlloc
	}

	standard := compactAlloc(0)
	floor := compactAlloc(minCompactWorkingBytes)

	const least = 1 << 20
	if standard < floor+least {
		t.Fatalf("compacting under the default allocated %d B and under the "+
			"%d B floor %d B, a saving of %d B: the figure is accepted and "+
			"divided and then not used by whatever actually allocates",
			standard, minCompactWorkingBytes, floor, int64(standard)-int64(floor))
	}
	t.Logf("default %d B, floor %d B, saved %d B of a modelled %d B",
		standard, floor, standard-floor,
		defaultCompactWorkingBytes-minCompactWorkingBytes)
}

// The merge's two bounds were constants read inside emit until now, so nothing
// could set them small and nothing did. They can be set small now.
func TestGPIRSorter_MergesUnderATinyReadBudget(t *testing.T) {
	const entries = 4000
	b := compactBuffers{
		revChunk:     4,                 // 1,000 runs
		mergeBudget:  4 * gpirEntrySize, // shared out, it is under the floor per run
		maxRunBuffer: gpirEntrySize,     // and so is the per-run cap
	}
	s := newGPIRSorter(t.TempDir(), b)
	defer s.close()

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)

	// Descending in, so a merge that dropped a run or read one out of order
	// cannot be mistaken for the input.
	for i := entries; i > 0; i-- {
		s.add(gpirEntry{ID: uint64(i), KeyID: uint16(i % 7), ValueOff: uint64(i)})
	}

	var got []gpirEntry
	if err := s.emit(func(e gpirEntry) { got = append(got, e) }); err != nil {
		t.Fatalf("emit: %v", err)
	}
	runtime.ReadMemStats(&m1)

	// The sizes have to be the ones asked for and not the constants. Correctness
	// would not tell us: a sorter that ignored every field would sort this
	// perfectly and hold four megabytes doing it. The ceiling is generous
	// against the few hundred kilobytes the sizes above come to -- the collected
	// output is most of it -- and a hundredth of what the three constants cost.
	const ceiling = 1 << 20
	if held := m1.TotalAlloc - m0.TotalAlloc; held > ceiling {
		t.Errorf("sorting %d entries under %+v allocated %d B, past the %d B "+
			"ceiling: a threshold is taking its constant rather than the size it "+
			"was given", entries, b, held, ceiling)
	}
	if len(got) != entries {
		t.Fatalf("the merge yielded %d of %d entries", len(got), entries)
	}
	for i := range got {
		if got[i].ID != uint64(i+1) {
			t.Fatalf("entry %d of %d is id %d, want %d: a merge under a budget "+
				"smaller than one run's floor reordered the section",
				i, len(got), got[i].ID, i+1)
		}
	}
}

// bufferFixture is a store with enough distinct indexed values to cross the
// floor's thresholds, and nothing else: this file tests the sizes, and the
// shapes that make the *structure* terms interesting are budget_test.go's.
func bufferFixture(t *testing.T, opts Options, nodes int) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < nodes; i++ {
		id, err := s.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeEvidenceFile},
		})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		// Two keys, every value distinct, so the value table has as many rows as
		// the reverse section has entries.
		if err := s.IndexNodeProperty(id, "bucket", fmt.Appendf(nil, "b%08d", i)); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
		if err := s.IndexNodeProperty(id, "digest", fmt.Appendf(nil, "d%08d", i)); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	return s
}
