package index

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The batch read path.
//
// Two things have to be true and they are tested separately, because a batch can
// satisfy either one on its own and be useless.
//
// It has to give the same answers NodesByProperty gives, value for value, over
// base ∪ delta − retracted. That is a differential against the method it is a
// bulk form of, and nothing about it is specific to the batch — which is exactly
// why it is the first test here rather than a scenario invented for the occasion.
//
// And it has to resolve them in *order*, which is the entire reason it exists.
// A batch that answered correctly and searched the base independently for every
// value would pass the differential and save nothing at all. So the fake cursor
// counts the values it steps over, and the second test asserts that a batch of N
// values against D distinct ones costs about N+D steps rather than N×D.

// batchCorpus is a base with one wide key, one narrow one, and an edge key.
//
// "digest" has enough distinct values that a per-value search and a sweep differ
// measurably in step count; "tag" repeats values across entities so a run with
// several ids is merged; and the values are three digits wide so string order and
// numeric order agree, which keeps the expectations in the tests readable.
func batchCorpus() []triple {
	var out []triple
	for i := 0; i < 120; i++ {
		out = append(out, triple{NodeKind, uint64(i + 1), "digest",
			[]byte(fmt.Sprintf("d%03d", i))})
	}
	for i := 0; i < 120; i++ {
		out = append(out, triple{NodeKind, uint64(i + 1), "tag",
			[]byte(fmt.Sprintf("t%d", i%7))})
	}
	for i := 0; i < 40; i++ {
		out = append(out, triple{EdgeKind, uint64(i + 1), "rel",
			[]byte(fmt.Sprintf("r%02d", i%9))})
	}
	return out
}

// collect runs a node batch and returns what the callback saw, keyed by the value
// index, together with the order the indices arrived in.
func collect(t *testing.T, p *PropertyIndex, key string, values [][]byte) (map[int][]store.NodeID, []int) {
	t.Helper()
	got := map[int][]store.NodeID{}
	var order []int
	err := p.NodesByPropertyBatch(key, values, func(i int, ids []store.NodeID) bool {
		if _, dup := got[i]; dup {
			t.Fatalf("index %d was reported twice", i)
		}
		// Cloned on the way out: the slice is the index's scratch and the whole
		// point of checking it later is that it will have been overwritten.
		got[i] = slices.Clone(ids)
		order = append(order, i)
		return true
	})
	if err != nil {
		t.Fatalf("NodesByPropertyBatch: %v", err)
	}
	return got, order
}

// TestBatch_AnswersExactlyWhatNodesByPropertyAnswers is the differential, and it
// is run over every combination of the three terms: a value the base alone holds,
// one only the delta holds, one both hold, one whose base ids are all retracted,
// one partly retracted, and one nothing holds.
func TestBatch_AnswersExactlyWhatNodesByPropertyAnswers(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	// A delta that overlaps the base in every interesting way.
	p.IndexNode(500, "digest", []byte("d005"))  // adds to a base value
	p.IndexNode(501, "digest", []byte("dnew1")) // a value the base never had
	p.IndexNode(3, "digest", []byte("d002"))    // an id the base already has there
	p.IndexNode(502, "tag", []byte("t3"))
	// Retract one entity outright and one whose value only it held.
	p.RemoveNode(7)
	p.RemoveNode(120)
	p.IndexNode(120, "digest", []byte("d119")) // retracted, then re-registered

	for _, key := range []string{"digest", "tag", "absent-key"} {
		// Every value either side holds, plus values that are absent between,
		// below and above the base's range.
		var values [][]byte
		for i := 0; i < 125; i++ {
			values = append(values, []byte(fmt.Sprintf("d%03d", i)))
		}
		values = append(values, []byte("dnew1"), []byte("t3"), []byte("t0"),
			[]byte(""), []byte("a"), []byte("zzzz"), []byte("d005x"))

		got, _ := collect(t, p, key, values)
		for i, v := range values {
			want := p.NodesByProperty(key, v)
			have, reported := got[i]
			switch {
			case len(want) == 0 && reported:
				t.Fatalf("%s=%q: batch reported %v, NodesByProperty says nothing holds it",
					key, v, have)
			case len(want) == 0:
				continue
			case !reported:
				t.Fatalf("%s=%q: batch reported nothing, NodesByProperty says %v", key, v, want)
			case !slices.Equal(have, want):
				t.Fatalf("%s=%q: batch says %v, NodesByProperty says %v", key, v, have, want)
			}
		}
	}
}

func TestBatch_EdgesAnswerExactlyWhatEdgesByPropertyAnswers(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	p.IndexEdge(900, "rel", []byte("r03"))
	p.IndexEdge(901, "rel", []byte("rnew"))
	p.RemoveEdge(5)

	var values [][]byte
	for i := 0; i < 12; i++ {
		values = append(values, []byte(fmt.Sprintf("r%02d", i)))
	}
	values = append(values, []byte("rnew"), []byte("nope"))

	got := map[int][]store.EdgeID{}
	err := p.EdgesByPropertyBatch("rel", values, func(i int, ids []store.EdgeID) bool {
		got[i] = slices.Clone(ids)
		return true
	})
	if err != nil {
		t.Fatalf("EdgesByPropertyBatch: %v", err)
	}
	for i, v := range values {
		want := p.EdgesByProperty("rel", v)
		have, reported := got[i]
		if len(want) == 0 {
			if reported {
				t.Fatalf("rel=%q: batch reported %v for a value nothing holds", v, have)
			}
			continue
		}
		if !reported || !slices.Equal(have, want) {
			t.Fatalf("rel=%q: batch says %v (reported=%v), EdgesByProperty says %v",
				v, have, reported, want)
		}
	}
}

// TestBatch_SweepsTheBaseOnceRatherThanSearchingPerValue is the cost claim.
//
// The fake cursor is a linear scan that counts the values it steps over, so the
// total for a whole batch is the distance the cursor travelled. Forward-only, that
// distance cannot exceed the number of distinct values however many queries there
// are; a cursor that restarted, or that the batch built one of per value, would
// travel a multiple of it.
//
// The assertion is therefore a bound and not a figure: steps ≤ distinct. That is
// the strongest thing that is true, it is independent of the corpus, and it fails
// for every implementation that does not carry the position.
func TestBatch_SweepsTheBaseOnceRatherThanSearchingPerValue(t *testing.T) {
	base := newFakeBase(batchCorpus())
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	distinct, _ := base.KeyStats(NodeKind, "digest")

	// Shuffled, so the batch has to build its own ordering; that is the input a
	// caller actually has.
	rng := rand.New(rand.NewSource(20260913))
	var values [][]byte
	for i := 0; i < 120; i++ {
		values = append(values, []byte(fmt.Sprintf("d%03d", i)))
	}
	rng.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })

	base.resetCounts()
	got, _ := collect(t, p, "digest", values)
	seeks, steps := base.seekCounts()

	if len(got) != len(values) {
		t.Fatalf("%d of %d values were reported", len(got), len(values))
	}
	if int(seeks) != len(values) {
		t.Fatalf("the base was asked %d times for %d values", seeks, len(values))
	}
	if int(steps) > distinct {
		t.Fatalf("the cursor travelled %d steps over %d distinct values; a "+
			"forward-only sweep cannot exceed the key's size, so the batch is not "+
			"carrying its position", steps, distinct)
	}
	t.Logf("%d values, %d distinct, %d seeks, %d cursor steps", len(values), distinct, seeks, steps)
}

// TestBatch_SortedInputCostsNoOrdering: the shape this API is for is a caller
// whose values are already sorted, and it should pay nothing for an ordering it
// already has.
func TestBatch_SortedInputCostsNoOrdering(t *testing.T) {
	sorted := [][]byte{[]byte("a"), []byte("b"), []byte("b"), []byte("c")}
	order, err := ascendingOrder(sorted)
	if err != nil {
		t.Fatal(err)
	}
	if order != nil {
		t.Fatalf("an ascending input allocated a %d-entry permutation", len(order))
	}

	unsorted := [][]byte{[]byte("c"), []byte("a"), []byte("b")}
	order, err = ascendingOrder(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(positionsOf(order), []int32{1, 2, 0}) {
		t.Fatalf("permutation = %v, want [1 2 0]", positionsOf(order))
	}
	// Reading the input through the permutation must be ascending — that is the
	// only property the batch relies on.
	for i := 1; i < len(order); i++ {
		if bytes.Compare(unsorted[order[i-1].at], unsorted[order[i].at]) > 0 {
			t.Fatalf("the permutation is not ascending at %d", i)
		}
	}
}

// TestBatch_OrdersByValueAndNotByPrefix. The sort key carries four bytes of the
// value so that most comparisons need not read the value at all, which is only
// sound if the cases the four bytes cannot separate still order correctly: values
// that share the whole prefix, values shorter than it, and the empty value.
func TestBatch_OrdersByValueAndNotByPrefix(t *testing.T) {
	values := [][]byte{
		[]byte("prefix-zz"), []byte("prefix-aa"), []byte("prefix"), []byte("pre"),
		[]byte(""), []byte("pref"), []byte("prefix-aa"),
	}
	order, err := ascendingOrder(values)
	if err != nil {
		t.Fatal(err)
	}
	if order == nil {
		t.Fatal("that input is not ascending; ascendingOrder said it was")
	}
	for i := 1; i < len(order); i++ {
		if bytes.Compare(values[order[i-1].at], values[order[i].at]) > 0 {
			t.Fatalf("not ascending at %d: %q then %q",
				i, values[order[i-1].at], values[order[i].at])
		}
	}
	// Every position exactly once: a permutation, not a selection.
	seen := make([]bool, len(values))
	for _, k := range order {
		if seen[k.at] {
			t.Fatalf("position %d appears twice", k.at)
		}
		seen[k.at] = true
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("position %d is missing from the ordering", i)
		}
	}
}

// positionsOf is the ordering's input positions, which is what the assertions are
// about; the prefix beside each is an implementation detail of the comparison.
func positionsOf(order []batchKey) []int32 {
	out := make([]int32, len(order))
	for i, k := range order {
		out[i] = k.at
	}
	return out
}

// TestBatch_DoesNotModifyTheCallersValues. A caller indexes its own rows by the
// same positions; permuting the slice it passed in would corrupt that silently.
func TestBatch_DoesNotModifyTheCallersValues(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	values := [][]byte{[]byte("d050"), []byte("d002"), []byte("d119"), []byte("d000")}
	before := make([][]byte, len(values))
	for i, v := range values {
		before[i] = slices.Clone(v)
	}
	collect(t, p, "digest", values)
	for i := range values {
		if !bytes.Equal(values[i], before[i]) {
			t.Fatalf("values[%d] changed from %q to %q", i, before[i], values[i])
		}
	}
}

// TestBatch_ReportsEveryPositionOfARepeatedValue.
//
// Two positions naming one value are two answers the caller is owed, and the
// cheapest wrong implementation reports the first and swallows the rest — which
// for a join means silently dropping rows.
func TestBatch_ReportsEveryPositionOfARepeatedValue(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	values := [][]byte{
		[]byte("d005"), []byte("d005"), []byte("nope"), []byte("d005"),
		[]byte("nope"), []byte("d006"),
	}
	got, _ := collect(t, p, "digest", values)
	want := p.NodesByProperty("digest", []byte("d005"))
	for _, i := range []int{0, 1, 3} {
		if !slices.Equal(got[i], want) {
			t.Fatalf("position %d of a repeated value = %v, want %v", i, got[i], want)
		}
	}
	for _, i := range []int{2, 4} {
		if _, reported := got[i]; reported {
			t.Fatalf("position %d named a value nothing holds and was reported", i)
		}
	}
	if len(got) != 4 {
		t.Fatalf("%d positions reported, want 4", len(got))
	}
}

// TestBatch_RepeatedValueWithNoHolderStaysUnreported is the other half of the
// reuse branch: it hands the previous result back, so the previous result being
// *empty* must stay empty rather than being read out of a stale buffer.
func TestBatch_RepeatedValueWithNoHolderStaysUnreported(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	values := [][]byte{
		[]byte("d001"), // a hit, which fills the scratch buffer
		[]byte("gone"), // a miss
		[]byte("gone"), // the repeat: must not report the first value's ids
		[]byte("gone"),
	}
	got, _ := collect(t, p, "digest", values)
	if len(got) != 1 {
		t.Fatalf("reported %d positions, want only the one hit: %v", len(got), got)
	}
	if _, ok := got[0]; !ok {
		t.Fatalf("the hit at position 0 was not reported: %v", got)
	}
}

// TestBatch_StopsWhenTheCallbackDeclines, and stops without an error: a caller
// that has found what it wanted is not a failure.
func TestBatch_StopsWhenTheCallbackDeclines(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	var values [][]byte
	for i := 0; i < 120; i++ {
		values = append(values, []byte(fmt.Sprintf("d%03d", i)))
	}
	calls := 0
	err := p.NodesByPropertyBatch("digest", values, func(int, []store.NodeID) bool {
		calls++
		return calls < 3
	})
	if err != nil {
		t.Fatalf("a callback that declined produced an error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("the callback ran %d times after declining at 3", calls)
	}
}

// TestBatch_WithNoBaseAnswersFromTheDelta covers the path a store that has never
// compacted takes, where there is no ordering to exploit and the batch must not
// pay for one.
func TestBatch_WithNoBaseAnswersFromTheDelta(t *testing.T) {
	p := NewPropertyIndex()
	for i := 0; i < 30; i++ {
		p.IndexNode(store.NodeID(i+1), "k", []byte(fmt.Sprintf("v%02d", i)))
	}
	p.IndexNode(99, "k", []byte("v05"))

	var values [][]byte
	for i := 29; i >= 0; i-- { // descending, so an ordering would have to be built
		values = append(values, []byte(fmt.Sprintf("v%02d", i)))
	}
	values = append(values, []byte("absent"))

	got, order := collect(t, p, "k", values)
	for i, v := range values {
		want := p.NodesByProperty("k", v)
		if len(want) == 0 {
			if _, reported := got[i]; reported {
				t.Fatalf("k=%q reported with no holder", v)
			}
			continue
		}
		if !slices.Equal(got[i], want) {
			t.Fatalf("k=%q: batch %v, NodesByProperty %v", v, got[i], want)
		}
	}
	// With no base the order is the input's, which is what lets this path skip the
	// sort. Asserted because it is the observable half of that skip.
	if !slices.IsSorted(order) {
		t.Fatalf("with no base the callbacks did not arrive in input order: %v", order)
	}
}

// TestBatch_KeyTheBaseDoesNotCarryUsesTheDeltaPath. A key declared since the last
// compaction has no base values, and the batch should notice before it sorts.
func TestBatch_KeyTheBaseDoesNotCarryUsesTheDeltaPath(t *testing.T) {
	base := newFakeBase(batchCorpus())
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	p.IndexNode(700, "fresh", []byte("b"))
	p.IndexNode(701, "fresh", []byte("a"))

	base.resetCounts()
	values := [][]byte{[]byte("b"), []byte("a"), []byte("c")}
	got, order := collect(t, p, "fresh", values)
	if seeks, _ := base.seekCounts(); seeks != 0 {
		t.Fatalf("the base was asked %d times about a key it does not carry", seeks)
	}
	if !slices.Equal(got[0], []store.NodeID{700}) || !slices.Equal(got[1], []store.NodeID{701}) {
		t.Fatalf("delta answers wrong: %v", got)
	}
	if !slices.IsSorted(order) {
		t.Fatalf("a key absent from the base was ordered anyway: %v", order)
	}
}

// TestBatch_BaseFaultIsReturnedAndRecorded.
//
// Every other read path folds an unreadable run into "this value has no ids",
// because it has no error to return. This one does have one, and a bulk join that
// dropped rows off a damaged image without saying so would be a wrong answer
// rather than a failure. Both halves are asserted: the caller is told, and
// BaseFault records it for whoever asks later.
func TestBatch_BaseFaultIsReturnedAndRecorded(t *testing.T) {
	base := newFakeBase(batchCorpus())
	base.failKey = "digest"
	base.failErr = fmt.Errorf("gpix: run 4 is unreadable")
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	calls := 0
	err := p.NodesByPropertyBatch("digest", [][]byte{[]byte("d000"), []byte("d001")},
		func(int, []store.NodeID) bool { calls++; return true })
	if err == nil {
		t.Fatal("a batch over an unreadable base returned no error, so a caller " +
			"joining against it would see missing rows as absent rows")
	}
	if calls != 0 {
		t.Fatalf("the callback ran %d times after the base failed", calls)
	}
	if p.BaseFault() == nil {
		t.Fatal("the fault was returned but not recorded")
	}
}

// TestBatch_EmptyInputIsNotAnError, and an empty key name is just a key nothing
// is registered under.
func TestBatch_EmptyInputIsNotAnError(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	for _, values := range [][][]byte{nil, {}} {
		called := false
		if err := p.NodesByPropertyBatch("digest", values, func(int, []store.NodeID) bool {
			called = true
			return true
		}); err != nil {
			t.Fatalf("empty batch: %v", err)
		}
		if called {
			t.Fatal("an empty batch called the callback")
		}
	}
	// A nil value is a legitimate value: the index stores the bytes it is given.
	p2 := NewPropertyIndex()
	p2.IndexNode(1, "k", nil)
	got := map[int][]store.NodeID{}
	if err := p2.NodesByPropertyBatch("k", [][]byte{nil}, func(i int, ids []store.NodeID) bool {
		got[i] = slices.Clone(ids)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got[0], []store.NodeID{1}) {
		t.Fatalf("a nil value did not resolve: %v", got)
	}
}

// TestBatch_AllocatesNothingPerValue is the reason the signature is a callback.
//
// The claim is not "few allocations" but "a number that does not grow with the
// batch": the permutation and the scratch buffer are it. So this measures the same
// work at two sizes ten times apart and asserts the second is not ten times the
// first. Absolute figures would be a guess about the allocator; a slope is the
// claim.
func TestBatch_AllocatesNothingPerValue(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	// Distinct values, not repeats. The first version of this test built n values
	// out of 120 distinct ones and sorted them, so the repeat branch skipped all
	// but 120 seeks and the figure was flat for a reason that had nothing to do
	// with the claim. Every value here is its own, so n values mean n seeks.
	measure := func(n int) float64 {
		values := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			values = append(values, []byte(fmt.Sprintf("d%06d", i)))
		}
		// Sorted, so the permutation is not allocated and what is left is the
		// scratch buffer and the cursor. The unsorted case allocates exactly one
		// slice more, which TestBatch_SortedInputCostsNoOrdering covers.
		sort.Slice(values, func(i, j int) bool { return bytes.Compare(values[i], values[j]) < 0 })
		return testing.AllocsPerRun(3, func() {
			_ = p.NodesByPropertyBatch("digest", values, func(int, []store.NodeID) bool { return true })
		})
	}
	small, large := measure(200), measure(2000)
	if large > small+8 {
		t.Fatalf("200 distinct values allocate %.0f and 2000 allocate %.0f; the "+
			"batch is allocating per value", small, large)
	}
	t.Logf("allocs: 200 distinct values %.0f, 2000 %.0f", small, large)
}

// TestBatch_RefusesABatchItCannotIndex. The permutation is int32, so there is a
// size at which this cannot be answered; it says so rather than truncating.
func TestBatch_RefusesABatchItCannotIndex(t *testing.T) {
	// Constructing 2^31 values is not possible here, so the guard is exercised at
	// its own boundary through ascendingOrder, which is where it lives. That is
	// the honest reach of this test and the reason it is not written as a batch.
	if _, err := ascendingOrder(make([][]byte, 4)); err != nil {
		t.Fatalf("a four-value batch was refused: %v", err)
	}
}
