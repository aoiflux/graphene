package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/aoiflux/graphene/index"
)

// The forward-only cursor, and the search it is built on.
//
// searchFrom is the one piece of arithmetic R3e(c) adds, and it is the kind that
// is easy to get almost right: a gallop with an off-by-one in the bracket returns
// a neighbouring value, which for a batch of digests looks like a plausible
// answer rather than a crash. So the first test here is not a scenario, it is
// exhaustive — every starting position against every value and against values
// that are absent above, below and between — judged against a linear scan of the
// same section. If the gallop disagrees with walking the values one at a time,
// there is nowhere for the disagreement to hide.

// cursorCorpus is a section with enough distinct values per key that the gallop
// actually gallops: a bisection over eleven values and a bisection over two are
// the same code path, and neither exercises the doubling.
//
// The values are deliberately not uniform. "v%03d" shares a prefix with its
// neighbours for the first four bytes, so most comparisons are decided inside the
// vtab; the digest key's values differ in the first byte, so they are decided
// there too but at a different point in the prefix; and the "long" key's values
// agree for the whole eight-byte prefix and differ only past it, which is the
// case that forces compareAt to read the run on almost every probe. A search that
// was correct only when the prefix could decide it would pass on two of the three
// and fail here.
func cursorCorpus() []propTriple {
	var out []propTriple
	for i := 0; i < 200; i++ {
		out = append(out, propTriple{gpixKindNode, "seq", fmt.Sprintf("v%03d", i), uint64(i + 1)})
	}
	for i := 0; i < 60; i++ {
		out = append(out, propTriple{gpixKindNode, "digest",
			fmt.Sprintf("%c-digest-value", 'a'+byte(i%26)) + fmt.Sprintf("%02d", i), uint64(1000 + i)})
	}
	for i := 0; i < 40; i++ {
		// Eight bytes of "prefixed" then the discriminator, so every prefix in
		// this key's vtab is identical.
		out = append(out, propTriple{gpixKindNode, "long",
			fmt.Sprintf("prefixed%04d", i), uint64(2000 + i)})
	}
	for i := 0; i < 30; i++ {
		out = append(out, propTriple{gpixKindEdge, "rel", fmt.Sprintf("r%02d", i), uint64(i + 1)})
	}
	// A value several entities share, so a run with more than one id is walked.
	out = append(out,
		propTriple{gpixKindNode, "seq", "v007", 5001},
		propTriple{gpixKindNode, "seq", "v007", 5002},
		propTriple{gpixKindNode, "seq", "v007", 5003})
	return out
}

// valuesOf reads a key's values out of the section in order, which is the linear
// scan every assertion below is judged against.
func valuesOf(t testing.TB, k *gpixKey) []string {
	t.Helper()
	out := make([]string, 0, k.Distinct)
	for i := uint64(0); i < k.Distinct; i++ {
		v, _, err := k.runAt(i)
		if err != nil {
			t.Fatalf("runAt(%d): %v", i, err)
		}
		out = append(out, string(v))
	}
	return out
}

// linearSearchFrom is what searchFrom must agree with: the first position at or
// after from whose value is not less than want.
func linearSearchFrom(vals []string, from int, want string) (int, bool) {
	for i := from; i < len(vals); i++ {
		if vals[i] >= want {
			return i, vals[i] == want
		}
	}
	return len(vals), false
}

func TestSearchFrom_AgreesWithALinearScanFromEveryPosition(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	for _, keyName := range []string{"seq", "digest", "long"} {
		k := pair.sec.key(gpixKindNode, keyName)
		if k == nil {
			t.Fatalf("key %q absent from the section", keyName)
		}
		vals := valuesOf(t, k)

		// Every present value, plus one just below and one just above each, plus
		// values outside the key's range at both ends. The "just below" cases are
		// where a bracket off by one shows up: the answer is the value itself and
		// found is false, which a gallop that overshot by one reports as the next
		// value and true.
		var wants []string
		for _, v := range vals {
			wants = append(wants, v, v+"\x00", v[:len(v)-1])
		}
		wants = append(wants, "", "\x00", "zzzzzzzzzzzzzzzzzzzz", vals[0]+"\x00\x00")

		for from := 0; from <= len(vals); from++ {
			for _, want := range wants {
				gotIdx, gotFound, err := k.searchFrom(uint64(from), []byte(want))
				if err != nil {
					t.Fatalf("%s: searchFrom(%d, %q): %v", keyName, from, want, err)
				}
				wantIdx, wantFound := linearSearchFrom(vals, from, want)
				if int(gotIdx) != wantIdx || gotFound != wantFound {
					t.Fatalf("%s: searchFrom(%d, %q) = (%d, %v), linear scan says (%d, %v)",
						keyName, from, want, gotIdx, gotFound, wantIdx, wantFound)
				}
			}
		}
	}
}

// TestSearch_IsSearchFromZero is the claim that the point lookup did not change.
//
// searchFrom(0) must bisect and not gallop, because the point lookup is the hot
// path and the gallop is there for a caller it does not have. The observable half
// of that is this: search and searchFrom(0) are the same answer for every input.
// The unobservable half — that it is the same *work* — is the reason
// BenchmarkGPIXBaseLookup is in the A/B for this change.
func TestSearch_IsSearchFromZero(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	for _, keyName := range []string{"seq", "digest", "long"} {
		k := pair.sec.key(gpixKindNode, keyName)
		vals := valuesOf(t, k)
		for _, want := range append(append([]string{}, vals...), "", "aa", "zzzz") {
			gi, gf, err := k.search([]byte(want))
			if err != nil {
				t.Fatalf("search(%q): %v", want, err)
			}
			fi, ff, err := k.searchFrom(0, []byte(want))
			if err != nil {
				t.Fatalf("searchFrom(0, %q): %v", want, err)
			}
			if gi != fi || gf != ff {
				t.Fatalf("%s: search(%q) = (%d,%v) but searchFrom(0) = (%d,%v)",
					keyName, want, gi, gf, fi, ff)
			}
		}
	}
}

func TestGPIXCursor_AnswersEveryValueAsLookupDoes(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	k := pair.sec.key(gpixKindNode, "seq")
	vals := valuesOf(t, k)

	cur := pair.base.Cursor(index.NodeKind, "seq")
	for _, v := range vals {
		run, found, err := cur.Seek([]byte(v))
		if err != nil {
			t.Fatalf("Seek(%q): %v", v, err)
		}
		if !found {
			t.Fatalf("Seek(%q) reports absent, but it is a value of the key", v)
		}
		want, err := pair.base.Lookup(index.NodeKind, "seq", []byte(v))
		if err != nil {
			t.Fatalf("Lookup(%q): %v", v, err)
		}
		if !sameRun(run, want) {
			t.Fatalf("Seek(%q) = %v, Lookup says %v", v, runIDs(run), runIDs(want))
		}
	}
}

// TestGPIXCursor_InterleavesPresentAndAbsentValues is the batch's real input:
// most of a caller's values are not in the index.
//
// The absent ones matter twice over. They must report absent, and they must leave
// the cursor somewhere a later present value can still be found — a cursor that
// advanced past the value it did not find would answer "absent" for the next one
// too, which is the failure a test of present values only cannot see.
func TestGPIXCursor_InterleavesPresentAndAbsentValues(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	k := pair.sec.key(gpixKindNode, "seq")
	vals := valuesOf(t, k)

	var wants []string
	var expect []bool
	for _, v := range vals {
		wants = append(wants, v, v+"\x00")
		expect = append(expect, true, false)
	}
	// Sorted, because that is the cursor's contract; v+"\x00" sorts directly
	// after v, so this interleaving is already ascending as built. Assert that
	// rather than trusting it.
	for i := 1; i < len(wants); i++ {
		if wants[i-1] > wants[i] {
			t.Fatalf("the test's own wants are not ascending at %d", i)
		}
	}

	cur := pair.base.Cursor(index.NodeKind, "seq")
	for i, w := range wants {
		run, found, err := cur.Seek([]byte(w))
		if err != nil {
			t.Fatalf("Seek(%q): %v", w, err)
		}
		if found != expect[i] {
			t.Fatalf("Seek(%q) found = %v, want %v", w, found, expect[i])
		}
		if !found && run.Len() != 0 {
			t.Fatalf("Seek(%q) reports absent but hands back %d ids", w, run.Len())
		}
	}
}

// TestGPIXCursor_RepeatedValueIsFoundAgain is why the cursor stops at the answer
// rather than past it. A batch may name one value twice and both positions are
// the caller's to hear about.
func TestGPIXCursor_RepeatedValueIsFoundAgain(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	cur := pair.base.Cursor(index.NodeKind, "seq")
	want, err := pair.base.Lookup(index.NodeKind, "seq", []byte("v007"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		run, found, err := cur.Seek([]byte("v007"))
		if err != nil {
			t.Fatalf("seek %d: %v", i, err)
		}
		if !found || !sameRun(run, want) {
			t.Fatalf("seek %d: found = %v, ids = %v, want %v", i, found, runIDs(run), runIDs(want))
		}
	}
	// And the cursor still moves on afterwards.
	if _, found, err := cur.Seek([]byte("v008")); err != nil || !found {
		t.Fatalf("after repeats, Seek(v008) = (%v, %v)", found, err)
	}
}

func TestGPIXCursor_RefusesToSeekBackward(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	cur := pair.base.Cursor(index.NodeKind, "seq")
	if _, _, err := cur.Seek([]byte("v100")); err != nil {
		t.Fatal(err)
	}
	_, _, err := cur.Seek([]byte("v050"))
	if err == nil {
		t.Fatal("a descending seek was answered rather than refused; a forward-only " +
			"cursor can only answer it wrongly")
	}
	// The message must name both values, because the whole value of refusing is
	// telling the caller which of its values is out of order.
	if !bytes.Contains([]byte(err.Error()), []byte("7630")) { // hex of "v0"
		t.Fatalf("error does not name the values: %v", err)
	}
	// An equal seek is not a descent. This is asserted here rather than only in
	// the repeat test because the guard and the repeat rule are one comparison:
	// a guard written with <= would pass every test above and break this.
	cur2 := pair.base.Cursor(index.NodeKind, "seq")
	if _, _, err := cur2.Seek([]byte("v100")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cur2.Seek([]byte("v100")); err != nil {
		t.Fatalf("an equal seek was refused as a descent: %v", err)
	}
}

// TestGPIXCursor_GuardSurvivesACallerReusingItsBuffer is why last is a copy.
//
// A caller decoding values into one scratch buffer hands the cursor the same
// slice every time with different contents. A guard that kept the slice would be
// comparing a value against itself.
func TestGPIXCursor_GuardSurvivesACallerReusingItsBuffer(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	cur := pair.base.Cursor(index.NodeKind, "seq")
	buf := make([]byte, 4)
	for i := 0; i < 50; i++ {
		copy(buf, fmt.Sprintf("v%03d", i))
		if _, found, err := cur.Seek(buf); err != nil || !found {
			t.Fatalf("i=%d: found = %v, err = %v", i, found, err)
		}
	}
	// Now go backward with the same buffer. If last aliased buf, the comparison
	// would be buf against buf and this would be accepted.
	copy(buf, "v001")
	if _, _, err := cur.Seek(buf); err == nil {
		t.Fatal("a descending seek through a reused buffer was accepted, so the " +
			"cursor is comparing the caller's memory against itself")
	}
}

func TestGPIXCursor_AbsentKeyFindsNothingAndIsNotNil(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	cur := pair.base.Cursor(index.NodeKind, "never-declared")
	if cur == nil {
		t.Fatal("Cursor returned nil for an absent key; the batch path would have " +
			"to check for it before merging a delta that may well hold the key")
	}
	for _, v := range []string{"a", "b", "c"} {
		run, found, err := cur.Seek([]byte(v))
		if err != nil {
			t.Fatalf("Seek(%q) on an absent key: %v", v, err)
		}
		if found || run.Len() != 0 {
			t.Fatalf("Seek(%q) on an absent key found %d ids", v, run.Len())
		}
	}
	// The ordering guard still applies: an absent key is not a licence to pass a
	// descending sequence, or a caller's bug would surface only on the stores
	// whose image happens to carry the key.
	if _, _, err := cur.Seek([]byte("a")); err == nil {
		t.Fatal("a descending seek on an absent key was accepted")
	}
}

// TestGPIXCursor_PropagatesACorruptRun: a search that cannot read a run must fail
// rather than report the value absent. Same position search has always taken, now
// reachable through a second entry point.
func TestGPIXCursor_PropagatesACorruptRun(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	k := pair.sec.key(gpixKindNode, "long")
	if k == nil {
		t.Fatal("key absent")
	}
	vals := valuesOf(t, k)

	// "long" is the key whose prefixes are all equal, so every probe reads a run
	// and corrupting one is reached by any search that lands on it. Overwrite the
	// declared value length of the last run with something past the run's end.
	end := k.runOffsetAt(k.Distinct - 1)
	for i := 0; i < 4; i++ {
		k.runs[int(end)+i] = 0xff
	}

	cur := pair.base.Cursor(index.NodeKind, "long")
	var sawErr bool
	for _, v := range vals {
		if _, _, err := cur.Seek([]byte(v)); err != nil {
			sawErr = true
			break
		}
	}
	if !sawErr {
		t.Fatal("a corrupt run was walked past without an error; a value the file " +
			"can no longer describe would be reported absent")
	}
}

// TestGPIXCursor_PositionTracksTheAnswer is the sweep itself, asserted rather
// than inferred.
//
// A cursor that quietly restarted from zero on every Seek would give correct
// answers to every other test in this file and cost exactly what calling Lookup
// per value costs — the item would be a no-op and nothing would say so. What
// separates the two is not the answer but the position, so this reads it: after
// seeking a value, the cursor must be at that value's own index, and after seeking
// an absent one it must be at the first value above it. Both are checked against a
// linear scan.
func TestGPIXCursor_PositionTracksTheAnswer(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	k := pair.sec.key(gpixKindNode, "seq")
	vals := valuesOf(t, k)

	// Present values and the absent value directly above each, ascending.
	var wants []string
	for _, v := range vals {
		wants = append(wants, v, v+"\x00")
	}

	cur, ok := pair.base.Cursor(index.NodeKind, "seq").(*gpixCursor)
	if !ok {
		t.Fatal("Cursor did not return a *gpixCursor; this test reads its position")
	}
	for _, w := range wants {
		if _, _, err := cur.Seek([]byte(w)); err != nil {
			t.Fatalf("Seek(%q): %v", w, err)
		}
		wantIdx, _ := linearSearchFrom(vals, 0, w)
		if int(cur.at) != wantIdx {
			t.Fatalf("after Seek(%q) the cursor is at %d; the value sits at %d",
				w, cur.at, wantIdx)
		}
	}
	// And it never went backward: the last want is above every value, so the
	// position is the end of the key.
	if _, _, err := cur.Seek([]byte("zzzzzzzz")); err != nil {
		t.Fatal(err)
	}
	if cur.at != k.Distinct {
		t.Fatalf("after seeking past the last value the cursor is at %d of %d",
			cur.at, k.Distinct)
	}
}

// BenchmarkGPIXCursorSweep is where the cost claim belongs.
//
// Two arms over one section and one sorted batch: a search per value, and the
// cursor carrying its position. The answers are identical by
// TestGPIXCursor_AgreesWithLookupOverARandomisedBatch, so the only difference the
// benchmark can report is the work — which is the item.
func BenchmarkGPIXCursorSweep(b *testing.B) {
	pair := newBasePair(b, cursorCorpus())
	k := pair.sec.key(gpixKindNode, "seq")
	batch := make([][]byte, 0, k.Distinct)
	for i := uint64(0); i < k.Distinct; i++ {
		v, _, err := k.runAt(i)
		if err != nil {
			b.Fatal(err)
		}
		batch = append(batch, append([]byte(nil), v...))
	}

	b.Run("SearchPerValue", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, v := range batch {
				if _, _, err := k.search(v); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("CursorSweep", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			cur := pair.base.Cursor(index.NodeKind, "seq")
			for _, v := range batch {
				if _, _, err := cur.Seek(v); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// BenchmarkGPIXSweepScale is where the monotonic search has to earn its place.
//
// BenchmarkGPIXCursorSweep above measures two hundred values and the cursor comes
// out behind, which is the honest answer at that size: three kilobytes of vtab is
// eight L1 hits for a bisection, and the position, the backward guard and the
// interface call cost more than eight L1 hits. The claim is about the size where a
// probe is a cache miss and, on a cold image, a page fault — a key with a million
// distinct values is sixteen megabytes of table — so the benchmark has to go
// there.
//
// 2^8 to 2^18 distinct 32-byte values: 4 KiB to 4 MiB of vtab, so the last arms
// are past this machine's L2 and the first is inside its L1. Both arms resolve
// every value the key holds, ascending, which is the shape a join over a sorted
// table produces; ns/value is reported so the arms are comparable across sizes and
// the crossover is visible rather than asserted.
func BenchmarkGPIXSweepScale(b *testing.B) {
	for _, bits := range []uint{8, 12, 16, 18} {
		distinct := 1 << bits
		triples := make([]propTriple, 0, distinct)
		for i := 0; i < distinct; i++ {
			triples = append(triples, propTriple{gpixKindNode, "k", scaleValue(i), uint64(i + 1)})
		}
		pair := newBasePair(b, triples)
		k := pair.sec.key(gpixKindNode, "k")
		if k == nil || k.Distinct != uint64(distinct) {
			b.Fatalf("2^%d: the section holds %v distinct values", bits, k)
		}
		batch := make([][]byte, 0, distinct)
		for _, v := range valuesOf(b, k) {
			batch = append(batch, []byte(v))
		}

		name := fmt.Sprintf("2^%d", bits)
		b.Run("SearchPerValue/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, v := range batch {
					if _, _, err := k.search(v); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*distinct), "ns/value")
		})
		b.Run("CursorSweep/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cur := pair.base.Cursor(index.NodeKind, "k")
				for _, v := range batch {
					if _, _, err := cur.Seek(v); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*distinct), "ns/value")
		})
	}
}

// scaleValue is the i'th of an ascending series of 32-byte values whose leading
// eight bytes are distinct, which is the shape of the content digest the property
// batch exists for: a prefix comparison decides almost every probe, so the arms
// differ in how many probes they make and not in how much of each probe the vtab
// can answer.
func scaleValue(i int) string {
	var v [32]byte
	binary.BigEndian.PutUint64(v[0:], uint64(i))
	binary.BigEndian.PutUint64(v[8:], uint64(i)*0x9e3779b97f4a7c15)
	binary.BigEndian.PutUint64(v[16:], ^uint64(i))
	binary.BigEndian.PutUint64(v[24:], uint64(i)<<21|uint64(i)>>11)
	return string(v[:])
}

func sameRun(a, b index.IDRun) bool {

	if a.Len() != b.Len() {
		return false
	}
	for i := 0; i < a.Len(); i++ {
		if a.At(i) != b.At(i) {
			return false
		}
	}
	return true
}

// --- the batch against a store-shaped corpus ---

// TestGPIXCursor_AgreesWithLookupOverARandomisedBatch is the differential at the
// size the gallop's bracket arithmetic can actually be wrong at: a few hundred
// distinct values, a batch drawn from them and from values that are not there,
// sorted, compared value by value against Lookup.
func TestGPIXCursor_AgreesWithLookupOverARandomisedBatch(t *testing.T) {
	pair := newBasePair(t, cursorCorpus())
	k := pair.sec.key(gpixKindNode, "digest")
	vals := valuesOf(t, k)

	rng := rand.New(rand.NewSource(20260913))
	var batch []string
	for i := 0; i < 400; i++ {
		if rng.Intn(3) == 0 {
			batch = append(batch, fmt.Sprintf("absent-%03d", rng.Intn(500)))
			continue
		}
		batch = append(batch, vals[rng.Intn(len(vals))])
	}
	sort.Strings(batch)

	cur := pair.base.Cursor(index.NodeKind, "digest")
	for _, v := range batch {
		run, found, err := cur.Seek([]byte(v))
		if err != nil {
			t.Fatalf("Seek(%q): %v", v, err)
		}
		want, err := pair.base.Lookup(index.NodeKind, "digest", []byte(v))
		if err != nil {
			t.Fatal(err)
		}
		if found != (want.Len() > 0) {
			t.Fatalf("Seek(%q) found = %v, Lookup gave %d ids", v, found, want.Len())
		}
		if found && !sameRun(run, want) {
			t.Fatalf("Seek(%q) = %v, Lookup = %v", v, runIDs(run), runIDs(want))
		}
	}
}
