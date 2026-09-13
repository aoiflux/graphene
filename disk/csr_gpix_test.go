package disk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sort"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// --- fixtures ---

// gpixFixture is the reference the encoded section is judged against: the same
// entries in the obvious structure, so a test can assert an answer rather than
// assert self-consistency.
type gpixFixture struct {
	nodes map[string]map[string][]uint64
	edges map[string]map[string][]uint64
}

func newGPIXFixture() *gpixFixture {
	return &gpixFixture{
		nodes: map[string]map[string][]uint64{},
		edges: map[string]map[string][]uint64{},
	}
}

func (f *gpixFixture) add(kind uint8, key, value string, ids ...uint64) {
	m := f.nodes
	if kind == gpixKindEdge {
		m = f.edges
	}
	if m[key] == nil {
		m[key] = map[string][]uint64{}
	}
	m[key][value] = append(m[key][value], ids...)
}

func (f *gpixFixture) keys(kind uint8) []string {
	m := f.nodes
	if kind == gpixKindEdge {
		m = f.edges
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// walker yields one key's distinct values ascending with ascending ids, which is
// the order writeGPIX requires of its source.
func (f *gpixFixture) walker(kind uint8) gpixValueWalker {
	m := f.nodes
	if kind == gpixKindEdge {
		m = f.edges
	}
	return func(key string, fn func(value []byte, ids []uint64) bool) error {
		bucket := m[key]
		vals := make([]string, 0, len(bucket))
		for v := range bucket {
			vals = append(vals, v)
		}
		sort.Strings(vals)
		for _, v := range vals {
			ids := append([]uint64(nil), bucket[v]...)
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			if !fn([]byte(v), ids) {
				return nil
			}
		}
		return nil
	}
}

// source returns the fixture as a gpixSource.
//
// The two thresholds are arguments because whether the value table and the
// reverse index are held in memory or spilled is the difference between the path
// a test-sized store takes and the path the store this program is aimed at takes.
// Zero means the production defaults, which nothing here is large enough to
// cross; the tests that care pass small ones.
func (f *gpixFixture) source(dir string, vtabCap, revChunk int) gpixSource {
	return gpixSource{
		NodeKeys:   f.keys(gpixKindNode),
		EdgeKeys:   f.keys(gpixKindEdge),
		NodeValues: f.walker(gpixKindNode),
		EdgeValues: f.walker(gpixKindEdge),
		ScratchDir: dir,
		vtabMemCap: vtabCap,
		revChunk:   revChunk,
	}
}

// encodeGPIXFrom writes one source's GPIX body and discards its reverse entries.
func encodeGPIXFrom(src gpixSource) ([]byte, error) {
	body, _, _, err := encodeMappedIndexFrom(src)
	return body, err
}

// encodeMappedIndexFrom writes both bodies, which is what a v9 image carries and
// what the reverse direction has to be judged against: a GPIR entry is only
// meaningful beside the GPIX runs its offsets address.
func encodeMappedIndexFrom(src gpixSource) (gpix, gpir []byte, dirs []gpixKeyDir, err error) {
	var fwd, rev bytes.Buffer
	iw := newImageWriter(&fwd, make([]byte, 0, 4096))
	nodeRev := newGPIRSorter(src.ScratchDir, src.revChunk)
	defer nodeRev.close()
	edgeRev := newGPIRSorter(src.ScratchDir, src.revChunk)
	defer edgeRev.close()
	if dirs, err = writeGPIX(iw, 0, src, nodeRev, edgeRev, nil); err != nil {
		return nil, nil, nil, err
	}
	if err = iw.flush(); err != nil {
		return nil, nil, nil, err
	}
	rw := newImageWriter(&rev, make([]byte, 0, 4096))
	writeGPIRHeader(rw, nodeRev.count(), edgeRev.count())
	if err = nodeRev.emit(func(e gpirEntry) { writeGPIREntry(rw, e) }); err != nil {
		return nil, nil, nil, err
	}
	if err = edgeRev.emit(func(e gpirEntry) { writeGPIREntry(rw, e) }); err != nil {
		return nil, nil, nil, err
	}
	if err = rw.flush(); err != nil {
		return nil, nil, nil, err
	}
	return fwd.Bytes(), rev.Bytes(), dirs, nil
}

// encode writes the fixture as a GPIX body.
func (f *gpixFixture) encode(t testing.TB) []byte {
	t.Helper()
	body, err := encodeGPIXFrom(f.source(t.TempDir(), 0, 0))
	if err != nil {
		t.Fatalf("writeGPIX: %v", err)
	}
	return body
}

// --- round trip ---

func TestGPIX_RoundTrip(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "sha256", "aaaa", 1, 7, 9)
	f.add(gpixKindNode, "sha256", "bbbb", 2)
	f.add(gpixKindNode, "sha256", "cccc", 3, 4)
	f.add(gpixKindNode, "bucket", "one", 1, 2, 3, 4)
	f.add(gpixKindNode, "bucket", "two", 5)
	f.add(gpixKindNode, "empty", "", 42)
	f.add(gpixKindEdge, "kind", "calls", 10, 11)

	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}

	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		want := f.nodes
		if kind == gpixKindEdge {
			want = f.edges
		}
		if got := sec.keyNames(kind); !equalStrings(got, f.keys(kind)) {
			t.Fatalf("kind %d keys = %v, want %v", kind, got, f.keys(kind))
		}
		for key, bucket := range want {
			k := sec.key(kind, key)
			if k == nil {
				t.Fatalf("kind %d key %q missing", kind, key)
			}
			if int(k.Distinct) != len(bucket) {
				t.Errorf("key %q distinct = %d, want %d", key, k.Distinct, len(bucket))
			}
			var entries int
			for _, ids := range bucket {
				entries += len(ids)
			}
			if int(k.Entries) != entries {
				t.Errorf("key %q entries = %d, want %d", key, k.Entries, entries)
			}
			for value, ids := range bucket {
				raw, err := k.idsOf([]byte(value))
				if err != nil {
					t.Fatalf("idsOf(%q, %q): %v", key, value, err)
				}
				if got := decodeIDs(raw); !equalIDs(got, sortedIDs(ids)) {
					t.Errorf("idsOf(%q, %q) = %v, want %v", key, value, got, sortedIDs(ids))
				}
			}
			// A value the key does not carry must be absent rather than
			// resolving to a neighbour.
			if raw, err := k.idsOf([]byte("\xff\xff\xff no such value")); err != nil || raw != nil {
				t.Errorf("idsOf(%q, absent) = %v, %v; want nil, nil", key, raw, err)
			}
		}
	}
	if sec.key(gpixKindNode, "nope") != nil {
		t.Error("key(nope) resolved")
	}
	// A node key must not be findable as an edge key: the directory is searched
	// by (kind, key) and collapsing the two would cross the halves.
	if sec.key(gpixKindEdge, "sha256") != nil {
		t.Error("node key sha256 resolved as an edge key")
	}
}

func TestGPIX_EmptySection(t *testing.T) {
	sec, err := parseGPIX(newGPIXFixture().encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	if len(sec.keys) != 0 {
		t.Fatalf("keys = %d, want 0", len(sec.keys))
	}
	if sec.key(gpixKindNode, "anything") != nil {
		t.Error("key resolved in an empty section")
	}
}

// TestGPIX_KeyWithNoValues covers a declared key the index holds nothing under:
// its vtab is the sentinel alone and its runs region is empty, which is the one
// case where a search must terminate without reading a run.
func TestGPIX_KeyWithNoValues(t *testing.T) {
	f := newGPIXFixture()
	f.nodes["declared"] = map[string][]uint64{}
	f.add(gpixKindNode, "other", "v", 1)

	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	k := sec.key(gpixKindNode, "declared")
	if k == nil {
		t.Fatal("declared key missing")
	}
	if k.Distinct != 0 || k.Entries != 0 {
		t.Fatalf("distinct=%d entries=%d, want 0/0", k.Distinct, k.Entries)
	}
	if ids, err := k.idsOf([]byte("v")); err != nil || ids != nil {
		t.Fatalf("idsOf on an empty key = %v, %v", ids, err)
	}
}

// --- the prefix's ordering obligation ---

// TestGPIX_PrefixOrderMatchesBytesCompare is the proof obligation the vtab's
// fast path rests on: wherever two values' eight-byte prefixes differ, their
// order must be the order bytes.Compare gives. Where the prefixes agree the
// search reads the values, so those pairs are unconstrained — and the cases that
// make that distinction necessary (a proper prefix, an embedded NUL, nine bytes
// agreeing in eight) are all here.
func TestGPIX_PrefixOrderMatchesBytesCompare(t *testing.T) {
	values := [][]byte{
		{},
		[]byte("a"),
		{'a', 0},
		{'a', 0, 0},
		[]byte("ab"),
		[]byte("abcdefg"),
		[]byte("abcdefgh"),
		[]byte("abcdefghi"),
		[]byte("abcdefgh\x00"),
		[]byte("abcdefghz"),
		{0},
		{0, 0},
		{0xff},
		bytes.Repeat([]byte{0xff}, 8),
		bytes.Repeat([]byte{0xff}, 9),
		[]byte("https://example.com/one"),
		[]byte("https://example.com/two"),
	}
	for i, a := range values {
		for j, b := range values {
			pa, pb := gpixPrefixOf(a), gpixPrefixOf(b)
			if pa == pb {
				continue
			}
			wantLess := bytes.Compare(a, b) < 0
			if (pa < pb) != wantLess {
				t.Errorf("prefix order disagrees for %d=%q and %d=%q: prefixes %#x/%#x, bytes.Compare=%d",
					i, a, j, b, pa, pb, bytes.Compare(a, b))
			}
		}
	}
}

// TestGPIX_SearchExactAcrossSharedPrefixes is the other half: a key whose values
// all agree in their first eight bytes reduces the prefix to no information at
// all, and every lookup must still be exact. This is the URL case the design
// note names, and the reason the comparator falls through to the value.
func TestGPIX_SearchExactAcrossSharedPrefixes(t *testing.T) {
	f := newGPIXFixture()
	const shared = "https://example.com/path/"
	var want []string
	for i := 0; i < 512; i++ {
		v := fmt.Sprintf("%s%04d", shared, i)
		want = append(want, v)
		f.add(gpixKindNode, "url", v, uint64(i), uint64(i+10000))
	}
	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	k := sec.key(gpixKindNode, "url")
	for i, v := range want {
		ids, err := k.idsOf([]byte(v))
		if err != nil {
			t.Fatalf("idsOf(%q): %v", v, err)
		}
		if got := decodeIDs(ids); !equalIDs(got, []uint64{uint64(i), uint64(i + 10000)}) {
			t.Fatalf("idsOf(%q) = %v", v, got)
		}
	}
	for _, absent := range []string{
		shared, shared + "0000x", shared + "9999", "https://example.co", "https://example.com0",
	} {
		if ids, err := k.idsOf([]byte(absent)); err != nil || ids != nil {
			t.Errorf("idsOf(%q) = %v, %v; want absent", absent, ids, err)
		}
	}
}

// TestGPIX_SearchLowerBoundIsOrdered checks the half of search a range query
// uses: the returned index must be the first value not less than the probe, for
// probes that fall before, between and after the values present.
func TestGPIX_SearchLowerBoundIsOrdered(t *testing.T) {
	f := newGPIXFixture()
	present := []string{"b", "d", "f", "h"}
	for i, v := range present {
		f.add(gpixKindNode, "k", v, uint64(i))
	}
	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	k := sec.key(gpixKindNode, "k")
	cases := []struct {
		probe string
		at    uint64
		exact bool
	}{
		{"a", 0, false}, {"b", 0, true}, {"c", 1, false}, {"d", 1, true},
		{"e", 2, false}, {"g", 3, false}, {"h", 3, true}, {"i", 4, false},
		{"", 0, false},
	}
	for _, c := range cases {
		at, exact, err := k.search([]byte(c.probe))
		if err != nil {
			t.Fatalf("search(%q): %v", c.probe, err)
		}
		if at != c.at || exact != c.exact {
			t.Errorf("search(%q) = %d,%v; want %d,%v", c.probe, at, exact, c.at, c.exact)
		}
	}
}

// TestGPIX_ForEachValueIsAscending covers the sequential walk a range scan and
// verification both use.
func TestGPIX_ForEachValueIsAscending(t *testing.T) {
	f := newGPIXFixture()
	for i := 0; i < 64; i++ {
		f.add(gpixKindNode, "k", fmt.Sprintf("v%03d", i), uint64(i))
	}
	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	k := sec.key(gpixKindNode, "k")

	var seen []string
	if err := k.forEachValue(0, func(value []byte, ids []byte) bool {
		seen = append(seen, string(value))
		if n := gpixIDCount(ids); n != 1 {
			t.Errorf("value %q has %d ids", value, n)
		}
		return true
	}); err != nil {
		t.Fatalf("forEachValue: %v", err)
	}
	if len(seen) != 64 || !sort.StringsAreSorted(seen) {
		t.Fatalf("walk produced %d values, sorted=%v", len(seen), sort.StringsAreSorted(seen))
	}

	// From the middle, and stopping early.
	var fromTen []string
	_ = k.forEachValue(10, func(value []byte, _ []byte) bool {
		fromTen = append(fromTen, string(value))
		return len(fromTen) < 3
	})
	if !equalStrings(fromTen, seen[10:13]) {
		t.Fatalf("forEachValue(10) = %v, want %v", fromTen, seen[10:13])
	}
}

// --- determinism ---

// TestGPIX_ByteDeterministic is the same contract TestCompact_IsByteDeterministic
// holds over the whole image: one set of entries has one encoding. Two dumps of
// an unchanged index that disagree in their bytes are two different images, and
// every digest, Merkle root and byte-comparison test downstream depends on that
// not happening.
func TestGPIX_ByteDeterministic(t *testing.T) {
	build := func() []byte {
		f := newGPIXFixture()
		// Insertion order deliberately varies nothing: the walker sorts, so the
		// encoder sees one order whatever the map iteration does.
		for i := 0; i < 200; i++ {
			f.add(gpixKindNode, fmt.Sprintf("key%d", i%7), fmt.Sprintf("value%d", i%31), uint64(i))
			f.add(gpixKindEdge, fmt.Sprintf("ekey%d", i%3), fmt.Sprintf("evalue%d", i%11), uint64(i))
		}
		return f.encode(t)
	}
	a, b := build(), build()
	if !bytes.Equal(a, b) {
		t.Fatalf("two encodings of the same entries differ: %d vs %d bytes", len(a), len(b))
	}
}

// A source is walked once per key now, so there is no second pass to disagree
// with the first. What is left to check is that a source or a destination which
// fails mid-section fails the section, and that where the intermediates are held
// makes no difference to the bytes.

// TestGPIX_RefusesAWriterThatRunsOut checks that a destination failing part way
// through a section is reported rather than producing a short one.
//
// The likeliest reason a compaction fails is a full disk, and the value table now
// travels through a second file, so there are two places for that to happen
// instead of one. A section that ends early is worse than a compaction that
// fails: the directory entry still claims the bytes, so a reader bounds a region
// that is not there.
func TestGPIX_RefusesAWriterThatRunsOut(t *testing.T) {
	f := newGPIXFixture()
	for i := 0; i < 64; i++ {
		f.add(gpixKindNode, "digest", fmt.Sprintf("v%04d", i), uint64(i))
	}
	src := f.source(t.TempDir(), 0, 0)
	rev := newGPIRSorter(src.ScratchDir, 0)
	defer rev.close()
	iw := newImageWriter(&shortWriter{limit: 24}, make([]byte, 0, 8))
	_, err := writeGPIX(iw, 0, src, rev, rev, nil)
	if err == nil {
		err = iw.flush()
	}
	if err == nil {
		t.Fatal("writeGPIX reported success over a writer that refused the bytes")
	}
}

// shortWriter accepts limit bytes and then fails, which is the shape of a full
// disk.
type shortWriter struct {
	limit int
	n     int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.limit {
		w.n = w.limit
		return 0, fmt.Errorf("no space left on device")
	}
	w.n += len(p)
	return len(p), nil
}

// TestGPIX_PropagatesWalkerError checks that a source failing mid-walk fails the
// section rather than writing a short one.
func TestGPIX_PropagatesWalkerError(t *testing.T) {
	boom := fmt.Errorf("index unavailable")
	_, err := encodeGPIXFrom(gpixSource{
		NodeKeys:   []string{"k"},
		NodeValues: func(string, func([]byte, []uint64) bool) error { return boom },
		ScratchDir: t.TempDir(),
	})
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("index unavailable")) {
		t.Fatalf("err = %v, want the walker's error", err)
	}
}

// TestMappedIndex_SpillingChangesNothingButWhereBytesWereHeld encodes one fixture
// with both intermediates in memory and again with both on disk, and asserts the
// two images are identical.
//
// This is the test that makes the spill safe to default to. The disk path is the
// one the store this program is aimed at takes and the memory path is the one
// every other test in this package takes, so without this the two halves of the
// encoder are each covered by a suite the other never runs. And "it also works"
// is not the claim: the claim is that it produces the same bytes, because the
// image's digest covers them and two compactions of one content must agree.
func TestMappedIndex_SpillingChangesNothingButWhereBytesWereHeld(t *testing.T) {
	f := newGPIXFixture()
	for i := 0; i < 200; i++ {
		f.add(gpixKindNode, "digest", fmt.Sprintf("v%06d", i), uint64(i+1))
		f.add(gpixKindNode, "bucket", fmt.Sprintf("b%02d", i%7), uint64(i+1))
		f.add(gpixKindEdge, "rel", fmt.Sprintf("r%03d", i%40), uint64(i+1))
	}
	held, revHeld, _, err := encodeMappedIndexFrom(f.source(t.TempDir(), 1<<20, 1<<20))
	if err != nil {
		t.Fatalf("in memory: %v", err)
	}
	// Sixteen bytes is one value-table entry and three entries is a fraction of
	// one key, so both intermediates spill many times over.
	spilled, revSpilled, _, err := encodeMappedIndexFrom(f.source(t.TempDir(), 16, 3))
	if err != nil {
		t.Fatalf("spilled: %v", err)
	}
	if !bytes.Equal(held, spilled) {
		t.Fatalf("GPIX differs: %d bytes held, %d spilled", len(held), len(spilled))
	}
	if !bytes.Equal(revHeld, revSpilled) {
		t.Fatalf("GPIR differs: %d bytes held, %d spilled", len(revHeld), len(revSpilled))
	}
}

// TestMappedIndex_ReverseMatchesTheObviousDerivation asserts that the external
// sort produces exactly the reverse section a reader would get by deriving it in
// memory from the forward one.
//
// The forward section is the authority: every valueOff in the derivation is read
// out of the value table the encoder wrote, so this compares the streaming sort
// against an independent statement of the same answer rather than against itself.
// Swept over both thresholds, because in-memory and merged are two different
// algorithms and only one of them is a sort of the whole set at once.
func TestMappedIndex_ReverseMatchesTheObviousDerivation(t *testing.T) {
	f := newGPIXFixture()
	for i := 1; i <= 300; i++ {
		id := uint64(i)
		f.add(gpixKindNode, "digest", fmt.Sprintf("d%08d", i), id)
		f.add(gpixKindNode, "bucket", fmt.Sprintf("b%d", i%5), id)
		if i%3 == 0 {
			f.add(gpixKindNode, "url", fmt.Sprintf("https://example.test/%d", i%11), id)
		}
		f.add(gpixKindEdge, "rel", fmt.Sprintf("r%d", i%7), id)
	}
	for _, tc := range []struct {
		name            string
		vtabCap, revLen int
	}{
		{"sorted in memory", 1 << 20, 1 << 20},
		{"merged from many runs", 16, 7},
		{"merged from two runs", 1 << 20, 900},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fwdBody, revBody, _, err := encodeMappedIndexFrom(f.source(t.TempDir(), tc.vtabCap, tc.revLen))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			sec, err := parseGPIX(fwdBody)
			if err != nil {
				t.Fatalf("parseGPIX: %v", err)
			}
			if want := buildGPIRFor(t, sec); !bytes.Equal(revBody, want) {
				t.Fatalf("GPIR is %d bytes, the derivation is %d; the sort disagrees with the section",
					len(revBody), len(want))
			}
			// Parsed as well as compared: equal bytes that neither reader accepts
			// would pass the comparison above and fail every query.
			rev, err := parseGPIR(revBody)
			if err != nil {
				t.Fatalf("parseGPIR: %v", err)
			}
			if _, err := newGPIXBase(sec, rev); err != nil {
				t.Fatalf("newGPIXBase: %v", err)
			}
		})
	}
}

// --- hostile and damaged input ---

// TestGPIX_RefusesTruncation truncates a valid section at every byte and asserts
// that parsing either succeeds or fails, but never panics and never hands back a
// reader that panics when used. This is the shape disk/fuzz_test.go's seeds take
// for the record stream, applied to the section that is about to become the
// index.
func TestGPIX_RefusesTruncation(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "sha256", "0123456789abcdef", 1, 2, 3)
	f.add(gpixKindNode, "sha256", "fedcba9876543210", 4)
	f.add(gpixKindNode, "bucket", "x", 5)
	f.add(gpixKindEdge, "kind", "calls", 6)
	body := f.encode(t)

	for n := 0; n < len(body); n++ {
		short := body[:n:n]
		sec, err := parseGPIX(short)
		if err != nil {
			continue
		}
		// Parsing a truncated section may legitimately succeed — a shorter
		// section is a valid shorter section if its directories happen to land
		// inside it — but every read it then offers must be bounded.
		drainGPIX(t, sec, fmt.Sprintf("truncated to %d", n))
	}
}

// TestGPIX_RefusesHostileDirectory rewrites each directory field to a value that
// would make a read leave the section, and asserts each is refused at parse time
// or bounded at read time.
func TestGPIX_RefusesHostileDirectory(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "sha256", "0123456789abcdef", 1, 2, 3)
	f.add(gpixKindNode, "bucket", "x", 5)
	body := f.encode(t)
	kdirOff := binary.LittleEndian.Uint64(body[len(body)-gpixFooterSize:])

	type patch struct {
		name string
		at   int
		val  uint64
		size int
	}
	patches := []patch{
		{"kdirOff past the end", len(body) - gpixFooterSize, uint64(len(body)) + 1, 8},
		{"kdirOff maximal", len(body) - gpixFooterSize, ^uint64(0), 8},
		{"kdirOff inside the header", len(body) - gpixFooterSize, 4, 8},
		{"node key count huge", 8, 1 << 30, 4},
		{"node key count maximal", 8, 0xffffffff, 4},
		{"edge key count huge", 12, 1 << 30, 4},
		{"keyOff maximal", int(kdirOff) + 0, ^uint64(0), 8},
		{"keyOff past the end", int(kdirOff) + 0, uint64(len(body)), 8},
		{"keyLen huge", int(kdirOff) + 8, 1 << 30, 4},
		{"kind invalid", int(kdirOff) + 12, 9, 1},
		{"keyID mismatched", int(kdirOff) + 14, 7, 2},
		{"distinct huge", int(kdirOff) + 16, 1 << 40, 8},
		{"distinct maximal", int(kdirOff) + 16, ^uint64(0), 8},
		{"entries maximal", int(kdirOff) + 24, ^uint64(0), 8},
		{"vtabOff maximal", int(kdirOff) + 32, ^uint64(0), 8},
		{"vtabOff past the end", int(kdirOff) + 32, uint64(len(body)) - 4, 8},
		{"runsOff maximal", int(kdirOff) + 40, ^uint64(0), 8},
		{"runsLen maximal", int(kdirOff) + 48, ^uint64(0), 8},
		{"runsLen past the end", int(kdirOff) + 48, uint64(len(body)), 8},
	}
	for _, p := range patches {
		t.Run(p.name, func(t *testing.T) {
			damaged := append([]byte(nil), body...)
			switch p.size {
			case 1:
				damaged[p.at] = byte(p.val)
			case 2:
				binary.LittleEndian.PutUint16(damaged[p.at:], uint16(p.val))
			case 4:
				binary.LittleEndian.PutUint32(damaged[p.at:], uint32(p.val))
			case 8:
				binary.LittleEndian.PutUint64(damaged[p.at:], p.val)
			}
			sec, err := parseGPIX(damaged)
			if err != nil {
				return // refused, which is the preferred outcome
			}
			drainGPIX(t, sec, p.name)
		})
	}
}

// TestGPIX_RefusesUnsortedDirectory covers the invariant the binary search rests
// on. An unsorted directory would make a present key unfindable, which is a
// wrong answer, so it is refused at parse.
func TestGPIX_RefusesUnsortedDirectory(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "aaa", "v", 1)
	f.add(gpixKindNode, "bbb", "v", 2)
	body := f.encode(t)
	kdirOff := binary.LittleEndian.Uint64(body[len(body)-gpixFooterSize:])

	// Swap the two entries' key pointers so the directory reads bbb before aaa.
	a := body[kdirOff : kdirOff+gpixKeyDirSize]
	b := body[kdirOff+gpixKeyDirSize : kdirOff+2*gpixKeyDirSize]
	aOff, aLen := binary.LittleEndian.Uint64(a[0:8]), binary.LittleEndian.Uint32(a[8:12])
	bOff, bLen := binary.LittleEndian.Uint64(b[0:8]), binary.LittleEndian.Uint32(b[8:12])
	binary.LittleEndian.PutUint64(a[0:8], bOff)
	binary.LittleEndian.PutUint32(a[8:12], bLen)
	binary.LittleEndian.PutUint64(b[0:8], aOff)
	binary.LittleEndian.PutUint32(b[8:12], aLen)

	if _, err := parseGPIX(body); err == nil {
		t.Fatal("parseGPIX accepted an unsorted key directory")
	}
}

func TestGPIX_RefusesBadHeader(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "k", "v", 1)
	good := f.encode(t)

	cases := map[string][]byte{
		"empty":         nil,
		"header only":   good[:gpixHeaderSize],
		"bad magic":     append([]byte("XXXX"), good[4:]...),
		"body version":  appendAt(good, 4, []byte{99, 0}),
		"no footer":     good[:len(good)-1],
		"all zero":      make([]byte, gpixHeaderSize+gpixFooterSize),
		"random header": bytes.Repeat([]byte{0xab}, gpixHeaderSize+gpixFooterSize),
	}
	for name, body := range cases {
		if _, err := parseGPIX(body); err == nil {
			t.Errorf("%s: parseGPIX accepted it", name)
		}
	}
}

// --- GPIR ---

func encodeGPIR(t testing.TB, nodes, edges []gpirEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	iw := newImageWriter(&buf, make([]byte, 0, 4096))
	writeGPIRHeader(iw, uint64(len(nodes)), uint64(len(edges)))
	for _, e := range nodes {
		writeGPIREntry(iw, e)
	}
	for _, e := range edges {
		writeGPIREntry(iw, e)
	}
	if err := iw.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return buf.Bytes()
}

func TestGPIR_RoundTrip(t *testing.T) {
	nodes := []gpirEntry{
		{ID: 1, KeyID: 0, ValLen: 4, ValueOff: 4},
		{ID: 1, KeyID: 1, ValLen: 1, ValueOff: 40},
		{ID: 3, KeyID: 0, ValLen: 4, ValueOff: 4},
		{ID: 9, KeyID: 2, ValLen: 2, ValueOff: 8},
	}
	edges := []gpirEntry{{ID: 7, KeyID: 3, ValLen: 5, ValueOff: 4}}

	sec, err := parseGPIR(encodeGPIR(t, nodes, edges))
	if err != nil {
		t.Fatalf("parseGPIR: %v", err)
	}
	if got := gpirCount(sec.entriesAt(gpixKindNode)); got != len(nodes) {
		t.Fatalf("node entries = %d, want %d", got, len(nodes))
	}
	if got := gpirCount(sec.entriesAt(gpixKindEdge)); got != len(edges) {
		t.Fatalf("edge entries = %d, want %d", got, len(edges))
	}
	for i, want := range nodes {
		if got := gpirEntryAt(sec.entriesAt(gpixKindNode), i); got != want {
			t.Errorf("node entry %d = %+v, want %+v", i, got, want)
		}
	}

	// The reverse lookup: every entry for one id, and nothing from its
	// neighbours.
	var got []gpirEntry
	sec.forEachEntryOf(gpixKindNode, 1, func(e gpirEntry) bool {
		got = append(got, e)
		return true
	})
	if len(got) != 2 || got[0].KeyID != 0 || got[1].KeyID != 1 {
		t.Fatalf("entries of id 1 = %+v", got)
	}
	// An id with no entries, one below the first and one above the last.
	for _, id := range []uint64{0, 2, 4, 100} {
		var n int
		sec.forEachEntryOf(gpixKindNode, id, func(gpirEntry) bool { n++; return true })
		if n != 0 {
			t.Errorf("id %d has %d entries, want 0", id, n)
		}
	}
	// Early exit.
	var seen int
	sec.forEachEntryOf(gpixKindNode, 1, func(gpirEntry) bool { seen++; return false })
	if seen != 1 {
		t.Errorf("early exit visited %d entries", seen)
	}

	// The distinct-id walk a bounded verification uses.
	var ids []uint64
	sec.forEachID(gpixKindNode, func(id uint64) bool { ids = append(ids, id); return true })
	if !equalIDs(ids, []uint64{1, 3, 9}) {
		t.Fatalf("forEachID = %v, want [1 3 9]", ids)
	}
}

func TestGPIR_RefusesBadInput(t *testing.T) {
	good := encodeGPIR(t, []gpirEntry{{ID: 1}}, nil)
	cases := map[string][]byte{
		"empty":       nil,
		"short":       good[:gpirHeaderSize-1],
		"bad magic":   append([]byte("XXXX"), good[4:]...),
		"version":     appendAt(good, 4, []byte{9, 0}),
		"node count":  appendAt(good, 8, u64le(1<<40)),
		"node max":    appendAt(good, 8, u64le(^uint64(0))),
		"edge count":  appendAt(good, 16, u64le(1<<40)),
		"edge max":    appendAt(good, 16, u64le(^uint64(0))),
		"both sum up": appendAt(appendAt(good, 8, u64le(1)), 16, u64le(1)),
	}
	for name, body := range cases {
		if _, err := parseGPIR(body); err == nil {
			t.Errorf("%s: parseGPIR accepted it", name)
		}
	}

	// Truncation at every length must not panic.
	for n := 0; n < len(good); n++ {
		sec, err := parseGPIR(good[:n:n])
		if err != nil {
			continue
		}
		sec.forEachID(gpixKindNode, func(uint64) bool { return true })
		sec.forEachEntryOf(gpixKindNode, 1, func(gpirEntry) bool { return true })
		sec.forEachID(gpixKindEdge, func(uint64) bool { return true })
	}
}

// TestGPIX_ValueOfResolvesReverseEntries checks the join between the two
// sections: a reverse entry names a key and an offset into that key's runs, and
// resolving it must give back the value the forward direction holds.
func TestGPIX_ValueOfResolvesReverseEntries(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "sha256", "0123456789abcdef", 1)
	f.add(gpixKindNode, "sha256", "fedcba9876543210", 2)
	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	k := sec.key(gpixKindNode, "sha256")

	// The offset a writer would record: where the value bytes sit inside the run.
	for i := uint64(0); i < k.Distinct; i++ {
		value, _, err := k.runAt(i)
		if err != nil {
			t.Fatalf("runAt(%d): %v", i, err)
		}
		start := binary.LittleEndian.Uint64(k.vtab[i*gpixVtabEntry+8:])
		e := gpirEntry{ID: i + 1, KeyID: k.KeyID, ValLen: uint32(len(value)), ValueOff: start + 4}
		got, err := sec.valueOf(e)
		if err != nil {
			t.Fatalf("valueOf(%+v): %v", e, err)
		}
		if !bytes.Equal(got, value) {
			t.Errorf("valueOf resolved %q, want %q", got, value)
		}
	}

	// Out of range in both directions must be an error, not a slice into
	// whatever follows.
	for _, bad := range []gpirEntry{
		{KeyID: 99},
		{KeyID: k.KeyID, ValLen: 8, ValueOff: uint64(len(k.runs))},
		{KeyID: k.KeyID, ValLen: 8, ValueOff: ^uint64(0)},
		{KeyID: k.KeyID, ValLen: ^uint32(0), ValueOff: 0},
	} {
		if _, err := sec.valueOf(bad); err == nil {
			t.Errorf("valueOf(%+v) succeeded", bad)
		}
	}
}

// --- differential, against the obvious structure ---

// TestGPIX_DifferentialAgainstMaps builds a random index, encodes it, and
// compares the section's answers against the maps for every value present and a
// stream of values that are not. It is the test that would catch an ordering or
// offset error that the hand-written cases happen to miss.
func TestGPIX_DifferentialAgainstMaps(t *testing.T) {
	rng := rand.New(rand.NewSource(20260912))
	f := newGPIXFixture()
	keys := []string{"sha256", "bucket", "name", "ts", "dup"}
	for _, key := range keys {
		n := 1 + rng.Intn(400)
		for i := 0; i < n; i++ {
			var value string
			switch key {
			case "sha256": // all distinct, 32 bytes
				b := make([]byte, 32)
				rng.Read(b)
				value = string(b)
			case "bucket": // low cardinality, so long posting lists
				value = fmt.Sprintf("b%d", rng.Intn(4))
			case "dup": // one value, every id
				value = "same"
			case "ts":
				value = fmt.Sprintf("%d", rng.Int63())
			default:
				value = fmt.Sprintf("n%d", rng.Intn(50))
			}
			f.add(gpixKindNode, key, value, uint64(rng.Int63n(1<<20)))
		}
	}
	// Dedup and sort the reference the way a real index holds it: ids ascending
	// with no repeats.
	for _, bucket := range f.nodes {
		for v, ids := range bucket {
			bucket[v] = sortedIDs(ids)
		}
	}

	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	for _, key := range keys {
		k := sec.key(gpixKindNode, key)
		if k == nil {
			t.Fatalf("key %q missing", key)
		}
		for value, want := range f.nodes[key] {
			raw, err := k.idsOf([]byte(value))
			if err != nil {
				t.Fatalf("idsOf(%q): %v", key, err)
			}
			if got := decodeIDs(raw); !equalIDs(got, want) {
				t.Fatalf("key %q value %q: %d ids, want %d", key, value, len(got), len(want))
			}
		}
		// Values that are not there.
		for i := 0; i < 500; i++ {
			b := make([]byte, 1+rng.Intn(40))
			rng.Read(b)
			if _, present := f.nodes[key][string(b)]; present {
				continue
			}
			if raw, err := k.idsOf(b); err != nil || raw != nil {
				t.Fatalf("key %q absent value resolved: %v, %v", key, raw, err)
			}
		}
		// The walk must see every distinct value exactly once, ascending.
		var walked []string
		if err := k.forEachValue(0, func(value []byte, _ []byte) bool {
			walked = append(walked, string(value))
			return true
		}); err != nil {
			t.Fatalf("forEachValue(%q): %v", key, err)
		}
		if len(walked) != len(f.nodes[key]) {
			t.Fatalf("key %q walk saw %d values, want %d", key, len(walked), len(f.nodes[key]))
		}
		if !sort.StringsAreSorted(walked) {
			t.Fatalf("key %q walk is not ascending", key)
		}
	}
}

// --- fuzz ---

// FuzzParseGPIX feeds arbitrary bytes to the section parser and then uses
// whatever it returns. A v9 image's index is untrusted input in exactly the way
// disk/csr_io.go's header comment describes, so the parser's contract is that no
// input produces a panic and no accepted input produces a read outside the body.
func FuzzParseGPIX(f *testing.F) {
	fix := newGPIXFixture()
	fix.add(gpixKindNode, "sha256", "0123456789abcdef", 1, 2)
	fix.add(gpixKindNode, "b", "x", 3)
	fix.add(gpixKindEdge, "e", "y", 4)
	f.Add(seedGPIX(fix))
	f.Add(newGPIXSeedEmpty())
	f.Add([]byte{})
	f.Add(make([]byte, gpixHeaderSize+gpixFooterSize))

	f.Fuzz(func(t *testing.T, body []byte) {
		sec, err := parseGPIX(body)
		if err != nil {
			return
		}
		drainGPIXFuzz(sec)
	})
}

// FuzzParseGPIR is FuzzParseGPIX for the reverse section.
func FuzzParseGPIR(f *testing.F) {
	var buf bytes.Buffer
	iw := newImageWriter(&buf, make([]byte, 0, 256))
	writeGPIRHeader(iw, 2, 1)
	writeGPIREntry(iw, gpirEntry{ID: 1, KeyID: 0, ValLen: 2, ValueOff: 4})
	writeGPIREntry(iw, gpirEntry{ID: 5, KeyID: 1, ValLen: 2, ValueOff: 8})
	writeGPIREntry(iw, gpirEntry{ID: 9, KeyID: 0, ValLen: 2, ValueOff: 4})
	_ = iw.flush()
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add(make([]byte, gpirHeaderSize))

	f.Fuzz(func(t *testing.T, body []byte) {
		sec, err := parseGPIR(body)
		if err != nil {
			return
		}
		for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
			sec.forEachID(kind, func(uint64) bool { return true })
			arr := sec.entriesAt(kind)
			for i := 0; i < gpirCount(arr); i++ {
				e := gpirEntryAt(arr, i)
				sec.forEachEntryOf(kind, e.ID, func(gpirEntry) bool { return true })
			}
			sec.forEachEntryOf(kind, ^uint64(0), func(gpirEntry) bool { return true })
		}
	})
}

// FuzzVerifyMappedIndex fuzzes the bounded check over *both* sections together,
// which is the only way the reverse half of it can be reached: a GPIR entry means
// nothing without the GPIX runs its offsets address, so a target taking one body
// can only ever exercise the forward pass.
//
// The pair is arbitrary rather than related, which is the point. A reverse section
// describing a forward section it has nothing to do with is exactly the input the
// cross-check has to survive without addressing memory outside either body, and it
// is not an input any writer produces.
func FuzzVerifyMappedIndex(f *testing.F) {
	fx := newGPIXFixture()
	fx.add(gpixKindNode, "k", "aa", 1, 2)
	fx.add(gpixKindNode, "k", "bb", 3)
	fx.add(gpixKindEdge, "r", "x", 4)
	fwd, rev, _, err := encodeMappedIndexFrom(fx.source("", 0, 0))
	if err != nil {
		f.Fatalf("encode: %v", err)
	}
	f.Add(fwd, rev)
	f.Add(newGPIXSeedEmpty(), rev)
	f.Add(fwd, []byte{})
	f.Add([]byte{}, []byte{})

	f.Fuzz(func(t *testing.T, gpix, gpir []byte) {
		fwdSec, err := parseGPIX(gpix)
		if err != nil {
			return
		}
		revSec, err := parseGPIR(gpir)
		if err != nil {
			return
		}
		b, err := newGPIXBase(fwdSec, revSec)
		if err != nil {
			return
		}
		cc := store.NewCancelCheck(context.Background())
		// An error is the expected outcome for almost every input. A panic is not,
		// and is what this target exists to find.
		_ = b.Verify(&cc)
	})
}

// --- helpers ---

// drainGPIX exercises every read a parsed section offers, so that a bound
// parseGPIX failed to establish shows up as a test failure rather than as a
// panic in production. Errors are expected and ignored; panics are not.
func drainGPIX(t *testing.T, sec *gpixSection, what string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s: reading a parsed section panicked: %v", what, r)
		}
	}()
	drainGPIXFuzz(sec)
}

func drainGPIXFuzz(sec *gpixSection) {
	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		for _, name := range sec.keyNames(kind) {
			k := sec.key(kind, name)
			if k == nil {
				continue
			}
			_, _ = k.idsOf([]byte("probe"))
			_, _ = k.idsOf(nil)
			_, _, _ = k.search(bytes.Repeat([]byte{0xff}, 40))
			_ = k.forEachValue(0, func(value []byte, ids []byte) bool {
				for i := 0; i < gpixIDCount(ids); i++ {
					_ = gpixIDAt(ids, i)
				}
				_ = len(value)
				return true
			})
			for i := uint64(0); i <= k.Distinct+1; i++ {
				_, _, _ = k.runAt(i)
				_, _ = k.runIndexOfValueAt(k.runOffsetAt(min(i, k.Distinct)) + 4)
			}
			_, _ = k.runIndexOfValueAt(0)
			_, _ = k.runIndexOfValueAt(^uint64(0))
			_, _ = sec.valueOf(gpirEntry{KeyID: k.KeyID, ValLen: 8, ValueOff: 0})
			// The verifier's forward pass is a reader of the same untrusted
			// section, so it belongs in the drain: a bound parseGPIX did not
			// establish must surface as a failure here rather than as a panic in
			// whatever runs `graphene verify`.
			cc := store.NewCancelCheck(context.Background())
			_ = verifyGPIXKey(k, &cc)
		}
	}
}

func seedGPIX(f *gpixFixture) []byte {
	body, _ := encodeGPIXFrom(f.source("", 0, 0))
	return body
}

func newGPIXSeedEmpty() []byte { return seedGPIX(newGPIXFixture()) }

func decodeIDs(raw []byte) []uint64 {
	out := make([]uint64, 0, gpixIDCount(raw))
	for i := 0; i < gpixIDCount(raw); i++ {
		out = append(out, gpixIDAt(raw, i))
	}
	return out
}

func sortedIDs(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	// Deduplicate: a real posting list holds an id once.
	var dedup []uint64
	for i, id := range out {
		if i == 0 || id != out[i-1] {
			dedup = append(dedup, id)
		}
	}
	return dedup
}

func equalIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func appendAt(src []byte, at int, patch []byte) []byte {
	out := append([]byte(nil), src...)
	copy(out[at:], patch)
	return out
}

func u64le(v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return b[:]
}

// --- the allocation the two-walk design exists to avoid ---

// flatGPIXSource is a pre-sorted key whose walker allocates nothing per call, so
// that what the guard below measures is the encoder and not the fixture.
type flatGPIXSource struct {
	values [][]byte
	ids    [][]uint64
}

func newFlatGPIXSource(distinct, idsPer int) *flatGPIXSource {
	s := &flatGPIXSource{}
	for i := 0; i < distinct; i++ {
		s.values = append(s.values, []byte(fmt.Sprintf("value%08d", i)))
		ids := make([]uint64, idsPer)
		for j := range ids {
			ids[j] = uint64(i*idsPer + j)
		}
		s.ids = append(s.ids, ids)
	}
	return s
}

func (s *flatGPIXSource) walk(_ string, fn func([]byte, []uint64) bool) error {
	for i := range s.values {
		if !fn(s.values[i], s.ids[i]) {
			return nil
		}
	}
	return nil
}

// TestGPIX_WriteAllocationIsFlatInEntries is the guard on the property the whole
// section rests on: writing the index must not hold the index.
//
// A slope and not a ceiling, which is the lesson TestAllocGuards_Paths records —
// a ceiling passes until the fixture grows and then says nothing about why. Ten
// times the entries through the same encoder must not cost ten times the memory:
// if it does, someone has reintroduced the intermediate the spill removed, and
// the compaction transient docs/MEMORY_MODEL.md §4 decomposes grows with the index
// again.
//
// Both thresholds are swept, because the encoder has two arrangements and the
// claim is about both: below the caps the intermediates are in memory and their
// allocation is the caps rather than the index, and above them they are files and
// the allocation is the caps plus the merge's read budget. Neither is a function
// of the entries, and neither grows when the entries do.
//
// It is also what caught the caps being grown into rather than allocated. Sixteen
// and twenty-four byte appends into a one-megabyte buffer cost about five
// megabytes on the way up, which made the encoder's allocation proportional to the
// index right up to the cap — bounded, and not what the cap is for.
func TestGPIX_WriteAllocationIsFlatInEntries(t *testing.T) {
	for _, tc := range []struct {
		name            string
		vtabCap, revLen int
	}{
		{"intermediates in memory", 0, 0},
		{"intermediates spilled", 512, 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			measure := func(distinct int) uint64 {
				src := newFlatGPIXSource(distinct, 4)
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				iw := newImageWriter(io.Discard, make([]byte, 0, 4096))
				rev := newGPIRSorter(dir, tc.revLen)
				defer rev.close()
				gsrc := gpixSource{NodeKeys: []string{"k"}, NodeValues: src.walk,
					ScratchDir: dir, vtabMemCap: tc.vtabCap, revChunk: tc.revLen}
				if _, err := writeGPIX(iw, 0, gsrc, rev, rev, nil); err != nil {
					t.Fatalf("writeGPIX: %v", err)
				}
				if err := iw.flush(); err != nil {
					t.Fatalf("flush: %v", err)
				}
				if err := rev.emit(func(gpirEntry) {}); err != nil {
					t.Fatalf("emit: %v", err)
				}
				runtime.ReadMemStats(&after)
				return after.TotalAlloc - before.TotalAlloc
			}

			small := measure(2_000)
			large := measure(20_000)
			if large > 2*small+64<<10 {
				t.Fatalf("writing 10x the entries allocated %d bytes against %d for the base "+
					"size: the encoder is holding the index, not streaming it", large, small)
			}
			t.Logf("gpix write allocation: 2k values %d B, 20k values %d B", small, large)
		})
	}
}

// BenchmarkGPIXWrite reports the per-entry cost of writing both sections, which is
// the figure the compaction arm of R3 is judged against.
//
// Both sections, and including the reverse index's sort and emit: the forward half
// alone is not what a compaction pays, and the sort is the part whose cost is not
// obvious. The thresholds are left at their defaults, so what this measures is the
// arrangement a real store gets — the intermediates held in memory until they
// exceed a cap and spilled after.
func BenchmarkGPIXWrite(b *testing.B) {
	for _, shape := range []struct {
		name             string
		distinct, idsPer int
	}{
		{"AllDistinct", 50_000, 1},
		{"LowCardinality", 50, 1_000},
	} {
		b.Run(shape.name, func(b *testing.B) {
			src := newFlatGPIXSource(shape.distinct, shape.idsPer)
			dir := b.TempDir()
			entries := shape.distinct * shape.idsPer
			gsrc := gpixSource{NodeKeys: []string{"k"}, NodeValues: src.walk, ScratchDir: dir}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				iw := newImageWriter(io.Discard, make([]byte, 0, 64<<10))
				rev := newGPIRSorter(dir, 0)
				if _, err := writeGPIX(iw, 0, gsrc, rev, nil, nil); err != nil {
					b.Fatal(err)
				}
				writeGPIRHeader(iw, rev.count(), 0)
				if err := rev.emit(func(e gpirEntry) { writeGPIREntry(iw, e) }); err != nil {
					b.Fatal(err)
				}
				if err := iw.flush(); err != nil {
					b.Fatal(err)
				}
				rev.close()
			}
			b.ReportMetric(float64(entries), "entries/op")
		})
	}
}

// TestGPIX_SkipsAValueNothingHolds asserts that a value with an empty id list is
// left out of the section entirely.
//
// A merged walk under a base never yields one — a value whose every id has been
// retracted is dropped there — but the writer must not lean on that. The section's
// distinct count and its runs would otherwise describe a value the index does not
// have, so a search could land on it and answer with no ids; and whether it is
// present would depend on which side of a compaction the retraction fell, which
// the image's digest covers.
func TestGPIX_SkipsAValueNothingHolds(t *testing.T) {
	src := gpixSource{
		NodeKeys: []string{"k"},
		NodeValues: func(_ string, fn func([]byte, []uint64) bool) error {
			for _, v := range []struct {
				value string
				ids   []uint64
			}{
				{"aaa", []uint64{1}},
				{"bbb", nil},
				{"ccc", []uint64{2, 3}},
			} {
				if !fn([]byte(v.value), v.ids) {
					return nil
				}
			}
			return nil
		},
		ScratchDir: t.TempDir(),
	}
	fwdBody, revBody, _, err := encodeMappedIndexFrom(src)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	sec, err := parseGPIX(fwdBody)
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	k := sec.key(gpixKindNode, "k")
	if k == nil {
		t.Fatal("the key is missing")
	}
	if k.Distinct != 2 || k.Entries != 3 {
		t.Fatalf("distinct = %d, entries = %d, want 2 and 3", k.Distinct, k.Entries)
	}
	ids, err := k.idsOf([]byte("bbb"))
	if err != nil {
		t.Fatalf("idsOf(bbb): %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("the value nothing holds is in the section, holding %v", decodeIDs(ids))
	}
	// The values that are held still are, and the value table's search still lands
	// on them: skipping one must not shift the offsets of the others.
	for _, tc := range []struct {
		value string
		want  []uint64
	}{{"aaa", []uint64{1}}, {"ccc", []uint64{2, 3}}} {
		got, err := k.idsOf([]byte(tc.value))
		if err != nil {
			t.Fatalf("idsOf(%s): %v", tc.value, err)
		}
		if !equalIDs(decodeIDs(got), tc.want) {
			t.Fatalf("idsOf(%s) = %v, want %v", tc.value, decodeIDs(got), tc.want)
		}
	}
	rev, err := parseGPIR(revBody)
	if err != nil {
		t.Fatalf("parseGPIR: %v", err)
	}
	if n := gpirCount(rev.entriesAt(gpixKindNode)); n != 3 {
		t.Fatalf("the reverse section holds %d entries, want 3", n)
	}
}
