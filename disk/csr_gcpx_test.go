package disk

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The GCPX format in isolation: what the encoder writes, what the parser
// refuses, and what the verifier catches that the parser deliberately does not.
//
// gcpx_base_test.go is the store-level half — that a compaction writes the
// section and a reopened store reads it in place. These are the bytes.

// gcpxTestSource builds a section from a literal map of tuples, so a test can
// state the content it wants rather than drive a store to produce it.
//
// The tuples arrive in whatever order the caller wrote them and are sorted here,
// because ascending order is the writer's *precondition* rather than something it
// establishes: writeGCPXKind checks the order and fails, which is a different
// thing from sorting. A test that wanted to break the order on purpose bypasses
// this helper.
func gcpxTestSource(t *testing.T, node map[string][]uint64) gcpxSource {
	t.Helper()
	keys := []string{"bucket", "shard"}
	ordered := make([]string, 0, len(node))
	for k := range node {
		ordered = append(ordered, k)
	}
	bytesLess(ordered)
	return gcpxSource{
		NodeComposites: [][]string{keys},
		Tuples: func(kind uint8, _ []string, fn func(tuple []byte, ids []uint64) bool) error {
			if kind != gcpxKindNode {
				return nil
			}
			for _, k := range ordered {
				if !fn([]byte(k), node[k]) {
					return nil
				}
			}
			return nil
		},
		ScratchDir: t.TempDir(),
	}
}

// bytesLess sorts encoded tuples the way the format requires.
func bytesLess(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && bytes.Compare([]byte(s[j]), []byte(s[j-1])) < 0; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// gcpxTuple encodes a tuple the way index.encodeTuple does.
//
// Duplicated here rather than exported from index, and that is deliberate: the
// encoding is index's and disk never constructs one in production code, so a
// test helper is the honest place for the one copy disk needs. If the two ever
// disagree, TestGCPX_ACompactionWritesTheSection fails, because the store-level
// tests drive the real encoder.
func gcpxTuple(values ...string) string {
	var b []byte
	var l [4]byte
	for _, v := range values {
		binary.LittleEndian.PutUint32(l[:], uint32(len(v)))
		b = append(b, l[:]...)
		b = append(b, v...)
	}
	return string(b)
}

func writeGCPXBody(t *testing.T, src gcpxSource) []byte {
	t.Helper()
	var buf bytes.Buffer
	iw := newImageWriter(&buf, make([]byte, 0, 4096))
	if _, err := writeGCPX(iw, 0, src); err != nil {
		t.Fatalf("writeGCPX: %v", err)
	}
	if err := iw.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return buf.Bytes()
}

// TestGCPX_RoundTrip is the encoder and the parser against each other.
func TestGCPX_RoundTrip(t *testing.T) {
	want := map[string][]uint64{
		gcpxTuple("b0", "s0"): {1, 5, 9},
		gcpxTuple("b0", "s1"): {2},
		gcpxTuple("b1", "s0"): {3, 4},
		// A value long enough that the 8-byte prefix cannot distinguish it from
		// its neighbour, which is the case the binary search falls through to a
		// full comparison for. The first four bytes of any encoded tuple are the
		// first member's length, so the prefix is weaker here than in GPIX and
		// this is the shape that exercises it.
		gcpxTuple("a-very-long-bucket-value-1", "s0"): {6},
		gcpxTuple("a-very-long-bucket-value-2", "s0"): {7},
	}
	body := writeGCPXBody(t, gcpxTestSource(t, want))

	sec, err := parseGCPX(body)
	if err != nil {
		t.Fatalf("parseGCPX: %v", err)
	}
	c := sec.find(gcpxKindNode, []string{"bucket", "shard"})
	if c == nil {
		t.Fatal("the section carries no composite for the declared keys")
	}
	if int(c.distinct) != len(want) {
		t.Errorf("the section holds %d tuples, want %d", c.distinct, len(want))
	}
	for tuple, ids := range want {
		got, err := c.lookup([]byte(tuple))
		if err != nil {
			t.Fatalf("lookup %x: %v", tuple, err)
		}
		if n := len(got) / 8; n != len(ids) {
			t.Errorf("tuple %x answers %d ids, want %d", tuple, n, len(ids))
			continue
		}
		for i, want := range ids {
			if got := binary.LittleEndian.Uint64(got[i*8:]); got != want {
				t.Errorf("tuple %x id %d = %d, want %d", tuple, i, got, want)
			}
		}
	}
	// A tuple the section never saw is absent rather than an error, which is
	// Lookup's contract for an absent key and is what lets the merge be written
	// once for both cases.
	if ids, err := c.lookup([]byte(gcpxTuple("b9", "s9"))); err != nil || ids != nil {
		t.Errorf("an absent tuple answered %v, %v; want nil, nil", ids, err)
	}
	if err := verifyGCPX(sec, noCancel()); err != nil {
		t.Errorf("the section this writer produced does not verify: %v", err)
	}
}

// TestGCPX_AnEmptyCompositeIsStillASection is the distinction the writer's own
// comment makes: "this composite matched nothing" is a fact a reader must be able
// to read rather than infer from an absence.
//
// Inferring it wrongly would mean filling from the entries — the slow correct
// answer rather than the fast one — so an image whose composites are all empty
// still carries their directory entries.
func TestGCPX_AnEmptyCompositeIsStillASection(t *testing.T) {
	body := writeGCPXBody(t, gcpxTestSource(t, nil))
	sec, err := parseGCPX(body)
	if err != nil {
		t.Fatalf("parseGCPX: %v", err)
	}
	c := sec.find(gcpxKindNode, []string{"bucket", "shard"})
	if c == nil {
		t.Fatal("an empty composite left no directory entry, so a reader cannot tell " +
			"'matched nothing' from 'not carried'")
	}
	if c.distinct != 0 || c.entries != 0 {
		t.Errorf("the empty composite holds %d tuples and %d entries", c.distinct, c.entries)
	}
	if ids, err := c.lookup([]byte(gcpxTuple("b0", "s0"))); err != nil || ids != nil {
		t.Errorf("a lookup in an empty composite answered %v, %v", ids, err)
	}
	if err := verifyGCPX(sec, noCancel()); err != nil {
		t.Errorf("an empty section does not verify: %v", err)
	}
}

// TestGCPX_TheWriterRefusesAnUnorderedSource is the format's precondition stated
// as a failure rather than assumed.
//
// Both orderings are what the reader depends on and does not re-check, so a
// source that breaks one must be refused where it costs a comparison per tuple,
// not discovered later by a search that answers "absent" for a present tuple.
func TestGCPX_TheWriterRefusesAnUnorderedSource(t *testing.T) {
	for _, tc := range []struct {
		name  string
		walk  func(fn func(tuple []byte, ids []uint64) bool)
		wants string
	}{
		{
			name: "descending tuples",
			walk: func(fn func([]byte, []uint64) bool) {
				fn([]byte(gcpxTuple("b1", "s0")), []uint64{1})
				fn([]byte(gcpxTuple("b0", "s0")), []uint64{2})
			},
			wants: "not ascending",
		},
		{
			name: "a repeated tuple",
			walk: func(fn func([]byte, []uint64) bool) {
				fn([]byte(gcpxTuple("b0", "s0")), []uint64{1})
				fn([]byte(gcpxTuple("b0", "s0")), []uint64{2})
			},
			wants: "not ascending",
		},
		{
			name: "descending ids",
			walk: func(fn func([]byte, []uint64) bool) {
				fn([]byte(gcpxTuple("b0", "s0")), []uint64{9, 1})
			},
			wants: "out of order",
		},
		{
			name: "a repeated id",
			walk: func(fn func([]byte, []uint64) bool) {
				fn([]byte(gcpxTuple("b0", "s0")), []uint64{4, 4})
			},
			wants: "out of order",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := gcpxSource{
				NodeComposites: [][]string{{"bucket", "shard"}},
				Tuples: func(kind uint8, _ []string, fn func(tuple []byte, ids []uint64) bool) error {
					if kind == gcpxKindNode {
						tc.walk(fn)
					}
					return nil
				},
				ScratchDir: t.TempDir(),
			}
			var buf bytes.Buffer
			iw := newImageWriter(&buf, make([]byte, 0, 4096))
			_, err := writeGCPX(iw, 0, src)
			if err == nil {
				t.Fatal("the writer accepted a source that breaks the order the reader assumes")
			}
			if !bytes.Contains([]byte(err.Error()), []byte(tc.wants)) {
				t.Errorf("err = %v, want it to name %q", err, tc.wants)
			}
		})
	}
}

// TestGCPX_ParseRefusesDamage covers what parseGCPX is for: bounding every extent
// the directory names, so that a lookup into a damaged section fails rather than
// addressing bytes outside the body.
//
// Every case here is a bound, not an ordering. The orderings are verifyGCPX's,
// and TestGCPX_VerifyCatchesWhatParseDoesNot is that half.
func TestGCPX_ParseRefusesDamage(t *testing.T) {
	good := writeGCPXBody(t, gcpxTestSource(t, map[string][]uint64{
		gcpxTuple("b0", "s0"): {1, 2},
		gcpxTuple("b1", "s0"): {3},
	}))

	for _, tc := range []struct {
		name   string
		break_ func(b []byte) []byte
	}{
		{"empty", func([]byte) []byte { return nil }},
		{"shorter than a header and footer", func(b []byte) []byte { return b[:gcpxHeaderSize] }},
		{"wrong magic", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			copy(c[0:4], "XXXX")
			return c
		}},
		{"truncated body", func(b []byte) []byte { return b[:len(b)/2] }},
		{"cdir offset past the body", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			binary.LittleEndian.PutUint64(c[len(c)-8:], uint64(len(c))+1)
			return c
		}},
		{"a composite count the body cannot hold", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			binary.LittleEndian.PutUint32(c[8:12], 1<<20)
			return c
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.break_(append([]byte(nil), good...))
			sec, err := parseGCPX(body)
			if err == nil {
				t.Fatalf("parseGCPX accepted %d damaged bytes and returned %d composites",
					len(body), len(sec.composites))
			}
		})
	}
}

// TestGCPX_VerifyCatchesWhatParseDoesNot is the other half of the split.
//
// Each case below parses cleanly — every offset is inside the body — and is
// nevertheless a section whose reads would be silently wrong. That is exactly the
// class index.Base.Verify exists for, and the reason it is a separate pass is
// that establishing any of these is O(entries).
func TestGCPX_VerifyCatchesWhatParseDoesNot(t *testing.T) {
	build := func(t *testing.T) ([]byte, *gcpxSection) {
		t.Helper()
		body := writeGCPXBody(t, gcpxTestSource(t, map[string][]uint64{
			gcpxTuple("b0", "s0"): {1, 2},
			gcpxTuple("b1", "s0"): {3},
			gcpxTuple("b2", "s0"): {4, 7},
		}))
		sec, err := parseGCPX(body)
		if err != nil {
			t.Fatalf("parseGCPX over undamaged bytes: %v", err)
		}
		if err := verifyGCPX(sec, noCancel()); err != nil {
			t.Fatalf("the undamaged section does not verify: %v", err)
		}
		return body, sec
	}

	for _, tc := range []struct {
		name   string
		break_ func(t *testing.T, c *gcpxComposite)
		wants  string
	}{
		{
			name: "a prefix that is not its tuple's",
			break_: func(_ *testing.T, c *gcpxComposite) {
				binary.BigEndian.PutUint64(c.ttab[0:8], 0)
			},
			wants: "prefix",
		},
		{
			name: "ids out of order inside a run",
			break_: func(t *testing.T, c *gcpxComposite) {
				// The first run's second id, made to sort below its first.
				tuple, ids, err := c.runAt(gcpxRunOffAt(c, 0))
				if err != nil || len(ids) < 16 {
					t.Skipf("the fixture's first run (%x) holds %d id bytes", tuple, len(ids))
				}
				binary.LittleEndian.PutUint64(ids[8:], 0)
			},
			wants: "out of order",
		},
		{
			name: "an entry count the runs do not hold",
			break_: func(_ *testing.T, c *gcpxComposite) {
				c.entries += 7
			},
			wants: "entries",
		},
		{
			name: "a sentinel prefix that is not all ones",
			break_: func(_ *testing.T, c *gcpxComposite) {
				binary.BigEndian.PutUint64(c.ttab[c.distinct*gcpxTtabEntry:], 1)
			},
			wants: "sentinel",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, sec := build(t)
			c := sec.find(gcpxKindNode, []string{"bucket", "shard"})
			if c == nil {
				t.Fatal("the fixture carries no composite")
			}
			tc.break_(t, c)
			err := verifyGCPX(sec, noCancel())
			if err == nil {
				t.Fatal("verifyGCPX accepted a section whose reads would be wrong")
			}
			if !bytes.Contains([]byte(err.Error()), []byte(tc.wants)) {
				t.Errorf("err = %v, want it to name %q", err, tc.wants)
			}
		})
	}
}

// TestGCPX_KeysRoundTripThroughTheDirectory is the identity the whole design
// rests on: a composite is found by its key tuple and never by its ordinal.
//
// A store can be reopened with its composites declared in a different order, or
// with one of them dropped, and an ordinal would then hand one composite's
// postings to another. So the directory carries the keys, and find compares them.
func TestGCPX_KeysRoundTripThroughTheDirectory(t *testing.T) {
	src := gcpxSource{
		NodeComposites: [][]string{{"bucket", "shard"}, {"a", "b", "c"}},
		EdgeComposites: [][]string{{"rel", "run"}},
		Tuples:         func(uint8, []string, func([]byte, []uint64) bool) error { return nil },
		ScratchDir:     t.TempDir(),
	}
	sec, err := parseGCPX(writeGCPXBody(t, src))
	if err != nil {
		t.Fatalf("parseGCPX: %v", err)
	}
	for _, tc := range []struct {
		kind uint8
		keys []string
		want bool
	}{
		{gcpxKindNode, []string{"bucket", "shard"}, true},
		{gcpxKindNode, []string{"a", "b", "c"}, true},
		{gcpxKindEdge, []string{"rel", "run"}, true},
		// The same keys in the other order are a different composite.
		{gcpxKindNode, []string{"shard", "bucket"}, false},
		// The same keys under the other kind.
		{gcpxKindEdge, []string{"bucket", "shard"}, false},
		{gcpxKindNode, []string{"rel", "run"}, false},
		// A prefix of a declared tuple.
		{gcpxKindNode, []string{"a", "b"}, false},
	} {
		got := sec.find(tc.kind, tc.keys) != nil
		if got != tc.want {
			t.Errorf("find(kind=%d, %v) = %v, want %v", tc.kind, tc.keys, got, tc.want)
		}
	}
}

// FuzzParseGCPX asserts the one thing a parser over file bytes must guarantee:
// it never panics, and anything it accepts can be read without addressing memory
// outside the body.
//
// Reading is included on purpose. A parser that bounded its directory and then
// handed back a composite whose runs walk off the end would pass a parse-only
// fuzz and fail in production, so every accepted section is walked and probed
// here.
func FuzzParseGCPX(f *testing.F) {
	add := func(node map[string][]uint64) {
		var buf bytes.Buffer
		iw := newImageWriter(&buf, make([]byte, 0, 4096))
		src := gcpxSource{
			NodeComposites: [][]string{{"bucket", "shard"}},
			Tuples: func(kind uint8, _ []string, fn func([]byte, []uint64) bool) error {
				if kind != gcpxKindNode {
					return nil
				}
				keys := make([]string, 0, len(node))
				for k := range node {
					keys = append(keys, k)
				}
				bytesLess(keys)
				for _, k := range keys {
					if !fn([]byte(k), node[k]) {
						return nil
					}
				}
				return nil
			},
			ScratchDir: f.TempDir(),
		}
		if _, err := writeGCPX(iw, 0, src); err != nil {
			f.Fatalf("seed: %v", err)
		}
		if err := iw.flush(); err != nil {
			f.Fatalf("seed flush: %v", err)
		}
		f.Add(buf.Bytes())
	}
	add(nil)
	add(map[string][]uint64{gcpxTuple("b0", "s0"): {1, 2, 3}})
	add(map[string][]uint64{
		gcpxTuple("b0", "s0"): {1},
		gcpxTuple("b1", "s0"): {2, 3},
	})
	f.Add([]byte{})
	f.Add([]byte("GCPX"))
	f.Add(bytes.Repeat([]byte{0xff}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		sec, err := parseGCPX(data)
		if err != nil {
			return
		}
		for i := range sec.composites {
			c := &sec.composites[i]
			// Every extent the directory named must be inside the body.
			if uint64(len(c.ttab)) != (c.distinct+1)*gcpxTtabEntry {
				t.Fatalf("composite %d has %d ttab bytes for %d tuples",
					i, len(c.ttab), c.distinct)
			}
			// A walk of everything it claims to hold, which is where an
			// unbounded run would show up.
			var seen uint64
			_ = c.forEachRun(func(tuple, ids []byte) bool {
				seen += uint64(len(ids) / 8)
				return true
			})
			if seen > uint64(len(data)) {
				t.Fatalf("composite %d yielded %d ids from %d bytes", i, seen, len(data))
			}
			// And a probe, which is the binary search rather than the walk.
			if _, err := c.lookup([]byte(gcpxTuple("b0", "s0"))); err != nil {
				continue
			}
		}
		// The verifier must also be total: it may reject anything, but it may
		// not panic or run unbounded over bytes a parse accepted.
		_ = verifyGCPX(sec, noCancel())
	})
}

// noCancel is a CancelCheck that never cancels, by pointer because that is the
// shape the verifiers take.
func noCancel() *store.CancelCheck {
	cc := store.NewCancelCheck(context.Background())
	return &cc
}
