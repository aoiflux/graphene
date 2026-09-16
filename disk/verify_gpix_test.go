package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// Tests for the bounded structural check over GPIX and GPIR.
//
// Every check in verify_gpix.go exists because some read path assumes the thing
// it checks and cannot afford to re-check it. So each one is tested the same way:
// a section the writer produced, damaged in exactly the way that check is for,
// and the assertion is that the damage is *named*. A verifier that returned nil
// for one of these would leave the engine answering queries out of a file it had
// declared sound, which is worse than having no verifier at all.
//
// The damage is applied to the encoded bodies rather than to a hand-written
// section, for the reason buildGPIRFor gives: what is being judged is a real
// file with one field changed, not a fabrication that may be wrong in ways the
// format could not produce.

// --- helpers ---

// orderFixture is a small, deterministic index with two node keys and one edge
// key. Values under "colour" are all three bytes wide so that one can be
// overwritten with another without moving anything.
func orderFixture() *gpixFixture {
	f := newGPIXFixture()
	f.add(gpixKindNode, "colour", "aaa", 1, 4, 9)
	f.add(gpixKindNode, "colour", "bbb", 2, 7)
	f.add(gpixKindNode, "colour", "ccc", 5)
	f.add(gpixKindNode, "size", "10", 1, 2)
	f.add(gpixKindNode, "size", "20", 5, 9)
	f.add(gpixKindEdge, "rel", "x", 3)
	f.add(gpixKindEdge, "rel", "y", 4, 6)
	return f
}

// mappedBodies encodes a fixture's two sections with the real writer.
func mappedBodies(t *testing.T, f *gpixFixture) (gpix, gpir []byte) {
	t.Helper()
	fwd, rev, _, err := encodeMappedIndexFrom(f.source(t.TempDir(), 0, 0))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return fwd, rev
}

// verifyBodies parses two bodies and runs the bounded check over them.
//
// A parse failure is returned rather than fatal: a mutation is meant to be
// something the parser accepts and the verifier rejects, so a parse error is a
// result the test should report, not an assertion the test should make.
func verifyBodies(gpix, gpir []byte) error {
	fwd, err := parseGPIX(gpix)
	if err != nil {
		return fmt.Errorf("parseGPIX: %w", err)
	}
	rev, err := parseGPIR(gpir)
	if err != nil {
		return fmt.Errorf("parseGPIR: %w", err)
	}
	b, err := newGPIXBase(fwd, rev, nil)
	if err != nil {
		return fmt.Errorf("newGPIXBase: %w", err)
	}
	cc := store.NewCancelCheck(context.Background())
	return b.Verify(&cc)
}

// gpixKdirEntry returns the i'th key directory entry of an encoded GPIX body, to
// be written through. Field offsets are gpixKeyDirSize's comment:
//
//	0 keyOff | 8 keyLen | 12 kind | 13 pad | 14 keyID |
//	16 distinct | 24 entries | 32 vtabOff | 40 runsOff | 48 runsLen
func gpixKdirEntry(body []byte, i int) []byte {
	kdirOff := binary.LittleEndian.Uint64(body[len(body)-gpixFooterSize:])
	at := kdirOff + uint64(i)*gpixKeyDirSize
	return body[at : at+gpixKeyDirSize : at+gpixKeyDirSize]
}

// gpixVtabOf returns key i's value table, including its sentinel.
func gpixVtabOf(body []byte, i int) []byte {
	ent := gpixKdirEntry(body, i)
	off := binary.LittleEndian.Uint64(ent[32:40])
	n := (binary.LittleEndian.Uint64(ent[16:24]) + 1) * gpixVtabEntry
	return body[off : off+n : off+n]
}

// gpixRunsOf returns key i's runs region.
func gpixRunsOf(body []byte, i int) []byte {
	ent := gpixKdirEntry(body, i)
	off := binary.LittleEndian.Uint64(ent[40:48])
	n := binary.LittleEndian.Uint64(ent[48:56])
	return body[off : off+n : off+n]
}

// gpixRunOf returns key i's j'th run, which is the extent between two adjacent
// value table offsets.
func gpixRunOf(body []byte, i, j int) []byte {
	vtab, runs := gpixVtabOf(body, i), gpixRunsOf(body, i)
	lo := binary.LittleEndian.Uint64(vtab[j*gpixVtabEntry+8:])
	hi := binary.LittleEndian.Uint64(vtab[(j+1)*gpixVtabEntry+8:])
	return runs[lo:hi:hi]
}

// setVtabPrefix writes a value table entry's prefix.
func setVtabPrefix(body []byte, i, j int, prefix uint64) {
	binary.BigEndian.PutUint64(gpixVtabOf(body, i)[j*gpixVtabEntry:], prefix)
}

// setVtabOffset writes a value table entry's run offset.
func setVtabOffset(body []byte, i, j int, off uint64) {
	binary.LittleEndian.PutUint64(gpixVtabOf(body, i)[j*gpixVtabEntry+8:], off)
}

// setRunValue overwrites a run's value bytes with an equally long replacement and
// repairs the value table prefix to match.
//
// Repairing the prefix is what makes this a test of the *ordering* check rather
// than of the prefix check: a value changed without it trips the prefix
// comparison first, which is a different guard for a different fault.
func setRunValue(t *testing.T, body []byte, i, j int, value string) {
	t.Helper()
	run := gpixRunOf(body, i, j)
	valLen := int(binary.LittleEndian.Uint32(run[0:4]))
	if valLen != len(value) {
		t.Fatalf("replacement %q is %d bytes against a %d-byte value", value, len(value), valLen)
	}
	copy(run[4:], value)
	setVtabPrefix(body, i, j, gpixPrefixOf([]byte(value)))
}

// gpirEntriesOf decodes a GPIR body back into the two slices it was written from,
// so a test can change the reverse direction structurally and re-encode it. The
// alternative is arithmetic on a packed array, which is how a test comes to
// assert something other than what it meant.
func gpirEntriesOf(t *testing.T, body []byte) (nodes, edges []gpirEntry) {
	t.Helper()
	rev, err := parseGPIR(body)
	if err != nil {
		t.Fatalf("parseGPIR: %v", err)
	}
	decode := func(arr []byte) []gpirEntry {
		out := make([]gpirEntry, gpirCount(arr))
		for i := range out {
			out[i] = gpirEntryAt(arr, i)
		}
		return out
	}
	return decode(rev.nodes), decode(rev.edges)
}

// --- the forward direction ---

func TestVerifyMappedIndex_AcceptsWhatTheWriterWrote(t *testing.T) {
	gpix, gpir := mappedBodies(t, orderFixture())
	if err := verifyBodies(gpix, gpir); err != nil {
		t.Fatalf("a section the writer produced did not verify: %v", err)
	}
	// And at a size where every key holds many values and many ids, from the
	// generator the rest of this package's GPIX tests use.
	f := newGPIXFixture()
	for _, tr := range generateTriples(0x5EED, 4000) {
		f.add(tr.kind, tr.key, tr.value, tr.id)
	}
	dedupeFixtureIDs(f)
	gpix, gpir = mappedBodies(t, f)
	if err := verifyBodies(gpix, gpir); err != nil {
		t.Fatalf("a generated section did not verify: %v", err)
	}
}

// dedupeFixtureIDs sorts and deduplicates every value's ids, which the encoder
// requires and the generator does not guarantee.
func dedupeFixtureIDs(f *gpixFixture) {
	for _, m := range []map[string]map[string][]uint64{f.nodes, f.edges} {
		for _, bucket := range m {
			for v, ids := range bucket {
				bucket[v] = sortedIDs(ids)
			}
		}
	}
}

// A key present in the directory with nothing behind it, which is reachable in a
// real store rather than a curiosity: a compaction skips a value whose every id
// has been retracted, so a key all of whose values went that way is written with
// a distinct count of zero. Its value table is the sentinel alone, and the two
// tiling checks then read that one entry twice and require it to be both the
// start and the end of a zero-byte runs region.
func TestVerifyMappedIndex_AcceptsAKeyWithNoValues(t *testing.T) {
	src := orderFixture().source(t.TempDir(), 0, 0)
	// Sorted ahead of "colour", because the encoder does not sort what it is
	// given and parseGPIX refuses a directory that is out of order.
	src.NodeKeys = append([]string{"absent"}, src.NodeKeys...)
	inner := src.NodeValues
	src.NodeValues = func(key string, fn func([]byte, []uint64) bool) error {
		if key == "absent" {
			return nil
		}
		return inner(key, fn)
	}
	fwd, rev, _, err := encodeMappedIndexFrom(src)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := verifyBodies(fwd, rev); err != nil {
		t.Fatalf("a key with no values did not verify: %v", err)
	}
	// And it is in the directory rather than silently dropped, so what verified
	// is the case this test is named for.
	sec, err := parseGPIX(fwd)
	if err != nil {
		t.Fatal(err)
	}
	k := sec.key(gpixKindNode, "absent")
	if k == nil {
		t.Fatal("the encoder dropped the empty key; this test asserted nothing")
	}
	if k.Distinct != 0 || k.Entries != 0 {
		t.Fatalf("the empty key holds %d values and %d entries", k.Distinct, k.Entries)
	}
}

func TestVerifyMappedIndex_NamesForwardDamage(t *testing.T) {
	// Each case damages a section the writer produced in one way, and names the
	// substring the verifier must report. The comment on each is the read that
	// would go wrong if nothing checked it.
	cases := []struct {
		name   string
		want   string
		mutate func(t *testing.T, gpix, gpir []byte)
	}{{
		// A runs region whose first run does not start at its beginning has
		// bytes nothing addresses, and the digest covers them.
		name: "the first run does not start at zero",
		want: "begins its runs at",
		mutate: func(t *testing.T, gpix, _ []byte) {
			setVtabOffset(gpix, 0, 0, 8)
		},
	}, {
		// The sentinel is what makes a run's extent the difference of two
		// entries. One short of the end leaves the last run short.
		name: "the sentinel is not the end of the runs",
		want: "its value table ends at",
		mutate: func(t *testing.T, gpix, _ []byte) {
			vtab := gpixVtabOf(gpix, 0)
			last := len(vtab)/gpixVtabEntry - 1
			cur := binary.LittleEndian.Uint64(vtab[last*gpixVtabEntry+8:])
			setVtabOffset(gpix, 0, last, cur-8)
		},
	}, {
		// A directory that claims one value fewer makes the last real entry the
		// sentinel, so the last value is unreachable and its ids are read as an
		// extent that ends early.
		name: "the distinct count is short",
		want: "its value table ends at",
		mutate: func(t *testing.T, gpix, _ []byte) {
			ent := gpixKdirEntry(gpix, 0)
			binary.LittleEndian.PutUint64(ent[16:24],
				binary.LittleEndian.Uint64(ent[16:24])-1)
		},
	}, {
		// Never compared by a read, which is exactly why nothing else would
		// notice: two compactions of the same content must produce the same
		// bytes.
		name: "the sentinel prefix is not the maximum",
		want: "sentinel prefix",
		mutate: func(t *testing.T, gpix, _ []byte) {
			vtab := gpixVtabOf(gpix, 0)
			setVtabPrefix(gpix, 0, len(vtab)/gpixVtabEntry-1, 0)
		},
	}, {
		// The binary search's first comparison. A prefix that disagrees with its
		// own value sends the search down the wrong half, and a present value is
		// reported absent.
		name: "a prefix disagrees with its value",
		want: "has prefix",
		mutate: func(t *testing.T, gpix, _ []byte) {
			vtab := gpixVtabOf(gpix, 0)
			binary.BigEndian.PutUint64(vtab[gpixVtabEntry:],
				binary.BigEndian.Uint64(vtab[gpixVtabEntry:])^1)
		},
	}, {
		// The ordering the whole value table is searched in, and §15.8's
		// ordering for the index as a whole.
		name: "values are not ascending",
		want: "values are not ascending",
		mutate: func(t *testing.T, gpix, _ []byte) {
			setRunValue(t, gpix, 0, 1, "aaa")
		},
	}, {
		// IDRun.Contains binary-searches a run and promises its caller the run
		// is ascending. Nothing on the read path checks it.
		name: "a run's ids are not ascending",
		want: "ids out of order",
		mutate: func(t *testing.T, gpix, _ []byte) {
			run := gpixRunOf(gpix, 0, 0) // colour=aaa, ids 1 4 9
			ids := run[len(run)-3*8:]
			binary.LittleEndian.PutUint64(ids[8:], 0)
		},
	}, {
		// What the planner weighs a key's selectivity by, and what the reverse
		// direction is counted against.
		name: "the entry count is wrong",
		want: "entries and holds",
		mutate: func(t *testing.T, gpix, _ []byte) {
			ent := gpixKdirEntry(gpix, 0)
			binary.LittleEndian.PutUint64(ent[24:32],
				binary.LittleEndian.Uint64(ent[24:32])+1)
		},
	}, {
		// A run that does not fill its extent leaves bytes between two values
		// that no read reaches.
		name: "a run does not fill its extent",
		want: "-byte run",
		mutate: func(t *testing.T, gpix, _ []byte) {
			run := gpixRunOf(gpix, 0, 0) // colour=aaa: 4 + 3 + 4 + 3*8 bytes
			valLen := binary.LittleEndian.Uint32(run[0:4])
			at := 4 + valLen
			binary.LittleEndian.PutUint32(run[at:], binary.LittleEndian.Uint32(run[at:])-1)
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gpix, gpir := mappedBodies(t, orderFixture())
			tc.mutate(t, gpix, gpir)
			err := verifyBodies(gpix, gpir)
			if err == nil {
				t.Fatal("the verifier accepted a damaged section")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("verify reported %q, which does not name %q", err, tc.want)
			}
		})
	}
}

// A value present in the table with no ids behind it is a value the index cannot
// produce: the writer skips one, and a merged walk that retracted every id of a
// value skips it too. It would also make the next compaction of the same content
// write different bytes.
//
// Reached by growing the value to swallow its own id count and id, which the
// arithmetic of a one-id run happens to allow exactly — see the assertion below,
// which is what stops this test from silently becoming a test of something else.
func TestVerifyMappedIndex_NamesAValueWithNoIDs(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "k", "v", 7)
	gpix, gpir := mappedBodies(t, f)

	run := gpixRunOf(gpix, 0, 0)
	if len(run) != 4+1+4+8 {
		t.Fatalf("a one-value one-id run is %d bytes, not the 17 this mutation assumes", len(run))
	}
	// valLen 1 -> 9 swallows the id count and the id. The four bytes now read as
	// the id count are the high half of id 7, which is zero — so the run declares
	// nine value bytes and no ids, and still fills its extent exactly.
	binary.LittleEndian.PutUint32(run[0:4], 9)
	setVtabPrefix(gpix, 0, 0, gpixPrefixOf(run[4:13]))

	err := verifyBodies(gpix, gpir)
	if err == nil {
		t.Fatal("the verifier accepted a value with no ids")
	}
	if !strings.Contains(err.Error(), "has no ids") {
		t.Fatalf("verify reported %q, which does not name an empty run", err)
	}
}

// --- the reverse direction ---

func TestVerifyMappedIndex_NamesReverseDamage(t *testing.T) {
	// These change the reverse direction structurally and re-encode it, so each
	// case is a GPIR a writer could have emitted rather than a packed array with
	// a byte moved.
	cases := []struct {
		name   string
		want   string
		mutate func(t *testing.T, nodes, edges []gpirEntry) ([]gpirEntry, []gpirEntry)
	}{{
		// The count check, and with it the completeness argument: the reverse
		// entries inject into the forward ones, so equal counts are what make
		// the two directions the same set. An entry short is an entry that
		// survives its entity when the entity is deleted.
		name: "an entry is missing",
		want: "reverse entries against",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			return append(n[:2:2], n[3:]...), e
		},
	}, {
		// forEachEntryOf walks the run of equal ids a search lands in and hands
		// each entry out as one entry, so a duplicate removes something twice.
		name: "an entry is duplicated",
		want: "not ascending",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n[:3]...)
			return append(out, n[2:]...), e
		},
	}, {
		// The array is binary-searched by id. A descending pair hides everything
		// after it from every search that lands past it.
		name: "two entries are out of order",
		want: "not ascending",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n...)
			out[1], out[2] = out[2], out[1]
			return out, e
		},
	}, {
		name: "an entry names a key that does not exist",
		want: "names key 99 of 3",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n...)
			out[0].KeyID = 99
			return out, e
		},
	}, {
		// The boundary, separately. The fixture declares three keys, so id 3 is
		// the first that does not exist — and a bound written as > rather than >=
		// accepts exactly that one and indexes past the directory. A case
		// comfortably past the end says nothing about it.
		name: "an entry names the key just past the last",
		want: "names key 3 of 3",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n...)
			out[0].KeyID = 3
			return out, e
		},
	}, {
		// Not a bound violation — the key exists and its runs are where they
		// should be — but it answers "what is this node indexed under" with an
		// edge key, and it counts against the wrong key.
		name: "a node entry names an edge key",
		want: "names edge key",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n...)
			out[0].KeyID = 2 // the edge key "rel"
			return out, e
		},
	}, {
		// valueOf bounds an entry inside the runs region, which is all a read
		// needs to be safe. This is what says it addresses a value rather than
		// the middle of one.
		name: "an entry points into the middle of a run",
		want: "not the start of a value",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n...)
			out[0].ValueOff++
			return out, e
		},
	}, {
		name: "an entry disagrees about the value length",
		want: "value bytes of key",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n...)
			out[0].ValLen++
			return out, e
		},
	}, {
		// The asymmetry the whole pass exists to find: the reverse direction
		// says this entity is indexed under a value whose postings do not list
		// it. Applied to the last entry so the array stays ascending.
		name: "an entry names an id its run does not hold",
		want: "and not that one",
		mutate: func(t *testing.T, n, e []gpirEntry) ([]gpirEntry, []gpirEntry) {
			out := append([]gpirEntry{}, n...)
			out[len(out)-1].ID = ^uint64(0)
			return out, e
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gpix, gpir := mappedBodies(t, orderFixture())
			nodes, edges := gpirEntriesOf(t, gpir)
			nodes, edges = tc.mutate(t, nodes, edges)
			err := verifyBodies(gpix, encodeGPIR(t, nodes, edges))
			if err == nil {
				t.Fatal("the verifier accepted a damaged reverse section")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("verify reported %q, which does not name %q", err, tc.want)
			}
		})
	}
}

// The two padding bytes are written as zero and read by nothing, which is what
// makes them worth checking: the digest covers them, so a file whose padding
// differs is a different file holding the same index.
func TestVerifyMappedIndex_NamesNonZeroReversePadding(t *testing.T) {
	gpix, gpir := mappedBodies(t, orderFixture())
	gpir[gpirHeaderSize+10] = 1

	err := verifyBodies(gpix, gpir)
	if err == nil {
		t.Fatal("the verifier accepted a reverse entry with padding set")
	}
	if !strings.Contains(err.Error(), "padding") {
		t.Fatalf("verify reported %q, which does not name the padding", err)
	}
}

// --- what it costs ---

// The requirement Base.Verify states, asserted rather than assumed: memory is
// one counter per declared key and one buffer bounded by the widest value, so a
// section holding ten times the entries costs the same to check.
//
// This is the property that makes the check usable on the store that needs it —
// one already near the ceiling this program is aimed at, where a verifier
// proportional to the index is a verifier that cannot be run.
func TestVerifyMappedIndex_MemoryDoesNotScaleWithEntries(t *testing.T) {
	measure := func(entries int) uint64 {
		f := newGPIXFixture()
		for i := 0; i < entries; i++ {
			// One key, one value per eight ids: distinct values and run lengths
			// both grow, and the widest value does not.
			f.add(gpixKindNode, "k", fmt.Sprintf("%08d", i/8), uint64(i)+1)
		}
		dedupeFixtureIDs(f)
		gpix, gpir := mappedBodies(t, f)
		fwd, err := parseGPIX(gpix)
		if err != nil {
			t.Fatal(err)
		}
		rev, err := parseGPIR(gpir)
		if err != nil {
			t.Fatal(err)
		}
		b, err := newGPIXBase(fwd, rev, nil)
		if err != nil {
			t.Fatal(err)
		}
		cc := store.NewCancelCheck(context.Background())
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		if err := b.Verify(&cc); err != nil {
			t.Fatalf("verify: %v", err)
		}
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}

	small := measure(2000)
	large := measure(20000)
	// Ten times the entries, and the allowance is generous on purpose: what is
	// being asserted is that the cost is flat, not what the constant is.
	if large > small+4096 {
		t.Fatalf("verifying 20,000 entries allocated %d bytes against %d for 2,000; "+
			"the check is not flat in the entries", large, small)
	}
}

func TestVerifyMappedIndex_StopsWhenCancelled(t *testing.T) {
	f := newGPIXFixture()
	for i := 0; i < 4000; i++ {
		f.add(gpixKindNode, "k", fmt.Sprintf("%08d", i), uint64(i)+1)
	}
	gpix, gpir := mappedBodies(t, f)
	fwd, err := parseGPIX(gpix)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := parseGPIR(gpir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newGPIXBase(fwd, rev, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cc := store.NewCancelCheck(ctx)
	if err := b.Verify(&cc); !isContextCancelled(err) {
		t.Fatalf("a cancelled verify returned %v, not the context's error", err)
	}
}

func isContextCancelled(err error) bool {
	return err != nil && strings.Contains(err.Error(), context.Canceled.Error())
}

// --- through the store ---

// The wiring, and the reason it is worth a test of its own: nothing else in the
// engine looks at a value table.
//
// A prefix changed to disagree with its value leaves the entries, the records and
// therefore every root exactly as they were, so the snapshot root matches, the
// index root matches, and — once the digest is recomputed, which is what anyone
// copying or repairing an image does — the digest matches too. The store opens,
// serves, and quietly reports a present value as absent. VerifyIndexes is the
// only thing that can say so.
func TestVerifyIndexes_NamesDamageInTheImagesIndex(t *testing.T) {
	dir := v9Store(t)

	// The control: the image the compaction wrote verifies.
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := s.StorageStats().IndexMode; got != IndexMapped.String() {
		s.Close()
		t.Skipf("this platform opened the index as %v, so there is no base to damage", got)
	}
	if err := s.VerifyIndexes(); err != nil {
		s.Close()
		t.Fatalf("a freshly compacted v9 store did not verify: %v", err)
	}
	s.Close()

	// Now damage one value table prefix in the image's GPIX section and repair
	// the digest over the result.
	path := filepath.Join(dir, csrFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := gpixBodyOf(t, data)
	vtab := gpixVtabOf(body, 0)
	binary.BigEndian.PutUint64(vtab[0:8], binary.BigEndian.Uint64(vtab[0:8])^1)
	repaired := computeCSRDigest(data)
	copy(data[csrDigestOffset:csrDigestOffset+csrDigestSize], repaired[:])
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	// Every other check passes, which is the point.
	if status, _, err := VerifyCSRDigest(dir); err != nil || status != DigestMatch {
		t.Fatalf("the digest should agree with the damaged bytes: status=%v err=%v", status, err)
	}
	if err := VerifyCSRRoots(dir); err != nil {
		t.Fatalf("the roots should still describe the damaged image: %v", err)
	}

	s, err = OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("the damaged image should still open: %v", err)
	}
	defer s.Close()
	err = s.VerifyIndexes()
	if err == nil {
		t.Fatal("VerifyIndexes accepted a store whose image holds a bad value table")
	}
	if !strings.Contains(err.Error(), "has prefix") {
		t.Fatalf("VerifyIndexes reported %q, which does not name the prefix", err)
	}
}

// gpixBodyOf returns the GPIX section body inside a whole image, as a slice into
// it, so a test can write through to the file's bytes.
func gpixBodyOf(t *testing.T, data []byte) []byte {
	t.Helper()
	sections, err := readCSRSectionDirectory(data, binary.LittleEndian.Uint64(data[62:70]))
	if err != nil {
		t.Fatalf("read the section directory: %v", err)
	}
	sec, ok := findSection(sections, csrSectionMappedIndex)
	if !ok {
		t.Fatalf("the image carries no %s section", csrSectionMappedIndex)
	}
	end := sec.Offset + sec.Length
	return data[sec.Offset:end:end]
}

// A store that has written a second image over a first still verifies, which is
// the case a fixture compacted once cannot reach: the base the second compaction
// wrote was merged out of a base and a delta, with retractions applied.
func TestVerifyIndexes_HoldsOverARecompactedBase(t *testing.T) {
	dir := v9Store(t)

	s, err := OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got := s.StorageStats().IndexMode; got != IndexMapped.String() {
		s.Close()
		t.Skipf("this platform opened the index as %v", got)
	}
	// A delta over the base, a retraction out of it, and then a second image. The
	// new node's values are one the base already holds and one it does not, so the
	// merged walk the second compaction reads from has to produce both a value
	// whose run grows and a value that is new.
	id := addNodeD(t, s, store.NodeTypeTag)
	for _, kv := range [][2]string{{"bucket", "b0"}, {"bucket", "bZ"}, {"seq", "9999"}} {
		if err := s.IndexNodeProperty(id, kv[0], []byte(kv[1])); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	if err := s.DeleteNode(store.NodeID(2)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := s.VerifyIndexes(); err != nil {
		s.Close()
		t.Fatalf("a base with a delta over it did not verify: %v", err)
	}
	if err := s.Compact(); err != nil {
		s.Close()
		t.Fatalf("compact: %v", err)
	}
	if err := s.VerifyIndexes(); err != nil {
		s.Close()
		t.Fatalf("a recompacted base did not verify: %v", err)
	}
	s.Close()

	// And once more from a fresh handle, which is what actually maps the second
	// image: the compaction that wrote it published a graph it built in memory.
	s, err = OpenWithOptions(dir, Options{IndexMode: IndexMapped})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	if err := s.VerifyIndexes(); err != nil {
		t.Fatalf("the reopened second image did not verify: %v", err)
	}
}

// Under IndexResident there is no base, so the check has nothing to do and must
// say so by doing nothing rather than by failing.
func TestVerifyIndexes_HasNothingToCheckWithoutABase(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{IndexMode: IndexResident})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	id := addNodeD(t, s, store.NodeTypeTag)
	if err := s.IndexNodeProperty(id, "k", []byte("v")); err != nil {
		t.Fatalf("IndexNodeProperty: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.VerifyIndexes(); err != nil {
		t.Fatalf("a resident index did not verify: %v", err)
	}
	if b := s.propIdx.AttachedBase(); b != nil {
		t.Fatalf("IndexResident attached a base: %T", b)
	}
	// The index-level entry point agrees.
	if err := s.propIdx.VerifyBase(); err != nil {
		t.Fatalf("VerifyBase with no base returned %v", err)
	}
	var _ index.Base = (*gpixBase)(nil)
}
