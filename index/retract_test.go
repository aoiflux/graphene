package index

// The differential suite for base ∪ delta − retracted.
//
// # The method, and why it is the only one worth running
//
// R3's whole premise is that an index split between an immutable disk-resident
// half and an in-memory delta answers *exactly* what one resident index answers.
// Not approximately, not for the queries someone thought of: exactly, because the
// split one is going to become the default and every existing test of the engine
// is a test of the resident one's answers. So nothing here asserts a value a test
// author chose. Every assertion compares two indexes loaded with the same triples
// — one whole, one split — and fails on any difference.
//
// # Why the base here is a fake, and why that is not a gap
//
// index cannot import disk, so the real base — disk's gpixBase over an image's
// GPIX and GPIR sections — cannot be used here. What is used instead is a fake
// built from maps and sorts, which is trivially correct and, more to the point,
// is *also* checked against a resident PropertyIndex by
// TestFakeBase_MatchesResident below. Composed with disk's own
// TestGPIXBase_LookupMatchesResident and its siblings, which check gpixBase
// against a resident PropertyIndex over the same shapes, that gives the chain
// this item needs:
//
//	gpixBase ≡ PropertyIndex   (disk/gpix_base_test.go)
//	fakeBase ≡ PropertyIndex   (here)
//	union(fakeBase, delta) ≡ PropertyIndex   (here, everything else)
//
// The integration — union over a real image — is R3d's, where a base is attached
// by a store for the first time and there is a v9 file to attach it from.

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// --- the fake base ---

// fakeBase is an in-memory index.Base: the same answers as a real one, from maps
// and sorts rather than from a file. Immutable once built, as the interface
// requires.
type fakeBase struct {
	sides [2]fakeSide

	// failKey, when set, makes every run-decoding method fail for that key. It is
	// how the damaged-base paths are reached without a damaged file.
	failKey string
	failErr error

	// walks counts calls to ForEachValue, so a test can assert what a path read
	// out of the base rather than only what it concluded. Atomic because one base
	// is read by sixty-four goroutines in the uniqueness test.
	walks     atomic.Int64
	seeks     atomic.Int64
	seekSteps atomic.Int64
}

// valueWalks is how many times ForEachValue has been entered.
func (b *fakeBase) valueWalks() int { return int(b.walks.Load()) }

// resetCounts zeroes the counters, for a test that reuses one base across arms.
func (b *fakeBase) resetCounts() {
	b.walks.Store(0)
	b.seeks.Store(0)
	b.seekSteps.Store(0)
}

type fakeSide struct {
	keys      []string                     // ascending
	values    map[string][]string          // key -> ascending distinct values
	ids       map[string]map[string][]byte // key -> value -> packed LE uint64s
	entriesOf map[uint64][]PropEntry       // id -> ascending by key then value
	allIDs    []uint64                     // ascending
	maxID     uint64
	entries   int
}

type triple struct {
	kind  EntityKind
	id    uint64
	key   string
	value []byte
}

func newFakeBase(triples []triple) *fakeBase {
	b := &fakeBase{}
	for i := range b.sides {
		b.sides[i] = fakeSide{
			values:    map[string][]string{},
			ids:       map[string]map[string][]byte{},
			entriesOf: map[uint64][]PropEntry{},
		}
	}
	// Deduplicated the way postings.add is idempotent, so a corpus that registers
	// a triple twice does not give the base two copies of it.
	seen := map[string]struct{}{}
	for _, t := range triples {
		fp := fmt.Sprintf("%d/%d/%s/%s", t.kind, t.id, t.key, t.value)
		if _, dup := seen[fp]; dup {
			continue
		}
		seen[fp] = struct{}{}
		s := &b.sides[t.kind]
		if s.ids[t.key] == nil {
			s.ids[t.key] = map[string][]byte{}
		}
		v := string(t.value)
		if _, known := s.ids[t.key][v]; !known {
			s.values[t.key] = append(s.values[t.key], v)
		}
		var idBytes [8]byte
		for i := 0; i < 8; i++ {
			idBytes[i] = byte(t.id >> (8 * i))
		}
		s.ids[t.key][v] = append(s.ids[t.key][v], idBytes[:]...)
		s.entriesOf[t.id] = append(s.entriesOf[t.id], PropEntry{Key: t.key, Value: bytes.Clone(t.value)})
		s.entries++
		if t.id > s.maxID {
			s.maxID = t.id
		}
	}
	for i := range b.sides {
		s := &b.sides[i]
		for k := range s.ids {
			s.keys = append(s.keys, k)
			slices.Sort(s.values[k])
			for v, packed := range s.ids[k] {
				s.ids[k][v] = sortPackedIDs(packed)
			}
		}
		slices.Sort(s.keys)
		for id := range s.entriesOf {
			sortPropEntries(s.entriesOf[id])
			s.allIDs = append(s.allIDs, id)
		}
		slices.Sort(s.allIDs)
	}
	return b
}

// sortPackedIDs sorts a packed little-endian id array ascending, deduplicated.
func sortPackedIDs(packed []byte) []byte {
	run := NewIDRun(packed)
	ids := make([]uint64, 0, run.Len())
	for i := 0; i < run.Len(); i++ {
		ids = append(ids, run.At(i))
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	out := make([]byte, 0, len(ids)*8)
	for _, id := range ids {
		for i := 0; i < 8; i++ {
			out = append(out, byte(id>>(8*i)))
		}
	}
	return out
}

// seeks and seekSteps are what the batch tests read: how many times a cursor was
// asked, and how many values it stepped over in total.
func (b *fakeBase) seekCounts() (seeks, steps int64) {
	return b.seeks.Load(), b.seekSteps.Load()
}

func (b *fakeBase) fail(key string) error {
	if b.failKey != "" && b.failKey == key {
		return b.failErr
	}
	return nil
}

func (b *fakeBase) Keys(kind EntityKind) []string { return b.sides[kind].keys }

func (b *fakeBase) KeyStats(kind EntityKind, key string) (distinct, entries int) {
	s := &b.sides[kind]
	for _, ids := range s.ids[key] {
		entries += NewIDRun(ids).Len()
	}
	return len(s.values[key]), entries
}

func (b *fakeBase) Lookup(kind EntityKind, key string, value []byte) (IDRun, error) {
	if err := b.fail(key); err != nil {
		return IDRun{}, err
	}
	bucket := b.sides[kind].ids[key]
	if bucket == nil {
		return IDRun{}, nil
	}
	return NewIDRun(bucket[string(value)]), nil
}

func (b *fakeBase) ForEachValue(kind EntityKind, key string, from []byte, fn func([]byte, IDRun) bool) error {
	b.walks.Add(1)
	if err := b.fail(key); err != nil {
		return err
	}
	s := &b.sides[kind]
	vals := s.values[key]
	start := 0
	if len(from) > 0 {
		start = sort.SearchStrings(vals, string(from))
	}
	for _, v := range vals[start:] {
		if !fn([]byte(v), NewIDRun(s.ids[key][v])) {
			return nil
		}
	}
	return nil
}

func (b *fakeBase) Cursor(kind EntityKind, key string) ValueCursor {
	return &fakeCursor{b: b, vals: b.sides[kind].values[key], ids: b.sides[kind].ids[key], key: key}
}

// fakeCursor walks a sorted []string forward, one value at a time.
//
// Linear on purpose: it counts every value it steps over in b.seekSteps, so a
// test can assert that a batch of N values over D distinct ones costs O(N+D)
// steps rather than O(N*D) — which is the whole claim the batch path makes, and
// which a cursor that bisected would hide behind its own cleverness.
type fakeCursor struct {
	b       *fakeBase
	key     string
	vals    []string
	ids     map[string][]byte
	at      int
	last    []byte
	hasLast bool
}

// Seek is deliberately allocation-free, which for a fake is not fussiness: the
// batch's own claim is that it allocates a fixed amount however many values it is
// given, and a fake that allocated per Seek would be the thing
// TestBatch_AllocatesNothingPerValue measured. last is a reused buffer for the
// same reason the real cursor keeps one, and the value comparisons go through
// unsafeBytes rather than converting the other way.
func (c *fakeCursor) Seek(want []byte) (IDRun, bool, error) {
	if c.hasLast && bytes.Compare(want, c.last) < 0 {
		return IDRun{}, false, fmt.Errorf("fake cursor: %x is below %x", want, c.last)
	}
	c.last, c.hasLast = append(c.last[:0], want...), true
	c.b.seeks.Add(1)
	if err := c.b.fail(c.key); err != nil {
		return IDRun{}, false, err
	}
	for c.at < len(c.vals) && bytes.Compare(unsafeBytes(c.vals[c.at]), want) < 0 {
		c.at++
		c.b.seekSteps.Add(1)
	}
	if c.at >= len(c.vals) || !bytes.Equal(unsafeBytes(c.vals[c.at]), want) {
		return IDRun{}, false, nil
	}
	return NewIDRun(c.ids[c.vals[c.at]]), true, nil
}

func (b *fakeBase) ForEachEntryOf(kind EntityKind, id uint64, fn func(string, []byte) bool) error {
	for _, e := range b.sides[kind].entriesOf[id] {
		if err := b.fail(e.Key); err != nil {
			return err
		}
		if !fn(e.Key, e.Value) {
			return nil
		}
	}
	return nil
}

func (b *fakeBase) HasEntries(kind EntityKind, id uint64) bool {
	return len(b.sides[kind].entriesOf[id]) > 0
}

func (b *fakeBase) ForEachID(kind EntityKind, fn func(uint64) bool) {
	for _, id := range b.sides[kind].allIDs {
		if !fn(id) {
			return
		}
	}
}

func (b *fakeBase) MaxID(kind EntityKind) uint64 { return b.sides[kind].maxID }

func (b *fakeBase) TotalEntries(kind EntityKind) int { return b.sides[kind].entries }

// Verify is the fake's own structural check, and it is a real one rather than a
// stub returning nil.
//
// The point of it is that the index package can then test VerifyBase — the
// fault-first ordering, the cancellation, the no-base case — without a file,
// and can watch it report a base it has deliberately damaged. A stub would make
// every one of those tests pass whatever VerifyBase did.
//
// It checks what a fake can: the two orderings the read paths assume, and that
// the reverse direction names exactly the entries the forward one holds.
func (b *fakeBase) Verify(cc *store.CancelCheck) error {
	for kind := range b.sides {
		s := &b.sides[kind]
		forward := 0
		for _, key := range s.keys {
			if err := cc.Step(); err != nil {
				return err
			}
			if err := b.fail(key); err != nil {
				return err
			}
			vals := s.values[key]
			for i, v := range vals {
				if i > 0 && v <= vals[i-1] {
					return fmt.Errorf("fake base: %v key %q values are not ascending: %q after %q",
						EntityKind(kind), key, v, vals[i-1])
				}
				run := NewIDRun(s.ids[key][v])
				for j := 1; j < run.Len(); j++ {
					if run.At(j) <= run.At(j-1) {
						return fmt.Errorf("fake base: %v key %q value %q ids are not ascending at %d",
							EntityKind(kind), key, v, j)
					}
				}
				forward += run.Len()
			}
		}
		reverse := 0
		for id, entries := range s.entriesOf {
			if err := cc.Step(); err != nil {
				return err
			}
			for _, e := range entries {
				if !NewIDRun(s.ids[e.Key][string(e.Value)]).Contains(id) {
					return fmt.Errorf("fake base: %v %d is listed under %q=%q, which does not hold it",
						EntityKind(kind), id, e.Key, e.Value)
				}
				reverse++
			}
		}
		if forward != reverse {
			return fmt.Errorf("fake base: %v holds %d entries forward and %d reverse",
				EntityKind(kind), forward, reverse)
		}
	}
	return nil
}

var _ Base = (*fakeBase)(nil)

// --- the corpus ---

const (
	keyDigest = "digest" // 32 bytes, all distinct: the brief's hash key
	keyBucket = "bucket" // eight values: the low-cardinality key
	keyURL    = "url"    // long shared prefixes
	keyKind   = "kind"   // one value for every node
	keyTS     = "ts"     // zero-padded decimal, declared ordered
	keyRel    = "rel"    // edges, four values
	keyWeight = "weight" // edges, zero-padded, declared ordered
)

// generateTriples builds the index shape the program's brief describes: one
// all-distinct digest key, a low-cardinality bucket, a key whose values share
// long prefixes, a key every entity carries one value of, and two ordered keys.
//
// The corpus is shuffled before it is returned, for two reasons. A split point
// taken at a fixed fraction of an unshuffled corpus puts one whole entity kind on
// one side of it — which is how the first run of this suite came to retract no
// edge at all and say nothing about it. And a shuffled split gives the base an
// arbitrary subset of the triples rather than a prefix of them, which is a
// stronger base than a real one: a real base holds every entry as of a
// compaction, and the merges must not come to depend on that.
func generateTriples(seed int64, nodes, edges int) []triple {
	rng := rand.New(rand.NewSource(seed))
	var out []triple
	for id := uint64(1); id <= uint64(nodes); id++ {
		d := sha256.Sum256([]byte(fmt.Sprintf("node-%d", id)))
		out = append(out,
			triple{NodeKind, id, keyDigest, d[:]},
			triple{NodeKind, id, keyBucket, []byte(fmt.Sprintf("b%d", id%8))},
			triple{NodeKind, id, keyKind, []byte("file")},
			triple{NodeKind, id, keyTS, []byte(fmt.Sprintf("%012d", 1_600_000_000+rng.Intn(400)))},
		)
		if rng.Intn(10) < 7 {
			out = append(out, triple{NodeKind, id, keyURL,
				[]byte("https://example.invalid/evidence/" + strings.Repeat("x", rng.Intn(4)) + fmt.Sprintf("%04d", rng.Intn(64)))})
		}
	}
	for id := uint64(1); id <= uint64(edges); id++ {
		out = append(out,
			triple{EdgeKind, id, keyRel, []byte(fmt.Sprintf("rel-%d", id%4))},
			triple{EdgeKind, id, keyWeight, []byte(fmt.Sprintf("%08d", rng.Intn(200)))},
		)
	}
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// orderedKeys are declared on both indexes of every pair, so that the ordered
// paths are exercised on a key the base holds and the delta's own ordered index
// does not cover.
var orderedNodeKeysUnderTest = []string{keyTS}
var orderedEdgeKeysUnderTest = []string{keyWeight}

// pair is one corpus loaded two ways: whole into one resident index, and split
// into a base plus a delta.
type pair struct {
	whole   *PropertyIndex
	split   *PropertyIndex
	base    *fakeBase
	triples []triple
}

// newPair splits the corpus at splitAt, with the last overlap triples before the
// split registered in the delta as well as in the base — so that the union has to
// deduplicate an id it meets on both sides rather than merely concatenate.
func newPair(t *testing.T, seed int64, nodes, edges, splitAt, overlap int) *pair {
	t.Helper()
	tr := generateTriples(seed, nodes, edges)
	if splitAt > len(tr) {
		splitAt = len(tr)
	}
	from := splitAt - overlap
	if from < 0 {
		from = 0
	}

	p := &pair{triples: tr, base: newFakeBase(tr[:splitAt])}
	p.whole, p.split = NewPropertyIndex(), NewPropertyIndex()
	for _, idx := range []*PropertyIndex{p.whole, p.split} {
		for _, k := range orderedNodeKeysUnderTest {
			idx.DeclareOrderedNodeKey(k)
		}
		for _, k := range orderedEdgeKeysUnderTest {
			idx.DeclareOrderedEdgeKey(k)
		}
	}
	if err := p.split.AttachBase(p.base); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	load(p.whole, tr)
	load(p.split, tr[from:])
	return p
}

func load(idx *PropertyIndex, triples []triple) {
	for _, t := range triples {
		if t.kind == EdgeKind {
			idx.IndexEdge(store.EdgeID(t.id), t.key, t.value)
			continue
		}
		idx.IndexNode(store.NodeID(t.id), t.key, t.value)
	}
}

// removeNodes drops the same ids from both indexes: from the whole one entirely,
// from the split one as a retraction plus a delta purge.
func (p *pair) removeNodes(ids []uint64) {
	for _, id := range ids {
		p.whole.RemoveNode(store.NodeID(id))
		p.split.RemoveNode(store.NodeID(id))
	}
}

func (p *pair) removeEdges(ids []uint64) {
	for _, id := range ids {
		p.whole.RemoveEdge(store.EdgeID(id))
		p.split.RemoveEdge(store.EdgeID(id))
	}
}

// corpusValues lists every (kind, key, value) the corpus mentions, plus a miss
// per key, so lookups are probed on absent values as densely as on present ones.
func (p *pair) corpusValues() []triple {
	seen := map[string]struct{}{}
	var out []triple
	for _, t := range p.triples {
		fp := fmt.Sprintf("%d/%s/%s", t.kind, t.key, t.value)
		if _, dup := seen[fp]; dup {
			continue
		}
		seen[fp] = struct{}{}
		out = append(out, triple{kind: t.kind, key: t.key, value: t.value})
		out = append(out, triple{kind: t.kind, key: t.key, value: append(bytes.Clone(t.value), 0)})
	}
	out = append(out,
		triple{kind: NodeKind, key: "never-declared", value: []byte("x")},
		triple{kind: EdgeKind, key: "never-declared", value: []byte("x")},
	)
	return out
}

// --- the assertions ---

func assertAgrees(t *testing.T, p *pair) {
	t.Helper()
	assertLookupsAgree(t, p)
	assertKeysAgree(t, p)
	assertValueWalksAgree(t, p)
	assertEntryWalksAgree(t, p)
	assertPerEntityAgrees(t, p)
	assertIndexedIDsAgree(t, p)
	assertOrderedRangesAgree(t, p)
	assertCountsAreUpperBounds(t, p)
}

func assertLookupsAgree(t *testing.T, p *pair) {
	t.Helper()
	for _, probe := range p.corpusValues() {
		if probe.kind == EdgeKind {
			want := p.whole.EdgesByProperty(probe.key, probe.value)
			got := p.split.EdgesByProperty(probe.key, probe.value)
			if !slices.Equal(want, got) {
				t.Fatalf("EdgesByProperty(%q, %q): resident %v, split %v", probe.key, probe.value, want, got)
			}
			continue
		}
		want := p.whole.NodesByProperty(probe.key, probe.value)
		got := p.split.NodesByProperty(probe.key, probe.value)
		if !slices.Equal(want, got) {
			t.Fatalf("NodesByProperty(%q, %q): resident %v, split %v", probe.key, probe.value, want, got)
		}
		if !slices.IsSorted(got) {
			t.Fatalf("NodesByProperty(%q, %q) is not ascending: %v", probe.key, probe.value, got)
		}
	}
}

// assertKeysAgree holds the key lists to what mergeKeys promises: a superset of
// the resident index's, differing only by keys the base carries whose every entry
// has been retracted — and those must contribute nothing to a walk, which is the
// half that makes the difference unobservable.
func assertKeysAgree(t *testing.T, p *pair) {
	t.Helper()
	assertKeySuperset(t, "node", p.whole.nodePropKeys(), p.split.nodePropKeys(),
		func(key string) int {
			n := 0
			p.split.ForEachNodeEntry(key, func(store.NodeID, []byte) bool { n++; return true })
			return n
		})
	assertKeySuperset(t, "edge", p.whole.edgePropKeys(), p.split.edgePropKeys(),
		func(key string) int {
			n := 0
			p.split.ForEachEdgeEntry(key, func(store.EdgeID, []byte) bool { n++; return true })
			return n
		})
	if want, got := p.whole.OrderedNodeKeys(), p.split.OrderedNodeKeys(); !slices.Equal(want, got) {
		t.Fatalf("ordered node keys: resident %v, split %v", want, got)
	}
}

func assertKeySuperset(t *testing.T, kind string, want, got []string, entries func(string) int) {
	t.Helper()
	if !slices.IsSorted(got) {
		t.Fatalf("%s keys are not sorted: %v", kind, got)
	}
	for _, k := range want {
		if !slices.Contains(got, k) {
			t.Fatalf("%s keys: resident has %q, split does not: %v", kind, k, got)
		}
	}
	for _, k := range got {
		if slices.Contains(want, k) {
			continue
		}
		if n := entries(k); n != 0 {
			t.Fatalf("%s key %q is absent from the resident index but yields %d entries", kind, k, n)
		}
	}
}

// assertValueWalksAgree compares ForEachNodeValue as a map, because the resident
// walk yields values in map order and the merged one yields them ascending. The
// order was never promised — forEachValue's own comment says it deliberately does
// not sort — so it is the content that has to agree.
func assertValueWalksAgree(t *testing.T, p *pair) {
	t.Helper()
	for _, key := range p.whole.nodePropKeys() {
		want := collectNodeValues(p.whole, key)
		got := collectNodeValues(p.split, key)
		if len(want) != len(got) {
			t.Fatalf("ForEachNodeValue(%q): resident has %d values, split %d", key, len(want), len(got))
		}
		for v, ids := range want {
			if !slices.Equal(ids, got[v]) {
				t.Fatalf("ForEachNodeValue(%q) value %q: resident %v, split %v", key, v, ids, got[v])
			}
		}
	}
	for _, key := range p.whole.edgePropKeys() {
		want := collectEdgeValues(p.whole, key)
		got := collectEdgeValues(p.split, key)
		if len(want) != len(got) {
			t.Fatalf("ForEachEdgeValue(%q): resident has %d values, split %d", key, len(want), len(got))
		}
		for v, ids := range want {
			if !slices.Equal(ids, got[v]) {
				t.Fatalf("ForEachEdgeValue(%q) value %q: resident %v, split %v", key, v, ids, got[v])
			}
		}
	}
	// Ascending is not promised, but the merged walk produces it and the ordered
	// paths lean on it, so it is pinned where it is produced.
	for _, key := range p.split.nodePropKeys() {
		var last []byte
		p.split.ForEachNodeValue(key, func(value []byte, _ []store.NodeID) bool {
			if last != nil && bytes.Compare(last, value) >= 0 {
				t.Fatalf("ForEachNodeValue(%q) under a base is not ascending: %q then %q", key, last, value)
			}
			last = bytes.Clone(value)
			return true
		})
	}
}

func collectNodeValues(p *PropertyIndex, key string) map[string][]store.NodeID {
	out := map[string][]store.NodeID{}
	p.ForEachNodeValue(key, func(value []byte, ids []store.NodeID) bool {
		out[string(value)] = slices.Clone(ids)
		return true
	})
	return out
}

func collectEdgeValues(p *PropertyIndex, key string) map[string][]store.EdgeID {
	out := map[string][]store.EdgeID{}
	p.ForEachEdgeValue(key, func(value []byte, ids []store.EdgeID) bool {
		out[string(value)] = slices.Clone(ids)
		return true
	})
	return out
}

// assertEntryWalksAgree compares the (value, id)-ordered walks exactly, because
// that order *is* contracted: NodeEntries' comment records that a compaction
// wrote these straight into the image and a different order made the file's
// digest useless as an identity for its contents.
func assertEntryWalksAgree(t *testing.T, p *pair) {
	t.Helper()
	for _, key := range p.whole.nodePropKeys() {
		want := collectNodeEntries(p.whole, key)
		got := collectNodeEntries(p.split, key)
		if !slices.Equal(want, got) {
			t.Fatalf("ForEachNodeEntry(%q): %d resident entries vs %d split, first difference %v",
				key, len(want), len(got), firstDifference(want, got))
		}
	}
	for _, key := range p.whole.edgePropKeys() {
		want := collectEdgeEntries(p.whole, key)
		got := collectEdgeEntries(p.split, key)
		if !slices.Equal(want, got) {
			t.Fatalf("ForEachEdgeEntry(%q): %d resident entries vs %d split, first difference %v",
				key, len(want), len(got), firstDifference(want, got))
		}
	}

	if want, got := propertyWalk(p.whole), propertyWalk(p.split); !slices.Equal(want, got) {
		t.Fatalf("ForEachNodeProperty: %d resident entries vs %d split, first difference %v",
			len(want), len(got), firstDifference(want, got))
	}
	if want, got := nodeEntriesAsStrings(p.whole), nodeEntriesAsStrings(p.split); !slices.Equal(want, got) {
		t.Fatalf("NodeEntries: %d resident vs %d split, first difference %v",
			len(want), len(got), firstDifference(want, got))
	}
	if want, got := edgeEntriesAsStrings(p.whole), edgeEntriesAsStrings(p.split); !slices.Equal(want, got) {
		t.Fatalf("EdgeEntries: %d resident vs %d split, first difference %v",
			len(want), len(got), firstDifference(want, got))
	}
}

func collectNodeEntries(p *PropertyIndex, key string) []string {
	var out []string
	p.ForEachNodeEntry(key, func(id store.NodeID, value []byte) bool {
		out = append(out, fmt.Sprintf("%q/%d", value, id))
		return true
	})
	return out
}

func collectEdgeEntries(p *PropertyIndex, key string) []string {
	var out []string
	p.ForEachEdgeEntry(key, func(id store.EdgeID, value []byte) bool {
		out = append(out, fmt.Sprintf("%q/%d", value, id))
		return true
	})
	return out
}

func propertyWalk(p *PropertyIndex) []string {
	var out []string
	p.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
		out = append(out, fmt.Sprintf("%s/%q/%d", key, value, id))
		return true
	})
	p.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
		out = append(out, fmt.Sprintf("%s/%q/%d", key, value, id))
		return true
	})
	return out
}

func nodeEntriesAsStrings(p *PropertyIndex) []string {
	var out []string
	for _, e := range p.NodeEntries() {
		out = append(out, fmt.Sprintf("%s/%q/%d", e.Key, e.Value, e.ID))
	}
	return out
}

func edgeEntriesAsStrings(p *PropertyIndex) []string {
	var out []string
	for _, e := range p.EdgeEntries() {
		out = append(out, fmt.Sprintf("%s/%q/%d", e.Key, e.Value, e.ID))
	}
	return out
}

func firstDifference(a, b []string) string {
	for i := range a {
		if i >= len(b) {
			return fmt.Sprintf("at %d resident %s, split ran out", i, a[i])
		}
		if a[i] != b[i] {
			return fmt.Sprintf("at %d resident %s, split %s", i, a[i], b[i])
		}
	}
	if len(b) > len(a) {
		return fmt.Sprintf("at %d resident ran out, split %s", len(a), b[len(a)])
	}
	return "none"
}

// assertPerEntityAgrees probes the reverse direction over a dense band of ids, so
// that entities that never existed, entities that were removed and entities that
// live only in the delta are all covered.
func assertPerEntityAgrees(t *testing.T, p *pair) {
	t.Helper()
	high := p.base.MaxID(NodeKind) + 8
	for id := uint64(0); id <= high; id++ {
		want := p.whole.NodeEntriesOf(store.NodeID(id))
		got := p.split.NodeEntriesOf(store.NodeID(id))
		if !slices.EqualFunc(want, got, samePropEntry) {
			t.Fatalf("NodeEntriesOf(%d): resident %v, split %v", id, want, got)
		}
		if want, got := p.whole.NodeHasEntries(store.NodeID(id)), p.split.NodeHasEntries(store.NodeID(id)); want != got {
			t.Fatalf("NodeHasEntries(%d): resident %v, split %v", id, want, got)
		}
	}
	highE := p.base.MaxID(EdgeKind) + 8
	for id := uint64(0); id <= highE; id++ {
		want := p.whole.EdgeEntriesOf(store.EdgeID(id))
		got := p.split.EdgeEntriesOf(store.EdgeID(id))
		if !slices.EqualFunc(want, got, samePropEntry) {
			t.Fatalf("EdgeEntriesOf(%d): resident %v, split %v", id, want, got)
		}
		if want, got := p.whole.EdgeHasEntries(store.EdgeID(id)), p.split.EdgeHasEntries(store.EdgeID(id)); want != got {
			t.Fatalf("EdgeHasEntries(%d): resident %v, split %v", id, want, got)
		}
	}
}

func samePropEntry(a, b PropEntry) bool {
	return a.Key == b.Key && bytes.Equal(a.Value, b.Value)
}

// assertIndexedIDsAgree compares the id enumerations as sets: both sides may
// repeat an id, the resident one across shards and the merged one across base and
// delta, and the contract says so.
func assertIndexedIDsAgree(t *testing.T, p *pair) {
	t.Helper()
	if want, got := indexedNodeIDs(p.whole), indexedNodeIDs(p.split); !slices.Equal(want, got) {
		t.Fatalf("ForEachIndexedNodeID: resident %d ids, split %d; first difference %v",
			len(want), len(got), firstDifferenceU64(want, got))
	}
	if want, got := indexedEdgeIDs(p.whole), indexedEdgeIDs(p.split); !slices.Equal(want, got) {
		t.Fatalf("ForEachIndexedEdgeID: resident %d ids, split %d; first difference %v",
			len(want), len(got), firstDifferenceU64(want, got))
	}
}

func indexedNodeIDs(p *PropertyIndex) []uint64 {
	var out []uint64
	p.ForEachIndexedNodeID(func(id store.NodeID) bool {
		out = append(out, uint64(id))
		return true
	})
	slices.Sort(out)
	return slices.Compact(out)
}

func indexedEdgeIDs(p *PropertyIndex) []uint64 {
	var out []uint64
	p.ForEachIndexedEdgeID(func(id store.EdgeID) bool {
		out = append(out, uint64(id))
		return true
	})
	slices.Sort(out)
	return slices.Compact(out)
}

func firstDifferenceU64(a, b []uint64) string {
	for i := range a {
		if i >= len(b) {
			return fmt.Sprintf("at %d resident %d, split ran out", i, a[i])
		}
		if a[i] != b[i] {
			return fmt.Sprintf("at %d resident %d, split %d", i, a[i], b[i])
		}
	}
	if len(b) > len(a) {
		return fmt.Sprintf("at %d resident ran out, split %d", len(a), b[len(a)])
	}
	return "none"
}

// rangeFilters is the battery of ordered predicates the merged range path is held
// to, with bounds on and off a present value and at both ends of the key.
func rangeFilters(key string, sample []string) []store.PropertyFilter {
	var out []store.PropertyFilter
	ops := []store.PropertyOp{
		store.PropertyOpGreaterThan,
		store.PropertyOpGreaterThanOrEqual,
		store.PropertyOpLessThan,
		store.PropertyOpLessThanOrEqual,
	}
	for _, v := range sample {
		for _, op := range ops {
			out = append(out, store.PropertyFilter{Key: key, Op: op, Value: []byte(v)})
		}
		out = append(out,
			store.PropertyFilter{Key: key, Op: store.PropertyOpPrefix, Value: []byte(v)},
			store.PropertyFilter{Key: key, Op: store.PropertyOpPrefix, Value: []byte(v[:len(v)-2])},
			store.PropertyFilter{Key: key, Op: store.PropertyOpBetweenInclusive, Value: []byte(v), ValueUpper: []byte(v)},
		)
	}
	if len(sample) > 1 {
		out = append(out,
			store.PropertyFilter{Key: key, Op: store.PropertyOpBetweenInclusive,
				Value: []byte(sample[0]), ValueUpper: []byte(sample[len(sample)-1])},
			store.PropertyFilter{Key: key, Op: store.PropertyOpBetweenInclusive,
				Value: []byte(sample[len(sample)-1]), ValueUpper: []byte(sample[0])},
		)
	}
	out = append(out,
		// A bound-less Between matches nothing; the empty prefix matches everything;
		// Equal and Contains are not an ordering's business.
		store.PropertyFilter{Key: key, Op: store.PropertyOpBetweenInclusive, Value: []byte("x")},
		store.PropertyFilter{Key: key, Op: store.PropertyOpPrefix},
		store.PropertyFilter{Key: key, Op: store.PropertyOpEqual, Value: []byte("x")},
		store.PropertyFilter{Key: key, Op: store.PropertyOpContains, Value: []byte("x")},
		store.PropertyFilter{Key: key, Op: store.PropertyOpGreaterThan, Value: []byte("")},
		store.PropertyFilter{Key: key, Op: store.PropertyOpLessThan, Value: []byte("\xff\xff")},
	)
	return out
}

func assertOrderedRangesAgree(t *testing.T, p *pair) {
	t.Helper()
	for _, key := range orderedNodeKeysUnderTest {
		sample := sampleValues(p.whole.nodePropValues(key), 6)
		for _, f := range rangeFilters(key, sample) {
			wantIDs, wantOK := p.whole.NodesMatchingOrdered(nil, f)
			gotIDs, gotOK := p.split.NodesMatchingOrdered(nil, f)
			if wantOK != gotOK {
				t.Fatalf("NodesMatchingOrdered(%v): resident served=%v, split served=%v", f, wantOK, gotOK)
			}
			if !wantOK {
				continue
			}
			if !slices.Equal(wantIDs, gotIDs) {
				t.Fatalf("NodesMatchingOrdered(op=%v value=%q upper=%q): resident %d ids, split %d",
					f.Op, f.Value, f.ValueUpper, len(wantIDs), len(gotIDs))
			}
			_, servedW := p.whole.NodeRangeCardinality(f)
			est, servedS := p.split.NodeRangeCardinality(f)
			if servedW != servedS {
				t.Fatalf("NodeRangeCardinality(%v): resident served=%v, split served=%v", f, servedW, servedS)
			}
			// A driver estimate may be high — mergeCardinality says why — but an
			// estimate of nothing for a filter that matches something would take
			// the range out of the comparison altogether.
			if servedS && len(gotIDs) > 0 && est == 0 {
				t.Fatalf("NodeRangeCardinality(op=%v value=%q) estimates 0 for %d matches",
					f.Op, f.Value, len(gotIDs))
			}
		}
	}
	for _, key := range orderedEdgeKeysUnderTest {
		sample := sampleValues(p.whole.edgePropValues(key), 6)
		for _, f := range rangeFilters(key, sample) {
			wantIDs, wantOK := p.whole.EdgesMatchingOrdered(nil, f)
			gotIDs, gotOK := p.split.EdgesMatchingOrdered(nil, f)
			if wantOK != gotOK {
				t.Fatalf("EdgesMatchingOrdered(%v): resident served=%v, split served=%v", f, wantOK, gotOK)
			}
			if wantOK && !slices.Equal(wantIDs, gotIDs) {
				t.Fatalf("EdgesMatchingOrdered(op=%v value=%q): resident %d ids, split %d",
					f.Op, f.Value, len(wantIDs), len(gotIDs))
			}
		}
	}
}

// nodePropValues lists a key's distinct values, ascending. Test-only, and it
// deliberately reads the whole index rather than the split one so that the sample
// covers values that live only in the base.
func (p *PropertyIndex) nodePropValues(key string) []string {
	var out []string
	p.ForEachNodeValue(key, func(value []byte, _ []store.NodeID) bool {
		out = append(out, string(value))
		return true
	})
	slices.Sort(out)
	return out
}

func (p *PropertyIndex) edgePropValues(key string) []string {
	var out []string
	p.ForEachEdgeValue(key, func(value []byte, _ []store.EdgeID) bool {
		out = append(out, string(value))
		return true
	})
	slices.Sort(out)
	return out
}

func sampleValues(vals []string, n int) []string {
	if len(vals) <= n {
		return vals
	}
	step := len(vals) / n
	var out []string
	for i := 0; i < len(vals) && len(out) < n; i += step {
		out = append(out, vals[i])
	}
	return out
}

// assertCountsAreUpperBounds holds the planner's inputs to the contract they
// actually carry: never below the truth, because a driver must be a superset.
func assertCountsAreUpperBounds(t *testing.T, p *pair) {
	t.Helper()
	for _, probe := range p.corpusValues() {
		if probe.kind == EdgeKind {
			exact := len(p.whole.EdgesByProperty(probe.key, probe.value))
			if got := p.split.EdgeCardinality(probe.key, probe.value); got < exact {
				t.Fatalf("EdgeCardinality(%q, %q) = %d, below the %d ids it returns",
					probe.key, probe.value, got, exact)
			}
			continue
		}
		exact := len(p.whole.NodesByProperty(probe.key, probe.value))
		if got := p.split.NodeCardinality(probe.key, probe.value); got < exact {
			t.Fatalf("NodeCardinality(%q, %q) = %d, below the %d ids it returns",
				probe.key, probe.value, got, exact)
		}
	}
	wantN, wantE := p.whole.EntryCounts()
	gotN, gotE := p.split.EntryCounts()
	if gotN < wantN || gotE < wantE {
		t.Fatalf("EntryCounts: resident (%d, %d) but split (%d, %d), which must not be lower",
			wantN, wantE, gotN, gotE)
	}
	for _, key := range p.whole.nodePropKeys() {
		exact := 0
		p.whole.ForEachNodeEntry(key, func(store.NodeID, []byte) bool { exact++; return true })
		if got := p.split.nodeKeyEntryCount(key); got < exact {
			t.Fatalf("nodeKeyEntryCount(%q) = %d, below the %d entries under it", key, got, exact)
		}
	}
}

// --- the tests ---

func TestFakeBase_MatchesResident(t *testing.T) {
	// Closes the chain the file comment sets out: the fake this suite trusts is
	// itself checked against the structure it stands in for.
	tr := generateTriples(1, 300, 120)
	resident := NewPropertyIndex()
	load(resident, tr)
	base := newFakeBase(tr)

	for _, kind := range []EntityKind{NodeKind, EdgeKind} {
		keys := base.Keys(kind)
		var want []string
		if kind == NodeKind {
			want = resident.nodePropKeys()
		} else {
			want = resident.edgePropKeys()
		}
		if !slices.Equal(want, keys) {
			t.Fatalf("%s keys: resident %v, fake %v", kind, want, keys)
		}
	}
	for _, tp := range tr {
		run, err := base.Lookup(tp.kind, tp.key, tp.value)
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		var want []uint64
		if tp.kind == NodeKind {
			for _, id := range resident.NodesByProperty(tp.key, tp.value) {
				want = append(want, uint64(id))
			}
		} else {
			for _, id := range resident.EdgesByProperty(tp.key, tp.value) {
				want = append(want, uint64(id))
			}
		}
		got := make([]uint64, 0, run.Len())
		for i := 0; i < run.Len(); i++ {
			got = append(got, run.At(i))
		}
		if !slices.Equal(want, got) {
			t.Fatalf("Lookup(%s, %q, %q): resident %v, fake %v", tp.kind, tp.key, tp.value, want, got)
		}
	}
	if want, got := len(resident.NodeEntries()), base.TotalEntries(NodeKind); want != got {
		t.Fatalf("TotalEntries(node): resident %d, fake %d", want, got)
	}
	for id := uint64(1); id <= 300; id++ {
		want := resident.NodeEntriesOf(store.NodeID(id))
		var got []PropEntry
		if err := base.ForEachEntryOf(NodeKind, id, func(k string, v []byte) bool {
			got = append(got, PropEntry{Key: k, Value: bytes.Clone(v)})
			return true
		}); err != nil {
			t.Fatalf("ForEachEntryOf: %v", err)
		}
		if !slices.EqualFunc(want, got, samePropEntry) {
			t.Fatalf("ForEachEntryOf(%d): resident %v, fake %v", id, want, got)
		}
	}
}

func TestUnion_MatchesResidentAtEverySplit(t *testing.T) {
	// The split point is where the two halves meet, so it is swept rather than
	// chosen: everything in the base, everything in the delta, and the boundary
	// falling inside a key's values, between two keys, and between the node and
	// edge halves of the corpus.
	total := len(generateTriples(7, 240, 90))
	for _, at := range []int{0, 1, 37, total / 4, total / 2, total - 1, total} {
		t.Run(fmt.Sprintf("split-%d", at), func(t *testing.T) {
			assertAgrees(t, newPair(t, 7, 240, 90, at, 0))
		})
	}
}

func TestUnion_MatchesResidentWithOverlap(t *testing.T) {
	// The overlap is the case a concatenation would get wrong: triples registered
	// in the delta that the base already holds, so every merge has to deduplicate.
	total := len(generateTriples(11, 240, 90))
	for _, overlap := range []int{1, 50, total / 2} {
		t.Run(fmt.Sprintf("overlap-%d", overlap), func(t *testing.T) {
			assertAgrees(t, newPair(t, 11, 240, 90, total/2, overlap))
		})
	}
}

func TestUnion_MatchesResidentAfterRemovals(t *testing.T) {
	for _, every := range []int{2, 3, 7} {
		t.Run(fmt.Sprintf("every-%d", every), func(t *testing.T) {
			p := newPair(t, 13, 240, 90, len(generateTriples(13, 240, 90))*2/3, 40)
			var nodes, edges []uint64
			for id := uint64(1); id <= 240; id++ {
				if id%uint64(every) == 0 {
					nodes = append(nodes, id)
				}
			}
			for id := uint64(1); id <= 90; id++ {
				if id%uint64(every) == 0 {
					edges = append(edges, id)
				}
			}
			p.removeNodes(nodes)
			p.removeEdges(edges)
			assertAgrees(t, p)

			n, e, residentBytes := p.split.RetractedCounts()
			if n == 0 || e == 0 {
				t.Fatalf("nothing was retracted: %d nodes, %d edges", n, e)
			}
			if residentBytes == 0 {
				t.Fatal("the retraction sets report no resident bytes after being written to")
			}
		})
	}
}

func TestUnion_MatchesResidentAfterRemoveAll(t *testing.T) {
	// The rebuild shape: every base entity removed, a new generation written into
	// the delta. This is the arrangement the whole item is for, and the one where a
	// base entry surviving its retraction would show as an id that has been deleted
	// still answering a query.
	tr := generateTriples(17, 200, 60)
	p := newPair(t, 17, 200, 60, len(tr), 0)
	all := make([]uint64, 0, 200)
	for id := uint64(1); id <= 200; id++ {
		all = append(all, id)
	}
	p.removeNodes(all)
	for id := uint64(201); id <= 320; id++ {
		d := sha256.Sum256([]byte(fmt.Sprintf("node-%d", id)))
		for _, idx := range []*PropertyIndex{p.whole, p.split} {
			idx.IndexNode(store.NodeID(id), keyDigest, d[:])
			idx.IndexNode(store.NodeID(id), keyBucket, []byte(fmt.Sprintf("b%d", id%8)))
			idx.IndexNode(store.NodeID(id), keyTS, []byte(fmt.Sprintf("%012d", 1_600_000_500+id)))
		}
	}
	p.triples = append(p.triples, triple{NodeKind, 320, keyDigest, nil})
	assertAgrees(t, p)

	for id := uint64(1); id <= 200; id++ {
		if p.split.NodeHasEntries(store.NodeID(id)) {
			t.Fatalf("node %d was removed but the base still answers for it", id)
		}
	}
	if got := len(p.split.NodesByProperty(keyKind, []byte("file"))); got != 0 {
		t.Fatalf("every holder of kind=file was removed, but %d came back", got)
	}
}

func TestUnion_NoResurrectionAcrossReindex(t *testing.T) {
	// docs/TECHNICAL_DETAILS.md §14.7's scenario, in the smallest form that shows
	// it: a value in the base, replaced through the purge-and-re-register path the
	// index actually offers, must not be answerable afterwards.
	base := newFakeBase([]triple{
		{NodeKind, 5, "k", []byte("old")},
		{NodeKind, 5, "other", []byte("keep")},
		{NodeKind, 6, "k", []byte("old")},
	})
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatal(err)
	}

	before := p.NodeEntriesOf(5)
	if len(before) != 2 {
		t.Fatalf("the base holds two entries for node 5, read back %v", before)
	}
	p.RemoveNode(5)
	for _, e := range before {
		p.IndexNode(5, e.Key, []byte("new"))
	}

	if ids := p.NodesByProperty("k", []byte("old")); !slices.Equal(ids, []store.NodeID{6}) {
		t.Fatalf("k=old should be held by node 6 alone, got %v", ids)
	}
	if ids := p.NodesByProperty("k", []byte("new")); !slices.Equal(ids, []store.NodeID{5}) {
		t.Fatalf("k=new should be held by node 5, got %v", ids)
	}
	if ids := p.NodesByProperty("other", []byte("keep")); len(ids) != 0 {
		t.Fatalf("other=keep was purged with the rest of node 5's entries, got %v", ids)
	}
	got := p.NodeEntriesOf(5)
	want := []PropEntry{{Key: "k", Value: []byte("new")}, {Key: "other", Value: []byte("new")}}
	if !slices.EqualFunc(want, got, samePropEntry) {
		t.Fatalf("NodeEntriesOf(5) after reindex: want %v, got %v", want, got)
	}
}

func TestUnion_EntriesOfDeduplicatesAcrossSides(t *testing.T) {
	// A caller re-registering what NodeEntriesOf handed it must not be handed the
	// same pair twice next time, or an idempotent re-ingest grows its own log.
	base := newFakeBase([]triple{{NodeKind, 9, "k", []byte("v")}})
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatal(err)
	}
	p.IndexNode(9, "k", []byte("v"))
	if got := p.NodeEntriesOf(9); len(got) != 1 {
		t.Fatalf("one (key, value) held by both sides should read back once, got %v", got)
	}
	if got := p.NodesByProperty("k", []byte("v")); !slices.Equal(got, []store.NodeID{9}) {
		t.Fatalf("one id held by both sides should read back once, got %v", got)
	}
}

// TestUnion_WalksStopWhenAsked is the case the merged walk got wrong first time:
// the base's ForEachValue reports a caller's stop and a completed walk the same
// way, so the delta's leftovers were drained into a callback that had already
// said it wanted nothing more. It is checked at every stop position, because a
// stop inside the base's half and a stop while draining the delta's leftovers are
// two different paths through the same function.
func TestUnion_WalksStopWhenAsked(t *testing.T) {
	p := newPair(t, 31, 60, 24, len(generateTriples(31, 60, 24))/2, 20)
	for _, key := range p.split.nodePropKeys() {
		total := 0
		p.split.ForEachNodeValue(key, func([]byte, []store.NodeID) bool { total++; return true })
		for stopAt := 1; stopAt <= total; stopAt++ {
			seen := 0
			p.split.ForEachNodeValue(key, func([]byte, []store.NodeID) bool {
				seen++
				return seen < stopAt
			})
			if seen != stopAt {
				t.Fatalf("ForEachNodeValue(%q) asked to stop after %d values of %d, saw %d",
					key, stopAt, total, seen)
			}
		}

		entries := 0
		p.split.ForEachNodeEntry(key, func(store.NodeID, []byte) bool { entries++; return true })
		for _, stopAt := range []int{1, entries / 2, entries} {
			if stopAt < 1 {
				continue
			}
			seen := 0
			p.split.ForEachNodeEntry(key, func(store.NodeID, []byte) bool {
				seen++
				return seen < stopAt
			})
			if seen != stopAt {
				t.Fatalf("ForEachNodeEntry(%q) asked to stop after %d entries of %d, saw %d",
					key, stopAt, entries, seen)
			}
		}
	}

	// The property walk stops across keys as well as within one, which is what
	// compaction's payload source and the export path both rely on.
	total := 0
	p.split.ForEachNodeProperty(func(store.NodeID, string, []byte) bool { total++; return true })
	for _, stopAt := range []int{1, total / 3, total / 2, total} {
		if stopAt < 1 {
			continue
		}
		seen := 0
		p.split.ForEachNodeProperty(func(store.NodeID, string, []byte) bool {
			seen++
			return seen < stopAt
		})
		if seen != stopAt {
			t.Fatalf("ForEachNodeProperty asked to stop after %d of %d entries, saw %d",
				stopAt, total, seen)
		}
	}

	edgeTotal := 0
	p.split.ForEachEdgeProperty(func(store.EdgeID, string, []byte) bool { edgeTotal++; return true })
	for _, stopAt := range []int{1, edgeTotal / 2, edgeTotal} {
		if stopAt < 1 {
			continue
		}
		seen := 0
		p.split.ForEachEdgeProperty(func(store.EdgeID, string, []byte) bool {
			seen++
			return seen < stopAt
		})
		if seen != stopAt {
			t.Fatalf("ForEachEdgeProperty asked to stop after %d of %d entries, saw %d",
				stopAt, edgeTotal, seen)
		}
	}
}

func TestRetractSet_LimitAndLazyAllocation(t *testing.T) {
	s := &retractSet{limit: 130}
	if s.bytes() != 0 {
		t.Fatalf("an untouched set holds %d bytes; a reader must allocate none", s.bytes())
	}
	if s.has(7) {
		t.Fatal("an untouched set reports a retraction")
	}
	s.add(7)
	s.add(130)
	// Above the base's highest id: ignored, because such an id was never in the
	// base and the delta is already the whole truth about it.
	s.add(131)
	s.add(1 << 40)
	if !s.has(7) || !s.has(130) {
		t.Fatal("a retracted id reads back as live")
	}
	if s.has(6) || s.has(8) || s.has(131) || s.has(1<<40) {
		t.Fatal("an id that was not retracted reads back as retracted")
	}
	if n := s.count(); n != 2 {
		t.Fatalf("count is %d after two retractions in range and two out of it", n)
	}
	if want := (130/64 + 1) * 8; s.bytes() != want {
		t.Fatalf("the set holds %d bytes, want %d for a limit of 130", s.bytes(), want)
	}
}

func TestRetractSet_ConcurrentAddAndHas(t *testing.T) {
	// The first add allocates. Racing several of them against readers is the whole
	// of what the allocation mutex is for.
	const n = 4096
	s := &retractSet{limit: n}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for id := uint64(w); id <= n; id += 8 {
				s.add(id)
			}
		}(w)
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := uint64(0); id <= n; id++ {
				_ = s.has(id)
			}
		}()
	}
	wg.Wait()
	for id := uint64(0); id <= n; id++ {
		if !s.has(id) {
			t.Fatalf("id %d was retracted by one of the writers but reads back live", id)
		}
	}
}

func TestAttachBase_OnceAndNotNil(t *testing.T) {
	p := NewPropertyIndex()
	if p.AttachedBase() != nil {
		t.Fatal("a fresh index reports a base")
	}
	if err := p.AttachBase(nil); err == nil {
		t.Fatal("attaching no base was accepted")
	}
	b := newFakeBase([]triple{{NodeKind, 1, "k", []byte("v")}})
	if err := p.AttachBase(b); err != nil {
		t.Fatal(err)
	}
	if p.AttachedBase() != Base(b) {
		t.Fatal("AttachedBase does not report the base that was attached")
	}
	if err := p.AttachBase(newFakeBase(nil)); err == nil {
		t.Fatal("a second base was attached over the first")
	}
	if p.AttachedBase() != Base(b) {
		t.Fatal("a refused attach replaced the base anyway")
	}
}

func TestUnique_AgreesWithResidentAcrossTheSplit(t *testing.T) {
	// digest is all-distinct, so it is declarable on both indexes; bucket is held
	// by many, so it is declarable on neither. Both answers have to come out the
	// same whichever side of the split the entries are on.
	tr := generateTriples(23, 120, 0)
	for _, at := range []int{0, len(tr) / 3, len(tr)} {
		t.Run(fmt.Sprintf("split-%d", at), func(t *testing.T) {
			p := newPair(t, 23, 120, 0, at, 0)
			for _, key := range []string{keyDigest, keyBucket, keyKind, "never-declared"} {
				wantC, wantErr := p.whole.DeclareUniqueNodeKey(key, nil)
				gotC, gotErr := p.split.DeclareUniqueNodeKey(key, nil)
				if (wantErr == nil) != (gotErr == nil) {
					t.Fatalf("DeclareUniqueNodeKey(%q): resident err %v, split err %v", key, wantErr, gotErr)
				}
				if len(wantC) != len(gotC) {
					t.Fatalf("DeclareUniqueNodeKey(%q): resident %d conflicts, split %d", key, len(wantC), len(gotC))
				}
				for i := range wantC {
					if !bytes.Equal(wantC[i].Value, gotC[i].Value) || !slices.Equal(wantC[i].IDs, gotC[i].IDs) {
						t.Fatalf("DeclareUniqueNodeKey(%q) conflict %d: resident %v, split %v",
							key, i, wantC[i], gotC[i])
					}
				}
				if want, got := p.whole.IsUniqueNodeKey(key), p.split.IsUniqueNodeKey(key); want != got {
					t.Fatalf("IsUniqueNodeKey(%q): resident %v, split %v", key, want, got)
				}
			}
			for _, probe := range p.corpusValues() {
				if probe.kind != NodeKind {
					continue
				}
				wantID, wantOK := p.whole.NodeUniqueOwner(probe.key, probe.value)
				gotID, gotOK := p.split.NodeUniqueOwner(probe.key, probe.value)
				if wantOK != gotOK || wantID != gotID {
					t.Fatalf("NodeUniqueOwner(%q, %q): resident (%d, %v), split (%d, %v)",
						probe.key, probe.value, wantID, wantOK, gotID, gotOK)
				}
			}
		})
	}
}

func TestUnique_GateRefusesAValueTheBaseHolds(t *testing.T) {
	base := newFakeBase([]triple{{NodeKind, 4, "k", []byte("taken")}})
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatal(err)
	}
	if conflicts, err := p.DeclareUniqueNodeKey("k", nil); err != nil || len(conflicts) > 0 {
		t.Fatalf("declaring k unique over one base holder: %v / %v", conflicts, err)
	}
	err := p.IndexNodeUnique(9, "k", []byte("taken"))
	var taken *ErrUniqueTaken
	if !errors.As(err, &taken) {
		t.Fatalf("registering a value the base holds returned %v, want ErrUniqueTaken", err)
	}
	if taken.Owner != 4 {
		t.Fatalf("the incumbent is reported as %d, want the base's holder 4", taken.Owner)
	}
	if !errors.Is(err, store.ErrUniqueViolation) {
		t.Fatalf("the refusal does not unwrap to ErrUniqueViolation: %v", err)
	}
	// The same entity re-registering its own value is not a conflict, base side
	// included — it is the steady state an idempotent re-ingest arrives at.
	if err := p.IndexNodeUnique(4, "k", []byte("taken")); err != nil {
		t.Fatalf("node 4 re-registering its own value: %v", err)
	}
	// And once the base's holder is retracted the value is free.
	p.RemoveNode(4)
	if err := p.IndexNodeUnique(9, "k", []byte("taken")); err != nil {
		t.Fatalf("the base's holder was removed, so the value is free: %v", err)
	}
}

func TestUnique_ConflictsSpanBaseAndDelta(t *testing.T) {
	// entries == distinct in the base, so the declaration takes the probe path;
	// the conflict is a delta entry meeting the base's single holder.
	base := newFakeBase([]triple{
		{NodeKind, 1, "k", []byte("a")},
		{NodeKind, 2, "k", []byte("b")},
	})
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatal(err)
	}
	p.IndexNode(3, "k", []byte("a"))
	conflicts, err := p.DeclareUniqueNodeKey("k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || !bytes.Equal(conflicts[0].Value, []byte("a")) ||
		!slices.Equal(conflicts[0].IDs, []uint64{1, 3}) {
		t.Fatalf("want one conflict on \"a\" held by 1 and 3, got %v", conflicts)
	}
	if p.IsUniqueNodeKey("k") {
		t.Fatal("a key with a conflict was declared anyway")
	}
	// Retracting the base's holder resolves it, and the walk path (entries >
	// distinct after a retraction is invisible to KeyStats) must agree.
	p.RemoveNode(1)
	if conflicts, err = p.DeclareUniqueNodeKey("k", nil); err != nil || len(conflicts) != 0 {
		t.Fatalf("after retracting the base's holder: %v / %v", conflicts, err)
	}
	if !p.IsUniqueNodeKey("k") {
		t.Fatal("the conflict was resolved but the key was not declared")
	}
}

func TestUnique_ConflictsWithinTheBaseTakeTheWalk(t *testing.T) {
	// entries > distinct, so mergeConflicts walks. The conflict is entirely inside
	// the base, which the probe path could not have found.
	base := newFakeBase([]triple{
		{NodeKind, 1, "k", []byte("a")},
		{NodeKind, 2, "k", []byte("a")},
		{NodeKind, 3, "k", []byte("c")},
	})
	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatal(err)
	}
	p.IndexNode(4, "k", []byte("b"))
	p.IndexNode(5, "k", []byte("b"))
	conflicts, err := p.DeclareUniqueNodeKey("k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 2 {
		t.Fatalf("want conflicts on \"a\" and \"b\", got %v", conflicts)
	}
	if !bytes.Equal(conflicts[0].Value, []byte("a")) || !slices.Equal(conflicts[0].IDs, []uint64{1, 2}) {
		t.Fatalf("first conflict: %v", conflicts[0])
	}
	if !bytes.Equal(conflicts[1].Value, []byte("b")) || !slices.Equal(conflicts[1].IDs, []uint64{4, 5}) {
		t.Fatalf("second conflict: %v", conflicts[1])
	}
	// Liveness filters the base side too: a posting that outlives its record is
	// not a holder, which is what makes the constraint declarable on a sound graph.
	conflicts, err = p.DeclareUniqueNodeKey("k", func(id store.NodeID) bool { return id != 2 && id != 5 })
	if err != nil || len(conflicts) != 0 {
		t.Fatalf("with the second holder of each value dead: %v / %v", conflicts, err)
	}
}

func TestBaseFault_IsRecordedAndRefusesTheGate(t *testing.T) {
	boom := errors.New("gpix: run will not decode")
	base := newFakeBase([]triple{
		{NodeKind, 1, "bad", []byte("v")},
		{NodeKind, 1, "good", []byte("v")},
	})
	base.failKey, base.failErr = "bad", boom

	p := NewPropertyIndex()
	if err := p.AttachBase(base); err != nil {
		t.Fatal(err)
	}
	p.IndexNode(2, "bad", []byte("v"))

	if err := p.Verify(); err != nil {
		t.Fatalf("nothing has read the damaged key yet, so Verify should pass: %v", err)
	}
	// The read answers from the delta, which is undamaged, rather than reporting
	// the value as unheld.
	if got := p.NodesByProperty("bad", []byte("v")); !slices.Equal(got, []store.NodeID{2}) {
		t.Fatalf("a damaged base should leave the delta's answer intact, got %v", got)
	}
	if err := p.BaseFault(); !errors.Is(err, boom) {
		t.Fatalf("the fault was not recorded: %v", err)
	}
	if err := p.Verify(); !errors.Is(err, boom) {
		t.Fatalf("Verify does not report the recorded fault: %v", err)
	}
	// The undamaged key still answers, so one bad run does not take the index out.
	if got := p.NodesByProperty("good", []byte("v")); !slices.Equal(got, []store.NodeID{1}) {
		t.Fatalf("an undamaged key should still answer, got %v", got)
	}

	// The uniqueness gate refuses rather than allowing a duplicate it cannot rule
	// out, and the declaration refuses rather than establishing what it cannot read.
	if _, err := p.DeclareUniqueNodeKey("bad", nil); !errors.Is(err, boom) {
		t.Fatalf("DeclareUniqueNodeKey over a damaged key returned %v", err)
	}
	p2 := NewPropertyIndex()
	if err := p2.AttachBase(base); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.DeclareUniqueNodeKey("good", nil); err != nil {
		t.Fatalf("declaring the undamaged key: %v", err)
	}
	p2.shardFor("bad").mu.Lock()
	p2.shardFor("bad").uniqueNodeKeys["bad"] = struct{}{}
	p2.shardFor("bad").mu.Unlock()
	if err := p2.IndexNodeUnique(3, "bad", []byte("v")); !errors.Is(err, boom) {
		t.Fatalf("the gate passed a registration it could not check: %v", err)
	}
}

func TestUnion_ConcurrentReadsAndRemovals(t *testing.T) {
	tr := generateTriples(29, 400, 0)
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(tr)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for id := uint64(1 + w); id <= 400; id += 4 {
				p.RemoveNode(store.NodeID(id))
				p.IndexNode(store.NodeID(id), keyBucket, []byte("rewritten"))
			}
		}(w)
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = p.NodesByProperty(keyBucket, []byte("b1"))
				_ = p.NodeCardinality(keyKind, []byte("file"))
				p.ForEachNodeValue(keyBucket, func([]byte, []store.NodeID) bool { return true })
				_ = p.NodeEntriesOf(store.NodeID(1 + i%400))
				_ = p.NodeHasEntries(store.NodeID(1 + i%400))
			}
		}()
	}
	wg.Wait()
	if err := p.Verify(); err != nil {
		t.Fatalf("the delta is inconsistent after the run: %v", err)
	}
	if got := len(p.NodesByProperty(keyBucket, []byte("rewritten"))); got != 400 {
		t.Fatalf("every node was rewritten under one value, %d came back", got)
	}
	if got := p.NodesByProperty(keyKind, []byte("file")); len(got) != 0 {
		t.Fatalf("every node was purged from kind=file, %d came back", len(got))
	}
}

// TestUnion_MergedWalkAllocatesPerValueNotPerEntry pins the scratch buffer's
// reuse. A merged walk cannot be allocation-free — a value both sides hold has no
// single array to hand out — but it must not allocate per entry, or a filter scan
// over a key the base holds allocates the whole key.
func TestUnion_MergedWalkAllocatesPerValueNotPerEntry(t *testing.T) {
	measure := func(perValue int) float64 {
		const values = 64
		var tr []triple
		id := uint64(1)
		for v := 0; v < values; v++ {
			for i := 0; i < perValue; i++ {
				tr = append(tr, triple{NodeKind, id, "k", []byte(fmt.Sprintf("%04d", v))})
				id++
			}
		}
		p := NewPropertyIndex()
		if err := p.AttachBase(newFakeBase(tr)); err != nil {
			t.Fatal(err)
		}
		return testing.AllocsPerRun(50, func() {
			p.ForEachNodeValue("k", func([]byte, []store.NodeID) bool { return true })
		})
	}
	small, large := measure(4), measure(40)
	// Ten times the entries under the same number of values. The buffer grows to
	// the widest value once, so the count may move by the odd re-grow and must not
	// scale with the entries.
	if large > small*2 {
		t.Fatalf("64 values of 40 ids allocated %.1f objects against %.1f for 4 ids: "+
			"the merged walk is allocating per entry, not per value", large, small)
	}
}

func BenchmarkUnionLookup(b *testing.B) {
	for _, shape := range []struct {
		name   string
		values int
		perVal int
	}{
		{"AllDistinct", 50_000, 1},
		{"LowCardinality", 50, 1_000},
	} {
		b.Run(shape.name, func(b *testing.B) {
			var tr []triple
			id := uint64(1)
			probes := make([][]byte, 0, shape.values)
			for v := 0; v < shape.values; v++ {
				d := sha256.Sum256([]byte(fmt.Sprintf("v-%d", v)))
				probes = append(probes, d[:])
				for i := 0; i < shape.perVal; i++ {
					tr = append(tr, triple{NodeKind, id, "k", d[:]})
					id++
				}
			}
			p := NewPropertyIndex()
			if err := p.AttachBase(newFakeBase(tr)); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if ids := p.NodesByProperty("k", probes[i%len(probes)]); len(ids) == 0 {
					b.Fatal("miss")
				}
			}
		})
	}
}
