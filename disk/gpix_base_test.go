package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// The tests in this file judge the mapped reader against the resident index,
// not against a model of it. That is the whole point of the exercise: the
// resident PropertyIndex is what every caller in the engine reads today, and R3
// replaces it with base ∪ delta − retracted. Unless the base alone answers
// exactly what the resident index answers for the same entries, the union
// cannot, and no amount of self-consistency in the format would say so.
//
// So each arm here loads one set of triples into both structures and compares
// answers, including for keys, values and ids that are absent — the cases a
// round-trip test never reaches, and where a reader that confuses "missing" with
// "empty" is indistinguishable from a correct one until it is not.

// --- the paired fixture ---

type propTriple struct {
	kind  uint8
	key   string
	value string
	id    uint64
}

type basePair struct {
	resident *index.PropertyIndex
	base     *gpixBase
	model    *gpixFixture
	sec      *gpixSection
}

// newBasePair loads the same triples into a resident index and an encoded,
// parsed GPIX/GPIR pair.
func newBasePair(t testing.TB, triples []propTriple) *basePair {
	t.Helper()

	resident := index.NewPropertyIndex()
	model := newGPIXFixture()
	seen := map[propTriple]bool{}
	for _, tr := range triples {
		if seen[tr] {
			continue
		}
		seen[tr] = true
		model.add(tr.kind, tr.key, tr.value, tr.id)
		if tr.kind == gpixKindNode {
			resident.IndexNode(store.NodeID(tr.id), tr.key, []byte(tr.value))
		} else {
			resident.IndexEdge(store.EdgeID(tr.id), tr.key, []byte(tr.value))
		}
	}
	// The encoder requires ascending ids per value, which the model's walker
	// sorts, but it also requires no repeats — the same contract the resident
	// index enforces through insertSorted.
	for _, m := range []map[string]map[string][]uint64{model.nodes, model.edges} {
		for _, bucket := range m {
			for v, ids := range bucket {
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				bucket[v] = ids
			}
		}
	}

	sec, err := parseGPIX(model.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	rev, err := parseGPIR(buildGPIRFor(t, sec))
	if err != nil {
		t.Fatalf("parseGPIR: %v", err)
	}
	base, err := newGPIXBase(sec, rev, nil)
	if err != nil {
		t.Fatalf("newGPIXBase: %v", err)
	}
	return &basePair{resident: resident, base: base, model: model, sec: sec}
}

// buildGPIRFor derives the reverse section from an encoded forward one.
//
// R3d builds GPIR with an external counting sort, because at 28M entries the
// slice below is 672 MiB and the problem this program exists to solve. Here the
// entries are thousands and holding them is the cheapest way to be sure the
// reader is judged against entries a writer could actually have produced: every
// valueOff is derived from the section the forward encoder wrote, not asserted.
func buildGPIRFor(t testing.TB, sec *gpixSection) []byte {
	t.Helper()
	var nodes, edges []gpirEntry
	for ki := range sec.keys {
		k := &sec.keys[ki]
		for i := uint64(0); i < k.Distinct; i++ {
			value, ids, err := k.runAt(i)
			if err != nil {
				t.Fatalf("runAt(%q, %d): %v", k.Name, i, err)
			}
			// A run is valLen u32 then the value, so the value bytes begin four
			// bytes into the run the value table points at.
			valueOff := binary.LittleEndian.Uint64(k.vtab[i*gpixVtabEntry+8:]) + 4
			for j := 0; j < gpixIDCount(ids); j++ {
				e := gpirEntry{
					ID:       gpixIDAt(ids, j),
					KeyID:    k.KeyID,
					ValLen:   uint32(len(value)),
					ValueOff: valueOff,
				}
				if k.Kind == gpixKindNode {
					nodes = append(nodes, e)
				} else {
					edges = append(edges, e)
				}
			}
		}
	}
	sortGPIREntries(nodes)
	sortGPIREntries(edges)
	return encodeGPIR(t, nodes, edges)
}

func sortGPIREntries(e []gpirEntry) {
	sort.Slice(e, func(i, j int) bool {
		if e[i].ID != e[j].ID {
			return e[i].ID < e[j].ID
		}
		if e[i].KeyID != e[j].KeyID {
			return e[i].KeyID < e[j].KeyID
		}
		return e[i].ValueOff < e[j].ValueOff
	})
}

// generateTriples produces a shape with the properties the brief's index has:
// an all-distinct digest key, a low-cardinality bucket key, a key whose values
// share a long prefix, a single-value key, and an edge key.
func generateTriples(seed int64, n int) []propTriple {
	rng := rand.New(rand.NewSource(seed))
	out := make([]propTriple, 0, n)
	for i := 0; i < n; i++ {
		id := uint64(rng.Int63n(1 << 18))
		switch rng.Intn(6) {
		case 0:
			b := make([]byte, 32)
			rng.Read(b)
			out = append(out, propTriple{gpixKindNode, "sha256", string(b), id})
		case 1:
			out = append(out, propTriple{gpixKindNode, "bucket", fmt.Sprintf("b%d", rng.Intn(4)), id})
		case 2:
			// Twenty-five shared bytes, so every comparison falls through the
			// eight-byte prefix to the value itself.
			out = append(out, propTriple{gpixKindNode, "url",
				fmt.Sprintf("https://example.com/a/%08d", rng.Intn(5000)), id})
		case 3:
			out = append(out, propTriple{gpixKindNode, "kind", "document", id})
		case 4:
			out = append(out, propTriple{gpixKindNode, "ts", fmt.Sprintf("%019d", rng.Int63()), id})
		default:
			out = append(out, propTriple{gpixKindEdge, "rel", fmt.Sprintf("r%d", rng.Intn(30)), id})
		}
	}
	return out
}

func (p *basePair) modelOf(kind uint8) map[string]map[string][]uint64 {
	if kind == gpixKindEdge {
		return p.model.edges
	}
	return p.model.nodes
}

// residentIDs asks the resident index the question Lookup asks the base.
func (p *basePair) residentIDs(kind uint8, key, value string) []uint64 {
	if kind == gpixKindEdge {
		ids := p.resident.EdgesByProperty(key, []byte(value))
		out := make([]uint64, len(ids))
		for i, id := range ids {
			out[i] = uint64(id)
		}
		return out
	}
	ids := p.resident.NodesByProperty(key, []byte(value))
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

func runIDs(r index.IDRun) []uint64 {
	out := make([]uint64, r.Len())
	for i := range out {
		out[i] = r.At(i)
	}
	return out
}

func kindName(kind uint8) string {
	if kind == gpixKindEdge {
		return "edge"
	}
	return "node"
}

func entityKind(kind uint8) index.EntityKind { return index.EntityKind(kind) }

// --- the differential arms ---

func TestGPIXBase_LookupMatchesResident(t *testing.T) {
	p := newBasePair(t, generateTriples(20260913, 4000))

	probes := 0
	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		for key, bucket := range p.modelOf(kind) {
			for value := range bucket {
				got, err := p.base.Lookup(entityKind(kind), key, []byte(value))
				if err != nil {
					t.Fatalf("Lookup(%s, %q): %v", kindName(kind), key, err)
				}
				want := p.residentIDs(kind, key, value)
				if !equalIDs(runIDs(got), want) {
					t.Fatalf("Lookup(%s, %q, %q) = %v, resident says %v",
						kindName(kind), key, value, runIDs(got), want)
				}
				probes++
			}
		}
	}
	if probes == 0 {
		t.Fatal("the fixture produced no values to probe")
	}

	// Absent values and absent keys are the half a round trip never reaches.
	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		for _, absent := range []struct{ key, value string }{
			{"sha256", "not a digest"},
			{"bucket", "b9999"},
			{"url", "https://example.com/a/99999999"},
			{"rel", "r99999"},
			{"no-such-key", "anything"},
			{"", ""},
		} {
			got, err := p.base.Lookup(entityKind(kind), absent.key, []byte(absent.value))
			if err != nil {
				t.Fatalf("Lookup of an absent %s %q=%q: %v",
					kindName(kind), absent.key, absent.value, err)
			}
			want := p.residentIDs(kind, absent.key, absent.value)
			if got.Len() != len(want) {
				t.Fatalf("Lookup(%s, %q, %q) returned %d ids, resident says %d",
					kindName(kind), absent.key, absent.value, got.Len(), len(want))
			}
		}
	}
}

func TestGPIXBase_KeysAndCountsMatchResident(t *testing.T) {
	p := newBasePair(t, generateTriples(20260914, 4000))

	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		wantKeys := p.model.keys(kind)
		if got := p.base.Keys(entityKind(kind)); !equalStrings(got, wantKeys) {
			t.Fatalf("Keys(%s) = %v, want %v", kindName(kind), got, wantKeys)
		}
		for _, key := range wantKeys {
			bucket := p.modelOf(kind)[key]
			wantEntries := 0
			for _, ids := range bucket {
				wantEntries += len(ids)
			}
			distinct, entries := p.base.KeyStats(entityKind(kind), key)
			if distinct != len(bucket) || entries != wantEntries {
				t.Fatalf("KeyStats(%s, %q) = (%d, %d), want (%d, %d)",
					kindName(kind), key, distinct, entries, len(bucket), wantEntries)
			}
		}
		if d, e := p.base.KeyStats(entityKind(kind), "no-such-key"); d != 0 || e != 0 {
			t.Fatalf("KeyStats of an absent key = (%d, %d), want (0, 0)", d, e)
		}
	}

	wantNodes, wantEdges := p.resident.EntryCounts()
	if got := p.base.TotalEntries(index.NodeKind); got != wantNodes {
		t.Fatalf("TotalEntries(node) = %d, resident says %d", got, wantNodes)
	}
	if got := p.base.TotalEntries(index.EdgeKind); got != wantEdges {
		t.Fatalf("TotalEntries(edge) = %d, resident says %d", got, wantEdges)
	}
}

func TestGPIXBase_ForEachValueIsAscendingAndComplete(t *testing.T) {
	p := newBasePair(t, generateTriples(20260915, 4000))

	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		for key, bucket := range p.modelOf(kind) {
			var last []byte
			seen := 0
			err := p.base.ForEachValue(entityKind(kind), key, nil,
				func(value []byte, ids index.IDRun) bool {
					if last != nil && bytes.Compare(last, value) >= 0 {
						t.Fatalf("ForEachValue(%q) yielded %q after %q", key, value, last)
					}
					last = append(last[:0:0], value...)
					if want := p.residentIDs(kind, key, string(value)); !equalIDs(runIDs(ids), want) {
						t.Fatalf("ForEachValue(%q) gave %q = %v, resident says %v",
							key, value, runIDs(ids), want)
					}
					seen++
					return true
				})
			if err != nil {
				t.Fatalf("ForEachValue(%q): %v", key, err)
			}
			if seen != len(bucket) {
				t.Fatalf("ForEachValue(%q) yielded %d values, the index holds %d", key, seen, len(bucket))
			}
		}
	}

	// A walk of an absent key is empty and not an error: the same answer the
	// resident index gives for a key nothing was ever registered under.
	called := false
	if err := p.base.ForEachValue(index.NodeKind, "no-such-key", nil,
		func([]byte, index.IDRun) bool { called = true; return true }); err != nil {
		t.Fatalf("ForEachValue of an absent key: %v", err)
	}
	if called {
		t.Fatal("ForEachValue of an absent key yielded a value")
	}
}

func TestGPIXBase_ForEachValueFromIsALowerBound(t *testing.T) {
	p := newBasePair(t, generateTriples(20260916, 4000))

	for key, bucket := range p.modelOf(gpixKindNode) {
		sorted := make([]string, 0, len(bucket))
		for v := range bucket {
			sorted = append(sorted, v)
		}
		sort.Strings(sorted)

		// Probe each present value, one below it and one above it, so the bound
		// is exercised on a hit, a near miss below and a near miss above.
		for _, i := range []int{0, len(sorted) / 3, len(sorted) / 2, len(sorted) - 1} {
			for _, from := range []string{sorted[i], sorted[i] + "\x00", trimLast(sorted[i])} {
				want := sorted[sort.SearchStrings(sorted, from):]
				var got []string
				err := p.base.ForEachValue(index.NodeKind, key, []byte(from),
					func(value []byte, _ index.IDRun) bool {
						got = append(got, string(value))
						return true
					})
				if err != nil {
					t.Fatalf("ForEachValue(%q, from %q): %v", key, from, err)
				}
				if !equalStrings(got, want) {
					t.Fatalf("ForEachValue(%q, from %q) yielded %d values, want %d (first got %q, want %q)",
						key, from, len(got), len(want), firstOr(got), firstOr(want))
				}
			}
		}

		// A bound past the last value yields nothing; the empty bound yields
		// everything, which is what a nil from means.
		var after int
		if err := p.base.ForEachValue(index.NodeKind, key, []byte{0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff, 0xff}, func([]byte, index.IDRun) bool { after++; return true }); err != nil {
			t.Fatalf("ForEachValue(%q) past the end: %v", key, err)
		}
		if after != 0 && sorted[len(sorted)-1] < "\xff\xff\xff\xff\xff\xff\xff\xff\xff" {
			t.Fatalf("ForEachValue(%q) past the last value yielded %d", key, after)
		}
	}
}

func trimLast(s string) string {
	if s == "" {
		return s
	}
	return s[:len(s)-1]
}

func firstOr(s []string) string {
	if len(s) == 0 {
		return "<none>"
	}
	return s[0]
}

func TestGPIXBase_ForEachValueStopsWhenAsked(t *testing.T) {
	p := newBasePair(t, generateTriples(20260917, 2000))
	for key := range p.modelOf(gpixKindNode) {
		seen := 0
		err := p.base.ForEachValue(index.NodeKind, key, nil, func([]byte, index.IDRun) bool {
			seen++
			return seen < 3
		})
		if err != nil {
			t.Fatalf("ForEachValue(%q): %v", key, err)
		}
		if distinct, _ := p.base.KeyStats(index.NodeKind, key); distinct >= 3 && seen != 3 {
			t.Fatalf("ForEachValue(%q) visited %d values after being stopped at 3", key, seen)
		}
	}
}

func TestGPIXBase_EntriesOfMatchesResident(t *testing.T) {
	p := newBasePair(t, generateTriples(20260918, 4000))

	checked := 0
	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		for _, id := range p.indexedIDs(kind) {
			var got []index.PropEntry
			err := p.base.ForEachEntryOf(entityKind(kind), id, func(key string, value []byte) bool {
				got = append(got, index.PropEntry{Key: key, Value: append([]byte(nil), value...)})
				return true
			})
			if err != nil {
				t.Fatalf("ForEachEntryOf(%s %d): %v", kindName(kind), id, err)
			}
			want := p.residentEntriesOf(kind, id)
			if !equalEntries(got, want) {
				t.Fatalf("ForEachEntryOf(%s %d) = %v, resident says %v",
					kindName(kind), id, got, want)
			}
			// GPIR's own order is key-then-value ascending; nothing sorted the
			// slice above, so this asserts the claim the reader documents.
			if !sortedEntries(got) {
				t.Fatalf("ForEachEntryOf(%s %d) yielded %v out of order", kindName(kind), id, got)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("the fixture indexed no entities")
	}

	// An id nothing was indexed under yields nothing, on both sides.
	for _, absent := range []uint64{0, 1 << 40, ^uint64(0)} {
		if p.resident.NodeHasEntries(store.NodeID(absent)) {
			continue
		}
		called := false
		if err := p.base.ForEachEntryOf(index.NodeKind, absent,
			func(string, []byte) bool { called = true; return true }); err != nil {
			t.Fatalf("ForEachEntryOf(node %d): %v", absent, err)
		}
		if called {
			t.Fatalf("ForEachEntryOf of unindexed node %d yielded an entry", absent)
		}
	}
}

func TestGPIXBase_HasEntriesMatchesResident(t *testing.T) {
	p := newBasePair(t, generateTriples(20260919, 3000))

	for _, id := range p.indexedIDs(gpixKindNode) {
		if !p.base.HasEntries(index.NodeKind, id) {
			t.Fatalf("HasEntries(node %d) is false, resident has entries for it", id)
		}
	}
	// Every id in a band, so the misses are probed as densely as the hits.
	for id := uint64(0); id < 4096; id++ {
		want := p.resident.NodeHasEntries(store.NodeID(id))
		if got := p.base.HasEntries(index.NodeKind, id); got != want {
			t.Fatalf("HasEntries(node %d) = %v, resident says %v", id, got, want)
		}
	}
	for id := uint64(0); id < 4096; id++ {
		want := p.resident.EdgeHasEntries(store.EdgeID(id))
		if got := p.base.HasEntries(index.EdgeKind, id); got != want {
			t.Fatalf("HasEntries(edge %d) = %v, resident says %v", id, got, want)
		}
	}
}

func TestGPIXBase_ForEachIDMatchesResident(t *testing.T) {
	p := newBasePair(t, generateTriples(20260920, 3000))

	for _, kind := range []uint8{gpixKindNode, gpixKindEdge} {
		var got []uint64
		var last uint64
		first := true
		p.base.ForEachID(entityKind(kind), func(id uint64) bool {
			if !first && id <= last {
				t.Fatalf("ForEachID(%s) yielded %d after %d", kindName(kind), id, last)
			}
			last, first = id, false
			got = append(got, id)
			return true
		})
		want := p.indexedIDs(kind)
		if !equalIDs(got, want) {
			t.Fatalf("ForEachID(%s) yielded %d ids, resident has %d",
				kindName(kind), len(got), len(want))
		}
		if len(want) > 0 {
			if max := p.base.MaxID(entityKind(kind)); max != want[len(want)-1] {
				t.Fatalf("MaxID(%s) = %d, the highest indexed id is %d",
					kindName(kind), max, want[len(want)-1])
			}
		}

		// Stopping the walk stops it.
		seen := 0
		p.base.ForEachID(entityKind(kind), func(uint64) bool { seen++; return seen < 5 })
		if len(want) >= 5 && seen != 5 {
			t.Fatalf("ForEachID(%s) visited %d ids after being stopped at 5", kindName(kind), seen)
		}
	}
}

// indexedIDs asks the resident index which ids carry an entry, ascending and
// deduplicated.
//
// The deduplication is not incidental. ForEachIndexedNodeID yields an id once
// per shard that holds a ref for it — documented, and deliberate, because
// deduplicating there is the map that walk exists to avoid. GPIR yields each id
// exactly once, because the array it walks is sorted by id and a sorted array
// costs nothing to deduplicate. So the two are compared as sets, and the mapped
// side is additionally asserted to be strictly ascending, which the resident
// side is not.
func (p *basePair) indexedIDs(kind uint8) []uint64 {
	var out []uint64
	if kind == gpixKindEdge {
		p.resident.ForEachIndexedEdgeID(func(id store.EdgeID) bool {
			out = append(out, uint64(id))
			return true
		})
	} else {
		p.resident.ForEachIndexedNodeID(func(id store.NodeID) bool {
			out = append(out, uint64(id))
			return true
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	n := 0
	for i, id := range out {
		if i == 0 || id != out[n-1] {
			out[n] = id
			n++
		}
	}
	return out[:n]
}

func (p *basePair) residentEntriesOf(kind uint8, id uint64) []index.PropEntry {
	if kind == gpixKindEdge {
		return p.resident.EdgeEntriesOf(store.EdgeID(id))
	}
	return p.resident.NodeEntriesOf(store.NodeID(id))
}

func equalEntries(a, b []index.PropEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

func sortedEntries(e []index.PropEntry) bool {
	for i := 1; i < len(e); i++ {
		if e[i-1].Key > e[i].Key {
			return false
		}
		if e[i-1].Key == e[i].Key && bytes.Compare(e[i-1].Value, e[i].Value) > 0 {
			return false
		}
	}
	return true
}

// --- the empty base ---

func TestGPIXBase_EmptySectionAnswersEverythingEmpty(t *testing.T) {
	p := newBasePair(t, nil)

	for _, kind := range []index.EntityKind{index.NodeKind, index.EdgeKind} {
		if got := p.base.Keys(kind); len(got) != 0 {
			t.Fatalf("Keys(%s) of an empty base = %v", kind, got)
		}
		if got := p.base.TotalEntries(kind); got != 0 {
			t.Fatalf("TotalEntries(%s) of an empty base = %d", kind, got)
		}
		if got := p.base.MaxID(kind); got != 0 {
			t.Fatalf("MaxID(%s) of an empty base = %d", kind, got)
		}
		if p.base.HasEntries(kind, 1) {
			t.Fatalf("HasEntries(%s, 1) on an empty base", kind)
		}
		ids, err := p.base.Lookup(kind, "k", []byte("v"))
		if err != nil || ids.Len() != 0 {
			t.Fatalf("Lookup on an empty base = (%d ids, %v)", ids.Len(), err)
		}
		if err := p.base.ForEachValue(kind, "k", nil, func([]byte, index.IDRun) bool {
			t.Fatal("an empty base yielded a value")
			return false
		}); err != nil {
			t.Fatalf("ForEachValue on an empty base: %v", err)
		}
		p.base.ForEachID(kind, func(uint64) bool {
			t.Fatal("an empty base yielded an id")
			return false
		})
	}
}

// --- refusals ---

func TestGPIXBase_RefusesHalfAnImage(t *testing.T) {
	p := newBasePair(t, generateTriples(20260921, 200))
	rev, err := parseGPIR(buildGPIRFor(t, p.sec))
	if err != nil {
		t.Fatalf("parseGPIR: %v", err)
	}
	for _, tc := range []struct {
		name string
		fwd  *gpixSection
		rev  *gpirSection
	}{
		{"neither", nil, nil},
		{"forward only", p.sec, nil},
		{"reverse only", nil, rev},
	} {
		if _, err := newGPIXBase(tc.fwd, tc.rev, nil); err == nil {
			t.Fatalf("newGPIXBase accepted %s", tc.name)
		}
	}
}

func TestGPIXBase_RefusesHostileReverseEntries(t *testing.T) {
	f := newGPIXFixture()
	f.add(gpixKindNode, "nkey", "nvalue", 7)
	f.add(gpixKindEdge, "ekey", "evalue", 7)
	sec, err := parseGPIX(f.encode(t))
	if err != nil {
		t.Fatalf("parseGPIX: %v", err)
	}
	// The node key is directory entry 0 and the edge key entry 1, which is what
	// the cross-kind case below relies on.
	if len(sec.keys) != 2 || sec.keys[0].Kind != gpixKindNode || sec.keys[1].Kind != gpixKindEdge {
		t.Fatalf("fixture produced %d keys in an unexpected order", len(sec.keys))
	}
	valueOff := binary.LittleEndian.Uint64(sec.keys[1].vtab[8:]) + 4

	for _, tc := range []struct {
		name  string
		entry gpirEntry
		want  string
	}{
		{
			// A node's reverse entry naming an edge key. The value resolves
			// cleanly out of the wrong key's runs, so only the kind check
			// catches it.
			name:  "cross-kind key",
			entry: gpirEntry{ID: 7, KeyID: 1, ValLen: uint32(len("evalue")), ValueOff: valueOff},
			want:  "is a edge key",
		},
		{
			name:  "key id past the directory",
			entry: gpirEntry{ID: 7, KeyID: 9, ValLen: 1, ValueOff: 0},
			want:  "names key 9",
		},
		{
			name:  "value past the key's runs",
			entry: gpirEntry{ID: 7, KeyID: 0, ValLen: 1, ValueOff: ^uint64(0)},
			want:  "addresses",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rev, err := parseGPIR(encodeGPIR(t, []gpirEntry{tc.entry}, nil))
			if err != nil {
				t.Fatalf("parseGPIR: %v", err)
			}
			base, err := newGPIXBase(sec, rev, nil)
			if err != nil {
				t.Fatalf("newGPIXBase: %v", err)
			}
			err = base.ForEachEntryOf(index.NodeKind, 7, func(string, []byte) bool { return true })
			if err == nil {
				t.Fatal("ForEachEntryOf accepted the entry")
			}
			if !bytes.Contains([]byte(err.Error()), []byte(tc.want)) {
				t.Fatalf("error %q does not name the problem (%q)", err, tc.want)
			}
		})
	}
}

// --- IDRun ---

func TestIDRun(t *testing.T) {
	var raw []byte
	want := []uint64{3, 9, 14, 900, 1 << 40}
	for _, id := range want {
		raw = binary.LittleEndian.AppendUint64(raw, id)
	}
	r := index.NewIDRun(raw)

	if r.Len() != len(want) {
		t.Fatalf("Len = %d, want %d", r.Len(), len(want))
	}
	if got := runIDs(r); !equalIDs(got, want) {
		t.Fatalf("run = %v, want %v", got, want)
	}
	if first, ok := r.First(); !ok || first != 3 {
		t.Fatalf("First = (%d, %v), want (3, true)", first, ok)
	}
	for _, id := range want {
		if !r.Contains(id) {
			t.Fatalf("Contains(%d) is false", id)
		}
	}
	for _, id := range []uint64{0, 2, 4, 8, 10, 13, 15, 899, 901, 1<<40 - 1, 1<<40 + 1, ^uint64(0)} {
		if r.Contains(id) {
			t.Fatalf("Contains(%d) is true", id)
		}
	}

	// A ragged length yields the whole ids and never addresses the remainder.
	ragged := index.NewIDRun(raw[:len(raw)-3])
	if got := ragged.Len(); got != len(want)-1 {
		t.Fatalf("a run with three trailing bytes reported %d ids, want %d", got, len(want)-1)
	}
	if got := runIDs(ragged); !equalIDs(got, want[:len(want)-1]) {
		t.Fatalf("a ragged run = %v, want %v", got, want[:len(want)-1])
	}
	if ragged.Contains(want[len(want)-1]) {
		t.Fatalf("a ragged run claims to contain %d, whose bytes are incomplete", want[len(want)-1])
	}

	var zero index.IDRun
	if zero.Len() != 0 || zero.Contains(1) {
		t.Fatal("the zero IDRun is not empty")
	}
	if _, ok := zero.First(); ok {
		t.Fatal("the zero IDRun has a first id")
	}
	if got := index.NewIDRun(nil).Len(); got != 0 {
		t.Fatalf("NewIDRun(nil).Len() = %d", got)
	}
}

// --- cost ---

// TestGPIXBase_ReadsDoNotAllocatePerEntry asserts the item itself: reading the
// base is flat in the number of ids it returns.
//
// A lookup that allocated per id would be the resident index with extra steps —
// the 2,748 MiB would simply move from Open to the query path. So the guard is a
// slope, as every guard in this program is: a key whose values carry ten times
// the ids must not cost ten times the allocations.
func TestGPIXBase_ReadsDoNotAllocatePerEntry(t *testing.T) {
	measure := func(idsPerValue int) float64 {
		f := newGPIXFixture()
		for v := 0; v < 64; v++ {
			value := fmt.Sprintf("v%06d", v)
			for i := 0; i < idsPerValue; i++ {
				f.add(gpixKindNode, "k", value, uint64(v*idsPerValue+i))
			}
		}
		sec, err := parseGPIX(f.encode(t))
		if err != nil {
			t.Fatalf("parseGPIX: %v", err)
		}
		rev, err := parseGPIR(buildGPIRFor(t, sec))
		if err != nil {
			t.Fatalf("parseGPIR: %v", err)
		}
		base, err := newGPIXBase(sec, rev, nil)
		if err != nil {
			t.Fatalf("newGPIXBase: %v", err)
		}
		var b index.Base = base
		probe := []byte("v000031")
		return testing.AllocsPerRun(200, func() {
			ids, err := b.Lookup(index.NodeKind, "k", probe)
			if err != nil {
				t.Fatalf("Lookup: %v", err)
			}
			var sum uint64
			for i := 0; i < ids.Len(); i++ {
				sum += ids.At(i)
			}
			if sum == 0 {
				t.Fatal("the probe found nothing")
			}
			_ = b.ForEachValue(index.NodeKind, "k", probe, func(_ []byte, r index.IDRun) bool {
				sum += uint64(r.Len())
				return false
			})
		})
	}

	small := measure(8)
	large := measure(80)
	if large > small+1 {
		t.Fatalf("reading values of 80 ids allocated %.1f objects against %.1f for 8: "+
			"the reader is copying postings, not viewing them", large, small)
	}
	t.Logf("allocations per read: %.1f at 8 ids per value, %.1f at 80", small, large)
}

func BenchmarkGPIXBaseLookup(b *testing.B) {
	for _, shape := range []struct {
		name        string
		distinct    int
		idsPerValue int
	}{
		{"AllDistinct", 50_000, 1},
		{"LowCardinality", 50, 1_000},
	} {
		b.Run(shape.name, func(b *testing.B) {
			f := newGPIXFixture()
			for v := 0; v < shape.distinct; v++ {
				value := fmt.Sprintf("v%08d", v)
				for i := 0; i < shape.idsPerValue; i++ {
					f.add(gpixKindNode, "k", value, uint64(v*shape.idsPerValue+i))
				}
			}
			body, err := encodeGPIXFrom(f.source(b.TempDir(), 0, 0))
			if err != nil {
				b.Fatalf("writeGPIX: %v", err)
			}
			sec, err := parseGPIX(body)
			if err != nil {
				b.Fatalf("parseGPIX: %v", err)
			}
			base := &gpixBase{fwd: sec, rev: &gpirSection{}}

			probes := make([][]byte, 256)
			for i := range probes {
				probes[i] = []byte(fmt.Sprintf("v%08d", (i*7919)%shape.distinct))
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sum uint64
			for i := 0; i < b.N; i++ {
				ids, err := base.Lookup(index.NodeKind, "k", probes[i%len(probes)])
				if err != nil {
					b.Fatalf("Lookup: %v", err)
				}
				sum += uint64(ids.Len())
			}
			if sum == 0 {
				b.Fatal("no probe matched")
			}
		})
	}
}
