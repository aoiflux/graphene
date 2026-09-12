//go:build stress

package disk

// What reading the property index in place is worth, measured.
//
//	go test ./disk/ -tags=stress -run TestIndexModeSpike -v
//
// # Why this exists before the default moves
//
// docs/TECHNICAL_DETAILS.md §14.13 records a residency figure of "±0.0%" for a
// change that plainly moved bytes, and the reason it recorded that is that no
// fixture reopened a store from disk — so the instrument agreed with whatever it
// was pointed at. The mapped index is a much larger claim than that one, and it
// is about a number this file is the only thing that measures: what a process
// holds after opening a store and doing nothing else, with the index in the image
// against the same index in the heap.
//
// It asserts nothing about the sizes — the point is the sizes — but it does fail
// if either arm answers differently from the other, so a broken fixture cannot be
// mistaken for a saving.
//
// # What it does not measure
//
// The heap is not the resident set. Mapped pages are file-backed and this reports
// HeapAlloc, so the index's bytes leave this measurement entirely rather than
// moving to another column in it; tests/rss_bench_test.go is what separates anon
// from file, and it cannot reach the writer that makes a v9 image. Read the two
// together: this says how much anonymous memory stops being allocated, and the
// RSS suite says what the kernel then holds on the store's behalf.
//
// The records carry no property blob, and that is not a gap either: under the
// default ImageMode the blob half of an image is already file-backed, so it is
// not in this figure under either arm and adding it would only make the store
// take longer to build. What is left in the figure is the record arrays, the
// label arenas, and the index — which is the term being moved.

import (
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// v9SpikeStore builds a store of n nodes with the brief's index shape in
// miniature — one all-distinct key, one low-cardinality key, and two that are a
// composite together — and rewrites its image with the index in it.
func v9SpikeStore(tb testing.TB, dir string, n int) {
	tb.Helper()
	s, err := Open(dir)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	if err := s.DeclareOrderedNodeProperty("seq"); err != nil {
		tb.Fatalf("DeclareOrderedNodeProperty: %v", err)
	}
	if err := s.DeclareCompositeNodeProperties([]string{"bucket", "shard"}); err != nil {
		tb.Fatalf("DeclareCompositeNodeProperties: %v", err)
	}
	for i := 0; i < n; i++ {
		id, err := s.AddNode(&store.Node{
			Labels: []store.NodeType{store.NodeTypeEvidenceFile},
		})
		if err != nil {
			tb.Fatalf("AddNode: %v", err)
		}
		for _, kv := range [][2]string{
			{"seq", fmt.Sprintf("%08d", i)},
			{"bucket", fmt.Sprintf("b%d", i%64)},
			{"shard", fmt.Sprintf("s%d", i%16)},
		} {
			if err := s.IndexNodeProperty(id, kv[0], []byte(kv[1])); err != nil {
				tb.Fatalf("IndexNodeProperty: %v", err)
			}
		}
	}
	if err := s.Compact(); err != nil {
		tb.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		tb.Fatalf("Close: %v", err)
	}
	rewriteImageAsV9(tb, dir)
}

// TestIndexModeSpike reports the prize: the anonymous memory a store stops
// holding when its index stays in the image.
func TestIndexModeSpike(t *testing.T) {
	// Two sizes, because the claim is that the saving is proportional to the
	// entries rather than a constant: three keys a node, so 60,000 entries against
	// 150,000.
	for _, nodes := range []int{20_000, 50_000} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			dir := t.TempDir()
			v9SpikeStore(t, dir, nodes)

			type arm struct {
				mode   IndexMode
				heap   uint64
				open   time.Duration
				lookup time.Duration
				ids    []store.NodeID
				stats  store.StorageStats
			}
			arms := make([]arm, 0, 2)
			for _, mode := range []IndexMode{IndexResident, IndexMapped} {
				base := liveHeapD(nil)
				start := time.Now()
				s, err := OpenWithOptions(dir, Options{IndexMode: mode})
				if err != nil {
					t.Fatalf("%v: Open: %v", mode, err)
				}
				took := time.Since(start)
				total := liveHeapD(s)

				// Point lookups on the all-distinct key, which is the latency this
				// trade spends. Warm: the pages were written moments ago. Enough of
				// them that the total clears the clock's granularity by a wide
				// margin — a per-probe time.Since here reports zero.
				const probes = 50_000
				keys := make([][]byte, probes)
				for i := range keys {
					keys[i] = []byte(fmt.Sprintf("%08d", i*37%nodes))
				}
				lookStart := time.Now()
				for _, k := range keys {
					if _, err := s.NodesByProperty("seq", k); err != nil {
						t.Fatalf("%v: NodesByProperty: %v", mode, err)
					}
				}
				perLookup := time.Since(lookStart) / probes

				ids, err := s.NodesByProperty("bucket", []byte("b7"))
				if err != nil {
					t.Fatalf("%v: NodesByProperty: %v", mode, err)
				}
				slices.Sort(ids)
				st := s.StorageStats()
				if err := s.Close(); err != nil {
					t.Fatalf("%v: Close: %v", mode, err)
				}
				arms = append(arms, arm{
					mode: mode, heap: total - base, open: took,
					lookup: perLookup, ids: ids, stats: st,
				})
			}

			res, mapped := arms[0], arms[1]
			if res.stats.IndexMode != IndexResident.String() {
				t.Fatalf("the resident arm reports %q", res.stats.IndexMode)
			}
			if mapped.stats.IndexMode != IndexMapped.String() {
				t.Fatalf("the mapped arm reports %q, so it did not attach a base", mapped.stats.IndexMode)
			}
			// The measurement is only a measurement if both arms are right.
			if !slices.Equal(res.ids, mapped.ids) {
				t.Fatalf("the two arms disagree: %d ids against %d", len(res.ids), len(mapped.ids))
			}

			t.Logf("resident: heap %s, open %v, lookup %v",
				mib(res.heap), res.open.Round(time.Millisecond), res.lookup)
			t.Logf("mapped:   heap %s, open %v, lookup %v",
				mib(mapped.heap), mapped.open.Round(time.Millisecond), mapped.lookup)
			if res.heap > 0 {
				t.Logf("heap %+.1f%%, open %+.1f%%, lookup %+.1f%%",
					pctD(mapped.heap, res.heap),
					pctD(uint64(mapped.open), uint64(res.open)),
					pctD(uint64(mapped.lookup), uint64(res.lookup)))
			}
			t.Logf("entries reported: resident %d, mapped %d (an upper bound under a base)",
				res.stats.PropertyNodeEntries, mapped.stats.PropertyNodeEntries)
		})
	}
	runtime.GC()
}

// pctD is the change from was to now, as a percentage of was.
func pctD(now, was uint64) float64 {
	if was == 0 {
		return 0
	}
	return (float64(now) - float64(was)) / float64(was) * 100
}
