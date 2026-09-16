package index

// What a capture costs the write path, which is the question that decides
// whether a compaction can afford to record its build window at all.
//
// The two arms are the same registrations against the same index with the only
// difference being whether a capture is open, so the gap is the recording and
// nothing else. Both figures in tail.go's tailBlockOps comment come from here:
// the first shape appended to one growing slice and cost 269 B of garbage per
// mutation and 1.2-1.3x the wall clock, and fixed blocks took that to the 48 B
// it actually holds and a gap inside the noise.

import (
	"strconv"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func benchmarkIndexNode(b *testing.B, capturing bool) {
	p := NewPropertyIndex()
	if capturing {
		// Large enough that the capture cannot be dropped part way, which would
		// turn the rest of the run into the arm this is being compared against.
		p.CaptureTail(1 << 40)
	}
	// A fixed set of values, so the benchmark measures registration rather than
	// the allocation of the values being registered.
	vals := make([]string, 256)
	for i := range vals {
		vals[i] = strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		p.IndexNode(store.NodeID(i), "k", []byte(vals[i&255]))
	}
}

func BenchmarkIndexNodeCaptureOff(b *testing.B) { benchmarkIndexNode(b, false) }
func BenchmarkIndexNodeCaptureOn(b *testing.B)  { benchmarkIndexNode(b, true) }
