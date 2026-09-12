package index

import (
	"fmt"
	"runtime"
	"testing"
)

func TestScratchSweep(t *testing.T) {
	for _, n := range []int{100000, 500000, 800000, 900000, 917504, 917505, 1000000, 1048576, 1200000, 1835008, 1835009} {
		before := heapBytes()
		m := make(map[uint64]propRef)
		for i := 0; i < n; i++ {
			m[uint64(i)] = propRef{keyID: 1}
		}
		after := heapBytes()
		fmt.Printf("n=%7d  ref1 %6.2f B/entry  total %8.2f KB\n", n, float64(after-before)/float64(n), float64(after-before)/1024)
		runtime.KeepAlive(m)
	}
	// presized
	for _, n := range []int{1000000} {
		before := heapBytes()
		m := make(map[uint64]propRef, n)
		for i := 0; i < n; i++ {
			m[uint64(i)] = propRef{keyID: 1}
		}
		after := heapBytes()
		fmt.Printf("presized n=%7d ref1 %6.2f B/entry\n", n, float64(after-before)/float64(n))
		runtime.KeepAlive(m)
	}
}
