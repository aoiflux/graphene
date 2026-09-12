package index

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/aoiflux/graphene/store"
)

func TestScratchSizes(t *testing.T) {
	fmt.Printf("propRef                = %d\n", unsafe.Sizeof(propRef{}))
	fmt.Printf("postings[NodeID]       = %d\n", unsafe.Sizeof(postings[store.NodeID]{}))
	fmt.Printf("propertyShard          = %d\n", unsafe.Sizeof(propertyShard{}))
	fmt.Printf("PropertyIndex          = %d\n", unsafe.Sizeof(PropertyIndex{}))
	fmt.Printf("sync.RWMutex           = %d\n", unsafe.Sizeof(sync.RWMutex{}))
	fmt.Printf("orderedValue[NodeID]   = %d\n", unsafe.Sizeof(orderedValue[store.NodeID]{}))
	fmt.Printf("orderedIndex[NodeID]   = %d\n", unsafe.Sizeof(orderedIndex[store.NodeID]{}))
	fmt.Printf("memberState            = %d\n", unsafe.Sizeof(memberState{}))
	fmt.Printf("compositeIndex[NodeID] = %d\n", unsafe.Sizeof(compositeIndex[store.NodeID]{}))
	fmt.Printf("compositeSet[NodeID]   = %d\n", unsafe.Sizeof(compositeSet[store.NodeID]{}))
	fmt.Printf("compositeMember[NodeID]= %d\n", unsafe.Sizeof(compositeMember[store.NodeID]{}))
	fmt.Printf("NodePropEntry          = %d\n", unsafe.Sizeof(NodePropEntry{}))
	fmt.Printf("PropEntry              = %d\n", unsafe.Sizeof(PropEntry{}))
	fmt.Printf("UniqueConflict         = %d\n", unsafe.Sizeof(store.UniqueConflict{}))
}

func heapBytes() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestScratchMapCost(t *testing.T) {
	const n = 1 << 20
	for _, tc := range []struct {
		name string
		fill func() any
	}{
		{"map[uint64]propRef", func() any {
			m := make(map[uint64]propRef)
			for i := 0; i < n; i++ {
				m[uint64(i)] = propRef{keyID: 1}
			}
			return m
		}},
		{"map[uint64][]propRef", func() any {
			m := make(map[uint64][]propRef)
			for i := 0; i < n; i++ {
				m[uint64(i)] = nil
			}
			return m
		}},
		{"map[uint64]*memberState", func() any {
			m := make(map[uint64]*memberState)
			for i := 0; i < n; i++ {
				m[uint64(i)] = nil
			}
			return m
		}},
		{"map[string]string(shared,16B keys)", func() any {
			m := make(map[string]string)
			for i := 0; i < n; i++ {
				s := fmt.Sprintf("%016d", i)
				m[s] = s
			}
			return m
		}},
	} {
		before := heapBytes()
		v := tc.fill()
		after := heapBytes()
		fmt.Printf("%-38s %8.2f B/entry (n=%d)\n", tc.name, float64(after-before)/float64(n), n)
		runtime.KeepAlive(v)
	}
}
