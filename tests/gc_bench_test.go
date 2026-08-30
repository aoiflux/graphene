//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// GC cycle cost on a store loaded from disk.
//
// This is the arena's actual claim and the one measurement the spike could not
// make: the spike modelled the layout standalone, so what it proved was that a
// pointer-free arena is cheap to scan, not that the shipped loader produces one.
// A CSR image lives for the whole life of the process and is scanned by every
// cycle, so this is the number the change stands or falls on — reopen cost is
// paid once, this is paid forever.
//
// Blob sizes are swept because the spike's own finding was that the resident
// prize collapses as blobs grow. If the GC prize does the same, the change has
// no case left; if it stays flat, that is the whole argument for it.
func benchGCLoaded(b *testing.B, nodes int, blob int) {
	dir, err := os.MkdirTemp("", "graphene-gc-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	recs := make([]*store.Node, nodes)
	props := make([]byte, blob)
	for i := range props {
		props[i] = byte(i)
	}
	for i := range recs {
		n := &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		if blob > 0 {
			n.Properties = append([]byte(nil), props...)
		}
		recs[i] = n
	}
	ids, err := g.AddNodes(recs)
	if err != nil {
		b.Fatal(err)
	}
	edges := make([]*store.Edge, 0, nodes*2)
	for i := 0; i < nodes; i++ {
		edges = append(edges, &store.Edge{
			Src: ids[i], Dst: ids[(i+1)%nodes],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
		edges = append(edges, &store.Edge{
			Src: ids[i], Dst: ids[(i+7)%nodes],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		b.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		b.Fatal(err)
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}

	// Reopen: THIS is the load path under test. The image now in memory was
	// built by the reader, not by Build, which is the whole point.
	live, err := graphene.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer live.Close()
	if _, err := live.GetNode(ids[nodes/2]); err != nil {
		b.Fatal(err)
	}

	// Fixed cycle count at a disabled GC percentage: at a fixed GOGC a smaller
	// live heap triggers proportionally more cycles, so a comparison of two
	// layouts at GOGC=on measures the trigger rate rather than the scan cost.
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	runtime.GC()

	// ns/op is the per-cycle scan cost directly: the loop body is one forced
	// collection and nothing else.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.GC()
	}
	b.StopTimer()
	runtime.KeepAlive(live)
}

func BenchmarkGCLoadedStore(b *testing.B) {
	for _, blob := range []int{0, 64, 512} {
		b.Run(fmt.Sprintf("blob%d", blob), func(b *testing.B) {
			benchGCLoaded(b, 100_000, blob)
		})
	}
}
