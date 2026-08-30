//go:build stress

package graphene_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Unfiltered neighbour walks across a range of degrees. This is the shape the
// denormalised neighbour arrays exist to serve — nil edgeTypes, so no edge
// record has to be resolved for its labels — and the suite had no benchmark of
// it at any degree above ~2, which is why the win was measured in a spike and
// never seen in an engine benchmark.
//
// The point of the sweep rather than a single fixture: the saving is per
// incident edge and the per-call overhead is fixed, so what matters is the
// degree at which the saving clears the result slice and the dedupe. One
// fixture answers that only for its own degree.

var (
	degSweepOnce sync.Once
	degSweepG    *graphene.Graph
	degSweepDir  string
	degSweepHubs map[int]store.NodeID
)

func degreeSweepGraph(b *testing.B) (*graphene.Graph, map[int]store.NodeID) {
	degSweepOnce.Do(func() {
		dir, err := os.MkdirTemp("", "graphene-degsweep-*")
		if err != nil {
			panic(err)
		}
		degSweepDir = dir
		g, err := graphene.Open(dir)
		if err != nil {
			panic(err)
		}
		degrees := []int{2, 8, 64, 512, 4096, 32768}
		total := 0
		for _, d := range degrees {
			total += d + 1
		}
		nodes := make([]*store.Node, total)
		for i := range nodes {
			nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		}
		ids, err := g.AddNodes(nodes)
		if err != nil {
			panic(err)
		}
		degSweepHubs = make(map[int]store.NodeID, len(degrees))
		pos := 0
		for _, d := range degrees {
			hub := ids[pos]
			pos++
			edges := make([]*store.Edge, d)
			for i := 0; i < d; i++ {
				edges[i] = &store.Edge{Src: hub, Dst: ids[pos+i], Labels: []store.EdgeType{store.EdgeTypeContains}}
			}
			if _, err := g.AddEdges(edges); err != nil {
				panic(err)
			}
			pos += d
			degSweepHubs[d] = hub
		}
		// Compact so the walk exercises the CSR, which is where the neighbour
		// arrays live. Without this every read is served from the delta and the
		// benchmark measures the wrong layer entirely.
		if err := g.Compact(); err != nil {
			panic(err)
		}
		degSweepG = g
	})
	return degSweepG, degSweepHubs
}

// BenchmarkNeighboursByDegree is the unfiltered hop, swept by degree.
func BenchmarkNeighboursByDegree(b *testing.B) {
	g, hubs := degreeSweepGraph(b)
	for _, d := range []int{2, 8, 64, 512, 4096, 32768} {
		hub := hubs[d]
		b.Run(fmt.Sprintf("deg%d", d), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := g.GraphStore.Neighbours(hub, store.DirectionOutbound, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
