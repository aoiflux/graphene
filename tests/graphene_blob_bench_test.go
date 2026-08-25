//go:build stress

// Read-path benchmarks for entities that carry property blobs.
//
// Every other read benchmark in this package builds nodes as
// `&store.Node{Labels: ...}` and populates the *property index* via
// IndexNodeProperties. The index is a separate structure: the `Properties`
// byte blob on the record itself stays nil. So until this file existed, no
// benchmark ever read back a non-empty blob, and the disk backend's handling
// of blobs on the read path was entirely unmeasured.
//
// That gap hid a question worth answering: the disk store copies a record's
// blob on every materialisation, and the cost of that copy is linear in blob
// size — invisible at zero bytes, which is the only size anything measured.
//
// Blob sizes sweep 32/128/512 B so the slope is visible rather than a single
// point. Node count is deliberately smaller than benchNodeCount: these measure
// per-operation cost, not scaling, and three disk fixtures at 100k nodes cost
// build time without buying signal.

package graphene_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	graphene "github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

const (
	blobNodeCount = 20_000
	blobFanout    = 4 // outbound edges per node, so Neighbours materialises several
)

var blobSizes = []int{32, 128, 512}

var (
	blobMu       sync.Mutex
	blobMemCache = map[int]*benchFixture{}
	blobDskCache = map[int]*benchFixture{}
	blobDskDirs  []string
)

// buildBlobFixture populates g with nodes and edges that carry property blobs
// of the given size.
func buildBlobFixture(g *graphene.Graph, n, blobSize int) *benchFixture {
	nodeBlob := make([]byte, blobSize)
	for i := range nodeBlob {
		nodeBlob[i] = byte('a' + i%26)
	}

	nodes := make([]*store.Node, n)
	for i := range nodes {
		// Each node gets its own copy: AddNodes may retain what it is handed,
		// and sharing one slice across every node would make the fixture lie
		// about how much distinct blob data the store holds.
		p := make([]byte, blobSize)
		copy(p, nodeBlob)
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabel(i)}, Properties: p}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		panic(fmt.Sprintf("blob fixture: AddNodes: %v", err))
	}

	edgeBlobSize := blobSize / 2
	edges := make([]*store.Edge, 0, n*blobFanout)
	for i, id := range ids {
		for k := 1; k <= blobFanout; k++ {
			dst := ids[(i+k*7)%len(ids)]
			p := make([]byte, edgeBlobSize)
			copy(p, nodeBlob[:edgeBlobSize])
			edges = append(edges, &store.Edge{
				Src:        id,
				Dst:        dst,
				Labels:     []store.EdgeType{store.EdgeTypeContains},
				Weight:     1,
				Properties: p,
			})
		}
	}
	if _, err := g.AddEdges(edges); err != nil {
		panic(fmt.Sprintf("blob fixture: AddEdges: %v", err))
	}

	return &benchFixture{g: g, ids: ids, hub: ids[n/2], midNode: ids[n/3]}
}

func blobMemGraph(blobSize int) *benchFixture {
	blobMu.Lock()
	defer blobMu.Unlock()
	if f, ok := blobMemCache[blobSize]; ok {
		return f
	}
	f := buildBlobFixture(graphene.NewInMemory(), blobNodeCount, blobSize)
	blobMemCache[blobSize] = f
	return f
}

func blobDiskGraph(blobSize int) *benchFixture {
	blobMu.Lock()
	defer blobMu.Unlock()
	if f, ok := blobDskCache[blobSize]; ok {
		return f
	}
	dir, err := os.MkdirTemp("", "graphene-blob-bench-*")
	if err != nil {
		panic(err)
	}
	blobDskDirs = append(blobDskDirs, dir)
	g, err := graphene.Open(dir)
	if err != nil {
		panic(err)
	}
	f := buildBlobFixture(g, blobNodeCount, blobSize)
	// Compact so reads exercise the CSR path, which is where blobs are copied.
	if err := g.Compact(); err != nil {
		panic(err)
	}
	blobDskCache[blobSize] = f
	return f
}

// -----------------------------------------------------------------------------
// Point lookup — one node blob per operation
// -----------------------------------------------------------------------------

func benchmarkBlobPointLookup(b *testing.B, f *benchFixture) {
	id := f.midNode
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := f.g.GraphStore.GetNode(id)
		if err != nil {
			b.Fatal(err)
		}
		if len(n.Properties) == 0 {
			b.Fatal("blob fixture produced an empty property blob")
		}
	}
}

func BenchmarkBlobPointLookupNode_Memory(b *testing.B) {
	for _, sz := range blobSizes {
		b.Run(fmt.Sprintf("blob=%d", sz), func(b *testing.B) {
			benchmarkBlobPointLookup(b, blobMemGraph(sz))
		})
	}
}

func BenchmarkBlobPointLookupNode_Disk(b *testing.B) {
	for _, sz := range blobSizes {
		b.Run(fmt.Sprintf("blob=%d", sz), func(b *testing.B) {
			benchmarkBlobPointLookup(b, blobDiskGraph(sz))
		})
	}
}

// -----------------------------------------------------------------------------
// Neighbours — blobFanout edge blobs per operation.
// -----------------------------------------------------------------------------

func benchmarkBlobEdgesOf(b *testing.B, f *benchFixture) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es, err := f.g.GraphStore.EdgesOf(f.midNode, store.DirectionOutbound, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(es) == 0 || len(es[0].Properties) == 0 {
			b.Fatal("blob fixture produced no edge blobs")
		}
	}
}

func BenchmarkBlobEdgesOf_Memory(b *testing.B) {
	for _, sz := range blobSizes {
		b.Run(fmt.Sprintf("blob=%d", sz), func(b *testing.B) {
			benchmarkBlobEdgesOf(b, blobMemGraph(sz))
		})
	}
}

func BenchmarkBlobEdgesOf_Disk(b *testing.B) {
	for _, sz := range blobSizes {
		b.Run(fmt.Sprintf("blob=%d", sz), func(b *testing.B) {
			benchmarkBlobEdgesOf(b, blobDiskGraph(sz))
		})
	}
}

// -----------------------------------------------------------------------------
// Bulk read — many node blobs per operation
// -----------------------------------------------------------------------------

func benchmarkBlobBulkRead(b *testing.B, f *benchFixture, n int) {
	ids := f.ids[:n]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		found, _, err := f.g.GetNodes(ids)
		if err != nil {
			b.Fatal(err)
		}
		if len(found) != n {
			b.Fatalf("got %d nodes, want %d", len(found), n)
		}
	}
}

func BenchmarkBlobBulkRead_GetNodes_Memory(b *testing.B) {
	for _, sz := range blobSizes {
		b.Run(fmt.Sprintf("blob=%d", sz), func(b *testing.B) {
			benchmarkBlobBulkRead(b, blobMemGraph(sz), 10_000)
		})
	}
}

func BenchmarkBlobBulkRead_GetNodes_Disk(b *testing.B) {
	for _, sz := range blobSizes {
		b.Run(fmt.Sprintf("blob=%d", sz), func(b *testing.B) {
			benchmarkBlobBulkRead(b, blobDiskGraph(sz), 10_000)
		})
	}
}
