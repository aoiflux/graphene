//go:build stress

package disk

// The CSR record arena spike: what packing the record arrays would be worth.
//
//	go test ./disk/ -tags=stress -run TestArenaSpike -v
//
// # Superseded on two counts — read TECHNICAL_DETAILS.md §14.13 first
//
// 1. Its GC columns were measured with the collector's trigger left on: the loop
//    below calls runtime.GC() but never debug.SetGCPercent(-1), so at a fixed
//    GOGC a smaller live heap triggers proportionally more cycles and the figure
//    mixes trigger rate with the scan cost it means to isolate. The rising
//    baseline it reports (1.34 / 2.29 / 5.77 ms) does not reproduce against the
//    shipped loader, which is flat. tests/gc_bench_test.go disables the trigger.
//
// 2. This is not a durable format change, contrary to the paragraph below. The
//    image already stores records as one packed byte stream and adjacency has
//    not been serialised since v7, so what shipped is an in-memory change on the
//    load path: no v9, no new section, no parser, no fuzz target owed.
//
// What shipped also keeps []NodeType and []byte on every record — it packs them
// into shared backing arrays via three-index slicing — so the arrays are still
// pointer-bearing and the pointer-free mechanism argued for below was never
// built. The measured win is the allocation-count half.
//
// # Why this is a measurement and not a design
//
// The Phase 2 mmap spike found this and recorded it as the better lead: with no
// property blobs at all, 14.1 MiB of a 19.4 MiB heap was the record arrays, and
// 48 of every 56-80 bytes per record were two slice headers pointing at two
// bytes of labels. Replacing the slices with (off, len uint32) pairs into two
// arenas would reclaim most of that without mapping anything.
//
// It is also a durable format change — v9, two new sections, an offset table,
// a new parser that owes a fuzz target — so it is exactly the kind of item that
// must be costed before it is built rather than after. The mmap spike is the
// precedent for why: its prize looked like 15.8% and became 2.6% as soon as the
// fixture carried realistic property blobs, and reporting only the flattering
// case is how that nearly went the other way.
//
// # The two claims, and why only one of them justifies a format break
//
// P2, resident memory. The header overhead is fixed per record, so a 100 000-node
// image carries roughly 4.8 MB of headers whatever the payload — but the *share*
// it represents collapses as blobs grow. Hence three blob sizes below, not one.
//
// P1, speed. Each record holds two pointers, so the node and edge arrays are
// pointer-bearing allocations the GC scans on every cycle, live for the whole
// life of the store. Arena records are pointer-free, so the collector stops
// scanning them entirely. That is latency on the first-ranked axis, and it is
// the claim that would justify the format break on its own. If it does not show
// up, the honest answer is to say so and stop.

import (
	"runtime"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

// arenaNode is nodeRecord with its two slice headers replaced by offsets into
// shared arenas. 8 + 4*4 = 24 bytes against nodeRecord's 56, and — the point —
// no pointers.
type arenaNode struct {
	ID                               store.NodeID
	labOff, labLen, propOff, propLen uint32
}

// arenaEdge is rawEdge under the same treatment: 48 bytes against 80.
type arenaEdge struct {
	ID, Src, Dst                     uint64
	Weight                           float32
	labOff, labLen, propOff, propLen uint32
}

// arenaImage is what the record half of a v9 CSRGraph would hold.
type arenaImage struct {
	nodes  []arenaNode
	edges  []arenaEdge
	labels []byte
	props  []byte
}

// buildSpikeRecords produces the current representation for n nodes and m edges
// carrying blob-byte property payloads.
func buildSpikeRecords(n, m, blob int) ([]nodeRecord, []rawEdge) {
	nodes := make([]nodeRecord, n+1)
	for i := 1; i <= n; i++ {
		nodes[i] = nodeRecord{
			ID:     store.NodeID(i),
			Labels: []store.NodeType{store.NodeTypeMicroArtefact},
		}
		if blob > 0 {
			nodes[i].Properties = make([]byte, blob)
		}
	}
	edges := make([]rawEdge, m+1)
	for i := 1; i <= m; i++ {
		edges[i] = rawEdge{
			ID:     store.EdgeID(i),
			Src:    store.NodeID(1 + (i-1)%n),
			Dst:    store.NodeID(1 + i%n),
			Labels: []store.EdgeType{store.EdgeTypeContains},
			Weight: 1,
		}
		if blob > 0 {
			edges[i].Properties = make([]byte, blob)
		}
	}
	return nodes, edges
}

// packArena converts the same records into the arena representation.
func packArena(nodes []nodeRecord, edges []rawEdge) *arenaImage {
	a := &arenaImage{
		nodes: make([]arenaNode, len(nodes)),
		edges: make([]arenaEdge, len(edges)),
	}
	for i, n := range nodes {
		rec := arenaNode{ID: n.ID}
		rec.labOff, rec.labLen = uint32(len(a.labels)), uint32(len(n.Labels)*2)
		for _, l := range n.Labels {
			a.labels = append(a.labels, byte(l), byte(uint16(l)>>8))
		}
		rec.propOff, rec.propLen = uint32(len(a.props)), uint32(len(n.Properties))
		a.props = append(a.props, n.Properties...)
		a.nodes[i] = rec
	}
	for i, e := range edges {
		rec := arenaEdge{ID: uint64(e.ID), Src: uint64(e.Src), Dst: uint64(e.Dst), Weight: e.Weight}
		rec.labOff, rec.labLen = uint32(len(a.labels)), uint32(len(e.Labels)*2)
		for _, l := range e.Labels {
			a.labels = append(a.labels, byte(l), byte(uint16(l)>>8))
		}
		rec.propOff, rec.propLen = uint32(len(a.props)), uint32(len(e.Properties))
		a.props = append(a.props, e.Properties...)
		a.edges[i] = rec
	}
	return a
}

// spikeLiveHeap is graphene_footprint_test.go's liveHeap, repeated here because
// that helper lives in the tests package.
func spikeLiveHeap(keepAlive any) uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(keepAlive)
	return ms.HeapAlloc
}

// gcCostOf reports the mean wall time of a forced GC cycle with v live. This is
// the P1 measurement: a pointer-free array is not scanned, so if the arena's
// claim is real it shows up here and nowhere else.
func gcCostOf(v any) time.Duration {
	const cycles = 12
	runtime.GC()
	start := time.Now()
	for i := 0; i < cycles; i++ {
		runtime.GC()
	}
	d := time.Since(start)
	runtime.KeepAlive(v)
	return d / cycles
}

func TestArenaSpike(t *testing.T) {
	const (
		n = 100_000
		m = 200_000
	)

	t.Logf("%-8s  %12s  %12s  %10s  %9s  %9s  %10s  %10s",
		"blob", "current B", "arena B", "saved", "B/node", "B/edge", "GC now", "GC arena")

	for _, blob := range []int{0, 64, 512} {
		var (
			curBytes, arenaBytes uint64
			curGC, arenaGC       time.Duration
		)

		func() {
			base := spikeLiveHeap(nil)
			nodes, edges := buildSpikeRecords(n, m, blob)
			curBytes = spikeLiveHeap(nodes) - base
			curGC = gcCostOf(&nodes)
			runtime.KeepAlive(edges)

			// Pack from the same records, then drop the originals so the arena
			// is measured on its own rather than on top of what it replaces.
			img := packArena(nodes, edges)
			nodes, edges = nil, nil
			runtime.GC()
			arenaBase := spikeLiveHeap(nil)
			img2 := packArena(buildSpikeRecords(n, m, blob))
			arenaBytes = spikeLiveHeap(img2) - arenaBase
			arenaGC = gcCostOf(img2)
			runtime.KeepAlive(img)
		}()

		saved := float64(0)
		if curBytes > 0 {
			saved = (1 - float64(arenaBytes)/float64(curBytes)) * 100
		}
		t.Logf("%-8d  %12d  %12d  %9.1f%%  %9.1f  %9.1f  %10s  %10s",
			blob, curBytes, arenaBytes, saved,
			float64(curBytes-arenaBytes)/float64(n),
			float64(curBytes-arenaBytes)/float64(m),
			curGC.Round(time.Microsecond), arenaGC.Round(time.Microsecond))
	}
}
