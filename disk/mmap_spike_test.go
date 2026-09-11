//go:build stress

package disk

// The mmap measurement spike: what mapping the CSR would actually be worth.
//
//	go test ./disk/ -tags=stress -run TestMmapSpike -v
//
// # Why this is a measurement and not a design
//
// Mapping the image instead of reading it into the heap is the obvious answer
// to "the store's resident memory is the size of its graph". It is obvious
// enough that it is worth being careful about, because this repository has
// already rejected the design it requires once — TECHNICAL_DETAILS §14.3 turned
// down the offset-table plus decode-on-access layout — and v7 deleted the flat
// adjacency arrays that were the mmap-friendly part of the format as dead
// bytes. Rebuilding both on the strength of "mmap is faster" would be
// reasoning from the name of a technique.
//
// So: measure the prize before committing to the design, and let the number
// decide.
//
// # What can and cannot be mapped
//
// A mapping displaces heap only where the bytes on disk are the bytes in
// memory: contiguous, pointer-free, and usable without decoding. Reading the
// CSRGraph against that rule splits it in three.
//
//	mappable    outOffset, inOffset, outEdges, inEdges — flat integer arrays,
//	            already contiguous, already exactly their on-disk form
//
//	decode-only nodes[] and edges[] — the record arrays. Each record holds two
//	            Go slice headers (Labels, Properties) pointing at separate heap
//	            allocations, so the array is not a byte image of anything.
//	            Mapping it is the §14.3 design: an offset table plus a decode on
//	            every access.
//
//	never       nodesByLabel, edgesByLabel — Go maps, derived at load time and
//	            not in the file at all.
//
// The two prizes are therefore very different sizes and very different amounts
// of work, which is the whole reason to measure them apart.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/aoiflux/graphene/store"
)

// csrFootprint is the CSR's heap broken down by what a mapping could reach.
type csrFootprint struct {
	mappable   uint64 // flat adjacency and offset arrays
	recordHdrs uint64 // the nodes[]/edges[] backing arrays themselves
	recordPay  uint64 // the Labels and Properties each record points at
	postings   uint64 // nodesByLabel / edgesByLabel
}

func (f csrFootprint) total() uint64 {
	return f.mappable + f.recordHdrs + f.recordPay + f.postings
}

func measureCSR(c *CSRGraph) csrFootprint {
	var f csrFootprint
	if c == nil {
		return f
	}

	f.mappable = uint64(len(c.outOffset))*8 + uint64(len(c.inOffset))*8 +
		uint64(len(c.outEdges))*uint64(unsafe.Sizeof(store.EdgeID(0))) +
		uint64(len(c.inEdges))*uint64(unsafe.Sizeof(store.EdgeID(0)))

	// recordHdrs counts the slots the record arenas materialise, which since the
	// page table is the live pages times their size rather than one per
	// identifier ever issued. The spike's arithmetic is unchanged; what it
	// measures got smaller.
	f.recordHdrs = uint64(len(c.nodeRecs))*uint64(unsafe.Sizeof(nodeRecord{})) +
		uint64(len(c.edgeRecs))*uint64(unsafe.Sizeof(rawEdge{}))

	for i := range c.nodeRecs {
		f.recordPay += uint64(len(c.nodeRecs[i].Labels)) * uint64(unsafe.Sizeof(store.NodeType(0)))
		f.recordPay += uint64(len(c.nodeRecs[i].Properties))
	}
	for i := range c.edgeRecs {
		f.recordPay += uint64(len(c.edgeRecs[i].Labels)) * uint64(unsafe.Sizeof(store.EdgeType(0)))
		f.recordPay += uint64(len(c.edgeRecs[i].Properties))
	}

	for _, ids := range c.nodesByLabel {
		f.postings += uint64(len(ids)) * uint64(unsafe.Sizeof(store.NodeID(0)))
	}
	for _, ids := range c.edgesByLabel {
		f.postings += uint64(len(ids)) * uint64(unsafe.Sizeof(store.EdgeID(0)))
	}
	return f
}

func liveHeapD(keepAlive any) uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(keepAlive)
	return ms.HeapAlloc
}

// TestMmapSpike reports the prize. It asserts nothing about the numbers — the
// point is the numbers — but it does fail if the store it measures is wrong,
// so a broken fixture cannot be mistaken for a result.
func TestMmapSpike(t *testing.T) {
	for _, size := range []struct {
		nodes, edges int
		propBytes    int
	}{
		{50_000, 150_000, 0},   // adjacency-heavy, no payload: the best case for mapping
		{50_000, 150_000, 64},  // a small property blob per record
		{50_000, 150_000, 512}, // a realistic forensic blob
	} {
		name := fmt.Sprintf("nodes=%d/edges=%d/prop=%dB", size.nodes, size.edges, size.propBytes)
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			build(t, dir, size.nodes, size.edges, size.propBytes)

			base := liveHeapD(nil)
			start := time.Now()
			s, err := Open(dir)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			openTook := time.Since(start)
			total := liveHeapD(s)

			c := s.cur().csr
			if c == nil {
				t.Fatal("fixture has no image — the compaction did not happen")
			}
			if got := c.NodeCount(); got != size.nodes {
				t.Fatalf("fixture holds %d nodes, wanted %d", got, size.nodes)
			}

			f := measureCSR(c)
			heap := total - base

			t.Logf("open %v, heap %s, image on disk %s",
				openTook.Round(time.Millisecond), mib(heap), mib(uint64(imageSize(t, dir))))
			t.Logf("  mappable  (flat arrays)     %10s  %5.1f%% of heap", mib(f.mappable), pct(f.mappable, heap))
			t.Logf("  decode    (record arrays)   %10s  %5.1f%%", mib(f.recordHdrs), pct(f.recordHdrs, heap))
			t.Logf("  decode    (record payloads) %10s  %5.1f%%", mib(f.recordPay), pct(f.recordPay, heap))
			t.Logf("  never     (label postings)  %10s  %5.1f%%", mib(f.postings), pct(f.postings, heap))
			t.Logf("  unattributed                %10s  %5.1f%%", mib(heap-min64(heap, f.total())), pct(heap-min64(heap, f.total()), heap))
			t.Logf("  PRIZE  map flat arrays only:      %5.1f%% of heap", pct(f.mappable, heap))
			t.Logf("  PRIZE  plus the §14.3 rewrite:    %5.1f%% of heap",
				pct(f.mappable+f.recordHdrs+f.recordPay, heap))

			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func build(t *testing.T, dir string, nodes, edges, propBytes int) {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	blob := make([]byte, propBytes)
	for i := range blob {
		blob[i] = byte(i)
	}

	ids := make([]store.NodeID, 0, nodes)
	for i := 0; i < nodes; i++ {
		n := &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
		if propBytes > 0 {
			n.Properties = blob
		}
		id, err := s.AddNode(n)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids = append(ids, id)
	}
	for i := 0; i < edges; i++ {
		e := &store.Edge{
			Src:    ids[i%len(ids)],
			Dst:    ids[(i*7+1)%len(ids)],
			Labels: []store.EdgeType{store.EdgeTypeContains},
		}
		if propBytes > 0 {
			e.Properties = blob
		}
		if _, err := s.AddEdge(e); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func imageSize(t *testing.T, dir string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(dir, csrFileName))
	if err != nil {
		t.Fatalf("stat image: %v", err)
	}
	return fi.Size()
}

func mib(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

func pct(part, whole uint64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) / float64(whole) * 100
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
