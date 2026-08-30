//go:build stress

package disk

// The v9 spike: two candidate format changes, measured against each other.
//
//	go test ./disk/ -tags=stress -run TestV9Spike -v
//
// # Why there are two options and not one
//
// TestArenaSpike costed one idea — packing each record's two slice headers into
// shared byte arenas — and returned a split verdict: no-go on resident memory,
// go on GC latency. Building v9 on that alone would answer the question the
// spike happened to ask rather than the question the read path actually poses.
//
// Reading the hot path first changes what is worth measuring. To learn one
// neighbour, reader.incidentEdges does three cache-hostile things per incident
// edge:
//
//	r.deltaEdge(eid)     a map probe, paid even when the delta is empty
//	g.edges[eid]         a random access into an 80-byte-stride array
//	rec.Labels           a pointer chase into a separately allocated slice
//
// The adjacency arrays hold EdgeID, so the far endpoint — the one field a hop
// actually needs — is the one field not present where the walk is looking. That
// is a layout problem, and the arena does not touch it.
//
// So two options, measured on the same fixture. Both were framed here as durable
// format changes; neither turned out to be one. The image already stores records
// as one packed byte stream, and adjacency has not been serialised since v7 —
// Build recomputes the neighbour arrays on every load — so both are in-memory
// representation changes on the load path, and there is no v9. See
// TECHNICAL_DETAILS.md §14.13.
//
// The two options:
//
//	A  record arena       labels and properties into two byte arenas; records
//	                      become (off, len) pairs and the arrays pointer-free
//	B  denormalised adj   the far endpoint stored inline beside each EdgeID, so
//	                      an unfiltered hop never touches the edge record at all
//
// B is not free: it adds 8 bytes per edge per direction, which is resident
// memory spent to buy speed. Under the standing priority order — speed first,
// resident second, allocations third — that is a trade the ordering permits and
// the measurement has to price. A and B also compose, and the combination is
// measured as its own arm rather than assumed to be the sum.
//
// # What each number is for
//
//	resident        P2. Three blob sizes, because a fixed per-record saving
//	                shrinks as a share and reporting only the empty case is how
//	                the mmap prize first looked good.
//	GC, idle        P1 as TestArenaSpike measured it: forced cycles, nothing else
//	                running.
//	GC, loaded      RQ6. The idle figure understates the prize for a process that
//	                is allocating — which is what an algorithm layer does. The
//	                mutator here allocates float64 arrays the way PageRank would.
//	hop throughput  P1, and the reason B exists. Unfiltered and filtered, because
//	                a filter still needs the record and B cannot help there.
//	intersection    RQ3. Sorted spans turn a hash build into a linear merge, which
//	                is the whole similarity and link-prediction category.
//	build cost      RQ3's other half. Sorting is paid once per compaction.

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sort"
	"testing"
	"time"

	"github.com/aoiflux/graphene/store"
)

const (
	spikeNodes = 100_000
	spikeEdges = 200_000
)

// --- option B: adjacency with the far endpoint denormalised -----------------

// denormCSR is the adjacency half of a v9 image under option B. The offset
// arrays and the EdgeID arrays are unchanged; nbr is new, and holds the node at
// the far end of the edge at the same position.
//
// It is a parallel array rather than a struct-of-two-fields so that a walk that
// wants only neighbours reads one contiguous run of 8-byte words. Interleaving
// them would halve the useful bytes per cache line on exactly the walk this
// exists to speed up.
type denormCSR struct {
	nodes []nodeRecord
	edges []rawEdge

	outOffset []uint64
	outEdges  []store.EdgeID
	outNbr    []store.NodeID

	inOffset []uint64
	inEdges  []store.EdgeID
	inNbr    []store.NodeID
}

// denormalise derives option B's arrays from a built CSR. sorted additionally
// orders each span by the neighbour's ID, which is what makes an intersection a
// merge instead of a hash build.
func denormalise(g *CSRGraph, sorted bool) *denormCSR {
	d := &denormCSR{
		nodes:     g.nodes,
		edges:     g.edges,
		outOffset: g.outOffset,
		inOffset:  g.inOffset,
		outEdges:  g.outEdges,
		inEdges:   g.inEdges,
		outNbr:    make([]store.NodeID, len(g.outEdges)),
		inNbr:     make([]store.NodeID, len(g.inEdges)),
	}
	for i, eid := range d.outEdges {
		d.outNbr[i] = g.edges[eid].Dst
	}
	for i, eid := range d.inEdges {
		d.inNbr[i] = g.edges[eid].Src
	}
	if sorted {
		sortSpans(d.outOffset, d.outEdges, d.outNbr)
		sortSpans(d.inOffset, d.inEdges, d.inNbr)
	}
	return d
}

// sortSpans orders each node's span by neighbour ID, carrying the EdgeID along
// so the two arrays stay positionally paired.
func sortSpans(offset []uint64, eids []store.EdgeID, nbr []store.NodeID) {
	for id := 0; id+1 < len(offset); id++ {
		lo, hi := offset[id], offset[id+1]
		if hi-lo < 2 {
			continue
		}
		s := spanSorter{eids: eids[lo:hi], nbr: nbr[lo:hi]}
		sort.Sort(s)
	}
}

type spanSorter struct {
	eids []store.EdgeID
	nbr  []store.NodeID
}

func (s spanSorter) Len() int           { return len(s.nbr) }
func (s spanSorter) Less(i, j int) bool { return s.nbr[i] < s.nbr[j] }
func (s spanSorter) Swap(i, j int) {
	s.nbr[i], s.nbr[j] = s.nbr[j], s.nbr[i]
	s.eids[i], s.eids[j] = s.eids[j], s.eids[i]
}

// --- fixture ---------------------------------------------------------------

// buildSpikeFixture produces records for a graph with shared neighbourhoods, so
// that an intersection has something to find. Deterministic: the same seed
// yields the same graph on every arm and every run.
func buildSpikeFixture(n, m, blob int) ([]nodeRecord, []rawEdge) {
	rng := rand.New(rand.NewSource(20260830))

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

	// A locality-biased random graph: most edges land inside a window, which
	// gives overlapping neighbourhoods without degenerating into a lattice.
	const window = 512
	edges := make([]rawEdge, m+1)
	for i := 1; i <= m; i++ {
		src := 1 + rng.Intn(n)
		dst := 1 + ((src - 1 + 1 + rng.Intn(window)) % n)
		lbl := store.EdgeTypeContains
		if i%3 == 0 {
			lbl = store.EdgeTypeSimilarTo
		}
		edges[i] = rawEdge{
			ID:     store.EdgeID(i),
			Src:    store.NodeID(src),
			Dst:    store.NodeID(dst),
			Labels: []store.EdgeType{lbl},
			Weight: 1,
		}
		if blob > 0 {
			edges[i].Properties = make([]byte, blob)
		}
	}
	return nodes, edges
}

// --- hop throughput --------------------------------------------------------

// hopCurrent walks every node's outbound span the way reader.incidentEdges does
// today: resolve the edge record, then read the far endpoint off it.
func hopCurrent(g *CSRGraph, filter bool) uint64 {
	var sum uint64
	for id := 1; id+1 < len(g.outOffset); id++ {
		lo, hi := g.outOffset[id], g.outOffset[id+1]
		for i := lo; i < hi; i++ {
			eid := g.outEdges[i]
			rec := g.edges[eid]
			if filter && !rawEdgeMatchesFilter(spikeFilter, rec.Labels) {
				continue
			}
			sum += uint64(rec.Dst)
		}
	}
	return sum
}

// hopDenorm is the same walk against option B. With no filter it never touches
// the edge record, which is the entire claim.
func hopDenorm(d *denormCSR, filter bool) uint64 {
	var sum uint64
	for id := 1; id+1 < len(d.outOffset); id++ {
		lo, hi := d.outOffset[id], d.outOffset[id+1]
		for i := lo; i < hi; i++ {
			if filter {
				rec := d.edges[d.outEdges[i]]
				if !rawEdgeMatchesFilter(spikeFilter, rec.Labels) {
					continue
				}
			}
			sum += uint64(d.outNbr[i])
		}
	}
	return sum
}

var spikeFilter = []store.EdgeType{store.EdgeTypeSimilarTo}

// --- intersection ----------------------------------------------------------

// intersectHash is what a triangle count or a Jaccard similarity has to do
// today: build a set from one neighbourhood and probe it with the other.
func intersectHash(d *denormCSR, a, b store.NodeID) int {
	al, ah := d.outOffset[a], d.outOffset[a+1]
	bl, bh := d.outOffset[b], d.outOffset[b+1]
	if ah-al > bh-bl {
		a, b = b, a
		al, ah, bl, bh = bl, bh, al, ah
	}
	set := make(map[store.NodeID]struct{}, ah-al)
	for i := al; i < ah; i++ {
		set[d.outNbr[i]] = struct{}{}
	}
	n := 0
	for i := bl; i < bh; i++ {
		if _, ok := set[d.outNbr[i]]; ok {
			n++
		}
	}
	return n
}

// intersectMerge is the same answer over sorted spans: one linear pass, no
// allocation, no hashing.
func intersectMerge(d *denormCSR, a, b store.NodeID) int {
	i, iEnd := d.outOffset[a], d.outOffset[a+1]
	j, jEnd := d.outOffset[b], d.outOffset[b+1]
	n := 0
	for i < iEnd && j < jEnd {
		switch x, y := d.outNbr[i], d.outNbr[j]; {
		case x == y:
			n++
			i++
			j++
		case x < y:
			i++
		default:
			j++
		}
	}
	return n
}

// v9Arm holds one candidate layout's whole image, so that two arms differ only
// in the representation under test and not in what they happen to keep alive.
type v9Arm struct {
	nodes []nodeRecord
	edges []rawEdge
	arena *arenaImage

	outOffset, inOffset []uint64
	outEdges, inEdges   []store.EdgeID
	outNbr, inNbr       []store.NodeID

	nodesByLabel map[store.NodeType][]store.NodeID
	edgesByLabel map[store.EdgeType][]store.EdgeID
}

// --- GC instruments --------------------------------------------------------

func v9LiveHeap(keepAlive any) uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(keepAlive)
	return ms.HeapAlloc
}

// gcIdle is TestArenaSpike's instrument: the mean wall time of a forced cycle
// with v live and nothing else running.
func gcIdle(v any) time.Duration {
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

// gcLoaded is RQ6's instrument, and it is the one that matches an algorithm
// workload. Rather than forcing cycles on an idle heap it runs a mutator that
// allocates float64 arrays the way PageRank does, lets the collector schedule
// itself, and reports the mutator's wall time and the total pause it absorbed.
//
// The distinction matters because a pointer-free image is not merely cheaper to
// scan — it drops out of the scan entirely, so its cost is avoided on every
// cycle the mutator triggers rather than on the handful a benchmark forces.
func gcLoaded(v any, n int) (wall time.Duration, cycles int) {
	// Two earlier versions of this were wrong in opposite directions. The first
	// allocated too little and triggered 0-2 collections. The second allocated
	// enough, but left GOGC alone — and at a fixed GOGC a *smaller* live heap
	// triggers proportionally *more* cycles, so the arm with the cheaper scan
	// paid for more scans and the wall times converged. That feedback loop is
	// real, and it is why the per-cycle figure alone overstates the prize.
	//
	// To isolate the layout, the collector's own policy is taken out: GC off,
	// and a fixed number of forced cycles for every arm. Equal cycles, equal
	// mutator work, so the difference in wall time is the difference in what the
	// collector had to walk.
	const (
		rounds  = 480
		gcEvery = 12
	)

	prev := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(prev)
	runtime.GC()

	start := time.Now()
	var sink float64
	for round := 0; round < rounds; round++ {
		ranks := make([]float64, n)
		next := make([]float64, n)
		for i := range ranks {
			ranks[i] = 1 / float64(n)
		}
		for i := range next {
			next[i] = 0.15/float64(n) + 0.85*ranks[i]
		}
		sink += next[n-1]
		if round%gcEvery == gcEvery-1 {
			runtime.GC()
			cycles++
		}
	}
	wall = time.Since(start)

	runtime.KeepAlive(v)
	runtime.KeepAlive(sink)
	return wall, cycles
}

// --- the spikes ------------------------------------------------------------

// TestV9SpikeFootprint prices both options on resident bytes and on GC, the
// second measured both idle and under an allocating mutator.
func TestV9SpikeFootprint(t *testing.T) {
	t.Logf("%-6s  %-16s  %12s  %8s  %10s  %11s  %7s",
		"blob", "layout", "resident B", "vs cur", "GC/cycle", "40-cycle run", "cycles")

	for _, blob := range []int{0, 64, 512} {
		var baseline uint64

		for _, arm := range []string{"current", "arena", "denorm", "arena+denorm"} {
			runtime.GC()
			base := v9LiveHeap(nil)

			var (
				resident uint64
				idle     time.Duration
				wall     time.Duration
				cycles   int
			)

			func() {
				nodes, edges := buildSpikeFixture(spikeNodes, spikeEdges, blob)
				g := Build(nodes, edges)
				nodes, edges = nil, nil

				// Every arm retains the same *set* of structures: records,
				// adjacency, offsets and the label postings. An earlier version
				// let the non-current arms drop g entirely, which silently freed
				// the two label-posting maps along with it — roughly 2.5 MB that
				// showed up as a saving the layout had not made. Comparing two
				// heaps means holding everything but the thing under test equal.
				img := &v9Arm{
					nodesByLabel: g.nodesByLabel,
					edgesByLabel: g.edgesByLabel,
					outOffset:    g.outOffset,
					inOffset:     g.inOffset,
					outEdges:     g.outEdges,
					inEdges:      g.inEdges,
				}

				switch arm {
				case "current":
					img.nodes, img.edges = g.nodes, g.edges
				case "arena":
					img.arena = packArena(g.nodes, g.edges)
				case "denorm":
					img.nodes, img.edges = g.nodes, g.edges
					d := denormalise(g, true)
					img.outNbr, img.inNbr = d.outNbr, d.inNbr
				case "arena+denorm":
					d := denormalise(g, true)
					img.outNbr, img.inNbr = d.outNbr, d.inNbr
					img.arena = packArena(g.nodes, g.edges)
				}

				g = nil
				runtime.GC()
				resident = v9LiveHeap(img) - base
				idle = gcIdle(img)
				wall, cycles = gcLoaded(img, spikeNodes)
			}()

			delta := "—"
			if arm == "current" {
				baseline = resident
			} else if baseline > 0 {
				delta = v9pct(float64(resident)/float64(baseline) - 1)
			}

			t.Logf("%-6d  %-16s  %12d  %8s  %10s  %11s  %7d",
				blob, arm, resident, delta,
				idle.Round(time.Microsecond),
				wall.Round(time.Millisecond),
				cycles)
		}
	}
}

// TestV9SpikeHops prices option B's actual claim: the cost of learning every
// neighbour in the graph, with and without a label filter.
func TestV9SpikeHops(t *testing.T) {
	nodes, edges := buildSpikeFixture(spikeNodes, spikeEdges, 64)
	g := Build(nodes, edges)
	d := denormalise(g, false)

	// Interleaved, alternating arms, minima reported — the same discipline the
	// A/B harness uses, because two representations in one process are still two
	// arms on one loaded host.
	const rounds = 7
	var curPlain, dnPlain, curFilt, dnFilt []time.Duration

	for r := 0; r < rounds; r++ {
		curPlain = append(curPlain, timePer(func() { sinkU += hopCurrent(g, false) }))
		dnPlain = append(dnPlain, timePer(func() { sinkU += hopDenorm(d, false) }))
		curFilt = append(curFilt, timePer(func() { sinkU += hopCurrent(g, true) }))
		dnFilt = append(dnFilt, timePer(func() { sinkU += hopDenorm(d, true) }))
	}

	perEdge := func(d time.Duration) string {
		return fmt.Sprintf("%.2f ns/edge", float64(d.Nanoseconds())/float64(len(g.outEdges)))
	}
	t.Logf("%-16s  %14s  %14s  %10s", "walk", "current", "denorm", "change")
	t.Logf("%-16s  %14s  %14s  %10s", "no filter",
		perEdge(minOf(curPlain)), perEdge(minOf(dnPlain)), pctOf(minOf(curPlain), minOf(dnPlain)))
	t.Logf("%-16s  %14s  %14s  %10s", "label filter",
		perEdge(minOf(curFilt)), perEdge(minOf(dnFilt)), pctOf(minOf(curFilt), minOf(dnFilt)))
	t.Logf("%d interleaved rounds, minima reported, %d edges per walk", rounds, len(g.outEdges))
}

// TestV9SpikeIntersection prices RQ3: what sorting each span buys the whole
// similarity and link-prediction category, and what it costs at build.
func TestV9SpikeIntersection(t *testing.T) {
	nodes, edges := buildSpikeFixture(spikeNodes, spikeEdges, 64)

	buildPlain := timeIt(func() { sinkP = Build(nodes, edges) })
	g := Build(nodes, edges)

	unsorted := denormalise(g, false)
	sortOnly := timeIt(func() {
		sortSpans(unsorted.outOffset, unsorted.outEdges, unsorted.outNbr)
		sortSpans(unsorted.inOffset, unsorted.inEdges, unsorted.inNbr)
	})
	sorted := unsorted

	// A triangle-count-shaped workload: intersect the endpoints of many edges.
	const pairs = 20_000
	work := func(fn func(*denormCSR, store.NodeID, store.NodeID) int) func() {
		return func() {
			n := 0
			for i := 1; i <= pairs; i++ {
				e := g.edges[store.EdgeID(i)]
				n += fn(sorted, e.Src, e.Dst)
			}
			sinkI += n
		}
	}

	const rounds = 7
	var hashT, mergeT []time.Duration
	for r := 0; r < rounds; r++ {
		hashT = append(hashT, timePer(work(intersectHash)))
		mergeT = append(mergeT, timePer(work(intersectMerge)))
	}

	t.Logf("build, unsorted spans      %12s", buildPlain.Round(time.Millisecond))
	t.Logf("sorting both directions    %12s  (%.1f%% of a build)",
		sortOnly.Round(time.Millisecond),
		float64(sortOnly)/float64(buildPlain)*100)
	t.Logf("")
	perPair := func(d time.Duration) string {
		return fmt.Sprintf("%.0f ns/pair", float64(d.Nanoseconds())/float64(pairs))
	}
	t.Logf("%-20s  %14s  %14s  %10s", "intersection", "hash set", "sorted merge", "change")
	t.Logf("%-20s  %14s  %14s  %10s", "20k endpoint pairs",
		perPair(minOf(hashT)), perPair(minOf(mergeT)), pctOf(minOf(hashT), minOf(mergeT)))
}

// --- helpers ---------------------------------------------------------------

var (
	sinkU uint64
	sinkI int
	sinkP *CSRGraph
)

// timeIt is a single-shot timer, kept only for the build-cost measurement where
// one sample is already tens of milliseconds.
func timeIt(fn func()) time.Duration {
	start := time.Now()
	fn()
	return time.Since(start)
}

// timePer repeats fn until the timed region clears minRun and returns the mean
// cost of one call.
//
// The first version of this spike timed one call and reported the result. On
// this host that produced 0s for the two fast arms and exactly 5.000ms for two
// arms that share no code — the clock's granularity, printed as if it were a
// measurement. A sub-tick sample is not a fast result; it is no result, and the
// -100% it renders as is the most flattering possible way to be wrong.
func timePer(fn func()) time.Duration {
	const minRun = 300 * time.Millisecond
	n := 1
	for {
		start := time.Now()
		for i := 0; i < n; i++ {
			fn()
		}
		d := time.Since(start)
		if d >= minRun {
			return d / time.Duration(n)
		}
		if d <= 0 {
			n *= 64
			continue
		}
		// Scale to land just past minRun, with headroom for super-linearity.
		next := int(float64(n) * (float64(minRun) / float64(d)) * 1.3)
		if next <= n {
			next = n * 2
		}
		n = next
	}
}

func minOf(ds []time.Duration) time.Duration {
	m := ds[0]
	for _, d := range ds[1:] {
		if d < m {
			m = d
		}
	}
	return m
}

func pctOf(from, to time.Duration) string {
	return v9pct(float64(to)/float64(from) - 1)
}

func v9pct(f float64) string {
	return fmt.Sprintf("%+.1f%%", f*100)
}
