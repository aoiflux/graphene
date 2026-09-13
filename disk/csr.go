package disk

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/aoiflux/graphene/store"
)

// CSR (Compressed Sparse Row) is an adjacency representation optimised for
// read-heavy, bulk-ingest workloads. For each node, all outbound (and
// separately all inbound) edge indices are stored contiguously in a flat array.
//
// # Records live in pages, not in one array slot per identifier
//
// Identifiers are never reused (§15.1), so a store that has deleted heavily
// carries a highest ID far above its live count. Indexing one array slot per
// identifier made the image cost memory for every identifier ever issued — 72 B
// per burned node ID and 80 B per burned edge ID — recoverable only by deleting
// the *high* end of the space (docs/benchmarks.md, "The identifier high-water
// mark"). A rebuild workload burns the low end, and paid that forever.
//
// The layout is therefore a two-level page table. The identifier space is cut
// into pages of csrPageSlots identifiers; a directory maps each page number to
// an arena page, or to csrDeadPage when no record falls in it. Records sit in
// one arena, at
//
//	slot(id) = dir[id >> csrPageBits] << csrPageBits | id & csrPageMask
//
// so a lookup is one directory read and one dependent record read, and a page
// with nothing in it costs one int32. Arena pages are assigned in ascending page
// number, which makes arena order identifier order: every walk over the arena
// that skips the zero-valued dead slots visits live records in ascending ID
// order, which is the order the file is written in and the order the Merkle
// leaves are hashed in. Nothing about the pages reaches the file — they are a
// load-time construct, like the adjacency arrays.
//
// Adjacency is a plain CSR over the same slot space:
//
//	outOffset[slot]   → start index in outEdges
//	outOffset[slot+1] → exclusive end index in outEdges
//	outEdges[i]       → EdgeID at position i
//
// with a single sentinel at outOffset[len(nodeRecs)]. The arena is contiguous,
// so the offset after a page's last slot is the offset of the next arena page's
// first slot, and no per-page sentinel is needed. Same structure for inbound
// adjacency (inOffset / inEdges).

// The page table's geometry.
//
// 4096 identifiers per page puts a 1.5M-identifier band in a directory of 367
// entries, and makes a page holding a single live record cost
// 4096 × (56 + 16) B ≈ 288 KB — small enough that a hostile file is bounded by
// how many pages it touches (checkIDCeiling) and large enough that a directory
// over the whole addressable space is 4 MiB per kind (maxCSREntityID).
const (
	csrPageBits  = 12
	csrPageSlots = 1 << csrPageBits
	csrPageMask  = csrPageSlots - 1

	// csrDeadPage marks a directory entry with no arena page behind it.
	csrDeadPage = int32(-1)
)

// rawEdge is the compact on-disk/in-memory edge representation used during
// CSR construction.
type rawEdge struct {
	ID         store.EdgeID
	Src        store.NodeID
	Dst        store.NodeID
	Labels     []store.EdgeType // one or more labels; nil/empty = unknown
	Weight     float32
	Properties []byte
}

// CSRGraph holds the built adjacency arrays plus the node/edge metadata slices.
type CSRGraph struct {
	// Node records. nodeDir is indexed by page number (id >> csrPageBits) and
	// holds the arena page index, or csrDeadPage. nodeRecs is the arena: one
	// zero-initialised slot per identifier of every materialised page, so a
	// dead slot inside a live page reads as a record whose ID is InvalidNodeID.
	// nodePages is the inverse of nodeDir over live pages — arena page index →
	// page number — which is what turns a slot back into an identifier.
	// nodeLiveBefore[p] is the number of live records in the arena pages before
	// p, so the position of a record among the live ones (the index an
	// inclusion proof is built at) costs one page scan rather than a walk from
	// the start of the arena.
	nodeDir        []int32
	nodeRecs       []nodeRecord // len = live node pages × csrPageSlots
	nodePages      []uint32
	nodeLiveBefore []int
	liveNodes      int
	highestNodeID  store.NodeID // highest ID carrying a record; 0 when none

	// Edge records, the same shape.
	edgeDir        []int32
	edgeRecs       []rawEdge // len = live edge pages × csrPageSlots
	edgePages      []uint32
	edgeLiveBefore []int
	liveEdges      int
	highestEdgeID  store.EdgeID

	// Outbound adjacency, indexed by node slot. Derived state, like the label
	// postings below; nothing in the file carries it. Written once, by
	// buildAdjacency, at Open or on first use — see AdjacencyMode.
	outOffset []uint64 // len = len(nodeRecs) + 1
	outEdges  []store.EdgeID

	// Inbound adjacency.
	inOffset []uint64
	inEdges  []store.EdgeID

	// adjBuilt is whether the four arrays above have been filled, and adjMu
	// serialises the single build that fills them.
	//
	// Atomic rather than a plain bool because under AdjacencyLazy the build can
	// be triggered by any reader, and readers of this image are not serialised
	// with each other: the fast read path takes no store lock. The Store after
	// the build pairs with the Load before every use, which is what publishes
	// the four slice headers to a goroutine that did not do the building.
	//
	// sync.Once would say this more briefly and allocate a closure per call to
	// say it, on a path that is a branch in OutDegree. The pair is the same
	// double-checked lock without that.
	adjBuilt atomic.Bool
	adjMu    sync.Mutex

	// Label postings, built once at construction time. Answering NodesByType /
	// EdgesByType used to mean scanning every record in the CSR; these give the
	// same answer in time proportional to the number of matches.
	//
	// Both are keyed by label and hold ascending ID lists (construction walks the
	// record arena in ID order). They are derived state: Build recomputes them,
	// so they are not part of the on-disk format and cost one pass at load time.
	nodesByLabel map[store.NodeType][]store.NodeID
	edgesByLabel map[store.EdgeType][]store.EdgeID

	// Sequence high-water marks: the largest node/edge IDs ever issued at the
	// time this CSR was built. Persisted so that IDs are never reused after a
	// delete-then-compact-then-reopen cycle drops the record that held the max
	// ID. Zero means "unknown" (older CSR formats) — callers fall back to the
	// max ID physically present, HighestNodeID and HighestEdgeID.
	nodeSeqHW uint64
	edgeSeqHW uint64

	// commitSeqHW is the largest batch-commit sequence number issued at the time
	// this CSR was built, and lastCompactUnixNano is when that build happened.
	//
	// Both exist because the WAL is truncated at compaction: without somewhere
	// durable to record them, the commit counter restarts and the last
	// compaction time is forgotten on every reopen. v8 is that somewhere. Zero
	// means "unknown", which is what every pre-v8 file reports.
	commitSeqHW         uint64
	lastCompactUnixNano int64

	// roots is the Merkle identity of this image, present when the file carried
	// a GHSH section or when this graph was just serialised with one. Zero
	// otherwise, which is what every image written before snapshot roots existed
	// reports.
	roots SnapshotRoots

	// tombstones are this image's record of deliberate removals, present when
	// the file carried a GRDT section. Nil otherwise, which for a pre-tombstone
	// image means "unknown" rather than "none" — see tombstone.go.
	tombstones []Tombstone

	// attestation is the signed assertion over roots.Snapshot, present when the
	// file carried a GATT section. Its Signature is nil when absent.
	attestation Attestation

	// imgBytes is the size of the mapped image this graph's property blobs
	// address, and zero when they are heap. It is the size rather than the
	// mapping itself, and that is deliberate: runtime.AddCleanup never runs a
	// cleanup whose argument is reachable from the object it watches, and the
	// mapping is exactly that argument. Store.images holds the mapping; see
	// mapping.go.
	//
	// A graph Build produced reports zero even when its records address a
	// mapping, which every graph a compaction publishes does. That is not a
	// discrepancy to fix: the figure answers "is this image being served from a
	// file", and a rebuilt image is being served from record arrays the
	// compaction allocated.
	imgBytes int64
}

// MappedBytes is the size of the image file this graph reads its property blobs
// from, or zero when they are on the heap. See StorageStats.ImageMappedBytes.
func (g *CSRGraph) MappedBytes() int64 { return g.imgBytes }

// Roots returns the Merkle identity of this image, and whether it has one.
func (g *CSRGraph) Roots() (SnapshotRoots, bool) {
	return g.roots, !g.roots.Zero()
}

// nodeRecord is the compact per-node record.
type nodeRecord struct {
	ID         store.NodeID
	Labels     []store.NodeType // one or more labels; nil/empty = unknown
	Properties []byte
}

// Build constructs a CSRGraph from a slice of nodes and edges. It is buildSeq
// over the two slices; the contract, and the reason a sequence form exists at
// all, are stated there.
func Build(nodes []nodeRecord, edges []rawEdge) (*CSRGraph, error) {
	return buildSeq(slices.Values(nodes), slices.Values(edges), AdjacencyEager)
}

// errUnstableBuild reports a build sequence that yielded a different number of
// records on a later pass than on its first. See buildSeq.
var errUnstableBuild = errors.New("csr: a build sequence yielded a different number of records on a later pass")

// buildSeq constructs a CSRGraph from sequences of nodes and edges. They must be
// complete at build time (this is the bulk-ingest path); their order does not
// matter, because every record is placed by its own identifier.
//
// # Why a sequence rather than a slice
//
// The one caller that does not already hold a slice is compaction. Building one
// for it meant copying every live record out of the image it was about to
// replace — 56 B per node and 80 B per edge, across the whole live set, held
// under the store lock for the length of the pin and beside the image it was
// copied from. A sequence lets the merge that produces those records run inside
// the build, so the copy is never made. deserialiseCSR holds a slice already and
// reaches this through Build.
//
// # The sequences are walked more than once
//
// Three times each, and that is inherent rather than lazy: the page directory is
// sized from the highest identifier, so nothing can be touched until every ID has
// been seen; the arena is sized from the touched pages, so nothing can be placed
// until every page has been touched; and the placement is a third pass. A
// sequence passed here must therefore be repeatable and must yield the same
// records every time.
//
// Adjacency used to make it five for edges, because it re-walked the sequence to
// count degrees and again to fill. It reads the arena instead — see
// buildAdjacency — which costs the compaction merge two passes less and is what
// lets the same code build adjacency at Open and on first use.
//
// That is checked rather than assumed, to the extent it can be cheaply: each
// later pass counts what it saw and disagreement with the first pass is refused
// with errUnstableBuild, at the pass where it appears rather than as an
// out-of-range write into the adjacency arrays two passes later. A sequence that
// yields the same number of *different* records is a caller bug this does not
// try to survive. Both sequences in this package are stable by construction — a
// slice, and a merge over an immutable image and two sorted slices — so this is
// a diagnostic for whatever is written next, not a defence against a file.
//
// It fails on a duplicate identifier. Two records claiming one ID is a file
// lying about its own record count, or a compaction plan that broke its
// disjointness rule; silently keeping the last one — what the one-slot-per-ID
// layout did — hid either. A record whose ID is the invalid zero is skipped
// rather than refused: the old layout dropped it into the unused slot 0, where
// nothing ever read it, and a file that carries one loads today.
//
// An edge names two endpoints, and the page of each is materialised whether or
// not a node record falls in it. Every live edge has both endpoints present —
// deletion cascades to incident edges — so for a well-formed input this
// materialises nothing extra. For a hostile one the cost is bounded by
// checkCSREntityIDs, which counts endpoint pages as touched.
//
// With no nodes the result is an empty graph and any edges are dropped, as
// before; the edge sequence is not walked at all in that case.
//
// Memory is proportional to the pages touched, not to the highest identifier:
// one int32 per page of the identifier space up to the highest ID named, plus
// csrPageSlots records and two csrPageSlots offsets per materialised page. The
// only transient beyond the result is the directory's own touched-page marks.
func buildSeq(nodes iter.Seq[nodeRecord], edges iter.Seq[rawEdge], adj AdjacencyMode) (*CSRGraph, error) {
	g := &CSRGraph{}

	// The extent of each directory. The node directory covers the highest node
	// ID and every endpoint of a live edge; the edge directory the highest edge
	// ID. An edge with the invalid zero ID is skipped everywhere, so its
	// endpoints extend nothing.
	//
	// yielded counts every node offered, which is what len(nodes) used to answer
	// for the empty case. wantNodes and wantEdges count the ones that will be
	// placed, and are what every later pass is held to.
	var highestNode, nodeExtent, highestEdge uint64
	var yielded, wantNodes, wantEdges int
	for n := range nodes {
		yielded++
		if n.ID == store.InvalidNodeID {
			continue
		}
		wantNodes++
		if id := uint64(n.ID); id > highestNode {
			highestNode = id
		}
	}
	if yielded == 0 {
		g.nodesByLabel = make(map[store.NodeType][]store.NodeID)
		g.edgesByLabel = make(map[store.EdgeType][]store.EdgeID)
		// buildAdjacency over zero slots produces one sentinel each, which is the
		// shape verifyAdjacency checks — the offset arrays are one longer than the
		// arena at every size, and the empty image is not an exception to that.
		if adj == AdjacencyEager {
			g.ensureAdjacency()
		}
		return g, nil
	}

	nodeExtent = highestNode
	for e := range edges {
		if e.ID == store.InvalidEdgeID {
			continue
		}
		wantEdges++
		if id := uint64(e.ID); id > highestEdge {
			highestEdge = id
		}
		if src := uint64(e.Src); src > nodeExtent {
			nodeExtent = src
		}
		if dst := uint64(e.Dst); dst > nodeExtent {
			nodeExtent = dst
		}
	}

	// Mark the pages that need an arena page, then hand them out in ascending
	// page number so that arena order is identifier order.
	g.nodeDir = newPageDir(nodeExtent)
	g.edgeDir = newPageDir(highestEdge)
	seen := 0
	for n := range nodes {
		if n.ID == store.InvalidNodeID {
			continue
		}
		seen++
		touchPage(g.nodeDir, uint64(n.ID))
	}
	if seen != wantNodes {
		return nil, errUnstableBuild
	}
	seen = 0
	for e := range edges {
		if e.ID == store.InvalidEdgeID {
			continue
		}
		seen++
		touchPage(g.edgeDir, uint64(e.ID))
		touchPage(g.nodeDir, uint64(e.Src))
		touchPage(g.nodeDir, uint64(e.Dst))
	}
	if seen != wantEdges {
		return nil, errUnstableBuild
	}
	g.nodePages = assignPages(g.nodeDir)
	g.edgePages = assignPages(g.edgeDir)
	g.nodeRecs = make([]nodeRecord, len(g.nodePages)<<csrPageBits)
	g.edgeRecs = make([]rawEdge, len(g.edgePages)<<csrPageBits)

	// Place the records. A slot that is already taken is the duplicate this
	// refuses.
	for n := range nodes {
		if n.ID == store.InvalidNodeID {
			continue
		}
		s := g.nodeSlotRaw(n.ID)
		if g.nodeRecs[s].ID != store.InvalidNodeID {
			return nil, fmt.Errorf("csr: duplicate node ID %d", n.ID)
		}
		g.nodeRecs[s] = n
		g.liveNodes++
	}
	if g.liveNodes != wantNodes {
		return nil, errUnstableBuild
	}
	for e := range edges {
		if e.ID == store.InvalidEdgeID {
			continue
		}
		s := g.edgeSlotRaw(e.ID)
		if g.edgeRecs[s].ID != store.InvalidEdgeID {
			return nil, fmt.Errorf("csr: duplicate edge ID %d", e.ID)
		}
		g.edgeRecs[s] = e
		g.liveEdges++
	}
	if g.liveEdges != wantEdges {
		return nil, errUnstableBuild
	}
	g.highestNodeID = store.NodeID(highestNode)
	g.highestEdgeID = store.EdgeID(highestEdge)
	g.nodeLiveBefore = make([]int, len(g.nodePages))
	for p := 1; p < len(g.nodePages); p++ {
		g.nodeLiveBefore[p] = g.nodeLiveBefore[p-1] + g.livePageNodes(p-1)
	}
	g.edgeLiveBefore = make([]int, len(g.edgePages))
	for p := 1; p < len(g.edgePages); p++ {
		g.edgeLiveBefore[p] = g.edgeLiveBefore[p-1] + g.livePageEdges(p-1)
	}

	g.buildLabelIndex()

	if adj == AdjacencyEager {
		g.ensureAdjacency()
	}

	return g, nil
}

// ensureAdjacency builds the adjacency arrays if they are not built, and is what
// every read of them goes through.
//
// The fast path is one relaxed-looking atomic load and a predicted branch, which
// is the whole cost AdjacencyLazy imposes on a store that does traverse. The slow
// path is taken once per image.
func (g *CSRGraph) ensureAdjacency() {
	if g.adjBuilt.Load() {
		return
	}
	g.buildAdjacencyOnce()
}

// buildAdjacencyOnce is ensureAdjacency's slow half, kept separate so the fast
// half is small enough to inline into the accessors.
func (g *CSRGraph) buildAdjacencyOnce() {
	g.adjMu.Lock()
	defer g.adjMu.Unlock()
	if g.adjBuilt.Load() {
		return
	}
	g.buildAdjacency()
	g.adjBuilt.Store(true)
}

// AdjacencyBuilt reports whether this image's adjacency arrays exist yet. Under
// AdjacencyEager it is true from the moment the image is loaded; under
// AdjacencyLazy it is what tells a caller whether its pass avoided the build.
func (g *CSRGraph) AdjacencyBuilt() bool {
	return g.adjBuilt.Load()
}

// buildAdjacency fills the four adjacency arrays: count degrees into
// offset[slot+1], prefix-sum, then fill. The fill uses the offset arrays
// themselves as the running cursors and shifts them back afterwards, which is
// what spares the two full-size counter copies the one-slot-per-ID layout made.
//
// It reads the edge arena rather than the sequence that filled it. That is what
// makes eager and lazy one piece of code called at two moments rather than two
// pieces that have to be kept agreeing — the failure mode a mode flag invites,
// where both paths pass their tests and disagree with each other. It also costs
// the caller two fewer walks of a sequence that, for compaction, is a merge over
// an image and two sorted slices.
//
// The consequence is that entries within a slot's range are in ascending edge-ID
// order, which is the arena's order, rather than in whatever order the build
// sequence yielded them. Both callers already sort by ID, so nothing observable
// moves; what changes is that nothing observable *can* move with the input.
//
// Built into locals and published at the end, so a reader that has not yet seen
// adjBuilt cannot encounter a half-filled array through a stale read of the
// header.
func (g *CSRGraph) buildAdjacency() {
	slots := len(g.nodeRecs)
	outOffset := make([]uint64, slots+1)
	inOffset := make([]uint64, slots+1)
	for i := range g.edgeRecs {
		e := &g.edgeRecs[i]
		if e.ID == store.InvalidEdgeID {
			continue
		}
		outOffset[g.nodeSlotRaw(e.Src)+1]++
		inOffset[g.nodeSlotRaw(e.Dst)+1]++
	}
	for i := 1; i <= slots; i++ {
		outOffset[i] += outOffset[i-1]
		inOffset[i] += inOffset[i-1]
	}
	outEdges := make([]store.EdgeID, g.liveEdges)
	inEdges := make([]store.EdgeID, g.liveEdges)
	for i := range g.edgeRecs {
		e := &g.edgeRecs[i]
		if e.ID == store.InvalidEdgeID {
			continue
		}
		so := g.nodeSlotRaw(e.Src)
		outEdges[outOffset[so]] = e.ID
		outOffset[so]++
		si := g.nodeSlotRaw(e.Dst)
		inEdges[inOffset[si]] = e.ID
		inOffset[si]++
	}
	// After the fill offset[s] is the end of slot s, which is the start of
	// slot s+1; one shift right restores the starts and reinstates offset[0].
	copy(outOffset[1:], outOffset[:slots])
	outOffset[0] = 0
	copy(inOffset[1:], inOffset[:slots])
	inOffset[0] = 0

	g.outOffset, g.outEdges = outOffset, outEdges
	g.inOffset, g.inEdges = inOffset, inEdges
}

// newPageDir returns a directory covering identifiers 0..extent with every
// page dead.
func newPageDir(extent uint64) []int32 {
	dir := make([]int32, extent>>csrPageBits+1)
	for i := range dir {
		dir[i] = csrDeadPage
	}
	return dir
}

// touchPage marks the page holding id as needing an arena page. The directory
// was sized to cover id, so there is no bounds check.
func touchPage(dir []int32, id uint64) {
	dir[id>>csrPageBits] = 0
}

// assignPages gives every touched page an arena index in ascending page-number
// order and returns the page number of each arena page.
func assignPages(dir []int32) []uint32 {
	n := 0
	for _, e := range dir {
		if e != csrDeadPage {
			n++
		}
	}
	pages := make([]uint32, 0, n)
	for p := range dir {
		if dir[p] == csrDeadPage {
			continue
		}
		dir[p] = int32(len(pages))
		pages = append(pages, uint32(p))
	}
	return pages
}

// livePageNodes counts the live records in arena page p.
func (g *CSRGraph) livePageNodes(p int) int {
	n := 0
	for _, rec := range g.nodeRecs[p<<csrPageBits : (p+1)<<csrPageBits] {
		if rec.ID != store.InvalidNodeID {
			n++
		}
	}
	return n
}

func (g *CSRGraph) livePageEdges(p int) int {
	n := 0
	for _, rec := range g.edgeRecs[p<<csrPageBits : (p+1)<<csrPageBits] {
		if rec.ID != store.InvalidEdgeID {
			n++
		}
	}
	return n
}

// nodeSlot returns the arena slot for id, or -1 when id is the invalid zero,
// lies beyond the directory, or falls in a page with no records. A slot says
// nothing about whether a record is there: the caller compares the record's ID.
//
// The zero identifier is rejected here rather than by the ID comparison
// because page 0's slot 0 holds a zero-valued record that satisfies `n.ID == 0`,
// which would make InvalidNodeID read as an entity that exists.
func (g *CSRGraph) nodeSlot(id store.NodeID) int {
	if id == store.InvalidNodeID {
		return -1
	}
	p := uint64(id) >> csrPageBits
	if p >= uint64(len(g.nodeDir)) {
		return -1
	}
	pi := g.nodeDir[p]
	if pi == csrDeadPage {
		return -1
	}
	return int(pi)<<csrPageBits | int(id&csrPageMask)
}

func (g *CSRGraph) edgeSlot(id store.EdgeID) int {
	if id == store.InvalidEdgeID {
		return -1
	}
	p := uint64(id) >> csrPageBits
	if p >= uint64(len(g.edgeDir)) {
		return -1
	}
	pi := g.edgeDir[p]
	if pi == csrDeadPage {
		return -1
	}
	return int(pi)<<csrPageBits | int(id&csrPageMask)
}

// nodeSlotRaw is nodeSlot for Build, where the directory is known to cover id
// and its page to be live, and where the zero identifier must map to its slot
// so that an edge naming it — tolerated, never valid — still has somewhere to
// count its adjacency.
func (g *CSRGraph) nodeSlotRaw(id store.NodeID) int {
	return int(g.nodeDir[uint64(id)>>csrPageBits])<<csrPageBits | int(id&csrPageMask)
}

func (g *CSRGraph) edgeSlotRaw(id store.EdgeID) int {
	return int(g.edgeDir[uint64(id)>>csrPageBits])<<csrPageBits | int(id&csrPageMask)
}

// nodeSlotID is the identifier a node slot stands for, live or not.
func (g *CSRGraph) nodeSlotID(slot int) store.NodeID {
	return store.NodeID(uint64(g.nodePages[slot>>csrPageBits])<<csrPageBits | uint64(slot&csrPageMask))
}

// buildLabelIndex populates the label postings by walking the record arenas in
// ID order, which yields ascending postings lists for free.
//
// A record's labels are deduplicated as we go: a repeated label would otherwise
// append the same ID twice in a row, breaking the strict-ascending invariant the
// postings rely on. Records carrying duplicates can come from a caller or from a
// CSR file written before that was normalised, so this cannot assume clean input.
func (g *CSRGraph) buildLabelIndex() {
	g.nodesByLabel = make(map[store.NodeType][]store.NodeID)
	g.edgesByLabel = make(map[store.EdgeType][]store.EdgeID)

	for n := range g.Nodes() {
		for j, lbl := range n.Labels {
			if nodeRecordHasLabel(n.Labels[:j], lbl) {
				continue
			}
			g.nodesByLabel[lbl] = append(g.nodesByLabel[lbl], n.ID)
		}
	}
	for e := range g.Edges() {
		for j, lbl := range e.Labels {
			if rawEdgeHasLabel(e.Labels[:j], lbl) {
				continue
			}
			g.edgesByLabel[lbl] = append(g.edgesByLabel[lbl], e.ID)
		}
	}
}

// Nodes yields every live node record in ascending ID order. That order is the
// file's record order and the Merkle leaf order, and it holds because arena
// pages are assigned in page-number order and a dead slot is the zero record.
func (g *CSRGraph) Nodes() iter.Seq[nodeRecord] {
	return func(yield func(nodeRecord) bool) {
		for i := range g.nodeRecs {
			if g.nodeRecs[i].ID == store.InvalidNodeID {
				continue
			}
			if !yield(g.nodeRecs[i]) {
				return
			}
		}
	}
}

// Edges yields every live edge record in ascending ID order.
func (g *CSRGraph) Edges() iter.Seq[rawEdge] {
	return func(yield func(rawEdge) bool) {
		for i := range g.edgeRecs {
			if g.edgeRecs[i].ID == store.InvalidEdgeID {
				continue
			}
			if !yield(g.edgeRecs[i]) {
				return
			}
		}
	}
}

// NodeIDs yields every live node ID in ascending order, without copying the
// records.
func (g *CSRGraph) NodeIDs() iter.Seq[store.NodeID] {
	return func(yield func(store.NodeID) bool) {
		for i := range g.nodeRecs {
			if id := g.nodeRecs[i].ID; id != store.InvalidNodeID && !yield(id) {
				return
			}
		}
	}
}

// EdgeIDs yields every live edge ID in ascending order.
func (g *CSRGraph) EdgeIDs() iter.Seq[store.EdgeID] {
	return func(yield func(store.EdgeID) bool) {
		for i := range g.edgeRecs {
			if id := g.edgeRecs[i].ID; id != store.InvalidEdgeID && !yield(id) {
				return
			}
		}
	}
}

// nodeIDCursor walks the live node IDs in ascending order one call at a time,
// so a scan can put the walk down across a lock release and pick it up again.
//
// It holds the arena rather than the graph, and the arena is immutable once
// published (§9.2), so a cursor stays valid for as long as the image it was
// taken from — which is what lets a Scanner resume into an image after the
// store has moved on to a newer one. The zero cursor is exhausted.
type nodeIDCursor struct {
	recs []nodeRecord
	slot int
}

// nodeIDCursor returns a cursor at the start of the image; a nil graph yields
// an exhausted one.
func (g *CSRGraph) nodeIDCursor() nodeIDCursor {
	if g == nil {
		return nodeIDCursor{}
	}
	return nodeIDCursor{recs: g.nodeRecs}
}

// next returns the next live ID, or false once the arena is walked.
func (c *nodeIDCursor) next() (store.NodeID, bool) {
	for c.slot < len(c.recs) {
		id := c.recs[c.slot].ID
		c.slot++
		if id != store.InvalidNodeID {
			return id, true
		}
	}
	return store.InvalidNodeID, false
}

// edgeIDCursor is nodeIDCursor for edges.
type edgeIDCursor struct {
	recs []rawEdge
	slot int
}

func (g *CSRGraph) edgeIDCursor() edgeIDCursor {
	if g == nil {
		return edgeIDCursor{}
	}
	return edgeIDCursor{recs: g.edgeRecs}
}

func (c *edgeIDCursor) next() (store.EdgeID, bool) {
	for c.slot < len(c.recs) {
		id := c.recs[c.slot].ID
		c.slot++
		if id != store.InvalidEdgeID {
			return id, true
		}
	}
	return store.InvalidEdgeID, false
}

// OutboundEdges returns the raw edges for nodeID in outbound direction.
func (g *CSRGraph) OutboundEdges(id store.NodeID) ([]rawEdge, error) {
	g.ensureAdjacency()
	return g.adjacentEdges(id, g.outOffset, g.outEdges)
}

// InboundEdges returns the raw edges for nodeID in inbound direction.
func (g *CSRGraph) InboundEdges(id store.NodeID) ([]rawEdge, error) {
	g.ensureAdjacency()
	return g.adjacentEdges(id, g.inOffset, g.inEdges)
}

// adjacentEdges errors for an identifier beyond the directory and answers an
// empty list for one the directory covers but no record or endpoint ever
// named — the same split the one-slot-per-ID layout made between "beyond the
// array" and "a dead slot inside it".
func (g *CSRGraph) adjacentEdges(id store.NodeID, offsets []uint64, edgeList []store.EdgeID) ([]rawEdge, error) {
	if uint64(id)>>csrPageBits >= uint64(len(g.nodeDir)) {
		return nil, fmt.Errorf("node %d out of range", id)
	}
	s := g.nodeSlot(id)
	if s < 0 {
		return nil, nil
	}
	start, end := offsets[s], offsets[s+1]
	result := make([]rawEdge, 0, end-start)
	for _, eid := range edgeList[start:end] {
		if e, ok := g.GetEdge(eid); ok {
			result = append(result, e)
		}
	}
	return result, nil
}

// verifyLabelIndex checks that the label postings describe exactly the records
// present in the CSR, in ascending order.
func (g *CSRGraph) verifyLabelIndex() error {
	for lbl, ids := range g.nodesByLabel {
		for i, id := range ids {
			if i > 0 && ids[i-1] >= id {
				return fmt.Errorf("csr node label index: %v postings not strictly ascending at %d", lbl, i)
			}
			n, ok := g.GetNode(id)
			if !ok {
				return fmt.Errorf("csr node label index: %v lists node %d, which is not in the CSR", lbl, id)
			}
			if !nodeRecordHasLabel(n.Labels, lbl) {
				return fmt.Errorf("csr node label index: %v lists node %d, which does not carry that label", lbl, id)
			}
		}
	}
	for n := range g.Nodes() {
		for _, lbl := range n.Labels {
			// Postings are ascending, so membership is a binary search. A linear
			// scan here would make verification quadratic in the size of the
			// largest label.
			if !sortedContainsNodeID(g.nodesByLabel[lbl], n.ID) {
				return fmt.Errorf("csr node label index: node %d carries %v but is missing from the postings", n.ID, lbl)
			}
		}
	}

	for lbl, ids := range g.edgesByLabel {
		for i, id := range ids {
			if i > 0 && ids[i-1] >= id {
				return fmt.Errorf("csr edge label index: %v postings not strictly ascending at %d", lbl, i)
			}
			e, ok := g.GetEdge(id)
			if !ok {
				return fmt.Errorf("csr edge label index: %v lists edge %d, which is not in the CSR", lbl, id)
			}
			if !rawEdgeHasLabel(e.Labels, lbl) {
				return fmt.Errorf("csr edge label index: %v lists edge %d, which does not carry that label", lbl, id)
			}
		}
	}
	for e := range g.Edges() {
		for _, lbl := range e.Labels {
			if !sortedContainsEdgeID(g.edgesByLabel[lbl], e.ID) {
				return fmt.Errorf("csr edge label index: edge %d carries %v but is missing from the postings", e.ID, lbl)
			}
		}
	}
	return nil
}

// sortedContainsNodeID reports membership in an ascending slice in O(log n).
func sortedContainsNodeID(ids []store.NodeID, target store.NodeID) bool {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] >= target })
	return i < len(ids) && ids[i] == target
}

// sortedContainsEdgeID reports membership in an ascending slice in O(log n).
func sortedContainsEdgeID(ids []store.EdgeID, target store.EdgeID) bool {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] >= target })
	return i < len(ids) && ids[i] == target
}

// verifyAdjacency checks that the offset arrays span the arena, are monotonic,
// and that every adjacency entry points at an edge whose endpoint matches.
//
// It walks every slot of every materialised page rather than only the live
// records: an endpoint page materialised for an edge whose node record is
// absent carries adjacency at a slot no record occupies, and that adjacency is
// exactly what a dangling edge would show up as.
func (g *CSRGraph) verifyAdjacency() error {
	// Checking adjacency is one of the things that needs it. A lazily loaded
	// image being verified builds it here, which is the honest reading of
	// "verify the adjacency": there is no state to check until it exists, and
	// reporting an unbuilt image as sound would make Verify weaker under one
	// mode than the other.
	g.ensureAdjacency()

	slots := len(g.nodeRecs)
	if len(g.outOffset) != slots+1 || len(g.inOffset) != slots+1 {
		return fmt.Errorf("csr adjacency: offset arrays are %d and %d long for %d slots",
			len(g.outOffset), len(g.inOffset), slots)
	}
	for i := 1; i <= slots; i++ {
		if g.outOffset[i] < g.outOffset[i-1] {
			return fmt.Errorf("csr adjacency: outOffset not monotonic at %d", i)
		}
		if g.inOffset[i] < g.inOffset[i-1] {
			return fmt.Errorf("csr adjacency: inOffset not monotonic at %d", i)
		}
	}
	if int(g.outOffset[slots]) != len(g.outEdges) {
		return fmt.Errorf("csr adjacency: outOffset tail %d != len(outEdges) %d", g.outOffset[slots], len(g.outEdges))
	}
	if int(g.inOffset[slots]) != len(g.inEdges) {
		return fmt.Errorf("csr adjacency: inOffset tail %d != len(inEdges) %d", g.inOffset[slots], len(g.inEdges))
	}

	for s := 0; s < slots; s++ {
		nodeID := g.nodeSlotID(s)
		for _, eid := range g.outEdges[g.outOffset[s]:g.outOffset[s+1]] {
			e, ok := g.GetEdge(eid)
			if !ok {
				return fmt.Errorf("csr adjacency: node %d lists outbound edge %d, which is not in the CSR", nodeID, eid)
			}
			if e.Src != nodeID {
				return fmt.Errorf("csr adjacency: node %d lists outbound edge %d, whose Src is %d", nodeID, eid, e.Src)
			}
		}
		for _, eid := range g.inEdges[g.inOffset[s]:g.inOffset[s+1]] {
			e, ok := g.GetEdge(eid)
			if !ok {
				return fmt.Errorf("csr adjacency: node %d lists inbound edge %d, which is not in the CSR", nodeID, eid)
			}
			if e.Dst != nodeID {
				return fmt.Errorf("csr adjacency: node %d lists inbound edge %d, whose Dst is %d", nodeID, eid, e.Dst)
			}
		}
	}
	return nil
}

// OutboundEdgeIDs returns the outbound edge IDs for nodeID as a sub-slice of the
// CSR's internal adjacency array — no copy, no rawEdge materialisation. The
// result aliases CSR-owned memory: callers must hold the store lock and must not
// retain or mutate it.
func (g *CSRGraph) OutboundEdgeIDs(id store.NodeID) []store.EdgeID {
	g.ensureAdjacency()
	return g.adjacencySlice(id, g.outOffset, g.outEdges)
}

// InboundEdgeIDs returns the inbound edge IDs for nodeID. Same aliasing contract
// as OutboundEdgeIDs.
func (g *CSRGraph) InboundEdgeIDs(id store.NodeID) []store.EdgeID {
	g.ensureAdjacency()
	return g.adjacencySlice(id, g.inOffset, g.inEdges)
}

// OutDegree returns the outbound degree of nodeID in constant time, straight
// from the offset array. It counts edges present in the CSR, so callers must
// still account for any delete masks held by the store.
func (g *CSRGraph) OutDegree(id store.NodeID) int {
	g.ensureAdjacency()
	return len(g.adjacencySlice(id, g.outOffset, g.outEdges))
}

// InDegree returns the inbound degree of nodeID in constant time.
func (g *CSRGraph) InDegree(id store.NodeID) int {
	g.ensureAdjacency()
	return len(g.adjacencySlice(id, g.inOffset, g.inEdges))
}

// adjacencySlice is nil for an identifier with no slot — beyond the directory,
// in a dead page, or the invalid zero — and the slot's range otherwise.
func (g *CSRGraph) adjacencySlice(id store.NodeID, offsets []uint64, edgeList []store.EdgeID) []store.EdgeID {
	s := g.nodeSlot(id)
	if s < 0 {
		return nil
	}
	return edgeList[offsets[s]:offsets[s+1]]
}

// GetNode returns the nodeRecord for the given ID.
func (g *CSRGraph) GetNode(id store.NodeID) (nodeRecord, bool) {
	s := g.nodeSlot(id)
	if s < 0 {
		return nodeRecord{}, false
	}
	n := g.nodeRecs[s]
	return n, n.ID == id
}

// containsNode is GetNode's presence half without the record copy.
//
// The record is two slice headers beside its identifier, so copying one is
// cheap -- but this is asked once per property-index entry while an image is
// being written, which on the store this program is aimed at is 28 million
// times, and the answer wanted there is a bool.
func (g *CSRGraph) containsNode(id store.NodeID) bool {
	s := g.nodeSlot(id)
	return s >= 0 && g.nodeRecs[s].ID == id
}

// containsEdge is containsNode for edges.
func (g *CSRGraph) containsEdge(id store.EdgeID) bool {
	s := g.edgeSlot(id)
	return s >= 0 && g.edgeRecs[s].ID == id
}

// GetEdge returns the rawEdge for the given ID.
func (g *CSRGraph) GetEdge(id store.EdgeID) (rawEdge, bool) {
	s := g.edgeSlot(id)
	if s < 0 {
		return rawEdge{}, false
	}
	e := g.edgeRecs[s]
	return e, e.ID == id
}

// NodeCount returns the number of stored nodes.
func (g *CSRGraph) NodeCount() int { return g.liveNodes }

// EdgeCount returns the number of stored edges.
func (g *CSRGraph) EdgeCount() int { return g.liveEdges }

// HighestNodeID is the highest node identifier carrying a record, or zero for
// an image with no nodes. It is what the image physically holds, not what the
// store has issued: the sequence high-water marks in the header are the latter,
// and are what the next identifier is taken from.
func (g *CSRGraph) HighestNodeID() store.NodeID { return g.highestNodeID }

// HighestEdgeID is HighestNodeID for edges.
func (g *CSRGraph) HighestEdgeID() store.EdgeID { return g.highestEdgeID }

// nodePageCount is the number of node pages materialised — the unit the image's
// resident cost is proportional to.
func (g *CSRGraph) nodePageCount() int { return len(g.nodePages) }

func (g *CSRGraph) edgePageCount() int { return len(g.edgePages) }

// nodeLeafIndex returns the position of id among the live nodes, which is the
// index an inclusion proof is built at: the number of live records with a
// smaller ID, since Nodes walks ascending. One page scan at most, thanks to the
// per-page prefix nodeLiveBefore.
func (g *CSRGraph) nodeLeafIndex(id store.NodeID) (int, bool) {
	s := g.nodeSlot(id)
	if s < 0 || g.nodeRecs[s].ID != id {
		return 0, false
	}
	p := s >> csrPageBits
	pos := g.nodeLiveBefore[p]
	for _, rec := range g.nodeRecs[p<<csrPageBits : s] {
		if rec.ID != store.InvalidNodeID {
			pos++
		}
	}
	return pos, true
}

// edgeLeafIndex is nodeLeafIndex for edges.
func (g *CSRGraph) edgeLeafIndex(id store.EdgeID) (int, bool) {
	s := g.edgeSlot(id)
	if s < 0 || g.edgeRecs[s].ID != id {
		return 0, false
	}
	p := s >> csrPageBits
	pos := g.edgeLiveBefore[p]
	for _, rec := range g.edgeRecs[p<<csrPageBits : s] {
		if rec.ID != store.InvalidEdgeID {
			pos++
		}
	}
	return pos, true
}

// NodesByType returns the ascending node IDs carrying the given label, served
// from the label index. The result aliases CSR-owned memory: callers must hold
// the store lock and must not retain or mutate it.
func (g *CSRGraph) NodesByType(t store.NodeType) []store.NodeID {
	return g.nodesByLabel[t]
}

// EdgesByType returns the ascending edge IDs carrying the given label. Same
// aliasing contract as NodesByType.
func (g *CSRGraph) EdgesByType(t store.EdgeType) []store.EdgeID {
	return g.edgesByLabel[t]
}

// SortedEdgesByType returns edge IDs sorted for deterministic output, as a copy
// the caller owns. The label index is already built in ascending ID order, so
// this only has to detach the result from CSR-owned memory.
func (g *CSRGraph) SortedEdgesByType(t store.EdgeType) []store.EdgeID {
	ids := g.EdgesByType(t)
	out := make([]store.EdgeID, len(ids))
	copy(out, ids)
	slices.Sort(out)
	return out
}

// nodeRecordHasLabel returns true if the label slice contains t.
func nodeRecordHasLabel(labels []store.NodeType, t store.NodeType) bool {
	for _, l := range labels {
		if l == t {
			return true
		}
	}
	return false
}

// rawEdgeHasLabel returns true if the label slice contains t.
func rawEdgeHasLabel(labels []store.EdgeType, t store.EdgeType) bool {
	for _, l := range labels {
		if l == t {
			return true
		}
	}
	return false
}
