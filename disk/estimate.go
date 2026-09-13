package disk

// What the store is holding, in bytes, answered in bounded time.
//
// # The relationship to preflight.go
//
// These are the same question asked at two different moments, and the difference
// in what is knowable at each is the reason they are two files rather than one.
// PreflightOpen answers "what would this cost" from a header and a log walk,
// before anything is allocated: every count it has is a bound read out of a file,
// so every figure it reports is an upper bound and it says so. This answers "what
// is this costing" from the structures themselves, so most of its counts are not
// estimates at all -- a slice's length is not a model of how long the slice is.
//
// What survives from that file is the posture, which is worth restating because
// it governs every constant below. A figure a caller refuses an operation on must
// be wrong in the direction that declines work that would have fit, never in the
// direction that admits work that will not. Where a term here has to be modelled,
// the model is the larger reading.
//
// # What it is, and what it is not
//
// It is retained heap: the bytes this process must keep. It is not resident set
// size. On the one fixture measured for it, RSS ran between 1.19x and 1.54x the
// live heap across three runs of an identical store -- the runtime's own
// allocator slack, its size-class rounding, the GC's headroom against GOGC, and
// the stacks and metadata no model here describes. So this is a floor on RSS and
// a ratio away from it, which is what makes it useful for comparing two
// configurations of the same store and misleading if read as a prediction of what
// the operating system will report.
//
// Mapped bytes are deliberately not in the total. An image read through a mapping
// is page cache the kernel may evict at will, and adding it to a figure that
// means "memory this process must keep" would say the opposite of the thing
// ImageMapped was built to achieve. It is reported beside the total instead.
//
// # Cost
//
// O(distinct labels + declared index keys), which is to say a constant for every
// store anyone runs. That is a requirement rather than an observation:
// AutoCompact evaluates a policy against StorageStats on a ticker, so a term here
// that walked the delta or the index would put a pass over the whole store on a
// timer.

import (
	"time"
)

// Sizes of the structures the model counts. Spelled out rather than taken from
// unsafe.Sizeof so that arithmetic does not put unsafe in this package's import
// list, which tests/build_constraints audits by exact set; estimate_test.go
// asserts every one of them against unsafe.Sizeof instead, so a struct that
// grows a field fails a test rather than quietly under-reporting.
const (
	sizeofNodeRecord = 56 // ID, labels header, properties header
	sizeofRawEdge    = 80 // ID, Src, Dst, labels header, Weight and its padding, properties header
	sizeofEdgeID     = 8
	sizeofNodeID     = 8
	sizeofDirEntry   = 4  // one int32 of a page directory
	sizeofPageEntry  = 4  // one uint32 of the inverse page table
	sizeofLiveBefore = 8  // one int of nodeLiveBefore
	sizeofOffset     = 8  // one uint64 of an adjacency offset array
	sizeofDeltaAdj   = 48 // two slice headers
	sizeofWALSlot    = 40 // seq, ready, recType, raw, padding, payload header
	sizeofPointer    = 8
)

// residentMapHeaderBytes is what an allocated Go map costs before it holds
// anything: the header, and the first table's own bookkeeping.
const residentMapHeaderBytes = 64

// residentMapBytes models a Go map holding n entries whose key and value occupy
// keyVal bytes between them.
//
// Swiss tables fill at most seven of every eight slots and carry one control byte
// per slot, so the storage is the entry width plus that byte over a load factor
// of 7/8. This ignores the transition at which a table doubles, which can leave a
// map holding twice the slots it needs -- an under-report bounded by a factor of
// two on the structural terms, which are the small ones. The payload terms, where
// the bytes actually are, are counted from lengths and not from this.
func residentMapBytes(n int, keyVal int64) int64 {
	if n == 0 {
		return 0
	}
	return residentMapHeaderBytes + (int64(n)*(keyVal+1)*8)/7
}

// ResidentEstimate is what a store is holding, term by term.
//
// The breakdown is the useful part. A total on its own tells an operator that
// their store is large, which they knew; the split tells them which of the four
// things they can actually change -- compact, map the image, map the index, defer
// adjacency -- would change it. It is also what makes the figure debuggable: a
// term that disagrees with a profile is a term, not a number.
//
// Every field is bytes.
type ResidentEstimate struct {
	// RecordArrays is the image's node and edge arenas and the page tables that
	// address them. Exact: these are slice lengths.
	//
	// This is the term that scales with identifiers issued rather than with
	// records held, because a page is materialised whole. It is what R10(b)
	// bounded and what still makes a store's lifetime finite.
	RecordArrays int64

	// Payload is what the image's record payloads occupy: label sequences
	// always, property blobs unless the image is mapped. See
	// CSRGraph.payloadBytes, which states the one case where this over-reports.
	Payload int64

	// LabelPostings is the image's by-label id lists. Exact.
	LabelPostings int64

	// Adjacency is the image's four adjacency arrays, and zero while
	// AdjacencyLazy has not built them. Exact.
	Adjacency int64

	// Index is the property index: the resident delta, the retraction sets, and
	// a mapped base's directory. Its counts are exact and its per-entry costs
	// measured; see index.PropertyIndex.ResidentBytes.
	Index int64

	// Delta is everything written since the last compaction: the record
	// payloads (exact, maintained -- see deltaLayer.bytes) plus the maps,
	// adjacency and type postings around them (modelled).
	Delta int64

	// WAL is the log's ring of pending frames. A constant in practice.
	WAL int64

	// Total is the sum of the fields above: heap this process must keep.
	Total int64

	// Mapped is the image file addressed through a mapping, and zero when the
	// image is in the heap. Not in Total, deliberately: these are page cache the
	// kernel may evict, so counting them as memory the process must keep would
	// invert the comparison ImageMapped exists to win.
	Mapped int64

	// At is when the estimate was taken.
	At time.Time
}

// EstimateResident reports what this store is currently holding, term by term.
//
// Read ResidentEstimate's own documentation and this file's header before acting
// on the total: it is retained heap rather than resident set size, it is a floor
// on the latter, and one of its terms over-reports for a bounded window after a
// compaction over a mapped image.
//
// Taken under the store read lock, so the terms are mutually consistent -- a
// caller comparing the delta against the image should not see figures from either
// side of a compaction.
func (s *Store) EstimateResident() ResidentEstimate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.estimateResidentLocked()
}

// estimateResidentLocked is EstimateResident with the lock already held, which is
// how StorageStats gets the figure without taking it twice.
func (s *Store) estimateResidentLocked() ResidentEstimate {
	est := ResidentEstimate{At: s.now()}
	v := s.viewPtr.Load()
	if csr := v.csr; csr != nil {
		est.RecordArrays = csr.recordArrayBytes()
		est.Payload = csr.payloadBytes
		est.LabelPostings = csr.labelPostingBytes()
		est.Adjacency = csr.adjacencyBytes()
		est.Mapped = csr.imgBytes
	}
	est.Index = s.index().ResidentBytes()
	est.Delta = v.delta.residentBytes()
	if s.wal != nil {
		est.WAL = s.wal.residentBytes()
	}
	est.Total = est.RecordArrays + est.Payload + est.LabelPostings +
		est.Adjacency + est.Index + est.Delta + est.WAL
	return est
}

// recordArrayBytes is the record arenas and the tables that address them.
//
// Every term is a slice length, so this is exact rather than modelled -- which is
// worth saying about the one term that is usually the largest after the index.
func (g *CSRGraph) recordArrayBytes() int64 {
	return int64(len(g.nodeRecs))*sizeofNodeRecord +
		int64(len(g.edgeRecs))*sizeofRawEdge +
		int64(len(g.nodeDir)+len(g.edgeDir))*sizeofDirEntry +
		int64(len(g.nodePages)+len(g.edgePages))*sizeofPageEntry +
		int64(len(g.nodeLiveBefore)+len(g.edgeLiveBefore))*sizeofLiveBefore
}

// adjacencyBytes is the four adjacency arrays, or zero while they are deferred.
//
// The Load pairs with buildAdjacency's Store for the same reason every other read
// of the flag does: the arrays are published by it, and reading their lengths
// without it is reading slice headers another goroutine is writing.
func (g *CSRGraph) adjacencyBytes() int64 {
	if !g.adjBuilt.Load() {
		return 0
	}
	return int64(len(g.outOffset)+len(g.inOffset))*sizeofOffset +
		int64(len(g.outEdges)+len(g.inEdges))*sizeofEdgeID
}

// labelPostingBytes is the by-label id lists.
//
// The loop is over distinct labels, of which a store has a handful, so the ids
// are counted exactly rather than modelled from a record count -- which matters
// because a record carrying three labels is in three postings and a model from
// the record count would say one.
func (g *CSRGraph) labelPostingBytes() int64 {
	total := residentMapBytes(len(g.nodesByLabel), 2+24) +
		residentMapBytes(len(g.edgesByLabel), 2+24)
	for _, ids := range g.nodesByLabel {
		total += int64(len(ids)) * sizeofNodeID
	}
	for _, ids := range g.edgesByLabel {
		total += int64(len(ids)) * sizeofEdgeID
	}
	return total
}

// residentBytes is what the delta layer holds: its maintained payload total plus
// the structure around it.
//
// The split is the point. bytes is exact and maintained on the write paths, and
// it is where the memory actually is -- a record with a blob costs hundreds of
// bytes and its map slot costs tens. Everything else here is modelled, and two of
// the models are deliberately loose in the safe direction:
//
// The adjacency lists are charged two entries per delta edge, which is the
// maximum: an edge the image already holds is listed in neither, so a store that
// only updates existing edges is over-charged for this term. Finding out how many
// would mean summing the lists, and the lists are the one delta structure whose
// length is proportional to what has been written rather than to how many
// distinct things it names.
//
// The type postings are summed exactly, because the loop is over distinct labels
// rather than over records -- the same argument as CSRGraph.labelPostingBytes,
// and it matters here for the same reason.
func (d *deltaLayer) residentBytes() int64 {
	total := d.bytes

	// The version maps: an id and a pointer per entry.
	total += residentMapBytes(len(d.nodes), sizeofNodeID+sizeofPointer)
	total += residentMapBytes(len(d.edges), sizeofEdgeID+sizeofPointer)

	// Delta adjacency: the map, one deltaAdj per listed node, and at most two
	// list entries per delta edge.
	total += residentMapBytes(len(d.adj), sizeofNodeID+sizeofPointer)
	total += int64(len(d.adj)) * sizeofDeltaAdj
	total += int64(len(d.edges)) * 2 * sizeofEdgeID

	// Type postings, counted rather than modelled.
	total += residentMapBytes(len(d.nodesByType), 2+24)
	total += residentMapBytes(len(d.edgesByType), 2+24)
	for _, ids := range d.nodesByType {
		total += int64(len(ids)) * sizeofNodeID
	}
	for _, ids := range d.edgesByType {
		total += int64(len(ids)) * sizeofEdgeID
	}
	return total
}

// residentBytes is the log's ring of pending frames.
//
// The ring alone. A slot's payload is released as soon as it is written (see
// drainTo), so what a log retains between writes is the slot array and nothing
// else; the frames in flight belong to the writers holding them and are bounded
// by the ring's length, not by the log's. Read without the lock because the ring
// is allocated once at open and never replaced -- the operations that end a log
// file write a new header, not a new ring.
func (w *WAL) residentBytes() int64 {
	return int64(len(w.ring)) * sizeofWALSlot
}
