package disk

// The memory budget: the one place an operation is refused for its size.
//
// # Why a refusal is the only available answer
//
// Go cannot catch an out-of-memory condition. There is no allocation failure to
// handle, no error returned from make, and no callback the runtime offers before
// the kernel's OOM killer arrives -- the process simply stops, mid-write, with no
// opportunity to flush, unwind, or record what it was doing. Every other
// mechanism a database reaches for under memory pressure (spill to disk, shrink a
// cache, degrade a plan) presupposes being able to observe the pressure and act
// while still running.
//
// So "fail predictably under memory pressure" reduces to one thing here: decide
// before allocating, and decline. That is what this file does, and the two
// decisions it makes are not the same decision twice.
//
// # The two moments, and why their models differ
//
// An Open is asked about a store nobody has read yet. Every figure comes out of a
// header, so every figure is a bound -- see preflight.go, and OpenEstimate's
// HeapBytesFor for the Options-dependent total this compares against.
//
// A Compact is asked about a store that is already open and whose figures are
// therefore mostly exact. What it needs is not a total but a difference: the
// additional heap a build holds while the image it is replacing is still live.
// That difference is not the size of the new image, because most of the new
// image is the old one -- see compactWorkingSet, where the reason is the whole of
// why this model is not simply "twice the store".
//
// # What is deliberately not here
//
// No runtime degradation of any kind. A budget does not shrink a cache, spill a
// merge, or reduce a working set: it declines an operation whole, before the
// operation has allocated anything, and the store is exactly as it was. A caller
// whose Compact is refused has a store that still works and a delta that is
// still growing, which is a worse position than having compacted -- but it is a
// position they can act on, which is the point.

import (
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// ErrMemoryBudget reports an operation refused because it would exceed
// Options.MemoryBudget.
//
// Test for it with errors.Is; recover the figures with errors.As and
// *MemoryBudgetError, which carries them separately so a caller can decide
// what to do rather than parse a sentence.
var ErrMemoryBudget = errors.New("disk: this operation would exceed the configured memory budget")

// MemoryBudgetError is a refusal with its arithmetic attached.
//
// A refusal that does not say by how much cannot be acted on: the difference
// between a store that needs 5% more budget and one that needs six times it is
// the difference between raising a limit and buying a machine. All four fields
// are bytes.
type MemoryBudgetError struct {
	// Op is what was refused: "open" or "compact". The same budget governs both
	// and their arithmetic is not comparable, so a caller reading the figures
	// has to know which question they answer.
	Op string

	// Need is what the operation would add. For an open that is the whole
	// modelled cost of the store, since nothing is held yet. For a compaction it
	// is the working set only -- the store it is compacting is in Have.
	Need int64

	// Have is what is already held, as EstimateResident reports it: zero for an
	// open, and the store's current retained heap for a compaction.
	Have int64

	// Budget is Options.MemoryBudget as configured.
	Budget int64
}

func (e *MemoryBudgetError) Error() string {
	if e.Have == 0 {
		return fmt.Sprintf("%s: %s needs about %d bytes, budget is %d (Options.MemoryBudget)",
			ErrMemoryBudget.Error(), e.Op, e.Need, e.Budget)
	}
	return fmt.Sprintf("%s: %s needs about %d bytes on top of the %d already held, budget is %d (Options.MemoryBudget)",
		ErrMemoryBudget.Error(), e.Op, e.Need, e.Have, e.Budget)
}

// Unwrap gives a caller one condition to test whichever operation was refused,
// the same shape store.UniqueViolationsError uses for the same reason.
func (e *MemoryBudgetError) Unwrap() error { return ErrMemoryBudget }

// Over is by how much the budget was exceeded, which is the figure to raise it
// by. Present so a caller does not have to redo the subtraction and risk
// disagreeing with the one that produced the refusal.
func (e *MemoryBudgetError) Over() int64 { return e.Need + e.Have - e.Budget }

// budgetRefusal is the one comparison, so that both call sites cannot drift into
// disagreeing about whether the budget is inclusive.
//
// Exceeding is strict: a store modelled at exactly the budget is admitted. The
// model is a floor on RSS rather than a prediction of it (see estimate.go), so
// the boundary is arbitrary either way; being strict means a caller who sets the
// budget to a figure this package reported can act on it, which is the only use
// for the equal case.
func budgetRefusal(op string, need, have, budget int64) error {
	if budget <= 0 || need+have <= budget {
		return nil
	}
	return &MemoryBudgetError{Op: op, Need: need, Have: have, Budget: budget}
}

// --- the Open side ---

// checkOpenBudget refuses an Open whose modelled cost exceeds
// Options.MemoryBudget, before anything is allocated.
//
// The estimate is taken for the Options actually being opened with rather than
// for the worst case over them, because the gap between the two is larger than
// the figure most callers are budgeting around: the same store models at 736 KB
// under the defaults and 1.45 MB under ImageHeap with a resident index. Refusing
// a mapped open on the cost of a heap one would decline work that would have fit
// by more than a factor of two.
//
// Cost: one addressed read of the image header and one sequential pass over the
// log. The pass is the expensive half and it is unavoidable -- the log's replay
// cost is a function of its contents, not its length, and a figure read out of
// the file's size would be the under-report that lets the open through. A store
// with both this and a replay budget set can therefore read the log twice before
// refusing. Bounded memory, not bounded I/O, is what is on offer; see
// PreflightOpen, which says the same thing about itself.
func checkOpenBudget(dir string, opts Options) error {
	if opts.MemoryBudget <= 0 {
		return nil
	}
	est, err := PreflightOpen(dir)
	if err != nil {
		// The estimate could not be taken, so there is no figure to refuse on.
		// Deliberately not an error: an Open that would have succeeded must not
		// start failing because a budget was set, and every way this can fail is
		// a way the open itself is about to fail more informatively.
		return nil
	}
	return budgetRefusal("open", est.HeapBytesFor(opts), 0, opts.MemoryBudget)
}

// --- the Compact side ---

// compactWorkingSet models the additional heap a build holds while the image it
// replaces is still live.
//
// # What a compaction actually duplicates
//
// Not the store. The obvious model -- a compaction holds two images, so it needs
// twice the image -- is wrong by the largest term in it, and wrong in the
// direction that refuses work that would have fit by a factor of two on a store
// whose records carry blobs.
//
// buildSeq copies record *values*, and a record value is two slice headers. So
// every record the delta did not touch arrives in the new image addressing the
// same bytes the old image addresses: the same arena, or the same mapping. The
// property blobs and label sequences are not duplicated by a compaction at all,
// at any size. What is duplicated is the structure around them -- the record
// arenas that hold the headers, the page tables that address the arenas, the
// by-label posting lists, and the adjacency arrays -- and every one of those is
// a function of records and identifiers rather than of bytes written.
//
// The delta is the exception, and it is already counted: compactPin copies each
// delta record's properties with cloneBytes, so those bytes exist twice from the
// pin onward. They are in the plan term below.
//
// # Where each bound comes from
//
// The record arenas dominate, and their size is a page count. A page is
// materialised whole, so what matters is how many pages the new image touches,
// which is not knowable without walking the records the build is about to walk.
// Bounded instead: the pages the pinned image already has, plus at most one new
// page per delta record, capped by the identifier space the high-water marks
// declare. A compaction can only ever reduce the first term -- dropping a page
// whose every record was deleted is the reclamation compaction exists for -- so
// the bound holds and is loose by however much the compaction is about to
// reclaim.
//
// The label postings are counted from the labels themselves rather than modelled
// from a record count, for the reason CSRGraph.labelPostingBytes gives: a record
// carrying three labels is in three postings, and a model that said one would
// under-report exactly the store that has the most of them.
//
// The index's *entries* are absent, and that is a finding rather than an
// omission. Both payload forms stream: mappedIndexSource walks the live index a
// value at a time reusing one id buffer, and nodePropSeq does the same for the
// v8 encoding. Neither materialises the index, and serialisation streams into
// the temp file rather than through an image-sized buffer, so neither has a term
// here that follows the number of entries.
//
// What the mapped form does hold while it streams is the machinery: the value
// table's in-memory cap, the reverse sort's chunk, and the merge that drains it.
// Those are a constant -- 4,325,376 bytes at the default, and whatever
// CompactOptions.MaxWorkingBytes asked for otherwise -- and they are the last
// term below. Constant is not free: on a store small enough that the structure
// terms are a few hundred kilobytes it is most of the total, which is precisely
// the store whose budget is set low enough for the difference to decide
// something. The v8 encoding holds none of it.
func (p *compactPlan) compactWorkingSet() int64 {
	// The plan. Already allocated by the time this is asked -- compactPin has
	// returned -- but not counted by EstimateResident, which describes the
	// store's own structures and not a caller's copy of part of it. So it
	// belongs in what the operation needs rather than in what is held.
	total := int64(len(p.deltaNodes))*sizeofNodeRecord +
		int64(len(p.deltaEdges))*sizeofRawEdge +
		int64(len(p.deltaKnownNodes))*sizeofNodeID +
		int64(len(p.deltaKnownEdges))*sizeofEdgeID +
		p.deltaBytes

	// The image the build is about to produce, structure only.
	nodePages, edgePages := p.newPageBound()
	nodeSlots, edgeSlots := nodePages*csrPageSlots, edgePages*csrPageSlots

	total += nodeSlots*sizeofNodeRecord + edgeSlots*sizeofRawEdge
	total += (spacePages(p.nodeSeqHW) + spacePages(p.edgeSeqHW)) * sizeofDirEntry
	total += (nodePages + edgePages) * sizeofPageEntry
	total += (nodePages + edgePages) * sizeofLiveBefore

	total += p.newLabelPostingBound()

	// Adjacency, if this image is going to have any. Two offsets per slot plus a
	// sentinel, and two entries per live edge.
	if p.adjacency == AdjacencyEager {
		liveEdges := int64(len(p.deltaEdges))
		if p.csr != nil {
			liveEdges += int64(p.csr.EdgeCount())
		}
		total += (nodeSlots+1)*2*sizeofOffset + liveEdges*2*sizeofEdgeID
	}

	// The mapped index's intermediates, which are a setting rather than a
	// consequence of the store. Only the v9 build has them: under IndexResident
	// the image carries GIDX, which is written by a path that spills nothing.
	if p.indexMode == IndexMapped {
		total += p.buffers.workingBytes()
	}
	return total
}

// newPageBound is how many arena pages of each kind the new image can touch.
//
// The delta term charges one page per delta record, which is the worst case and
// is reached only by a delta whose identifiers are spread one to a page. Two
// pages per delta edge on the node side, because an edge extends the node
// directory to cover both its endpoints -- ordinarily endpoints that are already
// live and already counted, but a dangling endpoint materialises its page and the
// bound has to survive one.
//
// Both are capped by the identifier space, which is what keeps the bound sane
// rather than merely correct: past that cap the delta cannot be introducing new
// pages because there are none left to introduce.
func (p *compactPlan) newPageBound() (nodePages, edgePages int64) {
	if p.csr != nil {
		nodePages = int64(p.csr.nodePageCount())
		edgePages = int64(p.csr.edgePageCount())
	}
	nodePages += int64(len(p.deltaNodes)) + 2*int64(len(p.deltaEdges))
	edgePages += int64(len(p.deltaEdges))

	if space := spacePages(p.nodeSeqHW); nodePages > space {
		nodePages = space
	}
	if space := spacePages(p.edgeSeqHW); edgePages > space {
		edgePages = space
	}
	return nodePages, edgePages
}

// newLabelPostingBound is what the new image's by-label id lists can cost.
//
// Two terms, and they are bounded differently. The id lists are summed: the
// pinned image's exactly, because a compaction only ever drops records and a
// dropped record leaves every posting it was in; the delta's over its records'
// own label slices, because a record carrying three labels is in three postings
// and a model charging one per record would under-report exactly the store that
// declares the most. That sum is O(delta), outside the lock, against a pin that
// has just done an O(delta) copy.
//
// The maps themselves are counted from the label set rather than modelled from
// either count, and they are the reason this is not simply
// CSRGraph.labelPostingBytes plus the delta's ids. The new image declares the
// union of the two sides' labels, which can be strictly larger than the pinned
// image's: a delta introducing two label values it had never seen grows both maps,
// and charging the pinned image's map size instead under-reports by the
// difference. Small -- 62 bytes on the fixture that found it -- and in the
// direction the posture forbids, which is what makes it worth a term.
//
// Both loops over the pinned image are over distinct labels, of which a store has
// a handful, so this stays the constant-time reading its caller needs.
func (p *compactPlan) newLabelPostingBound() int64 {
	nodeLabels := make(map[store.NodeType]struct{})
	edgeLabels := make(map[store.EdgeType]struct{})
	var ids int64

	if p.csr != nil {
		for label, list := range p.csr.nodesByLabel {
			nodeLabels[label] = struct{}{}
			ids += int64(len(list)) * sizeofNodeID
		}
		for label, list := range p.csr.edgesByLabel {
			edgeLabels[label] = struct{}{}
			ids += int64(len(list)) * sizeofEdgeID
		}
	}
	for _, n := range p.deltaNodes {
		for _, label := range n.Labels {
			nodeLabels[label] = struct{}{}
		}
		ids += int64(len(n.Labels)) * sizeofNodeID
	}
	for _, e := range p.deltaEdges {
		for _, label := range e.Labels {
			edgeLabels[label] = struct{}{}
		}
		ids += int64(len(e.Labels)) * sizeofEdgeID
	}
	return ids + residentMapBytes(len(nodeLabels), 2+24) +
		residentMapBytes(len(edgeLabels), 2+24)
}

// checkCompactBudget refuses a compaction whose working set, on top of what the
// store already holds, exceeds Options.MemoryBudget.
//
// Called after the pin, which is the first moment the counts are real and the
// last moment before anything has been allocated for the build. Nothing has to be
// undone by a refusal here: the pin's release is already deferred, no temp file
// exists, and the store is left exactly as a caller who never called Compact
// would have found it.
//
// A refusal is not a failure, and the caller's side of that matters. It is
// returned before the compaction metric is recorded, for the reason
// ErrCompactionInProgress is: a background compactor that reports a refusal as a
// failure gives an operator an error rate made entirely of the trigger working.
func (s *Store) checkCompactBudget(p *compactPlan) error {
	if s.memBudget <= 0 {
		return nil
	}
	return budgetRefusal("compact", p.compactWorkingSet(),
		s.EstimateResident().Total, s.memBudget)
}
