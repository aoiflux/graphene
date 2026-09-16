package disk

// Refusing a batch that is larger than the store can afford to hold.
//
// # What was unbounded
//
// AddNodesBatch and AddEdgesBatch accept a slice of any length. Each allocates
// an ID slice, a full copy of every record's properties and labels, and a framed
// WAL batch, all proportional to what the caller passed, and all under one lock
// hold. A caller handing ten million nodes to one call gets ten million nodes'
// worth of copies and no warning. Nothing in the engine bounded that: MemoryBudget
// gates Open and Compact, DeltaSoftLimit reports after the fact, and
// CompactionPolicy is evaluated between commits rather than inside one.
//
// # Two dimensions, whichever comes first
//
// A byte cap alone does not bound a batch of ten million empty nodes: each is 80
// bytes of structure -- a version cell and a record header -- and carries no
// payload at all, so the byte figure stays small while the allocation does not.
// A record cap alone does not bound a batch of ten records carrying a gigabyte
// each. Both are needed, and a batch crossing either is refused.
//
// This is the shape Badger arrived at for the same problem. Its Txn.checkSize
// counts entries and bytes and returns ErrTxnTooBig when either reaches its cap,
// and the caps themselves are derived from the memory configuration rather than
// set beside it -- maxBatchSize is a fraction of MemTableSize and maxBatchCount
// is that figure divided by the largest a single entry can be. A caller who
// turns memory down gets smaller batches without making a second decision, and
// the two settings cannot be put into disagreement. Both properties are worth
// having and both are reproduced below.
//
// # It is a refusal, not a split
//
// AddNodes documents that either every node is added or none is. Splitting an
// oversized batch here would quietly break that for the callers relying on it,
// so the batch is refused and the caller is told the two figures it needs to
// split by. The auto-splitting writer is a separate API with a separate contract
// -- Badger draws the same line, where WriteBatch retries around the refusal and
// is documented as not transactional.
//
// store.Budget takes the same position for a traversal, in the same words: "It
// is a refusal, not a truncation: a partial answer that looks like a complete
// one is the failure mode this exists to prevent."
//
// # Nothing is imposed on a store that asked for no budget
//
// A derived cap exists only where there is a budget to derive it from. A store
// opened with no MemoryBudget, and no explicit cap, is bounded exactly as it was
// before this file existed. Turning an unbounded call into a refusing one on
// nothing more than an upgrade would break working programs to enforce a figure
// their author never chose -- the same silent reinterpretation of a shipped
// default that was refused for what MaxDeltaBytes counts.

import (
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// ErrBatchTooLarge reports a batch refused for exceeding a configured cap.
//
// Errors returned wrap a *BatchTooLargeError carrying both caps and both
// measured figures, so a caller can split by the one that actually bound.
var ErrBatchTooLarge = errors.New("disk: this batch exceeds the configured batch limit")

// BatchTooLargeError is a refusal with its arithmetic attached, in the shape
// MemoryBudgetError uses and for the same reason: a refusal that does not say by
// how much cannot be acted on.
type BatchTooLargeError struct {
	// Op is the call that refused: "AddNodesBatch" or "AddEdgesBatch".
	Op string

	// Records and Bytes are what the batch was measured at.
	//
	// Bytes is the same accounting the delta uses for the same records -- see
	// nodeVersionBytes -- so this figure and MaxDeltaBytes are in the same units
	// and can be reasoned about together.
	//
	// The measurement stops at the first record that crosses a cap, so these are
	// the figures at the point of refusal rather than totals for the whole
	// slice. A batch ten times over the limit is refused after reading a
	// limit's worth of it, which is the point: walking ten million records to
	// report how far over they are would do the work the refusal exists to
	// avoid.
	Records int
	Bytes   int64

	// MaxRecords and MaxBytes are the caps in force. Zero means that dimension
	// is not capped.
	MaxRecords int
	MaxBytes   int64
}

func (e *BatchTooLargeError) Error() string {
	switch {
	case e.MaxRecords > 0 && e.Records >= e.MaxRecords:
		return fmt.Sprintf("%s: %s was given at least %d records, limit is %d (Options.MaxBatchRecords); split the batch",
			ErrBatchTooLarge.Error(), e.Op, e.Records, e.MaxRecords)
	default:
		return fmt.Sprintf("%s: %s was given at least %d bytes in %d records, limit is %d (Options.MaxBatchBytes); split the batch",
			ErrBatchTooLarge.Error(), e.Op, e.Bytes, e.Records, e.MaxBytes)
	}
}

// Unwrap gives a caller one condition to test, the shape MemoryBudgetError uses.
func (e *BatchTooLargeError) Unwrap() error { return ErrBatchTooLarge }

// ErrBatchCapUnusable reports a batch cap so small that no batch could ever
// pass it, including a batch of one record.
//
// Refused at Open rather than on the first write, for the reason
// CompactOptions.validate is: a configuration error that first surfaces on a
// background write surfaces to nobody. It is arithmetic on the Options alone, so
// it costs nothing and can come before the directory is touched.
var ErrBatchCapUnusable = errors.New("disk: Options.MaxBatchBytes is below the cost of a single record, so no batch could ever be accepted")

// validateBatchCaps refuses a byte cap that cannot admit one empty record.
//
// Only that case. A cap that admits one record and not two is a legitimate, if
// unusual, setting -- a caller who wants one record per commit is entitled to
// ask for it -- and a record cap of one is likewise a real configuration. What
// is not is a store that refuses every possible batch, which is a store nothing
// can write to.
func validateBatchCaps(opts Options) error {
	if opts.MaxBatchBytes > 0 && opts.MaxBatchBytes < minNodeBatchCost {
		return fmt.Errorf("%w: %d bytes, and an empty node costs %d",
			ErrBatchCapUnusable, opts.MaxBatchBytes, minNodeBatchCost)
	}
	return nil
}

// batchCaps is the pair a store enforces, resolved once at Open.
type batchCaps struct {
	records int
	bytes   int64
}

// The derivation, and both figures are provisional.
//
// batchBudgetPercent is what fraction of the memory budget one batch may hold.
// Badger's equivalent is 15% of MemTableSize, but MemTableSize is a write buffer
// and MemoryBudget is the whole store, so the same fraction of a much larger
// figure would be far too loose. Two percent of a 2 GiB budget is about 41 MiB,
// which is generous for a batch and small enough that a batch is never the term
// that decides whether a store fits.
//
// It has not been tuned against a measurement. Phase 0c of
// docs/PLAN_BOUNDED_INGEST.md is what picks the final figure, and until it does
// this is a bound chosen to be obviously safe rather than one chosen to be
// right.
const batchBudgetPercent = 2

// minNodeBatchCost is what one record costs with no payload at all: a version
// cell and a record header. Dividing the byte cap by it is how the record cap is
// derived, and it is Badger's arithmetic exactly -- maxBatchCount is maxBatchSize
// divided by the size of one skiplist node.
//
// The node figure is used for both kinds because it is the smaller of the two,
// so the record cap it yields is the tighter one. An edge costs 104 bytes empty
// against a node's 80; deriving from the edge would admit more records than the
// byte cap was meant to allow for.
const minNodeBatchCost = sizeofNodeVersion + sizeofNode

// batchCapsFor resolves the caps a store enforces.
//
// An explicit cap is used as given, on each dimension independently: a caller
// who sets only MaxBatchBytes gets a derived record cap beside it if there is a
// budget to derive one from, and no record cap at all if there is not.
//
// Called after resolveMemoryBudget, so a discovered budget bounds batches too --
// which is the whole point of deriving rather than defaulting. A store that
// found a 2 GiB cgroup limit refuses a batch that a store on a 64 GiB machine
// accepts, without either caller having written a figure down.
func batchCapsFor(opts Options) batchCaps {
	caps := batchCaps{records: opts.MaxBatchRecords, bytes: opts.MaxBatchBytes}
	if caps.bytes < 0 {
		caps.bytes = 0
	}
	if caps.records < 0 {
		caps.records = 0
	}

	if caps.bytes == 0 && opts.MemoryBudget > 0 {
		caps.bytes = opts.MemoryBudget / 100 * batchBudgetPercent
	}
	if caps.records == 0 && caps.bytes > 0 {
		caps.records = int(caps.bytes / minNodeBatchCost)
	}
	return caps
}

// none reports whether nothing is capped, so the check can be skipped entirely
// rather than walking a slice to decide it has no limit to compare against.
func (c batchCaps) none() bool { return c.records <= 0 && c.bytes <= 0 }

// checkNodes refuses a node batch over either cap.
//
// Walks the slice summing as it goes and stops at the first record that crosses,
// so the cost of refusing a wildly oversized batch is proportional to the limit
// rather than to the batch. A batch that fits is walked once, which is one pass
// over headers the caller is about to have copied anyway.
//
// # A batch of one is always admitted
//
// These caps bound how many records are grouped into one commit, not how large a
// record may be. A single record over the byte cap is refused by nothing else --
// AddNode takes it without a word -- so refusing it here would mean the same
// record is writable through one call and unwritable through another, which is
// not a bound, it is an inconsistency.
//
// It also has to be this way for the splitting adders to terminate: a chunk of
// one is the smallest they can cut, and a cap that refuses it leaves a record
// that can never be written and a loop with nothing smaller to try.
func (c batchCaps) checkNodes(op string, nodes []*store.Node) error {
	if c.none() || len(nodes) <= 1 {
		return nil
	}
	var bytes int64
	for i, n := range nodes {
		records := i + 1
		if c.records > 0 && records > c.records {
			return &BatchTooLargeError{
				Op: op, Records: records, Bytes: bytes,
				MaxRecords: c.records, MaxBytes: c.bytes,
			}
		}
		bytes += nodeVersionBytes(n)
		if c.bytes > 0 && bytes > c.bytes {
			return &BatchTooLargeError{
				Op: op, Records: records, Bytes: bytes,
				MaxRecords: c.records, MaxBytes: c.bytes,
			}
		}
	}
	return nil
}

// nodeChunk returns how many leading records of nodes fit inside the caps, and
// is what the splitting adders cut on.
//
// At least one record is always returned for a non-empty slice, even when that
// single record is itself over the byte cap -- which checkNodes admits, for the
// reason given there. A chunk of zero would be a caller looping forever on a
// record nothing can write.
func (c batchCaps) nodeChunk(nodes []*store.Node) int {
	if c.none() || len(nodes) == 0 {
		return len(nodes)
	}
	var bytes int64
	for i, n := range nodes {
		if c.records > 0 && i+1 > c.records {
			return i
		}
		bytes += nodeVersionBytes(n)
		if c.bytes > 0 && bytes > c.bytes && i > 0 {
			return i
		}
	}
	return len(nodes)
}

// edgeChunk is nodeChunk for edges.
func (c batchCaps) edgeChunk(edges []*store.Edge) int {
	if c.none() || len(edges) == 0 {
		return len(edges)
	}
	var bytes int64
	for i, e := range edges {
		if c.records > 0 && i+1 > c.records {
			return i
		}
		bytes += edgeVersionBytes(e)
		if c.bytes > 0 && bytes > c.bytes && i > 0 {
			return i
		}
	}
	return len(edges)
}

// checkEdges is checkNodes for edges.
func (c batchCaps) checkEdges(op string, edges []*store.Edge) error {
	if c.none() || len(edges) <= 1 {
		return nil
	}
	var bytes int64
	for i, e := range edges {
		records := i + 1
		if c.records > 0 && records > c.records {
			return &BatchTooLargeError{
				Op: op, Records: records, Bytes: bytes,
				MaxRecords: c.records, MaxBytes: c.bytes,
			}
		}
		bytes += edgeVersionBytes(e)
		if c.bytes > 0 && bytes > c.bytes {
			return &BatchTooLargeError{
				Op: op, Records: records, Bytes: bytes,
				MaxRecords: c.records, MaxBytes: c.bytes,
			}
		}
	}
	return nil
}
