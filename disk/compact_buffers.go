package disk

// One figure, divided.
//
// A compaction that writes a mapped index holds four intermediates whose sizes
// are a choice rather than a consequence: the GPIX value table's in-memory cap,
// the GPIR sort chunk, and the two bounds on the merge's read buffers. Every
// other allocation a compaction makes follows the store — the records it copies,
// the pages it fills, the postings it builds — and is what budget.go models and
// refuses on. These four follow nothing but the constants that set them.
//
// They were set small on purpose and they are documented that way where they are
// declared: memory is what this program spends down and sequential disk is what
// it is allowed to spend, so each is set where it bounds a transient rather than
// where it avoids a file. That is the right default and it is not the right
// answer for every machine, and until now there was no way to say so.
//
// MaxWorkingBytes is that way, and it is one figure rather than four because
// four would be four ways to get it wrong. The division below keeps the
// proportions the constants already stand in, so a caller raising the figure
// buys the same shape of compaction with more room in it, and a caller lowering
// it gets the same shape with less.
//
// # What it was worth, measured
//
// The plan that scheduled this option assumed the spill and the merge were a
// meaningful part of what a mapped index costs a compaction, and that buying
// them off would buy back some of the 15-24% it made compaction slower. Three
// interleaved rounds at 50,500 records say otherwise.
//
// Lowering the figure to the floor saved 3.32 MiB of allocation against a
// modelled 3.62 MiB, and cost 12%, 16% and 42% of the compaction's wall clock.
// Raising it to 64 MiB cost 38.04 MiB of allocation and moved the wall clock by
// +18%, 0% and -12% -- noise around zero. Fifteen times the memory bought
// nothing measurable, which is the finding: the intermediates are already cheap,
// in both directions, and this option is a small lever honestly labelled rather
// than a large one.
//
// Not measured, and the one shape where the answer could differ. That fixture
// writes thirteen indexed keys over 50,500 records, so the default chunk sorts
// it in about twenty runs; at the 28 million entries docs/MEMORY_MODEL.md
// projects it would be around 850. A merge forty times wider is not the merge
// that was timed, and the figures above should not be read as covering it.
//
// # What the figure is, exactly
//
// It is the peak of the four together, during the pass that fills them:
//
//	max(vtabMemCap, spillFlushBuffer)
//	  + 2 × (chunkBytes + max(chunkBytes, spillFlushBuffer) + spillFlushBuffer)
//
// The doubling is the two sorters, node and edge, which are filled together and
// live for the whole GPIX write. Each holds its chunk twice — once as the
// entries and once as the spill buffer sized to hold them when they are written
// out — and once more as the 64 KiB encode scratch. The value table is a third
// spill beside them, and is closed before GPIR is written.
//
// The merge is strictly smaller at every setting this file produces: it runs
// after the value table is closed and after the sorter being drained has
// released its chunk, so it costs the read budget plus what the other sorter is
// still holding — seven eighths of the part of the figure the division scales,
// against the whole of it during the fill, and the same fixed remainder in both.
// That is why the fill peak is the figure and the merge needs no term of its own.
//
// # The one term the figure does not bound
//
// A merge gives each run a buffer of min(mergeBudget/runs, maxRunBuffer) and
// never less than gpirMinRunBuffer, so a sort that produced more runs than
// mergeBudget/gpirMinRunBuffer costs 96 bytes per run rather than the budget.
// That threshold is 21,845 runs at the default, which is 715 million entries,
// and 2,048 runs at the floor, which is 6.3 million. Below the default, a store
// of that size pays the run count rather than the figure. This is a property of
// the merge as it has always been and not one the option introduces; it is
// stated because a bound whose exception is undocumented is worse than no bound.
//
// See gpir_sort.go and csr_gpix.go for what each of the four does, and spill.go
// for what a cap on an intermediate costs when it is exceeded.

import (
	"errors"
	"fmt"
	"math"
)

// CompactOptions bounds what a compaction holds while it builds.
//
// It is the second half of a pair and the two are not alternatives:
// Options.MemoryBudget refuses a compaction whose working set does not fit, and
// this sets how large part of that working set is. A caller who wants a store to
// compact in less memory sets this; a caller who wants to be told rather than to
// find out sets that.
type CompactOptions struct {
	// MaxWorkingBytes bounds the four intermediates a mapped-index compaction
	// sizes by choice rather than by the store: the GPIX value table, the GPIR
	// sort chunk, and the two bounds on the merge's read buffers. Zero takes the
	// default, which is 4,325,376 bytes and is exactly what every version before
	// this one held.
	//
	// The floor is 524,288. Below it the sort chunk falls under the spill's own
	// flush buffer and the figure stops naming what it bounds, so a smaller value
	// is refused at Open with ErrCompactWorkingBytes rather than quietly rounded
	// up — a bound that is silently not the bound asked for is the failure this
	// option exists to avoid. There is no ceiling short of the arithmetic's.
	//
	// Three things to know.
	//
	// It governs a v9 compaction only. Under IndexResident the image carries
	// GIDX, which is built by a different path holding none of these four, so the
	// figure is set and unused. StorageStats.IndexMode says which one a store is
	// taking.
	//
	// Lowering it costs wall clock and nothing else. Each of the four bounds an
	// intermediate that spills to a file in the store's own directory when it is
	// exceeded, so a smaller figure means more sequential IO and the same bytes
	// in the same order — the image a compaction produces is byte-identical at
	// every setting, and that is asserted rather than assumed.
	//
	// It is a small lever in both directions, and the figures are here so that a
	// caller can decide whether it is worth setting at all. At 50,500 records,
	// three interleaved rounds: the floor saved 3.32 MiB of allocation and cost
	// 12-42% of the compaction's wall clock, and 64 MiB cost 38.04 MiB and moved
	// the wall clock by -12% to +18%, which is noise. The default is 4 MiB
	// against a compaction whose working set docs/MEMORY_MODEL.md measures in
	// hundreds, so this is not where a memory problem is solved; and raising it
	// bought nothing on the shape measured, so it is not where a slow compaction
	// is fixed either. What it is for is a machine whose limit is tight enough
	// that four megabytes is worth three tenths of a second, and for that it is
	// exact.
	MaxWorkingBytes int64

	// MaxIndexTailBytes bounds the index mutation log a compaction records
	// while it builds, so that it can adopt its own output afterwards. Zero
	// takes the default, which is 16,777,216 bytes -- roughly 350,000
	// mutations, and exactly what every version before this one held.
	//
	// It is a second figure rather than a share of MaxWorkingBytes because it
	// bounds a different thing. Those four are intermediates the compaction
	// chooses the size of, and they scale with nothing: the same four bytes at
	// a thousand records and at a billion. This one is a log of what *other*
	// writers do while the build runs, so it scales with the write rate times
	// the build's duration and with neither the store nor the compaction.
	// Dividing one figure between two terms that move independently would give
	// a caller one knob that cannot be right for both.
	//
	// What it buys and what it costs, both exactly. A build during which more
	// than this many bytes of index mutations land drops the capture and falls
	// back to the behaviour that existed before captures: the compaction
	// completes, the image is correct, and the index does not give its memory
	// back until the store is reopened -- 83.8 MiB against 15.3 at the shape
	// docs/MEMORY_MODEL.md measures. A build during which fewer land holds
	// index.TailOpBytes -- 48 -- per mutation, transiently, and swaps the base.
	// So raising this is buying adoption with memory at 48 bytes a write, and
	// lowering it is the reverse.
	//
	// The floor is 65,536, which is 1,365 mutations. It is a weaker kind of
	// floor than MaxWorkingBytes's: that one is where the arithmetic stops
	// describing what is held, and this one is where the option stops being
	// able to do its job. Below it a capture is too small to cover a single
	// commit of any realistic size, so the store pays the recording cost on
	// every index mutation of every build and completes only for builds during
	// which nothing was written -- which is the gate that existed before
	// captures, bought rather than free. A caller who wants that gate wants
	// this option's default, not a figure near zero.
	MaxIndexTailBytes int64
}

// ErrCompactWorkingBytes is returned by Open when CompactOptions.MaxWorkingBytes
// is below the floor a compaction can be built within.
var ErrCompactWorkingBytes = errors.New("disk: the configured compaction working set is below the floor")

// ErrCompactIndexTailBytes is returned by Open when
// CompactOptions.MaxIndexTailBytes is below the floor a capture is useful at.
var ErrCompactIndexTailBytes = errors.New("disk: the configured index tail limit is below the floor")

const (
	// spillFlushBuffer is charged once per sorter. The value table's is covered
	// by its own cap, which the floor keeps above it.
	compactBufferFixedBytes = 2 * spillFlushBuffer

	// The part of the figure the division scales: the value table's cap plus four
	// chunks — two sorters, each holding its chunk as entries and again as the
	// buffer that writes them out.
	defaultCompactVariableBytes = gpixVtabMemCap + 4*gpirSortChunkEntries*gpirEntrySize

	// The default figure, spelled out: 4,325,376 bytes, 4.125 MiB.
	defaultCompactWorkingBytes = compactBufferFixedBytes + defaultCompactVariableBytes

	// minCompactWorkingBytes is where the sort chunk is still larger than the
	// flush buffer of the spill that writes it. Below that the model stops being
	// vtab + 4×chunk + 2×flush and the figure would over-promise, which is the
	// one direction a bound may not be wrong in.
	minCompactWorkingBytes = 512 << 10

	// maxCompactWorkingBytes keeps the division's products inside int64. A figure
	// above it is a typo rather than a working set: it is four thousand times the
	// default.
	maxCompactWorkingBytes = 1 << 34

	// defaultIndexTailBytes is what a compaction records before it gives up on
	// adopting its own output: index.TailOpBytes per mutation, so about 350,000
	// writes landing inside one build. That is a great many for a build measured
	// in milliseconds and not many for a whole-layer rebuild under a firehose.
	defaultIndexTailBytes = 16 << 20

	// minIndexTailBytes is 1,365 mutations. See MaxIndexTailBytes for why the
	// floor is here and why it is a softer argument than the other one.
	minIndexTailBytes = 64 << 10
)

// compactBuffers is the four sizes, resolved.
//
// A zero field takes that consumer's own default, which is how the tests set one
// threshold to a handful of bytes without having to state the other three.
type compactBuffers struct {
	vtabMemCap   int // GPIX value table, bytes held before it opens a file
	revChunk     int // GPIR sort, entries held and sorted at once
	mergeBudget  int // GPIR merge, all run readers together, bytes
	maxRunBuffer int // GPIR merge, one run reader, bytes
}

// compactBuffersFor divides one figure into four, keeping the proportions the
// constants stand in. A zero or negative figure takes the defaults, and does so
// by returning the zero value rather than by computing them: the consumers
// resolve their own defaults, and a division that reproduced them would be a
// second place for them to be written down.
//
// The figure must have passed CompactOptions.validate. Below the floor the
// arithmetic still produces four numbers and they no longer add up to what was
// asked, which is why the refusal is at Open and not here.
func compactBuffersFor(maxWorkingBytes int64) compactBuffers {
	if maxWorkingBytes <= 0 {
		return compactBuffers{}
	}
	if maxWorkingBytes > maxCompactWorkingBytes {
		maxWorkingBytes = maxCompactWorkingBytes
	}
	v := maxWorkingBytes - compactBufferFixedBytes

	scale := func(at int64) int64 { return at * v / defaultCompactVariableBytes }
	chunkBytes := scale(gpirSortChunkEntries * gpirEntrySize)
	return compactBuffers{
		vtabMemCap:   clampInt(scale(gpixVtabMemCap)),
		revChunk:     clampInt(chunkBytes / gpirEntrySize),
		mergeBudget:  clampInt(scale(gpirMergeReadBudget)),
		maxRunBuffer: clampInt(scale(gpirMaxRunBuffer)),
	}
}

// workingBytes is the peak the four reach together, which is the model
// CompactOptions.MaxWorkingBytes names and budget.go charges a compaction for.
// Every zero field is resolved to its default first, so this reports what the
// compaction will hold rather than what was written down.
//
// Truncation in the division makes this at most the figure asked for and never
// more, which is the direction a bound has to be wrong in.
func (b compactBuffers) workingBytes() int64 {
	b = b.resolved()
	chunk := int64(b.revChunk) * gpirEntrySize
	perSorter := chunk + max(chunk, spillFlushBuffer) + spillFlushBuffer
	return max(int64(b.vtabMemCap), spillFlushBuffer) + 2*perSorter
}

// resolved fills every zero field with the default its consumer would have used.
func (b compactBuffers) resolved() compactBuffers {
	if b.vtabMemCap <= 0 {
		b.vtabMemCap = gpixVtabMemCap
	}
	if b.revChunk <= 0 {
		b.revChunk = gpirSortChunkEntries
	}
	if b.mergeBudget <= 0 {
		b.mergeBudget = gpirMergeReadBudget
	}
	if b.maxRunBuffer <= 0 {
		b.maxRunBuffer = gpirMaxRunBuffer
	}
	return b
}

// validate refuses a figure a compaction cannot be built within.
//
// Checked at Open, before the directory is touched, because it is a property of
// the configuration and not of the store: a setting that can never be honoured
// should stop the store coming up rather than surface on the first compaction,
// which under AutoCompact is a background tick nobody is watching.
func (o CompactOptions) validate() error {
	if o.MaxIndexTailBytes != 0 && o.MaxIndexTailBytes < minIndexTailBytes {
		return fmt.Errorf("%w: MaxIndexTailBytes is %d, and the floor is %d",
			ErrCompactIndexTailBytes, o.MaxIndexTailBytes, minIndexTailBytes)
	}
	if o.MaxWorkingBytes == 0 {
		return nil
	}
	if o.MaxWorkingBytes < minCompactWorkingBytes {
		return fmt.Errorf("%w: MaxWorkingBytes is %d, and the floor is %d",
			ErrCompactWorkingBytes, o.MaxWorkingBytes, minCompactWorkingBytes)
	}
	return nil
}

// indexTailBytes resolves what a compaction will record, which is the default
// where nothing was asked for.
func (o CompactOptions) indexTailBytes() int64 {
	if o.MaxIndexTailBytes <= 0 {
		return defaultIndexTailBytes
	}
	return o.MaxIndexTailBytes
}

// clampInt narrows to int without wrapping, which matters only on a 32-bit
// platform and matters absolutely there: a wrapped cap is a negative one, and a
// negative one reads as "take the default" at every consumer.
func clampInt(v int64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	if v < 0 {
		return 0
	}
	return int(v)
}
