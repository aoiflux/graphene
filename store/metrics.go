package store

// Telling someone else what the engine is doing.
//
// There is no logging and no metrics anywhere in library code, and that is a
// deliberate position rather than an omission: an embedded engine that writes to
// a logger of its choosing has made a decision that belongs to the program
// embedding it. What it can do is offer a place to attach one.
//
// # Why one method and one struct
//
// The alternative shapes were a method per event and a stringly-typed
// counter/gauge pair. A method per event makes every new measurement a breaking
// change to an exported interface, which is the wrong trade for something whose
// whole purpose is to grow. A string-keyed sink allocates on the hot path and
// pushes the meaning of every number into documentation that nothing checks.
//
// One method taking a concrete struct is neither: adding a MetricKind is
// additive, the struct is passed by value so nothing escapes to the heap, and
// what each field means for each kind is written down below where a caller
// reading the type can see it.
//
// # What it costs when unset
//
// Nothing. Every call site is inside `if m != nil`, and the fields that cost
// something to compute — the clock reads, above all — are computed inside that
// branch. A store with no sink runs the code it ran before this existed.
//
// # What is deliberately not measured
//
// GetNode and the other point reads. That path is ~6 ns and lock-free; two clock
// reads would be an order of magnitude more than the work, and a measurement
// that dominates the thing it measures is not an observation. Query, commit,
// sync, compaction and replay are all far enough above the noise floor to carry
// a timer, and between them they account for where a store's time actually goes.

import (
	"time"
)

// MetricKind identifies what a Metric describes.
//
// New kinds are added to the end. A sink that does not recognise one should
// ignore it rather than fail: the engine will emit kinds a caller compiled
// against an older version has never heard of.
type MetricKind uint8

const (
	// MetricCommit is one committed batch, recorded after the epoch carrying it
	// becomes visible — so the duration includes the wait for the group's fsync
	// and is what a caller actually experienced.
	MetricCommit MetricKind = iota + 1

	// MetricSync is one fsync of the write-ahead log.
	//
	// Count is how many commits it made durable, which is the number that says
	// whether group commit is working: one sync per commit means it is not.
	MetricSync

	// MetricCompaction is one completed compaction, successful or not.
	MetricCompaction

	// MetricQuery is one resolved QueryNodeIDs or QueryEdgeIDs, including the
	// planning, the driving step and the residual pass.
	MetricQuery

	// MetricReplay is the write-ahead log replay one Open performed. Emitted
	// once per open, and the best single indicator of how overdue a compaction
	// is: the log is bounded only by compaction, so its size is also how long
	// the next open will take.
	//
	// Count is epochs advanced rather than records, because that is what the
	// replay actually applied — a torn tail contributes bytes and no epoch,
	// which is the distinction worth being able to see.
	MetricReplay

	// MetricSnapshotOpen and MetricSnapshotClose bracket a snapshot's life.
	// Count on the close is how long it was held, in nanoseconds, so a sink can
	// find the leaked one without keeping state of its own.
	MetricSnapshotOpen
	MetricSnapshotClose

	// MetricBackup is one completed Backup, successful or not.
	MetricBackup

	// MetricRefresh is one live reader's Refresh. Count is epochs advanced and
	// Bytes is how much log it applied — a reload, which re-reads the log from
	// the start, shows up as a large one.
	MetricRefresh

	// MetricIDHeadroomLow is emitted by a completed compaction when the
	// identifiers still unissued have fallen below Options.IDHeadroomWarn.
	// Count is the highest identifier issued for that kind and Examined is the
	// ceiling; the kind is named in Err's place by a separate audit entry, so a
	// sink reading only metrics sees two emissions, nodes and edges, with
	// different counts.
	//
	// Appended at the end: a kind's number is what a sink's own storage records,
	// and inserting one anywhere else renumbers every kind after it.
	MetricIDHeadroomLow

	// MetricImageFallback is one open that asked for a mapped image and did not
	// get one. Err names why: the platform has no mapping primitive, the store
	// holds no process lock and the mode requires one, the file is empty or too
	// large to address, or the mapping call itself failed.
	//
	// It exists because the fallback is correct, silent, and expensive. A store
	// reading its whole image into the heap when it was configured not to is a
	// memory regression with no symptom other than the memory, so it is reported
	// once, at the moment the decision is taken. StorageStats.ImageMode is the
	// same fact for a caller that did not attach a sink.
	MetricImageFallback

	// MetricIndexFallback is one open that asked for a property index read out of
	// the image and rebuilt it in the heap instead. Err names why: the mode is
	// IndexResident, the image is in the heap, where an index read in place would
	// pin the whole file to save part of it, or the image predates the format
	// that carries a mappable index.
	//
	// That third cause used to be excluded, and this said so: "a file that simply
	// carries no such index — anything written before the format could hold one —
	// is not a fallback and emits nothing. There is nothing in that file to read
	// in place, and the next compaction is what produces one." Every sentence of
	// that is true and the conclusion was wrong. The test is not where the cause
	// lies, it is what the caller is paying, and an old image is the most
	// expensive of the three: the entries load one at a time, which measured
	// about sevenfold on the open, and then sit in the heap at about a hundred
	// bytes each. It was also the only one with no in-process symptom at all —
	// IndexMode said "resident" with no reason attached and ImageMode said
	// "mapped", because the image maps perfectly well. Since v0.8.0 it is
	// reported, and its Err is the one that names a remedy.
	MetricIndexFallback

	// MetricDeltaOverBudget is emitted when the delta's records first reach
	// Options.DeltaSoftLimit. Bytes is what they hold and Examined is the limit.
	//
	// Once per crossing, not once per commit: a store sitting above its limit
	// while the compaction it needs is scheduled would otherwise emit an event
	// per write. A compaction that brings the delta back under the limit re-arms
	// it, so a store that crosses, compacts and crosses again reports twice.
	//
	// Nothing fails when this fires -- it is the advisory half of a soft limit.
	// See StorageStats.DeltaOverBudget for the polled form, and
	// CompactionPolicy.MaxDeltaBytes for the figure that acts rather than reports.
	//
	// Appended at the end, per MetricIDHeadroomLow's note.
	MetricDeltaOverBudget

	// MetricCompactPin, MetricCompactBuild and MetricCompactCommit are the three
	// stages of one compaction, emitted as each finishes. MetricCompaction still
	// reports the whole, and these say where inside it the memory went.
	//
	// They exist because the peak of a compaction was known only as a single
	// figure for the whole operation -- measured at roughly 2.6x the modelled
	// heap during a rebuild (docs/MEMORY_MODEL.md section 9.8) -- and "2.6x"
	// spread over three stages is not something an engineer can act on. The pin
	// copies the delta under the lock, the build materialises and serialises a
	// whole new image with no lock held, and the commit splices and publishes.
	// They hold very different amounts and only one of them can be made smaller
	// without changing what a compaction guarantees.
	//
	// Count is the modelled resident total, Examined is the process's anonymous
	// bytes and Bytes is its peak resident, all read at the moment the stage
	// ended. The peak never resets, so the stage whose Bytes exceeds the
	// previous stage's is the stage that moved the high-water mark -- which is
	// the question these were added to answer.
	//
	// A stage that failed still emits, with Err set: a compaction that ran out
	// of memory in the build is precisely the event worth having a measurement
	// of, and one that reported nothing because it failed would be missing its
	// most useful emission.
	//
	// The figures come from the operating system where it will answer and are
	// zero where it will not -- darwin reports only a peak, and platforms
	// outside linux, windows and darwin report nothing. A zero here means "not
	// measurable", never "no memory held"; see StorageStats.Process and its
	// Known, Split and Current flags for the same distinction in polled form.
	MetricCompactPin
	MetricCompactBuild
	MetricCompactCommit

	// MetricResidentAdvice is one piece of advice about a mapping that was asked
	// for and not given. Count is the advice, Bytes is the mapping it was to
	// apply to, and Err says what happened.
	//
	// Only failures are emitted. Advice that worked is the normal case and
	// saying so on every compaction would drown the sink; advice that did not is
	// the reportable event, because disk.Options.ResidentAdvice is then an
	// option asked for and not held. A platform with no advice to give -- windows
	// and everything outside linux and darwin -- reports through this on the
	// first attempt rather than staying quiet, for the reason
	// MetricIndexFallback's comment arrives at: what matters is what the caller
	// is paying, not where the cause lies.
	//
	// Nothing fails when this fires. Advice is a hint, and a compaction that
	// could not give one has still compacted.
	//
	// Appended at the end, per MetricIDHeadroomLow's note.
	MetricResidentAdvice
)

// String names the kind, for a sink that labels its output.
func (k MetricKind) String() string {
	switch k {
	case MetricCommit:
		return "commit"
	case MetricSync:
		return "sync"
	case MetricCompaction:
		return "compaction"
	case MetricQuery:
		return "query"
	case MetricReplay:
		return "replay"
	case MetricSnapshotOpen:
		return "snapshot-open"
	case MetricSnapshotClose:
		return "snapshot-close"
	case MetricBackup:
		return "backup"
	case MetricRefresh:
		return "refresh"
	case MetricIDHeadroomLow:
		return "id-headroom-low"
	case MetricImageFallback:
		return "image-fallback"
	case MetricIndexFallback:
		return "index-fallback"
	case MetricDeltaOverBudget:
		return "delta-over-budget"
	case MetricCompactPin:
		return "compact-pin"
	case MetricCompactBuild:
		return "compact-build"
	case MetricCompactCommit:
		return "compact-commit"
	case MetricResidentAdvice:
		return "resident-advice"
	default:
		return "unknown"
	}
}

// Metric is one thing the engine did.
//
// The numeric fields mean something different per kind, and the table is the
// contract. A field not listed for a kind is zero.
//
//	kind             Count                    Examined                 Bytes
//	---------------------------------------------------------------------------------
//	commit           records in the batch     —                        framed log bytes
//	sync             commits made durable     —                        log bytes synced
//	compaction       records in the image     records scanned          image size
//	query            IDs returned             candidates examined      —
//	replay           epochs advanced          —                        log bytes read
//	snapshot-open    —                        —                        —
//	snapshot-close   nanoseconds held         —                        —
//	backup           files copied             —                        bytes copied
//	refresh          epochs advanced          —                        log bytes applied
//	id-headroom-low  highest ID issued        the ID ceiling           -
//	compact-pin      modelled resident bytes  process anonymous bytes  process peak resident
//	compact-build    modelled resident bytes  process anonymous bytes  process peak resident
//	compact-commit   modelled resident bytes  process anonymous bytes  process peak resident
//	resident-advice  the advice, as an enum   —                        mapping bytes it covered
//
// Duration is wall-clock for the operation, measured around the work rather than
// around the whole call, and is zero for the two snapshot kinds. Err is non-nil
// when the operation failed, and a failed operation is still recorded — an error
// rate is a metric, and a sink that only ever hears about successes cannot
// compute one.
type Metric struct {
	Kind     MetricKind
	Duration time.Duration
	Count    int64
	Examined int64
	Bytes    int64
	Err      error
}

// Metrics receives what the engine did. Nil is the default and costs nothing.
//
// Record is called from whichever goroutine performed the work, several at once.
// An implementation must be cheap, must not block, and must never call back into
// the store — doing so from inside a commit deadlocks the store against itself.
//
// Every emission but one happens with no store lock held, because a sink is
// caller code and holding a lock across it would let a slow one stall the
// writers it is meant to be measuring. The exception is MetricSync, which is
// emitted inside the log's write lock: the duration of an fsync is only knowable
// where the fsync happens, and that is the one place a committer's own bytes are
// being written. A sink that blocks there blocks every commit in flight.
type Metrics interface {
	Record(m Metric)
}

// MetricsFunc adapts a plain function to Metrics.
//
// The interface exists rather than a bare func field for the same reason
// CompactionObserver does: disk.Options is a comparable value, and a struct with
// a func field is not.
type MetricsFunc func(m Metric)

// Record calls f.
func (f MetricsFunc) Record(m Metric) { f(m) }
