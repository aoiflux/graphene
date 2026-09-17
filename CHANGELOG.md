# Changelog

Release notes start here. Tags v0.1 through v0.4.0 predate this file; use
`git log` for those.

## Unreleased (v0.9.0) — an ingest the engine bounds, and a ceiling it can find for itself

The question this release answers is "what if I bulk-ingest ten million nodes?".
v0.8.0 made a whole-layer *rebuild* fit under 2 GiB. An ingest is the other
workload and nothing in the engine bounded it: `AddNodesBatch` took a slice of any
length, `MemoryBudget` gated `Open` and `Compact` and nothing in between, and a
`bulk` import accumulated an entire dump before anything folded it into an image.
Worse, the configuration §9.8 measured passing was undocumented, and the one
`USER_GUIDE.md` recommended was the arm that died.

### Inputs the engine refuses, on two dimensions

- **`disk.Options.MaxBatchBytes` and `MaxBatchRecords` cap one `AddNodesBatch` or
  `AddEdgesBatch`, and a batch over either is refused with `disk.ErrBatchTooLarge`.**
  The wrapped `*disk.BatchTooLargeError` carries both caps and both measured
  figures, so a caller can split by the one that actually bound. Two dimensions
  because neither bounds both shapes: a byte cap does not bound ten million empty
  nodes at 80 bytes of structure each, and a record cap does not bound ten records
  carrying a gigabyte apiece.

- **The check runs before any identifier is taken and before the lock, and stops
  at the first record that crosses.** A refusal burns nothing, and a batch ten
  times over the limit is refused after reading a limit's worth of it — walking ten
  million records to report how far over they are would do the work the refusal
  exists to avoid. The figures reported are therefore *at least*, not totals.

- **The caps derive from the memory budget rather than sitting beside it.** Left at
  zero, `MaxBatchBytes` takes a fraction of the budget in force and `MaxBatchRecords`
  comes out of that divided by the cost of one empty record. This is Badger's
  arrangement and its reason: a caller who turns memory down gets smaller batches
  without a second decision, and the two settings cannot be put into disagreement.
  A byte cap below the cost of a single record is refused at `Open` with
  `disk.ErrBatchCapUnusable`, not on the first write, where a configuration error
  surfaces to nobody.

- **With no budget at all, zero caps nothing.** An upgrade does not start refusing
  batches a program has always passed.

- **`AddNodesInBatches` / `AddEdgesInBatches` split instead of refusing, on the
  store and on `Graph` — and they are not atomic.** That is the whole trade, and it
  is why they are a separate call rather than a kinder `AddNodes`: `AddNodes`
  documents that either every node is added or none is, and two commits are two
  commits. A failure partway through returns the identifiers already committed
  alongside the error, because stranding records the caller has no identifier for
  is worse than a partial answer clearly labelled as one. Badger draws the line in
  the same place, between `Txn` and `WriteBatch`, and says the same thing about it.

### The engine can read the ceiling it is already under

- **`disk.Options.DiscoverMemoryBudget` derives `MemoryBudget` from the
  environment**: a cgroup v2 `memory.max` or v1 `memory.limit_in_bytes` on linux, a
  Job Object memory limit on windows, machine size on either when there is no limit,
  and `hw.memsize` alone on darwin, which enforces no ceiling over the class this
  engine cares about. Before this, every bound was a figure the caller had to know
  to supply.

- **It is a flag and not a redefinition of zero.** Zero `MemoryBudget` has meant
  unlimited since the option existed, and quietly changing that would mean a store
  that opened yesterday refusing to open today against a figure nobody configured —
  the same silent reinterpretation of a shipped default that was refused for what
  `MaxDeltaBytes` counts. An explicitly set budget always wins and this flag then
  does nothing; discovery can neither raise a stated figure nor lower one.

- **Only a fraction of the discovered ceiling is taken** — 50% of an enforced
  limit, 25% of machine size — because the budget is a whole-store figure and
  nothing here knows what else the program has allocated. A derived budget is always
  strictly below the ceiling it came from. Neither fraction is yet tuned against a
  measurement, and both are deliberately conservative until one exists.

- **`StorageStats.MemoryBudgetSource` names the instrument verbatim**, so an
  operator looking at a store that refused a compaction against a number nobody
  configured can see which file or call produced it. A budget with an empty source
  means the caller set it. Asking for discovery on a platform that answered nothing
  reports zero and `""` rather than inventing a ceiling.

### A fifth compaction rule, which sees what the other four cannot

- **`store.CompactionPolicy.MaxResidentBytes` fires on
  `StorageStats.EstimatedResidentBytes`.** The four existing rules watch the delta
  and the log; the property index those same writes created is in neither. At one
  measured shape a delta reporting **338 bytes per record was holding 1,893**, and
  rebuilding without compacting, at 400,000 records the resident property index was
  **546.4 MiB against the delta's 295.0** — the term the byte rule cannot see was
  1.85× the term it can.

- **It is additive, not a replacement.** What `MaxDeltaBytes` counts is unchanged,
  because moving it would silently retune every deployment already calibrated
  against it. A caller who sets both gets whichever fires first. Off by default.

- **It is a floor, not a budget**, and its doc comment says so: retained heap runs
  well under what the process charges — roughly 2.6× during a rebuild, 1.19–1.54× at
  rest. A caller with a 2 GiB ceiling does not set this to 2 GiB.

### A compaction that ran while anything was writing now gives its memory back

- **`adoptCompactedIndex` was gated on the WAL tail not having grown**, so a
  compaction under concurrent writes adopted nothing — which is every compaction
  during a bulk ingest, the one workload the bound exists for. `index.Tail` now
  records the mutations applied across a build and `SwapBase` replays them over the
  base it installs, so the precondition is met by reconstruction rather than by
  waiting for a quiet moment.

- **Not the shard split the plan named.** A retraction is a bit in
  `baseState.nodeGone`, not a shard entry, so a second set of shards would resurrect
  an entity deleted during the build. The log is fixed 1,024-op blocks rather than
  one growing slice: a growing one cost 269 B of garbage per mutation against the 48
  it keeps and put the registration path at 1.2–1.3×; in blocks the extra allocation
  is exactly `TailOpBytes` and the wall clock is inside the noise.

- **A latent bug closed alongside it.** A declaration takes the store lock but is
  not journaled, so `tail == 0` never implied that no declaration had happened, and
  one landing during a quiet build adopted onto an image whose GORD does not name
  the key. Adoption is now gated on index identity, capture completeness, and the
  declared key sets matching what went into the image.

### An import is bounded by default at the command line, and unchanged in the library

- **`bulk.Options` gains `MaxBatchBytes`, a `Compact` schedule and a `Reopen`
  hook.** `BatchSize` counts records, which is the wrong unit as soon as records
  vary in size — and a dump is precisely where the caller does not know which they
  have, because they are importing it to find out. Both limits are enforced and
  whichever is reached first flushes.

- **`Reopen` is a callback and not a flag because the handle belongs to the
  caller.** A compaction writes a new image and then goes on serving those records
  out of the heap, since a base is attached on the load path and a compaction is not
  one; reopening is what gives the bytes back. But reopening closes the handle, which
  is the thing the caller will use afterwards, so the caller performs the reopen and
  hands back what to write to next.

- **`graphene import graph` now defaults to the configuration that was measured
  passing**: `-bound 32` with a reopen after every interim compaction, where the
  shipped 128 MiB delta default died at 1,200,000 records holding 1,843 MiB under a
  2 GiB ceiling. `-bound 0` restores the unbounded behaviour for whoever has
  measured that it is faster on a dump that fits. **`store.DefaultCompactionPolicy`
  is unmoved** — an embedder gets exactly what they had.

### What a bulk ingest costs, measured on both arms

- **The quadratic is now a measurement rather than arithmetic in a plan.** The
  ceiling harness gains `TestCeiling_BulkIngestFitsUnderTheLimit`, which fills an
  empty store under a real limit and sums `MetricCompaction.Bytes` and
  `MetricCommit.Bytes` — figures the engine already emitted and nobody added up.
  `docs/MEMORY_MODEL.md` §9.9 carries the ladder.

- **The two arms bracket the whole axis.** At the 3.2 KB records this programme
  targets, unbounded holds memory *exactly* linear — 510.3 MiB at 100k, 1,010.0 at
  200k, dead at 400k — and writes at a flat 1.81×. Bounded at 32 MiB with a reopen
  holds **flat across a fourfold size range** (94.6, 76.3, 98.2 MiB) and pays 1.80×
  more amplification per doubling, twice: 6.53× → 11.77× → 21.24×.

- **So no setting of `MaxDeltaBytes` moves off that line**, it only moves your
  position on it. A one-pass loader is the only thing that changes the curve's shape,
  and that is now a measured conclusion rather than a projected one.

- **Bounding is *slower* on an ingest** — 1.8× at 100k, 3.0× at 200k — because wall
  clock follows bytes written. §9.8 found the exact opposite on a rebuild, and both
  are true: a rebuild that does not bound dies, while an ingest that does not bound
  is faster right up until it dies.

- **The unbounded arm stops at 1,843.0 MiB**, the identical figure §9.8 recorded for
  the 128 MiB rebuild arm at 1,200,000 records. The same wall, reached from two
  directions.

- **Per-stage attribution inside a compaction.** `MetricCompactPin`,
  `MetricCompactBuild` and `MetricCompactCommit` fire at the three stage boundaries,
  so "which stage peaked" no longer needs an external probe.

### The instrument was reporting a structural zero as a measurement

- **Windows estimates the anonymous/file split as `min(PrivateUsage, WorkingSetSize)`,
  and `PrivateUsage` is commit charge.** The Go runtime over-commits enough that a
  small process reports its entire working set as anonymous and **zero file-backed** —
  which printed identically to a measured zero. `rssSample.AnonClamped` now records
  that the estimate collapsed and the harness prints `n/a`, with a calibration test
  holding the invariant both ways.

- **That is what let the ingest's file-backed residency be trusted**: 251.6 → 572.9 →
  1,182.0 MiB across the ladder, tracking the store at ~0.78× and reaching **12× the
  anonymous class**. Free against a windows Job Object, which charges commit; charged
  against a linux cgroup, which charges page cache. That asymmetry is the gate on the
  resident-class work.

- **Peak anonymous and peak total are tracked apart**, because they are not the same
  instant — 242.7 against 477.8 MiB at 100k. Reading the file class as total-minus-anon
  would be wrong twice over: different instants, and the total's own sample already
  carries a `File` field for exactly that question.

### The documented recipe was the failing one

- **`USER_GUIDE.md` §10 is rewritten.** "Ingest everything, then `Compact()`" — the
  shipped recommendation — is the arm that peaks at 6,644 MiB. It is replaced with the
  bounded delta *plus* a reopen, both arms' figures, the write cost stated as a trade,
  and the batch caps shown in use.

- **A new §11, "Memory and capacity planning"**, with the per-node coefficients, the
  index multiplier `held ≈ recordBytes × (1 + 107 × entries / recordBytes)`, three
  worked shapes, and a table of every bound with its unit, default, and what it does
  *not* cover. §§11–14 renumber to 12–15.

- **`API_REFERENCE.md` gains the new surface with the trades in the text**: the batch
  caps and their refusal, the splitting adders and their loss of atomicity, budget
  discovery and its provenance, `MaxResidentBytes` as a floor, and the `bulk` schedule
  with its `Reopen` hook.

- **Two doc comments that were misleading are corrected.** `MaxDeltaBytes` now carries
  the index multiplier beside the delete multiplier it already admitted — 5.6× against
  1.8×. `AutoCompact` now states that it cannot reopen and therefore does not bound the
  payload term, which is invisible to it by construction.

### The mapped image stops being resident after a compaction has read it

- **`Options.ResidentAdvice` tells the kernel what the engine is about to do**:
  sequential over the parse at `Open` and over a compaction's read of the old image,
  random for the point-lookup life in between, and *drop these pages* once a
  compaction has finished with the image it copied forward. linux and darwin, through
  `madvise`. Off by default.

- **Measured on linux, interleaved, four rounds, 50,000 × 512 B:** file-backed
  residency after a compaction falls **53.28 → 6.38 MiB — 88.0% less, 8.35× — with
  the two arms' ranges not touching** (53.250–53.540 against 6.281–6.406). Total RSS
  falls 60.94 → 14.05 MiB.

- **The anonymous class did not move: 1.006, ranges overlapping**, and the Go heap is
  identical to three digits. That is the control, and it is the assertion worth making
  because it is the one that could have been false. `docs/benchmarks.md` carries the
  table.

- **It does not make a compaction hold less.** The peak *during* the compaction is
  unchanged at 0.994 — the pass still reads the whole image and still makes all of it
  resident while it does. This returns what the pass was finished with. Bounding the
  transient itself is Phase 4, and this measurement is an argument for it rather than
  a substitute.

- **The trade is less resident memory for more major faults, and only the first half
  is measured.** No arm reads anything back after the compaction, so the cost rests on
  what `MADV_DONTNEED` does rather than on a timing of this engine doing it. That is
  precisely why there is no default: a default would be asserting the second half on
  the strength of the first.

- **The drop is the record image alone.** The index mapping beside it is what
  `SwapBase` has just installed and what the next query reads, so returning it would
  pay the whole cost immediately to reclaim something the store is about to ask for
  again. That is not the same trade; it is simply worse.

- **It changes no contract, and that is the distinction that made it buildable.**
  `disk/mapping.go` records at length why a compaction cannot *unmap* the image it
  replaced — the graph it publishes carries slice headers into that image for every
  record the delta did not touch. Advice drops pages, not mappings: no address moves,
  no slice header is invalidated, and a read of a dropped page faults it back from the
  file.

- **darwin gets a raw `SYS_MADVISE`.** The standard library wraps `madvise` on linux
  and not on darwin — the wrapper is in `golang.org/x/sys/unix` and this module takes
  no dependency for a hint. `MADV_DONTNEED` rather than the `MADV_FREE` the plan named:
  `MADV_FREE` is for anonymous memory, and "the contents may be discarded" is
  meaningless for a read-only mapping whose contents are the file.

- **A platform that cannot advise says so** through `store.MetricResidentAdvice`,
  rather than leaving an option asked for and not held. Windows is that platform, and
  its answer to this problem is `LimitWorkingSet` above.

### The resident class is reported, and the report was hiding a zero

- **`StorageStats.Process` carries what the kernel says the whole process holds**:
  resident anonymous bytes, resident file-backed bytes, the peak, and the instrument
  that answered. It sits beside `EstimatedResidentBytes`, which is modelled, retained
  heap and a **floor** — a rebuild has been measured charging roughly 2.6× it.

- **It is the process, not the store, and it says so.** A library cannot separate its
  own pages from its host's, and a figure that claimed to would be the more misleading
  of the two. It is useful as a difference: read it either side of an ingest.

- **Off unless asked, behind `Options.ReportProcessMemory`.** `StorageStats` is polled
  — `AutoCompact` evaluates a policy against it on a ticker and a bulk import after
  every batch — and this is the only field in the struct that asks the operating
  system a question. Measured at ~800 ns and two allocations per read on windows 11,
  and a read and parse of `/proc/self/status` on linux. Everything else there is O(1)
  by requirement.

- **Building it found the windows reading collapsing to a structural zero.**
  `sysMemoryFromCounters` clamped `PrivateUsage` (commit charge) to the working set so
  a subtraction could not underflow, and then still reported `Split: true`. Whenever
  commit met or exceeded the working set — **the common case for a freshly started
  process, not an edge**: measured at 8.3 MiB against 8.3 MiB on an empty store — the
  file-backed share came out as zero, which reads as "none of the mapped image is
  resident". A clamped reading now reports `Split: false`, which already means "not
  separable" on a pre-4.5 linux kernel and on darwin, and the source string says why.
  This is the same defect `rssSample.AnonClamped` fixed in the harness, in the engine
  this time.

- **Unanswerable still does not read as zero.** `Known()` is false exactly when nothing
  was read; `Current` is false where only a peak is real, which is darwin; `Split` is
  false where the classes could not be told apart, and `AnonBytes` then carries the
  whole resident total — over-reporting the class that OOM-kills rather than under-
  reporting it.

### A resident-set cap on windows, as a function and not an option

- **`disk.LimitWorkingSet` and `disk.LimitWorkingSetFromCeiling`** install a hard
  maximum working set through `SetProcessWorkingSetSizeEx`. It is the one mechanism in
  this programme that **trims rather than kills**: the kernel takes pages out of the
  process instead of failing an allocation, so nothing dies and the program gets
  slower.

- **A function rather than an `Options` field, deliberately.** It acts on the process,
  so a second store opened with a different figure would silently move the first one's
  ceiling. That is the same reasoning that keeps `GOMEMLIMIT` a deployment knob: a
  library that re-sizes its host has substituted its own judgement for the caller's. A
  host that wants the cap calls it once, itself.

- **The limit is read back** through `GetProcessWorkingSetSizeEx`, and what is returned
  is what the kernel says it holds rather than what was asked for — including whether
  the maximum is actually hard. `tests/ceiling_test.go` refuses to report a pass under
  a limit it did not confirm; a caller of this has the same right, and a cap that
  silently failed to apply is worse than no cap.

- **linux and darwin get `ErrWorkingSetUnsupported`, which is not an omission.**
  `RLIMIT_RSS` is accepted and unenforced on every modern kernel, and `RLIMIT_AS`
  counts the mapped image's address space — the class this programme deliberately
  moved memory *into* — so a limit against it refuses a store that fits. What those
  platforms have instead is advice, below, and a cgroup the operator sets from outside.

### `GOMEMLIMIT` measured, and still not an `Options` field

- **§6.1 attributed 153 MiB to "the runtime — `GOGC`/`GOMEMLIMIT`, not code" and nothing
  had ever measured it.** Swept on the passing ingest arm, `docs/MEMORY_MODEL.md` §9.10:
  **128 MiB takes 76 MiB off a 252.7 MiB peak — 36% of the gap between modelled heap and
  charged bytes — for wall clock inside the noise.** 64 MiB takes a further 17 MiB and
  costs **51% of the ingest's wall clock**, which is the soft limit's death spiral
  arriving on schedule.

- **Anything above what the process already charges does nothing at all.** 1024, 512 and
  256 MiB were indistinguishable from unset on an arm charging 252.7 MiB, which places the
  runtime's own accounting in (128, 256] — a reading, not a figure to copy.

- **It reaches the gap and never the data.** The model of what this ingest holds peaks at
  43.4 MiB against 252.7 charged; the 209 MiB in between is uncollected garbage and
  unreturned arena, and that is the entire budget the knob has. Write amplification was
  6.53× at every setting, confirming it changes nothing about what is written.

- **It stays a deployment knob and will not become an `Options` field**, because it is
  process-global and this engine is a library in someone else's process. It is also the
  only bound darwin will ever have.

## v0.8.0 "Bookshelf_v2" — the rebuild fits, and the composites move onto the shelf

### A whole-layer rebuild fits under 2 GiB, and the knob was not what did it

- **v0.7.0's end-to-end acceptance failed and is now measured passing.**
  `docs/MEMORY_MODEL.md` §9.4 recorded a rebuild of all 1,400,000 nodes of a
  1,689 MiB store holding 3,853 MiB and peaking at 6,644 against a 2,048 MiB
  ceiling — 3.2× over — and said the figures had not been taken again since the
  index fix that shipped in the same release. Taken again, under a real 2 GiB Job
  Object: **1,285.2 MiB charged, 762.8 MiB of headroom, 37.2%**, settling at
  238.0 MiB of modelled heap against the read-only arm's 237.8. Having rebuilt
  every record in the store, the process holds what a fresh open holds.

- **The expected fix was the delta bound, and it is not.**
  `CompactionPolicy.MaxDeltaBytes` was swept across eight values at the full
  shape — unbounded, 512, 256, 128, 64, 32, 16 and 8 MiB — in a fresh process
  each. Every one died inside the rebuild, and the curve has an optimum rather
  than a direction: 32 and 16 MiB reach 1,350,000 of 1,400,000 records, 8 MiB
  turns back down to 1,250,000, and the loose bounds never fire at all because
  the process cannot reach them. §9.8 carries the table.

- **There are two runaway terms and a compaction moves cost between them.**
  Uncompacted, the term that grows is the resident property index over the new
  writes — at 400,000 records it is **546.4 MiB against the delta's 295.0**,
  1.85× larger, and no policy rule counts it. Compacted, that term folds into the
  image and the arm gets three times as far, but the record payloads stay in the
  heap: the payload term climbs 49.0 MiB per 100,000 records written and never
  resets, reaching 637.2 MiB. At the optimum, payload and records are 83% of the
  modelled heap. Those are bytes already written to the image and still carried,
  because `AttachBase` runs on the load path and a compaction is not one.

- **`Graph.CompactAndReopen` and `disk.Store.CompactAndReopen` compact and return
  a fresh handle on the same directory with the same options; the receiver is
  closed.** With a reopen after each of the fifteen interim compactions a 32 MiB
  bound produces, the payload term holds at 2.7 MiB instead of climbing, the
  property index goes back to being read in place — 140.9 MiB mapped where the
  in-place arm had 0 mapped and the shards resident — and anonymous memory holds
  flat across the rebuild instead of ratcheting: 502.4 MiB at 200,000 written,
  419.6 at 450,000, 687.8 at 1,050,000.

- **It is faster, which was not the argument for it.** The reopen arm finished the
  whole sequence in 242s; the 16 MiB arm spent 469s without finishing and the
  8 MiB arm 363s without finishing. Twenty-nine compactions against a heap that
  never stops growing cost more than fifteen against one that does not. A reopen
  is an open — 2.44s on this store, proportional to the image rather than to what
  changed — so it belongs after a compaction and not after a commit.

- **The bound and the reopen are one setting.** Reopening is not a substitute for
  bounding the delta: the same arm at 128 MiB reopens five times instead of
  fifteen and still fails, at 1,200,000 of 1,400,000. It is a large improvement on
  the 750,000 that 128 MiB reaches without reopening, and it is still a failure.
  A reopen sheds what accumulated since the last one, so how often it happens is
  the setting that matters.

- **Where it stops fitting is between 96 and 128 MiB, and the margin goes before
  the pass does.** Two further arms bisect the gap the sweep left open: 64 MiB
  passes at 1,368.5 MiB charged and 33.2% headroom, 96 MiB passes at 1,697.3 and
  17.1%, against 32 MiB's 1,285.2 and 37.2%. So the pass/fail boundary is in
  (96, 128] — and that is the less useful half. Headroom is flat from 32 to 64,
  four points for a doubling, then loses sixteen points over the next 50% and the
  rest over the 33% after that. Two thirds of the margin goes in the last
  doubling, because the transient a bound admits is not the bound: it is the
  bound plus a property index running at 1.85× it, plus the compaction that
  folds them in, against a ceiling the mapped image already takes 703 MiB of.

- **Nothing on the curve argues for loosening it.** Peak rises with the bound
  (1,285 → 1,369 → 1,697 MiB), compaction count falls as designed (15 → 9 → 7),
  and the wall clock *rises* rather than falling (242 → 273 → 288s) — the saving
  fewer compactions is supposed to buy does not appear. The other direction was
  already measured: 29 compactions cost more than 15. 32 MiB sits at the optimum
  of both curves, and it is now the documented configuration because three looser
  bounds were measured and none is better at anything.

- **The bound moves the peak and nothing else.** All three passing arms settle at
  238.0 MiB of modelled heap, term for term identical, against the read-only
  arm's 237.8. `MaxDeltaBytes` sizes a rebuild's transient; it does not change
  what the process holds at rest, at either end of the curve.

- **`DefaultCompactionPolicy` is unchanged, and that is the measurement's
  conclusion rather than a deferral.** The release was expected to move
  `MaxDeltaBytes` off this curve. No value on the curve completes the rebuild on
  its own, so there is no winner to promote; 128 MiB stays what it was, and
  moving it would change the compaction schedule of every deployment tuned
  against it for no measured benefit.

- **The lifetime contract is unchanged.** `CompactAndReopen` is rule A verbatim —
  the predecessor handle is closed, so nothing is retired out from under a live
  graph — which is why it was preferred to re-mapping a compaction's output in
  place. A `Properties` slice held across the call addresses a released mapping;
  copy first, as `CloneNode` does. Three tests in
  `tests/graphene_compact_reopen_test.go` assert that the payload term falls by
  the blobs, that the receiver is closed and the returned handle holds the lock,
  and that the in-memory backend hands back the same `Graph`.

### The composite index moves into the image: `GCPX`

- **What it buys, on the arm that was already passing.** Same fixture, same
  2,048 MiB Job Object, same 32 MiB delta bound and reopen, 1,400,000 nodes — the
  only difference is that the build writes and reads the section. The peak charged
  against the ceiling falls from **1,285.2 MiB to 880.7**, headroom from 37.2% to
  **57.0%**, and the settled modelled heap from 238.0 MiB to **109.9**. The heap
  falls by 128.1 and the composite term falls by 128.2: the same number to a tenth,
  with records, payload, label postings and adjacency reporting 75.0, 2.7, 10.7 and
  21.4 in both arms, digit for digit. Nothing else moved.

- **It costs 21.4 MiB of image for 128.1 MiB of heap — 6.0×.** The section is the
  postings and nothing else; what it replaces is the postings *plus* the per-entity
  rows of member values a resident composite needs in order to file a tuple, and
  those rows were 85% of what a composite index held. The image pays for the half
  that is data; the heap was paying for both halves.

- **An open is 2.9× faster into the bargain, which was not the argument for it.**
  The reopen phase goes from 2.44s to 0.828s, because an open over an image
  carrying the section does not walk the member keys' entries to fill the
  composites. The rebuild's wall clock is unchanged — 3m31.85s against 3m32.02s.

- **A pre-GCPX image reads exactly as it did, measured at the full shape on the
  same run.** A read-only arm over the old 1.4M fixture — a v9 image this build did
  not write — reports the composite term at 128.2 MiB and the total at 237.8,
  unchanged to the tenth, at 80.0% headroom.

- **It was the largest single term a default configuration held, and it is now a
  section rather than a structure.** At 1,400,000 nodes the declared composites
  held **128.2 MiB of 237.8 MiB — 53.9%**, ahead of record arrays at 74.8, and
  164.8 of 305.8 at the 1,800,000-node shape: the same fraction to a tenth of a
  point across a twentyfold difference in blob size. GPIX made the single-key
  postings readable in place; this is the same move for the one part of the index
  GPIX deliberately left behind.

- **What GPIX left behind, and why it was not an oversight.** A composite is an
  index over a tuple of keys the forward direction holds *separately*, so
  answering one out of GPIX would mean intersecting the member keys' runs on every
  query — exactly the work a composite is declared to do once. So composites were
  filled into the heap at open and stayed there. That argument establishes a
  composite cannot cheaply be *derived* from GPIX, which is a reason to store it,
  not a reason to store it in memory.

- **The one argument in the codebase against doing this is answered, not
  bypassed.** `appendCompositeSection` says the postings' bytes "buy nothing that
  the entries do not already hold". That was an argument about the *image*, made
  when the index was rebuilt into the heap at open and the only question was file
  size. Once the index is read in place those bytes buy the difference between
  mapped and resident. Its other clause — a derived structure written to a file
  can go stale — still holds, and GCPX takes GPIX's answer unchanged: a base is
  only installed by the code that just wrote it from the index it replaces, and
  everything since is `base − retracted ∪ delta`.

- **A new magic, and not an extension of `GCMP` — the plan's route would have
  broken older readers.** `GCMP` is not unused: it carries the composite
  *declarations* and a v0.7.x reader parses its body, so appending postings to it
  would hand an older build bytes it reads as a malformed declaration list and it
  would **fail the image rather than skip it**, inverting the exact guarantee the
  optional flag exists to provide. `GCPX` is optional and non-critical on its own,
  and `checkCriticalSections` skips a non-critical magic it does not understand.
  `GORD` is the precedent. **No version bump past v9, no stranded readers, no
  bidirectional migration.**

- **The reader that skips it is tested, because nothing else here can fail if it
  is wrong.** A v0.7.x binary is not available to run, so the claim is checked by
  building both bases this build can build out of one image — the same GPIX and
  GPIR, once with the section and once without — and asserting they answer every
  tuple identically while holding different amounts. Without the section is
  exactly what an older reader has: the decision is a type assertion on
  `index.CompositeBase` and no version number is consulted anywhere.

- **One thing a base-backed composite needs that a filled one does not.** A
  composite files a tuple only from a *complete* row, and over a base-backed
  composite an entity the image holds has no row at all — so a caller adding one
  member value to such an entity would file nothing, and the new tuple would be
  held by neither side. A row created for an entity the base knows is therefore
  hydrated from the base's reverse direction first. It costs one reverse walk per
  entity *written*, never per entity in the base.

- **A compaction now sheds the composites as well as the index.** `SwapBase` used
  to leave them alone, correctly: their content was already the entries the new
  base holds, so refiling would walk every entry under every member key to arrive
  back where it started. A base that *carries* them is different — the postings
  are in the image, and keeping the resident copy would mean the section bought
  the process nothing until something reopened the directory. That was the whole
  defect `SwapBase` exists to close for the rest of the index.

- **A capability that degrades into correctness needs a test that asserts the
  capability.** The compaction installs a `mappedIndexBase` wrapping the base it
  parsed, and that wrapper embeds `index.Base` as an *interface* — so it promoted
  `index.Base`'s methods and silently dropped `index.CompositeBase`'s. Every
  answer stayed correct, because the fallback is to fill from the entries; the
  only symptom was that the whole saving evaporated one compaction after it was
  won, with nothing logged and nothing failing. The wrapper forwards the
  capability now, and the end-to-end test asserts the resident figure rather than
  the answers.

- **Damage in a GPIX run now surfaces on the read rather than at open, and that is
  a real change.** Filling the composites used to read every run under every member
  key, so a run that would not decode was met at open and the store refused to come
  up. That was never a verification pass — it was a side effect of one, it covered
  only the member keys of declared composites, and a store with no composite had no
  such check at all. Those keys now behave like every other key in the image:
  damage is met by the read that touches it, recorded by `BaseFault`, and reported
  by `Verify` and `VerifyIndexes`. An open that wants the O(entries) pass asks for
  it with a `Verifier`. `TestIndexMapped_DamagedRunSurfacesAsAFault` pins both
  halves, including that the composite query is *unaffected* — its postings are in
  GCPX, which is the difference between a section that is derived and one that is
  a second copy.

- **The tuple encoding stays in one package, in both directions.** The writer is
  handed the map keys of the resident postings and the reader hands the same bytes
  back, so `disk` never parses or constructs a tuple. `index.encodeTuple` is
  little-endian length-prefixed and therefore **not order-preserving** with respect
  to the values it encodes, so two implementations of it that disagreed by a byte
  would produce a file whose binary search is bounded, silent and wrong.

- **`verifyGCPX` checks what the reader assumes and does not re-check**: that
  tuples ascend, that each tuple table entry's prefix is its tuple's, that a run's
  ids ascend, that the runs region is tiled exactly, and that the directory's
  counts are the counts the bytes hold. What it deliberately does not check is
  agreement with the property entries — that is an O(entries × width) pass over
  GPIX, and the staleness argument above is why it is not needed. What that leaves
  genuinely unverified is a writer that computed the wrong tuples, which a verifier
  cannot separate from a caller who registered them; stated rather than left to be
  discovered.

### The nightly gains a third arm, non-blocking

- **`ceiling-linux` and `ceiling-windows` each gain a 1.4M-node rebuild arm** with
  the delta bounded at 32 MiB and a reopen after each interim compaction — the
  configuration §9.8 measures passing. The repo already keeps read and write arms
  separate so that neither hides the other; this is the write arm at the full
  shape rather than at 200,000.

- **Both start `continue-on-error: true`, and the reason is not timidity.** The
  figures were taken on windows under a Job Object, which bounds commit charge;
  the linux arm runs under a cgroup, which bounds resident pages, and the two are
  not the same instrument. A nightly that went red on a difference between
  instruments would be switched off, and an arm nobody reads is worse than one
  that is honest about being new. Neither has run on a hosted runner, which has
  less commit and a slower disk than the machine the measurement was made on. Drop
  the flag once each is green, not before.

### CI was running the same 479 tests twice, and two gates were red

- **The push job ran the tests package, then ran it again.** `go test ./tests/
  -tags=stress` compiles the tagged files *and* every untagged one beside them,
  so the stress step re-ran all 479 tests the unit step had just finished for the
  sake of the nineteen that exist only under the tag — 253 seconds on the windows
  runner, more than the unit run itself, on all three platforms. The step now
  runs only the tests the tag adds, and the names are derived from the tree
  rather than written into the workflow, for the reason the fuzz matrix already
  gives: a hand-kept list of what to run is a list that silently shrinks relative
  to what exists. A test in a stress-tagged file is exactly a test that exists
  only under the tag, so the set comes out of the files and needs no build to
  find. Locally the arm goes from 147.6s to 60.7s.

- **Compile rot for the tagged files is now caught by `go vet -tags=stress
  ./...`** in the lint job, which is where it should have been: the old step
  covered `./tests/` only, so `disk`'s four measurement spikes and the benchmark
  files in every other package were compiled by nothing in CI at all. One command
  on one platform now covers more than a run on three did.

- **The benchmark smoke job moves to the nightly.** Its two purposes were "they
  still compile" and "they do not panic on their first iteration"; the vet above
  answers the first in seconds, and the second is not worth three minutes of
  every documentation commit.

- **`TestZeroCgo` was red on linux and macos for a property neither had lost.**
  It asserted that no package in the closure compiles a cgo file, with
  CGO_ENABLED forced on so the question would not be vacuous — but forcing it on
  also manufactured the answer: `cmd/graphene` writes a tar for `export bundle`,
  `archive/tar` reads owner names through `os/user`, and `os/user` compiles a cgo
  file when cgo is enabled and a pure-Go one when it is not. Windows stayed green
  throughout, because os/user needs no cgo there, and a gate whose result depends
  on which platform asks is measuring the platform. It now asks the two questions
  the release build actually rests on: nothing in *this module* compiles C, and
  nothing in the closure is left unbuildable with cgo *off* — which is how
  `build.sh` builds all six release targets.

- **The darwin arm of the RSS calibration could not pass.** Darwin reports the
  peak through getrusage and nothing about the size now, because the current
  figure needs Mach calls and therefore cgo; the calibration read that zero as a
  current reading and asserted a quarter-gigabyte allocation had moved it. The
  platform readers now say which of the two questions they answer, and the
  calibration asserts the peak where that is all there is — rather than skipping,
  which would let a reader returning a plausible constant through.

### The mapped-bytes accounting was wrong, in both directions

- **`ResidentEstimate.Mapped` and `StorageStats.ImageMappedBytes` reported zero
  for a file the process had open.** Both were taken off the live graph, which
  answers "is what a caller reads coming out of a file". After a compaction the
  honest answer to that is no — the graph on top was built in memory — but the
  mapping taken at `Open` is held until `Close`, because every blob the compaction
  carried forward still addresses it. A store that compacted once went on paying
  for its image and reporting nothing for it. Both figures now count the mappings
  the store *owns*.

- **`ImageMode` is unchanged and still follows the read path**, so a store that
  has compacted reports `ImageMode: "heap"` beside a non-zero `ImageMappedBytes`.
  That pairing is not a contradiction: they answer two questions, and both
  answers are wanted. The one that sizes a machine is the byte count.

- **`StorageStats.IndexMappedBytes` is new, and the thing it reports was reported
  nowhere at all.** `IndexMode` said `"mapped"` while every byte figure in the
  struct said zero. `disk/gpix_base.go` sent a reader to `ImageMappedBytes` for
  it, which was nearly right at open — GPIX and GPIR are sections *inside*
  `graphene.csr`, so a mapped image has a mapped index inside it — and not right
  at all after a compaction, when `SwapBase` installs a base over a second mapping
  held in a different list. The new field is the index's own sections, so it is a
  **subset** of `ImageMappedBytes` rather than a second figure: adding them
  double-counts, and `graphene store stats` prints it indented under "of which".

- **The reason to name it separately is that "this store has 1.6 GiB mapped" is
  not an answer to "what is the property index costing me".** A caller choosing
  between `IndexMapped` and `IndexResident` is asking the second question, and
  had no way to.

- `disk.ResidentEstimate` gains `MappedIndex` for the same figure.
  `index.MappedReporter` is the optional interface a base implements to answer
  it; a base that holds a decoded copy implements nothing and reports zero.

- No format change. The values `ImageMode` and `IndexMode` can take are
  unchanged.

### What the composites cost is now a term of its own

- **`ResidentEstimate.Composite` is taken out of `Index` rather than left inside
  it.** Every other term in the breakdown moves with what the store was asked to
  hold; this one moves with a declaration. A composite nothing queries costs
  exactly what one carrying the workload costs, and at the shape this engine is
  sized for the composites have been the largest single term in a default
  configuration — the one part of an index that mapping does not get out of the
  heap.

- **Exact, and free to read.** `index/resident.go` was already looping per
  declaration to compute this and summing it away; `PropertyIndex.
  CompositeResidentBytes` surfaces it and `ResidentBytesSplit` returns it with the
  total it is part of. One call rather than two deliberately: nothing holds a lock
  across the whole walk, so a caller subtracting two separately taken figures can
  get a negative "index" out of two honest numbers.

- **`store.CompositeIndexDeclarer`'s documentation now gives the size of "not
  free".** It already said a declaration no query matches costs memory for
  nothing. What it did not say is about 48 bytes an entry against a mapped
  index's near-zero, or that declaring several can put a store back over a ceiling
  the mapped index had just brought it under.

### A running store can say what format its image is

- **`StorageStats.ImageVersion` and `StorageStats.IndexOnDisk` are new.** An
  image written before v9 loads its property index entry by entry whatever
  `IndexMode` asks for, because GIDX is the only form it has — measured at about
  sevenfold on the open, 301 ms against 2,102 ms on a 462 MiB store and 1.40 s
  against 9.94 s at 2.26 GiB — and then holds it in the heap at roughly a hundred
  bytes an entry. Nothing in the running system said so. `IndexMode` reported
  `"resident"` with no reason attached, `ImageMode` reported `"mapped"` because
  the image itself maps perfectly well, and no metric fired.

- **`IndexOnDisk` is what separates three causes one field cannot.** A store
  reporting `IndexMode: "resident"` has either asked for it, or asked for a
  mapped index over an image that is itself in the heap, or asked for one over a
  file with nothing mappable in it. Only the third is fixed by compacting, and
  `"resident"`/`"entries"` read together is how a caller tells.

- **`MetricIndexFallback` now fires on that third cause, and its documentation
  records the reversal.** It used to exclude it, on the grounds that a file with
  no mappable index is not something to fall back *from*. That reasoning is about
  where the cause lies; the metric is about what the caller is paying. The old
  sentence is quoted in place rather than deleted. This case is also the only one
  of the three whose reason can name a remedy, and it does: `Compact` under
  `IndexMapped`, or `graphene store migrate -to 9`.

- **Auto-migrating on open was considered and declined.** An `Open` that silently
  rewrites the store is a surprise of a different kind, a compaction is not free,
  and it must never be the default for a read-only open. The remedy is one pass
  over the store and it is the caller's to run.

- `graphene store stats` prints the format version under "compacted image", with
  the consequence attached rather than left to a reader who would have to know
  what the number means.

### `OpenLive` says what its defaults cost

- **One option not set costs 419 MiB on a 248 MiB store**, and the number is now
  in the doc comment of the function that charges it. A read-only open of that
  store holds 146–149 MiB; `OpenLive` on the defaults holds 568.

- **The cascade is two steps and the second follows from the first.**
  `ImageMapped` maps only where something excludes a concurrent writer from the
  directory, and a live reader holds no lock by construction — that is what makes
  it live — so the image goes to the heap, and a mapped index is then declined
  because the image is in the heap. A caller reading only the first fallback would
  conclude that `ImageMappedUnlocked` buys back the image half alone. It buys
  back both: the same store falls to 144–150 MiB.

- **`OpenEstimate.FallbacksFor(Options)` is new**, and reports the same cascade
  before the open rather than after it. Both fallbacks already fired as metrics,
  naming their reasons — nothing here is a behaviour change — but a metric arrives
  once the memory has been spent, and the decision it informs is taken before.
  The reasons come from the two functions an `Open` asks rather than from a second
  copy of the rule.

- **The default is not moved, and the version number would permit it.** Mapping a
  file a writer may rewrite underneath you fails in ways a caller cannot handle,
  and the shape differs by platform: shortening the file removes pages a slice
  still addresses and the next access is `SIGBUS` on unix, while windows refuses
  to shorten a file with a live mapping at all. An engine may not choose that for
  a caller who has not asked. `ImageMappedUnlocked` is how a caller asks, having
  read what it trades.

### Six facts about the store are reachable from `Graph`

- **`Graph` embeds `store.GraphStore`, and a fact the interface does not carry was
  reachable only by type-asserting back to `*disk.Store`.** Six of them now
  forward: `EstimateResident`, `ReadOnly`, `RecoveredFromUncleanShutdown`,
  `Declarations`, `DroppedDeclarations` and `LockState`. A consumer sizing a 2 GB
  deployment needed the first, and reaching past the facade for it defeats the
  point of the facade.

- **Each returns `(value, ok)`, the shape `StorageStats` already used**, because
  the in-memory backend has no answer to any of them and its zero value reads like
  one. `false` from `ReadOnly` or from `LockEnforced` is a claim about the store;
  `ok == false` is the statement that the question does not apply to this backend.

- **`LockMode` was wrong, and writing the forward is what found it.** It
  re-derived its answer from `Options.ReadOnly` rather than reading the lock the
  store holds. `Options.LiveReader` implies `ReadOnly`, so a live reader — which
  takes no lock at all, by construction, because not excluding the writer is the
  whole of what `OpenLive` is for — reported `LockShared`, a mode that excludes
  every writer. `LockNone` existed, was documented as the mode `OpenLive` takes,
  and was unreachable from a handle. It now reports what it holds. A caller who
  branched on `LockMode() == LockShared` to decide whether a concurrent writer was
  possible got the opposite of the truth.

- **`LockMode` and `LockEnforced` forward as one accessor.** They are two facts
  about one thing, and the platform property (`LockEnforced`) is what says whether
  the open-mode property (`LockMode`) means anything.

- **`SetSyncOnCommit` is deliberately not forwarded**, and stays on `*disk.Store`.
  It is a mutator, and a durability switch that silently does nothing on a backend
  without durability is worse than one a caller had to prove which backend they
  held in order to reach.

### A projection cannot refuse a key, so the key list is now askable

- **A mistyped projection key is silent, and the result looks like an answer.**
  `NodeProjection` is positional: `Values[i]` is the value for `keys[i]`, the slice
  is always `len(keys)` long, and a key nothing is indexed under produces a nil
  entry — which is exactly what a key that is indexed but empty for this entity
  produces. The pass succeeds, `err` is nil, and nothing in the result separates
  the two.

- **`NodePropKeys` and `EdgePropKeys` are the discriminator**, on both backends,
  now that they are reachable from `Graph` and from `disk.Store`. They report the
  keys the property index actually carries — not the declared set, which is a
  different question: a projection reads whatever was handed to
  `IndexNodeProperty`, declared or not. Under a mapped base the list is an **upper
  bound**, because a key whose entries have all been retracted is still named by
  the image, so a validator may act on absence and not on presence. Absence means
  "matches nothing, for certain", which is the check worth having.

- **The list unions the mapped base with the delta.** A store opened on a v9 image
  and not yet written to holds every key it owns in the base and none in the
  shards, which is the shape this engine is built for; a list read off the shards
  alone would have rejected every key the store carries.

- **The all-nil pre-fill was left in place, because it is the contract and not the
  defect.** Removing it would shorten the result when every key missed, and turn an
  entity that happens to carry nothing into a panic for the positional callers the
  doc comment tells to index by key position. The silence is the surprising part,
  and it is now stated at `ForEachNodeProjection` and at `NodeProjection` rather
  than inferable.

- **`Options.RefuseUnindexedProjectionKeys` makes the silence an error**, at store
  level rather than per call, because what a consumer wants is strict in tests and
  lenient in production. It costs one binary search per key per pass and it is
  opt-in: the default is unchanged to the byte. It is not the default for the same
  reason the lifetime contract was not weakened — this release does not turn a
  silent success into a runtime error for callers who cannot see the change in a
  diff of their own code. The in-memory backend does not offer it.

### Documentation: the mapped-slice lifetime contract is two rules, not four

- **Eleven statements across the tree said four incompatible things about how
  long a `[]byte` a read returned stays valid.** No guarantee is weakened and no
  behaviour changes; what changes is that each statement now carries the
  precondition under which it is true.

- **Rule A, the default.** Under `ImageMapped` a returned slice is valid for the
  life of the handle and not past `Close()`. A compaction does not shorten that
  window — it neither creates nor retires a mapping — which is what three sites
  got wrong in the safe direction by saying the slice died at the next compaction.
  Safe is not free: it made "a compaction leaves its output resident until reopen"
  read as a bug rather than as a resident-set characteristic.

- **Rule A now has a test, and it pins the reason rather than the observation.**
  That a retained slice reads its own bytes across two compactions was already
  asserted; what was not is why. A compaction does publish a new graph, the old
  one does become unreachable, and the cleanup registered when the image was
  mapped does mark the mapping retirable — and retirable is not released, because
  nothing on the default path sweeps. `sweepImages` is called from `disk/live.go`
  and from nowhere else. The new test asserts that asymmetry directly, so adding a
  sweep to the compaction path "for symmetry" fails here instead of silently
  shortening every caller's slice lifetime.

- **Rule B, and it keeps its precondition now.** Under `OpenLive` *with*
  `ImageMappedUnlocked`, where a `Refresh` across a compaction maps the new image
  and retires the old, a slice is valid only until the second such reload after
  it. Two sites stated B without the precondition. It was scoped rather than
  deleted: it is correct and was misplaced.

- **`OpenReadOnly`'s doc comment still described the pre-mmap engine**, at four
  sites — "loads a store into memory once at open and never re-reads it". The
  conclusion each supported is still true and the justification was backwards
  under `ImageMapped`, which is the configuration that maps best. Corrected to the
  form `disk/lock.go` already used: one read *or one mapping*, and a mapping does
  not change it because the image is never rewritten in place.

- **A writer that deletes cannot skip the adjacency build**, and the argument for
  that already existed in `disk/adjacency_mode.go` — attached to no declaration,
  where godoc never rendered it. The consequence is now on `Options.Adjacency` and
  on `AdjacencyLazy`, which is where the choice is made.

- **And the obvious follow-up is closed rather than left open.** §6.7 of
  `MEMORY_MODEL.md` now says why the delete cascade cannot be given a cheaper route
  than the adjacency arrays: there is no index over `Edge.Src` or `Edge.Dst`
  anywhere in the engine, `EdgesOf`, `Neighbours`, `DegreeOf`, `IncidentEdges` and
  `EdgeBetween` all land on the same four arrays so the cascade is not even a
  special caller, and adjacency has not been written to the file since v7 — so
  there is no section to read instead. The only alternative is scanning the edge
  arena per delete: O(edges) *per delete* against one O(edges) build amortised over
  every delete in the process. `AdjacencyLazy` moves when the build is paid, not
  whether, and there is no third outcome behind an index nobody has written yet.

- **Corrections to `docs/MEMORY_MODEL.md`.** §8.6's footnote concluded that the
  composites were "no longer the largest term in the column" after scaling them to
  ~128 MiB, in a column whose record arrays are 74.8 — they still lead, by 1.7×;
  what changed is the size of the prize, not its rank. §8.4 stated three
  coefficients in one paragraph without saying which was the estimator's constant
  and which were measurements. §8.5 cited a section that does not discuss what it
  was cited for, cited a label ("B3") defined nowhere in the repository, and said
  a hazard is fatal "on every platform" where the code refuses it outright on
  windows. §9.4's retraction marker reached two paragraphs and the claim it
  retracts is 119 lines further on. Superseded figures are marked in place, not
  deleted.

- Stale figures: the fixture peak is 9.4 GiB and two code comments said 7.5;
  `GRAPHENE_RSS_NODES` defaults to 50,000 and the knob table said 200,000;
  `CommitSeq`'s high-water mark has been carried since v8 rather than by v8; and
  one CHANGELOG sentence reported a windows working set against a linux cgroup.

- **The ceiling harness's own documented invocation omitted `-count=1`.** Every
  arm of a sweep differs only in an environment variable, and `go test` replays a
  cached result across invocations that differ only in one it did not see the test
  read — so a sweep run without it reports the first arm's figures under every
  arm's name, identical to the millisecond, phase table and all, with "(cached)" on
  a line the eye skips. The differences the sweep exists to find are exactly the
  ones it then cannot show. This cost a full sweep before it was written down, and
  it is the same failure as a stale fixture cache: a measurement rig that answers
  from a cache is not a measurement rig, and its wrong answer looks exactly like
  the right one.

- **`DefaultCompactionPolicy` gains a sizing rule for `MaxDeltaBytes`** — the
  trade, stated so a caller can apply it to their own ceiling. The default is
  unchanged here; moving it is a measurement, not an edit.

### `MaxDeltaBytes` counts the records and not the map that holds them

- **The rule that exists to be the memory rule under-reports what the delta
  costs, and sharply so when the delta is mostly tombstones.**
  `CompactionPolicy.MaxDeltaBytes` is evaluated against
  `StorageStats.DeltaBytes`, which is `deltaLayer.bytes`: the version cells, the
  records, and those records' labels and property blobs. The version maps that
  hold the cells, the delta adjacency and the type postings are structure rather
  than payload, and they are modelled in `disk.ResidentEstimate.Delta` instead.
  Both halves say so where they are implemented. What was missing is that a
  caller reading *"on a machine with a memory limit, this is the figure that runs
  out"* is reading about the smaller of the two.

- **Measured, deleting a 1,400,000-node store entry by entry under a 32 MiB
  bound.** A tombstone contributes 24 bytes of counted cell and roughly 19
  further bytes of map the rule cannot see. At 1,300,000 tombstones the rule saw
  29.8 MiB and the store was holding 53.8 — **1.81×** — and no compaction fired
  until the 1,400,000th, which is exactly where 24 bytes a tombstone crosses
  32 MiB. The arithmetic and the measurement agree to a tenth of a MiB, so this
  is a mismatch between two definitions and not an error in either.

- **Nothing changes about when a store compacts.** The figure is correct for what
  it says it counts, and moving it would silently change the compaction schedule
  of every deployment that has tuned against it. What changes is the doc comment,
  which now carries the factor, the measurement behind it and the remedy: size a
  delete-heavy workload against `ResidentEstimate.Delta`, or halve the bound. The
  gap is negligible whenever the delta carries blobs, because then the blobs
  dominate and they are counted.

### What a read costs when its pages are not resident

- **Six statements across the tree declined to measure the cold path, all giving
  the same reason: there is no way to drop the page cache from a Go test on
  windows.** Four trade-offs rest on that unmeasured case — the residual probe's
  2.8x warm regression, the property join's 1.17x on unsorted values, the bounded
  batch API's whole rationale, and the mapped point lookup's doubling. They lean
  the same way, and the deployment this release is sized for is the one where the
  page cache is under pressure. The reason was half wrong, and the wrong half was
  the framing: the constraint is windows, not Go, and it does not hold on linux.

- **`tests/coldlookup_test.go`, behind the `stress` tag, measures four reads
  against an image whose pages have been made non-resident** — a record point
  read, an index point lookup, a projection batch, and a bounded batch read —
  reporting each one's cold pass beside a warm pass taken immediately after, in
  the same process, over the same ids.

- **The two platforms are not measuring the same thing, and the column says so.**
  On linux the image is evicted with `posix_fadvise(POSIX_FADV_DONTNEED)` through
  a raw `syscall.Syscall6`, with the store closed, because DONTNEED does not
  discard pages a mapping holds; the column is headed **evicted** and the next
  touch is a disk read. On windows the working set is trimmed with
  `EmptyWorkingSet` through `syscall.NewLazyDLL`, which moves the pages to the
  standby list, where the next touch is a soft fault of hundreds of nanoseconds
  rather than a seek of hundreds of microseconds; that column is headed
  **trimmed**, and it is a lower bound on the cold cost, not the cold cost.
  Labelling it otherwise would produce a number that later had to be retracted.
  `FILE_FLAG_NO_BUFFERING` and `SetSystemFileCacheSize` were both considered and
  are both rejected in the file, with the reason. The zero-dependency invariant
  survives either route: `go.mod` is 44 bytes and a test asserts it, so
  `golang.org/x/sys` is not available and the kernel is reached directly.

- **The windows arm fails rather than reports when the trim does nothing.** It
  reads the process working set before and after the call and refuses to continue
  if it did not fall, because `EmptyWorkingSet` returns success without doing
  anything under conditions a test cannot see. A rig that answers from a cache is
  not a rig, and that failure mode looks exactly like the right answer.

- The five live claims that the cold case could not be measured are superseded in
  place, in `docs/benchmarks.md` and `docs/TECHNICAL_DETAILS.md`.
  `BenchmarkRSS_ResidualRoutePages` keeps its page-count proxy and says why: a
  page count is stable across machines in a way a latency is not, and it runs on
  every platform rather than on two.

- **The figures, taken at 1,400,000 nodes against the 1,689.1 MiB image.** Trimmed
  against resident, three runs at 20,000 samples: record point read **20.6–26.1×**,
  index point lookup **4.8–6.1×**, projection batch **4.58–5.21×** ascending and
  **3.62–5.70×** shuffled, bounded batch read **16.5–21.1×**. Every one of them is a
  lower bound, because trimmed pages are a soft fault away and not a seek away.
  `docs/benchmarks.md` carries the table.

- **The ratio depends on how many samples you take, which is a property of the
  instrument and not of the store.** The same arm reports the index point lookup
  at 285× on 2,000 samples, 16× on 8,000 and 5–6× on 20,000, while three runs at
  20,000 agree within 20% of each other. A trimmed pass pays a fixed cost to fault
  its structure back in and then a marginal cost per operation, and the harness
  reports their sum over the sample count — so a small run measures the first
  lookup into a cold store and a large one measures the thousandth. Both are real
  questions; quoting one figure for both would have been an artefact. Use 20,000
  or more for the marginal number.

- **What the measurement settles.** The residual probe's 2.8× warm regression is
  vindicated: trimmed, the index pages it reads amortise across lookups to
  4.8–6.1× while the record payload pages a scan would touch do not amortise at
  all and cost 20.6–26.1×, so the planner's cost model leans the way the
  measurement does. The same argument carries the bounded batch API, and it makes
  the mapped point lookup's warm doubling a small part of the real cost.

- **The batch join's ordering now has a second arm, and it changes the number.**
  `projection batch` is two arms over the same ids in the same count — ascending,
  and a fixed shuffle — differing only in the order values reach the join. Across
  three runs at 1,400,000 nodes the shuffled arm costs **1.34–1.59×** the
  ascending one trimmed and **1.27–1.78×** resident, so the advice to supply
  ascending values rests on a measurement at the shape the advice is for, and the
  penalty is larger than the 1.17× the warm microbenchmark reports at a smaller
  one. The shuffle is a hand-rolled splitmix64 rather than `math/rand`, whose
  sequence for a given seed is explicitly not guaranteed across Go releases — a
  figure in `docs/benchmarks.md` should reproduce on a later toolchain.

- **What that arm does *not* show is the thing it was expected to.** The
  hypothesis was that the sort pays for itself cold, a search per value being
  eighteen page faults rather than eighteen cache misses. The trimmed and resident
  ranges overlap heavily and one run puts the resident penalty above the trimmed
  one, so at this sample count they cannot be separated. That is the instrument
  and it was predicted before the arm ran: ascending order buys a monotonic cursor
  advance *and* kernel readahead, and `EmptyWorkingSet` measures the first honestly
  while largely removing the second, because the standby list is where a
  mispredicted readahead costs almost nothing. The linux arm is the one that can
  size the readahead half, and it has not been run at this shape. Reading the null
  result as "ordering does not matter cold" would be reading the instrument.

- **What it still does not settle.** The bounded batch read's
  cold pass lands at 19.1–19.5 ms against the harness's own 20 ms trust threshold
  and was flagged in three runs of four; the flag is reported beside the figure
  rather than argued away. And the linux arm, the one that evicts for real, has
  not been run at this shape.

### The fixture builder can build a store larger than the machine's RAM

- **The ceiling harness could not build the fixture the ceiling job wants to
  measure.** A twenty-gigabyte store built in one pass holds its whole delta and
  then its whole compaction output, so the build peaks far above anything the read
  arm will ever hold, and the process dies before there is a store to measure.
  The limit is the builder's, not the store's.

- **`GRAPHENE_RSS_BUILD_CHUNK` builds in passes, and closes the store between
  them.** Each pass writes its chunk, compacts, closes and reopens. Closing is the
  point, not the compaction: §9.4 of `docs/MEMORY_MODEL.md` measures a compaction
  leaving its own output resident until the handle is reopened — 3,838 MiB after
  compacting against 1,078 after reopening — because `AttachBase` runs on the load
  path and nowhere else. Redeclaring a key on reopen is a no-op, so the schema
  survives the cycle unchanged. A 19 GiB fixture builds while anonymous memory
  holds near 4.2 GiB.

- **Build progress reports the store's size on disk and the process's anonymous
  and total resident bytes at every chunk boundary**, so a build that is going to
  fail says so in the first minute rather than in the fortieth.

- **The fixture's shape marker now carries the image's format version.** A cached
  fixture whose format predates the reader measures the wrong engine and reports a
  plausible number for it — the same bug class as a cached `go test` result, and
  the same tell: nothing looks wrong. The CI fixture cache is keyed on that
  marker, so a format change misses the cache rather than silently measuring the
  old one.

### An 18.4 GiB store opens and reads inside a 2 GiB limit, with three quarters of it unused

The memory model has always been measured at shapes that sit near the ceiling — 1.65 GiB on
disk against a 2 GiB limit — which leaves the question a consumer actually asks unanswered:
what happens when the store is much bigger than the machine's budget, not slightly smaller?

A second fixture answers it. 1,800,000 nodes carrying 10 KiB of blob each, the same schema
the rest of the document uses (8 unique keys, 5 ordered, 2 composite), 19,787,551,529 bytes
of `graphene.csr` and 23,400,000 node index entries. Opened read-only under the same 2 GiB
Job Object the ceiling harness has been using all along, in a fresh process, with the limit
read back from the kernel before the run is allowed to claim anything.

**It charges 527.2 MiB of the 2,048 MiB limit and finishes with 74.3% headroom.** The
process's peak working set is 7,598.2 MiB and that is not a contradiction: 7,071.2 MiB of it
is file-backed, the mapped image's pages, which the kernel may drop and re-fault at will and
which a commit-charge limit never sees. The figure a deployment has to budget for is the
527.2, and the distinction between the two is the entire reason §9.1 measures with a Job
Object rather than reading RSS.

**The terms do not care how big the blobs are.** Divided by node count, this fixture and the
1,400,000-node one agree to two decimal places across a twentyfold difference in blob size:
record arrays 56.10 against 56.02 bytes a node, label postings 7.98 against 8.01, adjacency
16.02 against 16.03, composites 96.00 against 96.02. The payload row is the one that
matters: twenty times the blob bytes — 17.6 GiB against 0.66 — moves it from 2.7 MiB to 3.4.
Records are not in the heap, a `Properties` slice addresses the mapping, and what is
retained is a slice header. **The ceiling binds on node count and schema, not on bytes on
disk**, and the sizing rule is ~178 bytes of modelled heap a node with a peak charged figure
of ~300-310.

**A prediction stated in advance was wrong by 10.4%, and the correction is worth more than
the prediction.** Before the run the charged figure was modelled from two points as 58.7 MiB
fixed plus 244 bytes a node, predicting 477.6 MiB against a measured 527.2. The shape of the
claim held; the coefficients did not. The error was the intercept — fitting an affine line
across two fixtures that differ in blob size invented 58.7 MiB of fixed cost for a model that
has almost none, and the slope absorbed the shortfall. Every term above is proportional to
node count, so there is no intercept to find and the per-node figure is the one to quote.

Two gates in the release read off the same line. **Composites are 53.9% of modelled heap at
both shapes** — 164.8 MiB of 305.8 and 128.2 of 237.8, the same fraction to a tenth of a
point — which is the strongest evidence yet for putting the composite index in the image, and
confirms `residentBytesPerCompositeEntry = 48` twice at scale. **Label postings are 8 bytes a
node, 2.6% of the charged figure and half the adjacency term**, where `MEMORY_MODEL` §6.7 had
asserted without measuring that they were the larger of the two; deferring them behind a mode
would move a measured 2.6% and would move it onto `NodesByType` rather than remove it. That
open question is now answered with a reason not to build the option.

`MEMORY_MODEL.md` §9.7 carries the phase table and both fixtures term by term. §9.3's
1,400,000-node figures are marked superseded in place rather than replaced: the new reading
is 320.3 MiB settled and 397.8 charged against 560.8 and 613.9, but the old arm ran on linux
under a cgroup and the new one on windows under a Job Object, and the composite coefficient
moved between them, so the two are not a clean before-and-after and the document says so.

## v0.7.0 "Bookshelf" — the store stops carrying what it can read in place

### Indexed values can be served without the records that carry them

- **`ForEachNodeProjection` reads the property index instead of the payloads.**
  A caller that wants three indexed fields of a million records reads a million
  records to get them, and under a mapped image that means faulting in a million
  payloads to reach a few dozen bytes of each. The index already holds those
  values. `GetNodesProjected` is the materialising form;
  `ForEachEdgeProjection`/`GetEdgesProjected` are the edge forms; each has a
  `…Ctx`.

- **It reads the index, not the record, and says so.** The values are the ones
  handed to `IndexNodeProperty`. A key that was never indexed produces no
  callback at all — silence, not an error — and a non-indexed field still needs
  the record. Exact for indexed keys and silent about the rest is the trade that
  makes it cheap.

- **The crossover is the result, not a footnote.** Reading a record is one
  contiguous read and reading the index is a few scattered ones, so the record
  wins while the payloads are in page cache and loses once they are not. Four
  keys an entity, 25,000 nodes, medians of four rounds — **89 → 418 ns/record**
  at 512-byte payloads and **172 → 449 ns** at 4 KiB (the record path winning),
  then **6,728 → 502 ns** at 16 KiB and **32,556 → 559 ns** at 64 KiB (the
  projection winning by 13× and 58×). The projection column is flat across a
  128-fold change in payload because it never touches one.

- **Below the crossover it costs about 300 ns a record more than reading the
  record**, so it pays for itself as soon as the caller's own decode of those
  fields exceeds 300 ns. Above it there is nothing to weigh up.

- **It allocates less than reading the records at every size**: 434,704 B and
  **150 allocations** per 25,000 records, against 2,009,600 B and 25,002. Against
  the index's existing reverse read it is 3.2× faster on the delta path with 22×
  less memory and 394× fewer allocations, because naming the keys means four
  shard locks a chunk rather than sixteen a record.

- **It is not a point-in-time read of the whole pass.** Ids resolve in batches,
  so a write landing mid-pass may be visible for later ids and not earlier ones;
  each entity's values are read at one instant. A shard lock is never held across
  the callback, which is the deadlock `disk/scan.go` documents.

- No format change, no API break: purely additive.

### A batch read can be bounded by the bytes it hands back

- **`ForEachNodeBatch` is `GetNodes` with a ceiling on the payload in hand.** The
  size of `GetNodes`' answer is set by the caller's id slice and the store's
  blobs, and nothing in between gets a say — which is right for a few hundred ids
  and wrong for half a million against a store whose payloads run from a few
  hundred bytes to 64 MiB. There is no id count that is safe at both ends of that
  distribution, so the unit is bytes. `GetNodesBounded` is one step of the loop
  for callers driving their own paging, `GetEdgesBounded` and `ForEachEdgeBatch`
  are the edge forms, and each loop has a `…Ctx` form cancelled between batches.

- **A call always consumes at least one id, and that is the whole safety
  argument.** The budget is not consulted until a record is already in the batch,
  so a record whose payload alone exceeds the budget comes back on its own rather
  than being refused. Without that rule a store holding one 64 MiB blob read at an
  8 MiB budget returns an empty batch and the same remainder forever — a hang with
  no error and no output. The price is that the ceiling governs all but the first
  record of a batch, and the docs say so. It also makes a budget of zero or less
  need no special case: nothing fits beside the first record, so a budget that
  slipped negative yields safe-and-slow instead of the unbounded batch it was
  avoiding.

- **−41.8% of the Go heap held, and the file-backed row does not move.** At
  200,000 records with 512-byte payloads and an 8 MiB budget against a reopened,
  mapped store: Go heap **30.09 → 17.50 MiB**, anonymous **86.8 → 73.6 MiB**
  (−15.2%), total resident **185.5 → 172.4 MiB**, file-backed **unchanged** at
  ~98.8 MiB. The heap delta is 183,616 records no longer live at 64 bytes apiece
  plus the pointer slice — 12.6 MiB against 12.59 measured. File-backed residency
  not moving is the contract as a number: under a mapped image both arms alias the
  same payloads, so what the budget bounds is what the *caller* takes on, not what
  the engine allocates.

- **It is cheaper, not dearer, per walk.** Over the whole 200,000 records:
  **−9.45% B/op** (p=0.002, n=6) for **+11 allocs/op**, wall clock unchanged
  (p=0.818) — `GetNodesBatch` presizes a per-id index slice the bounded path never
  needs. The first version was 23.5% *worse* on B/op, because each batch's slice
  grew by doubling from nil; it now reserves from the first record's payload,
  clamped so that a first record much smaller than its successors costs a fixed
  512 KiB of over-reservation rather than one pointer per id in the request.

- **A store that consumes nothing is an error, not a spin.** A third-party
  `store.BoundedBatchReader` that returns as many ids as it was given stops the
  loop with `ErrBatchStalled`. Neither bundled backend can reach it.

- No format change, no API break: purely additive.

### A transaction can take a node rather than copy it

- **`Tx.UpsertNodeOwned` is `Tx.UpsertNode` without the defensive copy.** The
  root package copies every record it buffers, which neither backend needs: disk's
  `putNode` hangs the caller's `*store.Node` straight off the delta version chain
  and memory's `upsertNodeLocked` puts it straight in the map. The copy is there
  because a transaction buffers — the record has to live from the call until
  `Commit` — and refilling one node in a loop is an ordinary thing for a caller to
  write. This method gives that up for callers who can promise not to: once it
  returns, the node and the memory it points at belong to the store.

- **One allocation and one blob per node, and the blob is the point.** Against
  `UpsertNode` on the same tree, separate fixtures, twelve interleaved rounds each:
  **16.86k → 15.86k allocs/op (−5.92%)** at a thousand nodes a
  transaction, and **5.387 → 4.898 MiB/op (−9.07%)** at 512-byte blobs,
  **492.4 → 429.9 MiB/op (−12.69%)** at 64 KiB. `copyNode` allocates three
  times — struct, labels, properties — but only one of the three shows up as a
  saving, because a caller who hands the node over makes it escape and pays for
  the struct and the labels itself. What is genuinely removed is the blob: the
  byte delta is 512.04 B per node at 512-byte blobs and 65,536 B at 64 KiB, which
  is the payload and nothing else. Wall clock is unchanged (p=0.114, p=0.128).

- **Where it is worth having: a large transaction held open.** A 200,000-node
  transaction measured while still buffered, one process per measurement, seven
  rounds — Go heap **267.7 → 170.0 MiB**, a delta of 97.7 MiB that is exactly
  200,000 × 512 B; anonymous memory **300.3 → 144.5 MiB** (−51.9%) and process
  peak **342.2 → 210.1 MiB** (−38.6%), both moving further than the heap does
  because a smaller live heap lowers the GC's goal as well. File-backed residency
  is unchanged at zero.

- **Ownership is of the node, not of everything passed.** The unique key's value
  and the index-entry map are still copied, deliberately: a digest in a reused
  scratch buffer and one entry map refilled per entity are both patterns a caller
  writes without thinking, both would break silently, and both are small beside
  the blob. A test asserts the two copies survive, so a later tidy-up has to delete
  the reason before it can delete the copy.

- **Nothing detects a broken promise.** A caller that writes to the slice after
  handing it over changes what the store answers with, with no error at `Commit`,
  at read, or at `VerifyIndexes` — the bytes are self-consistent at every layer.
  That is why it is a second method rather than a flag, and why the documentation
  says to use `UpsertNode` unless a measurement says the copy matters.
  `n.ID` is set to the resolved ID, which is the one visible effect on the
  caller's struct and the other way the two methods differ.

- **`UpsertNode` itself is unchanged, and that took work.** The first version gave
  both methods one body, which made `n` a leaking parameter and cost every
  existing caller two allocations a node — more than the new method saves. The two
  now share only the key resolution, which never sees `n`. Ten interleaved rounds
  against HEAD confirm `UpsertNode` is untouched: allocs/op and B/op identical,
  wall clock within noise. `TECHNICAL_DETAILS.md` §14.27.

- No format change, no API break: purely additive.

### A composite index stops holding a second copy of its members' values

- **The largest thing a default configuration held was a Go map of per-entity
  structs.** `MEMORY_MODEL.md` §8.4 named the composites as the one term no
  residency option moves — 61.04 MiB of the 90.9 a fully mapped store holds, two
  thirds of it, and 427.2 MiB of the 537 at the consumer's shape — and §8.6 said
  the 160 B per entry was the coefficient anything replacing it would have to
  beat. Measuring the two halves separately says where that coefficient lives:
  over 200,000 entities of a width-2 composite, the postings cost 11.02 B per
  entry and the member values **135.65**, and the member figure does not move
  with the tuple shape. It is about 85% of the term, and what it holds is values
  the index already has — one `*memberState` per entity, being a struct, a
  backing array of width string headers, and a freshly converted copy of every
  value's bytes, reached through a pointer in a map.

- **An entity's values are now a row of int32 references.** `slots` holds width
  of them per row, laid out row-major, into a value table that interns each
  distinct value once and counts its holders. Rows and value slots each have a
  free list, so a store that deletes its derived layer and writes a new one reuses
  both instead of growing either. Measured interleaved against HEAD over twelve
  samples, per (id, tuple) entry: **146.60 → 43.87 B, −70.08%** at the thousand
  distinct tuples the program's own fixture declares, and 262.6 → 257.4 B,
  −1.98%, at the degenerate shape of one distinct tuple per entity.

- **On a real process it is 35 MiB off a 241 MiB store.** Seven interleaved
  rounds, one process per measurement, 200,000 nodes with eight unique keys, five
  ordered and two composites, each arm opening the same fixture from disk:
  anonymous memory **124.40 → 89.08 MiB (−28.39%)**, Go heap **66.31 → 33.09 MiB
  (−50.10%)**, process peak 197.9 → 168.2 MiB (−15.01%), and resident bytes per
  byte of store **0.795× → 0.650×**. File-backed residency is 67.50 → 68.15 MiB,
  which is noise on an unchanged number (p=0.097) and is the right answer: no
  byte of the image moved.

- **The reverse direction is not a map, because the first version's was and it
  cost more than the rows saved.** Interning needs a value-to-reference lookup,
  and `map[string]int32` measures at ~96 B per distinct value — so on exactly the
  composite `postings.canonical` warns about, a member key whose values rarely
  repeat, the first attempt came out **16.6% worse per entry than the form it
  replaced**. `canonical`'s own answer is to intern only a value's second entry,
  which needs a count this side has no way to get before the lookup that would
  establish it. So the reverse direction is an open-addressed set of references,
  probed by hashing the value: 4 bytes per bucket at a load factor of one half,
  8 B per distinct value against 96, which turns that arm from +16.6% into
  −1.98% while the shape composites are actually for stays at a third.

- **What it costs is build time on the shape nobody should declare.** Registering
  200,000 entities into a composite whose every tuple is distinct is **+39.18%**
  (p=0.000, twelve samples); at the fixture's shape it is unchanged (p=0.932).
  The regression is the rehash, which re-hashes every live value on each
  doubling. Caching each value's hash beside it was built and measured: it bought
  8% of that time and cost 3.3% of that shape's bytes, which is the wrong
  direction under this programme's priority, so it was taken out again.

- **`EstimatedResidentBytes` moves with it, and the composites are still
  resident.** `residentBytesPerCompositeEntry` goes from 160 to 48 — the old
  figure scaled by the measured ratio at the shape it was chosen against —
  and `TestEstimatedResident_WithinBand` holds. Nothing here puts a composite on
  disk: it is still rebuilt from the base's member keys at open and still held
  under every one of §8's twelve specifications. What changed is the coefficient,
  so the follow-up §8.6 asks for now has 48 B per entry to beat rather than 160.

- **No format change and no API change.** `DeclareCompositeProperties`,
  `NodesByProperty` and the planner's composite driver are untouched; the whole
  change is behind `compositeIndex`. Four tests cover the parts that were not
  reachable before — the value table's free list, the row free list, an overflow
  list outliving its row, and the tombstone an open-addressed probe needs —
  and three mutations (a removal that empties its bucket instead of tombstoning
  it, a released row that keeps its overflow lists, and one that keeps its
  values) are each caught by the test written for it.
### A property walk no longer holds the lock its callback needs

- **`ForEachNodeProperty` ran the caller's function under a shard read lock, and
  that was half of a deadlock.** Every store write path takes `s.mu` first and an
  index shard lock second — `IndexNodeProperty` → `indexNodePropertyLocked` →
  `IndexNodeUnique` — so a walk holding a shard lock while the callback reads the
  store closed an AB-BA cycle. It took about a second to reproduce against a plain
  `IndexNodeProperty` write with no compaction involved, which is also how it was
  established that this predates the change above rather than being caused by it:
  that change widened the window, because a compaction now takes all sixteen shard
  locks under `s.mu`, but the inversion was already there. Everything built on the
  walk was exposed and all of it was doing the obvious thing with the id it had been
  handed: `bulk`'s export checks each entry's node exists before writing it,
  `graphene export graph` and `Graph.ForEachNodeProperty` go through the same code,
  `VerifyIndexes` enumerates against the store, and the memory backend's snapshot
  walk has the identical shape.

- **The fix is the shape the codebase already had, and its own comment had already
  named this.** `IDStream` copies a key's delta side out from under the shard lock
  rather than holding it across the consumer, and `index/stream.go`'s header says
  why in as many words: "a shard lock held across caller code is the deadlock
  `disk/scan.go`'s header describes". The entry walks now do the same — copy one
  key, release the lock, merge the immutable base and yield with no index lock held.
  Rejected on the way: holding `s.mu.RLock` across the walk (turns the cycle into a
  writer stall for the length of caller code), inverting to a per-node
  `NodeEntriesOf` (quadratic on the export path), and a fully resumable entry
  stream (the same result for several hundred more lines).

- **The base is loaded after the delta copy, and that order is load-bearing.**
  `SwapBase` installs the new base and then empties the shards, so a copy taken
  before the clear pairs with either base and is a superset either way, while a base
  read before the copy pairs a pre-swap base with a delta that has since been
  emptied — neither half, and no error. The window between the two is two
  instructions wide, so the concurrent test could not see the mutation that swapped
  them; it survived ten runs under `-race`.
  `TestForEachNodeEntry_LoadsTheBaseAfterCopyingTheDelta` performs `SwapBase`'s two
  halves by hand between the copy and the load instead, and reports "the walk
  answered [1], want [1 2]" three times out of three.

- **The copy costs 28 bytes per entry of the largest key, and nothing on a
  compacted store.** No value bytes are copied at all: the copied slice holds string
  headers into the shard's interned values, which is why the figure is 28 and not 28
  plus the value. The buffers are reused across a walk's keys, so what is retained
  is one key's worth and not the index's — at the 50,000-node never-compacted arm
  that is 1.34 MiB, released when the walk returns. Measured per-op bytes over four
  counts in each of three interleaved rounds: a store whose index is entirely in the
  base pays **+0.02%** (798.4 → 798.5 KiB) and one in the ordinary mixed state
  **+21%** (894.4 → 1,083.3 KiB), while one that has never compacted pays **+348%**
  (800.2 → 3,582.4 KiB), which is the case where the delta *is* the index.
  Allocations follow: 10 to 53 per op on the never-compacted arm, +0.01–0.23%
  everywhere else. The walks that keep the shard lock — `ForEachNodeValue`,
  `ForEachEdgeValue` and the value walkers a v9 compaction writes its index
  through — keep it deliberately, because their callbacks are the engine's own
  scans holding `s.mu.RLock` already, and charging the streaming query path for a
  copy is the opposite of what it exists for.

- **The wall-clock half is not reported, because it was not resolved.** Two
  interleaved A/Bs against a HEAD worktree, twelve samples per arm, the second with
  fixed iteration counts to remove `b.N` scaling as a variance source: not one of
  twelve per-arm comparisons reached significance (p from 0.091 to 0.843), spreads
  ran to ±260%, and the two geomeans came out **+7.06% and −18.38%** — opposite
  signs. Two runs disagreeing about direction is the instrument failing on this
  machine, not a result, so there is no timing figure here. The per-op byte figures
  above reproduced byte-identically across both runs, which is the reason to trust
  them and not the other column.

- **No format change and no API change.** `index.PropertyIndex` keeps every method
  signature; the new `deltaCopy` is unexported and threaded through the walks that
  need it. Invariant §15.15 states the rule the fix restores, so the next walk added
  here has something to be checked against.
### A compaction gives its own index back, without being reopened

- **The index a compaction writes is now the index it reads from.** A base was
  attached on the load path and nowhere else, so a compaction wrote every entry the
  property index held into the image and then went on holding all of them in the
  delta shards. Nothing gave that memory back short of reopening the directory. The
  commit now installs the base it has just written and empties the shards behind it.
  Measured on a 50,000-node store whose derived layer is deleted and rewritten whole,
  the index term falls from **83.8 MiB to 15.3 MiB at the compaction** — the same
  figure the store reports after a reopen, to the tenth of a mebibyte — and anonymous
  memory held over a reopened store falls from **100.2 MiB to 41.1 MiB**. What is
  left is the record payloads, which need the aliasing contract changed and are a
  separate decision.

- **It happens only when the compaction wrote everything the index holds.** If a
  commit lands between the pin and the drain, the image cannot hold every entry —
  one registered after the stream is not in it, and one naming an entity committed
  after the pin was filtered out of it deliberately — so the store keeps answering
  from the shards and gives nothing back until the next quiet compaction. Both cases
  are exactly "the log grew", because every index mutation is journaled before it is
  applied, so the gate is the log tail and costs nothing to evaluate. The floor is
  no worse than before, never a wrong answer.

- **Nothing here can fail a compaction.** The image is written, fsynced and renamed
  before any of this is attempted. A mapping that cannot be made or an index section
  that will not parse leaves the store answering from the resident index it already
  had, and reports `MetricIndexFallback` saying why. Refusing a compaction that
  committed, in order to announce that an optimisation did not apply, would turn a
  memory result into a durability event.

- **A property lookup after a compaction now costs what it costs after a restart.**
  On a 5,000-node store, `NodesByProperty` measures 98–124 ns once the compaction has
  adopted its own output and 95–101 ns on the same store reopened. Before this
  change the same store answered in 70–79 ns after compacting and 112–117 ns after
  reopening — it was faster only in the window where it was holding a second copy of
  its own index, and it paid 68 MiB per 50,000 nodes for that window. Compaction wall
  clock, `CompactStall` and the point-lookup control are all unchanged within noise
  over six interleaved rounds; the compaction allocates ~180 more objects and ~9 KB
  more per run.

- **Bounding the delta starts working, which is the larger result.** `MaxDeltaBytes`
  was measured as something that "works and buys almost nothing", and that was a
  consequence of this bug rather than of the knob: every interim compaction moved
  entries out of the delta and into a base nobody read back, so the peak followed
  the sum and not the bound. Under the ceiling harness at 200,000 nodes and a real
  2 GiB Job Object, with four interim compactions, the charge against the ceiling
  falls from **829.9 MiB to 520.0 MiB** and headroom from 59.5% to **74.6%**; the
  process peak falls from 1,008.8 MiB to 799.0 MiB. Unbounded on the same fixture,
  anonymous memory after compacting falls from 576.3 MiB to 335.9 MiB. `MEMORY_MODEL.md`
  §9.6 has both tables; §9.4's figures at 1,400,000 nodes are left as the record of
  the engine before this and have not been taken again.

- **The image is mapped through the handle that wrote it.** The first version opened
  the finished file a second time to read its index back, which measured as **28% of
  the whole compaction's CPU** at a thousand nodes on Windows — a file that has just
  been written is expensive to open, and `writeStreamSync` was closing a perfectly
  good read-write handle a moment earlier. It now hands that handle to an `afterSync`
  hook, which also puts the mapping and the parse in `build`, where no lock is held,
  rather than in the commit, where the store is shut to readers and writers.

- **`PropertyIndex.SwapBase`, and why `AttachBase` could not be used.** `AttachBase`
  is a compare-and-swap against nil by design: attaching a second base to an index
  whose delta still describes the first is how an entry goes missing. `SwapBase`
  states the precondition instead — the new base holds exactly what the index
  answers with — installs the base *before* clearing the delta, so a reader between
  the two sees a superset rather than neither half, and keeps every declaration
  while replacing the structure under it. `PropertyIndex.DeltaEntryCounts` reports
  what the shards alone are holding, which is the figure that now returns to zero at
  every quiet compaction.

### Measured under an actual RAM ceiling, not projected

- **`tests/ceiling_test.go` runs the consumer's own sequence under a real memory
  limit** — open, scan ten rows through the index, enumerate the live set, rebuild
  the derived layer, compact, reopen and check the result is still there. cgroup v2
  `memory.max` on linux, a Job Object `JOB_OBJECT_LIMIT_JOB_MEMORY` on windows, and
  two nightly CI jobs. `RLIMIT_AS` is deliberately not used: it counts the mapped
  image's address space, which is the term this whole programme moved *out* of the
  constrained class, so a run under `ulimit -v` would refuse a store that fits.

- **The run refuses to report unless a ceiling is in force.** It reads the limit back
  from the kernel and fails when there is none, or when the one in force is looser
  than the figure the run claims. A CI step whose limit silently failed to apply
  would otherwise run the workload unconstrained and go green — the same failure as
  an option asked for and not held, which is why `StorageStats` reports what is
  *held*. The peak is reported and never asserted: a process that exceeded its
  ceiling is already dead, so the assertion could only ever run once it had passed.

- **The read path reaches the target, measured at the target shape.** 1,400,000
  nodes in a 1,689 MiB v9 image with 18,200,000 index entries open and are read in
  **560.8 MiB of anonymous memory**, charging 613.9 MiB against a real 2 GiB limit
  and finishing with **70% headroom** — while holding a 1,354 MiB working set, the
  difference being the mapped image the ceiling does not count. `MEMORY_MODEL.md`
  §8.6 projected 537 MiB for this configuration and `EstimatedResidentBytes` reports
  537.1, so the model is confirmed by direct measurement rather than by scaling. The
  §6.4 estimate of ~1,113 MiB came from multiplying a 200,000-node arm by seven and
  **over-predicts by 1.98×**, because the fixed part of an anonymous reading does not
  scale.

- **A whole-layer rebuild does not reach it, and the delta is not the reason.**
  *(Measured before the section above; bounding the delta works properly now — see
  `MEMORY_MODEL.md` §9.6.)* Rebuilding all 1,400,000 nodes in one process holds
  **3,853 MiB** and peaks at 6,644 MiB. Bounding the delta with
  `CompactionPolicy.MaxDeltaBytes` works — nine interim compactions, the delta held
  at 56.6 MiB — and buys almost nothing. The
  cause is that **a compaction leaves its own output resident until the store is
  reopened**: the graph it publishes was built in the heap, and `AttachBase` is
  called only on the load path, so the 18,200,000 entries it just wrote stay in the
  shards. Anonymous memory is 3,838 MiB after compacting and **1,078 MiB after
  reopening the same directory**. The arrangement §8 measures is therefore one a
  process is in after Open and until its first compaction; a long-running writer
  leaves it and does not return. `MEMORY_MODEL.md` §9 has the full accounting. The
  index half of the remedy is the section above and is now what a compaction does;
  the image half — re-mapping and republishing the graph from the file just written
  — changes what a returned `Properties` slice aliases and is not shipped.

- **The nightly runs both arms and neither hides the other.** Read-only at 1,400,000
  nodes under 2 GiB, the whole sequence at 200,000 nodes under the same 2 GiB where
  it finishes with 53% headroom. Fixtures are built first and outside the limit,
  because a 1.4M-node build peaks at several times what the finished store costs to
  open.

### What each residency option costs, measured together rather than one at a time

- **`docs/MEMORY_MODEL.md` §8 is the per-configuration table.** `ImageMode`,
  `IndexMode` and `Adjacency` each had a section describing them alone. This is the
  cross-product: which of the seven terms each option moves, by how much, and what
  the combinations do. On a 200,000-node store with 2,600,000 index entries the
  three are independent and additive — **273.26 MiB** for a mapped index,
  **149.44 MiB** of heap for a mapped image, **6.11 MiB** for deferred adjacency —
  and the cheapest configuration holds a quarter of the anonymous memory of the
  dearest, on a store neither of them changed.

- **A live reader that sets nothing pays 3.8×.** `ImageMapped` maps only where
  something excludes a concurrent writer, and a live reader holds no lock by
  construction, so it falls back to a heap image — and a heap image then declines
  the mapped index, because an index read out of a heap buffer would pin the whole
  file to save part of it. Two fallbacks from one unset option: **568 MiB against
  149** on the fixture above. Both defaults are right and the price of them was not
  written down anywhere. `ImageMappedUnlocked` is how a live reader asks for the
  other trade, and §8.5 is now what it costs not to.

- **Twelve specifications, six configurations.** Four collapse because a heap image
  cannot carry a mapped index; four more because `ImageMappedUnlocked` and
  `ImageMapped` are the same thing to an opener that holds a lock. The new
  `BenchmarkRSS_ModeMatrix` reports what a store *holds* beside what was *asked*
  for precisely so that a table cannot report a fallback as a result.

- **The composites are the largest remaining item, and now have a figure.** 160 B
  per entry, resident under every one of the twelve specifications, and 61.04 MiB
  of the 90.9 that the cheapest configuration holds — two thirds of it.

### One figure for what a compaction holds while it builds

- **`CompactOptions{MaxWorkingBytes}`**, set through `Options.Compact`, sizes the
  four intermediates a mapped-index compaction holds by choice rather than by the
  store: the GPIX value table's in-memory cap, the GPIR sort chunk, and the two
  bounds on the merge that drains it. One figure rather than four, divided in the
  proportions the constants already stood in. Zero is the default and reproduces
  today's four numbers exactly, which is asserted rather than intended.

  The floor is 524,288 bytes, below which the sort chunk falls under the flush
  buffer of the spill that writes it and the figure would stop naming what it
  bounds. A smaller value is refused by `Open` with **`ErrCompactWorkingBytes`**,
  before the directory is created — rounding it up silently would be the failure
  the option exists to prevent, in miniature.

- **The image is byte-identical at every setting.** Each of the four decides
  whether something is held or written to a file and read back, and a spill may
  change where bytes live and never what they are or what order they come in.
  Asserted directly by compacting the same fixture at three settings and
  comparing the images, which is also what caught that the claim rests on the
  *merge's* comparator rather than the sort's: a tie spanning two runs is broken
  by the merge, and a merge that broke it by run would make the chunk size decide
  the bytes.

- **What it is worth, measured rather than assumed.** The plan that scheduled
  this assumed the spill and the merge were a meaningful part of the 15–24% a
  mapped index costs a compaction. Three interleaved rounds at 50,500 records say
  they are not. Lowering the figure to the floor saved **3.32 MiB** of allocation
  against a modelled 3.62 MiB and cost **12%, 16% and 42%** of the compaction's
  wall clock; raising it to 64 MiB cost **38.04 MiB** and moved the wall clock by
  **+18%, 0% and −12%**, which is noise. Fifteen times the memory bought nothing
  measurable. This is a small lever, and it is now labelled as one — for a
  machine whose limit is tight enough that four megabytes is worth three tenths
  of a second, and not for a slow compaction. `BenchmarkRSS_CompactWorkingBytes`
  is the instrument, one setting per process.

- **It also closed an under-report in the memory budget that shipped a commit
  ago.** `compactWorkingSet` charged a compaction nothing for the mapped index,
  on the finding that both payload encodings stream. True of the entries and
  false of the machinery: the four intermediates are 4,325,376 bytes that the
  build genuinely holds, and they are now the last term. The reason they could
  not have been charged before is the reason the option is worth having — there
  was no figure to charge, only four constants in two files with no owner.

### A memory budget, which is a refusal and never a degradation

Go cannot catch an out-of-memory condition. There is no allocation failure to
handle, no error returned from `make`, and no warning before the kernel's OOM
killer: the process stops mid-write with no chance to flush or record what it was
doing. So "fails predictably under memory pressure" has exactly one available
meaning here — decide before allocating, and decline.

- **`Options.MemoryBudget`** bounds the heap an `Open` or a `Compact` may need.
  Zero, and any negative value, is unlimited and is the behaviour of every version
  before this one. Exceeding it returns an error wrapping **`ErrMemoryBudget`** and
  carrying **`MemoryBudgetError{Op, Need, Have, Budget}`**, because a refusal that
  does not say by how much cannot be acted on: the difference between needing 5%
  more and needing six times as much is the difference between raising a limit and
  buying a machine.

  It does not shrink a cache, spill a merge, reduce a batch, or degrade a plan, and
  no later version will make it do any of those silently. An engine that quietly
  got slower instead of saying no is the outcome the option exists to prevent.

- **An `Open` is refused beside the replay budget**, on `HeapBytesFor` for the
  Options actually being opened with rather than on the worst case over them — the
  same store models at 736 KB under the defaults and 1.45 MB under `ImageHeap` with
  a resident index, so gating on the worst case would decline work that fits by
  more than a factor of two. Nothing has been replayed and no ledger is open at
  that point, so a refusal is repeatable and leaves no trace: a routine outcome
  that appended a forensic record of a crash that did not happen would be
  manufacturing history.

- **A `Compact` is refused after the pin**, which is the first moment the counts
  are real and the last before anything has been allocated for the build. It is
  *not* recorded as a compaction that failed, for the reason
  `ErrCompactionInProgress` is not: a background compactor reporting a refusal as a
  failure gives an operator an error rate made entirely of the trigger working. It
  arrives at `AutoCompactObserver`, and it recurs on every tick, because the
  refusal is what stopped the delta from shrinking — attach an observer if you set
  a budget.

- **What a compaction actually duplicates**, which had no model anywhere before
  this and turned out not to be what the obvious one says. Not the store:
  `buildSeq` copies record *values*, and a record value is two slice headers, so
  every record the delta did not touch arrives in the new image addressing the
  same arena or the same mapping. **The property blobs are not duplicated by a
  compaction at any size.** What is duplicated is structure — the record arenas
  holding the headers, the page tables addressing them, the by-label postings, the
  adjacency arrays — all of which follow records and identifiers rather than bytes
  written. The index is absent because both payload encodings stream, and
  serialisation streams into the temp file rather than through an image-sized
  buffer.

  Measured against the image its own build produced: **1.00x** on a dense
  identifier space, on a blob-carrying store, and on a heap image; 1.24x when the
  delta is written to a band of identifiers the image does not touch, and 1.62x on
  a sparsified space. Never below 1.00x.

  One term was wrong by 62 bytes in the direction the posture forbids, and only a
  fixture built to kill a mutation found it: a delta introducing label values the
  image had never seen grows the by-label *maps*, not only their id lists, so the
  bound is now taken over the union of the two sides' label sets rather than from
  the pinned image's.

### The pre-open estimate now describes the store you are about to open

`PreflightOpen` answers "what will this cost" from a header, in memory bounded by a
constant. It was answering it for a configuration nobody runs, and by a margin that
went both ways — measured against the opened store on a mode matrix, the model ran
between 0.51x and 1.72x of what the store actually held. `estimate.go`'s own header
states the posture it was breaking: a figure an operation is refused on must be wrong
in the direction that declines work that would have fit, never in the direction that
admits work that will not.

- **`OpenEstimate.HeapBytesFor(opts)`** is the figure that depends on Options, and
  the reason it has to exist is that the same store differs by more than a factor of
  two across them. `ImageMode` decides whether the property blobs are copied into
  arenas or addressed in the file; `IndexMode` decides whether the index costs 108
  bytes an entry or 88 bytes a key; `Adjacency` decides whether two arrays per node
  slot exist at all. The three modes are resolved the way an `Open` resolves them,
  which matters most where the answer is not the mode that was asked for — a live
  reader holds no lock and therefore reads a heap image under the default
  `ImageMode`, and a v8 image is read into a resident index whatever `IndexMode`
  says, because GIDX is the only form it has.

  Measured against the store it predicts: 1.00x under the defaults on both a dense
  and a sparse identifier space, rising to 1.38x on a v8 image in the heap, where the
  looseness is the format's — GIDX bounds its entry count by the section's length
  where GPIR states it.

- **A v9 image's property index is read at last.** The preflight matched `GIDX` and
  nothing else, so on every store the current defaults produce it reported *zero*
  property entries and omitted the term that dominates an indexed store. v9 carries
  GPIX and GPIR and no GIDX, deliberately. The new fields — `ImagePropertyEdgeEntries`,
  `ImagePropertyKeys`, `ImageIndexMapped` — come out of those two headers, and the
  counts are better than v8's: GPIR states both exactly where GIDX leaves the edge
  half past every variable-length node entry.

- **`WALHeapBytes`** is the log half, and it closes the case the whole surface exists
  for. A store that has never been compacted has no image, so every image field was
  zero and the only heap figure on the struct read as nothing — for the one store
  shape that replays its entire history into memory on every open.

- **`ImageHeapBytes` will report a larger number than it did**, for two reasons that
  were both under-reports. Record slots were capped at the identifier space, which a
  paged layout does not obey: a page is materialised whole, so an image whose
  identifiers stop at 2,000 holds 4,096 slots and the model said 2,001. And the
  inverse page table, the live-before prefix and the by-label postings were not
  counted at all. Its documentation now says what it is — the worst case over
  Options, not the default one.

  It is also *tighter* on the workload this program was commissioned for. The record
  stream is ascending by identifier, so one addressed read of the first record bounds
  the materialised pages from below. A rebuild cycle deletes the low identifiers and
  writes new ones at the top; a bound counting from zero charges for every page in
  between.

- **`estPropertyEntryBytes` raised from 107 to 108**, because the index's own
  `ResidentBytes` reports 107.128 bytes per entry on an all-distinct key and a
  constant meant to bound it was below it.

### Compacting on what the delta holds, not on what it counts

`StorageStats.DeltaBytes` arrived with nothing able to act on it. Its own
documentation named a `CompactionPolicy.MaxDeltaBytes` that did not exist, which is
a fair summary of the gap: the engine could measure the thing that runs a machine
out of memory and could only be told to compact on proxies for it.

- **`CompactionPolicy.MaxDeltaBytes`** is the rule on the figure itself. The other
  three are proxies: a hundred records holding 64 MiB blobs and a hundred thousand
  holding none are the same `MaxDeltaRecords` and four orders of magnitude apart in
  cost, and `MaxWALBytes` measures what has been written rather than what is still
  held — a store that overwrites the same records repeatedly trips it while holding
  almost nothing.

  **`DefaultCompactionPolicy` now sets it, to 128 MB.** That is a behaviour change
  for anyone using the default with `AutoCompact`: a blob-heavy store will compact
  sooner than it did. The figure is half the log limit rather than equal to it, and
  not for symmetry — every delta record was logged, so the log is a superset of the
  delta and a byte limit equal to it would almost never be the rule that fired.

- **`Graph.CompactIfDue(policy)`** and `CompactIfDueCtx` are the loop every caller
  was writing by hand: evaluate, and compact if the answer is yes. Both halves
  already existed; putting them together is what stops a caller evaluating one
  policy and compacting on another, or compacting having forgotten to ask. The rule
  that fired comes back whether or not the compaction succeeded, because that is the
  line an operator needs in a log when it did not.

- **`Options.DeltaSoftLimit`** is for the deployment that compacts on its own
  schedule and wants to know when the schedule is not keeping up. Soft is the
  contract: no write fails, none is delayed, nothing is spilled or degraded.
  `StorageStats.DeltaOverBudget` is the polled form and `MetricDeltaOverBudget` the
  pushed one — once per crossing rather than once per commit, re-armed by a
  compaction that resolves it. A limit that refused writes would be a worse feature
  than none: the delta is where a commit goes, so refusing to grow it means refusing
  data the caller has nowhere else to put.

- **Identifiers survive a compaction, and that is now written down as a guarantee**
  rather than left as something true of the implementation. A `NodeID` held across
  any number of compactions names the same record; identifiers are never reused and
  the sequence counters have high-water marks in the image header, so a reopen does
  not reissue them either. A `[]byte` a read returned under a mapped image survives a
  compaction too; what it does not survive is `Close()` — see `ImageMode` and
  `store.CloneNode`.

### The store can say what it is holding

Until now the only way to find out what a graphene store cost in memory was to measure
the process from outside. That is the right instrument for a document and no use to a
running store, which cannot stop and re-measure itself before deciding whether to
compact — and it is exactly what the consumer that prompted this work had to do, arriving
at 9,290 MiB with no way to ask the engine where any of it went.

`StorageStats` now carries two figures, and `disk.Store.EstimateResident` breaks the
second into its seven terms.

- **`DeltaBytes`** — what the delta's records hold: version cells, records, labels,
  blobs. The figure the delta counts cannot give, since a hundred records carrying large
  blobs and a hundred thousand carrying none are four orders of magnitude apart in cost
  and identical in `DeltaNodes`. Maintained on the write paths, so reading it is free, and
  recomputed by `VerifyIndexes` so it cannot drift unnoticed. A tombstone costs its cell
  alone, so deleting lowers it.
- **`EstimatedResidentBytes`** — record arrays, payload, label postings, adjacency, index,
  delta and the log ring, summed. `StorageStats.IndexEntries()` totals the indexed triples.

**Reading it costs nothing**, which is a requirement rather than a nicety: `AutoCompact`
evaluates its policy against these figures on a ticker, so the call is bounded by the
number of declared keys and distinct labels and not by the store. Measured flat across a
25× change in store size.

**It is retained heap, not resident set size.** Resident ran 1.19–1.54× live heap across
three runs of an identical store, so read it as a floor and a ratio; `store stats`,
`store health` and `debug stats` print the ratio beside the figure. Compared against
retained heap at three sizes the marginal cost per record agreed to within one percent —
**78.8 B modelled against 79.4 measured** — and the estimate was a floor at every size.

**Mapped image bytes are reported separately and are not in the total**, because page
cache the kernel may evict is not memory the process must keep, and a total that included
them would say mapping the image made the store more expensive.

What the breakdown shows about this engine, on a fixture of 32,000 records with two
indexed keys each: the property index is **236 bytes** — for 64,000 entries, against the
6.85 MB it would cost held resident, and the figure does not move between 2,000 records
and 32,000 because nothing in it is a function of entries. Records are now the largest
term at 57 B each, and the 256-byte blobs are absent from the heap entirely.

### A property read that yields its ids instead of returning them

`NodesByProperty` answers with a slice, which is right for a lookup and wrong for a
pass: one key held by half a million nodes is a four-megabyte array allocated so that
a caller can walk it once and drop it. The array is not incidental — the index builds
it by merging the image's run of identifiers with the delta's, and it sizes it from the
sum of the two.

`NodesByPropertyFunc`, `EdgesByPropertyFunc` and `ForEachNodeID` (each with a `Ctx`
form) are the same questions asked so that the answer need not be held. Over 500,000
nodes under one value of one key, the property read goes from **4,005,888 B/op and one
allocation to 128 B/op and three**, and the query form from **8,011,936 B/op and four
allocations** — it allocates the answer twice, once as candidates and once filtered —
**to the same 128 B/op**. Sampled halfway through the pass, the process holds **5.6 MiB
less**. It costs **1.34x to 1.38x the wall clock**: a cursor call and a callback per
identifier where the slice form had neither.

**The callback may read the store.** That is the difference between these and
`NodesByPropertyBatch`, which holds the store's read lock for its whole duration. The
natural use of an identifier is loading what it names, so a pass that held the lock
across the callback would deadlock any caller that read the store from inside its own
loop — the same reasoning `Scanner` records. The pass collects a batch of identifiers
under the lock, filters the dead ones, releases, and only then calls out. The reader is
pinned for the whole pass, so the answer still comes from one instant, and once a
compaction has published a new image the pinned one is frozen and the pass stops taking
the lock at all.

`ForEachNodeID` streams exactly one query shape — a single equality filter, ascending,
no labels, no identifier list — and runs the ordinary query and walks its result for
everything else, because an ordering, a window from the end, a conjunction or a label
union all need the candidate set in hand. Which shapes stream is not part of the
contract; the answer is, and a test holds `ForEachNodeID` to `QueryNodeIDs` over fifteen
shapes on both sides of a compaction.

No format change, additive API, and both backends implement it — the memory store has
no disk-resident side to walk in place, so it materialises and walks, which is what a
caller gets from the facade's fallback anyway.

### Adjacency can be built on first use instead of at open

An image carries edge records, each naming its two endpoints. What a traversal needs
is the inverse — given a node, the edges incident on it — and that is four arrays the
loader has always computed while reading the file: sixteen bytes per edge, sixteen per
node slot, anonymous, for the life of the handle.

`Options.Adjacency` decides when. `AdjacencyEager` is the zero value and builds them at
open, as every version before this one did. `AdjacencyLazy` defers the build to the
first thing that needs it: a traversal, a degree query, an edge-of-node read, a delete
cascade, or a verification. On a 150,000-node, 900,000-edge store, a process that opens
it and reads every node by an indexed property — never walking the graph — holds **15.5
MiB less anonymous memory** and opens **11% faster**. A process that does traverse pays
exactly what it paid before, at a later moment.

It is not offered as a better default, because for a large class of callers it is not
one. `DeleteNode` cascades to incident edges and finds them through these arrays, so a
writer that deletes anything builds them on its first delete. The mode is right for a
read-only aggregate process and worth nothing in a writer, and only the caller knows
which it is running. `StorageStats.Adjacency` reports `"built"` or `"deferred"` so that
a caller can check its pass really did avoid them.

The build now reads the record arena rather than the sequence that filled it, which is
what makes eager and lazy one piece of code called at two moments rather than two that
have to be kept agreeing. A side effect: `buildSeq` walks its edge sequence three times
instead of five, and for compaction that sequence is a merge over an image and two
sorted slices. The compaction clock does not move — it is dominated by writing the
image — but the passes are gone, and a test asserts there are three of them.

No format change. The default's numbers are unmoved: a hot degree read is 18.25 ns
against the previous 18.62, anonymous residency is within 0.4 MiB, and a compaction is
within noise.

### Residual query planning knows what a probe costs on disk

A query with more than one filter is driven from the most selective one the index can
serve, and the rest are applied afterwards — each of them either by testing every
candidate against its own registered values, or by resolving the filter to its own set
and intersecting. Which is cheaper depends on how many candidates there are, and the
planner has always decided it by comparing that count against the size of the set:
probing cost one lookup per candidate, and a set element cost one.

That comparison stopped being true when the property index moved into the image. A
probe reads the *reverse* direction, and once that lives on disk it is a binary search
of a mapped array — eighteen reads over this release's own fixture, twenty-five at
the size this program is aimed at — while the set it is weighed against is built by
a sequential walk of the same file. Charging the probe one unit made the planner prefer
it across a band twenty-five times wide, which is to say for almost every residual
filter a compacted store has.

So a probe is now costed from the reverse section it would search, and a store with no
base on disk — `IndexResident`, the memory backend, anything before its first
compaction — charges one exactly as before and makes every decision it made before.
The second half of the same decision is a footprint bound: building the set
materialises it, probing materialises nothing, so a residual set past four million ids
is probed however slow that is rather than held.

**What it measures**, on a 200,000-node store with 2,000 candidates and a 20,000-entry
residual, five interleaved passes, minima: **the query is 2.8× slower warm** (394 µs
→ 1,094 µs) and **touches 6.6× less of the image** (5.58 MiB of mapped pages
faulted in → 0.85 MiB), leaving the process holding 4.6 MiB less after the pass.
Allocation falls from **4,006 to 30 per query**, which was not the point and is the
clearest number here: the probe's base half allocates two per candidate, a captured
bool and a closure that escapes because the callback crosses an interface.

Warm is where the probe looks good and it is not the case this is for. The pages are
the same count of four-kilobyte reads on an image that has not been read yet, where
they are disk seeks rather than cache hits; that arm is the proxy, because there is no
way to drop the page cache from a Go test on Windows. `ExplainNodeQuery` reports the
decision per residual step, so the route is visible rather than inferred.

No format change, no API removed. `QueryPlan` is diagnostic output and has always
said its choices may change; results do not.

### A property join resolves many values in one pass

`NodesByPropertyBatch(key, values, fn)` and `EdgesByPropertyBatch` answer a whole list
of exact values against one key, carrying a position through the image's value table
instead of searching it from scratch for every value. A consumer joining an external
table against an indexed digest calls `NodesByProperty` a million times today and pays
about twenty random probes over a sixteen-megabyte table for each of them; the values
a base holds are sorted, so an *ascending* list of wants walks them monotonically and
the pass becomes a sweep. `store.PropertyBatcher` is the optional interface, both
backends implement it, and `Graph`'s helpers fall back to a loop where a backend does
not — so this never fails for a missing capability.

It is a callback and not a `[][]store.NodeID` on purpose: a million values returned
that way is a million slice headers and a million backing arrays, which is the cost
this whole release exists to remove. `i` is your own position in the list you passed,
`ids` is scratch until the callback returns, values nothing holds produce no callback,
and the callbacks arrive in value order rather than input order — that last is the
observable trace of the sweep, and it is documented as unspecified rather than relied
on. Your slice is never permuted.

**What it measures.** 50,000 values, half of them absent, against a 50,000-node
fixture's all-distinct 32-byte key, one arm per process, three interleaved passes:
**2.7x faster than the loop when the values are already ascending** (98 ns/value
against 262) and **1.17x slower when they are not** (307 ns/value). Residency is
unmoved — 0.3 MiB more than the loop at 50,000 values, which is the sort scratch, and
0.13 MiB *less* on ascending input where there is none — and allocation is flat: two
allocations whether the batch holds 200 distinct values or 2,000. Isolated, the sweep
costs a flat ~30 ns per value whether the key holds 256 distinct values or 262,144,
against 24–77 ns for a search per value over the same range; the crossover is between
256 and 4,096.

The 1.17x is the honest cost of ordering the values, and it is stated rather than
buried: warm, the sort does not pay for itself. It is expected to on a cold image,
where a search per value is eighteen page faults instead of eighteen cache misses, and
that has not been measured — there is no way to drop the page cache from a Go test on
Windows. Supply ascending values and the question does not arise. See
`docs/TECHNICAL_DETAILS.md` §14.21 for the design, including the two measurements that
changed it.

**The point lookup got faster on the way.** `NodesByProperty` against a mapped base is
now **11% faster on an all-distinct key and 22% faster on a low-cardinality one** (ten
interleaved passes, minima 90.3 → 79.9 ns and 74.7 → 58.6 ns). A key's values are
strictly ascending and distinct, so a probe that compares equal *is* the answer; the
search returns there, which removes two probes and a run read that the old shape spent
confirming what it already knew. Getting there cost one false start worth recording:
factoring the value comparison into a single tidy method put it over the inliner's
budget and made the all-distinct lookup 11% *slower*, consistently, because a
bisection's every probe became a call. The prefix comparison is now its own inlinable
function and the run read is a call only where prefixes tie.

No format change, no API removed, nothing on a write path touched.

### `store migrate -to` writes either format, in either direction

The compatibility statement for v9 said `Options.IndexMode: IndexResident` plus one
`Compact()` is the way back to v8 and that the convenient form would follow. This
is it: `graphene store migrate -to 8` downgrades an image, `-to 9` upgrades one, and
a bare `migrate` writes what the build writes, as before.

It is not a second code path, and the reason matters more than the flag. Which
version a compaction produces is `Options.IndexMode`'s decision — v9 carries the
property index as GPIX and GPIR, v8 carries GIDX, and the container is otherwise
identical — so `-to` opens the store under the mode that version implies and
compacts once. A downgrade is therefore exactly as crash-safe as any other
compaction: the new image is renamed in only once it is complete and fsynced, the
version is read back, and the indexes are re-verified. There is no conversion
routine to get wrong, because there is no conversion.

Only v8 and v9 are writable, and the refusal happens at the flag rather than in the
handler, so `-to 7` — a version this build reads perfectly well and cannot write —
does not acquire the exclusive lock on its way to being rejected. The accepted set
comes from the new `disk.CSRVersionsWritable`, and `disk.IndexModeForCSRVersion`
is the mapping the tool opens by, so neither can come to disagree with the writer.
A test compacts under each mode the mapping names and reads the version back out of
the bytes, rather than checking one table against another.

**What the round trip preserves.** v9 → v8 → v9 over a store with an all-distinct
key, a low-cardinality key, a declared ordered key, a composite tuple and an edge
key: every entry in the index, every distinct value and its count, exact-match,
prefix, range and composite queries, all identical at each step. The index is
re-encoded rather than copied, so that is the assertion worth making; the version
number is checked as well, because a migration that quietly did nothing would pass
an answers-only test.

**Two fixes that came with it.** `-dry-run migrate` used to fail: the framework
downgrades the open to read-only under it, the handler compacted anyway, and the
operator was handed the engine's "store is open read-only" by a store they had
asked it not to write to. It now reports what it would do, exactly as `-check`
does. And `-check`'s own documentation claimed it read the header *without opening
the store*, so that surveying a fleet took no exclusive lock. That was never true —
the framework opens what a command declared before any handler runs — and the claim
is now corrected in both the code and `TECHNICAL_DETAILS.md` rather than quietly
dropped. Making it true means letting a flag downgrade the open mode, which is a
larger change and has not been made; `info`, `csr` and `wal` read the files
directly today.

**Framework, for the curious.** A command's options struct may now implement
`tuneOpen(*disk.Options)`, which the CLI calls after the ledger detection and the
`-metrics` bind and before it opens anything. It exists because the framework opens
what a command declared *before* the handler runs, so a flag that decides an Option
cannot be acted on by the handler that reads it. It may set fields and nothing
else: it cannot refuse an open, and `ReadOnly` is applied after it, so a tuner
cannot turn `-dry-run`'s downgrade back into a suggestion. `store migrate` is the
only command with one, and a test names it so that a second is a decision somebody
makes on purpose.

No format change, no API removed, no read or write path touched.

### `VerifyIndexes` now checks the image's own property index

A v9 image carries its property index as two sections read in place, and until now
nothing established that those sections were *consistent* — only that reading them
could not address memory outside them. Those are very different guarantees, and the
gap between them is where a file that opens cleanly answers wrongly.

**What is checked.** Per key: that the value table is tiled by its runs with no
gap and no slack, that each 8-byte value prefix is the prefix of the value its run
holds, that values ascend strictly, that a run's postings ascend strictly, that no
value is present with no postings behind it, and that the directory's distinct and
entry counts are what the runs actually hold. Per reverse entry: that it names a
key of its own kind, addresses the *start* of a real run rather than a point
inside one, agrees with that run's value length, and names an id that run lists.
Then, per key, that the two directions hold the same number of entries.

**The last two together are exhaustive, not thorough.** The reverse entries are
strictly ascending in `(id, key, value offset)`, so they are distinct, so the map
from a reverse entry to the forward entry it resolves to is injective; the counts
then say the two sets are the same size. An injection between finite sets of equal
size is a bijection, so the two directions are *proved* to describe the same
entries — with neither side materialised. That is the property a delete cascade
rests on: entries are found through the reverse direction, and a forward entry the
reverse direction has never heard of outlives its entity with nothing to notice.

**Why it cannot live anywhere cheaper.** The parser already checks every bound,
and that is O(keys). Everything above is O(entries), so doing it at parse time
would make every `Open` walk the whole index — which is the pass the mapped index
exists to stop paying, handed straight back at startup. Doing it per read would be
a linear scan dressed as a binary search. So it is paid once, by whoever asks:
`graphene verify`, `store migrate` after it rewrites an image, or a caller with a
store it has reason to doubt.

**Why it is not redundant with what was already there.** A value table prefix
edited to disagree with its value changes no entry, no record and therefore no
Merkle root. The digest matches, `VerifyCSRRoots` passes, the store opens and
serves — and the search that prefix steers goes down the wrong half and reports a
present value as absent. Every other check in the engine looks at content; this is
the only one that looks at the tables used to find it.
`TestVerifyIndexes_NamesDamageInTheImagesIndex` builds exactly that file, digest
repaired, asserts the other three checks pass, and asserts this one does not. The
same reasoning covers the two fields no read ever looks at — the value table's
sentinel prefix and the reverse entries' padding — because both are covered by the
digest, so a difference in either makes two compactions of the same content
produce different files.

**What it costs, and what it must not cost.** Five keys per node, interleaved
against a control tree, three passes:

| `VerifyIndexes` | 25,000 entries | 100,000 entries |
|---|---|---|
| before | 0.44–0.59 ms | 1.84–2.10 ms |
| after | 2.24–2.28 ms | 9.20–9.71 ms |

About **76 ns per entry**, flat between the two sizes. Allocation went from 240 B
and 6 allocations to 416 B and 13 — **the same at both sizes**, because memory is
one counter per declared key plus one buffer sized by the widest value and nothing
else. That is a requirement rather than a measurement: the store most in need of
this check is the one nearest its memory ceiling, and a verifier proportional to
the index would be unusable exactly when it was wanted.

Nothing on the read or write path moved. `Base.Verify` is a new method on
`index.Base`, which is what puts the check with the encoding that knows what it
promises rather than with whoever happens to hold the concrete type;
`PropertyIndex.VerifyBase` is the entry point and stays separate from
`PropertyIndex.Verify`, which checks the delta and is called far more often. A
store with no base — `IndexResident`, or a pre-v9 image — has nothing to check and
says so by doing nothing.

### The index lives in the image: `IndexMapped` and v9 are the defaults

**This is a format change and it is the default.** A store compacted by this build
writes v9, and no earlier build opens a v9 image — GPIX and GPIR are critical
sections, so an older reader refuses the file rather than answering property
queries out of an index it cannot read. `Options.IndexMode: IndexResident` plus one
`Compact()` writes v8 again and is the supported way back; `store migrate -to 8`
is the convenient form of the same thing — see the entry above.

Everything before this change made the mapped index *possible*. This is the one
that measures it against the alternative and moves the default, which is the only
change in the sequence that was ever going to be interesting.

**What it is worth.** The consumer's index shape — eight unique keys including a
32-byte all-distinct digest, five ordered, two composites, thirteen entries on
every node — reopened from disk through `tests/rss_bench_test.go`, interleaved
against a control tree, two sizes:

| after Open | 50,000 nodes (650k entries) | 200,000 nodes (2.6M entries) |
|---|---|---|
| Go heap | 92.14 → 17.38 MiB (−81.1%) | 362.0 → 66.32 MiB (−81.7%) |
| anonymous RSS | 149.9–157.8 → 80.3–91.4 MiB | 439.5–442.4 → 146.9–171.1 MiB |
| file-backed RSS | 41.6–41.9 → 24.1–25.0 MiB | 172.9–173.8 → 103.8–103.9 MiB |
| total RSS | 191.8–199.3 → 104.4–116.0 MiB | 613.3–615.2 → 250.7–275.0 MiB |
| image on disk | 44.0 → 60.5 MiB (+37%) | 176.0 → 241.4 MiB (+37%) |
| **resident per byte of store** | **4.36× → 1.73–1.92×** | **3.49× → 1.04–1.14×** |

The heap figure is the term that was moved and it is identical to two decimals on
both arms across every pass. The saving is **119–121 bytes per indexed entry** at
both sizes, against the ~107 the cost model predicts — the model is slightly
conservative because the 32-byte digest cannot be interned, which is exactly why
that key is in the fixture.

The last row is the one to read. A 200,000-node store cost 3.5× its own file in
resident memory and now costs 1.04×, and the file-backed half of that *fell* while
the file itself grew by 37%: rebuilding the index meant walking every byte of GIDX,
so the old arrangement made the whole image resident on the way past. Reading a
directory touches a fraction of a larger file.

**What it costs, reported rather than buried.** A warm point lookup roughly
doubles. `BenchmarkIndexMode_PointLookup` runs both arms over one file, three runs
at 60,000 entries:

| | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `IndexResident` | 118.6, 121.1, 120.5 | 8 | 1 |
| `IndexMapped` | 219.4, 222.0, 260.4 | 8 | 1 |

**Allocation is identical** — one slice of one id either way: the resident arm
copies the postings list out from under the shard lock, and the mapped arm merges a
run out of the image with an empty delta, which costs the same one slice. So the
whole of the trade is in the nanoseconds and none of it is in garbage.

The plan budgeted 0.3–0.8 µs for this, so +100 ns is inside the budget — but it
corrects the "+4 ns" recorded when the loader landed. That figure came from a
session whose *resident* arm happened to be running at 272 ns; the mapped arm's
~220 ns is the stable half of the comparison across both sessions. A delta between
two noisy arms is not a measurement. Cold is still unmeasured and is where the batch
API earns its place.

Compaction takes **2.76–2.82 s → 3.24–3.50 s (+15 to +24%)** at 200,000 nodes
and its peak memory does not move (709–715 MiB against 720 MiB): the counting
sort and the value-table spill trade wall clock for a bound they already had. Open is
*faster*, because the resident arm rebuilds millions of entries and the mapped arm
reads a directory: 81–108 ms → 21–28 ms at 150,000 entries.

**A real bug, and it was the probe path.** A residual property filter is applied
one of two ways: build the filter's own set and intersect it, or ask each candidate
what it is indexed under and test the answer. The second reads the *reverse*
direction, and the delta's reverse direction knows only about entities written
since the last compaction — so under a base it concluded "no entry under this key,
therefore no match" about every entity in the image, and every type-narrowed
property query returned **an empty result**. Not a wrong one: empty, which is worse,
because a scan that finds nothing looks like a graph that contains nothing.

It was the one read path `base ∪ delta − retracted` had not reached, and nothing
found it until the default made a base exist outside this package's own tests.
`TestQueryParity_MemoryVsDisk_DeltaAndCompacted` — a test written long before any
of this — is what caught it.

What the fix does not do is tell the planner what a probe now costs.
`probeIsCheaper` still compares a candidate count against a set size on the
assumption that a probe is a map lookup; under a mapped index it is a binary search
through file-backed pages, so the planner will sometimes choose to probe where
building the set would have been cheaper. It is a plan-quality gap and not a
correctness one — the answers are the same either way — and closing it is R3e's
`probeCost()`.

The lesson is about the oracle rather than the bug. Comparing the two arms of
`IndexMode` through the index's own read methods compares them on the paths
somebody had already thought about; reaching them through the *planner* is what
asks the questions nobody had. The comparison now runs six filter shapes and both
entity kinds through query planning, and the fixture carries a cohort of a rare
type purely so that the planner has few enough candidates to choose the probe.

**Two more things the flip found.** `VerifyCSRRoots` recomputed the index root from
GIDX, which a v9 image does not carry — so it recomputed it from nothing and
reported every v9 image as one whose roots did not describe it. An offline verifier
that cannot verify the format the engine writes is worse than none: it is a tool
that reports damage where there is none, which is how operators learn to ignore it.
And `InspectCSR` reported zero property entries for a v9 image, which is
indistinguishable from an image whose index is empty; it now reads the counts out
of the key directory, so inspecting a multi-gigabyte index costs no more than
inspecting a small one.

**The Merkle identity does not change.** The index root is a tree over the property
entries in `(key, value, id)` order, and the two encodings hold the same entries in
the same order — so a store's snapshot root is the same number before and after
this change. Had that not held, every root anyone had written down would have been
invalidated, and the only symptom would have been a verification failure long
afterwards. `TestSnapshotRoots_DoNotChangeWithTheIndexEncoding` pins it, and the
CLI's golden corpus shows it incidentally: the recorded roots did not move when the
sections did.

**The writer checks the orders it is read through.** A value table is
binary-searched and a run is handed out as an ascending `IDRun`, and neither order
is checked when
the section is parsed — verifying either is a pass over the whole index, and that
pass belongs to the bounded verifier rather than to every open. So the encoder now
refuses a stream whose keys, values or ids are not strictly ascending, at one
comparison per key, per value and per entry. Failing the compaction is the point of
failing at all: the previous image is still installed and the store is still
serving, where a file written and then found unreadable at the next open is an
unavailable store.

**The encoding follows the option, not the machine.** A store that asked for a
mapped index and could not have one — `ImageHeap`, or a platform without mmap —
still writes v9. Two parties compacting the same content under the same options have
to produce the same bytes, and they would not if the format depended on a runtime
capability; what such a store cannot do is read *this* image in place, which is what
`store.MetricIndexFallback` already reports.

**Mutation testing found the hole the tests did not.** Twenty mutants, twenty
killed — but one survived the first pass: a writer that ignored the base and wrote
only the delta. Every fixture built a store from nothing and compacted it once, so
the delta *was* the index and a walk that skipped the base produced a correct file.
The second compaction is where that stops being true, and the second compaction is
what every store that stays open past its first one does. It would have shipped as
an index that quietly shrank to whatever had been written since the last compaction,
noticed one reopen later.

### A store can open the mapped index: `Options.IndexMode`

The two sections now have a reader on the other side of the loader. A v9 image
parses to an `index.Base`, `Options.IndexMode: IndexMapped` attaches it, and the
in-memory index becomes the delta over it — the arrangement `base ∪ delta −
retracted` was built for, reached from a file for the first time.

The default is still `IndexResident`, and that is deliberate. Reading the index
in place changes what a lookup costs as well as what a store holds, and the
change that moves the default is the one that measures both against each other.
Everything here is reachable today by asking for it.

**What it is worth, measured.** A store of 50,000 nodes carrying 150,000 indexed
entries, opened both ways from one image:

| | resident | mapped |
|---|---|---|
| heap after Open | 25.0 MiB | 10.3 MiB (−58.9%) |
| Open | 121–126 ms | 29–35 ms (−72 to −77%) |
| warm point lookup | 272–294 ns | 276–310 ns (+1.5 to +5.4%) |

Three runs, identical to the tenth of a percent on the heap figure. The saving is
103 bytes per indexed entry, which is the ~107 bytes the cost model predicts, and
it grows with the entries: 7.0 MiB at 60,000 and 14.8 MiB at 150,000. Open is
faster because the resident arm rebuilds 150,000 entries and the mapped arm reads
a directory. The latency this trade was expected to spend is not visible warm —
a lookup that was 0.2 µs is 0.28 µs, and the 13 random reads a cold one costs are
what `NodesByPropertyBatch` is for.

`disk/index_mode_spike_test.go` is that measurement, under `-tags=stress`. It
exists because §14.13 recorded "±0.0%" for a change that plainly moved bytes, and
the reason it recorded that is that nothing reopened a store from disk.

**Composites are the one thing a base cannot answer in place.** A composite is an
index over a tuple of keys the forward direction holds separately, so answering
one from the image would mean intersecting the member keys' runs on every query —
the work a composite exists to have done once. So `AttachBase` does it once, and
the fill is proportional to the member keys' entries rather than to the index: a
store that declares no composite reads nothing from the base at all.

Which is where the ordering bit. A store restores its declarations from its
catalogue *before* it loads its image, so at Open the composite exists and the
base does not — and a fill that ran only when a composite was declared left every
reopened store answering every composite query with no matches. Silently: an
empty result, not a slow one, which is worse than the scan the GORD section
exists to prevent. It is filled from both ends now, and a test asserts both
orders produce the same answers.

**A v9 image under `IndexResident` is not refused.** Its runs are walked into the
resident index through the same per-entry path GIDX's entries take, so the two
arms of the option produce the same index out of the same file — which is what
lets the resident arm be the oracle for every test of the mapped one. It is also
what keeps the bulk loader §14.4 measured and reverted reverted.

`IndexMapped` needs a mapped image. Under `ImageHeap` the file is a buffer the
parse copies what it needs out of, so an index read out of it would pin the whole
image in anonymous memory to save part of it — the opposite of the trade. The
store rebuilds it, `StorageStats.IndexMode` says "resident", and
`store.MetricIndexFallback` names why. A file that simply carries no mapped index
is not a fallback and reports nothing: there is nothing in it to read in place.

**A damaged run fails the open.** Most of this index's read paths have no error to
return, so a base records its first fault rather than reporting an unreadable run
as an absent value; a store checks that fault once its declarations are in and
refuses to come up on a file whose index it could only partly read. Both arms
refuse, for the same reason at different scales — the mapped arm reads the runs a
composite needs, and the resident arm reads all of them because rebuilding is
reading all of them.

**The loader.** GPIX and GPIR joined the understood-section list, which is what
lifts this build's refusal of an image it could already write — and they joined it
now rather than when the writer landed, because registering a magic is not
understanding a section. A file carrying one of the two is refused as damaged
rather than read as one carrying no index: the forward direction cannot say what
an entity is indexed under, which is what removing it needs, and the reverse names
values it cannot resolve. `InspectCSR` reports both as known, so an operator
diagnosing a file is not told it was written by a newer version than the build
reading it.

The index is also now loaded *before* the graph is published and the mapping
attached to it, which is not tidying: loading it can now fail, and the image
source is the loader's to release only until the mapping's lifetime passes to the
collector. A failure after that point would leave the file mapped for the life of
the process.

**Cost to everything that does not ask for this.** Nothing measurable. The
default is unchanged, a v8 image takes the same path it did, and the added work on
it is two lookups in a section directory of at most seven entries. Interleaved
against a control tree over seven passes: allocation identical (2.045–2.059 MB and
396–397 allocs per open on both sides), heap after Open identical to two decimals
(92.12–92.15 MiB on both sides), and a wall-clock difference smaller than the
spread within either arm. Twelve mutants, twelve killed.

### A compaction can write the mapped index: format v9

The two sections the entries below describe now have a writer. Give
`csrPayload` a `MappedIndex` source and the image carries GPIX and GPIR instead
of GIDX and declares itself v9; leave it nil and the image is v8, byte for byte
what it was. Nothing in the engine sets it yet — no store writes v9, the golden
v8 fixture still matches to the byte, and every file that loads today loads
identically. What is new is the encoder, and the two problems it turned out to
have.

**The two-walk design did not survive contact with a live index.** The previous
entry describes an encoder that walks each key twice: once summing run lengths
to size the value table, once writing the runs. That works against a frozen
source and the plan for this item specified one — the shards swapped for empty
ones at the pin. It does not work against the live index, and the live index is
what a compaction has: a walk of one key is atomic under that key's shard lock,
two walks are not, and a writer registering an entry between them changes the
key's value count so the second pass writes a run the first did not size. The
encoder catches that and refuses the compaction, which under the writer this
program was measured against — one rebuilding a derived layer of 1.5M entities —
would be most of them.

So the runs are written first, in one walk, and each key's value table is built
as they go. It cannot be held in memory: sixteen bytes per distinct value is 448
MiB for one 28M-entry key, which is the allocation this section of the program
exists to remove, reappearing in the code that writes it. It goes into a
`spillBuffer` — memory up to a cap, a file past it, copied into the image in one
pass once every key's runs are down. Nothing in the reader changed: the key
directory carries the value table's offset and the runs' offset independently and
bounds each on its own, so where they sit relative to each other was never part
of the format.

**GPIR is sorted outside memory.** The forward pass generates reverse entries in
key order and the section is searched by entity, and 24 bytes an entry is 672
MiB at this program's shape. So chunks of 32Ki entries are sorted in memory and
spilled, and the section is written by merging them with a heap — one sequential
write and one sequential read of the entries, memory bounded by one chunk plus a
read budget the merge divides between however many runs it has. A store below a
chunk is sorted in place and opens no file at all.

`(id, keyID, valueOff)` is a total order over the entries a valid index produces,
because an entity is indexed under one value of one key at most once. That is
what lets the sort have no tie-break and the merge no stable-run bookkeeping, and
it is what makes the section's bytes a function of its contents.

**The snapshot root does not move.** An image's identity is stated in terms of it
— custody chains, attestations, every expected value an operator has retained —
and changing how the property index is encoded must not change it. It does not,
because GPIX's walk yields the entries in the order GIDX wrote them: key
ascending, value ascending, id ascending. That is a coincidence of two orders
being the same order, which is exactly the kind of thing that quietly stops being
true, so `TestSerialiseTo_MappedIndexKeepsTheSnapshotRoots` serialises one graph
both ways and compares all four roots.

**Two version numbers where there was one.** `csrVersionCurrent` is what a store
writes and `csrVersionMax` is the highest it reads, and until v9 they were the
same number because every version before it was written by every build that could
read it. v9 is chosen by the payload, not by the build: it means "this image
carries the index as GPIX and GPIR". So this build reads v9 and writes v8, and
`graphene version` and `graphene migrate` go on reporting 8, which is the truth
about what an operator's next compaction will produce.

The version bump is not what makes an older reader refuse a v9 image — the two
sections are critical, and that alone stops a v8 build from opening a store whose
index it would then ignore. What the bump buys is the diagnosis: "written by a
newer version" rather than "unknown section". This build refuses a v9 image for
exactly that reason too, and will until the change that teaches the loader to
read the sections.

**Cost, measured.** Interleaved against a control tree at the previous commit,
four order-alternated passes, over 50,000 entries:

| | control (forward only) | this change (both sections) |
|---|---|---|
| all-distinct, ns/op | 1,837,000–1,952,000 | 6,099,000–7,385,000 |
| low-cardinality, ns/op | 285,900–330,700 | 4,659,000–6,537,000 |
| B/op, either shape | 65,800 | 2,955,000 |
| allocs/op | 9 | 36 |

The control writes only the forward section, so this is not like for like: the
low-cardinality gap is almost entirely the reverse index, which has 50,000
entries to sort where the forward section has 50 values to write. Per entry the
whole job is ~122 ns against ~38 ns for half of it. `BenchmarkGPIXBaseLookup` —
the read path, untouched — is flat across the same passes at 100.5 against 99.3
ns, which is what says the numbers above are the change and not the trees.

The allocation figure went the wrong way and is still the right trade. It was
65.8 kB because the old encoder streamed and held nothing; it is 2.96 MB because
the new one holds two intermediates, and those are bounded by three constants —
a 1 MiB value table, a 768 KiB sort chunk, a 2 MiB merge budget — rather than by
the index. All three are deliberately small: raising them buys back sequential IO
and spends the resource this program is spending down. The figure is now the same
for both shapes and does not move when the entries grow tenfold, which is the
property R1's acceptance criterion actually asks for.

That property had to be fixed rather than observed.
`TestGPIX_WriteAllocationIsFlatInEntries` caught the caps being *grown into*
rather than allocated: sixteen and twenty-four byte appends up to a one-megabyte
buffer cost about five megabytes on the way, which made the encoder's allocation
proportional to the index right up to the cap. Bounded, and not what a cap is
for.

**Method.** Twelve mutants, twelve killed, each a guard or an offset removed: a
value table entry pointing past its run, a missing sentinel, a reverse entry
addressing the run rather than the value, a value nothing holds written anyway,
value table offsets left spill-relative, an unsorted reverse section, a merge
ordering by id alone, a spilled id encoded big-endian, a spill swallowing a failed
write, a short copy into the image accepted, a mapped index stamped v8, and GPIX
written as optional. One further mutant survives and is equivalent: the reverse
entry's two pad bytes are never decoded and the buffer they are written into
already holds zeros, so nothing can observe them. No new fuzz target — the
encoder reads no untrusted bytes, and the parse surface `FuzzParseGPIX` and
`FuzzParseGPIR` cover is unchanged.

**A finding that changes what the next two items can deliver.** The plan has a
compaction install the new base at commit and keep only what was registered since
the pin as the delta, which is what would make the index resident cost fall
inside a running process. It cannot: a compaction publishes a graph it *built*,
not one parsed from a file, so nothing maps the image it has just written — the
mapping protocol says so in as many words, and deliberately, because a mapping
per compaction would pin the first one for the life of the store. There is
therefore no new base to attach at commit, and freezing the shards at the pin
would buy nothing, which is why this change has no freeze in it.

The residency win is an *open-time* win. A process that opens a v9 store holds
the fences and the key directory and nothing else; a process that then rebuilds
and compacts holds what it registered until it reopens. That is the shape the
consumer's read-only aggregate is in — the 4,871 MiB arm — and the writer arm
gets it at its next start. Closing the gap inside one process needs the index
sections mapped on a lifetime of their own, separately from the record stream,
which is its own item and not this one.

### The property index can be a base plus a delta: `base ∪ delta − retracted`

`PropertyIndex` can now answer from two halves. Attach an `index.Base` — the
reader the entry below describes — and the shards become the *delta* over it:
everything registered since the base was written, plus one bit per base id
naming what has been removed. Every read path merges the three terms, and the
answer is the one a single resident index would have given. Nothing attaches a
base yet except tests, so no production path changes, the image version is still
8, and no file that loads today loads differently.

**The retraction bit is how §14.7 is answered, not audited around.**
`TECHNICAL_DETAILS.md` §14.7 records the hazard that sank the earlier attempt at
a lazily-loaded index: a structure that reads part of the base into memory to
answer a query can re-read an entry deleted after it was loaded, and hand back
an entity that is gone. A bit removes the second copy rather than policing it.
The base is never written to and never read into a resident structure; the bit
is consulted on the way out of every read; resurrection is unrepresentable
rather than prevented. `TestUnion_NoResurrectionAcrossReindex` and
`TestUnion_MatchesResidentAfterRemoveAll` are that scenario, the second in the
shape the program's consumer actually runs — delete every entity of a type,
write a new generation, and the base must answer for none of the old ones.

One bit per *entity*, not per entry, because that is exactly the grain the
update path can express: the index has no per-key purge, so a caller changing
what one entity is indexed under drops every entry for it and registers the
whole new set. `NodeEntriesOf` now merges both sides and deduplicates so that a
caller re-registering what it was handed is not handed the same pair twice.

The set is an atomic bitset with no lock on either path, where the plan had an
RWMutex and a lock order to keep — shard, then retracted, with `RemoveNode`
taking retracted first. Its length is fixed when the base is attached, since an
id above the base's highest was never in it, so there is no resize to serialise
and a retraction is one atomic OR. The words are allocated on the first
retraction, so a process that only reads never allocates them: at this program's
sizes that is several megabytes a read-only consumer does not hold.

**What the planner is told, and where it is now an upper bound.**
`NodeCardinality`, `EntryCounts` and the per-key entry counts add the base's
figures, which the base reports from its key directory without being walked.
They over-count an id held by both sides and a retracted id the base still
holds. That is sound and it is the point: a driver has to be a *superset* of the
answer, so an over-count costs a worse plan and can never cost a wrong result —
counting exactly would turn a constant into a pass over a postings list on a
path the planner takes once per candidate filter per query.

Ranges keep their declared semantics. A base's values are not in the delta's
ordered index and cannot be — absorbing them is the residency this item removes
— so `NodesMatchingOrdered` walks the base from the filter's own lower bound,
stops at its upper bound, and merges value by value. Falling back to the scan
path instead would have been easier and wrong: the two compare differently, and
a declared key that silently started comparing numerically because it acquired
a base answers a different question than the one it was declared for.

Uniqueness is checked against both sides inside the shard write lock, so the
check and the insert stay one step. A declaration no longer walks the base in
the usual case: the base reports a key's distinct-value and entry counts, and
`entries == distinct` says every run holds one id, so no base value can conflict
with itself and only the delta's values need probing. A key that has always been
unique therefore costs a walk of the delta rather than a sequential read of the
whole index section at every open. When the two counts disagree the walk is what
the answer costs, and the caller is about to be told about a conflict anyway.

`DeclareUniqueNodeKey` and `DeclareUniqueEdgeKey` now return an error beside
their conflicts, which means something different: not "the data violates this"
but "the data could not be read, so whether it violates this is unknown".
Nothing is declared then. It is a return value rather than a recorded fault
because a declaration is the one place a constraint is established over data
that already exists — every later registration is gated by `IndexNodeUnique`,
which refuses on the same fault under the same lock.

Everywhere else a damaged base is recorded rather than returned, because most of
these methods have no error to return: `NodesByProperty` returns a slice. The
read answers from the delta, which is undamaged, and the fault is recorded on
the index where `BaseFault`, `Verify` and a store's statistics can all see it.
Reporting an unreadable run as "that value has no ids" would turn a damaged file
into a wrong answer, which is what `index.Base`'s own contract refuses.

**Method.** The suite compares two indexes loaded with the same triples — one
whole, one split into a base and a delta — and fails on any difference, across a
swept split point, a deliberate overlap so every merge has to deduplicate rather
than concatenate, and three densities of removal. The base here is a fake built
from maps, because `index` cannot import `disk`; it is itself checked against a
resident `PropertyIndex`, which composes with `disk`'s own checks of `gpixBase`
against one to give the chain the item needs. Nine mutants, each a guard
removed: all nine killed.

Three things this found. The merged walk did not honour an early stop: a base's
`ForEachValue` reports a caller's stop and a completed walk the same way, as no
error, so the delta's leftover values were drained into a callback that had
already said it wanted nothing more. It needed its own flag. The differential
assertions could not see it — they all walk to the end — so it took reading the
code back, and `TestUnion_WalksStopWhenAsked` now checks every stop position on
every key, because stopping inside the base's half and stopping while draining
the delta's leftovers are two different paths through one function. Compaction's
payload source and the export path are both callers that stop.

A base key can outlive its last entry — retracting every id that held it leaves
the key in the base's directory until a compaction declines to write it — which is
invisible to every caller because a key with no live entry contributes nothing
to any walk, and is now written down where the key list is built. And the first
run of the suite retracted no edge at all and said nothing about it, because a
split taken at a fraction of an unshuffled corpus puts one whole entity kind on
one side of it; the corpus is shuffled now, which also hands the base an
arbitrary subset of triples rather than a prefix, a stronger base than a real
one.

What the merges deliberately do not check is the base's own ordering: runs
ascend and values ascend because that is the base's promise, and re-checking it
per probe would make every query pay for the file being sound. A base that broke
it makes a merged result mis-ordered or a delta value visited twice — bounded,
since every walk here is guarded by its own slice length rather than by the
base's claims, but wrong. Establishing the order over a whole section is an
O(entries) pass, and that pass is the bounded verifier's.

**Cost, measured.** The path with no base pays one inlined atomic load of a nil
pointer and a branch: **+3 ns on a 57 ns warm equality lookup, about +5%**, four
order-alternated interleaved passes against a control tree, with
`BenchmarkPointLookupNode_Memory` — which touches no changed code — flat across
the same passes, so the difference is the change and not the trees. The
mechanism is a cache line the lookup did not previously touch. Allocations are
unchanged on every arm. Under the P0-memory rule this program runs to, that is
reported and accepted. With a base attached the merge allocates exactly once,
for the result the public API must hand back: 80 ns and 8 B/op against a 50,000
all-distinct-value key, 3.2 µs and 8 KB for a value holding a thousand ids. Those
are against a heap-backed fake, so they price the merge and not the image; the
mapped figures land with the flip.

### GPIX and GPIR are readable: `index.Base`

The two sections the entry below describes now have a reader, and it answers
through an interface rather than a concrete type: `index.Base` is what a
disk-resident property index looks like from the side that queries it, and
`disk` implements it over one image's two sections. Nothing in the engine reads
a base yet — the resident index is still the only index there is — so no image's
bytes change, the image version is still 8, and no file that loads today loads
differently.

**The interface is in `index` because `disk` imports `index`, not the other way
around.** The plan for this item put the reader in `index/mapped.go` and the format
in `disk`, which cannot be written: the two would have to import each other. The
format stays whole in `disk`, where its encoder already lives, because a format
whose encoder and decoder sit in different packages drifts. What `index` gains is
the contract — `Base`, `EntityKind`, `IDRun` — stated only in `store` types and
callbacks, so `disk` names nothing of `index`'s except those.

The boundary is drawn where the calls are coarse. Every `Base` method is entered
once per query, or once per distinct value of a range walk, never once per id: an
interface dispatch is amortised over a whole postings list rather than paid per
element. `IDRun` is the exception and therefore a concrete struct — it is the one
type on this boundary touched per id, and it is a *view* over the image's bytes, so
reading a value's postings copies nothing. **Reads through the interface allocate
nothing at all**: `TestGPIXBase_ReadsDoNotAllocatePerEntry` measures 0.0
allocations per lookup at eight ids per value and 0.0 at eighty, the closure
`ForEachValue` takes included, and asserts it as a slope so a reader that began
copying postings would show as growth rather than as a number under a ceiling. A
warm lookup costs ~250 ns against a 50,000-value all-distinct key and ~160–210 ns
against a 50-value one, 0 B/op in both.

`Base` promises immutability, which is not a convenience: a base is the index as of
the compaction that wrote the image and never changes again, so it needs no lock,
and nothing can be resurrected out of it — the hazard `TECHNICAL_DETAILS.md` §14.7
records against lazy index construction is answered by the shape of the type rather
than by a rule someone has to follow. Only the methods that decode a run can fail,
because a corrupt image can make one unreadable and the alternative — reporting an
unreadable run as an absent value — turns a damaged file into a wrong answer.

Correctness is established differentially: the same triples are loaded into a real
`index.PropertyIndex` and into an encoded GPIX/GPIR pair, and every method is
checked to answer exactly what the resident index answers — lookups present and
absent, key and value counts, ascending value walks, lower bounds probed one byte
either side of every present value against `sort.SearchStrings`, the entries of an
entity, and a dense band of ids so that misses are probed as densely as hits. A
base whose answers merely resembled the resident index's could not be unioned with
a delta to replace it.

Two things this found. A reverse entry naming a key of the *other* kind would have
resolved a value cleanly out of the wrong key's runs, because the two kinds share
one key-id space — checked now, and named as a corruption. And `parseGPIX` bounded
a key's distinct-value count against its own runs region but not its entry count;
the planner reads that count as an `int` to weigh selectivity, so a file-supplied
2^64−1 arriving there as a negative number would have made the planner choose by a
figure that means nothing. Both are refused at parse or at read, not at the first
query that trips over them.

### The property index has an on-disk form: GPIX and GPIR

The property index is 94.5% of the anonymous memory a 1.2 GiB store holds and
94.1% of the transient it pays to open one — 528 MiB of encoded entries become
2,748 MiB of Go maps, 5.20x their own size on disk, where the record image costs
0.229x once it is mapped. `docs/MEMORY_MODEL.md` measures it and ranks it: it is
the one term that decides whether a store fits a 2 GiB machine, and nothing else
in the engine is within an order of magnitude of it.

This is the format that lets the index be read in place instead of rebuilt into
the heap. Two sections, neither yet written by any writer nor read by any loader:

- **GPIX**, the forward direction. Per declared key, a table of its distinct
  values and, per value, a run of the ids carrying it. Values are searched by an
  eight-byte prefix held in the table, falling through to the value itself
  wherever two prefixes agree — so a key whose values share a long prefix is
  searched exactly rather than scanned.
- **GPIR**, the reverse direction: per entity, the keys it is indexed under, each
  naming its value by an offset into GPIX rather than carrying a copy of it.

Both are written forward-only, because `imageWriter` cannot seek and a writer that
could would invite holding the section in order to patch it. A key's value table
precedes its runs and is sized by a count that is not known until the key has been
walked, so the encoder walks each key twice: once summing run lengths to write the
table, once writing the runs. A run's length is a function of its value and its
posting list, so pass one needs nothing that pass two produces.

What that buys: **9 allocations and 65.8 kB to write 50,000 entries, flat in both
the number of entries and their shape** — and 64 kB of that figure is the caller's
own output buffer. Ten times the entries allocate sixteen bytes more.
`TestGPIX_WriteAllocationIsFlatInEntries` asserts it as a slope rather than a
ceiling, and it has already earned its place: the first draft allocated one scratch
array per distinct value, which the guard caught as a 9x growth. Wall clock is
~38 ns per entry for an all-distinct key and ~6 ns for a low-cardinality one.

No image's bytes change and no file that loads today loads differently. The image
version is still 8, nothing writes either section, and the two magics are
registered *without* being added to the set of critical sections this build claims
to understand — a build that listed them before its loader read them would accept
an image whose index it then ignored, which is the wrong answer their critical flag
exists to refuse.

### A compaction no longer materialises the property index

`compactPin` used to call `PropertyIndex.NodeEntries()` and `EdgeEntries()`,
which return every indexed triple in the store as a slice — 48 bytes of entry
header and a copy of the value each, because those functions hand the caller
ownership. Both were built under the store lock and held from the pin until the
image had been written. It was the last allocation a compaction made that was
proportional to something the store holds.

`csrPayload.NodeProps` and `EdgeProps` are `iter.Seq` now, and `build` fills them
with a walk of the live index through `ForEachNodeProperty` — the streaming form
that already existed for bulk export, whose `(key, value, id)` order was already
pinned to the materialised one. Nothing copies the value: the index owns the
bytes for the callback, the writer copies them into its output buffer, the Merkle
stream hashes them, and nothing retains them.

Measured on the consumer's index shape, thirteen entries per node, interleaved
against the previous tree:

| | 100k nodes (1.3M entries) | 200k nodes (2.6M entries) |
|---|---|---|
| bytes allocated by `Compact` | 95,870,000 → **14,250,000** (-85.1%) | 191,800,000 → **28,520,000** (-85.1%) |
| allocations by `Compact` | 1,317,000 → **3,541** (-99.7%) | 2,633,000 → **6,892** (-99.7%) |
| polled peak above steady state | 90.94 MiB → **13.45 MiB** (-85.2%) | 182.0 MiB → **27.23 MiB** (-85.0%) |

Both arms double exactly between the two sizes, which is what says the term
removed is proportional to the entries rather than to the records: about 66 bytes
per entry, the 48-byte entry header plus the copied value. A compaction of a
200k-node store with 2.6 million indexed triples now makes under seven thousand
allocations in total. The first compaction of a store, where everything is still
in the delta, falls by half rather than by 85% — the payload is one of two large
transients there. Steady-state residency does not move and must not: 183.1 MiB of
heap before and after at 100k.

The cost is 43 ms on a 1,143 ms compaction, **+3.8%** with non-overlapping
spreads, and `Forensic_Compact` agrees at +4.5% to +6.9% on fixtures small enough
that only the overhead shows. Accepted under this program's priority rule, and
stated plainly: the compaction got 43 ms slower and stopped touching 82 MiB.

The cost is a lock that moved rather than a lock that grew. The pin got shorter by
the whole enumeration, which is the interval every writer in the process waits
on; what got longer is one shard read lock per key, held during the build with no
store lock, blocking only writers touching that key's shard.

Because the walk now happens during the build rather than at the pin, the entries
a compaction writes are read from an index the store is still mutating. Three
things make that sound and none of them is "the index does not change": every
index mutation is journaled before it is applied, so an entry the stream caught
is in the log too and replay converges; an entry naming an entity the image does
not hold is filtered out by construction, against the image itself, because that
one is an orphan no convergence repairs; and a live reader, the one caller whose
index is replaced underneath it, cannot compact. `TECHNICAL_DETAILS.md` §14.19
has the argument and invariant §15.5 gained the stronger form it enforces.

The on-disk format did not move. The section's two entry counts are written as
zeros and patched after the flush, which is what the header's
`sectionTableOffset` already did, and `csr_golden_bytes_test.go` holds the result
to bytes captured before any of this.

### The compacted image is memory-mapped instead of copied into the heap

Opening a store read `graphene.csr` with one `os.ReadFile` and then copied every
property blob out of that buffer into an arena the graph keeps for its life. The
bytes on disk are contiguous, pointer-free and already in the form a reader wants,
so the copy bought nothing but the copy — and it bought it twice over at the peak,
once as the file buffer and once as the arena.

The image is now mapped, and a record's `Properties` addresses the file directly.
`Options.ImageMode` selects it: `ImageMapped` (the default), `ImageHeap` for the
previous behaviour, and `ImageMappedUnlocked` for a live reader. Labels are still
decoded into a heap arena in both modes, because a label is a `uint16` at an odd
offset in the record stream.

**What a caller has to know.** A `Properties` or `Labels` slice from a mapped
image is valid for the life of the handle and **not past `Close()`** — after
`Close` the process no longer has the memory, so reading it is a fault rather than
stale data. Compaction does not shorten that window: a compaction writes a new
file and goes on reading blobs from the mapping it already had, so a slice taken
before one is still valid after it, and a store holds exactly one image mapping
however often it compacts. `store.CloneNode`, `store.CloneEdge`, `store.CloneNodes`
and `store.CloneEdges` are new, and are the correct way to keep a record — they
copy `Labels` too, which is the half that is easy to forget.

A live reader is the one store that maps more than one image, because `Refresh`
rebuilds from the files across a compaction. It is therefore **not** mapped by
default: it holds no process lock, and a mapping is only as stable as the file
under it. `ImageMappedUnlocked` opts in, and documents both consequences — the
exposure to the directory being edited underneath it, and that a slice from image
N is valid until the second reload after it.

Mapping falls back to the copy rather than refusing an open: on a platform with no
mapping primitive, on an empty or unaddressably large file, when the syscall
fails, and for a store holding no lock under the default mode. Every fallback is
reported through the new `store.MetricImageFallback` and through
`StorageStats.ImageMode` and `StorageStats.ImageMappedBytes`, because a store
paying for a copy it was configured not to pay for is a memory regression with no
other symptom.

Measured against the tree before it, three interleaved rounds, reopened from
disk:

- **The Go heap falls 22.4%** on a 100 000-node store — 234.6 MiB to 182.1 MiB —
  and 22.5% at twice the size, 467.0 to 362.0 MiB. The saving is 52.5 MiB and
  105.0 MiB: exactly double, so what was removed is proportional to the image.
- **Anonymous resident memory falls 13.5% and 18.4%**, and 84.83 MiB of an
  88.02 MiB image becomes file-backed. *Total* resident goes up about 15%, and
  that is the change working rather than failing: file-backed pages are evictable
  without swap, and anonymous pages are what a memory ceiling constrains.
- **An open allocates 67.6% fewer bytes** — 6,295,947 to 2,038,619 B/op — and
  67.9% fewer with `VerifyOnOpen`, which also stopped reading the image a second
  time for itself. Wall clock fell on both.
- **The image on disk is byte-identical**: 175.0 B/node, 87.50 B/edge, 16.69 MiB
  on every round of both arms. Both point lookups are within noise.
- A compaction over an existing image inherits the same 22% heap saving and
  allocates the same bytes to four significant figures.

The cost is that the bytes are read when they are touched rather than at open, so
a caller that walks every blob pays it as page faults instead of as one sequential
read. `BenchmarkRSS_BlobTouch` is the new instrument for that case, and it is the
mapped path's worst case: after touching every page of every blob, total resident
memory is within a few per cent of the copying path's, with the same 22% off the
heap.

Also: `disk/mmap_windows.go` is the fifth file in the tree permitted to import
`unsafe`, and `TestUnsafeIsConfinedToKnownFiles` records the reason. It turns the
mapping address into a slice by assigning the slice header's fields, which is what
keeps `go vet`'s `unsafeptr` check enabled for the rest of the tree.

The on-disk format did not move.

### A compaction no longer copies the image it is replacing

Pinning a compaction built the new image's record set as two slices while
writers were blocked: one entry per live node and one per live edge, 56 and 80
bytes each, walked out of the image that was about to be replaced and held for
the whole build beside it. Neither slice was presized, so reaching that size had
allocated about five times as much again. On the store this program is aimed at
that is 84 MB held and several hundred allocated, to say something the engine
already had a pointer to.

A `CSRGraph` is immutable once published. So the plan now holds the image, and
copies only the delta — whose maps really are mutable — presized from the live
counts the delta already maintains. `build` merges the two: `Build` became a
wrapper over `buildSeq`, which takes `iter.Seq` instead of slices, and the plan's
`nodeSeq`/`edgeSeq` walk the image's arena and the sorted delta with two cursors,
materialising nothing. Every existing caller still passes slices and is unchanged.

Deciding which image records the delta displaces used to be a map probe and a
version-chain resolve per image record, under the lock. The pin now records every
identifier the delta had an opinion about — tombstones included, which is the only
reason a tombstone survives the pin — in the same pass that copies the records,
and the merge consumes that with a cursor. The test that ran once per image record
runs once per delta entry.

Measured against the tree before it, interleaved rounds, on a store with a
100,000-record image and a 1,000-record delta:

- **A compaction allocates 25.1% fewer bytes** — 128,050,664 B becomes
  95,859,304 B — and its transient peak over steady state falls from 122.0 MiB to
  **91.27 MiB**. The allocation *count* is unchanged; this was two large slices,
  not many small ones.
- **The cost removed was proportional to the image.** Per live record, before:
  1,268 B at 100,000 records and 1,267 B at 200,000. After: **949.1 B at both.**
  So the saving doubles when the image does — 30.70 MiB to 61.24 MiB.
- **The pin on its own is flat.** A pin over a 1,000-record image allocates
  2,416 B; over an 8,000-record image, **2,416 B**. Before, the same pair differed
  by 56 B a node.
- **A first compaction, where there is no image to copy, still gains from the
  presize**: transient peak 163.8 → **139.2 MiB**, and 41.9% fewer bytes at
  n=10,000 in `Forensic_Compact`.
- **The image is byte-identical**: 175.0 B/node, 87.50 B/edge, 16.69 MiB on every
  round of both arms.
- **A compaction is slower**, by +10.7%, +8.3% and +2.5% on the three
  `Forensic_Compact` arms at n=10,000, with the spreads overlapping. A three-arm
  measurement puts all of it on sorting the delta — about 330 ns a record — and
  shows the rest of the change to be about 10% *quicker* than what it replaced,
  with non-overlapping spreads. The sort is what makes the merge two cursors
  instead of a lookup structure, and it is kept.
- Opening a store pays the same indirection, `Build`'s other caller being the
  loader: **twenty-three allocations, no measurable bytes and no measurable time.**

Two things came out of it that were not the point.

A freshly compacted store's adjacency arrays now match the ones the same image
produces when it is read back from disk. The merge yields globally ascending
identifiers; before it, the delta's edges were appended in Go map order, so the
two could disagree about the order of a node's incident edges — and disagree
differently on every run.

And the compaction metric's `Examined` is now what `store/metrics.go` says it is.
It documented "records scanned" and reported the records in the new image, which
is what `Count` reports; it is now the image's live records plus every delta
entry, so `Examined − Count` is the work the compaction did that produced
nothing. Both figures were already maintained, so the pin states it for free.

`buildSeq` walks its inputs three times for nodes and five times for edges —
the page directory is sized from the highest identifier, the arena from the
touched pages, and adjacency is a count then a fill — so a sequence given to it
must yield the same records every time. Each later pass counts what it saw and
refuses a disagreement with `errUnstableBuild` at the pass where it appears,
rather than as an out-of-range write two passes downstream.

The on-disk format did not move and no exported API changed.

### Compaction writes the image instead of building it

Serialisation built the whole file in a `bytes.Buffer` and handed back a
`[]byte` for compaction to write. On a 1.2 GiB store that is a 1.2 GiB
allocation — transiently closer to 1.8 GiB, because a buffer that outgrows its
capacity allocates a larger one and copies — made at the moment compaction is
already holding a freshly built CSR and the plan it came from. Nothing read the
bytes twice.

Beside it sat a second cost nobody had counted. Computing the snapshot roots
built a `[]merkle.Hash` of every leaf — one 32-byte entry per node, per edge and
per property-index entry — folded the three arrays into three hashes and dropped
them. At that same shape it is another ~900 MB of hashes whose only purpose is
to produce ninety-six bytes.

`SerialiseTo` writes the same bytes through a 64 KiB buffer instead, and
`merkle.RootBuilder` folds each root a leaf at a time as the records stream past
on their way into the file, retaining one hash per set bit of the leaf count —
at most 64 — rather than one per leaf. `SerialiseWithPayload` is now a wrapper
that streams into memory, so there is one writer rather than two that could
drift apart.

What a serialisation costs is now a stated constant rather than an estimate: one
64 KiB buffer, three builder stacks, a leaf-encoding scratch sized by the largest
single record, and the section directory. Writing a 576 KB image allocates
74,576 B; writing a 4.6 MB one allocates 75,496 B — **920 bytes more for eight
times the image**, 64 KiB of both being the buffer.
`TestSerialiseTo_AllocatesIndependentlyOfImageSize` asserts both the ceiling and
the slope, because either alone passes for the wrong code.

Measured against the tree before it, interleaved rounds:

- **Compaction's transient peak falls 70.2%** — 555.1 MiB over steady state to
  165.2 MiB, on a 100,000-node store — and the process peak during it falls
  48.2%. What the store *holds* afterwards is unchanged to the decimal: 235.1 MiB
  of Go heap in both arms, which is the control.
- **A compaction allocates 47.6% fewer bytes and 83.1% fewer times**: at 10,000
  nodes, 9,149,776 B and 60,133 allocations become 4,793,200 B and 10,144. Five
  allocations per node disappear, and more of them than expected were Merkle
  hashers rather than image bytes.
- **The image is byte-identical**: 175.0 B/node, 87.50 B/edge, 16.69 MiB on every
  round of both arms.
- Wall clock is not resolved. The medians favour the new writer on every arm and
  the spreads overlap, so a compaction did not get slower; it is not established
  that it got faster.

The on-disk format did not move. The record order, the section order and the leaf
order are unchanged, and `RootBuilder` is the same RFC 6962 tree
`merkle.Root` builds rather than a cheaper one —
`TestRootBuilder_EqualsRoot` sweeps every leaf count from 0 to 1025 and
`FuzzRootBuilderMatchesRoot` sweeps arbitrary ones. The fixture written by the
build before the page table still reproduces byte for byte through the new
writer.

The cost is one extra sequential read of the file just written. The digest covers
the header, the header carries the section-table offset, and that offset is not
final until the last section is placed — so the digest is computed over the
finished image rather than teed off the write. The alternative was a second
implementation of every encoder to size the image up front, whose disagreement
with the real one would corrupt an image silently. The bytes are still in the
page cache, and wall clock is the cheaper thing to spend.

`merkle` gains `RootBuilder`, with `Add`, `AddLeafData`, `Root`, `Len` and
`Reset`. It is the public form of the same tree the package already computed.

### The CSR stores records in pages, so a burned identifier costs four bytes

The two measurements above — 79 B of resident memory per identifier issued and
never reusable, and a low-half deletion costing 152 B per identifier more than a
high-half one — describe the same defect. The CSR indexed one record slot per
identifier, so a store that rebuilds a derived layer paid 56 B of `nodeRecord`
and 16 B of offsets for every identifier it had ever issued, forever, and
compaction recovered none of it: compaction recovers the *top* of the identifier
space, and a rebuild burns the bottom.

Records now live in pages of 4096 identifiers. A per-kind directory of `int32`
maps a page number to an arena page or to −1, the arenas hold one slot per
identifier of the pages that are actually occupied, and adjacency is one CSR over
that slot space with a single sentinel. A page nothing occupies costs four bytes;
a page whose records have all been deleted and compacted away goes back to
costing four bytes.

Measured against the tree before it, three interleaved rounds:

- **17.65 B per burned identifier**, from 84.45 — **−79.1%**, with the tail slope
  falling from 1.611 to 0.337 MiB per cycle.
- **The two deletion shapes now cost the same**: 314.6 B/node for the low half
  against 314.2 for the high half, where they were 531.2 and 378.8. Where the
  surviving identifiers sit no longer changes what a store costs.
- Resident after twelve rebuild cycles **130.9 → 112.9 MiB**, Go heap
  **71.15 → 55.01 MiB**, peak **296.8 → 251.3 MiB**. A store that has burned
  nothing is unchanged, which is the control.

**The on-disk format did not move, and that is tested rather than asserted.**
Arena pages are assigned in ascending page number, so arena order is identifier
order, so the walk that writes the record stream and hashes the Merkle leaves is
the walk it always was. `disk/testdata/csr_v8_before_pages.bin` was written by
the build *preceding* this change; the test suite reproduces it byte for byte,
reparses it, recomputes its roots from the paged layout and checks an inclusion
proof against the root the file carries. The determinism tests already here could
not have caught a consistent reordering — both compare a build against itself.

Four behaviours changed deliberately:

- `Build` refuses a duplicate identifier instead of keeping the last silently, so
  a file whose records collide fails to open rather than opening with a header
  count that disagrees with its own contents.
- The addressable identifier space rose from **2²⁶ to 2³²**. The ceiling now
  bounds the page directory — 4 MiB per kind at the top of the space — rather
  than the record arrays, so the old value was costing addressable lifetime for a
  bound that no longer binds. An image naming identifiers above 2²⁶ cannot be
  read by a v0.7.0 build.
- The sparsity rule counts pages rather than the highest identifier:
  `touchedPages × 4096 ≤ max(65536, records × 256)`, with an edge's endpoint
  pages counted. It is identically tight against a hostile file and strictly more
  permissive for a real one — one record naming an identifier just under the
  ceiling now loads, because it now costs one page.
- An edge naming an endpoint no record occupies materialises that endpoint's page
  instead of panicking. No file that loaded before stops loading.

The cost is a dependent load on every lookup and the quantisation: a page keeps
its 4096 slots while one record in it survives. The lookup cost is not measurable
above this host's noise — the memory backend, untouched by this change, moved
10% in the same rounds while the disk arm moved 1.6% — and `store info` reports
the quantisation when it is worth an operator's attention.

`TestRebuildCycle_ResidentIsProportionalToLive` runs in `make check` and fails on
the tree before this change, at 72.0 B per burned identifier against a ceiling of
24.

### A store can now see the end of its own identifier space

Identifiers are never reused, compaction preserves them, and the ceiling above is
on the store's lifetime rather than on its size. Nothing reported that.

`StorageStats` gains `HighestNodeID`, `HighestEdgeID`, `IDCeiling`,
`NodeIDHeadroom` and `EdgeIDHeadroom`. The headroom is computed from identifiers
**issued**, not from the ones the image still holds: deleting the record that
held the highest identifier does not give headroom back, because the next write
will not reuse it, and a figure that said otherwise would be worse than none.

A completed compaction reports it unprompted when the space remaining falls below
`Options.IDHeadroomWarn` (default 0.10; negative disables): a
`MetricIDHeadroomLow` metric and an `AuditIDHeadroomLow` entry naming the kind and
the figures, once per kind per process so that a store compacting on a timer does
not fill its own chain with one observation. `graphene store stats` and
`graphene debug stats` print the identifiers issued against the ceiling, and
`graphene store health` raises `store.id_headroom_low` as a warning.

There is no refusal, deliberately. A store at 5% headroom works exactly as it did
at 95%, and the remedy — an export and import into a fresh store, which is a new
store with new identifiers rather than a renumbering of this one — is a decision
with downtime in it. The engine's job here is to make sure the decision is not
taken by surprise.

### Measuring the thing the budget is written against

A consumer sized for a 2 GB machine measured 9,290 MiB of peak RSS rebuilding a
derived layer against a 1.2 GiB store, and 4,871 MiB for a read that touches ten
rows. Nothing in this engine could have predicted either number, because nothing
in it measured resident memory at all: the footprint suite reports
`runtime.MemStats.HeapAlloc`, the Go heap a graph retains.

That is the wrong instrument for a RAM ceiling in a way that would have quietly
corrupted the work that follows. It cannot see a memory-mapped image — those
bytes are resident and are not Go heap — so the single change most likely to fix
this would have reported as a large win on every existing number while the
process occupied exactly as much RAM as before.

So this release opens with instruments and no engine changes at all. Nothing in
`disk/`, `index/` or `store/` moved.

**Process residency, split by class.** `tests/rss_test.go` reads the operating
system's own accounting and keeps **anonymous** pages — private, swap-backed,
what a ceiling constrains — apart from **file-backed** pages, which the kernel
can evict without swap. Linux publishes both directly; Windows derives them from
`WorkingSetSize` and `PrivateUsage`, one-sidedly and in the safe direction;
darwin reports the peak only and says so rather than inventing a split. Standard
library only, so the zero-cgo, zero-dependency closure is untouched.

The reader is calibrated before anything is believed of it. One test maps and
touches 128 MiB and fails if the reading does not land in the file class, because
a reader blind to mapped pages would veto the mapped-image design on its own
blind spot. Another skips itself under `-race` after finding that the detector's
shadow memory is itself anonymous, which makes the split unmeasurable under it —
worth knowing before someone reads a memory figure off a race-enabled run.

**Five baselines** (`make rssbench`) on a fixture matching the reported index
shape: eight unique keys, one an all-distinct 32-byte digest, five ordered, two
composites. Open, a ten-row read, a bulk write, a compaction, and a rebuild
cycle. Plus `make cpuprofile` and `make heapprofile`, and a `-memprofile` flag on
the CLI beside the existing `-cpuprofile` — the heap profile is written after the
command finishes but before the store closes, so it reports what was still held
rather than everything ever allocated.

**A regression guard that can fail.** `TestFootprintGuard_OpenSlope` asserts a
*slope* — retained bytes per record, fitted across two fixture sizes — rather
than a ceiling on the total. Fixed overhead cancels, so a change that adds a
constant is free and a change that adds bytes per record is caught. A ceiling
loose enough to be stable would have been loose enough to miss a doubling.

### What the baselines say

Four readings, on a 44 MiB store, that reorder the work ahead:

- **Resident is ~4.0× the store on disk** — against the 4.06× the consumer
  reported. The fixture reproduces the shape that was reported.
- **A ten-row read costs the same as opening**, to within 0.3 MiB. The entire
  figure is paid at `Open`; effort aimed at making reads cheaper is aimed at the
  wrong term.
- **Nothing is file-backed.** Every resident byte today is anonymous memory the
  kernel cannot evict.
- **Compaction's transient peak is +270 MiB over steady state** — 6.1× the store
  size, held for the duration.

### Deleting the low half is not the same as deleting the high half

`docs/benchmarks.md` has reported since v0.6.0 that a half-deleted store costs
4.5× until `Compact`. That measurement deleted the **high** half, where the
maximum identifier falls with the deletion and the dense arrays shrink with it.

A derived-layer rebuild does the opposite: it deletes the whole old layer — the
**low** identifiers — and writes the replacement above them, so the maximum only
ever rises. Two new benchmarks measure both arms with the same live count and the
same live bytes, and they differ by **152 B per burned identifier**, which
decomposes exactly as 72 B for the node slot plus 80 B for the edge slot the
delete cascade burned with it. Compaction recovers none of it.

`BenchmarkRSS_RebuildCycle` confirms it end to end and reports the whole series
rather than two endpoints, because a one-off start-up cost and a per-cycle leak
produce the same average and have completely different consequences at forty
cycles. The line is straight over twelve cycles and does not flatten:
**79 B of resident memory per identifier issued, deleted, and never reusable.**
At 1.5M nodes per rebuild that is ~119 MiB added permanently per rebuild, on top
of a live set that has not grown.

Neither figure is a regression — both describe behaviour that has been there
since the CSR image existed. What is new is that they are measured, and that the
existing published number is now qualified by the deletion shape it assumed.

### Compaction's ordering is now tested rather than argued

Compaction's crash-safety is not a property of any one statement. It is a
property of the *order* of about ten of them: the image is synced before the
checkpoint marker, because the marker vouches for the image; the rename happens
before the log is retired, because the log is what recovers the store if the
rename does not survive. Each of those is a claim about what a reopen finds if
the process dies at one exact point, and two of the ten were tested — the two a
test could stage by hand. The rest were held up by the comments asserting them.

`compactStepHook` makes every point reachable, and `TestCompact_FailsAtEveryStep`
stops a compaction at each one across four store shapes chosen to drive all three
`retireLog` branches — the branch that rotates the log whole, the one that carries
a tail, and the one that keeps the log and writes no marker at all, which is the
most dangerous difference in the file. Forty runs, each asserting the only thing
that matters: a reopen finds every record that was committed, with its index
entries intact and `VerifyIndexes` clean.

The suite was checked against injected defects rather than trusted. Moving
`retireLog` above the rename — the transposition the ordering exists to prevent —
loses committed nodes on reopen, and the test says which ones.

**A failed compaction no longer leaves the image behind.** The temporary image was
deliberately left on the write, drain and rename error paths, on the reasoning
that a stray temp file is ignored on the next open. It is, but the likeliest cause
of a compaction failing is a full disk, and the second failure is then a
multi-gigabyte carcass sitting in the directory the operator is trying to rescue.
Removing it is one call, and the step suite asserts nothing is left behind at any
of the ten points.

### The list of fuzz targets is derived from the tree

`make fuzz` kept its target list in a variable, the CI matrix kept a second copy,
and both fell behind: two merkle targets were written and then never run for a
release, while the local runner and the nightly job each went on reporting
success. A list of what to test is the one list that must not be maintained by
hand, so both now derive it — `make fuzz-targets` from `go test -list`, and the
workflow from a discovery job that fails if the list comes back empty. The matrix
went from four targets to nine on the first run.

Two of those nine are new — `FuzzReadOrderedKeySection` and
`FuzzReadCompositeSection`, the section readers that had none — and
`FuzzDeserialiseCSR` is now seeded with twenty-five hostile images built from a
valid one: truncated at each structural boundary, with `indexOffset` and
`sectionTableOffset` set to the length, one under it, `MaxInt64`, past it and
zero, with directory entries whose offset and length overflow when added, and with
a GIDX value length one byte past what remains. The seed builder walks the
directory with its own deliberately simpler code, so the corpus does not depend on
the parser it exists to attack.

### The zero-cgo, zero-dependency closure is asserted

Both were true and neither was checked. `TestZeroCgo` walks the whole build
closure — the engine's packages and the standard library's — and fails if any of
them compiles a cgo file; `TestZeroExternalDependencies` reads `go.mod` and fails
on any `require` line; and a third test asserts the exact set of files permitted
to import `unsafe`, in both directions, so a new one has to be argued for rather
than merely added.

The cgo test forces `CGO_ENABLED=1` for its own query, which is the whole test.
With cgo disabled, `go list` reports no cgo files for any package, so the check
would have passed on every machine that had it turned off — a test that cannot
fail, arrived at by an environment variable.

### Verification no longer builds a second copy of what it is verifying

`VerifyIndexes` is what an operator reaches for when something is already wrong,
which on this engine usually means the store has outgrown the machine. It was
allocating a full duplicate of the index to check the index.

Three structures did it. The postings cross-check accumulated a
`map[id]map[key-value]int` over every posting — the reverse index rebuilt, with a
map header of its own per entity — and then compared it against the reverse index
sitting beside it. Each composite index rebuilt its whole tuple-to-entities map
from its members. And `IndexedNodeIDs` returned every indexed identifier as a
slice, deduplicated through a map, to callers that kept only the ones they found
*wrong* — in a healthy store, none of them.

None of it was necessary, because both directions follow from probing and
counting instead of remembering. Postings lists are strictly ascending, so an
entity appears at most once under a key and value: "is it there" and "how many
times" are the same question and one binary search answers it. Probing every
reverse reference into the postings establishes that the reverse side is a subset
of the forward side; both are sets, so once the totals agree they are the same
set — which is exactly what "every posting has a reverse entry" was asking. The
composite index is checked the same way.

The count cannot name an offender, so when it disagrees a second walk goes and
finds it, with the same bound and the same messages as before, paid only by an
index already known to be broken.

Measured on a 32,000-entry index, `Verify` now allocates **0 bytes**. The
structure it replaced cost **410 bytes per indexed entry**, which is what the new
guard measures when it is reintroduced. The guard asserts a slope rather than a
ceiling, for the reason the alloc guards already record: a ceiling loose enough to
be stable is loose enough to miss a doubling.

Two of the corruptions the old shape could not report are now caught. A member
holding the same value twice at one position repeats a tuple, and the rebuilt set
quietly absorbed it; a member value stored past the composite's declared width is
never enumerated and contradicted nothing. Both are now named.

**`VerifyOnOpen` reads the image once.** Its three legs — the bytes are unchanged,
the roots describe those bytes, a named key vouched for the result — each opened
the file for itself, and the last two parsed it again from scratch. With a
verifier configured, `Open` read a file that on the target workload is most of the
machine's memory four times and decoded it three times. The legs still run in the
same order and still prove three separate things (a digest that matches is no
evidence the roots do, which is what catches an edit that repaired the digest);
they now share one read and one parse, and `Open` is down to two reads and two
decodes. Measured as a multiple of one read-and-parse of the same image, the check
went from 3.03× to 1.91×.

**API change, `index` package.** `PropertyIndex.IndexedNodeIDs` and
`IndexedEdgeIDs` are replaced by `ForEachIndexedNodeID` and
`ForEachIndexedEdgeID`, which take a callback and return nothing. The walk does
*not* deduplicate: the index is sharded by property key, so an entity carrying
entries under keys in different shards is offered once per shard. Deduplicating is
precisely the map this exists to avoid, and a caller that minds deduplicates what
it keeps — which is bounded by what it is looking for rather than by the index.

### What an `Open` will cost, answerable before it is paid

`Open` is the one operation whose cost nobody could see in advance. An image is a
file whose size is on disk. A log that was never compacted is a whole write
history that is replayed into memory on every open, and the only way to find out
how much was to try it — on a machine where trying it is the failure.

`disk.PreflightOpen(dir)` answers it. It takes no lock, opens no store, replays
nothing, and runs **in memory bounded by a constant** whatever the size of the
store. That last part is the whole point, and it is what the two existing
inspectors cannot do: `InspectCSR` reads the file and parses the entire graph,
and `InspectWAL` keeps a descriptor per record. Either one, asked about a store
that will not fit, does not fit either. Measured across a tenfold increase in log
size, the new walk allocates **0.00 bytes per record**.

It reports the two halves separately and deliberately — every `Image` field
describes the compacted image, every `WAL` field the log — because on a store
that has never been compacted the first half is empty and the second half is the
entire cost. A single whole-store number would report its smallest value for the
largest store.

**The record count is replay's own, not an approximation of it.** A batch that
never committed is read, verified, held and discarded, so its records are counted
apart from the ones that land; a commit that disagrees with the body it follows
discards its batch, which the walk knows because it recomputes the body checksum
as the bytes stream past rather than buffering them. A checkpoint stops the count
where it stops replay. The guard for all of this drives the real parser and
compares, rather than asserting a constant.

Three things it cannot know, all stated on the fields rather than left to be
discovered: it decodes no payloads, it applies no signature policy, and a
concurrent writer invalidates it. Every count is therefore an upper bound, which
is the safe direction for a figure an `Open` is about to be gated on.

**`Options.MaxReplayBytes` and `Options.MaxReplayRecords`** turn the answer into
a refusal. Over either, `Open` fails with `disk.ErrReplayBudget` naming both the
figure and the budget. The engine cannot catch an out-of-memory condition in Go,
so a bound can only mean refusing before allocating, never degrading while
allocating — and the refusal is placed before any ledger is opened, so a routine
budget refusal does not append a hash-chained record of a crash that did not
happen. Zero, and any negative value, is unlimited; `StrictOptions` sets neither,
and a test now says so.

The byte budget is compared against the log's *records region*, not its file
size. The two differ by the 50-byte container header — small enough to go
unnoticed, large enough to refuse a store that has not changed. The record budget
costs a counting pass, so it is skipped outright whenever the log is too small to
hold that many records (every compacted store) and otherwise stops one record
past the budget. Both bound the replay and nothing else: the image is loaded by
the same `Open` and is not covered.

### Fixed: a corrupt batch marker could ask replay for gigabytes

A WAL batch begins with a marker naming how many records follow, and replay
reserved that many pending slots before reading one of them. The bound above that
reservation has to admit a legitimate batch, so it is derived from the log's size
— and a log's size is a poor allocation limit. At nine bytes a record, a 2 GiB log
permits 238 million of them, and reserving that many asks for something over
seven gigabytes. The marker's own checksum is no defence: it is computed over the
bytes making the claim. Worse, the bound is skipped entirely when the log's size
cannot be determined, and then a four-byte field decides the allocation outright.

The reservation is now capped at 65 536 slots. This decides no outcome — a larger
batch still replays, because a capped reservation is still grown by `append`.
What it removes is the file's ability to name a number and have it allocated: a
marker declaring ten million records now reserves 2 MB where it previously
reserved 320 MB.

**The cap was 4096 first, and measurement rejected it.** Below the batch sizes
this engine actually writes, a cap does not save memory — it replaces one exact
allocation with a geometric growth sequence that allocates several times as many
bytes in total. Interleaved against the same tree without the change, a cold open
of a 10 000-node log cost **+3.15 MB/op and +11 allocs/op**: a memory regression
on precisely the path the cap was added to protect. The bulk writers batch in
chunks of ten thousand, so the cap belongs above them. At 65 536 the same
measurement is +864 B/op and +1 alloc/op, both inside run-to-run noise, and a
corrupt marker is still bounded by a constant rather than by the log's size.

### Fixed: a backup read back the whole image it had just streamed

A backup copies every file of the store through a fixed buffer, and the
program's plan recorded both backup and restore as streaming on the strength of
reading the code. This release's residency instruments gained
`BenchmarkRSS_Restore`, which samples the process during a backup and then a
restore of the same store, so that the claim stays measured. The restore is
streaming: **0.27 MiB** of peak growth against a 44 MiB store. The backup was
not. It ends by re-verifying the digest of the copied image — the check that
makes "the backup succeeded" a statement about the bytes in the destination
rather than the bytes sent there — and that check read the file whole. A backup
grew the process by **44.32 MiB on a 44.01 MiB store**: the entire image,
allocated a second time, on the one operation an operator runs *because* the
machine is short of memory.

The digest is now computed as the file is read. The two regions it excludes —
the digest field itself and the compaction timestamp, which records when the
image was written and is not content — are zeroed in a copy of the header
before it is hashed, and the body streams through the hash from there. One
definition of the covered bytes now serves both this and the whole-buffer form
`VerifyOnOpen` uses on an image it already holds, so the two cannot drift, and a
thirteen-case table plus a fuzz target compare them byte for byte over every
shape the pair can tell apart: files shorter than a header, files that predate
the digest, a tampered body, a changed timestamp. A file that fails to *read* is
reported as an error and never as "carries no digest" — a damaged disk and a
pre-v8 image are different answers, and only one of them is safe to act on.

Measured on the same 44 MiB image, three interleaved rounds against the tree
without the change: a backup went from **46.24 MB/op to 119 KB/op** and the
digest check alone from **46.16 MB/op to 34 KB/op** — one 32 KiB copy buffer
and the hash state — with wall clock inside run-to-run spread on both, the
streamed form marginally ahead in every round because a buffered read has no
file-sized allocation to fault in. The guard is
`TestVerifyCSRDigest_DoesNotReadTheImageWhole`, which bounds what one call
allocates at a megabyte against an eight-megabyte image; reintroducing the
whole read fails it and nothing else does.

`store csr -verify`, `debug hash-check` and `debug integrity` share the leg and
are bounded on it, not overall: their roots check still parses the image, which
is the whole-image verifier a later phase replaces with one over a mapping.

### Two promises about the store directory, written down before a mapping depends on them

The next phase of this program maps the image instead of decoding it, and a
mapped file has a failure mode a heap copy does not: when the bytes behind it
change or vanish, the next access is not an error but a machine fault. §15 of
`TECHNICAL_DETAILS.md` now states the two invariants that make a mapping safe
to introduce, so that the contract exists before the code that needs it does.

**A store file is never rewritten in place** (§15.13). Every replacement is
written to a temporary file, fsynced, and renamed over the old name, so the
bytes behind an open handle never change under it. Two things already rest on
this — the streamed digest above, and a live reader parsing an image while a
writer compacts — and a mapped image would rest on it absolutely.

The two platforms honour it by opposite routes, and writing the invariant down
is what found the difference. On unix the rename leaves the old inode alone and
a held handle goes on reading the image it opened. On windows, renaming a file
*over* another is refused while any handle to the target is open — with
`FILE_SHARE_DELETE` and without it alike, because that flag permits renaming a
file *aside*, which is a different operation. The reader is protected just as
completely, by the compaction failing rather than by two files coexisting: a
process holding `graphene.csr` open stalls a writer's compaction with `Access
is denied`, the store is left untouched, and the next attempt succeeds once the
reader lets go. For the mapping work this settles something the plan had only
guessed: installing an image underneath a mapping cannot be a single rename on
windows, so the old name has to be moved aside first and the new file put in
its place second. `TestImage_AHeldHandleNeverSeesTheBytesChange` pins both
routes.

**The directory is the engine's to write, and no one else's, while a handle is
open** (§15.14). This is an obligation on the caller rather than a property the
engine can enforce: the write lock keeps other graphene processes out, and
nothing keeps out `truncate`, an editor, or a backup tool that rewrites files in
place. With the image read into the heap, breaking it costs a parse error at the
next reopen. Under a mapping it costs the process, at whatever unrelated line of
caller code happened to touch the slice — `SIGBUS` on unix,
`EXCEPTION_IN_PAGE_ERROR` on windows, neither recoverable in Go. That is a
*defined* outcome rather than a safe one, and it binds hardest on `OpenLive`
readers, which take no lock at all. The subprocess test that asserts the fault
waits for the mapping to exist.

### Declarations are now a property of the store, not of the process

A declaration tells the engine something it cannot work out for itself, and it
did not survive being told. Ordered keys and composite tuples were written into
the CSR image, so they came back after a compaction and vanished after a bare
reopen. Unique property keys and edge cardinality constraints were written
nowhere at all: every process that opened a store had to re-declare them, and one
that forgot got a store with no constraints and no indication there had ever been
any.

The lost optimisation was the mild half. The sharp half was that **two processes
could open one directory enforcing different rules**, both believing they held
the guarantee — and the one that had not declared would write exactly the
duplicates the other existed to refuse.

Every declaration is now recorded in `graphene.schema` beside the image as it is
made, and re-applied by every `Open`:

```go
g, _ := graphene.Open(dir)
g.DeclareUniqueProperty("sha256")
g.Close()

g, _ = graphene.Open(dir)          // no re-declaration
_, err := g.AddNode(dup)           // still refused
```

It covers all four kinds — ordered keys, composite tuples, unique property keys,
unique edge types — and applies to a read-only store too, which previously got
none of the writer's declarations, so a declared range query silently became a
scan for one process and not the other.

**No format change.** `graphene.schema` is a sidecar, like `graphene.labels`: an
older engine ignores a file it does not know about, a v0.7.0 store opens
unchanged in v0.5.x and v0.6.0, and backup carries it with no change because
backup copies the directory. Ordered and composite declarations are still written
into the image at compaction as well, so an older engine keeps finding them
there. The two sources are unioned, which cannot conflict: both are monotone.

The file is plain text, sorted, and deterministic — two stores with the same
schema produce identical bytes — and property keys are escaped, so it imposes no
restriction on what a key may contain.

### Behaviour change: an `Open` can now refuse a store that violates its own catalogue

Because the record outlives the process, a store's data can stop satisfying a
constraint the file names — an older build, or a process that never declared,
could have written the duplicates. `Open` therefore re-validates, and by default
**refuses**, naming every violation rather than the first. That is the same check
`DeclareUniqueProperty` has always made, at the same moment; a store that came up
believing a constraint it did not keep would have the guarantee and none of the
behaviour.

A store that cannot be opened cannot be repaired, so there is an escape hatch:

```go
g, err := graphene.OpenWithOptions(dir, disk.Options{Constraints: disk.ConstraintDrop})
// opens without the violated declarations; s.DroppedDeclarations() names them
```

`OpenReadOnly` and `OpenLive` default to `ConstraintDrop`, because a reader
enforces nothing and reading a damaged store is what it opened one to do.

### Transactions can read, and can be refused over what they read

`Tx` was a write-only buffer, so read-modify-write — the most common shape there
is — could not be made atomic. You read a node, decided from it, wrote it back,
and nothing checked that the record you decided from was still the record you
were overwriting. Two workers advancing the same node's state lost one of the two
updates, silently.

Transactions now read:

```go
tx := g.Begin()
id := tx.AddNode(&store.Node{...})
n, _ := tx.GetNode(id)              // sees its own buffered write
edges, _ := tx.EdgesOf(src, store.DirectionOutbound, nil)
```

`GetNode`, `GetEdge`, `NodeExists`, `EdgesOf`, `Neighbours`, `UniqueNodeOwner`
and `UniqueEdgeOwner` all answer as the transaction will look once committed:
buffered writes visible, deletes gone — including the edges a node deletion
cascades to — and unique claims resolving to what this transaction claimed them
for. This works on every backend and changes no failure mode.

`BeginTracked` adds the conflict check:

```go
for {
    tx := g.BeginTracked()
    n, err := tx.GetNode(id)
    if err != nil { return err }
    tx.UpdateNode(advance(n))
    err = tx.Commit()
    if errors.Is(err, store.ErrWriteConflict) { continue }  // re-read and retry
    return err
}
```

Every read that reached the store is recorded, and re-read under the write lock
before anything is applied. If a record changed, disappeared, appeared, or a
unique key moved, the transaction is refused whole and nothing is written. The
error is a `*store.ReadConflictError` naming the observation that moved, and it
wraps `ErrWriteConflict`, so the retry loop callers already write for upserts
works unchanged.

Adjacency is tracked as a set, contents included, which makes "does this node
already have such an edge — if not, create one" safe. Where a store-level
constraint fits, it is still the better answer: see `DeclareUniqueEdge`.

**Opt-in, deliberately.** `Begin` behaves exactly as before and records nothing.
Tracking can only *add* a failure, and code that upgraded and started reading
inside its transactions should not begin failing at commit without asking for it.

**What it is not.** Not serialisability, and no protection from phantoms. A
transaction that read "nothing carries this label" is not protected against
something appearing — a predicate over the whole graph cannot be validated by
re-reading a bounded set — which is why there is no tracked `QueryNodeIDs`.
Validation compares a digest of what was observed rather than a version stamp,
so a record rewritten with identical bytes is deliberately not a conflict.

A backend that does not implement the new `store.CheckedTransactor` refuses a
transaction carrying reads rather than committing it unprotected. Both bundled
backends implement it.

### A query's cost tracks its answer, not the graph

`Offset`/`Limit` used to be a step at the end of the pipeline and nothing more.
The engine produced every row, sorted them, and copied ten out — so ten rows off
a 100 000-node label cost the label. Nothing streamed either: there was no
iterator anywhere in the module, and enumerating a graph meant materialising one
ID per record before the caller saw the first one.

**Label postings are merged rather than concatenated.** The delta's postings and
the image's are both ascending and duplicate-free, and were being appended
together and deduplicated through a map. Merging them drops the copy and the map
— but the point is that the result is now *ordered*, which is what makes
everything below sound: the label driver reports its candidates as ascending, and
labelled queries skip the sort they used to end with.

**A window is pushed back into the pipeline.** Where nothing between a step and
the window can remove a row, the bound is handed to that step so it stops
producing:

| Query, 100 000-node fixture | Before | After |
|---|---|---|
| `Types + Limit 10`, disk | 17 952 B, 121 allocs | **160 B, 2 allocs** |
| `Types + Limit 10`, memory | 7 416 B, 16 allocs | **240 B, 3 allocs** |
| `Types + prefix filter + Limit 10`, disk | 19.1 MB, 90 818 allocs | **722 KB, 6 allocs** |
| `Types + prefix filter + Limit 10`, memory | 9.55 MB, 559 allocs | **1.44 MB, 7 allocs** |

The filtered case cannot bound its driver — a filter removes candidates, so the
tenth candidate is not the tenth row — so the bound moves one step later: the
residual pass evaluates a prefix of the candidates, stops when enough survive,
and doubles the prefix when they do not. The doubling is what keeps §7.3's
per-step probe-versus-build costing meaningful while bounding the repeated work
at a small constant factor.

Four cases refuse the bound rather than approximating it, and go on costing what
they always did: a descending query, one driven from an explicit ID list, a
`MatchAny` query, and an edge query restricted by endpoint. The ordered-index
driver emits value order rather than ID order, so it cannot take one either.

**Iteration, over a snapshot.** `store.Scanner` adds `ScanNodes`, `ScanEdges` and
`ScanNodesByType`, each an `iter.Seq2[ID, error]`:

```go
snap, _ := g.Snapshot()
defer snap.Close()

for id, err := range snap.(store.Scanner).ScanNodes() {
    if err != nil {
        return err // closed or expired mid-scan
    }
    // ... break whenever you like
}
```

It is offered over a `Snapshot` and not a live store, which is the answer to what
a scan should do when a writer changes the graph beneath it rather than an
omission. `Seq2` rather than `Seq` because a scan can fail partway and a scan
that stopped early must not look like one that finished. IDs arrive ascending on
both backends — the same order the equivalent query returns — so a scan
substitutes for one: `bulk` now streams a source that implements `Scanner` and
enumerates one that does not, and the two produce byte-identical dumps.

The disk implementation resolves a batch under the store lock and yields it
outside, never holding the lock across caller code; the batch starts at 32 and
doubles to 512, so taking one ID does not pay to resolve five hundred. Peak
memory is the delta plus one batch rather than the graph — 6 allocations for a
full walk and 6 for a walk stopped at the first ID, neither growing with the
graph. A snapshot also enumerates its property entries now, which is what lets it
back a complete `bulk` export.

A snapshot reports the store's index declarations too — the read half of the
declarer interfaces, split out as `store.OrderedIndexReporter` and
`store.CompositeIndexReporter`, because a view may say what was declared and must
not declare anything itself. Without it a dump taken through a snapshot carried
an empty header and imported with no ordered or composite indexes, which is a
silent loss rather than a failure: the import succeeds and the restored store
answers range and composite queries the slow way. `graphene export graph` now
reads through a snapshot, so a whole-graph export streams its enumeration and
dumps one graph rather than whatever each read happens to find.

**Fixed: an export was not reproducible.** `PropertyIndex.ForEachNodeProperty`
is the streaming form of `NodeEntries` and writes a dump's property section, but
it ranged the per-value map directly instead of ordering it. Two exports of one
unchanged graph therefore disagreed about the order of their property lines
about one run in seven on a key with 200 distinct values — the same class of
defect `NodeEntries` sorts to avoid, and whose reasoning that function's comment
already gave. It now walks values in order. The buffer that orders them is sized
to the key rather than grown by doubling, which makes `NodeEntries` and
`EdgeEntries` cheaper as well: 33% fewer bytes on a 50 000-entry index, on the
path `Compact` writes the CSR index section from.

### The shortest path can now be the cheapest one

`ShortestPath` returns a path with the fewest edges, which is the right answer
only when the edges are interchangeable. When they carry a similarity, a
duration or a size, fewest is not cheapest — and the engine had no way to say
so. There is now Dijkstra and A\*:

```go
gap := func(e store.IncidentEdge) float64 { return 1 - float64(e.Weight) }

path, err := g.ShortestWeightedPath(src, dst, nil, gap)
path, err  = g.AStarPath(src, dst, nil, gap, estimateRemaining)
```

with `…Ctx` forms taking a context and a `store.Budget`, whose `MaxNodes` counts
nodes *settled* — reached with their final cost — so it keeps meaning what it
means for every other walk.

**The cost comes from the caller, and this is the design decision.** The obvious
alternative was to read `Edge.Weight` as a distance, and it is wrong twice over:
`Weight` is documented as a similarity score for `EdgeTypeSimilarTo` and encoded
as one in the Merkle leaf, so reading it as a distance makes "more similar" mean
"further away"; and no single reading suits every caller anyway. A selector
taking `*store.Edge` would have been the other obvious shape, and would force a
`GetEdge` per relaxation — the per-record catastrophe the adjacency extension
exists to avoid. So `store.IncidentEdge` grew a `Weight` field instead, on the
same argument that put `Neighbour` there: the store already holds the edge
record while filtering, so copying a float32 out is free.

That field is additive — all three construction sites use keyed literals — and
was measured against a control identical but for it: **allocation counts
unchanged on every traversal benchmark**, bytes up by one buffer entry's width
(24 B/op on a 3-hop BFS, 8 B out of 1.2 MB on a deep DFS).

**The search is unidirectional, and that costs something.** A bidirectional BFS
may stop at the first node both frontiers reach, because for an unweighted walk
that meeting is on a shortest path by construction. It is not, once edges have
costs — the first meeting is merely the first. Making bidirectional Dijkstra
correct means continuing past it with a different termination proof, so
`ShortestWeightedPath` does not do that. The consequence is measured and
reported rather than buried: on the benchmark fixture a weighted path runs ~15×
slower than the unweighted one, because it settles every node cheaper than the
destination instead of meeting in the middle. `docs/benchmarks.md` has the
numbers. A\* is the answer where an estimate exists — on a grid it is worth 2.8×
in time and 38% in memory, for the identical path.

**What it refuses, and what it cannot.** A nil cost function is an error rather
than a default unit cost, because a unit cost is exactly `ShortestPath` and
quietly returning the unweighted answer would hide the mistake for as long as
the weights happened not to matter. A negative or NaN cost is refused at the
relaxation that produces it: Dijkstra is not approximately wrong with a negative
edge, it is wrong, and a plausible bad path is worse than an error. That check
covers costs the search *uses* — it stops when the destination settles, so an
edge in a corner it never examined is never costed, and the path returned is
unaffected by one.

**A\* asks something in return.** `store.NodeHeuristic` must never overestimate
the remaining cost. One that does returns a dearer path and no error, because
catching it would mean computing the answer the estimate exists to avoid. A node
is settled once and never reopened, which under an inconsistent heuristic is
what turns the guarantee into "a path" rather than "the cheapest path";
reopening would restore optimality at the price of unbounded re-expansion, and
that trade is not made. A nil heuristic is exactly Dijkstra.

Where several routes tie for cheapest, which one comes back is unspecified: it
follows the order the backend reports incident edges in, which is not a promise
the store makes across layer states. The cost is determined by the graph; the
shape, among equals, is not.

### Fixed: `BeginTracked` livelocked under contention

A read-modify-write retry loop with a few concurrent writers never finished. The
transaction read at the visible epoch while its read set was validated at the
applied one, and group commit leaves those apart for the whole of a commit's
fsync — with the store lock released. So a transaction read the old value, was
refused by a writer it was not permitted to see, retried, read the *same* old
value because the fsync was still outstanding, and was refused again. With
several writers there is always a commit in flight, so no attempt could ever
incorporate the write refusing it. Measured at 15 877 conflicts with five of six
writers starved; with `SetSyncOnCommit(false)`, which closes the window, 1 046
conflicts and none starved.

A tracked transaction's reads now resolve through the new `store.AppliedReader` —
the same view its validation will use. The exchange is that such a read can see
state that is committed but not yet durable; a crash losing that write loses this
transaction's commit with it, since the commit is ordered after it in the same
log and replay stops at the first bad frame. Untracked transactions and ordinary
reads are unchanged and go on seeing only what is durable.

### Fixed: an export could write a dump that could not be imported

The property index is not versioned and its entries can outlive their records —
indexing a node that has been deleted leaves one, and so does `ReindexKeep` on an
update. `bulk` copied them out verbatim, so the dump named an entity it never
defined and the import of it failed with `ErrOutOfOrder`, after the operator had
a file they believed was a backup. Property entries whose entity is not there are
now dropped, which is one lookup per entry rather than a set of everything
exported — the export is streamed precisely so it does not hold the graph.

### Fixed

- **A node updated inside a transaction disappeared for concurrent readers.**
  The disk backend publishes a transaction's epoch only after the fsync that
  covers it, and releases the store lock before that wait so concurrent commits
  share one sync. Version retention did not account for it: a writer discarded
  the superseded version, and the superseded label posting, whenever no
  *snapshot* was open — but every plain reader in that window is running an
  epoch behind, and what it was entitled to see had just been deleted. So
  `GetNode` returned "not found" and `NodesByType` omitted it, for a live record,
  for the length of the fsync. Not stale: **absent**. This affects every release
  that has had transactions, is trivially reproducible with one committing
  goroutine and one reading one, and is why `tests/graphene_visibility_test.go`
  exists. Retention is now floored at the visible epoch while any commit is
  unpublished; a chain settles at two versions instead of one under a
  transactional writer, and single-record mutators are unaffected because they
  publish under the lock. Measured: no time regression, one allocation fewer.
- **`OpenWithOptions` never loaded the label table.** A store opened with
  `disk.StrictOptions` rendered every custom label as a number. Every open path
  now loads both sidecars.
- **`DeclareOrderedNodeProperty`, `DeclareOrderedEdgeProperty` and the two
  composite declarers took no store lock**, so two goroutines declaring
  concurrently raced. They now take the write lock, which they need anyway to
  write the catalogue atomically with the in-memory declaration.
- **The label table was written without an fsync.** A rename promotes whatever is
  on the medium, and without the sync that can be a prefix — which comes back as
  a truncated table and therefore as a mislabelling.
- **`GCMP` was missing from the CSR's known-section lists**, so `graphene csr`
  reported the composite section as unrecognised, and a future build marking it
  critical would have been refused for the wrong reason.

### Internal

- `FuzzDecodeCatalogue` covers the new parser from the start, rather than joining
  `readCompositeSection` on the list of hand-rolled readers that have none.
- Read-set comparison lives in `store.ReadSetValidator`, driven by both backends
  rather than implemented in each. Two entirely different storage layouts cannot
  be relied on to reach the same verdict if each owns its own copy of the
  reasoning — and a read set is a constraint, where a divergence is a lost write.
- `BenchmarkTxUpdate_Disk_NoSync_*` isolates the transactional write path from
  the fsync, which dominates `BenchmarkConcurrentCommits` by an order of
  magnitude on a loaded machine.
- `QueryPlan.Candidates` is now "candidates examined" rather than "size of the
  driving set". A driver that stopped at the window never learned its own full
  size, and running it again unbounded to fill the field in would make asking
  for the plan cost more than the query. `ResidualStep.Probe` has always been
  documented as a forecast rather than a fact; this is the same trade.
- `TestQueryPlanner_WindowParity*` compares every windowed query against the
  *unwindowed* one sliced, because the existing forced-scan oracle takes the
  same window through the same push-down and would agree with a bug in it.
  `TestResidual_BoundedPassCrossesChunkBoundaries` builds a candidate set
  several prefixes deep whose matches are all at the far end, which is the only
  way to reach the code that has to hold its place across them.
- `traversal/heap.go` is a hand-rolled binary min-heap rather than
  `container/heap`, whose interface costs an indirect call per comparison and
  whose `Push` takes `any` — boxing every element, which would put one
  allocation on the inner loop once per relaxed edge. The queue uses lazy
  deletion rather than decrease-key, because a decrease-key heap needs a
  position index and that per-node allocation is the whole saving.
- The weighted search has an oracle of its own: a naive Bellman-Ford written
  against the public API, in the spirit of `referenceBFS`. It walks `EdgesOf`
  rather than `Neighbours`, deliberately — `Neighbours` reports one result per
  distinct neighbour, so an oracle built on it cannot see the two parallel
  edges of unequal cost that are the case the feature exists for.
- `TestAllocGuards_Paths` asserts a *slope*, not a ceiling. A ceiling on a
  200-node chain would mostly measure the fixture, since the path is the graph
  and materialising it dominates; the property the searches actually promise is
  that cost tracks the path and not the walk, so it runs each search to two
  depths and checks the difference is the records. It also replaces a number
  nobody could fail: `BenchmarkShortestPath` recorded 37 allocs/op and had no
  `b.ReportAllocs()`, let alone a guard, while this release refactored the tail
  it shares with the weighted search.
- `BenchmarkWalk_Grid_*` has its own fixture because the shared one cannot host
  it: 1 000 inbound edges into a single hub put every node within two hops of
  every other, so any distance-based estimate overestimates — and an
  overestimating heuristic does not make A* slower, it makes it wrong. The
  admissibility check in the benchmark setup caught exactly that, and compares
  costs rather than hop counts, which was the original error.
- Both "applies to every walk" tables — the deadline table in
  `traversal/guard_test.go` and `TestBudget_AppliesToEveryTraversal` — gained
  the two new walks. Their whole point is that a walk they miss is unbounded.
- `TestAppliedReader_*` constructs the applied-but-not-visible state directly
  rather than racing for it: the window it covers is open only while a commit
  waits on its fsync, so a test that raced would pass on a fast disk.
- `TestVerifyCSRDigest_DoesNotReadTheImageWhole` bounds
  `runtime.MemStats.TotalAlloc` rather than sampling residency. The counter is
  cumulative and exact, unaffected by when the collector runs, so a single
  whole-file read shows up as one allocation the size of the file — and the
  test refuses a fixture too small to tell, rather than passing on it.
- `os.SameFile` cannot detect a replaced file on windows. Go's `fileStat` loads
  the file identifier lazily, by path, so two stats of the same name taken
  before and after a rename-over compare equal. The held-handle test re-reads
  the bytes it holds instead of comparing identities, which asserts the property
  directly on both platforms.

### Upgrading a store written by v0.6.0: compact it once

**Opening is several times faster, and none of that arrives until the image has
been rewritten.** Two things made a v0.6.0 open slow: the whole image was read
into the heap and every blob copied into an arena, and the property index was
rebuilt one entry at a time through `IndexNode`. `ImageMapped` removed the first
and v9's GPIX section removed the second, and both are defaults now.

But a file v0.6.0 wrote is format v8, and a v8 image carries its property index
as a GIDX section. `deserialiseCSR` loads a GIDX entry by entry **whatever
`IndexMode` says**, because `IndexMapped` has nothing to map — the mappable
section is exactly what v8 does not have. So upgrading the library under an
existing store leaves the index half of the open costing what it always cost.

Measured on a 462 MiB store, four interleaved rounds per arm, one process each,
on an AMD Ryzen 9 5980HS with the store on NVMe:

| Open | median | minimum |
|---|---:|---:|
| v9 image, defaults | 301 ms | 221 ms |
| **v8 image, same defaults** | **2,102 ms** | 1,262 ms |
| v9 image, forced into v0.6.0's modes (`ImageHeap` + `IndexResident`) | 2,149 ms | 1,097 ms |

At 2.26 GiB the v9 default open is **1.40 s** against **9.94 s** for the same
store served the v0.6.0 way — about 7×, holding across both sizes.

The fix is one `Compact()`, or `graphene store migrate -to 9`, which rewrites the
image as v9. It is a one-time cost and it is also what makes the memory figures in
this release apply: the same v8 file holds its whole property index on the heap.

Two other causes of a slow open that this does not address, worth ruling out
separately. **A store that has never been compacted replays its whole WAL**,
which is bounded by write history rather than by data size —
`disk.PreflightOpen(dir)` reports what an open is about to cost before you pay
it, and `Options.MaxReplayRecords` / `MaxReplayBytes` refuse rather than grind.
And spare RAM does not help either case: v0.6.0's open was slow because it
*copied* roughly 3.5× the file, not because it ran out of room.

The wall-clock spread above is wide — every arm varies about two-fold across
rounds on this machine — so the ratio is the claim and the individual figures are
not. Both minima and medians are given for that reason.

---

## v0.6.0 "Armchair" — structure, aggregates, and names

Three additions, all of them things a caller was working around, and one fix to
the only traversal budget dimension that was never enforceable. Nothing here
changes how an existing on-disk graph is read or written: **there is no format
change**, and a v0.6.0 store is readable by v0.4.0 and v0.5.x, and vice versa.

No breaking API changes — every addition is a new method or a new optional
interface. The one behaviour change is deliberate and is called out in its own
section: a walk that was overrunning its `MaxTime` now stops.

### Duplicate edges can be prevented without a unique key on every edge

`UpsertEdge` resolves an edge through a declared-unique *property*, which is
right for an edge carrying identity of its own and wrong for structure. An edge
that exists only to say "this owns that" has no value worth indexing, and giving
each one a unique key means paying an index entry per edge to restate what its
endpoints already say — which a caller writing millions of them cannot afford.
The alternative was a convention: own a subtree, delete it before re-ingesting,
never add an edge twice by hand. That holds exactly as long as nobody forgets.

```go
g.DeclareUniqueEdge(edgeOwns)     // at most one Owns edge per (src, dst)
_, err := g.AddEdge(dup)          // errors.Is(err, store.ErrUniqueViolation)
```

**Per label, not per label set.** Every filter in the engine treats a type list
as OR over labels, so a declaration names one type and means "at most one live
edge from src to dst carries this type". An edge labelled `{Owns, Contains}`
counts against a declaration on each independently. Direction is part of the
pair.

Enforced on every write funnel — `AddEdge`, `AddEdges`, `UpdateEdge` adding a
declared label, and all three inside a transaction — and identically on both
backends. A batch and a transaction also check against themselves, because two
duplicates arriving together are the same violation as one arriving after the
other. Deleting an edge frees the pair *within the same transaction*, which is
what makes delete-the-subtree-and-rebuild work.

Declaring over a graph that already violates the rule reports **every** offending
pair through `*store.EdgeCardinalityViolationsError` and declares nothing, the
way `DeclareUniqueProperty` does. A refused write carries the incumbent in
`*store.ErrEdgeTaken`. Both unwrap to the existing `store.ErrUniqueViolation`.

Nothing new is stored: the answer is in the source node's outbound adjacency, and
a second structure holding it is a second thing that can disagree with the first.
The cost is a scan of that adjacency per constrained write — cheap for the
ownership edges this exists for, worth knowing before declaring a type whose
sources are hubs.

Declarations live in memory and must be re-declared at every `Open`, as with
unique property keys. `graphene debug unique-edge -type <t> <dir>` answers "can
I?" from the shell.

### Aggregation, instead of pulling IDs into Go and counting them there

```go
freq, err := g.NeighbourFrequency(versions, store.DirectionOutbound,
        []store.EdgeType{edgeEmbeds}, []store.NodeType{nodeTracker})
counts, err := g.CountNodesByType()
dist, err := g.CountNodesByProperty("state")
```

`NeighbourFrequency` is the one most cross-graph analytics turn out to be: rank
what a set of sources point at, by how many of them point at it. It counts
**anchors, not edges** — a neighbour reached twice from one anchor is one vote,
and a repeated anchor is one anchor — because counting edges would make the
answer depend on how the graph happens to be modelled rather than on what it
says. It lives in `traversal` so it can take a `store.Budget`: the anchor set is
usually the result of an earlier query, so its size is data rather than something
the caller chose, and one hub among the anchors is enough to make it arbitrarily
large. `NeighbourFrequencyCtx` is the bounded, cancellable form.

`CountNodesByType` / `CountEdgesByType` are answered from the label postings, and
also appear on `GraphStats` and in `graphene store stats`. **An entity carrying
two labels is counted under both**, so they total more than `NodeCount` and
`EdgeCount` on any graph using multi-label entities; the CLI says so where the
tables would otherwise look wrong.

`CountNodesByProperty` / `CountEdgesByProperty` distribute an indexed field's
values, counting only live holders. Entities with no entry under the key have no
value to distribute and are absent, as are values held only by deleted entities.

All four are `store.Aggregator`, an optional extension both bundled backends
implement. It has no generic fallback — a `GraphStore` cannot enumerate its own
types — so a backend without it gets an error rather than an empty map that would
say the graph is empty.

### Custom labels can have names, and the store can carry them

`NodeTypeCustomBase` opens 32 768 values the application defines the meaning of,
and every one of them rendered as `Custom(7)`. An application with a dozen custom
types therefore carried its own number-to-name table and translated at every
boundary — and the moment that table drifted from the numbering the data was
written with, every log line, error and export silently misread the database.

```go
g.DeclareTypeNames(
    map[store.NodeType]string{nodeApp: "App", nodeVersion: "AppVersion"},
    map[store.EdgeType]string{edgeOwns: "Owns"},
)
```

The name is what `String()` renders and what `ParseNodeType` and `ParseEdgeType`
accept — **in addition to, never instead of** the `Custom(7)`, `custom:7` and
bare-numeric forms, so nothing that parsed before stops parsing. Names that could
not round-trip are refused: a built-in name, a numeric, anything shaped like a
custom-offset selector. Only the custom range can be named; the built-ins mean
what the engine says they mean.

**The half worth more is persistence.** On a disk store the table is written to
`graphene.labels` beside the image, so the graph stays interpretable without the
program that wrote it — which for a forensics engine is the normal case rather
than the exception. It is a sidecar, joining `graphene.grants`,
`graphene.redactions` and the rest, so the image format is untouched and an older
engine ignores it. Backup picks it up unchanged, because backup copies the
directory rather than a list of names it knows.

`Open` registers what the table says, and **a disagreement is an error rather
than a resolution** (`store.ErrTypeNameConflict`). The registry is process-wide,
so two stores in one process that disagree about what 32 768 means cannot both be
rendered correctly, and picking one silently is exactly the confident wrong
answer this exists to prevent.

### Fixed: `MaxTime` was the one budget dimension that did not bind

No API change and no format change here either, and no behaviour change at all
for a walk that does not set `Budget.MaxTime`.

#### `MaxTime` was unenforced on short walks

The traversal guard checked the context and the clock on the same 256-step
cadence. That number was chosen for cancellation, where overshooting by a few
microseconds costs nothing because the caller has already stopped caring about
the answer. As a deadline cadence it left a hole: **a walk charging fewer than
256 steps never looked at the clock at all**, so `MaxTime` was silently not
enforced on exactly the walks whose cost a caller cannot predict — few steps,
each of them slow, which is what a traversal over cold pages looks like.
`MaxNodes` and `MaxEdges` never had that hole, because they are charged per
visit.

A deadline is now checked every 16 steps. 16 is measured, not assumed: checking
on every step costs about a fifth of the walk, and 16 is indistinguishable from
no deadline at all. The full comparison is in `docs/benchmarks.md` under Phase 8,
along with `BenchmarkBFS_Deep_Budget` — the first benchmark in the tree to
exercise a non-zero `Budget`.

The check is paid only by callers who set `MaxTime`. With no deadline the guard
runs the code it always ran, and the benchmark's `unbounded` and `nodes` arms are
there to keep that true.

#### `enter()` now does what its comment claimed

It documented catching "a budget so small the origin alone exceeds it" and only
ever checked the context. A deadline that has already arrived now stops the walk
before it visits anything.

#### A negative `MaxTime` is a deadline already passed

It used to be ignored — `newGuard` tested `MaxTime > 0` — while `Unlimited()`
still reported `false`, so the walk paid for accounting and enforced nothing.
That was the one reading of "minus one second" that is neither a limit nor
unlimited. It is now refused at entry. Zero is still unlimited.

#### `MaxTime` is documented as best-effort, because it cannot be anything else

`MaxNodes` and `MaxEdges` are charged per visit and stop the walk on the step
that crosses them. `MaxTime` has to read a clock, and a clock has a granularity.
On Windows the Go monotonic reading is the system timer interrupt: 15.6 ms by
default, ~0.5 ms while some process on the machine has raised the timer
resolution, and which of the two applies changes as unrelated programs start and
stop. Two readings inside one tick are *equal* rather than ordered, so a
`MaxTime` below that floor cannot be observed as exceeded by `After`, by
`!Before`, or by any other comparison.

`store.Budget.MaxTime` now says this where a caller sizing a budget will read it,
as do `docs/API_REFERENCE.md` and `docs/TECHNICAL_DETAILS.md`. Nothing preempts a
single step that runs long either, so a cold page read can outlast a `MaxTime` on
its own. Bound a walk with `MaxNodes` and `MaxEdges`; let `MaxTime` catch what
they miss.

*If this affects you*: a walk that was overrunning its `MaxTime` now stops. That
is the fix, and it is a behaviour change for anyone who set a deadline the engine
was not honouring.

#### Deadline tests no longer depend on the machine's clock

`tests/graphene_budget_test.go` asserted that `MaxTime: time.Nanosecond` stops a
4 000-node walk. On a coarse-clock platform that is asserting the clock advanced
between two reads, which is a property of how busy the box is and of what
unrelated software has done to the timer resolution — so it failed intermittently
on Windows, moving between the `/disk` and `/memory` subtests from run to run.

It now uses a `MaxTime` already elapsed, which means the same thing everywhere.
Enforcement proper moved to `traversal/guard_test.go`, which drives an injected
clock and can therefore assert the cases the real one cannot reach: a deadline
reached exactly on a reading, the cadence itself, and the short-but-slow walk
that motivated the fix.

### Also

- `EdgeIDBetween(src, dst, types)` returns the incumbent rather than a boolean,
  which is what a caller checking for a duplicate actually wants. `EdgeExists` is
  now a wrapper on it and no longer materialises the entire outbound edge slice —
  records, label slices and property blobs — to compare one field.
- `graphene store stats` and `graphene debug stats` grow per-label tables, sorted
  by count. The golden corpus moved accordingly.

### Measured, not changed

Two costs that were being guessed at now have numbers in `docs/benchmarks.md`
(Phase 9), and neither needed code:

- **Declaring a unique key** costs about 14.7 ns per distinct value, linear, one
  allocation. A million distinct values is 14.7 ms once at Open. The
  declare-without-validating escape hatch this was going to justify is not worth
  building.
- **An open transaction** holds roughly 0.2 KB per buffered entity — about 400 MB
  for a million nodes and a million edges, or 1.75x what the data occupies once
  stored. One source is one transaction up to a few million entities; past that,
  `bulk` is the answer and per-entity atomicity is what it costs.

---

## v0.5.0 — idempotent ingest

The theme is one property: **re-ingesting a source that has already been ingested
produces the same graph, not a second copy of it.** Everything below is either
that property or something that was quietly preventing it.

There is **no on-disk format change**. The WAL record types this release puts
inside transactions (`0x03`/`0x04`, property-index entries) have existed and
replayed since before transactions did; what was missing was a way to frame them
between the begin and commit markers. A v0.5.0 store is readable by v0.4.0 and
vice versa.

### Breaking changes

Three, each deliberate. All are behaviour changes at the API surface; none change
how an existing file is read.

**1. `AddNode` and `AddEdge` reject an empty label set.**

They return an error wrapping `store.ErrNoLabels` instead of storing a
label-less entity. Such an entity indexes into no label posting, so it was
invisible to `NodesByType` and to every Types-filtered query while still being
counted — silent data loss with no error and no symptom. `UpdateNode` and the
transaction resolver had always rejected it, so the same struct that inserted
cleanly failed on update, and three separate documents described a requirement
no add path enforced. Edge labels were unchecked everywhere, including inside a
transaction; they are checked now too.

*If this breaks you*, you were writing entities you could only find again by an
ID you had to have kept.

**2. `ReindexReject` is the default `ReindexPolicy`, and the enum is renumbered.**

`UpdateNode` / `UpdateEdge` on an entity that carries property-index entries now
return `store.ErrIndexedPropertiesRequired` rather than leaving the index
describing something the entity no longer is. `ReindexReject` is the new zero
value, so `ReindexKeep` and `ReindexPurge` have moved by one — an API break for
anyone writing `store.ReindexPolicy(0)` rather than the constant.

The old default was a silently wrong answer that the query planner then trusted,
and avoiding it required every caller to remember a second method name. Note
that `ReindexPurge` was never the safe alternative: it drops the entity's
*untouched* keys along with the stale one, trading a stale-entry bug for a
missing-entry bug, and a missing entry costs a query result rather than a query.
Refusing is the only option that loses nothing.

*If this breaks you*: an entity with no index entries updates normally, so this
only fires where something was already at risk. Either move to
`UpdateNodeIndexed` / `UpdateNodePartialIndex`, or call
`g.SetReindexPolicy(store.ReindexKeep)` at open to restore the old behaviour
exactly.

**3. `UpdateNodeIndexed` / `UpdateEdgeIndexed` are now one transaction.**

They were three separate store calls that the documentation described as atomic
and that were not. Behaviour is otherwise unchanged; a crash can no longer land
the record without the entries.

### New

- **Property-index registration inside a transaction.**
  `Tx.IndexNodeProperties` / `Tx.IndexEdgeProperties`. Topology and index become
  durable together or not at all, which removes any need for a reconciliation
  pass after an unclean shutdown. Entries are framed in sorted key order, so two
  identical transactions write identical bytes.

- **Unique property keys.** `DeclareUniqueProperty`,
  `DeclareUniqueEdgeProperty`, `UniqueProperties`, `NodeByProperty`,
  `EdgeByProperty`. Declaring validates existing data and reports **every**
  violation through `*store.UniqueViolationsError`, so a graph can be repaired in
  one pass. Declaring is idempotent. Declarations live in memory and must be
  re-declared at each `Open` — which also re-validates the data.

- **Upsert.** `UpsertNode` / `UpsertEdge` on `Graph`, and `Tx.UpsertNode` /
  `Tx.UpsertEdge` which hand back the ID immediately so it can be used as an edge
  endpoint in the same transaction. On a hit the record is replaced and each key
  named in `props` is replaced rather than added to; keys not named survive.

- **`UpdateNodePartialIndex` / `UpdateEdgePartialIndex`**, which replace only the
  entries for the keys they name. This is the shape a state transition has, and
  the one `UpdateNodeIndexed` cannot express without the caller enumerating
  everything else just to preserve it.

- **The same four on a transaction**: `Tx.UpdateNodeIndexed`,
  `Tx.UpdateEdgeIndexed`, `Tx.UpdateNodePartialIndex`, `Tx.UpdateEdgePartialIndex`.
  A record and the entries describing it belong in one commit wherever they are
  written from.

- **`store.ErrWriteConflict`.** An upsert's key is resolved when the operation is
  buffered and re-checked under the write lock at commit, so two writers racing
  to create the same entity do not both succeed. This is optimistic concurrency
  control over exactly one predicate — there is still no general read-set
  conflict detection.

- **`graphene node upsert`, `graphene edge upsert` and `graphene debug unique`.**
  The shell can now express "make sure this exists" rather than only "add this":
  `node create` run twice adds two nodes, `node upsert` run twice leaves one.
  `debug unique` is the declaration run for its answer — it writes nothing and
  reports every value under a key that is held by more than one entity, which is
  the list a repair works from. Both upserts declare the key before writing, so a
  store that already violates the constraint is refused whole.

- **New errors**: `store.ErrNoLabels`, `store.ErrUniqueViolation`,
  `store.ErrIndexedPropertiesRequired`, `store.ErrWriteConflict`,
  `store.ErrKeyNotUnique`, and `*store.UniqueViolationsError` carrying
  `[]store.UniqueConflict`. Every sentinel is listed in
  [docs/API_REFERENCE.md](docs/API_REFERENCE.md) §4.

### Fixed

- **`disk.Store.IndexNodeProperty` took no lock and applied to the index before
  appending to the log** — the inverse of every other mutator, and of
  `PurgeNodeIndex`. A failed append returned an error having already registered
  the entry, leaving the index holding something the log had never heard of,
  which the next open silently dropped.

- **`memory.Store.ApplyTransaction` never bumped the version counter**, which is
  the memory backend's snapshot epoch. Two snapshots straddling a committed
  transaction reported the same `Epoch()` over different graphs — the one thing
  `Epoch` promises cannot happen.

- **A `TxOpUpdateEdge` that changed `Src` or `Dst` produced different graphs on
  the two backends**: the memory backend relocated the adjacency entries while
  the disk backend ignored the change. Endpoints are documented immutable, and
  both backends now pin them, matching what the non-transactional `UpdateEdge`
  has always done.

- **A property key longer than 65535 bytes was silently truncated** on the WAL
  round-trip and replayed under a different key. It is refused now.

### Upgrading

```go
g, _ := graphene.Open(dir)

// Declare at every open. Idempotent, and it re-validates the data.
if err := g.DeclareUniqueProperty("k"); err != nil {
    var v *store.UniqueViolationsError
    if errors.As(err, &v) {
        // v.Conflicts names every value held more than once
    }
}

// Ingest is now a transaction that includes its own index entries.
tx := g.Begin()
ver, created := tx.UpsertNode("k", []byte("ver:9f3a"), node,
    map[string][]byte{"state": []byte("extracted")})
perm, _ := tx.UpsertNode("k", []byte("perm:INTERNET"), permNode, nil)
tx.AddEdge(&store.Edge{Src: ver, Dst: perm, Labels: labels})
err := tx.Commit()   // store.ErrWriteConflict if another writer took a key
```

To keep the previous semantics with no other changes:

```go
g.SetReindexPolicy(store.ReindexKeep)
```
