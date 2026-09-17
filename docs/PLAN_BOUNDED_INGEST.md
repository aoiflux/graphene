# Memory: bounded ingest, a 2 GiB ceiling, and inputs the engine refuses to accept

> **Status.** Live tracker for this plan; updated as items land. `todo` / `wip` / `done`.
>
> **All sixteen items are done as of 2026-09-17.** Two resolved against the plan rather than to it, and both are written up where they landed: 0c's incremental arm at 1M and 2M is priced at 788 GiB and refused rather than run, and Phase 6 is resolved **without the v10 format change** because the term that was over the line turned out to be half loader transient.
>
> | item | what it is | status |
> |---|---|---|
> | 0a | per-stage attribution inside a compaction | **done** |
> | 0b | bulk-ingest arm + bytes-written counter + anon/RSS split | **done** - `TestCeiling_BulkIngestFitsUnderTheLimit`; the counter sums `MetricCompaction.Bytes` and `MetricCommit.Bytes`, which the engine already emitted. Peak anon is tracked apart from peak total, and a windows reading whose split collapsed now says so rather than printing 0.0 |
> | 0c | target-shape coefficients at 1M and 2M x 3.2 KB | **done** - MEMORY_MODEL §9.12. The bulk arm is measured at 1M and 2M: **3.70 and 7.39 GiB written, amplification 1.00× at both, 3,969 B per node against 3,971 at 25,000 records**, and the image write itself holds 13.910 and 12.391 MiB — an eightyfold size range inside a 10.3-13.9 MiB band. The incremental arm at those sizes is **priced and refused**: §9.9's amplification series is 1.802× and 1.805× per doubling, twice, which carried forward is ~46× and ~84× — 171 GiB and 617 GiB, against the 20 GiB testing ceiling. That also revises the 10M projection: the measured slope is below the pure quadratic's 2×, so the honest figure is ~37 GiB written once against a **band of 11.9-19.2 TiB**. The charged peak is the one linear column — 263 MiB per million records, crossing 2,048 MiB at **~7.6M records** — and it is the reopen, not the load, which is what settles Phase 6's gate. `batchBudgetPercent` is tuned and **stays at 2**: `BenchmarkBatchCapRatio` measures a batch holding **3.20-4.36× what the cap charges it** as peak live heap, so 2% of a 2 GiB budget is 41 MiB charged and ~176 MiB held, 8.6% of the budget. `maxIndexTailBytes` is now `Options.Compact.MaxIndexTailBytes`, default 16 MiB, floor 64 KiB, refused at `Open` with `ErrCompactIndexTailBytes` |
> | 0d | promote the scratch probe, delete `O:\tmp\ingestprobe` | **done** - the arm carries the probe's modes (`GRAPHENE_CEILING_DELTA_MIB`/`_REOPEN`), its budget knob (`_BUDGET_MIB`) and a discovery knob it did not have; the module is deleted |
> | 1a | batch caps on count and bytes, `ErrBatchTooLarge` | **done** |
> | 1b | caps derived from the budget | **done** - and the fraction is no longer provisional: 0c measured a batch holding **3.20-4.36× what the cap charges it** as peak live heap, so `batchBudgetPercent` stays at 2 and what that really chooses is 8.6% of the budget, not 2% |
> | 1c | auto-splitting writer, separate API, non-atomic | **done** - `AddNodesInBatches`/`AddEdgesInBatches` on the store and on `Graph` |
> | 1d | discover the environment's ceiling (cgroup / job object / sysctl) | **done** - `Options.DiscoverMemoryBudget`, reported via `StorageStats.MemoryBudgetSource` |
> | 1e | `CompactionPolicy.MaxResidentBytes` | **done** - fifth rule, between the delta rules and the WAL proxy |
> | 1f | index adoption gate under concurrent writes | **done** - not the shard split the plan named: `index.Tail` records the mutations and `SwapBase` replays them, because a shard split cannot carry a retraction. 0c to turn `maxIndexTailBytes` into an option |
> | 1g | `bulk` gets byte-sized batches and a compaction schedule | **done** - `Options.MaxBatchBytes`/`Compact`/`Reopen`; `import graph` bounded by default at `-bound 32` with a reopen through `Context.ReopenGraph` |
> | 2a-2d | bound the resident class (gated on 0b) | **done** - 2a/2b `Options.ResidentAdvice`: sequential over the parse and a compaction's read, random for the point-lookup life between, and the record image's pages dropped once a compaction has finished with it. **Measured on linux, interleaved, four rounds: file-backed residency after a compaction falls 53.28 -> 6.38 MiB, 88.0% less, 8.35x, with the arms' ranges not touching** - and the anonymous control is flat at 1.006. It does **not** reduce the peak *during* a compaction (0.994); that is Phase 4. Only the residency half of the trade is measured, the fault cost is reasoned, and that is why there is no default. The drop is the **record image alone** - the index mapping a commit just installed is what the next query reads. linux via `syscall.Madvise`, darwin via a raw `SYS_MADVISE` (the stdlib wraps it only on linux and this module takes no dependency for a hint); unsupported is reported through `MetricResidentAdvice`, never silent. 2c `disk.LimitWorkingSet`/`LimitWorkingSetFromCeiling`, a function and not an `Option` because `SetProcessWorkingSetSizeEx` acts on the process - the same reason `GOMEMLIMIT` is a deployment knob - read back through `GetProcessWorkingSetSizeEx` so a cap that failed to apply cannot report as applied. 2d `StorageStats.Process` carries the resident anon/file split behind `Options.ReportProcessMemory`, off by default because `StorageStats` is polled and this is its only field that asks the kernel a question (~800 ns, 2 allocs on windows); it found a defect - the windows reading **collapsed the split to zero file-backed bytes** whenever commit charge met or exceeded the working set, the common case rather than an edge, now reported as `Split=false`. Verified on a real kernel by cross-compiling the test binaries and running them under WSL2, which is **not a native filesystem** and is stated as a caveat in `docs/benchmarks.md` |
> | 3a-3e | the documentation | **done** - 3a `USER_GUIDE.md` §10 rewritten (the failing recipe replaced, both arms' figures, the write cost stated as a trade); 3b new §11 "Memory and capacity planning" (§§11-14 renumbered to 12-15); 3c `API_REFERENCE.md` §4/§5/§13b/§14 carry the batch caps and `ErrBatchTooLarge`, the splitting adders **and their loss of atomicity**, budget discovery with its provenance table, `MaxResidentBytes` as a floor, and `bulk`'s schedule with its `Reopen` hook - the resident/anon split it also lists is 2d and does not exist yet; 3d `MaxDeltaBytes` carries the index multiplier and `AutoCompact` admits it cannot reopen; 3e swept and written up as MEMORY_MODEL §9.10 - **128 MiB recovers 36% of the gap for free, 64 MiB recovers 44% for 51% of the wall clock**, and anything above what the process already charges does nothing. Stays a deployment knob. `CHANGELOG.md` has the Unreleased section for Phases 0, 1 and 3, and `docs/benchmarks.md` carries the A/B against v0.8.0 that closes the plan's "must not regress" list: point lookup, both bulk-write shapes, the batched ingest, `OpenWall_Defaults` and `RSS_CompactIncremental` all flat at 0.93-1.01x with allocation identical to the digit, and a capture across a whole compaction costing 18 allocations. The first attempt ran the arms back to back and read a **1.66x write regression that was the instrument** - v0.8.0 measured against itself drifted 1.28-1.73x between runs - so the figures are interleaved and the discarded attempt is recorded rather than deleted |
> | 4 | write-only streaming compaction | **done** - `CompactAndReopen` builds no `CSRGraph`. Gated first on 0a as the plan required, and the stage metrics answered it directly: the process peak is monotone, so the rise between two boundaries is what happened between them, and at 400,000 records the **build raises it by 197.9 MiB and the commit by 0.035**. The encoder was extracted from `(*CSRGraph).SerialiseTo` into `writeImage`, taking an `imageInput` and returning an `imageIdentity` — **one encoder, not two**, because a second would be a second definition of the format and the byte-identity test would then compare two guesses rather than one encoder reached two ways. Two things replace the graph: the header counts are **patched after the flush**, as the section table offset and both GIDX counts already were, and the property-index filter's liveness question is answered by the plan (`deltaNodes` is a subset of `deltaKnownNodes`, so an identifier the delta knows resolves inside the delta) through the new `imageMembers`. **Measured, four interleaved rounds at 400,000 × 512 B, medians: allocation 144.844 → 97.379 MB (0.672), which is 47.47 MB or 117.4 B per record against the model's ~118 B per node; the anonymous peak during the compaction 157.8 → 121.4 MiB (0.769); the whole working set peak 0.927** — with the arms' ranges not touching on any of the three. Wall clock differs by 1.119 on the medians and is **not resolvable**: the materialising arm's own rounds span 4,446–5,418 ms, a 1.22× swing, and a CPU profile puts the new membership test at 1.26% of samples against 28.4% in file IO. The first A/B was **discarded and is recorded rather than deleted**: it sampled `CompactAndReopen` against `Compact()` alone, so a whole reopen sat inside one window and outside the other, and it read a 1.38× allocation regression that was the instrument. Byte identity is a test over an image-plus-delta fixture with updates, deletes and a composite, in **both index modes**, plus an empty store and an empty delta, and it was checked against two mutants. `Compact()` is unchanged and measured so against the previous commit: allocation **1.00002**, wall clock 0.997. What the receiver holds after a streaming commit — the pinned image plus the whole delta, consistent but no longer economical — is stated in `disk/compact_stream.go` |
> | 5 | one-pass `BulkLoad` | **done** - `Graph.BulkLoad` fills an empty store in one forward pass and hands back a fresh handle. Records stream straight into the image as the caller yields them; the property index comes from an external sort of the triples going past rather than from an index the load would have to hold; GPIX, GPIR, GCPX and GIDX are written at the end, where the format already puts them. It is **the same encoder** — `writeImage` gained one field, a way to be told its sequence high-water marks after the records rather than before, which is what the header counts already did for a source that cannot know them up front. It is also the same commit: `compactPin`, a build, `compactStreamCommit`, because a load and a compaction are the same operation reached from different places. **Measured, four interleaved rounds at 100,000 × 3.2 KB under a real 2,048 MiB Job Object, against the bounded-plus-reopen arm the guide recommends: bytes written 2.42 → 0.37 GiB (amplification 6.53× → 1.00×), charged peak 249.3 → 76.3 MiB, headroom 87.8% → 96.3%, wall clock 25.12 → 4.39 s** — no row's ranges touching. And **what the image write itself holds is flat across a sixteenfold size range**, 10.305 to 12.680 MiB from 25,000 records to 400,000, taken through 0a’s stage boundaries rather than the phase sampler because a load reopens the store inside the call and an open is linear in the store by design. Projected to ten million: ~37 GiB written against ~19 TiB. The trade is stated first everywhere it appears — atomic and not incremental, the store unreadable while it runs, an empty store required (`ErrBulkLoadNotEmpty`); merging with an existing image is a concatenation on the record side and a re-emission of the old index on the other, which is a second correctness surface and is **not** built. Uniqueness is the one constraint a load must enforce for itself and is enforced exactly, from the sort rather than from an index. Byte identity against an ordinary ingest is tested in both index modes over a fixture with two composites, a member omitted from every tenth record, a key carrying two values on every seventh and a triple repeated on every fifth, plus the empty and no-edge shapes and a run with the sort forced to spill; crash injection at all five durability steps leaves the directory loadable. **Three defects found by those tests**: the merge handed out the run reader’s own buffer, so a spilled sort paired one entry’s value with another’s identifier; the two halves of the GPIX walk took two cursors over a sort that can only be drained once, silently dropping every edge property; and a bulk load emitted **no metric at all**, which made the one operation that writes a whole store invisible to every `Metrics` sink. Two mutants survived and both are written down rather than papered over |
> | 6 | map the record arena, v10 (gated on 0) | **done - resolved, and not as v10** - MEMORY_MODEL §9.13. The gate is "build it only if Phase 0 says the at-rest term, not the transient, is what is left over the line", and §9.12 said it was. Taking the peak apart instead of assuming it showed **half of it was the loader**: `deserialiseCSRFrom` parsed the record stream into a slice and `buildSeq` copied it into the arena, so every record existed twice for the length of the load - 125.90 MB allocated and immediately garbage at two million records, visible in the peak and in no profile taken afterwards. Three load-path changes, **no format change**: the loader walks the stream instead of materialising it, `buildSeq` is handed the shape its first two passes would compute (six passes over the mapping became three), and the label postings are counted before they are filled instead of grown by doubling. **Measured at two million, fresh process: peak anonymous 399.7 -> 223.7 MiB, the transient above settled 173.1 MiB -> 0.6, open wall clock 3.18 -> 2.12 s** - and an intermediate version at six passes was 69% *slower*, which is recorded rather than deleted. The peak is now the settled figure. The slope over 1M and 2M is 87.0 MiB per million above a 49.7 MiB intercept, so ten million projects to **920 MiB against a 2 GiB ceiling, 55% headroom**, where before it was ~2,000. So the premise the v10 rested on is no longer true and **v10 is not built**: the arena being anonymous was never the binding problem. Packing it 56 -> 24 bytes a record is costed in `disk/arena_spike_test.go`, worth another ~306 MiB at ten million, needs no format change either, and is not needed |

## Context

v0.8.0 shipped with the read path measured and passing: a 1.4M-node store opens at
**396.2 MiB charged against a 2 GiB Job Object, 80.7% headroom**, and the 18.4 GiB arm at
1.8M nodes charges 527.2 MiB at 74.3%. The write path was also measured and also passes —
but only in one configuration, and nobody outside `MEMORY_MODEL.md` §9.8 knows which one.

The question that opened this work was "what if I bulk-ingest 10 million nodes?". Measuring
it, and then comparing against an engine that does this well, turned up four things:

1. **Nothing in the engine bounds an ingest, and nothing bounds an *input*.**
   `AddNodesBatch` (`disk/store.go:1615`) accepts a slice of any length and allocates `ids`,
   `stored` and a WAL batch proportional to it — a caller handing it ten million nodes gets
   ten million nodes' worth of copies under one lock hold. `MemoryBudget` gates `Open` and
   `Compact` and nothing else, and at a compaction it *refuses* the operation that would
   relieve the pressure (`disk/budget.go:36-39` already admits this). `DeltaSoftLimit`
   reports and never acts. `CompactionPolicy` is advisory and its byte rule cannot see the
   property index. `bulk.Dest` has no compaction hook, so `graphene store import` of a large
   dump accumulates the whole delta and index unbounded.
2. **The engine has no idea what environment it is in.** There is no reading of a cgroup
   limit, a Job Object limit, or machine memory anywhere in the tree — `grep` for
   `MemTotal|GlobalMemoryStatus|memory.max|hw.memsize` returns nothing outside the stress
   tests. Every bound is a figure the caller had to know to supply.
3. **The configuration that passes is undocumented and non-obvious.** §9.8 establishes that
   1.4M passes at `MaxDeltaBytes` = 32 MiB **with a reopen after every interim compaction**
   (880.7 MiB and 57.0% headroom post-GCPX), and that 128 MiB — the shipped default —
   **fails**, dying at 1,200,000 records holding 1,843.0 MiB. `USER_GUIDE.md` §10's
   "Recommended lifecycle for large ingest" is ingest-everything-then-`Compact()`, which is
   the arm that peaks at 6,644 MiB.
4. **At the real target shape the knob does not reach.** The target is 10M records at
   ~3.2 KB each — ~32 GiB on disk. A bounded ingest compacts every `MaxDeltaBytes` worth of
   records and every compaction rewrites the whole image, so bytes written go as
   `N²·b / (2·chunk)`: **~16 TB** at a 32 MiB bound, ~3.9 TB at 128 MiB. Memory is not the
   binding constraint at that size — the rewrite is.

**Intended outcome.** A caller can state a memory ceiling, or have one discovered from the
environment, and the engine holds it — by refusing inputs it cannot afford, by bounding both
the anonymous and the resident class, and by making an ingest much larger than the image
linear in the data rather than quadratic. And the configuration that holds it is documented
in `USER_GUIDE.md` and `API_REFERENCE.md` rather than living in a measurement appendix.

## What the measurements establish

Taken this session, 1M nodes, 256 B blob, the audited consumer's declaration shape
(8 unique + 5 ordered + 2 composite = 13 indexed entries per record):

| ingest style | modelled heap | Go heap | peak working set |
|---|---:|---:|---:|
| ingest all, `Compact()` at the end (`USER_GUIDE.md` §10) | 1,805 MiB | 3,309 MiB | **4,325 MiB** |
| `CompactIfDue`, 128 MiB bound | 619 | 778 | 1,858 |
| …64 MiB | 322 | 1,061 | 1,989 |
| …32 MiB | 322 | 701 | 1,766 |
| …16 MiB | 322 | 568 | 1,650 |
| `CompactAndReopen`, 128 MiB bound | 424 | 566 | 1,907 |
| at rest, reopened | **78.5** | — | — |

**M1 — the store at rest is not the problem.** 78.5 MiB at 1M is 82 B/node, decomposing
exactly as §9.7's coefficients say: 56 B record arrays, 8 B label postings, 16 B adjacency,
2 B payload, composites 0 since GCPX. 10M is ~820 MiB anonymous plus ~32 GiB of evictable
page cache. Everything below is about the write path.

**M2 — `MaxDeltaBytes` watches a fraction of what a write holds, and the fraction depends on
the caller's schema.** `DeltaBytes` counts record payloads; the property index those same
writes created is not in it. At the probed shape: 338 B watched against 1,893 B held —
**5.6×**. The multiplier is `1 + 107·entries/recordBytes`, so ~1.45× for the target's fat
records. The doc comment documents the *smaller* gap (1.81× on a delete-heavy delta).

**M3 — a plain `Compact()` retains the whole payload until the handle is reopened.**
246.0 MiB after `Compact()` at 1M × 256 B against 1.9 MiB after reopening. §9.8 measured the
ratchet directly: the payload term climbed **49.0 MiB per 100,000 records and never reset**,
reaching 661.8 MiB at the point of death. `disk/compact.go:1416-1422` states the cause:
`AttachBase` runs on the load path and a compaction is not one.

**M4 — at scale the peak phase is the compaction, not the delta.** The partial 10M run
(`CompactAndReopen`, 128 MiB) reached **3,021 MiB of peak commit** against a 736.8 MiB
modelled floor. §9.8 puts it from the other side: *"the figure the process actually charges
is roughly 2.6× the modelled heap during a rebuild… a caller sizing a rebuild against
`EstimateResident` is reading a floor rather than a budget."* §4.2's transient model —
`512 B × delta records + 118 B × live nodes + 142 B × live edges + 16 B × live records` —
predicts to 2.3%, and at 10M its middle term alone is **1.18 GiB for the new `CSRGraph`**.

## What prior art says

Two engines were reviewed at the user's request. The lessons are asymmetric.

**Badger** is the one with the transferable design, and it is a design graphene has none of.
Three patterns, all worth copying:

- **Every buffer is sized in bytes and has a default.** `MemTableSize` 64 MB, `NumMemtables`
  5, `BlockCacheSize` 256 MB, `IndexCacheSize`, `ValueLogFileSize` 1 GB, `BaseLevelSize`
  10 MB. There is no term in Badger's steady state that is "however big the workload is".
- **A batch is capped on two dimensions at once and the engine refuses.** `Txn.checkSize`:

  ```go
  count := txn.count + 1
  size := txn.size + e.estimateSizeAndSetThreshold(txn.db.valueThreshold()) + 10
  if count >= txn.db.opt.maxBatchCount || size >= txn.db.opt.maxBatchSize {
  	return ErrTxnTooBig
  }
  ```

  Count *or* bytes, whichever comes first, and the refusal is a typed error the caller can
  act on rather than a fatal.
- **The cap is derived from the memory budget, not set beside it.** In
  `checkAndSetOptions`: `opt.maxBatchSize = (15 * opt.MemTableSize) / 100` and
  `opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)`. A caller who tunes memory
  down gets smaller batches automatically — the two knobs cannot be set inconsistently.
- **The ergonomic path never sees the refusal.** `WriteBatch` catches `ErrTxnTooBig`,
  commits, opens a fresh transaction and retries, and bounds in-flight work separately
  (`SetMaxPendingTxns`, default 16, *"to minimise memory usage"*). The price is stated
  plainly: `WriteBatch` is **not** transactional.

**Cayley**'s lesson is a negative one and short. Its loader is a thin `quad.CopyBatch` over a
batch-size flag (`internal/load.go`), and its memory behaviour is whatever KV backend it was
pointed at — Bolt, LevelDB or Badger. A graph layer that does not own its storage cannot make
a memory promise. Graphene owns its storage, which is why it *can* make one and therefore
should.

## What is already settled, and is not revisited here

- **Re-mapping the compaction's output in place (route (a))** — rejected;
  `MEMORY_MODEL.md:2088-2092`, `disk/mapping.go:33-40`.
- **Deferring label postings behind a mode** — closed, "do not build it"
  (`MEMORY_MODEL.md:977-995`).
- **Moving `DefaultCompactionPolicy.MaxDeltaBytes`** — rejected (`:2094-2099`).
- **Changing what `MaxDeltaBytes` counts** — rejected (`CHANGELOG.md:590-596`).
- **Refusing a write when the delta is large** — rejected (`disk/store.go:1018-1024`). Note
  this is *not* the same as refusing an oversized batch, which is Phase 1a: the delta is
  where a commit goes, but a ten-million-element slice is an argument.
- **`MemoryBudget` as a degradation rather than a refusal** — rejected (`disk/store.go:945-951`).
- **Raising `MaxWorkingBytes`** — measured; fifteen times the memory bought nothing
  (`disk/compact_buffers.go:24-37`).
- **`RLIMIT_AS` as the ceiling instrument** — wrong instrument (`MEMORY_MODEL.md:1513-1518`).

Two constraints to build inside: the image write **already streams** (`SerialiseTo`, one
64 KiB buffer) and GPIX/GPIR/GCPX **already sort externally** through `spillBuffer` under
`MaxWorkingBytes`. The record arena is the one thing left materialised whole.

## Enforcement is not control, and only one mechanism is cross-platform

**An OS ceiling enforces; it does not control.** It converts "used too much" into "stopped
existing" — SIGKILL from the cgroup OOM killer on linux, a refused commit and a Go runtime
`fatal error: out of memory` on windows. Neither is recoverable: an allocation failure in Go
is a runtime throw, not a panic. Graphene is a **library embedded in someone else's
process**, so an overshoot kills the host application. The WAL makes that survivable rather
than corrupting — a batch is applied by replay only when its commit marker landed
(`disk/wal.go:59-64`), a torn tail is dropped — so the cost is lost uncommitted work plus a
replay. That is the floor on the impact.

**So graphene reads limits; it does not impose them.** The mechanisms below are used to
*discover* the ceiling the process is already under, and the discovered figure then drives
the engine's own bounds. The one exception is the working-set cap in Phase 2, which trims
rather than kills.

| | discover the ceiling | bound anon | bound RSS | measure |
|---|---|---|---|---|
| linux | cgroup v2 `memory.max`/`memory.high`, v1 `memory.limit_in_bytes`, else `MemTotal` | `GOMEMLIMIT` | `madvise` on the mapping | `RssAnon`/`RssFile`, `memory.peak` |
| windows | `QueryInformationJobObject(NULL, …)` | `GOMEMLIMIT` | `SetProcessWorkingSetSizeEx` + `QUOTA_LIMITS_HARDWS_MAX_ENABLE` | commit charge, `PeakJobMemoryUsed` |
| darwin | `sysctl hw.memsize` only — **best effort** | `GOMEMLIMIT` | `madvise` | `getrusage` peak only |

`tests/ceiling_windows_test.go:13-31` and `tests/ceiling_other_test.go:7-17` already carry the
reasoning for the enforcement half, including why darwin gets a *refusal* rather than a skip:
`RLIMIT_AS` counts the mapped image — the class this programme deliberately moved memory
*into* — and `RLIMIT_RSS` is unenforced on every modern kernel. `tests/rss_darwin_test.go:6-12`
adds that darwin cannot report even current RSS without cgo, which the zero-cgo constraint
forbids.

**Consequence: darwin can never verify a ceiling, so it must never need one.** The
cross-platform guarantee comes from the in-code bounds, with linux and windows acceptance as
the *check* on arithmetic that holds everywhere.

**`GOMEMLIMIT` is the one portable mechanism and it reaches half the problem.** Identical on
all three platforms, no privilege, and it covers exactly the right class — Go-managed heap,
stacks and runtime metadata, and *not* mmap'd file pages. But it is **soft**: it reclaims
garbage and cannot shrink a live set. M4's dominant term is live for the whole compaction, so
`GOMEMLIMIT` targets precisely the gap between the modelled heap and the 2.6× charged, and
nothing below it. Past the live floor its failure mode is a GC death spiral. **Graphene must
not set it** — it is process-global and this is a guest in someone else's process. It is
documented as a deployment knob, never an `Options` field.

---

## Phase 0 — Instrument, and pin the target. Gates every later decision.

Nothing below Phase 1 should be built on the extrapolations in this document; §9.7 records a
prediction stated in advance that was wrong by 10.4%.

**0a — per-stage attribution inside a compaction.** `disk/compact.go` has three named stages.
Record the `readRSS()` peak and `estimateResidentLocked()` at each boundary through the
existing metrics sink (`store/metrics.go`), so "which stage peaked" stops needing an external
probe. §9.8's 2.6× is currently one number for the whole rebuild.

**0b — a bulk-ingest arm in the ceiling harness.** `tests/ceiling_test.go` has the write arm,
`GRAPHENE_CEILING_DELTA_MIB` and `GRAPHENE_CEILING_REOPEN`. It needs an arm that **ingests
into an empty store** rather than rebuilding, a **bytes-written counter** (which makes the
quadratic a measurement rather than arithmetic in a plan), and **the anon/RSS split reported
separately** rather than only the charged total — Phase 2 has nothing to aim at otherwise.

**0c — take the target shape's numbers.** 3.2 KB records at 1M and 2M, both arms, so the slope
is measured. Do **not** attempt 10M before Phase 4.

**0d — promote the probe.** `O:\tmp\ingestprobe` becomes the stress-tagged ingest arm in 0b
and the scratch module is deleted.

Deliverable: the coefficients, and a decision on whether Phase 5 is needed at all.

---

## Phase 1 — Refuse what we cannot afford, and discover the ceiling

This is the phase that answers the original question. No format change.

**1a — cap the batch on count and bytes, and refuse.** The Badger `checkSize` pattern, at
`disk/store.go:1615` (`AddNodesBatch`) and `:1779` (`AddEdgesBatch`), plus
`IndexNodeProperties`. New `Options.MaxBatchBytes` and `Options.MaxBatchRecords`, and a typed
`store.ErrBatchTooLarge` carrying both the offending figure and the cap, so the caller can
split without guessing. The check runs **before any ID is taken and before the lock** —
`AddNodesBatch` already validates labels there for exactly this reason, and its comment
explains why.

**1b — derive the caps from the budget, not beside it.** Badger's second lesson: a caller who
turns memory down must get smaller batches without a second decision. `MaxBatchBytes` defaults
to a fraction of the effective budget (Badger uses 15% of `MemTableSize`; the fraction here is
Phase 0's to pick), and `MaxBatchRecords` from `MaxBatchBytes` divided by the measured
per-record cost. A caller who sets both inconsistently is refused at `Open`, the way
`MaxWorkingBytes`'s floor already is.

**1c — an auto-splitting writer, as a separate API.** Badger's `WriteBatch` catches
`ErrTxnTooBig`, commits, and retries — and is explicitly **not transactional**. Graphene's
`AddNodes` documents the opposite guarantee at `helpers.go:847-849` ("either every node is
added or none is"), so auto-splitting must **not** be bolted onto it. It becomes a distinct
type — `graphene.Writer`, or `bulk`'s batcher generalised — whose doc comment states the
trade in its first paragraph. Silently splitting an atomic call is the one move here that
would be a defect rather than a feature.

**1d — discover the environment's ceiling.** New per-platform files following the existing
`disk/lock_unix.go` / `disk/lock_unsupported.go` convention: `disk/sysmem_linux.go`,
`sysmem_windows.go`, `sysmem_darwin.go`, `sysmem_other.go`, exposing one function that returns
a ceiling and its provenance. Linux reads cgroup v2 `memory.max` then v1
`memory.limit_in_bytes` then `/proc/meminfo` `MemTotal`; windows reuses
`ceilingRead`'s NULL-handle `QueryInformationJobObject` — including its flags check, which
exists because *"a process can be inside a job that constrains something else entirely"* —
falling back to `GlobalMemoryStatusEx`; darwin reads `hw.memsize` via stdlib
`syscall.SysctlUint64` (no cgo; confirm at implementation time) and declares itself
best-effort. `Options.MemoryBudget` left at zero adopts a **fraction** of the discovered
figure rather than the figure itself, and every `StorageStats` reader can see which source
was used. A discovered budget must never be *larger* than an explicitly configured one.

**1e — `CompactionPolicy.MaxResidentBytes`.** A fifth rule against
`StorageStats.EstimatedResidentBytes` in `Evaluate` (`store/interface.go:1460`). The figure
already sums the index and composites and is already O(1) *specifically so `AutoCompact` can
poll it* (`disk/estimate.go:36-43`). It is the only one of the five rules that sees what
actually grows, and it is additive — it does not change what `MaxDeltaBytes` counts, which is
what `CHANGELOG.md:590-596` refused. Its doc comment carries §9.8's 2.6× and says plainly that
the figure is a floor, not a budget.

**1f — the index adoption gate.** `adoptCompactedIndex` (`disk/compact.go:1228`) is gated on
`tail > 0`, so *a compaction that ran while anything was writing gives nothing back* — and a
bulk ingest is exactly that workload. Fix it the way the delta already does: **split the index
shards at the pin** as `delta.since` (`disk/view.go:274`) splits the record delta — freeze the
current shards, start fresh ones for writes landing during the build, drop the frozen set at
commit. `SwapBase`'s precondition (`index/retract.go:305-309`) becomes satisfiable under load.

**1g — `bulk` gets bytes and a compaction schedule.** `bulk.Options.BatchSize` counts
*records* (`bulk/bulk.go:125-132`) — the wrong unit when records vary in size, and its own doc
comment concedes "a batch of a million is a million records buffered". Add `MaxBatchBytes`
beside it, and a `CompactionPolicy` plus reopen switch defaulting to the configuration Phase 3
of the docs work documents, so `graphene store import` is bounded by default. `Dest` gains an
optional `compactor` fast-path beside the existing `nodeBatcher`/`edgeBatcher` pattern
(`bulk/bulk.go:184-190`).

---

## Phase 2 — Bound the resident class too, not only the anonymous one

The directive is anonymous-only, but a 32 GiB image whose pages are all resident charges a
cgroup, alarms an operator, and makes reclaim a latency event during exactly the ingest we are
trying to bound. File-backed residency is *reclaimable* where anonymous is not — that
asymmetry is real and should be stated rather than hidden — but "reclaimable" is not "free".

**2a — stop readahead inflating the mapping.** Sequential access over a multi-gigabyte mapping
pulls far more resident than is being read. `madvise(MADV_RANDOM)` on the property-index and
record mappings for point-lookup workloads, `MADV_SEQUENTIAL` for a compaction's scan, issued
through `syscall` with no cgo. Per-platform, no-op on `sysmem_other`.

**2b — release what a pass is finished with.** After a compaction's read of the old image, and
after a bulk load's scan, `MADV_DONTNEED` (`MADV_FREE` on darwin) on the ranges that will not
be touched again. This is safe in a way route (a) was not: it drops *pages*, not the mapping,
so no slice header is invalidated and `disk/mapping.go:27-52`'s contract is untouched. That
distinction is the reason this is buildable and re-mapping was not.

**2c — a hard working-set cap on windows.** `SetProcessWorkingSetSizeEx` with
`QUOTA_LIMITS_HARDWS_MAX_ENABLE` bounds resident bytes directly, trimming rather than killing.
Offered as opt-in through the discovered ceiling, never default: it is the one mechanism here
that can make a process slower in exchange for a smaller RSS, and that trade is the caller's.

**2d — report the split.** `StorageStats` gains resident anon and resident file alongside
`EstimatedResidentBytes`, on the platforms that can answer, with darwin honestly reporting
peak-only. An unanswerable figure must read as unanswerable, not as zero —
`tests/rss_darwin_test.go:16-19` already makes this argument for `rssCurrent`.

Gate 2a–2c on 0b's split measurement showing file-backed residency is actually large during an
ingest. If it is not, this phase shrinks to 2d.

---

## Phase 3 — The documentation, which is half the defect

Explicitly a deliverable, not a write-up at the end. The failing configuration is the shipped
default *and* the documented recipe, so a consumer following the guide today reproduces §9.8's
death.

**3a — `USER_GUIDE.md` §10 rewritten.** Replace the ingest-everything-then-`Compact()`
lifecycle with the arm that passes: bounded delta **plus `CompactAndReopen`**, with §9.8's
figures, the failing 128 MiB arm named, and the batch caps from Phase 1 shown in use.

**3b — a new "Memory and capacity planning" section**, modelled on what Badger publishes and
graphene currently does not: the per-record coefficients from §9.7, the multiplier formula
`1 + 107·entries/recordBytes`, a table of every bound with its unit, default and what it does
*not* cover, and three worked shapes — small heavily-indexed, the target's fat records, and
read-only.

**3c — `API_REFERENCE.md`** gains the new surface with the trades in the text: the batch caps
and `ErrBatchTooLarge`, the auto-splitting writer **and its loss of atomicity**,
`MaxResidentBytes`, the discovered budget and its provenance, the `bulk` schedule, and the
resident/anon split.

**3d — correct what is currently misleading.** `MaxDeltaBytes` gains the index multiplier
beside the existing delete multiplier. `AutoCompact` states that it cannot reopen and
therefore does not bound the payload term — M3 is invisible to it by construction.
`MemoryBudget` states that it gates `Open` and `Compact` only, and that a refused compaction
leaves a growing delta.

**3e — `GOMEMLIMIT`, measured once.** §6.1 attributes 153 MiB to the runtime and marks it
"`GOGC`/`GOMEMLIMIT`, not code"; nothing has ever measured it. Sweep it on the passing arm and
record both figures that matter — how much of the 2.6× gap it recovers, and the wall clock it
costs. Document it as a **deployment** knob for the reason above. It is the only bound darwin
will ever have.

---

## Phase 4 — Bound the compaction transient. No format change.

M4's dominant term is the `CSRGraph` `buildSeq` materialises whole (`disk/csr.go:378-379`,
pre-sized from touched pages, never grown) — ~118 B/node + ~142 B/edge.

**The observation this rests on: `CompactAndReopen` builds that graph and then throws it
away.** `disk/compact.go:1460-1467` is `CompactCtx` + `Close` + `OpenWithOptions`.

So: a **write-only compaction** streaming the merged record sequence straight through
`SerialiseTo` without constructing a `CSRGraph`, used by `CompactAndReopen` only. `Compact()`
keeps its behaviour exactly, so `disk/mapping.go:27-52` is untouched.

Feasible because records are written in ascending id order, which is already the merge order;
counts not known up front are already patched after the flush (`disk/csr_write.go:656-663`
does this for GIDX); the Merkle pass already streams alongside. What needs designing is
`nodeLiveBefore`/leaf-index accounting incrementally, and the `errUnstableBuild` guard
(`disk/csr.go:237`), which today depends on three passes over a repeatable sequence.

Expected: ~1.18 GiB off the 10M compaction transient. Gate on 0a confirming that term peaks.

---

## Phase 5 — A one-pass bulk load. The item that makes the target shape possible.

Release-sized, and the phase that answers "10 million fat records". Nothing in Phases 1–4
changes the fact that a bounded ingest rewrites the image once per window.

`BulkLoad` builds the image once, in one forward pass, memory bounded by a declared figure:

- records stream straight into the image's record stream — for an append-only ingest new ids
  are all above the high-water mark, so merging with an existing image is a **concatenation**;
- index entries accumulate into the existing external sorters and are written as GPIX/GPIR at
  the end — `gpirSorter` and `spillBuffer` used as they already are, plus one new sorter for
  the forward `(key, value, id)` order that today comes from walking the live in-memory index;
- composites through GCPX by the same route (`disk/csr_gcpx.go`);
- resident cost is `MaxWorkingBytes` + one batch + the page directory (4 B per 4,096 ids —
  9.8 KB at 10M). **No record arena at any point.**

**Stated up front:** it is atomic and not incremental — a crash means the load did not
happen — and the store is not readable mid-load. That is the standard bulk-load contract and
it is what buys the single pass. It needs its own durability design (build to a temp image,
rename and truncate the log on success only), which is the riskiest part of this phase.

**What it buys:** ~32 GiB written once plus ~3.1 GiB of index spill, against 3.9–16 TB.

---

## Phase 6 — Gated on Phase 0: map the record arena (v10)

The 56 B/node record arena is the largest remaining at-rest anonymous term (§8.1 lists it as
moved by **nothing**) and, doubled, a large part of any compaction that still builds a graph.
Taking it out of anonymous memory means a fixed-width on-disk arena read in place, so
`nodeRecord.Properties` and `.Labels` become slices into the mapping as blobs already are.

A format change and a release of its own; the section registry (`disk/csr_v8.go:90-122`) plus
`checkCriticalSections` is the mechanism. **Build it only if Phase 0 says the at-rest term,
not the transient, is what is left over the line after Phases 4-5.** If Phase 5 lands, an
ingest never holds an arena at all and this may be unnecessary.

> **Resolved 2026-09-17: not built, and the gate is why.** MEMORY_MODEL §9.13. Phase 0 did say
> the at-rest term was over the line, so the gate opened. Taking the peak apart before building
> anything showed that **half of it was not the at-rest term**: the loader parsed the record
> stream into a slice and `buildSeq` copied it into the arena, so every record existed twice for
> the length of the load -- 125.90 MB allocated and immediately garbage at two million records,
> visible in the peak and in no profile taken afterwards. Three load-path changes and **no format
> change** took the peak from 399.7 to 223.7 MiB and the open from 3.18 s to 2.12 s, and the peak
> is now the settled figure. Ten million projects to 920 MiB against a 2 GiB ceiling. The arena is
> untouched; packing it 56 -> 24 bytes a record is costed in `disk/arena_spike_test.go`, worth
> another ~306 MiB at ten million, needs no format change either, and is not needed.

---

## Verification

**Gates that must stay green throughout:** `tests/ceiling_test.go`'s refusal to run unless the
kernel reports a limit at least as tight as claimed, `oom_kill > 0` as a hard failure, and the
`-count=1` requirement documented at `:46-58`.

| item | proven by |
|---|---|
| 0a | a compaction's per-stage peaks sum to the process peak within §9.8's 2.6× band; stage metrics fire on every path including a refused budget |
| 0b/0c/0d | a bulk-ingest arm at 1M and 2M × 3.2 KB, both arms, reporting bytes written **and** the anon/file split; the quadratic visible in the counter |
| 1a/1b | an oversized batch returns `ErrBatchTooLarge` with no ID burned and no partial write; lowering the budget lowers the derived caps; inconsistent caps refused at `Open` |
| 1c | the splitting writer ingests a batch larger than the cap; a test asserts it is **not** atomic, so the documented trade is the measured one, and that `AddNodes` still is |
| 1d | a fabricated cgroup file, a job carrying the wrong limit flag, and a missing source each resolve to the right budget and provenance; a discovered budget never exceeds a configured one |
| 1e | `Evaluate` fires on the resident rule where the four existing rules do not; `AutoCompact` holds a probe ingest under a stated figure where 128 MiB `MaxDeltaBytes` does not |
| 1f | an interim compaction **under concurrent writes** adopts its base — the case that silently does nothing today; `ResidentEstimate.Index` falls and no entry written during the build is lost (the two directions `compact.go:1204-1211` names) |
| 1g | `graphene store import` of a fixture larger than the bound holds a stated ceiling |
| 2 | file-backed residency during an ingest falls measurably with 2a/2b and query latency does not regress; the windows cap trims rather than fails; darwin reports unanswerable as unanswerable |
| 3 | the §10 recipe, run **verbatim as written**, passes the ceiling harness on linux and windows — the check that the docs and the code agree |
| 4 | byte-identical image from the streaming and materialising paths (the comparator `MaxWorkingBytes` already uses); `CompactAndReopen`'s transient falls by the modelled term |
| 5 | a bulk load and an equivalent incremental ingest produce byte-identical images; crash injection between build and rename leaves the prior store intact; memory flat across a 10× size range |
| 6 | conditional on Phase 0 - **resolved without a format change**: every pass of the streaming loader yields the same records (`disk/csr_load_stream_test.go`, both index modes, mutation-checked against a drifting cursor and a lost three-index slice), and the open's peak falls to its settled figure |

**Must not regress:** `BenchmarkPointLookupNode_Disk`, `BenchmarkRSS_CompactIncremental`,
`BenchmarkOpenWall_Defaults`, the 1.4M read arm's 80.7% headroom, and
`go test ./tests/ -tags=stress -race -count=1 -run Test`.

**Docs updated as part of the work:** `CHANGELOG.md` (one prose section per change, stating the
trade), `docs/MEMORY_MODEL.md` (a section for the ingest arm; §9.8's sweep marked in place with
what Phases 4/5 changed), `docs/USER_GUIDE.md`, `docs/API_REFERENCE.md`, `docs/benchmarks.md`.
Superseded measurements marked in place, never deleted.

## Sequencing

**Phase 0 + Phase 1 + Phase 3 together** is the first shippable unit: it bounds the inputs,
discovers the ceiling, and makes the working configuration the documented one — which answers
the original question for every shape smaller than the target. Phase 2 follows once 0b says how
large the resident class actually is. Phase 4 next. **Phase 5 is its own release**, scoped once
Phase 0's counter has put a real number on the rewrite cost.
