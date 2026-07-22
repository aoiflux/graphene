# Benchmarks

## How these were produced

|              |                                                                                                                                                                                   |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Date**     | 2026-07-21                                                                                                                                                                        |
| **OS**       | Windows 11 (amd64)                                                                                                                                                                |
| **Go**       | go1.26.2                                                                                                                                                                          |
| **Hardware** | AMD Ryzen 9 5980HS, 16 cores                                                                                                                                                      |
| **Baseline** | git `036aac0`, the commit before this work began                                                                                                                                  |
| **Suites**   | [bench](../graphene_bench_test.go) · [parallel](../graphene_parallel_bench_test.go) · [coverage](../graphene_coverage_bench_test.go) · [footprint](../graphene_footprint_test.go) |
| **Method**   | Baseline and current run **interleaved** — alternating rounds of `-count=2` — then compared with `benchstat`, n=6 per side                                                        |

68 benchmarks, up from the 29 this work started with and the 5 the project had
before that.

### Two rounds discarded, and why the resolution limit is ~25%

The final comparison ran four interleaved rounds after a cooldown. **Round four
was dropped from both sides**, symmetrically: its samples on the new side
roughly doubled (`GetNode` 6.1 ns → 11.5 ns) while rounds one to three matched
the baseline closely. It ran last, after ~50 minutes of continuous benchmarking,
and the new side is the longer of the two because it carries benchmarks the
baseline has no equivalent for — so it absorbed peak thermal load. That is the
machine, not the code, and keeping a round measured in a demonstrably different
state would have inflated every figure in the same direction.

**The two controls then disagreed**, and that sets an honest floor on what this
data can resolve. `GetNode` came out flat (p=0.288) with tight variance, which
is correct — it is byte-identical on both sides. But `PointLookupNode_Memory`,
also byte-identical, reported −24% (p=0.015) off ±25% variance on the base side.
A statistically significant improvement on code that did not change is a false
positive, so:

> **Timing effects below roughly 25% are not resolvable in this dataset.** The
> order-of-magnitude results are safe; anything in the tens of percent should be
> read as directional.

The footprint numbers are exempt. They are deterministic measurements of
resident bytes, reported at ±0% variance, and they do not depend on how warm the
machine was.

### Why interleaving, demonstrated

An earlier attempt ran the two suites back to back and the second came out ~25%
slower across the board — including benchmarks the change never touched. That
was the machine throttling over a long run. Alternating the two sides spreads
that drift evenly, and the controls below confirm it worked.

**Controls** (paths no change touched, expected to read "no significant
change"): `GetNode`, point lookup (memory and disk), `NodesByProperty`,
`PropertyIndexLookup`, 1-hop neighbours, `Connect_EdgeExists`,
`Connect_NeighboursByNodeType`, `Walk_ProvenanceChain`, `Subgraph_Induced`,
`Degree_Typed_Disk`, `Pattern_TwoHop_Scoped`, `ColdOpen_UncompactedWAL`. All
read `~`. Absolute figures come from a warm laptop — read the **ratios** as the
signal.

### On the published 2026-06-01 figures

The README carried a five-row table dated 2026-06-01. Those numbers are **not a
valid baseline** for this work, for two independent reasons.

**The code moved underneath them.** They were published in `8a9a89b`; the
baseline commit is `036aac0`, two commits later, which changed `memory/store.go`
by ~200 lines. Allocation counts are deterministic, so identical code must give
identical counts — and it does not:

| Benchmark     | Published (`8a9a89b`) | Baseline today (`036aac0`) | Verdict          |
| ------------- | --------------------- | -------------------------- | ---------------- |
| BFS           | 3 058 allocs          | 3 058 allocs               | same code path   |
| Shortest path | 2 061 allocs          | 1 563 allocs               | **code changed** |

**And the measurement conditions moved.** BFS is the useful accident here: it
does provably identical work in both, so its timing gap is _pure_ session drift.

| Benchmark             |  Published | Same code, today |    Drift |
| --------------------- | ---------: | ---------------: | -------: |
| BFS traversal         | 475 381 ns |       396 100 ns | **−17%** |
| Get node              |   6.719 ns |         5.838 ns | **−13%** |
| Add node              |   831.5 ns |         748.2 ns | **−10%** |
| Property index lookup |   55.19 ns |         42.23 ns | **−23%** |

So **10–23% of any apparent improvement measured against the published table
would be artefact**, on a path where the code is provably unchanged. Everything
below is measured against `036aac0` run today, interleaved.

### Fixture

100 000 nodes, ~201 000 edges, 300 000 indexed property entries. Labels: `Case`
on every 1000th node (100, highly selective), `EvidenceFile` on every 10th (9
900), `MicroArtefact` on the rest. Properties: `sha256` (unique), `bucket` (1
000 distinct), `score` (1 000 distinct). Topology: chain + `+13` stride, plus a
hub node with 1 000 inbound edges. The disk fixture is `Compact()`ed first.

---

## Results

**Overall geomean: −86.4%.**

### Query planning

| Benchmark                               |   Before |        After |               Change |
| --------------------------------------- | -------: | -----------: | -------------------: |
| Equality property query (disk)          | 53.55 ms | **277.0 ns** | −100.00% (~193 000×) |
| Equality property query (memory)        | 44.00 ms | **320.1 ns** | −100.00% (~137 000×) |
| Type + equality property query (memory) | 58.71 ms | **7.171 µs** |    −99.99% (~8 200×) |
| Typed query, `Limit: 10` (memory)       | 13.76 ms | **12.36 µs** |              −99.91% |
| Typed query, `Limit: 10` (disk)         | 17.72 ms | **22.29 µs** |              −99.87% |
| Edge query by type (memory)             | 56.35 ms | **155.3 µs** |              −99.72% |
| Relation query, both directions (disk)  | 80.81 ms | **256.8 µs** |              −99.68% |
| Anchored relation query (disk)          | 43.70 ms | **228.7 µs** |              −99.48% |
| Edge query by type (disk)               | 40.33 ms | **264.8 µs** |              −99.34% |
| Anchored relation query (memory)        | 23.78 ms | **176.0 µs** |              −99.26% |
| `NodesByType`, selective label (disk)   | 445.7 µs | **5.059 µs** |              −98.87% |
| Prefix property query (memory)          | 48.34 ms | **16.26 ms** |              −66.36% |
| Range property query (disk)             | 71.72 ms | **28.86 ms** |              −59.75% |
| Range property query (memory)           | 63.00 ms | **28.80 ms** |              −54.28% |
| `NodesByType`, selective label (memory) | 242.5 ns |     225.4 ns |  ~ (already indexed) |

The last row is a useful negative: the memory backend already served selective
labels from postings before this work, so there was nothing to win. The disk
backend was scanning the CSR, which is the 88× difference between the two rows.

The prefix and range rows are the ones still measured in **milliseconds**. They
are scans unless the key is declared ordered — see the ordered-index section
below, where the same queries drop to microseconds.

### Residual filters — the second filter used to cost a scan

Driving a query from the most selective index is only half the job. The filters
that did _not_ drive it still have to be applied, and resolving each to its own
set means a filter no index can serve — a `Contains`, or a range on a key never
declared ordered — scans every entry under its key. A query driven down to a
single candidate was still doing work proportional to the whole graph.

Costing each residual both ways and probing the candidates when that is cheaper:

| Benchmark                        |   Before |        After |               Change |
| -------------------------------- | -------: | -----------: | -------------------: |
| Equality + `Contains` (memory)   | 12.97 ms | **443.1 ns** |  −100.00% (~29 000×) |
| Equality + `Contains` (disk)     | 12.94 ms | **429.2 ns** |  −100.00% (~30 000×) |
| — allocated bytes                |  4.10 MB |    **304 B** |              −99.99% |
| Two equality filters (memory)    | 717.1 ns | **567.9 ns** | −20.8% (allocs −64%) |
| Single equality filter (memory)  | 324.9 ns | **254.8 ns** |               −21.6% |
| Single equality filter (disk)    | 308.6 ns | **243.2 ns** |               −21.2% |
| _Control:_ point lookup (memory) | 23.42 ns |     23.23 ns |          ~ (p=0.198) |

The single-filter rows were not the target. The driving filter used to be
re-resolved to its own set and intersected against the candidates it had itself
produced; skipping it is where that 21% comes from.

**This first shipped as a regression, in the same run that produced the 29
000×.** Single-filter queries measured +14% (memory) and +23% (disk) with +70%
allocations, because they still built a residual plan — allocating a slice and
consulting the index to establish there was nothing to do. A short-circuit for
"the driver consumed every filter" fixed it and turned the row into the win
above. It is recorded here because the headline number and the regression came
out of the same benchmark run, and only a set wide enough to include the boring
case would have caught it.

#### The edge path, and a run where the control failed

The same evaluation on edge queries, measured separately because the edge half
had been written and left unreachable — only the node path was wired into the
stores, and a method nobody calls draws no complaint from the compiler,
`go vet`, or any test:

| Benchmark                           |   Before |        After |            Change |
| ----------------------------------- | -------: | -----------: | ----------------: |
| Edge equality + `Contains` (memory) | 2.357 ms | **575.5 ns** | −99.98% (~4 100×) |

**That run's control moved**, and it is worth saying so rather than quoting the
row alone. `PointLookupNode_Memory` shifted +23.7% (p=0.001) with ±17–31%
variance, because the race and stress suites had finished moments earlier and
the machine was still hot. Two conclusions follow, and they differ:

- the edge figure is four orders of magnitude, so no plausible drift touches it;
- the smaller readings in the same run — notably +14% on an unrelated node query
  — are **drift, not signal**. The control moved further, in the same direction,
  on a path the change cannot affect.

The final full-suite comparison waits out a cooldown before it starts, for
exactly this reason.

### Read consistency — what it costs to resolve postings against the records

`NodesByProperty` and `EdgesByProperty` went straight to the property index,
which is a separate structure under a separate lock from the records. A lookup
could therefore read postings that a concurrent `DeleteNode` had not reached yet
and return an entity the records no longer had. Resolving postings against the
records before returning makes the records the authority:

| Benchmark                        | Unfiltered |     Filtered |           Change |
| -------------------------------- | ---------: | -----------: | ---------------: |
| Raw single-key lookup (memory)   |   70.60 ns | **91.10 ns** | +29.0% (p=0.000) |
| Typed equality query (memory)    |   380.6 ns |     406.9 ns |      ~ (p=0.089) |
| Typed equality query (disk)      |   420.8 ns |     352.9 ns |                ~ |
| _Control:_ point lookup (memory) |   28.73 ns |     31.05 ns |      ~ (p=0.713) |

Twenty nanoseconds on the raw lookup, nothing measurable on the typed query path
— which already resolved its candidates that way — and no change in allocations,
since the filter runs in place over a slice the index had already copied. That
buys a guarantee that can be stated in a sentence: _every ID returned named an
entity that was live at the moment it was checked._

### Scale sweep — does cost track the answer, or the graph?

The clearest evidence the queries are index-served rather than scanning. Same
operation, 10× the data:

| Benchmark                        |        10k nodes |       100k nodes | Slope                 |
| -------------------------------- | ---------------: | ---------------: | --------------------- |
| Equality query — **before**      |         2.907 ms |        41.375 ms | **14×** (linear scan) |
| Equality query — **after**       |         704.5 ns |     **590.5 ns** | **flat**              |
| Type query, `Limit: 10` — before |         517.9 µs |         9 487 µs | 18×                   |
| Type query, `Limit: 10` — after  |         1.106 µs |         11.92 µs | 10.8× ¹               |
| Point lookup — before / after    | 18.25 / 17.57 ns | 25.62 / 25.79 ns | flat both             |

¹ Still climbs, and correctly so: the `Case` label has 10× more members at 100k,
so the result itself is 10× bigger. Cost tracks the answer, which is the goal.

### Label index

| Benchmark                               |   Before |        After |  Change |
| --------------------------------------- | -------: | -----------: | ------: |
| `NodesByType`, selective label (disk)   | 400.9 µs | **4.900 µs** | −98.78% |
| `NodesByType`, selective label (memory) | 220.4 ns |     216.5 ns |       ~ |

### Ordered (range) index

| Benchmark             |   Before |        After |  Change |
| --------------------- | -------: | -----------: | ------: |
| Prefix query (memory) | 45.67 ms | **7.656 ms** | −83.24% |
| Range query (memory)  | 60.39 ms | **20.00 ms** | −66.88% |
| Range query (disk)    | 66.72 ms | **29.11 ms** | −56.37% |

Those are the _undeclared_ figures — that improvement comes from confining the
scan to one key's buckets. With the key declared ordered, measured separately
against the same data:

| Query                                |     Scan |      Ordered |   Change |
| ------------------------------------ | -------: | -----------: | -------: |
| Range over 1 000 values (memory)     | 17.06 ms | **2.500 ms** |     6.8× |
| Range over 1 000 values (disk)       | 22.84 ms | **2.310 ms** |     9.9× |
| Prefix over 1 000 values             | 6.129 ms | **2.495 ms** |     2.5× |
| **Narrow range — 3 of 1 000 values** | 11.76 ms | **59.01 µs** | **199×** |

The narrow-range row is the one that matters: a scan costs the same however
selective the predicate is, while an ordered index costs what the answer costs.
Building the index over an already-populated store is a one-off 1.87 ms per key
at 20 000 nodes.

### Degree and mutation

| Benchmark                                    |   Before |        After |            Change |
| -------------------------------------------- | -------: | -----------: | ----------------: |
| Hub in-degree (disk)                         | 70.95 µs | **14.22 ns** | −99.98% (~4 990×) |
| Hub in-degree (memory)                       | 18.89 µs | **27.57 ns** |           −99.85% |
| `DeleteNode` with a populated property index | 1.201 ms | **1.589 µs** |   −99.87% (~756×) |
| `UpdateNode` relabelling within a 50k label  | 50.43 µs | **4.435 µs** |           −91.20% |
| `DeleteNode` from a 50k-member label         | 27.98 µs | **3.732 µs** |           −86.66% |
| `DeleteNode` from a 10k-member label         | 4.139 µs | **787.4 ns** |           −80.98% |

### Durability

| Benchmark                            |     Before |        After |  Change |
| ------------------------------------ | ---------: | -----------: | ------: |
| Reopen a compacted store (50k nodes) | 1 176.7 ms | **60.58 ms** | −94.85% |
| Compaction, steady state             |   652.8 ms | **50.70 ms** | −92.23% |
| End-to-end disk ingest, 10k nodes    |   556.1 ms | **433.5 ms** | −22.05% |

The property index now lives in the CSR file, so `Compact()` leaves the WAL
**empty** and restart cost no longer grows with index size.

### Traversal — allocation is the metric

| Benchmark                        |        Before |   After |  Change |
| -------------------------------- | ------------: | ------: | ------: |
| `BFS_Deep` — 10k-node chain      | 30 190 allocs | **216** | −99.28% |
| `Walk_DFS_Deep` — 10k-node chain | 20 193 allocs | **196** | −99.03% |
| `Walk_ShortestPath_Disk`         |  7 670 allocs | **138** | −98.20% |
| `BFS` — 1k-node chain            |  3 058 allocs |  **77** | −97.48% |
| `BFS_Wide` — 100×100 fan-out     |  1 323 allocs | **232** | −82.46% |
| `ShortestPath`                   |  1 563 allocs | **576** | −63.15% |
| `BFS_Disk_Deep`                  |    913 allocs | **394** | −56.85% |
| `Scale_BFS4Hop_100k`             |     47 allocs |  **30** | −36.17% |
| `BFS3Hop_Disk`                   |     70 allocs |  **47** | −32.86% |

Wall-clock followed: `Connect_IsConnected_Near` **−70.9%** (a bidirectional
search that now materialises records only for the final path), `BFS3Hop_Disk`
−18.2%, `BFS_Disk_Deep` −15.4%, `ShortestPath` −13.0%, `BFS` −10.8%, `BFS_Wide`
−7.0%.

`BFSIDs` walks without building a single record — no baseline, the API is new:
20 allocations and 36.3 µs on the disk fixture, against 394 and 113.9 µs for the
record-returning equivalent.

### Filter scans — comparing once per value, sorting without reflection

Two independent findings, both from re-profiling rather than from a plan item.

**The predicate ran once per entry, not once per distinct value.** A filter
reads only the value, so a value shared by a thousand entities was evaluated a
thousand times for the same answer. Interleaved, nine controls `~`:

|                                    |   Before |    After |     Change |
| ---------------------------------- | -------: | -------: | ---------: |
| `QueryNodes_PropertyRange_Disk`    | 27.45 ms | 11.74 ms | **−57.2%** |
| `QueryNodes_PropertyRange_Memory`  | 32.01 ms | 15.24 ms | **−52.4%** |
| `..._Range_Narrow_Scan_Memory`     | 26.16 ms | 13.99 ms |     −46.5% |
| `QueryNodes_PropertyPrefix_Memory` | 17.04 ms | 14.64 ms |     −14.0% |

~24% of the range query had been `strconv` float parsing, from the numeric-then-
bytes scan rule. It vanished without being touched: fewer comparisons, less
parsing.

**Then the sort dominated, and it was reflective.** `sort.Slice` with a closure
shows up as `sort.partition_func` + `reflectlite.Swapper`. Switching the five
ascending-ID sorts to `slices.Sort` (measured with the change above present on
both sides):

|                                   |   Before |       After |     Change |
| --------------------------------- | -------: | ----------: | ---------: |
| `QueryNodes_PropertyRange_Memory` | 15.33 ms | **9.36 ms** | **−38.9%** |
| `..._Range_Narrow_Scan_Memory`    | 13.82 ms | **8.63 ms** |     −37.5% |
| `QueryNodes_PropertyRange_Disk`   | 13.09 ms |    11.70 ms |          ~ |

Disk is `~` because its scan is dominated by materialising records rather than
sorting. The range scan overall moved from ~32 ms to ~9.4 ms; the two figures
are from separate controlled runs and are not multiplied.

### Property-index memory — interning only where it pays

A reverse index entry kept the caller's own string. The forward index
deduplicates by content, so a thousand nodes sharing one value left one string
on the forward side and a thousand copies pinned on the reverse side — ~32 B per
entry.

Interning unconditionally would lose on a unique-per-node key like a hash, which
would pay a table slot per value and save nothing. So a value is interned only
on its **second** entry, which `insertSorted` has already computed.

Footprint benchmarks (deterministic, ±0%; `NoIndex` control unchanged):

| 100k entries       | index B/node before |     after |     Change |
| ------------------ | ------------------: | --------: | ---------: |
| cardinality 1      |                93.3 |  **61.3** | **−34.3%** |
| cardinality 100    |                94.8 |  **62.9** |     −33.6% |
| cardinality 10 000 |               105.1 |  **89.3** |     −15.0% |
| all distinct       |               163.4 | **163.4** |   **0.0%** |

The last row is the design goal: where interning cannot pay, it costs nothing.
Whole-graph fixtures move −2.5% to −3.4%, and speed is `~` across read and
write.

### Cold open — replay was syscall-bound, not index-bound

`Open` on an uncompacted WAL replayed every record with three separate reads
straight to the file handle, so a 60 000-record log cost ~180 000 syscalls. A
CPU profile put **69% of the time in `syscall.readFile`**; index maintenance,
which the plan had assumed was the cost, did not appear in the top twenty.

Interleaved, 4 rounds, memory-backend controls all `~`:

| `ColdOpen_UncompactedWAL_10k` |     Before |       After |            Change |
| ----------------------------- | ---------: | ----------: | ----------------: |
| time                          | 1 263.9 ms | **42.7 ms** | **−96.6%** (~30×) |
| allocs/op                     |    285 100 |     285 000 |                 ~ |

Two fixes, the second only visible after the first:

1. **Buffered replay** — a `bufio.Reader` over the log. Safe because the WAL is
   `O_APPEND`, so reading past a record cannot disturb a later write. 1 263.9 ms
   → ~220 ms.
2. **`crc32.ChecksumIEEE`** — the checksum was a bit-by-bit loop, 8 iterations
   per byte, and became 46% of the time once syscalls were gone. Same
   polynomial, same values, hardware-accelerated. ~220 ms → 42.7 ms.

Allocations are unchanged, which is the point: no allocation figure could have
found either of these.

> The buffer is capped to the log's size. A fixed 1 MiB buffer initially showed
> as +1 MiB on every open, including compacted stores that read almost nothing.
>
> Also not claimed: the other reopen benchmarks trended ~25% faster in this run,
> and so did every untouched control. That is drift. Only the 30× clears it.

### Pattern matching — the worst path in the codebase, fixed

`FindPatterns` over a 2 000-node scope was 24.6 ms and **399 950 allocations**,
roughly 200 per scoped node. Interleaved, 4 rounds, four controls all `~`:

| `Pattern_TwoHop_Scoped` |    Before |        After |      Change |
| ----------------------- | --------: | -----------: | ----------: |
| time                    | 24.633 ms | **4.749 ms** |  **−80.7%** |
| B/op                    | 3 691 KiB |      839 KiB |      −77.3% |
| allocs/op               |   399 950 |      **150** | **−99.96%** |

Two independent changes, in the order they were found:

1. **Stop materialising discarded records** — the edge check called `EdgesOf`,
   building a record for every outbound edge just to compare one field. It now
   walks adjacency IDs and materialises only an edge that already matched on
   endpoint. Worth −99.96% allocations and −28.8% time.
2. **Memoise the adjacency walk** — backtracking holds the source fixed while
   iterating candidates, so the same adjacency was re-walked (and re-locked)
   once per candidate. A one-entry memo took the remainder: 17.2 ms → 4.75 ms.

The second is the interesting one: after step 1 the benchmark still spent 17 ms
doing **150 allocations**, so no allocation figure could have pointed at it. The
larger half of the win was repeated work that allocated nothing, and it only
became visible once the allocations were gone.

### Driver selection — a selective label no longer loses to a weak filter

The disk planner took any equality driver unconditionally. A query naming a
100-node label alongside a 25 000-hit property filter drove from the filter,
materialised 25 000 candidates, and discarded 24 900. Label posting sizes are
known in O(1), so the two are now costed on the same scale — as the in-memory
backend already did.

| `Planner_SelectiveLabelWeakFilter_Disk` |    Before |        After |     Change |
| --------------------------------------- | --------: | -----------: | ---------: |
| time                                    |  1 698 µs |  **28.9 µs** | **−98.3%** |
| B/op                                    | 1 963 KiB | **19.0 KiB** |     −99.0% |
| allocs/op                               |    25 005 |      **226** |     −99.1% |

The shapes that gain nothing from the change also lose nothing to it —
`Planner_SelectiveFilterWeakLabel_Disk` (equality was already correct),
`Planner_LabelOnly_Disk`, `QueryNodes_TypeLimit10_Disk`: all `~`, with identical
`B/op` and `allocs/op` despite the added cardinality lookup on every typed
query.

> Caveat on this run: one control (`QueryNodes_TypeLimit10_Memory`, untouched
> code) moved −4.34%, with all controls drifting slightly in the new side's
> favour. Treat anything under ~5% from this run as noise. The 58× headline and
> the identical allocation counts are unaffected.

This is the worst case for the old planner, not a typical query: the size of the
win is set by how badly label and filter selectivity are mismatched.

### Property blobs — reads no longer pay for blob size

The disk backend used to copy a record's property blob on every read. It now
hands back the record's own slice, which the API contract has always permitted
(`Labels` already did this). Ingest still copies what the caller passes in.

Interleaved, 4 rounds, memory backend as control — every control `~`:

| Disk read (blob size)      |   32 B |  128 B |  512 B |
| -------------------------- | -----: | -----: | -----: |
| Point lookup               | −30.0% | −42.4% | −70.1% |
| `EdgesOf`, 4 edges         | −16.5% | −22.5% | −40.8% |
| Bulk `GetNodes`, 10k nodes | −34.4% | −47.8% | −76.8% |

Geomean **−26.6%** sec/op, **−36.9%** B/op, **−25.8%** allocs/op, all at
p=0.029.

The absolute figures show the point better than the percentages:

| Disk read            |    32 B |   128 B |       512 B |                  |
| -------------------- | ------: | ------: | ----------: | ---------------- |
| Point lookup, before | 68.4 ns | 81.1 ns |    151.0 ns | scales with blob |
| Point lookup, after  | 47.8 ns | 46.7 ns | **45.1 ns** | flat             |
| Bulk 10k, before     |  705 µs |  889 µs |    2 047 µs | scales with blob |
| Bulk 10k, after      |  462 µs |  464 µs |  **475 µs** | flat             |

Blob size stopped being a cost dimension on the read path. The gain therefore
grows without bound in blob size — at 512 B a bulk read is 4.3× faster, and the
allocation drops from 5 785 KiB to 785 KiB because the copies are simply gone.

> These are new benchmarks (`graphene_blob_bench_test.go`), not a re-run of
> existing ones. Until they existed no benchmark had ever stored a property
> blob, so this copy cost was invisible to the whole suite. That is written up
> in [CONTRIBUTING.md](../CONTRIBUTING.md) §1 as a measurement lesson.

### Memory footprint (P1)

`B/op` is bytes _allocated during_ an operation, which says nothing about what a
loaded graph _occupies_. These build the graph, GC twice, then read `HeapAlloc`.

100 000 nodes, ~200 000 edges:

| Configuration                            | Bytes/node | Bytes/edge |            Total |
| ---------------------------------------- | ---------: | ---------: | ---------------: |
| In-memory, topology only                 |      447 B |      223 B |         42.6 MiB |
| On-disk (CSR), topology only             |      298 B |      149 B |         28.4 MiB |
| In-memory + property index (3 keys/node) |      767 B |      384 B |         73.2 MiB |
| On-disk + property index                 |      620 B |      310 B |         59.2 MiB |
| + one ordered key declared               |      778 B |      389 B |         74.2 MiB |
| **On-disk file**                         |      223 B |      111 B | 21.3 MiB (WAL 0) |

- **The property index costs ~107 bytes per entry** — a lot for an 8-byte ID and
  a short value, and the strongest lead for Phase 8: the sorted postings, the
  reverse `ID → [(key,value)]` map, and the value strings each hold a copy.
- **An ordered key costs 10.5 B/node**, not the "~2× that key's index" an
  earlier note claimed. That claim was wrong: cost scales with _distinct
  values_, not entries, and `score` has 1 000 distinct values across 100 000
  nodes.
- **The CSR is ~33% more compact in RAM** than the in-memory backend, and its
  on-disk form smaller again.

Deletions the CSR has not reclaimed — its arrays are sized by the highest ID
ever issued, not the live count:

| State                     | Bytes per live node |    Total |
| ------------------------- | ------------------: | -------: |
| Half deleted, uncompacted |               715 B | 34.1 MiB |
| Half deleted, compacted   |               158 B |  7.5 MiB |

**4.5× the memory per live node** until `Compact()` runs. Documented behaviour,
never before quantified.

---

## Regressions

Reported in full. Each is a trade, and the axis that won is named.

### The one that matters most: property-index memory, +19–48%

This is a **P1 regression**, it is not noise, and it is the price of the P0 wins
above.

| Footprint (B/node)            | Before |     After |       Change |
| ----------------------------- | -----: | --------: | -----------: |
| _Topology only, memory_       |  446.1 |     446.2 |   **+0.02%** |
| _No property index_           |  170.1 |     170.2 |   **+0.06%** |
| Memory store + property index |  563.8 | **723.6** |       +28.3% |
| Disk store + property index   |  388.4 | **575.7** |       +48.2% |
| Index at cardinality 1        |  179.0 | **231.6** |       +29.4% |
| Index at cardinality 100      |  180.5 |     233.2 |       +29.2% |
| Index at cardinality 10 000   |  194.0 |     259.6 |       +33.8% |
| Index, all values distinct    |  281.1 |     333.7 |       +18.7% |
| On-disk file size             |  248.0 | **175.0** | **−29.4%** ¹ |

The first two rows are controls, and they are what make the rest interpretable:
both sit at ~0%, so **the entire increase lives in the property index**, not in
the graph structures. These figures carry ±0% variance because they are
deterministic memory measurements rather than timings, so unlike the latency
numbers they are unaffected by the thermal caveats in the methodology section.

Three things drive it, in descending order:

1. **The reverse `ID → (key, value)` map.** This is what makes `RemoveNode`
   proportional to an entity's own entries instead of to the whole index, and it
   is what bought `DeleteNode` its −99.85%. It costs one map entry and one slice
   header per indexed entity.
2. **Sharding sixteen ways.** Each shard carries its own maps, so the fixed map
   overhead is paid sixteen times over. That is what bought concurrent
   distinct-key registration its −57.85%.
3. **The per-key entry counter**, added so the query planner can cost a scan of
   a key without walking its buckets.

The pattern across cardinalities is the tell: the overhead is worst where values
are _shared_ (+65% at cardinality 1) and mildest where every value is distinct
(+30%). At low cardinality the forward map is tiny while the reverse map still
holds one entry per entity, so the reverse map dominates the ratio.

**Partly recovered, twice.** Two changes have taken **32 B/node** off the
three-key fixture between them, both confirmed by the same signature: a constant
per-entry saving that reproduces across every cardinality and multiplies by the
key count.

**One — the reverse map is split by arity.** It was `map[T][]propRef`,
allocating a one-element backing array per entity on top of the map entry — and
sharding by key had quietly made one entry per shard the universal case, since
each key lives in exactly one shard. The single case is now stored inline, and
only entities with two keys hashing to the _same_ shard spill into a slice.

That recovered **21.5 B per (entity, key) entry**, which is where the numbers
above come from. The three-key fixture saved 65.9 B ≈ 3 × 21.5, the mechanism
confirming itself rather than a number that merely moved the right way; the
no-index control was unchanged, so the saving is attributable. No speed
regression was detected on the delete, registration, lookup or query paths.

**Two — the key in `propRef` is interned.** A shard sees a handful of keys but
holds a `propRef` per _(entity, key)_, so a 16-byte string header was describing
one of a handful of strings, hundreds of thousands of times. Now a `uint32`:
**−10.5 B per entry**, on every cardinality, with the no-index control
unchanged.

**What is left, and why it is not being taken.** Interning the _value_ too would
shrink `propRef` to 8 bytes — roughly −31 B/entry — but the value table costs
~51 B per _distinct_ value, so it breaks even at ~1.65 entities per value: a win
on low-cardinality keys, a loss on unique ones like `sha256`, ~5–6% on a
realistic mix. And every value comparison on the residual probe path would gain
a table indirection. That is P0 spent for 5% of P1.

Compressing the postings lists is the other obvious idea and also wrong: they
are 8 B of a ~93 B floor.

### Latency and allocation

| Benchmark                                       |    Before |     After |     Change |
| ----------------------------------------------- | --------: | --------: | ---------: |
| `NodesByProperty_Equal_Memory`                  |  49.15 ns |  78.31 ns | **+59.3%** |
| `PropertyIndexLookup`                           |  46.65 ns |  71.18 ns | **+52.6%** |
| `Parallel_IndexNodeProperty_DistinctKeys` bytes |   217.5 B |   427.0 B | **+96.3%** |
| `Parallel_IndexNodeProperty_SameKey` bytes      |   199.5 B |   376.5 B | **+88.7%** |
| `ReopenCompactedStore` bytes                    | 33.89 MiB | 55.29 MiB | **+63.1%** |

**The raw single-key lookup, +52–59%.** Two costs, both deliberate. The larger
is read consistency: postings are now resolved against the records before being
returned, because the index and the records are separate structures under
separate locks and a lookup consulting only the index could hand back an entity
a concurrent delete had already removed. An isolated A/B put that at +29% on its
own (70.6 → 91.1 ns). The remainder is sharding, which added a hash per lookup
to buy the concurrency win. The absolute figure is ~78 ns against a path that
used to be a 40 ms scan.

**Property-index registration allocates roughly twice as much.** The reverse map
entry per registration. This is the same purchase as the memory row above, seen
on the write path.

**Reopen allocates 63% more** while running 94% faster, because the index now
arrives as one file read rather than streaming from the log. Peak memory during
open is the cost; restart latency is what it bought.

### Previously reported regressions, now fixed

Recorded because they were published as regressions and should not stay that
way:

| Benchmark                                 |    Was |                              Now |
| ----------------------------------------- | -----: | -------------------------------: |
| `Parallel_BFS3Hop_Disk`                   | +35.1% | **−34.8%** (lock-free CSR reads) |
| `Parallel_IndexNodeProperty_DistinctKeys` | +18.5% |        **−57.9%** (key sharding) |
| `BFS_Deep` bytes                          | +13.0% |      **−18.1%** (two-buffer BFS) |
| `Ingest_AddNodes_Batch1000`               | +10.2% |                  not significant |

---

## What is still slow

- **`Contains` filters are a scan and will stay one.** No ordering can bound a
  substring match; a trigram index is the only route and is out of scope.
- **Range and prefix are only fast on _declared_ keys.** An undeclared key still
  scans its buckets — better than the original full-index materialisation, but
  linear.
- **Property-index memory** is now Go map machinery. The reverse map is 90% of
  the index; the pinned value strings inside it were removed by adaptive
  interning (below), and the sorted-array replacement that would remove the rest
  was reverted for making deletes 5.7× slower. See
  [TECHNICAL_DETAILS.md](TECHNICAL_DETAILS.md) §14.8.

## Reproducing

```powershell
./test.ps1 -Bench                                    # default 5s benchtime
go test . -tags=stress -bench=. -benchmem -count=6 -run='^$'
go test . -tags=stress -bench=Footprint -benchtime=1x -run='^$'
go test . -tags=stress -bench=Parallel -cpu=1,2,4,8,16 -run='^$'
```

Sweep the fixture size with `GRAPHENE_BENCH_NODES` (default 100 000).

---

## Appendix: every benchmark, both sides

Generated from the final interleaved A/B (`036aac0` vs current, three usable
rounds). `~` means benchstat found no significant difference. Read
[the resolution caveat](#two-rounds-discarded-and-why-the-resolution-limit-is-25)
before drawing conclusions from anything under 25%.

### Time (sec/op)

| Benchmark                                 |    Baseline | Current |   Change |
| ----------------------------------------- | ----------: | ------: | -------: |
| `PointLookupNode_Memory`                  |      34.96n |  26.48n |  -24.26% |
| `PointLookupNode_Disk`                    |      54.38n |  47.42n |        ~ |
| `NodesByType_Selective_Memory`            |      242.5n |  225.4n |        ~ |
| `NodesByType_Selective_Disk`              |    445.731µ |  5.059µ |  -98.87% |
| `QueryNodes_TypeLimit10_Memory`           |   13755.24µ |  12.36µ |  -99.91% |
| `QueryNodes_TypeLimit10_Disk`             |   17718.67µ |  22.29µ |  -99.87% |
| `NodesByProperty_Equal_Memory`            |      49.15n |  78.31n |  +59.34% |
| `QueryNodes_PropertyEqual_Memory`         | 43995135.5n |  320.1n | -100.00% |
| `QueryNodes_PropertyEqual_Disk`           | 53546608.5n |  277.0n | -100.00% |
| `QueryNodes_TypeAndPropertyEqual_Memory`  |  58712.784µ |  7.171µ |  -99.99% |
| `QueryNodes_PropertyPrefix_Memory`        |      48.34m |  16.26m |  -66.36% |
| `QueryNodes_PropertyRange_Memory`         |      63.00m |  28.80m |  -54.28% |
| `QueryNodes_PropertyRange_Disk`           |      71.72m |  28.86m |  -59.75% |
| `QueryRelations_Anchored_Memory`          |    23784.3µ |  176.0µ |  -99.26% |
| `QueryRelations_Anchored_Disk`            |    43701.9µ |  228.7µ |  -99.48% |
| `Neighbours1Hop_Memory`                   |      205.8n |  174.4n |  -15.26% |
| `Neighbours1Hop_Disk`                     |      466.4n |  414.3n |        ~ |
| `BFS3Hop_Memory`                          |      3.494µ |  2.628µ |  -24.79% |
| `BFS3Hop_Disk`                            |      5.239µ |  3.770µ |  -28.04% |
| `Degree_Hub_Memory`                       |   22289.00n |  27.54n |  -99.88% |
| `Degree_Hub_Disk`                         |   74334.00n |  15.18n |  -99.98% |
| `DeleteNode_WithPropertyIndex`            |   1423.053µ |  2.075µ |  -99.85% |
| `BFS_Deep`                                |      4.961m |  3.864m |  -22.12% |
| `BFS_Wide`                                |      3.500m |  2.977m |        ~ |
| `BFS_Disk_Deep`                           |      84.27µ |  66.78µ |  -20.75% |
| `ReopenCompactedStore`                    |    1283.50m |  73.60m |  -94.27% |
| `CompactSteadyState`                      |     648.81m |  64.01m |  -90.13% |
| `DeleteNode_HotLabel_10k`                 |     3690.0n |  977.0n |  -73.52% |
| `DeleteNode_HotLabel_50k`                 |     31.309µ |  4.014µ |  -87.18% |
| `UpdateNode_HotLabel_50k`                 |     49.157µ |  5.162µ |  -89.50% |
| `IndexNodeProperty`                       |      896.3n |  898.9n |        ~ |
| `Ingest_AddNode_Single`                   |      657.6n |  699.9n |        ~ |
| `Ingest_AddNodes_Batch100`                |      66.11µ |  69.48µ |        ~ |
| `Ingest_AddNodes_Batch1000`               |      679.7µ |  663.9µ |        ~ |
| `Ingest_AddEdge_Single`                   |      394.7n |  420.1n |        ~ |
| `Ingest_EndToEnd_Disk_10k`                |      588.3m |  448.8m |  -23.71% |
| `Ingest_AddNode_Disk`                     |      6.226µ |  6.231µ |        ~ |
| `Scale_PointLookup_10k`                   |      18.18n |  17.50n |        ~ |
| `Scale_PointLookup_100k`                  |      26.91n |  31.55n |        ~ |
| `Scale_EqualityQuery_10k`                 |  3296996.0n |  327.2n |  -99.99% |
| `Scale_EqualityQuery_100k`                | 44828172.0n |  285.9n | -100.00% |
| `Scale_TypeQuery_10k`                     |    550.956µ |  1.201µ |  -99.78% |
| `Scale_TypeQuery_100k`                    |   13240.05µ |  12.71µ |  -99.90% |
| `Scale_BFS4Hop_10k`                       |      6.361µ |  6.136µ |        ~ |
| `Scale_BFS4Hop_100k`                      |      5.509µ |  5.704µ |        ~ |
| `Walk_DFS_Deep`                           |      5.108m |  4.410m |  -13.66% |
| `Walk_ProvenanceChain`                    |      15.94µ |  15.82µ |        ~ |
| `Walk_ShortestPath_Disk`                  |     1606.4µ |  832.7µ |        ~ |
| `Pattern_TwoHop_Scoped`                   |      25.58m |  23.55m |        ~ |
| `Subgraph_Induced_1k`                     |      503.9µ |  440.6µ |  -12.55% |
| `Connect_EdgeExists`                      |      420.7n |  281.7n |        ~ |
| `Connect_IsConnected_Near`                |     157.69µ |  53.99µ |  -65.76% |
| `Connect_NeighboursByNodeType`            |      700.5n |  568.5n |  -18.84% |
| `QueryEdges_ByType_Memory`                |    56350.7µ |  155.3µ |  -99.72% |
| `QueryEdges_ByType_Disk`                  |    40333.2µ |  264.8µ |  -99.34% |
| `QueryRelations_Both_Disk`                |    80807.5µ |  256.8µ |  -99.68% |
| `Degree_Typed_Disk`                       |     79.430µ |  7.405µ |  -90.68% |
| `ColdOpen_UncompactedWAL_10k`             |      807.8m |  735.8m |   -8.91% |
| `Parallel_PointLookup_Memory`             |      48.48n |  47.98n |   -1.03% |
| `Parallel_PointLookup_Disk`               |      46.09n |  12.29n |  -73.34% |
| `Parallel_PropertyEqual_Memory`           |      44.87n |  37.98n |  -15.38% |
| `Parallel_Neighbours_Memory`              |      113.8n |  113.7n |        ~ |
| `Parallel_BFS3Hop_Disk`                   |     1163.5n |  758.7n |  -34.80% |
| `Parallel_AddNode_Memory`                 |      815.4n |  757.0n |        ~ |
| `Parallel_IndexNodeProperty_SameKey`      |      868.6n |  922.8n |        ~ |
| `Parallel_IndexNodeProperty_DistinctKeys` |     1013.6n |  427.2n |  -57.85% |
| `Parallel_MixedReadWrite_Memory`          |      540.9n |  608.0n |        ~ |
| `AddNode`                                 |      696.7n |  757.2n |        ~ |
| `GetNode`                                 |      6.008n |  6.196n |        ~ |
| `BFS`                                     |      426.6µ |  332.6µ |  -22.04% |
| `ShortestPath`                            |      242.6µ |  202.5µ |  -16.55% |
| `PropertyIndexLookup`                     |      46.65n |  71.18n |  +52.58% |

### Bytes allocated (B/op)

| Benchmark                                 |    Baseline | Current |   Change |
| ----------------------------------------- | ----------: | ------: | -------: |
| `PointLookupNode_Memory`                  |       0.000 |   0.000 |        ~ |
| `PointLookupNode_Disk`                    |       64.00 |   64.00 |        ~ |
| `NodesByType_Selective_Memory`            |       896.0 |   896.0 |        ~ |
| `NodesByType_Selective_Disk`              |    12.227Ki | 4.039Ki |  -66.96% |
| `QueryNodes_TypeLimit10_Memory`           |  1568.133Ki | 7.242Ki |  -99.54% |
| `QueryNodes_TypeLimit10_Disk`             |  15657.87Ki | 17.59Ki |  -99.89% |
| `NodesByProperty_Equal_Memory`            |       8.000 |   8.000 |        ~ |
| `QueryNodes_PropertyEqual_Memory`         |  92940286.0 |   176.0 | -100.00% |
| `QueryNodes_PropertyEqual_Disk`           | 100968176.0 |   176.0 | -100.00% |
| `QueryNodes_TypeAndPropertyEqual_Memory`  | 91476.466Ki | 2.781Ki | -100.00% |
| `QueryNodes_PropertyPrefix_Memory`        |    89.841Mi | 1.185Mi |  -98.68% |
| `QueryNodes_PropertyRange_Memory`         |    89.841Mi | 1.185Mi |  -98.68% |
| `QueryNodes_PropertyRange_Disk`           |    97.497Mi | 8.841Mi |  -90.93% |
| `QueryRelations_Anchored_Memory`          |    3160.1Ki | 113.2Ki |  -96.42% |
| `QueryRelations_Anchored_Disk`            |   34697.1Ki | 191.5Ki |  -99.45% |
| `Neighbours1Hop_Memory`                   |       48.00 |   48.00 |        ~ |
| `Neighbours1Hop_Disk`                     |       504.0 |   504.0 |        ~ |
| `BFS3Hop_Memory`                          |     1.703Ki | 1.453Ki |  -14.68% |
| `BFS3Hop_Disk`                            |     4.438Ki | 3.016Ki |  -32.04% |
| `Degree_Hub_Memory`                       |     8.000Ki | 0.000Ki | -100.00% |
| `Degree_Hub_Disk`                         |     175.4Ki |   0.0Ki | -100.00% |
| `DeleteNode_WithPropertyIndex`            |       0.000 |   0.000 |        ~ |
| `BFS_Deep`                                |     2.102Mi | 1.720Mi |  -18.14% |
| `BFS_Wide`                                |     3.062Mi | 2.071Mi |  -32.35% |
| `BFS_Disk_Deep`                           |     81.81Ki | 50.75Ki |  -37.97% |
| `ReopenCompactedStore`                    |     33.89Mi | 55.29Mi |  +63.13% |
| `CompactSteadyState`                      |     88.06Mi | 92.23Mi |   +4.73% |
| `DeleteNode_HotLabel_10k`                 |       0.000 |   0.000 |        ~ |
| `DeleteNode_HotLabel_50k`                 |       0.000 |   0.000 |        ~ |
| `UpdateNode_HotLabel_50k`                 |       149.0 |   146.0 |   -2.01% |
| `IndexNodeProperty`                       |       265.0 |   386.0 |  +45.66% |
| `Ingest_AddNode_Single`                   |       306.0 |   306.0 |        ~ |
| `Ingest_AddNodes_Batch100`                |     30.83Ki | 30.82Ki |        ~ |
| `Ingest_AddNodes_Batch1000`               |     299.6Ki | 260.4Ki |        ~ |
| `Ingest_AddEdge_Single`                   |       282.0 |   282.0 |        ~ |
| `Ingest_EndToEnd_Disk_10k`                |     44.85Mi | 52.20Mi |  +16.39% |
| `Ingest_AddNode_Disk`                     |       355.0 |   356.0 |        ~ |
| `Scale_PointLookup_10k`                   |       0.000 |   0.000 |        ~ |
| `Scale_PointLookup_100k`                  |       0.000 |   0.000 |        ~ |
| `Scale_EqualityQuery_10k`                 |   7190904.0 |   176.0 | -100.00% |
| `Scale_EqualityQuery_100k`                |  92940281.5 |   176.0 | -100.00% |
| `Scale_TypeQuery_10k`                     |    163976.0 |   680.0 |  -99.59% |
| `Scale_TypeQuery_100k`                    |  1568.133Ki | 7.242Ki |  -99.54% |
| `Scale_BFS4Hop_10k`                       |     3.930Ki | 3.328Ki |  -15.31% |
| `Scale_BFS4Hop_100k`                      |     3.453Ki | 2.953Ki |  -14.48% |
| `Walk_DFS_Deep`                           |     1.949Mi | 1.720Mi |  -11.74% |
| `Walk_ProvenanceChain`                    |     8.010Ki | 8.010Ki |        ~ |
| `Walk_ShortestPath_Disk`                  |    1848.9Ki | 419.6Ki |  -77.30% |
| `Pattern_TwoHop_Scoped`                   |     3.605Mi | 3.605Mi |        ~ |
| `Subgraph_Induced_1k`                     |     251.7Ki | 251.7Ki |        ~ |
| `Connect_EdgeExists`                      |       16.00 |   16.00 |        ~ |
| `Connect_IsConnected_Near`                |    106.76Ki | 51.28Ki |  -51.96% |
| `Connect_NeighboursByNodeType`            |       62.00 |   62.00 |        ~ |
| `QueryEdges_ByType_Memory`                |    3152.5Ki | 105.5Ki |  -96.65% |
| `QueryEdges_ByType_Disk`                  |   34689.5Ki | 235.8Ki |  -99.32% |
| `QueryRelations_Both_Disk`                |   69446.3Ki | 251.9Ki |  -99.64% |
| `Degree_Typed_Disk`                       |     175.2Ki |   0.0Ki | -100.00% |
| `ColdOpen_UncompactedWAL_10k`             |     12.07Mi | 16.74Mi |  +38.67% |
| `Footprint_Memory_TopologyOnly`           |     15.14Mi | 88.32Mi | +483.38% |
| `Footprint_Disk_TopologyOnly`             |     294.3Mi | 306.4Mi |   +4.10% |
| `Footprint_Memory_WithPropertyIndex`      |     118.7Mi | 156.5Mi |  +31.90% |
| `Footprint_Disk_WithPropertyIndex`        |     457.6Mi | 516.6Mi |  +12.90% |
| `Footprint_Memory_WithOrderedKey`         |     118.7Mi | 158.6Mi |  +33.61% |
| `Footprint_Disk_HalfDeleted_Uncompacted`  |     331.2Mi | 343.3Mi |   +3.65% |
| `Footprint_Disk_HalfDeleted_Compacted`    |     362.8Mi | 376.7Mi |   +3.84% |
| `Footprint_PropIndex_Cardinality1`        |     0.000Mi | 8.252Mi |        ? |
| `Footprint_PropIndex_Cardinality100`      |      0.00Mi | 16.41Mi |        ? |
| `Footprint_PropIndex_Cardinality10k`      |      0.00Mi | 43.84Mi |        ? |
| `Footprint_PropIndex_CardinalityAll`      |      0.00Mi | 50.54Mi |        ? |
| `Footprint_PropIndex_NoIndex`             |       0.000 |   0.000 |        ~ |
| `Footprint_DiskFileSize`                  |     457.6Mi | 516.6Mi |  +12.90% |
| `Parallel_PointLookup_Memory`             |       0.000 |   0.000 |        ~ |
| `Parallel_PointLookup_Disk`               |       64.00 |   64.00 |        ~ |
| `Parallel_PropertyEqual_Memory`           |       8.000 |   8.000 |        ~ |
| `Parallel_Neighbours_Memory`              |       48.00 |   48.00 |        ~ |
| `Parallel_BFS3Hop_Disk`                   |     4.629Ki | 3.162Ki |  -31.69% |
| `Parallel_AddNode_Memory`                 |       263.0 |   260.0 |        ~ |
| `Parallel_IndexNodeProperty_SameKey`      |       199.5 |   376.5 |  +88.72% |
| `Parallel_IndexNodeProperty_DistinctKeys` |       217.5 |   427.0 |  +96.32% |
| `Parallel_MixedReadWrite_Memory`          |       54.00 |   55.00 |        ~ |
| `AddNode`                                 |       306.0 |   306.0 |        ~ |
| `GetNode`                                 |       0.000 |   0.000 |        ~ |
| `BFS`                                     |     218.3Ki | 179.5Ki |  -17.77% |
| `ShortestPath`                            |    114.16Ki | 98.12Ki |  -14.06% |
| `PropertyIndexLookup`                     |       8.000 |   8.000 |        ~ |

### Allocation count (allocs/op)

| Benchmark                                 |   Baseline | Current |   Change |
| ----------------------------------------- | ---------: | ------: | -------: |
| `PointLookupNode_Memory`                  |      0.000 |   0.000 |        ~ |
| `PointLookupNode_Disk`                    |      1.000 |   1.000 |        ~ |
| `NodesByType_Selective_Memory`            |      1.000 |   1.000 |        ~ |
| `NodesByType_Selective_Disk`              |    110.000 |   5.000 |  -95.45% |
| `QueryNodes_TypeLimit10_Memory`           |      5.000 |  16.000 | +220.00% |
| `QueryNodes_TypeLimit10_Disk`             |   100562.0 |   123.0 |  -99.88% |
| `NodesByProperty_Equal_Memory`            |      1.000 |   1.000 |        ~ |
| `QueryNodes_PropertyEqual_Memory`         | 300043.000 |   4.000 | -100.00% |
| `QueryNodes_PropertyEqual_Disk`           | 300600.000 |   4.000 | -100.00% |
| `QueryNodes_TypeAndPropertyEqual_Memory`  | 300063.000 |   5.000 | -100.00% |
| `QueryNodes_PropertyPrefix_Memory`        |  300201.00 |   27.00 |  -99.99% |
| `QueryNodes_PropertyRange_Memory`         |  300201.00 |   27.00 |  -99.99% |
| `QueryNodes_PropertyRange_Disk`           |   300758.0 |   584.0 |  -99.81% |
| `QueryRelations_Anchored_Memory`          |      5.000 |  36.000 | +620.00% |
| `QueryRelations_Anchored_Disk`            |   202.077k |  1.038k |  -99.49% |
| `Neighbours1Hop_Memory`                   |      2.000 |   2.000 |        ~ |
| `Neighbours1Hop_Disk`                     |      8.000 |   8.000 |        ~ |
| `BFS3Hop_Memory`                          |      33.00 |   23.00 |  -30.30% |
| `BFS3Hop_Disk`                            |      70.00 |   45.00 |  -35.71% |
| `Degree_Hub_Memory`                       |      1.000 |   0.000 | -100.00% |
| `Degree_Hub_Disk`                         |     1.014k |  0.000k | -100.00% |
| `DeleteNode_WithPropertyIndex`            |      0.000 |   0.000 |        ~ |
| `BFS_Deep`                                |    30190.0 |   198.0 |  -99.34% |
| `BFS_Wide`                                |     1323.0 |   237.0 |  -82.09% |
| `BFS_Disk_Deep`                           |      913.0 |   395.0 |  -56.74% |
| `ReopenCompactedStore`                    |     557.4k |  558.1k |   +0.13% |
| `CompactSteadyState`                      |     300.2k |  100.2k |  -66.61% |
| `DeleteNode_HotLabel_10k`                 |      0.000 |   0.000 |        ~ |
| `DeleteNode_HotLabel_50k`                 |      0.000 |   0.000 |        ~ |
| `UpdateNode_HotLabel_50k`                 |      4.000 |   4.000 |        ~ |
| `IndexNodeProperty`                       |      5.000 |   5.000 |        ~ |
| `Ingest_AddNode_Single`                   |      3.000 |   3.000 |        ~ |
| `Ingest_AddNodes_Batch100`                |      302.0 |   302.0 |        ~ |
| `Ingest_AddNodes_Batch1000`               |     3.015k |  3.011k |        ~ |
| `Ingest_AddEdge_Single`                   |      2.000 |   2.000 |        ~ |
| `Ingest_EndToEnd_Disk_10k`                |     553.3k |  523.6k |   -5.36% |
| `Ingest_AddNode_Disk`                     |      6.000 |   6.000 |        ~ |
| `Scale_PointLookup_10k`                   |      0.000 |   0.000 |        ~ |
| `Scale_PointLookup_100k`                  |      0.000 |   0.000 |        ~ |
| `Scale_EqualityQuery_10k`                 |  30032.000 |   4.000 |  -99.99% |
| `Scale_EqualityQuery_100k`                | 300043.000 |   4.000 | -100.00% |
| `Scale_TypeQuery_10k`                     |      5.000 |   7.000 |  +40.00% |
| `Scale_TypeQuery_100k`                    |      5.000 |  16.000 | +220.00% |
| `Scale_BFS4Hop_10k`                       |      51.00 |   32.00 |  -37.25% |
| `Scale_BFS4Hop_100k`                      |      47.00 |   29.00 |  -38.30% |
| `Walk_DFS_Deep`                           |    20193.0 |   196.0 |  -99.03% |
| `Walk_ProvenanceChain`                    |      90.00 |   90.00 |        ~ |
| `Walk_ShortestPath_Disk`                  |    12872.0 |   124.0 |  -99.04% |
| `Pattern_TwoHop_Scoped`                   |     399.9k |  399.9k |        ~ |
| `Subgraph_Induced_1k`                     |     1.045k |  1.045k |        ~ |
| `Connect_EdgeExists`                      |      1.000 |   1.000 |        ~ |
| `Connect_IsConnected_Near`                |      54.00 |   37.00 |  -31.48% |
| `Connect_NeighboursByNodeType`            |      3.000 |   3.000 |        ~ |
| `QueryEdges_ByType_Memory`                |      5.000 |  31.000 | +520.00% |
| `QueryEdges_ByType_Disk`                  |   202.077k |  1.040k |  -99.49% |
| `QueryRelations_Both_Disk`                |   404.164k |  1.056k |  -99.74% |
| `Degree_Typed_Disk`                       |     1.012k |  0.000k | -100.00% |
| `ColdOpen_UncompactedWAL_10k`             |     284.6k |  314.9k |  +10.67% |
| `Footprint_Memory_TopologyOnly`           |     291.8k | 1702.4k | +483.38% |
| `Footprint_Disk_TopologyOnly`             |     2.602M |  2.603M |   +0.01% |
| `Footprint_Memory_WithPropertyIndex`      |     2.967M |  3.095M |   +4.29% |
| `Footprint_Disk_WithPropertyIndex`        |     5.368M |  4.895M |   -8.80% |
| `Footprint_Memory_WithOrderedKey`         |     2.967M |  3.103M |   +4.56% |
| `Footprint_Disk_HalfDeleted_Uncompacted`  |     3.354M |  3.354M |   +0.01% |
| `Footprint_Disk_HalfDeleted_Compacted`    |     3.354M |  3.354M |   +0.01% |
| `Footprint_PropIndex_Cardinality1`        |       0.0k |  128.8k |        ? |
| `Footprint_PropIndex_Cardinality100`      |       0.0k |  263.4k |        ? |
| `Footprint_PropIndex_Cardinality10k`      |       0.0k |  787.0k |        ? |
| `Footprint_PropIndex_CardinalityAll`      |       0.0k |  826.5k |        ? |
| `Footprint_PropIndex_NoIndex`             |      0.000 |   0.000 |        ~ |
| `Footprint_DiskFileSize`                  |     5.368M |  4.895M |   -8.80% |
| `Parallel_PointLookup_Memory`             |      0.000 |   0.000 |        ~ |
| `Parallel_PointLookup_Disk`               |      1.000 |   1.000 |        ~ |
| `Parallel_PropertyEqual_Memory`           |      1.000 |   1.000 |        ~ |
| `Parallel_Neighbours_Memory`              |      2.000 |   2.000 |        ~ |
| `Parallel_BFS3Hop_Disk`                   |      71.00 |   46.00 |  -35.21% |
| `Parallel_AddNode_Memory`                 |      3.000 |   3.000 |        ~ |
| `Parallel_IndexNodeProperty_SameKey`      |      5.000 |   5.000 |        ~ |
| `Parallel_IndexNodeProperty_DistinctKeys` |      6.000 |   6.000 |        ~ |
| `Parallel_MixedReadWrite_Memory`          |      2.000 |   2.000 |        ~ |
| `AddNode`                                 |      3.000 |   3.000 |        ~ |
| `GetNode`                                 |      0.000 |   0.000 |        ~ |
| `BFS`                                     |    3058.00 |   66.00 |  -97.84% |
| `ShortestPath`                            |     1563.0 |   576.0 |  -63.15% |
| `PropertyIndexLookup`                     |      1.000 |   1.000 |        ~ |

### Time per node (sec/node)

| Benchmark                                | Baseline | Current |  Change |
| ---------------------------------------- | -------: | ------: | ------: |
| `Ingest_AddNodes_Batch100`               |   661.1n |  694.8n |       ~ |
| `Ingest_AddNodes_Batch1000`              |   679.8n |  663.9n |       ~ |
| `Footprint_Memory_TopologyOnly`          |    223.1 |   223.1 |       ~ |
| `Footprint_Disk_TopologyOnly`            |    135.3 |   149.2 | +10.27% |
| `Footprint_Memory_WithPropertyIndex`     |    281.9 |   421.6 | +49.56% |
| `Footprint_Disk_WithPropertyIndex`       |    194.2 |   346.5 | +78.42% |
| `Footprint_Memory_WithOrderedKey`        |    281.9 |   426.9 | +51.44% |
| `Footprint_Disk_HalfDeleted_Uncompacted` |    659.6 |   714.9 |  +8.38% |
| `Footprint_Disk_HalfDeleted_Compacted`   |    149.0 |   158.0 |  +6.04% |
| `Footprint_DiskFileSize`                 |    124.0 |   111.5 | -10.08% |

### Resident bytes per node (B/node)

| Benchmark                                | Baseline | Current |   Change |
| ---------------------------------------- | -------: | ------: | -------: |
| `Footprint_Memory_TopologyOnly`          |    446.1 |   446.2 |   +0.02% |
| `Footprint_Disk_TopologyOnly`            |    270.7 |   298.3 |  +10.20% |
| `Footprint_Memory_WithPropertyIndex`     |    563.8 |   843.2 |  +49.56% |
| `Footprint_Disk_WithPropertyIndex`       |    388.4 |   693.0 |  +78.42% |
| `Footprint_Memory_WithOrderedKey`        |    563.8 |   853.8 |  +51.44% |
| `Footprint_Disk_HalfDeleted_Uncompacted` |    659.6 |   714.9 |   +8.38% |
| `Footprint_Disk_HalfDeleted_Compacted`   |    149.0 |   158.0 |   +6.04% |
| `Footprint_PropIndex_Cardinality1`       |    179.0 |   295.6 |  +65.14% |
| `Footprint_PropIndex_Cardinality100`     |    180.5 |   297.0 |  +64.54% |
| `Footprint_PropIndex_Cardinality10k`     |    194.0 |   307.4 |  +58.45% |
| `Footprint_PropIndex_CardinalityAll`     |    281.1 |   365.7 |  +30.10% |
| `Footprint_PropIndex_NoIndex`            |    170.1 |   170.2 |   +0.06% |
| `Footprint_DiskFileSize`                 |    248.0 |   223.0 |  -10.08% |
| `Footprint_Memory_TopologyOnly`          |    42.55 |   42.55 |        ~ |
| `Footprint_Disk_TopologyOnly`            |    25.82 |   28.45 |  +10.19% |
| `Footprint_Memory_WithPropertyIndex`     |    53.77 |   80.41 |  +49.54% |
| `Footprint_Disk_WithPropertyIndex`       |    37.04 |   66.09 |  +78.43% |
| `Footprint_Memory_WithOrderedKey`        |    53.77 |   81.43 |  +51.44% |
| `Footprint_Disk_HalfDeleted_Uncompacted` |    31.45 |   34.09 |   +8.39% |
| `Footprint_Disk_HalfDeleted_Compacted`   |    7.106 |   7.536 |   +6.05% |
| `Footprint_PropIndex_Cardinality1`       |    17.07 |   28.19 |  +65.14% |
| `Footprint_PropIndex_Cardinality100`     |    17.21 |   28.32 |  +64.56% |
| `Footprint_PropIndex_Cardinality10k`     |    18.50 |   29.31 |  +58.43% |
| `Footprint_PropIndex_CardinalityAll`     |    26.81 |   34.87 |  +30.06% |
| `Footprint_PropIndex_NoIndex`            |    16.23 |   16.23 |        ~ |
| `Footprint_DiskFileSize`                 |    12.68 |   21.27 |  +67.74% |
| `Footprint_DiskFileSize`                 |    10.97 |    0.00 | -100.00% |
