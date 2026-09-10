# Benchmarks

## How these were produced

|              |                                                                                                                                                                                   |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Date**     | 2026-07-21                                                                                                                                                                        |
| **OS**       | Windows 11 (amd64)                                                                                                                                                                |
| **Go**       | go1.26.2                                                                                                                                                                          |
| **Hardware** | AMD Ryzen 9 5980HS, 16 cores                                                                                                                                                      |
| **Baseline** | git `036aac0`, the commit before this work began                                                                                                                                  |
| **Suites**   | [bench](../tests/graphene_bench_test.go) · [parallel](../tests/graphene_parallel_bench_test.go) · [coverage](../tests/graphene_coverage_bench_test.go) · [footprint](../tests/graphene_footprint_test.go) |
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

## Phase 7 — allocation and footprint (2026-08-30)

A separate campaign from everything above, and measured to a stricter rule. The
sections before this one report a single A/B against `036aac0`; Phase 7 reports
**three columns for every item — sec/op, resident bytes, allocs/op — in the
project's standing priority order, speed > resident memory > allocations.** An
item that wins the third column and loses either of the first two is reverted,
not shipped with a caveat. That rule has precedent: bulk index loading cut
allocations 9-19%, cost 35-75% more resident memory, and was reverted.

### Method, and where the baseline comes from

| | |
| --- | --- |
| **Date** | 2026-08-30 |
| **Baseline** | git `2b94606`, built into a separate worktree, benchmarked as the other arm |
| **Method** | Interleaved A/B, alternating rounds at `-benchtime=1s`, warm-up round discarded, issued as one background job with nothing else touching the machine |
| **Control** | `PointLookupNode_Memory` — byte-identical between the arms — read **before** the headline |
| **Resident** | `-tags=stress -bench=Footprint -benchtime=1x`, five interleaved rounds. Not `B/op`, which is bytes allocated *during* an op and says nothing about what a long-lived process holds |
| **Profile** | `make allocprofile`, which runs at `-test.memprofilerate=1` — see below |

**The baseline is the HEAD arm of the interleaved runs, not a separately
recorded table.** A table taken in its own session and compared against later is
exactly the "never compare across sessions" mistake; a HEAD worktree measured in
the same alternating rounds as the tree is the same discipline the phases before
it used, and is a stronger baseline rather than a substitute for one.

### `make allocprofile`, and why the sample rate is the whole point

```
make allocprofile
```

runs the ingest and traversal benchmarks under `-memprofile` at
**`-test.memprofilerate=1`** and prints two `pprof` rankings, `-alloc_objects`
and `-alloc_space`, because the largest count and the largest bytes are rarely
the same site.

The rate is not a detail. At Go's default sampling interval the single-record
write path — three allocations per record, the phase's first finding — **did not
appear in the profile at all**; only `&store.Node{}` and `&nodeVersion{}` did.
Small short-lived allocations are precisely what gets under-sampled, and they
are precisely what this phase exists to remove. A profile that cannot see them
is worse than no profile, because it ranks confidently and wrongly.

### The write path — three allocations per record became one

Control read first: **−0.2%** (20.0-34.7 ns HEAD, 19.9-32.0 tree). The run stands.

| Benchmark | sec/op | resident | allocs/op | B/op |
| --- | ---: | ---: | ---: | ---: |
| `Ingest_AddNode_Disk` | **−18.1%** (4 465 → 3 655 ns) | flat | **8 → 5** (−37.5%) | −37.1% |
| `BulkWrite_AddNodes_Disk_NoSync` n=10000 | **−21.0%** (3.71 → 2.93 ms) | flat | **50 191 → 40 111** (−20.1%) | −28.3% |
| — n=1000 | **−23.7%** (628 → 479 µs) | flat | 5 066 → 4 045 (−20.2%) | −30.6% |
| — n=100 | −2.5% — inside the noise | flat | 540 → 430 (−20.4%) | −27.4% |
| `Ingest_AddNodes_Batch1000` | −5.7% — inside the noise | flat | flat | +0.2% |
| `ShortestPath` | −8.1% | n/a — traversal scratch | **576 → 37** (−93.6%) | −30.4% |

`marshalNode` built a payload, `WAL.append` copied it because a caller *may*
reuse its slice, and `writeRecord` allocated the frame and copied again. Two of
the three defended against a caller that does not exist: all eight single-record
call sites marshal a temporary and drop it. The path now frames straight into
the buffer the ring takes ownership of.

`ShortestPath`'s 576 allocations were not the visited maps the plan suspected.
`expandAndAdvance` grew the next level's frontier from `nil` on every level of
every direction — 590k of the benchmark's 789k allocations, **75% of the whole
thing**.

### The read path — one membership set, three representations

Control read first: **+5.6%** (19.8-98.5 ns HEAD, 20.9-111.7 tree), and the
drift runs *against* the tree, so each figure below is a floor rather than a
ceiling.

| Benchmark | sec/op | resident | allocs/op | B/op |
| --- | ---: | ---: | ---: | ---: |
| `BFSIDs_Wide` | **−58.2%** | n/a | **104 → 34** (−67.3%) | −61.6% |
| `BFS_Wide` | **−52.2%** | n/a | **237 → 97** (−59.1%) | −54.1% |
| `BFS_Deep` | **−41.7%** | n/a | **198 → 58** (−70.7%) | −65.1% |
| `BFSIDs_Deep` | **−34.9%** | n/a | **97 → 27** (−72.2%) | −61.9% |
| `BFS3Hop_Disk` | **−20.7%** | n/a | 45 → 39 (−13.3%) | −21.2% |
| `BFSIDs_Disk_Deep` | −7.2% | n/a | 20 → 15 (−25.0%) | −18.0% |
| `Neighbours1Hop_Disk` | −5.1% | n/a | 8 → 8 | ±0.0% |
| `BFS_Disk_Deep` | −2.2% | n/a | 395 → 385 (−2.5%) | −7.9% |
| `ReopenCompactedStore` | −1.0% | n/a | flat | flat |
| `Ingest_AddNode_Disk` | −0.1% | flat | 5 → 5 | overlapping ranges |

Resident bytes, five footprint fixtures, **±0.0% on every one**: DiskFileSize
175.0 B/node, HalfDeleted_Compacted 158.2, HalfDeleted_Uncompacted 835.0,
TopologyOnly 298.9, WithPropertyIndex 575.9.

At `-test.memprofilerate=1` on a wide BFS, the two `map[ID]struct{}` sets were
**1.13 MB of the walk's 2.17 MB/op** — more than half of everything it
allocated. They are now one `idSet` with three representations chosen by
measured density rather than by a tuned constant; see
[TECHNICAL_DETAILS.md](TECHNICAL_DETAILS.md) §14.12.

### The measurement that changed the design, and what it cost to find

The first `idSet` had two representations and **lost the A/B on every disk
walk** — a flat +192 B and +2 allocations per set, whatever the walk's size,
while winning 46-58% on the in-memory ones. Under the revert rule that is a
revert, so the cause had to be found rather than argued around.

It was not the algorithm. On the disk fixtures the density rule correctly never
builds a bitset at all: `midNode` sits at ID ~33 000 against ~15 entries
visited. `go build -gcflags='-m'` named it instead — **a map reached through a
pointer-held struct field cannot be proved non-escaping.** The maps this
replaced were short-lived locals the compiler put on the *stack*; moving one
behind `s.m` moves it to the heap, and that cost is paid by every walk whether
it grows or not.

The fix is a 32-entry inline array scanned linearly, so the small case allocates
nothing on any path. An isolated micro-benchmark at the fixtures' ID shape:
map 1 872 B / 10 allocs, first `idSet` 2 256 B / 14 allocs — reproducing the
+384 B / +4 on a two-set walk exactly — and **0 B / 0 allocs** after.

### One run voided, and why it is recorded rather than dropped

A run with the control at **+62.4%** and absolutes of 63-206 ns against 17-20 on
a quiet host was discarded whole. The load was this session's own tool calls
landing during the run. `B/op` and `allocs/op` from it are still valid and were
used — they are deterministic — but nothing in the `sec/op` column survived, and
a fourth run was taken. Phases 4 and 5 each discarded a run for the same reason;
this is the third.

## The CSR load path — two candidates, one kept (2026-08-30)

Both candidates came out of the Phase 7 arena spike ([TECHNICAL_DETAILS.md
§14.13](TECHNICAL_DETAILS.md)). Both were built, both were measured on all three
of the phase's columns, and the revert rule fired on one of them.

**Neither is a format change.** The on-disk image already stores records as one
packed byte stream, and adjacency has not been serialised since v7 — `Build`
recomputes the neighbour arrays on every load. Both candidates are in-memory
representation changes on the load path. `Footprint_DiskFileSize` is **175.0
B/node in every arm of every run below**, which is the empirical form of that
claim.

### Kept: the record arena

`disk/csr_io.go` packs every record's labels and property blob into shared
backing arrays and hands each record a three-index sub-slice — `arena[lo:hi:hi]`,
which preserves the `csrBytes` aliasing contract by forcing an append to copy
rather than scribble into the next record. Roughly 600 000 small objects become a
handful of large ones.

| column | result | |
| --- | --- | --- |
| allocations, load path | **−21.8%** | deterministic |
| `ReopenCompactedStore` | **+11.6%** (58.6 → 65.3 ms) | consistent in direction across three runs |
| GC cycle, 512-byte blobs | **−21.4%** (5.452 → 4.285 ms) | disjoint ranges |
| GC cycle, 64-byte blobs | +5.4% | overlapping, base spread 53% |
| GC cycle, no blobs | −14.7% | overlapping, tree spread 42% |
| resident, five footprint fixtures | ±0.0% | **weak instrument — see below** |

**Kept.** Speed is split and nets positive: the reopen cost is paid once per
process, the GC saving every cycle for the life of it, so break-even is ~6 forced
cycles at 512-byte blobs and ~14 at none. Nothing loses on resident bytes or
allocations, so §7.5's revert rule does not fire.

**The resident row is a "no regression", not a measurement.** No footprint
fixture reopens a store from disk, so not one of them executes the arena load
path. Saying "flat" is honest; saying "measured and unchanged" would not be.

### The GC benchmark, and two harness defects it took to get a number

`tests/gc_bench_test.go` (`stress`) builds 100 000 nodes and 200 000 edges,
compacts, closes, and **reopens** — the reopened image is the one under test,
because it is built by the reader rather than by `Build`. It then calls
`debug.SetGCPercent(-1)` and times a loop whose body is one `runtime.GC()` and
nothing else, so `ns/op` is the per-cycle scan cost directly.

**Disabling the trigger is not a detail.** At a fixed GOGC a smaller live heap
triggers proportionally more cycles, so comparing two layouts at GOGC=on measures
the trigger rate rather than the scan cost. The original spike did not disable
it, which is why its baseline — 1.34 / 2.29 / 5.77 ms, rising with blob size —
does not reproduce against the shipped loader, where the same three fixtures are
flat.

Two runs were then thrown away, both to the same family of defect:

1. **The 25 ns control shared a process with the GC benchmarks.** The control
   read through a live 100 000-node store and three forced-GC loops; it moved
   **−9.7%** across all rounds and **−13.3%** with the warm-up discarded, on
   byte-identical code, with a base-arm spread of **119%**. Voided.
2. **The three blob sizes shared a process with each other.** Control was clean
   at +1.6%, but each sub-benchmark's forced-GC loop was collecting its
   predecessor's store: blob0's *fastest* round was its highest iteration count
   (N=508 → 2.11 ms) and its slowest was N=364 → 5.83 ms, and blob0/blob64 swung
   against blob512 round by round. Base-arm spreads of **176%** and **147%** —
   uninterpretable in either direction, though blob512 was clean and disjoint in
   both runs.

The fix needed no code change, since `-test.bench` accepts a sub-benchmark
pattern: run every sub-benchmark in its own process, per arm, per round, control
first. Isolating them **flipped blob0's sign** (+41.1% → −14.7%) and tightened
blob64's base spread from 147% to 53% — which is what says the apparent
small-blob regression was the harness rather than the change.

Final run, six interleaved rounds, warm-up discarded, control read first:

| fixture | base (min) | spread | tree (min) | spread | change | |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| _Control:_ `PointLookupNode_Memory` | 28.05 ns | 37.2% | 27.87 ns | 20.3% | **−0.6%** | overlap — run stands |
| GC cycle, blob 0 B | 3.245 ms | 18.0% | 2.769 ms | 41.8% | −14.7% | overlap |
| GC cycle, blob 64 B | 3.419 ms | 53.1% | 3.605 ms | 37.4% | +5.4% | overlap |
| GC cycle, blob 512 B | 5.452 ms | 16.0% | 4.285 ms | 20.2% | **−21.4%** | **disjoint** |

The 512-byte row is disjoint for the third time under a third harness — −32.6%,
−34.0%, −21.4%. The magnitude tracks host temperature; the direction does not
move. The absolutes in this run are ~1.7× the previous one's on both arms
(control minima 28 ns against 19), which is thermal drift on a laptop after a
long benchmarking session — the reason interleaving is the method and minima are
the statistic.

### Reverted: denormalised neighbour arrays

Storing the far endpoint `NodeID` beside the `EdgeID` in the adjacency arrays,
plus an unfiltered fast-path hop that skips record resolution when a walk asks
for no edge-type filter.

| column | result |
| --- | --- |
| resident, `Footprint_Disk_*` | **+10.8% / +7.7% / +5.6%** |
| four walk benchmarks | inside the control's spread |
| degree sweep to 32 768 | scatter, with the opposite of the predicted shape |

**The cost needed no interpretation.** The footprint fixtures give each node two
edges, and 2 edges × 2 directions × 8 bytes is **32 B/node** against the **32.1
B/node** observed.

**The arithmetic is why the scatter is a decision and not a failed run.** At
degree 32 768 the walk costs **351 ns per edge**; the isolated denormalised hop
cost **14.6 ns per edge**. The work this change removes is ~4% of what
`Neighbours` does — the rest is result materialisation, dedupe and the caller's
iteration — so its ceiling is **~2.8% at any degree**, permanently below this
suite's noise floor. A fixture that could show it would be a fixture built to
flatter the change.

**Reverted** under §7.5: it loses the resident column and cannot win the speed
column by more than its own arithmetic allows. `disk/csr.go` is back at HEAD. It
stays cheap to revisit, because adjacency is recomputed on load and nothing about
it is durable.

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
go test ./tests/ -tags=stress -bench=. -benchmem -count=6 -run='^$'
go test ./tests/ -tags=stress -bench=Footprint -benchtime=1x -run='^$'
go test ./tests/ -tags=stress -bench=Parallel -cpu=1,2,4,8,16 -run='^$'
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

---

## Phase 8 — what a traversal deadline costs (2026-09-01)

`store.Budget` had three dimensions and only two of them were ever measured.
Every traversal benchmark in the tree passed the zero `Budget`, which takes the
guard's `off` fast path, so the accounting had never been run against the walk it
accounts for. `BenchmarkBFS_Deep_Budget` closes that: one 10 000-node chain
fixture, three arms in one binary.

| Arm         | Budget                    | What it isolates                          |
|-------------|---------------------------|-------------------------------------------|
| `unbounded` | `Budget{}`                | the `off` path — must never move           |
| `nodes`     | `Budget{MaxNodes: 1<<30}` | accounting on, no clock                    |
| `time`      | `Budget{MaxTime: 1h}`     | accounting plus a deadline                 |

The budgets are generous on purpose and asserted not to fire before the timer
starts; an arm that trips would be measuring the error path.

### Choosing the deadline cadence

`MaxTime` used to share cancellation's 256-step cadence, which meant a walk
charging fewer than 256 steps never checked the clock at all. Per step is the
obvious fix and it is not free. Ten interleaved runs of the prebuilt binary,
`benchstat` over the pair:

| Benchmark                     | every step | every 16 steps | vs base            |
|-------------------------------|-----------:|---------------:|--------------------|
| `BFS_Deep_Budget/unbounded`   |   2.167 ms |       2.087 ms | ~ (p=0.393, n=10)  |
| `BFS_Deep_Budget/nodes`       |   2.317 ms |       2.205 ms | ~ (p=0.739, n=10)  |
| `BFS_Deep_Budget/time`        |   2.712 ms |       2.185 ms | **−19.43%** (p=0.003) |
| `PointLookupNode_Memory`      |   27.18 ns |       26.61 ns | ~ (p=0.529, n=10)  |
| `BlobPointLookupNode/blob=32` |   15.80 ns |       14.94 ns | ~ (p=0.063, n=10)  |

`B/op` and `allocs/op` are identical across all three arms and both sides —
614.5 KiB and 58 allocations — so this is time and nothing else.

The `time` arm is the only significant movement in the comparison; `unbounded`,
`nodes` and all three controls sit still, which is what makes the number
readable. At 16 the deadline arm is indistinguishable from `nodes`, so the
enforcement is effectively free while the overshoot falls from 256 charged steps
to 16 — five or six nodes on a BFS. The cost is paid only by callers who set
`MaxTime`; with no deadline the guard runs the code it always ran, which is what
`unbounded` and `nodes` confirm.

Read against the control spread: `PointLookupNode_Memory` is a very short op and
came in at ±37% / ±15%, well past the ±6% CONTRIBUTING calls usable. The blob
controls at ±4–11% are the ones to trust here, and the p-values are computed over
ten runs rather than one.

### What no cadence can fix

The Go monotonic clock on Windows is `_INTERRUPT_TIME` from the
`KUSER_SHARED_DATA` page, which advances at the system timer interrupt — 15.6 ms
by default, ~0.5 ms while some process has raised the timer resolution. Two
readings inside one tick are equal rather than ordered, so a `MaxTime` shorter
than a tick cannot be observed as exceeded by any comparison. `MaxTime` is
documented as best-effort with that floor named, `MaxNodes` and `MaxEdges` remain
exact, and the enforcement tests drive an injected clock instead of asserting
that the real one moved.

---

## Phase 9 — two costs that were being guessed at (2026-09-01)

Neither of these produced a code change. Both were open questions an embedder
would otherwise have had to discover by running into them, which is the worst
way to learn a number.

### Declaring a unique key at Open

Unique declarations live in memory and must be re-declared by every process that
opens the store, which means the validation runs before the process can serve
anything. `DeclareUniqueNodeKey` walks the key's value buckets and skips any held
by fewer than two live entities: O(distinct values under that key), over an
already-resident index, with no I/O and no graph scan. The question was what that
constant is.

`BenchmarkDeclareUniqueProperty`, `-benchtime=3x`:

| Arm                            | Nodes     | ns/op        | B/op | allocs/op |
|--------------------------------|----------:|-------------:|-----:|----------:|
| `distinct/10000`               |    10 000 |      147 067 |  208 |         1 |
| `distinct/100000`              |   100 000 |    1 469 633 |  208 |         1 |
| `distinct/1000000`             | 1 000 000 |   14 679 700 |  208 |         1 |
| `distinct+bystander/1000000`   | 1 000 000 |   15 707 400 |  208 |         1 |

**About 14.7 ns per distinct value, linear, one allocation regardless of size.**
A million distinct values cost 14.7 ms; ten million would cost about 150 ms,
once, at Open. That is a number an embedder can plan around rather than trip
over, and it is small enough that the declare-without-validating escape hatch
this measurement was going to justify is not worth building.

Every arm declares over *distinct* values, because a declaration that would be
refused is not a declaration and would measure the failure path. The bystander
arm adds a second, low-cardinality key to the same index and costs the same
within noise, which is what says the walk is per key rather than per index.

### One transaction holding a large ingest

The `bulk` package batches, which bounds memory by giving up per-entity
atomicity: a load that fails partway leaves behind the batches that already
committed. A caller who wants one source to be one transaction cannot use that,
and a single source can be a million records.

`BenchmarkFootprint_OpenTransaction_*` measures the buffer while it is still
open — every record the transaction holds and the resolver's view of them, before
anything is written — over 200 000 nodes and 199 999 edges:

| Benchmark                            | Total     | Per entity |
|--------------------------------------|----------:|-----------:|
| `Footprint_OpenTransaction_Memory`   | 74.91 MiB |     ~196 B |
| `Footprint_OpenTransaction_Disk`     | 74.94 MiB |     ~196 B |

Identical across backends, which is expected: the buffer lives in the root
package and neither backend sees it until commit. For comparison, the same graph
*stored* in the memory backend is 42.57 MiB, so an open transaction costs roughly
1.75× what the data will occupy once it lands.

**The position, stated rather than left to be discovered:** one source is one
transaction up to a few million entities, at roughly 0.2 KB each plus whatever
the property blobs carry — about 400 MB for a million nodes and a million edges.
Past that, `bulk` is the answer and per-entity atomicity is what it costs. A
transaction that spilled to the WAL incrementally would remove the ceiling; at
these numbers it is not yet worth the complexity, and this is the measurement to
re-run before deciding otherwise.

### v0.7.0 — a window that costs the window

Before this, `Offset`/`Limit` was applied only at the end: the pipeline produced
every row, ordered it, and copied ten out. The measurements below are what that
cost, and what it costs now that the bound reaches back into the driving step
and the residual pass (TECHNICAL_DETAILS §7.3a).

Interleaved against a HEAD worktree with the control alternating, per
CONTRIBUTING §1, on the 100 000-node fixture. **Read the allocation columns.**
The machine was not quiet — several builds and test runs were competing for it —
and the time column moved by a factor of three between rounds on *both* arms.
Allocation counts are deterministic and did not move at all across five rounds.

`BenchmarkQueryNodes_TypeLimit10_*` — ten rows off a label, no filter:

| Arm | ns/op (median of 5) | B/op | allocs/op |
|---|---:|---:|---:|
| disk, before | 21 560 | 17 952 | 121 |
| **disk, after** | **423** | **160** | **2** |
| memory, before | 15 534 | 7 416 | 16 |
| **memory, after** | **977** | **240** | **3** |

`BenchmarkQueryNodes_TypeFilterLimit10_*` — ten rows off a label, narrowed by a
prefix filter the property index cannot drive from:

| Arm | ns/op (median of 3) | B/op | allocs/op |
|---|---:|---:|---:|
| disk, before | 43 116 988 | 19 118 154 | 90 818 |
| **disk, after** | **1 296 228** | **722 016** | **6** |
| memory, before | 51 303 314 | 9 551 867 | 559 |
| **memory, after** | **5 473 468** | **1 442 912** | **7** |

Two other benchmarks moved without being the target, both from the same change —
label postings merged rather than concatenated and deduplicated through a map,
which also retires the sort at the end of a labelled query:

| Benchmark | Before | After |
|---|---|---|
| `QueryEdges_ByType_Disk` | 241 352 B, 1 038 allocs | 5 248 B, 53 allocs |
| `QueryEdges_ByType_Memory` | 108 024 B, 31 allocs | 1 248 B, 3 allocs |
| `NodesByType_Selective_Disk` | 4 136 B, 5 allocs | 896 B, 1 alloc |
| `QueryNodes_PropertyRange_Disk` | 9 269 040 B, 573 allocs | 4 539 704 B, 43 allocs |

The last of those is the full-scan driver, which no longer builds a `seen` map
the delta/image disjointness already made redundant.

**Checked for regressions**, since the shared paths were touched: every other
query benchmark reported an identical allocation count on both arms. Two whose
medians looked worse on the noisy run — `QueryNodes_PropertyEqual_Disk` and
`QueryNodes_TwoEqualities_Memory` — were re-measured at `-benchtime=20000x`
interleaved over five rounds and came out flat (194.5 ns vs 202.8 ns, and 473.3
ns vs 468.9 ns; new arm first in both). That is what the short-run spread was:
the machine, not the change.

### v0.7.0 — the tracked-transaction livelock, measured

`BeginTracked` under six concurrent writers incrementing one counter, twelve
increments each, giving up after 2 000 conflicts. The read ran at the visible
epoch and the validation at the applied one, which group commit holds apart for
the whole of a commit's fsync:

| Arm | conflicts | writers that gave up |
|---|---:|---:|
| `SetSyncOnCommit(true)` — the default | 15 877 | 5 of 6 |
| `SetSyncOnCommit(false)` | 1 046 | 0 |

The sync is the entire window, which is what identified the cause. With the read
moved onto the applied view (`store.AppliedReader`) the default configuration
converges; the test that produced these numbers is the convergence assertion in
`tests/graphene_tx_read_test.go`, and the deterministic reproduction is
`disk/applied_test.go`, which constructs the applied-but-not-visible state
directly rather than racing for it.

### v0.7.0 — weighted shortest paths

New functionality, so there is no before-and-after: these are the numbers as
recorded, on the shared disk fixture (100 000 nodes, a chain plus a +13 stride
plus a 1 000-edge hub) at `-benchtime=200x -count=3`, medians.

The cost function is the similarity *gap*, `1 - Weight`, not the weight. The
fixture puts 0.5 on chain edges and 0.9 on strides because `Weight` is a
similarity score, so reading it directly as a distance inverts the graph — the
strides, which are the shortcuts, become the expensive edges. The gap reading
prices a stride at 0.1 against a chain hop's 0.5, which is the topology the
unweighted search sees.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Walk_ShortestPath_Disk` (unweighted, bidirectional) | 587 000 | 422 032 | 111 |
| `Walk_ShortestWeightedPath_Disk` (Dijkstra) | 9 006 000 | 4 135 776 | 194 |

**The 15× is the algorithm, not an inefficiency, and it is reported rather than
buried.** A bidirectional BFS meets in the middle after about fifteen hops. A
weighted search cannot stop at a meeting — the first meeting is not necessarily
on the cheapest path — so it is unidirectional, and settles every node cheaper
than the destination before it settles the destination. On this fixture that is
most of the graph. Callers who need a bound have `store.Budget`, whose
`MaxNodes` counts nodes settled; callers who can estimate the remaining distance
have A\*.

#### A\* against Dijkstra, on a graph a heuristic can speak about

The shared fixture cannot host this comparison: its 1 000 inbound edges into one
hub put every node within two hops of every other, so any estimate based on how
far apart two nodes look is an overestimate — and an overestimating heuristic
does not make A\* slower, it makes it wrong. The first draft of the A\*
benchmark asserted otherwise; the admissibility check written into the benchmark
setup is what caught it, and it compares **costs**, not hop counts, because
comparing hops was the original mistake.

So A\* is measured on a 100 × 100 four-connected lattice (10 000 nodes, 19 800
edges, compacted), corner to corner, with two edge cost classes (0.5 and 1.0)
alternating by row and column so the cheapest route is not simply the straight
one. Manhattan distance times the cheapest edge is admissible there by
construction, and consistent.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Walk_Grid_Dijkstra` | 4 384 780 | 1 612 096 | 493 |
| `Walk_Grid_AStar` | 1 539 218 | 994 240 | 466 |

**2.8× faster and 38% less memory, for the identical path** — the benchmark
fails rather than reports if the two costs differ.

#### The `IncidentEdge` field, measured against a control

`store.IncidentEdge` grew from 16 to 24 bytes to carry the edge weight, which
touches the buffer every traversal in the package reuses. HEAD was not a usable
control (it predates Phase 3 as well), so the comparison is against a copy of
this tree with the `Weight` field and its three fill sites removed and nothing
else changed, interleaved over four rounds at `-benchtime=400x`.

| Benchmark | allocs/op | B/op without | B/op with |
|---|---:|---:|---:|
| `Neighbours1Hop_Disk` | 8 → 8 | 504 | 504 |
| `BFS3Hop_Memory` | 17 → 17 | 832 | 856 |
| `BFS3Hop_Disk` | 39 → 39 | 2 432 | 2 456 |
| `Walk_DFS_Deep` | 126 → 126 | 1 216 416 | 1 216 424 |

Allocation counts are identical everywhere. Bytes grow by one buffer's width —
24 B/op on a 3-hop BFS, 8 B out of 1.2 MB on a deep DFS. Timings were noise in
both directions on a loaded machine, which is why the allocation columns are the
ones quoted; see CONTRIBUTING §1.


### v0.7.0 — what streaming an export actually saves

`export graph` used to hand `bulk` the live store, which is not a `store.Scanner`,
so the export materialised every node and edge ID before writing a record. It
now hands it a snapshot, which is. Both arms write the same JSONL dump of the
100 000-node fixture to `io.Discard`, three iterations, three rounds:

| Source | B/op | allocs/op |
|---|---:|---:|
| the live store — enumerate | 366 528 557 | 2 104 132 |
| a snapshot — stream | 351 647 970 | 2 104 080 |

**14.9 MB, or 4.1% of the export's total memory**, and the allocation count is
flat. That is the honest size of it: the enumeration's own cost drops from
O(V+E) IDs to one batch, but an export is dominated by hydrating and encoding
every record — 2.1 million allocations of which the enumeration is a handful —
so the saving is a slice, not a scaling change. It grows with the graph while
the batch does not, which is the reason to take it; the record cost grows with
the graph too, which is the reason it stays small as a fraction.

Times were 0.69–1.39 s/op across both arms with the ranges overlapping, i.e.
noise on a loaded machine (CONTRIBUTING §1), so no timing is quoted.

The second thing the snapshot buys is not a number: the dump is of one graph.
Reading through the store, each of the export's reads saw whatever was there
when it ran.

#### The property walk had to be sorted first, and the sort paid for itself

The export above was not reproducible. `PropertyIndex.ForEachNodeProperty` — the
streaming form of `NodeEntries`, and what writes a dump's property section —
ranged the per-value map directly, so two exports of one unchanged graph
disagreed about the order of their property lines about one run in seven on a
key with 200 distinct values. `NodeEntries` has always sorted, and its comment
explains at length why the order is a contract; the streaming form was written
without it. Found by the end-to-end gate, pinned by
`TestPropertyIndex_StreamingWalkMatchesNodeEntries` and
`TestScan_ExportIsReproducible`, both of which repeat because one agreeing run
proves nothing against a randomised map.

Sorting costs a slice of each key's distinct values. Reusing one buffer across
keys is not enough on its own — the first key grows it by doubling and pays
about twice what it ends up holding — so `sortedBucketValues` now sizes to the
bucket up front. That is a net win, because `NodeEntries` and `EdgeEntries`
already shared the helper and were doing the doubling too:

| Benchmark | B/op before | B/op after | allocs/op |
|---|---:|---:|---:|
| `NodeEntries/nodes=10000` | 1 950 514 | 1 467 033 | 20 014 → 20 005 |
| `NodeEntries/nodes=50000` | 10 742 601 | 7 219 813 | 100 021 → 100 005 |

**33% fewer bytes on the larger case**, on the path `Compact` uses to write the
CSR index section. The export's own numbers above are measured after this
change; the sort adds about 1.6 MB to a 366 MB export, which the sizing pays
back several times over elsewhere.

## Program baselines — memory optimisation program (2026-09-09)

|              |                                                                                  |
| ------------ | -------------------------------------------------------------------------------- |
| **Date**     | 2026-09-09                                                                       |
| **OS**       | Windows 11 (amd64), page size 4096, NVMe SSD                                     |
| **Go**       | go1.26                                                                           |
| **Hardware** | AMD Ryzen 9 5980HS, 16 cores                                                     |
| **Baseline** | git `a54b9aa` plus Phase 0 instruments only — no engine code changed             |
| **Suites**   | [rss](../tests/rss_bench_test.go) · [footprint](../tests/graphene_footprint_test.go) · [guard](../tests/footprint_guard_test.go) |
| **Method**   | One fixture size per process; `-benchtime=1x -count=3`; medians reported          |
| **Raw**      | [baseline-2026-09-09.txt](benchmarks/baseline-2026-09-09.txt)                     |

### Why a new kind of measurement

Every footprint number above this section is `runtime.MemStats.HeapAlloc`: the
Go heap a graph retains. That is the right question for the optimisation work
recorded above it and the wrong one for a budget expressed in machine RAM. It
cannot see a memory-mapped image at all — those bytes are resident, and are not
Go heap — it cannot see the runtime's own arenas and GC headroom, and it cannot
see page cache. A change that moved bytes out of the heap and into a mapping
would show as a large win on every existing number while the process occupied
exactly as much RAM as before.

So these baselines read the operating system's accounting instead, and keep two
classes apart: **anonymous** pages, which are private, swap-backed, and what a
RAM ceiling actually constrains, and **file-backed** pages, which are a view of
something on disk and which the kernel can evict without swap. Reporting one
combined figure would make a mapping change indistinguishable from no change.

The instrument is calibrated before it is believed — see
`TestRSSInstrument_SeparatesFileBackedResidency`, which maps and touches 128 MiB
and fails if the reading does not land in the file class. A reader blind to
mapped pages would veto the mapped-image design on its own blind spot.

**Never measure residency under `-race`.** The detector allocates shadow memory
for every byte the program touches, and that shadow is ordinary private
anonymous memory — touching a 128 MiB mapping produces about 128 MiB of
anonymous growth beside the file-backed growth, and no accounting separates the
mapping from its shadow. The calibration test skips itself under the detector
for exactly this reason. The race detector belongs on the concurrency tests, not
on these.

### The baseline, 50k nodes, consumer index shape

Thirteen index entries per node — eight unique keys, one of them an all-distinct
32-byte digest, five ordered, two composites — because at that shape the
property index, not the graph, is the dominant resident term.

| Operation | resident | anon | file | Go heap | on disk |
| --- | ---: | ---: | ---: | ---: | ---: |
| Open | 177.4 MiB | 177.4 | 0 | 118.2 | 44.0 MiB |
| Scan, ten rows | 177.8 MiB | 177.8 | 0 | 118.2 | — |
| Bulk write | 185.6 MiB | 185.6 | 0 | 120.3 | — |
| Compact, steady state | 187.2 MiB | 187.2 | 0 | 118.4 | — |
| Compact, transient peak | 460.0 MiB | — | — | — | — |

Four things follow, and they reorder the work:

**Resident is ~4.0× the store on disk.** The consumer that prompted this
measured 4,871 MiB against a 1.2 GiB store — 4.06×. The fixture reproduces the
reported ratio, so what is being optimised here is what was reported.

**A ten-row read costs the same as opening.** `Scan` and `Open` agree to within
0.3 MiB. The whole figure is paid at Open, and effort spent making reads cheaper
is aimed at the wrong term.

**Nothing is file-backed.** `fileMiB` is zero on every row: the entire resident
cost today is anonymous memory the kernel cannot evict. That is the headroom a
mapped image has to work with.

**Compaction's transient peak is +270 MiB over steady state on a 44 MiB store**
— 6.1× the store, held for the duration, sampled at 1 ms over ~780 samples.

### The identifier high-water mark

The cost that no estimate based on file size predicts. Each cycle deletes every
node the previous cycle wrote, writes the same number of fresh ones, and
compacts; the live set is identical at the end of every cycle, so anything that
accumulates is the cost of identifiers that were issued and can never be reused.

| cycle | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| resident MiB | 108.4 | 111.1 | 112.7 | 113.8 | 116.2 | 119.6 | 122.0 | 122.9 | 124.5 | 126.1 | 127.8 | 129.3 | 131.1 |

Straight line, 20,000 nodes burned per cycle. It does not flatten, and the tail
slope over the second half (1.507 MiB/cycle) is lower than the average
(1.870), so this is a per-cycle cost rather than a one-off paid at the first
rebuild — a distinction two endpoints cannot make, which is why the benchmark
reports the whole series. **79.0 B of resident memory per burned identifier.**

At 1.5M nodes per rebuild that is ~119 MiB added permanently per rebuild, on top
of a live set that has not grown.

### Compaction recovers this only if you delete the high half

Same 50,000 live nodes, same live bytes, compacted in both arms. Only the
position of the surviving identifiers differs.

| deletion shape | B/node | MiB total |
| --- | ---: | ---: |
| low half — a derived-layer rebuild | 530.8 | 25.31 |
| high half — what the numbers above measured | 378.8 | 18.06 |

7.26 MiB apart for 50,000 burned identifiers: **152 B each**, decomposing
exactly as 72 B for the node slot plus 80 B for the edge slot the delete cascade
burned with it. Spread across three runs is under 0.15%.

The [Memory footprint](#memory-footprint-p1) figure earlier in this document — 4.5×
until `Compact` — deleted the *high* half, where the maximum identifier falls
with the deletion and the dense arrays can shrink. A rebuild deletes the *low*
half and writes above it, so the maximum only rises and compaction recovers none
of it. The two measurements do not contradict each other; they measure different
deletion shapes, and only one of them is the workload.

### Reproducing

```sh
make rssbench                               # all five, default 50k
make rssbench FILTER=RSS_Open RSS_NODES=1500000
make rssbench FILTER=RSS_RebuildCycle RSS_CYCLES=40 RSS_COUNT=1
make heapprofile FILTER=BenchmarkRSS_Open   # what is retained, by call site
make cpuprofile FILTER=BenchmarkRSS_Open    # where the time goes
```

One fixture size per process, always: a process that builds several large
fixtures reports the high-water mark of the largest, and the peak figures then
say nothing about the smaller ones.

## Pre-open cost query and the replay budget (2026-09-10)

Method as §1 of `CONTRIBUTING.md`: three interleaved rounds, alternating the
working tree against a pristine `HEAD` tree, one fixture per process, medians
reported with the full spread. Same machine block as the program baselines above.

### What the change touches, and what it does not

`PreflightOpen` is new surface on no existing path, and the two `Options` fields
are inert at their zero value — `checkReplayBudget` returns on its first branch.
The one change on a measured path is replay's batch reservation, so that is what
the arms below are chosen to exercise. Both fixtures write through the bulk
writers, which batch in chunks of ten thousand, so both do reach it.

### `BenchmarkColdOpen_UncompactedWAL_10k` — wall clock and allocation

| | ns/op (median) | B/op (median) | allocs/op (median) |
| --- | ---: | ---: | ---: |
| HEAD | 33 038 200 | 25 071 216 | 378 181 |
| with the change | 33 044 630 | 25 072 080 | 378 182 |
| delta | +0.02% | **+864 B, +0.003%** | **+1** |

Spreads overlap on all three: 31.4–34.0 ms against 32.9–34.9 ms on wall clock,
and under 2 KB of range on bytes in both arms.

### `BenchmarkRSS_Open_UncompactedWAL` — residency, 50k nodes

| | anon MiB | file MiB | heap MiB | peak MiB |
| --- | ---: | ---: | ---: | ---: |
| HEAD | 181.2 (179.6–181.7) | 0 | 120.2 | 260.8 (250.1–271.4) |
| with the change | 181.5 (181.4–182.4) | 0 | 120.2 | 281.5 (269.1–293.7) |

Anonymous residency moves 0.3 MiB, inside `HEAD`'s own 2.1 MiB spread. The live
heap figure is identical to the tenth of a MiB in all six runs. Nothing is
file-backed on either side, which is unchanged and expected — no mapping exists
yet.

**The peak column says nothing and is printed anyway.** Its ranges overlap
(269.1 against 271.4) and the ordering between the arms *reversed* between two
sessions of this same A/B — an earlier set put `HEAD` at 286.7 and the change at
266.4. The sampler ticks at 1 ms against an open lasting a few hundred, so three
rounds cannot resolve a difference this size. Recorded rather than dropped,
because a peak figure quoted without its spread would have supported either
conclusion.

### The measurement that chose a constant

The batch reservation cap was 4096 first. At that value the same cold-open A/B
read **+3.15 MB/op and +11 allocs/op** — consistently, across three rounds with a
spread under 1 KB. A cap set below the batch sizes the engine actually writes
does not bound memory, it *costs* memory: it replaces one exact allocation with a
geometric growth sequence whose total is several times larger. The bulk writers
batch in chunks of ten thousand, so the cap has to sit above them. At 65 536 the
regression is gone and a corrupt marker is still bounded by a constant.

Worth generalising: **a bound that is not measured against the workload it bounds
can be a regression wearing the shape of a safety fix.** Both numbers were
available for the same three rounds of work.

### Reproducing

```sh
git archive HEAD | tar -x -C /tmp/head-control     # a control tree, no worktree needed
for i in 1 2 3; do
  ( cd . && go test ./tests/ -tags=stress -run='^$' \
      -bench=BenchmarkColdOpen_UncompactedWAL_10k -benchmem -benchtime=10x -count=1 )
  ( cd /tmp/head-control && go test ./tests/ -tags=stress -run='^$' \
      -bench=BenchmarkColdOpen_UncompactedWAL_10k -benchmem -benchtime=10x -count=1 )
done
```

The bounded-memory claim has its own guard rather than a benchmark:
`TestPreflightOpen_MemoryDoesNotFollowTheLog` fits an allocation slope across
6 000 and 60 000 record logs and reads **0.00 B/record**. Both fixtures are
deliberately past the read buffer's cap — below it the buffer is sized to the log,
and a smaller pair measures the buffer growing rather than the walk retaining.

## Backup, its digest leg, and the residency of a restore (2026-09-11)

Method as §1 of `CONTRIBUTING.md`: three interleaved rounds, alternating the
working tree against a pristine `9e1db48` tree (the commit before the change),
one fixture per process, medians reported with the full spread. Same machine
block as the program baselines above. The fixture is the residency suite's own —
50 000 nodes, 512-byte blobs, the consumer index shape — which compacts to a
**44.01 MiB** image, so every row below can be read against the same store.

### What the change touches, and what it does not

`VerifyCSRDigest` hashes the image as it reads it instead of reading it whole.
The whole-buffer form `VerifyOnOpen` uses on an image already in hand is
unchanged in what it computes, and the two now share one definition of the
covered bytes. On a measured store path the only caller is the final check of
every backup; `store csr -verify`, `debug hash-check` and `debug integrity`
share the leg. No read path and no write path is touched, so the point-lookup
control is expected to read as noise, and does.

### `BenchmarkRSS_Restore` — residency, 50k nodes, 44 MiB store

Peak residency is sampled on a 1 ms ticker during the backup and again during
the restore, each read against the settled residency just before it; the store
stays open across both so the deltas are measured against a heap that cannot
have shrunk for unrelated reasons.

| | anon MiB | backup peak MiB | backup Δ MiB | restore Δ MiB | heap MiB | B/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `9e1db48` | 176.9 (176.7–180.4) | 221.5 (221.4–224.6) | **44.30** (44.29–44.39) | 0.23 (0.10–0.30) | 118.2 | 46 613 632 |
| with the change | 176.3 (176.1–176.9) | 176.7 (176.3–177.2) | **0.22** (0.18–0.25) | 0.26 (0.25–0.35) | 118.2 | 489 560 |

The backup delta on the control tree *is* the image: 44.30 MiB of growth
against a 44.01 MiB file, in all three rounds, with a spread of a tenth of a
MiB. That is the whole-file read behind the copy's digest check, and it is what
the plan had recorded as "verified streaming" from a reading of the code. The
restore column is unchanged and was streaming all along — the restore is what
the benchmark was named for, and the backup half is what it found. Steady-state
residency and the live heap are identical to the tenth of a MiB on both sides;
nothing is retained. The process-lifetime `peakMiB` column (499.9–603.8 on
both arms) reports the fixture build and is omitted, as the benchmark's own
comment says to.

### `BenchmarkBackup_Quiescent` — wall clock and allocation

A backup of the same store with nothing else running, ten per iteration set,
the destination removed between them. This is the other half of the question
`BenchmarkBackupCost` asks: not what a writer pays during a backup, but what
the backup itself allocates.

| | ms/op (median) | B/op (median) | allocs/op (median) |
| --- | ---: | ---: | ---: |
| `9e1db48` | 220.2 (194.2–308.6) | 46 243 476 | 139 |
| with the change | 219.4 (187.8–219.6) | 119 390 | 132 |
| delta | inside spread | **−46.12 MB, −99.7%** | −7 |

### `BenchmarkVerifyCSRDigest` — the leg on its own

| | ms/op (median) | B/op (median) | allocs/op (median) |
| --- | ---: | ---: | ---: |
| `9e1db48` | 54.8 (49.7–77.0) | 46 156 782 | 14 |
| with the change | 48.5 (47.8–68.2) | 34 112 | 13 |
| delta | −11% at the median, spreads overlap | **−46.12 MB, −99.93%** | −1 |

What remains is one 32 KiB copy buffer and the hash state, and it does not move
with the file: 34 089–34 134 bytes across the three rounds against a 44 MiB
image. The wall-clock ordering favoured the streamed form in every round, which
is the expected direction — the SHA-256 work is identical, and a buffered read
has no file-sized allocation to fault in — but the spreads overlap and it is
reported as a wash. Under the program's priority rule this change would have
been taken at a wall-clock cost; it did not need one.

Round 3 of both benchmarks was disturbed on both arms (308 ms and 77 ms on the
control, 68 ms on the change) and is left in the spreads rather than re-run:
the memory columns it contributed to are within 15 KB of the other two rounds
on the change side and within 13 KB on the control side.

### The control

`BenchmarkPointLookupNode_Memory`, which this change cannot reach. A first set
at `-benchtime=1s`, taken while the machine was busy, spread 46–107 ns on the
change against 25–78 ns on the control — overlapping, and useless. Re-run at
`-benchtime=2s` once the machine was quiet:

| | ns/op (median) |
| --- | ---: |
| `9e1db48` | 25.20 (24.67–26.28) |
| with the change | 25.10 (24.60–27.65) |

Both sets are recorded because a control that had been quietly dropped for
reading badly would not be a control.

### Reproducing

```sh
git archive 9e1db48 | tar -x -C /tmp/control                      # a control tree, no worktree needed
cp tests/rss_bench_test.go tests/graphene_backup_bench_test.go /tmp/control/tests/   # same instrument, both engines
for i in 1 2 3; do
  for tree in . /tmp/control; do
    ( cd $tree && go test ./tests/ -tags=stress -run='^$' \
        -bench='^(BenchmarkBackup_Quiescent|BenchmarkVerifyCSRDigest)$' -benchmem -benchtime=10x -count=1 )
    ( cd $tree && go test ./tests/ -tags=stress -run='^$' \
        -bench='^BenchmarkRSS_Restore$' -benchtime=1x -count=1 )
  done
done
```

The streaming claim has a guard rather than a benchmark:
`TestVerifyCSRDigest_DoesNotReadTheImageWhole` bounds
`runtime.MemStats.TotalAlloc` across one call at 1 MiB against an image it
refuses to run on unless it is at least four times that. Reintroducing the
whole read fails it and no other test; the thirteen-case differential table
and `FuzzCSRDigestStream` (2.36 M executions, no divergence) are what hold the
streamed and whole-buffer digests to the same bytes.
