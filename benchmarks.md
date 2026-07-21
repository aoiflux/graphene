# Benchmarks

Measurements for the indexing, durability and traversal work tracked in
[plan.md](plan.md). Optimisation priority is **P0 speed → P1 memory/space →
P2 allocations**.

## How these were produced

| | |
|---|---|
| **Date** | 2026-07-21 |
| **OS** | Windows 11 (amd64) |
| **Go** | go1.26.2 |
| **Hardware** | AMD Ryzen 9 5980HS, 16 cores |
| **Baseline** | git `036aac0`, the commit before this work began |
| **Suites** | [bench](graphene_bench_test.go) · [parallel](graphene_parallel_bench_test.go) · [coverage](graphene_coverage_bench_test.go) · [footprint](graphene_footprint_test.go) |
| **Method** | Baseline and current run **interleaved** — alternating rounds of `-count=2` — then compared with `benchstat`, n=6 per side |

68 benchmarks, up from the 29 this work started with and the 5 the project had
before that.

### Two rounds discarded, and why the resolution limit is ~25%

The final comparison ran four interleaved rounds after a cooldown. **Round four
was dropped from both sides**, symmetrically: its samples on the new side roughly
doubled (`GetNode` 6.1 ns → 11.5 ns) while rounds one to three matched the
baseline closely. It ran last, after ~50 minutes of continuous benchmarking, and
the new side is the longer of the two because it carries benchmarks the baseline
has no equivalent for — so it absorbed peak thermal load. That is the machine,
not the code, and keeping a round measured in a demonstrably different state
would have inflated every figure in the same direction.

**The two controls then disagreed**, and that sets an honest floor on what this
data can resolve. `GetNode` came out flat (p=0.288) with tight variance, which is
correct — it is byte-identical on both sides. But `PointLookupNode_Memory`, also
byte-identical, reported −24% (p=0.015) off ±25% variance on the base side. A
statistically significant improvement on code that did not change is a false
positive, so:

> **Timing effects below roughly 25% are not resolvable in this dataset.** The
> order-of-magnitude results are safe; anything in the tens of percent should be
> read as directional.

The footprint numbers are exempt. They are deterministic measurements of resident
bytes, reported at ±0% variance, and they do not depend on how warm the machine
was.

### Why interleaving, demonstrated

An earlier attempt ran the two suites back to back and the second came out ~25%
slower across the board — including benchmarks the change never touched. That was
the machine throttling over a long run. Alternating the two sides spreads that
drift evenly, and the controls below confirm it worked.

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

| Benchmark | Published (`8a9a89b`) | Baseline today (`036aac0`) | Verdict |
|---|---|---|---|
| BFS | 3 058 allocs | 3 058 allocs | same code path |
| Shortest path | 2 061 allocs | 1 563 allocs | **code changed** |

**And the measurement conditions moved.** BFS is the useful accident here: it
does provably identical work in both, so its timing gap is *pure* session drift.

| Benchmark | Published | Same code, today | Drift |
|---|---:|---:|---:|
| BFS traversal | 475 381 ns | 396 100 ns | **−17%** |
| Get node | 6.719 ns | 5.838 ns | **−13%** |
| Add node | 831.5 ns | 748.2 ns | **−10%** |
| Property index lookup | 55.19 ns | 42.23 ns | **−23%** |

So **10–23% of any apparent improvement measured against the published table
would be artefact**, on a path where the code is provably unchanged. Everything
below is measured against `036aac0` run today, interleaved.

### Fixture

100 000 nodes, ~201 000 edges, 300 000 indexed property entries. Labels: `Case`
on every 1000th node (100, highly selective), `EvidenceFile` on every 10th
(9 900), `MicroArtefact` on the rest. Properties: `sha256` (unique), `bucket`
(1 000 distinct), `score` (1 000 distinct). Topology: chain + `+13` stride, plus
a hub node with 1 000 inbound edges. The disk fixture is `Compact()`ed first.

---

## Results

**Overall geomean: −86.4%.**

### Query planning

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Equality property query (disk) | 51.83 ms | **550.6 ns** | −100.00% (~94 000×) |
| Equality property query (memory) | 40.45 ms | **593.1 ns** | −100.00% (~68 000×) |
| Type + equality property query | 58.09 ms | **20.81 µs** | −99.96% |
| Typed query, `Limit: 10` (memory) | 10.95 ms | **11.58 µs** | −99.89% |
| Typed query, `Limit: 10` (disk) | 17.16 ms | **20.83 µs** | −99.88% |
| Edge query by type (memory) | 44.49 ms | **157.9 µs** | −99.65% |
| Relation query, both directions (disk) | 64.99 ms | **262.4 µs** | −99.60% |
| Anchored relation query (disk) | 32.56 ms | **215.8 µs** | −99.34% |
| Edge query by type (disk) | 38.18 ms | **264.2 µs** | −99.31% |
| Anchored relation query (memory) | 19.25 ms | **171.4 µs** | −99.11% |

### Residual filters — the second filter used to cost a scan

Driving a query from the most selective index is only half the job. The filters
that did *not* drive it still have to be applied, and resolving each to its own
set means a filter no index can serve — a `Contains`, or a range on a key never
declared ordered — scans every entry under its key. A query driven down to a
single candidate was still doing work proportional to the whole graph.

Costing each residual both ways and probing the candidates when that is cheaper:

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Equality + `Contains` (memory) | 12.97 ms | **443.1 ns** | −100.00% (~29 000×) |
| Equality + `Contains` (disk) | 12.94 ms | **429.2 ns** | −100.00% (~30 000×) |
| — allocated bytes | 4.10 MB | **304 B** | −99.99% |
| Two equality filters (memory) | 717.1 ns | **567.9 ns** | −20.8% (allocs −64%) |
| Single equality filter (memory) | 324.9 ns | **254.8 ns** | −21.6% |
| Single equality filter (disk) | 308.6 ns | **243.2 ns** | −21.2% |
| *Control:* point lookup (memory) | 23.42 ns | 23.23 ns | ~ (p=0.198) |

The single-filter rows were not the target. The driving filter used to be
re-resolved to its own set and intersected against the candidates it had itself
produced; skipping it is where that 21% comes from.

**This first shipped as a regression, in the same run that produced the 29 000×.**
Single-filter queries measured +14% (memory) and +23% (disk) with +70%
allocations, because they still built a residual plan — allocating a slice and
consulting the index to establish there was nothing to do. A short-circuit for
"the driver consumed every filter" fixed it and turned the row into the win
above. It is recorded here because the headline number and the regression came
out of the same benchmark run, and only a set wide enough to include the boring
case would have caught it.

#### The edge path, and a run where the control failed

The same evaluation on edge queries, measured separately because the edge half
had been written and left unreachable — only the node path was wired into the
stores, and a method nobody calls draws no complaint from the compiler, `go vet`,
or any test:

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Edge equality + `Contains` (memory) | 2.357 ms | **575.5 ns** | −99.98% (~4 100×) |

**That run's control moved**, and it is worth saying so rather than quoting the
row alone. `PointLookupNode_Memory` shifted +23.7% (p=0.001) with ±17–31%
variance, because the race and stress suites had finished moments earlier and the
machine was still hot. Two conclusions follow, and they differ:

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

| Benchmark | Unfiltered | Filtered | Change |
|---|---:|---:|---:|
| Raw single-key lookup (memory) | 70.60 ns | **91.10 ns** | +29.0% (p=0.000) |
| Typed equality query (memory) | 380.6 ns | 406.9 ns | ~ (p=0.089) |
| Typed equality query (disk) | 420.8 ns | 352.9 ns | ~ |
| *Control:* point lookup (memory) | 28.73 ns | 31.05 ns | ~ (p=0.713) |

Twenty nanoseconds on the raw lookup, nothing measurable on the typed query path
— which already resolved its candidates that way — and no change in allocations,
since the filter runs in place over a slice the index had already copied. That
buys a guarantee that can be stated in a sentence: *every ID returned named an
entity that was live at the moment it was checked.*

### Scale sweep — does cost track the answer, or the graph?

The clearest evidence the queries are index-served rather than scanning. Same
operation, 10× the data:

| Benchmark | 10k nodes | 100k nodes | Slope |
|---|---:|---:|---|
| Equality query — **before** | 2.907 ms | 41.375 ms | **14×** (linear scan) |
| Equality query — **after** | 704.5 ns | **590.5 ns** | **flat** |
| Type query, `Limit: 10` — before | 517.9 µs | 9 487 µs | 18× |
| Type query, `Limit: 10` — after | 1.106 µs | 11.92 µs | 10.8× ¹ |
| Point lookup — before / after | 18.25 / 17.57 ns | 25.62 / 25.79 ns | flat both |

¹ Still climbs, and correctly so: the `Case` label has 10× more members at 100k,
so the result itself is 10× bigger. Cost tracks the answer, which is the goal.

### Label index

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `NodesByType`, selective label (disk) | 400.9 µs | **4.900 µs** | −98.78% |
| `NodesByType`, selective label (memory) | 220.4 ns | 216.5 ns | ~ |

### Ordered (range) index

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Prefix query (memory) | 45.67 ms | **7.656 ms** | −83.24% |
| Range query (memory) | 60.39 ms | **20.00 ms** | −66.88% |
| Range query (disk) | 66.72 ms | **29.11 ms** | −56.37% |

Those are the *undeclared* figures — that improvement comes from confining the
scan to one key's buckets. With the key declared ordered, measured separately
against the same data:

| Query | Scan | Ordered | Change |
|---|---:|---:|---:|
| Range over 1 000 values (memory) | 17.06 ms | **2.500 ms** | 6.8× |
| Range over 1 000 values (disk) | 22.84 ms | **2.310 ms** | 9.9× |
| Prefix over 1 000 values | 6.129 ms | **2.495 ms** | 2.5× |
| **Narrow range — 3 of 1 000 values** | 11.76 ms | **59.01 µs** | **199×** |

The narrow-range row is the one that matters: a scan costs the same however
selective the predicate is, while an ordered index costs what the answer costs.
Building the index over an already-populated store is a one-off 1.87 ms per key
at 20 000 nodes.

### Degree and mutation

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Hub in-degree (disk) | 70.95 µs | **14.22 ns** | −99.98% (~4 990×) |
| Hub in-degree (memory) | 18.89 µs | **27.57 ns** | −99.85% |
| `DeleteNode` with a populated property index | 1.201 ms | **1.589 µs** | −99.87% (~756×) |
| `UpdateNode` relabelling within a 50k label | 50.43 µs | **4.435 µs** | −91.20% |
| `DeleteNode` from a 50k-member label | 27.98 µs | **3.732 µs** | −86.66% |
| `DeleteNode` from a 10k-member label | 4.139 µs | **787.4 ns** | −80.98% |

### Durability

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Reopen a compacted store (50k nodes) | 1 176.7 ms | **60.58 ms** | −94.85% |
| Compaction, steady state | 652.8 ms | **50.70 ms** | −92.23% |
| End-to-end disk ingest, 10k nodes | 556.1 ms | **433.5 ms** | −22.05% |

The property index now lives in the CSR file, so `Compact()` leaves the WAL
**empty** and restart cost no longer grows with index size.

### Traversal — allocation is the metric

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `BFS_Deep` — 10k-node chain | 30 190 allocs | **216** | −99.28% |
| `Walk_DFS_Deep` — 10k-node chain | 20 193 allocs | **196** | −99.03% |
| `Walk_ShortestPath_Disk` | 7 670 allocs | **138** | −98.20% |
| `BFS` — 1k-node chain | 3 058 allocs | **77** | −97.48% |
| `BFS_Wide` — 100×100 fan-out | 1 323 allocs | **232** | −82.46% |
| `ShortestPath` | 1 563 allocs | **576** | −63.15% |
| `BFS_Disk_Deep` | 913 allocs | **394** | −56.85% |
| `Scale_BFS4Hop_100k` | 47 allocs | **30** | −36.17% |
| `BFS3Hop_Disk` | 70 allocs | **47** | −32.86% |

Wall-clock followed: `Connect_IsConnected_Near` **−70.9%** (a bidirectional
search that now materialises records only for the final path), `BFS3Hop_Disk`
−18.2%, `BFS_Disk_Deep` −15.4%, `ShortestPath` −13.0%, `BFS` −10.8%,
`BFS_Wide` −7.0%.

`BFSIDs` walks without building a single record — no baseline, the API is new:
20 allocations and 36.3 µs on the disk fixture, against 394 and 113.9 µs for the
record-returning equivalent.

### Memory footprint (P1)

`B/op` is bytes *allocated during* an operation, which says nothing about what a
loaded graph *occupies*. These build the graph, GC twice, then read `HeapAlloc`.

100 000 nodes, ~200 000 edges:

| Configuration | Bytes/node | Bytes/edge | Total |
|---|---:|---:|---:|
| In-memory, topology only | 447 B | 223 B | 42.6 MiB |
| On-disk (CSR), topology only | 298 B | 149 B | 28.4 MiB |
| In-memory + property index (3 keys/node) | 767 B | 384 B | 73.2 MiB |
| On-disk + property index | 620 B | 310 B | 59.2 MiB |
| + one ordered key declared | 778 B | 389 B | 74.2 MiB |
| **On-disk file** | 223 B | 111 B | 21.3 MiB (WAL 0) |

- **The property index costs ~107 bytes per entry** — a lot for an 8-byte ID and
  a short value, and the strongest lead for Phase 8: the sorted postings, the
  reverse `ID → [(key,value)]` map, and the value strings each hold a copy.
- **An ordered key costs 10.5 B/node**, not the "~2× that key's index" an earlier
  note claimed. That claim was wrong: cost scales with *distinct values*, not
  entries, and `score` has 1 000 distinct values across 100 000 nodes.
- **The CSR is ~33% more compact in RAM** than the in-memory backend, and its
  on-disk form smaller again.

Deletions the CSR has not reclaimed — its arrays are sized by the highest ID ever
issued, not the live count:

| State | Bytes per live node | Total |
|---|---:|---:|
| Half deleted, uncompacted | 715 B | 34.1 MiB |
| Half deleted, compacted | 158 B | 7.5 MiB |

**4.5× the memory per live node** until `Compact()` runs. Documented behaviour,
never before quantified.

---

## Regressions

Reported in full. Each is a trade, and the axis that won is named.

### The one that matters most: property-index memory, +30–65%

This is a **P1 regression**, it is not noise, and it is the price of the P0 wins
above.

| Footprint (B/node) | Before | After | Change |
|---|---:|---:|---:|
| *Topology only, memory* | 446.1 | 446.2 | **+0.02%** |
| *No property index* | 170.1 | 170.2 | **+0.06%** |
| Memory store + property index | 563.8 | **843.2** | +49.56% |
| Disk store + property index | 388.4 | **693.0** | +78.42% |
| Index at cardinality 1 | 179.0 | **295.6** | +65.14% |
| Index at cardinality 100 | 180.5 | 297.0 | +64.54% |
| Index at cardinality 10 000 | 194.0 | 307.4 | +58.45% |
| Index, all values distinct | 281.1 | 365.7 | +30.10% |
| On-disk file size | 248.0 | **223.0** | −10.08% |

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
are *shared* (+65% at cardinality 1) and mildest where every value is distinct
(+30%). At low cardinality the forward map is tiny while the reverse map still
holds one entry per entity, so the reverse map dominates the ratio.

**What would recover it**, in the order worth trying: the value bucket layout is
`map[string][]ID` per shard, and the fixed overhead of a Go map header repeated
across 16 shards × N keys is the largest single component — that is the map
layout, not the postings. Compressing the postings lists themselves is the
obvious idea and the wrong one; they are 8 B of a 125 B floor (see the
cardinality sweep above), so the ceiling on that work is ~5%.

### Latency and allocation

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `NodesByProperty_Equal_Memory` | 49.15 ns | 78.31 ns | **+59.3%** |
| `PropertyIndexLookup` | 46.65 ns | 71.18 ns | **+52.6%** |
| `Parallel_IndexNodeProperty_DistinctKeys` bytes | 217.5 B | 427.0 B | **+96.3%** |
| `Parallel_IndexNodeProperty_SameKey` bytes | 199.5 B | 376.5 B | **+88.7%** |
| `ReopenCompactedStore` bytes | 33.89 MiB | 55.29 MiB | **+63.1%** |

**The raw single-key lookup, +52–59%.** Two costs, both deliberate. The larger is
read consistency: postings are now resolved against the records before being
returned, because the index and the records are separate structures under
separate locks and a lookup consulting only the index could hand back an entity a
concurrent delete had already removed. An isolated A/B put that at +29% on its
own (70.6 → 91.1 ns). The remainder is sharding, which added a hash per lookup to
buy the concurrency win. The absolute figure is ~78 ns against a path that used
to be a 40 ms scan.

**Property-index registration allocates roughly twice as much.** The reverse map
entry per registration. This is the same purchase as the memory row above, seen
on the write path.

**Reopen allocates 63% more** while running 94% faster, because the index now
arrives as one file read rather than streaming from the log. Peak memory during
open is the cost; restart latency is what it bought.

### Previously reported regressions, now fixed

Recorded because they were published as regressions and should not stay that way:

| Benchmark | Was | Now |
|---|---:|---:|
| `Parallel_BFS3Hop_Disk` | +35.1% | **−34.8%** (lock-free CSR reads) |
| `Parallel_IndexNodeProperty_DistinctKeys` | +18.5% | **−57.9%** (key sharding) |
| `BFS_Deep` bytes | +13.0% | **−18.1%** (two-buffer BFS) |
| `Ingest_AddNodes_Batch1000` | +10.2% | not significant |

---

## What is still slow

- **Pattern matching is now the worst path in the codebase.** `FindPatterns` over
  a 2 000-node scope: **23.5 ms and 399 900 allocations**, unchanged by any of
  this work — roughly 200 allocations per scoped node. It is the single largest
  allocation source left.
- **Cold open on an uncompacted WAL: 727 ms for 10 000 nodes**, against 60.6 ms
  to reopen a *compacted* 50 000-node store. Phase 4 fixed the compacted path;
  replay of an uncompacted log was never optimised and is ~60× worse per node.
- **`Contains` filters are a scan and will stay one.** No ordering can bound a
  substring match; a trigram index is the only route and is out of scope.
- **Range and prefix are only fast on *declared* keys.** An undeclared key still
  scans its buckets — better than the original full-index materialisation, but
  linear.
- **Property-index memory, ~107 B/entry.** Three copies of each ID plus the value
  strings. The largest P1 lead.
- **Edge property blobs are cloned on every materialisation.** `rawEdgeToStore`
  copies the blob whenever a CSR edge becomes a `*store.Edge`, and traversal
  never reads it — costs both time and heap.

## Reproducing

```powershell
./test.ps1 -Bench                                    # default 5s benchtime
go test . -tags=stress -bench=. -benchmem -count=6 -run='^$'
go test . -tags=stress -bench=Footprint -benchtime=1x -run='^$'
go test . -tags=stress -bench=Parallel -cpu=1,2,4,8,16 -run='^$'
```

Sweep the fixture size with `GRAPHENE_BENCH_NODES` (default 100 000).
