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

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `Parallel_BFS3Hop_Disk` | 1.078 µs | 1.456 µs | **+35.1%** |
| `Parallel_IndexNodeProperty_DistinctKeys` | 1.003 µs | 1.188 µs | **+18.5%** |
| `Parallel_AddNode_Memory` | 775.1 ns | 860.5 ns | **+11.0%** |
| `Ingest_AddNodes_Batch1000` | 572.7 µs | 631.3 µs | **+10.2%** |
| `Ingest_AddEdge_Single` | 342.3 ns | 376.2 ns | **+9.9%** |
| `Ingest_AddNodes_Batch100` | 61.15 µs | 63.81 µs | **+4.4%** |
| `Parallel_MixedReadWrite_Memory` | 646.5 ns | 671.8 ns | **+3.9%** |
| `ColdOpen_UncompactedWAL_10k` bytes | 12.07 MiB | 15.45 MiB | **+28.0%** |
| `BFS_Deep` bytes | 2.102 MiB | 2.374 MiB | **+13.0%** |

**Parallel BFS on disk, +35%** — the most interesting one, and only visible
because the concurrency benchmarks are new. Single-threaded the same walk is
−18%; contended it is worse. The walker takes more, smaller store-lock
acquisitions per expansion (`IncidentEdges`, then `GetNode`/`GetEdge` per kept
record) than the single `Neighbours` call it replaced. Uncontended those locks
are nearly free; contended, each is cache-line traffic between cores. The fix is
to batch record resolution behind one lock hold — now an open Phase 9 item.

**Write path, +4–10%** — the cost of keeping label postings sorted, which is what
made deletes 5–11× cheaper. Two rounds of fixing brought it down from an initial
+12–30%: an append fast path for the monotonic-ID case (the insert position is
almost always the end, so the binary search was pure waste), then inlining that
branch at the call sites because the generic helper would not inline. Single
`AddNode` is back to parity; batch ingest and `AddEdge` retain ~10%.

**Property-index registration under contention, +18%** — sorted insert plus the
reverse map. `PropertyIndex` still holds one global `RWMutex`, so the same-key
and distinct-key cases perform alike; sharding by key hash is the open item, and
these two benchmarks exist to measure it.

**Bytes on cold open and deep BFS** — the reverse map during WAL replay, and the
BFS queue retaining consumed entries (the change that bought 140× fewer
allocations). Both spend P1 to buy P0, which the priority ordering endorses.

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
