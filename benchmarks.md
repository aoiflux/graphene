# Benchmarks

Read-path measurements for the indexing work tracked in [plan.md](plan.md).

## How these were produced

| | |
|---|---|
| **Date** | 2026-07-21 |
| **OS** | Windows 11 (amd64) |
| **Go** | go1.26.2 |
| **Hardware** | AMD Ryzen 9 5980HS, 16 cores |
| **Suite** | [graphene_bench_test.go](graphene_bench_test.go) |
| **Command** | `go test . -tags=stress -bench=. -benchmem -benchtime=1s -count=2 -run='^$'` |
| **Method** | Baseline (git `036aac0`) and current tree run **interleaved**, alternating rounds of `-count=2`, then compared with `benchstat` (n=4–6 per side) |

The interleaving matters. A first attempt ran the two suites back to back, and the
second suite came out ~25% slower across the board — including `GetNode`, which
none of the changes touch. That was the machine thermally throttling over a long
run, not a code change. Alternating the two sides spreads that drift evenly, and
the control benchmarks below confirm it worked: everything untouched now reports
"no significant change".

Absolute numbers are therefore from a warm, loaded laptop. Treat the **ratios**
as the signal and the absolute values as an early indication.

### Fixture

100 000 nodes, ~201 000 edges, 300 000 indexed property entries
([graphene_bench_test.go](graphene_bench_test.go#L28)):

- Labels: `Case` on every 1000th node (100 total, highly selective),
  `EvidenceFile` on every 10th (9 900), `MicroArtefact` on the rest (90 000).
- Indexed properties per node: `sha256` (unique), `bucket` (1 000 distinct
  values, ~100 nodes each), `score` (1 000 distinct numeric-ish values).
- Topology: a chain, a `+13` stride, and one hub node carrying 1 000 inbound
  edges.
- The disk fixture is `Compact()`ed before reading, so it exercises the CSR path.

## Results

Every figure below is `benchstat` output at p < 0.05. Untouched paths are listed
under "Controls" and all report no significant change, which is what makes the
rest trustworthy.

### Durability — restart and compaction cost

Before CSR format v6, the property index lived only in the WAL: every `Compact()`
re-emitted every entry into the fresh log, and every restart replayed all of them.
Both costs grew with the total number of indexed entries regardless of how little
had changed. v6 stores the index in the CSR file, so the WAL is left **empty**
after a compaction.

Measured on a 50 000-node store with 100 000 indexed property entries:

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Reopen a compacted store | 1 176 ms | **62.26 ms** | −94.71% (~19×) |
| Compaction, steady state | 619.3 ms | **39.22 ms** | −93.67% (~16×) |
| Compaction allocations | 88.03 MiB | **70.27 MiB** | −20.18% |
| Compaction alloc count | 300.1 k | **100.2 k** | −66.62% |

Reopen allocates **more** — 33.89 MiB → 51.80 MiB (+52.83%) — because the index
now arrives as one file read rather than streaming from WAL replay, and because
the reverse `ID → (key, value)` map is built alongside it. Wall-clock is ~19×
better, so the trade is worth it, but peak memory during open did go up.

### Traversal — allocation, not latency, is the metric

Traversal allocated roughly three times per visited node: every hop built a
`[]NeighbourResult` and a dedupe map, and the queue was consumed by re-slicing
(`queue = queue[1:]`), which slides through the backing array so every append
past the original capacity reallocates.

Walks now pull adjacency into a reusable buffer via `store.AdjacencyReader`,
consume the queue with a head index, and materialise records only when kept.

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `BFS_Deep` — 10k-node chain | 30 190 allocs | **216 allocs** | −99.28% |
| `BFS` — 1k-node chain | 3 058 allocs | **77 allocs** | −97.48% |
| `BFS_Wide` — 100×100 fan-out | 1 323 allocs | **232 allocs** | −82.46% |
| `ShortestPath` | 1 563 allocs | **576 allocs** | −63.15% |
| `BFS_Disk_Deep` | 913 allocs | **394 allocs** | −56.85% |
| `BFS3Hop_Disk` | 70 allocs | **47 allocs** | −32.86% |
| `BFS3Hop_Memory` | 33 allocs | **25 allocs** | −24.24% |

Allocation geomean across the traversal suite: **−76.09%**.

Wall-clock followed, without being the target: `BFS_Disk_Deep` −20.88%,
`BFS3Hop_Disk` −13.62%, `BFS_Deep` −11.10%, `BFS_Wide` −11.03%, `ShortestPath`
−9.60%, `BFS` −8.38%.

**`BFSIDs`** walks the graph without building a single record. It has no baseline
— the API is new:

| Benchmark | Time | Allocations |
|---|---:|---:|
| `BFSIDs_Disk_Deep` | 36.34 µs | **20** |
| `BFSIDs_Deep` (10k chain) | 3.819 ms | **97** |
| `BFSIDs_Wide` (100×100) | 2.612 ms | **104** |

Against the record-returning equivalents that is 36.34 µs vs 113.9 µs and 20 vs
394 allocations on the disk fixture.

### Query planning — driving the query from an index instead of a full scan

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Equality property query (memory) | 39.74 ms | **567.8 ns** | −100.00% (~70 000×) |
| Equality property query (disk) | 50.40 ms | **539.9 ns** | −100.00% (~93 000×) |
| Type + equality property query (memory) | 56.55 ms | **20.78 µs** | −99.96% (~2 720×) |
| Type query, `Limit: 10` (memory) | 10.45 ms | **12.40 µs** | −99.88% (~843×) |
| Type query, `Limit: 10` (disk) | 16.40 ms | **20.47 µs** | −99.88% (~801×) |
| Anchored relation query (memory) | 20.07 ms | **181.2 µs** | −99.10% (~111×) |
| Anchored relation query (disk) | 33.10 ms | **206.6 µs** | −99.38% (~160×) |

### Label index

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `NodesByType`, selective label (disk/CSR) | 415.1 µs | **4.995 µs** | −98.80% (~83×) |
| `NodesByType`, selective label (memory) | 216.2 ns | 227.1 ns | ~ (no change) |

### Property index internals

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `DeleteNode` with a populated property index | 1.222 ms | **6.757 µs** | −99.45% (~181×) |
| Prefix property query (memory) | 46.44 ms | **7.644 ms** | −83.54% (6.1×) |
| Range property query (memory) | 59.00 ms | **21.72 ms** | −63.19% (2.7×) |
| Range property query (disk) | 67.34 ms | **28.31 ms** | −57.96% (2.4×) |

### Degree

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Hub in-degree (memory) | 18.66 µs | **28.79 ns** | −99.85% (~648×) |
| Hub in-degree (disk/CSR) | 70.01 µs | **15.46 ns** | −99.98% (~4 528×) |

### Allocation

Allocated bytes per query, where the change is significant:

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| Equality property query (memory) | 92.94 MB | **576 B** | −100.00% |
| Equality property query (disk) | 100.97 MB | **576 B** | −100.00% |
| Range property query (memory) | 89.84 MiB | **2.74 MiB** | −96.95% |
| Range property query (disk) | 97.50 MiB | **10.39 MiB** | −89.34% |
| Type query, `Limit: 10` (disk) | 15.29 MiB | **17.59 KiB** | −99.89% |
| Anchored relation query (disk) | 33.88 MiB | **191.5 KiB** | −99.45% |
| `NodesByType`, selective (disk) | 12.23 KiB | **4.04 KiB** | −66.96% |
| Hub in-degree (memory / disk) | 8 KiB / 175.4 KiB | **0 B** | −100.00% |

Two benchmarks show a *higher allocation count* alongside a far lower byte total —
the old code made a handful of enormous allocations, the new code makes more but
tiny ones:

| Benchmark | allocs/op | B/op |
|---|---:|---:|
| Type query `Limit: 10` (memory) | 5 → 18 | 1 568 KiB → 7.3 KiB |
| Anchored relation query (memory) | 5 → 36 | 3 160 KiB → 113 KiB |

### Regressions

| Benchmark | Before | After | Change |
|---|---:|---:|---:|
| `IndexNodeProperty` (write path) | 723.2 ns | 834.9 ns | **+15.43%** |
| `IndexNodeProperty` bytes | 202 B | 383 B | **+89.60%** |
| `NodesByProperty` direct lookup | 44.97 ns | 46.95 ns | **+4.41%** |
| Reopen allocations | 33.89 MiB | 51.80 MiB | **+52.83%** |
| `BFS3Hop_Memory` (wall-clock) | 5.106 µs | 5.583 µs | **+9.35%** |
| `BFS_Deep` bytes | 2.102 MiB | 2.374 MiB | **+12.96%** |

Registering a property now does a binary-search insert into a sorted postings list
and appends to the reverse `ID → [(key, value)]` map, instead of blindly appending
to a slice. That reverse map is what turns `DeleteNode` from 1.22 ms into 6.8 µs,
so the trade is roughly 110 ns per indexed property on write to save ~1.2 ms per
delete and remove ~93 MB of allocation per filtered query.

The byte figure is worst-cased by that benchmark's shape: it registers `b.N`
distinct values for a *single* node, so one reverse-map slice grows to millions of
entries. Real workloads spread entries across many IDs.

`BFS3Hop_Memory` walks seven nodes; at that size the walker's one-off setup costs
more than the per-hop allocations it removes, even though those still fall by a
quarter. `BFS_Deep` retains more bytes because the queue no longer discards
consumed entries — the trade that bought 140× fewer allocations.

### New measurements (no baseline — the API did not exist before)

| Benchmark | Result |
|---|---:|
| `VerifyIndexes` over a 100k-node compacted store | 205.8 ms, 43.42 MiB |

This is why verification does **not** run on `Open`. See
[plan.md](plan.md) Phase 4 for the reasoning.

### Controls (untouched paths, confirming the measurement is sound)

All report "no significant change" (p > 0.05): point lookup (memory and disk),
`NodesByType` (memory), 1-hop neighbours (memory and disk), 3-hop BFS (memory and
disk), `AddNode`, `GetNode`, long-chain BFS, and shortest path.

**Overall geomean: −94.58%.**

## What is still slow

- **Edge property blobs are still cloned on materialisation.** `rawEdgeToStore`
  copies the blob every time a CSR edge becomes a `*store.Edge`, and traversal
  never reads it. A copy-on-demand view would remove the last per-edge cost on
  the disk read path.
- **Prefix / range / contains queries remain scans.** They improved 2.4–6.1× only
  because the scan is now confined to one key's buckets instead of materialising
  the whole index. An ordered index (Phase 3) is what makes these logarithmic;
  `Contains` is not indexable at all and will stay a scan.
- **The disk label index is rebuilt on every open and every `Compact()`.** It is
  derived state, deliberately not persisted, so a large graph pays a one-pass
  build at load.
- **Property-index memory.** Sorted postings plus the reverse map add roughly 2–3×
  per-entry overhead versus the old bare postings.

## Reproducing

```powershell
./test.ps1 -Bench                      # default 5s benchtime
go test . -tags=stress -bench=. -benchmem -benchtime=1s -count=6 -run='^$'
```

Sweep the fixture size with `GRAPHENE_BENCH_NODES` (default 100 000).
