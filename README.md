# GrapheneDB

![GrapheneDB Logo](assets/graphene-logo.svg)

GrapheneDB is an experimental embeddable Go graph engine for teams that need to
ingest big connected datasets fast, keep them durable on disk, and run graph
queries without external infrastructure.

## Project Status

GrapheneDB is currently in an experimental, pre-production stage.

- The core architecture and APIs are implemented.
- Benchmarks and stress tests are available, but coverage is still growing.
- It is not startup-ready or production-ready yet.
- The on-disk backend is still maturing toward a fuller property-graph feature
  set.
- Treat current performance numbers as early signals, not final guarantees.

## Why It Exists

- Build once, query many times: optimized for heavy ingest followed by
  read-heavy graph analysis.
- Zero external runtime: no server to manage, no JVM, no sidecar.
- Typed graph model: predictable APIs with domain-friendly node and edge labels.
- Durable by design: WAL-backed persistence with replay and explicit compaction.
- Full mutation lifecycle: create, read, update, and delete nodes and edges —
  deletes cascade to incident edges and persist across restart.

## Benchmarked Snapshot (Early Signal)

The project includes repeatable benchmark and stress suites. Latest benchmark
results:

**Benchmarking Conditions:**

- **Date**: 2026-07-21
- **OS**: Windows 11 (amd64)
- **Go Version**: go1.26.2
- **Hardware**: AMD Ryzen 9 5980HS with Radeon Graphics (16 cores)
- **Architecture**: amd64
- **Command**: `./test.ps1 -Bench -BenchTime 1s`

**Core operations:**

| Benchmark             |       Result |                      Memory |
| --------------------- | -----------: | --------------------------: |
| Add node              |  814.0 ns/op |       306 B/op, 3 allocs/op |
| Get node              |  5.815 ns/op |         0 B/op, 0 allocs/op |
| BFS traversal         | 353200 ns/op |   234240 B/op, 77 allocs/op |
| Shortest path         | 188300 ns/op |  100472 B/op, 576 allocs/op |
| Property index lookup |  42.78 ns/op |          8 B/op, 1 alloc/op |

The suite now runs **68 benchmarks** covering reads, writes, concurrency, a
10k→100k scale sweep, and resident memory footprint — up from 5. See
[benchmarks.md](benchmarks.md) for methodology and the full table.

**Query operations** — measured on a 100,000-node / 201,000-edge graph with
300,000 indexed property entries, disk backend compacted to CSR:

| Benchmark                       |    In-memory |         On-disk |
| ------------------------------- | -----------: | --------------: |
| Point lookup by ID              |  26.46 ns/op |     50.09 ns/op |
| Node in-degree (1000-edge hub)  |  28.79 ns/op |     15.46 ns/op |
| Equality property query         |  567.8 ns/op |     539.9 ns/op |
| `NodesByType`, selective label  |  227.1 ns/op |     4.995 µs/op |
| Typed query with `Limit: 10`    |  12.40 µs/op |     20.47 µs/op |
| Anchored relation query         |  181.2 µs/op |     206.6 µs/op |
| 1-hop neighbours                |  179.2 ns/op |     410.1 ns/op |
| 3-hop BFS                       |  2.799 µs/op |     4.376 µs/op |

**Traversal** — allocations per walk, the metric that governs GC pressure here:

| Benchmark                        |         Time |   Allocations |
| -------------------------------- | -----------: | ------------: |
| BFS, 10,000-node chain           |  6.935 ms/op |           216 |
| `BFSIDs`, same walk (IDs only)   |  3.819 ms/op |            97 |
| BFS, 100×100 fan-out             |  5.326 ms/op |           232 |
| `BFSIDs`, same walk              |  2.612 ms/op |           104 |
| BFS on disk, 12 hops             |  113.9 µs/op |           394 |
| `BFSIDs` on disk, same walk      |  36.34 µs/op |            20 |

**Durability** — 50,000-node store with 100,000 indexed property entries:

| Benchmark                       |       Result |
| ------------------------------- | -----------: |
| Reopen a compacted store        |  62.26 ms/op |
| Compaction, steady state        |  39.22 ms/op |
| `VerifyIndexes` (100k nodes)    |  205.8 ms/op |

### Indexing & durability upgrade (2026-07-21)

The read path was reworked so queries start from an index rather than a full
enumeration of the graph, and the property index is now stored in the CSR file
instead of being rebuilt from the WAL on every restart.

Measured as an **interleaved** A/B against `036aac0`, 6 samples per side. The
interleaving is not incidental: measuring the two sides back to back produced a
~25% shift on benchmarks the changes never touched. See
[benchmarks.md](benchmarks.md#why-interleaving-demonstrated).

| Operation                              |    Before |       After |         Change |
| -------------------------------------- | --------: | ----------: | -------------: |
| Equality property query (disk)         |  50.40 ms |   539.9 ns  | ~93 000× faster |
| Equality property query (memory)       |  39.74 ms |   567.8 ns  | ~70 000× faster |
| Node in-degree on a hub (disk)         |  70.01 µs |   15.46 ns  |  ~4 528× faster |
| Type + property query (memory)         |  56.55 ms |   20.78 µs  |  ~2 720× faster |
| Typed query with `Limit: 10` (disk)    |  16.40 ms |   20.47 µs  |    ~801× faster |
| `DeleteNode` with a populated index    |  1.222 ms |   6.757 µs  |    ~181× faster |
| Anchored relation query (disk)         |  33.10 ms |   206.6 µs  |    ~160× faster |
| `NodesByType` on a selective label     | 415.1 µs  |   4.995 µs  |     ~83× faster |
| **Reopen a compacted store**           | **1 176 ms** | **62.26 ms** |  **~19× faster** |
| **Compaction, steady state**           | **619.3 ms** | **39.22 ms** |  **~16× faster** |
| Prefix property query                  |  45.67 ms |   7.656 ms  |     6.0× faster |
| Range property query                   |  60.39 ms |   20.00 ms  |     3.0× faster |
| `UpdateNode` in a 50k-member label     |  50.43 µs |   4.435 µs  |    11.4× faster |
| `DeleteNode` from a 50k-member label   |  27.98 µs |   3.732 µs  |     7.5× faster |
| Edge query by type (memory)            |  44.49 ms |   157.9 µs  |    ~282× faster |

Cost no longer tracks the graph. A 10× larger graph used to make an equality
query 14× slower (2.91 ms → 41.38 ms); it is now flat (704 ns → 590 ns).

Traversal was reworked separately, where the metric is **allocations per walk**
rather than latency — allocation is what the GC turns into tail latency:

| Operation                              |        Before |         After |        Change |
| -------------------------------------- | ------------: | ------------: | ------------: |
| BFS over a 10,000-node chain           | 30,190 allocs |   216 allocs  | ~140× fewer |
| BFS over a 1,000-node chain            |  3,058 allocs |    77 allocs  |  ~40× fewer |
| BFS over a 100×100 fan-out             |  1,323 allocs |   232 allocs  |  ~5.7× fewer |
| Shortest path                          |  1,563 allocs |   576 allocs  |  ~2.7× fewer |
| BFS on the disk backend                |    913 allocs |   394 allocs  |  ~2.3× fewer |

Wall-clock improved alongside it (disk BFS −21%, shortest path −10%), and the new
`BFSIDs` walks the graph without building a single record: 20 allocations where
the record-returning walk needs 394.

Allocation dropped alongside latency: a filtered query that used to allocate
**93 MB** now allocates **576 bytes**, and hub degree counting allocates nothing
at all. Geomean across the whole suite: **−94.6%**.

Restart and compaction no longer scale with index size. Before, every `Compact()`
re-emitted the entire property index into the fresh WAL and every restart
replayed it; now the index lives in the CSR file and the WAL is left empty after
a compaction.

The costs are on the write side and honest: `IndexNodeProperty` is ~15% slower
(723 → 835 ns/op) because registering a property maintains sorted postings and a
reverse `ID → (key, value)` map — that map is what makes deletes ~181× cheaper —
and reopening allocates ~53% more peak memory because the index arrives as one
file read rather than streaming from the log. A 3-hop in-memory walk is ~9%
slower in wall-clock, where the traversal setup outweighs what it saves on a
handful of nodes. Ingest and point lookups are unchanged.

Full methodology, per-benchmark detail, and the remaining slow paths are in
[benchmarks.md](benchmarks.md); the roadmap is in [plan.md](plan.md).

Scale validation covered by stress tests:

- 100,000 nodes + 500,000 edges large-ingest scenario.
- 50 goroutines concurrent write pressure.
- 50,000-node property index lookup validation.
- Optional persistent 1,000,000-node end-to-end test path.

## Showcased Features

- Full CRUD: add, get, update, and delete nodes and edges (cascade delete).
- Traversal toolkit: BFS, DFS, provenance chain, shortest path.
- Query primitives: type lookups, property lookups, degree/connectivity checks.
- Pattern discovery: scoped VF2-inspired subgraph matching.
- Persistence lifecycle: open, replay, compact, reopen.
- Visualization export: interactive HTML graph maps for quick analysis.

## Mutation (update / delete)

Entities can be edited and removed, and every change is durable:

```go
// Update replaces labels + properties in place (edge endpoints are immutable).
_ = g.UpdateNode(&store.Node{ID: artID, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("reclassified")})
_ = g.UpdateEdge(&store.Edge{ID: eid, Labels: []store.EdgeType{store.EdgeTypeReuse}, Weight: 0.42})

// DeleteEdge removes one relationship; DeleteNode cascades to incident edges.
_ = g.DeleteEdge(eid)
_ = g.DeleteNode(artID)
```

On the disk backend, updates and deletes are written to the WAL (deletes as
tombstone records) and take effect immediately for reads; the freed space is
reclaimed at the next `Compact()`. IDs are monotonic and never reused. See the
[API reference](API_REFERENCE.md#mutation) for full semantics.

## Query Model

GrapheneDB is API-first. It does not use a SQL-like or string-based query
language.

Query behavior is exposed as typed Go functions on Graph and GraphStore, for
example:

- Node and edge retrieval by indexed properties.
- Multi-property matching through function parameters.
- Typed query functions for nodes, edges, and relations.
- Traversal and pattern functions for graph-structured analysis.
- Built-in deterministic ordering with offset/limit pagination.
- Sort direction control with `Order: store.QueryOrderAsc|QueryOrderDesc`.
- Custom type selectors for user-defined labels (for example `custom:7`).

This keeps query behavior explicit, type-safe, and easy to compose inside Go
code.

```go
ids, _ := g.QueryNodeIDs(store.NodeQuery{
  Types:  []store.NodeType{store.NodeTypeMicroArtefact},
  Filters: []store.PropertyFilter{
    {Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-0")},
  },
  Order:  store.QueryOrderDesc,
  Offset: 0,
  Limit:  50,
})
```

## Indexing

Queries are served from indexes, not from scans. What exists today:

| Index                     | Backing structure                                     | Serves                                       |
| ------------------------- | ----------------------------------------------------- | -------------------------------------------- |
| Primary (ID → record)     | Hash map in memory; direct array offset in the CSR     | `GetNode`, `GetEdge`                          |
| Adjacency                 | CSR prefix-sum arrays, plus a delta overlay            | Neighbours, traversal, anchored relations, degree |
| Label (type)              | Postings per label, built for both the delta and CSR   | `NodesByType`, `EdgesByType`, `Types` filters  |
| Property (secondary)      | Sorted postings per `(key, value)` + reverse ID map    | Equality filters, `NodesByProperty`            |
| Ordered (range)           | Sorted values per *declared* key, ascending postings   | `>`, `>=`, `<`, `<=`, `Between`, `Prefix`      |

The query planner picks whichever of these bounds the result most tightly —
property postings, a declared ordered key's range, label postings, or the
anchors' incident-edge lists — and falls back to a full scan only when none
applies. Property indexing stays explicit and opt-in: you register the fields you want indexed with
`IndexNodeProperty` / `IndexNodeProperties`, so the storage layer never has to
understand your property encoding.

Index-accelerated operators: `PropertyOpEqual` always; the range operators and
`Prefix` once the key is declared ordered (below). `Contains` is a scan and will
stay one — no ordering can bound a substring match.

### Ordered keys for range queries

Declaring a key builds a sorted structure over its values, turning a range filter
into two binary searches:

```go
import "github.com/aoiflux/graphene/index/encoding"

g.IndexNodeProperty(id, "score", encoding.Int64(score))
g.DeclareOrderedProperty("score")            // absorbs entries already indexed

g.QueryNodes(store.NodeQuery{Filters: []store.PropertyFilter{{
    Key: "score", Op: store.PropertyOpBetweenInclusive,
    Value: encoding.Int64(100), ValueUpper: encoding.Int64(200),
}}})
```

**Declaring changes how that key compares.** Undeclared keys use the scan rule —
numeric when both sides parse, byte order otherwise — which is fine value by
value but is not a valid sort order: under it `"9" < "10" < "1x" < "9"`, a cycle.
A declared key is compared byte-wise throughout, so encode values with
`index/encoding` (or use a naturally ordered form such as zero-padded fixed-width
digits or hex). Equality lookups are unaffected either way.

Measured on 1,000 distinct values: a wide range goes 22.84 ms → 2.310 ms, and a
narrow one 11.76 ms → 59 µs.

### Durability

The property index is stored inside the CSR file (format v6), so a compacted
store reopens without replaying anything: the WAL is left empty by `Compact()`
and restart cost no longer grows with the number of indexed entries. Files
written by earlier versions (v2–v5) still open, with the WAL supplying the index
as before, and are upgraded on the next `Compact()`.

### Keeping the index truthful

The engine cannot re-derive property-index entries on its own: indexed values are
supplied by you in your own encoding, and the `Properties` blob is opaque to the
storage layer. So updating an indexed entity needs a choice, and the API makes it
explicit rather than silent:

```go
// Preferred: update and re-register in one call. No stale entry for the old
// value, no lost entry for the untouched ones.
_ = g.UpdateNodeIndexed(
    &store.Node{ID: artID, Labels: []store.NodeType{store.NodeTypeTag}},
    map[string][]byte{"sha256": newHash},
)

// Or pick a policy for plain UpdateNode / UpdateEdge:
//   ReindexKeep  (default) — entries are kept, and therefore go stale
//   ReindexPurge           — entries are dropped, including still-valid ones
g.SetReindexPolicy(store.ReindexPurge)
```

Two maintenance calls back this up. `g.VerifyIndexes()` cross-checks every index
against the records it describes — postings ordering, reverse-map agreement,
label postings, adjacency endpoints, and orphaned entries — and
`g.RebuildIndexes()` recomputes everything derivable from the records. Neither
runs automatically on `Open`: verification is O(V+E) (~200 ms on a 100k-node
store) and a damaged index section is already rejected while parsing, so the scan
would be a startup tax for little gain. Run them explicitly in tests, in CI, or
when recovering a suspect store.

## Quick Start

```go
package main

import (
    "fmt"

    "graphene"
    "graphene/store"
)

func main() {
    g := graphene.NewInMemory()

    a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
    b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
    _, _ = g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeContains}})

    walk, _ := g.BFS(a, 2, store.DirectionOutbound, nil)
    fmt.Println("visited nodes:", len(walk.Nodes))
}
```

## Run It

```powershell
go run ./examples
./test.ps1
./test.ps1 -Bench
```

## Docs

- Easy usage guide: [USER_GUIDE.md](USER_GUIDE.md)
- Complete API reference: [API_REFERENCE.md](API_REFERENCE.md)
- Deep technical architecture and LLD:
  [TECHNICAL_DETAILS.md](TECHNICAL_DETAILS.md)
- Benchmark methodology and results: [benchmarks.md](benchmarks.md)
- Indexing and read-performance roadmap: [plan.md](plan.md)
- Engine comparison notes: [comparison.md](comparison.md)

## Query Migration

Legacy property helpers remain supported:

- `NodesByProperty`, `EdgesByProperty`
- `NodesByProperties`, `EdgesByProperties`

Preferred new typed APIs for new code:

- `QueryNodeIDs` / `QueryNodes`
- `QueryEdgeIDs` / `QueryEdges`
- `QueryRelationIDs` / `QueryRelations`

Migration approach:

1. Keep existing property-index calls (`IndexNodeProperty`,
   `IndexEdgeProperty`).
2. Move single/multi-property lookups into typed query filters.
3. Add explicit `Order`, `Offset`, and `Limit` where paged output is required.

## Project Layout

- `graphene.go` and `helpers.go`: public API surface.
- `memory/` and `disk/`: storage backends.
- `index/`: property index (sorted postings + reverse map), ordered range index,
  and `index/encoding` order-preserving value encoders.
- `traversal/`: graph traversal and pattern matching.
- `viz/`: interactive HTML export.

## Current Fit

GrapheneDB is best used today for exploration, prototyping, and controlled
internal workloads where you want a native embeddable graph engine and can
tolerate ongoing validation work.
