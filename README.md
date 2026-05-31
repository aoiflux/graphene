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

## Benchmarked Snapshot (Early Signal)

The project includes repeatable benchmark and stress suites. Latest benchmark
results:

**Benchmarking Conditions:**

- **Date**: 2026-06-01
- **OS**: Windows 11 (amd64)
- **Go Version**: go1.26.2
- **Hardware**: AMD Ryzen 9 5980HS with Radeon Graphics (16 cores)
- **Architecture**: amd64
- **Command**: `./test.ps1 -Bench -BenchTime 1s`

**Results:**

| Benchmark             |       Result |                      Memory |
| --------------------- | -----------: | --------------------------: |
| Add node              |  831.5 ns/op |       248 B/op, 3 allocs/op |
| Get node              |  6.719 ns/op |         0 B/op, 0 allocs/op |
| BFS traversal         | 475381 ns/op | 223565 B/op, 3058 allocs/op |
| Shortest path         | 278819 ns/op | 124866 B/op, 2061 allocs/op |
| Property index lookup |  55.19 ns/op |          8 B/op, 1 alloc/op |

Scale validation covered by stress tests:

- 100,000 nodes + 500,000 edges large-ingest scenario.
- 50 goroutines concurrent write pressure.
- 50,000-node property index lookup validation.
- Optional persistent 1,000,000-node end-to-end test path.

## Showcased Features

- Traversal toolkit: BFS, DFS, provenance chain, shortest path.
- Query primitives: type lookups, property lookups, degree/connectivity checks.
- Pattern discovery: scoped VF2-inspired subgraph matching.
- Persistence lifecycle: open, replay, compact, reopen.
- Visualization export: interactive HTML graph maps for quick analysis.

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
- Deep technical architecture and LLD:
  [TECHNICAL_DETAILS.md](TECHNICAL_DETAILS.md)
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
- `index/`: type, property, and temporal indexes.
- `traversal/`: graph traversal and pattern matching.
- `viz/`: interactive HTML export.

## Current Fit

GrapheneDB is best used today for exploration, prototyping, and controlled
internal workloads where you want a native embeddable graph engine and can
tolerate ongoing validation work.
