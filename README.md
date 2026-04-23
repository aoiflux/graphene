# GrapheneDB

![GrapheneDB Logo](assets/graphene-logo.svg)

GrapheneDB is an embeddable Go graph engine for teams that need to ingest big
connected datasets fast, keep them durable on disk, and run graph queries
without external infrastructure.

## Project Status

GrapheneDB is currently in an experimental, pre-production stage.

- The core architecture and APIs are implemented.
- Benchmarks and stress tests are available, but coverage is still growing.
- It is not startup-ready or production-ready yet.
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

- **Date**: 2026-04-23
- **OS**: Windows 10/11 (amd64)
- **Go Version**: go1.25.5
- **Hardware**: AMD Ryzen 9 5980HS with Radeon Graphics (16 cores)
- **Architecture**: amd64
- **Command**: `./test.ps1 -Bench -BenchTime 1s`

**Results:**

| Benchmark             |       Result |                      Memory |
| --------------------- | -----------: | --------------------------: |
| Add node              |  587.2 ns/op |       303 B/op, 3 allocs/op |
| Get node              |  4.758 ns/op |         0 B/op, 0 allocs/op |
| BFS traversal         | 303155 ns/op | 223561 B/op, 3058 allocs/op |
| Shortest path         | 179960 ns/op | 124865 B/op, 2061 allocs/op |
| Property index lookup |  36.13 ns/op |          8 B/op, 1 alloc/op |

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

## Project Layout

- `graphene.go` and `helpers.go`: public API surface.
- `memory/` and `disk/`: storage backends.
- `index/`: type, property, and temporal indexes.
- `traversal/`: graph traversal and pattern matching.
- `viz/`: interactive HTML export.

## Current Fit

GrapheneDB is best used today for exploration, prototyping, and controlled
internal workloads where you want an embeddable graph engine and can tolerate
ongoing validation work.
