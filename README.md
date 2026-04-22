# GrapheneDB

GrapheneDB is an embeddable, typed graph database in Go for high-volume ingest
and read-heavy graph analytics.

It is built for workloads where you ingest large connected data, persist it
safely, and then run many traversals and lookups efficiently.

## Core Concepts

This section defines the vocabulary used across GrapheneDB.

### Graph

A graph is a set of nodes connected by directed edges.

### Node

A node is an entity in the graph.

Each node has:

- `NodeID` (assigned by store),
- one or more labels (`[]NodeType`),
- optional raw properties (`[]byte`).

Examples:

- a case,
- an evidence file,
- an artifact,
- a tag.

### Edge

An edge connects two nodes (`Src -> Dst`) and represents a relationship.

Each edge has:

- `EdgeID` (assigned by store),
- source and destination IDs,
- one or more labels (`[]EdgeType`),
- optional `Weight` (for similarity-style semantics),
- optional raw properties.

### Labels (Types)

Labels classify nodes and edges.

Built-in node labels:

- `EvidenceFile`
- `MicroArtefact`
- `Tag`
- `Case`

Built-in edge labels:

- `Contains`
- `SimilarTo`
- `Reuse`
- `Temporal`
- `TaggedWith`
- `BelongsTo`

Nodes and edges can carry multiple labels simultaneously.

### Direction

GrapheneDB supports directional traversal:

- outbound,
- inbound,
- both.

### Hop and Multi-Hop

A hop is one edge traversal.

Multi-hop traversal means exploring nodes multiple edges away from an origin
node (for example 3-hop or 6-hop BFS).

### Bucket (Property Bucket Concept)

A bucket is an indexed property value used to group nodes into partitions for
fast lookup.

Example: assigning each artifact to one of 1000 buckets using key `bucket` and
values like `bucket-042`.

Buckets are not a dedicated storage primitive. They are a query pattern
implemented through explicit property indexing.

### Scope

Scope is a constrained set of node IDs passed to expensive operations
(especially pattern matching) to reduce search cost.

### Induced Subgraph

Given a set of node IDs, the induced subgraph returns:

- those nodes,
- only edges whose endpoints are both inside the set.

## Why GrapheneDB

- Typed property graph model.
- First-class nodes and edges.
- Multiple labels per node and edge.
- In-memory store for development and tests.
- Persistent disk store with WAL + CSR compaction.
- Traversal primitives for connected-data analysis.
- Explicit indexing for predictable query behavior.
- No external runtime dependencies.

## Feature Coverage

### Data Ingest and Mutation

- `AddNode`
- `AddEdge`
- `AddNodes` (batch)
- `AddEdges` (batch)

### Entity Lookup

- `GetNode`
- `GetEdge`
- `GetNodes` (batch)
- `GetEdges` (batch)

### Topology and Neighborhood Query

- `Neighbours`
- `EdgesOf`
- `InDegree`
- `OutDegree`
- `Degree`
- `EdgeExists`
- `IsConnected`
- `HasCycle`

### Type-Based Query

- `NodesByType`
- `EdgesByType`
- `NodesByAnyType` (OR semantics)
- `EdgesByAnyType` (OR semantics)

### Property Index Query

- `IndexNodeProperty`
- `IndexEdgeProperty`
- `IndexNodeProperties` (bulk key-value)
- `IndexEdgeProperties` (bulk key-value)
- `NodesByProperty`
- `EdgesByProperty`
- `NodesByProperties` (AND semantics)
- `EdgesByProperties` (AND semantics)

### Traversal and Pathing

- `BFS` (k-hop, including multi-hop)
- `DFS`
- `ProvenanceChain`
- `ShortestPath` (bidirectional BFS)

### Subgraph and Pattern Features

- `InducedSubgraph`
- `FindPatterns` (VF2-inspired)
- traversal result helpers:
  - `NodesFromBFS`
  - `EdgesFromBFS`
  - `NodeIDsFromBFS`
  - `NodeIDsFromPath`
  - `FilterNodesByLabel`
  - `FilterEdgesByLabel`

### Persistence and Lifecycle

- `Open(dir)` for disk-backed graph.
- `Compact()` to merge delta into CSR and truncate WAL.
- `Close()` to release resources.
- WAL replay on restart.

### Stats

- `NodeCount`
- `EdgeCount`
- `Stats()`

### Visualization

GrapheneDB includes a first-class `viz` package for interactive HTML graph
export.

Core APIs:

- `viz.ExportInteractiveHTML(nodes, edges, outPath)`
- `viz.ExportInteractiveHTMLWithOptions(nodes, edges, outPath, opts)`

Interactive features in generated HTML:

- pan and zoom,
- drag-to-reposition nodes,
- edge-type filter controls,
- node search,
- selected-node focus,
- adjustable node size,
- SVG download,
- node/edge detail inspector.

## Architecture Overview

### Store Modes

1. In-memory store

- Use for tests, fast local development, small data.

2. Disk store

- Use for persistence and large datasets.

### Disk Store Data Flow

1. Writes append to WAL and update in-memory delta.
2. Reads merge CSR base + delta.
3. `Compact()` rebuilds CSR from merged state.
4. WAL is truncated.
5. Restart replays WAL if needed.

This favors bulk ingest followed by read-heavy query cycles.

## Project Structure

- `graphene.go`: Graph entrypoint and traversal wrappers.
- `helpers.go`: helper APIs and utility query methods.
- `viz/`: visualization export module (interactive HTML graph views).
- `store/`: core types and GraphStore interface.
- `memory/`: in-memory backend.
- `disk/`: persistent backend (WAL/CSR/compaction).
- `traversal/`: BFS/DFS/path/provenance/subgraph matching.
- `index/`: indexing internals.
- `examples/`: runnable examples including limit-scale flow.
- `graphene_stress_test.go`: stress workloads.

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

    caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
    fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
    artID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

    _, _ = g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
    _, _ = g.AddEdge(&store.Edge{Src: fileID, Dst: artID, Labels: []store.EdgeType{store.EdgeTypeContains}})

    _ = g.IndexNodeProperty(artID, "bucket", []byte("bucket-001"))
    _ = g.IndexNodeProperty(artID, "sha256", []byte("abc123"))

    bucketHits, _ := g.NodesByProperty("bucket", []byte("bucket-001"))
    bfs, _ := g.BFS(fileID, 2, store.DirectionBoth, nil)

    fmt.Println("bucket hits:", len(bucketHits), "multi-hop nodes:", len(bfs.Nodes))
}
```

## Running Examples

From repo root:

```powershell
go run ./examples
```

The examples cover:

- basic graph modeling,
- persistence and WAL replay,
- compaction,
- property and type indexing,
- helper methods,
- large-scale ingest and query,
- HTML visualization export,
- dedicated visualization scenarios.

Example output files:

- `graphene_visualization.html`
- `viz_case_map.html`
- `viz_similarity_mesh.html`
- `viz_pattern_scope.html`

Run controls:

- `GRAPHENE_RUN_LIMIT_EXAMPLE=1` to run extreme-scale example 18.
- `GRAPHENE_RUN_VIZ_EXAMPLES=1` to run visualization examples 19–21.

## Testing and Stress

```powershell
./test.ps1
./test.ps1 -Stress
./test.ps1 -Bench
./test.ps1 -All
```

```powershell
go test ./...
go test . -tags=stress -run TestStress
go test . -tags=stress -bench=. -run=^$
```

## Performance Guidance

- Batch writes where possible.
- Index only keys you query frequently.
- Use buckets for partitioning large keyspaces.
- Prefer scoped multi-hop traversal over full-graph scans.
- Compact after bulk ingest phases.
- Use directional filtering to reduce traversal fan-out.
- Scope pattern matching to BFS-derived node sets.

## Operational Guidance

- Always `Close()` disk-backed graphs.
- Keep one output directory per large run for reproducibility.
- Do not reuse stale run directories for fresh benchmark ingest.
- Track ingest parameters (`nodeTarget`, edge stride, key conventions) alongside
  datasets.

## Documentation Map

- High-level overview: this file.
- End-user, step-by-step guidance: `USER_GUIDE.md`.
- API and implementation details: code in `graphene.go`, `helpers.go`, `store/`,
  `disk/`, `traversal/`.

## License and Contribution

If publishing externally, add:

- `LICENSE`
- `CONTRIBUTING.md`
- issue/PR templates

for a complete contributor workflow.
