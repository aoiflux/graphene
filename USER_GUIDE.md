# GrapheneDB User Guide

This guide explains all major GrapheneDB concepts and features, then shows how
to use them in practical workflows.

## 1. Concept Glossary

### Graph

A graph is a set of nodes connected by directed edges.

### Node

A node represents an entity. Each node has:

- `NodeID` (store-assigned),
- one or more labels (`[]NodeType`),
- optional raw properties (`[]byte`).

### Edge

An edge represents a directed relationship between two nodes (`Src -> Dst`).

Each edge has:

- `EdgeID` (store-assigned),
- `Src` and `Dst`,
- one or more labels (`[]EdgeType`),
- optional `Weight`,
- optional raw properties (`[]byte`).

### Label / Type

Labels categorize nodes and edges and can be used in query filters.

### Direction

Traversal direction controls which relationships you follow:

- outbound,
- inbound,
- both.

### Hop

One hop equals traversing one edge.

### Multi-Hop

Traversing through multiple edges from an origin node. Example: BFS depth 6 is a
6-hop neighborhood exploration.

### Bucket

A bucket is a grouping key stored as an indexed property (for example key
`bucket`, values `bucket-000` to `bucket-999`).

Buckets in GrapheneDB are implemented using property index keys and values. They
are a modeling/query strategy, not a separate storage subsystem.

### Scope

A scoped operation runs on a selected node subset rather than entire graph.
Scoping is critical for efficient large-graph pattern matching.

### Induced Subgraph

Given a set of node IDs, returns those nodes and only edges whose endpoints are
both in that set.

## 2. Built-In Types

Node labels:

- `EvidenceFile`
- `MicroArtefact`
- `Tag`
- `Case`

Edge labels:

- `Contains`
- `SimilarTo`
- `Reuse`
- `Temporal`
- `TaggedWith`
- `BelongsTo`

Custom labels are available in the custom enum ranges.

## 3. Store Modes

### In-memory store

Use when you need fast setup and no persistence.

```go
g := graphene.NewInMemory()
```

### Disk store

Use for persistent and large-scale workflows.

```go
g, err := graphene.Open("./case-data")
if err != nil {
    return err
}
defer g.Close()
```

## 4. Data Modeling and Ingest

### Create nodes

```go
caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
artID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
```

### Create edges

```go
_, _ = g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
_, _ = g.AddEdge(&store.Edge{Src: fileID, Dst: artID, Labels: []store.EdgeType{store.EdgeTypeContains}})
```

### Batch ingest

```go
ids, err := g.AddNodes([]*store.Node{
    {Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
    {Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
})
if err != nil {
    return err
}

_, err = g.AddEdges([]*store.Edge{
    {Src: ids[0], Dst: ids[1], Labels: []store.EdgeType{store.EdgeTypeReuse}},
})
if err != nil {
    return err
}
```

## 5. Indexing and Bucketing

Property indexing is explicit. You decide which keys become queryable.

### Node property indexing

```go
_ = g.IndexNodeProperty(artID, "sha256", []byte("deadbeef"))
hashHits, _ := g.NodesByProperty("sha256", []byte("deadbeef"))
```

### Edge property indexing

```go
eid, _ := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
_ = g.IndexEdgeProperty(eid, "algorithm", []byte("tlsh"))
edgeHits, _ := g.EdgesByProperty("algorithm", []byte("tlsh"))
```

### Bucket pattern

```go
_ = g.IndexNodeProperty(artID, "bucket", []byte("bucket-042"))
bucketHits, _ := g.NodesByProperty("bucket", []byte("bucket-042"))
```

### Multi-key AND query

```go
hits, _ := g.NodesByProperties(map[string][]byte{
    "sha256": []byte("deadbeef"),
    "tool":   []byte("strings"),
})
```

## 6. Traversal and Multi-Hop Analysis

### BFS (multi-hop neighborhood)

```go
bfs, err := g.BFS(originID, 6, store.DirectionBoth, nil)
if err != nil {
    return err
}
fmt.Println(len(bfs.Nodes), len(bfs.Edges))
```

### DFS

```go
dfs, err := g.DFS(originID, 6, store.DirectionOutbound, nil)
```

### Provenance chain

```go
chain, err := g.ProvenanceChain(artifactID, 10, []store.EdgeType{store.EdgeTypeContains})
```

### Shortest path

```go
path, err := g.ShortestPath(srcID, dstID, nil)
```

## 7. Graph Structure Features

### Degree and connectivity

```go
inDeg, _ := g.InDegree(nodeID, nil)
outDeg, _ := g.OutDegree(nodeID, nil)
deg, _ := g.Degree(nodeID, nil)

exists, _ := g.EdgeExists(srcID, dstID, nil)
connected, _ := g.IsConnected(nodeA, nodeB)
```

### Cycle detection

```go
hasCycle, _ := g.HasCycle(originID, 12, nil)
```

### Typed neighborhood filtering

```go
nbrs, _ := g.NeighboursByNodeType(nodeID, store.DirectionOutbound, store.NodeTypeMicroArtefact, nil)
```

### Induced subgraph extraction

```go
nodes, edges, err := g.InducedSubgraph(scopeNodeIDs)
```

## 8. Pattern Matching

GrapheneDB supports VF2-inspired pattern matching through `FindPatterns`.

```go
pattern := &traversal.Pattern{
    Nodes: []traversal.PatternNode{
        {ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
        {ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
        {ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
    },
    Edges: []traversal.PatternEdge{
        {SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeReuse}},
        {SrcPatternID: 1, DstPatternID: 2, Labels: []store.EdgeType{store.EdgeTypeReuse}},
        {SrcPatternID: 2, DstPatternID: 0, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}},
    },
}

matches, err := g.FindPatterns(pattern, scopeNodeIDs, 100)
```

Best practice:

- run BFS first,
- use BFS node IDs as scope,
- then call `FindPatterns` on that subset.

## 9. Helper APIs and Result Utilities

### Batch and helper methods

- `GetNodes`, `GetEdges`
- `Stats`
- `NodesByAnyType`, `EdgesByAnyType`

### Traversal result helpers

- `NodesFromBFS`
- `EdgesFromBFS`
- `NodeIDsFromBFS`
- `NodeIDsFromPath`
- `FilterNodesByLabel`
- `FilterEdgesByLabel`

These methods make chained query pipelines easier to write.

## 10. Persistence Lifecycle

GrapheneDB disk mode uses WAL + delta + CSR compaction.

Recommended lifecycle for large ingest:

1. Open disk graph.
2. Ingest nodes and edges.
3. Index query-critical properties.
4. Run `Compact()`.
5. Close.
6. Reopen and run query/traversal workloads.

```go
g, _ := graphene.Open("./case-data")
// ingest + index
_ = g.Compact()
_ = g.Close()
```

## 11. Visualization

GrapheneDB visualization is provided by the core `viz` package.

APIs:

- `viz.ExportInteractiveHTML(nodes, edges, outPath)`
- `viz.ExportInteractiveHTMLWithOptions(nodes, edges, outPath, viz.ExportOptions{...})`

Main large example output:

- `graphene_visualization.html`

Additional visualization example outputs:

- `viz_case_map.html`
- `viz_similarity_mesh.html`
- `viz_pattern_scope.html`

It gives you:

- sampled connected subgraph view,
- node IDs and labels,
- edge labels,
- quick visual sanity check after ingest,
- interactive controls for exploration.

Interactive controls include:

- zoom and pan,
- drag nodes,
- filter by edge type,
- search by node ID/type,
- focus selected node,
- node size slider,
- download SVG.

It is a static HTML artifact with no external dependency.

## 12. End-to-End Large Workflow

Typical "push limits" workflow:

1. Define data model and key conventions.
2. Ingest large graph (including bucket keys).
3. Add connection-rich edges for multi-hop analysis.
4. Compact.
5. Reopen and validate counts.
6. Run type/property lookups.
7. Run BFS/DFS/provenance/path.
8. Run connectivity, degree, induced subgraph, pattern matching.
9. Export visualization and archive run metadata.

## 13. Commands

### Examples

```powershell
go run ./examples
```

Run the extreme-scale limit showcase:

```powershell
$env:GRAPHENE_RUN_LIMIT_EXAMPLE='1'
go run ./examples
```

Run the dedicated visualization examples:

```powershell
$env:GRAPHENE_RUN_VIZ_EXAMPLES='1'
go run ./examples
```

### Tests

```powershell
./test.ps1
./test.ps1 -Stress
./test.ps1 -Bench
./test.ps1 -All
```

### Direct Go stress targeting

```powershell
go test . -tags=stress -run TestStress
```

## 14. Troubleshooting

### AddEdge fails

Cause:

- source or destination node does not exist.

Fix:

- create nodes first, then create edges.

### Property query returns empty

Cause:

- key not indexed,
- value encoding mismatch,
- typo in key/value.

Fix:

- verify indexing path and deterministic bytes.

### Large queries are slow

Cause:

- unscoped traversal or pattern matching,
- no compaction after ingest.

Fix:

- scope operations using multi-hop BFS,
- compact and reopen for read-heavy phase.

## 15. Code Reference Map

- API entrypoint: `graphene.go`
- Helper APIs: `helpers.go`
- Store contract and types: `store/interface.go`, `store/types.go`
- Traversal and matching: `traversal/`
- Disk persistence internals: `disk/`
- Examples: `examples/main.go`
- Stress tests: `graphene_stress_test.go`

This guide covers the complete concept set currently implemented in GrapheneDB,
including buckets, nodes, edges, multi-hop traversal, indexing, persistence, and
visualization.
