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

### Update nodes and edges

`UpdateNode` / `UpdateEdge` replace an existing entity's labels and properties in
place (identified by ID). Edge **endpoints are immutable** — an update changes
labels, weight and properties only; to reconnect an edge, delete it and add a new
one. Both are durable (survive restart and compaction).

```go
// Re-classify a node and rewrite its payload.
_ = g.UpdateNode(&store.Node{
    ID:         artID,
    Labels:     []store.NodeType{store.NodeTypeTag},
    Properties: []byte("reclassified"),
})

// Adjust an edge's weight/labels (Src/Dst on the struct are ignored).
_ = g.UpdateEdge(&store.Edge{
    ID:     eid,
    Labels: []store.EdgeType{store.EdgeTypeReuse},
    Weight: 0.42,
})
```

> The property index is caller-maintained and is **not** auto-updated by
> `UpdateNode`/`UpdateEdge`. If you changed an indexed field, re-index it.

### Delete nodes and edges

`DeleteEdge` removes a single edge. `DeleteNode` **cascades** — it also removes
every edge incident to the node, so the graph never keeps an edge that points at a
missing node. Deleting a missing entity returns a not-found error you can ignore
for idempotency. IDs are never reused.

```go
_ = g.DeleteEdge(eid)  // remove one relationship
_ = g.DeleteNode(artID) // remove the node and all its edges
```

Deletes take effect immediately for reads and persist across restart. On the disk
store the freed space is reclaimed at the next `Compact()` (which rebuilds the CSR
without the deleted records).

## 5. Indexing and Bucketing

Property indexing is explicit. You decide which keys become queryable.

GrapheneDB query behavior is API-first. It does not use a text query language or
DSL parser. You compose queries through typed Go functions.

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

### Function-based query APIs

Use typed query structs and helper functions when you need richer filtering:

- `QueryNodeIDs`, `QueryNodes`
- `QueryEdgeIDs`, `QueryEdges`
- `QueryRelationIDs`, `QueryRelations`
- `NodesWithProperties`, `EdgesWithProperties`

Type-selector helpers are also available for user-defined custom labels:

- `NodesByTypeSelector`, `NodesByAnyTypeSelector`
- `EdgesByTypeSelector`, `EdgesByAnyTypeSelector`

These are API calls, not query strings.

Node query with pagination:

```go
nodeIDs, _ := g.QueryNodeIDs(store.NodeQuery{
    Types: []store.NodeType{store.NodeTypeMicroArtefact},
    Filters: []store.PropertyFilter{
        {Key: "size", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("1024")},
    },
    Order:  store.QueryOrderAsc,
    Offset: 100,
    Limit:  25,
})

nodes, _ := g.QueryNodes(store.NodeQuery{IDs: nodeIDs})
_ = nodes
```

Edge query with endpoint constraints and pagination:

```go
edgeIDs, _ := g.QueryEdgeIDs(store.EdgeQuery{
    Types:  []store.EdgeType{store.EdgeTypeSimilarTo},
    SrcIDs: []store.NodeID{artifactID},
    Filters: []store.PropertyFilter{
        {Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("sim-")},
    },
    Order:  store.QueryOrderDesc,
    Offset: 0,
    Limit:  100,
})
_ = edgeIDs
```

`Order` is optional. If omitted, queries default to `store.QueryOrderAsc`.

Custom type selection example:

```go
customNodeType := store.CustomNodeType(7)
_, _ = g.AddNode(&store.Node{Labels: []store.NodeType{customNodeType}})

customNodes, _ := g.NodesByTypeSelector("custom:7")
_ = customNodes
```

Migration map from legacy helpers:

- `NodesByProperty(key, value)` ->
  `QueryNodeIDs(NodeQuery{Filters: []PropertyFilter{{Key: key, Op: PropertyOpEqual, Value: value}}})`
- `EdgesByProperty(key, value)` ->
  `QueryEdgeIDs(EdgeQuery{Filters: []PropertyFilter{{Key: key, Op: PropertyOpEqual, Value: value}}})`
- `NodesByProperties(map)` ->
  `QueryNodeIDs(NodeQuery{Filters: ..., FilterMode: MatchAll})`
- `EdgesByProperties(map)` ->
  `QueryEdgeIDs(EdgeQuery{Filters: ..., FilterMode: MatchAll})`

Prefer typed query APIs when you need AND/OR composition, range/prefix/contains,
ordering, or pagination.

Relation query (function API, no DSL):

```go
rels, _ := g.QueryRelations(store.RelationQuery{
    Anchors:   []store.NodeID{artifactID},
    Direction: store.DirectionBoth,
    EdgeTypes: []store.EdgeType{store.EdgeTypeSimilarTo, store.EdgeTypeReuse},
    Filters: []store.PropertyFilter{
        {Key: "kind", Op: store.PropertyOpContains, Value: []byte("near")},
    },
    Order:  store.QueryOrderDesc,
    Offset: 0,
    Limit:  50,
})
_ = rels
```

ID-first relation query (useful for paged API responses):

```go
relIDs, _ := g.QueryRelationIDs(store.RelationQuery{
    Anchors:   []store.NodeID{artifactID},
    Direction: store.DirectionBoth,
    EdgeTypes: []store.EdgeType{store.EdgeTypeSimilarTo},
    Order:     store.QueryOrderDesc,
    Offset:    0,
    Limit:     25,
})
_ = relIDs
```

### Range and prefix queries: declare the key first

An equality lookup works on any key you have indexed. A **range** query (`>`,
`>=`, `<`, `<=`, `Between`) or a **prefix** query is a scan unless you declare
the key ordered:

```go
import "github.com/aoiflux/graphene/index/encoding"

// Encode the value so byte order matches numeric order, then declare the key.
_ = g.IndexNodeProperty(artID, "size", encoding.Int64(fileSize))
_ = g.DeclareOrderedProperty("size")

big, _ := g.QueryNodes(store.NodeQuery{Filters: []store.PropertyFilter{{
    Key: "size", Op: store.PropertyOpGreaterThan, Value: encoding.Int64(1 << 20),
}}})
_ = big
```

Declaring absorbs entries you have already indexed, so it can be done at any
point. On a thousand distinct values this took a wide range query from 22.8 ms to
2.3 ms, and a narrow one from 11.8 ms to 59 µs.

**Two things to know before you declare a key.**

First, it changes how that key compares. Undeclared keys try numeric comparison
and fall back to byte order; a declared key is compared byte-wise throughout.
That matters because the fallback rule is not a valid ordering — under it
`"9" < "10" < "1x" < "9"`, which is a cycle — so a sorted index cannot be built on
it. Use `index/encoding` and the two agree.

Second, the declaration is not stored. After reopening a store, declare the keys
again.

Timestamps work the same way:

```go
_ = g.IndexNodeProperty(evtID, "seen_at", encoding.Time(t))
_ = g.DeclareOrderedProperty("seen_at")

window, _ := g.QueryNodes(store.NodeQuery{Filters: []store.PropertyFilter{{
    Key:   "seen_at",
    Op:    store.PropertyOpBetweenInclusive,
    Value: encoding.Time(from), ValueUpper: encoding.Time(to),
}}})
_ = window
```

`Contains` cannot be accelerated by any ordering and always scans.

### Keeping the index correct when you update

The engine cannot re-derive your index entries: you supply the values in your own
encoding, and the properties blob is opaque to it. So an update has to say what
should happen to them.

The safe call updates the record and its entries together:

```go
_ = g.UpdateNodeIndexed(
    &store.Node{ID: artID, Labels: []store.NodeType{store.NodeTypeTag}},
    map[string][]byte{"sha256": newHash},
)
```

If you use plain `UpdateNode`, pick a policy and know its failure mode:

| Policy | Behaviour | What goes wrong |
|---|---|---|
| `store.ReindexKeep` (default) | entries kept | they go **stale** — the old value still matches |
| `store.ReindexPurge` | entries dropped | they are **lost**, including untouched keys |

```go
g.SetReindexPolicy(store.ReindexPurge)
```

### Checking the indexes

```go
if err := g.VerifyIndexes(); err != nil {
    // structural inconsistency between an index and the records it describes
}
_ = g.RebuildIndexes() // recompute what is derivable from the records
```

Neither runs automatically on `Open` — verification is O(V+E), around 200 ms on a
100k-node store, and a damaged file is already rejected while parsing. Call them
in tests, in CI, or when recovering a store you do not trust.

### Checking what a query actually does

A query can return exactly the right answer while doing far more work than it
needed to, and you cannot tell from the results. `ExplainNodeQuery` says which
index drove the query and what happened to the remaining filters:

```go
plan, _ := g.ExplainNodeQuery(store.NodeQuery{Filters: []store.PropertyFilter{
    {Key: "sha256", Op: store.PropertyOpEqual, Value: hash},
    {Key: "tool", Op: store.PropertyOpContains, Value: []byte("acquire")},
}})
fmt.Println(plan)
// driver=equality(sha256) candidates=1 residual=tool:probe~100000 results=1
```

Read it as: the `sha256` equality drove the query down to one candidate, and the
`tool` filter was then tested against that one candidate (`probe`) rather than
resolved to its own set — which would have meant scanning all 100,000 entries
under `tool`, since `Contains` cannot be served by any index.

Two things worth looking for:

- **`driver=scan`** on a query you expected to be indexed. Nothing was
  selective enough to drive it, usually because the key is not indexed at all.
- **`residual=<key>:set~<big>`** where `<big>` dwarfs `candidates`. The planner
  judged building that filter's set cheaper; if that looks wrong, the usual cause
  is a range filter on a key you have not declared ordered.

The plan is diagnostic. Which index gets picked may change between versions; the
results a query returns will not.

### What a read guarantees when something else is writing

Every operation is atomic on its own, and a completed `DeleteNode` leaves nothing
behind — no dangling edge, no index entry, under any key. What a read gives you
is:

> every ID returned named an entity that was live at the moment it was checked.

**The moment is inside the call, not after it.** By the time you act on a result
the entity may be gone, so `GetNode` on an ID you were just handed can
legitimately fail:

```go
ids, _ := g.NodesByProperty("sha256", hash)
for _, id := range ids {
    n, err := g.GetNode(id)
    if err != nil {
        continue // deleted between the lookup and here — expected, not a bug
    }
    _ = n
}
```

Measured against a deleter running flat out with six concurrent readers, this hit
**0.7%** of IDs from a single-key `NodesByProperty` and **4–11%** of IDs from a
typed `QueryNodeIDs` — a typed query returns more IDs over a longer call, so more
of them go stale before you reach them. Closing it would require snapshot
isolation, which Graphene does not offer, so treat any result set as candidates
rather than a guarantee.

A *sequence* of separate calls is not a transaction — but a `Tx` is. Use
`g.Begin()` when several writes must land together or not at all:

```go
tx := g.Begin()
caseID := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
fileID := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
tx.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})

if err := tx.Commit(); err != nil {
    // nothing was written — the store is exactly as it was
}
```

That covers creates, updates and deletes, and a `DeleteNode` inside it cascades
to the node's edges like the standalone call does. See
[API_REFERENCE.md](API_REFERENCE.md) §5.1.

**What it does not give you is isolation.** A transaction is not a snapshot: it
does not see a frozen view of the graph while open, and readers see its effects
as they land. So *read-decide-write* is still not safe against a concurrent
writer — reading inside a transaction reads live state, and nothing detects that
it changed before you commit. Enforce that invariant in your own code, or
serialise the writers.

## 6. Traversal and Multi-Hop Analysis

> **If you only need the IDs, use `BFSIDs`.** It walks the graph without
> building a single node or edge record — 20 allocations against 394 for the
> record-returning walk on the same 12-hop traversal. Reachability checks,
> scoping a pattern match, and feeding IDs into a follow-up query all want it.
>
> ```go
> ids, _ := g.BFSIDs(artID, 3, store.DirectionBoth, nil)
> ```

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

Scoping still matters most, but the matcher itself is no longer the bottleneck it
was: a two-hop pattern over a 2 000-node scope went from 24.6 ms and ~400 000
allocations to **4.75 ms and 150**, because edge checks now walk adjacency IDs
instead of materialising every candidate edge record.

## 9. Helper APIs and Result Utilities

### Batch and helper methods

- `AddNodes`, `AddEdges` — atomic ordered batch insert
- `Begin` → `*Tx` — creates, updates and deletes committing together (§5)
- `GetNodes`, `GetEdges` — resolve many IDs under one lock hold
- `Sync` — force pending writes durable without a full `Compact()`
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
- no compaction after ingest,
- a range or prefix filter on a key that was never declared ordered — that is a
  scan of the key's values, not an index lookup.

Fix:

- scope operations using multi-hop BFS,
- compact and reopen for read-heavy phase,
- `DeclareOrderedProperty(key)` before running range or prefix queries against
  it; on the benchmark fixture that is milliseconds against microseconds.

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
