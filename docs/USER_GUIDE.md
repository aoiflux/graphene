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

`Open` takes an **exclusive lock** on the directory until `Close`. No other
process can open that store while you hold it, and trying returns
`disk.ErrStoreLocked` naming the holder's PID. That is deliberate: two processes
writing one store directory corrupts it, and previously nothing stopped them.

### Read-only store

Use when you only need to query — a report, a proof export, a dashboard reading
a case another tool owns.

```go
g, err := graphene.OpenReadOnly("./case-data")
if errors.Is(err, disk.ErrStoreLocked) {
    // a writer has it — retry, wait, or report; the engine will not wait for you
}
defer g.Close()
```

This takes a **shared lock**, so any number of read-only opens coexist. Every
mutating call returns `disk.ErrReadOnly`, including `Compact`, and nothing under
the directory is modified.

Two things to know before you rely on it:

- **A reader is refused while a writer holds the store**, not queued behind it.
- **The view is fixed at open and never advances.** The engine loads the store
  into memory once and does not re-read, so later writes by anyone are invisible
  to this handle for its whole life. Reopen to advance.

Those two are the same fact. If a reader were admitted alongside a writer it
would serve a snapshot that silently aged, which is a worse answer than being
told the store is busy.

If a store's previous holder crashed, the next writer's
`(*disk.Store).RecoveredFromUncleanShutdown()` reports it — and an
`AuditUncleanRestart` entry records it when `Options.Audit` is on. The store
still opens; WAL replay handles the crash. Use the flag to decide whether *this*
store warrants running `VerifyIndexes` before you trust it.

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

### Ingest the same source twice

`AddNode` always adds, so ingesting one APK, disk image or case file twice makes
two nodes for one thing. `UpsertNode` is the other verb: it resolves a natural
key to the entity already carrying it, or creates one.

```go
// Once, at every Open. This key is the entity's name, not just a queryable field.
if err := g.DeclareUniqueProperty("k"); err != nil {
    return err
}

id, created, err := g.UpsertNode("k", []byte("ver:9f3a"),
    &store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: blob},
    map[string][]byte{"state": []byte("extracted")})
```

`created` separates the first ingest from every later one. On a hit the labels
and properties are replaced, and each key named in the last argument is replaced
rather than added to — so a re-run whose extraction improved leaves one entity
holding the newer answer instead of two entities disagreeing. Keys the map does
not name keep the entries they had.

For a whole source in one commit — the entities and the edges between them —
`tx.UpsertNode` hands the ID back before commit, so it can be an endpoint:

```go
tx := g.Begin()
ver, _ := tx.UpsertNode("k", []byte("ver:9f3a"), verNode, nil)
perm, _ := tx.UpsertNode("k", []byte("perm:INTERNET"), permNode, nil)
tx.AddEdge(&store.Edge{Src: ver, Dst: perm, Labels: []store.EdgeType{store.EdgeTypeContains}})
err := tx.Commit()   // store.ErrWriteConflict if another writer took a key first
```

`UpsertEdge` is the same shape over a declared-unique *edge* property — key it on
something that names the relationship, such as `<srcID>:<dstID>:<kind>`. Both
require the key to be declared unique first; see §5.

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

> **The property index is not maintained for you — and since v0.5.0 it will not
> let you leave it wrong.** `UpdateNode` / `UpdateEdge` on an entity that carries
> index entries is refused with `*store.ErrIndexedPropertiesRequired`. Use
> `UpdateNodeIndexed` or `UpdateNodePartialIndex`, both below in §5.

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

> **If the store holds evidence, `DeleteNode` is probably not what you want.**
> It records nothing about who removed the entity or why, and after the next
> compaction there is no trace it existed — so a lawful erasure and a destruction
> of evidence are indistinguishable afterwards. The attributed forms
> (`RedactNodeProperties`, `RedactNode`) keep the actor, the reason and a proof
> that the removal happened. See §16 below and
> [FORENSICS.md](FORENSICS.md).

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

### Unique keys: when a value is a name

`DeclareOrderedProperty` makes a key *comparable*. `DeclareUniqueProperty` makes
it *identifying*: at most one live entity may hold any one value under it.

```go
err := g.DeclareUniqueProperty("k")
```

Declaring **validates what is already there**, and a graph that violates the
constraint is refused with every offending value named, not the first:

```go
var v *store.UniqueViolationsError
if errors.As(err, &v) {
    for _, c := range v.Conflicts {
        log.Printf("%q is held by %v", c.Value, c.IDs) // c.IDs has two or more
    }
}
```

That list is what a repair works from — one pass, rather than one pass per
duplicate. Remove or re-key the extras and declare again.

Three things to know:

- **Declarations are not persisted.** Unlike an ordered key, which is written
  into the image at compaction, a unique key must be declared by every process
  that opens the store. Declare at `Open`: it is idempotent, and it re-validates
  the data, which a persisted flag would not.
- **`NodeByProperty(key, value)` returns the one entity**, or
  `*store.ErrNotFound`. It refuses a key that is not declared unique rather than
  returning the first of several.
- **A backend that cannot enforce it returns an error** rather than `nil`. The
  ordered and composite declarations are optimisations, and a store ignoring one
  still answers every query correctly; this one is a promise about what the graph
  may contain.

From a shell, `graphene debug unique -key k <dir>` asks the same question of an
existing store and writes nothing.

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

When only one field moved, name only that field — everything else survives:

```go
_ = g.UpdateNodePartialIndex(n, map[string][]byte{"state": []byte("analyzed")})
```

Plain `UpdateNode` on an entity that carries index entries is **refused** by
default, because the engine cannot tell which entries the change invalidated:

```go
err := g.UpdateNode(n)   // *store.ErrIndexedPropertiesRequired
```

| Policy | Behaviour | What goes wrong |
|---|---|---|
| `store.ReindexReject` (default since v0.5.0) | the update is refused | nothing — you get an error instead of a wrong query result |
| `store.ReindexKeep` (the default before) | entries kept | they go **stale** — the old value still matches |
| `store.ReindexPurge` | entries dropped | they are **lost**, including untouched keys |

```go
g.SetReindexPolicy(store.ReindexKeep) // restore the pre-v0.5.0 behaviour
```

An entity with no index entries updates normally under any policy.

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
id, found, _ := g.EdgeIDBetween(srcID, dstID, nil) // the edge itself, not just yes/no
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

### Preventing duplicate edges

`UpsertNode` on a unique key keeps re-ingest from duplicating *entities*. The
equivalent for *structure* is a cardinality declaration:

```go
g.DeclareUniqueEdge(edgeOwns)   // at most one Owns edge per (src, dst)
```

Reach for this instead of `UpsertEdge` when the edges are bulk relationships —
"this version owns that method", by the million. `UpsertEdge` names an edge by a
declared-unique *property*, which costs an index entry per edge to restate what
the endpoints already say. `DeclareUniqueEdge` costs nothing stored at all; the
rule is checked against the source node's outbound adjacency on the way in.

```go
_, err := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{edgeOwns}})
var taken *store.ErrEdgeTaken
if errors.As(err, &taken) {
    useExisting(taken.Owner)   // the edge that already joins them
}
```

Three things to know before you declare one:

- **It is per label and per direction.** An edge labelled `{Owns, Contains}`
  counts against a declaration on each independently, and `a→b` and `b→a` are two
  different pairs.
- **Declare it at every `Open`.** Like unique property keys, declarations live in
  memory. Declaring twice is a no-op, so put it beside the other declarations.
- **It costs a scan of the source node's outbound edges per write.** That is
  cheap when sources fan out to tens or hundreds of things, and expensive when
  the source is a hub with a hundred thousand outbound edges. Declare it on
  ownership edges, not on the edges pointing *into* your shared vocabulary.

Declaring over a graph that already has duplicates tells you all of them:

```go
if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
    var v *store.EdgeCardinalityViolationsError
    if errors.As(err, &v) {
        for _, c := range v.Conflicts {
            log.Printf("%d -> %d joined by %v", c.Pair.Src, c.Pair.Dst, c.IDs)
        }
    }
}
```

or from the shell, without writing any code:

```bash
graphene debug unique-edge -type custom:0 ./store
```

#### Re-ingesting a source

The constraint is designed around this shape, and the important part is that a
transaction's own deletions free the pairs they were holding:

```go
tx := g.Begin()
for _, id := range g.ownedBy(version) {
    tx.DeleteNode(id)          // cascades to the edges
}
rebuildSubtree(tx, version)    // the same pairs, allowed again
if err := tx.Commit(); err != nil { ... }
```

Run twice, that produces the same graph rather than a second copy of it — and
anything the subtree merely *points at*, like a shared vocabulary node, is
untouched.

The thing to expect on the way there: **once the type is declared, re-running an
ingest that adds its edges unconditionally is refused rather than absorbed.**
That is the constraint working — the alternative is the silent second copy it
exists to stop — but it does mean a pipeline written against a store without the
declaration needs one of the two shapes, not neither:

```go
// Either: delete what this source owns, then rebuild it. One transaction.
// Or: ask first, and use the edge that already joins them.
if id, ok, err := g.EdgeIDBetween(src, dst, []store.EdgeType{edgeOwns}); err != nil {
    return err
} else if ok {
    useExisting(id)
} else {
    g.AddEdge(&store.Edge{Src: src, Dst: dst, Labels: []store.EdgeType{edgeOwns}})
}
```

Inside a transaction the refusal fails the whole commit, so a partial re-ingest
is not a state the store can be left in.

### Aggregates

The counts you would otherwise write a loop for.

```go
// Rank what a set of nodes point at, by how many of them point at it.
freq, _ := g.NeighbourFrequency(versions, store.DirectionOutbound,
    []store.EdgeType{edgeEmbeds}, []store.NodeType{nodeTracker})

// What is in this store?
byType, _ := g.CountNodesByType()

// How does an indexed field distribute?
states, _ := g.CountNodesByProperty("state")
```

`NeighbourFrequency` counts **anchors, not edges**: a neighbour reached twice
from one anchor is one vote. So `freq[tracker]` is "how many of these versions
embed it", which is almost always the question, rather than "how many edges point
at it", which depends on how you modelled the graph.

Use the `Ctx` form whenever the anchor list came from a query rather than from
you, and give it a budget — one hub in the anchor set is enough to make the work
unbounded:

```go
freq, err := g.NeighbourFrequencyCtx(ctx, anchors, store.DirectionOutbound,
    nil, nil, store.Budget{MaxEdges: 5_000_000})
if errors.Is(err, store.ErrBudgetExceeded) {
    // narrow the anchor set, or the edge types
}
```

**A multi-label entity is counted under every label it carries**, so the per-type
counts total more than `NodeCount`. That is not a bug and the CLI says so; if you
want a partition rather than a breakdown, count a label you only ever apply
alone.

### Naming your custom types

If you use the custom label range, name it. Otherwise everything the engine
prints — logs, errors, CLI tables, exported visualisations — says `Custom(7)`,
and you end up maintaining a translation table that can drift from the data.

```go
g.DeclareTypeNames(
    map[store.NodeType]string{nodeApp: "App", nodeVersion: "AppVersion"},
    map[store.EdgeType]string{edgeOwns: "Owns", edgeEmbeds: "Embeds"},
)
```

Put it beside your other declarations at every `Open` — it is a no-op the second
time. On a disk store the table is written to `graphene.labels` next to the
image, which is the part that matters: **the store stays readable without your
program**. Someone handed the directory a year from now sees `App`, not `32768`.

`String()` renders the name and the parsers accept it, on top of everything they
accepted before — `custom:0`, `Custom(0)` and the bare number all still resolve.
Only the custom range can be named.

If `Open` returns `store.ErrTypeNameConflict`, the store's table disagrees with
what this process already registered. That is the engine refusing to render one
store's data with another store's names; reconcile the numbering, or open them in
separate processes.


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
- `ForEachNodeBatch`, `ForEachEdgeBatch` — the same, in batches bounded by
  payload bytes rather than by ID count, for resolving more IDs than you want
  records in hand at once. `GetNodesBounded`/`GetEdgesBounded` are one step of
  that loop. See API_REFERENCE §6 for what the bound does and does not count.
- `ForEachNodeProjection`, `ForEachEdgeProjection` — indexed property values for
  many IDs without reading the records that carry them; exact for indexed keys
  only. `GetNodesProjected`/`GetEdgesProjected` are the materialising forms.
  Cheaper than reading records once the payloads no longer fit in page cache,
  dearer while they do — API_REFERENCE §6 has the crossover.
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

GrapheneDB disk mode uses WAL + delta + CSR compaction. A write goes into the
log and into an in-memory delta; a compaction folds the delta into a new image
and truncates the log.

### The small case: ingest, compact, close

If everything you are ingesting fits comfortably in memory, this is the whole
lifecycle and there is nothing to tune.

```go
g, _ := graphene.Open("./case-data")
// ingest + index
_ = g.Compact()
_ = g.Close()
```

**"Comfortably" means the delta and the property index it creates, not the file
on disk.** The rule of thumb is that a record costs about its own size in the
delta *plus* roughly 107 bytes for every index entry the writes create, so a
store with thirteen declared keys per record multiplies a 338-byte record into
about 1.9 KiB held. §11 below has the coefficients and §9.7 of
`docs/MEMORY_MODEL.md` has how they were taken.
Below a few hundred thousand records at ordinary sizes, take this path.

### The large case: bound the delta *and* reopen

Past that, the recipe above is the wrong one, and this is worth stating plainly
because it was the recipe this guide gave until v0.9.0. Ingesting everything and
compacting once at the end holds **anonymous memory exactly linear in the record
count**: measured at 3.2 KB records, 510 MiB at 100,000 and 1,010 MiB at 200,000,
which is 10% of a 2 GiB ceiling left for a store of 757 MiB on disk. It does not
reach 400,000. The same shape under the recipe below ingests 400,000 holding
98 MiB.

Two settings fix it and **they are one setting**. Neither works without the
other:

```go
g, err := graphene.Open("./case-data")
if err != nil {
    return err
}

// Bound what the delta may hold before a compaction is due. 32 MiB is the
// figure MEMORY_MODEL section 9.8 measured passing at 1.4M nodes; the shipped
// default of 128 MiB is not.
policy := store.CompactionPolicy{MaxDeltaBytes: 32 << 20}

for _, chunk := range chunks {
    if _, err := g.AddNodes(chunk); err != nil {
        return err
    }
    // index the chunk...

    due, _ := g.ShouldCompact(policy)
    if !due {
        continue
    }
    // Compact *and reopen*. The reopen is not optional -- see below.
    next, err := g.CompactAndReopen()
    if err != nil {
        return err
    }
    g = next
}

if err := g.Compact(); err != nil { // the final fold
    return err
}
return g.Close()
```

**The reopen also makes the compaction itself cheaper**, which was not why it is
here. Because `CompactAndReopen` is going to close the handle, it does not build
the in-memory graph a compaction normally publishes — it writes the merged
records straight out. That is **117.4 bytes a record** not allocated and **23.1%
off the anonymous peak while the compaction runs**, measured over four interleaved
rounds and recorded in `docs/benchmarks.md`. The image is byte-identical to the one
`Compact()` writes. Nothing in this recipe changes for it; it is the same call.

**Why the bound alone is not enough.** A compaction writes a new image, but the
records this process is *holding* still carry their own payload bytes, because
`AttachBase` — the step that makes a record address the mapped file instead of
the heap — runs on the load path, and a compaction is not one. So the payload
term does not fall at a compaction; it ratchets, once per record written, for
the life of the handle. §9.8 measured it climbing **49.0 MiB per 100,000
records and never resetting**. Reopening is what makes the records address the
file again.

**Why the reopen alone is not enough.** A reopen sheds what accumulated since
the last one, so how often it happens is the setting that matters — and what
makes it happen is the delta bound. The same sequence at the default 128 MiB
reopens five times instead of fifteen and still fails.

**On a rebuild it is also faster**, which was not the argument for it: 242 s for
the whole sequence against 469 s for an arm that never finished. Twenty-nine
compactions against a heap that never stops growing cost more than fifteen
against one that does not.

**On an ingest it is slower, and increasingly so** — 1.8× at 100,000 records and
3.0× at 200,000. Both are true, and the difference is what the two workloads
start from. A rebuild that does not bound dies; an ingest that does not bound is
faster right up until it dies. The wall clock follows the bytes written, which
the next section puts a number on.

### What it costs on disk

Bounding the delta trades memory for writes, and the trade is steep, because
every compaction rewrites the *whole* image. Measured on a bulk ingest of
3.2 KB records at a 32 MiB bound:

| records ingested | store on disk | bytes written | amplification | anonymous, ingesting |
|---:|---:|---:|---:|---:|
| 100,000 | 378.7 MiB | 2.42 GiB | 6.5× | 94.6 MiB |
| 200,000 | 757.3 MiB | 8.71 GiB | 11.8× | 76.3 MiB |
| 400,000 | 1,514.3 MiB | 31.40 GiB | 21.2× | 98.2 MiB |

**Read those two columns together, because they are the trade.** Anonymous
memory is flat across a fourfold size range — that is the bound doing exactly
what it is for. The amplification grows by 1.80× every time the record count
doubles, measured twice, because the number of compactions and the mean size of
each both grow with the data. Extrapolating the same fit, ten million records at
this shape would write on the order of a terabyte to store thirty gigabytes.

So raise the bound as far as your memory ceiling allows rather than as far as it
will go: memory is linear in the bound and writes are linear in its reciprocal.
And if you are filling an *empty* store at that scale, do not sit on this curve
at all — the next section is the one-pass loader, and this table is why it exists.

### The largest case: fill an empty store in one pass

`BulkLoad` writes the image once. It is the right call when you are creating a
store rather than adding to one — an import, a migration, a nightly rebuild into a
fresh directory — and it is the only thing here that takes the amplification above
off the table instead of moving along it.

```go
g, err := graphene.Open(dir)
if err != nil {
    return err
}
// Declare first. The image is written under whatever is declared when the load
// starts; declaring afterwards means the next compaction, not this one.
if err := g.DeclareUniqueProperty("sha256"); err != nil {
    return err
}
if err := g.DeclareCompositeProperties([]string{"tool", "case_id"}); err != nil {
    return err
}

ids := make([]store.NodeID, 0, len(records))
g, err = g.BulkLoad(
    func(w *graphene.BulkNodeWriter) error {
        for _, r := range records {
            id, err := w.AddNode(graphene.BulkNode{
                Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
                Properties: r.Blob,
                Index: []graphene.BulkProperty{
                    {Key: "sha256", Value: r.Digest},
                    {Key: "tool", Value: r.Tool},
                },
            })
            if err != nil {
                return err
            }
            ids = append(ids, id)
        }
        return nil
    },
    func(w *graphene.BulkEdgeWriter) error {
        for _, e := range edges {
            if _, err := w.AddEdge(graphene.BulkEdge{
                Src:    ids[e.From],
                Dst:    ids[e.To],
                Labels: []store.EdgeType{store.EdgeTypeContains},
            }); err != nil {
                return err
            }
        }
        return nil
    })
if err != nil {
    return err
}
defer g.Close() // the handle you passed in is already closed
```

Two passes, because the image holds every node record before every edge record.
`AddNode` hands back the identifier as it goes, which is how the edge pass names
its endpoints — so a load reads its source twice, and a source you cannot read
twice (a pipe, a one-shot decoder) has to be staged first.

**Measured against the bounded-plus-reopen recipe above**, same store, same schema,
same 3.2 KB records, four interleaved rounds under a real 2 GiB ceiling:

| 100,000 records | bounded + reopen | `BulkLoad` |
|---|---:|---:|
| bytes written | 2.42 GiB | **0.37 GiB** |
| amplification | 6.5× | **1.0×** |
| peak charged | 249.3 MiB | **76.3 MiB** |
| headroom under 2 GiB | 87.8% | **96.3%** |
| wall clock | 25.1 s | **4.4 s** |

And what the write itself holds is flat: 10.3 to 12.7 MiB across a sixteenfold
range of record counts, because the index sort spills to a file in the store's own
directory rather than growing. `Options.Compact.MaxWorkingBytes` is the figure that
sizes it — the same one that sizes a compaction's intermediates, so you do not meet
a second knob.

**Three things it gives up, and you should decide about them before you reach for
it.**

*It is atomic and not incremental.* A crash during a load means the load did not
happen. There is no partial store and nothing to resume from — you start again.
That is the standard bulk-load contract and it is what buys the single pass.

*The store is not readable while it runs*, and the handle you called it on is
closed. It hands back a new one, exactly as `CompactAndReopen` does, and for the
same reason: the records it wrote were never registered with the index the old
handle holds. A `Properties` slice taken before the call addresses a released
mapping afterwards — copy first, with `store.CloneNode`.

*It needs an empty store.* A store holding records, a delta, or even an index entry
naming a record that was never added is refused with `disk.ErrBulkLoadNotEmpty`.
Load into a fresh directory; to replace an existing store, load into a new one and
swap.

Everything else the incremental path checks, a load checks too, including unique
keys: two records loaded under one value for a declared unique property fail the
load, before anything is installed.

### Let the engine refuse what it cannot afford

Since v0.9.0 the engine will decline an input it cannot hold, rather than
accepting it and hoping. Set a budget — or ask it to find one:

```go
opts := disk.Options{
    // Read the cgroup limit, Job Object or machine memory this process is
    // already under, and derive the budget from it.
    DiscoverMemoryBudget: true,
}
g, err := graphene.OpenWithOptions("./case-data", opts)
```

The batch caps come from that budget without a second decision, so a caller who
turns the budget down gets smaller batches automatically. An oversized batch
comes back as `disk.ErrBatchTooLarge` carrying both the offending figure and
the cap, before any ID is taken and before the lock:

```go
if _, err := g.AddNodes(huge); errors.Is(err, disk.ErrBatchTooLarge) {
    // split it, or use the splitting form below
}
```

`AddNodesInBatches` and `AddEdgesInBatches` do the splitting for you. **They are
not atomic**: `AddNodes` documents that either every node is added or none is,
and the splitting forms deliberately give that up — an error part-way leaves the
earlier chunks written. That is the trade, and it is the reason they are a
separate call rather than a flag on the existing one.

### For a dump rather than a program

`graphene import graph` takes the configuration above by default: `-bound 32`
with a reopen after every interim compaction. `-bound 0` restores the old
unbounded behaviour for a dump you know fits.

## 11. Memory and capacity planning

What a store costs to hold, and every bound you can put on it.

### The two sizing rules

**At rest, the ceiling binds on node count and schema, not on bytes on disk.**
A store that has been compacted and reopened holds about **178 bytes of heap per
node** plus its schema's per-entry index cost, and charges roughly **300–310
bytes per node** against an OS limit. Blob size sets the file and the page cache
and does not set the ceiling: measured across a twentyfold difference in blob
size — 1.4M × 512 B against 1.8M × 10 KiB — every per-node term agrees to two
decimal places.

| term | B/node | moved by |
|---|---:|---|
| record arrays | 56.0 | nothing yet |
| record payloads | 2.0 | already mapped — this is a slice header, not the blob |
| label postings | 8.0 | nothing; 4.5% of heap |
| adjacency | 16.0 | `Options.Adjacency` (deferred to first use) |
| composites | 96.0 | two declared composites; 54% of modelled heap |
| **modelled heap** | **178.1** | |
| charged, peak | 300–310 | |

An 18.4 GiB store of 1.8M fat nodes opens in **527 MiB charged against a 2 GiB
limit, 74.3% headroom**. 7,071 MiB of its 7,598 MiB resident is file-backed —
the mapped image's pages, which the kernel can drop and re-fault, and which a Job
Object's commit charge never sees.

**While writing, the figure that grows is not the one `MaxDeltaBytes` counts.**
`DeltaBytes` counts record payloads. The property index those same writes created
is not in it, and at a heavily indexed shape it is the larger of the two. The
multiplier is:

> held ≈ recordBytes × (1 + 107 × entries / recordBytes)

where `entries` is how many index entries one record creates — one per declared
unique or ordered key it carries, plus one per declared composite. At 338-byte
records with thirteen entries each, that is **1,893 bytes held against 338
watched: 5.6×**. At 3.2 KB records with the same schema it is about 1.45×.

### Three worked shapes

**Small records, heavily indexed** — 338 B, 13 entries/record. The index
dominates: at 400,000 uncompacted records the resident property index was
546.4 MiB against the delta's 295.0. Set `MaxDeltaBytes` to roughly a fifth of
what you can afford, because five times it is what you will hold. Or set
`MaxResidentBytes`, which is the one rule that sees the index.

**Fat records, same schema** — 3.2 KB, 13 entries/record. The multiplier is
small, so `MaxDeltaBytes` is close to honest. The binding cost is elsewhere: at a
32 MiB bound an ingest writes 6.5× the store at 100,000 records and 11.8× at
200,000, and that ratio keeps doubling. Set the bound as high as your ceiling
allows.

**Read-only** — no delta, no writes. Take the defaults for the residency modes
and you get neither the mapped image nor the mapped index and pay about 3.8× what
you need to. A live reader should ask for both explicitly.

### Every bound, and what it does not cover

| bound | unit | default | does **not** cover |
|---|---|---|---|
| `Options.MemoryBudget` | bytes | 0 (unlimited) | anything between `Open` and `Compact`. It is a refusal at two moments, not a cap — and at a compaction it refuses the operation that would have relieved the pressure, leaving a delta that keeps growing |
| `Options.DiscoverMemoryBudget` | bool | false | darwin, which can only read `hw.memsize`. A discovery that finds nothing leaves the budget at zero, which is unlimited — check `StorageStats.MemoryBudgetSource` |
| `Options.MaxBatchBytes` | bytes | 2% of the budget, else uncapped | a single record. A batch of one is always admitted, because these cap how many records share a commit, not how large a record may be |
| `Options.MaxBatchRecords` | count | `MaxBatchBytes` ÷ 80, else uncapped | the same |
| `Options.DeltaSoftLimit` | bytes | 0 | anything. It reports — `StorageStats.DeltaOverBudget` and one metric per crossing — and never acts |
| `Options.Compact.MaxWorkingBytes` | bytes | 4,325,376 (floor 524,288) | everything a compaction holds except the four sort intermediates. Raising it is measured to buy nothing; the floor saves 3.32 MiB for 12–42% of the compaction's wall clock. It also sizes a `BulkLoad`'s index sort, which is the whole of what a load holds — flat at 10.3–12.7 MiB across a sixteenfold size range |
| `CompactionPolicy.MaxDeltaRecords` | count | 100,000 | how large those records are |
| `CompactionPolicy.MaxDeltaBytes` | bytes | 128 MiB | the property index — see the multiplier above. **The default does not hold a large ingest**; §10 has the figure that does, and a load into an empty store should not be on this axis at all |
| `CompactionPolicy.MaxWALBytes` | bytes | 256 MiB | anything resident. The log is on disk |
| `CompactionPolicy.MaxDeltaRatio` | fraction | 0.5 | a large delta against a large image |
| `CompactionPolicy.MaxResidentBytes` | bytes | 0 (off) | the allocator's working set, the collector's headroom, or a compaction's transients. `EstimatedResidentBytes` models *retained* heap, and a process charges roughly **2.6× it** during a rebuild. Read it as a floor, not a budget |
| `GOMEMLIMIT` | bytes | unset | mapped file pages, which is the right exclusion — and the live set, which is most of what an unbounded ingest holds. It is soft: it reclaims garbage and cannot shrink live data. Measured below; **graphene never sets it**, because it is process-global and this is a library in your process |

Everything in that table bounds the **anonymous** class — the one that OOM-kills.
Two more bound the resident, file-backed one, which nothing above touches and which
at an ingest is the larger of the two: 1,182 MiB against 98.2 MiB at 400,000 records.

| bound | unit | default | does **not** cover |
|---|---|---|---|
| `Options.ResidentAdvice` | bool | false | windows, and anything outside linux and darwin — where it reports `MetricResidentAdvice` rather than silently doing nothing. It does **not** reduce what a compaction holds *while it runs* (measured at 0.994); it returns what the compaction was finished with, after the fact |
| `disk.LimitWorkingSet` | bytes | not called | linux and darwin, which have no enforced per-process resident cap worth using. It is a **function, not an option**, because it caps the whole process — including whatever else is linked into it |

`ResidentAdvice` is measured at **88.0% less file-backed residency after a
compaction, 8.35×**, with the anonymous class flat and the Go heap unmoved. The
trade is **more major faults**: a page dropped and then read again costs a fault,
so a store that compacts and then serves reads out of those pages pays for all of
them. Only the residency half of that has been measured, which is why it is off by
default. Turn it on for a bulk ingest; think before turning it on for a store that
serves reads. `docs/benchmarks.md` carries the table and the caveats.

`LimitWorkingSet` **trims rather than kills**: the kernel takes pages out of the
process instead of failing an allocation, so nothing dies and the program gets
slower. It reads the cap back from the kernel and reports whether it is actually
hard, because a cap that silently failed to apply is worse than no cap.

### The one thing none of them bound

A compaction's own transient. `EstimateResident` models what the store retains;
it does not model what a rebuild peaks at, and the gap is a factor of about 2.6.
If you are sizing against a hard ceiling, measure the rebuild rather than
modelling it — `tests/ceiling_test.go` is the harness, and `docs/MEMORY_MODEL.md`
§9 is what it has established so far.

### `GOMEMLIMIT`, if you set it

It is the one bound that is identical on linux, windows and darwin, needs no
privilege, and covers exactly the right class — Go-managed heap, stacks and
runtime metadata, and **not** mmap'd file pages. It is also the only bound darwin
will ever have, since `RLIMIT_AS` counts the mapped image and `RLIMIT_RSS` is
unenforced on every modern kernel.

Swept on the ingest configuration above — 100,000 × 3.2 KB, delta bounded at
32 MiB with a reopen, under a 2,048 MiB limit — it does this:

| `GOMEMLIMIT` | charged | headroom | ingest wall |
|---|---:|---:|---:|
| unset | 252.7 MiB | 87.7% | 13.78 s |
| 512 MiB | 233.0 | 88.6% | 14.26 s |
| 256 MiB | 252.3 | 87.7% | 13.56 s |
| **128 MiB** | **176.6** | **91.4%** | **13.92 s** |
| 64 MiB | 159.7 | 92.2% | **20.87 s** |

Three things to take from it.

**Anything above what the process already charges does nothing.** 1024, 512 and
256 MiB are indistinguishable from unset here, because this arm was charging
252.7 to begin with. Measure what your process charges, then set the limit below
it or do not bother.

**The first setting that bites is close to free, and the next one is not.**
128 MiB takes 76 MiB off the peak for wall clock inside the noise. 64 MiB takes a
further 17 and costs **51% of the ingest's wall clock**. That is the soft limit
doing what a soft limit does: it cannot shrink a live set, only collect harder
around one, so as it approaches the live floor the collector runs more and more
often to hold a line it can no longer move. Below the live floor it does not fail
— it simply stops making progress, which is worse, because a refusal is visible
and a spiral is not.

**What it reaches is the gap, never the data.** The engine's model of what this
ingest holds peaks at 43.4 MiB while the process charges 252.7; the 209 MiB in
between is garbage not yet collected and arena not yet returned to the OS, and
that is the entire budget `GOMEMLIMIT` has to work with. It will not rescue an
unbounded ingest, whose growth is live records — bound the delta and reopen for
that, as §10 describes, and treat this as a trim on top.

`docs/MEMORY_MODEL.md` §9.10 has the full sweep and what it does not establish.
**Graphene will never set this for you**: it is process-global and this is a
library running inside your program.

## 12. Visualization

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

## 13. End-to-End Large Workflow

Typical "push limits" workflow:

1. Define data model and key conventions.
2. Ingest large graph (including bucket keys), **bounding the delta and reopening
   as §10 describes** — step 4 on its own is the recipe that does not scale.
3. Add connection-rich edges for multi-hop analysis.
4. Compact (the final fold).
5. Reopen and validate counts.
6. Run type/property lookups.
7. Run BFS/DFS/provenance/path.
8. Run connectivity, degree, induced subgraph, pattern matching.
9. Export visualization and archive run metadata.

## 14. Commands

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
go test ./tests/ -tags=stress -run TestStress
```

## 15. Troubleshooting

### AddNode or AddEdge fails

Cause:

- source or destination node does not exist,
- the label set is empty — `store.ErrNoLabels`, refused since v0.5.0 because a
  label-less entity indexes into no posting and is invisible to every
  type-filtered query while still being counted.

Fix:

- create nodes first, then create edges,
- give every node and edge at least one label.

### UpdateNode returns ErrIndexedPropertiesRequired

Cause:

- the entity carries property-index entries, and a plain `UpdateNode` cannot
  know which of them the change invalidated. Refusing is the default since
  v0.5.0.

Fix:

- `UpdateNodeIndexed(n, props)` when you are restating every indexed key,
- `UpdateNodePartialIndex(n, changed)` when only one field moved,
- `g.SetReindexPolicy(store.ReindexKeep)` to restore the pre-v0.5.0 behaviour,
  knowing the entries then go stale.

### Re-ingesting a source duplicates it

Cause:

- `AddNode` adds; it has no way to recognise the entity it already wrote.

Fix:

- give the entity a natural key, `DeclareUniqueProperty` it at every `Open`, and
  ingest with `UpsertNode` / `tx.UpsertNode` (§4).

### DeclareUniqueProperty fails on an existing store

Cause:

- the data already violates the constraint. Declaring validates before it
  promises anything.

Fix:

- read `*store.UniqueViolationsError.Conflicts` — it names **every** offending
  value and its holders — remove or re-key the extras, and declare again.
  `graphene debug unique -key <k> <dir>` reports the same list from a shell.

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

---

## 16. Evidence and integrity

Everything up to here works the same whether you are storing a social graph or a
case file. This section is for the second one.

Full treatment lives in three places, and the split matters:
[SECURITY.md](../SECURITY.md) says what each mechanism **proves and does not**,
[FORENSICS.md](FORENSICS.md) is the **working guide**, and
[API_REFERENCE.md §22](API_REFERENCE.md#22-best-practices-for-evidentiary-use)
is the **checklist**. This is the orientation.

### It is all opt-in, and `Open` does not turn it on

```go
g, _ := graphene.Open(dir)   // unsigned, unverified, unaudited — the default
```

That is the right default for a graph database and the wrong one for evidence.
Ask for the other posture explicitly:

```go
key, pub, _ := signing.GenerateKey(1)
ring := signing.NewKeyring()
ring.Add(1, pub)

opts := disk.StrictOptions(key, ring, operatorActorID)
opts.Retention = disk.RetentionPolicy{MaxSegments: 50}  // keep commit history
opts.Redaction = true                                   // attributed removal
opts.Roles = true                                       // record privilege changes

g, _ := graphene.OpenWithOptions(dir, opts)
s, _ := g.Forensics()   // the machinery; false on the in-memory backend
```

`Forensics` returns the same store the Graph is using, not a copy.

### The four things worth knowing on day one

**Attribute your writes.** A bare `Begin()` commits anonymously and attribution
cannot be added later:

```go
tx := g.Begin().As(store.TxContext{ActorID: 7, RoleID: 3, KeyID: 1})
```

**Compaction is what makes things provable.** Snapshot roots and the attestation
over them are written by `Compact()`. Before the first one an entity is live but
unaccounted for — which is not the same as absent.

**Retain a root outside the system.** `SnapshotRoots().Snapshot` is the value to
keep. Every check the engine runs compares the store against itself; only a copy
you hold elsewhere distinguishes "internally consistent" from "not tampered
with".

**Hand over proofs, not files.** A few hundred bytes proving one entity was in
one snapshot, disclosing nothing about any other:

```go
blob, _ := s.ExportNodeProof(id)                       // you
proof, _ := disk.UnmarshalProof(blob)                  // them, with no store
err := disk.VerifyExportedProof(retainedRoot, proof)   // root from elsewhere
```

### Erasing something lawfully

```go
impact, _ := s.RedactionImpactFor(id)   // what a full removal would take with it
rec, _ := s.RedactNodeProperties(id, disk.RedactionRequest{
    ActorID: 7, RoleID: 3, Reason: "subject access request 41",
})
```

`RedactNodeProperties` removes the property blob and keeps the entity, its
labels and its edges — usually what an erasure request actually asks for.
A reason is **required**: an unexplained redaction is indistinguishable from
destroying evidence.

Compact afterwards, or the removal is known to the ledger and not to the image.

### From a shell

```
graphene provenance custody -node 7 <dir>    account for one entity across every history
graphene redaction impact -node 7 <dir>      what a removal would take with it — changes nothing
graphene redaction apply -node 7 -actor 3 -reason "..." -confirm <dir>
graphene redaction list <dir>                who removed what, when, and why
graphene grant list <dir>                    who was permitted to do it
graphene provenance export -node 7 -out c.gprf <dir>
graphene provenance verify -root <hex> c.gprf     # needs no store
```

`redaction impact` is a pure dry run and is available while refusing is still
possible, which is the point of it: the cost of a node redaction is the edges it
cascades to, and that is worth knowing before rather than after.

The rest of the shell surface, in one place:

```
graphene node list -type MicroArtefact -prop sha256=d4e5 <dir>
graphene traverse path -from 7 -to 42 <dir>
graphene traverse bfs -from 7 -depth 3 -viz out.html <dir>
graphene store health <dir>                  counts, compaction, lock, verification
graphene backup restore -to D -at-commit 4711 <backup>
graphene maintenance compact -confirm <dir>
graphene debug integrity <dir>
```

The flat spellings these commands had before groups existed — `custody`,
`redactions`, `grants`, `prove`, `verify-proof` — all still work and are not
going away; they are hidden from help rather than removed.

`graphene help` lists everything, and `-json` on any command gives a document
with a stable schema instead of a report. `-metrics` reports what the engine
actually did — commits, fsyncs, queries, and the replay the open performed.

Anything that changes bytes already on disk needs `-confirm` and supports
`-dry-run`. A dry run opens the store *read-only*, so it cannot write by
construction rather than by a handler remembering to check a flag.

### What none of it does

Graphene is a library in your process. Anything running there can call any API
and use your signing key, so **nothing here prevents anything** — it makes the
result detectable to someone outside. There is no access control: `RoleID` is
recorded and never checked. And until you supply an anchor, every guarantee is
the store vouching for itself.

`go run ./examples` executes the whole flow end to end.
