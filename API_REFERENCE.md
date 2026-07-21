# GrapheneDB API Reference

Complete reference for consumers of the GrapheneDB Go library.

- Module path: `github.com/aoiflux/graphene`
- Primary packages: `graphene` (facade), `store` (types, interface, queries),
  `traversal` (algorithms + result types), `viz` (HTML export).

```go
import (
    "github.com/aoiflux/graphene"
    "github.com/aoiflux/graphene/store"
    "github.com/aoiflux/graphene/traversal"
    "github.com/aoiflux/graphene/viz"
)
```

## Contents

1. [Concepts and conventions](#1-concepts-and-conventions)
2. [Constructing a graph](#2-constructing-a-graph)
3. [Core types](#3-core-types)
4. [Errors](#4-errors)
5. [Create](#5-create)
6. [Read](#6-read)
7. [Mutation](#7-mutation)  ← update / delete
8. [Type lookups](#8-type-lookups)
9. [Property index](#9-property-index)
9a. [Ordered (range) keys](#9a-ordered-range-keys)
10. [Typed queries](#10-typed-queries)
11. [Degree & connectivity](#11-degree--connectivity)
12. [Traversal & patterns](#12-traversal--patterns)
13. [Subgraph, cycles, result helpers](#13-subgraph-cycles-result-helpers)
14. [Persistence lifecycle](#14-persistence-lifecycle)
15. [Visualization export](#15-visualization-export)
16. [Concurrency & guarantees](#16-concurrency--guarantees)
17. [Index maintenance](#17-index-maintenance)
18. [Query plans](#18-query-plans)

---

## 1. Concepts and conventions

- **Node** — an entity with one or more type labels and an opaque property blob.
- **Edge** — a directed, typed, optionally-weighted relationship between two
  existing nodes. Multiple/parallel edges between the same pair are allowed.
- **ID** — `NodeID` / `EdgeID` are `uint64`, assigned monotonically by the store
  and **never reused**. `0` is the invalid sentinel (`InvalidNodeID` /
  `InvalidEdgeID`).
- **Label / type** — `NodeType` / `EdgeType` are `uint16`. Built-ins occupy the
  low range; `[32768, 65535]` is reserved for user-defined custom types.
- **Property blob** — `[]byte`, opaque to the engine (typically msgpack/JSON).
  Fast lookups on individual fields come from the explicit property index.
- **Direction** — `DirectionOutbound`, `DirectionInbound`, `DirectionBoth`.
- **Error convention** — every method returns an `error`; read/mutate of a
  missing entity returns `*store.ErrNotFound`.
- **Two backends, one interface** — `NewInMemory()` and `Open(dir)` both satisfy
  the same API and return identical results for the same operations.

All methods below are on `*graphene.Graph` unless noted. `Graph` embeds
`store.GraphStore`, so interface methods are promoted onto it directly.

---

## 2. Constructing a graph

```go
func NewInMemory() *Graph
func Open(dir string) (*Graph, error)
```

- `NewInMemory()` — volatile, thread-safe store. Best for tests, prototyping,
  and small in-process graphs.
- `Open(dir)` — durable on-disk store rooted at `dir` (created if absent). On
  restart the WAL is replayed automatically. Call `Compact()` after bulk work.

```go
g := graphene.NewInMemory()
defer g.Close()

// or
g, err := graphene.Open("/data/cases/case-01")
if err != nil { /* ... */ }
defer g.Close()
```

---

## 3. Core types

### Node
```go
type Node struct {
    ID         NodeID     // assigned by the store; ignored on AddNode
    Labels     []NodeType // one or more; must not be empty
    Properties []byte     // opaque blob; nil is valid
}
func (n *Node) HasLabel(t NodeType) bool
```

### Edge
```go
type Edge struct {
    ID         EdgeID
    Src        NodeID     // must reference an existing node
    Dst        NodeID     // must reference an existing node
    Labels     []EdgeType // one or more; must not be empty
    Weight     float32    // similarity score for SimilarTo; 0 otherwise
    Properties []byte
}
func (e *Edge) HasLabel(t EdgeType) bool
```

### NeighbourResult
```go
type NeighbourResult struct {
    Node *Node
    Edge *Edge
}
```

### Built-in labels
| NodeType             | EdgeType            |
|----------------------|---------------------|
| `NodeTypeUnknown`    | `EdgeTypeUnknown`   |
| `NodeTypeEvidenceFile`| `EdgeTypeContains`  |
| `NodeTypeMicroArtefact`| `EdgeTypeSimilarTo`|
| `NodeTypeTag`        | `EdgeTypeReuse`     |
| `NodeTypeCase`       | `EdgeTypeTemporal`  |
|                      | `EdgeTypeTaggedWith`|
|                      | `EdgeTypeBelongsTo` |

### Custom labels
```go
func CustomNodeType(offset uint16) NodeType   // offset in [0, 32767]
func CustomEdgeType(offset uint16) EdgeType
func (t NodeType) IsCustom() bool
func (t EdgeType) IsCustom() bool

// Parse from strings; supports "case", "custom:7", "custom(7)", "custom-7", "130".
func ParseNodeType(selector string) (NodeType, error)
func ParseEdgeType(selector string) (EdgeType, error)
```

---

## 4. Errors

```go
type ErrNotFound struct { Kind string; ID uint64 }   // "node" or "edge"
type ErrInvalidEdge struct { MissingID NodeID }       // AddEdge with missing endpoint
```

Match with `errors.As`:

```go
var nf *store.ErrNotFound
if _, err := g.GetNode(id); errors.As(err, &nf) {
    // id does not exist
}
```

---

## 5. Create

```go
func (g *Graph) AddNode(n *store.Node) (store.NodeID, error)
func (g *Graph) AddEdge(e *store.Edge) (store.EdgeID, error)

func (g *Graph) AddNodes(nodes []*store.Node) ([]store.NodeID, error)
func (g *Graph) AddEdges(edges []*store.Edge) ([]store.EdgeID, error)
```

- `AddNode` — assigns and returns a fresh `NodeID`. `n.Labels` must be non-empty.
- `AddEdge` — `Src` and `Dst` must already exist (and not be deleted), else
  `*store.ErrInvalidEdge`. Returns a fresh `EdgeID`.
- `AddNodes` / `AddEdges` — ordered batch insert; on error the already-added
  prefix is **not** rolled back and the partial IDs are returned.

```go
caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
eid, _   := g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
```

---

## 6. Read

```go
func (g *Graph) GetNode(id store.NodeID) (*store.Node, error)
func (g *Graph) GetEdge(id store.EdgeID) (*store.Edge, error)
func (g *Graph) GetNodes(ids []store.NodeID) ([]*store.Node, error) // first miss errors
func (g *Graph) GetEdges(ids []store.EdgeID) ([]*store.Edge, error)

func (g *Graph) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error)
func (g *Graph) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error)

func (g *Graph) NodeCount() (uint64, error)
func (g *Graph) EdgeCount() (uint64, error)
func (g *Graph) Stats() (*GraphStats, error) // {NodeCount, EdgeCount}
```

- Pass `nil` `edgeTypes` to match all edge types; otherwise OR semantics.
- `Neighbours` deduplicates by neighbour node ID (one entry per neighbour).

> **Do not mutate returned structs.** For performance, the in-memory and
> delta-resident reads may hand back pointers into internal state. Treat
> `*store.Node` / `*store.Edge` as read-only; use `UpdateNode`/`UpdateEdge` to
> change them.

---

## 7. Mutation

Update and delete are first-class and **durable** (they survive restart and
compaction on the disk backend).

```go
func (g *Graph) UpdateNode(n *store.Node) error
func (g *Graph) UpdateEdge(e *store.Edge) error
func (g *Graph) DeleteNode(id store.NodeID) error
func (g *Graph) DeleteEdge(id store.EdgeID) error
```

### UpdateNode
Replaces the **labels and properties** of the node identified by `n.ID`.

- `n.ID` must reference an existing node → else `*store.ErrNotFound`.
- `n.Labels` must be non-empty.
- The node's ID never changes. The new labels/properties fully replace the old.

### UpdateEdge
Replaces the **labels, weight, and properties** of the edge identified by `e.ID`.

- **Endpoints are immutable.** Any `e.Src` / `e.Dst` you set are ignored; the
  edge keeps its original endpoints. To reconnect an edge, `DeleteEdge` it and
  `AddEdge` a new one.
- `e.ID` must exist → else `*store.ErrNotFound`. `e.Labels` must be non-empty.

### DeleteEdge
Removes a single edge and purges its property-index entries. Missing edge →
`*store.ErrNotFound` (safe to ignore for idempotent callers).

### DeleteNode
Removes the node **and cascades** to every edge incident to it (inbound and
outbound), so the graph never keeps an edge pointing at a missing node.
Property-index entries for the node and every cascaded edge are purged. Missing
node → `*store.ErrNotFound`.

### Semantics & guarantees

| Property | Behavior |
|----------|----------|
| Durability | Updates re-append a record; deletes append a tombstone. Both replay on restart. |
| ID reuse | Never. A deleted ID is not handed out again (monotonic counters). |
| Referential integrity | `DeleteNode` cascades; `AddEdge` onto a deleted node fails with `*ErrInvalidEdge`. |
| Property index | Purged on delete. **Not** auto-updated on update (indexed values are caller-encoded) — re-index changed fields yourself. |
| Space reclamation (disk) | A deleted/updated record still occupies its CSR slot until the next `Compact()`, which rebuilds without it. Reads never see it in the meantime. |
| Visibility | Effective immediately for all subsequent reads. |

```go
// Edit in place (endpoints on the edge struct are ignored).
_ = g.UpdateNode(&store.Node{ID: artID, Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("reclassified")})
_ = g.UpdateEdge(&store.Edge{ID: eid, Labels: []store.EdgeType{store.EdgeTypeReuse}, Weight: 0.42})

// Remove.
_ = g.DeleteEdge(eid)   // one relationship
_ = g.DeleteNode(artID) // node + all incident edges

// Idempotent delete.
if err := g.DeleteNode(artID); err != nil {
    var nf *store.ErrNotFound
    if !errors.As(err, &nf) { return err } // already gone is fine
}
```

> There is currently no batch `DeleteNodes`/`DeleteEdges`; loop over IDs. Deleting
> a node you are simultaneously reading is safe, but there is no cross-operation
> transaction — a group of mutations is not atomic as a unit.

---

## 8. Type lookups

```go
func (g *Graph) NodesByType(t store.NodeType) ([]store.NodeID, error)
func (g *Graph) EdgesByType(t store.EdgeType) ([]store.EdgeID, error)

func (g *Graph) NodesByAnyType(types []store.NodeType) ([]store.NodeID, error) // OR, deduped
func (g *Graph) EdgesByAnyType(types []store.EdgeType) ([]store.EdgeID, error)

func (g *Graph) NodesByTypeSelector(selector string) ([]store.NodeID, error)      // "case", "custom:7"
func (g *Graph) NodesByAnyTypeSelector(selectors []string) ([]store.NodeID, error)
func (g *Graph) EdgesByTypeSelector(selector string) ([]store.EdgeID, error)
func (g *Graph) EdgesByAnyTypeSelector(selectors []string) ([]store.EdgeID, error)
```

Results reflect live state: deleted entities never appear, and a label added or
removed by `UpdateNode`/`UpdateEdge` is honored.

---

## 9. Property index

The property index is a secondary index over caller-chosen `(key, value)` pairs.
It is **explicit and additive** — you register values you want fast lookups on;
the engine does not derive them from the property blob.

```go
func (g *Graph) IndexNodeProperty(id store.NodeID, key string, value []byte) error
func (g *Graph) IndexEdgeProperty(id store.EdgeID, key string, value []byte) error
func (g *Graph) IndexNodeProperties(id store.NodeID, props map[string][]byte) error
func (g *Graph) IndexEdgeProperties(id store.EdgeID, props map[string][]byte) error

func (g *Graph) NodesByProperty(key string, value []byte) ([]store.NodeID, error)
func (g *Graph) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error)
func (g *Graph) NodesByProperties(props map[string][]byte) ([]store.NodeID, error) // AND
func (g *Graph) EdgesByProperties(props map[string][]byte) ([]store.EdgeID, error)
func (g *Graph) NodesWithProperties(props map[string][]byte) ([]*store.Node, error)
func (g *Graph) EdgesWithProperties(props map[string][]byte) ([]*store.Edge, error)
```

- Use the same encoding for indexing and querying a value.
- **Delete** purges all of an entity's index entries automatically.
- **Update** does not: a stale indexed value keeps matching until the entity is
  deleted. Re-index changed fields with a new `IndexNodeProperty` call.

```go
_ = g.IndexNodeProperty(artID, "sha256", []byte("deadbeef"))
hits, _ := g.NodesByProperty("sha256", []byte("deadbeef"))
```

---

## 9a. Ordered (range) keys

Declaring a key builds a sorted structure over its values, so range and prefix
filters on that key are answered by binary search instead of by scanning every
entry registered under it.

```go
func (g *Graph) DeclareOrderedProperty(key string) error
func (g *Graph) DeclareOrderedEdgeProperty(key string) error
func (g *Graph) OrderedProperties() (nodeKeys, edgeKeys []string)
```

Entries already registered are absorbed, so a key can be declared at any point.

**Declaring a key changes how it compares.** Undeclared keys use the scan rule:
numeric when both sides parse, byte order otherwise. That is fine value by value
but is not a valid sort order — under it `"9" < "10" < "1x" < "9"`, a cycle — so
no sorted structure can be built on it. A declared key is compared **byte-wise
throughout**, in the index and in the residual filter alike.

Encode values so byte order matches your intent. `index/encoding` provides
order-preserving encoders:

```go
import "github.com/aoiflux/graphene/index/encoding"

g.IndexNodeProperty(id, "score", encoding.Int64(score))
g.DeclareOrderedProperty("score")

g.QueryNodes(store.NodeQuery{Filters: []store.PropertyFilter{{
    Key: "score", Op: store.PropertyOpBetweenInclusive,
    Value: encoding.Int64(100), ValueUpper: encoding.Int64(200),
}}})
```

| Encoder | Use for |
|---|---|
| `encoding.Int64` / `Uint64` | integers — do not hand-pad decimal strings |
| `encoding.Float64` | floating point, including negatives |
| `encoding.Time` | timestamps (Unix nanoseconds; valid 1678–2262) |
| `encoding.String` | text, which already sorts lexicographically |
| `encoding.PrefixUpperBound` | the exclusive end of a prefix range |

Values encoded with different encoders must not share a key — their byte ranges
are not comparable.

Equality lookups are unaffected either way. `PropertyOpContains` cannot be served
by any ordering and remains a scan.

A declaration is a runtime choice about how to index, not part of the stored
data: after reopening a store, re-declare the keys you want ordered.

---

## 10. Typed queries

Composable, deterministic queries with filters and pagination.

```go
func (g *Graph) QueryNodeIDs(q store.NodeQuery) ([]store.NodeID, error)
func (g *Graph) QueryNodes(q store.NodeQuery) ([]*store.Node, error)
func (g *Graph) QueryEdgeIDs(q store.EdgeQuery) ([]store.EdgeID, error)
func (g *Graph) QueryEdges(q store.EdgeQuery) ([]*store.Edge, error)
func (g *Graph) QueryRelationIDs(q store.RelationQuery) ([]store.EdgeID, error)
func (g *Graph) QueryRelations(q store.RelationQuery) ([]*store.Edge, error)
```

### Query structs
```go
type NodeQuery struct {
    IDs        []NodeID        // optional pre-filter
    Types      []NodeType      // OR semantics
    Filters    []PropertyFilter
    FilterMode MatchMode       // MatchAll (default) | MatchAny
    Order      QueryOrder      // QueryOrderAsc (default) | QueryOrderDesc
    Offset, Limit int          // pagination; Limit <= 0 = no cap
}

type EdgeQuery struct {
    IDs        []EdgeID
    Types      []EdgeType
    SrcIDs     []NodeID
    DstIDs     []NodeID
    Filters    []PropertyFilter
    FilterMode MatchMode
    Order      QueryOrder
    Offset, Limit int
}

type RelationQuery struct {
    Anchors      []NodeID   // node(s) to traverse from (required)
    Direction    Direction
    Counterparts []NodeID   // optional constraint on the far endpoint
    EdgeTypes    []EdgeType
    Filters      []PropertyFilter
    FilterMode   MatchMode
    Order        QueryOrder
    Offset, Limit int
}
```

### Property filters
```go
type PropertyFilter struct {
    Key        string
    Op         PropertyOp
    Value      []byte
    ValueUpper []byte // required for PropertyOpBetweenInclusive
}
```

| `PropertyOp` | Meaning |
|--------------|---------|
| `PropertyOpEqual` | exact byte match |
| `PropertyOpPrefix` | value is a prefix |
| `PropertyOpContains` | substring |
| `PropertyOpGreaterThan` / `…OrEqual` | numeric if both parse as float, else lexicographic |
| `PropertyOpLessThan` / `…OrEqual` | same |
| `PropertyOpBetweenInclusive` | `Value <= x <= ValueUpper` |

```go
page, _ := g.QueryNodeIDs(store.NodeQuery{
    Types:   []store.NodeType{store.NodeTypeMicroArtefact},
    Filters: []store.PropertyFilter{{Key: "score", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("40")}},
    Order:   store.QueryOrderDesc,
    Offset:  0, Limit: 50,
})
```

---

## 11. Degree & connectivity

```go
func (g *Graph) InDegree(id store.NodeID, edgeTypes []store.EdgeType) (int, error)
func (g *Graph) OutDegree(id store.NodeID, edgeTypes []store.EdgeType) (int, error)
func (g *Graph) Degree(id store.NodeID, edgeTypes []store.EdgeType) (int, error) // in + out
func (g *Graph) EdgeExists(src, dst store.NodeID, edgeTypes []store.EdgeType) (bool, error)
func (g *Graph) IsConnected(src, dst store.NodeID) (bool, error) // any-path reachability
func (g *Graph) NeighboursByNodeType(id store.NodeID, dir store.Direction, nodeType store.NodeType, edgeTypes []store.EdgeType) ([]*store.Node, error)
```

---

## 12. Traversal & patterns

```go
func (g *Graph) BFS(origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*traversal.BFSResult, error)
func (g *Graph) DFS(origin store.NodeID, maxDepth int, dir store.Direction, edgeTypes []store.EdgeType) (*traversal.BFSResult, error)
func (g *Graph) ProvenanceChain(origin store.NodeID, maxDepth int, edgeTypes []store.EdgeType) (*traversal.DFSResult, error)
func (g *Graph) ShortestPath(src, dst store.NodeID, edgeTypes []store.EdgeType) (*traversal.PathResult, error)
func (g *Graph) FindPatterns(pattern *traversal.Pattern, scope []store.NodeID, maxMatches int) ([]traversal.SubgraphMatch, error)
```

### Result & pattern types
```go
type BFSResult struct { Nodes []*store.Node; Edges []*store.Edge }
type DFSResult struct { Chain []*store.Node; Edges []*store.Edge } // Chain ordered origin -> root
type PathResult struct { Nodes []*store.Node; Edges []*store.Edge } // Src -> Dst inclusive

type PatternNode struct { ID int; Labels []store.NodeType }
type PatternEdge struct { SrcPatternID, DstPatternID int; Labels []store.EdgeType }
type Pattern struct { Nodes []PatternNode; Edges []PatternEdge }
type SubgraphMatch struct { Mapping []store.NodeID }
```

- `ShortestPath` returns `traversal.ErrNoPath` when no path exists.
- `FindPatterns`: `scope` limits candidate nodes (pass a BFS result's IDs);
  `maxMatches` of `0` means unlimited.

```go
walk, _ := g.BFS(fileID, 2, store.DirectionOutbound, nil)
path, err := g.ShortestPath(a, b, nil)
if errors.Is(err, traversal.ErrNoPath) { /* disconnected */ }
```

---

## 13. Subgraph, cycles, result helpers

```go
func (g *Graph) InducedSubgraph(nodeIDs []store.NodeID) ([]*store.Node, []*store.Edge, error)
func (g *Graph) HasCycle(origin store.NodeID, maxDepth int, edgeTypes []store.EdgeType) (bool, error)

// Nil-safe result adapters (package-level functions).
func NodesFromBFS(r *traversal.BFSResult) []*store.Node
func EdgesFromBFS(r *traversal.BFSResult) []*store.Edge
func NodeIDsFromBFS(r *traversal.BFSResult) []store.NodeID
func NodeIDsFromPath(r *traversal.PathResult) []store.NodeID
func FilterNodesByLabel(ns []*store.Node, label store.NodeType) []*store.Node
func FilterEdgesByLabel(es []*store.Edge, label store.EdgeType) []*store.Edge
```

---

## 14. Persistence lifecycle

```go
func (g *Graph) Compact() error   // disk only; no-op in memory
func (g *Graph) Close() error
```

Disk write model: **WAL (append-only) + in-memory delta overlay + CSR snapshot.**

1. Every `Add`/`Update`/`Delete` appends a record to the WAL and updates the
   in-memory delta immediately (reads see it at once).
2. `Compact()` merges the delta into a fresh CSR, drops deleted/updated-away
   records (reclaiming their space), atomically swaps the CSR file, and truncates
   the WAL.
3. On `Open()` the CSR is loaded and the WAL is replayed to restore the delta.

WAL record types: `0x01` node (add/update), `0x02` edge (add/update), `0x03`
node-property, `0x04` edge-property, `0x05` node tombstone, `0x06` edge
tombstone, `0xFF` checkpoint.

Typical bulk pattern:

```go
g, _ := graphene.Open(dir)
// ... ingest many nodes/edges, edit/delete as needed ...
_ = g.Compact() // rebuild CSR, truncate WAL, reclaim deleted space
_ = g.Close()
```

`Close()` flushes and releases the backend. Always defer it.

---

## 15. Visualization export

Offline, self-contained interactive HTML (no runtime backend connection).

```go
func viz.ExportInteractiveHTML(nodes []*store.Node, edges []*store.Edge, outPath string) error
func viz.ExportInteractiveHTMLWithOptions(nodes []*store.Node, edges []*store.Edge, outPath string, opts viz.ExportOptions) error

type viz.ExportOptions struct { Title string; Subtitle string }
```

The exporter renders a **snapshot** of the slices you pass. Because deletes are
real, re-exporting after a `DeleteNode`/`DeleteEdge` simply omits the removed
entities.

```go
ids, _ := g.QueryNodeIDs(store.NodeQuery{})
nodes, _ := g.GetNodes(ids)
eids, _ := g.QueryEdgeIDs(store.EdgeQuery{})
edges, _ := g.GetEdges(eids)
_ = viz.ExportInteractiveHTMLWithOptions(nodes, edges, "graph.html", viz.ExportOptions{Title: "Case 01"})
```

---

## 16. Concurrency & guarantees

Both backends are safe for concurrent use; every method takes the locks it needs
internally. What follows is what that safety does and does not buy you.

### Writes

**Every individual operation is atomic.** Each `Add*`/`Update*`/`Delete*` call
validates, appends to the WAL, and applies to memory under a single lock hold, so
operations never interleave into a half-applied state. `AddEdge` racing
`DeleteNode` on the same node resolves one of two ways — the edge is created
before the node is gone and is then cascaded, or it is rejected with
`ErrInvalidEdge` — and never leaves an edge pointing at a missing node. A
completed `DeleteNode` leaves no dangling edge and no index entry behind, in any
index, under any key.

**A sequence of calls is not a transaction.** There is no multi-operation
rollback and no snapshot isolation. If an invariant has to hold across several
calls — read-decide-write being the usual one — enforce it in your own code.

### Reads

A read returns data that was correct at some instant during the call:

> Every ID a lookup or query returns named an entity that was live at the moment
> it was checked, and every record it returns is internally coherent — an edge is
> incident to the node it was requested for, and a neighbour is that edge's far
> endpoint.

**The instant is inside the call, not after it.** By the time you act on a result
the entity may already be gone, and `GetNode` on an ID you were just handed can
legitimately fail. That is not a bug in the store, and closing it would require
snapshot isolation, which is not on offer.

How often it happens depends on how much the call returns and how long it takes.
Measured against a deleter running flat out with six concurrent readers:

| Call | IDs that no longer resolved |
|---|---|
| `NodesByProperty`, single key | **0.7%** |
| `QueryNodeIDs`, typed query | **4–11%** |

A typed query returns far more IDs over a longer call, so more of them go stale
before the caller reaches them. Treat any result set as candidates, and expect a
lookup on one to fail.

This guarantee is not free, and it is not automatic either. Property lookups
resolve their postings against the records before returning, because the index
and the records are separate structures under separate locks: `DeleteNode` holds
the store lock across the whole cascade, but a lookup that consulted only the
index could read postings the delete had not reached yet and hand back an entity
the records no longer had. Making the records the authority costs about 20 ns on
a raw single-key lookup and nothing measurable on the typed query path, which
already resolved its candidates that way.

The same filter covers a second case: index writes do not verify that the entity
exists, so an entry can outlive — or precede — any record. Such an entry is
invisible to reads and reported by `VerifyIndexes`.

### What is actually enforced

`graphene_consistency_test.go` asserts these properties under concurrent
mutation rather than leaving them as prose. It separates the two failure modes
that a naive test conflates: a lookup returning an entity whose deletion had
already *completed* is a torn read and fails the suite, while a lookup returning
an entity deleted after the lookup began is the benign race above and is counted
and logged, never failed. The deleter publishes progress through an atomic that
readers sample before each lookup, which is what makes the two distinguishable.

### Other properties

- Reads may return pointers into internal state for speed. Treat results as
  read-only and mutate exclusively through the API.
- Type-lookup and property-lookup results are unordered. The typed `Query*` APIs
  apply deterministic ordering and pagination.
- Durability boundary (disk): a write is recoverable once its WAL append returns.
  Space held by deleted or superseded records is reclaimed at the next
  `Compact()`.
- IDs are monotonic and never reused for the lifetime of a store, across restarts
  and compactions alike.

---

## 17. Index maintenance

### Keeping the property index truthful across updates

The engine cannot re-derive property-index entries on its own: indexed values are
supplied by you in your own encoding, and the `Properties` blob is opaque to the
storage layer. Updating an indexed entity therefore needs a choice, and the API
makes it explicit rather than silent.

```go
func (g *Graph) UpdateNodeIndexed(n *store.Node, props map[string][]byte) error
func (g *Graph) UpdateEdgeIndexed(e *store.Edge, props map[string][]byte) error
func (g *Graph) SetReindexPolicy(p store.ReindexPolicy)
func (g *Graph) ReindexPolicy() store.ReindexPolicy
```

**Prefer `UpdateNodeIndexed`.** It updates the record and replaces its index
entries in one step, so neither failure mode below can occur:

```go
_ = g.UpdateNodeIndexed(
    &store.Node{ID: artID, Labels: []store.NodeType{store.NodeTypeTag}},
    map[string][]byte{"sha256": newHash},
)
```

For plain `UpdateNode` / `UpdateEdge`, the policy decides:

| Policy | Behaviour | Failure mode |
|---|---|---|
| `store.ReindexKeep` (default) | entries are left alone | entries go **stale** — the old value still matches |
| `store.ReindexPurge` | the entity's entries are dropped | entries are **lost**, including ones the update did not touch |

On the disk backend a purge is journalled, so replay cannot resurrect superseded
values.

### Verification and repair

```go
func (g *Graph) VerifyIndexes() error
func (g *Graph) RebuildIndexes() error
```

`VerifyIndexes` cross-checks every index against the records it describes —
postings ordering and deduplication, postings ↔ reverse-map agreement in both
directions, label postings against live labels, adjacency against edge endpoints,
and that no index entry outlives its entity. It cannot check that an indexed
*value* still matches the entity's properties: those values are caller-encoded
and opaque.

`RebuildIndexes` recomputes everything derivable from the records — label
postings and adjacency — and drops property entries whose entity is gone. It
repairs structure, not content.

**Neither runs automatically on `Open`.** Verification is O(V+E) — roughly 200 ms
on a 100k-node store — and a damaged index section is already rejected while the
file is parsed, so the scan would be a startup tax for little gain. Run them
explicitly in tests, in CI, or when recovering a suspect store.

---

## 18. Query plans

```go
func (g *Graph) ExplainNodeQuery(q store.NodeQuery) (store.QueryPlan, error)
func (g *Graph) ExplainEdgeQuery(q store.EdgeQuery) (store.QueryPlan, error)
```

Reports how the planner resolves a query: which index drove it, how many
candidates that produced, and how each remaining filter was applied.

```go
plan, _ := g.ExplainNodeQuery(store.NodeQuery{Filters: []store.PropertyFilter{
    {Key: "sha256", Op: store.PropertyOpEqual, Value: hash},
    {Key: "tool", Op: store.PropertyOpContains, Value: []byte("acquire")},
}})
fmt.Println(plan)
// driver=equality(sha256) candidates=1 residual=tool:probe~100000 results=1
```

| Field | Meaning |
|---|---|
| `Driver` | `ids`, `equality`, `ordered`, `labels`, `adjacency`, or `scan` |
| `DriverKey` | the property key, when a filter drove the query |
| `DriverFilter` | index into the query's `Filters`, or −1 |
| `Candidates` | size of the driving set |
| `Residuals` | the remaining filters, in the order they were applied |
| `Results` | final result count |

A residual is applied one of two ways. `Probe` tests the candidates directly
through the index's reverse map, costing one lookup each. Otherwise the filter is
resolved to its own set and intersected, costing that set's size — which for a
filter no index can serve means scanning every entry under its key. `Cost` is the
planner's estimate of that set's size: exact for equality, and the key's entry
count otherwise.

**What this is for.** A query can return exactly the right answer while doing far
more work than it needed to, and the difference is invisible from the results —
a test asserting only on results cannot tell an index lookup from a full scan
that happened to agree with it. This is how that gets checked, in tests and by
hand.

`adjacency` appears only for edge queries, where an anchored query is bounded by
the incident-edge lists of its endpoints.

**`Probe` is a forecast, the rest is fact.** The executor re-decides probe versus
set at each step, because every step shrinks the candidate set and a filter not
worth probing against a thousand candidates may be worth it against the five that
survive. The plan reports the decision as it stands at the start of the pass; the
residual order and the cost estimates are exact.

**The plan is diagnostic output, not contract.** Which index the planner picks
may change as the cost model improves. The results a query returns may not.

`ExplainNodeQuery` runs the driving step for real, because the candidate count is
what decides how each residual is applied, then stops. So it costs the driver,
not the whole query — except for the result count, which requires running it.
