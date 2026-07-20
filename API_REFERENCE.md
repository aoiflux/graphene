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
10. [Typed queries](#10-typed-queries)
11. [Degree & connectivity](#11-degree--connectivity)
12. [Traversal & patterns](#12-traversal--patterns)
13. [Subgraph, cycles, result helpers](#13-subgraph-cycles-result-helpers)
14. [Persistence lifecycle](#14-persistence-lifecycle)
15. [Visualization export](#15-visualization-export)
16. [Concurrency & guarantees](#16-concurrency--guarantees)

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

- Both backends are safe for concurrent use; each method takes the appropriate
  lock internally.
- **Every individual operation is atomic.** Each `Add*`/`Update*`/`Delete*` call
  validates, appends to the WAL, and applies to memory under a single lock hold,
  so operations never interleave into an inconsistent state — including
  `AddEdge` racing `DeleteNode` on the same node (the edge is either created
  before the node is gone and then cascaded, or rejected with `ErrInvalidEdge`).
  A completed `DeleteNode` never leaves a dangling edge, and the reopened state
  always matches the live state (WAL order equals apply order).
- **A *sequence* of calls is not a transaction.** There is no multi-operation
  rollback or snapshot isolation: if you need a higher-level invariant to hold
  across several calls (e.g. read-decide-write), coordinate that in your own
  code. Individual calls are always safe and consistent.
- Reads may return pointers into internal state for speed — treat results as
  read-only and mutate exclusively through the API.
- Ordering: type-lookup and property-lookup results are not sorted unless you use
  the typed `Query*` APIs, which apply deterministic ordering + pagination.
- Durability boundary (disk): a write is recoverable once its WAL append returns.
  Deleted/updated CSR space is reclaimed at the next `Compact()`.
- IDs are monotonic and never reused across the lifetime of a store, including
  across restarts and compactions.
```
