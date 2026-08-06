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
5.1. [Transactions — `Begin`](#51-transactions--begin) ← recommended for ingest
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
19. [Performance guide](#19-performance-guide)
20. [Process lifecycle](#20-process-lifecycle)

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
- `AddNodes` / `AddEdges` — ordered batch insert, and **transactional**: the whole
  batch is applied or none of it is. On error nothing is created and no IDs are
  returned. On the disk backend the batch is framed with begin/commit markers and
  committed with one write plus one `fsync`, so a crash mid-batch leaves the store
  as if the call never happened.

```go
caseID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
fileID, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
eid, _   := g.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
```

### 5.1 Transactions — `Begin` *(recommended for ingest)*

```go
func (g *Graph) Begin() *Tx

func (tx *Tx) AddNode(n *store.Node) store.NodeID
func (tx *Tx) AddEdge(e *store.Edge) store.EdgeID
func (tx *Tx) UpdateNode(n *store.Node)
func (tx *Tx) UpdateEdge(e *store.Edge)
func (tx *Tx) DeleteNode(id store.NodeID)
func (tx *Tx) DeleteEdge(id store.EdgeID)

func (tx *Tx) Commit() error
func (tx *Tx) Rollback() error
func (tx *Tx) Len() (nodes, edges int)   // creations only
func (tx *Tx) Ops() int                  // total buffered operations
func (tx *Tx) Atomic() bool
```

`AddNodes` and `AddEdges` are each atomic, but they are **two** transactions. A
graph is nodes *and* the edges between them, and that pairing is exactly what the
slice APIs cannot commit together:

```go
g.AddNodes(nodes)   // commit 1
// ← a crash here leaves the nodes with none of their edges
g.AddEdges(edges)   // commit 2
```

`Begin` makes it one commit, and lets an edge reference a node created in the
same transaction:

```go
tx := g.Begin()
caseID := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
fileID := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
tx.AddEdge(&store.Edge{Src: fileID, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})

if err := tx.Commit(); err != nil {
    // nothing was written; the store is exactly as it was
}
```

**IDs are returned immediately, before commit.** That is what makes `tx.AddEdge`
above able to name `fileID`. They are *reserved*, not created — a transaction
that is rolled back or fails burns the IDs it took, and a later write gets higher
ones. This is consistent with the standing guarantee: IDs are monotonic and never
reused, and have never been promised to be contiguous.

`AddNode`/`AddEdge` on a `Tx` return IDs rather than errors, and the mutation
methods return nothing. A problem detected while buffering (a nil record, use
after finish) is latched and returned by `Commit`, so checking `Commit` is
sufficient.

#### Operations apply in order

A transaction is a **sequence**, not a set. Each operation is evaluated against
the store as modified by the ones before it:

```go
tx := g.Begin()
id := tx.AddNode(&store.Node{Labels: ...})
tx.DeleteNode(id)     // created and removed before anything is written
tx.Commit()           // net effect: nothing, but the ID is spent
```

That ordering cuts both ways — an edge onto a node the same transaction has
already deleted is rejected, and the whole transaction fails with
`*store.ErrInvalidEdge`.

#### Deleting a node cascades

`tx.DeleteNode` removes every edge incident to the node, exactly as
`g.DeleteNode` does — **including edges created earlier in the same
transaction**:

```go
tx := g.Begin()
a := tx.AddNode(&store.Node{Labels: ...})
b := tx.AddNode(&store.Node{Labels: ...})
tx.AddEdge(&store.Edge{Src: a, Dst: b, ...})
tx.DeleteNode(a)      // the edge above goes too
tx.Commit()           // leaves exactly one node: b, with no dangling adjacency
```

The cascade is computed at commit under the store lock, not when you call
`DeleteNode` — buffering it earlier would resolve against a graph that can still
change before the transaction commits.

`UpdateNode` / `UpdateEdge` replace a record wholesale, the same as their
`Graph` counterparts, and require the target to exist at commit — either in the
store or created earlier in this transaction. A missing target fails the whole
transaction with `*store.ErrNotFound`.

> **Indexing properties is not part of a transaction.** `IndexNodeProperties`
> and friends are separate calls; a `Tx` does not buffer them. Index properties
> after the transaction commits.
>
> Index *cleanup* is included, because leaving it out would diverge from the
> non-transactional path: deleting a node inside a transaction removes its
> property-index entries, and updating one honours `SetReindexPolicy(ReindexPurge)`
> exactly as `UpdateNode` does.

#### Which should I use?

| Situation | Use | Why |
|---|---|---|
| Nodes **and** their edges together | **`Begin`** | The only shape that commits both atomically |
| Loading a graph from a file / another system | **`Begin`**, chunked | Atomic per chunk, and endpoints can be wired without a second pass |
| A multi-step edit that must not half-apply | **`Begin`** | e.g. delete a node, re-attach its edges elsewhere |
| Deleting several related entities | **`Begin`** | One commit; cascades resolve against the transaction's own view |
| Only nodes, no edges | `AddNodes` | Already atomic; no reason for the extra type |
| Only edges, endpoints already exist | `AddEdges` | Same |
| One record | `AddNode` / `AddEdge` | Nothing to batch |
| One update or delete | `UpdateNode` / `DeleteNode` | Already atomic on their own |
| Records arriving one at a time, latency-sensitive | `AddNode` / `AddEdge` | A transaction only pays off once it batches |

**Performance is the same** — a `Tx` commits through the same framed single-write
path as `AddNodes`, so choosing it costs nothing. Pick it for the semantics.

> **Size a transaction deliberately.** Writes are buffered in memory until
> `Commit`, so a transaction costs memory proportional to its size — the same
> trade `AddNodes` makes with its slice. For a bulk load that does not need
> whole-file atomicity, commit in chunks of a few thousand rather than opening one
> transaction over millions of records.

A `Tx` is **not** safe for concurrent use by multiple goroutines. It is a
caller-side buffer; the store lock is taken once, at `Commit`. Separate
goroutines may each hold their own transaction.

`Atomic()` reports false only for third-party backends that do not implement
`store.Transactor`; both bundled backends return true. Such a backend still
works, committing via the batch APIs, but without cross-boundary atomicity.

---

## 6. Read

```go
func (g *Graph) GetNode(id store.NodeID) (*store.Node, error)
func (g *Graph) GetEdge(id store.EdgeID) (*store.Edge, error)
func (g *Graph) GetNodes(ids []store.NodeID) (found []*store.Node, missing []store.NodeID, err error)
func (g *Graph) GetEdges(ids []store.EdgeID) (found []*store.Edge, missing []store.EdgeID, err error)

func (g *Graph) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error)
func (g *Graph) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error)

func (g *Graph) NodeCount() (uint64, error)
func (g *Graph) EdgeCount() (uint64, error)
func (g *Graph) Stats() (*GraphStats, error) // {NodeCount, EdgeCount}
```

**A missing ID is not an error.** It is returned in `missing`; `err` is reserved
for genuine failures. Under the read model (§16) an ID can be deleted between the
call that produced it and the call that resolves it, so treating that as
exceptional forced callers back into the per-item loop these methods exist to
replace.

`found` is compacted — misses leave no `nil` holes — and preserves request order.
Each record carries its own ID, so correlating results back to requested IDs is
`node.ID`, not position.

```go
found, missing, err := g.GetNodes(ids)
if err != nil { /* a real failure */ }
if len(missing) > 0 { /* these were deleted concurrently — usually fine */ }
```

Both backends implement `store.BatchReader`, resolving the whole batch under one
lock hold. Worth **10–15% on the in-memory backend**; the disk backend already
resolved reads without a per-item lock, so it gains nothing measurable there.

- Pass `nil` `edgeTypes` to match all edge types; otherwise OR semantics.
- `Neighbours` deduplicates by neighbour node ID (one entry per neighbour).

> **Do not mutate returned structs.** For performance, reads hand back pointers
> into internal state — this now includes `Labels` and `Properties` on *every*
> read path, on both backends, whether the record is delta-resident or in the
> CSR. Treat `*store.Node` / `*store.Edge` and their slices as read-only; use
> `UpdateNode`/`UpdateEdge` to change them.
>
> If you need to keep or modify a blob, copy it:
> `p := append([]byte(nil), n.Properties...)`. This is the only case where a copy
> is your responsibility — the reverse direction is handled for you: the store
> always copies what you pass to `AddNode`/`AddEdge`/`UpdateNode`, so you may
> reuse your own buffers freely after a write returns.

Removing the read-side copy made disk reads **flat in property-blob size** rather
than proportional to it — a 512-byte-blob point lookup went from 151 ns to 45 ns,
and a 10 000-node bulk read from 2.05 ms to 0.48 ms. See
[benchmarks.md](benchmarks.md).

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

> **`DeleteNode` is unattributed and unrecoverable.** It records nothing about
> who removed the entity or why, and after the next `Compact()` there is no trace
> it ever existed — so a lawful erasure and a destruction of evidence look
> identical afterwards. If the store holds anything evidentiary, use
> `disk.Store.RedactNodeProperties` (or `RedactNode`) instead: same effect, plus
> an actor, a reason, the version hash of what went, and a tombstone that makes
> the removal provable from the image. See §21 and
> [FORENSICS.md](FORENSICS.md).

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

### Walking without building records

```go
func (g *Graph) BFSIDs(origin store.NodeID, maxDepth int, dir store.Direction,
    edgeTypes []store.EdgeType) ([]store.NodeID, error)
```

Returns the reachable node IDs within `maxDepth` and nothing else — no
`*store.Node`, no `*store.Edge`, no property blobs copied. On a 12-hop walk this
is **20 allocations against 394** for the record-returning `BFS`.

Reach for it whenever the records are not the point: reachability checks,
scoping a pattern match, or producing IDs to feed into a follow-up query.

```go
ids, _ := g.BFSIDs(artID, 3, store.DirectionBoth, nil)
scoped, _ := g.QueryNodes(store.NodeQuery{
    IDs:     ids,
    Filters: []store.PropertyFilter{{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("acquire")}},
})
```

The node *set* is identical to `BFS`'s for the same arguments; only the absence
of records differs.


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
tombstone, `0x0D` key transition, `0xFF` checkpoint.

> **`Compact()` is not only space reclamation.** It is the operation that
> computes the snapshot roots and writes the attestation over them, so it is what
> makes anything *provable*. Before the first compaction an entity is live but
> unaccounted for — which is a different thing from absent — and a redaction is
> known to the ledger but not yet recorded in the image. It also truncates the
> WAL, so without `Options.Retention` every commit's actor, timestamp and
> signature from before it are discarded. See §21.

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
- **Durability boundary (disk) — read this carefully.** It depends on whether the
  write was batched, and the difference is the whole of it.

  **A committed batch is durable when `Commit()` returns.** `SetSyncOnCommit`
  defaults to `true`, so each batch commit is fsynced before it returns. This is
  the engine's one well-defined durability boundary and the reason transactions
  are the right unit for anything you intend to rely on.

  **An individual write is not.** `AddNode`, `UpdateNode` and `DeleteNode`
  outside a transaction append without fsync, and the WAL's drain is
  opportunistic, so a returned record may still be in the process's own ring
  buffer.

  | Failure | Batched write | Individual write |
  |---|---|---|
  | Nothing crashes | yes | yes — visible to all readers immediately |
  | Process crash | **yes** | **usually** — the drain normally succeeds; a record can linger in the ring only under write contention |
  | Power loss / kernel panic | **yes** | **only if `Sync()`, `Compact()` or `Close()` has run since** |

  Measured: 200 unbatched `AddNode` calls with no `Close()` or `Compact()` left
  all 4 800 bytes already in the file. So for individual writes the practical
  exposure is **power loss, not process crash**.

  **`Sync()` is the cheap durability point:**

  ```go
  func (g *Graph) Sync() error   // returns once prior writes survive power loss
  ```

  | Situation | What makes it durable |
  |---|---|
  | Batch write (`AddNodes`/`AddEdges`) | **automatic** — fsync at commit, unless disabled |
  | Individual writes | **`Sync()`**, or `Compact()`, or `Close()` |

  Individual writes are deliberately *not* synced as they happen: an fsync per
  `AddNode` would turn a ~6 µs operation into a ~1 ms one. `Sync()` exists so a
  caller can establish a durability point without paying for a full `Compact()`.
  It is a no-op on the in-memory backend, so callers need not know which backend
  they hold.

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

---

## 19. Performance guide

How to get the most out of Graphene, on both the read and the write path, with
the specific calls to reach for.

> If the store holds anything evidentiary, read this alongside
> [§22](#22-best-practices-for-evidentiary-use). Most of the advice here is
> neutral to integrity and a few items are not — §22.2 names them and says which
> to prefer.

Figures below are from the benchmark suite on a 100 000-node / 201 000-edge
fixture. They are **illustrative ratios, not promises** — see
[benchmarks.md](benchmarks.md) for method and caveats, and note that differences
under ~25% are not resolvable in that data. The order-of-magnitude gaps are the
ones worth designing around.

> **Rows marked ✔ were re-measured** on a Ryzen 9 5980HS at `-count=3..5` after
> the integrity work landed. Unmarked rows have **not** been re-run since
> signing, attestation, tombstones and the v3 leaf encoding; treat them as
> directional. §22.2 carries the forensic costs, which were measured separately.
>
> **Every row's advice survived re-measurement; three of the magnitudes did
> not.** `BFSIDs` was documented as "20 allocations vs 394" and is nearer half:
> 97 vs 198 on a deep walk, 104 vs 237 on a wide one — still the right call, for
> a smaller reason than claimed. `EdgeExists` was ~280 ns and is ~0.5 µs. The
> point-lookup figure was wrong outright; see §19.2.
>
> That pattern is worth stating rather than quietly patching: the *guidance* here
> has held up, and the numbers attached to it had drifted by up to 4×. A reader
> choosing between two calls was being told the right thing for the wrong reason.
>
> Benchmarks live behind the `stress` build tag. `go test -bench` **without**
> `-tags stress` matches nothing and exits successfully, which is an easy way to
> conclude there are no regressions.

#### Why batching is recommended, since the old numbers were wrong

The batch row previously claimed **−53 to −64% on disk, 21–38% in memory**.
Neither figure survives contact with the suite:

- **In memory, batching and looping measure the same** — 42.7 vs 48.2 µs at
  n=100, 440 vs 445 µs at n=1000, allocations identical to within one. The
  in-memory store has no WAL and no fsync, so there is nothing for a batch to
  amortise. The honest number is ~0%.
- **On disk there is no paired benchmark at all.** `BulkWrite_AddNodeLoop` exists
  only in a `_Memory` variant, so the headline disk percentage could not have
  been reproduced from this suite by anyone who tried.

**Batch anyway, for a better reason.** An unbatched write is *not* fsynced; a
batch commit is (§16). So a loop can genuinely be faster in wall-clock while
leaving the writes exposed to power loss — the throughput comparison was
measuring the wrong thing, and the direction it implied was not even safe. A
commit is also the only unit that carries an actor and a signature (§22.4).

Restoring a real number here needs a disk `AddNodeLoop` benchmark, which does not
exist. Until it does, the guidance stands on durability and attribution, which
are measured (§22.2) rather than asserted.

### 19.1 The fast-path matrix

The single most useful table here: what you want, the call that is slow, and the
call that is not.

| If you want… | Slower | **Faster** | Difference |
|---|---|---|---|
| **Degree of a node** | `EdgesOf(...)` then `len()` | **`Degree` / `InDegree` / `OutDegree` with `nil` types** | ~18 ns vs materialising every edge ✔ |
| **Degree of one edge type** | `Degree(id, []EdgeType{t})` | **`Degree(id, nil)`, filter later if you can** | **~470×** — 18 ns vs 8.3 µs ✔ |
| **Reachable node IDs** | `BFS(...)` then read `.Nodes` | **`BFSIDs(...)`** | ~half the allocations and ~half the bytes ✔ |
| **"Are these connected?"** | `ShortestPath(...)` then check error | **`IsConnected(src, dst)`** | no path materialised |
| **"Is there an edge?"** | `EdgesOf` then scan | **`EdgeExists(src, dst, types)`** | ~0.5 µs, stops at first hit ✔ |
| **IDs for a follow-up query** | `QueryNodes(...)` | **`QueryNodeIDs(...)`** | skips building every record |
| **One property lookup** | `QueryNodeIDs` with one filter | **`NodesByProperty(key, val)`** | ~77 ns vs ~270 ns — no planner ✔ |
| **Many nodes** | `AddNode` in a loop | **`AddNodes(batch)`** | **Durability, not throughput** — see below ✔ |
| **Nodes *and* their edges** | `AddNodes` then `AddEdges` | **`Begin()` / `Commit()`** | same speed, but one commit instead of two — a crash between them cannot orphan nodes |
| **Many index entries** | `IndexNodeProperty` × N | **`IndexNodeProperties(id, map)`** | one call per entity |
| **Update an indexed entity** | `UpdateNode` then re-index | **`UpdateNodeIndexed(n, props)`** | atomic; no stale window |
| **Range / prefix query** | filter on an undeclared key | **`DeclareOrderedProperty(key)` first** | 9.4 ms → 1.2 ms wide; 8.6 ms → 17 µs narrow |
| **Subgraph matching** | `FindPatterns` with `nil` scope | **scope it** — BFS/query IDs first | scope size drives cost directly; the matcher itself is 4.75 ms on a 2 000-node scope |
| **Concurrent reads** | in-memory backend | **disk backend** | 12.3 ns vs 48.0 ns at 16 cores — see §19.5 |
| **Space after bulk deletes** | leave it | **`Compact()`** | 4.5× less memory per live node |

### 19.2 Reads

#### Point lookups are already optimal — don't build around them

`GetNode` on the disk backend resolves through a direct array offset, so the
*lookup* is free. What it is not is allocation-free: measured at the store level
it is **~47 ns and one 64-byte allocation per call**, because the API hands back
a `*store.Node` and building that pointer is the cost.

> An earlier edition of this section quoted **~6 ns** here. That figure is the
> array offset — `CSRGraph.GetNode`, which returns a record by value — and not
> what `Store.GetNode` costs. It was attributed to the wrong call, and the number
> a caller actually gets is the one above. Re-measured on a Ryzen 9 5980HS;
> ratios, not promises.

There is still nothing to *tune* in the lookup itself. If a profile says point
lookups are your cost, you are making too many of them, and the allocation is
what you are paying for — batch with `GetNodes`, or avoid materialising records
at all (below).

#### Ask for IDs, not records

Every API that returns records has an ID-returning sibling. Records copy property
blobs; IDs do not.

```go
// Materialises every node and its property blob.
nodes, _ := g.QueryNodes(q)

// Returns only IDs — resolve just the ones you actually need.
ids, _ := g.QueryNodeIDs(q)
```

The same applies to traversal. `BFSIDs` walks the graph without constructing a
single `*store.Node` or `*store.Edge`:

```go
ids, _ := g.BFSIDs(origin, 3, store.DirectionBoth, nil)   // 20 allocations
walk, _ := g.BFS(origin, 3, store.DirectionBoth, nil)     // 394 allocations
```

The node *set* is identical. Use `BFS` only when you need the edges or the
records themselves.

#### Degree: the biggest single trap in the API

```go
g.Degree(id, nil)                             // ~15 ns — reads CSR offsets
g.Degree(id, []store.EdgeType{someType})      // ~7.4 µs — walks every incident edge
```

**~488× apart**, because an unfiltered degree is `outOffset[n+1] - outOffset[n]`
— one subtraction, no records — while a type filter has to inspect each incident
edge's labels. Both are far better than they were, but the gap is structural and
will not close: filtering requires looking.

If you can tolerate the unfiltered count, take it. If you genuinely need a typed
degree on a hot path, consider modelling that edge type as a separate anchored
query you can cache.

#### Let one filter drive, and make it selective

Only **one** filter drives a query; the rest are applied to the candidates it
produces. So the win comes from one *selective* indexed key, not from indexing
everything:

```go
// Good: sha256 is unique, so the driver produces one candidate.
g.QueryNodeIDs(store.NodeQuery{Filters: []store.PropertyFilter{
    {Key: "sha256", Op: store.PropertyOpEqual, Value: hash},   // ← drives
    {Key: "tool",   Op: store.PropertyOpContains, Value: []byte("acq")}, // ← residual, probed
}})
```

Verify with `ExplainNodeQuery` (§18) rather than assuming — §19.7.

#### Page with `Limit`, and prefer ascending order

`Offset`/`Limit` are applied after ordering. An ascending query over an already
ascending candidate set skips sorting entirely; a descending one costs a linear
reverse. Neither is expensive, but `QueryOrderAsc` is the cheaper default.

#### Scope pattern matching aggressively

`FindPatterns` is the most expensive path in the engine — roughly 200 allocations
per scoped node. Always pass a `scope`, ideally from `BFSIDs`:

```go
scope, _ := g.BFSIDs(origin, 2, store.DirectionBoth, nil)
matches, _ := g.FindPatterns(pattern, scope, 10)   // maxMatches caps the work
```

### 19.3 Writes

#### Batch, and understand what batching buys

```go
g.AddNodes(nodes)   // one lock hold, one pass
g.AddEdges(edges)
```

**On disk this is now a real bulk path**, not just a lock-amortising wrapper. The
whole batch is framed into one buffer and committed with a single write:

| Disk batch | per-record | batched | |
|---:|---:|---:|---|
| n=1 000 | 5.778 ms | 2.694 ms | **−53%** |
| n=10 000 | 53.57 ms | 19.42 ms | **−64%** |

That is ~1.94 µs/node against ~5.36 µs — and the batched path is *also* durable,
where the old one was not. Batching alone is worth ~66%; the `fsync` at commit
gives back a few points of it.

On the in-memory backend batching is worth **21–38%** (266 vs 365 ns/node at
n=100), since there is no WAL to amortise — only the lock.

**Durability:** a batch commit `fsync`s by default. Disable with
`SetSyncOnCommit(false)` on the disk store only if you sync explicitly via
`Compact()` or `Close()` and can afford to lose everything since.

**Batching is also atomic.** On error nothing is applied and no IDs are returned
— on disk the commit marker never reaches the file, so replay discards the
partial bytes. What batching cannot do is span the node/edge boundary: `AddNodes`
then `AddEdges` is two commits. For nodes and their edges together, use
[`Begin`](#51-transactions--begin), which costs the same and commits once.

#### Know what a disk write actually costs

| Operation | Memory | Disk |
|---|---:|---:|
| `AddNode` | ~700 ns | **~6.2 µs** |

The ~9× gap is the WAL: a durable write must reach the log before the call
returns. That is the price of crash safety, and it is not tunable — but it does
mean **ingest is write-bound on disk, not CPU-bound**, so optimising your own
property encoding will not help much.

#### Register properties per entity, not per key

```go
// One call per key — N lock acquisitions across shards.
g.IndexNodeProperty(id, "sha256", hash)
g.IndexNodeProperty(id, "bucket", bucket)

// One call — better.
g.IndexNodeProperties(id, map[string][]byte{"sha256": hash, "bucket": bucket})
```

Registration costs ~900 ns per entry and maintains sorted postings plus a reverse
map. That reverse map is why deletes are cheap (§19.6), so the cost is bought,
not wasted.

#### Use `UpdateNodeIndexed` — the plain update has a failure mode either way

The engine cannot re-derive index entries: your values are caller-encoded and the
property blob is opaque. So a plain `UpdateNode` must either leave entries stale
or drop them:

| Policy | Behaviour | Failure mode |
|---|---|---|
| `ReindexKeep` (default) | entries untouched | **stale** — the old value still matches |
| `ReindexPurge` | entity's entries dropped | **lost**, including keys you did not touch |

```go
// Avoids both: record and entries replaced together.
g.UpdateNodeIndexed(
    &store.Node{ID: id, Labels: labels},
    map[string][]byte{"sha256": newHash},
)
```

#### Deleting is cheap, but scales with label size

`DeleteNode` on a populated index costs ~2 µs, because the reverse map makes
removal proportional to the entity's *own* entries rather than to the index.

Label postings are the part that scales: removing a node from a 50 000-member
label costs ~4 µs against ~1 µs for a 10 000-member one. If you have a label
attached to most of the graph, deletes on it will be the slow part.

#### Compact after bulk mutation, not during

```go
g, _ := graphene.Open(dir)
// ... bulk ingest / deletion ...
g.Compact()
```

`Compact()` reclaims space from deleted and superseded records — **4.5× memory
per live node** on a half-deleted store — and empties the WAL, which is what
keeps the next `Open` fast. It costs ~64 ms on a 50 000-node store, so it is a
periodic operation, not a per-write one.

Compaction takes an exclusive lock. Do it between phases of work, not alongside
them.

### 19.4 Choosing indexes

Indexing is opt-in, which means the decision is yours and it is possible to get
it wrong in both directions.

> **The one rule:** index a key when it removes an O(N) scan from a path you have
> measured, and when you can afford the write cost on every mutation touching it.

#### What each operator costs

| Operator | Key indexed | Key declared ordered |
|---|---|---|
| `Equal` | **O(1)** postings lookup | same |
| `Prefix` | scan of that key's entries | **O(log n + k)** |
| `GreaterThan` / `LessThan` / `Between` (+ `…OrEqual`) | scan of that key's entries | **O(log n + k)** |
| `Contains` | scan of that key's entries | **scan** — no ordering can bound a substring |

"Scan of that key's entries" visits only what is registered under that key, not
the whole graph — but it is still linear in that, which is why a selective driver
plus a `Contains` residual probes candidates instead (§18).

#### Index a key when

- You filter on it with `Equal` and it is selective. A `sha256` unique per node is
  ideal: one postings entry, so the query is effectively a hash lookup.
- You filter on it in most queries, even at moderate selectivity — 1 000 distinct
  values across 100 000 nodes still cuts candidates 100× before residual work.
- It is the key you would *drive* from. Indexing a key you always pair with a
  better one buys less than it looks.

#### Do not index a key when

- You only ever use `Contains` on it. No index helps; you pay write and memory
  cost for nothing.
- It is low-cardinality *and* unselective — a boolean-ish key matching half the
  graph. The postings list is enormous and the planner will rightly ignore it.
- You never filter on it. **Storing a value in the property blob does not require
  indexing it** — the blob is opaque and costs nothing extra.

#### Declare a key ordered when

You run range, `Between`, or prefix queries on it *and* the values are encoded so
byte order matches your intent (`index/encoding`).

**Do not declare** a key you only use with `Equal` (already O(1)), only with
`Contains` (unservable), or whose values are numeric-looking strings you have not
encoded — declaring switches that key to byte-wise comparison, so `"9"` sorts
after `"10"` and **your results change**. That is a semantics change, not a
performance one (§9a).

Cost: ~10.5 B per node for a key with 1 000 distinct values, scaling with
*distinct values* rather than entries.

### 19.5 Concurrency

#### Concurrent reads are faster on the disk backend

Counter-intuitive, and worth designing around:

| At 16 cores | Memory | Disk |
|---|---:|---:|
| Point lookup | 48.0 ns | **12.3 ns** |

The disk backend has a lock-free fast path over the immutable CSR, so reads scale
~4.8×. The memory backend holds one `RWMutex`, and `RLock` on a very short
critical section is an atomic increment on a shared cache line — it scales
*negatively*. **`NewInMemory()` is the reference implementation and a good
choice for tests and small graphs; it is not the fast choice under read
concurrency.**

#### Spread property writes across keys

| Concurrent registration | Time |
|---|---:|
| 16 goroutines, **distinct** keys | **427 ns** |
| 16 goroutines, **same** key | 923 ns |

The property index is sharded 16 ways by key hash, so unrelated keys never
contend. Traffic concentrated on a single key contends on that one shard and
sharding cannot help it — ~2.2× apart.

#### Writes serialise; do not expect them to scale

Every write takes the store's exclusive lock, and on disk the WAL is a single
append point. `Parallel_AddNode` is flat across cores by design. Parallelise your
*preparation* — encoding, hashing, deriving property values — and keep the actual
`Add*` calls on one goroutine or accept that they will queue.

### 19.6 Memory

| Configuration | Bytes per node |
|---|---:|
| Topology only (memory) | ~446 B |
| + property index, 3 keys | ~745 B |
| Topology only (disk) | ~298 B |
| + property index, 3 keys | ~597 B |
| Half-deleted, uncompacted | **~715 B per live node** |
| Same, after `Compact()` | **~158 B** |

Two things follow. **Indexing is where memory goes** — roughly 93–163 B per
indexed entry depending on cardinality, so a key indexed on every node is a real
cost. And **compaction is the single biggest memory lever** on a store that
deletes.

If a workload is memory-bound rather than query-bound, indexing fewer keys is a
legitimate answer.

### 19.7 Diagnosing

Do not guess. `ExplainNodeQuery` reports what the planner actually did:

```go
plan, _ := g.ExplainNodeQuery(q)
fmt.Println(plan)
// driver=equality(sha256) candidates=1 residual=tool:probe~100000 results=1
```

| What you see | What it means | What to do |
|---|---|---|
| `driver=scan` | nothing bounded the query | index a key you filter on |
| `driver=labels`, huge `candidates` | the label is unselective | add a selective property filter |
| `residual=k:set~<big>`, big ≫ candidates | that filter built a large set | usually a range on an undeclared key — declare it |
| `candidates` ≫ `results` | the driver is weak | a different key would drive better |
| `candidates` ≈ `results` | the driver is doing its job | look elsewhere — record materialisation, or call volume |

For write-path or memory problems, `VerifyIndexes()` will not help — it checks
structure, not cost. Measure with the benchmark suite instead.

### 19.8 Checklist

**Reads**

1. Return IDs, not records, unless you need the records — `QueryNodeIDs`,
   `BFSIDs`.
2. `Degree(id, nil)` where you can; the typed form is ~488× dearer.
3. One selective indexed key to drive each query; verify with `ExplainNodeQuery`.
4. `DeclareOrderedProperty` for any key you range over, with `index/encoding`
   values.
5. `IsConnected` / `EdgeExists` for questions that are not "give me the path".
6. Scope `FindPatterns` with `BFSIDs`.

**Writes**

7. `AddNodes` / `AddEdges` over loops; `IndexNodeProperties` over per-key calls.
7a. `Begin()` when a write creates nodes *and* the edges between them — same
    cost, one commit instead of two. Chunk it; a transaction buffers in memory.
8. `UpdateNodeIndexed` rather than update-then-reindex.
9. Spread concurrent property writes across keys; expect no scaling on `Add*`.
10. `Compact()` between phases of bulk work — never per write.
11. Re-declare ordered keys after reopening; declarations are runtime state.

**Both**

12. Measure before indexing. The planner is exact about equality cardinality;
    intuition about selectivity usually is not.
13. Property blobs are free; indexes are not. Store everything, index what you
    query.

---

## 20. Process lifecycle

```go
func (g *Graph) HandleSignals(signals ...os.Signal) func()
```

Installs a handler that closes the graph cleanly when the process receives one of
`signals`, and returns a function that uninstalls it. The defaults are
platform-specific: `os.Interrupt` and `SIGTERM` everywhere, plus `SIGQUIT` on
Unix.

```go
g, _ := graphene.Open("./case-01")
defer g.Close()
stop := g.HandleSignals()
defer stop()
```

This matters more than it looks on the disk backend, though **not** because an
unclean exit loses committed data outright. The precise guarantee is in §16: a
single `AddNode`/`AddEdge` reaches the OS but is not `fsync`ed, so it survives a
process kill and is exposed to power loss until a `Sync()`, batch commit,
`Compact()` or `Close()`. Batch and transaction commits `fsync` by default.

What an abrupt kill does cost is consolidation: the WAL is left unconsolidated,
so the next `Open` replays every record written since the last `Compact()`.
Replay is roughly 30× cheaper than it was — a 10 000-node uncompacted log now
opens in ~43 ms rather than ~1.3 s — but it is still proportional to the log,
where a compacted reopen is proportional to nothing you wrote recently.

Closing cleanly is what keeps restart cost proportional to recent work rather
than to the whole log.

The returned uninstall function exists so tests and embedded uses can avoid
leaving a global signal handler behind.

---

## 21. Forensic integrity (`disk` package)

Every entry below is **opt-in** and lives on `disk.Store`, not `graphene.Graph`.
This section is a lookup table; [FORENSICS.md](FORENSICS.md) is the working
guide and [SECURITY.md](../SECURITY.md) is the authority on what each mechanism
proves and what it does not.

### Configuration

| Symbol | Purpose |
| --- | --- |
| `StrictOptions(signer, verifier, actorID) Options` | The cautious posture: commits signed and required to be signed, image verified on open, audit log on |
| `Options.Signer` / `.Verifier` | Sign each batch commit; check signatures during replay |
| `Options.RequireSignedCommits` | Reject a log containing an unsigned committed batch — closes the downgrade |
| `Options.VerifyOnOpen` | Check digest, roots and attestation before loading the image |
| `Options.Audit` | Hash-chained record of operator actions in `graphene.audit` |
| `Options.Retention` (`RetentionPolicy`) | Which retired WAL segments survive compaction |
| `Options.Redaction` / `.RedactionPolicy` | Enable the redaction ledger; bound a single cascade |

### Snapshot roots, attestations, proofs

| Symbol | Purpose |
| --- | --- |
| `(*Store).SnapshotRoots() (SnapshotRoots, error)` | The Merkle identity of the compacted image. **Retain `.Snapshot` externally** |
| `(*Store).SnapshotAttestation() (Attestation, error)` | The signed assertion over that image |
| `VerifyAttestation(v, a) error` | Check an attestation with only a public key |
| `(*Store).AttestNode(id) (NodeAttestation, error)` | A transferable claim about one entity |
| `VerifyNodeAttestation(v, na) error` | Check one |
| `(*Store).ProveNode(id) (NodeInclusionProof, error)` | Inclusion proof for an entity |
| `VerifyNodeInclusion(root, p) error` | Package-level; needs no store |
| `VerifyCSRDigest(path)` / `VerifyCSRRoots(path)` | Whole-image checks |
| `VerifyChain(earlier, later) error` | That one snapshot follows another |

### Exporting a proof

| Symbol | Purpose |
| --- | --- |
| `(*Store).ExportNodeProof(id) ([]byte, error)` | Inclusion proof as bytes |
| `(*Store).ExportRedactionProof(id) ([]byte, error)` | Node removal |
| `(*Store).ExportEdgeRedactionProof(id) ([]byte, error)` | Edge removal |
| `(*Store).ExportPropertyRedactionProof(id) ([]byte, error)` | Content-free property removal |
| `UnmarshalProof(data) (ExportedProof, error)` | Decode; says nothing about truth |
| `VerifyExportedProof(root, e) error` | **Needs no store.** The root is an argument, never read from the proof |
| `MarshalProof(e) ([]byte, error)` | Re-encode |
| `ErrProofMalformed` | Unreadable, as distinct from readable and false |

### Redaction

| Symbol | Purpose |
| --- | --- |
| `(*Store).RedactionImpactFor(id) (RedactionImpact, error)` | What a removal would take, before it takes it |
| `(*Store).RedactNode(id, req)` | The entity and every incident edge |
| `(*Store).RedactNodeProperties(id, req)` | The property blob; entity, labels and edges stay |
| `(*Store).RedactEdge(id, req)` | One relationship, both endpoints kept |
| `(*Store).RedactEdgeProperties(id, req)` | An edge's properties; the relationship stays |
| `(*Store).Redactions() ([]RedactionRecord, error)` | The ledger, oldest first |
| `ReadRedactions(dir)` | Same, without opening the store |
| `VerifyRedactionChain(records, verifier) error` | Hash chain and signatures |
| `(*Store).ProveRedaction(id)` / `.ProveEdgeRedaction(id)` | A removal is recorded under the snapshot root |
| `(*Store).ProvePropertyRedaction(id)` | Properties went and nothing else changed |
| `VerifyRedactionInclusion` / `VerifyPropertyRedaction` | Package-level verifiers |
| `(*Store).Tombstones() []Tombstone` | What the image records as removed |
| `ErrRedactionUnexplained` / `ErrCascadeTooLarge` / `ErrNoTombstone` | Refusals |

`RedactionRequest{ActorID, RoleID, Reason}` — **`Reason` is required.** An
unexplained redaction is indistinguishable from evidence destruction.

### Chain of custody

| Symbol | Purpose |
| --- | --- |
| `(*Store).CustodyFor(id, verifier)` | Walks every history. **Never `Complete()`** — it reports that all checks are self-referential |
| `(*Store).CustodyForAnchored(id, verifier, root)` | Against a root you retained |
| `(*Store).CustodyForAnchor(id, verifier, anchor)` | Against an anchor — strongest |
| `CustodyReport.Complete()` / `.Broken()` / `.Summary()` / `.Gaps` | Gaps, not a verdict |

### Anchoring

| Symbol | Purpose |
| --- | --- |
| `Anchor` (interface) | `Publish(digest)` and `Records()`. **The engine ships no transport** |
| `(*Store).Checkpoint() (Checkpoint, error)` | Capture every history's head without publishing |
| `(*Store).PublishCheckpoint(a)` | Capture, publish, record locally |
| `(*Store).VerifyAgainstAnchor(a) (AnchorAudit, error)` | Bidirectional: local vs published, both ways |
| `(*Store).CheckpointHistory()` / `ReadCheckpoints(dir)` | The local chain |
| `VerifyCheckpointChain(chain) error` | Local only — weak by design |
| `NewInsecureLocalAnchor(path, storeDir)` | **Not an anchor.** For tests and demos; refuses a path inside the store |

### Roles and capabilities

**Attribution, not enforcement — nothing in the engine consults any of this.**
See [SECURITY.md §7](../SECURITY.md).

| Symbol | Purpose |
| --- | --- |
| `Options.Roles` | Enable the grant ledger |
| `(*Store).GrantRole(subject, roleID, caps, req)` | Record capabilities given |
| `(*Store).RevokeRole(subject, roleID, caps, req)` | Record capabilities withdrawn |
| `(*Store).Capabilities(actor) (Capability, error)` | Derived from the ledger, and from nothing else |
| `(*Store).CheckCapability(actor, want) error` | **Advisory.** Call it at your own boundary |
| `(*Store).Grants()` / `ReadGrants(dir)` | The ledger, oldest first |
| `CapabilitiesFrom(records)` | Same derivation, for a third party holding only the ledger |
| `VerifyGrantChain(records, verifier) error` | Hash chain and signatures |
| `CapRead` … `CapGrant` | The capability bitmap |
| `ErrGrantUnexplained` / `ErrNoGrantLedger` / `ErrNotPermitted` | Refusals |

`GrantRequest{GrantedBy, Reason}` — **`Reason` is required**, for the same reason
a redaction's is.

### Audit log and key rotation

| Symbol | Purpose |
| --- | --- |
| `(*Store).AuditEntries()` / `ReadAuditLog(dir)` | Operator actions, oldest first |
| `(*Store).RecordAudit(kind, actorID, detail)` | Your own events. Kinds below `AuditCustom` are the engine's |
| `VerifyAuditChain(entries) error` | Distinguishes an edited entry from an excised one |
| `(*Store).RotateKey(...)` / `.KeyTimeline()` | Signed by the outgoing key |
| `ListSegments(dir)` / `VerifySegmentChain(segs)` | Retired WAL segments |

---

## 22. Best practices for evidentiary use

§19 optimises for speed. This section optimises for a store whose contents may
have to be **defended** — in an audit, a disclosure exercise, or a courtroom.
The two mostly agree; where they do not, this section says which to prefer and
why, because a fast answer nobody can stand behind is not cheaper.

Read [SECURITY.md](../SECURITY.md) for what the machinery proves, and
[FORENSICS.md](FORENSICS.md) for how to call it. This is the checklist.

### 22.1 The five-minute version

| Do | Instead of | Because |
|---|---|---|
| `disk.OpenWithOptions(dir, StrictOptions(...))` | `disk.Open(dir)` | Unsigned commits cannot be attributed to anyone, ever, retrospectively |
| `Begin()` / `Commit()` | loose `AddNode` calls | A commit is the only fsync'd durability boundary — **and** the only unit that carries an actor and a signature |
| `RedactNodeProperties` | `DeleteNode` | Erasure that is indistinguishable from destruction is a liability, not a feature |
| `Compact()` on a schedule | compacting when disk pressure says so | Nothing is provable between compactions — and the roots are computed either way, so you are already paying for it |
| `Options.Retention` set | the zero value | The default discards every pre-compaction actor, timestamp and signature |
| `PublishCheckpoint` on a schedule | never | Everything since the last checkpoint is freely rewritable |
| Retain `SnapshotRoots().Snapshot` externally | trusting the store | Every internal check compares the store against itself |

### 22.2 Performance, without giving up the record

Most of §19 is neutral to integrity. Four items are not:

| §19 advice | Under evidentiary use |
|---|---|
| **Batch with `AddNodes` / `Begin()`** | **Reinforced.** Batching is faster *and* it is what attaches an actor and a signature. There is no tension here — take it |
| **`disk.Store.SetSyncOnCommit(false)`** | **Do not.** It trades the one durability boundary the engine defines for throughput. A commit that is not on the platter is not a commit |
| **`Compact()` for space** | **Insufficient reason to skip it.** It is also what produces roots and attestations; schedule it on time, not on disk pressure |
| **`VerifyOnOpen`** | **On**, but know the price: it re-derives every Merkle root, which measured ~5× on a 10 k-node open — not the "one hash of the image" it sounds like. Where opening is rare relative to querying, still the right trade |

#### What it actually costs

Measured by `BenchmarkForensic_*`, on a Ryzen 9 5980HS. Ratios, not promises —
re-run them on your hardware. Where a figure is missing it is because the
timings overlapped and the honest answer is *unresolved*, not zero.

| Operation | Cost of turning the machinery on |
|---|---|
| **Batch commit, signing** | **+8 allocations and ~0.5 KB per commit, flat.** Independent of batch size — a signature is per commit, not per record. Timing is fsync-dominated and unresolvable |
| **Compaction, attestation** | **+6 allocations, ~0.8 KB.** One Ed25519 signature per compaction. Timing unresolvable at 1 k and 10 k nodes |
| **Compaction, audit + retention** | **~+15–16 ms, flat.** This is the real cost, and it is durable writes — a segment rotation and an fsynced audit append — not hashing |
| **`VerifyOnOpen`** | **~5× on a 10 k-node store** (3.6 ms → 20 ms), +10 MB and +90 k allocations |
| **Building an inclusion proof** | ~8 ms on 10 k nodes — one pass over the records, recomputing every leaf |
| **Verifying one** | **~5 µs**, 29 allocations — about 1 500× cheaper than building it |

Three of these are worth reading twice:

- **Signing is not the expensive part.** It is a flat handful of allocations per
  commit. §19's advice to batch is what makes it disappear, and batching is
  advice you should be taking anyway.
- **The Merkle pass is not a strict-options cost.** `Compact()` computes snapshot
  roots unconditionally, so *every* store already pays for it, configured or not.
  What strict options add at compaction time is the audit entry and the segment
  rotation — fixed-cost durable writes.
- **Verification is asymmetric, in the direction that matters.** Producing a
  proof is expensive and is paid by whoever owns the store; checking one is
  microseconds and is paid by the recipient, who did not choose to use this
  engine.

Everything else in §19 — ID-only queries, `Degree(id, nil)`, scoped pattern
matching, declared ordered keys — is free of integrity consequences. Use it.

### 22.3 Auditability

**Turn the audit log on** (`Options.Audit`, which `StrictOptions` sets). It
records operator actions — opens, compactions, key rotations, retention
deletions, checkpoint publications, redactions — hash-chained, so removing one
entry from the middle breaks every link after it.

**Record your own events.** The engine cannot know what matters in your
workflow:

```go
s.RecordAudit(disk.AuditCustom, actorID, "exported to case file 2026-114")
```

Kinds below `AuditCustom` are refused: a caller who could write `AuditCompact`
could fabricate engine history, and the custody report compares recorded
compactions against retired segments precisely to catch a compaction that was
never recorded.

**Know what is not recorded.** Reads and queries are not, deliberately — a
synchronous append on the query path is how audit logs come to be switched off.
If your obligations cover access as well as change, record it yourself at the
boundary where you know who is asking.

**Deleting the whole log is still undetectable** without an external anchor.
Chaining narrows the attack to wholesale deletion; anchoring closes it.

### 22.4 Accountability

**Attribute every mutation.** `Begin()` on its own commits anonymously; `As`
records who is responsible:

```go
tx := g.Begin().As(store.TxContext{ActorID: 7, RoleID: 3, KeyID: 1})
tx.AddNode(n)
err := tx.Commit()
```

Without it the log records that something changed and nothing about who changed
it. Attribution cannot be retrofitted — the commit is already written.

**Set `AttestActorID`** so compactions are attributable too. A compaction is an
operator action, not a transaction, and it carries its own actor rather than
inheriting one from whichever write happened last.

**`RoleID` is recorded and never checked.** There is no role model. Recording it
makes a later access decision reconstructible from the log; it does not enforce
anything today, and presenting it as a control would be misleading.

**Rotate keys through `RotateKey`**, which signs the transition with the
*outgoing* key so the chain of authority is unbroken. Rotations live in the WAL,
so they need `Options.Retention` to survive a compaction.

**Give redactions a real reason.** It is required, it is hashed, and it is the
only thing distinguishing a lawful erasure from destroying evidence. `"cleanup"`
satisfies the API and answers nothing.

### 22.5 Reliance — what a third party can actually check

The point of the machinery is a claim that survives leaving your process. Three
things make that real:

1. **Retain roots externally, at collection time.** `SnapshotRoots().Snapshot`
   is the value to keep. Without an independently held copy, every verification
   is the store agreeing with itself.
2. **Export proofs rather than files.** `ExportNodeProof` produces a few hundred
   bytes that prove one entity was in one snapshot and disclose nothing about
   any other. Handing over the image discloses everything.
3. **Never bundle the root with the proof.** There is no API that does, and
   `graphene verify-proof` requires `-root` for the same reason: a proof checked
   against a root its author chose proves nothing.

```go
blob, _ := s.ExportNodeProof(id)                      // producer
proof, _ := disk.UnmarshalProof(blob)                 // recipient, no store
err := disk.VerifyExportedProof(retainedRoot, proof)  // root from elsewhere
```

**Check custody before you rely on a store**, not after something looks wrong:

```go
report, _ := s.CustodyForAnchor(id, ring, anchor)
report.Broken()    // a chain actively failed
report.Complete()  // every layer established and unbroken
```

It returns gaps rather than a verdict, because "not verified" tells an
investigator nothing they can act on.

### 22.6 Keeping the forensic features intact

Ways to have the machinery configured and get no benefit from it:

| Anti-pattern | What breaks |
|---|---|
| `Options.Retention` left at zero | Every pre-compaction actor, timestamp, signature and key rotation is discarded at the next `Compact()` |
| Never compacting | Nothing is provable; the ledger knows about redactions and the image does not |
| Compacting but never publishing | The unanchored window is the whole store's life |
| Anchoring to the same disk | `InsecureLocalAnchor` is **not** an anchor; anyone who can rewrite the store can rewrite it |
| `DeleteNode` on evidentiary data | Unattributed and untraceable after the next compaction |
| Redacting without compacting | `CustodyReport.RemovalProvable` stays false; a recipient given only the image cannot tell the entity from one that never existed |
| Holding the signing key in the same process forever | Everything the key can sign, an attacker in that process can sign |
| Verifying with the store's own root | Circular; catches damage, never tampering |

**A store's own report of its health is not evidence of it.** Every check the
engine runs is internal. The external parts — a retained root, a published
checkpoint, a key held elsewhere, an anchor you do not control — are the parts
that make the rest mean anything, and they are the parts the engine cannot
supply for you.

### 22.7 A worked configuration

```go
opts := disk.StrictOptions(signer, verifier, operatorActorID)
opts.Retention = disk.RetentionPolicy{MaxSegments: 50}  // keep commit history
opts.Redaction = true                                   // attributed removal
opts.RedactionPolicy = disk.RedactionPolicy{MaxCascade: 100}

s, err := disk.OpenWithOptions(dir, opts)
```

Then, on a schedule rather than on demand:

```go
s.Compact()                  // roots + attestation; makes the interval provable
s.PublishCheckpoint(anchor)  // closes the rewritable window
```

And once, at collection time, into somewhere the store cannot reach:

```go
roots, _ := s.SnapshotRoots()
recordExternally(roots.Snapshot)
```
