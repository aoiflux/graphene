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
6a. [Snapshots — consistent reads](#6a-snapshots--consistent-reads)
7. [Mutation](#7-mutation)  ← update / delete
8. [Type lookups](#8-type-lookups)
9. [Property index](#9-property-index)
9a. [Ordered (range) keys](#9a-ordered-range-keys)
9b. [Composite (multi-key) indexes](#9b-composite-multi-key-indexes)
9c. [Unique keys and upsert](#9c-unique-keys-and-upsert)  ← idempotent ingest
10. [Typed queries](#10-typed-queries)
11. [Degree & connectivity](#11-degree--connectivity)
11a. [Edge cardinality](#11a-edge-cardinality--at-most-one-edge-of-a-type-per-pair)  ← duplicate edges without a key
11b. [Aggregation](#11b-aggregation)
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
  `InvalidEdgeID`). The usable space is capped at **2³² per kind**: an image
  naming a higher identifier is refused at open, and because identifiers are
  never reused that cap is on the store's lifetime rather than on its size.
  `StorageStats` reports the headroom remaining.
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
func OpenReadOnly(dir string) (*Graph, error)
func OpenLive(dir string) (*Graph, error)
```

- `NewInMemory()` — volatile, thread-safe store. Best for tests, prototyping,
  and small in-process graphs.
- `Open(dir)` — durable on-disk store rooted at `dir` (created if absent). On
  restart the WAL is replayed automatically. Call `Compact()` after bulk work.
- `OpenReadOnly(dir)` — read-only, under a shared lock: any number coexist, none
  runs alongside a writer. The view is fixed at open.
- `OpenLive(dir)` — read-only, under **no lock**: runs alongside a writer and
  advances when you call `Refresh()`. See §16 for what it gives up.

For anything beyond the defaults, open through the `disk` package:
`disk.OpenWithOptions(dir, disk.Options{...})`. One option changes what a read
hands back rather than only what the store does, so it is worth knowing about
before §6's aliasing note surprises you:

| `disk.Options.ImageMode` | Image held as | A returned `Properties` slice |
|---|---|---|
| `ImageMapped` (default) | the mapped file | valid for the life of the handle, not past `Close()` |
| `ImageHeap` | a heap copy | ordinary heap; outlives the handle |
| `ImageMappedUnlocked` | the mapped file, with no process lock | as `ImageMapped`, but a live reader's `Refresh` retires old mappings — see §6 |

`StorageStats.ImageMode` reports which one is actually in force, because mapping
falls back to a heap copy wherever it is unavailable rather than refusing the
open.

`disk.Options.IndexMode` is the same shape of choice for the property index.

| | index at open | next compaction writes |
|---|---|---|
| `IndexMapped` (default) | read in place out of the image | v9, index as GPIX and GPIR |
| `IndexResident` | rebuilt in the heap, one entry at a time | v8, index as GIDX |

The default reads it in place, which is about a hundred and twenty bytes per
indexed entry of anonymous memory that is never allocated — on the shape this
engine is sized for, the difference between a store costing 3.5× its own file in
resident memory and one costing 1.1×. What it costs is a warm point lookup of
roughly 190 ns instead of roughly 100: a binary search through file-backed pages
rather than a map probe. A *cold* one costs a handful of random reads, so a pass
that does many of them wants `NodesByPropertyBatch`.

It does not change what a read hands back: property values are copied out of the
index at every public boundary either way.

Two things to know before taking the default. The image it writes is **v9, which
no earlier build opens** — `IndexResident` plus one `Compact()` writes v8 again
and is the supported way back. And reading in place needs a mapped image: under
`ImageHeap` a base pointing into the heap buffer would pin the whole file to save a
fraction of it, so the store rebuilds the index instead. It still writes the format
it was asked for, because an image is portable and its digest is an identity for its
contents — the encoding follows the option, not the machine.
`StorageStats.IndexMode` reports what is in force and `store.MetricIndexFallback`
names any request that could not be honoured.

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

#### Naming them

Left unnamed, every custom type renders as `Custom(7)` — so an application with a
dozen of them carries its own number-to-name table and translates at every
boundary. That table is a second copy of the numbering, and the moment it drifts
from the one the data was written with, every log line, error and export silently
misreads the database.

```go
func (g *Graph) DeclareTypeNames(nodes map[store.NodeType]string, edges map[store.EdgeType]string) error
func (g *Graph) TypeNames() (nodes map[store.NodeType]string, edges map[store.EdgeType]string)

// The registry underneath, for a caller with no Graph in scope.
func store.RegisterNodeTypeName(t store.NodeType, name string) error
func store.RegisterEdgeTypeName(t store.EdgeType, name string) error
func store.NodeTypeNames() map[store.NodeType]string
func store.EdgeTypeNames() map[store.EdgeType]string
```

```go
g.DeclareTypeNames(
    map[store.NodeType]string{nodeApp: "App", nodeVersion: "AppVersion"},
    map[store.EdgeType]string{edgeOwns: "Owns"},
)
nodeApp.String()                  // "App"
store.ParseNodeType("App")        // nodeApp
store.ParseNodeType("custom:0")   // nodeApp, still
```

The name is what `String()` renders and what the parsers accept, **in addition to
— never instead of** — the `Custom(7)`, `custom:7` and bare-numeric forms.
Nothing that parsed before stops parsing.

**Only the custom range can be named.** The built-ins mean what the engine says
they mean, and renaming one would change what `ParseNodeType("case")` resolves
to. Names that could not round-trip are refused for the same reason: a built-in
name, a bare numeric, anything shaped like a custom-offset selector, anything
that normalises to nothing.

**On a disk store the table is persisted** to `graphene.labels` beside the image,
which is the half worth more: the graph stays interpretable without the program
that wrote it. It is a sidecar, not part of the image format, so an older engine
ignores it and a store carrying one opens unchanged. Backup includes it.

`Open` registers what the table says. **A disagreement is an error**
(`store.ErrTypeNameConflict`), not a resolution — the registry is process-wide,
so two stores in one process that disagree about what 32 768 means cannot both be
rendered correctly, and picking one silently is the confident wrong answer this
exists to prevent. Declaring the same names again is a no-op, so this belongs
beside the other declarations at every `Open`.

---

## 4. Errors

Two shapes carry structure, and are matched with `errors.As`:

```go
type ErrNotFound struct { Kind string; ID uint64 }   // "node" or "edge"
type ErrInvalidEdge struct { MissingID NodeID }      // AddEdge with missing endpoint
```

```go
var nf *store.ErrNotFound
if _, err := g.GetNode(id); errors.As(err, &nf) {
    // id does not exist
}
```

The rest are sentinels, matched with `errors.Is`:

| Sentinel | Returned by | Meaning |
|---|---|---|
| `store.ErrNoLabels` | `AddNode`, `AddEdge`, their batch and `Tx` forms, `UpdateNode`, `UpdateEdge` | the label set is empty |
| `store.ErrIndexedPropertiesRequired` | `UpdateNode`, `UpdateEdge` under `ReindexReject` | the entity carries index entries the update would invalidate (§17) |
| `store.ErrKeyNotUnique` | `UpsertNode`, `UpsertEdge`, `NodeByProperty`, `EdgeByProperty` | the key was never declared unique (§9c) |
| `store.ErrUniqueViolation` | any write registering a value another live entity holds, or a second edge of a declared type between one pair | the constraint would be broken |
| `store.ErrWriteConflict` | `Tx.Commit` | an upsert's key moved between buffering and commit; retry |
| `store.ErrBudgetExceeded` | any `*Ctx` traversal or aggregate | the walk hit `MaxNodes`, `MaxEdges` or `MaxTime` (§12) |
| `store.ErrTypeNameConflict` | `DeclareTypeNames`, `Open` on a store whose label table disagrees | two namings of one type value (§3) |

A uniqueness *declaration* that fails reports all of it at once:

```go
type UniqueConflict struct { Value []byte; IDs []uint64 }

type UniqueViolationsError struct {
    Kind      string            // "node" or "edge"
    Key       string
    Conflicts []UniqueConflict  // every offending value, ascending
}
```

It unwraps to `ErrUniqueViolation`, so one condition covers a violation whichever
stage it surfaced at — but `Conflicts` is the field a repair reads.

The edge-cardinality constraint (§11a) has the same pair of shapes, reporting
node pairs where the property constraint reports values:

```go
type EdgePair struct { Src, Dst NodeID }

type EdgeCardinalityConflict struct { Pair EdgePair; IDs []EdgeID }

type EdgeCardinalityViolationsError struct {   // from DeclareUniqueEdge
    Type      EdgeType
    Conflicts []EdgeCardinalityConflict        // every offending pair, ascending
}

type ErrEdgeTaken struct {                     // from a refused write
    Pair  EdgePair
    Type  EdgeType
    Owner EdgeID                               // the edge that already joins them
}
```

Both unwrap to `ErrUniqueViolation` as well, so a caller handling "this violates
a uniqueness rule" handles all four the same way.

---

## 5. Create

```go
func (g *Graph) AddNode(n *store.Node) (store.NodeID, error)
func (g *Graph) AddEdge(e *store.Edge) (store.EdgeID, error)

func (g *Graph) AddNodes(nodes []*store.Node) ([]store.NodeID, error)
func (g *Graph) AddEdges(edges []*store.Edge) ([]store.EdgeID, error)
```

- `AddNode` — assigns and returns a fresh `NodeID`. `n.Labels` must be non-empty,
  else `store.ErrNoLabels`.
- `AddEdge` — `Src` and `Dst` must already exist (and not be deleted), else
  `*store.ErrInvalidEdge`. `e.Labels` must be non-empty, else
  `store.ErrNoLabels`. Returns a fresh `EdgeID`.
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

func (tx *Tx) UpsertNode(key string, value []byte, n *store.Node,
    props map[string][]byte) (store.NodeID, bool)
func (tx *Tx) UpsertEdge(key string, value []byte, e *store.Edge,
    props map[string][]byte) (store.EdgeID, bool)

func (tx *Tx) IndexNodeProperties(id store.NodeID, props map[string][]byte)
func (tx *Tx) IndexEdgeProperties(id store.EdgeID, props map[string][]byte)
func (tx *Tx) UpdateNodeIndexed(n *store.Node, props map[string][]byte)
func (tx *Tx) UpdateEdgeIndexed(e *store.Edge, props map[string][]byte)
func (tx *Tx) UpdateNodePartialIndex(n *store.Node, changed map[string][]byte)
func (tx *Tx) UpdateEdgePartialIndex(e *store.Edge, changed map[string][]byte)

func (tx *Tx) Commit() error
func (tx *Tx) Rollback() error
func (tx *Tx) Len() (nodes, edges int)   // creations only
func (tx *Tx) Ops() int                  // total buffered operations
func (tx *Tx) Atomic() bool
```

Reads, which see the transaction's own buffered writes:

```go
func (g *Graph) BeginTracked() *Tx      // Begin, plus read-set conflict detection

func (tx *Tx) GetNode(id store.NodeID) (*store.Node, error)
func (tx *Tx) GetEdge(id store.EdgeID) (*store.Edge, error)
func (tx *Tx) NodeExists(id store.NodeID) (bool, error)
func (tx *Tx) EdgesOf(id store.NodeID, dir store.Direction,
    edgeTypes []store.EdgeType) ([]*store.Edge, error)
func (tx *Tx) Neighbours(id store.NodeID, dir store.Direction,
    edgeTypes []store.EdgeType) ([]store.NeighbourResult, error)
func (tx *Tx) UniqueNodeOwner(key string, value []byte) (store.NodeID, bool, error)
func (tx *Tx) UniqueEdgeOwner(key string, value []byte) (store.EdgeID, bool, error)

func (tx *Tx) Tracked() bool             // true only for BeginTracked
func (tx *Tx) Reads() int                // observations in the read set
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

#### Reading inside a transaction

A transaction sees the graph as it will be once it commits — its own additions,
its own updates, its own deletions and the edges they cascade to:

```go
tx := g.Begin()
id := tx.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
n, _ := tx.GetNode(id)                 // found, though nothing is committed yet
```

This works on every backend and changes nothing about how a transaction can
fail.

#### `BeginTracked` — read-modify-write that cannot lose an update

A plain read inside a transaction is unprotected: it happens while you are still
buffering, and what you decided from can move before `Commit` takes the lock.
`BeginTracked` records every read that reached the store and re-checks it under
that lock:

```go
for {
    tx := g.BeginTracked()
    n, err := tx.GetNode(id)
    if err != nil {
        return err
    }
    tx.UpdateNode(advance(n))

    err = tx.Commit()
    if errors.Is(err, store.ErrWriteConflict) {
        continue                       // someone won the race; re-read and retry
    }
    return err
}
```

A record that changed, disappeared or appeared, an incident-edge set that moved,
or a unique key that changed hands refuses the whole transaction and writes
nothing. The error is a `*store.ReadConflictError` naming what moved, wrapping
`store.ErrWriteConflict`.

It is opt-in because it can only add a failure — `Begin` keeps behaving exactly
as it did. And it is **not** serialisability: a decision made from the *absence*
of anything matching a predicate is not protected, which is why there is no
tracked `QueryNodeIDs`. See TECHNICAL_DETAILS §6.7a.

**The retry loop terminates.** A tracked read is taken from the same view its
validation will use — the newest *applied* state, not the newest durable one —
so a retry sees the write that refused the previous attempt. The one consequence
worth knowing is that a tracked read can therefore return state that is
committed but not yet on the platter. It cannot leave you with a durable result
that rests on one: this transaction's own commit is ordered after that write in
the same log, so a crash losing the write loses the commit too. Reads outside a
tracked transaction are unaffected and see only what is durable.

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

`tx.IndexNodeProperties` / `tx.IndexEdgeProperties` buffer property-index
entries, so **topology and index become durable together or not at all**:

```go
tx := g.Begin()
id := tx.AddNode(&store.Node{Labels: ...})
tx.IndexNodeProperties(id, map[string][]byte{"k": []byte("ver:9f3a")})
_ = tx.Commit()   // the node and its key land in one batch, or neither does
```

> **This changed in v0.5.0.** Indexing used to be a separate call that a
> transaction did not buffer, and the guidance here was to index *after* the
> commit. The window between the two is narrow and its consequence is not: a
> crash inside it leaves nodes whose records are correct and whose keys resolve
> to nothing, which cannot be found again by the key they were written under —
> so the next ingest of the same source writes a second one. The duplicate, not
> the crash, is what is left behind.

Index *cleanup* was already included, and still is: deleting a node inside a
transaction removes its property-index entries, and updating one honours
`SetReindexPolicy(ReindexPurge)` exactly as `UpdateNode` does.

Entries are applied in sorted key order rather than map order, so two identical
transactions frame identical bytes.

#### Upsert inside a transaction

`tx.UpsertNode` resolves a declared-unique key to an existing node or reserves a
new ID, and hands it back immediately so it can be used as an edge endpoint:

```go
_ = g.DeclareUniqueProperty("k")

tx := g.Begin()
ver, created := tx.UpsertNode("k", []byte("ver:9f3a"),
    &store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: blob},
    map[string][]byte{"state": []byte("extracted")})
perm, _ := tx.UpsertNode("k", []byte("perm:INTERNET"), permNode, nil)
tx.AddEdge(&store.Edge{Src: ver, Dst: perm, Labels: ...})
err := tx.Commit()   // *store.ErrWriteConflict if another writer took a key
```

That early resolution is re-checked under the write lock at commit. If another
writer took the key in between, the whole transaction is refused with
`store.ErrWriteConflict` rather than quietly creating a second entity; retrying
resolves to whatever the winner wrote. A single-writer caller never sees it.
`created` is therefore trustworthy exactly when `Commit` succeeds.

Upserting one key twice in a transaction returns the same ID both times, and the
later call wins.

#### Which should I use?

| Situation | Use | Why |
|---|---|---|
| Nodes **and** their edges together | **`Begin`** | The only shape that commits both atomically |
| Re-ingesting a source that may already be in the graph | **`Begin`** + `tx.UpsertNode` | Resolves each natural key to what is already there (§9c) |
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
func (g *Graph) StorageStats() (store.StorageStats, bool) // operational figures; false if unsupported
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
> `p := append([]byte(nil), n.Properties...)`, or `store.CloneNode(n)` /
> `store.CloneEdge(e)` for the whole record including its `Labels`. This is the
> only case where a copy is your responsibility — the reverse direction is
> handled for you: the store always copies what you pass to
> `AddNode`/`AddEdge`/`UpdateNode`, so you may reuse your own buffers freely
> after a write returns.

> **How long a returned blob stays valid, on the disk backend.** By default the
> compacted image is memory-mapped (`disk.Options.ImageMode` = `ImageMapped`), so
> a `Properties` slice from a record in the image addresses the file rather than
> the heap. That is valid **for the life of the handle and not past `Close()`** —
> after `Close` the process no longer has the memory, and reading it is a fault
> rather than stale data. Compaction does not shorten that window: a compaction
> writes a new file and leaves the mapping it was reading in place, so a slice
> taken before one is still valid after it.
>
> Two cases need more care.
>
> `graphene.OpenLive` with `disk.ImageMappedUnlocked`: a `Refresh()` that crosses
> a compaction maps the new image and eventually releases the old one, so a slice
> from image N is valid until the second such reload after it. Clone anything you
> keep across a `Refresh`.
>
> And a caller that wants blobs independent of any file can ask for the previous
> behaviour with `disk.ImageMode` = `ImageHeap`, which copies the whole blob half
> of the image into the heap at open. The cost of that is the reason it is not the
> default: see [benchmarks.md](benchmarks.md) and TECHNICAL_DETAILS §14.18.

Removing the read-side copy made disk reads **flat in property-blob size** rather
than proportional to it — a 512-byte-blob point lookup went from 151 ns to 45 ns,
and a 10 000-node bulk read from 2.05 ms to 0.48 ms. See
[benchmarks.md](benchmarks.md).

---

## 6a. Snapshots — consistent reads

```go
func (g *Graph) Snapshot() (store.Snapshot, error)
```

A plain read is point-in-time: it answers from whatever the store holds at the
instant it runs, and two reads in a row can disagree if a writer runs between
them (§16). A snapshot fixes the answer.

```go
snap, err := g.Snapshot()
if err != nil {
    return err
}
defer snap.Close()

n, _ := snap.GetNode(id)            // the same node, every time
count, _ := snap.NodeCount()        // the same count, every time
```

**Close it.** On the disk backend a snapshot pins the image it was taken against
and every record version written since; an abandoned one holds memory the next
compaction would otherwise release. `StorageStats().OpenSnapshots` is where a
leak shows up, and `disk.Options.MaxSnapshotAge` is the guard against one.

### Using a snapshot where a graph goes

`store.Snapshot` is the read half of `store.GraphStore` — the same methods, the
same semantics, minus everything that writes — so it satisfies
`store.GraphReader` and every traversal takes one directly:

```go
snap, _ := g.Snapshot()
defer snap.Close()

res, err := traversal.BFS(snap, origin, 3, store.DirectionBoth, nil)
path, err := traversal.ShortestPath(snap, src, dst, nil)
ids, err := snap.QueryNodeIDs(store.NodeQuery{Types: []store.NodeType{store.NodeTypeCase}})
```

This is what a multi-step read needs: a traversal is many reads, and on a live
store a concurrent delete can leave it holding an edge to a node that no longer
exists. Over a snapshot it cannot.

### Iterating a snapshot instead of materialising it

Every read above answers with a slice, which is the right shape for a lookup and
the wrong one for a whole graph: `QueryNodeIDs(store.NodeQuery{})` builds one ID
per record before you see the first. A snapshot also implements
`store.Scanner`, which yields them one at a time:

```go
type Scanner interface {
    ScanNodes() iter.Seq2[store.NodeID, error]
    ScanEdges() iter.Seq2[store.EdgeID, error]
    ScanNodesByType(t store.NodeType) iter.Seq2[store.NodeID, error]
}
```

```go
snap, _ := g.Snapshot()
defer snap.Close()

sc, ok := snap.(store.Scanner)
if !ok {
    return fmt.Errorf("this store does not stream")   // both bundled ones do
}
for id, err := range sc.ScanNodes() {
    if err != nil {
        return err          // the snapshot was closed or expired mid-scan
    }
    if done(id) {
        break               // stopping is break; nothing has to be closed
    }
}
```

**Check the error.** The sequences are `Seq2` rather than `Seq` because a scan
can fail partway — the snapshot behind it can be closed or can expire while you
are still pulling — and a non-nil error is the last thing the sequence yields,
with the ID beside it not a result. Ignoring it makes a scan that stopped early
look exactly like one that finished.

**IDs arrive ascending**, the same order the equivalent query returns, on both
backends. So a scan substitutes for a query without changing what the consumer
writes — `bulk` streams a source that implements `Scanner` and enumerates one
that does not, and the two produce byte-identical dumps.

**Iteration is offered over a snapshot and not over a live store.** That is the
answer to what a scan should do when a writer changes the graph beneath it: a
snapshot has already fixed it. It is also what makes the scan cheap — peak
memory is one batch plus the uncompacted delta, rather than the graph.

**A snapshot reports declarations but cannot make one.** It satisfies
`store.OrderedIndexReporter` and `store.CompositeIndexReporter` — the read half
of the declarer interfaces — so a consumer that only wants to know which keys
are indexed can ask the view rather than having to hold the store as well. That
is what lets a snapshot back a complete `bulk` export: the declarations reach
the dump header, and the import rebuilds the same indexes. It is deliberately
not an `OrderedIndexDeclarer`; a declaration belongs to the store, and a view
does not get to change it.

### What is fixed and what is not

**Fixed:** nodes, edges, adjacency, labels, and everything derived from them —
including `NodeCount`, `EdgeCount`, `NodesByType` and typed queries.

**Not fixed:** the property index, which is a live structure shared with the
store. `NodesByProperty` and `EdgesByProperty` resolve the *current* postings
against the *pinned* records: a posting whose node did not exist at this epoch,
or has since been deleted, is dropped. An entry registered after the snapshot was
taken, for a node that already existed, is the one case this cannot exclude.

```go
func (s store.Snapshot) Epoch() uint64
```

Names the version of the graph the snapshot reads. Two snapshots with the same
epoch over the same store see the same graph. Monotonic within one open store;
meaningless across processes.

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
- `n.Labels` must be non-empty → else `store.ErrNoLabels`.
- The node's ID never changes. The new labels/properties fully replace the old.

### UpdateEdge
Replaces the **labels, weight, and properties** of the edge identified by `e.ID`.

- **Endpoints are immutable.** Any `e.Src` / `e.Dst` you set are ignored; the
  edge keeps its original endpoints. To reconnect an edge, `DeleteEdge` it and
  `AddEdge` a new one.
- `e.ID` must exist → else `*store.ErrNotFound`. `e.Labels` must be non-empty →
  else `store.ErrNoLabels`.

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
| Property index | Purged on delete. **Not** auto-updated on update (indexed values are caller-encoded), and under the default `ReindexReject` an update that would leave entries stale is **refused** with `store.ErrIndexedPropertiesRequired`. Use `UpdateNodeIndexed` or `UpdateNodePartialIndex`. |
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
- **Update** does not, and since v0.5.0 refuses rather than pretending
  otherwise — see §17.
- Registration is **transactional** inside a `Tx` (§5.1), so a record and the
  keys that find it become durable together.

```go
_ = g.IndexNodeProperty(artID, "sha256", []byte("deadbeef"))
hits, _ := g.NodesByProperty("sha256", []byte("deadbeef"))
```

Property keys are at most 65535 bytes, which is what a property-index record can
encode a key length into. A longer key is refused rather than truncated.

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

A declaration is recorded in `graphene.schema` beside the image as it is made
and re-applied by every `Open`, including a read-only one, so an ordered key
survives a bare reopen. It is still written into the CSR image at compaction as
well, so an engine predating the catalogue finds it there. `OrderedProperties()`
reports what is currently declared.

**Declaring a key also changes how the planner costs it.** A range or prefix on a
declared key can be sized, so it competes with the other drivers on cost; on an
undeclared key it cannot be sized *or* served, so it neither drives nor sorts
ahead of anything. That is a second, independent reason to declare a key you
filter ranges on — see §18.

---

## 9b. Composite (multi-key) indexes

Declaring a set of keys builds an index over the *tuple* of their values, so a
query pinning all of them is answered by one lookup instead of by driving from
the most selective and eliminating against the rest.

```go
func (g *Graph) DeclareCompositeProperties(keys []string) error
func (g *Graph) DeclareCompositeEdgeProperties(keys []string) error
func (g *Graph) CompositeProperties() (nodeKeys, edgeKeys [][]string)
```

The win is the conjunction that is far more selective than any of its parts —
which is the common shape, not an exotic one. A case identifier and a bucket are
each worth little alone and pin a query together:

```go
g.DeclareCompositeProperties([]string{"case", "bucket"})

g.QueryNodes(store.NodeQuery{Filters: []store.PropertyFilter{
    {Key: "case", Op: store.PropertyOpEqual, Value: []byte("C-17")},
    {Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("hot")},
}})
```

Entries already registered are absorbed, so a tuple can be declared at any point.
Declaring the same tuple twice is a no-op.

**Every key must be pinned by an equality filter for the index to be used.** The
postings are keyed by the whole tuple, so a partially specified one has no entry
to look up: a declaration over three keys does nothing for a query fixing two.
`ExplainNodeQuery` will report `driver=equality` in that case, which is how to
tell. Order is part of a declaration's identity but not of its use — `(a, b)`
serves a query filtering on `b` and `a`.

**It is opt-in because it is not free.** Every registration on a member key files
the entity into the composite as well, and the composite keeps that entity's
values for each of its keys. Measured on a 100 000-node store: the conjunction
runs **~56× faster**, and registering the member keys costs **~10% more** on the
disk backend (~45% on the memory backend, where nothing else is in front of it).
Declare the tuples your queries actually use.

Returns an error for a tuple that cannot be indexed: fewer than two keys, a
repeated key, an empty key, or more than 64. A one-key composite is refused
because it is the single-key postings under another name, maintained twice to
answer one question.

Declarations are recorded in `graphene.schema` and re-applied by every `Open`,
and written into the CSR image at compaction as well, so they survive both a
compaction and a bare reopen. `CompositeProperties()` reports what is currently
declared.

---

## 9c. Unique keys and upsert

A unique key is what turns an indexed value into a *name*. Without one a natural
key resolves to a set, and "find the entity for this source, or make one" is not
expressible — every re-ingest appends to the set instead of finding what is
already there.

```go
func (g *Graph) DeclareUniqueProperty(key string) error
func (g *Graph) DeclareUniqueEdgeProperty(key string) error
func (g *Graph) UniqueProperties() (nodeKeys, edgeKeys []string)

func (g *Graph) NodeByProperty(key string, value []byte) (*store.Node, error)
func (g *Graph) EdgeByProperty(key string, value []byte) (*store.Edge, error)

func (g *Graph) UpsertNode(key string, value []byte, n *store.Node,
    props map[string][]byte) (store.NodeID, bool, error)
func (g *Graph) UpsertEdge(key string, value []byte, e *store.Edge,
    props map[string][]byte) (store.EdgeID, bool, error)
```

### Declaring

```go
if err := g.DeclareUniqueProperty("k"); err != nil {
    var v *store.UniqueViolationsError
    if errors.As(err, &v) {
        for _, c := range v.Conflicts {   // every offending value, not the first
            log.Printf("%q held by %v", c.Value, c.IDs)
        }
    }
}
```

- **Existing data is validated first**, and a graph that violates the constraint
  is reported through `*store.UniqueViolationsError` naming **every** offending
  value. A caller declaring a constraint on a graph that has been running without
  one is about to repair it, and one duplicate per pass is not a repair. Nothing
  is declared in that case.
- **Declaring a key already declared is a no-op**, so this belongs at every
  `Open`.
- **Declarations are durable.** The key is recorded in `graphene.schema` beside
  the image and re-applied by every `Open`, so a process that does not re-declare
  no longer gets a store with the constraint quietly absent. Re-declaring stays
  idempotent, so declaring at `Open` still works — it is now a habit rather than
  the contract.
- **`Open` re-validates.** Because the record outlives the process, the data can
  stop satisfying a recorded constraint — an older build could have written the
  duplicates. A writable `Open` refuses such a store, naming every violation.
  `disk.ConstraintPolicy` is the escape hatch: `ConstraintDrop` opens without the
  violated declarations and reports them through `DroppedDeclarations()`, and it
  is the default for `OpenReadOnly` and `OpenLive`, which enforce nothing anyway.
- **A backend that cannot enforce it returns an error**, unlike the ordered and
  composite declarations which return `nil`. Those are optimisations; a store
  ignoring one still answers every query correctly. This is a promise about what
  the graph can contain, and quietly not keeping it would give the caller the
  guarantee and none of the behaviour.

Registering a value a *different* live entity already holds fails with an error
satisfying `errors.Is(err, store.ErrUniqueViolation)`. Re-registering the value
an entity already holds is a no-op, which is the steady state an idempotent
re-ingest arrives at.

### Upserting

```go
id, created, err := g.UpsertNode("k", []byte("ver:9f3a"),
    &store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: blob},
    map[string][]byte{"state": []byte("extracted")})
```

- On a **miss** the entity is created; on a **hit** its labels and properties are
  replaced.
- Each key named in `props` is **replaced**, not added to, so the superseded
  value stops matching. Keys `props` does not name keep the entries they had.
- The identity entry (`key` → `value`) is registered for you.
- `key` must be declared unique, or the call returns `store.ErrKeyNotUnique`.
- The record and its entries are one transaction. For several upserts and the
  edges between them, use `Begin` and `tx.UpsertNode` (§5.1), which commits the
  lot as one unit.

`NodeByProperty` is `NodesByProperty` with the plural removed. It refuses on a
key that is not declared unique rather than returning the first of several, and
returns `*store.ErrNotFound` for a value no live node holds.

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

### Cancelling a query

```go
func (g *Graph) QueryNodeIDsCtx(ctx context.Context, q store.NodeQuery) ([]store.NodeID, error)
func (g *Graph) QueryNodesCtx(ctx context.Context, q store.NodeQuery) ([]*store.Node, error)
func (g *Graph) QueryEdgeIDsCtx(ctx context.Context, q store.EdgeQuery) ([]store.EdgeID, error)
func (g *Graph) QueryEdgesCtx(ctx context.Context, q store.EdgeQuery) ([]*store.Edge, error)
```

Each is the call above it, abandoned if `ctx` is cancelled. Passing
`context.Background()` is exactly the uncancellable call, so these are additive:
nothing that does not pass a context changes.

**Nothing partial comes back with the error.** A query stopped part way holds a
candidate set that some filters have been applied to and others have not — a
superset of the answer, shaped exactly like the answer. Returning it would be
the one result worse than none, so the error comes with a nil slice.

**What cancellation is actually for here.** The query's own time is the smaller
half. A query over a large candidate set holds the store's read lock for its
whole duration, so a caller that has stopped wanting the answer is holding up
every writer behind it; cancelling returns the lock. The first check happens
*before* the lock is taken, so a caller handing over an already-dead context
never makes a writer wait at all.

The checks inside the loops are amortised — one per 256 candidates, the same
interval `traversal`'s budget guard uses — so an uncancelled query pays a
predictable branch and nothing else. Measured interleaved against the same tree
with the checks stripped out, with `PointLookupNode_Memory` as the control: every
arm overlaps, control included.

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
func (g *Graph) EdgeIDBetween(src, dst store.NodeID, edgeTypes []store.EdgeType) (store.EdgeID, bool, error)
func (g *Graph) IsConnected(src, dst store.NodeID) (bool, error) // any-path reachability
func (g *Graph) NeighboursByNodeType(id store.NodeID, dir store.Direction, nodeType store.NodeType, edgeTypes []store.EdgeType) ([]*store.Node, error)
```

`EdgeIDBetween` returns the incumbent rather than a boolean, which is what a
caller checking for a duplicate actually wants — and what lets it use the edge
that exists instead of looking it up again. `EdgeExists` is a wrapper on it.
Which edge, when several match, is unspecified *unless* the type is declared
through `DeclareUniqueEdge` (§11a), which is the point of declaring it: the
question then has one answer.

---

## 11a. Edge cardinality — at most one edge of a type per pair

```go
func (g *Graph) DeclareUniqueEdge(t store.EdgeType) error
func (g *Graph) UniqueEdges() []store.EdgeType
```

Unique property keys (§9c) name an entity by a value. That is right for an edge
carrying identity of its own, and wrong for structure: an edge that exists only
to say "this owns that" has no value worth indexing, and giving each one a unique
key means paying an index entry per edge to restate what its endpoints already
say. A caller writing millions of them cannot afford that, and is left with a
convention — own a subtree, delete it before re-ingesting, never add an edge
twice by hand — which holds exactly as long as nobody forgets.

```go
if err := g.DeclareUniqueEdge(edgeOwns); err != nil {
    var v *store.EdgeCardinalityViolationsError
    if errors.As(err, &v) {
        // v.Conflicts names every pair joined more than once
    }
}

_, err := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{edgeOwns}})
var taken *store.ErrEdgeTaken
if errors.As(err, &taken) {
    // taken.Owner is the edge that already joins this pair
}
```

**Per label, not per label set.** Every filter in the engine treats a type list
as OR over labels, so a declaration names one type and means "at most one live
edge from src to dst carries this type". An edge labelled `{Owns, Contains}`
counts against a declaration on each independently. **Direction is part of the
pair**: src→dst and dst→src are two pairs.

Enforced on every path that can create an edge or give one a declared label —
`AddEdge`, `AddEdges`, `UpdateEdge`, and all three inside a transaction — and
identically on both backends. A batch and a transaction also check against
themselves: two duplicates arriving together are the same violation as one
arriving after the other. **Deleting an edge frees the pair within the same
transaction**, which is what makes delete-the-subtree-and-rebuild work:

```go
tx := g.Begin()
for _, id := range ownedByThisVersion {
    tx.DeleteNode(id)          // cascades to the edges
}
rebuild(tx)                    // the pairs the deletes freed are available again
tx.Commit()
```

Declaring over a graph that already violates the rule reports **every** offending
pair and declares nothing. Both errors unwrap to `store.ErrUniqueViolation`, so
one condition covers a refused write and a refused declaration.

Nothing new is stored: the answer is in the source node's outbound adjacency, and
a second structure holding it is a second thing that can disagree with the first.
The cost is a scan of that adjacency per constrained write — O(out-degree of
src), cheap for the ownership edges this exists for, and worth knowing about
before declaring a type whose sources are hubs.

Declarations are recorded in `graphene.schema` and re-applied by every `Open`,
exactly as with unique property keys — including the re-validation, and the
`disk.ConstraintPolicy` that decides what a violated one does to the open. A
backend that cannot enforce the constraint returns an error rather than accepting
the declaration. `graphene debug unique-edge -type
<t> <dir>` answers "can I?" from the shell.

---

## 11b. Aggregation

```go
func (g *Graph) NeighbourFrequency(anchors []store.NodeID, dir store.Direction, edgeTypes []store.EdgeType, nodeTypes []store.NodeType) (map[store.NodeID]uint64, error)
func (g *Graph) NeighbourFrequencyCtx(ctx context.Context, anchors []store.NodeID, dir store.Direction, edgeTypes []store.EdgeType, nodeTypes []store.NodeType, budget store.Budget) (map[store.NodeID]uint64, error)

func (g *Graph) CountNodesByType() (map[store.NodeType]uint64, error)
func (g *Graph) CountEdgesByType() (map[store.EdgeType]uint64, error)
func (g *Graph) CountNodesByProperty(key string) (map[string]uint64, error)
func (g *Graph) CountEdgesByProperty(key string) (map[string]uint64, error)
// plus a *Ctx form of each.
```

The counts a caller cannot get cheaply from outside. Pulling IDs into Go and
counting them there is fine at a thousand entities and stops being fine well
before a hundred thousand: a per-type breakdown that way is one `NodesByType`
call per type and a copy of every ID in the graph, built to be discarded.

**`NeighbourFrequency`** is the shape most cross-graph analytics turn out to be:
rank what a set of sources point at, by how many of them point at it.

```go
freq, err := g.NeighbourFrequency(versions, store.DirectionOutbound,
    []store.EdgeType{edgeEmbeds}, []store.NodeType{nodeTracker})
// freq[tracker] = how many of `versions` embed it
```

It counts **anchors, not edges**. A neighbour reached twice from one anchor — two
edges of different types, or one edge seen from both directions — is one vote,
and a repeated anchor is one anchor. Counting edges would make the answer depend
on how the graph happens to be modelled rather than on what it says. `nil`
`edgeTypes` follows every edge and `nil` `nodeTypes` counts every neighbour; both
are OR over labels. An anchor that is not live is skipped rather than reported:
the set usually came from an earlier query, and something deleted in between is a
race the caller cannot prevent.

Use `NeighbourFrequencyCtx` whenever the anchor set is data rather than something
you chose. It takes a `store.Budget` for the same reason the walks do — one hub
among the anchors is enough to make this arbitrarily large.

**`CountNodesByType` / `CountEdgesByType`** are answered from the label postings,
and also appear on `GraphStats` and in `graphene store stats`. **An entity
carrying two labels is counted under both**, so these total more than `NodeCount`
and `EdgeCount` on any graph using multi-label entities. Types with no live
entities are absent rather than zero.

**`CountNodesByProperty` / `CountEdgesByProperty`** distribute an indexed field's
values, counting only live holders. An entity with no entry under the key has no
value to distribute and is absent, as is a value held only by deleted entities.

All four are `store.Aggregator`, an optional extension both bundled backends
implement. There is no generic fallback — a `GraphStore` cannot enumerate its own
types — so a backend without it returns an error rather than an empty map that
would say the graph is empty.

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

### Weighted paths: the cheapest route, not the shortest

`ShortestPath` returns a path with the fewest edges. When the edges are not
interchangeable — a similarity score, a transfer size, a duration — fewest is
not cheapest, and `ShortestWeightedPath` is the question being asked instead.

```go
func (g *Graph) ShortestWeightedPath(src, dst store.NodeID, edgeTypes []store.EdgeType,
    cost store.EdgeCost) (*traversal.PathResult, error)
func (g *Graph) AStarPath(src, dst store.NodeID, edgeTypes []store.EdgeType,
    cost store.EdgeCost, heuristic store.NodeHeuristic) (*traversal.PathResult, error)

func (g *Graph) ShortestWeightedPathCtx(ctx context.Context, src, dst store.NodeID,
    edgeTypes []store.EdgeType, cost store.EdgeCost, budget store.Budget) (*traversal.PathResult, error)
func (g *Graph) AStarPathCtx(ctx context.Context, src, dst store.NodeID,
    edgeTypes []store.EdgeType, cost store.EdgeCost, heuristic store.NodeHeuristic,
    budget store.Budget) (*traversal.PathResult, error)
```

They return the same `*traversal.PathResult` and the same `traversal.ErrNoPath`,
and read the graph the same undirected way, so one can be swapped for the other
and the answers compared.

**The cost comes from you.** The engine has no single notion of distance to
offer: `Edge.Weight` is a similarity score for `EdgeTypeSimilarTo` and zero for
everything else, so reading it as a distance would make "more similar" mean
"further away". The selector is handed each step with the weight already in it,
so the usual readings cost nothing to express:

```go
// closer means more similar
gap := func(e store.IncidentEdge) float64 { return 1 - float64(e.Weight) }
path, err := g.ShortestWeightedPath(a, b, nil, gap)

// every hop the same — this is ShortestPath, and is why a nil cost is refused
// rather than defaulted to it
hops := func(store.IncidentEdge) float64 { return 1 }
```

A cost needing more than the weight can materialise the edge inside the
selector, but that is a store read per relaxation and will dominate the walk.

**What is refused.** `cost` must not be nil (`traversal.ErrNilCost`), and must
return a non-negative, non-NaN value (`traversal.ErrNegativeCost`). Dijkstra is
not merely inaccurate with a negative edge, it is wrong — so it is an error
rather than a silently bad path. The check covers costs the search *uses*: it
stops when the destination settles, so an edge it never examined is never
checked, and the path it returns is unaffected by one.

**Where several routes tie for cheapest**, which one comes back is unspecified —
it follows the order the backend reports incident edges in, which is not a
promise the store makes across layer states. The cost is determined by the
graph; the shape, among equals, is not.

**Cost of the walk.** A weighted search is unidirectional, because a
bidirectional one cannot stop at the first meeting once edges have costs. It is
therefore substantially more expensive than `ShortestPath` on a large graph —
roughly 15× on the benchmark fixture — since it settles every node cheaper than
the destination rather than meeting in the middle. Two things help: a
`store.Budget`, whose `MaxNodes` counts nodes settled; and `AStarPath`, if you
can estimate the remaining distance.

**A\* asks something of you in return.** `store.NodeHeuristic` must never
overestimate the remaining cost. One that does returns a dearer path and *no
error*, because catching it would mean computing the answer the estimate exists
to avoid. It should also be consistent — `h(a) <= cost(a,b) + h(b)` — since a
node is settled once and never reopened. Returning 0 everywhere is always valid
and is exactly Dijkstra, which is also what a nil heuristic means. On a grid
where Manhattan distance is admissible, the estimate is worth 2.8× in time and
38% in memory.

### Bounded and cancellable walks

```go
func (g *Graph) BFSCtx(ctx context.Context, origin store.NodeID, maxDepth int,
    dir store.Direction, edgeTypes []store.EdgeType, budget store.Budget) (*traversal.BFSResult, error)
func (g *Graph) BFSIDsCtx(ctx context.Context, origin store.NodeID, maxDepth int,
    dir store.Direction, edgeTypes []store.EdgeType, budget store.Budget) ([]store.NodeID, error)
func (g *Graph) DFSCtx(ctx context.Context, origin store.NodeID, maxDepth int,
    dir store.Direction, edgeTypes []store.EdgeType, budget store.Budget) (*traversal.BFSResult, error)
func (g *Graph) ProvenanceChainCtx(ctx context.Context, origin store.NodeID, maxDepth int,
    edgeTypes []store.EdgeType, budget store.Budget) (*traversal.DFSResult, error)
func (g *Graph) ShortestPathCtx(ctx context.Context, src, dst store.NodeID,
    edgeTypes []store.EdgeType, budget store.Budget) (*traversal.PathResult, error)
func (g *Graph) FindPatternsCtx(ctx context.Context, pattern *traversal.Pattern,
    scope []store.NodeID, maxMatches int, budget store.Budget) ([]traversal.SubgraphMatch, error)
```

Each is the method above it with a context and a budget. The unbounded forms are
unchanged and cost what they always did.

```go
type Budget struct {
    MaxNodes int           // nodes visited, including the origin
    MaxEdges int           // edges crossed
    MaxTime  time.Duration // wall clock from the start of the walk
}
var ErrBudgetExceeded = errors.New("graphene: traversal budget exceeded")
```

A zero `Budget` is unlimited, so `BFSCtx(ctx, ..., store.Budget{})` is exactly
`BFS`.

**Reach for these whenever the shape of the graph is not known in advance.**
Depth does not bound a walk that passes through a hub: one node of degree 100 000
puts 100 000 entries in the visited set at depth one, and without a budget the
only symptom is the process growing until it stops.

```go
res, err := g.BFSCtx(ctx, origin, 3, store.DirectionBoth, nil,
    store.Budget{MaxNodes: 100_000, MaxTime: 5 * time.Second})
switch {
case errors.Is(err, store.ErrBudgetExceeded):
    // too big — narrow the walk, or scope it and try again
case errors.Is(err, context.Canceled):
    // the caller gave up
}
```

Exceeding a limit is a **refusal, not a truncation**: nothing partial is returned
alongside the error, because a partial answer that looks complete is the failure
this exists to prevent.

`MaxNodes` bounds memory, `MaxEdges` bounds work on a dense graph, and `MaxTime`
is the limit of last resort. The context is checked every 256 steps, so a cancel
takes effect within a few microseconds of walking rather than instantly.

**`MaxNodes` and `MaxEdges` are exact; `MaxTime` is best-effort.** The first two
are charged per visit and stop the walk on the step that crosses them. `MaxTime`
has to read a clock, so it is checked every 16 steps when set — and it cannot see
what the platform's clock cannot resolve. On Linux that granularity is
nanoseconds. On Windows the Go monotonic clock is the system timer interrupt,
which advances every 15.6 ms by default and every ~0.5 ms while some process on
the machine has raised the timer resolution; a `MaxTime` below that does not stop
anything, and which of the two applies is a global property of the machine that
changes as unrelated programs start and stop.

Nothing preempts a step that runs long either, so a single cold page read can
outlast a `MaxTime` on its own. Size a walk with `MaxNodes` and `MaxEdges`, and
let `MaxTime` catch what they miss. A negative `MaxTime` is a deadline already
passed: the walk is refused before it visits anything.

The recursive walks — `DFSCtx`, `ProvenanceChainCtx`, `FindPatternsCtx` — also
carry a hard limit of 100 000 stack frames whatever the budget says. A goroutine
stack that runs out is a crash rather than an error, which is the one failure a
caller cannot handle.

Budgets compose with snapshots, which is the combination an analytical read over
a live store actually wants:

```go
snap, _ := g.Snapshot()
defer snap.Close()
res, err := traversal.BFSCtx(ctx, snap, origin, 3, store.DirectionBoth, nil,
    store.Budget{MaxNodes: 100_000})
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

- `ShortestPath`, `ShortestWeightedPath` and `AStarPath` return `traversal.ErrNoPath` when no path exists.
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

// store.PropertyEnumerator, so a Graph can be handed straight to the bulk package.
func (g *Graph) ForEachNodeProperty(fn func(id store.NodeID, key string, value []byte) bool)
func (g *Graph) ForEachEdgeProperty(fn func(id store.EdgeID, key string, value []byte) bool)
```

`ForEachNodeProperty` and `ForEachEdgeProperty` are written out on `Graph`
rather than inherited, and that is the point of them. `Graph` embeds
`store.GraphStore`, and an embedded interface promotes only the methods in its
own set — so without them a `Graph` would not satisfy `store.PropertyEnumerator`,
and `bulk.ExportJSONL(w, g, ...)` would compile, run, and produce a dump with
every indexed property entry silently missing.

`HasCycle` recurses once per hop, so its depth is the caller's `maxDepth` — and
a large one against a deep graph would overflow the goroutine stack, which is a
crash rather than an error, and the one failure an embedded engine has no
business handing its host. A walk deeper than `store.MaxRecursionDepth`
(100 000 frames) now returns `store.ErrBudgetExceeded` instead. The `traversal`
package's walks have been guarded this way since Phase 1; `HasCycle` is not in
that package, which is why it needed its own check and why the limit lives in
`store` beside `Budget` rather than in either.

---

## 13a. Metrics

```go
// disk.Options
Metrics store.Metrics   // nil by default

type store.Metrics interface{ Record(m store.Metric) }
type store.MetricsFunc func(store.Metric)   // adapter for a closure
```

There is no logging and no metrics anywhere in library code, which is a
deliberate position for an embedded engine: writing to a logger of its own
choosing is a decision belonging to the program embedding it. What it offers
instead is somewhere to attach one.

```go
g, _ := graphene.OpenWithOptions(dir, disk.Options{
    Metrics: store.MetricsFunc(func(m store.Metric) {
        prom.WithLabelValues(m.Kind.String()).Observe(m.Duration.Seconds())
    }),
})
```

One method taking a concrete struct, rather than a method per event or a
string-keyed counter. A method per event makes every new measurement a breaking
change to an exported interface; a string-keyed sink allocates on the hot path
and puts the meaning of every number in documentation nothing checks. Adding a
`MetricKind` is additive, and the struct is passed by value so nothing escapes.

### What is reported

| kind | Count | Examined | Bytes |
|---|---|---|---|
| `commit` | records in the batch | — | framed log bytes |
| `sync` | **commits made durable by this fsync** | — | — |
| `compaction` | records in the image | records scanned | image size on disk |
| `query` | IDs returned | candidates examined | — |
| `replay` | epochs advanced | — | log bytes read |
| `snapshot-open` | — | — | — |
| `snapshot-close` | nanoseconds held | — | — |
| `backup` | files copied | — | bytes copied |
| `refresh` | epochs advanced | — | log bytes applied |
| `id-headroom-low` | highest ID issued | the ID ceiling | — |

**id-headroom-low** is emitted by a completed compaction, at most once per kind
per process, when the identifiers still unissued fall below
`Options.IDHeadroomWarn` (default 0.10, negative disables). An audit entry of
the same name is written beside it, naming the kind and the figures, because
the question an operator asks later is when this first became true.

`Count` on a **sync** is the number group commit exists to move: one fsync per
commit means it is not working. `Examined` against `Count` on a **query** is the
planner's selectivity. **replay** is emitted once per open and is the best
single indicator of how overdue a compaction is.

`Examined` on a **compaction** is every record the merge considered: the live
records of the image being replaced, plus every entry in the delta — a tombstone
is an entry, and dropping the record it names is work. `Examined − Count` is
therefore the part of that work which produced nothing, and it is the number
that says whether compacting was worth the write. A delete contributes two to
it — the image record discarded and the tombstone that discarded it — and an
update one, because of the two records the merge read for it, the replacement is
in the image it wrote. It is never below `Count`, and equal to it only for a
compaction that dropped nothing at all: no deletes, no updates, a delta of
records the image had never held.

`Err` is non-nil when the operation failed, **and a failed operation is still
recorded** — an error rate is a metric, and a sink that only ever hears about
successes cannot compute one. A refusal is not a failure: a compaction that
returns `ErrCompactionInProgress` never ran and is not reported, or a background
compactor's error rate would be made entirely of the trigger working. Likewise a
query refused before it takes the lock, which is not a query that failed.

### What it costs, and where it is not measured

Nothing when unset. Every call site is inside a nil check and the clock reads
happen inside that branch, so a store with no sink runs the code it ran before
this existed.

`GetNode` and the other point reads are deliberately **not** measured. That path
is ~6 ns and lock-free; two clock reads would be an order of magnitude more than
the work, and a measurement that dominates the thing it measures is not an
observation.

`Record` is called from whichever goroutine did the work, several at once. It
must be cheap, must not block, and **must never call back into the store** —
doing so from inside a commit deadlocks the store against itself. Every emission
but one happens with no store lock held, because a slow sink would otherwise
stall the writers it is measuring. The exception is `sync`, emitted inside the
log's write lock: the duration of an fsync is only knowable where the fsync
happens.

---

## 13b. Bulk import and export

```go
func bulk.ExportJSONL(w io.Writer, src bulk.Source, opts bulk.Options) (bulk.Summary, error)
func bulk.ExportDump(w io.Writer, src bulk.Source, opts bulk.Options) (bulk.Summary, error)
func bulk.ExportCSV(dir string, src bulk.Source, opts bulk.Options) (bulk.Summary, error)

func bulk.ImportJSONL(r io.Reader, dst bulk.Dest, opts bulk.Options) (bulk.Summary, error)
func bulk.ImportDump(r io.Reader, dst bulk.Dest, opts bulk.Options) (bulk.Summary, error)
func bulk.ImportCSV(dir string, dst bulk.Dest, opts bulk.Options) (bulk.Summary, error)
```

A `*graphene.Graph` satisfies both `Source` and `Dest`.

| format | shape | for |
|---|---|---|
| **JSONL** | one stream, one JSON object per line | interchange; greppable, splittable, editable by hand |
| **graphene_dump** | one binary stream, WAL-style framing with CRCs | this engine reading its own data, exactly and quickly |
| **CSV** | a directory of tables plus `manifest.csv` | spreadsheets, notebooks, ETL — anything that is not this engine |

### What travels, and what does not

Nodes, edges, **and the indexed property entries** — the third is the one that
is easy to omit and impossible to recover. A record's `Properties` blob is
opaque to the engine, and the values in the property index were handed to
`IndexNodeProperty` separately by a caller who knew how to derive them. A dump
carrying only the records restores a graph that looks complete and answers
nothing; `RebuildIndexes` cannot fix it, because it repairs structure and not
content. A source that cannot enumerate them is **refused** with
`bulk.ErrNoPropertyEntries` rather than exported without them.

Index declarations travel too — ordered keys and composite tuples. They change
plans, not answers, so leaving them out would make an imported store quietly
slower than the one it came from.

**IDs do not travel.** A store assigns them and nothing can ask it for a
particular one, so an import allocates fresh IDs and rewrites every reference —
edge endpoints and property entries alike — through a map built as the nodes
arrive. The result is isomorphic to the original, not identical to it. That is
why records must arrive **nodes, then edges, then properties**; a stream in any
other order is refused with `bulk.ErrOutOfOrder` rather than buffered.

**History does not travel.** No WAL, no audit log, no redaction ledger, no
custody chain, no signatures. Anything needing those needs `Backup`, which
copies the store rather than describing it.

### Truncation, and the trailer

Every format ends with a trailer carrying the counts (CSV puts them in
`manifest.csv`, written last). A dump cut *mid-record* is caught by the decoder;
one cut *between* records is not, because every record that arrived is well
formed — only the trailer's absence says the rest is missing, and only its counts
catch a data file that lost rows from the middle. Same argument as
`graphene.backup.json` being written last.

**An import is not a transaction**, and cannot be: there is no API that could
make one span a million records, and buffering to pretend otherwise would move
the failure from "half imported" to "out of memory". Import into an empty store
and discard the directory if it fails.

---

## 14. Persistence lifecycle

```go
func (g *Graph) Compact() error                          // disk only; no-op in memory
func (g *Graph) CompactCtx(ctx context.Context) error    // the same, cancellable
func (g *Graph) ShouldCompact(p store.CompactionPolicy) (bool, string)
func (g *Graph) Backup(dst string) (disk.BackupInfo, error)          // disk only
func (g *Graph) BackupCtx(ctx context.Context, dst string) (disk.BackupInfo, error)
func (g *Graph) Close() error

func graphene.Restore(src, dst string, opts disk.RestoreOptions) (disk.RestoreInfo, error)
func graphene.VerifyBackup(dir string) (disk.BackupInfo, error)
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

### What a compaction blocks, and for how long

`Compact` pins its records under the store lock, builds and fsyncs the new image
with the lock **released**, and retakes it to install the result. Writers wait
for the pin and the commit, not for the build — ~17–20 ms on a 100 000-record
store rather than the whole rebuild. Readers are never blocked beyond the instant
the new view is published, and a `Snapshot()` opened before a compaction goes on
reading the graph it was opened against.

Commits that land while the image is building are not lost: they stay in the
delta and in the log, which is rebuilt around them rather than emptied.

`CompactCtx` cancels the build. Once the commit begins the compaction finishes
whatever the context says, because those steps are the ordering that makes a
crash recoverable. A cancelled compaction leaves the store exactly as it found
it. Calling `Compact` while one is already running returns
`disk.ErrCompactionInProgress` rather than queueing.

### Compacting automatically

Nothing bounds delta growth on its own: everything written since the last
compaction stays in memory and is replayed at every open, degrading memory, open
time and read speed at once with no error and no warning.

The documented shape is your own loop:

```go
if due, why := g.ShouldCompact(store.DefaultCompactionPolicy()); due {
    log.Printf("compacting: %s", why)
    _ = g.Compact()
}
```

Or hand it to the engine, which is off by default:

```go
policy := store.DefaultCompactionPolicy()
s, _ := disk.OpenWithOptions(dir, disk.Options{
    AutoCompact:         &policy,
    AutoCompactInterval: time.Minute, // zero means 30s
    AutoCompactObserver: store.CompactionObserverFunc(func(reason string, err error) {
        log.Printf("background compaction (%s): %v", reason, err)
    }),
})
```

This starts a background goroutine, which `Close` cancels and waits for. Set the
observer: a background compaction has no caller to return an error to, so without
one a failing compaction fails silently and repeats.

`store.DefaultCompactionPolicy()` is a starting point, not a tuned setting.

`Close()` flushes and releases the backend. Always defer it.

### Backing up a store that is still running

```go
func (g *Graph) Backup(dst string) (disk.BackupInfo, error)                      // disk only
func (g *Graph) BackupCtx(ctx context.Context, dst string) (disk.BackupInfo, error)
func graphene.Restore(src, dst string, opts disk.RestoreOptions) (disk.RestoreInfo, error)
func graphene.VerifyBackup(dir string) (disk.BackupInfo, error)
```

`Backup` writes a consistent copy of the store into `dst`, which must not already
hold a store or a backup. The graph stays open and writable throughout — only
compaction is refused for the duration, and a background compactor reports
`disk.ErrBackupInProgress` and retries on its next tick. Writers are not blocked
beyond what a commit already costs itself; the copy costs a few percent of write
throughput.

```go
info, err := g.Backup("/archive/case-1183/2026-08-28")
// info.CommitSeq      — the newest commit the copy holds
// info.ImageCommitSeq — the earliest point it can be rewound to
// info.Files          — every file, with its length and SHA-256
```

The copy is hashed as it is written and the manifest — `graphene.backup.json` —
is written **last**, so a directory without one is not a backup however complete
it looks. A cancelled `BackupCtx` therefore cannot be mistaken for a finished one.

`VerifyBackup` re-checks an archived backup against its manifest without
restoring it. Run it on a schedule: the alternative is finding out during a
recovery.

```go
out, err := graphene.Restore(backupDir, newStoreDir, disk.RestoreOptions{})
g2, err := graphene.Open(out.Dir)
```

`dst` must not exist or must be empty — a restore never merges into a directory
that already holds a store. The backup is verified before a byte is copied.

### Recovering to a point in time

```go
out, err := graphene.Restore(backupDir, newStoreDir, disk.RestoreOptions{
    AtCommitSeq: 41_207,        // or AtTime: someInstant, or both
    ActorID:     operatorID,    // recorded in the restored audit log
})
// out.CommitSeq      — where it actually landed
// out.DroppedRecords — records the cut discarded
```

The restored directory *is* the store as it stood at that point; the log is
truncated at the end of the last qualifying commit. Two limits, both reported
rather than silent:

- **Only commits are boundaries.** `AddNode`, `AddEdge`, `DeleteNode` and the
  other single-record calls append a record with no sequence number and no
  timestamp — they have never been transactions. They are restored with the
  commit that follows them and dropped with it otherwise, so
  `out.DroppedRecords` being large on a small rewind means the store was written
  through those calls rather than through `Begin()`. Use transactions for ingest
  you may want to rewind.
- **A point inside the image cannot be reached.** Compaction is not reversible,
  so a backup can only be rewound as far as the log it carries.
  `disk.ErrRestorePointTooEarly` names the earliest reachable commit. Going
  further back needs an older backup — which is the reason to keep more than one.

The in-memory backend has no directory to copy: `Backup` returns an error rather
than succeeding silently.

### Finding out what an open will cost, before paying it

```go
est, err := disk.PreflightOpen(dir)
// est.ImageBytes          — the compacted image on disk
// est.ImageHeapBytes      — modelled heap for the image alone (see below)
// est.WALReplayBytes      — the log's records region: what a replay reads
// est.WALRecords          — what a replay would apply
// est.WALRecordsBuffered  — what it would read, hold, and discard
```

`Open` is the one operation whose cost nobody can see in advance. An image is a
file whose size is on disk; a log that was never compacted is a whole write
history that is replayed into memory on every open, and until now the only way to
find out how much was to try. `PreflightOpen` takes no lock, opens no store, and
runs **in memory bounded by a constant** — which is the point, because a question
about a store too large to open is exactly the question worth asking.
`InspectCSR` and `InspectWAL` cannot answer it: both spend memory proportional to
the store to find out.

It costs one addressed read of the image header plus **one sequential pass over
the log**. Bounded memory, not bounded I/O — on a never-compacted store, a
preflight followed by an open reads the log twice.

Two budgets turn the answer into a refusal:

```go
s, err := disk.OpenWithOptions(dir, disk.Options{
    MaxReplayBytes:   512 << 20,   // compared against est.WALReplayBytes
    MaxReplayRecords: 5_000_000,   // zero, and any negative, is unlimited
})
if errors.Is(err, disk.ErrReplayBudget) { /* names both figures */ }
```

Four things to know before setting either.

- **They bound the replay and nothing else.** The image is loaded by the same
  `Open` and is not covered; on a compacted store it is the larger half. Gate on
  `est.ImageBytes` yourself.
- **`MaxReplayBytes` is compared against `WALReplayBytes`, not `WALBytes`** — the
  records region, not the file. The two differ by the log's 50-byte container
  header, which is small enough to go unnoticed and large enough to refuse a
  store that has not changed.
- **`MaxReplayRecords` costs a counting pass**, because how many records a log
  holds is not a property of its size. The pass is skipped when the log is too
  small to hold the budget's worth of records — every compacted store — and stops
  as soon as the count passes the budget. `MaxReplayBytes` costs nothing and
  bounds records implicitly, since no record is under nine bytes; prefer it.
- **`ImageHeapBytes` models the image only, as retained heap.** WAL replay is not
  in it, so it is *not* a whole-store figure, and on a never-compacted store it is
  zero while the true cost is at its highest. Resident memory ran 1.19–1.54× live
  heap on the one fixture measured, so read it as a floor.
  It is also an upper bound in the other direction: records are stored in pages
  of 4096 identifiers and the header cannot say how many pages an image touches,
  so the model charges one page per record until the identifier space runs out
  of pages to charge. A dense image costs far less than the figure; a maximally
  sparse one costs exactly it.

A refused `Open` is not a no-op on disk: the directory is created if missing, a
stranded rebuilt log is adopted, and an empty log is given its container header.
Nothing is replayed and no ledger is opened. `PreflightOpen` is the only
genuinely non-mutating way to ask.

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

Both backends are safe for concurrent use **by goroutines within one process**;
every method takes the locks it needs internally. Across processes, the disk
backend enforces one writer and many readers — see [Across
processes](#across-processes) below, which is a different set of rules and worth
reading before you run two programs against one directory. What follows first is
what in-process safety does and does not buy you.

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
rollback and no general snapshot isolation. If an invariant has to hold across
several calls — read-decide-write being the usual one — enforce it in your own
code.

The one read-decide-write that the engine does protect is an upsert's key
(§9c): it is resolved when the operation is buffered and re-checked under the
write lock at commit, so two writers racing to create the same entity do not both
succeed. The loser gets `store.ErrWriteConflict`. Nothing else a transaction read
is tracked.

### Reads

A read returns data that was correct at some instant during the call:

> Every ID a lookup or query returns named an entity that was live at the moment
> it was checked, and every record it returns is internally coherent — an edge is
> incident to the node it was requested for, and a neighbour is that edge's far
> endpoint.

**The instant is inside the call, not after it.** By the time you act on a result
the entity may already be gone, and `GetNode` on an ID you were just handed can
legitimately fail. That is not a bug in the store.

To close it, read through a snapshot (§6a). `g.Snapshot()` fixes the graph for as
long as it is held, so every read through it agrees with every other — which is
what a traversal, a report or an export needs, and what a single lookup does
not.

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

### Across processes

Everything above is about goroutines. Across processes the disk backend enforces
**one writer, many readers**, with an OS-level lock on the store directory
(`flock` on Linux/macOS/BSD, `LockFileEx` on Windows). This is not advice you
have to follow — a conflicting open is refused.

| Call | Lock | Coexists with | View |
|---|---|---|---|
| `graphene.Open` / `disk.Open` | exclusive | nothing | current, including its own writes |
| `graphene.OpenReadOnly` / `disk.OpenReadOnly` | shared | other readers only | fixed at open |
| `graphene.OpenLive` / `disk.OpenLive` | **none** | anything, writers included | fixed until `Refresh()` |

```go
g, err := graphene.OpenReadOnly(dir)
if errors.Is(err, disk.ErrStoreLocked) {
    // a writer has it. The error names the holder's PID.
}
```

Acquisition **never blocks and never retries**. Whether to wait for a busy store
depends on what you are — a CLI that should print and exit, an ingest worker that
should back off, a service that should fail its health check — so the engine
refuses immediately and leaves the policy to you.

The lock is held by the OS, so a process that crashes releases it. There is no
stale lock to clear and no recovery step.

#### An `OpenReadOnly` store is a snapshot, and this is the part to understand

**Its view is fixed at `Open` and never advances.** The engine materialises a
store into memory once — the delta layer and property index from a WAL replay,
the CSR from a single read — and nothing re-reads afterwards. Reopen to see later
writes.

That is also why such a reader is *refused* alongside a writer rather than
admitted. Admitting it would produce a permanently stale view with nothing to
signal it had gone stale, which is a worse failure than being told no.

What `ReadOnly` additionally guarantees is that nothing under `dir` is modified.
That took work: `OpenWAL` creates the log if it is missing and writes a container
header into an empty one, and the audit, redaction and grant ledgers all open for
append. A read-only store opens none of them, and every mutating method returns
`disk.ErrReadOnly` — including `Compact`.

#### `OpenLive` — a reader that follows a writer

```go
func OpenLive(dir string) (*Graph, error)
func (g *Graph) Refresh() (store.RefreshInfo, error)
func (g *Graph) IsLive() bool
```

`OpenLive` takes **no process lock at all**, so it neither excludes a writer nor
is excluded by one. That is the trade, and it is worth naming before you choose
it: what you give up is `OpenReadOnly`'s "no writer is running" guarantee, which
is the whole reason that mode may fix its view at open. Nothing on the *writing*
side changes — a writer still takes an exclusive lock, so two writers remain
impossible, and a store with live readers attached is written exactly as before.

```go
g, err := graphene.OpenLive(dir)
if err != nil { /* ... */ }
defer g.Close()

for range time.Tick(time.Second) {
    info, err := g.Refresh()
    if err != nil { /* ... */ }
    if info.Advanced() {
        // the graph moved; re-run whatever depends on it
    }
}
```

`Refresh` reports what it did:

| Field | Meaning |
|---|---|
| `Epoch` | the newest commit now visible |
| `LogGeneration` | which log file the reader is reading; a change means the writer compacted |
| `Reloaded` | true when the reader rebuilt from the image rather than advancing over appended bytes — because the log was replaced (the writer compacted) or because it is shorter than the reader had already read, which is what a restored directory put in place under a follower looks like |
| `Bytes` | how many bytes of log this call applied |
| `Advanced()` | `Reloaded || Bytes > 0` |

**Four things to hold on to.**

- **It advances only when you call `Refresh`.** There is no polling goroutine,
  because how often to look depends on what you are — the same reason a busy
  store is refused immediately rather than waited on. Between calls a live reader
  is exactly as fixed as `OpenReadOnly`, which is what makes its answers stable
  while you use them.
- **It trails the writer by at most one fsync.** A commit still inside the
  writer's group-commit gate is not in the log yet, so it is not visible. That is
  the durability boundary doing its job: a live reader never shows you a write
  that a crash would take back.
- **A reload is not free.** When the writer compacts, the log is replaced and the
  reader rebuilds its whole in-memory state from the image and the new log —
  roughly the cost of a reopen. Appends between compactions are cheap; the
  compaction is not.
- **Still read-only, and that includes the lock file.** Every mutating method
  returns `disk.ErrReadOnly`, and nothing under `dir` is created or modified —
  not by `Open`, not by `Refresh`. `OpenReadOnly` will create an empty
  `graphene.lock` if the directory has none, because it needs a file to take a
  shared lock on; `OpenLive` takes no lock, so it does not open that file either.

`Refresh` is safe to call while other goroutines read: the image, the delta and
the property index are published as one value, so a query sees all of the new
state or all of the old. A failed refresh leaves the reader serving exactly what
it was serving before.

Backends that cannot follow a writer — the memory store, and any disk store not
opened with `OpenLive` — return `disk.ErrNotLiveReader` from `Refresh` rather
than reporting a refresh they did not perform. Check with `IsLive()`.

#### Unclean shutdown

The lock file records the last exclusive holder's PID and whether it closed
cleanly. A store whose previous holder died reports
`(*disk.Store).RecoveredFromUncleanShutdown()`, and records an
`AuditUncleanRestart` entry when `Options.Audit` is on.

**It still opens.** WAL replay is crash-safe by construction, and refusing would
turn every killed process into a manual intervention. What the engine cannot
decide for you is whether *this* store, holding *this* evidence, is one where an
unclean restart warrants running `VerifyIndexes`, reopening under
`Options.VerifyOnOpen`, or escalating — so it reports the fact and leaves the
judgement where it belongs.

#### Platforms

Enforced on Windows, Linux, macOS and the BSDs. On platforms whose standard
library has no locking primitive — solaris, aix, plan9, js/wasm — the store still
opens and `(*disk.Store).LockEnforced()` returns `false`. Check it if you deploy
there; nothing excludes a second process.

#### Inspecting a busy store

`graphene store info`, `store csr`, `wal show`, `wal segments`, `wal verify`,
`anchor list`, `assertion list`, `redaction list` and `grant list` parse the
files directly and never open the store, so they work against a directory
another process is writing. That is what they are for: the moment you most want
to look at a store is the moment something is wrong with it and a live process
is still attached.

Everything else in the tool — the `node`, `edge`, `traverse` and `query` groups,
`store stats`, `store health`, `debug integrity` — needs the WAL replayed over
the image and so opens the store under the shared lock. Each says so on stderr
before it does, and `graphene help <group> <verb>` states which lock it takes.

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
func (g *Graph) UpdateNodePartialIndex(n *store.Node, changed map[string][]byte) error
func (g *Graph) UpdateEdgePartialIndex(e *store.Edge, changed map[string][]byte) error
func (g *Graph) SetReindexPolicy(p store.ReindexPolicy)
func (g *Graph) ReindexPolicy() store.ReindexPolicy
```

**Plain `UpdateNode` on an entity that carries index entries is refused** under
the default policy:

```go
err := g.UpdateNode(n)
// *store.ErrIndexedPropertiesRequired, if n carries indexed properties
```

Use whichever of the two knows what the update did:

```go
// The whole desired index state. Keys not mentioned are DROPPED.
_ = g.UpdateNodeIndexed(
    &store.Node{ID: artID, Labels: []store.NodeType{store.NodeTypeTag}},
    map[string][]byte{"sha256": newHash},
)

// Only the keys that moved. Everything else survives untouched — this is the
// shape a state transition has.
_ = g.UpdateNodePartialIndex(n, map[string][]byte{"state": []byte("analyzed")})
```

Both are one transaction, so the record and the entries cannot land separately.

| Policy | Behaviour | Failure mode |
|---|---|---|
| `store.ReindexReject` **(default from v0.5.0)** | an update to an entity carrying entries is refused | none — it produces no answer rather than a wrong one |
| `store.ReindexKeep` (the default before v0.5.0) | entries are left alone | entries go **stale** — the old value still matches |
| `store.ReindexPurge` | the entity's entries are dropped | entries are **lost**, including ones the update did not touch |

> **This default changed in v0.5.0.** `ReindexKeep` was a silently wrong answer
> that required every caller to remember a second method name, and a caller who
> forgot got a query result the planner trusted. Note that `ReindexPurge` is not
> the safe option either — it drops the entity's *untouched* keys along with the
> stale one. Refusing is the only choice that loses nothing. Set `ReindexKeep`
> explicitly to restore the old behaviour.

An entity with no index entries updates normally under any policy, so the refusal
costs nothing on a graph it could not hurt.

On the disk backend a purge is journalled, so replay cannot resurrect superseded
values.

### Verification and repair

```go
func (g *Graph) VerifyIndexes() error
func (g *Graph) RebuildIndexes() error
func (g *Graph) VerifyIndexesCtx(ctx context.Context) error
func (g *Graph) RebuildIndexesCtx(ctx context.Context) error
```

`VerifyIndexes` cross-checks every index against the records it describes —
postings ordering and deduplication, postings ↔ reverse-map agreement in both
directions, label postings against live labels, adjacency against edge endpoints,
and that no index entry outlives its entity. It cannot check that an indexed
*value* still matches the entity's properties: those values are caller-encoded
and opaque.

Under `IndexMapped` it also checks the image's own index sections, which is the
half of the question nothing else can ask. Parsing a v9 image establishes that no
read can address memory outside GPIX or GPIR; it does not establish that the
section is *consistent*, because that is a pass over every entry and charging
every query for it is the cost the mapped index exists to avoid. So this is where
it is paid: each key's value table is checked to be tiled by its runs, each
8-byte value prefix against the value it indexes, values and postings against the
orderings the binary searches assume, the directory's counts against what the
runs hold, and every reverse entry against the forward run it names. The last two
together are exhaustive — the reverse entries inject into the forward ones and
the counts make the two sets the same size, so the two directions are shown to
describe exactly the same entries.

It is deliberately reachable: a value table whose prefix disagrees with its value
leaves the entries, the records and therefore every Merkle root untouched, so the
digest matches, `VerifyCSRRoots` passes, the store opens — and a present value is
reported absent. This is the only check that says so.

Memory stays bounded: one counter per declared key and one buffer sized by the
widest value, whatever the index holds. That is a requirement rather than an
observation, because the store most in need of the check is the one closest to
its memory ceiling.

`RebuildIndexes` recomputes everything derivable from the records — label
postings and adjacency — and drops property entries whose entity is gone. It
repairs structure, not content.

**Neither runs automatically on `Open`.** Verification is O(V+E) — roughly 200 ms
on a 100k-node store, and O(entries) again on top of that for a mapped index —
and a damaged index section is already rejected while the file is parsed, so the
scan would be a startup tax for little gain. Run them explicitly in tests, in CI,
or when recovering a suspect store. `graphene verify` is the packaged form.

#### Cancelling them, and why only one of them cancels properly

`VerifyIndexesCtx` is interruptible throughout. It is read-only, so there is no
point at which stopping leaves anything behind, and a cancelled check means only
that the question went unanswered — `errors.Is(err, context.Canceled)`
distinguishes that from an inconsistency, which matters, because a caller that
treats every non-nil error here as corruption would otherwise take a timeout as
a reason to restore from backup.

`RebuildIndexesCtx` is **not** interruptible through the part that does the
repair, and this is deliberate rather than an omission. A rebuild clears the
label postings and the adjacency and repopulates them from the records; stopping
half way leaves postings naming only some of the records that carry a label,
which is the fault that costs a query *result* rather than a query. So it
cancels before the rebuild and during the dead-entry sweep that follows it, and
nowhere in between.

What that buys is stated exactly: a cancelled rebuild leaves the store **no worse
than it found it** — the sweep it interrupted was removing entries that were
already there — but it does not leave it repaired. `VerifyIndexes` will still
report whatever the sweep did not reach. A cancelled rebuild is one to run again.

Same shape as `CompactCtx`, and for the same reason: cancellation reaches the
part that can be thrown away and stops at the part that cannot.

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
| `Driver` | `ids`, `equality`, `ordered`, `composite`, `labels`, `adjacency`, or `scan` |
| `DriverKey` | the property key, when a filter drove the query; the `+`-joined tuple for a composite |
| `DriverFilters` | a `store.FilterMask` marking the `Filters` the driver already applied, so the residual pass skips them; empty when it consumed none. A composite consumes its whole tuple, which is why this is a set rather than one index |
| `Candidates` | size of the driving set |
| `Residuals` | the remaining filters, in the order they were applied |
| `Results` | final result count |

A residual is applied one of two ways. `Probe` tests the candidates directly
through the index's reverse map, costing one lookup each. Otherwise the filter is
resolved to its own set and intersected, costing that set's size — which for a
filter no index can serve means scanning every entry under its key. `Cost` is the
planner's estimate of that set's size: exact for equality, sized from the ordered
index for a range or prefix on a **declared** key, and the key's entry count
otherwise — an upper bound rather than an estimate, because without an ordering
there is nothing cheaper than a scan that could tighten it.

**What this is for.** A query can return exactly the right answer while doing far
more work than it needed to, and the difference is invisible from the results —
a test asserting only on results cannot tell an index lookup from a full scan
that happened to agree with it. This is how that gets checked, in tests and by
hand.

`adjacency` appears only for edge queries, where an anchored query is bounded by
the incident-edge lists of its endpoints.

**Every driver that can report a size is compared, not ranked.** `ordered` and
`composite` are the two to watch, and both fail the same way — silently, by not
appearing. A range on an undeclared key has no size and no index, so it will not
drive however selective it is; a composite drives only when the query pins every
one of its keys. If a range query plans as `labels` or `scan` when you expected
`ordered`, the key is almost certainly not declared; if a conjunction plans as
`equality` when you expected `composite`, either the tuple is not declared or the
query does not cover all of it (§9b).

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

`GetNode` on the disk backend resolves through a page directory and an arena
slot — two dependent loads, no hashing — so the *lookup* is free. What it is not is allocation-free: measured at the store level
it is **~47 ns and one 64-byte allocation per call**, because the API hands back
a `*store.Node` and building that pointer is the cost.

> An earlier edition of this section quoted **~6 ns** here. That figure is the
> layout lookup — `CSRGraph.GetNode`, which returns a record by value — and not
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

**~488× apart**, because an unfiltered degree is `outOffset[s+1] - outOffset[s]`
for the node's arena slot
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
| `ReindexReject` (default) | the update is refused | none |
| `ReindexKeep` | entries untouched | **stale** — the old value still matches |
| `ReindexPurge` | entity's entries dropped | **lost**, including keys you did not touch |

```go
// The whole entry set, record and entries replaced together.
g.UpdateNodeIndexed(
    &store.Node{ID: id, Labels: labels},
    map[string][]byte{"sha256": newHash},
)

// Or only the keys that moved.
g.UpdateNodePartialIndex(n, map[string][]byte{"state": []byte("analyzed")})
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
11. Declare ordered keys for the ranges you filter on — it is what lets the
    planner both *serve* and *cost* them. They are durable: declare once, not at
    every `Open`.
12. Declare a composite for a conjunction of equality filters you run often and
    whose keys are weak individually (§9b). Check it with `ExplainNodeQuery`:
    `driver=composite` means it is being used, `driver=equality` means it is not.
    Do not declare tuples speculatively — each one costs write time on every
    registration of a member key.

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

```go
func (g *Graph) Forensics() (*disk.Store, bool)
```

is the supported way to reach it — the store itself, not a copy, and `false` on
the in-memory backend, which supports none of this. Forwarding fifty methods
through the façade would double the API surface and give every one of them
somewhere to drift.

```go
if s, ok := g.Forensics(); ok {
    proof, err := s.ProveNode(id)
}
```

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
| `Options.MaxReplayBytes` / `.MaxReplayRecords` | Refuse an `Open` whose WAL replay exceeds the budget, with `ErrReplayBudget`, rather than replaying into an OOM (§14) |

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
| `VerifyCSRDigest(path)` / `VerifyCSRRoots(path)` | Whole-image checks. The digest is hashed as the file is read, in memory bounded by one copy buffer; the roots check parses the image |
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
  The pass costs time, not memory: the roots are folded a leaf at a time by
  `merkle.RootBuilder`, which retains one hash per set bit of the leaf count
  rather than one per leaf, and the leaves are hashed as the records stream
  into the image. A compaction holds a constant for the whole of writing it,
  whatever the store's size (§14.16 of TECHNICAL_DETAILS.md).
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
   `graphene provenance verify` requires `-root` for the same reason: a proof
   checked against a root its author chose proves nothing. `node verify` and
   `edge verify` accept `-root` and, without one, report the check as a finding
   rather than as a pass — the tool will not print a verdict that reads
   stronger than what it established.

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
