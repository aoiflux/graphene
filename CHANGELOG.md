# Changelog

Release notes start here. Tags v0.1 through v0.4.0 predate this file; use
`git log` for those.

## v0.6.0 — structure, aggregates, and names

Three additions, all of them things a caller was working around, and one fix to
the only traversal budget dimension that was never enforceable. Nothing here
changes how an existing on-disk graph is read or written: **there is no format
change**, and a v0.6.0 store is readable by v0.4.0 and v0.5.x, and vice versa.

No breaking API changes — every addition is a new method or a new optional
interface. The one behaviour change is deliberate and is called out in its own
section: a walk that was overrunning its `MaxTime` now stops.

### Duplicate edges can be prevented without a unique key on every edge

`UpsertEdge` resolves an edge through a declared-unique *property*, which is
right for an edge carrying identity of its own and wrong for structure. An edge
that exists only to say "this owns that" has no value worth indexing, and giving
each one a unique key means paying an index entry per edge to restate what its
endpoints already say — which a caller writing millions of them cannot afford.
The alternative was a convention: own a subtree, delete it before re-ingesting,
never add an edge twice by hand. That holds exactly as long as nobody forgets.

```go
g.DeclareUniqueEdge(edgeOwns)     // at most one Owns edge per (src, dst)
_, err := g.AddEdge(dup)          // errors.Is(err, store.ErrUniqueViolation)
```

**Per label, not per label set.** Every filter in the engine treats a type list
as OR over labels, so a declaration names one type and means "at most one live
edge from src to dst carries this type". An edge labelled `{Owns, Contains}`
counts against a declaration on each independently. Direction is part of the
pair.

Enforced on every write funnel — `AddEdge`, `AddEdges`, `UpdateEdge` adding a
declared label, and all three inside a transaction — and identically on both
backends. A batch and a transaction also check against themselves, because two
duplicates arriving together are the same violation as one arriving after the
other. Deleting an edge frees the pair *within the same transaction*, which is
what makes delete-the-subtree-and-rebuild work.

Declaring over a graph that already violates the rule reports **every** offending
pair through `*store.EdgeCardinalityViolationsError` and declares nothing, the
way `DeclareUniqueProperty` does. A refused write carries the incumbent in
`*store.ErrEdgeTaken`. Both unwrap to the existing `store.ErrUniqueViolation`.

Nothing new is stored: the answer is in the source node's outbound adjacency, and
a second structure holding it is a second thing that can disagree with the first.
The cost is a scan of that adjacency per constrained write — cheap for the
ownership edges this exists for, worth knowing before declaring a type whose
sources are hubs.

Declarations live in memory and must be re-declared at every `Open`, as with
unique property keys. `graphene debug unique-edge -type <t> <dir>` answers "can
I?" from the shell.

### Aggregation, instead of pulling IDs into Go and counting them there

```go
freq, err := g.NeighbourFrequency(versions, store.DirectionOutbound,
        []store.EdgeType{edgeEmbeds}, []store.NodeType{nodeTracker})
counts, err := g.CountNodesByType()
dist, err := g.CountNodesByProperty("state")
```

`NeighbourFrequency` is the one most cross-graph analytics turn out to be: rank
what a set of sources point at, by how many of them point at it. It counts
**anchors, not edges** — a neighbour reached twice from one anchor is one vote,
and a repeated anchor is one anchor — because counting edges would make the
answer depend on how the graph happens to be modelled rather than on what it
says. It lives in `traversal` so it can take a `store.Budget`: the anchor set is
usually the result of an earlier query, so its size is data rather than something
the caller chose, and one hub among the anchors is enough to make it arbitrarily
large. `NeighbourFrequencyCtx` is the bounded, cancellable form.

`CountNodesByType` / `CountEdgesByType` are answered from the label postings, and
also appear on `GraphStats` and in `graphene store stats`. **An entity carrying
two labels is counted under both**, so they total more than `NodeCount` and
`EdgeCount` on any graph using multi-label entities; the CLI says so where the
tables would otherwise look wrong.

`CountNodesByProperty` / `CountEdgesByProperty` distribute an indexed field's
values, counting only live holders. Entities with no entry under the key have no
value to distribute and are absent, as are values held only by deleted entities.

All four are `store.Aggregator`, an optional extension both bundled backends
implement. It has no generic fallback — a `GraphStore` cannot enumerate its own
types — so a backend without it gets an error rather than an empty map that would
say the graph is empty.

### Custom labels can have names, and the store can carry them

`NodeTypeCustomBase` opens 32 768 values the application defines the meaning of,
and every one of them rendered as `Custom(7)`. An application with a dozen custom
types therefore carried its own number-to-name table and translated at every
boundary — and the moment that table drifted from the numbering the data was
written with, every log line, error and export silently misread the database.

```go
g.DeclareTypeNames(
    map[store.NodeType]string{nodeApp: "App", nodeVersion: "AppVersion"},
    map[store.EdgeType]string{edgeOwns: "Owns"},
)
```

The name is what `String()` renders and what `ParseNodeType` and `ParseEdgeType`
accept — **in addition to, never instead of** the `Custom(7)`, `custom:7` and
bare-numeric forms, so nothing that parsed before stops parsing. Names that could
not round-trip are refused: a built-in name, a numeric, anything shaped like a
custom-offset selector. Only the custom range can be named; the built-ins mean
what the engine says they mean.

**The half worth more is persistence.** On a disk store the table is written to
`graphene.labels` beside the image, so the graph stays interpretable without the
program that wrote it — which for a forensics engine is the normal case rather
than the exception. It is a sidecar, joining `graphene.grants`,
`graphene.redactions` and the rest, so the image format is untouched and an older
engine ignores it. Backup picks it up unchanged, because backup copies the
directory rather than a list of names it knows.

`Open` registers what the table says, and **a disagreement is an error rather
than a resolution** (`store.ErrTypeNameConflict`). The registry is process-wide,
so two stores in one process that disagree about what 32 768 means cannot both be
rendered correctly, and picking one silently is exactly the confident wrong
answer this exists to prevent.

### Fixed: `MaxTime` was the one budget dimension that did not bind

No API change and no format change here either, and no behaviour change at all
for a walk that does not set `Budget.MaxTime`.

#### `MaxTime` was unenforced on short walks

The traversal guard checked the context and the clock on the same 256-step
cadence. That number was chosen for cancellation, where overshooting by a few
microseconds costs nothing because the caller has already stopped caring about
the answer. As a deadline cadence it left a hole: **a walk charging fewer than
256 steps never looked at the clock at all**, so `MaxTime` was silently not
enforced on exactly the walks whose cost a caller cannot predict — few steps,
each of them slow, which is what a traversal over cold pages looks like.
`MaxNodes` and `MaxEdges` never had that hole, because they are charged per
visit.

A deadline is now checked every 16 steps. 16 is measured, not assumed: checking
on every step costs about a fifth of the walk, and 16 is indistinguishable from
no deadline at all. The full comparison is in `docs/benchmarks.md` under Phase 8,
along with `BenchmarkBFS_Deep_Budget` — the first benchmark in the tree to
exercise a non-zero `Budget`.

The check is paid only by callers who set `MaxTime`. With no deadline the guard
runs the code it always ran, and the benchmark's `unbounded` and `nodes` arms are
there to keep that true.

#### `enter()` now does what its comment claimed

It documented catching "a budget so small the origin alone exceeds it" and only
ever checked the context. A deadline that has already arrived now stops the walk
before it visits anything.

#### A negative `MaxTime` is a deadline already passed

It used to be ignored — `newGuard` tested `MaxTime > 0` — while `Unlimited()`
still reported `false`, so the walk paid for accounting and enforced nothing.
That was the one reading of "minus one second" that is neither a limit nor
unlimited. It is now refused at entry. Zero is still unlimited.

#### `MaxTime` is documented as best-effort, because it cannot be anything else

`MaxNodes` and `MaxEdges` are charged per visit and stop the walk on the step
that crosses them. `MaxTime` has to read a clock, and a clock has a granularity.
On Windows the Go monotonic reading is the system timer interrupt: 15.6 ms by
default, ~0.5 ms while some process on the machine has raised the timer
resolution, and which of the two applies changes as unrelated programs start and
stop. Two readings inside one tick are *equal* rather than ordered, so a
`MaxTime` below that floor cannot be observed as exceeded by `After`, by
`!Before`, or by any other comparison.

`store.Budget.MaxTime` now says this where a caller sizing a budget will read it,
as do `docs/API_REFERENCE.md` and `docs/TECHNICAL_DETAILS.md`. Nothing preempts a
single step that runs long either, so a cold page read can outlast a `MaxTime` on
its own. Bound a walk with `MaxNodes` and `MaxEdges`; let `MaxTime` catch what
they miss.

*If this affects you*: a walk that was overrunning its `MaxTime` now stops. That
is the fix, and it is a behaviour change for anyone who set a deadline the engine
was not honouring.

#### Deadline tests no longer depend on the machine's clock

`tests/graphene_budget_test.go` asserted that `MaxTime: time.Nanosecond` stops a
4 000-node walk. On a coarse-clock platform that is asserting the clock advanced
between two reads, which is a property of how busy the box is and of what
unrelated software has done to the timer resolution — so it failed intermittently
on Windows, moving between the `/disk` and `/memory` subtests from run to run.

It now uses a `MaxTime` already elapsed, which means the same thing everywhere.
Enforcement proper moved to `traversal/guard_test.go`, which drives an injected
clock and can therefore assert the cases the real one cannot reach: a deadline
reached exactly on a reading, the cadence itself, and the short-but-slow walk
that motivated the fix.

### Also

- `EdgeIDBetween(src, dst, types)` returns the incumbent rather than a boolean,
  which is what a caller checking for a duplicate actually wants. `EdgeExists` is
  now a wrapper on it and no longer materialises the entire outbound edge slice —
  records, label slices and property blobs — to compare one field.
- `graphene store stats` and `graphene debug stats` grow per-label tables, sorted
  by count. The golden corpus moved accordingly.

### Measured, not changed

Two costs that were being guessed at now have numbers in `docs/benchmarks.md`
(Phase 9), and neither needed code:

- **Declaring a unique key** costs about 14.7 ns per distinct value, linear, one
  allocation. A million distinct values is 14.7 ms once at Open. The
  declare-without-validating escape hatch this was going to justify is not worth
  building.
- **An open transaction** holds roughly 0.2 KB per buffered entity — about 400 MB
  for a million nodes and a million edges, or 1.75x what the data occupies once
  stored. One source is one transaction up to a few million entities; past that,
  `bulk` is the answer and per-entity atomicity is what it costs.

---

## v0.5.0 — idempotent ingest

The theme is one property: **re-ingesting a source that has already been ingested
produces the same graph, not a second copy of it.** Everything below is either
that property or something that was quietly preventing it.

There is **no on-disk format change**. The WAL record types this release puts
inside transactions (`0x03`/`0x04`, property-index entries) have existed and
replayed since before transactions did; what was missing was a way to frame them
between the begin and commit markers. A v0.5.0 store is readable by v0.4.0 and
vice versa.

### Breaking changes

Three, each deliberate. All are behaviour changes at the API surface; none change
how an existing file is read.

**1. `AddNode` and `AddEdge` reject an empty label set.**

They return an error wrapping `store.ErrNoLabels` instead of storing a
label-less entity. Such an entity indexes into no label posting, so it was
invisible to `NodesByType` and to every Types-filtered query while still being
counted — silent data loss with no error and no symptom. `UpdateNode` and the
transaction resolver had always rejected it, so the same struct that inserted
cleanly failed on update, and three separate documents described a requirement
no add path enforced. Edge labels were unchecked everywhere, including inside a
transaction; they are checked now too.

*If this breaks you*, you were writing entities you could only find again by an
ID you had to have kept.

**2. `ReindexReject` is the default `ReindexPolicy`, and the enum is renumbered.**

`UpdateNode` / `UpdateEdge` on an entity that carries property-index entries now
return `store.ErrIndexedPropertiesRequired` rather than leaving the index
describing something the entity no longer is. `ReindexReject` is the new zero
value, so `ReindexKeep` and `ReindexPurge` have moved by one — an API break for
anyone writing `store.ReindexPolicy(0)` rather than the constant.

The old default was a silently wrong answer that the query planner then trusted,
and avoiding it required every caller to remember a second method name. Note
that `ReindexPurge` was never the safe alternative: it drops the entity's
*untouched* keys along with the stale one, trading a stale-entry bug for a
missing-entry bug, and a missing entry costs a query result rather than a query.
Refusing is the only option that loses nothing.

*If this breaks you*: an entity with no index entries updates normally, so this
only fires where something was already at risk. Either move to
`UpdateNodeIndexed` / `UpdateNodePartialIndex`, or call
`g.SetReindexPolicy(store.ReindexKeep)` at open to restore the old behaviour
exactly.

**3. `UpdateNodeIndexed` / `UpdateEdgeIndexed` are now one transaction.**

They were three separate store calls that the documentation described as atomic
and that were not. Behaviour is otherwise unchanged; a crash can no longer land
the record without the entries.

### New

- **Property-index registration inside a transaction.**
  `Tx.IndexNodeProperties` / `Tx.IndexEdgeProperties`. Topology and index become
  durable together or not at all, which removes any need for a reconciliation
  pass after an unclean shutdown. Entries are framed in sorted key order, so two
  identical transactions write identical bytes.

- **Unique property keys.** `DeclareUniqueProperty`,
  `DeclareUniqueEdgeProperty`, `UniqueProperties`, `NodeByProperty`,
  `EdgeByProperty`. Declaring validates existing data and reports **every**
  violation through `*store.UniqueViolationsError`, so a graph can be repaired in
  one pass. Declaring is idempotent. Declarations live in memory and must be
  re-declared at each `Open` — which also re-validates the data.

- **Upsert.** `UpsertNode` / `UpsertEdge` on `Graph`, and `Tx.UpsertNode` /
  `Tx.UpsertEdge` which hand back the ID immediately so it can be used as an edge
  endpoint in the same transaction. On a hit the record is replaced and each key
  named in `props` is replaced rather than added to; keys not named survive.

- **`UpdateNodePartialIndex` / `UpdateEdgePartialIndex`**, which replace only the
  entries for the keys they name. This is the shape a state transition has, and
  the one `UpdateNodeIndexed` cannot express without the caller enumerating
  everything else just to preserve it.

- **The same four on a transaction**: `Tx.UpdateNodeIndexed`,
  `Tx.UpdateEdgeIndexed`, `Tx.UpdateNodePartialIndex`, `Tx.UpdateEdgePartialIndex`.
  A record and the entries describing it belong in one commit wherever they are
  written from.

- **`store.ErrWriteConflict`.** An upsert's key is resolved when the operation is
  buffered and re-checked under the write lock at commit, so two writers racing
  to create the same entity do not both succeed. This is optimistic concurrency
  control over exactly one predicate — there is still no general read-set
  conflict detection.

- **`graphene node upsert`, `graphene edge upsert` and `graphene debug unique`.**
  The shell can now express "make sure this exists" rather than only "add this":
  `node create` run twice adds two nodes, `node upsert` run twice leaves one.
  `debug unique` is the declaration run for its answer — it writes nothing and
  reports every value under a key that is held by more than one entity, which is
  the list a repair works from. Both upserts declare the key before writing, so a
  store that already violates the constraint is refused whole.

- **New errors**: `store.ErrNoLabels`, `store.ErrUniqueViolation`,
  `store.ErrIndexedPropertiesRequired`, `store.ErrWriteConflict`,
  `store.ErrKeyNotUnique`, and `*store.UniqueViolationsError` carrying
  `[]store.UniqueConflict`. Every sentinel is listed in
  [docs/API_REFERENCE.md](docs/API_REFERENCE.md) §4.

### Fixed

- **`disk.Store.IndexNodeProperty` took no lock and applied to the index before
  appending to the log** — the inverse of every other mutator, and of
  `PurgeNodeIndex`. A failed append returned an error having already registered
  the entry, leaving the index holding something the log had never heard of,
  which the next open silently dropped.

- **`memory.Store.ApplyTransaction` never bumped the version counter**, which is
  the memory backend's snapshot epoch. Two snapshots straddling a committed
  transaction reported the same `Epoch()` over different graphs — the one thing
  `Epoch` promises cannot happen.

- **A `TxOpUpdateEdge` that changed `Src` or `Dst` produced different graphs on
  the two backends**: the memory backend relocated the adjacency entries while
  the disk backend ignored the change. Endpoints are documented immutable, and
  both backends now pin them, matching what the non-transactional `UpdateEdge`
  has always done.

- **A property key longer than 65535 bytes was silently truncated** on the WAL
  round-trip and replayed under a different key. It is refused now.

### Upgrading

```go
g, _ := graphene.Open(dir)

// Declare at every open. Idempotent, and it re-validates the data.
if err := g.DeclareUniqueProperty("k"); err != nil {
    var v *store.UniqueViolationsError
    if errors.As(err, &v) {
        // v.Conflicts names every value held more than once
    }
}

// Ingest is now a transaction that includes its own index entries.
tx := g.Begin()
ver, created := tx.UpsertNode("k", []byte("ver:9f3a"), node,
    map[string][]byte{"state": []byte("extracted")})
perm, _ := tx.UpsertNode("k", []byte("perm:INTERNET"), permNode, nil)
tx.AddEdge(&store.Edge{Src: ver, Dst: perm, Labels: labels})
err := tx.Commit()   // store.ErrWriteConflict if another writer took a key
```

To keep the previous semantics with no other changes:

```go
g.SetReindexPolicy(store.ReindexKeep)
```
