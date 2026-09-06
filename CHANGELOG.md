# Changelog

Release notes start here. Tags v0.1 through v0.4.0 predate this file; use
`git log` for those.

## Unreleased — v0.7.0

### Declarations are now a property of the store, not of the process

A declaration tells the engine something it cannot work out for itself, and it
did not survive being told. Ordered keys and composite tuples were written into
the CSR image, so they came back after a compaction and vanished after a bare
reopen. Unique property keys and edge cardinality constraints were written
nowhere at all: every process that opened a store had to re-declare them, and one
that forgot got a store with no constraints and no indication there had ever been
any.

The lost optimisation was the mild half. The sharp half was that **two processes
could open one directory enforcing different rules**, both believing they held
the guarantee — and the one that had not declared would write exactly the
duplicates the other existed to refuse.

Every declaration is now recorded in `graphene.schema` beside the image as it is
made, and re-applied by every `Open`:

```go
g, _ := graphene.Open(dir)
g.DeclareUniqueProperty("sha256")
g.Close()

g, _ = graphene.Open(dir)          // no re-declaration
_, err := g.AddNode(dup)           // still refused
```

It covers all four kinds — ordered keys, composite tuples, unique property keys,
unique edge types — and applies to a read-only store too, which previously got
none of the writer's declarations, so a declared range query silently became a
scan for one process and not the other.

**No format change.** `graphene.schema` is a sidecar, like `graphene.labels`: an
older engine ignores a file it does not know about, a v0.7.0 store opens
unchanged in v0.5.x and v0.6.0, and backup carries it with no change because
backup copies the directory. Ordered and composite declarations are still written
into the image at compaction as well, so an older engine keeps finding them
there. The two sources are unioned, which cannot conflict: both are monotone.

The file is plain text, sorted, and deterministic — two stores with the same
schema produce identical bytes — and property keys are escaped, so it imposes no
restriction on what a key may contain.

### Behaviour change: an `Open` can now refuse a store that violates its own catalogue

Because the record outlives the process, a store's data can stop satisfying a
constraint the file names — an older build, or a process that never declared,
could have written the duplicates. `Open` therefore re-validates, and by default
**refuses**, naming every violation rather than the first. That is the same check
`DeclareUniqueProperty` has always made, at the same moment; a store that came up
believing a constraint it did not keep would have the guarantee and none of the
behaviour.

A store that cannot be opened cannot be repaired, so there is an escape hatch:

```go
g, err := graphene.OpenWithOptions(dir, disk.Options{Constraints: disk.ConstraintDrop})
// opens without the violated declarations; s.DroppedDeclarations() names them
```

`OpenReadOnly` and `OpenLive` default to `ConstraintDrop`, because a reader
enforces nothing and reading a damaged store is what it opened one to do.

### Transactions can read, and can be refused over what they read

`Tx` was a write-only buffer, so read-modify-write — the most common shape there
is — could not be made atomic. You read a node, decided from it, wrote it back,
and nothing checked that the record you decided from was still the record you
were overwriting. Two workers advancing the same node's state lost one of the two
updates, silently.

Transactions now read:

```go
tx := g.Begin()
id := tx.AddNode(&store.Node{...})
n, _ := tx.GetNode(id)              // sees its own buffered write
edges, _ := tx.EdgesOf(src, store.DirectionOutbound, nil)
```

`GetNode`, `GetEdge`, `NodeExists`, `EdgesOf`, `Neighbours`, `UniqueNodeOwner`
and `UniqueEdgeOwner` all answer as the transaction will look once committed:
buffered writes visible, deletes gone — including the edges a node deletion
cascades to — and unique claims resolving to what this transaction claimed them
for. This works on every backend and changes no failure mode.

`BeginTracked` adds the conflict check:

```go
for {
    tx := g.BeginTracked()
    n, err := tx.GetNode(id)
    if err != nil { return err }
    tx.UpdateNode(advance(n))
    err = tx.Commit()
    if errors.Is(err, store.ErrWriteConflict) { continue }  // re-read and retry
    return err
}
```

Every read that reached the store is recorded, and re-read under the write lock
before anything is applied. If a record changed, disappeared, appeared, or a
unique key moved, the transaction is refused whole and nothing is written. The
error is a `*store.ReadConflictError` naming the observation that moved, and it
wraps `ErrWriteConflict`, so the retry loop callers already write for upserts
works unchanged.

Adjacency is tracked as a set, contents included, which makes "does this node
already have such an edge — if not, create one" safe. Where a store-level
constraint fits, it is still the better answer: see `DeclareUniqueEdge`.

**Opt-in, deliberately.** `Begin` behaves exactly as before and records nothing.
Tracking can only *add* a failure, and code that upgraded and started reading
inside its transactions should not begin failing at commit without asking for it.

**What it is not.** Not serialisability, and no protection from phantoms. A
transaction that read "nothing carries this label" is not protected against
something appearing — a predicate over the whole graph cannot be validated by
re-reading a bounded set — which is why there is no tracked `QueryNodeIDs`.
Validation compares a digest of what was observed rather than a version stamp,
so a record rewritten with identical bytes is deliberately not a conflict.

A backend that does not implement the new `store.CheckedTransactor` refuses a
transaction carrying reads rather than committing it unprotected. Both bundled
backends implement it.

### Fixed

- **A node updated inside a transaction disappeared for concurrent readers.**
  The disk backend publishes a transaction's epoch only after the fsync that
  covers it, and releases the store lock before that wait so concurrent commits
  share one sync. Version retention did not account for it: a writer discarded
  the superseded version, and the superseded label posting, whenever no
  *snapshot* was open — but every plain reader in that window is running an
  epoch behind, and what it was entitled to see had just been deleted. So
  `GetNode` returned "not found" and `NodesByType` omitted it, for a live record,
  for the length of the fsync. Not stale: **absent**. This affects every release
  that has had transactions, is trivially reproducible with one committing
  goroutine and one reading one, and is why `tests/graphene_visibility_test.go`
  exists. Retention is now floored at the visible epoch while any commit is
  unpublished; a chain settles at two versions instead of one under a
  transactional writer, and single-record mutators are unaffected because they
  publish under the lock. Measured: no time regression, one allocation fewer.
- **`OpenWithOptions` never loaded the label table.** A store opened with
  `disk.StrictOptions` rendered every custom label as a number. Every open path
  now loads both sidecars.
- **`DeclareOrderedNodeProperty`, `DeclareOrderedEdgeProperty` and the two
  composite declarers took no store lock**, so two goroutines declaring
  concurrently raced. They now take the write lock, which they need anyway to
  write the catalogue atomically with the in-memory declaration.
- **The label table was written without an fsync.** A rename promotes whatever is
  on the medium, and without the sync that can be a prefix — which comes back as
  a truncated table and therefore as a mislabelling.
- **`GCMP` was missing from the CSR's known-section lists**, so `graphene csr`
  reported the composite section as unrecognised, and a future build marking it
  critical would have been refused for the wrong reason.

### Internal

- `FuzzDecodeCatalogue` covers the new parser from the start, rather than joining
  `readCompositeSection` on the list of hand-rolled readers that have none.
- Read-set comparison lives in `store.ReadSetValidator`, driven by both backends
  rather than implemented in each. Two entirely different storage layouts cannot
  be relied on to reach the same verdict if each owns its own copy of the
  reasoning — and a read set is a constraint, where a divergence is a lost write.
- `BenchmarkTxUpdate_Disk_NoSync_*` isolates the transactional write path from
  the fsync, which dominates `BenchmarkConcurrentCommits` by an order of
  magnitude on a loaded machine.

---

## v0.6.0 "Armchair" — structure, aggregates, and names

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
