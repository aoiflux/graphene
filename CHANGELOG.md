# Changelog

Release notes start here. Tags v0.1 through v0.4.0 predate this file; use
`git log` for those.

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
