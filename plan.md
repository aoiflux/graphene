# Graphene DB — Engineering Plan

> **Internal planning document.** Not upstream material, not a spec, not user
> documentation. It is regenerated and amended as planning evolves.
>
> **Status legend.** 🟢 **DONE** · 🟡 **IN PROGRESS** · 🔴 **TODO** ·
> 🟣 **NEEDS INVESTIGATION**. Markers render identically in every Markdown
> viewer and in the raw file — no terminal required.
>
> **Method.** Everything below was derived from read-only inspection of the
> repository at `HEAD` (`6317181`, tag `v0.3.0`) using `git ls-tree`, `git log`,
> `git show`, `git grep`, and direct file reads. No repository code was modified,
> no patch was produced, and no write operation is proposed anywhere in this
> document. Where a claim could not be established from the tree it is marked
> 🟣 **NEEDS INVESTIGATION** rather than asserted.

---

## Contents

1. [High-Level Architecture Overview](#1-high-level-architecture-overview)
2. [Repository Audit Summary](#2-repository-audit-summary)
3. [Module-by-Module Plans](#3-module-by-module-plans)
   - [3.1 Core Engine](#31-core-engine)
   - [3.2 Storage Layer](#32-storage-layer)
   - [3.3 Indexing](#33-indexing)
   - [3.4 Traversal & Algorithms](#34-traversal--algorithms)
   - [3.5 Provenance & Temporal Graphs](#35-provenance--temporal-graphs)
   - [3.6 Visualization Layer](#36-visualization-layer)
   - [3.7 Developer Experience](#37-developer-experience)
4. [Dependency Graph](#4-dependency-graph)
5. [Missing Features & Gaps](#5-missing-features--gaps)
6. [Proposed Roadmap](#6-proposed-roadmap)
7. [Risk Analysis](#7-risk-analysis)
8. [Open Questions](#8-open-questions)
9. [Status Dashboard](#9-status-dashboard)

---

## 1. High-Level Architecture Overview

Graphene is an **embeddable, zero-dependency Go graph engine** (`module
github.com/aoiflux/graphene`, `go 1.26`, no `require` block — the entire engine
is standard library only). It is not a server, has no wire protocol, no query
language, and no daemon. The consumer links it into their own process.

### 1.1 Layering

```
                    ┌─────────────────────────────────────────┐
   public façade    │  graphene.Graph  (graphene.go)          │
                    │  + helpers.go  + transaction.go         │
                    │  + shutdown{,_unix,_windows}.go         │
                    └───────────────┬─────────────────────────┘
                                    │ embeds store.GraphStore
                                    │ type-asserts capability interfaces
                    ┌───────────────▼─────────────────────────┐
   contract         │  store/  — GraphStore + 11 optional     │
                    │  capability interfaces, model types,    │
                    │  query types, sorted-ID algebra         │
                    └───────┬─────────────────────┬───────────┘
                            │                     │
              ┌─────────────▼──────┐   ┌──────────▼───────────┐
   backends   │  memory/           │   │  disk/               │
              │  maps + adjacency  │   │  CSR + delta + WAL   │
              └─────────┬──────────┘   └──────────┬───────────┘
                        │                         │
                        └───────────┬─────────────┘
                                    │ both own an *index.PropertyIndex
                    ┌───────────────▼─────────────────────────┐
   indexes          │  index/  — sharded property postings,   │
                    │  ordered range index, residual planner  │
                    │  index/encoding — order-preserving codecs│
                    └─────────────────────────────────────────┘

   consumers of the contract, not of the backends:
     traversal/   BFS, DFS, provenance, bidirectional path, VF2 patterns
     viz/         self-contained interactive HTML export
     examples/    package main, runnable demonstrations
```

### 1.2 The three architectural commitments

**(a) One fat interface would have been wrong, so it was not built.**
`store.GraphStore` is the minimum every backend must satisfy (write, mutate,
point read, adjacency, type lookup, property index, query, lifecycle). Eleven
*optional* interfaces sit beside it — `Reindexer`, `OrderedIndexDeclarer`,
`IndexVerifier`, `IndexRebuilder`, `AdjacencyReader`, `DegreeCounter`,
`NodeQueryExplainer`, `EdgeQueryExplainer`, `BatchReader`, `Transactor`,
`Syncer`. `Graph` type-asserts each and degrades gracefully when a third-party
store lacks it (`graphene.go:100-194`, `helpers.go:429-438`, `transaction.go:72-83`).
Both bundled backends implement all eleven. This is the single most consequential
design decision in the tree: it is what keeps the public façade thin and what
makes third-party stores viable.

**(b) The disk backend is a two-layer read, not a B-tree.**
Bulk data lives in an immutable **CSR** (compressed sparse row) image; everything
written since the last `Compact()` lives in an in-memory **delta** (maps) plus
**delete masks** that hide superseded CSR records. A read merges the two. This is
explicitly optimised for *ingest-once, query-many* — the forensic-artefact
workload the engine was built for — and is why there is no in-place page
mutation, no free-list, and no buffer pool.

**(c) Durability is a WAL, and the fsync boundary is explicit.**
Every write is framed into an append-only log with a per-record CRC32 and, for
batches, begin/commit markers carrying a count and a body checksum. `fsync`
happens at batch commit (`syncOnCommit`), at explicit `Sync()`, at `Compact()`'s
checkpoint, and at `Close()` — **not** per single write. `docs/TECHNICAL_DETAILS.md:1052`
states this plainly: *"A returned write is not yet durable."*

### 1.3 What the architecture deliberately does not have

No query language. No snapshot isolation. No persisted statistics or histograms.
No secondary-index inference. No automatic compaction. No history. No process
boundary, therefore no replication, no sharding, no auth. Each of these is a
consequence of the embeddable, single-process, ingest-then-read premise rather
than an oversight — but several become gaps the moment the premise widens
(see §5).

---

## 2. Repository Audit Summary

### 2.1 Provenance of the audit

| Property | Value |
| --- | --- |
| Root | `O:/research/graphene` |
| Branch | `main` (only local branch; `origin/main` tracked) |
| HEAD | `6317181` — *"rm references to plan.md"*, tagged `v0.3.0` |
| Commits | 32 |
| Tags | `v0.1`, `v0.1.0`, `v0.1.1`, `v0.1.2`, `v0.1.3`, `v0.2.0`, `v0.3.0` |
| Working tree | clean except untracked `plan.md` (this file) |
| Go module | `github.com/aoiflux/graphene`, `go 1.26`, **zero external dependencies** |

### 2.2 Release history, read from the log

The tags mark genuine capability boundaries rather than arbitrary cut points:

| Tag | Commit | What it added |
| --- | --- | --- |
| — | `30c75fa` | Initial engine: CSR, WAL, both backends, traversal, viz, 9 239 lines in one commit |
| `v0.1` | `0401fb1` | Import-friendly package rename |
| `v0.1.0/1` | `e9d6cd1` | fsync reliability, graceful shutdown on signal |
| `v0.1.2` | `939dca4` | Visualization rebuilt (+1 351 lines in `viz/exporter.go`, 4 sample pages) |
| `v0.1.3` | `8a9a89b` | Query types, multi-hop queries, custom node types, helpers |
| `v0.2.0` | `036aac0` | **Mutation**: update/delete, cascade, tombstones |
| — | `e046db1`, `ac5a458` | Ordered index + `index/encoding`; **`temporal_index.go` and `type_index.go` deleted**; concurrency work |
| — | `985f16d` → `13cdeee` | Property index, residual planner, concurrent writes |
| — | `77ca114` → `2148b92` | Bulk read/write, WAL batching, CSR layout |
| — | `c5c3927`, `1a43b37` | **Transactions**: `Begin()`, `ApplyTransaction`, transactional mutate |
| — | `2e60300` → `eb4ee92` | Docs moved to `docs/`, buffered replay, CRC via stdlib, open-path and index-memory optimisation |
| `v0.3.0` | `6317181` | Documentation consolidation |

Two deletions are load-bearing for §3.3 and §3.5: `index/temporal_index.go`
(a sorted `[]TemporalEntry` with `Add`/`Range`/`Len`, visible at `ac5a458^`) and
`index/type_index.go` were removed in `ac5a458`. Type indexing was **absorbed**
into both backends (CSR label postings + delta postings). Temporal indexing was
**not** replaced by an equivalent — it was superseded by the general ordered
index plus `encoding.Time`, which is a different and weaker thing (see §3.5).

### 2.3 File inventory by package

| Package | Source files | Notable sizes | Role |
| --- | --- | --- | --- |
| root | `graphene.go`, `helpers.go`, `transaction.go`, `shutdown*.go` | 11.4 K / 22.1 K / 11.3 K | Public façade |
| `store/` | `interface.go`, `types.go`, `query_types.go` | 16.3 K / 10.2 K / 16.3 K | Contract + model + sorted-ID algebra |
| `disk/` | `store.go`, `csr.go`, `wal.go`, `walbatch.go`, `transaction.go` | **107.6 K** / 19.7 K / 20.4 K / — / 8.9 K | On-disk backend |
| `memory/` | `store.go`, `transaction.go` | 48.0 K / 7.7 K | Reference backend |
| `index/` | `property_index.go`, `narrow.go`, `ordered_index.go` | 29.3 K / 10.5 K / 6.0 K | Secondary indexes + residual planner |
| `index/encoding/` | `encoding.go` | 5.5 K | Order-preserving codecs |
| `traversal/` | `bfs.go`, `dfs.go`, `path.go`, `subgraph.go`, `walker.go` | — / 6.3 K / — / 10.9 K / — | Algorithms |
| `viz/` | `exporter.go` | 52.3 K | HTML export (template dominates) |
| `examples/` | `main.go` + 4 more | 59.3 K | `package main`, runnable |

**`disk/store.go` at 107 KB / ~3 500 lines is the outstanding structural problem
in the tree.** It contains the store, the query planner, the driver-selection
cost model, marshalling for four record kinds, CSR deserialisation, batch reads,
the transaction commit path, and index verification/rebuild. Its top-level
declaration list runs to 130+ symbols. Nothing about it is broken; everything
about it resists isolated change (see §3.7, §7).

### 2.4 Test surface

29 `_test.go` files, ~380 test and benchmark functions. Density is high and the
*kinds* of test present are unusually good:

- **Parity suites** — `graphene_index_parity_test.go`, `graphene_query_parity_test.go`,
  `graphene_label_driver_test.go` pin the two backends to identical behaviour,
  which is the mechanism preventing backend drift.
- **Integrity suites** — `graphene_index_integrity_test.go` (22.6 K),
  `graphene_consistency_test.go`, `graphene_residual_test.go` (27.5 K).
- **Format guarantees** — `disk/crc_test.go` pins CRC32 output vectors because
  the checksum is an on-disk format promise; `disk/csr_layout_test.go` includes
  `TestAdjacencyArraysAreDeadBytes`, which proves a region is unread by
  corrupting it.
- **Footprint and concurrency** — `graphene_footprint_test.go`,
  `graphene_concurrency_test.go`, `graphene_parallel_bench_test.go`.
- **Stress** — `graphene_stress_test.go` behind a `stress` build tag: 100 k nodes
  / 500 k edges, 50-goroutine write pressure, optional 1 M-node path.

**What is absent:** no CI configuration anywhere in the tree (no `.github/`, no
pipeline file). Every guarantee above is enforced only when a human runs
`test.ps1`. See §3.7.

### 2.5 Documentation surface

`docs/TECHNICAL_DETAILS.md` (65 K, 16 sections), `docs/API_REFERENCE.md` (58 K,
20 sections), `docs/USER_GUIDE.md` (21 K), `docs/benchmarks.md` (61 K),
`docs/comparison.md` (19 K), `CONTRIBUTING.md` (14 K). This is a *documentation-
dense* repository — roughly 240 KB of prose against ~430 KB of Go including
tests. `CONTRIBUTING.md` is not boilerplate: it encodes measurement methodology
(never compare across sessions; always include a control; read the control before
the headline) and a stated optimisation priority order. `docs/TECHNICAL_DETAILS.md:1460`
carries an honest 11-item known-limitations list, which this plan takes as input
rather than re-deriving.

### 2.6 Overall assessment

The engine is **materially more mature than its README claims** ("experimental,
pre-production"). CRUD, transactions, durability, a costed query planner, dual
backends with enforced parity, and a real benchmark methodology are all present
and tested. The gaps are not in the core — they are in (i) everything temporal,
(ii) operational/lifecycle automation, (iii) the tooling surface around the
engine, and (iv) the structural weight of `disk/store.go`.

---

## 3. Module-by-Module Plans

---

### 3.1 Core Engine

#### Overview

The core engine is the public façade (`graphene.Graph`), the storage contract
(`store/`), and the transaction machinery that spans both. It owns the model
(`Node`, `Edge`, labels, properties), the capability-negotiation pattern, and
the ID discipline.

#### Current Implementation (based on git audit)

`Graph` embeds `store.GraphStore` (`graphene.go:54-56`), so every contract method
is promoted verbatim and the façade only adds what needs a type assertion or a
composition of calls. Present today:

- **Model** — `Node{ID, Labels []NodeType, Properties []byte}`,
  `Edge{ID, Src, Dst, Labels []EdgeType, Weight float32, Properties []byte}`.
  Multi-label by construction; at least one label required. Properties are an
  opaque blob (msgpack by convention, but the engine never decodes it).
- **Type system** — `uint16` labels; 0–32767 reserved, 32768–65535 user-defined
  via `CustomNodeType`/`CustomEdgeType`; string selectors parsed by
  `ParseNodeType`/`ParseEdgeType` accepting `custom:7`, `custom(7)`, `custom-7`,
  and bare numerics (`store/types.go:44-261`).
- **Transactions** — `Graph.Begin()` returns a `*Tx` buffering `[]store.TxOp`.
  IDs are reserved eagerly (`ReserveNodeID`/`ReserveEdgeID`) so an edge can
  reference a node the transaction has not committed. Commit is one
  `ApplyTransaction` call; ops apply **in issue order**, each evaluated against
  the store as modified by its predecessors. A non-`Transactor` backend falls
  back to replaying through the public API with placeholder-ID rewriting —
  ordered but **not atomic**, and `Tx.Atomic()` reports exactly that
  (`transaction.go:36-378`).
- **Lifecycle** — `Open`/`NewInMemory`/`Close`/`Sync`/`Compact`, plus
  `HandleSignals` binding `Close` to OS signals with per-platform signal sets
  (`shutdown_unix.go`, `shutdown_windows.go`).
- **Composed helpers** — batch read/write, multi-key property queries, degree
  and connectivity, `InducedSubgraph`, `HasCycle`, relation queries with
  direction-aware anchoring, result adapters (`helpers.go`).

#### Data Structures

| Structure | Location | Shape | Note |
| --- | --- | --- | --- |
| `Graph` | `graphene.go:54` | embedded interface | zero state of its own |
| `Tx` | `transaction.go:45` | `[]TxOp` + counters + latched error | not goroutine-safe by contract |
| `TxOp` | `store/interface.go:394` | tagged union (`Kind` selects payload) | six kinds |
| `Node`/`Edge` | `store/types.go:300,321` | slices + opaque blob | copy-in on write, alias-out on read |
| sorted-ID algebra | `store/query_types.go:113-280` | generic over `~uint64` | `InsertSortedID`, `DeleteSortedID`, `IntersectSortedIDs`, `UnionSortedIDs`, `SortDedupeIDs` |

The sorted-ID algebra deserves note: it is generic, hand-rolled (`sortSearchID`
avoids `sort.Search`'s indirect call per probe), and comments record the measured
justification — the tail-check fast path in `InsertSortedID` exists because
without it ingest regressed 12–30 %.

#### Durability Model

The engine's contract, restated from the façade's own documentation:

1. A returned write is **visible** immediately (delta + index updated under lock).
2. A returned write is **not durable** until one of: batch commit with
   `syncOnCommit`, explicit `Graph.Sync()`, `Compact()`, or `Close()`.
3. `Tx.Commit()` is all-or-nothing on both bundled backends: one framed WAL
   batch, one write. If the write fails, the commit marker never lands and
   replay discards the partial bytes — *"that absence is the rollback"*
   (`disk/transaction.go:258-260`).
4. IDs are **monotonic and never reused**, but explicitly **not dense** — a
   rolled-back `Tx` burns the IDs it reserved, and `AddNodesBatch` burns IDs on
   WAL failure.

#### Concurrency Model

- `disk.Store` — one `sync.RWMutex` over delta/masks/counters; `atomic.Uint64`
  sequences; `atomic.Pointer[CSRGraph]` + `atomic.Int64` shadow counter enabling
  a genuinely lock-free CSR point read (§3.2).
- `memory.Store` — one `sync.RWMutex`. `docs/TECHNICAL_DETAILS.md` records that
  memory-backend read concurrency is **negative past one core**.
- `index.PropertyIndex` — 16 shards, each with its own `RWMutex`, keyed by FNV-1a
  of the property key. The reverse map is sharded **alongside** the forward map,
  so no operation needs two shard locks and there is no lock ordering to get
  wrong (`index/property_index.go:35-49`).
- `WAL` — ring buffer of 1 024 slots with atomic head/tail, a `writeMu` for the
  single drain point, and a barrier + in-flight counter for maintenance ops.
- `Tx` — explicitly **not** safe for concurrent use; it is a caller-side buffer.

#### Missing Features

| Gap | Consequence | Status |
| --- | --- | --- |
| No snapshot isolation | a sequence of reads is not a consistent view; documented at `TECHNICAL_DETAILS §10.1` | 🔴 **TODO** |
| No read transactions | cannot pin a view across a multi-step analysis | 🔴 **TODO** |
| `Tx` buffers wholly in memory | one enormous transaction is not free; callers must chunk manually | 🟡 **IN PROGRESS** |
| No context/cancellation | a long `FindPatterns` or `Compact` cannot be cancelled | 🔴 **TODO** |
| No structured error taxonomy beyond `ErrNotFound` / `ErrInvalidEdge` / `ErrTxDone` | callers string-match or type-switch narrowly | 🟡 **IN PROGRESS** |
| No `Stats` beyond node/edge counts | no per-label, per-key, or delta-size visibility for operators | 🔴 **TODO** |

#### Risks

- **R-CE-1 — the non-atomic `Tx` fallback is a silent semantic downgrade.**
  `commitFallback` preserves order but not atomicity. `Atomic()` reports it, but
  nothing forces a caller to check. A third-party store therefore gets a
  transaction that *looks* transactional.
- **R-CE-2 — ID density is not promised but is easy to assume.** Reserved-and-
  discarded IDs leave holes; any consumer treating IDs as dense indices breaks.
- **R-CE-3 — visibility-before-durability is the sharpest edge in the API.** A
  caller that reads back its own write and concludes it is safe is wrong. This is
  documented thoroughly and still mis-assumable.
- **R-CE-4 — `HasCycle` recurses without a depth guard beyond `maxDepth`**
  (`helpers.go:558-591`); a deep graph with a large `maxDepth` is a stack risk.

#### Roadmap

| Phase | Item | Rationale |
| --- | --- | --- |
| P1 | Extend `GraphStats` to expose delta size, per-label counts, WAL byte length, and time since last compact | operators currently have no signal for *when to compact*; this is the input to §3.2's policy work |
| P1 | `context.Context` variants on the long-running entry points (`FindPatterns`, `Compact`, `BFS`) | cancellation is unavailable at any granularity today |
| P2 | Read-snapshot handle over the disk backend | the immutable CSR pointer already provides most of the mechanism; the delta is what needs versioning |
| P2 | Streaming/chunked transaction commit | removes the "one enormous transaction is not free" caveat |
| P3 | Error taxonomy: wrap all engine errors in a small typed set with `errors.Is` targets | |

#### Status

🟢 **DONE** — core CRUD, transactions, capability negotiation, and lifecycle are
complete, dual-implemented, and parity-tested. Remaining work is additive
(observability, cancellation, snapshots), not corrective.

---

### 3.2 Storage Layer

#### Overview

Two backends implement one contract. `memory.Store` is the reference
implementation and the parity oracle. `disk.Store` is the production path: an
immutable CSR image, an in-memory delta of everything written since the last
compaction, delete masks that hide superseded CSR records, and an append-only
WAL in front of both.

#### WAL Design

**Framing.** `[type:1][length:4][payload:length][crc32:4]`.

**Record types** (`disk/wal.go:43-77`):

| Code | Meaning |
| --- | --- |
| `0x01` / `0x02` | node / edge record (re-appending an existing ID is an update — last-write-wins on replay) |
| `0x03` / `0x04` | node / edge property-index entry |
| `0x05` / `0x06` | node / edge tombstone (payload = ID:8) |
| `0x07` / `0x08` | node / edge property-index **purge** — drops all entries for an ID without deleting the entity; required by `ReindexPurge`, because without it replay resurrects superseded values |
| `0x09` / `0x0A` | batch begin / commit |
| `0xFF` | checkpoint (safe-truncation marker after compaction) |

**Atomicity.** Per-record CRCs catch a torn record but not a torn *batch* — 500
of 1 000 records are each individually valid. Hence begin/commit markers: begin
carries an expected count, commit carries count + a CRC over the accumulated
body. Replay buffers records between the markers and applies them only when the
commit both exists and *describes what was read back*; otherwise the whole batch
is discarded (`disk/wal.go:311-346`).

**Forward-compatibility is deliberately one-directional.** Replay *errors* on an
unknown record type rather than skipping it, because skipping would let an older
binary apply a rolled-back batch by ignoring the very markers meant to suppress
it (`disk/wal.go:353-358`). A WAL written by a newer build is unreadable by an
older one, on purpose.

**Write path.** A lock-free ring (1 024 slots, power-of-two mask, atomic
head/tail) absorbs concurrent appends; a single holder of `writeMu` drains them
in sequence order. Overflow takes the lock and drains until there is room.
Maintenance ops (`Checkpoint`, `Truncate`, `Replay`, `Close`, `Sync`) raise a
barrier, spin until in-flight appends reach zero, then take `writeMu`.

**Replay path.** Buffered through a `bufio.Reader` sized to the log and capped at
1 MiB (floor 4 KiB). The comment records the measurement that motivated it: three
`ReadFull` calls per record meant ~180 000 syscalls for a 60 000-record log, and
69 % of a cold open sat in `syscall.readFile`. CRC32 was then a bit-by-bit loop
costing 46 % of what remained; it was replaced by `crc32.ChecksumIEEE`, which
produces **byte-identical output** — pinned by `TestComputeCRC32Vectors` because
it is an on-disk format guarantee, not an implementation detail
(`disk/wal.go:250-272`, `579-594`).

**Windows-specific correctness.** `Truncate` closes the file before truncating,
because an `O_APPEND` handle cannot be truncated in place on Windows
(`disk/wal.go:202-217`).

#### Compaction Strategy

`Compact()` (`disk/store.go:2070-2172`) is a **full rebuild**, manual only:

1. Collect surviving CSR records (skipping delta-overridden and tombstoned IDs)
   plus all delta records.
2. `Build()` a fresh `CSRGraph`; stamp the current sequence high-water marks so a
   reopen never reuses an ID whose record was dropped.
3. `SerialiseWithIndex(propIdx.NodeEntries(), propIdx.EdgeEntries())` — the
   property index rides **inside** the CSR file (v6+), so the truncated WAL is
   left genuinely empty. Before v6 every compaction re-emitted the whole index
   and every restart replayed it, a cost that grew without bound.
4. Write to `graphene.csr.tmp`, WAL `Checkpoint()`, atomic `os.Rename`, WAL
   `Truncate()`, publish the new CSR, reset delta and masks.

**Properties.** Crash-safe by temp-write-plus-rename. Correct. And **entirely
unpoliced**: nothing triggers it, nothing bounds delta growth, nothing warns.
The entire serialised image is materialised as one `[]byte` in a
`bytes.Buffer` before hitting disk, so peak compaction memory is roughly the
whole graph plus the whole file.

#### Index Storage

**CSR file format v7**, magic `GCSR`, supported versions 2–7
(`disk/csr.go:407-536`, `disk/store.go:106-117`):

```
[magic:4 "GCSR"][version:2][nodeCount:8][edgeCount:8]
[nodeSeqHW:8][edgeSeqHW:8][indexOffset:8]          ← 46-byte header (v6+)
[node records]   id:8, labelCount:1, labels:2×N, propLen:4, props
[edge records]   id:8, src:8, dst:8, labelCount:1, labels:2×N, weight:4, propLen:4, props
[index section @ indexOffset]  magic "GIDX"
   [nodePropCount:8][entries…]  id:8, keyLen:2, key, valLen:4, value
   [edgePropCount:8][entries…]
```

What is persisted and what is not is a deliberate three-way split:

- **Property-index entries — persisted.** Values are caller-encoded; nothing in
  the file could reconstruct them.
- **Label postings — not persisted.** Derivable in one pass at load
  (`buildLabelIndex`), so persisting them would add size *and* a consistency risk.
- **Adjacency arrays — not persisted since v7.** They were written through v6 and
  never read; `deserialiseCSR` always rebuilt them via `Build()` and then jumped
  to `indexOffset`. On a 100 k-node fixture that was ~4.8 MB of a ~22 MB file.
  `TestAdjacencyArraysAreDeadBytes` corrupts the region and observes no
  difference — the deletion is proven, not assumed.

**In-memory CSR.** `nodes []nodeRecord` and `edges []rawEdge` indexed directly by
ID (1-based, slot 0 unused); `outOffset`/`inOffset` prefix-sum arrays with flat
`outEdges`/`inEdges`; `nodesByLabel`/`edgesByLabel` postings in ascending ID
order for free (construction walks records in ID order).

**Lock-free read path.** A published `CSRGraph` is immutable, so a reader that
loads the pointer atomically can read a record without `s.mu`. `csrShadowed`
counts CSR records superseded by a delta update or tombstone in the current
epoch; while it is zero every CSR record is still the truth. Crucially,
*appending* new entities shadows nothing, so ongoing ingest does not disable the
fast path for pre-existing records. The reader re-checks both the counter and
the CSR *pointer*, which is what catches a `Compact` that swapped the image and
reset the counter underneath it (`disk/store.go:48-67`, `757-782`).

#### Corruption Detection

| Layer | Mechanism | Behaviour on failure |
| --- | --- | --- |
| WAL record | CRC32-IEEE footer | stop replay at first mismatch — treated as torn tail |
| WAL batch | commit marker: expected count + body CRC | discard the whole batch |
| WAL type | `knownWALRecord` allow-list | **error**, do not skip |
| CSR header | `GCSR` magic + version range 2–7 | `deserialiseCSR: unsupported version` |
| CSR index section | `GIDX` magic + counts + bounds checks | parse error, open fails |
| Structural | `VerifyIndexes()` — postings ordering, reverse-map agreement, adjacency endpoint match, no entry outliving its entity | first inconsistency returned |
| Repair | `RebuildIndexes()` — recompute label postings and adjacency, drop property entries for dead entities | structure only, never content |

`Open` deliberately does **not** run `VerifyIndexes` — it measured ~200 ms on a
100 k-node store, an O(V+E) tax on every startup, and could not catch much that
parsing does not already reject. The reasoning is recorded in full at
`disk/store.go:167-181`.

**The honest limit:** verification is *structural*. It cannot check that an
indexed value still matches an entity's properties, because those values are
caller-encoded and the blob is opaque. There is also **no checksum on the CSR
file itself** — only its magic, version, and internal bounds. A silently
corrupted CSR body passes `Open`.

#### Cold Storage / Lazy Loading

**Absent entirely.** `loadCSR` reads the whole file and `deserialiseCSR` parses
every node and edge record into memory. `Open`'s doc comment says the CSR "is
memory-mapped"; the implementation does not mmap. Working-set size is therefore
the whole graph, always, and the largest graph is the one that fits in RAM.

Note also `csrBytes` / `cloneBytes`: the read path *aliases* CSR-owned property
blobs rather than copying (copy-in on write, alias-out on read), which is what
makes reads independent of blob size — and which makes any future mmap or
eviction scheme a correctness question, not just a performance one.

| Capability | Status |
| --- | --- |
| Whole-CSR load into heap | 🟢 **DONE** |
| `mmap`-backed CSR | 🔴 **TODO** |
| Per-node/per-edge lazy materialisation | 🔴 **TODO** |
| Property-blob paging (blobs are the bulk of most records) | 🔴 **TODO** |
| LRU / eviction of cold regions | 🔴 **TODO** |
| Tiered (hot delta / warm CSR / cold archive) storage | 🔴 **TODO** |

#### Missing Features

1. **No compaction policy** — no trigger, no threshold, no background worker, no
   advisory signal. Delta growth is unbounded until the caller intervenes.
2. **No CSR-level checksum** — body corruption is undetected.
3. **No incremental compaction** — always a full rebuild; cost is O(graph), not
   O(delta).
4. **No memory-bounded compaction** — whole image buffered before write.
5. **No mmap / lazy load** — §above.
6. **No WAL rotation or size cap** — the only bound on WAL size is `Compact`.
7. **No backup / snapshot / restore primitive** — no way to take a consistent
   copy of a live store.
8. **`memory.Store` has no persistence path** — no dump/load, so the reference
   backend cannot be checkpointed even for tests.

#### Roadmap

| Phase | Item | Notes |
| --- | --- | --- |
| **P0** | **Compaction policy + advisory API.** Expose delta node/edge counts and WAL bytes; add a `ShouldCompact()` advisory and an opt-in background trigger with configurable thresholds | the single highest-value storage gap: everything else here is a performance concern, this one is an operational failure mode |
| **P0** | **CSR body checksum** — extend the header with a CRC/hash over the body, verified at `Open` behind a flag | closes the last silent-corruption hole; format bump to v8, and the version range already accommodates it |
| P1 | **Streaming serialisation** — write the CSR through an `io.Writer` instead of a `bytes.Buffer` | removes peak-memory ≈ 2× graph during compaction |
| P1 | **WAL size cap + rotation** | bounds replay time independently of compaction cadence |
| P2 | **Incremental / partial compaction** — rebuild only affected regions | requires an ID-range-partitioned CSR; large design change |
| P2 | **mmap-backed CSR** with an explicit aliasing contract | the alias-out read model makes this delicate — see R-SL-3 |
| P3 | **Backup/snapshot primitive** — consistent copy of CSR + WAL under a barrier | |
| P3 | **`memory.Store` dump/load** | parity testing and fast fixtures |

#### Status

🟡 **IN PROGRESS** — WAL and CSR are well-designed, well-measured, and crash-safe. The layer
is incomplete in *operations*, not in mechanism: no compaction policy, no body
checksum, no memory bound on compaction, no lazy loading.

---

### 3.3 Indexing

#### Overview

Four index families, all in-memory, all rebuilt or reloaded at `Open`: the
primary ID→record mapping, adjacency, label postings, and the caller-driven
property index (with an opt-in ordered variant). A costed planner chooses which
one drives each query.

#### Current Index Types

| Index | Backing structure | Serves | Persisted? |
| --- | --- | --- | --- |
| Primary | map (memory) / direct array offset (CSR) | `GetNode`, `GetEdge` | with records |
| Adjacency | CSR prefix-sum arrays + `deltaAdj` overlay | neighbours, traversal, anchored relations, degree | rebuilt from records |
| Label | ascending postings per label, separately for CSR and delta | `NodesByType`, `EdgesByType`, `Types` pre-filter | rebuilt at load |
| Property (hash) | sorted postings per `(key, value)` + reverse ID→refs map, 16 shards | `PropertyOpEqual`, `NodesByProperty` | **yes**, in the CSR `GIDX` section |
| Ordered (range) | per *declared* key: values sorted by `bytes.Compare`, each with ascending postings | `>`, `>=`, `<`, `<=`, `Between`, `Prefix` | **no — declarations are lost on reopen** |

**Property index internals** (`index/property_index.go`). Sharded 16 ways by
FNV-1a of the key; postings sorted by ID so membership and insertion are
O(log n) + memmove, lookups return already-ordered slices the query path need not
re-sort, and duplicate registration is idempotent. A reverse map makes
`RemoveNode`/`RemoveEdge` proportional to that entity's own entries rather than
to the index. Key interning (`keyID`, added in `df7ad08` "index memory
optimisation") removes per-entry key-string duplication.

**Ordered index internals** (`index/ordered_index.go`). `[]orderedValue` sorted
ascending by `bytes.Compare`, each holding an ascending ID list. `rangeFor`
resolves an operator to a `[lo, hi)` window via `lowerBound`/`upperBound`; the
`Prefix` case computes an exclusive upper bound by incrementing the last non-`0xFF`
byte, with the all-`0xFF` case handled explicitly.

**The comparison-semantics fault line** — the sharpest correctness edge in the
whole engine. The scan path (`PropertyFilterMatches`) tries numeric comparison
first and falls back to bytes. That rule is *not a total order*: under it
`"9" < "10" < "1x" < "9"`, a cycle. No sorted structure can be built on it.
So a declared key switches to `PropertyFilterMatchesOrdered`, i.e. plain
`bytes.Compare`, **throughout** — including in the residual probe path, where
`postingsMatch` takes an `ordered` flag precisely so the probe cannot disagree
with the index serving the same predicate (`index/narrow.go:196-217`,
`store/query_types.go:354-416`). `index/encoding` exists to make byte order mean
what callers intend: `Uint64`, `Int64` (sign-flipped), `Float64`, `Time`,
`String`, `PrefixUpperBound`.

**Residual planner** (`index/narrow.go`). One filter drives the query; the rest
are residuals. Each is costed both ways — probe each candidate through the
reverse map, or resolve the filter's own set and intersect — and the cheaper is
chosen *per step*, re-decided after each step because every step shrinks the
candidate set. Residuals run most-selective-first and the pass short-circuits the
moment the candidate set empties. `noResiduals` fast-paths the single-filter
query, which is also the most common one; without it, planning cost +14 % time
and +70 % allocations.

**Driver selection** (`disk/store.go:1585-1868`, mirrored in `memory/store.go`).
Candidate drivers: explicit IDs, an equality filter's postings, a declared
ordered key's range, label postings, anchors' incident-edge lists, or a full
scan. `labelDriverWins` arbitrates label-vs-equality by estimated set size.
`ExplainNodeQuery`/`ExplainEdgeQuery` expose the resulting `QueryPlan`
(driver, driver key, candidate count, per-residual method and cost, result count)
— explicitly diagnostic, explicitly not part of the API contract.

#### Composite Index Plan

**Not implemented.** A multi-key `AND` query today drives from one filter and
applies the rest as residuals. That is a good general strategy and a poor one for
a *known, repeated* multi-key access path: a `(case_id, sha256)` lookup pays a
full `case_id` posting resolution before narrowing.

Proposed design, staged:

1. **Declaration API**, mirroring the ordered-key precedent —
   `DeclareCompositeNodeIndex(keys ...string)` on a new capability interface
   `CompositeIndexDeclarer`, so no backend is forced to implement it.
2. **Structure** — postings keyed by the concatenation of per-key encoded values
   under a length-prefixed framing (`len:2|value` repeated), which keeps the
   composite key unambiguous and preserves prefix semantics: an index on
   `(a, b, c)` can serve `a`, `(a, b)`, and `(a, b, c)` — a left-prefix rule
   worth stating in the API doc because callers will assume it either way.
3. **Maintenance** — the reverse map must gain composite refs so that
   `RemoveNode` still purges in time proportional to the entity's entries.
   This is the real cost of the feature.
4. **Planner integration** — a new `DriverComposite` kind; the driver chooser
   prefers the longest matching declared prefix over any single-key equality.
5. **Persistence** — declarations must survive reopen (see the ordered-key gap
   below), and the entries themselves are derivable from the hash postings, so
   only the *declaration* needs storing.

Status: 🔴 **TODO** — design sketched here, nothing built.

#### Temporal Index Plan

The original `index/temporal_index.go` (sorted `[]TemporalEntry{TimestampNs,
NodeID}`, `Add`, `Range(fromNs, toNs)`, `Len`) was **deleted in `ac5a458`** and
replaced by the general ordered index. The replacement is genuinely more
flexible — any key, any encoding, edges as well as nodes — and strictly weaker in
three ways:

1. **Declarations are not persisted.** Reopen and every range query silently
   reverts to a scan until the caller re-declares. There is no error, no warning,
   and `OrderedProperties()` will honestly report an empty list — the failure is
   a performance cliff, not a wrong answer.
2. **No time semantics.** `encoding.Time` gives an order-preserving byte encoding
   and nothing else: no interval type, no "as of", no overlap predicate.
3. **Timestamps are just another caller-registered property.** Nothing associates
   a timestamp with an entity's *validity*, only with the entity.

Proposed plan:

| Step | Item | Status |
| --- | --- | --- |
| T1 | **Persist ordered-key declarations** in the CSR header/index section and re-declare automatically at `Open` | 🔴 **TODO** |
| T2 | First-class `TimeRange` filter operator over a declared time key, so intent is expressible without the caller hand-encoding two bounds | 🔴 **TODO** |
| T3 | Interval semantics — `validFrom`/`validTo` as engine-known fields, enabling overlap and containment predicates | 🔴 **TODO** |
| T4 | Temporal driver in the planner (time-bounded query drives from the time index, not from labels) | 🔴 **TODO** |

T1 is small, high value, and independent of everything in §3.5 — it should not
wait for the temporal-graph work.

#### High-Degree Node Strategy

**No strategy exists.** Today:

- Adjacency for a CSR node is a contiguous slice — excellent for scanning,
  O(degree) for *any* filtered access. `degreeFilteredLocked` walks every
  incident edge to count a type-filtered degree.
- Delta adjacency is a plain `[]EdgeID` per node with linear `removeEdgeID`, so
  deleting one edge from a high-degree node in the delta is O(degree).
- Traversal deduplicates neighbours per expansion in a map (`walker.beginExpansion`),
  which is proportional to degree per visited node.
- A supernode therefore taxes every traversal that touches it, every filtered
  degree query, and every delta-layer edge deletion.

Proposed plan:

| Step | Item | Status |
| --- | --- | --- |
| H1 | **Measure first** — a benchmark with a deliberately skewed degree distribution (power-law fixture). `CONTRIBUTING.md` is explicit that the prize must be measured before the thing that wins it is built; there is currently no fixture that populates this case | 🔴 **TODO** |
| H2 | **Per-type adjacency sub-ranges** — sort each node's CSR adjacency by edge label and store per-label offsets, turning a filtered neighbour scan into a sub-slice | 🔴 **TODO** |
| H3 | **Degree cache** on the CSR (`OutDegree`/`InDegree` are already O(1); the *filtered* variants are not) | 🔴 **TODO** |
| H4 | **Sorted delta adjacency** — reuse `store.InsertSortedID`/`DeleteSortedID` so delta edge removal is O(log n) | 🟡 **IN PROGRESS** — the algebra exists; the delta simply does not use it |
| H5 | Supernode detection surfaced in `Stats` so callers can see the skew | 🔴 **TODO** |

#### Missing Features

1. Composite / multi-key indexes — none.
2. Ordered-key declarations not persisted (**the highest-value small fix in this
   module**).
3. No statistics persistence, no histograms — selectivity *within* a range is
   estimated by the key's total entry count, so a range matching 1 % and one
   matching 99 % of a key cost the same in the planner's eyes.
4. No regex, fuzzy, or full-text operators; `Contains` always scans.
5. No index on `Weight` — it is a first-class edge field yet unreachable by any
   index; a similarity-threshold query (`Weight > 0.8`) is a full scan.
6. No automatic property indexing — deliberate (the engine cannot decode the
   blob), but it means an un-indexed field is a silent scan.
7. No per-label adjacency segmentation (see H2).

#### Roadmap

| Phase | Item |
| --- | --- |
| **P0** | Persist ordered-key declarations; auto re-declare at `Open` (T1) |
| **P0** | Sorted delta adjacency using the existing sorted-ID algebra (H4) |
| P1 | Range-aware selectivity estimate — even a coarse equi-width histogram per declared key beats "count all entries under the key" |
| P1 | Indexable `Weight` (register it as a reserved ordered key) |
| P2 | Composite index: declaration API, structure, reverse-map refs, planner driver |
| P2 | Per-type adjacency sub-ranges (H2) after the skew benchmark (H1) lands |
| P3 | Temporal operators T2–T4, gated on §3.5 |

#### Status

🟡 **IN PROGRESS** — the property, ordered, and label indexes are strong and the residual
planner is genuinely sophisticated. Composite and temporal indexing are absent,
high-degree nodes are unaddressed, and the non-persisted ordered declaration is a
real, silent, per-restart regression.

---

### 3.4 Traversal & Algorithms

#### Overview

`traversal/` consumes only `store.GraphStore` plus the optional
`AdjacencyReader`, so it works against any backend. Its organising idea is the
`walker`: one reusable incident-edge buffer for a whole traversal instead of an
allocation per visited node.

#### Current Traversal Support

| Algorithm | Entry point | Implementation notes |
| --- | --- | --- |
| BFS (records) | `traversal.BFS` | two level buffers swapped per depth, not a growing queue — memory is O(widest level), not O(visited); depth is the loop counter so entries are bare IDs |
| BFS (IDs only) | `traversal.BFSIDs` | never materialises a record; `out` doubles as the queue; on an `AdjacencyReader` backend the whole walk allocates only the visited set and result |
| DFS | `traversal.DFS` | general depth-first with direction and type filters |
| Provenance chain | `traversal.ProvenanceChain` | inbound DFS, cycle-safe, default depth 64; uses `bestRemaining` rather than a plain `visited` set (`TECHNICAL_DETAILS §8.2`) |
| Shortest path | `traversal.ShortestPath` | **bidirectional BFS**, treats the graph as undirected, stores `{parent, edgeID}` per visit and materialises records only for the final path |
| Pattern matching | `traversal.FindSubgraphMatches` | VF2-inspired backtracking, pruned by label constraints, partial edge check at each depth, `maxMatches` cap |
| Cycle detection | `Graph.HasCycle` | recursive DFS with on-stack marking (in `helpers.go`, not `traversal/`) |
| Induced subgraph | `Graph.InducedSubgraph` | |

The `walker` (`traversal/walker.go`) is the shared substrate: it prefers
`AdjacencyReader.IncidentEdges` into a reused buffer, falls back to `EdgesOf`
for third-party stores, resolves liveness via `NodeExists` without materialising,
and replicates `Neighbours`' per-expansion deduplication in a cleared map rather
than a fresh one.

#### Bidirectional BFS

Implemented and in production use as `ShortestPath`. Expands forward and backward
alternately, one level at a time, checking each newly-visited node against the
opposite frontier. `visitEntry` deliberately stores the connecting **edge ID**
rather than the edge, because a bidirectional search touches far more nodes than
end up on the path.

Known gaps:

- **Unweighted only.** `Edge.Weight` is ignored; there is no Dijkstra and no A*.
  `docs/comparison.md:80` lists weighted shortest path as *"Not yet (weight
  available on edges, no Dijkstra)"*. For a similarity graph where `Weight` *is*
  the semantic — `EdgeTypeSimilarTo` carries a 0–1 score — this is a significant
  functional hole.
- **Single path only.** No k-shortest-paths, no all-shortest-paths.
- **No frontier-size balancing.** A textbook bidirectional search expands the
  smaller frontier each round; this one strictly alternates, which is
  materially worse on graphs with asymmetric branching.
- **Undirected only.** Direction cannot be constrained for pathfinding.

#### k-hop Neighborhood Caching

**Not implemented, nothing adjacent to it exists.** Every `BFS`/`BFSIDs` call
recomputes from scratch. For the stated workload — repeated k-hop expansion
around case and artefact nodes — this is the most obvious repeated-work saving in
the engine.

Design sketch:

- Cache key `(origin, maxDepth, direction, sorted edge-type set)`; value the
  ascending ID list from `BFSIDs`.
- **Invalidation is the entire problem.** The disk backend already has the right
  primitive: `csrShadowed` and the CSR pointer identify an epoch. A cache entry
  tagged with `(csrPtr, deltaVersion)` is valid while both are unchanged — which
  makes the cache trivially correct for a compacted, read-only store and
  automatically empty during ingest. A `deltaVersion` counter incremented on
  every mutating op does not exist yet and is a one-line addition.
- Bounded by entry count and total IDs held, LRU eviction, opt-in.
- Must be measured against the H1 skew fixture: caching a supernode's 2-hop
  neighbourhood may cost more memory than it saves time.

Status: 🔴 **TODO**.

#### Reachability Index

**Not implemented.** `Graph.IsConnected` runs a full `ShortestPath` and discards
the path, so a boolean reachability question costs a complete bidirectional
search.

Options, in ascending cost:

1. **Connected-component labelling** (undirected, recomputed at `Compact`).
   Answers "definitely not reachable" in O(1) and is *exact* for the undirected
   case `IsConnected` already implements. Cheap to build during the compaction
   pass that already walks every record. **This is the right first step.**
2. **2-hop / hop-labelling index** for directed reachability — much larger, much
   more useful, and a real research-grade build.
3. **Transitive-closure over a label subset** (e.g. only `Contains` edges), which
   is what provenance actually needs and is far smaller than full closure.

Status: 🔴 **TODO**, with option 1 recommended as P1.

#### Materialized Views

**Not implemented, and no mechanism exists that could support one** — there is no
change-notification hook, no trigger, no epoch/version counter exposed to
callers.

Prerequisites, in order: (i) a `deltaVersion` counter, (ii) a change-feed or
post-commit hook, (iii) a view registry with a refresh policy, (iv) persistence
of view results into the CSR image so they survive restart. Items (i) and (ii)
are also what k-hop caching (above) and any future incremental index needs — they
are the shared foundation, and worth building once with all three consumers in
mind.

Status: 🔴 **TODO**.

#### Missing Features

1. Weighted shortest path (Dijkstra / A*) — the highest-value gap in this module.
2. k-shortest-paths, all-paths, path enumeration with constraints.
3. Frontier-size balancing in the bidirectional search.
4. k-hop caching.
5. Reachability index.
6. Materialized views.
7. No centrality, community detection, PageRank, or triangle counting.
8. Pattern matching is unoptimised (documented, `TECHNICAL_DETAILS §8.3`): no
   candidate ordering by selectivity, no connectivity-driven match order, no
   symmetry breaking. `buildCandidates` also declines the unscoped case for
   unlabelled pattern nodes (`traversal/subgraph.go:174` — *"Unsupported without
   scope"*).
9. No parallel traversal — every walk is single-goroutine even though the CSR
   read path is lock-free and would permit it.
10. No cancellation (see §3.1).

#### Roadmap

| Phase | Item | Rationale |
| --- | --- | --- |
| **P0** | **Weighted shortest path** (Dijkstra with a binary heap; A* if a heuristic is supplied) | `Weight` is a first-class field with documented meaning and no algorithm reads it |
| P1 | Frontier-size balancing in `expandAndAdvance` | small, local, measurable |
| P1 | Connected-component labelling computed during `Compact`; `IsConnected` short-circuits on it | reuses a pass that already exists |
| P1 | `deltaVersion` counter + post-commit hook | shared prerequisite for caching, views, and incremental indexes |
| P2 | k-hop neighbourhood cache keyed on `(csrPtr, deltaVersion)` | |
| P2 | Pattern-matching optimisation: order candidates by selectivity, walk the pattern in connectivity order, break symmetry | documented as unoptimised; likely the largest constant-factor win available |
| P2 | Parallel BFS over the lock-free CSR path | |
| P3 | Materialized views |
| P3 | Reachability index (2-hop labelling), or label-scoped transitive closure for provenance |

#### Status

🟡 **IN PROGRESS** — the implemented algorithms are correct, allocation-conscious, and well
tested. Four named items in the brief (bidirectional BFS aside) are entirely
absent, and the missing weighted path is a functional gap rather than an
optimisation.

---

### 3.5 Provenance & Temporal Graphs

#### Overview

This is **the least-built module in the repository**, and the gap between what
the domain implies and what exists is the widest anywhere in the tree. The engine
was designed for forensic micro-artefact provenance — a domain in which "what did
this look like at time T" and "what changed between these two states" are the
core questions — and it can answer neither.

#### Node/Edge Versioning

**Does not exist.** The model is strictly current-state:

- `UpdateNode`/`UpdateEdge` **replace** labels and properties wholesale. The
  prior value is gone from the store the instant the WAL record is appended.
- The WAL *does* hold the history — 0x01/0x02 records are last-write-wins on
  replay, so earlier versions are physically present in the log — but `Compact()`
  checkpoints and truncates it. **History survives exactly until the next
  compaction**, is not addressable, and is not queryable.
- `DeleteNode` cascades and tombstones; after compaction the entity is
  unrecoverable.
- IDs are monotonic and never reused, which is the one property genuinely
  helpful for versioning: a deleted ID never comes back as something else.

What a versioned model would require, and none of which exists: a version or
transaction counter attached to each record; retention of superseded versions
through compaction; a `validFrom`/`validTo` interval per version; and an
as-of read path that resolves a version rather than the current record.

#### Timeline Replay

**Does not exist as a feature**, though the *mechanism* is closer than it looks.
`WAL.Replay(ReplayCallbacks)` already walks the log in order and dispatches
per-record-type; batch markers already delimit transaction boundaries. What is
missing:

- No timestamps in any WAL record — records carry type, length, payload, CRC and
  nothing else. **Every temporal capability downstream depends on fixing this
  first**, and it is a format change (v-next, additive: a timestamp field in the
  batch-commit payload costs 8 bytes per *transaction*, not per record).
- No replay-to-point (by time, by sequence, or by transaction).
- No log retention — truncation at compaction destroys the timeline.
- No public API: `Replay` is unexported to consumers and callback-shaped, not
  iterator-shaped.

#### Temporal Query Primitives

What exists is **conventional, not enforced**:

- `EdgeTypeTemporal` (value 4) — a label meaning "time-ordered relation" with no
  engine semantics whatsoever.
- `encoding.Time(t)` → order-preserving `[]byte`, plus `DecodeTime`. Combined
  with `DeclareOrderedProperty`, this gives binary-search range queries over a
  caller-registered timestamp property. That is the entire temporal story, and
  `docs/comparison.md:93` accurately advertises it as the "temporal / range
  index".

Missing: as-of queries (`GetNodeAsOf(id, t)`), time-travel traversal (BFS over
the graph as it stood at T), interval overlap/containment predicates, validity
intervals, and event-ordering guarantees.

#### Subgraph Diffing

**Does not exist.** No primitive compares two graphs, two subgraphs, or two
states of one subgraph. `InducedSubgraph` extracts a node set and the edges
between them; nothing compares two such extractions.

A diff API is, notably, the one item in this module that is buildable **today**
against current-state data with no format change — `DiffSubgraphs(a, b []NodeID)`
returning added/removed/changed nodes and edges is pure computation over two
`InducedSubgraph` results. It is worth doing early precisely because it delivers
user-visible value without waiting for the format work.

#### Missing Features

Effectively the entire module:

1. Node/edge versioning — 🔴 **TODO**
2. History retention across compaction — 🔴 **TODO**
3. WAL record timestamps — 🔴 **TODO** *(prerequisite for 4, 5, 6)*
4. Timeline replay to a point in time — 🔴 **TODO**
5. As-of / time-travel reads — 🔴 **TODO**
6. Time-travel traversal — 🔴 **TODO**
7. Validity intervals with overlap predicates — 🔴 **TODO**
8. Subgraph diffing — 🔴 **TODO** *(buildable now, no format change)*
9. Audit trail / who-changed-what — 🔴 **TODO**
10. Bitemporality (valid time vs transaction time) — 🔴 **TODO**, likely out of scope

#### Roadmap

Staged so that each phase is independently useful and the format change happens
once.

**Phase A — value without format change**

| Item | Notes |
| --- | --- |
| `DiffSubgraphs(a, b []NodeID) SubgraphDiff` | added/removed/changed nodes and edges, label and property deltas |
| `DiffAgainst(other *Graph, scope []NodeID)` | cross-store comparison, e.g. two case snapshots |
| Persist ordered-key declarations (§3.3 T1) | makes `encoding.Time` range queries survive restart — the immediate practical temporal win |

**Phase B — the format decision**

| Item | Notes |
| --- | --- |
| Add a monotonic **commit sequence number** and a **wall-clock timestamp** to the WAL batch-commit record | 16 bytes per transaction; additive; older readers already reject unknown types, so the version discipline holds |
| Decide the retention model | the fork in the road: (a) *log-retention* — keep segments past checkpoint, cheap to build, unbounded growth, replay-only access; (b) *versioned records* — keep superseded versions in the CSR with validity intervals, expensive, but queryable directly. **Recommendation: (a) first**, since it makes replay and audit possible with no read-path change, and (b) only if as-of queries prove necessary |
| WAL segmentation + retention policy | prerequisite for (a): `Truncate` must become "retire segments older than R" |

**Phase C — temporal queries** *(gated on B)*

| Item |
| --- |
| `ReplayTo(seq)` / `ReplayTo(time)` reconstructing a store state into a fresh `memory.Store` |
| Audit trail: enumerate changes to an ID over a sequence range |
| As-of point reads |
| Time-travel traversal (BFS over a reconstructed state) |
| Validity intervals + overlap/containment filter operators |

#### Status

🔴 **TODO** — a `Temporal` label, an `encoding.Time` codec, and a `ProvenanceChain`
traversal are all that exist. The name of this module currently describes an
intention, not an implementation. Phase A is buildable immediately; Phase B is
the decision that unblocks everything else.

---

### 3.6 Visualization Layer

#### Overview

`viz/` is a single-file, single-purpose exporter: given `[]*store.Node` and
`[]*store.Edge`, write a **self-contained** interactive HTML page. No server, no
CDN, no runtime dependency — the CSS and JS are a Go string constant
(`pageTemplate`) and the data is inlined as JSON.

#### Current Capabilities

Public API is two functions:

```go
func ExportInteractiveHTML(nodes, edges, outPath) error
func ExportInteractiveHTMLWithOptions(nodes, edges, outPath, opts ExportOptions) error
type ExportOptions struct { Title, Subtitle string }
```

Server-side (`viz/exporter.go:78-206`): builds `nodeVM`/`edgeVM` view models with
label names, colour, a **fixed circular layout** computed in Go (1500×980
viewBox, radius 360, `angle = 2πi/n`), property previews in three encodings
(text/base64/hex), and a pre-built `searchText` field per node. Edges whose
endpoints are absent from the node set are dropped. Edge types are collected and
sorted for the filter UI.

Client-side, read from the embedded template: SVG rendering with a canvas overlay
for edge labels, a canvas **mini-map**, zoom/pan, hover previews, click-through
node/edge detail cards, edge-type filtering, search, lazy rendering, and
**Download SVG**. There is *no* force simulation — positions come entirely from
the server-computed circle.

Four generated sample pages are committed at the repository root
(`viz_case_map.html`, `viz_exploration_demo.html`, `viz_pattern_scope.html`,
`viz_similarity_mesh.html`; ~50–70 KB each, ~220 KB total). They are build
artefacts checked into version control.

#### Layered Views

**Not implemented.** All nodes render on one circle in one plane. Node type
drives colour only — there is no grouping, no clustering, no per-layer show/hide,
no hierarchical containment (e.g. Case → EvidenceFile → MicroArtefact as nested
regions), and no semantic-zoom level-of-detail.

Plan: (i) group nodes by primary label into concentric rings or vertical bands
computed server-side; (ii) per-layer visibility toggles in the existing filter
panel, which already has the UI pattern for edge types; (iii) collapse/expand a
layer into an aggregate node with a member count.

#### Time Slider

**Not implemented, and cannot be implemented today** — it requires temporal data
this engine does not retain (§3.5). This is the clearest instance of a
visualization feature blocked by a storage-layer gap rather than by
visualization work.

Once §3.5 Phase B lands: emit a per-entity `[appeared, disappeared]` sequence
range into the view model, add a range input bound to it, and filter client-side.
The client work is small; the data is the whole problem.

#### Stable Layout Metadata

**Not implemented.** Positions are recomputed on every export, and because the
angle is `2πi/n` over the *input slice order*, adding one node reshuffles every
position. Two exports of a barely-changed graph are visually unrelated — which
defeats the analyst's core need to recognise a graph they have seen before.

Plan:

1. Make layout a function of node **ID** rather than slice index — an immediate,
   near-free improvement in stability.
2. `ExportOptions.Layout map[store.NodeID]struct{X, Y float64}` so a caller can
   supply prior positions.
3. Emit the computed layout as a sidecar JSON (or an embedded block the page can
   re-export) so it round-trips.
4. Optionally persist layout hints as reserved node properties, keeping the
   engine unaware of their meaning.

#### Filtering & Highlighting

Partially present:

- **Present**: edge-type filter, node search over `searchText` (ID + labels +
  property preview), hover highlight, click-to-inspect detail cards.
- **Absent**: node-type filter, property-value filter, degree/weight thresholds,
  path highlighting (given a `PathResult`), pattern-match highlighting (given
  `SubgraphMatch`), neighbourhood focus/isolate, multi-select.

The gap worth closing first is **result highlighting**: the engine produces
`PathResult`, `BFSResult`, and `[]SubgraphMatch`, and the exporter has no way to
accept any of them. An analyst who has just run a pattern match cannot see the
match.

#### Missing Features

1. No export of engine result types (`BFSResult`, `PathResult`, `SubgraphMatch`)
   — the exporter takes only bare node/edge slices.
2. No layout stability, no layout injection, no force/hierarchical layout.
3. No layered or hierarchical views.
4. No time slider (blocked on §3.5).
5. No node-type or property filtering.
6. No non-HTML formats — no GraphML, GEXF, DOT, JSON-LD, CSV. This blocks Gephi,
   Cytoscape, and every other analysis tool.
7. No scale guidance or limit — everything is inlined, so a 100 k-node export
   produces an enormous page with no warning. **No documented practical limit.**
   🟣 **NEEDS INVESTIGATION**
8. No streaming export; the whole page is built in memory.
9. Committed sample HTML artefacts are versioned build output.

#### Roadmap

| Phase | Item |
| --- | --- |
| **P0** | Accept engine result types: `ExportBFSResult`, `ExportPath`, `ExportMatches` — with the path/match highlighted. Small, additive, immediately useful |
| **P0** | ID-derived layout (stability step 1) + `ExportOptions.Layout` injection |
| P1 | Node-type filter and property-value filter in the existing filter panel |
| P1 | **Graph interchange formats** — GraphML and DOT first; this is a small exporter each and it unlocks the entire external tooling ecosystem |
| P1 | Establish and document a node-count limit; warn or refuse above it |
| P2 | Layered / hierarchical views with collapse-expand |
| P2 | Layout round-tripping via sidecar JSON |
| P2 | Force-directed or hierarchical layout server-side (keeps the page dependency-free) |
| P3 | Time slider (**gated on §3.5 Phase B**) |
| P3 | Streaming export for large graphs |

#### Status

🟡 **IN PROGRESS** — what exists is well built and genuinely usable: self-contained,
dependency-free, with mini-map, search, filter, and SVG download. It is also a
*static snapshot viewer*, not an analysis surface: it cannot render a query
result, cannot keep a layout stable, cannot layer, and cannot show time.

---

### 3.7 Developer Experience

#### Overview

DX here means everything around the engine: how it is built, tested, measured,
debugged, inspected, and learned. The **documentation and measurement discipline
are exceptional**; the **tooling is nearly absent**.

#### CLI Organization

**There is no CLI.** No `cmd/` directory, no `main` package other than
`examples/` (which is a demonstration program, not a tool). Consequences:

- A `graphene.csr` or `graphene.wal` on disk cannot be inspected without writing
  Go.
- There is no way to compact, verify, or rebuild a store from a shell.
- Nothing can be scripted, and nothing is usable in an incident.

Proposed shape — a single `cmd/graphene` binary with subcommands, deliberately
**read-only by default** so it is safe to point at a live store:

| Subcommand | Behaviour |
| --- | --- |
| `graphene info <dir>` | CSR version, node/edge counts, sequence high-water marks, WAL size, delta size, property-index entry counts |
| `graphene verify <dir>` | run `VerifyIndexes`, report the first inconsistency |
| `graphene wal <dir>` | dump WAL records: type, length, CRC validity, batch boundaries |
| `graphene csr <dir>` | dump CSR header and record summaries; `--node`/`--edge` for a single record |
| `graphene query <dir> …` | run a `NodeQuery` from flags or a JSON file and print the `QueryPlan` |
| `graphene explain <dir> …` | plan only, no execution |
| `graphene export <dir>` | drive `viz` from the shell |
| `graphene compact <dir>` | the one **write** subcommand — behind an explicit flag |

#### Debugging Tools

Present:

- `ExplainNodeQuery` / `ExplainEdgeQuery` → `QueryPlan` with driver, driver key,
  candidate count, per-residual method and cost, result count, and a one-line
  `String()`. This is a real, well-designed diagnostic.
- `VerifyIndexes()` — structural self-check across every index.
- `RebuildIndexes()` — structural repair.
- `test.ps1 -Bench` with `-benchmem`, plus a documented profiling workflow.

Absent:

- No tracing or structured logging anywhere in the engine — not even an optional
  `Logger` hook. Nothing reports a slow query, a compaction, or a replay.
- No metrics (counters, histograms) for operations, cache behaviour, or lock
  contention.
- No `-race`-independent deadlock or lock-contention diagnostics.
- No dump of the delta layer or the property index.
- No way to reproduce a query plan decision from a saved store state.

#### Read-only Inspection Tools

Nothing exists. Every inspection path today requires compiling a Go program
against the module. Since `Open` takes an exclusive-ish handle on the WAL,
inspecting a store also means contending with whatever process owns it — a
read-only inspector that parses `graphene.csr` **without opening the store** is
therefore the more valuable half of the CLI above, and is entirely feasible: the
format is documented and self-describing.

#### Module Isolation

Mixed, and this is the sharpest DX problem.

**Good.** Package boundaries are clean and the dependency graph is acyclic
(§4). `traversal/` and `viz/` depend only on `store/`. `index/` depends only on
`store/`. `index/encoding` depends on nothing but the standard library. There is
no cyclic import and no backend leakage into the contract.

**Bad.** `disk/store.go` is **107 KB / ~3 500 lines / 130+ top-level
declarations**, containing at minimum:

- the `Store` type, its lifecycle, and its lock discipline
- the query planner and driver-selection cost model (`driveNodeCandidates`,
  `driveEdgeCandidates`, `labelDriverWins`, ~280 lines)
- node/edge/property marshalling and unmarshalling (four record kinds)
- CSR deserialisation and index-section parsing (`deserialiseCSR`,
  `readCSRIndexSection`, ~230 lines)
- batch read paths, degree computation, compaction, WAL replay wiring,
  `VerifyIndexes`, `RebuildIndexes`

Every one of those is separable along lines the file *already* implies. Splitting
it is mechanical, and `memory/store.go` (48 KB) has the same shape at smaller
scale.

**Also bad.** The two backends duplicate substantial logic — `sortDedupeNodeIDs`,
`sortDedupeEdgeIDs`, `makeNodeIDSet`, `nodeHasAnyType`, `edgeHasAnyType`,
`removeEdgeID`, `matchNodeIDsByFilters`, `matchOneNodeFilter`,
`driveNodeCandidates`, `liveNodeIDs`, `incidentEdgeIDs`, `degreeSumLocked` all
appear in both with near-identical bodies. `store/query_types.go` shows the
pattern for fixing this — `SortIDsForOrder`'s comment says explicitly that both
backends had an identical copy of that switch and *"it lives here so the four
cannot drift apart."* The same argument applies to a dozen more functions.
`CONTRIBUTING.md §"Keep the backends in step"` names the hazard; the parity tests
catch drift after the fact rather than preventing it.

#### Missing Features

1. **No CI.** No `.github/`, no pipeline. Every guarantee — race detector, parity
   suites, integrity suites, format-vector tests — is enforced only by a human
   remembering to run `test.ps1`. **This is the single largest DX gap.**
2. **`test.ps1` is PowerShell-only.** No `Makefile`, no shell script, no
   `go generate` path. A Linux or macOS contributor has no documented entry point
   despite the engine being pure Go.
3. No CLI / inspection tooling (above).
4. No linter configuration (`golangci-lint`, `staticcheck`) and no `vet` step in
   the runner.
5. No coverage measurement or reporting.
6. No fuzz targets — despite the codebase having exactly the right shapes for
   them: WAL record parsing, CSR deserialisation, `ParseNodeType`/`ParseEdgeType`,
   and the property-filter comparison rules.
7. No benchmark result storage or regression gate; `CONTRIBUTING.md` mandates
   interleaved A/B measurement against a control but nothing automates or
   archives it.
8. No `CHANGELOG.md` despite seven tags.
9. No godoc examples (`Example*` functions) — the runnable examples live in
   `examples/main.go` as a program, so they are not exercised by `go test` and
   can silently rot.
10. No `SECURITY.md`, `LICENSE`, or issue/PR templates. 🟣 **NEEDS INVESTIGATION** — a `LICENSE` file
    is absent from the tree; whether that is intentional needs confirmation.

#### Roadmap

| Phase | Item | Rationale |
| --- | --- | --- |
| **P0** | **CI pipeline**: `go vet`, `go build`, `go test ./... -race`, stress-tagged suite on a schedule, on Linux + Windows | everything else in this table is worth less until the existing tests run automatically |
| **P0** | **Cross-platform task runner** (`Makefile` or a Go-based `task` command) with parity to `test.ps1` | removes the Windows-only barrier to contribution |
| **P0** | **Split `disk/store.go`** along the seams it already has: `store.go`, `planner.go`, `marshal.go`, `csr_io.go`, `verify.go`, `compact.go`. Mirror in `memory/` | pure file movement; no behaviour change; unblocks every subsequent change to the disk backend |
| P1 | **`cmd/graphene`** read-only inspector: `info`, `verify`, `wal`, `csr` | operational blindness is the sharpest day-two problem |
| P1 | **Lift duplicated backend logic** into `store/` or an internal shared package, following the `SortIDsForOrder` precedent | prevention beats parity-testing after the fact |
| P1 | **Fuzz targets** for WAL replay, CSR deserialisation, and type-selector parsing | these parse untrusted-shaped bytes and are exactly what fuzzing is for |
| P1 | Optional `Logger`/tracing hook on `Store` | |
| P2 | `golangci-lint` config + CI gate; coverage reporting |
| P2 | `cmd/graphene query`/`explain`/`export` |
| P2 | Godoc `Example*` functions compiled and run by `go test` |
| P2 | Benchmark archive + regression gate implementing `CONTRIBUTING.md`'s method |
| P3 | `CHANGELOG.md`, `LICENSE`, `SECURITY.md`, contribution templates |

#### Status

🟡 **IN PROGRESS** — documentation and measurement culture are genuinely first-rate and ahead
of most projects at this stage. Automation, tooling, and internal module size are
correspondingly behind: no CI, no CLI, no inspection path, and a 107 KB core
file.

---

## 4. Dependency Graph

### 4.1 Package dependencies (compile-time, acyclic)

```
                            index/encoding
                                  │ (stdlib only)
                                  ▼
   store  ◄──────────────────── index
     ▲  ▲  ▲                      ▲
     │  │  │                      │
     │  │  └──── traversal        │
     │  │            ▲            │
     │  └──── viz    │            │
     │               │            │
     ├──── memory ───┼────────────┤
     └──── disk  ────┴────────────┘
                     │
              graphene (root)
                     │
                 examples
```

Precisely:

| Package | Imports (internal) |
| --- | --- |
| `store` | — |
| `index/encoding` | — |
| `index` | `store` |
| `traversal` | `store` |
| `viz` | `store` |
| `memory` | `store`, `index` |
| `disk` | `store`, `index` |
| `graphene` (root) | `store`, `index`?, `memory`, `disk`, `traversal` |
| `examples` | `graphene`, `store`, `traversal`, `viz` |

`store` is the universal sink and depends on nothing. No cycles. `traversal` and
`viz` are backend-agnostic by construction — they cannot see `disk` or `memory`.

### 4.2 Runtime capability dependencies

`Graph` negotiates at runtime rather than at compile time. This is the second
dependency graph, and the one that actually determines behaviour:

```
Graph ──type-assert──► Transactor          → atomic Tx        else non-atomic replay
      ──type-assert──► BatchReader         → one lock hold    else per-ID loop
      ──type-assert──► AdjacencyReader     → buffered walk    else EdgesOf per node
      ──type-assert──► DegreeCounter       → O(1)/O(deg)      else len(EdgesOf(...))
      ──type-assert──► Syncer              → fsync            else no-op
      ──type-assert──► Reindexer           → policy + purge   else keep-stale
      ──type-assert──► OrderedIndexDeclarer→ range index      else scan
      ──type-assert──► IndexVerifier       → structural check else nil
      ──type-assert──► IndexRebuilder      → repair           else nil
      ──type-assert──► *QueryExplainer     → QueryPlan        else error
      ──type-assert──► *disk.Store         → Compact()        else no-op
```

**Every fallback is a silent behavioural downgrade** except the explainers (which
error) and `Tx.Atomic()` (which reports). That is a deliberate design choice, and
a hazard worth naming: see R-DEP-1.

### 4.3 Data-flow dependencies

```
write:   Graph.AddNode ─► disk.Store ─► marshal ─► WAL ring ─► drain ─► file
                                     └─► delta maps + delta label postings
                                     └─► PropertyIndex (on IndexNodeProperty)

commit:  Tx.Commit ─► resolveTransaction (validate against txView)
                   ─► walBatch (begin│records│commit) ─► one AppendBatch ─► [fsync]
                   ─► apply to delta / masks / propIdx

read:    Graph.GetNode ─► csrFastRead (lock-free when csrShadowed==0)
                       └─► else RLock ─► delta ─► masks ─► CSR

query:   QueryNodeIDs ─► driveNodeCandidates (ids│equality│ordered│labels│scan)
                      ─► NarrowNodesByFilters (residuals, probe vs set, per step)
                      ─► SortIDsForOrder ─► ApplyNodeQueryWindow

compact: Compact ─► collect(CSR∖overridden∖deleted, delta) ─► Build
                 ─► SerialiseWithIndex ─► tmp file ─► WAL Checkpoint
                 ─► rename ─► WAL Truncate ─► publishCSR ─► reset delta

open:    Open ─► loadCSR (deserialise + rebuild adjacency + label postings
                          + load GIDX property entries)
              ─► replayWAL (buffered, CRC-checked, batch-aware) ─► delta
```

### 4.4 Roadmap dependency ordering

Several planned items are gated. Building them out of order wastes work:

```
WAL timestamps + segmentation ──► timeline replay ──► as-of reads ──► time-travel traversal
                              └──► audit trail                    └──► viz time slider

deltaVersion counter ──► k-hop cache
                     └─► materialized views
                     └─► incremental index maintenance

Compact pass ──► connected-component labels ──► O(1) IsConnected

skew benchmark (H1) ──► per-type adjacency sub-ranges (H2) ──► supernode traversal work

CSR format bump ──► body checksum (v8)
               └──► persisted ordered-key declarations
               └──► persisted composite-index declarations
   ⇒ these three should land in ONE format change, not three

split disk/store.go ──► every subsequent disk-backend change
CI ──► every claim of correctness made by any subsequent change
```

The CSR format observation is worth stating plainly: **there are three separate
roadmap items that each require a CSR version bump.** They should be designed
together and shipped as one v8, not as v8/v9/v10.

---

## 5. Missing Features & Gaps

Consolidated, deduplicated, and prioritised across all modules. Priority reflects
*consequence*, not effort.

### 5.1 Critical — silent failure modes or operational blindness

| # | Gap | Module | Consequence |
| --- | --- | --- | --- |
| C1 | **No CI** | DX | every correctness guarantee depends on a human running a script |
| C2 | **No compaction policy** | Storage | delta and WAL grow without bound; open time and memory degrade until someone notices |
| C3 | **Ordered-key declarations not persisted** | Indexing | every restart silently converts range queries into scans; no error, no warning |
| C4 | **No CSR body checksum** | Storage | silent corruption passes `Open` |
| C5 | **No read-only inspection path** | DX | a suspect store cannot be examined without writing Go |
| C6 | **Whole-graph-in-memory, always** | Storage | the largest supported graph is the one that fits in RAM, undocumented |

### 5.2 High — functional holes in advertised territory

| # | Gap | Module |
| --- | --- | --- |
| H1 | Weighted shortest path — `Weight` is first-class and no algorithm reads it | Traversal |
| H2 | No versioning / history / audit trail | Provenance |
| H3 | No subgraph diffing (buildable today, no format change) | Provenance |
| H4 | No composite indexes | Indexing |
| H5 | Viz cannot render an engine result (`BFSResult`, `PathResult`, `SubgraphMatch`) | Viz |
| H6 | Viz layout is unstable across exports | Viz |
| H7 | No graph interchange formats (GraphML/DOT/GEXF) | Viz |
| H8 | `disk/store.go` at 107 KB resists isolated change | DX |
| H9 | Duplicated logic across the two backends | DX |
| H10 | No snapshot isolation / read transactions | Core |

### 5.3 Medium — performance and scale

| # | Gap | Module |
| --- | --- | --- |
| M1 | No high-degree-node strategy | Indexing |
| M2 | Delta adjacency deletion is O(degree) | Indexing |
| M3 | Pattern matching unoptimised (no selectivity ordering, no symmetry breaking) | Traversal |
| M4 | No k-hop caching | Traversal |
| M5 | No reachability index; `IsConnected` runs a full search | Traversal |
| M6 | Compaction is full-rebuild and buffers the whole image | Storage |
| M7 | Range selectivity estimated by total key entry count — no histograms | Indexing |
| M8 | Memory-backend read concurrency negative past one core | Core |
| M9 | No parallel traversal despite a lock-free read path | Traversal |
| M10 | `Weight` is unindexable | Indexing |

### 5.4 Low — completeness and polish

| # | Gap | Module |
| --- | --- | --- |
| L1 | No materialized views | Traversal |
| L2 | No layered viz views | Viz |
| L3 | No time slider (blocked on H2) | Viz |
| L4 | No graph algorithms (centrality, community, PageRank) | Traversal |
| L5 | No `CHANGELOG.md`; `LICENSE` absent | DX |
| L6 | No fuzz targets | DX |
| L7 | No godoc `Example*` functions | DX |
| L8 | Committed viz HTML build artefacts | Viz |
| L9 | No structured logging or metrics hook | DX |
| L10 | No context/cancellation | Core |

### 5.5 Documentation inaccuracies found during the audit

| Location | Issue |
| --- | --- |
| `graphene.go:9` | package doc advertises *"type index, temporal index, and property index"* — the temporal index was deleted in `ac5a458` and the type index was absorbed into the backends |
| `disk/store.go:121` | `Open` doc says the CSR *"is memory-mapped"*; it is read and parsed into the heap |
| `helpers.go:42-43,76-77` | `GetNodes`/`GetEdges` carry a stale first line (*"If any ID is not found the error is returned immediately"*) directly above the corrected doc that says the opposite |
| `README.md:9` | "Project Status: experimental, pre-production" understates a tree with transactions, dual-backend parity, a costed planner, and 380 tests |

These are cheap to fix and are the kind of drift that erodes trust in otherwise
excellent documentation.

---

## 6. Proposed Roadmap

Five phases. Each is independently shippable, and the ordering respects §4.4.

### Phase 0 — Foundation *(unblocks everything; no new features)*

| Item | Module | Why first |
| --- | --- | --- |
| CI pipeline (vet, build, `test -race`, scheduled stress) on Linux + Windows | DX | every later claim of correctness depends on it |
| Cross-platform task runner with `test.ps1` parity | DX | removes the Windows-only contribution barrier |
| Split `disk/store.go` (and `memory/store.go`) along existing seams | DX | mechanical, behaviour-preserving, unblocks every later disk change |
| Fix the four documentation inaccuracies in §5.5 | Docs | trivial; prevents propagating wrong statements |
| Extend `GraphStats`: delta size, WAL bytes, per-label counts, time since compact | Core | the observability input Phase 1 needs |

**Exit criterion:** a green pipeline on both platforms, no file over ~40 KB in
`disk/`, and `Stats()` reporting delta and WAL size.

### Phase 1 — Operational safety *(close the silent failure modes)*

| Item | Module |
| --- | --- |
| Compaction policy: `ShouldCompact()` advisory + opt-in threshold-triggered background compaction | Storage |
| **CSR format v8, one bump carrying: body checksum, persisted ordered-key declarations, reserved space for composite declarations** | Storage + Indexing |
| Auto re-declare ordered keys at `Open` | Indexing |
| `cmd/graphene` read-only inspector: `info`, `verify`, `wal`, `csr` | DX |
| Sorted delta adjacency via the existing sorted-ID algebra | Indexing |
| Fuzz targets: WAL replay, CSR deserialisation, type-selector parsing | DX |
| Document the practical graph-size ceiling and memory model | Docs |

**Exit criterion:** no silent failure mode from §5.1 remains; a store can be
inspected from a shell without opening it.

### Phase 2 — Functional completeness *(fill the advertised holes)*

| Item | Module |
| --- | --- |
| Weighted shortest path (Dijkstra + optional A*) | Traversal |
| `DiffSubgraphs` / `DiffAgainst` | Provenance |
| Viz: export `BFSResult` / `PathResult` / `SubgraphMatch` with highlighting | Viz |
| Viz: ID-derived stable layout + `ExportOptions.Layout` injection | Viz |
| Viz: GraphML and DOT exporters | Viz |
| Bidirectional-BFS frontier balancing | Traversal |
| Connected-component labelling during `Compact`; O(1) `IsConnected` | Traversal |
| `deltaVersion` counter + post-commit hook | Core |
| Lift duplicated backend logic into shared code | DX |

**Exit criterion:** every capability the docs and comparison table claim is
actually reachable through the API, and the visualization can show what a query
found.

### Phase 3 — Temporal foundation *(the format decision)*

| Item | Module |
| --- | --- |
| Commit sequence number + wall-clock timestamp in the WAL batch-commit record | Storage |
| WAL segmentation + retention policy (replacing unconditional truncate) | Storage |
| `ReplayTo(seq)` / `ReplayTo(time)` reconstructing state into a `memory.Store` | Provenance |
| Audit trail: enumerate an entity's changes over a sequence range | Provenance |
| Composite index: declaration, structure, reverse refs, planner driver | Indexing |
| Range-selectivity histograms per declared key | Indexing |

**Exit criterion:** the engine can answer "what changed, and when" for any entity
within the retention window.

### Phase 4 — Scale and depth

| Item | Module |
| --- | --- |
| Streaming CSR serialisation (bounded compaction memory) | Storage |
| Incremental / partial compaction | Storage |
| mmap-backed CSR with an explicit aliasing contract | Storage |
| High-degree strategy: skew benchmark → per-type adjacency sub-ranges | Indexing |
| Pattern-matching optimisation (selectivity ordering, symmetry breaking) | Traversal |
| k-hop neighbourhood cache keyed on `(csrPtr, deltaVersion)` | Traversal |
| Parallel BFS | Traversal |
| As-of reads and time-travel traversal | Provenance |
| Viz: time slider, layered views, force/hierarchical layout | Viz |
| Materialized views | Traversal |
| Read snapshots / snapshot isolation | Core |

### Phase 5 — Ecosystem

Benchmark archive and regression gate; `golangci-lint` and coverage gates; full
`cmd/graphene` (`query`, `explain`, `export`, `compact`); godoc examples;
`CHANGELOG.md`, `LICENSE`, `SECURITY.md`; graph algorithm library (centrality,
community detection); backup/restore primitive.

---

## 7. Risk Analysis

Scored **Likelihood × Impact**, each Low/Medium/High.

### 7.1 Correctness risks

| ID | Risk | L | I | Analysis & mitigation |
| --- | --- | --- | --- | --- |
| R-C1 | **Comparison-semantics divergence.** Declared keys compare byte-wise; undeclared keys use the numeric-then-bytes rule. Declaring a key *changes query results* | Med | **High** | Deeply documented in three places and enforced by `postingsMatch`'s `ordered` flag. Residual risk is a caller who declares a key mid-life and sees answers change. **Mitigation:** an `ExplainNodeQuery` field reporting which comparison rule each filter used |
| R-C2 | **Stale property index after `UpdateNode`.** Default `ReindexKeep` leaves entries pointing at values the entity no longer has, and the planner trusts them | **High** | **High** | The most likely real-world wrong-answer path. `UpdateNodeIndexed` exists and is the correct API; nothing steers callers to it. **Mitigation:** make the indexed variant the documented default in every example; consider a debug-mode warning when `UpdateNode` touches an entity with index entries |
| R-C3 | **CSR body corruption undetected.** Magic and version are checked; content is not | Low | **High** | Phase 1 checksum (C4) |
| R-C4 | **Non-atomic `Tx` fallback** on third-party stores | Low | High | `Atomic()` reports it; nothing enforces checking. **Mitigation:** consider making the fallback opt-in |
| R-C5 | **Lock-free read path subtlety.** Correctness rests on: CSR immutability once published, the shadow counter's monotonicity within an epoch, and the pointer re-check | Low | **High** | Reasoned through in `TECHNICAL_DETAILS §9.2` and covered by tests. `CONTRIBUTING.md` notes the race detector does **not** find stale reads — so this cannot be delegated to tooling. **Mitigation:** treat any change here as requiring a written argument, not just a green test run |
| R-C6 | **WAL forward-incompatibility.** A newer build's WAL is unreadable by an older one, by design | Med | Med | Intentional and correct. Needs to be a documented *downgrade* procedure (compact before downgrading), which does not exist yet |

### 7.2 Operational risks

| ID | Risk | L | I | Analysis & mitigation |
| --- | --- | --- | --- | --- |
| R-O1 | **Unbounded delta/WAL growth.** Nothing triggers compaction | **High** | **High** | Degrades open time, memory, and read speed with no signal. Phase 1 policy work is the fix. **Highest-likelihood operational failure in the system** |
| R-O2 | **Compaction memory spike** — whole image buffered as one `[]byte` | Med | High | ~2× graph size at the worst moment. Phase 4 streaming serialisation |
| R-O3 | **Whole-graph resident memory** with no documented ceiling | **High** | Med | Users discover the limit by hitting it. Documenting it is nearly free and should not wait for mmap |
| R-O4 | **Visibility ≠ durability.** A read-back write is not safe | Med | High | Documented thoroughly. **Mitigation:** make `syncOnCommit` and the durability boundary prominent in the quick-start, not only in §11 of the technical doc |
| R-O5 | **No inspection path for a suspect store** | Med | High | Phase 1 CLI |
| R-O6 | **Ordered declarations lost on restart** — a pure performance cliff with no symptom | **High** | Med | Phase 1 persistence |

### 7.3 Maintainability risks

| ID | Risk | L | I | Analysis & mitigation |
| --- | --- | --- | --- | --- |
| R-M1 | **`disk/store.go` at 107 KB.** Every disk change touches a file no one can hold in their head | **High** | Med | Phase 0 split. Mechanical, low-risk, high-leverage |
| R-M2 | **Backend duplication drifts.** Parity tests catch it *after* it happens | **High** | Med | Phase 2 lifting shared logic. The `SortIDsForOrder` comment already documents the pattern |
| R-M3 | **No CI.** A contributor can merge a change that fails the race detector | **High** | High | Phase 0. Compounds every other risk here |
| R-M4 | **Docs are 240 KB and drift is already visible** (§5.5) | **High** | Low | `CONTRIBUTING.md §4` already defines mechanical doc checks; they need to run in CI |
| R-M5 | **Benchmarks are the design authority but are unarchived.** `CONTRIBUTING.md` mandates interleaved A/B against a control; nothing stores results, so a slow regression across many commits is invisible | Med | Med | Phase 5 archive + gate |

### 7.4 Strategic risks

| ID | Risk | L | I | Analysis |
| --- | --- | --- | --- | --- |
| R-S1 | **Temporal is the domain's core question and the engine's largest gap** | **High** | **High** | Forensic provenance is *about* time. Phase 3 exists for this, and the retention-model choice (log-retention vs versioned records) should be made deliberately and early — it is hard to reverse |
| R-S2 | **No query language limits adoption** beyond Go callers | Med | Med | Deliberate; a Cypher-subset front-end over `NodeQuery` is feasible later precisely because the planner is already struct-driven |
| R-S3 | **Single-writer WAL bounds write scaling**, documented | Med | Med | Inherent to one append point; sharded WALs would be a major redesign |
| R-S4 | **Scope creep across seven module fronts** | Med | High | The roadmap's phase gating is the mitigation; Phase 0 deliberately ships no features |
| R-S5 | **No `LICENSE` file in the tree** | — | High | 🟣 **NEEDS INVESTIGATION** — needs confirmation; blocks external adoption entirely if unintentional |

### 7.5 Dependency risks

| ID | Risk | L | I | Analysis |
| --- | --- | --- | --- | --- |
| R-DEP-1 | **Silent capability downgrade.** Nine of eleven capability fallbacks change behaviour without telling anyone | Med | Med | A `Graph.Capabilities()` reporting which interfaces the backend satisfies would make the negotiation inspectable — cheap, and useful in the CLI's `info` output too |
| R-DEP-2 | **Zero external dependencies** | Low | Low | A genuine asset: no supply chain, no version conflicts, trivial vendoring. Worth protecting as a stated constraint |
| R-DEP-3 | **`go 1.26`** is a recent toolchain floor | Low | Low | Uses `slices`, `clear`, generics with `~uint64`. Fine, but worth stating as a support policy |

---

## 8. Open Questions

Ordered by how much they gate other decisions.

### Q1 — What is the temporal retention model? *(gates Phase 3 entirely)*

Log-retention (keep WAL segments past checkpoint; cheap, unbounded, replay-only
access) or versioned records (superseded versions kept in the CSR with validity
intervals; expensive, directly queryable)? This is the least reversible decision
in the roadmap. **Working assumption: log-retention first.**

### Q2 — What is the intended maximum graph size?

Everything is resident. Is the target 10⁶ nodes (fits comfortably), 10⁸ (needs
mmap), or 10⁹ (needs a different architecture)? `graphene_stress_test.go` has an
optional 1 M-node path, which suggests 10⁶–10⁷. **The answer determines whether
mmap and lazy loading are Phase 4 items or existential ones.** 🟣 **NEEDS INVESTIGATION**

### Q3 — Is compaction the caller's responsibility or the engine's?

Today it is entirely the caller's, and nothing tells them when. Automatic
background compaction changes the engine's concurrency and latency profile.
Advisory-only is safer and less useful. **Recommended: advisory in Phase 1,
opt-in automatic behind a config flag.**

### Q4 — Should `disk` and `memory` share an implementation core?

The duplication is extensive and the parity suites exist because of it. A shared
`internal/engine` holding the planner, filter matching, and ID algebra would
eliminate the drift class — at the cost of coupling the backends' evolution.
**Recommended: extract the pure functions (planner, matching, ID algebra) and
leave storage-specific paths separate.**

### Q5 — Who consumes the visualization?

If it is analysts inside a workflow, layered views and stable layout matter most.
If it is a demonstration surface, interchange formats matter more (let Gephi do
the work). The roadmap currently hedges by doing a little of both.

### Q6 — Is `LICENSE` intentionally absent? 🟣 **NEEDS INVESTIGATION**

No licence file exists in the tree. Needs confirmation before any external
distribution.

### Q7 — Should the sample `viz_*.html` artefacts stay in version control?

~220 KB of generated output at the repository root, regenerable from
`examples/`. Removing them is easy; they may be serving as README illustrations.
🟣 **NEEDS INVESTIGATION**

### Q8 — What is the supported downgrade path?

WAL forward-incompatibility means a newer build's log cannot be read by an older
one. Presumably "compact, then downgrade" — but that is neither documented nor
enforced.

### Q9 — Does the ordered-index declaration belong in the API or in the data?

Persisting declarations (Phase 1) makes the store self-describing but means a
declaration is a durable schema decision rather than a runtime hint. That is
probably right, and it is a change in character worth naming.

### Q10 — Is a query language in scope at all?

`TECHNICAL_DETAILS §16.1` lists "no query language" as a known limitation, which
implies eventual intent. The struct-driven planner makes a Cypher-subset
front-end tractable. If it is genuinely out of scope, saying so would settle a
recurring question.

---

## 9. Status Dashboard

### 9.1 Module status

| Module | Status | One-line assessment |
| --- | --- | --- |
| Core Engine | 🟢 **DONE** | CRUD, transactions, capability negotiation, lifecycle — complete and parity-tested |
| Storage Layer | 🟡 **IN PROGRESS** | WAL and CSR are excellent; operations (policy, checksum, bounds) are missing |
| Indexing | 🟡 **IN PROGRESS** | Property/ordered/label indexes strong; composite and temporal absent; declarations not persisted |
| Traversal & Algorithms | 🟡 **IN PROGRESS** | Implemented walks are sharp; weighted paths, caching, reachability, views absent |
| Provenance & Temporal | 🔴 **TODO** | A label, a codec, and one traversal. The module name describes an intention |
| Visualization | 🟡 **IN PROGRESS** | Good static snapshot viewer; cannot render results, keep layout, layer, or show time |
| Developer Experience | 🟡 **IN PROGRESS** | First-rate docs and measurement culture; no CI, no CLI, 107 KB core file |

### 9.2 Feature-level status

**Core Engine**

| Feature | Status |
| --- | --- |
| Node/Edge CRUD | 🟢 **DONE** |
| Multi-label entities | 🟢 **DONE** |
| Custom label ranges + selector parsing | 🟢 **DONE** |
| Cascade delete | 🟢 **DONE** |
| Transactions (`Begin`/`Commit`/`Rollback`) | 🟢 **DONE** |
| Batch read / batch write | 🟢 **DONE** |
| Capability negotiation | 🟢 **DONE** |
| Graceful shutdown on signal | 🟢 **DONE** |
| Query plans (`Explain*`) | 🟢 **DONE** |
| Extended statistics | 🔴 **TODO** |
| Context / cancellation | 🔴 **TODO** |
| Snapshot isolation / read transactions | 🔴 **TODO** |

**Storage Layer**

| Feature | Status |
| --- | --- |
| WAL framing + CRC32 + batch markers | 🟢 **DONE** |
| Lock-free ring-buffer write path | 🟢 **DONE** |
| Buffered, batch-aware replay | 🟢 **DONE** |
| CSR v7 format (property index in-file) | 🟢 **DONE** |
| Crash-safe compaction (tmp + rename) | 🟢 **DONE** |
| Lock-free CSR read path | 🟢 **DONE** |
| `VerifyIndexes` / `RebuildIndexes` | 🟢 **DONE** |
| Compaction policy / trigger | 🔴 **TODO** |
| CSR body checksum | 🔴 **TODO** |
| Streaming serialisation | 🔴 **TODO** |
| WAL rotation / size cap | 🔴 **TODO** |
| Incremental compaction | 🔴 **TODO** |
| mmap / lazy loading | 🔴 **TODO** |
| Backup / snapshot / restore | 🔴 **TODO** |
| `memory.Store` dump/load | 🔴 **TODO** |

**Indexing**

| Feature | Status |
| --- | --- |
| Primary + adjacency + label indexes | 🟢 **DONE** |
| Sharded property index (16 shards, sorted postings, reverse map) | 🟢 **DONE** |
| Key interning | 🟢 **DONE** |
| Ordered range index (opt-in per key) | 🟢 **DONE** |
| `index/encoding` order-preserving codecs | 🟢 **DONE** |
| Residual planner (probe vs set, per-step re-decision) | 🟢 **DONE** |
| Driver selection with cost model | 🟢 **DONE** |
| Persisted ordered-key declarations | 🔴 **TODO** |
| Composite indexes | 🔴 **TODO** |
| Range-selectivity histograms | 🔴 **TODO** |
| Indexable `Weight` | 🔴 **TODO** |
| High-degree strategy | 🔴 **TODO** |
| Sorted delta adjacency | 🟡 **IN PROGRESS** |
| Temporal index | 🔴 **TODO** *(deleted in `ac5a458`)* |

**Traversal & Algorithms**

| Feature | Status |
| --- | --- |
| BFS (records) + BFSIDs (allocation-light) | 🟢 **DONE** |
| DFS | 🟢 **DONE** |
| Provenance chain (inbound, cycle-safe) | 🟢 **DONE** |
| Bidirectional BFS shortest path | 🟢 **DONE** |
| VF2-inspired pattern matching | 🟡 **IN PROGRESS** *(works; documented as unoptimised)* |
| Cycle detection, induced subgraph | 🟢 **DONE** |
| Walker (shared, buffer-reusing) | 🟢 **DONE** |
| Weighted shortest path | 🔴 **TODO** |
| Frontier balancing | 🔴 **TODO** |
| k-hop caching | 🔴 **TODO** |
| Reachability index | 🔴 **TODO** |
| Materialized views | 🔴 **TODO** |
| Parallel traversal | 🔴 **TODO** |
| Graph algorithm library | 🔴 **TODO** |

**Provenance & Temporal**

| Feature | Status |
| --- | --- |
| `ProvenanceChain` traversal | 🟢 **DONE** |
| `EdgeTypeTemporal` label | 🟢 **DONE** *(convention only, no semantics)* |
| `encoding.Time` + ordered range queries | 🟢 **DONE** |
| Node/edge versioning | 🔴 **TODO** |
| History retention past compaction | 🔴 **TODO** |
| WAL timestamps | 🔴 **TODO** |
| Timeline replay | 🔴 **TODO** |
| As-of reads / time-travel traversal | 🔴 **TODO** |
| Validity intervals | 🔴 **TODO** |
| Subgraph diffing | 🔴 **TODO** |
| Audit trail | 🔴 **TODO** |

**Visualization**

| Feature | Status |
| --- | --- |
| Self-contained HTML export | 🟢 **DONE** |
| SVG + canvas overlay, mini-map, zoom/pan | 🟢 **DONE** |
| Hover previews, detail cards | 🟢 **DONE** |
| Edge-type filter, node search | 🟢 **DONE** |
| Download SVG | 🟢 **DONE** |
| Property preview (text/base64/hex) | 🟢 **DONE** |
| Result-type export with highlighting | 🔴 **TODO** |
| Stable layout / layout injection | 🔴 **TODO** |
| Layered / hierarchical views | 🔴 **TODO** |
| Time slider | 🔴 **TODO** *(blocked on Provenance)* |
| Node-type / property filters | 🔴 **TODO** |
| Interchange formats (GraphML/DOT/GEXF) | 🔴 **TODO** |
| Documented scale limit | 🟣 **NEEDS INVESTIGATION** |
| Force / hierarchical layout | 🔴 **TODO** |

**Developer Experience**

| Feature | Status |
| --- | --- |
| `CONTRIBUTING.md` measurement methodology | 🟢 **DONE** |
| 240 KB of technical documentation | 🟢 **DONE** |
| `test.ps1` runner (race / stress / bench) | 🟡 **IN PROGRESS** *(PowerShell only)* |
| ~380 tests incl. parity, integrity, format vectors | 🟢 **DONE** |
| Stress suite behind a build tag | 🟢 **DONE** |
| `ExplainNodeQuery` diagnostics | 🟢 **DONE** |
| CI pipeline | 🔴 **TODO** |
| Cross-platform task runner | 🔴 **TODO** |
| `cmd/graphene` CLI | 🔴 **TODO** |
| Read-only store inspector | 🔴 **TODO** |
| Linter config + coverage gate | 🔴 **TODO** |
| Fuzz targets | 🔴 **TODO** |
| Benchmark archive / regression gate | 🔴 **TODO** |
| `disk/store.go` decomposition | 🔴 **TODO** |
| Shared backend core | 🔴 **TODO** |
| `CHANGELOG.md` | 🔴 **TODO** |
| `LICENSE` | 🟣 **NEEDS INVESTIGATION** |

### 9.3 Phase status

| Phase | Theme | Status |
| --- | --- | --- |
| Phase 0 | Foundation — CI, task runner, file split, doc fixes, stats | 🔴 **TODO** |
| Phase 1 | Operational safety — compaction policy, CSR v8, CLI inspector, fuzzing | 🔴 **TODO** |
| Phase 2 | Functional completeness — weighted paths, diffing, viz results, shared core | 🔴 **TODO** |
| Phase 3 | Temporal foundation — WAL timestamps, retention, replay, composite index | 🔴 **TODO** |
| Phase 4 | Scale & depth — streaming/incremental compaction, mmap, caching, as-of | 🔴 **TODO** |
| Phase 5 | Ecosystem — gates, full CLI, licence/changelog, algorithm library | 🔴 **TODO** |

### 9.4 Risk register summary

| Severity | IDs |
| --- | --- |
| **Critical** (High × High) | R-C2 stale property index · R-O1 unbounded delta growth · R-M3 no CI · R-S1 temporal gap |
| **High** | R-C1, R-C3, R-C4, R-C5, R-O2, R-O4, R-O5, R-S4, R-S5 |
| **Medium** | R-C6, R-O3, R-O6, R-M1, R-M2, R-M5, R-S2, R-S3, R-DEP-1 |
| **Low** | R-M4, R-DEP-2, R-DEP-3 |

### 9.5 Top ten, in order

1. 🔴 **TODO** CI pipeline *(R-M3)*
2. 🔴 **TODO** Compaction policy + advisory API *(R-O1)*
3. 🔴 **TODO** Split `disk/store.go` *(R-M1)*
4. 🔴 **TODO** Persist ordered-key declarations *(R-O6)*
5. 🔴 **TODO** CSR v8: body checksum + declaration persistence, one bump *(R-C3)*
6. 🔴 **TODO** Steer callers to `UpdateNodeIndexed` in docs and examples *(R-C2)*
7. 🔴 **TODO** `cmd/graphene` read-only inspector *(R-O5)*
8. 🔴 **TODO** Cross-platform task runner *(R-M3)*
9. 🔴 **TODO** Weighted shortest path *(H1)*
10. 🔴 **TODO** Decide the temporal retention model *(Q1 / R-S1)*

---

## Change Log for This Plan

| Date | Revision | Change |
| --- | --- | --- |
| 2026-07-30 | v1 | Initial full audit of `HEAD` = `6317181` (`v0.3.0`) via read-only git operations. All nine sections populated; seven module templates completed; 32 commits, 7 tags, 9 packages, and 29 test files reviewed. |

> **Maintenance note.** This file is regenerated or amended as planning evolves;
> it is never shipped upstream and never contains code intended for the
> repository. Statuses in §9 are the single source of truth — module sections
> should be reconciled against the dashboard, not the reverse.

---

# Part II — Forensic Integrity Addendum (v2)

> **Appended, not merged.** Sections §1–§9 above are the v1 audit and are left
> byte-for-byte intact. Everything below is additive: it re-audits `HEAD` for
> cryptographic, adversarial, and formal-verification concerns that v1 did not
> consider, and then recomputes priority across every module in light of them.
> Where v2 supersedes a v1 judgement, it says so explicitly rather than editing
> the earlier text.
>
> **Method (unchanged).** Read-only inspection only — `git ls-tree`, `git log`,
> `git show`, `git grep`, `git rev-parse`, `git tag --list`, and direct file
> reads. No repository code was modified, no patch was produced, and no write
> operation to the repository is proposed anywhere below. Every design in this
> Part is a *plan*, not an instruction to change the tree.

## Contents (Part II)

10. [Audit Refresh — Δ since v1](#10-audit-refresh--δ-since-v1)
11. [Cryptographic Primitives — Merkle DAG, Signatures, Attestations](#11-cryptographic-primitives--merkle-dag-signatures-attestations)
12. [Forensic Threat Model](#12-forensic-threat-model)
13. [Formal Verification Hooks](#13-formal-verification-hooks)
14. [Updated Priority Dashboard](#14-updated-priority-dashboard)

---

## 10. Audit Refresh — Δ since v1

### 10.1 The tree moved

v1 was taken at `6317181` (`v0.3.0`). `HEAD` is now **`f6e1d7b` — "Add MIT
License to the project"**, one commit later, adding a single 21-line `LICENSE`
file and nothing else.

| v1 item | v1 state | v2 state |
| --- | --- | --- |
| §5.4 L5 — "`LICENSE` absent" | 🔴 **TODO** | **Resolved.** MIT licence present at `LICENSE` |
| §7.4 R-S5 — "No `LICENSE` file in the tree" | 🟣 **NEEDS INVESTIGATION**, Impact High | **Closed.** Retire from the register |
| §8 Q6 — "Is `LICENSE` intentionally absent?" | Open | **Answered.** It was not intentional; it is now MIT |
| §9.2 DX — `LICENSE` | 🟣 **NEEDS INVESTIGATION** | 🟢 **DONE** |

Nothing else changed: 9 packages, 29 test files, zero external dependencies,
`go 1.26`, tags unchanged at seven. `CHANGELOG.md` and `SECURITY.md` remain
absent — and §12 below gives `SECURITY.md` a much stronger justification than
"polish", which is where v1 filed it.

### 10.2 The cryptographic baseline, established by search

A repository-wide grep for `crypto`, `ed25519`, `ecdsa`, `sha256`, `hash.`,
`merkle`, `attest`, `signature`, `rbac`, `role`, `permission`, `actor`,
`principal`, `auth`, `redact`, and `audit` across all non-test `.go` files
returns **no cryptographic construct of any kind**. Precisely:

- **No `crypto/*` import anywhere in the module.** The only checksum in the tree
  is `crc32.ChecksumIEEE` (`disk/wal.go`).
- **`sha256` appears only as a caller-chosen property *key string*** in
  `examples/main.go` and in package doc comments (`graphene.go:27`). The engine
  never computes a hash; it stores an opaque value the caller labelled `"sha256"`
  and can neither verify nor interpret it.
- **No identity concept exists at any layer.** `store.TxOp`
  (`store/interface.go:394`) carries `Kind` and payload and nothing else. WAL
  records carry `[type][length][payload][crc32]` and nothing else. There is no
  actor, no principal, no session, no role, no capability token.
- **No `redact` symbol exists.** The nearest constructs are `DeleteNode`'s
  cascade + tombstone and `ReindexPurge` (`disk/store.go:464,503`), neither of
  which is a redaction primitive and neither of which records *who* invoked it.
- **No audit log exists.** The WAL is a durability mechanism, not an audit trail:
  it records *what the state became*, never *who caused it or when*.

The engine is therefore at cryptographic **zero**. That is not a criticism — it
is a coherent choice for an embeddable, single-process, zero-dependency library —
but it is the fact that governs everything in §11–§14.

### 10.3 CRC32 is an accident detector, not a tamper detector

This distinction is load-bearing and v1 did not draw it. `disk/wal.go`'s per-record
CRC32-IEEE footer and `walbatch.go`'s body CRC over the batch are excellent at
what they are for — detecting a torn write, a truncated tail, a bad sector. They
provide **zero** resistance to a deliberate edit, because CRC32 is public,
keyless, and trivially recomputable: an adversary who rewrites a WAL payload
recomputes the footer in one line and the log verifies clean. The same holds for
`walBatchCommitPayload`'s count-plus-CRC (`disk/walbatch.go:22`): forging a
consistent batch is arithmetic, not cryptanalysis.

**Consequence for the v1 risk register.** v1 scored R-C3 ("CSR body corruption
undetected") at **Likelihood Low** on the reasoning that silent bit-rot is rare.
Under an adversary model that likelihood is wrong by construction: a tampering
adversary is not a random process. §12 re-rates it.

### 10.4 A hard prerequisite v1 did not surface: serialisation is non-deterministic

`CSRGraph.SerialiseWithIndex` (`disk/csr.go:448`) writes the property-index
section from `propIdx.NodeEntries()` / `EdgeEntries()`
(`index/property_index.go:403,418`). Those walk all 16 shards and delegate to
`postings.forEachAll` (`index/property_index.go:812`), whose body is:

```go
for key, bucket := range p.byKey {
    for value, ids := range bucket {
```

Two nested **Go map range loops**, whose iteration order is randomised per
process by the runtime. The index entries are therefore emitted in a different
order on every run.

**Implication.** Compacting the same logical store twice produces two CSR files
that differ byte-for-byte in the `GIDX` section. So:

- A hash over the CSR file is **not reproducible** and cannot serve as a snapshot
  identity, a Merkle root, or an attestation subject.
- Two parties holding identical evidence cannot agree on a digest.
- A "did this snapshot change?" check reduces to a semantic comparison, not a
  digest comparison — which defeats the entire point.

**Canonical serialisation is therefore a strict prerequisite for every
cryptographic item in §11**, and it is cheap: sort entries by `(ID, Key, Value)`
before emission. It also has independent value (reproducible builds of fixtures,
stable diffs, byte-comparable golden files), and it is the reason §14 ranks it
first. Nothing in this document works without it.

Adjacent determinism questions, each 🟣 **NEEDS INVESTIGATION** before a digest
is defined over the corresponding bytes:

| Surface | Question |
| --- | --- |
| `Node.Labels` / `Edge.Labels` | Are label slices canonically ordered on write, or preserved as the caller supplied them? If the latter, two semantically identical nodes hash differently |
| `Node.Properties` / `Edge.Properties` | Opaque msgpack blob. The engine **cannot** canonicalise it — map ordering inside the caller's blob is the caller's problem. Any hash is over *bytes as supplied*, and that limit must be stated in the API contract |
| Delta → CSR record ordering in `Compact` | v1 §3.2 says collection walks CSR then delta; confirm the resulting record order is a pure function of ID |
| Float `Edge.Weight` | `float32` bit pattern is stable, but NaN payloads and ±0 are not canonical. Low likelihood, worth a decision |

### 10.5 The extension surface is unusually good, and that is the opportunity

Three properties of the existing tree make the whole of §11 tractable **as pure
additions**, with no redesign:

1. **The WAL record-type space is nearly empty.** `0x01`–`0x0A` are used and
   `0xFF` is the checkpoint (`disk/wal.go:44-61`). **`0x0B`–`0xFE` — 244 codes —
   are free.** Signature, attestation, actor, and audit records all fit as new
   types.
2. **`knownWALRecord` errors on an unknown type rather than skipping it**
   (`disk/wal.go`, and v1 §3.2 explains why). Under a cryptographic model this
   deliberate forward-incompatibility becomes a *security property*: an older
   binary **cannot** silently drop signature records and present the log as
   valid. Downgrade is a hard failure, which is exactly right for evidence.
3. **Capability negotiation already exists.** Eleven optional interfaces sit
   beside `store.GraphStore` and `Graph` type-asserts each
   (`store/interface.go:150-408`). `Attestor`, `Signer`, `AuditLogger`, and
   `MerkleVerifier` follow the identical pattern — third-party stores are not
   forced to implement any of them, and `Graph` degrades exactly as it does
   today. **No breaking change is required to add cryptographic capability to
   this engine.** That is a genuinely strong position to be starting from.

A fourth, weaker one: the CSR header is versioned (`GCSR`, versions 2–7,
`disk/csr.go:407-536`) with a self-describing `indexOffset`, so a v8 can append a
new addressable section beside `GIDX` without disturbing any reader that jumps by
offset.

### 10.6 The constraint that decides the algorithm choice

`go.mod` has no `require` block. v1 §7.5 R-DEP-2 rated zero-dependency as "Low ×
Low — a genuine asset". Under §11 it becomes **load-bearing**, because it
determines which primitives are admissible:

| Primitive | Go standard library | Verdict |
| --- | --- | --- |
| SHA-256 / SHA-512 | `crypto/sha256`, `crypto/sha512` | ✅ available |
| SHA-3 / SHAKE | `crypto/sha3` (Go 1.24+) | ✅ available |
| Ed25519 | `crypto/ed25519` | ✅ available |
| ECDSA (P-256/384) | `crypto/ecdsa`, `crypto/elliptic` | ✅ available |
| HMAC | `crypto/hmac` | ✅ available |
| **BLS12-381 aggregate signatures** | **absent** | ❌ **requires a third-party dependency** |
| BLAKE3 | absent (`x/crypto` has BLAKE2b, still external) | ❌ external |

**Finding.** The brief lists BLS as an optional multi-party attestation scheme.
Adopting it **breaks the zero-dependency property** — the single most distinctive
non-functional characteristic of this engine, and the one v1 called "worth
protecting as a stated constraint". Ed25519 + SHA-256 delivers the entire Merkle
/ signature / attestation programme with stdlib only. §11.4 therefore recommends
Ed25519 as mandatory, ECDSA P-256 as an optional interop mode, and BLS as
**deferred behind an explicitly-tagged optional module**, never in the core.

---

## 11. Cryptographic Primitives — Merkle DAG, Signatures, Attestations

### 11.1 The guarantee this can and cannot provide

Graphene is a library linked into the caller's process. The signing key, if one
exists, lives in the same address space as the code that decides what to sign.
An adversary with code execution in that process can sign anything the process is
authorised to sign, and no in-process construction changes that.

**Therefore the honest claim is tamper-*evidence*, not tamper-*prevention*.** The
programme below makes unauthorised modification **detectable by a third party who
holds an external reference point** — a co-signature, an anchored root, an
independently retained attestation. Everything in §11 should be read against that
statement, and §12 makes it the explicit boundary of the threat model. Any
documentation that claims more than this would be dishonest, and this plan
recommends `SECURITY.md` say exactly the above in its first paragraph.

### 11.2 Merkle DAG design

Four distinct DAGs, deliberately separate because they have different lifetimes,
different roots, and different verification audiences.

```
   ┌──────────────────────────────────────────────────────────────┐
   │ (D) AUDIT DAG        append-only chain of custody events      │
   │     A_n = H(A_{n-1} ‖ actor ‖ op ‖ ts ‖ subject)              │
   └───────────────▲──────────────────────────────────────────────┘
                   │ references
   ┌───────────────┴──────────────────────────────────────────────┐
   │ (B) WAL SEGMENT DAG   per-transaction leaves → segment roots  │
   │     leaf   = H(batch body)                                    │
   │     segRoot= Merkle(leaves) ; chained: S_k = H(S_{k-1}‖root_k) │
   └───────────────▲──────────────────────────────────────────────┘
                   │ compaction folds a segment range into…
   ┌───────────────┴──────────────────────────────────────────────┐
   │ (C) SNAPSHOT DAG      CSR image identity                      │
   │     nodeRoot = Merkle(H(node_i) ascending by ID)              │
   │     edgeRoot = Merkle(H(edge_j) ascending by ID)              │
   │     idxRoot  = Merkle(H(entry_k) canonical order)             │
   │     csrRoot  = H(header‖nodeRoot‖edgeRoot‖idxRoot‖prevCsrRoot) │
   └───────────────▲──────────────────────────────────────────────┘
                   │ leaves are…
   ┌───────────────┴──────────────────────────────────────────────┐
   │ (A) ENTITY HISTORY DAG   per-node / per-edge version chain    │
   │     v_n = H(canon(entity) ‖ v_{n-1} ‖ commitSeq ‖ actor)      │
   └──────────────────────────────────────────────────────────────┘
```

**Why a DAG and not a tree for (A).** An edge version depends on *three* parents:
its own predecessor version, and the current version hashes of its source and
target nodes. That is what makes graph history a DAG rather than a set of
independent chains, and it is what lets a verifier prove that an edge asserted a
relationship between two entities *as they stood at that moment* — the exact
question forensic provenance asks. It also means node updates must be ordered
before dependent edge hashes within a commit, which is a real constraint on the
commit path and is called out as invariant `INV-2` in §13.

**Why (B) chains segments rather than hashing the whole log.** v1 §3.5 Phase B
already proposes WAL segmentation for retention. A per-segment root plus a
running chain `S_k = H(S_{k-1} ‖ root_k)` means retiring an old segment does not
invalidate the chain: the successor's `S_{k-1}` input pins the retired segment's
root forever. **A deleted segment is provably absent rather than invisibly
absent** — which is precisely the "audit log deletion" attack surface in §12.

**Why (C) includes `prevCsrRoot`.** Compaction is where evidence is destroyed
today (v1 §3.5: "history survives exactly until the next compaction"). Binding
each snapshot root to its predecessor makes the sequence of compactions itself a
chain, so a substituted snapshot breaks the link even if the substitute is
internally consistent.

**Hash choice.** SHA-256, domain-separated by a one-byte tag per node kind
(`0x01` entity-version, `0x02` internal Merkle node, `0x03` batch leaf, `0x04`
segment root, `0x05` snapshot root, `0x06` audit entry) prefixed to every
pre-image. Domain separation is not optional: without it, a leaf pre-image can be
crafted to collide with an internal-node pre-image, which is the classic
second-preimage attack on Merkle trees.

### 11.3 Templates — Merkle Node and Merkle Edge

#### 11.3.1 Merkle Node Template

| Field | Encoding | Derivation | Source in tree |
| --- | --- | --- | --- |
| **Node ID** | `uint64` LE | `store.NodeID`; monotonic, never reused | `store/types.go:300` |
| **Hash** | 32 B | `SHA256(0x01 ‖ nodeID ‖ len(labels) ‖ sorted(labels) ‖ len(props) ‖ props ‖ parentHashes ‖ commitSeq ‖ actorID)` | new; requires §10.4 canonicalisation |
| **Children Hashes** | `n × 32 B` | Prior version hash of this node, plus current version hashes of every node reachable by an outbound edge created in the same commit. Empty for a node's first version | derived at commit in `disk/transaction.go:217` |
| **Timestamp** | `int64` ns UTC | From the batch-commit record. **Does not exist today** — WAL records carry no time (v1 §3.5) | new field in `walBatchCommitPayload` |
| **RBAC Context** | `uint32` role ID + `uint32` capability bitmap | The role under which the mutation was admitted | **no RBAC exists**; see §12.4 T-16 |
| **Signature** | 64 B Ed25519 | Over the Hash field, by the actor's key. Present on the *commit* by default; per-node signatures are the high-assurance mode | new |

> **Worked instance.** Node `5021`, a `MicroArtefact` also labelled
> `AntiForensicIndicator`, updated in commit `seq=91442` by role `analyst`:
> `ID=5021`; `Hash=SHA256(0x01 ‖ 5021 ‖ 2 ‖ [0x0003,0x0011] ‖ 412 ‖ <blob> ‖
> v_{n-1} ‖ 91442 ‖ actor:7)`; `Children=[v_{n-1}]`; `Timestamp=…`;
> `RBAC=(role=analyst, caps=READ|WRITE_ARTEFACT)`; `Signature=<64B>`.

**Cost.** 32 B per version per node, plus 32 B per parent link. On a 10⁶-node
store with one version each, the node layer of DAG (A) is ~32 MB — comparable to
the CSR image itself. **This is a memory-and-disk doubling, and it must be
measured before it is built** (`CONTRIBUTING.md`'s rule, and v1's H1 precedent).
Recommendation: hashes live *in the CSR file*, not resident in the heap, and are
loaded only by verification paths.

#### 11.3.2 Merkle Edge Template

| Field | Encoding | Derivation | Source in tree |
| --- | --- | --- | --- |
| **Edge ID** | `uint64` LE | `store.EdgeID` | `store/types.go:321` |
| **Hash** | 32 B | `SHA256(0x01 ‖ edgeID ‖ srcNodeHash ‖ dstNodeHash ‖ sorted(labels) ‖ weightBits ‖ propertiesHash ‖ prevEdgeHash ‖ commitSeq ‖ actorID)` | new |
| **Source Node Hash** | 32 B | Version hash of `Edge.Src` **as of this commit**, not its current value | DAG (A) lookup |
| **Target Node Hash** | 32 B | Version hash of `Edge.Dst` as of this commit | DAG (A) lookup |
| **Properties Hash** | 32 B | `SHA256(0x07 ‖ Edge.Properties)`; separated so a property-only redaction can be proven without revealing content | `store/types.go:321` |
| **Signature** | 64 B Ed25519 | As above | new |

Binding `srcNodeHash`/`dstNodeHash` rather than bare IDs is the whole point:
it makes an edge a statement about *specific versions* of two entities. An
adversary who mutates a node cannot leave its edges verifying — every incident
edge's hash is invalidated, which is detectable and is invariant `INV-1`.

**Note the cost this creates.** Recomputing incident edge hashes on every node
update is O(degree). On a supernode that is the same pathology v1 §3.3 flagged
as "no high-degree strategy exists" — **the crypto layer makes the unaddressed
supernode problem materially worse**, and §14 promotes the skew benchmark
accordingly. An alternative that avoids it (bind edges to node *IDs* plus the
commit root, not to node version hashes) is weaker but O(1); the choice is
recorded as open question **Q11** in §14.5.

### 11.4 Signatures

**Scheme selection**, constrained by §10.6:

| Scheme | Role | Rationale |
| --- | --- | --- |
| **Ed25519** | **Mandatory, default** | `crypto/ed25519`, stdlib. Deterministic (no RNG failure mode at signing), 64 B signatures, 32 B keys, fast verify. Deterministic signing matters here: a signature over a fixed pre-image is reproducible, which makes verification testable with golden vectors exactly as `TestComputeCRC32Vectors` pins CRC output |
| **ECDSA P-256** | Optional interop | `crypto/ecdsa`, stdlib. Only for environments mandating FIPS-style curves or hardware tokens. Note it is *non-deterministic* unless RFC 6979 is implemented — a different testing story |
| **BLS12-381** | **Deferred, out of core** | Aggregate multi-party attestation is genuinely valuable for co-signed custody, but there is no stdlib implementation. Confining it to a separately-tagged optional module preserves the zero-dependency guarantee for everyone who does not need it |

**What gets signed, and at what granularity.** Signing every record is
unaffordable; signing nothing is useless. Three tiers, selected by policy:

| Tier | Signed unit | Cost | Use |
| --- | --- | --- | --- |
| `SignNone` | — | 0 | default; preserves today's performance exactly |
| `SignCommit` | one signature per WAL batch-commit, over the batch Merkle leaf | ~1 sign per transaction (~50 µs Ed25519) | **recommended default for forensic deployments** |
| `SignEntity` | per node/edge version hash | O(entities) | high-assurance, per-artefact custody |

`SignCommit` is the right default because the batch-commit marker already exists
(`disk/walbatch.go`), already carries a CRC over exactly the bytes replay reads
back, and is already the atomicity boundary. Extending its payload from 8 bytes
to `count(4) ‖ crc(4) ‖ commitSeq(8) ‖ tsUnixNano(8) ‖ actorID(8) ‖
bodyHash(32) ‖ sigLen(2) ‖ sig(64)` = 126 B is **one amortised cost per
transaction**, not per record.

#### Signature Record Template

| Field | Encoding | Derivation | Integration point |
| --- | --- | --- | --- |
| **Operation ID** | `uint64` commit sequence | Monotonic per-store commit counter. **Does not exist**; v1 §3.5 Phase B already proposes it for temporal reasons — §14 promotes it because signatures need it too | new field, batch-commit payload |
| **Actor Identity** | `uint64` actor ID + 32 B public key fingerprint | Supplied by the caller at `Open` or per-transaction. The engine stores and binds it; it never authenticates it | new field on `store.TxOp` / a `TxContext` |
| **RBAC Role** | `uint32` role ID | The role asserted at admission time | **no RBAC exists** — §12.4 T-16 |
| **Payload Hash** | 32 B | `SHA256(0x03 ‖ batch body)` — the same byte range `walbatch.finish()` already CRCs (`disk/walbatch.go`) | `disk/walbatch.go:finish()` |
| **Signature** | 64 B Ed25519 | `Sign(sk, 0x03 ‖ commitSeq ‖ ts ‖ actorID ‖ payloadHash)` | new WAL record `0x0B` or extended `0x0A` payload |
| **Verification Metadata** | key ID (8 B), algorithm (1 B), key epoch (4 B), optional cert chain offset | Enables key rotation without invalidating history | new; see key-rotation note below |

**Key rotation is a first-class requirement, not an afterthought.** Evidence
outlives keys. The `key epoch` field plus a signed key-transition record
(WAL type `0x0D`) lets a verifier reconstruct which key was valid at which commit
range. Without it, a rotated or revoked key retroactively invalidates every
signature it ever made — which would make the system *worse* than no signatures,
because it would produce false tamper alarms on genuine evidence.

**Replay-path consequence.** Replay currently applies a batch when the commit
marker "describes what was read back" (`disk/wal.go`, batch-commit case). With
signatures, that check gains a second clause: the signature must verify against a
key valid at `commitSeq`. The failure mode must be **distinguishable** — a torn
batch (discard silently, this is normal crash recovery) is not a forged batch
(hard failure, surface loudly). Collapsing the two would let an adversary make
tampering look like a crash. This is proof obligation `PO-1` in §13.

### 11.5 Attestations

An attestation is the exportable, third-party-verifiable claim. It is the only
artefact in this design that *leaves* the process, and it is what makes §11.1's
"detectable by a third party" concrete.

#### Attestation Template

| Field | Encoding | Derivation | Notes |
| --- | --- | --- | --- |
| **Attestation ID** | 16 B — `SHA256(subject ‖ ts ‖ actor)[0:16]` | Content-derived, so it is stable and self-checking | avoids a UUID dependency |
| **Actor Identity** | actor ID + key fingerprint | Whoever asserts the claim | as §11.4 |
| **Operation Hash** | 32 B | The subject: an entity version hash, a batch leaf, a segment root, or a `csrRoot` | one attestation type per subject kind |
| **Merkle Path** | `depth × (32 B sibling ‖ 1 B direction)` | Inclusion proof from the subject leaf to the published root. `⌈log₂ n⌉ × 33 B` — ~700 B for a 10⁶-leaf tree | this is what makes the claim verifiable **without the database** |
| **Signature** | 64 B Ed25519 | Over `0x08 ‖ all preceding fields` | |
| **Timestamp** | `int64` ns UTC + optional RFC 3161 token | Local clock is *asserted*, not *proven*. An external timestamp authority token is what upgrades it | see T-09 |
| **Chain-of-Custody Link** | 32 B previous attestation hash + 8 B previous attestation seq | Makes the attestation stream itself a chain, so a removed attestation is provably missing | mirrors DAG (D) |

> **Worked instance — artefact custody.** Analyst attests that MicroArtefact
> `5021` was ingested at commit `91442`:
> `ID=SHA256(v_5021 ‖ ts ‖ actor:7)[0:16]`; `Actor=(7, fp:9f3c…)`;
> `OperationHash=v_5021`; `MerklePath=[(sib₀,R),(sib₁,L),…,(sib₁₉,R)]` proving
> `v_5021 ∈ nodeRoot` of snapshot `csrRoot=4a1e…`; `Signature=<64B>`;
> `Timestamp=2026-07-30T16:04:11.842Z`; `Prev=(a77b…, 8814)`.
> A recipient verifies this with the attestation, the published `csrRoot`, and
> `crypto/ed25519` — **no access to the store required**. That is the property
> that makes evidence transferable.

**Anchoring.** An attestation is only as strong as the reference point the
verifier trusts. In descending order of assurance: (1) publish `csrRoot` to an
external transparency log or notary; (2) co-sign with a second party's key held
elsewhere; (3) RFC 3161 timestamp token; (4) local signature only — which proves
*internal consistency* and nothing about an adversary who controls the process.
The design must not present (4) as if it were (1). §12's residual-risk column
holds the line on this.

### 11.6 Integration points, precisely located

Everything below is *additive*. No existing field changes meaning.

| # | Integration point | Location | Change | Format impact |
| --- | --- | --- | --- | --- |
| I-1 | Canonical index-entry ordering | `index/property_index.go:403,418,812` | sort by `(ID, Key, Value)` before emit | none — ordering only |
| I-2 | Batch-commit payload extension | `disk/walbatch.go:22` (`walBatchCommitPayload = 8`) | 8 B → 126 B: `count‖crc‖commitSeq‖ts‖actorID‖bodyHash‖sig` | WAL format bump |
| I-3 | New WAL record types | `disk/wal.go:44-61` (`0x0B`–`0xFE` free) | `0x0B` signature, `0x0C` attestation, `0x0D` key transition, `0x0E` audit event, `0x0F` segment root | uses reserved space |
| I-4 | Replay signature verification | `disk/wal.go` batch-commit case | second clause after the count/CRC check; **distinct** failure class | behavioural |
| I-5 | Segment roots + chaining | `disk/wal.go:173` `Checkpoint`, `Truncate` | emit `0x0F` before retiring a segment | requires v1 §3.5 segmentation |
| I-6 | CSR v8 attestation section | `disk/csr.go:407-536`, `disk/store.go:106-117` | new `"GATT"` section beside `GIDX`, addressable by a second offset; header gains `attestOffset:8` | CSR v7 → v8 |
| I-7 | Entity version hashes | `disk/csr.go` node/edge records | optional 32 B per record, flagged in the header | CSR v8 |
| I-8 | Merkle root recomputation | `disk/store.go:2070` `Compact()` | the pass already walks every surviving record — fold hashing into it | none extra |
| I-9 | Cryptographic verification capability | `store/interface.go:197` `IndexVerifier` precedent | new `MerkleVerifier` / `Attestor` / `Signer` / `AuditLogger` optional interfaces | none — additive |
| I-10 | Actor context | `store/interface.go:340,394` `Transactor`, `TxOp` | `TxContext{ActorID, RoleID, KeyID}` threaded to commit | contract addition |
| I-11 | Façade surface | `graphene.go`, `transaction.go:74` | `Graph.WithActor`, `Graph.Attest`, `Graph.VerifyChain`, negotiated by type assertion exactly as the existing eleven | none — additive |
| I-12 | Epoch binding | `disk/store.go:48-67` `atomic.Pointer[CSRGraph]` + `csrShadowed` | attestations bind the CSR epoch they were computed against | none |

**The format-bump observation from v1 §4.4 now has five members, not three.**
v1 said "three separate roadmap items each require a CSR version bump; ship one
v8, not three." v2 adds two more: the entity version-hash records (I-7) and the
`GATT` attestation section (I-6). **All five must be designed together.** If v8
ships without reserving space for the crypto sections, a v9 follows immediately
and every v8 store needs migrating — the single most expensive avoidable mistake
available in this roadmap.

### 11.7 What this costs

Stated plainly so the trade is visible, per `CONTRIBUTING.md`'s measurement
discipline. **None of these numbers are measured — they are estimates, and the
estimate is not the licence to build.** A skew-aware benchmark harness (v1 §3.3
H1, promoted in §14) must produce real figures first.

| Cost | Estimate | Mitigation |
| --- | --- | --- |
| Write path: one Ed25519 sign per commit | ~50 µs/txn | amortised over the batch; `SignNone` default preserves current behaviour exactly |
| Write path: SHA-256 over batch body | ~1 GB/s | replaces nothing — CRC32 stays for cheap tear detection |
| Node update: recompute O(degree) incident edge hashes | unbounded on supernodes | Q11 (§14.5) — or the H2 per-type adjacency work lands first |
| Disk: 32 B/entity version hash | +~6 % of a 100 k-node fixture's ~22 MB | optional, header-flagged |
| Disk: WAL retention for chain completeness | **unbounded** | the retention-policy decision (v1 Q1) is now a *forensic* decision, not a performance one |
| Open: chain verification | O(log) per attestation, O(n) full verify | must be opt-in, exactly as `Open` already declines to run `VerifyIndexes` (`disk/store.go:167-181`) — the same reasoning applies verbatim |
| Memory during compaction | already ~2× graph (v1 R-O2); hashing adds a pass | strengthens the case for v1's streaming-serialisation item |

### 11.8 Status

| Component | Status |
| --- | --- |
| Canonical serialisation | 🔴 **TODO** — prerequisite for everything below |
| Commit sequence + timestamp | 🔴 **TODO** — v1 Phase 3 item, now a Phase 1 prerequisite |
| Actor identity plumbing | 🔴 **TODO** — no identity concept exists at any layer |
| Entity-history DAG (A) | 🔴 **TODO** |
| WAL segment DAG (B) | 🔴 **TODO** — gated on segmentation (v1 §3.2) |
| Snapshot DAG (C) | 🔴 **TODO** — gated on CSR v8 |
| Audit DAG (D) | 🔴 **TODO** |
| Ed25519 commit signing | 🔴 **TODO** |
| Attestation export/verify | 🔴 **TODO** |
| Key rotation / epochs | 🔴 **TODO** |
| BLS aggregate attestation | 🟣 **NEEDS INVESTIGATION** — breaks zero-dependency; recommend deferral |
| Hashable determinism of `Labels`, `Weight`, delta→CSR order | 🟣 **NEEDS INVESTIGATION** — §10.4 |
| CRC32 framing (accident detection) | 🟢 **DONE** — and explicitly *not* a tamper control |

---

## 12. Forensic Threat Model

### 12.1 Trust boundary — the shape of the problem

```
   ┌───────────────────────── HOST PROCESS ─────────────────────────┐
   │                                                                 │
   │   caller code ──► graphene.Graph ──► disk.Store ──► WAL/CSR      │
   │        ▲                                   │                    │
   │        │  signing key lives HERE           │                    │
   │        └───────────────────────────────────┘                    │
   │                                                                 │
   │   ◄── NO PROCESS BOUNDARY, NO AUTHN, NO AUTHZ, NO IPC ──►        │
   └────────────────────────────┬────────────────────────────────────┘
                                │ filesystem
                    ┌───────────▼────────────┐
                    │ graphene.csr           │  ← attacker-reachable
                    │ graphene.wal           │  ← attacker-reachable
                    └────────────────────────┘
```

Two consequences govern every entry below:

1. **The TCB is the entire host process.** The engine has no process boundary
   (v1 §1.3: "no process boundary, therefore no replication, no sharding, no
   auth"). Any adversary executing code in-process is *inside* the trust
   boundary and can call `DeleteNode`, `Compact`, or the signing key directly.
   In-engine controls cannot mitigate that class; only external anchoring can
   *detect* it after the fact.
2. **The on-disk files are outside it.** `graphene.csr` and `graphene.wal` are
   ordinary files with ordinary permissions. Everything in §11 exists to make
   offline modification of those files detectable, and that is the class where
   this design is genuinely strong.

### 12.2 Adversary classes

| ID | Class | Capabilities assumed | In TCB? | Best available control |
| --- | --- | --- | --- | --- |
| **A1** | Internal malicious actor | Legitimate credentials, in-process API access, may hold the signing key | **Yes** | External co-signing + anchored roots + audit chain. Detection, never prevention |
| **A2** | External attacker | Filesystem read/write on the store directory; no code execution in the host process | No | Merkle chains + signatures — **this is the class §11 defeats outright** |
| **A3** | Compromised ingestion pipeline | Can submit arbitrary well-formed writes; cannot alter already-committed data | Partially | Per-actor keys, RBAC admission, ingest-side attestation. Bounds blast radius; cannot make bad input good |
| **A4** | Compromised storage medium | Arbitrary bit-level modification, silent bit-rot, rollback to an older file | No | Cryptographic digests (**not** CRC32) + snapshot-root chaining + external root retention |
| **A5** | Tampering during replay | Modifies WAL bytes between `Close` and the next `Open`; or a downgraded binary | No | Signature verification at replay + `knownWALRecord`'s hard-fail on unknown types (§10.5) |
| **A6** | RBAC privilege escalation | Assumes a higher role than granted | **Yes** | **No control exists — RBAC is absent entirely.** This is a design gap, not a weakness in a control |

### 12.3 Attack surfaces, mapped to code

| Surface | Where it lives today | Current defence | Adequate vs A2/A4? |
| --- | --- | --- | --- |
| **WAL corruption** | `disk/wal.go:44-61`, framing + CRC32 | per-record CRC32, batch count+CRC | **No** — keyless, recomputable (§10.3) |
| **Index poisoning** | `index/property_index.go`, `GIDX` section | `VerifyIndexes` — *structural only*; cannot check a value against an opaque blob (v1 §3.2) | **No** — a poisoned but structurally valid index verifies clean |
| **Snapshot tampering** | `disk/csr.go:407-536`; magic + version only | no body checksum at all (v1 C4) | **No** — v1 already flagged this; §10.3 raises its likelihood |
| **Redaction misuse** | `DeleteNode` cascade, tombstones, `ReindexPurge` (`disk/store.go:464,503`) | none — no actor, no record, no reversibility constraint | **No** — and after `Compact()` it is unrecoverable and unattributable |
| **Traversal manipulation** | `traversal/*`, planner `disk/store.go:1585-1868` | results follow from stored data; no result integrity claim | Partially — sound iff the data is sound; no independent check |
| **Audit log deletion** | **no audit log exists** | — | **N/A — nothing to delete because nothing is recorded** |

### 12.4 Threat register

Each entry uses the Threat Model Entry Template: *Threat ID · Adversary Class ·
Attack Surface · Impact · Mitigation · Residual Risk · Verification Hooks.*

| ID | Adversary | Attack Surface | Impact | Mitigation | Residual Risk | Verification Hooks |
| --- | --- | --- | --- | --- | --- | --- |
| **T-01** | A2, A4 | WAL corruption | Offline edit of a committed record; CRC32 recomputed; replay applies forged state as authentic. **Total loss of evidentiary value** | Ed25519 signature over the batch body in the commit marker (I-2); verify at replay (I-4) | Adversary holding the signing key (A1) signs freely. External co-signature required to bound | V-01, V-02 |
| **T-02** | A2, A5 | WAL corruption | **Truncation attack** — delete the log tail to roll back recent commits. Every remaining record verifies | Monotonic `commitSeq` in the commit payload + segment-root chaining (I-5); a gap is provable | Rollback to a state whose *latest* root was never externally published is undetectable | V-02, V-03 |
| **T-03** | A5 | WAL corruption | **Forgery disguised as a torn tail.** Replay currently discards a bad batch silently — indistinguishable from normal crash recovery | Split the failure classes: CRC mismatch → silent discard; signature mismatch → hard error surfaced to the caller | A truncation at an exact batch boundary remains ambiguous without T-02's sequence check | V-02, PO-1 |
| **T-04** | A5 | WAL corruption | **Downgrade attack** — an older binary reads the log ignoring signature records | Already mitigated by design: `knownWALRecord` errors on unknown types rather than skipping (§10.5) | Requires a documented downgrade procedure (v1 Q8, still open) | V-04 |
| **T-05** | A2, A4 | Snapshot tampering | Direct edit of `graphene.csr` node/edge records. Magic and version still parse; **`Open` succeeds** (v1 C4) | `csrRoot` over node/edge/index roots (§11.2 DAG C), stored in `GATT` (I-6); opt-in verify at `Open` | Verification is opt-in for the same startup-cost reason `VerifyIndexes` is (`disk/store.go:167-181`) — a caller who never verifies gains nothing | V-05, V-06 |
| **T-06** | A4 | Snapshot tampering | **Snapshot substitution / rollback** — swap in a genuine older CSR. Internally perfectly consistent | `prevCsrRoot` chaining (§11.2) + externally retained roots | Undetectable if the verifier's only reference is the store itself. **Anchoring is the only real control** | V-06, V-11 |
| **T-07** | A2 | Index poisoning | Edit the `GIDX` section so `NodesByProperty("sha256", X)` returns the wrong artefact. Structurally valid → `VerifyIndexes` passes | `idxRoot` over canonically-ordered entries (§11.2, requires I-1) | Cannot bind an indexed value to the entity's *actual* property — the blob is opaque (v1 §3.2). **This limit is permanent** | V-07, V-08 |
| **T-08** | A1, A3 | Index poisoning | **Stale-index exploitation.** v1 R-C2: default `ReindexKeep` leaves entries pointing at superseded values, and the planner trusts them. An insider updates an entity knowing queries still find the old value | Steer callers to `UpdateNodeIndexed`; consider making `ReindexPurge` the forensic-profile default; surface the policy in `ExplainNodeQuery` | Caller-side; the engine cannot detect it because it cannot decode the blob | V-08, V-13 |
| **T-09** | A1 | Snapshot tampering | **Backdated evidence** — a local clock supplies the attestation timestamp | RFC 3161 token or external notary (§11.5) | Local time is an *assertion*. Without an external authority, temporal claims are unproven. Must be stated in `SECURITY.md` | V-09 |
| **T-10** | A1, A6 | Redaction misuse | `DeleteNode` cascades to incident edges and tombstones them. **No record of who, when, or why**; after `Compact()` the entity is unrecoverable | Attested deletion record (WAL `0x0E`) capturing actor, role, reason, and the deleted entity's final version hash — so *what was removed* remains provable | An in-process A1 can call `Compact` and destroy the record too. Segment retention + external roots bound this | V-10, V-12 |
| **T-11** | A1 | Redaction misuse | **Over-broad cascade** — deleting a hub node silently removes a large evidentiary subgraph | Pre-deletion impact report; policy cap on cascade size; attest the full deleted ID set | Legitimate large deletions exist; policy is the caller's, not the engine's | V-10 |
| **T-12** | A1, A6 | Redaction misuse | **Irreversibility with no reversibility model.** Lawful redaction (PII) and evidence destruction are the same operation today | Crypto-erasure: retain the version hash + a tombstone attestation while destroying plaintext, so the *fact and shape* of the redaction survive | Genuinely hard. Interacts with retention policy and with the opaque-blob limit | V-10, PO-4 |
| **T-13** | A1, A2 | Audit log deletion | **No audit log exists.** Nothing records reads, queries, exports, compactions, or configuration changes | Append-only audit DAG (D), chained `A_n = H(A_{n-1} ‖ …)`, in its own file, sealed per segment | An adversary who deletes the whole file leaves only a gap the *last externally-published* anchor can prove | V-11, V-12 |
| **T-14** | A2, A4 | Audit log deletion | **Selective excision** — remove one embarrassing entry and re-chain the remainder | Chaining alone does not stop this if the adversary can recompute forward. Periodic signed + externally anchored checkpoints do | Bounded by anchoring interval: entries since the last anchor are excisable | V-11 |
| **T-15** | A3 | Traversal manipulation | Poisoned ingest injects fabricated edges; provenance chains and pattern matches then report attacker-chosen results with full internal consistency | Per-actor keys; edges bound to source/target *version* hashes (§11.3.2); attestation of ingest batches | **Garbage-in remains garbage-out.** Crypto proves *who asserted it*, never *whether it is true*. This must not be overstated anywhere | V-01, V-14 |
| **T-16** | A6 | Traversal manipulation / all | **No RBAC exists.** No role, no permission, no principal anywhere in the tree (§10.2). Every caller has every capability | Role model + admission checks at the façade; role ID bound into every signed commit | The engine is in-process — RBAC here is an *audit and attribution* mechanism, not a security boundary. Saying otherwise would be misleading | V-13 |
| **T-17** | A1, A4 | Snapshot tampering | **Non-deterministic serialisation (§10.4)** means two honest compactions produce different bytes. A verifier cannot distinguish "recompacted" from "tampered", so digest comparison is unusable | Canonical ordering (I-1) | None once fixed. **This is the cheapest high-value item in the entire plan** | V-05, PO-2 |
| **T-18** | A2 | WAL corruption / snapshot | **Parser-level attack.** `deserialiseCSR`, `readCSRIndexSection`, and WAL replay parse untrusted bytes. v1 §3.7 notes there are no fuzz targets | Fuzz targets (v1 Phase 1) — now a *security* control, not a QA nicety | Memory-safety in Go limits severity to panic/DoS rather than RCE | V-14 |

### 12.5 Integrity guarantees — current vs target

| Guarantee | Today | With §11 | Ceiling |
| --- | --- | --- | --- |
| **Tamper-evident storage** | ❌ CRC32 only — keyless and recomputable | ✅ vs A2/A4/A5 | Never vs A1 without external anchoring |
| **Cryptographically verifiable mutations** | ❌ nothing is signed | ✅ per-commit; per-entity in `SignEntity` | Bounded by key custody |
| **Immutable snapshots** | ⚠️ CSR is immutable *in memory once published*; the **file** is freely editable | ✅ `csrRoot` + chaining | Opt-in verification means an unverifying caller gains nothing |
| **Deterministic replay** | ⚠️ deterministic in *outcome*; **not** byte-deterministic in output (§10.4) | ✅ after canonicalisation | Caller-encoded blobs are outside engine control |
| **RBAC-enforced access boundaries** | ❌ absent entirely | ⚠️ attribution and audit only | **In-process RBAC is not a security boundary.** Real enforcement needs a process boundary the engine deliberately does not have (v1 §1.3) |

### 12.6 Residual risk summary

| Level | Threats | Why it stays |
| --- | --- | --- |
| **Unmitigable in-engine** | T-01(A1), T-10(A1), T-13(A1), T-16 | The adversary is inside the TCB. Only external anchoring, key custody outside the process, and organisational control apply |
| **Mitigable only by anchoring** | T-02, T-06, T-09, T-14 | Rollback and backdating are undetectable without an external reference point |
| **Fully mitigable** | T-03, T-04, T-05, T-07, T-17, T-18 | Design and implementation entirely within the engine's reach |
| **Permanent by design** | T-08, T-15 | The property blob is opaque and the engine cannot decode it (v1 §3.2). No cryptography makes an untrue assertion false |

**The single most important sentence for `SECURITY.md`:** *Graphene can make
modification of its stored evidence detectable by a party holding an
independently retained root; it cannot prevent modification by code running in
its own process, and it cannot attest to the truth of what it was told.*

---

## 13. Formal Verification Hooks

### 13.1 Approach

Three tiers, chosen because the cost profile differs by an order of magnitude at
each step and because the repository's existing test culture (v1 §2.4 — parity
suites, integrity suites, pinned format vectors) already occupies the cheapest
tier competently.

| Tier | Method | Tooling | Where it fits |
| --- | --- | --- | --- |
| **1 — Executable** | Property-based, metamorphic, and differential tests; fuzzing | `testing/quick`, Go native fuzzing, the existing parity suites | Extends what exists. **Highest value per hour, by a wide margin** |
| **2 — Model checking** | Specify concurrent/crash protocols; check exhaustively over small state spaces | **TLA+ / PlusCal** with TLC | WAL replay, batch atomicity, compaction, the lock-free CSR read path |
| **3 — Mechanised proof** | Machine-checked proofs of pure algorithms | **Lean 4** (preferred) or Coq | Merkle inclusion soundness, sorted-ID algebra, order-preserving codecs |

**Tool recommendation.** TLA+ for Tier 2 — the protocols here (a single-writer
log with markers, a crash boundary, an epoch swap under a shadow counter) are
exactly what TLC handles, and the specs are small. Lean 4 for Tier 3, over Coq,
for a shallower learning curve and better tooling; the proof surface is narrow
(inclusion-proof soundness, comparator totality, codec order preservation) and
deliberately excludes anything touching Go's memory model.

**Non-goal, stated so scope does not creep.** Verifying the Go implementation
itself is out of scope. The specs verify *designs*; the executable tier is what
connects a verified design to the running code — via **refinement tests** that
replay a TLC-generated trace against the real store and assert the same
observable outcome. That link is the part most such efforts skip, and skipping it
makes the proofs decorative.

### 13.2 Invariants

| ID | Invariant | Formal statement | Checkable today? |
| --- | --- | --- | --- |
| **INV-1** | **Node/edge hash consistency** | ∀ edge `e` live at commit `c`: `e.srcNodeHash = versionHash(e.Src, c)` ∧ `e.dstNodeHash = versionHash(e.Dst, c)`. Mutating an entity invalidates every incident edge hash | No — no hashes exist |
| **INV-2** | **Merkle DAG correctness** | The version-hash relation is acyclic; every parent hash resolves to a stored version; within a commit, node hashes are computed before dependent edge hashes; roots are a pure function of the leaf multiset | No |
| **INV-3** | **RBAC permission monotonicity** | Permissions granted within a session never increase without an attested grant record. Formally: `∀ t₁<t₂ : caps(s,t₂) ⊆ caps(s,t₁) ∪ granted(s,[t₁,t₂])` | No — no RBAC |
| **INV-4** | **Temporal ordering consistency** | `commitSeq` is strictly monotonic; `seq(a) < seq(b) ⇒ ts(a) ≤ ts(b)`; every entity version chain is totally ordered by `commitSeq` | No — no seq, no ts |
| **INV-5** | **Replay determinism** | `replay(log) = replay(log)` for state **and** for serialised bytes. The byte clause fails today (§10.4) | **Partially** — outcome determinism is already covered by `graphene_persistence_test.go`; byte determinism is falsifiable *right now* |
| **INV-6** | *(added)* **Batch atomicity under crash** | For any truncation point in the log, replay yields a state containing either all or none of each batch's records | Yes — existing framing supports it; not exhaustively tested |
| **INV-7** | *(added)* **Lock-free read soundness** | A reader observing `csrShadowed == 0` and an unchanged CSR pointer across its read observed a value that was current at some instant during the read (v1 R-C5) | Yes — and `CONTRIBUTING.md` explicitly notes the race detector **does not** find stale reads, which is precisely why this needs a model, not a test |
| **INV-8** | *(added)* **Comparator totality** | For a declared key, `bytes.Compare` is a total order. For an undeclared key the numeric-then-bytes rule is **not** (v1 §3.3: `"9" < "10" < "1x" < "9"`) — the invariant is that no sorted structure is ever built on the non-total comparator | Yes — statically checkable |

### 13.3 Proof obligations

| ID | Obligation | Statement | Tier | Gates |
| --- | --- | --- | --- | --- |
| **PO-1** | **WAL replay correctness** | Replay of any prefix of a valid log, truncated at any byte offset, yields a state reachable by some prefix of the committed transaction sequence — and **never** a state containing a partially-applied batch. With signing: a forged batch is rejected with a failure class *distinguishable* from a torn one (T-03) | 2 | T-01, T-03 |
| **PO-2** | **Snapshot immutability** | `csrRoot(compact(S)) = csrRoot(compact(S))` for identical logical `S`; and `csrRoot` changes iff the logical content changes. **Currently false** (§10.4) | 1+3 | T-05, T-17 |
| **PO-3** | **Index integrity** | Every posting is derivable from live entity state; `VerifyIndexes`'s structural invariants are complete w.r.t. the structural claims, **and its incompleteness w.r.t. value correctness is explicitly bounded** — an opaque blob cannot be checked (v1 §3.2) | 1+2 | T-07 |
| **PO-4** | **Redaction reversibility constraints** | Redaction destroys plaintext while preserving the version hash, the tombstone, and the attestation — so the *fact*, *actor*, *time*, and *shape* of the removal remain provable while the content does not | 2 | T-10, T-12 |
| **PO-5** | **Chain-of-custody completeness** | For any entity, the attestation chain from ingest to the present has no gap: every `commitSeq` touching it has a corresponding audit entry, and the audit chain hash-links without break | 2 | T-13, T-14 |
| **PO-6** | *(added)* **Compaction preservation** | `Compact` preserves the live entity set exactly (no live entity lost, no tombstoned entity resurrected) and preserves sequence high-water marks so no ID is reused | 1+2 | R-C5, T-06 |
| **PO-7** | *(added)* **Merkle inclusion soundness** | A verifying inclusion proof for leaf `l` under root `r` implies `l` is in the multiset that generated `r`, given a collision-resistant `H` and correct domain separation | 3 | T-05, T-07 |

### 13.4 Verification hook register

Each entry uses the Verification Hook Template: *Hook ID · Invariant ·
Verification Method · Tool · Proof Obligation · Integration Point.*

| Hook | Invariant | Verification Method | Tool | Proof Obligation | Integration Point |
| --- | --- | --- | --- | --- | --- |
| **V-01** | INV-1 | Property test: mutate an entity, assert every incident edge hash changes and re-verification fails | Go `testing/quick` | PO-7 | `disk/transaction.go:217` commit path |
| **V-02** | INV-6, INV-5 | **Crash-injection model**: truncate a log at every byte offset; assert all-or-nothing per batch and a distinguishable forgery class | TLA+ (TLC) + Go replay harness | PO-1 | `disk/wal.go` replay loop |
| **V-03** | INV-4 | Monotonicity assertion on `commitSeq` across replay; gap detection over segment roots | Go invariant check in replay | PO-1, PO-5 | batch-commit payload (I-2) |
| **V-04** | INV-5 | Version-compatibility matrix: assert an old reader hard-fails on a new log rather than skipping records | Go table test | PO-1 | `knownWALRecord` (`disk/wal.go`) |
| **V-05** | INV-5, INV-2 | **Byte-determinism test**: compact twice from identical state, assert byte-identical output. **Falsifiable against `HEAD` today — it will fail** (§10.4) | Go golden-file test | PO-2 | `disk/csr.go:448` `SerialiseWithIndex` |
| **V-06** | INV-2 | Root-chain verification: walk `prevCsrRoot` links across compactions | Go + `cmd/graphene verify` | PO-2, PO-6 | new `GATT` section (I-6) |
| **V-07** | INV-2 | Merkle inclusion proof soundness and second-preimage resistance under domain separation | **Lean 4** | PO-7 | pure hashing package |
| **V-08** | INV-1 | Differential: rebuild the index from records, assert the rebuilt `idxRoot` equals the stored one | Go, reusing `RebuildIndexes` | PO-3 | `store/interface.go:205` `IndexRebuilder` |
| **V-09** | INV-4 | Clock-skew model: assert no attestation claims a time before its causal predecessor | TLA+ | PO-5 | attestation record (I-3) |
| **V-10** | INV-3 | Redaction model: prove the post-redaction state retains hash + tombstone + attestation and loses only plaintext | TLA+ | PO-4 | `DeleteNode` / `ReindexPurge` (`disk/store.go:464,503`) |
| **V-11** | INV-2 | Audit-chain continuity: hash-link every entry; assert no gap between anchors | Go + `cmd/graphene audit` | PO-5 | audit DAG (D) |
| **V-12** | INV-3, INV-4 | Chain-of-custody completeness for a given entity across its whole lifetime | TLA+ + Go integration test | PO-5 | attestation + audit records |
| **V-13** | INV-3 | Permission monotonicity model-check over role transitions | TLA+ | — | RBAC admission layer (does not exist) |
| **V-14** | INV-5, INV-8 | **Fuzz** WAL replay, `deserialiseCSR`, `readCSRIndexSection`, `ParseNodeType`/`ParseEdgeType`, and the property comparators | Go native fuzzing | PO-1, PO-3 | v1 §3.7 fuzz item — **now a security control** (T-18) |
| **V-15** | INV-7 | Model the CSR epoch swap, shadow counter, and pointer re-check; check for stale-read interleavings | **TLA+** | PO-6 | `disk/store.go:48-67`, `757-782` |
| **V-16** | INV-8 | Static assertion that the non-total comparator is never used to build a sorted structure | Go test + lint rule | PO-3 | `index/narrow.go:196-217`, `store/query_types.go:354-416` |

### 13.5 Sequencing, and the one thing to do first

**V-05 is executable against `HEAD` today, requires no new subsystem, and will
fail.** It converts §10.4 from an argument into a red test. That is the correct
entry point for this entire programme: a failing test that motivates the cheapest
fix (I-1, canonical ordering), which in turn unblocks every cryptographic item.

Then, in order: V-14 (fuzz — pure win, already planned in v1, now security-
relevant) → V-02 and V-15 (TLA+ specs of the two hardest existing protocols,
valuable **even if no cryptography is ever added**, because they cover R-C5 and
batch atomicity as they stand) → V-01/V-03/V-06/V-08 as the crypto layer lands →
V-07 (Lean) → V-09/V-10/V-12 → V-13 last, since it depends on an RBAC layer that
does not exist.

**Note the standalone value.** V-02, V-05, V-14, V-15, and V-16 all pay for
themselves against the *current* tree. Nothing in §13 requires committing to §11
first.

### 13.6 Status

| Tier | Component | Status |
| --- | --- | --- |
| 1 | Parity, integrity, format-vector suites | 🟢 **DONE** (v1 §2.4) |
| 1 | Byte-determinism test (V-05) | 🔴 **TODO** — will fail on `HEAD` |
| 1 | Fuzz targets (V-14) | 🔴 **TODO** |
| 1 | Metamorphic / property tests (V-01, V-08) | 🔴 **TODO** |
| 2 | TLA+ spec — WAL replay & batch atomicity (V-02) | 🔴 **TODO** |
| 2 | TLA+ spec — lock-free CSR read path (V-15) | 🔴 **TODO** |
| 2 | TLA+ spec — compaction preservation (PO-6) | 🔴 **TODO** |
| 2 | TLA+ spec — redaction, custody, clock skew | 🔴 **TODO** |
| 3 | Lean 4 — Merkle inclusion soundness (V-07) | 🔴 **TODO** |
| 3 | Lean 4 — comparator totality, codec order preservation | 🟣 **NEEDS INVESTIGATION** — may be adequately covered by Tier 1 |
| — | Refinement link (TLC trace → Go harness) | 🟣 **NEEDS INVESTIGATION** — the part most efforts skip |

---

## 14. Updated Priority Dashboard

> **Marker convention for this section.** §14 uses ANSI SGR colour codes as
> specified by the current directive: green `DONE`, yellow `IN PROGRESS`, red
> `TODO`, magenta `NEEDS INVESTIGATION`. They render as colour in a terminal
> (`cat plan.md`, `less -R`) and as control characters in a plain Markdown
> viewer. **§1–§9 keep their emoji markers unchanged** — the two schemes are 1:1
> equivalent and neither supersedes the other.

### 14.1 Recalculation method

Priority is recomputed from the six factors in the directive. Benefit factors are
scored 1–5; complexity is a divisor; dependency ordering is a **gate**, not a
score, because a high-value item that cannot be built yet is not a high priority.

```
   Benefit  B = FS + SEC + DI + RR          (forensic soundness, security,
                                             data integrity, risk reduction)
   Cost     C = implementation complexity   (1 trivial … 5 major subsystem)

   Score    P = B / C          Rank by P, then reorder to respect DEP.
```

Two structural judgements shape the result:

1. **Adversarial re-rating.** v1 scored risks against accident. §12 scores them
   against an adversary. Anything whose only defence is CRC32 or "structurally
   valid" moves up sharply — an adversary is not a random process (§10.3).
2. **Prerequisite promotion.** Three items v1 placed in Phases 3–4 are
   prerequisites for everything cryptographic and move to the front:
   canonical serialisation, commit sequence + timestamp, and actor identity.

### 14.2 Scored register

Sorted by `P`. `DEP` names the gate that must land first.

| Rank | Item | Module | FS | SEC | DI | RR | B | C | **P** | DEP |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | **Steer callers to `UpdateNodeIndexed`; document `ReindexPurge` as the forensic default** | Indexing/Docs | 3 | 2 | 5 | 4 | 14 | 1 | **14.0** | — |
| 2 | **Canonical serialisation** (I-1) | Storage/Index | 5 | 3 | 5 | 4 | 17 | 2 | **8.5** | — |
| 3 | **Byte-determinism test + fuzz targets** (V-05, V-14) | DX | 3 | 4 | 5 | 5 | 17 | 2 | **8.5** | — |
| 4 | **CI pipeline** | DX | 2 | 4 | 4 | 5 | 15 | 2 | **7.5** | — |
| 5 | **Commit sequence + wall-clock timestamp in the commit record** (I-2) | Storage | 5 | 3 | 3 | 4 | 15 | 2 | **7.5** | canonical |
| 6 | **Cryptographic digest beside CRC32** (WAL body + CSR body) | Storage | 4 | 5 | 5 | 5 | 19 | 3 | **6.3** | canonical |
| 7 | Cross-platform task runner | DX | 0 | 1 | 2 | 3 | 6 | 1 | **6.0** | — |
| 8 | **Ed25519 commit signing** (`SignCommit` tier) | Storage | 5 | 5 | 3 | 4 | 17 | 3 | **5.7** | actor id, seq |
| 9 | **Actor identity plumbing** (`TxContext`, I-10) | Core | 5 | 5 | 2 | 4 | 16 | 3 | **5.3** | — |
| 10 | **Append-only audit log** (DAG D) | Core/Storage | 5 | 4 | 3 | 4 | 16 | 3 | **5.3** | actor id |
| 11 | **Attestation export + verify API** | Core | 5 | 4 | 2 | 4 | 15 | 3 | **5.0** | signing |
| 12 | **TLA+: WAL replay atomicity + lock-free read** (V-02, V-15) | DX | 4 | 3 | 5 | 4 | 16 | 4 | **4.0** → **4.75** *(pays off on the current tree alone)* | — |
| 13 | **Merkle DAG over WAL segments** (DAG B) | Storage | 5 | 5 | 4 | 5 | 19 | 4 | **4.75** | segmentation |
| 14 | Compaction policy + advisory API | Storage | 3 | 2 | 4 | 5 | 14 | 3 | **4.7** | stats |
| 15 | `cmd/graphene` inspector **+ `verify` / `audit` subcommands** | DX | 4 | 3 | 3 | 4 | 14 | 3 | **4.7** | — |
| 16 | Split `disk/store.go` | DX | 1 | 2 | 2 | 4 | 9 | 2 | **4.5** | — |
| 17 | **Chain-of-custody links** | Core | 5 | 3 | 2 | 3 | 13 | 3 | **4.3** | attestation |
| 18 | **CSR v8 — one bump, five payloads** (checksum→root, ordered decls, composite reservation, entity hashes, `GATT`) | Storage | 5 | 4 | 4 | 4 | 17 | 4 | **4.25** | canonical, digest |
| 19 | Persist ordered-key declarations | Indexing | 1 | 1 | 3 | 3 | 8 | 2 | **4.0** | rides on v8 |
| 20 | `DiffSubgraphs` / `DiffAgainst` | Provenance | 4 | 1 | 1 | 2 | 8 | 2 | **4.0** | — |
| 21 | **WAL segmentation + retention** (evidence preservation) | Storage | 5 | 3 | 3 | 4 | 15 | 4 | **3.75** | Q1 decision |
| 22 | Extended `GraphStats` | Core | 1 | 1 | 2 | 4 | 8 | 2 | **4.0** | — |
| 23 | Sorted delta adjacency | Indexing | 0 | 0 | 2 | 3 | 5 | 2 | **2.5** | — |
| 24 | **RBAC role model + admission** | Core | 3 | 5 | 2 | 3 | 13 | 4 | **3.25** | actor id |
| 25 | Skew benchmark (H1) → per-type adjacency (H2) | Indexing | 2 | 0 | 1 | 3 | 6 | 3 | **2.0** | — |
| 26 | **Redaction primitive (crypto-erasure)** | Provenance | 4 | 3 | 3 | 3 | 13 | 5 | **2.6** | audit, attestation |
| 27 | Lean 4 — Merkle inclusion soundness | DX | 3 | 4 | 3 | 2 | 12 | 5 | **2.4** | Merkle |
| 28 | Viz: export engine result types with highlighting | Viz | 1 | 0 | 0 | 1 | 2 | 2 | **1.0** | — |
| 29 | Viz: stable layout | Viz | 1 | 0 | 0 | 1 | 2 | 2 | **1.0** | — |
| 30 | Weighted shortest path (Dijkstra) | Traversal | 0 | 0 | 0 | 1 | 1 | 3 | **0.3** | — |
| 31 | k-hop cache · mmap · parallel BFS · materialized views | Traversal/Storage | 0 | 0 | 1 | 1 | 2 | 4–5 | **≤0.5** | — |

### 14.3 Module priority — recalculated

| Module | v1 status | v2 status | Δ | Driver |
| --- | --- | --- | --- | --- |
| Core Engine | 🟢 DONE | [33mIN PROGRESS[0m | **▼ downgraded** | No actor identity, no RBAC, no audit trail, no attestation surface. Complete as a *graph* engine; incomplete as a *forensic* one (§10.2, T-16) |
| Storage Layer | 🟡 IN PROGRESS | [33mIN PROGRESS[0m | **▲ urgency up** | Now the top-priority module. Owns canonical serialisation, digests, signing, segmentation, and CSR v8 (T-01, T-05, T-17) |
| Indexing | 🟡 IN PROGRESS | [33mIN PROGRESS[0m | ▲ slight | Canonical entry ordering is the gate for everything (§10.4); poisoning is undetectable today (T-07) |
| Traversal & Algorithms | 🟡 IN PROGRESS | [33mIN PROGRESS[0m | **▼ deprioritised** | Weighted paths, caching, and parallelism score near zero on every v2 factor. Correct and sufficient as-is |
| Provenance & Temporal | 🔴 TODO | [31mTODO[0m | **▲▲ sharply up** | Was "the least-built module"; now also the module the whole forensic argument depends on. Chain-of-custody, redaction, and history all live here |
| Visualization | 🟡 IN PROGRESS | [33mIN PROGRESS[0m | **▼ deprioritised** | Zero forensic or integrity impact. One addition worth noting: rendering *verification status* per entity |
| Developer Experience | 🟡 IN PROGRESS | [33mIN PROGRESS[0m | **▲ up** | CI and fuzzing are now security controls (T-18), not hygiene. `SECURITY.md` moves from "polish" to required (§12.6) |
| **Cryptographic Integrity** *(new)* | — | [31mTODO[0m | **new** | Nothing exists (§10.2) |
| **Formal Verification** *(new)* | — | [31mTODO[0m | **new** | Nothing exists; V-05 is executable today |

### 14.4 Recalculated phase plan

Phase numbering is continuous with v1 §6; **Phase F0–F3 are the forensic track**,
interleaved rather than appended, because several items are shared.

| Phase | Theme | Contents | Status |
| --- | --- | --- | --- |
| **F0** | **Determinism & enforcement** *(no crypto yet)* | Canonical serialisation (I-1) · byte-determinism test V-05 · fuzz targets V-14 · CI pipeline · cross-platform runner · `UpdateNodeIndexed` steering · split `disk/store.go` | [31mTODO[0m |
| **F1** | **Identity & provenance metadata** | Actor identity (I-10) · commit seq + timestamp (I-2) · extended `GraphStats` · compaction policy · `cmd/graphene` inspector · TLA+ specs V-02/V-15 | [31mTODO[0m |
| **F2** | **Integrity — one CSR v8, five payloads** | Cryptographic digests · entity version hashes (I-7) · `GATT` section (I-6) · snapshot roots · ordered-key declarations · composite reservation · `cmd/graphene verify` | [31mTODO[0m |
| **F3** | **Attestation & custody** | Ed25519 commit signing · key epochs/rotation · WAL segmentation + retention · segment-root chaining · audit log (DAG D) · attestation export/verify · chain-of-custody links · `SECURITY.md` | [31mTODO[0m |
| **F4** | **Governance & assurance** | RBAC role model · redaction primitive (crypto-erasure) · Lean 4 proofs · external anchoring / co-signing · refinement harness | [31mTODO[0m |
| v1 P2–P5 | Functional / scale / ecosystem | Weighted paths, diffing, viz, mmap, caching, algorithms — **unchanged in content, deprioritised in order** | [31mTODO[0m |

**Exit criteria.** *F0:* two compactions of identical state produce identical
bytes, verified in CI on Linux and Windows. *F1:* every committed transaction
carries a sequence number, a timestamp, and an actor. *F2:* a modified
`graphene.csr` fails an explicit verification pass. *F3:* an attestation can be
exported and verified by a third party holding only the public key and the
published root. *F4:* redaction is provably shape-preserving and role
transitions are model-checked.

### 14.5 Open questions added by this Part

Numbering continues from v1 §8.

- **Q11 — Do edge hashes bind node *version hashes* or node *IDs*?** Version
  binding is strictly stronger (T-15) but makes a node update O(degree) in hash
  recomputation, which collides head-on with the unaddressed supernode problem
  (v1 §3.3). ID binding is O(1) and weaker. **This is the sharpest cost/assurance
  trade in §11 and it should be decided before any hashing is built.**
- **Q12 — Is external anchoring in scope?** Without it, §12's residual risk
  against A1 and A4 is irreducible. With it, the engine acquires a network
  dependency it has never had. **Recommendation: define the anchoring
  *interface*, ship no transport.** That keeps zero-dependency intact and lets
  the caller choose.
- **Q13 — Does the zero-dependency constraint outrank BLS aggregate
  signatures?** §10.6 says it should. Needs an explicit ruling, because it is
  the only listed primitive Go's stdlib cannot supply.
- **Q14 — Is RBAC attribution or enforcement?** In-process it can only be
  attribution (T-16). If enforcement is genuinely required, that implies a
  process boundary and contradicts v1 §1.3's stated architecture — a much larger
  decision than an RBAC feature.
- **Q15 — What is the evidence retention period?** v1 Q1 framed retention as a
  performance/temporal choice. It is now also a legal and evidentiary one, and
  the two may disagree. Chain-of-custody completeness (PO-5) is undefined until
  this is answered.
- **Q16 — Where do signing keys live?** In-process (fast, and inside the TCB),
  OS keystore, or HSM/PKCS#11 (breaks zero-dependency). Determines the real value
  of every signature in §11.

### 14.6 Risk register — v2 amendments

| ID | v1 rating | v2 rating | Change |
| --- | --- | --- | --- |
| R-C3 — CSR body corruption undetected | Low × High | **High × High** | §10.3: an adversary is not a random process. Now **Critical** |
| R-O1 — unbounded delta/WAL growth | High × High | High × High | Unchanged in score; gains a forensic dimension — uncontrolled `Compact` destroys evidence (T-10) |
| R-M3 — no CI | High × High | High × High | Unchanged; now also the enforcement point for every §13 invariant |
| R-S5 — no `LICENSE` | — × High | **Closed** | Resolved at `f6e1d7b` (§10.1) |
| R-DEP-2 — zero dependencies | Low × Low | **Low × High** | Now load-bearing: it decides the algorithm set (§10.6) |
| **R-F1** *(new)* — non-deterministic serialisation | — | **High × High** | Blocks every cryptographic item (§10.4, T-17) |
| **R-F2** *(new)* — CRC32 mistaken for a tamper control | — | **Med × High** | Documentation risk as much as a technical one |
| **R-F3** *(new)* — no identity at any layer | — | **High × High** | Nothing can be signed or attributed (§10.2, T-16) |
| **R-F4** *(new)* — CSR v8 shipped without reserving crypto space | — | **Med × High** | Forces a v9 and a migration; entirely avoidable (§11.6) |
| **R-F5** *(new)* — overstating the guarantee | — | **Med × High** | Claiming tamper-*proof* rather than tamper-*evident* is the most damaging error available here (§11.1, §12.6) |

### 14.7 Critical path

```
canonical serialisation ──► digest ──► entity hashes ──► Merkle roots ──► attestation
        ▲                                                      ▲
        │                                                      │
   V-05 (fails today)                          actor identity ─┘
                                                      │
                                                      └──► audit log ──► chain of custody
                                                      └──► RBAC attribution

   commit seq + timestamp ──► temporal attestation ──► custody completeness (PO-5)
   WAL segmentation ────────► segment roots ─────────► deletion-provability (T-13/T-14)
   CSR v8 (ONE bump: digest ‖ entity hashes ‖ GATT ‖ ordered decls ‖ composite reservation)
   CI ──► every claim any of the above makes
```

### 14.8 Top fifteen, recalculated and dependency-ordered

| # | Item | Marker | Driver |
| --- | --- | --- | --- |
| 1 | Canonical serialisation of index entries and records | [31mTODO[0m | R-F1 · T-17 · gates everything |
| 2 | Byte-determinism test (V-05) — **fails on `HEAD` today** | [31mTODO[0m | PO-2 · turns §10.4 into a red test |
| 3 | CI pipeline (Linux + Windows, `-race`) | [31mTODO[0m | R-M3 · enforces every invariant below |
| 4 | Steer callers to `UpdateNodeIndexed`; `ReindexPurge` as forensic default | [31mTODO[0m | R-C2 · T-08 · **best benefit/cost in the plan** |
| 5 | Fuzz targets — WAL replay, CSR deserialisation, comparators | [31mTODO[0m | T-18 · now a security control |
| 6 | Actor identity plumbing (`TxContext`) | [31mTODO[0m | R-F3 · T-16 · prerequisite for all signing |
| 7 | Commit sequence + wall-clock timestamp in the commit record | [31mTODO[0m | INV-4 · promoted from v1 Phase 3 |
| 8 | Cryptographic digest beside CRC32 (WAL + CSR bodies) | [31mTODO[0m | R-C3 (re-rated Critical) · T-01 · T-05 |
| 9 | Split `disk/store.go` | [31mTODO[0m | R-M1 · marshal/compact/verify all gain crypto |
| 10 | TLA+ specs — WAL replay atomicity, lock-free CSR read | [31mTODO[0m | PO-1 · INV-7 · **pays off on the current tree alone** |
| 11 | CSR v8 — one bump carrying all five payloads | [31mTODO[0m | R-F4 · avoids a forced v9 migration |
| 12 | Ed25519 commit signing (`SignCommit`, key epochs) | [31mTODO[0m | T-01 · T-02 · the core tamper-evidence control |
| 13 | Append-only audit log (DAG D) | [31mTODO[0m | T-13 · nothing is recorded today |
| 14 | WAL segmentation + retention + segment-root chaining | [31mTODO[0m | T-14 · Q15 · makes deletion provable |
| 15 | Attestation export/verify + `SECURITY.md` stating the honest guarantee | [31mTODO[0m | §11.1 · R-F5 · what makes evidence transferable |

**Dropped out of the v1 top ten:** weighted shortest path (v1 #9 → v2 #30,
`P = 0.3`) — a real functional gap with no forensic, security, or integrity
impact. **Resolved:** `LICENSE` (v1 Q6 / R-S5). **Promoted from v1 Phase 3:**
commit sequence and timestamps, now a Phase F1 prerequisite rather than a
temporal-feature item.

---

## Change Log for This Plan *(continued)*

| Date | Revision | Change |
| --- | --- | --- |
| 2026-07-30 | v2 | **Appended Part II (§10–§14).** Re-audited at `HEAD` = `f6e1d7b` (one commit past v1's `6317181`) via read-only git operations. Recorded the `LICENSE` resolution (closes v1 Q6, R-S5, L5). Established the cryptographic baseline: **zero** crypto surface, zero identity surface, CRC32 re-classified as an accident detector rather than a tamper control. **Discovered that `SerialiseWithIndex` is byte-non-deterministic** (Go map iteration in `postings.forEachAll`), which blocks every hash-based construction and is now the plan's first item. Added four-DAG Merkle design, Ed25519/ECDSA/BLS selection under the zero-dependency constraint, and attestation design — all six directive templates instantiated. Added an 18-entry threat register across 6 adversary classes and 6 attack surfaces, and a 16-hook verification register across 8 invariants and 7 proof obligations. Recalculated priority across all modules on `P = (FS+SEC+DI+RR)/C` with dependency gating; Core Engine downgraded from DONE, Traversal and Viz deprioritised, Provenance sharply promoted, two new modules added. R-C3 re-rated Low→**High** likelihood under an adversary model; five new risks (R-F1–R-F5) and six new open questions (Q11–Q16) registered. No repository code was read for modification, no patch was produced, and no write operation to the repository is proposed. |

> **Maintenance note (v2).** Part II is additive and self-contained: §1–§9 were
> not edited. Where the two Parts disagree, **Part II governs** and says so
> at the point of disagreement (§10.1, §14.3, §14.6). §14 is the current
> priority authority; v1 §9 remains the record of the pre-forensic assessment.

---

# Part III — Build Log

> **Append-only, like the rest of this file.** One entry per landed change.
> Records what was built, what was measured, and where the implementation
> deviated from the plan — so a later reader sees the decision and its evidence
> rather than only the outcome. Parts I and II are unedited.

---

## BL-1 — F0 · Canonical serialisation (plan items I-1, V-05)

**Date:** 2026-07-31 · **Base:** `f6e1d7b` · **Status:** [32mDONE[0m

### What landed

| File | Change |
| --- | --- |
| `disk/csr_determinism_test.go` | **new** — three tests pinning byte-determinism of the serialised CSR |
| `index/property_index.go` | `NodeEntries` / `EdgeEntries` now enumerate in a canonical order; output pre-sized from the shards' cached counts |
| `index/entries_bench_test.go` | **new** — `BenchmarkNodeEntries` covers the enumeration path compaction depends on; `BenchmarkNodeEntriesOrdering` runs the landed ordering against the rejected one in a single binary, so the design choice below stays checkable |

### The failure, reproduced before it was fixed

V-05 was written first and failed on `HEAD`, exactly as §13.5 predicted:

```
two compactions of an unchanged store produced different bytes.
first difference at byte 6023 of 30126, in the index section (GIDX)
(indexOffset=6011): 0x13 vs 0x64
```

Byte 6023 is twelve bytes past `indexOffset` — `"GIDX"` (4) plus the entry count
(8) — so the divergence is at the **very first index entry**, and the entire
6 011-byte record section is byte-identical. That confirms §10.4's diagnosis and
also corrects one of its claims:

> **Correction to §10.4.** That section listed "delta → CSR record ordering in
> `Compact`" as a determinism suspect, on the grounds that `Compact` collects
> delta records by ranging over `s.deltaNodes` / `s.deltaEdges`
> (`disk/store.go:2111-2123`), which are maps. **It is not a defect.** `Build`
> scatters records into an ID-indexed array (`g.nodes[n.ID] = n`,
> `disk/csr.go:114-121`) and `Serialise` walks that array by index
> (`disk/csr.go:478`), so record order is ascending by ID regardless of the order
> records were collected in. The map range is invisible in the output. The index
> section was the sole byte-level non-determinism.

The two remaining 🟣 items from §10.4 — label-slice ordering and `float32`
weight canonicalisation — are untouched by this change and remain open. Neither
fires on the fixtures used here, so neither is currently falsifiable; they need
a fixture that provokes them before they can be called defects or non-issues.

### Deviation from the plan: the canonical order is (Key, Value, ID), not (ID, Key, Value)

§11.6 I-1 specified sorting by `(ID, Key, Value)`. The implementation orders by
**`(Key, Value, ID)`**. The reason is measured, not aesthetic.

ID-major order is a *transpose* of how the index is laid out. `postings` nests
key → value → ID-ascending postings list, so ID-major ordering requires a full
sort and a permutation in which every entry lands in a random slot.

The two candidate orderings are compared by `BenchmarkNodeEntriesOrdering`,
which runs both over **one fixture in one process** (50 000 nodes, 100 000
entries). That is the right shape for an algorithm choice: comparing two
implementations as two binaries makes the result hostage to machine drift, which
is what went wrong the first time this was measured (see the note below). Four
rounds, spread ≤ 6 %:

| Implementation | Time | Memory | Verdict |
| --- | --- | --- | --- |
| **Walk the index in its own nesting order** → `(Key, Value, ID)` (landed) | **15.46 ms** | 10.74 MB | one sort of the keys, one of each key's distinct values; postings are already ID-ascending, so entries emit straight into place |
| `slices.SortFunc` over `[]NodePropEntry` → `(ID, Key, Value)` | 23.58 ms | **6.40 MB** | the obvious implementation; 48-byte structs, so every swap moves six words and every compare chases `Key`/`Value` pointers into scattered heap |

**This is a trade, not a free win.** The landed order is **1.53x faster** and
costs **68 % more memory** than sorting entries directly — the values slice it
builds per key is the price of not permuting. It is the correct choice under the
optimisation priority `CONTRIBUTING.md` states (speed before memory before
allocations), and it would be the wrong choice under the opposite ordering. A
third implementation, a packed `(id, idx)` sort with a permutation pass, was
tried to get ID-major order cheaply and abandoned: it removes the fat-struct
swaps but the permutation still does 100 000 random reads over ~5 MB, and it
measured slower than both arms above.

The rejected arm is kept in the tree as `nodeEntriesByIDReference`, so the claim
stays checkable rather than remembered.

End-to-end on `BenchmarkCompactSteadyState` (50 000 nodes, 100 000 indexed
entries), interleaved A/B against a `f6e1d7b` worktree, warm-up discarded, five
rounds:

| | Control (`f6e1d7b`) | Landed | Δ |
| --- | --- | --- | --- |
| Time | 38.68 ms/op | 43.43 ms/op | **+12.3 %** |
| Memory | 96.70 MB/op | 78.02 MB/op | **−19.3 %** |
| Allocations | 100 211 | 100 203 | ~unchanged |

And on the enumeration path alone (`BenchmarkNodeEntries/nodes=50000`, six
interleaved rounds, spread 6 % on both arms): control 13.21 ms / 29.42 MB,
landed 18.03 ms / 10.74 MB — **+36.4 % time, −63 % memory**.

**The trade, stated plainly:** compaction costs ~12 % more time and ~19 % less
memory, and in exchange the CSR image becomes byte-reproducible. Pre-sizing the
result from the shards' cached `count` is what pays for the memory — the
previous `append`-growth path copied the whole slice ~log₂(n) times on the way
up, ~19 MB of pure copying per compaction, and that was there before this
change. It is an independent win that happens to land in the same commit.

> **Measurement note — the first pass at these numbers was wrong, and how.**
> This change was initially measured while the machine was progressively loading
> under back-to-back benchmark runs. The *control* drifted 13 → 27 → 30 ms for
> the identical binary across one session, and a round-1 outlier was discarded
> by judgement rather than by a warm-up. Those conditions produced "+53 % on
> `NodeEntries`" and "+20 % end-to-end"; re-measured on a settled machine with a
> discarded warm-up, the true figures are **+36.4 %** and **+12.3 %**. The
> direction was right and every conclusion survived, but the magnitudes were
> inflated by roughly half.
>
> `CONTRIBUTING.md` already forbids comparing across sessions and mandates a
> control. That was followed and was still not sufficient — interleaving
> protects against drift *between* the two arms, not against a control whose own
> absolute cost is doubling underneath the experiment. The additional rules this
> episode argues for: **discard a warm-up round**, **report the control's own
> spread** (6 % here, ~76 % in the bad pass) and treat a wide one as a failed
> measurement, and **compare two candidate algorithms inside one binary** rather
> than as two checkouts. Worth folding into `CONTRIBUTING.md`'s benchmarking
> section (§14.4 F0).

**What this costs downstream.** §11.3.1 argued ID-major order "groups an
entity's entries contiguously, which is what a future per-entity digest wants to
read". That convenience is given up. It is not a blocker: a per-entity digest
has the reverse map (`ID → refs`, already keyed by ID) available, and the
entity-history DAG (§11.2 DAG A) hashes an entity's *own* record, not this
enumeration. If a per-entity digest later proves to want ID-major order badly
enough to pay ~2.2x for it, that is a decision to revisit with its own
measurement — it is not free, and the number above is what it costs.

### Consequences for the plan

| Plan item | Effect |
| --- | --- |
| §14.8 #1 — canonical serialisation | [32mDONE[0m |
| §14.8 #2 — byte-determinism test (V-05) | [32mDONE[0m — now green, and guards the property going forward |
| §13.6 Tier 1 — byte-determinism test | [32mDONE[0m |
| §12.4 T-17 — recompacted vs tampered indistinguishable | **Closed.** Digest comparison is now a valid tamper check |
| §14.6 R-F1 — non-deterministic serialisation (High × High) | **Closed** |
| §11.8 — canonical serialisation prerequisite | Satisfied; unblocks digests, snapshot roots, and attestation |
| §10.4 — delta → CSR record ordering | **Withdrawn as a suspect** — proven a non-issue (above) |
| §10.4 — label ordering, `float32` weight | [35mNEEDS INVESTIGATION[0m — unchanged, needs a provoking fixture |

**Ordering stability is now a format property.** Two compactions by the *same
build* agree. Agreement across *builds* additionally requires that the ordering
rule never changes silently — so any future change to it is a CSR format
version bump, not an implementation detail. This is the same discipline
`TestComputeCRC32Vectors` already applies to the checksum, and it should be
stated in the format documentation when CSR v8 is specified (§14.4 F2).

### Verification

`go vet ./...` clean. `go test ./... -race -count=1` green across all nine
packages. `go test . -tags=stress -race` green. The three new determinism tests
fail on `f6e1d7b` and pass on the landed change — the property is pinned, not
merely satisfied.

Benchmarks were re-run after the measurement error described above: warm-up
discarded, control spread 6 % on the enumeration path and 17 % end-to-end, and
the ordering comparison moved into a single binary. The figures in this entry
are from that second pass.

### Not done

Nothing in this entry addresses digests, signing, or identity. It removes the
blocker; §14.8 #3 (CI) and #4 (`UpdateNodeIndexed` steering) are the next items
and neither depends on this one.

---

## BL-2 — F0 · CI pipeline and index-staleness steering (§14.8 #3, #4)

**Date:** 2026-07-31 · **Base:** `f6e1d7b` + BL-1 · **Status:** [33mIN PROGRESS[0m *(workflow unverified until pushed — see below)*

### What landed

| File | Change |
| --- | --- |
| `.github/workflows/ci.yml` | **new** — four jobs: `lint`, `test` (Linux + Windows matrix), `bench-smoke`, `stress-deep` (nightly) |
| `examples/mutation_examples.go` | Mutation 3 rewritten to demonstrate the stale-index failure *and* the fix |
| `store/interface.go` | `UpdateNode` / `UpdateEdge` contract docs name the consequence and point at the indexed variants |
| `disk/wal.go`, `store/query_types.go` | gofmt alignment only — 7 insertions, 7 deletions, no behaviour change |

### CI shape, and why each job is drawn where it is

| Job | Platform | Runs |
| --- | --- | --- |
| `lint` | Linux only | `gofmt -l`, `go vet ./...` |
| `test` | **Linux + Windows** | `go build ./...`, `go test ./... -race -count=1`, `go test . -tags=stress -race -run Test` |
| `bench-smoke` | Linux | `go test . ./index/ -tags=stress -bench=. -benchtime=1x -run='^$'` |
| `stress-deep` | Linux, nightly + manual | `GRAPHENE_PERSISTENT_STRESS=1`, 50 min timeout |

Four decisions worth recording:

1. **`lint` is Linux-only.** `gofmt -l` flags every file on a Windows checkout
   that has CRLF in the working tree, which is noise rather than signal. See the
   CRLF finding below.
2. **The stress suite runs on every push, not on a schedule** as §14.4 F0
   proposed. Its default path is 100k nodes / 500k edges and finishes in ~12 s
   under `-race`; the expensive 1M-node path is separately gated behind
   `GRAPHENE_PERSISTENT_STRESS` (`graphene_stress_test.go:374`). Since the suite
   is behind a build tag, `go test ./...` never compiles it — so without this it
   could rot unnoticed, which is the failure mode a schedule would catch late.
   The deep path keeps the nightly slot.
3. **Benchmarks are a compile-and-run smoke check, never a gate.** `-benchtime=1x`
   runs each once. Asserting on CI benchmark *timings* would be indefensible
   after BL-1: the measurement there needed a settled machine, a discarded
   warm-up, and a control spread under 6 % before it meant anything, and none of
   that is available on ephemeral shared runners. The check catches the real
   risk, which is a benchmark that no longer compiles or panics.
4. **`go-version-file: go.mod`** rather than a pinned version, so the toolchain
   floor has exactly one source of truth (currently `go 1.26`).

### Finding: 31 files looked unformatted; 2 actually were

`gofmt -l .` on the Windows working tree reported **31 files**. That is almost
entirely an artefact: `core.autocrlf=true` and no `.gitattributes`, so git stores
LF and checks out CRLF, and gofmt objects to every line of every file.

Re-running gofmt against the blobs **as git stores them** narrowed 31 to **2**
genuine cases, both pure `const`-block alignment:

- `disk/wal.go` — `walRecordCheckpoint` misaligned against its neighbours
- `store/query_types.go` — the `DriverKind` block's trailing comments

Both were fixed by hand rather than with `gofmt -w`, so the working tree's line
endings were left alone and the diff is 7 lines each way instead of a whole-file
rewrite. Without this the `lint` job would have failed on its first run.

> **Method note.** The useful check on a Windows checkout is not `gofmt -l .` but
> gofmt over LF-normalised content — `cat f | tr -d '\r' | gofmt -d`. Worth
> stating in `CONTRIBUTING.md` alongside the benchmarking rules from BL-1, since
> a contributor who runs `gofmt -w .` here would produce a 31-file diff of pure
> line-ending churn. Adding `*.go text eol=lf` to a `.gitattributes` would remove
> the whole class, at the cost of changing every Windows contributor's working
> tree — **not done, deliberately; it is the repository owner's call.**

### §14.8 #4 — steering, not new API

R-C2 is the highest-likelihood wrong-answer path in the engine and scored the
best benefit/cost ratio in §14.2 (`P = 14.0`) precisely because nothing needs
building: `UpdateNodeIndexed` / `UpdateEdgeIndexed` already exist
(`graphene.go:207,221`), and their doc comments are already good. The audit
turned up where the steering actually fails:

- `UpdateNodeIndexed` and `UpdateEdgeIndexed` appear in `docs/` 11 times and in
  `README.md` once — and **zero times in `examples/`**.
- `examples/mutation_examples.go` Mutation 3 demonstrated the trap *neutrally*.
  It indexed `status=suspect`, called `UpdateNode`, indexed `status=confirmed`,
  and reported "both linger after update — the index is additive across
  UpdateNode" as a semantic to be understood rather than a defect to be avoided.
  A reader copying that pattern gets a store where
  `NodesByProperty("status", "suspect")` returns a node whose status is
  `confirmed`, and the planner trusts it.

Mutation 3 now runs both paths and prints the difference:

```
UpdateNode        -> suspect:1 confirmed:1  <- WRONG: 'suspect' is stale and still matches
UpdateNodeIndexed -> suspect:0 confirmed:1  <- index agrees with the node
after DeleteNode  -> suspect:0 confirmed:0  (all entries purged)
```

The delete-purges-everything lesson the original example taught is kept; what
changed is that the wrong answer is now labelled as one and the correct API is
demonstrated beside it.

`store.GraphStore.UpdateNode`'s contract doc was also sharpened — it previously
said callers "should re-index explicitly", which is true but understates it. It
now states that the consequence is a wrong answer rather than a slow one, that
registering a new value does not displace the old one, and when plain
`UpdateNode` is nonetheless the right call.

`examples/main.go:685` also calls `UpdateNode`, and was checked and left alone:
that node carries no index entries, so plain `UpdateNode` is correct there.

### What is *not* verified

**The workflow has never executed.** Every command it runs was verified locally
on Windows — `gofmt` clean over LF-normalised content, `go vet ./...` clean,
`go build ./...`, `go test ./... -race -count=1` green across all nine packages,
`go test . -tags=stress -race -run Test` green in ~12 s, and the `bench-smoke`
invocation verbatim — and the YAML parses. What cannot be checked from here is
the runner wiring: action versions, cache behaviour, and in particular whether
`-race` works on `windows-latest` without extra setup (it needs cgo and a C
toolchain; the GitHub Windows image ships mingw-w64, so it is expected to work,
but expected is not observed). **First push is the real test of this file**, and
the Windows race step is the most likely thing to need adjustment.

### Consequences for the plan

| Plan item | Effect |
| --- | --- |
| §14.8 #3 — CI pipeline | [33mIN PROGRESS[0m — written and locally verified; green run still owed |
| §14.8 #4 — `UpdateNodeIndexed` steering | [32mDONE[0m |
| §14.6 R-M3 — no CI (High × High) | Downgraded, not closed, until a run is green |
| §12.4 T-08 — stale-index exploitation | Mitigated on the documentation axis. The engine still cannot detect it — the blob is opaque — so this stays open as a *residual*, exactly as §12.6 classifies it |
| §14.8 #5 — fuzz targets | Next. CI now exists to run them |
| §14.4 F0 — `CONTRIBUTING.md` additions | Two method notes now owed: BL-1's benchmarking rules and BL-2's gofmt/CRLF rule |

**Not addressed:** `ReindexPurge` as a forensic-profile default. §14.2 pairs it
with the steering work, but it changes runtime behaviour rather than
documentation, and choosing it as a default trades one failure mode (stale
entries) for another (lost entries) — `store/interface.go:127-140` documents
both honestly. That is a policy decision for the repository owner, not a
docs fix, and it is left open.

---

## BL-3 — F0 · Fuzz targets, and the three parser defects they found (§14.8 #5)

**Date:** 2026-07-31 · **Base:** `f6e1d7b` + BL-1 + BL-2 · **Status:** [32mDONE[0m *(WAL target under-powered — see Limitations)*

### What landed

| File | Change |
| --- | --- |
| `disk/fuzz_test.go` | **new** — `FuzzDeserialiseCSR`, `FuzzWALReplay`, plus three explicit bounds regression tests |
| `store/fuzz_test.go` | **new** — `FuzzParseNodeType`, `FuzzParseEdgeType`, `TestParseType_RejectionsAreUsable` |
| `disk/store.go` | bounds on header-declared counts, entity IDs, and edge endpoints; `checkCSREntityIDs`; `maxCSREntityID` |
| `disk/wal.go` | replay rejects a record longer than the log before allocating its payload |
| `store/types.go` | `NodeType.String` / `EdgeType.String` render an unnamed reserved value as its number |
| `.github/workflows/ci.yml` | nightly `fuzz` job, 5 min per target, uploads any crash reproducer as an artifact |
| `disk/testdata/fuzz/FuzzDeserialiseCSR/*` | **two retained crash inputs** — now permanent regression tests |

### Three defects, all reachable from files under 110 bytes

§12.4 T-18 predicted this class and rated severity as "panic/DoS, limited by Go's
memory safety". That was right about the ceiling and understated the reach: every
one of these is triggered by a file too small to contain a graph.

**D-1 — unbounded allocation from header counts.** `deserialiseCSR` read
`nodeCount` and `edgeCount` as `uint64`, narrowed to `int`, and passed them
straight to `make()`. A 46-byte header claiming 2⁴⁰ nodes allocated until the
process died; one claiming more than `MaxInt64` narrowed negative and panicked
inside `makeslice`. The same pattern appeared twice more in
`readCSRIndexSection`'s length prefixes. **Found by inspection while writing the
seed corpus**, not by the fuzzer — a fuzz target that OOMs its host is not one
worth running, so it was bounded first.

**D-2 — unbounded allocation from record IDs.** *Found by the fuzzer, ~3 s in.*
Bounding the counts is not sufficient, because `Build` sizes its arrays by
**maximum ID**, not by record count (`disk/csr.go:107`). Two records carrying IDs
of `0x3030303030303030` make a 105-byte file demand an exabyte-scale slice.
The counts were in range; the IDs were not.

**D-3 — edge endpoints indexed without bounds.** *Found by the fuzzer, ~45 s in,
after D-2 was fixed.* `Build` sizes the adjacency offset arrays from the highest
*node* ID and then indexes them by each edge's `Src`/`Dst` unchecked
(`disk/csr.go:125`), so an edge naming a node the file does not contain reads
past the end of the array: `index out of range [3472328296227680305] with length
3158066`. Distinct from D-2 and not covered by it.

**D-4 — WAL replay payload length.** The per-record length is a `uint32` read
straight from the log and handed to `make()`. A five-byte header claiming
`0xFFFFFFFF` demanded 4 GiB before reading any of it. Replay now treats a record
longer than its own log as a torn tail — the same disposition it already gives a
CRC mismatch, because a crash mid-header produces exactly this byte pattern.

#### On the bound for D-2

The check is in `checkCSREntityIDs`, and it deliberately does **not** bound IDs
by record count. IDs are monotonic and never reused (v1 §3.1, R-CE-2), so a
long-lived store that has deleted heavily legitimately has a maximum ID far above
its live count. Two rules instead:

- **IDs must not exceed the file's own sequence high-water marks** (v5+). Exact,
  free, and a real format invariant — `Compact` stamps those counters and IDs are
  drawn from them. A zero mark is read as *unstamped* rather than *max ID is
  zero*, because a `CSRGraph` serialised straight out of `Build` carries zeros
  and those files are valid; `TestCSRLayout_LastRecordEndingAtSectionBoundaryParses`
  caught that on the first attempt at this rule.
- **`maxCSREntityID` (2²⁸) as a backstop**, applied at every version, because the
  high-water marks live in the same header an attacker controls.

**The backstop is policy, and it is flagged as such in the code.** 2²⁸ caps the
node array near 15 GB — far above the engine's stated workload, far below what a
raw `uint64` permits. The defensible number follows from the maximum graph size
the engine intends to support, which is **still open question Q2**. It belongs
there, not in a constant chosen here.

### A fourth finding, from the round-trip property

The type-selector fuzzers assert that a selector which parses must survive
`Parse → String → Parse` with its value intact. That failed in seconds on `"7"`:

```
round-trip changed the value: "7" -> 7 -> "Unknown" -> 0
```

`ParseNodeType` documents accepting bare numerics (`"130" -> NodeType(130)`,
`store/types.go:70`), and `String()` renders the custom range as `Custom(N)` —
but any *reserved* value without a name returned the bare string `"Unknown"`.
So node type 7 and node type 130 printed identically, everywhere a label is
shown: query plans, error messages, visualization legends.

This is information loss rather than a crash, and it is the kind of thing a
round-trip property finds and an example-based test never would. `String()` now
renders an unnamed reserved value as its number, which round-trips and keeps the
types distinguishable. `NodeTypeUnknown` (0) still prints `"Unknown"` — the
existing assertions in `store/types_test.go` were the constraint, and they pass
unchanged.

### Limitations

**`FuzzWALReplay` is under-powered and should not be read as equivalent
coverage.** `Replay` reads from `*os.File`, so every candidate input has to be
written to disk and opened. At `t.TempDir()` per iteration it managed **1 927
executions in 60 s**; hoisting to one reused path per worker process got it to
**8 994** — against **3 144 525** for `FuzzDeserialiseCSR` over 90 s, which is
in-memory. Three orders of magnitude apart.

That matters because WAL replay is the *more* security-critical of the two
parsers: T-01, T-02, and T-03 all target it, and D-4 was found by reading rather
than by fuzzing precisely because the target cannot explore.

The fix is to extract replay's parse loop behind an `io.Reader` so it can be
fuzzed in memory. That is a change to a delicate, heavily-reasoned function
(`disk/wal.go:237`) for testability, which is legitimate but deserves its own
pass rather than being folded into this one. **Recommended as the next item**,
and it is also the natural precondition for V-02's TLA+ refinement harness,
which needs to drive replay from a generated trace rather than a file.

### Consequences for the plan

| Plan item | Effect |
| --- | --- |
| §14.8 #5 — fuzz targets | [32mDONE[0m — four targets, wired into CI nightly |
| §13.4 V-14 — fuzz WAL replay, CSR deserialisation, type selectors | [32mDONE[0m for CSR and selectors; [33mIN PROGRESS[0m for WAL, per Limitations |
| §12.4 T-18 — parser-level attack | Four instances closed. Severity assessment confirmed: panic/DoS, no memory-unsafety |
| §13.2 INV-8 — comparator totality | Untouched. The property comparators were not fuzzed this pass |
| §8 Q2 — intended maximum graph size | **Now has a dependent.** `maxCSREntityID` is a placeholder for the real answer |
| §14.4 F0 | Fuzzing complete bar the WAL refactor |

**A note on what fuzzing bought.** Three of the four defects were in code that
every existing test exercised and passed — the parity suites, the integrity
suites, and the pinned format vectors all read files the engine itself wrote.
None of them ever handed the parser a file it would refuse, which is exactly the
input an adversary supplies. The seed corpora are the reason the targets work at
all: random bytes never form a valid `GCSR` header, so without real images to
mutate the fuzzer spends its whole budget being rejected at the magic check.
