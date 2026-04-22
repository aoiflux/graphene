# Core Graph Database Feature Comparison

**Graphene** vs **Cayley** vs **Neo4j**

> Scope: core graph database features only — data model, storage, traversal,
> indexing, write model, crash safety.

---

## 1. Data Model

| Feature                               | Graphene                                                                   | Cayley                                                       | Neo4j                                          |
| ------------------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------ | ---------------------------------------------- |
| Graph model                           | Typed property graph                                                       | RDF quad store                                               | Labeled property graph (LPG)                   |
| Node classification                   | N labels per node (`[]NodeType`, uint8 enum values)                        | Blank node, IRI, or literal as subject                       | Zero or more string labels per node            |
| Edge classification                   | N labels per edge (`[]EdgeType`, uint8 enum values) + `float32` weight     | Predicate (IRI) — edges are triples, not first-class objects | Exactly one relationship type per relationship |
| Edge direction                        | Directed; queries can treat as undirected                                  | Directed (subject → object via predicate)                    | Directed; Cypher queries can ignore direction  |
| Properties on nodes                   | Opaque msgpack blob                                                        | No native per-node properties; encoded as additional triples | Arbitrary key-value map (typed values)         |
| Properties on edges                   | Opaque msgpack blob                                                        | No native per-edge properties; same workaround as nodes      | Arbitrary key-value map (typed values)         |
| Multiple edge types between same pair | Yes                                                                        | Yes (different predicates)                                   | Yes                                            |
| Self-loops                            | Yes                                                                        | Yes                                                          | Yes                                            |
| Domain schema                         | Fixed compile-time enums; nodes/edges carry multiple labels simultaneously | None — fully schemaless (anything is a valid quad)           | Optional — schema enforced via constraints     |

**Key difference:** Cayley's model is fundamentally RDF
(subject-predicate-object-label quads). Properties are not first-class; they
must be expressed as extra triples, which is verbose for SYNTHRA's artefact
records. Neo4j and Graphene both use property graph models, but Neo4j's
properties are richly typed while Graphene intentionally keeps properties as
opaque blobs to stay schema-agnostic — the CRL schema lives above the storage
layer. Unlike Neo4j (exactly one relationship type per edge) and Cayley (one
predicate per triple), Graphene allows both nodes and edges to carry N labels
simultaneously, matching how a single micro-artefact can play multiple forensic
roles at once.

---

## 2. Storage Model

| Feature                  | Graphene                                                                         | Cayley                                                                   | Neo4j                                                                                      |
| ------------------------ | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| Storage format           | Custom CSR (Compressed Sparse Row) flat arrays                                   | Pluggable backend: in-memory, BoltDB, LevelDB, PostgreSQL, MongoDB, etc. | Native graph store files (node store, relationship store, property store)                  |
| Adjacency representation | Outbound + inbound CSR arrays; O(degree) sequential read per neighbourhood query | Depends on backend; no native adjacency structure                        | **Index-free adjacency** — each record holds a pointer to its relationship chain; O(1) hop |
| Write path               | Append to WAL → delta layer (in-memory map)                                      | Write directly to chosen backend                                         | Transactional write to WAL, then store files                                               |
| Read path                | Merge CSR + delta layer                                                          | Backend read                                                             | Store file read via adjacency pointer chain                                                |
| Compaction               | Explicit `Compact()` — rebuilds CSR from CSR + delta, atomic rename              | No concept; backend handles it                                           | Page cache, periodic checkpoint                                                            |
| In-memory mode           | Yes (reference impl)                                                             | Yes (mem backend)                                                        | Yes (embedded / test mode)                                                                 |
| Property storage         | Co-serialised with node/edge records in WAL and CSR                              | Encoded as additional quads in backend                                   | Separate property store file; linked from node/rel records                                 |
| mmap                     | Designed for (CSR serialised to flat byte slice)                                 | Depends on backend (LevelDB/BoltDB do mmap internally)                   | Yes — page cache backed by mmap                                                            |

**Key difference:** Neo4j's index-free adjacency means a single relationship hop
touches only the records on that chain, making per-hop cost independent of graph
size. Graphene's CSR achieves the same sequential-access property for bulk-read
workloads after compaction, but requires an explicit `Compact()` call to
materialise. Cayley does not own its storage — it delegates entirely to an
external backend, which means adjacency traversal efficiency is determined by
that backend's data layout.

---

## 3. Query Interface

| Feature               | Graphene                                     | Cayley                                                      | Neo4j                                           |
| --------------------- | -------------------------------------------- | ----------------------------------------------------------- | ----------------------------------------------- |
| Query language        | None — pure Go API                           | Gizmo (JavaScript / Gremlin-inspired), GraphQL dialect, MQL | Cypher (declarative, SQL-like pattern language) |
| Query REPL / UI       | No                                           | Yes — built-in browser UI and REPL                          | Yes — Neo4j Browser, Bloom                      |
| Ad-hoc queries        | Only programmatically                        | Yes, via REPL and HTTP API                                  | Yes, via Cypher console                         |
| Pattern expression    | Programmatic `Pattern` structs (VF2 matcher) | Gizmo traversal chains                                      | Cypher `MATCH` patterns with arbitrary depth    |
| Parameterised queries | N/A (Go type-safe API)                       | Gizmo functions                                             | Cypher parameters (`$param`)                    |
| Aggregation           | Not built-in (caller aggregates results)     | Limited (via Gizmo)                                         | Full — `COUNT`, `SUM`, `AVG`, `COLLECT`, etc.   |

---

## 4. Traversal Algorithms

| Algorithm                   | Graphene                                                                       | Cayley                                  | Neo4j                                                     |
| --------------------------- | ------------------------------------------------------------------------------ | --------------------------------------- | --------------------------------------------------------- |
| BFS (k-hop neighbourhood)   | Built-in (`traversal.BFS`)                                                     | Via Gizmo `g.V().In/Out().All()` chains | Cypher `MATCH (n)-[*1..k]->(m)`                           |
| DFS                         | Built-in (`traversal.DFS`, `ProvenanceChain`)                                  | Via Gizmo traversal                     | Cypher with path expansion                                |
| Shortest path               | Bidirectional BFS (`traversal.ShortestPath`)                                   | Via Gizmo `shortestPath`                | Cypher `shortestPath()` and `allShortestPaths()` built-in |
| Weighted shortest path      | Not yet (weight available on edges, no Dijkstra)                               | No                                      | Via Graph Data Science (GDS) library                      |
| Subgraph / pattern matching | VF2-inspired backtracking (`traversal.FindSubgraphMatches`) with label pruning | No dedicated pattern matcher            | Native via Cypher `MATCH` — core feature                  |
| Provenance / ancestor walk  | Dedicated `ProvenanceChain` (inbound DFS, cycle-safe)                          | Via Gizmo In() chain                    | Cypher `MATCH (n)<-[*]-(root)`                            |
| Variable-depth path         | Configurable `maxDepth`                                                        | Via recursive Gizmo calls               | Cypher `*` quantifier with range `[*minHops..maxHops]`    |

---

## 5. Indexing

| Index type                         | Graphene                                                                                            | Cayley                                  | Neo4j                                               |
| ---------------------------------- | --------------------------------------------------------------------------------------------------- | --------------------------------------- | --------------------------------------------------- |
| Node type index                    | Yes — `TypeIndex`: O(1) lookup of all node IDs by label; nodes indexed under each of their N labels | Implicit via predicate/label quads      | Yes — label scan via token index                    |
| Edge type index                    | Yes — `TypeIndex`: O(1) lookup of all edge IDs by label; edges indexed under each of their N labels | Implicit via predicate index in backend | Yes — relationship type index                       |
| Temporal / range index             | Yes — `TemporalIndex`: sorted slice, binary-search range queries                                    | No                                      | Yes — B-tree range index on numeric/date properties |
| Full-text search                   | No                                                                                                  | No                                      | Yes — Lucene-backed full-text index                 |
| Vector / similarity index          | No                                                                                                  | No                                      | Yes — vector index (ANN search)                     |
| Property index                     | No — properties are opaque blobs                                                                    | Depends on backend                      | Yes — index on any property key                     |
| Secondary indexes on custom fields | No                                                                                                  | No                                      | Yes — composite and token indexes                   |

---

## 6. Write Model and Crash Safety

| Feature                    | Graphene                                                 | Cayley                                                 | Neo4j                                |
| -------------------------- | -------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------ |
| Write-ahead log (WAL)      | Yes — per-record CRC32, append-only, replayed on restart | Depends on backend (LevelDB/BoltDB have their own WAL) | Yes — transaction log                |
| ACID transactions          | No — single-write atomicity only (per node/edge)         | Depends on backend                                     | Full ACID                            |
| Multi-write transactions   | No                                                       | Depends on backend                                     | Yes                                  |
| Crash recovery             | WAL replay into delta layer on Open()                    | Backend-managed                                        | WAL replay + checkpoint              |
| Compaction / checkpointing | Manual `Compact()` — crash-safe via atomic rename        | Backend-managed                                        | Automatic                            |
| Concurrent write safety    | Mutex-protected delta writes                             | Backend-dependent                                      | Full concurrent transactional writes |
| Optimistic locking         | No                                                       | No                                                     | Yes                                  |

---

## 7. Target Scale and Write Pattern Fit

|                               | Graphene                                            | Cayley                           | Neo4j                                         |
| ----------------------------- | --------------------------------------------------- | -------------------------------- | --------------------------------------------- |
| Stated scale target           | Hundreds of millions of nodes / billions of edges   | Tested at ~134M quads on LevelDB | Billions of nodes; 34B+ nodes/rels tested     |
| Optimised write pattern       | **Bulk ingest then read-heavy** (CSR is built once) | Any pattern (backend-dependent)  | Any pattern (transactional)                   |
| Incremental write overhead    | Low — WAL + delta append; no index rebuild          | Backend-dependent                | Index updates on every write                  |
| Compaction cost               | O(N) one-time rebuild; amortised over bulk ingest   | Background (backend)             | Automatic / transparent                       |
| Read latency after compaction | Sequential CSR slice reads — cache-friendly         | Backend-dependent                | Index-free adjacency pointer chain — very low |

---

## 8. Maintenance Status

|                       | Graphene                                 | Cayley                                                                               | Neo4j               |
| --------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------ | ------------------- |
| Status                | Active (this project)                    | **Unmaintained** — last release v0.7.7 in October 2019; last meaningful commit ~2022 | Actively maintained |
| Language              | Go                                       | Go                                                                                   | Java                |
| Embeddable as library | Yes                                      | Yes                                                                                  | Yes (embedded Java) |
| Dependency footprint  | Zero external dependencies (stdlib only) | ~30 dependencies                                                                     | JVM required        |

---

## Summary

**Use Graphene when:**

- The workload is bulk-ingest (parsing a case file) followed by many read
  queries — CSR is purpose-built for this.
- The graph schema is fixed and known at compile time (SYNTHRA's
  `NodeType`/`EdgeType` enums), and elements can carry multiple labels to
  express composite forensic roles (e.g. a node that is both a `MicroArtefact`
  and an `AntiForensicIndicator`).
- Specific decoded property fields (hash, filename, MIME type) need O(1) lookup
  via the property index without loading and decoding every blob.
- Zero external dependencies matter (forensic air-gapped environments).
- Provenance chains and cross-case pattern matching are the primary query
  patterns.

**Neo4j would add value for:**

- Ad-hoc exploratory Cypher queries during investigation.
- Rich property indexing (arbitrary field queries without explicit
  pre-registration; full B-tree range support).
- ACID multi-step write transactions (e.g. atomic case ingestion rollback).
- Full-text and vector similarity search over properties.

**Cayley is not recommended** for SYNTHRA given its unmaintained state, the RDF
quad model mismatch with the typed artefact domain, and the lack of native
property support.

---

> Graphene's design choices (CSR, typed enum label slices, msgpack blobs,
> explicit compaction) are deliberate trade-offs for the SYNTHRA access pattern:
> parse once, query many times, stay embeddable, and stay schema-agnostic at the
> storage layer while the CRL schema evolves above it.
