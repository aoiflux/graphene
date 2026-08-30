# Research: the native-graph gap

**Status:** research brief, 2026-08-30. Nothing here is built or committed to.
It exists to make the next scope decision on evidence rather than instinct.

**Scope:** what Neo4j actually is, which of its claims survive scrutiny, and what
Graphene would have to build to stand beside it. `comparison.md` is the
feature-by-feature comparison as things stand today; this document is the
forward-looking half and deliberately does not repeat it.

---

## 1. Neo4j is three products, and only one of them is the graph database

Most confused comparisons against Neo4j come from treating it as one system. It
is three, they have different architectures, and they do not share a data
structure.

| Layer | What it is | Its data structure | Where we stand |
|---|---|---|---|
| **Storage engine** | Fixed-size record stores behind a page cache, with a transaction log, ACID transactions and index-free adjacency. This is the part that earns "native graph database" | Node and relationship record files, direct-addressed by ID; relationships as doubly-linked chains per node | **Comparable, and ahead in places.** A CSR image plus a delta layer, epoch-versioned, WAL-backed, group-committed |
| **Cypher engine** | A declarative language whose cost-based planner understands pattern matching and variable-length path expansion as first-class operations | Logical plan → physical operators including `Expand(All)` and `VarLengthExpand` | **Distant.** A typed `NodeQuery` struct and a planner that knows indexes and knows nothing about traversal |
| **Graph Data Science** | A separately licensed library of ~70 algorithms. It does *not* run on the transactional store | An in-memory compressed adjacency list — a CSR — plus columnar node property arrays | **Absent, but well-positioned.** Six traversals, no library. The projection target it needs already exists |

### The finding that reframes the question

**GDS projects into a CSR. Our on-disk image *is* a CSR.**

Neo4j's algorithm story requires copying the graph out of the record store into
a second, purpose-built structure — because a record store whose relationships
are pointer-chased linked lists is the wrong shape for iterating every edge ten
times over. That projection costs real time on a large graph, doubles resident
memory, and produces results against a copy that has no isolation guarantee and
is stale from the moment it is taken.

We skip that step. `CSRGraph` already holds `outOffset`/`outEdges` indexed
directly by `NodeID` — which is what GDS builds at projection time. An algorithm
layer here would run on the live image with no copy, and `store.Snapshot` offers
something GDS cannot: an epoch-named, genuinely consistent view that pins the
image it read. Results could be labelled with the epoch they describe.

That is a structural advantage the project already paid for and has never spent.

---

## 2. Which claims survive scrutiny

| The claim | What is actually true | What it means here |
|---|---|---|
| "Index-free adjacency makes a hop O(1), independent of graph size" | **Half true.** Resolving a record is genuine pointer arithmetic (`offset = id × recordSize`) and is O(1) at any scale. But relationships hang off a node as a *doubly-linked list*, so a type- or direction-filtered traversal walks the chain: O(degree), with a potential page fault per link. Relationship group records (dense-node threshold, 50 by default) bucket chains by type and direction to cut this down | Our CSR is **strictly better on this axis**: `outEdges[outOffset[id]:outOffset[id+1]]` is one contiguous span, sequentially prefetched, no per-edge pointer chase. Both designs are O(degree) for a filtered scan; only one is cache-friendly |
| "Neo4j runs graph algorithms natively" | **Misleading.** GDS copies the graph into its own in-memory catalog. Algorithms never touch the transactional store; results are streamed, written, or mutated back. The projection is not transactionally consistent | The honest framing of our gap is "we lack the algorithm *library*", not "we lack the architecture for one" |
| "Native graph storage beats a relational join" | **Depth-dependent.** A well-indexed RDBMS is competitive at one or two hops; the advantage compounds with depth. The crossover is empirical and rarely published | A benchmark we could run and publish — RQ5 below |
| "Cypher is the standard graph query language" | **Effectively true, and now formally so.** openCypher seeded ISO GQL (ISO/IEC 39075, published 2024) and Cypher is being aligned toward conformance | If we ever build a language, a GQL/openCypher subset costs the same as inventing one and buys familiarity plus an external conformance oracle |
| "Fixed-size records are what make it fast" | **Superseded by their own roadmap.** Neo4j 5 Enterprise introduced a block format that abandons uniform fixed-size records, inlining properties and relationships for locality | Vindicates the direction §14.13's arena spike points in: packing records to kill pointer chasing is where the field is moving, not away from it |

Version-dependent constants (record widths, the dense-node threshold, block
format availability) are cited from published documentation and vary across 4.x
and 5.x. The architecture is reliable; treat the numbers as approximate.

---

## 3. Matrix 1 — storage engine

The part Phases 0–7 were spent on. Largely finished.

| Property | Neo4j | Graphene | Verdict |
|---|---|---|---|
| Index-free adjacency | Direct-addressed records; relationship chains | CSR offset arrays indexed by `NodeID`; contiguous spans | **ahead** |
| Dense-node handling | Relationship group records above a threshold | Not needed — a span is a span. But no per-type sub-partitioning, so a type filter scans the whole span | level |
| Adjacency sorted by neighbour | No — chain order is insertion order | No — span order is build order | **both open** |
| Record packing / pointer elimination | Block format (Enterprise) | Built and kept (2026-08-30): labels and blobs packed into shared backing arrays on load — allocations −21.8%, GC cycle −21.4% at 512-byte blobs. Records still hold slice headers, so the arrays are **not** pointer-free; that half is unbuilt | **partly built** |
| ACID transactions | Full, with transaction log | WAL with CRC-covered framing, batch markers, group commit | level |
| MVCC / snapshot isolation | Read-committed by default; no user-visible epoch | Epoch MVCC with a `Snapshot` type that names its epoch | **ahead** |
| Group commit | Yes | Yes — 640 commits on 53 fsyncs, 9× at 16 writers | level |
| Online backup / PITR | Enterprise | Built, with a digest manifest and point-in-time restore | level |
| Cryptographic integrity | Checksums | Merkle roots, signed attestation, forensic ledgers | **ahead** |
| Page cache / larger-than-memory | Yes — working set need not fit in RAM | No. The image loads fully into the heap; the mmap spike was rejected on the prize available | **behind** |
| Clustering / replication | Raft causal cluster | Single-writer embedded engine plus live read-only followers | out of scope |
| Background compaction | Online store copy | Pin–build–commit outside the lock; worst writer stall 91 ms → 17 ms | level |

### The one real storage gap, and why it may be re-openable

**Larger-than-memory is the only structural item we are genuinely behind on.**
§14.3 and the Phase 2 mmap spike both said no, and both were right on what they
measured. A pointer-free record array is precisely the structure that is
*mappable*, because it contains no heap pointers to fix up on load — and neither
spike evaluated that pair.

**The arena that shipped is not that structure**, which is the thing to be clear
about before treating this as re-opened. It packs labels and blobs into shared
backing arrays, but `nodeRecord` and `rawEdge` still hold `[]NodeType` and
`[]byte`, so the record arrays are still pointer-bearing. Making them
`(off, len uint32)` is a further change, and it is the one this gap needs. See
RQ4.

---

## 4. Matrix 2 — query engine and data model

If "true native graph database" means what a user experiences rather than what
the bytes look like, this matrix is the gap — not the algorithm list.

| Capability | Neo4j | Graphene | Status |
|---|---|---|---|
| Declarative query language | Cypher, converging on ISO GQL | None. `NodeQuery`/`EdgeQuery` structs are the only entry point; the builder DSL is deferred | deferred |
| Variable-length paths in the planner | `(a)-[:R*1..5]->(b)` as a planned operator | Not representable. Traversal lives in `traversal/`, outside the planner; `RelationQuery` is one hop | **open** |
| Cost-based planning | Statistics-driven, `EXPLAIN`/`PROFILE` | Cost-based over exact cardinalities, ranges costed as drivers, `ExplainNodeQuery` | level for one hop |
| Pattern matching | Full `MATCH`, planned and optimised | `FindSubgraphMatches` — backtracking over an explicit `Pattern`, unplanned | partial |
| Typed property values | Typed property store: numbers, strings, arrays, points, temporals | Opaque msgpack blob on the record; the index stores caller-encoded bytes with order-preserving encoders | deliberate trade |
| Node property columns | GDS keeps columnar arrays per projected property | **Nothing.** No place for one `float64` per node — what every centrality and community algorithm reads *and* writes | **blocking** |
| Schema and constraints | Uniqueness, node key, property existence, property type | None. Labels required and non-empty; nothing else enforced | open |
| Index types | Range, text, point, full-text (Lucene), vector | Equality, ordered/range, prefix, composite | behind on text and vector |
| Full-text / vector search | Lucene-backed; vector index since 5.11 | Neither. Both are large subsystems and both strain zero-dependency | out of scope |

**Every row that reads *open* traces to one structural fact: the planner does not
know that this is a graph.** It plans index intersections well and has never
heard of a hop. Cypher's power is not the syntax — it is that `VarLengthExpand`
is a physical operator the cost model can reason about, so a pattern with a
filter at the far end can be driven from whichever end is cheaper.

---

## 5. Matrix 3 — algorithms

Mapped against the GDS catalogue. *Cost* is implementation effort in pure Go
with no dependencies. *Needs* names the blocking primitive from §6.

### Path finding and traversal

| Algorithm | Status | Cost | Needs | Note |
|---|---|---|---|---|
| BFS / DFS, depth- and type-bounded | **built** | — | — | With budgets, cancellation, recursion guard |
| Unweighted shortest path | **built** | — | — | Bidirectional BFS, 37 allocs/op |
| Subgraph pattern matching | **built** | — | — | `FindSubgraphMatches` |
| Cycle detection | **built** | — | — | `HasCycle`, guarded to `MaxRecursionDepth` |
| Dijkstra (source-target, single-source) | open | low | P6 | **Best first algorithm.** `Edge.Weight` already exists and is durable; only its *semantics* need generalising |
| A* | open | low | P3 | Needs a per-node heuristic value — the property-column gap exactly |
| Yen's k-shortest paths | open | medium | Dijkstra | Nearly free once Dijkstra exists |
| Bellman-Ford + negative cycle | open | low | P6 | Only interesting if negative weights are admitted — a data-model question |
| Delta-stepping (parallel SSSP) | open | high | P4 | Pointless before an executor exists |
| Minimum weight spanning tree | open | low | P6 | Prim's over a CSR is a textbook fit |
| Topological sort / DAG longest path | open | low | P1 | Kahn's; needs in-degree and full iteration |
| Random walk | open | low | P1 | Trivial on a span, and the substrate for node2vec |
| All-pairs shortest path | open | medium | P4 | Only tractable in parallel, only on small graphs |

### Centrality

| Algorithm | Status | Cost | Needs | Note |
|---|---|---|---|---|
| Degree centrality | **built** | — | — | `DegreeOf` counts without materialising |
| PageRank / ArticleRank | open | low | P1–P3 | **The marquee algorithm, and nearly free on a CSR.** Ten iterations of a sparse matrix-vector product over contiguous spans. The demo that proves the engine |
| Eigenvector centrality | open | low | P1, P3 | Power iteration — PageRank's loop, different normalisation |
| Betweenness (Brandes) | open | high | P1–P4 | O(VE) unsampled. GDS ships a sampled variant for the same reason |
| Closeness / harmonic | open | medium | P1, P4 | All-pairs BFS; embarrassingly parallel over sources |
| HITS | open | low | P1, P3 | Two coupled power iterations; needs both directions, which the CSR stores |

### Community detection

| Algorithm | Status | Cost | Needs | Note |
|---|---|---|---|---|
| Weakly connected components | open | low | P1, P2 | **Best effort-to-value ratio on this page.** Union-find over a dense ordinal array; one pass, no convergence loop |
| Strongly connected components | open | medium | P1, P2 | Tarjan's — recurses, so it needs the explicit-stack treatment the DFS guard already forced |
| Triangle count / clustering coefficient | open | low | P1, P7 | Set intersection per edge. Sorted adjacency turns hashing into a linear merge — RQ3 |
| Label propagation | open | low | P1–P3 | Iterate to convergence on the modal neighbour label. Cheap and effective |
| Louvain | open | high | P1–P4 | Multi-level modularity optimisation with coarsening — it builds a new CSR per level, which we are well placed to do |
| Leiden | open | high | Louvain | Louvain plus a refinement phase guaranteeing connected communities |
| K-core decomposition | open | low | P1, P2 | Iterative degree peeling |
| K-1 colouring, modularity, conductance | open | low | P1–P3 | Small; they round out the category once the primitives exist |

### Similarity and link prediction

| Algorithm | Status | Cost | Needs | Note |
|---|---|---|---|---|
| Node similarity (Jaccard, overlap, cosine) | open | low | P7 | Neighbourhood set comparison — the direct successor to the existing `SimilarTo` edge semantics |
| K-nearest neighbours | open | medium | P3, P4 | Needs node property vectors; lands after columns |
| Adamic-Adar, common neighbours, preferential attachment, resource allocation | open | low | P7 | Each is one function over two adjacency spans. Ship as a group — the whole category is perhaps a day once intersection is fast |

### Embeddings and ML

| Algorithm | Status | Cost | Needs | Note |
|---|---|---|---|---|
| FastRP | open | medium | P3, P4 | Sparse random projection plus neighbourhood averaging — pure arithmetic, no dependency. The only embedding that fits the constraints comfortably |
| node2vec | open | high | Random walk | Biased walks are easy; the skip-gram SGD loop is a project of its own |
| GraphSAGE, HashGNN, ML pipelines | **out of scope** | — | — | Neural network training in pure Go with zero dependencies is a different product, not a feature |

---

## 6. Matrix 4 — the primitives that are actually blocking

Nothing in §5 is hard. What is missing is the layer underneath it.

| # | Primitive | The gap, precisely | Cost | Unblocks |
|---|---|---|---|---|
| **P1** | Full live-node iteration | There is no `ForEachNodeID`. `NodesByType` requires a label, so "every node" today means a union over every label — allocating, sorting and deduplicating a slice the size of the graph. Every global algorithm starts with this call | very low | everything in §5 |
| **P2** | Dense ordinal mapping | On a compacted image `NodeID` *is* the array index (`nodes[id]`, `outOffset[id]`). But IDs are never reused, so after delete churn the space is sparse, and the delta layer has no array at all. Algorithms want a contiguous `[0, n)` domain to allocate result arrays against | low | all array-based algorithms |
| **P3** | Node property columns | Properties are an opaque msgpack blob. There is nowhere to put one `float64` per node — what PageRank reads and writes, what A* needs for a heuristic, and what every algorithm needs to *return* into. The only write-back path today is `IndexNodeProperty` with caller-encoded bytes: an index entry, not a column | medium | centrality, community, embeddings |
| **P4** | Parallel executor | There are exactly two goroutines in library code — the auto-compactor and the signal handler — and neither does query work. GDS is parallel by default. An immutable pinned `Snapshot` makes a bounded worker pool over ID ranges trivially safe; the safety argument is already paid for | medium | betweenness, closeness, Louvain, delta-stepping |
| **P5** | Raw CSR span access | Traversal reaches adjacency through `IncidentEdges`, one call per node. Ten PageRank iterations over ten million edges is a hundred million interface dispatches. A direct-span fast path would be much faster — but it exists only on the disk backend, which breaks the parity rule that `memory.Store` is the oracle | medium | all of them, by a constant factor |
| **P6** | General edge weight contract | `Edge.Weight float32` exists and is durable — but it is documented as a similarity score for `SimilarTo`, zero otherwise. Similarity is *higher-is-better*; a shortest path needs *lower-is-better* cost. Reinterpreting the field silently would be a data-model change wearing an algorithm costume | low | Dijkstra, A*, MST, Yen |
| **P7** | Adjacency sorted by neighbour ID | Spans are in build order. Sorted by neighbour ID, every set intersection — triangle count, Jaccard, common neighbours, all four link-prediction scores — becomes a linear merge instead of a hash build. It would also make `neighbours()` deduplication free, retiring `neighbourDedupeLinear` | low | similarity, link prediction, triangles |

### P7 had a deadline, and it dissolved

This section argued that P7 had to be decided alongside v9, because a format
rewrite is the moment when re-sorting every span is free and shipping v9 twice is
the alternative. **There is no v9.** Adjacency has not been serialised since v7 —
`Build` recomputes the neighbour arrays on every load — so sorting spans changes
nothing durable, costs no format generation, and can be taken at any time.

What it *does* cost is measured: sorting each node's span adds **+43.5% to a
build**, paid once per compaction, and buys **−68.2% on set intersection**. The
reason it is not built is neither cost nor timing: **nothing in the engine
intersects neighbourhoods yet**, and the change makes edge ordering observably
different for every existing caller. It becomes worth taking on the day the
similarity and link-prediction algorithms in P7's column actually exist — and on
that day it is still free of any migration.

---

## 7. Matrix 5 — proposed sequencing

Written to slot in after Phase 6 rather than compete with it.

| Phase | Theme | Contents | Gate |
|---|---|---|---|
| **8** | Algorithm foundations | P1 iteration, P2 ordinals, P3 property columns, P6 weight contract. Then P4 the parallel executor and P5 the span fast path, each behind its own measurement. Algorithms run against a `Snapshot` only — which makes them lock-free by construction and lets every result carry the epoch it describes | P5 is gated on RQ1: measure the dispatch cost before deciding whether to break parity for it |
| **9** | The algorithm library | **Tier 1** — WCC, triangle count, clustering coefficient, PageRank, Dijkstra, node similarity, the four link-prediction scores, K-core, label propagation, topological sort. Every one is low-cost and each proves a primitive. **Tier 2** — SCC, Louvain, betweenness, closeness, HITS, MST, Yen, FastRP, random walk. **Tier 3** — node2vec, delta-stepping, all-pairs; decide on evidence of demand | Tier 1 is the demo that proves the CSR was worth building. Ship it as one release |
| **10** | The graph query engine | Un-defer Phase 4's builder DSL, then the item that matters: **variable-length path expansion as a planned operator**, so the cost model can choose which end of a pattern to drive from. A GQL/openCypher subset on top, if wanted, is then a parser over a planner that already understands hops | The largest project here, and the one that changes what the product *is* |

**The ordering argument.** Phase 9 is more visible than Phase 10 and far
cheaper: it turns an existing structural advantage into something demonstrable,
and every Tier 1 item is days rather than weeks. But it does not change what the
database is. Phase 10 does, and is the only work here that closes the gap the
phrase "true native graph database" actually points at.

---

## 8. Open research questions

Each is answerable by measurement rather than opinion, which is the standard
CONTRIBUTING §3 already sets. Ordered by what a wrong answer would cost.

| # | Question | How to answer it | What it decides |
|---|---|---|---|
| **RQ1** | Does interface dispatch dominate algorithm runtime, or is it noise against memory bandwidth? | Implement PageRank twice against the same fixture — once through `IncidentEdges`, once over raw CSR spans — interleaved, with a control | Whether the algorithm package needs a backend-specific access path, and therefore whether the `memory.Store` parity rule has to bend. **The most consequential answer here** |
| **RQ2** | How sparse does the ID space get under realistic delete churn? | Measure `maxNodeID / liveNodeCount` across the existing footprint fixtures, including `HalfDeleted_Uncompacted` | Whether P2 needs a real ordinal map, or whether allocating at `maxNodeID+1` and tolerating holes is simply cheaper |
| **RQ3** | ~~What does sorting adjacency spans cost at build, and buy at read?~~ **Answered 2026-08-30**: +43.5% build, −68.2% set intersection | measured in `disk/v9_spike_test.go` | P7 does not ride along with anything — adjacency is not durable, so there is no deadline. Held on the fact that nothing intersects neighbourhoods yet, and that it changes observable edge order |
| **RQ4** | Does a pointer-free arena change the mmap verdict? | Re-run the Phase 2 mmap spike against a genuinely pointer-free layout. **Note:** what shipped is not that layout — records still hold `[]NodeType` and `[]byte`, so this needs the `(off, len uint32)` records built first | Whether larger-than-memory — the one genuine storage gap — becomes reachable. Both prior spikes measured the wrong pair |
| **RQ5** | At what hop depth does index-free adjacency beat an indexed relational join? | Build the same graph in SQLite and in Graphene; measure k-hop neighbourhood at k = 1…6 | Nothing internal — but it is the number the category argues about and nobody publishes |
| **RQ6** | Does the arena's GC win grow under algorithm workloads? | Extend `tests/gc_bench_test.go` with a mutator allocating large float arrays, as PageRank does, rather than timing an idle heap. Keep `debug.SetGCPercent(-1)` — without it the figure measures the trigger rate, which is the defect that voided the original spike's GC column | How much of the measured **−21.4% at 512-byte blobs** is left on the table. The verdict itself is taken: the arena is kept |
| **RQ7** | Should `Edge.Weight` be reinterpreted, or should weight be a caller-supplied selector? | A design question, not a measurement: prototype both signatures against Dijkstra and see which one lies less | P6, and with it the whole weighted path-finding group. A selector keeps the durable format honest at the cost of an allocation per call |

---

## 9. The one-sentence answer

Graphene is already a native graph **store** — in several respects a better one
than the engine it is being measured against — and it is not yet a graph
**database**, because the planner has never heard of a hop and there is no
algorithm library on top of a structure that was practically built to host one.
