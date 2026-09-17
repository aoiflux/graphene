# The memory model

How much memory graphene holds, what holds it, and which of those terms a
configuration can change. Every figure here is measured on this host by the
instruments in `tests/rss_test.go` and `tests/rss_bench_test.go`; none is
projected from a formula. Where a figure is a projection it says so.

Read this before changing anything that allocates on the open or compaction path,
and before sizing a machine for a store.

## In one page

At 1,400,000 nodes — a 1.2 GiB store, 512-byte blobs, thirteen index entries per node,
no edges — opening the store and doing nothing else costs **2,909 MiB of anonymous
memory**, peaks at **6,139–6,227 MiB** while opening, and holds another 1,220 MiB of mapped
image that the kernel can evict.

**The property index is 94.5% of the anonymous memory and 94.1% of the open transient.**
Everything else together is 161 MiB, and that figure is measured, not modelled: the same
store built with no index declared holds 161 MiB and peaks at 980 MiB.

| question | answer | where |
|---|---|---|
| What holds the memory? | the property index, 2,645 MiB of live heap; every other structure is 80 B/node and 104 B/edge | §1 |
| Is residency linear in size? | yes, to within 5–9% over a 28× range, deviating upward; disk is exactly linear | §2 |
| Index or image? | index 5.20× its disk size in private memory, image 0.229× — a 22.7× ratio | §3 |
| What is the compaction peak made of? | 512 B × delta records + 118 B × live records + 16 B × live records, predicting a 7× larger store to 2.3% | §4 |
| What does a rebuild cost, permanently? | 10.83 B per burned identifier, against 72 B before R10(b); ~83 MiB per forty rebuilds | §5 |
| Does the program reach 2 GiB? | not today — 2,909 MiB is 42% over. With R3, ~607 MiB. Only R3 closes it | §6 |
| Does it reach 2 GiB *measured*? | for the read path, yes: 561 MiB anonymous at 1.4M nodes, 70% headroom under a real 2 GiB cgroup. For a whole-layer rebuild in the same process, no — 3,853 MiB | §9 |
| What does each option cost? | index 273 MiB, image 149 MiB of heap, adjacency 6 MiB — independent and additive | §8 |
| What does a compaction leave behind? | it used to leave its own output resident — 3,838 MiB after compacting against 1,078 MiB after reopening. The index half of that is closed: a compaction now installs the base it wrote. What remains is the record payloads | §9.4, §9.6 |

Four things a reader should take away before the detail.

1. **Memory is not a function of store size.** It is a function of the *index shape*
   declared over it. The same engine holds 6.40× the store's disk size on one blob
   distribution and 0.139× on another (§3.2).
2. **Open, not compaction, is the high-water mark.** 6,139 MiB against compaction's
   4,323 MiB. R2 removed the file-sized transient that used to dominate the open path;
   what remains is the index rebuilding one entry at a time into maps that double.
3. **No single "bytes per entry" figure for the index is honest.** Go map cost per entry
   sawtooths with an amplitude of 1.60× purely by where the entry count falls against a
   growth boundary, and presizing does not help (§1.4). Model with a range.
4. **Report anonymous and file-backed separately, always.** A change that moves bytes
   from the first class to the second — which is what R2 is — leaves the total unmoved or
   higher while improving the only figure a RAM ceiling constrains.

## Conditions

Windows 11 Pro 26200, AMD Ryzen 9 5980HS, 16 logical CPUs, 31.4 GiB RAM, go 1.26,
NVMe, 4 KiB pages. Tree `d5ac514`. `ImageMode: ImageMapped`, `IndexMode`
resident, which was the default when these figures were taken. It is not any more:
R3 shipped and `IndexMapped` is the default, so §1 and §3 describe the arrangement
this engine *replaced*. They are kept as written because they are what ranked the
items — the index at 94–96% of everything is the reason R3 exists — and §6.4
records what the flip then measured.

The fixture is the one an embedding consumer reported against: 512-byte blobs and
a declared index of 8 unique keys (one of them a 32-byte digest, distinct on every
node), 5 ordered keys and 2 composites, every key populated on every node.
**Thirteen index entries per node.** At 1,400,000 nodes the store is
1,292,200,487 bytes = **1.2035 GiB**, which is the size the consumer measured.

Residency is reported in two classes, and the distinction is load-bearing:

- **anonymous** — private, swap-backed pages: the Go heap, stacks, runtime
  structures. This is what a hard RAM ceiling constrains.
- **file-backed** — pages that are a view of a file. The mapped image lives here.
  The kernel can evict these without swap, so they are real residency but cheap
  residency.

A change that moves bytes from the first class to the second improves the figure
that matters and leaves the total unmoved or higher. Reporting one combined number
would make such a change indistinguishable from no change, which is why the
instrument never does.

## 1. What holds the memory

### 1.1 The whole picture at 1.2 GiB

`BenchmarkRSS_Open` on the 1.4M-node master, three runs in three fresh processes:

| | MiB | note |
|---|---:|---|
| anonymous, settled | **2,909** (2,909 / 2,909 / 2,911) | the ceiling-relevant figure |
| file-backed, settled | 1,220 (1,220 / 1,221 / 1,221) | the mapped image, ≈ the store size |
| total resident, settled | **4,129** (4,129 / 4,130 / 4,131) | |
| Go live heap objects | 2,756 (identical across runs) | |
| process peak | 6,161–6,227 | during Open, before settling |
| store on disk | 1,232 | |
| resident per disk byte | **3.351×** | |

Spread across three processes is 0.07% on anon and 0.05% on total, so every
difference discussed below is far outside the host's variance.

Two readings before the decomposition:

**The 2 GiB target is missed by 42% on the anonymous figure alone**, after the
three items of the program that have shipped (R1, R10(b), R2). Opening this store
and doing nothing else costs 2.84 GiB of private memory.

**Open peaks at 6.1 GiB against 4.1 GiB settled.** R2 removed the file-sized
transient that used to sit on the open path, so the remaining 2 GiB gap is not the
image. It is the property index's maps growing: the index is rebuilt one entry at
a time from the image's GIDX section, and a Go map that doubles holds the old
table and the new one at once. §1.4 measures that directly.

### 1.2 The property index is 94-96% of everything

Measured by differencing, not by profiling. A heap profile attributes an
allocation to whichever map grew last, and the index is spread over 16 shards ×
(forward postings, `ref1`, `refN`, intern table, per-key counts, key intern) plus
five ordered indexes and two composites — so the only figure a profile reports
reliably is the total. Building the same shape with one group of keys at a time
and subtracting gives the per-group cost instead.

At 200,000 nodes, each arm a fresh `PropertyIndex`:

| arm | MiB | entries | B/entry |
|---|---:|---:|---:|
| `digest` only — 32 B, all distinct, unique | 31.18 | 200,000 | 163.45 |
| `uid0`–`uid6` only — 14 B, all distinct, unique | 187.74 | 1,400,000 | 140.62 |
| `ord0`–`ord4` only — 6 B, 1000 distinct, ordered | 70.82 | 1,000,000 | 74.26 |
| `ord0`–`ord4` + the 2 composites | 122.14 | 1,000,000 | 128.08 |
| **the full declared shape** | **336.90** | **2,600,000** | **135.87** |

The arms sum to 341.06 MiB against 336.90 measured — **additive to 1.2%**, which
is what licenses the subtraction. The small saving is real and identifiable: two
of the thirteen keys (`digest` and `uid2`) hash to the same shard, so a node's
reverse entries there fold from two `ref1` entries into one `refN` entry, and
per-shard fixed cost is paid once rather than three times.

Four things follow.

**135.87 B per entry on the shape a real consumer declared**, not the ~107 B/entry
the program has been sizing against (§14.8). The earlier figure was measured on a
bare shape: no ordered indexes, no composites, no unique declarations. Sizing work
against it under-counts by 27%.

**Cardinality dominates value length.** A 32-byte all-distinct digest costs 163
B/entry; a 6-byte value drawn from 1000 distinct values costs 74 B/entry. Values
5× apart in length, cost 2.2× apart in the other direction. Value interning
(§14.9) and shared posting lists are doing that work, and they can only do it when
values repeat.

**Two composites cost more per declaration than the five ordered keys they are
built from**: 51.32 MiB against 70.82 MiB, or 269 B per node for two composites
against 371 B per node for five ordered keys. Composites are **15.2% of index
residency** on this shape and appear nowhere in the program's sizing.

**The index is 1,766 B/node** (336.90 MiB / 200k) against a whole-process heap
slope of 1,887.4 B/node: **93.6% of Go heap residency is the property index.**
That is the case for R3, measured rather than extrapolated.

Two denominators are in play and they should not be confused. 93.6% is against the
*fitted slope*, which excludes the intercept; against total heap at 200,000 nodes the
index is 95.1%. §3 measures the same quantity a second way at 1,400,000 nodes, by
building the whole store with and without an index, and gets **96.0% of live heap and
94.5% of anonymous residency**. The anonymous figure is lower because the Go runtime's
own anonymous memory — stacks, spans, GC headroom — does not depend on the index.
Three methods at two sizes agree within two points, and the anonymous figure is the
one a ceiling constrains.

### 1.3 Everything else, from the structures

The non-index terms are small enough, and regular enough, to state exactly. Sizes
are `unsafe.Sizeof` on amd64; the fixture writes identifiers 1..N contiguously, so
every page of the CSR page table is fully occupied and there is no dead-slot slack.

Per live node:

| term | B/node |
|---|---:|
| `nodeRecs` — one `nodeRecord` (ID 8 + two slice headers 48) | 56 |
| `outOffset` + `inOffset` — one `uint64` each per slot | 16 |
| `nodesByLabel` — one `NodeID` | 8 |
| `nodeDir`, `nodePages`, `nodeLiveBefore` — per 4096 identifiers | 0.005 |
| **total** | **≈ 80** |

Per live edge: `edgeRecs` (`rawEdge`, 80) + `outEdges`/`inEdges` (16) +
`edgesByLabel` (8) ≈ **104 B/edge**.

Property blob bytes are **not** on this list, and that is R2: under `ImageMapped`
they are a three-index sub-slice of the mapping and cost anonymous nothing. Under
`ImageHeap` they return as arena bytes — mean blob size per live record.

80 + 1,766 = 1,846 B/node against the measured 1,887.4 B/node slope: a **2.2%
residual** covering the delta, the WAL write buffer, the label maps' own map
machinery and GC bookkeeping. The model closes, which is the point of stating it.

It closes a second time, independently, against a whole store. The no-index arm of §3
builds all 1,400,000 nodes with no index declared at all and holds **110.5 MiB of live
heap = 82.8 B/node**, against the 80 B/node this table predicts — a 3.5% residual over
a 7× larger store. The 80 B/node figure is not a model of the structures; it is what
the structures cost.

### 1.4 The Go map sawtooth, and why no single B/entry figure is honest

The largest single structure in the store is `ref1`, a `map[NodeID]propRef` — one
entry per node per shard that holds any of its keys. Its cost per entry is not a
constant. Measured over a sweep of sizes:

| entries | MiB held | B/entry |
|---:|---:|---:|
| 500,000 | 40.0 | 83.88 |
| 800,000 | 40.0 | 52.48 |
| 900,000 | 50.3 | 58.55 |
| 1,000,000 | 80.0 | 83.84 |
| 1,200,000 | 80.0 | 69.97 |
| 1,835,008 | 118.9 | 67.97 |

The totals give it away: one allocation of ~40 MiB serves anywhere from 500k to
800k entries, and the next step of ~80 MiB serves 1M to 1.2M. So per-entry cost
sawtooths between **52.5 and 83.9 B — an amplitude of 1.60×** — driven only by
where the count falls relative to a growth boundary, and not by anything about the
data.

**Presizing does not help.** `make(map[NodeID]propRef, 1_000_000)` measures 83.97
B/entry, the same as growing into it, because the requested capacity rounds up to
the same step. There is no version of this structure that avoids the sawtooth
while remaining a Go map.

Two consequences, both practical:

1. Any "B/entry" figure for the index — including 135.87 above and the ~107 in
   §14.8 — is one point on a sawtooth. Model with the range.
2. The sawtooth is also the 2 GiB transient in §1.1. A doubling map holds both
   tables, so the peak of a per-entry index rebuild is roughly 1.5× its settled
   size, arriving at whatever moment the largest shard crosses a boundary. This is
   why Open peaks at 6.1 GiB to settle at 4.1.

Supporting map costs at 2^20 entries, for the model:

| map | B/entry |
|---|---:|
| `map[uint64]propRef` (`ref1`) | 80.07 |
| `map[uint64][]propRef` (`refN`), nil values | 80.08 |
| `map[uint64]*memberState` (composites) | 36.08 |
| `map[string]string`, 16 B keys (the intern table) | 96.08 |

## 2. Is residency linear in dataset size?

Every extrapolation in the program's brief assumed it was, and nothing had
measured it. It is, to within 5–9% over a 28× range, and the deviation is upward.

Least squares over 50k / 100k / 200k / 400k nodes, one process per size:

| series | B/node | intercept | max residual |
|---|---:|---:|---:|
| anonymous | 2,033.8 | 52.93 MiB | 1.144 MiB (0.138%) |
| Go heap | 1,887.4 | 2.09 MiB | 0.079 MiB (0.011%) |
| store on disk | 923.0 | −0.01 MiB | 0.036 MiB (0.010%) |
| total resident | 2,947.8 | 51.12 MiB | 1.065 MiB (0.091%) |

Over that range the fit is very nearly exact — residuals are a tenth of a per
cent. Extended to the 1.4M master, 3.5× beyond the fitted range:

| series | fit predicts | measured | error |
|---|---:|---:|---:|
| anonymous | 2,768 MiB | **2,909 MiB** | **+5.1%** |
| Go heap | 2,522 MiB | **2,756 MiB** | **+9.3%** |
| store on disk | 1,232 MiB | 1,232 MiB | 0.0% |

So: **disk is exactly linear, residency is linear plus a few per cent, and the
excess is on the heap.** §1.4 names the mechanism — at 1.4M nodes the shards sit
at a less favourable point on the map sawtooth than they did at 400k, and there is
no reason to expect any particular size to be favourable.

Two working numbers come out of the fit, and both are used below:

- The disk slope, 923.0 B/node, is what makes a target store size a node count:
  1.2 GiB is N = 1,395,980, and the 1.4M master is 1.2035 GiB.
- The effective heap slope at scale is 2,064.8 B/node (2,756 MiB / 1.4M), not the
  1,887.4 the small sizes suggest. Size a machine with the larger figure.

**Do not extrapolate residency past what has been measured.** The fit is a good
description of a measured range and a 5–9% under-estimate one step beyond it; the
program's remaining projections are marked as projections for that reason.
## 3. Index versus image at 1.2 GiB

This is the number that ranks the two large items of the program against each
other, and it is measured by building the same store twice — once with the
declared index and once with `GRAPHENE_RSS_NOINDEX=1`, which skips every
declaration and every `IndexNodeProperties` call — and subtracting.

Nothing else would do. The index is spread across 16 shards, an intern table, five
ordered indexes and two composites; a heap profile attributes an allocation to
whichever of those maps grew last, so the only figure it reports reliably is the
total. Two stores and a subtraction give the split.

A note on how these were obtained, because the first attempt got it wrong. The
process that builds a 1.4M-node fixture peaks near 9.4 GiB, and `peakMiB` is a
process-lifetime high-water mark, so a process that builds and then measures reports
the build's peak under the name of the open's — and `debug.FreeOSMemory` does not fully
undo a build either, so its settled anonymous figure is high too. The first no-index
run reported 205.1 MiB anon and a 2,907 MiB peak for exactly that reason. Every figure
below comes from a fresh process opening a persistent master built by an earlier one
(`GRAPHENE_RSS_DIR`), which is what that environment variable exists for.

Both at 1,400,000 nodes with 512-byte blobs, three runs each in fresh processes:

| | indexed | no index | difference = the index |
|---|---:|---:|---:|
| store on disk, MiB | 1,232 | 703.6 | **528.4** |
| anonymous, MiB | 2,909 | 161.1 | **2,748** |
| file-backed, MiB | 1,220 | 699.9 | 520.1 |
| total resident, MiB | 4,129 | 861.3 | 3,268 |
| Go live heap, MiB | 2,756 | 110.5 | **2,645** |
| peak during open, MiB | 6,139 – 6,227 | **980.2** | — |
| resident per disk byte | 3.351 | **1.224** | — |

Spread on the no-index arm is 0.7% on anon and 0.05% on total across three processes,
and its heap figure is identical to four significant figures in all three.

**The index is 94.5% of anonymous residency and 96.0% of the live heap.** The first
figure is the one a ceiling constrains and the second is the larger, because the Go
runtime's own anonymous memory — stacks, GC metadata, spans not yet returned to the
OS — is the same whether an index exists or not.

The peak row is the other half of the answer, and it was not the question anyone asked.
**Opening the indexed store costs a 2,006 MiB transient above its own settled figure;
opening the same store with no index costs 118.9 MiB.** So 94.1% of the open transient
is the index being rebuilt one entry at a time out of GIDX, with the shards' maps
holding an old table and a new one every time one doubles (§1.4). R3 does not merely
remove 2.7 GiB of settled memory — it removes a 2 GiB transient that is, after R2,
the high-water mark of the entire store lifecycle (§4).

Stated as the thing a budget cares about — anonymous bytes held per byte of store
on disk:

| | on disk | anonymous | anon per disk byte |
|---|---:|---:|---:|
| image: records, blobs, adjacency, label postings | 703.6 MiB | 161.1 MiB | **0.229×** |
| property index | 528.4 MiB | **2,748 MiB** | **5.20×** |

**The index is 42.9% of the file and 94.5% of private memory.** Per byte of disk it
costs 22.7× what the image costs. That is the whole ranking: R2 addressed the 0.229×
term and did it well — 683.6 MiB of blob bytes now cost nothing anonymous (§1.3), and
§3.2 shows the same mechanism absorbing 911 MiB — but the term that breaks the budget
is the other one, and only R3 addresses it.

Two further readings.

**The index costs 5.20× in memory what it costs on disk.** 528.4 MiB of GIDX
becomes 2,748 MiB of maps. A design that reads it in place rather than decoding it
is not a marginal improvement on that ratio; it removes it.

**And it costs more on disk to read it in place — by exactly what stops being
built.** Measured on the same declared shape at 50,000 nodes and 650,000 entries,
the image is 60.46 MiB as v9 against 44.01 MiB as v8: **about 26 bytes an entry
more**, all of it in the index sections. The two terms are GPIR's 24 bytes per
entry and the value table's 16 bytes per distinct value, and predicting the
difference from those field widths gives 16.45 MiB against 16.45 MiB measured.
Those two structures are the reverse direction and the search structure — which
is to say the *whole* of what a v8 open reconstructs in the heap, at 5.2× the
bytes. The image grew by what stopped being built in RAM. Disk is unconstrained
here and memory is the budget, so this is the trade being made rather than a cost
being discovered; `graphene store migrate -to 8` is the way back for a reader for
whom it runs the other way round. See `docs/benchmarks.md`, "What a migration
costs, in both directions".

**155.9 B per entry at 1.4M**, against 135.87 measured at 200k (§1.2) — 14.7%
higher for the same declared shape, at a larger size. That is the map sawtooth
(§1.4) and it is the reason this document reports a range wherever it reports a
per-entry cost.

A note on the no-index arm's other half: its `residentPerDisk` of 1.224 is what
graphene costs today when the property index is not in play at all — 161 MiB of
private memory for a 703.6 MiB store, of which 110.5 MiB is the live heap that §1.3
predicts to 3.5% from structure sizes alone. That figure is the target architecture
already working for records and blobs, and it is what §7 extends to the index.

### 3.2 The same split under the consumer's real blob distribution

The 512-byte fixture is uniform, and the consumer's data is not: they report blobs
from a few hundred bytes to 64 MiB. `GRAPHENE_RSS_BLOB_DIST=1` writes the shape the
brief describes — 90% at 512 B, 9% at 64 KiB, 1% at 4 MiB, a mean of 48,302 B per
node — and it changes the ratio by a factor of 46.

Both arms at 20,000 nodes, three runs each:

| | fixed 512 B | long-tailed | ratio |
|---|---:|---:|---:|
| store on disk, MiB | 17.61 | 929.1 | **52.8×** |
| anonymous, MiB | 96.56 – 97.02 | 107.1 – 109.4 | **1.12×** |
| file-backed, MiB | 16.16 – 16.23 | 20.33 – 20.85 | 1.27× |
| total resident, MiB | 112.7 – 113.3 | 127.4 – 130.3 | 1.14× |
| Go live heap, MiB | 44.36 – 44.41 | 44.38 – 44.42 | **1.000×** |
| resident per disk byte | 6.404 – 6.433 | **0.1372 – 0.1402** | 0.022× |

**911 MiB of additional blob bytes cost 11.5 MiB of additional anonymous memory —
1.3% of themselves — and cost the Go heap nothing measurable at all** (44.4 MiB in
both arms, to four figures). That is R2 working exactly as designed, and it is worth
more on real data than on the uniform fixture every other section of this document
uses: the bigger the blobs, the more of the store is bytes that never need to be
private.

Two further readings, and the second is the more important one.

**Per byte of store, a blob-heavy graph is 46× cheaper to hold than a blob-light
one.** `residentPerDisk` is 6.40 on the uniform arm and 0.139 on the tailed arm. A
ratio of resident-to-disk is therefore not a property of graphene; it is a property of
the caller's data shape, and it should never be quoted without the shape beside it.
The 3.351× of §1.1 belongs to a 512-byte fixture with thirteen index entries per node.

**File-backed residency is evictable, and this arm shows it directly.** The tailed
store's image is 929 MiB and only 20.5 MiB of it stays resident — while the uniform
store's 17.6 MiB image is 16.2 MiB resident, nearly all of it. Open verifies the
digest and therefore reads every byte of both, so the difference is not access
pattern: it is that the tailed arm's build put the host under pressure and the kernel
trimmed the mapping's working set, with no effect on the anonymous figure. A mapped
image is real residency that a ceiling does not have to pay for, which is the entire
argument for the class split, demonstrated rather than asserted.

One caveat, and one finding hiding inside it. The tailed arm's `peakMiB` of
3,905–4,171 is build contamination: arm F has no persistent master, so each process
builds its own fixture and `peakMiB` is process-lifetime ("Reproducing these
figures", below). But the contaminating
figure is itself informative — writing a 929 MiB store whose tail is 4 MiB blobs peaks
near 4 GiB, because the write path buffers whole blobs in anonymous memory before they
reach the WAL. R2 addresses the read side only. **Large blobs are cheap to hold and
expensive to write**, and nothing in the current program changes the second half.
## 4. What the compaction transient is made of

Two compactions were measured, both by sampling resident memory on a ticker for the
duration of `Compact`, so the figures are peaks rather than averages, and both were
then decomposed by an allocation profile at `-memprofilerate=1` — affordable precisely
because R1 reduced a whole compaction to a few thousand allocations.

| | first compaction, 200,000 nodes | incremental, 1,400,000 nodes |
|---|---:|---:|
| settled anonymous before, MiB | 519.6 – 524.3 | 2,909 |
| peak during `Compact`, MiB | 670.7 – 676.2 | 4,323 |
| transient above settled, MiB | **142.1 – 143.3** | **185.5** |
| transient as % of settled | 27.4% | 6.4% |
| allocated during `Compact` | 138.7 MiB | 184.8 MiB in 53,306 allocations |
| resident samples taken | 4,338 – 6,648 | — |
| wall clock, s | — | 20.0 |

Spread across three runs of the 200k arm is 0.8% on the transient. Allocated and
transient agree to 2.5% on the first arm and 0.4% on the second, which says something
useful on its own: **nothing allocated during a compaction is released during it.**
The transient is the allocation total, so naming the allocation sites names the
transient exactly.

**The transient does not scale with the store.** A 7× larger store has a 1.3× larger
transient. That is R1 delivered — the peak is no longer a second copy of the image —
but the two arms differ by more than size, and the difference is the finding.

### 4.1 The decomposition

An incremental compaction of a 202,000-record store, every allocation sampled, focused
on the `Compact` call:

| site | MiB | per unit | what it is |
|---|---:|---:|---|
| `buildSeq` | 22.70 | 117.8 B/live record | the new `CSRGraph` |
| — of which `buildLabelIndex` | 8.62 | 44.7 B/node | the two label maps, append-grown |
| `SerialiseTo` | 3.16 | 16.4 B/live record | **the entire 176 MiB image write** |
| — of which the streamed payload | 3.09 | **1.19 B/index entry** | `ForEachNodeProperty` over 2.6M entries |
| `compactPin` | 1.10 | — | the plan |
| — of which `cloneBytes` | 0.98 | **512 B × delta records, exactly** | the delta payload copy |
| **total** | **26.96** | **140.0 B/live record** | |

Four readings, and each one closes a question the plan left open.

**The structural model of §1.3 is exact.** `buildSeq` costs 117.8 B per record, of
which 44.7 B is the label maps; the remaining 73.1 B/node is the record array plus the
two offset arrays, against the 72 B those structures measure by `unsafe.Sizeof`. A
1.5% residual. The new graph a compaction must build before it can publish it is
therefore not an estimate — it is 80 B/node and 104 B/edge, plus label maps that are
append-grown and could be presized.

**`SerialiseTo` writes a 176 MiB image with 3.16 MiB of allocation.** That is R1(ii)
stated as a measurement rather than an intention: the image write is a constant, not a
file-sized buffer. The old `SerialiseWithPayload` path materialised the whole image in
one `bytes.Buffer`.

**The streamed index payload costs 1.19 B per entry.** R1(i) replaced
`NodeEntries()`/`EdgeEntries()` — which built a slice of `PropEntry` (40 B each) with a
fresh `[]byte(v)` per triple — with a `ForEachNodeProperty` walk. For this store's 2.6M
entries that is 3.09 MiB against the ≥104 MiB the materialised form would hold, a **34×
reduction on that term**, and it is the reason a 1.4M-record compaction now allocates
53,306 times rather than tens of millions.

**`cloneBytes` allocates exactly the blob bytes of the delta** — 1,000 kB for 2,000
delta records at 512 B each, to the byte. This is the term R1(iii) did not remove: it
took out the CSR half of the plan copy and left the delta half
(`disk/compact.go:210-259`). It is invisible on an incremental compaction and it is
everything on a first one, where the delta *is* the store: 512 B × 200,000 = 97.7 MiB,
and the profile of the first compaction shows `cloneBytes` at 102 MiB flat.

### 4.2 The model, and what it predicts

```
transient  ≈  512 B (mean blob) × delta records          <- the compaction policy bounds this
            + 118 B × live nodes + 142 B × live edges    <- the new graph, unavoidable
            +  16 B × live records                       <- image write and streamed payload
```

| arm | model | measured | residual |
|---|---:|---:|---:|
| first compaction, 200k (delta = everything) | 123.3 MiB | 142.6 MiB | +15.6% |
| incremental, 1.4M (delta ≈ nothing) | 181.2 MiB | 185.5 MiB | −2.3% |

The 200k residual is the WAL frame buffers and the Merkle builders' leaf state, which
the incremental arm pays too but which are a smaller share of a larger figure. Fitting
the model on a 200k profile and predicting a 7× larger store to 2.3% is the strongest
statement this document can make about compaction: **the transient is understood.**

One term in it is an operator's decision and the rest are not. A store that is never
compacted accumulates an unbounded delta, and the compaction that finally runs pays for
all of it at once — which is why `CompactionPolicy.MaxDeltaBytes` (Phase 6) is a memory
control and not only a disk one.

### 4.2a The fourth term: a constant, and the only one an operator sets directly

The model above has three terms and every one of them follows the store. A
mapped-index compaction has a fourth that does not: the intermediates it holds
while it streams.

| intermediate | default | what it bounds |
|---|---:|---|
| GPIX value table (`gpixVtabMemCap`) | 1,048,576 B | distinct values held before the table opens a file |
| GPIR sort chunk (`gpirSortChunkEntries`) | 32,768 entries | entries sorted in memory at once, held twice — as entries and as the buffer that writes them out — in each of two sorters |
| GPIR merge budget (`gpirMergeReadBudget`) | 2,097,152 B | every run reader's buffer together |
| GPIR run buffer (`gpirMaxRunBuffer`) | 65,536 B | one run reader's |
| **peak, the four together** | **4,325,376 B** | **4.125 MiB, whatever the store holds** |

The peak is during the fill rather than the merge, because both sorters are
filled together and live for the whole GPIX write while the merge runs after the
value table is closed and after the sorter being drained has released its chunk.

Four megabytes is nothing beside the 181 MiB §4.2 predicts, which is exactly why
it was missed twice: it is absent from §4.1's decomposition, which was measured
on the v8 path, and it was absent from `compactWorkingSet` until `CompactOptions`
gave it a figure to be named by. On a small store it is most of the transient,
and a small store under a tight budget is the case a budget exists for.

`Options.Compact.MaxWorkingBytes` sets it, floor 524,288 B. Measured at 50,500
records over three interleaved rounds:

| setting | compaction allocation | vs default | wall clock vs default |
|---|---:|---:|---|
| default (4,325,376 B) | 10.19 MiB | — | — |
| floor (524,288 B) | 6.87 MiB | **−3.32 MiB** | +12%, +16%, +42% |
| 64 MiB | 48.23 MiB | +38.04 MiB | +18%, 0%, −12% |

**Fifteen times the memory buys nothing measurable.** The spill and the merge are
already cheap — sequential, page-cache backed, written once and read once — so
the direction that was expected to pay does not. Lowering it is a real 3.3 MiB
for a real third of a second. Both figures are small, and this table exists so
that nobody has to rediscover that they are.

Not covered: that fixture sorts in about twenty runs, and the 28 million entries
§1.2 projects would sort in around 850. A merge forty times wider was not timed.

### 4.3 A never-compacted store also opens expensively

`RSS_Open_UncompactedWAL` at 200,000 nodes replays the whole history from the WAL with
no image on disk at all:

| metric | value | a compacted open at the same size |
|---|---:|---:|
| anonymous, MiB | 533.4 | ~441 (from the §2 fit) |
| file-backed, MiB | 0 — there is no image to map | ~185 |
| peak during open, MiB | 902.7 | — |
| Go live heap, MiB | 475.4 | — |
| resident per disk byte | 2.666 | 3.351 |

**A WAL-replay open costs 21% more anonymous memory than an image open of the same
store and peaks at 1.69× its own settled figure.** Nothing is mapped because nothing
has been written in image form, so every byte of the replayed history lands in private
memory. That is the never-compacted-store case measured (§4.3), and the argument for
`Options.MaxReplayRecords` refusing an open by naming the figures rather than
discovering them inside it.

### 4.4 Why this section is a differencing result and not a heap profile

The plan specified `-memprofile` with `-memprofilerate=1` and `inuse_space` for §1 and
§4. For the compaction path that worked, and §4.1 is its output. For a *live store* it
cannot work, and the reason is structural rather than a matter of technique: `go test
-memprofile` writes the profile when the test binary exits, by which time the
benchmark's `defer g.Close()` has run and the store is gone. The attempt is on record —
an `inuse_space` profile of a 1.4M-node indexed Open reports **6.1 MB total, 91.7% of
it `runtime.mallocgc` under `runtime.newm`**, which is the scheduler's own allocations
after the store was torn down.

Profiling a live store needs `pprof.WriteHeapProfile` called inside the benchmark while
the handle is open, which no current instrument does. So the differencing method of
§1.2 and §3 was not a fallback chosen for convenience — for the settled figures it was
the only available method, and for an index spread across a hundred maps it is the
better one regardless.
## 5. The rebuild-cycle slope, and what R10(b) actually bought

This is the M8 instrument, and it is the one measurement in this document that no
file-size-based model predicts. A fixed live set of 200,000 nodes; each cycle deletes
every node and writes 200,000 new ones, then compacts. IDs are never reused
(invariant §15.1), so each cycle burns 200,000 node IDs permanently.

Resident memory after each of ten cycles:

| cycle | resident, MiB | Δ |
|---:|---:|---:|
| 0 | 571.07 | — |
| 1 | 590.20 | +19.14 |
| 2 | 603.81 | +13.61 |
| 3 | 611.09 | +7.28 |
| 4 | 614.59 | +3.50 |
| 5 | 619.04 | +4.45 |
| 6 | 623.09 | +4.05 |
| 7 | 624.79 | +1.71 |
| 8 | 629.35 | +4.56 |
| 9 | 632.87 | +3.52 |
| 10 | 629.4 | −3.5 |

| slope | value |
|---|---:|
| over all ten cycles | 5.647 MiB/cycle |
| over the second half only | **2.065 MiB/cycle** |
| per burned node ID | **10.83 B** |

**The series decelerates, which is the whole result.** A per-ID resident cost
produces a straight line; this line flattens from +19 MiB at the first cycle to
±3.5 MiB by the tenth. Measuring one cycle and multiplying — which is what a
`RebuildCycle_1` arm alone would do — reports 13.15 MiB/cycle and overstates the
forty-cycle cost by 6.4×. The instrument logs every cycle and fits the second half
for exactly this reason.

Against the pre-R10(b) figure the plan established by structure — **72 B resident per
burned node ID**, 1.5M IDs per rebuild ≈ 108 MiB per rebuild, permanently — the
measurement is **10.83 B/ID, a 6.6× reduction**, and it is not a per-ID cost at all:
the page directory is one `int32` per 4,096 IDs, so 200,000 burned IDs add 196 bytes
of directory. What remains is heap fragmentation and the map sawtooth (§1.4) stepping,
neither of which compounds.

Projected to the consumer's pattern at the tail slope: **40 rebuilds cost ~83 MiB**,
against ~4.3 GiB under the pre-R10(b) layout. The ID ceiling, not resident memory, is
now what bounds rebuild count: `maxCSREntityID` at 2^32 allows ~2,860 rebuilds of
1.5M nodes, and `StorageStats.NodeIDHeadroom` (B10) reports the margin.

One caveat on the arm's own figures: `fileMiB` is 0 for every rebuild and compaction
arm. That is correct and not a mapping failure — a store built in-process holds its
`CSRGraph` from `Build` and never deserialises an image, so R2's mapping only appears
in arms that reopen from disk. Rebuild-cycle residency is therefore all anonymous by
construction, which makes it the harshest arm for a budget and the right one for this
question.
## 6. Ranked consumers, and what each remaining item recovers

### 6.1 The ranking at 1.2 GiB

Anonymous residency of a bare Open at 1,400,000 nodes. Every row is measured, not
apportioned: the index rows come from the no-index differencing of §3, the CSR rows from
the structure sizes of §1.3 which that same arm confirms to 3.5%, and the runtime row
from the gap between the OS's anonymous figure and the Go runtime's live-heap figure.

| consumer | MiB | share of anon | what changes it |
|---|---:|---:|---|
| property index — reverse maps, forward postings, intern tables | **2,243** | **77.1%** | **R3** |
| property index — the two composites | **402** | **13.8%** | nothing planned |
| CSR node records (`nodeRecord`, 56 B/node) | 74.8 | 2.6% | nothing — already dense since R10(b) |
| Go runtime — spans not returned, stacks, GC metadata | 153 | 5.3% | `GOGC`/`GOMEMLIMIT`, not code |
| CSR adjacency (`outOffset`/`inOffset`, 16 B/node) | 21.4 | 0.7% | **R4** (lazy), readers that never delete |
| label postings (`nodesByLabel`, 8 B/node) | 10.7 | 0.4% | presizing, ~0.4% |
| delta, WAL buffer, page table, schema | 3.6 | 0.1% | |
| **total anonymous** | **2,909** | 100% | |
| property blob bytes, file-backed | 683.6 | — | **already taken by R2** |
| the rest of the mapped image, file-backed | 536 | — | evictable under pressure (§3.2) |

The two index rows split 2,645 MiB of live heap by the 15.2% composite share measured at
200,000 nodes (§1.2); everything else in the table is direct. As a check on the split as
a whole, §3's whole-store differencing puts the index at 2,748 MiB of *anonymous* memory,
which is these two rows plus the index's share of the runtime row: 2,243 + 402 + 103 =
2,748. The two decompositions close on each other.

The shape of this table is the program's argument in one place: **90.9% of private
memory is the property index's own structures** — 94.5% once the runtime memory they
force is counted with them, which is what §3 measures directly — and one planned item
addresses 77.1% of it.

### 6.2 Bytes recovered per unit of effort

Effort is the plan's own line estimate. "Recovered" is measured for shipped items and
projected for the rest, marked as such.

| item | status | anon recovered at 1.2 GiB | lines | MiB per 100 lines |
|---|---|---:|---:|---:|
| R2 — map the image | shipped | 683.6 measured, and 911 on §3.2's blob shape | ~800 | 85 – 114 |
| R10(b) — page-table CSR | shipped | 0 at one rebuild; **83 MiB per 40 rebuilds avoided** (measured, §5) | ~850 | 0 / 9.8 |
| R1 — bounded compaction peak | shipped | transient only: a 34× cut on the payload term, 176 MiB of image written in 3.16 MiB (measured, §4.1) | ~600 | — |
| **R3 — disk-resident index** | **next** | **2,243 settled + a 2,006 MiB open transient** (projected) | **~2,750** | **81 settled** |
| composites to disk | unplanned | 402 (projected) | ~400 | ~100 |
| R4 — lazy adjacency | later | 21.4, readers only (projected) | ~250 | 8.6 |
| presize the label maps | trivial | ~9 transient (measured as 44.7 B/node in §4.1) | ~10 | — |

Two items rank differently than the plan assumed.

**R10(b) recovers nothing on a freshly built store and everything on a rebuilt one.**
§5 measures 10.83 B per burned identifier against the 72 B the pre-R10(b) layout costs
by structure — so its value is not a figure at any one moment but a slope that no longer
rises. Ranked on a single open it scores zero; ranked on the consumer's actual pattern it
removes a 4.3 GiB accumulation.

**R3 is under-ranked by its settled figure alone.** It removes 2,243 MiB of settled
memory *and* the 2,006 MiB open transient that §3 attributes 94.1% of to the index
rebuild. After R2, that transient is the high-water mark of the whole store lifecycle —
higher than compaction's (§4) — so R3 is the only remaining item that lowers the peak an
OOM killer actually sees.

### 6.3 Does the program reach 2 GiB?

| after | anon, MiB | vs 2 GiB (2,048 MiB) |
|---|---:|---|
| v0.6.0, pre-program (projected from the consumer's 4.06× ratio) | ~3,530 | +72% |
| today — R1 + R10(b) + R2 shipped (**measured**) | **2,909** | **+42%** |
| + R3, `IndexMapped` the default (**measured at 200k, scaled**) | **~1,113** | **−46%** |
| + R3 as designed (projected, superseded — see §6.4) | ~607 | −70% |
| + the composites on disk too (projected) | ~205 | −90% |

The arithmetic of the two projected rows, so it can be argued with: **161** measured with
no index at all (§3), **+ 402** for the composites R3's design leaves resident (§6.1),
**+ ~44** for R3's own resident cost — a fence over every 4,096th value-table entry plus
the key directory, ~110 KB for a 28M-entry key, plus the delta since the last compaction
— **= 607**. Drop the composites to disk as well and the same sum is **205**, which is
the no-index measurement plus R3's fences and nothing else.

**R3 was the item that decided the target, and with it the target is reached.** Nothing
else in the program closes a 42% overshoot: R4 is 0.7% of anonymous residency and the
runtime's own 5.3% is a `GOMEMLIMIT` setting rather than an engineering item. §6.4 is the
measurement and is the row to argue with; the ~607 projection below it is kept because the
gap between the two is instructive.

The floor those projections approach is not a guess. The no-index arm of §3 measures what
this store costs when the property index is not in play at all — **161 MiB of anonymous
memory for a 703.6 MiB store, a ratio of 0.229×** — and the "+ R3 and the composites" row
is that measurement plus R3's own resident cost, which is a fence over each key's value
table and a key directory: kilobytes per key, not bytes per entry.

Three cautions on the projected rows.

**R3 as designed leaves the composites resident.** Its design rebuilds composite indexes
at open from the member keys' runs, so the 13.8% row survives it — 402 MiB here. That is
comfortably inside the budget on this fixture, but it is the largest remaining term
afterwards, it scales with composite entries, and §1.2 found composites costing more per
declaration than the ordered keys they are built from. A consumer who declares many
composites can put themselves back over a tight ceiling. That belongs in the options
documentation rather than in a later surprise.

> **Superseded by §8.6.** This caution was right for as long as it stood, and the
> term it names was measured at **128.2 MiB of 237.8 — 53.9%** at 1,400,000 nodes,
> the largest single item in that column. `GCPX` then moved the postings into the
> image, so a store reading a v9 image written by v0.8.0 or later holds its
> composites' *declarations* and not their entries. What survives of the caution is
> narrower and still worth stating: a composite declared over an image that does not
> carry it — declared after the last compaction, or carried by an older image — is
> still filled into the heap at open and still costs what this paragraph says.

**Do not read the projections as measurements.** §2 establishes that residency is linear
plus 5–9% and that the excess arrives through the map sawtooth (§1.4) at sizes nothing
has measured.

**A resident-per-disk ratio is a property of the caller's data, not of graphene.** §3.2
measures 6.40× and 0.139× on the same engine, the same index shape and the same node
count — the blob distribution alone. Every ratio in this document belongs to a 512-byte
fixture with thirteen index entries per node, and none of them should be quoted without
that shape beside it.

### 6.4 What the flip measured

R3's default landed in three steps — the format, the loader, and this — and only the
third one could be measured against the thing it replaced. Interleaved against a
control tree at HEAD, each arm building its own fixture so that the format under test
is the format that tree writes, `GRAPHENE_RSS_DIR` unset so nothing is shared:

| after Open | 50,000 nodes | 200,000 nodes |
|---|---|---|
| Go heap objects | 92.14 → 17.38 MiB | 362.0 → 66.32 MiB |
| anonymous RSS | 149.9–157.8 → 80.3–91.4 | 439.5–442.4 → 146.9–171.1 |
| file-backed RSS | 41.6–41.9 → 24.1–25.0 | 172.9–173.8 → 103.8–103.9 |
| total RSS | 191.8–199.3 → 104.4–116.0 | 613.3–615.2 → 250.7–275.0 |
| image on disk | 44.0 → 60.5 | 176.0 → 241.4 |
| resident per byte of store | 4.36× → 1.73–1.92× | 3.49× → 1.04–1.14× |

Same fixture shape as the master here — 512-byte blobs, thirteen index entries per
node — so the 200,000-node column scales by seven to the 1.4M-node store this
document is otherwise about, and that scaling is checkable rather than asserted: the
control arm's 441 MiB of anonymous memory scaled by seven is **3,087 MiB against the
2,909 this document measured directly**, a 6% disagreement between two harnesses and
two fixture builds. The mapped arm's 159 MiB scales to **~1,113 MiB**, which is the
row in §6.3. That scaling has since been checked against a direct run at 1,400,000
nodes and over-predicts by almost exactly two: the measured figure is **560.8 MiB**, and
§9.3 says which part of an anonymous reading does not scale.

**Why that is 1,113 and not the 607 projected.** The projection summed live
structures: 161 measured with no index, + 402 for the composites, + ~44 for R3's own
resident cost. What §6.3's column reports is *anonymous RSS*, which is the Go
runtime's retained address space and not the live set — heap objects at 200,000 nodes
are 66.3 MiB, and scaled that is 464 MiB, comfortably inside the sum. The projection
was not wrong about the structures; it was answering a different question from the
column it was written into. The measured row is the one to plan against, because it
is the number an OOM killer reads.

**The file-backed half fell while the file grew.** 172.9 → 103.8 MiB of resident
file pages against an image that went from 176 to 241 MiB. Rebuilding the index meant
walking every byte of GIDX, so the arrangement this replaced made essentially the
whole image resident on the way past; reading a key directory touches a fraction of a
larger file. It is the one result here that was not predicted at all.

**What it costs.** A warm point lookup is ~100 ns resident against ~190 ns mapped at
60,000 entries — it roughly doubles, inside the 0.3–0.8 µs the plan budgeted. Open is
*faster*: 81–108 ms → 21–28 ms at 150,000 entries, because one arm rebuilds the
entries and the other reads a directory. Compaction is 15–24% slower at 200,000 nodes
with its peak unchanged. Cold lookup latency is still unmeasured and is the open
question, not the warm figure.

**The composites are still resident**, exactly as the caution below says, and are now
the largest remaining term. Nothing has measured a store that declares many of them.

> **Superseded, v0.8.0 — both of these were measured, and one of them moved.** The cold
> question has an answer and a caveat. `tests/coldlookup_test.go` reads an image whose
> pages have been made non-resident and reports, at 1,400,000 nodes against the
> 1,689.1 MiB image, an index point lookup at **4.8–6.1×** its resident cost and a record
> point read at **20.6–26.1×** — so the mapped lookup's warm doubling is a small part of
> what a read costs when its pages are not there, and the page the index touches amortises
> across lookups where the payload page does not. The caveat is the instrument: the windows
> arm trims the working set, which leaves the pages on the standby list a soft fault away,
> so its column is headed *trimmed* and every ratio in it is a lower bound; only the linux
> arm evicts for real. `docs/benchmarks.md` carries the table, and the sample-count effect
> that makes a single ratio meaningless without it. The composite term was measured too —
> **128.2 MiB of 237.8 at 1,400,000 nodes**, the largest single item in that column — and
> §8.6 is where `GCPX` moves it out of the heap and into the image.

**Verifying the mapped index costs nothing resident.** Worth stating here because it
is the one operation that touches every entry in the index on purpose, and therefore
the obvious place for the residency win to be handed straight back. It is not:
`VerifyIndexes` holds one counter per declared key plus a buffer sized by the widest
value, measured at 416 bytes and thirteen allocations whether the index holds 25,000
entries or 100,000. The time is ~76 ns per entry. That was designed for rather than
observed — the store most likely to be verified is the one nearest its ceiling — and
`docs/TECHNICAL_DETAILS.md` §14.20 has the argument that makes an O(1)-memory check
of both directions exhaustive rather than partial.

### 6.5 Reading many values without holding many answers

A join is the one read shape whose memory cost is set by the *API* rather than by the
store. `NodesByProperty` returns a freshly allocated `[]store.NodeID` per call, which is
right for a lookup and, over a million rows, is a million slice headers and a million
backing arrays — tens of megabytes of garbage churned through a heap this document
spends six sections trying to shrink.

`NodesByPropertyBatch` is the same question asked so that the answer need not be held.
The caller gets a callback per matching value with the ids as scratch, and what the
engine holds for the whole pass is:

| term | bytes | when |
|---|---|---|
| id scratch | the widest run in the batch × 8 B | always, reused |
| the cursor | one allocation, ~48 B | always |
| sort keys | 8 B per value | only when the caller's values are not ascending |

Nothing else, and nothing per value. Measured at 50,000 values against an all-distinct
key: **2 allocations, unchanged between 200 and 2,000 distinct values**, and 0.3 MiB of
growth over the loop's 3.25 MiB — which is the 400 KiB of sort keys plus GC slack. On
ascending input there are no sort keys and the batch holds *less* than the loop.

Two consequences for anyone sizing a join against a budget. The sort scratch is
8 B × values, so a million-row join wants 8 MiB it would not otherwise need, and
supplying ascending values removes that term entirely. And the store lock is held for
the whole batch (see the locking note in `TECHNICAL_DETAILS.md` §14.21), so a batch is also
a writer stall of its own duration: split a million values into chunks rather than
passing them in one call.

### 6.6 A multi-filter query, and which half of it is bounded

A residual filter is applied one of two ways, and they have different footprints
rather than different speeds.

| route | holds | proportional to |
|---|---|---|
| probe | two allocations per candidate | the candidate set |
| build | one id slice, plus the sort | the residual filter's own set |

Neither is free and neither is always smaller, which is why the planner picks. What it
picks from is a cost in *reads of the image*, so under a mapped index it prefers the
build wherever the probe would be many searches — and the effect on residency is
measurable in both classes at once. Over a 200,000-node store with 2,000 candidates
and a 20,000-entry residual: the probe route leaves the process holding **5.58 MiB of
mapped image pages** and makes 4,006 allocations; the build route holds **0.85 MiB**
and makes 30. Total residency after the pass, 73.5 MiB against 68.9 MiB.

Two consequences for sizing a query against a budget. A residual build is bounded —
`residualBuildCap`, four million ids or 32 MiB — and past that bound the probe is taken
however slow it is, because a probe's footprint does not scale with the filter. And the
file-backed half of a probe is not something a heap budget sees: it is page cache the
kernel can reclaim, but it is resident while the query runs, and on a machine at its
ceiling that is the difference between reclaiming and swapping. `ExplainNodeQuery`
names the route per residual step, which is the only way to know which of these two
rows a given query is paying for.

### 6.7 Adjacency, and the process that never asks for it

Adjacency is not in the file. It is four arrays derived from the edge records while
the image loads, and its cost is arithmetic rather than measured:

| array | size | at 900k edges / 150k nodes |
|---|---|---|
| `outEdges` + `inEdges` | 8 B — live edge, each direction | 14.4 MiB |
| `outOffset` + `inOffset` | 8 B — node slot, each direction | 2.4 MiB |

All of it anonymous. Measured at 15.5 MiB against the 16.8 the table predicts, which
is the closest agreement between arithmetic and instrument anywhere in this document
and is what a derived array should look like.

**Who pays it.** Everyone, until now — including a process that opens a store to read
properties out of it and never touches an edge. `Options.Adjacency` is how that
process says so, and `AdjacencyLazy` then builds nothing: 15.5 MiB not held and 11%
off the open.

**Who cannot avoid it, which is the finding that keeps this from being a default.**
Any writer that deletes. `DeleteNode` cascades to incident edges and finds them
through these arrays, so a lazily opened writer builds them on its first delete. The
consumer this program is aimed at runs two processes — a rebuild that deletes 1.5M
nodes per cycle, and a read-only aggregate that opens and reads ten rows — and the
option is worth exactly nothing to the first and its full value to the second. That
asymmetry is why the engine does not choose: it cannot see which process it is in.

**Why the deleting writer cannot be given a cheaper route, which closes the
question rather than deferring it.** The obvious follow-up is to serve the delete
cascade from something other than the adjacency arrays, so that a deleting writer
could stay lazy too. There is nothing to serve it from. `DeleteNode` reaches
`incidentEdgeIDsLocked` → `reader.incidentEdgeIDsOf` → `csrEdgeIDs` →
`CSRGraph.OutboundEdgeIDs`, whose first statement is `ensureAdjacency()`; and
`EdgesOf`, `Neighbours`, `DegreeOf`, `IncidentEdges` and `EdgeBetween` all land on
the same four arrays, so the cascade is not even a special caller. There is no
index over `Edge.Src` or `Edge.Dst` anywhere in the engine — the edge arena is
`rawEdge` records addressed by identifier — and adjacency has not been written to
the file since v7 (`csrVersionNoAdjacency`), so there is no section to read
instead. The only remaining route is to scan the edge arena per delete: O(edges)
**per delete**, against one O(edges) build amortised over every delete in the
process. For a writer that deletes once it is a wash, and for the rebuild this
document is written against — 1.5M deletes per cycle — it is worse by six orders
of magnitude.

So the asymmetry is structural, not an omission. The build is the cheap way to
answer "which edges touch this node", it is paid once, and `AdjacencyLazy` moves
*when* it is paid rather than *whether*. A process that deletes pays it at the
first delete plus a branch on every read until then; a process that does not
never pays it at all. That is the whole of what the option does, and there is no
third outcome hiding behind an index someone has not written yet.

**What it does not cover, and why that is now a decision rather than a deferral.**
The label postings (`nodesByLabel`, `edgesByLabel`) are also derived, also
anonymous, also built at open — and at every compaction, inside `buildSeq` — and
also unread by a property-only pass. Deferring them would be a near-exact
structural clone of what `Options.Adjacency` already does: `buildSeq` takes the
mode parameter already and gates adjacency three lines earlier. The option was not
built. The reason is a measurement, and it is not the one this section expected.

> **Retracted: "at 2M edges they are the larger term of the two."** That sentence
> stood here as an assertion with nothing behind it. §9.7 measures both terms at two
> shapes under a real ceiling, and it is backwards. The postings are **8.0 bytes a
> node** and adjacency is **16.0** — so the postings are *half* the term they were
> said to exceed: 10.7 MiB against 21.4 at 1,400,000 nodes, 13.7 against 28.8 at
> 1,800,000. The two readings agree to a hundredth of a byte across a twentyfold
> difference in blob size, so this is the coefficient and not an artefact of a
> shape.

**What deferring them would buy, and why the size alone does not settle it.** 10.7
MiB is 4.5% of the 237.8 MiB the model accounts for at the consumer shape and 2.6%
of the 397.8 MiB actually charged against the 2 GiB ceiling. The gate this release
set was "under about 2% of the total and it is disproportionate", and 2.6% is on
the wrong side of that line by a whisker. The honest reading is that the number is
marginal: it neither clears the bar nor fails it cleanly, and anyone who wanted to
build the option could quote the 4.5% and be arguing in good faith.

**What settles it is that the cost is moved rather than removed.** `NodesByType`
and `CountNodesByType` read these postings. Adjacency earns its option because a
property-only process genuinely never asks the question — it opens, reads
properties, closes, and the arrays were pure waste. A label query is not like that.
It is an ordinary read on the ordinary path, so a process that defers the postings
and then runs one `NodesByType` builds all of them at that query, having also paid
a branch on every read until then. The asymmetry that makes `Options.Adjacency`
worth its complexity is much weaker over a term half the size, and the retraction
above removes the one argument — that the postings were the *larger* of the two —
that made them look like the better target of the pair.

Against that: a new mode type, an `Options` field, a `StorageStats` answer field, a
fallback rule and their tests, and one more dimension on the
`imageMappingAllowedFor` × `indexBaseAllowedFor` × adjacency matrix that is already
the hardest thing in this engine to reason about. **So the open question above is
closed: do not build it.** Not because the term is negligible, but because it is
half of what this section claimed, and because deferring it relocates the build
onto a query rather than retiring it. That is a measured reason to stop, which is
worth more than the option would have been.

**Where the rest of the edge cost is.** `[]rawEdge` is 80 bytes per edge — 160 MiB at
2M edges, an order above the adjacency arrays — and it is a record array, not derived
state. Nothing can defer it; R10(b)'s page table is what bounds it, by making it
proportional to live edges rather than to the highest identifier ever issued.

### 6.8 An answer nobody holds

§6.5 is about a join — many values, each with a small answer. This is the other shape:
one value with an enormous answer. A key that discriminates coarsely (a tenant, a type,
a status flag) has a run as long as the store, and `NodesByProperty` returns it as an
array sized from the image's run plus the delta's list.

| term | slice form | streamed form |
|---|---|---|
| the answer | 8 B × identifiers matched | — |
| the batch | — | 8 B × 512, reused |
| the cursor | — | one pending identifier, two indices |
| the delta's list | 8 B × delta entries for that value | the same |

Measured over 500,000 nodes under one value: **4,005,888 B/op and one allocation becomes
128 B/op and three**, and the residency sampled halfway through the pass falls from
69.8–70.1 MiB to **64.18–64.20 MiB** — 5.6 MiB, which is the four megabytes of array
plus the collector's headroom over it. The streamed arm's spread across three passes is
0.02 MiB against the slice arm's 0.28, which is what a bounded working set looks like on
an instrument this noisy.

The query form is worse before it is better: `QueryNodeIDs` on the same shape allocates
**8,011,936 B** in four allocations, because the candidate set and the liveness-filtered
result are two arrays. `ForEachNodeID` on the streamable shape holds the same 128 B as
the property form.

Three consequences for sizing a read against a budget.

**The saving is the base's, not the delta's.** The delta's list for the value is still
copied, because the alternative is holding a shard lock across caller code. The delta is
entries since the last compaction and the base is the store, so the term that scales is
the one removed — but a store that has not been compacted in a long time gets less of
this than a freshly compacted one.

**It costs 1.34x to 1.38x the wall clock**, which is a cursor call and a callback per
identifier. Under this program's priority that is a trade worth making and it is not a
trade worth making blind: a caller that genuinely wants the array should keep asking for
it.

**What the caller then does with each identifier is not bounded by any of this.** The
pass holds 128 bytes; a callback that appends every node it loads holds the whole store.
The API bounds the engine's side of a read, which is the half this document can speak
for.

### 6.9 The engine's own account of itself

Everything above is measured from outside: a process is built, settled and read through
`/proc` or `K32GetProcessMemoryInfo`. That is the right instrument for a document and
the wrong one for a running store, which cannot stop and re-measure itself before
deciding whether to compact. So `StorageStats` now carries the engine's own answer, and
this section is where the two are compared.

`EstimatedResidentBytes` totals seven terms. `disk.Store.EstimateResident` returns them
individually, which is the form worth reading — a total tells an operator their store is
large, the split tells them which of the four things they can change would change it.

| term | how it is obtained | what it scales with |
|---|---|---|
| record arrays | slice lengths — exact | pages of the identifier space that hold a record |
| payload | arena capacity at load, records referenced at build | labels always, blobs unless mapped |
| label postings | summed over distinct labels — exact | one identifier per (record, label) |
| adjacency | slice lengths, zero while deferred — exact | node slots and edges |
| index | maintained counts × measured bytes/entry, plus the base directory | entries **since the last compaction** |
| delta | maintained byte counter, plus modelled structure | writes since the last compaction |
| log | the ring — a constant | nothing |

The counts are exact everywhere. What is modelled is what one item costs, and three
figures carry that: 107 B per resident index entry (the same number, arrived at the same
way, as `preflight.go`'s), 32 B for an indexed value where a structure scales with
distinct values, and a Swiss-table load factor of 7/8 for the structural maps. Mapped
image bytes are reported beside the total and deliberately not in it: they are page cache
the kernel may evict, and a total that included them would invert the comparison a
mapped image exists to win.

**Measured against retained heap**, three sizes, 256 B blobs and two indexed keys per
record, compacted and reopened:

| records | estimate | retained heap | ratio |
|---|---|---|---|
| 2,000 | 356,238 B | 396,824 B | 1.11× |
| 8,000 | 711,166 B | 756,720 B | 1.06× |
| 32,000 | 2,720,734 B | 2,780,240 B | 1.02× |

Differenced across the ends: **78.8 B per record modelled against 79.4 measured, a ratio
of 0.99.** The totals are compared as a slope rather than at one size for the reason
`TestFootprintGuard_OpenSlope` does — both figures carry a fixed overhead that has
nothing to do with the data, and differencing cancels it. The estimate is also a floor at
every size, which it must be: it omits allocator slack, size-class rounding and the
runtime's own metadata by construction.

Against RSS the ratio is a different and much looser number — 1.19× to 1.54× on the one
fixture measured for it, varying across runs of an identical store — which is why the
estimate is compared against retained heap and why the CLI prints the ratio beside the
figure rather than letting an operator size a machine from it directly.

**What the breakdown says about this engine.** The same fixture, at two sizes:

| term | 2,000 records | 32,000 records |
|---|---|---|
| record arrays | 229,396 | 1,835,140 |
| label postings | 16,094 | 256,094 |
| adjacency | 65,552 | 524,304 |
| log ring | 40,960 | 40,960 |
| payload | 4,000 | 64,000 |
| **index** | **236** | **236** |
| total | 356,238 | 2,720,734 |
| mapped image, evictable | 777,573 | 11,907,573 |

Three things are worth reading off that table.

**The index is 236 bytes for 64,000 entries.** Held resident at the measured 107 B/entry
it would be 6.85 MB, so this is the whole of R3 stated from inside the engine: a factor
of about 29,000 on this fixture, and the figure does not move between the two sizes
because nothing in it is a function of entries. The shipped base is even cheaper than the
plan's — there is no resident fence over the value table at all, because the table is
binary-searched in the mapping where it lies, so what is left is the key directory and
two slice headers.

**Records are now the largest term, and page granularity is visible in it.** 57.3 B per
record at 32,000 and 114.7 B at 2,000 — the same 56-byte record, paid for a whole
4,096-slot page at a time. A store far smaller than one page pays for one page, which is
the floor R10(b) traded the unbounded identifier tax for, and it is a floor rather than a
slope.

**The blobs are absent.** `payload` is 2 B per record at both sizes, which is the one
label each, because the 256-byte blobs are in the mapped column instead.

## 7. Target architecture

The store should hold, per live record and in anonymous memory, only what cannot be
addressed in a file:

- **records**: the CSR arena, 56 B/node and 80 B/edge, dense over live pages. Measured,
  shipped.
- **adjacency**: 16 B/node and 16 B/edge, built eagerly for a writer and on demand for a
  reader that never deletes. R4.
- **index**: a fence over each key's value table plus the key directory — kilobytes per
  key, not bytes per entry — with everything since the last compaction in a delta whose
  size is bounded by the compaction policy. R3, plus the composite follow-up.
- **blobs**: nothing. They are a sub-slice of the mapping. Measured, shipped.
- **transients**: the new graph a compaction must build before publishing it (118 B/node,
  §4.1) and nothing else. The delta payload copy is the one remaining term that scales
  with anything other than the live set.

Everything else is file-backed and evictable — and §3.2 demonstrates the eviction rather
than assuming it: 929 MiB of mapped image holding 20 MiB resident under pressure, with no
movement in the anonymous figure.

On this fixture that architecture is **about 205 MiB of anonymous memory for a 1.2 GiB
store, a ratio of 0.17× against today's 3.351×**, and it is R3 plus the composite
follow-up that gets there. The no-index arm has already measured it.

## 8. Sizing a configuration

Everything above measures one arrangement at a time. Three options decide which
arrangement a store is in — `ImageMode`, `IndexMode` and `Adjacency` — and this is
the cross-product: which term each of them moves, by how much, and which of the
combinations are configurations at all.

Conditions for this section only. 200,000 nodes, 200,000 edges, 512-byte blobs, the
same declared shape as the rest of this document — 8 unique keys, 5 ordered, 2
composites — compacted once into a v9 image of **248.1 MiB**, holding **2,600,000
simple index entries and 400,000 composite entries**. Fourteen mode specifications,
three rounds, one process per specification, `BenchmarkRSS_ModeMatrix`. The fixture is
built once and shared, so every figure is a bare Open and not the residue of a build.

### 8.1 Which option moves which term

The engine's own models split the same seven ways, before an open (`OpenEstimate.HeapBytesFor(opts)`,
which takes the `Options` for exactly this reason) and during one (`Store.EstimateResident`).
Reading the two side by side is what this table is:

| term | what it is | scales with | moved by |
|---|---|---|---|
| record arrays | 56 B per node slot, 80 B per edge slot, 4 B per 4,096 identifiers ever issued | materialised pages | **nothing** |
| payload | label sequences always; property blobs unless the image is mapped | records, then blob bytes | **ImageMode** |
| label postings | 8 B per (record, label), plus two map headers | records | **nothing** |
| adjacency | 16 B × (node slots + 1) + 16 B × live edges | slots and edges | **Adjacency** |
| index | 107 B per resident entry; a key directory when mapped. Composite entries are 160 B under **both** | entries since the last compaction | **IndexMode** |
| delta | maintained payload counter plus modelled structure | writes since the last compaction | compaction, not an option |
| log ring | 1,024 × 40 B | nothing | **nothing** |

Three of the seven are the same under every configuration, and one of those three is
the term that decides how long a store can live (§5).

### 8.2 The measured terms, per configuration

MiB, from `EstimateResident` — the engine's own account, identical in all three rounds
because every term is a count of something rather than a sample of it:

| held configuration | records | payload | labels | adjacency | index | mapped (evictable) | total heap |
|---|---:|---:|---:|---:|---:|---:|---:|
| image mapped, index mapped, adjacency deferred | 26.03 | 0.76 | 3.05 | 0.00 | 61.04 | 248.1 | **90.9** |
| image mapped, index mapped, adjacency built | 26.03 | 0.76 | 3.05 | 6.11 | 61.04 | 248.1 | **97.0** |
| image mapped, index resident, adjacency deferred | 26.03 | 0.76 | 3.05 | 0.00 | 334.30 | 248.1 | **364.2** |
| image mapped, index resident, adjacency built | 26.03 | 0.76 | 3.05 | 6.11 | 334.30 | 248.1 | **370.3** |
| image heap, index resident, adjacency deferred | 26.03 | 150.20 | 3.05 | 0.00 | 334.30 | 0 | **513.6** |
| image heap, index resident, adjacency built | 26.03 | 150.20 | 3.05 | 6.11 | 334.30 | 0 | **519.7** |

And the same six measured from outside the process, anonymous and file-backed
separately, as the range over three rounds:

| held configuration | anon MiB | file MiB | total RSS | Open ms |
|---|---|---|---|---|
| mapped / mapped / deferred | **138.2–143.0** | 73.9–83.8 | 216.4–222.0 | 227–256 |
| mapped / mapped / built | **146.0–149.0** | 74.3–91.1 | 220.3–240.1 | 216–261 |
| mapped / resident / deferred | **440.2–442.1** | 148.3–185.7 | 589.1–627.6 | 1,722–3,772 |
| mapped / resident / built | **446.9–447.6** | 185.2–185.5 | 632.4–632.8 | 1,769–1,871 |
| heap / resident / deferred | **561.4–561.7** | 0.0 | 561.4–561.7 | 1,648–1,905 |
| heap / resident / built | **567.7–568.1** | 0.0 | 567.7–568.1 | 1,635–2,055 |

**The cheapest configuration holds a quarter of the anonymous memory of the dearest**,
138 MiB against 568, on one store that neither of them changed. **And the row with the
largest total RSS is not the row with the largest anonymous figure** — `mapped/resident/built`
totals 632 MiB against `heap/resident/built`'s 568, while holding 121 MiB *less* of the
class a RAM ceiling constrains. A combined number would rank those two backwards, which
is the fourth rule in this document's opening stated as a row rather than as advice.

### 8.3 The cube is not a cube

Twelve specifications, and a locked reader can reach **six** of them. The collapses are
not degradations to be fixed; each is a rule with a reason, and the instrument reports
what is *held* beside what was *asked* so that a table cannot present a fallback as a
result.

**A heap image cannot carry a mapped index.** `heap/mapped/*` measures byte-for-byte the
same as `heap/resident/*` — 334.30 MiB of index either way. An index read in place out of
a heap buffer would pin the whole file to save part of it, so `indexBaseAllowedFor`
declines, and four specifications become two. It is reported through `MetricIndexFallback`
and is otherwise invisible: nothing fails, and the store is simply 273 MiB larger than
the caller configured it to be.

**`ImageMappedUnlocked` is `ImageMapped` for anyone holding a lock.** Four more
specifications are duplicates of four others — 144.1–149.7 against 146.0–149.0 anon on the
default cell. The two modes differ only for an opener that excludes no writer, which is
§8.5.

**Adjacency is the one option that is orthogonal to both others.** 6.11 MiB in every
configuration that builds it, to the byte, whatever the image and index are doing.

### 8.4 What each option is worth, and what it costs

Differenced from the term table, so each figure is the one option and nothing else:

| option | term moved | worth on this store | what it costs |
|---|---|---|---|
| `IndexMapped` over `IndexResident` | index | **273.26 MiB** | Open 1,722–3,772 ms → 216–261 ms, so it is *not* a cost here; a warm point lookup roughly doubles (§6.4) |
| `ImageMapped` over `ImageHeap` | payload | **149.44 MiB** of heap, moved to 248.1 MiB of evictable page cache | returned slices address the file: valid for the life of the handle, not past `Close()` (§6.4, `CloneNode`) |
| `AdjacencyLazy` over `AdjacencyEager` | adjacency | **6.11 MiB** | nothing for a reader that never traverses or deletes; the same bytes later for one that does |

The three are independent and additive: 273.26 + 149.44 + 6.11 = 428.8 MiB, against the
519.7 → 90.9 MiB the total column actually moves, which is 428.8. That is not a
coincidence to be pleased about — it is the check that no term is being counted twice,
and it is the reason the split is worth reporting at all.

**The index arithmetic, worked.** The 273.26 MiB that `IndexMapped` removes is:

| part | count | bytes each | MiB |
|---|---:|---:|---:|
| simple postings | 2,600,000 | 107 | 265.31 |
| ordered indexes, five keys × 1,000 distinct values | 5,000 values + 1,000,000 ids | 72 + 8 | 7.97 |
| **total** | | | **273.28** |

Measured: 273.26. The model and the instrument agree to 0.01%, which they should, because
both are counting the same entries — what is being checked here is that the entries the
mapped arm *stops* holding are exactly the ones the resident arm holds.

**And the 61.04 MiB that no option removes is the composites**, exactly: 400,000 entries
× 160 B = 61.035 MiB. On this store they are 61.04 MiB of the 90.9 a fully mapped
configuration holds — two thirds of it — they are resident under every one of the twelve
specifications, and they are the largest remaining item in this document. §6.4 named them
as the next thing to look at; this is the figure that says how much is in it.

**Since measured: the model's coefficient is 48, not 160.** What that 160 B per entry
mostly was is a second copy of the member values — one `*memberState` per entity, a struct
and a
backing array of string headers and a copy of every value's bytes, behind a pointer in a
map. Splitting the composite index in two says how much: over 200,000 entities of a
width-2 composite the postings cost 11.02 B per entry and the member values **135.65**,
flat across tuple shapes, so about 85% of the term was the copy. Replacing it with a row
of int32 references into an interned value table measures **146.60 → 43.87 B per entry**
(twelve interleaved samples) and, on a 200,000-node store reopened from disk, **124.40 →
89.08 MiB of anonymous memory** and 66.31 → 33.09 MiB of Go heap.

Three numbers appear above and they are not alternatives. **146.60** and **43.87** are the
measured per-entry costs of the old and new forms at the shape a composite is declared for;
**48** is the constant in `index/resident.go`, which is the old 160 scaled by that same
ratio (160 × 43.87/146.60) so that it stays on the basis the 160 was chosen on. The
estimator uses 48; the measurements are 146.60 and 43.87.

The table above is left as the record of the tree it was taken on and has not been re-run
at this scale. Scaled by the measured ratio the 61.04 MiB row is about 18.3 MiB, which
would stop the composites being the largest thing a fully mapped configuration holds *at
this shape* — §8.6 works the same scaling at 1.4M nodes, where they still lead. The
follow-up §8.6 asks for — the composites on disk — is unchanged and unstarted; what moved
is the coefficient it has to beat.

### 8.5 The live reader takes the defaults and pays 3.8×

The one configuration in the sweep that nobody would choose deliberately, and the one a
caller reaches by writing nothing at all:

| opener | ImageMode asked | image held | index held | anon MiB |
|---|---|---|---|---|
| read-only | `ImageMapped` (default) | mapped | mapped | **146.0–149.0** |
| `OpenLive` | `ImageMapped` (default) | **heap** | **resident** | **567.7–568.2** |
| `OpenLive` | `ImageMappedUnlocked` | mapped | mapped | **144.1–149.6** |

`ImageMapped` maps only where something excludes a concurrent writer from the directory,
and a live reader holds no lock by construction — that is what makes it live. So it falls
back to a heap image, and a heap image then declines the mapped index by §8.3's first
rule. **One option not set costs 419 MiB on a 248 MiB store**, both fallbacks together,
and the second is a consequence of the first rather than an independent decision.

This is the correct default: mapping a file a writer may rewrite underneath you fails
in ways a caller cannot handle, and the shape differs by platform
(`docs/TECHNICAL_DETAILS.md` §15.14). Shortening the file removes pages a slice still
addresses, and on unix the next access is `SIGBUS` — unrecoverable in Go, at whatever
unrelated line touched the slice; windows refuses to shorten a file with a live mapping
at all, so that hazard does not exist there. Overwriting bytes in place is permitted on
both, and silently changes what the mapping reads. An engine may not choose either for a
caller who has not asked. `ImageMappedUnlocked` is how a caller asks, having read what it
trades. What the sweep adds is the price of *not* asking, which was previously documented
as a mode difference and not as a number.

*Since written*: the number is now where the charge is made rather than only here.
`graphene.OpenLive`'s doc comment carries the 419 MiB, names `ImageMappedUnlocked` as
the way to ask, and states the second fallback as a consequence of the first — a caller
reading only the image fallback would otherwise conclude that asking for the mapping
buys back the image half alone. `OpenEstimate.FallbacksFor(Options)` reports the same
cascade before the open; `store.MetricImageFallback` and `store.MetricIndexFallback`
already reported it at the open, which is one fsync too late to act on.

### 8.6 The consumer's store, per configuration

The terms applied to the shape this program was aimed at — 1,400,000 nodes, no edges,
512-byte blobs, thirteen entries per node and two composites, a 1.2 GiB image. A
projection from the coefficients above, not a measurement, and stated as one:

| term | mapped / mapped / lazy | mapped / resident / eager | heap / resident / eager |
|---|---:|---:|---:|
| record arrays | 74.8 | 74.8 | 74.8 |
| payload | 2.7 | 2.7 | 1,232 |
| label postings | 10.7 | 10.7 | 10.7 |
| adjacency | 0 | 21.4 | 21.4 |
| index, simple | 0.0 | 1,911 | 1,911 |
| index, composite | 427.2 | 427.2 | 427.2 |
| **heap** | **515** | **2,448** | **3,677** |

The first column defers adjacency and the other two build it. Building it costs
**21.4 MiB** here — all of it the two offset arrays, since this shape has no edges —
so the default configuration of the first column is **537**, and that is the one to
compare against a measured arm.

Two things to read off it before trusting it.

**The projection is a floor, and the gap is additive rather than proportional.**
Anonymous RSS is the Go runtime's retained address space rather than its live set, so
every row here sits below what a process reports. Against the two arms this document has
measured at this scale — §1.1's 2,909 MiB with a resident index and §6.4's ~1,113 MiB with
a mapped one, both of them adjacency-eager, so both against the 2,448 and 537 rows — the
gap is **461 MiB** and **576 MiB**: the same order on rows that differ by a factor of
nearly five. Read it as a fixed overhead to add, not a multiplier to apply, and
size a machine from a measured row. What the projection answers exactly is the other
question: *which term to attack*.

**The composite term is now the largest thing a default configuration holds**, at 427 MiB
of the 537. Nothing in the shipped program moves it; it is the R3 follow-up, and §8.4's
160 B per entry is the coefficient it would have to beat.

*Since measured*, and see §8.4: 160 B per entry was mostly a second copy of the member
values, and the columnar rows that replaced it measure 43.87. This row has not been taken
again at 1.4M nodes — scaled by that ratio the 427.2 MiB would be about 128. That takes
the default column's total from 537 to about 238, but it does not change which term leads:
at ~128 MiB the composites are still the largest single item in that column, ahead of
record arrays at 74.8. What changed is the size of the prize, not its rank. The item
itself, composites read from the image rather than rebuilt into the heap at open, is
unchanged and unstarted.

*Also since measured*: the term no longer has to be derived from this table.
`ResidentEstimate.Composite` reports it directly, taken out of `Index` rather than
summed into it, so a store can be asked what its own declarations cost instead of
being compared against a projection taken on another shape. It is exact in its counts
and free to read — the loop was already there, per declaration, and was summing itself
away. That is what the arm at 1.4M read the number off — 128.2 MiB, in the term-by-term
line of every arm in section 9.8 — and it is what a caller who declares composites should
look at before concluding that a mapped index brought them under a ceiling.

#### The composite index on disk: GCPX

This was the largest remaining term, and it is now a section of the image rather than a
structure in the heap.

**What it was.** At 1,400,000 nodes the declared composites held **128.2 MiB of the
237.8 MiB a default configuration holds — 53.9%** — and 164.8 of 305.8 at the
1,800,000-node shape, the same fraction to a tenth of a point across a twentyfold
difference in blob size (§9.7). It was the largest single item in that column, ahead of
record arrays at 74.8, and `ResidentEstimate.Composite` reports it directly.

**Why it was the part GPIX left behind.** GPIX made the single-key postings readable in
place, and `index/composite_base_test.go` states why the composites could not follow: a
composite is an index over a tuple of keys that the forward direction holds *separately*,
so answering one out of GPIX would mean intersecting the member keys' runs on every query
— exactly the work a composite is declared to do once. So they were filled into the heap
at open instead, by `fillCompositeFromBase`, and stayed there for the life of the handle.

Nothing in that argument is wrong. What it establishes is that a composite cannot be
cheaply *derived* from GPIX, which is a reason to **store** it — not a reason to store it
in memory. GCPX stores it.

**The one in-codebase argument against doing this, answered rather than bypassed.**
`appendCompositeSection` says of the declarations-only GCMP section:

> *the derived structure cannot go stale relative to the entries if it is always derived
> from them, and the bytes it would have taken buy nothing that the entries do not already
> hold.*

The staleness clause still holds and GCPX takes it unchanged: a base is only ever installed
by the code that just wrote it from the index it replaces (`index.SwapBase`'s precondition),
and everything since is `base − retracted ∪ delta`. The second clause was an argument about
the *image*, made when the index was rebuilt into the heap at open and the only question was
how large the file was. Once the index is read in place, those bytes buy the difference
between a mapped structure and a resident one.

**The format question was settled before the work started, and favourably — but not by the
route the plan assumed.** The plan proposed extending `GCMP`, which is already registered
*optional, non-critical*. That would have broken older readers: GCMP is not unused, it
carries the composite **declarations** and a v0.7.x reader parses its body, so appending
postings would hand an older build bytes it reads as a malformed declaration list and it
would **fail the image rather than skip it** — inverting the exact guarantee the optional
flag exists to provide. GCPX is a magic of its own, optional and non-critical, and
`checkCriticalSections` skips a non-critical magic it does not understand. `GORD` is the
precedent, stated in the same file. **No version bump past v9, no stranded readers, no
bidirectional migration**, and the conclusion the plan reached survives its argument not
doing so.

**The random-seed question needed no answer.** `index/composite_index.go` seeds each value
table's `maphash` per table on purpose — property values are caller-supplied, and a fixed
seed would let a caller choose values that collide — and a section cannot carry a
per-process random seed. GPIX met this and answered it by not hashing: it is sorted and
binary-searched. GCPX is GPIX's layout with an encoded key tuple where GPIX has a value.

**The tuple encoding is the index's, in both directions, and that is load-bearing.** The
writer is handed the map keys of the resident postings and the reader hands the same bytes
back, so `disk` never parses or constructs a tuple. This matters because
`index.encodeTuple` is little-endian length-prefixed and therefore **not order-preserving**
with respect to the values it encodes — two implementations that disagreed by a byte would
produce a file whose binary search is bounded, silent and wrong. One encoder, no agreement
to maintain.

**What a write to an entity the image already describes has to do.** This is the single
correctness question a base-backed composite raises that a filled one does not. A composite
files a tuple only from a *complete* row, and over a base-backed composite an entity the
image holds has no row at all — so a caller adding one member value to such an entity would
create a row holding that value alone, find it incomplete, and file nothing. The tuple would
be held by neither side. So a row created for an entity the base knows is **hydrated** from
the base's reverse direction first; see `index.baseSide.hydrateCompositeRow`. It costs one
reverse walk per entity *written*, never per entity in the base, which is the same shape as
the rest of the delta.

**What it costs at open in damage detection, stated because it is a real change.** Filling
the composites used to read every run under every member key, so a GPIX run that would not
decode was met at open and refused. That was never a verification pass — it was a side
effect of one, it covered only the member keys of declared composites, and a store with no
composite had no such check. With the postings read in place the fill is skipped, so those
keys now behave like every other key in the image: damage is met by the read that touches
it, recorded by `BaseFault`, and reported by `Verify` and `VerifyIndexes`. An open that
wants the O(entries) pass asks for it with a `Verifier`.
`TestIndexMapped_DamagedRunSurfacesAsAFault` pins both halves.

**Who it is worth it for.** At the shape this document is written against it was the biggest
prize left. At the audited consumer's shape — 387,000 records and two composites, one with
no reader at all — it is worth roughly 32 MiB, which is an engine win rather than a win for
the integrator who asked for the work. §9.8's acceptance was already passing at 37.2%
headroom before it landed, so nothing was blocked on it; what it buys is margin, and §9.8
measures how much.


## 9. Under a ceiling

Every section above reports a figure. This one reports a verdict, because the question
the program was started to answer is not "how many bytes" but "does it fit on the
machine" — a 2 GiB box, where slower is acceptable and OOM-killed is not.

The instrument is `tests/ceiling_test.go` behind the `stress` tag, and the sequence it
runs is the consumer's own: open, resolve ten rows through the index, enumerate the live
set, rebuild the derived layer, compact, reopen and check the result is still there.

### 9.1 What a ceiling is, and why the harness reads it back

A ceiling is only worth measuring under if the kernel is enforcing one, so the harness
refuses to produce a result until it has read back a limit at least as tight as the
figure the run claims. A CI step whose limit silently failed to apply would run this
workload unconstrained and go green, and nothing in the output would say so — the same
failure as an option asked for and not held, which is why `StorageStats` reports what is
*held* and why §8 prints both.

| | linux | windows |
|---|---|---|
| instrument | cgroup v2 `memory.max` | Job Object `JOB_OBJECT_LIMIT_JOB_MEMORY` |
| what it counts | resident bytes, page cache included but reclaimable | commit charge; a read-only file mapping counts for nothing |
| applied by | the caller, `systemd-run --scope` | the harness, on itself |
| read back from | the tightest `memory.max` from this cgroup up to the mount | `QueryInformationJobObject(NULL, …)` |
| exceeding it | the OOM killer | `VirtualAlloc` fails with `ERROR_COMMITMENT_LIMIT`, then `fatal error: out of memory` |

The asymmetry is not a preference. A process cannot place itself under a cgroup limit
without privileges, and there is no convenient way to apply a Job Object from outside. So
one platform is wrapped and reads an independent limit; the other applies its own and
therefore has to prove that limit binds, which `TestCeiling_TheAppliedCeilingBinds` does
in a subprocess by committing four times the ceiling and requiring the kernel to refuse.

**`RLIMIT_AS` is the wrong instrument and would have inverted the result.** It bounds the
virtual address space, so it counts every byte of the mapped image — 1,689 MiB of it here
— even though none of that is charged to the process. A run under `ulimit -v 2G` would
refuse a store that fits comfortably, and it constrains precisely the term this whole
program moved *out* of the constrained class. `RLIMIT_RSS` is unenforced on every modern
kernel.

**The peak is reported and never asserted.** A process that exceeded its ceiling is dead,
so `peak < ceiling` can only ever be evaluated when it already passed. What is asserted is
that the sequence completed, that the store afterwards holds what it should, and that a
ceiling at least as tight as the one quoted was in force while it happened. The peak, the
headroom, and on linux the cgroup's own reclaim and OOM counters are diagnostics: they say
whether a pass was comfortable or bare, which the pass alone cannot.

One consequence of the commit-charge rule is worth reading off directly. In the read-only
arm below, the process holds a working set of **1,354 MiB** while charging **614 MiB**
against its 2 GiB ceiling. The 740 MiB difference is the mapped image, and it is the whole
of R2 stated as one row: those bytes are resident, and they are not the process's to keep.

### 9.2 The consumer's sequence at the shape it was sized for

1,400,000 nodes, no edges, 512-byte blobs, 8 unique keys and 5 ordered and 2 composites,
compacted into a v9 image of **1,689.1 MiB** holding **18,200,000 index entries**. One
process, defaults throughout, the delta bounded at 64 MiB. Run under a deliberately
generous 10 GiB ceiling so that nothing was hidden by dying early. Conditions as above;
the anonymous/file split is windows', which overstates anonymous and understates file-backed
by a roughly fixed process overhead — one-sided in the direction that makes this table
pessimistic rather than flattering.

| phase | anon MiB | file MiB | total RSS | peak MiB | wall | held |
|---|---:|---:|---:|---:|---:|---|
| open | **560.8** | 740.4 | 1,301.2 | 1,354.4 | 3.1 s | image mapped, index mapped |
| scan, 10 rows | 560.5 | 740.8 | 1,301.3 | — | 1 ms | unchanged |
| enumerate 1.4M ids | 571.5 | 740.6 | 1,312.1 | 1,312.0 | 42 ms | unchanged |
| rebuild the layer | **3,853.5** | 1,256.8 | 5,110.3 | **6,644.0** | 7 m 11 s | — |
| compact | 3,837.6 | 1,257.1 | 5,094.7 | 5,346.1 | 24.9 s | image **heap**, index mapped |
| reopen | **1,078.0** | 733.4 | 1,811.4 | — | 2.9 s | image mapped, index mapped |

Read the first three rows and the last three as two different results, because they are.

### 9.3 The read path fits, and §8.6's projection was right

> **Superseded in part, and not by a clean comparison.** §9.7 re-runs this arm on the same
> fixture under v0.8.0 and reads **320.3 MiB settled anonymous and 397.8 MiB charged, 80.6%
> headroom** — against the 560.8 and 613.9 below. Two things changed at once and this
> document will not pretend to separate them. The figures below were taken **on linux under
> a cgroup**; §9.7's were taken **on windows under a Job Object**, and the two kernels
> retain freed anonymous pages differently. The composite coefficient also moved in between,
> from 160 to 48 bytes an entry (§8.4), which alone accounts for 299 MiB of the modelled
> 537.1 → 237.8. **The figures below are kept because they are the linux reading and there
> is no newer one**; what is retracted is the sentence after next.
>
> **"§8.6's projection was accurate to 24 MiB" no longer holds** — it was accurate against a
> composite term the engine has since corrected. The projection to compare against today is
> §8.6's own corrected ~238 MiB, and the engine's estimate at this shape now reports
> **237.8 MiB**, which is accurate to 0.2. The conclusion is unchanged and better supported
> than it was; only the arithmetic behind it moved.


**Opening a 1.65 GiB store and reading it costs 561 MiB of anonymous memory.** Under a real
2 GiB ceiling the same arm charges 613.9 MiB against the limit and finishes with **70.0%
headroom**. Enumerating all 1,400,000 identifiers adds 11.0 MiB, which is the 10.7 MiB of
`[]NodeID` the harness itself keeps at 8 bytes each: `ForEachNodeID` streams and holds
nothing, and the row measures what the caller chose to retain because the rebuild that
follows needs the ids.

This is the first direct measurement at the target shape, and it settles two things the
document had only projected.

**§8.6's projection was accurate to 24 MiB.** It summed the terms to 537 MiB of heap for
the default configuration at this shape. The engine's own `EstimatedResidentBytes` after
the run reports **537.1 MiB**, and the measured anonymous figure is 560.8 MiB. The 23.7 MiB
between them is the Go runtime's fixed overhead, not a term anyone forgot.

**§6.4's ~1,113 MiB was a scaling, and it over-predicted by 1.98×.** That figure came from
multiplying a 200,000-node arm by seven. The fixed runtime component of an anonymous
reading does not scale, so seven copies of a small arm count it seven times: at 200,000
nodes this harness measures 123.5 MiB, of which 71.8 MiB is composites plus records
(400,000 × 160 B and 200,704 slots × 56 B) and the remaining 51.7 MiB is fixed. Scale only
the part that scales — 427.2 MiB of composites and 74.8 MiB of records at 1.4M — and the
answer is **554 MiB against the 561 a direct run measures**. **§6.3's "does the program reach 2 GiB"
row should be read against 561, not 1,113** — the program reaches it for the read path with
room to spare, and by a wider margin than this document has been claiming.

### 9.4 The write path did not fit, and the delta was not the reason

> **Superseded, v0.8.0 — the acceptance holds, and this section's title is the part that
> did not survive.** The rebuild below fails because the process is holding the payload of
> every record it has compacted, not because the delta is large; §9.8 measures a sweep of
> eight `MaxDeltaBytes` values at this shape, none of which completes it, and then completes
> it at **1,285.2 MiB of 2,048 — 37.2% headroom** by reopening after each interim
> compaction. `Graph.CompactAndReopen` is that change. The 3,853 MiB and 6,644 MiB figures
> below stand as what an in-place compaction costs and are left in place for that reason;
> what is retracted is the conclusion that the write path does not fit.

**Rebuilding the whole layer in one process holds 3,853 MiB and peaks at 6,644 MiB** —
3.2× over the ceiling. That is the plan's end-to-end acceptance, and it does not hold.

The first suspicion was the delta, and it is wrong. A rebuild writes thirteen index entries
per node, so 1,400,000 nodes is 18,200,000 entries, and at ~107 B resident that is 1.81 GiB
in one delta. `CompactionPolicy.MaxDeltaBytes` exists for exactly this, and it works — the
run above bounded the delta at 64 MiB, fired **9 interim compactions**, and ended with the
delta at 56.6 MiB. It bought almost nothing. Differenced at 200,000 nodes, where both arms
are cheap enough to run side by side:

| 200,000 nodes | delta unbounded | delta bounded at 32 MiB |
|---|---:|---:|
| anonymous after the rebuild | 608.9 MiB | 573.8 MiB |
| charged against the ceiling, peak | 884.6 MiB | 814.2 MiB |
| interim compactions | 0 | 2 |

> **Superseded — "it bought almost nothing" is no longer true.** This paragraph and the
> table above it measure the engine *before* a compaction adopted the index it had just
> written. With that change in force the same knob is worth **290 MiB** at this shape. The
> figures are left standing as the measurement they were; §9.6 has the pair, and the reason
> the knob was worth so little here is in the second paragraph below.

**A compaction leaves its own output resident until the store is reopened.** That is the
reason, and it is visible in one row of §9.2's table: anonymous memory is 3,838 MiB
immediately after the final compaction and **1,078 MiB after reopening the same directory**,
with the same store on disk. The 2,760 MiB difference is 18,200,000 index entries at ~107 B
plus 1,400,000 blobs at 512 B — **2,541 MiB** of entries and payload that the reopened
process reads out of the mapping and the compacting process holds in the heap, against the
2,760 MiB actually observed.

Two mechanisms produce it, and both are deliberate as far as they go.

**The image.** `disk/compact.go` states it plainly: a compaction neither creates nor retires
a mapping. The graph it publishes was built in the heap, and its untouched records still
alias the mapping Open took, so unmapping would be a use-after-unmap and mapping the new
file would add one mapping per compaction. `StorageStats.ImageMode` reports `heap` from that
point, which is the honest answer to "is this image being served out of a file". After a
*full-layer* rebuild there are no untouched records, so every blob is a heap blob.

**The index.** `PropertyIndex.AttachBase` is called only on the load path
(`disk/csr_io.go`). A compaction writes a new index section into the new image and does not
attach it, so the entries it just wrote stay in the shards as resident entries and the
attached base is the one Open established. This is why bounding the delta does not help:
each interim compaction empties the delta into a base nobody reads back.

So the arrangement §8 measures — mapped image, mapped index, a store holding 1.04× its own
file — is an arrangement a process is in **after Open and until its first compaction**. A
long-running writer leaves it at the first compaction and does not return. Nothing above
this section measured that, because every arm in this document either opens and reads or
builds and compacts in a process that then exits.

**What that means for the target.** Open and scan at the consumer's shape fit 2 GiB with
70% headroom, measured. A whole-layer rebuild in the same process did not, and the two
mechanisms above are why.

**The index half is now closed.** A compaction installs the base it has just written and
empties the shards behind it — §9.6 measures what that is worth, and it is also what makes
bounding the delta start working. Everything in §9.2's table and in the two paragraphs above
is the engine *before* that change, and is left standing as the measurement it was rather
than rewritten; the figures at 1,400,000 nodes have not been taken again since.

**The image half is not.** Re-mapping the file a compaction wrote and republishing a graph
parsed from it would move the record payloads too, and it changes what a returned
`Properties` slice aliases — today a compaction neither creates nor retires the graph's
mapping, so such a slice stays valid for the life of the handle. That is an API decision
rather than an implementation one, and it is separate from the index half deliberately.

A consumer that must rebuild 1.4M nodes on a 2 GiB machine still has **reopen after
compacting** as the route that returns everything, at 2.9 seconds and 1,078 MiB.

### 9.5 What the nightly runs

Two arms, because only one of them holds at the full shape and both facts matter. The
read-only arm runs at 1,400,000 nodes under 2 GiB; the write arm runs the whole sequence at
200,000 nodes under the same 2 GiB, where it finishes with 53.2% headroom. Neither hides the
other, and the job fails if no ceiling is in force.

That 53.2% is the nightly's own arm at its own delta bound and matches neither column of
§9.6; it has not been re-taken since a compaction began adopting its index, and it is on
the list to retake at 1,400,000 nodes rather than at 200,000.

The fixtures are built first and outside the limit — a 1.4M-node build peaks at several
times what the finished store costs to open, so a run that built its own would fail in the
builder and say nothing about the store.

```sh
# Once, unconstrained.
GRAPHENE_CEILING_BUILD=1 GRAPHENE_RSS_DIR=/var/tmp/fixture GRAPHENE_RSS_NODES=1400000 \
  go test ./tests/ -tags=stress -run '^TestCeilingFixture_Build$' -v

# Then, under the ceiling. On windows the harness applies its own and needs no wrapper.
go test ./tests/ -tags=stress -c -o /tmp/graphene.test
sudo systemd-run --scope --uid=$(id -u) --gid=$(id -g) \
  -p MemoryMax=2G -p MemorySwapMax=0 \
  env GRAPHENE_CEILING_MIB=2048 GRAPHENE_CEILING_READONLY=1 \
      GRAPHENE_RSS_DIR=/var/tmp/fixture GRAPHENE_RSS_NODES=1400000 \
  /tmp/graphene.test -test.run '^TestCeiling_' -test.v
```

`MemorySwapMax=0` matters as much as `MemoryMax`: with swap available the cgroup pushes
anonymous pages out instead of failing, and the run then measures how patient the disk is
rather than whether the store fits.

### 9.6 What closing the index half is worth, measured

The same ceiling harness at 200,000 nodes under the same 2 GiB Job Object, on one fixture,
run with the adoption in force and with it switched off in the source. A 241.4 MiB store;
5,200,000 node index entries at the compaction, 2,600,000 after it.

**Delta unbounded — one compaction at the end:**

| phase | anon before | anon after |
|---|---:|---:|
| rebuild | 609.4 MiB | 604.8 MiB |
| compact | **576.3 MiB** | **335.9 MiB** |
| reopen | 214.3 MiB | 176.3 MiB |
| charged against the ceiling, peak | 863.0 MiB (57.9% headroom) | 810.1 MiB (60.4% headroom) |

**Delta bounded at 16 MiB — four interim compactions during the rebuild:**

| phase | anon before | anon after |
|---|---:|---:|
| rebuild | 566.2 MiB | **265.1 MiB** |
| compact | 564.2 MiB | **263.7 MiB** |
| reopen | 209.1 MiB | 168.0 MiB |
| charged against the ceiling, peak | 829.9 MiB (59.5% headroom) | **520.0 MiB (74.6% headroom)** |
| process peak | 1,008.8 MiB | 799.0 MiB |

Two things in those tables and the second is the larger.

**A compaction gives back 240 MiB at this shape** — 576.3 MiB of anonymous memory after
compacting becomes 335.9, against 176.3 for the same store reopened. The remainder is the
record payloads, which §9.4 explains and this change does not touch.

**And bounding the delta starts working.** §9.4 recorded that `MaxDeltaBytes` "works and
buys almost nothing", and that is visible here in the before column: 863.0 MiB charged
unbounded against 829.9 MiB bounded, a 3.8% difference for four extra compactions. Each of
those compactions was leaving its own output resident, so bounding the delta moved entries
from the delta into a base nobody read back and the peak followed the sum rather than the
bound. With the adoption in force the same knob is worth **290 MiB**: 810.1 MiB charged
unbounded against 520.0 MiB bounded, and the process peak falls from 1,008.8 to 799.0 MiB.

That is the answer to a question §9.4 could only pose: the peak was not merely following
this bug, it was compounding it once per interim compaction.

```sh
GRAPHENE_CEILING_BUILD=1 GRAPHENE_RSS_DIR=/var/tmp/graphene-200k \
    GRAPHENE_RSS_NODES=200000 go test ./tests/ -tags=stress -run TestCeilingFixture -v
GRAPHENE_CEILING_MIB=2048 GRAPHENE_CEILING_DELTA_MIB=16 GRAPHENE_RSS_DIR=/var/tmp/graphene-200k \
    GRAPHENE_RSS_NODES=200000 go test ./tests/ -tags=stress -run TestCeiling_Consumer -v
```

### 9.7 Twenty gigabytes under two, and what the terms do when the blobs get fat

The shape all of the above was taken at keeps the store near the ceiling: 1.65 GiB on disk
against a 2 GiB limit. That leaves the interesting question unasked. **What happens when the
store is an order of magnitude larger than the limit?**

A second fixture answers it: 1,800,000 nodes at 10,240 bytes of blob each, the same schema
(8 unique, 5 ordered, 2 composite), **19,787,551,529 bytes — 18.4 GiB — of `graphene.csr`**,
23,400,000 node index entries. Read-only arm, same 2 GiB Job Object, one fresh process.

Both columns below are **windows under a Job Object**, taken minutes apart on the same
engine, so the comparison between them is clean in the way §9.3's is not.

**It fits, with three quarters of the ceiling unused.**

| | 1,400,000 × 512 B | 1,800,000 × 10 KiB |
|---|---:|---:|
| image on disk | 1,689.1 MiB | **18,870.9 MiB** |
| charged against the 2 GiB ceiling, peak | 397.8 MiB | **527.2 MiB** |
| headroom | 80.6% | **74.3%** |
| modelled heap | 237.8 MiB | 305.8 MiB |
| process peak working set | 1,139.4 MiB | 7,598.2 MiB |

The fat arm, phase by phase:

| phase | anon | file-backed | RSS | peak | wall | note |
|---|---:|---:|---:|---:|---:|---|
| open | 436.9 | 7,071.2 | 7,508.1 | 7,597.9 | 35.481s | image mapped, index mapped, adjacency built |
| scan | 437.2 | 7,071.3 | 7,508.6 | — | 0.017s | 10 rows |
| enumerate | 451.0 | 7,071.4 | 7,522.4 | 7,577.3 | 0.053s | 1,800,000 ids |

**7,598 MiB resident is not 7,598 MiB used, and the split is the whole point.** 7,071 of
those MiB are file-backed: the mapped image's pages, which the kernel is free to drop and
re-fault. The Job Object charges committed private bytes, so it never sees them. A reader
who took RSS for "memory used" here would report the page cache as if it were the program's
and conclude a 20 GB store needs 8 GB of RAM, when the process is holding 527 MiB it cannot
give back. §9.1's choice of instrument is what makes the two columns above different
numbers rather than the same number twice.

#### The terms do not care how big the blobs are

Divide each term by the node count and the two fixtures agree to two decimal places, across
a twentyfold difference in blob size:

| term | 1.4M × 512 B | 1.8M × 10 KiB |
|---|---:|---:|
| record arrays | 56.02 B/node | 56.10 B/node |
| record payloads | 2.02 | 1.98 |
| label postings | 8.01 | 7.98 |
| adjacency | 16.03 | 16.02 |
| composites | 96.02 | 96.00 |
| **modelled heap** | **178.1** | **178.1** |
| charged, peak | 297.9 | 307.1 |

**The payload row is the result.** Twenty times the blob bytes — 0.66 GiB of payload against
17.6 GiB — moves the payload term from 2.7 MiB to 3.4 MiB. It does not scale with the blobs
because the records are not in the heap: a `Properties` slice addresses the mapping, and
what the heap holds is the slice header. This is the mapped image's contract paying off at
the size it was designed for, and it is why **the ceiling binds on node count and schema,
not on bytes on disk.**

So the sizing rule this document has been circling is simply:

> **~178 bytes of modelled heap per node**, plus the schema's per-entry index cost, and a
> peak charged figure of **~300–310 B/node**. Blob size sets the file and the page cache;
> it does not set the ceiling.

**A prediction stated in advance was wrong, and this is the correction.** Before the fat arm
ran, the charged figure was modelled from two points as *58.7 MiB fixed + 244 B/node*,
predicting **477.6 MiB**. The measurement is **527.2** — light by 49.6 MiB, 10.4%. The shape
of the claim held; the coefficients did not. The error is the intercept: fitting an affine
line across two fixtures that differ in blob size put 58.7 MiB of fixed cost into a model
that has almost none, and the slope absorbed the deficit. With both arms on the current
engine there is no meaningful intercept to find — every term above is proportional to node
count — and the figure to quote is the per-node one.

#### What this settles for the release

- **Composites are 53.9% of modelled heap at both shapes** — 128.2 of 237.8 MiB and 164.8
  of 305.8 MiB, the same fraction to a tenth of a point, at 96 B/node for two declared
  composites. That is §8.4's `residentBytesPerCompositeEntry = 48` confirmed twice at scale,
  and it is the strongest argument in the document for putting the composite index on disk.
  Against the *charged* figure it is 32.2% and 31.3%.
- **Label postings are 8 B/node, 4.5% of heap and 2.6–2.7% of the charged figure.** §6.7
  called them "the larger term of the two" without measuring; they are half the adjacency
  term. Deferring them behind a mode would move a measured 2.6% of the number that matters,
  and would move it onto `NodesByType` rather than removing it. §6.7's open question is
  answered, and the answer is not to build the option.
- **The mapped property index is 985.5 MiB at 1.4M and 1,267.0 MiB at 1.8M, against 0.0 MiB
  of heap.** `IndexMapped` is doing exactly what it shipped to do, and §9.7 is the first
  place both halves of that trade appear in one line.

```sh
GRAPHENE_RSS_DIR=/path/to/fat GRAPHENE_RSS_NODES=1800000 GRAPHENE_RSS_BLOB=10240 \
  GRAPHENE_RSS_BUILD_CHUNK=100000 go test ./tests/ -tags=stress -run='^$' \
  -bench='BenchmarkRSS_Open$' -benchtime=1x -timeout=180m
GRAPHENE_CEILING_MIB=2048 GRAPHENE_CEILING_READONLY=1 GRAPHENE_RSS_DIR=/path/to/fat \
  GRAPHENE_RSS_NODES=1800000 GRAPHENE_RSS_BLOB=10240 \
  go test ./tests/ -tags=stress -count=1 -run '^TestCeiling_Consumer' -v
```

### 9.8 The write path fits, and the knob was not what fixed it

§9.4 left the release's end-to-end acceptance failed and unretested: a whole-layer rebuild
at 1,400,000 nodes held 3,853 MiB and peaked at 6,644, against a 2,048 MiB ceiling. §9.6
then closed the index half of it — `SwapBase` installs the base a compaction wrote — and
measured that at 200,000 nodes, where bounding the delta went from worth 3.8% to worth 36%.
The open question was whether `MaxDeltaBytes` closes the rest at the full shape.

It does not. This section is the sweep that establishes it, and the change that does.

**The read arm first, because it is the half that already held.** Under the same 2 GiB Job
Object, opening the 1,689.1 MiB image and scanning it charges **396.2 MiB of 2,048 — 80.7%
headroom** — and settles at 237.8 MiB of modelled heap: records 74.8, composites 128.2,
adjacency 21.4, label postings 10.7, payload 2.7. The payload row is the point of the whole
programme: 1,400,000 records of blob, and the heap holds 2.7 MiB of it, because a
`Properties` slice addresses the mapping.

#### The delta sweep: eight bounds, one shape, no pass

Each arm is a fresh process under the same 2,048 MiB limit, running the consumer sequence —
open, scan, enumerate, then delete all 1,400,000 nodes and write 1,400,000 back — with
`CompactionPolicy.MaxDeltaBytes` as the only variable.

| `MaxDeltaBytes` | interim compactions | records written before it died | anonymous at the last sample |
|---|---:|---:|---:|
| unbounded | 0 | 400,000 | 1,916.2 MiB |
| 512 MiB | 0 | 400,000 | 1,924.9 MiB |
| 256 MiB | 1 | 400,000 | 1,888.4 MiB |
| 128 MiB | 3 | 750,000 | 1,855.8 MiB |
| 64 MiB | 5 | 800,000 | 1,959.3 MiB |
| **32 MiB** | 14 | **1,350,000** | 2,041.5 MiB |
| **16 MiB** | 29 | **1,350,000** | 2,042.0 MiB |
| 8 MiB | 29 | 1,250,000 | 1,980.2 MiB |

Every arm died of `ERROR_COMMIT_LIMIT` inside the *rebuild* phase — not at the final
compaction, and not in a sweep afterwards. The curve has a direction and an optimum: the
tightest useful bound reaches 96.4% of the rebuild, and below 16 MiB it turns back down.
**No value passes.**

**Why a bound helps at all, and why it then stops helping.** There are two growing terms, and
a compaction moves cost from one to the other rather than removing it.

*Uncompacted*, the term that runs away is the resident property index over the new writes.
At 400,000 records the unbounded arm holds **546.4 MiB of index against 295.0 MiB of delta**
— the index is 1.85× the delta, and no `CompactionPolicy` rule counts it. A rebuild writes
thirteen index entries a node, so this is the 18,200,000-entry figure §9.4 predicted,
arriving four times sooner than the delta it is measured beside. The 512 MiB and 256 MiB
bounds are simply above what the process can reach, so they behave as unbounded.

*Compacted*, that term is folded into the image and the arm gets three times as far — and
the record payloads stay in the heap. The 32 MiB arm's payload term climbs **49.0 MiB per
100,000 records written and never resets**, reaching 637.2 MiB at the point it died; the
16 MiB arm reached 661.8. Those are bytes the process has already written to the image and
is still carrying. At the optimum, payload and records together are **83% of the modelled
heap**.

That is §9.4's post-compaction characteristic, at the shape that matters: a compaction
publishes a graph it built in the heap, and `AttachBase` runs on the load path and nowhere
else. Compacting more often does not shrink it, because it is proportional to records
written rather than to compactions. It is outside the knob.

#### What fixes it is an open, and the cost is one

Reopening after each interim compaction is the same sequence with `GRAPHENE_CEILING_REOPEN=1`:

| phase | anonMiB | fileMiB | rssMiB | peakMiB | wall | note |
|---|---:|---:|---:|---:|---:|---|
| open | 316.6 | 703.4 | 1020.0 | 1116.6 | 2.003s | image mapped, index mapped, adjacency built |
| scan | 316.6 | 703.9 | 1020.5 | — | 0s | 10 rows |
| enumerate | 327.4 | 704.0 | 1031.4 | 1043.2 | 39ms | 1,400,000 ids |
| rebuild | 344.7 | 703.8 | 1048.5 | 2147.3 | 3m32.023s | 1,400,000 deleted, 1,400,000 written, 15 interim compactions and 15 reopens |
| compact | 337.5 | 1226.6 | 1564.0 | 1731.2 | 13.926s | image heap, index mapped, adjacency built |
| reopen | 332.5 | 704.1 | 1036.6 | 1517.8 | 2.44s | 1,400,000 nodes, image mapped, index mapped, adjacency built |

**1,285.2 MiB of the 2,048 MiB ceiling, 762.8 MiB of headroom — 37.2%.** The acceptance §9.4
records as failed by 3.2× passes, and the peak falls from 6,644 MiB to 1,285.2.

The mechanism is visible term by term. Matched against the 32 MiB arm that did not reopen,
at the same point in the same rebuild:

| at 200,000 written, 32 MiB bound | compacting in place | reopening after each |
|---|---:|---:|
| payload, heap | 98.0 MiB | **0.4 MiB** |
| property index, heap | 0.0 MiB | 0.0 MiB |
| property index, mapped | 0.0 MiB | **140.9 MiB** |
| modelled heap | 132.0 MiB | **34.3 MiB** |
| anonymous | 872.6 MiB | **502.4 MiB** |

A reopen restores *both* halves: the records address the image again, and the property index
goes back to being read in place rather than rebuilt in the shards. The arm then holds flat
rather than ratcheting — 502.4 MiB at 200,000 written, 419.6 at 450,000, 687.8 at 1,050,000
— and settles at **238.0 MiB of modelled heap, against the read-only arm's 237.8**. Having
rebuilt every record in the store, the process is holding what a fresh open holds.

**It is also faster, which was not the argument for it.** The reopen arm completed the whole
sequence in 242s. The 16 MiB arm spent 469s and did not finish, and the 8 MiB arm 363s and
did not finish: 29 compactions against a heap that never stops growing cost more than 15
against one that does not. A reopen is an open — the image mapped, the header and directory
parsed, 2.44s on this store — so it is a thing to do after a compaction and not after a
commit, and 15 of them are 5% of this rebuild's wall clock.

#### The bound and the reopen are one setting, not two

Reopening is not a substitute for bounding the delta. The same arm at a 128 MiB bound
reopens five times instead of fifteen, and **fails at 1,200,000 of 1,400,000** holding
1,843.0 MiB. It is a large improvement on the 750,000 that 128 MiB reaches without
reopening, and it is still a failure.

| 1,400,000-node rebuild | without reopen | with a reopen after each compaction |
|---|---:|---:|
| `MaxDeltaBytes` 128 MiB | 750,000 records | 1,200,000 records |
| `MaxDeltaBytes` 32 MiB | 1,350,000 records | **all 1,400,000, 37.2% headroom** |

The reason is that a reopen sheds what has accumulated *since the last one*, and between two
reopens a 128 MiB bound lets the delta and the index over it grow four times as far. The
configuration that fits is both: **bound the delta at 32 MiB and reopen after each of the
fifteen compactions that produces.** Neither half alone reaches the end of this rebuild.

#### Where it stops fitting is between 96 and 128, and the margin goes before the pass does

The table above measures the two ends and leaves the middle open. Two further arms — the same
fixture, the same 2,048 MiB Job Object, a reopen after each interim compaction, one fresh
process each — close it.

| `MaxDeltaBytes` | interim compactions | charged against the ceiling | headroom | rebuild | whole sequence | result |
|---|---:|---:|---:|---:|---:|---|
| **32 MiB** | 15 | **1,285.2 MiB** | **37.2%** | 3m32.0s | 242s | pass |
| 64 MiB | 9 | 1,368.5 MiB | 33.2% | 4m01.1s | 273s | pass |
| 96 MiB | 7 | 1,697.3 MiB | 17.1% | 4m05.9s | 288s | pass |
| 128 MiB | 5 | — | — | — | — | **fails at 1,200,000, holding 1,843.0 MiB** |

**The pass/fail boundary is in (96, 128].** That is the answer to the question as asked, and it
is the less useful half of what the arms show.

**The useful half is the shape of the margin.** Headroom is flat from 32 to 64 — 37.2% to 33.2%,
four points for a doubling — and then falls off a cliff: 33.2% to 17.1% over the next 50%, and
to nothing over the 33% after that. Two thirds of the loss happens in the last doubling. A
configuration at 96 MiB passes this rebuild and has 350.7 MiB to absorb everything the fixture
does not contain — a larger record, a fourteenth index entry a node, another process on the
machine. A configuration at 32 MiB has 762.8 MiB. These are not the same setting with different
compaction counts; they are a setting with margin and a setting without one.

**Nothing on the curve argues for loosening the bound.** The three figures move together and all
three move the wrong way: as the bound rises the peak rises (1,285 → 1,369 → 1,697), the
compaction count falls as designed (15 → 9 → 7), and the wall clock *rises* (242 → 273 → 288s).
The saving that fewer compactions is supposed to buy does not appear. §9.8's earlier arms found
the same thing in the other direction — 29 compactions cost more than 15 — so the wall clock has
an optimum near 32 rather than falling monotonically with compaction count, and the tightest
useful bound sits at the optimum of both curves at once. **`GRAPHENE_CEILING_DELTA_MIB=32`
remains the documented configuration, now because three looser bounds were measured and none of
them is better at anything.**

**The bound moves the peak and nothing else.** All three passing arms settle at **238.0 MiB of
modelled heap, term for term identical** — records 75.0, payload 2.7, labels 10.7, adjacency
21.4, index 0.0, composite 128.2 — against the read-only arm's 237.8. This is worth stating
plainly because it is what makes the knob easy to reason about: `MaxDeltaBytes` is a
transient-sizing knob, it does not change what the process holds at rest, and a caller choosing a
value is choosing how much headroom to spend during a rebuild and buying nothing at either end of
it.

**Why the cliff is where it is.** The two terms a reopen sheds are the delta and the property
index over it, and the second runs at **1.85× the first** (measured above at the unbounded arm:
546.4 MiB of index against 295.0 MiB of delta). So the transient a bound admits between two
reopens is not the bound — it is the bound plus an index roughly twice its size, plus the
compaction that folds them in, against a ceiling the mapped image already takes 703 MiB of. A
bound of 96 admits a working set that clears the ceiling by 350 MiB; a bound of 128 does not
clear it at all. There is no room on this shape for a fourth doubling, which is why the curve has
a cliff rather than a slope.

**Both arms also read the composite term at exactly 128.2 MiB**, independently of §8.6 and §9.7
and of each other, which is now four measurements of that figure at this shape from three
different harness paths.

#### What GCPX buys, measured on the arm that was already passing

§8.6 moves the composite postings into the image. The same arm, the same fixture, the
same 2,048 MiB Job Object, the same 32 MiB bound and reopen — the only difference is
that the build writes and reads GCPX.

| reopen-32, 1,400,000 nodes | without GCPX | with GCPX | change |
|---|---:|---:|---|
| charged against the ceiling | 1,285.2 MiB | **880.7 MiB** | **−404.5 MiB, −31.5%** |
| headroom | 37.2% | **57.0%** | **+19.8 points** |
| settled modelled heap | 238.0 MiB | **109.9 MiB** | −128.1 MiB |
|  of which composites | 128.2 MiB | **0.0 MiB** | the whole term |
| process peak | 2,147.3 MiB | 1,872.6 MiB | −274.7 MiB |
| image, mapped | 1,689.1 MiB | 1,710.5 MiB | **+21.4 MiB** |
| rebuild wall | 3m32.02s | 3m31.85s | unchanged |
| reopen wall | 2.44s | **0.828s** | **2.9× faster** |
| whole sequence | 242s | 242s | unchanged |

**The settled heap falls by 128.1 MiB and the composite term falls by 128.2.** Those are
the same number to a tenth, which is the result this section exists to state: nothing else
moved. Records, payload, label postings and adjacency report 75.0, 2.7, 10.7 and 21.4 in
both arms, digit for digit.

**It costs 21.4 MiB of image for 128.1 MiB of heap — a ratio of 6.0×.** The section is
the postings and nothing else; what it replaces is the postings *plus* the per-entity rows
of member values a resident composite needs in order to file a tuple, and those rows were
85% of what a composite index held (§8.4). The image pays for the half that is data; the
heap was paying for both halves.

**The reopen phase is 2.9× faster, which was not the argument for it.** An open over an
image carrying the section does not walk the member keys' entries to fill the composites,
so what was 2.44s of fill is 0.828s of parse. The rebuild's wall clock is unchanged —
3m31.85s against 3m32.02s — because the fifteen reopens it performs were already 5% of it.

**The margin is what a caller gets.** 37.2% headroom was a pass with room for the fixture
and not much else; 57.0% absorbs a larger record, a fourteenth index entry a node, or
another process on the machine. Read against the bound sweep above, GCPX moves this
configuration from the edge of the cliff back behind the flat part of the curve: 880.7 MiB
is below what even a 32 MiB bound charged before, and below the 1,368.5 that 64 MiB charged.

**The old image still reads exactly as it did, and that was measured on the same run.** A
read-only arm over the *pre-GCPX* fixture — a v9 image this build did not write — reports
the composite term at 128.2 MiB and the total at 237.8, unchanged to the tenth, at 80.0%
headroom. That is the compatibility claim at the full shape rather than at a fixture's: an
image without the section is filled from the entries exactly as before, and nothing about
the new build's behaviour on it has moved.



#### The decision

`Graph.CompactAndReopen` ships. It is §9.4's route (b): compact, close, return a fresh handle
on the same directory with the same options.

**Route (a) — re-mapping the compaction's output in place — is not shipped, and the sweep
does not change that.** It is the contract change rather than a side effect of one: old
records would address mapping N and new ones N+1, and retirement becomes reachability-based.
`disk/mapping.go`'s header derives rule A rather than asserting it, and route (b) is rule A
verbatim — the predecessor is closed, so nothing is retired out from under a live graph.

**`DefaultCompactionPolicy` is unchanged.** The sweep's honest conclusion is that no
`MaxDeltaBytes` completes this rebuild on its own, so there is no value on this curve to
promote as a default; 128 MiB remains reasonable for the workloads it was chosen for, and
moving it would change the compaction schedule of every deployment tuned against it for no
measured benefit. A consumer on a bounded machine needs the pair, and the pair is documented
at `CompactAndReopen` rather than buried in a default.

#### What this does not establish

The arms above are windows under a Job Object, which limits commit charge. The linux arm
under a cgroup has not been rerun at this shape since §9.6. ~~Only two bounds have been
measured with a reopen, so where between 32 and 128 MiB the configuration stops fitting is
not known.~~ **Superseded: 64 and 96 MiB were measured afterwards and both pass, placing the
boundary in (96, 128] — see "Where it stops fitting" above. What remains unmeasured is
narrower: no bound between 96 and 128 has been run, so the boundary is located to a 32 MiB
interval and not to a value.** And the figure the process actually charges is roughly **2.6× the modelled heap**
during a rebuild — 559.8 MiB modelled against 1,454.8 anonymous at one sample — where at
rest the model is within a few percent. `EstimateResident` models retained heap; it does not
model the allocator's working set, the collector's headroom, or a compaction's transients,
and a caller sizing a rebuild against it is reading a floor rather than a budget.

### 9.9 A bulk ingest holds its memory, and pays for it in writes

§9.8 measured a *rebuild*: a store that already exists, whose derived layer is deleted and
written back. That is the consumer's sequence, and it is not the question this programme
opened with, which was "what if I bulk-ingest ten million nodes?". A rebuild starts from an
image, so the delta it accumulates is measured against something already on disk. An ingest
starts from nothing, and every structure it holds is one it created.

`TestCeiling_BulkIngestFitsUnderTheLimit` is that arm. It builds an empty store, declares the
same 8 unique + 5 ordered + 2 composite schema, and fills it under a real ceiling with the
configuration §9.8 established: `MaxDeltaBytes` at 32 MiB with a reopen after every interim
compaction. It also counts what the engine wrote, by summing `MetricCompaction.Bytes` and
`MetricCommit.Bytes` — which turns the rewrite cost from arithmetic in a plan into a
measurement.

Both arms were run: bounded with a reopen, and the unbounded arm that `USER_GUIDE.md` §10
recommended until this release — ingest everything, compact once at the end.

#### The unbounded arm dies of the data, and the crossing is visible

| records | anon, ingesting | peak RSS | charged | headroom | bytes written | amplification | wall |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 510.3 MiB | 956.7 | 996.6 | 51.3% | 0.67 GiB | 1.81× | 14.0 s |
| 200,000 | **1,010.0 MiB** | 1,801.6 | 1,843.0 | **10.0%** | 1.34 GiB | 1.81× | 23.5 s |

Anonymous memory is **exactly linear in the record count** — 510.3 doubles to 1,010.0 for
twice the data — and the charged figure with it. At 200,000 records of 3.2 KB, a store of
757 MiB on disk, the process has 10% of a 2 GiB ceiling left. It does not reach 400,000.

The figure it stops at, 1,843.0 MiB, is the same one §9.8 recorded for the 128 MiB rebuild arm
at 1,200,000 records. That is not a coincidence about the workload; it is where a 2 GiB
machine gives out, reached from two different directions.

Note what the amplification column does: **1.81× at both sizes, flat**. The unbounded arm
writes the image exactly once (1.00×) plus the log (0.81×). It is the cheapest possible ingest
in write volume and the most expensive in memory, and the bounded arm below is the exact
converse. There is no configuration on this axis that is good at both.

#### The bounded arm holds flat

Three sizes, at the 3.2 KB records this programme targets, each a fresh process under a
2,048 MiB Job Object, `MaxDeltaBytes` at 32 MiB with a reopen after every interim compaction:

| records | store on disk | anon, ingesting | file-backed, ingesting | peak anon | peak RSS | charged | headroom | wall |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 378.7 MiB | **94.6** | 251.6 | 242.7 | 477.8 | 244.0 | 88.1% | 24.7 s |
| 200,000 | 757.3 MiB | **76.3** | 572.9 | 270.6 | 825.8 | 271.1 | 86.8% | 71.0 s |
| 400,000 | 1,514.3 MiB | **98.2** | 1,182.0 | 336.1 | 1,615.6 | 336.1 | 83.6% | 214.4 s |

**The anonymous column is the result.** It is flat across a fourfold size range while the
store on disk quadruples — against the unbounded arm's exact linearity in the same units, at
the same shape, on the same machine. The bound plus the reopen do exactly what §9.8 said they would, and
they do it on the ingest path too — which was not established, because §9.8's arms all had an
image to start from. The charged figure grows, but slowly and sublinearly: 244 → 271 → 336 MiB
for 4× the data, and the headroom is still 83.6% at the largest arm.

**The file-backed column is the one Phase 2 is about.** It tracks the store at about 0.78×
and is **12× the anonymous class** by the third arm. On windows that costs nothing against the
ceiling — a Job Object charges committed private bytes, so the charged column and the anon
column agree to a tenth of a megabyte at every size. Under a linux cgroup, `memory.max`
charges page cache. **So the same ingest that has 83.6% headroom here would be charged
1,518 MiB rather than 336 under a v2 cgroup**, and the gate on Phase 2 is met: the resident
class during an ingest is large, it grows with the data, and only one of the two platforms
gets it for free.

#### The quadratic, measured

| records | bytes written | amplification | B/node | compactions | mean image per compaction |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 2.42 GiB | 6.53× | 25,942 | 10 | 216.7 MiB |
| 200,000 | 8.71 GiB | 11.77× | 46,747 | 20 | 415.1 MiB |
| 400,000 | 31.40 GiB | 21.24× | 84,301 | 39 | 793.0 MiB |

Amplification grows by **1.80× per doubling of the record count, twice**, and bytes per node by
the same factor. Exact quadratic growth would be 2.00×; the shortfall is the early compactions
writing a small image, which the closed form `(N·b)² / (2·chunk)` treats as asymptotic. The log
term is a constant 0.81× of the store at all three sizes and is not the problem — the image
is, at 5.72×, 10.96× and 20.42×.

Extrapolating the measured fit rather than the plan's arithmetic: ten million records at this
shape write on the order of a **terabyte** to store thirty gigabytes.

#### What the bound costs in wall clock

| records | unbounded | bounded + reopen | ratio |
|---:|---:|---:|---:|
| 100,000 | 14.0 s | 24.7 s | 1.8× |
| 200,000 | 23.5 s | 71.0 s | 3.0× |

Worth stating plainly rather than leaving in the phase tables: bounding the delta is **slower**
on an ingest, and increasingly so, because the wall clock follows the bytes written. §9.8 found
the opposite on a *rebuild* — the bounded arm finished in 242 s where an unbounded one never
finished at all — and both are true. A rebuild that does not bound dies; an ingest that does
not bound is faster right up until it dies.

**This settles the Phase 5 decision.** The memory question is answered — an ingest of any size
holds flat anonymous memory under a bound and a reopen. The write question is not, and no
value of `MaxDeltaBytes` answers it, because the two arms above bracket the whole axis:
unbounded is 1.81× writes and linear memory, bounded is flat memory and 1.80× writes per
doubling, and tightening the bound moves along that line rather than off it. A one-pass bulk
load writes the data once at flat memory; that is the only thing that changes the shape of the
curve rather than the position on it.

#### What this does not establish

The three arms are windows under a Job Object. **The anon/file split was not measurable at
all below this record size**: windows estimates the split as `min(PrivateUsage, WorkingSet)`
and the Go runtime's over-commit is large enough that a small process reports the whole
working set as anonymous and zero file-backed. `rssSample.AnonClamped` now reports that state
as unmeasured rather than as zero — the 256-byte arms print `n/a` in the file column, and the
3.2 KB arms are the first where the working set outgrows the runtime's reserve. A linux arm
would measure the split exactly, and has not been run at this shape.

The largest arm is 400,000 records. 1M and 2M are what the plan asks for, and the cost of
taking them is the table above: about 805 GiB written between the two.

```sh
GRAPHENE_CEILING_MIB=2048 GRAPHENE_CEILING_INGEST_DIR=/path/to/empty \
  GRAPHENE_RSS_NODES=400000 GRAPHENE_RSS_BLOB=3200 \
  GRAPHENE_CEILING_DELTA_MIB=32 GRAPHENE_CEILING_REOPEN=1 \
  go test ./tests/ -tags=stress -count=1 -run TestCeiling_BulkIngest -v -timeout=180m
```

## Reproducing these figures

Every number above comes from `tests/rss_bench_test.go` behind the `stress` tag. The
fixture knobs:

| variable | effect |
|---|---|
| `GRAPHENE_RSS_NODES` | node count (default 50,000; the figures in this document name the count they were taken at) |
| `GRAPHENE_RSS_BLOB` | blob bytes per node (default 512) |
| `GRAPHENE_RSS_DIR` | build the fixture **once** into this directory and reuse it; a shape marker makes a later run with different settings fail rather than silently measure the wrong store |
| `GRAPHENE_RSS_EDGE_STRIDE` | write one edge per N nodes (0 = no edges) |
| `GRAPHENE_RSS_BLOB_DIST` | the long-tailed distribution of §3.2 instead of a fixed size |
| `GRAPHENE_RSS_NOINDEX` | declare and populate no index at all — the differencing arm of §3 |
| `GRAPHENE_RSS_CYCLES` | rebuild cycles for `BenchmarkRSS_RebuildCycle` (§5) |
| `GRAPHENE_RSS_MODES` | the configuration `BenchmarkRSS_ModeMatrix` opens under (§8): `<image>/<index>/<adjacency>[/live]`, one per process |
| `GRAPHENE_CEILING_MIB` | the ceiling `TestCeiling_*` claims to run under (§9). The run refuses unless the kernel reports one at least this tight |
| `GRAPHENE_CEILING_BUILD` | build the ceiling fixture and measure nothing — the step that must run *outside* the limit |
| `GRAPHENE_CEILING_READONLY` | stop the sequence after the scan: the consumer's aggregate process, and the arm that fits at the full shape |
| `GRAPHENE_CEILING_DELTA_MIB` | bound the delta during the rebuild via `CompactionPolicy.MaxDeltaBytes`; unset rebuilds into one delta |
| `GRAPHENE_CEILING_REOPEN` | reopen the store after every interim compaction — route (b) of §9.8, and meaningless without a delta bound to produce interim compactions |
| `GRAPHENE_CEILING_INGEST_DIR` | where `TestCeiling_BulkIngest*` builds its store (§9.9). Required above 200,000 nodes, must be empty, and deliberately **not** `GRAPHENE_RSS_DIR`: that names a reused fixture guarded by a shape marker, and an ingest arm writing into it would leave the next run a store the marker still called pristine |
| `GRAPHENE_CEILING_BUDGET_MIB` | `Options.MemoryBudget` for the ingest arm. With a delta bound set too, this is the configuration where the two can contradict each other — the policy says compact, the budget refuses |
| `GRAPHENE_CEILING_DISCOVER` | `Options.DiscoverMemoryBudget`: the engine reads the ceiling the harness just applied and derives its own budget from it. The run fails if a budget was asked for and none is in force |

**Use `GRAPHENE_RSS_DIR` for anything that reports a peak.** Building a 1.4M-node
fixture peaks near 9.4 GiB and `peakMiB` is a process-lifetime high-water mark, so a
process that builds and then measures reports the build's peak under the name of the
measurement's — and `debug.FreeOSMemory` does not fully undo a build, so its settled
anonymous figure is a few per cent high as well. Build the master once, then measure in
fresh processes:

```sh
# Build the master (slow, ~9.4 GiB peak, once).
GRAPHENE_RSS_DIR=/path/to/master GRAPHENE_RSS_NODES=1400000 \
  go test ./tests/ -tags=stress -run='^$' -bench='BenchmarkRSS_Open$' -benchtime=1x

# Measure against it, three fresh processes.
GRAPHENE_RSS_DIR=/path/to/master GRAPHENE_RSS_NODES=1400000 \
  go test ./tests/ -tags=stress -run='^$' -bench='BenchmarkRSS_Open$' \
  -benchtime=1x -count=3 -timeout=60m
```

Two rules the instrument cannot enforce for you.

**Never measure residency under `-race`.** The race detector's own shadow memory is
several times the heap.

**`fileMiB` of 0 is not a mapping failure** when the benchmark built its store in the
same process. A store built in-process holds the `CSRGraph` that `Build` produced and
never deserialises an image, so R2's mapping appears only in arms that reopen from disk.
The rebuild-cycle and compaction arms are therefore all-anonymous by construction, which
makes them the harshest arms for a budget and the right ones for their questions.

`BenchmarkRSS_Scan` resolves rows through `NodesByProperty` and asserts it found them,
so it cannot run against a `GRAPHENE_RSS_NOINDEX` fixture.

The structural figures — §1.2's per-key-group differencing, §1.4's map sweep, and the
`unsafe.Sizeof` table — come from three probes in the index package rather than from the
benchmark suite, because they build a bare `PropertyIndex` with no store around it:

| file | what it establishes |
|---|---|
| `index/zz_scratch_sizeof_test.go` | `unsafe.Sizeof` of every index type; `heapBytes()` (two GCs, then `ReadMemStats().HeapAlloc`); per-map cost fills |
| `index/zz_scratch_sweep_test.go` | the size sweep that found the sawtooth, including the presized arm |
| `index/zz_scratch_model_test.go` | the fixture's declared shape built one key group at a time, and the key-to-shard occupancy |

They are untagged and take about 12 s together: `go test ./index/ -run TestScratch -v`.
