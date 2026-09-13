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
memory. That is B6's case measured, and the argument for `Options.MaxReplayRecords`
refusing an open by naming the figures rather than discovering them inside it.

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
row in §6.3.

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

**Verifying the mapped index costs nothing resident.** Worth stating here because it
is the one operation that touches every entry in the index on purpose, and therefore
the obvious place for the residency win to be handed straight back. It is not:
`VerifyIndexes` holds one counter per declared key plus a buffer sized by the widest
value, measured at 416 bytes and thirteen allocations whether the index holds 25,000
entries or 100,000. The time is ~76 ns per entry. That was designed for rather than
observed — the store most likely to be verified is the one nearest its ceiling — and
`docs/TECHNICAL_DETAILS.md` §14.20 has the argument that makes an O(1)-memory check
of both directions exhaustive rather than partial.

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

## Reproducing these figures

Every number above comes from `tests/rss_bench_test.go` behind the `stress` tag. The
fixture knobs:

| variable | effect |
|---|---|
| `GRAPHENE_RSS_NODES` | node count (default 200,000) |
| `GRAPHENE_RSS_BLOB` | blob bytes per node (default 512) |
| `GRAPHENE_RSS_DIR` | build the fixture **once** into this directory and reuse it; a shape marker makes a later run with different settings fail rather than silently measure the wrong store |
| `GRAPHENE_RSS_EDGE_STRIDE` | write one edge per N nodes (0 = no edges) |
| `GRAPHENE_RSS_BLOB_DIST` | the long-tailed distribution of §3.2 instead of a fixed size |
| `GRAPHENE_RSS_NOINDEX` | declare and populate no index at all — the differencing arm of §3 |
| `GRAPHENE_RSS_CYCLES` | rebuild cycles for `BenchmarkRSS_RebuildCycle` (§5) |

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
