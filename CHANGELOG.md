# Changelog

Release notes start here. Tags v0.1 through v0.4.0 predate this file; use
`git log` for those.

## Unreleased — v0.7.0

### The CSR stores records in pages, so a burned identifier costs four bytes

The two measurements above — 79 B of resident memory per identifier issued and
never reusable, and a low-half deletion costing 152 B per identifier more than a
high-half one — describe the same defect. The CSR indexed one record slot per
identifier, so a store that rebuilds a derived layer paid 56 B of `nodeRecord`
and 16 B of offsets for every identifier it had ever issued, forever, and
compaction recovered none of it: compaction recovers the *top* of the identifier
space, and a rebuild burns the bottom.

Records now live in pages of 4096 identifiers. A per-kind directory of `int32`
maps a page number to an arena page or to −1, the arenas hold one slot per
identifier of the pages that are actually occupied, and adjacency is one CSR over
that slot space with a single sentinel. A page nothing occupies costs four bytes;
a page whose records have all been deleted and compacted away goes back to
costing four bytes.

Measured against the tree before it, three interleaved rounds:

- **17.65 B per burned identifier**, from 84.45 — **−79.1%**, with the tail slope
  falling from 1.611 to 0.337 MiB per cycle.
- **The two deletion shapes now cost the same**: 314.6 B/node for the low half
  against 314.2 for the high half, where they were 531.2 and 378.8. Where the
  surviving identifiers sit no longer changes what a store costs.
- Resident after twelve rebuild cycles **130.9 → 112.9 MiB**, Go heap
  **71.15 → 55.01 MiB**, peak **296.8 → 251.3 MiB**. A store that has burned
  nothing is unchanged, which is the control.

**The on-disk format did not move, and that is tested rather than asserted.**
Arena pages are assigned in ascending page number, so arena order is identifier
order, so the walk that writes the record stream and hashes the Merkle leaves is
the walk it always was. `disk/testdata/csr_v8_before_pages.bin` was written by
the build *preceding* this change; the test suite reproduces it byte for byte,
reparses it, recomputes its roots from the paged layout and checks an inclusion
proof against the root the file carries. The determinism tests already here could
not have caught a consistent reordering — both compare a build against itself.

Four behaviours changed deliberately:

- `Build` refuses a duplicate identifier instead of keeping the last silently, so
  a file whose records collide fails to open rather than opening with a header
  count that disagrees with its own contents.
- The addressable identifier space rose from **2²⁶ to 2³²**. The ceiling now
  bounds the page directory — 4 MiB per kind at the top of the space — rather
  than the record arrays, so the old value was costing addressable lifetime for a
  bound that no longer binds. An image naming identifiers above 2²⁶ cannot be
  read by a v0.7.0 build.
- The sparsity rule counts pages rather than the highest identifier:
  `touchedPages × 4096 ≤ max(65536, records × 256)`, with an edge's endpoint
  pages counted. It is identically tight against a hostile file and strictly more
  permissive for a real one — one record naming an identifier just under the
  ceiling now loads, because it now costs one page.
- An edge naming an endpoint no record occupies materialises that endpoint's page
  instead of panicking. No file that loaded before stops loading.

The cost is a dependent load on every lookup and the quantisation: a page keeps
its 4096 slots while one record in it survives. The lookup cost is not measurable
above this host's noise — the memory backend, untouched by this change, moved
10% in the same rounds while the disk arm moved 1.6% — and `store info` reports
the quantisation when it is worth an operator's attention.

`TestRebuildCycle_ResidentIsProportionalToLive` runs in `make check` and fails on
the tree before this change, at 72.0 B per burned identifier against a ceiling of
24.

### A store can now see the end of its own identifier space

Identifiers are never reused, compaction preserves them, and the ceiling above is
on the store's lifetime rather than on its size. Nothing reported that.

`StorageStats` gains `HighestNodeID`, `HighestEdgeID`, `IDCeiling`,
`NodeIDHeadroom` and `EdgeIDHeadroom`. The headroom is computed from identifiers
**issued**, not from the ones the image still holds: deleting the record that
held the highest identifier does not give headroom back, because the next write
will not reuse it, and a figure that said otherwise would be worse than none.

A completed compaction reports it unprompted when the space remaining falls below
`Options.IDHeadroomWarn` (default 0.10; negative disables): a
`MetricIDHeadroomLow` metric and an `AuditIDHeadroomLow` entry naming the kind and
the figures, once per kind per process so that a store compacting on a timer does
not fill its own chain with one observation. `graphene store stats` and
`graphene debug stats` print the identifiers issued against the ceiling, and
`graphene store health` raises `store.id_headroom_low` as a warning.

There is no refusal, deliberately. A store at 5% headroom works exactly as it did
at 95%, and the remedy — an export and import into a fresh store, which is a new
store with new identifiers rather than a renumbering of this one — is a decision
with downtime in it. The engine's job here is to make sure the decision is not
taken by surprise.

### Measuring the thing the budget is written against

A consumer sized for a 2 GB machine measured 9,290 MiB of peak RSS rebuilding a
derived layer against a 1.2 GiB store, and 4,871 MiB for a read that touches ten
rows. Nothing in this engine could have predicted either number, because nothing
in it measured resident memory at all: the footprint suite reports
`runtime.MemStats.HeapAlloc`, the Go heap a graph retains.

That is the wrong instrument for a RAM ceiling in a way that would have quietly
corrupted the work that follows. It cannot see a memory-mapped image — those
bytes are resident and are not Go heap — so the single change most likely to fix
this would have reported as a large win on every existing number while the
process occupied exactly as much RAM as before.

So this release opens with instruments and no engine changes at all. Nothing in
`disk/`, `index/` or `store/` moved.

**Process residency, split by class.** `tests/rss_test.go` reads the operating
system's own accounting and keeps **anonymous** pages — private, swap-backed,
what a ceiling constrains — apart from **file-backed** pages, which the kernel
can evict without swap. Linux publishes both directly; Windows derives them from
`WorkingSetSize` and `PrivateUsage`, one-sidedly and in the safe direction;
darwin reports the peak only and says so rather than inventing a split. Standard
library only, so the zero-cgo, zero-dependency closure is untouched.

The reader is calibrated before anything is believed of it. One test maps and
touches 128 MiB and fails if the reading does not land in the file class, because
a reader blind to mapped pages would veto the mapped-image design on its own
blind spot. Another skips itself under `-race` after finding that the detector's
shadow memory is itself anonymous, which makes the split unmeasurable under it —
worth knowing before someone reads a memory figure off a race-enabled run.

**Five baselines** (`make rssbench`) on a fixture matching the reported index
shape: eight unique keys, one an all-distinct 32-byte digest, five ordered, two
composites. Open, a ten-row read, a bulk write, a compaction, and a rebuild
cycle. Plus `make cpuprofile` and `make heapprofile`, and a `-memprofile` flag on
the CLI beside the existing `-cpuprofile` — the heap profile is written after the
command finishes but before the store closes, so it reports what was still held
rather than everything ever allocated.

**A regression guard that can fail.** `TestFootprintGuard_OpenSlope` asserts a
*slope* — retained bytes per record, fitted across two fixture sizes — rather
than a ceiling on the total. Fixed overhead cancels, so a change that adds a
constant is free and a change that adds bytes per record is caught. A ceiling
loose enough to be stable would have been loose enough to miss a doubling.

### What the baselines say

Four readings, on a 44 MiB store, that reorder the work ahead:

- **Resident is ~4.0× the store on disk** — against the 4.06× the consumer
  reported. The fixture reproduces the shape that was reported.
- **A ten-row read costs the same as opening**, to within 0.3 MiB. The entire
  figure is paid at `Open`; effort aimed at making reads cheaper is aimed at the
  wrong term.
- **Nothing is file-backed.** Every resident byte today is anonymous memory the
  kernel cannot evict.
- **Compaction's transient peak is +270 MiB over steady state** — 6.1× the store
  size, held for the duration.

### Deleting the low half is not the same as deleting the high half

`docs/benchmarks.md` has reported since v0.6.0 that a half-deleted store costs
4.5× until `Compact`. That measurement deleted the **high** half, where the
maximum identifier falls with the deletion and the dense arrays shrink with it.

A derived-layer rebuild does the opposite: it deletes the whole old layer — the
**low** identifiers — and writes the replacement above them, so the maximum only
ever rises. Two new benchmarks measure both arms with the same live count and the
same live bytes, and they differ by **152 B per burned identifier**, which
decomposes exactly as 72 B for the node slot plus 80 B for the edge slot the
delete cascade burned with it. Compaction recovers none of it.

`BenchmarkRSS_RebuildCycle` confirms it end to end and reports the whole series
rather than two endpoints, because a one-off start-up cost and a per-cycle leak
produce the same average and have completely different consequences at forty
cycles. The line is straight over twelve cycles and does not flatten:
**79 B of resident memory per identifier issued, deleted, and never reusable.**
At 1.5M nodes per rebuild that is ~119 MiB added permanently per rebuild, on top
of a live set that has not grown.

Neither figure is a regression — both describe behaviour that has been there
since the CSR image existed. What is new is that they are measured, and that the
existing published number is now qualified by the deletion shape it assumed.

### Compaction's ordering is now tested rather than argued

Compaction's crash-safety is not a property of any one statement. It is a
property of the *order* of about ten of them: the image is synced before the
checkpoint marker, because the marker vouches for the image; the rename happens
before the log is retired, because the log is what recovers the store if the
rename does not survive. Each of those is a claim about what a reopen finds if
the process dies at one exact point, and two of the ten were tested — the two a
test could stage by hand. The rest were held up by the comments asserting them.

`compactStepHook` makes every point reachable, and `TestCompact_FailsAtEveryStep`
stops a compaction at each one across four store shapes chosen to drive all three
`retireLog` branches — the branch that rotates the log whole, the one that carries
a tail, and the one that keeps the log and writes no marker at all, which is the
most dangerous difference in the file. Forty runs, each asserting the only thing
that matters: a reopen finds every record that was committed, with its index
entries intact and `VerifyIndexes` clean.

The suite was checked against injected defects rather than trusted. Moving
`retireLog` above the rename — the transposition the ordering exists to prevent —
loses committed nodes on reopen, and the test says which ones.

**A failed compaction no longer leaves the image behind.** The temporary image was
deliberately left on the write, drain and rename error paths, on the reasoning
that a stray temp file is ignored on the next open. It is, but the likeliest cause
of a compaction failing is a full disk, and the second failure is then a
multi-gigabyte carcass sitting in the directory the operator is trying to rescue.
Removing it is one call, and the step suite asserts nothing is left behind at any
of the ten points.

### The list of fuzz targets is derived from the tree

`make fuzz` kept its target list in a variable, the CI matrix kept a second copy,
and both fell behind: two merkle targets were written and then never run for a
release, while the local runner and the nightly job each went on reporting
success. A list of what to test is the one list that must not be maintained by
hand, so both now derive it — `make fuzz-targets` from `go test -list`, and the
workflow from a discovery job that fails if the list comes back empty. The matrix
went from four targets to nine on the first run.

Two of those nine are new — `FuzzReadOrderedKeySection` and
`FuzzReadCompositeSection`, the section readers that had none — and
`FuzzDeserialiseCSR` is now seeded with twenty-five hostile images built from a
valid one: truncated at each structural boundary, with `indexOffset` and
`sectionTableOffset` set to the length, one under it, `MaxInt64`, past it and
zero, with directory entries whose offset and length overflow when added, and with
a GIDX value length one byte past what remains. The seed builder walks the
directory with its own deliberately simpler code, so the corpus does not depend on
the parser it exists to attack.

### The zero-cgo, zero-dependency closure is asserted

Both were true and neither was checked. `TestZeroCgo` walks the whole build
closure — the engine's packages and the standard library's — and fails if any of
them compiles a cgo file; `TestZeroExternalDependencies` reads `go.mod` and fails
on any `require` line; and a third test asserts the exact set of files permitted
to import `unsafe`, in both directions, so a new one has to be argued for rather
than merely added.

The cgo test forces `CGO_ENABLED=1` for its own query, which is the whole test.
With cgo disabled, `go list` reports no cgo files for any package, so the check
would have passed on every machine that had it turned off — a test that cannot
fail, arrived at by an environment variable.

### Verification no longer builds a second copy of what it is verifying

`VerifyIndexes` is what an operator reaches for when something is already wrong,
which on this engine usually means the store has outgrown the machine. It was
allocating a full duplicate of the index to check the index.

Three structures did it. The postings cross-check accumulated a
`map[id]map[key-value]int` over every posting — the reverse index rebuilt, with a
map header of its own per entity — and then compared it against the reverse index
sitting beside it. Each composite index rebuilt its whole tuple-to-entities map
from its members. And `IndexedNodeIDs` returned every indexed identifier as a
slice, deduplicated through a map, to callers that kept only the ones they found
*wrong* — in a healthy store, none of them.

None of it was necessary, because both directions follow from probing and
counting instead of remembering. Postings lists are strictly ascending, so an
entity appears at most once under a key and value: "is it there" and "how many
times" are the same question and one binary search answers it. Probing every
reverse reference into the postings establishes that the reverse side is a subset
of the forward side; both are sets, so once the totals agree they are the same
set — which is exactly what "every posting has a reverse entry" was asking. The
composite index is checked the same way.

The count cannot name an offender, so when it disagrees a second walk goes and
finds it, with the same bound and the same messages as before, paid only by an
index already known to be broken.

Measured on a 32,000-entry index, `Verify` now allocates **0 bytes**. The
structure it replaced cost **410 bytes per indexed entry**, which is what the new
guard measures when it is reintroduced. The guard asserts a slope rather than a
ceiling, for the reason the alloc guards already record: a ceiling loose enough to
be stable is loose enough to miss a doubling.

Two of the corruptions the old shape could not report are now caught. A member
holding the same value twice at one position repeats a tuple, and the rebuilt set
quietly absorbed it; a member value stored past the composite's declared width is
never enumerated and contradicted nothing. Both are now named.

**`VerifyOnOpen` reads the image once.** Its three legs — the bytes are unchanged,
the roots describe those bytes, a named key vouched for the result — each opened
the file for itself, and the last two parsed it again from scratch. With a
verifier configured, `Open` read a file that on the target workload is most of the
machine's memory four times and decoded it three times. The legs still run in the
same order and still prove three separate things (a digest that matches is no
evidence the roots do, which is what catches an edit that repaired the digest);
they now share one read and one parse, and `Open` is down to two reads and two
decodes. Measured as a multiple of one read-and-parse of the same image, the check
went from 3.03× to 1.91×.

**API change, `index` package.** `PropertyIndex.IndexedNodeIDs` and
`IndexedEdgeIDs` are replaced by `ForEachIndexedNodeID` and
`ForEachIndexedEdgeID`, which take a callback and return nothing. The walk does
*not* deduplicate: the index is sharded by property key, so an entity carrying
entries under keys in different shards is offered once per shard. Deduplicating is
precisely the map this exists to avoid, and a caller that minds deduplicates what
it keeps — which is bounded by what it is looking for rather than by the index.

### What an `Open` will cost, answerable before it is paid

`Open` is the one operation whose cost nobody could see in advance. An image is a
file whose size is on disk. A log that was never compacted is a whole write
history that is replayed into memory on every open, and the only way to find out
how much was to try it — on a machine where trying it is the failure.

`disk.PreflightOpen(dir)` answers it. It takes no lock, opens no store, replays
nothing, and runs **in memory bounded by a constant** whatever the size of the
store. That last part is the whole point, and it is what the two existing
inspectors cannot do: `InspectCSR` reads the file and parses the entire graph,
and `InspectWAL` keeps a descriptor per record. Either one, asked about a store
that will not fit, does not fit either. Measured across a tenfold increase in log
size, the new walk allocates **0.00 bytes per record**.

It reports the two halves separately and deliberately — every `Image` field
describes the compacted image, every `WAL` field the log — because on a store
that has never been compacted the first half is empty and the second half is the
entire cost. A single whole-store number would report its smallest value for the
largest store.

**The record count is replay's own, not an approximation of it.** A batch that
never committed is read, verified, held and discarded, so its records are counted
apart from the ones that land; a commit that disagrees with the body it follows
discards its batch, which the walk knows because it recomputes the body checksum
as the bytes stream past rather than buffering them. A checkpoint stops the count
where it stops replay. The guard for all of this drives the real parser and
compares, rather than asserting a constant.

Three things it cannot know, all stated on the fields rather than left to be
discovered: it decodes no payloads, it applies no signature policy, and a
concurrent writer invalidates it. Every count is therefore an upper bound, which
is the safe direction for a figure an `Open` is about to be gated on.

**`Options.MaxReplayBytes` and `Options.MaxReplayRecords`** turn the answer into
a refusal. Over either, `Open` fails with `disk.ErrReplayBudget` naming both the
figure and the budget. The engine cannot catch an out-of-memory condition in Go,
so a bound can only mean refusing before allocating, never degrading while
allocating — and the refusal is placed before any ledger is opened, so a routine
budget refusal does not append a hash-chained record of a crash that did not
happen. Zero, and any negative value, is unlimited; `StrictOptions` sets neither,
and a test now says so.

The byte budget is compared against the log's *records region*, not its file
size. The two differ by the 50-byte container header — small enough to go
unnoticed, large enough to refuse a store that has not changed. The record budget
costs a counting pass, so it is skipped outright whenever the log is too small to
hold that many records (every compacted store) and otherwise stops one record
past the budget. Both bound the replay and nothing else: the image is loaded by
the same `Open` and is not covered.

### Fixed: a corrupt batch marker could ask replay for gigabytes

A WAL batch begins with a marker naming how many records follow, and replay
reserved that many pending slots before reading one of them. The bound above that
reservation has to admit a legitimate batch, so it is derived from the log's size
— and a log's size is a poor allocation limit. At nine bytes a record, a 2 GiB log
permits 238 million of them, and reserving that many asks for something over
seven gigabytes. The marker's own checksum is no defence: it is computed over the
bytes making the claim. Worse, the bound is skipped entirely when the log's size
cannot be determined, and then a four-byte field decides the allocation outright.

The reservation is now capped at 65 536 slots. This decides no outcome — a larger
batch still replays, because a capped reservation is still grown by `append`.
What it removes is the file's ability to name a number and have it allocated: a
marker declaring ten million records now reserves 2 MB where it previously
reserved 320 MB.

**The cap was 4096 first, and measurement rejected it.** Below the batch sizes
this engine actually writes, a cap does not save memory — it replaces one exact
allocation with a geometric growth sequence that allocates several times as many
bytes in total. Interleaved against the same tree without the change, a cold open
of a 10 000-node log cost **+3.15 MB/op and +11 allocs/op**: a memory regression
on precisely the path the cap was added to protect. The bulk writers batch in
chunks of ten thousand, so the cap belongs above them. At 65 536 the same
measurement is +864 B/op and +1 alloc/op, both inside run-to-run noise, and a
corrupt marker is still bounded by a constant rather than by the log's size.

### Fixed: a backup read back the whole image it had just streamed

A backup copies every file of the store through a fixed buffer, and the
program's plan recorded both backup and restore as streaming on the strength of
reading the code. This release's residency instruments gained
`BenchmarkRSS_Restore`, which samples the process during a backup and then a
restore of the same store, so that the claim stays measured. The restore is
streaming: **0.27 MiB** of peak growth against a 44 MiB store. The backup was
not. It ends by re-verifying the digest of the copied image — the check that
makes "the backup succeeded" a statement about the bytes in the destination
rather than the bytes sent there — and that check read the file whole. A backup
grew the process by **44.32 MiB on a 44.01 MiB store**: the entire image,
allocated a second time, on the one operation an operator runs *because* the
machine is short of memory.

The digest is now computed as the file is read. The two regions it excludes —
the digest field itself and the compaction timestamp, which records when the
image was written and is not content — are zeroed in a copy of the header
before it is hashed, and the body streams through the hash from there. One
definition of the covered bytes now serves both this and the whole-buffer form
`VerifyOnOpen` uses on an image it already holds, so the two cannot drift, and a
thirteen-case table plus a fuzz target compare them byte for byte over every
shape the pair can tell apart: files shorter than a header, files that predate
the digest, a tampered body, a changed timestamp. A file that fails to *read* is
reported as an error and never as "carries no digest" — a damaged disk and a
pre-v8 image are different answers, and only one of them is safe to act on.

Measured on the same 44 MiB image, three interleaved rounds against the tree
without the change: a backup went from **46.24 MB/op to 119 KB/op** and the
digest check alone from **46.16 MB/op to 34 KB/op** — one 32 KiB copy buffer
and the hash state — with wall clock inside run-to-run spread on both, the
streamed form marginally ahead in every round because a buffered read has no
file-sized allocation to fault in. The guard is
`TestVerifyCSRDigest_DoesNotReadTheImageWhole`, which bounds what one call
allocates at a megabyte against an eight-megabyte image; reintroducing the
whole read fails it and nothing else does.

`store csr -verify`, `debug hash-check` and `debug integrity` share the leg and
are bounded on it, not overall: their roots check still parses the image, which
is the whole-image verifier a later phase replaces with one over a mapping.

### Two promises about the store directory, written down before a mapping depends on them

The next phase of this program maps the image instead of decoding it, and a
mapped file has a failure mode a heap copy does not: when the bytes behind it
change or vanish, the next access is not an error but a machine fault. §15 of
`TECHNICAL_DETAILS.md` now states the two invariants that make a mapping safe
to introduce, so that the contract exists before the code that needs it does.

**A store file is never rewritten in place** (§15.13). Every replacement is
written to a temporary file, fsynced, and renamed over the old name, so the
bytes behind an open handle never change under it. Two things already rest on
this — the streamed digest above, and a live reader parsing an image while a
writer compacts — and a mapped image would rest on it absolutely.

The two platforms honour it by opposite routes, and writing the invariant down
is what found the difference. On unix the rename leaves the old inode alone and
a held handle goes on reading the image it opened. On windows, renaming a file
*over* another is refused while any handle to the target is open — with
`FILE_SHARE_DELETE` and without it alike, because that flag permits renaming a
file *aside*, which is a different operation. The reader is protected just as
completely, by the compaction failing rather than by two files coexisting: a
process holding `graphene.csr` open stalls a writer's compaction with `Access
is denied`, the store is left untouched, and the next attempt succeeds once the
reader lets go. For the mapping work this settles something the plan had only
guessed: installing an image underneath a mapping cannot be a single rename on
windows, so the old name has to be moved aside first and the new file put in
its place second. `TestImage_AHeldHandleNeverSeesTheBytesChange` pins both
routes.

**The directory is the engine's to write, and no one else's, while a handle is
open** (§15.14). This is an obligation on the caller rather than a property the
engine can enforce: the write lock keeps other graphene processes out, and
nothing keeps out `truncate`, an editor, or a backup tool that rewrites files in
place. With the image read into the heap, breaking it costs a parse error at the
next reopen. Under a mapping it costs the process, at whatever unrelated line of
caller code happened to touch the slice — `SIGBUS` on unix,
`EXCEPTION_IN_PAGE_ERROR` on windows, neither recoverable in Go. That is a
*defined* outcome rather than a safe one, and it binds hardest on `OpenLive`
readers, which take no lock at all. The subprocess test that asserts the fault
waits for the mapping to exist.

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

### A query's cost tracks its answer, not the graph

`Offset`/`Limit` used to be a step at the end of the pipeline and nothing more.
The engine produced every row, sorted them, and copied ten out — so ten rows off
a 100 000-node label cost the label. Nothing streamed either: there was no
iterator anywhere in the module, and enumerating a graph meant materialising one
ID per record before the caller saw the first one.

**Label postings are merged rather than concatenated.** The delta's postings and
the image's are both ascending and duplicate-free, and were being appended
together and deduplicated through a map. Merging them drops the copy and the map
— but the point is that the result is now *ordered*, which is what makes
everything below sound: the label driver reports its candidates as ascending, and
labelled queries skip the sort they used to end with.

**A window is pushed back into the pipeline.** Where nothing between a step and
the window can remove a row, the bound is handed to that step so it stops
producing:

| Query, 100 000-node fixture | Before | After |
|---|---|---|
| `Types + Limit 10`, disk | 17 952 B, 121 allocs | **160 B, 2 allocs** |
| `Types + Limit 10`, memory | 7 416 B, 16 allocs | **240 B, 3 allocs** |
| `Types + prefix filter + Limit 10`, disk | 19.1 MB, 90 818 allocs | **722 KB, 6 allocs** |
| `Types + prefix filter + Limit 10`, memory | 9.55 MB, 559 allocs | **1.44 MB, 7 allocs** |

The filtered case cannot bound its driver — a filter removes candidates, so the
tenth candidate is not the tenth row — so the bound moves one step later: the
residual pass evaluates a prefix of the candidates, stops when enough survive,
and doubles the prefix when they do not. The doubling is what keeps §7.3's
per-step probe-versus-build costing meaningful while bounding the repeated work
at a small constant factor.

Four cases refuse the bound rather than approximating it, and go on costing what
they always did: a descending query, one driven from an explicit ID list, a
`MatchAny` query, and an edge query restricted by endpoint. The ordered-index
driver emits value order rather than ID order, so it cannot take one either.

**Iteration, over a snapshot.** `store.Scanner` adds `ScanNodes`, `ScanEdges` and
`ScanNodesByType`, each an `iter.Seq2[ID, error]`:

```go
snap, _ := g.Snapshot()
defer snap.Close()

for id, err := range snap.(store.Scanner).ScanNodes() {
    if err != nil {
        return err // closed or expired mid-scan
    }
    // ... break whenever you like
}
```

It is offered over a `Snapshot` and not a live store, which is the answer to what
a scan should do when a writer changes the graph beneath it rather than an
omission. `Seq2` rather than `Seq` because a scan can fail partway and a scan
that stopped early must not look like one that finished. IDs arrive ascending on
both backends — the same order the equivalent query returns — so a scan
substitutes for one: `bulk` now streams a source that implements `Scanner` and
enumerates one that does not, and the two produce byte-identical dumps.

The disk implementation resolves a batch under the store lock and yields it
outside, never holding the lock across caller code; the batch starts at 32 and
doubles to 512, so taking one ID does not pay to resolve five hundred. Peak
memory is the delta plus one batch rather than the graph — 6 allocations for a
full walk and 6 for a walk stopped at the first ID, neither growing with the
graph. A snapshot also enumerates its property entries now, which is what lets it
back a complete `bulk` export.

A snapshot reports the store's index declarations too — the read half of the
declarer interfaces, split out as `store.OrderedIndexReporter` and
`store.CompositeIndexReporter`, because a view may say what was declared and must
not declare anything itself. Without it a dump taken through a snapshot carried
an empty header and imported with no ordered or composite indexes, which is a
silent loss rather than a failure: the import succeeds and the restored store
answers range and composite queries the slow way. `graphene export graph` now
reads through a snapshot, so a whole-graph export streams its enumeration and
dumps one graph rather than whatever each read happens to find.

**Fixed: an export was not reproducible.** `PropertyIndex.ForEachNodeProperty`
is the streaming form of `NodeEntries` and writes a dump's property section, but
it ranged the per-value map directly instead of ordering it. Two exports of one
unchanged graph therefore disagreed about the order of their property lines
about one run in seven on a key with 200 distinct values — the same class of
defect `NodeEntries` sorts to avoid, and whose reasoning that function's comment
already gave. It now walks values in order. The buffer that orders them is sized
to the key rather than grown by doubling, which makes `NodeEntries` and
`EdgeEntries` cheaper as well: 33% fewer bytes on a 50 000-entry index, on the
path `Compact` writes the CSR index section from.

### The shortest path can now be the cheapest one

`ShortestPath` returns a path with the fewest edges, which is the right answer
only when the edges are interchangeable. When they carry a similarity, a
duration or a size, fewest is not cheapest — and the engine had no way to say
so. There is now Dijkstra and A\*:

```go
gap := func(e store.IncidentEdge) float64 { return 1 - float64(e.Weight) }

path, err := g.ShortestWeightedPath(src, dst, nil, gap)
path, err  = g.AStarPath(src, dst, nil, gap, estimateRemaining)
```

with `…Ctx` forms taking a context and a `store.Budget`, whose `MaxNodes` counts
nodes *settled* — reached with their final cost — so it keeps meaning what it
means for every other walk.

**The cost comes from the caller, and this is the design decision.** The obvious
alternative was to read `Edge.Weight` as a distance, and it is wrong twice over:
`Weight` is documented as a similarity score for `EdgeTypeSimilarTo` and encoded
as one in the Merkle leaf, so reading it as a distance makes "more similar" mean
"further away"; and no single reading suits every caller anyway. A selector
taking `*store.Edge` would have been the other obvious shape, and would force a
`GetEdge` per relaxation — the per-record catastrophe the adjacency extension
exists to avoid. So `store.IncidentEdge` grew a `Weight` field instead, on the
same argument that put `Neighbour` there: the store already holds the edge
record while filtering, so copying a float32 out is free.

That field is additive — all three construction sites use keyed literals — and
was measured against a control identical but for it: **allocation counts
unchanged on every traversal benchmark**, bytes up by one buffer entry's width
(24 B/op on a 3-hop BFS, 8 B out of 1.2 MB on a deep DFS).

**The search is unidirectional, and that costs something.** A bidirectional BFS
may stop at the first node both frontiers reach, because for an unweighted walk
that meeting is on a shortest path by construction. It is not, once edges have
costs — the first meeting is merely the first. Making bidirectional Dijkstra
correct means continuing past it with a different termination proof, so
`ShortestWeightedPath` does not do that. The consequence is measured and
reported rather than buried: on the benchmark fixture a weighted path runs ~15×
slower than the unweighted one, because it settles every node cheaper than the
destination instead of meeting in the middle. `docs/benchmarks.md` has the
numbers. A\* is the answer where an estimate exists — on a grid it is worth 2.8×
in time and 38% in memory, for the identical path.

**What it refuses, and what it cannot.** A nil cost function is an error rather
than a default unit cost, because a unit cost is exactly `ShortestPath` and
quietly returning the unweighted answer would hide the mistake for as long as
the weights happened not to matter. A negative or NaN cost is refused at the
relaxation that produces it: Dijkstra is not approximately wrong with a negative
edge, it is wrong, and a plausible bad path is worse than an error. That check
covers costs the search *uses* — it stops when the destination settles, so an
edge in a corner it never examined is never costed, and the path returned is
unaffected by one.

**A\* asks something in return.** `store.NodeHeuristic` must never overestimate
the remaining cost. One that does returns a dearer path and no error, because
catching it would mean computing the answer the estimate exists to avoid. A node
is settled once and never reopened, which under an inconsistent heuristic is
what turns the guarantee into "a path" rather than "the cheapest path";
reopening would restore optimality at the price of unbounded re-expansion, and
that trade is not made. A nil heuristic is exactly Dijkstra.

Where several routes tie for cheapest, which one comes back is unspecified: it
follows the order the backend reports incident edges in, which is not a promise
the store makes across layer states. The cost is determined by the graph; the
shape, among equals, is not.

### Fixed: `BeginTracked` livelocked under contention

A read-modify-write retry loop with a few concurrent writers never finished. The
transaction read at the visible epoch while its read set was validated at the
applied one, and group commit leaves those apart for the whole of a commit's
fsync — with the store lock released. So a transaction read the old value, was
refused by a writer it was not permitted to see, retried, read the *same* old
value because the fsync was still outstanding, and was refused again. With
several writers there is always a commit in flight, so no attempt could ever
incorporate the write refusing it. Measured at 15 877 conflicts with five of six
writers starved; with `SetSyncOnCommit(false)`, which closes the window, 1 046
conflicts and none starved.

A tracked transaction's reads now resolve through the new `store.AppliedReader` —
the same view its validation will use. The exchange is that such a read can see
state that is committed but not yet durable; a crash losing that write loses this
transaction's commit with it, since the commit is ordered after it in the same
log and replay stops at the first bad frame. Untracked transactions and ordinary
reads are unchanged and go on seeing only what is durable.

### Fixed: an export could write a dump that could not be imported

The property index is not versioned and its entries can outlive their records —
indexing a node that has been deleted leaves one, and so does `ReindexKeep` on an
update. `bulk` copied them out verbatim, so the dump named an entity it never
defined and the import of it failed with `ErrOutOfOrder`, after the operator had
a file they believed was a backup. Property entries whose entity is not there are
now dropped, which is one lookup per entry rather than a set of everything
exported — the export is streamed precisely so it does not hold the graph.

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
- `QueryPlan.Candidates` is now "candidates examined" rather than "size of the
  driving set". A driver that stopped at the window never learned its own full
  size, and running it again unbounded to fill the field in would make asking
  for the plan cost more than the query. `ResidualStep.Probe` has always been
  documented as a forecast rather than a fact; this is the same trade.
- `TestQueryPlanner_WindowParity*` compares every windowed query against the
  *unwindowed* one sliced, because the existing forced-scan oracle takes the
  same window through the same push-down and would agree with a bug in it.
  `TestResidual_BoundedPassCrossesChunkBoundaries` builds a candidate set
  several prefixes deep whose matches are all at the far end, which is the only
  way to reach the code that has to hold its place across them.
- `traversal/heap.go` is a hand-rolled binary min-heap rather than
  `container/heap`, whose interface costs an indirect call per comparison and
  whose `Push` takes `any` — boxing every element, which would put one
  allocation on the inner loop once per relaxed edge. The queue uses lazy
  deletion rather than decrease-key, because a decrease-key heap needs a
  position index and that per-node allocation is the whole saving.
- The weighted search has an oracle of its own: a naive Bellman-Ford written
  against the public API, in the spirit of `referenceBFS`. It walks `EdgesOf`
  rather than `Neighbours`, deliberately — `Neighbours` reports one result per
  distinct neighbour, so an oracle built on it cannot see the two parallel
  edges of unequal cost that are the case the feature exists for.
- `TestAllocGuards_Paths` asserts a *slope*, not a ceiling. A ceiling on a
  200-node chain would mostly measure the fixture, since the path is the graph
  and materialising it dominates; the property the searches actually promise is
  that cost tracks the path and not the walk, so it runs each search to two
  depths and checks the difference is the records. It also replaces a number
  nobody could fail: `BenchmarkShortestPath` recorded 37 allocs/op and had no
  `b.ReportAllocs()`, let alone a guard, while this release refactored the tail
  it shares with the weighted search.
- `BenchmarkWalk_Grid_*` has its own fixture because the shared one cannot host
  it: 1 000 inbound edges into a single hub put every node within two hops of
  every other, so any distance-based estimate overestimates — and an
  overestimating heuristic does not make A* slower, it makes it wrong. The
  admissibility check in the benchmark setup caught exactly that, and compares
  costs rather than hop counts, which was the original error.
- Both "applies to every walk" tables — the deadline table in
  `traversal/guard_test.go` and `TestBudget_AppliesToEveryTraversal` — gained
  the two new walks. Their whole point is that a walk they miss is unbounded.
- `TestAppliedReader_*` constructs the applied-but-not-visible state directly
  rather than racing for it: the window it covers is open only while a commit
  waits on its fsync, so a test that raced would pass on a fast disk.
- `TestVerifyCSRDigest_DoesNotReadTheImageWhole` bounds
  `runtime.MemStats.TotalAlloc` rather than sampling residency. The counter is
  cumulative and exact, unaffected by when the collector runs, so a single
  whole-file read shows up as one allocation the size of the file — and the
  test refuses a fixture too small to tell, rather than passing on it.
- `os.SameFile` cannot detect a replaced file on windows. Go's `fileStat` loads
  the file identifier lazily, by path, so two stats of the same name taken
  before and after a rename-over compare equal. The held-handle test re-reads
  the bytes it holds instead of comparing identities, which asserts the property
  directly on both platforms.

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
