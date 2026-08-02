# Contributing to Graphene

How to change this engine without breaking it, and how to know whether a change
actually helped.

Most of what follows is written from failures that happened here. Each one cost
real time, and each is the kind that repeats.

- For the design, read [TECHNICAL_DETAILS.md](docs/TECHNICAL_DETAILS.md).
- For the API, read [API_REFERENCE.md](docs/API_REFERENCE.md).
- For numbers and method, read [benchmarks.md](docs/benchmarks.md).

---

## 1. Measuring

### Never compare across sessions

**This is the mistake that recurs most.** Three separate times in one
development run, a change looked like a large win because "before" came from an
earlier benchmark session and "after" from the current one:

| Claimed                                            | Actually                            |
| -------------------------------------------------- | ----------------------------------- |
| batch writes worth ~5%                             | 21–38%                              |
| batch reads worth −47%                             | −10 to −15%                         |
| disk bulk write improving, memory backend "slower" | memory backend was _unchanged code_ |

Machine state drifts — thermals, background load, page cache. Drift of ±25% on
untouched benchmarks has been observed here, which is larger than most effects
worth measuring.

**Always interleave.** Alternate the two variants inside one run, several
rounds:

```
for round in 1 2 3 4; do
  <restore variant A>; go test -bench ... >> a.txt
  <apply variant B>;   go test -bench ... >> b.txt
done
benchstat a.txt b.txt
```

### Interleaving is not sufficient on its own

Interleaving protects against drift *between* the two arms. It does not protect
against a control whose own absolute cost is moving underneath the experiment,
and that is a different failure.

A measured example, from the property-index ordering work: the same control
binary read **13 ms, then 27 ms, then 30 ms** across one session as the machine
loaded under back-to-back benchmark runs. Every pair was interleaved and every
pair had a control. The conclusions survived; the magnitudes did not — "+53%"
and "+20%" became **+36%** and **+12%** when re-measured on a settled machine.
Inflated by roughly half, from a procedure that followed both rules above.

Three additions that would have caught it:

- **Discard a warm-up round.** Run both arms once, throw the numbers away, then
  start measuring. The first round is routinely the outlier.
- **Report the control's own spread**, and treat a wide one as a failed
  measurement rather than a noisy result. 6% across rounds is usable; the bad
  session above was at 76%.
- **Prebuild the test binaries** so compilation is not inside the first timed
  round.

### Comparing two implementations? One binary, not two checkouts

A worktree A/B is the right tool for "is my change faster than `HEAD`". It is the
wrong tool for "which of these two algorithms should I write", because it makes
the answer hostage to machine drift for no reason.

Write both as sub-benchmarks over one fixture in one process and let the testing
framework run them back to back:

```go
func BenchmarkNodeEntriesOrdering(b *testing.B) {
    p := benchPropertyIndex(50_000)
    b.Run("structure-order/KeyValueID", func(b *testing.B) { ... })
    b.Run("sort-entries/IDKeyValue",    func(b *testing.B) { ... })
}
```

Keep the losing implementation in the tree as the comparison arm. `NodeEntries`
is ordered the way it is because that benchmark says the alternative is 1.53x
slower — and `nodeEntriesByIDReference` is still there so the claim stays
checkable rather than remembered.

That comparison also surfaced a trade the worktree runs had hidden: the chosen
ordering is faster **and uses 68% more memory** than the one it beat. Under the
priority order in §5 that is the right call, but it is a trade, and it was only
visible with both arms side by side.

### Always include a control

A control is a benchmark whose code is **byte-identical** on both sides. If it
moves, the run is invalid and no figure from it is quotable — however good the
headline looks.

The third mistake above was caught exactly this way: the in-memory backend's
`AddNodesBatch` never touches the WAL and had not been edited, yet appeared
33–89% slower. Unchanged code cannot move. That single observation invalidated
the whole comparison.

Good controls here: `PointLookupNode_Memory`, `GetNode`, or whichever backend
the change does not touch.

### Read the control before the headline

Ordering matters more than it sounds. Looking at a −99.98% result first and the
control second creates an incentive to explain the control away. Print the
control first and decide whether the run is valid _before_ seeing the wins.

### Footprint numbers are exempt

`graphene_footprint_test.go` measures resident bytes deterministically at ±0%
variance. Those figures do not need interleaving and do not care about machine
temperature. When a memory question can be answered by footprint rather than
timing, prefer it.

### Benchmark the boring case too

The residual-filter change delivered ~29 000× on its target benchmark and, in
the same run, made the most common query shape **14–23% slower** — single-filter
queries were still building a residual plan to conclude there was nothing to do.

Only a benchmark set wide enough to include the unremarkable case caught it. A
change that is spectacular on the case it was written for and untested elsewhere
is not measured.

### Check the fixture actually populates what you are optimising

A change to how the disk store handles property blobs measured at +2.57% — and
the run was meaningless. The giveaway was in the columns nobody reads first:

```
B/op      96.00 ± ∞     96.00 ± ∞     ~ (p=1.000)
allocs/op  2.000 ± ∞     2.000 ± ∞     ~ (p=1.000)
```

**Byte-identical allocation on both sides.** Removing a copy that runs must
change `B/op`. Identical allocation is not a weak result, it is proof the code
under test never executed.

It never executed because no benchmark in the repository had ever stored a
property blob. Every read fixture built `&store.Node{Labels: ...}` and called
`IndexNodeProperties` — which populates the property _index_, a different
structure. The record's own `Properties` field stayed nil, so the copy being
optimised was copying zero bytes everywhere it was measured.

Two habits follow:

- **Read `B/op` and `allocs/op` as a sanity check on the timing column.** If a
  change adds or removes work and they do not move, distrust the timing.
- **Assert inside the benchmark that the fixture has the property under test** —
  `graphene_blob_bench_test.go` fails the benchmark outright if a blob comes
  back empty. A fixture that silently degenerates to the trivial case turns a
  benchmark into a random number generator.

Then mutation-test the _benchmark_, the same way you would a test: restore the
behaviour you are removing and confirm the numbers move. If they do not, the
benchmark cannot see the thing it exists to measure.

---

## 2. Testing

### A test that cannot fail is worse than no test

After writing rollback tests for the WAL's transactional framing, they were
**mutation-tested**: the rollback was deliberately disabled to confirm the tests
noticed.

```
rollback disabled → TruncatedBeforeCommit fails with [keep1 keep2 keep1 keep2 lost1 lost2]
rollback disabled → TruncatedInsideCommit  fails with [x y] instead of nothing
```

Do this for any test covering a path that does not run in normal operation —
rollback, recovery, corruption handling. Those tests are otherwise only
exercised by the incident they exist to prevent.

The same technique confirmed the phantom-index-entry test: with the live-filter
removed, it returns the phantom ID, so the test genuinely guards the fix.

### Verify the code under test actually runs

A whole feature — the edge half of residual filtering: `NarrowEdgesByFilters`,
`probeEdges`, `matchEdges`, `PlanEdgeResiduals` — was written, compiled, and
**never called**, because only the node path had been wired into the stores.

It passed `go vet`, the full race suite and the stress suite while doing
nothing, because **Go does not warn about a method nobody calls**. It was found
by re-reading the file.

When adding a capability to both backends or both entity types, check that each
half is reachable. A quick `grep` for the call site is enough.

### Test against expectations, not against the other code path

Residual-filter semantics are checked against results computed **in the test**,
not against the engine's other evaluation path. A self-consistency check passes
just as happily when both paths are wrong in the same way.

### Assert on the work, not only the result

A query can return exactly the right IDs while scanning the whole graph, and no
assertion on results would notice. `ExplainNodeQuery` exists so planner
behaviour can be pinned:

```go
plan, _ := g.ExplainNodeQuery(q)
// driver=equality(sha256) candidates=1 residual=tool:probe~100000 results=1
```

These assertions are expected to need updating when the cost model improves.
Assertions on _results_ needing an update means something is wrong.

### The race detector does not find stale reads

The lock-free CSR read path was, in its first version, **perfectly synchronised
and perfectly incorrect** — a reader could sample a new generation counter, load
the old pointer, and accept a superseded CSR. The race detector finds
unsynchronised access, not wrong answers. Neither did the concurrency tests: the
window is two instructions wide.

For lock-free work the correctness argument has to rest on the design. Tests are
defence in depth, not proof.

---

## 3. Changing things

### Check the premise before optimising

Two planned optimisations dissolved on contact with the code:

- **"mmap the adjacency arrays so open does not parse them"** — the reader never
  parsed them. It rebuilds adjacency from the records and skips the arrays
  entirely. They were dead bytes: ~21% of the file, written on every `Compact()`
  and read by nobody.
- **"Defer index maintenance during batch writes"** — label postings already had
  an append fast path that batch IDs always hit, and batch writes never touch
  the property index at all.

Read the code first. Both items had been written from a plausible model of a
shape the code did not have.

### Deleting dead weight surfaces bugs

Removing those adjacency arrays immediately exposed a latent bounds bug: the
record checks demanded four bytes more than a v3+ record needs, constants
carried over from v2. They had never fired because the never-read arrays always
supplied slack. **Any valid file ending exactly at its last record would have
been rejected.**

Dead weight in a format hides the bugs that would otherwise surface. That is an
argument for removing it beyond the space saved.

### Measure the prize before building the thing that wins it

The reverse property-index map was replaced with a sorted array on the strength
of a _model_ saying map machinery was ~62% of index memory. The model was right,
and the change still failed — but the useful number came from an experiment that
took two minutes and could have run first:

```
index with the reverse map disabled → 179.4 B/node
full index                          → 263.7 B/node   ⇒ reverse map = 84.3 B (90%)
```

Deliberately breaking a structure to measure its share is cheap, exact, and
answers "how much is even available here?" before any design work. It also
decomposes: the same technique showed the array recovered only the map overhead,
and that **~32 B/entry was value strings pinned by the reverse entries** — a
component the model had not accounted for at all, and the largest one left.

Do this first for any memory work. A change that recovers 100% of the wrong
component is worse than no change.

### Re-profile after a change, not just before

The scan path was found by profiling the whole suite once several other changes
had landed — the slowest paths were no longer the ones written down, and the two
worst were not on any list.

It happened twice inside one change. A profile put ~24% of a range query in
`strconv` float parsing; the fix that shipped never touched parsing, it cut how
many comparisons ran, and the parsing disappeared with them. Re-profiling then
showed the new dominant cost was a reflective `sort.Slice`, which had been
invisible underneath.

Profile → fix → **profile again**. A cost that was 24% is not necessarily worth
attacking; it may be a symptom of a cost that is 100× more frequent than it
needs to be. And the thing underneath only becomes measurable once the thing on
top is gone.

### Be willing to revert

Bulk index loading was built, tested for equivalence, measured, and
**reverted**: it cut allocations 9–19% but cost 35–75% more resident memory.
Marshal-in-place was kept despite delivering no measurable speed win, because no
axis regressed.

### Measure resident memory, not just allocations

The bulk index loader looked excellent on `allocs/op` and was a disaster on
resident bytes. `-benchmem` reports allocation counts and bytes _allocated_;
neither is what a long-lived process holds. Use the footprint benchmarks.

### Watch for retention hazards

Slab-allocating many small objects cuts allocations, but a slab lives until
**every** object in it dies. That is a P2 gain bought with a P1 loss. It is why
`Node`, `Labels` and `deltaAdj` are still allocated individually while the
temporary marshal buffer was pooled — the latter is discarded immediately, the
former are retained.

### Keep the backends in step

`memory.Store` is the parity oracle the disk backend is tested against. When
behaviour changes on one, check the other.

Batch writes became transactional on disk and, for a while, were not on memory —
so a failed batch left partial edges on one backend and nothing on the other.
Since memory is what disk is _compared against_, that divergence would have been
measured as correctness. It was caught by a contract test written for disk and
run against both.

---

## 4. Documentation

- **Numbers live in `benchmarks.md`.** Everywhere else quotes at most a headline
  figure and links. A figure repeated in four files gets corrected in one.
- **Design lives in `TECHNICAL_DETAILS.md`**, including _why not_. "We tried X,
  it cost Y, we reverted it" is the most useful thing a codebase can tell the
  next person.

Never overstate a guarantee. `API_REFERENCE` and `TECHNICAL_DETAILS` both
claimed "a write is recoverable once its WAL append returns" — which was false;
`fsync` happened only in `Compact()` and `Close()`. An overstated durability
guarantee is the worst kind of documentation error, because it is believed
precisely when it matters.

Cheap checks worth running before committing docs:

````
# every exported Graph method is documented
# (anchor the receiver: a greedy `.*)` swallows the method name when the method
# takes no parameters, and reports the return type as undocumented instead)
go doc -all . | grep -E '^func \(g \*Graph\)' | sed -E 's/^func \([^)]*\) //; s/\(.*//' \
  | while read m; do grep -q "\b$m\b" API_REFERENCE.md || echo "UNDOCUMENTED: $m"; done

# code fences balance
for f in *.md docs/*.md; do awk -v F="$f" '/^```/{n++} END{if(n%2)print F": UNBALANCED"}' "$f"; done

# internal links resolve — checked *from each file's own directory*, because
# links are relative and the reference docs live one level down in docs/.
# Checking only root *.md silently passed 11 broken links when the docs moved,
# and a pattern anchored on '.md)' missed two more carrying a #section anchor.
# Strip the anchor before testing the path.
for d in . docs; do
  ( cd "$d" && grep -ohE '\]\([A-Za-z0-9_/.-]+\.(go|md)(#[A-Za-z0-9_-]+)?\)' *.md \
      | sed -E 's/^\]\(//; s/\)$//; s/#.*//' | sort -u \
      | while read t; do [ -e "$t" ] || echo "BROKEN LINK in $d/: $t"; done )
done
````

---

## 5. Before opening a change

`make check` on Linux and macOS, `.\test.ps1` on Windows. Both run:

```
go build ./...
go vet ./... && go vet -tags=stress ./...
go test -race ./...
go test -tags=stress -race -run 'Test' .
```

Neither runner is the authority on what must pass —
`.github/workflows/ci.yml` calls `go` directly and is the definition. The
runners exist to reproduce CI locally, so a target that disagrees with the
workflow is a bug in the target.

If the change touches a parser, also run `make fuzz` (or the equivalent
`go test <pkg> -run=XXX -fuzz=<Target>`). Fuzzing found six defects in the WAL
and CSR readers on its first outing, every one reachable from a file under 110
bytes, and every one sitting behind code that already had a bounds check beside
it. A failure writes the offending input to `testdata/fuzz` — **commit that
file**, it becomes a permanent regression test.

And, if the change is meant to be faster: an **interleaved** A/B with a control,
per §1. If the control moved, you do not have a result yet.

### On Windows, `gofmt -l .` lies

The repository is stored with LF and `core.autocrlf` checks it out as CRLF, so
`gofmt` objects to every line of every file and reports around thirty of them as
unformatted. Nearly all of that is noise.

To see the real answer, normalise before checking:

```sh
cat file.go | tr -d '\r' | gofmt -d
```

When this was first run against the tree, 31 reported files came down to **2**
genuinely misformatted ones. CI's `lint` job runs on Linux for exactly this
reason. Fix real findings by hand rather than with `gofmt -w`, which would
rewrite every line ending in the file and bury the change.

### The priority order

Ties are settled speed → memory → allocations, and correctness is not on that
scale at all — it is the precondition for the other three meaning anything. The
ordering exists to forbid one specific trade: spending memory purely to lower
allocation counts with no latency gain. That has happened once, and the change
was reverted.
