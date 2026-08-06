# Forensic Integrity — a working guide

Graphene can produce evidence about what it holds: that an entity was in a
specific snapshot, that another was deliberately removed and by whom, and that
neither claim has been altered since. This guide is **how to call it**.

Two companion documents, and the split matters:

- [SECURITY.md](../SECURITY.md) is the authority on **what each mechanism
  proves and what it does not.** Read it before relying on any of this for
  something evidentiary. Everything here assumes you have.
- [`examples/forensic_examples.go`](../examples/forensic_examples.go) is the
  same material as runnable code. `go run ./examples` executes it.
- [API_REFERENCE.md §22](API_REFERENCE.md#22-best-practices-for-evidentiary-use)
  is the checklist: what to do, what not to, and where the performance guide's
  advice and evidentiary requirements pull in different directions.

## Two things to know first

**All of it is opt-in.** A default store writes unsigned commits and verifies
nothing, which is the historical behaviour and stays the default. Nothing below
happens unless you ask for it.

**It lives on `disk.Store`, not `graphene.Graph`** — but you can get there from
the façade, and if you already hold a `Graph` that is the way:

```go
g, err := graphene.OpenWithOptions(dir, opts)   // opts below
s, ok := g.Forensics()                          // false on the in-memory backend
```

`Forensics` returns the *same* store the Graph is using, so writes through
either are visible to both. `graphene.Open` still gives the historical defaults —
unsigned, unverified — which is why the strict posture needs
`OpenWithOptions`.

Examples in this guide call the store directly for brevity:

```go
import "github.com/aoiflux/graphene/disk"
```

## Adoption order

Each step is useful on its own and each assumes the ones above it.

| | Step | Calls |
| --- | --- | --- |
| 1 | Sign the log, verify the image on open | `StrictOptions` |
| 2 | Prove an entity was in a snapshot | `ProveNode` / `VerifyNodeInclusion` |
| 3 | Hand that proof to someone else | `ExportNodeProof` / `VerifyExportedProof` |
| 4 | Redact lawfully, and prove you did | `RedactNodeProperties` / `ProvePropertyRedaction` |
| 5 | Account for an entity end to end | `CustodyFor` |
| 6 | Publish a checkpoint to an anchor | `PublishCheckpoint` / `VerifyAgainstAnchor` |

---

## 1. Turning it on

```go
key, pub, err := signing.GenerateKey(1)          // or implement store.Signer
ring := signing.NewKeyring()
ring.Add(1, pub)

opts := disk.StrictOptions(key, ring, 42)        // 42 = actor recorded in attestations
opts.Retention = disk.RetentionPolicy{MaxSegments: 20}
opts.Redaction = true
opts.RedactionPolicy = disk.RedactionPolicy{MaxCascade: 50}

s, err := disk.OpenWithOptions(dir, opts)

// or, from the façade:
g, err := graphene.OpenWithOptions(dir, opts)
s, _ := g.Forensics()
```

`StrictOptions` is the cautious posture: every commit signed **and required to
be signed**, the image verified before it is loaded, and the audit log on.

The two extra opt-ins cost extra files, which is why they are separate:

| Option | File | What you lose without it |
| --- | --- | --- |
| `Retention` | `graphene.NNN.wal` | Every commit's actor, timestamp and signature from before the last compaction, plus key rotations |
| `Redaction` | `graphene.redactions` | Attributed removal — `RedactNode` refuses rather than degrading to `DeleteNode` |

**Compaction is what makes things provable.** Snapshot roots and the attestation
over them are written by `Compact()`. Before it, an entity is live but
unaccounted for — a different thing from absent.

```go
s.Compact()
roots, _ := s.SnapshotRoots()
```

**Retain `roots.Snapshot` outside the system.** Every check the engine can run
compares the store against itself; only a root you kept elsewhere distinguishes
"internally consistent" from "not tampered with".

## 2. Proving an entity was in a snapshot

```go
proof, err := s.ProveNode(id)
err = disk.VerifyNodeInclusion(retainedRoot, proof)
```

`VerifyNodeInclusion` is package-level and takes no store: a recipient checking
evidence has the proof and a root, and nothing else. The root is an **argument**,
never read from the proof — checking a proof against a root carried inside it is
circular, because whoever produced both chose both.

## 3. Handing a proof over

```go
blob, err := s.ExportNodeProof(id)                    // producer
proof, err := disk.UnmarshalProof(blob)               // recipient
err = disk.VerifyExportedProof(retainedRoot, proof)   // recipient
```

From a shell, deliberately as two commands:

```
graphene prove -node 7 -out claim.gprf <dir>
graphene verify-proof -root <hex> claim.gprf     # touches no store
```

What travels is the leaf, the sibling hashes and the component roots — never the
image and never any other entity. That is what makes handing a proof over safe
where handing over the file would not be.

A proof file is not signed, and does not need to be: every field a verifier
relies on is bound into the root it is checked against, so one altered byte
either fails to parse or fails to verify. Signing would say who *sent* it, which
is a different question from whether it is *true*.

## 4. Redaction

Four scopes, because they are four different decisions:

```go
s.RedactNode(id, req)               // the entity, and every edge touching it
s.RedactNodeProperties(id, req)     // the property blob; ID, labels and edges stay
s.RedactEdge(edgeID, req)           // one relationship, both endpoints kept
s.RedactEdgeProperties(edgeID, req) // an edge's properties; the relationship stays
```

**`RedactNodeProperties` is usually the one you want.** An erasure request is
almost always about personal data, not about the existence of the artefact that
carried it — so removing the whole node destroys evidence that was never in
scope, including every relationship that made the artefact meaningful.

Look before you leap:

```go
impact, err := s.RedactionImpactFor(id)
// impact.CascadedEdges — what a whole-entity removal would take with it
// impact.ExceedsPolicy — whether that exceeds RedactionPolicy.MaxCascade
```

Every form requires a `Reason`. This is not a formality: an unexplained
redaction is indistinguishable from evidence destruction, which is the entire
distinction the ledger exists to draw.

```go
rec, err := s.RedactNodeProperties(id, disk.RedactionRequest{
    ActorID: 7, RoleID: 3, Reason: "subject access request 41",
})
```

### Proving a redaction

Compact first — until then the ledger knows and the image does not, and a
recipient given only the image cannot tell the entity from one that never
existed. `CustodyReport.RemovalProvable` reports that window.

```go
s.Compact()

proof, err := s.ProvePropertyRedaction(id)               // content-free
err = disk.VerifyPropertyRedaction(retainedRoot, proof)
```

That check establishes something a bare hash comparison cannot: that the
entity's **identity did not change**, only its properties — and the verifier
never sees what was removed.

For whole-entity and edge removals:

```go
proof, err := s.ProveRedaction(id)          // node
proof, err := s.ProveEdgeRedaction(edgeID)  // edge
err = disk.VerifyRedactionInclusion(retainedRoot, proof)
```

Reading the ledger:

```go
records, err := s.Redactions()
err = disk.VerifyRedactionChain(records, ring)   // ring may be nil
```

```
graphene redactions <dir>            # who removed what, when, and why
graphene redactions -node 7 <dir>
```

## 5. Chain of custody

One report walking every history for one entity:

```go
report, err := s.CustodyFor(id, ring)              // ring may be nil
report, err = s.CustodyForAnchored(id, ring, retainedRoot)
report, err = s.CustodyForAnchor(id, ring, anchor) // strongest
```

It returns **gaps, not a verdict**. "Not verified" is useless to someone holding
evidence; "the segment chain is intact but nothing attested the current
snapshot" says what to do next.

```go
report.Complete()   // no gaps at all
report.Broken()     // some chain actively failed, as opposed to never established
report.Summary()    // one line for a log or a CLI
```

`CustodyFor` **can never be `Complete()`** — it always reports that every check
compared the store against itself. Supplying a retained root or an anchor is
what closes that.

```
graphene custody -node 7 <dir>
```

## 6. Anchoring

A `Checkpoint` binds every history's head — snapshot, attestation, WAL segments,
audit log, redaction ledger — into one digest. Publishing only the snapshot root
would leave the others free.

**The engine ships no anchor transport, by design.** What makes an anchor an
anchor is being beyond the reach of whoever can rewrite the store, and nothing
in-process can establish that. Implement the interface:

```go
type Anchor interface {
    Publish(digest merkle.Hash) (AnchorRecord, error)
    Records() ([]AnchorRecord, error)
}
```

against a timestamp authority, a transparency log, another organisation's
storage, or a printed sheet in a safe.

```go
c, receipt, err := s.PublishCheckpoint(anchor)
audit, err := s.VerifyAgainstAnchor(anchor)
```

`disk.InsecureLocalAnchor` exists **only** so the verification path can be
tested and demonstrated. A file on the same machine is not an anchor; its
constructor refuses a path inside the store directory, which is a tripwire
against the most likely misuse and not a security property.

The guarantee is bounded by how often you publish — everything since the last
checkpoint is still freely rewritable, and `AnchorAudit` reports that window
rather than leaving you to infer it.

```
graphene anchor -publish -insecure-local-file <path> <dir>
graphene anchor -insecure-local-file <path> <dir>
```

## Reading a store from a shell

```
graphene info    <dir>        summary of the image and the log
graphene csr     <dir>        CSR header detail (-verify checks digest and roots)
graphene wal     <dir>        record-by-record log dump
graphene verify  <dir>        structural index check
graphene custody <dir>        account for one entity across every history
graphene redactions <dir>     ledger of attributed removals
graphene anchor  <dir>        publish or check a checkpoint
graphene prove   <dir>        export a proof
graphene verify-proof <file>  check a proof against a root you retained
```

`info`, `csr`, `wal` and `redactions` read the files directly and are safe
against a store another process is using. `verify-proof` touches nothing but the
file you give it. Everything else opens the store.

`anchor -publish` is the only subcommand that writes, and it only appends.

## What none of this does

Summarised here so it is not only in `SECURITY.md`, which you should still read:

- **It does not prevent anything.** Graphene is a library in your process; code
  running there can call any API and use your signing key. What the machinery
  does is make the *result* detectable to someone outside.
- **It does not make contents true.** Signatures prove who asserted something,
  never whether it is so.
- **Nothing is anchored until you supply an anchor**, and until then every
  guarantee is the store vouching for itself.
- **There is no access control.** `RoleID` is recorded and never checked.
