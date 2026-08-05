# Security

Graphene is an embeddable graph engine built for forensic artefact provenance.
It has integrity machinery — content digests, Merkle snapshot roots, inclusion
proofs, Ed25519 signatures, signed attestations — and this document exists to
say precisely what that machinery does and, more importantly, what it does not.

Overstating a security guarantee is worse than not having one, because it is
believed exactly when it matters. If anything below reads as hedging, that is
deliberate.

---

## The one-sentence version

**Graphene can make modification of its stored evidence detectable by a party
holding an independently retained reference value. It cannot prevent
modification by code running in its own process, and it cannot attest to the
truth of what it was told.**

Everything else here elaborates that sentence.

---

## 1. The trust boundary

Graphene is a **library linked into your process**. It is not a server, has no
process boundary, no authentication, and no authorisation.

```
 ┌───────────────────── YOUR PROCESS ──────────────────────┐
 │                                                          │
 │   your code ──► graphene.Graph ──► disk.Store ──► files   │
 │       ▲                                    │             │
 │       │  signing key lives HERE            │             │
 │       └────────────────────────────────────┘             │
 │                                                          │
 │   ◄── no process boundary, no authn, no authz ──►         │
 └────────────────────────┬─────────────────────────────────┘
                          │ ordinary filesystem
              ┌───────────▼────────────┐
              │ graphene.csr           │  ← reachable by anything
              │ graphene.wal           │    with file permissions
              └────────────────────────┘
```

Two consequences follow, and they govern everything below.

**Your process is the trusted computing base.** Anything executing in it can call
`DeleteNode`, run `Compact`, or use your signing key directly. No in-engine
mechanism changes that. What the engine can do is make the *result* detectable
to someone outside the process.

**The files are not.** `graphene.csr` and `graphene.wal` are ordinary files with
ordinary permissions. This is the boundary the integrity machinery genuinely
defends, and it defends it well.

---

## 2. What each mechanism actually does

| Mechanism | Detects | Does **not** detect |
| --- | --- | --- |
| **CRC32** per WAL record | accidental damage: torn writes, bad sectors, bit rot | **any deliberate edit.** CRC32 is keyless — anyone who changes a byte recomputes it in one line |
| **SHA-256 digest** over the CSR image | any change to the compacted file since it was written | who changed it. Anyone who can rewrite the body can rewrite the digest |
| **Merkle snapshot roots** | that the file's records do not match the roots beside them — including an edit that repaired the digest | a wholesale replacement where roots were recomputed too |
| **Inclusion proofs** | that a specific entity was in a specific snapshot | anything about entities not in the proof |
| **Ed25519 commit signatures** | an edited, forged, or reattributed transaction in the log | a signature made by a compromised process holding the key |
| **Signed snapshot attestations** | a substituted or unattested compacted image | the same: authorship, not authority |

### CRC32 is not a security control

Stated separately because it is the easiest thing to misread. The WAL's
per-record CRC32 exists to catch a torn write after a crash. It provides **zero**
resistance to a deliberate edit. Do not treat the presence of checksums as
tamper detection.

### WAL framing, and a limitation that applies only to older logs

The log carries a container header naming its **framing version**, and current
framing (v2) checksums each record's type and length along with its payload.

Logs written before that header existed use the original framing, where the CRC
covered **the payload only**. There the type byte is unprotected, and one
consequence is characterised and tested: flipping the type byte of a batch-begin
marker for a transaction that was *in flight when the process died* can cause
that uncommitted batch's records to be applied.

Bounded even there:

- It requires corruption at one specific byte.
- A **committed** batch fails closed — the orphaned commit marker is rejected.
- Only the batch in flight at crash time can fail open.
- Against a deliberate edit, CRC32 was never a defence anyway.

**A pre-existing log keeps its original framing until the store is compacted**,
because changing what its checksums cover would invalidate every record already
in it. Compaction rewrites the log and adopts the stronger framing, so the
migration needs no explicit step — but a store that has never been compacted
since upgrading still carries the older rule. `graphene wal <dir>` reports which
framing a log uses.

---

## 3. What is not implemented

Do not assume any of the following. None of it exists:

- **No encryption.** Data at rest is not encrypted. Property blobs are stored as
  you supply them. Use filesystem or volume encryption if you need it.
- **No access control.** There is no RBAC, no permissions, no principals. Every
  caller has every capability. `TxContext.RoleID` is *recorded* for later audit
  reconstruction; **nothing checks it**.
- **The audit log records operator actions, not reads.** With `Audit` enabled,
  compactions, key rotations and retention deletions are hash-chained into
  `graphene.audit`, and callers can add their own entries. Reads and queries are
  **not** recorded — that would put a synchronous append on the query path, and
  a caller needing it should record what matters via the API. Removing an entry
  from the middle is detectable; deleting the whole file is not, without an
  external anchor — see §5.
- **Key history depends on retention being switched on.** Rotations *are*
  recorded (see §4), but in the WAL, which a compaction truncates. With
  `Options.Retention` set, retired segments survive and so do the rotations in
  them; with the zero value — the default — every rotation before the last
  compaction is gone.
- **No anchoring transport.** §5's checkpoints define what to publish and how to
  check it, and the engine ships nowhere to publish *to*. Supplying an anchor is
  the deployment's job, and until one exists every guarantee in this document is
  the store vouching for itself.
- **`DeleteNode` is still unattributed.** It cascades and tombstones, records
  nothing about who or why, and after a compaction leaves no trace at all. It is
  unchanged and still the default. `RedactNode` (§6) is the attributed form, and
  a store that has not set `Options.Redaction` has only the unattributed one.
- **No history.** The engine stores current state. Superseded versions survive
  only until the next compaction truncates the log.
- **No network exposure**, and therefore no transport security to configure.

---

## 4. Using the integrity machinery

All of it is opt-in. A default store writes unsigned commits and does not verify
anything, which is the historical behaviour.

> Every flow in this section is executed by `graphene_security_doc_test.go`,
> including the forgery in "The part that makes verification mean anything". If
> the API moves underneath this document the tests break rather than the
> document quietly becoming wrong.

### Signing and verifying the log

The safe posture is one call:

```go
key, pub, err := signing.GenerateKey(1)   // or implement store.Signer yourself
if err != nil { return err }

ring := signing.NewKeyring()
if err := ring.Add(1, pub); err != nil { return err }

s, err := disk.OpenWithOptions(dir, disk.StrictOptions(key, ring, actorID))
```

`StrictOptions` sets every protection the engine offers: commits signed,
**unsigned commits refused**, the image verified before it is loaded, and
operator actions recorded in the audit log. It exists because those were
separate settings a caller had to discover individually, and a security control
nobody can find is a security control nobody uses.

Spelled out, it is:

```go
disk.Options{
    Signer:               key,
    Verifier:             ring,
    RequireSignedCommits: true,   // see below — this flag is the point
    VerifyOnOpen:         true,
    Audit:                true,
    AttestActorID:        actorID,
}
```

It does **not** set `Retention`, so compaction still discards the log. How long
evidence is kept is a decision the engine cannot make for you — see §4's
retention note.

### Why the default is permissive

`disk.Open` does **not** check the digest, the Merkle roots, or the attestation
signature. That is a deliberate compatibility choice rather than a
recommendation.

It is not, however, "no checking at all", and the distinction matters if you are
reasoning about what a default store catches. Structural validation always runs
at load and cannot be switched off: header and section bounds, record counts
against the file's own length, entity IDs against what the file can address, a
critical section this build does not understand, a snapshot root that does not
follow from its components, and an attestation naming a snapshot other than the
one in the same file. Those reject a malformed or internally inconsistent file
regardless of settings.

What the default misses is an edit to the **records themselves** in a file that
remains internally consistent — which is precisely what a deliberate edit looks
like. That is the gap `VerifyOnOpen` closes.

Two reasons the default is where it is:

- **An existing store keeps opening.** Turning verification on by default would
  reject every image written before the machinery existed, and every store whose
  commits predate signing.
- **Image verification costs time proportional to the file, on every startup.**
  That is the same reasoning that keeps `Open` from running `VerifyIndexes`: a
  check that makes every startup slower gets switched off, and a switched-off
  check protects nothing.

`VerifyOnOpen` is the right setting where opening is rare relative to querying,
which is the ingest-once/query-many shape this engine is built for. **If you are
storing evidence, turn it on.**

Log signature verification is not covered by that flag and always runs when a
`Verifier` is set, because it rides on the replay `Open` performs anyway.

**`RequireSignedCommits` is not optional if you care about signing.** Without
it, stripping a signature turns a signed commit into an ordinary unsigned one
and verification is simply skipped — an attacker with file access downgrades
every commit and nothing complains. It is separate from `Verifier` only because
a store predating your signing has genuinely unsigned commits that must still
replay until a compaction retires them.

### Rotating a signing key

```go
err := s.RotateKey(newKey, newPublicKey)
```

This writes a **key-transition record**, signed by the **outgoing** key, naming
the commit sequence from which the new key is authoritative.

The outgoing key signs because that is what makes the record evidence rather
than an assertion. Anyone can claim a key was replaced; only the holder of the
old key can produce a signature the old key verifies. A transition signed by the
incoming key would prove nothing — the incoming key is exactly what an attacker
substituting their own would hold.

Why this matters more than keeping both public keys around: if a key is later
found to be compromised, **without a transition record every commit it ever
signed becomes equally suspect**, including ones made long before the
compromise. Genuine evidence turns into apparent tampering, which is worse than
not rotating at all. With the record, a verifier can say "key 1 was
authoritative for commits 1 through 4,812" rather than "key 1 is or is not
trusted":

```go
tl := s.KeyTimeline()
if err := tl.VerifyChain(ring, firstKeyID); err != nil { /* a gap or a forgery */ }
who := tl.AuthoritativeAt(firstKeyID, someCommitSeq)
```

The **first** key cannot be introduced this way — it has no predecessor to sign
for it — so it is established out of band, in the keyring you supply. That is
the same trust root every signature here rests on.

`VerifyChain` distinguishes a **gap** (a transition missing from the log, which
after a compaction is expected) from a **forgery** (a transition the outgoing
key did not authorise). They mean different things and call for different
responses.

### Verifying a file

```
graphene csr -verify <dir>
```

Runs two independent checks. The **digest** asks whether the bytes match what
was written. The **roots** ask whether the Merkle roots describe the records
beside them — which still fails if an attacker edited a record and repaired the
digest. Defeating both requires recomputing the roots, which changes the
snapshot root.

Digest verification is **not** run at `Open`, because hashing the image on every
startup is a cost that gets the check disabled, and a disabled check protects
nothing. Signature verification *is* run at open, because it rides on the replay
that happens anyway.

### The part that makes verification mean anything

**Retain the snapshot root somewhere the attacker does not control.**

Every check above compares a file against values inside that same file. An
attacker who can edit the file can recompute all of them. What they cannot do is
change a value you wrote down elsewhere:

```go
roots, _ := s.SnapshotRoots()
// publish, co-sign, or store roots.Snapshot outside this system
```

Without an externally retained root, the machinery detects **damage and
accident**. With one, it detects **tampering**. That difference is the whole
point, and it is the caller's responsibility.

### Transferable claims

```go
claim, _ := s.AttestNode(id)                    // producer
err := disk.VerifyNodeAttestation(ring, claim)  // recipient, with only a public key
```

A verified claim means: *the holder of key K asserted, at time T, that a
snapshot with root R contained this entity.* It does not mean the entity's
contents are true, that T is accurate, or that K's holder was uncompromised.

### Handing a proof over

Proofs can leave the process as bytes, and be checked by a recipient with no
store, no image and no directory:

```go
blob, err := s.ExportNodeProof(id)                    // producer
proof, err := disk.UnmarshalProof(blob)               // recipient
err = disk.VerifyExportedProof(retainedRoot, proof)   // recipient
```

or from a shell, where the two halves are deliberately different commands:

```
graphene prove -node 7 -out claim.gprf <dir>
graphene verify-proof -root <hex> claim.gprf          # touches no store
```

**The root is never bundled with the proof, and there is no form of the API or
the command that lets it be.** A proof checked against a root carried inside it
proves nothing, because whoever wrote the file chose both. The root has to come
from somewhere its author does not control — a published checkpoint (§5), a
co-signed attestation, or a value written down when the evidence was collected.

What travels is the leaf, the sibling hashes and the component roots — never the
image, and never any other entity. That is what makes handing a proof over safe
where handing over the file would not be. A redaction proof carries less still:
a tombstone names an entity and a digest, never an actor or a reason.

A proof file is **not** signed. Its integrity does not depend on that: every
field a verifier relies on is bound into the root it is checked against, so a
single altered byte either fails to parse or fails to verify. Signing it would
say who sent it, which is a different question from whether it is true.

---

## 5. Anchoring: the only check that is not the store's own word

Everything in §4 compares the store against the store. That catches corruption
and it catches an outsider. It does not catch someone who holds the signing key
and can rewrite every chain consistently — the result verifies perfectly, because
there is nothing left to disagree with it.

An **anchor** is somewhere outside the store where a digest is published and
cannot afterwards be changed by whoever controls the store. That property comes
entirely from where the anchor lives and who runs it. Nothing in this process can
establish it, which is why the engine defines the interface and ships no
transport.

### What is published

A `Checkpoint` binds all four histories at one instant — snapshot root,
attestation, WAL segment head, audit head, plus the counts of each:

```go
c, rec, err := s.PublishCheckpoint(anchor)   // captures, publishes, records
```

Binding all four matters. Publishing only the snapshot root leaves the other
three free: an adversary who rewrites the audit log alone changes no snapshot
root and passes that check. A checkpoint does not offer the choice.

A zero head is committed to as a zero head, so "there was no audit log" cannot
later be retrofitted into "here is the audit log".

### What is checked

```go
report, err := s.VerifyAgainstAnchor(anchor)
custody, err := s.CustodyForAnchor(id, verifier, anchor)   // §4's custody, anchored
```

The check runs in both directions, and both directions are load-bearing:

- **A local checkpoint the anchor never witnessed** means the local chain was
  rewritten. An adversary can forge a self-consistent chain; they cannot forge a
  publication that already happened.
- **A published digest with no local checkpoint** means the local record was
  destroyed. This is reported as **broken**, never as "unanchored" — deleting a
  file is easier than forging a chain, and a scheme where destroying the evidence
  produces the innocent verdict is not a scheme.

`VerifyCheckpointChain` additionally checks that the local chain links up. That
is a weak check by design: an adversary who recomputes it forward passes. It is
there for when the anchor is unreachable, not as a substitute for one.

### What an anchor does not prove

Only that a digest existed at a time. Specifically:

- **It says nothing about the unanchored tail.** Everything since the last
  publication is still freely rewritable. `AnchorAudit` reports that window
  rather than leaving you to infer it, and the guarantee is bounded by how often
  you publish — not by anything the engine does.
- **A store that moved on is not a store that was tampered with.** Ordinary
  progress makes the live state differ from the last witnessed checkpoint. That
  is reported as a finding, not a break. History that *shrank* is a break.
- **It does not make the contents true.** Same limit as every signature here.

### The one implementation, and why you must not use it

`disk.InsecureLocalAnchor` writes digests to a file on the same machine. **It is
not an anchor** — anyone who can rewrite the store can rewrite it. It exists so
the verification path can be tested and demonstrated without the engine
acquiring a network dependency. Its constructor refuses a path inside the store
directory, which is a tripwire against the most likely misuse and not a security
property.

A real deployment implements `disk.Anchor` against a timestamp authority, a
transparency log, another organisation's storage, or a printed sheet in a safe.
The engine does not care which; it cares that whoever can rewrite the store
cannot rewrite that.

---

## 6. Redaction: removing content without removing the record

`DeleteNode` destroys an entity and says nothing about it. After a compaction
there is no trace it ever existed — which means **lawful redaction and evidence
destruction look identical**. An engine holding evidence has to be able to comply
with an erasure order without that compliance being indistinguishable from
tampering.

`RedactNode` is the attributed form. It requires `Options.Redaction`, and refuses
rather than quietly falling back to a plain delete.

```go
opts := disk.StrictOptions(key, ring, actorID)
opts.Redaction = true
opts.RedactionPolicy = disk.RedactionPolicy{MaxCascade: 50}

impact, err := s.RedactionImpactFor(id)     // what would go, before anything goes
rec, err := s.RedactNode(id, disk.RedactionRequest{
    ActorID: 7, RoleID: 3, Reason: "subject access request 41",
})
```

### What survives

| Kept | Why it matters |
| --- | --- |
| the **fact** | a record exists that a redaction happened |
| the **actor**, role and time | the removal is attributable |
| the **reason** | required; an unexplained redaction is indistinguishable from destruction |
| the **shape** | which entity, and every edge cascaded with it |
| the **version hash** | the identity of what was destroyed |

The version hash is the load-bearing part, and it is deliberately **the same
value as that entity's Merkle leaf in a snapshot**. A party holding the redacted
content can prove it is what was removed; a party holding only the ledger can
prove something specific was removed without learning what it was.

### Three scopes, because they are three different decisions

```go
s.RedactNode(id, req)               // the entity, and every edge touching it
s.RedactNodeProperties(id, req)     // the property blob; ID, labels and edges stay
s.RedactEdge(edgeID, req)           // one relationship, both endpoints kept
s.RedactEdgeProperties(edgeID, req) // an edge's properties; the relationship stays
```

**`RedactNodeProperties` is the lawful-erasure case and should usually be the
one you want.** An erasure request is almost always about personal data, not
about the existence of the artefact that carried it. Removing the whole node
destroys evidence that was never in scope — including every relationship that
made the artefact meaningful — so the narrow form keeps the graph's shape and
the entity's availability as a subject of provenance.

Its record keeps the version hash the node had *before* and the one it has
*after*, so a recipient can confirm the entity now in the image is the one the
record describes rather than taking it on trust.

**A property redaction is provable without revealing what was removed:**

```go
proof, err := s.ProvePropertyRedaction(id)
err = disk.VerifyPropertyRedaction(retainedRoot, proof)   // recipient's side
```

That check establishes something a bare version-hash comparison cannot — that
the entity's **identity did not change**, only its properties. Entity leaves
commit to `SHA256(0x07 ‖ Properties)` rather than to the blob itself, so the
leaf before and the leaf after are byte-identical except in their final 32
bytes. A verifier sees that equality directly. Nothing in the proof carries the
removed content; the strongest thing it holds is a digest of it, which a party
who already has the blob can use to confirm it is what was destroyed and a party
who does not learns nothing from.

**Property redaction purges the property index unconditionally**, regardless of
`ReindexPolicy`. The index holds the values themselves; leaving its entries
behind would keep redacted content queryable, which would defeat the operation
entirely. This is the one place that policy does not get a say.

**A cascade is tombstoned edge by edge.** `RedactNode` records the version hash
of every edge it takes with it, and the image gets a tombstone for each — so an
edge removed as collateral is provable in the same way as one removed
deliberately. Without that, an auditor asking "why is there no edge between A
and B" would get nothing.

### The rules the engine enforces

- **A reason is mandatory.** `ErrRedactionUnexplained`.
- **The cascade is capped if you cap it.** `RedactionPolicy.MaxCascade` bounds how
  much one redaction may take with it — the hub-node problem. The zero value is
  unbounded; how much removal is too much is a legal question, not an
  engineering one.
- **The record is written before the deletion.** A record with no matching
  deletion is explicable; a deletion with no record is the exact hole this
  exists to prevent.
- **The ledger is not in the WAL.** Compaction truncates the WAL, so a deletion
  record there would be destroyed by the operation that makes the deletion
  permanent. `graphene.redactions` is its own append-only, hash-chained file that
  compaction never touches.
- **Its head is bound into the checkpoint** (§5), so deleting the ledger wholesale
  is externally detectable rather than silent.

### Proving a removal to someone holding only the image

The ledger persuades whoever holds the store. It does nothing for the party who
matters most in an evidentiary exchange: **someone handed a single compacted
image and nothing else.** To them a redacted entity is simply not there,
indistinguishable from one that never existed.

So the image carries its own record. Each compaction writes a `GRDT` section of
**tombstones** — entity ID, version hash, and which ledger record it came from —
Merkle-rooted, with that root bound into the snapshot root:

```go
proof, err := s.ProveRedaction(id)
err = disk.VerifyRedactionInclusion(retainedRoot, proof)   // recipient's side
```

That is the same shape as `ProveNode`, for an absence instead of a presence. A
snapshot root retained outside the system therefore commits to **what was taken
out of the image as well as what is in it** — so a removal cannot be erased from
the record without changing the root someone already wrote down.

Three properties worth stating:

- **A tombstone carries no circumstances.** No content, no reason, no actor —
  those stay in the ledger. The image is the artefact most likely to be handed to
  someone who should not also receive an operator's name and a case reference.
  The tombstone's record hash says *which* ledger entry to ask for, and lets the
  recipient check they were given the right one.
- **Tombstones are rebuilt from the ledger at each compaction**, never carried
  forward. A second copy could drift, and an image and ledger that disagree about
  what was destroyed while each verifies perfectly is worse than neither.
- **Snapshot roots are versioned, because adding to them changes what they are.**
  v1 binds four components; v2 adds the tombstone root; v3 additionally changes
  entity leaves to commit to a property *hash* rather than the blob. A root
  retained from an older image stays verifiable — it commits to less, which is
  not the same as being wrong — and every proof and recomputation uses the
  encoding the image was written with, not the one this build prefers.
  `VerifyRedactionInclusion` refuses a v1 root outright rather than checking a
  proof against a value that root never covered, and
  `ProvePropertyRedaction` refuses a pre-v3 image rather than pretending a
  content-free proof is available where the leaves inline the content.

### What it does not do

- **It does not undo.** The content is gone. This is crypto-erasure's *shape*,
  not key-wrapped reversibility.
- **A removal is only provable once compacted.** Between `RedactNode` and the
  next `Compact` the ledger knows and the image does not; `CustodyReport`
  reports that window as `RemovalProvable: false` rather than leaving it implicit.
- **`RoleID` is recorded, never checked.** There is still no role model (§3).

---

## 7. Residual risks

Stated plainly, because they do not go away:

| Risk | Status |
| --- | --- |
| An attacker with **code execution in your process** | **Unmitigable in-engine.** They hold the key and can call any API. Only external anchoring (§5) and organisational controls apply |
| **Rollback of the entire store** to an earlier valid state | Undetectable without an externally retained root. Every internal value is consistent in the older state. §5 is the control; it is only as good as your publishing interval |
| **Everything since the last checkpoint** | Freely rewritable even with anchoring configured. The window is set by how often you publish, not by the engine |
| **Backdated timestamps** | Local clock readings are asserted, not proven. An external time authority (RFC 3161) is what would fix this; none is integrated |
| **Poisoned ingest** | Signatures prove *who asserted* something, never *whether it is true*. Garbage in, signed garbage out |
| **Stale property index** | `UpdateNode` leaves index entries pointing at superseded values, and the planner trusts them. Use `UpdateNodeIndexed`. The engine cannot detect this — property blobs are opaque to it |
| **Unsigned compaction** on a store without a signer | Produces integrity evidence without authorship. Configure a `Signer` if you need both |

---

## 8. Reporting a vulnerability

**When in doubt, report privately.** Private is the default this policy assumes,
and no report is ever criticised for having been sent that way.

### Where to send it

Use GitHub's **[private vulnerability
reporting](https://github.com/aoiflux/graphene/security/advisories/new)** on this
repository. That keeps the report and the fix under embargo until an advisory is
published.

If that is unavailable to you, open a public issue containing only *"security
report, requesting a private channel"* and no detail, and wait to be contacted.

### What to include

- Affected version, tag, or commit.
- What an attacker gains — the impact matters more than the mechanism.
- A reproducer. A failing input for one of the fuzz targets is ideal:
  `FuzzDeserialiseCSR`, `FuzzWALReplay`, `FuzzParseNodeType`, `FuzzParseEdgeType`,
  `FuzzProofSoundness`, `FuzzRootDistinguishesContent`. Committing the file under
  `testdata/fuzz/<Target>/` turns it into a permanent regression test, which is
  how every parser defect found so far has been fixed.

### Two tiers, and why

Treating parser robustness as a security concern (§9) and requiring private
disclosure pull against each other: applied strictly, every fuzz-found panic
becomes an embargoed report, which slows fixes for bugs that benefit from public
corpora and outside eyes. So the routing depends on impact, not on category:

| Tier | Examples | Route |
| --- | --- | --- |
| **Integrity** | verification that passes when it should fail; silent acceptance of tampered data; signature or proof bypass; a way to produce a colliding root or forge an attestation | **Private.** Always |
| **Availability** | a parser panic, an unbounded allocation, or a hang on a malformed file, with no path to accepting bad data as good | Private is welcome and never wrong. A public issue is also fine, and often faster |

The dividing question is simple: **could this make a reader trust something it
should not?** If yes, or if you are unsure, it is Integrity and it goes
privately.

Parser robustness is in scope at all — even where the impact is only a crash —
because the files being parsed are evidence, and a tool that crashes on the
evidence it was handed is a tool that cannot be used at the moment it is needed.

---

## 9. Scope

**In scope**

- Parser robustness against malformed or hostile files.
- Silent acceptance of tampered data.
- Integrity checks that pass when they should fail.
- Signature, inclusion-proof, or attestation verification flaws.
- Unbounded resource consumption driven by file contents.
- Any documented guarantee in this file that does not hold. **If §2, §4, §5 or §6
  claims something the code does not do, that is a security issue** — an
  overstated guarantee is believed exactly when it matters.

**Out of scope**

- Anything requiring code execution in the host process. That is inside the
  trust boundary by design (§1), not a defect.
- Key management. The engine holds no keys.
- The absent features listed in §3. Those are documented gaps, not
  vulnerabilities — a report that Graphene has no access control is a feature
  request.
- Performance regressions with no integrity or availability impact.
