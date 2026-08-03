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
- **No audit log.** Reads, queries, exports, and configuration changes are not
  recorded. Only mutations reach the WAL, and only batched ones carry
  provenance.
- **No durable key history.** Rotations *are* recorded (see §4), but in the WAL,
  which a compaction truncates. Rotations from before the last compaction are
  gone, so a full key history needs log retention, which is not built.
- **No redaction primitive.** `DeleteNode` cascades and tombstones; after a
  compaction the entity is unrecoverable and the deletion is unattributed.
  Lawful redaction and evidence destruction are currently the same operation.
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
**unsigned commits refused**, and the image verified before it is loaded. It
exists because those were three settings a caller had to discover individually,
and a security control nobody can find is a security control nobody uses.

Spelled out, it is:

```go
disk.Options{
    Signer:               key,
    Verifier:             ring,
    RequireSignedCommits: true,   // see below — this flag is the point
    VerifyOnOpen:         true,
    AttestActorID:        actorID,
}
```

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

---

## 5. Residual risks

Stated plainly, because they do not go away:

| Risk | Status |
| --- | --- |
| An attacker with **code execution in your process** | **Unmitigable in-engine.** They hold the key and can call any API. Only external anchoring and organisational controls apply |
| **Rollback of the entire store** to an earlier valid state | Undetectable without an externally retained root. Every internal value is consistent in the older state |
| **Backdated timestamps** | Local clock readings are asserted, not proven. An external time authority (RFC 3161) is what would fix this; none is integrated |
| **Poisoned ingest** | Signatures prove *who asserted* something, never *whether it is true*. Garbage in, signed garbage out |
| **Stale property index** | `UpdateNode` leaves index entries pointing at superseded values, and the planner trusts them. Use `UpdateNodeIndexed`. The engine cannot detect this — property blobs are opaque to it |
| **Unsigned compaction** on a store without a signer | Produces integrity evidence without authorship. Configure a `Signer` if you need both |

---

## 6. Reporting a vulnerability

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

Treating parser robustness as a security concern (§7) and requiring private
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

## 7. Scope

**In scope**

- Parser robustness against malformed or hostile files.
- Silent acceptance of tampered data.
- Integrity checks that pass when they should fail.
- Signature, inclusion-proof, or attestation verification flaws.
- Unbounded resource consumption driven by file contents.
- Any documented guarantee in this file that does not hold. **If §2 or §4 claims
  something the code does not do, that is a security issue** — an overstated
  guarantee is believed exactly when it matters.

**Out of scope**

- Anything requiring code execution in the host process. That is inside the
  trust boundary by design (§1), not a defect.
- Key management. The engine holds no keys.
- The absent features listed in §3. Those are documented gaps, not
  vulnerabilities — a report that Graphene has no access control is a feature
  request.
- Performance regressions with no integrity or availability impact.
