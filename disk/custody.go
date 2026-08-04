package disk

// Chain of custody: one answer, assembled from four separate histories.
//
// By this point the engine keeps four hash-linked records, and each was built
// to answer a different question:
//
//	snapshot roots   what the compacted image contains, and what it replaced
//	attestations     who asserted that image, and when
//	WAL segments     the commits that produced it, across past compactions
//	audit entries    what was done to the store, including deletions
//
// Every one of them verifies on its own. None of them, on its own, answers the
// question an investigation actually asks: *can this artefact's presence be
// accounted for, end to end, with nothing missing?*
//
// That question is the plan's PO-5, and answering it is synthesis rather than
// mechanism — no new format, no new record type, just walking what is already
// there and being precise about where it stops.
//
// # Gaps, not a boolean
//
// A custody report names what is missing rather than returning a verdict. "Not
// verified" is useless to someone holding evidence; "the segment chain is intact
// but nothing attested the current snapshot" tells them what to do next. Each
// gap says which layer it came from and what it means.
//
// # What it cannot tell you
//
// Every check here compares the store against itself. A report can therefore say
// the store is internally consistent and complete, and still be describing a
// store that was rewritten wholesale by someone holding the key. Only a root
// retained outside the system closes that, and the report says so rather than
// letting completeness be mistaken for proof.

import (
	"fmt"
	"time"

	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// CustodyLayer names which history a gap came from.
type CustodyLayer string

const (
	LayerSnapshot    CustodyLayer = "snapshot"
	LayerAttestation CustodyLayer = "attestation"
	LayerSegments    CustodyLayer = "segments"
	LayerAudit       CustodyLayer = "audit"
	LayerRedaction   CustodyLayer = "redaction"
	LayerExternal    CustodyLayer = "external"
)

// CustodyGap is one thing that could not be accounted for.
type CustodyGap struct {
	Layer CustodyLayer

	// Detail says what is missing in terms the reader can act on.
	Detail string

	// Fatal distinguishes "this chain is broken" from "this chain was never
	// established". A store that was never signed has no attestation gap worth
	// alarm; a store whose attestation does not verify has one.
	Fatal bool
}

func (g CustodyGap) String() string {
	severity := "incomplete"
	if g.Fatal {
		severity = "BROKEN"
	}
	return fmt.Sprintf("[%s/%s] %s", g.Layer, severity, g.Detail)
}

// CustodyReport is the assembled account for one entity.
type CustodyReport struct {
	NodeID store.NodeID

	// Live reports whether the store knows the entity at all, and InSnapshot
	// whether it is in the compacted image.
	//
	// The two are separate on purpose. An entity written since the last
	// compaction is live but unaccounted for; an entity the store has never
	// heard of is absent. Both fail an inclusion proof identically, and
	// reporting them the same way would have the report assert the entity
	// exists on the strength of the caller having named it.
	Live       bool
	InSnapshot bool

	// SnapshotRoot is the image the entity was found in, and Inclusion proves
	// it was there. Zero when InSnapshot is false.
	SnapshotRoot merkle.Hash
	Inclusion    *NodeInclusionProof

	// Attested and AttestationVerified describe the signed assertion over that
	// snapshot, if any.
	Attested            bool
	AttestationVerified bool
	AttestActorID       uint64

	// Counts of what was walked, so a reader can tell "no gaps" from "nothing
	// was there to check".
	SegmentsChecked     int
	AuditEntriesWalked  int
	CompactionsRecorded int
	RedactionsWalked    int

	// Redacted is the ledger record for this entity, when one exists. Non-nil
	// turns an absence from an unexplained hole into a documented removal — the
	// distinction between lawful redaction and evidence destruction.
	Redacted *RedactionRecord

	// RemovalProvable reports whether the compacted image itself records the
	// removal, under the snapshot root, rather than only the ledger doing so.
	//
	// The distinction is who can be convinced. A ledger entry persuades someone
	// holding the store; a tombstone under a retained root persuades someone
	// holding nothing but the image.
	RemovalProvable bool

	// Gaps is everything that could not be accounted for.
	Gaps []CustodyGap
}

// Complete reports whether every layer was established and unbroken.
//
// False whenever any gap exists, including non-fatal ones — an unattested
// snapshot is not a broken chain, but it is not a complete custody record
// either, and conflating the two is how a partial account gets presented as a
// whole one.
func (r CustodyReport) Complete() bool { return len(r.Gaps) == 0 }

// Broken reports whether any chain actively failed, as opposed to never having
// been established.
func (r CustodyReport) Broken() bool {
	for _, g := range r.Gaps {
		if g.Fatal {
			return true
		}
	}
	return false
}

// Summary is a one-line verdict for a log or a CLI.
func (r CustodyReport) Summary() string {
	switch {
	case r.Broken():
		return fmt.Sprintf("node %d: custody BROKEN (%d gaps)", r.NodeID, len(r.Gaps))
	case r.Redacted != nil && !r.Live:
		// **Before the unknown-entity branch.** A redacted entity is also absent,
		// and reporting it as unknown would describe a documented removal in the
		// same words as a question about something that never existed.
		return fmt.Sprintf("node %d: redacted at %s by actor %d — %q",
			r.NodeID, time.Unix(0, r.Redacted.UnixNano).UTC().Format(time.RFC3339),
			r.Redacted.ActorID, r.Redacted.Reason)
	case !r.Live:
		// Said before the gap count, because "3 gaps" on an entity the store
		// never held describes the store, not the entity, and a reader chasing
		// one artefact will take it for an answer about that artefact.
		return fmt.Sprintf("node %d: unknown to this store (%d gaps in the store's own record)",
			r.NodeID, len(r.Gaps))
	case !r.Complete():
		return fmt.Sprintf("node %d: custody incomplete (%d gaps)", r.NodeID, len(r.Gaps))
	default:
		return fmt.Sprintf("node %d: custody accounted for across %d segments and %d audit entries",
			r.NodeID, r.SegmentsChecked, r.AuditEntriesWalked)
	}
}

// CustodyFor assembles the custody account for one entity.
//
// verifier may be nil, in which case signature-dependent layers are reported as
// unestablished rather than failing — a caller without the public key can still
// learn whether the hash chains hold.
func (s *Store) CustodyFor(id store.NodeID, verifier store.Verifier) (CustodyReport, error) {
	r := CustodyReport{NodeID: id, Live: s.NodeExists(id)}

	// --- the snapshot, and the entity's place in it ---
	proof, err := s.ProveNode(id)
	switch {
	case err == nil:
		r.InSnapshot = true
		r.Inclusion = &proof
		r.SnapshotRoot = proof.Roots.Snapshot

		if verr := VerifyNodeInclusion(proof.Roots.Snapshot, proof); verr != nil {
			r.Gaps = append(r.Gaps, CustodyGap{
				Layer: LayerSnapshot, Fatal: true,
				Detail: fmt.Sprintf("the inclusion proof does not resolve against the snapshot root: %v", verr),
			})
		}
	case errorIs(err, ErrNotInSnapshot):
		detail := "the entity is live but not in the compacted image; compact to bring it under custody"
		if !r.Live {
			// Not a gap in the store's account of itself — a gap in the
			// question. Reporting it as a snapshot gap anyway keeps the caller
			// from reading a short gap list as reassurance.
			detail = "the store has no such entity; nothing was found to account for"
		}
		r.Gaps = append(r.Gaps, CustodyGap{Layer: LayerSnapshot, Detail: detail})
	case errorIs(err, ErrNoSnapshotRoots):
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer:  LayerSnapshot,
			Detail: "the store has no snapshot roots; it has never been compacted, or predates them",
		})
	default:
		return r, err
	}

	// --- who asserted that snapshot ---
	att, aerr := s.SnapshotAttestation()
	switch {
	case aerr == nil:
		r.Attested = true
		r.AttestActorID = att.ActorID
		if verifier == nil {
			r.Gaps = append(r.Gaps, CustodyGap{
				Layer:  LayerAttestation,
				Detail: "the snapshot is attested but no verifier was supplied, so the signature was not checked",
			})
		} else if verr := VerifyAttestation(verifier, att); verr != nil {
			r.Gaps = append(r.Gaps, CustodyGap{
				Layer: LayerAttestation, Fatal: true,
				Detail: fmt.Sprintf("the snapshot's attestation does not verify: %v", verr),
			})
		} else {
			r.AttestationVerified = true
			// An attestation is only about *its* snapshot. If it names a
			// different one than the entity was proved into, it vouches for
			// something else.
			if r.InSnapshot && att.Subject != r.SnapshotRoot {
				r.Gaps = append(r.Gaps, CustodyGap{
					Layer: LayerAttestation, Fatal: true,
					Detail: "the attestation names a different snapshot than the entity was proved into",
				})
			}
		}
	default:
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer:  LayerAttestation,
			Detail: "the snapshot carries no attestation, so nothing records who produced it",
		})
	}

	// --- the commits that produced it, across past compactions ---
	segs, serr := ListSegments(s.dir)
	if serr != nil {
		return r, serr
	}
	r.SegmentsChecked = len(segs)
	if len(segs) == 0 {
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer: LayerSegments,
			Detail: "no retired WAL segments; the commit history of past compactions was discarded. " +
				"Set Options.Retention to keep it",
		})
	} else if cerr := VerifySegmentChain(segs); cerr != nil {
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer: LayerSegments, Fatal: true,
			Detail: fmt.Sprintf("the segment chain is broken: %v", cerr),
		})
	}

	// --- what was done to the store ---
	entries, eerr := ReadAuditLog(s.dir)
	if eerr != nil {
		return r, eerr
	}
	r.AuditEntriesWalked = len(entries)
	for _, e := range entries {
		if e.Kind == AuditCompact {
			r.CompactionsRecorded++
		}
	}
	switch {
	case len(entries) == 0:
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer:  LayerAudit,
			Detail: "no audit log; operator actions are unrecorded. Set Options.Audit to record them",
		})
	default:
		if verr := VerifyAuditChain(entries); verr != nil {
			r.Gaps = append(r.Gaps, CustodyGap{
				Layer: LayerAudit, Fatal: true,
				Detail: fmt.Sprintf("the audit chain is broken: %v", verr),
			})
		}
		// Every compaction should have left a record. Fewer audit entries than
		// retired segments means a compaction happened without being recorded,
		// which is exactly the gap the audit log exists to expose.
		if r.CompactionsRecorded < len(segs) {
			r.Gaps = append(r.Gaps, CustodyGap{
				Layer: LayerAudit, Fatal: true,
				Detail: fmt.Sprintf("%d retired segments but only %d recorded compactions; "+
					"a compaction is missing from the audit log", len(segs), r.CompactionsRecorded),
			})
		}
	}

	// --- whether this entity was destroyed on purpose ---
	//
	// **The one layer that can turn "missing" into "accounted for".** An entity
	// absent from the snapshot is a gap everywhere else in this report; a
	// redaction record turns it into a documented removal with an actor, a time,
	// a reason and the version hash of what went. That is the whole distinction
	// between lawful redaction and evidence destruction, so it is checked before
	// the report concludes anything about absence.
	reds, rderr := ReadRedactions(s.dir)
	if rderr != nil {
		return r, rderr
	}
	r.RedactionsWalked = len(reds)
	if len(reds) > 0 {
		if verr := VerifyRedactionChain(reds, verifier); verr != nil {
			r.Gaps = append(r.Gaps, CustodyGap{
				Layer: LayerRedaction, Fatal: true,
				Detail: fmt.Sprintf("the redaction ledger is broken: %v", verr),
			})
		}
		for i := range reds {
			if reds[i].NodeID == id {
				rec := reds[i]
				r.Redacted = &rec
				break
			}
		}
	}

	// Whether the *image* records the removal, or only the ledger does. The
	// difference matters to anyone who will be handed the image alone: a ledger
	// entry is the store's word, a tombstone under the snapshot root is evidence.
	if r.Redacted != nil {
		if _, perr := s.ProveRedaction(id); perr == nil {
			r.RemovalProvable = true
		} else if !errorIs(perr, ErrNoTombstone) && !errorIs(perr, ErrNoSnapshotRoots) {
			return r, perr
		} else {
			r.Gaps = append(r.Gaps, CustodyGap{
				Layer: LayerRedaction,
				Detail: "the removal is in the ledger but not in the compacted image; " +
					"compact to bind it into the snapshot root, or a recipient given only the " +
					"image cannot tell this entity from one that never existed",
			})
		}
	}

	// A redaction explains an absence. Report it as such rather than leaving the
	// snapshot gap to read as an unexplained hole.
	if r.Redacted != nil && !r.InSnapshot {
		// The snapshot layer ran before the ledger was read and concluded the
		// store had never heard of this entity. It had; the entity was removed on
		// purpose. Leaving both lines in place would have the report contradict
		// itself in consecutive sentences, so the earlier, now-wrong one is
		// corrected rather than merely outvoted by the one below it.
		for i := range r.Gaps {
			if r.Gaps[i].Layer == LayerSnapshot && !r.Gaps[i].Fatal {
				r.Gaps[i].Detail = "the entity is absent from the compacted image because it was " +
					"redacted; see the redaction layer below"
			}
		}
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer: LayerRedaction,
			Detail: fmt.Sprintf("the entity was redacted at %s by actor %d (role %d): %q. "+
				"Its content is gone; the fact, actor, time and version hash are not",
				time.Unix(0, r.Redacted.UnixNano).UTC().Format(time.RFC3339),
				r.Redacted.ActorID, r.Redacted.RoleID, r.Redacted.Reason),
		})
	}

	// --- the limit no internal check can pass ---
	//
	// Reported as a gap on purpose. Every check above compares the store against
	// itself, so a report with no other gaps still describes a store that could
	// have been rewritten wholesale. Leaving this implicit would let a clean
	// report be read as proof it is not.
	r.Gaps = append(r.Gaps, CustodyGap{
		Layer: LayerExternal,
		Detail: "no externally retained root was supplied, so every check above compares the store " +
			"against itself. Use CustodyForAnchored to close this",
	})

	return r, nil
}

// CustodyForAnchored is CustodyFor, additionally checking the snapshot against a
// root the caller retained outside the system.
//
// This is the only form that can distinguish "internally consistent" from "not
// tampered with", because it is the only one comparing the store against
// something an attacker with full access could not change.
func (s *Store) CustodyForAnchored(id store.NodeID, verifier store.Verifier, anchor merkle.Hash) (CustodyReport, error) {
	r, err := s.CustodyFor(id, verifier)
	if err != nil {
		return r, err
	}

	// Drop the placeholder gap; an anchor was supplied.
	kept := r.Gaps[:0]
	for _, g := range r.Gaps {
		if g.Layer != LayerExternal {
			kept = append(kept, g)
		}
	}
	r.Gaps = kept

	switch {
	case !r.InSnapshot:
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer:  LayerExternal,
			Detail: "the entity is not in a snapshot, so it cannot be checked against the anchor",
		})
	case r.SnapshotRoot != anchor:
		r.Gaps = append(r.Gaps, CustodyGap{
			Layer: LayerExternal, Fatal: true,
			Detail: fmt.Sprintf("the store's snapshot root is %x but the retained anchor is %x — "+
				"the image is not the one that was recorded", r.SnapshotRoot[:8], anchor[:8]),
		})
	}
	return r, nil
}

// errorIs is errors.Is, kept local so this file's imports stay minimal.
func errorIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
