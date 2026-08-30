package main

// The assertion group: add, list, verify.
//
// The brief asked for an `assertion` group. There is no Assertion type in this
// engine, and inventing one for the CLI would have created a concept the
// library does not have and cannot verify. What does exist is the hash-chained
// audit log — an append-only record of who did what, in the operator's own
// words — and the signed attestation over the image. `assertion` is those two,
// named so that somebody looking for the concept finds the mechanism.
//
// `assertion add` writes an audit entry, `assertion list` reads the chain, and
// `assertion verify` checks the chain and the attestation over it. None of them
// can alter or remove anything already recorded, which is why `add` is not
// behind -confirm.

import (
	"flag"
	"strconv"
	"strings"
	"time"

	"github.com/aoiflux/graphene/disk"
)

// --- assertion add ---

type assertAddOpts struct {
	kind   string
	actor  uint64
	detail string
}

var assertionAdd = cmd(Command{
	Group: "assertion", Name: "add", Usage: "<dir>",
	Short: "record an operator action in the audit chain",
	Long: "Append-only. The entry is hash-linked to the one before it, so it cannot\n" +
		"be edited or removed without breaking the chain — which is why this is\n" +
		"not behind -confirm: there is nothing here for a confirmation to protect.\n\n" +
		"The engine never interprets -detail. It is hashed into the chain and read\n" +
		"by people, so write it for whoever reads this log in a year without the\n" +
		"context you have now.\n\n" +
		"This needs a store opened with an audit ledger. A store built without one\n" +
		"has nowhere to put the entry and says so.",
	Notice: "assertion add opens the store for writing and takes the exclusive lock",
	Open:   OpenDiskRW, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *assertAddOpts) {
		fs.StringVar(&o.kind, "kind", "custom",
			"custom, or a number at or above 1000; the lower kinds are the engine's")
		fs.Uint64Var(&o.actor, "actor", 0, "the actor this action is attributed to (required)")
		fs.StringVar(&o.detail, "detail", "", "what happened, in your own words (required)")
	},
	runAssertionAdd)

func runAssertionAdd(cx *Context, o *assertAddOpts) (Result, error) {
	var r Result
	if o.actor == 0 {
		return r, Usagef("need -actor: an unattributed entry records that something " +
			"happened and not who did it, which is most of what an audit log is for")
	}
	if strings.TrimSpace(o.detail) == "" {
		return r, Usagef("need -detail: the entry is read by people, and an empty one " +
			"tells them nothing")
	}
	kind, err := parseAuditKind(o.kind)
	if err != nil {
		return r, err
	}
	if kind < disk.AuditCustom {
		return r, Usagef("-kind %s is the engine's to write: an audit log a caller "+
			"can write engine history into is not evidence of what the engine did. "+
			"Use custom, or a number at or above %d", kind, uint64(disk.AuditCustom))
	}

	if cx.Globals.DryRun {
		s := r.Section("would record")
		s.Add("kind", Str(kind.String()))
		s.Add("actor", Uint(o.actor))
		s.Add("detail", Str(o.detail))
		return r, nil
	}

	// Counted before, because RecordAudit on a store with no audit ledger
	// succeeds and writes nothing — the engine treats auditing as optional and
	// a disabled log as "nothing to record", which is right for a library and
	// silent in exactly the wrong way for a command whose only job is to record
	// something. Comparing the chain before and after is how this reports the
	// absence rather than a success that wrote nothing.
	before, _ := disk.ReadAuditLog(cx.Target)

	if err := cx.Disk().RecordAudit(kind, o.actor, o.detail); err != nil {
		return r, err
	}

	entries, err := disk.ReadAuditLog(cx.Target)
	if err != nil && !isAbsent(err) {
		return r, err
	}
	if len(entries) == len(before) {
		return r, Usagef("this store has no audit ledger, so there is nowhere to " +
			"record this and nothing was written. A store gets one at creation, " +
			"through Options.Audit")
	}

	s := r.Section("recorded")
	if n := len(entries); n > 0 {
		e := entries[n-1]
		s.Add("sequence", Uint(e.Seq))
		s.Add("at", Time(e.UnixNano))
		s.Add("kind", Str(e.Kind.String()))
		s.Add("actor", Uint(e.ActorID))
		s.Add("detail", Str(e.Detail))
		s.Add("hash", Hex(e.Hash[:]))
	}
	s.Add("entries in the chain", Int(int64(len(entries))))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- assertion list ---

type assertListOpts struct {
	kind  string
	actor uint64
	since string
	page  Paging
}

var assertionList = cmd(Command{
	Group: "assertion", Name: "list", Usage: "<dir>",
	Short: "the audit chain: who did what, and when",
	Long: "Reads the ledger file directly and takes no lock, so it answers against\n" +
		"a store another process is writing — which is when an audit log is most\n" +
		"often wanted.\n\n" +
		"The chain is verified over the whole ledger before any filter is applied.\n" +
		"A break anywhere makes every entry suspect, including the ones you asked\n" +
		"for, so attaching the verdict to a filtered subset would attach it to the\n" +
		"wrong set.",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *assertListOpts) {
		fs.StringVar(&o.kind, "kind", "", "show only this audit kind: "+auditKindList())
		fs.Uint64Var(&o.actor, "actor", 0, "show only entries attributed to this actor")
		fs.StringVar(&o.since, "since", "", "show only entries at or after this RFC 3339 instant")
		o.page.Bind(fs, 50, "entries to show (0 = all)")
	},
	runAssertionList)

func runAssertionList(cx *Context, o *assertListOpts) (Result, error) {
	var r Result

	var wantKind disk.AuditKind
	if o.kind != "" {
		k, err := parseAuditKind(o.kind)
		if err != nil {
			return r, err
		}
		wantKind = k
	}
	var since int64
	if o.since != "" {
		t, err := time.Parse(time.RFC3339, o.since)
		if err != nil {
			return r, Usagef("-since %q: want RFC 3339: %v", o.since, err)
		}
		since = t.UnixNano()
	}

	entries, err := disk.ReadAuditLog(cx.Target)
	if err != nil && !isAbsent(err) {
		return r, err
	}
	if len(entries) == 0 {
		r.Section("").Add("audit chain", Str("no entries"))
		r.Find(SevWarn, "assertion.no_audit",
			"no audit log; operator actions are unrecorded. Set Options.Audit to record them")
		return r, nil
	}

	// Over the whole ledger, before filtering. See the Long text.
	chainErr := disk.VerifyAuditChain(entries)

	kept := entries[:0:0]
	for _, e := range entries {
		if o.kind != "" && e.Kind != wantKind {
			continue
		}
		if o.actor != 0 && e.ActorID != o.actor {
			continue
		}
		if since != 0 && e.UnixNano < since {
			continue
		}
		kept = append(kept, e)
	}

	lo, hi, trunc := o.page.Window(len(kept))
	t := r.Table("entries",
		RCol("seq"), Col("at"), Col("kind"), RCol("actor"), Col("detail"))
	for _, e := range kept[lo:hi] {
		t.Row(Uint(e.Seq), Time(e.UnixNano), Str(e.Kind.String()),
			Uint(e.ActorID), Str(e.Detail))
	}
	t.Truncated, t.Total = trunc, len(kept)

	filtered := o.kind != "" || o.actor != 0 || o.since != ""
	summarizeLedger(&r, "audit entry", len(entries), len(kept), filtered, chainErr, nil)
	return r, nil
}

// --- assertion verify ---

type assertVerifyOpts struct{ keys pubkeyList }

var assertionVerify = cmd(Command{
	Group: "assertion", Name: "verify", Usage: "<dir>",
	Short: "the audit chain, and the signed attestation over the image",
	Long: "Two claims. The chain is hash-linked and needs no key: a break in it is\n" +
		"a fact about the file. The attestation is signed, and checking it needs\n" +
		"the public key — without -pubkey it is reported as unchecked rather than\n" +
		"as verified, because a check that was not performed and a check that\n" +
		"passed must never render the same way.",
	Notice: "opens the store read-only (shared lock) to read the attestation",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *assertVerifyOpts) {
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
	},
	runAssertionVerify)

func runAssertionVerify(cx *Context, o *assertVerifyOpts) (Result, error) {
	var r Result
	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}

	entries, err := disk.ReadAuditLog(cx.Target)
	if err != nil && !isAbsent(err) {
		return r, err
	}

	s := r.Section("audit chain")
	s.Add("entries", Int(int64(len(entries))))
	switch {
	case len(entries) == 0:
		s.Add("chain", Str("absent"))
		r.Find(SevWarn, "assertion.no_audit",
			"no audit log; operator actions are unrecorded. Set Options.Audit to record them")
	default:
		if cerr := disk.VerifyAuditChain(entries); cerr != nil {
			s.Add("chain", Str("BROKEN"))
			r.Find(SevBroken, "audit.chain_broken", "%v", cerr)
		} else {
			s.Add("chain", Str("intact"))
			s.Add("first", Time(entries[0].UnixNano))
			s.Add("last", Time(entries[len(entries)-1].UnixNano))
		}
	}

	a := r.Section("snapshot attestation")
	att, aerr := cx.Disk().SnapshotAttestation()
	switch {
	case aerr != nil:
		a.Add("attestation", Str("absent"))
		r.Find(SevWarn, "signatures.no_attestation",
			"the image carries no attestation, so nothing records who produced it")
	case verifier == nil:
		a.Add("attestation", Str("present, unchecked"))
		r.Find(SevWarn, "signatures.unchecked",
			"no -pubkey was supplied, so the attestation's signature was not checked")
	default:
		if verr := disk.VerifyAttestation(verifier, att); verr != nil {
			a.Add("attestation", Str("FAILED"))
			r.Find(SevBroken, "signatures.attestation_failed", "%v", verr)
		} else {
			a.Add("attestation", Str("verified"))
		}
	}

	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}

// auditKinds is the name-to-kind table, and the source of the help string, so
// the two cannot disagree.
var auditKinds = []struct {
	name string
	kind disk.AuditKind
}{
	{"custom", disk.AuditCustom},
	{"verify", disk.AuditVerify},
	{"checkpoint", disk.AuditCheckpoint},
	{"compact", disk.AuditCompact},
	{"redaction", disk.AuditRedaction},
	{"role-grant", disk.AuditRoleGrant},
	{"key-rotation", disk.AuditKeyRotation},
	{"attestation-export", disk.AuditAttestationExport},
	{"backup", disk.AuditBackup},
	{"restore", disk.AuditRestore},
}

func auditKindList() string {
	names := make([]string, len(auditKinds))
	for i, k := range auditKinds {
		names[i] = k.name
	}
	return strings.Join(names, " | ")
}

// parseAuditKind resolves a name or a number.
//
// Numbers are accepted because the engine's kinds are a growing enum and a
// store written by a newer build may carry one this build has no name for.
// Refusing to filter on it would make the log unreadable from an older tool.
func parseAuditKind(s string) (disk.AuditKind, error) {
	want := strings.ToLower(strings.TrimSpace(s))
	for _, k := range auditKinds {
		if k.name == want {
			return k.kind, nil
		}
	}
	if n, err := strconv.ParseUint(want, 10, 16); err == nil {
		return disk.AuditKind(n), nil
	}
	return 0, Usagef("-kind %q: want one of %s, or a number", s, auditKindList())
}
