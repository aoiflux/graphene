package main

// The grant group: list is in cmd_ledger.go; verify, check, add and revoke are
// here.
//
// # RBAC is recorded and never enforced
//
// Every command in this group says so, and `grant check` says it twice. The
// engine does not gate a single operation on a capability: the ledger records
// who was permitted to do what, and nothing consults it. A report from
// `grant check` read as an access-control decision the store made would be
// wrong in the way that matters most — it describes intent, not enforcement.
//
// `add` and `revoke` are append-only. A revocation does not erase the grant it
// withdraws: both records stay, and what an actor holds now is what replaying
// both leaves. An audit that could not see the grant that was later revoked
// would be missing the interesting half.

import (
	"errors"
	"flag"
	"strings"

	"github.com/aoiflux/graphene/disk"
)

// --- grant verify ---

type grantVerifyOpts struct{ keys pubkeyList }

var grantVerify = cmd(Command{
	Group: "grant", Name: "verify", Usage: "<dir>",
	Short: "the grant ledger's hash chain, and its signatures",
	Long: "RBAC here is recorded and never enforced by the engine. This checks that\n" +
		"the record of what was granted is intact — not that anything was ever\n" +
		"prevented.",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *grantVerifyOpts) {
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
	},
	runGrantVerify)

func runGrantVerify(cx *Context, o *grantVerifyOpts) (Result, error) {
	var r Result
	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}
	records, err := disk.ReadGrants(cx.Target)
	if err != nil && !isAbsent(err) {
		return r, err
	}
	if len(records) == 0 {
		r.Section("").Add("grant ledger", Str("no records"))
		r.Find(SevWarn, "grants.absent",
			"no role grants are recorded, so nothing says who was permitted to "+
				"write, redact or compact. Set Options.Roles to record it")
		return r, nil
	}
	summarizeLedger(&r, "grant", len(records), len(records), false,
		disk.VerifyGrantChain(records, verifier), verifier)
	return r, nil
}

// --- grant check ---

type grantCheckOpts struct {
	actor uint64
	caps  string
}

var grantCheck = cmd(Command{
	Group: "grant", Name: "check", Usage: "<dir>",
	Short: "whether the ledger says an actor holds a capability",
	Long: "Advisory. The engine does not gate a single operation on a capability:\n" +
		"the ledger records who was permitted to do what, and nothing consults it.\n" +
		"A \"no\" here does not mean the actor was stopped, and a \"yes\" does not\n" +
		"mean they acted.\n\n" +
		"Read it as a question about the record, which is what it is: does the\n" +
		"ledger, replayed from the beginning, leave this actor holding this\n" +
		"capability. That replay is where a mistake would otherwise be invisible.",
	Notice: "opens the store read-only (shared lock)",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *grantCheckOpts) {
		fs.Uint64Var(&o.actor, "actor", 0, "the actor to ask about (required)")
		fs.StringVar(&o.caps, "cap", "", "capabilities to check for, comma-separated: "+capabilityList())
	},
	runGrantCheck)

func runGrantCheck(cx *Context, o *grantCheckOpts) (Result, error) {
	var r Result
	if o.actor == 0 {
		return r, Usagef("need -actor")
	}
	if o.caps == "" {
		return r, Usagef("need -cap: one or more of %s", capabilityList())
	}
	want, err := parseCapabilities(o.caps)
	if err != nil {
		return r, err
	}

	s := cx.Disk()
	held, err := s.Capabilities(o.actor)
	if err != nil && !errors.Is(err, disk.ErrNoGrantLedger) {
		return r, err
	}

	sec := r.Section("")
	sec.Add("actor", Uint(o.actor))
	sec.Add("asked about", Str(want.String()))
	sec.Add("holds", Str(held.String()))

	switch cerr := s.CheckCapability(o.actor, want); {
	case errors.Is(cerr, disk.ErrNoGrantLedger):
		sec.Add("answer", Str("unknowable"))
		r.Find(SevWarn, "grants.absent",
			"this store has no grant ledger, so nothing records what anyone was permitted to do")
	case cerr != nil:
		sec.Add("answer", Str("the ledger does not grant this"))
		// A finding rather than a verdict of broken: the ledger is intact and
		// answering correctly. What it says is "no".
		r.Find(SevInfo, "grants.not_held",
			"the ledger does not leave actor %d holding %s (%v)", o.actor, want.String(), cerr)
	default:
		sec.Add("answer", Str("the ledger grants this"))
		r.Verdict = VerdictVerified
	}
	r.Notes("what this is not",
		"the engine enforces nothing; this describes intent, not what was prevented")
	return r, nil
}

// --- grant add / revoke ---

type grantChangeOpts struct {
	subject uint64
	role    uint
	caps    string
	by      uint64
	reason  string
}

func bindGrantChange(fs *flag.FlagSet, o *grantChangeOpts) {
	fs.Uint64Var(&o.subject, "subject", 0, "the actor whose capabilities change (required)")
	fs.UintVar(&o.role, "role", 0, "role ID, recorded for reconstruction and never interpreted")
	fs.StringVar(&o.caps, "cap", "", "capabilities, comma-separated: "+capabilityList())
	fs.Uint64Var(&o.by, "by", 0, "the actor making the change (required)")
	fs.StringVar(&o.reason, "reason", "", "why, in your own words (required)")
}

var grantAdd = cmd(Command{
	Group: "grant", Name: "add", Usage: "<dir>",
	Short: "record that an actor was granted capabilities",
	Long: "Append-only: it adds a ledger record and can neither alter nor remove\n" +
		"an earlier one, which is why it is not behind -confirm.\n\n" +
		"-reason is required by the engine, not by this tool. A grant with no\n" +
		"stated reason is a record that something was permitted and no record of\n" +
		"why, which is the half that matters a year later.\n\n" +
		"An actor granting themselves a capability is legal and recorded as such.\n" +
		"That is deliberate: it is exactly the shape an audit wants to see.",
	Notice: "grant add opens the store for writing and takes the exclusive lock",
	Open:   OpenDiskRW, Tier: CtxAdvisory,
}, bindGrantChange,
	func(cx *Context, o *grantChangeOpts) (Result, error) { return runGrantChange(cx, o, true) })

var grantRevoke = cmd(Command{
	Group: "grant", Name: "revoke", Usage: "<dir>",
	Short: "record that capabilities were withdrawn from an actor",
	Long: "Append-only, like `grant add`. A revocation does not erase the grant it\n" +
		"withdraws — both records stay in the ledger, and the capability an actor\n" +
		"holds now is what replaying both leaves. An audit that could not see the\n" +
		"grant that was later revoked would be missing the interesting half.",
	Notice: "grant revoke opens the store for writing and takes the exclusive lock",
	Open:   OpenDiskRW, Tier: CtxAdvisory,
}, bindGrantChange,
	func(cx *Context, o *grantChangeOpts) (Result, error) { return runGrantChange(cx, o, false) })

func runGrantChange(cx *Context, o *grantChangeOpts, granting bool) (Result, error) {
	var r Result
	verb := "revoke"
	if granting {
		verb = "grant"
	}

	if o.subject == 0 {
		return r, Usagef("need -subject")
	}
	if o.by == 0 {
		return r, Usagef("need -by: an unattributed change to who may do what is " +
			"the one record an audit cannot work without")
	}
	if strings.TrimSpace(o.reason) == "" {
		return r, Usagef("need -reason: the engine requires one, and a %s with no "+
			"stated reason is the half that matters a year later", verb)
	}
	if o.caps == "" {
		return r, Usagef("need -cap: one or more of %s", capabilityList())
	}
	caps, err := parseCapabilities(o.caps)
	if err != nil {
		return r, err
	}

	if cx.Globals.DryRun {
		s := r.Section("would record")
		s.Add("kind", Str(verb))
		s.Add("subject", Uint(o.subject))
		s.Add("role", Uint(uint64(o.role)))
		s.Add("capabilities", Str(caps.String()))
		s.Add("by", Uint(o.by))
		s.Add("reason", Str(o.reason))
		return r, nil
	}

	req := disk.GrantRequest{GrantedBy: o.by, Reason: o.reason}
	var rec disk.RoleGrant
	if granting {
		rec, err = cx.Disk().GrantRole(o.subject, uint32(o.role), caps, req)
	} else {
		rec, err = cx.Disk().RevokeRole(o.subject, uint32(o.role), caps, req)
	}
	if err != nil {
		if errors.Is(err, disk.ErrNoGrantLedger) {
			return r, Usagef("this store has no grant ledger, so there is nowhere to " +
				"record this. A store gets one at creation, through Options.Roles")
		}
		return r, err
	}

	s := r.Section("recorded")
	s.Add("sequence", Uint(rec.Seq))
	s.Add("at", Time(rec.UnixNano))
	s.Add("kind", Str(verb))
	s.Add("subject", Uint(rec.Subject))
	s.Add("role", Uint(uint64(rec.RoleID)))
	s.Add("capabilities", Str(rec.Capabilities.String()))
	s.Add("by", Uint(rec.GrantedBy))
	s.Add("reason", Str(rec.Reason))
	s.Add("hash", Hex(rec.Hash[:]))

	// The derived state, which is the question the operator actually has: what
	// does this actor hold now, after every grant and revocation in the ledger.
	if held, herr := cx.Disk().Capabilities(o.subject); herr == nil {
		r.Section("").AddNote("actor now holds", Str(held.String()),
			"(recorded; the engine enforces nothing)")
	}
	r.Verdict = VerdictVerified
	return r, nil
}

// capabilities is the name-to-bit table, matching disk.Capability.String().
var capabilities = []struct {
	name string
	cap  disk.Capability
}{
	{"read", disk.CapRead},
	{"write", disk.CapWrite},
	{"delete", disk.CapDelete},
	{"redact", disk.CapRedact},
	{"compact", disk.CapCompact},
	{"rotate-key", disk.CapRotateKey},
	{"publish", disk.CapPublish},
	{"grant", disk.CapGrant},
}

func capabilityList() string {
	names := make([]string, len(capabilities))
	for i, c := range capabilities {
		names[i] = c.name
	}
	return strings.Join(names, ", ")
}

func parseCapabilities(spec string) (disk.Capability, error) {
	var out disk.Capability
	for _, part := range strings.Split(spec, ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		found := false
		for _, c := range capabilities {
			if c.name == part {
				out |= c.cap
				found = true
				break
			}
		}
		if !found {
			return 0, Usagef("-cap %q: want one or more of %s", part, capabilityList())
		}
	}
	if out == 0 {
		return 0, Usagef("-cap named no capability; want one or more of %s", capabilityList())
	}
	return out, nil
}
