package main

// debug indexes, provenance custody, anchor verify.
//
// These three open the store, because what they check includes state that only
// exists once the log has been replayed. The framework does the opening and the
// closing; see context.go for why that is structural rather than stylistic.
//
// custody and anchor are where the lock leak lived. Both ended with
// `if report.Broken() { os.Exit(1) }` inside a function holding the store under
// a deferred Close, and os.Exit does not run deferred functions — so a store
// that failed its check was left locked by a process that had already gone. The
// verdict is returned now and the process ends in one place.

import (
	"encoding/hex"
	"flag"
	"time"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// --- debug indexes ---

var debugIndexes = plain(Command{
	Group: "debug", Name: "indexes", Aliases: []string{"verify"},
	Usage: "<dir>", Short: "structural index check",
	Long: "Runs over reconstructed in-memory indexes rather than over the files, so\n" +
		"it has to replay the log to have anything to check. Read-only: a shared\n" +
		"lock lets this run beside other readers, and a writer is refused outright\n" +
		"rather than warned about.",
	Notice: "verify opens the store read-only; a writer must not hold it",
	Open:   OpenGraphRO, Tier: CtxExact,
}, runDebugIndexes)

func runDebugIndexes(cx *Context) (Result, error) {
	var r Result
	g := cx.Graph()

	if err := g.VerifyIndexesCtx(cx.Ctx); err != nil {
		r.Find(SevBroken, "indexes.inconsistent", "index check failed: %v", err)
		return r, nil
	}

	st, err := g.Stats()
	if err != nil {
		return r, err
	}
	s := r.Section("")
	s.Add("indexes", Str("consistent"))
	s.Add("nodes", Int(int64(st.NodeCount)))
	s.Add("edges", Int(int64(st.EdgeCount)))
	if st.HasStorage {
		s.Add("delta records", Int(int64(st.Storage.DeltaRecords())))
		s.Add("log", Bytes(st.Storage.WALBytes))
	}
	r.Verdict = VerdictVerified
	return r, nil
}

// --- provenance custody ---

type custodyOpts struct {
	node   uint64
	anchor string
	keys   pubkeyList
}

var provenanceCustody = cmd(Command{
	Group: "provenance", Name: "custody", Aliases: []string{"custody"},
	Usage: "<dir>", Short: "account for one entity across every history",
	Notice: "custody opens the store read-only; a writer must not hold it",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *custodyOpts) {
		fs.Uint64Var(&o.node, "node", 0, "node ID to account for")
		// The noun-verb spelling of the same field. Two names, one variable,
		// the same default: whichever appears last wins, and an absent flag is
		// indistinguishable either way. This is how `provenance custody -id 7`
		// and the legacy `custody -node 7` reach one place with stdlib flag and
		// no aliasing machinery.
		fs.Uint64Var(&o.node, "id", 0, "node ID to account for (noun-verb spelling)")
		fs.StringVar(&o.anchor, "anchor", "",
			"hex snapshot root retained outside this system")
		fs.Var(&o.keys, "pubkey",
			"ID:HEX Ed25519 public key to verify signatures with (repeatable)")
	},
	runProvenanceCustody)

func runProvenanceCustody(cx *Context, o *custodyOpts) (Result, error) {
	var r Result

	if o.node == 0 {
		return r, Usagef("need -node (or -id)")
	}
	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}
	s := cx.Disk()

	// Without -pubkey the verifier is nil and signature-dependent layers report
	// as unchecked rather than verified — which the summary says, so a reader
	// cannot mistake "no gaps" for "signatures confirmed".
	var report disk.CustodyReport
	if o.anchor != "" {
		var h merkle.Hash
		raw, derr := hex.DecodeString(o.anchor)
		if derr != nil || len(raw) != len(h) {
			return r, Usagef("-anchor must be %d hex bytes", len(h))
		}
		copy(h[:], raw)
		report, err = s.CustodyForAnchored(store.NodeID(o.node), verifier, h)
	} else {
		report, err = s.CustodyFor(store.NodeID(o.node), verifier)
	}
	if err != nil {
		return r, err
	}

	sec := r.Section("")
	sec.Add("node", ID(uint64(report.NodeID)))
	sec.Add("known to store", Bool(report.Live))
	sec.Add("in snapshot", Bool(report.InSnapshot))
	if report.InSnapshot {
		sec.Add("snapshot root", Hash(report.SnapshotRoot))
	}
	sec.AddNote("attested", Bool(report.Attested),
		"(verified: "+boolWord(report.AttestationVerified)+")")
	sec.Add("signatures", Str(signatureNote(verifier)))
	sec.Add("segments walked", Int(int64(report.SegmentsChecked)))
	sec.Addf("audit entries", "%d (%d compactions)",
		report.AuditEntriesWalked, report.CompactionsRecorded)

	r.Notes("summary").Line("%s", report.Summary())
	for _, g := range report.Gaps {
		// A gap is a finding, not a failure. Most stores are legitimately not
		// fully provisioned, and exiting non-zero on that would make the
		// command useless in the scripts that most want it.
		r.Find(SevWarn, "custody.gap", "%s", g)
	}
	setVerdict(&r, report.Broken())
	return r, nil
}

// --- anchor verify ---

type anchorOpts struct {
	publish  bool
	insecure string
}

var anchorVerify = cmd(Command{
	Group: "anchor", Name: "verify", Aliases: []string{"anchor"},
	Usage: "<dir>", Short: "publish or check a checkpoint",
	Long: "The only anchor this command can offer is disk.InsecureLocalAnchor, which\n" +
		"is not an anchor: a file on the same machine is reachable by anyone who can\n" +
		"rewrite the store. The flag is named to say so, because a command that made\n" +
		"real anchoring look like a one-liner would produce stores that believe they\n" +
		"are witnessed and are not. A deployment with a genuine anchor implements the\n" +
		"disk.Anchor interface and calls the API.",
	Open: OpenDiskRW, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *anchorOpts) {
		fs.BoolVar(&o.publish, "publish", false,
			"capture and publish a checkpoint instead of verifying")
		fs.StringVar(&o.insecure, "insecure-local-file", "",
			"path to a local anchor file, which is NOT an anchor; must be outside the store directory")
	},
	runAnchorVerify)

func runAnchorVerify(cx *Context, o *anchorOpts) (Result, error) {
	var r Result

	if o.insecure == "" {
		return r, Usagef(
			"need -insecure-local-file: this command ships no real anchor transport.\n" +
				"See disk.Anchor — a genuine anchor lives where whoever can rewrite " +
				"the store cannot reach it")
	}
	anchor, err := disk.NewInsecureLocalAnchor(o.insecure, cx.Target)
	if err != nil {
		return r, err
	}
	s := cx.Disk()

	if o.publish {
		c, rec, perr := s.PublishCheckpoint(anchor)
		if perr != nil {
			return r, perr
		}
		sec := r.Section("")
		sec.Addf("checkpoint", "%s", c)
		sec.Addf("published as", "%q at %s", rec.Ref,
			time.Unix(0, rec.UnixNano).UTC().Format(time.RFC3339))
		return r, nil
	}

	audit, err := s.VerifyAgainstAnchor(anchor)
	if err != nil {
		return r, err
	}

	sec := r.Section("")
	sec.Addf("checkpoints", "%d local, %d published",
		len(audit.Checkpoints), len(audit.Published))
	sec.Add("confirmed", Int(int64(audit.Matched)))
	if audit.LastAnchored != nil {
		sec.Addf("last witnessed", "checkpoint %d at %s", audit.LastAnchored.Seq,
			time.Unix(0, audit.LastAnchoredAt).UTC().Format(time.RFC3339))
		sec.Add("store unchanged since", Bool(audit.CurrentMatchesLast))
	}

	r.Notes("summary").Line("%s", audit.Summary())
	for _, g := range audit.Gaps {
		// Same rule as custody: an unanchored window is a finding, not a break.
		r.Find(SevWarn, "anchor.gap", "%s", g)
	}
	setVerdict(&r, audit.Broken())
	return r, nil
}

// setVerdict records the outcome of a check that distinguishes broken from
// merely incomplete. This is the exit-status policy in one place: only the
// first of those is a failure.
func setVerdict(r *Result, broken bool) {
	switch {
	case broken:
		r.Verdict = VerdictBroken
	case len(r.Findings) > 0:
		r.Verdict = VerdictFindings
	default:
		r.Verdict = VerdictVerified
	}
}

func boolWord(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
