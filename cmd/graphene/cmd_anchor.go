package main

// anchor list, anchor show, anchor add, keys timeline.
//
// # What an anchor is, and why this tool ships none
//
// An anchor is somewhere outside the store where a digest can be put and later
// read back, and what makes it an anchor is that whoever can rewrite the store
// cannot reach it. Nothing inside this process can establish that property, so
// the engine ships no implementation and neither does this. The one transport
// here is -insecure-local-file, named to say what it is: a file beside the
// store, witnessed by the same person who can rewrite it, useful for trying the
// mechanism out and worth nothing as evidence.
//
// # Why `anchor add` is not behind -confirm
//
// The gate exists for commands that change bytes already on disk. Publishing
// appends a checkpoint and an anchor record and can neither alter nor remove
// anything already there, so there is nothing for a confirmation to protect.
// Gating it anyway would teach the habit of typing -confirm, which is the one
// thing that makes the gate stop working on the commands that need it.
//
// -dry-run is honoured, and is exactly `anchor show`: it captures the
// checkpoint that would be published and publishes nothing.

import (
	"flag"

	"github.com/aoiflux/graphene/disk"
)

// --- anchor list ---

type anchorListOpts struct{ page Paging }

var anchorList = cmd(Command{
	Group: "anchor", Name: "list", Usage: "<dir>",
	Short: "the local checkpoint chain",
	Long: "Reads the checkpoint file directly and takes no lock. Each checkpoint\n" +
		"names the digest of the one before it, so a removed checkpoint is a\n" +
		"broken link rather than an absence.\n\n" +
		"This is the store's own account of its history. It says nothing about\n" +
		"whether any of it was witnessed — `anchor verify` is what compares it\n" +
		"against what an anchor holds.",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *anchorListOpts) {
		o.page.Bind(fs, 50, "checkpoints to show (0 = all)")
	},
	runAnchorList)

func runAnchorList(cx *Context, o *anchorListOpts) (Result, error) {
	var r Result

	chain, err := disk.ReadCheckpoints(cx.Target)
	if err != nil {
		return r, err
	}
	if len(chain) == 0 {
		r.Section("").Add("checkpoints", Str("none recorded"))
		r.Find(SevWarn, "anchor.no_checkpoints",
			"this store has never captured a checkpoint, so there is nothing an "+
				"anchor could have witnessed")
		return r, nil
	}

	lo, hi, trunc := o.page.Window(len(chain))
	t := r.Table("checkpoints",
		RCol("seq"), Col("captured"), RCol("actor"), Col("snapshot root"), Col("digest"))
	for _, c := range chain[lo:hi] {
		t.Row(
			Uint(c.Seq),
			Time(c.UnixNano),
			Uint(c.ActorID),
			Hex(c.SnapshotRoot[:8]),
			Hex(c.Digest[:8]),
		)
	}
	t.Truncated, t.Total = trunc, len(chain)

	if cx.Globals.Verbose {
		full := r.Notes("in full")
		for _, c := range chain[lo:hi] {
			full.Line("%s", c)
		}
	}

	// Verified over the whole chain, not the page: a break anywhere makes every
	// checkpoint suspect, including the ones shown.
	s := r.Section("")
	s.Add("checkpoints", Int(int64(len(chain))))
	s.Add("latest", Uint(chain[len(chain)-1].Seq))
	if err := disk.VerifyCheckpointChain(chain); err != nil {
		s.Add("chain", Str("BROKEN"))
		r.Find(SevBroken, "anchor.chain_broken", "%v", err)
		return r, nil
	}
	s.Add("chain", Str("intact"))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- anchor show ---

var anchorShow = plain(Command{
	Group: "anchor", Name: "show", Usage: "<dir>",
	Short: "capture a checkpoint without publishing it",
	Long: "Capturing is not recording. This builds the checkpoint the store would\n" +
		"publish right now — the heads of every history it keeps, and the counts\n" +
		"beside them so a truncation is visible as more than a changed hash — and\n" +
		"writes nothing.\n\n" +
		"Two consecutive calls with no intervening publication produce the same\n" +
		"sequence number, because nothing was recorded in between.",
	Notice: "opens the store read-only (shared lock) to read every history's head",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
}, runAnchorShow)

func runAnchorShow(cx *Context) (Result, error) {
	var r Result
	c, err := cx.Disk().Checkpoint()
	if err != nil {
		return r, err
	}
	addCheckpoint(&r, c)
	r.Verdict = VerdictVerified
	return r, nil
}

// addCheckpoint renders a checkpoint, shared by `anchor show` and `anchor add`
// so that what a dry run displays is laid out exactly like what a publication
// reports.
func addCheckpoint(r *Result, c disk.Checkpoint) {
	s := r.Section("checkpoint")
	s.Add("sequence", Uint(c.Seq))
	s.Add("captured", Time(c.UnixNano))
	s.Add("actor", Uint(c.ActorID))
	s.AddNote("digest", Hash(c.Digest), "(this is the value an anchor publishes)")
	s.AddNote("previous", Hash(c.Prev), "(zero for the first)")

	h := r.Table("history heads", Col("history"), Col("head"), RCol("records"))
	// A zero head is committed to as zero, so "there was no audit log" cannot be
	// retrofitted later into "here is the audit log". Showing the zeroes is the
	// point rather than noise.
	h.Row(Str("snapshot"), Hash(c.SnapshotRoot), Nil())
	h.Row(Str("attestation"), Hex(c.AttestationID[:]), Nil())
	h.Row(Str("wal segments"), Hex(c.SegmentHead[:]), Uint(c.SegmentCount))
	h.Row(Str("audit"), Hex(c.AuditHead[:]), Uint(c.AuditCount))
	h.Row(Str("redaction"), Hex(c.RedactionHead[:]), Uint(c.RedactionCount))
	h.Row(Str("grant"), Hex(c.GrantHead[:]), Uint(c.GrantCount))
}

// --- anchor add ---

type anchorAddOpts struct{ insecure string }

var anchorAdd = cmd(Command{
	Group: "anchor", Name: "add", Usage: "<dir>",
	Short: "capture a checkpoint and publish it to an anchor",
	Long: "Append-only: it adds a checkpoint and an anchor record, and can neither\n" +
		"alter nor remove anything already recorded. That is why it is not behind\n" +
		"-confirm — there is nothing for a confirmation to protect, and a gate on\n" +
		"a command that cannot lose anything teaches the habit of typing -confirm\n" +
		"without reading, which is what makes the gate stop working where it\n" +
		"matters.\n\n" +
		"-dry-run captures the checkpoint and publishes nothing, which is the same\n" +
		"report `anchor show` gives.\n\n" +
		"-insecure-local-file is not an anchor. It writes beside the store, so it\n" +
		"is witnessed by whoever can rewrite the store. A deployment with a real\n" +
		"anchor implements disk.Anchor and calls the API.",
	Notice: "anchor add opens the store for writing and takes the exclusive lock",
	Open:   OpenDiskRW, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *anchorAddOpts) {
		fs.StringVar(&o.insecure, "insecure-local-file", "",
			"path to a local anchor file, which is NOT an anchor; must be outside the store directory")
	},
	runAnchorAdd)

func runAnchorAdd(cx *Context, o *anchorAddOpts) (Result, error) {
	var r Result

	// The dry run comes first, before the anchor is even constructed:
	// NewInsecureLocalAnchor creates the file, and a dry run that created a
	// file would not be one.
	if cx.Globals.DryRun {
		c, err := cx.Disk().Checkpoint()
		if err != nil {
			return r, err
		}
		addCheckpoint(&r, c)
		r.Notice("this checkpoint would be published; nothing was written")
		return r, nil
	}

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

	c, rec, err := cx.Disk().PublishCheckpoint(anchor)
	if err != nil {
		return r, err
	}
	addCheckpoint(&r, c)

	s := r.Section("published")
	s.Add("reference", Str(rec.Ref))
	s.AddNote("at", Time(rec.UnixNano), "(the anchor's clock, not the store's)")
	s.Add("digest", Hash(rec.Digest))

	r.Notes("what this is worth",
		"an anchor witnessed by whoever can rewrite the store witnesses nothing;",
		"-insecure-local-file is for trying the mechanism, not for evidence")
	r.Verdict = VerdictVerified
	return r, nil
}

// --- keys timeline ---

type keysOpts struct {
	first uint64
	keys  pubkeyList
}

var keysTimeline = cmd(Command{
	Group: "keys", Name: "timeline", Usage: "<dir>",
	Short: "the signing-key rotations the current log records",
	Long: "Each transition is signed by the outgoing key over the incoming one, so\n" +
		"the chain can be walked from a single known starting key. -first-key is\n" +
		"that starting point: it cannot be derived from the log, because the first\n" +
		"key has no transition introducing it.\n\n" +
		"Bounded by the log's lifetime. A compaction truncates the log, so\n" +
		"transitions from before the last compaction are not here — which this\n" +
		"reports rather than letting an empty timeline read as \"no rotations\".",
	Notice: "opens the store read-only (shared lock)",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *keysOpts) {
		fs.Uint64Var(&o.first, "first-key", 0,
			"the key ID in force before the first transition (required to verify)")
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
	},
	runKeysTimeline)

func runKeysTimeline(cx *Context, o *keysOpts) (Result, error) {
	var r Result

	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}

	kt := cx.Disk().KeyTimeline()
	if len(kt.Transitions) == 0 {
		s := r.Section("")
		s.Add("transitions", Str("none in the current log"))
		r.Find(SevWarn, "keys.no_timeline",
			"the current log records no key rotations — either none happened, or "+
				"they were in a log a compaction has since truncated")
		return r, nil
	}

	t := r.Table("transitions",
		RCol("from key"), RCol("to key"), RCol("from commit"), Col("at"), Col("signed"))
	for _, tr := range kt.Transitions {
		t.Row(
			Uint(tr.PrevKeyID),
			Uint(tr.NewKeyID),
			Uint(tr.AtCommitSeq),
			Time(tr.UnixNano),
			Bool(len(tr.Signature) > 0),
		)
	}

	s := r.Section("")
	s.Add("transitions", Int(int64(len(kt.Transitions))))

	switch {
	case verifier == nil:
		s.Add("chain", Str("unchecked"))
		r.Find(SevWarn, "keys.unchecked",
			"signatures were not checked: supply -pubkey for each key in the chain")
	case o.first == 0:
		s.Add("chain", Str("unchecked"))
		r.Find(SevWarn, "keys.no_first_key",
			"supply -first-key: the chain can only be walked from a starting key, "+
				"and the first key has no transition introducing it")
	default:
		if err := kt.VerifyChain(verifier, o.first); err != nil {
			s.Add("chain", Str("BROKEN"))
			r.Find(SevBroken, "keys.chain_broken", "%v", err)
			return r, nil
		}
		s.Add("chain", Str("intact"))
		r.Verdict = VerdictVerified
	}
	return r, nil
}
