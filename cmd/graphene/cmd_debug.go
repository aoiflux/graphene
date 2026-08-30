package main

// The debug group: hash-check, signature-check, stats, integrity.
//
// Three narrow checks and one that runs everything. The narrow ones exist
// because they fail for different reasons and a monitoring system wants to
// alert on them differently:
//
//   hash-check       the image's own bytes — a file that changed since it was
//                    written. Reads files, takes no lock, works against a busy
//                    store.
//   signature-check  the signed layers — who vouched for what. Needs -pubkey,
//                    and says so rather than reporting "verified" for checks it
//                    could not perform.
//   integrity        all of it, one verdict.
//
// `debug profile` is not here. Metrics can only be attached at open time and
// the framework owns the open, so profiling is the global -metrics flag, which
// works on every command rather than on the ones a wrapper knew about. See
// metrics.go.

import (
	"errors"
	"flag"
	"path/filepath"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// --- debug hash-check ---

var debugHashCheck = plain(Command{
	Group: "debug", Name: "hash-check", Usage: "<dir|file>",
	Short: "the image's digest and Merkle roots, against its own contents",
	Long: "Two separate questions. The digest asks whether the file is the file\n" +
		"that was written; the roots ask whether the Merkle values inside it\n" +
		"actually describe the records sitting next to them. A file can pass the\n" +
		"first and fail the second — that is a file rewritten wholesale, digest\n" +
		"and all — and reporting them together would hide it.\n\n" +
		"Neither can say whether a change was damage or an edit.",
	Open: OpenFiles, Tier: CtxAdvisory,
}, runDebugHashCheck)

func runDebugHashCheck(cx *Context) (Result, error) {
	var r Result
	addHashChecks(&r, cx.Target)
	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}

// addHashChecks is the file-level half, shared with `debug integrity`.
func addHashChecks(r *Result, target string) {
	v := r.Section("image")

	status, computed, err := disk.VerifyCSRDigest(target)
	switch {
	case err != nil && isAbsent(err):
		v.Add("image", Str("none; this store has never been compacted"))
		return
	case err != nil:
		v.Add("digest", Str("could not be read"))
		r.Find(SevBroken, "csr.unreadable", "%v", err)
		return
	}

	v.Addf("digest", "%v", status)
	if status != disk.DigestAbsent {
		v.Add("computed", Hex(computed[:]))
	}
	if status == disk.DigestMismatch {
		r.Find(SevBroken, "csr.digest_mismatch",
			"the file does not match the digest it carries — it has changed since "+
				"it was written. The digest cannot say whether that was damage or an edit.")
	}

	switch err := disk.VerifyCSRRoots(target); {
	case err == nil:
		v.Add("roots", Str("describe the records"))
	case errors.Is(err, disk.ErrNoSnapshotRoots):
		v.Add("roots", Str("absent (image predates snapshot roots)"))
	default:
		v.Add("roots", Str("FAILED"))
		r.Find(SevBroken, "csr.roots_mismatch",
			"the Merkle roots do not describe the records beside them: %v", err)
	}
}

// --- debug signature-check ---

type signatureOpts struct {
	keys  pubkeyList
	first uint64
}

var debugSignatureCheck = cmd(Command{
	Group: "debug", Name: "signature-check", Usage: "<dir>",
	Short: "every signed layer, against the keys you supply",
	Long: "The attestation over the image, the audit chain, the redaction ledger,\n" +
		"the grant ledger and the key timeline.\n\n" +
		"Without -pubkey nothing can be verified, and this reports that rather\n" +
		"than reporting success: a check that was not performed and a check that\n" +
		"passed must never render the same way. The keys go on the command line\n" +
		"so an auditor holding the public key and not the store can run it —\n" +
		"nothing here ever touches private material.",
	Notice: "opens the store read-only (shared lock)",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *signatureOpts) {
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
		fs.Uint64Var(&o.first, "first-key", 0,
			"the key ID in force before the first rotation, for the key timeline")
	},
	runDebugSignatureCheck)

func runDebugSignatureCheck(cx *Context, o *signatureOpts) (Result, error) {
	var r Result

	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}
	addSignatureChecks(&r, cx.Disk(), cx.Target, verifier, o.first)
	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}

// addSignatureChecks is the signed half, shared with `debug integrity`.
//
// Every row reports one of three states and never conflates them: verified,
// broken, or unchecked. "Unchecked" is the row that matters — it is what a
// store with no keys supplied produces, and a report that showed it as a tick
// would be the single most misleading thing this tool could print.
func addSignatureChecks(r *Result, s *disk.Store, dir string,
	verifier store.Verifier, firstKey uint64) {

	t := r.Table("signed layers", Col("layer"), Col("result"), RCol("records"))

	row := func(layer string, n int, err error, checkable bool, code string) {
		switch {
		case !checkable:
			t.Row(Str(layer), Str("unchecked"), Int(int64(n)))
		case err != nil:
			t.Row(Str(layer), Str("BROKEN"), Int(int64(n)))
			r.Find(SevBroken, code, "%s: %v", layer, err)
		default:
			t.Row(Str(layer), Str("verified"), Int(int64(n)))
		}
	}

	// The attestation over the image.
	att, aerr := s.SnapshotAttestation()
	switch {
	case aerr != nil:
		t.Row(Str("snapshot attestation"), Str("absent"), Int(0))
		r.Find(SevWarn, "signatures.no_attestation",
			"the image carries no attestation, so nothing records who produced it")
	default:
		row("snapshot attestation", 1, disk.VerifyAttestation(verifier, att), verifier != nil,
			"signatures.attestation_failed")
	}

	// The audit chain is hash-linked and needs no key: a break in it is a fact
	// about the file, not about who signed it. It is in this table because a
	// reader checking "what can I trust here" wants one place to look.
	entries, eerr := disk.ReadAuditLog(dir)
	if eerr != nil && !isAbsent(eerr) {
		r.Find(SevBroken, "audit.unreadable", "%v", eerr)
	}
	if len(entries) == 0 {
		t.Row(Str("audit chain"), Str("absent"), Int(0))
		r.Find(SevWarn, "signatures.no_audit",
			"no audit log; operator actions are unrecorded. Set Options.Audit to record them")
	} else {
		row("audit chain", len(entries), disk.VerifyAuditChain(entries), true,
			"audit.chain_broken")
	}

	reds, rerr := disk.ReadRedactions(dir)
	if rerr != nil && !isAbsent(rerr) {
		r.Find(SevBroken, "redactions.unreadable", "%v", rerr)
	}
	if len(reds) == 0 {
		t.Row(Str("redaction ledger"), Str("empty"), Int(0))
	} else {
		row("redaction ledger", len(reds), disk.VerifyRedactionChain(reds, verifier), true,
			"redactions.chain_broken")
	}

	grants, gerr := disk.ReadGrants(dir)
	if gerr != nil && !isAbsent(gerr) {
		r.Find(SevBroken, "grants.unreadable", "%v", gerr)
	}
	if len(grants) == 0 {
		t.Row(Str("grant ledger"), Str("empty"), Int(0))
	} else {
		row("grant ledger", len(grants), disk.VerifyGrantChain(grants, verifier), true,
			"grants.chain_broken")
	}

	kt := s.KeyTimeline()
	switch {
	case len(kt.Transitions) == 0:
		t.Row(Str("key timeline"), Str("no rotations in the current log"), Int(0))
	case verifier == nil || firstKey == 0:
		t.Row(Str("key timeline"), Str("unchecked"), Int(int64(len(kt.Transitions))))
	default:
		row("key timeline", len(kt.Transitions), kt.VerifyChain(verifier, firstKey), true,
			"keys.chain_broken")
	}

	if verifier == nil {
		r.Find(SevWarn, "signatures.unchecked",
			"no -pubkey was supplied, so no signature was checked — the hash chains "+
				"above stand on their own and say nothing about who wrote them")
	}
}

// --- debug stats ---

var debugStats = plain(Command{
	Group: "debug", Name: "stats", Usage: "<dir>",
	Short: "the same figures as `store stats`, kept for the debug group",
	Long: "One report, two names. `store stats` is the spelling to prefer; this\n" +
		"exists because somebody looking through `debug` for a size figure should\n" +
		"find it rather than conclude the tool cannot report one.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
}, runStoreStats)

// --- debug integrity ---

type integrityOpts struct {
	keys  pubkeyList
	first uint64
}

var debugIntegrity = cmd(Command{
	Group: "debug", Name: "integrity", Usage: "<dir>",
	Short: "every check this tool can make, one verdict",
	Long: "The image's digest and roots, the property indexes, the WAL segment\n" +
		"chain, the checkpoint chain, and every signed layer. One report, one\n" +
		"exit status, so a scheduled check needs one line rather than nine.\n\n" +
		"It opens the store, which the file-level checks do not need — so on a\n" +
		"store a writer holds, `debug hash-check` and `wal verify` still answer\n" +
		"and this does not.\n\n" +
		"Exit status follows the usual rule: an incomplete account — never signed,\n" +
		"never anchored, no retained history — is findings and exits zero, because\n" +
		"most stores are legitimately not fully provisioned. Only something\n" +
		"actually broken exits non-zero.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *integrityOpts) {
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
		fs.Uint64Var(&o.first, "first-key", 0,
			"the key ID in force before the first rotation, for the key timeline")
	},
	runDebugIntegrity)

func runDebugIntegrity(cx *Context, o *integrityOpts) (Result, error) {
	var r Result

	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}
	g := cx.Graph()
	dir := cx.Target

	// 1. The image's own bytes.
	addHashChecks(&r, filepath.Join(dir, "graphene.csr"))

	// 2. Derived state against the records it describes.
	idx := r.Section("indexes")
	if err := g.VerifyIndexesCtx(cx.Ctx); err != nil {
		idx.Add("property index", Str("disagrees with the records"))
		r.Find(SevBroken, "indexes.disagree",
			"the property index disagrees with the records: %v — "+
				"`maintenance reindex` rebuilds it", err)
	} else {
		idx.Add("property index", Str("agrees with the records"))
	}

	// 3. The log's retained history.
	segs, serr := disk.ListSegments(dir)
	if serr != nil {
		return r, serr
	}
	seg := r.Section("wal segments")
	seg.Add("segments", Int(int64(len(segs))))
	switch {
	case len(segs) == 0:
		seg.Add("digest chain", Str("nothing retained"))
		r.Find(SevWarn, "wal.no_segments",
			"no retired WAL segments; the commit history of past compactions was "+
				"discarded. Set Options.Retention to keep it")
	default:
		if err := disk.VerifySegmentChain(segs); err != nil {
			seg.Add("digest chain", Str("BROKEN"))
			r.Find(SevBroken, "wal.segment_chain_broken", "%v", err)
		} else {
			seg.Add("digest chain", Str("intact"))
		}
	}

	// 4. The checkpoint chain.
	chain, cerr := disk.ReadCheckpoints(dir)
	if cerr != nil && !isAbsent(cerr) {
		return r, cerr
	}
	cp := r.Section("checkpoints")
	cp.Add("checkpoints", Int(int64(len(chain))))
	switch {
	case len(chain) == 0:
		cp.Add("chain", Str("none captured"))
		r.Find(SevWarn, "anchor.no_checkpoints",
			"this store has never captured a checkpoint, so there is nothing an "+
				"anchor could have witnessed")
	default:
		if err := disk.VerifyCheckpointChain(chain); err != nil {
			cp.Add("chain", Str("BROKEN"))
			r.Find(SevBroken, "anchor.chain_broken", "%v", err)
		} else {
			cp.Add("chain", Str("intact"))
		}
	}

	// 5. Everything signed. Needs the disk store under the graph.
	if ds, ok := g.Forensics(); ok {
		addSignatureChecks(&r, ds, dir, verifier, o.first)
	}

	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}
