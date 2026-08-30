package main

// store info, store csr, wal show.
//
// These three read graphene.csr and graphene.wal directly and never open the
// store. That is deliberate. Opening replays the log, rebuilds indexes and
// takes a handle on the WAL — and the moment you most want to look at a store
// is the moment something has gone wrong with it and a live process is probably
// still attached. An inspector that cannot be used then is not much of an
// inspector.

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/aoiflux/graphene/disk"
)

// --- store info ---

var storeInfo = plain(Command{
	Group: "store", Name: "info", Aliases: []string{"info"},
	Usage: "<dir>", Short: "summary of the image and the log",
	Long: "Reads the two files directly and takes no lock, so it works against a\n" +
		"store another process is writing to — which is when you most need it.",
	Open: OpenFiles, Tier: CtxAdvisory,
}, runStoreInfo)

func runStoreInfo(cx *Context) (Result, error) {
	var r Result
	dir := cx.Target

	head := r.Section("")
	head.Add("store", Str(dir))

	csr, csrErr := disk.InspectCSR(dir)
	img := r.Section("CSR image")
	switch {
	case csrErr != nil && isAbsent(csrErr):
		img.Add("status", Str("absent (store has never been compacted)"))
	case csrErr != nil:
		img.Addf("status", "UNREADABLE: %v", csrErr)
		r.Find(SevBroken, "csr.unreadable", "the CSR image cannot be read: %v", csrErr)
	default:
		img.Addf("version", "v%d", csr.Version)
		img.Add("size", Bytes(csr.FileBytes))
		img.Add("nodes", Int(int64(csr.NodeCount)))
		img.Add("edges", Int(int64(csr.EdgeCount)))
		img.Addf("property entries", "%d node, %d edge",
			csr.PropertyNodeEntries, csr.PropertyEdgeEntries)
		img.Addf("sequence high-water", "node %d, edge %d", csr.NodeSeqHW, csr.EdgeSeqHW)
		img.AddNote("highest ID",
			Str(fmt.Sprintf("node %d, edge %d", csr.MaxNodeID, csr.MaxEdgeID)),
			sparsityNote(csr))
	}

	wal, walErr := disk.InspectWAL(dir)
	log := r.Section("Write-ahead log")
	switch {
	case walErr != nil && isAbsent(walErr):
		log.Add("status", Str("absent"))
	case walErr != nil:
		log.Addf("status", "UNREADABLE: %v", walErr)
		r.Find(SevBroken, "wal.unreadable", "the log cannot be read: %v", walErr)
	default:
		log.Add("size", Bytes(wal.FileBytes))
		log.Add("records", Int(int64(len(wal.Records))))
		log.Add("commits", Int(int64(len(wal.Commits))))
		if wal.Checkpointed {
			log.Add("checkpoint", Str("present — replay stops there"))
		}
		if wal.Truncated {
			log.Addf("tail", "TRUNCATED at byte %d — records past this point are ignored on replay",
				wal.TruncatedAt)
			r.Find(SevWarn, "wal.truncated",
				"the log is truncated at byte %d; records past it are ignored on replay",
				wal.TruncatedAt)
		}
		if wal.OpenBatch {
			log.Add("open batch", Str("YES — a batch began and never committed; replay discards it"))
			r.Find(SevWarn, "wal.open_batch",
				"a batch began and never committed; replay will discard it")
		}
		if n := attributedCommits(wal); n > 0 {
			log.Addf("attributed", "%d of %d commits carry an actor", n, len(wal.Commits))
		}
		if last, ok := lastCommit(wal); ok {
			log.Addf("last commit", "seq %d at %s", last.CommitSeq, formatNano(last.UnixNano))
		}
	}

	// The two files together are what an operator actually needs to judge.
	if csrErr == nil && walErr == nil {
		r.Section("").Add("unreplayed log",
			Str(humanBytes(wal.FileBytes)+" of records to apply on next open"))
	}
	return r, nil
}

// --- store csr ---

type csrOpts struct{ verify bool }

var storeCSR = cmd(Command{
	Group: "store", Name: "csr", Aliases: []string{"csr"},
	Usage: "<dir|file>", Short: "CSR header detail, and optionally its digest",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *csrOpts) {
		fs.BoolVar(&o.verify, "verify", false,
			"check the stored digest against the file's contents")
	},
	runStoreCSR)

func runStoreCSR(cx *Context, o *csrOpts) (Result, error) {
	var r Result
	target := cx.Target

	// Verification first: if the file has been altered, that is the answer, and
	// the parsed detail below should be read in that light.
	if o.verify {
		status, computed, err := disk.VerifyCSRDigest(target)
		if err != nil {
			return r, err
		}
		v := r.Section("verification")
		v.Addf("digest", "%v", status)
		if status != disk.DigestAbsent {
			v.Add("computed", Hex(computed[:]))
		}
		if status == disk.DigestMismatch {
			r.Find(SevBroken, "csr.digest_mismatch",
				"the file does not match the digest it carries — it has changed since "+
					"it was written. The digest cannot say whether that was damage or an edit.")
		}

		// A separate question from the digest: do the Merkle roots actually
		// describe the records sitting next to them?
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
		if r.Verdict == VerdictNone {
			r.Verdict = VerdictVerified
		}
	}

	c, err := disk.InspectCSR(target)
	if err != nil {
		// If -verify has already found the image broken, failing to parse it is
		// the same fact told twice, not a separate operational failure. Report
		// it as part of the verdict: the answer to "is this image sound" is no,
		// and the caller should not have to distinguish "it verified as broken"
		// from "it was too broken to read".
		if r.Verdict == VerdictBroken {
			r.Find(SevBroken, "csr.unparseable",
				"the image cannot be parsed either: %v", err)
			return r, nil
		}
		return r, err
	}

	h := r.Section("")
	h.Add("file", Str(c.Path))
	h.Addf("size", "%d bytes", c.FileBytes)
	h.Addf("format", "v%d", c.Version)
	h.Add("nodes", Int(int64(c.NodeCount)))
	h.Add("edges", Int(int64(c.EdgeCount)))
	h.Add("node seq high-water", Uint(c.NodeSeqHW))
	h.Add("edge seq high-water", Uint(c.EdgeSeqHW))
	h.Add("highest node ID", Uint(c.MaxNodeID))
	h.Add("highest edge ID", Uint(c.MaxEdgeID))
	h.Addf("property entries", "%d node, %d edge", c.PropertyNodeEntries, c.PropertyEdgeEntries)
	if c.CommitSeqHW > 0 {
		h.Add("commit seq high-water", Uint(c.CommitSeqHW))
	}
	if c.LastCompactUnixNano != 0 {
		h.Add("last compacted", Str(formatNano(c.LastCompactUnixNano)))
	}

	if c.HasSnapshotRoots {
		s := r.Section("")
		s.Add("snapshot root", Hex(c.SnapshotRoot[:]))
		s.Add("  node root", Hex(c.NodeRoot[:]))
		s.Add("  edge root", Hex(c.EdgeRoot[:]))
		s.Add("  index root", Hex(c.IndexRoot[:]))
		if c.PrevSnapshotRoot != [32]byte{} {
			s.Add("  replaces", Hex(c.PrevSnapshotRoot[:]))
		} else {
			s.Add("  replaces", Str("(first compaction)"))
		}
	}

	if c.HasAttestation {
		a := r.Section("")
		a.Add("attestation", Hex(c.AttestationID[:]))
		a.Add("  actor", Uint(c.AttestActorID))
		a.Add("  signed by key", Uint(uint64(c.AttestKeyID)))
		a.Add("  at", Str(formatNano(c.AttestUnixNano)))
		if c.AttestPrev != [16]byte{} {
			a.Add("  follows", Hex(c.AttestPrev[:]))
		} else {
			a.Add("  follows", Str("(first attestation)"))
		}
	}

	if len(c.Sections) > 0 {
		s := r.Section("")
		s.Add("sections", Int(int64(len(c.Sections))))
		for _, sec := range c.Sections {
			kind := "optional"
			if sec.Critical {
				kind = "CRITICAL"
			}
			note := ""
			if !sec.Known {
				note = "  <- not understood by this build"
			}
			s.AddNote("  "+sec.Magic,
				Str(fmt.Sprintf("%s, %d bytes at %d", kind, sec.Length, sec.Offset)), note)
			if !sec.Known && sec.Critical {
				r.Find(SevBroken, "csr.unknown_critical_section",
					"section %s is marked critical and this build does not understand it",
					sec.Magic)
			}
		}
	}
	return r, nil
}

// --- wal show ---

type walOpts struct {
	paging      Paging
	commitsOnly bool
}

var walShow = cmd(Command{
	Group: "wal", Name: "show", Aliases: []string{"wal"},
	Usage: "<dir|file>", Short: "record-by-record log dump",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *walOpts) {
		// The default of 50 and the "0 = all" convention predate this design
		// and are kept exactly: `wal -limit 0` has meant "everything" for as
		// long as the command has existed. This is also why -limit is not a
		// global flag — see the note in globals.go.
		o.paging.Bind(fs, 50, "stop after this many records (0 = all)")
		fs.BoolVar(&o.commitsOnly, "commits", false,
			"show only batch commits and their provenance")
	},
	runWALShow)

func runWALShow(cx *Context, o *walOpts) (Result, error) {
	var r Result

	info, err := disk.InspectWAL(cx.Target)
	if err != nil {
		return r, err
	}

	h := r.Section("")
	h.Addf("file", "%s (%s)", info.Path, humanBytes(info.FileBytes))
	h.Addf("records", "%d in %d commits", len(info.Records), len(info.Commits))

	if o.commitsOnly {
		t := r.Table("commits",
			RCol("offset"), RCol("seq"), Col("when"), Col("actor"),
			RCol("records"), Col("valid"))
		lo, hi, cut := o.paging.Window(len(info.Commits))
		for _, c := range info.Commits[lo:hi] {
			seq, when, actor := Nil(), Nil(), Nil()
			if c.HasDetail {
				seq = Uint(c.CommitSeq)
				when = Str(formatNano(c.UnixNano))
				actor = Uint(c.ActorID)
				if c.ActorID == 0 {
					actor = Str("unattributed")
				}
			}
			t.Row(Int(c.Offset), seq, when, actor, Int(int64(c.RecordsIn)), Bool(c.Validated))
		}
		t.Truncated, t.Total = cut, len(info.Commits)
	} else {
		t := r.Table("records", RCol("offset"), Col("type"), RCol("len"), Col("crc"), Col("in batch"))
		lo, hi, cut := o.paging.Window(len(info.Records))
		bad := 0
		for _, rec := range info.Records[lo:hi] {
			crc := "ok"
			if !rec.CRCValid {
				crc = "BAD"
				bad++
			}
			t.Row(Int(rec.Offset), Str(rec.TypeName), Int(int64(rec.Length)),
				Str(crc), Bool(rec.InBatch))
		}
		t.Truncated, t.Total = cut, len(info.Records)
		if bad > 0 {
			r.Find(SevBroken, "wal.crc_mismatch",
				"%d record(s) shown fail their CRC", bad)
		}
	}

	if info.Truncated {
		r.Find(SevWarn, "wal.truncated",
			"the tail is truncated at byte %d — replay stops there", info.TruncatedAt)
	}
	if info.OpenBatch {
		r.Find(SevWarn, "wal.open_batch",
			"a batch began and never committed — replay discards it")
	}
	return r, nil
}

// isAbsent reports whether err is "the file is not there", as opposed to "the
// file is there and wrong". The distinction matters: an absent CSR is a store
// that has never been compacted, which is normal.
func isAbsent(err error) bool {
	return os.IsNotExist(underlying(err))
}
