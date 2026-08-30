package main

// wal segments, wal verify.
//
// Both read the files directly and take no lock, like `wal show`. A retired
// segment is an immutable file with a header naming its predecessor's digest;
// nothing about reading one depends on replayed state, and the moment you most
// want to check a segment chain is the moment a store is under suspicion and
// probably still open somewhere.
//
// There is no `wal compact` and there will not be one. Truncating a log is the
// one operation that can destroy history that nothing else records, which is
// exactly what the doctrine at the top of main.go refuses. `maintenance
// compact` folds the delta into the image and retires the log through the
// engine's own path, which keeps the segments.

import (
	"flag"
	"path/filepath"

	"github.com/aoiflux/graphene/disk"
)

// --- wal segments ---

type segmentOpts struct{ page Paging }

var walSegments = cmd(Command{
	Group: "wal", Name: "segments", Usage: "<dir>",
	Short: "the retired logs, and the digest chain linking them",
	Long: "Each retired segment's header names the digest of the one before it, so\n" +
		"a removed segment is a broken link rather than an absence. That is the\n" +
		"whole point of retaining them: a log that can be silently shortened\n" +
		"records nothing an adversary cannot un-record.\n\n" +
		"A store with no segments is not broken. Retention is off by default, and\n" +
		"a store that has never had it on discarded its history at each\n" +
		"compaction — which is reported, because it is the kind of gap that is\n" +
		"invisible until somebody needs what is missing.",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *segmentOpts) {
		o.page.Bind(fs, 50, "segments to show (0 = all)")
	},
	runWALSegments)

func runWALSegments(cx *Context, o *segmentOpts) (Result, error) {
	var r Result

	segs, err := disk.ListSegments(cx.Target)
	if err != nil {
		return r, err
	}
	if len(segs) == 0 {
		r.Section("").Add("segments", Str("none retained"))
		r.Find(SevWarn, "wal.no_segments",
			"no retired WAL segments; the commit history of past compactions was "+
				"discarded. Set Options.Retention to keep it")
		return r, nil
	}

	lo, hi, trunc := o.page.Window(len(segs))
	t := r.Table("segments",
		RCol("seq"), Col("file"), RCol("size"), Col("modified"), Col("digest"))
	for _, sg := range segs[lo:hi] {
		t.Row(
			Uint(sg.Sequence),
			Str(filepath.Base(sg.Path)),
			Bytes(sg.Bytes),
			Time(sg.Modified.UnixNano()),
			Hex(sg.Digest[:8]),
		)
	}
	t.Truncated, t.Total = trunc, len(segs)

	// The chain is checked over every segment, not over the page: a break
	// anywhere makes every segment suspect, including the ones shown.
	s := r.Section("")
	s.Add("segments", Int(int64(len(segs))))
	if err := disk.VerifySegmentChain(segs); err != nil {
		s.Add("digest chain", Str("BROKEN"))
		r.Find(SevBroken, "wal.segment_chain_broken", "%v", err)
		return r, nil
	}
	s.Add("digest chain", Str("intact"))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- wal verify ---

var walVerify = plain(Command{
	Group: "wal", Name: "verify", Usage: "<dir>",
	Short: "check the active log's records and the retired segment chain",
	Long: "Two separate claims, reported separately because they fail for\n" +
		"different reasons and call for different reactions:\n\n" +
		"  the active log   every record's CRC, and whether the log ends in a\n" +
		"                   torn write — which is ordinary after a crash and is\n" +
		"                   what replay exists to handle\n" +
		"  the segments     the digest chain across retired logs, where a break\n" +
		"                   means a file was removed or altered\n\n" +
		"A torn tail is not corruption. A broken segment chain is.",
	Open: OpenFiles, Tier: CtxAdvisory,
}, runWALVerify)

func runWALVerify(cx *Context) (Result, error) {
	var r Result

	info, err := disk.InspectWAL(filepath.Join(cx.Target, "graphene.wal"))
	if err != nil && !isAbsent(err) {
		return r, err
	}

	log := r.Section("active log")
	switch {
	case isAbsent(err):
		log.Add("log", Str("none; this store has never been written to since compaction"))
	default:
		// InspectWAL stops where replay would stop, so a record that fails its
		// CRC ends the walk rather than appearing in the list with a flag. The
		// count below is therefore over what replay would apply, and a bad
		// checksum shows up as Truncated — which is why the two are reported
		// together rather than as independent figures.
		bad := 0
		for _, rec := range info.Records {
			if !rec.CRCValid {
				bad++
			}
		}
		unvalidated := 0
		for _, c := range info.Commits {
			if !c.Validated {
				unvalidated++
			}
		}

		log.Add("records", Int(int64(len(info.Records))))
		log.Add("commits", Int(int64(len(info.Commits))))
		log.Add("bytes", Bytes(info.FileBytes))
		log.Addf("framing", "v%d", info.Framing)
		log.Add("bad checksums", Int(int64(bad)))
		log.Add("unvalidated commits", Int(int64(unvalidated)))
		log.Add("truncated tail", Bool(info.Truncated))
		log.Add("open batch", Bool(info.OpenBatch))

		if bad > 0 {
			// A CRC failure is not a torn tail: it is a record that was written
			// completely and then changed.
			r.Find(SevBroken, "wal.bad_checksum",
				"%d record(s) fail their CRC — written whole, and altered since", bad)
		}
		if unvalidated > 0 {
			r.Find(SevBroken, "wal.commit_unvalidated",
				"%d commit record(s) do not match the records they claim to cover",
				unvalidated)
		}
		if info.Truncated {
			r.Find(SevInfo, "wal.torn_tail",
				"the log ends at byte %d in a partial record, which is what a crash "+
					"mid-commit looks like; replay discards it and loses nothing that "+
					"was committed", info.TruncatedAt)
		}
		if info.OpenBatch {
			r.Find(SevInfo, "wal.open_batch",
				"the log ends inside an uncommitted batch; replay would discard it")
		}
	}

	segs, serr := disk.ListSegments(cx.Target)
	if serr != nil {
		return r, serr
	}
	seg := r.Section("retired segments")
	seg.Add("segments", Int(int64(len(segs))))
	switch {
	case len(segs) == 0:
		seg.Add("digest chain", Str("nothing to check"))
		r.Find(SevWarn, "wal.no_segments",
			"no retired WAL segments; the commit history of past compactions was "+
				"discarded. Set Options.Retention to keep it")
	default:
		if cerr := disk.VerifySegmentChain(segs); cerr != nil {
			seg.Add("digest chain", Str("BROKEN"))
			r.Find(SevBroken, "wal.segment_chain_broken", "%v", cerr)
		} else {
			seg.Add("digest chain", Str("intact"))
		}
	}

	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}
