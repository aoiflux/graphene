package main

// backup restore, maintenance compact, maintenance reindex, and the two
// refusals.
//
// # backup restore
//
// The sharpest gap the CLI had. graphene.Restore with AtCommitSeq/AtTime has
// existed since Phase 3 and there was no way to call it without writing a Go
// program — which meant point-in-time recovery, the thing an operator reaches
// for at the worst moment of their week, required a compiler.
//
// It writes into a directory that must not exist or must be empty, so it cannot
// damage anything: restoring over a store is refused rather than merged,
// because a directory holding half of one store and half of another is not
// recoverable by anything.
//
// # maintenance
//
// compact and reindex are the legitimate half of maintenance. Both are argued
// below. repair and vacuum are refused, and the refusal names what does exist
// rather than being a bare "no" — an operator who has been told a tool cannot
// help them goes looking for one that can, and the one they find will not have
// this tool's caution.

import (
	"flag"
	"fmt"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// --- backup restore ---

type restoreOpts struct {
	to       string
	atCommit uint64
	atTime   string
}

var backupRestore = cmd(Command{
	Group: "backup", Name: "restore", Usage: "<backup-dir>",
	Short: "rebuild a store from a backup, optionally rewound to a point in time",
	Long: "The operand is the backup; -to is the store to create. -to must not\n" +
		"exist, or must be an empty directory: restoring over a store is refused\n" +
		"rather than merged, because a directory holding half of one store and\n" +
		"half of another is not recoverable by anything.\n\n" +
		"The backup is verified against its manifest before a byte is copied, and\n" +
		"the copy is hashed again as it is written — so a restore that succeeds is\n" +
		"one whose source was whole and whose destination matches it.\n\n" +
		"Rewinding:\n\n" +
		"  -at-commit N   the last commit at or below sequence N\n" +
		"  -at-time T     the last commit at or before T (RFC 3339)\n\n" +
		"Both may be given and the tighter one wins. Prefer -at-commit: commit\n" +
		"timestamps come from the writer's clock, so a store whose clock stepped\n" +
		"backwards has timestamps that do not increase with sequence, and the\n" +
		"sequence is what the backup manifest reports.\n\n" +
		"There is a floor. Everything at or below the backup's image commit is\n" +
		"already folded into the image and cannot be undone by replaying less log\n" +
		"— `backup verify` reports that figure as \"restorable back to commit\".",
	Open: OpenNone, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *restoreOpts) {
		fs.StringVar(&o.to, "to", "", "directory to create the store in (required)")
		fs.Uint64Var(&o.atCommit, "at-commit", 0,
			"rewind to the last commit at or below this sequence (0 = the whole backup)")
		fs.StringVar(&o.atTime, "at-time", "",
			"rewind to the last commit at or before this RFC 3339 instant")
	},
	runBackupRestore)

func runBackupRestore(cx *Context, o *restoreOpts) (Result, error) {
	var r Result
	if cx.Target == "" {
		return r, Usagef("need the backup directory: graphene backup restore -to <new-dir> <backup-dir>")
	}
	if o.to == "" {
		return r, Usagef("need -to <dir>")
	}

	opts := disk.RestoreOptions{AtCommitSeq: o.atCommit}
	if o.atTime != "" {
		t, err := time.Parse(time.RFC3339, o.atTime)
		if err != nil {
			return r, Usagef("-at-time %q: want RFC 3339, e.g. 2026-08-30T14:00:00Z: %v",
				o.atTime, err)
		}
		opts.AtTime = t
	}
	rewinding := o.atCommit > 0 || o.atTime != ""

	// A dry run verifies the backup and reports where the cut would land,
	// without writing. That is most of the value: the question an operator
	// actually has before a restore is "will this give me back the state I
	// want", and finding out by producing a store is an expensive way to ask.
	if cx.Globals.DryRun {
		info, err := graphene.VerifyBackup(cx.Target)
		if err != nil {
			r.Find(SevBroken, "backup.verification_failed", "%v", err)
			return r, nil
		}
		s := r.Section("the backup")
		s.Add("taken", Time(info.TakenAt.UnixNano()))
		s.Add("newest commit", Uint(info.CommitSeq))
		s.AddNote("restorable back to commit", Uint(info.ImageCommitSeq),
			"(everything below this is inside the image)")
		s.Add("files", Int(int64(len(info.Files))))
		s.Add("size", Bytes(info.Bytes()))

		w := r.Section("would restore")
		w.Add("into", Str(o.to))
		if !rewinding {
			w.Add("rewind", Str("none; the whole backup"))
		} else {
			if o.atCommit > 0 {
				w.Add("at commit", Uint(o.atCommit))
				if o.atCommit < info.ImageCommitSeq {
					// Said before the restore rather than after it: this is the
					// one request the engine cannot satisfy, and finding out
					// from a restored store that is newer than asked for is how
					// somebody comes to believe a rewind happened when it did not.
					r.Find(SevWarn, "restore.below_floor",
						"commit %d is below the backup's image commit %d — the result "+
							"will stand at %d, not where you asked",
						o.atCommit, info.ImageCommitSeq, info.ImageCommitSeq)
				}
			}
			if o.atTime != "" {
				w.Add("at time", Str(o.atTime))
			}
		}
		return r, nil
	}

	// The destination check is here rather than in a Before hook because this
	// command's operand is the *backup* — the framework's target check already
	// covers that, and -to is ours to police.
	if err := mustBeEmpty(o.to); err != nil {
		return r, err
	}

	info, err := graphene.Restore(cx.Target, o.to, opts)
	if err != nil {
		return r, err
	}

	s := r.Section("restored")
	s.Add("into", Str(info.Dir))
	s.Add("from backup taken", Time(info.From.TakenAt.UnixNano()))
	s.Add("stands at commit", Uint(info.CommitSeq))
	s.Add("commit made", Time(info.CommitTime.UnixNano()))
	s.Add("log size", Bytes(info.WALBytes))

	if rewinding {
		s.Add("asked for commit", Uint(o.atCommit))
		s.AddNote("records dropped", Int(int64(info.DroppedRecords)),
			"(what a full restore would have replayed and this one will not)")

		if info.CommitSeq != o.atCommit && o.atCommit > 0 {
			// Not a failure: the cut lands on the last commit at or below the
			// figure asked for, and there may be no commit at exactly N. Worth
			// stating, because a report that only said "stands at 47" invites
			// the reader to assume 47 was what they typed.
			r.Find(SevInfo, "restore.cut_landed_elsewhere",
				"asked for commit %d; the last commit at or below it is %d",
				o.atCommit, info.CommitSeq)
		}
	}

	r.Notice("open it with: graphene store info %s", o.to)
	r.Verdict = VerdictVerified
	return r, nil
}

// --- maintenance compact ---

var maintenanceCompact = plain(Command{
	Group: "maintenance", Name: "compact", Usage: "<dir>",
	Short: "fold the delta into the image",
	Long: "What the library does on its own schedule, done deliberately. It rewrites\n" +
		"the store's image from records it already holds; a compaction that fails\n" +
		"leaves the old image in place, because the new one is renamed in only\n" +
		"once it is complete and fsynced.\n\n" +
		"It is not a repair and cannot recover anything. What it does is make the\n" +
		"next open fast, by turning a long log into a short one.\n\n" +
		"-dry-run reports whether a compaction is due and why, and changes nothing.",
	Notice:  "maintenance compact opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxExact,
}, runMaintenanceCompact)

func runMaintenanceCompact(cx *Context) (Result, error) {
	var r Result
	g := cx.Graph()

	st, err := g.Stats()
	if err != nil {
		return r, err
	}
	before := r.Section("before")
	before.Add("nodes", Uint(st.NodeCount))
	before.Add("edges", Uint(st.EdgeCount))
	if st.HasStorage {
		before.Add("delta nodes", Int(int64(st.Storage.DeltaNodes)))
		before.Add("delta edges", Int(int64(st.Storage.DeltaEdges)))
		before.Add("log size", Bytes(st.Storage.WALBytes))
	}

	due, why := g.ShouldCompact(store.DefaultCompactionPolicy())
	before.Add("compaction due", Bool(due))
	if due {
		before.Add("because", Str(why))
	}

	if cx.Globals.DryRun {
		// The dry run stops here on purpose. It has already opened read-only —
		// the framework downgraded the mode — so there is nothing to undo.
		if !due {
			r.Notes("what would happen",
				"the store is under every compaction threshold; compacting it now",
				"would still work and would still cost a full rewrite of the image")
		}
		return r, nil
	}

	if err := g.CompactCtx(cx.Ctx); err != nil {
		return r, err
	}

	after, err := g.Stats()
	if err != nil {
		return r, err
	}
	a := r.Section("after")
	a.Add("nodes", Uint(after.NodeCount))
	a.Add("edges", Uint(after.EdgeCount))
	if after.HasStorage {
		a.Add("delta nodes", Int(int64(after.Storage.DeltaNodes)))
		a.Add("delta edges", Int(int64(after.Storage.DeltaEdges)))
		a.Add("log size", Bytes(after.Storage.WALBytes))
	}

	// Counts must not change across a compaction. Checking rather than
	// announcing: a maintenance command that reports success without reading
	// back what it wrote is one whose failure mode is silence.
	if after.NodeCount != st.NodeCount || after.EdgeCount != st.EdgeCount {
		r.Find(SevBroken, "compact.counts_changed",
			"the record counts changed across the compaction: %d/%d nodes, %d/%d edges",
			st.NodeCount, after.NodeCount, st.EdgeCount, after.EdgeCount)
		return r, nil
	}
	r.Verdict = VerdictVerified
	return r, nil
}

// --- maintenance reindex ---

var maintenanceReindex = plain(Command{
	Group: "maintenance", Name: "reindex", Usage: "<dir>",
	Short: "rebuild the secondary indexes from the records",
	Long: "Safe because indexes are derived state: every entry is reconstructed\n" +
		"from the authoritative records, so nothing that is not already in the\n" +
		"store can be lost by rebuilding them.\n\n" +
		"The reason to reach for it is `debug indexes` reporting a disagreement.\n" +
		"An index that disagrees with the records returns entities that no longer\n" +
		"hold the value they were found by, and the query planner trusts it — so\n" +
		"the symptom is a wrong answer rather than a slow one.\n\n" +
		"-dry-run verifies the indexes and rebuilds nothing.",
	Notice:  "maintenance reindex opens the store for writing and takes the exclusive lock",
	Open:    OpenGraphRW,
	Mutates: true,
	Tier:    CtxExact,
}, runMaintenanceReindex)

func runMaintenanceReindex(cx *Context) (Result, error) {
	var r Result
	g := cx.Graph()

	before := g.VerifyIndexesCtx(cx.Ctx)
	s := r.Section("before")
	if before != nil {
		s.Add("indexes", Str("disagree with the records"))
		s.Add("first disagreement", Errv(before))
	} else {
		s.Add("indexes", Str("agree with the records"))
	}

	if cx.Globals.DryRun {
		if before != nil {
			r.Find(SevBroken, "indexes.disagree", "%v", before)
		} else {
			r.Verdict = VerdictVerified
			r.Notes("what would happen",
				"the indexes already verify; a rebuild would produce the same entries")
		}
		return r, nil
	}

	if err := g.RebuildIndexesCtx(cx.Ctx); err != nil {
		return r, err
	}

	// And check the result, which is the only thing that makes this worth
	// running: a rebuild that still disagrees means the records themselves are
	// the problem, and that is a very different conversation.
	after := r.Section("after")
	if err := g.VerifyIndexesCtx(cx.Ctx); err != nil {
		after.Add("indexes", Str("still disagree"))
		r.Find(SevBroken, "indexes.rebuild_failed",
			"the indexes still disagree after a rebuild, so the records are the "+
				"problem and not the index: %v", err)
		return r, nil
	}
	after.Add("indexes", Str("agree with the records"))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- the refusals ---

// refusal builds a command that exists only to explain why it does not.
//
// Better than an "unknown subcommand" error. Somebody typing `maintenance
// repair` has a store they believe is damaged; telling them the command does
// not exist sends them looking for a tool that has one, and the tool they find
// will not have this one's caution. Naming what does exist, and why the thing
// they asked for is absent, is the more useful answer.
func refusal(name, short, why string) *Command {
	return plain(Command{
		Group: "maintenance", Name: name, Usage: "<dir>",
		Short: short, Long: why,
		Open: OpenNone, Tier: CtxAdvisory,
	}, func(cx *Context) (Result, error) {
		return Result{}, &Fault{
			Kind: FaultRefused,
			Err:  fmt.Errorf("maintenance %s is not implemented, deliberately", name),
			Hint: why,
		}
	})
}

var maintenanceRepair = refusal("repair",
	"not implemented, deliberately",
	"There is no repair. A tool that rewrites records it has decided are wrong "+
		"is a tool that can destroy the evidence it was pointed at, and it does "+
		"so at exactly the moment nobody is in a position to check its work.\n\n"+
		"What exists instead:\n"+
		"  maintenance reindex   rebuilds derived state from the records\n"+
		"  backup restore        rebuilds a whole store from a verified copy\n"+
		"  store csr -verify     says what is actually wrong, before anything acts")

var maintenanceVacuum = refusal("vacuum",
	"not implemented, deliberately",
	"There is no vacuum. Reclaiming space means discarding history, and the "+
		"history is the product: a store that can be silently shortened records "+
		"nothing an adversary cannot un-record.\n\n"+
		"`maintenance compact` folds the delta into the image and shortens the "+
		"log through the engine's own path, which retires segments rather than "+
		"deleting them when retention is on.")
