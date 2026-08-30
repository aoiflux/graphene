package main

// store stats, store snapshot, store health.
//
// `info` reads the files and takes no lock, which is why it works against a
// store a writer holds. These three cannot: a count that excludes the delta
// layer is not a count, the snapshot roots live behind the store's own lock,
// and health is mostly questions about state that only exists once the log has
// been replayed. So they open, they say so, and `info` remains the command to
// reach for when the store is busy.

import (
	"flag"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// --- store stats ---

var storeStats = plain(Command{
	Group: "store", Name: "stats", Usage: "<dir>",
	Short: "counts, delta size, and how far the log has grown",
	Long: "The figures that decide whether a store wants compacting. WAL bytes is\n" +
		"the best proxy for how long the next open will take, and the delta counts\n" +
		"are what a compaction would fold into the image.\n\n" +
		"Open snapshots are here for one reason: a store whose memory will not come\n" +
		"down after a compaction is usually explained by something still holding an\n" +
		"old view open, and that is invisible from the files.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
}, runStoreStats)

func runStoreStats(cx *Context) (Result, error) {
	var r Result

	st, err := cx.Graph().Stats()
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("nodes", Uint(st.NodeCount))
	s.Add("edges", Uint(st.EdgeCount))

	if !st.HasStorage {
		// The in-memory backend has no delta layer and no log. Saying so beats
		// printing a screen of zeroes that read like an empty disk store.
		s.Add("storage", Str("this backend keeps no delta layer or log"))
		return r, nil
	}
	addStorage(&r, st.Storage)
	return r, nil
}

// addStorage renders StorageStats, shared with `debug stats` and `store health`.
func addStorage(r *Result, ss store.StorageStats) {
	img := r.Section("compacted image")
	img.Add("nodes", Int(int64(ss.CSRNodes)))
	img.Add("edges", Int(int64(ss.CSREdges)))

	d := r.Section("delta, written since the last compaction")
	d.Add("nodes", Int(int64(ss.DeltaNodes)))
	d.Add("edges", Int(int64(ss.DeltaEdges)))
	d.Add("deleted nodes", Int(int64(ss.DeletedNodes)))
	d.Add("deleted edges", Int(int64(ss.DeletedEdges)))

	idx := r.Section("property index")
	idx.Add("node entries", Int(int64(ss.PropertyNodeEntries)))
	idx.Add("edge entries", Int(int64(ss.PropertyEdgeEntries)))

	l := r.Section("log")
	l.AddNote("size", Bytes(ss.WALBytes), "(the best proxy for how long the next open takes)")
	l.Add("commit high-water", Uint(ss.CommitSeq))
	l.Add("visible epoch", Uint(ss.VisibleEpoch))
	if ss.OpenSnapshots > 0 {
		l.AddNote("open snapshots", Int(int64(ss.OpenSnapshots)),
			"(each pins the image it was taken against)")
		l.Add("oldest snapshot epoch", Uint(ss.OldestSnapshotEpoch))
	}
}

// --- store snapshot ---

var storeSnapshot = plain(Command{
	Group: "store", Name: "snapshot", Usage: "<dir>",
	Short: "the current epoch and the roots that commit to it",
	Long: "The snapshot root is the number worth retaining outside this system. It\n" +
		"binds the node, edge, index and tombstone roots together with the root of\n" +
		"the image this one replaced, so a substituted snapshot breaks the chain\n" +
		"even when the substitute is internally consistent.\n\n" +
		"Retain it somewhere this store cannot reach. A root checked against the\n" +
		"store that produced it proves the store agrees with itself.",
	Notice: "opens the store read-only (shared lock) to read the image's roots",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
}, runStoreSnapshot)

func runStoreSnapshot(cx *Context) (Result, error) {
	var r Result
	s := cx.Disk()

	roots, err := s.SnapshotRoots()
	if err != nil {
		// No roots is an ordinary state for a store that has never compacted,
		// not a failure. Reported as a finding so a scheduled check sees it
		// without a script treating it as corruption.
		r.Find(SevWarn, "store.no_roots",
			"this store has no snapshot roots yet: %v — compact it to write an image that has them", err)
		return r, nil
	}

	sec := r.Section("roots")
	sec.AddNote("snapshot", Hash(roots.Snapshot), "(retain this one)")
	sec.Add("nodes", Hash(roots.NodeRoot))
	sec.Add("edges", Hash(roots.EdgeRoot))
	sec.Add("property index", Hash(roots.IndexRoot))
	sec.Add("tombstones", Hash(roots.TombstoneRoot))
	sec.AddNote("previous snapshot", Hash(roots.PrevRoot), "(zero for the first image)")
	sec.Add("body version", Uint(uint64(roots.BodyVersion)))

	// The live epoch, which is a different thing from the image's roots: the
	// roots describe what was compacted, the epoch what a reader would see now.
	snap, err := s.Snapshot()
	if err == nil {
		r.Section("live view").AddNote("epoch", Uint(snap.Epoch()),
			"(orders versions within this open store; meaningless across processes)")
		_ = snap.Close()
	}

	r.Verdict = VerdictVerified
	return r, nil
}

// --- store health ---

type healthOpts struct {
	maxDelta int
	maxWAL   int64
	ratio    float64
}

var storeHealth = cmd(Command{
	Group: "store", Name: "health", Usage: "<dir>",
	Short: "one composite read-only account of a store's condition",
	Long: "Counts, whether a compaction is due, whether the last shutdown was\n" +
		"clean, what lock this process holds, and whether the image verifies.\n" +
		"Read-only throughout: it reports and never acts, so nothing here can\n" +
		"turn a store that needs attention into a store that has been changed\n" +
		"without anybody deciding to change it.\n\n" +
		"The compaction thresholds are a starting point and not a tuned\n" +
		"recommendation — override them to match what your store actually does.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *healthOpts) {
		def := store.DefaultCompactionPolicy()
		fs.IntVar(&o.maxDelta, "max-delta", def.MaxDeltaRecords,
			"compaction is due past this many delta records")
		fs.Int64Var(&o.maxWAL, "max-wal", def.MaxWALBytes,
			"compaction is due past this log size in bytes")
		fs.Float64Var(&o.ratio, "max-ratio", def.MaxDeltaRatio,
			"compaction is due past this delta-to-image ratio")
	},
	runStoreHealth)

func runStoreHealth(cx *Context, o *healthOpts) (Result, error) {
	var r Result
	g := cx.Graph()

	st, err := g.Stats()
	if err != nil {
		return r, err
	}
	s := r.Section("")
	s.Add("nodes", Uint(st.NodeCount))
	s.Add("edges", Uint(st.EdgeCount))

	due, why := g.ShouldCompact(store.CompactionPolicy{
		MaxDeltaRecords: o.maxDelta, MaxWALBytes: o.maxWAL, MaxDeltaRatio: o.ratio,
	})
	s.Add("compaction due", Bool(due))
	if due {
		s.Add("because", Str(why))
		// Advice, not alarm. A store past its compaction threshold is working
		// normally and will keep working; it is simply slower to open than it
		// needs to be.
		r.Find(SevInfo, "store.compaction_due",
			"%s — `graphene maintenance compact -confirm` folds the delta into the image", why)
	}

	// The forensic half needs the disk store underneath, which a Graph exposes
	// only when it is disk-backed.
	if ds, ok := g.Forensics(); ok {
		addHealthDisk(&r, ds)
	}

	if st.HasStorage {
		addStorage(&r, st.Storage)
	}

	// And the checks themselves, so "healthy" is a claim about verification and
	// not just about size.
	if err := g.VerifyIndexesCtx(cx.Ctx); err != nil {
		r.Find(SevBroken, "store.indexes_disagree",
			"the property index disagrees with the records: %v", err)
	} else if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}

func addHealthDisk(r *Result, s *disk.Store) {
	sec := r.Section("this process")
	sec.Add("lock", Str(lockName(s.LockMode())))
	sec.AddNote("lock enforced by the OS", Bool(s.LockEnforced()),
		"(false on platforms with no file-locking primitive)")
	sec.Add("read only", Bool(s.ReadOnly()))
	if s.RecoveredFromUncleanShutdown() {
		sec.Add("last shutdown", Str("unclean; the log was replayed to recover"))
		// Worth saying and not worth alarming about: recovery is the WAL doing
		// its job. It becomes interesting only if it happens every time.
		r.Find(SevWarn, "store.unclean_shutdown",
			"the previous run did not close cleanly and the log was replayed — "+
				"no data was lost, but something killed the writer")
	} else {
		sec.Add("last shutdown", Str("clean"))
	}
}

func lockName(m disk.LockMode) string {
	if m == disk.LockExclusive {
		return "exclusive (writer)"
	}
	return "shared (reader)"
}

// forensicsOrRefuse returns the disk store behind a Graph, or an error naming
// why there is not one. Shared by the commands that need both layers.
func forensicsOrRefuse(g *graphene.Graph) (*disk.Store, error) {
	ds, ok := g.Forensics()
	if !ok {
		return nil, Usagef("this store is not disk-backed, so it keeps none of the " +
			"forensic history this command reports on")
	}
	return ds, nil
}
