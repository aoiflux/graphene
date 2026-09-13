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
	"cmp"
	"flag"
	"fmt"
	"slices"

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

	addTypeCounts(&r, st)

	if !st.HasStorage {
		// The in-memory backend has no delta layer and no log. Saying so beats
		// printing a screen of zeroes that read like an empty disk store.
		s.Add("storage", Str("this backend keeps no delta layer or log"))
		return r, nil
	}
	addStorage(&r, st.Storage)
	return r, nil
}

// addTypeCounts renders the per-label breakdown when the backend can produce it.
//
// Two tables rather than two lists, and sorted by count: the question an
// operator has when they run this is "what is in here", and the answer is a
// shape — a hundred thousand of one type and nine of another is the interesting
// fact, not the alphabet.
//
// The totals deliberately do not add up to the node and edge counts above, and
// the note says so rather than leaving it to be noticed: an entity carrying two
// labels is counted under both, and a reader who assumes otherwise concludes the
// store is corrupt.
func addTypeCounts(r *Result, st *graphene.GraphStats) {
	if !st.HasTypeCounts || (len(st.NodesByType) == 0 && len(st.EdgesByType) == 0) {
		return
	}
	multi := false

	if len(st.NodesByType) > 0 {
		var summed uint64
		t := r.Table("nodes by label", Col("label"), Col("count"))
		for _, e := range sortedCounts(st.NodesByType) {
			summed += e.count
			t.Row(Str(e.label), Uint(e.count))
		}
		multi = multi || summed > st.NodeCount
	}
	if len(st.EdgesByType) > 0 {
		var summed uint64
		t := r.Table("edges by label", Col("label"), Col("count"))
		for _, e := range sortedCounts(st.EdgesByType) {
			summed += e.count
			t.Row(Str(e.label), Uint(e.count))
		}
		multi = multi || summed > st.EdgeCount
	}
	if multi {
		r.Notes("about these counts",
			"an entity carrying two labels is counted under both, so these total "+
				"more than the counts above")
	}
}

type labelCount struct {
	label string
	count uint64
}

// sortedCounts orders a label breakdown by count descending, then by name, so
// two runs over one store print the same table.
func sortedCounts[T interface {
	~uint16
	String() string
}](counts map[T]uint64) []labelCount {
	out := make([]labelCount, 0, len(counts))
	for t, n := range counts {
		out = append(out, labelCount{label: t.String(), count: n})
	}
	slices.SortFunc(out, func(a, b labelCount) int {
		if c := cmp.Compare(b.count, a.count); c != 0 {
			return c
		}
		return cmp.Compare(a.label, b.label)
	})
	return out
}

// idHeadroomFinding is the headroom below which `store health` says so. It
// matches disk.Options.IDHeadroomWarn's default rather than reading it, because
// health inspects a store it did not configure — a store opened with a lower
// threshold has chosen when to be told, and this is the command's own opinion.
const idHeadroomFinding = 0.10

// pctOf renders a headroom fraction at a fixed two decimal places.
//
// Not %g: a store at 99.9997% headroom and one at 9.9997% are the same number
// of significant figures and completely different situations, and an operator
// scanning a column wants them to line up.
func pctOf(fraction float64) string {
	return fmt.Sprintf("%.2f%%", fraction*100)
}

// addStorage renders StorageStats, shared with `debug stats` and `store health`.
func addStorage(r *Result, ss store.StorageStats) {
	img := r.Section("compacted image")
	img.Add("nodes", Int(int64(ss.CSRNodes)))
	img.Add("edges", Int(int64(ss.CSREdges)))

	// Identifiers issued against identifiers addressable. It sits beside the
	// record counts because that is where an operator will compare it with
	// them, and the comparison is the point: a store whose records number in
	// the thousands can have issued millions of identifiers, and only this
	// pair shows it.
	if ss.IDCeiling > 0 {
		ids := r.Section("identifiers issued")
		ids.AddNote("nodes", Uint(ss.HighestNodeID),
			fmt.Sprintf("of %d (%s headroom)", ss.IDCeiling, pctOf(ss.NodeIDHeadroom)))
		ids.AddNote("edges", Uint(ss.HighestEdgeID),
			fmt.Sprintf("of %d (%s headroom)", ss.IDCeiling, pctOf(ss.EdgeIDHeadroom)))
	}

	d := r.Section("delta, written since the last compaction")
	d.Add("nodes", Int(int64(ss.DeltaNodes)))
	d.Add("edges", Int(int64(ss.DeltaEdges)))
	d.Add("deleted nodes", Int(int64(ss.DeletedNodes)))
	d.Add("deleted edges", Int(int64(ss.DeletedEdges)))

	// Bytes beside the counts, because the counts cannot tell the two cases
	// apart: a hundred records carrying large blobs and a hundred thousand
	// carrying none are four orders of magnitude apart in what they cost, and
	// whichever one an operator has, the other is what the record count suggests.
	d.AddNote("record bytes", Bytes(ss.DeltaBytes),
		"(what compaction would move into the image)")

	idx := r.Section("property index")
	idx.Add("node entries", Int(int64(ss.PropertyNodeEntries)))
	idx.Add("edge entries", Int(int64(ss.PropertyEdgeEntries)))

	// The estimate, with the caveat attached rather than left in the godoc. An
	// operator who reads a figure like this and sizes a machine from it will be
	// wrong by the ratio between retained heap and what the operating system
	// reports, and here is the only place they are told before they are wrong.
	if ss.EstimatedResidentBytes > 0 {
		m := r.Section("memory")
		m.AddNote("estimated heap", Bytes(ss.EstimatedResidentBytes),
			"(retained heap, a floor on RSS: measured at 1.19-1.54x this)")
		if ss.ImageMappedBytes > 0 {
			m.AddNote("mapped image", Bytes(ss.ImageMappedBytes),
				"(page cache, evictable, not counted above)")
		}
	}

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
	maxDelta      int
	maxDeltaBytes int64
	maxWAL        int64
	ratio         float64
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
		fs.Int64Var(&o.maxDeltaBytes, "max-delta-bytes", def.MaxDeltaBytes,
			"compaction is due past this many bytes held in the delta")
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
		MaxDeltaRecords: o.maxDelta, MaxDeltaBytes: o.maxDeltaBytes,
		MaxWALBytes: o.maxWAL, MaxDeltaRatio: o.ratio,
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

	// The identifier space is the one resource compaction cannot give back:
	// identifiers are never reused, and the remedy is an export and import into
	// a fresh store. A warning rather than an error, because a store at 5%
	// headroom works exactly as it did at 95%.
	if st.HasStorage && st.Storage.IDCeiling > 0 {
		for _, k := range []struct {
			kind     string
			issued   uint64
			headroom float64
		}{
			{"node", st.Storage.HighestNodeID, st.Storage.NodeIDHeadroom},
			{"edge", st.Storage.HighestEdgeID, st.Storage.EdgeIDHeadroom},
		} {
			if k.headroom >= idHeadroomFinding {
				continue
			}
			r.Find(SevWarn, "store.id_headroom_low",
				"%s identifiers: %d of %d issued, %s of the space left — identifiers are "+
					"never reused, so the remedy is an export and import into a fresh store",
				k.kind, k.issued, st.Storage.IDCeiling, pctOf(k.headroom))
		}
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
