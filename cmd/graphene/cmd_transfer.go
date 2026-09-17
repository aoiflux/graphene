package main

// backup create, backup verify, export graph, import graph, store migrate.
//
// These are the subcommands that write, and the doctrine at the top of main.go
// says a mutation should be a deliberate decision rather than a convenience. So
// each is argued rather than assumed, and none of them can lose what is already
// there:
//
//   - backup and export write somewhere else entirely and never touch the store.
//   - import refuses any destination that is not empty, so there is nothing
//     there for it to damage.
//   - migrate rewrites the store's own image — Open, Compact, verify, which is
//     what the library does on its own schedule anyway — and a compaction that
//     fails leaves the old image in place, because the new one is renamed in
//     only once it is complete and fsynced.
//
// There is still no repair and no truncate.

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// --- backup create ---

type backupOpts struct{ to string }

var backupCreate = cmd(Command{
	Group: "backup", Name: "create", Aliases: []string{"backup"},
	Usage: "<dir>", Short: "consistent copy into -to",
	Long: "Opened read-only, which is a real constraint and not a convenience: a\n" +
		"backup of a store this process is not writing to cannot be a backup of a\n" +
		"half-written commit. The store's own Backup can run against a live writer\n" +
		"because it pins under the store lock; this cannot take that lock from\n" +
		"outside the process, so it takes the shared one and is refused while a\n" +
		"writer holds the store.",
	Notice: "backup opens the store read-only; a writer must not hold it",
	Open:   OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *backupOpts) {
		fs.StringVar(&o.to, "to", "",
			"directory to write the backup into (required, must not exist or be empty)")
	},
	runBackupCreate)

func runBackupCreate(cx *Context, o *backupOpts) (Result, error) {
	var r Result
	if o.to == "" {
		return r, Usagef("need -to <dir>")
	}

	info, err := cx.Graph().BackupCtx(cx.Ctx, o.to)
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("written to", Str(info.Dir))
	s.Add("commit", Uint(info.CommitSeq))
	s.Add("epoch", Uint(info.Epoch))
	s.Add("files", Int(int64(len(info.Files))))
	s.Add("size", Bytes(info.Bytes()))
	s.Add("earliest restorable commit", Uint(info.ImageCommitSeq))
	r.Notice("verify it with: graphene backup verify %s", o.to)
	return r, nil
}

// --- backup verify ---

var backupVerify = plain(Command{
	Group: "backup", Name: "verify", Aliases: []string{"verify-backup"},
	Usage: "<dir>", Short: "check a backup against its manifest",
	Long: "Touches no store and takes no lock: it opens the copied files, hashes\n" +
		"them, and compares against the manifest. Worth running on a schedule\n" +
		"against an archive — the alternative is finding out during a recovery,\n" +
		"which is the one moment there is no time to react.",
	Open: OpenNone, Tier: CtxAdvisory,
}, runBackupVerify)

func runBackupVerify(cx *Context) (Result, error) {
	var r Result
	if cx.Target == "" {
		return r, Usagef("need a backup directory")
	}

	info, err := graphene.VerifyBackup(cx.Target)
	if err != nil {
		// A backup that does not verify is a verdict, not a crash: the whole
		// point of running this on a schedule is to learn about it early.
		r.Find(SevBroken, "backup.verification_failed", "%v", err)
		r.Verdict = VerdictBroken
		return r, nil
	}

	s := r.Section("")
	s.Add("files", Int(int64(len(info.Files))))
	s.Add("size", Bytes(info.Bytes()))
	s.Add("taken", Str(info.TakenAt.Format("2006-01-02 15:04:05")))
	s.Add("commit", Uint(info.CommitSeq))
	s.Add("restorable back to commit", Uint(info.ImageCommitSeq))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- export graph ---

// exportFormats maps the -format flag to what it writes and where.
var exportFormats = map[string]string{
	"jsonl": "one JSON object per line, to a file",
	"dump":  "the native binary format, to a file",
	"csv":   "a directory of CSV tables",
}

type exportOpts struct {
	format    string
	to        string
	skipProps bool
	gzip      bool
}

var exportGraph = cmd(Command{
	Group: "export", Name: "graph", Aliases: []string{"export"},
	Usage: "<dir>", Short: "the whole graph as jsonl, csv or a native dump",
	Notice: "export opens the store read-only; a writer must not hold it",
	Open:   OpenGraphRO, Streams: true, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *exportOpts) {
		fs.StringVar(&o.format, "format", "jsonl", "jsonl | dump | csv")
		fs.StringVar(&o.to, "to", "",
			"file (jsonl, dump) or directory (csv) to write (required)")
		fs.BoolVar(&o.skipProps, "no-properties", false, "omit indexed property entries")
		fs.BoolVar(&o.gzip, "gzip", false, "compress the output (not with -format csv)")
	},
	runExportGraph)

func runExportGraph(cx *Context, o *exportOpts) (Result, error) {
	var r Result
	if o.to == "" {
		return r, Usagef("need -to <path>")
	}
	if _, ok := exportFormats[o.format]; !ok {
		return r, Usagef("unknown format %q; want one of %s", o.format, formatList())
	}
	if err := checkGzip(o.format, o.gzip); err != nil {
		return r, err
	}

	// Through the same writer `export subgraph` uses, so the two cannot disagree
	// about what a jsonl export is, about refusing to overwrite, or about the
	// order the gzip and file handles are closed in.
	src, closeSrc := exportSource(cx.Graph())
	sum, err := writeExport(o.format, o.to, o.gzip, src,
		bulk.Options{SkipProperties: o.skipProps})
	// The view is released whether or not the export worked, and its error is
	// kept only when there is no better one to report.
	if cerr := closeSrc(); err == nil {
		err = cerr
	}
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("to", Str(o.to))
	s.Add("format", Str(o.format))
	reportCompression(&r, s, o.gzip, o.to)
	s.Add("nodes", Int(int64(sum.Nodes)))
	s.Add("edges", Int(int64(sum.Edges)))
	s.Add("node properties", Int(int64(sum.NodeProperties)))
	s.Add("edge properties", Int(int64(sum.EdgeProperties)))
	if o.skipProps {
		r.Notice("-no-properties was set: the entries were counted and not written. " +
			"A store built from this dump answers no property query.")
	}
	return r, nil
}

// exportSource is the whole graph, as something bulk can stream.
//
// A snapshot rather than the store itself, for two reasons. bulk enumerates
// through store.Scanner when its source offers one and otherwise materialises
// every ID first — one uint64 per node and per edge, all of them, before the
// first record is written — and Scanner is offered over a view and not over a
// live store, for the reason store/scan.go gives. And a view fixes what the
// dump is a dump of: the export reads one graph rather than whatever each read
// happens to find, which matters most here, because a whole-graph export is the
// longest read the tool makes.
//
// A backend that cannot provide a view falls back to the store, which is what
// this command did before and is correct, only heavier. It does not arise
// through the CLI, which opens the disk backend — but failing the export over
// it would be the wrong answer to it.
func exportSource(g *graphene.Graph) (bulk.Source, func() error) {
	snap, err := g.Snapshot()
	if err != nil {
		return g.GraphStore, func() error { return nil }
	}
	return snap, snap.Close
}

// --- import graph ---

type importOpts struct {
	format     string
	from       string
	batch      int
	batchBytes int64
	boundMiB   int64
	compact    bool
	onePass    bool

	// fs is the command's own flag set, kept so the handler can ask which flags
	// the caller actually named rather than comparing values against defaults.
	// A caller who writes -bound 32 means it even though 32 is the default, and
	// a routing decision that could not tell the two apart would ignore them.
	fs *flag.FlagSet
}

// incrementalFlag names the first flag the caller gave that only the
// incremental importer honours, or "" if they gave none.
//
// The one-pass loader has no delta to bound, no batch to size and nothing to
// compact afterwards, so these four flags describe work it does not do. Silently
// ignoring a figure someone typed is the failure worth avoiding here: -bound is
// how an operator holds a ceiling, and an import that took a different path and
// said nothing would look like the bound had been honoured.
func (o *importOpts) incrementalFlag() string {
	if o.fs == nil {
		return ""
	}
	name := ""
	o.fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "batch", "batch-bytes", "bound", "compact":
			if name == "" {
				name = f.Name
			}
		}
	})
	return name
}

// wantsOnePass reports whether -one-pass was named explicitly.
func (o *importOpts) wantsOnePass() bool {
	if o.fs == nil {
		return false
	}
	named := false
	o.fs.Visit(func(f *flag.Flag) {
		if f.Name == "one-pass" {
			named = true
		}
	})
	return named
}

// What the import bounds itself by when nobody says otherwise.
//
// An import with no bound accumulates the whole dump -- records in the delta,
// entries in the property index -- until something folds it into an image, so
// its peak follows the size of the dump rather than the size of the machine.
// docs/MEMORY_MODEL.md section 9.8 measured that shape dying at 1,200,000
// records while holding 1,843.0 MiB under a 2 GiB ceiling, and it died at
// exactly the library's shipped 128 MiB delta bound. The arm that passed was
// 32 MiB *with a reopen after every interim compaction*, at 880.7 MiB and 57.0%
// headroom: the reopen is not an optimisation, it is the half that gives back
// the payload a compaction goes on holding.
//
// So the tool takes the passing configuration and store.DefaultCompactionPolicy
// keeps the compatible one. A caller embedding the library gets what they had;
// an operator running this command against a dump larger than memory gets a
// store instead of a fatal error. -bound 0 restores the unbounded behaviour for
// whoever has measured that it is faster on a dump that fits.
//
// The byte cap is a second unit rather than a smaller -batch: -batch counts
// records, and a thousand records is a few hundred kilobytes of small nodes or
// a gigabyte of large ones. A dump is precisely where the operator does not
// know which they have, because they are importing it to find out.
const (
	defaultImportBoundMiB   = 32
	defaultImportBatchBytes = 8 << 20
)

var importGraph = cmd(Command{
	Group: "import", Name: "graph", Aliases: []string{"import"},
	Usage: "<dir>", Short: "build a store from a dump, into an empty directory",
	Long: "The one subcommand that creates a store. It writes into a directory it\n" +
		"has established is empty, so there is nothing there for it to damage —\n" +
		"which is what keeps \"no repair\" intact while still allowing a write.",
	Open: OpenGraphRW, Creates: true, Tier: CtxAdvisory,
	Before: func(cx *Context) error { return mustBeEmpty(cx.Target) },
},
	func(fs *flag.FlagSet, o *importOpts) {
		o.fs = fs
		fs.StringVar(&o.format, "format", "jsonl", "jsonl | dump | csv")
		fs.StringVar(&o.from, "from", "",
			"file (jsonl, dump) or directory (csv) to read (required)")
		fs.IntVar(&o.batch, "batch", 0, "records committed together (0 = default)")
		fs.Int64Var(&o.batchBytes, "batch-bytes", defaultImportBatchBytes,
			"bytes of records committed together, whichever limit is reached first (0 = no byte cap)")
		fs.Int64Var(&o.boundMiB, "bound", defaultImportBoundMiB,
			"MiB of delta after which the import compacts and reopens mid-import (0 = never)")
		fs.BoolVar(&o.compact, "compact", true,
			"compact once at the end, so the first open of the result is fast")
		fs.BoolVar(&o.onePass, "one-pass", true,
			"build the image in a single pass where the dump allows it (-format dump); "+
				"-batch, -batch-bytes, -bound and -compact do not apply to it")
	},
	runImportGraph)

// runImportGraph builds a store from a dump. The emptiness check is a Before
// hook rather than the first line here, because graphene.Open creates the
// directory: by the time this function runs, the destination is no longer empty
// whatever it held a moment ago.
func runImportGraph(cx *Context, o *importOpts) (Result, error) {
	var r Result
	if o.from == "" {
		return r, Usagef("need -from <path>")
	}
	if _, ok := exportFormats[o.format]; !ok {
		return r, Usagef("unknown format %q; want one of %s", o.format, formatList())
	}

	g := cx.Graph()
	opts := bulk.Options{BatchSize: o.batch, MaxBatchBytes: o.batchBytes}
	interim := 0
	if o.boundMiB > 0 {
		// The other four rules come from the library default so that -bound
		// changes one thing and only one thing. A schedule watching bytes alone
		// would never fire on a dump of small nodes -- MaxDeltaRecords is what
		// covers that shape -- and a store with no image yet fails the ratio
		// rule by construction, which is why the default sets both.
		opts.Compact = store.DefaultCompactionPolicy()
		opts.Compact.MaxDeltaBytes = o.boundMiB << 20
		// Compacts and reopens in one call, and installs the new handle on the
		// Context, so the closer the framework is holding and the `g` below both
		// end up on the handle the import finished with rather than one this
		// hook closed. Returning the new store is what the rest of the dump is
		// written to; see bulk.Options.Reopen for why bulk cannot do this
		// itself.
		opts.Reopen = func(bulk.Dest) (bulk.Dest, error) {
			if err := cx.ReopenGraph(); err != nil {
				return nil, err
			}
			interim++
			return cx.Graph().GraphStore, nil
		}
	}
	var sum bulk.Summary
	var err error

	// The one-pass loader first, where it applies. The destination is empty --
	// the Before hook established that and this subcommand is the only one that
	// creates a store -- so the only remaining question is whether the dump
	// carries each record's index entries, which its header answers.
	//
	// A caller who named one of the incremental flags gets the incremental path,
	// because those flags are only meaningful there. Naming both is a
	// contradiction rather than a precedence question, and is refused.
	incFlag := o.incrementalFlag()
	if o.onePass && o.wantsOnePass() && incFlag != "" {
		return r, Usagef("-one-pass and -%s cannot both be given: "+
			"a one-pass load has no delta to bound, no batch to size and nothing to compact "+
			"afterwards. Drop -%s for the single-pass build, or -one-pass=false for the "+
			"bounded incremental one", incFlag, incFlag)
	}
	if o.onePass && incFlag == "" && o.format == "dump" {
		done, res, err := runImportOnePass(cx, o)
		if err != nil {
			return res, err
		}
		if done {
			return res, nil
		}
	}

	var compressed bool
	if o.format == "csv" {
		sum, err = bulk.ImportCSV(o.from, g.GraphStore, opts)
	} else {
		src, closeSrc, gz, ferr := openDump(o.from)
		if ferr != nil {
			return r, ferr
		}
		compressed = gz
		if o.format == "jsonl" {
			sum, err = bulk.ImportJSONL(src, g.GraphStore, opts)
		} else {
			sum, err = bulk.ImportDump(src, g.GraphStore, opts)
		}
		closeSrc()
	}
	// Any interim reopen replaced the handle, so the one opened at the top of
	// this function may be closed. Everything below reads the current one.
	g = cx.Graph()
	if err != nil {
		// Said plainly, because it is the one thing an operator has to act on:
		// an import is not a transaction and cannot be, so what is in the
		// directory now is however far it got.
		return r, fmt.Errorf("%w\n  the import stopped part way; %s now holds a partial "+
			"graph. Delete it and start again", err, cx.Target)
	}

	s := r.Section("")
	s.Add("into", Str(cx.Target))
	if compressed {
		s.Add("compressed", Str("gzip"))
	}
	s.Add("nodes", Int(int64(sum.Nodes)))
	s.Add("edges", Int(int64(sum.Edges)))
	s.Add("node properties", Int(int64(sum.NodeProperties)))
	s.Add("edge properties", Int(int64(sum.EdgeProperties)))
	if sum.Declarations > 0 {
		s.Add("index declarations re-declared", Int(int64(sum.Declarations)))
	}
	if interim > 0 {
		// Reported because it is the difference between an import that held a
		// ceiling and one that happened to fit, and because each one is a whole
		// rewrite of the image: an operator watching this climb is watching the
		// bound they chose decide how much the import writes.
		s.Add("interim compactions", Int(int64(interim)))
	}
	r.Notice("IDs were reassigned: the imported graph is isomorphic to the original, not identical")

	if o.compact {
		// Everything imported is in the WAL and the delta until something
		// compacts, so without this the first open of the result replays the
		// whole import. Doing it here costs one pass over a store that is
		// already in memory.
		if err := g.CompactCtx(cx.Ctx); err != nil {
			return r, fmt.Errorf("compact after import: %w", err)
		}
		s.Add("compacted", Bool(true))
	}
	return r, nil
}

// runImportOnePass builds the store with graphene.ImportDumpBulk.
//
// Reports done=false when the dump cannot drive a one-pass load, which leaves
// the caller to run the incremental importer over the same file. That is a
// fallback rather than a failure and it is the one place this command chooses
// silently between two very different amounts of writing, so which path ran is
// in the output either way.
//
// # Why the source is a function
//
// A load asks for every node and then, separately, for every edge, so the dump
// is read twice. openDump is called again for the second pass rather than
// rewound, because a gzipped dump is read through a decompressor that cannot
// seek, and re-opening handles both shapes with one rule.
//
// That also means a dump arriving on a pipe cannot take this path. -from names a
// file, so it always can; if that ever changes, the open below fails on the
// second call and the load fails with it, which is the right answer rather than
// a silent half-import.
func runImportOnePass(cx *Context, o *importOpts) (bool, Result, error) {
	var r Result

	compressed := false
	open := func() (io.ReadCloser, error) {
		src, closeSrc, gz, err := openDump(o.from)
		if err != nil {
			return nil, err
		}
		compressed = gz
		return readCloser{Reader: src, close: closeSrc}, nil
	}

	loaded, sum, err := cx.Graph().ImportDumpBulkCtx(cx.Ctx, open, graphene.BulkImportOptions{})
	if errors.Is(err, graphene.ErrDumpNotBulkLoadable) {
		// The receiver survives this refusal by contract, so the incremental
		// path can run on the handle the framework already holds.
		return false, r, nil
	}
	if err != nil {
		// Unlike the incremental path, there is no partial graph to warn about:
		// a load commits or it does not, so the directory is as it was found.
		return true, r, fmt.Errorf("%w\n  nothing was written; %s is as it was", err, cx.Target)
	}
	cx.AdoptGraph(loaded)

	s := r.Section("")
	s.Add("into", Str(cx.Target))
	if compressed {
		s.Add("compressed", Str("gzip"))
	}
	s.Add("nodes", Int(int64(sum.Nodes)))
	s.Add("edges", Int(int64(sum.Edges)))
	s.Add("node properties", Int(int64(sum.NodeProperties)))
	s.Add("edge properties", Int(int64(sum.EdgeProperties)))
	if sum.Declarations > 0 {
		s.Add("index declarations re-declared", Int(int64(sum.Declarations)))
	}
	// The figure an operator is here for. An incremental import of the same dump
	// rewrites the whole image once per -bound worth of records; this writes it
	// once, and reporting the mode is how the difference is visible without
	// reading the manual.
	s.Add("mode", Str("one-pass"))
	s.Add("compacted", Bool(true))
	r.Notice("IDs were reassigned: the imported graph is isomorphic to the original, not identical")
	return true, r, nil
}

// readCloser adapts openDump's reader-plus-closer pair to an io.ReadCloser.
type readCloser struct {
	io.Reader
	close func()
}

func (r readCloser) Close() error {
	r.close()
	return nil
}

// openDump opens a dump for reading and transparently decompresses a gzipped
// one. Sniffed rather than flagged, for two reasons: the magic number is two
// bytes and unambiguous, so a -gzip flag here could only ever be a way to get it
// wrong; and it makes `export graph -gzip` reversible under any filename, which
// matters because the export deliberately does not rename what it was told to
// write. The reader stays buffered either way — the peek needs a buffer, and the
// import readers are better off with one.
func openDump(path string) (io.Reader, func(), bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, false, fmt.Errorf("open %s: %w", path, err)
	}
	br := bufio.NewReader(f)
	// A short read here is not an error worth reporting: a file too small to
	// hold a magic number is too small to hold a dump, and the format reader
	// gives a better account of what is wrong with it than this could.
	if magic, perr := br.Peek(2); perr == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		zr, zerr := gzip.NewReader(br)
		if zerr != nil {
			f.Close()
			return nil, nil, false, fmt.Errorf("read %s as gzip: %w", path, zerr)
		}
		return zr, func() { zr.Close(); f.Close() }, true, nil
	}
	return br, func() { f.Close() }, false, nil
}

// --- store migrate ---

// csrTarget is `store migrate -to`: a container version this build can write.
//
// A flag.Value rather than a plain integer, so a number this build cannot write
// is refused by the flag parser — before the store is opened and before the
// exclusive lock is taken. A handler checking it afterwards would have acquired
// the lock in order to reject a typo, and the check would have had to come
// before its first read to avoid reporting on a migration it was about to
// refuse.
type csrTarget struct{ v uint16 }

// String is what -h prints as the default: empty when unset, because 0 is not a
// version and the flag's own help says what unset means.
func (t *csrTarget) String() string {
	if t == nil || t.v == 0 {
		return ""
	}
	return fmt.Sprintf("v%d", t.v)
}

// Set accepts "8" and "v8" alike. The report prints versions with the v, so
// pasting one back in has to work.
func (t *csrTarget) Set(s string) error {
	n, err := strconv.ParseUint(strings.TrimPrefix(strings.TrimSpace(s), "v"), 10, 16)
	if err != nil {
		return fmt.Errorf("%q is not a format version", s)
	}
	if _, ok := disk.IndexModeForCSRVersion(uint16(n)); !ok {
		return fmt.Errorf("this build writes %s, not v%d", writableVersions(), n)
	}
	t.v = uint16(n)
	return nil
}

// writableVersions spells disk.CSRVersionsWritable for a message: "v8 or v9".
//
// Read from the engine rather than written out here, because a tool naming the
// formats it can produce from its own copy of the list is a tool that will one
// day name the wrong ones.
func writableVersions() string {
	vs := disk.CSRVersionsWritable()
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = fmt.Sprintf("v%d", v)
	}
	if len(parts) < 2 {
		return strings.Join(parts, "")
	}
	return strings.Join(parts[:len(parts)-1], ", ") + " or " + parts[len(parts)-1]
}

// indexEncodingOf describes what version v does with the property index.
//
// The report says it in words because it is the whole of what -to chooses. An
// operator downgrading is trading a store that reads its index off the disk for
// one that holds it in memory, and that is a sentence, not a version number.
func indexEncodingOf(v uint16) string {
	mode, ok := disk.IndexModeForCSRVersion(v)
	switch {
	case !ok:
		return "unknown to this build"
	case mode == disk.IndexMapped:
		return "GPIX and GPIR, which a store can read in place"
	default:
		return "GIDX, which every open rebuilds in the heap"
	}
}

type migrateOpts struct {
	check bool
	to    csrTarget
}

// want is the version this invocation is aiming at.
func (o *migrateOpts) want() uint16 {
	if o.to.v == 0 {
		return disk.CSRVersionCurrent
	}
	return o.to.v
}

// tuneOpen asks for the index encoding the target version implies — see
// openTuner for why this cannot be the handler's job.
//
// Total by construction: csrTarget.Set has already refused every version this
// build cannot write, so the lookup here cannot fail, and an unset -to resolves
// to the current version rather than to no decision at all. The default case is
// therefore the option's zero value being set to itself, which is what makes
// adding the flag no change at all to an invocation that does not use it.
func (o *migrateOpts) tuneOpen(opts *disk.Options) {
	if mode, ok := disk.IndexModeForCSRVersion(o.want()); ok {
		opts.IndexMode = mode
	}
}

var storeMigrate = cmd(Command{
	Group: "store", Name: "migrate", Aliases: []string{"migrate"},
	Usage: "<dir>", Short: "rewrite the image in a given format version",
	Long: "Open, Compact, verify — which is what the library does on its own\n" +
		"schedule. A compaction that fails leaves the old image in place: the new\n" +
		"one is renamed in only once it is complete and fsynced.\n" +
		"\n" +
		"-to picks the format the new image is written in, in either direction. It\n" +
		"is not a second code path: the store is opened with the index mode that\n" +
		"version implies and then compacted once, so a downgrade is exactly as\n" +
		"crash-safe as any other compaction and costs the same one pass. v9 carries\n" +
		"the property index as GPIX and GPIR, which is what lets a store read it\n" +
		"off the disk instead of holding it in memory; v8 carries GIDX and is read\n" +
		"by every build since v8, at that memory. Downgrade to hand a store to an\n" +
		"older build, or to trade the memory back for the lookup latency.",
	Notice: "migrate opens the store for writing and takes the exclusive lock",
	Open:   OpenGraphRW, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *migrateOpts) {
		fs.BoolVar(&o.check, "check", false, "report what would happen and change nothing")
		fs.Var(&o.to, "to", "format version to write: "+writableVersions()+
			" (default: what this build writes)")
	},
	runStoreMigrate)

func runStoreMigrate(cx *Context, o *migrateOpts) (Result, error) {
	var r Result
	dir := cx.Target
	want := o.want()

	// -dry-run asks the same question -check does, so it gets the same answer.
	// What it used to get was the engine's: the framework downgrades the open to
	// read-only under -dry-run, the handler compacted anyway, and the operator
	// was told "store is open read-only" by a store they had asked it not to
	// write to. A global flag the tool advertises, failing on the one command
	// that had a perfectly good account of what it would have done.
	plan := o.check || (cx.Globals != nil && cx.Globals.DryRun)

	// The header, before the graph is touched: the report describes the image on
	// disk rather than what the open made of it, and a store this build could not
	// open still gets an account of why it needs migrating.
	//
	// It does not avoid the lock, whatever this used to claim. The framework opens
	// what the command declared before any handler runs, so -check on a store
	// another process holds fails like every other subcommand — `info`, `csr` and
	// `wal` are the ones that read the files directly. Making -check one of them
	// means letting a flag downgrade the open mode, which is a framework change
	// and not a line in this function.
	info, err := disk.InspectCSR(filepath.Join(dir, "graphene.csr"))
	s := r.Section("")
	switch {
	case err == nil:
		s.Addf("image format", "v%d", info.Version)
		s.Addf("this build writes", "v%d", disk.CSRVersionCurrent)
		s.Addf("target format", "v%d", want)
		s.Add("property index", Str(indexEncodingOf(want)))
		if info.Version == want && plan {
			s.Add("action", Str("nothing to do"))
			return r, nil
		}
	case errors.Is(err, os.ErrNotExist):
		// No image at all is a store that has never compacted — an ordinary
		// state, and one migrate handles by giving it one. Distinguished from a
		// malformed image deliberately: "there is no image" and "the image will
		// not parse" call for very different reactions, and collapsing them
		// would send an operator looking for corruption in a store that has
		// simply never been compacted.
		s.Add("image", Str("none yet; this store has never been compacted"))
		s.Addf("target format", "v%d", want)
		s.Add("property index", Str(indexEncodingOf(want)))
	default:
		return r, fmt.Errorf("read image: %w", err)
	}

	if plan {
		s.Addf("action", "would open the store, compact it as v%d, and verify the result", want)
		return r, nil
	}

	g := cx.Graph()

	// The migration itself. The reader already accepts every version back to v2
	// by additive gating, so opening is the upgrade *in memory*; compacting is
	// what makes it durable, because the image is always written at the current
	// version.
	if err := g.CompactCtx(cx.Ctx); err != nil {
		return r, fmt.Errorf("compact: %w", err)
	}

	// And then check the result rather than announcing it. A migration that
	// reports success without reading back what it wrote is a migration whose
	// failure mode is silence.
	after, err := disk.InspectCSR(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		return r, fmt.Errorf("read migrated image: %w", err)
	}
	if after.Version != want {
		return r, fmt.Errorf("image is v%d after compaction; expected v%d",
			after.Version, want)
	}
	if err := g.VerifyIndexesCtx(cx.Ctx); err != nil {
		return r, fmt.Errorf("indexes do not verify after migration: %w", err)
	}

	s.Addf("migrated to", "v%d", after.Version)
	s.Add("nodes", Int(int64(after.NodeCount)))
	s.Add("edges", Int(int64(after.EdgeCount)))
	s.Add("image size", Bytes(after.FileBytes))
	r.Verdict = VerdictVerified
	return r, nil
}

// mustBeEmpty refuses a destination that already holds anything.
//
// An import into a store that already has records would succeed and produce a
// graph that is the union of two — which is occasionally what someone wants and
// never what they expect from a subcommand called import. Refusing keeps the
// tool's "cannot lose what is already there" property intact.
func mustBeEmpty(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", dir, err)
	}
	if len(entries) > 0 {
		return Usagef("%s is not empty; import creates a store rather than adding to one", dir)
	}
	return nil
}

// formatList names the accepted formats, sorted so the error message does not
// depend on map iteration order.
func formatList() string {
	names := make([]string, 0, len(exportFormats))
	for k := range exportFormats {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
