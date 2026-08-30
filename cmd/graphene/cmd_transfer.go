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
	"strings"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/disk"
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
	sum, err := writeExport(o.format, o.to, o.gzip, cx.Graph().GraphStore,
		bulk.Options{SkipProperties: o.skipProps})
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

// --- import graph ---

type importOpts struct {
	format  string
	from    string
	batch   int
	compact bool
}

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
		fs.StringVar(&o.format, "format", "jsonl", "jsonl | dump | csv")
		fs.StringVar(&o.from, "from", "",
			"file (jsonl, dump) or directory (csv) to read (required)")
		fs.IntVar(&o.batch, "batch", 0, "records committed together (0 = default)")
		fs.BoolVar(&o.compact, "compact", true,
			"compact once at the end, so the first open of the result is fast")
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
	opts := bulk.Options{BatchSize: o.batch}
	var sum bulk.Summary
	var err error

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

type migrateOpts struct{ check bool }

var storeMigrate = cmd(Command{
	Group: "store", Name: "migrate", Aliases: []string{"migrate"},
	Usage: "<dir>", Short: "bring the image up to the current format",
	Long: "Open, Compact, verify — which is what the library does on its own\n" +
		"schedule. A compaction that fails leaves the old image in place: the new\n" +
		"one is renamed in only once it is complete and fsynced.",
	Notice: "migrate opens the store for writing and takes the exclusive lock",
	Open:   OpenGraphRW, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *migrateOpts) {
		fs.BoolVar(&o.check, "check", false, "report what would happen and change nothing")
	},
	runStoreMigrate)

func runStoreMigrate(cx *Context, o *migrateOpts) (Result, error) {
	var r Result
	dir := cx.Target

	// -check reads the header rather than opening the store: an operator
	// surveying a fleet should not have to take the exclusive lock on every one
	// of them to find out which need anything.
	info, err := disk.InspectCSR(filepath.Join(dir, "graphene.csr"))
	s := r.Section("")
	switch {
	case err == nil:
		s.Addf("image format", "v%d", info.Version)
		s.Addf("this build writes", "v%d", disk.CSRVersionCurrent)
		if info.Version == disk.CSRVersionCurrent && o.check {
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
	default:
		return r, fmt.Errorf("read image: %w", err)
	}

	if o.check {
		s.Add("action", Str("would open the store, compact it, and verify the result"))
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
	if after.Version != disk.CSRVersionCurrent {
		return r, fmt.Errorf("image is still v%d after compaction; expected v%d",
			after.Version, disk.CSRVersionCurrent)
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
