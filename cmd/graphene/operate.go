package main

// The subcommands that write, and the policy change they represent.
//
// The tool's stated position is "no repair, no truncate, no compact": a tool
// that is safe to point at production is worth more than one that can also fix
// things, and every mutation should be a deliberate decision rather than a
// convenience. Five subcommands here cross that line, and each is argued
// separately rather than admitted as a group.
//
//	backup        reads the store, writes somewhere else entirely
//	verify-backup reads a backup, writes nothing
//	export        reads the store, writes somewhere else entirely
//	import        writes a graph into an empty directory it creates
//	migrate       rewrites the store's own image, in place
//
// The first four never touch the store they are pointed at. `backup` and
// `export` take a read-only lock on the source and write to a destination that
// must not already exist; `import` refuses anything but an empty or absent
// directory. None of them can lose data that is already there, which is the
// property the policy was protecting.
//
// `migrate` is the real exception. It opens the store for writing, compacts,
// and leaves a new image behind. It is here because the alternative is worse: a
// store written by an older build reads fine and stays at its old format until
// something compacts it, so an operator upgrading a fleet has no way to say
// "bring these files up to date" except by writing a program. A migration that
// requires writing a program is a migration that does not happen, and a store
// that never gets one is a store still being read by compatibility paths years
// after they stopped being tested.
//
// What makes it acceptable is that it adds nothing new. `migrate` is Open,
// Compact, verify — three operations the library already performs on its own
// schedule, in an order the engine already takes. It cannot truncate, cannot
// repair, and cannot remove anything: a compaction that fails leaves the old
// image in place, because the new one is written to a temporary name and
// renamed only after it is complete and fsynced.

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/disk"
)

// --- backup ---

func cmdBackup(args []string) error {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	to := fs.String("to", "", "directory to write the backup into (required, must not exist or be empty)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}
	if *to == "" {
		return fmt.Errorf("need -to <dir>")
	}

	// Opened read-only, which is a real constraint and not a convenience: a
	// backup of a store this process is not writing to cannot be a backup of a
	// half-written commit. The store's own Backup can run against a live writer
	// because it pins under the store lock; this cannot take that lock from
	// outside the process, so it takes the shared one instead and is refused
	// while a writer holds the store.
	fmt.Fprintln(os.Stderr, "backup opens the store read-only; a writer must not hold it")

	g, err := graphene.OpenReadOnly(dir)
	if err != nil {
		return err
	}
	defer g.Close()

	info, err := g.Backup(*to)
	if err != nil {
		return err
	}
	fmt.Printf("backup written to %s\n", info.Dir)
	fmt.Printf("  commit %d, epoch %d, %d files, %s\n",
		info.CommitSeq, info.Epoch, len(info.Files), humanBytes(info.Bytes()))
	fmt.Printf("  earliest restorable commit: %d\n", info.ImageCommitSeq)
	fmt.Fprintln(os.Stderr, "verify it with: graphene verify-backup "+*to)
	return nil
}

// --- verify-backup ---

func cmdVerifyBackup(args []string) error {
	fs := flag.NewFlagSet("verify-backup", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a backup directory")
	}

	// Touches no store and takes no lock: it opens the copied files, hashes
	// them, and compares against the manifest. Worth running on a schedule
	// against an archive — the alternative is finding out during a recovery,
	// which is the one moment there is no time to react.
	info, err := graphene.VerifyBackup(dir)
	if err != nil {
		return err
	}
	fmt.Printf("backup verified — %d files, %s, taken %s\n",
		len(info.Files), humanBytes(info.Bytes()), info.TakenAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("  commit %d; restorable back to commit %d\n", info.CommitSeq, info.ImageCommitSeq)
	return nil
}

// --- export ---

// exportFormats maps the -format flag to what it writes and where.
var exportFormats = map[string]string{
	"jsonl": "one JSON object per line, to a file",
	"dump":  "the native binary format, to a file",
	"csv":   "a directory of CSV tables",
}

func cmdExport(args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	format := fs.String("format", "jsonl", "jsonl | dump | csv")
	to := fs.String("to", "", "file (jsonl, dump) or directory (csv) to write (required)")
	skipProps := fs.Bool("no-properties", false, "omit indexed property entries")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}
	if *to == "" {
		return fmt.Errorf("need -to <path>")
	}
	if _, ok := exportFormats[*format]; !ok {
		return fmt.Errorf("unknown format %q; want one of %s", *format, formatList())
	}

	fmt.Fprintln(os.Stderr, "export opens the store read-only; a writer must not hold it")
	g, err := graphene.OpenReadOnly(dir)
	if err != nil {
		return err
	}
	defer g.Close()

	opts := bulk.Options{SkipProperties: *skipProps}
	var sum bulk.Summary

	if *format == "csv" {
		if sum, err = bulk.ExportCSV(*to, g.GraphStore, opts); err != nil {
			return err
		}
	} else {
		// O_EXCL: refusing to overwrite is the same rule ExportCSV applies to a
		// directory that already holds a manifest, and for the same reason —
		// a half-overwritten export is worse than no export.
		f, ferr := os.OpenFile(*to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if ferr != nil {
			return fmt.Errorf("create %s: %w", *to, ferr)
		}
		if *format == "jsonl" {
			sum, err = bulk.ExportJSONL(f, g.GraphStore, opts)
		} else {
			sum, err = bulk.ExportDump(f, g.GraphStore, opts)
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}

	fmt.Printf("exported %d nodes, %d edges, %d node properties, %d edge properties to %s\n",
		sum.Nodes, sum.Edges, sum.NodeProperties, sum.EdgeProperties, *to)
	if *skipProps {
		fmt.Fprintln(os.Stderr,
			"  -no-properties was set: the entries were counted and not written. "+
				"A store built from this dump answers no property query.")
	}
	return nil
}

// --- import ---

func cmdImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	format := fs.String("format", "jsonl", "jsonl | dump | csv")
	from := fs.String("from", "", "file (jsonl, dump) or directory (csv) to read (required)")
	batch := fs.Int("batch", 0, "records committed together (0 = default)")
	compact := fs.Bool("compact", true, "compact once at the end, so the first open of the result is fast")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a destination store directory")
	}
	if *from == "" {
		return fmt.Errorf("need -from <path>")
	}
	if _, ok := exportFormats[*format]; !ok {
		return fmt.Errorf("unknown format %q; want one of %s", *format, formatList())
	}
	if err := mustBeEmpty(dir); err != nil {
		return err
	}

	// The one subcommand that creates a store. It writes into a directory it has
	// established is empty, so there is nothing there for it to damage — which
	// is what keeps "no repair" intact while still allowing a write.
	g, err := graphene.Open(dir)
	if err != nil {
		return err
	}
	defer g.Close()

	opts := bulk.Options{BatchSize: *batch}
	var sum bulk.Summary

	if *format == "csv" {
		sum, err = bulk.ImportCSV(*from, g.GraphStore, opts)
	} else {
		f, ferr := os.Open(*from)
		if ferr != nil {
			return fmt.Errorf("open %s: %w", *from, ferr)
		}
		if *format == "jsonl" {
			sum, err = bulk.ImportJSONL(f, g.GraphStore, opts)
		} else {
			sum, err = bulk.ImportDump(f, g.GraphStore, opts)
		}
		f.Close()
	}
	if err != nil {
		// Said plainly, because it is the one thing an operator has to act on:
		// an import is not a transaction and cannot be, so what is in the
		// directory now is however far it got.
		fmt.Fprintf(os.Stderr,
			"  the import stopped part way; %s now holds a partial graph. Delete it and start again.\n", dir)
		return err
	}

	fmt.Printf("imported %d nodes, %d edges, %d node properties, %d edge properties into %s\n",
		sum.Nodes, sum.Edges, sum.NodeProperties, sum.EdgeProperties, dir)
	if sum.Declarations > 0 {
		fmt.Printf("  re-declared %d index declarations from the dump\n", sum.Declarations)
	}
	fmt.Fprintln(os.Stderr, "  IDs were reassigned: the imported graph is isomorphic to the original, not identical")

	if *compact {
		// Everything imported is in the WAL and the delta until something
		// compacts, so without this the first open of the result replays the
		// whole import. Doing it here costs one pass over a store that is
		// already in memory.
		if err := g.Compact(); err != nil {
			return fmt.Errorf("compact after import: %w", err)
		}
		fmt.Println("  compacted")
	}
	return nil
}

// --- migrate ---

func cmdMigrate(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	check := fs.Bool("check", false, "report what would happen and change nothing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}

	// -check first, and without opening the store: the header says what version
	// the image is at, and an operator surveying a fleet should not have to take
	// the exclusive lock on every store to find out which ones need anything.
	info, err := disk.InspectCSR(filepath.Join(dir, "graphene.csr"))
	switch {
	case err == nil:
		fmt.Printf("image is format v%d; this build writes v%d\n", info.Version, disk.CSRVersionCurrent)
		if info.Version == disk.CSRVersionCurrent && *check {
			fmt.Println("nothing to do")
			return nil
		}
	case errors.Is(err, os.ErrNotExist):
		// No image at all is a store that has never compacted — an ordinary
		// state, and one migrate handles by giving it one. Distinguished from a
		// malformed image deliberately: "there is no image" and "the image will
		// not parse" call for very different reactions, and collapsing them
		// would send an operator looking for corruption in a store that has
		// simply never been compacted.
		fmt.Println("no image yet; this store has never been compacted")
	default:
		return fmt.Errorf("read image: %w", err)
	}
	if *check {
		fmt.Println("migrate would open the store, compact it, and verify the result")
		return nil
	}

	// Writes, so it takes the exclusive lock and says so before it does.
	fmt.Fprintln(os.Stderr, "migrate opens the store for writing and takes the exclusive lock")

	g, err := graphene.Open(dir)
	if err != nil {
		return err
	}
	defer g.Close()

	// The migration itself. The reader already accepts every version back to v2
	// by additive gating, so opening is the upgrade *in memory*; compacting is
	// what makes it durable, because the image is always written at the current
	// version.
	if err := g.Compact(); err != nil {
		return fmt.Errorf("compact: %w", err)
	}

	// And then check the result rather than announcing it. A migration that
	// reports success without reading back what it wrote is a migration whose
	// failure mode is silence.
	after, err := disk.InspectCSR(filepath.Join(dir, "graphene.csr"))
	if err != nil {
		return fmt.Errorf("read migrated image: %w", err)
	}
	if after.Version != disk.CSRVersionCurrent {
		return fmt.Errorf("image is still v%d after compaction; expected v%d",
			after.Version, disk.CSRVersionCurrent)
	}
	if err := g.VerifyIndexes(); err != nil {
		return fmt.Errorf("indexes do not verify after migration: %w", err)
	}

	fmt.Printf("migrated to v%d — %d nodes, %d edges, %s\n",
		after.Version, after.NodeCount, after.EdgeCount, humanBytes(after.FileBytes))
	return nil
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
		return fmt.Errorf("%s is not empty; import creates a store rather than adding to one", dir)
	}
	return nil
}

func formatList() string {
	names := make([]string, 0, len(exportFormats))
	for k := range exportFormats {
		names = append(names, k)
	}
	// Sorted so the error message does not depend on map iteration order.
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return strings.Join(names, ", ")
}
