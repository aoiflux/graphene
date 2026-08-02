// Command graphene inspects a Graphene store from a shell.
//
// Every subcommand except `verify` reads graphene.csr and graphene.wal directly
// and never opens the store. That is deliberate. Opening replays the log,
// rebuilds indexes, and takes a handle on the WAL — and the moment you most want
// to look at a store is the moment something has gone wrong with it and a live
// process is probably still attached. An inspector that cannot be used then is
// not much of an inspector.
//
// Nothing here writes. There is no repair, no truncate, no compact, and adding
// one should be a deliberate decision rather than a convenience: a tool that is
// safe to point at production is worth more than one that can also fix things.
//
//	graphene info <dir>          summary of the image and the log
//	graphene csr  <dir|file>     CSR header detail
//	graphene wal  <dir|file>     record-by-record log dump
//	graphene verify <dir>        structural index check (this one opens the store)
package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "info":
		err = cmdInfo(args)
	case "csr":
		err = cmdCSR(args)
	case "wal":
		err = cmdWAL(args)
	case "verify":
		err = cmdVerify(args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "graphene: unknown subcommand %q\n\n", cmd)
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "graphene %s: %v\n", cmd, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `graphene — read-only inspection of a Graphene store

Usage:
  graphene info   <dir>         summary of the image and the log
  graphene csr    <dir|file>    CSR header detail
  graphene wal    <dir|file>    record-by-record log dump
  graphene verify <dir>         structural index check (opens the store)

Only 'verify' opens the store; everything else reads the files directly and is
safe to run against a store another process is using. Nothing here writes.

Flags:
  wal  -limit N     stop after N records (0 = all, default 50)
       -commits     show only batch commits and their provenance
`)
}

// --- info ---

func cmdInfo(args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintf(w, "store\t%s\n", dir)

	csr, csrErr := disk.InspectCSR(dir)
	fmt.Fprintln(w, "\t")
	fmt.Fprintln(w, "CSR image\t")
	switch {
	case csrErr != nil && os.IsNotExist(underlying(csrErr)):
		fmt.Fprintf(w, "  status\tabsent (store has never been compacted)\n")
	case csrErr != nil:
		fmt.Fprintf(w, "  status\tUNREADABLE: %v\n", csrErr)
	default:
		fmt.Fprintf(w, "  version\tv%d\n", csr.Version)
		fmt.Fprintf(w, "  size\t%s\n", humanBytes(csr.FileBytes))
		fmt.Fprintf(w, "  nodes\t%d\n", csr.NodeCount)
		fmt.Fprintf(w, "  edges\t%d\n", csr.EdgeCount)
		fmt.Fprintf(w, "  property entries\t%d node, %d edge\n",
			csr.PropertyNodeEntries, csr.PropertyEdgeEntries)
		fmt.Fprintf(w, "  sequence high-water\tnode %d, edge %d\n", csr.NodeSeqHW, csr.EdgeSeqHW)
		fmt.Fprintf(w, "  highest ID\tnode %d, edge %d%s\n",
			csr.MaxNodeID, csr.MaxEdgeID, sparsityNote(csr))
	}

	wal, walErr := disk.InspectWAL(dir)
	fmt.Fprintln(w, "\t")
	fmt.Fprintln(w, "Write-ahead log\t")
	switch {
	case walErr != nil && os.IsNotExist(underlying(walErr)):
		fmt.Fprintf(w, "  status\tabsent\n")
	case walErr != nil:
		fmt.Fprintf(w, "  status\tUNREADABLE: %v\n", walErr)
	default:
		fmt.Fprintf(w, "  size\t%s\n", humanBytes(wal.FileBytes))
		fmt.Fprintf(w, "  records\t%d\n", len(wal.Records))
		fmt.Fprintf(w, "  commits\t%d\n", len(wal.Commits))
		if wal.Checkpointed {
			fmt.Fprintf(w, "  checkpoint\tpresent — replay stops there\n")
		}
		if wal.Truncated {
			fmt.Fprintf(w, "  tail\tTRUNCATED at byte %d — records past this point are ignored on replay\n",
				wal.TruncatedAt)
		}
		if wal.OpenBatch {
			fmt.Fprintf(w, "  open batch\tYES — a batch began and never committed; replay discards it\n")
		}
		if n := attributedCommits(wal); n > 0 {
			fmt.Fprintf(w, "  attributed\t%d of %d commits carry an actor\n", n, len(wal.Commits))
		}
		if last, ok := lastCommit(wal); ok {
			fmt.Fprintf(w, "  last commit\tseq %d at %s\n", last.CommitSeq, formatNano(last.UnixNano))
		}
	}

	// The two files together are what an operator actually needs to judge.
	if csrErr == nil && walErr == nil {
		fmt.Fprintln(w, "\t")
		fmt.Fprintf(w, "unreplayed log\t%s of records to apply on next open\n", humanBytes(wal.FileBytes))
	}
	return nil
}

// sparsityNote flags an ID space far larger than the record count, which is what
// makes an open expensive: the CSR indexes its arrays by ID, not by count.
func sparsityNote(c disk.CSRInfo) string {
	if c.NodeCount == 0 || c.MaxNodeID == 0 {
		return ""
	}
	ratio := float64(c.MaxNodeID) / float64(c.NodeCount)
	if ratio < 4 {
		return ""
	}
	return fmt.Sprintf("  (node IDs are %.0fx sparser than the count — arrays are sized by ID)", ratio)
}

// --- csr ---

func cmdCSR(args []string) error {
	fs := flag.NewFlagSet("csr", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	target := fs.Arg(0)
	if target == "" {
		return fmt.Errorf("need a store directory or a graphene.csr path")
	}

	c, err := disk.InspectCSR(target)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()
	fmt.Fprintf(w, "file\t%s\n", c.Path)
	fmt.Fprintf(w, "size\t%d bytes\n", c.FileBytes)
	fmt.Fprintf(w, "format\tv%d\n", c.Version)
	fmt.Fprintf(w, "nodes\t%d\n", c.NodeCount)
	fmt.Fprintf(w, "edges\t%d\n", c.EdgeCount)
	fmt.Fprintf(w, "node seq high-water\t%d\n", c.NodeSeqHW)
	fmt.Fprintf(w, "edge seq high-water\t%d\n", c.EdgeSeqHW)
	fmt.Fprintf(w, "highest node ID\t%d\n", c.MaxNodeID)
	fmt.Fprintf(w, "highest edge ID\t%d\n", c.MaxEdgeID)
	fmt.Fprintf(w, "index section at\tbyte %d\n", c.IndexOffset)
	fmt.Fprintf(w, "property entries\t%d node, %d edge\n", c.PropertyNodeEntries, c.PropertyEdgeEntries)
	return nil
}

// --- wal ---

func cmdWAL(args []string) error {
	fs := flag.NewFlagSet("wal", flag.ExitOnError)
	limit := fs.Int("limit", 50, "stop after this many records (0 = all)")
	commitsOnly := fs.Bool("commits", false, "show only batch commits and their provenance")
	if err := fs.Parse(args); err != nil {
		return err
	}
	target := fs.Arg(0)
	if target == "" {
		return fmt.Errorf("need a store directory or a graphene.wal path")
	}

	info, err := disk.InspectWAL(target)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintf(w, "file\t%s (%s)\n", info.Path, humanBytes(info.FileBytes))
	fmt.Fprintf(w, "records\t%d in %d commits\n\n", len(info.Records), len(info.Commits))

	if *commitsOnly {
		fmt.Fprintln(w, "OFFSET\tSEQ\tWHEN\tACTOR\tRECORDS\tVALID")
		for _, c := range info.Commits {
			seq, when, actor := "-", "-", "-"
			if c.HasDetail {
				seq = fmt.Sprint(c.CommitSeq)
				when = formatNano(c.UnixNano)
				actor = fmt.Sprint(c.ActorID)
				if c.ActorID == 0 {
					actor = "unattributed"
				}
			}
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%v\n",
				c.Offset, seq, when, actor, c.RecordsIn, c.Validated)
		}
	} else {
		fmt.Fprintln(w, "OFFSET\tTYPE\tLEN\tCRC\tIN BATCH")
		for i, r := range info.Records {
			if *limit > 0 && i >= *limit {
				fmt.Fprintf(w, "…\t(%d more records; -limit 0 for all)\t\t\t\n", len(info.Records)-i)
				break
			}
			crc := "ok"
			if !r.CRCValid {
				crc = "BAD"
			}
			fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%v\n", r.Offset, r.TypeName, r.Length, crc, r.InBatch)
		}
	}

	if info.Truncated {
		fmt.Fprintf(w, "\ntail truncated at byte %d — replay stops here\n", info.TruncatedAt)
	}
	if info.OpenBatch {
		fmt.Fprint(w, "\na batch began and never committed — replay discards it\n")
	}
	return nil
}

// --- verify ---

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}

	// The one subcommand that opens the store, because the check it runs is over
	// reconstructed in-memory indexes rather than over the files. That means it
	// replays the log and contends for the WAL handle — do not point it at a
	// store another process is writing.
	fmt.Fprintln(os.Stderr, "verify opens the store; do not run it against one another process is writing")

	g, err := graphene.Open(dir)
	if err != nil {
		return err
	}
	defer g.Close()

	if err := g.VerifyIndexes(); err != nil {
		return fmt.Errorf("index check failed: %w", err)
	}

	st, err := g.Stats()
	if err != nil {
		return err
	}
	fmt.Printf("indexes consistent — %d nodes, %d edges\n", st.NodeCount, st.EdgeCount)
	if st.HasStorage {
		fmt.Printf("delta holds %d records; log is %s\n",
			st.Storage.DeltaRecords(), humanBytes(st.Storage.WALBytes))
	}
	return nil
}

// --- helpers ---

func attributedCommits(w disk.WALInfo) int {
	n := 0
	for _, c := range w.Commits {
		if c.HasDetail && c.ActorID != 0 {
			n++
		}
	}
	return n
}

func lastCommit(w disk.WALInfo) (disk.WALCommitInfo, bool) {
	for i := len(w.Commits) - 1; i >= 0; i-- {
		if w.Commits[i].HasDetail {
			return w.Commits[i], true
		}
	}
	return disk.WALCommitInfo{}, false
}

func formatNano(ns int64) string {
	if ns == 0 {
		return "-"
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// underlying unwraps to the innermost error so os.IsNotExist can see it.
func underlying(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		next := u.Unwrap()
		if next == nil {
			return err
		}
		err = next
	}
}
