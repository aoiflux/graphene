// Command graphene inspects a Graphene store from a shell.
//
// The inspection subcommands read graphene.csr and graphene.wal directly and
// never open the store. That is deliberate. Opening replays the log, rebuilds
// indexes, and takes a handle on the WAL — and the moment you most want to look
// at a store is the moment something has gone wrong with it and a live process
// is probably still attached. An inspector that cannot be used then is not much
// of an inspector.
//
// `verify`, `custody` and `anchor` do open the store, because what they check
// includes state that only exists once the log has been replayed. Each says so
// on stderr before it does.
//
// There is no repair, no truncate, no compact. A tool that is safe to point at
// production is worth more than one that can also fix things, and adding a
// mutation should be a deliberate decision rather than a convenience.
//
// `anchor -publish` is the one subcommand that writes, and the exception is
// argued rather than assumed: it only ever appends — a checkpoint and an audit
// entry — and it can neither alter nor remove anything already recorded. It is
// there because publishing a checkpoint is a routine scheduled action, and an
// anchoring scheme that requires writing a program to use is one that does not
// get used.
//
//	graphene info <dir>          summary of the image and the log
//	graphene csr  <dir|file>     CSR header detail
//	graphene wal  <dir|file>     record-by-record log dump
//	graphene verify <dir>        structural index check (opens the store)
//	graphene custody <dir>       chain-of-custody account for one entity (opens the store)
//	graphene anchor <dir>        publish or check a checkpoint (opens the store; -publish writes)
//	graphene redactions <dir>    the ledger of attributed removals
package main

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
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
	case "custody":
		err = cmdCustody(args)
	case "anchor":
		err = cmdAnchor(args)
	case "redactions":
		err = cmdRedactions(args)
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
	fmt.Fprint(os.Stderr, `graphene — inspection of a Graphene store

Usage:
  graphene info    <dir>        summary of the image and the log
  graphene csr     <dir|file>   CSR header detail
  graphene wal     <dir|file>   record-by-record log dump
  graphene verify  <dir>        structural index check (opens the store)
  graphene custody <dir>        account for one entity across every history (opens the store)
  graphene anchor  <dir>        publish or check a checkpoint (opens the store)
  graphene redactions <dir>     ledger of attributed removals: who, when, why

info, csr, wal and redactions read the files directly and are safe to run
against a store another process is using. verify, custody and anchor open the store, which
replays the log and takes a handle on the WAL.

'anchor -publish' is the only subcommand that writes, and it only appends.

Flags:
  wal      -limit N     stop after N records (0 = all, default 50)
           -commits     show only batch commits and their provenance
  csr      -verify      check the stored digest and Merkle roots
  custody  -node ID     the entity to account for (required)
           -anchor HEX  a snapshot root retained outside this system
  redactions -node ID   show only records for one entity
             -edge ID   show only records for one relationship
  anchor   -publish     capture and publish a checkpoint instead of checking one
           -insecure-local-file PATH
                        a local anchor file, which is NOT an anchor: anyone who
                        can rewrite the store can rewrite it. The engine ships no
                        real transport; implement disk.Anchor for one.

Exit status is non-zero only when something is actually broken. An incomplete
account — a store that was never signed, audited, or set to retain history — is
reported as findings and exits zero.
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
	verify := fs.Bool("verify", false, "check the stored digest against the file's contents")
	if err := fs.Parse(args); err != nil {
		return err
	}
	target := fs.Arg(0)
	if target == "" {
		return fmt.Errorf("need a store directory or a graphene.csr path")
	}

	// Verification first: if the file has been altered, that is the answer, and
	// the parsed detail below should be read in that light.
	if *verify {
		status, computed, err := disk.VerifyCSRDigest(target)
		if err != nil {
			return err
		}
		fmt.Printf("digest    %v\n", status)
		if status != disk.DigestAbsent {
			fmt.Printf("computed  %x\n", computed)
		}
		if status == disk.DigestMismatch {
			fmt.Fprintln(os.Stderr,
				"\nthe file does not match the digest it carries — it has changed since it was written.\n"+
					"the digest cannot say whether that was damage or an edit.")
			os.Exit(1)
		}

		// A separate question from the digest: do the Merkle roots actually
		// describe the records sitting next to them?
		switch err := disk.VerifyCSRRoots(target); {
		case err == nil:
			fmt.Println("roots     describe the records")
		case errors.Is(err, disk.ErrNoSnapshotRoots):
			fmt.Println("roots     absent (image predates snapshot roots)")
		default:
			fmt.Printf("roots     FAILED\n")
			fmt.Fprintf(os.Stderr, "\n%v\n", err)
			os.Exit(1)
		}
		fmt.Println()
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
	fmt.Fprintf(w, "property entries\t%d node, %d edge\n", c.PropertyNodeEntries, c.PropertyEdgeEntries)
	if c.CommitSeqHW > 0 {
		fmt.Fprintf(w, "commit seq high-water\t%d\n", c.CommitSeqHW)
	}
	if c.LastCompactUnixNano != 0 {
		fmt.Fprintf(w, "last compacted\t%s\n", formatNano(c.LastCompactUnixNano))
	}
	if c.HasSnapshotRoots {
		fmt.Fprintf(w, "\t\n")
		fmt.Fprintf(w, "snapshot root\t%x\n", c.SnapshotRoot)
		fmt.Fprintf(w, "  node root\t%x\n", c.NodeRoot)
		fmt.Fprintf(w, "  edge root\t%x\n", c.EdgeRoot)
		fmt.Fprintf(w, "  index root\t%x\n", c.IndexRoot)
		if c.PrevSnapshotRoot != [32]byte{} {
			fmt.Fprintf(w, "  replaces\t%x\n", c.PrevSnapshotRoot)
		} else {
			fmt.Fprintf(w, "  replaces\t(first compaction)\n")
		}
		fmt.Fprintf(w, "\t\n")
	}
	if c.HasAttestation {
		fmt.Fprintf(w, "attestation\t%x\n", c.AttestationID)
		fmt.Fprintf(w, "  actor\t%d\n", c.AttestActorID)
		fmt.Fprintf(w, "  signed by key\t%d\n", c.AttestKeyID)
		fmt.Fprintf(w, "  at\t%s\n", formatNano(c.AttestUnixNano))
		if c.AttestPrev != [16]byte{} {
			fmt.Fprintf(w, "  follows\t%x\n", c.AttestPrev)
		} else {
			fmt.Fprintf(w, "  follows\t(first attestation)\n")
		}
		fmt.Fprintf(w, "\t\n")
	}
	if len(c.Sections) > 0 {
		fmt.Fprintf(w, "sections\t%d\n", len(c.Sections))
		for _, s := range c.Sections {
			kind := "optional"
			if s.Critical {
				kind = "CRITICAL"
			}
			note := ""
			if !s.Known {
				note = "  <- not understood by this build"
			}
			fmt.Fprintf(w, "  %s\t%s, %d bytes at %d%s\n", s.Magic, kind, s.Length, s.Offset, note)
		}
	}
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

// --- custody ---

func cmdCustody(args []string) error {
	fs := flag.NewFlagSet("custody", flag.ExitOnError)
	node := fs.Uint64("node", 0, "node ID to account for")
	anchor := fs.String("anchor", "", "hex snapshot root retained outside this system")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}
	if *node == 0 {
		return fmt.Errorf("need -node")
	}

	// Opens the store: the report walks in-memory state as well as the files.
	fmt.Fprintln(os.Stderr, "custody opens the store; do not run it against one another process is writing")

	s, err := disk.Open(dir)
	if err != nil {
		return err
	}
	defer s.Close()

	// No verifier: this command has no key material, so signature-dependent
	// layers report as unchecked rather than verified. A caller holding a public
	// key gets a stronger answer from the API.
	var report disk.CustodyReport
	if *anchor != "" {
		var h merkle.Hash
		raw, derr := hex.DecodeString(*anchor)
		if derr != nil || len(raw) != len(h) {
			return fmt.Errorf("-anchor must be %d hex bytes", len(h))
		}
		copy(h[:], raw)
		report, err = s.CustodyForAnchored(store.NodeID(*node), nil, h)
	} else {
		report, err = s.CustodyFor(store.NodeID(*node), nil)
	}
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "node\t%d\n", report.NodeID)
	fmt.Fprintf(w, "known to store\t%v\n", report.Live)
	fmt.Fprintf(w, "in snapshot\t%v\n", report.InSnapshot)
	if report.InSnapshot {
		fmt.Fprintf(w, "snapshot root\t%x\n", report.SnapshotRoot)
	}
	fmt.Fprintf(w, "attested\t%v (verified: %v)\n", report.Attested, report.AttestationVerified)
	fmt.Fprintf(w, "segments walked\t%d\n", report.SegmentsChecked)
	fmt.Fprintf(w, "audit entries\t%d (%d compactions)\n", report.AuditEntriesWalked, report.CompactionsRecorded)
	w.Flush()

	fmt.Printf("\n%s\n", report.Summary())
	if len(report.Gaps) > 0 {
		fmt.Println()
		for _, g := range report.Gaps {
			fmt.Printf("  %s\n", g)
		}
	}

	// Exit non-zero only for an actual break. An incomplete account is a
	// finding, not a failure — most stores are legitimately not fully
	// provisioned, and exiting 1 on that would make the command useless in a
	// script.
	if report.Broken() {
		os.Exit(1)
	}
	return nil
}

// cmdAnchor publishes a checkpoint to an anchor, or checks the store against
// one.
//
// The only anchor this command can offer is disk.InsecureLocalAnchor, which is
// not an anchor — a file on the same machine is reachable by anyone who can
// rewrite the store. The flag is named to say so, because a command that made
// real anchoring look like a one-liner would produce stores that believe they
// are witnessed and are not. A deployment with a genuine anchor implements the
// disk.Anchor interface and calls the API.
func cmdAnchor(args []string) error {
	fs := flag.NewFlagSet("anchor", flag.ExitOnError)
	publish := fs.Bool("publish", false, "capture and publish a checkpoint instead of verifying")
	insecure := fs.String("insecure-local-file", "",
		"path to a local anchor file, which is NOT an anchor; must be outside the store directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}
	if *insecure == "" {
		return fmt.Errorf("need -insecure-local-file: this command ships no real anchor transport.\n" +
			"See disk.Anchor — a genuine anchor lives where whoever can rewrite the store cannot reach it")
	}

	anchor, err := disk.NewInsecureLocalAnchor(*insecure, dir)
	if err != nil {
		return err
	}

	fmt.Fprintln(os.Stderr, "anchor opens the store; do not run it against one another process is writing")
	fmt.Fprintln(os.Stderr, "warning: a local file is not an external witness; this proves nothing against "+
		"an adversary who can write to this machine")

	s, err := disk.Open(dir)
	if err != nil {
		return err
	}
	defer s.Close()

	if *publish {
		c, rec, perr := s.PublishCheckpoint(anchor)
		if perr != nil {
			return perr
		}
		fmt.Println(c)
		fmt.Printf("published as %q at %s\n", rec.Ref,
			time.Unix(0, rec.UnixNano).UTC().Format(time.RFC3339))
		return nil
	}

	audit, err := s.VerifyAgainstAnchor(anchor)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "checkpoints\t%d local, %d published\n", len(audit.Checkpoints), len(audit.Published))
	fmt.Fprintf(w, "confirmed\t%d\n", audit.Matched)
	if audit.LastAnchored != nil {
		fmt.Fprintf(w, "last witnessed\tcheckpoint %d at %s\n", audit.LastAnchored.Seq,
			time.Unix(0, audit.LastAnchoredAt).UTC().Format(time.RFC3339))
		fmt.Fprintf(w, "store unchanged since\t%v\n", audit.CurrentMatchesLast)
	}
	w.Flush()

	fmt.Printf("\n%s\n", audit.Summary())
	if len(audit.Gaps) > 0 {
		fmt.Println()
		for _, g := range audit.Gaps {
			fmt.Printf("  %s\n", g)
		}
	}

	// Same rule as custody: non-zero for a break, not for an unanchored window.
	if audit.Broken() {
		os.Exit(1)
	}
	return nil
}

// cmdRedactions prints the redaction ledger.
//
// Reads the file directly rather than opening the store, like info/csr/wal. The
// ledger is a standalone hash-chained file with no dependency on replayed state,
// and the moment you most want to read it is the moment someone is asking what
// was removed from a store that may well be in use.
func cmdRedactions(args []string) error {
	fs := flag.NewFlagSet("redactions", flag.ExitOnError)
	node := fs.Uint64("node", 0, "show only records for this node ID")
	edge := fs.Uint64("edge", 0, "show only records for this edge ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir := fs.Arg(0)
	if dir == "" {
		return fmt.Errorf("need a store directory")
	}

	records, err := disk.ReadRedactions(dir)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		fmt.Println("no redactions recorded")
		return nil
	}

	// Verified over the whole ledger before anything is filtered: a chain break
	// anywhere makes every record suspect, including the ones asked for.
	chainErr := disk.VerifyRedactionChain(records, nil)

	shown := 0
	for _, r := range records {
		// A node filter must not match an edge record, which leaves NodeID zero.
		if *node != 0 && (r.EdgeID != 0 || uint64(r.NodeID) != *node) {
			continue
		}
		if *edge != 0 && uint64(r.EdgeID) != *edge {
			continue
		}
		shown++
		fmt.Println(r)
		fmt.Printf("    scope        %s\n", r.Scope)
		fmt.Printf("    version hash %x\n", r.VersionHash)
		if r.SurvivingHash != (merkle.Hash{}) {
			fmt.Printf("    survives as  %x\n", r.SurvivingHash)
		}
		if r.PriorPropertiesHash != (merkle.Hash{}) {
			fmt.Printf("    prior props  %x\n", r.PriorPropertiesHash)
		}
		if len(r.CascadedEdges) > 0 {
			fmt.Printf("    edges        %v", r.CascadedEdges)
			if len(r.CascadedHashes) != len(r.CascadedEdges) {
				// Worth saying: without hashes these edges are named but not
				// identified, so the image carries no tombstone for them.
				fmt.Printf("  (unidentified: no tombstones in the image)")
			}
			fmt.Println()
		}
		if len(r.Signature) > 0 {
			fmt.Printf("    signed by    key %d\n", r.KeyID)
		} else {
			fmt.Printf("    unsigned\n")
		}
	}

	noun := "records"
	if len(records) == 1 {
		noun = "record"
	}
	// The chain covers the whole ledger, so the count reported alongside its
	// verdict has to be the whole ledger too — saying "2 records, chain intact"
	// after a filter would attach the verdict to the wrong set.
	if *node != 0 || *edge != 0 {
		fmt.Printf("\n%d shown of ", shown)
	} else {
		fmt.Print("\n")
	}
	fmt.Printf("%d %s in the ledger", len(records), noun)
	if chainErr != nil {
		// No key material here, so signatures are unchecked; the hash chain is
		// not, and a break in it is the loud case.
		fmt.Printf("\nCHAIN BROKEN: %v\n", chainErr)
		os.Exit(1)
	}
	fmt.Println(", hash chain intact (signatures unchecked: no key supplied)")
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
