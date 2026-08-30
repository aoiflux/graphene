package main

// export bundle.
//
// For handing to somebody who will not have the store: the snapshot roots, the
// checkpoint chain, a slice of the audit log, the governance ledgers verbatim,
// and an exported inclusion proof per node named.
//
// What it is not is a copy of the graph — `backup create` is that, and
// `export graph` is the portable form. This is the material somebody needs to
// check claims about a store without being given the store.
//
// The ledgers go in verbatim rather than re-encoded, because their hash chains
// only verify over the bytes they were written as. The manifest is written last
// so it can name everything and read first by anyone opening the bundle: tar
// has no index, and the manifest is what makes this something other than a pile
// of files.

import (
	"archive/tar"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// --- export bundle ---

type bundleOpts struct {
	to     string
	nodes  idList
	audit  int
	pretty bool
}

var exportBundle = cmd(Command{
	Group: "export", Name: "bundle", Usage: "<dir>",
	Short: "a tar of proofs, checkpoints and audit history for a recipient",
	Long: "For handing to somebody who will not have the store. It carries the\n" +
		"snapshot roots, the checkpoint chain, a slice of the audit log, the\n" +
		"redaction and grant ledgers, and an exported inclusion proof for each\n" +
		"node named with -node.\n\n" +
		"What it is not: a copy of the graph. `backup create` is that, and\n" +
		"`export graph` is the portable form. This is the material somebody needs\n" +
		"to check claims about the store without being given the store.\n\n" +
		"The manifest names every member and the snapshot root everything in it is\n" +
		"relative to. Verify the proofs with `provenance verify -root HEX`, against\n" +
		"a root obtained from somewhere other than this bundle — a root that\n" +
		"travels with the thing it attests proves only internal consistency.",
	Notice: "opens the store read-only (shared lock)",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *bundleOpts) {
		fs.StringVar(&o.to, "to", "", "tar file to write (required)")
		fs.Var(&o.nodes, "node", "include an inclusion proof for this node (repeatable)")
		fs.IntVar(&o.audit, "audit", 0, "include only the last N audit entries (0 = all)")
		fs.BoolVar(&o.pretty, "pretty", false, "indent the JSON members")
	},
	runExportBundle)

func runExportBundle(cx *Context, o *bundleOpts) (Result, error) {
	var r Result
	if o.to == "" {
		return r, Usagef("need -to <file.tar>")
	}

	s := cx.Disk()
	roots, err := s.SnapshotRoots()
	if err != nil {
		return r, fmt.Errorf("this store has no snapshot roots, so a bundle from it "+
			"would attest nothing: %w", err)
	}

	f, err := os.OpenFile(o.to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return r, fmt.Errorf("create %s: %w", o.to, err)
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	man := bundleManifest{
		Format:       1,
		WrittenAt:    cx.Now().UTC().Format(time.RFC3339),
		Store:        filepath.Base(cx.Target),
		SnapshotRoot: fmt.Sprintf("%x", roots.Snapshot[:]),
	}
	added := r.Table("members", Col("name"), RCol("bytes"))

	add := func(name string, body []byte) error {
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Mode: 0o644, Size: int64(len(body)),
			// A fixed modification time, because a bundle of the same material
			// should hash the same however many times it is written. The
			// interesting timestamps are inside the records.
			ModTime: time.Unix(0, 0).UTC(),
		}); err != nil {
			return err
		}
		if _, err := tw.Write(body); err != nil {
			return err
		}
		man.Members = append(man.Members, bundleMember{Name: name, Bytes: len(body)})
		added.Row(Str(name), Int(int64(len(body))))
		return nil
	}

	marshal := func(v any) ([]byte, error) {
		if o.pretty {
			return json.MarshalIndent(v, "", "  ")
		}
		return json.Marshal(v)
	}

	// The proofs, one file per node.
	for _, id := range o.nodes {
		raw, perr := s.ExportNodeProof(store.NodeID(id))
		if perr != nil {
			// A node that cannot be proved is a finding, not a failure: the
			// bundle is still worth producing without it, and the recipient
			// needs to know which node is missing and why.
			r.Find(SevWarn, "bundle.proof_unavailable",
				"no inclusion proof for node %d: %v", id, perr)
			continue
		}
		if err := add(fmt.Sprintf("proofs/node-%d.json", id), raw); err != nil {
			return r, err
		}
	}

	// The checkpoint chain.
	if chain, cerr := disk.ReadCheckpoints(cx.Target); cerr == nil && len(chain) > 0 {
		body, merr := marshal(checkpointsJSON(chain))
		if merr != nil {
			return r, merr
		}
		if err := add("checkpoints.json", body); err != nil {
			return r, err
		}
	}

	// The audit slice.
	if entries, aerr := disk.ReadAuditLog(cx.Target); aerr == nil && len(entries) > 0 {
		if o.audit > 0 && o.audit < len(entries) {
			// From the end: the recent entries are the ones a recipient is
			// checking against. The manifest records that it was cut, so a
			// truncated chain is not mistaken for a broken one.
			entries = entries[len(entries)-o.audit:]
			man.AuditTruncated = true
		}
		body, merr := marshal(auditJSON(entries))
		if merr != nil {
			return r, merr
		}
		if err := add("audit.json", body); err != nil {
			return r, err
		}
	}

	// The governance ledgers, verbatim, because their hash chains only verify
	// over the bytes they were written as.
	for _, name := range []string{"graphene.redactions", "graphene.grants"} {
		raw, rerr := os.ReadFile(filepath.Join(cx.Target, name))
		if rerr != nil {
			continue
		}
		if err := add("ledgers/"+name, raw); err != nil {
			return r, err
		}
	}

	body, err := marshal(man)
	if err != nil {
		return r, err
	}
	// Written last so it can name everything, and read first by anyone opening
	// the bundle — tar has no index, so the manifest is what makes this
	// something other than a pile of files.
	if err := tw.WriteHeader(&tar.Header{
		Name: "manifest.json", Mode: 0o644, Size: int64(len(body)),
		ModTime: time.Unix(0, 0).UTC(),
	}); err != nil {
		return r, err
	}
	if _, err := tw.Write(body); err != nil {
		return r, err
	}
	if err := tw.Close(); err != nil {
		return r, err
	}

	sec := r.Section("")
	sec.Add("to", Str(o.to))
	sec.Add("snapshot root", Hash(roots.Snapshot))
	sec.Add("members", Int(int64(len(man.Members)+1)))
	r.Notes("for the recipient",
		"check the proofs against a snapshot root obtained from somewhere other than",
		"this bundle: a root that travels with what it attests proves only that the",
		"bundle agrees with itself")
	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}

// bundleManifest is the bundle's index. Deliberately its own shape rather than
// a library type: it describes the bundle, which is this tool's construct.
type bundleManifest struct {
	Format         int            `json:"format"`
	WrittenAt      string         `json:"writtenAt"`
	Store          string         `json:"store"`
	SnapshotRoot   string         `json:"snapshotRoot"`
	AuditTruncated bool           `json:"auditTruncated,omitempty"`
	Members        []bundleMember `json:"members"`
}

type bundleMember struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
}

// checkpointsJSON and auditJSON render the engine's types with their hashes as
// hex rather than as the byte arrays encoding/json would otherwise produce.
func checkpointsJSON(chain []disk.Checkpoint) []map[string]any {
	out := make([]map[string]any, len(chain))
	for i, c := range chain {
		out[i] = map[string]any{
			"seq":            c.Seq,
			"unixNano":       c.UnixNano,
			"actorID":        c.ActorID,
			"snapshotRoot":   fmt.Sprintf("%x", c.SnapshotRoot[:]),
			"segmentCount":   c.SegmentCount,
			"auditCount":     c.AuditCount,
			"redactionCount": c.RedactionCount,
			"grantCount":     c.GrantCount,
			"prev":           fmt.Sprintf("%x", c.Prev[:]),
			"digest":         fmt.Sprintf("%x", c.Digest[:]),
		}
	}
	return out
}

func auditJSON(entries []disk.AuditEntry) []map[string]any {
	out := make([]map[string]any, len(entries))
	for i, e := range entries {
		out[i] = map[string]any{
			"seq":      e.Seq,
			"unixNano": e.UnixNano,
			"actorID":  e.ActorID,
			"kind":     e.Kind.String(),
			"detail":   e.Detail,
			"prevHash": fmt.Sprintf("%x", e.PrevHash[:]),
			"hash":     fmt.Sprintf("%x", e.Hash[:]),
		}
	}
	return out
}
