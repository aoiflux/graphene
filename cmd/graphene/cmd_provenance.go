package main

// provenance export, provenance verify.
//
// The pair is the point. One builds a proof from the compacted image and the
// ledger; the other checks it against a root the recipient obtained by some
// other route, and touches no store at all. If the checking half needed the
// store it would be verifying evidence against its own author, which proves
// nothing.

import (
	"encoding/hex"
	"flag"
	"os"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// --- provenance export ---

type proveOpts struct {
	node uint64
	edge uint64
	kind string
	out  string
}

var provenanceExport = cmd(Command{
	Group: "provenance", Name: "export", Aliases: []string{"prove"},
	Usage: "<dir>", Short: "export a proof to hand to someone else",
	Notice: "prove opens the store read-only; a writer must not hold it",
	Open:   OpenDiskRO, Streams: true, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *proveOpts) {
		fs.Uint64Var(&o.node, "node", 0, "entity to prove something about")
		fs.Uint64Var(&o.edge, "edge", 0, "relationship to prove something about")
		fs.StringVar(&o.kind, "kind", "inclusion",
			"inclusion | redaction | property-redaction")
		fs.StringVar(&o.out, "out", "", "write the proof here instead of stdout")
	},
	runProvenanceExport)

func runProvenanceExport(cx *Context, o *proveOpts) (Result, error) {
	var r Result

	if (o.node == 0) == (o.edge == 0) {
		return r, Usagef("need exactly one of -node or -edge")
	}
	s := cx.Disk()

	var blob []byte
	var err error
	switch {
	case o.edge != 0 && o.kind == "redaction":
		blob, err = s.ExportEdgeRedactionProof(store.EdgeID(o.edge))
	case o.edge != 0:
		// There is no ProveEdge in the engine: an edge's inclusion is not
		// separately provable, only its redaction is.
		return r, Usagef("-edge supports only -kind redaction")
	case o.kind == "inclusion":
		blob, err = s.ExportNodeProof(store.NodeID(o.node))
	case o.kind == "redaction":
		blob, err = s.ExportRedactionProof(store.NodeID(o.node))
	case o.kind == "property-redaction":
		blob, err = s.ExportPropertyRedactionProof(store.NodeID(o.node))
	default:
		return r, Usagef("unknown -kind %q", o.kind)
	}
	if err != nil {
		return r, err
	}

	if o.out == "" {
		// The payload is the output. In JSON mode this would interleave binary
		// with the envelope, so the caller is told to name a file instead —
		// stdout being a single parseable document is what -json is for.
		if cx.Globals.JSON {
			return r, Usagef(
				"-json needs -out: the proof is a binary payload and would not " +
					"survive being mixed into the document")
		}
		_, werr := cx.Out.Write(blob)
		return r, werr
	}
	if err := os.WriteFile(o.out, blob, 0600); err != nil {
		return r, err
	}
	sec := r.Section("")
	sec.Add("wrote", Bytes(int64(len(blob))))
	sec.Add("to", Str(o.out))
	return r, nil
}

// --- provenance verify ---

type verifyProofOpts struct{ root string }

var provenanceVerify = cmd(Command{
	Group: "provenance", Name: "verify", Aliases: []string{"verify-proof"},
	Usage: "<file>", Short: "check a proof against a root you retained",
	Long: "Touches no store. That is the property the whole exercise exists for: a\n" +
		"recipient has the bytes and a root they obtained independently, and needs\n" +
		"nothing else. A proof checked against the root inside it proves nothing,\n" +
		"because whoever wrote the file chose both.",
	Open: OpenNone, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *verifyProofOpts) {
		fs.StringVar(&o.root, "root", "",
			"the snapshot root you retained, as hex (required)")
	},
	runProvenanceVerify)

func runProvenanceVerify(cx *Context, o *verifyProofOpts) (Result, error) {
	var r Result

	path := cx.Target
	if path == "" {
		return r, Usagef("need a proof file")
	}
	if o.root == "" {
		return r, Usagef(
			"need -root: a proof checked against the root inside it proves nothing,\n" +
				"because whoever wrote the file chose both")
	}

	var root merkle.Hash
	raw, derr := hex.DecodeString(o.root)
	if derr != nil || len(raw) != len(root) {
		return r, Usagef("-root must be %d hex bytes", len(root))
	}
	copy(root[:], raw)

	blob, err := os.ReadFile(path)
	if err != nil {
		return r, err
	}
	proof, err := disk.UnmarshalProof(blob)
	if err != nil {
		return r, err
	}

	sec := r.Section("")
	sec.Addf("kind", "%s", proof.Kind)
	sec.Addf("subject", "%s", proof.Subject())
	sec.Add("checked against", Hash(root))

	if err := disk.VerifyExportedProof(root, proof); err != nil {
		r.Find(SevBroken, "proof.verification_failed", "%v", err)
		r.Verdict = VerdictBroken
		return r, nil
	}
	r.Notes("").Line("VERIFIED: %s of %s under the root supplied", proof.Kind, proof.Subject())
	r.Verdict = VerdictVerified
	return r, nil
}
