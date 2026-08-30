package main

// The redaction group: list is in cmd_ledger.go; verify, impact, tombstones and
// apply are here.
//
// `redaction apply` is the one command in this tool that destroys content and
// records the fact. It differs from a delete in every way that matters: the
// removal is signed into a hash-chained ledger, a tombstone goes into the next
// image under the snapshot root, and the version hash of what was destroyed is
// kept — so a recipient holding an earlier image can match the two and confirm
// that what is missing is what the record says is missing.
//
// That is what separates lawful redaction from evidence destruction, and it is
// why the engine requires a reason rather than this tool merely asking for one.

import (
	"flag"
	"strings"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// --- redaction verify ---

type redactVerifyOpts struct{ keys pubkeyList }

var redactionVerify = cmd(Command{
	Group: "redaction", Name: "verify", Usage: "<dir>",
	Short: "the redaction ledger's hash chain, and its signatures",
	Long: "A break here is the loud case. The ledger is what separates lawful\n" +
		"redaction from evidence destruction: an absence with a signed, chained\n" +
		"record explaining it is one thing, and an absence with a broken chain is\n" +
		"another entirely.",
	Open: OpenFiles, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *redactVerifyOpts) {
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
	},
	runRedactionVerify)

func runRedactionVerify(cx *Context, o *redactVerifyOpts) (Result, error) {
	var r Result
	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}
	records, err := disk.ReadRedactions(cx.Target)
	if err != nil && !isAbsent(err) {
		return r, err
	}
	if len(records) == 0 {
		r.Section("").Add("redaction ledger", Str("no records"))
		r.Verdict = VerdictVerified
		return r, nil
	}
	summarizeLedger(&r, "redaction", len(records), len(records), false,
		disk.VerifyRedactionChain(records, verifier), verifier)
	return r, nil
}

// --- redaction impact ---

type impactOpts struct{ node uint64 }

var redactionImpact = cmd(Command{
	Group: "redaction", Name: "impact", Usage: "<dir>",
	Short: "what redacting a node would remove, before anything is destroyed",
	Long: "A pure dry run: it computes the cascade and changes nothing, and it is\n" +
		"available while refusing is still possible. That is the whole point —\n" +
		"the cost of a redaction is the edges it takes with it, and finding that\n" +
		"out afterwards is finding it out too late.",
	Notice: "opens the store read-only (shared lock)",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *impactOpts) {
		fs.Uint64Var(&o.node, "node", 0, "node ID (required)")
		fs.Uint64Var(&o.node, "id", 0, "node ID (noun-verb spelling)")
	},
	runRedactionImpact)

func runRedactionImpact(cx *Context, o *impactOpts) (Result, error) {
	var r Result
	if o.node == 0 {
		return r, Usagef("need -node (or -id)")
	}

	imp, err := cx.Disk().RedactionImpactFor(nodeID(o.node))
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("node", ID(uint64(imp.NodeID)))
	s.Add("version hash", Hash(imp.VersionHash))
	s.Add("edges cascaded", Int(int64(len(imp.CascadedEdges))))

	if len(imp.CascadedEdges) > 0 {
		t := r.Table("edges that would be tombstoned", RCol("edge"), Col("version hash"))
		for i, id := range imp.CascadedEdges {
			h := Nil()
			if i < len(imp.CascadedHashes) {
				h = Hash(imp.CascadedHashes[i])
			}
			t.Row(ID(uint64(id)), h)
		}
	}

	if imp.ExceedsPolicy {
		s.Add("exceeds the store's redaction policy", Bool(true))
		r.Find(SevWarn, "redaction.exceeds_policy",
			"this cascade is larger than the store's RedactionPolicy allows, "+
				"and the redaction would be refused")
	}
	r.Notes("this changed nothing", "no record was written and nothing was removed")
	return r, nil
}

// --- redaction tombstones ---

type tombstoneOpts struct{ page Paging }

var redactionTombstones = cmd(Command{
	Group: "redaction", Name: "tombstones", Usage: "<dir>",
	Short: "the removals the compacted image itself records",
	Long: "Different from `redaction list`, and the difference is who can be\n" +
		"convinced. A ledger record persuades someone holding the store; a\n" +
		"tombstone is committed to under the snapshot root, so it persuades\n" +
		"someone holding nothing but the image.\n\n" +
		"An entity redacted since the last compaction has a ledger record and no\n" +
		"tombstone yet. That is not a discrepancy — it is the image being older\n" +
		"than the removal.",
	Notice: "opens the store read-only (shared lock) to read the image's tombstone list",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *tombstoneOpts) {
		o.page.Bind(fs, 50, "tombstones to show (0 = all)")
	},
	runRedactionTombstones)

func runRedactionTombstones(cx *Context, o *tombstoneOpts) (Result, error) {
	var r Result

	tombs := cx.Disk().Tombstones()
	if len(tombs) == 0 {
		r.Section("").Add("tombstones", Str("none; this image records no removals"))
		r.Verdict = VerdictVerified
		return r, nil
	}

	lo, hi, trunc := o.page.Window(len(tombs))
	t := r.Table("tombstones",
		Col("scope"), RCol("node"), RCol("edge"), Col("version hash"), RCol("ledger record"))
	for _, tb := range tombs[lo:hi] {
		node, edge := Nil(), Nil()
		if tb.NodeID != 0 {
			node = ID(uint64(tb.NodeID))
		}
		if tb.EdgeID != 0 {
			edge = ID(uint64(tb.EdgeID))
		}
		t.Row(Str(tb.Scope.String()), node, edge, Hash(tb.VersionHash), Uint(tb.RedactionSeq))
	}
	t.Truncated, t.Total = trunc, len(tombs)

	r.Section("").Add("tombstones", Int(int64(len(tombs))))
	r.Verdict = VerdictVerified
	return r, nil
}

// --- redaction apply ---

type redactApplyOpts struct {
	node       uint64
	edge       uint64
	scope      string
	actor      uint64
	role       uint
	reason     string
	acceptCost bool
}

var redactionApply = cmd(Command{
	Group: "redaction", Name: "apply", Usage: "<dir>",
	Short: "remove an entity, with an attributed and chained record of why",
	Long: "The one command here that destroys content, and the only supported way\n" +
		"to do it. It differs from a delete in every way that matters: the removal\n" +
		"is signed into a hash-chained ledger, a tombstone goes into the next\n" +
		"image under the snapshot root, and the version hash of what was destroyed\n" +
		"is kept — so a recipient holding an earlier image can match the two and\n" +
		"confirm that what is missing is what the record says is missing.\n\n" +
		"That is what separates lawful redaction from evidence destruction, and it\n" +
		"is why -reason is required by the engine rather than by this tool.\n\n" +
		"Scopes:\n\n" +
		"  node        the node and every edge incident to it\n" +
		"  node-props  the node's properties, keeping the node\n" +
		"  edge        the edge\n" +
		"  edge-props  the edge's properties, keeping the edge\n\n" +
		"Run `redaction impact -node N` first. The cost of a node redaction is the\n" +
		"edges it takes with it, and -dry-run here reports exactly that.",
	Notice:  "redaction apply opens the store for writing and takes the exclusive lock",
	Open:    OpenDiskRW,
	Mutates: true,
	Tier:    CtxAdvisory,
},
	func(fs *flag.FlagSet, o *redactApplyOpts) {
		fs.Uint64Var(&o.node, "node", 0, "node ID")
		fs.Uint64Var(&o.edge, "edge", 0, "edge ID")
		fs.StringVar(&o.scope, "scope", "node", "node | node-props | edge | edge-props")
		fs.Uint64Var(&o.actor, "actor", 0, "the actor accountable for this removal (required)")
		fs.UintVar(&o.role, "role", 0, "role ID, recorded and never interpreted")
		fs.StringVar(&o.reason, "reason", "", "why, in your own words (required)")
	},
	runRedactionApply)

func runRedactionApply(cx *Context, o *redactApplyOpts) (Result, error) {
	var r Result

	if o.actor == 0 {
		return r, Usagef("need -actor: an unattributed removal is a deletion with " +
			"extra steps, and the attribution is the point")
	}
	if strings.TrimSpace(o.reason) == "" {
		return r, Usagef("need -reason: the engine requires one. An absence with no " +
			"stated reason is indistinguishable from evidence destruction")
	}

	scope := strings.ToLower(strings.TrimSpace(o.scope))
	switch scope {
	case "node", "node-props":
		if o.node == 0 {
			return r, Usagef("-scope %s needs -node", scope)
		}
	case "edge", "edge-props":
		if o.edge == 0 {
			return r, Usagef("-scope %s needs -edge", scope)
		}
	default:
		return r, Usagef("-scope %q: want node, node-props, edge or edge-props", o.scope)
	}

	s := cx.Disk()

	if cx.Globals.DryRun {
		w := r.Section("would remove")
		w.Add("scope", Str(scope))
		if o.node != 0 {
			w.Add("node", ID(o.node))
		}
		if o.edge != 0 {
			w.Add("edge", ID(o.edge))
		}
		w.Add("actor", Uint(o.actor))
		w.Add("reason", Str(o.reason))

		// The cascade is the cost, and it is only knowable for a node scope.
		if scope == "node" {
			imp, err := s.RedactionImpactFor(nodeID(o.node))
			if err != nil {
				return r, err
			}
			w.Add("edges cascaded", Int(int64(len(imp.CascadedEdges))))
			if len(imp.CascadedEdges) > 0 {
				t := r.Table("edges that would go with it", RCol("edge"))
				for _, id := range imp.CascadedEdges {
					t.Row(ID(uint64(id)))
				}
			}
			if imp.ExceedsPolicy {
				r.Find(SevWarn, "redaction.exceeds_policy",
					"this cascade is larger than the store's RedactionPolicy allows, "+
						"and the redaction would be refused")
			}
		}
		return r, nil
	}

	req := disk.RedactionRequest{ActorID: o.actor, RoleID: uint32(o.role), Reason: o.reason}
	var rec disk.RedactionRecord
	var err error
	switch scope {
	case "node":
		rec, err = s.RedactNode(nodeID(o.node), req)
	case "node-props":
		rec, err = s.RedactNodeProperties(nodeID(o.node), req)
	case "edge":
		rec, err = s.RedactEdge(edgeID(o.edge), req)
	case "edge-props":
		rec, err = s.RedactEdgeProperties(edgeID(o.edge), req)
	}
	if err != nil {
		// The engine's own message already names Options.Redaction and the
		// cascade limit, so there is nothing to add: wrapping it would only put
		// this tool's words in front of the engine's more specific ones.
		return r, err
	}

	sec := r.Section("recorded")
	sec.Add("sequence", Uint(rec.Seq))
	sec.Add("at", Time(rec.UnixNano))
	sec.Add("scope", Str(rec.Scope.String()))
	sec.Add("actor", Uint(rec.ActorID))
	sec.Add("reason", Str(rec.Reason))
	sec.Add("version hash of what was removed", Hash(rec.VersionHash))
	if len(rec.CascadedEdges) > 0 {
		sec.Add("edges cascaded", Int(int64(len(rec.CascadedEdges))))
	}
	sec.Add("signed", Bool(len(rec.Signature) > 0))

	r.Notice("the tombstone reaches the image at the next compaction; " +
		"until then the removal is recorded in the ledger only")
	r.Verdict = VerdictVerified
	return r, nil
}

// nodeID and edgeID keep the conversions at the call sites short enough to read.
func nodeID(n uint64) store.NodeID { return store.NodeID(n) }
func edgeID(n uint64) store.EdgeID { return store.EdgeID(n) }
