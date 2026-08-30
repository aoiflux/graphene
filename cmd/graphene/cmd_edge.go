package main

// The edge group: get, list, count, of, explain, verify, provenance.
//
// Two of these need explaining, because what they can prove is narrower than
// their names suggest and the gap is the kind that gets a report thrown out.
//
//   - `edge verify` proves *redaction*, not inclusion. The engine has ProveNode
//     and no ProveEdge: there is no Merkle commitment to an edge's presence to
//     build an inclusion proof from. What it does have is a tombstone tree, so
//     "this image records edge 7 as deliberately removed" is provable and "edge
//     7 is in this image" is not. Saying so is the whole point — a command that
//     printed VERIFIED for the weaker claim would be read as the stronger one.
//
//   - `edge provenance` reports the custody of the edge's two endpoints and
//     whatever tombstone the image holds for the edge itself. disk.CustodyFor
//     is NodeID-only; an edge has no custody record of its own, and inventing
//     one by averaging its endpoints' would be a fabrication.

import (
	"encoding/hex"
	"errors"
	"flag"
	"strings"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/merkle"
	"github.com/aoiflux/graphene/store"
)

// --- edge get ---

type edgeGetOpts struct {
	ids idList
}

var edgeGet = cmd(Command{
	Group: "edge", Name: "get", Usage: "<dir>",
	Short:  "fetch edges by ID",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *edgeGetOpts) {
		fs.Var(&o.ids, "id", "edge ID to fetch (repeatable, or comma-separated)")
	},
	runEdgeGet)

func runEdgeGet(cx *Context, o *edgeGetOpts) (Result, error) {
	var r Result
	if len(o.ids) == 0 {
		return r, Usagef("need at least one -id")
	}

	found, missing, err := cx.Graph().GetEdges(o.ids.edgeIDs())
	if err != nil {
		return r, err
	}

	edgeTable(&r, "edges", found, Paging{})

	s := r.Section("")
	s.Add("asked for", Int(int64(len(o.ids))))
	s.Add("found", Int(int64(len(found))))
	missingNote(s, "missing", uint64s(missing))

	if cx.Globals.Verbose {
		ids := make([]uint64, len(found))
		blobs := make([][]byte, len(found))
		for i, e := range found {
			ids[i], blobs[i] = uint64(e.ID), e.Properties
		}
		propertyDump(&r, "properties (msgpack, hex)", ids, blobs)
	}
	return r, nil
}

// --- edge list ---

type edgeListOpts struct {
	types stringList
	src   idList
	dst   idList
	props stringList
	match string
	order string
	page  Paging
}

var edgeList = cmd(Command{
	Group: "edge", Name: "list", Usage: "<dir>",
	Short: "query edges by label, endpoint and property",
	Long: "-src and -dst are pre-filters on the stored direction, which is not the\n" +
		"same question as `edge of -node N -direction both`: an edge is stored with\n" +
		"a Src and a Dst and that never changes. Use -src to ask what a node points\n" +
		"at, and `edge of` to ask what touches it.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	bindEdgeQuery, runEdgeList)

func bindEdgeQuery(fs *flag.FlagSet, o *edgeListOpts) {
	fs.Var(&o.types, "type", "edge label to match (repeatable; OR semantics)")
	fs.Var(&o.src, "src", "keep only edges leaving this node (repeatable)")
	fs.Var(&o.dst, "dst", "keep only edges arriving at this node (repeatable)")
	fs.Var(&o.props, "prop", filterHelp)
	fs.StringVar(&o.match, "match", "all", "all (AND) or any (OR) across -prop terms")
	fs.StringVar(&o.order, "order", "asc", "asc or desc, by ID")
	o.page.Bind(fs, 50, "rows to return (0 = all)")
}

func (o *edgeListOpts) edgeQuery() (store.EdgeQuery, error) {
	var q store.EdgeQuery

	types, err := edgeTypes(o.types)
	if err != nil {
		return q, err
	}
	filters, mode, err := parseFilters(o.props, o.match)
	if err != nil {
		return q, err
	}
	order, err := parseOrder(o.order)
	if err != nil {
		return q, err
	}
	q.Types, q.Filters, q.FilterMode, q.Order = types, filters, mode, order
	q.SrcIDs, q.DstIDs = o.src.nodeIDs(), o.dst.nodeIDs()
	q.Offset, q.Limit = o.page.Offset, o.page.Limit
	return q, nil
}

func runEdgeList(cx *Context, o *edgeListOpts) (Result, error) {
	var r Result

	q, err := o.edgeQuery()
	if err != nil {
		return r, err
	}
	probe := q.Limit > 0
	if probe {
		q.Limit++
	}

	edges, err := cx.Graph().QueryEdgesCtx(cx.Ctx, q)
	if err != nil {
		return r, err
	}
	more := probe && len(edges) == q.Limit
	if more {
		edges = edges[:len(edges)-1]
	}

	edgeTable(&r, "edges", edges, Paging{})
	describeQuery(&r, o.types, o.props, o.match, len(edges), o.page, more)
	if len(o.src) > 0 {
		r.Sections[len(r.Sections)-1].Add("src", Str(o.src.String()))
	}
	if len(o.dst) > 0 {
		r.Sections[len(r.Sections)-1].Add("dst", Str(o.dst.String()))
	}
	return r, nil
}

// --- edge count ---

// edgeCount and nodeCount run the same handler deliberately. Both figures come
// from the same open and cost nothing extra, and a `node count` that hid the
// edge count would send whoever wanted both through two opens of the store.
// Two names for one report, rather than two reports that could disagree.
var edgeCount = plain(Command{
	Group: "edge", Name: "count", Usage: "<dir>",
	Short:  "how many edges the store holds",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
}, runNodeCount)

// --- edge of ---

type edgeOfOpts struct {
	node      uint64
	direction string
	edgeTypes stringList
	page      Paging
}

var edgeOf = cmd(Command{
	Group: "edge", Name: "of", Usage: "<dir>",
	Short:  "the edges incident to a node",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *edgeOfOpts) {
		fs.Uint64Var(&o.node, "node", 0, "node ID (required)")
		fs.StringVar(&o.direction, "direction", "both", "out, in or both")
		fs.Var(&o.edgeTypes, "edge-type", "keep only these edge labels (repeatable)")
		o.page.Bind(fs, 50, "rows to return (0 = all)")
	},
	runEdgeOf)

func runEdgeOf(cx *Context, o *edgeOfOpts) (Result, error) {
	var r Result
	if o.node == 0 {
		return r, Usagef("need -node <id>")
	}
	dir, err := parseDirection(o.direction)
	if err != nil {
		return r, err
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return r, err
	}

	edges, err := cx.Graph().EdgesOf(store.NodeID(o.node), dir, ets)
	if err != nil {
		return r, err
	}
	edgeTable(&r, "edges", edges, o.page)

	s := r.Section("")
	s.Add("node", ID(o.node))
	s.Add("direction", Str(directionName(dir)))
	s.Add("edges", Int(int64(len(edges))))
	return r, nil
}

// --- edge explain ---

var edgeExplain = cmd(Command{
	Group: "edge", Name: "explain", Usage: "<dir>",
	Short:  "how the planner would resolve an edge query",
	Long:   "Takes the same flags as `edge list`. See `graphene help node explain`.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	bindEdgeQuery, runEdgeExplain)

func runEdgeExplain(cx *Context, o *edgeListOpts) (Result, error) {
	var r Result
	q, err := o.edgeQuery()
	if err != nil {
		return r, err
	}
	plan, err := cx.Graph().ExplainEdgeQuery(q)
	if err != nil {
		return r, err
	}
	addPlan(&r, plan)
	return r, nil
}

// --- edge verify ---

type edgeVerifyOpts struct {
	id   uint64
	root string
}

var edgeVerify = cmd(Command{
	Group: "edge", Name: "verify", Usage: "<dir>",
	Short: "prove this image records an edge as redacted",
	Long: "This proves a removal, not a presence. The engine commits to node\n" +
		"identities and to tombstones; there is no Merkle commitment to an edge's\n" +
		"presence, so there is no inclusion proof to build for one. What this\n" +
		"answers is \"does this image attest that edge N was deliberately removed,\n" +
		"and does that attestation resolve to the snapshot root\".\n\n" +
		"Give -root the snapshot root you obtained independently — from an anchor,\n" +
		"a countersigned checkpoint, an earlier report. Without it the proof is\n" +
		"only checked against the root it carries itself, which proves the proof\n" +
		"is well formed and nothing about the store it came from.",
	Notice: "opens the store read-only (shared lock) to read the image's tombstone tree",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *edgeVerifyOpts) {
		fs.Uint64Var(&o.id, "id", 0, "edge ID (required)")
		fs.StringVar(&o.root, "root", "",
			"snapshot root, hex, obtained independently of this store")
	},
	runEdgeVerify)

func runEdgeVerify(cx *Context, o *edgeVerifyOpts) (Result, error) {
	var r Result
	if o.id == 0 {
		return r, Usagef("need -id <edge>")
	}

	proof, err := cx.Disk().ProveEdgeRedaction(store.EdgeID(o.id))
	switch {
	case errors.Is(err, disk.ErrNoTombstone):
		// A verdict, not a fault: "this image does not record that edge as
		// removed" is exactly the answer somebody checking a redaction claim
		// needs, and it is not a failure of the command.
		s := r.Section("")
		s.Add("edge", ID(o.id))
		s.Add("tombstone", Str("none in this image"))
		r.Find(SevWarn, "edge.no_tombstone",
			"the image records no removal of edge %d — it was never redacted, "+
				"or the image predates the redaction", o.id)
		return r, nil
	case errors.Is(err, disk.ErrNoSnapshotRoots):
		r.Section("").Add("edge", ID(o.id))
		r.Find(SevWarn, "edge.no_roots",
			"this image carries no snapshot roots, so nothing in it can be proved; "+
				"compact the store to write an image that does")
		return r, nil
	case err != nil:
		return r, err
	}

	t := proof.Tombstone
	s := r.Section("tombstone")
	s.Add("scope", Str(t.Scope.String()))
	s.Add("edge", ID(uint64(t.EdgeID)))
	if t.NodeID != 0 {
		s.Add("node", ID(uint64(t.NodeID)))
	}
	s.Add("version hash", Hash(t.VersionHash))
	s.Add("redaction record", Uint(t.RedactionSeq))
	s.Add("redaction hash", Hex(t.RedactionHash[:]))

	roots := r.Section("roots")
	roots.Add("tombstone root", Hash(proof.TombstoneRoot))
	roots.Add("snapshot root", Hash(proof.Roots.Snapshot))

	against := proof.Roots.Snapshot
	independent := false
	if o.root != "" {
		h, herr := parseRoot(o.root)
		if herr != nil {
			return r, herr
		}
		against, independent = h, true
		roots.Add("checked against", Hash(h))
	}

	if verr := disk.VerifyRedactionInclusion(against, proof); verr != nil {
		r.Find(SevBroken, "edge.proof_failed", "%v", verr)
		return r, nil
	}

	if !independent {
		// The load-bearing caveat. Verifying against the root the proof itself
		// states is circular, and a report that said VERIFIED without saying so
		// would be quoted as though it meant the other thing.
		r.Find(SevWarn, "edge.root_not_independent",
			"the proof is well formed, but it was checked against the root it carries "+
				"itself — supply -root to check it against one you obtained elsewhere")
		return r, nil
	}
	r.Verdict = VerdictVerified
	return r, nil
}

// --- edge provenance ---

type edgeProvenanceOpts struct {
	id   uint64
	keys pubkeyList
}

var edgeProvenance = cmd(Command{
	Group: "edge", Name: "provenance", Usage: "<dir>",
	Short: "the custody of an edge's endpoints, and its own tombstone",
	Long: "An edge has no custody record. disk.CustodyFor is node-only, because the\n" +
		"attestations and the audit entries the report is assembled from are\n" +
		"written about entities rather than relationships. So this reports what\n" +
		"does exist: the custody of each endpoint, and whatever the image records\n" +
		"about the edge's own removal. It is not a custody report for the edge and\n" +
		"does not claim to be one.",
	Notice: "opens the store read-only (shared lock)",
	Open:   OpenDiskRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *edgeProvenanceOpts) {
		fs.Uint64Var(&o.id, "id", 0, "edge ID (required)")
		fs.Var(&o.keys, "pubkey", "ID:HEX Ed25519 public key (repeatable)")
	},
	runEdgeProvenance)

func runEdgeProvenance(cx *Context, o *edgeProvenanceOpts) (Result, error) {
	var r Result
	if o.id == 0 {
		return r, Usagef("need -id <edge>")
	}
	verifier, err := verifierFromFlag(o.keys)
	if err != nil {
		return r, err
	}

	s := cx.Disk()
	e, err := s.GetEdge(store.EdgeID(o.id))
	if err != nil {
		return r, err
	}

	head := r.Section("edge")
	head.Add("id", ID(uint64(e.ID)))
	head.Add("src", ID(uint64(e.Src)))
	head.Add("dst", ID(uint64(e.Dst)))
	head.Add("labels", Strs(edgeLabelList(e)))

	// Endpoint custody, one section each, so a reader can see which side of the
	// relationship is the one that is unattested.
	for _, ep := range []struct {
		label string
		id    store.NodeID
	}{{"src", e.Src}, {"dst", e.Dst}} {
		rep, cerr := s.CustodyFor(ep.id, verifier)
		if cerr != nil {
			return r, cerr
		}
		sec := r.Section("custody of " + ep.label + " node " + Uint(uint64(ep.id)).String())
		addCustody(sec, rep, verifier)
		for _, gap := range rep.Gaps {
			r.Find(SevWarn, "edge.custody_gap", "%s node %d: %s", ep.label, ep.id, gap)
		}
	}

	// And the edge's own tombstone, if the image has one.
	tomb := r.Section("removal")
	found := false
	for _, t := range s.Tombstones() {
		if t.EdgeID == e.ID {
			found = true
			tomb.Add("scope", Str(t.Scope.String()))
			tomb.Add("version hash", Hash(t.VersionHash))
			tomb.Add("redaction record", Uint(t.RedactionSeq))
		}
	}
	if !found {
		tomb.Add("tombstone", Str("none — this image records no removal of this edge"))
	}
	if r.Verdict == VerdictNone {
		r.Verdict = VerdictVerified
	}
	return r, nil
}

// parseRoot reads a 32-byte hex Merkle root from a flag.
func parseRoot(s string) (merkle.Hash, error) {
	var h merkle.Hash
	raw, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return h, Usagef("-root %q: %v", s, err)
	}
	if len(raw) != len(h) {
		return h, Usagef("-root %q: a Merkle root is %d hex bytes, got %d", s, len(h), len(raw))
	}
	copy(h[:], raw)
	return h, nil
}
