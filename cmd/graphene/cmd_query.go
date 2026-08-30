package main

// provenance chain, and the query group.
//
// # provenance chain is not a cryptographic chain
//
// It is a reverse DFS over inbound edges — "what did this artefact come from",
// answered from the graph's own structure. The cryptographic account of an
// entity is `provenance custody`. The two have similar names and answer
// completely different questions, and conflating them in a report is how a
// structural walk gets quoted as an attestation. The command says so in its own
// output, not only in its help.
//
// `query relations` is the adjacency-driven query: given the nodes you care
// about, it resolves through the adjacency index rather than scanning edges.

import (
	"flag"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// --- provenance chain ---

type chainOpts struct {
	node      uint64
	depth     int
	edgeTypes stringList
	budget    budgetOpts
	viz       string
}

var provenanceChain = cmd(Command{
	Group: "provenance", Name: "chain", Usage: "<dir>",
	Short: "walk back through what an entity came from",
	Long: "A graph traversal, not a cryptographic chain. It follows inbound edges\n" +
		"from the node until nothing further leads in, which answers \"where did\n" +
		"this artefact come from\" from the graph's own structure.\n\n" +
		"The cryptographic account of an entity is `provenance custody`. This\n" +
		"proves nothing and signs nothing, and a report from it should never be\n" +
		"quoted as though it did.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *chainOpts) {
		fs.Uint64Var(&o.node, "node", 0, "node to walk back from (required)")
		fs.Uint64Var(&o.node, "id", 0, "node to walk back from (noun-verb spelling)")
		fs.IntVar(&o.depth, "depth", 20, "maximum hops back")
		fs.Var(&o.edgeTypes, "edge-type", "follow only these edge labels (repeatable)")
		fs.StringVar(&o.viz, "viz", "", "also write an interactive HTML view of the chain")
		o.budget.Bind(fs)
	},
	runProvenanceChain)

func runProvenanceChain(cx *Context, o *chainOpts) (Result, error) {
	var r Result
	if o.node == 0 {
		return r, Usagef("need -node (or -id)")
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return r, err
	}
	bud, err := o.budget.budget()
	if err != nil {
		return r, err
	}

	res, err := cx.Graph().ProvenanceChainCtx(cx.Ctx,
		store.NodeID(o.node), o.depth, ets, bud)
	if err != nil {
		if budgetFinding(&r, err, o.budget) {
			return r, nil
		}
		return r, err
	}

	t := r.Table("chain", RCol("step"), RCol("node"), Col("labels"), RCol("via edge"))
	for i, n := range res.Chain {
		via := Nil()
		if i > 0 && i-1 < len(res.Edges) {
			via = ID(uint64(res.Edges[i-1].ID))
		}
		t.Row(Int(int64(i)), ID(uint64(n.ID)), Strs(nodeLabelList(n)), via)
	}

	s := r.Section("")
	s.Add("from", ID(o.node))
	s.Add("depth limit", Int(int64(o.depth)))
	s.Add("chain length", Int(int64(len(res.Chain))))
	if len(res.Chain) > 0 {
		s.Add("root reached", ID(uint64(res.Chain[len(res.Chain)-1].ID)))
	}
	if len(res.Chain) == o.depth+1 {
		// The one thing worth warning about: a chain that stopped at the depth
		// limit has not reached a root, and reading its last node as the origin
		// of the artefact would be wrong.
		s.AddNote("truncated", Bool(true), "(-depth reached; the last node is not a root)")
	}
	o.budget.describe(s)
	r.Notes("what this is",
		"a structural walk over inbound edges — see `graphene provenance custody`",
		"for the cryptographic account of this entity")

	return r, writeViz(&r, o.viz, res.Chain, res.Edges,
		fmt.Sprintf("provenance chain from node %d", o.node))
}

// --- query relations ---

type relationOpts struct {
	anchors      idList
	counterparts idList
	direction    string
	edgeTypes    stringList
	props        stringList
	match        string
	order        string
	page         Paging
}

var queryRelations = cmd(Command{
	Group: "query", Name: "relations", Usage: "<dir>",
	Short: "edges around one or more anchor nodes",
	Long: "The adjacency-driven query: give it the nodes you care about and it\n" +
		"resolves through the adjacency index rather than scanning edges. -direction\n" +
		"says which side of each relation the anchor has to be on, and -counterpart\n" +
		"constrains the other end.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *relationOpts) {
		fs.Var(&o.anchors, "anchor", "node ID to anchor on (repeatable; required)")
		fs.Var(&o.counterparts, "counterpart", "constrain the far endpoint (repeatable)")
		fs.StringVar(&o.direction, "direction", "both", "out, in or both")
		fs.Var(&o.edgeTypes, "edge-type", "keep only these edge labels (repeatable)")
		fs.Var(&o.props, "prop", filterHelp)
		fs.StringVar(&o.match, "match", "all", "all (AND) or any (OR) across -prop terms")
		fs.StringVar(&o.order, "order", "asc", "asc or desc, by ID")
		o.page.Bind(fs, 50, "rows to return (0 = all)")
	},
	runQueryRelations)

func runQueryRelations(cx *Context, o *relationOpts) (Result, error) {
	var r Result
	if len(o.anchors) == 0 {
		return r, Usagef("need at least one -anchor")
	}
	dir, err := parseDirection(o.direction)
	if err != nil {
		return r, err
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return r, err
	}
	filters, mode, err := parseFilters(o.props, o.match)
	if err != nil {
		return r, err
	}
	order, err := parseOrder(o.order)
	if err != nil {
		return r, err
	}

	edges, err := cx.Graph().QueryRelations(store.RelationQuery{
		Anchors:      o.anchors.nodeIDs(),
		Direction:    dir,
		Counterparts: o.counterparts.nodeIDs(),
		EdgeTypes:    ets,
		Filters:      filters,
		FilterMode:   mode,
		Order:        order,
		Offset:       o.page.Offset,
		Limit:        o.page.Limit,
	})
	if err != nil {
		return r, err
	}

	edgeTable(&r, "relations", edges, Paging{})

	s := r.Section("")
	s.Add("anchors", Str(o.anchors.String()))
	s.Add("direction", Str(directionName(dir)))
	if len(o.counterparts) > 0 {
		s.Add("counterparts", Str(o.counterparts.String()))
	}
	s.Add("relations", Int(int64(len(edges))))
	if o.page.Limit > 0 && len(edges) == o.page.Limit {
		s.AddNote("at the limit", Bool(true), "(raise -limit, or page with -offset)")
	}
	return r, nil
}
