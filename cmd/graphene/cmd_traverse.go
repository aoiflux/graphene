package main

// The traverse and query groups, and provenance chain.
//
// This is the largest thing the tool could not do. BFS, DFS, bidirectional
// shortest path, VF2 subgraph matching, cycle detection and induced subgraphs
// have been in the engine since Phase 1, and none of them had a shell path: the
// CLI could tell you how many records a store held and nothing whatsoever about
// their shape.
//
// # Budgets
//
// Every walk here binds -max-nodes, -max-edges and -max-time, and -max-nodes
// defaults to 100 000 rather than to unlimited. Depth does not bound a walk —
// one hub of degree 100 000 puts 100 000 entries in the visited set at depth
// one — and the difference between a tool that stops and a tool that exhausts
// the machine is whether somebody remembered to pass a limit. The library
// refuses rather than truncating when a budget is hit, so a partial answer is
// never returned looking like a complete one, and the refusal is reported here
// as a finding naming the limit that stopped it.
//
// # provenance chain is not a cryptographic chain
//
// It is a reverse DFS over inbound edges — "what did this artefact come from",
// answered from the graph's own structure. The cryptographic account of an
// entity is `provenance custody`. The two have similar names and answer
// completely different questions, and conflating them in a report is how a
// structural walk gets quoted as an attestation.

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
	"github.com/aoiflux/graphene/viz"
)

// walkOpts are the flags every traversal shares.
type walkOpts struct {
	from      uint64
	to        uint64
	depth     int
	direction string
	edgeTypes stringList
	budget    budgetOpts
	viz       string
	page      Paging
}

func (o *walkOpts) bindCommon(fs *flag.FlagSet) {
	fs.Uint64Var(&o.from, "from", 0, "node to start from (required)")
	fs.IntVar(&o.depth, "depth", 3, "maximum hops")
	fs.StringVar(&o.direction, "direction", "both", "out, in or both")
	fs.Var(&o.edgeTypes, "edge-type", "follow only these edge labels (repeatable)")
	fs.StringVar(&o.viz, "viz", "",
		"also write an interactive HTML view of the result to this path")
	o.budget.Bind(fs)
	o.page.Bind(fs, 50, "rows to show per table (0 = all)")
}

// resolve turns the shared flags into library arguments, once.
func (o *walkOpts) resolve() (store.Direction, []store.EdgeType, store.Budget, error) {
	dir, err := parseDirection(o.direction)
	if err != nil {
		return 0, nil, store.Budget{}, err
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return 0, nil, store.Budget{}, err
	}
	bud, err := o.budget.budget()
	if err != nil {
		return 0, nil, store.Budget{}, err
	}
	if o.from == 0 {
		return 0, nil, store.Budget{}, Usagef("need -from <node>")
	}
	return dir, ets, bud, nil
}

// --- traverse bfs / dfs ---

var traverseBFS = cmd(Command{
	Group: "traverse", Name: "bfs", Usage: "<dir>",
	Short: "the k-hop neighbourhood of a node, breadth first",
	Long: "Bounded by -depth and by the budget flags. A budget that is hit is a\n" +
		"refusal and not a truncation: nothing partial is returned, because a\n" +
		"partial answer that looks complete is the failure this exists to prevent.\n" +
		"Raise the limit, or narrow the walk with -depth and -edge-type.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *walkOpts) { o.bindCommon(fs) },
	func(cx *Context, o *walkOpts) (Result, error) { return runWalk(cx, o, "bfs") })

var traverseDFS = cmd(Command{
	Group: "traverse", Name: "dfs", Usage: "<dir>",
	Short: "the same neighbourhood, depth first",
	Long: "Same reachable set as bfs for the same depth and filters; the difference\n" +
		"is the order edges are crossed in, which matters for the shape of a\n" +
		"partial result and not for the answer.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *walkOpts) { o.bindCommon(fs) },
	func(cx *Context, o *walkOpts) (Result, error) { return runWalk(cx, o, "dfs") })

func runWalk(cx *Context, o *walkOpts, kind string) (Result, error) {
	var r Result

	dir, ets, bud, err := o.resolve()
	if err != nil {
		return r, err
	}

	g := cx.Graph()
	var res *traversal.BFSResult
	if kind == "bfs" {
		res, err = g.BFSCtx(cx.Ctx, store.NodeID(o.from), o.depth, dir, ets, bud)
	} else {
		res, err = g.DFSCtx(cx.Ctx, store.NodeID(o.from), o.depth, dir, ets, bud)
	}
	if err != nil {
		if budgetFinding(&r, err, o.budget) {
			return r, nil
		}
		return r, err
	}

	nodeTable(&r, "nodes reached", res.Nodes, o.page)
	edgeTable(&r, "edges crossed", res.Edges, o.page)

	s := r.Section("")
	s.Add("walk", Str(kind))
	s.Add("from", ID(o.from))
	s.Add("depth", Int(int64(o.depth)))
	s.Add("direction", Str(directionName(dir)))
	if len(o.edgeTypes) > 0 {
		s.Add("edge labels", Str(o.edgeTypes.String()))
	}
	s.Add("nodes", Int(int64(len(res.Nodes))))
	s.Add("edges", Int(int64(len(res.Edges))))
	o.budget.describe(s)

	return r, writeViz(&r, o.viz, res.Nodes, res.Edges,
		fmt.Sprintf("%s from node %d", kind, o.from))
}

// --- traverse path ---

var traversePath = cmd(Command{
	Group: "traverse", Name: "path", Usage: "<dir>",
	Short: "the shortest path between two nodes",
	Long: "Bidirectional BFS, which meets in the middle and treats the graph as\n" +
		"undirected for path finding — so -direction does not apply here. Reports\n" +
		"the nodes and the edges in order, so the path can be read as a route\n" +
		"rather than as a set.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *walkOpts) {
		fs.Uint64Var(&o.from, "from", 0, "node to start from (required)")
		fs.Uint64Var(&o.to, "to", 0, "node to reach (required)")
		fs.Var(&o.edgeTypes, "edge-type", "follow only these edge labels (repeatable)")
		fs.StringVar(&o.viz, "viz", "", "also write an interactive HTML view of the path")
		o.budget.Bind(fs)
		o.page.Bind(fs, 0, "rows to show per table (0 = all)")
	},
	runTraversePath)

func runTraversePath(cx *Context, o *walkOpts) (Result, error) {
	var r Result
	if o.from == 0 || o.to == 0 {
		return r, Usagef("need -from <node> and -to <node>")
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return r, err
	}
	bud, err := o.budget.budget()
	if err != nil {
		return r, err
	}

	res, err := cx.Graph().ShortestPathCtx(cx.Ctx,
		store.NodeID(o.from), store.NodeID(o.to), ets, bud)
	if err != nil {
		if budgetFinding(&r, err, o.budget) {
			return r, nil
		}
		return r, err
	}

	if res == nil || len(res.Nodes) == 0 {
		// No path is an answer, not an error. Reported as a finding so a script
		// checking the JSON verdict can tell it apart from a path of length
		// zero, which is what `-from N -to N` legitimately returns.
		s := r.Section("")
		s.Add("from", ID(o.from))
		s.Add("to", ID(o.to))
		s.Add("path", Str("none"))
		r.Find(SevWarn, "path.unreachable",
			"no path from %d to %d under these edge labels", o.from, o.to)
		o.budget.describe(s)
		return r, nil
	}

	// The route, in order, with the edge that was taken at each step. A node
	// table and an edge table side by side would lose the ordering, which is
	// the only thing that makes this a path rather than a subgraph. It comes
	// before the summary because it is the answer, and every other command in
	// the tool puts its answer first.
	t := r.Table("route", RCol("step"), RCol("node"), Col("labels"), RCol("via edge"), Col("edge labels"))
	for i, n := range res.Nodes {
		via, el := Nil(), Nil()
		if i > 0 && i-1 < len(res.Edges) {
			via = ID(uint64(res.Edges[i-1].ID))
			el = Strs(edgeLabelList(res.Edges[i-1]))
		}
		t.Row(Int(int64(i)), ID(uint64(n.ID)), Strs(nodeLabelList(n)), via, el)
	}

	s := r.Section("")
	s.Add("from", ID(o.from))
	s.Add("to", ID(o.to))
	s.Add("hops", Int(int64(len(res.Edges))))
	o.budget.describe(s)

	r.Verdict = VerdictVerified
	return r, writeViz(&r, o.viz, res.Nodes, res.Edges,
		fmt.Sprintf("path %d -> %d", o.from, o.to))
}

// --- traverse cycle ---

type cycleOpts struct {
	from      uint64
	depth     int
	edgeTypes stringList
}

var traverseCycle = cmd(Command{
	Group: "traverse", Name: "cycle", Usage: "<dir>",
	Short: "whether a cycle is reachable from a node",
	Long: "Answers yes or no within -depth hops following outbound edges. It does\n" +
		"not report where the cycle is: HasCycle detects one and does not record\n" +
		"it, and reconstructing a plausible-looking cycle from a second walk\n" +
		"would risk naming a different one from the one that was found.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *cycleOpts) {
		fs.Uint64Var(&o.from, "from", 0, "node to start from (required)")
		fs.IntVar(&o.depth, "depth", 100, "maximum hops")
		fs.Var(&o.edgeTypes, "edge-type", "follow only these edge labels (repeatable)")
	},
	runTraverseCycle)

func runTraverseCycle(cx *Context, o *cycleOpts) (Result, error) {
	var r Result
	if o.from == 0 {
		return r, Usagef("need -from <node>")
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return r, err
	}

	found, err := cx.Graph().HasCycle(store.NodeID(o.from), o.depth, ets)
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("from", ID(o.from))
	s.Add("depth", Int(int64(o.depth)))
	s.Add("cycle reachable", Bool(found))
	if found {
		// Not broken. A cycle is ordinary in most graphs; it is only a defect
		// in one that was meant to be acyclic, and the tool does not know
		// which this is.
		r.Find(SevInfo, "traverse.cycle",
			"a cycle is reachable from node %d within %d hops", o.from, o.depth)
	}
	return r, nil
}

// --- traverse subgraph ---

type subgraphOpts struct {
	ids  idList
	viz  string
	page Paging
}

var traverseSubgraph = cmd(Command{
	Group: "traverse", Name: "subgraph", Usage: "<dir>",
	Short: "the induced subgraph over a set of nodes",
	Long: "Every edge whose two endpoints are both in the set. Not a neighbourhood:\n" +
		"nothing outside the set is reached, and an edge to a node you did not\n" +
		"name is not included.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *subgraphOpts) {
		fs.Var(&o.ids, "id", "node ID in the set (repeatable, or comma-separated)")
		fs.StringVar(&o.viz, "viz", "", "also write an interactive HTML view")
		o.page.Bind(fs, 50, "rows to show per table (0 = all)")
	},
	runTraverseSubgraph)

func runTraverseSubgraph(cx *Context, o *subgraphOpts) (Result, error) {
	var r Result
	if len(o.ids) == 0 {
		return r, Usagef("need at least one -id")
	}

	nodes, edges, err := cx.Graph().InducedSubgraph(o.ids.nodeIDs())
	if err != nil {
		return r, err
	}

	nodeTable(&r, "nodes", nodes, o.page)
	edgeTable(&r, "edges", edges, o.page)

	s := r.Section("")
	s.Add("asked for", Int(int64(len(o.ids))))
	s.Add("nodes", Int(int64(len(nodes))))
	s.Add("edges", Int(int64(len(edges))))
	if len(nodes) < len(o.ids) {
		s.AddNote("not found", Int(int64(len(o.ids)-len(nodes))),
			"(IDs in the set that the store does not hold)")
	}
	return r, writeViz(&r, o.viz, nodes, edges, "induced subgraph")
}

// --- traverse pattern ---

type patternOpts struct {
	spec    string
	scope   idList
	max     int
	page    Paging
	budgets budgetOpts
}

var traversePattern = cmd(Command{
	Group: "traverse", Name: "pattern", Usage: "<dir>",
	Short: "find subgraphs matching a shape",
	Long: "VF2-inspired matching over a pattern read from a JSON file:\n\n" +
		"  {\"nodes\": [{\"labels\": [\"EvidenceFile\"]}, {\"labels\": [\"MicroArtefact\"]}],\n" +
		"   \"edges\": [{\"src\": 0, \"dst\": 1, \"labels\": [\"Contains\"]}]}\n\n" +
		"Node and edge indices in the edge list refer to positions in the node\n" +
		"list. Labels are the same selectors -type takes everywhere else.\n\n" +
		"Scope it. An unscoped match over a large store searches the whole graph,\n" +
		"and -scope with the IDs from a `node list` or a `traverse bfs` is the\n" +
		"difference between a second and an afternoon.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *patternOpts) {
		fs.StringVar(&o.spec, "spec", "", "JSON pattern file (required)")
		fs.Var(&o.scope, "scope", "limit the search to these node IDs (repeatable)")
		fs.IntVar(&o.max, "max", 100, "stop after this many matches (0 = all)")
		o.budgets.Bind(fs)
		o.page.Bind(fs, 50, "matches to show (0 = all)")
	},
	runTraversePattern)

func runTraversePattern(cx *Context, o *patternOpts) (Result, error) {
	var r Result
	if o.spec == "" {
		return r, Usagef("need -spec <file.json>")
	}
	pattern, err := loadPattern(o.spec)
	if err != nil {
		return r, err
	}
	bud, err := o.budgets.budget()
	if err != nil {
		return r, err
	}

	matches, err := cx.Graph().FindPatternsCtx(cx.Ctx, pattern, o.scope.nodeIDs(), o.max, bud)
	if err != nil {
		if budgetFinding(&r, err, o.budgets) {
			return r, nil
		}
		return r, err
	}

	lo, hi, trunc := o.page.Window(len(matches))
	t := r.Table("matches", RCol("#"), Col("mapping (pattern node -> store node)"))
	for i, m := range matches[lo:hi] {
		parts := make([]string, len(m.Mapping))
		for j, id := range m.Mapping {
			parts[j] = fmt.Sprintf("%d->%d", j, id)
		}
		t.Row(Int(int64(lo+i)), Str(strings.Join(parts, ", ")))
	}
	t.Truncated, t.Total = trunc, len(matches)

	s := r.Section("")
	s.Add("pattern", Str(o.spec))
	s.Add("pattern nodes", Int(int64(len(pattern.Nodes))))
	s.Add("pattern edges", Int(int64(len(pattern.Edges))))
	if len(o.scope) > 0 {
		s.Add("scope", Int(int64(len(o.scope))))
	} else {
		s.AddNote("scope", Str("whole graph"), "(-scope narrows it)")
	}
	s.Add("matches", Int(int64(len(matches))))
	if o.max > 0 && len(matches) == o.max {
		s.AddNote("capped", Bool(true), "(-max reached; there may be more)")
	}
	o.budgets.describe(s)
	return r, nil
}

// --- shared ---

// budgetFinding turns a budget refusal into a finding and reports whether it
// handled the error.
//
// A walk that hit its limit is not a broken store and not a broken command: it
// is a question that was too big for the limits it was asked under. Exiting
// non-zero on it would make every scripted traversal fail closed on a hub node,
// so it is a finding — visible, greppable by code, exit zero.
func budgetFinding(r *Result, err error, b budgetOpts) bool {
	if !errors.Is(err, store.ErrBudgetExceeded) {
		return false
	}
	r.Find(SevWarn, "traverse.budget_exceeded",
		"the walk hit a budget and was refused rather than truncated: %v", err)
	s := r.Section("")
	b.describe(s)
	s.Add("returned", Str("nothing — a partial walk is not returned as if it were complete"))
	return true
}

// writeViz optionally writes the interactive HTML view.
//
// Another library capability with no shell path before now: viz has been able
// to write a self-contained interactive page since Phase 2 and the only way to
// call it was from Go.
func writeViz(r *Result, path string, nodes []*store.Node, edges []*store.Edge, title string) error {
	if path == "" {
		return nil
	}
	if len(nodes) == 0 {
		return fmt.Errorf("-viz needs at least one node and the walk returned none")
	}
	if err := viz.ExportInteractiveHTMLWithOptions(nodes, edges, path,
		viz.ExportOptions{Title: "graphene", Subtitle: title}); err != nil {
		return err
	}
	r.Section("").Add("visualisation", Str(path))
	return nil
}
