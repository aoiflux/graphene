package main

// The node group: get, list, count, degree, neighbours, explain.
//
// None of this was reachable from a shell before. The engine has had a query
// planner, a property index with seven comparison operators and a degree API
// since Phase 1, and the only way to ask any of it a question was to write a Go
// program — which meant the tool shipped alongside the engine could inspect the
// files a store is made of but could not answer "what is in it".
//
// Every command here opens the graph read-only and so takes the shared lock:
// unlike `info` and `csr`, these questions cannot be answered from the files
// alone, because the answer depends on the WAL replayed over the image. That is
// a real cost and the Notice says so.

import (
	"flag"
	"strings"

	"github.com/aoiflux/graphene/store"
)

// roNotice is the same sentence on every read command in this group and the
// next. Written once so the fifteenth command cannot describe the lock
// differently from the first.
const roNotice = "opens the store read-only (shared lock); refused while a writer holds it"

// --- node get ---

type nodeGetOpts struct {
	ids idList
}

var nodeGet = cmd(Command{
	Group: "node", Name: "get", Usage: "<dir>",
	Short: "fetch nodes by ID",
	Long: "Repeat -id, or give it a comma-separated list, to fetch a batch in one\n" +
		"pass. An ID that is not there is reported as missing rather than as an\n" +
		"error: \"this node does not exist\" is a complete answer to the question\n" +
		"that was asked.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *nodeGetOpts) {
		fs.Var(&o.ids, "id", "node ID to fetch (repeatable, or comma-separated)")
	},
	runNodeGet)

func runNodeGet(cx *Context, o *nodeGetOpts) (Result, error) {
	var r Result
	if len(o.ids) == 0 {
		return r, Usagef("need at least one -id")
	}

	found, missing, err := cx.Graph().GetNodes(o.ids.nodeIDs())
	if err != nil {
		return r, err
	}

	nodeTable(&r, "nodes", found, Paging{})

	s := r.Section("")
	s.Add("asked for", Int(int64(len(o.ids))))
	s.Add("found", Int(int64(len(found))))
	missingNote(s, "missing", uint64s(missing))

	if cx.Globals.Verbose {
		ids := make([]uint64, len(found))
		blobs := make([][]byte, len(found))
		for i, n := range found {
			ids[i], blobs[i] = uint64(n.ID), n.Properties
		}
		propertyDump(&r, "properties (msgpack, hex)", ids, blobs)
	}
	return r, nil
}

// --- node list ---

type nodeListOpts struct {
	types stringList
	props stringList
	match string
	order string
	page  Paging
}

var nodeList = cmd(Command{
	Group: "node", Name: "list", Usage: "<dir>",
	Short: "query nodes by label and property",
	Long: "The window is pushed down to the query planner rather than applied to the\n" +
		"answer, so -limit bounds the work and not just the output. One row beyond\n" +
		"the limit is asked for to find out whether more match; that row is not\n" +
		"shown.\n\n" +
		"Use `node explain` with the same flags to see how the planner resolved it.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	bindNodeQuery, runNodeList)

func bindNodeQuery(fs *flag.FlagSet, o *nodeListOpts) {
	fs.Var(&o.types, "type", "node label to match (repeatable; OR semantics)")
	fs.Var(&o.props, "prop", filterHelp)
	fs.StringVar(&o.match, "match", "all", "all (AND) or any (OR) across -prop terms")
	fs.StringVar(&o.order, "order", "asc", "asc or desc, by ID")
	o.page.Bind(fs, 50, "rows to return (0 = all)")
}

// nodeQuery builds the library query from the flags, shared by list and explain
// so the plan reported is the plan the listing would run.
func (o *nodeListOpts) nodeQuery() (store.NodeQuery, error) {
	var q store.NodeQuery

	types, err := nodeTypes(o.types)
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
	q.Offset, q.Limit = o.page.Offset, o.page.Limit
	return q, nil
}

func runNodeList(cx *Context, o *nodeListOpts) (Result, error) {
	var r Result

	q, err := o.nodeQuery()
	if err != nil {
		return r, err
	}

	// One past the limit, so "there are more" is a fact rather than a guess.
	// Without it the only honest thing to print after exactly -limit rows is
	// nothing, and a reader has no way to tell a full page from a final one.
	probe := q.Limit > 0
	if probe {
		q.Limit++
	}

	nodes, err := cx.Graph().QueryNodesCtx(cx.Ctx, q)
	if err != nil {
		return r, err
	}
	more := probe && len(nodes) == q.Limit
	if more {
		nodes = nodes[:len(nodes)-1]
	}

	nodeTable(&r, "nodes", nodes, Paging{})
	describeQuery(&r, o.types, o.props, o.match, len(nodes), o.page, more)
	return r, nil
}

// --- node count ---

var nodeCount = plain(Command{
	Group: "node", Name: "count", Usage: "<dir>",
	Short:  "how many nodes the store holds",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
}, runNodeCount)

func runNodeCount(cx *Context) (Result, error) {
	var r Result
	n, err := cx.Graph().NodeCount()
	if err != nil {
		return r, err
	}
	e, err := cx.Graph().EdgeCount()
	if err != nil {
		return r, err
	}
	s := r.Section("")
	s.Add("nodes", Uint(n))
	s.Add("edges", Uint(e))
	return r, nil
}

// --- node degree ---

type nodeDegreeOpts struct {
	id        uint64
	edgeTypes stringList
}

var nodeDegree = cmd(Command{
	Group: "node", Name: "degree", Usage: "<dir>",
	Short: "how many edges touch a node, in and out",
	Long: "Reports all three figures rather than the one -direction would have\n" +
		"selected. They are cheap next to the open, and a node's shape is the\n" +
		"in/out split — a hub with 10 000 inbound and 1 outbound is a different\n" +
		"thing from the reverse, and a single number hides which one you have.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *nodeDegreeOpts) {
		fs.Uint64Var(&o.id, "id", 0, "node ID (required)")
		fs.Var(&o.edgeTypes, "edge-type", "count only these edge labels (repeatable)")
	},
	runNodeDegree)

func runNodeDegree(cx *Context, o *nodeDegreeOpts) (Result, error) {
	var r Result
	if o.id == 0 {
		return r, Usagef("need -id <node>")
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return r, err
	}

	g := cx.Graph()
	id := store.NodeID(o.id)

	in, err := g.InDegree(id, ets)
	if err != nil {
		return r, err
	}
	out, err := g.OutDegree(id, ets)
	if err != nil {
		return r, err
	}
	total, err := g.Degree(id, ets)
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("node", ID(o.id))
	s.Add("inbound", Int(int64(in)))
	s.Add("outbound", Int(int64(out)))
	s.Add("total", Int(int64(total)))
	if len(o.edgeTypes) > 0 {
		s.Add("edge labels", Str(o.edgeTypes.String()))
	}
	return r, nil
}

// --- node neighbours ---

type nodeNeighboursOpts struct {
	id        uint64
	direction string
	nodeType  string
	edgeTypes stringList
	page      Paging
}

var nodeNeighbours = cmd(Command{
	Group: "node", Name: "neighbours", Aliases: nil, Usage: "<dir>",
	Short: "the nodes one hop away, and the edges that reach them",
	Long: "One hop. For more than one, use `traverse bfs -depth N`, which bounds\n" +
		"the walk with a budget — this does not need one, because the answer is\n" +
		"the node's degree and no larger.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	func(fs *flag.FlagSet, o *nodeNeighboursOpts) {
		fs.Uint64Var(&o.id, "id", 0, "node ID (required)")
		fs.StringVar(&o.direction, "direction", "both", "out, in or both")
		fs.StringVar(&o.nodeType, "node-type", "", "keep only neighbours carrying this label")
		fs.Var(&o.edgeTypes, "edge-type", "follow only these edge labels (repeatable)")
		o.page.Bind(fs, 50, "rows to return (0 = all)")
	},
	runNodeNeighbours)

func runNodeNeighbours(cx *Context, o *nodeNeighboursOpts) (Result, error) {
	var r Result
	if o.id == 0 {
		return r, Usagef("need -id <node>")
	}
	dir, err := parseDirection(o.direction)
	if err != nil {
		return r, err
	}
	ets, err := edgeTypes(o.edgeTypes)
	if err != nil {
		return r, err
	}
	g := cx.Graph()
	id := store.NodeID(o.id)

	// NeighboursByNodeType returns nodes without their connecting edges, so the
	// two paths produce genuinely different reports rather than one being a
	// filtered version of the other. Said in the output rather than papered
	// over: a table with an "edge" column that is empty half the time would be
	// worse than two honest shapes.
	if o.nodeType != "" {
		nt, terr := store.ParseNodeType(o.nodeType)
		if terr != nil {
			return r, Usagef("-node-type %q: %v", o.nodeType, terr)
		}
		ns, nerr := g.NeighboursByNodeType(id, dir, nt, ets)
		if nerr != nil {
			return r, nerr
		}
		nodeTable(&r, "neighbours", ns, o.page)
		s := r.Section("")
		s.Add("node", ID(o.id))
		s.Add("direction", Str(directionName(dir)))
		s.Add("node label", Str(nt.String()))
		s.Add("neighbours", Int(int64(len(ns))))
		return r, nil
	}

	res, err := g.Neighbours(id, dir, ets)
	if err != nil {
		return r, err
	}

	lo, hi, trunc := o.page.Window(len(res))
	t := r.Table("neighbours",
		RCol("node"), Col("labels"), RCol("via edge"), Col("edge labels"), Col("direction"))
	for _, nr := range res[lo:hi] {
		via, elabels, way := Nil(), Nil(), Nil()
		if nr.Edge != nil {
			via = ID(uint64(nr.Edge.ID))
			elabels = Strs(edgeLabelList(nr.Edge))
			if nr.Edge.Src == id {
				way = Str("out")
			} else {
				way = Str("in")
			}
		}
		t.Row(ID(uint64(nr.Node.ID)), Strs(nodeLabelList(nr.Node)), via, elabels, way)
	}
	t.Truncated, t.Total = trunc, len(res)

	s := r.Section("")
	s.Add("node", ID(o.id))
	s.Add("direction", Str(directionName(dir)))
	s.Add("neighbours", Int(int64(len(res))))
	return r, nil
}

// --- node explain ---

var nodeExplain = cmd(Command{
	Group: "node", Name: "explain", Usage: "<dir>",
	Short: "how the planner would resolve a node query",
	Long: "Takes the same flags as `node list` and runs the plan rather than the\n" +
		"query. The planner's choices are diagnostic output and not an API\n" +
		"contract: they change as the cost model improves, and results never do.\n" +
		"Read it to find out why a query was slow, not to depend on a shape.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxAdvisory,
},
	bindNodeQuery, runNodeExplain)

func runNodeExplain(cx *Context, o *nodeListOpts) (Result, error) {
	var r Result
	q, err := o.nodeQuery()
	if err != nil {
		return r, err
	}
	plan, err := cx.Graph().ExplainNodeQuery(q)
	if err != nil {
		return r, err
	}
	addPlan(&r, plan)
	return r, nil
}

// --- shared ---

// parseOrder maps -order onto the library's QueryOrder.
func parseOrder(s string) (store.QueryOrder, error) {
	switch s {
	case "asc", "":
		return store.QueryOrderAsc, nil
	case "desc":
		return store.QueryOrderDesc, nil
	}
	return 0, Usagef("-order %q: want asc or desc", s)
}

// addPlan renders a QueryPlan, in both forms.
//
// The one-line String() the library already has goes in as well as the fields.
// It is what the engine's own tests print, so an operator quoting a plan in a
// bug report and a maintainer reading a test failure are looking at the same
// string.
func addPlan(r *Result, p store.QueryPlan) {
	s := r.Section("plan")
	s.Add("driver", Str(p.Driver.String()))
	if p.DriverKey != "" {
		s.Add("driver key", Str(p.DriverKey))
	}
	s.Add("candidates", Int(int64(p.Candidates)))
	s.Add("results", Int(int64(p.Results)))

	if len(p.Residuals) > 0 {
		t := r.Table("residual filters", Col("key"), Col("method"), RCol("cost"))
		for _, res := range p.Residuals {
			method := "set"
			if res.Probe {
				method = "probe"
			}
			t.Row(Str(res.Key), Str(method), Int(int64(res.Cost)))
		}
	}
	r.Notes("one line", p.String())
}

// describeQuery adds the closing summary shared by the listing commands: what
// was asked for, what came back, and whether there is more.
func describeQuery(r *Result, types, props []string, match string,
	shown int, page Paging, more bool) {

	s := r.Section("")
	if len(types) > 0 {
		s.Add("labels", Str(strings.Join(types, ",")))
	}
	for _, spec := range props {
		f, err := parseFilter(spec)
		if err != nil {
			continue // already parsed once without error to get here
		}
		s.Addf("filter", "%s %s %s", f.Key, opName(f.Op), filterValue(f))
	}
	if len(props) > 1 {
		s.Add("match", Str(match))
	}
	s.Add("shown", Int(int64(shown)))
	if page.Offset > 0 {
		s.Add("offset", Int(int64(page.Offset)))
	}
	if more {
		s.AddNote("more match", Bool(true), "(raise -limit, or page with -offset)")
	}
}

func filterValue(f store.PropertyFilter) string {
	if f.Op == store.PropertyOpBetweenInclusive {
		return string(f.Value) + " .. " + string(f.ValueUpper)
	}
	return string(f.Value)
}

// uint64s widens a slice of IDs for reporting.
func uint64s[T ~uint64](in []T) []uint64 {
	out := make([]uint64, len(in))
	for i, v := range in {
		out[i] = uint64(v)
	}
	return out
}
