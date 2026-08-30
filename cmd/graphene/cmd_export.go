package main

// export subgraph, export viz. The bundle is in cmd_bundle.go.
//
// Two ways of getting part of a store out of it, neither of which touches the
// store itself.
//
// `export subgraph` is the interesting one. bulk.ExportJSONL takes a
// bulk.Source, so scoping an export is a matter of putting an adapter in front
// of the graph that answers the enumeration with a subset — no second export
// path, no second place for the ordering rule to be got wrong. The adapter has
// to forward the property enumeration too: bulk refuses a source that cannot
// enumerate its property entries rather than quietly exporting without them,
// which is exactly the right refusal, and an adapter that dropped the interface
// would trip it.

import (
	"flag"
	"fmt"
	"os"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/store"
)

// --- export subgraph ---

type subgraphExportOpts struct {
	ids       idList
	from      uint64
	depth     int
	direction string
	edgeTypes stringList
	budget    budgetOpts
	format    string
	to        string
	skipProps bool
}

var exportSubgraph = cmd(Command{
	Group: "export", Name: "subgraph", Usage: "<dir>",
	Short: "export part of a graph, by ID set or by neighbourhood",
	Long: "Scope it either way:\n\n" +
		"  -id N         name the nodes, repeatable or comma-separated\n" +
		"  -from N -depth D   take everything within D hops instead\n\n" +
		"Every edge whose two endpoints are both in the scope is included, and no\n" +
		"edge that leaves it — so the result is a self-contained graph rather than\n" +
		"one with dangling references.\n\n" +
		"IDs are preserved in the dump and reassigned on import, so the result is\n" +
		"isomorphic to the region it came from and not identical to it.",
	Notice: roNotice, Open: OpenGraphRO, Streams: true, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *subgraphExportOpts) {
		fs.Var(&o.ids, "id", "node ID in the scope (repeatable, or comma-separated)")
		fs.Uint64Var(&o.from, "from", 0, "scope by neighbourhood: the node to start from")
		fs.IntVar(&o.depth, "depth", 2, "hops, with -from")
		fs.StringVar(&o.direction, "direction", "both", "out, in or both, with -from")
		fs.Var(&o.edgeTypes, "edge-type", "follow only these edge labels, with -from")
		fs.StringVar(&o.format, "format", "jsonl", "jsonl | dump | csv")
		fs.StringVar(&o.to, "to", "", "file (jsonl, dump) or directory (csv) to write (required)")
		fs.BoolVar(&o.skipProps, "no-properties", false, "omit indexed property entries")
		o.budget.Bind(fs)
	},
	runExportSubgraph)

func runExportSubgraph(cx *Context, o *subgraphExportOpts) (Result, error) {
	var r Result
	if o.to == "" {
		return r, Usagef("need -to <path>")
	}
	if _, ok := exportFormats[o.format]; !ok {
		return r, Usagef("unknown format %q; want one of %s", o.format, formatList())
	}
	if len(o.ids) == 0 && o.from == 0 {
		return r, Usagef("need -id (repeatable) or -from, to say what to export")
	}
	if len(o.ids) > 0 && o.from != 0 {
		// Two scopes would have to be combined, and neither union nor
		// intersection is obviously what was meant. Refusing beats guessing.
		return r, Usagef("give -id or -from, not both")
	}

	g := cx.Graph()
	scope := o.ids.nodeIDs()
	if o.from != 0 {
		dir, err := parseDirection(o.direction)
		if err != nil {
			return r, err
		}
		ets, err := edgeTypes(o.edgeTypes)
		if err != nil {
			return r, err
		}
		bud, err := o.budget.budget()
		if err != nil {
			return r, err
		}
		// BFSIDs rather than BFS: the walk only has to establish which nodes
		// are in scope, and materialising every record twice — once for the
		// walk and once for the export — is work for nothing.
		scope, err = g.BFSIDsCtx(cx.Ctx, store.NodeID(o.from), o.depth, dir, ets, bud)
		if err != nil {
			if budgetFinding(&r, err, o.budget) {
				return r, nil
			}
			return r, err
		}
	}
	if len(scope) == 0 {
		return r, Usagef("the scope is empty; there is nothing to export")
	}

	src := &scopedSource{g: g, nodes: make(map[store.NodeID]bool, len(scope))}
	for _, id := range scope {
		src.nodes[id] = true
	}

	sum, err := writeExport(o.format, o.to, src, bulk.Options{SkipProperties: o.skipProps})
	if err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("to", Str(o.to))
	s.Add("format", Str(o.format))
	if o.from != 0 {
		s.Addf("scope", "%d nodes within %d hops of node %d", len(scope), o.depth, o.from)
	} else {
		s.Addf("scope", "%d nodes named", len(scope))
	}
	s.Add("nodes", Int(int64(sum.Nodes)))
	s.Add("edges", Int(int64(sum.Edges)))
	s.Add("node properties", Int(int64(sum.NodeProperties)))
	s.Add("edge properties", Int(int64(sum.EdgeProperties)))
	r.Notice("IDs are reassigned on import: the result is isomorphic to this region, not identical")
	return r, nil
}

// scopedSource restricts a graph to a set of nodes.
//
// The edge rule is the one that matters: an edge is in scope only when *both*
// endpoints are, so the export cannot produce a dangling reference. An edge
// leaving the scope is dropped rather than pulling its far endpoint in, because
// a scope that grows to close its own edges is not the scope that was asked
// for.
type scopedSource struct {
	g     *graphene.Graph
	nodes map[store.NodeID]bool
}

func (s *scopedSource) QueryNodeIDs(q store.NodeQuery) ([]store.NodeID, error) {
	all, err := s.g.QueryNodeIDs(q)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, id := range all {
		if s.nodes[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *scopedSource) QueryEdgeIDs(q store.EdgeQuery) ([]store.EdgeID, error) {
	all, err := s.g.QueryEdgeIDs(q)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, id := range all {
		e, err := s.g.GetEdge(id)
		if err != nil {
			continue // deleted between the enumeration and the fetch
		}
		if s.nodes[e.Src] && s.nodes[e.Dst] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *scopedSource) GetNode(id store.NodeID) (*store.Node, error) { return s.g.GetNode(id) }
func (s *scopedSource) GetEdge(id store.EdgeID) (*store.Edge, error) { return s.g.GetEdge(id) }

// ForEachNodeProperty and ForEachEdgeProperty make this a
// store.PropertyEnumerator, which bulk requires: a source that cannot enumerate
// its property entries is refused rather than silently exported without them.
// An adapter that forgot these would trip that refusal, which is the refusal
// working.
func (s *scopedSource) ForEachNodeProperty(fn func(id store.NodeID, key string, value []byte) bool) {
	s.g.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
		if !s.nodes[id] {
			return true
		}
		return fn(id, key, value)
	})
}

func (s *scopedSource) ForEachEdgeProperty(fn func(id store.EdgeID, key string, value []byte) bool) {
	s.g.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
		e, err := s.g.GetEdge(id)
		if err != nil || !s.nodes[e.Src] || !s.nodes[e.Dst] {
			return true
		}
		return fn(id, key, value)
	})
}

// writeExport is the format switch, shared with `export graph`.
func writeExport(format, to string, src bulk.Source, opts bulk.Options) (bulk.Summary, error) {
	if format == "csv" {
		return bulk.ExportCSV(to, src, opts)
	}
	// O_EXCL: refusing to overwrite is the rule ExportCSV applies to a directory
	// that already holds a manifest, for the same reason — a half-overwritten
	// export is worse than no export.
	f, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return bulk.Summary{}, fmt.Errorf("create %s: %w", to, err)
	}
	var sum bulk.Summary
	if format == "jsonl" {
		sum, err = bulk.ExportJSONL(f, src, opts)
	} else {
		sum, err = bulk.ExportDump(f, src, opts)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return sum, err
}

// --- export viz ---

type vizOpts struct {
	ids       idList
	from      uint64
	depth     int
	direction string
	edgeTypes stringList
	budget    budgetOpts
	to        string
	title     string
}

var exportViz = cmd(Command{
	Group: "export", Name: "viz", Usage: "<dir>",
	Short: "a self-contained interactive HTML view of part of a graph",
	Long: "Scoped the same way as `export subgraph`: -id for a named set, -from and\n" +
		"-depth for a neighbourhood.\n\n" +
		"Scope it deliberately. The output embeds every node and edge in the page,\n" +
		"so a whole large store produces a file no browser will open — this is a\n" +
		"way of looking at a region, not at a store.\n\n" +
		"The `traverse` commands take -viz directly, which is usually easier: the\n" +
		"walk you were doing anyway becomes the picture.",
	Notice: roNotice, Open: OpenGraphRO, Tier: CtxExact,
},
	func(fs *flag.FlagSet, o *vizOpts) {
		fs.Var(&o.ids, "id", "node ID to include (repeatable, or comma-separated)")
		fs.Uint64Var(&o.from, "from", 0, "scope by neighbourhood: the node to start from")
		fs.IntVar(&o.depth, "depth", 2, "hops, with -from")
		fs.StringVar(&o.direction, "direction", "both", "out, in or both, with -from")
		fs.Var(&o.edgeTypes, "edge-type", "follow only these edge labels, with -from")
		fs.StringVar(&o.to, "to", "", "HTML file to write (required)")
		fs.StringVar(&o.title, "title", "", "subtitle for the page")
		o.budget.Bind(fs)
	},
	runExportViz)

func runExportViz(cx *Context, o *vizOpts) (Result, error) {
	var r Result
	if o.to == "" {
		return r, Usagef("need -to <file.html>")
	}
	if len(o.ids) == 0 && o.from == 0 {
		return r, Usagef("need -id (repeatable) or -from, to say what to draw")
	}
	if len(o.ids) > 0 && o.from != 0 {
		return r, Usagef("give -id or -from, not both")
	}

	g := cx.Graph()
	var nodes []*store.Node
	var edges []*store.Edge
	var err error

	if o.from != 0 {
		dir, derr := parseDirection(o.direction)
		if derr != nil {
			return r, derr
		}
		ets, terr := edgeTypes(o.edgeTypes)
		if terr != nil {
			return r, terr
		}
		bud, berr := o.budget.budget()
		if berr != nil {
			return r, berr
		}
		res, werr := g.BFSCtx(cx.Ctx, store.NodeID(o.from), o.depth, dir, ets, bud)
		if werr != nil {
			if budgetFinding(&r, werr, o.budget) {
				return r, nil
			}
			return r, werr
		}
		nodes, edges = res.Nodes, res.Edges
	} else {
		nodes, edges, err = g.InducedSubgraph(o.ids.nodeIDs())
		if err != nil {
			return r, err
		}
	}

	subtitle := o.title
	if subtitle == "" {
		if o.from != 0 {
			subtitle = fmt.Sprintf("%d hops from node %d", o.depth, o.from)
		} else {
			subtitle = fmt.Sprintf("%d nodes", len(o.ids))
		}
	}
	if err := writeViz(&r, o.to, nodes, edges, subtitle); err != nil {
		return r, err
	}

	s := r.Section("")
	s.Add("nodes", Int(int64(len(nodes))))
	s.Add("edges", Int(int64(len(edges))))
	return r, nil
}
