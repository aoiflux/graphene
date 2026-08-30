package main

// Turning command-line strings into query values.
//
// Every read command in the node, edge, traverse and query groups takes some
// combination of the same four things: a set of IDs, a set of labels, a set of
// property filters, and a direction. Parsing them once here rather than per
// command is not only about repetition — it is what makes `-type` mean the same
// thing on `node list` as on `traverse bfs`, and what keeps the error message
// for a malformed filter identical wherever it is typed.
//
// All of it goes through the library's own parsers. store.ParseNodeType already
// accepts names, snake_case, `custom:7` and bare numerics, and reimplementing
// any part of that here would create a second, worse answer to a question the
// engine has already answered.

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aoiflux/graphene/store"
)

// --- repeatable flags ---

// stringList collects a repeatable string flag. Values may also be
// comma-separated, so `-type a -type b` and `-type a,b` agree.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

// idList collects a repeatable numeric ID flag, comma-separated or not.
type idList []uint64

func (l *idList) String() string {
	parts := make([]string, len(*l))
	for i, v := range *l {
		parts[i] = strconv.FormatUint(v, 10)
	}
	return strings.Join(parts, ",")
}

func (l *idList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not an ID: %w", part, err)
		}
		if n == 0 {
			// Zero is not a valid entity ID anywhere in the engine, and a
			// silently-ignored 0 in a batch is the kind of thing that makes a
			// `node get` look like it found everything it was asked for.
			return fmt.Errorf("0 is not an entity ID")
		}
		*l = append(*l, n)
	}
	return nil
}

func (l idList) nodeIDs() []store.NodeID {
	out := make([]store.NodeID, len(l))
	for i, v := range l {
		out[i] = store.NodeID(v)
	}
	return out
}

func (l idList) edgeIDs() []store.EdgeID {
	out := make([]store.EdgeID, len(l))
	for i, v := range l {
		out[i] = store.EdgeID(v)
	}
	return out
}

// --- types ---

// nodeTypes parses `-type` selectors through the library's own parser.
func nodeTypes(specs []string) ([]store.NodeType, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make([]store.NodeType, 0, len(specs))
	for _, s := range specs {
		t, err := store.ParseNodeType(s)
		if err != nil {
			return nil, Usagef("-type %q: %v", s, err)
		}
		out = append(out, t)
	}
	return out, nil
}

// edgeTypes parses `-edge-type` / `-type` selectors for edges.
func edgeTypes(specs []string) ([]store.EdgeType, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make([]store.EdgeType, 0, len(specs))
	for _, s := range specs {
		t, err := store.ParseEdgeType(s)
		if err != nil {
			return nil, Usagef("-edge-type %q: %v", s, err)
		}
		out = append(out, t)
	}
	return out, nil
}

// --- direction ---

// parseDirection maps the -direction flag onto store.Direction.
//
// Directed storage is not a query constraint: an edge is stored with a Src and
// a Dst and that never changes. This flag says which side of an edge the node
// being asked about has to be on, which is why `both` is a perfectly ordinary
// answer and not a way of creating an undirected edge.
func parseDirection(s string) (store.Direction, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "out", "outbound", "":
		return store.DirectionOutbound, nil
	case "in", "inbound":
		return store.DirectionInbound, nil
	case "both", "any":
		return store.DirectionBoth, nil
	}
	return 0, Usagef("-direction %q: want out, in or both", s)
}

func directionName(d store.Direction) string {
	switch d {
	case store.DirectionInbound:
		return "in"
	case store.DirectionBoth:
		return "both"
	default:
		return "out"
	}
}

// --- property filters ---

// filterHelp is the one description of the filter grammar, shared by every
// command that binds -prop so the help cannot disagree with the parser.
const filterHelp = "property filter (repeatable): key=v equals, key~v contains, " +
	"key^v prefix, key>v, key>=v, key<v, key<=v, key[lo:hi] between-inclusive"

// parseFilters turns the -prop specs into the library's filter type.
func parseFilters(specs []string, mode string) ([]store.PropertyFilter, store.MatchMode, error) {
	var m store.MatchMode
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "all", "and", "":
		m = store.MatchAll
	case "any", "or":
		m = store.MatchAny
	default:
		return nil, 0, Usagef("-match %q: want all or any", mode)
	}
	if len(specs) == 0 {
		return nil, m, nil
	}
	out := make([]store.PropertyFilter, 0, len(specs))
	for _, s := range specs {
		f, err := parseFilter(s)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, f)
	}
	return out, m, nil
}

// parseFilter reads one `key<op>value` term.
//
// The operator is found as the first character of the operator set, so a value
// may contain any of them: `path^C:\Users` is a prefix filter on a value that
// itself holds a colon and a backslash. A key may not, which is the trade — and
// the right way round, because keys are chosen by whoever built the store and
// values are whatever the evidence contained.
func parseFilter(spec string) (store.PropertyFilter, error) {
	var f store.PropertyFilter

	i := strings.IndexAny(spec, "=~^><[")
	if i <= 0 {
		return f, Usagef("-prop %q: want key=value (or ~ ^ > >= < <= [lo:hi])", spec)
	}
	f.Key, spec = spec[:i], spec[i:]

	switch {
	case strings.HasPrefix(spec, ">="):
		f.Op, f.Value = store.PropertyOpGreaterThanOrEqual, []byte(spec[2:])
	case strings.HasPrefix(spec, "<="):
		f.Op, f.Value = store.PropertyOpLessThanOrEqual, []byte(spec[2:])
	case spec[0] == '=':
		f.Op, f.Value = store.PropertyOpEqual, []byte(spec[1:])
	case spec[0] == '~':
		f.Op, f.Value = store.PropertyOpContains, []byte(spec[1:])
	case spec[0] == '^':
		f.Op, f.Value = store.PropertyOpPrefix, []byte(spec[1:])
	case spec[0] == '>':
		f.Op, f.Value = store.PropertyOpGreaterThan, []byte(spec[1:])
	case spec[0] == '<':
		f.Op, f.Value = store.PropertyOpLessThan, []byte(spec[1:])
	case spec[0] == '[':
		if !strings.HasSuffix(spec, "]") {
			return f, Usagef("-prop %q: a between filter is key[lo:hi]", f.Key+spec)
		}
		lo, hi, ok := strings.Cut(spec[1:len(spec)-1], ":")
		if !ok {
			return f, Usagef("-prop %q: a between filter is key[lo:hi]", f.Key+spec)
		}
		f.Op, f.Value, f.ValueUpper = store.PropertyOpBetweenInclusive, []byte(lo), []byte(hi)
	}
	return f, nil
}

// opName renders an operator for the report, so a reader can check that what
// was parsed is what they meant.
func opName(op store.PropertyOp) string {
	switch op {
	case store.PropertyOpPrefix:
		return "prefix"
	case store.PropertyOpContains:
		return "contains"
	case store.PropertyOpGreaterThan:
		return ">"
	case store.PropertyOpGreaterThanOrEqual:
		return ">="
	case store.PropertyOpLessThan:
		return "<"
	case store.PropertyOpLessThanOrEqual:
		return "<="
	case store.PropertyOpBetweenInclusive:
		return "between"
	default:
		return "="
	}
}

// --- budgets ---

// budgetOpts are the traversal limits, bound by every command that walks.
//
// Depth alone does not bound a walk: one hub of degree 100 000 puts 100 000
// entries in the visited set at depth one and the walk has no way to say it is
// in trouble. store.Budget is how the library bounds that, and a CLI that never
// set one would be the easiest way in the tree to exhaust a machine's memory
// against a graph whose shape nobody knew.
type budgetOpts struct {
	maxNodes int
	maxEdges int
	maxTime  string
}

func (b *budgetOpts) Bind(fs *flag.FlagSet) {
	fs.IntVar(&b.maxNodes, "max-nodes", 100000, "stop after visiting this many nodes (0 = unlimited)")
	fs.IntVar(&b.maxEdges, "max-edges", 0, "stop after crossing this many edges (0 = unlimited)")
	fs.StringVar(&b.maxTime, "max-time", "", "stop after this much wall clock (e.g. 30s; empty = unlimited)")
}

func (b budgetOpts) budget() (store.Budget, error) {
	bud := store.Budget{MaxNodes: b.maxNodes, MaxEdges: b.maxEdges}
	if b.maxTime != "" {
		d, err := time.ParseDuration(b.maxTime)
		if err != nil {
			return bud, Usagef("-max-time %q: %v", b.maxTime, err)
		}
		bud.MaxTime = d
	}
	return bud, nil
}

// describe adds the limits to a report, so an answer that was cut short can be
// read alongside what cut it.
func (b budgetOpts) describe(s *Section) {
	if b.maxNodes > 0 {
		s.Add("max nodes", Int(int64(b.maxNodes)))
	}
	if b.maxEdges > 0 {
		s.Add("max edges", Int(int64(b.maxEdges)))
	}
	if b.maxTime != "" {
		s.Add("max time", Str(b.maxTime))
	}
}
