package graphene_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/index/encoding"
	"github.com/aoiflux/graphene/store"
)

// Residual filter evaluation changes *how* a multi-filter query is computed:
// filters the planner did not use as the driver are now tested against the
// candidates through the reverse map, rather than each being resolved to its own
// set and intersected. Same answer, different work.
//
// "Same answer" is the whole claim, so these tests check results against
// expectations computed in the test itself, not against the engine's other code
// path. A self-consistency check would pass just as happily if both paths were
// wrong in the same way.
//
// The sharp edge is comparison. An undeclared key compares numerically when both
// operands parse as numbers and byte-wise otherwise; a key declared ordered
// compares byte-wise throughout. The probe has to pick the same rule the index
// would have picked for that key, and the ordered cases below exist to catch it
// if it does not.

type residualFixture struct {
	g      *graphene.Graph
	ids    []store.NodeID
	sha    []string
	bucket []string
	tool   []string
	size   []int64
}

// newResidualFixture builds a graph whose property values are known to the test,
// so expected results can be computed directly rather than asked of the engine.
//
// The string values are deliberately non-numeric, which makes the scan rule and
// byte order agree on them. Numeric comparison is exercised separately, where it
// is the point rather than a confound.
func newResidualFixture(t *testing.T, g *graphene.Graph, n int) *residualFixture {
	t.Helper()
	f := &residualFixture{g: g}
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{benchLabelFor(i)}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	f.ids = ids
	for i, id := range ids {
		sha := fmt.Sprintf("hash-%06d", i)
		bucket := fmt.Sprintf("bucket-%03d", i%50)
		tool := fmt.Sprintf("tool-%d", i%7)
		size := int64(i%997) * 13
		f.sha = append(f.sha, sha)
		f.bucket = append(f.bucket, bucket)
		f.tool = append(f.tool, tool)
		f.size = append(f.size, size)
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"sha256": []byte(sha),
			"bucket": []byte(bucket),
			"tool":   []byte(tool),
			"size":   encoding.Int64(size),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}
	return f
}

// want computes the expected ID set for a predicate over the fixture's own data.
func (f *residualFixture) want(pred func(i int) bool) []store.NodeID {
	var out []store.NodeID
	for i, id := range f.ids {
		if pred(i) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

func assertIDs(t *testing.T, label string, got, want []store.NodeID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d ids, want %d", label, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: ids differ at %d: got %d, want %d", label, i, got[i], want[i])
		}
	}
}

func TestResidual_MatchAllAgreesWithExpectedSemantics(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newResidualFixture(t, g, 600)

			cases := []struct {
				name    string
				filters []store.PropertyFilter
				pred    func(i int) bool
			}{
				{
					// A unique driver plus a residual the index cannot serve.
					// This is the case the change exists for: without it the
					// Contains costs a scan of every "tool" entry to narrow one
					// candidate.
					name: "unique-equality + contains",
					filters: []store.PropertyFilter{
						{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000123")},
						{Key: "tool", Op: store.PropertyOpContains, Value: []byte("ool-")},
					},
					pred: func(i int) bool {
						return f.sha[i] == "hash-000123" && strings.Contains(f.tool[i], "ool-")
					},
				},
				{
					name: "unique-equality + non-matching contains",
					filters: []store.PropertyFilter{
						{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000123")},
						{Key: "tool", Op: store.PropertyOpContains, Value: []byte("nope")},
					},
					pred: func(i int) bool { return false },
				},
				{
					// Driver is the smaller of the two equality sets; the other
					// is applied as a residual.
					name: "two equalities of different selectivity",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
						{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-0")},
					},
					pred: func(i int) bool {
						return f.bucket[i] == "bucket-007" && f.tool[i] == "tool-0"
					},
				},
				{
					name: "equality + prefix",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
						{Key: "sha256", Op: store.PropertyOpPrefix, Value: []byte("hash-0000")},
					},
					pred: func(i int) bool {
						return f.bucket[i] == "bucket-007" && strings.HasPrefix(f.sha[i], "hash-0000")
					},
				},
				{
					// No filter can drive; the candidate set is every node and
					// building each residual's set should win over probing.
					name: "three broad filters, no selective driver",
					filters: []store.PropertyFilter{
						{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-3")},
						{Key: "sha256", Op: store.PropertyOpPrefix, Value: []byte("hash-00")},
						{Key: "bucket", Op: store.PropertyOpContains, Value: []byte("-0")},
					},
					pred: func(i int) bool {
						return f.tool[i] == "tool-3" &&
							strings.HasPrefix(f.sha[i], "hash-00") &&
							strings.Contains(f.bucket[i], "-0")
					},
				},
				{
					// A filter repeated. Whichever copy the planner consumes,
					// the other must still be applied, and the answer is the
					// same as applying it once.
					name: "duplicate filters",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-011")},
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-011")},
					},
					pred: func(i int) bool { return f.bucket[i] == "bucket-011" },
				},
				{
					// Contradictory filters must produce nothing, not the
					// driver's set.
					name: "contradiction",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-011")},
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-012")},
					},
					pred: func(i int) bool { return false },
				},
				{
					// A key the entity has no entry for cannot match.
					name: "unknown key",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-011")},
						{Key: "absent", Op: store.PropertyOpEqual, Value: []byte("x")},
					},
					pred: func(i int) bool { return false },
				},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got, err := g.QueryNodeIDs(store.NodeQuery{
						Filters: tc.filters, FilterMode: store.MatchAll,
					})
					if err != nil {
						t.Fatalf("QueryNodeIDs: %v", err)
					}
					assertIDs(t, tc.name, got, f.want(tc.pred))
				})
			}
		})
	}
}

// The comparison rule a probe uses has to match the rule the index would have
// used for that key, and the two rules genuinely disagree. These pin both sides.
func TestResidual_OrderedKeysCompareByteWise(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newResidualFixture(t, g, 600)
			if err := g.DeclareOrderedProperty("size"); err != nil {
				t.Fatalf("DeclareOrderedProperty: %v", err)
			}

			// A selective driver forces "size" to be evaluated as a residual —
			// by probe, since the candidate set is tiny — while the declaration
			// means the index would have answered it byte-wise.
			for _, tc := range []struct {
				name    string
				filters []store.PropertyFilter
				pred    func(i int) bool
			}{
				{
					name: "driven equality + ordered range residual",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
						{Key: "size", Op: store.PropertyOpGreaterThan, Value: encoding.Int64(5000)},
					},
					pred: func(i int) bool { return f.bucket[i] == "bucket-007" && f.size[i] > 5000 },
				},
				{
					name: "driven equality + ordered between residual",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
						{Key: "size", Op: store.PropertyOpBetweenInclusive,
							Value: encoding.Int64(2000), ValueUpper: encoding.Int64(9000)},
					},
					pred: func(i int) bool {
						return f.bucket[i] == "bucket-007" && f.size[i] >= 2000 && f.size[i] <= 9000
					},
				},
				{
					// The ordered key drives, and the residual is the string key.
					name: "ordered range drives, equality residual",
					filters: []store.PropertyFilter{
						{Key: "size", Op: store.PropertyOpBetweenInclusive,
							Value: encoding.Int64(0), ValueUpper: encoding.Int64(200)},
						{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-2")},
					},
					pred: func(i int) bool {
						return f.size[i] >= 0 && f.size[i] <= 200 && f.tool[i] == "tool-2"
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := g.QueryNodeIDs(store.NodeQuery{
						Filters: tc.filters, FilterMode: store.MatchAll,
					})
					if err != nil {
						t.Fatalf("QueryNodeIDs: %v", err)
					}
					assertIDs(t, tc.name, got, f.want(tc.pred))
				})
			}
		})
	}
}

// Undeclared numeric-looking values keep the scan rule, where "9" > "10". If a
// probe used byte order on such a key it would disagree with every other path,
// so this pins the rule rather than the implementation.
func TestResidual_UndeclaredKeysKeepScanRule(t *testing.T) {
	g := graphene.NewInMemory()
	// Values chosen so numeric and byte order disagree: byte-wise "10" < "9".
	values := []string{"9", "10", "100", "25"}
	ids := make([]store.NodeID, len(values))
	for i, v := range values {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeTag}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids[i] = id
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"count": []byte(v),
			"group": []byte("g"),
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
	}

	// "group" drives (one value, every node), leaving "count" as a residual
	// evaluated by probe. Numerically, > 9 selects 10, 100 and 25.
	got, err := g.QueryNodeIDs(store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "group", Op: store.PropertyOpEqual, Value: []byte("g")},
			{Key: "count", Op: store.PropertyOpGreaterThan, Value: []byte("9")},
		},
		FilterMode: store.MatchAll,
	})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	want := []store.NodeID{ids[1], ids[2], ids[3]} // 10, 100, 25
	sort.Slice(want, func(a, b int) bool { return want[a] < want[b] })
	assertIDs(t, "scan rule", got, want)
}

// MatchAny is untouched by the change — a set driven by one filter is not a
// superset of a union — but it shares the plumbing, so it is worth pinning.
func TestResidual_MatchAnyUnaffected(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newResidualFixture(t, g, 400)

			got, err := g.QueryNodeIDs(store.NodeQuery{
				Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
					{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-0")},
				},
				FilterMode: store.MatchAny,
			})
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			assertIDs(t, "MatchAny", got, f.want(func(i int) bool {
				return f.bucket[i] == "bucket-007" || f.tool[i] == "tool-0"
			}))
		})
	}
}

// Deletions must not survive into a narrowed result: the probe reads the index's
// reverse map, which is a different structure from the records.
func TestResidual_NarrowedResultsExcludeDeleted(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newResidualFixture(t, g, 400)

			deleted := make(map[int]bool)
			for i := 0; i < len(f.ids); i += 3 {
				if err := g.DeleteNode(f.ids[i]); err != nil {
					t.Fatalf("DeleteNode: %v", err)
				}
				deleted[i] = true
			}

			got, err := g.QueryNodeIDs(store.NodeQuery{
				Filters: []store.PropertyFilter{
					{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
					{Key: "tool", Op: store.PropertyOpContains, Value: []byte("ool")},
				},
				FilterMode: store.MatchAll,
			})
			if err != nil {
				t.Fatalf("QueryNodeIDs: %v", err)
			}
			assertIDs(t, "after deletes", got, f.want(func(i int) bool {
				return !deleted[i] && f.bucket[i] == "bucket-007" && strings.Contains(f.tool[i], "ool")
			}))
		})
	}
}

// Planner regression tests.
//
// The tests above pin the answers. These pin the *work*, which the answers
// cannot: a query can return exactly the right IDs while scanning the whole
// graph to do it, and no assertion on results would notice. That is also what
// makes them the check on the residual change itself — without them, the
// semantics tests would pass just as well if the new path never ran.
//
// These assert planner behaviour, which is explicitly not part of the API
// contract. When the cost model improves they are expected to need updating;
// when the result assertions need updating, something is wrong.
func TestPlanner_ChoosesTheExpectedIndex(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newResidualFixture(t, g, 600)
			if err := g.DeclareOrderedProperty("size"); err != nil {
				t.Fatalf("DeclareOrderedProperty: %v", err)
			}

			cases := []struct {
				name       string
				query      store.NodeQuery
				wantDriver store.DriverKind
				wantKey    string
				// wantProbe, when set, names a residual key that must be applied
				// by probing the candidates rather than by building its own set.
				wantProbe string
				maxCands  int // 0 means unchecked
			}{
				{
					name: "unique equality drives, and beats the label list",
					query: store.NodeQuery{
						Types: []store.NodeType{store.NodeTypeCase},
						Filters: []store.PropertyFilter{
							{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000123")},
						},
					},
					wantDriver: store.DriverEquality,
					wantKey:    "sha256",
					maxCands:   1,
				},
				{
					name: "the more selective of two equalities drives",
					query: store.NodeQuery{Filters: []store.PropertyFilter{
						{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-0")},
						{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000123")},
					}},
					wantDriver: store.DriverEquality,
					wantKey:    "sha256",
					maxCands:   1,
				},
				{
					// The point of the residual change: one candidate must not
					// trigger a scan of every "tool" entry to eliminate it.
					name: "a filter the index cannot serve is probed, not built",
					query: store.NodeQuery{Filters: []store.PropertyFilter{
						{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000123")},
						{Key: "tool", Op: store.PropertyOpContains, Value: []byte("ool")},
					}},
					wantDriver: store.DriverEquality,
					wantKey:    "sha256",
					wantProbe:  "tool",
					maxCands:   1,
				},
				{
					name: "a declared ordered key drives a range",
					query: store.NodeQuery{Filters: []store.PropertyFilter{
						{Key: "size", Op: store.PropertyOpBetweenInclusive,
							Value: encoding.Int64(0), ValueUpper: encoding.Int64(100)},
					}},
					wantDriver: store.DriverOrdered,
					wantKey:    "size",
				},
				{
					name:       "labels drive when no filter can",
					query:      store.NodeQuery{Types: []store.NodeType{store.NodeTypeCase}},
					wantDriver: store.DriverLabels,
				},
				{
					name:       "an unconstrained query scans",
					query:      store.NodeQuery{},
					wantDriver: store.DriverScan,
				},
				{
					name: "explicit IDs drive",
					query: store.NodeQuery{IDs: []store.NodeID{f.ids[0], f.ids[1]},
						Filters: []store.PropertyFilter{
							{Key: "tool", Op: store.PropertyOpContains, Value: []byte("ool")},
						}},
					wantDriver: store.DriverIDs,
					wantProbe:  "tool",
					maxCands:   2,
				},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					plan, err := g.ExplainNodeQuery(tc.query)
					if err != nil {
						t.Fatalf("ExplainNodeQuery: %v", err)
					}
					if plan.Driver != tc.wantDriver {
						t.Errorf("driver = %v, want %v — plan: %s", plan.Driver, tc.wantDriver, plan)
					}
					if tc.wantKey != "" && plan.DriverKey != tc.wantKey {
						t.Errorf("driver key = %q, want %q — plan: %s", plan.DriverKey, tc.wantKey, plan)
					}
					if tc.maxCands > 0 && plan.Candidates > tc.maxCands {
						t.Errorf("candidates = %d, want <= %d — plan: %s", plan.Candidates, tc.maxCands, plan)
					}
					if tc.wantProbe != "" {
						found := false
						for _, r := range plan.Residuals {
							if r.Key == tc.wantProbe {
								found = true
								if !r.Probe {
									t.Errorf("residual %q built its own set; expected a probe — plan: %s",
										r.Key, plan)
								}
							}
						}
						if !found {
							t.Errorf("no residual for %q — plan: %s", tc.wantProbe, plan)
						}
					}
				})
			}
		})
	}
}

// The driving filter must not be evaluated twice: it built the candidate set, so
// re-deriving it is pure waste.
func TestPlanner_DrivingFilterIsNotReapplied(t *testing.T) {
	g := graphene.NewInMemory()
	f := newResidualFixture(t, g, 300)
	_ = f

	plan, err := g.ExplainNodeQuery(store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "sha256", Op: store.PropertyOpEqual, Value: []byte("hash-000042")},
		{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-0")},
	}})
	if err != nil {
		t.Fatalf("ExplainNodeQuery: %v", err)
	}
	if plan.DriverKey != "sha256" {
		t.Fatalf("expected sha256 to drive, got %s", plan)
	}
	for _, r := range plan.Residuals {
		if r.Key == "sha256" {
			t.Fatalf("the driving filter was scheduled as a residual — plan: %s", plan)
		}
	}
	if len(plan.Residuals) != 1 || plan.Residuals[0].Key != "tool" {
		t.Fatalf("expected exactly the tool residual, got %s", plan)
	}
}

// Residuals run most-selective-first so candidates die as early as possible.
func TestPlanner_ResidualsAreOrderedMostSelectiveFirst(t *testing.T) {
	g := graphene.NewInMemory()
	newResidualFixture(t, g, 600)

	plan, err := g.ExplainNodeQuery(store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "tool", Op: store.PropertyOpEqual, Value: []byte("tool-0")},     // ~86 nodes
		{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-1")}, // 0 nodes
		{Key: "sha256", Op: store.PropertyOpPrefix, Value: []byte("hash-0")},  // whole key
	}})
	if err != nil {
		t.Fatalf("ExplainNodeQuery: %v", err)
	}
	for i := 1; i < len(plan.Residuals); i++ {
		if plan.Residuals[i-1].Cost > plan.Residuals[i].Cost {
			t.Fatalf("residuals out of order at %d — plan: %s", i, plan)
		}
	}
}

// Contains and Equal on a key that *is* declared ordered.
//
// These are the two operators the ordered index declines to serve, so the
// set-building path falls back to a scan using the scan-rule comparator while a
// probe of the same filter uses the byte-wise one. The two comparators disagree
// in general — that is why both exist — but neither operator consults a
// comparator at all: Contains is strings.Contains and Equal is bytes.Equal on
// both sides.
//
// So the paths agree, and this pins that they do. If either predicate ever grows
// a comparator-dependent branch for these operators, the probe and the scan would
// start returning different answers for the same query, and this is what would
// notice.
func TestResidual_OrderedKeyOperatorsTheIndexDeclines(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newResidualFixture(t, g, 400)
			if err := g.DeclareOrderedProperty("size"); err != nil {
				t.Fatalf("DeclareOrderedProperty: %v", err)
			}

			target := encoding.Int64(f.size[7])

			for _, tc := range []struct {
				name    string
				filters []store.PropertyFilter
				pred    func(i int) bool
			}{
				{
					// Equality on the ordered key, applied as a residual.
					name: "equality residual on an ordered key",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
						{Key: "size", Op: store.PropertyOpEqual, Value: target},
					},
					pred: func(i int) bool {
						return f.bucket[i] == "bucket-007" && f.size[i] == f.size[7]
					},
				},
				{
					// Contains on the ordered key. No index can serve it, and the
					// encoded bytes make it a genuine substring test.
					name: "contains residual on an ordered key",
					filters: []store.PropertyFilter{
						{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("bucket-007")},
						{Key: "size", Op: store.PropertyOpContains, Value: target[:4]},
					},
					pred: func(i int) bool {
						return f.bucket[i] == "bucket-007" &&
							strings.Contains(string(encoding.Int64(f.size[i])), string(target[:4]))
					},
				},
				{
					// The same Contains with no other filter, so it is resolved
					// by scanning the key rather than by probing candidates. Same
					// predicate, other code path.
					name: "contains alone, resolved by scan",
					filters: []store.PropertyFilter{
						{Key: "size", Op: store.PropertyOpContains, Value: target[:4]},
					},
					pred: func(i int) bool {
						return strings.Contains(string(encoding.Int64(f.size[i])), string(target[:4]))
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := g.QueryNodeIDs(store.NodeQuery{
						Filters: tc.filters, FilterMode: store.MatchAll,
					})
					if err != nil {
						t.Fatalf("QueryNodeIDs: %v", err)
					}
					assertIDs(t, tc.name, got, f.want(tc.pred))
				})
			}
		})
	}
}

// Edge residual filters.
//
// The edge half of the residual planner existed as compiling, passing, entirely
// unreachable code before these: only the node path had been wired into the two
// stores, and Go does not warn about a method nobody calls. These cover the same
// ground the node tests do, so the edge path is exercised rather than merely
// present.
type edgeResidualFixture struct {
	g     *graphene.Graph
	ids   []store.EdgeID
	kind  []string
	stage []string
}

func newEdgeResidualFixture(t *testing.T, g *graphene.Graph, n int) *edgeResidualFixture {
	t.Helper()
	nodes := make([]*store.Node, n+1)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeTag}}
	}
	nodeIDs, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	edges := make([]*store.Edge, n)
	for i := range edges {
		edges[i] = &store.Edge{Src: nodeIDs[i], Dst: nodeIDs[i+1],
			Labels: []store.EdgeType{store.EdgeTypeContains}}
	}
	ids, err := g.AddEdges(edges)
	if err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	f := &edgeResidualFixture{g: g, ids: ids}
	for i, id := range ids {
		kind := fmt.Sprintf("kind-%06d", i)
		stage := fmt.Sprintf("stage-%d", i%6)
		f.kind = append(f.kind, kind)
		f.stage = append(f.stage, stage)
		if err := g.IndexEdgeProperties(id, map[string][]byte{
			"kind":  []byte(kind),
			"stage": []byte(stage),
		}); err != nil {
			t.Fatalf("IndexEdgeProperties: %v", err)
		}
	}
	return f
}

func (f *edgeResidualFixture) want(pred func(i int) bool) []store.EdgeID {
	var out []store.EdgeID
	for i, id := range f.ids {
		if pred(i) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

func TestResidual_EdgeQueriesAgreeWithExpectedSemantics(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newEdgeResidualFixture(t, g, 500)

			for _, tc := range []struct {
				name    string
				filters []store.PropertyFilter
				pred    func(i int) bool
			}{
				{
					name: "unique equality + contains",
					filters: []store.PropertyFilter{
						{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("kind-000101")},
						{Key: "stage", Op: store.PropertyOpContains, Value: []byte("stage-")},
					},
					pred: func(i int) bool {
						return f.kind[i] == "kind-000101" && strings.Contains(f.stage[i], "stage-")
					},
				},
				{
					name: "unique equality + non-matching residual",
					filters: []store.PropertyFilter{
						{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("kind-000101")},
						{Key: "stage", Op: store.PropertyOpEqual, Value: []byte("stage-9")},
					},
					pred: func(i int) bool { return false },
				},
				{
					name: "two equalities",
					filters: []store.PropertyFilter{
						{Key: "stage", Op: store.PropertyOpEqual, Value: []byte("stage-3")},
						{Key: "kind", Op: store.PropertyOpPrefix, Value: []byte("kind-0000")},
					},
					pred: func(i int) bool {
						return f.stage[i] == "stage-3" && strings.HasPrefix(f.kind[i], "kind-0000")
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := g.QueryEdgeIDs(store.EdgeQuery{
						Filters: tc.filters, FilterMode: store.MatchAll,
					})
					if err != nil {
						t.Fatalf("QueryEdgeIDs: %v", err)
					}
					want := f.want(tc.pred)
					if len(got) != len(want) {
						t.Fatalf("%s: got %d, want %d", tc.name, len(got), len(want))
					}
					for i := range got {
						if got[i] != want[i] {
							t.Fatalf("%s: differ at %d: %d vs %d", tc.name, i, got[i], want[i])
						}
					}
				})
			}
		})
	}
}

func TestPlanner_EdgeQueriesChooseTheExpectedIndex(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			f := newEdgeResidualFixture(t, g, 500)

			plan, err := g.ExplainEdgeQuery(store.EdgeQuery{Filters: []store.PropertyFilter{
				{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("kind-000101")},
				{Key: "stage", Op: store.PropertyOpContains, Value: []byte("stage")},
			}})
			if err != nil {
				t.Fatalf("ExplainEdgeQuery: %v", err)
			}
			if plan.Driver != store.DriverEquality || plan.DriverKey != "kind" {
				t.Errorf("expected the kind equality to drive — plan: %s", plan)
			}
			if plan.Candidates != 1 {
				t.Errorf("candidates = %d, want 1 — plan: %s", plan.Candidates, plan)
			}
			for _, r := range plan.Residuals {
				if r.Key == "kind" {
					t.Errorf("the driving filter was scheduled as a residual — plan: %s", plan)
				}
			}
			if plan.Results != 1 {
				t.Errorf("results = %d, want 1 — plan: %s", plan.Results, plan)
			}

			// An anchored query is bounded by the anchors' degree.
			srcs := []store.NodeID{}
			if e, err := g.GetEdge(f.ids[0]); err == nil {
				srcs = append(srcs, e.Src)
			}
			anchored, err := g.ExplainEdgeQuery(store.EdgeQuery{SrcIDs: srcs})
			if err != nil {
				t.Fatalf("ExplainEdgeQuery: %v", err)
			}
			if anchored.Driver != store.DriverAdjacency {
				t.Errorf("expected adjacency to drive an anchored query — plan: %s", anchored)
			}
		})
	}
}
