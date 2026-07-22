package graphene_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

// Subgraph matching.
//
// The matcher was rewritten to stop materialising records it discards: edge
// checks now walk adjacency IDs, and candidate building tests label postings
// instead of loading each node. Both are pure optimisations, so the property
// that matters is that **results are unchanged** — and the existing coverage
// (one triangle, one no-match, one empty pattern) was far too thin to show that.
//
// In particular nothing exercised multi-label pattern edges, which is where the
// rewrite is most likely to be wrong: the store's edge-type filter is OR over
// the list it is given, while a pattern's labels are AND. The fast path only
// skips materialisation when a single label is required, precisely because the
// store's filter can prove that one on its own and no more.
//
// The oracle is brute force over every mapping, written independently here.

// patternOracle enumerates matches by trying every assignment of scope nodes to
// pattern nodes and checking each edge directly. Exponential, so keep fixtures
// small — but it shares no code with the matcher it checks.
func patternOracle(t *testing.T, g *graphene.Graph, p *traversal.Pattern, scope []store.NodeID) [][]store.NodeID {
	t.Helper()

	nodeOK := func(id store.NodeID, want []store.NodeType) bool {
		if len(want) == 0 {
			return true
		}
		n, err := g.GraphStore.GetNode(id)
		if err != nil {
			return false
		}
		for _, w := range want {
			found := false
			for _, have := range n.Labels {
				if have == w {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}

	edgeOK := func(src, dst store.NodeID, want []store.EdgeType) bool {
		edges, err := g.GraphStore.EdgesOf(src, store.DirectionOutbound, nil)
		if err != nil {
			return false
		}
		for _, e := range edges {
			if e.Dst != dst {
				continue
			}
			all := true
			for _, w := range want {
				found := false
				for _, have := range e.Labels {
					if have == w {
						found = true
						break
					}
				}
				if !found {
					all = false
					break
				}
			}
			if all {
				return true
			}
		}
		return false
	}

	var out [][]store.NodeID
	mapping := make([]store.NodeID, len(p.Nodes))
	used := make(map[store.NodeID]bool)

	var rec func(depth int)
	rec = func(depth int) {
		if depth == len(p.Nodes) {
			for _, pe := range p.Edges {
				if !edgeOK(mapping[pe.SrcPatternID], mapping[pe.DstPatternID], pe.Labels) {
					return
				}
			}
			out = append(out, append([]store.NodeID(nil), mapping...))
			return
		}
		for _, id := range scope {
			if used[id] || !nodeOK(id, p.Nodes[depth].Labels) {
				continue
			}
			mapping[depth] = id
			used[id] = true
			rec(depth + 1)
			used[id] = false
		}
	}
	rec(0)
	return out
}

func normaliseMatches(ms []traversal.SubgraphMatch) [][]store.NodeID {
	out := make([][]store.NodeID, 0, len(ms))
	for _, m := range ms {
		out = append(out, append([]store.NodeID(nil), m.Mapping...))
	}
	sortMappings(out)
	return out
}

func sortMappings(ms [][]store.NodeID) {
	sort.Slice(ms, func(i, j int) bool {
		a, b := ms[i], ms[j]
		for k := range a {
			if a[k] != b[k] {
				return a[k] < b[k]
			}
		}
		return false
	})
}

func patternBackends(t *testing.T) map[string]*graphene.Graph {
	t.Helper()
	disk, err := graphene.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	return map[string]*graphene.Graph{
		"memory": graphene.NewInMemory(),
		"disk":   disk,
	}
}

// TestPatternMatchesOracle drives randomly generated graphs through the matcher
// and the brute-force oracle and compares the full match sets.
func TestPatternMatchesOracle(t *testing.T) {
	patterns := []struct {
		name string
		p    *traversal.Pattern
	}{
		{"single edge", &traversal.Pattern{
			Nodes: []traversal.PatternNode{
				{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			},
			Edges: []traversal.PatternEdge{
				{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeContains}},
			},
		}},
		{"multi-label edge (AND)", &traversal.Pattern{
			Nodes: []traversal.PatternNode{
				{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			},
			Edges: []traversal.PatternEdge{
				{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{
					store.EdgeTypeContains, store.EdgeTypeSimilarTo}},
			},
		}},
		{"unlabelled edge", &traversal.Pattern{
			Nodes: []traversal.PatternNode{
				{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{ID: 1, Labels: []store.NodeType{store.NodeTypeCase}},
			},
			Edges: []traversal.PatternEdge{
				{SrcPatternID: 0, DstPatternID: 1},
			},
		}},
		{"unlabelled pattern node", &traversal.Pattern{
			Nodes: []traversal.PatternNode{
				{ID: 0},
				{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			},
			Edges: []traversal.PatternEdge{
				{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeContains}},
			},
		}},
		{"triangle", &traversal.Pattern{
			Nodes: []traversal.PatternNode{
				{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				{ID: 2, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			},
			Edges: []traversal.PatternEdge{
				{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeContains}},
				{SrcPatternID: 1, DstPatternID: 2, Labels: []store.EdgeType{store.EdgeTypeContains}},
				{SrcPatternID: 2, DstPatternID: 0, Labels: []store.EdgeType{store.EdgeTypeContains}},
			},
		}},
	}

	for name, g := range patternBackends(t) {
		scope := buildPatternFixture(t, g, 12, 20260722)
		for _, tc := range patterns {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				got, err := g.FindPatterns(tc.p, scope, 0)
				if err != nil {
					t.Fatalf("FindPatterns: %v", err)
				}
				gotN := normaliseMatches(got)

				want := patternOracle(t, g, tc.p, scope)
				sortMappings(want)

				if !reflect.DeepEqual(gotN, want) {
					t.Errorf("match sets differ\n got %d: %v\nwant %d: %v",
						len(gotN), gotN, len(want), want)
				}
			})
		}
	}
}

// buildPatternFixture creates a small graph with mixed node labels and edges
// carrying one or two labels, so multi-label AND semantics are exercised.
func buildPatternFixture(t *testing.T, g *graphene.Graph, n int, seed int64) []store.NodeID {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))

	ids := make([]store.NodeID, n)
	for i := range ids {
		label := store.NodeTypeMicroArtefact
		if i%4 == 0 {
			label = store.NodeTypeCase
		}
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{label}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		ids[i] = id
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j || rng.Intn(100) >= 25 {
				continue
			}
			labels := []store.EdgeType{store.EdgeTypeContains}
			switch rng.Intn(3) {
			case 1:
				labels = []store.EdgeType{store.EdgeTypeContains, store.EdgeTypeSimilarTo}
			case 2:
				labels = []store.EdgeType{store.EdgeTypeSimilarTo}
			}
			if _, err := g.AddEdge(&store.Edge{Src: ids[i], Dst: ids[j], Labels: labels}); err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
		}
	}
	return ids
}

// TestPatternMaxMatchesIsDeterministic pins that capping results is stable and
// respects scope order — candidate order decides *which* matches a capped search
// returns, so a reordering inside buildCandidates would silently change output.
func TestPatternMaxMatchesIsDeterministic(t *testing.T) {
	for name, g := range patternBackends(t) {
		t.Run(name, func(t *testing.T) {
			scope := buildPatternFixture(t, g, 10, 99)
			p := &traversal.Pattern{
				Nodes: []traversal.PatternNode{
					{ID: 0, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
					{ID: 1, Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
				},
				Edges: []traversal.PatternEdge{
					{SrcPatternID: 0, DstPatternID: 1, Labels: []store.EdgeType{store.EdgeTypeContains}},
				},
			}

			first, err := g.FindPatterns(p, scope, 3)
			if err != nil {
				t.Fatalf("FindPatterns: %v", err)
			}
			for i := 0; i < 5; i++ {
				again, err := g.FindPatterns(p, scope, 3)
				if err != nil {
					t.Fatalf("FindPatterns: %v", err)
				}
				if !reflect.DeepEqual(first, again) {
					t.Fatalf("capped results not deterministic:\n%v\n%v", first, again)
				}
			}

			// Every capped result must also be a real match.
			all := patternOracle(t, g, p, scope)
			allSet := map[string]bool{}
			for _, m := range all {
				allSet[fmt.Sprint(m)] = true
			}
			for _, m := range first {
				if !allSet[fmt.Sprint(m.Mapping)] {
					t.Errorf("capped search returned %v, which is not a valid match", m.Mapping)
				}
			}
		})
	}
}
