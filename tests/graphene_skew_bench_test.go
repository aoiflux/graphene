// Skewed-degree fixture and benchmarks.
//
// Two open questions need this and neither can be answered without it:
//
//   - §3.3 H1 asks for a power-law fixture before any supernode work is built,
//     because CONTRIBUTING.md requires the prize to be measured first and no
//     fixture populated that case.
//   - §14.5 Q11 asks whether an edge's hash should bind its endpoints' version
//     hashes (strong, but a node update then costs O(degree) rehashing) or their
//     IDs (O(1), weaker). The plan calls it the sharpest cost/assurance trade in
//     the design and says to decide it before building any hashing.
//
// Q11's cost is modelled rather than implemented: nothing hashes entities yet,
// so these measure the work version-binding *would* require — walking a node's
// incident edges and hashing each one — against the single hash ID-binding
// needs. That is the whole difference between the two designs.
//
//	go test . -tags=stress -bench=Skew -benchmem -run='^$'

//go:build stress

package graphene_test

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// skewFixture builds a graph by preferential attachment, which produces the
// heavy-tailed degree distribution real graphs have and uniform fixtures do not.
//
// Each new node attaches m edges to existing nodes chosen with probability
// proportional to their current degree, so a few nodes accumulate very large
// neighbourhoods while most stay near m. Seeded, so the shape is identical on
// every run and two benchmark arms see the same graph.
func skewFixture(tb testing.TB, nodes, m int) (*graphene.Graph, string, []store.NodeID) {
	tb.Helper()

	dir, err := os.MkdirTemp("", "graphene-skew-*")
	if err != nil {
		tb.Fatal(err)
	}
	g, err := graphene.Open(dir)
	if err != nil {
		tb.Fatal(err)
	}

	ids := make([]store.NodeID, 0, nodes)
	for i := 0; i < nodes; i++ {
		id, err := g.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: []byte(fmt.Sprintf("node-%d-payload", i)),
		})
		if err != nil {
			tb.Fatal(err)
		}
		ids = append(ids, id)
	}

	// targets holds each node once per unit of degree, so drawing from it
	// uniformly is drawing proportional to degree.
	rng := rand.New(rand.NewSource(20260802))
	targets := make([]store.NodeID, 0, nodes*m*2)
	targets = append(targets, ids[0])

	for i := 1; i < nodes; i++ {
		for j := 0; j < m; j++ {
			dst := targets[rng.Intn(len(targets))]
			if dst == ids[i] {
				continue
			}
			if _, err := g.AddEdge(&store.Edge{
				Src:    ids[i],
				Dst:    dst,
				Labels: []store.EdgeType{store.EdgeTypeContains},
			}); err != nil {
				tb.Fatal(err)
			}
			targets = append(targets, ids[i], dst)
		}
	}

	if err := g.Compact(); err != nil {
		tb.Fatal(err)
	}
	return g, dir, ids
}

// degreeProfile returns the ids sorted by total degree, descending.
func degreeProfile(tb testing.TB, g *graphene.Graph, ids []store.NodeID) []store.NodeID {
	tb.Helper()
	out := append([]store.NodeID(nil), ids...)
	deg := make(map[store.NodeID]int, len(ids))
	for _, id := range ids {
		d, err := g.Degree(id, nil)
		if err != nil {
			tb.Fatal(err)
		}
		deg[id] = d
	}
	sort.Slice(out, func(i, j int) bool { return deg[out[i]] > deg[out[j]] })
	return out
}

// TestSkewFixture_IsActuallySkewed guards the fixture itself. A benchmark over a
// distribution that turned out uniform would answer a different question than the
// one it claims to, and quietly.
func TestSkewFixture_IsActuallySkewed(t *testing.T) {
	g, dir, ids := skewFixture(t, 5_000, 2)
	defer os.RemoveAll(dir)
	defer g.Close()

	ranked := degreeProfile(t, g, ids)
	hub, _ := g.Degree(ranked[0], nil)
	median, _ := g.Degree(ranked[len(ranked)/2], nil)

	t.Logf("degree: hub=%d median=%d ratio=%.0fx", hub, median, float64(hub)/float64(max(median, 1)))
	if hub < median*20 {
		t.Fatalf("fixture is not meaningfully skewed: hub=%d median=%d", hub, median)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// --- Q11: what version-binding would cost per node update ---

// hashNodeRecord models the single hash ID-binding needs when a node changes.
func hashNodeRecord(n *store.Node) [32]byte {
	h := sha256.New()
	var id [8]byte
	binary.LittleEndian.PutUint64(id[:], uint64(n.ID))
	h.Write(id[:])
	for _, l := range n.Labels {
		var lb [2]byte
		binary.LittleEndian.PutUint16(lb[:], uint16(l))
		h.Write(lb[:])
	}
	h.Write(n.Properties)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// hashEdgeRecord models rehashing one incident edge, which version-binding
// requires for every edge touching a changed node — an edge's hash includes its
// endpoints' version hashes, so changing an endpoint invalidates it.
func hashEdgeRecord(e *store.Edge, srcHash, dstHash [32]byte) [32]byte {
	h := sha256.New()
	var id [8]byte
	binary.LittleEndian.PutUint64(id[:], uint64(e.ID))
	h.Write(id[:])
	h.Write(srcHash[:])
	h.Write(dstHash[:])
	h.Write(e.Properties)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// BenchmarkSkew_Q11 compares the two bindings at the hub and at the median, which
// is where the whole trade lives: they are indistinguishable on an ordinary node
// and diverge by the hub's degree on a supernode.
//
// Both arms run over one fixture in one process, per CONTRIBUTING.md — comparing
// two designs across two binaries makes the answer hostage to machine drift.
func BenchmarkSkew_Q11(b *testing.B) {
	g, dir, ids := skewFixture(b, 20_000, 2)
	defer os.RemoveAll(dir)
	defer g.Close()

	ranked := degreeProfile(b, g, ids)
	hub := ranked[0]
	median := ranked[len(ranked)/2]

	hubDeg, _ := g.Degree(hub, nil)
	medDeg, _ := g.Degree(median, nil)
	b.Logf("hub degree=%d  median degree=%d", hubDeg, medDeg)

	for _, tc := range []struct {
		name string
		id   store.NodeID
	}{{"hub", hub}, {"median", median}} {
		node, err := g.GetNode(tc.id)
		if err != nil {
			b.Fatal(err)
		}

		// ID-binding: the changed node's own hash, and nothing else.
		b.Run(tc.name+"/bind-ids", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = hashNodeRecord(node)
			}
		})

		// Version-binding: the node's hash, then every incident edge's.
		b.Run(tc.name+"/bind-version-hashes", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				nh := hashNodeRecord(node)
				edges, err := g.EdgesOf(tc.id, store.DirectionBoth, nil)
				if err != nil {
					b.Fatal(err)
				}
				for _, e := range edges {
					_ = hashEdgeRecord(e, nh, nh)
				}
			}
		})
	}
}

// --- H1: the supernode pathology as it stands today ---

// BenchmarkSkew_H1 measures what a supernode already costs, with no hashing
// involved. §3.3 records that filtered degree walks every incident edge and that
// delta adjacency removal is linear; this is the fixture that shows by how much.
func BenchmarkSkew_H1(b *testing.B) {
	g, dir, ids := skewFixture(b, 20_000, 2)
	defer os.RemoveAll(dir)
	defer g.Close()

	ranked := degreeProfile(b, g, ids)
	hub, median := ranked[0], ranked[len(ranked)/2]

	for _, tc := range []struct {
		name string
		id   store.NodeID
	}{{"hub", hub}, {"median", median}} {
		// Unfiltered degree is O(1) on the CSR: a prefix-sum subtraction.
		b.Run(tc.name+"/degree-unfiltered", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := g.Degree(tc.id, nil); err != nil {
					b.Fatal(err)
				}
			}
		})

		// Filtered degree is not: it walks every incident edge to test its label.
		b.Run(tc.name+"/degree-filtered", func(b *testing.B) {
			b.ReportAllocs()
			types := []store.EdgeType{store.EdgeTypeContains}
			for i := 0; i < b.N; i++ {
				if _, err := g.Degree(tc.id, types); err != nil {
					b.Fatal(err)
				}
			}
		})

		// Neighbour expansion, which every traversal touching the node pays.
		b.Run(tc.name+"/neighbours", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := g.Neighbours(tc.id, store.DirectionBoth, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
