package graphene_test

// Options.Adjacency end to end: what a store actually holds, and when.
//
// The unit tests in disk/ prove that deferring the build changes no answer. What
// they cannot show is the thing that decides whether the option is worth having:
// which real operations reach the adjacency arrays. Two findings live here.
//
// The first is that a delete reaches them. DeleteNode cascades to incident edges
// and finds them through exactly these arrays, so a writer that deletes anything
// builds adjacency on its first delete however it was opened. That is why the
// mode is not a default and why its documentation says so rather than selling it.
//
// The second is that a read-only property pass does not reach them at all, which
// is the shape the option exists for: open, query, project, close.

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// adjacencyFixture writes a chain of n nodes joined by n-1 edges, indexes one
// property per node, compacts, and closes. What comes back is a directory
// holding a compacted image, which is the only shape adjacency is a question
// about — a store with no image has no arrays to defer.
func adjacencyFixture(t *testing.T, n int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")

	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperty(id, "seq", []byte(fmt.Sprintf("%06d", i))); err != nil {
			t.Fatalf("IndexNodeProperty: %v", err)
		}
	}
	edges := make([]*store.Edge, 0, n-1)
	for i := 1; i < len(ids); i++ {
		edges = append(edges, &store.Edge{
			Src: ids[i-1], Dst: ids[i], Labels: []store.EdgeType{store.EdgeTypeContains},
		})
	}
	if _, err := g.AddEdges(edges); err != nil {
		t.Fatalf("AddEdges: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir
}

// openAdjacency reopens the fixture under one mode, for reading. Reopened rather
// than reused, because a store acquires its image at open: the handle that
// compacted goes on holding what it held.
//
// Read-only so that both modes can be open over one directory at once, which is
// what lets a test compare them against the same bytes. A writer takes the
// directory exclusively; two readers share it.
func openAdjacency(t *testing.T, dir string, mode disk.AdjacencyMode) *graphene.Graph {
	t.Helper()
	g, err := graphene.OpenWithOptions(dir, disk.Options{Adjacency: mode, ReadOnly: true})
	if err != nil {
		t.Fatalf("open %s: %v", mode, err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// openAdjacencyWriter is openAdjacency for the two tests that mutate.
func openAdjacencyWriter(t *testing.T, dir string, mode disk.AdjacencyMode) *graphene.Graph {
	t.Helper()
	g, err := graphene.OpenWithOptions(dir, disk.Options{Adjacency: mode})
	if err != nil {
		t.Fatalf("open %s: %v", mode, err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// adjacencyHolding is what the handle is holding right now, which is not the
// same as what it was asked for.
func adjacencyHolding(t *testing.T, g *graphene.Graph) string {
	t.Helper()
	st, ok := g.StorageStats()
	if !ok {
		t.Fatal("StorageStats is unavailable on a disk-backed store")
	}
	if st.Adjacency == "" {
		t.Fatal("StorageStats.Adjacency is empty on a store with an image")
	}
	return st.Adjacency
}

// firstNodeID is the node in the middle of the chain's head — sequence number 1,
// not 0 — so it has an edge in each direction and deleting it has two effects to
// check rather than one.
func firstNodeID(t *testing.T, g *graphene.Graph) store.NodeID {
	t.Helper()
	return nodeBySeq(t, g, 1)
}

// nodeBySeq is the node the fixture gave the given sequence number.
func nodeBySeq(t *testing.T, g *graphene.Graph, i int) store.NodeID {
	t.Helper()
	key := []byte(fmt.Sprintf("%06d", i))
	ids, err := g.NodesByProperty("seq", key)
	if err != nil || len(ids) != 1 {
		t.Fatalf("NodesByProperty seq=%s = (%v, %v), want one id", key, ids, err)
	}
	return ids[0]
}

// The option's claim, and the two modes asserted to be in different states while
// the claim is checked. Without the second half a change that stopped honouring
// the option leaves this passing.
func TestAdjacency_LazyDefersTheBuildUntilSomethingTraverses(t *testing.T) {
	dir := adjacencyFixture(t, 400)

	eager := openAdjacency(t, dir, disk.AdjacencyEager)
	if got := adjacencyHolding(t, eager); got != "built" {
		t.Fatalf("an eagerly opened store reports Adjacency %q, want \"built\"", got)
	}

	lazy := openAdjacency(t, dir, disk.AdjacencyLazy)
	if got := adjacencyHolding(t, lazy); got != "deferred" {
		t.Fatalf("a lazily opened store reports Adjacency %q at open, want \"deferred\"", got)
	}

	id := firstNodeID(t, lazy)
	want, err := eager.Degree(id, nil)
	if err != nil {
		t.Fatalf("eager Degree: %v", err)
	}
	if want == 0 {
		t.Fatal("the fixture gives the first node no edges, so this asserts nothing")
	}

	got, err := lazy.Degree(id, nil)
	if err != nil {
		t.Fatalf("lazy Degree: %v", err)
	}
	if got != want {
		t.Fatalf("Degree over a deferred image = %d, want %d", got, want)
	}
	if holding := adjacencyHolding(t, lazy); holding != "built" {
		t.Fatalf("after a degree query the store reports Adjacency %q, want \"built\"", holding)
	}
}

// The finding that decides the framing: a writer that deletes cannot skip this.
func TestAdjacency_ADeleteCascadeBuildsIt(t *testing.T) {
	dir := adjacencyFixture(t, 400)
	g := openAdjacencyWriter(t, dir, disk.AdjacencyLazy)

	if got := adjacencyHolding(t, g); got != "deferred" {
		t.Fatalf("Adjacency %q at open, want \"deferred\"", got)
	}

	id := firstNodeID(t, g)

	// The chain's second node, which the deleted one points at: it starts with
	// two incident edges and must end with one.
	next := nodeBySeq(t, g, 2)
	if deg, err := g.Degree(next, nil); err != nil || deg != 2 {
		t.Fatalf("Degree of the second node before the delete = (%d, %v), want (2, nil)", deg, err)
	}

	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	if got := adjacencyHolding(t, g); got != "built" {
		t.Fatalf("after a delete the store reports Adjacency %q, want \"built\" — "+
			"the cascade finds incident edges through the adjacency arrays, so a "+
			"delete cannot have skipped them", got)
	}

	// And the cascade did its job, which is what makes the build necessary
	// rather than incidental: the node is gone and the edge that named it with
	// it. A degree query on an absent node is not an error — it is zero, which
	// is the honest count of edges incident on nothing.
	if _, err := g.GetNode(id); err == nil {
		t.Fatal("the deleted node is still readable")
	}
	if deg, err := g.Degree(id, nil); err != nil || deg != 0 {
		t.Fatalf("Degree of the deleted node = (%d, %v), want (0, nil)", deg, err)
	}
	if deg, err := g.Degree(next, nil); err != nil || deg != 1 {
		t.Fatalf("Degree of the node the deleted one pointed at = (%d, %v), want (1, nil) — "+
			"it should have lost the incident edge and kept the other", deg, err)
	}
}

// The shape the option is for: open, read properties, close, having never built
// the arrays.
func TestAdjacency_APropertyOnlyPassNeverBuildsIt(t *testing.T) {
	dir := adjacencyFixture(t, 400)
	g := openAdjacency(t, dir, disk.AdjacencyLazy)

	seen := 0
	for i := 0; i < 400; i++ {
		ids, err := g.NodesByProperty("seq", []byte(fmt.Sprintf("%06d", i)))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		for _, id := range ids {
			if _, err := g.GetNode(id); err != nil {
				t.Fatalf("GetNode(%d): %v", id, err)
			}
			seen++
		}
	}
	if seen != 400 {
		t.Fatalf("the pass read %d nodes, want 400", seen)
	}
	if got := adjacencyHolding(t, g); got != "deferred" {
		t.Fatalf("after reading every node by property the store reports Adjacency %q, "+
			"want \"deferred\" — a property pass that reaches the adjacency arrays is "+
			"the case this option was measured against", got)
	}
}

// A compaction produces a new image, and it has to be held on the terms the
// handle was opened with. Re-eagering here would undo the setting at the one
// moment a caller is least likely to look.
func TestAdjacency_ACompactionKeepsTheMode(t *testing.T) {
	dir := adjacencyFixture(t, 200)
	g := openAdjacencyWriter(t, dir, disk.AdjacencyLazy)

	// A write, so the compaction has something to fold in and is not a no-op.
	if _, err := g.AddNodes([]*store.Node{
		{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if got := adjacencyHolding(t, g); got != "deferred" {
		t.Fatalf("after a compaction under AdjacencyLazy the store reports %q, "+
			"want \"deferred\"", got)
	}

	// And the new image still answers, which is the half that matters.
	id := firstNodeID(t, g)
	if _, err := g.Degree(id, nil); err != nil {
		t.Fatalf("Degree after compaction: %v", err)
	}
	if got := adjacencyHolding(t, g); got != "built" {
		t.Fatalf("the compacted image reports %q after a degree query, want \"built\"", got)
	}
}

// Whole-graph agreement. Every node's degree, under both modes, over an image
// that one of them has not built.
func TestAdjacency_BothModesAgreeOnEveryDegree(t *testing.T) {
	dir := adjacencyFixture(t, 300)
	eager := openAdjacency(t, dir, disk.AdjacencyEager)
	lazy := openAdjacency(t, dir, disk.AdjacencyLazy)

	if adjacencyHolding(t, eager) == adjacencyHolding(t, lazy) {
		t.Fatal("both handles are in the same state, so the comparison below " +
			"compares one mode against itself")
	}

	total := 0
	for i := 0; i < 300; i++ {
		ids, err := eager.NodesByProperty("seq", []byte(fmt.Sprintf("%06d", i)))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		for _, id := range ids {
			want, err := eager.Degree(id, nil)
			if err != nil {
				t.Fatalf("eager Degree(%d): %v", id, err)
			}
			got, err := lazy.Degree(id, nil)
			if err != nil {
				t.Fatalf("lazy Degree(%d): %v", id, err)
			}
			if got != want {
				t.Fatalf("Degree(%d) = %d lazily and %d eagerly", id, got, want)
			}
			total += want
		}
	}
	if total != 2*(300-1) {
		t.Fatalf("the degrees sum to %d over a %d-edge chain counted twice, want %d",
			total, 300-1, 2*(300-1))
	}
}
