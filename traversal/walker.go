package traversal

import (
	"github.com/aoiflux/graphene/store"
)

// walker is the shared adjacency access layer for every traversal in this
// package. It exists to keep allocation off the hot path.
//
// The naive shape — calling g.Neighbours(id, ...) once per visited node — makes
// a k-hop walk allocate proportionally to the number of nodes it visits: each
// call builds a []NeighbourResult and a dedupe map, and on the disk backend a
// *store.Edge and a cloned property blob per incident edge. A walker instead
// reuses one incident-edge buffer for the whole traversal and materialises
// records only when the caller actually keeps them.
//
// Backends implementing store.AdjacencyReader (both bundled ones do) take the
// buffered path. Anything else falls back to EdgesOf, which allocates per call
// but keeps third-party stores working unchanged.
type walker struct {
	g        store.GraphStore
	adj      store.AdjacencyReader // nil when the backend lacks the extension
	edgeBuf  []store.IncidentEdge  // reused across every expansion
	seenNbrs map[store.NodeID]struct{}
}

func newWalker(g store.GraphStore) *walker {
	w := &walker{g: g}
	if adj, ok := g.(store.AdjacencyReader); ok {
		w.adj = adj
	}
	return w
}

// incidentEdges returns the edges incident to id, each paired with the node at
// the far end. The returned slice is owned by the walker and is invalidated by
// the next call — callers must finish with it before expanding another node.
func (w *walker) incidentEdges(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.IncidentEdge, error) {
	if w.adj != nil {
		buf, err := w.adj.IncidentEdges(w.edgeBuf[:0], id, dir, edgeTypes)
		w.edgeBuf = buf // keep the grown capacity for the next expansion
		return buf, err
	}

	edges, err := w.g.EdgesOf(id, dir, edgeTypes)
	if err != nil {
		return nil, err
	}
	buf := w.edgeBuf[:0]
	for _, e := range edges {
		nb := e.Dst
		if e.Src != id {
			nb = e.Src
		}
		buf = append(buf, store.IncidentEdge{Edge: e.ID, Neighbour: nb})
	}
	w.edgeBuf = buf
	return buf, nil
}

// nodeExists reports node liveness without materialising the node when possible.
func (w *walker) nodeExists(id store.NodeID) bool {
	if w.adj != nil {
		return w.adj.NodeExists(id)
	}
	_, err := w.g.GetNode(id)
	return err == nil
}

// beginExpansion resets the per-expansion neighbour set, reusing its storage.
//
// Neighbours deduplicates by neighbour ID within a single call, so two edges
// reaching the same neighbour yield one result. Traversals replicate that here
// rather than allocating a fresh map per node.
func (w *walker) beginExpansion() {
	if w.seenNbrs == nil {
		w.seenNbrs = make(map[store.NodeID]struct{})
		return
	}
	clear(w.seenNbrs)
}

// markNeighbour reports whether id is new for this expansion.
func (w *walker) markNeighbour(id store.NodeID) bool {
	if _, seen := w.seenNbrs[id]; seen {
		return false
	}
	w.seenNbrs[id] = struct{}{}
	return true
}
