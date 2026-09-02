package traversal

// Counting neighbours across many anchors at once.
//
// # The query this answers
//
// "Which of these things do the most of those point at." Rank trackers by how
// many app versions embed them; find the permissions that trend together; count
// which libraries are shared across a corpus. Every one of those is the same
// shape: take a set of anchor nodes, follow one kind of edge, and tally what is
// on the other end.
//
// Done in the caller it is a loop over Neighbours, which on a corpus of any size
// means materialising every neighbouring record — labels, property blobs and all
// — in order to increment a counter and throw the record away. That is fine at a
// thousand anchors and stops being fine well before a hundred thousand.
//
// # Why it is in traversal
//
// Because it needs the guard. An aggregation over a large graph is exactly the
// call that must be cancellable and bounded: the anchor set is often the result
// of an earlier query, so its size is data rather than something the caller
// chose, and one hub among the anchors can make the walk arbitrarily large. The
// guard is package-private, and a second copy of budget accounting is a second
// thing that can disagree with the first.
//
// It is not a walk: nothing is followed past one hop, nothing is visited twice,
// and there is no frontier. What it shares with the walks is what it costs.

import (
	"context"

	"github.com/aoiflux/graphene/store"
)

// NeighbourFrequency counts, for each node one hop from any anchor, how many
// distinct anchors reach it.
//
// See NeighbourFrequencyCtx, which this calls with a background context and no
// budget.
func NeighbourFrequency(g store.GraphReader, anchors []store.NodeID, dir store.Direction, edgeTypes []store.EdgeType, nodeTypes []store.NodeType) (map[store.NodeID]uint64, error) {
	return NeighbourFrequencyCtx(context.Background(), g, anchors, dir, edgeTypes, nodeTypes, store.Budget{})
}

// NeighbourFrequencyCtx is NeighbourFrequency with a cancellable context and a
// budget.
//
// # What the count means
//
// Anchors, not edges. A neighbour reached twice from one anchor — two edges of
// different types between the same pair, or one edge seen from both directions —
// counts once for that anchor. Counting edges instead would make the answer
// depend on how the graph happens to be modelled rather than on what it says,
// and "how many app versions use this tracker" is a question about versions.
//
// A repeated anchor in the input is one anchor, for the same reason.
//
// # Filters
//
// nil edgeTypes follows every edge; nil nodeTypes counts every neighbour.
// nodeTypes is OR over labels, as everywhere else, and is the filter worth
// setting: it is applied before the neighbour is counted, so an anchor
// surrounded by nodes of the wrong kind costs a label test rather than a map
// entry.
//
// An anchor that is not live is skipped rather than reported. The anchor set
// usually comes from an earlier query, and a node deleted between the two is a
// race the caller cannot prevent and does not need to hear about — the answer is
// still exactly the frequencies among the anchors that existed.
func NeighbourFrequencyCtx(ctx context.Context, g store.GraphReader, anchors []store.NodeID, dir store.Direction, edgeTypes []store.EdgeType, nodeTypes []store.NodeType, budget store.Budget) (map[store.NodeID]uint64, error) {
	guard := newGuard(ctx, budget)
	if err := guard.enter(); err != nil {
		return nil, err
	}

	w := newWalker(g)
	counts := make(map[store.NodeID]uint64)

	// Anchors are deduplicated as they are consumed rather than up front: the
	// set is usually already distinct, and a map built to discover that costs
	// one allocation the size of the input for nothing.
	var seenAnchors map[store.NodeID]struct{}
	if len(anchors) > 1 {
		seenAnchors = make(map[store.NodeID]struct{}, len(anchors))
	}

	for _, anchor := range anchors {
		if seenAnchors != nil {
			if _, dup := seenAnchors[anchor]; dup {
				continue
			}
			seenAnchors[anchor] = struct{}{}
		}
		if err := guard.visitNode(); err != nil {
			return nil, err
		}
		if !w.nodeExists(anchor) {
			continue
		}

		incident, err := w.incidentEdges(anchor, dir, edgeTypes)
		if err != nil {
			return nil, err
		}
		// Per-anchor dedupe, so two edges to one neighbour are one vote. The
		// walker owns this set and clears it rather than allocating per anchor,
		// which is the difference between one map and one per anchor.
		w.beginExpansion()
		for _, step := range incident {
			if err := guard.crossEdge(); err != nil {
				return nil, err
			}
			if !w.markNeighbour(step.Neighbour) {
				continue
			}
			if !matchesNodeTypes(w, step.Neighbour, nodeTypes) {
				continue
			}
			counts[step.Neighbour]++
		}
	}
	return counts, nil
}

// matchesNodeTypes reports whether a neighbour should be counted.
//
// With no filter this is a liveness test, which the walker answers without
// building a record. With one there is no way around loading the node: labels
// live on the record, and the label postings answer the opposite question — give
// me every node of this type — which over a whole corpus is far more work than
// reading the handful of neighbours an anchor actually has.
func matchesNodeTypes(w *walker, id store.NodeID, nodeTypes []store.NodeType) bool {
	if len(nodeTypes) == 0 {
		return w.nodeExists(id)
	}
	node, err := w.g.GetNode(id)
	if err != nil || node == nil {
		return false
	}
	for _, t := range nodeTypes {
		if node.HasLabel(t) {
			return true
		}
	}
	return false
}
