package traversal

// Weighted shortest paths: Dijkstra, and A* over the same core.
//
// # Why this is not the bidirectional search next door
//
// ShortestPathCtx meets in the middle and stops at the first node both
// frontiers have reached. That is correct for an unweighted walk, where the
// first meeting is on a shortest path by construction, and it is *wrong* the
// moment edges have costs: the first meeting is merely the first, and a cheaper
// path can still be one long hop away on either side. Making bidirectional
// Dijkstra correct means continuing past the meeting until the two settled
// frontiers' costs provably cannot improve on the best joint path found so far
// — a different algorithm with a different termination proof, not a weight
// added to this one. So this search is unidirectional, and what carries over
// from path.go is the provenance map and the record-materialising tail, not the
// two-frontier machinery.
//
// # Why the neighbour dedupe is gone
//
// Every other walk here calls walker.beginExpansion and drops the second edge
// reaching a neighbour it has already seen this expansion, matching what
// Neighbours reports. A weighted walk must not: two parallel edges between the
// same pair are two different costs, and taking whichever came first out of
// adjacency order would return a path that is not the cheapest while reporting
// that it is. Every incident edge is relaxed, and the relaxation itself is what
// discards the dearer one.

import (
	"context"
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// ErrNilCost is returned when a weighted walk is given no cost function.
//
// It is refused rather than defaulted to a unit cost, because a unit cost is
// exactly ShortestPath — a nil selector quietly meaning "hop count" would make
// the weighted call return the unweighted answer, which is the kind of silent
// agreement that hides a caller's mistake for as long as the weights happen not
// to matter.
var ErrNilCost = errors.New("weighted traversal requires an edge cost function")

// ErrNegativeCost is returned when an EdgeCost yields a negative or NaN cost.
//
// Dijkstra settles a node the first time it is popped and never reconsiders it,
// which a negative edge invalidates: a longer prefix can become cheaper after
// the fact, so a settled node's recorded distance is no longer final. The
// result is not "slightly off", it is a wrong path reported as the cheapest one,
// and detecting it costs one comparison per relaxation.
//
// NaN travels with it. NaN fails every comparison, so a NaN cost does not make
// the search wrong so much as arbitrary — nodes stop being ordered at all — and
// one test catches both.
var ErrNegativeCost = errors.New("edge cost must be non-negative and not NaN")

// weightedEntry is what the search knows about one node: how it was reached,
// how much that cost, and whether that answer is final.
//
// One map rather than the two the textbook shape uses (a distance table and a
// predecessor table): every write touches both and every read that matters
// reads both, so splitting them doubles the hashing for nothing.
type weightedEntry struct {
	parent  store.NodeID
	edge    store.EdgeID
	dist    float64
	settled bool
}

// ShortestWeightedPath finds the cheapest path between src and dst, where the
// cost of a step is whatever cost reports for it.
//
// It is ShortestPath with distances: the same undirected reading of the graph
// (DirectionBoth), the same *PathResult, the same ErrNoPath when the two nodes
// are not connected — so a caller can swap one for the other and compare the
// answers. What differs is which path comes back: ShortestPath returns one with
// the fewest edges, this one returns the cheapest, and on any graph where those
// are not the same sequence the two disagree by design.
//
// cost must not be nil and must obey store.EdgeCost's contract; a negative or
// NaN cost is refused with ErrNegativeCost rather than mishandled. Pass nil
// edgeTypes to traverse all edge types.
//
// Where several paths tie for cheapest, which one is returned is unspecified —
// it follows the order the backend reports incident edges in, which is not a
// promise the store makes across layer states. The cost of the returned path is
// determined by the graph; its shape, among equals, is not.
func ShortestWeightedPath(g store.GraphReader, src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost) (*PathResult, error) {
	return ShortestWeightedPathCtx(context.Background(), g, src, dst, edgeTypes, cost, store.Budget{})
}

// ShortestWeightedPathCtx is ShortestWeightedPath with a cancellable context
// and a budget. See BFSCtx.
//
// MaxNodes is charged when a node is *settled* — popped with its final cost —
// not when it is first discovered. That keeps it meaning what it means for
// every other walk here: nodes visited. A weighted search discovers a node
// several times over, once per improvement found, and charging those would make
// the same limit stop this walk far earlier than a BFS across the same graph.
// MaxEdges is charged per edge examined, as elsewhere.
func ShortestWeightedPathCtx(ctx context.Context, g store.GraphReader, src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost, budget store.Budget) (*PathResult, error) {
	return weightedPath(ctx, g, src, dst, edgeTypes, cost, nil, budget)
}

// AStarPath finds the cheapest path between src and dst, guided by an estimate
// of how far each node still is from dst.
//
// It is ShortestWeightedPath with a hint. Dijkstra expands outward in every
// direction because it has no reason to prefer one; A* orders the queue by
// cost-so-far plus estimated-cost-remaining instead, so a good estimate walks
// more or less straight at the destination and touches a fraction of the nodes.
// A bad estimate costs time; an *overestimating* one costs correctness, silently
// returning a path that is not the cheapest. store.NodeHeuristic states the
// obligation and it is the caller's to keep: it cannot be checked here without
// computing the answer the heuristic exists to avoid computing.
//
// heuristic may be nil, which is exactly Dijkstra.
func AStarPath(g store.GraphReader, src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost, heuristic store.NodeHeuristic) (*PathResult, error) {
	return AStarPathCtx(context.Background(), g, src, dst, edgeTypes, cost, heuristic, store.Budget{})
}

// AStarPathCtx is AStarPath with a cancellable context and a budget.
func AStarPathCtx(ctx context.Context, g store.GraphReader, src, dst store.NodeID, edgeTypes []store.EdgeType, cost store.EdgeCost, heuristic store.NodeHeuristic, budget store.Budget) (*PathResult, error) {
	return weightedPath(ctx, g, src, dst, edgeTypes, cost, heuristic, budget)
}

// weightedPath is the shared core. A nil heuristic is plain Dijkstra: the
// branch is one nil test per discovery, against an indirect call per discovery
// for a heuristic that always returns zero.
func weightedPath(
	ctx context.Context,
	g store.GraphReader,
	src, dst store.NodeID,
	edgeTypes []store.EdgeType,
	cost store.EdgeCost,
	heuristic store.NodeHeuristic,
	budget store.Budget,
) (*PathResult, error) {
	if cost == nil {
		return nil, ErrNilCost
	}

	guard := newGuard(ctx, budget)
	if err := guard.enter(); err != nil {
		return nil, err
	}

	if src == dst {
		node, err := g.GetNode(src)
		if err != nil {
			return nil, err
		}
		return &PathResult{Nodes: []*store.Node{node}}, nil
	}

	w := newWalker(g)

	best := map[store.NodeID]weightedEntry{
		src: {parent: store.InvalidNodeID, edge: store.InvalidEdgeID},
	}
	var queue minHeap[store.NodeID]
	queue.push(estimate(heuristic, src), src)

	found := false
	for {
		_, cur, ok := queue.pop()
		if !ok {
			break
		}
		entry := best[cur]
		// A node is queued once per improvement found, so most pops after the
		// first for a given node are stale. Discarding them here is what buys
		// the search a heap with no decrease-key and no position index — the
		// per-node allocation that shape would need is the whole saving.
		if entry.settled {
			continue
		}
		entry.settled = true
		best[cur] = entry

		if err := guard.visitNode(); err != nil {
			return nil, err
		}

		// Settling dst means its recorded cost is final: everything still
		// queued is at least as expensive, so nothing left can improve on it.
		if cur == dst {
			found = true
			break
		}

		incident, err := w.incidentEdges(cur, store.DirectionBoth, edgeTypes)
		if err != nil {
			continue
		}
		for _, step := range incident {
			if err := guard.crossEdge(); err != nil {
				return nil, err
			}
			if !w.nodeExists(step.Neighbour) {
				continue
			}

			c := cost(step)
			// Written as a failed non-negative test rather than c < 0: NaN
			// fails every comparison, so c < 0 would wave it through into a
			// queue whose ordering it then makes arbitrary. One test rejects
			// both.
			if !(c >= 0) {
				return nil, fmt.Errorf("%w: edge %d cost %v", ErrNegativeCost, step.Edge, c)
			}

			nd := entry.dist + c
			prev, seen := best[step.Neighbour]
			// For a plain Dijkstra the settled test is redundant and kept for
			// what it says: nodes settle in non-decreasing order of distance,
			// and a non-negative cost cannot round a sum below its own left
			// operand, so a settled neighbour already fails prev.dist <= nd.
			//
			// A* is where it decides something. That queue is ordered by
			// distance plus estimate, so an *inconsistent* heuristic can reach a
			// settled node again later at a genuinely smaller distance. This
			// test declines to reopen it, which is the whole of "settled once":
			// every node is expanded at most once, so the walk's cost stays
			// bounded by the graph. Reopening instead would restore the optimal
			// answer under such a heuristic, and would do it by re-expanding
			// nodes in cascades that nothing here bounds — the trade this
			// package does not make, and the reason store.NodeHeuristic states
			// consistency as an obligation rather than a preference.
			//
			// It also covers cur itself, which is how a self-loop terminates.
			if seen && (prev.settled || prev.dist <= nd) {
				continue
			}
			best[step.Neighbour] = weightedEntry{parent: cur, edge: step.Edge, dist: nd}
			queue.push(nd+estimate(heuristic, step.Neighbour), step.Neighbour)
		}
	}

	if !found {
		return nil, ErrNoPath
	}
	return reconstructWeighted(g, src, dst, best)
}

// estimate applies the heuristic, treating nil as zero.
func estimate(h store.NodeHeuristic, id store.NodeID) float64 {
	if h == nil {
		return 0
	}
	return h(id)
}

// reconstructWeighted walks the parent chain from dst back to src.
//
// Counted first, then filled back-to-front, for the reason reconstructPath
// gives: the count loop mirrors the fill loop condition for condition, so the
// two cannot drift into disagreeing about the length of the same chain and
// silently truncating the path.
func reconstructWeighted(g store.GraphReader, src, dst store.NodeID, best map[store.NodeID]weightedEntry) (*PathResult, error) {
	n, e := 0, 0
	for cur := dst; ; {
		n++
		v := best[cur]
		if v.edge == store.InvalidEdgeID {
			break
		}
		e++
		cur = v.parent
	}

	ids := make([]store.NodeID, n)
	edges := make([]store.EdgeID, e)
	i, j := n, e
	for cur := dst; ; {
		i--
		ids[i] = cur
		v := best[cur]
		if v.edge == store.InvalidEdgeID {
			break
		}
		j--
		edges[j] = v.edge
		cur = v.parent
	}

	// The chain must terminate at src. It always does — only src is seeded with
	// an invalid edge — but a path that silently started somewhere else would be
	// a wrong answer rather than an error, and this is one comparison.
	if ids[0] != src {
		return nil, fmt.Errorf("path reconstruction ended at node %d, not the source %d", ids[0], src)
	}

	return materialisePath(g, ids, edges)
}
