package index

// Narrowing enough, and stopping.
//
// NarrowNodesByFilters evaluates the residual filters over every candidate it
// is given, because until now every caller wanted every row. A caller with a
// window does not: a query for ten rows off a labelled, filtered set has no use
// for the ten thousandth match, and paying for it is the same failure the limit
// push-down exists to fix one step earlier — cost tracking the graph rather
// than the answer. The push-down itself cannot help here, because a filter
// removes candidates and the tenth candidate is therefore not the tenth row.
//
// So the pass is run over a prefix of the candidates at a time, and stops as
// soon as enough have survived. What that must not cost is the adaptivity: the
// narrow re-decides probe-versus-build per filter against the *current*
// candidate count, and that decision is the difference between a lookup and a
// walk of the graph. Narrowing a chunk keeps it — the decision is simply made
// against the chunk — but it is made again per chunk, so a run that ends up
// draining everything would pay the fixed part of that decision once per chunk
// rather than once.
//
// The chunk therefore doubles. A window that is satisfied early touches a small
// prefix; one that is never satisfied reaches the end in a logarithmic number
// of passes, which bounds the repeated work at a small constant factor of the
// single-pass cost rather than a linear one.

import (
	"context"

	"github.com/aoiflux/graphene/store"
)

const (
	// residualFirstChunk is the first prefix a bounded narrow evaluates. Big
	// enough that a selective filter usually fills a small window from it,
	// small enough that a window of ten does not pay for a thousand.
	residualFirstChunk = 256

	// residualMaxChunk caps the doubling, so a long run settles into passes
	// large enough for the probe-versus-build decision to be the one the
	// unbounded pass would have made.
	residualMaxChunk = 8192
)

// NarrowNodesUntil returns the first need candidates matching every filter, in
// ascending order.
//
// It is NarrowNodesByFilters with a bound: same inputs, same contract — the
// candidates slice is consumed — and the same answer, truncated. need must be
// positive; a caller wanting all of them wants NarrowNodesByFilters.
//
// The result is a fresh slice rather than the candidates' backing array,
// because the survivors of several chunks are not contiguous in it.
func (p *PropertyIndex) NarrowNodesUntil(ctx context.Context, candidates []store.NodeID, filters []store.PropertyFilter, skip store.FilterMask, need int) ([]store.NodeID, error) {
	if need <= 0 {
		return nil, nil
	}
	out := make([]store.NodeID, 0, min(need, len(candidates)))
	size := residualFirstChunk
	for start := 0; start < len(candidates) && len(out) < need; {
		end := min(start+size, len(candidates))
		// Three-index slice, so the narrow's in-place filtering cannot reach
		// past the chunk into candidates this pass has not looked at yet.
		kept, err := p.NarrowNodesByFiltersCtx(ctx, candidates[start:end:end], filters, skip)
		if err != nil {
			return nil, err
		}
		out = append(out, kept...)
		start = end
		if size < residualMaxChunk {
			size *= 2
		}
	}
	if len(out) > need {
		out = out[:need]
	}
	return out, nil
}

// NarrowEdgesUntil is NarrowNodesUntil for edges.
func (p *PropertyIndex) NarrowEdgesUntil(ctx context.Context, candidates []store.EdgeID, filters []store.PropertyFilter, skip store.FilterMask, need int) ([]store.EdgeID, error) {
	if need <= 0 {
		return nil, nil
	}
	out := make([]store.EdgeID, 0, min(need, len(candidates)))
	size := residualFirstChunk
	for start := 0; start < len(candidates) && len(out) < need; {
		end := min(start+size, len(candidates))
		kept, err := p.NarrowEdgesByFiltersCtx(ctx, candidates[start:end:end], filters, skip)
		if err != nil {
			return nil, err
		}
		out = append(out, kept...)
		start = end
		if size < residualMaxChunk {
			size *= 2
		}
	}
	if len(out) > need {
		out = out[:need]
	}
	return out, nil
}
