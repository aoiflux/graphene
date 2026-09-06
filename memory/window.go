package memory

// Where a query's window is allowed to reach back into the driving step.
//
// Identical to the disk planner's rule, and deliberately a separate copy rather
// than a shared one: the condition is about what this backend's own drivers
// emit and in what order, and a rule imported from the other store would be
// right by coincidence. The parity suite is what holds the two to the same
// answer. See disk/planner.go for the reasoning.

import "github.com/aoiflux/graphene/store"

// nodeDriverNeed reports how many leading candidates the driving step may stop
// at, or 0 when it must produce all of them.
func nodeDriverNeed(query store.NodeQuery) int {
	need := store.QueryWindowNeed(query.Offset, query.Limit)
	if need == 0 || len(query.Filters) > 0 || len(query.IDs) > 0 {
		return 0
	}
	if store.NormalizedQueryOrder(query.Order) != store.QueryOrderAsc {
		return 0
	}
	return need
}

// edgeDriverNeed is nodeDriverNeed for edge queries, with the two extra ways an
// edge query narrows after the driver: an endpoint restriction removes
// candidates exactly as a filter does.
func edgeDriverNeed(query store.EdgeQuery) int {
	if len(query.SrcIDs) > 0 || len(query.DstIDs) > 0 {
		return 0
	}
	need := store.QueryWindowNeed(query.Offset, query.Limit)
	if need == 0 || len(query.Filters) > 0 || len(query.IDs) > 0 {
		return 0
	}
	if store.NormalizedQueryOrder(query.Order) != store.QueryOrderAsc {
		return 0
	}
	return need
}

// nodeResidualNeed reports how many leading rows the residual pass may stop
// after, or 0 when it must evaluate every candidate. Filters are tolerated
// here — evaluating them is what the pass is — but the window must still be
// taken from the front. See disk/planner.go.
func nodeResidualNeed(query store.NodeQuery) int {
	need := store.QueryWindowNeed(query.Offset, query.Limit)
	if need == 0 || store.NormalizedQueryOrder(query.Order) != store.QueryOrderAsc {
		return 0
	}
	return need
}

// edgeResidualNeed is nodeResidualNeed for edge queries.
func edgeResidualNeed(query store.EdgeQuery) int {
	need := store.QueryWindowNeed(query.Offset, query.Limit)
	if need == 0 || store.NormalizedQueryOrder(query.Order) != store.QueryOrderAsc {
		return 0
	}
	return need
}
