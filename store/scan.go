package store

// Iteration.
//
// Every read on GraphReader answers with a slice, which is the right shape for
// a lookup and the wrong one for a graph: QueryNodeIDs(NodeQuery{}) on a
// million-node store builds a million-element slice before the caller sees the
// first ID, and a query asking for ten of them builds it too. Scanner is the
// other shape — the IDs arrive one at a time and the caller decides when to
// stop.
//
// It is offered over a Snapshot and not over a live store, which is not a
// limitation so much as the answer to the question a live iterator cannot
// answer: what a scan should do when a writer changes the graph underneath it.
// A snapshot has already fixed that. It also makes the scan cheap — a delta
// layer a compaction has superseded is never written to again, so iterating one
// takes no lock at all.

import "iter"

// Scanner is an optional capability: iteration over a graph's IDs without
// materialising them all. Both bundled backends implement it on their Snapshot;
// a caller type-asserts for it and falls back to the slice-returning reads when
// a store does not offer it.
//
// # Order
//
// Every sequence is ascending by ID, the same order the equivalent query
// returns, so a scan can replace one without changing what a consumer writes.
//
// # Errors
//
// The sequences are Seq2 rather than Seq because a scan can fail partway: the
// snapshot behind it can be closed or can expire while the caller is still
// pulling. A non-nil error is the last thing a sequence yields, and the ID
// alongside it is not a result. Yielding it rather than swallowing it is the
// point — a scan that stopped early and a scan that finished must not look the
// same to the caller.
//
// # Stopping
//
// break, as with any range-over-func. Nothing has to be closed.
type Scanner interface {
	// ScanNodes yields every live node ID, ascending.
	ScanNodes() iter.Seq2[NodeID, error]

	// ScanEdges yields every live edge ID, ascending.
	ScanEdges() iter.Seq2[EdgeID, error]

	// ScanNodesByType yields the live node IDs carrying t, ascending. It is the
	// streaming form of NodesByType and resolves each candidate against the
	// records the same way.
	ScanNodesByType(t NodeType) iter.Seq2[NodeID, error]
}
