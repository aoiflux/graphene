package disk

// Writing a batch larger than the caps allow, by writing several.
//
// # Why this is a different call and not a kinder AddNodesBatch
//
// AddNodesBatch is atomic: either every node is added or none is. That is a
// guarantee callers build on -- a failed batch leaves nothing behind to clean
// up -- and it is not a guarantee that can survive being split. Two commits are
// two commits, and a crash between them leaves the first.
//
// So the split is a separate call with its own contract rather than a quiet
// change to the existing one. Badger draws the line in the same place and in the
// same terms: Txn refuses with ErrTxnTooBig, WriteBatch retries around the
// refusal by committing and starting a fresh transaction, and its documentation
// says plainly that WriteBatch is not transactional. A caller who needs
// atomicity keeps the atomic call and splits the work themselves; a caller
// loading data wants this one.
//
// # Why it is not a stateful writer
//
// A buffering writer cannot return an identifier from AddNode, because the
// record has not been written when the call returns -- and an edge cannot name
// an endpoint that is still sitting in the buffer. Both problems disappear when
// the whole slice is handed over at once: the split happens inside, the
// identifiers come back in order, and an edge batch can reference nodes a
// previous call committed.
//
// The caller's own slice is still the caller's to bound. What this bounds is
// everything the engine does with it -- the copies, the framed log batch, and
// the delta growth between commits -- which is the part they could not bound
// themselves. bulk.Options is where a source too large to hold at all belongs.

import (
	"github.com/aoiflux/graphene/store"
)

// AddNodesInBatches adds nodes in as many batches as the configured caps
// require, returning every identifier assigned, in order.
//
// # It is not atomic, and that is the whole trade
//
// Each chunk is its own commit. A failure partway through returns the
// identifiers already committed alongside the error, and those records are in
// the store: there is no rollback, and a crash mid-call leaves whatever had been
// committed. This is the same shape AddNodes already documents for a third-party
// backend without the batch interface, so the contract is not a new one in this
// package -- only newly available on a backend that does have it.
//
// Use AddNodesBatch when either-all-or-none matters. Use this when the work is
// larger than one batch may be and the caller would otherwise be writing the
// same loop.
//
// With no caps configured this is one call to AddNodesBatch and is therefore
// atomic after all -- but a caller must not rely on that, because it depends on
// the store's configuration rather than on the call.
func (s *Store) AddNodesInBatches(nodes []*store.Node) ([]store.NodeID, error) {
	if err := s.mustWrite(); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, nil
	}

	// Sized for the whole result up front: the count is known, and growing it
	// per chunk would be the one allocation in this path proportional to
	// something other than the answer.
	out := make([]store.NodeID, 0, len(nodes))
	for len(nodes) > 0 {
		n := s.batchCaps.nodeChunk(nodes)
		ids, err := s.AddNodesBatch(nodes[:n])
		out = append(out, ids...)
		if err != nil {
			// The committed prefix goes back with the error. Discarding it would
			// strand records in the store that the caller has no identifier for,
			// which is worse than a partial answer clearly labelled as one.
			return out, err
		}
		nodes = nodes[n:]
	}
	return out, nil
}

// AddEdgesInBatches is AddNodesInBatches for edges, with the same contract.
//
// Endpoints must already exist, as they must for AddEdgesBatch. Splitting does
// not change that: an edge in a later chunk may name a node committed by an
// earlier AddNodesInBatches call, but nothing here creates nodes.
func (s *Store) AddEdgesInBatches(edges []*store.Edge) ([]store.EdgeID, error) {
	if err := s.mustWrite(); err != nil {
		return nil, err
	}
	if len(edges) == 0 {
		return nil, nil
	}

	out := make([]store.EdgeID, 0, len(edges))
	for len(edges) > 0 {
		n := s.batchCaps.edgeChunk(edges)
		ids, err := s.AddEdgesBatch(edges[:n])
		out = append(out, ids...)
		if err != nil {
			return out, err
		}
		edges = edges[n:]
	}
	return out, nil
}
