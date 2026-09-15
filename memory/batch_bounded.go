package memory

// Byte-aware batch reads. See disk/batch_bounded.go for what the budget counts
// and why the first record of a batch is exempt from it; the contract is
// store.BoundedBatchReader's and is the same for both backends.

import "github.com/aoiflux/graphene/store"

// batchHint is disk.batchHint, which the two backends cannot share without one
// of them importing the other. See disk/batch_bounded.go for why the clamps are
// where they are; the arithmetic is kept identical so a batch of the same shape
// reserves the same amount on either backend.
func batchHint(n int, maxBytes, first int64) int {
	const maxHint = 1 << 16
	est := int64(n)
	if first > 0 {
		if maxBytes <= 0 {
			return 1
		}
		est = maxBytes/first + 1
	}
	if est > int64(n) {
		est = int64(n)
	}
	if est > maxHint {
		est = maxHint
	}
	if est < 1 {
		est = 1
	}
	return int(est)
}

// GetNodesBatchBounded is GetNodesBatch with a ceiling on the payload bytes it
// returns, implementing store.BoundedBatchReader.
//
// The memory backend holds the caller's own records, so a batch's payload bytes
// are already resident whether this method hands them back or not. Bounding
// them is still worth having: what the budget limits here is what the *caller*
// accumulates in the slice it builds from the batch, and a consumer written
// against the interface should behave the same way on both backends rather than
// discovering that one of them ignores the argument.
func (s *Store) GetNodesBatchBounded(ids []store.NodeID, maxBytes int64) (found []*store.Node, missing []store.NodeID, rest []store.NodeID) {
	if len(ids) == 0 {
		return nil, nil, nil
	}
	var total int64

	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, id := range ids {
		n, ok := s.nodes[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		b := int64(len(n.Properties))
		if found == nil {
			found = make([]*store.Node, 0, batchHint(len(ids), maxBytes, b))
		} else if total+b > maxBytes {
			return found, missing, ids[i:]
		}
		found = append(found, n)
		total += b
	}
	return found, missing, nil
}

// GetEdgesBatchBounded is GetNodesBatchBounded for edges.
func (s *Store) GetEdgesBatchBounded(ids []store.EdgeID, maxBytes int64) (found []*store.Edge, missing []store.EdgeID, rest []store.EdgeID) {
	if len(ids) == 0 {
		return nil, nil, nil
	}
	var total int64

	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, id := range ids {
		e, ok := s.edges[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		b := int64(len(e.Properties))
		if found == nil {
			found = make([]*store.Edge, 0, batchHint(len(ids), maxBytes, b))
		} else if total+b > maxBytes {
			return found, missing, ids[i:]
		}
		found = append(found, e)
		total += b
	}
	return found, missing, nil
}
