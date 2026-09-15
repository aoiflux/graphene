package disk

// Byte-aware batch reads: GetNodesBatch with a ceiling on how much payload one
// call hands back.
//
// GetNodesBatch resolves every id it is given. That is the right default for a
// few hundred ids and the wrong one for half a million: the caller asked for a
// slice and gets a slice, however many bytes of blob it points at. These are the
// same read with a budget, returning the ids they did not reach so the caller
// can ask again.
//
// **What the budget counts, and what it does not.** It counts the payloads —
// the sum of len(Properties) over the records returned. It does not count the
// per-record *store.Node the batch allocates (56 bytes, one 64-byte size class)
// nor the slice of pointers holding them, which together come to about 72 bytes
// a record whatever the blobs weigh. A caller budgeting for a million records
// should allow for that separately; a caller budgeting for the blobs, which is
// the term that varies by five orders of magnitude here, is budgeting for the
// term that matters.
//
// **Under a mapped image the budget is not a bound on what the engine
// allocates.** It never was. A CSR-resident record's Properties slice points
// into the image mapping (see csrBytes), so the engine allocates the struct and
// nothing else, and the blob bytes are file-backed pages that become resident
// when the caller reads them. What the budget bounds is what the *caller* takes
// on: the bytes it is about to touch, decode, or copy. That is the useful
// quantity in both modes, and it is the only one that means the same thing in
// both.

import "github.com/aoiflux/graphene/store"

// GetNodesBatchBounded is GetNodesBatch with a ceiling on the payload bytes it
// returns, implementing store.BoundedBatchReader.
//
// It resolves ids in order and stops before the record that would take the
// batch's payload total past maxBytes, returning the ids it did not reach in
// rest. missing reports ids it did reach and did not find, exactly as
// GetNodesBatch does.
//
// **A call always consumes at least one id.** The budget is checked only once
// something is already in the batch, so a record whose blob alone exceeds
// maxBytes is returned on its own rather than refused. Without that rule a
// resume loop over a store holding one 64 MiB blob would spin forever on a
// caller's 8 MiB budget, which is a hang rather than an error and would be
// found in production rather than in a test. The cost is that the bound is on
// all but the first record of a batch, and the doc says so.
//
// A maxBytes of zero or less is not a special case and needs none: nothing fits
// beside the first record, so the call yields exactly one. A caller computing a
// budget as `remaining - used` gets safe-and-slow from an arithmetic slip
// rather than the unbounded batch it was trying to avoid.
func (s *Store) GetNodesBatchBounded(ids []store.NodeID, maxBytes int64) (found []*store.Node, missing []store.NodeID, rest []store.NodeID) {
	if len(ids) == 0 {
		return nil, nil, nil
	}

	// Lock-free first, on the same terms as GetNode: if every id this window
	// reaches lives in an unshadowed CSR, the store lock buys nothing.
	if csr, ok := s.csrFastRead(); ok {
		if f, used, whole := boundedFromCSR(csr, ids, maxBytes); whole && s.csrFastReadValid(csr) {
			return f, nil, ids[used:]
		}
		// One id outside the CSR — absent, or delta-resident, and the fast path
		// cannot tell which — or the CSR moved under us. Either way this window
		// is redone under the lock rather than merged with a locked remainder.
		// That is what the point read does on a miss, at a batch's granularity
		// instead of one record's; a bounded window is small by construction, so
		// what a miss costs is a second pass over a window, not over the batch.
	}

	s.mu.RLock()
	r := s.readerLocked()
	found, missing, rest = boundedFromReader(r, ids, maxBytes)
	s.mu.RUnlock()
	return found, missing, rest
}

// GetEdgesBatchBounded is GetNodesBatchBounded for edges.
func (s *Store) GetEdgesBatchBounded(ids []store.EdgeID, maxBytes int64) (found []*store.Edge, missing []store.EdgeID, rest []store.EdgeID) {
	if len(ids) == 0 {
		return nil, nil, nil
	}

	if csr, ok := s.csrFastRead(); ok {
		if f, used, whole := boundedEdgesFromCSR(csr, ids, maxBytes); whole && s.csrFastReadValid(csr) {
			return f, nil, ids[used:]
		}
	}

	s.mu.RLock()
	r := s.readerLocked()
	found, missing, rest = boundedEdgesFromReader(r, ids, maxBytes)
	s.mu.RUnlock()
	return found, missing, rest
}

// batchHint guesses how many records will fit, from the size of the first one.
//
// Without it a batch's slice grows by doubling from nil and allocates about
// twice the pointers it ends up holding — measured at +23.5% B/op against the
// unbounded read, which is churn the paging is supposed to be saving rather
// than adding. The first record's payload is an exact predictor when payloads
// are uniform and a guess when they are not, so the guess is clamped at both
// ends: never more than the ids on offer, and never more than 65,536 entries
// (512 KiB of pointers) however small the first record was. That bound is what
// makes a first record much smaller than its successors cost a fixed 512 KiB of
// over-reservation instead of one pointer per id in the whole request.
//
// A first record with no payload carries no information, so it reserves for the
// whole request and lets the clamp hold it down.
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

// boundedFromCSR fills a batch from the image alone. whole is false the moment
// an id is not in the CSR, and the partial result is then discarded — so the
// caller must check it before trusting f.
func boundedFromCSR(csr *CSRGraph, ids []store.NodeID, maxBytes int64) (f []*store.Node, used int, whole bool) {
	var total int64
	for i, id := range ids {
		rec, ok := csr.GetNode(id)
		if !ok {
			return nil, 0, false
		}
		n := int64(len(rec.Properties))
		if f == nil {
			f = make([]*store.Node, 0, batchHint(len(ids), maxBytes, n))
		} else if total+n > maxBytes {
			return f, i, true
		}
		f = append(f, &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)})
		total += n
	}
	return f, len(ids), true
}

func boundedEdgesFromCSR(csr *CSRGraph, ids []store.EdgeID, maxBytes int64) (f []*store.Edge, used int, whole bool) {
	var total int64
	for i, id := range ids {
		rec, ok := csr.GetEdge(id)
		if !ok {
			return nil, 0, false
		}
		n := int64(len(rec.Properties))
		if f == nil {
			f = make([]*store.Edge, 0, batchHint(len(ids), maxBytes, n))
		} else if total+n > maxBytes {
			return f, i, true
		}
		f = append(f, rawEdgeToStore(rec))
		total += n
	}
	return f, len(ids), true
}

// boundedFromReader fills a batch from one reader, so every record in it
// describes a single instant. Caller holds s.mu.
//
// A missing id consumes its place in the batch and contributes nothing to the
// total: a run of deleted ids does not stall the walk, and does not get charged
// for bytes it did not hand over.
func boundedFromReader(r reader, ids []store.NodeID, maxBytes int64) (found []*store.Node, missing []store.NodeID, rest []store.NodeID) {
	var total int64
	for i, id := range ids {
		n, ok := r.node(id)
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

func boundedEdgesFromReader(r reader, ids []store.EdgeID, maxBytes int64) (found []*store.Edge, missing []store.EdgeID, rest []store.EdgeID) {
	var total int64
	for i, id := range ids {
		e, ok := r.edge(id)
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
