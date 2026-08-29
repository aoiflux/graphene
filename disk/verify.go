package disk

// Structural self-check and repair across every index the store holds.
//
// Both are structural only. Neither can tell whether an indexed *value* still
// matches its entity's properties, because those values are caller-encoded and
// the property blob is opaque to the engine — see Store.VerifyIndexes.
//
// # What versioning changed here
//
// The delta label postings used to be an exact description of the delta: every
// posting named a live record carrying that label, and every such record was
// posted. They are now a *superset*. A posting is left in place when removing it
// could hide a record from a snapshot that still reads at an epoch where the
// label was there, and the read path re-resolves every candidate anyway.
//
// So the invariant this checks is the one that still has teeth: every posting
// the read path needs must be present. An extra posting costs a candidate that
// gets filtered out; a missing one costs a query result, silently. Only the
// second is a bug, and only the second is checked.

import (
	"context"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// VerifyIndexes implements store.IndexVerifier. It cross-checks the CSR label
// postings, the CSR adjacency arrays, the delta label postings, and the property
// index against the live records, returning the first inconsistency found.
func (s *Store) VerifyIndexes() error {
	return s.VerifyIndexesCtx(context.Background())
}

// VerifyIndexesCtx is VerifyIndexes, abandoned if ctx is cancelled.
//
// Every pass here is read-only, so there is no point at which stopping leaves
// anything behind, and a cancelled verification means only that the question
// was not answered. That is what separates it from RebuildIndexesCtx, where
// most of the work cannot be abandoned safely.
//
// The read lock is the reason to bother. This walks every retained version,
// every posting and every property entry in the store with s.mu held; on a
// large store that is seconds during which no writer runs. A caller that has
// given up — a CI step past its deadline, an operator who cancelled — gets the
// lock back rather than waiting out a check nobody will read.
func (s *Store) VerifyIndexesCtx(ctx context.Context) error {
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	if err := s.propIdx.VerifyCtx(ctx); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	r := s.readerLocked()
	d := r.v.delta

	if r.v.csr != nil {
		if err := r.v.csr.verifyLabelIndex(); err != nil {
			return err
		}
		if err := r.v.csr.verifyAdjacency(); err != nil {
			return err
		}
	}

	// Postings must be strictly ascending and free of duplicates — the merge and
	// binary-search paths depend on both — and must name IDs the delta knows.
	for lbl, ids := range d.nodesByType {
		if err := cc.Step(); err != nil {
			return err
		}
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("delta node label index: %v postings are not strictly ascending", lbl)
		}
		for _, id := range ids {
			if _, ok := d.nodes[id]; !ok {
				return fmt.Errorf("delta node label index: %v lists node %d, which is not in the delta", lbl, id)
			}
		}
	}
	for lbl, ids := range d.edgesByType {
		if err := cc.Step(); err != nil {
			return err
		}
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("delta edge label index: %v postings are not strictly ascending", lbl)
		}
		for _, id := range ids {
			if _, ok := d.edges[id]; !ok {
				return fmt.Errorf("delta edge label index: %v lists edge %d, which is not in the delta", lbl, id)
			}
		}
	}

	// The direction that matters: nothing a read needs may be missing. Every
	// version still retained is reachable by some reader, so every version's
	// labels must be posted, not just the newest one's.
	for id, ver := range d.nodes {
		if err := cc.Step(); err != nil {
			return err
		}
		for cur := ver; cur != nil; cur = cur.prev {
			if cur.node == nil {
				continue
			}
			for _, lbl := range cur.node.Labels {
				if !containsSortedNodeID(d.nodesByType[lbl], id) {
					return fmt.Errorf("delta node label index: node %d carries %v at epoch %d but is missing from the postings",
						id, lbl, cur.epoch)
				}
			}
		}
	}
	for id, ver := range d.edges {
		if err := cc.Step(); err != nil {
			return err
		}
		for cur := ver; cur != nil; cur = cur.prev {
			if cur.edge == nil {
				continue
			}
			for _, lbl := range cur.edge.Labels {
				if !containsSortedEdgeID(d.edgesByType[lbl], id) {
					return fmt.Errorf("delta edge label index: edge %d carries %v at epoch %d but is missing from the postings",
						id, lbl, cur.epoch)
				}
			}
		}
	}

	// The maintained counters replaced questions that used to be answered by
	// len() on a map, so nothing else would notice them drifting. Recomputing
	// them is cheap next to the passes above and is the only check that would
	// catch a mutator forgetting to adjust one.
	live, masked := 0, 0
	for id, ver := range d.nodes {
		if ver.node != nil {
			live++
		} else if r.nodeInCSR(id) {
			masked++
		}
	}
	if live != d.liveNodes || masked != d.maskedNodes {
		return fmt.Errorf("delta node counters drifted: live %d (recorded %d), masked %d (recorded %d)",
			live, d.liveNodes, masked, d.maskedNodes)
	}
	live, masked = 0, 0
	for id, ver := range d.edges {
		if ver.edge != nil {
			live++
		} else if r.edgeInCSR(id) {
			masked++
		}
	}
	if live != d.liveEdges || masked != d.maskedEdges {
		return fmt.Errorf("delta edge counters drifted: live %d (recorded %d), masked %d (recorded %d)",
			live, d.liveEdges, masked, d.maskedEdges)
	}

	// Delta adjacency must agree with the delta edges' endpoints.
	//
	// An entry whose chain has been reduced to a bare tombstone is expected, not
	// an error: adjacency is append-only, so an edge added and then deleted with
	// no snapshot open leaves its ID listed here with nothing but the tombstone
	// behind it. The read path drops it. What would be an error is an ID the
	// delta has never heard of, or a record whose endpoint disagrees — endpoints
	// are immutable, so any retained version answers for all of them.
	for nodeID, a := range d.adj {
		if err := cc.Step(); err != nil {
			return err
		}
		for _, eid := range a.out {
			ver, known := d.edges[eid]
			if !known {
				return fmt.Errorf("delta adjacency: node %d lists outbound edge %d, which is not in the delta", nodeID, eid)
			}
			if e, live := anyEdgeVersion(ver); live && e.Src != nodeID {
				return fmt.Errorf("delta adjacency: node %d lists outbound edge %d, whose Src is %d", nodeID, eid, e.Src)
			}
		}
		for _, eid := range a.in {
			ver, known := d.edges[eid]
			if !known {
				return fmt.Errorf("delta adjacency: node %d lists inbound edge %d, which is not in the delta", nodeID, eid)
			}
			if e, live := anyEdgeVersion(ver); live && e.Dst != nodeID {
				return fmt.Errorf("delta adjacency: node %d lists inbound edge %d, whose Dst is %d", nodeID, eid, e.Dst)
			}
		}
	}

	// The property index must not outlive the entities it describes.
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if err := cc.Step(); err != nil {
			return err
		}
		if !r.nodeExists(id) {
			return fmt.Errorf("property index: node %d has entries but is not live", id)
		}
	}
	for _, id := range s.propIdx.IndexedEdgeIDs() {
		if err := cc.Step(); err != nil {
			return err
		}
		if !r.edgeExists(id) {
			return fmt.Errorf("property index: edge %d has entries but is not live", id)
		}
	}
	return nil
}

// anyEdgeVersion returns any retained record for the chain — they all agree on
// the endpoints, which are the only fields adjacency depends on. A chain of
// nothing but tombstones reports false.
func anyEdgeVersion(ver *edgeVersion) (*store.Edge, bool) {
	for cur := ver; cur != nil; cur = cur.prev {
		if cur.edge != nil {
			return cur.edge, true
		}
	}
	return nil, false
}

func containsSortedNodeID(ids []store.NodeID, target store.NodeID) bool {
	lo, hi := 0, len(ids)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch {
		case ids[mid] == target:
			return true
		case ids[mid] < target:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false
}

func containsSortedEdgeID(ids []store.EdgeID, target store.EdgeID) bool {
	lo, hi := 0, len(ids)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch {
		case ids[mid] == target:
			return true
		case ids[mid] < target:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false
}

// RebuildIndexes implements store.IndexRebuilder. It recomputes the CSR label
// postings, the delta label postings, and the delta adjacency from the records
// they describe, then drops property-index entries whose entity is not live.
//
// Rebuilt from every *retained* version, not only the newest. A version that is
// still in a chain is still reachable by some open snapshot, so rebuilding from
// the head alone would repair the index for current readers by breaking it for
// older ones — the one way a repair operation could do real damage.
//
// Everything it touches is derived state held in memory, so it writes nothing to
// disk and needs no WAL records. Use it after recovering a store whose indexes
// may not match its records; a following Compact persists the repaired state.
func (s *Store) RebuildIndexes() error {
	return s.RebuildIndexesCtx(context.Background())
}

// RebuildIndexesCtx is RebuildIndexes, abandoned if ctx is cancelled — but only
// where abandoning it is safe, which is not most of it.
//
// The rebuild proper clears the delta label postings and the delta adjacency
// and repopulates them from the records. Stopping half way through that leaves
// postings that name only some of the records carrying a label, which is
// precisely the fault this file's header calls the one that matters: a missing
// posting costs a query result, silently. So the rebuild is not interruptible,
// and the check before it is what makes cancellation useful — a caller with a
// dead context does not start.
//
// The dead-entry sweep afterwards is interruptible, and the guarantee it gives
// is not "the store is consistent" but "the store is no worse than this call
// found it". Those entries were already there; the sweep removes a prefix of
// them and stopping early leaves the rest, which is exactly the state the
// caller had before it asked. VerifyIndexes will still name them — an entry
// outliving its entity is a fault it reports — so a cancelled rebuild is a
// rebuild to run again, not a rebuild that succeeded. What it is not is a new
// fault: reads filter those candidates out, and the structural half, the half
// whose failure loses results silently, is whole.
//
// Same shape as CompactCtx, and for the same reason: cancellation reaches the
// part that can be thrown away and stops at the part that cannot.
func (s *Store) RebuildIndexesCtx(ctx context.Context) error {
	if err := s.mustWrite(); err != nil {
		return err
	}
	cc := store.NewCancelCheck(ctx)
	if err := cc.Check(); err != nil {
		return err
	}
	s.mu.Lock()

	r := s.readerLocked()
	d := r.v.delta

	if r.v.csr != nil {
		r.v.csr.buildLabelIndex()
	}

	d.nodesByType = make(map[store.NodeType][]store.NodeID, len(d.nodesByType))
	d.edgesByType = make(map[store.EdgeType][]store.EdgeID, len(d.edgesByType))
	d.adj = make(map[store.NodeID]*deltaAdj, len(d.adj))

	for id, ver := range d.nodes {
		ensureAdj(d, id)
		for cur := ver; cur != nil; cur = cur.prev {
			if cur.node != nil {
				s.indexNodeLabels(d, id, cur.node.Labels)
			}
		}
	}
	for id, ver := range d.edges {
		endpoints, live := anyEdgeVersion(ver)
		for cur := ver; cur != nil; cur = cur.prev {
			if cur.edge != nil {
				s.indexEdgeLabels(d, id, cur.edge.Labels)
			}
		}
		// Mirrors putEdge: delta adjacency lists only edges the CSR adjacency
		// does not already cover, so the two never double-count.
		if live && !r.edgeInCSR(id) {
			ensureAdj(d, endpoints.Src).out = append(ensureAdj(d, endpoints.Src).out, id)
			ensureAdj(d, endpoints.Dst).in = append(ensureAdj(d, endpoints.Dst).in, id)
		}
	}

	// From here on the structure is whole again, so cancelling costs only the
	// sweep. The scan runs under the lock and the removals do not, which is why
	// the two loops are separate; both are interruptible.
	var deadNodes []store.NodeID
	var cerr error
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if cerr = cc.Step(); cerr != nil {
			break
		}
		if !r.nodeExists(id) {
			deadNodes = append(deadNodes, id)
		}
	}
	var deadEdges []store.EdgeID
	if cerr == nil {
		for _, id := range s.propIdx.IndexedEdgeIDs() {
			if cerr = cc.Step(); cerr != nil {
				break
			}
			if !r.edgeExists(id) {
				deadEdges = append(deadEdges, id)
			}
		}
	}
	s.mu.Unlock()
	if cerr != nil {
		return cerr
	}

	for _, id := range deadNodes {
		if err := cc.Step(); err != nil {
			return err
		}
		s.propIdx.RemoveNode(id)
	}
	for _, id := range deadEdges {
		if err := cc.Step(); err != nil {
			return err
		}
		s.propIdx.RemoveEdge(id)
	}
	return nil
}

// ForEachNodeProperty implements store.PropertyEnumerator.
//
// It reads the property index directly rather than through the view, which is
// correct for what it is for: an export wants every entry the index holds, and
// the index is not versioned — an entry removed by a later epoch is gone from
// it, and one added is present. A caller needing a consistent pair of records
// and entries should take a Snapshot and export from that instead.
func (s *Store) ForEachNodeProperty(fn func(id store.NodeID, key string, value []byte) bool) {
	s.propIdx.ForEachNodeProperty(fn)
}

// ForEachEdgeProperty implements store.PropertyEnumerator.
func (s *Store) ForEachEdgeProperty(fn func(id store.EdgeID, key string, value []byte) bool) {
	s.propIdx.ForEachEdgeProperty(fn)
}
