package disk

// The write side of the delta layer: appending versions.
//
// Every function here requires s.mu held exclusively, which is what makes epoch
// allocation order equal apply order. They replace the direct map assignments
// that used to be scattered through the mutators, and the difference is not
// bookkeeping: a write no longer overwrites what was there, it stacks a new
// version on top, and a delete stacks a tombstone rather than erasing the
// entry. That is the whole of what makes a read at an older epoch possible.
//
// # What "retain" governs
//
// Three structures cannot express a version — the adjacency lists, the label
// postings, and the property index — so for them the choice is between removing
// an entry (cheap, and wrong for any reader that still needs it) and leaving it
// (correct, and paid for by a filtered-out candidate on read). The rule is the
// same in each case: remove when no reader can be behind, leave when one can.
// That is sound because a reader that starts *later* runs at a higher epoch and
// should not see the removed entry anyway.
//
// "No reader can be behind" is what retainLocked's second return value means,
// and it is not the same as "no snapshot is open". A plain read runs at
// visibleEpoch, which trails mutEpoch for the length of a transaction's
// durability wait — with the lock released across it. Removing a superseded
// posting in that window took a live record out of its own label's posting for
// every reader in it, which is why the condition is the wider one.

import (
	"github.com/aoiflux/graphene/store"
)

// putNode stacks a live version of n at epoch.
func (s *Store) putNode(epoch uint64, n *store.Node) {
	d := s.cur().delta
	retain, held := s.retainLocked()

	prev := d.nodes[n.ID]
	if prev != nil {
		if prev.node != nil {
			if !held {
				s.unindexNodeLabels(d, n.ID, prev.node.Labels)
			}
		} else {
			d.liveNodes++
			if s.readerLocked().nodeInCSR(n.ID) {
				d.maskedNodes--
			}
		}
	} else {
		d.liveNodes++
	}

	head := &nodeVersion{epoch: epoch, node: n, prev: prev}
	truncateNodeChain(head, retain, held)
	d.nodes[n.ID] = head

	s.indexNodeLabels(d, n.ID, n.Labels)
	// No adjacency entry is created here. An empty one is indistinguishable from
	// an absent one to every reader — deltaEdgeIDs and degree both treat a nil
	// entry as no edges, and the two passes that iterate d.adj only ever read
	// a.out and a.in — so seeding one per node bought an allocation and a map
	// insert on every write in exchange for nothing observable. The edge paths
	// create the entry when there is an edge to put in it, which is the only
	// moment it carries information.
}

// putEdge stacks a live version of e at epoch.
//
// fresh says the ID has just been allocated, so the caller already knows the
// edge is in neither layer and the two membership probes can be skipped. On the
// AddEdge path that is the difference between one map write and one map write
// plus two hash lookups per edge.
func (s *Store) putEdge(epoch uint64, e *store.Edge, fresh bool) {
	d := s.cur().delta
	retain, held := s.retainLocked()

	prev := d.edges[e.ID]
	if prev != nil {
		if prev.edge != nil {
			if !held {
				s.unindexEdgeLabels(d, e.ID, prev.edge.Labels)
			}
		} else {
			d.liveEdges++
			if s.readerLocked().edgeInCSR(e.ID) {
				d.maskedEdges--
			}
		}
	} else {
		d.liveEdges++
	}

	head := &edgeVersion{epoch: epoch, edge: e, prev: prev}
	truncateEdgeChain(head, retain, held)
	d.edges[e.ID] = head

	s.indexEdgeLabels(d, e.ID, e.Labels)

	// Delta adjacency lists only edges the image does not already hold, so the
	// two never name the same edge and the degree counts simply add. An edge
	// already known to either layer keeps whichever list already carries it.
	if fresh || (prev == nil && !s.readerLocked().edgeInCSR(e.ID)) {
		ensureAdj(d, e.Src).out = append(ensureAdj(d, e.Src).out, e.ID)
		ensureAdj(d, e.Dst).in = append(ensureAdj(d, e.Dst).in, e.ID)
	}
}

// tombstoneNode stacks a delete of id at epoch.
//
// Unlike the delete mask it replaces, this records delta-only deletions too:
// erasing the entry would make the deletion visible to every reader at once,
// including one whose epoch predates it.
func (s *Store) tombstoneNode(epoch uint64, id store.NodeID) {
	d := s.cur().delta
	retain, held := s.retainLocked()

	prev := d.nodes[id]
	inCSR := s.readerLocked().nodeInCSR(id)
	switch {
	case prev == nil:
		if inCSR {
			d.maskedNodes++
		}
	case prev.node != nil:
		d.liveNodes--
		if inCSR {
			d.maskedNodes++
		}
		if !held {
			s.unindexNodeLabels(d, id, prev.node.Labels)
		}
	default:
		// Already a tombstone; stacking another changes nothing but the epoch.
		return
	}

	head := &nodeVersion{epoch: epoch, node: nil, prev: prev}
	truncateNodeChain(head, retain, held)
	d.nodes[id] = head

	// The adjacency entry stays. Its edges are tombstoned by the cascade, and a
	// snapshot at an older epoch still has to walk them.
	s.propIdx.RemoveNode(id)
}

// tombstoneEdge stacks a delete of id at epoch.
func (s *Store) tombstoneEdge(epoch uint64, id store.EdgeID) {
	d := s.cur().delta
	retain, held := s.retainLocked()

	prev := d.edges[id]
	inCSR := s.readerLocked().edgeInCSR(id)
	switch {
	case prev == nil:
		if inCSR {
			d.maskedEdges++
		}
	case prev.edge != nil:
		d.liveEdges--
		if inCSR {
			d.maskedEdges++
		}
		if !held {
			s.unindexEdgeLabels(d, id, prev.edge.Labels)
		}
	default:
		return
	}

	head := &edgeVersion{epoch: epoch, edge: nil, prev: prev}
	truncateEdgeChain(head, retain, held)
	d.edges[id] = head

	s.propIdx.RemoveEdge(id)
}

// --- label postings ---

// indexNodeLabels adds id to the delta postings for each distinct label.
//
// Labels are deduplicated here because a caller may pass the same label twice;
// without this the postings would list id once per repetition, yielding
// duplicate query results. The record's own Labels slice is left untouched.
func (s *Store) indexNodeLabels(d *deltaLayer, id store.NodeID, labels []store.NodeType) {
	for i, lbl := range labels {
		if containsNodeTypeValue(labels[:i], lbl) {
			continue
		}
		ids := d.nodesByType[lbl]
		if n := len(ids); n == 0 || ids[n-1] < id {
			d.nodesByType[lbl] = append(ids, id)
			continue
		}
		if updated, added := store.InsertSortedID(ids, id); added {
			d.nodesByType[lbl] = updated
		}
	}
}

// unindexNodeLabels removes id from the delta postings for each of its labels.
//
// Only called when no reader can be behind; see the file comment for why.
// Otherwise the posting is left in place and the read path's re-resolution
// drops it. Leaving it is a no-op whenever the new version carries the same
// label, because the posting is a sorted set and the index call that follows
// re-adds what this would have removed — so the cost falls only on an update
// that actually drops a label.
func (s *Store) unindexNodeLabels(d *deltaLayer, id store.NodeID, labels []store.NodeType) {
	for _, lbl := range labels {
		ids, removed := store.DeleteSortedID(d.nodesByType[lbl], id)
		if !removed {
			continue
		}
		if len(ids) == 0 {
			delete(d.nodesByType, lbl)
			continue
		}
		d.nodesByType[lbl] = ids
	}
}

func (s *Store) indexEdgeLabels(d *deltaLayer, id store.EdgeID, labels []store.EdgeType) {
	for i, lbl := range labels {
		if containsEdgeTypeValue(labels[:i], lbl) {
			continue
		}
		ids := d.edgesByType[lbl]
		if n := len(ids); n == 0 || ids[n-1] < id {
			d.edgesByType[lbl] = append(ids, id)
			continue
		}
		if updated, added := store.InsertSortedID(ids, id); added {
			d.edgesByType[lbl] = updated
		}
	}
}

func (s *Store) unindexEdgeLabels(d *deltaLayer, id store.EdgeID, labels []store.EdgeType) {
	for _, lbl := range labels {
		ids, removed := store.DeleteSortedID(d.edgesByType[lbl], id)
		if !removed {
			continue
		}
		if len(ids) == 0 {
			delete(d.edgesByType, lbl)
			continue
		}
		d.edgesByType[lbl] = ids
	}
}

func containsNodeTypeValue(types []store.NodeType, t store.NodeType) bool {
	for _, v := range types {
		if v == t {
			return true
		}
	}
	return false
}

func containsEdgeTypeValue(types []store.EdgeType, t store.EdgeType) bool {
	for _, v := range types {
		if v == t {
			return true
		}
	}
	return false
}

func ensureAdj(d *deltaLayer, id store.NodeID) *deltaAdj {
	a, ok := d.adj[id]
	if !ok {
		a = &deltaAdj{}
		d.adj[id] = a
	}
	return a
}
