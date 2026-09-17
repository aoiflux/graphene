package memory

// Iteration for the reference backend. See store/scan.go for the contract and
// disk/scan.go for the implementation that has to work for its living.
//
// This one materialises and sorts, which is the same trade the rest of this
// package makes and for the same reason: a snapshot here is already a copy of
// the whole graph, so a streaming scan over it would save nothing, and what the
// oracle owes the disk backend is an answer that is obviously right rather than
// one that is cheap. What it does share is the contract — ascending, and an
// error yielded rather than swallowed when the snapshot goes away underneath.

import (
	"iter"
	"slices"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

func (sn *snapshot) ScanNodes() iter.Seq2[store.NodeID, error] {
	return func(yield func(store.NodeID, error) bool) {
		s, err := sn.live()
		if err != nil {
			yield(store.InvalidNodeID, err)
			return
		}
		ids := make([]store.NodeID, 0, len(s.nodes))
		for id := range s.nodes {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			if _, err := sn.live(); err != nil {
				yield(store.InvalidNodeID, err)
				return
			}
			if !yield(id, nil) {
				return
			}
		}
	}
}

func (sn *snapshot) ScanEdges() iter.Seq2[store.EdgeID, error] {
	return func(yield func(store.EdgeID, error) bool) {
		s, err := sn.live()
		if err != nil {
			yield(store.InvalidEdgeID, err)
			return
		}
		ids := make([]store.EdgeID, 0, len(s.edges))
		for id := range s.edges {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			if _, err := sn.live(); err != nil {
				yield(store.InvalidEdgeID, err)
				return
			}
			if !yield(id, nil) {
				return
			}
		}
	}
}

func (sn *snapshot) ScanNodesByType(t store.NodeType) iter.Seq2[store.NodeID, error] {
	return func(yield func(store.NodeID, error) bool) {
		s, err := sn.live()
		if err != nil {
			yield(store.InvalidNodeID, err)
			return
		}
		// The postings are exact here — every write maintains them and the
		// snapshot cloned them — so unlike the disk reader there is nothing to
		// re-resolve against the records.
		for _, id := range s.nodesByType[t] {
			if _, err := sn.live(); err != nil {
				yield(store.InvalidNodeID, err)
				return
			}
			if !yield(id, nil) {
				return
			}
		}
	}
}

// --- store.PropertyEnumerator ---

// ForEachNodeProperty implements store.PropertyEnumerator for a snapshot. See
// disk/scan.go for why the entries are filtered against the frozen records: the
// property index is shared with the live store, and an entry naming a node this
// snapshot does not have would be filed against an ID a bulk import never
// mapped.
func (sn *snapshot) ForEachNodeProperty(fn func(id store.NodeID, key string, value []byte) bool) {
	s, err := sn.live()
	if err != nil {
		return
	}
	s.propIdx.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
		if !s.NodeExists(id) {
			return true
		}
		return fn(id, key, value)
	})
}

// ForEachEdgeProperty is ForEachNodeProperty for edge properties.
func (sn *snapshot) ForEachEdgeProperty(fn func(id store.EdgeID, key string, value []byte) bool) {
	s, err := sn.live()
	if err != nil {
		return
	}
	s.propIdx.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
		s.mu.RLock()
		live := s.edgeExistsLocked(id)
		s.mu.RUnlock()
		if !live {
			return true
		}
		return fn(id, key, value)
	})
}

// NodePropertyEntries implements store.PropertyEntryGrouper for a snapshot. See
// disk/scan.go for why the entries are filtered against the frozen records.
func (sn *snapshot) NodePropertyEntries(id store.NodeID) []store.PropertyEntry {
	s, err := sn.live()
	if err != nil {
		return nil
	}
	if !s.NodeExists(id) {
		return nil
	}
	return index.CopyEntries(s.propIdx.NodeEntriesOf(id))
}

// EdgePropertyEntries implements store.PropertyEntryGrouper for a snapshot.
func (sn *snapshot) EdgePropertyEntries(id store.EdgeID) []store.PropertyEntry {
	s, err := sn.live()
	if err != nil {
		return nil
	}
	s.mu.RLock()
	the := s.edgeExistsLocked(id)
	s.mu.RUnlock()
	if !the {
		return nil
	}
	return index.CopyEntries(s.propIdx.EdgeEntriesOf(id))
}

// --- Declarations ---

// The reference backend's half of the rule disk/scan.go states: a snapshot
// reports what the store declared and cannot declare anything itself, and a
// closed one reports nothing.

func (sn *snapshot) OrderedNodeProperties() []string {
	s, err := sn.live()
	if err != nil {
		return nil
	}
	return s.OrderedNodeProperties()
}

func (sn *snapshot) OrderedEdgeProperties() []string {
	s, err := sn.live()
	if err != nil {
		return nil
	}
	return s.OrderedEdgeProperties()
}

func (sn *snapshot) CompositeNodeProperties() [][]string {
	s, err := sn.live()
	if err != nil {
		return nil
	}
	return s.CompositeNodeProperties()
}

func (sn *snapshot) CompositeEdgeProperties() [][]string {
	s, err := sn.live()
	if err != nil {
		return nil
	}
	return s.CompositeEdgeProperties()
}
