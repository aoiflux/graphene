package disk

// Compaction: merging the delta layer into a freshly built CSR image and
// truncating the WAL behind it. Split out of store.go, unchanged.
//
// Crash safety rests on the ordering here — write a temp file, checkpoint the
// WAL, rename atomically, then truncate — so the steps are not independent and
// should not be reordered without rereading why each sits where it does.

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/aoiflux/graphene/store"
)

// Compact merges the delta layer into the CSR and truncates the WAL.
// This should be called after a bulk ingest is complete.
// Compact is crash-safe: it writes a temp CSR file then atomically renames it.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect all nodes and edges from both CSR and delta.
	var nodes []nodeRecord
	var edges []rawEdge

	// From existing CSR — skip entries that a delta update has overridden or a
	// tombstone has deleted, so the rebuilt CSR reclaims their space and never
	// double-counts an updated entry.
	if s.csr != nil {
		for i := 1; i < len(s.csr.nodes); i++ {
			n := s.csr.nodes[i]
			if n.ID == store.InvalidNodeID {
				continue
			}
			if _, over := s.deltaNodes[n.ID]; over {
				continue
			}
			if _, del := s.deletedNodes[n.ID]; del {
				continue
			}
			nodes = append(nodes, n)
		}
		for i := 1; i < len(s.csr.edges); i++ {
			e := s.csr.edges[i]
			if e.ID == store.InvalidEdgeID {
				continue
			}
			if _, over := s.deltaEdges[e.ID]; over {
				continue
			}
			if _, del := s.deletedEdges[e.ID]; del {
				continue
			}
			edges = append(edges, e)
		}
	}

	// From delta.
	for _, n := range s.deltaNodes {
		nodes = append(nodes, nodeRecord{ID: n.ID, Labels: n.Labels, Properties: cloneBytes(n.Properties)})
	}
	for _, e := range s.deltaEdges {
		edges = append(edges, rawEdge{
			ID:         e.ID,
			Src:        e.Src,
			Dst:        e.Dst,
			Labels:     e.Labels,
			Weight:     e.Weight,
			Properties: cloneBytes(e.Properties),
		})
	}

	// Build new CSR.
	newCSR := Build(nodes, edges)
	// Persist the current sequence high-water marks so a subsequent reopen never
	// reuses an ID whose record was dropped from this rebuilt CSR.
	newCSR.nodeSeqHW = s.nodeSeq.Load()
	newCSR.edgeSeqHW = s.edgeSeq.Load()

	// Serialise to temp file, carrying the property index into the CSR so it no
	// longer has to be reconstructed from the WAL on the next open.
	data := newCSR.SerialiseWithIndex(s.propIdx.NodeEntries(), s.propIdx.EdgeEntries())
	tmpPath := filepath.Join(s.dir, csrFileName+".tmp")
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("compact: write tmp CSR: %w", err)
	}

	// Checkpoint WAL then atomic rename.
	if err := s.wal.Checkpoint(); err != nil {
		return fmt.Errorf("compact: wal checkpoint: %w", err)
	}

	csrPath := filepath.Join(s.dir, csrFileName)
	if err := os.Rename(tmpPath, csrPath); err != nil {
		return fmt.Errorf("compact: rename CSR: %w", err)
	}

	// Truncate WAL.
	if err := s.wal.Truncate(); err != nil {
		return fmt.Errorf("compact: wal truncate: %w", err)
	}

	// The property index is now inside the CSR file, so the truncated WAL is left
	// empty. Before v6 every compaction re-emitted the whole index here and every
	// restart replayed it, making both costs grow with the total number of
	// indexed entries no matter how little had changed.

	// Swap in new CSR and clear delta + delete masks (both are now baked into
	// the freshly built CSR).
	s.publishCSR(newCSR)
	s.deltaNodes = make(map[store.NodeID]*store.Node)
	s.deltaEdges = make(map[store.EdgeID]*store.Edge)
	s.deltaAdj = make(map[store.NodeID]*deltaAdj)
	s.deletedNodes = make(map[store.NodeID]struct{})
	s.deletedEdges = make(map[store.EdgeID]struct{})
	s.deltaNodesByType = make(map[store.NodeType][]store.NodeID)
	s.deltaEdgesByType = make(map[store.EdgeType][]store.EdgeID)

	return nil
}
