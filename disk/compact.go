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
	"time"

	"github.com/aoiflux/graphene/merkle"
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

	// v8 carries two more marks across the truncation that follows.
	//
	// Compaction empties the WAL, and until v8 that took the commit sequence and
	// the last compaction time with it: the counter restarted from zero on the
	// next open, and nothing could say when the image was built. Both now survive
	// in the header, which is what makes a commit sequence number a durable
	// identity rather than an ordering within one log generation.
	compactedAt := time.Now()
	newCSR.commitSeqHW = s.commitSeq.Load()
	newCSR.lastCompactUnixNano = compactedAt.UnixNano()

	// Serialise to temp file, carrying the property index into the CSR so it no
	// longer has to be reconstructed from the WAL on the next open.
	// Ordered-key declarations travel with the image. Without this every reopen
	// silently turned declared range queries back into scans.
	// Chain this image to the one it replaces, so the sequence of compactions is
	// itself verifiable. A substituted snapshot breaks the link even when the
	// substitute is internally consistent.
	var prevRoot merkle.Hash
	if s.csr != nil {
		if r, ok := s.csr.Roots(); ok {
			prevRoot = r.Snapshot
		}
	}

	// The attestation chains to the previous image's, so a removed attestation is
	// provably missing rather than invisibly absent.
	var prevAttestation [attestationIDSize]byte
	if s.csr != nil {
		prevAttestation = s.csr.attestation.ID
	}

	data, err := newCSR.SerialiseWithPayload(csrPayload{
		NodeProps:         s.propIdx.NodeEntries(),
		EdgeProps:         s.propIdx.EdgeEntries(),
		OrderedNodeKeys:   s.propIdx.OrderedNodeKeys(),
		OrderedEdgeKeys:   s.propIdx.OrderedEdgeKeys(),
		PrevSnapshotRoot:  prevRoot,
		WithSnapshotRoots: true,
		Signer:            s.signer,
		AttestActorID:     s.attestActorID,
		AttestUnixNano:    compactedAt.UnixNano(),
		PrevAttestation:   prevAttestation,
	})
	if err != nil {
		return fmt.Errorf("compact: %w", err)
	}
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

	// Retire the WAL.
	//
	// With retention configured the log is rotated to a numbered segment and
	// kept; without it, it is truncated as before. The distinction is what a
	// caller asked for, not what the engine thinks best — how long evidence is
	// held is not the engine's decision.
	//
	// Either way this happens *after* the CSR has been renamed into place, so a
	// crash between the two leaves a log that still holds everything the new
	// image already contains. Replaying it again is harmless; losing it would
	// not be.
	if s.retention.Keeps() {
		seg, err := s.wal.Rotate(s.dir, s.segmentSeq)
		if err != nil {
			return fmt.Errorf("compact: wal rotate: %w", err)
		}
		s.segmentSeq++

		removed, err := applyRetention(s.dir, s.retention)
		if err != nil {
			return fmt.Errorf("compact: %w", err)
		}
		// Recorded because it is a deletion of evidence, whoever authorised it.
		// A retention policy quietly discarding history is the thing an audit is
		// for, even — especially — when the discarding was intended.
		for _, r := range removed {
			if aerr := s.recordAudit(AuditRetentionDelete, s.attestActorID,
				fmt.Sprintf("segment %d (%d bytes, digest %x)", r.Sequence, r.Bytes, r.Digest[:8])); aerr != nil {
				return fmt.Errorf("compact: %w", aerr)
			}
		}
		if aerr := s.recordAudit(AuditCompact, s.attestActorID,
			fmt.Sprintf("retired segment %d; %d nodes, %d edges; snapshot %x",
				seg.Sequence, len(nodes), len(edges), newCSR.roots.Snapshot[:8])); aerr != nil {
			return fmt.Errorf("compact: %w", aerr)
		}
	} else {
		if err := s.wal.Truncate(); err != nil {
			return fmt.Errorf("compact: wal truncate: %w", err)
		}
		if aerr := s.recordAudit(AuditCompact, s.attestActorID,
			fmt.Sprintf("log discarded; %d nodes, %d edges; snapshot %x",
				len(nodes), len(edges), newCSR.roots.Snapshot[:8])); aerr != nil {
			return fmt.Errorf("compact: %w", aerr)
		}
	}

	// Rotation starts a fresh active log, so the rotations recorded in the
	// retired one are no longer in the live timeline. They remain readable in
	// the segment, which is the point of keeping it.
	s.keyTimeline = nil

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

	s.lastCompact = compactedAt

	return nil
}
