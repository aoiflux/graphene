package disk

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// Store is the on-disk implementation of store.GraphStore.
//
// Architecture:
//   - New writes go to WAL + delta (in-memory map) immediately.
//   - Reads merge CSR (bulk data after last compaction) + delta layer.
//   - Compact() rebuilds the CSR from the merged set and truncates the WAL.
//
// This is optimised for bulk ingest → read-heavy workloads. CSR provides
// O(degree) neighbourhood queries with sequential memory access.
type Store struct {
	mu  sync.RWMutex
	dir string
	wal *WAL
	csr *CSRGraph // nil until first compaction

	// Delta layer: nodes and edges added since last compaction.
	deltaNodes map[store.NodeID]*store.Node
	deltaEdges map[store.EdgeID]*store.Edge
	deltaAdj   map[store.NodeID]*deltaAdj

	// Delete masks: IDs removed since the last compaction that still live in the
	// CSR. They hide the stale CSR record from every read until Compact rebuilds
	// the CSR without them. Delta-only deletions are handled by removing the
	// entry from the delta maps directly and never appear here.
	deletedNodes map[store.NodeID]struct{}
	deletedEdges map[store.EdgeID]struct{}

	// Type indexes over delta (CSR has its own type lookups).
	deltaNodesByType map[store.NodeType][]store.NodeID
	deltaEdgesByType map[store.EdgeType][]store.EdgeID

	// --- lock-free read support ---
	//
	// A CSRGraph is immutable once published, so a reader that gets the pointer
	// atomically can read a record from it without holding s.mu. The lock is
	// still needed for the delta maps and the delete masks, which are ordinary Go
	// maps — but those only *shadow* CSR records, they never rewrite them.
	//
	// csrShadowed counts CSR records that a delta update or a tombstone has
	// superseded within the current epoch. While it is zero, every CSR record is
	// still the truth, so a point read that finds its answer in the CSR needs no
	// lock at all. Crucially, appending new entities does not shadow anything, so
	// ongoing writes do not disable the fast path for pre-existing records.
	//
	// The counter is only ever incremented within the life of one CSR (Compact
	// resets it when it publishes the next), so reading zero after a lookup proves
	// it was zero throughout. A reader also re-checks the CSR *pointer* it read
	// from, which is what catches a Compact that swapped the CSR and cleared the
	// counter underneath it.
	csrPtr      atomic.Pointer[CSRGraph]
	csrShadowed atomic.Int64

	// Property index (in-memory; rebuilt from WAL on restart).
	propIdx *index.PropertyIndex

	// reindexPolicy governs what updates do to propIdx. Guarded by mu.
	reindexPolicy store.ReindexPolicy

	// Sequence counters (shared across CSR and delta).
	nodeSeq atomic.Uint64
	edgeSeq atomic.Uint64
}

type deltaAdj struct {
	out []store.EdgeID
	in  []store.EdgeID
}

const (
	walFileName = "graphene.wal"
	csrFileName = "graphene.csr"

	labelCountFieldBytes      = 1
	legacyLabelBytesPerValue  = 1
	currentLabelBytesPerValue = 2

	nodePayloadIDBytes      = 8
	nodePayloadLabelStart   = nodePayloadIDBytes + labelCountFieldBytes
	nodePayloadPropLenBytes = 4

	edgePayloadIDsBytes      = 8 + 8 + 8
	edgePayloadLabelStart    = edgePayloadIDsBytes + labelCountFieldBytes
	edgePayloadWeightBytes   = 4
	edgePayloadPropLenBytes  = 4
	edgePayloadTailFixedSize = edgePayloadWeightBytes + edgePayloadPropLenBytes

	csrVersionV2             = 2
	csrVersionV3             = 3
	csrVersionWithU16Labels  = 4
	csrVersionWithSeqHW      = 5
	csrVersionWithPropIndex  = 6
	csrVersionCurrent        = csrVersionWithPropIndex
	csrV6HeaderSize          = 46 // magic4 + version2 + counts16 + seqHW16 + indexOffset8
	csrV5HeaderSize          = 38
	csrIndexSectionMagic     = "GIDX"
	csrIndexSectionMagicSize = 4
)

// Open opens (or creates) a disk-backed Store rooted at dir.
// On first use dir will be created. On restart, the WAL is replayed into the
// delta layer; the existing CSR (if any) is memory-mapped.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("disk.Open: mkdir %s: %w", dir, err)
	}

	wal, err := OpenWAL(filepath.Join(dir, walFileName))
	if err != nil {
		return nil, err
	}

	s := &Store{
		dir:              dir,
		wal:              wal,
		deltaNodes:       make(map[store.NodeID]*store.Node),
		deltaEdges:       make(map[store.EdgeID]*store.Edge),
		deltaAdj:         make(map[store.NodeID]*deltaAdj),
		deletedNodes:     make(map[store.NodeID]struct{}),
		deletedEdges:     make(map[store.EdgeID]struct{}),
		deltaNodesByType: make(map[store.NodeType][]store.NodeID),
		deltaEdgesByType: make(map[store.EdgeType][]store.EdgeID),
		propIdx:          index.NewPropertyIndex(),
	}

	// From here on the WAL file handle is open, so every failure path has to
	// close it — otherwise a rejected store leaks the handle and, on Windows,
	// leaves the file undeletable.
	fail := func(format string, args ...any) (*Store, error) {
		wal.Close()
		return nil, fmt.Errorf(format, args...)
	}

	// Try to load existing CSR.
	csrPath := filepath.Join(dir, csrFileName)
	if _, err := os.Stat(csrPath); err == nil {
		if err := s.loadCSR(csrPath); err != nil {
			return fail("disk.Open: load CSR: %w", err)
		}
	}

	// Replay WAL into delta.
	if err := s.replayWAL(); err != nil {
		return fail("disk.Open: replay WAL: %w", err)
	}

	// Open deliberately does NOT run VerifyIndexes.
	//
	// It measured ~200ms on a 100k-node store — an O(V+E) tax on every startup —
	// and it cannot catch much that is not already covered:
	//
	//   - Corruption in the CSR index section is rejected while parsing it
	//     (magic, counts, and bounds are all checked in readCSRIndexSection).
	//   - The label postings and the property index are both reconstructed by
	//     inserting through the normal code paths, which sort and deduplicate, so
	//     the structures are correct by construction whatever the file contained.
	//
	// What is left is engine bugs, which belong in tests, not in a startup scan.
	// Callers recovering a suspect store can run Graph.VerifyIndexes() and then
	// Graph.RebuildIndexes() explicitly.

	return s, nil
}

// --- GraphStore implementation ---

func (s *Store) AddNode(n *store.Node) (store.NodeID, error) {
	stored := &store.Node{}
	if len(n.Labels) > 0 {
		stored.Labels = make([]store.NodeType, len(n.Labels))
		copy(stored.Labels, n.Labels)
	}
	if len(n.Properties) > 0 {
		stored.Properties = make([]byte, len(n.Properties))
		copy(stored.Properties, n.Properties)
	}

	// ID assignment, WAL append, and delta apply are all done under s.mu so the
	// operation is atomic w.r.t. other writers (WAL order == apply order).
	s.mu.Lock()
	defer s.mu.Unlock()

	id := store.NodeID(s.nodeSeq.Add(1))
	stored.ID = id

	// Serialise to WAL payload: id(8) + labelCount(1) + labels(2*N) + propLen(4) + props
	if err := s.wal.AppendNode(marshalNode(stored)); err != nil {
		return store.InvalidNodeID, fmt.Errorf("AddNode: wal: %w", err)
	}

	s.deltaNodes[id] = stored
	s.indexDeltaNodeLabels(id, stored.Labels)
	s.ensureDeltaAdj(id)

	return id, nil
}

// AddNodesBatch adds nodes in order and returns assigned IDs.
// On error, successfully written prefixes are committed and returned.
func (s *Store) AddNodesBatch(nodes []*store.Node) ([]store.NodeID, error) {
	ids := make([]store.NodeID, len(nodes))
	stored := make([]*store.Node, len(nodes))

	// The whole batch runs under one lock hold: WAL append order matches apply
	// order and the batch is atomic w.r.t. other writers.
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, n := range nodes {
		id := store.NodeID(s.nodeSeq.Add(1))
		ids[i] = id

		node := &store.Node{ID: id}
		if len(n.Labels) > 0 {
			node.Labels = make([]store.NodeType, len(n.Labels))
			copy(node.Labels, n.Labels)
		}
		if len(n.Properties) > 0 {
			node.Properties = make([]byte, len(n.Properties))
			copy(node.Properties, n.Properties)
		}
		stored[i] = node
	}

	committed := 0
	for i := range stored {
		if err := s.wal.AppendNode(marshalNode(stored[i])); err != nil {
			s.commitNodesBatch(stored[:committed])
			return ids[:committed], fmt.Errorf("AddNodesBatch: wal: %w", err)
		}
		committed++
	}

	s.commitNodesBatch(stored)
	return ids, nil
}

func (s *Store) AddEdge(e *store.Edge) (store.EdgeID, error) {
	stored := &store.Edge{
		Src:    e.Src,
		Dst:    e.Dst,
		Weight: e.Weight,
	}
	if len(e.Labels) > 0 {
		stored.Labels = make([]store.EdgeType, len(e.Labels))
		copy(stored.Labels, e.Labels)
	}
	if len(e.Properties) > 0 {
		stored.Properties = make([]byte, len(e.Properties))
		copy(stored.Properties, e.Properties)
	}

	// Endpoint validation, WAL append, and delta apply are all done under one
	// lock hold, so an edge can never be created onto a node that a concurrent
	// DeleteNode has already removed.
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.nodeExistsLocked(e.Src) {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Src}
	}
	if !s.nodeExistsLocked(e.Dst) {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Dst}
	}

	id := store.EdgeID(s.edgeSeq.Add(1))
	stored.ID = id

	if err := s.wal.AppendEdge(marshalEdge(stored)); err != nil {
		return store.InvalidEdgeID, fmt.Errorf("AddEdge: wal: %w", err)
	}

	s.deltaEdges[id] = stored
	s.indexDeltaEdgeLabels(id, stored.Labels)
	s.ensureDeltaAdj(stored.Src).out = append(s.ensureDeltaAdj(stored.Src).out, id)
	s.ensureDeltaAdj(stored.Dst).in = append(s.ensureDeltaAdj(stored.Dst).in, id)

	return id, nil
}

// AddEdgesBatch adds edges in order and returns assigned IDs.
// On error, successfully written prefixes are committed and returned.
func (s *Store) AddEdgesBatch(edges []*store.Edge) ([]store.EdgeID, error) {
	ids := make([]store.EdgeID, len(edges))
	stored := make([]*store.Edge, len(edges))

	// Whole batch under one lock hold: endpoint validation cannot race a
	// concurrent DeleteNode, and WAL order matches apply order.
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, e := range edges {
		if !s.nodeExistsLocked(e.Src) {
			return ids[:i], &store.ErrInvalidEdge{MissingID: e.Src}
		}
		if !s.nodeExistsLocked(e.Dst) {
			return ids[:i], &store.ErrInvalidEdge{MissingID: e.Dst}
		}

		id := store.EdgeID(s.edgeSeq.Add(1))
		ids[i] = id

		edge := &store.Edge{
			ID:     id,
			Src:    e.Src,
			Dst:    e.Dst,
			Weight: e.Weight,
		}
		if len(e.Labels) > 0 {
			edge.Labels = make([]store.EdgeType, len(e.Labels))
			copy(edge.Labels, e.Labels)
		}
		if len(e.Properties) > 0 {
			edge.Properties = make([]byte, len(e.Properties))
			copy(edge.Properties, e.Properties)
		}
		stored[i] = edge
	}

	committed := 0
	for i := range stored {
		if err := s.wal.AppendEdge(marshalEdge(stored[i])); err != nil {
			s.commitEdgesBatch(stored[:committed])
			return ids[:committed], fmt.Errorf("AddEdgesBatch: wal: %w", err)
		}
		committed++
	}

	s.commitEdgesBatch(stored)
	return ids, nil
}

// The four mutators hold s.mu across BOTH the WAL append and the in-memory
// apply. This keeps the WAL record order identical to the apply order (so the
// reopened state always matches the live state) and makes DeleteNode's cascade
// atomic. The WAL append is safe under s.mu: WAL maintenance ops (Checkpoint /
// Truncate) only run inside Compact, which itself holds s.mu, so they can never
// run concurrently with a mutator.

// SetReindexPolicy implements store.Reindexer.
func (s *Store) SetReindexPolicy(p store.ReindexPolicy) {
	s.mu.Lock()
	s.reindexPolicy = p
	s.mu.Unlock()
}

// ReindexPolicy implements store.Reindexer.
func (s *Store) ReindexPolicy() store.ReindexPolicy {
	s.mu.RLock()
	p := s.reindexPolicy
	s.mu.RUnlock()
	return p
}

// DeclareOrderedNodeProperty implements store.OrderedIndexDeclarer.
func (s *Store) DeclareOrderedNodeProperty(key string) error {
	s.propIdx.DeclareOrderedNodeKey(key)
	return nil
}

// DeclareOrderedEdgeProperty implements store.OrderedIndexDeclarer.
func (s *Store) DeclareOrderedEdgeProperty(key string) error {
	s.propIdx.DeclareOrderedEdgeKey(key)
	return nil
}

// OrderedNodeProperties implements store.OrderedIndexDeclarer.
func (s *Store) OrderedNodeProperties() []string { return s.propIdx.OrderedNodeKeys() }

// OrderedEdgeProperties implements store.OrderedIndexDeclarer.
func (s *Store) OrderedEdgeProperties() []string { return s.propIdx.OrderedEdgeKeys() }

// PurgeNodeIndex implements store.Reindexer. The purge is journalled so replay
// does not resurrect the superseded entries.
func (s *Store) PurgeNodeIndex(id store.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.purgeNodeIndexLocked(id)
}

// PurgeEdgeIndex implements store.Reindexer.
func (s *Store) PurgeEdgeIndex(id store.EdgeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.purgeEdgeIndexLocked(id)
}

// purgeNodeIndexLocked journals then applies a property-index purge. Caller must
// hold s.mu. Writing the record unconditionally keeps replay deterministic even
// when the index currently holds nothing for id.
func (s *Store) purgeNodeIndexLocked(id store.NodeID) error {
	if err := s.wal.AppendNodePropPurge(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("purge node index: wal: %w", err)
	}
	s.propIdx.RemoveNode(id)
	return nil
}

// purgeEdgeIndexLocked journals then applies a property-index purge for an edge.
func (s *Store) purgeEdgeIndexLocked(id store.EdgeID) error {
	if err := s.wal.AppendEdgePropPurge(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("purge edge index: wal: %w", err)
	}
	s.propIdx.RemoveEdge(id)
	return nil
}

func (s *Store) UpdateNode(n *store.Node) error {
	if len(n.Labels) == 0 {
		return fmt.Errorf("UpdateNode: node %d must carry at least one label", n.ID)
	}

	stored := &store.Node{ID: n.ID}
	stored.Labels = make([]store.NodeType, len(n.Labels))
	copy(stored.Labels, n.Labels)
	if len(n.Properties) > 0 {
		stored.Properties = make([]byte, len(n.Properties))
		copy(stored.Properties, n.Properties)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.nodeExistsLocked(n.ID) {
		return &store.ErrNotFound{Kind: "node", ID: uint64(n.ID)}
	}
	// An edit is a fresh node record re-appended with the same ID; replay applies
	// it as an upsert (last write wins).
	if err := s.wal.AppendNode(marshalNode(stored)); err != nil {
		return fmt.Errorf("UpdateNode: wal: %w", err)
	}
	if s.reindexPolicy == store.ReindexPurge {
		if err := s.purgeNodeIndexLocked(n.ID); err != nil {
			return fmt.Errorf("UpdateNode: %w", err)
		}
	}
	s.applyNodeUpsert(stored)
	return nil
}

func (s *Store) UpdateEdge(e *store.Edge) error {
	if len(e.Labels) == 0 {
		return fmt.Errorf("UpdateEdge: edge %d must carry at least one label", e.ID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Endpoints are immutable: load the current edge to preserve Src/Dst.
	cur, ok := s.getEdgeLocked(e.ID)
	if !ok {
		return &store.ErrNotFound{Kind: "edge", ID: uint64(e.ID)}
	}

	stored := &store.Edge{
		ID:     e.ID,
		Src:    cur.Src,
		Dst:    cur.Dst,
		Weight: e.Weight,
	}
	stored.Labels = make([]store.EdgeType, len(e.Labels))
	copy(stored.Labels, e.Labels)
	if len(e.Properties) > 0 {
		stored.Properties = make([]byte, len(e.Properties))
		copy(stored.Properties, e.Properties)
	}

	if err := s.wal.AppendEdge(marshalEdge(stored)); err != nil {
		return fmt.Errorf("UpdateEdge: wal: %w", err)
	}
	if s.reindexPolicy == store.ReindexPurge {
		if err := s.purgeEdgeIndexLocked(e.ID); err != nil {
			return fmt.Errorf("UpdateEdge: %w", err)
		}
	}
	s.applyEdgeUpsert(stored)
	return nil
}

func (s *Store) DeleteEdge(id store.EdgeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.edgeExistsLocked(id) {
		return &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
	}
	if err := s.wal.AppendEdgeDelete(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("DeleteEdge: wal: %w", err)
	}
	s.applyEdgeDelete(id)
	return nil
}

func (s *Store) DeleteNode(id store.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.nodeExistsLocked(id) {
		return &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	incident := s.incidentEdgeIDsLocked(id)

	// Durably record tombstones for the cascaded edges first, then the node, so
	// a crash mid-delete never leaves an edge pointing at a missing node.
	for _, eid := range incident {
		if err := s.wal.AppendEdgeDelete(marshalID(uint64(eid))); err != nil {
			return fmt.Errorf("DeleteNode: wal edge tombstone: %w", err)
		}
	}
	if err := s.wal.AppendNodeDelete(marshalID(uint64(id))); err != nil {
		return fmt.Errorf("DeleteNode: wal node tombstone: %w", err)
	}

	for _, eid := range incident {
		s.applyEdgeDelete(eid)
	}
	s.applyNodeDelete(id)
	return nil
}

// publishCSR makes csr visible to lock-free readers and starts a fresh epoch.
// Caller must hold s.mu.
//
// The shadow counter resets because a freshly built CSR already incorporates
// every update and tombstone that had accumulated against the previous one.
func (s *Store) publishCSR(csr *CSRGraph) {
	s.csr = csr
	// Order matters: publish the pointer *before* clearing the shadow count.
	//
	// Clearing first would open a window in which a reader still holding the old
	// pointer sees a clean count and concludes the old CSR is authoritative —
	// even though the CSR it is about to read from was just superseded, tombstones
	// and all. Publishing first means any reader that trusts a zero count is
	// re-checking against a pointer that has already moved, and bails out.
	s.csrPtr.Store(csr)
	s.csrShadowed.Store(0)
}

// shadowCSRNode records that a CSR-resident node has been superseded by the
// delta layer, disabling the lock-free read path until the next Compact.
// Caller must hold s.mu.
func (s *Store) shadowCSRNode(id store.NodeID) {
	if s.nodeInCSR(id) {
		s.csrShadowed.Add(1)
	}
}

// shadowCSREdge is shadowCSRNode for edges. Caller must hold s.mu.
func (s *Store) shadowCSREdge(id store.EdgeID) {
	if s.edgeInCSR(id) {
		s.csrShadowed.Add(1)
	}
}

// --- in-memory apply helpers (shared by live mutators and WAL replay) ---
// All require s.mu held.

// applyNodeUpsert inserts or updates a node in the delta overlay, reconciling
// the delta type index and clearing any delete mask. A CSR-resident node being
// updated simply gains a delta entry that shadows the CSR copy.
func (s *Store) applyNodeUpsert(n *store.Node) {
	s.shadowCSRNode(n.ID)
	if prev, ok := s.deltaNodes[n.ID]; ok {
		s.unindexDeltaNodeLabels(n.ID, prev.Labels)
	}
	s.deltaNodes[n.ID] = n
	s.indexDeltaNodeLabels(n.ID, n.Labels)
	s.ensureDeltaAdj(n.ID)
	delete(s.deletedNodes, n.ID)
}

// applyEdgeUpsert inserts or updates an edge in the delta overlay. Delta
// adjacency is recorded only for a genuinely new edge (not previously in the
// delta and not present in the CSR, whose adjacency arrays already list it), so
// an updated edge is never double-listed.
func (s *Store) applyEdgeUpsert(e *store.Edge) {
	s.shadowCSREdge(e.ID)
	prev, inDelta := s.deltaEdges[e.ID]
	if inDelta {
		s.unindexDeltaEdgeLabels(e.ID, prev.Labels)
	}
	s.deltaEdges[e.ID] = e
	s.indexDeltaEdgeLabels(e.ID, e.Labels)
	if !inDelta && !s.edgeInCSR(e.ID) {
		s.ensureDeltaAdj(e.Src).out = append(s.ensureDeltaAdj(e.Src).out, e.ID)
		s.ensureDeltaAdj(e.Dst).in = append(s.ensureDeltaAdj(e.Dst).in, e.ID)
	}
	delete(s.deletedEdges, e.ID)
}

// applyNodeDelete removes a node from the delta overlay and masks any CSR copy.
// Incident-edge cascade is performed by the caller (DeleteNode) / by separate
// edge tombstones on replay, so this does not touch edges.
func (s *Store) applyNodeDelete(id store.NodeID) {
	s.shadowCSRNode(id)
	if n, ok := s.deltaNodes[id]; ok {
		s.unindexDeltaNodeLabels(id, n.Labels)
		delete(s.deltaNodes, id)
	}
	delete(s.deltaAdj, id)
	if s.nodeInCSR(id) {
		s.deletedNodes[id] = struct{}{}
	}
	s.propIdx.RemoveNode(id)
}

// applyEdgeDelete removes an edge from the delta overlay and masks any CSR copy.
func (s *Store) applyEdgeDelete(id store.EdgeID) {
	s.shadowCSREdge(id)
	if e, ok := s.deltaEdges[id]; ok {
		s.unindexDeltaEdgeLabels(id, e.Labels)
		if a := s.deltaAdj[e.Src]; a != nil {
			a.out = removeEdgeID(a.out, id)
		}
		if a := s.deltaAdj[e.Dst]; a != nil {
			a.in = removeEdgeID(a.in, id)
		}
		delete(s.deltaEdges, id)
	}
	if s.edgeInCSR(id) {
		s.deletedEdges[id] = struct{}{}
	}
	s.propIdx.RemoveEdge(id)
}

// nodeExistsLocked reports whether the node is live (present in delta or CSR and
// not masked by a tombstone). Caller must hold s.mu.
func (s *Store) nodeExistsLocked(id store.NodeID) bool {
	if _, del := s.deletedNodes[id]; del {
		return false
	}
	if _, ok := s.deltaNodes[id]; ok {
		return true
	}
	return s.nodeInCSR(id)
}

// edgeExistsLocked reports whether the edge is live. Caller must hold s.mu.
func (s *Store) edgeExistsLocked(id store.EdgeID) bool {
	if _, del := s.deletedEdges[id]; del {
		return false
	}
	if _, ok := s.deltaEdges[id]; ok {
		return true
	}
	return s.edgeInCSR(id)
}

// getEdgeLocked returns the authoritative live edge (delta override or CSR copy)
// or (nil, false) if it is missing or masked. Caller must hold s.mu.
func (s *Store) getEdgeLocked(id store.EdgeID) (*store.Edge, bool) {
	if _, del := s.deletedEdges[id]; del {
		return nil, false
	}
	if e, ok := s.deltaEdges[id]; ok {
		return e, true
	}
	if s.csr != nil {
		if rec, found := s.csr.GetEdge(id); found {
			return rawEdgeToStore(rec), true
		}
	}
	return nil, false
}

func (s *Store) nodeInCSR(id store.NodeID) bool {
	if s.csr == nil {
		return false
	}
	_, found := s.csr.GetNode(id)
	return found
}

func (s *Store) edgeInCSR(id store.EdgeID) bool {
	if s.csr == nil {
		return false
	}
	_, found := s.csr.GetEdge(id)
	return found
}

// incidentEdgeIDsLocked returns the deduped, still-live edge IDs incident to id
// (as Src or Dst) gathered from both the delta adjacency and the CSR. Caller
// must hold s.mu.
func (s *Store) incidentEdgeIDsLocked(id store.NodeID) []store.EdgeID {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	add := func(eid store.EdgeID) {
		if _, del := s.deletedEdges[eid]; del {
			return
		}
		if _, ok := seen[eid]; ok {
			return
		}
		seen[eid] = struct{}{}
		out = append(out, eid)
	}
	if a := s.deltaAdj[id]; a != nil {
		for _, eid := range a.out {
			add(eid)
		}
		for _, eid := range a.in {
			add(eid)
		}
	}
	if s.csr != nil {
		if outE, err := s.csr.OutboundEdges(id); err == nil {
			for _, re := range outE {
				add(re.ID)
			}
		}
		if inE, err := s.csr.InboundEdges(id); err == nil {
			for _, re := range inE {
				add(re.ID)
			}
		}
	}
	return out
}

// csrFastRead returns the published CSR when a point read may safely bypass the
// store lock, along with the epoch the caller must re-check afterwards.
//
// It returns nil when no CSR exists or something has already shadowed part of
// it. Callers must confirm with csrFastReadValid *after* reading the record;
// only then is the answer known to have been current throughout.
func (s *Store) csrFastRead() (*CSRGraph, bool) {
	csr := s.csrPtr.Load()
	if csr == nil || s.csrShadowed.Load() != 0 {
		return nil, false
	}
	return csr, true
}

// csrFastReadValid reports whether nothing invalidated csr during the read.
//
// The validity check is on the **pointer itself**, not a separate generation
// counter. An earlier version used a counter and was wrong: `Compact` bumped the
// generation before storing the new pointer, so a reader could sample the new
// generation, load the *old* pointer, see a shadow count that had already been
// cleared, and accept a superseded CSR — tombstoned records and all. Every
// ordering of two independent atomics has some such window, because the reader
// needs the pointer and its validity to agree and they were separate words.
//
// Comparing the pointer removes the second word entirely. It is sound because
// the caller still holds a reference to the CSR it read, so that object cannot
// be collected and its address cannot be reused by a later one while the check
// is running.
func (s *Store) csrFastReadValid(csr *CSRGraph) bool {
	return s.csrShadowed.Load() == 0 && s.csrPtr.Load() == csr
}

func (s *Store) GetNode(id store.NodeID) (*store.Node, error) {
	// Unlocked path first: if the record lives in an unshadowed CSR, the store
	// lock buys nothing — the CSR cannot change under us.
	if csr, ok := s.csrFastRead(); ok {
		if rec, found := csr.GetNode(id); found {
			node := &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: cloneBytes(rec.Properties)}
			if s.csrFastReadValid(csr) {
				return node, nil
			}
		}
	}

	// Hold RLock across the delta + CSR lookup so the CSR pointer read is not
	// racing a concurrent Compact swap.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, del := s.deletedNodes[id]; del {
		return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
	}
	if n, ok := s.deltaNodes[id]; ok {
		return n, nil
	}
	if s.csr != nil {
		rec, found := s.csr.GetNode(id)
		if found {
			return &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: cloneBytes(rec.Properties)}, nil
		}
	}
	return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
}

func (s *Store) GetEdge(id store.EdgeID) (*store.Edge, error) {
	if csr, ok := s.csrFastRead(); ok {
		if rec, found := csr.GetEdge(id); found {
			edge := rawEdgeToStore(rec)
			if s.csrFastReadValid(csr) {
				return edge, nil
			}
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, del := s.deletedEdges[id]; del {
		return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
	}
	if e, ok := s.deltaEdges[id]; ok {
		return e, nil
	}
	if s.csr != nil {
		rec, found := s.csr.GetEdge(id)
		if found {
			return rawEdgeToStore(rec), nil
		}
	}
	return nil, &store.ErrNotFound{Kind: "edge", ID: uint64(id)}
}

func (s *Store) EdgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]*store.Edge, error) {
	var result []*store.Edge

	// Hold the read lock across BOTH the delta and CSR passes: the CSR pointer,
	// its adjacency, and the delete masks must be read from one consistent
	// snapshot, otherwise a concurrent Compact could swap the CSR between reading
	// its adjacency and consulting the (now-cleared) masks — re-emitting a
	// deleted edge.
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect from delta.
	da := s.deltaAdj[id]
	if da != nil {
		var eids []store.EdgeID
		switch dir {
		case store.DirectionOutbound:
			eids = da.out
		case store.DirectionInbound:
			eids = da.in
		case store.DirectionBoth:
			eids = make([]store.EdgeID, 0, len(da.out)+len(da.in))
			eids = append(eids, da.out...)
			eids = append(eids, da.in...)
		}
		for _, eid := range eids {
			e := s.deltaEdges[eid]
			if e == nil {
				continue
			}
			if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, e) {
				continue
			}
			result = append(result, e)
		}
	}

	// Collect from CSR.
	if s.csr != nil {
		var rawEdges []rawEdge
		var err error
		switch dir {
		case store.DirectionOutbound:
			rawEdges, err = s.csr.OutboundEdges(id)
		case store.DirectionInbound:
			rawEdges, err = s.csr.InboundEdges(id)
		case store.DirectionBoth:
			out, e1 := s.csr.OutboundEdges(id)
			in, e2 := s.csr.InboundEdges(id)
			if e1 == nil {
				rawEdges = append(rawEdges, out...)
			}
			if e2 == nil {
				rawEdges = append(rawEdges, in...)
			}
			err = nil
		}
		if err == nil {
			for _, re := range rawEdges {
				if _, del := s.deletedEdges[re.ID]; del {
					continue
				}
				// A CSR edge updated in the delta is emitted from the delta
				// (authoritative) copy; endpoints are immutable so CSR adjacency
				// still lists it correctly.
				if de, ok := s.deltaEdges[re.ID]; ok {
					if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, de) {
						continue
					}
					result = append(result, de)
					continue
				}
				if edgeTypes != nil && !rawEdgeMatchesFilter(edgeTypes, re.Labels) {
					continue
				}
				result = append(result, rawEdgeToStore(re))
			}
		}
	}

	return result, nil
}

// IncidentEdges implements store.AdjacencyReader. It walks the same delta-then-
// CSR sequence as EdgesOf, applying the same tombstone and delta-override rules,
// but appends to the caller's buffer instead of materialising edge records.
func (s *Store) IncidentEdges(dst []store.IncidentEdge, id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.IncidentEdge, error) {
	// One lock hold across both layers, for the same reason EdgesOf does it: a
	// concurrent Compact must not swap the CSR between reading its adjacency and
	// consulting the delete masks.
	s.mu.RLock()
	defer s.mu.RUnlock()

	add := func(eid store.EdgeID, src, dstNode store.NodeID) {
		nb := dstNode
		if src != id {
			nb = src
		}
		dst = append(dst, store.IncidentEdge{Edge: eid, Neighbour: nb})
	}

	// Delta layer.
	if da := s.deltaAdj[id]; da != nil {
		appendDelta := func(eids []store.EdgeID) {
			for _, eid := range eids {
				e := s.deltaEdges[eid]
				if e == nil {
					continue
				}
				if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, e) {
					continue
				}
				add(eid, e.Src, e.Dst)
			}
		}
		switch dir {
		case store.DirectionOutbound:
			appendDelta(da.out)
		case store.DirectionInbound:
			appendDelta(da.in)
		case store.DirectionBoth:
			appendDelta(da.out)
			appendDelta(da.in)
		}
	}

	// CSR layer.
	if s.csr != nil {
		appendCSR := func(eids []store.EdgeID) {
			for _, eid := range eids {
				if _, del := s.deletedEdges[eid]; del {
					continue
				}
				// A CSR edge updated in the delta is authoritative there; its
				// labels may have changed, so filter against the delta copy.
				if de, ok := s.deltaEdges[eid]; ok {
					if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, de) {
						continue
					}
					add(eid, de.Src, de.Dst)
					continue
				}
				rec, found := s.csr.GetEdge(eid)
				if !found {
					continue
				}
				if edgeTypes != nil && !rawEdgeMatchesFilter(edgeTypes, rec.Labels) {
					continue
				}
				add(eid, rec.Src, rec.Dst)
			}
		}
		switch dir {
		case store.DirectionOutbound:
			appendCSR(s.csr.OutboundEdgeIDs(id))
		case store.DirectionInbound:
			appendCSR(s.csr.InboundEdgeIDs(id))
		case store.DirectionBoth:
			appendCSR(s.csr.OutboundEdgeIDs(id))
			appendCSR(s.csr.InboundEdgeIDs(id))
		}
	}

	return dst, nil
}

// NodeExists implements store.AdjacencyReader.
func (s *Store) NodeExists(id store.NodeID) bool {
	if csr, ok := s.csrFastRead(); ok {
		if _, found := csr.GetNode(id); found && s.csrFastReadValid(csr) {
			return true
		}
	}

	s.mu.RLock()
	ok := s.nodeExistsLocked(id)
	s.mu.RUnlock()
	return ok
}

func (s *Store) Neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) ([]store.NeighbourResult, error) {
	edges, err := s.EdgesOf(id, dir, edgeTypes)
	if err != nil {
		return nil, err
	}

	seen := make(map[store.NodeID]struct{})
	var results []store.NeighbourResult

	for _, e := range edges {
		var nbID store.NodeID
		if e.Src == id {
			nbID = e.Dst
		} else {
			nbID = e.Src
		}
		if _, already := seen[nbID]; already {
			continue
		}
		seen[nbID] = struct{}{}
		n, err := s.GetNode(nbID)
		if err != nil {
			continue
		}
		results = append(results, store.NeighbourResult{Node: n, Edge: e})
	}
	return results, nil
}

func (s *Store) NodesByType(t store.NodeType) ([]store.NodeID, error) {
	// Candidate collection and re-validation share one lock hold: taking the read
	// lock once per candidate (via GetNode) dominated the cost on large graphs.
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := make([]store.NodeID, len(s.deltaNodesByType[t]))
	copy(candidates, s.deltaNodesByType[t])
	if s.csr != nil {
		candidates = append(candidates, s.csr.NodesByType(t)...)
	}

	// Re-validate against the authoritative view: a candidate may be masked by a
	// tombstone or have had label t removed/added by an update. Dedup as we go.
	seen := make(map[store.NodeID]struct{}, len(candidates))
	out := make([]store.NodeID, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := seen[id]; ok {
			continue
		}
		if !s.nodeHasLabelLocked(id, t) {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

// nodeHasLabelLocked reports whether the live record for id carries label t,
// without allocating a *store.Node. Caller must hold s.mu.
func (s *Store) nodeHasLabelLocked(id store.NodeID, t store.NodeType) bool {
	if _, del := s.deletedNodes[id]; del {
		return false
	}
	if n, ok := s.deltaNodes[id]; ok {
		return n.HasLabel(t)
	}
	if s.csr == nil {
		return false
	}
	rec, found := s.csr.GetNode(id)
	return found && nodeRecordHasLabel(rec.Labels, t)
}

func (s *Store) EdgesByType(t store.EdgeType) ([]store.EdgeID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := make([]store.EdgeID, len(s.deltaEdgesByType[t]))
	copy(candidates, s.deltaEdgesByType[t])
	if s.csr != nil {
		candidates = append(candidates, s.csr.EdgesByType(t)...)
	}

	seen := make(map[store.EdgeID]struct{}, len(candidates))
	out := make([]store.EdgeID, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := seen[id]; ok {
			continue
		}
		if !s.edgeHasLabelLocked(id, t) {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

// edgeHasLabelLocked reports whether the live record for id carries label t,
// without allocating a *store.Edge. Caller must hold s.mu.
func (s *Store) edgeHasLabelLocked(id store.EdgeID, t store.EdgeType) bool {
	if _, del := s.deletedEdges[id]; del {
		return false
	}
	if e, ok := s.deltaEdges[id]; ok {
		return e.HasLabel(t)
	}
	if s.csr == nil {
		return false
	}
	rec, found := s.csr.GetEdge(id)
	return found && rawEdgeHasLabel(rec.Labels, t)
}

func (s *Store) NodeCount() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Every delta node is live (deleted entries are removed from the map).
	total := uint64(len(s.deltaNodes))
	if s.csr != nil {
		// Count CSR nodes that are neither overridden by a delta entry nor
		// masked by a tombstone.
		for i := 1; i < len(s.csr.nodes); i++ {
			id := s.csr.nodes[i].ID
			if id == store.InvalidNodeID {
				continue
			}
			if _, over := s.deltaNodes[id]; over {
				continue
			}
			if _, del := s.deletedNodes[id]; del {
				continue
			}
			total++
		}
	}
	return total, nil
}

func (s *Store) EdgeCount() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := uint64(len(s.deltaEdges))
	if s.csr != nil {
		for i := 1; i < len(s.csr.edges); i++ {
			id := s.csr.edges[i].ID
			if id == store.InvalidEdgeID {
				continue
			}
			if _, over := s.deltaEdges[id]; over {
				continue
			}
			if _, del := s.deletedEdges[id]; del {
				continue
			}
			total++
		}
	}
	return total, nil
}

func (s *Store) Close() error {
	return s.wal.Close()
}

func (s *Store) IndexNodeProperty(id store.NodeID, key string, value []byte) error {
	s.propIdx.IndexNode(id, key, value)
	payload := marshalNodeProp(id, key, value)
	if err := s.wal.AppendNodeProp(payload); err != nil {
		return fmt.Errorf("IndexNodeProperty: wal: %w", err)
	}
	return nil
}

func (s *Store) IndexEdgeProperty(id store.EdgeID, key string, value []byte) error {
	s.propIdx.IndexEdge(id, key, value)
	payload := marshalEdgeProp(id, key, value)
	if err := s.wal.AppendEdgeProp(payload); err != nil {
		return fmt.Errorf("IndexEdgeProperty: wal: %w", err)
	}
	return nil
}

// NodesByProperty returns the nodes indexed under key with exactly value.
//
// The postings are resolved against the records before being returned. Without
// that step a caller can observe a deletion mid-flight: DeleteNode journals its
// tombstones and applies them under one write lock, but this path only locks the
// index, so it can read postings that the delete has not reached yet and hand
// back an entity the records no longer have. Filtering makes the records the
// authority, so every ID returned resolved to a live node at the instant it was
// checked.
//
// That is the guarantee, and it is deliberately not stronger: the node may be
// deleted the moment this returns. Ruling that out needs snapshot isolation,
// which the store does not offer.
func (s *Store) NodesByProperty(key string, value []byte) ([]store.NodeID, error) {
	return s.liveNodeIDs(s.propIdx.NodesByProperty(key, value)), nil
}

// EdgesByProperty returns the edges indexed under key with exactly value. It
// resolves postings against the records for the reason given on NodesByProperty.
func (s *Store) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error) {
	return s.liveEdgeIDs(s.propIdx.EdgesByProperty(key, value)), nil
}

func (s *Store) QueryNodeIDs(query store.NodeQuery) ([]store.NodeID, error) {
	candidates, sortedAsc, plan := s.driveNodeCandidates(query)

	if len(query.Types) > 0 {
		typeSet := make(map[store.NodeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		filtered := make([]store.NodeID, 0, len(candidates))
		for _, id := range candidates {
			n, err := s.GetNode(id)
			if err != nil {
				continue
			}
			if nodeHasAnyType(n, typeSet) {
				filtered = append(filtered, id)
			}
		}
		candidates = filtered
	}

	if len(query.Filters) > 0 {
		// Both sides must be ascending for the merge. The driving set often
		// already is; when it is not, sorting it once here is repaid immediately
		// because the merge output is ascending too, which retires the sort below.
		if !sortedAsc {
			candidates = store.SortDedupeIDs(candidates)
			sortedAsc = true
		}
		if store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
			// Every filter's own set contains the answer, so the residual pass
			// can narrow the candidates directly and skip the driving filter
			// entirely rather than re-deriving a set it was already built from.
			candidates = s.propIdx.NarrowNodesByFilters(candidates, query.Filters, plan.DriverFilter)
		} else {
			matched := s.matchNodeIDsByFilters(query.Filters, store.MatchAny)
			candidates = store.IntersectSortedIDs(candidates, matched)
		}
	}

	order := store.NormalizedQueryOrder(query.Order)
	// An ascending candidate set needs no sort at all, and only a linear reverse
	// to satisfy a descending query.
	switch {
	case sortedAsc && order == store.QueryOrderAsc:
		// already in the requested order
	case sortedAsc:
		store.ReverseIDs(candidates)
	default:
		sort.Slice(candidates, func(i, j int) bool {
			if order == store.QueryOrderDesc {
				return candidates[i] > candidates[j]
			}
			return candidates[i] < candidates[j]
		})
	}
	return store.ApplyNodeQueryWindow(candidates, query.Offset, query.Limit), nil
}

func (s *Store) QueryEdgeIDs(query store.EdgeQuery) ([]store.EdgeID, error) {
	candidates, sortedAsc, plan := s.driveEdgeCandidates(query)

	if len(query.Types) > 0 || len(query.SrcIDs) > 0 || len(query.DstIDs) > 0 {
		typeSet := make(map[store.EdgeType]struct{}, len(query.Types))
		for _, t := range query.Types {
			typeSet[t] = struct{}{}
		}
		srcSet := makeNodeIDSet(query.SrcIDs)
		dstSet := makeNodeIDSet(query.DstIDs)

		filtered := make([]store.EdgeID, 0, len(candidates))
		for _, id := range candidates {
			e, err := s.GetEdge(id)
			if err != nil {
				continue
			}
			if len(typeSet) > 0 && !edgeHasAnyType(e, typeSet) {
				continue
			}
			if len(srcSet) > 0 {
				if _, ok := srcSet[e.Src]; !ok {
					continue
				}
			}
			if len(dstSet) > 0 {
				if _, ok := dstSet[e.Dst]; !ok {
					continue
				}
			}
			filtered = append(filtered, id)
		}
		candidates = filtered
	}

	if len(query.Filters) > 0 {
		if !sortedAsc {
			candidates = store.SortDedupeIDs(candidates)
			sortedAsc = true
		}
		if store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
			candidates = s.propIdx.NarrowEdgesByFilters(candidates, query.Filters, plan.DriverFilter)
		} else {
			matched := s.matchEdgeIDsByFilters(query.Filters, store.MatchAny)
			candidates = store.IntersectSortedIDs(candidates, matched)
		}
	}

	order := store.NormalizedQueryOrder(query.Order)
	switch {
	case sortedAsc && order == store.QueryOrderAsc:
		// already in the requested order
	case sortedAsc:
		store.ReverseIDs(candidates)
	default:
		sort.Slice(candidates, func(i, j int) bool {
			if order == store.QueryOrderDesc {
				return candidates[i] > candidates[j]
			}
			return candidates[i] < candidates[j]
		})
	}
	return store.ApplyEdgeQueryWindow(candidates, query.Offset, query.Limit), nil
}

// VerifyIndexes implements store.IndexVerifier. It cross-checks the CSR label
// postings, the CSR adjacency arrays, the delta label postings, and the property
// index against the live records, returning the first inconsistency found.
func (s *Store) VerifyIndexes() error {
	if err := s.propIdx.Verify(); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.csr != nil {
		if err := s.csr.verifyLabelIndex(); err != nil {
			return err
		}
		if err := s.csr.verifyAdjacency(); err != nil {
			return err
		}
	}

	// Delta label postings must describe live delta records. Membership sets are
	// built once and reused for the reverse direction; scanning the postings per
	// record would make verification quadratic in the size of the largest label.
	deltaNodeMembers := make(map[store.NodeType]map[store.NodeID]struct{}, len(s.deltaNodesByType))
	for lbl, ids := range s.deltaNodesByType {
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("delta node label index: %v postings are not strictly ascending", lbl)
		}
		seen := make(map[store.NodeID]struct{}, len(ids))
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("delta node label index: %v lists node %d twice", lbl, id)
			}
			seen[id] = struct{}{}
			n, ok := s.deltaNodes[id]
			if !ok {
				return fmt.Errorf("delta node label index: %v lists node %d, which is not in the delta", lbl, id)
			}
			if !n.HasLabel(lbl) {
				return fmt.Errorf("delta node label index: %v lists node %d, which does not carry that label", lbl, id)
			}
		}
		deltaNodeMembers[lbl] = seen
	}
	for id, n := range s.deltaNodes {
		for _, lbl := range n.Labels {
			if _, ok := deltaNodeMembers[lbl][id]; !ok {
				return fmt.Errorf("delta node label index: node %d carries %v but is missing from the postings", id, lbl)
			}
		}
	}

	deltaEdgeMembers := make(map[store.EdgeType]map[store.EdgeID]struct{}, len(s.deltaEdgesByType))
	for lbl, ids := range s.deltaEdgesByType {
		if !store.IsSortedIDs(ids) {
			return fmt.Errorf("delta edge label index: %v postings are not strictly ascending", lbl)
		}
		seen := make(map[store.EdgeID]struct{}, len(ids))
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("delta edge label index: %v lists edge %d twice", lbl, id)
			}
			seen[id] = struct{}{}
			e, ok := s.deltaEdges[id]
			if !ok {
				return fmt.Errorf("delta edge label index: %v lists edge %d, which is not in the delta", lbl, id)
			}
			if !e.HasLabel(lbl) {
				return fmt.Errorf("delta edge label index: %v lists edge %d, which does not carry that label", lbl, id)
			}
		}
		deltaEdgeMembers[lbl] = seen
	}
	for id, e := range s.deltaEdges {
		for _, lbl := range e.Labels {
			if _, ok := deltaEdgeMembers[lbl][id]; !ok {
				return fmt.Errorf("delta edge label index: edge %d carries %v but is missing from the postings", id, lbl)
			}
		}
	}

	// A delta entry and a tombstone for the same ID are mutually exclusive.
	for id := range s.deltaNodes {
		if _, del := s.deletedNodes[id]; del {
			return fmt.Errorf("node %d is both present in the delta and masked as deleted", id)
		}
	}
	for id := range s.deltaEdges {
		if _, del := s.deletedEdges[id]; del {
			return fmt.Errorf("edge %d is both present in the delta and masked as deleted", id)
		}
	}

	// Delta adjacency must agree with the delta edges' endpoints.
	for nodeID, a := range s.deltaAdj {
		for _, eid := range a.out {
			e, ok := s.deltaEdges[eid]
			if !ok {
				return fmt.Errorf("delta adjacency: node %d lists outbound edge %d, which is not in the delta", nodeID, eid)
			}
			if e.Src != nodeID {
				return fmt.Errorf("delta adjacency: node %d lists outbound edge %d, whose Src is %d", nodeID, eid, e.Src)
			}
		}
		for _, eid := range a.in {
			e, ok := s.deltaEdges[eid]
			if !ok {
				return fmt.Errorf("delta adjacency: node %d lists inbound edge %d, which is not in the delta", nodeID, eid)
			}
			if e.Dst != nodeID {
				return fmt.Errorf("delta adjacency: node %d lists inbound edge %d, whose Dst is %d", nodeID, eid, e.Dst)
			}
		}
	}

	// The property index must not outlive the entities it describes.
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if !s.nodeExistsLocked(id) {
			return fmt.Errorf("property index: node %d has entries but is not live", id)
		}
	}
	for _, id := range s.propIdx.IndexedEdgeIDs() {
		if !s.edgeExistsLocked(id) {
			return fmt.Errorf("property index: edge %d has entries but is not live", id)
		}
	}
	return nil
}

// RebuildIndexes implements store.IndexRebuilder. It recomputes the CSR label
// postings, the delta label postings, and the delta adjacency from the records
// they describe, then drops property-index entries whose entity is not live.
//
// Everything it touches is derived state held in memory, so it writes nothing to
// disk and needs no WAL records. Use it after recovering a store whose indexes
// may not match its records; a following Compact persists the repaired state.
func (s *Store) RebuildIndexes() error {
	s.mu.Lock()

	if s.csr != nil {
		s.csr.buildLabelIndex()
	}

	s.deltaNodesByType = make(map[store.NodeType][]store.NodeID, len(s.deltaNodesByType))
	s.deltaEdgesByType = make(map[store.EdgeType][]store.EdgeID, len(s.deltaEdgesByType))
	s.deltaAdj = make(map[store.NodeID]*deltaAdj, len(s.deltaAdj))

	for id, n := range s.deltaNodes {
		s.indexDeltaNodeLabels(id, n.Labels)
		s.ensureDeltaAdj(id)
	}
	for id, e := range s.deltaEdges {
		s.indexDeltaEdgeLabels(id, e.Labels)
		// Mirrors applyEdgeUpsert: delta adjacency lists only edges the CSR
		// adjacency does not already cover, so the two never double-count.
		if !s.edgeInCSR(id) {
			s.ensureDeltaAdj(e.Src).out = append(s.ensureDeltaAdj(e.Src).out, id)
			s.ensureDeltaAdj(e.Dst).in = append(s.ensureDeltaAdj(e.Dst).in, id)
		}
	}

	var deadNodes []store.NodeID
	for _, id := range s.propIdx.IndexedNodeIDs() {
		if !s.nodeExistsLocked(id) {
			deadNodes = append(deadNodes, id)
		}
	}
	var deadEdges []store.EdgeID
	for _, id := range s.propIdx.IndexedEdgeIDs() {
		if !s.edgeExistsLocked(id) {
			deadEdges = append(deadEdges, id)
		}
	}
	s.mu.Unlock()

	for _, id := range deadNodes {
		s.propIdx.RemoveNode(id)
	}
	for _, id := range deadEdges {
		s.propIdx.RemoveEdge(id)
	}
	return nil
}

// sortDedupeNodeIDs sorts ids ascending and removes duplicates, in place.
//
// The ordered index emits IDs in value order, and an entity registered under two
// values for the same key appears once per value, so both properties have to be
// restored before the result can be used as a driving set.
func sortDedupeNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) < 2 {
		return ids
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// sortDedupeEdgeIDs is sortDedupeNodeIDs for edge IDs.
func sortDedupeEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) < 2 {
		return ids
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// --- query planning ---
//
// Queries used to start from a full enumeration of the delta layer plus every
// live CSR record. The drive* helpers instead pick the cheapest index that is
// still a guaranteed superset of the result, leaving the filter stages (and
// therefore the results) unchanged.

// driveNodeCandidates returns the starting candidate set for a node query and
// whether it is already in ascending ID order.
// driveNodeCandidates picks the cheapest source that is guaranteed to contain
// the query's answer. It returns the candidates, whether they are ascending, and
// a plan describing that choice — including the filter it consumed, so the
// residual pass does not evaluate that filter a second time.
func (s *Store) driveNodeCandidates(query store.NodeQuery) ([]store.NodeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		return s.collectCandidateNodeIDs(query.IDs), false, store.QueryPlan{Driver: store.DriverIDs, DriverFilter: -1}
	}

	// Most selective equality filter, if any qualifies as a driver. Its
	// cardinality is known exactly and is almost always far below the graph size.
	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := s.propIdx.NodeCardinality(f.Key, f.Value)
		if bestFilter == nil || size < bestSize {
			filter := f
			bestFilter = &filter
			bestSize = size
		}
	}
	if bestFilter != nil {
		return s.liveNodeIDs(s.propIdx.NodesByProperty(bestFilter.Key, bestFilter.Value)), true, store.QueryPlan{
			Driver:       store.DriverEquality,
			DriverKey:    bestFilter.Key,
			DriverFilter: store.FilterIndexOf(query.Filters, *bestFilter),
		}
	}

	// A range or prefix filter on a key declared ordered bounds the result too,
	// and resolving it here means the query never enumerates the whole graph.
	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		if ids, served := s.propIdx.NodesMatchingOrdered(nil, f); served {
			return s.liveNodeIDs(sortDedupeNodeIDs(ids)), true, store.QueryPlan{
				Driver:       store.DriverOrdered,
				DriverKey:    f.Key,
				DriverFilter: store.FilterIndexOf(query.Filters, f),
			}
		}
	}

	// Label lookup still scans the CSR (there is no persisted label index yet),
	// but it filters during the scan and resolves only the matches, which beats
	// enumerating every node and resolving each one.
	if len(query.Types) > 0 {
		seen := make(map[store.NodeID]struct{})
		var out []store.NodeID
		for _, t := range query.Types {
			ids, err := s.NodesByType(t)
			if err != nil {
				continue
			}
			for _, id := range ids {
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out, false, store.QueryPlan{Driver: store.DriverLabels, DriverFilter: -1}
	}

	return s.collectCandidateNodeIDs(nil), false, store.QueryPlan{Driver: store.DriverScan, DriverFilter: -1}
}

// liveNodeIDs drops IDs that no longer resolve to a live node, preserving order.
func (s *Store) liveNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) == 0 {
		return ids // a miss should not pay for the lock
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := ids[:0]
	for _, id := range ids {
		if s.nodeExistsLocked(id) {
			out = append(out, id)
		}
	}
	return out
}

// liveEdgeIDs drops IDs that no longer resolve to a live edge, preserving order.
func (s *Store) liveEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) == 0 {
		return ids // a miss should not pay for the lock
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := ids[:0]
	for _, id := range ids {
		if s.edgeExistsLocked(id) {
			out = append(out, id)
		}
	}
	return out
}

// driveEdgeCandidates returns the starting candidate set for an edge query and
// whether it is already in ascending ID order.
// driveEdgeCandidates mirrors driveNodeCandidates: it returns the candidates,
// whether they are ascending, and a plan naming the source it drove from and the
// filter it consumed, so the residual pass does not re-evaluate that filter.
func (s *Store) driveEdgeCandidates(query store.EdgeQuery) ([]store.EdgeID, bool, store.QueryPlan) {
	if len(query.IDs) > 0 {
		return s.collectCandidateEdgeIDs(query.IDs), false, store.QueryPlan{Driver: store.DriverIDs, DriverFilter: -1}
	}

	var bestFilter *store.PropertyFilter
	bestSize := 0
	for _, f := range store.EqualityDrivers(query.Filters, query.FilterMode) {
		size := s.propIdx.EdgeCardinality(f.Key, f.Value)
		if bestFilter == nil || size < bestSize {
			filter := f
			bestFilter = &filter
			bestSize = size
		}
	}

	// Anchored queries are bounded by the anchors' degree, which the CSR offset
	// arrays give us without touching a single edge record.
	anchorDir := store.DirectionOutbound
	anchors := query.SrcIDs
	anchorSize := -1
	if len(query.SrcIDs) > 0 {
		s.mu.RLock()
		anchorSize = s.degreeSumLocked(query.SrcIDs, store.DirectionOutbound)
		s.mu.RUnlock()
	}
	if len(query.DstIDs) > 0 {
		s.mu.RLock()
		dstSize := s.degreeSumLocked(query.DstIDs, store.DirectionInbound)
		s.mu.RUnlock()
		if anchorSize < 0 || dstSize < anchorSize {
			anchorSize = dstSize
			anchorDir = store.DirectionInbound
			anchors = query.DstIDs
		}
	}

	switch {
	case bestFilter != nil && (anchorSize < 0 || bestSize <= anchorSize):
		return s.liveEdgeIDs(s.propIdx.EdgesByProperty(bestFilter.Key, bestFilter.Value)), true, store.QueryPlan{
			Driver:       store.DriverEquality,
			DriverKey:    bestFilter.Key,
			DriverFilter: store.FilterIndexOf(query.Filters, *bestFilter),
		}
	case anchorSize >= 0:
		return s.incidentEdgeIDs(anchors, anchorDir), false, store.QueryPlan{
			Driver: store.DriverAdjacency, DriverFilter: -1,
		}
	}

	for _, f := range store.OrderedDrivers(query.Filters, query.FilterMode) {
		if ids, served := s.propIdx.EdgesMatchingOrdered(nil, f); served {
			return s.liveEdgeIDs(sortDedupeEdgeIDs(ids)), true, store.QueryPlan{
				Driver:       store.DriverOrdered,
				DriverKey:    f.Key,
				DriverFilter: store.FilterIndexOf(query.Filters, f),
			}
		}
	}

	if len(query.Types) > 0 {
		seen := make(map[store.EdgeID]struct{})
		var out []store.EdgeID
		for _, t := range query.Types {
			ids, err := s.EdgesByType(t)
			if err != nil {
				continue
			}
			for _, id := range ids {
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out, false, store.QueryPlan{Driver: store.DriverLabels, DriverFilter: -1}
	}

	return s.collectCandidateEdgeIDs(nil), false, store.QueryPlan{Driver: store.DriverScan, DriverFilter: -1}
}

// degreeSumLocked totals the live incident-edge counts for ids across the delta
// overlay and the CSR. Caller must hold s.mu.
func (s *Store) degreeSumLocked(ids []store.NodeID, dir store.Direction) int {
	total := 0
	for _, id := range ids {
		total += s.degreeLocked(id, dir)
	}
	return total
}

// degreeLocked counts live incident edges for one node. Caller must hold s.mu.
//
// Delta adjacency and CSR adjacency never list the same edge (applyEdgeUpsert
// records delta adjacency only for edges absent from the CSR), so the two counts
// simply add.
func (s *Store) degreeLocked(id store.NodeID, dir store.Direction) int {
	total := 0
	if a := s.deltaAdj[id]; a != nil {
		switch dir {
		case store.DirectionOutbound:
			total += len(a.out)
		case store.DirectionInbound:
			total += len(a.in)
		default:
			total += len(a.out) + len(a.in)
		}
	}
	if s.csr == nil {
		return total
	}
	countCSR := func(eids []store.EdgeID) {
		if len(s.deletedEdges) == 0 {
			total += len(eids)
			return
		}
		for _, eid := range eids {
			if _, del := s.deletedEdges[eid]; !del {
				total++
			}
		}
	}
	switch dir {
	case store.DirectionOutbound:
		countCSR(s.csr.OutboundEdgeIDs(id))
	case store.DirectionInbound:
		countCSR(s.csr.InboundEdgeIDs(id))
	default:
		countCSR(s.csr.OutboundEdgeIDs(id))
		countCSR(s.csr.InboundEdgeIDs(id))
	}
	return total
}

// incidentEdgeIDs returns the deduplicated live edge IDs incident to ids in dir.
func (s *Store) incidentEdgeIDs(ids []store.NodeID, dir store.Direction) []store.EdgeID {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	add := func(eids []store.EdgeID) {
		for _, eid := range eids {
			if _, ok := seen[eid]; ok {
				continue
			}
			if !s.edgeExistsLocked(eid) {
				continue
			}
			seen[eid] = struct{}{}
			out = append(out, eid)
		}
	}
	for _, id := range ids {
		if a := s.deltaAdj[id]; a != nil {
			switch dir {
			case store.DirectionOutbound:
				add(a.out)
			case store.DirectionInbound:
				add(a.in)
			default:
				add(a.out)
				add(a.in)
			}
		}
		if s.csr == nil {
			continue
		}
		switch dir {
		case store.DirectionOutbound:
			add(s.csr.OutboundEdgeIDs(id))
		case store.DirectionInbound:
			add(s.csr.InboundEdgeIDs(id))
		default:
			add(s.csr.OutboundEdgeIDs(id))
			add(s.csr.InboundEdgeIDs(id))
		}
	}
	return out
}

// DegreeOf implements store.DegreeCounter. With no edge-type filter it answers
// from the CSR offset arrays and the delta adjacency lists without materialising
// a single edge record.
func (s *Store) DegreeOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) (int, error) {
	// Deliberately mirrors EdgesOf, which reports an unknown node as an empty
	// adjacency rather than an error on this backend.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if edgeTypes == nil {
		return s.degreeLocked(id, dir), nil
	}
	return s.degreeFilteredLocked(id, dir, edgeTypes), nil
}

// degreeFilteredLocked counts incident edges carrying one of edgeTypes, without
// materialising any of them.
//
// The untyped path reads offsets and is O(1); this one has to inspect each
// incident edge's labels, so it is O(degree) — but O(degree) reads, not
// O(degree) allocations. Routing this through EdgesOf instead, as it used to,
// built a *store.Edge and cloned a property blob per incident edge just to take
// len() of the result: 73.8 µs against 14.22 ns for the same node untyped.
//
// The order of checks mirrors EdgesOf exactly — delta layer first, then CSR with
// tombstones skipped and delta overrides taking precedence — so the count always
// equals len(EdgesOf(...)).
// Caller must hold s.mu.
func (s *Store) degreeFilteredLocked(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) int {
	total := 0

	if da := s.deltaAdj[id]; da != nil {
		countDelta := func(eids []store.EdgeID) {
			for _, eid := range eids {
				e := s.deltaEdges[eid]
				if e == nil {
					continue
				}
				if storeEdgeMatchesFilter(edgeTypes, e) {
					total++
				}
			}
		}
		switch dir {
		case store.DirectionOutbound:
			countDelta(da.out)
		case store.DirectionInbound:
			countDelta(da.in)
		default:
			countDelta(da.out)
			countDelta(da.in)
		}
	}

	if s.csr == nil {
		return total
	}
	// On a compacted store with no pending deletions — the steady state for a
	// read-heavy workload — no CSR edge can be masked or overridden, so the two
	// map probes per edge are pure overhead and are skipped entirely.
	pristine := len(s.deletedEdges) == 0 && len(s.deltaEdges) == 0
	countCSR := func(eids []store.EdgeID) {
		if pristine {
			for _, eid := range eids {
				if rec, found := s.csr.GetEdge(eid); found && rawEdgeMatchesFilter(edgeTypes, rec.Labels) {
					total++
				}
			}
			return
		}
		for _, eid := range eids {
			if _, del := s.deletedEdges[eid]; del {
				continue
			}
			// A CSR edge updated in the delta is authoritative there, and its
			// labels may have changed.
			if de, ok := s.deltaEdges[eid]; ok {
				if storeEdgeMatchesFilter(edgeTypes, de) {
					total++
				}
				continue
			}
			rec, found := s.csr.GetEdge(eid)
			if found && rawEdgeMatchesFilter(edgeTypes, rec.Labels) {
				total++
			}
		}
	}
	switch dir {
	case store.DirectionOutbound:
		countCSR(s.csr.OutboundEdgeIDs(id))
	case store.DirectionInbound:
		countCSR(s.csr.InboundEdgeIDs(id))
	default:
		countCSR(s.csr.OutboundEdgeIDs(id))
		countCSR(s.csr.InboundEdgeIDs(id))
	}
	return total
}

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

// --- internals ---

// indexDeltaNodeLabels adds id to the delta postings for each distinct label.
//
// Labels are deduplicated here because a caller may pass the same label twice;
// without this the postings would list id once per repetition, yielding
// duplicate query results. The record's own Labels slice is left untouched.
// Caller must hold s.mu.
func (s *Store) indexDeltaNodeLabels(id store.NodeID, labels []store.NodeType) {
	for i, lbl := range labels {
		if containsNodeTypeValue(labels[:i], lbl) {
			continue
		}
		ids := s.deltaNodesByType[lbl]
		if n := len(ids); n == 0 || ids[n-1] < id {
			s.deltaNodesByType[lbl] = append(ids, id)
			continue
		}
		if updated, added := store.InsertSortedID(ids, id); added {
			s.deltaNodesByType[lbl] = updated
		}
	}
}

// unindexDeltaNodeLabels removes id from the delta postings for each of its
// labels. Caller must hold s.mu.
func (s *Store) unindexDeltaNodeLabels(id store.NodeID, labels []store.NodeType) {
	for _, lbl := range labels {
		ids, removed := store.DeleteSortedID(s.deltaNodesByType[lbl], id)
		if !removed {
			continue
		}
		if len(ids) == 0 {
			delete(s.deltaNodesByType, lbl)
			continue
		}
		s.deltaNodesByType[lbl] = ids
	}
}

// indexDeltaEdgeLabels adds id to the delta postings for each distinct label.
// Caller must hold s.mu.
func (s *Store) indexDeltaEdgeLabels(id store.EdgeID, labels []store.EdgeType) {
	for i, lbl := range labels {
		if containsEdgeTypeValue(labels[:i], lbl) {
			continue
		}
		ids := s.deltaEdgesByType[lbl]
		if n := len(ids); n == 0 || ids[n-1] < id {
			s.deltaEdgesByType[lbl] = append(ids, id)
			continue
		}
		if updated, added := store.InsertSortedID(ids, id); added {
			s.deltaEdgesByType[lbl] = updated
		}
	}
}

// unindexDeltaEdgeLabels removes id from the delta postings for each of its
// labels. Caller must hold s.mu.
func (s *Store) unindexDeltaEdgeLabels(id store.EdgeID, labels []store.EdgeType) {
	for _, lbl := range labels {
		ids, removed := store.DeleteSortedID(s.deltaEdgesByType[lbl], id)
		if !removed {
			continue
		}
		if len(ids) == 0 {
			delete(s.deltaEdgesByType, lbl)
			continue
		}
		s.deltaEdgesByType[lbl] = ids
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

func (s *Store) ensureDeltaAdj(id store.NodeID) *deltaAdj {
	a, ok := s.deltaAdj[id]
	if !ok {
		a = &deltaAdj{}
		s.deltaAdj[id] = a
	}
	return a
}

// commitNodesBatch applies node records to in-memory delta/index state.
// Caller must hold s.mu.
func (s *Store) commitNodesBatch(nodes []*store.Node) {
	for _, n := range nodes {
		s.deltaNodes[n.ID] = n
		s.indexDeltaNodeLabels(n.ID, n.Labels)
		s.ensureDeltaAdj(n.ID)
	}
}

// commitEdgesBatch applies edge records to in-memory delta/index state.
// Caller must hold s.mu.
func (s *Store) commitEdgesBatch(edges []*store.Edge) {
	for _, e := range edges {
		s.deltaEdges[e.ID] = e
		s.indexDeltaEdgeLabels(e.ID, e.Labels)
		s.ensureDeltaAdj(e.Src).out = append(s.ensureDeltaAdj(e.Src).out, e.ID)
		s.ensureDeltaAdj(e.Dst).in = append(s.ensureDeltaAdj(e.Dst).in, e.ID)
	}
}

func (s *Store) replayWAL() error {
	return s.wal.Replay(ReplayCallbacks{
		NodeFunc: func(payload []byte) error {
			n, err := unmarshalNode(payload)
			if err != nil {
				return err
			}
			// Upsert: a re-appended record for an existing ID is an edit.
			s.applyNodeUpsert(n)
			if uint64(n.ID) > s.nodeSeq.Load() {
				s.nodeSeq.Store(uint64(n.ID))
			}
			return nil
		},
		EdgeFunc: func(payload []byte) error {
			e, err := unmarshalEdge(payload)
			if err != nil {
				return err
			}
			s.applyEdgeUpsert(e)
			if uint64(e.ID) > s.edgeSeq.Load() {
				s.edgeSeq.Store(uint64(e.ID))
			}
			return nil
		},
		NodeDeleteFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.applyNodeDelete(store.NodeID(id))
			if id > s.nodeSeq.Load() {
				s.nodeSeq.Store(id)
			}
			return nil
		},
		EdgeDeleteFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.applyEdgeDelete(store.EdgeID(id))
			if id > s.edgeSeq.Load() {
				s.edgeSeq.Store(id)
			}
			return nil
		},
		NodePropFunc: func(payload []byte) error {
			id, key, value, err := unmarshalNodeProp(payload)
			if err != nil {
				return err
			}
			s.propIdx.IndexNode(id, key, value)
			return nil
		},
		EdgePropFunc: func(payload []byte) error {
			id, key, value, err := unmarshalEdgeProp(payload)
			if err != nil {
				return err
			}
			s.propIdx.IndexEdge(id, key, value)
			return nil
		},
		NodePropPurgeFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.propIdx.RemoveNode(store.NodeID(id))
			return nil
		},
		EdgePropPurgeFunc: func(payload []byte) error {
			id, err := unmarshalID(payload)
			if err != nil {
				return err
			}
			s.propIdx.RemoveEdge(store.EdgeID(id))
			return nil
		},
	})
}

func (s *Store) loadCSR(path string) error {
	// For this first implementation, we load the CSR into memory from the
	// serialised file. A future enhancement can mmap this file directly.
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	csr, section, err := deserialiseCSR(data)
	if err != nil {
		return err
	}
	s.publishCSR(csr)

	// Load the persisted property index (v6+). This happens before WAL replay so
	// that post-compaction WAL records — including purges — apply on top of it.
	// A pre-v6 file carries no section; its entries come from the WAL as before,
	// and the next Compact writes them into the CSR.
	if section != nil {
		// Deliberately per-entry. Bulk loading was built and measured here — one
		// lock per shard, parallel fill, presized reverse map, batch-local value
		// interning — and it cut allocations 9-19% but cost 35-75% more resident
		// memory, because partitioning copies every entry into per-shard slices
		// and the presize is keyed on entry count rather than entity count. Index
		// memory is already the project's largest regression, so spending more of
		// it to save allocations inverts the priority order. See plan.md.
		for _, e := range section.NodeProps {
			s.propIdx.IndexNode(e.ID, e.Key, e.Value)
		}
		for _, e := range section.EdgeProps {
			s.propIdx.IndexEdge(e.ID, e.Key, e.Value)
		}
	}
	// Advance sequence counters past existing CSR IDs.
	for i := len(csr.nodes) - 1; i >= 1; i-- {
		if csr.nodes[i].ID != store.InvalidNodeID {
			if uint64(csr.nodes[i].ID) > s.nodeSeq.Load() {
				s.nodeSeq.Store(uint64(csr.nodes[i].ID))
			}
			break
		}
	}
	for i := len(csr.edges) - 1; i >= 1; i-- {
		if csr.edges[i].ID != store.InvalidEdgeID {
			if uint64(csr.edges[i].ID) > s.edgeSeq.Load() {
				s.edgeSeq.Store(uint64(csr.edges[i].ID))
			}
			break
		}
	}
	// Honour the persisted high-water marks so IDs are never reused even when the
	// record that held the max ID was deleted before this CSR was written.
	if csr.nodeSeqHW > s.nodeSeq.Load() {
		s.nodeSeq.Store(csr.nodeSeqHW)
	}
	if csr.edgeSeqHW > s.edgeSeq.Load() {
		s.edgeSeq.Store(csr.edgeSeqHW)
	}
	return nil
}

// --- serialisation helpers ---

// marshalNode encodes a Node: id(8) labelCount(1) labels(2*N) propLen(4) props(n)
func marshalNode(n *store.Node) []byte {
	labelCount := len(n.Labels)
	propLen := len(n.Properties)
	labelsBytes := currentLabelBytesPerValue * labelCount
	// 8 (id) + 1 (labelCount) + 2*N (labels) + 4 (propLen) + propLen
	buf := make([]byte, nodePayloadLabelStart+labelsBytes+nodePayloadPropLenBytes+propLen)
	binary.LittleEndian.PutUint64(buf[0:nodePayloadIDBytes], uint64(n.ID))
	buf[nodePayloadIDBytes] = byte(labelCount)
	for i, lbl := range n.Labels {
		base := nodePayloadLabelStart + (currentLabelBytesPerValue * i)
		binary.LittleEndian.PutUint16(buf[base:base+2], uint16(lbl))
	}
	base := nodePayloadLabelStart + labelsBytes
	binary.LittleEndian.PutUint32(buf[base:base+4], uint32(propLen))
	if propLen > 0 {
		copy(buf[base+4:], n.Properties)
	}
	return buf
}

func unmarshalNode(b []byte) (*store.Node, error) {
	if len(b) < nodePayloadLabelStart {
		return nil, fmt.Errorf("unmarshalNode: payload too short (%d bytes)", len(b))
	}
	id := store.NodeID(binary.LittleEndian.Uint64(b[0:nodePayloadIDBytes]))
	labelCount := int(b[nodePayloadIDBytes])

	newBase := nodePayloadLabelStart + (currentLabelBytesPerValue * labelCount)
	oldBase := nodePayloadLabelStart + (legacyLabelBytesPerValue * labelCount)

	decodeV2 := false
	decodeV1 := false

	if len(b) >= newBase+4 {
		propLenNew := int(binary.LittleEndian.Uint32(b[newBase : newBase+4]))
		if newBase+4+propLenNew == len(b) {
			decodeV2 = true
		}
	}
	if !decodeV2 && len(b) >= oldBase+4 {
		propLenOld := int(binary.LittleEndian.Uint32(b[oldBase : oldBase+4]))
		if oldBase+4+propLenOld == len(b) {
			decodeV1 = true
		}
	}
	if !decodeV2 && !decodeV1 {
		return nil, fmt.Errorf("unmarshalNode: payload truncated (labels)")
	}

	labels := make([]store.NodeType, labelCount)
	base := oldBase
	if decodeV2 {
		for i := 0; i < labelCount; i++ {
			lb := nodePayloadLabelStart + (currentLabelBytesPerValue * i)
			labels[i] = store.NodeType(binary.LittleEndian.Uint16(b[lb : lb+2]))
		}
		base = newBase
	} else {
		for i := 0; i < labelCount; i++ {
			labels[i] = store.NodeType(b[nodePayloadLabelStart+i])
		}
	}

	propLen := int(binary.LittleEndian.Uint32(b[base : base+4]))
	if len(b) < base+4+propLen {
		return nil, fmt.Errorf("unmarshalNode: payload truncated (props)")
	}
	var props []byte
	if propLen > 0 {
		props = make([]byte, propLen)
		copy(props, b[base+4:base+4+propLen])
	}
	return &store.Node{ID: id, Labels: labels, Properties: props}, nil
}

// marshalEdge encodes an Edge: id(8) src(8) dst(8) labelCount(1) labels(2*N) weight(4) propLen(4) props(n)
func marshalEdge(e *store.Edge) []byte {
	labelCount := len(e.Labels)
	propLen := len(e.Properties)
	labelsBytes := currentLabelBytesPerValue * labelCount
	// 8+8+8 (ids) + 1 (labelCount) + 2*N (labels) + 4 (weight) + 4 (propLen) + propLen
	buf := make([]byte, edgePayloadLabelStart+labelsBytes+edgePayloadTailFixedSize+propLen)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.ID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(e.Src))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(e.Dst))
	buf[edgePayloadIDsBytes] = byte(labelCount)
	for i, lbl := range e.Labels {
		base := edgePayloadLabelStart + (currentLabelBytesPerValue * i)
		binary.LittleEndian.PutUint16(buf[base:base+2], uint16(lbl))
	}
	base := edgePayloadLabelStart + labelsBytes
	binary.LittleEndian.PutUint32(buf[base:base+4], math.Float32bits(e.Weight))
	binary.LittleEndian.PutUint32(buf[base+4:base+8], uint32(propLen))
	if propLen > 0 {
		copy(buf[base+8:], e.Properties)
	}
	return buf
}

func unmarshalEdge(b []byte) (*store.Edge, error) {
	if len(b) < edgePayloadLabelStart {
		return nil, fmt.Errorf("unmarshalEdge: payload too short (%d bytes)", len(b))
	}
	id := store.EdgeID(binary.LittleEndian.Uint64(b[0:8]))
	src := store.NodeID(binary.LittleEndian.Uint64(b[8:16]))
	dst := store.NodeID(binary.LittleEndian.Uint64(b[16:24]))
	labelCount := int(b[edgePayloadIDsBytes])

	newBase := edgePayloadLabelStart + (currentLabelBytesPerValue * labelCount)
	oldBase := edgePayloadLabelStart + (legacyLabelBytesPerValue * labelCount)

	decodeV2 := false
	decodeV1 := false

	if len(b) >= newBase+8 {
		propLenNew := int(binary.LittleEndian.Uint32(b[newBase+4 : newBase+8]))
		if newBase+8+propLenNew == len(b) {
			decodeV2 = true
		}
	}
	if !decodeV2 && len(b) >= oldBase+8 {
		propLenOld := int(binary.LittleEndian.Uint32(b[oldBase+4 : oldBase+8]))
		if oldBase+8+propLenOld == len(b) {
			decodeV1 = true
		}
	}
	if !decodeV2 && !decodeV1 {
		return nil, fmt.Errorf("unmarshalEdge: payload truncated (labels)")
	}

	labels := make([]store.EdgeType, labelCount)
	base := oldBase
	if decodeV2 {
		for i := 0; i < labelCount; i++ {
			lb := edgePayloadLabelStart + (currentLabelBytesPerValue * i)
			labels[i] = store.EdgeType(binary.LittleEndian.Uint16(b[lb : lb+2]))
		}
		base = newBase
	} else {
		for i := 0; i < labelCount; i++ {
			labels[i] = store.EdgeType(b[edgePayloadLabelStart+i])
		}
	}

	weight := math.Float32frombits(binary.LittleEndian.Uint32(b[base : base+4]))
	propLen := int(binary.LittleEndian.Uint32(b[base+4 : base+8]))
	if len(b) < base+8+propLen {
		return nil, fmt.Errorf("unmarshalEdge: payload truncated (props)")
	}
	var props []byte
	if propLen > 0 {
		props = make([]byte, propLen)
		copy(props, b[base+8:base+8+propLen])
	}
	return &store.Edge{ID: id, Src: src, Dst: dst, Labels: labels, Weight: weight, Properties: props}, nil
}

func rawEdgeToStore(re rawEdge) *store.Edge {
	return &store.Edge{
		ID:         re.ID,
		Src:        re.Src,
		Dst:        re.Dst,
		Labels:     re.Labels,
		Weight:     re.Weight,
		Properties: cloneBytes(re.Properties),
	}
}

// deserialiseCSR reconstructs a CSRGraph from Serialise() byte slices.
// Format v2 stores labels plus a reserved property offset; format v3 stores
// 1-byte labels plus inline property blobs; format v4 stores uint16 labels
// plus inline property blobs.
func deserialiseCSR(data []byte) (*CSRGraph, *csrIndexSection, error) {
	if len(data) < 22 {
		return nil, nil, fmt.Errorf("deserialiseCSR: data too short")
	}
	if string(data[0:4]) != "GCSR" {
		return nil, nil, fmt.Errorf("deserialiseCSR: invalid magic")
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version < csrVersionV2 || version > csrVersionCurrent {
		return nil, nil, fmt.Errorf("deserialiseCSR: unsupported version %d (supported: %d-%d)",
			version, csrVersionV2, csrVersionCurrent)
	}
	nodeCount := int(binary.LittleEndian.Uint64(data[6:14]))
	edgeCount := int(binary.LittleEndian.Uint64(data[14:22]))
	pos := 22

	// Sequence high-water marks (version 5+).
	var nodeSeqHW, edgeSeqHW uint64
	if version >= csrVersionWithSeqHW {
		if len(data) < csrV5HeaderSize {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated sequence high-water header")
		}
		nodeSeqHW = binary.LittleEndian.Uint64(data[22:30])
		edgeSeqHW = binary.LittleEndian.Uint64(data[30:38])
		pos = csrV5HeaderSize
	}

	// Property-index section offset (version 6+).
	var indexOffset uint64
	if version >= csrVersionWithPropIndex {
		if len(data) < csrV6HeaderSize {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated index-offset header")
		}
		indexOffset = binary.LittleEndian.Uint64(data[38:46])
		pos = csrV6HeaderSize
	}

	nodes := make([]nodeRecord, nodeCount)
	for i := range nodes {
		if pos+9 > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated node record %d", i)
		}
		nid := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		labelCount := int(data[pos])
		pos++
		labelBytes := labelCount
		if version >= csrVersionWithU16Labels {
			labelBytes = labelCount * currentLabelBytesPerValue
		}
		if pos+labelBytes+8 > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated node labels %d", i)
		}
		labels := make([]store.NodeType, labelCount)
		for j := 0; j < labelCount; j++ {
			if version >= csrVersionWithU16Labels {
				labels[j] = store.NodeType(binary.LittleEndian.Uint16(data[pos:]))
				pos += currentLabelBytesPerValue
			} else {
				labels[j] = store.NodeType(data[pos])
				pos++
			}
		}
		props, nextPos, err := readCSRProperties(data, pos, version, "node", i)
		if err != nil {
			return nil, nil, err
		}
		pos = nextPos
		nodes[i] = nodeRecord{ID: nid, Labels: labels, Properties: props}
	}

	edges := make([]rawEdge, edgeCount)
	for i := range edges {
		if pos+25 > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated edge record %d", i)
		}
		eid := store.EdgeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		src := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		dst := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		labelCount := int(data[pos])
		pos++
		labelBytes := labelCount
		if version >= csrVersionWithU16Labels {
			labelBytes = labelCount * currentLabelBytesPerValue
		}
		if pos+labelBytes+12 > len(data) {
			return nil, nil, fmt.Errorf("deserialiseCSR: truncated edge labels %d", i)
		}
		labels := make([]store.EdgeType, labelCount)
		for j := 0; j < labelCount; j++ {
			if version >= csrVersionWithU16Labels {
				labels[j] = store.EdgeType(binary.LittleEndian.Uint16(data[pos:]))
				pos += currentLabelBytesPerValue
			} else {
				labels[j] = store.EdgeType(data[pos])
				pos++
			}
		}
		weight := math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		props, nextPos, err := readCSRProperties(data, pos, version, "edge", i)
		if err != nil {
			return nil, nil, err
		}
		pos = nextPos
		edges[i] = rawEdge{ID: eid, Src: src, Dst: dst, Labels: labels, Weight: weight, Properties: props}
	}

	csr := Build(nodes, edges)
	csr.nodeSeqHW = nodeSeqHW
	csr.edgeSeqHW = edgeSeqHW

	// Pre-v6 files carry no index section; the caller falls back to the WAL.
	if version < csrVersionWithPropIndex {
		return csr, nil, nil
	}
	section, err := readCSRIndexSection(data, int(indexOffset))
	if err != nil {
		return nil, nil, err
	}
	return csr, section, nil
}

// csrIndexSection holds the property-index entries read out of a v6+ CSR file.
type csrIndexSection struct {
	NodeProps []index.NodePropEntry
	EdgeProps []index.EdgePropEntry
}

// readCSRIndexSection parses the property-index section at the given offset.
func readCSRIndexSection(data []byte, offset int) (*csrIndexSection, error) {
	if offset <= 0 || offset > len(data) {
		return nil, fmt.Errorf("readCSRIndexSection: index offset %d out of range (file is %d bytes)", offset, len(data))
	}
	pos := offset
	if pos+csrIndexSectionMagicSize > len(data) {
		return nil, fmt.Errorf("readCSRIndexSection: truncated index magic")
	}
	if string(data[pos:pos+csrIndexSectionMagicSize]) != csrIndexSectionMagic {
		return nil, fmt.Errorf("readCSRIndexSection: bad index magic %q", data[pos:pos+csrIndexSectionMagicSize])
	}
	pos += csrIndexSectionMagicSize

	readCount := func(what string) (int, error) {
		if pos+8 > len(data) {
			return 0, fmt.Errorf("readCSRIndexSection: truncated %s count", what)
		}
		n := int(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		if n < 0 {
			return 0, fmt.Errorf("readCSRIndexSection: negative %s count", what)
		}
		return n, nil
	}
	readEntry := func(what string, i int) (uint64, string, []byte, error) {
		if pos+14 > len(data) {
			return 0, "", nil, fmt.Errorf("readCSRIndexSection: truncated %s entry %d", what, i)
		}
		id := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		keyLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+keyLen+4 > len(data) {
			return 0, "", nil, fmt.Errorf("readCSRIndexSection: truncated %s key %d", what, i)
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen
		valLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+valLen > len(data) {
			return 0, "", nil, fmt.Errorf("readCSRIndexSection: truncated %s value %d", what, i)
		}
		val := make([]byte, valLen)
		copy(val, data[pos:pos+valLen])
		pos += valLen
		return id, key, val, nil
	}

	section := &csrIndexSection{}

	nodeCount, err := readCount("node property")
	if err != nil {
		return nil, err
	}
	section.NodeProps = make([]index.NodePropEntry, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id, key, val, err := readEntry("node property", i)
		if err != nil {
			return nil, err
		}
		section.NodeProps = append(section.NodeProps, index.NodePropEntry{ID: store.NodeID(id), Key: key, Value: val})
	}

	edgeCount, err := readCount("edge property")
	if err != nil {
		return nil, err
	}
	section.EdgeProps = make([]index.EdgePropEntry, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		id, key, val, err := readEntry("edge property", i)
		if err != nil {
			return nil, err
		}
		section.EdgeProps = append(section.EdgeProps, index.EdgePropEntry{ID: store.EdgeID(id), Key: key, Value: val})
	}

	return section, nil
}

// storeEdgeMatchesFilter returns true if the edge carries any label in the filter (OR semantics).
func storeEdgeMatchesFilter(filter []store.EdgeType, e *store.Edge) bool {
	for _, ft := range filter {
		if e.HasLabel(ft) {
			return true
		}
	}
	return false
}

// rawEdgeMatchesFilter returns true if the raw label slice contains any filter label (OR semantics).
func rawEdgeMatchesFilter(filter []store.EdgeType, labels []store.EdgeType) bool {
	for _, ft := range filter {
		if rawEdgeHasLabel(labels, ft) {
			return true
		}
	}
	return false
}

func readCSRProperties(data []byte, pos int, version uint16, kind string, index int) ([]byte, int, error) {
	if version == 2 {
		if pos+8 > len(data) {
			return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
		}
		return nil, pos + 8, nil
	}
	if pos+4 > len(data) {
		return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s properties %d", kind, index)
	}
	propLen := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if pos+propLen > len(data) {
		return nil, pos, fmt.Errorf("deserialiseCSR: truncated %s property blob %d", kind, index)
	}
	if propLen == 0 {
		return nil, pos, nil
	}
	props := make([]byte, propLen)
	copy(props, data[pos:pos+propLen])
	return props, pos + propLen, nil
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// marshalID encodes an 8-byte little-endian ID payload for tombstone records.
func marshalID(id uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, id)
	return buf
}

// unmarshalID decodes an 8-byte tombstone payload.
func unmarshalID(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, fmt.Errorf("unmarshalID: payload too short (%d bytes)", len(b))
	}
	return binary.LittleEndian.Uint64(b[:8]), nil
}

// removeEdgeID returns ids with occurrences of target removed, preserving order.
//
// This is for **adjacency lists only**. Delta label postings are sorted and use
// store.DeleteSortedID instead; adjacency cannot be sorted because EdgesOf must
// return edges in insertion order for traversal results to stay stable.
func removeEdgeID(ids []store.EdgeID, target store.EdgeID) []store.EdgeID {
	out := ids[:0]
	for _, id := range ids {
		if id != target {
			out = append(out, id)
		}
	}
	return out
}

func (s *Store) collectCandidateNodeIDs(ids []store.NodeID) []store.NodeID {
	if len(ids) > 0 {
		out := make([]store.NodeID, 0, len(ids))
		seen := make(map[store.NodeID]struct{}, len(ids))
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, err := s.GetNode(id); err == nil {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out
	}

	out := make([]store.NodeID, 0)
	seen := make(map[store.NodeID]struct{})

	s.mu.RLock()
	for id := range s.deltaNodes {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	s.mu.RUnlock()

	if s.csr != nil {
		s.mu.RLock()
		for i := 1; i < len(s.csr.nodes); i++ {
			n := s.csr.nodes[i]
			if n.ID == store.InvalidNodeID {
				continue
			}
			if _, del := s.deletedNodes[n.ID]; del {
				continue
			}
			if _, ok := seen[n.ID]; !ok {
				seen[n.ID] = struct{}{}
				out = append(out, n.ID)
			}
		}
		s.mu.RUnlock()
	}

	return out
}

func (s *Store) collectCandidateEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) > 0 {
		out := make([]store.EdgeID, 0, len(ids))
		seen := make(map[store.EdgeID]struct{}, len(ids))
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			if _, err := s.GetEdge(id); err == nil {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
		return out
	}

	out := make([]store.EdgeID, 0)
	seen := make(map[store.EdgeID]struct{})

	s.mu.RLock()
	for id := range s.deltaEdges {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	s.mu.RUnlock()

	if s.csr != nil {
		s.mu.RLock()
		for i := 1; i < len(s.csr.edges); i++ {
			e := s.csr.edges[i]
			if e.ID == store.InvalidEdgeID {
				continue
			}
			if _, del := s.deletedEdges[e.ID]; del {
				continue
			}
			if _, ok := seen[e.ID]; !ok {
				seen[e.ID] = struct{}{}
				out = append(out, e.ID)
			}
		}
		s.mu.RUnlock()
	}

	return out
}

// matchNodeIDsByFilters returns the ascending, deduplicated set of node IDs
// satisfying the filters under the given mode.
//
// Each filter is resolved to a sorted slice and the slices are merged, rather
// than each being built into a map and the maps intersected. Merging is one pass
// per side with no hashing, the output stays sorted so the query path can skip
// its final sort, and an empty intersection under MatchAll can stop early.
func (s *Store) matchNodeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) []store.NodeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.NodeID
	for i, f := range filters {
		set := s.matchOneNodeFilter(f)
		if i == 0 {
			acc = set
			continue
		}
		if mode == store.MatchAny {
			acc = store.UnionSortedIDs(acc, set)
			continue
		}
		acc = store.IntersectSortedIDs(acc, set)
		if len(acc) == 0 {
			// Nothing can re-enter an empty intersection.
			return acc
		}
	}
	return acc
}

// matchOneNodeFilter resolves a single filter to an ascending, deduplicated set.
func (s *Store) matchOneNodeFilter(f store.PropertyFilter) []store.NodeID {
	if f.Op == store.PropertyOpEqual {
		// Postings are already ascending and deduplicated.
		return s.propIdx.NodesByProperty(f.Key, f.Value)
	}
	// A key declared ordered answers ranges and prefixes by binary search. Its
	// comparison is byte-wise, so the whole predicate is resolved there — mixing
	// it with the scan matcher below would apply two orderings to one key.
	if ids, served := s.propIdx.NodesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	// Otherwise scan only the buckets belonging to this key, never the whole index.
	var out []store.NodeID
	s.propIdx.ForEachNodeEntry(f.Key, func(id store.NodeID, value []byte) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, id)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

// matchEdgeIDsByFilters is matchNodeIDsByFilters for edge properties.
func (s *Store) matchEdgeIDsByFilters(filters []store.PropertyFilter, mode store.MatchMode) []store.EdgeID {
	if len(filters) == 0 {
		return nil
	}
	var acc []store.EdgeID
	for i, f := range filters {
		set := s.matchOneEdgeFilter(f)
		if i == 0 {
			acc = set
			continue
		}
		if mode == store.MatchAny {
			acc = store.UnionSortedIDs(acc, set)
			continue
		}
		acc = store.IntersectSortedIDs(acc, set)
		if len(acc) == 0 {
			return acc
		}
	}
	return acc
}

func (s *Store) matchOneEdgeFilter(f store.PropertyFilter) []store.EdgeID {
	if f.Op == store.PropertyOpEqual {
		return s.propIdx.EdgesByProperty(f.Key, f.Value)
	}
	if ids, served := s.propIdx.EdgesMatchingOrdered(nil, f); served {
		return store.SortDedupeIDs(ids)
	}
	var out []store.EdgeID
	s.propIdx.ForEachEdgeEntry(f.Key, func(id store.EdgeID, value []byte) bool {
		if store.PropertyFilterMatches(f, value) {
			out = append(out, id)
		}
		return true
	})
	return store.SortDedupeIDs(out)
}

func nodeHasAnyType(n *store.Node, typeSet map[store.NodeType]struct{}) bool {
	for _, lbl := range n.Labels {
		if _, ok := typeSet[lbl]; ok {
			return true
		}
	}
	return false
}

func edgeHasAnyType(e *store.Edge, typeSet map[store.EdgeType]struct{}) bool {
	for _, lbl := range e.Labels {
		if _, ok := typeSet[lbl]; ok {
			return true
		}
	}
	return false
}

func makeNodeIDSet(ids []store.NodeID) map[store.NodeID]struct{} {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[store.NodeID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

// marshalNodeProp encodes a node property index entry:
// nodeID(8) + keyLen(2) + key(keyLen) + valLen(4) + val(valLen)
func marshalNodeProp(id store.NodeID, key string, value []byte) []byte {
	kl := len(key)
	vl := len(value)
	buf := make([]byte, 8+2+kl+4+vl)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(id))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(kl))
	copy(buf[10:10+kl], key)
	binary.LittleEndian.PutUint32(buf[10+kl:14+kl], uint32(vl))
	copy(buf[14+kl:], value)
	return buf
}

func unmarshalNodeProp(b []byte) (id store.NodeID, key string, value []byte, err error) {
	if len(b) < 14 {
		return 0, "", nil, fmt.Errorf("unmarshalNodeProp: too short")
	}
	id = store.NodeID(binary.LittleEndian.Uint64(b[0:8]))
	kl := int(binary.LittleEndian.Uint16(b[8:10]))
	if len(b) < 10+kl+4 {
		return 0, "", nil, fmt.Errorf("unmarshalNodeProp: truncated key")
	}
	key = string(b[10 : 10+kl])
	vl := int(binary.LittleEndian.Uint32(b[10+kl : 14+kl]))
	if len(b) < 14+kl+vl {
		return 0, "", nil, fmt.Errorf("unmarshalNodeProp: truncated value")
	}
	value = make([]byte, vl)
	copy(value, b[14+kl:])
	return id, key, value, nil
}

// marshalEdgeProp encodes an edge property index entry:
// edgeID(8) + keyLen(2) + key(keyLen) + valLen(4) + val(valLen)
func marshalEdgeProp(id store.EdgeID, key string, value []byte) []byte {
	kl := len(key)
	vl := len(value)
	buf := make([]byte, 8+2+kl+4+vl)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(id))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(kl))
	copy(buf[10:10+kl], key)
	binary.LittleEndian.PutUint32(buf[10+kl:14+kl], uint32(vl))
	copy(buf[14+kl:], value)
	return buf
}

func unmarshalEdgeProp(b []byte) (id store.EdgeID, key string, value []byte, err error) {
	if len(b) < 14 {
		return 0, "", nil, fmt.Errorf("unmarshalEdgeProp: too short")
	}
	id = store.EdgeID(binary.LittleEndian.Uint64(b[0:8]))
	kl := int(binary.LittleEndian.Uint16(b[8:10]))
	if len(b) < 10+kl+4 {
		return 0, "", nil, fmt.Errorf("unmarshalEdgeProp: truncated key")
	}
	key = string(b[10 : 10+kl])
	vl := int(binary.LittleEndian.Uint32(b[10+kl : 14+kl]))
	if len(b) < 14+kl+vl {
		return 0, "", nil, fmt.Errorf("unmarshalEdgeProp: truncated value")
	}
	value = make([]byte, vl)
	copy(value, b[14+kl:])
	return id, key, value, nil
}

// ExplainNodeQuery reports how the planner would resolve query, without
// returning the matching entities.
//
// It runs the driving step for real — that is the only way to know how large the
// candidate set is, and the candidate size is what decides how each residual
// filter is applied — but stops before applying them. So the cost is the driver,
// not the query.
//
// The plan is diagnostic. Which index the planner picks is free to change as the
// cost model improves; the results a query returns are not.
func (s *Store) ExplainNodeQuery(query store.NodeQuery) (store.QueryPlan, error) {
	candidates, _, plan := s.driveNodeCandidates(query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = s.propIdx.PlanNodeResiduals(query.Filters, plan.DriverFilter, len(candidates))
	}
	ids, err := s.QueryNodeIDs(query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}

// ExplainEdgeQuery is ExplainNodeQuery for edge queries.
func (s *Store) ExplainEdgeQuery(query store.EdgeQuery) (store.QueryPlan, error) {
	candidates, _, plan := s.driveEdgeCandidates(query)
	plan.Candidates = len(candidates)
	if len(query.Filters) > 0 && store.NormalizedFilterMode(query.FilterMode) == store.MatchAll {
		plan.Residuals = s.propIdx.PlanEdgeResiduals(query.Filters, plan.DriverFilter, len(candidates))
	}
	ids, err := s.QueryEdgeIDs(query)
	if err != nil {
		return plan, err
	}
	plan.Results = len(ids)
	return plan, nil
}
