package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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

	// syncOnCommit forces an fsync at each batch commit. Guarded by mu.
	syncOnCommit bool

	// reindexPolicy governs what updates do to propIdx. Guarded by mu.
	reindexPolicy store.ReindexPolicy

	// Sequence counters (shared across CSR and delta).
	nodeSeq atomic.Uint64
	edgeSeq atomic.Uint64

	// commitSeq numbers batch commits. Unlike nodeSeq and edgeSeq it has no
	// high-water mark in the CSR header, so it currently resumes from the
	// highest value the surviving WAL replays and restarts after a compaction
	// truncates that log. See batchMeta for why persisting it is being held for
	// the v8 format change rather than spent on a bump of its own.
	commitSeq atomic.Uint64

	// nowUnixNano supplies the commit timestamp. Indirected so tests can pin it;
	// production leaves it nil and reads the clock.
	nowUnixNano func() int64

	// lastCompact is when Compact last completed in this process. Guarded by mu.
	// Not persisted — a reopened store reports zero even if its image on disk was
	// compacted a moment earlier. Persisting it needs a CSR header field and is
	// held for the v8 format change, like the commit sequence high-water mark.
	lastCompact time.Time
}

type deltaAdj struct {
	out []store.EdgeID
	in  []store.EdgeID
}

// nextCommitMeta allocates the provenance for one batch commit.
//
// Callers hold s.mu, but commitSeq is atomic anyway so the number is unique
// even for a future writer that does not.
func (s *Store) nextCommitMeta(ctx store.TxContext) batchMeta {
	now := time.Now().UnixNano
	if s.nowUnixNano != nil {
		now = s.nowUnixNano
	}
	return batchMeta{
		CommitSeq: s.commitSeq.Add(1),
		UnixNano:  now(),
		ActorID:   ctx.ActorID,
	}
}

// StorageStats implements store.StorageReporter.
//
// Taken under a read lock so the delta counts are mutually consistent — a
// caller comparing DeltaNodes against CSRNodes should not see figures from
// either side of a compaction. The WAL size is read outside that consistency
// guarantee (see WAL.Size) and may lag by one record.
func (s *Store) StorageStats() store.StorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := store.StorageStats{
		DeltaNodes:   len(s.deltaNodes),
		DeltaEdges:   len(s.deltaEdges),
		DeletedNodes: len(s.deletedNodes),
		DeletedEdges: len(s.deletedEdges),
		WALBytes:     s.wal.Size(),
		CommitSeq:    s.commitSeq.Load(),
		LastCompact:  s.lastCompact,
	}
	if csr := s.csr; csr != nil {
		st.CSRNodes = csr.NodeCount()
		st.CSREdges = csr.EdgeCount()
	}
	st.PropertyNodeEntries, st.PropertyEdgeEntries = s.propIdx.EntryCounts()
	return st
}

// observeCommitMeta advances the commit counter past a value read back from the
// log, so a reopened store does not reissue sequence numbers the log already
// used.
func (s *Store) observeCommitMeta(m batchMeta) {
	for {
		cur := s.commitSeq.Load()
		if m.CommitSeq <= cur || s.commitSeq.CompareAndSwap(cur, m.CommitSeq) {
			return
		}
	}
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
	csrVersionNoAdjacency    = 7 // stopped writing the never-read adjacency arrays
	csrVersionCurrent        = csrVersionSectioned
	csrV6HeaderSize          = 46 // magic4 + version2 + counts16 + seqHW16 + indexOffset8
	csrV5HeaderSize          = 38
	csrIndexSectionMagic     = "GIDX"
	csrIndexSectionMagicSize = 4

	// Smallest number of bytes a single record can occupy on disk. Used to bound
	// header-declared counts against the file's actual remaining length before
	// allocating from them — see deserialiseCSR.
	//
	// node:  id8 + labelCount1 + propLen4                       (zero labels, zero-length blob)
	// edge:  id8 + src8 + dst8 + labelCount1 + weight4 + propLen4
	// entry: id8 + keyLen2 + valLen4                            (index section)
	minNodeRecordBytes = 13
	minEdgeRecordBytes = 33
	minPropEntryBytes  = 14

	// Build indexes its arrays by entity ID, so opening a file costs memory
	// proportional to its highest ID, not to how many records it holds. Two
	// bounds keep that from being a file's to choose.
	//
	// maxCSREntityID is the absolute ceiling: ~67M IDs, so the node array tops
	// out near 4 GB. IDs are never reused, and Compact preserves them, so a
	// long-lived store's highest ID only ever grows — this is the ceiling on the
	// engine's addressable lifetime, and the right value follows from the maximum
	// graph size it intends to support, still open as plan §8 Q2.
	//
	// csrIDSparsityFactor bounds the highest ID against the record count actually
	// present, because the absolute ceiling alone is no protection: one 13-byte
	// record naming ID 2^26 is enough to demand the whole 4 GB. Allowing 256 IDs
	// burned per surviving record is far past what rollbacks and deletions
	// produce in practice, and it makes a small file's worst case small.
	//
	// csrIDSparsityFloor keeps the relative bound from over-constraining a nearly
	// empty file, whose record count is too small to derive a useful ceiling
	// from. It is what caps a hostile minimal file, at ~4 MB.
	maxCSREntityID      = 1 << 26
	csrIDSparsityFactor = 256
	csrIDSparsityFloor  = 1 << 16
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
		syncOnCommit:     true,
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

	// One framed, transactional write for the whole batch. Previously this was
	// one WAL append — and one write syscall — per node.
	batch := newWALBatch(len(stored) * 64)
	for _, n := range stored {
		node := n
		batch.addWith(walRecordNode, func(dst []byte) []byte {
			return appendMarshalledNode(dst, node)
		})
	}
	if err := s.wal.AppendBatch(batch.finish(s.nextCommitMeta(store.TxContext{})), s.syncOnCommit); err != nil {
		// Apply nothing. The commit marker never reached the file, so replay will
		// discard whatever partial bytes did — that absence *is* the rollback.
		// The IDs assigned above are simply never used, which is allowed: IDs are
		// monotonic and never reused, but were never promised to be dense.
		return nil, fmt.Errorf("AddNodesBatch: wal: %w", err)
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
		// Validation happens before anything is written, so a failure here means
		// the transaction never started. Returning ids[:i] would name IDs for
		// edges that do not exist — the old non-atomic behaviour, and now wrong.
		if !s.nodeExistsLocked(e.Src) {
			return nil, &store.ErrInvalidEdge{MissingID: e.Src}
		}
		if !s.nodeExistsLocked(e.Dst) {
			return nil, &store.ErrInvalidEdge{MissingID: e.Dst}
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

	batch := newWALBatch(len(stored) * 80)
	for _, e := range stored {
		edge := e
		batch.addWith(walRecordEdge, func(dst []byte) []byte {
			return appendMarshalledEdge(dst, edge)
		})
	}
	if err := s.wal.AppendBatch(batch.finish(s.nextCommitMeta(store.TxContext{})), s.syncOnCommit); err != nil {
		return nil, fmt.Errorf("AddEdgesBatch: wal: %w", err)
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
			node := &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)}
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
			return &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)}, nil
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

// SetSyncOnCommit controls whether a batch write is flushed to the platter
// before it returns.
//
// Default is true: a transaction whose commit is not fsynced is not a commit,
// and batching gives the engine its first well-defined durability boundary. The
// cost is one fsync per batch (~0.1–1 ms depending on device), which a batch of
// any size amortises well and a batch of one does not.
//
// Turn it off only if you sync explicitly — via Compact or Close — and can
// afford to lose everything since.
func (s *Store) SetSyncOnCommit(v bool) {
	s.mu.Lock()
	s.syncOnCommit = v
	s.mu.Unlock()
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
// ReserveNodeID implements store.Transactor.
func (s *Store) ReserveNodeID() store.NodeID {
	return store.NodeID(s.nodeSeq.Add(1))
}

// ReserveEdgeID implements store.Transactor.
func (s *Store) ReserveEdgeID() store.EdgeID {
	return store.EdgeID(s.edgeSeq.Add(1))
}

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
		CommitFunc: s.observeCommitMeta,
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

// csrBytes returns the property blob for a record that lives in the CSR.
//
// The CSR is immutable once published, and the API contract states that reads
// may hand back pointers into internal state (see API_REFERENCE §"Do not mutate
// returned structs"). Delta-resident reads already alias; this makes
// CSR-resident reads consistent with them.
func csrBytes(src []byte) []byte {
	return src
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
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

// GetNodesBatch resolves many node IDs with one lock-free attempt for the whole
// batch, falling back to a single locked pass for whatever it could not serve.
//
// The per-item path takes the store lock (or performs the lock-free
// validity dance) once per ID. Batching amortises both: one `csrFastRead`, one
// validity re-check, and at most one `RLock` for the remainder.
//
// **Order is preserved.** Records resolved without the lock and records resolved
// under it are interleaved back into request order rather than concatenated,
// because a caller that asked for [a b c] and received [a c b] would have no way
// to tell without comparing IDs.
func (s *Store) GetNodesBatch(ids []store.NodeID) ([]*store.Node, []store.NodeID) {
	if len(ids) == 0 {
		return nil, nil
	}
	// Positional: slot i holds the record for ids[i], or nil if unresolved.
	slots := make([]*store.Node, len(ids))
	pending := make([]int, 0, len(ids)) // indices still needing the locked path

	if csr, ok := s.csrFastRead(); ok {
		for i, id := range ids {
			if rec, found := csr.GetNode(id); found {
				slots[i] = &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)}
				continue
			}
			pending = append(pending, i)
		}
		if !s.csrFastReadValid(csr) {
			// The CSR moved under us: discard everything the fast path produced
			// and redo the whole batch under the lock. Partial trust is not an
			// option — some slots could be from the superseded CSR.
			for i := range slots {
				slots[i] = nil
			}
			pending = pending[:0]
			for i := range ids {
				pending = append(pending, i)
			}
		}
	} else {
		for i := range ids {
			pending = append(pending, i)
		}
	}

	if len(pending) > 0 {
		s.mu.RLock()
		for _, i := range pending {
			if n, ok := s.getNodeLocked(ids[i]); ok {
				slots[i] = n
			}
		}
		s.mu.RUnlock()
	}

	return compactNodes(slots, ids)
}

// GetEdgesBatch is GetNodesBatch for edges.
func (s *Store) GetEdgesBatch(ids []store.EdgeID) ([]*store.Edge, []store.EdgeID) {
	if len(ids) == 0 {
		return nil, nil
	}
	slots := make([]*store.Edge, len(ids))
	pending := make([]int, 0, len(ids))

	if csr, ok := s.csrFastRead(); ok {
		for i, id := range ids {
			if rec, found := csr.GetEdge(id); found {
				slots[i] = rawEdgeToStore(rec)
				continue
			}
			pending = append(pending, i)
		}
		if !s.csrFastReadValid(csr) {
			for i := range slots {
				slots[i] = nil
			}
			pending = pending[:0]
			for i := range ids {
				pending = append(pending, i)
			}
		}
	} else {
		for i := range ids {
			pending = append(pending, i)
		}
	}

	if len(pending) > 0 {
		s.mu.RLock()
		for _, i := range pending {
			// The existing two-value helper is the authority here; duplicating
			// its delta-then-CSR logic would be a second place to keep correct.
			if e, ok := s.getEdgeLocked(ids[i]); ok {
				slots[i] = e
			}
		}
		s.mu.RUnlock()
	}

	return compactEdges(slots, ids)
}

// getNodeLocked returns the authoritative live node (delta override or CSR copy)
// or (nil, false) if it is missing or masked. Caller must hold s.mu.
//
// Deliberately mirrors getEdgeLocked's shape, so the two layers are resolved the
// same way on both paths.
func (s *Store) getNodeLocked(id store.NodeID) (*store.Node, bool) {
	if _, del := s.deletedNodes[id]; del {
		return nil, false
	}
	if n, ok := s.deltaNodes[id]; ok {
		return n, true
	}
	if s.csr != nil {
		if rec, found := s.csr.GetNode(id); found {
			return &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)}, true
		}
	}
	return nil, false
}

// compactNodes removes the nil slots, preserving order, and reports which ids
// they corresponded to.
func compactNodes(slots []*store.Node, ids []store.NodeID) ([]*store.Node, []store.NodeID) {
	found := slots[:0]
	var missing []store.NodeID
	for i, n := range slots {
		if n == nil {
			missing = append(missing, ids[i])
			continue
		}
		found = append(found, n)
	}
	return found, missing
}

func compactEdges(slots []*store.Edge, ids []store.EdgeID) ([]*store.Edge, []store.EdgeID) {
	found := slots[:0]
	var missing []store.EdgeID
	for i, e := range slots {
		if e == nil {
			missing = append(missing, ids[i])
			continue
		}
		found = append(found, e)
	}
	return found, missing
}

// Sync forces everything written so far to durable storage.
//
// Single writes are not synced individually — that would cost an fsync per
// AddNode, turning a ~6 µs operation into a ~1 ms one. Batch commits do sync by
// default (see SetSyncOnCommit); for individual writes this is how a caller
// establishes a durability point without paying for a full Compact.
//
// After it returns, everything written before the call survives power loss.
func (s *Store) Sync() error {
	return s.wal.Sync()
}
