package disk

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
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

	// Type indexes over delta (CSR has its own type lookups).
	deltaNodesByType map[store.NodeType][]store.NodeID
	deltaEdgesByType map[store.EdgeType][]store.EdgeID

	// Property index (in-memory; rebuilt from WAL on restart).
	propIdx *index.PropertyIndex

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
		deltaNodesByType: make(map[store.NodeType][]store.NodeID),
		deltaEdgesByType: make(map[store.EdgeType][]store.EdgeID),
		propIdx:          index.NewPropertyIndex(),
	}

	// Try to load existing CSR.
	csrPath := filepath.Join(dir, csrFileName)
	if _, err := os.Stat(csrPath); err == nil {
		if err := s.loadCSR(csrPath); err != nil {
			return nil, fmt.Errorf("disk.Open: load CSR: %w", err)
		}
	}

	// Replay WAL into delta.
	if err := s.replayWAL(); err != nil {
		return nil, fmt.Errorf("disk.Open: replay WAL: %w", err)
	}

	return s, nil
}

// --- GraphStore implementation ---

func (s *Store) AddNode(n *store.Node) (store.NodeID, error) {
	id := store.NodeID(s.nodeSeq.Add(1))

	stored := &store.Node{
		ID: id,
	}
	if len(n.Labels) > 0 {
		stored.Labels = make([]store.NodeType, len(n.Labels))
		copy(stored.Labels, n.Labels)
	}
	if len(n.Properties) > 0 {
		stored.Properties = make([]byte, len(n.Properties))
		copy(stored.Properties, n.Properties)
	}

	// Serialise to WAL payload: id(8) + labelCount(1) + labels(N) + propLen(4) + props
	payload := marshalNode(stored)
	if err := s.wal.AppendNode(payload); err != nil {
		return store.InvalidNodeID, fmt.Errorf("AddNode: wal: %w", err)
	}

	s.mu.Lock()
	s.deltaNodes[id] = stored
	for _, lbl := range n.Labels {
		s.deltaNodesByType[lbl] = append(s.deltaNodesByType[lbl], id)
	}
	s.ensureDeltaAdj(id)
	s.mu.Unlock()

	return id, nil
}

// AddNodesBatch adds nodes in order and returns assigned IDs.
// On error, successfully written prefixes are committed and returned.
func (s *Store) AddNodesBatch(nodes []*store.Node) ([]store.NodeID, error) {
	ids := make([]store.NodeID, len(nodes))
	stored := make([]*store.Node, len(nodes))
	payloads := make([][]byte, len(nodes))

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
		payloads[i] = marshalNode(node)
	}

	committed := 0
	for i := range payloads {
		if err := s.wal.AppendNode(payloads[i]); err != nil {
			s.mu.Lock()
			s.commitNodesBatch(stored[:committed])
			s.mu.Unlock()
			return ids[:committed], fmt.Errorf("AddNodesBatch: wal: %w", err)
		}
		committed++
	}

	s.mu.Lock()
	s.commitNodesBatch(stored)
	s.mu.Unlock()

	return ids, nil
}

func (s *Store) AddEdge(e *store.Edge) (store.EdgeID, error) {
	// Validate src/dst exist (check delta + CSR).
	if err := s.nodeExists(e.Src); err != nil {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Src}
	}
	if err := s.nodeExists(e.Dst); err != nil {
		return store.InvalidEdgeID, &store.ErrInvalidEdge{MissingID: e.Dst}
	}

	id := store.EdgeID(s.edgeSeq.Add(1))

	stored := &store.Edge{
		ID:     id,
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

	payload := marshalEdge(stored)
	if err := s.wal.AppendEdge(payload); err != nil {
		return store.InvalidEdgeID, fmt.Errorf("AddEdge: wal: %w", err)
	}

	s.mu.Lock()
	s.deltaEdges[id] = stored
	for _, lbl := range e.Labels {
		s.deltaEdgesByType[lbl] = append(s.deltaEdgesByType[lbl], id)
	}
	s.ensureDeltaAdj(e.Src).out = append(s.ensureDeltaAdj(e.Src).out, id)
	s.ensureDeltaAdj(e.Dst).in = append(s.ensureDeltaAdj(e.Dst).in, id)
	s.mu.Unlock()

	return id, nil
}

// AddEdgesBatch adds edges in order and returns assigned IDs.
// On error, successfully written prefixes are committed and returned.
func (s *Store) AddEdgesBatch(edges []*store.Edge) ([]store.EdgeID, error) {
	ids := make([]store.EdgeID, len(edges))
	stored := make([]*store.Edge, len(edges))
	payloads := make([][]byte, len(edges))

	for i, e := range edges {
		if err := s.nodeExists(e.Src); err != nil {
			return ids[:i], &store.ErrInvalidEdge{MissingID: e.Src}
		}
		if err := s.nodeExists(e.Dst); err != nil {
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
		payloads[i] = marshalEdge(edge)
	}

	committed := 0
	for i := range payloads {
		if err := s.wal.AppendEdge(payloads[i]); err != nil {
			s.mu.Lock()
			s.commitEdgesBatch(stored[:committed])
			s.mu.Unlock()
			return ids[:committed], fmt.Errorf("AddEdgesBatch: wal: %w", err)
		}
		committed++
	}

	s.mu.Lock()
	s.commitEdgesBatch(stored)
	s.mu.Unlock()

	return ids, nil
}

func (s *Store) GetNode(id store.NodeID) (*store.Node, error) {
	s.mu.RLock()
	n, ok := s.deltaNodes[id]
	s.mu.RUnlock()
	if ok {
		return n, nil
	}
	// Fall through to CSR.
	if s.csr != nil {
		rec, found := s.csr.GetNode(id)
		if found {
			return &store.Node{ID: rec.ID, Labels: rec.Labels}, nil
		}
	}
	return nil, &store.ErrNotFound{Kind: "node", ID: uint64(id)}
}

func (s *Store) GetEdge(id store.EdgeID) (*store.Edge, error) {
	s.mu.RLock()
	e, ok := s.deltaEdges[id]
	s.mu.RUnlock()
	if ok {
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

	// Collect from delta.
	s.mu.RLock()
	da := s.deltaAdj[id]
	if da != nil {
		var eids []store.EdgeID
		switch dir {
		case store.DirectionOutbound:
			eids = da.out
		case store.DirectionInbound:
			eids = da.in
		case store.DirectionBoth:
			eids = append(da.out, da.in...)
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
	s.mu.RUnlock()

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
				if edgeTypes != nil && !rawEdgeMatchesFilter(edgeTypes, re.Labels) {
					continue
				}
				result = append(result, rawEdgeToStore(re))
			}
		}
	}

	return result, nil
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
	s.mu.RLock()
	delta := s.deltaNodesByType[t]
	out := make([]store.NodeID, len(delta))
	copy(out, delta)
	s.mu.RUnlock()

	if s.csr != nil {
		out = append(out, s.csr.NodesByType(t)...)
	}
	return out, nil
}

func (s *Store) EdgesByType(t store.EdgeType) ([]store.EdgeID, error) {
	s.mu.RLock()
	delta := s.deltaEdgesByType[t]
	out := make([]store.EdgeID, len(delta))
	copy(out, delta)
	s.mu.RUnlock()

	if s.csr != nil {
		out = append(out, s.csr.EdgesByType(t)...)
	}
	return out, nil
}

func (s *Store) NodeCount() (uint64, error) {
	s.mu.RLock()
	dn := uint64(len(s.deltaNodes))
	s.mu.RUnlock()
	csrN := uint64(0)
	if s.csr != nil {
		csrN = uint64(s.csr.NodeCount())
	}
	return dn + csrN, nil
}

func (s *Store) EdgeCount() (uint64, error) {
	s.mu.RLock()
	de := uint64(len(s.deltaEdges))
	s.mu.RUnlock()
	csrE := uint64(0)
	if s.csr != nil {
		csrE = uint64(s.csr.EdgeCount())
	}
	return de + csrE, nil
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

func (s *Store) NodesByProperty(key string, value []byte) ([]store.NodeID, error) {
	return s.propIdx.NodesByProperty(key, value), nil
}

func (s *Store) EdgesByProperty(key string, value []byte) ([]store.EdgeID, error) {
	return s.propIdx.EdgesByProperty(key, value), nil
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

	// From existing CSR.
	if s.csr != nil {
		for i := 1; i < len(s.csr.nodes); i++ {
			n := s.csr.nodes[i]
			if n.ID != store.InvalidNodeID {
				nodes = append(nodes, n)
			}
		}
		for i := 1; i < len(s.csr.edges); i++ {
			e := s.csr.edges[i]
			if e.ID != store.InvalidEdgeID {
				edges = append(edges, e)
			}
		}
	}

	// From delta.
	for _, n := range s.deltaNodes {
		nodes = append(nodes, nodeRecord{ID: n.ID, Labels: n.Labels})
	}
	for _, e := range s.deltaEdges {
		edges = append(edges, rawEdge{
			ID:     e.ID,
			Src:    e.Src,
			Dst:    e.Dst,
			Labels: e.Labels,
			Weight: e.Weight,
		})
	}

	// Build new CSR.
	newCSR := Build(nodes, edges)

	// Serialise to temp file.
	data := newCSR.Serialise()
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

	// Re-emit all property index entries into the fresh (empty) WAL so they
	// survive the next restart.
	for _, e := range s.propIdx.NodeEntries() {
		if err := s.wal.AppendNodeProp(marshalNodeProp(e.ID, e.Key, e.Value)); err != nil {
			return fmt.Errorf("compact: re-emit node prop: %w", err)
		}
	}
	for _, e := range s.propIdx.EdgeEntries() {
		if err := s.wal.AppendEdgeProp(marshalEdgeProp(e.ID, e.Key, e.Value)); err != nil {
			return fmt.Errorf("compact: re-emit edge prop: %w", err)
		}
	}

	// Swap in new CSR and clear delta.
	s.csr = newCSR
	s.deltaNodes = make(map[store.NodeID]*store.Node)
	s.deltaEdges = make(map[store.EdgeID]*store.Edge)
	s.deltaAdj = make(map[store.NodeID]*deltaAdj)
	s.deltaNodesByType = make(map[store.NodeType][]store.NodeID)
	s.deltaEdgesByType = make(map[store.EdgeType][]store.EdgeID)

	return nil
}

// --- internals ---

func (s *Store) nodeExists(id store.NodeID) error {
	s.mu.RLock()
	_, ok := s.deltaNodes[id]
	s.mu.RUnlock()
	if ok {
		return nil
	}
	if s.csr != nil {
		if _, found := s.csr.GetNode(id); found {
			return nil
		}
	}
	return &store.ErrNotFound{Kind: "node", ID: uint64(id)}
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
		for _, lbl := range n.Labels {
			s.deltaNodesByType[lbl] = append(s.deltaNodesByType[lbl], n.ID)
		}
		s.ensureDeltaAdj(n.ID)
	}
}

// commitEdgesBatch applies edge records to in-memory delta/index state.
// Caller must hold s.mu.
func (s *Store) commitEdgesBatch(edges []*store.Edge) {
	for _, e := range edges {
		s.deltaEdges[e.ID] = e
		for _, lbl := range e.Labels {
			s.deltaEdgesByType[lbl] = append(s.deltaEdgesByType[lbl], e.ID)
		}
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
			s.deltaNodes[n.ID] = n
			for _, lbl := range n.Labels {
				s.deltaNodesByType[lbl] = append(s.deltaNodesByType[lbl], n.ID)
			}
			s.ensureDeltaAdj(n.ID)
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
			s.deltaEdges[e.ID] = e
			for _, lbl := range e.Labels {
				s.deltaEdgesByType[lbl] = append(s.deltaEdgesByType[lbl], e.ID)
			}
			s.ensureDeltaAdj(e.Src).out = append(s.ensureDeltaAdj(e.Src).out, e.ID)
			s.ensureDeltaAdj(e.Dst).in = append(s.ensureDeltaAdj(e.Dst).in, e.ID)
			if uint64(e.ID) > s.edgeSeq.Load() {
				s.edgeSeq.Store(uint64(e.ID))
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
	})
}

func (s *Store) loadCSR(path string) error {
	// For this first implementation, we load the CSR into memory from the
	// serialised file. A future enhancement can mmap this file directly.
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	csr, err := deserialiseCSR(data)
	if err != nil {
		return err
	}
	s.csr = csr
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
	return nil
}

// --- serialisation helpers ---

// marshalNode encodes a Node: id(8) labelCount(1) labels(N) propLen(4) props(n)
func marshalNode(n *store.Node) []byte {
	labelCount := len(n.Labels)
	propLen := len(n.Properties)
	// 8 (id) + 1 (labelCount) + N (labels) + 4 (propLen) + propLen
	buf := make([]byte, 8+1+labelCount+4+propLen)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(n.ID))
	buf[8] = byte(labelCount)
	for i, lbl := range n.Labels {
		buf[9+i] = byte(lbl)
	}
	base := 9 + labelCount
	binary.LittleEndian.PutUint32(buf[base:base+4], uint32(propLen))
	if propLen > 0 {
		copy(buf[base+4:], n.Properties)
	}
	return buf
}

func unmarshalNode(b []byte) (*store.Node, error) {
	if len(b) < 9 {
		return nil, fmt.Errorf("unmarshalNode: payload too short (%d bytes)", len(b))
	}
	id := store.NodeID(binary.LittleEndian.Uint64(b[0:8]))
	labelCount := int(b[8])
	if len(b) < 9+labelCount+4 {
		return nil, fmt.Errorf("unmarshalNode: payload truncated (labels)")
	}
	labels := make([]store.NodeType, labelCount)
	for i := 0; i < labelCount; i++ {
		labels[i] = store.NodeType(b[9+i])
	}
	base := 9 + labelCount
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

// marshalEdge encodes an Edge: id(8) src(8) dst(8) labelCount(1) labels(N) weight(4) propLen(4) props(n)
func marshalEdge(e *store.Edge) []byte {
	labelCount := len(e.Labels)
	propLen := len(e.Properties)
	// 8+8+8 (ids) + 1 (labelCount) + N (labels) + 4 (weight) + 4 (propLen) + propLen
	buf := make([]byte, 24+1+labelCount+4+4+propLen)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.ID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(e.Src))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(e.Dst))
	buf[24] = byte(labelCount)
	for i, lbl := range e.Labels {
		buf[25+i] = byte(lbl)
	}
	base := 25 + labelCount
	binary.LittleEndian.PutUint32(buf[base:base+4], math.Float32bits(e.Weight))
	binary.LittleEndian.PutUint32(buf[base+4:base+8], uint32(propLen))
	if propLen > 0 {
		copy(buf[base+8:], e.Properties)
	}
	return buf
}

func unmarshalEdge(b []byte) (*store.Edge, error) {
	if len(b) < 25 {
		return nil, fmt.Errorf("unmarshalEdge: payload too short (%d bytes)", len(b))
	}
	id := store.EdgeID(binary.LittleEndian.Uint64(b[0:8]))
	src := store.NodeID(binary.LittleEndian.Uint64(b[8:16]))
	dst := store.NodeID(binary.LittleEndian.Uint64(b[16:24]))
	labelCount := int(b[24])
	if len(b) < 25+labelCount+8 {
		return nil, fmt.Errorf("unmarshalEdge: payload truncated (labels)")
	}
	labels := make([]store.EdgeType, labelCount)
	for i := 0; i < labelCount; i++ {
		labels[i] = store.EdgeType(b[25+i])
	}
	base := 25 + labelCount
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
		ID:     re.ID,
		Src:    re.Src,
		Dst:    re.Dst,
		Labels: re.Labels,
		Weight: re.Weight,
	}
}

// deserialiseCSR reconstructs a CSRGraph from a Serialise() v2 byte slice.
// Format v2: variable-length label arrays per node and edge record.
func deserialiseCSR(data []byte) (*CSRGraph, error) {
	if len(data) < 22 {
		return nil, fmt.Errorf("deserialiseCSR: data too short")
	}
	if string(data[0:4]) != "GCSR" {
		return nil, fmt.Errorf("deserialiseCSR: invalid magic")
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version != 2 {
		return nil, fmt.Errorf("deserialiseCSR: unsupported version %d (expected 2)", version)
	}
	nodeCount := int(binary.LittleEndian.Uint64(data[6:14]))
	edgeCount := int(binary.LittleEndian.Uint64(data[14:22]))
	pos := 22

	nodes := make([]nodeRecord, nodeCount)
	for i := range nodes {
		if pos+9 > len(data) {
			return nil, fmt.Errorf("deserialiseCSR: truncated node record %d", i)
		}
		nid := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		labelCount := int(data[pos])
		pos++
		if pos+labelCount+8 > len(data) {
			return nil, fmt.Errorf("deserialiseCSR: truncated node labels %d", i)
		}
		labels := make([]store.NodeType, labelCount)
		for j := 0; j < labelCount; j++ {
			labels[j] = store.NodeType(data[pos])
			pos++
		}
		propOffset := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		nodes[i] = nodeRecord{ID: nid, Labels: labels, PropOffset: propOffset}
	}

	edges := make([]rawEdge, edgeCount)
	for i := range edges {
		if pos+25 > len(data) {
			return nil, fmt.Errorf("deserialiseCSR: truncated edge record %d", i)
		}
		eid := store.EdgeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		src := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		dst := store.NodeID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		labelCount := int(data[pos])
		pos++
		if pos+labelCount+12 > len(data) {
			return nil, fmt.Errorf("deserialiseCSR: truncated edge labels %d", i)
		}
		labels := make([]store.EdgeType, labelCount)
		for j := 0; j < labelCount; j++ {
			labels[j] = store.EdgeType(data[pos])
			pos++
		}
		weight := math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		propOffset := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		edges[i] = rawEdge{ID: eid, Src: src, Dst: dst, Labels: labels, Weight: weight, PropOffset: propOffset}
	}

	csr := Build(nodes, edges)
	return csr, nil
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
