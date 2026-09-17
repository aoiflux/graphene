package bulk

// The walk, written once.
//
// Three formats, and only one of them should know how to enumerate a graph. An
// encoder decides how a record is spelled; the walk below decides what records
// there are and in what order, which is where the ordering rule lives and where
// a bug would corrupt all three formats at once rather than one.
//
// The decoder side mirrors it: a decoder yields records in stream order and
// load() below does the ID mapping, the batching and the ordering checks for
// every format identically.

import (
	"errors"
	"fmt"
	"io"

	"github.com/aoiflux/graphene/store"
)

// eachNodeID visits every node ID src offers, in ascending order.
//
// A source that implements store.Scanner — which is what both bundled backends'
// Snapshot does — is streamed, so the export holds one batch of IDs instead of
// one per node in the graph. Anything else is enumerated into a slice first,
// which is what every source did before and what a third-party store still
// gets. Both orders are ascending, so which path ran is not observable in the
// dump.
func eachNodeID(src Source, visit func(store.NodeID) error) error {
	if sc, ok := src.(store.Scanner); ok {
		for id, err := range sc.ScanNodes() {
			if err != nil {
				return fmt.Errorf("bulk: enumerate nodes: %w", err)
			}
			if err := visit(id); err != nil {
				return err
			}
		}
		return nil
	}
	ids, err := src.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		return fmt.Errorf("bulk: enumerate nodes: %w", err)
	}
	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

// eachEdgeID is eachNodeID for edges.
func eachEdgeID(src Source, visit func(store.EdgeID) error) error {
	if sc, ok := src.(store.Scanner); ok {
		for id, err := range sc.ScanEdges() {
			if err != nil {
				return fmt.Errorf("bulk: enumerate edges: %w", err)
			}
			if err := visit(id); err != nil {
				return err
			}
		}
		return nil
	}
	ids, err := src.QueryEdgeIDs(store.EdgeQuery{})
	if err != nil {
		return fmt.Errorf("bulk: enumerate edges: %w", err)
	}
	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

// encoder is the per-format half of an export.
//
// Methods are called in the order the package doc gives — header, nodes, edges,
// node properties, edge properties, trailer — and Close is always called, so an
// encoder holding a buffered writer flushes there and nowhere else.
type encoder interface {
	header(h Header) error
	node(n *store.Node) error
	edge(e *store.Edge) error
	nodeProperty(id store.NodeID, key string, value []byte) error
	edgeProperty(id store.EdgeID, key string, value []byte) error
	trailer(t Trailer) error
	Close() error
}

// walk exports src through enc.
//
// The records are fetched one at a time, never all at once: a hydrated record
// carries its properties and a dump of a large graph would not fit if they were
// all held. The IDs used to be materialised up front, one uint64 per record,
// which is the cheap half of the same problem but still O(V+E) — so a source
// that can stream them is streamed instead, and the export then holds one batch
// of IDs rather than all of them. See eachNodeID.
func walk(src Source, enc encoder, opts Options) (Summary, error) {
	var sum Summary

	// Inline needs both ends and is not a preference: an encoder with nowhere to
	// put a nested list, or a source that cannot group its entries by entity,
	// writes the Format 1 shape inside a Format 2 envelope. The header says
	// which happened, so no reader has to infer it.
	inl, _ := enc.(inlineEncoder)
	grp, _ := src.(store.PropertyEntryGrouper)
	inline := inl != nil && grp != nil && !opts.SkipProperties

	ordNodes, ordEdges, compNodes, compEdges := declarationsOf(src)
	h := Header{
		Format:            Format,
		OrderedNodeKeys:   ordNodes,
		OrderedEdgeKeys:   ordEdges,
		CompositeNodeKeys: compNodes,
		CompositeEdgeKeys: compEdges,
		InlineEntries:     inline,
	}
	if err := enc.header(h); err != nil {
		return sum, err
	}

	if err := eachNodeID(src, func(id store.NodeID) error {
		n, err := src.GetNode(id)
		if err != nil {
			// A record that vanished between the enumeration and the fetch is a
			// concurrent delete, not a corrupt store. Skipping it is what makes
			// an export of a live store possible at all; exporting from a
			// Snapshot is how a caller gets one instant instead.
			if isNotFound(err) {
				return nil
			}
			return fmt.Errorf("bulk: read node %d: %w", id, err)
		}
		if inline {
			e := grp.NodePropertyEntries(id)
			if err := inl.nodeWith(n, e); err != nil {
				return err
			}
			sum.NodeProperties += int64(len(e))
		} else if err := enc.node(n); err != nil {
			return err
		}
		sum.Nodes++
		return nil
	}); err != nil {
		return sum, err
	}

	if err := eachEdgeID(src, func(id store.EdgeID) error {
		e, err := src.GetEdge(id)
		if err != nil {
			if isNotFound(err) {
				return nil
			}
			return fmt.Errorf("bulk: read edge %d: %w", id, err)
		}
		if inline {
			en := grp.EdgePropertyEntries(id)
			if err := inl.edgeWith(e, en); err != nil {
				return err
			}
			sum.EdgeProperties += int64(len(en))
		} else if err := enc.edge(e); err != nil {
			return err
		}
		sum.Edges++
		return nil
	}); err != nil {
		return sum, err
	}

	if !opts.SkipProperties && !inline {
		// A source that cannot enumerate its property entries is refused, not
		// quietly exported without them. Silently dropping them is the exact
		// failure this package's doc comment is about — a dump that restores a
		// graph which looks complete and answers nothing — and it is easy to
		// reach by accident: *graphene.Graph embeds store.GraphStore, and an
		// embedded interface promotes only its own method set, so a Graph does
		// not satisfy PropertyEnumerator unless it says so itself.
		pe, ok := src.(store.PropertyEnumerator)
		if !ok {
			return sum, fmt.Errorf("%w: %T", ErrNoPropertyEntries, src)
		}
		{
			// An entry whose entity is not there is dropped rather than written.
			// The property index is not versioned and an entry can outlive its
			// record — a node indexed after it was deleted leaves one, and so does
			// ReindexKeep — so without this an export succeeds and the import of
			// what it wrote fails with "referenced before it is defined", which is
			// a dump that cannot be restored and says nothing about why.
			//
			// It is a lookup per entry, not a set of everything exported: the
			// export is streamed precisely so it does not hold the graph, and
			// building the set to check against would put it back. A record
			// deleted between its export and this check is dropped too, which is
			// the same tolerance the record loops above already apply.
			var perr error
			pe.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
				if _, err := src.GetNode(id); err != nil {
					if isNotFound(err) {
						return true
					}
					perr = fmt.Errorf("bulk: read node %d: %w", id, err)
					return false
				}
				if perr = enc.nodeProperty(id, key, value); perr != nil {
					return false
				}
				sum.NodeProperties++
				return true
			})
			if perr != nil {
				return sum, perr
			}
			pe.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
				if _, err := src.GetEdge(id); err != nil {
					if isNotFound(err) {
						return true
					}
					perr = fmt.Errorf("bulk: read edge %d: %w", id, err)
					return false
				}
				if perr = enc.edgeProperty(id, key, value); perr != nil {
					return false
				}
				sum.EdgeProperties++
				return true
			})
			if perr != nil {
				return sum, perr
			}
		}
	}

	if err := enc.trailer(Trailer{
		Nodes:          sum.Nodes,
		Edges:          sum.Edges,
		NodeProperties: sum.NodeProperties,
		EdgeProperties: sum.EdgeProperties,
	}); err != nil {
		return sum, err
	}
	return sum, enc.Close()
}

// isNotFound reports the store's "it is gone" error, which an export treats as
// a record that was deleted underneath it rather than as a failure.
func isNotFound(err error) bool {
	var nf *store.ErrNotFound
	return errors.As(err, &nf)
}

// isEOF ends a decoded stream. Wrapped as well as bare, because a decoder that
// reads through a bufio.Reader reports the wrapped form.
func isEOF(err error) bool { return errors.Is(err, io.EOF) }

// decoder is the per-format half of an import. next returns one record at a
// time; io.EOF ends the stream.
type decoder interface {
	header() (Header, error)
	next() (record, error)
	Close() error
}

// recordKind names what a decoded record carries.
type recordKind uint8

const (
	recNode recordKind = iota + 1
	recEdge
	recNodeProp
	recEdgeProp
	recTrailer
)

// record is one decoded thing. A tagged union rather than an interface: the
// decoders build one per record on a path that runs once per node in the graph,
// and an interface would allocate for every one of them.
type record struct {
	kind recordKind

	node *store.Node
	edge *store.Edge

	// entries are the index entries a Format 2 record carries inline. Nil on a
	// Format 1 record, whose entries arrive later as recNodeProp/recEdgeProp.
	entries []store.PropertyEntry

	// id is the *exported* id a property entry names, before mapping.
	nodeID store.NodeID
	edgeID store.EdgeID
	key    string
	value  []byte

	trailer Trailer
}

// load imports a decoded stream into dst.
//
// Nodes and edges are accumulated into batches and committed together where the
// destination supports it. Property entries are not batched: IndexNodeProperty
// takes one entry, and on the disk backend it is a single unbatched WAL append
// either way, so buffering them would add memory and remove nothing.
//
// A batch is committed when it reaches Options.BatchSize records or
// Options.MaxBatchBytes bytes, whichever comes first, and Options.Compact is
// evaluated after every commit. dst is reassigned rather than fixed because
// Options.Reopen may hand back a different destination -- every closure below
// reads the variable, so a swap reaches all of them.
func load(dec decoder, dst Dest, opts Options) (Summary, error) {
	var sum Summary

	h, err := dec.header()
	if err != nil {
		return sum, err
	}
	if err := checkFormat(h.Format); err != nil {
		return sum, err
	}
	sum.Declarations = applyDeclarations(dst, h, opts)

	ids := newIDMap()
	size := opts.batchSize()
	maxBytes := opts.MaxBatchBytes

	// pendingNodes holds the exported records alongside the values handed to the
	// store, because the store rewrites ID in place and the map needs both.
	var pendingNodes []*store.Node
	var pendingNodeIDs []store.NodeID
	var pendingNodeBytes int64
	var pendingEdges []*store.Edge
	var pendingEdgeIDs []store.EdgeID
	var pendingEdgeBytes int64

	// A Format 2 record's entries wait with it, because they are filed against
	// the identifier the destination assigns and that is not known until the
	// batch commits. One slice per pending record rather than a flat list: an
	// entry has to find its own record's new identifier, and pairing by position
	// is what the ID map does for a Format 1 dump one record at a time.
	var pendingNodeEntries [][]store.PropertyEntry
	var pendingEdgeEntries [][]store.PropertyEntry

	// indexEntries files one committed record's inline entries.
	//
	// Counted even when they are skipped, because the trailer counts what the
	// export wrote and Options.SkipProperties changes what an import applies,
	// not what the dump contained. The Format 1 path does the same.
	indexNodeEntries := func(id store.NodeID, entries []store.PropertyEntry) error {
		sum.NodeProperties += int64(len(entries))
		if opts.SkipProperties {
			return nil
		}
		for _, p := range entries {
			if err := dst.IndexNodeProperty(id, p.Key, p.Value); err != nil {
				return fmt.Errorf("bulk: index node property %q: %w", p.Key, err)
			}
		}
		return nil
	}
	indexEdgeEntries := func(id store.EdgeID, entries []store.PropertyEntry) error {
		sum.EdgeProperties += int64(len(entries))
		if opts.SkipProperties {
			return nil
		}
		for _, p := range entries {
			if err := dst.IndexEdgeProperty(id, p.Key, p.Value); err != nil {
				return fmt.Errorf("bulk: index edge property %q: %w", p.Key, err)
			}
		}
		return nil
	}

	flushNodes := func() error {
		if len(pendingNodes) == 0 {
			return nil
		}
		newIDs, err := addNodes(dst, pendingNodes)
		if err != nil {
			return err
		}
		for i, old := range pendingNodeIDs {
			ids.nodes[old] = newIDs[i]
		}
		// Before the schedule runs, so a compaction fired by this batch folds in
		// the entries this batch created rather than leaving them for the next.
		for i, e := range pendingNodeEntries {
			if len(e) == 0 {
				continue
			}
			if err := indexNodeEntries(newIDs[i], e); err != nil {
				return err
			}
		}
		sum.Nodes += int64(len(pendingNodes))
		pendingNodes = pendingNodes[:0]
		pendingNodeIDs = pendingNodeIDs[:0]
		pendingNodeEntries = pendingNodeEntries[:0]
		pendingNodeBytes = 0
		dst, err = maybeCompact(dst, opts)
		return err
	}
	flushEdges := func() error {
		if len(pendingEdges) == 0 {
			return nil
		}
		newIDs, err := addEdges(dst, pendingEdges)
		if err != nil {
			return err
		}
		for i, old := range pendingEdgeIDs {
			ids.edges[old] = newIDs[i]
		}
		for i, e := range pendingEdgeEntries {
			if len(e) == 0 {
				continue
			}
			if err := indexEdgeEntries(newIDs[i], e); err != nil {
				return err
			}
		}
		sum.Edges += int64(len(pendingEdges))
		pendingEdges = pendingEdges[:0]
		pendingEdgeIDs = pendingEdgeIDs[:0]
		pendingEdgeEntries = pendingEdgeEntries[:0]
		pendingEdgeBytes = 0
		dst, err = maybeCompact(dst, opts)
		return err
	}

	var trailer Trailer
	seenTrailer := false

	for {
		rec, err := dec.next()
		if err != nil {
			if isEOF(err) {
				break
			}
			return sum, err
		}

		switch rec.kind {
		case recNode:
			exported := rec.node.ID
			rec.node.ID = 0
			pendingNodes = append(pendingNodes, rec.node)
			pendingNodeIDs = append(pendingNodeIDs, exported)
			pendingNodeEntries = append(pendingNodeEntries, rec.entries)
			pendingNodeBytes += nodeBatchBytes(rec.node)
			if len(pendingNodes) >= size || (maxBytes > 0 && pendingNodeBytes >= maxBytes) {
				if err := flushNodes(); err != nil {
					return sum, err
				}
			}

		case recEdge:
			// Every node must be in before any edge is resolved, and the
			// ordering rule guarantees it — but only once the last node batch
			// is committed, which is what this flush is for.
			if err := flushNodes(); err != nil {
				return sum, err
			}
			src, err := ids.node(rec.edge.Src)
			if err != nil {
				return sum, err
			}
			dstID, err := ids.node(rec.edge.Dst)
			if err != nil {
				return sum, err
			}
			exported := rec.edge.ID
			rec.edge.ID = 0
			rec.edge.Src = src
			rec.edge.Dst = dstID
			pendingEdges = append(pendingEdges, rec.edge)
			pendingEdgeIDs = append(pendingEdgeIDs, exported)
			pendingEdgeEntries = append(pendingEdgeEntries, rec.entries)
			pendingEdgeBytes += edgeBatchBytes(rec.edge)
			if len(pendingEdges) >= size || (maxBytes > 0 && pendingEdgeBytes >= maxBytes) {
				if err := flushEdges(); err != nil {
					return sum, err
				}
			}

		case recNodeProp:
			if err := flushNodes(); err != nil {
				return sum, err
			}
			if opts.SkipProperties {
				sum.NodeProperties++
				continue
			}
			id, err := ids.node(rec.nodeID)
			if err != nil {
				return sum, err
			}
			if err := dst.IndexNodeProperty(id, rec.key, rec.value); err != nil {
				return sum, fmt.Errorf("bulk: index node property %q: %w", rec.key, err)
			}
			sum.NodeProperties++

		case recEdgeProp:
			if err := flushNodes(); err != nil {
				return sum, err
			}
			if err := flushEdges(); err != nil {
				return sum, err
			}
			if opts.SkipProperties {
				sum.EdgeProperties++
				continue
			}
			id, err := ids.edge(rec.edgeID)
			if err != nil {
				return sum, err
			}
			if err := dst.IndexEdgeProperty(id, rec.key, rec.value); err != nil {
				return sum, fmt.Errorf("bulk: index edge property %q: %w", rec.key, err)
			}
			sum.EdgeProperties++

		case recTrailer:
			trailer = rec.trailer
			seenTrailer = true

		default:
			return sum, fmt.Errorf("bulk: unknown record kind %d", rec.kind)
		}
	}

	if err := flushNodes(); err != nil {
		return sum, err
	}
	if err := flushEdges(); err != nil {
		return sum, err
	}
	if !seenTrailer {
		return sum, fmt.Errorf("bulk: dump has no trailer; it is truncated")
	}
	if err := trailer.check(sum); err != nil {
		return sum, err
	}
	return sum, dec.Close()
}

// addNodes commits a batch through the batched path when the destination has
// one, and a record at a time when it does not.
func addNodes(dst Dest, nodes []*store.Node) ([]store.NodeID, error) {
	if b, ok := dst.(nodeBatcher); ok {
		ids, err := b.AddNodesBatch(nodes)
		if err != nil {
			return nil, fmt.Errorf("bulk: add nodes: %w", err)
		}
		return ids, nil
	}
	ids := make([]store.NodeID, len(nodes))
	for i, n := range nodes {
		id, err := dst.AddNode(n)
		if err != nil {
			return nil, fmt.Errorf("bulk: add node: %w", err)
		}
		ids[i] = id
	}
	return ids, nil
}

func addEdges(dst Dest, edges []*store.Edge) ([]store.EdgeID, error) {
	if b, ok := dst.(edgeBatcher); ok {
		ids, err := b.AddEdgesBatch(edges)
		if err != nil {
			return nil, fmt.Errorf("bulk: add edges: %w", err)
		}
		return ids, nil
	}
	ids := make([]store.EdgeID, len(edges))
	for i, e := range edges {
		id, err := dst.AddEdge(e)
		if err != nil {
			return nil, fmt.Errorf("bulk: add edge: %w", err)
		}
		ids[i] = id
	}
	return ids, nil
}
