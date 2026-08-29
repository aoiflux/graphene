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
// The two ID enumerations happen up front and the records are fetched one at a
// time. That split is deliberate: the ID lists are one uint64 per record and
// the hydrated records are not, so materialising the first and streaming the
// second is what keeps an export's memory proportional to the graph's shape
// rather than to its contents.
func walk(src Source, enc encoder, opts Options) (Summary, error) {
	var sum Summary

	ordNodes, ordEdges, compNodes, compEdges := declarationsOf(src)
	h := Header{
		Format:            Format,
		OrderedNodeKeys:   ordNodes,
		OrderedEdgeKeys:   ordEdges,
		CompositeNodeKeys: compNodes,
		CompositeEdgeKeys: compEdges,
	}
	if err := enc.header(h); err != nil {
		return sum, err
	}

	nodeIDs, err := src.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		return sum, fmt.Errorf("bulk: enumerate nodes: %w", err)
	}
	for _, id := range nodeIDs {
		n, err := src.GetNode(id)
		if err != nil {
			// A record that vanished between the enumeration and the fetch is a
			// concurrent delete, not a corrupt store. Skipping it is what makes
			// an export of a live store possible at all; exporting from a
			// Snapshot is how a caller gets one instant instead.
			if isNotFound(err) {
				continue
			}
			return sum, fmt.Errorf("bulk: read node %d: %w", id, err)
		}
		if err := enc.node(n); err != nil {
			return sum, err
		}
		sum.Nodes++
	}

	edgeIDs, err := src.QueryEdgeIDs(store.EdgeQuery{})
	if err != nil {
		return sum, fmt.Errorf("bulk: enumerate edges: %w", err)
	}
	for _, id := range edgeIDs {
		e, err := src.GetEdge(id)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return sum, fmt.Errorf("bulk: read edge %d: %w", id, err)
		}
		if err := enc.edge(e); err != nil {
			return sum, err
		}
		sum.Edges++
	}

	if !opts.SkipProperties {
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
			var perr error
			pe.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
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

	// pendingNodes holds the exported records alongside the values handed to the
	// store, because the store rewrites ID in place and the map needs both.
	var pendingNodes []*store.Node
	var pendingNodeIDs []store.NodeID
	var pendingEdges []*store.Edge
	var pendingEdgeIDs []store.EdgeID

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
		sum.Nodes += int64(len(pendingNodes))
		pendingNodes = pendingNodes[:0]
		pendingNodeIDs = pendingNodeIDs[:0]
		return nil
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
		sum.Edges += int64(len(pendingEdges))
		pendingEdges = pendingEdges[:0]
		pendingEdgeIDs = pendingEdgeIDs[:0]
		return nil
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
			if len(pendingNodes) >= size {
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
			if len(pendingEdges) >= size {
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
