package graphene

// Importing a dump through the one-pass loader.
//
// bulk.ImportDump writes the records as it reads them, which is correct for a
// destination that may already hold a store and is the wrong shape for one that
// does not: every MaxDeltaBytes worth of records is folded into the image by a
// compaction that rewrites the whole image, so what an import writes grows with
// the square of what it is importing. BulkLoad writes the image once. This is
// the join between the two, and it lives here rather than in bulk because bulk
// is backend-agnostic by design and BulkLoad is the disk backend.
//
// # What it costs, stated before what it buys
//
// The dump is read twice. A load asks for every node and then, separately, for
// every edge, so the source has to be re-openable -- a file is, a pipe is not,
// which is why the source is a function that opens rather than a reader.
//
// It needs a Format 2 dump whose records carry their own index entries. A
// Format 1 dump keeps every entry in a key-major section after the records, and
// a record's entries are then spread across the whole of it; gathering them
// means holding every triple, which is the memory a one-pass load exists not to
// spend. That is refused at the header, before anything is written.
//
// And it holds eight bytes per node. The dump names endpoints by the identifiers
// the *source* store assigned, and the load assigns its own, so the edge pass has
// to be able to map one to the other. The map is the exported identifiers in
// ascending order, searched: 8 B per node, 80 MB at ten million, against a
// map[NodeID]NodeID of the same population at roughly seven times that. It is
// the one term here that grows with the graph, and it is why this is documented
// as bounded rather than flat.
//
// # What it buys
//
// Amplification 1.00x instead of 6.53x and upwards. See Graph.BulkLoad.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/store"
)

// ErrDumpNotBulkLoadable is returned when a dump cannot drive a one-pass load.
//
// It is a refusal and not a fallback, because the choice between the two import
// paths belongs to the caller: silently doing the quadratic thing when the fast
// path was asked for would turn a 3.7 GiB load into a 171 GiB one without
// saying so. ImportDumpBulk reports it before writing anything, so a caller that
// wants the fallback can call bulk.ImportDump on the same file.
var ErrDumpNotBulkLoadable = errors.New("graphene: dump cannot drive a one-pass load")

// BulkImportOptions tunes ImportDumpBulk.
//
// It is a type of its own rather than bulk.Options because most of that struct
// describes an incremental import -- the batch size, the byte cap, the
// compaction schedule and the reopen hook are all about how often a growing
// delta is folded into an image, and a one-pass load never has one. Offering
// them here and ignoring them would be a worse answer than not offering them.
type BulkImportOptions struct {
	// SkipDeclarations leaves the destination's index declarations alone.
	//
	// The declarations are applied before the load rather than after it, which
	// is the one ordering that matters: a composite tuple is computed from a
	// record's entries as the record goes past, so a tuple declared afterwards
	// describes nothing until the next compaction.
	SkipDeclarations bool

	// SkipProperties loads the records and none of their index entries.
	//
	// The entries are still read and still counted, because the trailer counts
	// what the export wrote. What changes is whether they are filed.
	SkipProperties bool
}

// ImportDumpBulk fills an empty store from a graphene_dump in one forward pass
// and returns a fresh Graph on it. The receiver is closed and must not be used
// again, exactly as BulkLoad's is.
//
//	g, sum, err := g.ImportDumpBulk(func() (io.ReadCloser, error) {
//		return os.Open(path)
//	}, graphene.BulkImportOptions{})
//
// open is called twice and must return a reader over the same bytes each time.
// Each reader is closed by this function. A source that cannot be re-opened --
// a pipe, a network stream, a decompressor over either -- has to be staged to a
// file first, and staging it is cheaper than what the incremental path would
// write.
//
// The requirements are checked before anything is written: the destination must
// be backed by a directory and empty (disk.ErrBulkLoadNotEmpty), and the dump
// must be Format 2 with inline entries (ErrDumpNotBulkLoadable). A failure at
// any point leaves the directory as it was found, which is BulkLoad's contract
// and not an addition here.
func (g *Graph) ImportDumpBulk(open func() (io.ReadCloser, error), opts BulkImportOptions) (*Graph, bulk.Summary, error) {
	return g.ImportDumpBulkCtx(context.Background(), open, opts)
}

// ImportDumpBulkCtx is ImportDumpBulk, with the build abandoned if ctx is
// cancelled. The commit and the reopen are not cancellable, for the reason
// BulkLoadCtx gives.
func (g *Graph) ImportDumpBulkCtx(ctx context.Context,
	open func() (io.ReadCloser, error), opts BulkImportOptions) (*Graph, bulk.Summary, error) {

	var sum bulk.Summary
	if open == nil {
		return nil, sum, fmt.Errorf("ImportDumpBulk: no source to open")
	}

	// The header decides whether this can run at all, and it is read on a
	// throwaway pass so that the refusal happens before the receiver is closed.
	// BulkLoad closes it whether the load succeeded or not, so a refusal raised
	// from inside the node pass would cost the caller their handle for a
	// condition that was knowable up front.
	hdr, err := peekDumpHeader(open)
	if err != nil {
		return nil, sum, err
	}
	if !hdr.InlineEntries {
		return nil, sum, fmt.Errorf(
			"%w: it is format %d without inline entries, and a load needs each record's "+
				"index entries with the record; re-export it with this build, or import it "+
				"with bulk.ImportDump",
			ErrDumpNotBulkLoadable, hdr.Format)
	}
	// Declared before the load, never after. See BulkImportOptions.
	if !opts.SkipDeclarations {
		sum.Declarations = bulk.ApplyDeclarations(g.GraphStore, hdr)
	}

	ids := &exportedNodeIDs{}
	var trailer bulk.Trailer

	loaded, err := g.BulkLoadCtx(ctx,
		func(w *BulkNodeWriter) error {
			return eachDumpPass(open, func(scan *bulk.DumpScan) error {
				return scan.Nodes(func(n *store.Node, entries []store.PropertyEntry) error {
					id, err := w.AddNode(BulkNode{
						Labels:     n.Labels,
						Properties: n.Properties,
						Index:      bulkProperties(entries, opts.SkipProperties),
					})
					if err != nil {
						return err
					}
					sum.Nodes++
					sum.NodeProperties += int64(len(entries))
					return ids.add(n.ID, id)
				})
			})
		},
		func(w *BulkEdgeWriter) error {
			return eachDumpPass(open, func(scan *bulk.DumpScan) error {
				if err := scan.Edges(func(e *store.Edge, entries []store.PropertyEntry) error {
					src, ok := ids.lookup(e.Src)
					if !ok {
						return fmt.Errorf("bulk: node %d is referenced before it is defined", e.Src)
					}
					dst, ok := ids.lookup(e.Dst)
					if !ok {
						return fmt.Errorf("bulk: node %d is referenced before it is defined", e.Dst)
					}
					if _, err := w.AddEdge(BulkEdge{
						Src: src, Dst: dst,
						Labels:     e.Labels,
						Weight:     e.Weight,
						Properties: e.Properties,
						Index:      bulkProperties(entries, opts.SkipProperties),
					}); err != nil {
						return err
					}
					sum.Edges++
					sum.EdgeProperties += int64(len(entries))
					return nil
				}); err != nil {
					return err
				}
				// Read on the second pass, where the cursor is already past the
				// edges. Reading it on the first would mean draining the whole
				// file twice over.
				t, err := scan.Trailer()
				if err != nil {
					return err
				}
				trailer = t
				return nil
			})
		})
	if err != nil {
		return nil, sum, err
	}

	// After the load rather than during it. The counts are the dump's own
	// description of itself and disagreeing with them means the stream was
	// truncated or rewritten -- but a load commits or does not, so there is no
	// half-written store to protect by checking earlier, and checking here lets
	// the message name both figures.
	if err := checkDumpTrailer(trailer, sum); err != nil {
		loaded.Close()
		return nil, sum, err
	}
	return loaded, sum, nil
}

// peekDumpHeader opens the source once for its header alone.
func peekDumpHeader(open func() (io.ReadCloser, error)) (bulk.Header, error) {
	r, err := open()
	if err != nil {
		return bulk.Header{}, fmt.Errorf("ImportDumpBulk: open dump: %w", err)
	}
	defer func() { _ = r.Close() }()
	scan, err := bulk.OpenDumpScan(r)
	if err != nil {
		return bulk.Header{}, fmt.Errorf("ImportDumpBulk: %w", err)
	}
	return scan.Header(), nil
}

// eachDumpPass opens the source, runs one pass over it and closes it.
//
// The close is the reason this is a function rather than two inline blocks: a
// load holds its temp image open for the whole of both passes, and a pass that
// leaked its reader would hold a second descriptor on the same file for as long
// again.
func eachDumpPass(open func() (io.ReadCloser, error), pass func(*bulk.DumpScan) error) error {
	r, err := open()
	if err != nil {
		return fmt.Errorf("open dump: %w", err)
	}
	defer func() { _ = r.Close() }()
	scan, err := bulk.OpenDumpScan(r)
	if err != nil {
		return err
	}
	if err := pass(scan); err != nil {
		return err
	}
	return scan.Close()
}

// bulkProperties converts a record's entries into the loader's shape.
//
// Returns nil when properties are being skipped, which is what makes
// SkipProperties free rather than filed-and-discarded: the sorters never see the
// entries at all, so the load does not pay to sort what it was told to drop.
func bulkProperties(entries []store.PropertyEntry, skip bool) []BulkProperty {
	if skip || len(entries) == 0 {
		return nil
	}
	out := make([]BulkProperty, len(entries))
	for i, e := range entries {
		out[i] = BulkProperty{Key: e.Key, Value: e.Value}
	}
	return out
}

// checkDumpTrailer compares what the dump said it held against what was loaded.
func checkDumpTrailer(t bulk.Trailer, sum bulk.Summary) error {
	for _, c := range []struct {
		what       string
		want, have int64
	}{
		{"nodes", t.Nodes, sum.Nodes},
		{"edges", t.Edges, sum.Edges},
		{"node properties", t.NodeProperties, sum.NodeProperties},
		{"edge properties", t.EdgeProperties, sum.EdgeProperties},
	} {
		if c.want != c.have {
			return fmt.Errorf("bulk: dump declares %d %s and %d were loaded", c.want, c.what, c.have)
		}
	}
	return nil
}

// exportedNodeIDs maps the identifiers a dump names onto the ones the load gave.
//
// # Why not a map
//
// map[store.NodeID]store.NodeID is what the incremental importer uses and it is
// the right structure there, where the destination may already hold a store and
// the identifiers arriving have no relationship to each other. Here they do: an
// export walks its nodes in ascending identifier order, so the exported column
// arrives sorted, and a load assigns from an empty store's counter, so the
// assigned column is consecutive. A sorted column and a consecutive one need one
// slice and a search, not a hash table -- 8 bytes a node against roughly 56.
//
// # What breaks the consecutive half, and why it is handled rather than assumed
//
// BulkLoad takes identifiers from the store's own sequence counter rather than
// numbering the records 1..N, precisely so that a write landing during the build
// cannot collide with one the load already used. That is a real possibility --
// the load refuses a store that holds records, not one that is about to -- and it
// leaves a gap in the assigned column. So the gap is detected and the assigned
// column materialised from that point on, which costs 8 more bytes a node for
// the remainder and only for a load that was actually raced.
type exportedNodeIDs struct {
	exported []uint64

	// base is the first identifier the load assigned. While assigned is nil the
	// k-th node has identifier base+k, which is the case for every load nothing
	// wrote into.
	base     uint64
	assigned []uint64
}

// add records one node, and refuses a source whose identifiers are not ascending.
//
// Ascending is what the search below needs and what the ordering rule already
// requires of every exporter here. A dump that violates it fails the load rather
// than being sorted into shape: sorting means holding a permutation of both
// columns, which is the memory this structure exists to avoid, and a stream this
// build did not write is better refused with a reason than silently reordered.
func (m *exportedNodeIDs) add(exported store.NodeID, assigned store.NodeID) error {
	if n := len(m.exported); n > 0 && uint64(exported) <= m.exported[n-1] {
		return fmt.Errorf(
			"%w: node identifiers are not ascending (%d follows %d), and a one-pass load "+
				"maps endpoints by searching them",
			ErrDumpNotBulkLoadable, exported, m.exported[n-1])
	}
	k := len(m.exported)
	m.exported = append(m.exported, uint64(exported))

	switch {
	case k == 0:
		m.base = uint64(assigned)
	case m.assigned != nil:
		m.assigned = append(m.assigned, uint64(assigned))
	case uint64(assigned) != m.base+uint64(k):
		// The first gap. Everything up to here was consecutive, so it is
		// reconstructed rather than remembered.
		m.assigned = make([]uint64, 0, k+1)
		for i := range k {
			m.assigned = append(m.assigned, m.base+uint64(i))
		}
		m.assigned = append(m.assigned, uint64(assigned))
	}
	return nil
}

// lookup maps an exported identifier onto the assigned one.
func (m *exportedNodeIDs) lookup(exported store.NodeID) (store.NodeID, bool) {
	i, ok := slices.BinarySearch(m.exported, uint64(exported))
	if !ok {
		return 0, false
	}
	if m.assigned != nil {
		return store.NodeID(m.assigned[i]), true
	}
	return store.NodeID(m.base + uint64(i)), true
}
