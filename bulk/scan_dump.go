package bulk

// Reading a dump in the order a one-pass load needs it.
//
// load() in walk.go reads a stream once, front to back, and writes as it goes.
// disk.Store.BulkLoad cannot be driven that way: it writes the image's node
// records before any edge record, so it asks its caller for every node and then,
// separately, for every edge. A reader that yielded nodes and edges interleaved
// would have to hold one of the two groups, which is the memory a one-pass load
// exists not to spend.
//
// So a load reads the dump twice, and this is the half that makes that explicit.
// Each pass is a fresh stream over the same bytes, opened by the caller: a file
// can be re-opened, a pipe cannot, and which of those a caller has is not
// something this package can find out for itself.
//
// # Why this is separate from load()
//
// It yields records rather than applying them, and it never touches a Dest. That
// keeps the ID remapping, the batching and the compaction schedule -- all of
// which a bulk load does differently or not at all -- in one place instead of
// behind flags in load(). The two paths share the decoder and the format, which
// is where a disagreement would actually matter.

import (
	"fmt"
	"io"

	"github.com/aoiflux/graphene/store"
)

// DumpScan is one pass over a graphene_dump stream.
//
// The zero value is not usable; call OpenDumpScan. A scan is forward-only and
// each of Nodes, Edges and Trailer consumes the part of the stream it names, so
// they are called in that order and at most once each. Skipping one is allowed
// -- an edge pass calls Edges without Nodes -- and costs the decode of the
// records it passes over.
type DumpScan struct {
	dec  *dumpDecoder
	hdr  Header
	held *record // one record read past the end of a section, kept for the next
	done bool
}

// OpenDumpScan reads r's header and positions the scan at the first record.
//
// The header is read here rather than lazily because every caller needs it
// before deciding anything: whether the format is readable, whether the records
// carry their own index entries, and what to declare on the destination.
func OpenDumpScan(r io.Reader) (*DumpScan, error) {
	dec, err := newDumpDecoder(r)
	if err != nil {
		return nil, err
	}
	h, err := dec.header()
	if err != nil {
		return nil, err
	}
	if err := checkFormat(h.Format); err != nil {
		return nil, err
	}
	return &DumpScan{dec: dec, hdr: h}, nil
}

// Header is what the stream declared.
func (d *DumpScan) Header() Header { return d.hdr }

// next returns the next record, honouring one held over from a previous section.
func (d *DumpScan) next() (record, error) {
	if d.held != nil {
		r := *d.held
		d.held = nil
		return r, nil
	}
	return d.dec.next()
}

// Nodes calls fn for every node record, with the index entries it carries.
//
// The entries are nil unless Header().InlineEntries is set. An error from fn
// stops the scan and is returned unwrapped, so a caller can test for its own
// sentinel.
//
// It stops at the first record that is not a node, which the ordering rule makes
// the first edge or the trailer, and holds that record for the next section. So
// a node pass over a dump whose edges outnumber its nodes reads the nodes and
// stops, rather than decoding the whole file to find out it is finished.
func (d *DumpScan) Nodes(fn func(n *store.Node, entries []store.PropertyEntry) error) error {
	return d.section(recNode, func(r record) error { return fn(r.node, r.entries) })
}

// Edges calls fn for every edge record, with the index entries it carries.
//
// Any node records still ahead of the cursor are decoded and dropped, which is
// what a second pass over the same stream costs: the bytes are read either way,
// and nothing is retained.
func (d *DumpScan) Edges(fn func(e *store.Edge, entries []store.PropertyEntry) error) error {
	return d.section(recEdge, func(r record) error { return fn(r.edge, r.entries) })
}

// section consumes every record of one kind, skipping the kinds that precede it.
//
// The comparison is on recordKind's own order, which is the order the package
// doc requires a stream to be in. A kind below the one wanted is a record an
// earlier section would have taken and this pass is not interested in; a kind
// above it ends the section.
func (d *DumpScan) section(want recordKind, apply func(record) error) error {
	for {
		rec, err := d.next()
		if err != nil {
			if isEOF(err) {
				d.done = true
				return nil
			}
			return err
		}
		switch {
		case rec.kind == want:
			if err := apply(rec); err != nil {
				return err
			}
		case rec.kind < want:
			// Decoded and dropped: a node record on a pass that skipped Nodes.
		default:
			d.held = &rec
			return nil
		}
	}
}

// Trailer drains what is left and returns the counts the dump recorded.
//
// A stream that ends without one is truncated and says so, which is the whole
// reason the counts are at the end: a dump cut between records is well formed up
// to the cut, and only the absent trailer tells it from a complete one.
func (d *DumpScan) Trailer() (Trailer, error) {
	var (
		tr   Trailer
		seen bool
	)
	for !d.done {
		rec, err := d.next()
		if err != nil {
			if isEOF(err) {
				break
			}
			return Trailer{}, err
		}
		if rec.kind == recTrailer {
			tr, seen = rec.trailer, true
		}
	}
	if !seen {
		return Trailer{}, fmt.Errorf("bulk: dump has no trailer; it is truncated")
	}
	return tr, nil
}

// Close releases the decoder. It does not close the reader it was given, which
// belongs to whoever opened it.
func (d *DumpScan) Close() error { return d.dec.Close() }

// ApplyDeclarations re-declares on dst the ordered keys and composite tuples h
// carried, and reports how many were applied.
//
// Exported for a bulk load, which has to declare before it builds: a composite
// is computed from a record's entries as the record goes past, so a tuple
// declared afterwards describes nothing. The import path in this package calls
// it at the same point and for the same reason.
//
// dst is any, because the capabilities are optional interfaces and a caller
// holding a *graphene.Graph, a *disk.Store or a third-party backend should not
// have to know which of them it satisfies. A destination that takes none of the
// declarations is not an error: they change the plan a query uses and never its
// result, so refusing here would be refusing a correct import over a hint.
func ApplyDeclarations(dst any, h Header) int {
	applied := 0
	if d, ok := dst.(store.OrderedIndexDeclarer); ok {
		for _, k := range h.OrderedNodeKeys {
			if err := d.DeclareOrderedNodeProperty(k); err == nil {
				applied++
			}
		}
		for _, k := range h.OrderedEdgeKeys {
			if err := d.DeclareOrderedEdgeProperty(k); err == nil {
				applied++
			}
		}
	}
	if d, ok := dst.(store.CompositeIndexDeclarer); ok {
		for _, keys := range h.CompositeNodeKeys {
			// A tuple this build will not accept is skipped, not fatal: the same
			// rule the GCMP section follows on open, and for the same reason: a
			// reader ignoring a declaration still answers every query correctly.
			if err := d.DeclareCompositeNodeProperties(keys); err == nil {
				applied++
			}
		}
		for _, keys := range h.CompositeEdgeKeys {
			if err := d.DeclareCompositeEdgeProperties(keys); err == nil {
				applied++
			}
		}
	}
	return applied
}
