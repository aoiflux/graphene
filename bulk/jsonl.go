package bulk

// JSON Lines: one JSON object per line, first line the header, last the trailer.
//
// The interchange format. Every line stands alone, so a dump can be grepped,
// split, streamed through a pipe, and diffed against another one without a
// parser — which is most of what an export is for. It is also the only format
// here that survives being edited by hand, which matters more than it sounds:
// the reason to take a graph out of an engine is usually to change something
// about it before putting it back.
//
// # Why encoding/json, in a zero-dependency engine
//
// Same argument the backup manifest makes. The alternative is a hand-rolled
// parser over caller-supplied text, and this package's whole job is reading
// files it did not write. encoding/json is stdlib, so it costs no dependency,
// and it is the one place where not hand-rolling is clearly the safer choice.
//
// []byte marshals to base64 without being asked, which is what makes the opaque
// property blob round-trip exactly. A hand-rolled encoder would have had to
// choose an escaping and get it right.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/aoiflux/graphene/store"
)

// jsonlLine is one line of a dump. The tag comes first so a reader can see what
// it is holding before it looks at anything else, and every other field is
// omitempty so a node's line does not carry an edge's empty endpoints.
type jsonlLine struct {
	Kind string `json:"kind"`

	// Header fields, on the first line only.
	Format            int        `json:"format,omitempty"`
	OrderedNodeKeys   []string   `json:"orderedNodeKeys,omitempty"`
	OrderedEdgeKeys   []string   `json:"orderedEdgeKeys,omitempty"`
	CompositeNodeKeys [][]string `json:"compositeNodeKeys,omitempty"`
	CompositeEdgeKeys [][]string `json:"compositeEdgeKeys,omitempty"`

	// Record fields.
	ID     uint64   `json:"id,omitempty"`
	Src    uint64   `json:"src,omitempty"`
	Dst    uint64   `json:"dst,omitempty"`
	Labels []uint16 `json:"labels,omitempty"`
	Weight float32  `json:"weight,omitempty"`
	Props  []byte   `json:"props,omitempty"`

	// Property-entry fields.
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`

	// Counts, on the trailer line only.
	//
	// Nested behind a pointer rather than flattened into four omitempty fields,
	// and the reason is what a reader sees. A dump of an empty graph has a
	// trailer of four zeros, so the fields cannot be omitempty — and if they
	// are not, every node line in the file also carries `"nodes":0`, which is a
	// header claiming a count it does not have and a node claiming one that
	// means nothing. One nullable object says exactly what it means: absent on
	// every line but the last, and present with zeros when the graph is empty.
	Counts *Trailer `json:"counts,omitempty"`
}

const (
	kindHeader   = "header"
	kindNode     = "node"
	kindEdge     = "edge"
	kindNodeProp = "node-property"
	kindEdgeProp = "edge-property"
	kindTrailer  = "trailer"
)

// ExportJSONL writes src to w as JSON Lines and reports what it wrote.
//
// w is buffered internally and flushed before this returns; it is not closed,
// because the caller owns it.
func ExportJSONL(w io.Writer, src Source, opts Options) (Summary, error) {
	enc := &jsonlEncoder{w: bufio.NewWriter(w)}
	return walk(src, enc, opts)
}

// ImportJSONL reads a JSON Lines dump into dst.
//
// The destination is written to as the stream is read, so a dump that fails
// half way leaves half a graph. That is deliberate and is the only honest
// option: an import is not a transaction, there is no API that could make one
// span a million records, and pretending otherwise by buffering would move the
// failure from "half imported" to "out of memory". Import into an empty store
// and discard the directory if it fails.
func ImportJSONL(r io.Reader, dst Dest, opts Options) (Summary, error) {
	dec := newJSONLDecoder(r)
	return load(dec, dst, opts)
}

type jsonlEncoder struct {
	w   *bufio.Writer
	enc *json.Encoder
}

func (e *jsonlEncoder) line(l jsonlLine) error {
	if e.enc == nil {
		e.enc = json.NewEncoder(e.w)
	}
	if err := e.enc.Encode(l); err != nil {
		return fmt.Errorf("bulk: write jsonl: %w", err)
	}
	return nil
}

func (e *jsonlEncoder) header(h Header) error {
	return e.line(jsonlLine{
		Kind:              kindHeader,
		Format:            h.Format,
		OrderedNodeKeys:   h.OrderedNodeKeys,
		OrderedEdgeKeys:   h.OrderedEdgeKeys,
		CompositeNodeKeys: h.CompositeNodeKeys,
		CompositeEdgeKeys: h.CompositeEdgeKeys,
	})
}

func (e *jsonlEncoder) node(n *store.Node) error {
	return e.line(jsonlLine{
		Kind:   kindNode,
		ID:     uint64(n.ID),
		Labels: nodeLabelsOut(n.Labels),
		Props:  n.Properties,
	})
}

func (e *jsonlEncoder) edge(ed *store.Edge) error {
	return e.line(jsonlLine{
		Kind:   kindEdge,
		ID:     uint64(ed.ID),
		Src:    uint64(ed.Src),
		Dst:    uint64(ed.Dst),
		Labels: edgeLabelsOut(ed.Labels),
		Weight: ed.Weight,
		Props:  ed.Properties,
	})
}

func (e *jsonlEncoder) nodeProperty(id store.NodeID, key string, value []byte) error {
	return e.line(jsonlLine{Kind: kindNodeProp, ID: uint64(id), Key: key, Value: value})
}

func (e *jsonlEncoder) edgeProperty(id store.EdgeID, key string, value []byte) error {
	return e.line(jsonlLine{Kind: kindEdgeProp, ID: uint64(id), Key: key, Value: value})
}

func (e *jsonlEncoder) trailer(t Trailer) error {
	return e.line(jsonlLine{Kind: kindTrailer, Counts: &t})
}

func (e *jsonlEncoder) Close() error {
	if err := e.w.Flush(); err != nil {
		return fmt.Errorf("bulk: flush jsonl: %w", err)
	}
	return nil
}

type jsonlDecoder struct {
	dec  *json.Decoder
	line int
}

func newJSONLDecoder(r io.Reader) *jsonlDecoder {
	// A json.Decoder over the raw reader, not a line splitter: it consumes one
	// value at a time and never needs a line to fit in a buffer, so a dump whose
	// property blobs push a line past bufio.Scanner's 64 KiB limit still reads.
	// That limit is exactly the kind of ceiling a bulk format must not have.
	return &jsonlDecoder{dec: json.NewDecoder(r)}
}

func (d *jsonlDecoder) read() (jsonlLine, error) {
	var l jsonlLine
	if err := d.dec.Decode(&l); err != nil {
		if err == io.EOF {
			return l, io.EOF
		}
		return l, fmt.Errorf("bulk: jsonl line %d: %w", d.line+1, err)
	}
	d.line++
	return l, nil
}

func (d *jsonlDecoder) header() (Header, error) {
	l, err := d.read()
	if err != nil {
		if err == io.EOF {
			return Header{}, fmt.Errorf("bulk: jsonl dump is empty")
		}
		return Header{}, err
	}
	if l.Kind != kindHeader {
		return Header{}, fmt.Errorf("bulk: jsonl dump starts with %q, not a header", l.Kind)
	}
	return Header{
		Format:            l.Format,
		OrderedNodeKeys:   l.OrderedNodeKeys,
		OrderedEdgeKeys:   l.OrderedEdgeKeys,
		CompositeNodeKeys: l.CompositeNodeKeys,
		CompositeEdgeKeys: l.CompositeEdgeKeys,
	}, nil
}

func (d *jsonlDecoder) next() (record, error) {
	l, err := d.read()
	if err != nil {
		return record{}, err
	}
	switch l.Kind {
	case kindNode:
		return record{kind: recNode, node: &store.Node{
			ID:         store.NodeID(l.ID),
			Labels:     nodeLabelsIn(l.Labels),
			Properties: l.Props,
		}}, nil
	case kindEdge:
		return record{kind: recEdge, edge: &store.Edge{
			ID:         store.EdgeID(l.ID),
			Src:        store.NodeID(l.Src),
			Dst:        store.NodeID(l.Dst),
			Labels:     edgeLabelsIn(l.Labels),
			Weight:     l.Weight,
			Properties: l.Props,
		}}, nil
	case kindNodeProp:
		return record{kind: recNodeProp, nodeID: store.NodeID(l.ID), key: l.Key, value: l.Value}, nil
	case kindEdgeProp:
		return record{kind: recEdgeProp, edgeID: store.EdgeID(l.ID), key: l.Key, value: l.Value}, nil
	case kindTrailer:
		if l.Counts == nil {
			return record{}, fmt.Errorf("bulk: jsonl line %d is a trailer with no counts", d.line)
		}
		return record{kind: recTrailer, trailer: *l.Counts}, nil
	case kindHeader:
		return record{}, fmt.Errorf("bulk: jsonl line %d is a second header", d.line)
	default:
		return record{}, fmt.Errorf("bulk: jsonl line %d has unknown kind %q", d.line, l.Kind)
	}
}

func (d *jsonlDecoder) Close() error { return nil }

// The label conversions. NodeType and EdgeType are uint16 and are written as
// numbers rather than names, deliberately: ParseNodeType accepts a bare numeric
// and String() renders an unregistered one as a number too, so names would be a
// lossy encoding of a type this build has not heard of — which is exactly the
// type a dump from a newer build carries.
func nodeLabelsOut(ls []store.NodeType) []uint16 {
	if len(ls) == 0 {
		return nil
	}
	out := make([]uint16, len(ls))
	for i, l := range ls {
		out[i] = uint16(l)
	}
	return out
}

func edgeLabelsOut(ls []store.EdgeType) []uint16 {
	if len(ls) == 0 {
		return nil
	}
	out := make([]uint16, len(ls))
	for i, l := range ls {
		out[i] = uint16(l)
	}
	return out
}

func nodeLabelsIn(ls []uint16) []store.NodeType {
	if len(ls) == 0 {
		return nil
	}
	out := make([]store.NodeType, len(ls))
	for i, l := range ls {
		out[i] = store.NodeType(l)
	}
	return out
}

func edgeLabelsIn(ls []uint16) []store.EdgeType {
	if len(ls) == 0 {
		return nil
	}
	out := make([]store.EdgeType, len(ls))
	for i, l := range ls {
		out[i] = store.EdgeType(l)
	}
	return out
}
