package bulk

// CSV: a directory of tables, for everything that is not this engine.
//
// The other two formats are one stream each because a stream is what a pipe
// and a parser want. CSV is not for a parser — it is for a spreadsheet, a
// notebook, an ETL tool, a `sort | uniq -c`. Those want one file per shape,
// with a header row naming the columns, so that is what this writes:
//
//	nodes.csv             id,labels,properties
//	edges.csv             id,src,dst,labels,weight,properties
//	node_properties.csv   id,key,value
//	edge_properties.csv   id,key,value
//	manifest.csv          field,value
//
// # Two encodings that are not obvious, and why
//
// **Properties are base64, not raw and not hex.** The blob is opaque bytes and
// CSV is text; anything that does not round-trip arbitrary bytes exactly is not
// an option, which rules out writing them raw. Between hex and base64, base64
// is a third smaller on a field that dominates the file. It is not meant to be
// read by eye — nothing in that column is, because the engine itself cannot
// parse it.
//
// **Labels are semicolon-separated numbers.** A record carries several, so the
// column is a list; a comma would need quoting in a comma-separated file, and a
// quoted list is worse for every tool that would otherwise split on the
// separator. Numbers rather than names, for the reason jsonl.go gives: a name
// is lossy for a type this build has not been told about, and a dump from a
// newer build is exactly where that happens.
//
// # manifest.csv is written last and read first
//
// It carries the format, the index declarations, and the record counts, which
// makes it both the header and the trailer of the other three formats. Written
// last so that a directory holding one is a directory whose export finished —
// the same argument graphene.backup.json makes — and its counts still catch a
// data file truncated between rows, which is the failure no row-level check can
// see.

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aoiflux/graphene/store"
)

// File names inside a CSV export directory.
const (
	CSVNodes          = "nodes.csv"
	CSVEdges          = "edges.csv"
	CSVNodeProperties = "node_properties.csv"
	CSVEdgeProperties = "edge_properties.csv"
	CSVManifest       = "manifest.csv"
)

// ExportCSV writes src into dir as a set of CSV tables.
//
// dir is created if absent and must not already hold a manifest: overwriting
// half of an export in place would leave a directory whose manifest describes
// one graph and whose tables hold two.
func ExportCSV(dir string, src Source, opts Options) (Summary, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Summary{}, fmt.Errorf("bulk: mkdir %s: %w", dir, err)
	}
	if _, err := os.Stat(filepath.Join(dir, CSVManifest)); err == nil {
		return Summary{}, fmt.Errorf("bulk: %s already holds a CSV export", dir)
	}
	enc, err := newCSVEncoder(dir)
	if err != nil {
		return Summary{}, err
	}
	sum, werr := walk(src, enc, opts)
	if werr != nil {
		enc.Close()
		return sum, werr
	}
	return sum, nil
}

// ImportCSV reads a CSV export directory into dst. See ImportJSONL on what a
// failure part way through leaves behind.
func ImportCSV(dir string, dst Dest, opts Options) (Summary, error) {
	dec, err := newCSVDecoder(dir)
	if err != nil {
		return Summary{}, err
	}
	sum, lerr := load(dec, dst, opts)
	if lerr != nil {
		dec.Close()
	}
	return sum, lerr
}

// --- encoding ---

type csvTable struct {
	f *os.File
	w *csv.Writer
}

func newCSVTable(dir, name string, columns []string) (*csvTable, error) {
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("bulk: create %s: %w", name, err)
	}
	t := &csvTable{f: f, w: csv.NewWriter(f)}
	if err := t.w.Write(columns); err != nil {
		f.Close()
		return nil, fmt.Errorf("bulk: write %s header: %w", name, err)
	}
	return t, nil
}

func (t *csvTable) close() error {
	t.w.Flush()
	if err := t.w.Error(); err != nil {
		t.f.Close()
		return err
	}
	return t.f.Close()
}

type csvEncoder struct {
	dir     string
	nodes   *csvTable
	edges   *csvTable
	nprops  *csvTable
	eprops  *csvTable
	hdr     Header
	closed  bool
	rowbuf  []string
	written Trailer
}

func newCSVEncoder(dir string) (*csvEncoder, error) {
	e := &csvEncoder{dir: dir, rowbuf: make([]string, 0, 6)}
	var err error
	if e.nodes, err = newCSVTable(dir, CSVNodes, []string{"id", "labels", "properties"}); err != nil {
		return nil, err
	}
	if e.edges, err = newCSVTable(dir, CSVEdges, []string{"id", "src", "dst", "labels", "weight", "properties"}); err != nil {
		return nil, err
	}
	if e.nprops, err = newCSVTable(dir, CSVNodeProperties, []string{"id", "key", "value"}); err != nil {
		return nil, err
	}
	if e.eprops, err = newCSVTable(dir, CSVEdgeProperties, []string{"id", "key", "value"}); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *csvEncoder) header(h Header) error {
	// Held rather than written: the manifest goes out last, so that a directory
	// containing one is a directory whose export finished.
	e.hdr = h
	return nil
}

func (e *csvEncoder) row(t *csvTable, fields ...string) error {
	if err := t.w.Write(fields); err != nil {
		return fmt.Errorf("bulk: write csv row: %w", err)
	}
	return nil
}

func (e *csvEncoder) node(n *store.Node) error {
	return e.row(e.nodes,
		strconv.FormatUint(uint64(n.ID), 10),
		joinNodeLabels(n.Labels),
		base64.StdEncoding.EncodeToString(n.Properties))
}

func (e *csvEncoder) edge(ed *store.Edge) error {
	return e.row(e.edges,
		strconv.FormatUint(uint64(ed.ID), 10),
		strconv.FormatUint(uint64(ed.Src), 10),
		strconv.FormatUint(uint64(ed.Dst), 10),
		joinEdgeLabels(ed.Labels),
		// 'g' with -1 precision is the shortest text that parses back to the
		// same float32. Anything fixed-width either loses a value or pads every
		// row for the sake of the rare one.
		strconv.FormatFloat(float64(ed.Weight), 'g', -1, 32),
		base64.StdEncoding.EncodeToString(ed.Properties))
}

func (e *csvEncoder) nodeProperty(id store.NodeID, key string, value []byte) error {
	return e.row(e.nprops, strconv.FormatUint(uint64(id), 10), key,
		base64.StdEncoding.EncodeToString(value))
}

func (e *csvEncoder) edgeProperty(id store.EdgeID, key string, value []byte) error {
	return e.row(e.eprops, strconv.FormatUint(uint64(id), 10), key,
		base64.StdEncoding.EncodeToString(value))
}

func (e *csvEncoder) trailer(t Trailer) error {
	e.written = t
	return nil
}

// Close flushes the tables and only then writes the manifest, so a manifest
// exists exactly when everything it describes is on disk.
func (e *csvEncoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	for _, t := range []*csvTable{e.nodes, e.edges, e.nprops, e.eprops} {
		if t == nil {
			continue
		}
		if err := t.close(); err != nil {
			return fmt.Errorf("bulk: close csv table: %w", err)
		}
	}
	return e.writeManifest()
}

func (e *csvEncoder) writeManifest() error {
	m, err := newCSVTable(e.dir, CSVManifest, []string{"field", "value"})
	if err != nil {
		return err
	}
	rows := [][2]string{
		{"format", strconv.Itoa(e.hdr.Format)},
		{"nodes", strconv.FormatInt(e.written.Nodes, 10)},
		{"edges", strconv.FormatInt(e.written.Edges, 10)},
		{"nodeProperties", strconv.FormatInt(e.written.NodeProperties, 10)},
		{"edgeProperties", strconv.FormatInt(e.written.EdgeProperties, 10)},
	}
	for _, k := range e.hdr.OrderedNodeKeys {
		rows = append(rows, [2]string{"orderedNodeKey", k})
	}
	for _, k := range e.hdr.OrderedEdgeKeys {
		rows = append(rows, [2]string{"orderedEdgeKey", k})
	}
	// A tuple is a list inside a cell, so it takes the same semicolon the label
	// column does. A property key containing a semicolon would break that, and
	// is rejected on the way in rather than written out ambiguously.
	for _, t := range e.hdr.CompositeNodeKeys {
		rows = append(rows, [2]string{"compositeNodeKeys", strings.Join(t, ";")})
	}
	for _, t := range e.hdr.CompositeEdgeKeys {
		rows = append(rows, [2]string{"compositeEdgeKeys", strings.Join(t, ";")})
	}
	for _, r := range rows {
		if strings.Contains(r[1], ";") && !strings.HasPrefix(r[0], "composite") {
			return fmt.Errorf("bulk: cannot write %s %q to CSV: a semicolon is the list separator", r[0], r[1])
		}
		if err := m.w.Write([]string{r[0], r[1]}); err != nil {
			m.f.Close()
			return fmt.Errorf("bulk: write manifest: %w", err)
		}
	}
	return m.close()
}

func joinNodeLabels(ls []store.NodeType) string {
	if len(ls) == 0 {
		return ""
	}
	var b strings.Builder
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(strconv.FormatUint(uint64(l), 10))
	}
	return b.String()
}

func joinEdgeLabels(ls []store.EdgeType) string {
	if len(ls) == 0 {
		return ""
	}
	var b strings.Builder
	for i, l := range ls {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(strconv.FormatUint(uint64(l), 10))
	}
	return b.String()
}

// --- decoding ---

// csvDecoder reads the four tables in the order the ordering rule requires,
// then emits the manifest's counts as a trailer.
type csvDecoder struct {
	dir   string
	hdr   Header
	trail Trailer

	stage int // which table is being read
	cur   *os.File
	rd    *csv.Reader
	line  int
	name  string

	// emitted marks the one trailer as delivered. Without it the stream never
	// ends: a decoder that answers "trailer" whenever it is past the last table
	// answers it forever, and load's loop only stops on io.EOF.
	emitted bool
}

func newCSVDecoder(dir string) (*csvDecoder, error) {
	d := &csvDecoder{dir: dir}
	if err := d.readManifest(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *csvDecoder) readManifest() error {
	f, err := os.Open(filepath.Join(d.dir, CSVManifest))
	if err != nil {
		return fmt.Errorf("bulk: %s is not a CSV export (%w)", d.dir, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	// The manifest's rows are all two columns, but FieldsPerRecord is left
	// unfixed so a manifest written by a later version with a third column is a
	// clear error here rather than a silent misread.
	rows, err := r.ReadAll()
	if err != nil {
		return fmt.Errorf("bulk: read manifest: %w", err)
	}
	if len(rows) == 0 || len(rows[0]) != 2 || rows[0][0] != "field" {
		return fmt.Errorf("bulk: %s is not a Graphene manifest", CSVManifest)
	}
	for _, row := range rows[1:] {
		if len(row) != 2 {
			return fmt.Errorf("bulk: manifest row %q has %d columns, want 2", row, len(row))
		}
		field, value := row[0], row[1]
		var err error
		switch field {
		case "format":
			d.hdr.Format, err = strconv.Atoi(value)
		case "nodes":
			d.trail.Nodes, err = strconv.ParseInt(value, 10, 64)
		case "edges":
			d.trail.Edges, err = strconv.ParseInt(value, 10, 64)
		case "nodeProperties":
			d.trail.NodeProperties, err = strconv.ParseInt(value, 10, 64)
		case "edgeProperties":
			d.trail.EdgeProperties, err = strconv.ParseInt(value, 10, 64)
		case "orderedNodeKey":
			d.hdr.OrderedNodeKeys = append(d.hdr.OrderedNodeKeys, value)
		case "orderedEdgeKey":
			d.hdr.OrderedEdgeKeys = append(d.hdr.OrderedEdgeKeys, value)
		case "compositeNodeKeys":
			d.hdr.CompositeNodeKeys = append(d.hdr.CompositeNodeKeys, strings.Split(value, ";"))
		case "compositeEdgeKeys":
			d.hdr.CompositeEdgeKeys = append(d.hdr.CompositeEdgeKeys, strings.Split(value, ";"))
		default:
			// Ignored, not fatal. A later version adding a manifest field it
			// does not need this reader to understand should not make its
			// exports unreadable — the same rule the optional CSR sections
			// follow.
		}
		if err != nil {
			return fmt.Errorf("bulk: manifest field %q: %w", field, err)
		}
	}
	return nil
}

func (d *csvDecoder) header() (Header, error) { return d.hdr, nil }

// stages names the tables in the order they must be read, which is the ordering
// rule expressed as a list.
var csvStages = []struct {
	name    string
	columns int
	kind    recordKind
}{
	{CSVNodes, 3, recNode},
	{CSVEdges, 6, recEdge},
	{CSVNodeProperties, 3, recNodeProp},
	{CSVEdgeProperties, 3, recEdgeProp},
}

func (d *csvDecoder) openStage() error {
	s := csvStages[d.stage]
	f, err := os.Open(filepath.Join(d.dir, s.name))
	if err != nil {
		return fmt.Errorf("bulk: open %s: %w", s.name, err)
	}
	r := csv.NewReader(f)
	r.FieldsPerRecord = s.columns
	// Skip the column header. Its contents are not checked against the expected
	// names: FieldsPerRecord already pins the shape, and a caller who renamed a
	// column but kept the order has produced a file this can still read.
	if _, err := r.Read(); err != nil && err != io.EOF {
		f.Close()
		return fmt.Errorf("bulk: read %s header: %w", s.name, err)
	}
	d.cur, d.rd, d.name, d.line = f, r, s.name, 1
	return nil
}

func (d *csvDecoder) next() (record, error) {
	for {
		if d.stage >= len(csvStages) {
			// The manifest is this format's trailer, so it is emitted once —
			// after the last table — and the stream ends immediately after.
			if d.emitted {
				return record{}, io.EOF
			}
			d.emitted = true
			return record{kind: recTrailer, trailer: d.trail}, nil
		}
		if d.rd == nil {
			if err := d.openStage(); err != nil {
				return record{}, err
			}
		}
		row, err := d.rd.Read()
		if err == io.EOF {
			d.closeStage()
			d.stage++
			continue
		}
		if err != nil {
			return record{}, fmt.Errorf("bulk: %s line %d: %w", d.name, d.line+1, err)
		}
		d.line++
		return d.decodeRow(csvStages[d.stage].kind, row)
	}
}

func (d *csvDecoder) decodeRow(kind recordKind, row []string) (record, error) {
	fail := func(err error) (record, error) {
		return record{}, fmt.Errorf("bulk: %s line %d: %w", d.name, d.line, err)
	}
	switch kind {
	case recNode:
		id, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil {
			return fail(err)
		}
		labels, err := splitLabels(row[1])
		if err != nil {
			return fail(err)
		}
		props, err := base64.StdEncoding.DecodeString(row[2])
		if err != nil {
			return fail(err)
		}
		return record{kind: recNode, node: &store.Node{
			ID:         store.NodeID(id),
			Labels:     nodeLabelsIn(labels),
			Properties: emptyToNil(props),
		}}, nil

	case recEdge:
		id, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil {
			return fail(err)
		}
		src, err := strconv.ParseUint(row[1], 10, 64)
		if err != nil {
			return fail(err)
		}
		dstID, err := strconv.ParseUint(row[2], 10, 64)
		if err != nil {
			return fail(err)
		}
		labels, err := splitLabels(row[3])
		if err != nil {
			return fail(err)
		}
		w, err := strconv.ParseFloat(row[4], 32)
		if err != nil {
			return fail(err)
		}
		props, err := base64.StdEncoding.DecodeString(row[5])
		if err != nil {
			return fail(err)
		}
		return record{kind: recEdge, edge: &store.Edge{
			ID:         store.EdgeID(id),
			Src:        store.NodeID(src),
			Dst:        store.NodeID(dstID),
			Labels:     edgeLabelsIn(labels),
			Weight:     float32(w),
			Properties: emptyToNil(props),
		}}, nil

	case recNodeProp, recEdgeProp:
		id, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil {
			return fail(err)
		}
		value, err := base64.StdEncoding.DecodeString(row[2])
		if err != nil {
			return fail(err)
		}
		if kind == recNodeProp {
			return record{kind: recNodeProp, nodeID: store.NodeID(id), key: row[1], value: value}, nil
		}
		return record{kind: recEdgeProp, edgeID: store.EdgeID(id), key: row[1], value: value}, nil
	}
	return fail(fmt.Errorf("unexpected record kind %d", kind))
}

func (d *csvDecoder) closeStage() {
	if d.cur != nil {
		d.cur.Close()
	}
	d.cur, d.rd = nil, nil
}

func (d *csvDecoder) Close() error {
	d.closeStage()
	return nil
}

// emptyToNil keeps a nil Properties nil across the round trip.
//
// base64 of no bytes decodes to an empty non-nil slice, so without this a
// record that had no blob comes back holding a zero-length one.
//
// **Removing it survives every test here, and that is not a gap.** Both bundled
// stores copy a blob only when len(...) > 0, so an empty slice handed to AddNode
// is stored as nil anyway and the difference is invisible from outside. It stays
// because the decoder's contract should not rest on a normalisation the store
// happens to perform: a backend that kept what it was given would silently start
// producing empty-blob records, and this file would be the reason.
func emptyToNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func splitLabels(s string) ([]uint16, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ";")
	out := make([]uint16, len(parts))
	for i, p := range parts {
		v, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("label %q: %w", p, err)
		}
		out[i] = uint16(v)
	}
	return out, nil
}
