package bulk

// graphene_dump: the native format.
//
// CSV and JSONL exist to be read by something else. This exists to be read by
// this engine, quickly and exactly, and it is what a caller should reach for
// when the destination is another Graphene store: the record it decodes is the
// record that was encoded, with no text in between to escape, quote, parse or
// round.
//
// # Framing
//
// Deliberately the WAL's framing, not a new one:
//
//	["GDMP"][format:2]
//	then, repeated: [type:1][length:4][payload:length][crc32:4]
//
// The CRC covers the type, the length and the payload — the same three the WAL
// covers, for the same reason. A checksum over the payload alone leaves the
// framing itself unprotected, so a corrupted length is read as a valid length
// and takes the reader somewhere arbitrary before anything notices.
//
// # Reading a file this package did not write
//
// Every length is bounded against the bytes actually available before it is
// used to size anything, which is the rule readOrderedKeySection follows in the
// CSR reader. A record's payload is read into a slice sized by its own length
// field, so that field is checked against the declared record length first, and
// the record length is checked against maxRecordBytes before a single
// allocation happens.
//
// **This parser has no fuzz target.** New fuzz work is deferred, and this is
// what the deferral costs: a hand-rolled binary reader over
// attacker-controllable lengths is precisely what CONTRIBUTING §2 points at.
// The bounds above and the CRC are the defence, and they are an argument rather
// than evidence. It is the first thing to fuzz when that deferral lifts.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/aoiflux/graphene/store"
)

var dumpMagic = [4]byte{'G', 'D', 'M', 'P'}

// maxRecordBytes bounds one record's payload.
//
// 64 MiB is far past any single node — a property blob that large is a file,
// not a property — and far short of a length field's range, which is the point:
// a corrupted length must fail as a refusal rather than as an allocation the
// process cannot make. Without it, a flipped bit in a length field is an
// out-of-memory that looks like the caller's fault.
const maxRecordBytes = 64 << 20

const (
	dumpHeader uint8 = 1
	dumpNode   uint8 = 2
	dumpEdge   uint8 = 3
	dumpNProp  uint8 = 4
	dumpEProp  uint8 = 5
	dumpTrail  uint8 = 6
)

// ErrNotADump is returned when a stream does not begin with the magic.
var ErrNotADump = errors.New("bulk: not a graphene_dump stream")

// ExportDump writes src to w in the native format.
func ExportDump(w io.Writer, src Source, opts Options) (Summary, error) {
	enc := &dumpEncoder{w: bufio.NewWriter(w)}
	return walk(src, enc, opts)
}

// ImportDump reads a native dump into dst. See ImportJSONL on what a failure
// part way through leaves behind.
func ImportDump(r io.Reader, dst Dest, opts Options) (Summary, error) {
	dec, err := newDumpDecoder(r)
	if err != nil {
		return Summary{}, err
	}
	return load(dec, dst, opts)
}

// --- encoding ---

type dumpEncoder struct {
	w    *bufio.Writer
	buf  []byte // reused payload scratch
	rec  []byte // reused frame scratch
	done bool
}

func (e *dumpEncoder) frame(t uint8, payload []byte) error {
	if len(payload) > maxRecordBytes {
		return fmt.Errorf("bulk: record of %d bytes exceeds the %d-byte limit", len(payload), maxRecordBytes)
	}
	e.rec = e.rec[:0]
	e.rec = append(e.rec, t)
	e.rec = binary.BigEndian.AppendUint32(e.rec, uint32(len(payload)))
	e.rec = append(e.rec, payload...)
	sum := crc32.ChecksumIEEE(e.rec)
	e.rec = binary.BigEndian.AppendUint32(e.rec, sum)
	if _, err := e.w.Write(e.rec); err != nil {
		return fmt.Errorf("bulk: write dump: %w", err)
	}
	return nil
}

func appendBytes32(dst, b []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b)))
	return append(dst, b...)
}

func appendString16(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func (e *dumpEncoder) header(h Header) error {
	if _, err := e.w.Write(dumpMagic[:]); err != nil {
		return fmt.Errorf("bulk: write dump: %w", err)
	}
	var pre [2]byte
	binary.BigEndian.PutUint16(pre[:], uint16(h.Format))
	if _, err := e.w.Write(pre[:]); err != nil {
		return fmt.Errorf("bulk: write dump: %w", err)
	}

	e.buf = e.buf[:0]
	e.buf = appendStrings(e.buf, h.OrderedNodeKeys)
	e.buf = appendStrings(e.buf, h.OrderedEdgeKeys)
	e.buf = appendTuples(e.buf, h.CompositeNodeKeys)
	e.buf = appendTuples(e.buf, h.CompositeEdgeKeys)
	return e.frame(dumpHeader, e.buf)
}

func appendStrings(dst []byte, ss []string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(ss)))
	for _, s := range ss {
		dst = appendString16(dst, s)
	}
	return dst
}

func appendTuples(dst []byte, ts [][]string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(ts)))
	for _, t := range ts {
		dst = appendStrings(dst, t)
	}
	return dst
}

func (e *dumpEncoder) node(n *store.Node) error {
	e.buf = e.buf[:0]
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(n.ID))
	e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(len(n.Labels)))
	for _, l := range n.Labels {
		e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(l))
	}
	e.buf = appendBytes32(e.buf, n.Properties)
	return e.frame(dumpNode, e.buf)
}

func (e *dumpEncoder) edge(ed *store.Edge) error {
	e.buf = e.buf[:0]
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(ed.ID))
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(ed.Src))
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(ed.Dst))
	e.buf = binary.BigEndian.AppendUint32(e.buf, floatBits(ed.Weight))
	e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(len(ed.Labels)))
	for _, l := range ed.Labels {
		e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(l))
	}
	e.buf = appendBytes32(e.buf, ed.Properties)
	return e.frame(dumpEdge, e.buf)
}

func (e *dumpEncoder) prop(t uint8, id uint64, key string, value []byte) error {
	e.buf = e.buf[:0]
	e.buf = binary.BigEndian.AppendUint64(e.buf, id)
	e.buf = appendString16(e.buf, key)
	e.buf = appendBytes32(e.buf, value)
	return e.frame(t, e.buf)
}

func (e *dumpEncoder) nodeProperty(id store.NodeID, key string, value []byte) error {
	return e.prop(dumpNProp, uint64(id), key, value)
}

func (e *dumpEncoder) edgeProperty(id store.EdgeID, key string, value []byte) error {
	return e.prop(dumpEProp, uint64(id), key, value)
}

func (e *dumpEncoder) trailer(t Trailer) error {
	e.buf = e.buf[:0]
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(t.Nodes))
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(t.Edges))
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(t.NodeProperties))
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(t.EdgeProperties))
	e.done = true
	return e.frame(dumpTrail, e.buf)
}

func (e *dumpEncoder) Close() error {
	if err := e.w.Flush(); err != nil {
		return fmt.Errorf("bulk: flush dump: %w", err)
	}
	return nil
}

// --- decoding ---

type dumpDecoder struct {
	r      *bufio.Reader
	format int
	hdr    []byte
	n      int64 // records read, for error messages
}

func newDumpDecoder(r io.Reader) (*dumpDecoder, error) {
	br := bufio.NewReader(r)
	var magic [4]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotADump, err)
	}
	if magic != dumpMagic {
		return nil, fmt.Errorf("%w: magic is %q", ErrNotADump, magic[:])
	}
	var pre [2]byte
	if _, err := io.ReadFull(br, pre[:]); err != nil {
		return nil, fmt.Errorf("bulk: dump has no format: %w", err)
	}
	return &dumpDecoder{r: br, format: int(binary.BigEndian.Uint16(pre[:]))}, nil
}

// readFrame reads one record and verifies its checksum.
//
// The length is bounded before it is used to allocate, which is the whole of
// what stands between a corrupted byte and an allocation the process cannot
// make.
func (d *dumpDecoder) readFrame() (uint8, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(d.r, head[:]); err != nil {
		if err == io.EOF {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("bulk: dump record %d: %w", d.n+1, err)
	}
	length := binary.BigEndian.Uint32(head[1:5])
	if length > maxRecordBytes {
		return 0, nil, fmt.Errorf("bulk: dump record %d declares %d bytes, past the %d-byte limit",
			d.n+1, length, maxRecordBytes)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(d.r, payload); err != nil {
		return 0, nil, fmt.Errorf("bulk: dump record %d is truncated: %w", d.n+1, err)
	}
	var tail [4]byte
	if _, err := io.ReadFull(d.r, tail[:]); err != nil {
		return 0, nil, fmt.Errorf("bulk: dump record %d has no checksum: %w", d.n+1, err)
	}

	h := crc32.NewIEEE()
	h.Write(head[:])
	h.Write(payload)
	if got, want := h.Sum32(), binary.BigEndian.Uint32(tail[:]); got != want {
		return 0, nil, fmt.Errorf("bulk: dump record %d fails its checksum (%08x != %08x)", d.n+1, got, want)
	}
	d.n++
	return head[0], payload, nil
}

func (d *dumpDecoder) header() (Header, error) {
	t, payload, err := d.readFrame()
	if err != nil {
		if err == io.EOF {
			return Header{}, errors.New("bulk: dump has no header")
		}
		return Header{}, err
	}
	if t != dumpHeader {
		return Header{}, fmt.Errorf("bulk: dump starts with record type %d, not a header", t)
	}
	rd := &cursor{b: payload}
	h := Header{Format: d.format}
	if h.OrderedNodeKeys, err = rd.strings(); err != nil {
		return Header{}, err
	}
	if h.OrderedEdgeKeys, err = rd.strings(); err != nil {
		return Header{}, err
	}
	if h.CompositeNodeKeys, err = rd.tuples(); err != nil {
		return Header{}, err
	}
	if h.CompositeEdgeKeys, err = rd.tuples(); err != nil {
		return Header{}, err
	}
	d.hdr = payload
	return h, nil
}

func (d *dumpDecoder) next() (record, error) {
	t, payload, err := d.readFrame()
	if err != nil {
		return record{}, err
	}
	rd := &cursor{b: payload}
	switch t {
	case dumpNode:
		id, err := rd.u64()
		if err != nil {
			return record{}, err
		}
		labels, err := rd.labels()
		if err != nil {
			return record{}, err
		}
		props, err := rd.bytes32()
		if err != nil {
			return record{}, err
		}
		return record{kind: recNode, node: &store.Node{
			ID:         store.NodeID(id),
			Labels:     nodeLabelsIn(labels),
			Properties: props,
		}}, nil

	case dumpEdge:
		id, err := rd.u64()
		if err != nil {
			return record{}, err
		}
		src, err := rd.u64()
		if err != nil {
			return record{}, err
		}
		dstID, err := rd.u64()
		if err != nil {
			return record{}, err
		}
		w, err := rd.u32()
		if err != nil {
			return record{}, err
		}
		labels, err := rd.labels()
		if err != nil {
			return record{}, err
		}
		props, err := rd.bytes32()
		if err != nil {
			return record{}, err
		}
		return record{kind: recEdge, edge: &store.Edge{
			ID:         store.EdgeID(id),
			Src:        store.NodeID(src),
			Dst:        store.NodeID(dstID),
			Weight:     floatFrom(w),
			Labels:     edgeLabelsIn(labels),
			Properties: props,
		}}, nil

	case dumpNProp, dumpEProp:
		id, err := rd.u64()
		if err != nil {
			return record{}, err
		}
		key, err := rd.string16()
		if err != nil {
			return record{}, err
		}
		value, err := rd.bytes32()
		if err != nil {
			return record{}, err
		}
		if t == dumpNProp {
			return record{kind: recNodeProp, nodeID: store.NodeID(id), key: key, value: value}, nil
		}
		return record{kind: recEdgeProp, edgeID: store.EdgeID(id), key: key, value: value}, nil

	case dumpTrail:
		var tr Trailer
		for _, f := range []*int64{&tr.Nodes, &tr.Edges, &tr.NodeProperties, &tr.EdgeProperties} {
			v, err := rd.u64()
			if err != nil {
				return record{}, err
			}
			*f = int64(v)
		}
		return record{kind: recTrailer, trailer: tr}, nil

	case dumpHeader:
		return record{}, fmt.Errorf("bulk: dump record %d is a second header", d.n)

	default:
		// Unknown record types are refused rather than skipped. A dump is not
		// the CSR container: there is no optional-section flag here saying which
		// records a reader may ignore and still be correct, so a type this build
		// does not know is a record whose absence might change the graph.
		return record{}, fmt.Errorf("bulk: dump record %d has unknown type %d", d.n, t)
	}
}

func (d *dumpDecoder) Close() error { return nil }

// cursor reads a payload, refusing to run past its end.
//
// Every read checks the bytes remaining before it consumes any, so a length
// field inside a payload cannot reach beyond the payload the frame's own length
// and checksum already established.
type cursor struct {
	b   []byte
	off int
}

func (c *cursor) take(n int) ([]byte, error) {
	if n < 0 || c.off+n > len(c.b) {
		return nil, fmt.Errorf("bulk: dump payload wants %d bytes at offset %d of %d", n, c.off, len(c.b))
	}
	out := c.b[c.off : c.off+n]
	c.off += n
	return out, nil
}

func (c *cursor) u16() (uint16, error) {
	b, err := c.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (c *cursor) u32() (uint32, error) {
	b, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (c *cursor) u64() (uint64, error) {
	b, err := c.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (c *cursor) string16() (string, error) {
	n, err := c.u16()
	if err != nil {
		return "", err
	}
	b, err := c.take(int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// bytes32 copies rather than aliasing. The payload slice is reused by the
// caller's next read, and a property blob handed to the store outlives it.
func (c *cursor) bytes32() ([]byte, error) {
	n, err := c.u32()
	if err != nil {
		return nil, err
	}
	b, err := c.take(int(n))
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	return bytes.Clone(b), nil
}

func (c *cursor) labels() ([]uint16, error) {
	n, err := c.u16()
	if err != nil {
		return nil, err
	}
	// Bounded by the payload before allocating: two bytes per label, so a count
	// needing more than what remains is a corrupt count and not a large record.
	if int(n)*2 > len(c.b)-c.off {
		return nil, fmt.Errorf("bulk: dump record claims %d labels, more than the payload holds", n)
	}
	out := make([]uint16, n)
	for i := range out {
		if out[i], err = c.u16(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *cursor) strings() ([]string, error) {
	n, err := c.u32()
	if err != nil {
		return nil, err
	}
	// Each string costs at least its two length bytes, so a count past that
	// bound cannot be honest whatever the strings turn out to be.
	if int64(n)*2 > int64(len(c.b)-c.off) {
		return nil, fmt.Errorf("bulk: dump header claims %d strings, more than the payload holds", n)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]string, n)
	for i := range out {
		if out[i], err = c.string16(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *cursor) tuples() ([][]string, error) {
	n, err := c.u32()
	if err != nil {
		return nil, err
	}
	if int64(n)*4 > int64(len(c.b)-c.off) {
		return nil, fmt.Errorf("bulk: dump header claims %d tuples, more than the payload holds", n)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([][]string, n)
	for i := range out {
		if out[i], err = c.strings(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// floatBits and floatFrom move a weight through the format without going near
// a decimal string.
//
// Weight is the one non-integer field a record carries, and text is where a
// float loses. The IEEE bit pattern is exact and is the same 4 bytes on every
// platform this builds for, which is what makes a dump written on one readable
// on another.
func floatBits(f float32) uint32 { return math.Float32bits(f) }

func floatFrom(u uint32) float32 { return math.Float32frombits(u) }
