package main

// Rendering nodes and edges.
//
// One table shape for nodes and one for edges, used by every command that
// returns them — `node list`, `node get`, `edge of`, `traverse bfs`, the lot.
// A reader who has learned to read the output of one has learned to read all of
// them, and a script that parses the JSON of one parses the JSON of all of them.
//
// Properties are reported as a size rather than a value. They are a msgpack
// blob whose schema belongs to whoever built the store — the engine treats them
// as opaque bytes and so does this. Printing a decoded guess would be inventing
// a schema the store never claimed to have; printing the raw blob would fill a
// terminal with binary. The size is the honest thing an inspector can say, and
// -verbose adds the hex for someone who wants to decode it themselves.

import (
	"strings"

	"github.com/aoiflux/graphene/store"
)

// nodeCols and edgeCols are the table shapes.
var (
	nodeCols = []Column{RCol("id"), Col("labels"), RCol("properties")}
	edgeCols = []Column{RCol("id"), RCol("src"), RCol("dst"), Col("labels"),
		RCol("weight"), RCol("properties")}
)

// nodeLabelList and edgeLabelList name a set of labels. Both renderings come
// from Strs, so the human report reads "EvidenceFile,Tag" and the JSON carries
// an array a script can index.
func nodeLabelList(n *store.Node) []string {
	parts := make([]string, len(n.Labels))
	for i, l := range n.Labels {
		parts[i] = l.String()
	}
	return parts
}

func edgeLabelList(e *store.Edge) []string {
	parts := make([]string, len(e.Labels))
	for i, l := range e.Labels {
		parts[i] = l.String()
	}
	return parts
}

// nodeLabels and edgeLabels are the string form, for a sentence rather than a
// field.
func nodeLabels(n *store.Node) string { return strings.Join(nodeLabelList(n), ",") }
func edgeLabels(e *store.Edge) string { return strings.Join(edgeLabelList(e), ",") }

func nodeRow(n *store.Node) []Value {
	return []Value{
		ID(uint64(n.ID)),
		Strs(nodeLabelList(n)),
		Bytes(int64(len(n.Properties))),
	}
}

func edgeRow(e *store.Edge) []Value {
	return []Value{
		ID(uint64(e.ID)),
		ID(uint64(e.Src)),
		ID(uint64(e.Dst)),
		Strs(edgeLabelList(e)),
		Float(float64(e.Weight)),
		Bytes(int64(len(e.Properties))),
	}
}

// nodeTable adds a node table, applying paging client-side.
//
// Used where the whole set is already in hand — a traversal result, a batch get.
// A query that can push the window down to the planner should do that instead
// and not call this: paging after the fact means the store did the work anyway.
func nodeTable(r *Result, title string, ns []*store.Node, p Paging) *Section {
	lo, hi, trunc := p.Window(len(ns))
	s := r.Table(title, nodeCols...)
	for _, n := range ns[lo:hi] {
		s.Row(nodeRow(n)...)
	}
	s.Truncated, s.Total = trunc, len(ns)
	return s
}

// edgeTable adds an edge table, applying paging client-side. See nodeTable.
func edgeTable(r *Result, title string, es []*store.Edge, p Paging) *Section {
	lo, hi, trunc := p.Window(len(es))
	s := r.Table(title, edgeCols...)
	for _, e := range es[lo:hi] {
		s.Row(edgeRow(e)...)
	}
	s.Truncated, s.Total = trunc, len(es)
	return s
}

// propertyDump adds the raw property blobs, for -verbose.
//
// Separate from the table on purpose: a hex blob in a cell destroys the column
// alignment that makes the table readable, and the people who want the bytes
// are not the people scanning a listing.
func propertyDump(r *Result, title string, ids []uint64, blobs [][]byte) {
	any := false
	for _, b := range blobs {
		if len(b) > 0 {
			any = true
			break
		}
	}
	if !any {
		return
	}
	s := r.Notes(title)
	for i, b := range blobs {
		if len(b) == 0 {
			continue
		}
		s.Line("%d  %x", ids[i], b)
	}
}

// missingNote records IDs that were asked for and are not there.
//
// Not a finding. A node that does not exist is a complete and correct answer to
// "does this node exist", and grading it as an incomplete account would put
// `node get -id 999` in the same category as a store that was never signed.
func missingNote(s *Section, label string, missing []uint64) {
	if len(missing) == 0 {
		return
	}
	parts := make([]string, len(missing))
	for i, id := range missing {
		parts[i] = uintString(id)
	}
	s.Add(label, Str(strings.Join(parts, ", ")))
}

func uintString(n uint64) string { return Uint(n).String() }
