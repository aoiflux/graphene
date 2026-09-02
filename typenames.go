package graphene

// The label table, written beside the image.
//
// # Why persist it at all
//
// store.RegisterNodeTypeName makes an application's custom labels render as
// names instead of Custom(7), which is worth having. Persisting the table is
// worth more, and for a different reason: it makes the store **self-describing**.
//
// A graphene directory records that node 4 981 is type 32 768. What 32 768 means
// lives in the application that wrote it, so a store outliving that application —
// or examined by anything else, which for a forensics engine is the normal case
// rather than the exception — is a graph of numbers. Worse, an application whose
// own table has drifted since the data was written reads every one of those
// numbers as the wrong thing, confidently and with no symptom.
//
// # Why a sidecar and not the image
//
// Because the image format does not change. graphene.labels joins
// graphene.grants, graphene.redactions, graphene.audit and graphene.checkpoints
// beside graphene.csr: an older version of the engine ignores a file it does not
// know about, a newer one reads it, and a store written either way is readable
// by both. Backup picks it up with no change, because backup copies the
// directory rather than a list of names it knows.
//
// # What Open does with it, and why it is strict
//
// It registers what the file says, and a disagreement with what this process
// already believes is an error rather than a resolution. The registry is
// process-wide (store/typenames.go says why), so two stores in one process that
// disagree about what 32 768 means cannot both be rendered correctly — and
// picking one silently is exactly the confident wrong answer this exists to
// prevent. Open them in separate processes, or reconcile the numbering.
//
// A store with no label table opens exactly as it always did.

import (
	"bufio"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/aoiflux/graphene/store"
)

// typeNamesFileName is the label table's name in the store directory.
const typeNamesFileName = "graphene.labels"

// typeNamesHeader is the first line, so a reader can tell the file apart from
// anything that happens to share its name and can refuse a generation it does
// not understand rather than misreading it.
const typeNamesHeader = "graphene-labels v1"

// dirNamer is any store that can say where it lives. Both disk stores can; the
// in-memory one has nowhere to write, which is why DeclareTypeNames registers
// there and persists nothing.
type dirNamer interface{ Dir() string }

// DeclareTypeNames names custom node and edge types, and records the naming in
// the store so the graph stays interpretable without this program.
//
//	err := g.DeclareTypeNames(
//	    map[store.NodeType]string{nodeApp: "App", nodeVersion: "AppVersion"},
//	    map[store.EdgeType]string{edgeOwns: "Owns"},
//	)
//
// The names are what String() renders and what ParseNodeType and ParseEdgeType
// accept, in addition to — never instead of — the Custom(7), custom:7 and
// bare-numeric forms. Only the custom range can be named.
//
// Declaring the same names again is a no-op, so this belongs beside the other
// declarations at every Open. A name that disagrees with one already in force
// returns store.ErrTypeNameConflict and nothing is registered or written: two
// namings of one number mean a table has drifted from the data it describes, and
// the engine has no basis for choosing between them.
//
// On a disk store the table is written to graphene.labels beside the image. It
// is not part of the image format, so a store carrying one opens unchanged in an
// engine that predates this. On the in-memory backend the names are registered
// and nothing is written, there being nowhere to write it.
func (g *Graph) DeclareTypeNames(nodes map[store.NodeType]string, edges map[store.EdgeType]string) error {
	// Registration first, and in a deterministic order so a conflicting table
	// reports the same entry every time rather than whichever the map handed
	// over first.
	for _, t := range slices.Sorted(maps.Keys(nodes)) {
		if err := store.RegisterNodeTypeName(t, nodes[t]); err != nil {
			return fmt.Errorf("DeclareTypeNames: %w", err)
		}
	}
	for _, t := range slices.Sorted(maps.Keys(edges)) {
		if err := store.RegisterEdgeTypeName(t, edges[t]); err != nil {
			return fmt.Errorf("DeclareTypeNames: %w", err)
		}
	}

	d, ok := g.GraphStore.(dirNamer)
	if !ok {
		return nil
	}

	// Merged with what the file already holds rather than written from the
	// process registry: the registry may carry names declared for a different
	// store, and this file describes this one.
	haveNodes, haveEdges, err := readTypeNames(d.Dir())
	if err != nil {
		return fmt.Errorf("DeclareTypeNames: %w", err)
	}
	for t, name := range nodes {
		haveNodes[t] = name
	}
	for t, name := range edges {
		haveEdges[t] = name
	}
	if err := writeTypeNames(d.Dir(), haveNodes, haveEdges); err != nil {
		return fmt.Errorf("DeclareTypeNames: %w", err)
	}
	return nil
}

// TypeNames returns the custom node and edge names currently in force, each a
// copy safe to keep.
//
// Process-wide rather than per-store: see store/typenames.go.
func (g *Graph) TypeNames() (nodes map[store.NodeType]string, edges map[store.EdgeType]string) {
	return store.NodeTypeNames(), store.EdgeTypeNames()
}

// loadTypeNames registers the table a store directory carries, if it carries
// one. Called by every Open.
func loadTypeNames(gs store.GraphStore) error {
	d, ok := gs.(dirNamer)
	if !ok {
		return nil
	}
	nodes, edges, err := readTypeNames(d.Dir())
	if err != nil {
		return err
	}
	for _, t := range slices.Sorted(maps.Keys(nodes)) {
		if err := store.RegisterNodeTypeName(t, nodes[t]); err != nil {
			return fmt.Errorf("%s: %w", typeNamesFileName, err)
		}
	}
	for _, t := range slices.Sorted(maps.Keys(edges)) {
		if err := store.RegisterEdgeTypeName(t, edges[t]); err != nil {
			return fmt.Errorf("%s: %w", typeNamesFileName, err)
		}
	}
	return nil
}

// readTypeNames parses the label table in dir. A missing file is two empty maps
// and no error: most stores have never named anything.
func readTypeNames(dir string) (map[store.NodeType]string, map[store.EdgeType]string, error) {
	nodes := make(map[store.NodeType]string)
	edges := make(map[store.EdgeType]string)

	f, err := os.Open(filepath.Join(dir, typeNamesFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nodes, edges, nil
		}
		return nil, nil, fmt.Errorf("read %s: %w", typeNamesFileName, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return nil, nil, fmt.Errorf("read %s: %w", typeNamesFileName, err)
		}
		return nil, nil, fmt.Errorf("read %s: file is empty", typeNamesFileName)
	}
	if got := sc.Text(); got != typeNamesHeader {
		// A generation this build does not know rather than a file to guess at.
		// Guessing is how a label table becomes a mislabelling.
		return nil, nil, fmt.Errorf("read %s: unrecognised header %q, want %q",
			typeNamesFileName, got, typeNamesHeader)
	}

	for line := 2; sc.Scan(); line++ {
		text := sc.Text()
		if text == "" {
			continue
		}
		kind, value, name, err := parseTypeNameLine(text)
		if err != nil {
			return nil, nil, fmt.Errorf("read %s line %d: %w", typeNamesFileName, line, err)
		}
		switch kind {
		case "node":
			nodes[store.NodeType(value)] = name
		case "edge":
			edges[store.EdgeType(value)] = name
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", typeNamesFileName, err)
	}
	return nodes, edges, nil
}

func parseTypeNameLine(text string) (kind string, value uint16, name string, err error) {
	parts := strings.Split(text, "\t")
	if len(parts) != 3 {
		return "", 0, "", fmt.Errorf("want three tab-separated fields, got %d", len(parts))
	}
	kind = parts[0]
	if kind != "node" && kind != "edge" {
		return "", 0, "", fmt.Errorf("unknown kind %q", kind)
	}
	n, convErr := strconv.ParseUint(parts[1], 10, 16)
	if convErr != nil {
		return "", 0, "", fmt.Errorf("type value %q: %w", parts[1], convErr)
	}
	if parts[2] == "" {
		return "", 0, "", fmt.Errorf("type %d has an empty name", n)
	}
	return kind, uint16(n), parts[2], nil
}

// writeTypeNames replaces the label table atomically.
//
// Written to a temporary and renamed, because a half-written label table is a
// mislabelling that survives — and .tmp is what backup already skips.
func writeTypeNames(dir string, nodes map[store.NodeType]string, edges map[store.EdgeType]string) error {
	var b strings.Builder
	b.WriteString(typeNamesHeader)
	b.WriteByte('\n')
	// Nodes then edges, each by ascending value, so one table produces one file
	// and two stores with the same naming produce the same bytes.
	for _, t := range slices.Sorted(maps.Keys(nodes)) {
		fmt.Fprintf(&b, "node\t%d\t%s\n", uint16(t), nodes[t])
	}
	for _, t := range slices.Sorted(maps.Keys(edges)) {
		fmt.Fprintf(&b, "edge\t%d\t%s\n", uint16(t), edges[t])
	}

	final := filepath.Join(dir, typeNamesFileName)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", typeNamesFileName, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", typeNamesFileName, err)
	}
	return nil
}
