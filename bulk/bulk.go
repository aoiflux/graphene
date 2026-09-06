// Package bulk moves a whole graph in and out of a store.
//
// Three formats, one model. CSV and JSONL exist because the data has to leave
// the engine sometimes — for a spreadsheet, a notebook, another database, a
// diff between two runs — and a store whose only export is its own image file
// is a store nobody can check. graphene_dump exists because those two are lossy
// about types and slow about volume, and a backup is not an export: a backup is
// the same store's bytes and can only be read by this engine at this version.
//
// # What a dump contains, and why it is more than the records
//
// Nodes, edges, and **indexed property entries**. The third is the one that is
// easy to leave out and impossible to recover afterwards. A record's Properties
// field is an opaque blob — msgpack, JSON, anything; the engine never parses it
// — and the values in the property index were handed to IndexNodeProperty
// separately by a caller who knew how to derive them. A dump carrying only the
// records restores a graph that looks complete and answers nothing, and no
// amount of RebuildIndexes fixes it, because rebuilding repairs structure and
// not content.
//
// Index *declarations* travel too — ordered keys and composite tuples. They do
// not change any answer, only the plan that produces it, so leaving them out
// would make an imported store quietly slower than the one it came from.
//
// # What a dump does not preserve, stated plainly
//
// **Identity.** A store assigns IDs; nothing can ask it for a particular one,
// and adding a way to would break the monotonic-and-never-reused property that
// the WAL, the CSR and every ID in every ledger depend on. So an import
// allocates fresh IDs and rewrites every reference to them — edge endpoints and
// property entries alike — through a map built as the nodes arrive. The
// imported graph is isomorphic to the exported one, not identical to it.
//
// That is why the ordering rule below is a rule and not a convention.
//
// **History.** A dump is the graph as it stands, not how it got there: no WAL,
// no audit log, no redaction ledger, no custody chain, no signatures. Anything
// that needs those needs Backup, which copies the store rather than describing
// it.
//
// # The ordering rule
//
// Nodes, then edges, then property entries. An edge cannot be added before its
// endpoints exist and a property entry cannot be filed against an ID that has
// not been mapped, so an importer reading a stream in any other order would
// have to buffer the whole graph to make sense of it. Every exporter here
// writes that order; every importer checks it and refuses a stream that does
// not, rather than half-loading one.
package bulk

import (
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// Format is the dump format version. It appears in every header, and an
// importer refuses anything it does not recognise rather than guessing.
const Format = 1

// ErrUnsupportedFormat is returned when a dump names a format this build cannot
// read. It names both numbers, because "unsupported" without them sends an
// operator to the source.
var ErrUnsupportedFormat = errors.New("bulk: unsupported dump format")

// ErrOutOfOrder is returned when a stream presents an edge before its endpoints
// or a property entry before its entity.
//
// A refusal rather than a buffer-and-retry. Reordering on the importer's side
// would mean holding the whole graph in memory to import a graph that is
// already too large to hold, and silently succeeding on a stream that no
// exporter here produces would make a third-party writer's bug into this
// package's problem later.
var ErrOutOfOrder = errors.New("bulk: records out of order")

// Header describes a dump. It is written first and read first.
type Header struct {
	Format int `json:"format"`

	// OrderedNodeKeys and the three fields after it are the index declarations
	// the source store carried. They change plans, not answers, so an import
	// that cannot apply them is degraded rather than wrong — see Options.
	OrderedNodeKeys   []string   `json:"orderedNodeKeys,omitempty"`
	OrderedEdgeKeys   []string   `json:"orderedEdgeKeys,omitempty"`
	CompositeNodeKeys [][]string `json:"compositeNodeKeys,omitempty"`
	CompositeEdgeKeys [][]string `json:"compositeEdgeKeys,omitempty"`
}

// Trailer closes a dump with what it actually contained.
//
// The counts are here rather than in the header, and the reason is the whole of
// what they are worth. A header count has to be known before the first record
// is written, so it can only come from asking the store how many records it
// thinks it has — which is a second source that can disagree with the walk, and
// a disagreement would fail a perfectly good dump. Counted as they are written,
// they are what was written by construction.
//
// It is also what makes truncation detectable. A dump cut mid-record is caught
// by the decoder, because the bytes stop inside a length the record declared. A
// dump cut *between* records is not: every record that arrived is well formed,
// and only the absence of the trailer says the rest is missing. Same argument
// as graphene.backup.json being written last.
type Trailer struct {
	Nodes          int64 `json:"nodes"`
	Edges          int64 `json:"edges"`
	NodeProperties int64 `json:"nodeProperties"`
	EdgeProperties int64 `json:"edgeProperties"`
}

// Summary reports what an export wrote or an import read.
type Summary struct {
	Nodes          int64
	Edges          int64
	NodeProperties int64
	EdgeProperties int64

	// Declarations is how many ordered keys and composite tuples were applied.
	// Zero on an import that was told to skip them.
	Declarations int
}

// Options tunes an import. The zero value is the sensible one.
type Options struct {
	// BatchSize is how many records are committed at once. Zero means
	// DefaultBatchSize.
	//
	// It is a trade between commits and memory, and the default is chosen for
	// the shape an import has: one fsync per batch, so a batch of one is an
	// fsync per node and a batch of a million is a million records buffered
	// before anything is durable.
	BatchSize int

	// SkipDeclarations leaves the destination's index declarations alone.
	//
	// Off by default, because a dump that carries declarations and an import
	// that ignores them produces a store that answers every query correctly and
	// slowly, which is the kind of difference nobody notices until they measure
	// it. Turn it on when importing into a store whose declarations are already
	// managed elsewhere.
	SkipDeclarations bool

	// SkipProperties drops the indexed property entries.
	//
	// Off by default. Turning it on produces a store whose records are complete
	// and whose property queries return nothing — occasionally what a caller
	// wants when re-deriving entries from the blobs themselves, and never what
	// they want by accident, which is why it has to be asked for.
	SkipProperties bool
}

// DefaultBatchSize is how many records an import commits at once when Options
// does not say.
const DefaultBatchSize = 1000

func (o Options) batchSize() int {
	if o.BatchSize > 0 {
		return o.BatchSize
	}
	return DefaultBatchSize
}

// Source is what an export reads from. Both bundled backends satisfy it, and a
// store.Snapshot does not — deliberately: a snapshot cannot enumerate the
// property index, which is most of what makes a dump complete.
type Source interface {
	QueryNodeIDs(q store.NodeQuery) ([]store.NodeID, error)
	QueryEdgeIDs(q store.EdgeQuery) ([]store.EdgeID, error)
	GetNode(id store.NodeID) (*store.Node, error)
	GetEdge(id store.EdgeID) (*store.Edge, error)
}

// Dest is what an import writes to.
type Dest interface {
	AddNode(n *store.Node) (store.NodeID, error)
	AddEdge(e *store.Edge) (store.EdgeID, error)
	IndexNodeProperty(id store.NodeID, key string, value []byte) error
	IndexEdgeProperty(id store.EdgeID, key string, value []byte) error
}

// nodeBatcher and edgeBatcher are the optional fast paths. A store that has
// them commits a whole batch in one framed WAL write; one that does not falls
// back to a record at a time, which is correct and slower.
type nodeBatcher interface {
	AddNodesBatch(nodes []*store.Node) ([]store.NodeID, error)
}

type edgeBatcher interface {
	AddEdgesBatch(edges []*store.Edge) ([]store.EdgeID, error)
}

// header reads the declarations off a source that reports them. A source that
// does not is not an error: the dump simply carries none, and an import of it
// leaves the destination's own declarations alone.
//
// The assertion is on the reporter halves and not on the declarers, because a
// Snapshot is the source an export should be taking — it streams, and it fixes
// what the dump is a dump of — and a view is deliberately not allowed to
// declare. Asserting the full declarer here meant a snapshot source silently
// wrote a header with no declarations in it, so the dump imported without its
// ordered and composite indexes.
func declarationsOf(src Source) (ordNodes, ordEdges []string, compNodes, compEdges [][]string) {
	if d, ok := src.(store.OrderedIndexReporter); ok {
		ordNodes = d.OrderedNodeProperties()
		ordEdges = d.OrderedEdgeProperties()
	}
	if d, ok := src.(store.CompositeIndexReporter); ok {
		compNodes = d.CompositeNodeProperties()
		compEdges = d.CompositeEdgeProperties()
	}
	return
}

// applyDeclarations re-declares on dst what the header carried.
//
// A destination that cannot take one is not an error. Declarations affect the
// plan and never the result, so an import into a backend without composite
// indexes is slower and complete, and failing it would be refusing a correct
// import over a performance hint.
func applyDeclarations(dst Dest, h Header, opts Options) int {
	if opts.SkipDeclarations {
		return 0
	}
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
			// A tuple this build will not accept is skipped, not fatal — the
			// same rule the GCMP section follows on open, and for the same
			// reason: a reader ignoring a declaration still answers every query
			// correctly.
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

// checkFormat rejects a dump this build cannot read.
func checkFormat(got int) error {
	if got != Format {
		return fmt.Errorf("%w: dump is format %d, this build reads %d", ErrUnsupportedFormat, got, Format)
	}
	return nil
}

// idMap rewrites the exported IDs onto the ones the destination assigned.
//
// A map rather than a slice, because exported IDs are not dense: a store that
// has deleted anything has holes, and a dump of a filtered subset has more
// holes than records.
type idMap struct {
	nodes map[store.NodeID]store.NodeID
	edges map[store.EdgeID]store.EdgeID
}

func newIDMap() *idMap {
	return &idMap{
		nodes: make(map[store.NodeID]store.NodeID),
		edges: make(map[store.EdgeID]store.EdgeID),
	}
}

func (m *idMap) node(old store.NodeID) (store.NodeID, error) {
	id, ok := m.nodes[old]
	if !ok {
		return 0, fmt.Errorf("%w: node %d is referenced before it is defined", ErrOutOfOrder, old)
	}
	return id, nil
}

func (m *idMap) edge(old store.EdgeID) (store.EdgeID, error) {
	id, ok := m.edges[old]
	if !ok {
		return 0, fmt.Errorf("%w: edge %d is referenced before it is defined", ErrOutOfOrder, old)
	}
	return id, nil
}

// check compares what a dump said it held against what was read out of it.
func (t Trailer) check(got Summary) error {
	type pair struct {
		what       string
		want, have int64
	}
	for _, p := range []pair{
		{"nodes", t.Nodes, got.Nodes},
		{"edges", t.Edges, got.Edges},
		{"node properties", t.NodeProperties, got.NodeProperties},
		{"edge properties", t.EdgeProperties, got.EdgeProperties},
	} {
		if p.want != p.have {
			return fmt.Errorf("bulk: dump's trailer declares %d %s but %d were read; it is truncated or was written by a broken writer",
				p.want, p.what, p.have)
		}
	}
	return nil
}

// ErrNoPropertyEntries is returned by an export whose source cannot enumerate
// its indexed property entries.
//
// A refusal rather than a dump without them. Those entries are not derivable
// from anything else in the dump — the record's Properties blob is opaque to
// the engine — so a dump missing them restores a graph that looks complete and
// answers no property query, which is the one failure this package exists to
// prevent. Options.SkipProperties is how a caller asks for that on purpose.
var ErrNoPropertyEntries = errors.New("bulk: source cannot enumerate property entries; pass the backing store, or set Options.SkipProperties")
