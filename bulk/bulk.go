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
//
// # Why this rule is what keeps an import off disk.Store.BulkLoad
//
// A bulk load writes a whole store in one forward pass and would be the right
// engine for an import into an empty directory. It cannot be driven from a dump,
// and the reason is this section rather than anything in the loader: a load takes
// a record's index entries *with* the record, and the property section here is
// written in (key, value, id) order, which index.PropertyIndex.ForEachNodeProperty
// states as a contract. So one record's entries are scattered across the whole
// section, one per key, and gathering them means holding every triple — the memory
// a load exists not to spend. Composites are worse: they are built per record from
// that record's whole property set, so re-ordering a scattered stream does not
// reconstruct one.
//
// Closing that gap is a Format 2 carrying each record's entries inline, which
// index.NodeEntriesOf already produces on the export side, plus reading the dump
// twice so the node pass can finish before the edge pass begins. It is a format
// change, not a switch at the destination, and it is not made here.
package bulk

import (
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// Format is the dump format version written by an export. It appears in every
// header, and an importer refuses anything it does not recognise rather than
// guessing.
//
// # 2: a record can carry its own index entries
//
// Format 1 put every indexed property entry in a section of its own, after all
// the records, in the (key, value, id) order PropertyEnumerator walks. That is
// the right order for a streamed export and the wrong one for a streamed import
// into an empty store: disk.Store.BulkLoad takes a record's entries *with* the
// record, so reassembling them from a key-major section means holding every
// triple, which is the memory a one-pass load exists not to spend.
//
// Format 2 lets a record carry its entries inline. The separate section is still
// legal and still read, so 2 is a superset of 1 rather than a replacement, and
// Header.InlineEntries says which shape a given dump actually used -- known
// before the first record, so a load that needs the inline shape refuses at the
// header instead of half way through.
//
// Only graphene_dump carries entries inline. CSV and JSONL are tabular and
// line-oriented and a nested entry list fits neither; they write Format 2 in
// their headers with InlineEntries false, which is a truthful description of a
// dump whose records simply do not use the new field.
const Format = 2

// MinReadableFormat is the oldest dump this build reads. Every format from here
// to Format is accepted, because a dump is a backup shape as well as a transfer
// shape and refusing to read one this project wrote is a data-loss bug rather
// than a compatibility policy.
const MinReadableFormat = 1

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

	// InlineEntries reports that every record in this dump carries its own
	// index entries and that there is no separate property section.
	//
	// It is in the header rather than inferred from the records because the one
	// caller that needs to know is the one that cannot recover from finding out
	// late: a bulk load into an empty store commits nothing until the whole
	// stream has been read, so "this dump is not the shape I need" has to be
	// answerable before the first record rather than after the last.
	//
	// False on a Format 1 dump, on every CSV and JSONL dump, and on a Format 2
	// dump whose source could not group its entries by entity.
	InlineEntries bool `json:"inlineEntries,omitempty"`
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

	// MaxBatchBytes commits a batch once its records hold this many bytes,
	// whichever of the two limits is reached first. Zero caps nothing by bytes.
	//
	// BatchSize counts records, which is the wrong unit as soon as records vary
	// in size: a thousand records is a few hundred kilobytes of small nodes and
	// a gigabyte of large ones, and a figure chosen for the first is a memory
	// event on the second. A dump is exactly where the caller does not know
	// which they have, because they are importing it.
	//
	// The two are both enforced for the reason disk.Options.MaxBatchBytes gives:
	// a byte cap alone does not bound a million empty nodes, and a record cap
	// alone does not bound ten records carrying a gigabyte each.
	//
	// Counted as the destination counts it, near enough: the record structures
	// plus their labels and property blobs. This package is backend-agnostic and
	// cannot ask the store what it will charge, so the figure is an estimate
	// used to decide when to flush and never reported as a measurement.
	MaxBatchBytes int64

	// Compact is evaluated after every batch, and a destination that can compact
	// is compacted when it fires. The zero policy never fires, which is what an
	// import did before this existed.
	//
	// An import with no schedule accumulates the whole dump in the delta and the
	// index before anything folds it into an image, so its peak follows the size
	// of the dump rather than the size of a batch. That is fine for a dump that
	// fits and is the failure mode for one that does not. See
	// store.DefaultCompactionPolicy and, for what to set it to against a
	// ceiling, docs/USER_GUIDE.md.
	//
	// The destination must report StorageStats and offer Compact for this to do
	// anything; both bundled backends do. One that does not is not an error and
	// the schedule is silently inert, the same position every other optional
	// fast-path in this package takes.
	Compact store.CompactionPolicy

	// Reopen, when set, is called after each compaction Compact fired, and
	// returns the destination the import goes on writing to.
	//
	// # Why this is a callback and not a flag
	//
	// A compaction writes a new image and then goes on serving the records it
	// wrote from the heap, because a base is attached on the load path and a
	// compaction is not one. Reopening after one is what gives those bytes back,
	// and it is the difference between an ingest that holds a stated ceiling and
	// one that ratchets past it -- docs/MEMORY_MODEL.md section 9.8 measured a
	// rebuild that died holding 661.8 MiB of payload it had already written.
	//
	// But reopening closes the handle, and the handle belongs to the caller: it
	// is the thing they will use after this returns, and this package cannot
	// reach it. So the caller performs the reopen and hands back what to write
	// to next. disk.Store.CompactAndReopen is the call to make inside it.
	//
	// Returning an error stops the import. Returning a nil Dest is an error for
	// the same reason -- there would be nothing to write the rest of the dump
	// to.
	Reopen func(Dest) (Dest, error)
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

// compactor is the optional schedule fast-path, in the same shape and for the
// same reason as the two batchers above: a destination that offers both halves
// can be kept bounded during the import, and one that does not is imported
// exactly as it was before.
//
// Two methods rather than a CompactIfDue, because those are the two the disk
// backend actually exposes on the store -- the paired helper lives a layer up on
// graphene.Graph, and asserting for it here would mean the wrapper satisfied the
// interface and the thing the wrapper is holding did not.
type compactor interface {
	StorageStats() store.StorageStats
	Compact() error
}

// inlineEncoder is the optional half of Format 2: an encoder that can take a
// record together with the entries it is indexed under.
//
// Optional, and asserted for rather than added to encoder, for the reason the
// format constant gives -- CSV and JSONL have nowhere sensible to put a nested
// list, and making them carry a method that returns an error would be spelling
// "not supported" twice. Same shape as nodeBatcher and compactor above: a
// capability the walk uses when both ends have it.
type inlineEncoder interface {
	nodeWith(n *store.Node, entries []store.PropertyEntry) error
	edgeWith(e *store.Edge, entries []store.PropertyEntry) error
}

// nodeBatchBytes estimates what a batch of nodes holds.
//
// The constant is the disk backend's empty-record cost -- a version cell and a
// record header -- and the labels are two bytes each, which is what a NodeType
// is. It is an estimate and is documented as one on Options.MaxBatchBytes: this
// package writes to an interface and cannot ask what the implementation on the
// other side will charge. What it has to be is monotonic in the record's size,
// and it is.
func nodeBatchBytes(n *store.Node) int64 {
	return 80 + int64(len(n.Properties)) + int64(len(n.Labels))*2
}

// edgeBatchBytes is nodeBatchBytes for edges, whose empty cost is higher by the
// two endpoints and the weight.
func edgeBatchBytes(e *store.Edge) int64 {
	return 104 + int64(len(e.Properties)) + int64(len(e.Labels))*2
}

// maybeCompact runs the schedule after a batch, returning the destination to go
// on writing to.
//
// Returns dst unchanged when nothing fired, when the destination cannot compact,
// or when no reopen hook was given -- three quiet paths and one that does work,
// which is the shape that lets the caller assign unconditionally.
func maybeCompact(dst Dest, opts Options) (Dest, error) {
	// Before the assertion and before StorageStats, which on the disk backend is
	// a read lock and a reach into the log and the property index. An import
	// with no schedule runs this once per batch and it must cost a comparison,
	// so that "behaves exactly as it did before" is true of the work as well as
	// of the result.
	if opts.Compact == (store.CompactionPolicy{}) {
		return dst, nil
	}
	c, ok := dst.(compactor)
	if !ok {
		return dst, nil
	}
	if due, _ := opts.Compact.Evaluate(c.StorageStats()); !due {
		return dst, nil
	}
	if opts.Reopen == nil {
		if err := c.Compact(); err != nil {
			return dst, fmt.Errorf("bulk: interim compaction: %w", err)
		}
		return dst, nil
	}
	// The hook compacts as well as reopens -- disk.Store.CompactAndReopen is one
	// call and doing the compaction here first would do it twice.
	next, err := opts.Reopen(dst)
	if err != nil {
		return dst, fmt.Errorf("bulk: reopen after interim compaction: %w", err)
	}
	if next == nil {
		return dst, fmt.Errorf("bulk: Options.Reopen returned no destination")
	}
	return next, nil
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
	return ApplyDeclarations(dst, h)
}

// checkFormat rejects a dump this build cannot read.
func checkFormat(got int) error {
	if got < MinReadableFormat || got > Format {
		return fmt.Errorf("%w: dump is format %d, this build reads %d to %d",
			ErrUnsupportedFormat, got, MinReadableFormat, Format)
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
