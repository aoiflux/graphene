package disk

// Building the image once, in one forward pass.
//
// # The problem this is the answer to
//
// A bounded ingest compacts every MaxDeltaBytes worth of records, and every
// compaction rewrites the whole image. Bytes written therefore go as
// N²·b / (2·chunk) — measured, not arithmetic: docs/MEMORY_MODEL.md §9.9 has
// the amplification rising 6.53× → 11.77× → 21.24× across three doublings of a
// 3.2 KB-record ingest, 1.80× per doubling twice. At the shape that opened this
// programme, ten million records of about 3.2 KB, that is terabytes of writing
// to put down thirty-two gigabytes of data. Memory stopped being the binding
// constraint some way below that size; the rewrite is what is left.
//
// So: an ingest that writes the image once. The records stream straight into it
// as the caller yields them, the index entries accumulate in an external sort
// beside them, and the sections that need the whole picture — GPIX, GPIR, GCPX —
// are written at the end, where the format already puts them.
//
// # What it gives up, stated first because it is the whole trade
//
// It is atomic and not incremental. A crash during a load means the load did not
// happen; there is no partial result and nothing to resume from. The store is
// not readable while it runs, and the handle it was called on is closed: a load
// hands back a reopened store, exactly as CompactAndReopen does, because the
// records it wrote were never registered with the index this handle holds.
//
// And it loads into an empty store. That is a v1 scope and not a property of the
// design — the plan notes that identifiers above the high-water mark make a
// merge with an existing image a concatenation — but merging means re-emitting
// the old image's index entries into the sort, which is a second correctness
// surface with no test to hold it, and the shape this exists for is a fresh
// load. A store that holds anything is refused by name.
//
// # Why this is not a second compaction
//
// It is compactPin, then a build, then compactStreamCommit. The pin, the
// budget gate, the step hooks, the rename-and-retire, the audit record: all of
// it is compaction's, unchanged, because a bulk load and a compaction are the
// same operation reached from different places — write an image, install it,
// retire the log. The one thing that differs is where the records come from, and
// that is the one thing this file adds.
//
// The encoder is the same encoder for the same reason Phase 4 gave: a second one
// would be a second definition of the format, and the test that holds this
// honest — an image byte-identical to the one an incremental ingest of the same
// records produces — would then be comparing two guesses rather than one encoder
// reached two ways.
//
// # The one thing learned during the write and used after it
//
// GPIX's key list. A compaction reads it off the live property index before it
// starts; a load has no index and discovers the keys as records go past. The
// encoder writes every record before it writes any section, so the list is
// complete by the time GPIX asks for it — the same shape as the header counts,
// which are written as zero and patched after the flush, and the same shape as
// the section table offset, which has always been written that way. The sort is
// sealed when the last record has gone past and refuses to be read before that,
// so an encoder that ever reordered the two would fail rather than write a short
// index.

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// ErrBulkLoadNotEmpty refuses a load into a store that already holds records.
var ErrBulkLoadNotEmpty = errors.New("graphene: BulkLoad needs an empty store")

// BulkProperty is one indexed property of one record.
//
// It is what IndexNodeProperty would have filed. A bulk load does not derive
// index entries from the property blob — nothing in the engine does, because
// what is indexed is the caller's decision and not a function of what is
// stored — so a record that should be findable by a key has to say so here.
type BulkProperty struct {
	Key   string
	Value []byte
}

// BulkNode is one node to load.
//
// Labels and Properties are store.Node's, with the same rules: at least one
// label, and Properties is the msgpack blob a reader gets back. Neither is
// retained past the call that adds it.
type BulkNode struct {
	Labels     []store.NodeType
	Properties []byte
	Index      []BulkProperty
}

// BulkEdge is one edge to load. Src and Dst are identifiers AddNode returned
// during the node pass.
type BulkEdge struct {
	Src, Dst   store.NodeID
	Labels     []store.EdgeType
	Weight     float32
	Properties []byte
	Index      []BulkProperty
}

// BulkNodeWriter takes nodes during a load's node pass.
//
// There are two writers rather than one because the image holds every node
// record before every edge record, so a load has a node pass and then an edge
// pass and they cannot interleave. Two types rather than one type that refuses:
// a caller who tries to add an edge while the nodes are still streaming should
// not compile.
type BulkNodeWriter struct {
	b     *bulkBuilder
	yield func(nodeRecord) bool
	// stopped records that the encoder stopped pulling, which it does only when
	// the write has already failed. AddNode then reports that rather than
	// looking like it succeeded.
	stopped bool
}

// BulkEdgeWriter takes edges during a load's edge pass.
type BulkEdgeWriter struct {
	b       *bulkBuilder
	yield   func(rawEdge) bool
	stopped bool
}

// BulkLoad writes an image from nodes and edges and returns the store reopened
// on it.
//
// nodes is called once and must add every node; edges is called once afterwards
// and must add every edge. Identifiers are handed back by AddNode as the nodes
// go past, so the edge pass can name them — which is why a load reads its source
// twice and why a source it cannot read twice has to be staged first.
//
// The receiver is closed, whether the load succeeded or not, and must not be
// used again. On success the returned store is open on the new image. On failure
// the directory is left as it was found, minus a temp file the open path already
// ignores.
//
// See the file comment for what a load gives up. The short form: it is atomic
// and not incremental, it needs an empty store, and it is the only thing here
// that writes a store's data without the WAL in front of it.
func (s *Store) BulkLoad(nodes func(*BulkNodeWriter) error, edges func(*BulkEdgeWriter) error) (*Store, error) {
	return s.BulkLoadCtx(context.Background(), nodes, edges)
}

// BulkLoadCtx is BulkLoad, abandoned if ctx is cancelled.
//
// Cancellation reaches the build and stops there, as it does for a compaction
// and for the same reason: once the commit begins, the flush, the rename and the
// retire are the ordering that makes a crash recoverable and there is no correct
// place to stop in the middle of them.
func (s *Store) BulkLoadCtx(ctx context.Context,
	nodes func(*BulkNodeWriter) error, edges func(*BulkEdgeWriter) error) (*Store, error) {

	if err := s.mustWrite(); err != nil {
		return nil, err
	}
	if nodes == nil || edges == nil {
		return nil, fmt.Errorf("BulkLoad: both passes are required; pass a function that adds nothing for an empty one")
	}

	dir, opts := s.dir, s.openOpts
	if err := s.bulkLoad(ctx, nodes, edges); err != nil {
		// The handle is still whole -- nothing was installed -- so it is closed
		// rather than left for a caller who has been told not to use it.
		s.Close()
		return nil, err
	}
	if err := s.Close(); err != nil {
		return nil, fmt.Errorf("BulkLoad: close: %w", err)
	}
	reopened, err := OpenWithOptions(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("BulkLoad: reopen %s: %w", dir, err)
	}
	return reopened, nil
}

// bulkLoad is the pin, the build and the commit, with the reopen left to the
// caller.
//
// The metrics are a compaction's, and that is a decision rather than reuse. A
// load writes the image and retires the log, which is every part of a compaction
// a metric sink can see, so an operator's existing "bytes written" and "image
// writes" figures stay right without learning a new kind -- and a metric kind
// nobody's sink handles yet would make the one operation in the engine that
// writes a whole store the one operation that reports nothing. The three stage
// gauges are emitted for the same reason: item 0a's attribution is what a caller
// measuring this will reach for, and a load has the same three stages.
//
// What it does cost is that a store's compaction count now includes its loads,
// so the mean image per compaction is pulled down by one entry that was not a
// merge. That is stated where the counter is read; it is the smaller error.
func (s *Store) bulkLoad(ctx context.Context,
	nodes func(*BulkNodeWriter) error, edges func(*BulkEdgeWriter) error) (err error) {

	started := time.Now()
	stageStart := started

	plan, err := s.compactPin()
	if err != nil {
		// Unrecorded, for the reason a compaction's own pin refusals are: this
		// means no load happened, not that one failed.
		return err
	}
	defer s.compactRelease()

	if err := bulkLoadable(plan); err != nil {
		return err
	}
	s.recordCompactStage(store.MetricCompactPin, stageStart, nil)
	if s.metricsOn() {
		stageStart = time.Now()
	}

	b := newBulkBuilder(s, plan, nodes, edges)
	defer b.close()

	var img imageIdentity
	defer func() {
		if s.metricsOn() {
			s.record(store.Metric{
				Kind:     store.MetricCompaction,
				Duration: time.Since(started),
				Count:    int64(img.nodeCount + img.edgeCount),
				// The same figure as Count, and unlike a compaction that is not
				// a coincidence to be papered over: a load examines exactly what
				// it writes. There is no image underneath it and no delta to
				// merge, which is the whole of why it writes what it writes once.
				Examined: int64(img.nodeCount + img.edgeCount),
				Bytes:    s.imageBytes(),
				Err:      err,
			})
		}
	}()

	img, tmpPath, err := b.build(ctx)
	s.recordCompactStage(store.MetricCompactBuild, stageStart, err)
	if err != nil {
		return err
	}
	if s.metricsOn() {
		stageStart = time.Now()
	}
	err = s.compactStreamCommit(plan, img, tmpPath)
	s.recordCompactStage(store.MetricCompactCommit, stageStart, err)
	return err
}

// bulkLoadable refuses a store that holds anything.
//
// Three questions, because "empty" has three meanings here and a load that met
// two of them would write an image missing whatever the third held: no image, no
// delta, and nothing indexed. The index is asked through its key lists, which
// are the keys something is actually filed under -- a declaration alone does not
// put a key there, and a declaration is exactly what a caller is expected to
// have made before loading.
func bulkLoadable(p *compactPlan) error {
	if p.csr != nil && (p.csr.NodeCount() > 0 || p.csr.EdgeCount() > 0) {
		return fmt.Errorf("%w: the image holds %d nodes and %d edges",
			ErrBulkLoadNotEmpty, p.csr.NodeCount(), p.csr.EdgeCount())
	}
	if n, e := len(p.deltaKnownNodes), len(p.deltaKnownEdges); n > 0 || e > 0 {
		return fmt.Errorf("%w: the delta holds %d nodes and %d edges", ErrBulkLoadNotEmpty, n, e)
	}
	if n, e := len(p.propIdx.NodePropKeys()), len(p.propIdx.EdgePropKeys()); n > 0 || e > 0 {
		return fmt.Errorf("%w: the property index holds entries under %d node keys and %d edge keys",
			ErrBulkLoadNotEmpty, n, e)
	}
	return nil
}

// bulkBuilder is one load.
type bulkBuilder struct {
	s   *Store
	p   *compactPlan
	dir string

	addNodes func(*BulkNodeWriter) error
	addEdges func(*BulkEdgeWriter) error

	// props sorts the property triples and comps the composite tuples. Two
	// sorters because they are walked by two different encoders in two different
	// key orders; see bulk_sort.go.
	props *bulkSorter
	comps *bulkSorter

	nodeComposites, edgeComposites [][]string

	// uniqueNode and uniqueEdge are the keys declared unique, as a set.
	//
	// The incremental path enforces uniqueness one write at a time, by asking the
	// index whether anyone else holds the value. A load has no index to ask -- and
	// it does not need one: a duplicate is exactly a value under a unique key
	// with more than one identifier filed against it, and the sort groups by value
	// already. So the constraint is checked where the postings are read, at the
	// cost of one map lookup per key rather than one index probe per record, and
	// it is exact rather than best-effort.
	uniqueNode, uniqueEdge map[string]bool
	// compSlot maps a composite's kind and name to the slot its tuples are filed
	// under, which is its position in the list the encoder will walk.
	compSlot map[bulkCompositeName]uint16

	// members is the per-record scratch the composite tuples are built from:
	// each member key's distinct values for the record being added. Reused
	// across records, so a load allocates it once and not once a record.
	members map[string][]string
	tuple   []string

	nodeCount, edgeCount int

	// err is the first failure from the caller's own function or from a check
	// this file makes, latched because an iter.Seq cannot return one. The walk
	// stops at it and build reports it once the encoder has returned -- while
	// the image is still a temp file nobody has been told about.
	err error
}

// bulkCompositeName identifies one declared composite.
type bulkCompositeName struct {
	name string
	kind uint8
}

func newBulkBuilder(s *Store, p *compactPlan,
	nodes func(*BulkNodeWriter) error, edges func(*BulkEdgeWriter) error) *bulkBuilder {

	dir := s.dir
	b := &bulkBuilder{
		s: s, p: p, dir: dir,
		addNodes:       nodes,
		addEdges:       edges,
		nodeComposites: p.payload.CompositeNodeKeys,
		edgeComposites: p.payload.CompositeEdgeKeys,
		compSlot:       make(map[bulkCompositeName]uint16),
		members:        make(map[string][]string),
		uniqueNode:     setOf(p.propIdx.UniqueNodeKeys()),
		uniqueEdge:     setOf(p.propIdx.UniqueEdgeKeys()),
	}
	b.props = newBulkSorter(dir, p.buffers, nil)
	b.props.keyLess = bulkKeyOrder(b.props)
	b.comps = newBulkSorter(dir, p.buffers, bulkSlotOrder)
	return b
}

// close releases both sorts.
func (b *bulkBuilder) close() {
	b.props.close()
	b.comps.close()
}

// declareComposites pins each declared composite to the slot its walker will ask
// for, in the order the encoder walks them: every node composite, then every
// edge composite. That is writeGCPX's order and the runs region is laid out in
// it, so a load walking any other order would write a correct image with
// different bytes. See bulk_sort.go for why the slot is the thing sorted on even
// though the source list arrives sorted by name as well.
func (b *bulkBuilder) declareComposites() error {
	for _, keys := range b.nodeComposites {
		if err := b.declareComposite(bulkKindNode, keys); err != nil {
			return err
		}
	}
	for _, keys := range b.edgeComposites {
		if err := b.declareComposite(bulkKindEdge, keys); err != nil {
			return err
		}
	}
	return nil
}

func (b *bulkBuilder) declareComposite(kind uint8, keys []string) error {
	name := index.CompositeName(keys)
	slot, err := b.comps.declare(bulkCompositeKey(kind, name))
	if err != nil {
		return err
	}
	b.compSlot[bulkCompositeName{name: name, kind: kind}] = slot
	return nil
}

// bulkCompositeKey is a composite's key in the sort's own table. The kind is in
// it because one store may declare the same key tuple over nodes and over edges,
// and they are two composites with two slots.
func bulkCompositeKey(kind uint8, name string) string {
	return string(rune('0'+kind)) + "\x00" + name
}

// build writes the image and returns what the commit needs.
func (b *bulkBuilder) build(ctx context.Context) (imageIdentity, string, error) {
	var out imageIdentity

	if err := b.declareComposites(); err != nil {
		return out, "", err
	}
	if err := ctx.Err(); err != nil {
		return out, "", err
	}

	// The index sources, built now and read by the encoder after the records.
	// bulkIndexSource explains why a source whose key list is not yet known is
	// safe to hand over here.
	if b.p.indexMode == IndexMapped {
		b.p.payload.MappedIndex = b.gpixSource()
		if len(b.nodeComposites) > 0 || len(b.edgeComposites) > 0 {
			b.p.payload.Composites = b.gcpxSource()
		}
	} else {
		// One walk shared by both sequences, because the encoder drains them in
		// order and the sort can only be drained once.
		w := &bulkWalk{s: b.props}
		b.p.payload.NodeProps = b.propSeq(w)
		b.p.payload.EdgeProps = b.edgePropSeq(w)
	}

	if err := b.p.fireStep(compactStepSerialise); err != nil {
		return out, "", err
	}
	tmpPath := filepath.Join(b.dir, csrFileName+".tmp")
	if err := b.p.fireStep(compactStepWriteTmp); err != nil {
		return out, "", err
	}

	beforeSync := func() error { return b.p.fireStep(compactStepSyncTmp) }
	write := func(f *os.File) error {
		id, err := writeImage(f, b.imageInput(ctx), b.p.payload)
		out = id
		return err
	}
	if err := writeStreamSync(tmpPath, 0600, beforeSync, nil, write); err != nil {
		removeTmpImage(tmpPath)
		if b.err != nil {
			// The caller's own failure, which is what stopped the walk and is
			// what a caller wants reported. The encoder's error is a consequence
			// of the short stream.
			return imageIdentity{}, "", b.err
		}
		return imageIdentity{}, "", fmt.Errorf("BulkLoad: write tmp CSR: %w", err)
	}
	if b.err != nil {
		removeTmpImage(tmpPath)
		return imageIdentity{}, "", b.err
	}
	return out, tmpPath, nil
}

// imageInput points the encoder at the two passes.
//
// Both counts are countUnknown and the high-water marks are read back after the
// records, because a load assigns identifiers as it goes: every one of the four
// is a consequence of the write rather than an input to it, and the encoder
// patches all four in the same pass it has always patched the section table
// offset in.
func (b *bulkBuilder) imageInput(ctx context.Context) imageInput {
	return imageInput{
		nodes:               b.nodeSeqFor(ctx),
		edges:               b.edgeSeqFor(ctx),
		nodeCount:           countUnknown,
		edgeCount:           countUnknown,
		seqHWAfterRecords:   func() (uint64, uint64) { return b.s.nodeSeq.Load(), b.s.edgeSeq.Load() },
		commitSeqHW:         b.p.commitSeqHW,
		lastCompactUnixNano: b.p.compactedAt.UnixNano(),
	}
}

// nodeSeqFor is the node pass as the encoder pulls it.
func (b *bulkBuilder) nodeSeqFor(ctx context.Context) iter.Seq[nodeRecord] {
	return func(yield func(nodeRecord) bool) {
		w := &BulkNodeWriter{b: b, yield: yield}
		if err := b.addNodes(w); err != nil && b.err == nil {
			b.err = fmt.Errorf("BulkLoad: the node pass failed: %w", err)
		}
		if b.err == nil {
			b.err = ctx.Err()
		}
	}
}

// edgeSeqFor is the edge pass, and is where both sorts are sealed: the encoder
// writes every edge record before it writes any section, so this returning is
// exactly "no more index entries can arrive".
func (b *bulkBuilder) edgeSeqFor(ctx context.Context) iter.Seq[rawEdge] {
	return func(yield func(rawEdge) bool) {
		defer b.sealIndex()
		if b.err != nil {
			return
		}
		w := &BulkEdgeWriter{b: b, yield: yield}
		if err := b.addEdges(w); err != nil && b.err == nil {
			b.err = fmt.Errorf("BulkLoad: the edge pass failed: %w", err)
		}
		if b.err == nil {
			b.err = ctx.Err()
		}
	}
}

// AddNode adds one node and returns the identifier it was given.
//
// The identifier comes from the store's own sequence counter, exactly as
// AddNodesBatch's does, and that is what makes a load safe to have running while
// another goroutine writes. It would have been enough to number the records
// 1..N -- the store was empty when the load was pinned -- but then a write that
// landed during the build would take an identifier this load had already used,
// and the two records would collide when the log was replayed over the image. A
// load refuses a store that holds anything; it does not refuse one that is
// *about* to, and the counter is what makes the difference harmless.
func (w *BulkNodeWriter) AddNode(n BulkNode) (store.NodeID, error) {
	if w.stopped {
		return 0, fmt.Errorf("BulkLoad: the image write has already failed")
	}
	if w.b.err != nil {
		return 0, w.b.err
	}
	if len(n.Labels) == 0 {
		return 0, w.b.refuse(fmt.Errorf("BulkLoad: node %d: %w", w.b.nodeCount+1, store.ErrNoLabels))
	}
	w.b.nodeCount++
	id := store.NodeID(w.b.s.nodeSeq.Add(1))
	if err := w.b.index(bulkKindNode, uint64(id), n.Index); err != nil {
		return 0, w.b.refuse(err)
	}
	if !w.yield(nodeRecord{ID: id, Labels: n.Labels, Properties: n.Properties}) {
		w.stopped = true
		return 0, fmt.Errorf("BulkLoad: the image write has already failed")
	}
	return id, nil
}

// AddEdge adds one edge and returns the identifier it was given.
//
// The endpoints are not checked against the nodes that went past. A compaction
// does not check them either -- the image has never held referential integrity
// as an invariant, because a delete is allowed to leave a dangling edge until
// the cascade reaches it -- and checking here would mean holding the node
// identifiers, which is the memory a load exists not to spend.
func (w *BulkEdgeWriter) AddEdge(e BulkEdge) (store.EdgeID, error) {
	if w.stopped {
		return 0, fmt.Errorf("BulkLoad: the image write has already failed")
	}
	if w.b.err != nil {
		return 0, w.b.err
	}
	if len(e.Labels) == 0 {
		return 0, w.b.refuse(fmt.Errorf("BulkLoad: edge %d: %w", w.b.edgeCount+1, store.ErrNoLabels))
	}
	w.b.edgeCount++
	id := store.EdgeID(w.b.s.edgeSeq.Add(1))
	if err := w.b.index(bulkKindEdge, uint64(id), e.Index); err != nil {
		return 0, w.b.refuse(err)
	}
	if !w.yield(rawEdge{
		ID: id, Src: e.Src, Dst: e.Dst,
		Labels: e.Labels, Weight: e.Weight, Properties: e.Properties,
	}) {
		w.stopped = true
		return 0, fmt.Errorf("BulkLoad: the image write has already failed")
	}
	return id, nil
}

// index files one record's entries into both sorts.
//
// Every key is checked before any entry is filed. Halfway through would leave the
// sort holding postings for a record the image is not going to carry -- an
// identifier has already been taken by the time this runs -- and that is an orphan
// posting rather than a missing one, which is the worse of the two: invariant 15.5
// is what stops a query returning a row that is not there.
func (b *bulkBuilder) index(kind uint8, id uint64, props []BulkProperty) error {
	if len(props) == 0 {
		return nil
	}
	for _, p := range props {
		if err := checkPropKey(p.Key); err != nil {
			return fmt.Errorf("BulkLoad: %w", err)
		}
		if _, err := b.props.slot(p.Key); err != nil {
			return err
		}
	}
	for _, p := range props {
		slot, _ := b.props.slot(p.Key)
		b.props.add(kind, slot, p.Value, id)
	}
	return b.composites(kind, id, props)
}

// refuse latches a failure so a caller who ignored the error cannot go on.
//
// AddNode takes an identifier before it can fail, so a record it rejects has
// already consumed one -- and a caller who treated the error as "skip this one"
// would produce an image with a gap the log has no record of, or worse, index
// entries the checks above stopped halfway. Neither is a state to continue from,
// so the load ends here rather than at the caller's discretion.
//
// Not used for the stop the encoder signals by refusing a record: that one means
// the image write has already failed for its own reason, and latching would
// replace the error that explains the outcome with one that does not.
func (b *bulkBuilder) refuse(err error) error {
	if b.err == nil {
		b.err = err
	}
	return err
}

// composites files the tuples this record completes.
//
// A record appears under a composite only when it has a value at every member
// position, and under every tuple in the cross product of its values when a
// position has more than one. Both are compositeIndex's rules -- complete() and
// forEachTuple() -- restated here rather than reached through the index, because
// reaching through the index would mean building the index.
//
// That restatement is the one place a bulk load re-implements a rule instead of
// calling it, and it is why the byte-identity test declares two composites, omits
// a member from every tenth record and indexes a key twice on every seventh: a
// rule stated twice is a rule that can drift, and the test is what stops it.
//
// The completeness test below is a short-circuit and not where the rule lives.
// The cross product enforces it on its own -- a position with no values
// contributes no tuples -- so deleting the test changes nothing but the work done
// for a record that was going to file nothing. It is kept because that record is
// the common case for a sparse composite, and it is documented as a
// short-circuit because a reader who took it for the rule would put the rule in
// the wrong place the next time this is edited.
func (b *bulkBuilder) composites(kind uint8, id uint64, props []BulkProperty) error {
	declared := b.nodeComposites
	if kind == bulkKindEdge {
		declared = b.edgeComposites
	}
	if len(declared) == 0 {
		return nil
	}

	clear(b.members)
	for _, p := range props {
		vals := b.members[p.Key]
		v := string(p.Value)
		dup := false
		for _, have := range vals {
			if have == v {
				dup = true
				break
			}
		}
		if !dup {
			b.members[p.Key] = append(vals, v)
		}
	}

	for _, keys := range declared {
		complete := true
		for _, k := range keys {
			if len(b.members[k]) == 0 {
				complete = false
				break
			}
		}
		if !complete {
			continue
		}
		slot := b.compSlot[bulkCompositeName{name: index.CompositeName(keys), kind: kind}]
		if cap(b.tuple) < len(keys) {
			b.tuple = make([]string, len(keys))
		}
		b.tuple = b.tuple[:len(keys)]
		b.crossProduct(keys, 0, slot, kind, id)
	}
	return nil
}

// crossProduct walks every combination of the record's member values, filing one
// tuple each. Recursive over the member positions, which a composite declares a
// handful of; the breadth is the values at one position, which is one for
// essentially every record.
func (b *bulkBuilder) crossProduct(keys []string, pos int, slot uint16, kind uint8, id uint64) {
	if pos == len(keys) {
		b.comps.add(kind, slot, index.EncodeCompositeTuple(b.tuple), id)
		return
	}
	for _, v := range b.members[keys[pos]] {
		b.tuple[pos] = v
		b.crossProduct(keys, pos+1, slot, kind, id)
	}
}

// setOf turns a key list into a set.
func setOf(keys []string) map[string]bool {
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]bool, len(keys))
	for _, k := range keys {
		out[k] = true
	}
	return out
}

// checkUnique refuses a value more than one record was filed under, for a key
// declared unique.
//
// This is the one constraint a bulk load has to enforce for itself. Everything
// else the incremental path checks -- labels, key names -- is a property of a
// single record and is checked as it arrives; uniqueness is a property of the
// whole load and cannot be known until the last record has gone past. So it is
// checked here, while the image is still a temp file nobody has been told about,
// and the load fails rather than installing a store whose unique key is not.
func (b *bulkBuilder) checkUnique(kind uint8, key string, ids []uint64) error {
	set := b.uniqueNode
	what := "node"
	if kind == bulkKindEdge {
		set, what = b.uniqueEdge, "edge"
	}
	if !set[key] || len(ids) < 2 {
		return nil
	}
	return fmt.Errorf("BulkLoad: %q is a unique %s property and %d records were loaded with the same value "+
		"(%s %d and %s %d among them)", key, what, len(ids), what, ids[0], what, ids[1])
}
