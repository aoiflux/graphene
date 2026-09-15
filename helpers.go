package graphene

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/aoiflux/graphene/store"
	"github.com/aoiflux/graphene/traversal"
)

type nodeBatchAdder interface {
	AddNodesBatch(nodes []*store.Node) ([]store.NodeID, error)
}

type edgeBatchAdder interface {
	AddEdgesBatch(edges []*store.Edge) ([]store.EdgeID, error)
}

// GraphStats holds high-level statistics about the graph.
type GraphStats struct {
	NodeCount uint64
	EdgeCount uint64

	// NodesByType and EdgesByType break the totals down by label. Valid only
	// when HasTypeCounts is true; a backend that cannot enumerate its own types
	// has nothing to report, and an empty map would say the graph is empty.
	//
	// An entity carrying two labels is counted under both, so these sum to more
	// than NodeCount and EdgeCount on any graph using multi-label entities.
	NodesByType   map[store.NodeType]uint64
	EdgesByType   map[store.EdgeType]uint64
	HasTypeCounts bool

	// Storage describes what the backend is holding — delta size, log size, and
	// when it last compacted. Valid only when HasStorage is true; backends
	// without a delta layer or a log have nothing to report.
	Storage    store.StorageStats
	HasStorage bool
}

// Stats returns high-level counts for the graph, and storage detail where the
// backend can supply it.
func (g *Graph) Stats() (*GraphStats, error) {
	nc, err := g.NodeCount()
	if err != nil {
		return nil, err
	}
	ec, err := g.EdgeCount()
	if err != nil {
		return nil, err
	}
	out := &GraphStats{NodeCount: nc, EdgeCount: ec}
	if sr, ok := g.GraphStore.(store.StorageReporter); ok {
		out.Storage = sr.StorageStats()
		out.HasStorage = true
	}
	if a, ok := g.GraphStore.(store.Aggregator); ok {
		ctx := context.Background()
		nodes, err := a.CountNodesByType(ctx)
		if err != nil {
			return nil, err
		}
		edges, err := a.CountEdgesByType(ctx)
		if err != nil {
			return nil, err
		}
		out.NodesByType, out.EdgesByType, out.HasTypeCounts = nodes, edges, true
	}
	return out, nil
}

// --- Aggregation ---
//
// The counts a caller cannot get cheaply from outside. Pulling IDs into Go and
// counting them there is fine at a thousand entities and stops being fine well
// before a hundred thousand: a per-type breakdown that way is one NodesByType
// call per type and a copy of every ID in the graph, built to be discarded.
//
// These return an error on a backend that cannot answer them, rather than an
// empty map. There is no generic fallback to degrade to — a GraphStore cannot
// enumerate its own types — and an empty map would say the graph is empty.
// Both bundled backends implement them.

// CountNodesByType returns the number of live nodes carrying each type.
//
// A node carrying two labels is counted under both, so the values sum to more
// than NodeCount on any graph using multi-label nodes. Types with no live nodes
// are absent rather than zero.
func (g *Graph) CountNodesByType() (map[store.NodeType]uint64, error) {
	return g.CountNodesByTypeCtx(context.Background())
}

// CountNodesByTypeCtx is CountNodesByType, cancellable.
func (g *Graph) CountNodesByTypeCtx(ctx context.Context) (map[store.NodeType]uint64, error) {
	a, ok := g.GraphStore.(store.Aggregator)
	if !ok {
		return nil, fmt.Errorf("CountNodesByType: %T cannot count by type", g.GraphStore)
	}
	return a.CountNodesByType(ctx)
}

// CountEdgesByType returns the number of live edges carrying each type, with the
// same multi-label caveat as CountNodesByType.
func (g *Graph) CountEdgesByType() (map[store.EdgeType]uint64, error) {
	return g.CountEdgesByTypeCtx(context.Background())
}

// CountEdgesByTypeCtx is CountEdgesByType, cancellable.
func (g *Graph) CountEdgesByTypeCtx(ctx context.Context) (map[store.EdgeType]uint64, error) {
	a, ok := g.GraphStore.(store.Aggregator)
	if !ok {
		return nil, fmt.Errorf("CountEdgesByType: %T cannot count by type", g.GraphStore)
	}
	return a.CountEdgesByType(ctx)
}

// CountNodesByProperty returns how many live nodes hold each distinct value
// under key — the distribution of an indexed field.
//
// Only nodes with an index entry under key are counted, which is the useful
// reading: a node that never registered a value has no value to distribute.
// Values are the caller-encoded bytes as stored, keyed by their string form.
func (g *Graph) CountNodesByProperty(key string) (map[string]uint64, error) {
	return g.CountNodesByPropertyCtx(context.Background(), key)
}

// CountNodesByPropertyCtx is CountNodesByProperty, cancellable.
func (g *Graph) CountNodesByPropertyCtx(ctx context.Context, key string) (map[string]uint64, error) {
	a, ok := g.GraphStore.(store.Aggregator)
	if !ok {
		return nil, fmt.Errorf("CountNodesByProperty: %T cannot count by property", g.GraphStore)
	}
	return a.CountNodesByProperty(ctx, key)
}

// CountEdgesByProperty is CountNodesByProperty for edge properties.
func (g *Graph) CountEdgesByProperty(key string) (map[string]uint64, error) {
	return g.CountEdgesByPropertyCtx(context.Background(), key)
}

// CountEdgesByPropertyCtx is CountEdgesByProperty, cancellable.
func (g *Graph) CountEdgesByPropertyCtx(ctx context.Context, key string) (map[string]uint64, error) {
	a, ok := g.GraphStore.(store.Aggregator)
	if !ok {
		return nil, fmt.Errorf("CountEdgesByProperty: %T cannot count by property", g.GraphStore)
	}
	return a.CountEdgesByProperty(ctx, key)
}

// NodesByPropertyBatch resolves many exact values against one indexed key in a
// single pass, calling fn with the live nodes for each value that has any.
//
// Use it to join a set of values you already have — content digests, external
// ids — against the nodes indexed under them. It is the same answer as calling
// NodesByProperty once per value, and on a store whose index is disk-resident it
// is dramatically cheaper, because the values are resolved in sorted order and
// the index is swept forward once instead of being searched per value.
//
// Three things to hold on to. fn is given the *index* of the value within values,
// because results do not arrive in input order. A value with no live node
// produces no call at all. And the id slice belongs to the store for the duration
// of the call: copy anything you keep.
//
// Unlike the other optional extensions this one has no error for a backend that
// lacks it, because it has an exact fallback: NodesByProperty in a loop, which is
// what runs. So this is always callable and the only thing a backend without
// PropertyBatcher costs is the speed.
func (g *Graph) NodesByPropertyBatch(key string, values [][]byte,
	fn func(i int, ids []store.NodeID) bool,
) error {
	return g.NodesByPropertyBatchCtx(context.Background(), key, values, fn)
}

// NodesByPropertyBatchCtx is NodesByPropertyBatch, cancellable.
func (g *Graph) NodesByPropertyBatchCtx(ctx context.Context, key string, values [][]byte,
	fn func(i int, ids []store.NodeID) bool,
) error {
	if b, ok := g.GraphStore.(store.PropertyBatcher); ok {
		return b.NodesByPropertyBatch(ctx, key, values, fn)
	}
	for i, v := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids, err := g.NodesByProperty(key, v)
		if err != nil {
			return err
		}
		if len(ids) > 0 && !fn(i, ids) {
			return nil
		}
	}
	return nil
}

// EdgesByPropertyBatch is NodesByPropertyBatch for edges.
func (g *Graph) EdgesByPropertyBatch(key string, values [][]byte,
	fn func(i int, ids []store.EdgeID) bool,
) error {
	return g.EdgesByPropertyBatchCtx(context.Background(), key, values, fn)
}

// EdgesByPropertyBatchCtx is EdgesByPropertyBatch, cancellable.
func (g *Graph) EdgesByPropertyBatchCtx(ctx context.Context, key string, values [][]byte,
	fn func(i int, ids []store.EdgeID) bool,
) error {
	if b, ok := g.GraphStore.(store.PropertyBatcher); ok {
		return b.EdgesByPropertyBatch(ctx, key, values, fn)
	}
	for i, v := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids, err := g.EdgesByProperty(key, v)
		if err != nil {
			return err
		}
		if len(ids) > 0 && !fn(i, ids) {
			return nil
		}
	}
	return nil
}

// NodesByPropertyFunc calls fn once per live node indexed under key=value,
// ascending, stopping early if fn returns false.
//
// The fallback is NodesByProperty and a range loop, which is the whole answer
// rather than a degraded one — so this is always callable, and what a backend
// without store.PropertyStreamer costs is that the ids are materialised before
// the first one reaches fn.
func (g *Graph) NodesByPropertyFunc(key string, value []byte,
	fn func(id store.NodeID) bool,
) error {
	return g.NodesByPropertyFuncCtx(context.Background(), key, value, fn)
}

// NodesByPropertyFuncCtx is NodesByPropertyFunc, cancellable.
func (g *Graph) NodesByPropertyFuncCtx(ctx context.Context, key string, value []byte,
	fn func(id store.NodeID) bool,
) error {
	if s, ok := g.GraphStore.(store.PropertyStreamer); ok {
		return s.NodesByPropertyFunc(ctx, key, value, fn)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ids, err := g.NodesByProperty(key, value)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// EdgesByPropertyFunc is NodesByPropertyFunc for edges.
func (g *Graph) EdgesByPropertyFunc(key string, value []byte,
	fn func(id store.EdgeID) bool,
) error {
	return g.EdgesByPropertyFuncCtx(context.Background(), key, value, fn)
}

// EdgesByPropertyFuncCtx is EdgesByPropertyFunc, cancellable.
func (g *Graph) EdgesByPropertyFuncCtx(ctx context.Context, key string, value []byte,
	fn func(id store.EdgeID) bool,
) error {
	if s, ok := g.GraphStore.(store.PropertyStreamer); ok {
		return s.EdgesByPropertyFunc(ctx, key, value, fn)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ids, err := g.EdgesByProperty(key, value)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachNodeID calls fn once per node id matching query, in the order
// QueryNodeIDs would have returned them.
//
// The fallback runs the query and walks its result, which is what a backend
// without store.NodeQueryStreamer would do anyway — and what any backend does
// for a query whose ordering or window needs the candidates in hand.
func (g *Graph) ForEachNodeID(query store.NodeQuery, fn func(id store.NodeID) bool) error {
	return g.ForEachNodeIDCtx(context.Background(), query, fn)
}

// ForEachNodeIDCtx is ForEachNodeID, cancellable.
func (g *Graph) ForEachNodeIDCtx(ctx context.Context, query store.NodeQuery,
	fn func(id store.NodeID) bool,
) error {
	if s, ok := g.GraphStore.(store.NodeQueryStreamer); ok {
		return s.ForEachNodeID(ctx, query, fn)
	}
	ids, err := g.QueryNodeIDsCtx(ctx, query)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// StorageStats reports the backend's storage state, and whether it could.
//
// Cheaper than Stats when only the operational figures are wanted: it does not
// count nodes and edges, which on the disk backend means it does not merge the
// CSR and delta views.
func (g *Graph) StorageStats() (store.StorageStats, bool) {
	sr, ok := g.GraphStore.(store.StorageReporter)
	if !ok {
		return store.StorageStats{}, false
	}
	return sr.StorageStats(), true
}

// ShouldCompact reports whether the store has breached policy, and which rule
// fired.
//
// Advisory only. Nothing in the engine acts on it, and calling it changes
// nothing — compaction rebuilds the entire image, so when to pay that is the
// caller's decision, not the engine's. A backend that cannot report its storage
// state returns false.
//
// The intended shape is a periodic check in the caller's own loop:
//
//	if due, why := g.ShouldCompact(store.DefaultCompactionPolicy()); due {
//	    log.Printf("compacting: %s", why)
//	    g.Compact()
//	}
//
// This exists because nothing else bounds delta growth. Everything written since
// the last compaction stays in memory and is replayed at every open, so a store
// that is never compacted degrades in memory, open time, and read speed at once
// — with no error and no warning until someone measures it.
func (g *Graph) ShouldCompact(p store.CompactionPolicy) (bool, string) {
	s, ok := g.StorageStats()
	if !ok {
		return false, ""
	}
	return p.Evaluate(s)
}

// CompactIfDue compacts when policy says the store is due, and reports which
// rule fired.
//
// The two halves have always been here -- ShouldCompact reads the figures and
// Compact acts -- and every caller that wanted the pair wrote the same four lines
// around them. This is those four lines, so that the decision and the action
// cannot drift apart: a caller cannot evaluate one policy and compact on another,
// and cannot compact having forgotten to ask.
//
// The reason comes back whether or not anything was compacted, because it is what
// an operator needs in a log line. It is empty exactly when the store was not due.
//
// A backend that reports no storage statistics is never due, so this is a no-op on
// the in-memory store rather than an unconditional compaction of something that
// has nothing to compact.
//
// Identifiers survive a compaction, so a slice of ids collected before this call
// is still valid after it. See disk.Store.CompactCtx.
func (g *Graph) CompactIfDue(p store.CompactionPolicy) (bool, string, error) {
	return g.CompactIfDueCtx(context.Background(), p)
}

// CompactIfDueCtx is CompactIfDue, with the compaction abandoned if ctx is
// cancelled. The policy is evaluated first and is not cancellable: it is a read
// lock and four comparisons.
func (g *Graph) CompactIfDueCtx(ctx context.Context, p store.CompactionPolicy) (bool, string, error) {
	due, why := g.ShouldCompact(p)
	if !due {
		return false, "", nil
	}
	if err := g.CompactCtx(ctx); err != nil {
		return false, why, err
	}
	return true, why, nil
}

// --- Batch reads ---

// GetNodes fetches multiple nodes by ID in the order given.
// If any ID is not found the error is returned immediately.
// GetNodes fetches multiple nodes by ID, preserving request order.
//
// **A missing ID is not an error.** It is reported in missing, and err is
// reserved for genuine failures. That is deliberate: under the read model
// (API_REFERENCE §16) an ID can be deleted between the call that produced it and
// the call that resolves it, so treating that as exceptional forced callers back
// into the per-item loop this method exists to replace.
//
// found is compacted — missing IDs leave no nil holes — and each record carries
// its own ID, so a caller needing to correlate results back to requested IDs can
// read node.ID rather than relying on position.
func (g *Graph) GetNodes(ids []store.NodeID) (found []*store.Node, missing []store.NodeID, err error) {
	if br, ok := g.GraphStore.(store.BatchReader); ok {
		f, m := br.GetNodesBatch(ids)
		return f, m, nil
	}
	found = make([]*store.Node, 0, len(ids))
	for _, id := range ids {
		n, err := g.GetNode(id)
		if err != nil {
			var nf *store.ErrNotFound
			if errors.As(err, &nf) {
				missing = append(missing, id)
				continue
			}
			return nil, nil, err
		}
		found = append(found, n)
	}
	return found, missing, nil
}

// GetEdges fetches multiple edges by ID in the order given.
// If any ID is not found the error is returned immediately.
// GetEdges fetches multiple edges by ID, preserving request order. A missing ID
// is reported in missing rather than returned as an error — see GetNodes.
func (g *Graph) GetEdges(ids []store.EdgeID) (found []*store.Edge, missing []store.EdgeID, err error) {
	if br, ok := g.GraphStore.(store.BatchReader); ok {
		f, m := br.GetEdgesBatch(ids)
		return f, m, nil
	}
	found = make([]*store.Edge, 0, len(ids))
	for _, id := range ids {
		e, err := g.GetEdge(id)
		if err != nil {
			var nf *store.ErrNotFound
			if errors.As(err, &nf) {
				missing = append(missing, id)
				continue
			}
			return nil, nil, err
		}
		found = append(found, e)
	}
	return found, missing, nil
}

// --- Bounded batch reads ---

// ErrBatchStalled is returned by ForEachNodeBatch and ForEachEdgeBatch when the
// store hands back a batch that consumed none of the ids it was given.
//
// Both bundled backends guarantee progress — store.BoundedBatchReader requires
// it, and the rule that delivers it is that the budget is not consulted until a
// record is already in the batch. A third-party store that gets that wrong
// turns the paging loop into a spin with no output and no error, which is the
// worst shape a bug can take. This makes it an error instead, named for what
// went wrong and where.
var ErrBatchStalled = errors.New("graphene: bounded batch consumed no ids")

// GetNodesBounded is GetNodes with a ceiling on the payload bytes one call
// returns, for a caller resolving more ids than it wants records in hand at
// once.
//
// It resolves ids in order and stops before the record that would take the
// batch's payload total past maxBytes, returning the ids it did not reach in
// rest. A call always consumes at least one id — a record whose blob alone
// exceeds maxBytes comes back on its own rather than being refused — so a
// resume loop terminates:
//
//	for len(ids) > 0 {
//		found, missing, rest, err := g.GetNodesBounded(ids, 8<<20)
//		if err != nil {
//			return err
//		}
//		use(found, missing)
//		ids = rest
//	}
//
// ForEachNodeBatch is that loop, written once.
//
// **What the ceiling counts.** The payloads, and only those: the per-record
// struct and the slice holding it come to about 72 bytes a record whatever the
// blobs weigh, and are not counted. Nor is it a claim about resident memory —
// under Options.ImageMode = ImageMapped a record's Properties points into the
// image file, so the bound is on the bytes the *caller* is about to touch and
// retain, not on an allocation the engine made. See store.BoundedBatchReader.
//
// The fallback for a backend implementing neither batch interface resolves one
// id at a time, which honours the budget exactly and pays a lock acquisition per
// record to do it.
func (g *Graph) GetNodesBounded(ids []store.NodeID, maxBytes int64) (found []*store.Node, missing []store.NodeID, rest []store.NodeID, err error) {
	if len(ids) == 0 {
		return nil, nil, nil, nil
	}
	if br, ok := g.GraphStore.(store.BoundedBatchReader); ok {
		f, m, r := br.GetNodesBatchBounded(ids, maxBytes)
		return f, m, r, nil
	}
	var total int64
	for i, id := range ids {
		n, err := g.GetNode(id)
		if err != nil {
			var nf *store.ErrNotFound
			if errors.As(err, &nf) {
				missing = append(missing, id)
				continue
			}
			return nil, nil, nil, err
		}
		b := int64(len(n.Properties))
		if len(found) > 0 && total+b > maxBytes {
			return found, missing, ids[i:], nil
		}
		found = append(found, n)
		total += b
	}
	return found, missing, nil, nil
}

// GetEdgesBounded is GetNodesBounded for edges.
func (g *Graph) GetEdgesBounded(ids []store.EdgeID, maxBytes int64) (found []*store.Edge, missing []store.EdgeID, rest []store.EdgeID, err error) {
	if len(ids) == 0 {
		return nil, nil, nil, nil
	}
	if br, ok := g.GraphStore.(store.BoundedBatchReader); ok {
		f, m, r := br.GetEdgesBatchBounded(ids, maxBytes)
		return f, m, r, nil
	}
	var total int64
	for i, id := range ids {
		e, err := g.GetEdge(id)
		if err != nil {
			var nf *store.ErrNotFound
			if errors.As(err, &nf) {
				missing = append(missing, id)
				continue
			}
			return nil, nil, nil, err
		}
		b := int64(len(e.Properties))
		if len(found) > 0 && total+b > maxBytes {
			return found, missing, ids[i:], nil
		}
		found = append(found, e)
		total += b
	}
	return found, missing, nil, nil
}

// ForEachNodeBatch resolves ids in payload-bounded batches, calling fn once per
// batch and stopping early without an error if fn returns false.
//
// This is the loop GetNodesBounded is the step of, and it is the form to reach
// for: a caller that wants every record of a large id set but never more than
// maxBytes of payload in hand writes the body and not the paging. Records are
// delivered in request order across batches, and every batch holds at least one
// record.
//
// missing accumulates the ids that were looked for and not found, over the
// batches actually run — an early stop leaves the rest of the ids unexamined,
// so a short missing does not mean the remaining ids exist.
//
// The batch slice handed to fn is fresh each call and is fn's to keep. Its
// *records* are subject to the usual rule: reads may alias store-internal
// memory, so a record kept past the life of the handle — or mutated — must be
// copied with store.CloneNode (API_REFERENCE §"Do not mutate returned structs").
// A compaction does not shorten that window; Close ends it.
//
// A store whose bounded read consumes nothing stops the loop with
// ErrBatchStalled rather than spinning on it.
func (g *Graph) ForEachNodeBatch(ids []store.NodeID, maxBytes int64,
	fn func(batch []*store.Node) bool,
) (missing []store.NodeID, err error) {
	return g.ForEachNodeBatchCtx(context.Background(), ids, maxBytes, fn)
}

// ForEachNodeBatchCtx is ForEachNodeBatch, cancellable.
//
// Cancellation is checked once per batch rather than once per record, so it
// takes effect between batches and a batch already resolved is still delivered
// to fn. That is the useful granularity: a batch is bounded work by
// construction, and tearing one up mid-flight would only hand fn a partial view
// it has no way to recognise.
func (g *Graph) ForEachNodeBatchCtx(ctx context.Context, ids []store.NodeID, maxBytes int64,
	fn func(batch []*store.Node) bool,
) (missing []store.NodeID, err error) {
	for len(ids) > 0 {
		if err := ctx.Err(); err != nil {
			return missing, err
		}
		found, miss, rest, err := g.GetNodesBounded(ids, maxBytes)
		if err != nil {
			return missing, err
		}
		if len(rest) >= len(ids) {
			return missing, fmt.Errorf("%w: %d ids in, %d back", ErrBatchStalled, len(ids), len(rest))
		}
		missing = append(missing, miss...)
		if len(found) > 0 && !fn(found) {
			return missing, nil
		}
		ids = rest
	}
	return missing, nil
}

// ForEachEdgeBatch is ForEachNodeBatch for edges.
func (g *Graph) ForEachEdgeBatch(ids []store.EdgeID, maxBytes int64,
	fn func(batch []*store.Edge) bool,
) (missing []store.EdgeID, err error) {
	return g.ForEachEdgeBatchCtx(context.Background(), ids, maxBytes, fn)
}

// ForEachEdgeBatchCtx is ForEachEdgeBatch, cancellable.
func (g *Graph) ForEachEdgeBatchCtx(ctx context.Context, ids []store.EdgeID, maxBytes int64,
	fn func(batch []*store.Edge) bool,
) (missing []store.EdgeID, err error) {
	for len(ids) > 0 {
		if err := ctx.Err(); err != nil {
			return missing, err
		}
		found, miss, rest, err := g.GetEdgesBounded(ids, maxBytes)
		if err != nil {
			return missing, err
		}
		if len(rest) >= len(ids) {
			return missing, fmt.Errorf("%w: %d ids in, %d back", ErrBatchStalled, len(ids), len(rest))
		}
		missing = append(missing, miss...)
		if len(found) > 0 && !fn(found) {
			return missing, nil
		}
		ids = rest
	}
	return missing, nil
}

// --- Projections ---

// NodeProjection is one entity's requested values, positioned to match the keys
// that were asked for.
//
// Values[i] holds every value the index carries for keys[i] — several, because a
// key may be registered more than once for one entity — and is nil for a key the
// entity is not indexed under. A projected value is a copy and is the caller's.
type NodeProjection struct {
	ID     store.NodeID
	Values [][][]byte
}

// EdgeProjection is NodeProjection for edges.
type EdgeProjection struct {
	ID     store.EdgeID
	Values [][][]byte
}

// ForEachNodeProjection calls fn once per (id, key, value) the index holds for
// the requested keys, without reading the records that carry them.
//
// fn receives positions, not names: idIdx indexes ids and keyIdx indexes keys.
// The value belongs to the store for the duration of the call — read it, do not
// retain it. Returning false stops the pass without an error.
//
// **It reads the index, not the record.** The values are the ones handed to
// IndexNodeProperty or to a transaction's index entries. A key that was never
// indexed produces no callback at all, and a non-indexed field still needs the
// record. See store.Projector for when this is cheaper than reading the records,
// which is not always: the record wins while the payloads are in page cache and
// loses once they are not.
//
// A backend without store.Projector is an error rather than a slow fallback,
// because there is no fallback that is this API: reading the records to
// reconstruct a projection is the thing the caller was avoiding, and doing it
// silently would turn an optimisation into a pessimisation nobody asked for.
func (g *Graph) ForEachNodeProjection(ids []store.NodeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) error {
	return g.ForEachNodeProjectionCtx(context.Background(), ids, keys, fn)
}

// ForEachNodeProjectionCtx is ForEachNodeProjection, cancellable.
func (g *Graph) ForEachNodeProjectionCtx(ctx context.Context, ids []store.NodeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) error {
	p, ok := g.GraphStore.(store.Projector)
	if !ok {
		return ErrNoProjector
	}
	return p.ForEachNodeProjection(ctx, ids, keys, fn)
}

// ForEachEdgeProjection is ForEachNodeProjection for edges.
func (g *Graph) ForEachEdgeProjection(ids []store.EdgeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) error {
	return g.ForEachEdgeProjectionCtx(context.Background(), ids, keys, fn)
}

// ForEachEdgeProjectionCtx is ForEachEdgeProjection, cancellable.
func (g *Graph) ForEachEdgeProjectionCtx(ctx context.Context, ids []store.EdgeID, keys []string,
	fn func(idIdx, keyIdx int, value []byte) bool,
) error {
	p, ok := g.GraphStore.(store.Projector)
	if !ok {
		return ErrNoProjector
	}
	return p.ForEachEdgeProjection(ctx, ids, keys, fn)
}

// ErrNoProjector is returned by the projection methods for a backend that does
// not implement store.Projector. Both bundled backends do.
var ErrNoProjector = errors.New("graphene: this store cannot project indexed values")

// GetNodesProjected materialises what ForEachNodeProjection streams: one
// NodeProjection per id, in request order.
//
// It is the convenient form and not the one to reach for at scale. One entry per
// id means one slice header per id per key and a copy of every value, which at a
// million ids is the allocation the callback form exists to avoid — the same
// argument index/batch.go makes about returning a slice of slices. Use it for
// thousands; use ForEachNodeProjection for millions.
func (g *Graph) GetNodesProjected(ids []store.NodeID, keys []string) ([]NodeProjection, error) {
	return g.GetNodesProjectedCtx(context.Background(), ids, keys)
}

// GetNodesProjectedCtx is GetNodesProjected, cancellable.
func (g *Graph) GetNodesProjectedCtx(ctx context.Context, ids []store.NodeID, keys []string) ([]NodeProjection, error) {
	out := make([]NodeProjection, len(ids))
	for i, id := range ids {
		out[i] = NodeProjection{ID: id, Values: make([][][]byte, len(keys))}
	}
	err := g.ForEachNodeProjectionCtx(ctx, ids, keys, func(idIdx, keyIdx int, value []byte) bool {
		out[idIdx].Values[keyIdx] = append(out[idIdx].Values[keyIdx], bytes.Clone(value))
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetEdgesProjected is GetNodesProjected for edges.
func (g *Graph) GetEdgesProjected(ids []store.EdgeID, keys []string) ([]EdgeProjection, error) {
	return g.GetEdgesProjectedCtx(context.Background(), ids, keys)
}

// GetEdgesProjectedCtx is GetEdgesProjected, cancellable.
func (g *Graph) GetEdgesProjectedCtx(ctx context.Context, ids []store.EdgeID, keys []string) ([]EdgeProjection, error) {
	out := make([]EdgeProjection, len(ids))
	for i, id := range ids {
		out[i] = EdgeProjection{ID: id, Values: make([][][]byte, len(keys))}
	}
	err := g.ForEachEdgeProjectionCtx(ctx, ids, keys, func(idIdx, keyIdx int, value []byte) bool {
		out[idIdx].Values[keyIdx] = append(out[idIdx].Values[keyIdx], bytes.Clone(value))
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// --- Batch writes ---

// AddNodes adds multiple nodes in order, returning their assigned IDs.
//
// On both bundled backends this is atomic: either every node is added or none
// is, and a failure returns a nil ID slice rather than a partial one. Third-party
// stores that do not implement the batch interface fall back to a per-node loop,
// which is not atomic — it returns the IDs assigned so far alongside the error.
//
// For a batch of nodes *and* the edges between them, use Begin: AddNodes
// followed by AddEdges is two transactions, and a crash between them leaves the
// nodes without their edges.
func (g *Graph) AddNodes(nodes []*store.Node) ([]store.NodeID, error) {
	if b, ok := g.GraphStore.(nodeBatchAdder); ok {
		return b.AddNodesBatch(nodes)
	}

	ids := make([]store.NodeID, len(nodes))
	for i, n := range nodes {
		id, err := g.AddNode(n)
		if err != nil {
			return ids[:i], err
		}
		ids[i] = id
	}
	return ids, nil
}

// AddEdges adds multiple edges in order, returning their assigned IDs.
//
// On both bundled backends this is atomic, and every endpoint is validated
// before anything is written — so a batch containing one dangling edge adds
// nothing and returns ErrInvalidEdge. Third-party stores without the batch
// interface fall back to a non-atomic per-edge loop.
//
// Endpoints must already exist. To create nodes and the edges between them
// together, use Begin.
func (g *Graph) AddEdges(edges []*store.Edge) ([]store.EdgeID, error) {
	if b, ok := g.GraphStore.(edgeBatchAdder); ok {
		return b.AddEdgesBatch(edges)
	}

	ids := make([]store.EdgeID, len(edges))
	for i, e := range edges {
		id, err := g.AddEdge(e)
		if err != nil {
			return ids[:i], err
		}
		ids[i] = id
	}
	return ids, nil
}

// --- Bulk property indexing ---

// IndexNodeProperties indexes all key-value pairs in props for the given node.
// Indexing stops and the error is returned on first failure.
func (g *Graph) IndexNodeProperties(id store.NodeID, props map[string][]byte) error {
	for k, v := range props {
		if err := g.IndexNodeProperty(id, k, v); err != nil {
			return err
		}
	}
	return nil
}

// IndexEdgeProperties indexes all key-value pairs in props for the given edge.
// Indexing stops and the error is returned on first failure.
func (g *Graph) IndexEdgeProperties(id store.EdgeID, props map[string][]byte) error {
	for k, v := range props {
		if err := g.IndexEdgeProperty(id, k, v); err != nil {
			return err
		}
	}
	return nil
}

// --- Multi-key property queries ---

// NodesByProperties returns the intersection of all NodeIDs that match every
// key-value pair in props (AND semantics). Returns an empty slice when props is empty.
//
// This is the un-planned path: it resolves each pair to its own set and folds
// them together, where QueryNodes would cost the pairs and drive from the most
// selective one. Prefer QueryNodes with equality filters — and, for a set of
// keys queried together often, a composite index (DeclareCompositeProperties),
// which answers the whole conjunction from one lookup.
//
// The fold is store.IntersectSortedIDs, the same sorted merge the planner's
// residual pass uses. It requires both sides ascending and duplicate-free, which
// NodesByProperty guarantees: postings are kept sorted by ID and the liveness
// filter preserves order. The engine used to carry a second, hash-set
// intersection here for no reason but that this path predated the merge —
// two implementations of one operation, which is one more than can be kept
// correct.
func (g *Graph) NodesByProperties(props map[string][]byte) ([]store.NodeID, error) {
	var result []store.NodeID
	first := true
	for k, v := range props {
		hits, err := g.NodesByProperty(k, v)
		if err != nil {
			return nil, err
		}
		if first {
			result = hits
			first = false
			continue
		}
		result = store.IntersectSortedIDs(result, hits)
		if len(result) == 0 {
			return nil, nil
		}
	}
	return result, nil
}

// EdgesByProperties returns the intersection of all EdgeIDs that match every
// key-value pair in props (AND semantics). Returns an empty slice when props is
// empty. See NodesByProperties for what this path costs and what it requires of
// its inputs.
func (g *Graph) EdgesByProperties(props map[string][]byte) ([]store.EdgeID, error) {
	var result []store.EdgeID
	first := true
	for k, v := range props {
		hits, err := g.EdgesByProperty(k, v)
		if err != nil {
			return nil, err
		}
		if first {
			result = hits
			first = false
			continue
		}
		result = store.IntersectSortedIDs(result, hits)
		if len(result) == 0 {
			return nil, nil
		}
	}
	return result, nil
}

// NodesWithProperties returns hydrated nodes matching all key-value pairs.
func (g *Graph) NodesWithProperties(props map[string][]byte) ([]*store.Node, error) {
	ids, err := g.NodesByProperties(props)
	if err != nil {
		return nil, err
	}
	found, _, err := g.GetNodes(ids)
	return found, err
}

// EdgesWithProperties returns hydrated edges matching all key-value pairs.
func (g *Graph) EdgesWithProperties(props map[string][]byte) ([]*store.Edge, error) {
	ids, err := g.EdgesByProperties(props)
	if err != nil {
		return nil, err
	}
	found, _, err := g.GetEdges(ids)
	return found, err
}

// QueryNodeIDs returns node IDs that satisfy query constraints.
func (g *Graph) QueryNodeIDs(query store.NodeQuery) ([]store.NodeID, error) {
	return g.GraphStore.QueryNodeIDs(query)
}

// QueryNodeIDsCtx is QueryNodeIDs, abandoned if ctx is cancelled. A backend
// that cannot cancel runs the query to completion.
//
// Nothing partial is returned with the error: a query stopped part way holds a
// superset of the answer shaped exactly like the answer. See store.QuerierCtx.
func (g *Graph) QueryNodeIDsCtx(ctx context.Context, query store.NodeQuery) ([]store.NodeID, error) {
	if q, ok := g.GraphStore.(store.QuerierCtx); ok {
		return q.QueryNodeIDsCtx(ctx, query)
	}
	return g.GraphStore.QueryNodeIDs(query)
}

// QueryNodes returns hydrated nodes that satisfy query constraints.
func (g *Graph) QueryNodes(query store.NodeQuery) ([]*store.Node, error) {
	ids, err := g.QueryNodeIDs(query)
	if err != nil {
		return nil, err
	}
	found, _, err := g.GetNodes(ids)
	return found, err
}

// QueryNodesCtx is QueryNodes, abandoned if ctx is cancelled.
//
// Cancellation reaches the query; the hydration that follows is a lookup per
// surviving ID and is left alone, because by then the expensive part is done
// and the result is about to be complete.
func (g *Graph) QueryNodesCtx(ctx context.Context, query store.NodeQuery) ([]*store.Node, error) {
	ids, err := g.QueryNodeIDsCtx(ctx, query)
	if err != nil {
		return nil, err
	}
	found, _, err := g.GetNodes(ids)
	return found, err
}

// QueryEdgeIDs returns edge IDs that satisfy query constraints.
func (g *Graph) QueryEdgeIDs(query store.EdgeQuery) ([]store.EdgeID, error) {
	return g.GraphStore.QueryEdgeIDs(query)
}

// QueryEdgeIDsCtx is QueryEdgeIDs, abandoned if ctx is cancelled.
func (g *Graph) QueryEdgeIDsCtx(ctx context.Context, query store.EdgeQuery) ([]store.EdgeID, error) {
	if q, ok := g.GraphStore.(store.QuerierCtx); ok {
		return q.QueryEdgeIDsCtx(ctx, query)
	}
	return g.GraphStore.QueryEdgeIDs(query)
}

// QueryEdges returns hydrated edges that satisfy query constraints.
func (g *Graph) QueryEdges(query store.EdgeQuery) ([]*store.Edge, error) {
	ids, err := g.QueryEdgeIDs(query)
	if err != nil {
		return nil, err
	}
	found, _, err := g.GetEdges(ids)
	return found, err
}

// QueryEdgesCtx is QueryEdges, abandoned if ctx is cancelled.
func (g *Graph) QueryEdgesCtx(ctx context.Context, query store.EdgeQuery) ([]*store.Edge, error) {
	ids, err := g.QueryEdgeIDsCtx(ctx, query)
	if err != nil {
		return nil, err
	}
	found, _, err := g.GetEdges(ids)
	return found, err
}

// QueryRelations returns relation edges around anchor nodes using direction-aware matching.
func (g *Graph) QueryRelationIDs(query store.RelationQuery) ([]store.EdgeID, error) {
	if len(query.Anchors) == 0 {
		return nil, nil
	}
	mode := store.NormalizedFilterMode(query.FilterMode)
	order := store.NormalizedQueryOrder(query.Order)

	buildEdgeQuery := func(srcIDs, dstIDs []store.NodeID, withWindow bool) store.EdgeQuery {
		eq := store.EdgeQuery{
			SrcIDs:     srcIDs,
			DstIDs:     dstIDs,
			Types:      query.EdgeTypes,
			Filters:    query.Filters,
			FilterMode: mode,
			Order:      order,
		}
		if withWindow {
			eq.Offset = query.Offset
			eq.Limit = query.Limit
		}
		return eq
	}

	switch query.Direction {
	case store.DirectionInbound:
		return g.QueryEdgeIDs(buildEdgeQuery(query.Counterparts, query.Anchors, true))
	case store.DirectionBoth:
		outbound, err := g.QueryEdgeIDs(buildEdgeQuery(query.Anchors, query.Counterparts, false))
		if err != nil {
			return nil, err
		}
		inbound, err := g.QueryEdgeIDs(buildEdgeQuery(query.Counterparts, query.Anchors, false))
		if err != nil {
			return nil, err
		}
		ids := dedupeEdgeIDs(append(outbound, inbound...))
		sort.Slice(ids, func(i, j int) bool {
			if order == store.QueryOrderDesc {
				return ids[i] > ids[j]
			}
			return ids[i] < ids[j]
		})
		return store.ApplyEdgeQueryWindow(ids, query.Offset, query.Limit), nil
	case store.DirectionOutbound:
		fallthrough
	default:
		return g.QueryEdgeIDs(buildEdgeQuery(query.Anchors, query.Counterparts, true))
	}
}

// QueryRelations returns relation edges around anchor nodes using direction-aware matching.
func (g *Graph) QueryRelations(query store.RelationQuery) ([]*store.Edge, error) {
	ids, err := g.QueryRelationIDs(query)
	if err != nil {
		return nil, err
	}
	found, _, err := g.GetEdges(ids)
	return found, err
}

// --- Multi-type queries ---

// NodesByAnyType returns all NodeIDs that carry at least one of the given labels
// (OR semantics). Duplicate IDs are deduplicated.
func (g *Graph) NodesByAnyType(types []store.NodeType) ([]store.NodeID, error) {
	seen := make(map[store.NodeID]struct{})
	var out []store.NodeID
	for _, t := range types {
		ids, err := g.NodesByType(t)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	return out, nil
}

// EdgesByAnyType returns all EdgeIDs that carry at least one of the given labels
// (OR semantics). Duplicate IDs are deduplicated.
func (g *Graph) EdgesByAnyType(types []store.EdgeType) ([]store.EdgeID, error) {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	for _, t := range types {
		ids, err := g.EdgesByType(t)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	return out, nil
}

// NodesByTypeSelector parses selector and returns matching node IDs.
// Supports built-in names and custom selectors such as "custom:7".
func (g *Graph) NodesByTypeSelector(selector string) ([]store.NodeID, error) {
	t, err := store.ParseNodeType(selector)
	if err != nil {
		return nil, err
	}
	return g.NodesByType(t)
}

// NodesByAnyTypeSelector returns all NodeIDs matching at least one selector.
func (g *Graph) NodesByAnyTypeSelector(selectors []string) ([]store.NodeID, error) {
	types := make([]store.NodeType, 0, len(selectors))
	for _, s := range selectors {
		t, err := store.ParseNodeType(s)
		if err != nil {
			return nil, err
		}
		types = append(types, t)
	}
	return g.NodesByAnyType(types)
}

// EdgesByTypeSelector parses selector and returns matching edge IDs.
// Supports built-in names and custom selectors such as "custom:7".
func (g *Graph) EdgesByTypeSelector(selector string) ([]store.EdgeID, error) {
	t, err := store.ParseEdgeType(selector)
	if err != nil {
		return nil, err
	}
	return g.EdgesByType(t)
}

// EdgesByAnyTypeSelector returns all EdgeIDs matching at least one selector.
func (g *Graph) EdgesByAnyTypeSelector(selectors []string) ([]store.EdgeID, error) {
	types := make([]store.EdgeType, 0, len(selectors))
	for _, s := range selectors {
		t, err := store.ParseEdgeType(s)
		if err != nil {
			return nil, err
		}
		types = append(types, t)
	}
	return g.EdgesByAnyType(types)
}

// --- Degree helpers ---

// degreeOf counts incident edges, using the store's DegreeCounter fast path when
// the backend provides one (both bundled backends do) and falling back to
// materialising the edge slice otherwise.
func (g *Graph) degreeOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) (int, error) {
	if counter, ok := g.GraphStore.(store.DegreeCounter); ok {
		return counter.DegreeOf(id, dir, edgeTypes)
	}
	edges, err := g.EdgesOf(id, dir, edgeTypes)
	if err != nil {
		return 0, err
	}
	return len(edges), nil
}

// InDegree returns the number of inbound edges for node id.
// Pass nil edgeTypes to count all inbound edges.
func (g *Graph) InDegree(id store.NodeID, edgeTypes []store.EdgeType) (int, error) {
	return g.degreeOf(id, store.DirectionInbound, edgeTypes)
}

// OutDegree returns the number of outbound edges for node id.
// Pass nil edgeTypes to count all outbound edges.
func (g *Graph) OutDegree(id store.NodeID, edgeTypes []store.EdgeType) (int, error) {
	return g.degreeOf(id, store.DirectionOutbound, edgeTypes)
}

// Degree returns the total (in + out) edge count for node id.
// Pass nil edgeTypes to count all edges. Note that for undirected use-cases,
// edges that appear in both directions are counted twice.
func (g *Graph) Degree(id store.NodeID, edgeTypes []store.EdgeType) (int, error) {
	return g.degreeOf(id, store.DirectionBoth, edgeTypes)
}

// --- Connectivity helpers ---

// EdgeExists reports whether at least one direct edge exists from src to dst.
// Pass nil edgeTypes to consider edges of any type.
func (g *Graph) EdgeExists(src, dst store.NodeID, edgeTypes []store.EdgeType) (bool, error) {
	_, found, err := g.EdgeIDBetween(src, dst, edgeTypes)
	return found, err
}

// EdgeIDBetween returns a live edge from src to dst carrying any of edgeTypes.
// Pass nil edgeTypes to consider edges of any type.
//
// Which edge, when several match, is unspecified — unless the type is declared
// through DeclareUniqueEdge, which is the point of declaring it: the question
// then has one answer. found is false when nothing matches, which is not an
// error.
//
// This exists because a caller checking for a duplicate almost always wants the
// incumbent rather than a boolean, and because EdgeExists used to materialise
// the entire outbound edge slice — records, label slices and property blobs —
// to compare one field. The backends can answer from adjacency without building
// any of that, and both bundled ones do.
func (g *Graph) EdgeIDBetween(src, dst store.NodeID, edgeTypes []store.EdgeType) (store.EdgeID, bool, error) {
	if b, ok := g.GraphStore.(store.EdgeCardinalityDeclarer); ok {
		return b.EdgeBetween(src, dst, edgeTypes)
	}
	// A third-party store without the extension still answers correctly, just
	// by building what it then throws away — the same fallback shape degreeOf
	// uses for DegreeCounter.
	edges, err := g.EdgesOf(src, store.DirectionOutbound, edgeTypes)
	if err != nil {
		return store.InvalidEdgeID, false, err
	}
	for _, e := range edges {
		if e.Dst == dst {
			return e.ID, true, nil
		}
	}
	return store.InvalidEdgeID, false, nil
}

// IsConnected reports whether src and dst are reachable from one another via
// any sequence of edges. It uses the shortest-path algorithm internally and
// considers all edge types.
func (g *Graph) IsConnected(src, dst store.NodeID) (bool, error) {
	result, err := g.ShortestPath(src, dst, nil)
	if err != nil {
		if errors.Is(err, traversal.ErrNoPath) {
			return false, nil
		}
		var notFound *store.ErrNotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	return len(result.Nodes) > 0, nil
}

// --- Neighbour helpers ---

// NeighboursByNodeType returns all directly connected nodes of a specific
// NodeType, optionally filtered by edge types.
// Pass nil edgeTypes to follow all edge types.
func (g *Graph) NeighboursByNodeType(id store.NodeID, dir store.Direction, nodeType store.NodeType, edgeTypes []store.EdgeType) ([]*store.Node, error) {
	neighbours, err := g.Neighbours(id, dir, edgeTypes)
	if err != nil {
		return nil, err
	}
	var out []*store.Node
	for _, nb := range neighbours {
		if nb.Node.HasLabel(nodeType) {
			out = append(out, nb.Node)
		}
	}
	return out, nil
}

// --- Subgraph extraction ---

// InducedSubgraph returns the nodes and all edges between them for the given
// set of node IDs. The result edges are those whose Src AND Dst are both in
// the provided set.
func (g *Graph) InducedSubgraph(nodeIDs []store.NodeID) ([]*store.Node, []*store.Edge, error) {
	inSet := make(map[store.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		inSet[id] = struct{}{}
	}

	nodes := make([]*store.Node, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		n, err := g.GetNode(id)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, n)
	}

	seen := make(map[store.EdgeID]struct{})
	var edges []*store.Edge
	for _, id := range nodeIDs {
		outEdges, err := g.EdgesOf(id, store.DirectionOutbound, nil)
		if err != nil {
			return nil, nil, err
		}
		for _, e := range outEdges {
			if _, ok := seen[e.ID]; ok {
				continue
			}
			if _, dstIn := inSet[e.Dst]; dstIn {
				seen[e.ID] = struct{}{}
				edges = append(edges, e)
			}
		}
	}
	return nodes, edges, nil
}

// --- Cycle detection ---

// HasCycle reports whether any cycle is reachable from origin within maxDepth
// hops following outbound edges. It uses DFS and detects back-edges in the
// recursion stack. Pass nil edgeTypes to follow all edge types.
//
// A walk deeper than store.MaxRecursionDepth returns ErrBudgetExceeded rather
// than recursing further. maxDepth normally bounds this, but it is the
// caller's number and a large one against a deep graph would otherwise
// overflow the goroutine stack, which is a crash and not something a caller
// can handle. The traversal package's walks are guarded the same way; this one
// is not in that package, which is why it needed its own check and why the
// limit lives in store rather than in either.
func (g *Graph) HasCycle(origin store.NodeID, maxDepth int, edgeTypes []store.EdgeType) (bool, error) {
	visited := make(map[store.NodeID]bool) // true = on current stack
	found := false

	var dfs func(id store.NodeID, depth int) error
	dfs = func(id store.NodeID, depth int) error {
		if found || depth > maxDepth {
			return nil
		}
		if depth > store.MaxRecursionDepth {
			return fmt.Errorf("%w: recursed more than %d levels", store.ErrBudgetExceeded, store.MaxRecursionDepth)
		}
		if onStack, seen := visited[id]; seen {
			if onStack {
				found = true
			}
			return nil
		}
		visited[id] = true
		neighbours, err := g.Neighbours(id, store.DirectionOutbound, edgeTypes)
		if err != nil {
			return err
		}
		for _, nb := range neighbours {
			if err := dfs(nb.Node.ID, depth+1); err != nil {
				return err
			}
			if found {
				return nil
			}
		}
		visited[id] = false // pop from stack
		return nil
	}

	return found, dfs(origin, 0)
}

// --- Result helpers ---

// NodesFromBFS returns the slice of nodes from a BFS result. Nil-safe.
func NodesFromBFS(r *traversal.BFSResult) []*store.Node {
	if r == nil {
		return nil
	}
	return r.Nodes
}

// EdgesFromBFS returns the slice of edges from a BFS result. Nil-safe.
func EdgesFromBFS(r *traversal.BFSResult) []*store.Edge {
	if r == nil {
		return nil
	}
	return r.Edges
}

// NodeIDsFromBFS returns the node IDs from a BFS result for use as scope in
// follow-up queries (e.g. FindPatterns).
func NodeIDsFromBFS(r *traversal.BFSResult) []store.NodeID {
	if r == nil {
		return nil
	}
	ids := make([]store.NodeID, len(r.Nodes))
	for i, n := range r.Nodes {
		ids[i] = n.ID
	}
	return ids
}

// NodeIDsFromPath returns the ordered node IDs from a PathResult.
func NodeIDsFromPath(r *traversal.PathResult) []store.NodeID {
	if r == nil {
		return nil
	}
	ids := make([]store.NodeID, len(r.Nodes))
	for i, n := range r.Nodes {
		ids[i] = n.ID
	}
	return ids
}

// FilterNodesByLabel returns only the nodes from ns that carry the given label.
func FilterNodesByLabel(ns []*store.Node, label store.NodeType) []*store.Node {
	var out []*store.Node
	for _, n := range ns {
		if n.HasLabel(label) {
			out = append(out, n)
		}
	}
	return out
}

// FilterEdgesByLabel returns only the edges from es that carry the given label.
func FilterEdgesByLabel(es []*store.Edge, label store.EdgeType) []*store.Edge {
	var out []*store.Edge
	for _, e := range es {
		if e.HasLabel(label) {
			out = append(out, e)
		}
	}
	return out
}

// --- Internal helpers ---

func dedupeEdgesByID(edges []*store.Edge) []*store.Edge {
	if len(edges) == 0 {
		return nil
	}
	seen := make(map[store.EdgeID]struct{}, len(edges))
	out := make([]*store.Edge, 0, len(edges))
	for _, e := range edges {
		if _, ok := seen[e.ID]; ok {
			continue
		}
		seen[e.ID] = struct{}{}
		out = append(out, e)
	}
	return out
}

func dedupeEdgeIDs(ids []store.EdgeID) []store.EdgeID {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[store.EdgeID]struct{}, len(ids))
	out := make([]store.EdgeID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// --- Query diagnostics ---

// ExplainNodeQuery reports how the planner resolves q: which index drove it, how
// many candidates that produced, and how each remaining filter was applied.
//
// This is how planner behaviour gets verified. A query can return the right
// answer while doing far more work than it needed to, and the difference is
// invisible from the results alone — a test that asserts only on results cannot
// tell an index lookup from a full scan that happened to agree with it.
//
// The plan is diagnostic output. Which index the planner picks may change as the
// cost model improves; the results a query returns may not.
func (g *Graph) ExplainNodeQuery(q store.NodeQuery) (store.QueryPlan, error) {
	ex, ok := g.GraphStore.(store.NodeQueryExplainer)
	if !ok {
		return store.QueryPlan{}, fmt.Errorf("ExplainNodeQuery: %T does not support query plans", g.GraphStore)
	}
	return ex.ExplainNodeQuery(q)
}

// ExplainEdgeQuery reports how the planner resolves q. See ExplainNodeQuery.
func (g *Graph) ExplainEdgeQuery(q store.EdgeQuery) (store.QueryPlan, error) {
	ex, ok := g.GraphStore.(store.EdgeQueryExplainer)
	if !ok {
		return store.QueryPlan{}, fmt.Errorf("ExplainEdgeQuery: %T does not support query plans", g.GraphStore)
	}
	return ex.ExplainEdgeQuery(q)
}

// Sync forces everything written so far to durable storage, returning once it
// survives power loss.
//
// This matters because individual writes are *not* synced as they happen: an
// fsync per AddNode would turn a ~6 µs operation into a ~1 ms one. Batch commits
// sync by default; single writes rely on this, on Compact, or on Close.
//
// On a backend without durability — the in-memory store — this is a no-op and
// returns nil.
func (g *Graph) Sync() error {
	if s, ok := g.GraphStore.(store.Syncer); ok {
		return s.Sync()
	}
	return nil
}
