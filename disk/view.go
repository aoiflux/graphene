package disk

// The read view: what a reader is allowed to see, and when.
//
// # The problem this solves
//
// Before this file the store's state was seven loose maps plus a CSR pointer,
// each field read under s.mu and all of them replaced wholesale by Compact. A
// reader could name none of it. There was no way to say "the graph I am
// reading" and therefore no way to read twice and be promised the same answer —
// the limitation TECHNICAL_DETAILS §16 recorded as "no snapshot isolation" and
// the reason a BFS running beside a writer could follow an edge to a node that
// had already been deleted.
//
// # The two words
//
//   - view pairs the CSR image with the delta layer sitting on top of it. The
//     pair is atomic because the halves are not independent: a delta layer
//     belongs to exactly one image, and pinning a new layer against an old image
//     loses everything the compaction that produced them folded down.
//   - epoch orders mutations. Every change to the delta stamps one, and a reader
//     at epoch E sees a change if and only if the change's epoch is at most E.
//
// They are separate atomics, which §9.2 warns about — that section records a
// lock-free path that sampled a generation counter and a pointer and got a
// consistent-looking pair that was wrong. The difference here is the direction
// of the error. Views only ever gain content and epochs only ever advance, so
// reading the epoch first and the view second yields a pair where the view is
// never missing a commit the epoch names. The one operation that breaks that
// monotonicity — Compact, which replaces a layer rather than extending it —
// holds the write lock, and Snapshot reads both words under the read lock. That
// is a smaller thing to keep true than making a torn pair harmless.
//
// # Why versions rather than a copy
//
// A snapshot could be a copy of the delta maps, and for the memory backend it
// is. Here it cannot be: the delta is unbounded between compactions, so copying
// it would make opening a snapshot cost O(everything written since the last
// compaction) in both time and resident bytes — the opposite of the bounded
// memory the same design is meant to deliver. Versioning makes a snapshot two
// words and a registry entry.

import (
	"slices"

	"github.com/aoiflux/graphene/store"
)

// nodeVersion is one version of one node, newest first along prev.
//
// node == nil is a tombstone, which is what lets a delete be visible at one
// epoch and invisible at an older one. The delete masks this replaces could not
// express that: they recorded that an ID was gone, never when.
type nodeVersion struct {
	epoch uint64
	node  *store.Node
	prev  *nodeVersion
}

// edgeVersion is nodeVersion for edges.
type edgeVersion struct {
	epoch uint64
	edge  *store.Edge
	prev  *edgeVersion
}

// at resolves the version visible at epoch.
//
// ok reports whether the delta has anything to say about this ID at that epoch
// at all — distinct from the record being nil, which says the ID was deleted.
// Callers need both apart: "no opinion" falls through to the CSR, "deleted"
// does not.
func (v *nodeVersion) at(epoch uint64) (n *store.Node, ok bool) {
	for cur := v; cur != nil; cur = cur.prev {
		if cur.epoch <= epoch {
			return cur.node, true
		}
	}
	return nil, false
}

func (v *edgeVersion) at(epoch uint64) (e *store.Edge, ok bool) {
	for cur := v; cur != nil; cur = cur.prev {
		if cur.epoch <= epoch {
			return cur.edge, true
		}
	}
	return nil, false
}

// truncateNodeChain drops versions no open snapshot can still reach.
//
// retain is the oldest epoch any live snapshot reads at; ok is false when there
// are none, which is the common case and the one worth being cheap: with no
// snapshot open every chain is exactly one version long, so the memory profile
// is the plain map this replaced plus a three-word header per entry.
//
// With snapshots open, everything above retain is kept (a newer snapshot may
// want it) along with the first version at or below it (the oldest snapshot
// wants exactly that one). Everything past that is unreachable by definition.
func truncateNodeChain(head *nodeVersion, retain uint64, ok bool) {
	if !ok {
		head.prev = nil
		return
	}
	cur := head
	for cur.prev != nil && cur.epoch > retain {
		cur = cur.prev
	}
	cur.prev = nil
}

func truncateEdgeChain(head *edgeVersion, retain uint64, ok bool) {
	if !ok {
		head.prev = nil
		return
	}
	cur := head
	for cur.prev != nil && cur.epoch > retain {
		cur = cur.prev
	}
	cur.prev = nil
}

// deltaAdj is the delta layer's adjacency for one node.
//
// Append-only. An edge removed from the graph stays listed here and is dropped
// at read time by resolving its ID through the version chain, because a
// snapshot at an older epoch still needs to traverse it. Compaction is what
// reclaims the space, which is the same thing that reclaims a tombstone.
type deltaAdj struct {
	out []store.EdgeID
	in  []store.EdgeID
}

// deltaLayer is everything written since the CSR image beneath it was built.
//
// Its maps are ordinary Go maps guarded by Store.mu — versioning makes reads
// consistent, not lock-free. What it does buy is that a layer Compact has
// superseded is never written again, so a snapshot older than the last
// compaction reads with no lock at all.
type deltaLayer struct {
	nodes map[store.NodeID]*nodeVersion
	edges map[store.EdgeID]*edgeVersion
	adj   map[store.NodeID]*deltaAdj

	// Type postings. Candidate supersets, not answers: entries are only ever
	// left behind while a snapshot is open, and every read re-resolves each
	// candidate against the record visible at its own epoch. A stale posting
	// therefore costs a filtered-out candidate and never a wrong result.
	nodesByType map[store.NodeType][]store.NodeID
	edgesByType map[store.EdgeType][]store.EdgeID

	// Counts maintained rather than derived, because both are asked for often
	// enough that scanning the maps to answer would cost more than keeping them.
	//
	// live is the number of entries whose newest version is a record rather than
	// a tombstone — the delta size an operator means by "delta size". masked is
	// the number of tombstones that hide a record the image still holds, which
	// is the only thing that can make a CSR adjacency count wrong: while it is
	// zero, degree answers from the offset arrays alone.
	liveNodes   int
	liveEdges   int
	maskedNodes int
	maskedEdges int
}

func newDeltaLayer() *deltaLayer {
	return &deltaLayer{
		nodes:       make(map[store.NodeID]*nodeVersion),
		edges:       make(map[store.EdgeID]*edgeVersion),
		adj:         make(map[store.NodeID]*deltaAdj),
		nodesByType: make(map[store.NodeType][]store.NodeID),
		edgesByType: make(map[store.EdgeType][]store.EdgeID),
	}
}

// since builds the layer that survives a compaction: everything this layer
// holds that the new image does not, and nothing else.
//
// epoch is the newest epoch folded into csr, so a chain whose head is at or
// before it has been fully absorbed and is dropped — which is what makes
// compaction the point where a delete stops costing memory, exactly as an
// emptied layer used to.
//
// Only the head of a surviving chain is carried. The versions beneath it are
// not reachable through this layer: the new view is published under the store
// lock together with the epoch, so nothing can pin it at an epoch older than
// the compaction, and every read of it therefore resolves to the head. Older
// readers hold the *previous* view, whose chains this does not touch — which is
// also why the surviving heads are copied rather than relinked.
//
// The returned count is how many image records the surviving layer supersedes,
// for csrShadowed. It is not the same figure as maskedNodes+maskedEdges: a
// tombstone and an update both shadow, only a tombstone masks.
func (l *deltaLayer) since(epoch uint64, csr *CSRGraph) (*deltaLayer, int64) {
	out := newDeltaLayer()
	var shadowed int64

	hasNode := func(id store.NodeID) bool {
		if csr == nil {
			return false
		}
		_, found := csr.GetNode(id)
		return found
	}
	hasEdge := func(id store.EdgeID) bool {
		if csr == nil {
			return false
		}
		_, found := csr.GetEdge(id)
		return found
	}

	for id, ver := range l.nodes {
		if ver.epoch <= epoch {
			continue
		}
		out.nodes[id] = &nodeVersion{epoch: ver.epoch, node: ver.node}
		if hasNode(id) {
			shadowed++
		}
		if ver.node == nil {
			if hasNode(id) {
				out.maskedNodes++
			}
			continue
		}
		out.liveNodes++
		appendNodeLabels(out, id, ver.node.Labels)
		ensureAdj(out, id)
	}

	for id, ver := range l.edges {
		if ver.epoch <= epoch {
			continue
		}
		out.edges[id] = &edgeVersion{epoch: ver.epoch, edge: ver.edge}
		inCSR := hasEdge(id)
		if inCSR {
			shadowed++
		}
		if ver.edge == nil {
			if inCSR {
				out.maskedEdges++
			}
			continue
		}
		out.liveEdges++
		appendEdgeLabels(out, id, ver.edge.Labels)
		// The same rule putEdge applies: delta adjacency lists only edges the
		// image does not hold, so the two never name the same edge and the
		// degree counts add. An edge created before the compaction and updated
		// after it is in the rebuilt image, so its adjacency is there too.
		if !inCSR {
			ensureAdj(out, ver.edge.Src).out = append(ensureAdj(out, ver.edge.Src).out, id)
			ensureAdj(out, ver.edge.Dst).in = append(ensureAdj(out, ver.edge.Dst).in, id)
		}
	}

	// Postings are built by appending in map order and sorted once, rather than
	// through indexNodeLabels' sorted insert: this runs over the whole surviving
	// layer at once, where the insert is quadratic and this is not. Sortedness
	// itself is not optional — the query paths merge these lists, and
	// VerifyIndexes checks it.
	for t, ids := range out.nodesByType {
		slices.Sort(ids)
		out.nodesByType[t] = ids
	}
	for t, ids := range out.edgesByType {
		slices.Sort(ids)
		out.edgesByType[t] = ids
	}
	for _, a := range out.adj {
		slices.Sort(a.out)
		slices.Sort(a.in)
	}

	return out, shadowed
}

// appendNodeLabels adds id to each distinct label posting without maintaining
// order; since sorts once at the end.
func appendNodeLabels(d *deltaLayer, id store.NodeID, labels []store.NodeType) {
	for i, lbl := range labels {
		if containsNodeTypeValue(labels[:i], lbl) {
			continue
		}
		d.nodesByType[lbl] = append(d.nodesByType[lbl], id)
	}
}

func appendEdgeLabels(d *deltaLayer, id store.EdgeID, labels []store.EdgeType) {
	for i, lbl := range labels {
		if containsEdgeTypeValue(labels[:i], lbl) {
			continue
		}
		d.edgesByType[lbl] = append(d.edgesByType[lbl], id)
	}
}

// view is a CSR image and the delta layer that sits on it. Immutable as a pair:
// replaced only by Compact, and only under the write lock.
type view struct {
	csr   *CSRGraph // nil until the first compaction
	delta *deltaLayer
}

// reader is a resolved read context — a pinned view at a fixed epoch.
//
// Every read in the backend goes through one. The store's own methods build a
// fresh reader per call at the newest visible epoch, which is the point-in-time
// behaviour that was always documented; a Snapshot holds one still, which is
// the behaviour that was not available before.
type reader struct {
	v     *view
	epoch uint64
}

// node resolves the record visible to r, delta first and then the image.
func (r reader) node(id store.NodeID) (*store.Node, bool) {
	if ver, in := r.v.delta.nodes[id]; in {
		if n, ok := ver.at(r.epoch); ok {
			return n, n != nil
		}
	}
	if r.v.csr == nil {
		return nil, false
	}
	rec, found := r.v.csr.GetNode(id)
	if !found {
		return nil, false
	}
	return &store.Node{ID: rec.ID, Labels: rec.Labels, Properties: csrBytes(rec.Properties)}, true
}

func (r reader) edge(id store.EdgeID) (*store.Edge, bool) {
	if ver, in := r.v.delta.edges[id]; in {
		if e, ok := ver.at(r.epoch); ok {
			return e, e != nil
		}
	}
	if r.v.csr == nil {
		return nil, false
	}
	rec, found := r.v.csr.GetEdge(id)
	if !found {
		return nil, false
	}
	return rawEdgeToStore(rec), true
}

// nodeExists answers the same question as node without materialising a record.
func (r reader) nodeExists(id store.NodeID) bool {
	if ver, in := r.v.delta.nodes[id]; in {
		if n, ok := ver.at(r.epoch); ok {
			return n != nil
		}
	}
	return r.nodeInCSR(id)
}

func (r reader) edgeExists(id store.EdgeID) bool {
	if ver, in := r.v.delta.edges[id]; in {
		if e, ok := ver.at(r.epoch); ok {
			return e != nil
		}
	}
	return r.edgeInCSR(id)
}

func (r reader) nodeInCSR(id store.NodeID) bool {
	if r.v.csr == nil {
		return false
	}
	_, found := r.v.csr.GetNode(id)
	return found
}

func (r reader) edgeInCSR(id store.EdgeID) bool {
	if r.v.csr == nil {
		return false
	}
	_, found := r.v.csr.GetEdge(id)
	return found
}

// nodeHasLabel reports whether the record visible to r carries t, without
// allocating a *store.Node.
func (r reader) nodeHasLabel(id store.NodeID, t store.NodeType) bool {
	if ver, in := r.v.delta.nodes[id]; in {
		if n, ok := ver.at(r.epoch); ok {
			return n != nil && n.HasLabel(t)
		}
	}
	if r.v.csr == nil {
		return false
	}
	rec, found := r.v.csr.GetNode(id)
	return found && nodeRecordHasLabel(rec.Labels, t)
}

func (r reader) edgeHasLabel(id store.EdgeID, t store.EdgeType) bool {
	if ver, in := r.v.delta.edges[id]; in {
		if e, ok := ver.at(r.epoch); ok {
			return e != nil && e.HasLabel(t)
		}
	}
	if r.v.csr == nil {
		return false
	}
	rec, found := r.v.csr.GetEdge(id)
	return found && rawEdgeHasLabel(rec.Labels, t)
}

// deltaEdge resolves an edge ID against the delta alone.
//
// Split out because the adjacency walks need all three answers apart: an edge
// the delta owns, an edge the delta has deleted, and an edge the delta has
// never heard of, which the image may still hold. known is false for the last;
// a known edge with a nil record is a tombstone.
func (r reader) deltaEdge(id store.EdgeID) (e *store.Edge, known bool) {
	ver, in := r.v.delta.edges[id]
	if !in {
		return nil, false
	}
	res, ok := ver.at(r.epoch)
	if !ok {
		return nil, false
	}
	return res, true
}

// deltaNodeKnown reports whether the delta has an opinion about id at this
// epoch, whatever that opinion is. The counting paths use it to decide whether
// a CSR record has already been accounted for by the delta pass.
func (r reader) deltaNodeKnown(id store.NodeID) bool {
	ver, in := r.v.delta.nodes[id]
	if !in {
		return false
	}
	_, ok := ver.at(r.epoch)
	return ok
}

func (r reader) deltaEdgeKnown(id store.EdgeID) bool {
	ver, in := r.v.delta.edges[id]
	if !in {
		return false
	}
	_, ok := ver.at(r.epoch)
	return ok
}
