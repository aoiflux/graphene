package disk

// The traversal and aggregate reads, expressed against a pinned view.
//
// These were methods on Store that read s.csr and the delta maps directly. They
// are the same algorithms — the delta layer first, then the image with
// tombstones skipped and delta overrides taking precedence — resolved against a
// reader instead of against whatever the store happened to hold at each field
// access. That is what lets one snapshot and the live store share a single
// implementation of each, rather than growing a second copy that has to be kept
// in step.

import (
	"github.com/aoiflux/graphene/store"
)

// deltaEdgeIDs returns the delta adjacency for id in dir, without copying when
// only one side is wanted.
func (r reader) deltaEdgeIDs(id store.NodeID, dir store.Direction) (first, second []store.EdgeID) {
	da := r.v.delta.adj[id]
	if da == nil {
		return nil, nil
	}
	switch dir {
	case store.DirectionOutbound:
		return da.out, nil
	case store.DirectionInbound:
		return da.in, nil
	default:
		return da.out, da.in
	}
}

// csrEdgeIDs is deltaEdgeIDs for the image.
func (r reader) csrEdgeIDs(id store.NodeID, dir store.Direction) (first, second []store.EdgeID) {
	if r.v.csr == nil {
		return nil, nil
	}
	switch dir {
	case store.DirectionOutbound:
		return r.v.csr.OutboundEdgeIDs(id), nil
	case store.DirectionInbound:
		return r.v.csr.InboundEdgeIDs(id), nil
	default:
		return r.v.csr.OutboundEdgeIDs(id), r.v.csr.InboundEdgeIDs(id)
	}
}

// collectDeltaEdges appends the live delta edges named by eids to dst.
//
// The adjacency lists are append-only, so an entry here may name an edge this
// reader cannot see — one deleted at or before its epoch, or one created after
// it. Resolving each ID through the version chain is what filters both out.
func (r reader) collectDeltaEdges(dst []*store.Edge, eids []store.EdgeID, edgeTypes []store.EdgeType) []*store.Edge {
	for _, eid := range eids {
		e, known := r.deltaEdge(eid)
		if !known || e == nil {
			continue
		}
		if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, e) {
			continue
		}
		dst = append(dst, e)
	}
	return dst
}

// collectCSREdges appends the live image edges in raw to dst.
func (r reader) collectCSREdges(dst []*store.Edge, raw []rawEdge, edgeTypes []store.EdgeType) []*store.Edge {
	for _, re := range raw {
		// A CSR edge the delta has an opinion about is answered from the delta:
		// updated (emit that copy, whose labels may differ), deleted (skip), or
		// not yet created at this epoch (fall through to the image copy, which
		// is what the graph held then). Endpoints are immutable, so CSR
		// adjacency still lists an updated edge correctly.
		if de, known := r.deltaEdge(re.ID); known {
			if de == nil {
				continue
			}
			if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, de) {
				continue
			}
			dst = append(dst, de)
			continue
		}
		if edgeTypes != nil && !rawEdgeMatchesFilter(edgeTypes, re.Labels) {
			continue
		}
		dst = append(dst, rawEdgeToStore(re))
	}
	return dst
}

// edgesOf collects the live edges incident to id.
func (r reader) edgesOf(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) []*store.Edge {
	var result []*store.Edge

	dOut, dIn := r.deltaEdgeIDs(id, dir)
	result = r.collectDeltaEdges(result, dOut, edgeTypes)
	result = r.collectDeltaEdges(result, dIn, edgeTypes)

	if r.v.csr == nil {
		return result
	}

	// Image layer. Each direction is collected in its own pass rather than
	// concatenated into one slice first: the concatenation existed only so a
	// single loop could be written, and it copied every incident edge record to
	// buy that.
	switch dir {
	case store.DirectionOutbound:
		out, _ := r.v.csr.OutboundEdges(id)
		result = r.collectCSREdges(result, out, edgeTypes)
	case store.DirectionInbound:
		in, _ := r.v.csr.InboundEdges(id)
		result = r.collectCSREdges(result, in, edgeTypes)
	default:
		out, e1 := r.v.csr.OutboundEdges(id)
		in, e2 := r.v.csr.InboundEdges(id)
		if e1 == nil {
			result = r.collectCSREdges(result, out, edgeTypes)
		}
		if e2 == nil {
			result = r.collectCSREdges(result, in, edgeTypes)
		}
	}
	return result
}

// incidentEdges walks the same sequence as edgesOf but appends IDs to the
// caller's buffer instead of materialising edge records.
func (r reader) incidentEdges(dst []store.IncidentEdge, id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) []store.IncidentEdge {
	add := func(eid store.EdgeID, src, dstNode store.NodeID) {
		nb := dstNode
		if src != id {
			nb = src
		}
		dst = append(dst, store.IncidentEdge{Edge: eid, Neighbour: nb})
	}

	dOut, dIn := r.deltaEdgeIDs(id, dir)
	appendDelta := func(eids []store.EdgeID) {
		for _, eid := range eids {
			e, known := r.deltaEdge(eid)
			if !known || e == nil {
				continue
			}
			if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, e) {
				continue
			}
			add(eid, e.Src, e.Dst)
		}
	}
	appendDelta(dOut)
	appendDelta(dIn)

	if r.v.csr == nil {
		return dst
	}
	cOut, cIn := r.csrEdgeIDs(id, dir)
	appendCSR := func(eids []store.EdgeID) {
		for _, eid := range eids {
			if de, known := r.deltaEdge(eid); known {
				if de == nil {
					continue
				}
				if edgeTypes != nil && !storeEdgeMatchesFilter(edgeTypes, de) {
					continue
				}
				add(eid, de.Src, de.Dst)
				continue
			}
			rec, found := r.v.csr.GetEdge(eid)
			if !found {
				continue
			}
			if edgeTypes != nil && !rawEdgeMatchesFilter(edgeTypes, rec.Labels) {
				continue
			}
			add(eid, rec.Src, rec.Dst)
		}
	}
	appendCSR(cOut)
	appendCSR(cIn)
	return dst
}

// neighbourDedupeLinear is the neighbour count up to which the dedupe is a
// linear scan of a stack array rather than a map.
//
// A map costs an allocation (plus its buckets) on every call, and the reader is
// per-call, so walker.beginExpansion's trick of reusing one map and clearing it
// has nothing here to hang off — and a reader value is used concurrently, so a
// Store field would be a data race. A linear scan needs no allocation at all,
// which for the degrees this path actually sees is not a compromise but simply
// the faster answer: at 32 entries the scan is a handful of cache-resident
// comparisons against a map's hash, probe and bucket walk.
const neighbourDedupeLinear = 32

// neighbours resolves edgesOf into distinct neighbouring nodes.
func (r reader) neighbours(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) []store.NeighbourResult {
	edges := r.edgesOf(id, dir, edgeTypes)

	// Above the threshold the scan's O(n^2) would overtake the map it saves, so
	// the map comes back. Sized from the edge count, which bounds the distinct
	// neighbours exactly.
	var seen map[store.NodeID]struct{}
	var scratch [neighbourDedupeLinear]store.NodeID
	linear := scratch[:0]
	if len(edges) > neighbourDedupeLinear {
		seen = make(map[store.NodeID]struct{}, len(edges))
	}

	var results []store.NeighbourResult
	if len(edges) > 0 {
		results = make([]store.NeighbourResult, 0, len(edges))
	}
	for _, e := range edges {
		nbID := e.Dst
		if e.Src != id {
			nbID = e.Src
		}
		if seen != nil {
			if _, already := seen[nbID]; already {
				continue
			}
			seen[nbID] = struct{}{}
		} else {
			already := false
			for _, s := range linear {
				if s == nbID {
					already = true
					break
				}
			}
			if already {
				continue
			}
			linear = append(linear, nbID)
		}
		n, ok := r.node(nbID)
		if !ok {
			continue
		}
		results = append(results, store.NeighbourResult{Node: n, Edge: e})
	}
	return results
}

// degree counts live incident edges without materialising any of them.
//
// Delta adjacency and CSR adjacency never list the same edge — an edge is
// recorded in delta adjacency only when the image does not already hold it — so
// the two counts simply add.
func (r reader) degree(id store.NodeID, dir store.Direction) int {
	// Written flat, switching on the direction once, rather than through the
	// deltaEdgeIDs/csrEdgeIDs helpers the other walks share. This is the
	// allocation-free path a hub's degree is answered from — 27 ns against
	// HEAD's — and routing it through two helpers that each return a pair of
	// slices, half of them nil, cost 22% of that in the interleaved A/B. The
	// duplication is the price of the number.
	total := 0
	if a := r.v.delta.adj[id]; a != nil {
		switch dir {
		case store.DirectionOutbound:
			total += r.countLiveDelta(a.out)
		case store.DirectionInbound:
			total += r.countLiveDelta(a.in)
		default:
			total += r.countLiveDelta(a.out) + r.countLiveDelta(a.in)
		}
	}

	if r.v.csr == nil {
		return total
	}
	// On a compacted store with no pending deletions — the steady state for a
	// read-heavy workload — no CSR edge can be masked, so the per-edge probe is
	// pure overhead and the offset arithmetic answers on its own.
	if r.v.delta.maskedEdges == 0 {
		switch dir {
		case store.DirectionOutbound:
			return total + len(r.v.csr.OutboundEdgeIDs(id))
		case store.DirectionInbound:
			return total + len(r.v.csr.InboundEdgeIDs(id))
		default:
			return total + len(r.v.csr.OutboundEdgeIDs(id)) + len(r.v.csr.InboundEdgeIDs(id))
		}
	}
	switch dir {
	case store.DirectionOutbound:
		total += r.countUnmaskedCSR(r.v.csr.OutboundEdgeIDs(id))
	case store.DirectionInbound:
		total += r.countUnmaskedCSR(r.v.csr.InboundEdgeIDs(id))
	default:
		total += r.countUnmaskedCSR(r.v.csr.OutboundEdgeIDs(id))
		total += r.countUnmaskedCSR(r.v.csr.InboundEdgeIDs(id))
	}
	return total
}

// countLiveDelta counts the IDs that resolve to a live delta record for r.
func (r reader) countLiveDelta(eids []store.EdgeID) int {
	n := 0
	for _, eid := range eids {
		if e, known := r.deltaEdge(eid); known && e != nil {
			n++
		}
	}
	return n
}

// countUnmaskedCSR counts image edges the delta has not tombstoned for r.
func (r reader) countUnmaskedCSR(eids []store.EdgeID) int {
	n := 0
	for _, eid := range eids {
		if e, known := r.deltaEdge(eid); known && e == nil {
			continue
		}
		n++
	}
	return n
}

// degreeFiltered counts incident edges carrying one of edgeTypes.
//
// The untyped path reads offsets and is O(1); this one has to inspect each
// incident edge's labels, so it is O(degree) — but O(degree) reads, not
// O(degree) allocations. Routing this through edgesOf instead, as it used to,
// built a *store.Edge and cloned a property blob per incident edge just to take
// len() of the result: 73.8 µs against 14.22 ns for the same node untyped.
//
// The order of checks mirrors edgesOf exactly, so the count always equals
// len(edgesOf(...)).
func (r reader) degreeFiltered(id store.NodeID, dir store.Direction, edgeTypes []store.EdgeType) int {
	total := 0
	dOut, dIn := r.deltaEdgeIDs(id, dir)
	countDelta := func(eids []store.EdgeID) {
		for _, eid := range eids {
			e, known := r.deltaEdge(eid)
			if !known || e == nil {
				continue
			}
			if storeEdgeMatchesFilter(edgeTypes, e) {
				total++
			}
		}
	}
	countDelta(dOut)
	countDelta(dIn)

	if r.v.csr == nil {
		return total
	}
	pristine := len(r.v.delta.edges) == 0
	cOut, cIn := r.csrEdgeIDs(id, dir)
	countCSR := func(eids []store.EdgeID) {
		if pristine {
			for _, eid := range eids {
				if rec, found := r.v.csr.GetEdge(eid); found && rawEdgeMatchesFilter(edgeTypes, rec.Labels) {
					total++
				}
			}
			return
		}
		for _, eid := range eids {
			if de, known := r.deltaEdge(eid); known {
				if de != nil && storeEdgeMatchesFilter(edgeTypes, de) {
					total++
				}
				continue
			}
			rec, found := r.v.csr.GetEdge(eid)
			if found && rawEdgeMatchesFilter(edgeTypes, rec.Labels) {
				total++
			}
		}
	}
	countCSR(cOut)
	countCSR(cIn)
	return total
}

// degreeSum totals live incident-edge counts for ids.
func (r reader) degreeSum(ids []store.NodeID, dir store.Direction) int {
	total := 0
	for _, id := range ids {
		total += r.degree(id, dir)
	}
	return total
}

// incidentEdgeIDsOf returns the deduped, still-live edge IDs incident to id in
// both directions. Used by the delete cascade, which needs the set rather than
// the records.
func (r reader) incidentEdgeIDsOf(id store.NodeID) []store.EdgeID {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	add := func(eid store.EdgeID) {
		if !r.edgeExists(eid) {
			return
		}
		if _, ok := seen[eid]; ok {
			return
		}
		seen[eid] = struct{}{}
		out = append(out, eid)
	}
	dOut, dIn := r.deltaEdgeIDs(id, store.DirectionBoth)
	for _, eid := range dOut {
		add(eid)
	}
	for _, eid := range dIn {
		add(eid)
	}
	cOut, cIn := r.csrEdgeIDs(id, store.DirectionBoth)
	for _, eid := range cOut {
		add(eid)
	}
	for _, eid := range cIn {
		add(eid)
	}
	return out
}

// incidentEdgeIDsForAll is incidentEdgeIDsOf over several nodes in one
// direction, deduplicated across the whole set.
func (r reader) incidentEdgeIDsForAll(ids []store.NodeID, dir store.Direction) []store.EdgeID {
	seen := make(map[store.EdgeID]struct{})
	var out []store.EdgeID
	add := func(eids []store.EdgeID) {
		for _, eid := range eids {
			if _, ok := seen[eid]; ok {
				continue
			}
			if !r.edgeExists(eid) {
				continue
			}
			seen[eid] = struct{}{}
			out = append(out, eid)
		}
	}
	for _, id := range ids {
		dOut, dIn := r.deltaEdgeIDs(id, dir)
		add(dOut)
		add(dIn)
		cOut, cIn := r.csrEdgeIDs(id, dir)
		add(cOut)
		add(cIn)
	}
	return out
}

// nodesByType returns the live node IDs carrying t.
//
// The delta postings and the image's label index are candidate sources; each
// candidate is re-resolved against this reader, because a posting may name a
// node the reader cannot see, one whose label was since removed, or one that
// was deleted.
func (r reader) nodesByType(t store.NodeType) []store.NodeID {
	candidates := make([]store.NodeID, len(r.v.delta.nodesByType[t]))
	copy(candidates, r.v.delta.nodesByType[t])
	if r.v.csr != nil {
		candidates = append(candidates, r.v.csr.NodesByType(t)...)
	}
	seen := make(map[store.NodeID]struct{}, len(candidates))
	out := make([]store.NodeID, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := seen[id]; ok {
			continue
		}
		if !r.nodeHasLabel(id, t) {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (r reader) edgesByType(t store.EdgeType) []store.EdgeID {
	candidates := make([]store.EdgeID, len(r.v.delta.edgesByType[t]))
	copy(candidates, r.v.delta.edgesByType[t])
	if r.v.csr != nil {
		candidates = append(candidates, r.v.csr.EdgesByType(t)...)
	}
	seen := make(map[store.EdgeID]struct{}, len(candidates))
	out := make([]store.EdgeID, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := seen[id]; ok {
			continue
		}
		if !r.edgeHasLabel(id, t) {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// nodeCount counts live nodes across both layers.
//
// The delta pass counts every ID the delta has an opinion about and resolves
// to a live record — which covers both delta-only nodes and image nodes the
// delta has updated. The image pass then adds the records the delta has nothing
// to say about at this epoch. "Nothing to say" is the load-bearing part: a node
// created after this reader's epoch is in the map but invisible here, and the
// image copy (if any) is what the graph held then.
func (r reader) nodeCount() uint64 {
	var total uint64
	for _, ver := range r.v.delta.nodes {
		if n, ok := ver.at(r.epoch); ok && n != nil {
			total++
		}
	}
	if r.v.csr == nil {
		return total
	}
	for i := 1; i < len(r.v.csr.nodes); i++ {
		id := r.v.csr.nodes[i].ID
		if id == store.InvalidNodeID {
			continue
		}
		if r.deltaNodeKnown(id) {
			continue
		}
		total++
	}
	return total
}

func (r reader) edgeCount() uint64 {
	var total uint64
	for _, ver := range r.v.delta.edges {
		if e, ok := ver.at(r.epoch); ok && e != nil {
			total++
		}
	}
	if r.v.csr == nil {
		return total
	}
	for i := 1; i < len(r.v.csr.edges); i++ {
		id := r.v.csr.edges[i].ID
		if id == store.InvalidEdgeID {
			continue
		}
		if r.deltaEdgeKnown(id) {
			continue
		}
		total++
	}
	return total
}

// allNodeIDs returns every live node ID. The planner's last-resort driver.
func (r reader) allNodeIDs() []store.NodeID {
	out := make([]store.NodeID, 0, len(r.v.delta.nodes))
	seen := make(map[store.NodeID]struct{}, len(r.v.delta.nodes))
	for id, ver := range r.v.delta.nodes {
		if n, ok := ver.at(r.epoch); !ok || n == nil {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if r.v.csr == nil {
		return out
	}
	for i := 1; i < len(r.v.csr.nodes); i++ {
		id := r.v.csr.nodes[i].ID
		if id == store.InvalidNodeID {
			continue
		}
		if r.deltaNodeKnown(id) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (r reader) allEdgeIDs() []store.EdgeID {
	out := make([]store.EdgeID, 0, len(r.v.delta.edges))
	seen := make(map[store.EdgeID]struct{}, len(r.v.delta.edges))
	for id, ver := range r.v.delta.edges {
		if e, ok := ver.at(r.epoch); !ok || e == nil {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if r.v.csr == nil {
		return out
	}
	for i := 1; i < len(r.v.csr.edges); i++ {
		id := r.v.csr.edges[i].ID
		if id == store.InvalidEdgeID {
			continue
		}
		if r.deltaEdgeKnown(id) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// nodeLabelCandidateCount is an upper bound on the number of nodes carrying any
// of types. It double-counts a node present in both layers and ignores
// tombstones.
//
// An upper bound is exactly what costing needs: the chosen driver must be a
// superset of the answer, so overestimating can only make the planner more
// reluctant to pick labels — never wrong, just occasionally conservative.
func (r reader) nodeLabelCandidateCount(types []store.NodeType) int {
	total := 0
	for _, t := range types {
		total += len(r.v.delta.nodesByType[t])
		if r.v.csr != nil {
			// NodesByType aliases CSR memory; len is O(1) and nothing escapes.
			total += len(r.v.csr.NodesByType(t))
		}
	}
	return total
}

func (r reader) edgeLabelCandidateCount(types []store.EdgeType) int {
	total := 0
	for _, t := range types {
		total += len(r.v.delta.edgesByType[t])
		if r.v.csr != nil {
			total += len(r.v.csr.EdgesByType(t))
		}
	}
	return total
}
