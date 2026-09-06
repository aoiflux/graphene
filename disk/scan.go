package disk

// Chunked iteration over a snapshot.
//
// The obvious implementation of a scan — range the delta map, yield each ID,
// then walk the image — cannot be written here, because a snapshot whose view
// the store is still writing to reads under s.mu and yielding to caller code
// while holding it is a deadlock waiting for a caller that reads the store from
// inside its own loop. So a scan collects a bounded batch under the lock,
// releases it, and yields the batch. The lock is held for a fixed number of
// records at a time and never across caller code.
//
// The batch also settles the memory question honestly. A scan is not O(1): the
// delta half has to be gathered and sorted up front, because a map cannot be
// ranged across a lock release and because the result is promised ascending.
// What it is not is O(V+E) — the image, which is the large half, is walked in
// place, so peak memory is the delta plus one batch rather than the graph. On a
// compacted store that is the whole win, which is also when it matters.

import (
	"iter"
	"slices"

	"github.com/aoiflux/graphene/store"
)

// scanChunk is the largest number of IDs a scan resolves per acquisition of the
// store lock: large enough that the lock traffic disappears against the yield,
// small enough that the buffer is a rounding error next to the delta beside it.
//
// scanFirstChunk is where a scan starts, and it doubles from there. A caller
// that takes one ID and breaks should not pay to resolve five hundred, and a
// caller draining the graph reaches the full batch after five of them — so the
// ramp costs the long scan almost nothing and saves the short one almost
// everything. It is the same reason the window is pushed into the label
// driver: the cost is supposed to track the answer.
const (
	scanChunk      = 512
	scanFirstChunk = 32
)

// scanFill appends at most limit IDs to dst and reports whether the scan is
// done. It runs with the store lock held whenever the snapshot's view still
// needs it.
type scanFill[T store.EntityID] func(r reader, dst []T, limit int) ([]T, bool)

// scanChunked turns a fill function into a sequence.
//
// The snapshot is re-validated once per batch rather than once per ID: a
// snapshot that closes mid-scan has to be reported, and checking between
// batches bounds how long that takes without putting an atomic load on every
// element. Already-collected IDs stay valid across a Close because they are
// numbers; it is the next batch that would read state the close released.
func scanChunked[T store.EntityID](sn *snapshot, fill scanFill[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		buf := make([]T, 0, scanChunk)
		size := scanFirstChunk
		for {
			r, locked, err := sn.use()
			if err != nil {
				yield(zero, err)
				return
			}
			if locked {
				sn.s.mu.RLock()
			}
			batch, done := fill(r, buf[:0], size)
			if locked {
				sn.s.mu.RUnlock()
			}
			buf = batch
			for _, id := range batch {
				if !yield(id, nil) {
					return
				}
			}
			if done {
				return
			}
			if size < scanChunk {
				size *= 2
			}
		}
	}
}

// --- all live IDs ---

// nodeScanCursor merges the delta's live IDs with the image's, ascending.
//
// The two halves are disjoint by construction — an image record the delta has
// an opinion about is skipped on the image side, which is the same rule
// allNodeIDs and nodeCount apply — so the merge has nothing to deduplicate. It
// deduplicates anyway, for the reason the label merge does: a merge that met a
// duplicate would drop a record rather than repeat one.
type nodeScanCursor struct {
	delta   []store.NodeID
	di      int
	ii      int // next image record index
	n       int // one past the last image record index
	pending store.NodeID
	held    bool
	started bool
}

func (c *nodeScanCursor) start(r reader) {
	c.delta = r.deltaLiveNodeIDs()
	slices.Sort(c.delta)
	c.ii, c.n = 1, r.imageNodeLen()
	c.started = true
}

// nextImage returns the next live image node ID the delta says nothing about.
func (c *nodeScanCursor) nextImage(r reader) (store.NodeID, bool) {
	for c.ii < c.n {
		id := r.v.csr.nodes[c.ii].ID
		c.ii++
		if id == store.InvalidNodeID || r.deltaNodeKnown(id) {
			continue
		}
		return id, true
	}
	return store.InvalidNodeID, false
}

func (c *nodeScanCursor) fill(r reader, dst []store.NodeID, limit int) ([]store.NodeID, bool) {
	if !c.started {
		c.start(r)
	}
	for len(dst) < limit {
		if !c.held {
			c.pending, c.held = c.nextImage(r)
		}
		switch {
		case c.di < len(c.delta) && (!c.held || c.delta[c.di] <= c.pending):
			id := c.delta[c.di]
			c.di++
			if c.held && c.pending == id {
				c.held = false
			}
			dst = append(dst, id)
		case c.held:
			dst = append(dst, c.pending)
			c.held = false
		default:
			return dst, true
		}
	}
	return dst, false
}

// edgeScanCursor is nodeScanCursor for edges.
type edgeScanCursor struct {
	delta   []store.EdgeID
	di      int
	ii      int
	n       int
	pending store.EdgeID
	held    bool
	started bool
}

func (c *edgeScanCursor) start(r reader) {
	c.delta = r.deltaLiveEdgeIDs()
	slices.Sort(c.delta)
	c.ii, c.n = 1, r.imageEdgeLen()
	c.started = true
}

func (c *edgeScanCursor) nextImage(r reader) (store.EdgeID, bool) {
	for c.ii < c.n {
		id := r.v.csr.edges[c.ii].ID
		c.ii++
		if id == store.InvalidEdgeID || r.deltaEdgeKnown(id) {
			continue
		}
		return id, true
	}
	return store.InvalidEdgeID, false
}

func (c *edgeScanCursor) fill(r reader, dst []store.EdgeID, limit int) ([]store.EdgeID, bool) {
	if !c.started {
		c.start(r)
	}
	for len(dst) < limit {
		if !c.held {
			c.pending, c.held = c.nextImage(r)
		}
		switch {
		case c.di < len(c.delta) && (!c.held || c.delta[c.di] <= c.pending):
			id := c.delta[c.di]
			c.di++
			if c.held && c.pending == id {
				c.held = false
			}
			dst = append(dst, id)
		case c.held:
			dst = append(dst, c.pending)
			c.held = false
		default:
			return dst, true
		}
	}
	return dst, false
}

// --- one label ---

// labelScanCursor is the streaming form of reader.nodesByType: the same merge
// of the same two ascending postings, resolved against the records the same
// way, stopped and resumed at a batch boundary.
//
// The delta posting is copied once because a writer inserts into it in place;
// the image posting is built at construction and never written again, so it is
// held by reference.
type labelScanCursor struct {
	t       store.NodeType
	delta   []store.NodeID
	di      int
	csr     []store.NodeID
	ci      int
	started bool
}

func (c *labelScanCursor) start(r reader) {
	c.delta = slices.Clone(r.v.delta.nodesByType[c.t])
	if r.v.csr != nil {
		c.csr = r.v.csr.NodesByType(c.t)
	}
	c.started = true
}

func (c *labelScanCursor) fill(r reader, dst []store.NodeID, limit int) ([]store.NodeID, bool) {
	if !c.started {
		c.start(r)
	}
	for len(dst) < limit {
		if c.di >= len(c.delta) && c.ci >= len(c.csr) {
			return dst, true
		}
		var id store.NodeID
		if c.ci >= len(c.csr) || (c.di < len(c.delta) && c.delta[c.di] <= c.csr[c.ci]) {
			id = c.delta[c.di]
			if c.ci < len(c.csr) && c.csr[c.ci] == id {
				c.ci++
			}
			c.di++
		} else {
			id = c.csr[c.ci]
			c.ci++
		}
		// A posting can name a node this reader cannot see, one whose label was
		// since removed, or one that was deleted. The records are the authority.
		if r.nodeHasLabel(id, c.t) {
			dst = append(dst, id)
		}
	}
	return dst, false
}

// --- store.Scanner ---

func (sn *snapshot) ScanNodes() iter.Seq2[store.NodeID, error] {
	var c nodeScanCursor
	return scanChunked(sn, c.fill)
}

func (sn *snapshot) ScanEdges() iter.Seq2[store.EdgeID, error] {
	var c edgeScanCursor
	return scanChunked(sn, c.fill)
}

func (sn *snapshot) ScanNodesByType(t store.NodeType) iter.Seq2[store.NodeID, error] {
	c := labelScanCursor{t: t}
	return scanChunked(sn, c.fill)
}

// --- store.PropertyEnumerator ---

// ForEachNodeProperty implements store.PropertyEnumerator for a snapshot.
//
// The property index is a live structure shared with the store — store.Snapshot
// documents property reads as "the postings now, resolved against the records
// then" — so this walks the current entries and drops the ones naming a node
// this snapshot cannot see. That is what makes a snapshot usable as a
// bulk.Source: every entry it emits belongs to a node it also exports, which is
// exactly what the importer's ordering rule requires.
//
// The liveness test is taken through NodeExists, one lock acquisition at a
// time, rather than under a lock held for the whole walk. fn is caller code and
// a scan that held the store lock across it would stall every writer in the
// process for as long as the caller took — the same reason a scan yields
// batches instead of yielding under the lock.
func (sn *snapshot) ForEachNodeProperty(fn func(id store.NodeID, key string, value []byte) bool) {
	if _, _, err := sn.use(); err != nil {
		return
	}
	sn.s.propIdx.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
		if !sn.NodeExists(id) {
			return true
		}
		return fn(id, key, value)
	})
}

// ForEachEdgeProperty is ForEachNodeProperty for edge properties.
func (sn *snapshot) ForEachEdgeProperty(fn func(id store.EdgeID, key string, value []byte) bool) {
	if _, _, err := sn.use(); err != nil {
		return
	}
	sn.s.propIdx.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
		if !sn.edgeExists(id) {
			return true
		}
		return fn(id, key, value)
	})
}

// edgeExists is NodeExists for edges. There is no exported form because
// store.GraphReader has none; this exists for the property walk above.
func (sn *snapshot) edgeExists(id store.EdgeID) bool {
	r, locked, err := sn.use()
	if err != nil {
		return false
	}
	if locked {
		sn.s.mu.RLock()
		defer sn.s.mu.RUnlock()
	}
	return r.edgeExists(id)
}
