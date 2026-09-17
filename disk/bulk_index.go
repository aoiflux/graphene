package disk

// The index sections of a bulk-loaded image, read out of a sort instead of an
// index.
//
// GPIX, GPIR, GCPX and GIDX are all written from one shape: for each key in the
// order that section walks its keys, each distinct value ascending, with the
// ascending identifiers filed under it. A compaction has that shape already —
// the live property index is grouped and sorted, and a posting list *is* the
// group. A bulk load builds no index, so the shape has to be produced, and what
// produces it is bulk_sort.go.
//
// What is left is the adapter: a cursor over the sorted triples, presented as
// the walkers the four encoders expect. It is one forward pass in every case,
// because the sort's order and the encoders' order are the same order — which is
// the reason the sort is keyed the way it is rather than a happy coincidence.

import (
	"bytes"
	"fmt"
	"iter"

	"github.com/aoiflux/graphene/index"
	"github.com/aoiflux/graphene/store"
)

// sealIndex closes both sorts to writes and publishes what the records taught
// them.
//
// Called when the edge pass returns, which is the moment no further index entry
// can arrive and is still before the encoder has written a single section. The
// key lists are the one thing a streaming build learns during the record pass
// and needs afterwards; see the file comment in bulk_load.go for why that is the
// same shape as the header counts rather than a new kind of coupling.
func (b *bulkBuilder) sealIndex() {
	b.props.seal()
	b.comps.seal()
	if src := b.p.payload.MappedIndex; src != nil {
		src.NodeKeys = b.props.keyNames(bulkKindNode)
		src.EdgeKeys = b.props.keyNames(bulkKindEdge)
	}
}

// bulkWalk is a cursor over one sort, opened on first use.
//
// Opened lazily because taking a cursor drains the sorter and can fail, and
// neither is something to do while the caller is still streaming records. Its
// first use is inside the encoder, after the records; whatever it reports there
// fails the image write, which is where a failure belongs.
type bulkWalk struct {
	s   *bulkSorter
	c   *bulkCursor
	err error

	// value and ids are the group being assembled, reused across groups. ids
	// grows to the widest value's holder list and value to the widest value,
	// which is the one thing here that is not flat in the entries -- the same
	// term, for the same reason, that mappedIndexSource's own buffers are.
	value []byte
	ids   []uint64
}

func (w *bulkWalk) open() error {
	if w.err != nil {
		return w.err
	}
	if w.c == nil {
		c, err := w.s.cursor()
		if err != nil {
			w.err = err
			return err
		}
		w.c = c
	}
	return w.c.err
}

// group collects the entries of one (kind, key slot) group whose value matches
// the cursor's current one, advancing past them.
//
// Identical triples are collapsed. A caller may index the same key and value for
// one record twice -- through two passes over its source, or because its source
// says so -- and the index would have collapsed them, so the image must too or
// the two paths write different files.
func (w *bulkWalk) group(kind uint8, keyID uint16) ([]byte, []uint64, bool) {
	head, ok := w.c.peek()
	if !ok || head.kind != kind || head.keyID != keyID {
		return nil, nil, false
	}
	w.value = append(w.value[:0], head.value...)
	w.ids = w.ids[:0]
	for {
		e, ok := w.c.peek()
		if !ok || e.kind != kind || e.keyID != keyID || !bytes.Equal(e.value, w.value) {
			break
		}
		if len(w.ids) == 0 || w.ids[len(w.ids)-1] != e.id {
			w.ids = append(w.ids, e.id)
		}
		w.c.advance()
	}
	return w.value, w.ids, true
}

// slotFor resolves the key the encoder asked for, refusing one the cursor has
// already gone past.
//
// The encoders walk their keys in the sort's own order, so this can only differ
// if the key list and the sort disagree -- which would be the one failure this
// design could have and would otherwise show up as a silently short section.
func (w *bulkWalk) slotFor(kind uint8, key string) (uint16, error) {
	slot, ok := w.s.slotOf[key]
	if !ok {
		return 0, fmt.Errorf("bulk: the encoder asked for key %q, which no record indexed", key)
	}
	if head, live := w.c.peek(); live && head.kind == kind {
		if w.s.keyLess(head.keyID, slot) < 0 {
			return 0, fmt.Errorf("bulk: the encoder asked for key %q after the sort had passed it", key)
		}
	}
	return slot, nil
}

// gpixSource is the sort in the shape GPIX is written from.
//
// NodeKeys and EdgeKeys are filled by sealIndex, not here: they are not known
// until the records have gone past, and the encoder reads them after that. The
// sort refuses to produce a cursor before it has been sealed, so an encoder that
// ever asked earlier would fail the write rather than write a short index.
func (b *bulkBuilder) gpixSource() *gpixSource {
	w := &bulkWalk{s: b.props}
	return &gpixSource{
		NodeValues: b.valueWalker(w, bulkKindNode),
		EdgeValues: b.valueWalker(w, bulkKindEdge),
		// The store's own directory, for the reason mappedIndexSource gives: an
		// index too large to sort in memory spills onto the filesystem whose free
		// space was sized for the image it is being written beside.
		ScratchDir: b.dir,
		buffers:    b.p.buffers,
	}
}

// valueWalker is one kind's half of the GPIX walk.
//
// Both halves share one cursor, because the sort puts every node entry before
// every edge entry and GPIX writes every node key before every edge key. Two
// bulkWalks over one sorter would be two cursors over a sort that can only be
// drained once; this hands the same one to both and the kind check is what keeps
// them apart.
func (b *bulkBuilder) valueWalker(w *bulkWalk, kind uint8) gpixValueWalker {
	return func(key string, fn func(value []byte, ids []uint64) bool) error {
		if err := w.open(); err != nil {
			return err
		}
		slot, err := w.slotFor(kind, key)
		if err != nil {
			return err
		}
		for {
			value, ids, ok := w.group(kind, slot)
			if !ok {
				break
			}
			if err := b.checkUnique(kind, key, ids); err != nil {
				return err
			}
			if !fn(value, ids) {
				break
			}
		}
		return w.c.err
	}
}

// gcpxSource is the composite sort in the shape GCPX is written from.
func (b *bulkBuilder) gcpxSource() *gcpxSource {
	w := &bulkWalk{s: b.comps}
	return &gcpxSource{
		NodeComposites: b.nodeComposites,
		EdgeComposites: b.edgeComposites,
		Tuples: func(kind uint8, keys []string, fn func(tuple []byte, ids []uint64) bool) error {
			if err := w.open(); err != nil {
				return err
			}
			slot, ok := b.compSlot[bulkCompositeName{name: index.CompositeName(keys), kind: kind}]
			if !ok {
				return fmt.Errorf("bulk: the encoder asked for a composite over %v that was never declared", keys)
			}
			// The same check the property walk makes, and it is worth as much
			// here: a cursor already past this composite's slot yields nothing,
			// and nothing is indistinguishable from "this composite matched no
			// record" -- which is a legitimate answer GCPX is required to be able
			// to record. So the disagreement has to be caught rather than read.
			if head, live := w.c.peek(); live && head.kind == kind && w.s.keyLess(head.keyID, slot) < 0 {
				return fmt.Errorf("bulk: the encoder asked for the composite over %v after the sort had passed it", keys)
			}
			for {
				tuple, ids, ok := w.group(kind, slot)
				if !ok {
					break
				}
				if !fn(tuple, ids) {
					break
				}
			}
			return w.c.err
		},
		ScratchDir: b.dir,
		buffers:    b.p.buffers,
	}
}

// propSeq is the sort flattened back to one triple per entry, which is what GIDX
// is written from.
//
// The two sequences share one cursor for the reason the two GPIX walkers do:
// writeImage writes every node entry before every edge entry, and the sort is in
// that order already. The order within a kind is (key, value, id), which is
// ForEachNodeProperty's documented contract and therefore what an image built the
// other way carries.
func (b *bulkBuilder) propSeq(w *bulkWalk) iter.Seq[index.NodePropEntry] {
	return func(yield func(index.NodePropEntry) bool) {
		b.flatten(w, bulkKindNode, func(key string, value []byte, id uint64) bool {
			return yield(index.NodePropEntry{ID: store.NodeID(id), Key: key, Value: value})
		})
	}
}

// edgePropSeq is propSeq for edge properties.
func (b *bulkBuilder) edgePropSeq(w *bulkWalk) iter.Seq[index.EdgePropEntry] {
	return func(yield func(index.EdgePropEntry) bool) {
		b.flatten(w, bulkKindEdge, func(key string, value []byte, id uint64) bool {
			return yield(index.EdgePropEntry{ID: store.EdgeID(id), Key: key, Value: value})
		})
	}
}

// flatten walks one kind's entries in key order, one triple at a time.
//
// A failure here cannot be returned -- an iter.Seq has no way to report one --
// so it is latched on the builder, exactly as the passes' own failures are, and
// build reports it while the image is still a temp file.
func (b *bulkBuilder) flatten(w *bulkWalk, kind uint8, yield func(key string, value []byte, id uint64) bool) {
	if err := w.open(); err != nil {
		if b.err == nil {
			b.err = err
		}
		return
	}
	for _, key := range w.s.keyNames(kind) {
		slot, err := w.slotFor(kind, key)
		if err != nil {
			if b.err == nil {
				b.err = err
			}
			return
		}
		for {
			value, ids, ok := w.group(kind, slot)
			if !ok {
				break
			}
			if err := b.checkUnique(kind, key, ids); err != nil {
				if b.err == nil {
					b.err = err
				}
				return
			}
			for _, id := range ids {
				if !yield(key, value, id) {
					return
				}
			}
		}
	}
	if w.c.err != nil && b.err == nil {
		b.err = w.c.err
	}
}
