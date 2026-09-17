package disk

// The sort a bulk load needs and a compaction does not.
//
// # Why there is a new sorter at all
//
// GPIX is written from a walk: for each key in ascending order, each of its
// distinct values in ascending order, with the ascending ids filed under it. A
// compaction gets that walk for free, because the thing it is writing out is the
// live property index and a posting list *is* the group, already sorted. A bulk
// load has no index — building one is precisely the memory it exists not to
// spend — so the same order has to come from somewhere, and the only thing it
// can come from is a sort of the triples as they go past.
//
// gpirSorter next door already does an external sort, and this is deliberately
// its shape: fill a chunk, sort it, write it out as a run, merge the runs at the
// end. What it is not is a second copy of it, because the two sort different
// things. A GPIR entry is twenty-four fixed bytes and a triple is a key, an
// arbitrary byte string and an id, so every decision that follows from
// fixed-width — the stride, the count-addressed runs, the buffer arithmetic —
// is a decision this one cannot take.
//
// # What is stored per entry, and what is not
//
// The key is interned. A store declares a handful of property keys and indexes
// them ten million times, so writing the key's bytes into every entry would put
// tens of bytes of repetition into a spill file already sized by the data. A
// sixteen-bit slot instead, resolved through the sorter's own table when two
// entries are compared.
//
// The value is not interned, and that is the asymmetry: a value is the thing
// being indexed and is different nearly every time. It is copied into a chunk
// arena rather than allocated per entry, so a chunk is two allocations and not
// two million, and the arena is released with the chunk.
//
// # The order is the caller's, because the two callers walk differently
//
// GPIX walks its keys by name, ascending, and refuses a list that is not
// (writeGPIXKind says why: an unsorted list writes a file this build could not
// reopen). GCPX walks its composites in whatever order the source hands them
// over, and lays its runs region out in that order.
//
// Those two happen to coincide today, because index.PropertyIndex sorts its
// composite list by name before returning it — so a sort keyed by name would
// currently produce the same bytes. It is keyed by slot anyway, and the
// difference is not fastidiousness: the slot *is* the walk position, so this
// follows the encoder whatever the source does, while name order follows it only
// for as long as that other package goes on sorting. A test cannot tell the two
// apart at the moment, and that is stated rather than left to look like coverage.

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strings"
)

// bulkEntryHeader is the fixed prefix of a spilled entry: id, value length, key
// slot, kind, and one byte of padding so the value starts eight-byte aligned in
// the reader's own buffer. The value follows.
const bulkEntryHeader = 16

// bulkKindNode and bulkKindEdge are the sort's leading term.
//
// They are gpixKindNode and gpixKindEdge, and gcpxKindNode and gcpxKindEdge,
// which are the same two values for the same reason: both sections write every
// node key before every edge key, so a single forward cursor over this sort
// serves both halves of the walk. Written out here rather than aliased because a
// sort order that silently depended on another section's constant would be a
// coupling nobody reading either file could see.
const (
	bulkKindNode uint8 = 0
	bulkKindEdge uint8 = 1
)

// bulkEntry is one triple while its chunk is in memory.
//
// The value lives in the chunk's arena at off, for vlen bytes. Twenty-four bytes
// per entry, the same as a GPIR entry, which is not a coincidence: the chunk is
// sized from the same figure and the two are meant to be comparable when a
// caller is deciding what MaxWorkingBytes should be.
type bulkEntry struct {
	id        uint64
	off, vlen uint32
	keyID     uint16
	kind      uint8
}

// bulkSorter accumulates (kind, key, value, id) and yields them in that order.
//
// Fill it with add, call seal once the last record has gone past, then take one
// cursor from it. A failure to spill is held by the spill and reported when the
// bytes are needed, which is spillBuffer's bargain and is stated there.
type bulkSorter struct {
	spill *spillBuffer

	// keyLess orders two key slots; see the file comment for why it is the
	// caller's decision.
	keyLess func(a, b uint16) int

	// keys is slot to key, in the order the slots were handed out. slotOf is its
	// inverse. Both are small: a store declares keys, it does not generate them.
	keys   []string
	slotOf map[string]uint16

	// The chunk: entries and the arena their values were copied into. Both are
	// bounded, and both are released when the chunk is written out.
	buf   []bulkEntry
	arena []byte

	maxEntries int
	maxArena   int

	// runs are the sorted chunks in the spill, as byte extents: a run's entries
	// are variable-length, so unlike GPIR's this cannot be a count.
	runs []bulkRun

	// scratch is the encode buffer a chunk is written through.
	scratch []byte

	// buffers sizes the merge, and is the same figure that sized the chunk.
	buffers compactBuffers

	// present is every (kind, key slot) pair that has been added. It is the key
	// list GPIX is given, and it is recorded here because the alternative is a
	// pass over the whole sort to learn which of a handful of keys are in it.
	// Bounded by the number of declared keys, not by the data.
	present map[bulkKeySlot]bool

	n       uint64
	sealed  bool
	opened  bool
	drained bool
}

// bulkRun is one sorted chunk inside the spill.
type bulkRun struct {
	off, length uint64
}

// newBulkSorter returns a sorter spilling into dir, sized by b, ordering key
// slots with less.
//
// The chunk takes the GPIR sort chunk's size in both dimensions — its entry
// count, and the same number of GPIR entries' worth of value bytes. That is a
// choice and not an equivalence: it means MaxWorkingBytes governs a bulk load's
// sort exactly as it governs a compaction's, so a caller who has already decided
// what a compaction may hold has decided this too rather than meeting a second
// figure. compact_buffers.go carries what that figure was worth when it was
// measured.
func newBulkSorter(dir string, b compactBuffers, less func(a, b uint16) int) *bulkSorter {
	b = b.resolved()
	chunkBytes := b.revChunk * gpirEntrySize
	return &bulkSorter{
		spill:      newSpill(dir, chunkBytes),
		keyLess:    less,
		slotOf:     make(map[string]uint16),
		present:    make(map[bulkKeySlot]bool),
		buffers:    b,
		maxEntries: b.revChunk,
		maxArena:   chunkBytes,
	}
}

// slot returns key's slot, assigning one if this is the first time it has been
// seen.
//
// uint16 is the width GPIX's own key directory uses for a key id, so a store
// with more keys than this could not be written even if the sort held them.
// Refusing here rather than overflowing turns that into an error naming the key
// that crossed the line.
func (s *bulkSorter) slot(key string) (uint16, error) {
	if id, ok := s.slotOf[key]; ok {
		return id, nil
	}
	if len(s.keys) >= 1<<16 {
		return 0, fmt.Errorf("bulk: more than %d distinct property keys; %q is one too many", 1<<16, key)
	}
	id := uint16(len(s.keys))
	s.keys = append(s.keys, key)
	s.slotOf[key] = id
	return id, nil
}

// declare pins key to the next slot, so a caller whose order is by slot gets the
// order it declared.
//
// A caller ordering by slot calls this for every key up front, in the order it
// will walk them, and the sort then produces that order. Calling it twice for one
// key is the caller's own bug and is reported rather than quietly handing back the
// first slot, because a composite declared twice would otherwise file its tuples
// under a slot its walker never asks for.
func (s *bulkSorter) declare(key string) (uint16, error) {
	if _, ok := s.slotOf[key]; ok {
		return 0, fmt.Errorf("bulk: %q is declared twice", key)
	}
	return s.slot(key)
}

// bulkKeyOrder orders slots by the key's own bytes. GPIX walks this way.
func bulkKeyOrder(s *bulkSorter) func(a, b uint16) int {
	return func(a, b uint16) int {
		if a == b {
			return 0
		}
		return strings.Compare(s.keys[a], s.keys[b])
	}
}

// bulkSlotOrder orders slots by the slot itself. GCPX walks this way.
func bulkSlotOrder(a, b uint16) int { return cmp.Compare(a, b) }

// add records one triple. value is copied and is the caller's again on return.
func (s *bulkSorter) add(kind uint8, keyID uint16, value []byte, id uint64) {
	if s.sealed {
		// Not an error return, because there is no caller that can be in this
		// state legitimately and none that could do anything about it. The
		// cursor refuses on the same footing.
		panic("bulk: sorter written to after it was sealed")
	}
	if len(s.buf) >= s.maxEntries || len(s.arena)+len(value) > s.maxArena {
		s.spillRun()
	}
	off := uint32(len(s.arena))
	s.arena = append(s.arena, value...)
	s.buf = append(s.buf, bulkEntry{
		id: id, off: off, vlen: uint32(len(value)), keyID: keyID, kind: kind,
	})
	s.present[bulkKeySlot{kind: kind, keyID: keyID}] = true
	s.n++
}

// count is how many triples have been added.
func (s *bulkSorter) count() uint64 { return s.n }

// less is the sort order, over two entries of one chunk.
func (s *bulkSorter) less(a, b bulkEntry) int {
	if a.kind != b.kind {
		return cmp.Compare(a.kind, b.kind)
	}
	if c := s.keyLess(a.keyID, b.keyID); c != 0 {
		return c
	}
	if c := bytes.Compare(s.arena[a.off:a.off+a.vlen], s.arena[b.off:b.off+b.vlen]); c != 0 {
		return c
	}
	return cmp.Compare(a.id, b.id)
}

// spillRun sorts what is buffered and appends it to the spill as one run.
func (s *bulkSorter) spillRun() {
	if len(s.buf) == 0 {
		return
	}
	slices.SortFunc(s.buf, s.less)
	if s.scratch == nil {
		s.scratch = make([]byte, 0, 64<<10)
	}
	off := s.spill.len()
	dst := s.scratch[:0]
	var hdr [bulkEntryHeader]byte
	for _, e := range s.buf {
		if len(dst)+bulkEntryHeader+int(e.vlen) > cap(dst) {
			s.spill.write(dst)
			dst = dst[:0]
		}
		binary.LittleEndian.PutUint64(hdr[0:8], e.id)
		binary.LittleEndian.PutUint32(hdr[8:12], e.vlen)
		binary.LittleEndian.PutUint16(hdr[12:14], e.keyID)
		hdr[14], hdr[15] = e.kind, 0
		if bulkEntryHeader+int(e.vlen) > cap(dst) {
			// A value wider than the encode buffer goes straight through: the
			// spill takes a large write without staging it, and staging it here
			// would mean growing a buffer to the widest value in the store.
			s.spill.write(hdr[:])
			s.spill.write(s.arena[e.off : e.off+e.vlen])
			continue
		}
		dst = append(dst, hdr[:]...)
		dst = append(dst, s.arena[e.off:e.off+e.vlen]...)
	}
	s.spill.write(dst)
	s.runs = append(s.runs, bulkRun{off: off, length: s.spill.len() - off})
	s.buf, s.arena = s.buf[:0], s.arena[:0]
	s.opened = true
}

// seal closes the sorter to writes. It must be called before a cursor is taken.
func (s *bulkSorter) seal() { s.sealed = true }

// close releases the sorter's spill.
func (s *bulkSorter) close() { s.spill.close() }

// bulkKeySlot is one kind's use of one key slot.
type bulkKeySlot struct {
	keyID uint16
	kind  uint8
}

// keyNames returns the distinct keys of one kind, in this sorter's key order.
//
// This is what GPIX's key list is built from, and it is available only after the
// records have gone past -- which is the one temporal coupling a streaming build
// has, and it is the same one the header counts have. writeImage writes every
// record before it writes any section, so the list is complete by the time the
// encoder asks for it, and the cursor refuses outright if the sort has not been
// sealed.
func (s *bulkSorter) keyNames(kind uint8) []string {
	slots := make([]uint16, 0, len(s.keys))
	for p := range s.present {
		if p.kind == kind {
			slots = append(slots, p.keyID)
		}
	}
	slices.SortFunc(slots, s.keyLess)
	out := make([]string, len(slots))
	for i, sl := range slots {
		out[i] = s.keys[sl]
	}
	return out
}

// bulkCursor is a forward reader over the sorted triples.
//
// One cursor drains one sorter. It is a cursor and not the callback gpirSorter
// emits through because both consumers pull: GPIX asks for one key's values at a
// time and GCPX for one composite's tuples, and a callback would have to be
// inverted into exactly this.
type bulkCursor struct {
	s    *bulkSorter
	heap []*bulkRunReader

	// mem is the final chunk when nothing was ever spilled, sorted in place.
	mem []bulkEntry
	at  int

	// val is where a merged entry's value is copied to.
	//
	// It has to be copied. A run reader parses into one buffer and reuses it, so
	// the head the cursor has just taken is the buffer the very next advance of
	// that reader overwrites -- and the cursor advances the reader it just read
	// from, every time. Handing the reader's own slice out paired the value of
	// one entry with the identifier of another, which is a wrong answer rather
	// than a crash and is why this is a copy and not a slice.
	val []byte

	head  bulkCursorEntry
	valid bool
	err   error
}

// bulkCursorEntry is one triple as the cursor presents it. value points into a
// buffer the cursor owns and is valid until the next advance.
type bulkCursorEntry struct {
	kind  uint8
	keyID uint16
	id    uint64
	value []byte
}

// cursor drains the sorter once.
func (s *bulkSorter) cursor() (*bulkCursor, error) {
	if !s.sealed {
		return nil, fmt.Errorf("bulk: the index sort was read before the records finished streaming")
	}
	// Once, because a cursor consumes the sort: the spill's run readers are
	// taken and the in-memory chunk is handed over. A second one would restart at
	// the beginning of *everything*, which for a caller walking one kind at a
	// time reads as "this kind has no entries" and writes an index silently
	// missing half of itself. That is exactly what it did before this check.
	if s.drained {
		return nil, fmt.Errorf("bulk: the index sort was read twice")
	}
	s.drained = true
	c := &bulkCursor{s: s}

	// Nothing was ever spilled: the whole sort is one chunk, sorted where it is,
	// and no file was opened. The arena is still alive under it, so the values
	// are read straight out of it.
	if !s.opened {
		slices.SortFunc(s.buf, s.less)
		c.mem = s.buf
		c.advance()
		return c, nil
	}

	s.spillRun()
	// The chunk and the encode buffer go before the merge allocates its readers,
	// so the two are not held at once. gpirSorter does the same and says so.
	s.buf, s.arena, s.scratch = nil, nil, nil
	if s.spill.err != nil {
		return nil, s.spill.err
	}

	bufSize := min(s.buffers.mergeBudget/len(s.runs), s.buffers.maxRunBuffer)
	if bufSize < bulkMinRunBuffer {
		bufSize = bulkMinRunBuffer
	}
	for _, run := range s.runs {
		r, err := s.spill.section(run.off, run.length)
		if err != nil {
			return nil, err
		}
		rr := &bulkRunReader{s: s, r: bufio.NewReaderSize(r, bufSize), left: run.length}
		if !rr.advance() {
			if rr.err != nil {
				return nil, rr.err
			}
			continue
		}
		c.heap = append(c.heap, rr)
	}
	for i := len(c.heap)/2 - 1; i >= 0; i-- {
		c.siftDown(i)
	}
	c.advance()
	return c, nil
}

// bulkMinRunBuffer is the floor on one run reader, so a sort that produced very
// many runs reads a whole entry at a time rather than a byte at a time. The
// header plus a short value; anything wider is read through it.
const bulkMinRunBuffer = 4 * bulkEntryHeader

// peek returns the entry the cursor is on.
func (c *bulkCursor) peek() (bulkCursorEntry, bool) { return c.head, c.valid }

// advance moves to the next entry.
func (c *bulkCursor) advance() {
	if c.err != nil {
		c.valid = false
		return
	}
	if c.mem != nil {
		if c.at >= len(c.mem) {
			c.valid = false
			return
		}
		e := c.mem[c.at]
		c.at++
		c.head = bulkCursorEntry{
			kind: e.kind, keyID: e.keyID, id: e.id,
			value: c.s.arena[e.off : e.off+e.vlen],
		}
		c.valid = true
		return
	}
	if len(c.heap) == 0 {
		c.valid = false
		return
	}
	r := c.heap[0]
	c.head, c.valid = r.head, true
	c.val = append(c.val[:0], r.head.value...)
	c.head.value = c.val
	if r.advance() {
		c.siftDown(0)
		return
	}
	if r.err != nil {
		c.err = r.err
	}
	last := len(c.heap) - 1
	c.heap[0] = c.heap[last]
	c.heap = c.heap[:last]
	if len(c.heap) > 0 {
		c.siftDown(0)
	}
}

// cursorLess is the merge's comparison, over two run heads.
func (c *bulkCursor) cursorLess(a, b bulkCursorEntry) int {
	if a.kind != b.kind {
		return cmp.Compare(a.kind, b.kind)
	}
	if d := c.s.keyLess(a.keyID, b.keyID); d != 0 {
		return d
	}
	if d := bytes.Compare(a.value, b.value); d != 0 {
		return d
	}
	return cmp.Compare(a.id, b.id)
}

func (c *bulkCursor) siftDown(i int) {
	for {
		l, r, small := 2*i+1, 2*i+2, i
		if l < len(c.heap) && c.cursorLess(c.heap[l].head, c.heap[small].head) < 0 {
			small = l
		}
		if r < len(c.heap) && c.cursorLess(c.heap[r].head, c.heap[small].head) < 0 {
			small = r
		}
		if small == i {
			return
		}
		c.heap[i], c.heap[small] = c.heap[small], c.heap[i]
		i = small
	}
}

// bulkRunReader reads one sorted run, one buffered entry at a time.
type bulkRunReader struct {
	s    *bulkSorter
	r    *bufio.Reader
	left uint64

	hdr  [bulkEntryHeader]byte
	val  []byte
	head bulkCursorEntry
	err  error
}

// advance reads the next entry, reporting whether there was one.
func (r *bulkRunReader) advance() bool {
	if r.err != nil || r.left == 0 {
		return false
	}
	if _, err := io.ReadFull(r.r, r.hdr[:]); err != nil {
		r.err = fmt.Errorf("bulk: read sorted run: %w", err)
		return false
	}
	vlen := binary.LittleEndian.Uint32(r.hdr[8:12])
	if uint64(bulkEntryHeader)+uint64(vlen) > r.left {
		r.err = fmt.Errorf("bulk: sorted run is short: an entry claims %d bytes and %d remain",
			uint64(bulkEntryHeader)+uint64(vlen), r.left)
		return false
	}
	if cap(r.val) < int(vlen) {
		r.val = make([]byte, vlen)
	}
	r.val = r.val[:vlen]
	if _, err := io.ReadFull(r.r, r.val); err != nil {
		r.err = fmt.Errorf("bulk: read sorted run value: %w", err)
		return false
	}
	r.left -= uint64(bulkEntryHeader) + uint64(vlen)
	r.head = bulkCursorEntry{
		id:    binary.LittleEndian.Uint64(r.hdr[0:8]),
		keyID: binary.LittleEndian.Uint16(r.hdr[12:14]),
		kind:  r.hdr[14],
		value: r.val,
	}
	return true
}
