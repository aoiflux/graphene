package index

import (
	"encoding/binary"

	"github.com/aoiflux/graphene/store"
)

// This file declares what a *disk-resident* property index looks like from the
// index package's side: the Base interface, the entity kind it is addressed by,
// and the view type its postings come back as.
//
// # Why an interface, and why here
//
// The plan for R3 named one file, index/mapped.go, holding the reader over the
// image's GPIX and GPIR sections. That cannot be written: disk imports index, so
// index cannot name a disk type, and the sections are read where they are
// written — disk/csr_gpix.go — because a format whose encoder and decoder live
// in different packages drifts. So the reader is in disk and what is here is the
// contract it satisfies.
//
// The boundary is drawn where the calls are coarse. Every method below is
// entered once per *query* (or once per distinct value of a walk), never once
// per id, so an interface dispatch is amortised over a whole postings list
// rather than paid per element. That is the reason IDRun is a concrete struct
// and not an interface: it is the one type on this boundary that is touched per
// id, and a virtual call there would cost more than reading the id.
//
// # What a Base promises
//
// A Base is immutable and safe for concurrent readers. It is the state of the
// index as of the compaction that wrote the image, and it never changes again:
// everything since is the in-memory delta, and everything removed since is the
// retracted set. That immutability is not a convenience, it is the whole design
// — it is why nothing can be resurrected out of it (docs/TECHNICAL_DETAILS.md
// §14.7's hazard) and why it needs no lock.
//
// Values and ids handed to callbacks belong to the base, which under the mapped
// image means they are file-backed memory. Read them, do not retain them. That
// is the same contract ForEachNodeEntry already states for the resident index's
// interned strings, which is what lets one caller serve both.

// EntityKind names which of the two entity kinds an operation addresses.
//
// The values match the on-disk kind byte, so the disk-side implementation is a
// conversion rather than a lookup table; disk asserts that agreement in a test
// rather than leaving it to two constants that merely happen to line up today.
type EntityKind uint8

const (
	// NodeKind addresses the node half of a base.
	NodeKind EntityKind = 0
	// EdgeKind addresses the edge half of a base.
	EdgeKind EntityKind = 1
)

func (k EntityKind) String() string {
	if k == EdgeKind {
		return "edge"
	}
	return "node"
}

// IDRun is a read-only view of one value's postings list, ascending.
//
// It is a view and not a slice of IDs because the bytes it addresses are the
// image: turning a run into []store.NodeID is exactly the allocation this whole
// item exists to stop paying. A caller that needs to keep the ids copies them
// out — and every *public* caller does, at the boundary where PropertyIndex
// returns a result, so no mapped memory ever reaches a caller of the store. The
// view is for the code in between, which merges a base run against the delta and
// keeps neither.
//
// # The encoding
//
// raw is a packed array of little-endian uint64s and nothing else — no header,
// no padding, length implied by len(raw). That is this interface's contract, not
// GPIX's: any implementation presents its postings this way, and GPIX stores
// them this way because it is the shape that needs no conversion. Eight-byte
// alignment is *not* assumed, because a run begins wherever its value's length
// leaves it; binary.LittleEndian handles that, and the load is the cheap half of
// touching a page that may not be resident.
//
// The zero IDRun is an empty run and every method below is valid on it.
type IDRun struct {
	raw []byte
}

// NewIDRun wraps packed little-endian uint64s as a run.
//
// A length that is not a multiple of eight yields the ids that are whole and
// ignores the remainder, because Len floors — the trailing bytes are never
// addressed by any method here. It is not refused: the caller has already
// bounded the region this came out of, and a ragged length is a corruption for
// the verifier to name, not something a lookup can act on.
func NewIDRun(raw []byte) IDRun {
	return IDRun{raw: raw}
}

// Len reports how many whole ids the run holds.
func (r IDRun) Len() int { return len(r.raw) / 8 }

// At returns the i'th id. It panics for i outside [0, Len).
func (r IDRun) At(i int) uint64 {
	return binary.LittleEndian.Uint64(r.raw[i*8:])
}

// Contains reports whether id is in the run, by binary search.
//
// The run is ascending — that is the base's promise, and a base that broke it
// would make this answer "absent" for a present id. Verification checks the
// ordering over the whole section; this path does not re-check it per probe,
// for the same reason the resident index's containsSorted does not.
func (r IDRun) Contains(id uint64) bool {
	lo, hi := 0, r.Len()
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if r.At(mid) < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < r.Len() && r.At(lo) == id
}

// First returns the lowest id in the run, and whether there is one.
func (r IDRun) First() (uint64, bool) {
	if r.Len() == 0 {
		return 0, false
	}
	return r.At(0), true
}

// ValueCursor is a position in one key's values that can only move forward.
//
// It exists because resolving many exact values costs far less when they are
// resolved in order. A base's values are sorted, so for an ascending sequence of
// wants each answer is at or after the last one, and a search that starts where
// the previous one finished never re-examines the part of the key it has already
// passed. Under a mapped image that is the difference between a random probe per
// query and one forward sweep for the whole batch — the reason
// NodesByPropertyBatch exists, stated as the one operation it needs.
//
// A cursor is not safe for concurrent use and is not meant to outlive the call
// that made it. The base it reads is immutable, so nothing here can go stale;
// the cursor's own position is what is unshareable.
type ValueCursor interface {
	// Seek positions at the first value not less than want, and returns that
	// value's ids together with whether it equals want. An absent want yields an
	// empty run and false, and leaves the cursor at the first value above it, so
	// the next Seek continues from there.
	//
	// A want below the previous one is refused with an error rather than
	// answered. This is the one place in the read paths where a caller's mistake
	// is reported instead of absorbed, and it is because of what the alternative
	// would be: a forward-only position asked to look backward can only say
	// "absent" for a value that is present, which is a wrong answer produced
	// silently by an index. An *equal* want is not a descent — the cursor stays
	// where it is and answers again — so a batch may name the same value twice.
	Seek(want []byte) (ids IDRun, found bool, err error)
}

// Base is a disk-resident property index: the forward direction (key and value
// to ids) and the reverse (entity to the keys it is indexed under), read in
// place.
//
// Methods that decode a run return an error, because a corrupt image can make
// one unreadable and the alternative — treating an unreadable run as an absent
// value — turns a damaged file into a wrong answer. Methods that read only what
// parsing already bounded do not.
type Base interface {
	// Keys returns the keys this base carries for kind, ascending. A key the
	// caller declared but that carried no entry at the last compaction is
	// absent, exactly as an empty bucket is absent from the resident maps.
	Keys(kind EntityKind) []string

	// KeyStats returns the number of distinct values under key and the number
	// of (id, value) entries across them. Both are zero for an absent key.
	// This is the planner's selectivity input and the ordered index's totalIDs.
	KeyStats(kind EntityKind, key string) (distinct, entries int)

	// Lookup returns the ids indexed under key=value, ascending. An absent key
	// or value yields an empty run and no error.
	Lookup(kind EntityKind, key string, value []byte) (IDRun, error)

	// Cursor returns a forward-only cursor over key's values.
	//
	// A base that does not carry key returns a cursor that finds nothing rather
	// than nil, so a caller resolving a thousand values against a key the image
	// never held is written once instead of twice. It is not an error and not a
	// missing capability: the delta may well hold the key, and the batch path
	// merges the two sides exactly as every other read path does.
	Cursor(kind EntityKind, key string) ValueCursor

	// ForEachValue walks key's distinct values in ascending byte order,
	// beginning at the first value not less than from. A nil from starts at the
	// first value. Returning false from fn stops the walk without an error.
	//
	// This is the whole of the range path: a caller resolves its filter to a
	// lower bound, walks from there, and stops itself at the upper bound — one
	// search rather than two, and no index arithmetic on this side of the
	// boundary.
	ForEachValue(kind EntityKind, key string, from []byte, fn func(value []byte, ids IDRun) bool) error

	// ForEachEntryOf walks the entries registered for one entity, ascending by
	// key and then by value.
	ForEachEntryOf(kind EntityKind, id uint64, fn func(key string, value []byte) bool) error

	// HasEntries reports whether id carries any entry at all.
	HasEntries(kind EntityKind, id uint64) bool

	// ForEachID walks every distinct indexed id of kind, ascending. Returning
	// false from fn stops the walk.
	ForEachID(kind EntityKind, fn func(id uint64) bool)

	// MaxID returns the highest indexed id of kind, or zero if there are none.
	// This sizes the retracted bitset: an id above it was never in the base, so
	// it can never need retracting.
	MaxID(kind EntityKind) uint64

	// TotalEntries returns the entry count across every key of kind.
	TotalEntries(kind EntityKind) int

	// Verify checks the base's own structure and returns the first
	// inconsistency, or cc's error if it is cancelled.
	//
	// This is the pass the read paths above deliberately do not make. Every one
	// of them is bounded — a malformed run makes a lookup fail rather than
	// address memory it should not — but none of them establishes that the base
	// is *consistent*: that its values ascend the way the search assumes, that
	// its runs hold the number of entries its directory claims, that the forward
	// and reverse directions describe the same set of entries. Each of those is
	// a pass over every entry, and charging every query for one is the cost this
	// whole design exists to remove.
	//
	// So it is a method rather than something a reader does on the way past, and
	// it is on this interface rather than left to whoever happens to hold the
	// concrete type: an implementation is the only thing that knows what its own
	// encoding promises, and a base nobody can ask to check itself is a base that
	// silently goes unchecked. PropertyIndex.VerifyBase is what calls it.
	//
	// Memory must be bounded — that is the requirement, not an observation: this
	// is what a store runs on an index that is already near the ceiling the
	// program is aimed at, so an implementation that materialised the entries in
	// order to check them would be unusable exactly when it was needed.
	Verify(cc *store.CancelCheck) error
}
