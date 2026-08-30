package store

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors for conditions a caller is expected to branch on.
//
// Structured errors — ErrNotFound, ErrInvalidEdge — live in interface.go next to
// the methods that return them. What lives here is the flat kind: a condition
// with nothing to report but itself, where the caller's question is "which of
// the documented refusals was this" and errors.Is answers it.

// ErrNoLabels is returned when a node or edge is written with an empty label
// set.
//
// A label-less node is storable in principle — nothing in the record layout
// forbids it — and that is exactly the problem. It indexes into no label
// posting, so it is invisible to NodesByType and to every Types-filtered query,
// while NodeCount still counts it. The write succeeds and the data is gone: no
// error, no symptom, and no way to find the node again except by an ID the
// caller has to have kept.
//
// UpdateNode has always rejected this. AddNode did not, so the same struct that
// inserted cleanly failed on update, and three separate documents described a
// requirement no add path enforced.
var ErrNoLabels = errors.New("graphene: node or edge must carry at least one label")

// ErrUniqueViolation is returned when a write would leave two live entities
// holding one value under a key declared unique.
//
// It is a refusal, never a repair: the engine does not pick a winner, because
// which of two entities is the real one is a question about the caller's data
// and not about the graph. Errors carrying it name the incumbent, so the caller
// can decide.
var ErrUniqueViolation = errors.New("graphene: unique constraint violated")

// UniqueConflict names one value held by more than one live entity.
//
// IDs are uint64 rather than NodeID or EdgeID so that one type serves both:
// the caller that has to repair a graph should not have two nearly identical
// shapes to handle.
type UniqueConflict struct {
	Value []byte
	IDs   []uint64
}

// UniqueViolationsError reports every value blocking a uniqueness declaration.
//
// Every one, not the first. A caller declaring a constraint on an existing graph
// is about to repair it, and reporting one duplicate at a time means one full
// pass per duplicate — which is how a repair that should be a single script
// becomes an afternoon.
type UniqueViolationsError struct {
	Kind      string // "node" or "edge"
	Key       string
	Conflicts []UniqueConflict
}

func (e *UniqueViolationsError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "graphene: cannot declare %s property %q unique: %d value(s) held more than once",
		e.Kind, e.Key, len(e.Conflicts))
	// A bounded sample, because a broken import can produce thousands and an
	// error string is read by a person. The full set is on Conflicts.
	const sample = 5
	for i, c := range e.Conflicts {
		if i == sample {
			fmt.Fprintf(&b, "; and %d more", len(e.Conflicts)-sample)
			break
		}
		fmt.Fprintf(&b, "; %q held by %v", c.Value, c.IDs)
	}
	return b.String()
}

// Unwrap makes errors.Is(err, ErrUniqueViolation) true for a declaration that
// found existing duplicates, so a caller has one condition to test whatever
// stage the violation surfaced at.
func (e *UniqueViolationsError) Unwrap() error { return ErrUniqueViolation }

// Sorted reports whether the conflicts are in ascending value order, which they
// always are as returned. Present so a test can say so without reaching for
// bytes.Compare itself.
func (e *UniqueViolationsError) Sorted() bool {
	for i := 1; i < len(e.Conflicts); i++ {
		if bytes.Compare(e.Conflicts[i-1].Value, e.Conflicts[i].Value) > 0 {
			return false
		}
	}
	return true
}

// ErrIndexedPropertiesRequired is returned by UpdateNode or UpdateEdge when the
// entity carries property-index entries and the store's ReindexPolicy is
// ReindexReject.
//
// The entries are caller-encoded and the properties blob is opaque, so an update
// cannot know which of them the change invalidated. Every available answer to
// that is wrong in some case — keeping them leaves the index matching a value the
// entity no longer holds, purging them drops keys the update never touched — so
// the default answer is to say so and let the caller use UpdateNodeIndexed or
// UpdateNodePartialIndex, which do know.
var ErrIndexedPropertiesRequired = errors.New(
	"graphene: entity carries indexed properties; use UpdateNodeIndexed, UpdateNodePartialIndex, or set a ReindexPolicy")

// ErrWriteConflict is returned when a transaction's view of a unique key changed
// under it between the upsert being buffered and the transaction committing.
//
// An upsert has to resolve its key to an ID at buffer time, because the ID is
// what the caller names in the edges it buffers next; it cannot be re-mapped at
// commit without rewriting those edges. So the resolution is re-checked under the
// write lock instead, and a transaction that raced another writer for the same
// key is refused whole rather than silently creating a second entity. Retrying
// is the fix, and the retry finds what the winner wrote.
var ErrWriteConflict = errors.New("graphene: write conflict; retry the transaction")
