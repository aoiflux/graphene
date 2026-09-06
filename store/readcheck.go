package store

import (
	"errors"
	"math"
)

// Read sets.
//
// A transaction that only writes needs no protection: every operation is
// resolved and applied under one exclusive hold, so nothing can change between
// deciding and doing. A transaction that *reads* is different. The read happens
// while the caller is still buffering, outside any lock, and whatever it decided
// from can move before Commit takes the lock — which is the whole hazard in
// read-modify-write, the most common thing a database is asked to do:
//
//	n, _ := tx.GetNode(id)          // reads "state: pending"
//	n.Properties = mark(n, "done")  // decides from it
//	tx.UpdateNode(n)                // writes back
//	err := tx.Commit()              // another writer got there first?
//
// Without a read set the last line cannot answer that question, and the update
// silently overwrites work it never saw.
//
// A ReadCheck is one such observation, recorded at read time and re-evaluated
// under the write lock at commit. Validation re-reads and compares rather than
// consulting a version stamp, which is what lets both bundled backends
// implement it without either of them growing per-record versioning: the
// in-memory store keeps a single store-wide counter and nothing finer, so an
// epoch-based read set would have been disk-only and would have broken the rule
// that the two backends accept and reject exactly the same things.
//
// # What this does and does not prevent
//
// It prevents a decision made from a record that has since changed or been
// deleted, and a read-modify-write racing another writer over the same records.
//
// It does **not** give serialisability, and does not prevent phantoms: a
// transaction that read "no node holds this label" is not protected against one
// appearing, because a predicate over the whole graph cannot be validated by
// re-reading a bounded set of records. That is why the tracked read methods stop
// at reads anchored to an identity — a node, an edge, one node's adjacency, one
// unique key — and why there is deliberately no tracked QueryNodeIDs.

// ReadRefKind selects what a ReadCheck observed, and therefore how it is
// validated.
type ReadRefKind uint8

const (
	// ReadNode observed a node's whole record: existence, labels and
	// properties.
	ReadNode ReadRefKind = iota + 1

	// ReadNodeExists observed only whether a node is live. It is a weaker check
	// than ReadNode on purpose — NodeExists does not materialise the record, and
	// a check that needed the record would have made it do so.
	ReadNodeExists

	// ReadEdge observed an edge's whole record: existence, endpoints, weight,
	// labels and properties.
	ReadEdge

	// ReadNodeAdjacency observed the set of edges incident to one node in one
	// direction under one type filter, contents included. It catches an edge
	// appearing, disappearing, or changing.
	ReadNodeAdjacency

	// ReadUniqueNodeOwner observed which node holds a value under a
	// declared-unique key, or that none does.
	ReadUniqueNodeOwner

	// ReadUniqueEdgeOwner is ReadUniqueNodeOwner for edges.
	ReadUniqueEdgeOwner
)

func (k ReadRefKind) String() string {
	switch k {
	case ReadNode:
		return "node"
	case ReadNodeExists:
		return "node-exists"
	case ReadEdge:
		return "edge"
	case ReadNodeAdjacency:
		return "node-adjacency"
	case ReadUniqueNodeOwner:
		return "unique-node-owner"
	case ReadUniqueEdgeOwner:
		return "unique-edge-owner"
	default:
		return "unknown"
	}
}

// ReadCheck is one observation a transaction made about the store, to be
// re-evaluated under the write lock before the transaction is applied.
//
// Only the fields Kind selects are meaningful. A backend validating one must
// treat every field it does not understand as absent rather than guessing, and
// must refuse a Kind it does not recognise — a check silently skipped is a
// guarantee silently withdrawn.
type ReadCheck struct {
	Kind ReadRefKind

	// ID is the node or edge the observation is anchored to, for every kind but
	// the unique-owner ones, where it is the observed owner and is meaningful
	// only when Exists is true.
	ID uint64

	// Exists is what the read found: whether the record was live, or whether
	// any entity held the unique value. It is unused by ReadNodeAdjacency, whose
	// whole answer is in Digest.
	Exists bool

	// Digest fingerprints the content observed, and is 0 when Exists is false.
	// It is not a version number: it is recomputed from the current record at
	// validation time and compared, so a record rewritten with identical bytes
	// is deliberately not a conflict.
	Digest uint64

	// Key and Value name the declared-unique property, for the unique-owner
	// kinds.
	Key   string
	Value []byte

	// Dir and Types are the filter the adjacency read ran under, for
	// ReadNodeAdjacency. Validation must re-read with exactly these or it is
	// comparing two different questions.
	Dir   Direction
	Types []EdgeType
}

// CheckedTransactor is an optional extension implemented by stores that can
// validate a transaction's read set. Both bundled backends implement it.
//
// It is separate from Transactor because it is a *constraint*, not an
// optimisation: a store that accepted a read set and did not check it would
// hand the caller the guarantee they asked for and none of the behaviour. So a
// transaction carrying reads is refused outright on a backend that does not
// implement this, rather than committing unprotected.
type CheckedTransactor interface {
	Transactor

	// ApplyTransactionChecked validates checks and applies ops under one
	// exclusive hold, so nothing can change between the two.
	//
	// Every check is evaluated against the store as it stands *before* any of
	// ops is applied, which is the state the reads were taken against. If any
	// check fails, nothing is written and the error wraps ErrWriteConflict,
	// naming the observation that moved.
	//
	// A nil or empty checks is exactly ApplyTransactionAs.
	ApplyTransactionChecked(ops []TxOp, checks []ReadCheck, ctx TxContext) error
}

// FNV-1a, hand-rolled rather than taken from hash/fnv, because the interface
// there boxes its state and these run once per record read in a transaction.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

func fnvByte(h uint64, b byte) uint64 { return (h ^ uint64(b)) * fnvPrime64 }

func fnvBytes(h uint64, b []byte) uint64 {
	for _, c := range b {
		h = fnvByte(h, c)
	}
	return h
}

func fnvUint64(h uint64, v uint64) uint64 {
	for range 8 {
		h = fnvByte(h, byte(v))
		v >>= 8
	}
	return h
}

// NodeDigest fingerprints a node's content for read-set validation.
//
// Labels are folded in the order they are stored rather than sorted, so a node
// rewritten with the same labels in a different order reads as changed. That is
// the conservative direction: the cost of a false conflict is one retry, and
// the cost of a missed one is a lost write.
//
// A nil node digests to 0, which is also what a ReadCheck carries when the
// record did not exist — the two are never compared, because Exists is checked
// first.
func NodeDigest(n *Node) uint64 {
	if n == nil {
		return 0
	}
	h := fnvUint64(fnvOffset64, uint64(n.ID))
	h = fnvUint64(h, uint64(len(n.Labels)))
	for _, l := range n.Labels {
		h = fnvUint64(h, uint64(l))
	}
	return fnvBytes(h, n.Properties)
}

// EdgeDigest fingerprints an edge's content for read-set validation. Weight is
// folded by its bit pattern, so a change the float comparison would call equal
// is still a change.
func EdgeDigest(e *Edge) uint64 {
	if e == nil {
		return 0
	}
	h := fnvUint64(fnvOffset64, uint64(e.ID))
	h = fnvUint64(h, uint64(e.Src))
	h = fnvUint64(h, uint64(e.Dst))
	h = fnvUint64(h, uint64(math.Float32bits(e.Weight)))
	h = fnvUint64(h, uint64(len(e.Labels)))
	for _, l := range e.Labels {
		h = fnvUint64(h, uint64(l))
	}
	return fnvBytes(h, e.Properties)
}

// EdgeSetDigest fingerprints a set of edges without regard to the order they
// were produced in.
//
// Order-independence is required, not merely convenient: the disk backend emits
// a node's incident edges delta-first and then from the image, so a compaction
// passing between the read and the commit reorders them without changing the
// set. Comparing an order-sensitive digest would report that as a conflict.
//
// The fold is XOR, which is safe here because edge IDs are distinct within one
// adjacency list, so no two members can cancel. The count is mixed in
// afterwards so that the empty set does not collide with a set that XORs to
// zero.
func EdgeSetDigest(edges []*Edge) uint64 {
	var acc uint64
	for _, e := range edges {
		acc ^= EdgeDigest(e)
	}
	return fnvUint64(fnvUint64(fnvOffset64, acc), uint64(len(edges)))
}

// ReadConflictError reports which observation moved.
//
// It wraps ErrWriteConflict, so the existing errors.Is(err, ErrWriteConflict)
// check that upserts already taught callers to write keeps working unchanged,
// and a caller who wants to know *what* moved can errors.As this out instead of
// parsing a message.
type ReadConflictError struct {
	Check ReadCheck
	Why   string
}

func (e *ReadConflictError) Error() string {
	return "graphene: read set conflict on " + e.Check.Kind.String() + " " +
		uint64ToStr(e.Check.ID) + ": " + e.Why + "; retry the transaction"
}

func (e *ReadConflictError) Unwrap() error { return ErrWriteConflict }

// ReadSetValidator validates a read set against a store, through reads the
// caller has already put under its own write lock.
//
// It exists so the comparison lives in one place. The two bundled backends
// reach their records by entirely different routes — one through a delta layer
// over a CSR image, the other through plain maps — but they must accept and
// refuse exactly the same read sets, and the surest way to guarantee that is
// for neither of them to own the decision. Each supplies the reads; this
// decides.
//
// Every function must read without taking a lock the caller already holds, and
// must answer from the state the transaction is about to be applied to.
type ReadSetValidator struct {
	Node            func(NodeID) (*Node, bool)
	Edge            func(EdgeID) (*Edge, bool)
	NodeExists      func(NodeID) bool
	EdgesOf         func(NodeID, Direction, []EdgeType) []*Edge
	UniqueNodeOwner func(key string, value []byte) (NodeID, bool, error)
	UniqueEdgeOwner func(key string, value []byte) (EdgeID, bool, error)
}

// Validate reports the first check that no longer holds, as a
// *ReadConflictError wrapping ErrWriteConflict.
//
// It stops at the first failure rather than collecting them all, which is the
// opposite of what the unique-constraint declarers do. The difference is what
// the caller does next: a constraint violation is repaired, so naming every
// offender saves a pass, whereas a conflict is retried, and the retry re-reads
// everything anyway.
//
// An unrecognised Kind is an error, not a conflict, and not a skip. A check
// this code does not understand is a guarantee it cannot honour, and the only
// safe thing to do with one is to refuse the transaction and say so.
func (v ReadSetValidator) Validate(checks []ReadCheck) error {
	for _, c := range checks {
		switch c.Kind {
		case ReadNode:
			n, ok := v.Node(NodeID(c.ID))
			if ok != c.Exists {
				return &ReadConflictError{Check: c, Why: existenceMoved(c.Exists)}
			}
			if ok && NodeDigest(n) != c.Digest {
				return &ReadConflictError{Check: c, Why: "the record changed"}
			}

		case ReadNodeExists:
			if v.NodeExists(NodeID(c.ID)) != c.Exists {
				return &ReadConflictError{Check: c, Why: existenceMoved(c.Exists)}
			}

		case ReadEdge:
			e, ok := v.Edge(EdgeID(c.ID))
			if ok != c.Exists {
				return &ReadConflictError{Check: c, Why: existenceMoved(c.Exists)}
			}
			if ok && EdgeDigest(e) != c.Digest {
				return &ReadConflictError{Check: c, Why: "the record changed"}
			}

		case ReadNodeAdjacency:
			if EdgeSetDigest(v.EdgesOf(NodeID(c.ID), c.Dir, c.Types)) != c.Digest {
				return &ReadConflictError{Check: c, Why: "the incident edges changed"}
			}

		case ReadUniqueNodeOwner:
			owner, found, err := v.UniqueNodeOwner(c.Key, c.Value)
			if err != nil {
				return err
			}
			if found != c.Exists || (found && uint64(owner) != c.ID) {
				return &ReadConflictError{Check: c, Why: "the key " + quoteKey(c.Key) + " now names " + ownerDesc(found, uint64(owner))}
			}

		case ReadUniqueEdgeOwner:
			owner, found, err := v.UniqueEdgeOwner(c.Key, c.Value)
			if err != nil {
				return err
			}
			if found != c.Exists || (found && uint64(owner) != c.ID) {
				return &ReadConflictError{Check: c, Why: "the key " + quoteKey(c.Key) + " now names " + ownerDesc(found, uint64(owner))}
			}

		default:
			return errors.New("graphene: unrecognised read check kind " + uint64ToStr(uint64(c.Kind)) +
				"; refusing the transaction rather than committing it unvalidated")
		}
	}
	return nil
}

func existenceMoved(was bool) string {
	if was {
		return "it has been deleted"
	}
	return "it has been created"
}

func ownerDesc(found bool, id uint64) string {
	if !found {
		return "nothing"
	}
	return uint64ToStr(id)
}

func quoteKey(k string) string { return "\"" + k + "\"" }
