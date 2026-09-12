package index

// Unique property keys.
//
// A unique key is a promise that at most one live entity carries any given value
// under it, which is what lets a caller treat a value as a name: "the node whose
// k is ver:9f3a" is a thing that exists or does not, never a thing there might be
// two of. Without it, an upsert cannot be written at all — resolve-by-key is
// undefined when the key resolves to a set.
//
// Nothing new is stored. The hash postings already hold, per key, every distinct
// value with its posting list, so "this key is unique" is exactly "every value
// under it has one posting", and the owner of a value is that posting's single
// element. Building a second structure would mean a second thing that can
// disagree with the first, in a package whose whole verification story is that
// the forward and reverse maps agree.
//
// The declaration set lives on the shard, which needs no new lock and no lock
// ordering: shards are chosen by hashing the property key, so a key and its
// uniqueness live in the same shard by construction.

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/aoiflux/graphene/store"
)

// LiveNode reports whether a node id still names a live record.
//
// Declaration takes a predicate rather than consulting the store itself, so this
// package stays free of any knowledge of what a record is — and so the whole
// check runs inside one shard lock hold, with no window between deciding a key
// is unique and marking it so.
type LiveNode func(store.NodeID) bool

// LiveEdge is LiveNode for edges.
type LiveEdge func(store.EdgeID) bool

// DeclareUniqueNodeKey marks key unique, or reports why it cannot be.
//
// Every conflicting value is returned, not the first: the caller's next move is
// to repair the graph, and repairing it one error at a time means one full pass
// per duplicate. A non-empty result means nothing was declared.
//
// Declaring a key already declared is a no-op and returns nothing, which is what
// makes it safe to declare at every Open — the same contract the ordered and
// composite declarations carry.
//
// The error is separate from the conflicts and means something else entirely: not
// "the data violates this" but "the data could not be read, so whether it violates
// this is unknown". Only a damaged base produces one, and nothing is declared when
// it does. It is a return value rather than a recorded fault because a declaration
// is the one place a constraint is established over data that already exists —
// every later registration is gated by IndexNodeUnique, which has its own error and
// its own lock hold, but nothing re-checks the existing entries afterwards.
func (p *PropertyIndex) DeclareUniqueNodeKey(key string, live LiveNode) ([]store.UniqueConflict, error) {
	sh := p.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if _, exists := sh.uniqueNodeKeys[key]; exists {
		return nil, nil
	}
	isLive := func(id store.NodeID) bool { return live == nil || live(id) }
	var conflicts []store.UniqueConflict
	if s, hasBase := p.nodeBase(); hasBase {
		var err error
		if conflicts, err = s.mergeConflicts(key, sh.nodes.byKey[key], isLive); err != nil {
			return nil, err
		}
	} else {
		conflicts = conflictsUnder(sh.nodes.byKey[key], isLive)
	}
	if len(conflicts) > 0 {
		return conflicts, nil
	}
	sh.uniqueNodeKeys[key] = struct{}{}
	return nil, nil
}

// DeclareUniqueEdgeKey is DeclareUniqueNodeKey for edge properties.
func (p *PropertyIndex) DeclareUniqueEdgeKey(key string, live LiveEdge) ([]store.UniqueConflict, error) {
	sh := p.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if _, exists := sh.uniqueEdgeKeys[key]; exists {
		return nil, nil
	}
	isLive := func(id store.EdgeID) bool { return live == nil || live(id) }
	var conflicts []store.UniqueConflict
	if s, hasBase := p.edgeBase(); hasBase {
		var err error
		if conflicts, err = s.mergeConflicts(key, sh.edges.byKey[key], isLive); err != nil {
			return nil, err
		}
	} else {
		conflicts = conflictsUnder(sh.edges.byKey[key], isLive)
	}
	if len(conflicts) > 0 {
		return conflicts, nil
	}
	sh.uniqueEdgeKeys[key] = struct{}{}
	return nil, nil
}

// conflictsUnder collects every value in bucket held by two or more live ids.
//
// The liveness filter is the whole reason this is not a length check. A posting
// can outlive the entity it names while a snapshot pins an older view, and
// refusing a declaration because of an entry no read would ever return would
// make the constraint undeclarable on a perfectly sound graph.
func conflictsUnder[T entityID](bucket map[string][]T, live func(T) bool) []store.UniqueConflict {
	var conflicts []store.UniqueConflict
	for value, ids := range bucket {
		if len(ids) < 2 {
			continue
		}
		var holders []uint64
		for _, id := range ids {
			if live(id) {
				holders = append(holders, uint64(id))
			}
		}
		if len(holders) > 1 {
			conflicts = append(conflicts, store.UniqueConflict{
				Value: []byte(value),
				IDs:   holders,
			})
		}
	}
	// Sorted by value: map iteration would hand a repair tool, a test and an
	// error message three different orders for one graph.
	slices.SortFunc(conflicts, func(a, b store.UniqueConflict) int {
		return bytes.Compare(a.Value, b.Value)
	})
	return conflicts
}

// IsUniqueNodeKey reports whether key is declared unique for nodes.
func (p *PropertyIndex) IsUniqueNodeKey(key string) bool {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	_, ok := sh.uniqueNodeKeys[key]
	return ok
}

// IsUniqueEdgeKey reports whether key is declared unique for edges.
func (p *PropertyIndex) IsUniqueEdgeKey(key string) bool {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	_, ok := sh.uniqueEdgeKeys[key]
	return ok
}

// UniqueNodeKeys returns the declared unique node keys, sorted.
func (p *PropertyIndex) UniqueNodeKeys() []string {
	return p.collectUniqueKeys(func(sh *propertyShard) map[string]struct{} {
		return sh.uniqueNodeKeys
	})
}

// UniqueEdgeKeys returns the declared unique edge keys, sorted.
func (p *PropertyIndex) UniqueEdgeKeys() []string {
	return p.collectUniqueKeys(func(sh *propertyShard) map[string]struct{} {
		return sh.uniqueEdgeKeys
	})
}

func (p *PropertyIndex) collectUniqueKeys(pick func(*propertyShard) map[string]struct{}) []string {
	var keys []string
	for i := range p.shards {
		sh := &p.shards[i]
		sh.mu.RLock()
		for k := range pick(sh) {
			keys = append(keys, k)
		}
		sh.mu.RUnlock()
	}
	slices.Sort(keys)
	return keys
}

// NodeUniqueOwner returns the single node holding value under key.
//
// It answers from the postings whether or not key is declared unique, because
// the answer is the same either way when there is one holder — and the caller
// that needs the declaration checked has IsUniqueNodeKey for that. ok is false
// when the value is unheld, and also when it is held by more than one entity,
// which cannot happen under a declaration and is not this function's business to
// diagnose.
//
// # Why a damaged base reports "unheld" here and does not return an error
//
// Under a base the answer spans both sides, and a base that will not decode makes
// it unknown. ok is false then, and the fault is recorded on the index
// (BaseFault), because every caller of this reaches the shard lock again before
// anything is committed: an upsert that fails to resolve a key goes on to register
// the value through IndexNodeUnique, and a transaction's pre-check is re-run by
// txcheck under the same lock the apply holds. Both of those return an error, and
// both refuse. So a fault costs a caller a wasted resolve, not a duplicate — and
// the fourteen call sites that would have to thread an error do not have to.
func (p *PropertyIndex) NodeUniqueOwner(key string, value []byte) (store.NodeID, bool) {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if s, hasBase := p.nodeBase(); hasBase {
		id, ok, err := s.soleHolder(key, value, sh.nodes.byKey[key][string(value)])
		return id, ok && err == nil
	}
	return soleHolder(sh.nodes.byKey[key], string(value))
}

// EdgeUniqueOwner is NodeUniqueOwner for edges.
func (p *PropertyIndex) EdgeUniqueOwner(key string, value []byte) (store.EdgeID, bool) {
	sh := p.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if s, hasBase := p.edgeBase(); hasBase {
		id, ok, err := s.soleHolder(key, value, sh.edges.byKey[key][string(value)])
		return id, ok && err == nil
	}
	return soleHolder(sh.edges.byKey[key], string(value))
}

// soleHolderLocked is the uniqueness predicate both gates use, with the shard
// already held: the one id holding value under key, across the base and the delta.
//
// It is written once over the kind rather than twice over the ID type because the
// two gates want the answer as a uint64 either way, and a generic over T would
// need the shard's postings passed in as well as the kind — which is the argument
// that both parameters describe the same thing.
func (p *PropertyIndex) soleHolderLocked(sh *propertyShard, kind EntityKind, key, value string) (uint64, bool, error) {
	if kind == EdgeKind {
		if s, hasBase := p.edgeBase(); hasBase {
			id, ok, err := s.soleHolder(key, unsafeBytes(value), sh.edges.byKey[key][value])
			return uint64(id), ok, err
		}
		id, ok := soleHolder(sh.edges.byKey[key], value)
		return uint64(id), ok, nil
	}
	if s, hasBase := p.nodeBase(); hasBase {
		id, ok, err := s.soleHolder(key, unsafeBytes(value), sh.nodes.byKey[key][value])
		return uint64(id), ok, err
	}
	id, ok := soleHolder(sh.nodes.byKey[key], value)
	return uint64(id), ok, nil
}

func soleHolder[T entityID](bucket map[string][]T, value string) (T, bool) {
	var zero T
	if bucket == nil {
		return zero, false
	}
	ids := bucket[value]
	if len(ids) != 1 {
		return zero, false
	}
	return ids[0], true
}

// ErrUniqueTaken reports a registration refused because another entity already
// holds the value.
//
// It carries the incumbent so the caller can say which one, and so an upsert can
// distinguish "someone else has it" from "I already have it" without a second
// lookup.
type ErrUniqueTaken struct {
	Key   string
	Value []byte
	Owner uint64
}

// Unwrap gives every refusal one testable condition, whether it came from a
// registration or from a declaration over existing data.
func (e *ErrUniqueTaken) Unwrap() error { return store.ErrUniqueViolation }

func (e *ErrUniqueTaken) Error() string {
	return fmt.Sprintf("graphene: unique key %q value %q is already held by id %d", e.Key, e.Value, e.Owner)
}

// IndexNodeUnique registers one entry, refusing it if key is declared unique and
// a different node already holds value.
//
// Check and insert happen inside one shard write lock. Splitting them — ask, then
// register — would leave exactly the window the constraint exists to close, and
// would leave it open to the very concurrency the caller declared uniqueness in
// order to stop worrying about.
//
// Re-registering the same (id, key, value) is not a conflict. An entity holding
// its own value is the steady state an idempotent re-ingest arrives at, and the
// underlying insert is already a no-op for it.
// Under a base the check reads the base inside the shard write lock, which can
// mean a page fault while a writer's shard is held. That is the price of the check
// and the insert being one step, and the alternative — read the base first, then
// take the lock — is the window the constraint exists to close.
func (p *PropertyIndex) IndexNodeUnique(id store.NodeID, key string, value []byte) error {
	vk := string(value)
	sh := p.shardFor(key)
	sh.mu.Lock()
	if _, unique := sh.uniqueNodeKeys[key]; unique {
		owner, held, err := p.soleHolderLocked(sh, NodeKind, key, vk)
		if err != nil {
			sh.mu.Unlock()
			// A gate that cannot read cannot pass. Every other read path in this
			// package answers from the delta and records the fault; this one is
			// the one whose wrong answer is a duplicate.
			return err
		}
		if held && store.NodeID(owner) != id {
			sh.mu.Unlock()
			return &ErrUniqueTaken{Key: key, Value: []byte(vk), Owner: owner}
		}
	}
	sh.nodes.add(id, key, vk)
	if idx := sh.orderedNodeKeys[key]; idx != nil {
		idx.add(id, vk)
	}
	sh.mu.Unlock()

	if p.compDeclared.Load() {
		p.nodeComposites.registered(id, key, vk)
	}
	return nil
}

// IndexEdgeUnique is IndexNodeUnique for edges.
func (p *PropertyIndex) IndexEdgeUnique(id store.EdgeID, key string, value []byte) error {
	vk := string(value)
	sh := p.shardFor(key)
	sh.mu.Lock()
	if _, unique := sh.uniqueEdgeKeys[key]; unique {
		owner, held, err := p.soleHolderLocked(sh, EdgeKind, key, vk)
		if err != nil {
			sh.mu.Unlock()
			return err
		}
		if held && store.EdgeID(owner) != id {
			sh.mu.Unlock()
			return &ErrUniqueTaken{Key: key, Value: []byte(vk), Owner: owner}
		}
	}
	sh.edges.add(id, key, vk)
	if idx := sh.orderedEdgeKeys[key]; idx != nil {
		idx.add(id, vk)
	}
	sh.mu.Unlock()

	if p.compDeclared.Load() {
		p.edgeComposites.registered(id, key, vk)
	}
	return nil
}
