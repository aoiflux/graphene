package index

// Walking the index in the order the image is written in.
//
// Every other enumeration on PropertyIndex exists to answer a query. These exist
// to *write* one: a compaction that encodes GPIX needs a key's distinct values in
// bytes.Compare order with each value's ids ascending, because that is the order
// the value table is binary-searched in and the order a run is handed out as an
// ascending IDRun. ForEachNodeValue cannot be that walk — it promises no order at
// all, deliberately, because its callers are filter scans that do not care and it
// is the hot path this one is not.
//
// The three pieces here are one walk split where the locking is: the key list is
// read across every shard, and then each key is walked under its own shard's read
// lock. Which is the same window ForEachNodeProperty has had since R1(i) — a key
// added between the two is absent from the image and present in the log tail,
// and replay re-applies it as an idempotent upsert. See compact.go's payload
// comment for why that is sound rather than merely convergent.

import "github.com/aoiflux/graphene/store"

// NodePropKeys returns every indexed node key, sorted, across the delta and the
// base if one is attached.
//
// Sorted is the contract: it is what the caller writing a key directory needs,
// and a directory in any other order is binary-searched wrongly.
func (p *PropertyIndex) NodePropKeys() []string { return p.nodePropKeys() }

// EdgePropKeys is NodePropKeys for edge properties.
func (p *PropertyIndex) EdgePropKeys() []string { return p.edgePropKeys() }

// NodeValueWalker returns a walk over one node key's distinct values, ascending
// by bytes.Compare, each with that value's ids ascending and deduplicated.
//
// A factory rather than a method because the scratch is the point: ordering a
// key's values means materialising the delta's value strings for that key, and a
// method would allocate them again for every key. The returned function reuses
// one buffer across calls, so a walk of the whole index grows it to the widest
// key rather than to the index. That mattered enough to be worth the shape: on
// the export path the same reuse was 7 MB of a 100,000-entry key.
//
// Under a base only the delta's values are materialised — the base's arrive in
// order from the image — so the buffer is bounded by writes since the last
// compaction rather than by the key.
//
// Not safe for concurrent use: the buffer belongs to the walk. One walker per
// goroutine. The value and id slices handed to fn belong to the index for the
// duration of the call, the same contract ForEachNodeValue states.
func (p *PropertyIndex) NodeValueWalker() func(key string, fn func(value []byte, ids []store.NodeID) bool) {
	var vals []string
	var buf []store.NodeID
	return func(key string, fn func(value []byte, ids []store.NodeID) bool) {
		sh := p.shardFor(key)
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		bucket := sh.nodes.byKey[key]
		if s, hasBase := p.nodeBase(); hasBase {
			vals, buf = s.mergeForEachValue(key, bucket, vals, buf, fn)
			return
		}
		vals = sortedBucketValues(bucket, vals)
		for _, value := range vals {
			if !fn(unsafeBytes(value), bucket[value]) {
				return
			}
		}
	}
}

// EdgeValueWalker is NodeValueWalker for edge properties.
func (p *PropertyIndex) EdgeValueWalker() func(key string, fn func(value []byte, ids []store.EdgeID) bool) {
	var vals []string
	var buf []store.EdgeID
	return func(key string, fn func(value []byte, ids []store.EdgeID) bool) {
		sh := p.shardFor(key)
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		bucket := sh.edges.byKey[key]
		if s, hasBase := p.edgeBase(); hasBase {
			vals, buf = s.mergeForEachValue(key, bucket, vals, buf, fn)
			return
		}
		vals = sortedBucketValues(bucket, vals)
		for _, value := range vals {
			if !fn(unsafeBytes(value), bucket[value]) {
				return
			}
		}
	}
}
