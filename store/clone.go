package store

// Copying a record out of whatever the engine handed it back from.
//
// # Why a caller would need this
//
// Every backend returns records that alias its own memory. The disk store's
// invariant says so in as many words — "reads alias out" — and it is what keeps
// a point lookup at six nanoseconds instead of an allocation and a copy of a
// blob that is usually not even looked at. A caller that reads a node, pulls one
// field out of it and drops it never notices.
//
// A caller that *keeps* records does. What the alias points at depends on how
// the image is held:
//
//   - an image copied into the heap (disk.ImageHeap) backs a returned slice with
//     an arena the graph owns, so the bytes outlive the store handle: holding
//     them is a leak of the whole arena rather than a fault.
//   - a mapped image (disk.ImageMapped, the default) backs it with the file. The
//     bytes are valid for the life of the handle, and reading them after Close
//     is a fault, not stale data.
//   - a record from the delta layer aliases the blob the caller passed in at
//     write time.
//
// So the rule for a caller that retains records across a Close, or across a live
// reader's Refresh, is to copy them, and these are the two functions that do it
// correctly — including the part that is easy to get wrong, which is that
// Labels needs copying too.
//
// # Why this is not the default
//
// Because it is the caller's decision and not the engine's. Copying on the way
// out would put an allocation and a memcpy of the blob on every read, for the
// benefit of the reads that retain — and in the workload this engine is built
// for, scanning a type and projecting one field, that is nearly none of them.
// The engine's job is to make the alias contract explicit and to provide the
// copy; choosing to pay for it is the caller's.

// CloneNode returns a deep copy of n, sharing nothing with the store that
// produced it.
//
// Nil in, nil out. An empty Labels or Properties stays empty rather than
// becoming nil and a nil one stays nil, because both stores normalise an empty
// blob to nil on the way in and a copy that changed which one it was would be
// distinguishable from its original.
func CloneNode(n *Node) *Node {
	if n == nil {
		return nil
	}
	return &Node{
		ID:         n.ID,
		Labels:     cloneSlice(n.Labels),
		Properties: cloneSlice(n.Properties),
	}
}

// CloneEdge returns a deep copy of e, sharing nothing with the store that
// produced it. Nil in, nil out.
func CloneEdge(e *Edge) *Edge {
	if e == nil {
		return nil
	}
	return &Edge{
		ID:         e.ID,
		Src:        e.Src,
		Dst:        e.Dst,
		Labels:     cloneSlice(e.Labels),
		Weight:     e.Weight,
		Properties: cloneSlice(e.Properties),
	}
}

// CloneNodes copies a batch, preserving nil entries — GetNodesBatch leaves a
// hole where an ID was not found, and a copy that closed the holes would change
// the correspondence between the result and the IDs asked for.
func CloneNodes(nodes []*Node) []*Node {
	if nodes == nil {
		return nil
	}
	out := make([]*Node, len(nodes))
	for i, n := range nodes {
		out[i] = CloneNode(n)
	}
	return out
}

// CloneEdges copies a batch on the same terms as CloneNodes.
func CloneEdges(edges []*Edge) []*Edge {
	if edges == nil {
		return nil
	}
	out := make([]*Edge, len(edges))
	for i, e := range edges {
		out[i] = CloneEdge(e)
	}
	return out
}

// cloneSlice copies s, keeping nil distinct from empty. The three-index form on
// the result is not needed — make gives cap == len already — but the nil case is,
// for the reason CloneNode's comment gives.
func cloneSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	out := make([]T, len(s))
	copy(out, s)
	return out
}
