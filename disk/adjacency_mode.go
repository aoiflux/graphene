package disk

// Choosing when a CSR image's adjacency is built.
//
// # What the choice is
//
// Adjacency is derived state. The image carries edge records, each naming its
// two endpoints; what a traversal needs is the inverse — given a node, the edges
// incident to it — and that is four arrays the loader computes at Open:
// outOffset and inOffset, one uint64 per node slot, and outEdges and inEdges,
// one EdgeID per live edge in each direction. Sixteen bytes per edge and sixteen
// per node slot, in anonymous memory, for the life of the handle.
//
// Nothing in the file changes. AdjacencyMode decides only whether that pass runs
// at Open or the first time something asks for it.
//
// # Why it is not simply "lazy is better"
//
// Because the writer cannot skip it. DeleteNode cascades to incident edges, and
// it finds them through exactly these arrays — so a process that deletes
// anything builds adjacency on its first delete, having also paid for the branch
// on every read until then. The mode is worth asking for in a process that opens
// a store and reads aggregates out of it without traversing and without
// deleting, which is a real and common shape, and is worth nothing at all in a
// writer.
//
// That is why AdjacencyEager is the default. The lazy mode is not a better
// setting that compatibility keeps from being the default; it is the right
// setting for one kind of process and the wrong one for another, and only the
// caller knows which it is running.
//
// # One build, two moments
//
// The build itself reads the edge arena, not the sequence that filled it, so the
// eager and the lazy path are the same function called at different times rather
// than two pieces of code that have to be kept agreeing. A lazy store that
// builds adjacency on its first traversal is holding what an eager one holds,
// arranged identically, and StorageStats.Adjacency is how to tell which of the
// two moments a given handle is past.

import "fmt"

// AdjacencyMode decides whether a CSR image's adjacency arrays are built at Open
// or on first use.
type AdjacencyMode uint8

const (
	// AdjacencyEager builds adjacency while the image is being loaded. This is
	// the zero value and the default: every traversal, every degree query and
	// every delete cascade needs it, so for any store that does one of those it
	// is work that has to happen and happens once.
	AdjacencyEager AdjacencyMode = iota

	// AdjacencyLazy defers the build to the first call that needs it — a
	// traversal, a degree, an edge-of-node read, a delete cascade, or
	// VerifyIndexes checking it.
	//
	// Ask for it in a process that opens a store to read node and edge
	// properties out of it and never walks the graph: the arrays are then never
	// built and never held, which is 16 bytes per edge and 16 per node slot of
	// anonymous memory, plus one pass over the edge arena at Open.
	//
	// It is a deferral and not a refusal. A process that asks for it and then
	// traverses gets the same arrays, built at the first call rather than at
	// Open, and pays the same total. There is no mode in which a traversal
	// answers without them.
	//
	// In particular a writer that deletes gets nothing from it: DeleteNode's
	// cascade to incident edges reads these arrays, so the first delete builds
	// them. Check StorageStats.Adjacency to see which side of the build a handle
	// is actually on.
	AdjacencyLazy
)

// String names the mode for StorageStats and for an error message.
func (m AdjacencyMode) String() string {
	switch m {
	case AdjacencyEager:
		return "eager"
	case AdjacencyLazy:
		return "lazy"
	}
	return fmt.Sprintf("AdjacencyMode(%d)", uint8(m))
}

// rootsAdjacencyMode is how the throwaway graph a root check builds should hold
// its adjacency, and it is a function rather than a literal at the one call site
// because it states a premise that a later change could invalidate.
//
// The premise: verifying an image's roots reads records, property entries and
// tombstones, and no adjacency — see verifyCSRRootsOf. So the graph verifyImage
// builds is discarded three lines later having never looked at the four arrays,
// which under the eager mode were 16 bytes per edge and 16 per node slot,
// allocated at Open, read by nothing, and freed.
//
// TestRoots_DoNotNeedTheAdjacency holds that premise to account. If a root is
// ever added over adjacency, that test fails and this is where the answer
// changes — rather than the arrays quietly being built on demand inside a
// verification that then measures a different thing than it used to.
func rootsAdjacencyMode() AdjacencyMode { return AdjacencyLazy }

// adjacencyHolding names what the store's image is actually holding, for
// StorageStats: "built" once the arrays exist, "deferred" while they do not.
//
// Read off the graph rather than off the option, for the reason indexHolding is:
// the option is a request and this is the answer. A store opened AdjacencyLazy
// that has since traversed reports "built", because that is what it is paying
// for, and a caller checking whether its read-only pass really avoided the build
// has no other way to find out.
func (s *Store) adjacencyHolding() string {
	csr := s.cur().csr
	if csr == nil || csr.AdjacencyBuilt() {
		return "built"
	}
	return "deferred"
}
