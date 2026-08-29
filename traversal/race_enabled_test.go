//go:build race

package traversal

// raceEnabled reports whether the binary was built with the race detector. The
// detector adds allocations of its own, so an exact allocation count measured
// under it is not the count the engine performs. Same contract and same reason
// as the pair in tests/; the constant is per-package because a build tag cannot
// be shared across one.
const raceEnabled = true
