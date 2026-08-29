//go:build race

package graphene_test

// raceEnabled reports whether the binary was built with the race detector.
//
// The detector adds allocations of its own, so an exact allocation count
// measured under it is not the count the engine performs. The guards in
// graphene_alloc_guard_test.go skip rather than assert a number that would be
// wrong — and rather than being tagged out of the default suite, which is where
// they are worth the most.
const raceEnabled = true
