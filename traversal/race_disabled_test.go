//go:build !race

package traversal

// raceEnabled reports whether the binary was built with the race detector.
// See race_enabled_test.go.
const raceEnabled = false
