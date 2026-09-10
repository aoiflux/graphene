//go:build !linux && !darwin && !windows

package graphene_test

// No instrument for this platform. Reporting zeros with rssSupported false makes
// the absence visible in the benchmark log rather than turning every memory
// result into an apparent success.
const rssSupported = false

func readRSS() rssSample { return rssSample{} }
