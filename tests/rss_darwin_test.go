//go:build darwin

package graphene_test

import "syscall"

// Darwin supplies the peak cheaply through getrusage, but the anon/file split
// and the current resident size both require Mach calls (task_info with
// MACH_TASK_BASIC_INFO, or the phys_footprint counter) that are not reachable
// from the standard library without cgo. This engine's zero-cgo constraint is
// not worth breaking for a benchmark instrument, so darwin reports the peak
// only and declares no split, and callers that need the split run on linux.
const rssSupported = true

// Peak only: getrusage reports the high-water mark and nothing about the size
// now, so a caller — and the calibration in rss_test.go — must not read a zero
// Total as "this process holds nothing". This is what separates "no instrument"
// from "an instrument that answers one of the two questions".
const rssCurrent = false

func readRSS() rssSample {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return rssSample{}
	}
	// Unlike linux, darwin reports ru_maxrss in bytes.
	peak := uint64(ru.Maxrss)
	return rssSample{Peak: peak, Split: false}
}
