//go:build !unix && !windows

package disk

// Platforms with no file mapping in the standard library: js/wasm, plan9,
// wasip1.
//
// An honest refusal rather than a stub that pretends, which is the shape
// lock_unsupported.go already sets. openImage treats the error as a reason to
// read the image into the heap instead, reports store.MetricImageFallback and
// says so in StorageStats.ImageMode, so a store on one of these platforms opens
// and works — it simply pays the copy.

import "os"

// mappingSupported is what openImage tests before trying.
const mappingSupported = false

func mapFile(f *os.File, size int) ([]byte, func() error, error) {
	return nil, nil, errImageMappingUnsupported
}
