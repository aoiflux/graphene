package disk

// Telling the kernel how a mapping will be used, and when it is finished with.
//
// # What this is for
//
// The 2 GiB directive bounds anonymous memory, and the mapped image is
// deliberately not that: it is file-backed page cache, which the kernel reclaims
// under pressure instead of killing the process. Reclaimable is a real
// distinction and this file does not pretend otherwise. It is also not free.
// Those pages are charged against a linux cgroup exactly like anonymous ones, so
// a store whose image is most of a container's limit puts the container into
// reclaim during the ingest it was trying to bound -- and reclaim in the middle
// of a compaction is a latency event inside the operation that is already the
// peak. Measured at 1,182 MiB of file-backed residency during an ingest of
// 400,000 x 3.2 KB records, twelve times the anonymous class and linear in the
// store.
//
// Two mechanisms, both advisory:
//
// Access-pattern advice stops readahead inflating the resident set. A sequential
// walk of a multi-gigabyte mapping pulls in far more than it reads; a point
// lookup workload wants none of it. The engine knows which it is doing and the
// kernel does not, which is the whole case for saying so.
//
// Dropping pages returns what a pass is finished with. A compaction reads the
// whole of the old image and then never touches most of it again.
//
// # Why dropping pages is safe where unmapping was not
//
// mapping.go records at length why a compaction cannot unmap the image it just
// replaced: the graph it publishes carries slice headers into that image for
// every record the delta did not touch, so the mapping has to outlive every
// generation built from it. None of that is affected here. Advice drops *pages*,
// not the mapping. No slice header is invalidated, no address changes, and a
// read of a dropped page faults it back from the file transparently. The
// contract on a returned Properties slice is untouched.
//
// The mapping is read-only, file-backed and MAP_SHARED, so there is also nothing
// to lose: the pages are a cache of the file, and the file is the truth. The
// per-platform files carry the argument for why MADV_DONTNEED's destructive
// reading, which applies to anonymous memory, cannot arise here.
//
// # What it costs, and why it is off by default
//
// A dropped page that is read again is a major fault. That is the trade in one
// sentence: less resident memory, more faults, and which of those matters is the
// caller's workload rather than the engine's business. A store that compacts
// once and then serves reads out of the pages a compaction dropped will pay for
// every one of them again.
//
// So Options.ResidentAdvice is off unless asked for, and the advice given is the
// engine's own knowledge of what it is about to do rather than a knob per stage.

import (
	"errors"
	"fmt"

	"github.com/aoiflux/graphene/store"
)

// mappingAdvice is what the engine knows about how a mapping is about to be
// used, in the form the platform halves translate.
type mappingAdvice uint8

const (
	// adviceNormal is the kernel's default readahead. Not used by the engine --
	// it is what a mapping already has -- and present so a platform half can
	// name it and a test can round-trip the mapping.
	adviceNormal mappingAdvice = iota

	// adviceRandom is a point-lookup workload: no readahead, because the next
	// page read is not the next page. This is what an image mapping carries for
	// its whole life after the parse that created it.
	adviceRandom

	// adviceSequential is one forward pass over the whole thing: the parse at
	// open, and a compaction's read of the old image. Readahead is wanted here
	// and the pages are freed behind the reader as it goes.
	adviceSequential

	// adviceDontNeed says the resident pages of this range may be dropped. The
	// bytes remain readable and the next read of one faults it back.
	adviceDontNeed
)

// String names the advice for a metric and for an error.
func (a mappingAdvice) String() string {
	switch a {
	case adviceNormal:
		return "normal"
	case adviceRandom:
		return "random"
	case adviceSequential:
		return "sequential"
	case adviceDontNeed:
		return "dont-need"
	}
	return fmt.Sprintf("mappingAdvice(%d)", uint8(a))
}

// errAdviceUnsupported is what a platform with no post-mapping advice reports.
// Not a failure: nothing was attempted. See advise_other.go.
var errAdviceUnsupported = errors.New("this platform gives no advice about a live mapping")

// advise applies a to the whole of this mapping.
//
// A released mapping is a no-op rather than an error, because the sweep that
// released it and a compaction reaching this point are ordinary concurrent
// events and neither is wrong.
func (m *mapping) advise(a mappingAdvice) error {
	if m == nil || m.unmapped.Load() || len(m.data) == 0 {
		return nil
	}
	return adviseRange(m.data, a)
}

// adviseImages applies an access-pattern hint to every image mapping the store
// owns.
//
// It takes the read lock itself, and the lock is doing real work rather than
// guarding the slice header. sweepImages and closeImages release mappings and
// both require s.mu exclusively, so holding it shared here is what makes
// mapping.unmapped's check meaningful: without it the flag could be set between
// the test and the syscall, and the advice would name a region the process had
// already given back. A read lock is also the right shape for it -- every caller
// is a compaction boundary, and nothing about advising a mapping needs to
// exclude a reader.
//
// Both lists, because both are mappings of an image file and both carry resident
// pages a caller is charged for -- the distinction mappedBytes draws between
// them is about which section is read out of which, not about what the machine
// is paying. See Store.mappedBytes. Access-pattern advice is safe to apply to
// both because it changes only how the kernel reads ahead; dropping pages is
// not, which is why dropImagePages is a separate function.
//
// Errors are reported and not returned. Advice is a hint: a compaction that
// could not tell the kernel what it was about to do has still compacted, and
// failing the operation over a hint would turn an optimisation into an outage.
// The reporting is what keeps it from being silent.
func (s *Store) adviseImages(a mappingAdvice) {
	if !s.residentAdvice {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.images {
		s.noteAdvice(m.advise(a), a, m.bytes())
	}
	for _, m := range s.indexImages {
		s.noteAdvice(m.advise(a), a, m.bytes())
	}
}

// dropImagePages returns the resident pages of the record image, after a pass
// that read all of them.
//
// # Why the record image and not the index mapping beside it
//
// The rule is "release what a pass is finished with", and only one of the two
// lists qualifies. s.images is the image a compaction has just read end to end
// in order to copy forward every record the delta did not touch; nothing asked
// for the whole of it to be resident, the compaction made it so, and a reader
// afterwards wants the handful of blobs it actually looks up.
//
// s.indexImages is the opposite case. It is the mapping a compaction made of the
// image it had just written so that SwapBase could install an index base out of
// it, and that base is what every query reads from the moment the commit lands.
// Dropping it would make the next lookup fault the index back in a page at a
// time -- paying the whole cost immediately, to reclaim something the store is
// about to ask for again. That is not the same trade; it is simply worse.
//
// So the pages dropped here are the ones a compaction made resident on its own
// account. A read that wants one back faults it from the file transparently, and
// nothing about the mapping, the addresses or the slices into it changes.
func (s *Store) dropImagePages() {
	if !s.residentAdvice {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.images {
		s.noteAdvice(m.advise(adviceDontNeed), adviceDontNeed, m.bytes())
	}
}

// noteAdvice reports advice that was asked for and not given.
//
// Unsupported is reported exactly like a failure, and deliberately: from the
// caller's side there is no difference worth drawing between "this platform
// cannot" and "this platform would not". Both mean the option is not in force,
// and both are things an operator who set it is entitled to find out without
// reading the source. It is the same argument MetricIndexFallback's own comment
// arrives at after having got it wrong once.
func (s *Store) noteAdvice(err error, a mappingAdvice, bytes int64) {
	if err == nil {
		return
	}
	s.record(store.Metric{
		Kind:  store.MetricResidentAdvice,
		Count: int64(a),
		Bytes: bytes,
		Err:   fmt.Errorf("advice %s over %d bytes: %w", a, bytes, err),
	})
}
