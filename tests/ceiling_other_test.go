//go:build stress && !linux && !windows

package graphene_test

import "errors"

// Darwin and everything else. There is no portable per-process memory ceiling
// that constrains the class this program cares about: setrlimit's RLIMIT_AS
// counts the mapped image's address space, which is the term the whole memory
// programme moved *out* of the constrained class, and RLIMIT_RSS is unenforced
// on every modern kernel. A run here would report a comfortable pass under a
// limit that never bound anything.
//
// So the harness says so and the test refuses rather than skipping into a green
// tick: this is the programme's end-to-end acceptance, and "the platform could
// not check" must not read the same as "the platform checked".
const ceilingSelfApplies = false

const ceilingHowTo = `This platform has no memory-ceiling instrument in this harness.
Run the ceiling acceptance on linux (cgroup v2 memory.max) or windows (Job Object
JOB_OBJECT_LIMIT_JOB_MEMORY).`

func ceilingApply(uint64) error {
	return errors.New("no ceiling instrument on this platform")
}

func ceilingRead() (ceilingInfo, error) {
	return ceilingInfo{}, errors.New("no ceiling instrument on this platform")
}

func ceilingProbe(uint64) error {
	return errors.New("no commit probe on this platform")
}
