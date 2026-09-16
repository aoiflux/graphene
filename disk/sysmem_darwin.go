//go:build darwin

package disk

// The darwin half, and it is best-effort by necessity rather than by choice.
//
// # What is missing, and why it is not worked around
//
// Darwin supplies the peak cheaply through getrusage. The current resident size
// and the anonymous/file split both need Mach calls -- task_info with
// MACH_TASK_BASIC_INFO, or the phys_footprint counter -- that are not reachable
// from the standard library without cgo. This engine's zero-cgo constraint is
// worth more than a current reading on one platform, so this reports the peak,
// declares Current false, and lets every caller see that it did.
//
// # There is no ceiling here, and reporting one would be a lie
//
// No per-process memory ceiling on darwin constrains the class this engine cares
// about. RLIMIT_AS counts the mapped image's address space, which is the term
// the memory programme deliberately moved memory *into*, so a limit set against
// it would refuse a store that fits comfortably. RLIMIT_RSS is accepted and
// unenforced on every modern kernel, so a limit set against it binds nothing at
// all. tests/ceiling_other_test.go refuses to run the acceptance here for
// exactly this reason.
//
// What is left is hw.memsize: how much memory the machine has. That is not a
// limit on this process and sysCeiling.Enforced says so, but it is a real figure
// to take a conservative fraction of, which is more than nothing and is the
// whole of what "best effort" means on this platform.

import (
	"encoding/binary"
	"syscall"
)

func readSysMemory() (sysMemory, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return sysMemory{}, false
	}
	// Unlike linux, darwin reports ru_maxrss in bytes.
	return sysMemory{
		Peak: uint64(ru.Maxrss),
		// Both false, and both deliberately: Anon and Resident are left at zero
		// because there is no reading behind them, and Current says so rather
		// than letting a caller read that zero as "this process holds nothing".
		Split:   false,
		Current: false,
	}, true
}

// readSysCeiling reads hw.memsize, which is a 64-bit integer the standard
// library has no 64-bit accessor for.
//
// The stdlib syscall package on darwin offers exactly two: Sysctl, returning a
// string, and SysctlUint32, which cannot hold the size of any machine this
// engine is interesting on. SysctlUint64 and SysctlRaw live in
// golang.org/x/sys/unix, and this module has no dependencies and is not
// acquiring one for a single integer.
//
// So the string form is decoded here, and the one subtlety is documented rather
// than discovered later: Sysctl strips a single trailing NUL from the bytes the
// kernel returned. hw.memsize is little-endian, so the trailing bytes are the
// high-order ones and any that were stripped were zero -- padding back to eight
// with zeros reconstructs the value exactly. That is true for every machine
// whose memory size does not fill all 64 bits, which is every machine.
func readSysCeiling() (sysCeiling, bool) {
	val, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return sysCeiling{}, false
	}
	raw := []byte(val)
	if len(raw) == 0 || len(raw) > 8 {
		return sysCeiling{}, false
	}
	var buf [8]byte
	copy(buf[:], raw)
	n := binary.LittleEndian.Uint64(buf[:])
	if n == 0 {
		return sysCeiling{}, false
	}
	return sysCeiling{
		Bytes:  n,
		Source: "sysctl hw.memsize (machine size; darwin enforces no ceiling)",
		// Nothing enforces this against this process. It is the machine, most of
		// which belongs to something else.
		Enforced: false,
	}, true
}
