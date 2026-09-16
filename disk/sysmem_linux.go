//go:build linux

package disk

// The linux half: /proc/self/status for what we hold, cgroup for what we are
// allowed.
//
// Both are files rather than syscalls, which is why there is no cgo here and no
// hand-resolved procedure the way the windows half needs.

import (
	"bytes"
	"os"
	"strconv"
)

// procSelfStatus is read rather than a constant so a test can point this at a
// fixture. Nothing else writes it.
var (
	procSelfStatus = "/proc/self/status"
	procMeminfo    = "/proc/meminfo"
	cgroupRoot     = "/sys/fs/cgroup"
)

// readSysMemory parses RssAnon, RssFile and VmHWM out of /proc/self/status.
//
// RssAnon and RssFile rather than VmRSS, because the split is the whole point:
// VmRSS is their sum and cannot distinguish the class that OOM-kills from the
// class the kernel reclaims. Kernels before 4.5 do not publish the split; on one
// of those this reports the total in both fields and says Split is false, which
// is the same contract the darwin and windows halves use.
func readSysMemory() (sysMemory, bool) {
	raw, err := os.ReadFile(procSelfStatus)
	if err != nil {
		return sysMemory{}, false
	}
	return parseProcStatus(raw), true
}

// parseProcStatus is the parsing half, split out so it is testable against a
// fixture rather than against whatever the machine running the tests happens to
// hold.
func parseProcStatus(raw []byte) sysMemory {
	var m sysMemory
	m.Current = true

	var anon, file, rss uint64
	var haveAnon, haveFile, haveRSS bool

	for _, line := range bytes.Split(raw, []byte("\n")) {
		key, val, ok := bytes.Cut(line, []byte(":"))
		if !ok {
			continue
		}
		switch string(key) {
		case "RssAnon":
			anon, haveAnon = parseStatusKB(val)
		case "RssFile":
			file, haveFile = parseStatusKB(val)
		case "VmRSS":
			rss, haveRSS = parseStatusKB(val)
		case "VmHWM":
			m.Peak, _ = parseStatusKB(val)
		}
	}

	switch {
	case haveAnon && haveFile:
		m.Anon = anon
		m.Resident = anon + file
		m.Split = true
	case haveRSS:
		// Pre-4.5, or a kernel that publishes the total and not the parts. Both
		// fields carry the total: a caller comparing Anon against a budget then
		// over-reports rather than under-reports, which is the safe direction,
		// and Split says not to trust the difference between them.
		m.Anon = rss
		m.Resident = rss
	default:
		m.Current = false
	}
	return m
}

// parseStatusKB reads the "   12345 kB" form every size field in
// /proc/self/status uses.
func parseStatusKB(val []byte) (uint64, bool) {
	f := bytes.Fields(val)
	if len(f) == 0 {
		return 0, false
	}
	n, err := strconv.ParseUint(string(f[0]), 10, 64)
	if err != nil {
		return 0, false
	}
	// Every size field in this file is kB, including the ones that say so and
	// the ones that do not.
	return n << 10, true
}

// readSysCeiling looks for a cgroup limit and falls back to the machine's size.
//
// The order is v2, then v1, then MemTotal, and the first two are what a
// container actually runs under. /sys/fs/cgroup/memory.max is tried directly
// before resolving the path out of /proc/self/cgroup, because in a container the
// cgroup namespace is rooted at the mount and the direct read is both correct
// and the common case; the resolved path is for a process in a nested cgroup on
// the host, where the direct read would report the root's limit -- usually
// "max", which is a wrong answer that looks like a right one.
func readSysCeiling() (sysCeiling, bool) {
	if c, ok := readCgroupV2(); ok {
		return c, true
	}
	if c, ok := readCgroupV1(); ok {
		return c, true
	}
	if n, ok := readMemTotal(); ok {
		return sysCeiling{
			Bytes:  n,
			Source: "/proc/meminfo MemTotal",
			// Machine size. Nothing enforces it against this process, and most
			// of it belongs to something else.
			Enforced: false,
		}, true
	}
	return sysCeiling{}, false
}

func readCgroupV2() (sysCeiling, bool) {
	for _, p := range cgroupV2Paths() {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		field := string(bytes.TrimSpace(raw))
		// "max" is the literal cgroup v2 writes for no limit. Reading it as a
		// number fails, which is the correct outcome, but it is worth naming:
		// an unlimited cgroup is not a ceiling and must fall through to the next
		// source rather than being reported as one.
		if field == "max" {
			continue
		}
		n, err := strconv.ParseUint(field, 10, 64)
		if err != nil || n == 0 {
			continue
		}
		return sysCeiling{Bytes: n, Source: "cgroup v2 memory.max", Enforced: true}, true
	}
	return sysCeiling{}, false
}

// cgroupV2Paths is the direct read followed by the one resolved out of
// /proc/self/cgroup. See readSysCeiling for why both.
func cgroupV2Paths() []string {
	paths := []string{cgroupRoot + "/memory.max"}
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return paths
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		// The v2 line is "0::<path>"; v1 lines carry a controller list in the
		// middle and are handled by readCgroupV1.
		rest, ok := bytes.CutPrefix(line, []byte("0::"))
		if !ok {
			continue
		}
		rel := string(bytes.TrimSpace(rest))
		if rel == "" || rel == "/" {
			continue
		}
		paths = append(paths, cgroupRoot+rel+"/memory.max")
	}
	return paths
}

// cgroupV1Unlimited is what v1 writes for "no limit": PAGE_COUNTER_MAX pages,
// which lands a hair under 2^63 and is emphatically not a ceiling.
const cgroupV1Unlimited = 1 << 62

func readCgroupV1() (sysCeiling, bool) {
	raw, err := os.ReadFile(cgroupRoot + "/memory/memory.limit_in_bytes")
	if err != nil {
		return sysCeiling{}, false
	}
	n, err := strconv.ParseUint(string(bytes.TrimSpace(raw)), 10, 64)
	if err != nil || n == 0 || n >= cgroupV1Unlimited {
		return sysCeiling{}, false
	}
	return sysCeiling{Bytes: n, Source: "cgroup v1 memory.limit_in_bytes", Enforced: true}, true
}

func readMemTotal() (uint64, bool) {
	raw, err := os.ReadFile(procMeminfo)
	if err != nil {
		return 0, false
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		key, val, ok := bytes.Cut(line, []byte(":"))
		if ok && string(key) == "MemTotal" {
			return parseStatusKB(val)
		}
	}
	return 0, false
}
