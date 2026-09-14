//go:build stress && linux

package graphene_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Linux cannot apply its own ceiling from inside the process.
//
// The instrument a RAM budget means on linux is a cgroup v2 memory.max, and
// moving a process into a cgroup that carries one needs write access to the
// hierarchy -- a privilege a test binary does not have and should not ask for.
// setrlimit is available without privilege and is the wrong instrument: RLIMIT_AS
// bounds the virtual address space, which counts every byte of the mapped image
// even though none of it is charged to the process's own memory, and RLIMIT_DATA
// does not see mmap at all. A run under either would be measuring the wrong
// thing while looking exactly like a run under the right one.
//
// So the harness reads the ceiling rather than setting it, and refuses to
// produce a result when there is none. See ceilingHowTo for what the caller must
// do instead.
const ceilingSelfApplies = false

const ceilingHowTo = `Run the compiled test binary inside a cgroup that carries a memory limit:

    go test ./tests/ -tags=stress -c -o /tmp/graphene.test
    sudo systemd-run --scope --uid=$(id -u) --gid=$(id -g) \
        -p MemoryMax=2G -p MemorySwapMax=0 \
        env GRAPHENE_CEILING_MIB=2048 GRAPHENE_RSS_DIR=/var/tmp/graphene-fixture \
        /tmp/graphene.test -test.run TestCeiling -test.v

MemorySwapMax=0 matters as much as MemoryMax: with swap available the cgroup
pushes anonymous pages out instead of failing, and the run then measures how
patient the disk is rather than whether the store fits.`

func ceilingApply(uint64) error {
	return errors.New("a process cannot place itself under a cgroup memory limit without privileges")
}

// ceilingRead reports the tightest memory.max in force over this process's
// cgroup and all of its ancestors.
//
// The whole chain, not just the leaf: a limit set on a parent constrains this
// process exactly as much as one set on its own cgroup, and reading only the
// leaf would report "no ceiling" for the common case of a limit applied to the
// scope a level up. The level that produced the figure is named in Source, and
// the peak and event counters are read from that same level, because those are
// the counters the limit is enforced against.
func ceilingRead() (ceilingInfo, error) {
	mount, err := cgroup2Mount()
	if err != nil {
		return ceilingInfo{}, err
	}

	dir := filepath.Join(mount, cgroup2RelPath())
	if _, err := os.Stat(dir); err != nil {
		// A cgroup namespace makes /proc/self/cgroup report a path relative to a
		// root this process cannot see. The mount point is then this process's
		// own cgroup, which is the only level it can read anyway.
		dir = mount
	}

	best, bestDir := tightestMemoryMax(mount, dir)
	if bestDir == "" {
		return ceilingInfo{}, fmt.Errorf(
			"memory.max is unset or \"max\" at every level from %s up to %s", dir, mount)
	}

	info := ceilingInfo{
		Bytes:  best,
		Source: filepath.Join(bestDir, "memory.max"),
	}
	if v, err := os.ReadFile(filepath.Join(bestDir, "memory.swap.max")); err == nil {
		info.SwapMax = strings.TrimSpace(string(v))
	}
	if v, ok := readCgroupBytes(filepath.Join(bestDir, "memory.peak")); ok {
		info.Peak = v
	}
	if ev, err := os.ReadFile(filepath.Join(bestDir, "memory.events")); err == nil {
		info.Reclaims, _ = cgroupEvent(ev, "max")
		info.OOMs, _ = cgroupEvent(ev, "oom")
		var have bool
		info.OOMKills, have = cgroupEvent(ev, "oom_kill")
		info.Counters = have
	}
	return info, nil
}

// cgroup2Mount finds where the unified hierarchy is mounted. It is read from
// mountinfo rather than assumed to be /sys/fs/cgroup so that a container or a
// hybrid host, where the v2 hierarchy hangs off /sys/fs/cgroup/unified, reports
// the real path instead of a missing file.
func cgroup2Mount() (string, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	return cgroup2MountFrom(data)
}

// tightestMemoryMax walks from dir up to mount and returns the smallest
// memory.max it finds, with the directory it came from. An empty directory
// means no level carried a limit.
//
// The whole chain rather than the leaf, because a limit on an ancestor binds
// this process exactly as hard as one on its own cgroup, and reading only the
// leaf would report "no ceiling" for the ordinary case of a limit applied to
// the scope one level up -- which is precisely how systemd-run --scope applies
// one.
func tightestMemoryMax(mount, dir string) (uint64, string) {
	best := ^uint64(0)
	bestDir := ""
	for d := dir; ; {
		if v, ok := readCgroupBytes(filepath.Join(d, "memory.max")); ok && v < best {
			best, bestDir = v, d
		}
		if d == mount {
			break
		}
		parent := filepath.Dir(d)
		if parent == d || len(parent) < len(mount) {
			break
		}
		d = parent
	}
	return best, bestDir
}

// cgroup2MountFrom is the parsing half of cgroup2Mount.
func cgroup2MountFrom(data []byte) (string, error) {
	for _, line := range strings.Split(string(data), "\n") {
		// The optional-fields section ends at a lone "-", and the filesystem type
		// is the field after it. Splitting on that separator is the only way to
		// read mountinfo without guessing how many optional fields are present.
		sep := strings.Index(line, " - ")
		if sep < 0 {
			continue
		}
		rest := strings.Fields(line[sep+3:])
		if len(rest) == 0 || rest[0] != "cgroup2" {
			continue
		}
		if head := strings.Fields(line[:sep]); len(head) >= 5 {
			return head[4], nil
		}
	}
	return "", errors.New("no cgroup2 filesystem is mounted; this harness does not read cgroup v1")
}

// cgroup2RelPath returns the v2 path from /proc/self/cgroup, which is the line
// with hierarchy id 0 and no controller name. An empty result means the root.
func cgroup2RelPath() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if after, ok := strings.CutPrefix(line, "0::"); ok {
			return strings.TrimSpace(after)
		}
	}
	return ""
}

// readCgroupBytes reads one of the single-value memory files. "max" is not a
// number and not an error: it is the documented spelling of "no limit", and
// reporting it as not-ok is what lets the caller distinguish an unlimited level
// from an unreadable one.
func readCgroupBytes(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	field := strings.TrimSpace(string(data))
	if field == "max" {
		return 0, false
	}
	n, err := strconv.ParseUint(field, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// cgroupEvent reads one counter out of the key/value memory.events file.
func cgroupEvent(data []byte, key string) (uint64, bool) {
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		name, value, ok := bytes.Cut(bytes.TrimSpace(line), []byte{' '})
		if !ok || string(name) != key {
			continue
		}
		n, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// ceilingProbe is unused on linux: the ceiling comes from outside this process,
// so there is no self-applied limit to check and the read-back is already
// independent. It exists so the acceptance test compiles from one source on
// every platform.
func ceilingProbe(uint64) error {
	return errors.New("no commit probe on linux; the ceiling here is read, not applied")
}

// The parsers below are the half of this file that no ceiling run exercises
// directly: a run either finds a limit or refuses, and both outcomes look the
// same whether the reader understood these files or misread them. These check
// the understanding against the shapes the kernel actually writes.

func TestTightestMemoryMax_AParentLimitCounts(t *testing.T) {
	mount := t.TempDir()
	leaf := filepath.Join(mount, "user.slice", "run-42.scope")
	if err := os.MkdirAll(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(dir, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte(value+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// The limit one level up and the leaf unlimited: the shape systemd-run
	// --scope produces when the test binary ends up in a cgroup of its own
	// underneath the scope carrying the limit.
	write(mount, "max")
	write(filepath.Join(mount, "user.slice"), "2147483648")
	write(leaf, "max")

	got, dir := tightestMemoryMax(mount, leaf)
	if dir == "" {
		t.Fatal("a limit on the parent was read as no limit at all")
	}
	if got != 2147483648 {
		t.Errorf("ceiling = %d, want 2147483648 (from %s)", got, dir)
	}
}

func TestTightestMemoryMax_TheTightestWins(t *testing.T) {
	mount := t.TempDir()
	leaf := filepath.Join(mount, "a", "b")
	if err := os.MkdirAll(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	for dir, value := range map[string]string{
		mount:                          "8589934592",
		filepath.Join(mount, "a"):      "2147483648",
		filepath.Join(mount, "a", "b"): "4294967296",
	} {
		if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte(value), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if got, _ := tightestMemoryMax(mount, leaf); got != 2147483648 {
		t.Errorf("ceiling = %d, want the tightest of the three, 2147483648", got)
	}
}

func TestTightestMemoryMax_AllUnlimitedIsNoCeiling(t *testing.T) {
	mount := t.TempDir()
	leaf := filepath.Join(mount, "a")
	if err := os.MkdirAll(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{mount, leaf} {
		if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// "max" is the kernel's spelling of no limit, and reading it as a number
	// would hand the acceptance test a ceiling of zero bytes that every peak is
	// comfortably under.
	if _, dir := tightestMemoryMax(mount, leaf); dir != "" {
		t.Errorf("an unlimited chain reported a ceiling from %s", dir)
	}
}

func TestCgroup2MountFrom_FindsTheUnifiedHierarchy(t *testing.T) {
	// A v1/v2 hybrid, which is the case that makes assuming /sys/fs/cgroup
	// wrong: several cgroup mounts, only one of them cgroup2, and not the first.
	const mountinfo = `25 30 0:22 / /sys rw,nosuid shared:7 - sysfs sysfs rw
31 25 0:26 / /sys/fs/cgroup ro,nosuid shared:9 - tmpfs tmpfs ro,mode=755
32 31 0:27 / /sys/fs/cgroup/systemd rw,nosuid shared:10 - cgroup cgroup rw,name=systemd
33 31 0:28 / /sys/fs/cgroup/unified rw,nosuid shared:11 - cgroup2 cgroup2 rw,nsdelegate
34 31 0:29 / /sys/fs/cgroup/memory rw,nosuid shared:12 - cgroup cgroup rw,memory
`
	got, err := cgroup2MountFrom([]byte(mountinfo))
	if err != nil {
		t.Fatal(err)
	}
	if got != "/sys/fs/cgroup/unified" {
		t.Errorf("mount = %q, want /sys/fs/cgroup/unified", got)
	}
}

func TestCgroup2MountFrom_NoUnifiedHierarchyIsAnError(t *testing.T) {
	const v1Only = `32 31 0:27 / /sys/fs/cgroup/systemd rw - cgroup cgroup rw,name=systemd
34 31 0:29 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory
`
	if got, err := cgroup2MountFrom([]byte(v1Only)); err == nil {
		t.Errorf("a cgroup v1 host reported a v2 mount at %q", got)
	}
}

func TestCgroupEvent_ReadsTheCountersTheKernelWrites(t *testing.T) {
	const events = "low 0\nhigh 0\nmax 17\noom 2\noom_kill 1\n"
	for _, c := range []struct {
		key  string
		want uint64
	}{{"max", 17}, {"oom", 2}, {"oom_kill", 1}, {"low", 0}} {
		got, ok := cgroupEvent([]byte(events), c.key)
		if !ok {
			t.Errorf("%s: not found", c.key)
			continue
		}
		if got != c.want {
			t.Errorf("%s = %d, want %d", c.key, got, c.want)
		}
	}
	// "oom" must not match "oom_kill": a prefix match would report kills as
	// allocation failures, and the two mean different things.
	if got, _ := cgroupEvent([]byte("oom_kill 9\n"), "oom"); got != 0 {
		t.Errorf("oom matched oom_kill and read %d", got)
	}
}
