//go:build linux

package disk

import (
	"os"
	"path/filepath"
	"testing"
)

// /proc/self/status parsed against fixtures rather than against whatever the
// machine running the tests happens to hold. A test that asserts on the real
// file can only check that the numbers are plausible, which is not a check.

func TestParseProcStatus_SplitsAnonFromFile(t *testing.T) {
	raw := []byte(`Name:	graphene.test
VmPeak:	  2097152 kB
VmHWM:	   524288 kB
VmRSS:	   262144 kB
RssAnon:	   131072 kB
RssFile:	   129024 kB
RssShmem:	     2048 kB
Threads:	12
`)
	m := parseProcStatus(raw)

	if !m.Split {
		t.Fatal("a status carrying RssAnon and RssFile must report Split")
	}
	if !m.Current {
		t.Error("a status carrying the Rss fields is a reading of now")
	}
	if want := uint64(131072) << 10; m.Anon != want {
		t.Errorf("Anon = %d, want %d", m.Anon, want)
	}
	// Anon plus file, and deliberately not VmRSS: VmRSS includes RssShmem, and a
	// Resident that disagreed with Anon+File by the shmem term would make the
	// file-backed share a caller computes by subtraction wrong by that much.
	if want := uint64(131072+129024) << 10; m.Resident != want {
		t.Errorf("Resident = %d, want %d", m.Resident, want)
	}
	if want := uint64(524288) << 10; m.Peak != want {
		t.Errorf("Peak = %d, want %d", m.Peak, want)
	}
}

// Kernels before 4.5 publish VmRSS and not the split. Both fields must then
// carry the total, so a caller comparing Anon against a budget over-reports
// rather than under-reports, and Split says not to trust their difference.
func TestParseProcStatus_PreSplitKernelReportsTheTotalInBoth(t *testing.T) {
	raw := []byte("VmHWM:\t   524288 kB\nVmRSS:\t   262144 kB\n")
	m := parseProcStatus(raw)

	if m.Split {
		t.Fatal("a status with no RssAnon must not claim a split")
	}
	if !m.Current {
		t.Error("VmRSS is still a reading of now")
	}
	want := uint64(262144) << 10
	if m.Anon != want || m.Resident != want {
		t.Errorf("Anon=%d Resident=%d, want both %d", m.Anon, m.Resident, want)
	}
}

// A status with no size fields at all must say Current is false rather than
// report a process holding nothing.
func TestParseProcStatus_NoFiguresIsNotZeroBytes(t *testing.T) {
	m := parseProcStatus([]byte("Name:\tgraphene.test\nThreads:\t12\n"))
	if m.Current {
		t.Error("a status carrying no Rss figures must not claim to be a current reading")
	}
}

// cgroup v2 writes the literal "max" for no limit. Reading it as a ceiling would
// report a limit of zero -- or, worse, be believed.
func TestReadCgroupV2_UnlimitedFallsThrough(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	restore := cgroupRoot
	cgroupRoot = dir
	t.Cleanup(func() { cgroupRoot = restore })

	if c, ok := readCgroupV2(); ok {
		t.Fatalf(`a cgroup reporting "max" was read as a %d byte ceiling (%s)`, c.Bytes, c.Source)
	}
}

func TestReadCgroupV2_ReadsALimit(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("2147483648\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	restore := cgroupRoot
	cgroupRoot = dir
	t.Cleanup(func() { cgroupRoot = restore })

	c, ok := readCgroupV2()
	if !ok {
		t.Fatal("a cgroup carrying a byte limit was not read")
	}
	if c.Bytes != 2<<30 {
		t.Errorf("ceiling = %d, want %d", c.Bytes, uint64(2)<<30)
	}
	if !c.Enforced {
		t.Error("a cgroup limit is enforced; exceeding it is a kill")
	}
}

// cgroup v1 writes PAGE_COUNTER_MAX for no limit, which parses perfectly well as
// a number and is emphatically not a ceiling.
func TestReadCgroupV1_UnlimitedFallsThrough(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "memory"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "memory", "memory.limit_in_bytes")
	if err := os.WriteFile(path, []byte("9223372036854771712\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	restore := cgroupRoot
	cgroupRoot = dir
	t.Cleanup(func() { cgroupRoot = restore })

	if c, ok := readCgroupV1(); ok {
		t.Fatalf("an unlimited v1 cgroup was read as a %d byte ceiling", c.Bytes)
	}
}

// MemTotal is a real figure and is not a limit on this process. Conflating the
// two is how a library ends up helping itself to a whole machine.
func TestReadSysCeiling_MachineSizeIsNotEnforced(t *testing.T) {
	dir := t.TempDir()
	meminfo := filepath.Join(dir, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal:       32000000 kB\nMemFree:  1000 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	restoreRoot, restoreInfo := cgroupRoot, procMeminfo
	cgroupRoot = filepath.Join(dir, "no-such-cgroup")
	procMeminfo = meminfo
	t.Cleanup(func() { cgroupRoot, procMeminfo = restoreRoot, restoreInfo })

	c, ok := readSysCeiling()
	if !ok {
		t.Fatal("MemTotal was readable and was not read")
	}
	if c.Enforced {
		t.Error("machine size must not be reported as an enforced ceiling")
	}
	if want := uint64(32000000) << 10; c.Bytes != want {
		t.Errorf("ceiling = %d, want %d", c.Bytes, want)
	}
}
