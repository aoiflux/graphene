//go:build windows

package disk

import "testing"

// The two interpretation halves, tested without needing a process that
// reproduces the conditions they exist for.

// A process can be inside a job that constrains something else entirely -- a CI
// runner puts its steps in one, and so does a container runtime. Believing the
// memory field of such a job hands the engine a ceiling of zero, and a budget
// derived from zero is a store that refuses to open.
func TestCeilingFromJobLimits_RefusesAJobThatLimitsSomethingElse(t *testing.T) {
	var info jobObjectExtendedLimitInformation
	info.BasicLimitInformation.LimitFlags = 0x00002000 // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	info.JobMemoryLimit = 1 << 30
	info.ProcessMemoryLimit = 1 << 30

	if c, ok := ceilingFromJobLimits(info); ok {
		t.Fatalf("a job carrying no memory limit flag was read as a %d byte ceiling (%s)", c.Bytes, c.Source)
	}
}

func TestCeilingFromJobLimits_ReadsTheLimitItIsGiven(t *testing.T) {
	var info jobObjectExtendedLimitInformation
	info.BasicLimitInformation.LimitFlags = jobObjectLimitJobMemory
	info.JobMemoryLimit = 2048 << 20

	c, ok := ceilingFromJobLimits(info)
	if !ok {
		t.Fatal("a job carrying a memory limit was refused")
	}
	if c.Bytes != 2048<<20 {
		t.Errorf("ceiling = %d, want %d", c.Bytes, uint64(2048)<<20)
	}
	if !c.Enforced {
		t.Error("a job memory limit is enforced; exceeding it fails the commit")
	}
}

// When a job carries both, the per-process limit binds this process while the
// job-wide one is shared with siblings we cannot see. The tighter of the two is
// the only reading that is right in both directions.
func TestCeilingFromJobLimits_TakesTheTighterOfTheTwo(t *testing.T) {
	for _, tc := range []struct {
		name            string
		perProcess, job uintptr
		want            uintptr
	}{
		{"per-process is tighter", 512 << 20, 2048 << 20, 512 << 20},
		{"job-wide is tighter", 2048 << 20, 512 << 20, 512 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var info jobObjectExtendedLimitInformation
			info.BasicLimitInformation.LimitFlags = jobObjectLimitProcessMemory | jobObjectLimitJobMemory
			info.ProcessMemoryLimit = tc.perProcess
			info.JobMemoryLimit = tc.job

			c, ok := ceilingFromJobLimits(info)
			if !ok {
				t.Fatal("a job carrying both limits was refused")
			}
			if c.Bytes != uint64(tc.want) {
				t.Errorf("ceiling = %d, want %d", c.Bytes, tc.want)
			}
		})
	}
}

// Commit charge can exceed the working set when private memory has been paged
// out. A caller computing the file-backed share by subtraction must get zero
// rather than a number near 2^64.
func TestSysMemoryFromCounters_ClampsCommitToTheWorkingSet(t *testing.T) {
	m := sysMemoryFromCounters(processMemoryCountersEx{
		WorkingSetSize:     100 << 20,
		PrivateUsage:       400 << 20,
		PeakWorkingSetSize: 512 << 20,
	})
	if m.Anon != 100<<20 {
		t.Errorf("Anon = %d, want it clamped to the working set %d", m.Anon, uint64(100)<<20)
	}
	if m.Resident < m.Anon {
		t.Errorf("Resident %d below Anon %d would underflow a subtraction", m.Resident, m.Anon)
	}
	// A clamped reading has no split left in it: the subtraction yields zero, and
	// zero file-backed bytes would be a claim about the store rather than about
	// the instrument. Split=false is what stops a caller reading it as one.
	if m.Split {
		t.Error("a clamped reading still claims the classes were told apart")
	}
	if m.Source == "" {
		t.Error("a clamped reading names no instrument")
	}
}

// The clamp fires on equality too, and that is the common case rather than an
// edge: a process that has committed exactly what it holds resident has told us
// nothing about the file-backed share either.
func TestSysMemoryFromCounters_EqualCommitAndWorkingSetIsNotASplit(t *testing.T) {
	m := sysMemoryFromCounters(processMemoryCountersEx{
		WorkingSetSize:     8 << 20,
		PrivateUsage:       8 << 20,
		PeakWorkingSetSize: 8 << 20,
	})
	if m.Split {
		t.Error("commit equal to the working set leaves nothing to attribute to files")
	}
	if m.Anon != 8<<20 {
		t.Errorf("Anon = %d, want the whole resident total %d", m.Anon, uint64(8)<<20)
	}
}

func TestSysMemoryFromCounters_ReportsTheSplit(t *testing.T) {
	m := sysMemoryFromCounters(processMemoryCountersEx{
		WorkingSetSize:     900 << 20,
		PrivateUsage:       400 << 20,
		PeakWorkingSetSize: 1024 << 20,
	})
	if !m.Split || !m.Current {
		t.Fatalf("windows answers both halves when commit is below the working set: Split=%v Current=%v", m.Split, m.Current)
	}
	if m.Anon != 400<<20 || m.Resident != 900<<20 {
		t.Errorf("Anon=%d Resident=%d, want %d and %d", m.Anon, m.Resident, uint64(400)<<20, uint64(900)<<20)
	}
	if m.Peak != 1024<<20 {
		t.Errorf("Peak = %d, want %d", m.Peak, uint64(1024)<<20)
	}
}

// The reader against the real kernel. It cannot assert a figure -- the numbers
// are whatever this process holds -- but it can assert that the call worked and
// that the invariants the rest of the engine relies on hold on a live process,
// which a fixture cannot.
func TestReadSysMemory_AnswersOnThisMachine(t *testing.T) {
	m, ok := readSysMemory()
	if !ok {
		t.Fatal("K32GetProcessMemoryInfo is present on every supported windows and did not answer")
	}
	if m.Resident == 0 {
		t.Error("a running process holds a non-zero working set")
	}
	if m.Anon > m.Resident {
		t.Errorf("Anon %d exceeds Resident %d after the clamp", m.Anon, m.Resident)
	}
	// Whichever way this process happens to sit, the two must agree: a reading
	// that reports a split must have a subtraction left to make.
	if m.Split && m.Anon >= m.Resident {
		t.Errorf("Split reported with Anon %d not below Resident %d", m.Anon, m.Resident)
	}
	if m.Peak < m.Resident {
		t.Errorf("Peak %d below the current Resident %d", m.Peak, m.Resident)
	}
}
