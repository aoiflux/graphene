package disk

import "testing"

// The arithmetic that turns a discovered ceiling into a budget, tested without a
// kernel. The platform readers answer what the kernel said; this decides what to
// do about it, and the split exists so this half is testable everywhere.

func TestBudgetFromCeiling_TakesLessOfAMachineThanOfALimit(t *testing.T) {
	const gib = 1 << 30

	enforced := budgetFromCeiling(sysCeiling{Bytes: 2 * gib, Enforced: true})
	machine := budgetFromCeiling(sysCeiling{Bytes: 2 * gib, Enforced: false})

	if enforced <= machine {
		t.Fatalf("an enforced ceiling must yield a larger budget than the same number of bytes of machine: enforced=%d machine=%d", enforced, machine)
	}
	if want := int64(2 * gib / 100 * sysCeilingFraction); enforced != want {
		t.Errorf("enforced budget = %d, want %d", enforced, want)
	}
	if want := int64(2 * gib / 100 * sysCeilingFractionMachine); machine != want {
		t.Errorf("machine budget = %d, want %d", machine, want)
	}
}

// A budget must never exceed the ceiling it was derived from. This is the
// property that matters more than either constant: whatever the fractions are
// tuned to later, deriving a budget larger than the limit would turn discovery
// into a way of guaranteeing the death it exists to prevent.
func TestBudgetFromCeiling_NeverExceedsTheCeiling(t *testing.T) {
	for _, c := range []sysCeiling{
		{Bytes: 1 << 20, Enforced: true},
		{Bytes: 2 << 30, Enforced: true},
		{Bytes: 64 << 30, Enforced: false},
		{Bytes: 1, Enforced: true},
	} {
		if got := budgetFromCeiling(c); got < 0 || uint64(got) > c.Bytes {
			t.Errorf("ceiling %d bytes (enforced=%v) derived a budget of %d", c.Bytes, c.Enforced, got)
		}
	}
}

// Unreadable and absurd figures both produce zero, which means "unset" rather
// than "no memory at all" -- the distinction sysmem_other.go exists to preserve.
func TestBudgetFromCeiling_UnusableFiguresYieldUnset(t *testing.T) {
	for _, c := range []sysCeiling{
		{},
		{Bytes: 0, Enforced: true},
		// Larger than any machine, and large enough that the multiply below
		// would be the interesting part rather than the answer. cgroup v1 writes
		// a figure in this range for "no limit".
		{Bytes: 1 << 63, Enforced: true},
	} {
		if got := budgetFromCeiling(c); got != 0 {
			t.Errorf("ceiling %+v derived %d, want 0", c, got)
		}
	}
}

// --- Options.DiscoverMemoryBudget ---

// A stated budget is used exactly as given. Discovery can never raise it and can
// never lower it: a caller who named a figure has already decided, and a second
// opinion from the environment would make the store's behaviour depend on where
// it happened to be running after the caller had said it should not.
func TestResolveMemoryBudget_AStatedBudgetWins(t *testing.T) {
	const stated = 123 << 20
	got, source := resolveMemoryBudget(Options{
		MemoryBudget:         stated,
		DiscoverMemoryBudget: true,
	})
	if got.MemoryBudget != stated {
		t.Errorf("a stated budget of %d became %d", stated, got.MemoryBudget)
	}
	if source != "" {
		t.Errorf("a stated budget was attributed to %q; it came from the caller", source)
	}
}

// Not asking for discovery leaves zero meaning what it has always meant.
// Redefining it would change the behaviour of every deployment that never set
// it, which is the silent reinterpretation of a shipped default this flag exists
// to avoid.
func TestResolveMemoryBudget_OffByDefault(t *testing.T) {
	got, source := resolveMemoryBudget(Options{})
	if got.MemoryBudget != 0 || source != "" {
		t.Errorf("discovery ran without being asked: budget=%d source=%q", got.MemoryBudget, source)
	}
}

// Asked for, the result is either a budget with a source naming the instrument,
// or nothing at all on a platform that cannot answer. What must never happen is
// one without the other -- a budget with no provenance is a refusal an operator
// cannot act on, and a source with no budget is a claim with nothing behind it.
func TestResolveMemoryBudget_BudgetAndSourceArriveTogether(t *testing.T) {
	got, source := resolveMemoryBudget(Options{DiscoverMemoryBudget: true})

	switch {
	case got.MemoryBudget == 0 && source == "":
		t.Logf("this platform discovers no ceiling, which is a valid answer")
	case got.MemoryBudget > 0 && source != "":
		t.Logf("discovered %d bytes from %s", got.MemoryBudget, source)
	default:
		t.Fatalf("budget and source disagree: budget=%d source=%q", got.MemoryBudget, source)
	}

	// And whatever was discovered must be below the ceiling it came from, which
	// is the property budgetFromCeiling exists to guarantee.
	if c, ok := readSysCeiling(); ok && got.MemoryBudget > 0 {
		if uint64(got.MemoryBudget) >= c.Bytes {
			t.Errorf("derived budget %d is not below the %d byte ceiling it came from",
				got.MemoryBudget, c.Bytes)
		}
	}
}

// End to end: a store opened with discovery reports both the figure and where it
// came from, and one opened without it reports neither. StorageStats is the only
// place a caller who did not construct the store can see which.
func TestStorageStats_ReportsTheDiscoveredBudgetAndItsSource(t *testing.T) {
	plain, err := OpenWithOptions(t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("open without discovery: %v", err)
	}
	defer plain.Close()
	if st := plain.StorageStats(); st.MemoryBudgetBytes != 0 || st.MemoryBudgetSource != "" {
		t.Errorf("a store that asked for no budget reports %d bytes from %q",
			st.MemoryBudgetBytes, st.MemoryBudgetSource)
	}

	found, err := OpenWithOptions(t.TempDir(), Options{DiscoverMemoryBudget: true})
	if err != nil {
		t.Fatalf("open with discovery: %v", err)
	}
	defer found.Close()

	st := found.StorageStats()
	if st.MemoryBudgetSource == "" {
		if st.MemoryBudgetBytes != 0 {
			t.Errorf("a budget of %d bytes with no source attached", st.MemoryBudgetBytes)
		}
		t.Skip("this platform discovers no ceiling; nothing further to assert")
	}
	if st.MemoryBudgetBytes <= 0 {
		t.Errorf("source %q reported with a budget of %d", st.MemoryBudgetSource, st.MemoryBudgetBytes)
	}
}

// A stated budget is reported with no source, so an operator can tell a figure
// the configuration carries from one the environment supplied.
func TestStorageStats_AStatedBudgetHasNoSource(t *testing.T) {
	s, err := OpenWithOptions(t.TempDir(), Options{
		MemoryBudget:         512 << 20,
		DiscoverMemoryBudget: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	st := s.StorageStats()
	if st.MemoryBudgetBytes != 512<<20 {
		t.Errorf("budget = %d, want %d", st.MemoryBudgetBytes, 512<<20)
	}
	if st.MemoryBudgetSource != "" {
		t.Errorf("a caller's own figure was attributed to %q", st.MemoryBudgetSource)
	}
}
