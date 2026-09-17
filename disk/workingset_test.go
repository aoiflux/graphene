package disk

import (
	"errors"
	"strings"
	"testing"
)

// The process working-set cap. Every assertion here is about the contract rather
// than about a figure, because the figure is whatever the machine running the
// test will grant -- and because the contract is the part that has to hold on
// the platforms that refuse.

func TestLimitWorkingSet_RefusesANonPositiveFigure(t *testing.T) {
	for _, n := range []int64{0, -1, -(1 << 30)} {
		if _, err := LimitWorkingSet(n); err == nil {
			t.Errorf("LimitWorkingSet(%d) was accepted; a limit must be stated positively", n)
		}
	}
}

// The refusal must be legible on the platforms that have no such mechanism, and
// it must be the sentinel rather than a bare string, because a caller deciding
// whether to fall back has to be able to test for it.
func TestLimitWorkingSet_UnsupportedIsASentinel(t *testing.T) {
	// A hard cap left installed would trim every test that runs after this one
	// in the same binary, and an unrelated failure would then look like a memory
	// bug. Restoring is not tidiness; it is what keeps the rest of the package
	// measuring itself rather than this test.
	t.Cleanup(func() { _ = restoreWorkingSetDefault() })

	got, err := LimitWorkingSet(256 << 20)
	if err == nil {
		// Supported here. The cap applied; assert what it promised rather than
		// what it was asked for.
		if got.MaxBytes == 0 {
			t.Error("a cap reported as applied with a zero maximum")
		}
		if !got.Hard {
			t.Errorf("a soft limit was installed where a hard one was asked for: %s", got)
		}
		if got.MinBytes >= got.MaxBytes {
			t.Errorf("minimum %d is not below maximum %d", got.MinBytes, got.MaxBytes)
		}
		t.Logf("applied: %s", got)
		return
	}
	if !errors.Is(err, ErrWorkingSetUnsupported) {
		t.Fatalf("a platform with no cap must refuse with ErrWorkingSetUnsupported, got %v", err)
	}
	if got != (WorkingSetLimit{}) {
		t.Errorf("a refused cap reported figures: %s", got)
	}
}

// Discovery names its instrument, the way a discovered budget does. A figure the
// caller did not choose has to be explicable.
func TestLimitWorkingSetFromCeiling_NamesItsSourceOrRefuses(t *testing.T) {
	t.Cleanup(func() { _ = restoreWorkingSetDefault() })

	got, err := LimitWorkingSetFromCeiling()
	if err != nil {
		if errors.Is(err, ErrWorkingSetUnsupported) {
			t.Skip("this platform has no working-set cap, which is a valid answer")
		}
		if _, ok := readSysCeiling(); !ok {
			t.Skip("this platform discovers no ceiling, which is a valid answer")
		}
		t.Fatalf("a ceiling was discoverable and the cap still failed: %v", err)
	}
	if got.Source == "" {
		t.Error("a discovered cap named no instrument")
	}
	if got.MaxBytes == 0 {
		t.Error("a discovered cap reported a zero maximum")
	}

	// And it must be strictly below the ceiling it came from, which is the
	// property budgetFromCeiling exists to guarantee and the reason the whole
	// figure is never taken.
	if c, ok := readSysCeiling(); ok && got.MaxBytes >= c.Bytes {
		t.Errorf("cap %d is not below the %d byte ceiling it came from", got.MaxBytes, c.Bytes)
	}
	t.Logf("discovered: %s", got)
}

func TestWorkingSetLimit_StringSaysWhetherItBinds(t *testing.T) {
	soft := WorkingSetLimit{MinBytes: 1, MaxBytes: 2}.String()
	if !strings.Contains(soft, "soft") {
		t.Errorf("a soft limit does not say so: %q", soft)
	}
	hard := WorkingSetLimit{MinBytes: 1, MaxBytes: 2, Hard: true, Source: "a ceiling"}.String()
	if !strings.Contains(hard, "hard") || !strings.Contains(hard, "a ceiling") {
		t.Errorf("a hard discovered limit does not say so: %q", hard)
	}
}
