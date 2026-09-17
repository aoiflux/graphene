package disk

import (
	"testing"

	"github.com/aoiflux/graphene/store"
)

// What the operating system says the process holds, and the rules that keep an
// unanswerable figure from reading as zero.
//
// The translation half is tested against fabricated readings, because the whole
// point of the contract is the platforms this machine is not. The end-to-end
// half asserts only what is true everywhere: off unless asked, and self-
// consistent when the platform answers.

func TestProcessMemoryFrom_SubtractsOnlyWhereTheClassesWereToldApart(t *testing.T) {
	split := processMemoryFrom(sysMemory{
		Anon: 100, Resident: 350, Peak: 400,
		Split: true, Current: true, Source: "fabricated split",
	})
	if !split.Known() {
		t.Fatal("a reading with a source is not Known")
	}
	if split.AnonBytes != 100 || split.FileBytes != 250 {
		t.Errorf("split reading: anon=%d file=%d, want 100 and 250", split.AnonBytes, split.FileBytes)
	}

	// The case this rule exists for. A platform that could not separate the two
	// classes puts the whole total in both fields, so the subtraction that is
	// right above would yield zero here -- and zero file-backed bytes is a
	// statement about the store, not about the instrument. It must not be made.
	unsplit := processMemoryFrom(sysMemory{
		Anon: 350, Resident: 350, Peak: 400,
		Split: false, Current: true, Source: "fabricated, no split",
	})
	if unsplit.FileBytes != 0 {
		t.Errorf("an unsplit reading reported %d file-backed bytes", unsplit.FileBytes)
	}
	if unsplit.Split {
		t.Error("an unsplit reading reports Split")
	}
	if unsplit.AnonBytes != 350 {
		t.Errorf("an unsplit reading must carry the whole total in the class that OOM-kills, got %d", unsplit.AnonBytes)
	}
	// And a caller must be able to tell that zero apart from a measured zero,
	// which is the whole of what Split is for.
	if !unsplit.Known() {
		t.Error("an unsplit reading is still a reading")
	}
}

// Darwin's shape: a peak and nothing else. The zeros beside it must be legible
// as "not measured" rather than as "holds nothing".
func TestProcessMemoryFrom_PeakOnlyIsKnownButNotCurrent(t *testing.T) {
	p := processMemoryFrom(sysMemory{
		Peak: 4096, Split: false, Current: false, Source: "fabricated peak-only",
	})
	if !p.Known() {
		t.Fatal("a peak-only reading is still a reading")
	}
	if p.Current {
		t.Error("a peak-only reading claims a current figure")
	}
	if p.PeakBytes != 4096 {
		t.Errorf("peak = %d, want 4096", p.PeakBytes)
	}
	if p.AnonBytes != 0 || p.FileBytes != 0 {
		t.Errorf("a peak-only reading invented current figures: anon=%d file=%d", p.AnonBytes, p.FileBytes)
	}
}

// A reading with no instrument behind it is not a reading, however many bytes it
// carries. Nothing in the tree produces one; the rule is asserted because Known
// is what every caller branches on.
func TestProcessMemoryFrom_NoSourceIsNoReading(t *testing.T) {
	p := processMemoryFrom(sysMemory{Anon: 1 << 20, Resident: 1 << 21, Peak: 1 << 21, Split: true, Current: true})
	if p.Known() {
		t.Fatalf("a sourceless reading reported as Known: %+v", p)
	}
	if p != (store.ProcessMemory{}) {
		t.Errorf("a sourceless reading carried figures: %+v", p)
	}
}

func TestStorageStats_ProcessMemoryIsOffUnlessAsked(t *testing.T) {
	plain, err := OpenWithOptions(t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer plain.Close()

	got := plain.StorageStats().Process
	if got.Known() {
		t.Errorf("a store that did not ask for process memory reported %+v", got)
	}
	if got.AnonBytes != 0 || got.FileBytes != 0 || got.PeakBytes != 0 {
		t.Errorf("a store that did not ask reported figures: %+v", got)
	}
}

func TestStorageStats_ProcessMemoryWhenAsked(t *testing.T) {
	s, err := OpenWithOptions(t.TempDir(), Options{ReportProcessMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	got := s.StorageStats().Process
	if !got.Known() {
		if _, ok := readSysMemory(); ok {
			t.Fatal("the platform reports process memory but the store did not pass it on")
		}
		t.Skip("this platform reports no process memory, which is a valid answer")
	}

	// Every platform that answers at all answers the peak, and a process that
	// has just opened a store has a non-zero one.
	if got.PeakBytes == 0 {
		t.Errorf("source %q reported a zero peak", got.Source)
	}
	if got.Current && got.AnonBytes == 0 {
		t.Errorf("source %q claims a current reading of zero anonymous bytes", got.Source)
	}
	if !got.Split && got.FileBytes != 0 {
		t.Errorf("source %q reported %d file-backed bytes without a split", got.Source, got.FileBytes)
	}
	t.Logf("anon=%d file=%d peak=%d split=%v current=%v source=%q",
		got.AnonBytes, got.FileBytes, got.PeakBytes, got.Split, got.Current, got.Source)
}
