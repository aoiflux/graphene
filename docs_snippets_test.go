package graphene

// The configuration docs/MEMORY_AND_PERFORMANCE.md recommends, compiled and run.
//
// A guide that names an option is a claim that the option exists and means what
// the surrounding paragraph says. Prose cannot be type-checked, so the usual
// failure is silent and slow: a field is renamed, every test still passes, and
// the document goes on recommending a spelling that stopped compiling two
// releases ago. Somebody finds out by pasting it.
//
// So the parts a reader would paste are here instead of only there. This is not
// a test of the engine -- everything it exercises is covered properly elsewhere
// -- it is a test of the document, and it fails for exactly one reason: the guide
// and the API have drifted apart.
//
// Keep it in step by hand when the guide changes. A test that generated the
// snippet, or a guide that generated the test, would be asserting that two
// copies of the same string match rather than that the API is still there.

import (
	"io"
	"os"
	"testing"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// Section 1, "Start here": the opening configuration opens a store.
func TestGuide_TheRecommendedOptionsOpen(t *testing.T) {
	g, err := OpenWithOptions(t.TempDir(), disk.Options{
		DiscoverMemoryBudget: true,
		MemoryBudget:         2 << 30,
		ImageMode:            disk.ImageMapped,
		IndexMode:            disk.IndexMapped,
		AutoCompact: &store.CompactionPolicy{
			MaxDeltaBytes:    32 << 20,
			MaxDeltaRecords:  200_000,
			MaxResidentBytes: 1 << 30,
		},
	})
	if err != nil {
		t.Fatalf("the configuration the guide opens with does not open: %v", err)
	}
	defer g.Close()

	// Section 8's table names these as the bounds a reader sets. An Options
	// literal naming all of them has to compile for the table to be actionable.
	bounded, err := OpenWithOptions(t.TempDir(), disk.Options{
		MaxBatchBytes:   8 << 20,
		MaxBatchRecords: 50_000,
		ResidentAdvice:  true,
		Adjacency:       disk.AdjacencyLazy,
		Compact: disk.CompactOptions{
			MaxWorkingBytes:   4 << 20,
			MaxIndexTailBytes: 16 << 20,
		},
	})
	if err != nil {
		t.Fatalf("the bounds named in the guide's table do not open together: %v", err)
	}
	if err := bounded.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Section 10, "Verifying it where you run": every field the guide tells a reader
// to watch is a field that exists.
func TestGuide_TheFieldsItTellsYouToWatchExist(t *testing.T) {
	g, err := OpenWithOptions(t.TempDir(), disk.Options{ReportProcessMemory: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer g.Close()

	st, ok := g.StorageStats()
	if !ok {
		t.Fatal("the disk backend reports no StorageStats")
	}
	_ = st.EstimatedResidentBytes
	_ = st.DeltaBytes
	_ = st.MemoryBudgetSource
	_ = st.ImageMode
	_ = st.IndexMode
	_ = st.Adjacency
	// Read before trusted, which is the point the guide makes about it: on
	// windows the split collapses whenever commit charge meets the working set.
	if st.Process.Split {
		_ = st.Process.AnonBytes
		_ = st.Process.FileBytes
	}

	if _, ok := g.EstimateResident(); !ok {
		t.Fatal("EstimateResident, which section 9 tells a reader to read rather than guess, reports nothing")
	}
}

// Section 4.5, "When you must ingest incrementally": the loop as written.
//
// The assertion worth making here is the one the section is about -- that the
// reopen hands back a handle the loop goes on using -- because a reader who
// keeps writing to the old one is the failure the section exists to prevent.
func TestGuide_TheIncrementalIngestLoopWorks(t *testing.T) {
	g, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	policy := store.DefaultCompactionPolicy()
	policy.MaxDeltaBytes = 32 << 20

	for range 3 {
		if _, err := g.AddNodesInBatches([]*store.Node{
			{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
			{Labels: []store.NodeType{store.NodeTypeMicroArtefact}},
		}); err != nil {
			t.Fatalf("AddNodesInBatches: %v", err)
		}
		st, ok := g.StorageStats()
		if !ok {
			t.Fatal("StorageStats")
		}
		if due, _ := policy.Evaluate(st); due {
			if g, err = g.CompactAndReopen(); err != nil {
				t.Fatalf("CompactAndReopen: %v", err)
			}
		}
	}
	defer g.Close()

	ids, err := g.QueryNodeIDs(store.NodeQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(ids) != 6 {
		t.Fatalf("the documented loop wrote %d nodes, want 6", len(ids))
	}
}

// Section 4.4, "Importing a dump": the call shape, and the refusal it documents.
//
// Run against something that is not a dump, because what this asserts is the
// signature and the promise attached to it -- that a refusal leaves the receiver
// usable, which is what makes the documented fallback to bulk.ImportDump
// reachable. bulkimport_test.go covers the refusal on a real dump.
func TestGuide_TheImportCallShapeAndItsRefusal(t *testing.T) {
	g, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer g.Close()

	_, _, err = g.ImportDumpBulk(func() (io.ReadCloser, error) {
		return os.Open(os.DevNull)
	}, BulkImportOptions{})
	if err == nil {
		t.Fatal("an empty file was accepted as a dump")
	}
	if _, err := g.AddNode(&store.Node{
		Labels: []store.NodeType{store.NodeTypeMicroArtefact},
	}); err != nil {
		t.Fatalf("the guide says the handle survives a refusal, and it did not: %v", err)
	}
}
