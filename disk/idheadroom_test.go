package disk

// The identifier space as a reported quantity.
//
// Identifiers are never reused and compaction preserves them, so the one
// resource a store can exhaust without its record count moving at all is the
// space it hands them out of. These tests pin what is reported, what it is
// measured from, and when the store says so unprompted.

import (
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Headroom is computed from identifiers *issued*, not from the ones the image
// still holds. Deleting the record that held the highest identifier must not
// give headroom back, because the next write will not reuse it.
func TestStorageStats_IDHeadroom(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	var last store.NodeID
	for i := 0; i < 10; i++ {
		last = addNodeD(t, s, store.NodeTypeMicroArtefact)
	}

	st := s.StorageStats()
	if st.IDCeiling != maxCSREntityID {
		t.Fatalf("IDCeiling = %d, want %d", st.IDCeiling, uint64(maxCSREntityID))
	}
	if st.HighestNodeID != 10 {
		t.Fatalf("HighestNodeID = %d, want 10", st.HighestNodeID)
	}
	want := float64(maxCSREntityID-10) / float64(maxCSREntityID)
	if st.NodeIDHeadroom != want {
		t.Fatalf("NodeIDHeadroom = %v, want %v", st.NodeIDHeadroom, want)
	}
	if st.EdgeIDHeadroom != 1.0 {
		t.Fatalf("EdgeIDHeadroom = %v on a store with no edges, want 1", st.EdgeIDHeadroom)
	}

	// Delete the top of the space and compact, which is the operation that most
	// looks like it should recover identifiers.
	if err := s.DeleteNode(last); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := s.StorageStats()
	if after.HighestNodeID != st.HighestNodeID {
		t.Fatalf("deleting the highest identifier and compacting moved HighestNodeID from %d to %d",
			st.HighestNodeID, after.HighestNodeID)
	}
	if after.CSRNodes != 9 {
		t.Fatalf("CSRNodes = %d after deleting one of ten, want 9", after.CSRNodes)
	}

	// And it survives a reopen, because the marks are in the image header.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := reopened.StorageStats().HighestNodeID; got != st.HighestNodeID {
		t.Fatalf("HighestNodeID = %d after reopen, want %d", got, st.HighestNodeID)
	}
}

// countingSink collects metrics for the test below.
type countingSink struct {
	metrics []store.Metric
}

func (c *countingSink) Record(m store.Metric) { c.metrics = append(c.metrics, m) }

// A store near the end of its identifier space says so when it compacts: once
// per kind, with the figures, in the metrics and in the audit chain.
func TestIDHeadroom_WarnsOncePerKindAtCompaction(t *testing.T) {
	sink := &countingSink{}
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{
		Metrics: sink,
		Audit:   true,
		// Anything under 100% headroom warns, so three nodes is enough to cross
		// it without pretending to burn four billion identifiers.
		IDHeadroomWarn: 1.0,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	for i := 0; i < 3; i++ {
		addNodeD(t, s, store.NodeTypeMicroArtefact)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	warned := 0
	for _, m := range sink.metrics {
		if m.Kind != store.MetricIDHeadroomLow {
			continue
		}
		warned++
		if m.Count != 3 {
			t.Fatalf("metric Count = %d, want the highest identifier issued (3)", m.Count)
		}
		if m.Examined != int64(uint64(maxCSREntityID)) {
			t.Fatalf("metric Examined = %d, want the ceiling %d", m.Examined, uint64(maxCSREntityID))
		}
	}
	if warned != 1 {
		t.Fatalf("%d node-kind warnings after one compaction, want 1 (edges have issued none)", warned)
	}

	entries, err := s.AuditEntries()
	if err != nil {
		t.Fatalf("AuditEntries: %v", err)
	}
	audits := 0
	for _, e := range entries {
		if e.Kind != AuditIDHeadroomLow {
			continue
		}
		audits++
		if !strings.Contains(e.Detail, "node identifiers: 3 of") {
			t.Fatalf("audit detail does not name the figures: %q", e.Detail)
		}
	}
	if audits != 1 {
		t.Fatalf("%d audit entries, want 1", audits)
	}

	// A second compaction with the same figures says nothing more: the warning
	// is about the store's lifetime, and a store compacting on a timer would
	// otherwise fill its own chain with one observation.
	before := len(sink.metrics)
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for _, m := range sink.metrics[before:] {
		if m.Kind == store.MetricIDHeadroomLow {
			t.Fatal("the headroom warning fired twice for the same kind")
		}
	}
}

// A negative threshold disables the warning outright.
func TestIDHeadroom_NegativeThresholdDisables(t *testing.T) {
	sink := &countingSink{}
	s, err := OpenWithOptions(t.TempDir(), Options{Metrics: sink, IDHeadroomWarn: -1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	addNodeD(t, s, store.NodeTypeMicroArtefact)
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for _, m := range sink.metrics {
		if m.Kind == store.MetricIDHeadroomLow {
			t.Fatal("a negative IDHeadroomWarn still warned")
		}
	}
}
