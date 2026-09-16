package disk

import (
	"errors"
	"sync"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// The three per-stage compaction metrics. A compaction's peak was known only as
// one figure for the whole operation, which is not something an engineer can act
// on; these say which stage moved it. See store.MetricCompactPin.

// stageSink collects the stage metrics in the order they were emitted.
type stageSink struct {
	mu   sync.Mutex
	kind []store.MetricKind
	got  []store.Metric
}

func (s *stageSink) Record(m store.Metric) {
	switch m.Kind {
	case store.MetricCompactPin, store.MetricCompactBuild, store.MetricCompactCommit, store.MetricCompaction:
	default:
		return
	}
	s.mu.Lock()
	s.kind = append(s.kind, m.Kind)
	s.got = append(s.got, m)
	s.mu.Unlock()
}

func (s *stageSink) reset() {
	s.mu.Lock()
	s.kind, s.got = nil, nil
	s.mu.Unlock()
}

func (s *stageSink) snapshot() ([]store.MetricKind, []store.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := append([]store.MetricKind(nil), s.kind...)
	m := append([]store.Metric(nil), s.got...)
	return k, m
}

func TestCompactStages_EmitInOrderAroundTheCompaction(t *testing.T) {
	sink := &stageSink{}
	s := budgetFixture(t, Options{Metrics: sink}, 200, 0, 64)
	defer s.Close()

	// The fixture compacts, so start counting from a clean slate.
	sink.reset()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	kinds, metrics := sink.snapshot()
	want := []store.MetricKind{
		store.MetricCompactPin,
		store.MetricCompactBuild,
		store.MetricCompactCommit,
		store.MetricCompaction,
	}
	if len(kinds) != len(want) {
		t.Fatalf("emitted %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("emitted %v, want %v", kinds, want)
		}
	}

	// The stages precede the whole they decompose, so a sink that aggregates as
	// it reads has the parts before it is told the total.
	for _, m := range metrics[:3] {
		if m.Err != nil {
			t.Errorf("%s reported %v on a compaction that succeeded", m.Kind, m.Err)
		}
		if m.Count <= 0 {
			t.Errorf("%s modelled %d resident bytes; a store holding 200 nodes holds something", m.Kind, m.Count)
		}
	}
}

// The peak never resets, so it must not fall from one stage to the next. This is
// the property that makes "which stage moved the high-water mark" answerable by
// comparing consecutive emissions, which is what these metrics are for.
func TestCompactStages_PeakIsMonotonic(t *testing.T) {
	sink := &stageSink{}
	s := budgetFixture(t, Options{Metrics: sink}, 200, 0, 64)
	defer s.Close()

	sink.reset()
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	_, metrics := sink.snapshot()
	var prev int64
	for _, m := range metrics {
		if m.Kind == store.MetricCompaction {
			continue
		}
		if m.Bytes == 0 {
			// The platform does not answer. Documented as "not measurable"
			// rather than "no memory held", and there is nothing to compare.
			t.Skipf("%s reports no process peak on this platform", m.Kind)
		}
		if m.Bytes < prev {
			t.Errorf("%s reports a peak of %d after a previous stage reported %d; "+
				"a high-water mark does not fall", m.Kind, m.Bytes, prev)
		}
		prev = m.Bytes
	}
}

// A refused compaction emits the pin and nothing else.
//
// Both halves matter. The pin ran, so its figures exist and are the ones an
// operator needs -- a refusal says the store is too large to compact within the
// budget, and the pin says how much of that is the delta it just copied. And no
// compaction happened, so there is no MetricCompaction and no build or commit
// stage: the stage gauges must not manufacture a compaction the store refused to
// perform.
func TestCompactStages_RefusedBudgetEmitsThePinAndNoMore(t *testing.T) {
	sink := &stageSink{}
	s := budgetFixture(t, Options{Metrics: sink}, 200, 0, 64)
	defer s.Close()

	sink.reset()
	s.memBudget = 1
	if err := s.Compact(); !errors.Is(err, ErrMemoryBudget) {
		t.Fatalf("Compact under a 1 byte budget: got %v, want ErrMemoryBudget", err)
	}

	kinds, _ := sink.snapshot()
	if len(kinds) != 1 || kinds[0] != store.MetricCompactPin {
		t.Fatalf("a refused compaction emitted %v, want exactly [compact-pin]", kinds)
	}
}

// Nothing is emitted when no sink is attached, and more to the point nothing is
// computed: recordCompactStage reads the process's memory counters and takes the
// store's read lock, and a store with no sink must run the code it ran before
// these existed.
func TestCompactStages_CostNothingWithNoSink(t *testing.T) {
	s := budgetFixture(t, Options{}, 200, 0, 64)
	defer s.Close()

	if s.metricsOn() {
		t.Fatal("a store built with no Metrics option reports a sink attached")
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact with no sink: %v", err)
	}
}
