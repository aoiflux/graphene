package disk

import (
	"bytes"
	"sync"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// Advice about a mapping, and the two things that have to be true of it
// everywhere: it never changes an answer, and a store that asked for it and did
// not get it says so.
//
// The measurement half -- how much residency this actually returns -- is a linux
// figure and is not here. What is here is the part that can be asserted on any
// platform, including the one that gives no advice at all, because the contract
// is written for that one too.

// adviceSink collects only the advice metric.
type adviceSink struct {
	mu  sync.Mutex
	got []store.Metric
}

func (s *adviceSink) Record(m store.Metric) {
	if m.Kind != store.MetricResidentAdvice {
		return
	}
	s.mu.Lock()
	s.got = append(s.got, m)
	s.mu.Unlock()
}

func (s *adviceSink) snapshot() []store.Metric {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.Metric(nil), s.got...)
}

func TestMappingAdvice_NamesItself(t *testing.T) {
	for _, tc := range []struct {
		a    mappingAdvice
		want string
	}{
		{adviceNormal, "normal"},
		{adviceRandom, "random"},
		{adviceSequential, "sequential"},
		{adviceDontNeed, "dont-need"},
	} {
		if got := tc.a.String(); got != tc.want {
			t.Errorf("advice %d names itself %q, want %q", uint8(tc.a), got, tc.want)
		}
	}
	// An advice the platform halves do not translate must not be reported as one
	// they do, because the name goes into an error a caller reads.
	if got := mappingAdvice(200).String(); got == "normal" {
		t.Error("an unknown advice named itself as a real one")
	}
}

// A released mapping is not an error to advise. A sweep and a compaction reaching
// the advice are ordinary concurrent events and neither of them is wrong.
func TestMappingAdvice_ReleasedMappingIsQuiet(t *testing.T) {
	var m *mapping
	if err := m.advise(adviceRandom); err != nil {
		t.Errorf("advising a nil mapping: %v", err)
	}
	released := &mapping{data: make([]byte, 4096), release: func() error { return nil }}
	if err := released.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := released.advise(adviceDontNeed); err != nil {
		t.Errorf("advising a released mapping: %v", err)
	}
}

// The option asked for and not held. On a platform with no advice to give this
// must be reported rather than silently doing nothing -- the failure
// MetricImageFallback exists for, in the same shape.
func TestResidentAdvice_ReportsWhenThePlatformCannot(t *testing.T) {
	sink := &adviceSink{}
	s := budgetFixture(t, Options{Metrics: sink, ResidentAdvice: true}, 200, 0, 64)
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	got := sink.snapshot()
	if adviceSupported {
		for _, m := range got {
			t.Errorf("advice failed on a platform that supports it: %v", m.Err)
		}
		return
	}
	if len(got) == 0 {
		t.Fatal("a platform that gives no advice reported nothing when advice was asked for")
	}
	for _, m := range got {
		if m.Err == nil {
			t.Error("an advice metric with no error attached")
		}
		if m.Bytes <= 0 {
			t.Errorf("advice reported over %d bytes", m.Bytes)
		}
	}
}

// And a store that did not ask reports nothing, whichever platform this is.
func TestResidentAdvice_SilentWhenNotAsked(t *testing.T) {
	sink := &adviceSink{}
	s := budgetFixture(t, Options{Metrics: sink}, 200, 0, 64)
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := sink.snapshot(); len(got) != 0 {
		t.Errorf("a store that asked for no advice emitted %d advice metrics", len(got))
	}
}

// The contract claim, asserted rather than argued: advice drops pages and not
// mappings, so a record read before a compaction still reads correctly after one
// -- including the blobs that alias the image the compaction was just told it
// had finished with.
//
// On a platform with no advice this asserts that the option changes nothing,
// which is the other half of the same promise.
func TestResidentAdvice_ChangesNoAnswerAcrossACompaction(t *testing.T) {
	const nodes, blob = 200, 64
	s := budgetFixture(t, Options{ResidentAdvice: true}, nodes, 0, blob)

	want := bytes.Repeat([]byte("p"), blob)

	// Read one record before the compaction, and keep the slice. If it aliases
	// the mapping, this is the slice the whole argument in disk/advise.go is
	// about.
	before, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode before compaction: %v", err)
	}
	held := before.Properties

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if !bytes.Equal(held, want) {
		t.Errorf("a Properties slice read before the compaction reads %q after it", held)
	}
	after, err := s.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode after compaction: %v", err)
	}
	if !bytes.Equal(after.Properties, want) {
		t.Errorf("Properties after compaction = %q, want %q", after.Properties, want)
	}

	// And every record, not just the one held: a dropped page that faults back
	// wrong would show up somewhere in the image rather than at record 1.
	for id := store.NodeID(1); id <= nodes; id++ {
		n, err := s.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		if !bytes.Equal(n.Properties, want) {
			t.Fatalf("node %d reads %q after a compaction with advice on", id, n.Properties)
		}
	}
}
