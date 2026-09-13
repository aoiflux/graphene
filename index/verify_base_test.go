package index

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Tests for VerifyBase — the entry point, not the check.
//
// What an implementation's Verify establishes is tested where that
// implementation lives, over a real encoded section (disk/verify_gpix_test.go).
// What is tested here is the four things this package decides: that no base means
// nothing to do rather than a failure, that a base is actually asked, that a
// fault already recorded is reported in preference to whatever a fresh pass would
// reach first, and that cancellation is honoured.
//
// The fake base has a real structural check of its own, which is what makes the
// second of those testable without a file — see fakeBase.Verify.

func TestVerifyBase_HasNothingToDoWithoutABase(t *testing.T) {
	p := NewPropertyIndex()
	p.IndexNode(1, "k", []byte("v"))
	if err := p.VerifyBase(); err != nil {
		t.Fatalf("VerifyBase with no base returned %v", err)
	}
}

func TestVerifyBase_AcceptsASoundBase(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(probeCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	if err := p.VerifyBase(); err != nil {
		t.Fatalf("a sound base did not verify: %v", err)
	}
}

// The base is asked, rather than assumed sound because it is a base.
//
// The damage is applied after the attach, which the interface forbids of anything
// but a test: what matters is that VerifyBase reaches the implementation's own
// check and returns what it says, not how the base came to be wrong.
func TestVerifyBase_ReportsWhatTheBaseSaysAboutItself(t *testing.T) {
	b := newFakeBase(probeCorpus())
	p := NewPropertyIndex()
	if err := p.AttachBase(b); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	if err := p.VerifyBase(); err != nil {
		t.Fatalf("the corpus should verify before it is damaged: %v", err)
	}

	// One entity listed under a value whose postings do not hold it — the
	// asymmetry between the two directions that a delete cascade depends on.
	s := &b.sides[NodeKind]
	key := s.keys[0]
	s.entriesOf[^uint64(0)] = []PropEntry{{Key: key, Value: []byte(s.values[key][0])}}

	err := p.VerifyBase()
	if err == nil {
		t.Fatal("VerifyBase accepted a base whose two directions disagree")
	}
	if !strings.Contains(err.Error(), "does not hold it") {
		t.Fatalf("VerifyBase reported %q, which is not what the base said", err)
	}
}

// A fault already read out of the base is the diagnosis, and it is reported in
// preference to a fresh pass.
//
// Constructed so the two answers differ: a read is made to fail, which records
// the fault, and then the base is made sound again. A VerifyBase that only ran
// the structural pass would now return nil and report a store with known damage
// as healthy.
func TestVerifyBase_ReportsARecordedFaultBeforeItChecksAnything(t *testing.T) {
	corpus := probeCorpus()
	b := newFakeBase(corpus)
	b.failKey = corpus[0].key
	b.failErr = errors.New("a run in this key would not decode")

	p := NewPropertyIndex()
	if err := p.AttachBase(b); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	// A read that touches the failing key, which is what records the fault. The
	// result is discarded: what it answered is another test's subject.
	_ = p.NodesByProperty(corpus[0].key, corpus[0].value)
	if p.BaseFault() == nil {
		t.Fatal("reading a failing key recorded no fault; this test cannot say anything")
	}

	b.failKey, b.failErr = "", nil
	if err := p.VerifyBase(); !errors.Is(err, p.BaseFault()) {
		t.Fatalf("VerifyBase returned %v rather than the recorded fault %v", err, p.BaseFault())
	}
}

func TestVerifyBase_StopsWhenCancelled(t *testing.T) {
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(probeCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.VerifyBaseCtx(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled VerifyBase returned %v", err)
	}
}

// Verify and VerifyBase are separate, and the separation is what stops an
// O(index) pass from being paid by every caller that wanted the delta checked.
//
// Asserted through the fake's read counter: Verify must not walk the base.
func TestVerify_DoesNotWalkTheBase(t *testing.T) {
	b := newFakeBase(probeCorpus())
	p := NewPropertyIndex()
	if err := p.AttachBase(b); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	p.IndexNode(9_000_001, "k", []byte("v"))
	b.resetCounts()
	if err := p.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got := b.valueWalks(); got != 0 {
		t.Fatalf("Verify walked the base %d times; that pass belongs to VerifyBase", got)
	}
}
