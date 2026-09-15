package graphene

import (
	"testing"

	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// The forwarded accessors answer on the backend that has the facts and decline
// on the one that does not.
//
// The declining half is the half worth asserting. Every one of these returns a
// zero value that reads like an answer -- false from ReadOnly is "this store
// accepts writes", false from LockEnforced is "nothing excludes a second
// process", a zero LockMode is "exclusive" -- and none of those is true of an
// in-memory graph, which has no posture, no lock and no directory to hold one
// over. The second return value is what separates "no" from "the question does
// not apply", and a caller who ignores it gets the wrong one.
func TestAccessors_DiskAnswersAndMemoryDeclines(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	m := NewInMemory()
	defer m.Close()

	// Each case reports only whether the backend claimed to have an answer, so
	// the table can cover six differently typed accessors at once.
	answered := map[string]func(g *Graph) bool{
		"EstimateResident": func(g *Graph) bool {
			_, ok := g.EstimateResident()
			return ok
		},
		"ReadOnly": func(g *Graph) bool {
			_, ok := g.ReadOnly()
			return ok
		},
		"RecoveredFromUncleanShutdown": func(g *Graph) bool {
			_, ok := g.RecoveredFromUncleanShutdown()
			return ok
		},
		"Declarations": func(g *Graph) bool {
			_, ok := g.Declarations()
			return ok
		},
		"DroppedDeclarations": func(g *Graph) bool {
			_, ok := g.DroppedDeclarations()
			return ok
		},
		"LockState": func(g *Graph) bool {
			_, _, ok := g.LockState()
			return ok
		},
	}

	for name, ask := range answered {
		if !ask(d) {
			t.Errorf("%s reports no answer on the disk backend, which has the fact", name)
		}
		if ask(m) {
			t.Errorf("%s claims an answer on the in-memory backend, which has nothing to "+
				"report and would be reporting a zero value as one", name)
		}
	}
}

// What each accessor forwards is what the backend says, not a second derivation
// of it.
//
// A façade method that recomputed an answer would be a place for the two to
// drift, which is the argument Forensics makes for not forwarding forty of
// these. This is that argument as an assertion, on the six that are forwarded.
func TestAccessors_ForwardTheBackendsOwnAnswer(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	s, ok := g.Forensics()
	if !ok {
		t.Fatal("Forensics: a disk-backed Graph has no disk store")
	}

	if ro, _ := g.ReadOnly(); ro != s.ReadOnly() {
		t.Errorf("ReadOnly = %v, store says %v", ro, s.ReadOnly())
	}
	if rec, _ := g.RecoveredFromUncleanShutdown(); rec != s.RecoveredFromUncleanShutdown() {
		t.Errorf("RecoveredFromUncleanShutdown = %v, store says %v",
			rec, s.RecoveredFromUncleanShutdown())
	}
	if mode, enforced, _ := g.LockState(); mode != s.LockMode() || enforced != s.LockEnforced() {
		t.Errorf("LockState = %v/%v, store says %v/%v",
			mode, enforced, s.LockMode(), s.LockEnforced())
	}

	// The estimate is taken twice and cannot be compared field by field -- At
	// differs, and a background compaction may move a term between the two
	// calls. What is asserted is that the forward reaches the real thing: an
	// empty store still has a WAL ring and a total, and the total agrees with the
	// one StorageStats reports through an entirely separate path.
	est, _ := g.EstimateResident()
	if est.Total <= 0 {
		t.Errorf("EstimateResident().Total = %d on an open store", est.Total)
	}
	if est.At.IsZero() {
		t.Error("EstimateResident().At is zero, so the estimate was never taken")
	}
	ss, _ := g.StorageStats()
	if ss.EstimatedResidentBytes != est.Total {
		t.Errorf("StorageStats says %d resident bytes and the breakdown totals %d; one of "+
			"the two paths is not reading the other", ss.EstimatedResidentBytes, est.Total)
	}
}

// Declarations reports what was declared, and DroppedDeclarations reports what
// an open declined to apply.
//
// The pair is the whole of what ConstraintDrop trades away, and neither half is
// meaningful without the other: a catalogue says what the store was asked to
// enforce, and only the second says whether it does.
func TestAccessors_DeclarationsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer g.Close()

	if err := g.DeclareUniqueProperty("sha256"); err != nil {
		t.Fatalf("DeclareUniqueProperty: %v", err)
	}
	if err := g.DeclareOrderedProperty("mtime"); err != nil {
		t.Fatalf("DeclareOrderedProperty: %v", err)
	}

	cat, ok := g.Declarations()
	if !ok {
		t.Fatal("Declarations reports nothing on a disk-backed graph")
	}
	if got := cat.UniqueNodeKeys; len(got) != 1 || got[0] != "sha256" {
		t.Errorf("UniqueNodeKeys = %q, want [sha256]", got)
	}
	if got := cat.OrderedNodeKeys; len(got) != 1 || got[0] != "mtime" {
		t.Errorf("OrderedNodeKeys = %q, want [mtime]", got)
	}

	// Nothing violated anything, so nothing was dropped -- and the accessor says
	// so with an answer rather than with a missing one.
	dropped, ok := g.DroppedDeclarations()
	if !ok {
		t.Fatal("DroppedDeclarations reports nothing on a disk-backed graph")
	}
	if len(dropped) != 0 {
		t.Errorf("DroppedDeclarations = %v on a store whose data satisfies every "+
			"declaration", dropped)
	}
}

// LockState reports the lock each open mode actually takes, including the one a
// handle could not report before v0.8.0.
//
// LockMode re-derived its answer from Options.ReadOnly, which LiveReader
// implies, so a live reader -- holding no lock at all -- was described as
// holding a shared one. That is not a near miss: a shared lock excludes every
// writer, and not excluding the writer is exactly what OpenLive gives up its
// fixed view to buy. See disk.LockMode.
//
// enforced is a property of the platform and not of the open, so it does not
// move across the three.
func TestLockState_ReportsTheModeEachOpenTakes(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := w.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	mode, enforced, ok := w.LockState()
	if !ok {
		t.Fatal("a writer reports no lock state")
	}
	if mode != disk.LockExclusive {
		t.Errorf("writer LockState = %v, want exclusive", mode)
	}
	if !enforced {
		t.Error("LockEnforced is false on a platform whose build tags say it locks")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	if mode, _, _ := r.LockState(); mode != disk.LockShared {
		t.Errorf("read-only LockState = %v, want shared", mode)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l, err := OpenLive(dir)
	if err != nil {
		t.Fatalf("OpenLive: %v", err)
	}
	defer l.Close()
	mode, enforced, _ = l.LockState()
	if mode != disk.LockNone {
		t.Errorf("live-reader LockState = %v, want none: a live reader holds no lock, and "+
			"reporting a shared one claims it excludes the writer it exists to follow", mode)
	}
	if !enforced {
		t.Error("enforced went false for a live reader: it reports whether the platform " +
			"locks at all, not whether this store took one")
	}
	if ro, _ := l.ReadOnly(); !ro {
		t.Error("a live reader does not report ReadOnly, though every mutator refuses")
	}
}
