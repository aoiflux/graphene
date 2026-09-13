package index

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// TestResidentBytes_ScalesWithEntries is the floor of the whole model: the figure
// has to move with the thing it measures.
//
// Trivial to state and the one mutation that would otherwise survive everything.
// A ResidentBytes that returned a constant — or zero — passes every band and slope
// check in the suite, because those compare two sizes of the *same* store and a
// constant error cancels in the difference exactly as a fixed overhead does.
func TestResidentBytes_ScalesWithEntries(t *testing.T) {
	p := NewPropertyIndex()
	empty := p.ResidentBytes()

	for i := 0; i < 1000; i++ {
		p.IndexNode(store.NodeID(i+1), "sha256", []byte(fmt.Sprintf("hash-%06d", i)))
	}
	full := p.ResidentBytes()

	if full <= empty {
		t.Fatalf("1000 entries report %d bytes against an empty index's %d", full, empty)
	}
	perEntry := (full - empty) / 1000
	// The postings term is the measured per-entry figure, so the marginal cost
	// should land on it almost exactly — the only other thing 1000 entries under
	// one key adds is that key's declaration, amortised to nothing.
	if perEntry < residentBytesPerEntry/2 || perEntry > residentBytesPerEntry*2 {
		t.Errorf("the marginal cost is %d B/entry, nowhere near the modelled %d",
			perEntry, residentBytesPerEntry)
	}
}

// TestResidentBytes_ChargesForDeclaredStructures is why the ordered and composite
// terms are separate rather than folded into the per-entry figure.
//
// An ordered index is a second copy of its key's entries in a different shape and a
// composite is a third structure over several keys, so a store that declares
// neither must not be charged for either — and a store that declares them must be.
// Without this nothing in the suite notices those two terms disappearing: the
// fixtures the band and slope checks use declare no ordered key and no composite,
// which is the ordinary case and therefore the one that hides them.
func TestResidentBytes_ChargesForDeclaredStructures(t *testing.T) {
	const n = 500

	fill := func(p *PropertyIndex) {
		for i := 0; i < n; i++ {
			id := store.NodeID(i + 1)
			p.IndexNode(id, "score", []byte(fmt.Sprintf("%08d", i)))
			p.IndexNode(id, "bucket", []byte(fmt.Sprintf("b%03d", i%100)))
		}
	}

	plain := NewPropertyIndex()
	fill(plain)

	ordered := NewPropertyIndex()
	ordered.DeclareOrderedNodeKey("score")
	fill(ordered)

	composite := NewPropertyIndex()
	if err := composite.DeclareCompositeNodeKeys([]string{"score", "bucket"}); err != nil {
		t.Fatalf("DeclareCompositeNodeKeys: %v", err)
	}
	fill(composite)

	base, withOrdered, withComposite := plain.ResidentBytes(), ordered.ResidentBytes(), composite.ResidentBytes()
	t.Logf("plain %d, ordered %d, composite %d", base, withOrdered, withComposite)

	if withOrdered <= base {
		t.Errorf("declaring an ordered key over %d entries costs nothing: %d against %d",
			n, withOrdered, base)
	}
	if withComposite <= base {
		t.Errorf("declaring a composite over %d entries costs nothing: %d against %d",
			n, withComposite, base)
	}
}

// TestResidentBytes_ABaseCostsItsDirectoryAndNotItsEntries is R3's claim, checked
// at the seam rather than through a store.
//
// The base is charged through an optional interface, so there are two ways for this
// to be wrong and they fail differently: an implementation that does not report
// falls back to a per-key floor, and one that reports a figure scaled by entries
// would make a mapped index cost what a resident one costs. A fake base is used
// precisely because it can be asked to lie about both.
func TestResidentBytes_ABaseCostsItsDirectoryAndNotItsEntries(t *testing.T) {
	small := &countingBase{keys: []string{"sha256"}, entries: 10}
	large := &countingBase{keys: []string{"sha256"}, entries: 10_000_000}

	if got, want := baseResidentBytes(small), baseResidentBytes(large); got != want {
		t.Errorf("a base with 10 entries is charged %d and one with ten million %d; "+
			"the charge is scaling with entries", got, want)
	}
	// And an implementation that reports for itself must be believed over the
	// fallback, or the optional interface is decoration.
	reporting := &reportingBase{countingBase: countingBase{keys: []string{"sha256"}}, bytes: 4242}
	if got := baseResidentBytes(reporting); got != 4242 {
		t.Errorf("a base that reports 4242 bytes is charged %d", got)
	}
}

// countingBase is a Base that answers only what baseResidentBytes asks and panics
// on anything else, so a change that starts reading entries to size the charge
// fails loudly rather than quietly costing a pass.
type countingBase struct {
	keys    []string
	entries int
}

func (b *countingBase) Keys(kind EntityKind) []string {
	if kind == NodeKind {
		return b.keys
	}
	return nil
}
func (b *countingBase) TotalEntries(kind EntityKind) int { return b.entries }
func (b *countingBase) KeyStats(EntityKind, string) (int, int) {
	panic("baseResidentBytes must not size its charge from a key's contents")
}
func (b *countingBase) Lookup(EntityKind, string, []byte) (IDRun, error) { panic("unused") }
func (b *countingBase) Cursor(EntityKind, string) ValueCursor            { panic("unused") }
func (b *countingBase) ForEachValue(EntityKind, string, []byte, func([]byte, IDRun) bool) error {
	panic("baseResidentBytes must not walk values")
}
func (b *countingBase) ForEachEntryOf(EntityKind, uint64, func(string, []byte) bool) error {
	panic("unused")
}
func (b *countingBase) HasEntries(EntityKind, uint64) bool { panic("unused") }
func (b *countingBase) ForEachID(EntityKind, func(uint64) bool) {
	panic("baseResidentBytes must not walk ids")
}
func (b *countingBase) MaxID(EntityKind) uint64         { return 0 }
func (b *countingBase) Verify(*store.CancelCheck) error { return nil }

// reportingBase also implements ResidentReporter.
type reportingBase struct {
	countingBase
	bytes int64
}

func (b *reportingBase) ResidentBytes() int64 { return b.bytes }
