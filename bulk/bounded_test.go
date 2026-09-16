package bulk_test

// Bounding an import: batches in bytes, and a compaction schedule.
//
// An import with neither accumulates the whole dump in the delta and the index
// before anything folds it into an image, so its peak follows the size of the
// dump. These are the two controls over that, and what is pinned here is that
// each one does something the other cannot and that neither changes the result.

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/memory"
	"github.com/aoiflux/graphene/store"
)

// countingDest wraps a destination and records the size of every batch it was
// asked to commit, which is how a flush decision is observed from outside.
type countingDest struct {
	inner       storeUnderTest
	nodeBatch   []int
	edgeBatch   []int
	compactions int

	// sinceCompact is what StorageStats reports as the delta: records committed
	// and not yet folded away.
	sinceCompact int
}

func (c *countingDest) AddNode(n *store.Node) (store.NodeID, error) { return c.inner.AddNode(n) }
func (c *countingDest) AddEdge(e *store.Edge) (store.EdgeID, error) { return c.inner.AddEdge(e) }

func (c *countingDest) IndexNodeProperty(id store.NodeID, key string, value []byte) error {
	return c.inner.IndexNodeProperty(id, key, value)
}

func (c *countingDest) IndexEdgeProperty(id store.EdgeID, key string, value []byte) error {
	return c.inner.IndexEdgeProperty(id, key, value)
}

func (c *countingDest) AddNodesBatch(nodes []*store.Node) ([]store.NodeID, error) {
	c.nodeBatch = append(c.nodeBatch, len(nodes))
	c.sinceCompact += len(nodes)
	b := c.inner.(interface {
		AddNodesBatch([]*store.Node) ([]store.NodeID, error)
	})
	return b.AddNodesBatch(nodes)
}

func (c *countingDest) AddEdgesBatch(edges []*store.Edge) ([]store.EdgeID, error) {
	c.edgeBatch = append(c.edgeBatch, len(edges))
	c.sinceCompact += len(edges)
	b := c.inner.(interface {
		AddEdgesBatch([]*store.Edge) ([]store.EdgeID, error)
	})
	return b.AddEdgesBatch(edges)
}

// StorageStats and Compact make this a bulk compactor, so the schedule reaches
// it.
//
// The figures are this double's own rather than the wrapped backend's: the
// memory backend reports no storage statistics at all, so forwarding would hand
// the policy a zero and the schedule would be untestable against the one
// destination that keeps the test fast. What is under test here is the wiring --
// that the schedule is evaluated where a batch lands and that a compaction
// clears what it was evaluated on -- and this reports exactly that.
func (c *countingDest) StorageStats() store.StorageStats {
	return store.StorageStats{DeltaNodes: c.sinceCompact}
}

func (c *countingDest) Compact() error {
	c.compactions++
	c.sinceCompact = 0
	if x, ok := c.inner.(interface{ Compact() error }); ok {
		return x.Compact()
	}
	return nil
}

// fatDump builds a dump of n nodes each carrying size bytes of properties.
func fatDump(t *testing.T, n, size int) []byte {
	t.Helper()
	src := memory.New()
	for range n {
		if _, err := src.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: make([]byte, size),
		}); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	return buf.Bytes()
}

// The byte cap flushes where the record count cannot.
//
// This is the case BatchSize was always going to miss and the reason the second
// unit exists: a batch size chosen for small records is a memory event on large
// ones, and a dump is exactly where the caller does not know which they have.
func TestImport_ByteCapFlushesWhereTheRecordCountCannot(t *testing.T) {
	const records, size = 40, 4 << 10
	dump := fatDump(t, records, size)

	// Generous against the count, tight against the bytes: any flush here is a
	// flush on the bytes.
	c := &countingDest{inner: memory.New()}
	if _, err := bulk.ImportDump(bytes.NewReader(dump), c, bulk.Options{
		BatchSize: 10_000, MaxBatchBytes: 16 << 10,
	}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if len(c.nodeBatch) < 4 {
		t.Fatalf("%d nodes of %d bytes under a %d byte cap committed in %d batches (%v), want several",
			records, size, 16<<10, len(c.nodeBatch), c.nodeBatch)
	}
	for i, n := range c.nodeBatch {
		if n > 5 {
			t.Fatalf("batch %d held %d records of %d bytes, past the %d byte cap",
				i, n, size, 16<<10)
		}
	}

	// And the same dump under the record rule alone is one batch, which is what
	// makes the cap above the thing that fired.
	loose := &countingDest{inner: memory.New()}
	if _, err := bulk.ImportDump(bytes.NewReader(dump), loose, bulk.Options{
		BatchSize: 10_000,
	}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if len(loose.nodeBatch) != 1 {
		t.Fatalf("the record rule alone committed %d batches (%v), want 1",
			len(loose.nodeBatch), loose.nodeBatch)
	}
}

// Neither control changes what is imported. The whole value of a bound is that
// it is invisible in the result.
func TestImport_BoundsDoNotChangeTheResult(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	want := describe(t, src)

	var buf bytes.Buffer
	if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}

	for _, tc := range []struct {
		name string
		opts bulk.Options
	}{
		{"unbounded", bulk.Options{}},
		{"bytes", bulk.Options{MaxBatchBytes: 128}},
		{"bytes and records", bulk.Options{BatchSize: 2, MaxBatchBytes: 64}},
		{"a schedule that fires constantly", bulk.Options{
			BatchSize: 1, Compact: store.CompactionPolicy{MaxDeltaRecords: 1},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst, err := disk.OpenWithOptions(t.TempDir(), disk.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			if _, err := bulk.ImportDump(bytes.NewReader(buf.Bytes()), dst, tc.opts); err != nil {
				t.Fatalf("import: %v", err)
			}
			compare(t, want, describe(t, dst))
		})
	}
}

// The schedule fires during the import rather than after it, which is the only
// thing that bounds an import larger than memory.
func TestImport_ScheduleCompactsDuringTheImport(t *testing.T) {
	dump := fatDump(t, 40, 512)

	c := &countingDest{inner: memory.New()}
	if _, err := bulk.ImportDump(bytes.NewReader(dump), c, bulk.Options{
		BatchSize: 4,
		Compact:   store.CompactionPolicy{MaxDeltaRecords: 4},
	}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if c.compactions == 0 {
		t.Fatal("a schedule set to fire every four records compacted nothing during a 40 record import")
	}
	if c.compactions > len(c.nodeBatch) {
		t.Fatalf("%d compactions against %d batches: the schedule is running somewhere other than a flush",
			c.compactions, len(c.nodeBatch))
	}
}

// A zero policy is the behaviour an import had before the schedule existed, and
// it must stay exactly that: nothing evaluated and nothing compacted.
func TestImport_NoScheduleCompactsNothing(t *testing.T) {
	dump := fatDump(t, 40, 512)

	c := &countingDest{inner: memory.New()}
	if _, err := bulk.ImportDump(bytes.NewReader(dump), c, bulk.Options{BatchSize: 4}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if c.compactions != 0 {
		t.Fatalf("an import with no schedule compacted %d times", c.compactions)
	}
}

// The reopen hook receives the destination, and what it returns is what the
// rest of the dump is written to.
//
// Pinned against a real reopen rather than a stub that returns its argument,
// because the thing that could go wrong is writing the remainder to a handle
// the hook has closed.
func TestImport_ReopenHookTakesOverTheDestination(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	want := describe(t, src)

	var buf bytes.Buffer
	if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}

	dst, err := disk.OpenWithOptions(t.TempDir(), disk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	current := dst
	defer func() { current.Close() }()

	reopens := 0
	_, err = bulk.ImportDump(bytes.NewReader(buf.Bytes()), current, bulk.Options{
		BatchSize: 1,
		Compact:   store.CompactionPolicy{MaxDeltaRecords: 1},
		Reopen: func(d bulk.Dest) (bulk.Dest, error) {
			s, ok := d.(*disk.Store)
			if !ok {
				return nil, fmt.Errorf("hook was handed a %T, not the store it was given", d)
			}
			next, err := s.CompactAndReopen()
			if err != nil {
				return nil, err
			}
			reopens++
			current = next
			return next, nil
		},
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if reopens == 0 {
		t.Fatal("the hook was never called")
	}
	compare(t, want, describe(t, current))
}

// A hook that fails stops the import rather than carrying on against a handle
// whose state nobody knows.
func TestImport_AFailingReopenStopsTheImport(t *testing.T) {
	dump := fatDump(t, 40, 512)

	dst, err := disk.OpenWithOptions(t.TempDir(), disk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	boom := fmt.Errorf("no handle for you")
	_, err = bulk.ImportDump(bytes.NewReader(dump), dst, bulk.Options{
		BatchSize: 4,
		Compact:   store.CompactionPolicy{MaxDeltaRecords: 1},
		Reopen:    func(bulk.Dest) (bulk.Dest, error) { return nil, boom },
	})
	if err == nil {
		t.Fatal("a failing reopen hook did not stop the import")
	}
	if !bytes.Contains([]byte(err.Error()), []byte(boom.Error())) {
		t.Fatalf("error does not carry the one the hook returned: %v", err)
	}
}

// A hook that returns nothing is an error, not a silent continuation against
// the old handle: it has almost certainly closed it.
func TestImport_AReopenReturningNoDestinationIsRefused(t *testing.T) {
	dump := fatDump(t, 40, 512)

	dst, err := disk.OpenWithOptions(t.TempDir(), disk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	_, err = bulk.ImportDump(bytes.NewReader(dump), dst, bulk.Options{
		BatchSize: 4,
		Compact:   store.CompactionPolicy{MaxDeltaRecords: 1},
		Reopen:    func(bulk.Dest) (bulk.Dest, error) { return nil, nil },
	})
	if err == nil {
		t.Fatal("a hook returning no destination did not stop the import")
	}
}

// A destination that cannot compact is not an error. The schedule is inert,
// which is the position every optional fast-path in this package takes.
func TestImport_AScheduleAgainstABackendThatCannotCompactIsInert(t *testing.T) {
	src := memory.New()
	fixture(t, src)
	want := describe(t, src)

	var buf bytes.Buffer
	if _, err := bulk.ExportDump(&buf, src, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}

	dst := memory.New()
	if _, err := bulk.ImportDump(bytes.NewReader(buf.Bytes()), dst, bulk.Options{
		BatchSize: 1,
		Compact:   store.CompactionPolicy{MaxDeltaRecords: 1},
	}); err != nil {
		t.Fatalf("import: %v", err)
	}
	compare(t, want, describe(t, dst))
}
