//go:build stress

package graphene_test

// What routing an import through the one-pass loader is worth.
//
// ceiling_ingest_test.go measures a bulk load against a bounded incremental
// ingest and reports amplification 6.53x against 1.00x. That measurement is of
// the engine's two write paths, reached directly. This one is of the two *import*
// paths, reached the way a consumer reaches them -- through a dump -- because
// between those two results sits everything the import does that a load does
// not: decoding the stream, remapping identifiers, and holding whatever it needs
// to do the remapping.
//
// The last of those is the term worth watching. An incremental import holds a
// map[NodeID]NodeID of every node it has seen; the one-pass path holds a sorted
// slice of eight bytes a node. Both grow with the graph, and the doc claims a
// ratio between them, so the peak is reported here rather than reasoned about.
//
//	GRAPHENE_IMPORT_NODES=100000 GRAPHENE_IMPORT_BLOB=3200 \
//	  go test ./tests/ -tags=stress -count=1 -run TestImportOnePass_AgainstIncremental -v
//
// # Budget
//
// The incremental arm rewrites the whole image once per bound, so the arm's cost
// is quadratic in the node count while the one-pass arm's is linear. At 100,000
// records of 3.2 KB that is roughly 2.4 GiB against 0.37. Raise the count only
// with that in hand.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/bulk"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

var (
	importNodes = envInt("GRAPHENE_IMPORT_NODES", 0)
	importBlob  = envInt("GRAPHENE_IMPORT_BLOB", 3200)
	importBound = envInt("GRAPHENE_IMPORT_BOUND_MIB", 32)
)

// importArm is what one import path cost.
type importArm struct {
	name     string
	written  int64
	peakAnon uint64
	wall     time.Duration
	sum      bulk.Summary
}

func (a importArm) String() string {
	return fmt.Sprintf("%-11s wrote %7.2f GiB, peak anon %7.1f MiB, %8.2fs, %d nodes / %d entries",
		a.name, float64(a.written)/(1<<30), float64(a.peakAnon)/(1<<20),
		a.wall.Seconds(), a.sum.Nodes, a.sum.NodeProperties)
}

// importFixtureDump builds a source store and exports it, returning the path.
//
// Four index entries a record, which is the audited consumer's declaration shape
// and what MEMORY_MODEL's multiplier formula is written against. The source is
// built with a bulk load, because building it incrementally would cost more than
// both arms being measured put together.
func importFixtureDump(t *testing.T, dir string, nodes, blob int) string {
	t.Helper()
	src := filepath.Join(dir, "source")
	g, err := graphene.Open(src)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	if err := g.DeclareOrderedProperty("seq"); err != nil {
		t.Fatalf("declare: %v", err)
	}
	payload := make([]byte, blob)
	for i := range payload {
		payload[i] = byte('a' + i%23)
	}
	loaded, err := g.BulkLoad(
		func(w *graphene.BulkNodeWriter) error {
			for i := 1; i <= nodes; i++ {
				if _, err := w.AddNode(graphene.BulkNode{
					Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
					Properties: payload,
					Index: []graphene.BulkProperty{
						{Key: "seq", Value: []byte(fmt.Sprintf("%010d", i))},
						{Key: "sha256", Value: []byte(fmt.Sprintf("sha-%09d", i))},
						{Key: "tool", Value: []byte(fmt.Sprintf("tool-%03d", i%97))},
						{Key: "host", Value: []byte(fmt.Sprintf("host-%03d", i%53))},
					},
				}); err != nil {
					return err
				}
			}
			return nil
		},
		func(w *graphene.BulkEdgeWriter) error { return nil })
	if err != nil {
		t.Fatalf("build source: %v", err)
	}

	path := filepath.Join(dir, "graph.dump")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create dump: %v", err)
	}
	if _, err := bulk.ExportDump(f, loaded, bulk.Options{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close dump: %v", err)
	}
	if err := loaded.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	if fi, err := os.Stat(path); err == nil {
		t.Logf("dump is %.2f GiB over %d records", float64(fi.Size())/(1<<30), nodes)
	}
	// The source store is not needed again and is the largest thing on disk.
	if err := os.RemoveAll(src); err != nil {
		t.Fatalf("remove source: %v", err)
	}
	return path
}

// TestImportOnePass_AgainstIncremental imports one dump both ways.
//
// Not interleaved, and that is deliberate rather than an oversight: the two
// figures that matter here are counters -- bytes written, and a peak -- and
// docs/benchmarks.md's interleaving rule is about wall clock. The wall clock is
// reported and should be read as an order of magnitude, not a measurement.
func TestImportOnePass_AgainstIncremental(t *testing.T) {
	if importNodes <= 0 {
		t.Skip("set GRAPHENE_IMPORT_NODES to the record count to measure")
	}
	if !rssSupported {
		t.Fatal("no RSS instrument on this platform; half of this measurement has nothing to report")
	}

	root := t.TempDir()
	dump := importFixtureDump(t, root, importNodes, importBlob)

	arms := []importArm{
		runImportArm(t, root, dump, "incremental", false),
		runImportArm(t, root, dump, "one-pass", true),
	}
	for _, a := range arms {
		t.Log(a.String())
	}

	inc, one := arms[0], arms[1]
	if one.sum.Nodes != inc.sum.Nodes || one.sum.NodeProperties != inc.sum.NodeProperties {
		t.Fatalf("the two paths imported different graphs:\n %s\n %s", inc, one)
	}
	if inc.written > 0 {
		t.Logf("writes: one-pass is %.2fx the incremental arm (%.2f GiB against %.2f)",
			float64(one.written)/float64(inc.written),
			float64(one.written)/(1<<30), float64(inc.written)/(1<<30))
	}
	if one.peakAnon > 0 {
		t.Logf("peak anonymous: one-pass is %.2fx the incremental arm (%.1f MiB against %.1f)",
			float64(one.peakAnon)/float64(inc.peakAnon),
			float64(one.peakAnon)/(1<<20), float64(inc.peakAnon)/(1<<20))
	}
}

// runImportArm imports the dump into a fresh directory and reports the cost.
func runImportArm(t *testing.T, root, dump, name string, onePass bool) importArm {
	t.Helper()
	into := filepath.Join(root, name)

	counter := &ingestCounter{}
	g, err := graphene.OpenWithOptions(into, disk.Options{Metrics: counter})
	if err != nil {
		t.Fatalf("%s: open: %v", name, err)
	}

	runtime.GC()
	sampler := startOpenPeak(500 * time.Microsecond)
	started := time.Now()

	arm := importArm{name: name}
	if onePass {
		loaded, sum, lerr := g.ImportDumpBulk(func() (io.ReadCloser, error) { return os.Open(dump) }, graphene.BulkImportOptions{})
		if lerr != nil {
			sampler.stop()
			t.Fatalf("%s: %v", name, lerr)
		}
		arm.sum, g = sum, loaded
	} else {
		f, ferr := os.Open(dump)
		if ferr != nil {
			sampler.stop()
			t.Fatalf("%s: open dump: %v", name, ferr)
		}
		policy := store.DefaultCompactionPolicy()
		policy.MaxDeltaBytes = int64(importBound) << 20
		sum, ierr := bulk.ImportDump(f, g.GraphStore, bulk.Options{
			MaxBatchBytes: 8 << 20,
			Compact:       policy,
			Reopen: func(bulk.Dest) (bulk.Dest, error) {
				next, rerr := g.CompactAndReopen()
				if rerr != nil {
					return nil, rerr
				}
				g = next
				return g.GraphStore, nil
			},
		})
		_ = f.Close()
		if ierr != nil {
			sampler.stop()
			t.Fatalf("%s: %v", name, ierr)
		}
		arm.sum = sum
		// The arm the user guide recommends ends with a compaction, so the store
		// is comparable to the one a load leaves behind.
		if cerr := g.Compact(); cerr != nil {
			sampler.stop()
			t.Fatalf("%s: final compaction: %v", name, cerr)
		}
	}

	arm.wall = time.Since(started)
	arm.peakAnon = sampler.stop().Anon
	w := counter.snapshot()
	arm.written = w.Image + w.Log
	if err := g.Close(); err != nil {
		t.Fatalf("%s: close: %v", name, err)
	}
	if err := os.RemoveAll(into); err != nil {
		t.Fatalf("%s: clean: %v", name, err)
	}
	return arm
}
