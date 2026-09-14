//go:build stress

package graphene_test

// What each residency configuration actually holds.
//
// Three options decide where a store's bytes live: ImageMode (is the image
// mapped or copied into the heap), IndexMode (is the property index read out of
// the image or rebuilt in the heap) and Adjacency (are the four derived arrays
// built at Open or on demand). docs/MEMORY_MODEL.md describes each of them
// alone. This is the instrument for the table that describes them together.
//
// # One cell per process, named by the environment
//
// GRAPHENE_RSS_MODES selects the cell: "<image>/<index>/<adjacency>", with an
// optional fourth field "live" to open as a live reader rather than a read-only
// one. Image is mapped, heap or unlocked; index is mapped or resident;
// adjacency is eager or lazy.
//
// One per process rather than a sub-benchmark per cell, for the reason
// docs/TECHNICAL_DETAILS.md §14.13 gives: peakMiB is a process high-water mark,
// and a process that has already opened the store under another configuration
// reports that one's peak and that one's retained heap under this one's name.
//
// # Asked and held are different questions
//
// Every one of the three options is a request that the store may decline, and
// declining is not a defect: a heap image cannot carry a mapped index, because
// an index read out of a heap buffer would pin the whole file to save part of
// it; a v8 image carries no mapped index to read; and ImageMapped declines to
// map for a live reader, which by construction holds no lock against a writer.
//
// So this reports both. The three *Asked metrics are what the cell asked for
// and the three *Held metrics are what StorageStats says the store is holding.
// A row where they differ is the finding, not a failed measurement, and a table
// that reported only one of the two would present a fallback as a result.

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
)

// modeCell is one configuration of the three options.
type modeCell struct {
	image disk.ImageMode
	index disk.IndexMode
	adj   disk.AdjacencyMode
	live  bool
	name  string
}

// parseModeCell reads GRAPHENE_RSS_MODES. An unset variable takes the defaults,
// so the benchmark run with no environment measures the shipped configuration.
//
// An unrecognised field fails rather than defaulting: a typo that silently
// measured the default cell would land in the table under another cell's name,
// which is the one error this instrument cannot recover from afterwards.
func parseModeCell(b *testing.B, spec string) modeCell {
	b.Helper()

	c, err := parseModeSpec(spec)
	if err != nil {
		b.Fatalf("GRAPHENE_RSS_MODES: %v", err)
	}
	return c
}

// parseModeSpec is the rule itself, separated from the benchmark for the reason
// disk's imageMappingAllowedFor is separated from its method: a rule that can
// only be reached through a b.Fatalf is a rule that cannot be tested, and an
// untested parser here does not fail — it files a measurement under the name of
// a configuration it was not taken under.
func parseModeSpec(spec string) (modeCell, error) {
	c := modeCell{name: spec}
	if spec == "" {
		c.name = "mapped/mapped/eager"
		return c, nil
	}
	parts := strings.Split(spec, "/")
	if len(parts) < 3 || len(parts) > 4 {
		return c, fmt.Errorf("%q: want <image>/<index>/<adjacency>[/live]", spec)
	}
	switch parts[0] {
	case "mapped":
		c.image = disk.ImageMapped
	case "heap":
		c.image = disk.ImageHeap
	case "unlocked":
		c.image = disk.ImageMappedUnlocked
	default:
		return c, fmt.Errorf("%q: image is %q, want mapped, heap or unlocked", spec, parts[0])
	}
	switch parts[1] {
	case "mapped":
		c.index = disk.IndexMapped
	case "resident":
		c.index = disk.IndexResident
	default:
		return c, fmt.Errorf("%q: index is %q, want mapped or resident", spec, parts[1])
	}
	switch parts[2] {
	case "eager":
		c.adj = disk.AdjacencyEager
	case "lazy":
		c.adj = disk.AdjacencyLazy
	default:
		return c, fmt.Errorf("%q: adjacency is %q, want eager or lazy", spec, parts[2])
	}
	if len(parts) == 4 {
		if parts[3] != "live" {
			return c, fmt.Errorf("%q: fourth field is %q, want live", spec, parts[3])
		}
		c.live = true
	}
	return c, nil
}

// options turns the cell into the Options a caller would write.
//
// The non-live arms are read-only because that is the process the configuration
// space is for: a writer that deletes cannot use AdjacencyLazy, and a writer is
// not what a reader sizes a machine for. The live arm is the only way to reach
// ImageMappedUnlocked's distinguishing case, since for a locked opener it and
// ImageMapped resolve to the same thing.
func (c modeCell) options() disk.Options {
	o := disk.Options{ImageMode: c.image, IndexMode: c.index, Adjacency: c.adj}
	if c.live {
		o.LiveReader = true
		o.Constraints = disk.ConstraintDrop
		return o
	}
	o.ReadOnly = true
	return o
}

// oneIfMapped and friends make a held mode a number, because b.ReportMetric is
// the only channel benchstat reads. The strings go to the log beside them.
func oneIf(cond bool) float64 {
	if cond {
		return 1
	}
	return 0
}

// BenchmarkRSS_ModeMatrix opens the shared fixture under one configuration,
// reads ten rows through the index, and reports what the process is holding
// before and after the read.
//
// Run with -benchtime=1x. The read matters: a mapped index that nothing has
// queried has paged in nothing, and a table built from the figure at Open alone
// would report the mapped arms as free.
func BenchmarkRSS_ModeMatrix(b *testing.B) {
	cell := parseModeCell(b, os.Getenv("GRAPHENE_RSS_MODES"))
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	diskMiB := float64(rssStoreBytes(dir)) / bytesPerMiB

	b.ResetTimer()
	started := time.Now()
	g, err := graphene.OpenWithOptions(dir, cell.options())
	openMs := float64(time.Since(started).Microseconds()) / 1000
	if err != nil {
		b.Fatalf("open %s: %v", cell.name, err)
	}
	b.StopTimer()
	defer g.Close()

	atOpen, _ := settledRSS(g)

	// Ten rows through the index, which is BenchmarkRSS_Scan's shape: enough to
	// make a mapped index do its work, few enough that what is held afterwards
	// is the configuration's cost and not the answer's.
	const rows = 10
	stride := rssNodes / rows
	found := 0
	for i := 0; i < rows; i++ {
		ids, err := g.GraphStore.NodesByProperty("digest", rssProps(i * stride)["digest"])
		if err != nil {
			b.Fatalf("NodesByProperty: %v", err)
		}
		nodes, _, err := g.GetNodes(ids)
		if err != nil {
			b.Fatalf("GetNodes: %v", err)
		}
		found += len(nodes)
	}
	if found != rows {
		b.Fatalf("%s resolved %d rows, want %d — this cell measured a store that does "+
			"not answer, which is not a data point", cell.name, found, rows)
	}

	st, ok := g.StorageStats()
	if !ok {
		b.Fatal("StorageStats is unavailable")
	}

	s := reportRSS(b, g)
	if atOpen.Split {
		b.ReportMetric(float64(atOpen.Anon)/bytesPerMiB, "openAnonMiB")
		b.ReportMetric(float64(atOpen.File)/bytesPerMiB, "openFileMiB")
	}
	b.ReportMetric(openMs, "openMs")
	b.ReportMetric(diskMiB, "diskMiB")
	if diskMiB > 0 && s.Total > 0 {
		b.ReportMetric(float64(s.Total)/bytesPerMiB/diskMiB, "residentPerDisk")
	}

	// What was asked for, and what is held. Both, always: see the file header.
	b.ReportMetric(oneIf(cell.image != disk.ImageHeap), "imageMapAsked")
	b.ReportMetric(oneIf(st.ImageMode == "mapped"), "imageMapHeld")
	b.ReportMetric(oneIf(cell.index == disk.IndexMapped), "indexMapAsked")
	b.ReportMetric(oneIf(st.IndexMode == "mapped"), "indexMapHeld")
	b.ReportMetric(oneIf(cell.adj == disk.AdjacencyEager), "adjEagerAsked")
	b.ReportMetric(oneIf(st.Adjacency == "built"), "adjBuiltHeld")

	// The engine's own account of the same store, so the table can carry both
	// columns and §6.9's comparison extends to every cell rather than one.
	b.ReportMetric(float64(st.EstimatedResidentBytes)/bytesPerMiB, "estMiB")
	b.ReportMetric(float64(st.DeltaBytes)/bytesPerMiB, "deltaMiB")
	b.ReportMetric(float64(st.IndexEntries()), "indexEntries")
	b.ReportMetric(float64(st.HighestNodeID), "highestNodeID")

	// And the seven terms behind that total, which are the point: a total says
	// the configuration is expensive and the split says which option to change.
	// Without them the table would report differences between cells and leave
	// the reader to attribute them, which is how a term gets blamed for another
	// term's bytes.
	if ds, ok := g.GraphStore.(*disk.Store); ok {
		e := ds.EstimateResident()
		for _, t := range []struct {
			name  string
			bytes int64
		}{
			{"tRecordsMiB", e.RecordArrays},
			{"tPayloadMiB", e.Payload},
			{"tLabelsMiB", e.LabelPostings},
			{"tAdjMiB", e.Adjacency},
			{"tIndexMiB", e.Index},
			{"tDeltaMiB", e.Delta},
			{"tWALMiB", e.WAL},
			{"tMappedMiB", e.Mapped},
		} {
			b.ReportMetric(float64(t.bytes)/bytesPerMiB, t.name)
		}
	} else {
		b.Fatalf("the graph is backed by %T, not *disk.Store — this arm measures "+
			"the disk backend's term split and has nothing to report otherwise",
			g.GraphStore)
	}

	b.Logf("cell %s: image asked=%v held=%s | index asked=%v held=%s | adjacency asked=%v held=%s",
		cell.name, cell.image, st.ImageMode, cell.index, st.IndexMode, cell.adj, st.Adjacency)
}

// TestModeMatrix_EveryCellParses is the guard the sweep script cannot be, since
// a script that mistypes a cell gets a b.Fatal in a process whose output nobody
// reads until the table is being written.
//
// It also states the mode space this document's table covers: three by two by
// two, plus the live arm that is the only way ImageMappedUnlocked differs from
// ImageMapped.
func TestModeMatrix_EveryCellParses(t *testing.T) {
	seen := map[string]int{}
	for _, img := range []string{"mapped", "heap", "unlocked"} {
		for _, idx := range []string{"mapped", "resident"} {
			for _, adj := range []string{"eager", "lazy"} {
				spec := fmt.Sprintf("%s/%s/%s", img, idx, adj)
				c, err := parseModeSpec(spec)
				if err != nil {
					t.Fatalf("%s: %v", spec, err)
				}
				if c.live {
					t.Errorf("%s parsed as live", spec)
				}
				seen[fmt.Sprintf("%v/%v/%v", c.image, c.index, c.adj)]++
			}
		}
	}
	if len(seen) != 12 {
		t.Fatalf("the mode space collapsed to %d distinct cells, want 12: %v", len(seen), seen)
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("cell %s parsed from %d different specs", k, n)
		}
	}
	if c, err := parseModeSpec("unlocked/mapped/lazy/live"); err != nil || !c.live {
		t.Fatalf("the live suffix did not take: %+v %v", c, err)
	}
	if c, err := parseModeSpec(""); err != nil || c.image != disk.ImageMapped ||
		c.index != disk.IndexMapped || c.adj != disk.AdjacencyEager {
		t.Fatalf("an unset variable must be the shipped defaults, got %+v %v", c, err)
	}
}

// TestModeMatrix_TheLiveArmOpensLive guards the translation from a parsed cell
// to the Options a caller would write.
//
// It exists because a mutation survived without it, and the mutation was the
// expensive one: a live arm that quietly opened read-only would hold a lock,
// would therefore map its image and its index, and would report ~149 MiB where
// the live configuration actually holds ~568. §8.5's whole finding would have
// become a duplicate of the row above it, with three rounds of agreement behind
// the wrong number.
func TestModeMatrix_TheLiveArmOpensLive(t *testing.T) {
	live, err := parseModeSpec("unlocked/mapped/eager/live")
	if err != nil {
		t.Fatal(err)
	}
	o := live.options()
	if !o.LiveReader {
		t.Error("the live arm did not ask for a live reader, so it holds a lock — " +
			"and a locked opener maps, which is the opposite of what this arm measures")
	}
	if o.ReadOnly {
		t.Error("the live arm asked for ReadOnly, which takes the shared lock a live " +
			"reader exists not to take")
	}
	if o.ImageMode != disk.ImageMappedUnlocked {
		t.Errorf("the live arm's ImageMode is %v, want ImageMappedUnlocked", o.ImageMode)
	}

	locked, err := parseModeSpec("unlocked/mapped/eager")
	if err != nil {
		t.Fatal(err)
	}
	if o := locked.options(); o.LiveReader || !o.ReadOnly {
		t.Errorf("a non-live cell must open read-only and locked, got live=%v readonly=%v",
			o.LiveReader, o.ReadOnly)
	}
}

// TestModeMatrix_RefusesASpecItCannotHonour is the guard that matters, and it is
// not about parsing.
//
// Every other failure mode of this instrument is loud. This one is silent: a
// field that fell through to a default would open the store under the shipped
// configuration, measure it correctly, and report it under the name of the
// configuration that was asked for — and the table would then carry a row that
// is a real measurement of the wrong thing, with no symptom anywhere.
func TestModeMatrix_RefusesASpecItCannotHonour(t *testing.T) {
	for _, spec := range []string{
		"mapped/mapped",                  // too few fields
		"mapped/mapped/eager/live/extra", // too many
		"mmapped/mapped/eager",           // a typo in the image
		"mapped/residnet/eager",          // a typo in the index
		"mapped/mapped/eagre",            // a typo in the adjacency
		"mapped/mapped/eager/love",       // a typo in the suffix
		"MAPPED/mapped/eager",            // case is not folded
		"mapped/mapped/eager/",           // an empty fourth field
	} {
		if _, err := parseModeSpec(spec); err == nil {
			t.Errorf("%q was accepted; a spec this instrument cannot honour must be "+
				"refused, not defaulted — a defaulted field measures the shipped "+
				"configuration and files it under another one's name", spec)
		}
	}
}
