// What a compaction leaves behind, measured term by term against the same store
// reopened from the same directory.
//
// The two ought to be the same store. They are not, and the difference is not
// small. A compaction publishes a graph it built in the heap, and it publishes
// the property index it started with -- every entry it has just written to the
// image is still sitting in the delta shards, because a base is attached on the
// load path and nowhere else. So a process that compacts goes on holding the
// compaction's own output until something reopens the directory underneath it.
//
// This test names which of the seven terms do that and by how much. The estimate
// and the operating system's own figure are read at the same points, because one
// term of the estimate is known to over-report here: buildSeq records what the
// records it places reference, so a graph published over a mapped image counts
// page cache as heap until the next open. See CSRGraph.payloadBytes. Anonymous
// RSS is the arbiter whenever the two disagree.
//
//	go test ./tests/ -tags=stress -run TestResidency_AfterCompactionVersusReopen -v
//	GRAPHENE_RSS_NODES=200000 GRAPHENE_RSS_DIR=/var/tmp/fix go test ./tests/ -tags=stress \
//	    -run TestResidency_AfterCompactionVersusReopen -v

//go:build stress

package graphene_test

import (
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/store"
)

// residencyRebuildChunk is how many nodes are written between the delete pass
// and the write pass reporting progress. It has no effect on the result; it
// exists so a long run says something while it is going.
const residencyRebuildChunk = 50_000

// residencyPoint is one reading: the seven-term estimate beside the operating
// system's figure, so a term that moves can be checked against a total that
// moved with it.
type residencyPoint struct {
	Name string
	Est  disk.ResidentEstimate
	RSS  rssSample
}

// residencyAt settles the heap and reads both instruments.
//
// Deliberately not a t.Helper(): when this fails it is the reading that failed,
// and the line worth reporting is the one inside here rather than the phase that
// called it.
func residencyAt(t *testing.T, name string, g *graphene.Graph) residencyPoint {
	s, ok := g.Forensics()
	if !ok {
		t.Fatalf("%s: Forensics reported no disk store behind this graph", name)
	}
	rss, _ := settledRSS(g)
	return residencyPoint{Name: name, Est: s.EstimateResident(), RSS: rss}
}

func TestResidency_AfterCompactionVersusReopen(t *testing.T) {
	if !rssSupported {
		t.Skipf("no RSS instrument on this platform; the estimate alone cannot arbitrate this")
	}

	dir := rssMutableFixtureDir(t, rssNodes, rssBlob)
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if g != nil {
			g.Close()
		}
	}()

	points := []residencyPoint{residencyAt(t, "open", g)}

	// The live set, enumerated the streaming way. A materialised slice of every
	// identifier is itself one of the terms under measurement, so this one is
	// held deliberately and its cost is known: eight bytes an id.
	query := store.NodeQuery{Types: []store.NodeType{
		store.NodeTypeMicroArtefact, store.NodeTypeEvidenceFile, store.NodeTypeCase,
	}}
	live := make([]store.NodeID, 0, rssNodes)
	if err := g.ForEachNodeID(query, func(id store.NodeID) bool {
		live = append(live, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if len(live) != rssNodes {
		t.Fatalf("enumerated %d live nodes, want %d", len(live), rssNodes)
	}

	// One rebuild cycle, which is what makes every record in the published graph
	// a heap record rather than one aliasing the mapping. A compaction that
	// touched a handful of records would leave most of them addressing the image
	// and would not show the effect at all, which is exactly why the consumer's
	// workload finds it and a small one does not.
	for _, id := range live {
		if err := g.DeleteNode(id); err != nil {
			t.Fatalf("DeleteNode(%d): %v", id, err)
		}
	}
	written := 0
	for base := 0; base < rssNodes; base += residencyRebuildChunk {
		n := residencyRebuildChunk
		if remaining := rssNodes - base; remaining < n {
			n = remaining
		}
		written += len(rssWriteNodes(t, g, rssNodes+base, n, rssBlob))
	}
	if written != rssNodes {
		t.Fatalf("wrote %d nodes, want %d", written, rssNodes)
	}
	live = nil
	points = append(points, residencyAt(t, "rebuilt", g))

	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	points = append(points, residencyAt(t, "compacted", g))

	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	g = nil
	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	g = reopened

	// Counted before the reading, so a run cannot report a beautifully small
	// store that a compaction had quietly emptied.
	counts, err := g.CountNodesByType()
	if err != nil {
		t.Fatalf("CountNodesByType: %v", err)
	}
	var total uint64
	for _, n := range counts {
		total += n
	}
	if total != uint64(rssNodes) {
		t.Fatalf("reopened store holds %d nodes, want %d", total, rssNodes)
	}
	points = append(points, residencyAt(t, "reopened", g))

	residencyReport(t, points)
	residencyAssert(t, points)
}

// residencyAssert states the claim the report only describes: a compaction gives
// its own index back, at the compaction, without being reopened.
//
// Only the index term is asserted. The record payloads are still held until a
// reopen -- recovering those means re-mapping the image and republishing a graph
// parsed from it, which changes the aliasing contract on a returned Properties
// slice and is a separate decision from this one. The report above prints that
// gap every run, so the day it is closed the number says so.
func residencyAssert(t *testing.T, points []residencyPoint) {
	byName := map[string]residencyPoint{}
	for _, p := range points {
		byName[p.Name] = p
	}
	rebuilt, compacted, reopened := byName["rebuilt"], byName["compacted"], byName["reopened"]

	// First, that there was anything to give back. Without this the assertion
	// below passes on a fixture whose rebuild happened to index nothing, which is
	// the way a guard like this goes quiet rather than red.
	grew := rebuilt.Est.Index - reopened.Est.Index
	if grew < 8*bytesPerMiB {
		t.Fatalf("the rebuild left the index only %.1f MiB above a freshly opened store; "+
			"this fixture no longer produces the state the compaction is supposed to release",
			float64(grew)/bytesPerMiB)
	}

	// And then the claim. Equal, not merely smaller: the compaction wrote exactly
	// the entries a reopen reads back, so holding any more of them than a reopen
	// does is holding its own output. The tolerance is for the fences and key
	// directory a base keeps resident, which are a function of declared keys and
	// not of entries.
	const slack = 1 << 20
	if held := compacted.Est.Index - reopened.Est.Index; held > slack {
		t.Errorf("after compacting, the index holds %.1f MiB more than the same store reopened "+
			"(%.1f vs %.1f); the compaction is still holding the entries it just wrote",
			float64(held)/bytesPerMiB,
			float64(compacted.Est.Index)/bytesPerMiB, float64(reopened.Est.Index)/bytesPerMiB)
	}

	// The operating system has to agree. The estimate is a model and this is the
	// arbiter; a model that improved while the process did not would be a wrong
	// model rather than a result. Half the rebuild's growth is a deliberately
	// loose bar -- the record payloads are still held, and they are most of what
	// is left -- and it fails the case that matters, which is a compacted store
	// sitting at the rebuild's figure.
	if compacted.RSS.Split && rebuilt.RSS.Split && reopened.RSS.Split {
		grewRSS := int64(rebuilt.RSS.Anon) - int64(reopened.RSS.Anon)
		heldRSS := int64(compacted.RSS.Anon) - int64(reopened.RSS.Anon)
		if grewRSS > 0 && heldRSS > grewRSS/2 {
			t.Errorf("after compacting, anonymous RSS is %.1f MiB above a reopened store, "+
				"more than half the %.1f MiB the rebuild added; the index came back and the process did not",
				float64(heldRSS)/bytesPerMiB, float64(grewRSS)/bytesPerMiB)
		}
	}
}

// residencyReport prints the terms and then states the gap the fix has to close.
func residencyReport(t *testing.T, points []residencyPoint) {
	mib := func(b int64) string { return fmt.Sprintf("%.1f", float64(b)/bytesPerMiB) }
	umib := func(b uint64) string { return fmt.Sprintf("%.1f", float64(b)/bytesPerMiB) }

	t.Logf("shape: %d nodes, %d-byte blobs, %d index entries per node",
		rssNodes, rssBlob, len(rssUniqueKeys)+len(rssOrderedKeys))
	t.Logf("%-10s %10s %10s %10s %10s %10s %10s %10s %10s %10s",
		"point", "records", "payload", "labels", "adjacency", "index", "delta",
		"est total", "anon", "file")
	for _, p := range points {
		anon, file := "-", "-"
		if p.RSS.Split {
			anon, file = umib(p.RSS.Anon), umib(p.RSS.File)
		}
		t.Logf("%-10s %10s %10s %10s %10s %10s %10s %10s %10s %10s",
			p.Name,
			mib(p.Est.RecordArrays), mib(p.Est.Payload), mib(p.Est.LabelPostings),
			mib(p.Est.Adjacency), mib(p.Est.Index), mib(p.Est.Delta),
			mib(p.Est.Total), anon, file)
	}

	compacted, reopened := points[len(points)-2], points[len(points)-1]
	t.Logf("the gap a compaction leaves, compacted minus reopened:")
	t.Logf("  index        %s MiB", mib(compacted.Est.Index-reopened.Est.Index))
	t.Logf("  records      %s MiB", mib(compacted.Est.RecordArrays-reopened.Est.RecordArrays))
	t.Logf("  payload      %s MiB (over-reports: see CSRGraph.payloadBytes)",
		mib(compacted.Est.Payload-reopened.Est.Payload))
	t.Logf("  est total    %s MiB", mib(compacted.Est.Total-reopened.Est.Total))
	if compacted.RSS.Split && reopened.RSS.Split {
		// Signed, and deliberately so: an unsigned subtraction of a larger
		// reopened figure reports four million terabytes rather than a negative
		// number, and a gap that comes out negative is a result worth seeing
		// rather than one worth hiding.
		t.Logf("  anon RSS     %s MiB  <- the figure that arbitrates",
			mib(int64(compacted.RSS.Anon)-int64(reopened.RSS.Anon)))
	}
}
