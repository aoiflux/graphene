// How long does opening a large store actually take, and what did this program
// change about it?
//
// The residency suite next door deliberately zeroes ns/op: a one-shot fixture
// makes a benchmark's own timing meaningless, and residency was the question.
// Open *time* is a different question and the one a caller notices first, so it
// gets its own arms and its own timer.
//
// The two arms are the store as it is served today and the store served the way
// v0.6.0 served it, on the same bytes and in the same process image:
//
//   - defaults: ImageMapped + IndexMapped. The image is mapped, so no part of it
//     is read or copied at open; the property index is read from the image's own
//     GPIX section, so no entry is replayed.
//
//   - heapResident: ImageHeap + IndexResident. The image is read whole into the
//     heap and every blob copied into an arena, and the property index is
//     rebuilt one entry at a time through IndexNode. That is the v0.6.0 open
//     path, reachable today because both options survived as options.
//
// Each arm is its own function and is meant to be run in its own process. They
// share a fixture directory, so the second process to run finds the file in the
// page cache; alternating the arms across rounds is what keeps that from
// favouring one of them.
//
//	GRAPHENE_RSS_DIR=/o/tmp/openfixture GRAPHENE_RSS_NODES=1500000 GRAPHENE_RSS_BLOB=1365 \
//	  go test ./tests/ -tags=stress -run=^$ -bench=OpenWall_Defaults -benchtime=1x -count=1

//go:build stress

package graphene_test

import (
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
)

// openWallOnce opens dir under opts, times it, and reports the store's size
// beside the duration so seconds-per-gigabyte can be read off one row.
func openWallOnce(b *testing.B, dir string, opts disk.Options) {
	b.Helper()

	diskMiB := float64(rssStoreBytes(dir)) / bytesPerMiB
	var (
		g   *graphene.Graph
		err error
	)

	b.ResetTimer()
	for b.Loop() {
		g, err = graphene.OpenWithOptions(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		// Close is not part of what is being timed, but it has to happen inside
		// the loop: the directory takes an exclusive lock for the life of the
		// handle, so a second iteration would be refused rather than measured.
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()

	b.ReportMetric(diskMiB, "diskMiB")
	if diskMiB > 0 {
		b.ReportMetric(float64(b.Elapsed().Seconds())/float64(b.N)/(diskMiB/1024), "secPerGiB")
	}
}

// BenchmarkOpenWall_Defaults times an open under the shipped defaults.
func BenchmarkOpenWall_Defaults(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	openWallOnce(b, dir, disk.Options{})
}

// BenchmarkOpenWall_HeapResident times the same open under the modes v0.6.0 had
// no alternative to: the image read into the heap, the index rebuilt entry by
// entry.
func BenchmarkOpenWall_HeapResident(b *testing.B) {
	dir := rssFixtureDir(b, rssNodes, rssBlob)
	openWallOnce(b, dir, disk.Options{
		ImageMode: disk.ImageHeap,
		IndexMode: disk.IndexResident,
	})
}

// BenchmarkOpenWall_V8UnderDefaults times opening a **v8** image under today's
// defaults, which is what a store written by v0.6.0 gets when the library is
// upgraded under it.
//
// The answer is not the fast one, and this arm exists so that is measured rather
// than assumed. A v8 image carries its property index as a GIDX section, and
// deserialiseCSR loads a GIDX one entry at a time through IndexNode whatever
// IndexMode says — IndexMapped has nothing to map, because the mappable section
// is the thing v8 does not have. So the index half of the open costs what it
// cost before, and only rewriting the image as v9 changes that. One Compact
// does it, or `graphene store migrate --to 9`.
func BenchmarkOpenWall_V8UnderDefaults(b *testing.B) {
	dir := v8FixtureDir(b)
	openWallOnce(b, dir, disk.Options{})
}

// v8FixtureDir copies the fixture and rewrites its image as v8 by compacting it
// under IndexResident, which is the mode that still writes GIDX.
//
// The version is read back off the file rather than inferred from the option: a
// fixture that silently stayed v9 would make this arm a duplicate of the
// defaults arm and the duplicate would look like a result.
func v8FixtureDir(b *testing.B) string {
	b.Helper()

	dir := rssMutableFixtureDir(b, rssNodes, rssBlob)
	g, err := graphene.OpenWithOptions(dir, disk.Options{
		ImageMode: disk.ImageHeap,
		IndexMode: disk.IndexResident,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		b.Fatal(err)
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}

	info, err := disk.InspectCSR(dir)
	if err != nil {
		b.Fatal(err)
	}
	if info.Version != 8 {
		b.Fatalf("fixture is version %d, not the v8 this arm is about", info.Version)
	}
	b.Logf("v8 fixture: version %d", info.Version)
	return dir
}
