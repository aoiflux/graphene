// Is serving property values out of the index cheaper than reading the record?
//
// This is the measurement N1 (GetNodesBatchProjected) has to pass before it is
// worth building, and it is deliberately run before any of it is written. The
// claim N1 would make is that a caller wanting three indexed fields of a record
// should not have to fault in the record's whole payload to get them. Whether
// that is true is not an argument, it is a ratio, and the ratio moves with the
// payload size — which is why the two arms below are run at more than one.
//
// The arms are the two ends of what N1 would sit between:
//
//   - the record path, as a caller writes it today: one batch read, then the
//     payload bytes are there to decode. The engine never parses the blob, so
//     what is charged here is reaching it, not decoding it — the decode is the
//     caller's and is the part N1 would remove. Summing the bytes stands in for
//     touching them, which under a mapped image is what makes them resident.
//
//   - the index path, at its **worst**: NodeEntriesOf, which takes all sixteen
//     shard locks, copies every value, sorts the result, and does it once per
//     id. A real N1 would group the keys by shard, take one lock each, skip the
//     sort, and walk a sorted id set through the base's reverse section
//     monotonically instead of binary-searching per id. So this arm is an upper
//     bound and not a proposal: if it is already close to the record path, the
//     optimised form has room; if it is an order of magnitude away, no amount of
//     grouping closes that and N1 should not be built.
//
// Each arm is its own function, per TECHNICAL_DETAILS.md §14.27.
//
//	go test ./disk/ -tags=stress -run=^$ -bench=ProjectFeasible -benchtime=1x -count=6
//	GRAPHENE_PROJ_BLOB=65536 go test ./disk/ -tags=stress -run=^$ -bench=ProjectFeasible -benchtime=1x -count=6

//go:build stress

package disk

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"strconv"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func projEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

var (
	projNodes = projEnvInt("GRAPHENE_PROJ_NODES", 100_000)
	projBlob  = projEnvInt("GRAPHENE_PROJ_BLOB", 512)
)

// projKeys is the shape the consumer declares: a handful of keys per entity, of
// which a reader wants a few. Four is enough to make the per-id fixed costs
// (the locks, the sort) visible without swamping them.
var projKeys = []string{"digest", "bucket", "kind", "seq"}

var projSink int64

// projStore builds a compacted, reopened store whose nodes each carry projBlob
// payload bytes and one indexed value under each of projKeys.
//
// Reopened, because a compaction leaves its own image on the heap and only the
// load path maps it — measuring without the reopen would charge the record arm
// heap reads and the index arm a heap base, which is neither the shipped default
// nor the shape N1 is for.
func projStore(tb testing.TB, nodes int) (*Store, []store.NodeID) {
	tb.Helper()
	return projStoreIn(tb, nodes, true)
}

// projStoreDelta is projStore without the compaction, so the entries stay in
// the index's shards.
//
// It exists because every other arm here measures the *base* path: the fixture
// compacts, so the shard path — which is where naming the keys turns sixteen
// lock acquisitions a record into four a chunk — was never under measurement,
// and a claim about it would have rested on reading the code.
func projStoreDelta(tb testing.TB, nodes int) (*Store, []store.NodeID) {
	tb.Helper()
	return projStoreIn(tb, nodes, false)
}

func projStoreIn(tb testing.TB, nodes int, compact bool) (*Store, []store.NodeID) {
	tb.Helper()

	dir := tb.TempDir()
	s, err := Open(dir)
	if err != nil {
		tb.Fatal(err)
	}

	blob := bytes.Repeat([]byte{0xA5}, projBlob)
	ids := make([]store.NodeID, 0, nodes)
	val := make([]byte, 32)
	for i := 0; i < nodes; i++ {
		id, err := s.AddNode(&store.Node{
			Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
			Properties: blob,
		})
		if err != nil {
			tb.Fatal(err)
		}
		for k, key := range projKeys {
			binary.BigEndian.PutUint64(val[:8], uint64(i))
			binary.BigEndian.PutUint64(val[8:16], uint64(k))
			if err := s.IndexNodeProperty(id, key, val); err != nil {
				tb.Fatal(err)
			}
		}
		ids = append(ids, id)
	}
	if !compact {
		tb.Cleanup(func() { s.Close() })
		st := s.StorageStats()
		tb.Logf("fixture (delta-resident): %d nodes, %d-byte payloads, %d index entries",
			nodes, projBlob, st.IndexEntries())
		return s, ids
	}

	if err := s.Compact(); err != nil {
		tb.Fatal(err)
	}
	if err := s.Close(); err != nil {
		tb.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { reopened.Close() })

	st := reopened.StorageStats()
	tb.Logf("fixture: %d nodes, %d-byte payloads, image %s (%d mapped bytes), %d index entries",
		nodes, projBlob, st.ImageMode, st.ImageMappedBytes, st.IndexEntries())
	return reopened, ids
}

// BenchmarkProjectFeasible_Record reads every record and touches its payload,
// which is the path a caller takes today to reach any field.
func BenchmarkProjectFeasible_Record(b *testing.B) {
	s, ids := projStore(b, projNodes)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		found, missing := s.GetNodesBatch(ids)
		if len(missing) != 0 {
			b.Fatalf("%d missing", len(missing))
		}
		for _, n := range found {
			// Touching the payload is what makes it resident under a mapping;
			// a benchmark that only took len() would measure a pointer.
			for j := 0; j < len(n.Properties); j += 4096 {
				projSink += int64(n.Properties[j])
			}
			projSink += int64(len(n.Properties))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(projNodes), "records")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*projNodes), "ns/record")
}

// BenchmarkProjectFeasible_Index reads the same values out of the property
// index instead, at the worst cost the index can charge for them.
func BenchmarkProjectFeasible_Index(b *testing.B) {
	s, ids := projStore(b, projNodes)
	want := make(map[string]struct{}, len(projKeys))
	for _, k := range projKeys {
		want[k] = struct{}{}
	}
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for _, id := range ids {
			entries := s.propIdx.NodeEntriesOf(id)
			hits := 0
			for _, e := range entries {
				if _, ok := want[e.Key]; ok {
					hits++
					projSink += int64(len(e.Value))
				}
			}
			if hits != len(projKeys) {
				b.Fatalf("node %d: %d of %d keys found", id, hits, len(projKeys))
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(projNodes), "records")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*projNodes), "ns/record")
}

// TestProjectFeasible_ArmsAgree checks the two arms are reading the same store
// and finding the same keys, so a ratio between them is a ratio between two
// answers rather than between an answer and a miss.
func TestProjectFeasible_ArmsAgree(t *testing.T) {
	s, ids := projStore(t, 200)
	found, missing := s.GetNodesBatch(ids)
	if len(missing) != 0 || len(found) != len(ids) {
		t.Fatalf("record arm: %d found, %d missing, want %d", len(found), len(missing), len(ids))
	}
	for _, n := range found {
		if len(n.Properties) != projBlob {
			t.Fatalf("node %d: payload %d bytes, want %d", n.ID, len(n.Properties), projBlob)
		}
	}
	for _, id := range ids {
		got := map[string]int{}
		for _, e := range s.propIdx.NodeEntriesOf(id) {
			got[e.Key]++
		}
		for _, k := range projKeys {
			if got[k] != 1 {
				t.Fatalf("node %d: key %q appears %d times, want 1 (%v)", id, k, got[k], got)
			}
		}
	}
}

// BenchmarkProjectFeasible_Projection is the shipped form: ForEachNodeProjection,
// which names its keys and so takes one lock per key's shard per chunk instead
// of sixteen per record, skips the sort, and copies each value once into a
// scratch slab rather than allocating a PropEntry for it.
//
// It is the arm the other two exist to be read against: the record path is what
// a caller does today, and the NodeEntriesOf path is what the index cost before
// the keys were named.
func BenchmarkProjectFeasible_Projection(b *testing.B) {
	s, ids := projStore(b, projNodes)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		hits := 0
		err := s.ForEachNodeProjection(context.Background(), ids, projKeys,
			func(_, _ int, value []byte) bool {
				hits++
				projSink += int64(len(value))
				return true
			})
		if err != nil {
			b.Fatal(err)
		}
		if hits != len(ids)*len(projKeys) {
			b.Fatalf("%d hits, want %d", hits, len(ids)*len(projKeys))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(projNodes), "records")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*projNodes), "ns/record")
}

// BenchmarkProjectFeasible_ProjectionDelta is the shipped projection against a
// store whose entries are still in the index's shards, which is the half the
// compacted arms never reach.
func BenchmarkProjectFeasible_ProjectionDelta(b *testing.B) {
	s, ids := projStoreDelta(b, projNodes)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		hits := 0
		err := s.ForEachNodeProjection(context.Background(), ids, projKeys,
			func(_, _ int, value []byte) bool {
				hits++
				projSink += int64(len(value))
				return true
			})
		if err != nil {
			b.Fatal(err)
		}
		if hits != len(ids)*len(projKeys) {
			b.Fatalf("%d hits, want %d", hits, len(ids)*len(projKeys))
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(projNodes), "records")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*projNodes), "ns/record")
}

// BenchmarkProjectFeasible_IndexDelta is NodeEntriesOf against the same
// delta-resident store, so the shard path has both arms too.
func BenchmarkProjectFeasible_IndexDelta(b *testing.B) {
	s, ids := projStoreDelta(b, projNodes)
	want := make(map[string]struct{}, len(projKeys))
	for _, k := range projKeys {
		want[k] = struct{}{}
	}
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for _, id := range ids {
			hits := 0
			for _, e := range s.propIdx.NodeEntriesOf(id) {
				if _, ok := want[e.Key]; ok {
					hits++
					projSink += int64(len(e.Value))
				}
			}
			if hits != len(projKeys) {
				b.Fatalf("node %d: %d of %d keys found", id, hits, len(projKeys))
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(projNodes), "records")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*projNodes), "ns/record")
}
