package graphene_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/index/encoding"
	"github.com/aoiflux/graphene/store"
)

// An ordered index is only useful if it returns exactly what a scan would. These
// tests compare it against a brute-force evaluation of the same predicate over
// the same values, using byte comparison — the ordering a declared key commits
// to. Anything else would mean the index and the fallback path disagree.

// bruteForceMatch evaluates a filter directly against a value table using byte
// ordering, with no index involved.
func bruteForceMatch(values map[store.NodeID][]byte, f store.PropertyFilter) []store.NodeID {
	var out []store.NodeID
	for id, v := range values {
		if byteWiseMatches(f, v) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// byteWiseMatches is the predicate a declared ordered key implements: the same
// operators as store.PropertyFilterMatches, but always comparing bytes.
func byteWiseMatches(f store.PropertyFilter, actual []byte) bool {
	switch f.Op {
	case store.PropertyOpGreaterThan:
		return bytes.Compare(actual, f.Value) > 0
	case store.PropertyOpGreaterThanOrEqual:
		return bytes.Compare(actual, f.Value) >= 0
	case store.PropertyOpLessThan:
		return bytes.Compare(actual, f.Value) < 0
	case store.PropertyOpLessThanOrEqual:
		return bytes.Compare(actual, f.Value) <= 0
	case store.PropertyOpBetweenInclusive:
		if len(f.ValueUpper) == 0 {
			return false
		}
		return bytes.Compare(actual, f.Value) >= 0 && bytes.Compare(actual, f.ValueUpper) <= 0
	case store.PropertyOpPrefix:
		return bytes.HasPrefix(actual, f.Value)
	case store.PropertyOpEqual:
		return bytes.Equal(actual, f.Value)
	default:
		return false
	}
}

func queryIDs(t *testing.T, g *graphene.Graph, f store.PropertyFilter) []store.NodeID {
	t.Helper()
	ids, err := g.QueryNodeIDs(store.NodeQuery{Filters: []store.PropertyFilter{f}})
	if err != nil {
		t.Fatalf("QueryNodeIDs: %v", err)
	}
	return ids
}

// buildOrderedFixture registers encoded int64 scores plus a string bucket, and
// returns the value tables so tests can brute-force the same predicates.
func buildOrderedFixture(t *testing.T, g *graphene.Graph, n int, seed int64) (scores, buckets map[store.NodeID][]byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	scores = make(map[store.NodeID][]byte, n)
	buckets = make(map[store.NodeID][]byte, n)

	for i := 0; i < n; i++ {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{benchLabelFor(i)}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		// Deliberately spans negatives and zero — the case raw decimal strings
		// and naive byte comparison both get wrong.
		score := encoding.Int64(int64(rng.Intn(2000) - 1000))
		bucket := encoding.String(fmt.Sprintf("bucket-%03d", rng.Intn(50)))
		if err := g.IndexNodeProperties(id, map[string][]byte{
			"score":  score,
			"bucket": bucket,
		}); err != nil {
			t.Fatalf("IndexNodeProperties: %v", err)
		}
		scores[id] = score
		buckets[id] = bucket
	}
	return scores, buckets
}

func orderedFilterCases() []store.PropertyFilter {
	i64 := encoding.Int64
	return []store.PropertyFilter{
		{Key: "score", Op: store.PropertyOpGreaterThan, Value: i64(0)},
		{Key: "score", Op: store.PropertyOpGreaterThan, Value: i64(-1000)},
		{Key: "score", Op: store.PropertyOpGreaterThan, Value: i64(9999)},
		{Key: "score", Op: store.PropertyOpGreaterThanOrEqual, Value: i64(0)},
		{Key: "score", Op: store.PropertyOpGreaterThanOrEqual, Value: i64(-1000)},
		{Key: "score", Op: store.PropertyOpLessThan, Value: i64(0)},
		{Key: "score", Op: store.PropertyOpLessThan, Value: i64(-9999)},
		{Key: "score", Op: store.PropertyOpLessThanOrEqual, Value: i64(500)},
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: i64(-100), ValueUpper: i64(100)},
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: i64(-2000), ValueUpper: i64(2000)},
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: i64(50), ValueUpper: i64(50)},
		{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: i64(100), ValueUpper: i64(-100)}, // empty range
		{Key: "score", Op: store.PropertyOpEqual, Value: i64(42)},
		{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-0")},
		{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-01")},
		{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("nope")},
		{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("")},
		{Key: "bucket", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("bucket-025")},
		{Key: "bucket", Op: store.PropertyOpLessThan, Value: []byte("bucket-010")},
	}
}

func filterLabel(f store.PropertyFilter) string {
	return fmt.Sprintf("%s/op=%d/v=%x/u=%x", f.Key, f.Op, f.Value, f.ValueUpper)
}

// The core guarantee: a declared ordered key must return exactly what brute
// force does, on both backends, before and after compaction.
func TestOrderedIndex_MatchesBruteForce(t *testing.T) {
	for name, open := range traversalBackends() {
		for _, compact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compacted=%v", name, compact), func(t *testing.T) {
				g := open(t)
				scores, buckets := buildOrderedFixture(t, g, 400, 21)

				if err := g.DeclareOrderedProperty("score"); err != nil {
					t.Fatalf("DeclareOrderedProperty: %v", err)
				}
				if err := g.DeclareOrderedProperty("bucket"); err != nil {
					t.Fatalf("DeclareOrderedProperty: %v", err)
				}
				if compact {
					if err := g.Compact(); err != nil {
						t.Fatalf("Compact: %v", err)
					}
				}

				for _, f := range orderedFilterCases() {
					table := scores
					if f.Key == "bucket" {
						table = buckets
					}
					want := bruteForceMatch(table, f)
					got := queryIDs(t, g, f)
					if len(got) == 0 && len(want) == 0 {
						continue
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("%s\n got  (%d): %v\n want (%d): %v", filterLabel(f), len(got), got, len(want), want)
					}
				}
			})
		}
	}
}

// Declaring a key must not change the results of an equality lookup, and must
// not change results for keys left undeclared.
func TestOrderedIndex_DeclarationDoesNotDisturbOtherPaths(t *testing.T) {
	g := graphene.NewInMemory()
	scores, _ := buildOrderedFixture(t, g, 200, 5)

	equality := store.PropertyFilter{Key: "score", Op: store.PropertyOpEqual, Value: encoding.Int64(42)}
	prefixOnUndeclared := store.PropertyFilter{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("bucket-0")}

	beforeEq := queryIDs(t, g, equality)
	beforePrefix := queryIDs(t, g, prefixOnUndeclared)

	if err := g.DeclareOrderedProperty("score"); err != nil {
		t.Fatalf("DeclareOrderedProperty: %v", err)
	}

	if got := queryIDs(t, g, equality); !reflect.DeepEqual(got, beforeEq) {
		t.Fatalf("equality result changed after declaration:\n before: %v\n after:  %v", beforeEq, got)
	}
	if got := queryIDs(t, g, prefixOnUndeclared); !reflect.DeepEqual(got, beforePrefix) {
		t.Fatalf("undeclared key changed after declaring another:\n before: %v\n after:  %v", beforePrefix, got)
	}
	if got := bruteForceMatch(scores, equality); !reflect.DeepEqual(queryIDs(t, g, equality), got) {
		t.Fatalf("equality disagrees with brute force")
	}

	nodeKeys, _ := g.OrderedProperties()
	if !reflect.DeepEqual(nodeKeys, []string{"score"}) {
		t.Fatalf("OrderedProperties = %v, want [score]", nodeKeys)
	}
}

// Declaring after data is already indexed must absorb the existing entries.
func TestOrderedIndex_AbsorbsPreexistingEntries(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			scores, _ := buildOrderedFixture(t, g, 300, 9)

			f := store.PropertyFilter{Key: "score", Op: store.PropertyOpGreaterThan, Value: encoding.Int64(0)}
			want := bruteForceMatch(scores, f)

			if err := g.DeclareOrderedProperty("score"); err != nil {
				t.Fatalf("DeclareOrderedProperty: %v", err)
			}
			if got := queryIDs(t, g, f); !reflect.DeepEqual(got, want) {
				t.Fatalf("after declaring:\n got  (%d): %v\n want (%d): %v", len(got), got, len(want), want)
			}
			if err := g.VerifyIndexes(); err != nil {
				t.Fatalf("VerifyIndexes: %v", err)
			}
		})
	}
}

// The ordered index has to survive the full mutation lifecycle, staying in step
// with the hash postings it mirrors.
func TestOrderedIndex_StaysConsistentThroughMutations(t *testing.T) {
	for name, open := range traversalBackends() {
		t.Run(name, func(t *testing.T) {
			g := open(t)
			if err := g.DeclareOrderedProperty("score"); err != nil {
				t.Fatalf("DeclareOrderedProperty: %v", err)
			}

			rng := rand.New(rand.NewSource(33))
			scores := make(map[store.NodeID][]byte)
			var ids []store.NodeID

			for step := 0; step < 300; step++ {
				switch {
				case len(ids) < 20 || rng.Intn(100) < 60: // add
					id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
					if err != nil {
						t.Fatalf("AddNode: %v", err)
					}
					v := encoding.Int64(int64(rng.Intn(400) - 200))
					if err := g.IndexNodeProperty(id, "score", v); err != nil {
						t.Fatalf("IndexNodeProperty: %v", err)
					}
					scores[id] = v
					ids = append(ids, id)

				case rng.Intn(100) < 50: // re-register a new value on an existing node
					victim := ids[rng.Intn(len(ids))]
					v := encoding.Int64(int64(rng.Intn(400) - 200))
					if err := g.UpdateNodeIndexed(
						&store.Node{ID: victim, Labels: []store.NodeType{store.NodeTypeTag}},
						map[string][]byte{"score": v},
					); err != nil {
						t.Fatalf("UpdateNodeIndexed: %v", err)
					}
					scores[victim] = v

				default: // delete
					pos := rng.Intn(len(ids))
					victim := ids[pos]
					if err := g.DeleteNode(victim); err != nil {
						t.Fatalf("DeleteNode: %v", err)
					}
					delete(scores, victim)
					ids = append(ids[:pos], ids[pos+1:]...)
				}

				if err := g.VerifyIndexes(); err != nil {
					t.Fatalf("step %d: VerifyIndexes: %v", step, err)
				}
			}

			for _, f := range []store.PropertyFilter{
				{Key: "score", Op: store.PropertyOpGreaterThan, Value: encoding.Int64(0)},
				{Key: "score", Op: store.PropertyOpLessThanOrEqual, Value: encoding.Int64(-50)},
				{Key: "score", Op: store.PropertyOpBetweenInclusive, Value: encoding.Int64(-100), ValueUpper: encoding.Int64(100)},
			} {
				want := bruteForceMatch(scores, f)
				got := queryIDs(t, g, f)
				if len(got) == 0 && len(want) == 0 {
					continue
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s after mutations\n got  (%d): %v\n want (%d): %v",
						filterLabel(f), len(got), got, len(want), want)
				}
			}
		})
	}
}

// A declared key must survive a compact/reopen cycle: the CSR carries the
// entries, and re-declaring rebuilds the ordering over them.
func TestOrderedIndex_SurvivesReopenAfterRedeclaration(t *testing.T) {
	dir := t.TempDir()
	g, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	scores, _ := buildOrderedFixture(t, g, 200, 17)
	if err := g.DeclareOrderedProperty("score"); err != nil {
		t.Fatalf("DeclareOrderedProperty: %v", err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := graphene.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	// The declaration itself is not persisted — it is a runtime choice about how
	// to index, like an index definition in any other engine. Re-declaring must
	// reproduce the same answers from the persisted entries.
	nodeKeys, _ := reopened.OrderedProperties()
	if len(nodeKeys) != 0 {
		t.Fatalf("expected no declared keys after reopen, got %v", nodeKeys)
	}
	if err := reopened.DeclareOrderedProperty("score"); err != nil {
		t.Fatalf("DeclareOrderedProperty after reopen: %v", err)
	}

	f := store.PropertyFilter{Key: "score", Op: store.PropertyOpBetweenInclusive,
		Value: encoding.Int64(-200), ValueUpper: encoding.Int64(200)}
	want := bruteForceMatch(scores, f)
	if got := queryIDs(t, reopened, f); !reflect.DeepEqual(got, want) {
		t.Fatalf("after reopen\n got  (%d): %v\n want (%d): %v", len(got), got, len(want), want)
	}
	if err := reopened.VerifyIndexes(); err != nil {
		t.Fatalf("VerifyIndexes: %v", err)
	}
}
