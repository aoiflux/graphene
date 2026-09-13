package index

// The stream is the same merge as the slice, so the property to hold is that it
// answers the same. mergeRun and IDStream share one cursor today, which is a fact
// about today's code; what a later change can break is the agreement between the
// two drivers, and the agreement is what these assert.

import (
	"fmt"
	"slices"
	"testing"

	"github.com/aoiflux/graphene/store"
)

// drain reads a stream to the end.
func drain[T entityID](s *IDStream[T]) []T {
	var out []T
	for {
		id, ok := s.Next()
		if !ok {
			return out
		}
		out = append(out, id)
	}
}

// streamIndex is batch_test.go's fixture: a base, and a delta that overlaps it in
// every way the three terms allow.
func streamIndex(t *testing.T) *PropertyIndex {
	t.Helper()
	p := NewPropertyIndex()
	if err := p.AttachBase(newFakeBase(batchCorpus())); err != nil {
		t.Fatalf("AttachBase: %v", err)
	}
	p.IndexNode(500, "digest", []byte("d005"))  // adds to a value the base holds
	p.IndexNode(501, "digest", []byte("dnew1")) // a value the base never had
	p.IndexNode(3, "digest", []byte("d002"))    // an id the base already has there
	p.IndexNode(502, "tag", []byte("t3"))
	// Below every base id for this value, and between two of them. Without these
	// the delta never sorts before the base, and a merge that emits the base side
	// first and pushes the delta id to the end holds the same *set* — which a
	// mutation that skipped a low delta id proved a differential on sets alone
	// cannot see. The base's t3 ids are 4, 11, 18, ...
	p.IndexNode(1, "tag", []byte("t3"))
	p.IndexNode(5, "tag", []byte("t3"))
	p.RemoveNode(7)
	p.RemoveNode(120)
	p.IndexNode(120, "digest", []byte("d119")) // retracted, then re-registered
	p.IndexEdge(900, "rel", []byte("r03"))
	p.IndexEdge(901, "rel", []byte("rnew"))
	p.RemoveEdge(5)
	return p
}

// streamValues covers every value either side holds, plus values absent below,
// between and above the base's range.
func streamValues() [][]byte {
	var values [][]byte
	for i := 0; i < 125; i++ {
		values = append(values, []byte(fmt.Sprintf("d%03d", i)))
	}
	return append(values, []byte("dnew1"), []byte("t3"), []byte("t0"),
		[]byte(""), []byte("a"), []byte("zzzz"), []byte("d005x"))
}

func TestIDStream_AnswersExactlyWhatNodesByPropertyAnswers(t *testing.T) {
	p := streamIndex(t)
	for _, key := range []string{"digest", "tag", "absent-key"} {
		for _, v := range streamValues() {
			want := p.NodesByProperty(key, v)
			got := drain(p.NodesByPropertyStream(key, v))
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%s=%q: stream says %v, NodesByProperty says %v", key, v, got, want)
			}
		}
	}
}

func TestIDStream_AnswersExactlyWhatEdgesByPropertyAnswers(t *testing.T) {
	p := streamIndex(t)
	var values [][]byte
	for i := 0; i < 12; i++ {
		values = append(values, []byte(fmt.Sprintf("r%02d", i)))
	}
	values = append(values, []byte("rnew"), []byte(""), []byte("zz"))
	for _, v := range values {
		want := p.EdgesByProperty("rel", v)
		got := drain(p.EdgesByPropertyStream("rel", v))
		if len(want) == 0 && len(got) == 0 {
			continue
		}
		if !slices.Equal(got, want) {
			t.Fatalf("rel=%q: stream says %v, EdgesByProperty says %v", v, got, want)
		}
	}
}

// TestIDStream_WithNoBaseWalksTheDelta holds the branch that states the no-base
// case as a zero-value cursor rather than as a second loop. If the run were ever
// consulted there the nil retract set would panic, so this is also the guard that
// nothing starts consulting it.
func TestIDStream_WithNoBaseWalksTheDelta(t *testing.T) {
	p := NewPropertyIndex()
	for i := 1; i <= 50; i++ {
		p.IndexNode(store.NodeID(i), "k", []byte("v"))
	}
	got := drain(p.NodesByPropertyStream("k", []byte("v")))
	want := p.NodesByProperty("k", []byte("v"))
	if !slices.Equal(got, want) {
		t.Fatalf("stream says %v, NodesByProperty says %v", got, want)
	}
	if n := len(drain(p.NodesByPropertyStream("k", []byte("absent")))); n != 0 {
		t.Fatalf("an absent value yielded %d ids", n)
	}
}

// TestIDStream_FillIsTheSameSequenceAsNext holds the batch driver against the
// per-id one, at sizes that do and do not divide the answer.
func TestIDStream_FillIsTheSameSequenceAsNext(t *testing.T) {
	p := streamIndex(t)
	want := drain(p.NodesByPropertyStream("tag", []byte("t3")))
	if len(want) < 10 {
		t.Fatalf("fixture too small to batch: %d ids", len(want))
	}
	for _, size := range []int{1, 2, 7, len(want) - 1, len(want), len(want) + 1, 4096} {
		s := p.NodesByPropertyStream("tag", []byte("t3"))
		var got []store.NodeID
		for {
			var done bool
			got, done = s.Fill(got, len(got)+size)
			if done {
				break
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("size %d: Fill says %v, Next says %v", size, got, want)
		}
	}
}

// TestIDStream_FillRefusesANonPositiveLimit: a computed batch size that came out
// zero is a bug in the caller, and the two ways to be unhelpful about it are to
// loop forever and to report a stream drained that is not. Neither happens.
func TestIDStream_FillRefusesANonPositiveLimit(t *testing.T) {
	p := streamIndex(t)
	s := p.NodesByPropertyStream("tag", []byte("t3"))
	for _, limit := range []int{0, -1} {
		got, done := s.Fill(nil, limit)
		if len(got) != 0 || done {
			t.Fatalf("limit %d: appended %d ids, done=%v", limit, len(got), done)
		}
	}
	if n := len(drain(s)); n == 0 {
		t.Fatal("the stream was consumed by a refused Fill")
	}
}
