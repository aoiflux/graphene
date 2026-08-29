package graphene_test

// Cancelling the four calls that are not traversals.
//
// VerifyIndexes, RebuildIndexes, QueryNodes and QueryEdges each walk something
// proportional to the whole store while holding a lock, and until now none of
// them could be stopped. The tests here assert two separate things, which is
// the distinction the feature turns on: that a cancelled call *returns*, and
// that a cancelled call returns nothing that could be mistaken for an answer.
//
// # Why the contexts here are hand-rolled
//
// A real cancelled context is easy to test with and only reaches the check
// made before the work starts. What needs pinning is the check *inside* the
// loops — the one that decides whether a call already ten seconds in can be
// stopped at all — and racing a goroutine against a loop to hit it is a flake
// waiting to happen. countdownCtx reports "not cancelled" for a fixed number
// of Err() calls and "cancelled" afterwards, so the cancellation lands inside
// the loop at a point the test chooses rather than one the scheduler does.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/disk"
	"github.com/aoiflux/graphene/memory"
	"github.com/aoiflux/graphene/store"
)

// Both bundled backends must offer every cancellable capability. A backend that
// implements the plain interface and not the Ctx one still works — the Graph
// falls back — so a missing implementation would otherwise show up as a test
// that quietly stops testing anything.
var (
	_ store.QuerierCtx        = (*disk.Store)(nil)
	_ store.QuerierCtx        = (*memory.Store)(nil)
	_ store.IndexVerifierCtx  = (*disk.Store)(nil)
	_ store.IndexVerifierCtx  = (*memory.Store)(nil)
	_ store.IndexRebuilderCtx = (*disk.Store)(nil)
	_ store.IndexRebuilderCtx = (*memory.Store)(nil)
)

// countdownCtx is cancelled on the nth call to Err, whichever call that is.
//
// Done returns a channel that is never closed but is never nil either, which is
// what stops store.CancelCheck taking its uncancellable fast path. Nothing here
// selects on it; the checks all go through Err.
type countdownCtx struct {
	context.Context
	done      chan struct{}
	remaining atomic.Int64
}

func newCountdownCtx(clean int64) *countdownCtx {
	c := &countdownCtx{Context: context.Background(), done: make(chan struct{})}
	c.remaining.Store(clean)
	return c
}

func (c *countdownCtx) Done() <-chan struct{} { return c.done }

func (c *countdownCtx) Err() error {
	if c.remaining.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

// checks reports how many times the context was consulted, which is what the
// amortisation assertions are about.
func (c *countdownCtx) checks(clean int64) int64 { return clean - c.remaining.Load() }

// propertyStore builds n nodes carrying three indexed properties: one shared by
// all of them, one shared by three quarters, and one distinct per node.
//
// The three exist to reach both residual routes. A residual filter is either
// probed against each candidate or resolved to its own set and merged, chosen
// by which is smaller — so a query driven from the key every node has takes the
// merge, and one driven from the three-quarter key takes the probe. Testing
// only one of those leaves the other's cancellation check unpinned, which is
// how the first version of this file got it wrong.
func propertyStore(t *testing.T, g *graphene.Graph, n int) {
	t.Helper()
	nodes := make([]*store.Node, n)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		if err := g.IndexNodeProperty(id, "bucket", []byte("b")); err != nil {
			t.Fatalf("IndexNodeProperty bucket: %v", err)
		}
		if err := g.IndexNodeProperty(id, "seq", []byte(fmt.Sprintf("%07d", i))); err != nil {
			t.Fatalf("IndexNodeProperty seq: %v", err)
		}
		if i < n*3/4 {
			if err := g.IndexNodeProperty(id, "grp", []byte("g")); err != nil {
				t.Fatalf("IndexNodeProperty grp: %v", err)
			}
		}
	}
}

// wideQuery drives from the value every node shares, so the residual filter's
// own set is no larger than the candidate set and the planner resolves it by
// merge rather than by probe.
func wideQuery() store.NodeQuery {
	return store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpEqual, Value: []byte("b")},
			{Key: "seq", Op: store.PropertyOpContains, Value: []byte("0")},
		},
		FilterMode: store.MatchAll,
	}
}

// probeQuery drives from the three-quarter key, so the residual set is larger
// than the candidates and the planner probes each one instead. Same residual
// filter, different loop underneath it.
func probeQuery() store.NodeQuery {
	return store.NodeQuery{
		Filters: []store.PropertyFilter{
			{Key: "grp", Op: store.PropertyOpEqual, Value: []byte("g")},
			{Key: "seq", Op: store.PropertyOpContains, Value: []byte("0")},
		},
		FilterMode: store.MatchAll,
	}
}

func TestQueryNodesCtx_ADeadContextIsHonouredBeforeAnyWork(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		propertyStore(t, g, 2000)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ids, err := g.QueryNodeIDsCtx(ctx, wideQuery())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("QueryNodeIDsCtx with a cancelled context: want context.Canceled, got %v", err)
		}
		// Not "some results and an error". A caller that checks the error first
		// is fine either way; one that checks the length first must not be
		// handed a plausible-looking short answer.
		if ids != nil {
			t.Errorf("QueryNodeIDsCtx returned %d ids alongside the cancellation", len(ids))
		}

		nodes, err := g.QueryNodesCtx(ctx, wideQuery())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("QueryNodesCtx with a cancelled context: want context.Canceled, got %v", err)
		}
		if nodes != nil {
			t.Errorf("QueryNodesCtx returned %d nodes alongside the cancellation", len(nodes))
		}
	})
}

func TestQueryEdgesCtx_ADeadContextIsHonouredBeforeAnyWork(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		snapChain(t, g, 200)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		q := store.EdgeQuery{Types: []store.EdgeType{store.EdgeTypeContains}}
		ids, err := g.QueryEdgeIDsCtx(ctx, q)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("QueryEdgeIDsCtx with a cancelled context: want context.Canceled, got %v", err)
		}
		if ids != nil {
			t.Errorf("QueryEdgeIDsCtx returned %d ids alongside the cancellation", len(ids))
		}

		edges, err := g.QueryEdgesCtx(ctx, q)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("QueryEdgesCtx with a cancelled context: want context.Canceled, got %v", err)
		}
		if edges != nil {
			t.Errorf("QueryEdgesCtx returned %d edges alongside the cancellation", len(edges))
		}
	})
}

// The assertion that matters most: a query already inside its residual pass can
// still be stopped. Without a check in that loop the call below runs to
// completion and returns a full, correct answer — which is exactly what makes
// the failure invisible without a test that reaches inside.
func TestQueryNodesCtx_CancellationReachesInsideTheResidualPass(t *testing.T) {
	// Both routes, because they are different loops with different checks in
	// them. Each query consults the context enough times that every countdown
	// below lands before it finishes: the first few at a phase boundary, the
	// rest inside the per-candidate or per-value loop, which is the check a
	// coarser implementation would not have.
	routes := map[string]store.NodeQuery{"merge": wideQuery(), "probe": probeQuery()}
	for route, q := range routes {
		for _, clean := range []int64{0, 1, 3, 7, 10} {
			t.Run(fmt.Sprintf("%s/after-%d-checks", route, clean), func(t *testing.T) {
				backends(t, func(t *testing.T, g *graphene.Graph) {
					propertyStore(t, g, 4000)

					ids, err := g.QueryNodeIDsCtx(newCountdownCtx(clean), q)
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("QueryNodeIDsCtx (%s route) cancelled after %d checks: want context.Canceled, got %v (%d ids)",
							route, clean, err, len(ids))
					}
					if ids != nil {
						t.Errorf("QueryNodeIDsCtx returned %d ids alongside the cancellation", len(ids))
					}
				})
			})
		}
	}
}

// The two routes must both be reached, or the loop above tests one of them
// twice and reports success. Asserted rather than assumed: which route the
// planner takes is a cost decision, and a change to probeIsCheaper or to the
// fixture's proportions would silently collapse the pair.
func TestQueryNodesCtx_TheTwoResidualRoutesAreBothExercised(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		propertyStore(t, g, 4000)

		wide, err := g.ExplainNodeQuery(wideQuery())
		if err != nil {
			t.Fatalf("ExplainNodeQuery: %v", err)
		}
		probe, err := g.ExplainNodeQuery(probeQuery())
		if err != nil {
			t.Fatalf("ExplainNodeQuery: %v", err)
		}
		if len(wide.Residuals) != 1 || len(probe.Residuals) != 1 {
			t.Fatalf("each query should leave exactly one residual filter: wide %d, probe %d",
				len(wide.Residuals), len(probe.Residuals))
		}
		if wide.Residuals[0].Probe == probe.Residuals[0].Probe {
			t.Errorf("both queries take the same residual route (probe=%v); the pair tests one loop twice",
				wide.Residuals[0].Probe)
		}
	})
}

// The same loop, uncancelled, must not consult the context per candidate.
//
// This is the cost side of the same change. 4 000 candidates through a residual
// probe would be 4 000 mutex acquisitions on a cancellable context if the check
// were not amortised; at one check per 256 steps it is a handful.
func TestQueryNodesCtx_TheContextIsNotConsultedPerCandidate(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		const n = 4000
		propertyStore(t, g, n)

		const clean = 1 << 30 // never trips
		ctx := newCountdownCtx(clean)
		if _, err := g.QueryNodeIDsCtx(ctx, probeQuery()); err != nil {
			t.Fatalf("QueryNodeIDsCtx: %v", err)
		}
		// Generous: several loops each contribute n/256, plus the unamortised
		// checks at the phase boundaries. A per-candidate check would be well
		// past n.
		if got := ctx.checks(clean); got > n/4 {
			t.Errorf("a %d-candidate query consulted the context %d times; the checks are not amortised", n, got)
		}
	})
}

func TestVerifyIndexesCtx_ADeadContextIsHonoured(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		propertyStore(t, g, 500)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := g.VerifyIndexesCtx(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("VerifyIndexesCtx with a cancelled context: want context.Canceled, got %v", err)
		}

		// And the uncancelled call still answers, so the check has not been
		// wired in front of the verification it is meant to interrupt.
		if err := g.VerifyIndexesCtx(context.Background()); err != nil {
			t.Fatalf("VerifyIndexesCtx: %v", err)
		}
	})
}

// A cancellation reaching inside the verification, rather than only in front of
// it. The countdown is set past the property index's own passes so that it
// trips somewhere in the middle of the walk.
func TestVerifyIndexesCtx_CancellationReachesInsideTheWalk(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		propertyStore(t, g, 4000)

		// The walk consults the context dozens of times end to end, so 3 clean
		// checks leave it deep inside the property index's own passes.
		ctx := newCountdownCtx(3)
		if err := g.VerifyIndexesCtx(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("VerifyIndexesCtx cancelled mid-walk: want context.Canceled, got %v", err)
		}
		// Read-only: whatever it stopped in the middle of, the store is
		// untouched and the check passes when asked again.
		if err := g.VerifyIndexes(); err != nil {
			t.Fatalf("VerifyIndexes after a cancelled one: %v", err)
		}
	})
}

func TestRebuildIndexesCtx_ADeadContextStopsBeforeTheRebuild(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		propertyStore(t, g, 500)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := g.RebuildIndexesCtx(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("RebuildIndexesCtx with a cancelled context: want context.Canceled, got %v", err)
		}

		// The store it refused to rebuild is the store it was given. A rebuild
		// that had begun and stopped would leave postings naming only some of
		// the records that carry a label, which VerifyIndexes reports.
		if err := g.VerifyIndexes(); err != nil {
			t.Fatalf("VerifyIndexes after a refused rebuild: %v", err)
		}

		// And the uncancelled rebuild still works.
		if err := g.RebuildIndexesCtx(context.Background()); err != nil {
			t.Fatalf("RebuildIndexesCtx: %v", err)
		}
		if err := g.VerifyIndexes(); err != nil {
			t.Fatalf("VerifyIndexes after a rebuild: %v", err)
		}
	})
}

// The structural half of a rebuild is not interruptible, and this is what that
// promise is worth: however late the cancellation lands, the indexes still
// describe every record.
//
// Run across a range of countdowns rather than one, because "the cancellation
// landed inside the rebuild" is exactly the case a single well-chosen number
// would miss.
func TestRebuildIndexesCtx_NoCancellationCanLeaveAPartialRebuild(t *testing.T) {
	for _, clean := range []int64{0, 1, 2, 3, 5, 8, 13, 21} {
		t.Run(fmt.Sprintf("after-%d-checks", clean), func(t *testing.T) {
			backends(t, func(t *testing.T, g *graphene.Graph) {
				propertyStore(t, g, 1500)

				err := g.RebuildIndexesCtx(newCountdownCtx(clean))
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("RebuildIndexesCtx: %v", err)
				}
				// Whether it finished or was cancelled, the structure is whole.
				// The only fault a cancelled sweep can leave is a property entry
				// outliving its entity, and this store has none — every node it
				// indexed is still live.
				if err := g.VerifyIndexes(); err != nil {
					t.Fatalf("VerifyIndexes after a rebuild cancelled at %d checks: %v", clean, err)
				}
			})
		})
	}
}

// manyKeyStore builds a store whose property index dwarfs its record count:
// few nodes, many keys each. That proportion is the point — it makes the index's
// contribution to a verification separable from the store's own, which is what
// the test below measures.
func manyKeyStore(t *testing.T, g *graphene.Graph, nodes, keys int) {
	t.Helper()
	recs := make([]*store.Node, nodes)
	for i := range recs {
		recs[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(recs)
	if err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		for k := 0; k < keys; k++ {
			if err := g.IndexNodeProperty(id, fmt.Sprintf("k%02d", k), []byte(fmt.Sprintf("%07d", i))); err != nil {
				t.Fatalf("IndexNodeProperty: %v", err)
			}
		}
	}
}

// The property index is most of what a verification walks, and it is behind its
// own call — so passing it an uncancellable context would leave the expensive
// half of the check running after the caller gave up, while the cheap half
// still reported a cancellation and made it look like the whole thing stopped.
//
// Counting is the only way to see that from outside, and the counting only
// works on a store where the index dominates: with a property per node the
// store's own passes contribute more checks than the index does, and a
// threshold that tolerates them tolerates the fault too. That is why this uses
// its own fixture rather than the shared one — the first version of this test
// did not, and the mutant walked past it.
func TestVerifyIndexesCtx_ThePropertyIndexIsPartOfWhatCanBeCancelled(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		const nodes, keys = 500, 40
		manyKeyStore(t, g, nodes, keys)

		const clean = 1 << 30 // never trips
		ctx := newCountdownCtx(clean)
		if err := g.VerifyIndexesCtx(ctx); err != nil {
			t.Fatalf("VerifyIndexesCtx: %v", err)
		}
		// The store's own passes walk ~3 * nodes entries, so they can account
		// for at most a handful of checks at one per 256 steps. The index walks
		// nodes * keys, which is 40 times that.
		const floor = 4 * nodes / 256
		if got := ctx.checks(clean); got <= floor {
			t.Errorf("a verification over %d property entries consulted the context %d times (floor %d); "+
				"the property index is not being handed the caller's context", nodes*keys, got, floor)
		}
	})
}

// Cancellation must be distinguishable from an inconsistency. A caller that
// treats every non-nil error from VerifyIndexes as corruption would otherwise
// take a cancelled check as a reason to restore from backup.
func TestVerifyIndexesCtx_CancellationIsNotAnInconsistency(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		propertyStore(t, g, 1000)

		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		err := g.VerifyIndexesCtx(ctx)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("VerifyIndexesCtx past its deadline: want DeadlineExceeded or nil, got %v", err)
		}
	})
}
