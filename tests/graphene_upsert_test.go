package graphene_test

// Upsert: create-or-replace keyed on a declared-unique property.
//
// The requirement behind all of it is one sentence: re-ingesting a source that
// has already been ingested must produce the same graph, not a second copy of
// it. Everything here is a way of asking whether that holds — for one node, for
// a whole transaction, across a reopen, and when two writers race for the same
// key.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func TestUpsert_CreatesThenUpdates(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		key := []byte("ver:9f3a")

		id, created, err := g.UpsertNode("k", key,
			&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte("v1")},
			map[string][]byte{"state": []byte("discovered")})
		if err != nil {
			t.Fatalf("first upsert: %v", err)
		}
		if !created {
			t.Fatal("first upsert reported created=false")
		}

		again, created, err := g.UpsertNode("k", key,
			&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte("v2")},
			map[string][]byte{"state": []byte("extracted")})
		if err != nil {
			t.Fatalf("second upsert: %v", err)
		}
		if created {
			t.Fatal("second upsert reported created=true")
		}
		if again != id {
			t.Fatalf("second upsert returned %d, want %d", again, id)
		}

		n, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if n != 1 {
			t.Fatalf("two upserts of one key produced %d nodes", n)
		}

		// The record was replaced, not merged.
		got, err := g.NodeByProperty("k", key)
		if err != nil {
			t.Fatalf("NodeByProperty: %v", err)
		}
		if string(got.Properties) != "v2" {
			t.Fatalf("properties are %q, want v2", got.Properties)
		}

		// And so were the entries the caller named. The old value must not still
		// match, or the pipeline queue this field represents never drains.
		stale, err := g.NodesByProperty("state", []byte("discovered"))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(stale) != 0 {
			t.Fatalf("the superseded state value still matches: %v", stale)
		}
		fresh, err := g.NodesByProperty("state", []byte("extracted"))
		if err != nil {
			t.Fatalf("NodesByProperty: %v", err)
		}
		if len(fresh) != 1 || fresh[0] != id {
			t.Fatalf("the new state value resolves to %v, want [%d]", fresh, id)
		}
	})
}

func TestUpsert_RefusesAnUndeclaredKey(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		_, _, err := g.UpsertNode("k", []byte("v"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}, nil)
		if !errors.Is(err, store.ErrKeyNotUnique) {
			t.Fatalf("upsert on an undeclared key: got %v, want ErrKeyNotUnique", err)
		}
	})
}

// The ID comes back at buffer time precisely so it can be used as an endpoint
// before the transaction commits — that is the whole reason the key is resolved
// early rather than at commit.
func TestUpsert_IDIsUsableAsAnEndpointWithinTheTransaction(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}

		tx := g.Begin()
		ver, _ := tx.UpsertNode("k", []byte("ver:1"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}, nil)
		perm, _ := tx.UpsertNode("k", []byte("perm:INTERNET"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}, nil)
		tx.AddEdge(&store.Edge{Src: ver, Dst: perm, Labels: []store.EdgeType{store.EdgeTypeTaggedWith}})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		out, err := g.EdgesOf(ver, store.DirectionOutbound, nil)
		if err != nil {
			t.Fatalf("EdgesOf: %v", err)
		}
		if len(out) != 1 || out[0].Dst != perm {
			t.Fatalf("the edge did not connect the upserted pair: %v", out)
		}
	})
}

// Upserting one key twice in a transaction is one entity, not a conflict. The
// index cannot say so — nothing is registered there yet — so the transaction has
// to remember what it has already resolved.
func TestUpsert_SameKeyTwiceInOneTransactionIsOneNode(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		tx := g.Begin()
		a, createdA := tx.UpsertNode("k", []byte("perm:INTERNET"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("first")}, nil)
		b, createdB := tx.UpsertNode("k", []byte("perm:INTERNET"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte("second")}, nil)
		if a != b {
			t.Fatalf("one key resolved to two ids in one transaction: %d and %d", a, b)
		}
		if !createdA || createdB {
			t.Fatalf("created flags are (%v, %v), want (true, false)", createdA, createdB)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		n, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if n != 1 {
			t.Fatalf("produced %d nodes, want 1", n)
		}
		got, err := g.NodeByProperty("k", []byte("perm:INTERNET"))
		if err != nil {
			t.Fatalf("NodeByProperty: %v", err)
		}
		if string(got.Properties) != "second" {
			t.Fatalf("properties are %q; the later call should win", got.Properties)
		}
	})
}

func TestUpsertEdge_CreatesThenUpdatesAndPinsEndpoints(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueEdgeProperty("ek"); err != nil {
			t.Fatalf("DeclareUniqueEdgeProperty: %v", err)
		}
		src, dst := twoNodes(t, g)
		other, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}

		id, created, err := g.UpsertEdge("ek", []byte("uses"),
			&store.Edge{Src: src, Dst: dst, Labels: []store.EdgeType{store.EdgeTypeContains}}, nil)
		if err != nil {
			t.Fatalf("first upsert: %v", err)
		}
		if !created {
			t.Fatal("first edge upsert reported created=false")
		}

		again, created, err := g.UpsertEdge("ek", []byte("uses"),
			&store.Edge{Src: other, Dst: other, Labels: []store.EdgeType{store.EdgeTypeReuse}}, nil)
		if err != nil {
			t.Fatalf("second upsert: %v", err)
		}
		if created || again != id {
			t.Fatalf("second edge upsert: created=%v id=%d, want false and %d", created, again, id)
		}

		e, err := g.GetEdge(id)
		if err != nil {
			t.Fatalf("GetEdge: %v", err)
		}
		if e.Src != src || e.Dst != dst {
			t.Fatalf("endpoints moved to (%d,%d), want (%d,%d)", e.Src, e.Dst, src, dst)
		}
		if !e.HasLabel(store.EdgeTypeReuse) {
			t.Fatal("the second upsert did not replace the labels")
		}
		count, err := g.EdgeCount()
		if err != nil {
			t.Fatalf("EdgeCount: %v", err)
		}
		if count != 1 {
			t.Fatalf("two upserts of one edge key produced %d edges", count)
		}
	})
}

// A transaction whose key moved under it is refused whole, and the ID it already
// handed the caller is never used. This is the write half of conflict detection:
// a second writer cannot be allowed to quietly create a duplicate.
func TestUpsert_RefusesWhenTheKeyMovedUnderTheTransaction(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}

		// Buffer an upsert that resolved "absent", then let another writer take
		// the key before it commits.
		tx := g.Begin()
		tx.UpsertNode("k", []byte("contested"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeCase}}, nil)

		winner, _, err := g.UpsertNode("k", []byte("contested"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("winner")}, nil)
		if err != nil {
			t.Fatalf("competing upsert: %v", err)
		}

		if err := tx.Commit(); !errors.Is(err, store.ErrWriteConflict) {
			t.Fatalf("Commit: got %v, want ErrWriteConflict", err)
		}

		n, err := g.NodeCount()
		if err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if n != 1 {
			t.Fatalf("the loser of the race left %d nodes, want 1", n)
		}

		// The retry converges on what the winner wrote.
		retry := g.Begin()
		id, created := retry.UpsertNode("k", []byte("contested"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeCase}, Properties: []byte("retried")}, nil)
		if err := retry.Commit(); err != nil {
			t.Fatalf("retry: %v", err)
		}
		if created || id != winner {
			t.Fatalf("retry produced created=%v id=%d, want false and %d", created, id, winner)
		}
	})
}

// The requirement, stated as a test: ingest an APK-shaped subgraph twice and the
// graph must be identical — same counts, same records, same index entries.
func TestUpsert_ReIngestingProducesAnIdenticalGraph(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		for _, key := range []string{"k"} {
			if err := g.DeclareUniqueProperty(key); err != nil {
				t.Fatalf("DeclareUniqueProperty(%s): %v", key, err)
			}
		}
		if err := g.DeclareUniqueEdgeProperty("ek"); err != nil {
			t.Fatalf("DeclareUniqueEdgeProperty: %v", err)
		}

		// Two "APKs" sharing hub nodes, which is where duplication would show
		// first: a permission referenced by every app is the node a broken
		// upsert makes one copy of per app.
		ingest := func(apk string) {
			t.Helper()
			tx := g.Begin()
			ver, _ := tx.UpsertNode("k", []byte("ver:"+apk),
				&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte(apk)},
				map[string][]byte{"state": []byte("analyzed")})
			for _, perm := range []string{"INTERNET", "CAMERA", "READ_SMS"} {
				p, _ := tx.UpsertNode("k", []byte("perm:"+perm),
					&store.Node{Labels: []store.NodeType{store.NodeTypeTag}, Properties: []byte(perm)}, nil)
				tx.UpsertEdge("ek", []byte("uses:"+apk+":"+perm),
					&store.Edge{Src: ver, Dst: p, Labels: []store.EdgeType{store.EdgeTypeTaggedWith}}, nil)
			}
			for i := 0; i < 25; i++ {
				m, _ := tx.UpsertNode("k", []byte(fmt.Sprintf("meth:%s:%d", apk, i)),
					&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}, nil)
				tx.UpsertEdge("ek", []byte(fmt.Sprintf("contains:%s:%d", apk, i)),
					&store.Edge{Src: ver, Dst: m, Labels: []store.EdgeType{store.EdgeTypeContains}}, nil)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("ingest %s: %v", apk, err)
			}
		}

		ingest("aaa")
		ingest("bbb")
		first := fingerprint(t, g)

		// Every hunt re-run, twice over.
		ingest("aaa")
		ingest("bbb")
		ingest("aaa")
		if got := fingerprint(t, g); got != first {
			t.Fatalf("re-ingest changed the graph:\n before: %s\n  after: %s", first, got)
		}

		// And the hubs really are shared rather than merely counted right.
		perm, err := g.NodeByProperty("k", []byte("perm:INTERNET"))
		if err != nil {
			t.Fatalf("NodeByProperty: %v", err)
		}
		in, err := g.EdgesOf(perm.ID, store.DirectionInbound, nil)
		if err != nil {
			t.Fatalf("EdgesOf: %v", err)
		}
		if len(in) != 2 {
			t.Fatalf("the shared permission has %d inbound edges, want 2 (one per apk)", len(in))
		}
	})
}

// fingerprint summarises everything a re-ingest must leave alone: the record
// counts, and every indexed (id, key, value) triple.
func fingerprint(t *testing.T, g *graphene.Graph) string {
	t.Helper()
	nc, err := g.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	ec, err := g.EdgeCount()
	if err != nil {
		t.Fatalf("EdgeCount: %v", err)
	}

	var entries []string
	g.ForEachNodeProperty(func(id store.NodeID, key string, value []byte) bool {
		entries = append(entries, fmt.Sprintf("n/%d/%s/%s", id, key, value))
		return true
	})
	g.ForEachEdgeProperty(func(id store.EdgeID, key string, value []byte) bool {
		entries = append(entries, fmt.Sprintf("e/%d/%s/%s", id, key, value))
		return true
	})
	sortStringsInPlace(entries)

	h := fmt.Sprintf("nodes=%d edges=%d entries=%d", nc, ec, len(entries))
	for _, e := range entries {
		h += "\n" + e
	}
	return h
}

func sortStringsInPlace(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// The same requirement, across a restart: the store must resolve keys written
// before the reopen, or a scheduled hunt duplicates everything each time the
// process is restarted.
func TestUpsert_ReIngestingAfterAReopenProducesAnIdenticalGraph(t *testing.T) {
	dir := t.TempDir()

	ingest := func(g *graphene.Graph) {
		t.Helper()
		tx := g.Begin()
		ver, _ := tx.UpsertNode("k", []byte("ver:aaa"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}, nil)
		p, _ := tx.UpsertNode("k", []byte("perm:INTERNET"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}, nil)
		tx.UpsertEdge("ek", []byte("uses:aaa:INTERNET"),
			&store.Edge{Src: ver, Dst: p, Labels: []store.EdgeType{store.EdgeTypeTaggedWith}}, nil)
		if err := tx.Commit(); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	open := func() *graphene.Graph {
		t.Helper()
		g, err := graphene.Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		if err := g.DeclareUniqueEdgeProperty("ek"); err != nil {
			t.Fatalf("DeclareUniqueEdgeProperty: %v", err)
		}
		return g
	}

	g := open()
	ingest(g)
	want := fingerprint(t, g)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for round := 0; round < 3; round++ {
		re := open()
		ingest(re)
		if got := fingerprint(t, re); got != want {
			t.Fatalf("round %d changed the graph:\n before: %s\n  after: %s", round, want, got)
		}
		if err := re.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// And once more with a compaction in between, so the records are read back
	// from the image rather than replayed from the log.
	c := open()
	if err := c.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	ingest(c)
	if got := fingerprint(t, c); got != want {
		t.Fatalf("re-ingest after a compaction changed the graph:\n before: %s\n  after: %s", want, got)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
