package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// ----------------------------------------------------------------------------
// Mutation Example 1 — Editing nodes and edges in place
// ----------------------------------------------------------------------------
//
// UpdateNode / UpdateEdge replace an existing entity's labels and properties by
// ID. Edge endpoints (Src/Dst) are immutable — any Src/Dst set on the struct
// passed to UpdateEdge is ignored. To move an edge, delete it and add a new one.
func exampleMutation1_EditEntities() {
	fmt.Println("--- Mutation 1: Edit nodes and edges in place ---")

	g := graphene.NewInMemory()
	defer g.Close()

	art, _ := g.AddNode(&store.Node{
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte(`{"score":10}`),
	})
	other, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	eid, _ := g.AddEdge(&store.Edge{
		Src:    art,
		Dst:    other,
		Labels: []store.EdgeType{store.EdgeTypeSimilarTo},
		Weight: 0.40,
	})

	// Rewrite the node payload (e.g. after re-scoring an artefact).
	if err := g.UpdateNode(&store.Node{
		ID:         art,
		Labels:     []store.NodeType{store.NodeTypeMicroArtefact},
		Properties: []byte(`{"score":92}`),
	}); err != nil {
		log.Fatalf("UpdateNode: %v", err)
	}

	// Re-weight and re-type the edge. The Src/Dst below are deliberately wrong to
	// show they are ignored — the edge keeps its original endpoints.
	if err := g.UpdateEdge(&store.Edge{
		ID:     eid,
		Src:    999, // ignored
		Dst:    999, // ignored
		Labels: []store.EdgeType{store.EdgeTypeReuse},
		Weight: 0.95,
	}); err != nil {
		log.Fatalf("UpdateEdge: %v", err)
	}

	n, _ := g.GetNode(art)
	e, _ := g.GetEdge(eid)
	fmt.Printf("  Node payload now: %s\n", string(n.Properties))
	fmt.Printf("  Edge now: type=%s weight=%.2f src=%d dst=%d (endpoints unchanged)\n",
		e.Labels[0], e.Weight, e.Src, e.Dst)
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Mutation Example 2 — Deleting edges and cascading node deletes
// ----------------------------------------------------------------------------
//
// DeleteEdge removes one relationship. DeleteNode also removes every edge
// incident to that node, so the graph never keeps an edge that points at a
// missing node. Deletes are durable across restart on the disk backend.
func exampleMutation2_DeleteAndCascade() {
	fmt.Println("--- Mutation 2: Delete edges and cascade node deletes ---")

	dir, err := os.MkdirTemp("", "graphene-mut2-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var caseID store.NodeID

	// Session 1: build a small case, then delete a file (cascades its edges).
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}

		caseID, _ = g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeCase}})
		file1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		file2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		a1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		a2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

		g.AddEdge(&store.Edge{Src: file1, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
		g.AddEdge(&store.Edge{Src: file2, Dst: caseID, Labels: []store.EdgeType{store.EdgeTypeBelongsTo}})
		g.AddEdge(&store.Edge{Src: file1, Dst: a1, Labels: []store.EdgeType{store.EdgeTypeContains}})
		g.AddEdge(&store.Edge{Src: file2, Dst: a2, Labels: []store.EdgeType{store.EdgeTypeContains}})

		nc, _ := g.NodeCount()
		ec, _ := g.EdgeCount()
		fmt.Printf("  Built — nodes: %d  edges: %d\n", nc, ec)

		// Deleting file2 removes it AND its two incident edges (belongs + contains).
		if err := g.DeleteNode(file2); err != nil {
			log.Fatalf("DeleteNode: %v", err)
		}

		nc, _ = g.NodeCount()
		ec, _ = g.EdgeCount()
		fmt.Printf("  After DeleteNode(file2) — nodes: %d  edges: %d (2 edges cascaded)\n", nc, ec)
		g.Close()
	}

	// Session 2: reopen — the delete persisted via the WAL tombstones.
	{
		g, err := graphene.Open(dir)
		if err != nil {
			log.Fatal(err)
		}
		defer g.Close()

		nc, _ := g.NodeCount()
		ec, _ := g.EdgeCount()
		fmt.Printf("  Reopened — nodes: %d  edges: %d (delete survived restart)\n", nc, ec)

		files, _ := g.NodesByType(store.NodeTypeEvidenceFile)
		fmt.Printf("  EvidenceFile nodes remaining: %d\n", len(files))
		_ = caseID
	}
	fmt.Println()
}

// ----------------------------------------------------------------------------
// Mutation Example 3 — Reclassify a node without leaving the index lying
// ----------------------------------------------------------------------------
//
// The engine cannot maintain the property index across UpdateNode. Indexed
// values are caller-encoded and decoupled from Properties, so nothing in the
// engine knows that "status" changed — and index entries are additive, so a
// value registered before an update keeps matching afterwards. The result is not
// a slow query or a missing row: NodesByProperty returns an entity that no
// longer holds the value it was found by, and the query planner trusts that
// answer.
//
// Since v0.5.0 the engine refuses rather than doing that. ReindexReject is the
// default policy, and UpdateNode on an entity carrying index entries returns
// store.ErrIndexedPropertiesRequired — a loud failure in place of a silent wrong
// answer. The two calls that do know what the update did are the way through:
// UpdateNodeIndexed states the whole desired index state, UpdateNodePartialIndex
// states only the keys that moved.
func exampleMutation3_ReclassifyAndReindex() {
	fmt.Println("--- Mutation 3: Reclassify; keeping the index honest ---")

	// --- What the old default did, shown by asking for it explicitly ---
	//
	// This reads as though it works. It does not: the old value is still in the
	// index, so the node now matches BOTH statuses.
	bad := graphene.NewInMemory()
	bad.SetReindexPolicy(store.ReindexKeep)
	id, _ := bad.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	_ = bad.IndexNodeProperty(id, "status", []byte("suspect"))

	if err := bad.UpdateNode(&store.Node{ID: id, Labels: []store.NodeType{store.NodeTypeMicroArtefact, store.NodeTypeTag}}); err != nil {
		log.Fatalf("UpdateNode: %v", err)
	}
	_ = bad.IndexNodeProperty(id, "status", []byte("confirmed"))

	// The re-classification itself is reflected in type lookups immediately.
	tags, _ := bad.NodesByType(store.NodeTypeTag)
	arts, _ := bad.NodesByType(store.NodeTypeMicroArtefact)
	fmt.Printf("  Node appears under Tag(%d) and MicroArtefact(%d) lookups\n", len(tags), len(arts))

	fmt.Printf("  ReindexKeep       -> suspect:%d confirmed:%d  <- WRONG: 'suspect' is stale and still matches\n",
		countHits(bad, "status", "suspect"), countHits(bad, "status", "confirmed"))
	bad.Close()

	// --- What the default does now: refuse ---
	strict := graphene.NewInMemory()
	sid, _ := strict.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	_ = strict.IndexNodeProperty(sid, "status", []byte("suspect"))

	err := strict.UpdateNode(&store.Node{ID: sid, Labels: []store.NodeType{store.NodeTypeTag}})
	fmt.Printf("  ReindexReject     -> UpdateNode refused: %v\n", errors.Is(err, store.ErrIndexedPropertiesRequired))
	strict.Close()

	// --- The fix: state the whole desired index state in one call ---
	good := graphene.NewInMemory()
	defer good.Close()

	id2, _ := good.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	_ = good.IndexNodeProperty(id2, "status", []byte("suspect"))

	if err := good.UpdateNodeIndexed(
		&store.Node{ID: id2, Labels: []store.NodeType{store.NodeTypeMicroArtefact, store.NodeTypeTag}},
		map[string][]byte{"status": []byte("confirmed")},
	); err != nil {
		log.Fatalf("UpdateNodeIndexed: %v", err)
	}

	fmt.Printf("  UpdateNodeIndexed -> suspect:%d confirmed:%d  <- index agrees with the node\n",
		countHits(good, "status", "suspect"), countHits(good, "status", "confirmed"))

	// --- And when only one field moved, name only that field ---
	//
	// UpdateNodeIndexed would have needed "case" listed again just to keep it.
	// Forgetting it is how an untouched key goes missing.
	_ = good.IndexNodeProperty(id2, "case", []byte("c-17"))
	if err := good.UpdateNodePartialIndex(
		&store.Node{ID: id2, Labels: []store.NodeType{store.NodeTypeTag}},
		map[string][]byte{"status": []byte("closed")},
	); err != nil {
		log.Fatalf("UpdateNodePartialIndex: %v", err)
	}
	fmt.Printf("  PartialIndex      -> confirmed:%d closed:%d case:%d  <- only 'status' moved\n",
		countHits(good, "status", "confirmed"), countHits(good, "status", "closed"),
		countHits(good, "case", "c-17"))

	// Deleting the node purges every index entry it had, under either approach.
	if err := good.DeleteNode(id2); err != nil {
		log.Fatalf("DeleteNode: %v", err)
	}
	fmt.Printf("  after DeleteNode  -> suspect:%d confirmed:%d  (all entries purged)\n",
		countHits(good, "status", "suspect"), countHits(good, "status", "confirmed"))
	fmt.Println()
}

func countHits(g *graphene.Graph, key, val string) int {
	hits, _ := g.NodesByProperty(key, []byte(val))
	return len(hits)
}

// ----------------------------------------------------------------------------
// Mutation Example 4 — Ingesting the same source twice
// ----------------------------------------------------------------------------
//
// AddNode always adds. Run an ingest twice — a re-run after a crash, a hunt
// re-executed with a better rule — and the graph holds two nodes for one thing
// with nothing to say which is current. The engine could not have known they
// were the same, because the identity lives in the caller's data.
//
// A unique property key is how that identity is declared, and UpsertNode is the
// verb that uses it: resolve the key to the entity already carrying it, or
// create one. So a re-ingest of an unchanged source is a no-op, and a re-ingest
// of a changed one is an update.
func exampleMutation4_IdempotentIngest() {
	fmt.Println("--- Mutation 4: Ingesting the same source twice ---")

	const key = "k" // the caller's natural key: "ver:<sha256>", "perm:<name>", ...

	// --- What AddNode does with a re-run ---
	dup := graphene.NewInMemory()
	for range 2 {
		id, _ := dup.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}})
		_ = dup.IndexNodeProperty(id, key, []byte("ver:9f3a"))
	}
	st, _ := dup.Stats()
	fmt.Printf("  AddNode x2     -> %d nodes for one source  <- WRONG: a duplicate, not a re-ingest\n", st.NodeCount)
	dup.Close()

	// --- The same run, with the key declared ---
	g := graphene.NewInMemory()
	defer g.Close()

	// Declare at every Open: idempotent, and it validates the data already there
	// rather than trusting a flag. A graph that violates the constraint is
	// refused with *store.UniqueViolationsError naming every offending value.
	if err := g.DeclareUniqueProperty(key); err != nil {
		log.Fatalf("DeclareUniqueProperty: %v", err)
	}

	for _, state := range []string{"extracted", "analyzed"} {
		id, created, err := g.UpsertNode(key, []byte("ver:9f3a"),
			&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}},
			map[string][]byte{"state": []byte(state)})
		if err != nil {
			log.Fatalf("UpsertNode: %v", err)
		}
		fmt.Printf("  UpsertNode     -> id=%d created=%v state=%q\n", id, created, state)
	}

	st, _ = g.Stats()
	fmt.Printf("  after x2       -> %d node, extracted:%d analyzed:%d  <- the entry moved; it did not accumulate\n",
		st.NodeCount, countHits(g, "state", "extracted"), countHits(g, "state", "analyzed"))

	// NodeByProperty is what a unique key buys on the read side: one entity or a
	// not-found, rather than a set to disambiguate.
	found, err := g.NodeByProperty(key, []byte("ver:9f3a"))
	if err != nil {
		log.Fatalf("NodeByProperty: %v", err)
	}
	fmt.Printf("  NodeByProperty -> id=%d\n", found.ID)

	// --- A whole source in one commit ---
	//
	// tx.UpsertNode hands the ID back before commit, so an edge can name it. The
	// key is re-read under the write lock at commit; if another writer took it in
	// between, the whole transaction is refused with store.ErrWriteConflict
	// rather than quietly creating a second entity.
	tx := g.Begin()
	ver, _ := tx.UpsertNode(key, []byte("ver:9f3a"),
		&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}, nil)
	perm, _ := tx.UpsertNode(key, []byte("perm:INTERNET"),
		&store.Node{Labels: []store.NodeType{store.NodeTypeTag}}, nil)
	tx.AddEdge(&store.Edge{Src: ver, Dst: perm, Labels: []store.EdgeType{store.EdgeTypeContains}})
	if err := tx.Commit(); err != nil {
		log.Fatalf("Commit: %v", err)
	}

	st, _ = g.Stats()
	fmt.Printf("  one Tx         -> %d nodes, %d edge; the re-upserted source kept id=%d\n",
		st.NodeCount, st.EdgeCount, ver)
	fmt.Println()
}
