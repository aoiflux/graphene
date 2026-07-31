package main

import (
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
// engine knows that "status" changed — and the public index API is additive, so
// a value registered before an update keeps matching afterwards. The result is
// not a slow query or a missing row: NodesByProperty returns an entity that no
// longer holds the value it was found by, and the query planner trusts that
// answer.
//
// UpdateNodeIndexed is the fix. It updates the entity and replaces its index
// entries in one step, so the index cannot disagree with the entity. Prefer it
// for any entity with indexed properties; plain UpdateNode is the right call
// only for entities that carry none.
func exampleMutation3_ReclassifyAndReindex() {
	fmt.Println("--- Mutation 3: Reclassify; keeping the index honest ---")

	// --- The trap: UpdateNode, then register the new value ---
	//
	// This reads as though it works. It does not: the old value is still in the
	// index, so the node now matches BOTH statuses.
	bad := graphene.NewInMemory()
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

	fmt.Printf("  UpdateNode        -> suspect:%d confirmed:%d  <- WRONG: 'suspect' is stale and still matches\n",
		countHits(bad, "status", "suspect"), countHits(bad, "status", "confirmed"))
	bad.Close()

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
