package main

import (
	"fmt"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Example 26 focuses on relation queries, including ID-first retrieval for
// service-style pagination.
func example26_QueryRelationsAdvanced() {
	fmt.Println("--- Example 26: Advanced relation query APIs ---")

	g := graphene.NewInMemory()

	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	e1, _ := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	e2, _ := g.AddEdge(&store.Edge{Src: c, Dst: a, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	g.IndexEdgeProperty(e1, "kind", []byte("near"))
	g.IndexEdgeProperty(e2, "kind", []byte("near"))

	relIDs, _ := g.QueryRelationIDs(store.RelationQuery{
		Anchors:   []store.NodeID{a},
		Direction: store.DirectionBoth,
		EdgeTypes: []store.EdgeType{store.EdgeTypeSimilarTo},
		Filters: []store.PropertyFilter{
			{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("near")},
		},
		Order:  store.QueryOrderDesc,
		Offset: 0,
		Limit:  10,
	})
	fmt.Printf("  QueryRelationIDs (both): %v\n", relIDs)

	rels, _ := g.QueryRelations(store.RelationQuery{
		Anchors:   []store.NodeID{a},
		Direction: store.DirectionBoth,
		EdgeTypes: []store.EdgeType{store.EdgeTypeSimilarTo},
		Filters: []store.PropertyFilter{
			{Key: "kind", Op: store.PropertyOpEqual, Value: []byte("near")},
		},
		Order: store.QueryOrderAsc,
	})
	fmt.Printf("  QueryRelations hydrated count: %d\n", len(rels))

	fmt.Println()
}
