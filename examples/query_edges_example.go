package main

import (
	"fmt"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Example 25 focuses on typed edge queries with endpoint constraints,
// property predicates, and pagination.
func example25_QueryEdgesAdvanced() {
	fmt.Println("--- Example 25: Advanced edge query APIs ---")

	g := graphene.NewInMemory()

	a, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	b, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	c, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})

	e1, _ := g.AddEdge(&store.Edge{Src: a, Dst: b, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	e2, _ := g.AddEdge(&store.Edge{Src: a, Dst: c, Labels: []store.EdgeType{store.EdgeTypeSimilarTo}})
	_, _ = g.AddEdge(&store.Edge{Src: b, Dst: c, Labels: []store.EdgeType{store.EdgeTypeReuse}})

	g.IndexEdgeProperty(e1, "bucket", []byte("sim-high"))
	g.IndexEdgeProperty(e2, "bucket", []byte("sim-low"))

	hits, _ := g.QueryEdgeIDs(store.EdgeQuery{
		Types:  []store.EdgeType{store.EdgeTypeSimilarTo},
		SrcIDs: []store.NodeID{a},
		Filters: []store.PropertyFilter{
			{Key: "bucket", Op: store.PropertyOpPrefix, Value: []byte("sim-")},
		},
		Order:  store.QueryOrderDesc,
		Offset: 0,
		Limit:  1,
	})
	fmt.Printf("  QueryEdgeIDs (desc page): %v\n", hits)

	fmt.Println()
}
