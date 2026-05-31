package main

import (
	"fmt"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

// Example 24 focuses on typed node queries with boolean composition, order,
// pagination, and custom type selectors.
func example24_QueryNodesAdvanced() {
	fmt.Println("--- Example 24: Advanced node query APIs ---")

	g := graphene.NewInMemory()

	customType := store.CustomNodeType(7)
	n1, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	n2, _ := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
	_, _ = g.AddNode(&store.Node{Labels: []store.NodeType{customType}})

	g.IndexNodeProperty(n1, "size", []byte("100"))
	g.IndexNodeProperty(n2, "size", []byte("200"))
	g.IndexNodeProperty(n1, "family", []byte("artefact"))
	g.IndexNodeProperty(n2, "family", []byte("sample"))

	hits, _ := g.QueryNodeIDs(store.NodeQuery{
		Types: []store.NodeType{store.NodeTypeMicroArtefact},
		Filters: []store.PropertyFilter{
			{Key: "size", Op: store.PropertyOpGreaterThanOrEqual, Value: []byte("100")},
			{Key: "family", Op: store.PropertyOpContains, Value: []byte("artefact")},
		},
		FilterMode: store.MatchAll,
		Order:      store.QueryOrderDesc,
		Offset:     0,
		Limit:      10,
	})
	fmt.Printf("  QueryNodeIDs (AND + desc): %v\n", hits)

	customHits, _ := g.NodesByTypeSelector("custom:7")
	fmt.Printf("  NodesByTypeSelector(custom:7): %v\n", customHits)

	fmt.Println()
}
