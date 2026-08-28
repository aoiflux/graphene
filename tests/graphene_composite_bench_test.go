//go:build stress

// What a composite index is worth, and what declaring one costs.
//
// A conjunction of equality filters is answered today by driving from the more
// selective one and eliminating against the other. That is the right choice
// among the sets available, and it costs the driving set's size however small
// the answer is — so the work is set by the *better* of the two keys while the
// answer is set by both.
//
// The A/B here is declared against undeclared, in one build. That is deliberate:
// an undeclared store takes exactly the plan a HEAD build takes, because the
// planner reaches the composite case only when a declaration matches. Comparing
// the two arms therefore isolates the feature rather than the whole diff, and it
// does not need a benchmark that compiles on both sides of an API addition.
//
// The write side is measured too. A declaration is not free — every registration
// on a member key files the entity into the composite as well — and a feature
// that quietly taxes ingest to speed up queries should say so in a number.

package graphene_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	graphene "github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

const compositeBenchNodes = 100_000

// The two member keys are deliberately near-useless alone. 11 and 13 are coprime
// so the conjunction is a real intersection rather than one key implying the
// other, and each key leaves about a twelfth of the graph standing while the
// pair leaves about a hundred and forty-third.
const (
	compositeBenchCases   = 11
	compositeBenchBuckets = 13
)

var (
	compositeBenchOnce  sync.Once
	compositeBenchDecl  *graphene.Graph // "case"+"bucket" declared
	compositeBenchPlain *graphene.Graph // nothing declared
)

func compositeBenchKeys(i int) (caseV, bucketV []byte) {
	return []byte(fmt.Sprintf("case-%02d", i%compositeBenchCases)),
		[]byte(fmt.Sprintf("bucket-%02d", i%compositeBenchBuckets))
}

// compositeBenchFill builds one arm's graph. declare controls whether the
// composite exists; everything else is identical, so the two arms differ only in
// the plan the same query gets.
func compositeBenchFill(b *testing.B, dir string, declare bool) *graphene.Graph {
	b.Helper()
	g, err := graphene.Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	if declare {
		if err := g.DeclareCompositeProperties([]string{"case", "bucket"}); err != nil {
			b.Fatalf("DeclareCompositeProperties: %v", err)
		}
	}
	nodes := make([]*store.Node, compositeBenchNodes)
	for i := range nodes {
		nodes[i] = &store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}}
	}
	ids, err := g.AddNodes(nodes)
	if err != nil {
		b.Fatalf("AddNodes: %v", err)
	}
	for i, id := range ids {
		c, bk := compositeBenchKeys(i)
		if err := g.IndexNodeProperties(id, map[string][]byte{"case": c, "bucket": bk}); err != nil {
			b.Fatalf("IndexNodeProperties: %v", err)
		}
	}
	if err := g.Compact(); err != nil {
		b.Fatalf("Compact: %v", err)
	}
	return g
}

func compositeBenchFixture(b *testing.B) (declared, plain *graphene.Graph) {
	b.Helper()
	compositeBenchOnce.Do(func() {
		d1, err := os.MkdirTemp("", "graphene-comp-decl-")
		if err != nil {
			b.Fatalf("MkdirTemp: %v", err)
		}
		d2, err := os.MkdirTemp("", "graphene-comp-plain-")
		if err != nil {
			b.Fatalf("MkdirTemp: %v", err)
		}
		compositeBenchDecl = compositeBenchFill(b, d1, true)
		compositeBenchPlain = compositeBenchFill(b, d2, false)
	})
	if compositeBenchDecl == nil || compositeBenchPlain == nil {
		b.Fatal("composite benchmark fixture was not built")
	}
	return compositeBenchDecl, compositeBenchPlain
}

// compositeBenchQuery pins both keys. Roughly 100000/143 nodes satisfy it.
func compositeBenchQuery() store.NodeQuery {
	c, bk := compositeBenchKeys(3)
	return store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "case", Op: store.PropertyOpEqual, Value: c},
		{Key: "bucket", Op: store.PropertyOpEqual, Value: bk},
	}}
}

// BenchmarkComposite_Conjunction is the headline: two weak equality filters
// against a declared composite.
func BenchmarkComposite_Conjunction(b *testing.B) {
	g, _ := compositeBenchFixture(b)
	q := compositeBenchQuery()
	reportPlan(b, g, q)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComposite_Conjunction_Undeclared is the same query on the same graph
// with nothing declared — the plan a build without composites produces.
func BenchmarkComposite_Conjunction_Undeclared(b *testing.B) {
	_, g := compositeBenchFixture(b)
	q := compositeBenchQuery()
	reportPlan(b, g, q)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComposite_SingleKey is the control: a query on one of the same keys,
// which no composite can serve. It must not move between the two arms — if it
// does, the headline is measuring something other than the driver.
func BenchmarkComposite_SingleKey(b *testing.B) {
	g, _ := compositeBenchFixture(b)
	c, _ := compositeBenchKeys(3)
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "case", Op: store.PropertyOpEqual, Value: c},
	}}
	reportPlan(b, g, q)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkComposite_SingleKey_Undeclared(b *testing.B) {
	_, g := compositeBenchFixture(b)
	c, _ := compositeBenchKeys(3)
	q := store.NodeQuery{Filters: []store.PropertyFilter{
		{Key: "case", Op: store.PropertyOpEqual, Value: c},
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.QueryNodeIDs(q); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComposite_Ingest and its Undeclared twin are the write side: what a
// declaration costs the registration path it hooks into. Each iteration
// registers both member keys for one new node, which is one composite filing.
func BenchmarkComposite_Ingest(b *testing.B)            { benchCompositeIngest(b, true) }
func BenchmarkComposite_Ingest_Undeclared(b *testing.B) { benchCompositeIngest(b, false) }

func benchCompositeIngest(b *testing.B, declare bool) {
	g := graphene.NewInMemory()
	if declare {
		if err := g.DeclareCompositeProperties([]string{"case", "bucket"}); err != nil {
			b.Fatalf("DeclareCompositeProperties: %v", err)
		}
	}
	ids := make([]store.NodeID, b.N)
	for i := range ids {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			b.Fatalf("AddNode: %v", err)
		}
		ids[i] = id
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, bk := compositeBenchKeys(i)
		if err := g.IndexNodeProperty(ids[i], "case", c); err != nil {
			b.Fatal(err)
		}
		if err := g.IndexNodeProperty(ids[i], "bucket", bk); err != nil {
			b.Fatal(err)
		}
	}
}

// The disk arms are the number a user actually feels. The memory arms above
// isolate the index maintenance; on a disk store the same registration also
// frames and appends a WAL record, which is the larger cost and does not change
// between the two arms — so the ratio here is the honest one and the ratio above
// is the upper bound.
func BenchmarkComposite_IngestDisk(b *testing.B)            { benchCompositeIngestDisk(b, true) }
func BenchmarkComposite_IngestDisk_Undeclared(b *testing.B) { benchCompositeIngestDisk(b, false) }

func benchCompositeIngestDisk(b *testing.B, declare bool) {
	g, err := graphene.Open(b.TempDir())
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() { _ = g.Close() }()
	if declare {
		if err := g.DeclareCompositeProperties([]string{"case", "bucket"}); err != nil {
			b.Fatalf("DeclareCompositeProperties: %v", err)
		}
	}
	ids := make([]store.NodeID, b.N)
	for i := range ids {
		id, err := g.AddNode(&store.Node{Labels: []store.NodeType{store.NodeTypeMicroArtefact}})
		if err != nil {
			b.Fatalf("AddNode: %v", err)
		}
		ids[i] = id
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, bk := compositeBenchKeys(i)
		if err := g.IndexNodeProperty(ids[i], "case", c); err != nil {
			b.Fatal(err)
		}
		if err := g.IndexNodeProperty(ids[i], "bucket", bk); err != nil {
			b.Fatal(err)
		}
	}
}
