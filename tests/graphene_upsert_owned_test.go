package graphene_test

// UpsertNodeOwned hands the store the caller's node instead of a copy of it.
// Ownership is the only difference, so what these ask is that nothing else
// differs: the same record lands, the update path still replaces, and the bytes
// survive a reopen — which is where a record the engine merely borrowed would
// come back wrong.
//
// The pointer-level contract (the buffered op *is* the caller's node) is asserted
// in the root package, where tx.ops is visible.

import (
	"bytes"
	"testing"

	"github.com/aoiflux/graphene"
	"github.com/aoiflux/graphene/store"
)

func TestUpsertOwned_LandsTheSameRecordAsUpsert(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}

		labels := []store.NodeType{store.NodeTypeEvidenceFile, store.NodeTypeTag}
		blob := []byte("a blob long enough that a copy would be visible in a profile")

		tx := g.Begin()
		copied, _ := tx.UpsertNode("k", []byte("copied"),
			&store.Node{Labels: labels, Properties: blob},
			map[string][]byte{"state": []byte("discovered")})
		owned, _ := tx.UpsertNodeOwned("k", []byte("owned"),
			&store.Node{Labels: labels, Properties: blob},
			map[string][]byte{"state": []byte("discovered")})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}

		a := getNodeOrFail(t, g, copied)
		b := getNodeOrFail(t, g, owned)
		if !bytes.Equal(a.Properties, b.Properties) {
			t.Fatalf("properties differ: copied %q, owned %q", a.Properties, b.Properties)
		}
		if !bytes.Equal(b.Properties, blob) {
			t.Fatalf("owned properties are %q, want %q", b.Properties, blob)
		}
		if len(a.Labels) != len(b.Labels) {
			t.Fatalf("labels differ: copied %v, owned %v", a.Labels, b.Labels)
		}
		for i := range a.Labels {
			if a.Labels[i] != b.Labels[i] {
				t.Fatalf("labels differ at %d: copied %v, owned %v", i, a.Labels, b.Labels)
			}
		}

		// Both keys resolved to their own node, so the second call did not
		// quietly reuse the first one's row.
		if copied == owned {
			t.Fatal("two distinct keys resolved to one node")
		}
	})
}

func TestUpsertOwned_SecondUpsertReplacesRatherThanCreates(t *testing.T) {
	backends(t, func(t *testing.T, g *graphene.Graph) {
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		key := []byte("ver:9f3a")

		tx := g.Begin()
		first, created := tx.UpsertNodeOwned("k", key,
			&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte("v1")},
			map[string][]byte{"state": []byte("discovered")})
		if !created {
			t.Fatal("first upsert reported created=false")
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("first Commit: %v", err)
		}

		tx = g.Begin()
		again, created := tx.UpsertNodeOwned("k", key,
			&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: []byte("v2")},
			map[string][]byte{"state": []byte("extracted")})
		if created {
			t.Fatal("second upsert reported created=true")
		}
		if again != first {
			t.Fatalf("second upsert resolved to %d, want %d", again, first)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("second Commit: %v", err)
		}

		if got := getNodeOrFail(t, g, first).Properties; !bytes.Equal(got, []byte("v2")) {
			t.Fatalf("properties are %q after the replacing upsert, want v2", got)
		}
		// The superseded entry must stop matching, or a field used as a work
		// queue never drains.
		if n, err := g.NodeByProperty("state", []byte("discovered")); err == nil && n != nil {
			t.Fatalf("the superseded entry still matches, on node %d", n.ID)
		}
	})
}

func TestUpsertOwned_SurvivesAReopen(t *testing.T) {
	dir := t.TempDir()
	blob := []byte("bytes the engine never copied")

	open := func() *graphene.Graph {
		t.Helper()
		g, err := graphene.Open(dir)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := g.DeclareUniqueProperty("k"); err != nil {
			t.Fatalf("DeclareUniqueProperty: %v", err)
		}
		return g
	}

	g := open()
	tx := g.Begin()
	id, _ := tx.UpsertNodeOwned("k", []byte("ver:aaa"),
		&store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}, Properties: blob}, nil)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g = open()
	defer g.Close()
	if got := getNodeOrFail(t, g, id).Properties; !bytes.Equal(got, blob) {
		t.Fatalf("after a reopen the blob is %q, want %q", got, blob)
	}
}

func getNodeOrFail(t *testing.T, g *graphene.Graph, id store.NodeID) *store.Node {
	t.Helper()
	n, err := g.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode(%d): %v", id, err)
	}
	return n
}
