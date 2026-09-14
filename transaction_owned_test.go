package graphene

// UpsertNodeOwned's contract is about who owns a pointer, and the only place
// that is decidable is here: the buffered op. Everything downstream — both
// backends retain the record rather than copy it — is already covered by the
// upsert suite in tests/, which runs both methods to the same fingerprint.

import (
	"strings"
	"testing"

	"github.com/aoiflux/graphene/store"
)

func ownedTestGraph(t *testing.T) *Graph {
	t.Helper()
	g := NewInMemory()
	t.Cleanup(func() { _ = g.Close() })
	if err := g.DeclareUniqueProperty("k"); err != nil {
		t.Fatalf("DeclareUniqueProperty: %v", err)
	}
	return g
}

func TestUpsertNodeOwned_BuffersTheCallersNode(t *testing.T) {
	g := ownedTestGraph(t)

	n := &store.Node{
		Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
		Properties: []byte("blob"),
	}

	tx := g.Begin()
	id, created := tx.UpsertNodeOwned("k", []byte("ver:1"), n, nil)
	if !created {
		t.Fatal("first upsert reported created=false")
	}
	if len(tx.ops) != 1 {
		t.Fatalf("buffered %d ops, want 1", len(tx.ops))
	}

	// The point of the method: the buffered record *is* the caller's node, not a
	// copy of it. Pointer identity rather than an allocation count, because an
	// allocation count would also pass if the copy were merely cheaper.
	if tx.ops[0].Node != n {
		t.Fatal("UpsertNodeOwned buffered a copy of the node")
	}
	if &tx.ops[0].Node.Properties[0] != &n.Properties[0] {
		t.Fatal("UpsertNodeOwned copied the properties slice")
	}
	if n.ID != id {
		t.Fatalf("caller's node has ID %d, resolved ID is %d", n.ID, id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestUpsertNode_BuffersACopy(t *testing.T) {
	g := ownedTestGraph(t)

	n := &store.Node{
		Labels:     []store.NodeType{store.NodeTypeEvidenceFile},
		Properties: []byte("blob"),
	}

	tx := g.Begin()
	if _, created := tx.UpsertNode("k", []byte("ver:1"), n, nil); !created {
		t.Fatal("first upsert reported created=false")
	}
	if tx.ops[0].Node == n {
		t.Fatal("UpsertNode buffered the caller's node rather than a copy")
	}
	if &tx.ops[0].Node.Properties[0] == &n.Properties[0] {
		t.Fatal("UpsertNode shared the properties slice with the caller")
	}
	// The copying method leaves the caller's struct alone, ID included. That is
	// the half of the contract the owned variant gives up, so it is worth an
	// assertion of its own rather than an implication of the one above.
	if n.ID != store.InvalidNodeID {
		t.Fatalf("UpsertNode wrote ID %d into the caller's node", n.ID)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestUpsertNodeOwned_StillCopiesTheKeyAndTheEntries(t *testing.T) {
	g := ownedTestGraph(t)

	value := []byte("ver:1")
	props := map[string][]byte{"state": []byte("discovered")}
	n := &store.Node{Labels: []store.NodeType{store.NodeTypeEvidenceFile}}

	tx := g.Begin()
	if _, created := tx.UpsertNodeOwned("k", value, n, props); !created {
		t.Fatal("first upsert reported created=false")
	}
	op := tx.ops[0]

	// Ownership is of the node alone. A reused key buffer and a refilled entry
	// map are both ordinary caller patterns and both are small beside the blob,
	// so these two copies stay — and a future change that drops them for symmetry
	// should have to delete a test that says why.
	if &op.Value[0] == &value[0] {
		t.Fatal("UpsertNodeOwned retained the caller's key value")
	}
	if &op.Props["state"][0] == &props["state"][0] {
		t.Fatal("UpsertNodeOwned retained the caller's entry value")
	}
	props["state"] = []byte("mutated")
	if string(op.Props["state"]) != "discovered" {
		t.Fatalf("buffered entry followed the caller's map: %q", op.Props["state"])
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestUpsertNodeOwned_ErrorsNameTheMethodCalled(t *testing.T) {
	g := ownedTestGraph(t)

	tx := g.Begin()
	if _, created := tx.UpsertNodeOwned("k", []byte("ver:1"), nil, nil); created {
		t.Fatal("a nil node reported created=true")
	}
	err := tx.Commit()
	if err == nil {
		t.Fatal("Commit of a failed transaction succeeded")
	}
	if !strings.Contains(err.Error(), "Tx.UpsertNodeOwned") {
		t.Fatalf("error names the wrong method: %v", err)
	}
}
